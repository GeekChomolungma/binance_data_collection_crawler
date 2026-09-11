#!/usr/bin/env python3
"""Import downloaded Binance USD-M futures 1-minute klines into ClickHouse.

Source  : rawdata/futures/um/monthly/klines/<symbol>/1m/<SYMBOL>-1m-YYYY-MM.csv
          (produced by ``python -m binance_archive crawl`` with market=um interval=1m)
Target  : market.fapi_kline_1m   (see into_clickhouse/deployed/001_fapi_kline.sql)


Why the CSV and the table do not line up 1:1
--------------------------------------------
The archive CSV has 12 positional columns and NO stable header row (months up to
~2024 have none, 2025+ months carry ``open_time,open,high,...``).  Binance's
column order is NOT the table's column order, CSV column 11 ("ignore") is
dropped, and ``symbol`` is not in the file at all - it comes from the path/name.
The table then has a 13th column, ``created_at``, that the DDL fills with
``DEFAULT now()``.

    archive CSV (index)               market.fapi_kline_1m
    --------------------------------  ------------------------------------
    (from file name)                  symbol                  LowCardinality(String)
    0   open_time            (ms/us)  start_time              DateTime64(3,'UTC')
    6   close_time           (ms/us)  end_time                DateTime64(3,'UTC')
    1   open                          open                    Float64
    2   high                          high                    Float64
    3   low                           low                     Float64
    4   close                         close                   Float64
    5   volume                        volume                  Float64
    7   quote_asset_volume            quote_volume            Float64
    9   taker_buy_base_volume         taker_buy_volume        Float64
    10  taker_buy_quote_volume        taker_buy_quote_volume  Float64
    8   count                         trades_count            UInt32
    11  ignore                        (dropped)
    -                                 created_at              DateTime DEFAULT now()

The 13-vs-12 mismatch is solved by NEVER doing a positional INSERT.  We send an
explicit list of exactly the 12 columns we have; ClickHouse then applies the
``created_at`` DEFAULT itself.  The reorder / drop / epoch->DateTime64 conversion
is pushed down to the server with ``INSERT ... SELECT ... FROM input(...)``, so
the Python side only strips an optional header line and streams the raw CSV bytes
over the HTTP interface (no new dependency - reuses ``requests``).

Idempotent: the target is ReplacingMergeTree((symbol, start_time)), so re-running
is safe.  A local manifest (``into_clickhouse/.imported.log``) additionally lets a
re-run skip files already loaded; pass --reimport to ignore it.

Usage
-----
    python into_clickhouse/import_fapi_kline.py                   # load everything
    python into_clickhouse/import_fapi_kline.py --symbols BTCUSDT,ETHUSDT
    python into_clickhouse/import_fapi_kline.py --start 2024-01 --end 2024-12
    python into_clickhouse/import_fapi_kline.py --dry-run -v

Connection (flag > env > default):
    --url        CLICKHOUSE_URL        http://localhost:8123
    --database   CLICKHOUSE_DATABASE   market
    --user       CLICKHOUSE_USER       default
    --password   CLICKHOUSE_PASSWORD   (empty)
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import sys
import time
from pathlib import Path

import requests

LOG = logging.getLogger("import_fapi_kline")

REPO_ROOT = Path(__file__).resolve().parents[1]
MANIFEST = Path(__file__).resolve().with_name(".imported.log")

# archive layout for market=um, data_frequency=monthly, data_type=klines, interval=1m
ARCHIVE_SUBTREE = Path("futures/um/monthly/klines")
# a handful of listings carry non-ASCII names (e.g. 我踏马来了USDT); accept any
# token before the -1m- stamp so nothing is silently dropped.
FILE_RE = re.compile(r"^(?P<sym>.+?)-1m-(?P<month>\d{4}-\d{2})\.csv$")

# target columns, in the exact order the SELECT below produces them. `created_at`
# is deliberately omitted -> ClickHouse fills it from `DEFAULT now()`.
TARGET_COLS = [
    "symbol",
    "start_time",
    "end_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "quote_volume",
    "taker_buy_volume",
    "taker_buy_quote_volume",
    "trades_count",
]

# schema handed to input(): the raw CSV's 12 positional columns, named c0..c11.
INPUT_SCHEMA = (
    "c0 Int64, c1 Float64, c2 Float64, c3 Float64, c4 Float64, c5 Float64, "
    "c6 Int64, c7 Float64, c8 Int64, c9 Float64, c10 Float64, c11 String"
)

# epoch magnitudes: >=1e14 can only be microseconds; anything smaller is millis.
_US_THRESHOLD = 100_000_000_000_000


def _build_query(table: str, symbol: str, ts_fn: str) -> str:
    """INSERT ... SELECT ... FROM input(): server-side reorder + drop + ts cast.

    ``ts_fn`` is fromUnixTimestamp64Milli or fromUnixTimestamp64Micro, chosen per
    file from the magnitude of its first timestamp.
    """
    sym = symbol.replace("\\", "\\\\").replace("'", "\\'")
    cols = ", ".join(TARGET_COLS)
    # NOTE: must NOT end with a newline. ClickHouse's HTTP interface joins the
    # `query` param and the POST body with a single '\n'; a trailing '\n' here
    # would make the data stream start with a blank line, which FORMAT CSV reads
    # as an empty row 1 ("Cannot parse input: expected ',' before: '\n...'").
    return (
        f"INSERT INTO {table} ({cols}) "
        f"SELECT "
        f"'{sym}' AS symbol, "
        f"{ts_fn}(c0, 'UTC') AS start_time, "
        f"{ts_fn}(c6, 'UTC') AS end_time, "
        f"c1, c2, c3, c4, c5, c7, c9, c10, "
        f"toUInt32(c8) AS trades_count "
        f"FROM input('{INPUT_SCHEMA}') "
        f"FORMAT CSV"
    )


class Client:
    """Thin wrapper over the ClickHouse HTTP interface (port 8123)."""

    def __init__(self, url: str, database: str, user: str, password: str) -> None:
        self.url = url.rstrip("/")
        self.database = database
        self.session = requests.Session()
        if user:
            self.session.auth = (user, password)
        self.session.headers["User-Agent"] = "fapi-kline-importer/1.0"

    def query(self, sql: str, *, timeout: int = 30) -> str:
        resp = self.session.post(
            self.url, params={"database": self.database, "query": sql}, timeout=timeout
        )
        if resp.status_code != 200:
            raise RuntimeError(f"query failed ({resp.status_code}): {resp.text[:2000]}")
        return resp.text

    def insert_csv(self, sql: str, body: bytes, *, timeout: int = 600) -> None:
        resp = self.session.post(
            self.url,
            params={
                "database": self.database,
                "query": sql,
                # a header line is stripped before send, but be forgiving anyway
                "input_format_allow_errors_num": "0",
            },
            data=body,
            timeout=timeout,
        )
        if resp.status_code != 200:
            raise RuntimeError(f"insert failed ({resp.status_code}): {resp.text[:2000]}")

    def preflight(self, table: str) -> None:
        if self.query("SELECT 1").strip() != "1":
            raise RuntimeError("SELECT 1 did not return 1 - bad connection")
        db, _, name = table.partition(".")
        if not name:
            db, name = self.database, db
        exists = self.query(
            f"EXISTS TABLE {db}.{name}"  # noqa: S608 - identifiers are ours
        ).strip()
        if exists != "1":
            raise RuntimeError(
                f"table {db}.{name} not found - run into_clickhouse/deployed/001_fapi_kline.sql first"
            )
        described = self.query(f"DESCRIBE TABLE {db}.{name} FORMAT TSV")
        cols = {line.split("\t", 1)[0] for line in described.splitlines() if line}
        missing = [c for c in TARGET_COLS if c not in cols]
        if missing:
            raise RuntimeError(f"table {db}.{name} is missing expected columns: {missing}")
        if "created_at" not in cols:
            LOG.warning("table has no `created_at` column - DDL may be out of date")
        LOG.info("target %s.%s OK (%d columns)", db, name, len(cols))


def discover(rawdata: Path, symbols: set[str] | None, start: str | None, end: str | None):
    """Yield (path, symbol, month) for every matching monthly CSV, sorted."""
    base = rawdata / ARCHIVE_SUBTREE
    if not base.is_dir():
        raise FileNotFoundError(f"{base} not found - nothing downloaded for market=um interval=1m")
    hits: list[tuple[Path, str, str]] = []
    for csv_path in base.glob("*/1m/*.csv"):
        m = FILE_RE.match(csv_path.name)
        if not m:
            LOG.debug("skip unrecognised file name: %s", csv_path.name)
            continue
        sym, month = m["sym"], m["month"]
        if symbols is not None and sym not in symbols:
            continue
        if start and month < start:
            continue
        if end and month > end:
            continue
        hits.append((csv_path, sym, month))
    hits.sort(key=lambda t: (t[1], t[2]))
    return hits


def _strip_header(raw: bytes) -> bytes:
    """Drop a leading `open_time,...` header line if present (2025+ archives)."""
    if raw[:9].lower() == b"open_time":
        nl = raw.find(b"\n")
        return raw[nl + 1 :] if nl != -1 else b""
    return raw


def _ts_fn_for(body: bytes) -> str:
    """Pick the epoch->DateTime64 function from the first data row's magnitude."""
    comma = body.find(b",")
    if comma > 0:
        head = body[:comma].strip()
        if head.isdigit() and int(head) >= _US_THRESHOLD:
            return "fromUnixTimestamp64Micro"
    return "fromUnixTimestamp64Milli"


def load_manifest(use_it: bool) -> set[str]:
    if not use_it or not MANIFEST.exists():
        return set()
    return {
        line.strip()
        for line in MANIFEST.read_text(encoding="utf-8").splitlines()
        if line.strip()
    }


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--rawdata", type=Path, default=REPO_ROOT / "rawdata",
                    help="root of the downloaded archive tree (default: <repo>/rawdata)")
    ap.add_argument("--url", default=os.environ.get("CLICKHOUSE_URL", "http://localhost:8123"))
    ap.add_argument("--database", default=os.environ.get("CLICKHOUSE_DATABASE", "market"))
    ap.add_argument("--user", default=os.environ.get("CLICKHOUSE_USER", "default"))
    ap.add_argument("--password", default=os.environ.get("CLICKHOUSE_PASSWORD", ""))
    ap.add_argument("--table", default="market.fapi_kline_1m")
    ap.add_argument("--symbols", help="comma-separated allow-list, e.g. BTCUSDT,ETHUSDT")
    ap.add_argument("--start", help="earliest month to load, YYYY-MM (inclusive)")
    ap.add_argument("--end", help="latest month to load, YYYY-MM (inclusive)")
    ap.add_argument("--limit", type=int, help="stop after N files (smoke test)")
    ap.add_argument("--reimport", action="store_true", help="ignore the manifest, load every file again")
    ap.add_argument("--no-manifest", action="store_true", help="do not read or write the manifest")
    ap.add_argument("--dry-run", action="store_true", help="list what would be loaded, do not POST")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(message)s",
        datefmt="%H:%M:%S",
    )
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="backslashreplace")
        except (AttributeError, ValueError):
            pass

    for m in (args.start, args.end):
        if m and not re.match(r"^\d{4}-\d{2}$", m):
            ap.error(f"month must be YYYY-MM, got {m!r}")

    symbols = None
    if args.symbols:
        symbols = {s.strip().upper() for s in args.symbols.split(",") if s.strip()}

    files = discover(args.rawdata, symbols, args.start, args.end)
    if not files:
        LOG.warning("no CSV files matched")
        return 0

    use_manifest = not args.no_manifest and not args.reimport
    done = load_manifest(use_manifest)
    pending = [
        (p, s, mo)
        for (p, s, mo) in files
        if str(p.relative_to(args.rawdata).as_posix()) not in done
    ]
    skipped = len(files) - len(pending)
    if args.limit:
        pending = pending[: args.limit]

    LOG.info(
        "%d file(s) matched, %d already in manifest, %d to load",
        len(files), skipped, len(pending),
    )
    if not pending:
        LOG.info("nothing to do")
        return 0

    if args.dry_run:
        for p, s, mo in pending[:50]:
            print(f"{s:20} {mo}  {p}")
        if len(pending) > 50:
            print(f"... (+{len(pending) - 50} more)")
        sample = pending[0][0].read_bytes()[:4096]
        body = _strip_header(sample)
        print("\n--- generated statement (first file) ---")
        print(_build_query(args.table, pending[0][1], _ts_fn_for(body)))
        return 0

    client = Client(args.url, args.database, args.user, args.password)
    client.preflight(args.table)

    total_rows = 0
    errors: list[str] = []
    started = time.monotonic()
    mf = None if not use_manifest else MANIFEST.open("a", encoding="utf-8")
    try:
        for i, (path, sym, month) in enumerate(pending, 1):
            raw = path.read_bytes()
            body = _strip_header(raw)
            if not body.strip():
                LOG.warning("[%d/%d] %s %s  empty, skipped", i, len(pending), sym, month)
                continue
            n_rows = body.count(b"\n") + (0 if body.endswith(b"\n") else 1)
            sql = _build_query(args.table, sym, _ts_fn_for(body))
            try:
                client.insert_csv(sql, body)
            except Exception as exc:  # noqa: BLE001 - report and continue
                LOG.error("[%d/%d] %s %s  FAILED: %s", i, len(pending), sym, month, exc)
                errors.append(f"{sym} {month}: {exc}")
                continue
            total_rows += n_rows
            if mf is not None:
                mf.write(f"{path.relative_to(args.rawdata).as_posix()}\n")
                mf.flush()
            if i % 50 == 0 or i == len(pending):
                elapsed = time.monotonic() - started
                LOG.info(
                    "[%d/%d] %s %s  ~%d rows  (cum %.2fM rows, %.0fs, %.0f rows/s)",
                    i, len(pending), sym, month, n_rows,
                    total_rows / 1e6, elapsed, total_rows / elapsed if elapsed else 0,
                )
            else:
                LOG.debug("[%d/%d] %s %s  ~%d rows", i, len(pending), sym, month, n_rows)
    finally:
        if mf is not None:
            mf.close()

    elapsed = time.monotonic() - started
    LOG.info(
        "done: %d/%d file(s), ~%.2fM rows, %.0fs%s",
        len(pending) - len(errors), len(pending), total_rows / 1e6, elapsed,
        f", {len(errors)} error(s)" if errors else "",
    )
    for e in errors[:20]:
        print(f"  - {e}", file=sys.stderr)
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
