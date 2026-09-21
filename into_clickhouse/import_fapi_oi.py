#!/usr/bin/env python3
"""Import downloaded Binance USD-M futures 5-minute metrics (open interest) into ClickHouse.

Source  : rawdata/futures/um/daily/metrics/<symbol>/<SYMBOL>-metrics-YYYY-MM-DD.csv
          (produced by ``python -m binance_archive crawl --config config/system_metrics.yaml``)
Target  : market.fapi_oi_5m   (see into_clickhouse/deployed/004_fapi_oi.sql)


Timestamp semantics (verified, not assumed)
-------------------------------------------
``create_time`` is a wall-clock string ``YYYY-MM-DD HH:MM:SS`` in **UTC** and is the
CLOSE time of the 5m bar the snapshot belongs to.  Verified by comparing a full day of
BTCUSDT archive rows with ``/futures/data/openInterestHist`` (UTC epoch ms): median
relative difference 0.04% at offset 0h vs 0.25% at +-1h and 0.9-1.4% at +-5/8h.

    start_time = create_time(UTC) - 5 minutes      (kline OPEN time; joins fapi_kline_5m)
    snap_time  = create_time(UTC)

The subtraction is done on the full timestamp, so midnight needs no special casing:
the ``00:00:00`` row of file D becomes ``23:55`` of day D-1 (that day's last candle
therefore only arrives with the NEXT day's file).  Rows inside a file are NOT sorted
by time; order is irrelevant here since (symbol, start_time) is the key.

    archive CSV                       market.fapi_oi_5m
    --------------------------------  ------------------------------------
    (from file name)                  symbol
    create_time  'YYYY-MM-DD HH:MM:SS'  start_time = create_time - 5 min
                                      snap_time  = create_time
    sum_open_interest                 sum_open_interest
    (other 5 columns)                 dropped
    -                                 src_rank   = 3 (archive: outranks live=1, hist=2)
    -                                 created_at DEFAULT now()

Rows with an empty sum_open_interest are skipped.  Idempotent (ReplacingMergeTree by
src_rank; equal rank -> last inserted wins).  Files are loaded in per-symbol batches to
keep the number of HTTP requests sane (~hundreds of thousands of daily files).  The
manifest ``into_clickhouse/.imported.log`` is shared with import_fapi_kline.py.

Usage
-----
    python into_clickhouse/import_fapi_oi.py
    python into_clickhouse/import_fapi_oi.py --symbols BTCUSDT,ETHUSDT
    python into_clickhouse/import_fapi_oi.py --start 2024-01 --end 2024-12
    python into_clickhouse/import_fapi_oi.py --dry-run -v

Connection (flag > env > default): CLICKHOUSE_URL / _DATABASE / _USER / _PASSWORD.
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import sys
import time
from itertools import groupby
from pathlib import Path

import requests

LOG = logging.getLogger("import_fapi_oi")

REPO_ROOT = Path(__file__).resolve().parents[1]
MANIFEST = Path(__file__).resolve().with_name(".imported.log")

ARCHIVE_SUBTREE = Path("futures/um/daily/metrics")
FILE_RE = re.compile(r"^(?P<sym>.+?)-metrics-(?P<day>\d{4}-\d{2}-\d{2})\.csv$")

SRC_RANK_ARCHIVE = 3
BAR_MINUTES = 5
BATCH_FILES = 100  # daily files per POST (~29k rows)

TARGET_COLS = ["symbol", "start_time", "sum_open_interest", "snap_time", "src_rank"]

# raw CSV: create_time,symbol,sum_open_interest,sum_open_interest_value,
#          count_toptrader_long_short_ratio,sum_toptrader_long_short_ratio,
#          count_long_short_ratio,sum_taker_long_short_vol_ratio
INPUT_SCHEMA = (
    "c0 String, c1 String, c2 String, c3 String, "
    "c4 String, c5 String, c6 String, c7 String"
)


def _build_query(table: str, symbol: str) -> str:
    sym = symbol.replace("\\", "\\\\").replace("'", "\\'")
    cols = ", ".join(TARGET_COLS)
    # must NOT end with a newline (see import_fapi_kline.py)
    return (
        f"INSERT INTO {table} ({cols}) "
        f"SELECT "
        f"'{sym}' AS symbol, "
        f"toDateTime64(c0, 3, 'UTC') - INTERVAL {BAR_MINUTES} MINUTE AS start_time, "
        f"toFloat64OrNull(c2) AS sum_open_interest, "
        f"toDateTime64(c0, 3, 'UTC') AS snap_time, "
        f"toUInt8({SRC_RANK_ARCHIVE}) AS src_rank "
        f"FROM input('{INPUT_SCHEMA}') "
        f"WHERE sum_open_interest IS NOT NULL "
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
        self.session.headers["User-Agent"] = "fapi-oi-importer/1.0"

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
            params={"database": self.database, "query": sql, "input_format_allow_errors_num": "0"},
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
        if self.query(f"EXISTS TABLE {db}.{name}").strip() != "1":  # noqa: S608
            raise RuntimeError(
                f"table {db}.{name} not found - run into_clickhouse/deployed/004_fapi_oi.sql first"
            )
        described = self.query(f"DESCRIBE TABLE {db}.{name} FORMAT TSV")
        cols = {line.split("\t", 1)[0] for line in described.splitlines() if line}
        missing = [c for c in TARGET_COLS if c not in cols]
        if missing:
            raise RuntimeError(f"table {db}.{name} is missing expected columns: {missing}")
        LOG.info("target %s.%s OK (%d columns)", db, name, len(cols))


def discover(rawdata: Path, symbols: set[str] | None, start: str | None, end: str | None):
    """Return (path, symbol, day) for every matching daily CSV, sorted by symbol/day.

    ``start`` / ``end`` are YYYY-MM months (inclusive) compared against the file's day.
    """
    base = rawdata / ARCHIVE_SUBTREE
    if not base.is_dir():
        raise FileNotFoundError(f"{base} not found - nothing downloaded for um daily metrics")
    hits: list[tuple[Path, str, str]] = []
    for csv_path in base.glob("*/*.csv"):
        m = FILE_RE.match(csv_path.name)
        if not m:
            LOG.debug("skip unrecognised file name: %s", csv_path.name)
            continue
        sym, day = m["sym"], m["day"]
        if symbols is not None and sym not in symbols:
            continue
        if start and day[:7] < start:
            continue
        if end and day[:7] > end:
            continue
        hits.append((csv_path, sym, day))
    hits.sort(key=lambda t: (t[1], t[2]))
    return hits


def _read_body(path: Path) -> bytes:
    """File bytes without the header line, always newline-terminated (CRLF -> LF)."""
    raw = path.read_bytes().replace(b"\r\n", b"\n")
    if raw[:11].lower() == b"create_time":
        nl = raw.find(b"\n")
        raw = raw[nl + 1 :] if nl != -1 else b""
    if raw and not raw.endswith(b"\n"):
        raw += b"\n"
    return raw


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
    ap.add_argument("--rawdata", type=Path, default=REPO_ROOT / "rawdata")
    ap.add_argument("--url", default=os.environ.get("CLICKHOUSE_URL", "http://localhost:8123"))
    ap.add_argument("--database", default=os.environ.get("CLICKHOUSE_DATABASE", "market"))
    ap.add_argument("--user", default=os.environ.get("CLICKHOUSE_USER", "default"))
    ap.add_argument("--password", default=os.environ.get("CLICKHOUSE_PASSWORD", ""))
    ap.add_argument("--table", default="market.fapi_oi_5m")
    ap.add_argument("--symbols", help="comma-separated allow-list, e.g. BTCUSDT,ETHUSDT")
    ap.add_argument("--start", help="earliest month to load, YYYY-MM (inclusive)")
    ap.add_argument("--end", help="latest month to load, YYYY-MM (inclusive)")
    ap.add_argument("--limit", type=int, help="stop after N files (smoke test)")
    ap.add_argument("--batch-files", type=int, default=BATCH_FILES, help="daily files per POST")
    ap.add_argument("--reimport", action="store_true", help="ignore the manifest")
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
        (p, s, d) for (p, s, d) in files
        if p.relative_to(args.rawdata).as_posix() not in done
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
        for p, s, d in pending[:50]:
            print(f"{s:20} {d}  {p}")
        if len(pending) > 50:
            print(f"... (+{len(pending) - 50} more)")
        print("\n--- generated statement (first file) ---")
        print(_build_query(args.table, pending[0][1]))
        return 0

    client = Client(args.url, args.database, args.user, args.password)
    client.preflight(args.table)

    total_rows = 0
    n_ok = 0
    errors: list[str] = []
    started = time.monotonic()
    mf = None if not use_manifest else MANIFEST.open("a", encoding="utf-8")
    try:
        for sym, grp in groupby(pending, key=lambda t: t[1]):
            items = list(grp)
            for b in range(0, len(items), args.batch_files):
                batch = items[b : b + args.batch_files]
                parts = [(p, _read_body(p)) for p, _, _ in batch]
                parts = [(p, body) for p, body in parts if body.strip()]
                if not parts:
                    continue
                body = b"".join(bd for _, bd in parts)
                n_rows = body.count(b"\n")
                first_day, last_day = batch[0][2], batch[-1][2]
                try:
                    client.insert_csv(_build_query(args.table, sym), body)
                except Exception as exc:  # noqa: BLE001 - report and continue
                    LOG.error("%s %s..%s FAILED: %s", sym, first_day, last_day, exc)
                    errors.append(f"{sym} {first_day}..{last_day}: {exc}")
                    continue
                total_rows += n_rows
                n_ok += len(parts)
                if mf is not None:
                    for p, _ in parts:
                        mf.write(f"{p.relative_to(args.rawdata).as_posix()}\n")
                    mf.flush()
                elapsed = time.monotonic() - started
                LOG.info(
                    "%-14s %s..%s  %d file(s) ~%d rows  (cum %.2fM rows, %.0fs)",
                    sym, first_day, last_day, len(parts), n_rows, total_rows / 1e6, elapsed,
                )
    finally:
        if mf is not None:
            mf.close()

    LOG.info(
        "done: %d/%d file(s), ~%.2fM rows, %.0fs%s",
        n_ok, len(pending), total_rows / 1e6, time.monotonic() - started,
        f", {len(errors)} error(s)" if errors else "",
    )
    for e in errors[:20]:
        print(f"  - {e}", file=sys.stderr)
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
