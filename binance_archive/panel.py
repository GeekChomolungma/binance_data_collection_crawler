"""Stitch the per-symbol archive CSVs into one long-format panel CSV.

Decoupled from the crawler: it only reads files off disk (whatever ``crawl``
produced under ``input_dir``) and writes a single aggregated CSV.  It never
touches the network.

Panel schema
------------
    timestamp, symbol, <remaining original columns...>

``timestamp`` + ``symbol`` form the composite key.  ``timestamp`` comes from
column 0 of the raw CSV; ``symbol`` is the name as written in the symbols file
(original case, e.g. ``BTCUSDT``).

Raw kline CSV columns (Binance publishes no header row) - 12 columns:
    0  open time            6  close time
    1  open                 7  quote asset volume
    2  high                 8  number of trades
    3  low                  9  taker buy base asset volume
    4  close               10  taker buy quote asset volume
    5  volume              11  ignore (unused by Binance)

Heads-up: Binance switched the archive time unit from **milliseconds** to
**microseconds** during 2025 (columns 0 and 6).  ``panel.timestamp`` controls how
that is reconciled - the default (``ms``) auto-detects the unit per row and
normalises everything to integer epoch milliseconds.
"""

from __future__ import annotations

import csv
import datetime as dt
import heapq
import logging
from dataclasses import dataclass
from pathlib import Path

from .config import DATA_TYPES, KLINE_TYPES, MARKETS, _MONTH_RE, Config
from . import symbols as symbols_mod
from . import vision

LOG = logging.getLogger(__name__)

_KLINE_COLS = [
    "open_time", "open", "high", "low", "close", "volume", "close_time",
    "quote_volume", "count", "taker_buy_volume", "taker_buy_quote_volume", "ignore",
]
# raw-CSV column names per data_type (no header rows exist in the files)
_SCHEMAS: dict[str, list[str]] = {k: _KLINE_COLS for k in KLINE_TYPES}
_SCHEMAS["aggTrades"] = [
    "agg_trade_id", "price", "quantity", "first_trade_id", "last_trade_id",
    "transact_time", "is_buyer_maker", "is_best_match",
]
_SCHEMAS["trades"] = [
    "trade_id", "price", "quantity", "quote_quantity", "time",
    "is_buyer_maker", "is_best_match",
]
# column indices holding an epoch time that should be unit-normalised
_TIME_COLS: dict[str, tuple[int, ...]] = {k: (0, 6) for k in KLINE_TYPES}
_TIME_COLS["aggTrades"] = (5,)
_TIME_COLS["trades"] = (4,)

_EPOCH = dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)
_TS_MODES = {"ms", "us", "iso", "raw"}


# ── config ──────────────────────────────────────────────────────────────────
@dataclass
class PanelConfig:
    input_dir: str
    output_dir: str
    output_name: str
    start_month: str | None
    end_month: str | None
    timestamp: str
    write_header: bool
    market: str
    data_frequency: str
    data_type: str
    interval: str
    symbols_file: str
    columns: list[str] | None

    _ALLOWED = {
        "input_dir", "output_dir", "output_name", "start_month", "end_month",
        "timestamp", "write_header", "market", "data_frequency", "data_type",
        "interval", "symbols_file", "columns",
    }

    @classmethod
    def build(cls, cfg: Config) -> "PanelConfig":
        raw = dict(cfg.panel or {})
        unknown = set(raw) - cls._ALLOWED
        if unknown:
            raise ValueError(f"panel: unknown keys: {sorted(unknown)}")
        pc = cls(
            input_dir=raw.get("input_dir", "./rawdata"),
            output_dir=raw.get("output_dir", "./panel"),
            output_name=raw.get("output_name", "panel.csv"),
            start_month=raw.get("start_month"),
            end_month=raw.get("end_month"),
            timestamp=raw.get("timestamp", "ms"),
            write_header=bool(raw.get("write_header", True)),
            # discovery scope: fall back to the crawl block when unset
            market=raw.get("market") or cfg.market,
            data_frequency=raw.get("data_frequency") or cfg.data_frequency,
            data_type=raw.get("data_type") or cfg.data_type,
            interval=raw.get("interval") or cfg.interval,
            symbols_file=raw.get("symbols_file") or cfg.symbols_file,
            columns=raw.get("columns"),
        )
        pc.validate()
        return pc

    @property
    def is_kline(self) -> bool:
        return self.data_type in KLINE_TYPES

    def validate(self) -> None:
        if self.timestamp not in _TS_MODES:
            raise ValueError(f"panel.timestamp must be one of {sorted(_TS_MODES)}")
        if self.market not in MARKETS:
            raise ValueError(f"panel.market must be one of {sorted(MARKETS)}")
        if self.data_type not in DATA_TYPES:
            raise ValueError(f"panel.data_type must be one of {sorted(DATA_TYPES)}")
        if self.is_kline and not self.interval:
            raise ValueError("panel.interval is required for kline data types")
        for name in ("start_month", "end_month"):
            val = getattr(self, name)
            if val is not None and not _MONTH_RE.match(str(val)):
                raise ValueError(f"panel.{name} must be null or 'YYYY-MM', got {val!r}")
        if self.start_month and self.end_month and self.start_month > self.end_month:
            raise ValueError("panel.start_month must be <= panel.end_month")
        if self.columns is not None and not (
            isinstance(self.columns, list) and all(isinstance(c, str) for c in self.columns)
        ):
            raise ValueError("panel.columns must be a list of strings")

    def _scope(self) -> Config:
        """A throwaway Config so we can reuse vision's path layout helpers."""
        return Config(
            market=self.market,
            data_frequency=self.data_frequency,
            data_type=self.data_type,
            interval=self.interval,
            output_dir=self.input_dir,
        )

    def symbol_dir(self, symbol: str) -> Path:
        return vision.local_dir(self._scope(), symbol)

    def output_path(self) -> Path:
        scope = self._scope()
        parts = [self.output_dir, scope.market_path, self.data_frequency, self.data_type]
        if self.is_kline:
            parts.append(self.interval)
        return Path(*parts, self.output_name)


@dataclass
class PanelResult:
    symbols: int          # symbols actually merged
    files: int
    rows: int
    path: Path
    skipped: list[str]    # symbols from the list with no local CSV (warned + skipped)

    def __str__(self) -> str:
        base = f"symbols={self.symbols}, files={self.files}, rows={self.rows}, path={self.path}"
        if self.skipped:
            base += f", skipped={len(self.skipped)}"
        return base


# ── timestamp handling ──────────────────────────────────────────────────────
def _to_millis(n: int) -> int:
    if n >= 1_000_000_000_000_000:   # 1e15+  -> microseconds
        return n // 1000
    if n >= 1_000_000_000_000:       # 1e12+  -> milliseconds
        return n
    return n * 1000                  # seconds


def _norm_ts(value: str, mode: str) -> str:
    if mode == "raw":
        return value
    ms = _to_millis(int(value))
    if mode == "ms":
        return str(ms)
    if mode == "us":
        return str(ms * 1000)
    # iso
    return (_EPOCH + dt.timedelta(milliseconds=ms)).isoformat().replace("+00:00", "Z")


def _sort_key(mode: str):
    if mode == "iso":
        return lambda row: (row[0], row[1])  # ISO strings sort chronologically

    def key(row):
        v = row[0]
        # numeric epoch: compare as int so mixed ms/us history still orders right
        return (int(v), row[1]) if v.lstrip("-").isdigit() else (v, row[1])

    return key


# ── discovery ───────────────────────────────────────────────────────────────
def _discover(
    pc: PanelConfig, symbols: list[str]
) -> tuple[dict[str, list[Path]], list[str]]:
    """Map each symbol to its (time-sorted) CSV paths.

    A symbol whose folder is missing or holds no CSV in range is warned about and
    left out of the result - it never aborts the merge of the other symbols.
    """
    found: dict[str, list[Path]] = {}
    skipped: list[str] = []
    for sym in symbols:
        d = pc.symbol_dir(sym)
        dated: list[tuple[str, Path]] = []
        if d.is_dir():
            for p in d.glob("*.csv"):
                mo = vision.month_of(p.name)
                if mo is None:
                    continue
                if pc.start_month and mo < pc.start_month:
                    continue
                if pc.end_month and mo > pc.end_month:
                    continue
                dated.append((mo, p))
        if not dated:
            reason = "no folder" if not d.is_dir() else "no CSV in range"
            LOG.warning("skip %-16s (%s: %s)", sym, reason, d)
            skipped.append(sym)
            continue
        dated.sort()
        found[sym] = [p for _, p in dated]
    return found, skipped


def _header(pc: PanelConfig, ncols: int) -> list[str]:
    names = pc.columns or _SCHEMAS.get(pc.data_type)
    if not names or len(names) != ncols:
        if names:
            LOG.warning(
                "panel: %s schema has %d columns but the CSV has %d - using positional names",
                pc.data_type, len(names), ncols,
            )
        names = [f"field_{i}" for i in range(ncols)]
    return ["timestamp", "symbol", *names[1:]]


# ── row streams ─────────────────────────────────────────────────────────────
def _symbol_rows(files: list[Path], symbol: str, ts_mode: str, tcols: tuple[int, ...]):
    """Yield transformed rows for one symbol, ordered by time.

    Each monthly file is read fully then closed before the next is opened, so at
    most one file handle is live and only one month sits in memory per symbol.
    """
    for path in files:
        with path.open("r", newline="", encoding="utf-8") as fh:
            rows = list(csv.reader(fh))
        for r in rows:
            if not r or not r[0]:
                continue
            r = list(r)
            for i in tcols:
                if i < len(r):
                    r[i] = _norm_ts(r[i], ts_mode)
            yield [r[0], symbol, *r[1:]]


# ── entry point ─────────────────────────────────────────────────────────────
def build_panel(
    cfg: Config,
    symbols: list[str] | None = None,
    *,
    start: str | None = None,
    end: str | None = None,
    dry_run: bool = False,
) -> PanelResult:
    pc = PanelConfig.build(cfg)
    if start:
        pc.start_month = start
    if end:
        pc.end_month = end
    pc.validate()

    if symbols is None:
        symbols = symbols_mod.read_symbols_file(pc.symbols_file)

    discovered, skipped = _discover(pc, symbols)
    if skipped:
        preview = ", ".join(skipped[:20]) + (" ..." if len(skipped) > 20 else "")
        LOG.warning(
            "panel: %d/%d symbol(s) skipped - no local CSV: %s",
            len(skipped), len(symbols), preview,
        )
    if not discovered:
        raise RuntimeError(
            f"no input CSVs found under {pc.input_dir} - run `crawl` first"
        )

    n_files = sum(len(v) for v in discovered.values())
    first_file = next(iter(discovered.values()))[0]
    with first_file.open("r", newline="", encoding="utf-8") as fh:
        ncols = len(next(csv.reader(fh)))
    header = _header(pc, ncols)
    out_path = pc.output_path()

    LOG.info(
        "panel: %d symbols, %d files, %s..%s -> %s",
        len(discovered), n_files,
        pc.start_month or "earliest", pc.end_month or "latest", out_path,
    )

    if dry_run:
        for sym, files in discovered.items():
            print(
                f"{sym:16} {len(files):4} files  "
                f"{vision.month_of(files[0].name)}..{vision.month_of(files[-1].name)}"
            )
        print(f"\nheader: {','.join(header)}")
        print(f"-> {out_path}  (not written, dry-run)")
        return PanelResult(len(discovered), n_files, 0, out_path, skipped)

    tcols = _TIME_COLS.get(pc.data_type, (0,))
    streams = [
        _symbol_rows(files, sym, pc.timestamp, tcols)
        for sym, files in discovered.items()
    ]
    key = _sort_key(pc.timestamp)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_path.with_suffix(out_path.suffix + ".part")
    rows = 0
    with tmp.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        if pc.write_header:
            writer.writerow(header)
        for row in heapq.merge(*streams, key=key):
            writer.writerow(row)
            rows += 1
    tmp.replace(out_path)

    LOG.info("panel: wrote %d rows -> %s", rows, out_path)
    return PanelResult(len(discovered), n_files, rows, out_path, skipped)
