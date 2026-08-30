"""Load and validate the crawler configuration (``config/system.yaml``)."""

from __future__ import annotations

import dataclasses
import re
from dataclasses import dataclass
from pathlib import Path

import yaml

# data_type values that live under a ``<symbol>/<interval>/`` folder on the archive
KLINE_TYPES = {
    "klines",
    "indexPriceKlines",
    "markPriceKlines",
    "premiumIndexKlines",
}
# every data_type the archive currently exposes (kept permissive on purpose)
DATA_TYPES = KLINE_TYPES | {
    "trades",
    "aggTrades",
    "bookTicker",
    "bookDepth",
    "metrics",
    "fundingRate",
    "liquidationSnapshot",
}
MARKETS = {"spot", "um", "cm"}
FREQUENCIES = {"monthly", "daily"}

_MONTH_RE = re.compile(r"^\d{4}-\d{2}$")


@dataclass
class Config:
    market: str = "spot"
    data_frequency: str = "monthly"
    data_type: str = "klines"
    interval: str = "4h"
    quote_asset: str = "USDT"
    output_dir: str = "./rawdata"
    symbols_file: str = "./config/symbols.txt"
    start_month: str | None = None
    end_month: str | None = None
    max_workers: int = 6
    skip_existing: bool = True
    verify_checksum: bool = True
    strict_checksum: bool = False
    keep_zip: bool = False
    base_api_url: str | None = None

    # ── derived helpers ────────────────────────────────────────────────────
    @property
    def is_kline(self) -> bool:
        return self.data_type in KLINE_TYPES

    @property
    def market_path(self) -> str:
        """URL/archive path fragment for the selected market."""
        return {"spot": "spot", "um": "futures/um", "cm": "futures/cm"}[self.market]

    # ── loading / validation ──────────────────────────────────────────────
    @classmethod
    def load(cls, path: str | Path) -> "Config":
        raw = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
        if not isinstance(raw, dict):
            raise ValueError(f"{path}: top-level YAML must be a mapping")
        known = {f.name for f in dataclasses.fields(cls)}
        unknown = set(raw) - known
        if unknown:
            raise ValueError(f"{path}: unknown config keys: {sorted(unknown)}")
        cfg = cls(**raw)
        cfg.validate()
        return cfg

    def validate(self) -> None:
        if self.market not in MARKETS:
            raise ValueError(f"market must be one of {sorted(MARKETS)}")
        if self.data_frequency not in FREQUENCIES:
            raise ValueError(f"data_frequency must be one of {sorted(FREQUENCIES)}")
        if self.data_type not in DATA_TYPES:
            raise ValueError(f"data_type must be one of {sorted(DATA_TYPES)}")
        if self.is_kline and not self.interval:
            raise ValueError(f"interval is required when data_type={self.data_type!r}")
        if not self.quote_asset:
            raise ValueError("quote_asset must not be empty")
        for name in ("start_month", "end_month"):
            val = getattr(self, name)
            if val is not None and not _MONTH_RE.match(str(val)):
                raise ValueError(f"{name} must be null or 'YYYY-MM', got {val!r}")
        if (
            self.start_month
            and self.end_month
            and self.start_month > self.end_month
        ):
            raise ValueError("start_month must be <= end_month")
        if self.max_workers < 1:
            raise ValueError("max_workers must be >= 1")

        self.quote_asset = self.quote_asset.upper()
