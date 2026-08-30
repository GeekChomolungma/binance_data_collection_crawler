"""Orchestrate a full crawl: symbols file + system config -> extracted CSVs."""

from __future__ import annotations

import logging
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field

import requests

from .config import Config
from . import vision

LOG = logging.getLogger(__name__)


@dataclass
class CrawlResult:
    status: Counter = field(default_factory=Counter)
    errors: list[str] = field(default_factory=list)

    def add(self, status: str) -> None:
        self.status[status] += 1

    def __str__(self) -> str:
        parts = ", ".join(f"{k}={v}" for k, v in sorted(self.status.items()))
        return parts or "nothing to do"


def _make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": "binance-archive-crawler/0.1 (+https://data.binance.vision)"})
    return s


def crawl(
    cfg: Config,
    symbols: list[str],
    *,
    limit: int | None = None,
    dry_run: bool = False,
) -> CrawlResult:
    result = CrawlResult()
    session = _make_session()

    # keep only pairs quoted in the configured asset (guard against a hand-edited file)
    wanted = [s for s in symbols if s.endswith(cfg.quote_asset)]
    dropped = sorted(set(symbols) - set(wanted))
    if dropped:
        LOG.warning("ignoring %d non-%s symbol(s): %s", len(dropped), cfg.quote_asset, ", ".join(dropped))

    def _work(sym: str, af: vision.ArchiveFile) -> str:
        return vision.download_file(session, cfg, af, vision.local_dir(cfg, sym))

    total = len(wanted)
    # Process one symbol at a time: list it, then immediately download its
    # months, then move on. Files land on disk right away and progress is
    # visible, instead of waiting for every symbol to be listed first.
    with ThreadPoolExecutor(max_workers=cfg.max_workers) as pool:
        for idx, sym in enumerate(wanted, 1):
            try:
                files = vision.available_files(session, cfg, sym)
            except Exception as exc:  # noqa: BLE001 - report and continue
                LOG.error("[%d/%d] %s listing failed: %s", idx, total, sym, exc)
                result.errors.append(f"{sym}: list: {exc}")
                continue
            if limit:
                files = files[-limit:]
            if not files:
                LOG.info("[%d/%d] %-14s no archives in range", idx, total, sym)
                continue
            LOG.info(
                "[%d/%d] %-14s %d archive(s) %s..%s",
                idx, total, sym, len(files), files[0].month, files[-1].month,
            )

            if dry_run:
                for af in files:
                    print(f"{vision.download_url(af.key)}  ->  {vision.local_dir(cfg, sym) / af.csv_name}")
                result.status["planned"] += len(files)
                continue

            futs = {pool.submit(_work, sym, af): af for af in files}
            for fut in as_completed(futs):
                af = futs[fut]
                try:
                    status = fut.result()
                except Exception as exc:  # noqa: BLE001
                    LOG.error("%s %s: %s", sym, af.name, exc)
                    result.errors.append(f"{sym} {af.name}: {exc}")
                    result.add("error")
                    continue
                result.add(status)
                if status == "downloaded":
                    LOG.info("  ok %s", af.name)

    return result
