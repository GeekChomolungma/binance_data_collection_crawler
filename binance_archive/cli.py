"""``python -m binance_archive`` command-line interface."""

from __future__ import annotations

import argparse
import logging
import sys

from .config import Config
from .crawler import crawl
from .panel import build_panel
from . import symbols as symbols_mod
from . import vision


def _add_common(p: argparse.ArgumentParser) -> None:
    p.add_argument("--config", default="config/system.yaml", help="path to system.yaml")
    p.add_argument("-v", "--verbose", action="store_true", help="debug logging")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="binance_archive", description=__doc__)
    sub = parser.add_subparsers(dest="cmd", required=True)

    sp = sub.add_parser("sync-symbols", help="refresh the symbols file from exchangeInfo")
    _add_common(sp)
    sp.add_argument("--dry-run", action="store_true", help="print symbols, do not write the file")

    cp = sub.add_parser("crawl", help="download + extract archives for every symbol")
    _add_common(cp)
    cp.add_argument("--symbols", help="comma-separated override, e.g. BTCUSDT,ETHUSDT")
    cp.add_argument("--start", help="override start_month (YYYY-MM)")
    cp.add_argument("--end", help="override end_month (YYYY-MM)")
    cp.add_argument("--limit", type=int, help="only the N most recent months per symbol")
    cp.add_argument("--dry-run", action="store_true", help="list what would be fetched")

    lp = sub.add_parser("list-remote", help="show archives available for one symbol")
    _add_common(lp)
    lp.add_argument("symbol")

    pp = sub.add_parser("panel", help="stitch downloaded CSVs into one long panel CSV")
    _add_common(pp)
    pp.add_argument("--symbols", help="comma-separated override, e.g. BTCUSDT,ETHUSDT")
    pp.add_argument("--start", help="override panel.start_month (YYYY-MM)")
    pp.add_argument("--end", help="override panel.end_month (YYYY-MM)")
    pp.add_argument("--dry-run", action="store_true", help="list inputs + output path, write nothing")

    return parser


def _load_cfg(args: argparse.Namespace) -> Config:
    cfg = Config.load(args.config)
    # --start/--end mean the crawl range here; the panel command applies its own
    if args.cmd == "crawl":
        if getattr(args, "start", None):
            cfg.start_month = args.start
        if getattr(args, "end", None):
            cfg.end_month = args.end
        cfg.validate()
    return cfg


def main(argv: list[str] | None = None) -> int:
    # some symbols carry non-ASCII names (e.g. 币安人生USDT); keep console output sane
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="backslashreplace")
        except (AttributeError, ValueError):
            pass

    args = _build_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(message)s",
        datefmt="%H:%M:%S",
    )

    cfg = _load_cfg(args)

    if args.cmd == "sync-symbols":
        found = symbols_mod.fetch_active_symbols(cfg)
        if args.dry_run:
            print("\n".join(found))
            print(f"\n{len(found)} symbols (dry-run, not written)")
            return 0
        symbols_mod.write_symbols_file(cfg.symbols_file, found, cfg)
        print(f"wrote {len(found)} symbols -> {cfg.symbols_file}")
        return 0

    if args.cmd == "list-remote":
        import requests

        sess = requests.Session()
        files = vision.available_files(sess, cfg, args.symbol.upper())
        for f in files:
            print(f"{f.month}  {vision.download_url(f.key)}")
        print(f"\n{len(files)} archive(s)")
        return 0

    if args.cmd == "crawl":
        if args.symbols:
            syms = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
        else:
            syms = symbols_mod.read_symbols_file(cfg.symbols_file)
        result = crawl(cfg, syms, limit=args.limit, dry_run=args.dry_run)
        print(f"\ndone: {result}")
        if result.errors:
            print(f"{len(result.errors)} error(s):", file=sys.stderr)
            for e in result.errors[:20]:
                print(f"  - {e}", file=sys.stderr)
            return 1
        return 0

    if args.cmd == "panel":
        syms = None
        if args.symbols:
            syms = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
        try:
            result = build_panel(cfg, syms, start=args.start, end=args.end, dry_run=args.dry_run)
        except (RuntimeError, FileNotFoundError) as exc:
            print(f"panel: {exc}", file=sys.stderr)
            return 1
        print(f"\ndone: {result}")
        if result.skipped:
            print(
                f"{len(result.skipped)} symbol(s) skipped (no local CSV): "
                + ", ".join(result.skipped),
                file=sys.stderr,
            )
        return 0

    return 2


if __name__ == "__main__":
    raise SystemExit(main())
