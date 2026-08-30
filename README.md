# binance_data_collection_crawler

A small, dependency-light crawler for the **Binance public data archive**
(<https://data.binance.vision>).

It figures out which trading pairs are *currently live*, asks the archive which
monthly (or daily) files actually exist for each one, then downloads, verifies
and unzips them into a local folder tree that mirrors the archive URL layout.

```text
remote  https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/4h/BTCUSDT-4h-2026-07.zip
local                           rawdata/spot/monthly/klines/btcusdt/4h/BTCUSDT-4h-2026-07.csv
```

---

## What it does

| Step | Detail |
|---|---|
| **1. Discover symbols** | Calls the public `exchangeInfo` REST endpoint for the chosen market and keeps only pairs that are `TRADING` and quoted in `quote_asset` (e.g. `USDT`). Writes them to `config/symbols.txt`. |
| **2. Discover files** | For each symbol, queries the archive's S3 `ListBucket` XML API to get the real list of available `.zip` files — no brute-force date guessing. |
| **3. Download & archive** | Downloads each `.zip`, verifies its `sha256` against the published `.CHECKSUM` sidecar, extracts the CSV, and drops it under `rawdata/…` following the same path structure as the URL. |

### Design notes

- **USDT-only (or any quote asset)** — driven by `quote_asset` in the config, not a hard-coded list. A second guard in the crawler ignores any non-matching line if you hand-edit the symbols file.
- **Delisted coins are skipped for free** — the symbol list comes from `exchangeInfo`, so pairs that no longer trade are never requested. If a listed month happens to have no `.zip`, it is logged as `missing` and skipped rather than failing the run.
- **New listings are picked up automatically** — just re-run `sync-symbols`.
- **Idempotent & resumable** — a month whose `.csv` already exists on disk is reported as `skipped`. Downloads are written to a `.part` file and renamed on completion, so an interrupted run leaves no corrupt files; just run `crawl` again to fill the gaps.
- **Integrity checked** — every archive is `sha256`-verified before it is extracted (toggle with `verify_checksum`).

---

## Requirements

- Python 3.10+
- `requests`, `PyYAML` (that's it — see [requirements.txt](requirements.txt))

## Install

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

(On macOS / Linux use `./.venv/bin/python` instead of `.\.venv\Scripts\python.exe`.)

---

## Configure

Everything lives in [config/system.yaml](config/system.yaml):

| Key | Meaning |
|---|---|
| `market` | `spot` \| `um` (USDⓈ-M futures) \| `cm` (COIN-M futures) |
| `data_frequency` | `monthly` \| `daily` |
| `data_type` | `klines`, `trades`, `aggTrades`, `bookTicker`, … |
| `interval` | e.g. `4h`, `1h`, `1d` — only used for the `*klines` data types |
| `quote_asset` | `USDT` for spot / `um`; `USD` for `cm` (COIN-M is USD-quoted, coin-margined, symbols like `BTCUSD_PERP`) |
| `start_month` / `end_month` | optional `YYYY-MM` bounds, inclusive (`null` = no bound) |
| `output_dir` | where the extracted CSVs go (default `./rawdata`) |
| `symbols_file` | path to the generated symbol list (default `./config/symbols.txt`) |
| `max_workers` | concurrent downloads per symbol (default `6`) |
| `skip_existing` | skip a month when its `.csv` is already present (default `true`) |
| `verify_checksum` | download the `.CHECKSUM` sidecar and verify `sha256` (default `true`) |
| `strict_checksum` | fail instead of warn when a `.CHECKSUM` is missing (default `false`) |
| `keep_zip` | keep the `.zip` next to the extracted `.csv` (default `false`) |
| `base_api_url` | full override for the `exchangeInfo` URL (regional mirror / testnet) |

---

## Usage

All commands are subcommands of `python -m binance_archive`.

### 1. Refresh the symbol list

```powershell
.\.venv\Scripts\python.exe -m binance_archive sync-symbols
```

Writes `config/symbols.txt` with every live `<quote_asset>`-quoted pair for the
configured `market`. Add `--dry-run` to print them without writing.

### 2. Crawl

```powershell
.\.venv\Scripts\python.exe -m binance_archive crawl
```

Reads `config/symbols.txt` + `config/system.yaml`, then downloads and extracts
everything. Prints a summary like `done: downloaded=812, skipped=1043, missing=4`.

| Flag | Effect |
|---|---|
| `--symbols BTCUSDT,ETHUSDT` | crawl just these, ignore the symbols file |
| `--start 2024-01` / `--end 2024-12` | override the month range for this run |
| `--limit N` | only the `N` most recent months per symbol |
| `--dry-run` | list the URLs that would be fetched, download nothing |
| `-v` | debug logging |
| `--config path/to.yaml` | use a different config file |

### 3. Inspect one symbol

```powershell
.\.venv\Scripts\python.exe -m binance_archive list-remote BTCUSDT
```

Prints every archive file available for that symbol and exits.

---

## Examples

```powershell
# One-off: last 3 months of BTC and ETH 4h klines, just show what would happen
.\.venv\Scripts\python.exe -m binance_archive crawl --symbols BTCUSDT,ETHUSDT --limit 3 --dry-run

# Backfill a specific year for every USDT pair
.\.venv\Scripts\python.exe -m binance_archive crawl --start 2024-01 --end 2024-12

# Switch to USDⓈ-M perpetual futures: edit system.yaml (market: um), then
.\.venv\Scripts\python.exe -m binance_archive sync-symbols
.\.venv\Scripts\python.exe -m binance_archive crawl
```

---

## Output layout

```text
rawdata/
└── spot/monthly/klines/
    ├── btcusdt/4h/
    │   ├── BTCUSDT-4h-2017-08.csv
    │   ├── BTCUSDT-4h-2017-09.csv
    │   └── …
    └── ethusdt/4h/
        └── …
```

The path is `output_dir / <market> / <frequency> / <data_type> / <symbol> / [<interval>]`,
matching the archive URL (symbol folder lower-cased). The CSV columns are exactly
as Binance publishes them (open time, open, high, low, close, volume, close time,
quote volume, trade count, taker buy base, taker buy quote, ignore).

---

## Project layout

```text
binance_archive/
  config.py    load + validate config/system.yaml
  symbols.py   exchangeInfo REST -> live symbol list (spot / um / cm)
  vision.py    list files / build URLs / download / sha256-verify / safe-unzip
  crawler.py   orchestrate the crawl with a thread pool, one symbol at a time
  cli.py       argparse entry point (python -m binance_archive ...)
config/
  system.yaml  the knobs above
  symbols.txt  generated by `sync-symbols`
```
