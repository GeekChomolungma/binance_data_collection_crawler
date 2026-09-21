# into_clickhouse

Import the downloaded Binance USD-M futures **1-minute** klines from `rawdata/`
into ClickHouse (`market.fapi_kline_1m`, deployed by
[deployed/001_fapi_kline.sql](deployed/001_fapi_kline.sql)).

## Column mapping

The archive CSV has 12 positional columns (no stable header — 2025+ months carry
one, older months don't). Its order is **not** the table's order, CSV column 11
(`ignore`) is dropped, `symbol` comes from the file name, and the table's 13th
column `created_at` is filled by `DEFAULT now()`.

| archive CSV | `market.fapi_kline_1m` |
|---|---|
| *(file name)* | `symbol` |
| 0 `open_time` | `start_time` `DateTime64(3,'UTC')` |
| 6 `close_time` | `end_time` `DateTime64(3,'UTC')` |
| 1/2/3/4/5 | `open` / `high` / `low` / `close` / `volume` |
| 7 | `quote_volume` |
| 9 / 10 | `taker_buy_volume` / `taker_buy_quote_volume` |
| 8 `count` | `trades_count` `UInt32` |
| 11 `ignore` | *dropped* |
| — | `created_at` `DEFAULT now()` |

The 12-vs-13 mismatch is handled by sending an **explicit 12-column list** in
`INSERT ... SELECT ... FROM input(...)`; ClickHouse fills `created_at` itself. The
reorder / drop / `ms→DateTime64` cast runs server-side, so Python only strips the
optional header line and streams raw CSV bytes over the HTTP interface. No new
dependency — reuses `requests`.

Re-runnable: target is `ReplacingMergeTree((symbol, start_time))`, and a local
manifest `.imported.log` lets a re-run skip files already loaded (`--reimport` to
ignore it).

## One-shot full import

```powershell
# defaults: url=http://localhost:8123  database=market  user=default  password=""
.\.venv\Scripts\python.exe .\into_clickhouse\import_fapi_kline.py
```

With a non-default server (flag > env var > default):

```powershell
$env:CLICKHOUSE_URL      = "http://<host>:8123"
$env:CLICKHOUSE_DATABASE = "market"
$env:CLICKHOUSE_USER     = "default"
$env:CLICKHOUSE_PASSWORD = "<pw>"
.\.venv\Scripts\python.exe .\into_clickhouse\import_fapi_kline.py
```

Useful flags: `--symbols BTCUSDT,ETHUSDT`, `--start 2024-01 --end 2024-12`,
`--limit 5` (smoke test), `--dry-run -v`, `--reimport`.

## Open interest (5m metrics) -> `market.fapi_oi_5m`

```powershell
# 1. download daily metrics for every USDT perp (config/system_metrics.yaml)
.\.venv\Scripts\python.exe -m binance_archive crawl --config config/system_metrics.yaml
# 2. load (table: deployed/004_fapi_oi.sql; same CLICKHOUSE_* env vars as above)
.\.venv\Scripts\python.exe .\into_clickhouse\import_fapi_oi.py
```

`create_time` in the archive is **UTC** and is the *close* of the 5m bar, so
`start_time = create_time - 5 min` (the midnight row of day D lands on 23:55 of D-1;
rows inside a file are unsorted). Written with `src_rank=3`, so it overrides live/hist rows.
Details in the [import_fapi_oi.py](import_fapi_oi.py) docstring.

### Why `create_time` is UTC and marks the bar close (how it was verified)

Binance's docs and the `binance-public-data` repo say nothing about the timezone or
meaning of `create_time` in `futures/um/*/metrics`, so both facts were checked
empirically (2026-09-20, BTCUSDT, file `BTCUSDT-metrics-2026-09-18.csv`, 288 rows):

**1. Timezone = UTC.** The REST endpoint `/futures/data/openInterestHist?period=5m`
returns epoch-ms timestamps (always UTC). Each archive `create_time` was parsed as if
it were UTC+k for k in {-8, -5, -1, 0, +1, +8}, converted to epoch ms and matched
against the API row with the same timestamp. Median relative difference of
`sum_open_interest`:

| assumed offset | matched rows | median rel. diff |
|---|---|---|
| **0 (UTC)** | 211 | **0.04%** |
| ±1h | 223 / 199 | 0.25% / 0.29% |
| -5h / ±8h | 271 / 288 / 115 | 0.85% / 0.93% / 1.43% |

Only UTC lines the two sources up (the residual 0.04% is the two feeds not being
snapshotted at the exact same instant; an exact match is not expected).
Note: `openInterestHist` only serves the latest ~30 days, so re-run this check on a
recent day.

**2. `create_time` = close of the 5m bar.** Archive rows cover `00:00:00 ... 23:55:00`
(288 = 24h x 12, no `24:00`), and the value labelled `T` is the same one
`openInterestHist` labels `T` — the snapshot taken when the bar `[T-5m, T)` closes.
So the bar's open time is `T - 5m`, and the `00:00:00` row belongs to `23:55` of the
previous day. The last bar of day D therefore arrives in day D+1's file.

To re-check quickly: download one recent day, then compare its `create_time`s (parsed
as UTC) with `openInterestHist` timestamps for the same `symbol`; the values should be
within ~0.1% row by row. Also note rows inside a file are **not** sorted by time.

### Connecting to ClickHouse (password) and first run

Priority: command-line flag > environment variable > default.

| flag | env var | default |
|---|---|---|
| `--url` | `CLICKHOUSE_URL` | `http://localhost:8123` |
| `--database` | `CLICKHOUSE_DATABASE` | `market` |
| `--user` | `CLICKHOUSE_USER` | `default` |
| `--password` | `CLICKHOUSE_PASSWORD` | *(empty)* |

Prefer the env var (keeps the password out of the shell history; it lives only in the
current PowerShell window):

```powershell
$env:CLICKHOUSE_PASSWORD = "<pw>"
.\.venv\Scripts\python.exe .\into_clickhouse\import_fapi_oi.py
```

or pass it inline: `... import_fapi_oi.py --user default --password "<pw>"`.

Create the table first (idempotent; the file has two statements, so use
`clickhouse-client --multiquery`, the HTTP interface accepts only one per request):

```powershell
clickhouse-client --password "<pw>" --multiquery < into_clickhouse\deployed\004_fapi_oi.sql
```

Smoke test, then check the result (the earliest `start_time` should be `23:55` of the
day before the first file's date, because `create_time` 00:00 -> `start_time` 23:55):

```powershell
.\.venv\Scripts\python.exe .\into_clickhouse\import_fapi_oi.py --symbols BTCUSDT --limit 3
clickhouse-client --password "<pw>" -q "SELECT symbol, min(start_time), max(start_time), count() FROM market.fapi_oi_5m GROUP BY symbol"
```

Then run without `--symbols/--limit` for the full load. Files are sent in per-symbol
batches of 100; a re-run skips files already listed in `.imported.log`
(`--reimport` to ignore it). Other flags: `--start/--end YYYY-MM`, `--dry-run -v`.
