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
