# ThetaData Options Pipeline

[![CI](https://github.com/frankliuao/thetadata-options-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/frankliuao/thetadata-options-pipeline/actions)
[![Python Version](https://img.shields.io/badge/python-3.12%2B-blue.svg)](https://www.python.org/)
[![Storage](https://img.shields.io/badge/storage-Apache%20Parquet%20(ZSTD)-orange.svg)](https://parquet.apache.org/)
[![Engine](https://img.shields.io/badge/engine-Polars-teal.svg)](https://pola.rs/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A production-grade, fault-tolerant Python ETL pipeline for downloading, normalizing, and archiving multi-year US options chains, Black-Scholes/Binomial Greeks, and Open Interest from **ThetaData v3** into partitioned, high-compression **Apache Parquet** lakehouses with automated Google Drive sync.

Designed specifically for quantitative researchers, algorithmic traders, and financial engineers who require clean, standardized, point-in-time options data for backtesting strategies (cash-secured puts, covered calls, dispersion, volatility trading) without suffering from vendor SDK timeouts, silent data dropouts, or unscaled Greeks.

---

## 🌟 Key Highlights & Engineering Features

```
┌─────────────────┐       ┌──────────────────────┐       ┌────────────────────────┐
│  ThetaData v3   │ ────> │  Polars Normalizer   │ ────> │  Atomic Parquet Lake   │
│  gRPC / REST    │       │  & Math Scaler       │       │  (ZSTD / Partitioned)  │
└─────────────────┘       └──────────────────────┘       └────────────────────────┘
         │                           │                                │
  Auto-Reauth Lock           Greeks Standardization          Optional Cloud Sync
 (15m Token Refresh)        (Vega/Rho 1%, Prior OI)          (Google Drive Mutex)
```

- **⚡ Zero-Drop Session Persistence**: Proactive 8-minute token renewal and thread-safe dynamic re-authentication completely bypasses ThetaData's ~15-minute gRPC session token TTL, enabling seamless multi-day backfills without `UNAUTHENTICATED` disconnections.
- **📐 Quantitative Finance Greeks Standardization**: Continuous Black-Scholes/Binomial Vega & Rho derivatives ($\partial V / \partial \sigma$ and $\partial V / \partial r$) are scaled to standard financial market convention (sensitivity per 1 percentage point move) via `vega_per_pct` and `rho_per_pct`, while preserving raw vendor values.
- **🏷️ Dual-Class OCC Symbology Translation**: Automatically converts dual-class equity tickers to official OCC option roots (e.g. `BRK.B` → `BRKB`, `BF.B` → `BFB`) across all options endpoints, preventing missing chains on S&P 500 constituents.
- **📊 Non-Lookahead Open Interest Alignment**: Daily OPRA open interest prints (published at ~06:30 ET) are joined to the active trade session and explicitly documented as `open_interest_prior_close` to guarantee point-in-time backtest integrity.
- **🛡️ Null-Preserving Deduplication & Atomic Writes**: Stacking daily chains into yearly archives uses `group_by.drop_nulls().last()`, ensuring partial raw updates never erase existing open interest. Writes use `.parquet.tmp` atomic renaming to eliminate corrupted files.
- **🔄 Fault-Tolerant Catch-up Updater (`update-options`)**: Automatically detects the latest downloaded trading day per symbol, iterates missing sessions across symbols in parallel, and immediately halts on failed days so subsequent runs resume cleanly without historical holes.
- **☁️ Cloud Backup & Mutex Sync**: Integrates with the Google Drive CLI (`gdrive`) with full mutex lock protection to ensure multi-threaded jobs never race on folder creation or in-place file updates.

---

## 📦 Installation

### Prerequisites
- Python **3.12+** (Tested on Python 3.12, 3.13)
- [Poetry](https://python-poetry.org/) (recommended) or `pip`
- A valid [ThetaData](https://thetadata.net/) subscription (**Standard** or **Pro** tier required for historical Greeks and Open Interest)

### Install via Poetry

```bash
git clone https://github.com/frankliuao/thetadata-options-pipeline.git
cd thetadata-options-pipeline

# Install dependencies in virtualenv
poetry install
```

### Install via Pip

```bash
pip install -e .
```

---

## ⚙️ Configuration

Copy the example environment file and add your ThetaData API key:

```bash
cp .env.example .env
```

Edit `.env`:

```ini
# ThetaData v3 API Key (Obtain from https://thetadata.net user portal)
THETADATA_API_KEY="your_thetadata_api_key_here"

# Default concurrency limit (Standard Tier = 4, Pro Tier = 8, Value Tier = 2)
THETADATA_MAX_WORKERS=4

# Optional: Google Drive folder ID for automated cloud backup (leave blank to disable cloud sync)
GDRIVE_FOLDER_ID=""

# Optional: Google Sheet ID if refreshing universe from Google Sheets
SP500_UNIVERSE_SHEET_ID=""
```

---

## 🚀 Quickstart & Usage

### 1. Offline Mock Verification
Verify your Polars environment and Parquet round-trip without making any live network calls or using API credentials:

```bash
poetry run download-options --mock-test
```

### 2. Download Historical Options for a Symbol

Download 2016–present EOD options + Greeks + Open Interest for SPY:

```bash
poetry run download-options --symbols SPY --start-date 2020-01-01 --end-date 2024-12-31 --workers 4
```

### 3. Download the S&P 500 Universe

The repository bundles a curated, actively trading universe of 575 S&P 500 constituents (`config/sp500_universe.csv`):

```bash
# Filter options within 50 strikes of ATM and 150 DTE to optimize storage
poetry run download-options \
  --universe active \
  --start-date 2016-01-01 \
  --strike-range 50 \
  --max-dte 150 \
  --workers 4
```

### 4. Incremental Catch-up / Daily Updating (`update-options`)

Run daily after market close (17:15 America/New_York) to automatically detect missing days per symbol and update your archives:

```bash
poetry run update-options --workers 4
```

---

## 🗄️ Storage Layout

All data is stored in partitioned, Zstandard-compressed Parquet files:

```
data/
├── raw/
│   └── options_eod/
│       └── symbol=AAPL/
│           ├── year=2024/
│           │   ├── AAPL_2024-01-02.parquet
│           │   └── AAPL_2024-01-03.parquet
│           └── year=2025/
│               └── ...
└── processed/
    └── options_eod/
        └── symbol=AAPL/
            ├── AAPL_2023.parquet
            ├── AAPL_2024.parquet
            └── AAPL_2025.parquet
```

- **`data/raw/`**: Atomic daily downloads partitioned by `symbol=<SYM>/year=<YEAR>/`.
- **`data/processed/`**: Consolidated yearly files deduplicated on `(trade_date, expiration, strike, right)` with null-preserving aggregation.

---

## 📊 Data Schema & Dictionary

Each option record contains 40+ standardized attributes:

| Column | Type | Description |
| :--- | :--- | :--- |
| `trade_date` | `Date` | Trade session date (`America/New_York`). |
| `symbol` | `Utf8` | Underlying equity ticker (e.g. `AAPL`, `BRK.B`). |
| `expiration` | `Date` | Option expiration date. |
| `dte` | `Int32` | Days to expiration: `(expiration - trade_date).days`. |
| `strike` | `Float64` | Contract strike price in dollars. |
| `right` | `Utf8` | Contract type (`call` or `put`). |
| `underlying_price`| `Float64` | Underlying asset closing price on `trade_date`. |
| `open`, `high`, `low`, `close` | `Float64` | Option transaction prices for the session. |
| `bid`, `ask`, `mid`, `spread` | `Float64` | Closing NBBO quotes and derived midpoint/spread. |
| `volume`, `count` | `Int64` | Contract volume and transaction count. |
| `open_interest` | `Int64` | Total open contracts. |
| `open_interest_prior_close` | `Int64` | Prior session's OPRA open interest known on `trade_date`. |
| `implied_vol` | `Float64` | Black-Scholes implied volatility (annualized). |
| `delta` | `Float64` | Option delta ($\partial V / \partial S$). |
| `gamma` | `Float64` | Option gamma ($\partial^2 V / \partial S^2$). |
| `theta` | `Float64` | Option 1-day theta ($\partial V / \partial t$). |
| `vega` | `Float64` | Raw mathematical derivative ($\partial V / \partial \sigma$). |
| `vega_per_pct` | `Float64` | **Standardized Vega**: dollar change per 1% move in IV (`vega / 100`). |
| `rho` | `Float64` | Raw mathematical derivative ($\partial V / \partial r$). |
| `rho_per_pct` | `Float64` | **Standardized Rho**: dollar change per 1% move in interest rate (`rho / 100`). |

---

## 💻 Python Querying API

### Query Chains with Polars

```python
import polars as pl
from options_data.loader import OptionsDataLoader

loader = OptionsDataLoader(data_dir="data")

# Query 30-45 DTE Put options for cash-secured put strategies
chain = loader.query_chain(
    symbol="AAPL",
    trade_date="2024-06-03",
    right="put",
    min_dte=30,
    max_dte=45,
    min_delta=-0.35,
    max_delta=-0.15,
)

print(chain.select([
    "trade_date", "expiration", "dte", "strike", "underlying_price",
    "bid", "ask", "mid", "delta", "vega_per_pct", "open_interest_prior_close"
]))
```

### Querying Directly with DuckDB / SQL

```python
import duckdb

con = duckdb.connect()
df = con.execute("""
    SELECT trade_date, strike, right, mid, delta, vega_per_pct, open_interest_prior_close
    FROM 'data/processed/options_eod/symbol=AAPL/*.parquet'
    WHERE trade_date = '2024-01-10'
      AND dte BETWEEN 25 AND 60
      AND strike BETWEEN 170 AND 190
    ORDER BY strike, right
""").pl()

print(df)
```

---

## 🧪 Testing

The test suite includes 17 offline unit tests covering schema casting, mathematical scaling, multi-threaded catch-up, and error recovery without requiring ThetaData credentials:

```bash
poetry run python -m unittest discover -s tests -v
```

---

## 📄 License

This project is licensed under the [MIT License](LICENSE).

---

## ⚠️ Disclaimer

*This toolkit is for research and educational purposes only. Historical options data and Greeks calculations are provided by ThetaData. Past performance is no guarantee of future trading results. This software is not affiliated with or endorsed by ThetaData LLC or the Options Clearing Corporation (OCC).*
