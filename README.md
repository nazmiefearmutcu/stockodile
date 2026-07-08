# Stockodile 🐊

Stockodile is an open-source, self-hosted US-equity market data engine designed to ingest, normalize, store, and query market data from **exclusively free sources ($0 budget)**. It is the US-equity sibling of `Crypcodile`, mirroring its robust pipeline: msgspec record decoding, Polars transformations, hive-partitioned Zstd compressed Parquet lakes, and DuckDB SQL catalog view scanning.

Additionally, Stockodile features **aggressive evasion utilities** (proxy rotation, API key pools, cookies spoofing) to safely bypass rate limits and scrape premium indicators without credit cards.

---

## 🚀 Key Features

### 1. Robust Free Providers ($0 API Cost)
* **Google Finance Scraper**: Scrapes real-time stock/index price updates and key-value metrics (ratios) without tokens.
* **MSN Money Scraper**: Scrapes historical EOD/intraday chart data and ex-dividend/split actions via Chart/QuoteSummary APIs.
* **SEC EDGAR**: Fetches index filings (10-K, 10-Q, 8-K, etc.), parses company facts, and extracts XBRL statement lines with support for nightly bulk ZIP files.
* **Stooq Scraper**: Automates visual CAPTCHA solver APIs and JavaScript SHA-256 Proof-of-Work (PoW) cookie verification to download historical CSV and bulk world/US EOD ZIP archives.
* **Yahoo Finance**: Fetches options chains, financials, deep EOD history, intraday bars, and insider trades, wrapped in thread pool executors with crumb/cookie refreshing.
* **Tiingo Basic**: Fetches EOD history with inline corporate actions, intraday IEX bars, and registers symbols using a monthly quota cap.
* **Alpaca Basic & Finnhub WS**: Streams live trade/quotes (IEX feed only) via WebSockets under connection/symbol limits.
* **OpenFIGI Mapper**: Batch-maps tickers to FIGI, exchange, and asset type codes with persistent SQLite caching.

### 2. Aggressive Bypass & Rate Limiting
* **Proxy Rotator**: Automatically rotates a pool of HTTP/HTTPS proxies upon timeouts or connection errors to evade IP bans.
* **API Key Pooler**: Rotates a list of multiple free keys per provider, switching keys dynamically when throttled (handling 429s with exponential backoffs) or exhausted.
* **Token-Bucket Limiter**: Asynchronously acquires tokens and suspends tasks in a fair queue format to respect endpoint caps.

### 3. Ingestion, Storage & Replay
* **Hive Parquet Sink**: Buffers records by channel and partitions them cleanly to disk:
  `provider={P}/channel={C}/date=YYYY-MM-DD/bucket={B}/part-*.parquet`
* **DuckDB View Catalog**: Lazily registers SQL views over Parquet folders on disk, utilizing UTC date-based directory partition pruning for lightning-fast queries.
* **Gap Sync & DLQ**: Synchronizes book updates, handles unparseable frames, and detects trade sequence gaps.
* **Replay K-Way Merge**: Deterministically merges multiple files chronologically based on timestamps.

### 4. Scheduler & Calendar
* **Scheduled Pulls**: Dynamically schedules low-frequency daily, weekly, or bi-monthly pulls (SEC EDGAR, FINRA, Stooq) with period catch-up logic and persistent execution states.
* **US Market Calendar**: Tracks NYSE/Nasdaq holidays, holiday observation rules, and standard/early trading hours.

### 5. Analytics & Resampling
* **Quantitative Metrics**: Calculates log/simple returns, beta, realized volatility, and basic financial ratios (P/E, P/B, ROE, margins).
* **Options Analytics**: Computes continuous-dividend Black-Scholes-Merton pricing, Greeks (Delta, Gamma, Vega, Theta, Rho), and fits Implied Volatility using a hybrid Newton-Raphson/Bisection solver.
* **OHLCV & Book Resampler**: Resamples trades, quotes, and L2 streams into higher resolution bars (1s, 1m, 1h, 1d) or book snapshot depths.

---

## 📁 Package Layout

```
src/stockodile/
├── schema/          # Canonical Records + StrEnums
├── store/           # Flat Row Conversion, Parquet Sink, DuckDB Catalog
├── providers/       # Connectors (sec_edgar, yahoo, stooq, openfigi, etc.)
├── ingest/          # WS Transport, DLQ, Gap Sync Resync Bridge
├── ratelimit/       # TokenBucket, ProxyRotator, ApiKeyPool
├── scheduler/       # ScheduledPullCoordinator, USMarketCalendar, StateStores
├── reference/       # Security Master Database (SQLite)
├── corpactions/     # Splits/Dividends CRSP Cumulative Factors
├── resample/        # OHLCV Resampling & L2 Book Snapshots
├── analytics/       # Volatility, Beta, BSM Option Pricing, Financial Ratios
├── client/          # StockodileClient, Collect Orchestrator, CSV/JSON Export
└── cli.py           # Typer application entrypoint
```

---

## 🔧 Installation & Usage

Stockodile is managed using **uv** for fast package installation:

```bash
# Clone the repository
git clone https://github.com/nazmiefearmutcu/stockodile.git
cd stockodile

# Synchronize python dependencies and setup virtual environment
uv sync
```

### Running the CLI

```bash
# Run collect to stream live WS feeds
uv run stockodile collect --symbols AAPL,MSFT --channels trade,quote --provider alpaca

# Execute DuckDB SQL queries against the catalog
uv run stockodile query "SELECT date, open, close FROM bar WHERE symbol = 'AAPL' ORDER BY date"

# Export normalized datasets to CSV or JSON
uv run stockodile export --channel bar --symbol AAPL --format csv --output aapl.csv

# Resample ticks to a 5-minute interval
uv run stockodile resample --symbol AAPL --interval 5m
```

---

## 🧪 Testing & Development

We enforce a strict coding standard: **mypy strict mode** type safety and **ruff** linting.

# Run the complete test suite
uv run pytest
```

### 🍏 macOS Apple Silicon & Headless Runs Optimization
To ensure reliable operation on modern macOS Apple Silicon devices:
* **Apple Silicon OpenMP/OpenBLAS Thread Limits**: Programmatic thread limit overrides (`OMP_NUM_THREADS=1`, `OPENBLAS_NUM_THREADS=1`, etc.) are configured globally in [tests/conftest.py](file:///Users/nazmi/Desktop/Stockodile/tests/conftest.py) to prevent process thread-group lockups.
* **Global CLI Path Resolution**: The CLI shebang and library search paths are configured to automatically resolve package routes, enabling instant execution of the global `stockodile` command line.

```bash
# Check code formatting & linting
uv run ruff check src tests

# Check strict type safety
uv run mypy src tests

```

---

## 📄 License

Stockodile is released under the **Apache-2.0** License. See [LICENSE](LICENSE) for details.
