# Changelog

All notable changes to the **Stockodile** project will be documented in this file. This project follows [Semantic Versioning](https://semver.org/).

---

## [0.2.0] - 2026-07-18
### Added
- **Synthetic Market Depth (`depth` command / channel)**: A $0, legal US-equity market-depth capability. Keyless default synthesizes a volume-at-price ladder from free Yahoo 1-minute bars (`basis="yahoo_1m_vap"`, `is_synthetic=True`), clearly labeled as *relative* liquidity rather than real resting orders.
- **Transparent Alpaca L1 upgrade**: With `ALPACA_API_KEY`/`ALPACA_API_SECRET` set, the same `depth` surface upgrades to real top-of-book L1 via Alpaca's official REST latest-quote API (`basis="alpaca_l1"`, `is_synthetic=False`) with no code changes, selected by `stockodile.depth.select_depth_source`.
- **New `DepthProfile` record + `depth` channel** persisted to the Parquet/DuckDB lake (queryable via `SELECT ... FROM depth`).
- **CLI `depth SYMBOL`** command, plus `GET /api/v1/depth/{symbol}` REST endpoint and a `depth` MCP tool for full surface parity.
- **Pure VAP synthesis** (`stockodile.depth.vap`): volume-conserving `uniform`/`typical`/`close` bucketing with full unit-test coverage.

## [0.1.2] - 2026-07-09
### Added
- **Technical Analysis Indicators Engine**: Implemented SMA, EMA, RSI, MACD, and Bollinger Bands using Polars.
- **CLI Subcommand (`indicators` command)**: Added CLI support to compute indicators on historical data.
- **Unit Testing**: Added unit tests under `tests/test_indicators.py` verifying all indicator mathematical calculations.

## [0.1.1] - 2026-07-09
### Fixed
- **Thread Deadlocks**: Resolved Apple Silicon thread deadlocks in market data stream buffers.

## [0.1.0] - 2026-07-09
### Added
- **Initial Release**: Ported the US-equity sibling of Crypcodile.
