# Changelog

All notable changes to the **Stockodile** project will be documented in this file. This project follows [Semantic Versioning](https://semver.org/).

---

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
