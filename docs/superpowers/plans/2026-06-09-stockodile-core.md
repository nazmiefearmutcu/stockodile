# Stockodile Core Implementation Plan

This plan details the implementation steps for Stockodile, the US-equity sibling of Crypcodile.
It is divided into 20 bite-sized tasks, each corresponding to a subagent execution.

## Milestone 1: Core Skeleton & Environment Setup
- **Task 1:** Workspace Initialization. Create `pyproject.toml`, `.gitignore`, `.python-version`, and directories.
  - Subagent Role: Env Architect
- **Task 2:** Schema Enums. Implement `src/stockodile/schema/enums.py`.
  - Subagent Role: Enum Designer
- **Task 3:** Msgspec Record Schemas. Implement `src/stockodile/schema/records.py`.
  - Subagent Role: Record Schema Coder
- **Task 4:** Row Converter. Implement `src/stockodile/store/rows.py`.
  - Subagent Role: Row Serializer Developer
- **Task 5:** Base & Parquet Sink. Implement `src/stockodile/sink/base.py` and `src/stockodile/store/parquet_sink.py`.
  - Subagent Role: Sink Engineer
- **Task 6:** DuckDB Catalog. Implement `src/stockodile/store/catalog.py`.
  - Subagent Role: Catalog Database Coder

## Milestone 2: Reference, Spine & Utilities
- **Task 7:** Provider Base & Transport. Implement `src/stockodile/providers/base.py` and transport layers.
  - Subagent Role: Transport Architect
- **Task 8:** Rate Limiter. Implement token-bucket `src/stockodile/ratelimit/`.
  - Subagent Role: Throttle Engineer
- **Task 9:** Scheduler. Implement low-frequency REST scheduling in `src/stockodile/scheduler/`.
  - Subagent Role: Job Scheduler Developer
- **Task 10:** Corporate Actions. Implement adjustment algorithms under `src/stockodile/corpactions/`.
  - Subagent Role: CorpAction Coder
- **Task 11:** Security Master. Implement security registry in `src/stockodile/reference/`.
  - Subagent Role: Security Master Developer

## Milestone 3: Core Providers
- **Task 12:** OpenFIGI Provider. Implement mapping client in `src/stockodile/providers/openfigi/`.
  - Subagent Role: OpenFIGI Connector Coder
- **Task 13:** Stooq Provider. Implement PoW/CAPTCHA-gated EOD fetcher in `src/stockodile/providers/stooq/`.
  - Subagent Role: Stooq Scraper Coder
- **Task 14:** SEC EDGAR Provider. Implement submissions + company facts indexer in `src/stockodile/providers/sec_edgar/`.
  - Subagent Role: EDGAR XBRL Coder
- **Task 15:** Yahoo Finance Provider. Implement yfinance wrapper in `src/stockodile/providers/yahoo/`.
  - Subagent Role: Yahoo Connector Developer
- **Task 16:** Tiingo Provider. Implement daily + IEX connector in `src/stockodile/providers/tiingo/`.
  - Subagent Role: Tiingo Connector Coder
- **Task 17:** Alpaca-IEX & Finnhub. Implement WebSocket and REST sources.
  - Subagent Role: Live Stream Architect

## Milestone 4: Analytics, Replay & CLI
- **Task 18:** Resampler. Implement OHLCV and book snapshot resampling in `src/stockodile/resample/`.
  - Subagent Role: Resampling Engineer
- **Task 19:** Options & Ratios Analytics. Implement BSM pricing + fundamental ratios in `src/stockodile/analytics/`.
  - Subagent Role: Quant Developer
- **Task 20:** Replay, Client & CLI. Wire up everything, write `src/stockodile/cli.py` and run tests.
  - Subagent Role: Integration Master
