# Stockodile — Core Design (v1)

**Date:** 2026-06-09
**Status:** Approved (architecture) by user. Research appendix + implementation plan to follow.
**Sibling project:** [`Crypcodile`](../../../../Crypcodile) — Stockodile inherits Crypcodile's
proven ingest→normalize→store→retrieve→resample→analyze pipeline and engineering standard
(mypy strict, ruff, network-free tests, hive-partitioned Parquet + DuckDB), retargeted from
crypto venues to the **US equity market**.

**Companion (authoritative implementation detail):**
`2026-06-09-stockodile-research-appendix.md` — provider-verified endpoints, auth/free-tier
requirements, rate limits, field mappings, and a consolidated gotchas list. Where this design
and the appendix differ on a low-level detail, **the appendix wins** (it is sourced from each
provider's official docs and adversarially gap-checked).

---

## 1. Vision & Goal

Stockodile is an **open-source, self-hosted** engine that ingests US-equity market data
(live + historical) from **every freely accessible source**, normalizes everything into
**one canonical schema**, stores it in a compressed columnar lake (Parquet + DuckDB), and
makes it retrievable **anywhere, at any resolution** (replay + multi-format export + SQL),
with equity-specific analytics on top.

**North star (user's words):** *"Access every source without paying a single penny; nothing
in the US stock market should escape."* The coverage target is the **union of all free
providers**, squeezed to the maximum the free tier physically allows — at the same depth and
engineering standard that Crypcodile achieves for crypto.

**Coverage = the provider × data-type matrix is full** (Section 6). "Dominance %" is the
filled fraction of that matrix.

## 2. The free-data reality (locked constraint)

US equity data is **not** free-and-open like crypto exchange APIs. Honest ceiling:

- **Fully free AND complete:** fundamentals & financial statements (SEC EDGAR XBRL
  `company-facts`), all SEC filings (10-K/10-Q/8-K/S-1/…), insider transactions (Forms
  3/4/5), institutional holdings (13F), short interest + Reg SHO daily short volume (FINRA),
  corporate actions (splits/dividends/ticker changes), the full symbol/reference universe
  (SEC `company_tickers`, OpenFIGI), **full-market daily & intraday OHLCV aggregates**
  (Stooq, Yahoo, Polygon-free EOD, Alpaca IEX historical), **options chains** (Yahoo +
  Tradier sandbox, free), indices and macro (FRED, CBOE VIX).
- **Free but partial:** real-time trades + depth. The only legitimately free real *tick*
  sources are **IEX** (via Alpaca free) and **Finnhub free WebSocket** — real trades, but a
  fraction of consolidated volume. **IEX DEEP** gives full depth for IEX only.
- **Not legitimately free at all:** the full **SIP consolidated tape** (every trade/NBBO
  across all venues) and full **real-time OPRA** options stream — both are licensed.

**Design consequence:** the architecture guarantees "nothing escapes" by unioning every
free source into one schema, and stays **paid-ready** — dropping in a Polygon/Databento key
later fills the last % through the same schema, but is **never required**.

## 3. Non-Goals (v1)

- No paid feeds **required** (paid adapters are stubbed/ready, never mandatory).
- No licensed full SIP tape / full real-time OPRA (documented ceiling, not a bug).
- No **news/sentiment** (explicitly excluded by user) — `schema` leaves room; not built.
- No REST/WebSocket **server** yet (client + CLI + export covers delivery now).
- No dashboard/UI yet.
- No non-US markets (US-equity-first).

## 4. Architecture Decisions (locked)

| Decision | Choice | Why |
|---|---|---|
| Language/runtime | **Python 3.12** (asyncio) | Mirrors Crypcodile; richest equity-data ecosystem. |
| Packaging | Single installable package `stockodile`, `src/` layout, `uv` | Same as Crypcodile sibling. |
| Hot path / decode | **msgspec** structs + **Polars** transforms | Identical to Crypcodile. |
| Storage | **Hive-partitioned Parquet** (zstd) + **DuckDB** SQL | Reuse Crypcodile `store/` + `catalog`. |
| Source abstraction | **Provider-adapter** (`providers/`) + **coverage-resolver** | Mirrors Crypcodile `exchanges/`; resolver unions/dedupes overlapping free sources. |
| Delivery (v1) | Python client + Typer/Rich CLI + multi-format export + replay | Same as Crypcodile. |
| Free-tier safety | **`ratelimit/` + `scheduler/`** (new) | Free tiers throttle hard; first-class quota budgeting + backoff. |
| License | **Apache-2.0** | Same as Crypcodile. |
| Tooling | uv · ruff · pytest (network-free) · mypy strict | Same standard as Crypcodile. |

Rejected: capability-router (runtime "best source" routing) — diverges from Crypcodile's
clean, testable per-source design.

## 5. Canonical Schema (`schema/`)

Every record carries **dual nanosecond timestamps** (`source_ts` + `local_ts`) for
deterministic replay/resample, exactly like Crypcodile.

**Market microstructure:** `Trade` (price, size, venue, condition codes, tape A/B/C,
trade_id) · `Quote` (bid/ask + sizes + venue best; consolidated NBBO only if a paid key is
present, else venue-best) · `BookDelta` / `BookSnapshot` (L2; IEX DEEP full depth) · `Bar`
(OHLCV + VWAP + trade_count; `1s`→`1d`+) · `Auction` / `Imbalance` (IEX cross) ·
`TradingStatus` (LULD bands, halt, SSR).

**Reference & corporate:** `Instrument` (symbol, name, CIK, FIGI, CUSIP-where-free,
exchange, security type CS/ETF/ADR/REIT/PFD/WARRANT/UNIT/RIGHT, SIC, shares outstanding,
listing date, status) · `CorporateAction` (split, cash/stock dividend, spinoff, merger,
ticker/CUSIP change; ex/record/pay dates).

**Fundamentals & regulatory:** `Fundamental` (XBRL company-facts: revenue, net income, EPS,
assets, liabilities, equity, cash-flow lines; period Q/FY, fiscal date, form, frame) ·
`InsiderTransaction` (Forms 3/4/5) · `InstitutionalHolding` (13F) · `ShortInterest`
(bi-monthly) + `ShortVolume` (Reg SHO daily) · `Filing` (all EDGAR metadata + document links).

**Options, index, macro:** `OptionQuote` (underlying, expiry, strike, type, bid/ask/last,
volume, OI, IV, greeks) · `IndexValue` (S&P 500, Nasdaq, Russell, VIX, …) · `MacroSeries`
(FRED observations).

Each record type is a `msgspec.Struct`, has a stable `channel` name (its partition key), and
a Polars/Arrow schema for the Parquet sink. Enums (security type, tape, condition, action
type, option type, period) live in `schema/enums.py`.

## 6. Providers & Coverage Matrix (all $0)

`providers/<name>/` mirrors Crypcodile's `exchanges/<name>/`: `connector` (live, where a free
stream exists), `normalize` (raw → canonical), `backfill` (REST/historical). Coverage =
**union**; a coverage-resolver merges/dedupes overlapping cells.

| Provider | Live RT | OHLCV | Trades | Quote | L2 | Reference | Corp-act | Fundamentals | Insider/13F | Short | Filings | Options | Index/Macro |
|---|:--:|:--:|:--:|:--:|:--:|:--:|:--:|:--:|:--:|:--:|:--:|:--:|:--:|
| SEC EDGAR | — | — | — | — | — | ✅ | ✅ | ✅✅ | ✅✅ | — | ✅✅ | — | — |
| FINRA | — | — | — | — | — | — | — | — | — | ✅✅ | — | — | — |
| Alpaca (IEX-free) | ✅ | ✅ | ✅ | ✅ | — | ✅ | — | — | — | — | — | ✅ | — |
| Finnhub-free | ✅ | ✅ | ✅ | — | — | ✅ | — | ~ | — | — | — | — | — |
| IEX (HIST/DEEP) | ~ | ✅ | ✅ | ✅ | ✅ | — | — | — | — | — | — | — | — |
| yfinance (Yahoo) | — | ✅ | — | — | — | ✅ | ✅ | ✅ | — | — | — | ✅✅ | ✅ |
| Stooq | — | ✅✅ | — | — | — | ✅ | — | — | — | — | — | — | ✅ |
| Polygon-free | — | ✅ | — | — | — | ✅ | ✅ | — | — | — | — | — | — |
| Tiingo-free | — | ✅ | — | — | — | ✅ | — | ~ | — | — | — | — | — |
| Alpha Vantage | — | ✅ | — | — | — | — | — | ✅ | — | — | — | — | ✅ |
| Tradier sandbox | — | — | — | ✅ | — | ✅ | — | — | — | — | — | ✅✅ | — |
| OpenFIGI | — | — | — | — | — | ✅✅ | — | — | — | — | — | — | — |
| FRED | — | — | — | — | — | — | — | — | — | — | — | — | ✅✅ |
| CBOE | — | ✅ | — | — | — | — | — | — | — | — | — | ✅ | ✅ |

(`✅✅` = primary/authoritative source for that cell; `~` = limited.) Exact endpoints, auth,
free-tier limits, and field mappings are pinned in the **research appendix** (must be
verified before each provider is implemented).

## 7. Module tree (`src/stockodile/`)

```
schema/        canonical records + enums (dual-ns ts)                [Crypcodile pattern]
providers/     sec_edgar · finra · alpaca · finnhub · iex · yahoo · stooq · polygon ·
               tiingo · alphavantage · tradier · openfigi · fred · cboe
               (each: connector[live] + normalize + backfill[REST])
ingest/        run-loop · transport · gap-bridge · dead-letter        [from Crypcodile]
ratelimit/     NEW — free-tier quota budgeting + exponential backoff
scheduler/     NEW — scheduled pulls for low-frequency REST sources
reference/     NEW — security master (symbol universe + FIGI/CIK/CUSIP mapping)
corpactions/   NEW — corporate actions + split/dividend price adjustment
fundamentals/  NEW — SEC XBRL parse/normalize
coverage/      NEW — coverage matrix + resolver/merge/dedup
sink/ store/   Parquet sink + DuckDB catalog (hive-partitioned, zstd)  [from Crypcodile]
replay/        k-way merge + order-book reconstruction                [from Crypcodile]
resample/      OHLCV · book snapshots · VWAP/$-vol · corp-action-aware
analytics/     adjusted-price · returns/realized-vol/beta · options IV/greeks
               (Black-Scholes-Merton + dividend) · fundamental ratios (P/E,P/B,ROE,margin)
client/        StockodileClient + collect/export                      [from Crypcodile]
cli.py         the `stockodile` command (Typer + Rich)                [from Crypcodile]
```

## 8. Data flow, storage, error handling

- **Flow:** providers → ingest / scheduler → normalize → coverage-resolver → Parquet lake ↔
  DuckDB → client / CLI / replay / export / resample / analytics.
- **Storage:** hive-partitioned Parquet (zstd). High-frequency: `channel=<type>/
  symbol_bucket=<n>/date=<day>`. Low-frequency (fundamentals/13F/filings):
  `channel=<type>/date=<filed>`. Reuse Crypcodile `store/catalog.py` + DuckDB unchanged.
- **Error handling:** gap-bridge + dead-letter (from Crypcodile) **+** free-tier-aware rate
  limiting (new `ratelimit/`): per-provider quota budget, 429/backoff, and a scheduler that
  spaces low-frequency pulls. Each provider is isolated — one failing source does not stop
  the union.

## 9. Analytics (equity-specific)

- **Adjusted prices:** apply `CorporateAction` (split + dividend) to produce
  total-return-adjusted series — foundational for equities.
- **Returns / risk:** simple & log returns, realized vol, beta vs an index series.
- **Options:** reuse Crypcodile's Black-Scholes/vol-surface, extended to **Black-Scholes-
  Merton with dividend yield**; document the European-approx caveat for American options.
- **Fundamental ratios:** P/E, P/B, ROE, margins from XBRL facts + price.

## 10. Stack & packaging

Python 3.12 · uv · `msgspec` / Polars / PyArrow / DuckDB · `websockets` / `aiohttp` ·
Typer + Rich · **mypy strict + ruff** · pytest (network-free, recorded fixtures per provider
normalize) · benchmarks. New repo at `/Users/nazmi/Desktop/Stockodile`, package
`src/stockodile`, CLI `stockodile`, own git, Apache-2.0. Mirrors Crypcodile's `pyproject.toml`.

## 11. Milestones (drive the implementation plan; phased but full taxonomy is the target)

- **M1 — Core skeleton:** schema + sink + store/catalog + ingest run-loop + CLI shell +
  first provider end-to-end (SEC EDGAR reference + one OHLCV source, e.g. Stooq) → query.
- **M2 — Reference & corporate spine:** security master (`reference/`), OpenFIGI mapping,
  corporate actions, adjusted prices.
- **M3 — Aggregates breadth:** full-market OHLCV via Stooq/Yahoo/Polygon-free/Alpaca-IEX +
  coverage-resolver dedupe.
- **M4 — Microstructure (free RT):** Alpaca-IEX + Finnhub live trades/quotes, IEX DEEP L2,
  replay + book reconstruction.
- **M5 — Fundamentals & regulatory:** SEC XBRL fundamentals, Forms 3/4/5, 13F, FINRA short
  interest + Reg SHO, filings index.
- **M6 — Options & macro:** Yahoo/Tradier option chains, FRED macro, CBOE/VIX, options
  analytics.
- **M7 — Hardening:** benchmarks, honest "coverage ceiling" README, full mypy/ruff/test gate.

Each milestone: real provider wired, normalized to canonical schema, network-free tests,
mypy strict + ruff clean.

## 12. Success criteria

1. `stockodile collect`/`query`/`replay`/`export`/`resample` work end-to-end against a local
   lake built purely from free sources — no API payment anywhere.
2. Coverage matrix (Section 6) is fully wired; every `✅` has a real, tested provider path.
3. Engineering parity with Crypcodile: mypy strict, ruff clean, high test count, benchmarks.
4. README documents the honest free-data ceiling and the paid-ready (never-required) path.
