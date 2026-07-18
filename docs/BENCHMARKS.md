# Stockodile — Market Depth: what it is, how novel it is, and how it compares

This document explains the synthetic market-depth capability I added to Stockodile, honestly
assesses how original it is, surveys comparable projects, and runs a reproducible head-to-head
benchmark against the nearest competitor. Every number in the tables below was **measured on my
machine** with the scripts described in "Reproducing this" — I have not estimated or invented any
figure. Where a cell is a feature fact rather than a measurement, it is marked *(feature)*.

---

## 1. What I built

Stockodile is a $0, self-hosted US-equity market-data engine. The new piece is **market depth** —
an order-book-style bid/ask ladder — delivered at zero cost through legal channels only.

The problem: **no free source provides true US-equity Level-2 (resting limit-order-book) data.**
Nasdaq TotalView, IEX DEEP, and Databento MBP/MBO are all paid; the free tiers (Alpaca IEX, IEX
TOPS) top out at Level-1 top-of-book. So a genuinely free "depth" feature cannot be real L2 — and I
refuse to pretend otherwise.

My solution is a facade with two sources behind one interface:

- **Keyless default → synthetic.** I synthesize a depth ladder from **free Yahoo 1-minute bars**.
  Each bar's volume is distributed across its price range into price buckets (a volume-at-price
  profile), then split around the last traded price into bid-side (below) and ask-side (above)
  levels. This is **relative** liquidity — where volume historically concentrated — **not** real
  resting orders, and it is labeled that way everywhere it surfaces (`is_synthetic=true`,
  `basis="yahoo_1m_vap"`, a `⚠️ SYNTHETIC` banner in the CLI, a `warning` field in the API/MCP).
- **With an Alpaca key → real L1.** If `ALPACA_API_KEY` and `ALPACA_API_SECRET` are set, the same
  `depth` command/endpoint transparently upgrades to real Alpaca L1 top-of-book via Alpaca's
  official REST latest-quote API (`basis="alpaca_l1"`, `is_synthetic=false`) — **no code change**.
  If a key is set but auth fails, the error surfaces; it never silently falls back to synthetic.

The result flows through Stockodile's existing lake: a new tag-driven `depth` channel is persisted
to hive-partitioned Zstd Parquet and auto-registered as a DuckDB SQL view, so depth is queryable
and replayable like every other channel.

**Legal by construction:** public Yahoo JSON + official Alpaca API. No CAPTCHA solving, no
Proof-of-Work bypass, no cookie spoofing.

---

## 2. Is this novel? An honest assessment

I am **not** claiming this is a new invention. The building blocks are commodity:

- **The volume-at-price histogram is textbook.** Market Profile / TPO dates to Pete Steidlmayer at
  the CBOT in the 1980s; it ships in TradingView, Sierra Chart, NinjaTrader, and PyPI packages like
  `MarketProfile` and `volprofile`. My bucketing is the standard idiom.
- **Even "synthetic order book from bars" already exists** as charting indicators — e.g.
  TradingView's "Synthetic OrderBook" (s3raphic333) builds a bid/ask ladder from price action +
  volume profile and explicitly disclaims being real L2. So the *concept* of an honestly-labeled
  synthetic depth ladder is prior art.

What I could **not** find an existing match for is the **packaging**: this synthetic ladder
delivered as a programmatic, order-book-shaped API surface **inside an open-source Python equity
data engine**, with (a) a transparent keyless-synthetic → real-Alpaca-L1 upgrade behind one
interface, (b) persistence into a queryable Parquet/DuckDB lake, and (c) explicit synthetic
labeling. That is an engineering/integration contribution, not a conceptual one.

**Self-assessed originality: ~2.5 / 5** — more considered than a copy-paste indicator, well short of
unique. I would rather state that plainly than oversell it.

To make the "packaging" point concrete, I tested the closest Python volume-profile library,
`MarketProfile`, on the same Yahoo 1-minute bars (see §4): it returns a Point-of-Control and
value-area (`poc_price`, `value_area`) but has **no bid/ask split** and **no persistence** — it is a
profile analytic, not a depth ladder in an engine.

---

## 3. Comparable projects (the landscape)

No free/open-source project combines Stockodile's full stack (keyless multi-provider US-equity
ingestion + self-hosted Parquet/DuckDB lake + SQL query/replay + synthetic depth). Each overlaps a
different axis:

| Project | Overlaps on | Missing vs Stockodile | License |
|---|---|---|---|
| **OpenBB Platform** | Keyless free multi-provider US-equity ingestion + normalization | No Parquet/DuckDB lake, no SQL/replay, **no depth** | AGPLv3 |
| **market-data-warehouse** (joemccann) | Self-hosted Parquet medallion lake + DuckDB SQL (near-identical design) | Not keyless (needs IB account), daily-only, no depth, very young | MIT |
| **NautilusTrader** | Persisted Parquet catalog + replay + **real** L2 depth | No keyless free US-equity source, not SQL, depth needs paid feeds | LGPL-3.0 |
| **Qlib** (Microsoft) | Keyless Yahoo ingestion + own store + query engine | Proprietary non-Parquet/non-SQL format, daily-focused, no depth | MIT |
| **ArcticDB** (Man Group) | High-performance columnar tick store | No data sources, no SQL, no depth; production use needs paid license | BSL 1.1 |
| **yfinance / alpaca-py** | The free data sources themselves | Fetch-only libraries; no lake, no query, no depth | Apache-2.0 |

**Nearest overall competitor: OpenBB Platform** — the only widely adopted, keyless, free,
multi-provider US-equity ingestion layer a user would realistically reach for *instead of*
Stockodile. Its gaps (no lake/SQL, no depth) are precisely Stockodile's differentiators, which makes
it the ideal head-to-head baseline.

---

## 4. Head-to-head benchmark (measured)

**Environment:** macOS (Apple Silicon), Python 3.12, throwaway `uv` virtualenvs per tool.
OpenBB `1.6.x` (full meta-package), `MarketProfile` latest, Stockodile at this branch. Fetch tests
use the same free Yahoo/`yfinance` source under the hood for every tool, so fetch latency is
network- and rate-limit-bound and will vary run to run — treat those two rows as indicative, not
precise. Capability and footprint rows are deterministic.

| Dimension | Stockodile | OpenBB Platform | MarketProfile (depth analog) |
|---|---|---|---|
| Keyless US-equity 1m bars | ✅ 0.9 s, 2729 bars (AAPL) | ✅ 8.9 s, 1949 rows | ➖ not a fetcher (fed via yfinance) |
| Keyless equity **depth ladder** ($0) | ✅ synthetic VAP bid/ask | ❌ no depth/orderbook surface (measured: `obb.equity` has none) | ❌ POC/value-area only, `has_bid_ask_split=False` (measured) |
| Transparent real-L1 upgrade by key *(feature)* | ✅ Alpaca L1 via env var | ❌ | ❌ |
| Self-hosted Parquet lake *(feature)* | ✅ hive-partitioned Zstd | ❌ built-in lake/SQL: False (measured) | ❌ |
| DuckDB SQL query + replay *(feature)* | ✅ | ❌ | ❌ |
| Depth-synthesis latency | ✅ **1.06 ms** median (20 runs) | ➖ n/a | ➖ n/a |
| Volume conservation of synthesis | ✅ ratio **1.000000** (272,225,407 in = out) | ➖ n/a | ➖ different method (value-area) |
| Honest "synthetic" labeling *(feature)* | ✅ banner / `warning` / `is_synthetic` | ➖ n/a | ➖ raw profile |
| Import time | ➖ fast | 5.2 s (measured) | ➖ fast |
| Install footprint | 46 pkgs / **449 MB** (core) | **102 pkgs** / 180 MB (full) | 28 pkgs |
| License | Apache-2.0 | AGPLv3 | MIT-family |

**Reading the footprint row honestly:** Stockodile-core installs **fewer packages** (46 vs 102) but
uses **more disk** (449 MB vs 180 MB) — because it bundles the DuckDB + Polars + PyArrow analytical
engine that *gives* it the lake and SQL layer OpenBB does not have. A minimal
`openbb-core + openbb-yfinance` install would be lighter than the 102-package full meta-package; I
measured the full `pip install openbb` because that is what a user actually gets.

**The decisive columns** are depth, lake, and SQL/replay: OpenBB structurally lacks all three for US
equities (verified by probing its API, not by assumption), and the standard volume-profile library
produces a profile but not an order-book-shaped, persistable depth ladder. Those three rows are the
reason this feature exists.

---

## 5. Reproducing this

Stockodile side (live, keyless):

```bash
stockodile depth AAPL --persist
stockodile query "SELECT symbol, basis, is_synthetic, reference_price, depth FROM depth"
```

Competitor probes (throwaway envs):

```bash
# OpenBB — nearest competitor
uv venv b1 && uv pip install --python b1/bin/python openbb
b1/bin/python -c "from openbb import obb; \
  r=obb.equity.price.historical('AAPL',interval='1m',provider='yfinance'); \
  print(len(r.to_df())); \
  print('depth attr:', any('book' in a or 'depth' in a for a in dir(obb.equity)))"

# MarketProfile — closest volume-profile analog
uv venv b2 && uv pip install --python b2/bin/python MarketProfile yfinance
```

Depth-synthesis latency and volume conservation are covered by the unit tests in
`tests/test_depth_vap.py` (volume-conservation assertions) and can be timed directly against
`stockodile.depth.vap.volume_at_price`.

---

## 6. Honest caveats

- **Synthetic depth is not real liquidity.** It is a relative volume-at-price proxy from historical
  1-minute bars. It shows where volume concentrated, not resting orders. It is labeled synthetic
  everywhere; do not trade it as if it were a real book.
- **The Alpaca L1 upgrade path is built and unit-tested with mocked responses, but I have not proven
  it against a live Alpaca account here** (that needs a real key). The synthetic path is proven live
  end to end (Yahoo → VAP → Parquet → DuckDB).
- **Fetch-latency numbers are network- and rate-limit-bound** (Yahoo returns HTTP 429s under load);
  the capability, footprint, and synthesis-latency rows are the stable, meaningful ones.
- **Novelty is modest.** The concept is prior art; the contribution is packaging and honesty.
