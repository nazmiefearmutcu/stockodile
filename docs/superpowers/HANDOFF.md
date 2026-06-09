# Stockodile — Session Handoff

> **Devam etmek için (TR):** Beyin fırtınası bitti ve kullanıcı tarafından onaylandı. Sıradaki tek
> adım: `superpowers:writing-plans` becerisini çağırıp aşağıdaki spec+appendix'e dayanan
> uygulama planını yazmak, sonra `superpowers:subagent-driven-development` ile kodu yazdırmak.
> Kullanıcının kuralı: **tek kuruş ödemeden, ücretsiz kaynaklarla, hiçbir veri kaçmadan; haber (news) HARİÇ.**

**Last updated:** 2026-06-09
**Project:** Stockodile — free-only US-equity market-data engine, 1:1 sibling of Crypcodile.
**Repo:** `/Users/nazmi/Desktop/Stockodile` · package `src/stockodile` · CLI `stockodile` ·
git branch `main` (no remote yet) · Apache-2.0.
**Reference project to mirror:** `/Users/nazmi/Desktop/Crypcodile` (study it for every pattern).

---

## 1. Where we are in the process

Process is: **brainstorming → writing-plans → subagent-driven-development** (user invoked
`/subagent-driven-development` and `/goal`; `/goal` is not an installed skill — ignored).

- [x] **Brainstorming COMPLETE & APPROVED** by user (2026-06-09).
- [x] Design spec written + committed.
- [x] Provider research appendix produced (17-source parallel research + adversarial
      verification, 35 agents) + committed.
- [x] Design matrix reconciled with verified corrections + committed.
- [x] Memory written (`stockodile-project.md` + MEMORY.md index).
- [ ] **NEXT: writing-plans** — produce `docs/superpowers/plans/2026-06-09-stockodile-core.md`
      (was just started, then paused for this handoff). NOT yet written.
- [ ] **THEN: subagent-driven-development** — execute the plan task-by-task, fresh subagent
      per task, two-stage review.

### How to resume (exact next actions)
1. Read `docs/superpowers/specs/2026-06-09-stockodile-design.md` (the design).
2. Read `docs/superpowers/specs/2026-06-09-stockodile-research-appendix.md` (authoritative
   implementation detail — **appendix wins** on any low-level conflict).
3. Skim Crypcodile's patterns (Section 4 below) — Stockodile mirrors them.
4. Invoke `superpowers:writing-plans`. Write the plan in **bite-sized TDD tasks** (failing
   test → run → minimal impl → run → commit), with exact file paths and real code (no
   placeholders). Phase it by the M1–M7 milestones (Section 5). Save to
   `docs/superpowers/plans/2026-06-09-stockodile-core.md`. Consider splitting into multiple
   plans (e.g. `-core`, `-providers`, `-fundamentals`, `-analytics`) — each must produce
   working, testable software on its own.
5. Invoke `superpowers:subagent-driven-development` to execute.
   - Note: session is in **learning + explanatory output style** — at genuine design-decision
     points, offer the user the chance to write 5–10 lines of the key logic, and include
     `★ Insight` educational notes. Honor user instructions over skill defaults.

---

## 2. Locked decisions (do NOT relitigate)

| Decision | Choice |
|---|---|
| Goal | US-equity sibling of Crypcodile; maximum coverage |
| Source strategy | **Free sources only — $0, never a credit card.** Paid adapters stay stubbed/ready, never required. |
| Scope | **Maximum taxonomy** — all asset classes + all data categories (microstructure, reference, corp-actions, fundamentals, insider, 13F, short-interest, filings, options, index, macro). |
| News | **EXCLUDED** (user decision). Schema leaves room; not built. |
| Architecture | **Approach A**: provider-adapter (`providers/`, mirrors Crypcodile `exchanges/`) + coverage-resolver. |
| Stack | Python 3.12 · uv · msgspec/Polars/PyArrow/DuckDB · websockets/aiohttp · Typer+Rich · mypy strict + ruff · pytest (network-free). |

---

## 3. Honesty-critical verified facts (from the appendix — design depends on these)

- **No free real-time consolidated SIP tape and no free real-time OPRA — ever** (licensed).
  Document this ceiling; never label free data as consolidated/NBBO/real-time.
- **Free real-time = IEX only:** Alpaca-IEX (~2.5–3% of tape; quotes are IEX **BBO, not
  NBBO** → stamp `is_nbbo=false`/`is_consolidated=false`) + Finnhub WS trades (≤50 symbols).
- **Free equity L2 = raw IEX DEEP only** (binary PCAP, T+1, IEX-venue). None on
  Alpaca/Finnhub at any tier.
- **SEC EDGAR is the backbone:** fundamentals (XBRL companyfacts), 13F, insider (Forms
  3/4/5), all filings — 100% free/official. **FINRA:** short interest + Reg SHO, free.
- **Premium traps (route around):** Finnhub `/stock/candle` OHLCV (premium → use
  Alpaca/Tiingo/Polygon/Yahoo/Stooq); Alpha Vantage full daily + historical index (premium;
  25 req/day; long free history via weekly/monthly `_ADJUSTED`); Tiingo News/Fundamentals/
  corp-action endpoints (paid; 500 symbols/mo cap); Tradier sandbox Greeks/index
  (production-only → option Greeks from **Cboe** delayed snapshots); Polygon free = EOD only,
  5/min, 2yr; Alpaca options = indicative 15-min-delayed, Feb-2024+.
- **FRED SP500 & DJIA = rolling-10yr only** → deep index history from NASDAQCOM (1971+) /
  Cboe daily (VIX to 1990).
- **Dead, do not use:** IEX Cloud (iexcloud.io), api.iextrading.com REST,
  `pandas-datareader` stooq reader, `alpaca-trade-api`, `iexfinance`. Stooq now gates CSV
  behind JS proof-of-work + CAPTCHA apikey.
- **Integration gotchas:** SEC CIK 10-digit zero-pad + mandatory descriptive User-Agent;
  `company_tickers.json` `cik_str` is plain int (left-pad); Tiingo auth prefix `Token ` (not
  Bearer); FRED `file_type=json` (default XML), values are strings, missing=`'.'`; Cboe
  indices underscore-prefixed (`_VIX`); OpenFIGI POST JSON-array, sliding 6s window; FINRA/
  Nasdaq files pipe-delimited (`sep='|'`) with footer row to strip, filter `Test Issue=Y`;
  13F `value` units thousands→whole-dollars from Jan 3 2023; Tradier/IEX single-vs-multi
  JSON shape (object vs list) must be normalized.
- **Corp-action adjustment:** CRSP cumulative factor, `f = FACPR + 1`, apply on ex-date,
  `adj_price = raw / CFACPR`, volume uses `CFACSHR`. Store raw + factors + dividend cash;
  compute adjusted on read. Full worked example in appendix §6.

---

## 4. Crypcodile patterns to mirror (already studied — file paths)

Stockodile reuses these almost verbatim (rename `exchange`→`provider` where it appears):

- **Canonical schema** — `Crypcodile/src/crypcodile/schema/records.py` + `enums.py`. Each
  record is a `msgspec.Struct(frozen=True, tag="<channel>", tag_field="channel")` with dual
  timestamps (`exchange_ts`+`local_ts` → Stockodile uses `source_ts`+`local_ts`). A `Record`
  union type aggregates them.
- **Connector ABC** — `Crypcodile/src/crypcodile/exchanges/base.py`. Abstract
  `normalize()`, `list_instruments()`, `_subscribe()`; optional `backfill()` async-gen;
  supervised `run()` loop with `backoff_delays()` (exp + jitter, capped) + DeadLetterQueue.
  Stockodile's `Provider` ABC = this; pull-only providers (SEC/FINRA/Stooq) make `_subscribe`
  a no-op and put the work in `backfill`.
- **Sink** — `Crypcodile/src/crypcodile/sink/base.py` (`Sink` ABC: put/flush/close).
- **ParquetSink** — `Crypcodile/src/crypcodile/store/parquet_sink.py`. Per-channel buffers,
  `_COMMON_FIELDS` + `_CHANNEL_EXTRA` Polars schema maps, zstd-5, row-group 250k, hive path
  `exchange={E}/channel={C}/date=YYYY-MM-DD/bucket={0..127}/part-{uuid}.parquet`. Stockodile:
  `provider={P}/channel={C}/date=.../bucket=...`; bucket from symbol hash (mmh3). Low-freq
  channels (fundamentals/13F/filings) can partition by `channel/date=filed` (no bucket).
- **Catalog** — `Crypcodile/src/crypcodile/store/catalog.py`. In-memory DuckDB, one VIEW per
  channel over `read_parquet(glob, hive_partitioning=true, union_by_name=true)`; `query(sql)`
  + `scan(channel, symbol, start_ns, end_ns)` with date-glob partition pruning; returns Polars.
- **rows** — `Crypcodile/src/crypcodile/store/rows.py` (`to_row(record)` → dict with
  channel/date/bucket; not yet read in detail — read it when implementing the sink).
- **Factory** — `Crypcodile/src/crypcodile/exchanges/factory.py` (`make_connector` name→class
  registry). Stockodile: `make_provider`.
- **Ingest** — `Crypcodile/src/crypcodile/ingest/` (transport, gap_bridge, deadletter).
- **Client/CLI** — `Crypcodile/src/crypcodile/client/client.py`, `cli.py` (Typer).
- **pyproject.toml** — copy Crypcodile's (deps: msgspec, polars, pyarrow, duckdb, websockets,
  aiohttp, typer, rich, mmh3; dev: pytest(+asyncio,cov,timeout), ruff, mypy; mypy strict,
  ruff E/F/I/UP/B/ASYNC/RUF, pytest pythonpath=["src"]). Rename package crypcodile→stockodile.

**New layers (not in Crypcodile):** `ratelimit/` (per-provider token-bucket + 429 backoff),
`scheduler/` (trading-calendar-aware low-freq REST pulls), `reference/` (security master +
FIGI/CIK/CUSIP mapping), `corpactions/` (CRSP adjustment), `fundamentals/` (XBRL parse),
`coverage/` (matrix + resolver/merge/dedup across overlapping free sources).

**Canonical channels to define** (schema): `trade`, `quote`, `book_delta`/`book_snapshot`
(l2), `bar` (ohlcv), `auction`/`imbalance`, `trading_status`, `instrument`,
`corp_action`, `fundamental`, `insider`, `holding_13f`, `short_interest`, `short_volume`,
`filing`, `option_quote`, `index_value`, `macro_series`. Field-level provider→canonical
mappings are in appendix §3.

---

## 5. Milestones (phase the plan around these)

- **M1 — Core skeleton:** schema + enums, Sink + ParquetSink, Catalog, Provider ABC + run
  loop, CLI shell, first provider end-to-end (SEC EDGAR reference + one OHLCV source such as
  Stooq or Alpaca-IEX) → `query` works.
- **M2 — Reference & corporate spine:** security master (`reference/`), OpenFIGI mapping,
  exchange-directory universe, corporate actions, CRSP adjusted prices.
- **M3 — Aggregates breadth:** full-market OHLCV (Stooq/Yahoo/Polygon-free/Alpaca-IEX) +
  coverage-resolver dedupe; resample.
- **M4 — Microstructure (free RT):** Alpaca-IEX + Finnhub live trades/quotes, IEX DEEP L2,
  replay + book reconstruction.
- **M5 — Fundamentals & regulatory:** SEC XBRL fundamentals, Forms 3/4/5, 13F, FINRA short
  interest + Reg SHO, filings index.
- **M6 — Options & macro:** Cboe/Tradier/Yahoo option chains, FRED macro, CBOE/VIX, options
  analytics (Black-Scholes-Merton + dividend).
- **M7 — Hardening:** benchmarks, honest "coverage ceiling" README, full mypy/ruff/test gate.

Each milestone: real provider wired → normalized to canonical schema → network-free tests →
mypy strict + ruff clean → commit.

---

## 6. Environment notes

- Working dir for the assistant: `/Users/nazmi/Desktop`. Stockodile + Crypcodile are siblings
  there.
- Python managed by **uv**. Mirror Crypcodile's `uv sync` workflow. NOTE (from Crypcodile
  memory): uv editable `.pth` is flaky → rely on `pytest pythonpath=["src"]`,
  `mypy mypy_path=src`, and an example bootstrap rather than editable install.
- Git: branch `main`, committed: design spec, research appendix, .gitignore. No remote.
  If the user later wants to push, GitHub push needs a Contents:write PAT (per Crypcodile).
- Memory files: `~/.claude/projects/-Users-nazmi-Desktop/memory/stockodile-project.md`
  (+ indexed in `MEMORY.md`).

---

## 7. Commits so far (in `/Users/nazmi/Desktop/Stockodile`)

```
b388d3e docs: add verified provider research appendix + reconcile design matrix
c49d512 docs: Stockodile v1 design spec (US-equity sibling of Crypcodile)
```
