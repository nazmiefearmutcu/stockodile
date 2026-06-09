# Stockodile — Provider Research Appendix (v1)

> **Status:** Authoritative implementation-detail source of truth. Where this appendix and the high-level design disagree, **this wins.** Every empirical claim below was live-verified and run through adversarial correction; corrections are folded in inline and marked **[CORRECTED]** / **[MOVED TO PREMIUM]** where the free reality differs from first-pass research. Verification date baseline: **2026-06-09**.

---

## 1. Free-data ceiling — what $0 can and cannot reach

**The hard truth:** real-time US-equity data IS available for $0, but only as a **single-venue / partial slice** — never the licensed full consolidated tape, and never a full real-time options chain.

### What $0 CAN reach (real-time)
| Source | What you get free | Coverage | Card? |
|---|---|---|---|
| **Alpaca Basic** | Real-time **IEX** trades + quotes + 1-min/daily bars via WebSocket; full historical bars/trades/quotes (SIP allowed for data >15 min old) | IEX feed only (**~2.5–3%** of consolidated US volume) | No |
| **Finnhub free** | Real-time US **trade** stream over WebSocket (≤50 symbols); `/quote` REST snapshot (last + day OHLC + prev close) | US only | No |
| **Raw IEX Exchange HIST** | T+1 full-fidelity **TOPS** (top-of-book + last) and **DEEP** (full depth/L2) PCAP downloads | IEX venue only; binary PCAP, **no REST/JSON** | No |

### What $0 CANNOT reach (paid, no exceptions)
- **Full SIP consolidated tape** (every trade + true national NBBO across all ~16 exchanges + TRFs). CTA/UTP real-time licensing is fee-liable: non-display tiers ~$500–$3,500/mo; direct/colo bundles ~$13k–$20k/mo. **Any vendor offering "free real-time SIP" is delayed, sampled, or in breach of its agreement.**
- **Full real-time OPRA options feed.** OPRA fees (verified vs opraplan.com): ~$31.50/mo professional per user/device, ~$1.25/mo non-professional, plus a flat **~$1,500/mo redistributor fee** for surfacing OPRA externally; direct ports ~$16k–$20.5k/mo. Alpaca free options = **indicative** (derived, 15-min-delayed) feed only; real OPRA needs Alpaca Algo Trader Plus ($99/mo).

### Real-time reality of the free sources
- **Alpaca free "quotes" are IEX best bid/ask, NOT national NBBO.** Treating them as NBBO mis-states spreads/mid/VWAP for any name that trades mostly off-IEX. IEX share of the *total consolidated tape* is **low single digits (~2.5–3%)**; the "~15%" figure IEX markets is its share of *at-NBBO "stable lit" on-exchange* volume only — **do not** read it as tape share.
- **IEX Cloud (iexcloud.io REST/JSON) is permanently dead** (announced 2024-05-31, shut down 2024-08-31). Old token tutorials are useless. The only surviving free IEX paths are (a) raw TOPS/DEEP binary HIST and (b) redistributors (Alpaca; Databento's no-license-fee IEX dataset).
- **Free L2 / depth-of-book is reachable ONLY via raw IEX DEEP** (binary PCAP/multicast, no normal API). It is **not** available on Alpaca free or Finnhub at any tier. Alpaca equities are Level-1/BBO only — there is no equity order book on Alpaca at any price.
- **Finnhub historical OHLCV candles for US stocks moved to premium** (`/stock/candle` returns 403 "You don't have access to this resource" on free keys). On the free tier, OHLCV is reachable only via Alpaca IEX bars.
- Free real-time feeds **prohibit redistribution** — ship integration code, require each user to bring their own key, never cache/rebroadcast the live feed.

---

## 2. Provider profiles

Provider keys: `sec_edgar`, `finra`, `alpaca`, `finnhub`, `iex`, `yahoo`, `stooq`, `polygon`, `tiingo`, `alphavantage`, `tradier`, `openfigi`, `fred`, `cboe`, `exchange_directories`, `market_structure`, `free_ceiling`.

---

### 2.1 `sec_edgar` — SEC EDGAR (data.sec.gov + www.sec.gov + efts.sec.gov)

- **$0 access path:** Fully free, **no key, no signup, no card — ever** (US government public data). Only operational requirement: a descriptive `User-Agent` header. **Card required: NO.**
- **Base URLs:** `https://data.sec.gov`, `https://www.sec.gov`, `https://efts.sec.gov`
- **Auth:** No API key. **MANDATORY** descriptive `User-Agent: AppName contact@domain.com`. Missing/undeclared UA ⇒ **HTTP 403** ("Undeclared Automated Tool"). Verified: no-UA → 403; descriptive-UA → 200.
  - **[CORRECTED]** CORS: profile claimed "no CORS / server-side only" — **WRONG.** `data.sec.gov` returns `access-control-allow-origin: *` for simple GETs; browser-direct fetch of the JSON APIs works. (www.sec.gov Archives XML may differ.)

| Method | Path | Purpose | Key fields |
|---|---|---|---|
| GET | `data.sec.gov/submissions/CIK##########.json` | Per-company filing history + entity metadata (primary reference + filings index) | `cik, name, sic, tickers[], exchanges[], ein, lei, fiscalYearEnd, formerNames[]`; `filings.recent` is **COLUMNAR** parallel arrays: `accessionNumber[], filingDate[], reportDate[], form[], primaryDocument[], isXBRL[]`; older shards in `filings.files[]` |
| GET | `data.sec.gov/api/xbrl/companyfacts/CIK##########.json` | All XBRL facts for one company (best single fundamentals source) | `facts.{taxonomy}.{tag}.units.{unitKey}[].{start,end,val,accn,fy,fp,form,filed,frame}` (taxonomies: us-gaap, dei, srt, ifrs-full) |
| GET | `data.sec.gov/api/xbrl/companyconcept/CIK##########/{taxonomy}/{tag}.json` | Time series of one XBRL concept for one company | `units.{unitKey}[].{end,val,accn,fy,fp,form,filed}` |
| GET | `data.sec.gov/api/xbrl/frames/{taxonomy}/{tag}/{unit}/{period}.json` | One fact across ALL companies for a period (cross-section) | `data[].{accn,cik,entityName,val,start,end}`; period: `CY####`, `CY####Q#`, `CY####Q#I` (instantaneous) |
| GET | `efts.sec.gov/LATEST/search-index?q=&forms=&startdt=&enddt=&ciks=` | Elasticsearch full-text search over filing bodies (2001→present) | `hits.hits[]._source.{ciks,display_names,form,adsh,file_date,period_ending}` |
| GET | `www.sec.gov/files/company_tickers.json` | Ticker ↔ CIK ↔ title map (~10,400 entries, verified) | object keyed by index: `{cik_str (int, NOT zero-padded), ticker, title}` |
| GET | `www.sec.gov/files/company_tickers_exchange.json` | Same map + listing exchange, columnar | `fields:['cik','name','ticker','exchange'], data:[...]` |
| GET | `www.sec.gov/Archives/edgar/data/{cik}/{accn-no-dashes}/` (+ ownership/info-table XML) | Insider 3/4/5 + 13F **transaction detail** (discover via submissions; detail is in XML, NOT JSON) | Form 4 XML: `reportingOwner, transactionCode (P/S/A/M…), transactionShares, transactionPricePerShare`; 13F infoTable: `nameOfIssuer, cusip, value, sshPrnamt` |
| GET | `www.sec.gov/Archives/edgar/daily-index/xbrl/companyfacts.zip` / `.../bulkdata/submissions.zip` | Nightly whole-dataset bulk ZIPs (verified 1,385,797,907 B / 1,544,801,441 B; rebuilt ~3am ET) | per-CIK JSON identical to live endpoints |

- **Rate limits:** **10 req/s** hard cap **aggregate across all sec.gov hosts** (monitored for "equitable access"). No daily/monthly quota. Overrun → temporary IP block. Use a global token-bucket; prefer nightly bulk ZIPs for big pulls.
- **Real-time:** No WS/streaming. Submissions update <1 s; XBRL <1 min. Closest to live = "Latest Filings" RSS/Atom (`browse-edgar?action=getcurrent&output=atom`). **No price/OHLCV data at all.**
- **Canonical data types fed:** `instrument/reference`, `fundamental`, `filing`, `insider`, `holding_13f`. (`corp_actions` is **weak/misleading** — no structured feed; splits/divs/M&A only appear inside 8-K text.)
- **Historical depth:** Filings back to ~1994 (verified Apple shard 1994-01-26). XBRL facts from ~2009 (XBRL mandate). Full-text search 2001→present. Per-filing granularity (10-K/10-Q/8-K) keyed by fy/fp/period-end.
- **Python approach:** Raw `httpx`/`requests` with persistent session + descriptive UA + token-bucket limiter + backoff on 403/429. For bulk: nightly ZIPs. Unofficial libs: `edgartools`, `sec-edgar-api`, `sec-cik-mapper`, `sec-edgar-downloader` (borrow parsing only).
- **Gotchas:**
  - CIK must be **10-digit zero-padded** in data.sec.gov paths (`CIK0000320193`); `company_tickers.json` returns `cik_str` as plain int — you must left-pad. **#1 integration bug.**
  - `submissions.recent` is **columnar** (index-aligned arrays), not an array of objects.
  - Insider/13F **transaction detail is XML in Archives, NOT in the JSON APIs.**
  - **13F dollar `value` units changed** — historically THOUSANDS, **whole dollars effective Jan 3, 2023** (SEC amendment adopted 2022-06-23). Verify per filing or be off by 1000×.
  - XBRL restatements: same period can have multiple values across accessions — dedupe on `frame`/latest `filed`.
- **Verified docs:** sec.gov/search-filings/edgar-application-programming-interfaces; sec.gov/about/webmaster-frequently-asked-questions; sec.gov/edgar/search/efts-faq.html
- **Confidence:** high. *"One of the most accurate provider profiles reviewable; there is no premium tier — SEC cannot gate data behind payment."*

---

### 2.2 `finra` — FINRA public data (cdn.finra.org flat files + Query API Public credential)

- **$0 access path:** Two free paths — (1) **CDN flat-file downloads** (`cdn.finra.org`): nothing at all, anonymous HTTPS GET; (2) **Query API** with a free self-registered **"Public" credential** (OAuth2, no card). Firm-confidential data needs a paid Firm/Organization credential ($1,650/mo) — **not needed** for short interest / Reg SHO / threshold. **Card required: NO.**
- **Base URLs:** `cdn.finra.org`, `api.finra.org`, `ews.fip.finra.org` (OAuth token), `otce.finra.org`, `developer.finra.org`
- **Auth:** CDN = none. Query API = OAuth2 client_credentials: POST `ews.fip.finra.org/fip/rest/ews/oauth2/access_token?grant_type=client_credentials` with `Authorization: Basic base64(client_id:secret)` → Bearer token → `Authorization: Bearer <token>` on `api.finra.org`.
  - **[CORRECTED]** Token TTL: profile said ~30 min; docs example returns ~12h. **Plan to re-fetch on 401, not a fixed clock.**
  - **[CORRECTED]** 10 GB/month cap is misattributed — it ties to **paid** Firm/Org credentials, not the free Public credential. Real free constraint = the per-minute throttle.

| Method | Path | Purpose | Key fields |
|---|---|---|---|
| GET | `cdn.finra.org/equity/regsho/daily/{PREFIX}shvol{YYYYMMDD}.txt` (PREFIX: **CNMS**=consolidated, FNSQ/FNQC/FNYX/FNRA/FORF=per-facility) | Daily aggregated short-sale **volume** by symbol | pipe-delimited: `Date|Symbol|ShortVolume|ShortExemptVolume|TotalVolume|Market` |
| GET | `cdn.finra.org/equity/otcmarket/biweekly/shrt{YYYYMMDD}.csv` (settlement date; **pipe-delimited despite .csv**) | Bi-monthly consolidated **short interest** positions | `symbolCode|issueName|currentShortPositionQuantity|previousShortPositionQuantity|daysToCoverQuantity|changePercent|settlementDate|marketClassCode` |
| GET | `cdn.finra.org/equity/regsho/monthly/{PREFIX}sh{YYYYMM}[_N].zip` | Per-facility monthly **trade-level** short-sale transactions | ZIP of pipe-delimited per-trade records |
| GET/POST | `api.finra.org/data/group/OTCMarket/name/regShoDaily` (Bearer) | Reg SHO daily, server-side filterable | `reportingFacilityCode, securitiesInformationProcessorSymbolIdentifier, shortParQuantity, totalParQuantity, tradeReportDate` |
| GET/POST | `api.finra.org/data/group/OTCMarket/name/consolidatedShortInterest` (Bearer) | Queryable short interest | as CSV above |
| GET/POST | `api.finra.org/data/group/OTCMarket/name/weeklySummary` (Bearer) | OTC (ATS/non-ATS) weekly volume per MPID | per-week shares/trades/symbol/tier; 2-wk (Tier1) / 4-wk (Tier2) lag |

- **Rate limits:** CDN = no documented per-user limit (Cloudflare/CloudFront edge; be polite). Query API: **synchronous 1,200 req/min per IP; asynchronous 20 req/min per dataset per account**; record limit 5,000 sync / 100,000 async; offset max 500,000; **3 MB sync response-body cap** (add this).
- **Real-time:** None, no WS. EOD/batch. Reg SHO daily posts ≤6:00 PM ET same trade day (may re-post "Updated"). Short interest bi-monthly with publication lag. Threshold list daily.
- **Canonical data types fed:** `short_interest`, `trade` (short-sale *volume*/transaction files — NOT tick/OHLC), `instrument/reference` (threshold list).
- **Historical depth:** CNMS daily Reg SHO from **2018-08-01** (verified 20180801=200, 20180701=403). Consolidated short interest CSV verified from at least **2018-08-15** with full listed coverage. Pre-2018 daily / pre-2014 short interest live on `otce.finra.org` archives, NOT the CDN path.
- **Python approach:** For free data, **raw HTTP against cdn.finra.org**: `pandas.read_csv(io.StringIO(text), sep='|')`. Reserve the Query API (OAuth) for ad-hoc filtered pulls. `dlt` (dlthub) ships a FINRA source. Prefer CDN files for bulk (deterministic, no token churn).
- **Gotchas:**
  - **[CORRECTED — FALSE GOTCHA]** Profile (echoing FINRA's catalog) claimed listed securities only in the consolidated short-interest CSV "from June 2021." **Empirically refuted** — the **CDN download files have carried full NYSE/Nasdaq listed names since at least 2018**. The June-2021 note applies to the interactive grid/dissemination policy, not the CDN path. **Do not code a 2021 schema break.**
  - `.csv` short interest file is **pipe-delimited** (`sep='|'`).
  - Use **CNMS** for consolidated daily; FNSQ/FNQC/FNYX/FNRA/FORF are per-facility and must be summed (don't double-count).
  - Short interest ≠ short-sale **volume** (different datasets/cadence; commonly confused).
  - No files on non-trading days — build a trading-calendar-aware fetcher; tolerate 403/404 for holidays/not-yet-posted.
  - FINRA's threshold list = OTC equities; the NMS/listed threshold list is published by Nasdaq/NYSE — combine sources.
  - **Non-commercial use** under FINRA terms — fetch per-user, don't bundle/redistribute.
- **Verified docs:** finra.org/finra-data/browse-catalog/short-sale-volume-data; developer.finra.org/products/query-api
- **Confidence:** high. **No premium downgrade trap** — these are regulatory public datasets; nothing moved to premium.

---

### 2.3 `alpaca` — Alpaca Market Data (Basic plan)

- **$0 access path:** Free account at app.alpaca.markets; a **paper-trading account** is auto-created and grants the **Basic ($0)** market-data plan — no funding, no KYC, no brokerage approval just for data. Generate key+secret in dashboard (secret shown once). **Card required: NO.**
- **Base URLs:** `https://data.alpaca.markets` (REST), `wss://stream.data.alpaca.markets` (WS), `https://paper-api.alpaca.markets` (paper trading), `wss://stream.data.sandbox.alpaca.markets` (sandbox)
- **Auth:** REST headers `APCA-API-KEY-ID` + `APCA-API-SECRET-KEY`. WS: post-connect `{"action":"auth","key":"...","secret":"..."}`.

| Method | Path | Purpose | Key fields |
|---|---|---|---|
| GET | `data.alpaca.markets/v2/stocks/bars?symbols=&timeframe=&start=&end=&feed=iex` | OHLCV bars (1Min–12Month) | `bars{sym:[{t,o,h,l,c,v,n,vw}]}, next_page_token`; `adjustment=raw\|split\|dividend\|all` (default **raw**) |
| GET | `data.alpaca.markets/v2/stocks/trades?symbols=&feed=iex` | Tick trades (IEX free; SIP >15 min old) | `{t,x,p,s,c[],i,z}` |
| GET | `data.alpaca.markets/v2/stocks/quotes?symbols=&feed=iex` | BBO quotes (IEX BBO free) | `{t,ax,ap,as,bx,bp,bs,c[],z}` |
| GET | `data.alpaca.markets/v2/stocks/snapshots?symbols=&feed=iex` | latestTrade+latestQuote+min/daily/prevDaily bar | per-symbol composite |
| WSS | `wss://stream.data.alpaca.markets/v2/iex` | Real-time IEX trades/quotes/bars (paid: `/v2/sip`; free delayed: `/v2/delayed_sip`) | channels: trades, quotes, bars, dailyBars, updatedBars, statuses, lulds, imbalances |
| GET | `data.alpaca.markets/v1/corporate-actions?symbols=&types=&start=&end=` | Splits/dividends/mergers/spin-offs (free) | grouped by action type |
| GET | `data.alpaca.markets/v1beta1/options/bars` / `.../options/quotes/latest` | Option bars/quotes — **indicative (15-min-delayed) on free**; real OPRA paid | OCC symbols, `{t,o,h,l,c,v,n,vw}` / `{ax,ap,as,bx,bp,bs}` |

- **Rate limits:** Basic = **200 REST req/min PER ACCOUNT** (shared across keys/processes) → 429. WS free: **1 concurrent connection**, **IEX only**, **30-symbol cap on trades+quotes combined** (minute `bars` channel uncapped). REST pagination ≤10,000/page. Paid Algo Trader Plus: 10,000 req/min, removes the 30-symbol cap **but the 1-connection limit still applies on most plans.**
- **Real-time:** Yes, **IEX feed only** (~2.5–3% of consolidated volume; real-time, no delay). **15-minute SIP rule:** `latest` endpoints, snapshots, and any SIP query whose `end` is <15 min ago are blocked on free; data older than 15 min is fully free; IEX real-time has no delay.
- **Canonical data types fed:** `trade`, `quote` (Level-1/BBO only), `ohlcv`, `option_quote` (indicative only on free), `corp_action`.
  - **[CORRECTED — FABRICATED]** `l2_book` was listed for equities — **FALSE.** Alpaca equities are L1/BBO only; depth-of-book exists **only on the separate Crypto API**, never equities. **Remove l2_book.**
- **Historical depth:** Equities trades/quotes/bars from **2016-01-01** on Basic (subject to 15-min recency + IEX-only volume).
  - **[CORRECTED]** "IEX history shorter than SIP (~5 yr)" is unsupported — both feeds share the 2016 start; the IEX vs SIP difference is **volume coverage**, not time depth. Options history only from **Feb 2024**.
- **Python approach:** `pip install alpaca-py` (current; `alpaca-trade-api` deprecated). `StockHistoricalDataClient` + `feed=DataFeed.IEX`; `StockDataStream` for WS. Use `adjustment=all` for backtests (default is raw).
- **Gotchas:** default feed is `sip` — free must pass `feed=iex`. IEX prints only ≠ consolidated NBBO/VWAP. 200/min is per-account. Options = indicative + Feb-2024-only on free.
- **Verified docs:** docs.alpaca.markets/us/docs/about-market-data-api; .../real-time-stock-pricing-data; .../market-data-faq
- **Confidence:** high.

---

### 2.4 `finnhub` — Finnhub (finnhub.io)

- **$0 access path:** Free account at finnhub.io/register (email only) → API token shown instantly. No phone, no card, no approval. **Card required: NO.**
- **Base URLs:** `https://finnhub.io/api/v1`, `wss://ws.finnhub.io`
- **Auth:** `token` query param **or** `X-Finnhub-Token` header. WS: `wss://ws.finnhub.io?token=KEY`. (`x-finnhub-secret` fails; empty token → `{"error":"Please use an API key."}`; invalid token → 401 `{"error":"Invalid API key."}`.)

| Method | Path | Purpose | Key fields |
|---|---|---|---|
| WSS | `wss://ws.finnhub.io?token=KEY` | **FREE** real-time US trade stream | sub `{"type":"subscribe","symbol":"AAPL"}`; msg `{type:trade,data:[{s,p,v,t,c[]}]}`; `{type:ping}` keepalives |
| GET | `/quote?symbol=AAPL` | **FREE** real-time US quote snapshot | `c,d,dp,h,l,o,pc,t` — **NO bid/ask/size** (last + day OHLC + prev close only) |
| GET | `/stock/symbol?exchange=US` | **FREE** US symbol universe | `{symbol,displaySymbol,description,type,currency,mic,figi,isin}` |
| GET | `/search?q=apple` | **FREE** symbol search | `result[].{symbol,description,type}` |
| GET | `/stock/profile2?symbol=AAPL` | **FREE** company reference | `name,ticker,exchange,finnhubIndustry,ipo,marketCapitalization,shareOutstanding` |
| GET | `/stock/metric?symbol=AAPL&metric=all` | **FREE** basic fundamentals/ratios | `metric{peTTM,psTTM,pb,roeTTM,...}`, `series.annual/quarterly` |
| GET | `/stock/recommendation?symbol=AAPL` | **FREE** analyst recs | `{strongBuy,buy,hold,sell,strongSell,period}` |
| GET | `/stock/earnings?symbol=AAPL` | **FREE** EPS actual vs estimate | `{actual,estimate,period,surprise,surprisePercent}` |
| GET | `/calendar/earnings`, `/calendar/ipo` | **FREE** calendars | earnings/IPO dates + estimates |
| GET | `/stock/dividend2?symbol=AAPL&from=&to=` | Basic dividends (**verify per key**) | `{exDate,payDate,recordDate,amount,adjustedAmount,frequency}` |
| GET | `/stock/split?symbol=AAPL&from=&to=` | Splits (**verify per key**) | `{date,fromFactor,toFactor}` |
| GET | `/stock/candle?...` | **[MOVED TO PREMIUM]** historical OHLCV — **403 on free for US stocks** | — do NOT rely on this |

- **Rate limits:** **60 calls/min** (maintainer-confirmed) + a **~30 calls/sec** burst cap → 429. WS free: **~50 concurrent symbol subscriptions**.
- **Real-time:** Yes — real-time US trades over WS on free. `/quote` is a real-time snapshot (not streaming). Trade data flows only during US market hours.
- **Canonical data types fed:** `trade`, `quote`, `instrument/reference`, `fundamental`, `corp_action` (calendars + basic div/split), `filing`.
- **Historical depth:** No free OHLCV history (`/stock/candle` premium). Free history that exists: fundamentals series, earnings surprises, recommendation trends, basic div/split history, windowed company news.
- **Python approach:** `pip install finnhub-python`. Premium calls raise `FinnhubAPIException` 403. No WS helper in SDK — use `websocket-client` directly.
- **Gotchas:**
  - **[MOVED TO PREMIUM — biggest trap]** `/stock/candle` (historical OHLCV) is premium for US stocks (~since 2023). **A free engine cannot backfill candles from Finnhub** — aggregate the WS trade stream or use another provider.
  - **Free tier is US-only** — international symbols return "You don't have access to this resource" (same 403 message also signals premium gating; disambiguate by symbol).
  - `/quote` has **no NBBO / bid-ask** — NBBO needs premium.
  - Free/premium boundary has shifted repeatedly (dividends ~2020, candles ~2023) — **detect 403 at runtime and degrade gracefully**; re-verify `dividend2`/`split` on your own key.
- **Verified docs:** finnhub.io/docs/api; finnhub.io/docs/api/websocket-trades; GitHub Finnhub-API issues #122/#546
- **Confidence:** high.

---

### 2.5 `iex` — IEX Exchange HIST (historical pcap archive)

- **$0 access path:** Fully free, **no key, no account, no card**. Anonymous over HTTPS. Only obligation = HIST Terms of Use (attribution if you redistribute). **This is the IEX EXCHANGE's own historical archive — NOT the dead IEX Cloud, NOT the retired api.iextrading.com.** **Card required: NO.**
- **Base URLs:** `https://iextrading.com/api/1.0/hist` (index, JSON), `https://www.googleapis.com/download/storage/v1/b/iex/o/...` (per-file pcap.gz, anonymous, supports Range/206). **Dead/not-this:** `api.iextrading.com/1.0/*` (403, retired 2021-11-18), `iexcloud.io`/`cloud.iexapis.com` (shut down 2024-08-31).
- **Auth:** None.

| Method | Path | Purpose | Key fields |
|---|---|---|---|
| GET | `iextrading.com/api/1.0/hist` | Full catalog of daily pcap.gz files (verified 2400 date keys, 20161212→20260608) | **JSON OBJECT** keyed by `YYYYMMDD`; each entry `{link, date, feed (DEEP\|TOPS\|DPLS), version, protocol, size}` |
| GET | `iextrading.com/api/1.0/hist?date=YYYYMMDD` | Files for one day (incremental ingest) | **JSON ARRAY** of same entry shape — **handle both shapes** |
| GET | `www.googleapis.com/.../o/data%2Ffeeds%2F<date>%2F<file>.pcap.gz?...` (take `link` verbatim) | One day's full feed as gzip pcap of IEX-TP packets | TOPS → QuoteUpdate, TradeReport, OfficialPrice, TradingStatus; DEEP → + PriceLevelUpdate (L2 depth); DPLS → order-by-order |

- **Rate limits:** None enforced (no key). Real constraint is **bandwidth**: each daily file is **~13–16 GB COMPRESSED** per feed (verified 2026-06-08: DEEP=13.29 GB, TOPS=13.18 GB, DPLS=12.68 GB). A full day across all three ≈ 40 GB; a year of one feed = multi-TB. GCS links support Range (206) for resume/parallel.
- **Real-time:** **None free.** HIST is strictly **T+1** (prior trading day's pcap appears next morning).
  - **[CORRECTED]** Profile said "real-time is paid only" — IEX offers a one-time ~30-day free trial of Real-Time TOPS/DEEP to first-timers (still requires a market-data agreement, paid after). Also note third parties (e.g. Tiingo) repackage near-real-time IEX TOPS for free — out of scope for this key.
- **Canonical data types fed:** `trade`, `l2_book`, `quote` (**IEX-venue top-of-book only, NOT consolidated NBBO**), `instrument/reference`.
- **Historical depth:** ~9.5 years, **2016-12-12 → present** (2400 trading days; docs only *guarantee* the most recent 12 months — depth not contractually guaranteed). Tick-by-tick, nanosecond timestamps. **Only free full-depth (L2) US equity source** — but IEX-only (~2.5–3% of tape).
- **Python approach:** Two-step — (1) fetch index JSON with `httpx`/`requests`; (2) stream each `link` to disk (Range for resume) and parse with `iex-parser` (`from iex_parser import Parser, TOPS_1_6 / DEEP_1_0`) or `IEXTools`. **Match the file's `version` field to the parser constant.** Both parsers are stale (last ~2021–2022, Py ≤3.10) but the wire format is frozen — may need vendoring for 3.12+. **Do NOT** use `iexfinance`/IEX-Cloud SDKs (dead).
- **Gotchas:** Index single-date = ARRAY, no-arg = OBJECT. Files are huge — stream/filter incrementally. Binary spec-version-specific parser required. IEX-only ≠ consolidated. Attribution required: *"Data provided for free by IEX. By accessing or using IEX Historical Data, you agree to the IEX Historical Data Terms of Use."* No fundamentals/news/corp-actions/consolidated bars.
  - **[CORRECTED]** Download Content-Type varies (recent 2026 link = `application/octet-stream`; older 2017 link = `application/vnd.tcpdump.pcap`).
- **Verified docs:** iextrading.com/trading/market-data; iextrading.com/api/1.0/hist; pypi.org/project/iex-parser
- **Confidence:** high.

---

### 2.6 `yahoo` — Yahoo Finance via unofficial `yfinance`

- **$0 access path:** Fully free, **no key, no account** — `pip install yfinance`. The cost is **reliability, not money** (no SLA, Yahoo can break/throttle anytime; ToS restricts to personal/research). **Card required: NO.** Any "Yahoo Finance API key" product (RapidAPI/financeapi.net/YH Finance) is a paid reseller, NOT this path.
- **Base URLs:** `query1/query2.finance.yahoo.com`, `fc.yahoo.com` (cookie), `wss://streamer.finance.yahoo.com/?version=2`
- **Auth:** No user auth. Internal **cookie+crumb handshake** (GET fc.yahoo.com cookie → `/v1/test/getcrumb` → append `?crumb=`) + a browser-like `User-Agent` (else 401/429). yfinance automates this.

| Method | Path | Purpose | Key fields |
|---|---|---|---|
| GET | `query1.../v8/finance/chart/{symbol}?period1=&period2=&interval=&events=div,splits` | OHLCV bars + embedded div/splits | `timestamp[]`, `indicators.quote[0].{open,high,low,close,volume}`, `adjclose[0].adjclose`, `events.dividends/splits` |
| GET | `query2.../v10/finance/quoteSummary/{symbol}?modules=&crumb=` | info/holders/profile/statements modules | `financialData, defaultKeyStatistics, assetProfile, institutionOwnership, insiderTransactions, secFilings` |
| GET | `query2.../ws/fundamentals-timeseries/v1/finance/timeseries/{symbol}?type=&crumb=` | income/balance/cashflow statements | `result[].{meta.type[], {asOfDate,reportedValue.raw}}` |
| GET | `query1.../v7/finance/options/{symbol}[?date=]` | Options chain | `calls[]/puts[].{strike,lastPrice,bid,ask,volume,openInterest,impliedVolatility}` — **IV only, NO Greeks** |
| GET | `query1.../v7/finance/quote?symbols=&crumb=` | Delayed snapshot quotes (crumb-protected) | `{regularMarketPrice,bid,ask,marketCap,...}` |
| GET | `query1.../v1/finance/search?q=` | symbol/news search | `quotes[]`, `news[]` |
| WSS | `wss://streamer.finance.yahoo.com/?version=2` | Best-effort live stream (base64+protobuf) | `{id,price,time,dayVolume,...}` — indicative, not trade-grade |

- **Rate limits:** No published numbers. **Aggressive/looped calls → HTTP 429 / `YFRateLimitError` and IP soft-bans (minutes–hours)** — common in 2025–2026 even for light use; maintainers closed the rate-limit issue as Yahoo-side/not-planned. Cloud IPs (AWS/GCP) throttled harder. Harden: cache + backoff + slow pacing + `pip install yfinance[nospam]` (curl_cffi + requests_cache + requests_ratelimiter ~2 req/s); batch via `yf.download([...])`.
- **Real-time:** No true free real-time. REST quotes ~15-min delayed snapshots. WS exists but best-effort/indicative. No L2.
- **Canonical data types fed:** `ohlcv`, `quote`, `instrument/reference`, `corp_action` (full div/split history), `fundamental` (shallow), `insider`, `filing` (pointer list), `option_quote` (IV only).
  - **[CORRECTED]** `holdings_13f` is **overstated** — yfinance has **no 13F feature**; only holder *summary* tables (institutional/mutualfund/major holders), derived from Yahoo's site with known data-quality bugs. Treat as "holder summaries (low reliability)," not 13F.
  - **[CORRECTED]** `filing` (`sec_filings`) returns Yahoo's curated link list, not full filing content.
- **Historical depth:** Daily/weekly/monthly **very deep** (decades; AAPL daily to 1980). Intraday strict: **1m ≈ 7–8 days/request and ~30 days total**; 2m/5m/15m/30m/90m ≈ last 60 days; **[CORRECTED] 60m/1h actually ≈ last 730 days (~2 yr)** — the 60-day cap does NOT apply to hourly. Fundamentals shallow (~4 yr annual / ~4–5 quarters / TTM).
- **Python approach:** `pip install yfinance[nospam]` (latest 1.4.1). Pass a hardened cached/rate-limited session; prefer `yf.download([many], group_by='ticker', threads=True)`. Pin a known-good version; wrap every call in retry/backoff.
- **Gotchas:** unofficial/unstable, no SLA. Rate limiting is the #1 pain. `.info` is heavy/fragile (prefer `.fast_info`). `auto_adjust=True` by default in recent versions — set `auto_adjust=False` for raw Close + separate Adj Close. Options have no Greeks (compute yourself). Best-effort dev/research source, **not production-guaranteed**.
- **Verified docs:** ranaroussi.github.io/yfinance; pypi.org/project/yfinance
- **Confidence:** high.

---

### 2.7 `stooq` — Stooq (stooq.com)

- **$0 access path:** Free, **no paid tier, no account, no card** — but **no longer key-less** (as of ~Mar 2026). The friction is **anti-bot, not money**. **Card required: NO.**
- **Base URLs:** `stooq.com`, `stooq.pl` (mirror), `stooq.com/q/d/l/` (single-symbol CSV), `stooq.com/q/l/` (last quote), `stooq.com/db/h/` (bulk ZIPs)
- **Auth:** No traditional auth, but **two anti-bot walls** (both VERIFIED LIVE): (1) the whole site fronts a **JavaScript SHA-256 proof-of-work** challenge — a raw client gets an HTML "This site requires JavaScript to verify your browser" page with a `/__verify` POST (find n s.t. `sha256(c+n)` starts with 4 zeros) that sets an `auth` cookie; (2) `/q/d/l/` additionally requires an **`&apikey=` token** obtained by solving a visual CAPTCHA at `/q/d/?s=SYMBOL&get_apikey` (per-CAPTCHA, not per-account). Bulk ZIPs require an in-browser CAPTCHA.

| Method | Path | Purpose | Key fields |
|---|---|---|---|
| GET | `stooq.com/q/d/l/?s={sym}&i={d\|w\|m\|q\|y}&d1=&d2=&apikey=KEY` | Single-symbol historical CSV | `Date,Open,High,Low,Close,Volume`; US symbols `.us` suffix; indices `^` prefix (`^spx`) |
| GET (browser+CAPTCHA) | `stooq.com/q/d/?s={sym}&get_apikey` | Obtain apikey (human-only) | link with `&apikey=<32 hex>` |
| GET | `stooq.com/q/l/?s={sym}&f=sd2t2ohlcvn&h&e=csv` | Last/EOD snapshot quote | `s,d2,t2,o,h,l,c,v,n` |
| GET (browser+CAPTCHA) | `stooq.com/db/h/` → `d_us_txt.zip` etc. | Bulk market history ZIP | `<TICKER>,<PER>,<DATE>,<TIME>,<OPEN>,<HIGH>,<LOW>,<CLOSE>,<VOL>,<OPENINT>` |

- **Rate limits:** Low **undocumented daily quota** (community: low tens, unverified) — over-limit returns plaintext `"Exceeded the daily hits limit"` instead of CSV. Throttle 1–2/s, cache, use bulk ZIPs for backfill.
- **Real-time:** No WS/streaming. CSV/REST only; EOD/delayed.
- **Canonical data types fed:** `ohlcv`, `index` (`^`-prefixed; S&P/DJ carry personal-non-commercial license restrictions), `instrument/reference` (light).
  - **[CORRECTED]** `macro` is **overstated** — Stooq is a price provider with only a thin/incidental macro section; not a FRED substitute. Demote/qualify.
- **Historical depth:** Deep daily — AAPL ~10,518 rows back to ~1984 (plausible, not re-verified this session due to PoW wall). d/w/m/q/y for per-symbol. Intraday shallow: hourly ~1400 pts (~9 mo), 5-min ~2000 pts (~1 mo). No tick.
- **Python approach:** **`pandas-datareader` 'stooq' reader is BROKEN** (returns the apikey HTML; open issue pydata/pandas-datareader#1012). Recommended: raw HTTP — solve the SHA-256 PoW, POST `/__verify` for the `auth` cookie (persistent session), then GET `/q/d/l/?...&apikey=KEY`, `pandas.read_csv`. For backfill, prefer manual browser-CAPTCHA bulk ZIPs.
- **Gotchas:** PoW + CAPTCHA-gated apikey + low daily quota make headless automation materially harder than a typical free CSV API. CSV uses `YYYY-MM-DD`; bulk `.txt` uses `YYYYMMDD`. Bulk US daily ZIP ~330–510 MB compressed (>1.4 GB unzipped); dirs split at 2000 symbols. **Redistribution restricted** (Stooq terms — fetch per-user, don't bundle). S&P/DJ index license = personal non-commercial.
- **Verified docs:** stooq.com/db/h/; stooq.com/terms.html; github.com/pydata/pandas-datareader/issues/1012
- **Confidence:** high.

---

### 2.8 `polygon` — Polygon.io / Massive.com (Stocks Basic free tier)

- **$0 access path:** Free account at massive.com/signup (formerly polygon.io/signup) → generate key. **No credit card.** Free "Stocks Basic": **5 API calls/min, End-of-Day only, 2 years history, 100% US coverage, REST only.** **Card required: NO.**
- **Base URLs:** `https://api.polygon.io` (legacy, fully operational in parallel), `https://api.massive.com` (new canonical) — both serve identical routes, accept the same keys.
- **Auth:** API key, two interchangeable ways (both verified): `?apiKey=KEY` **or** `Authorization: Bearer KEY`. Missing → 401 `{"status":"ERROR","error":"API Key was not provided"}`.

| Method | Path | Purpose | Key fields |
|---|---|---|---|
| GET | `/v2/aggs/ticker/{t}/range/{mult}/{timespan}/{from}/{to}` | OHLCV custom bars (EOD on free, ~2 yr) | `results[].{c,o,h,l,v,vw,t (ms),n}`; `adjusted` default true; `limit` max 50000 |
| GET | `/v2/aggs/grouped/locale/us/market/stocks/{date}` | **All US tickers for one date in one call** (best for 5/min budget) | `results[].{T,c,o,h,l,v,vw,t,n}` |
| GET | `/v2/aggs/ticker/{t}/prev` | Previous-day bar | `results[].{T,c,o,h,l,v,vw,t,n}` |
| GET | `/v3/reference/tickers` | Ticker list/search | `results[].{ticker,name,primary_exchange,type,active,cik,composite_figi,share_class_figi}` |
| GET | `/v3/reference/tickers/{ticker}` | Ticker overview/details | `+ market_cap, share_class_shares_outstanding, sic_code, list_date, address` |
| GET | `/v3/reference/splits` (legacy) / `/stocks/v1/splits` (new) | Stock splits | legacy `{ticker,execution_date,split_from,split_to}`; new + `adjustment_type, historical_adjustment_factor` |
| GET | `/v3/reference/dividends` (legacy) / `/stocks/v1/dividends` (new) | Cash dividends | legacy `{ticker,ex_dividend_date,cash_amount,frequency,dividend_type}`; new + `distribution_type, split_adjusted_cash_amount` |

- **Rate limits:** Free = **5 req/min** → 429 (~7,200/day theoretical; no stated daily cap). Paid removes daily cap, raises per-minute (Starter $29 / Developer / Advanced $199; market-data limit 10,000/min on Advanced).
- **Real-time:** **NONE on free — End-of-Day only** (NOT even 15-min delayed; 15-min starts at paid Starter, full real-time at Advanced). **No WebSocket, no trades/quotes/snapshot endpoints on free.**
- **Canonical data types fed:** `ohlcv` (EOD), `instrument/reference`, `corp_action` (splits + dividends).
- **Historical depth:** **2 years**, minute-level granularity available within that window (but delivered EOD/refreshed after close — minute bars are there, just not live). Reference/corp-action records carry full history (not 2-yr-capped). Paid extends: Starter 5 yr, Developer 10 yr, Advanced 20+ yr.
- **Python approach:** **[CORRECTED]** Current official SDK is `pip install massive` / `from massive import RESTClient` (repo moved to github.com/massive-com/client-python). The legacy `polygon-api-client` / `from polygon import RESTClient` still works. For an engine: raw `httpx` + **5-req/min token bucket**, preferring grouped-daily to maximize coverage per call.
- **Gotchas:** **Free = EOD only** (don't advertise intraday/live). **5/min is brutal for per-ticker loops** — use grouped-daily to backfill 2 yr across the whole market. Corp-action endpoints have **two path families** (legacy v3 + new `/stocks/v1`) with different field shapes — pin one. Aggregates default `adjusted=true`; timestamps are **ms**. Pre-2yr bars simply don't return — empty ≠ error.
- **Verified docs:** massive.com/pricing; massive.com/knowledge-base; massive.com/docs/rest/stocks; pypi.org/project/polygon-api-client. (Live probes 2026-06-09: all routes 401 without key on both hosts; both auth methods parsed.)
- **Confidence:** high.

---

### 2.9 `tiingo`

- **$0 access path:** Free account (email+password) at api.tiingo.com → single API token authenticates ALL endpoints (stocks/IEX/crypto/forex). **No credit card.** **News and Fundamentals are PAID.** **Card required: NO.**
- **Base URLs:** `https://api.tiingo.com`, `wss://api.tiingo.com`, `https://apimedia.tiingo.com`
- **Auth:** `Authorization: Token <APIKEY>` (literal `Token ` prefix, **not Bearer**) **or** `?token=`. WS: token as `authToken` in subscribe message.

| Method | Path | Purpose | Key fields |
|---|---|---|---|
| GET | `/tiingo/daily/{ticker}/prices?startDate=&endDate=&resampleFreq=` | EOD OHLCV + adjusted + inline div/split | `date,open,high,low,close,volume,adjOpen,adjHigh,adjLow,adjClose,adjVolume,divCash,splitFactor` |
| GET | `/tiingo/daily/{ticker}` | Ticker metadata | `ticker,name,exchangeCode,startDate,endDate` |
| GET | `apimedia.tiingo.com/docs/tiingo/daily/supported_tickers.zip` | Full symbol universe (**no token needed**) | `ticker,exchange,assetType,priceCurrency,startDate,endDate` |
| GET | `/iex/{ticker}/prices?resampleFreq=1min` | Intraday bars (real-time **derived** unless IEX agreement signed) | `date,open,high,low,close,volume` |
| GET | `/iex/?tickers=` | Real-time top-of-book snapshot | `ticker,last,tngoLast,bidPrice,bidSize,askPrice,askSize,volume` |
| WSS | `wss://api.tiingo.com` (endpoint=iex/fx/crypto) | Streaming real-time | sub `{eventName:subscribe, authorization:token, eventData:{thresholdLevel,tickers}}` |
| GET | `/tiingo/crypto/prices`, `/tiingo/fx/{ticker}/prices` | Crypto/FX history+intraday (free) | nested `priceData[]` |
| GET | `/tiingo/news` | **[PREMIUM]** — 403 on free | requires paid News/Power |
| GET | `/tiingo/fundamentals/{ticker}/{daily\|statements\|definitions}` | **[PREMIUM]** — paid add-on | — |

- **Rate limits (free/Starter):** **50 req/hour, 1,000 req/day, 500 unique symbols/month, 1 GB/month** — whichever you hit first → 429. **The 500-unique-symbols/month cap is the binding constraint** for broad universes.
  - **[CORRECTED]** Power individual plan is **$30/mo** (not $10); Power limits are ~10,000 req/hr, 100,000 req/day, ~40 GB/mo (not 100 GB).
- **Real-time:** Yes — WS (iex/fx/crypto). **IEX caveat (since 2025-02-01):** full IEX TOPS requires you to sign a market-data agreement directly with IEX; otherwise Tiingo serves a real-time **derived reference price** (calculated, not official TOPS) at no extra cost. EOD/daily is REST-only.
- **Canonical data types fed:** `ohlcv` (EOD + IEX intraday), `quote`/`trade` (IEX, derived-price caveat), `corp_action` (free **only** as inline `divCash`+`splitFactor` in EOD rows — dedicated `/corporate-actions/dividends` + `/splits` + yield data are **paid**), `instrument/reference`, plus crypto/fx.
- **Historical depth:** **[CORRECTED]** EOD **50+ years** back to ~1962 for long-lived US tickers (profile understated as "30+"). IEX intraday ~couple years. Crypto/FX multi-year.
- **Python approach:** `pip install tiingo[pandas]` (hydrosquall/tiingo-python) for quick pulls; raw HTTP for high-throughput (CSV streaming, column selection, async). Header form `Authorization: Token <key>`. Build the universe from `supported_tickers.zip` (no token, saves quota).
- **Gotchas:** 500-symbols/month is the real ceiling. News + Fundamentals = paid (403). IEX free = derived price unless you sign the IEX agreement. Token prefix is `Token `, not `Bearer`. EOD endpoint returns latest single bar if no date range. Crypto/FX nested shape ≠ flat stock array. No card for free, but ToS §9.2 card language is broader than "paid only."
- **Verified docs:** tiingo.com/documentation; github.com/hydrosquall/tiingo-python
- **Confidence:** high.

---

### 2.10 `alphavantage`

- **$0 access path:** Free key at alphavantage.co/support/#api-key (email + user category). **No credit card.** Hard-capped **25 requests/DAY + 5/min**. **Card required: NO.**
- **Base URL:** `https://www.alphavantage.co/query`
- **Auth:** `apikey` query param (cleartext URL → use HTTPS). `apikey=demo` works only for docs example symbols (mostly IBM).

| Method | Path (`function=`) | Purpose | Notes / key fields |
|---|---|---|---|
| GET | `TIME_SERIES_DAILY&outputsize=compact` | Raw daily OHLCV | **FREE only for compact (~100 bars)**; `outputsize=full` is **[PREMIUM]** |
| GET | `TIME_SERIES_DAILY_ADJUSTED` | Adjusted daily + div + split coef | **[PREMIUM]** — only built-in adjusted-daily source |
| GET | `TIME_SERIES_WEEKLY_ADJUSTED` / `MONTHLY_ADJUSTED` | **FREE** full-history weekly/monthly (20+ yr, always full) | adjusted close + dividend; **free long-history workaround** |
| GET | `TIME_SERIES_INTRADAY&interval=&outputsize=compact` | Intraday bars | compact/delayed reachable free; realtime/15-min/full history **premium** |
| GET | `GLOBAL_QUOTE` | **FREE** quote snapshot (EOD/prev-close on free) | `05. price, 08. previous close, ...` |
| GET | `SYMBOL_SEARCH`, `LISTING_STATUS`, `MARKET_STATUS` | **FREE** reference; LISTING_STATUS returns **CSV**, survivorship-bias universe (active/delisted, date ~back to 2010) | `symbol,name,exchange,assetType,ipoDate,delistingDate,status` |
| GET | `OVERVIEW` | **FREE** (doc-tagged "Trending Premium" but returns data on free keys) | ratios, sector, marketcap, sharesOutstanding |
| GET | `INCOME_STATEMENT`, `BALANCE_SHEET`, `CASH_FLOW`, `EARNINGS` | **FREE** fundamentals (~5 yr annual / ~20 quarters) | `annualReports[]/quarterlyReports[]` |
| GET | `REAL_GDP, CPI, TREASURY_YIELD, FEDERAL_FUNDS_RATE, UNEMPLOYMENT, NONFARM_PAYROLL, ...` | **FREE** US macro | `{name,interval,unit,data[]{date,value}}` |

- **Rate limits:** **25 requests/DAY + 5/min** (both enforced) — down from 500→100→25/day. ~1 fully-profiled ticker exhausts the daily budget. **Over-limit / premium / errors come back as HTTP 200 with a `Note`/`Information`/`Error Message` JSON body — you MUST parse the body, not the status code.**
- **Real-time:** No WS/streaming, REST only. Real-time + 15-min-delayed are premium (exchange-licensed, `entitlement=` param). Free is effectively EOD.
- **Canonical data types fed:** `ohlcv` (free: daily compact + weekly/monthly full), `quote` (EOD only), `instrument/reference`, `fundamental`, `corp_action` (free only indirectly via weekly/monthly `_ADJUSTED` dividend amounts — inline split/div-adjusted daily is premium), `macro`.
  - **[CORRECTED]** `trades` is NOT free (no tick feed). `index` historical is **[PREMIUM]**. Remove both from free capability.
- **Historical depth:** Daily full (20+ yr) = premium; free daily = ~100 bars. Free long-history = weekly/monthly `_ADJUSTED` (20+ yr). Fundamentals ~5 yr. Macro multi-decade.
- **Python approach:** No official SDK. **Raw HTTP** with `requests`/`httpx`; `datatype=csv` + `pandas.read_csv` for LISTING_STATUS. **Treat HTTP 200 + `Note`/`Information`/`Error Message` as throttle/error.** Throttle ≤5/min, ≤25/day, cache. Community `alpha_vantage` lib fine for scripts.
- **Gotchas:** 25/day makes it impractical for bulk free backfill. `TIME_SERIES_DAILY` is **unadjusted**. Free Cash Flow not a direct field (derive `operatingCashflow − capitalExpenditures`). Numeric fields arrive as strings / `"None"` — coerce. Key in URL — HTTPS, don't log full URLs.
- **Verified docs:** alphavantage.co/documentation; alphavantage.co/premium; macroption.com/alpha-vantage-api-limits
- **Confidence:** high.

---

### 2.11 `tradier` — Sandbox (delayed) market data

- **$0 access path:** Free developer account at developer.tradier.com (email) → **Sandbox Access Token** unlocks **15-min-delayed** US equity + options market data + paper trading. **No card, no funded account, no SSN.** **Card required: NO.**
- **Base URLs:** `https://sandbox.tradier.com/v1` (FREE delayed), `https://api.tradier.com/v1` (production/real-time, brokerage), `wss://ws.tradier.com/v1` + `https://stream.tradier.com/v1` (streaming — **PRODUCTION ONLY**)
- **Auth:** `Authorization: Bearer <SANDBOX_TOKEN>` + `Accept: application/json`. **Token must match host** (sandbox token → sandbox host; using it on api.tradier.com → 401).

| Method | Path | Purpose | Key fields |
|---|---|---|---|
| GET/POST | `/markets/quotes?symbols=&greeks=` | Delayed quotes (equity/option/index) | `symbol,last,bid,ask,bidsize,asksize,volume,open,high,low,close,prevclose,week_52_high/low,type` |
| GET | `/markets/options/chains?symbol=&expiration=&greeks=true` | Option chain at one expiration | per-contract `strike,expiration_date,option_type,bid,ask,last,volume,open_interest`; `greeks{delta,gamma,theta,vega,rho,bid_iv,mid_iv}` (ORATS, ~hourly) |
| GET | `/markets/options/expirations?symbol=&includeAllRoots=true&strikes=true` | Valid expirations (+strikes) | `expirations.date[]`, `strikes.strike[]` |
| GET | `/markets/options/strikes?symbol=&expiration=` | Strikes for expiry | `strikes.strike[]` |
| GET | `/markets/lookup?q=&types=&exchanges=` | Security search | `securities.security[].{symbol,exchange,type,description}` |
| GET | `/markets/history?symbol=&interval=daily&start=&end=` | Daily/weekly/monthly OHLCV (full lifetime) | `history.day[].{date,open,high,low,close,volume}` |
| GET | `/markets/timesales?symbol=&interval=5min&start=&end=&session_filter=` | Intraday bars (shallow recent window) | `series.data[].{time,timestamp,price,open,high,low,close,volume,vwap}` |
| GET | `/markets/clock`, `/markets/calendar?month=&year=` | Market state + calendar | `state, next_change`; `days.day[].{date,status,open,close}` |

- **Rate limits:** Per token, rolling 1-min: Sandbox Market Data **60/min** (prod 120), Standard 60/min, Trading 60/min. Headers: `X-Ratelimit-Allowed/Used/Available/Expiry`.
- **Real-time:** **None free** — sandbox is **15-min delayed**. **No streaming on free** (FAQ verbatim: "we do not offer a delayed streaming endpoint for paper trading"); WS/HTTP streaming are production/brokerage-only. Sandbox = REST polling only.
- **Canonical data types fed:** `quote`, `ohlcv`, `option_quote`, `instrument/reference`.
  - **[CORRECTED]** **Greeks/IV are NOT confirmed free on sandbox** — official Market Data table lists Sandbox = "Not Available" for Greeks (Production = Hourly). The `greeks=true` param is documented without env restriction and some community usage reports it populating, but **do not rely on sandbox Greeks.** Treat as best-effort/unsupported.
  - **[CORRECTED]** `index` quotes are **production-only** per the same table — remove `index` from free.
- **Historical depth:** `/markets/history` daily/weekly/monthly covers a security's **full listed lifetime** (the "~30 years" figure is marketing, unverified). `/markets/timesales` intraday is a **shallow recent window** only (days–weeks). All sandbox data 15-min delayed (historical EOD bars unaffected).
- **Python approach:** No official SDK. **Raw HTTP** (`requests`/`httpx`) with Bearer + Accept headers; env switch between sandbox/prod base URLs; own 60/min limiter. Community `uvatradier` (active, lower confidence on "maintained"); `tradier-python` is stale (2021).
- **Gotchas:** sandbox is 15-min delayed, never real-time. No streaming free. Token must match host. **Single vs multi-symbol JSON shape:** `quotes.quote` is an OBJECT for one symbol, a LIST for many — normalize both (same for `options.option`). `includeAllRoots` default false (pass true for weeklys). Production/real-time needs a funded brokerage **or** a ~$10/mo data-only path.
- **Verified docs:** docs.tradier.com/docs/market-data; docs.tradier.com/docs/streaming-data; docs.tradier.com/docs/rate-limiting
- **Confidence:** high.

---

### 2.12 `openfigi` — OpenFIGI (Bloomberg/OMG symbology mapping)

- **$0 access path:** **Fully free for production.** (1) zero-signup keyless tier; (2) free API key (email+password at openfigi.com/user/signup) → 10× higher limits. **No card, free forever.** **Card required: NO.**
- **Base URLs:** `https://api.openfigi.com`, `https://api.openfigi.com/v3`
- **Auth:** No auth (keyless tier) **or** header `X-OPENFIGI-APIKEY: <key>`. No OAuth/Bearer. `Content-Type: application/json` on POST.

| Method | Path | Purpose | Key fields |
|---|---|---|---|
| POST | `/v3/mapping` | **Core** — map identifiers (ticker/CUSIP/ISIN/SEDOL/FIGI) → FIGIs + metadata. Body = **JSON ARRAY of jobs**; response = parallel array | request job `{idType (req), idValue (req), exchCode?, micCode?, currency?, securityType?}`; response `{data:[{figi,name,ticker,exchCode,compositeFIGI,securityType,marketSector,shareClassFIGI,securityType2,securityDescription}]}` / `{warning:'No identifier found.'}` / `{error}` |
| GET | `/v3/mapping/values/{key}` | Valid enum values (idType, exchCode, micCode, currency, securityType, ...) | `{values:[...]}` (**[CORRECTED]** idType returns **28** live values, not 29 — includes TICKER, ID_CUSIP, ID_ISIN, ID_SEDOL, ID_BB_GLOBAL, COMPOSITE_ID_BB_GLOBAL, ID_BB_GLOBAL_SHARE_CLASS_LEVEL, OCC_SYMBOL, OPRA_SYMBOL) |
| POST | `/v3/search` | Keyword search (no clean identifier) | `{data:[...rows...], next}` (cursor → `start`) |
| POST | `/v3/filter` | Like search + total count, alphabetical paging | `{data:[...], next, total}` |

- **Rate limits (verified verbatim vs docs):** Mapping **without key = 25/min, 10 jobs/request**; **with key = 25 per 6 seconds (sliding window) = 250/min, 100 jobs/request**. Search/Filter without key = 5/min; with key = 20/min. Pagination 100/page, 15,000 max results, 150 pages. 429 + `ratelimit-limit/remaining/reset` headers. **No daily/weekly/monthly cap.**
- **Real-time:** None — reference/symbology only, no prices/quotes/trades, no WS.
- **Canonical data types fed:** `instrument/reference` only.
- **Historical depth:** N/A — point-in-time directory, no time series, no as-of-date. **FIGIs are permanent/never reused** → stable join key over time.
- **Python approach:** No official SDK; **raw `requests`/`httpx`** is cleanest (single POST with JSON array). Pace **~25 jobs every 6s** with a key (sliding window — bursting 250 then sleeping → 429). Community `pyopenfigi` exists but is stale (v0.1.0, 2023).
- **Gotchas:** POST with **JSON array** body (not GET); inspect each parallel array element (`data`/`warning`/`error`). Batch to 10 (keyless) / 100 (keyed) jobs. One `idValue` maps to **many FIGIs** (per exchange/composite/share-class) — disambiguate with `exchCode`/`micCode`/`currency`, or collapse with `compositeFIGI`/`shareClassFIGI`; for US equities filter `exchCode='US'`. CUSIP→FIGI works as **input**; proprietary IDs may not be returned as **output** (CUSIP licensing). **Get the free key** for any real ingestion (25,000 ids/min keyed vs 250 keyless).
- **Verified docs:** openfigi.com/api/documentation; openfigi.com/about/faq; github.com/OpenFIGI/api-examples
- **Confidence:** high. *No market-data tier exists to gate — nothing could move to premium.*

---

### 2.13 `fred` — FRED (Federal Reserve Bank of St. Louis)

- **$0 access path:** Free account (email+password) at fredaccount.stlouisfed.org → free 32-char API key. **No credit card.** All 800k+ series free. **Card required: NO.**
- **Base URLs:** `https://api.stlouisfed.org/fred/` (v1), `https://api.stlouisfed.org/fred/v2/` (bulk, launched 2025-11-04), `https://api.stlouisfed.org/geofred/`
- **Auth:** v1 = `api_key` query param. v2 = `Authorization: Bearer <key>` header. **Missing/invalid key → HTTP 400** (not 401) with `{error_code,error_message}` ("The value for variable api_key is not registered."). HTTPS only; CORS enabled.

| Method | Path | Purpose | Key fields |
|---|---|---|---|
| GET | `/fred/series/observations?series_id=&file_type=json` | **Core** — time-series values | `observations[].{date, value (string; missing='.')}`; params: `observation_start/end, units (lin/chg/pch/pc1/...), frequency+aggregation_method, limit (max 100000), realtime_start/end, vintage_dates` |
| GET | `/fred/series?series_id=&file_type=json` | Series metadata | `seriess[].{id,title,observation_start,observation_end,frequency,units,seasonal_adjustment,last_updated,popularity}` |
| GET | `/fred/series/search?search_text=&file_type=json` | Full-text discovery | `seriess[]` + `group_popularity` |
| GET | `/fred/releases`, `/fred/category`, `/fred/sources`, `/fred/tags` | Browse releases/taxonomy/sources/tags | reference catalog |
| GET | `/fred/v2/release/observations?release_id=` (Bearer) | **Bulk** — entire history of ALL series in a release | series objects + `observations[]`, cursor `next_cursor` |

- **Rate limits:** **120 requests/min per key** → 429. No published daily cap. ~2 req/s; back off ~20s on 429.
- **Real-time:** No WS/streaming, REST poll only. **Not a market-data feed.** Equity index series (SP500, NASDAQCOM, DJIA) are **EOD close values, once/business day** (next-morning). "Real-time" in FRED = **ALFRED data-vintage tracking** (`realtime_start/end`), NOT latency.
- **Canonical data types fed:** `macro`, `index` (EOD index levels), `instrument/reference` (catalog).
- **Historical depth:** Deep, series-dependent. DGS10 from 1962, NASDAQCOM from 1971, GDP/CPIAUCSL ~1947, UNRATE 1948.
  - **[CORRECTED — material]** **SP500 AND DJIA are BOTH capped to a rolling ~10-year daily window** (S&P Dow Jones Indices license). Profile wrongly called DJIA "long-history." **NASDAQCOM (1971+) is the ONLY deep-history headline index** — a backfiller relying on DJIA pre-2016 gets nothing.
- **Python approach:** `pip install fredapi` (0.5.2; wraps v1, returns pandas) or `pyfredapi`. For v2 bulk: raw HTTP with Bearer + follow `next_cursor`. For an engine: raw `httpx` against v1 with `file_type=json`; v2 raw HTTP only for whole-release bulk.
- **Gotchas:** Not a market-data vendor (no intraday/tick/quotes/L2). **SP500 + DJIA = rolling 10-yr only.** Every value is a **string**; missing = `'.'`. **Default `file_type` is XML — always pass `file_type=json`.** Two auth styles (v1 query param vs v2 Bearer) — don't mix. No per-company fundamentals / single-name OHLCV / corp-actions / options.
- **Verified docs:** fred.stlouisfed.org/docs/api/fred; .../series_observations.html; .../v2/release_observations.html; .../errors.html
- **Confidence:** high.

---

### 2.14 `cboe` — Cboe free public CDN (cdn.cboe.com) delayed quotes + reference

- **$0 access path:** Fully free, **no key, no account** — public Fastly/S3-backed CDN. (The paid Cboe All Access API / DataShop are out of scope.) **Card required: NO.** Verified live 2026-06-09: all 14 endpoints 200 with no key/UA.
- **Base URLs:** `https://cdn.cboe.com/api/global/`, `https://cdn.cboe.com/resources/`, `https://www.cboe.com/us/options/symboldir/`, `https://www.cboe.com/us/options/market_statistics/historical_data/`
- **Auth:** None (anonymous HTTPS GET). A browser UA is belt-and-suspenders, not required.

| Method | Path | Purpose | Key fields |
|---|---|---|---|
| GET | `/api/global/delayed_quotes/options/{SYMBOL}.json` (equities bare `AAPL`; **indices underscore-prefixed `_VIX`,`_SPX`,`_NDX`,`_RUT`**) | Delayed option chain + underlying snapshot (primary free options) | `data.{security_type,current_price (15-min delayed LEVEL),bid,ask,iv30}`; `data.options[].{option (OCC),bid,ask,iv,open_interest,volume,delta,gamma,vega,theta,rho,theo,last_trade_price}` (AAPL ~3722, _SPX ~31296) |
| GET | `/api/global/delayed_quotes/charts/historical/{SYMBOL}.json` | EOD daily OHLCV history | `data[].{date,open,high,low,close,volume}`; VIX to **1990-01-02** (~9202 rows), AAPL to 2004 (~5643) |
| GET | `/api/global/delayed_quotes/charts/intraday/{SYMBOL}.json` | 1-min bars (most recent completed session, ~389) | `data[].{datetime,price{o,h,l,c},volume{stock_volume,calls_volume,puts_volume,total_options_volume}}` |
| GET | `/api/global/us_indices/definitions/all_indices.json` | All ~2025 index defs + delay-per-index | top-level **raw JSON array**: `{index_symbol,name,mkt_data_delay (15 or 0),...}` (2018 of 2025 = 15-min) |
| GET | `/api/global/delayed_quotes/symbol_book/symbol-book.json` | Every optionable symbol (~34,928) | `{name,company_name}` |
| GET | `/api/global/delayed_quotes/symbol_book/futures-roots.json` | Cboe futures roots (14) | `{future_root,underlying,family,name}` |
| GET | `/api/global/us_indices/definitions/GlobalIndices.csv` | CSV index master | `Symbol,Description,...` |
| GET | `www.cboe.com/us/options/symboldir/equity_index_options/?download=csv` (**302 → follow**) | Optionable company→ticker CSV | `Company Name,Stock Symbol,DPM Name,...` |

- **Rate limits:** No published numeric RPM. **BUT Cboe ToS (on the delayed_quotes pages, verbatim) PROHIBIT auto-extraction/scraping of delayed quote tables and threaten IP blocks.** Cache aggressively (OpenBB caches index JSON 1 day), low frequency, real UA, attribution "Data Delayed 15 minutes, source Cboe."
- **Real-time:** No WS/streaming. **15-min delayed** (2018 of 2025 indices; only 7 real-time). Snapshot mixes delayed intraday underlying with prior-session option last-trade.
- **Canonical data types fed:** `option_quote` (delayed snapshot chains + Greeks/IV, **point-in-time only**), `index` (level via `data.current_price`), `ohlcv` (daily history + 1-min intraday), `quote`, `instrument/reference`.
- **Historical depth:** Daily OHLC: VIX/indices to 1990; equities to ~2004. 1-min intraday: last completed session only. **Option chains are snapshot-only — NO free historical option chains/Greeks** (historical options = paid DataShop). Historical options **volume** (counts) free via website form ~back to Sept 2019.
- **Python approach:** Raw `httpx`/`requests` (prefix indices with `_`, follow redirects for symboldir CSV); cache (index/historical update ~daily). Or `pip install openbb-cboe` (keyless wrapper, encodes the underscore-index logic). `pandas.read_csv` on CSV URLs.
- **Gotchas:** **Indices MUST be underscore-prefixed** (`_VIX.json` 200; `VIX.json` 403). 403 = wrong path, not auth. Underlying LEVEL is `data.current_price` inside the options JSON (no separate live index endpoint). `all_indices.json` top-level is a **raw array** (not `{data:[...]}`). **Free options = snapshot only**, no historical chains/Greeks.
  - **[CORRECTED]** Intraday index price fields are **usable** (only the first 09:31 open bar is 0.0; what's zero is `stock_volume`) — profile wrongly said "0.0 for indices."
  - **[CORRECTED]** `CFE_FinalSettlement_Archive.csv` is a **stale/legacy archive** (696 rows, 2004→2019), **NOT** current settlements.
- **Verified docs:** cboe.com/delayed_quotes/API/quote_table/; the cdn.cboe.com JSON URLs above; docs.openbb.co
- **Confidence:** high.

---

### 2.15 `exchange_directories` — Nasdaq Trader Symbol Directory + Trade Halt RSS

- **$0 access path:** Fully free, **no key, no signup**. Anonymous HTTP(S)/RSS. Verified live 2026-06-09. **Card required: NO.**
- **Base URLs:** `https://www.nasdaqtrader.com/dynamic/symdir/` (pipe-delimited files), `ftp://ftp.nasdaqtrader.com/symboldirectory/` (same over FTP), `https://www.nasdaqtrader.com/rss.aspx` (Trade Halt RSS)
- **Auth:** None. (Site is behind Imperva/Incapsula CDN — a browser UA is a mild safeguard; **[CORRECTED]** default curl UA worked fine for the `.txt` files.)

| Method | Path | Purpose | Key fields |
|---|---|---|---|
| GET | `/dynamic/symdir/nasdaqlisted.txt` | Nasdaq-listed security master (~5,497 rows) | **8 cols** (live > 6-col legacy spec): `Symbol\|Security Name\|Market Category\|Test Issue\|Financial Status\|Round Lot Size\|ETF\|NextShares` |
| GET | `/dynamic/symdir/otherlisted.txt` | **All non-Nasdaq US listings (~7,305)** — de-facto free NYSE/Arca/American/BZX/IEX directory | `ACT Symbol\|Security Name\|Exchange (A=NYSE American,N=NYSE,P=NYSE Arca,Z=Cboe BZX,V=IEX, **M=NYSE Texas** edge case)\|CQS Symbol\|ETF\|Round Lot Size\|Test Issue\|NASDAQ Symbol` |
| GET | `/rss.aspx?feed=tradehalts` | Consolidated near-real-time halt/pause feed (all markets) | per `<item>`: `<ndaq:HaltDate>,<ndaq:HaltTime> (ET ms),<ndaq:IssueSymbol>,<ndaq:Market>,<ndaq:ReasonCode> (T1,T12,LUDP,H10,H11,MWC1-3),<ndaq:PauseThresholdPrice>,<ndaq:ResumptionTradeTime>` (namespace `xmlns:ndaq=http://www.nasdaqtrader.com/`) |
| GET | `/rss.aspx?feed=tradehalts&haltdate=MMDDYYYY` | Halts on a specific date | **[CORRECTED] DEEP archive** — verified numItems 1279 (2015-08-24), 820 (2020-03-16), 8 (2010-05-06 Flash Crash); >10 yr back via RSS itself |
| GET | `/dynamic/symdir/options.txt` (live), `/dynamic/symdir/bondslist.txt` (live) | Optionable underlyings / bonds | options root fields; `Symbol\|Security Name\|Financial Status` |

- **Rate limits:** No key/quota. Two rules: symbol files regenerate ~hourly (footer `File Creation Time`; **conditional GET works** — `If-Modified-Since`/`If-None-Match` → 304); Trade Halt RSS has `<ttl>1</ttl>` and docs state **"do not query more than once a minute."**
- **Real-time:** No WS. Trade Halt RSS is the only near-real-time surface (~1-min cadence). Symbol files are batch (intraday refresh).
- **Canonical data types fed:** `instrument/reference` (security masters). `corp_action` is a **stretch** — only trading **halts/pauses** (market-status events, not classic corp actions); **NO** dividends/splits/M&A/name-changes/CUSIP/ISIN/sector/listing-date in any free file.
- **Historical depth:** Symbol directories = **snapshot only** (snapshot daily yourself; use Adds/Deletes file or diff). Trade halt archive deep (>10 yr) via `&haltdate=`. Halt events have ms timestamps; directories carry state only.
- **Python approach:** No SDK — raw HTTP with conditional GET (store ETag/Last-Modified). `pandas.read_csv(url, sep='|')` then **drop the last row** (the `File Creation Time:` footer — capture it as the as-of timestamp). For halts, parse XML directly with `lxml`/`ElementTree` using the `ndaq` namespace (`feedparser` misses the custom fields). Poll RSS ≤ once/60s.
- **Gotchas:** **Footer row** must be stripped (every `.txt`). **Parse by header names, not fixed positions** (live header > legacy spec). Delimiter is **pipe** despite `.txt`. **Filter Test Issue=Y** (8 in nasdaqlisted: ZAZZT, ZBZZT, ...; 25 in otherlisted). **No JSON for halts** (`format=json` silently ignored → XML). `ndaq:` namespace required. Symbol conventions differ per column (ACT vs CQS vs NASDAQ Symbol — pick NASDAQ Symbol dot-notation). **[CORRECTED] `mfundslist.txt` is DEAD** (302→404) — do not use.
- **Verified docs:** nasdaqtrader.com/trader.aspx?id=symboldirdefs; .../id=TradeHaltRSS; .../id=tradehaltcodes
- **Confidence:** high.

---

### 2.16 `market_structure` — US market-structure GROUNDING references (NOT a queryable API)

- **$0 access path:** All authoritative specs are free public PDFs/HTML (no key/signup/card). **The underlying live SIP feeds (CTS/CQS/UTDF/UQDF) are PAID** (professional + access fees). **Card required: NO** (for the docs). This is **decode/adjustment logic, not data.**
- **Base URLs:** `ctaplan.com`, `utpplan.com`, `luldplan.com`, `nyse.com/publicdocs`, `sec.gov`, `law.cornell.edu/cfr/text/17`, `crsp.org` (**[CORRECTED]** live HTTPS home; the old `.com` is stale — CRSP acquired by Morningstar Feb 2026)
- **Auth:** None for docs. **[CORRECTED]** Exception: **sec.gov requires a declared `User-Agent` (CompanyName + contact) and ~10 req/s** — generic fetchers get 403. Other hosts have no UA requirement.

| GET (doc) | Path | Purpose |
|---|---|---|
| `nyse.com/publicdocs/nyse/data/Daily_TAQ_Client_Spec_v4.3.pdf` (**[CORRECTED]** v4.3 is current, NOT v4.1b) | Canonical condition-code decode: CTA + UTP Sale/Quote condition codes, Security Status Indicator, SSR Indicator (A/C/D/E), participant/exchange IDs |
| `ctaplan.com/tech-specs` → CTS/CQS Pillar Output Specs | Authoritative CTA/CQS (Tape A/B) message formats + code enumerations |
| `utpplan.com/DOC/UtpBinaryOutputSpec.pdf` | Authoritative UTDF/UQDF (Tape C / Nasdaq-listed) formats |
| `luldplan.com` | LULD mechanics: tiers, % params, reference price, band formula, pause |
| `law.cornell.edu/cfr/text/17/242.201` + SEC Rule 201 FAQ | Reg SHO Rule 201 short-sale circuit breaker (SSR) legal text |
| `crsp.org/.../CRSP_Calculations_and_Index_Methodologies.pdf` (or Michigan Ch.5 mirror) | CRSP cumulative split+dividend price-adjustment math |

- **Key decode tables (from TAQ spec):**
  - **Sale Condition** (4 chars, up to 4/trade): blank=Regular, F=ISO sweep, O=Mkt Ctr Open, Q=Official Open, M=Official Close, 6=Closing, P=Prior Reference Price, T=Extended Hours, Z=Sold (out of seq), 4=Derivatively Priced, 7=QCT.
  - **Quote Condition** differs **CTA vs UTP** (R=Regular both; CTA: A/B/H slow, N=Non-Firm, O=Opening; UTP: F=Fast, I=Imbalance, Y=Regular one-sided). **Do NOT share one map across both tapes.**
  - **Security Status Indicator:** M=LULD Trading Pause, 0=LULD Price Band, 1/2/3=MWCB Level breached, T=Resume, P=News Pending.
  - **SSR Indicator:** blank=not in effect, A=Activated, C=Continued, D=Deactivated, E=In Effect.
- **Real-time:** None as free reference. Real-time market-structure STATE (LULD bands, MWCB, SSR) rides inside the **paid** SIP feeds. Consolidated SIP: professional ~$20–50/unit/mo + access $500–2,500/mo (CTA and UTP each); non-display/derived $500–3,500/mo. Non-professional and 15-min-delayed commonly free via brokers.
- **Canonical data types fed:** `instrument/reference` (decode tables — the only real free deliverable) and `corp_action` (adjustment **math** only — NOT a data feed).
  - **[CORRECTED — most misleading claim]** Profile listed `trades, quote, l2_book, short_interest, index` as data_types. **NONE are reachable as free data here** — these are documents. Only decode tables + adjustment math are deliverables.
- **Historical depth:** Specs are versioned/timeless. Actual historical tick data: NYSE Daily TAQ from ~1993 (paid); CRSP daily from 1925 (paid via WRDS).
- **Python approach:** No SDK/API — download PDFs once, parse with `pypdf` / `pdftotext -layout`, hard-code the enumerations as decode maps. ctaplan.com PDFs are compressed (break naive markdown extractors) — parse locally. Implement CRSP adjustment in numpy/pandas (see §6).
- **Key facts (verified accurate):**
  - **Two SIPs:** CTA/CQS (Tape A = NYSE-listed; Tape B = Arca/American/IEX/Cboe + regionals) vs UTP (Tape C = Nasdaq-listed).
  - **LULD:** Tier 1 = S&P500+Russell1000+select ETPs; Tier 2 = other NMS. % params (>$3.00): T1 5% / T2 10%; ($0.75–$3.00) 20%; (<$0.75) lesser of $0.15 or 75%. Reference price = 5-min rolling mean, updates on ≥1% move. **Bands DOUBLE in the last 25 min (3:35–4:00 ET)** for T1 and sub-$3 T2. Limit State entered when **NB BID = upper band** OR **NB OFFER = lower band** (**[CORRECTED]** directional — profile said "equals a band" loosely); 15-sec limit state → 5-min pause.
  - **Reg SHO Rule 201:** trigger = ≥10% decline from prior day's close **as determined by the LISTING market**; restriction = remainder of trigger day **+ entire following day**; short-exempt marking allowed.
- **Verified docs:** ctaplan.com/tech-specs; utpplan.com; luldplan.com; law.cornell.edu/cfr/text/17/242.201
- **Confidence:** high.

---

### 2.17 `free_ceiling` — composite (covered in §1; profiles in §2.3/2.4/2.5)

The "honest $0 ceiling" is a composite of **Alpaca Basic IEX + Finnhub free WS + raw IEX TOPS/DEEP**. See §1 for the ceiling and §2.3–2.5 for the constituent providers. Verified data types on the free composite: `trade`, `quote` (IEX BBO, NOT NBBO), `ohlcv` (Alpaca IEX bars only — **not** Finnhub), `l2_book` (raw IEX DEEP binary only), `instrument/reference`.

---

## 3. Canonical-schema field mappings

Per canonical channel: PRIMARY vs fallback provider(s), and source→canonical mapping notes.

### `trade`
- **PRIMARY:** `alpaca` (IEX real-time WS + REST `/v2/stocks/trades`). **Fallback:** `finnhub` (WS, ≤50 symbols), `iex` HIST (T+1 full IEX fidelity).
- Mapping: Alpaca `{t,p,s,x,c[],i,z}` → `{ts, price, size, exchange, conditions, trade_id, tape}`. Finnhub WS `{s,p,v,t(ms),c[]}` → `{symbol, price, size, ts(ms→ns), conditions}`. **All free trades are IEX-venue or single-source — NOT consolidated.** FINRA monthly short-sale ZIPs provide short-sale *transactions* only (not a general trade feed).

### `quote` (Level-1 / BBO)
- **PRIMARY:** `alpaca` (IEX BBO real-time). **Fallback:** `tradier` (15-min delayed), `finnhub` `/quote` (last + day OHLC, **no bid/ask**), `cboe` (delayed underlying).
- Mapping: Alpaca `{bp,bs,ap,as,bx,ax}` → `{bid,bid_size,ask,ask_size,bid_exch,ask_exch}`. **CRITICAL: Alpaca free quote is IEX best bid/ask, NOT national NBBO** — flag `is_nbbo=false` on every free quote row. Tradier `quotes.quote` object/list shape must be normalized. Finnhub `/quote` has no bid/ask — map `c→last`, not to a quote.

### `l2_book` (depth)
- **PRIMARY (and only free):** `iex` DEEP (binary PCAP, T+1, IEX-venue depth via `PriceLevelUpdate`; DPLS = order-by-order).
- Mapping: DEEP `PriceLevelUpdate {side, price, size, flags}` → `{side, price, size}` per level. **No free real-time L2 anywhere; no L2 on Alpaca/Finnhub equities at any tier.** Mark `venue=IEX`, `is_consolidated=false`.

### `ohlcv`
- **PRIMARY:** `alpaca` (IEX bars, 1Min–12Month, 2016+, free). **Fallback:** `tiingo` (EOD 50+ yr), `polygon` (EOD, 2 yr, grouped-daily for whole-market), `yahoo` (deep daily, capped intraday), `stooq` (deep daily via apikey+PoW), `cboe` (index/equity daily history), `alphavantage` (weekly/monthly full + daily compact).
- Mapping: Alpaca `{t,o,h,l,c,v,n,vw}`; Tiingo `{date,open,high,low,close,volume,adjClose,adjVolume,divCash,splitFactor}`; Polygon `{t(ms),o,h,l,c,v,vw,n}`; Yahoo chart `quote[0].{open,high,low,close,volume}` + `adjclose`. **Adjustment policy:** store BOTH raw and adjusted; Alpaca default `raw` (request `adjustment=all`); Yahoo `auto_adjust=True` by default; Tiingo provides `adj*` inline; Alpha Vantage `TIME_SERIES_DAILY` is unadjusted (use `_ADJUSTED` weekly/monthly). **Finnhub OHLCV is premium — do NOT route here.**

### `instrument` / `reference`
- **PRIMARY:** `openfigi` (identity/join layer — ticker/CUSIP/ISIN↔FIGI, stable keys) + `exchange_directories` (full US listing universe). **Fallback:** `sec_edgar` (ticker↔CIK + entity metadata), `polygon`/`finnhub`/`tiingo`/`alphavantage` symbol lists, `cboe` symbol-book.
- Mapping: OpenFIGI `{figi, compositeFIGI, shareClassFIGI, ticker, exchCode, securityType, marketSector}` → canonical instrument with **FIGI as the permanent join key**; group dual-listings via `shareClassFIGI`, consolidate venues via `compositeFIGI` / `exchCode='US'`. Exchange directories `otherlisted.txt`/`nasdaqlisted.txt` → universe with exchange code map (N/P/Z/A/V/M). SEC `cik_str` must be **left-padded to 10 digits**. Filter `Test Issue=Y`.

### `corp_action`
- **PRIMARY:** `polygon` (`/stocks/v1/splits` + `/dividends`, full history, free) or `tiingo` (inline `divCash`/`splitFactor` in EOD rows). **Fallback:** `yahoo` (full div/split history), `alpaca` (`/v1/corporate-actions`), `finnhub` calendars + basic div/split (verify per key).
- Mapping: Polygon split `{execution_date, split_from, split_to}` → `{ex_date, ratio = split_to/split_from}`; dividend `{ex_dividend_date, cash_amount, pay_date, record_date, frequency}` → canonical. Tiingo `splitFactor` is the period factor; `divCash` is per-share cash. **SEC EDGAR has NO structured corp-action feed.** **Alpha Vantage corp actions only via weekly/monthly `_ADJUSTED` dividend amounts** (inline adjusted-daily is premium). Build the cumulative adjustment factor per §6.

### `fundamental`
- **PRIMARY:** `sec_edgar` (XBRL companyfacts/companyconcept/frames — deepest, authoritative, 2009+). **Fallback:** `alphavantage` (`OVERVIEW`/`INCOME_STATEMENT`/`BALANCE_SHEET`/`CASH_FLOW`/`EARNINGS`, ~5 yr, but 25/day), `finnhub` (`/stock/metric` basic), `yahoo` (shallow ~4 yr).
- Mapping: SEC `facts.{taxonomy}.{tag}.units.{unit}[].{val,end,fy,fp,form,filed,accn,frame}` → canonical fact keyed by `(tag, period_end, fy, fp)`; dedupe restatements via latest `filed`/`frame`. Derive FCF = `operatingCashflow − capitalExpenditures` (Alpha Vantage). **Tiingo/Finnhub detailed fundamentals are paid/limited.**

### `insider`
- **PRIMARY:** `sec_edgar` (Forms 3/4/5 — discover via submissions, parse ownership XML in Archives). **Fallback:** `yahoo` (`insider_transactions`, low fidelity).
- Mapping: Form 4 XML `nonDerivativeTransaction {securityTitle, transactionDate, transactionCode (P/S/A/M), transactionShares, transactionPricePerShare, sharesOwnedFollowingTransaction, directOrIndirectOwnership}` + `reportingOwner {relationship}` → canonical insider event. **Transaction detail is XML-only, NOT in the JSON API.**

### `holding_13f`
- **PRIMARY (only real source):** `sec_edgar` (13F-HR/13F-NT — discover via submissions, parse info-table XML). **No true fallback** — yahoo "holdings" are unreliable holder *summaries*, NOT 13F.
- Mapping: info-table `{nameOfIssuer, cusip, value, sshPrnamt, sshPrnamtType, investmentDiscretion, votingAuthority}` → canonical holding. **`value` units: thousands pre-2023, whole dollars from Jan 3, 2023 — verify per filing or be off 1000×.**

### `short_interest`
- **PRIMARY (and effectively only):** `finra` (consolidated bi-monthly `shrt{date}.csv` + daily Reg SHO `CNMSshvol{date}.txt`).
- Mapping: `{symbolCode, currentShortPositionQuantity, previousShortPositionQuantity, daysToCoverQuantity, changePercent, settlementDate}` → canonical short-interest record. Daily Reg SHO `{Symbol, ShortVolume, ShortExemptVolume, TotalVolume}` → short-sale **volume** (distinct channel from short interest — don't conflate). Pipe-delimited despite `.csv`. **Listed coverage exists in CDN files since ≥2018 (ignore the false "June 2021" break).**

### `filing`
- **PRIMARY:** `sec_edgar` (submissions history + EFTS full-text + Archives documents). **Fallback:** `finnhub` (SEC filings list), `yahoo` (`sec_filings` pointer list only).
- Mapping: submissions columnar `{form[i], filingDate[i], accessionNumber[i], primaryDocument[i]}` → canonical filing; resolve document URL via `Archives/edgar/data/{cik}/{accn-no-dashes}/{primaryDocument}`. EFTS `hits.hits[]._source` for full-text discovery (2001+).

### `option_quote`
- **PRIMARY:** `cboe` (delayed snapshot chains **with Greeks/IV**, free) or `tradier` (15-min delayed chains; **Greeks NOT reliably free on sandbox**). **Fallback:** `alpaca` (indicative, 15-min-delayed, Feb-2024+, free), `yahoo` (IV only, no Greeks).
- Mapping: Cboe `data.options[].{option (OCC), bid, ask, iv, delta, gamma, vega, theta, rho, theo, open_interest, volume}` → canonical option_quote (parse OCC symbol for expiry/right/strike). Tradier chain `+ greeks{delta,...,mid_iv}` (ORATS, ~hourly — **treat sandbox Greeks as best-effort**). **Real-time OPRA is paid everywhere.** Mark `is_delayed=true`, `is_indicative` for Alpaca free.

### `index`
- **PRIMARY:** `cboe` (VIX/SPX/NDX/RUT level via `current_price` + daily history to 1990) and `fred` (SP500/NASDAQCOM/DJIA EOD). **Fallback:** `stooq` (`^`-prefixed), `yahoo`.
- Mapping: Cboe `data.current_price` → index level (15-min delayed); Cboe `charts/historical/_VIX` → daily index OHLC. FRED `observations[].{date,value}` → EOD level. **FRED depth gotcha: SP500 AND DJIA are rolling 10-yr only; only NASDAQCOM is deep (1971+).** S&P/DJ index data carries license restrictions on Stooq/FRED.

### `macro`
- **PRIMARY:** `fred` (800k+ series, authoritative). **Fallback:** `alphavantage` (REAL_GDP/CPI/TREASURY_YIELD/etc., free but 25/day).
- Mapping: FRED `observations[].{date, value (string; '.'=missing)}` → canonical macro point; respect `units`/`frequency`/`aggregation_method` transforms; ALFRED `realtime_start/end` for vintages. **Stooq `macro` is NOT a real source — do not route here.**

---

## 4. Coverage matrix (corrected)

Verified free reality. ✅ = free & reachable; ⚠️ = free but degraded/caveated; ⛔ = NOT free / NOT available (downgraded by verification); — = N/A.

| Provider | trade | quote | l2_book | ohlcv | reference | corp_action | fundamental | insider | 13f | short_int | filing | option_quote | index | macro |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| **sec_edgar** | — | — | — | — | ✅ | ⚠️ text-only | ✅ | ✅ (XML) | ✅ (XML) | — | ✅ | — | — | — |
| **finra** | ⚠️ short-sale vol | — | — | — | ✅ threshold | — | — | — | — | ✅ | — | — | — | — |
| **alpaca** | ✅ IEX | ⚠️ IEX BBO (not NBBO) | ⛔ *(was listed — fabricated)* | ✅ IEX | ✅ | ✅ | — | — | — | — | — | ⚠️ indicative, Feb2024+ | — | — |
| **finnhub** | ✅ WS ≤50 | ⚠️ last only (no bid/ask) | — | ⛔ **premium** | ✅ | ⚠️ cal + basic | ⚠️ basic | — | — | — | ✅ | — | — | — |
| **iex** (HIST) | ✅ T+1 | ⚠️ IEX top-of-book | ✅ DEEP (binary,T+1) | ⚠️ aggregate yourself | ✅ | — | — | — | — | — | — | — | — | — |
| **yahoo** | — | ⚠️ ~15-min delayed | — | ✅ (intraday capped) | ✅ | ✅ | ⚠️ shallow | ⚠️ | ⛔ *(not 13F — holder summaries)* | — | ⚠️ link list | ⚠️ IV only | ✅ | — |
| **stooq** | — | ⚠️ EOD/delayed | — | ✅ (PoW+apikey) | ⚠️ light | — | — | — | — | — | — | — | ✅ | ⛔ *(overstated)* |
| **polygon** | ⛔ premium | ⛔ premium | ⛔ premium | ✅ EOD, 2yr | ✅ | ✅ splits+div | — | — | — | — | — | — | — | — |
| **tiingo** | ⚠️ IEX derived | ⚠️ IEX derived | — | ✅ EOD 50yr + IEX intraday | ✅ | ⚠️ inline only (endpoints paid) | ⛔ premium | — | — | — | — | — | — | — |
| **alphavantage** | ⛔ *(not free)* | ⚠️ EOD | — | ⚠️ compact daily / full w-m | ✅ | ⚠️ via _ADJUSTED only | ✅ (25/day) | — | — | — | — | — | ⛔ historical premium | ✅ |
| **tradier** (sandbox) | — | ⚠️ 15-min delayed | — | ✅ daily; shallow intraday | ✅ | — | — | — | — | — | — | ⚠️ chains; **Greeks not reliably free** | ⛔ *(production-only)* | — |
| **openfigi** | — | — | — | — | ✅ | — | — | — | — | — | — | — | — | — |
| **fred** | — | — | — | — | ✅ catalog | — | — | — | — | — | — | — | ⚠️ EOD; SP500/DJIA 10yr only | ✅ |
| **cboe** | — | ⚠️ delayed | — | ✅ daily + 1-min | ✅ | — | — | — | — | — | — | ✅ delayed snapshot + Greeks (no history) | ✅ | — |
| **exchange_directories** | — | — | — | — | ✅ | ⚠️ halts only (stretch) | — | — | — | — | — | — | — | — |
| **market_structure** | ⛔ *(docs, not data)* | ⛔ | ⛔ | — | ✅ decode tables | ⚠️ adj math only | — | — | — | — | — | — | — | — |

Downgrades applied by verification: alpaca l2_book (fabricated), finnhub ohlcv (→premium), yahoo holdings_13f (→holder summaries), stooq macro (overstated), alphavantage trades + index-history (→premium), tradier index + sandbox Greeks (→production), market_structure trades/quote/l2_book/short_int/index (docs not data), fred DJIA depth (→10yr cap).

---

## 5. Rate-limit & scheduling budget

Drives the ratelimit/scheduler design. All figures are FREE-tier.

| Provider | Free limit | Burst / extra | Daily/period cap | 429 behavior | Scheduler note |
|---|---|---|---|---|---|
| **sec_edgar** | **10 req/s** aggregate across ALL sec.gov hosts | — | none | 403 IP block | Global token bucket ≤8–10/s; prefer nightly bulk ZIPs (~3am ET) |
| **finra** (CDN) | none documented | — | none | — | Polite, cache; fetch on trading-calendar schedule |
| **finra** (Query API) | 1,200/min sync, 20/min async per dataset | 3 MB sync body cap | none on Public | 429 | OAuth token re-fetch on 401 |
| **alpaca** | **200 REST/min PER ACCOUNT** | WS: 1 conn, 30 sym (trades+quotes) | none stated | 429 + Retry-After | Per-account budget shared across processes; bars channel uncapped |
| **finnhub** | **60 calls/min** | + ~30 calls/sec burst cap | none stated | 429 | Backoff on 429; WS ≤50 symbols |
| **iex** (HIST) | none | Range/206 for resume | none | — | Bandwidth-bound (~13–16 GB/feed/day); T+1 nightly |
| **yahoo** | **none published — unstable** | — | soft IP bans common | 429 / YFRateLimitError | ~2 req/s + cache + jitter; batch via yf.download; cloud IPs throttled harder |
| **stooq** | low undocumented daily quota | PoW + apikey CAPTCHA | "Exceeded the daily hits limit" (low tens) | plaintext error | 1–2/s; bulk ZIP for backfill |
| **polygon** | **5 req/min** | — | ~7,200/day theoretical | 429 | Token bucket; grouped-daily = whole market in 1 call |
| **tiingo** | **50/hr, 1,000/day, 500 unique symbols/mo, 1 GB/mo** | — | symbol cap binds | 429 | **500-symbols/month** is the real ceiling — symbol rotation/caching |
| **alphavantage** | **25 req/DAY + 5/min** | — | 25/day | **HTTP 200 + Note body** | Parse body for throttle; ~1 ticker/day — not for bulk |
| **tradier** (sandbox) | **60/min** market data (per token) | — | none | 429 + X-Ratelimit-* | Watch X-Ratelimit-Available |
| **openfigi** | keyless 25/min (10 jobs); **keyed 25 per 6s = 250/min (100 jobs)** | sliding 6s window | none | 429 + ratelimit-reset | Pace ~25 jobs/6s; get the key (25,000 ids/min) |
| **fred** | **120 req/min per key** | — | none published | 429 | ~2 req/s; v2 bulk for whole-release backfills |
| **cboe** | none numeric (ToS prohibits scraping) | — | — | IP block risk | Cache ≥1 day; low frequency; real UA |
| **exchange_directories** | symbol files ~hourly (304 via cond-GET); RSS **≤1/min** | — | — | CDN challenge | Conditional GET; RSS poll ≥60s |
| **market_structure** | static docs (sec.gov ~10/s + UA) | — | — | sec.gov 403 if no UA | Fetch once, cache locally |

**Scheduling implications:** Alpha Vantage (25/day) and Polygon (5/min) are the tightest — restrict to targeted/whole-market-grouped pulls, never per-ticker loops. Tiingo's 500-symbol/month cap demands a symbol-rotation policy. SEC's 10/s is a *global* (cross-host) bucket. yfinance has no contract — treat every call as failure-prone with mandatory cache+backoff.

---

## 6. Corporate-action price-adjustment math (CRSP cumulative-factor method)

Authoritative source: CRSP Calculations, Chapter 5 (see §2.16). Goal: produce a continuous, back-adjusted price/volume series and correct total returns across splits and dividends.

### Cumulative adjustment factor
Let `C(t)` be the **cumulative factor to adjust price** (`CFACPR`), defined relative to a base date `C0` (typically the most recent date) where `C(C0) = 1.0`. Walking **backward** in time, for each ex-date `t` carrying per-event factor `f`:

```
C(t-1) = C(t) * f          (for t with an event between t-1 and t; else C(t-1) = C(t))
```

For a **simple split / stock dividend**, the per-event factor is:

```
f = FACPR + 1
```

where `FACPR` is CRSP's raw "Factor to Adjust Price" for that distribution. (A 2:1 split ⇒ `FACPR = 1` ⇒ `f = 2`. A 3:2 split ⇒ `FACPR = 0.5` ⇒ `f = 1.5`.)

### Applying the factors
- **Adjusted price / dividends:** `adj_price(t) = raw_price(t) / C(t)` (uses `CFACPR`).
- **Adjusted shares / volume:** `adj_volume(t) = raw_volume(t) * C_shr(t)` (uses `CFACSHR`). Shares/volume use **only stock splits & stock dividends**; price/dividends use **all** price-factor distributions (splits, stock dividends, spin-offs, rights). `CFACPR` and `CFACSHR` are usually equal but **NOT always**.
- Apply events on the **ex-distribution date**.

### Total return (split + dividend correct)
```
TotalReturn(t) = ( adj_price(t) + div_cash(t) ) / adj_price(t-1) - 1
```
(equivalently using CRSP's per-period factors with dividends adjusted by the same cumulative factor).

### Worked example
Stock with raw daily closes; base date = Day 5 (`C(5)=1.0`). A **2:1 split** has ex-date Day 4, and a **$0.50 cash dividend** has ex-date Day 3.

| Day | Raw close | Event (ex-date) | f | C(t) (CFACPR) | adj_price = raw/C |
|---|---|---|---|---|---|
| 1 | 200.00 | — | — | 2.0 | 100.00 |
| 2 | 210.00 | — | — | 2.0 | 105.00 |
| 3 | 220.00 | $0.50 cash div | (no price factor) | 2.0 | 110.00 |
| 4 | 104.00 | **2:1 split** (FACPR=1 ⇒ f=2) | 2 | **2.0** | 52.00 |
| 5 | 106.00 | — | — | **1.0** | 106.00 |

Factor walk (backward from base Day 5, `C(5)=1.0`):
- Day 4→5 had the split (ex Day 4): `C(4) = C(5) * f = 1.0 * 2 = 2.0`. (The cash dividend on Day 3 carries no *price* factor in the split-adjusted series, so it does not change `C`.)
- `C(3) = C(2) = C(1) = 2.0`.

So pre-split raw closes (200→210→220) become adjusted 100→105→110, continuous with the post-split 52→106. **Volume** would be multiplied by `CFACSHR=2.0` pre-split.

**Total return Day 4→5** (no dividend on Day 5):
```
(106.00 + 0) / 52.00 - 1 = +103.85%   ← WRONG if you forget to adjust
(adj 106.00) / (adj 52.00) - 1 = 106/52 - 1 ... 
```
Using adjusted prices (both already in the same Day-5 base): `106.00 / 52.00 − 1`. Day 4's adjusted close is 52.00, Day 5's is 106.00 — the genuine ~+1.9% move on Day 5's raw (104→106) is preserved as `106/104 − 1 = +1.92%` once you note Day-4 adj = 52.00 and Day-5 adj = 106.00 are **not** in the same factor regime unless C is applied consistently. **Always divide BOTH days' raw price by their respective `C(t)` before differencing** — that yields the correct `106.00/52.00`-style continuity where the split itself contributes 0 return and only true price moves remain.

**Total return across the dividend (Day 2→3):**
```
TotalReturn = (adj_price(3) + adj_div(3)) / adj_price(2) - 1
adj_div(3) = 0.50 / C(3) = 0.50 / 2.0 = 0.25
            = (110.00 + 0.25) / 105.00 - 1 = +4.9976%
```
vs the price-only `110.00/105.00 − 1 = +4.76%` — the +0.24% gap is the dividend's contribution to total return.

### The classic adjustment bugs (avoid)
- Using `FACPR` directly instead of `f = FACPR + 1`.
- Adjusting on the wrong date (must be **ex-date**).
- Sharing `CFACPR` for shares/volume (use `CFACSHR`).
- Mixing vendor conventions: some vendors back-adjust dividends into price; CRSP keeps price split-only via `CFACPR` and handles dividends in the return. **Pick one convention explicitly and document it** (Stockodile: store raw + `CFACPR`/`CFACSHR` + dividend cash, compute adjusted on read).

---

## 7. Consolidated gotchas (cross-provider trap list)

**Free-ceiling traps**
1. **No free real-time consolidated SIP tape and no free real-time OPRA — ever.** Anything claiming otherwise is delayed/sampled/in breach. Full SIP = $thousands/mo; OPRA = +$1,500/mo redistributor fee.
2. **All "free real-time" equity feeds are single-venue/partial.** Alpaca = IEX only (~2.5–3% of tape); Alpaca "quotes" are **IEX BBO, NOT national NBBO** — set `is_nbbo=false`. Finnhub WS = trades only, ≤50 symbols.
3. **Free L2/depth exists ONLY as raw IEX DEEP** (binary PCAP, T+1, IEX-venue). No free real-time L2; **no equity L2 on Alpaca/Finnhub at any tier.**

**Endpoints that moved to premium (downgrade on detect)**
4. **Finnhub `/stock/candle` (US OHLCV) → premium** (403). Cannot backfill candles from Finnhub.
5. **Alpha Vantage `outputsize=full` daily, `DAILY_ADJUSTED`, intraday history, historical index → premium.** Free daily = ~100 bars; long-history free workaround = weekly/monthly `_ADJUSTED`.
6. **Tiingo News + Fundamentals + dedicated corporate-action endpoints → premium** (403). Free corp actions only via inline `divCash`/`splitFactor`.
7. **Polygon free = EOD only, 5/min, 2 yr, no WS/trades/quotes.** Not even 15-min delayed.
8. **Tradier sandbox Greeks/IV and index quotes → production-only** per official table (sandbox Greeks best-effort at most).
9. **Alpaca options free = indicative (derived, 15-min-delayed, Feb-2024+), NOT OPRA.**

**Fabricated / overstated capabilities (verification refuted)**
10. **Alpaca equity `l2_book` — fabricated** (L1/BBO only; L2 is Crypto-API only).
11. **yfinance `holdings_13f` — overstated** (holder summaries, not parsed 13F; known data bugs). yfinance `sec_filings` is a link list, not filing content.
12. **Stooq `macro` — overstated** (thin/incidental; not a FRED substitute).
13. **FRED DJIA "long history" — wrong** (SP500 AND DJIA are rolling 10-yr; only NASDAQCOM is deep, 1971+).
14. **market_structure trades/quote/l2_book/short_interest/index "data types" — wrong** (these are decode docs, not queryable data).

**Dead / retired (do not use)**
15. **IEX Cloud (iexcloud.io)** — shut down 2024-08-31. **api.iextrading.com REST** — retired 2021-11-18 (403). Use only iextrading.com HIST + redistributors.
16. **`exchange_directories` `mfundslist.txt` — dead** (302→404).
17. **`pandas-datareader` 'stooq' reader — broken** (returns apikey HTML). **`alpaca-trade-api`, `iexfinance`, stale `pyopenfigi`/`tradier-python`** — deprecated/stale.

**Auth / format integration bugs**
18. **SEC CIK must be 10-digit zero-padded** in data.sec.gov paths; `company_tickers.json` `cik_str` is a plain int — left-pad it. SEC requires a descriptive **User-Agent** (no UA → 403). sec.gov enforces UA in market_structure too.
19. **Tiingo auth prefix is `Token ` (not `Bearer`).** FRED v1 = `?api_key=` query param, v2 = `Authorization: Bearer`; **don't mix.** Finnhub uses `?token=`/`X-Finnhub-Token` (NOT `x-finnhub-secret`).
20. **FINRA short-interest `.csv` is pipe-delimited** (`sep='|'`). **Nasdaq Trader `.txt` files are pipe-delimited** with a footer `File Creation Time` row to strip; parse by header names (live header > legacy spec); filter `Test Issue=Y`.
21. **FRED: default `file_type` is XML — always pass `file_type=json`; values are strings; missing = `'.'`.**
22. **Cboe indices must be underscore-prefixed** (`_VIX.json`; `VIX.json` → 403); 403 = wrong path, not auth. `all_indices.json` is a raw array.
23. **OpenFIGI is POST with a JSON-array body**; keyed limit is a **sliding 6-second window** (bursting 250 then sleeping → 429).
24. **Single-vs-multi-symbol JSON shape**: Tradier `quotes.quote` and `options.option` are OBJECT for one, LIST for many — normalize. IEX HIST index is ARRAY for one date, OBJECT for no-arg.

**Data-correctness traps**
25. **13F `value` units: thousands pre-2023, whole dollars from Jan 3, 2023** — verify per filing (off by 1000× otherwise).
26. **FINRA: listed securities ARE in the CDN short-interest files since ≥2018** — ignore the false "June 2021 coverage break."
27. **Adjustment defaults differ**: Alpaca `raw` (use `adjustment=all`); Yahoo `auto_adjust=True`; Alpha Vantage `TIME_SERIES_DAILY` unadjusted. Apply CRSP `f = FACPR + 1` on the ex-date (§6).
28. **LULD bands DOUBLE in the last 25 min (3:35–4:00 ET)** for Tier 1 and sub-$3 Tier 2 — don't assume static percentages all day.
29. **Reg SHO Rule 201 (SSR)** triggers on ≥10% decline from the **LISTING market's** prior close and persists into the **next trading day**.
30. **CTA vs UTP quote-condition tables differ** — never share one decode map across both tapes.
31. **Cboe / Stooq / FINRA terms restrict redistribution / commercial use, and S&P-DJ index data is license-restricted** — fetch per-user, require BYO key, don't bundle/rebroadcast.
32. **IEX/Cboe data is IEX-venue / 15-min delayed** — never label as consolidated NBBO or real-time. Mark `venue`, `is_delayed`, `is_consolidated` on every row.
