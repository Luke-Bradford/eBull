# Lightweight ETL audit — per-job incremental fetch plan

**Status:** Research complete. Tickets filed. Implementation sequenced below.
**Date:** 2026-04-17
**Trigger:** Production lockup when 45-min `daily_research_refresh` ran on every dev-stack restart (issue diagnosed + catch-up disabled in PR #267). Underlying cost: we pull every company's full XBRL `companyfacts.json` (100KB+) every day regardless of whether anything filed. User directive: "there HAS to be a better route" — audit every job, replace full-pull with change-driven fetch, watermark what needs watermarking, preserve historical depth for reporting.

---

## Architecture principle

Every data-ingest job splits into **two modes**:

1. **Backfill** — one-time or low-frequency. Fills historical depth (candles history, past 10-K/10-Q/8-K filings, fundamentals periods). Expensive, but runs once per instrument or on-demand when an instrument joins the universe.
2. **Incremental** — frequent (daily or sub-daily). Uses a **watermark** (latest accession, latest `acceptedDate`, ETag, etc.) to fetch only what the provider has added since the watermark. Target: zero-byte response (HTTP 304 or short "no new filings" list) on days nothing changed.

Jobs that today only have mode 1 (full pull every day) will be split into mode 1 + mode 2, with mode 1 running on-demand or weekly at most.

---

## Per-provider capability summary

### SEC EDGAR

- `www.sec.gov/files/company_tickers.json` — honours `If-Modified-Since` → 304. [Tested.]
- `data.sec.gov/submissions/CIK*.json` — **no conditional-GET support**. AWS API Gateway strips validators.
- `data.sec.gov/api/xbrl/companyfacts/CIK*.json` — same, no conditional-GET.
- `www.sec.gov/cgi-bin/browse-edgar?...&output=atom` — per-CIK Atom feed, ~14 KB, no conditional-GET but much cheaper than full submissions.
- `www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip` — **1.53 GB** nightly, honours `If-Modified-Since` + `ETag`.
- `www.sec.gov/Archives/edgar/daily-index/xbrl/companyfacts.zip` — **1.37 GB** nightly, honours `If-Modified-Since` + `ETag`.
- `www.sec.gov/Archives/edgar/daily-index/YYYY/QTRn/master.YYYYMMDD.idx` — **~67 KB** per trading day, one line per filing, honours `If-Modified-Since`.
- Rate limit: **10 rps total**, `User-Agent` with email required.

### eToro (official public API)

- **OpenAPI v1.158.0**, 57 paths, documented at `api-portal.etoro.com`. (Note: prior memory note calling this a reverse-engineered private API is wrong.)
- No conditional-GET / `If-Modified-Since` / `ETag` on any documented endpoint.
- Candles endpoint takes `candlesCount` (1-1000), **no date-range filter** — you ask for last N bars. Fine for incremental once seeded.
- WebSocket at `wss://ws.etoro.com/ws`. `instrument:<id>` topics push rate updates; `private` topic pushes order/position events.
- Rate limit: **60 GET/min, 20 write/min** per user key.

### Financial Modeling Prep (FMP)

- No conditional-GET support.
- No "since X" delta endpoint.
- Bulk endpoints (`site.financialmodelingprep.com/datasets`) and `/earnings-calendar?from=&to=` are the intended alternatives.
- Per-plan quota (Basic 250/day, Starter 300/min).

### Companies House

- REST `/filing-history` — no `ETag` / `If-Modified-Since` / `since=`.
- **Streaming API** with resumable `timepoint` watermark — the intended delta channel. Requires separate streaming key.
- REST rate limit: 600 req / 5-min rolling.

### Frankfurter (ECB reference rates)

- **`ETag` + `If-None-Match` → 304**. [Tested.]
- `If-Modified-Since` is ignored.
- ECB publishes ~16:00 CET on TARGET working days. Polling outside that window is pure waste.

---

## Per-job audit

### `universe` → `nightly_universe_sync` (eToro)

**Current:** Full `/instruments` pull every night.
**Provider capability:** No delta endpoint. One request returns ~15k rows.
**Recommendation:** Keep full pull, drop cadence from daily to **weekly**. Delistings surfaced in a secondary daily thin `search`-filter pass.
**Impact:** Low (single request already cheap). Reduces write amplification.
**Ticket:** #269 (proposed — see below).

### `cik_mapping` → `daily_cik_refresh` (SEC)

**Current:** Full `company_tickers.json` pull every day (~800 KB).
**Provider capability:** Honours `If-Modified-Since` → 304.
**Recommendation:** **Conditional GET**. Persist `Last-Modified` + body hash. On 304, noop (no DB writes). On 200, diff against stored hash, upsert changed mappings only.
**Impact:** 99% of daily runs will be 304 zero-byte. Eliminates the noisy daily failure we just fixed in PR #267.
**Ticket:** #270.

### `candles` → `daily_candle_refresh` (eToro)

**Current:** Per-instrument `candlesCount=400` pull. ~500+ instruments daily.
**Provider capability:** No date-range filter; `candlesCount` is the only knob.
**Recommendation:** **Two-mode split.**
- **Backfill** (new instrument promoted into the universe, or gap detected): `candlesCount=400`.
- **Incremental** (instrument already has candles): `candlesCount=3` (yesterday + today + one correction day). Dedup locally on `(instrument_id, date)`.

**Impact:** ~99% reduction per instrument on incremental runs. Candles refresh time drops from minutes to seconds.
**Ticket:** #271.

### `fundamentals` + `financial_facts` + `financial_normalization` → `daily_financial_facts` + `daily_research_refresh` (SEC)

**Current:** For every covered US instrument, pull full `companyfacts.json` (100 KB+) and `submissions.json` every day. This is the **45-min job** that caused today's lockup.
**Provider capability:** No conditional-GET on either endpoint. But three alternative change signals exist:
1. Bulk `submissions.zip` (1.53 GB, conditional) — one download per day, unzip, read only the CIKs in our universe.
2. Daily master index (~67 KB) — one line per filing for one trading day, conditional-GET.
3. Per-CIK Atom feed (~14 KB) — ordered newest-first by filing date.

**Recommendation:** **Option 2 (daily master index) + per-CIK watermark.**
- Fetch today's `master.YYYYMMDD.idx` with `If-Modified-Since`. Parse CIK list of filings accepted today.
- Intersect with our covered-CIK set.
- For each covered CIK that filed something today: compare top `accessionNumber` from `submissions.json` against stored watermark. If changed, fetch `companyfacts.json` for that CIK only.
- Persist `(cik, latest_accession_number, latest_accepted_at)` watermark in a new `external_data_watermarks` table.

**Why not option 1 (bulk zip):** 1.53 GB daily download is wasteful when ~0.5% of our universe files on any given day.
**Why not option 3 (Atom per-CIK):** Still 500 × 14 KB = 7 MB + 500 rate-limited calls just to detect "nothing new." Master index beats it.

**Impact:** 45 min → ~2-5 min on typical days. Provider requests drop from ~1000/day (500 × submissions + 500 × companyfacts) to ~5-20/day (1 master index + N companyfacts for the small subset that actually filed).
**Ticket:** #272.

### `news` → `daily_news_refresh`

No provider wired. Out of scope. When wired, apply the same watermark principle (persist latest article ID, fetch only newer).

### `thesis` → `daily_thesis_refresh` (Anthropic)

**Current:** Per-instrument Claude call. Gated by `find_stale_instruments` + `coverage.review_frequency`.
**Provider capability:** Claude API call. No incremental option — each call regenerates.
**Recommendation:** **Event-driven trigger in addition to time-based staleness.**
- Today: "thesis older than review_frequency window" triggers refresh.
- Add: "instrument has a new 10-Q / 10-K / 8-K since last thesis" triggers refresh.
- Remove: instruments without the minimum filings-coverage bar (#268) from the refresh pool entirely — no point generating a thesis on 1 data point.

**Impact:** Keeps thesis relevant to material-event cadence. Prevents waste on instruments that have no new information. Claude cost proportional to real change, not calendar.
**Ticket:** #273.

### `portfolio_sync` → `daily_portfolio_sync` (eToro)

**Current:** Full portfolio pull every 5 minutes.
**Provider capability:** No conditional-GET on REST. WebSocket `private` channel pushes order/position events.
**Recommendation:** **WebSocket subscriber for events, REST reconcile at lower cadence.**
- Subscribe to WebSocket `private` topic at backend boot. On each event, upsert the affected position/order row.
- Run full REST `/trading/info/portfolio` every 15 min (not 5) as a reconcile — catches missed events and refreshes cash (cash is NOT in the private push stream per eToro docs).

**Impact:** Near-real-time position updates (seconds not 5 min) AND ~3x reduction in REST polling.
**Ticket:** #274.

### `fx_rates` — split into live + EOD sources

Decision 2026-04-17: two FX sources, split by purpose, no accidental mixing.

**Live stock / portfolio / P&L conversions → eToro FX instruments** (ticket #281):
- FX pairs (GBP/USD, GBP/EUR, EUR/USD, …) are tradable instruments on eToro with their own WebSocket rate streams.
- Subscribe via the WebSocket integration from #274.
- Marks match what actually executes on the broker — no drift between eBull P&L and eToro statements.

**Tax report conversions → Frankfurter ECB** (ticket #275):
- ECB published central-bank reference rate — the correct source for tax filings wanting a regulatory-approved rate.
- Fetched once per day at 16:15 CET with `If-None-Match` → 304 on no-change.
- Skip non-TARGET days (weekends + TARGET holidays).

Every conversion path must be tagged at the call site with which source should feed it. No caller should ever be able to pick up the wrong one by accident — separate service modules (`fx_live.py` vs `fx_eod.py`) keep the boundary explicit.

**Impact:** Live P&L becomes intraday-accurate. Frankfurter polling drops from 24/day → 1-2/day. Broker-statement reconciliation becomes trivial.

### `cost_models` / `weekly_reports` / `monthly_reports`

Internal jobs, no external providers. Out of scope.

### `scoring` + `recommendations` → `morning_candidate_review`

Derived from internal data only. No external fetch to optimise. Could benefit from:
- Only re-score instruments whose underlying `candles`, `fundamentals`, or `thesis` layer changed since last scoring run. Dependency-driven recompute rather than blanket re-score.

**Ticket:** #276.

---

## Cross-cutting: watermark infrastructure

Every recommendation above needs a per-source watermark table. Propose:

```sql
CREATE TABLE external_data_watermarks (
    source          TEXT NOT NULL,             -- 'sec.companyfacts', 'sec.submissions', 'sec.tickers', 'frankfurter.latest', etc.
    key             TEXT NOT NULL,             -- CIK for SEC per-company watermarks, 'global' for singletons
    watermark       TEXT NOT NULL,             -- latest accession_number, ETag value, date string, etc.
    watermark_at    TIMESTAMPTZ,               -- when the provider says the data was last updated
    fetched_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    response_hash   TEXT,                      -- optional sha256 of body for dedup decisions
    PRIMARY KEY (source, key)
);
```

A thin helper `get_watermark(source, key)` / `set_watermark(source, key, ...)` lives in `app/services/watermarks.py`. Each job uses it as its single source of truth for "what did we last see from this provider".

**Ticket:** #269 (cross-cutting infrastructure — land before the per-job tickets can reuse it).

---

## Historical coverage preservation

User directive: "I want to know we have a decent amount of history to be able to report anything which has over time metrics."

How each recommendation preserves history:

- **Candles:** Backfill mode still pulls 400 bars on first sight of an instrument. Incremental mode only appends new bars; existing history never touched.
- **Filings / fundamentals:** Watermark gates only the *refresh*, not the initial backfill. A new instrument entering the universe still triggers full `companyfacts.json` + full submissions history pull. Existing rows remain.
- **Filings coverage bar** (#268): orthogonal — defines "enough history to analyse" (≥2× 10-K, ≥4× 10-Q, 12 mo of 8-K). An instrument below the bar is surfaced as `coverage.filings_status = 'insufficient'` and excluded from thesis/scoring until backfill catches up.
- **FX:** Always daily cadence; watermark ensures we catch every publication. No loss of time-series density.
- **Portfolio:** Event stream captures every change; reconcile is a safety net, not a data source.

---

## Sequencing (implementation order)

1. **#269 — watermarks table + helpers** (blocker for the per-job tickets). ~1 day.
2. **#270 — SEC CIK conditional GET** (smallest scope, validates watermark pattern). ~0.5 day.
3. **#275 — Frankfurter ETag + TARGET calendar** (also small, proves the pattern on a second source). ~0.5 day.
4. **#271 — eToro candles two-mode split** (biggest daily win outside research). ~1 day.
5. **#272 — SEC master-index + per-CIK watermark** (biggest daily win overall; the 45-min job). ~2 days.
6. **#274 — eToro WebSocket portfolio + quotes** (architecturally significant; moves us off polling for push-stream data). ~2-3 days.
7. **#273 — thesis event-driven trigger** (depends on #272 for filings events). ~1 day.
8. **#277 — universe weekly cadence** (cosmetic). ~0.5 day.
9. **#276 — dependency-driven re-scoring** (depends on #269 and good upstream watermarks). ~1 day.
10. **#268 — filings coverage bar** (already filed; implement after #272 so the coverage check has accurate filing counts).
11. **#278 — FMP earnings-calendar gating** (follow-up, non-blocking).
12. **#279 — Companies House streaming** (follow-up, non-blocking).

---

## Related immediate fix

**Reaper-on-boot**: `app/services/sync_orchestrator/reaper.py` already exists but isn't called on app lifespan startup. A crashed / killed sync leaves `sync_runs.status='running'` forever, blocking the concurrency gate until manual SQL intervention (as happened today). Add `reap_stale_sync_runs()` to the lifespan startup hook. Shipped as a small separate PR alongside this plan — not a ticket.

---

## Expected aggregate impact

Assumptions: 500 tradable instruments, 300 of which are US (have CIK).

| Metric | Today | After | Change |
|---|---|---|---|
| Daily SEC API requests | ~1300 (tickers + 300 submissions + 300 companyfacts + misc) | ~5-20 (1 master index + N for CIKs that filed) | -98% |
| Daily SEC bytes down | ~200 MB | ~1-10 MB | -95% |
| Research refresh wall time | 45 min | 2-5 min typical | -90% |
| Candle refresh wall time | 3-5 min | <30 sec incremental | -90% |
| Hourly Frankfurter calls | 24/day | 1-2/day | -95% |
| Portfolio REST polls | 288/day (5min) | 96/day (15min) + WebSocket events | -66% polls, +real-time events |
| Claude thesis calls | time-driven | event-driven | proportional to change |

---

## Out of scope for this plan

- Live-price streaming architecture (covered separately in `2026-04-13-live-pricing-architecture-design.md`).
- News ingestion (no provider wired).
- Changes to the scoring / ranking algorithm itself.
- Replacing eToro as the broker.
