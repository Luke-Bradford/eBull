# eBull refocus — research tool, not autonomous fund

**Date:** 2026-04-19
**Author intent:** stop polishing ETL / admin; ship a per-ticker research page I actually open on a Sunday. AI assists research, human decides, eToro executes.

## Why this exists

Two weeks of work produced: a typed state machine, execution guard safety rules, v2 API, Admin UI rewrite, cascade collapse, 11 PRs, 127 new backend tests. None of it is visible on the front page beyond a slightly better Admin page. No ticker lookup, no financials view, no real research surface.

Reframe: stop building "AI auto-trades, you watch." Build "Bloomberg-for-me." AI summarises filings + computes factor scores + generates thesis on-demand. Human decides. eToro executes via the already-built guard + order path.

Realistic ambition: 1-3% annual edge vs passive, from better-informed retail decisions. Not generational wealth, not early retirement. A finite tool that's useful to open weekly. Secondary: skill + artifact that isn't day-job code.

## What gets dropped

- Sub-project A.6 (auto-retry on self-heal failures) — needed only if the app autonomously trades. It doesn't. Close the umbrella idea; file real issues only if a specific layer's failure pattern becomes annoying.
- Sub-project B (layer metrics history) — no UI consumer.
- Sub-project C (full three-zone Admin redesign) — Admin becomes a hidden `/ops` route, not the main UX.
- Sub-project D (raw data cleanup visibility) — just flip `raw_retention_dry_run=False` per #325 and move on.
- Sub-project E (delta-pull audit) — nice-to-have, can happen inside individual provider work if a provider becomes a problem.

Issues to close with "refocused; out of scope" comments: #329, #330, #331, #332.

## Phase 1 — collapse the ETL bloat

Single-session goal. ~5-6 PRs, each small.

### 1.1 Merge SEC jobs

**Before:** `daily_cik_refresh` + `daily_financial_facts` + `daily_research_refresh` + `weekly_coverage_review` + `weekly_coverage_audit`.

**After:** one scheduled job `fundamentals_sync`, weekly cadence. Walks held positions + watchlist + universe top tier. For each ticker:
- Ensure CIK mapped (upsert — fixes #257).
- If last SEC filing date > 90 days or we have no filing cached: pull latest 10-K + 10-Q XBRL, extract facts, normalise.
- Update `fundamentals_snapshot` row.

Collapse service files: `financial_facts.py` + `financial_normalization.py` + `sec_incremental.py` + `fundamentals.py` → `fundamentals.py` (one module). Same for `coverage.py` + `coverage_audit.py` + `filings_backfill.py` → `coverage.py`.

Net: ~8 SEC-related files → 2.

### 1.2 Make news + thesis on-demand, not scheduled

**Drop the scheduled jobs** `daily_news_refresh` and `daily_thesis_refresh`.

**New backend endpoints:**
- `GET /instruments/{symbol}/news?max_age_hours=24` — returns cached if fresh, else pulls from Finnhub + runs Anthropic sentiment, caches in DB, returns.
- `POST /instruments/{symbol}/thesis` — runs Anthropic thesis generation on-demand using current fundamentals + news + price. Cached 24h per-ticker.

Only tickers the user opens get LLM spend. Blast cost drops ~50-200× depending on usage.

### 1.3 Scope-limit candle refresh

`daily_candle_refresh` currently pulls ~15k instruments. Change to pull:
- All currently-held positions (5-min cadence during market hours).
- All watchlist tickers (5-min cadence).
- Top-1000 universe by coverage tier (once daily, not 5-min).

One job, three scopes. Backs off aggressively outside market hours.

### 1.4 Drop or merge other jobs

Drop or one-shot:
- `seed_cost_models` — one-off migration, not a job.
- `attribution_summary` — no UI consumer, revisit when reporting matters.
- `daily_tax_reconciliation` — same.
- `weekly_report` + `monthly_report` — same.
- `retry_deferred_recommendations` — only if there's real usage.

Keep:
- `nightly_universe_sync` (weekly).
- `daily_portfolio_sync` (5-min market hours).
- `monitor_positions` (5-min market hours).
- `execute_approved_orders` (when there are approved recs).
- `fx_rates_refresh` (daily).
- `raw_data_retention_sweep` (daily).
- `fundamentals_sync` (weekly, new).
- Candle refresh (three-scoped, new).

**Target: ~8 scheduled jobs, down from ~18.**

### 1.5 Kill the 4 persistent red rows on Admin

- Fix #257 (CIK UniqueViolation → INSERT ... ON CONFLICT ... DO UPDATE). Unlocks downstream cascade.
- Set `ANTHROPIC_API_KEY` in the operator's env. Clears News + Thesis.
- Run coverage audit to populate NULL `filings_status` rows.

Admin goes green. Then hide it at `/ops`.

## Phase 2 — instrument research page

The actual user-visible payoff. ~4-5 PRs.

### 2.1 Add yfinance provider

```bash
uv add yfinance
```

Thin wrapper at `app/providers/yfinance_provider.py`. Used for non-US tickers where SEC XBRL doesn't apply. Exposes: company profile, financials (quarterly + annual), price history, dividends, analyst estimates, major holders.

yfinance scrapes Yahoo Finance's public pages. Unstable (Yahoo can break without notice) but MIT-licensed, no API key, no rate limit, proven in the open-source community.

### 2.2 Instrument summary endpoint

`GET /instruments/{symbol}/summary` returns:
- Core identity: symbol, display_name, sector, industry, exchange, country, market_cap.
- Price: current, day_change, day_change_pct, 52w_high, 52w_low.
- Key stats: pe_ratio, pb_ratio, dividend_yield, payout_ratio, roe, roa, debt_to_equity, revenue_growth_yoy.
- Data source: "SEC EDGAR" / "yfinance" / "Finnhub" per field.

Pull priority:
- US tickers: SEC XBRL (already in DB) + Finnhub for current price + yfinance for gaps.
- Non-US: yfinance primary + Finnhub for current price.

### 2.3 Financials endpoint

`GET /instruments/{symbol}/financials?period=quarterly|annual&statement=income|balance|cashflow` returns the structured rows for that statement.

For US: reads directly from our `financial_facts` table (XBRL-sourced, already populated by fundamentals_sync).

For non-US: yfinance lookup, cached 24h.

### 2.4 Thesis endpoint

`POST /instruments/{symbol}/thesis` generates a fresh thesis using Anthropic. Prompts Claude with:
- Latest 4 quarters of income + balance + cash flow.
- Current price + 52w range + recent news headlines.
- Sector median for key ratios.
- Known positions + cost basis if held.

Returns a structured thesis: bull_case, bear_case, valuation_assessment, catalysts, risks. Cached 24h.

### 2.5 Instrument page frontend

`frontend/src/pages/InstrumentPage.tsx` at route `/instrument/:symbol`. Six tabs:

1. **Overview** — price chart, company description, key stats table, top 5 news items.
2. **Financials** — quarterly + annual income/balance/cash-flow grids. Growth rate column. Margin sparkline.
3. **Analysis** — our scoring (quality/value/momentum/sentiment/turnaround) with each component explained. AI thesis (fetched on-demand). Comparison to sector median.
4. **Positions** — if held, current units + entry + P&L. "Close" button. If not, "Add to watchlist".
5. **News** — Finnhub feed, sentiment badge per item.
6. **Filings** — SEC filings list. "Summarise with AI" button per filing (one-shot Anthropic call, cached).

Price chart: TradingView Lightweight Charts (free, Apache 2.0). Plugs into existing candles data.

Click a ticker anywhere (portfolio row, universe list, recommendations) → navigate here.

## Phase 3 — dashboard rewrite

### 3.1 Dashboard as portfolio cockpit

`/` (root / Dashboard) becomes:
- Big number: portfolio total value + today's change.
- Positions table: ticker, units, entry, current price with live flicker, P&L, trend sparkline, click-through to instrument page.
- Watchlist section: same shape, no P&L column.
- Top movers today (from held + watchlist).
- News feed filtered to held positions.
- "Run morning review" button (triggers scoring + recommendations for held + watchlist).

Drop: the ops-grid. Drop: coverage percentages as Dashboard content. Drop: bootstrap-progress widget (move to /ops).

### 3.2 Watchlist data model

Add `watchlist` table: operator_id, instrument_id, added_at. Endpoints: `GET/POST/DELETE /watchlist`. Frontend: "Add to watchlist" from InstrumentPage, list/remove on Dashboard.

## Phase 4 — live quotes

### 4.1 eToro WebSocket

Issue #274. eToro publishes quote + private-event WebSocket feeds. Subscribe to:
- All held instrument quotes.
- All watchlist instrument quotes.
- Portfolio state updates.

Backend: a long-running subscriber process. Writes to Redis pub/sub channels keyed by instrument_id.

Frontend: `useQuoteStream(instrumentId)` hook opens a Server-Sent Events or WebSocket connection from the UI to a FastAPI endpoint that relays from Redis. Prices flicker on the dashboard + instrument page during market hours.

## Phase 5 — nice-to-haves (defer until phases 1-4 land)

- Insider transactions panel (SEC EDGAR Form 4 or Finnhub).
- 13F major-holders panel.
- Earnings calendar widget.
- AI filing summariser that actually summarises (currently only news does).
- Paper-trading mode toggle per position (test entries without real capital).

## Phase 6 — maybe never

- Auto-trade / auto-execute approved recommendations. Keep human in the loop.
- Copy-trading feed from eToro top traders (interesting but complex auth scope).
- Portfolio optimisation (mean-variance, risk parity). Overkill for retail scale.
- MCP server (#206) — conversational interface. Defer indefinitely.

## Process rules (not negotiable)

Lessons from sub-project A + A.5:

1. **No subagent ceremony.** One implementer per task, run Codex before first push only. Fix findings inline. Ship.
2. **No spec → plan → subagent-implementer → spec-reviewer → code-quality-reviewer chain.** That chain 4×'d time-to-ship and produced no better code than one careful pass.
3. **No new sub-project umbrella issues without a user-visible deliverable named in the first sentence.** If the umbrella is "make a clean state machine" → no. If it's "click ticker → see financials" → yes.
4. **Delete before adding.** Phase 1 is deletion. Don't start Phase 2 until jobs are collapsed + services merged.
5. **Codex runs pre-push. Bot review informs but doesn't gate if Codex + local gates agree.** Bot quota permitting.
6. **Docs are comments in PRs, not separate spec files.** Exception: this doc (the overall refocus).
7. **Tests assert behaviour, not fixtures.** No "test that MagicMock was called" without a functional assertion.

## Success criteria (measurable, finite)

- [ ] Admin page has zero red rows (Phase 1.5).
- [ ] `du -sh data/raw` < 1 GB (Phase 1.4 + retention sweep flipped on).
- [ ] Scheduled job count ≤ 8 (Phase 1.1–1.4).
- [ ] `app/services/` file count < 30 (down from 41; Phase 1.1 collapses ~6-8 files).
- [ ] `/instrument/AAPL` renders financials grid with data from SEC XBRL (Phase 2).
- [ ] `/instrument/VOD.L` (non-US) renders financials grid via yfinance (Phase 2).
- [ ] Dashboard shows live P&L for held positions with 5-min refresh (Phase 3).
- [ ] Quote flicker during market hours (Phase 4).
- [ ] One usable Sunday-evening research session end-to-end: open Dashboard → click a held position → read Analysis tab → decide to hold/add/trim (Phase 2 + 3 done).

## Next session

Start fresh. Paste the anchor at the top (see this doc's intro). Begin Phase 1.1 (merge SEC jobs). First PR should:

1. Close PR #343 (redundant A.5 docs).
2. Close umbrella issues #329 / #330 / #331 / #332 with "refocused; out of scope".
3. Commit this doc on a doc-only PR or fold into the first Phase-1 PR.
4. Start Phase 1.1 on a new branch: `feature/349-phase-1-collapse-sec`.

Then move through the phases.

One rule for the author: if a session goes >4 PRs without a visible-to-you UI change, stop and ask why.
