# #591 Risk/Returns drill page — design

Status: approved (operator, 2026-06-14). Parent epic #585. Roadmap R4.

## Goal

New L2 drill page `/instrument/:symbol/risk` with quant risk/return view:
drawdown (underwater), rolling annualized volatility, returns histogram,
beta-vs-SPY scatter. Beta needs a benchmark price series we do not yet
ingest, so the work splits into two PRs.

## Settled inputs (do not re-debate)

- Library/theme/drill-contract locked by #585 spec
  (`docs/proposals/ui/instrument-charts-quant-redesign.md`): recharts +
  `frontend/src/lib/chartTheme.ts`, L1 pane / L2 route / L3 `?view=raw`.
- Operator decisions 2026-06-14: split data→feature PRs; seed
  benchmark set = SPY + QQQ + 11 GICS sector SPDRs; range picker
  1Y/3Y/5Y/All; beta vs SPY for all instruments with empty-state
  fallback.

## Why SPY has 0 bars (root cause)

`daily_candle_refresh` (`app/workers/scheduler.py` ~L2090) scopes the
candle universe to: held positions + T1/T2 covered tradable + a slow
T3 bootstrap batch. SPY (id 3000) is `is_tradable` but `coverage_tier=3`
and not held → only reachable via the throttled T3 batch → still 0
`price_daily` rows. Same for the sector SPDRs (tier 3). QQQ is tier 1 so
it already gets candles.

---

## PR-A — benchmark candle ingest (backend, ETL-tier)

### Constant (single source of truth)

`BENCHMARK_SYMBOLS` — frozenset of symbols, all confirmed present /
tradable / ETF in the universe:

```
SPY  QQQ  XLB  XLC  XLE  XLF  XLI  XLK  XLP  XLRE  XLU  XLV  XLY
```

S&P 500 + Nasdaq-100 + 11 GICS sector SPDRs. Keyed by **symbol**
(env-agnostic; resolved to `(instrument_id, symbol)` at runtime so a
symbol absent from a given env is silently skipped, not a hard ref to a
possibly-divergent id).

### Scope change

`daily_candle_refresh` gains a benchmark sub-query alongside
held/T1-T2/T3: resolve `BENCHMARK_SYMBOLS` → `(instrument_id, symbol)`
from `instruments WHERE symbol = ANY(...) AND is_tradable`, fold into the
existing dedupe set (held → T1/T2 → T3 → **benchmark**). Always included
regardless of coverage tier → ongoing daily freshness. Mirrors the
`held_rows` "always included" rationale already in the file.

### One-shot backfill

After merge, run `refresh_market_data(provider, conn,
benchmark_instruments, force_backfill=True)` once on dev (jobs proc is
operator-owned) to pull ~1000 bars (~4 trading years, eToro per-request
ceiling per #603) for each benchmark. No new endpoint; the steady-state
inclusion is the shipped code, the backfill is the operator-visible
verify step.

### Non-goals / invariants

- No schema change (`price_daily` + ETF instrument rows already exist).
- Benchmarks are NOT promoted into scoring/ranking/thesis universe —
  inclusion is candle-ingest scope only.
- Daily eToro call weight rises by 13 incremental fetches/day
  (negligible vs ~500-instrument incremental cadence).

### DoD (ETL clauses 8-12)

- Smoke: SPY, QQQ, XLK get bars; AAPL/MSFT unaffected.
- Cross-source: SPY latest close vs an independent public source.
- Backfill executed on dev (not "queued").
- Operator-visible: `GET /instruments/SPY/candles?range=max` returns a
  populated `rows[]`; record bar count + latest close + commit SHA.

---

## PR-B — risk drill page (frontend)

### Data flow

- Route `instrument/:symbol/risk` in `frontend/src/App.tsx` →
  `RiskPage.tsx` (mirrors `DividendsPage` registration).
- Single fetch: `fetchInstrumentCandles(symbol, "max")` for the
  instrument + `fetchInstrumentCandles("SPY", "max")` for beta. `useAsync`
  per the drill-page pattern; the SPY fetch degrades the beta card only
  (404 / empty → empty-state, rest of page renders).
- **Range picker slices client-side.** `CandleRange` has no `3y`
  (`frontend/src/api/types.ts:369` — 1y/5y/max only), so fetch `max`
  once and slice the in-memory series to the selected 1Y/3Y/5Y/All
  window. No backend candle-endpoint change.
- `CandleBar.{open,high,low,close,volume}` are `string | null` → math
  parses to number and null-guards.

### Pure math (`frontend/src/lib/riskMath.ts`, exported, unit-tested)

- `drawdownSeries(closes): {date, drawdownPct}[]` — running peak →
  `(close/peak − 1) × 100`, ≤ 0.
- `rollingAnnualizedVol(returns, window=30): {date, volPct}[]` — std dev
  of daily simple returns × √252 × 100; needs ≥ window+1 closes.
- `returnsHistogram(returns, bins=30): {binStart, binEnd, count}[]` +
  `{mean, std}` annotations.
- `olsBeta(assetReturns, benchReturns): {beta, alpha, r2, n}` — least
  squares on date-aligned daily returns; `n` = overlapping days.

All consume aligned daily simple returns derived from closes; helpers are
DB-free and the unit-test surface.

### Charts (`frontend/src/components/risk/riskCharts.tsx`, chartTheme)

1. Underwater area (fills below 0).
2. Rolling-vol line.
3. Returns histogram bar (mean + ±σ reference lines).
4. Beta scatter (asset vs SPY daily returns) + OLS regression line; card
   shows β and R².

### States

- Per-card empty-state (`EmptyState`): vol/beta need ≥ 60 aligned days;
  beta also empty when SPY series unavailable.
- `SectionSkeleton` while loading, `SectionError` + retry on fetch fail.

### L3 raw (`?view=raw`)

Table of per-day {date, close, daily return, drawdown} + CSV export,
following the existing drill-page raw-view convention.

### Entry point

Link to `/instrument/:symbol/risk` from the PriceChart pane / chart
workspace (existing drill-link pattern).

## Testing

- `riskMath.ts`: pure unit tests — known-series fixtures for drawdown,
  vol (incl. <window empty), histogram binning, OLS (synthetic β=1 / β=2
  / zero-overlap). Boundary: exactly 60 days.
- `RiskPage.test.tsx`: renders 4 cards with data; beta empty-state when
  SPY fetch fails; range slice changes series; `?view=raw` table + CSV.

## Risks

| Risk | Mitigation |
|------|-----------|
| 5Y/All overstate coverage (~4yr data today) | Honest empty-state per range; series grows as history deepens. |
| Beta vs SPY meaningless for non-US / ETFs | Empty-state on insufficient overlap; v1 SPY-only is documented. |
| Benchmark backfill not run post-merge | DoD requires executed-on-dev + recorded figure before PR-A done. |
