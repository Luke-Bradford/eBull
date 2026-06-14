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
existing dedupe set. **Order: held → T1/T2 → benchmark → T3** (benchmarks
inserted BEFORE the T3 bootstrap batch). Rationale (Codex ckpt-1): the
dedupe is order-sensitive and `_T3_BOOTSTRAP_SELECT` is `LIMIT`-bounded —
if benchmarks dedupe AFTER T3, a tier-3 benchmark with no candles would
consume a scarce T3 bootstrap slot. Inserting benchmarks first means a
benchmark already covered by held/T1-T2 is skipped, and tier-3
benchmarks do not steal T3 capacity. Always included regardless of
coverage tier → ongoing daily freshness. Mirrors the `held_rows`
"always included" rationale already in the file.

Resolution trusts the seeded universe: the query filters `is_tradable`
but not asset-class, so it does not guard a hypothetical symbol collision
(e.g. a non-ETF row sharing `XLC`). Low risk for this fixed symbol set;
noted so a reviewer need not re-derive it.

### One-shot backfill

After merge, run `refresh_market_data(provider, conn,
benchmark_instruments, force_backfill=True, skip_quotes=True)` once on
dev (jobs proc is operator-owned) to pull ~1000 bars (~4 trading years,
eToro per-request ceiling per #603) for each benchmark. `skip_quotes=True`
(Codex ckpt-1): scope is candle-ingest only — the default quote
fetch/upsert would add eToro call weight and contradict the
candle-only intent. No new endpoint; the steady-state inclusion is the
shipped code, the backfill is the operator-visible verify step.

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
- **Range picker slices client-side.** `CandleRange`
  (`frontend/src/api/types.ts:369`) has no `3y` member, so fetch `max`
  once and slice the in-memory series to the selected 1Y/3Y/5Y/All
  window. No backend candle-endpoint change.
- `CandleBar.{open,high,low,close,volume}` are `string | null` → math
  parses to number and null-guards.

### Pure math (`frontend/src/lib/riskMath.ts`, exported, unit-tested)

**Input validation (shared, Codex ckpt-1):** a close is *valid* only if
finite and `> 0`. `dailyReturns(bars)` consumes the candle rows in
date order, drops invalid closes, and computes a simple return
`close[i]/close[i−1] − 1` **only between two consecutive surviving rows**.
`price_daily` is trading-day-grained so consecutive rows are consecutive
trading days; a dropped/invalid row breaks the chain (no synthetic
return spanning the gap). Returns carry the end date.

- `drawdownSeries(closes): {date, drawdownPct}[]` — running peak →
  `(close/peak − 1) × 100`, ≤ 0. Valid closes only.
- `rollingAnnualizedVol(returns, window=30): {date, volPct}[]` —
  **sample** std dev (n−1 denominator) of daily simple returns × √252 ×
  100, over a trailing `window`-return window; emits a point only once
  ≥ `window` returns are available (so ≥ window+1 valid closes). `window`
  ≤ 1 or a window with < 2 returns → no point (n−1 undefined).
- `returnsHistogram(returns, bins=30): {bins: {binStart, binEnd,
  count}[], mean, std}` — `std` is sample (n−1), consistent with vol.
  **Degenerate guard:** `min === max` (constant returns, zero range) →
  a single bin holding all observations, never a zero-width division.
  Empty input → empty bins, `mean=std=null`.
- `olsBeta(assetReturns, benchReturns): {beta, alpha, r2, n} | null` —
  least squares of asset on benchmark over **date-aligned** returns.
  Alignment (Codex ckpt-1): pair returns by their end date and keep a
  point only when BOTH series have that return (i.e. both have closes at
  `t−1` and `t`); never pair across mismatched intervals. `n` = count of
  aligned return observations. **Zero-variance guards:** benchmark return
  variance 0 → β undefined → return `null`; total asset variance 0 → R²
  denominator 0 → return `null`. Otherwise `r2 ∈ [0,1]`.

All consume aligned daily simple returns derived from valid closes;
helpers are DB-free and the unit-test surface.

### Charts (`frontend/src/components/risk/riskCharts.tsx`, chartTheme)

1. Underwater area (fills below 0).
2. Rolling-vol line.
3. Returns histogram bar (mean + ±σ reference lines).
4. Beta scatter (asset vs SPY daily returns) + OLS regression line; card
   shows β and R².

### States

- Per-card empty-state (`EmptyState`): the threshold is in **return
  space** — vol/beta need ≥ 60 return observations (≈ 61 valid closes;
  Codex ckpt-1 off-by-one). Beta also empty when the SPY series is
  unavailable or `olsBeta` returns `null` (zero-variance / too few
  aligned returns).
- `SectionSkeleton` while loading, `SectionError` + retry on fetch fail.

### L3 raw (`?view=raw`)

Table of per-day {date, close, daily return, drawdown} + CSV export,
following the existing drill-page raw-view convention.

### Entry point

Link to `/instrument/:symbol/risk` from the PriceChart pane / chart
workspace (existing drill-link pattern).

## Testing

- `riskMath.ts`: pure unit tests — known-series fixtures for drawdown,
  vol (incl. < window → empty, sample-std value), histogram (normal,
  `min===max` single-bin, empty), OLS (synthetic β=1 / β=2 / zero-overlap
  → null / zero-benchmark-variance → null / R² value), `dailyReturns`
  (invalid/≤0 close breaks the chain, no synthetic gap-spanning return),
  date-alignment (asset and SPY with a non-overlapping holiday).
  Boundary: exactly 60 return observations (61 closes) renders; 59
  (60 closes) is empty.
- `RiskPage.test.tsx`: renders 4 cards with data; beta empty-state when
  SPY fetch fails; range slice changes series; `?view=raw` table + CSV.

## Risks

| Risk | Mitigation |
|------|-----------|
| 5Y/All overstate coverage (~4yr data today) | Honest empty-state per range; series grows as history deepens. |
| Beta vs SPY meaningless for non-US / ETFs | Empty-state on insufficient overlap; v1 SPY-only is documented. |
| Benchmark backfill not run post-merge | DoD requires executed-on-dev + recorded figure before PR-A done. |
