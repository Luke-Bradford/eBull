# #591 Risk/returns drill — design (rev 2: backend risk-evidence layer)

Status: design. Parent epic #585, roadmap R4. Supersedes rev 1 (FE-only).

## What changed from rev 1 and why

Rev 1 was a frontend-only recharts page computing risk math in client TS.
A 5-persona domain committee (quant, portfolio-manager, thesis-engine,
naïve-user, ranking-engine — 2026-06-14) reshaped it:

- The two *data consumers* (thesis AI + ranking engine, both Python)
  cannot consume client-side chart math — they need **structured,
  versioned, quality-flagged risk scalars**. Operator chose **Option B**:
  a backend risk-metrics service that the page renders AND the engines
  can ingest.
- Verified data constraints cut/changed several rev-1 assumptions
  (sector-relative views, Sharpe, total-return, indicator history).

## Operator-locked decisions

- Split delivery; benchmark seed = SPY + QQQ + 11 sector SPDRs (ingest
  now, future use); ranges 1Y/3Y/5Y/All; beta vs SPY with empty-state.
- **Architecture B** (2026-06-14): backend risk-metrics service +
  endpoint; page renders from it; thesis/ranking consumption enabled.
- **Consumer staging (author interpretation, confirm at review):** build
  the evidence layer + endpoint + page this round; file thesis-evidence
  ingestion and ranking risk-adjustment as dedicated follow-ups — a
  scoring-model change carries its own model-versioning / score-
  auditability burden (settled decisions) and must not ride a UI round.

## Verified data constraints (grep-confirmed 2026-06-14)

- **TA columns are latest-row-only** (`sql/025` header: only the most
  recent `price_date` row carries `volatility_30d`/`sma_*`/`macd_*`/etc;
  history is NULL — AAPL 1006 rows, 3 non-null). ⇒ no indicator *series*;
  rolling vol is computed from `close` returns. The latest
  `volatility_30d` is a current-scalar cross-check only.
- **Sector data unusable for sector-relative work.** `instruments.sector`
  is an opaque code 1–9 with no GICS/SPDR table; SPY/XLE/XLF/XLK/JPM are
  all `4`, AAPL `3` but MSFT `8`. ⇒ sector-relative beta/overlay would
  compare against the wrong ETF → **cut from v1** (follow-up to build a
  curated symbol→GICS→SPDR map). SPDRs still ingested in PR-A.
- **No risk-free-rate series** → no honest Sharpe. Risk-adjusted summary
  = **Calmar** (annualized return ÷ |max drawdown|); never labelled
  Sharpe.
- **Total-return not confirmed** (only price `close`). v1 = price return,
  labelled honestly; dividend-adjusted TR = follow-up.
- **Benchmark-definition mismatch.** `return_attribution.py` "market" =
  equal-weight Tier-1 basket, "sector" = equal-weight peers — NOT
  SPY/SPDR. This page's "vs SPY" is a different benchmark; label so.
- **Scorer has no realized-risk term** (`scoring.py` v1.1 has a 15%
  *volatility-regime* subcomponent, but it's **TA-based** — Bollinger
  position + ATR context — not realized vol / drawdown / beta;
  `volatility_30d` + `instrument_profile.beta` are ingested but unread by
  the scorer). Risk metrics here are surfaced as "risk context, not a
  v1.1 score input"; a future risk-adjusted score is a filed follow-up.

## Committee-converged metric set (the evidence)

Computed from `price_daily.close` series (instrument + SPY). Each is a
versioned, windowed, quality-flagged scalar; the page also shows the
matching chart.

1. **Rebased growth-of-100 vs SPY** (headline) — instrument & SPY indexed
   to 100 over the window; per-line CAGR. Chart: multi-line, optional log
   y. Scalar: `cagr`, `excess_cagr_vs_spy`.
2. **Max drawdown + current drawdown** — running peak on `close`;
   `max_drawdown_pct`, `max_dd_peak_date`, `max_dd_trough_date`,
   `current_drawdown_pct`. Chart: underwater area (≤0).
3. **Annualized volatility (realized)** — sample-std of daily returns ×
   √252; `vol_annualized_pct` over standard windows + rolling series for
   the chart. **Name it distinctly** from the scorer's separate TA
   "volatility regime" term (Bollinger/ATR) — the page/glossary must make
   clear this realized vol is NOT the figure the score uses (ranking
   validation).
4. **Beta vs SPY** — OLS of date-aligned daily returns; `beta`, `r2`,
   `n_obs`; static + rolling (no `alpha` in v1). Chart: scatter + fit
   line.
5. **Return distribution** — `skew`, `excess_kurtosis`, `worst_day_pct`,
   `best_day_pct`, `var_5pct` (empirical), `n_obs`. Chart: histogram +
   mean/±σ.
6. **Multi-horizon trailing returns vs SPY** — instrument
   `return_1m/3m/6m/1y` (precomputed) minus SPY same-window; the excess
   columns. Chart: grouped bar.
7. **Calmar (risk-adjusted summary)** — `calmar = annualized_return /
   abs(max_drawdown)`; the one headline risk-adjusted number.

Omitted (unanimous + #585 mandate): all day-trader TA — `rsi_14`,
`macd_*`, `stoch_*`, `bb_*`, `atr_14`, fast EMAs. `sma_200` may appear as
a faint regime line only. Raw values live in the raw/advanced tab.

---

## PR-A — benchmark candle ingest (backend, ETL-tier)

Unchanged from rev 1.

- `BENCHMARK_SYMBOLS` constant (SSOT): `SPY QQQ XLB XLC XLE XLF XLI XLK
  XLP XLRE XLU XLV XLY` — all present/tradable/ETF.
- Fold into `daily_candle_refresh` scope (`app/workers/scheduler.py`,
  `JOB_DAILY_CANDLE_REFRESH`) **before T3** in the dedupe order
  (held → T1/T2 → benchmark → T3) so tier-3 benchmarks don't steal T3
  bootstrap slots. Resolve symbols → `(id, symbol)` via `instruments
  WHERE symbol = ANY(...) AND is_tradable` (trusts the seeded universe;
  no asset-class collision guard — noted).
- One-shot backfill post-merge: `refresh_market_data(provider, conn,
  benchmark_instruments, force_backfill=True, skip_quotes=True)` on dev.
- No schema change. DoD: smoke SPY/QQQ/XLK + AAPL/MSFT unaffected;
  cross-source SPY close; backfill executed; `GET
  /instruments/SPY/candles?range=max` populated (record bars + close +
  SHA).

---

## PR-B — risk-metrics service + endpoint (backend)

### Service `app/services/risk_metrics.py`

Pure-compute functions over a daily close series, mirroring
`return_attribution.py` conventions: **Decimal** arithmetic for persisted
figures, a `RISK_METRICS_VERSION = "risk_v1"` constant (SSOT, no magic
strings), windows as named constants.

Math contracts (carry the rev-1 Codex ckpt-1 findings + rev-2 review):

- **Return basis: SIMPLE returns throughout v1** (Codex rev-2: a single
  basis avoids log/simple mismatch between displayed pct moves and the
  estimators; the daily log-vs-simple difference is negligible). Log
  returns are a documented future refinement. Return = `close[i]/close
  [i−1] − 1` between two *consecutive surviving* rows; a close is valid
  only if finite and `> 0`; an invalid row breaks the chain (no
  gap-spanning synthetic return).
- **Trailing returns recomputed here** from the close series with the
  service's own window/as_of rules — do NOT reuse `price_daily`'s
  precomputed `return_1m/3m/6m/1y` (latest-row-only, 400-row-fetch
  semantics that would leak into persisted evidence; Codex rev-2).
- **Volatility:** sample std (n−1) of daily returns × √252; emits only
  with ≥ 2 returns in window.
- **Beta:** OLS on **date-aligned** returns (pair only dates where BOTH
  series have a return); reports `beta`, `r2`, `n_obs`. Guards:
  benchmark-variance 0 → β `null`; total-variance 0 → R² `null`.
  **`alpha` dropped from v1** (Codex rev-2: intercept units undefined
  without a risk-free rate / annualization convention → unauditable;
  re-add with the risk-free follow-up).
- **Distribution:** sample (n−1) std; skew/kurtosis emit `n_obs`
  (flag below ~250). `var_5pct` = the empirical 5th-percentile of daily
  simple returns, **signed** (a loss is negative; one fixed convention
  across UI/CSV/consumers; Codex rev-2).
- **Calmar:** `annualized_return / abs(max_drawdown)`; `max_drawdown`
  near 0 → `null` (not ∞).
- **Min-window guard (quant validation):** annualized figures (CAGR,
  Calmar) below ~1y of returns are flagged `partial_window`, not shown as
  a precise number — a thin-history name must not surface a wild
  annualized value. Skew/excess-kurtosis below ~250 obs return `n_obs`
  and the FE de-emphasizes them (no precise-looking moment on thin
  samples).
- **"full" window:** standalone metrics (drawdown, CAGR, vol, dist) use
  the instrument's full valid history; **benchmark metrics** (beta,
  excess) use the aligned window starting at `max(first valid instrument
  return, first valid SPY return)` so the comparison is apples-to-apples
  (Codex rev-2).

### Quality flags (per metric)

Each metric carries a status (Codex rev-2 — coarse `benchmark_missing`
was not enough): `ok | insufficient_history | partial_window |
benchmark_missing | benchmark_insufficient_history | invalid_price_chain
| stale`. Thresholds in **return space** (vol/beta need ≥ 60 return obs
≈ 61 closes; Codex off-by-one). `benchmark_insufficient_history` =
SPY exists but the *aligned* overlap is too short. `stale` = the
backing candle snapshot is older than the freshness SLA. Never
substitute a fallback zero for unknown (the `return_attribution.py`
ZERO-fallback anti-pattern must NOT repeat here; honest status per the
#1581 precedent).

### Persistence `sql/198_instrument_risk_metrics.sql`

Table `instrument_risk_metrics`:
PK `(instrument_id, as_of_date, metric_version, window_key)` — `window_key`
**must** be in the PK (Codex rev-2: otherwise the 1y/3y/full rows
collide). One row per (instrument, snapshot, version, window). Columns:
scalar NUMERIC values + a per-metric `*_status` (or one `quality` JSONB),
`n_obs`, `benchmark_instrument_id`, `window_days`, `computed_at`.
Persisted `window_key` ∈ {`1y`, `3y`, `full`}. **No 5Y row** — given the
~4yr data ceiling 5Y ≡ full; the page's 5Y range is a display slice of
the `full` series, not a separate persisted window (Codex rev-2). This is
the auditable evidence row thesis/ranking consume; a thesis citing
"beta 1.3" resolves to `{value, window_key, as_of_date, metric_version,
status}`.

**Immutability (thesis validation):** rows are append-only per
`(instrument_id, as_of_date, metric_version, window_key)`; a math change
bumps `metric_version` (`risk_v2`) rather than mutating in place, so a
cached thesis citation stays reproducible. **Per-metric status must be
predicable** — if `quality` is JSONB, key it per metric so a consumer can
cheaply filter `quality->>'beta_status' = 'ok'` without parsing.

### Job `risk_metrics_refresh`

`daily_candle_refresh` is **orchestrator-driven, not a plain cron**
(Codex rev-2: it's wrapped as a DAG node in
`app/services/sync_orchestrator/adapters.py`, mapped to the `candles`
layer in `registry.py::JOB_TO_LAYERS`, freshness-gated in
`freshness.py`). So the risk job registers the same way, NOT as a bare
later cron:

- New `risk_metrics` layer node in `registry.py` with
  `dependencies=("candles",)` + `requires_layer_initialized=("candles",)`
  + its own `is_fresh` / `refresh`; `is_blocking=False`. Map
  `risk_metrics_refresh → ("risk_metrics",)` in `JOB_TO_LAYERS`; adapter
  in `adapters.py`; invoker in `app/jobs/runtime.py`.
- **Lane:** add a `risk_metrics` lane to the `Lane` Literal in
  `app/jobs/sources.py` + `JOB_NAME_TO_SOURCE`, with a starvation
  regression test (per-job-lane precedent #1527). "Own lane" alone is
  insufficient — the lane vocabulary + registry coverage must be added
  (Codex rev-2).
- **Batch consistency / concurrency (Codex rev-2):** the candles
  dependency guarantees fresh candles before compute; the job stamps a
  single `as_of_date` = the consistent candle snapshot
  (`max(price_date)` it read) for the whole batch, so it never mixes
  pre-/post-refresh instruments. If candle freshness fails its check the
  node is skipped (no half-stale batch).
- Backfill = one-shot invocation on dev post-merge.

### Endpoint `GET /instruments/{symbol}/risk-metrics`

In `app/api/instruments.py` (alongside `/candles`). Returns the latest
persisted scalars for the symbol **plus** on-read display series
(drawdown curve, rolling-vol line, histogram bins, beta-scatter points)
computed by the **same** service functions — single source of math, no
TS/Python drift. **Series are cut at the scalars' `as_of_date`** (Codex
rev-2: computing series over live `range=max` while serving scalars from
an older snapshot makes chart and table disagree). The response carries
`as_of_date` so the FE shows the snapshot date honestly; a metric whose
backing snapshot is older than the SLA passes through `stale`. Honest
per-metric status passthrough.

---

## PR-C — risk drill page (frontend)

- Route `instrument/:symbol/risk` in `frontend/src/App.tsx` →
  `RiskPage.tsx` (mirrors `DividendsPage`).
- Fetches `/instruments/{symbol}/risk-metrics` (+ position endpoint for
  the held overlay). `useAsync`. **No risk math in TS** — pure render of
  the endpoint payload; range picker slices the returned `max` series
  client-side (display only; scalars are window-labelled and
  authoritative).
- Charts (`components/risk/riskCharts.tsx`, chartTheme): rebased-vs-SPY
  headline, underwater area, rolling-vol line, returns histogram, beta
  scatter + fit (β, R²).
- **Naïve-user layer:** a plain-language verdict chip (Calm / Medium /
  Bumpy / Wild derived from vol+dd+beta) + one **driver-specific**
  sentence that names the dominant reason in plain terms (e.g. "Bumpy —
  has dropped 35% before and swings ~2× the US market"), not a generic
  label gloss; beta/vol rendered as English sentences with comparator
  gauges; glossary "?" tooltips (focusable, a11y); progressive
  disclosure — simple default (chip, rebased chart vs "the US market",
  worst-drop, returns row, beta sentence, dividend yes/no), advanced
  behind disclosure, raw values in the `?view=raw` tab. **The verdict
  chip is strictly FE-only** — derived for display, never persisted to
  `instrument_risk_metrics` nor read by thesis/ranking (else it backdoors
  a risk score the spec defers; PM validation).
- **Drawdown card framing (naïve validation):** the populated underwater
  card pairs max-drop with its recovery — uses the computed
  `max_dd_peak_date` → `max_dd_trough_date` to caption "fell X% from its
  {date} high, recovered by {date}" so a layperson doesn't read
  "−52%" as "will lose half my money."
- Held overlay: cost-line on the rebased chart, unrealized
  drawdown-from-entry (distinct from market max-DD), yield-on-cost — only
  when held.
- States: per-card `EmptyState` keyed on the metric `status` (e.g.
  `insufficient_history` → "Not enough history yet"), `SectionSkeleton`,
  `SectionError`. Benchmark-missing → honest beta/excess empty-state.
- `?view=raw`: per-day {date, close, daily return, drawdown} table + the
  raw scalar/flag list + CSV export.
- Entry link from the PriceChart pane / chart workspace.

---

## PR ordering / dependency

PR-A → PR-B → PR-C (Codex rev-2: not fully independent). **PR-A lands
first** with a hard SPY-verification gate (DoD requires SPY candles
present). **PR-B** is then shippable and is itself robust to a missing
benchmark — every benchmark metric degrades to
`benchmark_missing` / `benchmark_insufficient_history` rather than
failing — so PR-B can merge before the dev backfill fully completes.
**PR-C** depends on the PR-B endpoint shape. Each PR is independently
reviewable; the runtime quality of PR-B/PR-C beta/excess depends on
PR-A's SPY data.

## Follow-ups (file, do not bundle)

- **Thesis-evidence ingestion** — thesis engine reads
  `instrument_risk_metrics` as structured risk evidence (supports/
  contradicts a long thesis; the critic's falsification kit).
- **Ranking risk-adjustment v2** — a risk-adjusted scoring term
  (Calmar / vol / downside) in `scoring.py`; deliberate model-version
  change with its own validation. The scorer is *realized-risk-blind*
  today (it has a TA-based vol-regime term, no realized vol/dd/beta).
- **Position-vs-portfolio correlation / marginal risk contribution** —
  the true sizing metric (how much this candidate moves *my* book's
  risk); needs covariance vs current holdings, out of this single-
  instrument layer. Standalone beta-vs-SPY is the v1 proxy. (PM
  validation — filed explicitly.)
- **Sector classification fix** — curated symbol→GICS→SPDR map to
  re-enable sector-relative beta/overlay.
- **Total-return series** — dividend-adjusted closes so headline/dd/beta
  run on TR not price.

## Testing

- `risk_metrics.py`: pure unit tests — drawdown, vol (< window → flagged,
  sample-std value), distribution (skew/kurtosis with n_obs, signed
  empirical `var_5pct`), OLS (β=1 / β=2 / zero-overlap → null /
  zero-bench-variance → null / R²), Calmar (zero-dd → null), return chain
  (invalid/≤0 close breaks chain, simple-return basis), date-alignment
  (holiday gap), full-window benchmark alignment (instrument vs SPY with
  different start dates), trailing-return recompute (does not read the
  precomputed columns), boundary 60 returns (61 closes) ok / 59 flagged,
  status mapping (benchmark_insufficient_history vs benchmark_missing vs
  stale).
- Endpoint: one integration test — symbol with data returns scalars +
  series + statuses; insufficient-history symbol returns flagged
  `no_data` not zeros.
- `RiskPage.test.tsx`: renders cards from a payload; verdict-chip mapping;
  beta empty-state on `benchmark_missing`; range slice; `?view=raw` + CSV.

## DoD

- PR-A: ETL clauses (smoke / cross-source / backfill executed / live
  figure / SHA).
- PR-B: `risk_metrics_refresh` run on dev; `instrument_risk_metrics`
  populated for the panel (AAPL/GME/MSFT/JPM/HD); cross-source one beta
  or vol vs a public source; `GET /instruments/AAPL/risk-metrics`
  returns sane scalars + statuses. Record SHA + figures.
- PR-C: page renders the panel on dev; empty-states honest for a
  thin-history symbol; raw tab + CSV verified.

## Risks

| Risk | Mitigation |
|------|-----------|
| Scope (3 PRs + table + job) larger than #591-as-filed | Rescope #591 into children; PR-A/B/C independently shippable; consumers filed separately. |
| 5Y/All overstate coverage (~4yr data) | Honest per-window status; series grows. |
| TS/Python math drift | Single math source in `risk_metrics.py`; endpoint serves series; FE renders only. |
| Persisted metric goes stale if candle refresh lags | `as_of_date` surfaced; status reflects staleness; job ordered after candle refresh. |
| Sector data tempts a sector-relative view | Explicitly cut; documented; follow-up owns it. |
