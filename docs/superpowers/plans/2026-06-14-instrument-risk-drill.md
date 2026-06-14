# Instrument Risk-Evidence Layer (#591) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship a versioned, auditable backend risk-metrics evidence layer for single equities, rendered by a frontend risk drill page and consumable by the thesis/ranking engines.

**Architecture:** Three sequential PRs. PR-A seeds benchmark candles (SPY+QQQ+sector SPDRs) into the existing daily candle refresh. PR-B adds a pure-Python `risk_metrics` service (versioned `risk_v1`, Decimal), a persisted `instrument_risk_metrics` table, an orchestrator DAG job that depends on the candles layer, and a `/instruments/{symbol}/risk-metrics` endpoint. PR-C is the frontend page that renders the endpoint with a naïve-user layer. Consumers (thesis/ranking) + sector-fix + total-return are filed follow-ups, not built here.

**Tech Stack:** Python 3.14 / FastAPI / psycopg / Postgres; React + recharts + TypeScript; eToro market-data provider.

**Spec:** `docs/superpowers/specs/2026-06-14-instrument-risk-drill-design.md` (approved; 5/5 committee + codex ckpt-1 ×2).

**PR ordering:** PR-A → PR-B → PR-C. PR-A lands first (hard SPY-verification gate). PR-B degrades benchmark metrics to `benchmark_missing` so it can merge before the dev backfill fully drains. PR-C depends on the PR-B endpoint shape.

**This document fully details PR-A.** PR-B and PR-C are scoped at component level here and each gets its own bite-sized plan authored when it is reached (separate PRs, separate review cycles; PR-B's exact shape firms up once PR-A is in place — per the writing-plans per-subsystem scope rule).

---

## File structure

**PR-A** (no schema change):
- Modify `app/workers/scheduler.py` — add `BENCHMARK_SYMBOLS` constant + benchmark scope sub-query folded into `daily_candle_refresh` dedupe (before T3).
- Test `tests/test_daily_candle_refresh.py` — benchmark scope inclusion + dedupe order.

**PR-B**:
- Create `app/services/risk_metrics.py` — pure compute (returns, drawdown, vol, beta/OLS, distribution, Calmar, windows, statuses), `RISK_METRICS_VERSION="risk_v1"`.
- Create `sql/198_instrument_risk_metrics.sql` — table.
- Modify `app/services/sync_orchestrator/registry.py` — `risk_metrics` layer node + `JOB_TO_LAYERS` entry.
- Modify `app/services/sync_orchestrator/adapters.py` + `freshness.py` — adapter + freshness.
- Modify `app/jobs/sources.py` — `risk_metrics` lane in `Lane` + `JOB_NAME_TO_SOURCE`.
- Modify `app/jobs/runtime.py` + `app/workers/scheduler.py` — `risk_metrics_refresh` job fn + invoker.
- Modify `app/api/instruments.py` — `GET /instruments/{symbol}/risk-metrics`.
- Tests: `tests/test_risk_metrics.py` (pure), `tests/test_api_risk_metrics.py` (endpoint), scheduler/registry wiring tests.

**PR-C**:
- Create `frontend/src/pages/RiskPage.tsx`, `frontend/src/components/risk/riskCharts.tsx`.
- Modify `frontend/src/api/instruments.ts` + `frontend/src/api/types.ts` — `fetchInstrumentRiskMetrics` + types.
- Modify `frontend/src/App.tsx` — route.
- Tests: `frontend/src/pages/RiskPage.test.tsx`.

---

## PR-A — benchmark candle ingest

**Branch:** `feature/591-benchmark-ingest` (off main). ETL-tier DoD applies.

### Task A1: `BENCHMARK_SYMBOLS` constant

**Files:**
- Modify: `app/workers/scheduler.py` (near `_T3_BOOTSTRAP_BATCH_SIZE`, ~L2031)
- Test: `tests/test_daily_candle_refresh.py`

- [ ] **Step 1: Write the failing test**

```python
def test_benchmark_symbols_constant_is_the_expected_set():
    from app.workers.scheduler import BENCHMARK_SYMBOLS
    assert BENCHMARK_SYMBOLS == frozenset(
        {"SPY", "QQQ", "XLB", "XLC", "XLE", "XLF", "XLI",
         "XLK", "XLP", "XLRE", "XLU", "XLV", "XLY"}
    )
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_daily_candle_refresh.py::test_benchmark_symbols_constant_is_the_expected_set -v`
Expected: FAIL — `ImportError: cannot import name 'BENCHMARK_SYMBOLS'`.

- [ ] **Step 3: Add the constant**

In `app/workers/scheduler.py`, beside `_T3_BOOTSTRAP_BATCH_SIZE`:

```python
# Benchmark instruments (S&P 500 + Nasdaq-100 + 11 GICS sector SPDRs)
# always candle-refreshed regardless of coverage tier so the risk layer
# (#591) has a benchmark series for beta/excess. Keyed by symbol
# (env-agnostic; resolved to ids at runtime). Candle-scope only — NOT
# promoted into scoring/ranking/thesis universe.
BENCHMARK_SYMBOLS: frozenset[str] = frozenset(
    {"SPY", "QQQ", "XLB", "XLC", "XLE", "XLF", "XLI",
     "XLK", "XLP", "XLRE", "XLU", "XLV", "XLY"}
)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_daily_candle_refresh.py::test_benchmark_symbols_constant_is_the_expected_set -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add app/workers/scheduler.py tests/test_daily_candle_refresh.py
git commit -m "feat(#591): BENCHMARK_SYMBOLS constant for candle ingest"
```

### Task A2: fold benchmark scope into `daily_candle_refresh` + exclude benchmarks from T3 SQL

**Why the SQL exclusion (Codex plan ckpt, BLOCKING):** `_T3_BOOTSTRAP_SELECT`
applies `LIMIT %(limit)s` in SQL *before* the Python dedupe, and its
`WHERE` matches `coverage_tier = 3 AND NOT EXISTS price_daily` — which is
exactly a never-yet-fetched tier-3 benchmark (SPY today). So merely
reordering the Python dedupe does NOT stop a benchmark from occupying one
of the 200 T3 slots. The T3 SQL must exclude `BENCHMARK_SYMBOLS`.

**Files:**
- Modify: `app/workers/scheduler.py` (`_T3_BOOTSTRAP_SELECT` ~L2046, `daily_candle_refresh` scope build ~L2108-2152)
- Test: `tests/test_daily_candle_refresh.py` (incl. updating the shared `_make_mock_conn` helper)

- [ ] **Step 1: Update the shared test helper for the new query**

`_make_mock_conn` (~L22-36) currently stubs exactly three `execute`
results (`held`, `tier12`, `t3`). The benchmark query is inserted between
tier12 and t3, so add a fourth:

```python
def _make_mock_conn(tier12_rows, t3_rows, held_rows=None, benchmark_rows=None):
    conn = MagicMock()
    result_held = MagicMock(); result_held.fetchall.return_value = held_rows or []
    result_12 = MagicMock();   result_12.fetchall.return_value = tier12_rows
    result_bm = MagicMock();   result_bm.fetchall.return_value = benchmark_rows or []
    result_t3 = MagicMock();   result_t3.fetchall.return_value = t3_rows
    conn.execute.side_effect = [result_held, result_12, result_bm, result_t3]
    return conn
```

The existing `test_t3_query_uses_limit_param` inspects
`call_args_list[2]` (was the T3 call) — bump it to `[3]`, and update its
param assertion to also expect the benchmark-exclusion param (Step 3):
`assert params == {"limit": _T3_BOOTSTRAP_BATCH_SIZE, "benchmark_symbols": sorted(BENCHMARK_SYMBOLS)}`.

- [ ] **Step 2: Write the failing tests**

Two assertions — benchmark inclusion+ordering, and the T3 SQL exclusion
(the slot-consumption guard the reorder alone can't prove):

```python
def test_daily_candle_refresh_includes_benchmark_before_t3():
    # held=[], tier12=[], benchmark=[(3000,"SPY")], t3=[(900,"AAA")]
    mock_conn = _make_mock_conn([], [(900, "AAA")], None, [(3000, "SPY")])
    # ... wire creds/provider/refresh capture per this file's existing tests ...
    ids = [iid for iid, _ in captured["instruments"]]
    assert 3000 in ids
    assert ids.index(3000) < ids.index(900)   # benchmark precedes T3

def test_t3_select_excludes_benchmark_symbols():
    # The T3 SQL must filter out BENCHMARK_SYMBOLS so a tier-3 benchmark
    # cannot consume a LIMIT-bounded T3 slot.
    assert "benchmark_symbols" in _T3_BOOTSTRAP_SELECT
    # and the runtime call passes the param (covered by the updated
    # test_t3_query_uses_limit_param assertion).
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `uv run pytest tests/test_daily_candle_refresh.py -v`
Expected: the two new tests FAIL; `test_t3_query_uses_limit_param` FAILS on the index/param change.

- [ ] **Step 4: Implement — benchmark query + T3 SQL exclusion + dedupe order**

(a) In `_T3_BOOTSTRAP_SELECT`, add to the `WHERE` (after the tier filter):

```sql
  AND i.symbol <> ALL(%(benchmark_symbols)s)
```

(b) Update the T3 call to pass the param:

```python
            t3_rows = conn.execute(
                _T3_BOOTSTRAP_SELECT,
                {"limit": _T3_BOOTSTRAP_BATCH_SIZE,
                 "benchmark_symbols": sorted(BENCHMARK_SYMBOLS)},
            ).fetchall()
```

(c) After the `tier12_rows` query and before the `t3_rows` query, add the
benchmark query:

```python
            # Benchmark instruments (#591): always included regardless of
            # coverage tier, like held_rows. Excluded from the T3 SQL
            # above so they never consume a LIMIT-bounded bootstrap slot.
            benchmark_rows = conn.execute(
                """
                SELECT instrument_id, symbol
                FROM instruments
                WHERE symbol = ANY(%(symbols)s)
                  AND is_tradable = TRUE
                ORDER BY symbol, instrument_id
                """,
                {"symbols": sorted(BENCHMARK_SYMBOLS)},
            ).fetchall()
```

(d) Change the dedupe loop source from `held_rows + tier12_rows +
t3_rows` to `held_rows + tier12_rows + benchmark_rows + t3_rows`, and add
`len(benchmark_rows)` to the `logger.info(...)` scope summary.

- [ ] **Step 5: Run tests + lint/typecheck**

Run:
```bash
uv run pytest tests/test_daily_candle_refresh.py -v
uv run ruff check app/workers/scheduler.py
uv run pyright app/workers/scheduler.py
```
Expected: all PASS / no errors (including the updated `test_t3_query_uses_limit_param`).

- [ ] **Step 6: Commit**

```bash
git add app/workers/scheduler.py tests/test_daily_candle_refresh.py
git commit -m "feat(#591): seed benchmark candles + exclude them from T3 bootstrap SQL"
```

### Task A3: one-shot dev backfill + verification (operator step, ETL DoD)

**Not code — an operational step run after the branch is on the dev jobs proc.** Records the DoD evidence the spec requires.

- [ ] **Step 1: Drive the one-shot backfill on dev**

```bash
# from repo root, dev .env, jobs proc restarted onto the branch
uv run python - <<'PY'
import psycopg
from app.workers.scheduler import BENCHMARK_SYMBOLS
from app.config import settings
from app.providers.implementations.etoro_market_data import EtoroMarketDataProvider
from app.services.market_data import refresh_market_data
url = settings.database_url
with psycopg.connect(url) as conn:
    rows = conn.execute(
        "SELECT instrument_id, symbol FROM instruments "
        "WHERE symbol = ANY(%s) AND is_tradable = TRUE ORDER BY symbol",
        (sorted(BENCHMARK_SYMBOLS),),
    ).fetchall()
api_key, user_key = ...  # load via the same path daily_candle_refresh uses
with EtoroMarketDataProvider(api_key=api_key, user_key=user_key, env=settings.etoro_env) as p, \
     psycopg.connect(url) as conn:
    s = refresh_market_data(p, conn, [(i, sym) for i, sym in rows],
                            force_backfill=True, skip_quotes=True)
    print(s)
PY
```

- [ ] **Step 2: Verify operator-visible figures**

```bash
curl -s "http://localhost:8000/instruments/SPY/candles?range=max" | python -c "import sys,json; d=json.load(sys.stdin); print('rows', len(d['rows']), 'latest', d['rows'][-1])"
```
Expected: hundreds of rows; latest close cross-checks to a public SPY source.

- [ ] **Step 3: Smoke the panel + record DoD evidence**

Confirm SPY/QQQ/XLK populated; AAPL/MSFT bar counts unchanged. Record in the PR description: instruments exercised, SPY bar count + latest close + the cross-source figure, the backfill summary, and the commit SHA (ETL DoD clauses 8-12).

---

## PR-B — risk-metrics service + endpoint (component outline)

Authored as its own bite-sized plan when reached. Components + key contracts (all detailed in the spec):

1. **`app/services/risk_metrics.py`** — pure compute, `RISK_METRICS_VERSION="risk_v1"`, Decimal. Functions: `daily_returns` (simple, valid-close chain), `drawdown` (max/current + peak/trough dates), `annualized_vol` (sample n-1 ×√252), `ols_beta` (date-aligned, β/r²/n_obs, zero-variance→null), `distribution` (skew/kurtosis/signed `var_5pct`/worst/best, n_obs), `calmar` (null on ~0 dd), trailing returns (recomputed, not the latest-row columns), per-metric `status` (the 7-state enum), windows (1y/3y/full; full benchmark = aligned overlap start). TDD: one test per function incl. the boundary (60 returns ok / 59 flagged), partial_window guard, and the degenerate guards.
2. **`sql/198_instrument_risk_metrics.sql`** — PK `(instrument_id, as_of_date, metric_version, window_key)`; scalar NUMERIC cols + per-metric-keyed `quality` JSONB + `n_obs`, `benchmark_instrument_id`, `window_days`, `computed_at`. Append-only.
3. **Orchestrator wiring** — `risk_metrics` layer node in `registry.py` (`dependencies=("candles",)`, `requires_layer_initialized=("candles",)`, `is_blocking=False`), `JOB_TO_LAYERS["risk_metrics_refresh"]=("risk_metrics",)`, adapter in `adapters.py`, freshness in `freshness.py`.
4. **Lane** — add `risk_metrics` to `Lane` Literal + `JOB_NAME_TO_SOURCE` in `app/jobs/sources.py` + a starvation regression test.
5. **Job** — `risk_metrics_refresh` in `scheduler.py` + invoker in `runtime.py`: compute covered universe + benchmarks, stamp one batch `as_of_date` = consistent candle snapshot, upsert table; skip if candles stale.
6. **Endpoint** — `GET /instruments/{symbol}/risk-metrics` in `app/api/instruments.py`: latest persisted scalars + on-read series cut at scalar `as_of_date`; `as_of_date` + per-metric status in payload. Integration test: data symbol → scalars+series+statuses; thin-history → flagged `no_data` not zeros.

DoD: `risk_metrics_refresh` run on dev; table populated for AAPL/GME/MSFT/JPM/HD; cross-source one beta/vol; `GET /instruments/AAPL/risk-metrics` sane. Record SHA + figures.

## PR-C — risk drill page (component outline)

Authored as its own bite-sized plan when reached:

1. **API client** — `fetchInstrumentRiskMetrics(symbol)` in `instruments.ts` + response types in `types.ts`.
2. **`RiskPage.tsx`** — route `instrument/:symbol/risk` in `App.tsx` (mirror DividendsPage); `useAsync`; pure render (no TS risk math); client-side range slice of the `max` series.
3. **`riskCharts.tsx`** — rebased-vs-SPY headline, underwater area (with recovery-frame caption from peak/trough dates), rolling-vol line, returns histogram, beta scatter + fit (β, R²); chartTheme.
4. **Naïve-user layer** — verdict chip (Calm/Medium/Bumpy/Wild, FE-only, driver-specific sentence), beta/vol English sentences + gauges, glossary tooltips (a11y), progressive disclosure, `?view=raw` table + CSV.
5. States — per-card `EmptyState` keyed on metric `status`; `SectionSkeleton`/`SectionError`; benchmark-missing empty-state.

DoD: page renders the panel on dev; honest empty-states on a thin-history symbol; raw tab + CSV verified.

---

## Tickets to file (tracking)

- **Rescope #591** into children: #591-A benchmark ingest, #591-B risk-metrics service, #591-C risk page (or three new issues linked to #585 + #591).
- **Follow-ups:** thesis-evidence ingestion; ranking risk-adjustment v2 (model-version change); sector-classification fix (symbol→GICS→SPDR map); total-return (dividend-adjusted) series; position-vs-portfolio correlation / marginal risk.

---

## Self-review

- **Spec coverage:** PR-A fully covers the benchmark-ingest section. PR-B/PR-C outlines map 1:1 to the spec's PR-B/PR-C sections + committee advisories (partial_window, immutability, FE-only chip, recovery caption, distinct vol naming). Follow-ups all listed.
- **Placeholder scan:** PR-A steps carry real code/commands. PR-B/PR-C are intentionally component outlines (separate plans, per the per-subsystem scope rule) — flagged as such, not hidden placeholders.
- **Type consistency:** `BENCHMARK_SYMBOLS` (frozenset[str]) consistent across A1/A2/A3; `RISK_METRICS_VERSION`, `window_key`, status-enum names consistent with the spec.
