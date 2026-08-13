# #591 PR-B — risk-metrics service + endpoint (bite-sized TDD plan)

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development. Steps use `- [ ]` checkboxes. TDD throughout: failing test → run-fail → implement → run-pass → commit.

**Branch:** `feature/591-risk-metrics-service` (off main `3f7966ec`).
**Parent:** epic #591, roadmap R4. **Spec:** `docs/superpowers/specs/2026-06-14-instrument-risk-drill-design.md` (§PR-B authoritative). PR-A merged (PR #1631).

**Goal:** A versioned (`risk_v1`), windowed (1y/3y/full), quality-flagged backend risk-metrics evidence layer computed from `price_daily.close` (instrument + SPY), persisted in the repo's two-layer pattern (append-only partitioned observations + write-through current), refreshed by the orchestrator DAG, served by `GET /instruments/{symbol}/risk-metrics`. Page (PR-C) renders it; thesis/ranking consumption are filed follow-ups (#1632/#1633).

> **Plan rev 2** — Codex ckpt-1 (2026-06-14) rejected a current-only simplification (BLOCKER): `price_daily` is mutable (`market_data.py:321 ON CONFLICT … DO UPDATE` — vendor corrections rewrite OHLCV), so risk metrics are NOT reconstructable and the append-only observation history captures real, otherwise-lost state. Reverted to the spec's append-only design via the repo's two-layer pattern. Also folded Codex's 4 wiring fixes (orchestrator-driven not standalone-cron; INIT_CHECKS["candles"]; per-instrument as_of_date; current-table as_of advance) + math pins.

---

## Storage — two-layer (data-engineer recipe + spec append-only)

Mirrors the canonical ownership two-layer pattern (`sql/114_ownership_institutions_observations.sql`, partition horizon `sql/177`):

**`instrument_risk_metrics_observations`** — append-only audit log, `PARTITION BY RANGE (as_of_date)`, **quarterly** partitions (mirror `sql/177` template; real lower bound `2010-01-01`, never MINVALUE) + a `_default` catch-all.
- PK `(instrument_id, as_of_date, metric_version, window_key, computed_at)` (window_key in PK — else 1y/3y/full collide; **`computed_at` in PK** — Codex rev2 BLOCKER: a vendor correction to a historical bar that does NOT advance the latest close date produces different metrics on the same `(instrument_id, as_of_date, version, window_key)` — without `computed_at` the recompute collides and is silently dropped, defeating the whole append-only-because-mutable rationale). `as_of_date` is the partition key and is in the PK ✓ (Postgres requires partition key ⊆ unique key).
- **Content-dedup append:** INSERT a new observation only when the computed content **differs** from the most recent existing row for `(instrument_id, as_of_date, metric_version, window_key)` (look up `ORDER BY computed_at DESC LIMIT 1`, compare scalars+statuses). Identical re-run (e.g. intra-day manual re-trigger, no new data) → skip. Correction (same as_of, changed content) → new `computed_at` row. Normal weekly advance (new as_of) → new key → always insert. Never UPDATE in place (audit history is immutable).
- `as_of_date DATE NOT NULL` = the date of the **instrument's own latest VALID close** actually used by the compute (Codex rev2 MED: NOT raw `MAX(price_date)` — `close` is nullable and invalids are excluded/break the chain; stamping a date whose close wasn't used would lie). If the latest raw row is invalid/null, as_of = the prior valid date.
- Columns (all returns stored as **FRACTIONS**, 0.10 = 10%; column names carry NO `_pct` suffix — the suffix would lie; the API/FE multiplies ×100 for display): `cagr`, `excess_cagr_vs_spy`, `max_drawdown`, `max_dd_peak_date DATE`, `max_dd_trough_date DATE`, `current_drawdown`, `vol_annualized`, `beta`, `beta_r2`, `skew`, `excess_kurtosis`, `var_5` (signed), `worst_day`, `best_day`, `calmar`, `trailing_1m`/`3m`/`6m`/`1y`, `excess_trailing_1m`/`3m`/`6m`/`1y` (all `NUMERIC`).
- Evidence: `n_returns INT` (instrument own chain length — floors vol/dist/cagr/trailing), `beta_n_obs INT` (aligned-pair count — floors beta/excess), `benchmark_instrument_id BIGINT` (stored + validated at write; **no DB FK** — observation log must survive instrument churn, per data-engineer), `window_days INT`, `computed_at TIMESTAMPTZ NOT NULL DEFAULT now()`.
- Per-metric **discrete CHECK status columns** (NOT a `quality` JSONB — zero JSONB-status precedent; closed enum; indexable): `cagr_status`, `vol_status`, `beta_status`, `drawdown_status`, `distribution_status`, `calmar_status`, `trailing_status`, each `TEXT CHECK (… IN ('ok','insufficient_history','partial_window','benchmark_missing','benchmark_insufficient_history','invalid_price_chain','stale'))`. Per-metric grain so an instrument with good prices but no SPY overlap reads `vol_status='ok'`, `beta_status='benchmark_missing'`.
- `window_key TEXT CHECK (window_key IN ('1y','3y','full'))` (extendable by a one-line later migration if 6m added).
- `metric_version TEXT NOT NULL` (in PK so `risk_v2` coexists).
- **Write-boundary guard:** reject `as_of_date > current_date` (DEFAULT-partition alarm is a lagging indicator otherwise — prevention-log §DEFAULT). Test `_default` is empty post-backfill.
- Index `(instrument_id, as_of_date DESC)` — the `_current` rebuild source + operator "as of date X" query.

**`instrument_risk_metrics_current`** — latest-only write-through for fast reads.
- PK `(instrument_id, metric_version, window_key)`. Same scalar/status/evidence columns + `as_of_date DATE NOT NULL` + `computed_at TIMESTAMPTZ` (carried from the winning observation, for the tuple tiebreak) + `refreshed_at TIMESTAMPTZ` (this table's own write time, SET-only).
- Populated from observations: `INSERT … SELECT DISTINCT ON (instrument_id, metric_version, window_key) … ORDER BY instrument_id, metric_version, window_key, as_of_date DESC, computed_at DESC ON CONFLICT (instrument_id, metric_version, window_key) DO UPDATE SET <all cols incl as_of_date, refreshed_at> WHERE (excluded.as_of_date, excluded.computed_at) > (instrument_risk_metrics_current.as_of_date, instrument_risk_metrics_current.computed_at) OR (<business scalar+status cols>) IS DISTINCT FROM (<excluded …>)`. The `(as_of_date, computed_at) >` tuple advances `_current` to a newer snapshot OR a same-as_of correction (Codex rev2: deterministic tiebreak once same-as_of observations exist). The `as_of_date >` clause makes a fresh snapshot advance `as_of_date` even when rounded values are unchanged (Codex HIGH — SET-only would freeze the page's data date); `refreshed_at`/`computed_at` are in the SET only, **never** in the IS DISTINCT FROM tuple (prevention-log §MERGE bloat variant-A). Churn = ~15k rows/week max, trivial.
- Index `(window_key, metric_version)` for the ranking "all instruments at 3y" scan (#1633).

**Retention:** none shipped — the observations table IS the audit log and is kept. Justified in the migration header: at the 7-day cadence growth is ~5,138 subjects × 3 windows × 52 ≈ 800k rows/yr (~150 MB/yr), quarterly-partitioned, crosses 1M rows in ~15mo and 1GB in ~6yr — bounded; `DROP PARTITION` is available if a horizon is ever wanted; retention would defeat the audit purpose. File a tracking follow-up (#TBD risk-metrics retention-when-needed). This is the data-engineer's "OR justify in the migration comment" escape from the mega-table NOT-MERGEABLE clause.

**Mega-table compliance:** partitioned at design-time ✓ (>1M rows within 15mo); real lower bound + DEFAULT-empty test ✓; bloat-safe upsert ✓.

---

## Orchestrator wiring — DAG-driven, NOT a standalone cron (Codex HIGH)

`daily_candle_refresh` is **not** in `SCHEDULED_JOBS`; the candles layer is fired by `orchestrator_full_sync` (daily 03:00 UTC walks the DAG, calls each layer's `refresh` adapter, gated by the layer's own `cadence` + `is_fresh`). Registering risk_metrics as BOTH a DAG node and a `ScheduledJob` would double-fire. So:
- **DAG node only** in `registry.LAYERS` with `cadence=Cadence(interval=timedelta(days=7))` — the orchestrator refreshes it weekly (skips while fresh).
- `_INVOKERS` entry + a `MANUAL_TRIGGER_JOB_SOURCES` lane entry → operator can `POST /jobs/risk_metrics_refresh/run` on demand; gives it a JobLock lane.
- **NO `ScheduledJob` entry.**
- `requires_layer_initialized=("candles",)` REQUIRES adding `INIT_CHECKS["candles"]` (Codex HIGH — only `universe` exists today); else the preflight gate raises "no INIT_CHECKS entry".

---

## Math contracts (quant wash — each becomes a unit test)

- **Returns:** SIMPLE, `r = close[i]/close[i-1] - 1` between **consecutive surviving** rows. Valid close iff finite & `> 0`. Invalid row **breaks the chain** — no gap-spanning synthetic return; r keyed to the **later** close's date.
- **Decimal everywhere EXCEPT one sanctioned float island:** skew, excess_kurtosis, var_5 percentile in `float` (numpy 2.4.4, already a dep; two-pass — mean then central moments) → quantized at the persistence boundary via `Decimal(str(round(v, 8)))` (note: rounding, pinned by test — not bit-exact quantize). Reason: Decimal has no fractional power for `m3/m2**1.5`; cubing tiny deviations in Decimal is numerically worse. Returns/vol/drawdown/beta/CAGR/Calmar stay end-to-end Decimal. `Decimal(str(x))` at every DB-numeric boundary (prevention-log #925).
- **vol:** sample std (n−1) × `Decimal(252).sqrt()`. `TRADING_DAYS=252` constant. ≥2 returns else null.
- **CAGR:** **calendar-time** `(final/first)^(Decimal(365)/Decimal(calendar_days)) - 1` (`calendar_days=(last-first).days`); NOT `(252/n_returns)` (inflates on gaps). Calmar numerator uses the SAME fn. `calendar_days==0` → null.
- **Calmar:** `annualized_return / abs(max_drawdown)`; `abs(max_dd) < Decimal("1e-9")` → null; inherits `partial_window`.
- **drawdown:** running peak; `dd[i]=close[i]/peak[i]-1` (≤0); `max_drawdown=min(dd)`, `current_drawdown=dd[-1]`, peak/trough dates.
- **beta (OLS):** date-aligned only — `{date:return}` both, **intersect keys**, regress on intersection. `beta=cov/var_m`, `r2=corr²` (closed form). n−1 denominators (match). Guards: `var_m==0`→beta null; `var_i==0 or var_m==0`→r2 null; `<2` pairs→both null + n_obs. **NEVER positional-zip.** No alpha.
- **distribution:** shared `_sample_std` (n−1, same as vol). `skew`, `excess_kurtosis=m4/m2²-3` (Fisher, biased moment form), `var_5`=type-7 linear-interpolation 5th percentile (`h=0.05·(n-1)`), **signed** (persist as-is), `worst_day=min`, `best_day=max`, `n_obs`. Low-sample flag `n_obs<250`.
- **trailing returns (1m/3m/6m/1y):** RECOMPUTED here (NOT `price_daily.return_*` latest-row-only cols). Window semantics: calendar lookback (1m=30d,3m=91d,6m=182d,1y=365d) from as_of; return = `close[as_of]/close[nearest valid close ≤ as_of−lookback] - 1`; null + status if no close ≥ lookback ago. `excess_trailing_* = instrument − SPY same calendar window` (uses beta-alignment dates; status `benchmark_missing` if no SPY). Floors on `n_returns`/`beta_n_obs`.
- **excess_cagr_vs_spy:** first-class — `cagr(instrument over aligned window) − cagr(SPY over aligned window)`; status mirrors beta (benchmark_missing / benchmark_insufficient_history). Own test.
- **windows:** standalone metrics (drawdown/CAGR/vol/dist/trailing) use instrument full valid history in window. **Benchmark metrics (beta, excess_cagr, excess_trailing)** use the aligned-overlap window from `max(first valid instrument return date, first valid SPY return date)`.
- **boundaries:** count **returns** not closes — vol/beta need ≥60 returns (≥61 consecutive closes; 61 closes with one mid-gap = 59 returns → fails). annualized partial_window threshold = <252 returns.
- **`invalid_price_chain` trigger:** invalids always break the chain (no synthetic return). A metric emits `invalid_price_chain` ONLY when invalids are why its valid sub-chain fell below its min-obs; otherwise compute normally on the valid sub-chain (a single break with enough remaining obs is `ok`/`partial_window`).

---

## File structure

Create: `app/services/risk_metrics.py`, `sql/198_instrument_risk_metrics.sql`, `tests/test_risk_metrics.py`, `tests/test_risk_metrics_wiring.py`, `tests/test_api_risk_metrics.py`.
Modify: `app/workers/scheduler.py` (const + job fn, **no ScheduledJob**), `app/jobs/runtime.py` (`_INVOKERS`), `app/jobs/sources.py` (`risk_metrics` Lane + `MANUAL_TRIGGER_JOB_SOURCES`), `app/services/sync_orchestrator/registry.py` (LAYERS node + `JOB_TO_LAYERS` + `INIT_CHECKS["candles"]`), `adapters.py` (`refresh_risk_metrics`), `freshness.py` (`risk_metrics_is_fresh`), `app/api/instruments.py` (models + endpoint), `.claude/skills/market-data/SKILL.md` + `.claude/skills/metrics-analyst/SKILL.md`.

---

## Task B1 — `risk_metrics.py` pure compute (TDD, no DB)

Constants: `RISK_METRICS_VERSION="risk_v1"`, `TRADING_DAYS=252`, `ZERO=Decimal("0")`, `MIN_RETURNS_VOL_BETA=60`, `MIN_RETURNS_ANNUALIZED=252`, `MIN_OBS_MOMENTS=250`, `CALMAR_DD_EPSILON=Decimal("1e-9")`, `WINDOW_KEYS=("1y","3y","full")`, status `Literal` + `RiskStatus` set, trailing lookback-day map.

Pure fns, each TDD'd (failing test → run-fail → implement → run-pass → commit per group):
- [ ] `daily_returns(rows) -> list[(date, Decimal)]` — valid-close chain. Tests: `[100,110,121]`→`[0.10,0.10]`; mid-series NaN/0/negative (each a case) → break, no gap return, correct date keys both sides; <2 valid → empty.
- [ ] `_sample_std(returns) -> Decimal|None` — n−1, None<2.
- [ ] `annualized_vol(returns)` — `_sample_std×Decimal(252).sqrt()`. Tests: hand std×√252; 1 return→None; equals distribution std.
- [ ] `drawdown(closes) -> DrawdownResult`. Tests: monotonic→0/0; V `[100,120,60,90]`→max=−0.5(trough@60),current=−0.25,peak@120; open dd→current==max.
- [ ] `ols_beta(inst, bench) -> BetaResult`. Tests: `r_i=2·r_m`→beta=2,r2=1,n_obs; `0.5·r_m+noise` hand value; flat bench→null; **date-misalign**→date-join beta=2 ≠ positional-zip; <2 pairs→null+n_obs; aligned start=max(first dates) excludes earlier inst-only history.
- [ ] `distribution(returns) -> DistributionResult` — float island. Tests: var_5 sign<0 + exact type-7 on fixed 20-array; symmetric skew~0; right-skew>0; heavy-tail kurt>0, normal~0; constant→variance0→null (no div0); n<250→low_sample; worst==min,best==max; persisted type is Decimal.
- [ ] `cagr(closes)` — calendar-time. Tests: 2× over 365d→1.0; **same total_return diff gap counts→SAME cagr** (anti-regression for 252/n_returns); calendar_days=0→null.
- [ ] `calmar(ann_ret, max_dd)`. Tests: known fixture; abs(max_dd)<1e-9→null; partial_window inherit.
- [ ] `trailing_return(closes, as_of, lookback_days)` + `excess`. Tests: nearest-prior-close pick; no close ≥lookback→null+status.
- [ ] `excess_cagr(inst, spy, window)` — first-class. Tests: aligned diff; no SPY→benchmark_missing.
- [ ] status helpers — (n_returns, aligned n, window coverage, benchmark presence, snapshot staleness, invalid-chain)→per-metric status. Tests: 60→ok/59→insufficient; <252→partial_window (251 vs 252); SPY absent→beta benchmark_missing while vol ok; SPY short overlap→benchmark_insufficient_history; invalids dropping below min→invalid_price_chain.
- [ ] `compute_instrument_risk(inst_closes, spy_closes, window_key, as_of_date) -> WindowMetrics` — orchestrates per window; window slice + benchmark alignment. Tests: slicing; full-window benchmark alignment with different starts.

Result types: frozen dataclasses carrying scalars + per-metric status + n_obs.

## Task B2 — `sql/198_instrument_risk_metrics.sql`

- [ ] `instrument_risk_metrics_observations` partitioned quarterly (mirror `sql/114`+`sql/177` DO-loop, lower bound `2010-01-01`, `_default`) + indexes, per Storage section. PK incl as_of_date+window_key. Per-metric CHECK status cols. No FK.
- [ ] `instrument_risk_metrics_current` (PK without as_of_date) + `(window_key, metric_version)` index.
- [ ] Header: provenance (#591 PR-B rev2), append-only-because-price_daily-mutable rationale, retention-none justification, spec link. Idempotent (`IF NOT EXISTS`, per-col `ADD COLUMN IF NOT EXISTS`). **198 is next free; never edit after applied (prevention-log #1333) — fixes go in 199.**
- [ ] db-tier test: `_default` empty post-insert; partition routing for a current-quarter as_of.

## Task B3 — persist layer in `risk_metrics.py`

- [ ] `load_close_series(conn, instrument_id, end_date)` — mirror `return_attribution._load_price_series` (`SELECT price_date, close FROM price_daily WHERE instrument_id=%s AND close IS NOT NULL AND price_date<=%s ORDER BY price_date ASC`), `Decimal(str(...))`.
- [ ] `compute_and_store_risk_metrics(conn) -> int`:
  - **Read phase under `snapshot_read(conn)`** (Codex rev2 HIGH — a plain `conn.transaction()` is READ COMMITTED; concurrent candle refresh/correction would let different instruments see different candle states). Resolve scope = instruments with **≥2 valid closes** (≥1 return computable) + `BENCHMARK_SYMBOLS`; load SPY close series once; load each scoped instrument's close series. Build all results in memory. (snapshot_read commits the pending txn on entry — do all reads here, no writes.)
  - **Compute:** per instrument, `as_of_date = date of the latest VALID close in its chain` (not raw MAX(price_date)); compute all windows (thin-history instruments get null metrics + per-metric `insufficient_history`/`partial_window` statuses — Codex rev2 HIGH: persist the flagged row, do NOT exclude, so the endpoint contract "insufficient-history returns flagged not absence" holds). Guard `as_of_date ≤ current_date`.
  - **Write phase (separate txn):** for each (instrument, as_of, version, window), content-dedup append into observations — read latest row for the key (`ORDER BY computed_at DESC LIMIT 1`), INSERT only if scalars+statuses differ (or none exists); then rebuild `_current` from observations via the `(as_of_date, computed_at) >`-gated upsert (Storage section). Return observation rows written.
  - Instruments with **0–1 valid closes** → no row (endpoint 404/no-data for those).
- [ ] db-tier tests: (a) synthetic candles 1 instrument + SPY → observation + current row, sane scalars+statuses; (b) re-run unchanged → no new observation (content-dedup), current unchanged (no churn); (c) **correction same as_of, changed historical close → new observation row (new computed_at) + current advances** (the rev2 BLOCKER scenario); (d) thin-history instrument (<60 returns) → persisted row with flagged statuses, not absent.

## Task B4 — orchestrator wiring

- [ ] `freshness.py`: `risk_metrics_is_fresh(conn)` = `_fresh_by_audit(conn, "risk_metrics_refresh", timedelta(days=7) × grace)`.
- [ ] `registry.py`: `INIT_CHECKS["candles"] = "SELECT EXISTS (SELECT 1 FROM price_daily)"` (Codex HIGH). `LAYERS["risk_metrics"] = DataLayer(name="risk_metrics", display_name="Risk Metrics", tier=2, cadence=Cadence(interval=timedelta(days=7)), is_fresh=risk_metrics_is_fresh, refresh=refresh_risk_metrics, dependencies=("candles",), requires_layer_initialized=("candles",), is_blocking=False, plain_language_sla="Recomputed weekly from price history.")`. `JOB_TO_LAYERS["risk_metrics_refresh"]=("risk_metrics",)`.
- [ ] `adapters.py`: `refresh_risk_metrics(*, sync_run_id, progress, upstream_outcomes)` → `_wrap_single(job_name="risk_metrics_refresh", layer_name="risk_metrics", legacy_fn=risk_metrics_refresh, progress=progress)`.
- Wiring tests: node present, depends candles, is_blocking False, INIT_CHECKS["candles"] present, JOB_TO_LAYERS maps, adapter callable.

## Task B5 — job fn + invoker + lane (NO ScheduledJob)

- [ ] `scheduler.py`: `JOB_RISK_METRICS_REFRESH="risk_metrics_refresh"`; `def risk_metrics_refresh() -> None` (mirror `portfolio_eod_snapshot_job`: `_tracked_job` + `psycopg.connect`; `tracker.row_count=compute_and_store_risk_metrics(conn)`). **No `ScheduledJob` entry** — orchestrator drives cadence.
- [ ] `sources.py`: add `"risk_metrics"` to `Lane` Literal; `MANUAL_TRIGGER_JOB_SOURCES["risk_metrics_refresh"]="risk_metrics"` (own write-disjoint lane, #1527 class). Extend starvation regression test for the new lane.
- [ ] `runtime.py`: import + `_INVOKERS[JOB_RISK_METRICS_REFRESH]=_adapt_zero_arg(risk_metrics_refresh)`.
- Wiring tests (mirror `tests/test_finra_regsho_daily_scheduler_wiring.py` minus the ScheduledJob asserts): constant, invoker `__wrapped__`, `source_for(...)=="risk_metrics"`, `MANUAL_TRIGGER_JOB_SOURCES` entry. Assert it is NOT in SCHEDULED_JOBS.

## Task B6 — endpoint `GET /instruments/{symbol}/risk-metrics`

- [ ] Pydantic models: `RiskWindowMetrics` (scalars as fractions + per-metric status + n_obs + window_key), `RiskSeries` (drawdown curve, rolling-vol line, histogram bins, beta-scatter points), `InstrumentRiskMetrics` (symbol, as_of_date, benchmark_symbol, windows: list, series). `Decimal|None` fields.
- [ ] `@router.get("/{symbol}/risk-metrics")` — resolve symbol→instrument_id (standard primary-listing tiebreak), 404 absent. `with snapshot_read(conn):` read `_current` scalars + compute on-read series via the SAME service fns **cut at the row's `as_of_date`**; any config read inside the block. Thin-history/missing-benchmark → honest status passthrough, never zeros (ZERO-fallback ban). Snapshot older than SLA → `stale`.
- [ ] db-tier test: data symbol → scalars+series+statuses; insufficient-history → flagged (not zeros); missing-benchmark → `beta_status='benchmark_missing'`.

## Task B7 — skill updates (same PR, skill-ownership rule)

- [ ] `market-data/SKILL.md`: TRADING_DAYS=252, vol×√252, calendar-CAGR, var_5 type-7, simple-return valid-close chain.
- [ ] `metrics-analyst/SKILL.md`: risk-metrics row (source→transform→table→endpoint→chart) + caveats (price not total return; Calmar not Sharpe; realized vol ≠ scorer TA vol-regime; mutable price_daily ⇒ observations is the audit record).

## Task B8 — dev backfill + DoD (operator step; I own the dev jobs proc)

After merge → restart dev jobs proc onto main → `POST /jobs/risk_metrics_refresh/run`. Record: `instrument_risk_metrics_current` populated for AAPL/GME/MSFT/JPM/HD (+SPY); cross-source ONE beta or vol vs a public source; `GET /instruments/AAPL/risk-metrics` sane scalars+statuses+as_of_date. SHA + figures in PR (ETL DoD 8–12).

---

## Process
1. Codex ckpt-1 re-confirm on THIS rev2 (deviation reverted; verify wiring fixes land) → fold.
2. subagent-driven-development per task (implementer + spec-review + code-quality-review).
3. `uv run pytest -m "not db"` + smoke; `-m db` for B2/B3/B6.
4. Codex ckpt-2 on branch before push.
5. Push, PR "Part of #591" (closing/ref verb in BODY); Claude review APPROVE + CI green; squash-merge.
6. B8 backfill + DoD; then PR-C.

## Self-review
- **Spec coverage:** all §PR-B metrics+statuses+windows+endpoint; append-only two-layer honors the spec (rev2 reverted the deviation).
- **Codex ckpt-1 folded:** BLOCKER (price_daily mutable → keep append-only) ✓; HIGH upsert as_of advance ✓; HIGH orchestrator-driven not standalone-cron ✓; HIGH INIT_CHECKS["candles"] ✓; HIGH per-instrument as_of ✓; MED units(fractions)/trailing-semantics/excess_cagr-first-class/invalid_price_chain-trigger/quantize-note ✓; MED n_obs grain (n_returns+beta_n_obs rule) ✓.
- **Personas folded:** quant math contracts → B1 tests; data-engineer (two-layer, quarterly partition, discrete CHECK status, bloat-safe upsert, no-FK, own lane, retention justification) → B2/B3/B5; consumers (per-metric status grain, n_obs per metric, TEXT window_key, 3y window) → B2/B6.
- **Prevention-log:** ZERO-fallback ban, `Decimal(str(x))`, snapshot_read, applied-migration immutability, ON-CONFLICT bloat predicate, mega-table partition-at-design-time.
