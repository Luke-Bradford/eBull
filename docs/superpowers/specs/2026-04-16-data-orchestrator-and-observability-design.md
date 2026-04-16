# Data orchestrator and system observability

**Date**: 2026-04-16
**Status**: READY TO PLAN — Codex approval on round 15 (2026-04-16)

## Problem

eBull has 17 scheduled cron jobs (`SCHEDULED_JOBS` in [scheduler.py](app/workers/scheduler.py)) plus 2 manually-triggered jobs (19 total in [`_INVOKERS`](app/jobs/runtime.py#L123)). The operator must understand the ETL pipeline to know what to run, in what order, and why. The jobs page shows function names with brief notes — no live status, no progress, no duration, no error drill-down. Nothing feels live. Errors sit unmonitored.

This is backwards. The system should manage itself. Data should stay fresh according to its natural cadence. The operator should see a health dashboard, not a job scheduler. One "sync" button should check everything and update only what's stale, in the right order, as lightly as possible.

### Specific pain points

1. **17 jobs with implicit ordering.** No dependency awareness. Operator must mentally reconstruct the DAG.
2. **No live refresh.** Page loads once. Running jobs look the same as finished jobs.
3. **No progress indicators.** No sense of "what's happening right now" or "how far along."
4. **No duration tracking.** `started_at` and `finished_at` exist in `job_runs` but duration is never displayed.
5. **No error visibility.** Errors are truncated strings in a table. No drill-down, no failure streaks, no actionable detail.
6. **No catch-up intelligence.** After a week off, no way to know what's stale or trigger a smart refresh.
7. **Prices feel dead.** Hourly REST polling for quotes. No sub-minute updates. FX treated as a separate concern when it's just another instrument price.

### Relationship to existing specs

The [live pricing architecture spec](2026-04-13-live-pricing-architecture-design.md) (approved, 7 Codex rounds) covers:
- WebSocket connection for real-time prices
- Redis IPC for SSE fanout
- Currency conversion
- TradingView Lightweight Charts
- Market hours awareness

This spec covers the **remaining two pillars**: the data orchestrator (replacing cron jobs with a staleness-driven DAG) and the observability UI (replacing the jobs page with a system health dashboard). Together with the live pricing spec, these form the complete "self-managing, always-current" system.

---

## 1. Data layer model

Instead of 17 jobs, the system thinks in **data layers**. Each layer has a freshness contract, dependencies, and a refresh action.

### 1.1 Layer definitions

All layers use `job_runs` as the primary freshness watermark (see §1.3 for why data-table watermarks are unreliable). This table lists target cadence, dependencies, and refresh cost only.

| Layer | Freshness target | Dependencies | Refresh cost |
|---|---|---|---|
| `universe` | Daily | None | 1 API call (full list) |
| `cik_mapping` | Daily | `universe` | 1 API call (full mapping) |
| `candles` | Daily (after market close) | `universe` | Per-instrument, skip if today's candle exists |
| `financial_facts` | Daily | `cik_mapping` | 1 API call per CIK |
| `financial_normalization` | After `financial_facts` | `financial_facts` | Computed, no API |
| `fundamentals` | Quarterly | `universe` | 100-300 API calls/day |
| `news` | Every 4 hours | `universe` | Provider-dependent |
| `thesis` | Per review_frequency | `fundamentals`, `financial_normalization`, `news` | ~2 Claude calls per stale instrument |
| `scoring` | After upstream changes | `thesis`, `candles` | Deterministic formulas, no API |
| `recommendations` | After `scoring` | `scoring` | Portfolio review logic |
| `portfolio_sync` | Every 5 minutes | None (independent) | 1 API call (full reconciliation) |
| `fx_rates` | Every 5 minutes | None (independent) | 1 API call (Frankfurter) |
| `cost_models` | Daily | `universe` | Computed from quotes/trade history |
| `weekly_reports` | Weekly | None (reads latest of whatever is there) | Computed |
| `monthly_reports` | Monthly | None (reads latest of whatever is there) | Computed |

### 1.2 Dependency DAG

```
universe ─┬─→ cik_mapping ─→ financial_facts ─→ financial_normalization ─┐
          ├─→ candles ─→ (TA indicators computed inline)                 │
          ├─→ fundamentals                                               │
          ├─→ cost_models                                                │
          └─→ news ──────────────────────────────────────────────────────┤
                                                                         │
          ┌──────────────────────────────────────────────────────────────┘
          ↓
        thesis ─→ scoring ─→ recommendations

portfolio_sync (independent, high-frequency)
fx_rates (independent, high-frequency)
weekly_reports (independent, periodic)
monthly_reports (independent, periodic)
```

### 1.3 Freshness predicates

Each layer has an `is_fresh(conn) -> (fresh: bool, detail: str)` predicate. **Authoritative source of truth for all layers is `job_runs`** (the existing audit table written by `record_job_start`/`record_job_finish` in [ops_monitor.py](app/services/ops_monitor.py)). Data-table watermarks (e.g. `MAX(last_seen_at)`) are **not reliable** as freshness signals because several jobs update those fields only on actual change (a successful no-change run leaves them untouched).

Every layer freshness predicate therefore has this shape:

```text
fresh_by_audit   = latest job_runs row for this layer (see rule below) within the freshness window
fresh_by_content = per-layer content check (e.g. "no T1 instrument missing today's candle")
layer is fresh iff (fresh_by_audit AND fresh_by_content)
```

**What `fresh_by_audit` counts as "ran within window":**

Orchestrator adapters write skip reasons through a single helper `prereq_skip_reason(detail: str) -> str` which returns `"prereq_missing: {detail}"`. The constant prefix `PREREQ_SKIP_MARKER = "prereq_missing:"` lives in `app/services/sync_orchestrator.py` and is the **only** way PREREQ_SKIP rows are written; pre-orchestrator skips (legacy `record_job_skip` calls in the existing codebase) do not use it.

`fresh_by_audit` true iff the latest `job_runs` row for the layer (scoped to its target freshness window) matches one of:

- `status = 'success'`, or
- `status = 'skipped'` AND `error_msg` starts with `PREREQ_SKIP_MARKER`.

Everything else (`status = 'failure'`, `status = 'skipped'` without the marker) does **not** count — treated as stale.

Per-layer freshness predicates in the bullet list below therefore mean "latest counting audit row" per this rule, not "latest status='success'" literally. The phrase "latest successful `job_runs`" is a shorthand throughout the rest of the spec for this exact definition.

This split is load-bearing: a blocking layer whose latest row is `PREREQ_SKIP` is `fresh_by_audit = True` (dashboard amber, not red), but §2.4's dependency-skip logic still prevents its downstream from running in that sync — see §2.4. The dashboard separately surfaces "last PREREQ_SKIP with reason" so operators fix the prerequisite (e.g. add the missing API key).

Predicates below reference verified code locations:

- **`universe`**: latest successful `job_runs` for `nightly_universe_sync` within 24h. (Do **not** use `MAX(instruments.last_seen_at)` — the universe upsert at [universe.py:75](app/services/universe.py#L75) only updates `last_seen_at` on actual change, so no-change syncs leave it stale forever.)
- **`cik_mapping`**: latest successful `job_runs` for `daily_cik_refresh` within 24h.
- **`candles`**: both (a) latest successful `job_runs` for `daily_candle_refresh` within 24h **AND** (b) count of T1/T2 instruments where `MAX(price_date) < most_recent_trading_day(today)` = 0.
- **`financial_facts`**: latest successful `job_runs` for `daily_financial_facts` within 24h. (`daily_financial_facts` at [scheduler.py:1021](app/workers/scheduler.py#L1021) wraps both the SEC XBRL fetch and the normalization pipeline in a single job; the adapter in Phase 1 splits its outcome into the two layer rows — see §4.5.)
- **`financial_normalization`**: latest successful `job_runs` for `daily_financial_facts` within 24h, same source job as `financial_facts`. The two layers share one audit event because they run together; the adapter writes two `sync_layer_progress` rows (one per layer) but only one `job_runs` row (under the legacy job name). A finer-grained `normalization_runs` watermark is **out of scope** for this spec — added later if the two layers need to diverge.
- **`fundamentals`**: both (a) latest successful `job_runs` for `daily_research_refresh` within 24h **AND** (b) every tradable instrument has a `fundamentals_snapshot` row with `as_of_date >= current_quarter_start` (not "count of snapshots with older as_of_date = 0" — older rows are expected and do not mean staleness, per [001_init.sql:29](sql/001_init.sql#L29)).
- **`news`**: latest successful `job_runs` for `daily_news_refresh` within 4h. (NOTE: the current `daily_news_refresh` has no provider wired and always returns `row_count=0`. The freshness predicate is correct, but the layer is effectively a no-op today. Flag this as an implementation prerequisite before the layer can be considered meaningful.)
- **`thesis`**: both (a) latest successful `job_runs` for `daily_thesis_refresh` within 24h **AND** (b) `find_stale_instruments(conn, tier=1)` returns empty (per [thesis.py:143](app/services/thesis.py#L143)).
- **`scoring`**: latest successful `job_runs` for `morning_candidate_review` within 24h, AND `MAX(scores.scored_at) WHERE model_version = <default>` is newer than the latest thesis write and latest candle write. (Scoring has no `score_run_id` identity today — scoring-run identity is `(model_version, MAX(scored_at))` per [scores.py:11](app/api/scores.py#L11).)
- **`recommendations`**: `MAX(trade_recommendations.created_at)` is newer than `MAX(scores.scored_at)` for the default model, OR latest successful `job_runs` for `morning_candidate_review` within 24h.
- **`portfolio_sync`**: latest successful `job_runs` for `daily_portfolio_sync` within 5 minutes.
- **`fx_rates`**: latest successful `job_runs` for `fx_rates_refresh` within 5 minutes. (Do **not** use `MAX(live_fx_rates.quoted_at)` — the Frankfurter writer at [scheduler.py:1875](app/workers/scheduler.py#L1875) intentionally stores ECB **publication date** in `quoted_at`, which can be days old on weekends/holidays.)
- **`cost_models`**: latest successful `job_runs` for `seed_cost_models` within 24h. (Table is `cost_model` singular with `valid_from`/`valid_to` per [031_transaction_cost_model.sql:14](sql/031_transaction_cost_model.sql#L14) — there is no `cost_models` table and no `created_at` column to check.)
- **`weekly_reports`**: latest counting `job_runs` (§1.3 marker rule) for `weekly_report` within 7 days.
- **`monthly_reports`**: latest counting `job_runs` (§1.3 marker rule) for `monthly_report` within 31 days.

The `detail` string is human-readable: "3 instruments missing today's candle", "thesis stale for 5 instruments", "last sync 47 minutes ago".

---

## 2. Orchestrator

### 2.1 Sync entry points

Two public entry points (both defined fully in §2.2):

- `run_sync(scope, trigger) -> SyncResult` — synchronous. Plans, executes, finalizes in the caller's thread. Used by APScheduler triggers and boot-time catch-up.
- `submit_sync(scope, trigger) -> tuple[int, ExecutionPlan]` — async. Plans synchronously, submits execution to the worker thread pool, returns immediately. Used by the HTTP handler.

Both share the same `_start_sync_run` (synchronous gate + plan persistence) and `_safe_run_and_finalize` (the crash-guarded layer loop + finalize wrapper — §2.2). `_run_layers_loop` and `_finalize_sync_run` are only ever invoked from inside `_safe_run_and_finalize`. HTTP handlers call `submit_sync`, **not** the private helpers directly.

**Connection ownership:** the orchestrator **opens and owns its own connections**. Callers (HTTP handler, APScheduler job, boot-time catch-up) do not pass a connection in. This is deliberate — the orchestrator needs multiple short-lived transaction scopes (planning, per-layer, audit writes), and layer refresh functions already open their own connections (see [scheduler.py:539](app/workers/scheduler.py#L539)). Sharing a caller's transaction would break the per-layer isolation model in §2.2.

`SyncScope` options:

- `FULL` — walk entire DAG, refresh everything stale (default for "sync" button and catch-up).
- `LAYER(name)` — refresh a specific layer and its dependencies (power-user override). `name` is an emitted layer name (e.g. `financial_normalization`).
- `JOB(legacy_job_name, force: bool = True)` — run exactly the adapter mapped from `JOB_TO_LAYERS[legacy_job_name]` (1 or more emits). Persisted as `sync_runs.scope='job'` (new CHECK value — see §2.8 schema) with `scope_detail=legacy_job_name`. `force=True` (the default for legacy `POST /jobs/{name}/run`) means the planner includes the target adapter **even when its emits' freshness predicates say fresh** — manual triggers mean "run now regardless". Dependencies are included only if *they* are stale, never force-run. Used so callers of composite jobs like `daily_financial_facts` get the full pair `(financial_facts, financial_normalization)` without having to pick a "terminal" layer.
- `HIGH_FREQUENCY` — only independent high-frequency layers: `portfolio_sync`, `fx_rates` (for the periodic timer).

`SyncTrigger`: `'manual' | 'scheduled' | 'catch_up'` — persisted to `sync_runs.trigger`.

### 2.2 Execution algorithm

Two distinct public entry points — one synchronous for the scheduler + boot-time catch-up, one async for the HTTP handler:

- `run_sync(scope, trigger) -> SyncResult` — synchronous. Calls `_start_sync_run` then `_safe_run_and_finalize` inline. Returns only after finalize. Used by APScheduler and catch-up.
- `submit_sync(scope, trigger) -> tuple[int, ExecutionPlan]` — async entry point for HTTP. Calls `_start_sync_run` (synchronous — gate + plan persistence), submits `_safe_run_and_finalize` to the worker thread pool, returns immediately with `(sync_run_id, plan)`. The HTTP handler turns this into 202 + plan; the client polls `GET /sync/status`.

Both entry points share the same `_start_sync_run` and `_safe_run_and_finalize` — only the "do I wait for the loop?" choice differs. `_safe_run_and_finalize` is the **only** path that calls `_run_layers_loop` + `_finalize_sync_run`; no caller invokes either directly. This guarantees the crash/finalize guard applies uniformly.

Pseudocode reflects the invariants below. Real implementation will read very similarly.

```python
def run_sync(scope: SyncScope, trigger: SyncTrigger) -> SyncResult:
    """Synchronous entry: plan + execute + finalize in caller's thread."""
    sync_run_id, plan = _start_sync_run(scope, trigger)
    outcomes = _safe_run_and_finalize(sync_run_id, plan)
    return SyncResult(sync_run_id=sync_run_id, outcomes=outcomes)


def submit_sync(scope: SyncScope, trigger: SyncTrigger) -> tuple[int, ExecutionPlan]:
    """Async entry: plan + submit to worker; return before layers run."""
    sync_run_id, plan = _start_sync_run(scope, trigger)
    _manual_executor.submit(_safe_run_and_finalize, sync_run_id, plan)
    return sync_run_id, plan


def _safe_run_and_finalize(
    sync_run_id: int, plan: ExecutionPlan
) -> dict[str, LayerOutcome]:
    """Crash-guarded layer loop + finalize. Used by both entry points so any
    uncaught exception in _run_layers_loop (e.g. audit write failure, bug
    in outcome-building) still writes a terminal sync_runs row and releases
    the partial unique index gate.

    **Shared-outcomes contract:** the `outcomes` dict is passed into
    `_run_layers_loop` by reference so that successfully-completed layers
    recorded before a crash are preserved. The exception path only
    fills missing entries with FAILED; it does NOT overwrite already-
    recorded SUCCESS/NO_WORK/PARTIAL values."""
    outcomes: dict[str, LayerOutcome] = {}
    try:
        _run_layers_loop(sync_run_id, plan, outcomes)  # mutates outcomes in-place
    except Exception:
        logger.exception("sync run %s crashed in loop", sync_run_id)
        for lp in plan.layers_to_refresh:
            for name in lp.emits:
                outcomes.setdefault(name, LayerOutcome.FAILED)  # only missing entries
    finally:
        # Mark any sync_layer_progress row still 'running'/'pending' as failed
        # (best-effort; its own connection, swallows secondary errors). This
        # also updates `outcomes` in-memory for any layer the loop never reached.
        for name, outcome in _fail_unfinished_layers(sync_run_id).items():
            outcomes.setdefault(name, outcome)
        # Finalize the sync_runs row. Releases the gate. `_finalize_sync_run`
        # reads authoritative per-layer status from sync_layer_progress to
        # populate layers_done/layers_failed/layers_skipped, then cross-checks
        # against `outcomes` for drift logging. If they disagree, the DB rows
        # win (durable source of truth) and the drift is logged as a bug.
        try:
            _finalize_sync_run(sync_run_id, outcomes)
        except Exception:
            logger.exception(
                "sync run %s finalize failed — relying on boot reaper",
                sync_run_id,
            )
    return outcomes


def _start_sync_run(
    scope: SyncScope, trigger: SyncTrigger
) -> tuple[int, ExecutionPlan]:
    """Plan + gate + insert sync_runs and pending sync_layer_progress rows.
    UniqueViolation on idx_sync_runs_single_running → SyncAlreadyRunning
    carrying the active sync_run_id (looked up in a separate statement
    inside the exception handler so the 409 response can cite it)."""
    with psycopg.connect(settings.database_url, autocommit=True) as conn:
        try:
            with conn.transaction():
                plan = build_execution_plan(conn, scope)
                sync_run_id = _insert_sync_run(conn, scope, trigger, plan)
                _insert_layer_progress_rows(conn, sync_run_id, plan)
        except psycopg.errors.UniqueViolation:
            active_id = conn.execute(
                "SELECT sync_run_id FROM sync_runs WHERE status='running' LIMIT 1"
            ).fetchone()
            raise SyncAlreadyRunning(
                scope, active_sync_run_id=active_id[0] if active_id else None
            )
    return sync_run_id, plan


def _run_layers_loop(
    sync_run_id: int,
    plan: ExecutionPlan,
    outcomes: dict[str, LayerOutcome],
) -> None:
    """Walk layers in topological order, mutating `outcomes` in place as
    each layer completes. The caller passes `outcomes` by reference so
    that if the loop raises, the caller preserves partial results rather
    than losing successfully-recorded layers. Keys are emitted layer
    names (not LayerPlan.name)."""
    for layer_plan in plan.layers_to_refresh:
        # Resolve dependency outcomes FIRST. This combines outcomes from
        # this sync run (`outcomes` map) with last-counting job_runs rows
        # for unplanned (already-fresh) deps — see _build_upstream_outcomes.
        # Both the blocking gate and the refresh adapter see the same
        # resolved view.
        upstream_outcomes = _build_upstream_outcomes(layer_plan, outcomes)

        # Skip if any blocking dependency did not complete successfully.
        # Passes the resolved map so an unplanned dep whose latest job_runs
        # row is PREREQ_SKIP correctly blocks downstream execution.
        blocking_failure = _blocking_dependency_failed(
            layer_plan, upstream_outcomes
        )
        if blocking_failure is not None:
            for emitted_name in layer_plan.emits:
                _record_layer_skipped(sync_run_id, emitted_name, blocking_failure)
                outcomes[emitted_name] = LayerOutcome.DEP_SKIPPED
            continue

        for emitted_name in layer_plan.emits:
            _record_layer_started(sync_run_id, emitted_name)
        try:
            # `_invoke_layer_refresh` always returns a list of
            # (emitted_name, RefreshResult) pairs — one element for
            # single-layer adapters, N elements for composite adapters.
            results = _invoke_layer_refresh(
                layer_plan, sync_run_id, upstream_outcomes
            )
        except Exception as exc:
            logger.exception("layer %s failed", layer_plan.name)
            for emitted_name in layer_plan.emits:
                _record_layer_failed(sync_run_id, emitted_name, error=exc)
                outcomes[emitted_name] = LayerOutcome.FAILED
            continue

        # Contract-violation guard: returned names must be exactly the
        # planned emits, no more, no less, no duplicates. An adapter that
        # returns [] or omits/duplicates an emit is a bug; treat the whole
        # layer as FAILED and propagate the skip to dependents.
        returned_names = [name for name, _ in results]
        if (
            sorted(returned_names) != sorted(layer_plan.emits)
            or len(set(returned_names)) != len(returned_names)
        ):
            logger.error(
                "layer %s violated refresh contract: emits=%s returned=%s",
                layer_plan.name, layer_plan.emits, returned_names,
            )
            contract_exc = RuntimeError(
                f"refresh contract violation: expected {set(layer_plan.emits)}, "
                f"got {returned_names}"
            )
            for emitted_name in layer_plan.emits:
                _record_layer_failed(sync_run_id, emitted_name, error=contract_exc)
                outcomes[emitted_name] = LayerOutcome.FAILED
            continue

        for emitted_name, emitted_result in results:
            _record_layer_result(sync_run_id, emitted_name, emitted_result)
            outcomes[emitted_name] = emitted_result.outcome
    # no return — outcomes was mutated in place.


def _build_upstream_outcomes(
    layer_plan: LayerPlan, outcomes: dict[str, LayerOutcome]
) -> Mapping[str, LayerOutcome]:
    """Populate upstream_outcomes for this layer's dependencies.

    For each dep:
    - If the dep ran in THIS sync run, use `outcomes[dep]` directly (SUCCESS,
      NO_WORK, PARTIAL, PREREQ_SKIP, FAILED, DEP_SKIPPED).
    - If the dep did NOT run in this sync run (was already fresh at plan time),
      look up its LAST counting outcome from `job_runs` and map back to
      LayerOutcome: a `success` row → `SUCCESS`, a PREREQ_SKIP-marked `skipped`
      row → `PREREQ_SKIP`. Anything else → the layer's freshness predicate
      would not have counted it, so it cannot be a dep of a planned layer —
      treat as `FAILED` defensively.

    Why this matters (Codex round 7 blocker 2): a non-blocking dep like `news`
    can have PREREQ_SKIP as its last-counting row. The downstream thesis must
    see PREREQ_SKIP — not a falsely-collapsed SUCCESS — so its adapter can
    run with the "no recent news" fallback and record that in its detail."""
    resolved: dict[str, LayerOutcome] = {}
    for dep in layer_plan.dependencies:
        if dep in outcomes:
            resolved[dep] = outcomes[dep]
        else:
            resolved[dep] = _last_counting_outcome_from_job_runs(dep)
    return resolved
```

`_finalize_sync_run(sync_run_id, outcomes)` opens its own autocommit connection and reads the authoritative per-layer state from `sync_layer_progress` for the given `sync_run_id` — that's the source of truth because each layer's progress row was committed on its own connection and cannot be erased by a later loop crash. Those rows drive `layers_done`, `layers_failed`, `layers_skipped` and the terminal `status` (`complete` | `partial` | `failed`), plus `finished_at = now()`. The `outcomes` map passed in is used only for drift-logging: if the in-memory view disagrees with `sync_layer_progress` (e.g. a bug left a key FAILED in memory while the DB says complete), the DB rows win and the disagreement is logged. Terminal status transitions release the partial unique index gate so the next sync can start.

**Transaction invariants (load-bearing, every one of these is testable):**

1. **Every orchestrator connection uses `autocommit=True` + `with conn.transaction()`** — matches the pattern at [ops_monitor.py:322](app/services/ops_monitor.py#L322) (`record_job_skip`). This is the only way `with conn.transaction():` issues a durable COMMIT in psycopg3; without autocommit, the outer connection stays in an implicit transaction and `with conn.transaction():` becomes a SAVEPOINT, not a real commit. Review-prevention-log has seen this exact bug before.
2. **No connection is shared across layers.** Each `_invoke_layer_refresh` opens its own connection. Matches current services that require caller-owned commits ([portfolio_sync.py:521](app/services/portfolio_sync.py#L521), [transaction_cost.py:315](app/services/transaction_cost.py#L315)).
3. **Orchestrator audit writes** (`_record_layer_*`, `_finalize_sync_run`) each open a fresh short-lived connection with `autocommit=True` + `with conn.transaction()`. A layer rollback cannot erase its own "failed" progress row because the orchestrator's audit row is already committed on a different connection.
4. **On layer exception, the orchestrator never calls `rollback()`** — it has no shared connection to roll back. It writes the failure progress row on a fresh connection and continues.
5. **No retries within a sync run.** A failed layer is failed for that run. Next scheduled sync retries naturally via the freshness predicate.

### 2.3 Refresh contract (layer adapter)

Every layer's `refresh` callable **must** conform to this contract. Existing job functions in [scheduler.py](app/workers/scheduler.py) do **not** meet it today and must be adapted (see §8.1 for migration). The orchestrator treats contract violations as `FAILED` outcomes.

**Outcome enumeration.** Three distinct "ran successfully" outcomes avoid the freshness-vs-skip confusion Codex flagged:

```python
class LayerOutcome(str, Enum):
    SUCCESS      = "success"       # ran, did work, wrote rows
    NO_WORK      = "no_work"       # ran successfully, nothing to do this cycle
    PARTIAL      = "partial"       # ran, some items succeeded and some failed
    FAILED       = "failed"        # refresh aborted; treated as blocking for dependents
    DEP_SKIPPED  = "dep_skipped"   # never ran; upstream blocking layer failed (orchestrator-set)
    PREREQ_SKIP  = "prereq_skip"   # never produced useful work; prerequisite missing
                                    #   (e.g. missing API creds, unwired provider)

@dataclass(frozen=True)
class RefreshResult:
    outcome: LayerOutcome
    row_count: int              # rows written (audit-consistent with job_runs)
    items_processed: int        # for progress display
    items_total: int | None     # None if the layer is not item-oriented
    detail: str                 # human-readable summary
    error_category: str | None  # sanitized category; full error in logs (§3.4)

# Canonical refresh signature — one declaration for both single-layer and
# composite adapters. Single-layer adapters return a one-element list;
# composite adapters return one element per emitted layer.
class LayerRefresh(Protocol):
    def __call__(
        self,
        *,
        sync_run_id: int,
        progress: ProgressCallback,
        upstream_outcomes: Mapping[str, LayerOutcome],
    ) -> Sequence[tuple[str, RefreshResult]]: ...
```

**`upstream_outcomes` contract.** The orchestrator passes this mapping to every adapter; keys are the names in `layer_plan.dependencies` (not `emits`). Values are the **resolved** `LayerOutcome` for each dependency: when the dep ran in this sync run, its outcome from the `outcomes` map; when the dep was already fresh and not planned, the `LayerOutcome` derived from its last counting `job_runs` row via `_last_counting_outcome_from_job_runs(dep)`. Thesis uses this to detect a `PREREQ_SKIP` or `FAILED` news layer (whether from this run or the last counting row) and fall back accordingly — this is how "no silent stale-data consumption" is implemented in code.

**Mapping to existing `job_runs` status values.** `record_job_finish` currently accepts `success | failure` and `record_job_skip` writes `status='skipped'` (verified at [ops_monitor.py:291, 322](app/services/ops_monitor.py#L291)). Adapters map as follows — and `fresh_by_audit` counts `success` AND `skipped` (where the skip was `PREREQ_SKIP`, not `DEP_SKIPPED`) as "ran within window":

| `LayerOutcome` | `record_job_*` call | `sync_layer_progress.status` | Counts toward `fresh_by_audit`? |
|---|---|---|---|
| `SUCCESS` | `record_job_finish(status='success', ...)` | `complete` | yes |
| `NO_WORK` | `record_job_finish(status='success', row_count=0, ...)` | `complete` | yes |
| `PARTIAL` | `record_job_finish(status='success', ...)` + log warnings | `partial` | yes (best-effort watermark) |
| `FAILED` | adapter raises → `_tracked_job` records `failure` | `failed` | no |
| `DEP_SKIPPED` | adapter never runs | `skipped` | no |
| `PREREQ_SKIP` | `record_job_skip(reason=...)` | `skipped` | **yes** — the layer is "up to date" in the sense that it ran to its prerequisite check |

This three-way distinction is load-bearing. Without it, a `daily_news_refresh` that correctly skips due to missing provider credentials would either mark the layer permanently stale (blocking thesis forever) or silently mark it fresh (making the dashboard lie).

**Contract obligations on each layer adapter:**

- The adapter **opens and owns its own connections**. No connection is passed in. This is the `refresh: LayerRefresh` callable signature in §6.
- On unrecoverable failure the adapter **raises**. The orchestrator turns the exception into a `FAILED` outcome and writes the audit row.
- On `NO_WORK` (ran, nothing stale), the adapter calls `record_job_finish(status='success', row_count=0)`.
- On `PREREQ_SKIP` (e.g. missing API creds), the adapter calls `record_job_skip(reason=prereq_skip_reason(detail))` (the helper prefixes `PREREQ_SKIP_MARKER` — see §1.3) and returns `PREREQ_SKIP`. Without the marker, `fresh_by_audit` would lock the layer out forever.
- Each adapter writes exactly one `job_runs` row per call, before returning, via the existing `_tracked_job` helper or explicit `record_job_*` pair.

**Existing early-return holes to close in §8.1 migration** (all verified via grep):

- [scheduler.py:651](app/workers/scheduler.py#L651) `nightly_universe_sync`: `if creds is None: return` → convert to `record_job_skip` + `PREREQ_SKIP`.
- [scheduler.py:749](app/workers/scheduler.py#L749) `daily_candle_refresh`: same credential early-return.
- [scheduler.py:1080](app/workers/scheduler.py#L1080), [1135](app/workers/scheduler.py#L1135) `daily_news_refresh` / `daily_thesis_refresh`: missing Anthropic key early-return.
- [scheduler.py:1110](app/workers/scheduler.py#L1110) `daily_news_refresh`: "no provider wired" success-with-zero-rows path — becomes `PREREQ_SKIP` with `detail="news provider not configured"`.
- [scheduler.py:1150](app/workers/scheduler.py#L1150) `daily_thesis_refresh`: thesis query failure inside `_tracked_job` returning success.
- [scheduler.py:1206](app/workers/scheduler.py#L1206) `daily_portfolio_sync`: credential early-return.
- [scheduler.py:1266, 1290](app/workers/scheduler.py#L1266) `morning_candidate_review`: scoring / recommendation failures returning success.

Each line listed is either silently succeeding on a failure condition or marking the layer stale-forever on a legitimate skip. The Phase 1 adapter layer in §8.1 is what closes every hole.

### 2.3.1 Composite adapters (one legacy job → N layers)

Two current jobs refresh multiple layers in a single body:

- `daily_financial_facts` → emits `financial_facts` + `financial_normalization`.
- `morning_candidate_review` → emits `scoring` + `recommendations`. **Does not emit `execute_approved_orders`** — see the side-effect split below.

Composite adapters use the same `LayerRefresh` signature declared in §2.3 — the one canonical declaration. They return multiple `(emitted_name, result)` pairs instead of one.

**Invariants on composite adapters:**

- Composite emits are **atomic** for both execution and dependency-skip: the adapter runs once producing all emits together, or does not run at all. The dependency gate in §2.4 runs once per `LayerPlan` and, if it fires, writes the same `skip_reason` to every emitted layer's `sync_layer_progress` row. This is deliberate — a composite adapter cannot partially emit (the underlying legacy job is a single function call), so inter-emit dependencies inside a composite are not modelled. If the two emits ever need independent scheduling (e.g. normalization without fetching), the composite must be split into two separate adapters.
- Write exactly **one** `job_runs` row via `_tracked_job` under the legacy job name. This preserves existing ops_monitor history and tests.
- Write one `sync_layer_progress` row per emitted layer (the orchestrator loop does this in §2.2).
- If the legacy job raises, **all** emitted layers are marked `FAILED` with the same exception message. `LayerPlan.emits` for a composite is the ordered tuple of layer names.
- If the legacy job returns successfully, each emitted layer's outcome is reported individually by the adapter (e.g. facts=SUCCESS, normalization=NO_WORK is legal and useful).

**`morning_candidate_review` side-effect split (critical):**

The current [`morning_candidate_review` body](app/workers/scheduler.py#L1329) calls `execute_approved_orders()` after recommendations complete (guarded by kill-switch and `enable_auto_trading`). **The orchestrator adapter MUST NOT invoke `execute_approved_orders`.** Options for implementation:

1. **Preferred:** extract the scoring+recommendations logic from `morning_candidate_review` into a new function `compute_morning_recommendations()` that returns results without the order-execution side effect. The orchestrator adapter calls this; the legacy `morning_candidate_review` job body in scheduler.py becomes `compute_morning_recommendations()` followed by the existing `execute_approved_orders()` call — keeping legacy scheduled behaviour intact during Phase 1–3.
2. **Fallback if extraction is too risky:** the adapter explicitly pre-sets a module-level flag `_SUPPRESS_ORDER_EXECUTION = True` for its call, which the legacy body checks; clean up once Phase 4 removes the legacy scheduled fire.

The preferred option is required before the spec is `READY TO PLAN`. The fallback is a tech-debt option only if the extraction proves load-bearing on tests. Either way, **a FULL sync must never trigger `execute_approved_orders` as a side effect** — this is a safety invariant, not a convenience.

### 2.4 Dependency skip logic

When a layer's direct dependency (from the derived `LayerPlan.dependencies` per §2.6) did not complete successfully, the layer is **not executed** — its progress row is written with `status = 'skipped'` and `skip_reason` cites the failed direct dependency or the direct dependency that was `DEP_SKIPPED`. Transitive ancestor failures propagate only through planned intermediate layers that were themselves `DEP_SKIPPED`; fresh unplanned direct dependencies do not retroactively invalidate downstream execution (see §2.6 fresh-direct-dep-wins).

**"Did not complete successfully"** for a dependency means one of:

- `LayerOutcome.FAILED` (adapter raised or returned FAILED).
- `LayerOutcome.DEP_SKIPPED` (this dep was itself skipped because its own dep failed — transitive).
- `LayerOutcome.PREREQ_SKIP` **on a blocking layer** (a blocking layer that didn't run is equivalent to it failing — dependents must skip).

`LayerOutcome.PARTIAL` is **not** treated as "did not complete successfully" for dependency-skip purposes — partial is explicitly "some items worked". Downstream layers proceed. This is a deliberate design choice for layers like news or thesis that may partially succeed and still produce useful input.

```python
def _blocking_dependency_failed(
    layer: LayerPlan,
    upstream_outcomes: Mapping[str, LayerOutcome],
) -> str | None:
    """Return a skip_reason string if any blocking dep did not complete
    successfully; None if the layer is free to run.

    `upstream_outcomes` is the resolved map from _build_upstream_outcomes —
    it contains every dep, whether or not it was planned in this sync run
    (unplanned deps are populated from their last counting job_runs row).
    This lets the gate correctly block on an unplanned blocking dep whose
    latest row is PREREQ_SKIP."""
    blocking_bad_outcomes = {LayerOutcome.FAILED, LayerOutcome.DEP_SKIPPED}
    for dep_name in layer.dependencies:
        dep_outcome = upstream_outcomes[dep_name]  # always present post-resolution
        if dep_outcome in blocking_bad_outcomes and _is_blocking(dep_name):
            return f"blocking dependency {dep_name} did not complete ({dep_outcome.value})"
        if dep_outcome is LayerOutcome.PREREQ_SKIP and _is_blocking(dep_name):
            return f"blocking dependency {dep_name} skipped (prerequisite missing)"
    return None
```

**Per-layer `is_blocking` policy (stale-data safety):**

- `is_blocking = True` (default for all data-producing layers): `universe`, `cik_mapping`, `candles`, `financial_facts`, `financial_normalization`, `fundamentals`, `thesis`, `scoring`, `recommendations`, `cost_models`.
- `is_blocking = False` (tolerate stale data): `news`, `portfolio_sync`, `fx_rates`, `weekly_reports`, `monthly_reports`.
  - `news` is non-blocking because theses can fall back to "no recent news" without corrupting the thesis. But the thesis must **see** the news failure — propagated via `upstream_outcomes["news"]` kwarg on `LayerRefresh` (§2.3). The thesis adapter's `detail` string records whether it ran with stale or missing news.
  - `portfolio_sync` and `fx_rates` have no downstream consumers in the DAG (independent high-frequency layers). `is_blocking` is effectively N/A.
  - `weekly_reports` / `monthly_reports` are terminal layers; no downstream consumers.

**Invariant:** if a **direct** blocking dependency in `LayerPlan.dependencies` resolves to `FAILED` / `DEP_SKIPPED` / `PREREQ_SKIP`, the layer does not run. Non-blocking direct dependencies (news in thesis's dep set) never gate; the downstream runs with an explicit `upstream_outcomes` view of what happened. Transitive ancestors are out of scope for the gate — see §2.6 fresh-direct-dep-wins for the rationale (the orchestrator treats cached-fresh data as valid regardless of concurrent upstream refresh failures, because the freshness predicate already said so).

### 2.5 Concurrency guard (sync-level lock)

**Invariant (simplified from the earlier draft):** **at most one sync run is in `status='running'` at any time, regardless of scope.** No scope-overlap gymnastics. A high-frequency sync that arrives while a full sync is still running simply 409s and is skipped — the scheduler logs it and waits for the next 5-minute tick. This is safe because the high-frequency scope is a subset of the full scope, so a full sync already refreshes `portfolio_sync` and `fx_rates`.

**Authoritative gate: partial unique index** on `sync_runs`. The INSERT that creates the row is the atomic step that decides "can this sync start?". No cross-thread advisory locks.

```sql
-- Exactly one 'running' sync_runs row at any time.
CREATE UNIQUE INDEX idx_sync_runs_single_running
    ON sync_runs((TRUE))
    WHERE status = 'running';
```

**Why this is correct where an advisory lock isn't.** Codex round 2 correctly pointed out that Postgres session advisory locks are tied to the connection that acquired them. A request thread cannot acquire the lock and then hand the connection to a background executor thread — that's precisely the unsafe pattern [runtime.py:481](app/jobs/runtime.py#L481) and [locks.py:60](app/jobs/locks.py#L60) were written to avoid. The partial unique index sidesteps this entirely: the gate is the atomic INSERT itself, and the "lock" is the row's `status='running'` state. Releasing the lock = updating `status` in `_finalize_sync_run`.

**Request-to-worker handoff (§4.4 reworked):**

1. Request thread: call `_start_sync_run(scope, trigger)` per §2.2. This opens a connection, runs planning, INSERTs the `sync_runs` row with `status='running'`. Success = 202 with `sync_run_id`; `UniqueViolation` = 409.
2. Request thread: submit `_safe_run_and_finalize(sync_run_id, plan)` to the APScheduler `ThreadPoolExecutor` (the same `_manual_executor` already used by `JobRuntime.trigger`). This is the only entry point that calls `_run_layers_loop` + `_finalize_sync_run` — never the handler directly.
3. Request thread: close its connection and return 202.
4. Worker thread: `_safe_run_and_finalize` opens its own connections for layer execution and audit writes. Never touches the request thread's connection. On any uncaught exception in `_run_layers_loop`, it falls into the `finally` block that marks unfinished layers failed and calls `_finalize_sync_run` unconditionally — so `sync_runs.status` always transitions out of `'running'` and the gate always releases.

**Defence-in-depth (optional, inside worker only):** the worker's layer execution loop MAY acquire a per-layer Postgres advisory lock to serialize against legacy cron triggers of the same job name. This is exactly the pattern in [runtime.py:481](app/jobs/runtime.py#L481) — acquired on the worker's own connection, held for the duration of that layer's refresh, released at layer end. This lock is **not** the authoritative gate; the partial unique index on `sync_runs` is. This is here to prevent an orchestrator-triggered layer and a leftover cron-triggered job (during Phase 1–3) from racing each other inside the same underlying service.

**Orphaned rows (worker crashes mid-sync):** a `status='running'` row whose `started_at` is older than a configured timeout (default: 1 hour, longer than any realistic FULL sync) is considered dead. On app boot, a reaper updates every such row to `status='failed'` with `error_category='orchestrator_crash'` before starting the normal scheduler. This is the same pattern `ops_monitor` already uses for stuck `job_runs` rows.

**Scheduled-trigger behaviour on 409:** the APScheduler trigger that calls `run_sync` catches `SyncAlreadyRunning` and logs `scheduled <scope> sync skipped — sync <id> already running`. No retry inside the trigger — the next natural tick picks it up. Manual triggers from the UI return HTTP 409 with `{ "error": "sync_already_running", "sync_run_id": <int> }` and the UI shows a toast with a link to the running sync's status.

**Testable invariants this design guarantees:**

- `test_two_concurrent_posts_to_sync_returns_409`: second request to `POST /sync` while one is running returns 409 with the first `sync_run_id`.
- `test_crashed_sync_is_reaped_on_boot`: manually insert a `status='running'` row with `started_at = now - 2h`, boot the app, assert the row is `failed` with `error_category='orchestrator_crash'`.
- `test_finalize_releases_the_gate`: run sync A, wait for finalize, start sync B, observe B's INSERT succeeds without UniqueViolation.

### 2.6 Execution plan

`build_execution_plan()` produces:

```python
@dataclass(frozen=True)
class ExecutionPlan:
    layers_to_refresh: list[LayerPlan]    # topological order, only stale layers
    layers_skipped: list[LayerSkip]       # already fresh, with detail
    estimated_duration: timedelta | None  # from historical job_runs avg

@dataclass(frozen=True)
class LayerPlan:
    name: str                  # legacy job name (matches _INVOKERS key)
    emits: tuple[str, ...]     # layer names this plan produces; length 1 for single-layer
                               # adapters, 2 for composite (e.g. financial_facts +
                               # financial_normalization from daily_financial_facts).
                               # Sourced from JOB_TO_LAYERS at build_execution_plan time.
    reason: str                # "3 instruments missing today's candle"
    dependencies: tuple[str, ...]  # derived per rule below
    is_blocking: bool
    estimated_items: int
```

**`LayerPlan.dependencies` derivation rule (authoritative):**

`build_execution_plan` computes `LayerPlan.dependencies` from `LAYERS` as:

```text
emit_deps   = union over name in emits of LAYERS[name].dependencies
external    = emit_deps minus set(emits)         # remove intra-composite deps
dependencies = topological_sort(external)         # deterministic order
```

This gives the **direct external** dependency set — intra-composite edges (e.g. `financial_normalization → financial_facts` or `recommendations → scoring` within `morning_candidate_review`) are dropped because the underlying legacy job body runs them atomically. **Transitive ancestors are not computed and not included** — each LayerPlan's gate only inspects its direct dependencies.

**Fresh-direct-dep-wins semantics (intentional).** Transitive ancestor failures only propagate through layers that were themselves stale-and-planned in the current sync. Example: if `universe` is stale, planned, and fails, and `candles` is already fresh (not planned this sync), then `candles`'s last counting `job_runs` row is `success` from a prior run — `_build_upstream_outcomes` sees `candles = SUCCESS` and `morning_candidate_review` runs. This is correct behaviour: fresh cached data is still valid even if an upstream refresh job later fails; the freshness predicate already confirmed the cached data is within its target window. The universe failure is surfaced in the sync run result as a FAILED `universe` row (operator sees it on the dashboard), and the next sync will re-attempt universe and re-invalidate candles/etc only when their freshness windows expire.

The implication: "blocking dependency" gating protects against running downstream with `DEP_SKIPPED` or `PREREQ_SKIP` or `FAILED` data **observed in this sync run or recent enough to be the latest counting row**. It does not retroactively poison layers that were already fresh at plan time.

Worked examples (direct external deps only, no transitive closure):

- `morning_candidate_review` emits `(scoring, recommendations)`. `LAYERS["scoring"].dependencies = (thesis, candles)`. `LAYERS["recommendations"].dependencies = (scoring,)`. `emit_deps = {thesis, candles, scoring}`. `external = emit_deps - {scoring, recommendations} = {thesis, candles}`. Derived `dependencies = (thesis, candles)`.
- `daily_financial_facts` emits `(financial_facts, financial_normalization)`. `LAYERS["financial_facts"].dependencies = (cik_mapping,)`. `LAYERS["financial_normalization"].dependencies = (financial_facts,)`. `emit_deps = {cik_mapping, financial_facts}`. `external = emit_deps - {financial_facts, financial_normalization} = {cik_mapping}`. Derived `dependencies = (cik_mapping,)`.

`_blocking_dependency_failed` gates once per `LayerPlan` using this derived `dependencies` tuple. For single-layer plans, `dependencies = LAYERS[emits[0]].dependencies` unchanged.

Plan counts are persisted to `sync_runs` (see §2.8: `layers_planned` + `scope` + `scope_detail`). The `sync_layer_progress` row for each planned layer is inserted with `status='pending'` in the same transaction; it stores `items_total` (from `estimated_items`) but **not** `reason`, `dependencies`, or `is_blocking` — those live in the in-memory `ExecutionPlan` and are re-derived from the `LAYERS` registry on `GET /sync/status`. The full plan is returned to the UI synchronously on `POST /sync` so the user sees it before execution begins.

### 2.7 Progress tracking table

```sql
CREATE TABLE sync_layer_progress (
    sync_run_id    BIGINT NOT NULL REFERENCES sync_runs(sync_run_id),
    layer_name     TEXT NOT NULL,
    status         TEXT NOT NULL DEFAULT 'pending'
                   CHECK (status IN ('pending', 'running', 'complete', 'failed', 'skipped', 'partial')),
    started_at     TIMESTAMPTZ,
    finished_at    TIMESTAMPTZ,
    items_total    INTEGER,
    items_done     INTEGER,
    row_count      INTEGER,
    error_category TEXT,              -- sanitized category (see §3.4)
    skip_reason    TEXT,
    PRIMARY KEY (sync_run_id, layer_name)
);
```

Each layer calls `progress(items_done, items_total)` periodically during execution; the callback opens a short-lived connection, updates its own progress row, and closes. Interval: every N items **or** every 10 seconds, whichever comes first.

**Note:** `error_category` replaces the previously-proposed `error_msg` column. Full error detail (including stack traces) is written to **logs only**, not this table. See §3.4.

### 2.8 Sync runs table

```sql
CREATE TABLE sync_runs (
    sync_run_id    BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    scope          TEXT NOT NULL CHECK (scope IN ('full', 'layer', 'high_frequency', 'job')),
    scope_detail   TEXT,                 -- layer name if scope='layer', legacy job name if scope='job', NULL for 'full' and 'high_frequency'
    trigger        TEXT NOT NULL CHECK (trigger IN ('manual', 'scheduled', 'catch_up')),
    started_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at    TIMESTAMPTZ,
    status         TEXT NOT NULL DEFAULT 'running'
                   CHECK (status IN ('running', 'complete', 'partial', 'failed')),
    layers_planned INTEGER NOT NULL,
    layers_done    INTEGER NOT NULL DEFAULT 0,
    layers_failed  INTEGER NOT NULL DEFAULT 0,
    layers_skipped INTEGER NOT NULL DEFAULT 0,
    error_category TEXT                  -- sanitized category when status='failed' (§3.4)
);

CREATE UNIQUE INDEX idx_sync_runs_single_running
    ON sync_runs((TRUE))
    WHERE status = 'running';
```

`status = 'partial'` means some layers succeeded, some failed. Not a full failure, not fully complete. The partial unique index guarantees at most one `running` row across all scopes (§2.5). `error_category` is set by the orchestrator-crash reaper and by `_finalize_sync_run` when every attempted layer failed.

### 2.9 Scheduling

The orchestrator replaces most cron jobs with two scheduled triggers:

| Trigger | Schedule | Scope |
|---|---|---|
| **Full sync** | Daily at 03:00 UTC | `FULL` — walks entire DAG |
| **High-frequency sync** | Every 5 minutes | `HIGH_FREQUENCY` — portfolio_sync + fx_rates only |

Plus:

- **Catch-up on boot** — same as current behavior, but uses orchestrator instead of individual job catch-up.
- **Manual "Sync Now" button** — triggers `FULL` scope.

The 12 scheduled cron triggers that overlap the DAG are removed in §8.4; five (execute_approved_orders, monitor_positions, retry_deferred_recommendations, weekly_coverage_review, attribution_summary) stay on their own cron triggers. `nightly_universe_sync` and `daily_tax_reconciliation` are already on-demand only; no cron entries to remove. The existing job functions remain as the `refresh()` action inside each layer, adapted per the refresh contract (§2.3).

### 2.10 Backward compatibility with job_runs

The orchestrator writes to `job_runs` as before — each layer refresh still calls `record_job_start`/`record_job_finish` from inside its adapter. This preserves historical data and existing monitoring. The `sync_layer_progress` table adds the orchestrator-level view on top and links back via `sync_run_id`; `job_runs` keeps layer-level audit as today.

---

## 3. Observability UI

### 3.1 Mental model shift

**Before:** "Here are 17 jobs. Figure out which ones matter."
**After:** "Here's how fresh your data is. Everything green = you're current."

### 3.2 System health dashboard (replaces jobs page)

Three sections, top to bottom:

#### Section 1: System status banner

Single line at the top:
- **"All data current"** (green) — every layer fresh
- **"3 layers stale — sync recommended"** (amber) — some layers past freshness target
- **"Sync in progress — 4/12 layers complete"** (blue) — active sync running
- **"Sync failed — 2 errors"** (red) — last sync had failures

With a **"Sync Now"** button (right-aligned). Disabled with spinner while sync is running.

#### Section 2: Data layer grid

Each layer is a card/row showing:

| Field | Source |
|---|---|
| **Layer name** (human-readable) | Layer definition |
| **Status icon** | Green check / amber clock / red X / blue spinner |
| **Freshness detail** | From `is_fresh()` detail string |
| **Last updated** | `finished_at` from latest successful layer run |
| **Duration** | `finished_at - started_at` from latest run |
| **Progress bar** | `items_done / items_total` (only while running) |
| **Error** (expandable) | `error_category` + short detail from latest failed run (§3.4) |

Layers grouped visually by dependency tier:
- **Tier 0 (sources):** Universe, CIK, Portfolio Sync, FX Rates
- **Tier 1 (raw data):** Candles, Financial Facts, Fundamentals, News
- **Tier 2 (computed):** Normalization, Thesis, Cost Models
- **Tier 3 (decisions):** Scoring, Recommendations, Reports

#### Section 3: Recent activity log

Chronological list of recent layer runs (newest first). Replaces the "Recent Runs" table. Shows:
- Layer name, status, started/finished, duration, row count, error (expandable)
- Filterable by: status (failed/success/skipped), layer name
- Last 100 entries by default

### 3.3 Auto-refresh

The dashboard polls every 10 seconds while a sync is running (detected via `sync_runs` with `status = 'running'`). Drops to every 60 seconds when idle. No manual refresh needed.

Implementation: React Query with dynamic `refetchInterval`:

```typescript
const { data: syncStatus } = useQuery({
  queryKey: ["sync-status"],
  queryFn: fetchSyncStatus,
  refetchInterval: (query) =>
    query.state.data?.is_running ? 10_000 : 60_000,
});
```

### 3.4 Error visibility

Errors are first-class but server-sanitised. **No raw stack traces, SQL fragments, or driver internals ever reach the response body** — that is review-prevention-log rule #70 and must not be violated here just because the audience is "the operator."

What the UI shows:

- **Error category** (stable, finite set): `provider_auth`, `provider_rate_limit`, `provider_unavailable`, `db_constraint`, `db_connection`, `validation`, `unknown`. One per failed layer run, sourced from `sync_layer_progress.error_category`.
- **Short, human-readable detail** derived from the category + layer context (e.g. "SEC EDGAR rate limit hit — retry scheduled for next sync"). Server-side constructed; never contains exception `repr`.
- **Failure streak indicator:** if a layer has failed N consecutive times (computed from `job_runs` history), show "failed 3x in a row" badge.
- **Copy button:** copies `sync_run_id` + `layer_name` + timestamp + category — enough for an operator to grep logs, **not** the full trace.

Full tracebacks, SQL, and driver internals are written to **logs only** (standard Python logging through the existing `ops_monitor` path). Operators who need them read logs; the dashboard does not expose them.

### 3.5 Elapsed duration for running layers

For layers currently in `running` status, the UI shows a live elapsed timer computed client-side from `started_at`:

```typescript
function ElapsedTimer({ startedAt }: { startedAt: string }) {
  const [elapsed, setElapsed] = useState("");
  useEffect(() => {
    const interval = setInterval(() => {
      const seconds = Math.floor((Date.now() - new Date(startedAt).getTime()) / 1000);
      setElapsed(formatDuration(seconds));
    }, 1000);
    return () => clearInterval(interval);
  }, [startedAt]);
  return <span className="elapsed">{elapsed}</span>;
}
```

No server-side timer — just `started_at` + client clock.

---

## 4. API changes

### 4.1 New endpoints

| Endpoint | Purpose |
|---|---|
| `POST /sync` | Trigger a sync. Body: `{"scope": "full"}` or `{"scope": "layer", "layer": "candles"}`. Synchronous planning + async execution. See §4.4 for exact semantics. |
| `GET /sync/status` | Current sync state: running sync (if any) + layer-by-layer progress. |
| `GET /sync/runs` | Recent sync runs with layer results. Query params: `limit` (default 20, `ge=1, le=100`). |
| `GET /sync/layers` | All layers with current freshness status. No active sync required. |

### 4.2 Deprecated endpoints (kept for backward compatibility)

| Endpoint | Notes |
|---|---|
| `POST /jobs/{job_name}/run` | Still works. Routing: if `JOB_TO_LAYERS[name]` is non-empty, handler calls `submit_sync(SyncScope.JOB(name, force=True), trigger='manual')` (returns 202 + `sync_run_id`). `force=True` means "run now even if fresh" — matching legacy manual-trigger behaviour. The `JOB` scope ensures composite jobs like `daily_financial_facts` run their full emit set. Persisted as `sync_runs.scope='job'`, `scope_detail=<legacy_job_name>`. If `JOB_TO_LAYERS[name]` is `()` (outside-DAG jobs like `execute_approved_orders`, `monitor_positions`, `daily_tax_reconciliation`, `weekly_coverage_review`, `attribution_summary`, `retry_deferred_recommendations`), the handler calls `JobRuntime.trigger(name)` ([runtime.py:468](app/jobs/runtime.py#L468)) — same path as today — returning 202 with no `sync_run_id`. Unknown job name → 404. |
| `GET /jobs/runs` | Still works — reads from `job_runs` table as before. |
| `GET /system/jobs` | Replaced by `GET /sync/layers` but kept until frontend migration complete. |

### 4.3 Response shapes

**`GET /sync/status`:**

```json
{
  "is_running": true,
  "current_run": {
    "sync_run_id": 42,
    "scope": "full",
    "trigger": "manual",
    "started_at": "2026-04-16T03:00:00Z",
    "layers_planned": 12,
    "layers_done": 4,
    "layers_failed": 0,
    "layers_skipped": 2
  },
  "active_layer": {
    "name": "financial_facts",
    "started_at": "2026-04-16T03:12:00Z",
    "items_total": 200,
    "items_done": 87
  }
}
```

**`GET /sync/layers`:**

```json
{
  "layers": [
    {
      "name": "universe",
      "display_name": "Tradable Universe",
      "tier": 0,
      "is_fresh": true,
      "freshness_detail": "Last synced today at 03:01 UTC",
      "last_success_at": "2026-04-16T03:01:30Z",
      "last_duration_seconds": 12,
      "last_error_category": null,
      "consecutive_failures": 0,
      "dependencies": []
    },
    {
      "name": "candles",
      "display_name": "Daily Price Candles",
      "tier": 1,
      "is_fresh": false,
      "freshness_detail": "3 instruments missing today's candle",
      "last_success_at": "2026-04-15T22:05:00Z",
      "last_duration_seconds": 340,
      "last_error_category": null,
      "consecutive_failures": 0,
      "dependencies": ["universe"]
    }
  ]
}
```

### 4.4 `POST /sync` execution model (synchronous planning, async execution)

`POST /sync` is **not** a fire-and-forget queue. Sequence (matches §2.2 `run_sync` and the concurrency model in §2.5):

1. Handler calls `_start_sync_run(scope, trigger)`:
   - Opens a short-lived connection with `autocommit=True`.
   - Runs `build_execution_plan()`.
   - INSERTs the `sync_runs` row with `status='running'` and all pending `sync_layer_progress` rows inside a single `with conn.transaction():` block.
   - **UniqueViolation on `idx_sync_runs_single_running`** → handler returns **HTTP 409** with `{ "error": "sync_already_running", "sync_run_id": <active_id> }`.
2. Handler submits `_safe_run_and_finalize(sync_run_id, plan)` (the sole crash-guarded entry; never `_run_layers_loop` directly) to the APScheduler `ThreadPoolExecutor` — the same `_manual_executor` already used by `JobRuntime.trigger` at [runtime.py:468](app/jobs/runtime.py#L468). No connections are passed across the thread boundary.
3. Handler returns **HTTP 202** with the freshly-inserted `sync_run_id` and the execution plan. The client can immediately poll `GET /sync/status`.
4. Worker thread runs the §2.2 layer walk, each layer on its own connection.
5. `_safe_run_and_finalize`'s `finally` block calls `_finalize_sync_run` which moves `status` out of `'running'` and releases the partial unique index gate. If `_finalize_sync_run` itself crashes, the boot-time reaper will transition the row.

**Orphaned rows** (worker crashes mid-sync): on app boot, before the scheduler starts, a reaper runs `UPDATE sync_runs SET status='failed', error_category='orchestrator_crash' WHERE status='running' AND started_at < now() - interval '1 hour'`. Mirrors the existing `ops_monitor` running-job reaper pattern.

### 4.5 `JOB_TO_LAYERS` mapping

`JOB_TO_LAYERS: dict[str, tuple[str, ...]]` lives in `app/services/sync_orchestrator.py` (new module) alongside the `LAYERS` registry. Each entry maps a historical `job_name` (from [`_INVOKERS`](app/jobs/runtime.py#L123)) to the ordered tuple of layer names that replaces it.

Mapping (verified against current `_INVOKERS`):

Mapping keys are the exact `_INVOKERS` keys from [runtime.py:123](app/jobs/runtime.py#L123). Verified against source; `retry_deferred_recommendations` is the full name used by both [`JOB_RETRY_DEFERRED`](app/workers/scheduler.py#L200) and `_INVOKERS[JOB_RETRY_DEFERRED]`.

```python
JOB_TO_LAYERS: dict[str, tuple[str, ...]] = {
    # Jobs absorbed into the DAG — orchestrator drives them as layer refreshes:
    "nightly_universe_sync":         ("universe",),
    "daily_cik_refresh":             ("cik_mapping",),
    "daily_candle_refresh":          ("candles",),
    "daily_financial_facts":         ("financial_facts", "financial_normalization"),
    "daily_research_refresh":        ("fundamentals",),
    "daily_news_refresh":            ("news",),
    "daily_thesis_refresh":          ("thesis",),
    "daily_portfolio_sync":          ("portfolio_sync",),
    "morning_candidate_review":      ("scoring", "recommendations"),
    "seed_cost_models":              ("cost_models",),
    "weekly_report":                 ("weekly_reports",),
    "monthly_report":                ("monthly_reports",),
    "fx_rates_refresh":              ("fx_rates",),
    # Jobs that remain outside the orchestrator (still scheduled individually):
    "execute_approved_orders":       (),  # transaction execution, not data
    "weekly_coverage_review":        (),  # periodic governance, no reusable data
    "retry_deferred_recommendations":(),  # recommendation-path retry, separate concern
    "monitor_positions":             (),  # safety monitor, runs frequently
    "attribution_summary":           (),  # periodic computation, no downstream consumer
    # Manual-only (not in SCHEDULED_JOBS today):
    "daily_tax_reconciliation":      (),  # on-demand reconciliation, see scheduler.py:467
}
```

Jobs with `()` are **not** absorbed into the DAG — they remain as-is in `_INVOKERS`; the scheduled ones stay in `SCHEDULED_JOBS`; `daily_tax_reconciliation` stays manual-only. The dashboard shows the "outside the DAG" set in a separate "Background tasks" panel (not a data layer card).

`job_runs.job_name` is preserved exactly as today — the adapter writes the old name to `job_runs` so historical queries and tests continue to match. `sync_layer_progress.layer_name` uses the new layer name. The correlation between the two is by **time window** (the `job_runs` row and `sync_layer_progress` row are written close together under the same adapter), not by a shared `sync_run_id` column — `job_runs` is intentionally unchanged in Phase 1 to minimize migration risk. Adding `job_runs.sync_run_id` is a Phase 5 follow-up if operators need tighter correlation.

---

## 5. Schema changes

### 5.1 New tables

```sql
-- Sync run envelope
CREATE TABLE sync_runs (
    sync_run_id    BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    scope          TEXT NOT NULL CHECK (scope IN ('full', 'layer', 'high_frequency', 'job')),
    scope_detail   TEXT,
    trigger        TEXT NOT NULL CHECK (trigger IN ('manual', 'scheduled', 'catch_up')),
    started_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at    TIMESTAMPTZ,
    status         TEXT NOT NULL DEFAULT 'running'
                   CHECK (status IN ('running', 'complete', 'partial', 'failed')),
    layers_planned INTEGER NOT NULL,
    layers_done    INTEGER NOT NULL DEFAULT 0,
    layers_failed  INTEGER NOT NULL DEFAULT 0,
    layers_skipped INTEGER NOT NULL DEFAULT 0,
    error_category TEXT                   -- sanitized category when status='failed' (§3.4)
);

CREATE INDEX idx_sync_runs_started ON sync_runs(started_at DESC);

-- Authoritative concurrency gate: at most one 'running' sync_runs row
-- across all scopes. The INSERT that sets status='running' is the atomic
-- step that decides whether a new sync can start; UniqueViolation → 409.
-- See §2.5 for rationale vs per-scope advisory locks.
CREATE UNIQUE INDEX idx_sync_runs_single_running
    ON sync_runs((TRUE))
    WHERE status = 'running';

-- Per-layer progress within a sync run
CREATE TABLE sync_layer_progress (
    sync_run_id    BIGINT NOT NULL REFERENCES sync_runs(sync_run_id),
    layer_name     TEXT NOT NULL,
    status         TEXT NOT NULL DEFAULT 'pending'
                   CHECK (status IN ('pending', 'running', 'complete', 'failed', 'skipped', 'partial')),
    started_at     TIMESTAMPTZ,
    finished_at    TIMESTAMPTZ,
    items_total    INTEGER,
    items_done     INTEGER,
    row_count      INTEGER,
    error_category TEXT,   -- sanitized category only; full error in logs (§3.4)
    skip_reason    TEXT,
    PRIMARY KEY (sync_run_id, layer_name)
);
```

### 5.2 Unchanged tables

- `job_runs` — still written to by each layer refresh (backward compatibility). Not removed.
- All existing data tables — untouched. Orchestrator calls existing refresh functions.

---

## 6. Layer registry (Python)

```python
# Freshness predicates receive a connection (they only read, and the
# orchestrator's planning connection is already open). Refresh callables
# receive NO connection — they open their own, per the §2.3 contract.

@dataclass(frozen=True)
class DataLayer:
    name: str
    display_name: str
    tier: int                                    # 0=source, 1=raw, 2=computed, 3=decision
    is_fresh: Callable[[psycopg.Connection[Any]], tuple[bool, str]]  # read-only
    refresh: LayerRefresh                        # see §2.3: receives sync_run_id + progress, no conn
    dependencies: tuple[str, ...]                # layer names
    is_blocking: bool = True                     # if True, failure skips dependents
    cadence: str = "daily"                       # for display only

LAYERS: dict[str, DataLayer] = {
    "universe": DataLayer(
        name="universe",
        display_name="Tradable Universe",
        tier=0,
        is_fresh=universe_is_fresh,
        refresh=refresh_universe,
        dependencies=(),
    ),
    "cik_mapping": DataLayer(
        name="cik_mapping",
        display_name="SEC CIK Mapping",
        tier=0,
        is_fresh=cik_is_fresh,
        refresh=refresh_cik,
        dependencies=("universe",),
    ),
    # ... 15 layers total — enumerated in §1.1 (weekly_reports and monthly_reports split).
}
```

Each `refresh()` function **adapts** (does not merely wrap) the existing job logic per the contract in §2.3:

1. Opens its own connection; no connection is passed in.
2. Calls the underlying job logic, converting prerequisite-missing early-returns into `SKIPPED` outcomes with explanatory `detail`, and provider/DB failures into raised exceptions.
3. Periodically calls `progress(items_done, items_total)` via the `ProgressCallback`.
4. Returns `RefreshResult(outcome, row_count, items_processed, items_total, detail, error_category)` (or a sequence for composite adapters per §2.3.1).

### 6.1 Progress callback

```python
class ProgressCallback(Protocol):
    def __call__(self, items_done: int, items_total: int | None = None) -> None: ...
```

The callback opens a short-lived connection per invocation, updates the `sync_layer_progress` row for `(sync_run_id, layer_name)`, and commits. It does **not** share the layer's transaction — progress writes are durable even if the layer later rolls back. Call frequency: every N items or every 10 seconds, whichever comes first; the implementation uses a small helper that debounces.

---

## 7. Catch-up behavior

### 7.1 First run (empty database)

Orchestrator runs `FULL` scope. Every layer is stale. Runs in dependency order top-down. Progress visible per-layer. Expected duration: 15-30 minutes depending on universe size and API rate limits.

### 7.2 After extended downtime (1-2 weeks)

Same as first run — `FULL` scope, everything stale, full DAG traversal. But most data is idempotent on re-fetch:
- Universe: ON CONFLICT, only writes changes
- Candles: fetches 400-day lookback, fills gaps
- Financial facts: full re-fetch per CIK, ON CONFLICT handles restatements
- Thesis: stale instruments get new theses (Claude calls)

Duration depends on how many theses need regeneration (most expensive layer).

### 7.3 Normal daily operation

Full sync at 03:00 UTC. Most layers fresh from previous day. Only stale layers run. Typical: universe + candles + financial_facts + news + a few stale theses. Duration: 5-10 minutes.

### 7.4 High-frequency loop

Every 5 minutes: portfolio_sync + fx_rates only. These are independent layers with no DAG dependencies. Duration: <30 seconds.

---

## 8. Migration path

The migration is additive-first and removes the old surface only after the new one is running. Phases are sequential; each is a mergeable chunk.

### 8.1 Phase 1: Adapters + registry (behind a flag)

- Add `app/services/sync_orchestrator.py` with `LAYERS`, `LayerOutcome`, `RefreshResult`, `LayerRefresh`, `ExecutionPlan`, `LayerPlan`, `SyncAlreadyRunning`, **public entry points `run_sync` and `submit_sync`** (§2.1), and private helpers `_start_sync_run`, `_run_layers_loop`, `_safe_run_and_finalize`, `_finalize_sync_run`, `_fail_unfinished_layers`, `build_execution_plan`. Also `JOB_TO_LAYERS` (§4.5), `PREREQ_SKIP_MARKER`, `prereq_skip_reason`.
- Add `sync_runs`, `sync_layer_progress` tables + `idx_sync_runs_single_running` partial unique index in migration `sql/033_sync_orchestrator.sql` (next available — `032_financial_data_enrichment_p1.sql` already exists).
- Write refresh adapters for every `_INVOKERS` entry whose `JOB_TO_LAYERS` value is **non-empty** (the 13 in-DAG entries in §4.5). Outside-DAG entries with `()` (`execute_approved_orders`, `weekly_coverage_review`, `retry_deferred_recommendations`, `monitor_positions`, `attribution_summary`, `daily_tax_reconciliation`) are **not** wrapped — they stay on their existing cron triggers / manual-only invocation and are surfaced in the "Background tasks" panel (§3). For each in-DAG adapter:
  - Rewrite every early-return-on-missing-prerequisite path listed in §2.3 (the complete list with file:line citations):
    - [`nightly_universe_sync`](app/workers/scheduler.py#L651) credential early-return → `record_job_skip` + `PREREQ_SKIP`
    - [`daily_candle_refresh`](app/workers/scheduler.py#L749) credential early-return → `record_job_skip` + `PREREQ_SKIP`
    - [`daily_news_refresh`](app/workers/scheduler.py#L1080) missing Anthropic key → `PREREQ_SKIP`
    - [`daily_news_refresh`](app/workers/scheduler.py#L1110) "no provider wired" success-with-zero-rows → `PREREQ_SKIP` with detail
    - [`daily_thesis_refresh`](app/workers/scheduler.py#L1135) missing key → `PREREQ_SKIP`
    - [`daily_thesis_refresh`](app/workers/scheduler.py#L1150) thesis query failure silent return → raise
    - [`daily_portfolio_sync`](app/workers/scheduler.py#L1206) credential early-return → `record_job_skip` + `PREREQ_SKIP`
    - [`morning_candidate_review`](app/workers/scheduler.py#L1266) scoring failure silent return → raise
    - [`morning_candidate_review`](app/workers/scheduler.py#L1290) recommendation failure silent return → raise
    - [`morning_candidate_review`](app/workers/scheduler.py#L1329) `execute_approved_orders()` side-effect call → extract per §2.3.1 preferred option
  - Confirm each adapter raises on internal error-return paths so `_tracked_job` records a failure `job_runs` row.
- **Cross-locking during Phase 1:** Both old cron-fired jobs and the orchestrator can trigger the same underlying refreshes. The existing [`JobLock`](app/jobs/locks.py#L60) is a context manager that wraps `pg_try_advisory_lock` and **raises `JobAlreadyRunning`** on contention — it does not wait. Concrete behaviour:
  - The orchestrator adapter uses `with JobLock(settings.database_url, job_name):` before calling the underlying job body, the same pattern used elsewhere.
  - The adapter catches `JobAlreadyRunning`, writes `record_job_skip(reason=prereq_skip_reason("legacy cron holder active"))`, and returns `PREREQ_SKIP`. `fresh_by_audit` counts this (§1.3 marker rule) so legitimate contention does not mark the layer stale forever.
  - The scheduled cron fire sees `JobAlreadyRunning` from the orchestrator side via the existing `runtime.py` no-op path ([runtime.py:590, 641](app/jobs/runtime.py#L590)) — unchanged behaviour.
  - Phase 4 removes the 12 overlapping cron triggers, ending all contention. (`nightly_universe_sync` is on-demand only today per [scheduler.py:352](app/workers/scheduler.py#L352), so it is not a trigger that needs removal.)
- Feature flag: `ORCHESTRATOR_ENABLED=false` by default. When false, `POST /sync` returns 503 "sync orchestrator disabled." Existing `/system/*` and `/jobs/*` endpoints untouched. This phase merges without removing anything.

### 8.2 Phase 2: Progress callbacks

- Add optional `progress: ProgressCallback | None = None` parameter to long-running layer refreshes: `candles`, `financial_facts`, `thesis` first; others follow.
- Parameter is optional and defaults to None, so scheduled cron fires that don't pass a callback continue unchanged.

### 8.3 Phase 3: Observability UI

- Replace the AdminPage jobs section with the system health dashboard from §3, feature-gated behind the same `ORCHESTRATOR_ENABLED` flag.
- Existing `/system/jobs` UI stays visible as a secondary tab while operators verify the new dashboard.
- `POST /sync` stays 503 until Phase 4 flips the flag.

### 8.4 Phase 4: Cutover (flag flip + cron removal)

This is the only breaking phase and happens in one PR:

- Flip `ORCHESTRATOR_ENABLED=true`.
- Remove the 12 `SCHEDULED_JOBS` cron triggers whose `JOB_TO_LAYERS` value is non-empty **and** which are currently scheduled (they become orchestrator-driven). The five that stay in `SCHEDULED_JOBS` with their own cron triggers: `execute_approved_orders`, `monitor_positions`, `retry_deferred_recommendations`, `weekly_coverage_review`, `attribution_summary`. Note: `nightly_universe_sync` is in `_INVOKERS` and `JOB_TO_LAYERS` but **not** in `SCHEDULED_JOBS` ([scheduler.py:352](app/workers/scheduler.py#L352)); its adapter stays registered so `POST /jobs/nightly_universe_sync/run` continues to work, but there is no cron entry to remove. `daily_tax_reconciliation` is also not in `SCHEDULED_JOBS` ([scheduler.py:467](app/workers/scheduler.py#L467)) and stays manual-only.
- Add two new orchestrator triggers: `FULL @ 03:00 UTC` and `HIGH_FREQUENCY @ */5min`.
- Keep `_INVOKERS` intact so `POST /jobs/{name}/run` continues to work via the adapter (the five outside-DAG jobs still need invokers; the 13 absorbed ones still have invokers so manual triggers via `JOB_TO_LAYERS` continue to work).
- Cron-triggered catch-up on boot becomes `run_sync(FULL, trigger='catch_up')`.

**Test plan for Phase 4 (must be in the Phase 4 PR):**

- Existing tests that assert on `SCHEDULED_JOBS` count/names must be rewritten or deleted. Grep: `tests/test_jobs_runtime.py`, `tests/test_scheduler.py` (if present), and any test that references `SCHEDULED_JOBS`.
- New tests:
  - `test_orchestrator_full_sync_records_sync_run` — happy path, one row in `sync_runs`, one row per layer in `sync_layer_progress`, all `complete`.
  - `test_blocking_failure_skips_dependents` — `candles` refresh raises; `morning_candidate_review` is a composite LayerPlan whose emits are `(scoring, recommendations)` and whose derived direct external `dependencies` are `(thesis, candles)` per the §2.6 derivation rule (direct union-minus-emits, not transitive closure). The loop evaluates `_blocking_dependency_failed` once per LayerPlan: both emitted rows get `status='skipped'` with the same external `skip_reason` citing `candles`. This is the documented semantics — composite adapters skip atomically because they run atomically. `thesis` is NOT affected (its deps are `fundamentals`, `financial_normalization`, `news` — not `candles`). Similarly for `daily_financial_facts` composite: if `cik_mapping` fails, both `financial_facts` and `financial_normalization` skip with the same reason.
  - `test_high_frequency_sync_is_blocked_by_full_sync` — 409 via `idx_sync_runs_single_running`.
  - `test_orphaned_running_row_is_reaped_on_boot` — stale `status='running'` rows become `failed` at startup.
  - `test_legacy_job_endpoint_runs_via_orchestrator` — `POST /jobs/daily_candle_refresh/run` creates a `sync_runs` row with `scope='job'` and `scope_detail='daily_candle_refresh'` (per §2.1 JOB scope persistence).

### 8.5 Phase 5: Deprecation

- Once the new dashboard has been in production for a release, remove the legacy `/system/jobs` UI tab.
- Keep `POST /jobs/{name}/run` indefinitely for scripts and manual triggers — it's cheap and harmless. Mark deprecated in OpenAPI.

---

## 9. Non-goals (explicit scope boundaries)

- **No real-time price streaming.** Covered by [live pricing spec](2026-04-13-live-pricing-architecture-design.md).
- **No TradingView charts.** Covered by live pricing spec.
- **No currency conversion.** Covered by live pricing spec.
- **No new data sources.** Orchestrator manages existing sources with existing fetch logic.
- **No parallel layer execution.** Layers run sequentially in topological order. Parallelism is a future optimization.
- **No job-level configuration UI.** Operator sees layers and health, not cron expressions or job parameters.
- **No alerting/notifications.** Error visibility is in-dashboard only. Email/Slack alerting is a future concern.
- **No historical analytics.** No "average duration trend" or "failure rate over time" charts. Just current state and recent runs.

---

## 10. Relationship to other specs

| Spec | Relationship |
|---|---|
| [Live pricing architecture](2026-04-13-live-pricing-architecture-design.md) | Complementary. Pricing spec handles real-time WebSocket + charts. This spec handles batch data freshness + observability. Together they make the system self-managing. |
| [Financial data enrichment](2026-04-16-financial-data-enrichment-design.md) | `financial_facts` and `financial_normalization` layers wrap the Phase 1 pipeline. Orchestrator calls these as layer refresh functions. |
| [Portfolio and frontend redesign](2026-04-14-portfolio-and-frontend-redesign.md) | Observability dashboard is a new page, not a modification of the portfolio redesign. No conflict. |

---

## 11. Resolved design questions

These were open in the draft; the Codex round-1 review pushed us to commit.

1. **High-frequency `sync_runs` volume.** Every 5 minutes → ~288 rows/day for the HF scope. That's fine; `idx_sync_runs_started` keeps lookups cheap and a monthly retention job can archive rows older than 90 days (out-of-scope for Phase 1). Decision: reuse the same `sync_runs` table; no separate lighter-weight path.
2. **Default `is_blocking`.** `True` for all data-producing layers (see §2.4 table). `False` only for `news`, `portfolio_sync`, `fx_rates`, `weekly_reports`, `monthly_reports` — with explicit rationale per layer. No silent stale-data consumption anywhere in the DAG.
3. **Retry within a sync run.** **Not supported.** Append-only layers (thesis, recommendations) would duplicate rows on retry, and Claude calls are expensive to repeat. A failed layer is simply failed for that run; the next scheduled sync will retry naturally because the freshness predicate still says stale. If a specific layer later needs retry, it will be added as an explicit, per-layer idempotent retry with row-level dedupe — not a generic orchestrator feature.
4. **Progress callback invasiveness.** Adding an optional `progress` kwarg to layer refresh functions is the least invasive option we could find that still meets the UI requirements. It defaults to `None`, old callers are unaffected, and only the layers that want progress bars opt in. Accepted.

## 12. Naming map: old ops_monitor labels → layer names

[ops_monitor.py](app/services/ops_monitor.py) currently uses `prices`, `theses`, `scores` as layer-ish labels in its `DATA_SOURCE_FRESHNESS` tables. Layer names in this spec use `candles`, `thesis`, `scoring`. The migration must keep old labels working for ops_monitor's existing freshness API while new dashboards use new names.

| Old label (ops_monitor) | New layer name | Notes |
|---|---|---|
| `prices` | `candles` | ops_monitor query `SELECT MAX(price_date)::timestamptz FROM price_daily` remains unchanged; layer freshness predicate in §1.3 references both the audit watermark and the same content query. |
| `theses` | `thesis` | Singular preferred for layer (one logical concept); plural retained in ops_monitor for backward compatibility with its public API. |
| `scores` | `scoring` | Same rationale. |

No database column renames. The mapping lives in `sync_orchestrator.py` as `OPS_MONITOR_LABEL_TO_LAYER: dict[str, str]` and is used only when rendering combined views.
