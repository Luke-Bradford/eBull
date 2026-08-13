# Spec: /admin ProblemsPanel — count problems from the processes catalogue, not the legacy jobs list (#1959)

## Problem
`/admin` shows two disagreeing problem counts on one page:
- Top **ProblemsPanel** banner: "1 problem(s) need attention" — its failing-job sub-source is `/system/jobs` (`fetchJobsOverview`, 43 rows), filtered `health_verdict === "attention"`.
- **Processes control hub** (`ProcessesTable` → `StaleBanner`): "2 need attention: fundamentals_sync, nport_sweep" — reads `/system/processes` (50 rows), counting `health_verdict === "attention"` over steady-state rows.

`nport_sweep` (and the other `sec_*_sweep` ingest sweeps) are `mechanism = "ingest_sweep"` processes that the `/system/jobs` registry does not represent, so the top banner structurally cannot see their failures and under-reports.

## Source rule / falsification (verified live @ 8357f1c9, service-token curl)
- `/system/jobs` (43) is a **strict subset** of `/system/processes` (50): `set(jobs) - set(processes) == {}`. Delta = `bootstrap` + 6 `ingest_sweep` rows (`nport_sweep`, `sec_13f_sweep`, `sec_8k_sweep`, `sec_def14a_sweep`, `sec_form3_sweep`, `sec_form4_sweep`).
- Processes attention set = `{fundamentals_sync (scheduled_job), nport_sweep (ingest_sweep)}`, both `role=steady_state`. Jobs attention set = `{fundamentals_sync}`.
- Control-hub count is computed by `ProcessesTable` → `steadyStateRows` (`role === "steady_state" && mechanism !== "bootstrap"`) → `StaleBanner` filters `health_verdict === "attention"`.
- v2 layers `action_needed`/`secret_missing` are keyed by `root_layer` at a different granularity; `orchestrator_full_sync` already appears in BOTH the jobs list and processes today, so switching the source introduces **no new** overlap-with-layers double-count class.
- `health_verdict === "attention"` already excludes `stale_manual`/`paused`/`working`/`current` (#1689/#1831), so aged bootstrap/backfill one-shots do not false-red.

## Change (FE only, display reconciliation — no health-verdict logic change)
1. New shared helper `frontend/src/lib/processHealth.ts`:
   - `isSteadyStateProcess(r)` = `r.role === "steady_state" && r.mechanism !== "bootstrap"` (the exact `!isBootstrapOrBackfill` predicate from `ProcessesTable`).
   - `steadyStateAttentionRows(rows)` = `rows.filter(isSteadyStateProcess).filter(r => r.health_verdict === "attention")`.
   - `ProcessesTable` imports `isSteadyStateProcess` (replacing its local `isBootstrapOrBackfill`) so the two surfaces share ONE predicate and cannot drift again (this is the bug's own lesson).
2. `ProblemsPanel`:
   - Replace props `jobs: JobsListResponse | null` / `jobsError` with `processes: ProcessListResponse | null` / `processesError`.
   - Cache key `jobs` → `processes`; pending-source label `"jobs"` → `"processes"`.
   - `failingProcesses = steadyStateAttentionRows(cache.processes?.rows ?? [])`.
   - Row render uses `ProcessRowResponse` fields: label `display_name`, reason `verdict_reason`, failed-at `last_run?.finished_at`, drill link `/admin/processes/{process_id}` (the modern ProcessDetailPage; `nport_sweep` has no `/admin/jobs` route). "Clears when the next run of {display_name} succeeds."
3. `AdminPage`: pass `processes={processes.data}` / `processesError={processes.error !== null}` (both already fetched via `useProcesses()`), drop the `jobs`/`jobsError` props to the panel. The separate "Background tasks" jobs table below is unchanged (still uses `jobs.data`).

## Deliberate scope change (Codex ckpt-1)
The old jobs source counted ANY `/system/jobs` row with `health_verdict === "attention"` — including `role="bootstrap"`/`role="backfill"` scheduled jobs. The new steady-state predicate intentionally excludes those, so a freshly-failed bootstrap/backfill one-shot no longer raises the TOP red banner. This is correct, not a regression: the control-hub (#1530 C7) already folds bootstrap/backfill into a separate collapsed section and excludes them from its "N need attention" count. Matching that scope is the entire objective (the two counts must agree; the control-hub scope is canonical). Bootstrap/backfill attention still surfaces in the Processes "Bootstrap & backfill" section. Live attention set today = `{fundamentals_sync, nport_sweep}` (both steady-state) → no live row is dropped. Codified by a test asserting a `role="backfill"` attention row is NOT counted by the top banner.

## Non-goals
- No backend change. No health-verdict semantics change. The layer/secret/coverage/credential sub-sources of ProblemsPanel are untouched.
- No dedupe of existing layer↔process overlap. If a v2 `action_needed` root layer and a process attention row describe the same failure they still count twice, exactly as today — the swap only replaces the jobs sub-source with the superset processes source; it does not change overlap behaviour.

## Tests
- `ProblemsPanel.test.tsx`: swap the failing-job fixtures to `ProcessRowResponse` rows; assert an `ingest_sweep` attention row (nport_sweep) is counted + rendered with a `/admin/processes/nport_sweep` link; assert a `role !== "steady_state"` / `mechanism === "bootstrap"` attention row is NOT counted; assert `health_verdict !== "attention"` rows excluded.
- `processHealth.test.ts`: unit-table the predicate + selector.
- `AdminPage.test.tsx`: update prop wiring.

## Verification (dev)
- Load `/admin`, confirm top banner reads "2 problem(s) need attention" and lists fundamentals_sync + nport_sweep, matching the control hub below. Spot-check against `/system/processes`.
