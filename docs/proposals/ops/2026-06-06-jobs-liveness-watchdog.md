# GAP-D — jobs liveness watchdog (#1500)

Umbrella: #1472. Follow-up to PR-visibility (#1501, `EVENT_JOB_MAX_INSTANCES` listener).

## Problem
The max-instances listener makes *one known* APScheduler suppression visible (a `job_runs` 'skipped' row per suppressed fire). The broader silent-failure class remains unguarded: a job that **stops firing for ANY reason** (wedge, crash-loop, mis-schedule) writes no row at all. Ops-monitor mandate: "silent failure = failure." Need a periodic watchdog comparing **expected fires (per-job cadence)** vs **actual `job_runs` rows**, alerting when a job has had zero fires for ≥ K cadence cycles.

Plus a related visibility nuance (Codex PR-visibility ckpt-1b MEDIUM): `check_job_health()` is latest-row-based, so new 'skipped' rows MASK an older still-`running` row that evidences a wedge. The health surface must expose the aged `running` row separately.

## Scope
1. A periodic **per-job** stall check (expected-vs-actual fires) with its own alert semantics, separate from the listener.
2. An "active runs" surface exposing the oldest still-`running` row per job + its age.

**NOT in scope: scheduler-wide / process-down liveness.** A watchdog that is itself an APScheduler job cannot detect the scheduler being wedged or the process being dead — it would be dead too (Codex ckpt-1). That failure mode is already covered by the `job_runtime_heartbeat` table + `supervisor.py` (#719), which upserts a `main` + per-subsystem heartbeat the supervisor monitors out-of-band. This watchdog assumes the scheduler loop is alive and detects *individual jobs* that have gone silent. The spec references — does not duplicate — the heartbeat path.

## Design

### Expected-fire computation (pure, testable)
Reuse `compute_next_run(cadence, now)` (`app/workers/scheduler.py`). For a job with cadence `C` over a lookback window `[now − W, now]`, the **expected fire count** = number of `compute_next_run` occurrences landing in the window (iterate from `now − W`). The **cadence period** `P` is derived per kind (every_n_minutes → interval; hourly → 1h; daily → 24h; yearly → ~365d).

### Actual-fire signal — "did this job fire in W?" (Codex ckpt-1 fixes)
A job counts as **alive in W** if EITHER:
- ≥1 `job_runs` row with `started_at >= now() − W`, **any status** — crucially `skipped` counts. Bootstrap/universal-gate blocks on scheduled fires DO write a `job_runs.skipped` row (`record_job_skip` in `runtime.py` scheduled path + listener). So a bootstrap-gated job still records a fire every cadence cycle → it is NOT false-stalled, and **no separate bootstrap exclusion is needed** (this removes the original, partly-wrong bootstrap-incomplete exclusion). OR
- an active `status='running'` row exists (any age). A long/stuck run that self-skips later fires on the advisory `JobLock` (`JobAlreadyRunning`) without writing a row would otherwise read as "stopped firing" — but a live running row means the job is *stuck*, a different failure mode surfaced by the aged-running query + the #1474 reaper, not "scheduler not firing it." Counting it as alive prevents that false positive.

### Self-tracked jobs are excluded (Codex ckpt-1)
`job_runs` is **not** universal for `SCHEDULED_JOBS`. `orchestrator_full_sync` + `orchestrator_high_frequency_sync` opt out of the prelude and write `sync_runs`, not `job_runs` (scheduler.py:624). v1 **excludes** these from the job_runs-based stall check — their freshness is already covered by `check_layer_staleness` / `sync_runs`. (A future v2 could add a `sync_runs`-based check; out of scope.)

### Alert rule
Window `W = K × P` (default `K = 3`), so "≥ K missed cadence cycles" is exact by construction — expected fires in `W` ≈ `K`, and `actual == 0` over `W` means all `K` were missed. (No `W_max` cap: an earlier draft capped `W`, which would have let a monthly job trip after ~1 missed cycle — Codex ckpt-1. Plain `W = K × P` is correct; for yearly cadence this yields a ~3-year window, which simply means a yearly job is flagged only after genuinely missing ~3 years, the right behaviour.)

A job is **stalled** when: eligible (below) AND `actual == 0` over `W` AND it has **≥1 lifetime `job_runs` row** (proves it *used* to fire — the never-run guard, using real data instead of a non-existent registry `first_seen`; a brand-new job with zero lifetime rows is simply not evaluated until its first fire). Stalled jobs are logged at WARNING + surfaced on the health endpoint. No alerts table in v1 — matches the existing posture (log + `job_runs` + endpoint; no `ops_alert` table exists).

### Eligibility (replaces the old exclusion list)
- **Self-tracked sync_runs jobs** — excluded (above).
- **Low-frequency cadence** — handled by `W = K × P` itself, no special-casing: a yearly job's window is ~3 years, so it is only ever flagged on a genuine multi-year silence. (Chosen over a `W_max` cap or per-kind K: the plain formula is both simpler and correct.)
- **Never-run** — handled by the ≥1-lifetime-row requirement, not a separate exclusion. Tradeoff: a job broken from day-1 is not flagged until it fires once; acceptable for v1 (target = regression of a previously-working job). Noted as a known gap.
- No `catch_up_on_boot`-based exclusion (Codex: many real scheduled jobs set it false; it is not a manual-only signal). Every `SCHEDULED_JOBS` entry has a cadence, so all are evaluable except the self-tracked exclusion.

### Aged-running surface
`SELECT DISTINCT ON (job_name) job_name, started_at FROM job_runs WHERE status='running' ORDER BY job_name, started_at ASC` — oldest running row per job; compute age on the DB clock (mirror `check_job_health` tz handling). Expose age so the operator sees the stuck run even when newer 'skipped' rows top the latest-row health (the Codex PR-visibility ckpt-1b nuance). Complements (does not replace) the #1474 orphaned-`running` reaper.

### Where it runs
New scheduled job `jobs_liveness_watchdog` in the jobs process (`app/workers/scheduler.py` + body), cadence `every_n_minutes(interval=15)` (cheap: 2 aggregate queries + pure compute). **Non-exempt** (`exempt_from_universal_bootstrap_gate=False`, the default) with no per-job prerequisite: like any non-exempt job it is gated by the universal bootstrap gate, so it pauses cleanly during bootstrap (writing `skipped` rows) and resumes once complete — simpler than carrying the exempt-allowlist burden, and a stalled job during bootstrap is not the steady-state signal this targets. Listed in the bootstrap-gating drift test's `NON_GATED_SCHEDULED` (the "no per-job prereq" set, #1504 pattern). Added to `_INVOKERS` for manual trigger. **Caveat (in-scope-but-honest):** because it is itself a scheduled job, it cannot report its own stall — that is the heartbeat path's job (see Scope).

### Surface (API)
Extend `app/api/system.py`:
- `/system/status` (or `/system/jobs`): add `stalled_jobs: list[...]` + `active_runs: [{job_name, started_at, age_seconds}]`.
- Add the aged-running + stalled data to the response models in `app/api/types`-mirrored Pydantic classes. Frontend (#1480 area, ProcessesTable/AdminPage) can later render it — out of scope here unless trivial.

## Out of scope (v1)
- A persistent alert/dedup table (revisit if log noise becomes a problem).
- Frontend rendering of stalled/active-runs (separate FE ticket if wanted).
- Predicting *partial* under-firing (fired 2× when 3 expected) — v1 only flags total silence (`actual == 0`), which is the dangerous class.

## Test plan
- Pure expected-fire counter: per cadence kind, window boundaries (exact fire on boundary, zero-in-window, K-cycle math). Mutation-proved per the test-quality content-isolation rule.
- Eligibility: skipped rows count as fires (bootstrap-gated job NOT false-stalled — no bootstrap exclusion); self-tracked sync_runs jobs excluded; active running row counts as alive (stuck-run not double-flagged); yearly job not stalled within a normal window (W = K×P ≈ 3y); never-run (zero lifetime rows) not evaluated vs stopped-firing (≥1 lifetime row).
- Aged-running query: oldest-per-job, age computed on DB clock (mirror `check_job_health` tz handling).
- Integration (`@pytest.mark.integration`): seed `job_runs` rows, assert stalled detection + active-runs surface against a per-worker DB; assert entity-scoped (don't assert global cardinality — shared-DB rule).

## Open decisions for sign-off
1. `K` (missed-cycle threshold): default **3**. Lower = more sensitive/noisier, higher = slower to alert.
2. Alert sink: **log + endpoint only** (recommended v1) vs add a persisted alert/dedup table now.
3. Watchdog cadence: **15 min** (recommended) vs other.
4. Surface placement: extend `/system/status` vs `/system/jobs` vs a new `/system/job-liveness` endpoint (+ whether to render in the Admin Processes UI now or as a follow-up FE ticket).

(Resolved during ckpt-1: no `W_max` cap — `W = K × P`; self-tracked orchestrator jobs excluded; skipped rows count as fires so no bootstrap exclusion; never-run guarded by ≥1-lifetime-row; scheduler-wide-down out of scope, owned by the heartbeat path.)
