# job_runs telemetry: orchestrator last-run + orphaned-running reaper

**Status:** proposed (2026-06-05). Codex ckpt-1 reviewed (both premises TRUE; column + status corrections folded). Issue: #1474 (re-scoped per its 2026-06-04 triage comment).
**Goal:** fix the two NARROW, display-layer defects that remain in #1474 after the root-cause connection-wedge (the "13 stale" cohort) was addressed by #1472/#1475 + #1479 + #1478 + a clean jobs restart.

## Re-scope (per the issue's 2026-06-04 triage comment)

The original "13 stale / SCHEDULE MISSED" cohort was **TRUE, not cosmetic** — the SEC discovery layer was genuinely frozen by the #1472 connection-herd wedging `wrapped()` before `record_job_start` (→ APScheduler `max_instances=1` never decrements → silent skip). That root cause is fixed elsewhere (PGCONNECT_TIMEOUT #1475; boot liveness + bounded outbound #1479; sec_manifest lane #1478). After a clean restart the cohort fires again.

This issue's **remaining valid scope** is two display-layer items only:

1. **Part 1 — `orchestrator_high_frequency_sync` "last run" from `sync_runs`.** It is the ONE genuinely telemetry-frozen job: it records completions in `sync_runs` (`scope='high_frequency'`, `complete` every 5m) but `app/services/processes/scheduled_adapter.py` computes `terminal_row` / `expected_fire_at` / stale-reasons from **`job_runs`** (`_latest_terminal`, adapter:270). Its `job_runs` row is frozen at whenever it last wrote one → permanent false `schedule_missed`.
2. **Part 2 — orphaned `job_runs` `running` reaper.** A `running` `job_runs` row from a prior boot (e.g. `sec_daily_index_reconcile` run_id 67, a double-dispatch orphan) survives a jobs-process restart → "NO PROGRESS NNNNm". `reap_orphaned_syncs` reaps `sync_runs`, not `job_runs`; boot-recovery never resets stale `job_runs` `running` rows.

## Verified facts (code-grounded)

- `job_runs` has **no boot_id / owner column** (sql/020,137,141…) — status ∈ `running|success|failure|skipped|cancelled` (137). So a job_runs reaper keys on age / reap-all, NOT boot-id. **At boot Step 4** (`__main__.py`, before boot-drain Step 7 + `_catch_up` dispatch anything), no job in the new process has started → EVERY `running` `job_runs` row is necessarily a prior-boot orphan → `reap_all=True` is safe there (exact mirror of `reap_orphaned_syncs(reap_all=True)` at the same step).
- `sync_runs` has a `scope` column (CHECK `full|layer|high_frequency|job|behind`, sql/041) + `status` (`running|complete|partial|cancelled`, 139) + `started_at`/`finished_at`. `orchestrator_high_frequency_sync` → `sync_runs WHERE scope='high_frequency'`.
- The adapter builds `terminal_row` via `_latest_terminal` (job_runs) at :270; `expected_fire_at = compute_next_run(job.cadence, terminal_row['started_at'])` at :633; stale via `compute_stale_reasons` at :649.

## Workstreams

**PR1 — orphaned job_runs reaper (Part 2; do first — clean, mirrors existing pattern):**
- New `reap_orphaned_job_runs(conn, *, timeout, reap_all=False)` (mirror `app/services/sync_orchestrator/reaper.py::reap_orphaned_syncs`): `UPDATE job_runs SET status='failure', finished_at=now(), error_msg='orphaned: reaped at boot (no live thread)' WHERE status='running' AND (reap_all OR started_at < now() - timeout)`.
  - **Codex ckpt-1 corrections (verified):** the column is **`error_msg`** (per `record_job_finish`, `ops_monitor.py:339`), NOT `error_message`. Use status **`failure`** (NOT `cancelled`): the sync reaper marks crash-orphans `failed`, and the adapter treats `cancelled` as a quiet operator-ish terminal that would also require a `cancelled_at` — `failure` is the consistent, unambiguous reaped-orphan status. `status` CHECK allows `running|success|failure|skipped|cancelled` (sql/137).
- Wire into `app/jobs/__main__.py` boot Step 4, immediately after `reap_orphaned_syncs(reap_all=True)`, with `reap_all=True`. **Boot-safety verified (Codex):** boot-drain (:1085), `runtime.start()` (:1098), and `_catch_up()` (inside `start()`, after scheduler start) all run AFTER Step 4 — so no new-process job has written a `running` row yet → `reap_all` cannot touch a live row. Best-effort (log + swallow), like the sync reaper.
- Tests: a `running` job_runs row → terminal after reap; a fresh `running` row started AFTER boot is NOT reaped by the age path (only reap_all at boot touches it — assert the age-guarded variant leaves a recent row).

**PR2 — orchestrator_high_frequency_sync last-run from sync_runs (Part 1):**
- In `scheduled_adapter.py`, for the orchestrator-driven job(s), resolve `terminal_row` (last-run + `started_at` for `expected_fire_at`) from `sync_runs WHERE scope='high_frequency'` instead of `job_runs`. Smallest surface: a per-job override map `{job_name: sync_runs_scope}` (start with just `orchestrator_high_frequency_sync → 'high_frequency'`) consulted in `_latest_terminal` / the terminal-row fetch; fall back to job_runs for everything else.
- **Status normalization (Codex ckpt-1):** `sync_runs.status` is `running|complete|partial|failed|cancelled` — NOT the job_runs `success|failure|skipped`. Map deliberately: `complete→success`, `failed→failure`, `partial→failure` (a partial high-frequency sync left some layer behind — surface as actionable, not green), `cancelled→cancelled`. Also map `sync_run_id→run_id`, `{}` for `rows_skipped_by_reason`, `0` for `rows_errored` (sync_runs has no per-error-class breakdown). Do this in a dedicated sync_runs→`ProcessRunSummary` builder, not by abusing the job_runs `_build_last_run`.
- Map `sync_runs` columns to the `ProcessRunSummary` shape — verify field names align or adapt.
- Keep it ONE job for now (the triage named only `orchestrator_high_frequency_sync`); do NOT speculatively re-home the Layer-1/2/3 crons (they write job_runs again post-restart). Note as a follow-up if more surface as frozen.
- **Intentional scope (Codex ckpt-1):** PR2 fixes ONLY the process-row last-run + staleness (the `schedule_missed` chip). The History tab (`list_runs` / `list_run_errors`) still reads `job_runs` — out of scope for this issue; note it in the PR so reviewers don't flag the asymmetry as a miss.
- Tests: with a `sync_runs` high_frequency `complete` row and a stale `job_runs` row, the process renders `last_run` from sync_runs and is NOT `schedule_missed`.

## Discipline (CLAUDE.md)
- Not committee-grade (display/telemetry, no regulated-source / data-correctness surface). Spec → Codex ckpt-1 → implement → Codex ckpt-2 → review.
- Settled-decisions: none pinned on job_runs-as-sole-truth; the adapter reading sync_runs for orchestrator jobs is consistent with the #260/#1155 orchestrator migration.
- ETL DoD 8-12: N/A (telemetry only).
- Sequencing: lands AFTER #1478 (done) — #1478 already drained the SEC-producer chunk of the false-stale cohort, so this targets only the genuinely-frozen `orchestrator_high_frequency_sync` + the orphan reaper.
- Prevention-log: candidate lesson — "a process that records to a NON-default run-table (sync_runs) is invisible to job_runs-based stale-detection; the telemetry layer must know each job's run-table of record."
