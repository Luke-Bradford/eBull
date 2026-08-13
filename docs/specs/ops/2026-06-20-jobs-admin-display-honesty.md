# Jobs admin display honesty (#1689)

**Status:** spec (Codex ckpt-1 applied) • **Issue:** #1689 • **Follow-up:** #1690 (statement_timeout) • **Date:** 2026-06-20

## Premise falsification (working-order 4b)

Handoff: "the admin page paints transient/retrying/restart-reaped jobs the same red as genuinely-dead ones." **Corrected against the code + full live population (all 38 rendered jobs, not a sample):**

- The **Processes Control Hub** (`/system/processes` → `ProcessRow`) ALREADY renders honestly via `compute_verdict()` (#1512, `health_verdict.py`): reaped-failure+`next_retry_at` → `self_healing`; role-split #1530; wedge-never-masked. **It is not a false-red source.**
- Only the **Background Jobs table** (`/system/jobs` → `AdminPage::JobsTable`; `system.py:330 _build_jobs_overview`) is naive: it renders raw `last_status` through `STATUS_TONE` (`AdminPage.tsx:51`, `failure`→`text-red-600`). It is a redundant, dumber view of the **same** registry — both `_build_jobs_overview` and `scheduled_adapter.list_rows()` iterate `SCHEDULED_JOBS` (38 jobs).
- **No job is rendered-red right now** — all 38 `SCHEDULED_JOBS` are `success`/`skipped`/`running`. The false-red is **latent**: it manifests when a steady-state job fails transiently or is restart-reaped (the restart churn of the prior insider-staleness session). The fix is a latent-bug fix, not a visible-today fix.
- **`sec_rebuild` (and other manual-trigger jobs) are NOT in `SCHEDULED_JOBS`** — they render on neither admin table (they are invoked via the manual queue + tracked in `job_runs` only). The handoff's "sec_rebuild stale red" is a `job_runs` artifact, not a rendered figure. `stale_manual` therefore targets the **rendered** non-steady jobs: the `bootstrap`/`backfill` role entries.

→ Correct fix = **converge the naive surface onto the existing verdict model** (single source of truth), NOT a parallel status enum.

## Settled decisions applied
- **#719 process topology** (`settled-decisions.md:372`): jobs process owns scheduler/reaper/dispatcher; API serves HTTP only. No runtime/scheduling added to the API. ✅ all changes are read-path (endpoint projection) or in `app.services`.
- **#1512 single computed verdict** (`health_verdict.py`): the one choke point. Extended, never forked.
- **#1530 role split**: `ScheduledJob.role ∈ {steady_state, bootstrap, backfill}` (`scheduler.py:295`), surfaced on `ProcessRow.role`.
- **#1508 C3** (self_healing rendered calm-green): **deliberately superseded** here — operator now wants retrying visible as amber (3-state semaphore). C3 tests/comments updated, not ignored.

## Prevention-log applied
- L1796 "multi-surface states share one copy source" → reuse `compute_verdict` + `VERDICT_VISUAL`; no second model.
- L1915-1917 "terminal job-status write outside `record_job_finish` MUST run `_retry_plan`" → no new terminal-write site; the `status='running'` guard only protects the existing path.
- L1691 "prefer pure policy over real DBs" → verdict logic stays pure-testable; one `db` test for the SQL guard.

## Source rule
Operator-UI verdict — no SEC reg governs it. Governing invariant = the in-repo settled model `compute_verdict` (#1512) + `REMEDIES.self_heal` (`layer_types.py:60`, single transient-vs-permanent source, already consumed by `_is_transient`). **Full-population check:** verified against all 38 rendered jobs (0 red, all ok) — not a sample.

## Decision 1 — converge `/system/jobs` on the verdict (reconcile)

Operator's explicit ask: derived status *on the jobs-overview endpoint*. Reuse, don't duplicate.

1. **Extract** `verdict_for_row(row: ProcessRow, *, now) -> tuple[HealthVerdict, bool, str]` (new fn in `health_verdict.py`) from `processes.py::_convert_row:432-444` — the `retry_in_flight = row.next_retry_at is not None` / `retry_at_display` derivation + the `compute_verdict(...)` call + the new `manual_aged_exhausted` derivation (Decision 3). `_convert_row` calls it; `/system/jobs` calls it. One source.
2. **`/system/jobs`** (`system.py` `get_jobs` / `_build_jobs_overview`): wrap the build in `snapshot_read(conn)` **explicitly in the handler path** and call `scheduled_adapter.list_rows(conn)` once inside it (its documented caller-MUST). Map each `ProcessRow` → enriched `JobOverviewResponse`. New fields: `health_verdict`, `self_healing`, `verdict_reason`, `role`, `attempt`, `next_retry_at`. `next_run_time` now sourced from `row.next_fire_at` (same basis as the verdict inputs; fall back to `compute_next_run(job.cadence, now)` only when `next_fire_at is None`) — removes the millisecond/cadence-boundary drift of computing it separately. The per-job `check_job_health` loop is replaced (also kills its N+1).
   - **Failure mode unchanged:** `get_jobs` keeps its existing 503-on-build-failure (`system.py:673`). Partial-degradation (`partial=True`) is the Hub's contract, not this legacy endpoint's — not expanded here.
   - **Perf:** `list_rows()` runs per-job probes under a repeatable-read snapshot — materially heavier than the old `check_job_health` loop, and the page polls `/system/jobs` + `/system/processes` together (≈2× probe load). Read-only MVCC, no row-lock concern; acceptable at 38 jobs. The redundant fetch is fully eliminated when `JobsTable` is retired (recommended follow-up note below).
3. **`ProcessRow`** (`processes/__init__.py:170`) gains `attempt: int | None = None`, sourced in `_build_row` from `terminal_row.get("attempt")` (the latest terminal `job_runs` row already read). DTO `ProcessRowResponse` + wire type mirror it.
4. **FE `JobsTable`** (`AdminPage.tsx:354`): render the verdict pill via shared `VERDICT_VISUAL` (drop `STATUS_TONE`); role-split `role !== "steady_state"` into a collapsed "Manual & backfill" section (mirror Hub); a retrying row shows `attempt N · next HH:MM` from `attempt`/`next_retry_at`.
5. **Type** `JobOverviewResponse` (`types.ts:114`) gains the six fields.

> **Non-blocking review note:** with both surfaces now verdict-identical, the naive `JobsTable` is redundant with the Hub. Recommend retiring it in a follow-up; kept here because the operator asked for the endpoint-level fix and deletion is an operator-visible scope call.

## Decision 2 — `self_healing` → amber (supersedes #1508 C3)
`VERDICT_VISUAL.self_healing` currently renders green `CALM_TONE` (`processStatus.ts`); `compute_verdict`'s own docstring already calls it amber (`health_verdict.py:33,88`). Change it to an amber tone. 3-state semaphore: green=ok/working, amber=self_healing/retrying, red=attention. **Update the C3 assertions** (`processStatus.test.ts:45` "only attention pins" stays valid for *sort*; any color assertion expecting calm-green for self_healing is updated). Shared `VERDICT_VISUAL` ⇒ applies to both surfaces (intended).

## Decision 3 — `stale_manual` aging
New 5th verdict `stale_manual` (muted/slate). Gate (precomputed in `verdict_for_row`, passed to `compute_verdict` as one bool `manual_aged_exhausted`):
`role in ("bootstrap", "backfill")` AND `status == "failed"` AND `next_retry_at is None` (exhausted/permanent — no retry in flight) AND `finished_at < now - STALE_MANUAL_WINDOW` (24h).
- `bootstrap` **intentionally included** — an aged, exhausted one-time install/backfill failure is history, not a steady-state alarm.
- A *recent* (<24h) bootstrap/backfill failure still reads `attention` so the operator sees their triggered job failed.
- A `steady_state` failure is never muted (role gate) — stays red.
- Manual-trigger jobs (`sec_rebuild`) are out of scope (not rendered on these surfaces).

**Precedence (Codex BLOCKING):** the `stale_manual` branch sits in the status-only section **after** the kill-switch check, **after** the actionable-stale block (so `queue_stuck`/`mid_flight_stuck`/`watermark_gap` still outrank → a genuinely-wedged bootstrap job stays red), **after** the retry/kick suppression, and **before** the `status == "failed" → attention` branch:
```
if status == "failed" and manual_aged_exhausted:
    return ("stale_manual", False, "aged one-shot failure")   # ASCII, role-neutral
if status == "failed":
    return ("attention", False, "last run failed")
```
**Full contract surface (Codex HIGH):** backend `HealthVerdict` Literal (`processes/__init__.py`), FE `HealthVerdict` (`types.ts:1382`), `VERDICT_VISUAL` (muted tone), `VERDICT_SORT_PRIORITY` (`processStatus.ts:204` — sort like `current`, i.e. not pinned, collapsible), the `processStatus.test.ts` priority test, and `compute_verdict` fixtures.

## Decision 4 — hung-job
**Pick timeout-bounded adapters (root cause); reject the blind periodic reaper** (it can't reclaim a wedged thread → would false-amber a genuinely-stuck job). Active mitigation (per-job `statement_timeout` via a shared `connect_job()`) needs an ~82-site raw-`psycopg.connect` migration → **#1690** (its own reviewed PR).

Ship here (safe, in-scope):
- **`record_job_finish` status guard** (`ops_monitor.py:452`): `UPDATE job_runs SET ... WHERE run_id = %(run_id)s AND status = 'running'`. Check rowcount; `rowcount == 0` → gate-safe no-op log ("finalize raced — row already terminal, skipping"), **no secondary write**. `_retry_plan` runs before the UPDATE (read-only; harmless on a raced row). First-writer-wins; mirrors `_finalize_sync_run`.
- **running_too_long: no new code.** A wedged non-heartbeating `running` row is **already** caught — `stale_detection.py:166-172` keys `mid_flight_stuck` on `COALESCE(last_progress_at, started_at)` vs `get_threshold(process_id)`, so an old `running` row with no progress already reads `attention` (red). Add a **test** asserting it; do not add a redundant reason.

## Tests
- Pure-logic `compute_verdict`: `stale_manual` (aged bootstrap & backfill → stale_manual; recent → attention; steady-state-exhausted → stays attention); wedge (`queue_stuck`) still outranks an aged-exhausted bootstrap → attention; `manual_aged_exhausted` role-gated.
- `verdict_for_row`: same `ProcessRow` → identical verdict from `_convert_row` and the `/system/jobs` mapper (parity).
- `mid_flight_stuck` via `started_at` fallback (no heartbeat) → attention (regression assertion).
- `record_job_finish` guard: one `db` test — finalize a row already flipped to `failure` no-ops (rowcount 0, status unchanged); normal finalize of a `running` row succeeds.
- FE `processStatus`: amber for self_healing, red for attention, muted for stale_manual; `VERDICT_SORT_PRIORITY` includes stale_manual.

## DoD verification (dev)
1. `/system/jobs` 200 (authed) carries `health_verdict`/`self_healing`/`role`/`attempt`/`next_retry_at`; live `/admin` `JobsTable` renders pills, no red while all ok.
2. Synthetic state on a `SCHEDULED_JOB` via crafted `job_runs` row: failure+`next_retry_at` future → amber "retrying attempt N · next HH:MM"; non-self-heal (`auth_expired`, `next_retry_at` NULL) → red attention; aged bootstrap/backfill failure → muted stale_manual in the collapsed section.
3. Record commit SHA + figures observed. Clauses 8-12 (ownership/parser/schema-ingest) N/A — read-path display change only.
