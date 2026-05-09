# Runbook — cancel and resume

When and how to stop a long-running ingest, walk away, and pick it up
later from the last successful watermark.

This runbook covers the `/admin` Processes table — the unified surface
introduced by umbrella #1064. Cancel coverage by mechanism:

- **Bootstrap** — full cooperative-cancel support. The orchestrator
  polls `is_stop_requested()` at every stage boundary
  (`app/services/bootstrap_orchestrator.py:488`).
- **Sync orchestrator (`orchestrator_full_sync`)** — full cooperative
  cancel support, with checkpoints between layers and at finalize
  (`app/services/sync_orchestrator/executor.py:724`, `:1074`).
- **Other scheduled jobs** — `request_stop()` writes the
  `process_stop_requests` row plus `job_runs.cancel_requested_at`, but
  per-job worker checkpoints are NOT mandatory. Cooperative cancel is
  best-effort for these jobs: cancel signal is durable, but the worker
  only observes if its loop polls `is_stop_requested`. If a particular
  job lacks a checkpoint, restart the jobs process to free the worker.
- **Ingest sweeps** — cannot be cancelled directly. The cancel API
  returns `409 cancel_not_supported`; cancel the underlying scheduled
  job instead (see `app/api/processes.py:1440`).

## Mode choice — Iterate, Full-wash, Cancel

The Processes table exposes three primary actions per row. Pick the
right one based on what you want the next run to do.

| Action | What the next run reads | When to use |
| --- | --- | --- |
| **Iterate** | Resume from the last successful watermark. Idempotent ON CONFLICT writes re-fetch only what wasn't committed. | Default. After a cancel, after a transient failure, or when the source has fresh data and the schedule hasn't re-fired yet. |
| **Full-wash** | Resets the watermark for this process before running. Re-fetches everything from scratch within the configured scope. | After a parser-version bump (see `runbook-after-parser-change.md`), after a schema migration that changed how rows are shaped, or when a bug is suspected to have left rows partially written. |
| **Cancel** | Stops the in-flight run; the next *Iterate* picks up from the last successful watermark. | Long ingest is hogging the SEC token bucket and a higher-priority job needs to run. |

Trigger / cancel conflict copy (the `409 reason` text rendered in the
modal when an action is rejected) lives at
`frontend/src/components/admin/processStatus.ts::REASON_TOOLTIP`. That
file is the single source of truth — when in doubt about what a
specific reason means, grep for the key in that map. Examples:
`kill_switch_active`, `bootstrap_already_running`,
`full_wash_already_pending`, `shared_source_active_run`.

## What watermarks mean

Each process pins its resume cursor on one of six `cursor_kind` values
(see `app/services/processes/watermarks.py`):

- **`filed_at`** — SEC-filing-driven scheduled jobs (Form 4, 13D/G,
  DEF 14A). Cursor = "we've seen everything filed up to this
  timestamp". Iterate fetches anything filed after.
- **`accession`** — SEC-manifest-driven jobs (per-accession ingest).
  Cursor = "the last accession number we drained". Iterate processes
  the next pending accession in `sec_filing_manifest`.
- **`instrument_offset`** — per-instrument fan-out jobs (candle
  refresh, fundamentals). Cursor = "we've processed instrument_id ≤
  N". Iterate continues from N+1.
- **`stage_index`** — bootstrap stages. Cursor = "stage K out of 17 is
  the last successful one". Iterate resumes at K+1.
- **`epoch`** — universe sync, monthly snapshots. Cursor = "epoch
  2026-04". Iterate produces the next epoch.
- **`atom_etag`** — Atom-feed-polled sources (SEC submissions). Cursor
  = "the ETag the source returned last time". Iterate sends
  If-None-Match; 304 means nothing new.

The Iterate button tooltip surfaces a `human` field summarising the
cursor in plain English ("at filed_at = 2026-05-08T14:32:11Z" / "at
stage 13/17"). The resolver builds that string per-mechanism — it is
NOT free-form operator prose.

## How cancel works (cooperative state machine)

The Cancel button writes a row into `process_stop_requests` (see
`app/services/process_stop.py:100-163`) targeting the in-flight run.
The cancel state machine has three transitions; cooperative cancel
walks through all three:

1. **Request:** `request_stop()` inserts a row with `requested_at`,
   `target_run_kind`, `target_run_id`, `mode='cooperative'`. The API
   handler runs `SELECT … FOR UPDATE` on the active run row before the
   insert, so the target id is pinned to the truly-running run.
2. **Observe:** The worker polls `is_stop_requested()` at well-defined
   checkpoints (between bootstrap stages, between SEC manifest
   accessions, between sync orchestrator layers). On hit, it calls
   `mark_observed()` which sets `observed_at = now()` and lets the
   in-flight item finish cleanly.
3. **Complete:** After the in-flight item commits, the caller
   transitions the run row to `status='cancelled'`
   (`bootstrap_runs.status` via `mark_run_cancelled`; `sync_runs.status`
   via the orchestrator's finalize path; `job_runs.status` for
   scheduled jobs that have a checkpoint loop), then calls
   `mark_completed()` on the stop request. `mark_completed` sets
   `process_stop_requests.completed_at = now()` and frees the
   partial-unique active-stop slot; the run-row transition is
   caller-side and happens just before.

Watermarks advance during normal in-flight commits — the cooperative
checkpoint never advances past a row the worker hasn't actually
written. The next Iterate reads the watermark and re-fetches anything
queued but not committed, with ON CONFLICT idempotency on
`sec_filing_manifest` / `data_freshness_index` keeping writes safe.

### How to confirm a cancel landed

Open the row's drill-in at `/admin/processes/{process_id}`:

- **Logs tab** — search for `"cancel observed at checkpoint"`
  (bootstrap; emitted by `app/services/bootstrap_orchestrator.py:495`)
  or `"cancel signal observed"` (sync orchestrator; emitted by
  `app/services/sync_orchestrator/executor.py:756`). Either confirms
  the worker saw the signal.
- **Timeline tab** — for bootstrap, the parallel-lane stage tree shows
  every stage that was running or pending at cancel time tinted red
  (`status='error'`). `mark_run_cancelled` sweeps `running` and
  `pending` stages to the error status; the Timeline does NOT
  distinguish "cancelled" from "errored" stages today. To confirm the
  red stage is the cancellation point and not a real error, check the
  Logs tab for `"cancel observed at checkpoint"` immediately preceding
  the red stages.

Or query the DB directly:

```sql
SELECT id, requested_at, observed_at, completed_at, mode
  FROM process_stop_requests
 WHERE target_run_kind = 'job_run'   -- or bootstrap_run / sync_run
   AND target_run_id = <run_id>
 ORDER BY requested_at DESC LIMIT 1;
```

`completed_at IS NOT NULL` on its own means the stop request is
**closed** — but that includes boot-recovery sweeps that close
abandoned stop rows (see `runbook-stuck-process-triage.md`). To
confirm a clean cooperative cancel:

- `observed_at IS NOT NULL` — worker saw the signal.
- The run-row status is `cancelled` on the matching mechanism table:

```sql
-- pick one based on target_run_kind. Each table has a different PK column:
SELECT status, cancelled_at        FROM job_runs        WHERE run_id      = <run_id>;
SELECT status, cancel_requested_at FROM bootstrap_runs  WHERE id          = <run_id>;
SELECT status, cancel_requested_at FROM sync_runs       WHERE sync_run_id = <run_id>;
```

Both signals together = clean cooperative cancel.

## Resume after cancel

Click *Iterate* on the same row. The next run reads the watermark and
picks up from where the cancelled run left off. There is no separate
"resume" verb — Iterate IS resume.

When the cancelled run was the bootstrap orchestrator, Iterate
specifically resumes the failed/cancelled stages only (see
`app/services/bootstrap_orchestrator.py`). Stages that already
committed their work do not re-run; the orchestrator's stage-level
ON CONFLICT idempotency means a re-run would be a no-op anyway.

## Cancel-mode choice — cooperative vs terminate

The cancel modal has two modes, picked **at cancel time** before any
stop request is in flight:

- **Cooperative** (default). Writes `mode='cooperative'`. Worker
  observes at next checkpoint; in-flight item finishes cleanly. Caller
  transitions the run row to `cancelled`. This is the right choice 99%
  of the time.
- **Terminate (mark for cleanup)** — exposed via the modal's "More"
  disclosure. Writes `mode='terminate'` (see `app/services/process_stop.py`
  `StopMode` Literal). Mode honoured by mechanism:
  - **Scheduled jobs and `orchestrator_full_sync`**: pass through the
    operator's chosen mode (see `app/api/processes.py::_cancel_orchestrator_full_sync`
    and the scheduled-job branch around line 1471).
  - **Bootstrap**: the cancel helper currently hardcodes
    `mode="cooperative"` regardless of modal choice (see
    `app/services/bootstrap_state.py:742`). Selecting Terminate on the
    bootstrap row therefore lands a cooperative stop row; the operator
    still escalates by restarting the jobs process when the worker is
    genuinely wedged.

  Terminate does NOT kill the worker mid-write. It records the
  operator's intent on the stop row; the operator separately restarts
  the jobs process to free the wedged worker. The age-gated
  boot-recovery sweeps (`reap_orphaned_stop_requests` 6h for
  never-observed; `reap_observed_unfinished_stop_requests` 24h for
  observed-unfinished) close the stop row only once the threshold is
  past — the runbook for that wait is `runbook-stuck-process-triage.md`.
  Use Terminate ONLY when the worker is genuinely wedged (not making
  heartbeat progress past `2× per-process threshold`).

Once a cooperative stop request is pending, **a second cancel cannot
"upgrade" it to terminate** — the partial-unique index
`process_stop_requests_active_unq` (sql/135) rejects the second insert
with `409 stop_already_pending`. If the cooperative cancel is genuinely
not landing (worker has been past `2× threshold` without observing),
the path is: restart the jobs process → boot recovery sweeps the
stranded stop row → re-issue cancel if needed.

## Why cooperative-by-default

Hard-kill mid-write leaves partial rows on disk; the next run reads
a watermark that incorrectly suggests "we got that far" and skips
re-fetching. Cooperative cancel + watermark-aware resume guarantees
the next iterate reads a clean cursor and re-fetches anything not
committed. See `docs/settled-decisions.md` `## Cancel UX` and
`docs/review-prevention-log.md` `### Cancel UX must be
cooperative-with-checkpoints, never faked hard-kill` for the full
rationale.
