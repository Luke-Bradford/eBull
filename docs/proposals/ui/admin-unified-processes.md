# Admin page — unified-processes redesign

Author: claude (autonomous, drafting)
Date: 2026-05-08
Status: Draft (pre-Codex, pre-implementation)

## Problem

The admin page currently surfaces ETL / job state across **seven
fragmented components**, each rendering similar information in
different ways:

| Component | Surfaces | Lives at |
|---|---|---|
| ProblemsPanel | failing layers + failing jobs + null coverage rows + credential health | top of admin |
| FundDataRow | live cells (analysable, recommendations) + pending placeholders | below problems |
| LayerHealthList | 15 orchestrator-managed layers (per JOB_TO_LAYERS DAG) | "Layer health" collapsible |
| SyncDashboard | orchestrator full-sync history + 15-layer grid + recent runs | "Orchestrator details" collapsible |
| BootstrapPanel | first-install bootstrap (17 stages, init+lanes) | top of admin |
| SeedProgressPanel | SEC ingest seed progress + per-CIK timing | bottom of admin |
| Background-tasks (table) | scheduled jobs outside the orchestrator DAG | "Background tasks" collapsible |

Each one polls separately, has its own poll cadence, its own button
copy ("Run now", "Sync now", "Trigger drain"), its own mid-flight
spinner, its own error row. Operator feedback (2026-05-08): "we have
a number of categories which show processes, in different ways. But
we just need a list of the processes, the ability to kick off a new
iterate get fresh or a full wash, the ability to click in and see
the last 3 days or a weeks worth of history runs — timings, records
processed, errors. In the errors, what errors were logged for each
item found. Don't think we need much more than that."

## Goal

One **unified processes view** on the admin page. Each row is a
single "process" regardless of underlying mechanism — scheduled
job, orchestrator layer, bootstrap stage, SEC ingest sweep all
look the same to the operator. From a row the operator can:

- See current status / last run timing / last record count / last
  error count at a glance.
- Click a row to drill into the **last 3-7 days of run history**
  for that process: per-run timing, records processed, errors,
  per-error item drill-down.
- Trigger an "iterate / fresh refresh" or a "full wash" via two
  clearly-distinct buttons (depending on which the process
  supports).
- See **live activity** when a run starts: records-processed
  ticking up, error count climbing, a visible motion indicator
  ("something whirring"), a stop button.

Out of scope for this spec: AI / ranking / thesis pipelines.
Bootstrap-stage parallelism (already covered by the existing
BootstrapPanel — to be folded into the unified view, not removed).

## Unified data model

Every process surfaced on the admin page is one of four mechanism
types, but all conform to a single ``Process`` envelope:

```python
@dataclass(frozen=True)
class ProcessRow:
    # Stable identifier — matches the backend job/stage/layer name.
    process_id: str
    # Operator-facing display name (e.g. "SEC 13F holdings — quarterly
    # sweep" not "sec_13f_quarterly_sweep").
    display_name: str
    # One of: "scheduled_job", "orchestrator_layer", "bootstrap_stage",
    # "ingest_sweep". Drives which trigger surface to show.
    mechanism: str
    # Current state — one of: idle / running / success / failed / skipped /
    # disabled.
    status: str
    # Last completed run summary (None for never-run processes).
    last_run: ProcessRunSummary | None
    # Active run summary (None when not running).
    active_run: ActiveRunSummary | None
    # Trigger capabilities the process exposes. Operator UI uses this
    # to render the right button set.
    can_trigger_iterate: bool       # incremental / fresh-data refresh
    can_trigger_full_wash: bool     # full re-ingest / re-process
    can_stop: bool                  # cooperative cancel signal supported
    # Cadence metadata.
    cadence_human: str              # "every 5m", "daily 03:00 UTC", "on demand"
    next_scheduled_at: datetime | None

@dataclass(frozen=True)
class ProcessRunSummary:
    run_id: int
    started_at: datetime
    finished_at: datetime
    duration_seconds: float
    rows_processed: int | None
    error_count: int
    status: str  # success / failed / partial / skipped
    last_error: str | None  # display-only excerpt; full list via drill-in

@dataclass(frozen=True)
class ActiveRunSummary:
    run_id: int
    started_at: datetime
    elapsed_seconds: float           # client-side ticks; backend reports started_at
    rows_processed_so_far: int       # mid-flight counter
    error_count_so_far: int          # mid-flight counter
    progress_units_done: int | None  # populated when underlying job exposes it
    progress_units_total: int | None
    last_log_message: str | None     # tail of the underlying job's logs
```

## Single endpoint

```
GET /system/processes               → list[ProcessRow]
GET /system/processes/{id}/history  → list[ProcessRunSummary] (last N days)
GET /system/processes/{id}/history/{run_id}/errors → list[PerItemError]
POST /system/processes/{id}/trigger      body: {"mode": "iterate" | "full_wash"}
POST /system/processes/{id}/stop          (only when can_stop=True)
```

Backend adapters per mechanism translate to this envelope:

| Mechanism | Source | Adapter |
|---|---|---|
| scheduled_job | ``app/workers/scheduler.py::SCHEDULED_JOBS`` + ``job_runs`` table | reads job_runs latest + computes next_scheduled_at from cron |
| orchestrator_layer | ``app/services/sync_orchestrator/layer_state`` + ``layer_runs`` | reads sync_runs filtered to layer |
| bootstrap_stage | ``bootstrap_stages`` rows on the latest ``bootstrap_runs`` | wraps existing bootstrap_state read |
| ingest_sweep | dedicated ingest tables (``institutional_holdings_ingest_log``, ``def14a_ingest_log``, etc.) | aggregates per-stage status |

The orchestrator layer + bootstrap stage are NOT separate processes
in the list — they're each ONE row representing the parent job
("Orchestrator full sync", "First-install bootstrap"), with the
inner stages visible only on drill-in.

## UI shape

### Top-level: process table

One table, sortable columns:

| Process | Status | Last run | Records | Errors | Cadence | Actions |
|---|---|---|---|---|---|---|
| Universe sync (eToro) | ✅ | 7s ago, 1.3s | 12,416 | 0 | daily 00:00 | [Iterate] [Full wash] [History] |
| SEC 13F quarterly sweep | 🔵 running | — (in flight) | 1,247 so far | 23 so far | weekly Sat 02:00 | [Stop] [History] |
| Insider Form 4 ingest | ⚠️ partial | 12m ago, 3m 2s | 4,521 / 5,200 | 21 | hourly :05 | [Iterate] [Retry failed] [History] |
| First-install bootstrap | ✅ | 1h ago, 92m | 17/17 stages | 0 | on demand | [Re-run] [History] |

- **Status pill colours**: idle (slate), running (sky pulsing), success (emerald), failed (red), partial (amber), skipped (slate-light), disabled (slate-disabled).
- **Live row**: when `status='running'`, the row has a subtle animated accent (left border pulsing sky) + the Records cell shows a ticking client-side counter pulled from `active_run.rows_processed_so_far` updated every poll. Mirrors GitHub Actions / Vercel Deploy step UI.
- **Single primary action button** per row, contextual to status:
  - idle → "Iterate" (default) + secondary "Full wash" + tertiary "History"
  - running → "Stop" (if `can_stop`) + tertiary "History"
  - failed → "Retry failed" + secondary "Full wash" + tertiary "History"
  - success → "Iterate" + secondary "Full wash" + tertiary "History"
  - disabled → "Enable" only

### Drill-in: process history view

Click "History" → side-panel slide-in (not a route change — keep
operator's table state). Renders:

- Process name + cadence + current status pill.
- Run history list: last 7 days by default, sortable columns
  (started, duration, rows, errors, status). Each row click
  expands inline.
- Expanded run shows: full timing, full error list (paginated if
  > 50), per-item errors (e.g. for an SEC ingest: which CIKs/
  accessions failed and why).
- Top of panel has "Re-run this run's failed items only" button
  for processes that support it.

### Live activity

Three conventions for "something whirring":

1. **Pulsing left border** on running rows — pure CSS, no data
   needed. Visible motion regardless of progress field.
2. **Client-side elapsed counter** ticks every second in the row,
   computed from `active_run.started_at`. Works without backend
   changes.
3. **Records-processed counter** pulled from `active_run.rows_processed_so_far`
   each poll. Backend processes need to update their own counter
   mid-flight; this is the cooperative-progress work tracked in
   #1005. UI degrades gracefully — shows "—" for processes that
   don't yet expose it.

### Stop button

`POST /system/processes/{id}/stop` writes a cooperative-cancel
signal (a row in a `process_stop_requests` table or a flag on the
underlying run row). Long-running jobs (bootstrap, SEC drain, 13F
sweep) need to check the flag between iterations. Not all jobs
support stop — `can_stop` flag in the envelope hides the button
when not supported.

For v1 implementation, only orchestrator full sync + bootstrap
orchestrator + SEC sweeps need stop support. Scheduled-job
short-runners (heartbeat, monitor_positions) finish in < 30s so
stop is not useful.

## Migration / consolidation

Current admin-page sections in their proposed final state:

| Old | New |
|---|---|
| ProblemsPanel | **Keep** as-is at top — surfaces cross-process problems (credential health, layer-state anomalies, null coverage rows). Independent of the process list. |
| FundDataRow | **Keep** as-is — operator-visible KPIs, not a process. |
| LayerHealthList | **Fold into process list** — 15 orchestrator layers each become a row in the process list, mechanism = `orchestrator_layer`. The collapsible "Layer health" section is removed. |
| SyncDashboard | **Fold into drill-in** for the "Orchestrator full sync" row. The flat 15-layer grid becomes the drill-in's run-history view. |
| BootstrapPanel | **Fold into process list as one row** ("First-install bootstrap") with a custom drill-in showing the 17-stage parallel-lane timeline. |
| SeedProgressPanel | **Replace** — it duplicates info now available via the SEC ingest process rows. |
| Background-tasks table | **Fold into process list** — these are scheduled_jobs already; no transformation needed. |

Result: admin page becomes 3 sections (Problems / Fund-data KPIs /
**Processes**) instead of 8. Process list is sortable + searchable
(operator filters "show me only failed processes" / "show me only
SEC processes").

## Implementation plan (multi-PR)

| PR | Scope | Depends |
|---|---|---|
| PR1 | Backend: `/system/processes` GET endpoint + adapters for scheduled_job + bootstrap_stage. Empty stub for orchestrator_layer + ingest_sweep. | — |
| PR2 | Backend: orchestrator_layer + ingest_sweep adapters. | PR1 |
| PR3 | Backend: `/system/processes/{id}/history` + per-item error endpoint. Schema migration for `process_run_history` (or aggregation across existing tables). | PR1 |
| PR4 | Backend: cooperative-cancel infra. `process_stop_requests` table + check-flag helper used by long-running jobs. Plumb into bootstrap orchestrator + SEC sweeps. | PR1 |
| PR5 | Frontend: `ProcessTable.tsx` replacing the current LayerHealthList + Background-tasks table. Read-only at first; trigger buttons stubbed. | PR2 |
| PR6 | Frontend: trigger buttons (`Iterate` / `Full wash` / `Stop`) wired to `POST /trigger` + `POST /stop`. | PR4, PR5 |
| PR7 | Frontend: `ProcessHistoryDrawer.tsx` slide-in for drill-in views. Per-error pagination. | PR3, PR5 |
| PR8 | Frontend: live-activity polish — pulsing left border, ticking elapsed, mid-flight record counter. Folds in #1005 progress callback work. | PR5 |
| PR9 | Frontend: fold BootstrapPanel + SyncDashboard + SeedProgressPanel into the unified table. Custom drill-ins for bootstrap (parallel-lane timeline) + orchestrator (DAG view). | PR5, PR7 |

## Codex check before commit

Before any of PR1-PR9 lands, the unified data model + endpoint
shape go to Codex for second opinion. Specifically: is the
`Process` envelope the right abstraction across the four
mechanism types, or is one of them being squeezed into a shape
that doesn't fit (e.g. orchestrator layers' parallel
semantics inside a row-per-process model).

## Open scope calls

1. **Should bootstrap stages each be processes too**, or only the
   parent? Spec proposes parent-only; the 17-stage internal
   parallelism is shown in the drill-in. Argument for stages-as-
   processes: each is independently retriable. Argument against:
   they're tightly coupled — retrying one without the others
   makes no sense. **Default: parent-only.**

2. **What's "iterate" vs "full wash"** for each mechanism? Needs
   per-process documentation:
   - scheduled_job iterate = trigger now (same as today's `Run now`).
   - scheduled_job full_wash = ?? not all jobs have a meaningful
     full-wash mode. For ingest jobs, full_wash = re-fetch the
     full window; for non-ingest jobs, full_wash button is hidden
     (`can_trigger_full_wash=False`).
   - orchestrator_layer iterate = single layer refresh.
   - orchestrator_layer full_wash = same layer with "ignore
     freshness" forcing a fetch.
   - bootstrap_stage iterate = retry-failed (current behaviour).
   - bootstrap_stage full_wash = re-run all (current "Re-run all" button).
   - ingest_sweep iterate = next batch.
   - ingest_sweep full_wash = re-process from scratch (operator's
     "wash" word).

3. **History retention**. Spec says "last 3-7 days". Backend already
   has `job_runs` retention via existing housekeeping. Process
   history view filters by `started_at >= now() - interval`. No
   new retention needed.

4. **Stop semantics**. Cooperative-cancel flag set in DB; long-run
   jobs check the flag every N iterations. Mid-stage interrupts
   (e.g. mid-CIK-fetch on the SEC drain) leave manifest in a
   partial state — that's already handled by the manifest worker's
   tombstone-on-retry logic. v1 stop is "stop after current
   per-CIK iteration completes".

## Acceptance

- [ ] Admin page collapses from 8 sections to 3 (Problems / KPIs / Processes).
- [ ] Process table shows every backend job/layer/sweep/stage parent in one place, sortable/searchable.
- [ ] Drill-in slide-out shows last 7d run history per process with per-item errors expandable.
- [ ] Trigger buttons follow the iterate / full-wash / stop convention consistently across mechanisms.
- [ ] Running rows have visible motion regardless of whether the underlying process exposes per-unit progress.
- [ ] Operator can stop a long-running process from the row.
- [ ] No regression in current admin-page coverage of failure surfaces (ProblemsPanel still works, all layer states still visible).
