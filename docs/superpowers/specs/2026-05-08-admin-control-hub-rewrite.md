# Admin control hub — full rewrite

Author: claude (autonomous, supersedes 2026-05-08-admin-page-unified-processes-redesign + 2026-05-08-bootstrap-services-ui-redesign)
Date: 2026-05-09
Status: Operator-amendment round 1 (post-PR1 merge; addresses operator pushback on stale-detection + Iterate/Full-wash IA + per-process progress reporting)

## Operator-amendment round 1 (2026-05-09, post-PR1 merge)

After PR1 (#1066) shipped the schema + cancel infra, operator review of the IA surfaced four substantive corrections:

### A1. Stale-detection reframed (was: "running too long")

The original spec's stale-detection (`elapsed > 2 * expected_p95`) was the wrong shape for v1. The operator concern is NOT "running job is taking too long" — it's any of:

1. **Schedule miss.** Cron should have fired by now, didn't.
2. **Watermark gap.** `data_freshness_index.expected_next_at < now()` and we haven't pulled the new data yet. Source has fresh; we're behind.
3. **Queue stuck.** `pending_job_requests.status='dispatched'` and age > 30 min with no terminal status.
4. **Mid-flight stuck.** Job is `running` but no rows written to sink in N minutes (default 5; per-job override based on natural row-write cadence — see `sec-edgar.md`, `data-engineer.md` skills).

All four are computable from data we already store (or will after the small schema additions in A3). No heartbeat infra required — `last_progress_at` IS the heartbeat (set by `record_processed()`).

`ProcessRow` gains a `stale_reasons: list[Literal["schedule_missed", "watermark_gap", "queue_stuck", "mid_flight_stuck"]]` field. Adapters compute and surface; FE renders subtle row chips ("schedule missed" / "source has fresh data" / "queue stuck" / "no progress 7m"). Multiple reasons can fire simultaneously.

### A2. Iterate is the primary verb; Full-wash demoted to drill-in Advanced

Operator's data-engineering model: **one script per data source, parameterised**. `sec_form4_ingester.py` knows watermarks + filters. Iterate = "fetch since last watermark". Re-fetch with custom params (e.g. `since=2 years ago`) is just calling the same script differently — not a separate script, not a primary affordance.

Implications:
- Drop `Full-wash` button from primary row affordances.
- Drop `Full-wash` button from `ProcessRow.can_full_wash` envelope field.
- Drill-in route gains an "Advanced" tab with a custom-params trigger surface ("Re-fetch with params: since=…, filter=…"). Used for: data corruption replay, schema migration backfill, debugging.
- The full-wash advisory-lock + `pending_job_requests.mode='full_wash'` fence machinery (PR1 sql/138) remains — it's still the correctness mechanism for ANY watermark-resetting trigger, primary or advanced. Just less surfaced to the operator.
- `ProcessRow.full_wash_label` field deleted; `ProcessRow.iterate_label` (per-mechanism, e.g. "Run now" / "Retry failed") kept.

### A3. Per-process progress reporting + four-case stale model

Operator wants a live `Processed: X` ticker per running row, with optional `Rows: N · Processed: X (Y%)` when target is known, plus `⚠ N` and `✗ N` chips when warnings/errors exist.

Schema additions (folded into PR2 schema or as sql/140 — TBD by PR2 author):

```sql
ALTER TABLE job_runs
    ADD COLUMN processed_count   INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN target_count      INTEGER,                  -- NULL = unbounded
    ADD COLUMN last_progress_at  TIMESTAMPTZ,              -- heartbeat
    ADD COLUMN warnings_count    INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN warning_classes   JSONB NOT NULL DEFAULT '{}'::jsonb;

-- Mirror onto bootstrap_stages for parity.
ALTER TABLE bootstrap_stages
    ADD COLUMN processed_count   INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN target_count      INTEGER,
    ADD COLUMN last_progress_at  TIMESTAMPTZ,
    ADD COLUMN warnings_count    INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN warning_classes   JSONB NOT NULL DEFAULT '{}'::jsonb;

-- sync_runs already has layers_done; add the rest for parity.
ALTER TABLE sync_runs
    ADD COLUMN processed_count   INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN target_count      INTEGER,
    ADD COLUMN last_progress_at  TIMESTAMPTZ,
    ADD COLUMN warnings_count    INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN warning_classes   JSONB NOT NULL DEFAULT '{}'::jsonb;
```

Producer API extension to `JobTelemetryAggregator` (already shipped in PR1):

```python
agg.set_target(1547)                    # optional; bounded jobs only
agg.record_processed(n=1)               # increments; updates last_progress_at
agg.record_warning(error_class="...",   # parallel to existing record_error
                   message="...", subject="...")
agg.maybe_flush(conn, run_id=...)       # writes to job_runs every 5s elapsed
```

**Flush cadence (producer → DB):** 5s default elapsed-time-based. `maybe_flush` is called inside the producer's natural per-item loop; checks `now() - self._last_flush_at > 5s` and writes if so. No background thread, no timer. Per-job override: override the threshold via `JobTelemetryAggregator(flush_interval_seconds=N)` — e.g. SEC bulk-download writes one tick per archive completion (~1 per minute), so configure 60s.

**Bounded vs unbounded jobs:**
- Bounded (e.g. `bootstrap_filings_history_seed` over a CIK list): `agg.set_target(len(cik_list))` once at start; `record_processed()` per CIK. FE shows `Rows: 1547 · Processed: 312 (20%)`.
- Unbounded (e.g. SEC drain "anything since T?"): no `set_target`; `record_processed()` per accession. FE shows `Processed: 312` only — no percentage.

**Mid-flight stuck (the 4th stale case):** computed as `status='running' AND last_progress_at < now() - STALE_PROGRESS_THRESHOLD`. Default 5 min; per-job override on a per-ingester basis (constant in the ingester module sourced from skill notes).

### A4. Polling-cadence tiers (live-ish without streaming)

| Surface | Cadence | Justification |
|---|---|---|
| Producer → DB flush | 5s elapsed-time-based, per-job override | 1 DB write per 5s per running job; negligible |
| FE poll of `/system/processes` (admin index) | 5s when any row=running, 30s otherwise | one query for the whole table; cheap |
| FE poll of `/system/processes/{id}` (drill-in detail) | **1.5s when status=running**, 30s otherwise | a few queries/sec on one operator; fine |

Drill-in feels near-live (1.5s perceived) without committing to SSE/WebSocket infra. **SSE is the v2 upgrade path** (Postgres NOTIFY on flush → SSE push to subscribed FE sockets) — filed as a follow-up ticket only if 1.5s feels laggy after PR5 ships. ~200 LoC.

### A5. Bootstrap row hides on `complete`

Spec was already correct on this implicitly (bootstrap status semantics in §Status enum). Tightening to invariant: **the bootstrap row is rendered in the Processes table only when `bootstrap_state.status != 'complete'`**. Once first-install completes successfully, the row disappears from the index. Operator-driven re-bootstrap (universe churn, schema migration) is a v2 affordance via `/admin/danger-zone` route with typed-name confirm, NOT a primary button. v1 path: if operator genuinely needs to re-bootstrap, restart the jobs process + manually flip `bootstrap_state.status='pending'` via SQL (documented in PR10 runbook).

### A6. CIK gap rolled into PR10 (was: out-of-scope)

The TSLA / GOOGL CIK→canonical-name programmatic bridge from operator quote §3.7 was originally out-of-scope. Operator-confirmed 2026-05-09: roll into PR10. Filed separately under #1064.

PR10 scope adds:
- Audit current TSLA / GOOGL gaps (which instruments lack CIK mapping; what fuzzy-match said).
- Build CIK→canonical-name bridge via `company_tickers.json` (programmatic, not fuzzy guessing).
- Drop fuzzy-match fallback to bound-0.92-only, last-resort.
- Operator runbook entry: "how to diagnose missing CIK".

## Codex round 6 amendments (recorded for audit)

## Codex round 4 amendments (recorded for audit)

Round 4 found 2 BLOCKING + 2 WARNING + 1 NIT after the round-3 patches landed. Fixes:

- **R4-B1 watermark FOR UPDATE replaced with pg_advisory_xact_lock.** Round-3 design assumed the watermark row exists, but on first install several mechanisms have NO row yet (`data_freshness_index`, `sec_filing_manifest`, etc.). A zero-row `SELECT … FOR UPDATE` takes no lock; the race re-opened. Fix: both full-wash AND scheduled/iterate workers acquire `pg_advisory_xact_lock(hashtext(process_id)::bigint)` at the very start of their prelude transaction. Lock is guaranteed (no row required), tx-scoped (released at COMMIT), and serialises both paths' preludes. Worker's `job_runs`/`sync_runs` INSERT happens inside the locked tx; full-wash's fence-row INSERT happens inside the locked tx. After COMMIT, the durable fence row (full-wash) or active run row (worker) is what each path queries; the advisory lock no longer needs to be held.
- **R4-B2 sql/139 widens sync_runs.status to include `cancelled`.** Verified at sql/033:21 — current set is `('running','complete','partial','failed')`. Worker must transition `orchestrator_full_sync`'s sync_run to `cancelled`; without the widen, the UPDATE fails. Migration also adds `cancel_requested_at TIMESTAMPTZ`.
- **R4-W1 boot-recovery prose says `failed`; SQL says `rejected`.** Prose corrected.
- **R4-W2 R2 amendment text re DELETE / 'failed' explicitly struck through** to match round-1 W7/W8 style.
- **R4-N1 deduplicated round-headers** (cosmetic — single section per round).

## Codex round 3 amendments (recorded for audit)

Round 3 found 4 BLOCKING + 2 WARNING + 2 NIT. Fixes:

- **R3-B1 sql/138 partial index → UNIQUE.** Concurrent full-wash POSTs could both pass the fence check and both INSERT. Index is now `CREATE UNIQUE INDEX … WHERE mode='full_wash' AND status IN ('pending','claimed','dispatched')`. Handler catches `UniqueViolation` and returns 409 `{"reason":"full_wash_already_pending"}`. Code path matches existing partial-unique pattern at `bootstrap_runs_one_running_idx`.
- **R3-B2 pending_job_requests boot-recovery uses `rejected`, not `failed`.** Verified at sql/084:23 — status set is `('pending','claimed','dispatched','completed','rejected')`. Boot-recovery sweep transitions stuck dispatched fence rows to `rejected` with `error_msg="dispatched row stuck >6h, freed by boot-recovery"`.
- ~~**R3-B3 fence + watermark ordering for scheduled/iterate path.**~~ **SUPERSEDED by R4-B1** — `SELECT … FOR UPDATE` on watermark rows fails when no row exists yet (first install). Replaced with `pg_advisory_xact_lock(hashtext(process_id)::bigint)` which is guaranteed lock-able and serialises both paths' preludes identically.
- **R3-B4 `target_run_kind` widened to include `sync_run`.** orchestrator_full_sync writes `sync_runs`, not `job_runs`; cancel resolution against that process needs `sync_run` targeting. sql/135 CHECK widened; cancel handler adds sync_runs lookup branch.
- **R3-W1 worker fence-row finalisation pinned to status transition.** Workers transition the fence row to `status='completed'` (success path) or `status='rejected'` (failure path). NEVER DELETE — preserves audit + matches existing `pending_job_requests` lifecycle.
- **R3-W2 PR3 stub list trimmed.** sync_layer purged; PR3 stubs `ingest_sweep` adapter only.
- **R3-N1 deduplicated round-1 amendments header.** (cosmetic — single section.)
- **R3-N2 sql/137b references struck through.** Round-1 W7/W8 amendment text + open question 6 marked SUPERSEDED.

## Codex round 2 amendments (recorded for audit)

Round 2 found 4 BLOCKING + 6 WARNING + 1 NIT after the round-1 patches landed. Fixes:

- **R2-B1 full-wash persistent fence:** advisory-xact-lock releases at COMMIT, leaving a window before the queued worker starts. Replaced with a **persistent fence via `pending_job_requests`**: the full-wash handler INSERTs a queue row with `mode='full_wash'`; scheduled runs + Iterate gate on `WHERE pending_job_requests.process_id = ? AND mode='full_wash' AND status IN ('pending','claimed','dispatched')` (skip if exists). Worker ~~DELETEs~~ **transitions** the row to `status='completed'` (success) or `'rejected'` (failure) on completion (Codex round 3 R3-W1). Boot-recovery sweeps stuck dispatched rows >6h old → ~~`failed`~~ **`rejected`** (Codex round 3 R3-B2) to free the gate. Schema changes folded into sql/138 below.
- **R2-B2 `job_runs.cancel_requested_at`:** added to sql/137 ALTER alongside `cancelled_at`.
- **R2-B3 `bootstrap_runs.last_error`:** column doesn't exist. Boot-recovery writes the cancellation reason into `bootstrap_runs.notes` (column exists in sql/129) instead.
- **R2-B4 sql/137b decision pinned:** v1 ships index NON-concurrently in the same transactional sql/137. Online deployment is unblocked because table is at our-scale-small; PR1 measures the lock duration on dev DB and documents the maintenance-window need only if duration >250ms. Removed the dual-file split.
- **R2-W1 last-writer-wins text removed:** stale Failure-mode invariants paragraph rewritten to point at the persistent-fence model.
- **R2-W2 cancel-lost recovery mutation:** explicit semantics: boot-recovery scans `process_stop_requests WHERE completed_at IS NULL AND requested_at < now() - 6h`. For each: SET `completed_at = now()`, `observed_at = NULL` (sentinel: "abandoned, never observed"). Inserts a `bootstrap_runs.notes` line OR `job_runs.error_msg` line: `"stop request abandoned by jobs restart"`. Audit-logged. Frees the partial-unique slot for future cancels.
- **R2-W3 `ProcessRunSummary.status` adds `skipped`:** literal type widened.
- **R2-W4 sync_layer purged everywhere:** scope, envelope, PR3 stub, acceptance criteria all updated.
- **R2-W5 trigger preconditions queue lifecycle:** widened to `status IN ('pending','claimed','dispatched')`.
- **R2-W6 REPEATABLE READ via existing helper:** `app/db/snapshot.py::snapshot_read()` is the existing pattern; spec references it explicitly so PR3 doesn't reinvent.
- **R2-N1 goal alignment:** goal text updated — bootstrap one row, layers drill-in only.

## Codex round 1 amendments (recorded for audit)

This spec was rewritten after Codex flagged 8 BLOCKING + 9 WARNING + 3 NIT findings. Each amendment is in-line below; this note inventories the changes:

- **B1 / sql/137:** widens existing `job_runs_status_check` to allow `cancelled` (matches sql/020 pattern).
- **B2 / B3 / W6 process_stop_requests:** schema rewritten — `target_run_kind` + `target_run_id` are NOT NULL and pinned at insert; partial unique on `(target_run_kind, target_run_id) WHERE completed_at IS NULL`. Cancel handler runs atomic SELECT-FOR-UPDATE on the active-run row before INSERT.
- **B4 terminate honesty:** terminate does NOT transition the run to a terminal state in v1; it marks for boot-recovery sweep only. New triggers 409-rejected until cooperative-cancel completes or operator restarts the jobs process.
- **B5 full-wash fence (round-1 advisory lock; replaced in round 2 — see R2-B1):** persistent fence via `pending_job_requests.mode='full_wash'` queue row.
- **B6 bootstrap watermark:** explicit acknowledgement that `_run_one_stage` is NOT transactional with the invoker; resume relies on **idempotent replay** (ON CONFLICT) plus boot-recovery, not transactional commit.
- **B7 / W2 auto-hide rule:** error display is latest-TERMINAL-run only; older window errors live on History tab. `pending_retry` requires evidence the failed scope will be reattempted (kill_switch off + freshness window covers + job not paused).
- **B8 read isolation:** `/system/processes` opens REPEATABLE READ for cross-adapter snapshot consistency.
- **W1 cancel observation points:** dispatcher checks before Phase A first batch + between batches + before Phase B fan-out.
- **W3 trigger preconditions matrix:** explicit per-mechanism prereq table; 409 with structured reason on failure.
- **W4 / W5 sync_layer adapter:** deferred to v2. v1 surfaces `orchestrator_full_sync` as one scheduled_job row; layers shown only on its drill-in. Independently triggerable layers are surfaced via their underlying scheduled_job, not as `sync_layer` rows.
- ~~**W7 / W8 migration locks:** sql/137 splits the index create into a follow-up `CREATE INDEX CONCURRENTLY` step~~ **SUPERSEDED by R2-B4** — sql/137 now ships as one transactional file with a non-CONCURRENT index; table size makes lock duration sub-second.
- **W9 rows_skipped naming:** field renamed to `rows_skipped_by_reason` (JSONB dict) to disambiguate from existing scalar.
- **N1 rows_errored consistency:** Scope section updated.
- **N2 layer count:** current registry has 10 LAYERS (not 15); spec corrected.
- **N3 operator_id type:** `operators.operator_id` is UUID (verified at sql/016:26); all FKs widened to UUID.

## Problem

The admin page has been a pain point from day one. Multiple half-landings (BootstrapPanel, SeedProgressPanel, LayerHealthList, FundDataRow, SyncDashboard, Background-tasks table, ProblemsPanel) each surface fragments of the same data with different visual languages, different polling cadences, different error shapes, different button verbs ("Run now" / "Sync now" / "Trigger drain" / "Retry failed").

Operator feedback (verbatim, 2026-05-07/08):

1. *"There is still no real progress indicators yet there is a progress field. If there is nothing that can go in there, why are we showing it."*
2. *"The error section is a waste of space. When there is an error you can't see anything till you click it at which point it messes up the page layout to display a single line."*
3. *"Can we make the gui for this page nicer, simpler, not just for bootstrap but for all services its jumbled up mess."*
4. *"There's no cancel button on the page either, who is coming up with these designs?"*
5. *"Restarting jobs but the jobs are still running."*
6. *"Are you expecting to see every little bit for specific jobs listed to break the page and make it look janky?"*
7. *"There must be a programmatic rule for those gaps."* (re TSLA / GOOGL CIK→canonical mapping)
8. *"An iterate of a fresh check should be able to pick up from where it left off or we should be able to run a new check to pick up missing data… watermarking would be important for each step."*
9. *"Errors should be clearly visible and not hidden but if a run on a failed process is running we should hide the visible page errors because each run should be regetting anything that was a prior issue."*
10. *"Optimised, slick, not heavy, careful about how we spend processing and download resources, self healing."*

The data layer is settled (PR #1063 closeout): SEC ingest, ownership, candles, and freshness state all work. **The remaining problem is the operator surface and the cancel/resume contract.**

## Goal

A single **control hub** at `/admin` that:

1. Surfaces bootstrap (one parent row), scheduled jobs (each a row), and ingest sweeps (each a row) in **one unified Processes table** — same columns, same drill-in shape, same verbs. Bootstrap stages live on the bootstrap row's drill-in. Sync-orchestrator layers live on the `orchestrator_full_sync` scheduled_job row's drill-in. (Layers are NOT individually surfaced as rows in v1 — see "Sync-orchestrator surface" below.)
2. Shows **failures floated to top** by default; status-at-a-glance with no scanning.
3. Hides the noise: detail behind drill-in routes, not stacked vertically.
4. Supports **cooperative cancel + watermark resume** — operator can stop a long ingest, walk away, click *Iterate* later, work resumes from the last successful watermark with no double-fetch.
5. Treats errors as **auto-clearing on retry-in-flight** (Sidekiq pattern): visible when you need to act, hidden while a retry is genuinely re-fetching the failed items.
6. **Self-heals** — failed items roll into the next scheduled run via existing `data_freshness_index` + `sec_filing_manifest` retry windows; the operator gets a *will-retry-at* indicator instead of red.
7. Bootstrap is **one row** in the table, with a custom drill-in showing the parallel-lane timeline. No separate panel.
8. Process is **rate-respecting** — `Iterate` and `Cancel` and `Run now` all share the same SEC token bucket, no double-spend.

## Scope

### In

- Replacement of: BootstrapPanel, SeedProgressPanel, LayerHealthList, SyncDashboard collapsible, Background-tasks table.
- New: ProcessesTable + ProcessDetail route + ProcessHistoryDrawer + per-item ErrorList.
- Schema: `process_stop_requests` (cooperative cancel signal — `target_run_kind` + `target_run_id` pinned at insert, partial-unique on active), `bootstrap_runs.cancel_requested_at`, `job_runs.rows_skipped_by_reason` (JSONB) + `rows_errored` (INT) + `error_classes` (JSONB) + `cancelled_at` (TIMESTAMPTZ) + `cancel_requested_at` (TIMESTAMPTZ), widened `job_runs_status_check` to allow `cancelled`, `pending_job_requests` schema extension for full-wash fence (`process_id` TEXT + `mode` TEXT), per-process watermark surfacing view.
- Backend `/system/processes/*` envelope + **three adapters** (bootstrap, scheduled_job, ingest_sweep). `sync_layer` mechanism is deferred to v2.
- Cooperative-cancel signal infra plumbed into bootstrap orchestrator, sync orchestrator, SEC drain, 13F sweep, NPORT ingest.
- Operator runbook entries (`docs/operator/`) for cancel / iterate / full-wash semantics.

### Kept verbatim

- ProblemsPanel — surfaces cross-process problems (credential health, layer-state anomalies, null coverage rows). Independent of the process list. Keep at top.
- FundDataRow — operator KPIs (analysable count, recommendations, per-tier coverage). Not a process. Keep below ProblemsPanel.
- Underlying ingest plumbing: `data_freshness_index`, `sec_filing_manifest`, `bootstrap_archive_results`, `external_data_watermarks`, all ON CONFLICT idempotency. **Nothing under `app/services/` for ingest changes; this is purely operator surface + cancel signal infra.**

### Out (file as follow-ups)

- WebSocket-driven streaming progress (current 5s/60s polling stays). #TBD
- Per-CIK / per-instrument deep drill-in beyond the per-error-class group view. #TBD
- Bootstrap dry-run / preview mode. #TBD
- AI / ranking / thesis pipeline rows. Not yet penciled.
- Multi-region universe rows. v1 is eToro tradable + US-SEC.

## Settled-decisions check

This spec amends two prior decisions; both amendments require explicit operator sign-off (already given 2026-05-08):

- **#993 §Cancel ("out of scope for v1"):** AMENDED. Cooperative cancel becomes a v1 requirement. Justification: operator drives this directly ("we need to know we can cancel a run"). Implementation is cooperative-flag (not hard-kill) — orchestrator checks the flag between stages, mid-stage work runs to completion. Watermarks ensure resume picks up where the cancelled stage stopped.
- **#993 + admin-page-unified-processes spec coexistence:** SUPERSEDED. Both prior specs are subsumed by this one. Useful fragments salvaged below.

Other settled decisions preserved:
- **#719 process topology:** API process serves HTTP only. All cancel signals write to a queue / state table; orchestrator reads.
- **#532 fundamentals from regulated sources:** Unchanged. New surface only.
- **Provider strategy:** unchanged. Adapters read existing tables.

## Prevention-log applicable entries

- *"Hint / warning state with no clear-on-next-transition" (#321):* The auto-hide-errors-on-retry rule is precisely this — when a process moves to `running` after being `failed`, prior-run errors are hidden; if the new run also fails, errors re-show with the new error set, NOT a stale stack.
- *"Multi-query read handlers must use a single snapshot" (#1024):* `GET /system/processes` reads from N adapter sources; the handler runs them inside one `conn.transaction()` so the page can never show internally-inconsistent state across adapters.
- *"Fire-and-forget job triggers missing first-time guard" (#145):* Trigger endpoints check `kill_switch` + `bootstrap_state.status='complete'` (where applicable) before enqueueing. No silent kickoffs.
- *"Naive datetime in TIMESTAMPTZ query params" (#278):* All cancel + watermark timestamps stored as `timestamptz` with timezone-aware UTC `datetime.now(timezone.utc)`.
- *"Latest-run-first audit" (existing in `data_freshness.py`):* Process status reads the latest `job_runs` row only; older successes do NOT mask a fresh failure.

## Information architecture

The admin page collapses from **8 sections** to **3**:

```
┌─────────────────────────────────────────────────┐
│ ProblemsPanel                  (kept verbatim)  │
│ Cross-process problems: creds, null cov, etc.   │
├─────────────────────────────────────────────────┤
│ FundDataRow                    (kept verbatim)  │
│ KPIs: analysable, recs, tier coverage           │
├─────────────────────────────────────────────────┤
│ ProcessesTable                 (NEW — replaces  │
│                                 5 panels)       │
│ Lane chips: [All] [Setup] [Universe] [Candles]  │
│             [SEC] [Ownership] [Fundamentals]    │
│                                                 │
│ Process │ Lane │ Status │ Last run │ Next │ ⋮  │
│ ───────────────────────────────────────────────│
│ ●bootstrap │Setup│ pending │ — │ on-demand │ ⋮ │
│ ✗sec_form4 │SEC  │ failed3 │ 12m  │ in 1m   │ ⋮ │
│   ↑ retry-pending: next run will reattempt    │
│ ●13F_sweep │SEC  │ running │ — │ —          │ ⋮ │
│ ✓universe  │Univ │ ok      │ 7m  │ in 23h    │ ⋮ │
└─────────────────────────────────────────────────┘
```

Drill-in to a single process opens a **route** `/admin/processes/{id}`, three tabs:

```
/admin/processes/sec_form4_ingest

[Runs]  [Errors (3 grouped)]  [Schedule]

[Run now] [Iterate ⓘ] [⋮ Cancel · Full-wash]

╶─ Runs tab ────────────────────────────╴
2026-05-08 14:02 · 3m 21s · 4520 rows · ✓
2026-05-08 13:02 · 4m 02s · 4521 rows · ✗ 3 errors
  ↳ click expands per-item errors inline
…
```

**Why route, not modal:** deep-links survive refresh; operators can paste links in tickets / Slack. (Temporal pattern; rules from Agent C survey.)

**Why three tabs, not one giant view:** keeps default scannable. Errors tab pre-grouped by exception class (GoodJob pattern); operator triage is "5 ConnectionTimeouts" not "12 chronological rows".

## Process envelope (the unified data model)

Every row in the Processes table conforms to this envelope, regardless of whether it backs a scheduled cron job, a bootstrap stage parent, a sync-orchestrator layer, or an ingest sweep. Adapter functions translate from the underlying source.

```python
@dataclass(frozen=True, slots=True)
class ProcessRow:
    process_id: str            # stable: "bootstrap" | "sec_form4_ingest" | "ownership_layer" | ...
    display_name: str          # "First-install bootstrap" | "Insider Form 4 ingest" | ...
    lane: Literal["setup", "universe", "candles", "sec", "ownership",
                  "fundamentals", "ops", "ai"]
    mechanism: Literal["bootstrap", "scheduled_job", "ingest_sweep"]
                                # sync_layer deferred to v2 (Codex round 2 W4)
    status: Literal["idle", "pending_first_run", "running", "ok",
                    "failed", "stale", "pending_retry", "cancelled", "disabled"]
    last_run: ProcessRunSummary | None
    active_run: ActiveRunSummary | None    # populated when status='running'
    cadence_human: str         # "every 5m", "daily 03:00 UTC", "on demand"
    cadence_cron: str | None   # "*/5 * * * *" for verification; None for on-demand
    next_fire_at: datetime | None
    watermark: ProcessWatermark | None     # surfaced for operator visibility on Iterate
    can_iterate: bool          # incremental from watermark
    can_full_wash: bool        # reset watermark + re-run from epoch
    can_cancel: bool           # cooperative-cancel flag supported
    last_n_errors: list[ErrorClassSummary]  # last-7-days, GROUPED by error class
                                            # auto-empty when status='running' (auto-hide-on-retry)

@dataclass(frozen=True, slots=True)
class ProcessRunSummary:
    run_id: int
    started_at: datetime
    finished_at: datetime
    duration_seconds: float
    rows_processed: int | None
    rows_skipped_by_reason: dict[str, int]  # {"unresolved_cusip": 42, "rate_limited": 3}
                                            # adapters without per-reason granularity emit {"unknown": <count>}
    rows_errored: int
    status: Literal["success", "failure", "partial", "cancelled", "skipped"]
                                            # 'skipped' covers prereq-gated scheduled_job runs
    cancelled_by_operator_id: UUID | None   # operators.operator_id is UUID; NULL if not cancelled

@dataclass(frozen=True, slots=True)
class ActiveRunSummary:
    run_id: int
    started_at: datetime
    rows_processed_so_far: int | None
    progress_units_done: int | None         # populated where producer cooperates
    progress_units_total: int | None
    expected_p95_seconds: float | None      # from rolling 30-day stats; powers stale flag
    is_cancelling: bool                     # cancel flag observed by worker
    is_stale: bool                          # elapsed > 2 * expected_p95 AND no log line in 90s

@dataclass(frozen=True, slots=True)
class ProcessWatermark:
    """Operator-visible resume cursor. Mechanism-specific opaque token plus a
    human-readable summary for tooltips on the Iterate button."""
    cursor_kind: Literal["filed_at", "accession", "instrument_offset",
                         "stage_index", "epoch", "atom_etag"]
    cursor_value: str          # opaque to UI; renders verbatim in tooltip
    human: str                 # "Resume from filings filed after 2026-05-08T13:00Z"
    last_advanced_at: datetime # when the watermark last moved forward

@dataclass(frozen=True, slots=True)
class ErrorClassSummary:
    error_class: str           # "ConnectionTimeout" | "MissingCIK" | "Form4ParseError"
    count: int
    last_seen_at: datetime
    sample_message: str        # one truncated example
    sample_subject: str | None # e.g. "CIK 320193 / accession 0000320193-..."
```

The envelope is the single source of truth for the FE. Adapters fill it.

## Adapter map

| Mechanism | Source tables | Adapter file |
|---|---|---|
| `bootstrap` | `bootstrap_state`, `bootstrap_runs`, `bootstrap_stages`, `bootstrap_archive_results` | `app/services/processes/bootstrap_adapter.py` |
| `scheduled_job` | `app/workers/scheduler.py::SCHEDULED_JOBS` + `job_runs` + `pending_job_requests` | `app/services/processes/scheduled_adapter.py` |
| `ingest_sweep` | `sec_filing_manifest` aggregates + `data_freshness_index` aggregates + per-source ingest logs (`institutional_holdings_ingest_log`, `n_port_ingest_log`, etc.) | `app/services/processes/ingest_sweep_adapter.py` |

Each adapter exposes `list_rows() -> list[ProcessRow]` + `get_detail(process_id) -> ProcessRow | None` + `list_runs(process_id, days) -> list[ProcessRunSummary]` + `list_run_errors(process_id, run_id) -> list[ErrorClassSummary]`.

The 17 bootstrap stages are NOT separate process rows. The bootstrap row is one parent; the per-stage parallel-lane timeline lives in its custom drill-in.

**Sync-orchestrator surface (post-Codex):**
- The orchestrator surfaces as ONE row of `mechanism="scheduled_job"` with `process_id="orchestrator_full_sync"`. It is not its own mechanism in v1.
- The 10 underlying `LAYERS` (verified at `app/services/sync_orchestrator/registry.py:101`) are NOT independently surfaced as process rows. Layer health lives on the orchestrator row's drill-in (Runs tab → expand a run → per-layer summary).
- Layers triggered independently via standalone scheduled jobs (e.g. `JOB_DAILY_RESEARCH_REFRESH`, `JOB_NIGHTLY_UNIVERSE_SYNC`) appear as their own scheduled_job rows. Operators wanting to refresh a single layer go via "Run now" on that scheduled_job, not via a layer-row trigger.
- A future v2 may introduce a `sync_layer` mechanism that is independently triggerable; v1 keeps the model honest by deferring it.

## Watermark + resume contract

This is the load-bearing contract that makes Iterate / Cancel safe. Operator surfaces are described above; this section is the invariants.

### Watermark sources (existing, no new infra)

For each mechanism, the watermark cursor comes from an existing table:

| Mechanism family | Watermark source | Cursor kind |
|---|---|---|
| SEC submissions ingest (Form 3/4/5, DEF 14A, 8-K, 10-K, 13D/G) | `data_freshness_index.last_known_filed_at` per (subject_type, subject_id, source) | `filed_at` |
| SEC manifest worker | `sec_filing_manifest.next_retry_at` + `last_attempted_at` | `accession` |
| 13F quarterly sweep | `external_data_watermarks` provider-native ETag per filer CIK | `atom_etag` |
| NPORT ingest | `n_port_ingest_log` last_processed accession | `accession` |
| CUSIP universe / CIK refresh | `external_data_watermarks` ETag for `company_tickers.json` / SEC official 13(f) list | `atom_etag` |
| Candle refresh | `instrument_market_data_refresh` per-instrument `last_synced_at` | `instrument_offset` |
| Universe sync | `pending_job_requests` last successful run row id | `epoch` (no fine cursor; cheap rerun) |
| Bootstrap stages | `bootstrap_stages.stage_order` of last `success` per lane | `stage_index` |
| Sync orchestrator layer | `sync_runs.layer_state_at_finish` per layer | `stage_index` |
| Fundamentals (XBRL Company Facts) | `data_freshness_index` per CIK + `instrument_sec_profile.last_synced_at` | `filed_at` |

The adapter computes a `ProcessWatermark` from the appropriate source. The `human` field is rendered into the Iterate button tooltip:

> **Iterate** ⓘ
> Resume from filings filed after 2026-05-08T13:00Z (12 of 1547 instruments awaiting next poll)

### Iterate semantics

`POST /system/processes/{id}/trigger { "mode": "iterate" }`:

1. Reads current watermark.
2. Enqueues the underlying job with the watermark as the "since" parameter (or no-op if the watermark is already at present-state).
3. Idempotent at the watermark — running Iterate twice with no fresh data does NOT re-fetch the already-ingested filings. ON CONFLICT clauses on `sec_filing_manifest` and `data_freshness_index` provide the de-dupe guarantee.

### Full-wash semantics

`POST /system/processes/{id}/trigger { "mode": "full_wash" }`:

1. Resets the watermark to epoch (or process-specific minimum, e.g. 2 years ago for filings).
2. Enqueues the underlying job with no `since` filter.
3. Re-fetches everything. Idempotency layer (ON CONFLICT) prevents row duplication; the cost is bandwidth + rate-budget, not data corruption.
4. **Gated behind a typed-name confirm modal** — operator must type the process name. Argo `--restart` pattern.

### Cancel semantics — cooperative

`POST /system/processes/{id}/cancel { "mode": "cooperative" }`:

**Atomicity (post-Codex B3):** the entire flow runs in ONE transaction with row-level locking on the active run.

1. `BEGIN`.
2. Resolve active run for this process (mechanism-specific):
   - bootstrap → `SELECT * FROM bootstrap_runs WHERE id = (SELECT last_run_id FROM bootstrap_state WHERE id = 1) AND status = 'running' FOR UPDATE`
   - scheduled_job → `SELECT * FROM job_runs WHERE job_name = ? AND status = 'running' ORDER BY started_at DESC LIMIT 1 FOR UPDATE`
   - `orchestrator_full_sync` (special-cased: writes `sync_runs`, not `job_runs`) → `SELECT * FROM sync_runs WHERE status = 'running' ORDER BY started_at DESC LIMIT 1 FOR UPDATE`. `target_run_kind='sync_run'`.
3. If no active running row → `ROLLBACK`, return **409** with structured reason `{"reason": "no_active_run"}`.
4. INSERT into `process_stop_requests` with `(process_id, mechanism, target_run_kind, target_run_id, mode='cooperative', requested_by_operator_id)` — `target_run_id` is the locked run's id, not nullable. Partial unique index `(target_run_kind, target_run_id) WHERE completed_at IS NULL` prevents duplicate active stop rows; a duplicate insert returns 409 `{"reason": "stop_already_pending"}`.
5. UPDATE the active run row's `cancel_requested_at = now()` for fast-path observation by the worker. Per-mechanism columns: `bootstrap_runs.cancel_requested_at` (sql/136 in PR2), `job_runs.cancel_requested_at` (sql/137 in PR3), `sync_runs.cancel_requested_at` (sql/139 in PR6 — Codex round 5 R5-W3 ensures sync_runs has a fast-path column too, otherwise orchestrator_full_sync cancel relies solely on `process_stop_requests` polling and lacks symmetry with the other two).
6. `COMMIT`. Return 202.

**Observation points** (PR2 wires bootstrap, PR3 wires others):
- **Bootstrap orchestrator (PR2):** the dispatcher checks `is_stop_requested(target_run_kind='bootstrap_run', target_run_id=current_run_id)` at:
  - (W1) before submitting Phase A's first batch,
  - between any two ready batches in the topological dispatcher,
  - before kicking off Phase B lanes,
  - between stages within a lane (current dispatcher loop boundary).
  Worst-case observation latency: duration of the longest single in-flight stage. A 13F sweep is ~30 min; a CIK refresh is ~30s. Acceptable.
- **SEC manifest worker (covered separately, PR-TBD outside this redesign):** between accessions in the drain loop. Mid-accession parse runs to completion (writes are idempotent; watermark advances on commit).
- **Sync orchestrator (`orchestrator_full_sync`):** between layers in the DAG fixed-point loop.
- **Scheduled jobs (short-runners):** `can_cancel=False`. heartbeat / monitor_positions / fx_rates_refresh complete in <30s; not worth the plumbing.

Once the worker observes the flag, it:
- Sets `process_stop_requests.observed_at = now()` (tells the UI "stop signal saw the worker").
- Lets the in-flight checkpoint complete.
- Transitions the active run to `cancelled` status (`bootstrap_runs.status='cancelled'`, `bootstrap_state.status='cancelled'`, `job_runs.status='cancelled'`, etc.).
- Sets `process_stop_requests.completed_at = now()` (releases the active-stop unique-index slot for future cancels).

Audit-logged via existing operator-action audit trail.

UI flow: operator clicks Cancel → confirm modal → optimistic chip `cancelling…` → adapter polls and shows `cancelling (observed)` once `observed_at` is set → flips to `cancelled` once `completed_at` is set. Tooltip: `"will stop after current stage (avg ~30s)"`.

### Cancel — terminate (escape hatch, post-Codex B4)

`POST /system/processes/{id}/cancel { "mode": "terminate" }`:

**Critical honesty rewrite:** terminate in v1 is a **mark-for-cleanup signal**, NOT a forced state transition.

1. Same atomic flow as cooperative but with `mode='terminate'`.
2. Inserts the stop row + sets `cancel_requested_at` on the active run.
3. **Does NOT** transition the run to `cancelled` or `error`.
4. **Does NOT** release the active-run slot in the unique index.
5. **Does NOT** permit `Iterate` or `Full-wash` on the same process while the worker is still alive — UI surfaces the run as `terminating (will be cleaned up on next jobs restart)` and trigger endpoints return 409 `{"reason": "active_run_pending_termination"}`.
6. The worker, if still running, observes the terminate flag at the next cooperative checkpoint and treats it identically to cooperative-cancel (transitions to `cancelled`, sets `completed_at`).
7. If the worker is genuinely stuck (e.g. blocked in a non-cooperative C-extension call), the operator restarts the jobs process. Boot-recovery sweeps any `bootstrap_runs.cancel_requested_at IS NOT NULL AND status='running'` to `cancelled` and appends to `bootstrap_runs.notes` (column exists in sql/129; `bootstrap_runs` has no `last_error` column — Codex round 2 R2-B3) the line: `terminated by operator before jobs restart`. The orphaned bootstrap stages are also swept to `error` per existing reap_orphaned_running logic.

UI copy is honest: `"Terminate marks for cleanup. Active SEC fetches continue. To force a stop, use cooperative cancel and wait, or restart the jobs process."` This avoids the Argo stuck-after-stop bug pattern (#14709) where a faked hard-kill creates ghost runs.

**Why no real hard-kill in v1:** Python sync threads cannot be safely interrupted mid-IO. `os.kill(thread)` is impossible; the only true hard-stops are `SystemExit` injection (unsafe across psycopg transactions, can leak DB advisory locks) or process-level kill (operator restarts jobs container). Implementing a fake hard-kill at the API level would create a state-vs-reality divergence that's worse than the current "restart jobs process" path.

### Resume after cancel — the watermark guarantee (post-Codex B6)

**Honest version after Codex flagged the original transactional claim as false.**

The resume guarantee depends on **idempotent replay**, not transactional commit between worker writes and stage status. Specifically:

- **SEC manifest worker:** writes to `sec_filing_manifest` are ON CONFLICT idempotent (sql/118). Watermark advances per-accession on commit. Cancel mid-drain → next Iterate sees the same `WHERE ingest_status='pending'` cohort minus the accessions already ingested. No double-fetch, no missing rows.
- **`data_freshness_index`:** per-poll outcomes ON CONFLICT (sql/120). Cancel mid-poll-loop → next run picks up at the same `subjects_due_for_poll()` cohort. Idempotent.
- **`bootstrap_archive_results`:** per-archive PRIMARY KEY `(bootstrap_run_id, stage_key, archive_name)` (sql/130). Idempotent re-write.
- **`bootstrap_stages` status transitions:** NOT atomic with the underlying invoker's writes. `_run_one_stage` (verified at `app/services/bootstrap_orchestrator.py:271`) marks `status='success'` AFTER the invoker returns, in a separate transaction. The failure mode this admits: invoker writes commit, status update fails (DB hiccup), stage stays `running`. Boot-recovery sweep on jobs restart resets such orphaned stages to `error`. Operator clicks Iterate → ON CONFLICT no-ops the already-ingested rows → invoker re-runs cleanly → status finally lands at `success`. Worst case: one extra wasted invocation per orphaned stage on a jobs restart. Acceptable for v1.

**Tightening the bootstrap stage transactional gap is deferred to a v2 ticket** (would require pushing the status transition into each invoker's write transaction — invasive across all 17 invokers; cost > benefit for a rare failure mode).

Watermark visibility: the Iterate button tooltip renders the human-readable cursor (`"Resume from filings filed after 2026-05-08T13:00Z"`) so the operator always sees what would happen. If they want a clean slate, they pick Full-wash (gated by typed-name confirm + advisory-lock fence — see Full-wash semantics).

### Full-wash execution fence (post-Codex round 2 R2-B1)

The round-1 advisory-lock design was unsafe: `pg_try_advisory_xact_lock` releases at COMMIT, leaving a window between full-wash COMMIT and the queued worker actually starting; a scheduled run could race in and read the reset watermark before the full-wash worker started. The fix is a **persistent fence backed by `pending_job_requests`**.

Schema extension (sql/138, see Schema migrations):

```sql
ALTER TABLE pending_job_requests
    ADD COLUMN process_id TEXT,
    ADD COLUMN mode TEXT CHECK (mode IN ('iterate','full_wash'));

CREATE UNIQUE INDEX pending_job_requests_active_full_wash_idx
    ON pending_job_requests (process_id)
    WHERE mode = 'full_wash' AND status IN ('pending','claimed','dispatched');
-- (Codex round 6 R6-W1: matches the canonical sql/138 definition below;
-- partial UNIQUE catches concurrent INSERTs as UniqueViolation → 409.)
```

Full-wash handler runs in ONE transaction (Codex round 4 R4-B1 — pg_advisory_xact_lock instead of FOR UPDATE on possibly-empty watermark rows):

1. `BEGIN`.
2. **Acquire prelude lock:** `SELECT pg_advisory_xact_lock(hashtext(?process_id)::bigint)`. Lock is guaranteed (no row required), tx-scoped (auto-released at COMMIT), and identical key across ALL paths that mutate this process's state. Scheduled runs and Iterate workers acquire the same lock at start-of-work; whichever path commits first wins, and the other path observes the durable consequence (fence row OR active job_runs/sync_runs row).
3. **Fence check:** `SELECT 1 FROM pending_job_requests WHERE process_id = ? AND mode='full_wash' AND status IN ('pending','claimed','dispatched') FOR UPDATE`. If a row exists → `ROLLBACK`, return **409** `{"reason": "full_wash_already_pending"}`. (Note: the partial UNIQUE index would also catch a race past this check; this query short-circuits the common case before INSERT.)
4. **Active-run check:** `bootstrap_state.status != 'running'` for bootstrap; `EXISTS(SELECT 1 FROM job_runs WHERE job_name=? AND status='running')` for scheduled_job; `sync_runs` analogue for orchestrator_full_sync. If active → `ROLLBACK` 409 `{"reason": "active_run_in_progress", "advice": "cancel first"}`.
5. **Reset the watermark** to mechanism-specific minimum:
   - SEC ingest: `UPDATE data_freshness_index SET last_known_filed_at = NULL, state = 'unknown' WHERE source = ?`
   - Bootstrap: `UPDATE bootstrap_stages SET status='pending', started_at=NULL, completed_at=NULL, last_error=NULL WHERE bootstrap_run_id = (latest) AND status IN ('success','error','blocked','skipped')`
   - Manifest-driven: `UPDATE sec_filing_manifest SET ingest_status='pending', last_attempted_at=NULL, next_retry_at=NULL WHERE source = ?`
6. **INSERT** trigger row: `pending_job_requests (request_kind='manual_job', process_id=?, mode='full_wash', status='pending')`. UNIQUE partial index catches any duplicate concurrent insert as `UniqueViolation` → 409 `{"reason":"full_wash_already_pending"}`.
7. `COMMIT`.

**The queue row IS the persistent fence.** It survives across the gap between full-wash COMMIT and worker-start because the row is durable. Worker finalisation (Codex round 3 R3-W1): worker transitions the fence row to `status='completed'` (success path) or `status='rejected'` (failure path) — **never DELETE**. Preserves audit + matches existing `pending_job_requests` lifecycle (sql/084).

**Scheduled run interaction (post-Codex rounds 4 R4-B1 + 5 R5-W1/W2):**

The prelude is integrated into the existing `_tracked_job` wrapper at `app/jobs/runtime.py` so the SAME tx that opens the `job_runs` row also acquires the advisory lock, runs the fence check, and writes the row's terminal status. This avoids R5-W2's double-write concern (no separate prelude tx and `_tracked_job` tx; one writer per run).

```sql
-- _tracked_job wrapper (extended in PR3)
BEGIN;
-- (1) Acquire the same advisory lock the full-wash uses. Guaranteed lock,
--     no row required, auto-released at COMMIT.
SELECT pg_advisory_xact_lock(hashtext(?process_id)::bigint);

-- (2) Fence check.
SELECT 1 FROM pending_job_requests
 WHERE process_id = ? AND mode = 'full_wash'
   AND status IN ('pending','claimed','dispatched');

-- (3a) Fence held → INSERT job_runs (status='skipped',
--      error_msg='full-wash in progress for this process'),
--      COMMIT, return early. R5-W1: the skip row is COMMITTED, not rolled
--      back, so audit survives.
-- (3b) No fence → INSERT job_runs (status='running'). COMMIT.
```

After step 3b commits, the worker proceeds with its actual work. Mid-flight writes happen in their own transactions, not the prelude one. The terminal status update (`success`/`failure`/`cancelled`) writes on a separate transaction at run end — same as today's `_tracked_job` shape, just preceded by the locked prelude.

Iterate trigger handler invokes the same `_tracked_job` extension. No silent fallthrough — every entry path acquires the advisory lock, reads the fence, then either skips-and-commits OR publishes its own active marker before COMMIT.

**Why one writer:** the prelude IS the `job_runs` writer for scheduled + iterate paths. Existing pre-PR3 code that already INSERTed inside `_tracked_job` is unchanged in shape — we wrap the existing INSERT with the lock-and-fence-check. PR3's diff replaces, not duplicates.

**sync_runs analogue:** for `orchestrator_full_sync` the equivalent integration point is the orchestrator's run-start path (writes `sync_runs`). PR6 wires the same advisory-lock + fence-check prelude there, with the same R5-W1 commit-skip-row discipline.

**Iterate handler also gates:** Iterate is rejected with 409 while a full-wash is pending/dispatched (otherwise an Iterate could re-fill the watermark before the full-wash worker reads the reset state).

**Boot-recovery sweep for stuck dispatched rows (Codex round 4 R4-W1 prose fix):** if a full-wash queue row stays in `status='dispatched'` for >6 hours, boot-recovery transitions it to `status='rejected'` (verified at sql/084:23 — `failed` is not in the CHECK set) with `error_msg="dispatched row stuck >6h, freed by boot-recovery"`. Frees the fence for future triggers. (Reasonable upper bound: a worst-case bootstrap full-wash is ~2h; SEC drain ~1h.)

**Why this beats advisory lock:** advisory locks are tx-scoped; queue rows are durable. The fence covers the entire pending → claimed → dispatched → completed lifecycle in a single mechanism.

UI gate: full-wash button is behind a typed-name confirm modal. If the API returns 409, the modal shows the structured reason and offers a "Cancel active run first" link.

## Auto-hide-on-retry rule (Sidekiq pattern, post-Codex B7 + W2)

`last_n_errors` reads from the **latest TERMINAL run only**, never the 7-day window. This preserves latest-run-first audit (#1024 prevention-log entry): a successful run today must not show error chips from a 6-day-old failure.

Computation per row:

| latest terminal run status | next-run scheduled to cover failed scope? | retry currently in-flight? | `last_n_errors` returned | row status |
|---|---|---|---|---|
| `success` | — | — | empty | `ok` |
| `failure` | yes (covered) | yes | empty | `running` (auto-hide during retry) |
| `failure` | yes (covered) | no | empty | `pending_retry` (auto-hide; tooltip: "will retry at HH:MM") |
| `failure` | no | no | full grouped errors | `failed` |
| `cancelled` | — | — | empty | `cancelled` |

**"Covered" check** (post-W2): `pending_retry` only auto-hides errors when the adapter can prove the next run will reattempt the failed scope. Concretely:
- `kill_switch` is OFF for this job (else the next fire is gated; failure is actionable).
- The job is not paused via per-job pause (where supported).
- The next scheduled fire is within the freshness/retry window. For SEC ingest jobs, this means `next_retry_at` on the failed `sec_filing_manifest` rows is ≤ next scheduled fire. For watermark-driven sweeps, `data_freshness_index.next_recheck_at` covers the failed subjects.
- For one-shot jobs (no scheduled cadence), `pending_retry` is never set; auto-hide only fires when an explicit Iterate is in flight.

If any of these checks fail, the row stays `failed` with errors visible.

**Tooltip on auto-hidden errors:** `"hiding 3 prior errors during retry — re-shown if retry also fails or fails to reattempt failed subjects (next: 14:02 UTC)"`.

**Re-show on next failure:** if Iterate runs and ALSO fails, auto-hide flips off; new grouped errors take the slot. Operator never wonders "did the retry quietly fail?" — the row turns red again with the new error set, NOT a stale stack.

History tab in drill-in shows ALL last-7-day runs, including older failures, with full per-class drill-down. Auto-hide rule does NOT touch the History tab.

## Stale-detection rule

A process row is `stale` when:
- `status == "running"` AND `elapsed > 2 * expected_p95_seconds` AND no log line in 90s.

Computed in the adapter from rolling 30-day per-job p95 (existing `job_runs.duration` query). Surfaces as a third color (amber) on the row, plus a banner above the table:

> ⚠ `sec_form4_ingest` has emitted no progress in 1m 47s — expected ~3m, running 7m. [Cancel] [Logs]

The 90s "no log line" check requires a heartbeat. Until heartbeat plumbing is wired (PR4 in this spec), the rule degrades to "elapsed > 2 * expected_p95" — false-positive risk for genuinely long runs but no false-negatives.

## Error display rules — full

- **Group by `error_class`**, not chronologically. (GoodJob.)
- **Failed processes float to top** of the Processes table (sort: status priority, then next-fire ASC). (Dagster.)
- **Inline preview**: error_class + count + last_seen + truncated sample message. **Always visible** on the row, no click-to-reveal.
- **Drill-in**: Errors tab on `/admin/processes/{id}` shows the grouped list expanded; each group expands to last-N items with full message + per-item subject (CIK / accession / instrument).
- **No layout shift on expand** — drill-in is a route, not an inline accordion.
- **Re-show on next failure**: if Iterate runs and ALSO fails, the auto-hide flips off; new errors take the slot. (Operator never has to wonder "did the retry quietly fail?" — the row turns red again.)
- **Per-item items page** (deferred to PR5): paginated list of failed accessions / CIKs with link to SEC filing URL, error class, retry-eligible-after timestamp from `sec_filing_manifest.next_retry_at`.

## Visible-motion rules ("something whirring")

Three independent signals:
1. **Pulsing left border** on running rows — pure CSS, no data needed. Visible regardless of progress field. (NN/g long-wait pattern.)
2. **Client-side elapsed counter** ticks every second, computed from `active_run.started_at`.
3. **Records-processed counter** when the producer cooperates (`progress_units_done` populated). Degrades gracefully to spinner-only when not.

The row never displays an empty progress bar (operator's quote #1) — when no fraction is known, the progress slot is omitted, replaced by `<phase-label> · <elapsed>` text.

## API surface

All new endpoints under `/system/processes/...`:

| Method | Path | Returns | Notes |
|---|---|---|---|
| GET | `/system/processes` | `list[ProcessRow]` | Cross-adapter snapshot via existing `app/db/snapshot.py::snapshot_read()` helper (post-Codex round 2 R2-W6: a naive nested `conn.transaction()` silently degrades to a savepoint and stays READ COMMITTED). Handler enters `with snapshot_read(conn):` and calls every adapter inside that block. Required because plain READ COMMITTED would let bootstrap_state.status read pre-cancel while bootstrap_runs.status reads post-cancel within the same handler. |
| GET | `/system/processes/{id}` | `ProcessRow` | Detail incl. last 7d run summaries |
| GET | `/system/processes/{id}/runs?days=7` | `list[ProcessRunSummary]` | History, default 7d |
| GET | `/system/processes/{id}/runs/{run_id}/errors` | `list[ErrorClassSummary]` | Grouped errors |
| GET | `/system/processes/{id}/runs/{run_id}/items?error_class=X&page=N` | `list[PerItemError]` | Paginated per-item drill (deferred PR5) |
| POST | `/system/processes/{id}/trigger` | 202 + `request_id` | Body: `{"mode": "iterate" \| "full_wash"}`. Preconditions matrix below; 409 with structured `{"reason": ...}` on prereq failure. |
| POST | `/system/processes/{id}/cancel` | 202 | Body: `{"mode": "cooperative" \| "terminate"}`. Atomic SELECT-FOR-UPDATE on active run; 409 if no active run; 409 if active stop already pending. |

### Trigger preconditions matrix (post-Codex W3)

`POST /system/processes/{id}/trigger` checks these per-mechanism prerequisites in order. Failure short-circuits with **409** + `{"reason": "<key>", "advice": "<operator action>"}`. No silent enqueues.

| Process kind | Preconditions (in order) |
|---|---|
| bootstrap | (1) `kill_switch` is OFF, (2) `bootstrap_state.status NOT IN ('running')`, (3) no `pending_job_requests` row for `process_id='bootstrap'` with `mode='full_wash' AND status IN ('pending','claimed','dispatched')` (full-wash fence) |
| scheduled_job (gated by `_bootstrap_complete`) | (1) `kill_switch` OFF, (2) `bootstrap_state.status='complete'`, (3) no active `pending_job_requests` row for same `job_name` with `request_kind='manual_job'` AND `status IN ('pending','claimed','dispatched')`, (4) no full-wash queue row pending/dispatched for the same `process_id` |
| scheduled_job (ungated) | (1) `kill_switch` OFF, (2) no active manual_job request for same job (`status IN ('pending','claimed','dispatched')`), (3) no full-wash queue row pending/dispatched for the same `process_id` |
| ingest_sweep | (1) `kill_switch` OFF, (2) upstream filer-directory rows exist (`institutional_filers` non-empty for 13F sweep, etc.), (3) no full-wash queue row pending/dispatched for the same `process_id` |

The full-wash fence check is part of the precondition for ALL mechanisms because Iterate must skip if a full-wash is in progress (and vice versa). Single source of truth is `pending_job_requests` filtered by the partial index `pending_job_requests_active_full_wash_idx`.

UI surfaces the structured `reason` in the disabled-button tooltip (e.g. `"disabled: bootstrap_not_complete — finish setup first"`).

**Bootstrap-specific** endpoints retained as thin compatibility shim (the existing UI is being replaced, but operator-facing scripts may reference them):

- `GET /system/bootstrap/status` — kept; calls into bootstrap adapter.
- `POST /system/bootstrap/run` — redirects to `POST /system/processes/bootstrap/trigger { mode:"full_wash" }`.
- `POST /system/bootstrap/retry-failed` — redirects to `POST /system/processes/bootstrap/trigger { mode:"iterate" }`.
- `POST /system/bootstrap/mark-complete` — kept verbatim (escape hatch unchanged).
- **NEW** `POST /system/bootstrap/cancel` — calls cooperative-cancel path.

Shim removed in a follow-up after FE deprecation lands.

## Schema migrations

Three new migrations:

### `sql/135_process_stop_requests.sql`

Schema rewritten post-Codex (B2 + B3 + W6). `target_run_id` is NOT NULL and pinned at insert; partial unique on active stop slot prevents duplicate stop rows for the same in-flight run.

```sql
CREATE TABLE process_stop_requests (
    id                       BIGSERIAL PRIMARY KEY,
    process_id               TEXT        NOT NULL,
    mechanism                TEXT        NOT NULL
        CHECK (mechanism IN ('bootstrap', 'scheduled_job', 'ingest_sweep')),
    target_run_kind          TEXT        NOT NULL
        CHECK (target_run_kind IN ('bootstrap_run', 'job_run', 'sync_run')),
        -- 'sync_run' supports cancelling orchestrator_full_sync mid-DAG
        -- (Codex round 3 R3-B4: orchestrator_full_sync writes sync_runs,
        -- not job_runs).
    target_run_id            BIGINT      NOT NULL,
    mode                     TEXT        NOT NULL
        CHECK (mode IN ('cooperative', 'terminate')),
    requested_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    requested_by_operator_id UUID REFERENCES operators(operator_id),
    observed_at              TIMESTAMPTZ,
    completed_at             TIMESTAMPTZ
);

-- Partial unique: at most one ACTIVE stop request per (run_kind, run_id).
-- A second cancel against the same in-flight run gets a 409 from the API
-- atomically rather than racing.
CREATE UNIQUE INDEX process_stop_requests_active_unq
    ON process_stop_requests (target_run_kind, target_run_id)
    WHERE completed_at IS NULL;

-- Forensic lookup: list all stop requests for a process across history.
CREATE INDEX process_stop_requests_process_idx
    ON process_stop_requests (process_id, requested_at DESC);
```

Worker poll at a checkpoint:

```sql
SELECT id, mode FROM process_stop_requests
 WHERE target_run_kind = ? AND target_run_id = ?
   AND completed_at IS NULL
 ORDER BY requested_at DESC
 LIMIT 1;
```

The worker pins on the EXACT run id it owns, so a stop row for a later run cannot wrongly cancel the current one.

### `sql/136_bootstrap_runs_cancel.sql`

```sql
ALTER TABLE bootstrap_runs ADD COLUMN cancel_requested_at TIMESTAMPTZ;
ALTER TABLE bootstrap_runs DROP CONSTRAINT bootstrap_runs_status_check;
ALTER TABLE bootstrap_runs ADD CONSTRAINT bootstrap_runs_status_check
    CHECK (status IN ('running', 'complete', 'partial_error', 'cancelled'));

ALTER TABLE bootstrap_state DROP CONSTRAINT bootstrap_state_status_check;
ALTER TABLE bootstrap_state ADD CONSTRAINT bootstrap_state_status_check
    CHECK (status IN ('pending', 'running', 'complete', 'partial_error', 'cancelled'));

-- Partial-unique on running run is unchanged: bootstrap_runs_one_running_idx
-- continues to prevent two concurrent running runs.
```

`cancelled` is a new terminal state. Boot recovery sweeps it like `partial_error`: any `running` row whose `cancel_requested_at IS NOT NULL` flips to `cancelled` and appends to `bootstrap_runs.notes` the line `terminated by operator before jobs restart` (Codex round 2 R2-B3 — `bootstrap_runs` has no `last_error` column; the existing `notes TEXT` column is the audit field). `_bootstrap_complete` returns `(False, ...)` for `cancelled` (operator must Iterate or Re-run to advance the gate).

### `sql/137_job_runs_per_item_telemetry.sql`

Single transactional file (post-Codex round 2 R2-B4: dropped the dual-file CONCURRENTLY split because the eBull migration runner runs every file in one tx; index build lands in the same tx as the ALTERs).

```sql
-- sql/137_job_runs_per_item_telemetry.sql (transactional)
ALTER TABLE job_runs
    ADD COLUMN rows_skipped_by_reason JSONB NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN rows_errored           INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN error_classes          JSONB NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN cancel_requested_at    TIMESTAMPTZ,    -- post-Codex R2-B2
    ADD COLUMN cancelled_at           TIMESTAMPTZ;

-- Widen the existing CHECK to permit the new 'cancelled' status.
-- Mirrors the sql/020 widening pattern.
ALTER TABLE job_runs DROP CONSTRAINT job_runs_status_check;
ALTER TABLE job_runs ADD CONSTRAINT job_runs_status_check
    CHECK (status IN ('running', 'success', 'failure', 'skipped', 'cancelled'));

-- Per-process history accelerator. NON-concurrent (transactional file).
-- Acceptable: job_runs is small at our scale (~tens of thousands of rows);
-- index build expected sub-second. PR1 measures on dev DB and only adds a
-- maintenance-window note in PR10 runbook if duration >250ms.
CREATE INDEX IF NOT EXISTS job_runs_status_started_idx
    ON job_runs (job_name, started_at DESC)
    WHERE status IN ('failure', 'success', 'cancelled');
```

**Lock impact (Codex round 1 W8):** PG 11+ optimises `ADD COLUMN ... DEFAULT <constant>` to a metadata-only update — no full-table rewrite, no AccessExclusive lock for the rewrite phase. The CHECK widen takes a brief AccessExclusive on `job_runs` to drop+add the constraint; existing rows are already a subset of the new set so validation is fast. Dev + prod both PG 14+. Safe online.

**`error_classes` shape:** `{"ConnectionTimeout": {"count": 12, "sample_message": "...", "last_subject": "CIK 320193", "last_seen_at": "..."}}`. Producer-side: each ingester emits errors via `record_per_item_error(error_class, message, subject)` helper (new, in `app/services/job_telemetry.py`); aggregated into the JSONB on job completion.

**`rows_skipped_by_reason` naming (Codex W9):** chosen over plain `rows_skipped` to disambiguate from the existing scalar in `bootstrap_archive_results.rows_skipped` (which is also JSONB) and various ingest-log scalar `rows_skipped` columns. Adapters that don't have per-reason granularity emit `{"unknown": <count>}`.

`cancelled_at` is the per-run cancel timestamp; distinguishes `failure` from `cancelled` in the UI. `cancel_requested_at` is the operator-click moment; the worker observes this column at cancel checkpoints (fast-path) before falling back to `process_stop_requests`.

### `sql/138_pending_job_requests_full_wash_fence.sql`

Persistent fence for full-wash (post-Codex round 2 R2-B1).

```sql
ALTER TABLE pending_job_requests
    ADD COLUMN process_id TEXT,
    ADD COLUMN mode       TEXT
        CHECK (mode IN ('iterate', 'full_wash'));

-- UNIQUE partial index (Codex round 3 R3-B1): at most one ACTIVE full-wash
-- queue row per process. Concurrent POSTs racing past the fence-check are
-- caught here as UniqueViolation; handler maps to 409.
CREATE UNIQUE INDEX IF NOT EXISTS pending_job_requests_active_full_wash_idx
    ON pending_job_requests (process_id)
    WHERE mode = 'full_wash' AND status IN ('pending', 'claimed', 'dispatched');
```

`process_id` is nullable on legacy rows (everything written before this migration); `mode` is nullable for legacy rows. New triggers via `/system/processes/{id}/trigger` always populate both.

Boot-recovery responsibility (lives in `app/services/process_stop.py` under PR1):

```sql
-- Free a stuck full-wash fence after >6h dispatched. Status 'rejected'
-- (Codex round 3 R3-B2: pending_job_requests CHECK does not allow 'failed';
-- verified at sql/084:23 — set is pending|claimed|dispatched|completed|rejected).
UPDATE pending_job_requests
   SET status = 'rejected',
       error_msg = 'dispatched row stuck >6h, freed by boot-recovery'
 WHERE mode = 'full_wash'
   AND status = 'dispatched'
   AND requested_at < now() - interval '6 hours';

-- Free abandoned cancel stop rows.
UPDATE process_stop_requests
   SET completed_at = now(),
       observed_at = NULL                  -- sentinel: never observed
 WHERE completed_at IS NULL
   AND requested_at < now() - interval '6 hours';
```

The cancel-stop recovery mutation is post-Codex R2-W2's explicit semantics. `observed_at = NULL` after `completed_at` is set is the "abandoned, never observed" sentinel; UI surfaces this as a small audit row in the History tab so operators can see "this cancel request was abandoned by a jobs restart". Frees the partial-unique index slot for future cancels against the same run.

### `sql/139_sync_runs_cancel.sql`

Widens `sync_runs.status` to allow `cancelled` + adds `cancel_requested_at` (post-Codex round 4 R4-B2; current set verified at sql/033:21).

```sql
ALTER TABLE sync_runs ADD COLUMN cancel_requested_at TIMESTAMPTZ;

ALTER TABLE sync_runs DROP CONSTRAINT sync_runs_status_check;
ALTER TABLE sync_runs ADD CONSTRAINT sync_runs_status_check
    CHECK (status IN ('running', 'complete', 'partial', 'failed', 'cancelled'));
```

Cancel handler for `orchestrator_full_sync` writes `target_run_kind='sync_run'` + locked `sync_runs.sync_run_id`; on observation the worker transitions `sync_runs.status='cancelled'`.

**Finalizer-preserves-cancelled invariant (Codex round 5 R5-W4):** the existing sync orchestrator finalisation path computes terminal `sync_runs.status` from per-layer outcomes (complete / partial / failed). Without a guard, finalisation could overwrite a `cancelled` status set by the cancel checkpoint. PR6 amends the finaliser's UPDATE to:

```sql
UPDATE sync_runs
   SET status = ?, finished_at = now(), …
 WHERE sync_run_id = ?
   AND status = 'running'   -- R5-W4 guard: do not overwrite 'cancelled'
```

If the row has already transitioned to `cancelled` (the cancel checkpoint won the race), the UPDATE no-ops and the `cancelled` terminal status is preserved.

## Frontend components

| Component | Path | Replaces |
|---|---|---|
| `ProcessesTable.tsx` | `frontend/src/components/admin/ProcessesTable.tsx` | LayerHealthList, BootstrapPanel header, Background-tasks |
| `ProcessRow.tsx` | `frontend/src/components/admin/ProcessRow.tsx` | per-row in BootstrapPanel + LayerHealthList |
| `ProcessDetailRoute.tsx` | `frontend/src/pages/ProcessDetailPage.tsx` | SyncDashboard collapsible (drilled-in instead of inline) |
| `ProcessDetailRunsTab.tsx` | `frontend/src/components/admin/ProcessDetailRunsTab.tsx` | (new — last-7d run history) |
| `ProcessDetailErrorsTab.tsx` | `frontend/src/components/admin/ProcessDetailErrorsTab.tsx` | error sections in BootstrapPanel + ProblemsPanel-failing-jobs |
| `ProcessDetailScheduleTab.tsx` | `frontend/src/components/admin/ProcessDetailScheduleTab.tsx` | (new — cron + human + next-fire) |
| `BootstrapTimelineDrawer.tsx` | `frontend/src/components/admin/BootstrapTimelineDrawer.tsx` | full BootstrapPanel body (parallel-lane timeline as drill-in) |
| `useProcesses.ts` (hook) | `frontend/src/hooks/useProcesses.ts` | useBootstrapStatus + per-panel polling hooks |
| `LaneFilter.tsx` | `frontend/src/components/admin/LaneFilter.tsx` | (new — chip filter row) |

Decommissioned (deleted at end of PR sequence):
- `BootstrapPanel.tsx`
- `LayerHealthList.tsx`
- `SeedProgressPanel.tsx`
- `SyncDashboard.tsx`
- Background-tasks table inline JSX in AdminPage

Kept verbatim:
- `ProblemsPanel.tsx`
- `FundDataRow.tsx`
- `CollapsibleSection.tsx` (utility, used elsewhere)

## Status semantics — full enum

| Status | Color | Meaning | Transitions |
|---|---|---|---|
| `idle` | slate | Process registered, never run | → running on trigger / next fire |
| `pending_first_run` | slate-blue | First-install, not yet fired | → running on bootstrap dispatch |
| `running` | sky pulsing | Active worker | → ok / failed / cancelled |
| `ok` | emerald | Last run succeeded | → running on next fire |
| `failed` | red | Last run had errors AND no retry in flight | → pending_retry on next-fire scheduling, → running on retry |
| `pending_retry` | amber muted | Last run failed, next scheduled run within freshness window | → running when next fire happens |
| `stale` | amber pulsing | Running but past 2× expected_p95 | → running normal once heartbeat resumes |
| `cancelled` | slate-strikethrough | Operator-cancelled, watermark intact | → running on iterate / full-wash |
| `disabled` | slate-disabled | Killed by kill_switch or per-job pause | → idle when re-enabled |

## Failure-mode invariants

- **Adapter throws → `/system/processes` returns 200 with that mechanism's rows omitted plus a `partial: true` flag in the envelope.** Operator sees other lanes; ProblemsPanel surfaces the adapter failure separately. Page never goes white.
- **Cancel signal lost (jobs process restart between insert and observe) → boot recovery scans `process_stop_requests WHERE completed_at IS NULL AND requested_at < now() - 6 hours` (Codex round 2 R2-W2). Mutation: SET `completed_at = now()`, leave `observed_at = NULL` as the "abandoned, never observed" sentinel.** Frees the partial-unique active-stop slot. UI History tab surfaces these as audit rows so the operator sees "this cancel request was abandoned by a jobs restart". No silent stuck-cancelled state.
- **Watermark advances only on commit.** If a worker crashes mid-batch, the watermark stays at the last committed batch — the next Iterate re-attempts the failed batch. (Already true; PR1 verifies via test.)
- **Iterate twice in <1s** — second request gets 409 "iterate already in flight" by reading `pending_job_requests` for the same job_name + `request_kind='manual_job'` + status `pending`/`claimed`. (Existing dedup.)
- **Full-wash + Iterate race:** the persistent `pending_job_requests.mode='full_wash'` row is the fence (Codex round 2 R2-B1). Iterate is 409-rejected while the fence row exists in `('pending','claimed','dispatched')`. Scheduled runs that race in skip with `status='skipped'`, `error_msg="full-wash in progress for this process"`. The fence is durable across the gap between full-wash COMMIT and worker-start.

## Implementation PR sequence

Each PR is its own branch + PR + Codex pre-push + ticket-decomposed acceptance criteria. Squash-merge each.

### PR1 — Schema + cancel signal infra (S)

Branch: `feature/<umbrella>-1-cancel-schema`
- sql/135 + sql/136 + sql/137 + sql/138 (full-wash fence on `pending_job_requests`) + sql/139 (sync_runs cancel widening — Codex round 6 R6-W2: schema must land before PR6 wires sync cancel; keeping all schema in PR1 avoids cross-PR ordering risk).
- `app/services/process_stop.py`: helpers `request_stop()`, `is_stop_requested(process_id)`, `mark_observed()`, `mark_completed()`.
- `app/services/job_telemetry.py`: `record_per_item_error(error_class, message, subject)`; aggregator on job completion.
- Smoke: `tests/smoke/test_app_boots.py` asserts `process_stop_requests` table exists post-migration.
- Tests: unit on the helpers + transition invariants.

### PR2 — Bootstrap cooperative cancel + cancelled state (S)

Branch: `feature/<umbrella>-2-bootstrap-cancel`
- Hook `is_stop_requested("bootstrap")` between phases in `app/services/bootstrap_orchestrator.py:run_bootstrap_orchestrator`.
- Add `cancelled` to bootstrap state machine; boot recovery handles it; `_bootstrap_complete` rejects it.
- New endpoint `POST /system/bootstrap/cancel` in `app/api/bootstrap.py` (writes to `process_stop_requests`, mirrors to `bootstrap_runs.cancel_requested_at`).
- Tests: cancel mid-Phase-B-lane, cancel before Phase-A, cancel-then-iterate resume.

### PR3 — Backend `/system/processes` envelope + scheduled adapter + bootstrap adapter (M)

Branch: `feature/<umbrella>-3-processes-backend`
- `app/services/processes/__init__.py` + envelope dataclasses.
- `bootstrap_adapter.py` + `scheduled_adapter.py`.
- New router `app/api/processes.py` with GET endpoints + POST `/trigger` + POST `/cancel`.
- Stub adapter for ingest_sweep returning empty (filled in PR6). (sync_layer mechanism purged from v1 per Codex round 2 R2-W4.)
- Tests: adapter round-trip + endpoint contracts + 409 dedup.

### PR4 — Watermark surfacing + per-process resume contract (M)

Branch: `feature/<umbrella>-4-watermarks`
- `app/services/processes/watermarks.py`: per-mechanism resolver returning `ProcessWatermark`.
- Wire each adapter to the resolver.
- Iterate / full-wash reset semantics on the trigger handler.
- Tests: per-mechanism watermark round-trip.

### PR5 — Frontend `ProcessesTable` + drill-in route (M)

Branch: `feature/<umbrella>-5-processes-frontend`
- `useProcesses.ts` polls `/system/processes` 5s while any row is `running`, 30s otherwise.
- `ProcessesTable.tsx` + `ProcessRow.tsx` + `LaneFilter.tsx` rendered on `/admin`.
- `/admin/processes/{id}` route + three-tab layout.
- Trigger buttons (Iterate / Full-wash / Cancel) wired with confirm modals where required.
- Auto-hide-errors-on-retry computed in adapter (BE) — FE just renders.
- Tests: per-status rendering + button visibility + lane filter + auto-hide rule.
- Manual: BFE smoke against live dev stack — operator clicks Iterate on Form 4 ingest, watermark visible in tooltip, second Iterate is no-op.

### PR6 — Ingest-sweep adapter + orchestrator-full-sync drill-in (M, post-Codex W4)

Branch: `feature/<umbrella>-6-remaining-adapters`
- Implement `ingest_sweep_adapter.py` (sec_filing_manifest + data_freshness_index aggregates per source).
- `orchestrator_full_sync` is one scheduled_job row already (PR3); add a custom drill-in tab for it that renders the 10-LAYERS DAG state for the latest run (data sourced from `sync_runs` + layer-state files). NOT a separate `sync_layer` mechanism.
- Decommission `LayerHealthList.tsx` — data now lives in the orchestrator drill-in + scheduled_job rows for independently-triggerable jobs.
- Decommission `SyncDashboard.tsx` — its content moves to the `orchestrator_full_sync` row's drill-in (Runs tab + custom DAG tab).
- The `sync_layer` mechanism is explicitly deferred to a v2 ticket (filed in PR10's runbook deliverable).

### PR7 — Bootstrap timeline drawer + decommission BootstrapPanel (M)

Branch: `feature/<umbrella>-7-bootstrap-as-row`
- Bootstrap row in ProcessesTable; clicking the timeline icon opens the parallel-lane timeline drawer.
- Drawer shows the 17-stage tree with archive sublist as a side-strip (Agent C "archive squares" pattern); per-archive detail in nested drawer (no inline expansion).
- Delete `BootstrapPanel.tsx` + tests.

### PR8 — Stale-detection + visible-motion polish (S)

Branch: `feature/<umbrella>-8-stale-detection`
- Adapter computes `is_stale` from rolling p95 + last log timestamp.
- Banner above ProcessesTable for stale-detected processes.
- Pulsing-left-border CSS on running rows.

### PR9 — Decommission SeedProgressPanel + dark-mode + a11y pass (S)

Branch: `feature/<umbrella>-9-decommission`
- Delete SeedProgressPanel + tests (data fully covered by SEC ingest process rows).
- Dark-mode class hygiene per `frontend/scripts/check-dark-classes.mjs` 5 checks.
- a11y: keyboard nav on ProcessesTable, screen-reader labels on lane chips + status pills, `prefers-reduced-motion` for the pulsing animations.
- Lighthouse + axe runs against dev stack.

### PR10 — Operator runbook + spec amendments (S)

Branch: `feature/<umbrella>-10-runbook`
- `docs/operator/cancel-and-resume.md`: when to use Iterate vs Full-wash vs Cancel; what watermarks mean; how to recover from `cancelled` state.
- `docs/operator/stuck-process-triage.md`: stale flag + heartbeat + jobs-process restart procedure.
- Update `docs/settled-decisions.md` to record the cancel-amendment.
- Add prevention-log entry: "Cancel UX must be cooperative-with-checkpoints, never faked hard-kill."

PR1 + PR2 + PR3 are linear (each blocks the next).
PR4 can land in parallel with PR5 (both depend on PR3).
PR6 + PR7 depend on PR5.
PR8 + PR9 + PR10 can land in any order after PR7.

## Test plan

### Unit (Python)

- `tests/test_process_stop.py` — request / observe / complete state machine; race conditions.
- `tests/test_job_telemetry.py` — per-item error aggregation; JSONB shape; idempotent on re-record.
- `tests/test_processes_envelope.py` — envelope dataclass invariants.
- `tests/test_bootstrap_adapter.py` — round-trip from bootstrap_runs/stages → ProcessRow.
- `tests/test_scheduled_adapter.py` — round-trip from SCHEDULED_JOBS + job_runs → ProcessRow.
- `tests/test_processes_endpoints.py` — `/system/processes` contract + 409 dedup + 422 on bad mode + auth.
- `tests/test_bootstrap_cancel.py` — cancel between stages; cancel-then-iterate resume; cancel during Phase A; cancel during sec lane.
- `tests/test_watermark_resolver.py` — each cursor_kind round-trips correctly.
- `tests/smoke/test_app_boots.py` — assert new tables + indexes exist.

### Integration

- `tests/integration/test_admin_control_hub_flow.py` — full happy path: bootstrap → cancel → iterate → complete; Form 4 ingest fail → next-fire retry → auto-hide errors → success.

### Frontend

- `ProcessesTable.test.tsx` — render with each status, lane filter, auto-hide rule.
- `ProcessRow.test.tsx` — buttons by status; watermark tooltip; pulsing border CSS.
- `ProcessDetailErrorsTab.test.tsx` — group-by-error-class rendering; expand-without-shift.
- `BootstrapTimelineDrawer.test.tsx` — 17-stage parallel-lane render; archive side-strip click → nested drawer.
- `useProcesses.test.ts` — polling cadence transitions on status change.

### Manual

- Trigger Iterate on a process with a known watermark; verify the watermark advances correctly via DB query.
- Trigger Cancel mid-bootstrap (Phase B SEC lane); verify `bootstrap_state.status='cancelled'`, current stage finishes, no later stages start.
- Trigger Iterate on the cancelled bootstrap; verify it resumes from the next stage in the lane (not from scratch).
- Trigger Full-wash on `sec_form4_ingest`; verify watermark resets to 2 years ago, full re-fetch happens.
- Make a process fail (force a bad CIK); verify errors appear; verify they auto-hide when the next scheduled run starts; verify they re-show if the next run also fails.
- Resize browser to 768px; verify layout remains usable (no horizontal scroll on the ProcessesTable).
- Tab through the page with keyboard; verify focus rings + activation on every action.

## Open questions remaining after Codex round 1

(Closed by amendments above: cancel atomicity, terminate honesty, full-wash fence, schema CHECK widening, latest-terminal-run audit, REPEATABLE READ snapshot, sync_layer dishonesty, advisory-lock fence, sql/137 status widening, partial-unique on stop rows.)

Open for Codex round 2 + operator review:

1. **Heartbeat plumbing.** Stale-detection by `2 * expected_p95` alone is false-positive-prone on slower-than-usual runs. Worth adding a heartbeat write (touch a `job_runs.heartbeat_at` every N rows or every 30s) in PR8, or defer to v2 ticket? Spec assumes deferred — v1 stale rule is `elapsed > 2 * expected_p95` only.

2. **Per-mechanism cancel checkpoint cadence.** Bootstrap stage-boundary checks have worst-case latency = duration of longest stage (~30 min for 13F sweep). Acceptable for v1 because operators typically cancel and walk away; not for tight feedback loops. Tightening would push checkpoints into invokers themselves (mid-stage). Defer to v2.

3. **Watermark visibility on the row.** Iterate button tooltip shows the watermark. Should we ALSO show it as a small caption under the row's last-run timestamp (`watermark: 2026-05-08T13:00`)? Adds row noise; answers "what would Iterate fetch?" without hover. Spec leaves this for FE PR5 to prototype both and pick.

4. **Lane chip filter persistence.** Chip selection preserved across page navigation via URL query (`?lane=sec`). Prefer URL for shareability over `localStorage`. Decided.

5. **Full-wash button copy per mechanism.** `mode=full_wash` on bootstrap → "Re-run all stages". On `sec_form4_ingest` → "Re-fetch from epoch". The trigger contract returns mechanism-specific labels in the `ProcessRow` envelope (`full_wash_label: str`); FE renders verbatim. Decided.

6. ~~**Migration runner CONCURRENTLY support.**~~ **SUPERSEDED by R2-B4** — index now ships in the transactional sql/137 (non-CONCURRENT). Acceptable at our table size; PR1 measures lock duration on dev DB and documents only if >250ms.

## Acceptance criteria (unified)

- [ ] Admin page collapses from 8 sections to 3 (Problems / KPIs / Processes).
- [ ] Single ProcessesTable shows every backend job/layer/sweep/bootstrap-stage parent.
- [ ] Failures float to top by default; lane chips filter the view.
- [ ] Drill-in by route, three tabs (Runs / Errors / Schedule), per-error-class grouping.
- [ ] Cooperative cancel works end-to-end on bootstrap + SEC manifest worker + sync orchestrator.
- [ ] Iterate resumes from durable watermark; double-Iterate is a no-op.
- [ ] Full-wash gated behind typed-name confirm; resets watermark + re-runs.
- [ ] Errors auto-hide while a retry is in flight; re-show on next failure.
- [ ] Stale-detection flags running rows past 2× p95.
- [ ] Bootstrap is one row; parallel-lane timeline lives in drawer.
- [ ] No layout shift on any expand/collapse interaction.
- [ ] No empty progress affordance ever.
- [ ] All existing operator coverage preserved (ProblemsPanel + FundDataRow unchanged).
- [ ] Dev-stack smoke: full happy-path manual test passes.

## Out of scope (file as follow-ups)

- WebSocket-driven streaming progress (#TBD).
- Per-CIK / per-instrument deep drill-in beyond per-error-class group view (#TBD).
- Bootstrap dry-run / preview (#TBD).
- Heartbeat plumbing (decided in Codex Q1; likely v2).
- AI / ranking / thesis pipeline rows (out of scope per #993).
- TSLA / GOOGL CIK→canonical-name programmatic bridge (still wanted; separate ticket — not part of this redesign).
