# Post-bootstrap auto-current (#1511 / T5 of #1508)

Status: Spec (2026-06-06). Child of epic #1508; builds on the shipped verdict (#1512, merged 5b38dde).
Parent proposal: `docs/proposals/ui/admin-processes-self-healing-health.md` §3 decision 3(a–c).

## Goal

After a clean bootstrap the Processes page reads ~all **Current** with zero operator clicks, and
gate-skipped catch-up jobs recover without a process restart.

## Empirical grounding (dev DB, 2026-06-06 — verified, do not re-diagnose)

- `bootstrap_state.status='complete'` since 06-03 19:47.
- **`data_freshness_index` is ALREADY fully seeded** by bulk ingest (`record_manifest_entry` →
  `seed_freshness_for_manifest_row`). Every SEC source has thousands of `state='current'` rows,
  newest filing within days: `sec_13f_hr` 9558 rows (max 06-05), `sec_form4` 4873 (max 06-06),
  `sec_xbrl_facts` via companyfacts, etc. **So "seed watermarks" (issue title) is a no-op on a
  real bootstrap — the defect is read-side: the verdict never consults the watermark.**
- Per-subject `expected_next_at < now` ("overdue") counts are huge (`sec_form3` 4795/5358) because
  event-driven filers file once then stop. So `_has_data_freshness_gap` (ANY-subject-overdue) is
  the WRONG freshness signal for the look-through — use **source-level recency** (newest filing
  within cadence) instead.
- **Catch-up trap CONFIRMED LIVE:** `cusip_extid_sweep` + `ownership_observations_sync`
  (both `catch_up_on_boot=True`) latest terminal = `skipped / bootstrap_not_complete` (06-03);
  never recovered. These only un-stick on a jobs-process restart today.
- The issue's named "genuine gaps" are stale: bulk refreshes ran 06-06, RegSHO ran 06-05,
  `finra_short_interest_refresh` is **failing** (#1516) not absent. **(b) has ∅ live targets** —
  the kick is a self-populating mechanism, correct-by-construction-empty now.

## Scope — three parts, two independent code areas

### Part (a) — verdict look-through (READ-SIDE; not a write)

`compute_verdict` (`app/services/processes/health_verdict.py:120`) maps `pending_first_run` →
`("working", "first run pending")` unconditionally; it receives only `status` + `stale_reasons`.
A never-run steady-state poll job whose SEC data bootstrap already filled therefore reads blue
"working" forever instead of green Current.

Fix: thread a `watermark_is_fresh: bool` signal into the verdict.

1. **`compute_verdict`** — add keyword `watermark_is_fresh: bool = False`. Only the
   `pending_first_run` branch consumes it:
   ```python
   if status == "pending_first_run":
       if watermark_is_fresh:
           return ("current", False, "")          # data seeded by bootstrap; steady-state poll not yet due
       return ("working", False, "first run pending")
   ```
   Precedence unchanged: an actionable stale reason still outranks (a genuinely overdue source
   keeps its `watermark_gap → attention`). Default `False` keeps every other caller + adapter
   (bootstrap, ingest_sweep) byte-identical.
2. **Adapter** (`scheduled_adapter._build_row`) computes the signal and stores it on `ProcessRow`:
   ```python
   source = freshness_source_for(job.name)
   source_watermark_fresh = (
       source in BOOTSTRAP_COVERED_FRESHNESS_SOURCES
       and _source_watermark_fresh(conn, source=source, now=now)
   )
   ```
   New `ProcessRow` field `source_watermark_fresh: bool = False` (frozen-slots dataclass; default
   keeps the other two adapters unchanged).
3. **`_convert_row`** (`app/api/processes.py:419`) passes `watermark_is_fresh=row.source_watermark_fresh`.
4. **`_source_watermark_fresh`** (new, in `watermarks.py` beside `resolve_watermark`):
   ```sql
   SELECT MAX(last_known_filed_at) FROM data_freshness_index WHERE source = %(source)s
   ```
   fresh ⇔ `max IS NOT NULL AND max >= now - cadence_for(source) - WATERMARK_GAP_TOLERANCE_S`.
   Source-level (robust to per-subject event-driven noise). Reuses `data_freshness.cadence_for`.

**Net visible effect:** a never-run job on a covered+fresh source (e.g. `fundamentals_sync` →
`sec_xbrl_facts`, fresh post-bootstrap) flips blue→green. Jobs with no freshness source
(orchestrator_full_sync, retention/maintenance sweeps) keep "working" — honest; they run at their
next slot. No false green: covered-but-stale → not fresh → stays working/attention. (Verify the
exact look-through job set at impl against `_JOB_REGISTRY` freshness sources × the never-run set —
several `_JOB_REGISTRY` entries, e.g. `sec_8k_events_ingest`, are retired from `SCHEDULED_JOBS` and
won't appear as scheduled-adapter rows.)

### Parts (b)+(c) — finalize_run audited activation (WRITE-SIDE)

Single chokepoint: `bootstrap_state.finalize_run` (`app/services/bootstrap_state.py:945`). Inside
the existing `with conn.transaction()`, capture `rowcount` of the `bootstrap_state` UPDATE (the
`running→complete` transition). **AFTER that transaction commits** (status durable), call activation
iff **both** `terminal == "complete"` AND the captured `rowcount == 1` (this call won the
transition — once-per-completion, idempotent on iterate/retry re-finalize).

**Why post-commit, not in-tx (Codex ckpt-1 BLOCKING):** psycopg3 aborts the whole transaction on
the first DB error; a caught enqueue/audit failure inside finalize_run's tx would NOT un-abort it,
so the status-flip commit would itself fail and roll the completion back (cf.
`feedback_psycopg3_savepoint_commit`). Running activation after the status commit makes completion
durable first; the gate is already open at that point so the kick needs no override, and an
activation failure can no longer poison the completion. NOTIFY-with-status atomicity is deliberately
**not** required — activation is best-effort (see backstop below).

New module `app/services/processes/post_bootstrap_activation.py`:
`activate_post_bootstrap(conn, *, run_id) -> list[str]` (returns enqueued job names; lazy-imports
scheduler registry + dispatcher to avoid load cycles — repo pattern, cf. `app/jobs/sources.py`).
Manages its own transactions on the (now-idle) conn — **one `with conn.transaction()` per candidate**
so a single candidate's failure rolls back only its own savepoint and the loop continues
(best-effort per candidate; Codex ckpt-1 WARNING 2/3).

Candidate set (union of (c) then (b)):

- **(c) catch-up-trap recovery:** scheduled jobs with `catch_up_on_boot=True` whose LATEST terminal
  `job_runs` row is `status='skipped'` with `error_msg = bootstrap_not_complete`. → live: exactly
  `cusip_extid_sweep`, `ownership_observations_sync`.
- **(b) self-populating genuine-gap kick:** scheduled jobs where ALL of:
  `catch_up_on_boot == False` (catch-up jobs handled by (c)/boot loop) AND
  no terminal `job_runs` row at all (never fired) AND
  `freshness_source_for(job) is not None AND that source ∉ BOOTSTRAP_COVERED_FRESHNESS_SOURCES`
  (its operator-visible data is a genuine gap bootstrap did NOT fill) AND
  `prerequisite is None` (empty-DB-safe proxy, per settled-decision #1181 carve-out criteria).
  → live: ∅ (every registered freshness source is bootstrap-covered; finra jobs have no registry
  source and already run daily). Mechanism present, fires only when a real uncovered gap exists.

Per candidate, gated by a guard against double-fire (skip if an active `pending_job_requests`
row — `status IN ('pending','claimed','dispatched')` — already exists for that `job_name`):

1. `publish_manual_job_request_with_conn(conn, job_name, requested_by="system:post_bootstrap_activation")`
   — INSERT + NOTIFY inside the per-candidate `conn.transaction()`; status is already
   `complete`-committed, so NOTIFY flushes to a listener that finds an already-open gate.
   **No `override_bootstrap_gate`** → allow-list stays 2, test-pinned (#1064/#1181 preserved).
2. One `decision_audit` row (mirrors `_write_override_audit` insert shape):
   `stage='post_bootstrap_activation'`, `pass_fail='KICK'`,
   `explanation` naming job + reason (`catch_up_trap_recovery` | `genuine_gap_kick`),
   `evidence_json={job_name, reason, run_id, bootstrap_completed_at}`. Audit-write failure logs but
   does not abort the run (same posture as the override audit).

### Shared — bootstrap-covered source map + drift test

`app/services/processes/bootstrap_coverage.py`:
```python
_BOOTSTRAP_STAGE_FRESHNESS_SOURCES: dict[str, frozenset[ManifestSource]] = {
    # stage job_name -> data_freshness_index sources its sink populates ; frozenset() = no freshness sink
    "sec_submissions_ingest":          frozenset({"sec_10k","sec_10q","sec_8k","sec_def14a","sec_13d","sec_13g"}),
    "sec_first_install_drain":         frozenset({"sec_10k","sec_10q","sec_8k","sec_def14a","sec_13d","sec_13g"}),
    "sec_master_idx_gap_close":        GAP_CLOSE_FILING_METADATA_SOURCES,  # imported, not re-listed
    "sec_companyfacts_ingest":         frozenset({"sec_xbrl_facts"}),
    "sec_13f_ingest_from_dataset":     frozenset({"sec_13f_hr"}),
    "sec_insider_ingest_from_dataset": frozenset({"sec_form3","sec_form4","sec_form5"}),
    "sec_nport_ingest_from_dataset":   frozenset({"sec_n_port"}),
    "sec_8k_events_ingest":            frozenset(),  # parses events; freshness owned by submissions/gap_close
    # …every other stage job_name → frozenset() (no freshness sink)
}
BOOTSTRAP_COVERED_FRESHNESS_SOURCES: frozenset[ManifestSource] = frozenset().union(*…values())
```
Excluded (NOT bootstrap-covered): `finra_short_interest`, `finra_regsho_daily` (steady-state FINRA
lanes), `sec_n_csr` (steady-state manifest-worker/lazy discovery — `mf_directory_sync` seeds the
directory, not N-CSR filings).

**Drift test** (`tests/services/processes/test_bootstrap_coverage.py`, mirrors
`tests/test_job_registry.py::test_registry_covers_every_bootstrap_stage`):
- Every `stage.job_name` in `_BOOTSTRAP_STAGE_SPECS` is a key in the map (a new/renamed stage forces
  a deliberate `frozenset()`-or-sources decision — no silent staleness).
- Every value ⊆ valid `ManifestSource` (typo guard).
- `BOOTSTRAP_COVERED_FRESHNESS_SOURCES` == expected pinned set (regression pin on the union).

## Settled-decision / prevention compliance

- **Universal gate #1064/#1181:** kick via audited manual-queue AFTER the gate opens; no new
  `exempt_from_universal_bootstrap_gate` carve-out (allow-list stays 2, test-pinned).
- **catch-up trap (prevention 1339-1343):** part (c) is the documented fix — audited re-queue, not
  a silently-lost fire.
- **skip ≠ completed (1217):** activation reads/writes terminal state correctly; re-enqueue routes
  through the normal dispatch→`_tracked_job` path which records terminal status.
- **none vs skipped (249):** `pending_first_run` (never-run) stays a distinct verdict input; not
  folded into attention. Look-through promotes it to Current ONLY on covered+fresh source.

## Risks

- **Re-kick into a held rate-limit/gate (relates #1484):** part (b) is ∅ now and predicate-gated to
  `prerequisite is None` + empty-DB-safe; (c)'s two jobs are bounded DB sweeps (one indexed JOIN /
  drift scan), not rate-bound. No re-enqueue-into-own-limit path.
- **Map drift:** the drift test is load-bearing; without it a new stage silently mis-classifies a
  source as covered (false green) or uncovered (spurious kick).
- **finalize_run coupling / abort-safety (Codex ckpt-1 BLOCKING):** activation runs AFTER the
  status-commit, in its own per-candidate `conn.transaction()` (savepoint-isolated), lazy-importing
  scheduler/dispatcher. A candidate's enqueue/audit failure rolls back only that candidate's
  savepoint; the completion is already durable and the loop proceeds. The top-level
  `activate_post_bootstrap` call is itself wrapped (catch + log) so it can never propagate past
  finalize_run. Each candidate's enqueue + its audit row share one savepoint (either both land or
  neither — no orphan audit without a queued request, no queued request without an audit).
- **Best-effort is acceptable (Codex ckpt-1 WARNING 2/3):** activation is a completion-time
  accelerator, NOT the sole recovery path. The durable backstop is that finalize flips the gate
  OPEN, after which the trapped jobs' **normal scheduled fires** re-evaluate the (now-passing) gate
  — both are daily (`ownership_observations_sync` 03:30, `cusip_extid_sweep` 04:50), so ≤24h
  worst-case even if activation is skipped/fails — plus the next boot catch-up. The one-shot
  `rowcount==1` guard and partial-activation are therefore tolerable; no durable retry/marker is
  warranted. Re-enqueue bodies are idempotent (`ownership_observations_sync` = ON CONFLICT DO UPDATE
  on natural keys; `cusip_extid_sweep` = bounded indexed JOIN), so a duplicate with the next
  scheduled fire is harmless.

## Test plan

- `test_health_verdict.py`: extend the table — `pending_first_run` × `watermark_is_fresh ∈ {T,F}` →
  `{current, working}`; confirm an actionable stale reason still wins regardless of freshness.
- `test_bootstrap_coverage.py`: the three drift assertions above.
- `_source_watermark_fresh`: fresh / stale / no-rows → {True, False, False}; boundary at
  `cadence + tolerance`.
- `activate_post_bootstrap`: (c) picks exactly the gated-skip catch_up jobs; (b) ∅ on the real
  registry; synthetic uncovered-source never-run job → kicked; double-fire guard skips when an
  active request exists; non-complete terminal → no-op; `rowcount==0` (lost race) → no-op; audit
  row shape; activation exception is swallowed (completion still returns `complete`).
- Adapter: `source_watermark_fresh` True only for covered+fresh; False for uncovered / stale / None.

## DoD — operator-visible verification on dev (ETL clauses 8–12 adaptation)

This work changes verdict logic + a finalize_run enqueue; it does NOT alter parsers/schema/
observations data, so the ownership-rollup clauses are a regression check, not a backfill. Record at
the commit SHA:

1. **Verdict look-through:** query `data_freshness_index` source recency + the never-run job set;
   show ≥1 named `pending_first_run` job (e.g. `fundamentals_sync` / `sec_8k_events_ingest`) now
   computes `current` (drive `compute_verdict` with the live signal, or hit `/system/processes`).
2. **Catch-up recovery:** exercise `activate_post_bootstrap` against dev (or simulate the
   finalize→complete transition) and show `cusip_extid_sweep` + `ownership_observations_sync` get a
   fresh `pending_job_requests` row + `decision_audit` row, then a non-skipped `job_runs` row after
   the listener drains.
3. **(b) ∅ proof:** show the predicate yields the empty set against the live registry.
4. **Regression:** `/instruments/AAPL/ownership-rollup` still renders (data path untouched).
5. **`/system/job-liveness`** unaffected.
