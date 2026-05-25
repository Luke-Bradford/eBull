# Audit memo — #1225 bulk SEC ingesters land `rows_processed=NULL`

**Status**: Forensic record (does NOT gate the #1225 fix)
**Date**: 2026-05-25
**Author**: Phase 0 §2.1 work
**Source PR**: fix/1225-rows-processed-null-resolver-hardening
**Reference**: `docs/proposals/etl/phase-0-instrumentation.md` §2.1

## Symptom (issue body)

Run_id=3 (dev DB, 2026-05-17): S23 (`ownership_observations_backfill`) and S24 (`fundamentals_sync`) ended `blocked` with structured reason:

```
S23: missing capability institutional_inputs_seeded; no surviving provider met
     rows floor 1 (providers: sec_13f_ingest_from_dataset=success
     [rows_processed=NULL], sec_13f_recent_sweep=?)
S24: missing capability fundamentals_raw_seeded; no surviving provider met
     rows floor 1 (providers: sec_companyfacts_ingest=success
     [rows_processed=NULL])
```

`sec_13f_ingest_from_dataset` and `sec_companyfacts_ingest` landed `status='success'` but `bootstrap_stages.rows_processed=NULL`. The strict-gate cap-eval (`_capability_is_dead` at `bootstrap_orchestrator.py:732`, floor=1) marked the capability dead → S23/S24 blocked.

## Run_id=3 evidence is GONE

The 2026-05-17 dev DB state cannot be reproduced. Subsequent merges to main mutated the relevant code paths:

- **#1233 PR-3** (COPY refactor) — touched bulk-ingester rowcount paths
- **#1233 PR-12** (MERGE writer) — touched `ownership_*_current` refresh helpers
- **#1294** (`1692252`, 2026-05-23) — fixed `sec_companyfacts_ingest` `rows_written` semantics specifically
- **#1218** (XBRL partition guards) — touched downstream ingest path

Audit cannot dispositively reproduce the original NULL. This memo is FORENSIC — documents the diagnosis + cites the fix. The fix lands regardless.

## 4-candidate decision tree (per Phase 0 §2.1 plan v1.5)

The plan listed 4 root-cause candidates. Multi-agent + Codex review eliminated/recharacterised them all:

### Candidate 1 — `_current_running_bootstrap_run_id()` returns wrong run

**Symptom**: when bulk job runs OUTSIDE orchestrator context (manual `/jobs/<name>/run` fire), `_current_running_bootstrap_run_id()` at `sec_bulk_orchestrator_jobs.py:90` returns `None`, the `if run_id is not None` guard skips `_record_archive_result`, source 1 stays empty for that run.

**Disposition**: PARTIAL — collapses onto Candidate 2 as the same chokepoint (writer side). Manual-fire path has `run_id=None` so the resolver isn't invoked anyway (no stage to resolve); cannot produce the run_id=3 symptom (`success [rows_processed=NULL]` on a stage row).

### Candidate 2 — writer/reader run-id mismatch

**Symptom**: `_record_archive_result` writes with `_current_running_bootstrap_run_id()` value; `_resolve_stage_rows` reads with `_run_one_stage`'s `bootstrap_run_id` param. Failover/race could diverge.

**Disposition**: CANNOT FIRE on healthy DB. `sql/129` line 75-76 has partial UNIQUE `bootstrap_runs_one_running_idx ON bootstrap_runs(status) WHERE status='running'`. At most one `running` row. `_current_running_bootstrap_run_id`'s `ORDER BY id DESC LIMIT 1` resolves deterministically. Merged into Candidate 1 (same chokepoint, different angles).

### Candidate 3 — `rows_skipped` semantics on no-op rerun

**Symptom**: `companyfacts.facts_seen=0` on no-op rerun gives `rows_written=0`; strict-gate floor=1 marks cap dead.

**Disposition**: ALREADY FIXED by #1294 (commit `1692252`). Test `tests/test_companyfacts_rows_processed.py` covers the regression. Other 4 bulk ingesters (`sec_submissions`, `sec_13f_ingest_from_dataset`, `sec_insider_ingest_from_dataset`, `sec_nport_ingest_from_dataset`) all `raise RuntimeError` on `total_written == 0` (lines 425, 605, 794 of `sec_bulk_orchestrator_jobs.py`) — so they produce `status='error'`, not `success`+0. Candidate 3 cannot produce `success`+NULL anywhere; produces 0 (companyfacts, now fixed) or error (others).

### Candidate 4 — transaction discipline (`_record_archive_result` shares ingester `with conn.transaction()`)

**Disposition**: STALE against current code. `_record_archive_result` at `sec_bulk_orchestrator_jobs.py:124-133` opens its own `psycopg.connect()` + commits independently. NOT shared with ingester. Survives ingester rollback. Cannot fire.

## NEW CANDIDATE 5 — `_resolve_stage_rows` exceptions silently swallowed (THE REAL BUG CLASS)

**Discovery**: Codex iter-1 review of plan §2.1 spotted the swallow at `bootstrap_orchestrator.py:1503-1518` (pre-fix):

```python
resolved_rows: int | None = None
try:
    with psycopg.connect(database_url) as conn:
        resolved_rows = _resolve_stage_rows(conn, ...)
except Exception as exc:  # noqa: BLE001 — auditing must not fail the stage
    logger.warning("... failed to resolve rows_processed: %s", stage_key, exc)

# resolved_rows still None here ⇒ mark_stage_success writes NULL ⇒ strict-gate fires
mark_stage_success(..., rows_processed=resolved_rows)
```

**Any DB error during resolution** (`SerializationFailure`, `OperationalError` connection blip, `DataError` from prepared-statement hash mismatch, etc.) → `resolved_rows` stays `None` → `bootstrap_stages.rows_processed=NULL` → strict-gate floor=1 marks cap dead → S23/S24 block.

This is the load-bearing bug class. Matches the run_id=3 symptom exactly. Any of the post-#1140 bulk-ingester runs that experienced a transient resolver-side DB error would have produced the observed NULL.

## NEW CANDIDATE 6 — `_resolve_stage_rows` source contract is structurally asymmetric for the 5 bulk jobs

**Discovery**: data-engineer lens review during plan §2.1 audit.

The 3-source precedence in `_resolve_stage_rows` (`bootstrap_orchestrator.py:1253`):
- **Source 1** (`bootstrap_archive_results` non-`__job__` rows SUM): populated by each of 5 bulk ingester `_record_archive_result` calls at `sec_bulk_orchestrator_jobs.py:211/280/386/579/744`. ✓ FUNCTIONAL.
- **Source 2** (`__job__` row > 0): orchestrator writes `__job__` with `rows_written=0` via `record_archive_result_if_absent` at `bootstrap_orchestrator.py:1481-1492`. Always 0 for the 5 bulk jobs. ✗ EFFECTIVELY DEAD.
- **Source 3** (`job_runs.row_count`): the 5 bulk jobs are wrapped via `_adapt_zero_arg` at `app/jobs/runtime.py:371-377`, NOT via `_tracked_job`. No `job_runs` row is written. ✗ STRUCTURALLY DEAD.

**Net**: Source 1 is the only functional source for these 5 jobs. Any code path that fails to populate it OR resolves through an exception → NULL.

## Fix shipped in #1225

Per Phase 0 §2.1 v5.2:

- **Layer A** — `_resolve_stage_rows` invocation retry-once + contained-fail in `_run_one_stage`. On persistent failure (both attempts raise), call `mark_stage_error(..., error_message="rows_processed_resolution_failed after 2 attempts: <exc>")` + `conn.commit()` + return `_StageOutcome(success=False, ...)`. Stage status persists at `error` (not `running` or `success`). Operator sees real error immediately, not days later when downstream cap dies.

- **Layer B** — parametrized regression test at `tests/test_bootstrap_rows_processed_resolution.py` covers 5 bulk stage keys in orchestrated context. Asserts `_resolve_stage_rows` returns SUM when source 1 is populated. Plus a separate Layer A exception-path test that mocks `_resolve_stage_rows` to raise twice and asserts the contained-fail behaviour.

- **Layer C** — `_resolve_stage_rows` docstring updated at `bootstrap_orchestrator.py:1253` to document the asymmetric source contract for the 5 bulk jobs (source 1 is load-bearing; sources 2 and 3 are dead by design).

## Manual-fire path NOT in scope

Manual-fire (`/jobs/<name>/run` outside bootstrap) has `_current_running_bootstrap_run_id()=None`. The bulk wrappers skip `_record_archive_result`. `_resolve_stage_rows` is NOT invoked outside orchestration (no stage row exists). Manual-fire's "NULL" is correct-by-design — there is no `bootstrap_stages` row to populate.

## Cross-impact

- `_resolve_stage_rows` is called at exactly 1 site (`bootstrap_orchestrator.py:1505`). Layer A change does not widen reader contract.
- `record_archive_result_if_absent` callers — unaffected.
- `_record_archive_result` callers in tests — only `tests/test_companyfacts_rows_processed.py` exists pre-fix; new `tests/test_bootstrap_rows_processed_resolution.py` adds 5 parametrized cases + 1 Layer A case.
- Tests asserting `bootstrap_stages.rows_processed IS NULL` under failure mode — verified none exist (grep clean). Layer A's "mark stage error on persistent failure" changes the contract from "silent success with NULL rows" to "loud error with `rows_processed_resolution_failed` prefix message"; no existing test breaks.

## Lessons + prevention candidates

- **Silent-exception-swallow pattern is a NULL-producing bug class**. The `try/except: logger.warning` + use-the-still-None-value pattern at `bootstrap_orchestrator.py:1503-1518` is the canonical shape. Consider a lint pass for similar patterns in audit/observability code paths.
- **Asymmetric source contracts deserve docstring callouts**. Future maintainers reading `_resolve_stage_rows` without the asymmetry note will assume sources 2 + 3 are reasonable fallbacks for ANY job, and will design fixes that "fall through to source 3" — which is dead for these 5 jobs. Layer C docstring update mitigates.

## Forensic disposition

#1225 closed in this PR via structural fix (Layers A/B/C). Audit memo (this doc) records what could and couldn't be proven from current-state evidence. Original 2026-05-17 incident is forensically closed.
