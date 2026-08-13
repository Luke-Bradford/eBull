# Phase 0 — instrumentation + dispatcher kill/confirm gate

**Status**: Proposal · 2026-05-25 · draft 1.5 (Codex-iter-4 fold — sentinel-preflight direction + state-anchored drifted + dry-run-API hallucination; pre-Codex-iter-5)
**Owner**: TBD (operator-named per master plan §5 rule 5)
**Parent plan**: [`bootstrap-sub-1h-plan.md`](./bootstrap-sub-1h-plan.md) (master plan v5.2)
**Target**: precondition for Phases 1-11

**Operator discipline (mandatory per `feedback_no_ticket_count_obsession`)**:
The work is the work. No "audit-only + ticket spawn", no "frontend deferred to ticket #NNNN", no "conditional fix if X". Every issue in front of us = fully fixed in Phase 0. Tested with real numbers. Cross-impact checked. No exceptions.

**Changelog**:
- v1.0 — initial draft
- v1.1 — multi-agent v1 fold (15 BLOCKING + 25 IMPORTANT + 7 hallucinated)
- v1.2 — Codex iter-1 fold (5 BLOCKING + 7 IMPORTANT + 3 hallucinated)
- v1.3 — operator no-ticket-count discipline + Codex iter-2 fold (2 BLOCKING + 3 IMPORTANT + 1 NIT + 1 hallucinated)
- v1.4 — Codex iter-3 fold: NEW-C only. 2 BLOCKING + 2 IMPORTANT + 2 HALLUCINATED in NEW-C
- v1.5 (this) — Codex iter-4 fold: NEW-C only (iter-4 confirmed iter-3 schema fixes solid via NIT-1). 1 BLOCKING + 3 IMPORTANT + 1 HALLUCINATED-API. See §12 v1.4 → v1.5

---

## 0. Why Phase 0 is FIRST

Per master plan §1 meta-issue: every later phase makes perf claims. Perf claims without artifacts = unverifiable. Phase 0 lands the artifact harness + the data-quality + process gate + minimum fixture seeder that Phases 1-11 ride on. It also resolves the only outstanding correctness question — whether dispatcher residual idle is dependency-natural or capacity-bug. Phase 0 closes #1225 fully.

Phase 0 itself ships ZERO perf claims. Therefore Phase 0 PRs need NO `var/perf_baselines/*` artifacts (§4 trigger is "perf claim", not "any change"). Phase 0 BUILDS the harness + seeds the fixture + wires the operator-visible surfaces.

---

## 1. Scope — work items

| ID | Title |
|---|---|
| #1225 | Bulk SEC ingesters `rows_processed=NULL` — audit + FIX |
| #1273 | Long-pole stage instrumentation (`target_count` + `processed_count` writes + cohort-fingerprint with full frontend wiring) |
| #1322 | `test_manifest_source_has_observation_table` + `test_categories_match_ownership_writers` smoke |
| #1256 | Strengthen `check_ownership_refresh_writer_pattern.sh` invariant I to full set-equality |
| #1327 | Raise `DEFAULT_WAIT_FOR_JOBS_SEC` 600 → 1800 |
| NEW-A | `scripts/perf_bench/{run_explain.sh, lint_pr_artifacts.py, floors.yaml}` + `perf-claim-lint` CI job |
| NEW-B | `.claude/skills/engineering/etl-perf-claims.md` skill |
| NEW-C | `scripts/perf_bench/seed_synthetic_fixture/` — scaffold + table-specific seed plan for all 7 floor tables + `ownership_institutions_current` reference implementation + `docs/operator/runbooks/perf-investigation.md` |
| 0.5 | Dispatcher residual-idle telemetry + multi-run measurement (#1275) + IN-SCOPE fix if RESULT_A surfaces a capacity bug |

**Out of Phase 0 scope** (strictly out — not "deferred", but genuinely separate concern):

| Concern | Owner |
|---|---|
| Production-shaped seeder for the 6 floor tables beyond `ownership_institutions_current` | Whichever later phase first needs them (per `feedback_fix_in_scope_default` — handled when in front of us, not pre-emptively built) |
| Per-source documentation §14 + §15 lint | Phase 7 + Phase 8 (per master plan §3) |
| Mirror pre-push lint scripts into ci.yml | Phase 8 (#1329 per master plan §3) |

---

## 2. Per-ticket detail

### 2.1 #1225 — bulk SEC ingesters `rows_processed=NULL`: STRUCTURAL FIX

**Why** (multi-agent + Codex v2 BLOCKING fold): plan v1.5 had a 4-candidate decision tree. 3 lenses converged on it being structurally wrong:

- **Candidate 4 (tx discipline) is STALE**: `_record_archive_result` already opens its own connection + commits at `sec_bulk_orchestrator_jobs.py:124-133`. Confirmed by all 3 reviewers. Drop.
- **Candidate 3 (companyfacts no-op) is PARTIALLY FIXED**: #1294 (commit `1692252`, 2026-05-23) shipped the rows_written semantics fix for `sec_companyfacts_ingest`. Test `tests/test_companyfacts_rows_processed.py` exists.
- **Candidates 1 + 2 collapse onto same chokepoint**: writer/reader divergence on `_current_running_bootstrap_run_id()`. Distinct symptoms (manual-fire vs race) but single fix.
- **run_id=3 evidence is GONE**: dev DB has been mutated by #1233/PR-3/PR-12/#1294/#1218 since 2026-05-17. Cannot dispositively reproduce the original NULL. Audit memo is forensic-only; cannot gate the fix.

**NEW PRIMARY BUG (Codex iter-1 BLOCKING-2 fold)**: `_resolve_stage_rows` exceptions are SWALLOWED at `bootstrap_orchestrator.py:1503-1518`:

```python
try:
    with psycopg.connect(database_url) as conn:
        resolved_rows = _resolve_stage_rows(...)
except Exception as exc:  # noqa: BLE001 — auditing must not fail the stage
    logger.warning("... failed to resolve rows_processed: %s", stage_key, exc)

# resolved_rows still None here ⇒ mark_stage_success writes NULL ⇒ strict-gate fires
mark_stage_success(..., rows_processed=resolved_rows)
```

Any DB error during resolution (connection blip, serialization failure, prepared-statement hash mismatch) → `resolved_rows` stays `None` → `bootstrap_stages.rows_processed=NULL` → strict-gate floor=1 marks cap dead → S23/S24 block. **This matches the run_id=3 symptom exactly.**

**NEW SECONDARY (DE NEW candidate-5)**: `_resolve_stage_rows` source contract is structurally asymmetric for the 5 bulk jobs:
- **Source 1** (`bootstrap_archive_results` non-`__job__` rows SUM): populated by all 5 bulk ingester `_record_archive_result` calls (verified at lines 211/280/386/579/744).
- **Source 2** (`__job__` row > 0): orchestrator writes `__job__` with `rows_written=0` at `record_archive_result_if_absent` (`bootstrap_orchestrator.py:1481-1492`). Always 0 for these 5 jobs; effectively dead.
- **Source 3** (`job_runs.row_count`): the 5 bulk jobs are wrapped via `_adapt_zero_arg` at `app/jobs/runtime.py:371-377`, NOT via `_tracked_job`. No `job_runs` row written. Source 3 always returns nothing.

Net: **Source 1 is the only functional source for these 5 jobs.** Any code path that fails to populate it OR resolves through an exception → NULL.

**STRUCTURAL FIX** (per operator no-ticket-count + work-is-the-work; ship all 3 layers in #1225):

1. **Layer A — retry-once + contained-fail in `_resolve_stage_rows` invocation** (`bootstrap_orchestrator.py:1503-1518`; Codex iter-2 BLOCKING fold — commit to single approach, not author choice):
   - Don't silently swallow. Retry once with WARN log. On persistent failure: convert to a CONTAINED stage-error inside `_run_one_stage` (return `_StageOutcome(success=False, error="rows_processed_resolution_failed after 2 attempts: <exc>")` after persisting `mark_stage_error`); do NOT let the exception escape to `future.result()` (raw escape would tear through `_phase_batched_dispatch`).
   - Reject sentinel approach (Codex iter-2 IMPORTANT-1 fold): `rows_processed=-1` would flow through strict-floor logic as "below floor" anyway; a new `rows_processed_resolution_error` text column widens scope across cap-eval + API.
   - Failing the stage preserves the existing "success means bookkeeping succeeded enough to evaluate caps" semantics. Operator sees real error immediately, not days later when a downstream cap dies.
   - Implementation pattern (concrete; persists status before returning per Codex iter-3 BLOCKING fold):
     ```python
     resolved_rows: int | None = None
     last_resolution_error: str | None = None
     resolution_succeeded = False
     for attempt in range(2):
         try:
             with psycopg.connect(database_url) as conn:
                 resolved_rows = _resolve_stage_rows(conn, ...)
             resolution_succeeded = True
             break  # success — `resolved_rows` may legitimately be None (no side-channel)
         except Exception as exc:  # noqa: BLE001
             last_resolution_error = f"{type(exc).__name__}: {exc}"
             logger.warning("bootstrap stage %s resolve_rows attempt %d: %s", stage_key, attempt + 1, exc)
     if not resolution_succeeded:
         error_msg = f"rows_processed_resolution_failed after 2 attempts: {last_resolution_error}"
         # Persist DB status BEFORE returning — leaves stage at 'error' not 'running'
         with psycopg.connect(database_url) as conn:
             mark_stage_error(conn, run_id=run_id, stage_key=stage_key, error_message=error_msg)
             conn.commit()
         return _StageOutcome(
             stage_key=stage_key, success=False,
             error=error_msg,
             rows_processed=None,
         )
     # ... mark_stage_success path unchanged
     ```

2. **Layer B — pin source 1 via regression test**:
   - Parametrize over 5 bulk job functions (orchestrated context ONLY — Codex iter-2 BLOCKING fold)
   - Manual-fire path (`run_id=None`) is intentionally out of scope: bulk wrappers don't write `bootstrap_archive_results` AND `_resolve_stage_rows` is not invoked outside orchestration. Manual-fire's NULL is correct-by-design.
   - Assert `bootstrap_archive_results` non-`__job__` row is present + populated after each orchestrated invocation
   - Assert `_resolve_stage_rows` returns a non-None int
   - Add a separate test for Layer A: mock `_resolve_stage_rows` to raise twice; assert `_run_one_stage` returns `_StageOutcome(success=False, error="rows_processed_resolution_failed ...")` (NOT a raw escape; NOT a silent NULL on success)

3. **Layer C — close the asymmetric source contract**:
   - Document the asymmetry in `_resolve_stage_rows` docstring (sources 2 + 3 are not reliable for bulk jobs)
   - Add lint check or invariant test: "for the 5 bulk jobs, source 1 is the load-bearing source" — assertion in a smoke test that runs the resolver in a controlled scenario where source 1 is the only source available.

**Cross-impact check** (Codex iter-1 IMPORTANT-2 fold):
- Grep `_resolve_stage_rows` callers (single call site: `bootstrap_orchestrator.py:1505`). Fix doesn't widen reader contract.
- Grep `record_archive_result_if_absent` callers — verify Layer A doesn't break them.
- Grep `_record_archive_result` callers in tests — confirm Layer B regression test doesn't duplicate existing coverage (only `tests/test_companyfacts_rows_processed.py` exists per #1294).
- Grep for assertions that `bootstrap_stages.rows_processed IS NULL` in tests — Layer A's "raise on persistent failure" changes the behavior contract; any test asserting NULL under failure mode breaks.

**Audit memo** at `docs/proposals/etl/audits/1225-rows-processed-null.md` (Codex iter-1 IMPORTANT-3 fold — `docs/proposals/etl/audits/` matches existing path convention; create dir if absent):

- Forensic record only; does NOT gate the fix
- Documents the 3-lens convergence + what #1294 shipped + why run_id=3 cannot be reproduced
- Documents structural source-asymmetry diagnosis
- Cites fix layers A/B/C + commit SHA

**Acceptance**:
- Layer A: `_resolve_stage_rows` exception handling hardened — retry once; on persistent failure, call `mark_stage_error(..., error_message=...)` + `conn.commit()` + return `_StageOutcome(success=False, error=...)` where the error message starts with the prefix `rows_processed_resolution_failed` (impl uses the format `"rows_processed_resolution_failed after 2 attempts: {exc}"` per the concrete pattern; test asserts prefix-match not exact-equality so the exception class detail can vary). Test mocks `_resolve_stage_rows` to raise on both attempts + asserts `_run_one_stage` returns failed outcome AND `bootstrap_stages.status='error'` is persisted (NOT 'running').
- Layer B: parametrized regression test in `tests/services/test_sec_bulk_orchestrator_jobs_rows_processed.py` covers 5 bulk jobs in orchestrated context (5 cases — manual-fire is out of scope per §Layer B; resolver isn't invoked outside orchestration). All green.
- Layer C: asymmetric-source documentation in `_resolve_stage_rows` docstring + invariant test pass.
- Audit memo at `docs/proposals/etl/audits/1225-rows-processed-null.md` committed (forensic only).
- Cross-impact grep output captured in PR.
- Dev-DB real-numbers smoke: trigger 1 representative bulk ingester end-to-end orchestrated; assert `bootstrap_stages.rows_processed > 0`. No manual-fire smoke needed (out of scope per §Layer B).
- #1225 CLOSED.

**Sub-agents**: data-engineer + code-simplifier + edgartools + adversarial reviewer (challenge the structural-source-contract diagnosis)

---

### 2.2 #1273 — long-pole stage instrumentation (with full frontend wiring)

**Why**: Operator runs S22 (~344min), S16 (~85min), S25 (>60min) with no in-flight progress signal. `bootstrap_stages` has `target_count` + `processed_count` per `sql/140` — no migration needed.

**Existing surfaces** (confirmed via grep):
- Endpoint: `GET /processes/{process_id}/timeline` at `app/api/processes.py:648`
- SELECT projects `rows_processed, processed_count, target_count` at `:694`
- Response model `BootstrapTimelineStageResponse` at `:276-301`
- TS type at `frontend/src/api/types.ts:1417-1435`
- Frontend bar at `frontend/src/pages/ProcessDetailPage.tsx:1186-1221`

**Plumbing pattern** (multi-agent BLOCKING-B2 fold): use existing `_current_running_bootstrap_run_id()` at `sec_bulk_orchestrator_jobs.py:90` + NEW `_current_running_stage_key(job_name) -> str | None` helper. NOT JOB_INTERNAL_KEYS injection.

**What**:

1. **NEW helpers** in `app/services/bootstrap_state.py` (Codex iter-1 IMPORTANT-1 fold — no `conn` param):

```python
def set_stage_target(*, run_id: int, stage_key: str, target_count: int,
                     cohort_fingerprint: str | None = None) -> None:
    """Fresh-connection write of target_count + cohort_fingerprint.
    Opens own psycopg connection, commits, closes. Survives caller rollback."""

def set_stage_processed(*, run_id: int, stage_key: str, processed_count: int) -> None:
    """Fresh-connection write of processed_count (ABSOLUTE value, not delta).
    Codex iter-1 NIT-1 fold: was bump_stage_processed; renamed for clarity."""
```

2. **NEW helper** `_current_running_stage_key(job_name: str) -> str | None` — resolves stage_key via `bootstrap_stages WHERE bootstrap_run_id=<current> AND status='running' AND job_name=<job>`. Handles S25's stage_key/job_name divergence: stage_key=`fundamentals_sync`, job_name=`fundamentals_sync_bootstrap` per `bootstrap_orchestrator.py:1166-1168` (Codex iter-2 IMPORTANT-3 fold).

3. **Per-job cohort-shape audit FIRST** (Day 1; multi-agent BLOCKING-B9 fold):

   | Stage | stage_key | job_name | Cohort | Strategy |
   |---|---|---|---|---|
   | S14 | sec_submissions_files_walk | sec_submissions_files_walk | list | count + 30s time |
   | S15 | filings_history_seed | filings_history_seed | list | count + 30s time |
   | S16 | sec_first_install_drain | sec_first_install_drain | streaming cursor | time-based only |
   | S17 | sec_def14a_bootstrap | sec_def14a_bootstrap | list | count + 30s time |
   | S18 | sec_business_summary_bootstrap | sec_business_summary_bootstrap | list | count + 30s time |
   | S22 | sec_13f_recent_sweep | sec_13f_quarterly_sweep | list | count + 30s time |
   | S25 | fundamentals_sync | fundamentals_sync_bootstrap | list | count + 30s time |

   Total: 6 list-shaped + 1 streaming = 7 stages (Codex iter-2 IMPORTANT-3 fold — corrected count from v1.2). Day-1 audit verifies each cohort shape via code-grep before code.

4. **Instrumentation pattern**:
   - On entry: `run_id = _current_running_bootstrap_run_id(); stage_key = _current_running_stage_key(__job_name__); if run_id is None or stage_key is None: skip all progress writes`
   - After cohort materialisation (list-shaped): `set_stage_target(run_id=run_id, stage_key=stage_key, target_count=len(cohort), cohort_fingerprint=<computed>)`
   - **Hybrid count+time cadence**: emit `set_stage_processed(..., processed_count=i)` every `max(1, len(cohort)//100)` iterations OR every 30s wall-clock, whichever first. Stage tracks both counters; cooperative emission.
   - Streaming path (S16): no target write; emit `set_stage_processed(..., processed_count=running_count)` every 30s wall-clock; on exit, final write.
   - On exit (success): final `set_stage_processed(..., processed_count=final_count)`.

5. **Cohort-definition fingerprint** — full operator-visible wiring (Codex iter-2 BLOCKING-3 fold; operator discipline: no frontend deferral):

   **DB layer**: new sql/NNN migration: `ALTER TABLE bootstrap_stages ADD COLUMN IF NOT EXISTS target_cohort_fingerprint TEXT`.

   **Backend layer**: extend SELECT at `app/api/processes.py:694` to project `target_cohort_fingerprint`; add field to `BootstrapTimelineStageResponse` at `:276-301`.

   **Frontend type layer**: add `target_cohort_fingerprint: string | null` to `BootstrapTimelineStageResponse` at `frontend/src/api/types.ts:1417-1435`.

   **Frontend rendering layer**: extend `ProcessDetailPage.tsx:1186-1221` progress-bar block to include cohort-fingerprint as a `title=` tooltip on the bar element (operator hovers, sees "cutoff=2025-05-15;universe_overlap=False" or similar). NO new visual chrome — additive tooltip only.

   **Cohort fingerprint format**: `<key>=<value>;<key>=<value>;...` (semicolon-separated). For S22: `cutoff=<YYYY-MM-DD>;universe_overlap=<bool>;` etc. Per-stage fingerprint shape documented in cohort-shape audit memo (step 3).

   **Cross-impact check**: grep for `BootstrapTimelineStageResponse` usage — verify no other consumers break (e.g. test fixtures, type-checked stubs).

6. **Cross-impact check on instrumentation writes**: grep for callers of `set_stage_progress` (none expected; the helper is NEW). Grep for callers of new helpers' SQL columns — confirm no other writer pattern conflicts.

7. **Metrics-analyst skill update** SAME-PR per CLAUDE.md skill-ownership inline rule:
   - `.claude/skills/metrics-analyst/SKILL.md` master index: split `bootstrap_stages` row into `rows_processed` (→ Overview), `target_count + processed_count` (→ Timeline bar), `target_cohort_fingerprint` (→ Timeline tooltip).
   - §8 new sub-section "Bootstrap stage progress (live)" — full source→sink template fill.
   - Cross-reference Phase 0 doc.

**Acceptance**:
- All 7 stages: list-shaped (6) write `target_count IS NOT NULL` + `target_cohort_fingerprint IS NOT NULL`; streaming (S16) writes `processed_count` advancing.
- **Real-numbers smoke** on dev DB: start a fresh bootstrap, wait until each instrumented stage runs, screenshot Timeline (showing bar + tooltip) + capture SQL `SELECT stage_key, target_count, processed_count, target_cohort_fingerprint FROM bootstrap_stages WHERE bootstrap_run_id=<id>`. Output in PR description.
- **Unit test** (`tests/smoke/test_long_pole_progress.py`): per stage, 10-row synthetic cohort + assert `target_count == 10` (where applicable) + `processed_count` monotonic + survives mid-stage ingester ROLLBACK (proves fresh-connection discipline).
- **No regression**: stages NOT in the 7-pole list continue to write `processed_count=0 + target_count=NULL` (chart renders identically to pre-PR).
- **Cross-impact**: BootstrapTimelineStageResponse consumers in tests / fixtures audited; metrics-analyst skill updated.

**Sub-agents**: data-engineer + code-simplifier + metrics-analyst + frontend-design (cohort-fingerprint tooltip rendering) + adversarial reviewer

---

### 2.3 #1322 — manifest-source-has-sink-tables + categories-match-writers smoke

**Why**: `tests/smoke/test_etl_source_to_sink.py` covers parser/cadence/form/spec/sections but NOT sink-table existence. Drop a `_current` table from a migration → smoke passes, prod fails.

**Source-of-truth approach** (revised per multi-agent + Codex v2 BLOCKING fold): the (source, target_table) mapping is **NOT derivable from production registries** — `ManifestSource` has 15 entries but 4 are pure synth-noop (sec_10q, sec_xbrl_facts, finra_short_interest, finra_regsho_daily), sec_n_csr is non-ownership/fund-metadata (writes `fund_metadata_observations` + `fund_metadata_current`), and sec_def14a fans out to 2 ownership categories (def14a + esop). Plan v2 declares the mapping EXPLICITLY in `scripts/_etl_source_inventory.py`. Real registries (grep-verified — Codex iter-1 IMPORTANT-1 fold corrects paths):
- `ManifestSource` Literal at `app/services/sec_manifest.py:106` (15 entries)
- `_CATEGORIES` 4-tuple at `app/jobs/ownership_observations_repair.py:80` (7 entries)
- Parser registry at `app/jobs/sec_manifest_worker.py` (NOT `app/services/`)
- `refresh_*_current` callable surface in `app/services/ownership_observations.py`

**What**:

1. **NEW const** `MANIFEST_SOURCE_SINKS: dict[str, tuple[tuple[str, ...], str]]` in `scripts/_etl_source_inventory.py` (multi-agent BLOCKING-B1 + CS SIMPLIFY-F2 fold; explicit declaration):

   ```python
   # source → (target_tables, kind)
   # kind ∈ {ownership_observation, fund_metadata, business_summary, eight_k, synth_noop}
   MANIFEST_SOURCE_SINKS: dict[str, tuple[tuple[str, ...], str]] = {
       "sec_form3":            (("ownership_insiders_observations", "ownership_insiders_current"), "ownership_observation"),
       "sec_form4":            (("ownership_insiders_observations", "ownership_insiders_current"), "ownership_observation"),
       "sec_form5":            (("ownership_insiders_observations", "ownership_insiders_current"), "ownership_observation"),
       "sec_13d":              (("ownership_blockholders_observations", "ownership_blockholders_current"), "ownership_observation"),
       "sec_13g":              (("ownership_blockholders_observations", "ownership_blockholders_current"), "ownership_observation"),
       "sec_13f_hr":           (("ownership_institutions_observations", "ownership_institutions_current"), "ownership_observation"),
       "sec_def14a":           (("ownership_def14a_observations", "ownership_def14a_current",
                                 "ownership_esop_observations", "ownership_esop_current"), "ownership_observation"),  # fan-out
       "sec_n_port":           (("ownership_funds_observations", "ownership_funds_current"), "ownership_observation"),
       "sec_n_csr":            (("fund_metadata_observations", "fund_metadata_current"), "fund_metadata"),
       "sec_10k":              (("instrument_business_summary", "instrument_business_summary_sections"), "business_summary"),
       "sec_10q":              ((), "synth_noop"),
       "sec_8k":               (("eight_k_filings", "eight_k_items", "eight_k_exhibits"), "eight_k"),
       "sec_xbrl_facts":       ((), "synth_noop"),
       "finra_short_interest": ((), "synth_noop"),
       "finra_regsho_daily":   ((), "synth_noop"),
   }
   ```

   Each cited table name verified against `sql/*.sql` at implementation time.

2. **NEW closure test** `test_manifest_source_sinks_complete` (Codex iter-2 IMPORTANT-1 fold; guards against future 16th `ManifestSource` landing without sink declaration):
   - Assert `set(MANIFEST_SOURCE_SINKS) == set(get_args(ManifestSource))`. Any new entry in the Literal that doesn't get a sink declaration fails this test loudly.

3. **NEW test** `test_manifest_source_has_sink_tables` in `tests/smoke/test_etl_source_to_sink.py`. Parametrize over `MANIFEST_SOURCE_SINKS.items()`. Per-row assertion:
   - For each table in `target_tables`: existence via `SELECT 1 FROM pg_tables WHERE schemaname='public' AND tablename=%s` (multi-agent BLOCKING-B3 fold — `pg_tables` check avoids `UndefinedTable` aborted-tx hazard of `SELECT 1 FROM <table>`)
   - **Parity** (Codex iter-3 IMPORTANT fold): for EVERY source whose parser module exists, assert `getattr(parser_module, '_SYNTH_NOOP', False) is (kind == 'synth_noop')`. This is bidirectional — catches both "synth-noop kind without flag" AND "flag stuck True on a parser that grew into a real writer". Requires NEW module-level constant `_SYNTH_NOOP: Final[bool] = True` in each of the 4 synth-noop parser modules (`sec_10q.py`, `sec_xbrl_facts.py`, `finra_short_interest.py`, `finra_regsho_daily.py`); real-parser modules either don't declare it OR declare `_SYNTH_NOOP: Final[bool] = False`. Test fails loudly if any module diverges from its `kind` classification.

4. **NEW test** `test_categories_match_ownership_writers` (multi-agent B2 + Codex iter-1 BLOCKING-2 fold — use `_CATEGORIES` tuple fields directly, no string templating). For each of 7 `_CATEGORIES` 4-tuple `(current_table, observations_table, category_literal, refresh_fn)`:
   - Assert `callable(refresh_fn)` (the lambda)
   - Assert `current_table` exists via `pg_tables` check
   - Assert `observations_table` exists via `pg_tables` check
   - Assert `refresh_<category>_current` importable from `app.services.ownership_observations` namespace (catches refactor renaming refresh fn but leaving lambda intact)

5. **Empirical negative test** in NEW file `tests/smoke/test_etl_source_to_sink_negative.py` (separate file isolates DDL-in-test failure mode per CS-F1):
   - Target: `ownership_funds_current` (no inbound FKs per `sql/123`; in `_PLANNER_TABLES` — multi-agent IMPORTANT-I1 fold)
   - Pattern: per-test fresh conn + explicit `try/finally` with `conn.rollback()` in `finally` (NOT bare `with conn.transaction():` per CS-F1 + prevention-log line 637 — psycopg v3 conn.transaction() is savepoint)
   - Steps: BEGIN → SAVEPOINT s1 → `DROP TABLE ownership_funds_current` → invoke positive-test assertion → assert it raises → `ROLLBACK TO SAVEPOINT s1` → `ROLLBACK`
   - Post-rollback: assert `pg_tables` shows `ownership_funds_current` exists (multi-agent IMPORTANT-I5; proves rollback worked, leaves DB clean)
   - `pytestmark = pytest.mark.xdist_group(name="etl_source_to_sink_negative")` (serializes within xdist worker group, no cross-test pollution)

6. **Cross-impact** (multi-agent IMPORTANT-I4):
   - `tests/test_capability_manifest_mapping.py`: closure test against `ManifestSource`; new tests parametrize over same Literal; both read-only on `_PARSERS` registry — no race
   - `tests/test_ownership_refresh_writer_merge.py`: uses `ebull_test_conn`; negative test uses ITS OWN fresh conn + xdist serialization — no race
   - Grep `_PARSERS.clear()` callers in `tests/` → if any, document new tests do NOT call clear
   - PR description: xdist run output showing all parametrized cases pass + negative test passes + post-rollback existence verified

7. **Cross-reference** (NIT-N4): cite `app/services/capability_manifest_mapping.py:179` `CATEGORY_TO_MANIFEST_SOURCES` as existing REVERSE-direction mapping (category → manifest sources); `MANIFEST_SOURCE_SINKS` is forward direction.

**Acceptance**:
- `uv run pytest tests/smoke/test_etl_source_to_sink.py -k "test_manifest_source_sinks_complete or test_manifest_source_has_sink_tables or test_categories_match_ownership_writers"` green
- `uv run pytest tests/smoke/test_etl_source_to_sink_negative.py` green (DROP + assertion-fires + ROLLBACK + post-rollback exists)
- All 15 `MANIFEST_SOURCE_SINKS` entries pass per-kind assertion; closure test confirms set-equality with `get_args(ManifestSource)`
- All 4 synth-noop parser modules declare `_SYNTH_NOOP: Final[bool] = True`; parity assertion holds for ALL sources (synth ↔ flag True; real ↔ flag absent/False)
- Cross-impact: no race with sibling test files; captured in PR
- Runtime <2s for all 4 tests combined
- PR description includes pre-rollback FAIL output + post-rollback PASS output as empirical proof

**Sub-agents**: data-engineer + code-simplifier + adversarial reviewer (challenge that 15-entry mapping is complete + correct — what if a 16th source lands tomorrow?)

---

### 2.4 #1256 — strengthen `check_ownership_refresh_writer_pattern.sh` invariant I

**Why**: Current invariant I (`scripts/check_ownership_refresh_writer_pattern.sh:431-448`) pins `refreshed_at = now()` once. Spec §5 (archived at `docs/_archive/2026-05/2026-05-21-pr12-ownership-current-writer-merge.md`) requires:
- **I.a (set-equality)**: `UPDATE SET cols == diff-tuple cols ∪ {refreshed_at}`
- **I.b (LHS-RHS ordered equality)**: `IS DISTINCT FROM` LHS tuple cols ≡ RHS tuple cols (exact ordered names, modulo `tgt.`/`src.` prefix)
- **I.c (scope)**: enforced on BOTH 7 single-instrument helpers (`refresh_<cat>_current`) AND 3 batch helpers (`refresh_insiders_current_batch`, `refresh_institutions_current_batch`, `refresh_funds_current_batch`) — batch helpers are hot path for bulk ingest; same drift risk; data-engineer review BLOCKING-B2 fold

**Approach** (revised per multi-agent BLOCKING + code-simplifier SIMPLIFY fold): Python helper script mirroring existing `check_caller_owned_tx.{py,sh}` precedent. Awk cannot cleanly tokenize comma-separated multi-col-per-line tuple spans (data-engineer + Codex BLOCKING-B1 fold: real helper SQL at `app/services/ownership_observations.py:470-479` puts multiple diff cols on each line). Python with `re` (no `sqlparse` dependency per CLAUDE.md "do not add libraries casually") handles tokenization in ~30-40 LOC vs awk's ~60-80 fragile LOC.

**What**:

1. **NEW** `scripts/_check_ownership_writer_columns.py` (~120 LOC; revised per Codex iter-2 BLOCKING fold):
   - **CLI** (Codex iter-3 BLOCKING-1 fold — exact function name disambiguation):
     - `uv run python scripts/_check_ownership_writer_columns.py --function <exact_function_name> <source-file>` (e.g. `--function refresh_insiders_current_batch`)
     - OR `--function <exact_function_name> --source-text <text>` (for tests; Codex iter-2 IMPORTANT-1 fold)
     - Diagnostic output names the exact function checked + invariant axes evaluated
     - Coverage report mode `--coverage-report <source-file>` lists every function in scope (single + batch) that the lint exercised, so shell wrapper can audit "10 functions covered" not "10 invocations" (defends against double-checking)
   - **Shape-gate** (prefix-asymmetric; Codex iter-2 BLOCKING-1 fold):
     - **Diff-tuple LHS** lines match `^\s*tgt\.\w+(\s*,\s*tgt\.\w+)*\s*,?\s*$` (ALL `tgt.` prefix, no `src.`)
     - **Diff-tuple RHS** lines match `^\s*src\.\w+(\s*,\s*src\.\w+)*\s*,?\s*$` (ALL `src.` prefix, no `tgt.`)
     - **UPDATE SET** lines match `^\s*(\w+)\s*=\s*src\.(\w+)\s*,?\s*$` OR terminal `^\s*refreshed_at\s*=\s*now\(\)\s*,?\s*$`
     - Any non-matching line → fail with actionable diagnostic (line number + offending text)
   - **Span extraction**:
     - LHS span: between `WHEN MATCHED AND (` and `) IS DISTINCT FROM (`
     - RHS span: between `) IS DISTINCT FROM (` and `) THEN UPDATE SET`
     - UPDATE SET span: between `THEN UPDATE SET` and `WHEN NOT MATCHED BY TARGET`
   - **Tokenization**:
     - Diff tuples: split on commas + trim + strip `tgt.` / `src.` → ordered list
     - UPDATE SET: parse assignment pairs `(lhs_col, rhs_col)` where rhs has `src.` stripped (Codex iter-2 BLOCKING-3 fold; not just LHS-only)
   - **Assertions** (revised 5 axes, not 3; Codex iter-2 BLOCKING-1/2/3 fold):
     - **I.a (set-equality)**: `set(UPDATE_SET_lhs_cols) - {'refreshed_at'} == set(DIFF_LHS_cols)`
     - **I.b (LHS-RHS ordered equality)**: `DIFF_LHS_cols == DIFF_RHS_cols` (list equality preserves order)
     - **I.c (refreshed_at exactly-once placement)**: `UPDATE_SET_lhs_cols.count('refreshed_at') == 1` AND `'refreshed_at' not in DIFF_LHS_cols` AND `'refreshed_at' not in DIFF_RHS_cols`
     - **I.d (uniqueness)**: `len(UPDATE_SET_lhs_cols) == len(set(UPDATE_SET_lhs_cols))` AND same for DIFF_LHS_cols AND DIFF_RHS_cols (Codex iter-2 BLOCKING-2 fold; defends against duplicate cols)
     - **I.e (UPDATE assignment LHS==RHS)**: for each non-refreshed_at UPDATE SET pair `(lhs, rhs)`: `lhs == rhs` (Codex iter-2 BLOCKING-3 fold; catches `foo = src.bar` typo)
   - **Exit codes**: 0 on pass; 2 on any assertion failure with diagnostic listing concrete delta

2. **`scripts/check_ownership_refresh_writer_pattern.sh`** edit:
   - Replace existing invariant I awk block (lines 431-448) with per-function invocation: `uv run python scripts/_check_ownership_writer_columns.py --function "refresh_${helper}_current" "$FILE_OBS" || fail "I helper=${helper}: see python output above"`
   - Add 3 additional invocations for batch helpers (exact function names): `refresh_insiders_current_batch`, `refresh_institutions_current_batch`, `refresh_funds_current_batch`
   - **Coverage audit** at end of script: `uv run python scripts/_check_ownership_writer_columns.py --coverage-report "$FILE_OBS" | grep -E '10 functions covered' || fail "coverage drift: expected 10 functions; see python output"`. Defends against silent double-check (Codex iter-3 BLOCKING-1 fold).
   - Net shell-script delta: ~ -18 LOC + ~15 LOC = -3 LOC

3. **NEW** `tests/scripts/test_check_ownership_writer_columns.py` — pytest with fixture strings (multi-agent I2 fold; lint accepts a `--source-text <text>` arg so tests don't need to write fixture files):
   - **Happy path (10 cases)**: each of 7 single-instrument + 3 batch helpers passes
   - **Negative tests** (multi-agent I1 fold; 7 axes, not 3):
     - drop-column-from-update-set
     - wrong-prefix-in-diff-tuple (`tgt.x` on RHS where `src.x` belongs)
     - LHS-RHS-name-mismatch (same column count, different col names)
     - duplicated-col-in-update-set
     - case-sensitivity-mismatch (`Refreshed_At` vs `refreshed_at`)
     - shape-violation (UPDATE SET line with inline comment / multi-col / expression)
     - trailing-comma-edge-case (last col missing comma where parser might lose it)
   - Each negative test asserts exit-code 2 + diagnostic text mentions the failing col

4. **Cross-impact** (data-engineer + Codex):
   - `tests/test_ownership_refresh_writer_merge.py` — unaffected at runtime (lint is text-only against source files; doesn't touch the DB / fixture xmin snapshots)
   - Cross-impact assertion in PR: run full `bash scripts/check_ownership_refresh_writer_pattern.sh` after change; assert exit 0 + all 10 helpers covered
   - Archived spec reference: header comment in Python helper points to `docs/_archive/2026-05/2026-05-21-pr12-ownership-current-writer-merge.md` §5 (spec is frozen; no risk of moving)

5. **Skill doc update** (`.claude/skills/data-engineer/SKILL.md` ownership writer section):
   - 2-line pointer: "UPDATE SET / diff-tuple format is enforced by `scripts/_check_ownership_writer_columns.py`. One col per line in UPDATE SET; comma-separated prefix.col tokens per diff-tuple line; no inline comments in either span."
   - Per multi-agent N1 fold: NO load-bearing rules in skill — lint self-enforces

**Acceptance**:
- `bash scripts/check_ownership_refresh_writer_pattern.sh` exits 0 on current main (10 helpers pass: 7 single + 3 batch)
- 7 negative tests in `tests/scripts/test_check_ownership_writer_columns.py` all fire as expected (each asserts exit-code 2 + correct diagnostic)
- Python helper runtime budget: <5s for all 10 helpers
- Skill doc updated with 2-line pointer (no social-contract rules)
- Cross-impact: `uv run pytest tests/test_ownership_refresh_writer_merge.py` green (unaffected; runtime tests don't depend on lint)

**Sub-agents**: data-engineer + code-simplifier + adversarial reviewer (challenge that 7 negative tests cover the regression class spec §5 actually wants)

---

### 2.5 #1327 — raise `DEFAULT_WAIT_FOR_JOBS_SEC` 600 → 1800

**Why** (data-engineer + code-simplifier review v1 fold — see §12 v1.5 per-PR addendum): original plan claimed "defense-in-depth dual edit" but grep proves `safety.py:175` default is unreachable (all 3 callers pass explicit `timeout_sec=`: `stream_a_run_8_verify.py:540`, `tests/test_runbook_safety.py:143/154`). v1.6 cleaner: bump constant + make signature parameter REQUIRED (remove default). `DEFAULT_WAIT_FOR_JOBS_SEC` constant becomes the single source of truth.

**What**:
1. `app/runbooks/stream_a_run_8_verify.py:90`: `DEFAULT_WAIT_FOR_JOBS_SEC: int = 600` → `1800`
2. `app/runbooks/safety.py:175` `timeout_sec: int = 600` → `timeout_sec: int` (REQUIRED kwarg, no default — code-simplifier SIMPLIFY fold; future callers must make explicit budget decision)
3. Doc updates (both doc-engineer + Codex flagged: TWO lines in `run-8-readiness.md`):
   - `docs/operator/runbooks/run-8-readiness.md:189` §3.1 parameter table (operator-visible default value)
   - `docs/operator/runbooks/run-8-readiness.md:496` OP-O2 entry
4. Test updates:
   - `tests/test_runbook_safety.py`: add `inspect.signature` test asserting `wait_for_jobs_process_started`'s `timeout_sec` parameter is REQUIRED (no default) — Codex+CS NIT fold; cleaner than slow timeout-path test
   - `tests/test_stream_a_runbooks_cli.py:111+` (CLI default coverage): add assertion that argparse default equals `1800` (the constant) — per data-engineer IMPORTANT fold; argparse consumes `DEFAULT_WAIT_FOR_JOBS_SEC`
5. **Cross-impact check**: grep for `600` literals in `app/runbooks/` + `tests/` tied to `wait_for_jobs` / `JOBS_PROCESS_LOCK` — capture in PR.

**Acceptance**:
- `grep -nE 'DEFAULT_WAIT_FOR_JOBS_SEC[[:space:]]*[:=]' app/` → only `1800`
- `grep -nE 'timeout_sec[[:space:]]*[:=][[:space:]]*600' app/runbooks/` → empty
- `grep -nE '\b600\b' app/runbooks/ docs/operator/runbooks/run-8-readiness.md` → no matches against wait-for-jobs context
- `inspect.signature` test passes (parameter REQUIRED)
- CLI default test passes (argparse default = 1800)
- Existing safety tests still pass (explicit `timeout_sec=60` / `=2` unchanged)
- `docs/proposals/etl/stream-a-run-8-fixes.md:670` (frozen historical spec) NOT edited — documents state at original ship; PR description notes the historical drift

**Sub-agents**: code-simplifier + data-engineer

---

### 2.6 NEW-A — `scripts/perf_bench/` harness + `perf-claim-lint` CI job

**Why**: §4 mandates immutable evidence bundle. Harness collapses authoring friction; lint enforces.

**Single source of truth**: `scripts/perf_bench/floors.yaml` (consumed by harness + lint + skill + NEW-C seeder):

```yaml
ownership_institutions_current: 1000000
ownership_institutions_observations: 2000000
ownership_insiders_observations: 500000
ownership_funds_observations: 200000
financial_facts_raw: 10000000
sec_filing_manifest: 1000000
filing_events: 2000000
```

#### NEW-A.1 — `scripts/perf_bench/run_explain.sh`

Usage (matches master plan §4): `scripts/perf_bench/run_explain.sh <ticket_id>`.

Per-ticket config at `scripts/perf_bench/<ticket_id>.yaml`:
```yaml
sql_file: path/to/perf_query.sql      # required
fixture_label: bench-dev              # required
target_table: ownership_institutions_current   # required if perf claim touches a floored table; null otherwise
```

Harness reads config, executes EXPLAIN, produces 3 artifacts under `var/perf_baselines/<ticket>-<sha>.{txt,json,manifest.yaml}`.

Refusals:
- `EBULL_BENCH_DB_URL` env var unset
- `<ticket_id>.yaml` config missing
- `--check-floors-only`: floor-check fails (citing table + floor + measured)
- Uncommitted target SQL file

#### NEW-A.2 — `scripts/perf_bench/lint_pr_artifacts.py` (~150 LOC)

- Perf claim detected: PR labeled `perf` OR description has `## Performance impact` header.
- Resolve ticket from PR body `Closes #(\d+)`; resolve sha from `os.environ['GITHUB_PR_HEAD_SHA']`.
- Validate 3 artifacts + 3 PR-description sections.
- **Gated bypass** (Codex iter-1 IMPORTANT-6 fold): `PERF_CLAIM_LINT_BYPASS=true` alone NOT sufficient. Requires ALL of: PR labeled `emergency` + PR body `## Bypass justification` section with operator name + reason + env var.

#### NEW-A.3 — `.github/workflows/ci.yml`

```yaml
perf-claim-lint:
  if: github.event_name == 'pull_request'
  runs-on: ubuntu-latest
  steps:
    - uses: actions/checkout@v4
      with:
        ref: ${{ github.event.pull_request.head.sha }}
    - uses: actions/setup-python@v5
      with: { python-version: '3.13' }
    - run: pip install pyyaml
    - run: python scripts/perf_bench/lint_pr_artifacts.py
      env:
        GITHUB_PR_BODY: ${{ github.event.pull_request.body }}
        GITHUB_PR_LABELS: ${{ toJson(github.event.pull_request.labels.*.name) }}
        GITHUB_PR_HEAD_SHA: ${{ github.event.pull_request.head.sha }}
        PERF_CLAIM_LINT_BYPASS: ${{ vars.PERF_CLAIM_LINT_BYPASS || 'false' }}
```

Branch protection update making `perf-claim-lint` required on `main` operator-executed in Phase 0 close. Captured in §6.

#### NEW-A.4 — Self-test

`tests/scripts/test_perf_bench_lint.py`: scenarios:
- clean-no-claim → exit 0
- perf-labeled-missing-artifact → exit non-zero
- perf-labeled-floor-fail → exit non-zero
- bypass-no-label → exit non-zero (bypass var alone insufficient)
- bypass-no-justification → exit non-zero
- bypass-fully-gated → exit 0 with WARN

#### NEW-A.5 — Cross-impact

- Grep `.github/workflows/*.yml` for existing checkout-step patterns; verify `head.sha` checkout is consistent with siblings.
- Grep CI for jobs depending on the merge-commit checkout; verify perf-claim-lint's head-sha doesn't trip them.
- Branch-protection update procedure documented in PR description.

**Acceptance**: harness produces 3 valid artifacts on bench DB; lint exits 0 for Phase-0 PRs; 6 self-test scenarios green; workflow green on Phase 0 PR; bypass-gating tested; cross-impact captured.

**Sub-agents**: data-engineer + code-simplifier + metrics-analyst

---

### 2.7 NEW-B — `.claude/skills/engineering/etl-perf-claims.md`

≤80 lines. Structure unchanged from v1.1.

**Behaviour-enforcement hooks**:
1. PR-template update at `.github/pull_request_template.md` (lowercase per repo): add comment block flagging perf PRs.
2. Cross-link in `.claude/skills/engineering/pre-flight-review.md` + `pre-pr-fresh-agent-review.md`.
3. Runtime enforcement = `perf-claim-lint`.
4. **Cross-impact**: master plan §5 references this skill back (newly added cross-link in master plan).

**Acceptance**: skill ≤80 lines; YAML frontmatter validates; PR template updated; 2 cross-links added; master plan §5 cross-linked back to NEW-B; discoverable via Skill tool.

**Sub-agents**: data-engineer + code-simplifier

---

### 2.8 NEW-C — `scripts/perf_bench/seed_synthetic_fixture/` — scaffold + 1 table

**Why** (operator-chosen v1.3 scope, Codex iter-2/iter-3 BLOCKING fold): Phase 0 done-state requires `perf-claim-lint` workflow green end-to-end; harness refuses to run if §4 floors unmet; floor tables have varying FK + partition + writer-safety profiles. Per operator discipline: design covers all 7 floor tables with grep-verified real schemas; implementation lands the ONE table Phase 1 (#1346) actually needs.

**Writer-safety strategy (Codex iter-3 BLOCKING-1 + iter-4 fold)** — the writer at `app/services/ownership_observations.py:505` does `WHEN NOT MATCHED BY SOURCE AND tgt.instrument_id = %(iid)s THEN DELETE` (wipes ALL `_current` rows for instrument_id when refresh fires). Filer-CIK offset does NOT protect synthetic rows. Real safety:

- **Sentinel instrument_ids in range `>= 1_000_000_000`** — outside any real `instruments.instrument_id` value (verified at impl time).
- **Sentinel preflight assertion** (Codex iter-4 BLOCKING-1 fold — direction was inverted in v1.4): seeder asserts `SELECT MAX(instrument_id) FROM instruments < 1_000_000_000` AND every emitted sentinel ID `> MAX(real_instrument_id)`. Refuses to run if real-max ever crosses 1e9 (future-proof tripwire).
- **Refresh-job iteration is STATE-anchored, not observations-anchored** (Codex iter-4 IMPORTANT-1 fold; verified via `app/jobs/ownership_observations_repair.py:152`): the `_drifted_instruments` query selects from `ownership_refresh_state s LEFT JOIN obs_max ON ... WHERE s.last_drained_observations_max_ingested_at IS DISTINCT FROM obs_max.m`. Sentinel rows are safe IFF (a) no sentinel row in `ownership_<cat>_observations` (so `obs_max` for sentinel ID stays NULL/absent), AND (b) no sentinel row in `ownership_refresh_state` (so the LEFT JOIN never starts from a sentinel anchor).
- **Direct `_current` seed protocol**: seeder writes ONLY to `ownership_institutions_current`. Does NOT write to `ownership_institutions_observations`. Does NOT write to `ownership_refresh_state`. Both must remain sentinel-free.
- **Backfill hazard** (Codex iter-4 IMPORTANT-1 fold): if `sql/163_ownership_refresh_state.sql` is re-applied post-seed, its backfill block walks `_current` and could insert a sentinel state row. Seeder PR ships a post-backfill assertion + docstring on `sql/163` warning bench-DB operators to re-run sentinel-cleanup if migration re-applied.
- **`_validate_no_refresh_leak()` implementation** (Codex iter-4 HALLUCINATED-API-1 fold; no `dry_run` flag in `run_observations_repair_sweep`): test calls `_drifted_instruments(conn, current_table, observations_table, category_literal)` directly (it's importable per `tests/test_ownership_refresh_writer_merge.py:627`) + asserts every returned ID is `< 1_000_000_000`. Test refuses to call `refresh_institutions_current` on any sentinel ID. If `_drifted_instruments` returns any sentinel, test fails loudly.

**Architecture**:

`scripts/perf_bench/seed_synthetic_fixture/` directory:
- `__init__.py` — common helpers (sentinel-range allocation, partition routing, validation queries)
- `seed_<table>.py` — one module per floor table (1 implemented; 6 stubs)
- `docs/operator/runbooks/perf-investigation.md` — caveat + per-table plan + usage

**The 7 plans (grep-verified real schemas — Codex iter-3 BLOCKING-2 + IMPORTANT-1/2 + HALLUCINATED-API fold)**:

| Table | Real PK | Real FK | Real partitioning | Synthetic strategy |
|---|---|---|---|---|
| `ownership_institutions_current` | `(instrument_id, filer_cik, ownership_nature, exposure_kind)` per `sql/114:134` | NONE (no FK to instruments) | NONE | Direct INSERT into `_current` with sentinel instrument_id (≥ 1e9). Bypass `_observations`. No refresh ever fires for sentinel IDs (writer-safety strategy above). Required NOT NULL fields: filer_name, source ('derived'), source_document_id, filed_at, period_end. CHECK-constrained: filer_type ∈ {ETF,INV,INS,BD,OTHER}, ownership_nature ∈ {direct,indirect,beneficial,voting,economic}, exposure_kind ∈ {EQUITY,PUT,CALL}. **IMPLEMENTED in Phase 0**. |
| `ownership_institutions_observations` | `(instrument_id, filer_cik, ownership_nature, period_end, source_document_id, exposure_kind)` per `sql/114:71` | NONE | PARTITIONED BY RANGE(period_end), quarterly 2010-Q1..2030-Q4 + DEFAULT per `sql/114:80-105` | STUB. Plan: direct INSERT routed to existing quarterly partitions; sentinel instrument_id; spread period_end across last 8 quarters. NOT IMPLEMENTED in Phase 0. |
| `ownership_insiders_observations` | `(instrument_id, holder_identity_key, ownership_nature, source, source_document_id, period_end)` per `sql/113:77` (Codex iter-4 IMPORTANT-3 fold — grep-verified) | NONE | PARTITIONED BY RANGE(period_end), quarterly 2010-2030 + DEFAULT | STUB. Plan: direct INSERT routed to existing quarterly partitions; sentinel instrument_id; synthesize `holder_cik` / `holder_name` + source ∈ allowed set + spread period_end across last 8 quarters. DB generates `holder_identity_key` via GENERATED ALWAYS column per sql/113 — seeder does NOT write this column. NOT IMPLEMENTED in Phase 0. |
| `ownership_funds_observations` | `(instrument_id, fund_series_id, period_end, source_document_id)` per `sql/123:89` (Codex iter-4 IMPORTANT-3 fold — grep-verified) | NONE | PARTITIONED BY RANGE(period_end), N-PORT-era partitions (2018-2030 dominant) | STUB. Plan: direct INSERT routed to existing partitions; sentinel instrument_id; synthetic `fund_series_id` TEXT; payoff_profile='Long' + asset_category='EC' (CHECK-pinned). NOT IMPLEMENTED in Phase 0. |
| `financial_facts_raw` | `(fact_id, period_end)` per `sql/156:22`; fact_id BIGSERIAL DEFAULT nextval | FK to `instruments(instrument_id)` + `data_ingestion_runs(ingestion_run_id)` | PARTITIONED BY RANGE(period_end), quarterly 2010-2030 + pre2010 + DEFAULT per `sql/156:24-` | STUB. Synthetic sentinel instrument_ids DO NOT WORK here because FK to instruments fires. Plan: real-instrument-id replication × N copies with different period_end (across existing partitions); `data_ingestion_runs` must exist or be seeded first; let DB assign fact_id. NOT IMPLEMENTED in Phase 0; Phase 4 (S22 work) likely first to need it. |
| `sec_filing_manifest` | `accession_number` (TEXT PK) per `sql/118:30` | FK to `instruments(instrument_id)` ON DELETE CASCADE (issuer-scoped rows); self-FK `amends_accession` ON DELETE SET NULL | NONE | STUB. Plan: synthetic `accession_number` = `'SYN-' || generate_series(...)`; for issuer-scoped rows MUST use real instrument_id (FK fires); for institutional-filer-scoped rows instrument_id is NULL; preserve self-FK as NULL on synthetic rows; respect CHECK constraints (source ∈ allowlist, subject_type ∈ allowlist, ingest_status ∈ allowlist). NOT IMPLEMENTED. |
| `filing_events` | `filing_event_id` BIGSERIAL PK per `sql/001:46` | FK to `instruments(instrument_id)` | NONE (NOT partitioned per `sql/001:46`; column is `filing_date` not `filed_at`) | STUB. Plan: real-instrument-id replication × N with synthetic `filing_date` spread; let DB assign filing_event_id. NOT IMPLEMENTED. |

**Implementation in Phase 0**: ONLY `seed_ownership_institutions_current.py`. Other 6: stub files containing the above plan as docstring + `NotImplementedError` on call. When a later phase first needs one, that phase implements per the plan with full engineering rigor (per operator discipline).

**Common-helper modules** (Codex iter-4 BLOCKING-1 + HALLUCINATED-API-1 fold):
- `_sentinel_instrument_id(i) -> int` — returns `1_000_000_000 + i`; preflight asserts `SELECT MAX(instrument_id) FROM instruments < 1_000_000_000` (future-state tripwire if real IDs grow past 1e9; corrected direction from v1.4 inverted condition)
- `_validate_floor(target_db, table, floor) -> bool` — reads `SELECT COUNT(*) FROM <table>`; compares to `floors.yaml`
- `_validate_no_refresh_leak(target_db) -> bool` — imports `_drifted_instruments` from `app.jobs.ownership_observations_repair` (per `tests/test_ownership_refresh_writer_merge.py:627` precedent) + calls directly with `(conn, 'ownership_institutions_current', 'ownership_institutions_observations', 'institutions')` + asserts every returned ID `< 1_000_000_000`. Never calls `refresh_institutions_current`. Test fails loudly if any sentinel surfaces.

**Refusals**:
- DB URL must contain `bench` (allowlist); refuses if contains `dev` or `prod`
- `EBULL_BENCH_DB_URL` unset
- For FK-bound tables (financial_facts_raw, sec_filing_manifest issuer-scoped, filing_events): refuses if `instruments` table has 0 rows (cites required FK target + actionable error)

**Caveat documentation** at `docs/operator/runbooks/perf-investigation.md`:
- What seeder does + does NOT do (no skew preservation, no realistic distribution)
- Writer-safety analysis (sentinel-range + drifted-set audit)
- When to use (harness validation, Phase 1-2 perf claims where realism doesn't drive the cliff)
- When NOT to use (Phase 4 S22 MERGE cliff which depends on row distribution)
- The 7 table plans (grep-verified schemas); explicit note "implemented: ownership_institutions_current; remaining 6 implemented when first needed per `feedback_no_ticket_count_obsession`"

**Real-numbers verification** (Codex iter-4 IMPORTANT-2 fold — added 2 sentinel-invariant assertions):
- Run `seed_ownership_institutions_current.py` against bench DB
- Assert `SELECT COUNT(*) FROM ownership_institutions_current` ≥ 1,000,000
- Assert PK uniqueness: `SELECT COUNT(*) FROM (SELECT instrument_id, filer_cik, ownership_nature, exposure_kind FROM ownership_institutions_current GROUP BY 1,2,3,4 HAVING COUNT(*) > 1) x` = 0
- Assert sentinel range: `SELECT COUNT(*) FROM ownership_institutions_current WHERE instrument_id < 1000000000` = 0
- Assert no overlap with real instruments: `SELECT COUNT(*) FROM ownership_institutions_current oic JOIN instruments i ON i.instrument_id = oic.instrument_id` = 0
- **NEW** Assert observations sentinel-free: `SELECT COUNT(*) FROM ownership_institutions_observations WHERE instrument_id >= 1000000000` = 0 (proves direct-current-seed protocol holds)
- **NEW** Assert refresh-state sentinel-free: `SELECT COUNT(*) FROM ownership_refresh_state WHERE instrument_id >= 1000000000 AND category = 'institutions'` = 0 (proves drifted set cannot return sentinel)
- Run `_validate_no_refresh_leak()`: calls `_drifted_instruments` directly + asserts no sentinel IDs returned
- Run `run_explain.sh 1346 --check-floors-only` against seeded DB; assert PASS
- Log all 7 assertions + counts in PR description

**Cross-impact**:
- Grep for `ownership_institutions_current` writers — confirmed: only `refresh_institutions_current` at `app/services/ownership_observations.py:415` (writer iterates from `drifted`; sentinel IDs excluded by design)
- Grep for `ownership_institutions_current` consumers (rollup endpoints + frontend) — sentinel instrument_ids never reach UI (no real instrument has ID ≥ 1e9; rollup endpoints take instrument_id from operator query which targets real IDs)
- Grep for any maintenance script that bulk-rebuilds `_current` from `_observations` — verify none silently drop sentinel rows (any such script must explicitly preserve sentinel range or be tagged bench-DB-unsafe)
- Document expected bench-DB behavior in refresh helper docstring (additive comment): "synthetic rows in instrument_id ≥ 1e9 sentinel range survive because they're never in `drifted`"

**Acceptance**:
- Seeder runs against bench DB; produces fixture meeting `ownership_institutions_current` floor
- All 7 real-numbers assertions pass; output in PR description
- 7-table plan-doc lands at `docs/operator/runbooks/perf-investigation.md` with grep-verified schemas
- 6 stub files for unimplemented tables (each containing plan as docstring; raises `NotImplementedError`)
- Refusal paths tested (synthetic DB URL with `dev`; missing env; empty FK target)
- Cross-impact grep captured (writers + consumers + maintenance scripts)
- Refresh-leak test (`_validate_no_refresh_leak`) committed and passes
- Docstring on `refresh_institutions_current` updated with bench-DB safety comment

**Sub-agents**: data-engineer (schema verification + writer-safety analysis) + code-simplifier + adversarial reviewer (challenge sentinel-range assumption; what if FK is added to instruments later?) + edgartools (schema cross-check)

---

### 2.9 Phase 0.5 — dispatcher residual-idle MEASUREMENT (#1275 re-investigation) + IN-SCOPE FIX

**Why**: #1275 status STALE (PR-2 #1233 fixed ALL_COMPLETED → FIRST_COMPLETED). Hypothesis "30-50% wall-clock waste" never empirically validated POST-fix. Per operator discipline: if measurement reveals a capacity bug (RESULT_A), Phase 0 fixes it in scope (no follow-up ticket spawn).

**Dispatcher state primitives** (Codex iter-1 BLOCKING-5 + HALLUCINATED-API-3 fold — grep-verified):

Real names at `app/services/bootstrap_orchestrator.py:_phase_batched_dispatch` (1603+):
- `runnable: list[_RunnableStage]` (1606)
- `statuses: dict[str, str]` (1710)
- `in_flight: dict[Future, tuple[stage_key, lane]]` (1734)
- `lane_in_flight_count: dict[str, int]` (1735)
- `caps = _satisfied_capabilities(...)` (1841) — recomputed per iteration
- `in_flight_keys = {sk for sk, _ in in_flight.values()}` (1859)
- `pending_keys = [k for k, s in statuses.items() if s == "pending" and k not in in_flight_keys]` (1860)
- Cap requirement: stages declare `CapRequirement` at `:343`; predicate is the existing `_requirement_satisfied` (reuse; do NOT invent — verify exact symbol via grep at implementation time)

#### Phase 0.5.1 — per-iteration dispatcher telemetry

**Telemetry placement** (Codex iter-1 BLOCKING-4 fold): refactor at `bootstrap_orchestrator.py:2023-2031` so emission fires on BOTH branches:

```python
done, _pending_futs = wait(set(in_flight.keys()), return_when=FIRST_COMPLETED, timeout=_CANCEL_POLL_INTERVAL)
wait_returned_empty = not done
_emit_dispatcher_telemetry(  # NEW; fires every iteration
    run_id=run_id,
    statuses=statuses,
    in_flight_keys={sk for sk, _ in in_flight.values()},
    lane_in_flight_count=lane_in_flight_count,
    caps=caps,
    by_key=by_key,
    wait_returned_empty=wait_returned_empty,
)
if wait_returned_empty:
    continue
for fut in done:
    ...
```

`_emit_dispatcher_telemetry` writes one JSONL line per iteration to `var/dispatcher_idle/<run_id>.jsonl` via direct `open(path, "a")`.

#### Phase 0.5.2 — classifier

Per lane per iteration where `wait_returned_empty=True AND lane_in_flight_count[lane]=0`:

- Use `pending_keys = {k for k, s in statuses.items() if s == "pending" and k not in in_flight_keys}` (Codex iter-1 BLOCKING-5 fold).
- For each `pending_keys` member in this lane:
  - ready: `_requirement_satisfied(by_key[k].requires, caps)` True
  - blocked: same False
- IDLE_TYPE_A (dependency-natural): `len(ready)=0 AND len(blocked)>0`
- IDLE_TYPE_B (actionable bug): `len(ready)>0`

Per-run per-lane aggregates: `busy_iter, idle_a_iter, idle_b_iter, idle_b_max_run_iters, idle_b_stages_seen`.

Analysis script: `scripts/dispatcher_idle_analysis.py` consumes `var/dispatcher_idle/<run_id>.jsonl`, emits JSON aggregates.

#### Phase 0.5.3 — multi-run measurement

R1 / R2 / R3 design (per multi-agent BLOCKING-2 fold):
- **R1 (CONTROL)**: BEFORE #1273 lands. Pure telemetry on current dispatcher.
- **R2 (POST-#1273)**: AFTER #1273 lands.
- **R3 (REPRODUCIBILITY)**: same conditions as R2, different starting state.

Decision rule:
- All 3 runs `idle_b_iter ≈ 0` → RESULT_B → close #1275 with multi-run evidence.
- ANY run sustained `idle_b_iter > 5 consecutive iterations` OR `idle_b_iter > 10% busy_iter` → RESULT_A.
- Disagreement → default RESULT_A (action bias).

**RESULT_A in-scope fix** (operator discipline; no follow-up ticket spawn):
- Phase 0.5 capacity-bug fix lands in Phase 0 as an additional PR.
- Fix scope determined by `idle_b_stages_seen` and code inspection of the dispatcher's submit-loop (line 1925-1972).
- Most likely fix: lane-cap arithmetic off-by-one or capability-recompute race. Both fixable in <50 LOC.
- Real-numbers verification: re-run R2 + R3 post-fix; assert `idle_b_iter == 0` across both.
- Cross-impact: grep all `_phase_batched_dispatch` callers; verify lane semantics unchanged; verify cancel-checkpoint timing unchanged.

**Acceptance**:
- Telemetry fires every iteration (BOTH timeout + completion branches).
- Per-run file at `var/dispatcher_idle/<run_id>.jsonl` populated.
- 3 measurement runs captured + classified + recorded in §10.
- RESULT_A → fix shipped in same Phase 0; re-measurement confirms fix.
- RESULT_B → #1275 closed with multi-run evidence.

**Sub-agents**: data-engineer + adversarial reviewer (challenge multi-run design + classifier) + code-simplifier

---

## 3. Sequencing within Phase 0

Work order (single author, serial PRs per CLAUDE.md branch + Claude-bot-review-per-push):

```
1. #1327 (config)
2. #1256 (lint strengthen)
3. #1322 (smoke tests)
4. #1225 audit + fix (single PR — audit informs fix in same PR)
5. NEW-A bundle (harness + lint + CI + floors.yaml + self-test)
6. NEW-C scaffold + ownership_institutions_current implementation + plan-doc for 6 others
7. NEW-B skill + PR-template + cross-links
8. Phase 0.5 telemetry PR (enables R1 control read)
9. #1273 PR1 (helpers + cohort-shape audit memo)
10. Phase 0.5 R1 measurement (control, pre-#1273-instrumentation)
11. #1273 PR2 (7-stage instrumentation + cohort-fingerprint frontend wiring + metrics-analyst skill update)
12. Phase 0.5 R2 + R3 measurements + classifier output
13. If RESULT_A: Phase 0.5 capacity-bug fix PR + re-measurement
14. Phase 0 close-out doc + master plan §3 update
```

Bundling guidance:
- DO bundle NEW-A.1-A.4 + `floors.yaml`.
- DO NOT bundle #1225 + #1273.
- DO NOT bundle #1225 audit + fix as conditional — keep them in ONE PR with audit memo + fix landed together.
- DO split #1273 into PR1 (helpers + audit) + PR2 (instrumentation + frontend wiring + skill update) — both PRs land before Phase 0.5 R2.
- DO split Phase 0.5 into telemetry-PR + measurement-PR (+ optional fix-PR if RESULT_A).

---

## 4. Process protocol (master plan §6 + CLAUDE.md overlay)

Per CLAUDE.md "Working order for every task" + master plan §6:

**Per-PR sequence**:

```
0. CLAUDE.md overlay (pre-protocol):
   a. Read docs/settled-decisions.md → state which apply
   b. Read docs/review-prevention-log.md → state which entries relevant
   c. If implementation pressure suggests changing settled decision → STOP, surface
   d. Branch named feature/NNN-* or fix/NNN-* BEFORE first code touch
1. Draft sub-plan section (per-ticket in §2 above)
2. Self-review against master plan §1-§5 + Phase 0 §0-§3
3. Multi-agent review per-ticket (sub-agents listed in §2.X)
4. Codex 1 as CTO on per-PR plan
5. Iterate to APPROVED
6. **Per-PR operator sign-off** (Codex iter-2 IMPORTANT-2 fold — per master plan §5
   audit-trail requirements; plan-as-whole sign-off does NOT replace per-PR)
7. Implement per the sub-plan
8. Pre-PR agent review + evidence bundle
9. Codex 1 on diff
10. Push + CI/CD
11. Merge after APPROVE on latest commit
12. Post-deploy SLO check
13. Move to next PR
```

---

## 5. Evidence bundle (PR-description hard-wire)

```markdown
## Plan reference
- Master plan: docs/proposals/etl/bootstrap-sub-1h-plan.md (v5.2)
- Phase 0 sub-plan: docs/proposals/etl/phase-0-instrumentation.md (v1.3)
  - §X.Y for this ticket

## CLAUDE.md overlay (step 0)
- Settled decisions applied: [list, or "none"]
- Prevention-log entries relevant: [list, or "none"]
- Branch: feature/NNN-* or fix/NNN-*

## Per-PR review transcripts (paste in full)
- data-engineer plan-review
- code-simplifier plan-review
- [other skill] plan-review
- Codex 1 plan-review
- data-engineer diff-review (step 8)
- Codex 1 diff-review (step 9)

## Per-PR operator sign-off
[date + signature]

## Real-numbers verification (per operator discipline)
- [Command + actual output OR test name + result]
- [Dev DB pre/post state where applicable]

## Cross-impact check (per operator discipline)
- grep for [symbol] callers → [count + audit notes]
- Sibling system review → [outcome]

## Acceptance evidence (per-criterion)

## Sibling-shape audit
N/A — Phase 0 builds harness; no perf claim.

## Rollback criteria

## Post-deploy SLO
```

---

## 6. Done-state for Phase 0

Closes when ALL of:

- [ ] #1225 audit memo + structural fix (Layers A/B/C) shipped; prevention test green for 5 bulk jobs orchestrated context (manual-fire out of scope per §2.1 Layer B); Layer A exception path test green; dev-DB real-numbers smoke in PR description; cross-impact captured; #1225 CLOSED.
- [ ] #1273 PR1 + PR2 merged; cohort-fingerprint wired through API + TS + frontend tooltip; Timeline screenshot + SQL output in PR description; metrics-analyst skill updated; cross-impact on `BootstrapTimelineStageResponse` consumers captured.
- [ ] #1322 merged; smoke tests + negative-test green; xdist cross-impact captured.
- [ ] #1256 merged; 3 negative tests fire; 7 helpers continue to pass; format-constraint doc landed.
- [ ] #1327 merged; grep checks pass; cross-impact `600`-literal grep captured.
- [ ] NEW-A merged; 6 self-test scenarios green; bypass-gating tested; branch-protection rule operator-executed with timestamp captured.
- [ ] NEW-B merged; skill discoverable; PR template updated; 2 cross-links added; master plan §5 cross-linked back.
- [ ] NEW-C merged; `ownership_institutions_current` seeded; 7-table plan-doc landed at `docs/operator/runbooks/perf-investigation.md`; refusal paths tested; cross-impact on writer + consumers captured.
- [ ] Phase 0.5 telemetry merged; 3 measurement runs captured + classified; per-run logs at `var/dispatcher_idle/`.
- [ ] If Phase 0.5 RESULT_A: in-scope fix PR shipped + re-measurement confirms `idle_b_iter == 0`. If RESULT_B: #1275 closed with multi-run evidence.
- [ ] Operator-readable Phase 0 close-out doc at `docs/proposals/etl/phase-0-close.md`.
- [ ] Master plan §3 wall-clock target table updated (note: Phase 0 baseline N/A — no perf claim).
- [ ] Master plan §3 estimate row for Phase 0 updated from current "3-5 days" to actual on close (Codex iter-2 NIT-1 fold).
- [ ] Dev DB floor audit captured in §11; under-floor tables seeded via NEW-C output.

**Phase 1 cannot start until all 14 are checked.**

---

## 7. Risks + mitigations

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| #1225 audit reveals bug deeper than 4 candidates | Medium | More fix scope | Per operator discipline: scope the fix correctly + ship; no "if subtle, defer" |
| #1273 cohort-shape audit shows ≥ 1 non-`__len__()` iterator beyond S16 | Medium | Per-stage strategy split | Day-1 audit in #1273 PR1 |
| Phase 0.5 measurement runs disagree | Medium | Inconclusive → RESULT_A default | Action bias per decision rule |
| `EBULL_BENCH_DB_URL` setup friction for Phase 1 first-perf-author | Medium | Author confusion | Documented in NEW-C runbook + NEW-B skill + PR template comment |
| NEW-C seeder for `ownership_institutions_current` produces fixture that doesn't reflect prod cliff | Medium | Phase 4 perf claim could miss real bug | Caveat doc explicit; Phase 4 author re-evaluates seeder adequacy before perf claim |
| RESULT_A capacity-bug fix introduces regression | Low | Bootstrap broken | Re-measurement (R2 + R3 post-fix) + cross-impact grep on `_phase_batched_dispatch` callers |
| Codex 1 surfaces new BLOCKING | Expected | Iterate per §6 step 5 | Process working as designed |
| `perf-claim-lint` CI false-positive | Low | Hotfix needed | Gated bypass with audit (emergency label + justification + env) |
| Skill (NEW-B) drift after master §5 revision | Medium-long | Skill cites stale §5.X | Cross-link in master plan §5 → NEW-B added in Phase 0 close |
| Phase 0.5 telemetry placement regression (forgetting timeout branch) | Medium | Half the iterations missed | Pre-PR test asserts log line count == iteration count |
| `target_cohort_fingerprint` frontend tooltip clashes with existing tooltip rendering | Low | Visual regression | Frontend-design sub-agent review; screenshot in PR; existing `title=` attribute on bar element is additive |

---

## 8. References

- Master plan: `docs/proposals/etl/bootstrap-sub-1h-plan.md` (v5.2 — Phase 0 estimate row UPDATED in Phase 0 close per Codex iter-2 NIT-1)
- PR12 archived spec: `docs/_archive/2026-05/2026-05-21-pr12-ownership-current-writer-merge.md`
- Bootstrap orchestrator (grep-verified):
  - `_resolve_stage_rows` `:1253`
  - `_capability_is_dead` `:732`
  - `_phase_batched_dispatch` `:1603`
  - dispatcher state primitives `:1709-1972`
  - `_satisfied_capabilities` `:1841`
  - `CapRequirement` `:343`
  - `in_flight_keys` derivation `:1859`
  - `pending_keys` derivation `:1860`
  - Telemetry placement target `:2023-2031`
  - S25 stage_key/job_name divergence `:1166-1168` (stage_key=`fundamentals_sync`, job_name=`fundamentals_sync_bootstrap`)
- Bulk ingester: `app/services/sec_bulk_orchestrator_jobs.py` (`_current_running_bootstrap_run_id:90`, `_record_archive_result:124`)
- Bootstrap state: `app/services/bootstrap_state.py` (`mark_stage_success:543`, `mark_stage_running:514`)
- Timeline endpoint: `app/api/processes.py:648`; SELECT `:694`; response model `:276-301`
- Frontend type: `frontend/src/api/types.ts:1417-1435`
- Frontend bar: `frontend/src/pages/ProcessDetailPage.tsx:1186-1221`
- PR template: `.github/pull_request_template.md` (lowercase per repo)
- MERGE-writer lint: `scripts/check_ownership_refresh_writer_pattern.sh:431-448`
- Engineering skills + data-engineer skill + metrics-analyst skill

---

## 9. Operator sign-off

**STATUS**: SIGNED OFF.

```
SIGNED-OFF: 2026-05-25 luke.bradford@hotmail.co.uk
BLOCKERS: none
REVIEW PROVENANCE:
  - Multi-agent v1 (4-lens parallel): 15 BLOCKING + 25 IMPORTANT + 7 hallucinated → folded
  - Codex iter-1: 5 BLOCKING + 7 IMPORTANT + 3 hallucinated → folded (v1.2)
  - Codex iter-2 + operator no-ticket-count discipline: 2 BLOCKING + 3 IMPORTANT + 1 hallucinated → folded (v1.3)
  - Codex iter-3: 2 BLOCKING + 2 IMPORTANT + 2 hallucinated → folded (v1.4)
  - Codex iter-4: 1 BLOCKING + 3 IMPORTANT + 1 hallucinated → folded (v1.5)
  - Codex iter-5: BLOCKING=none, IMPORTANT=none, HALLUCINATED-API=none, 2 NITs folded → APPROVED
```

Per-PR sign-off (master plan §6 step 6, Codex iter-2 IMPORTANT-2 fold) still required BEFORE code starts on each individual PR.

---

## 10. Phase 0.5 measurement result

**STATUS**: pending Phase 0.5 telemetry + 3-run measurement.

```
RUN R1 (CONTROL, pre-#1273):
  RUN-ID: [tbd]
  LOG: var/dispatcher_idle/<id>.jsonl
  CLASSIFIER: { busy_iter, idle_a_iter, idle_b_iter, idle_b_max_run_iters, idle_b_stages_seen }

RUN R2 (POST-#1273):
  [same structure]

RUN R3 (REPRODUCIBILITY):
  [same structure]

DECISION: [RESULT_A | RESULT_B | INCONCLUSIVE]
ACTION: [fix PR # + re-measurement evidence OR #1275 closure SHA]
```

---

## 11. Dev DB floor audit

**STATUS**: pending NEW-A + NEW-C merge.

```
table                                   floor       dev_measured   bench_post_seed   status
ownership_institutions_current          1,000,000   [tbd]          [tbd]             [PASS|UNDER]
ownership_institutions_observations     2,000,000   [tbd]          [tbd]             [PASS|UNDER]
ownership_insiders_observations           500,000   [tbd]          [tbd]             [PASS|UNDER]
ownership_funds_observations              200,000   [tbd]          [tbd]             [PASS|UNDER]
financial_facts_raw                    10,000,000   [tbd]          [tbd]             [PASS|UNDER]
sec_filing_manifest                     1,000,000   [tbd]          [tbd]             [PASS|UNDER]
filing_events                           2,000,000   [tbd]          [tbd]             [PASS|UNDER]
```

UNDER on bench-post-seed (for the 1 implemented table) → seeder bug → fix in same Phase 0 PR.

UNDER on bench-post-seed for the 6 stub tables → expected (not implemented); when first needed by a later phase, that phase implements per the NEW-C plan-doc.

---

## 12. Multi-agent + Codex review dispositions

### v1 → v1.1 (multi-agent 4-lens)
15 BLOCKING + 25 IMPORTANT + 7 NIT + 7 HALLUCINATED-API. All BLOCKING + most IMPORTANT folded. (Full table in commit history.)

### v1.1 → v1.2 (Codex 1 iter-1)
5 BLOCKING + 7 IMPORTANT + 2 NIT + 3 HALLUCINATED-API. All folded. (Full table in commit history.)

### v1.2 → v1.3 (operator discipline + Codex 1 iter-2)

**Operator discipline fold (`feedback_no_ticket_count_obsession`)**:
- All "ticket spawn" patterns → in-scope fix (frontend wiring for cohort-fingerprint; Phase 0.5 RESULT_A fix; #1225 unconditional fix)
- All "audit-only + conditional fix" → audit informs fix; both land in same PR
- All "defer to later phase" → only retained when genuinely different concern (e.g. 6 floor-table seeders that aren't needed yet — operator chose scaffold+1; when later phase first needs another, that phase implements per the plan)
- Real-numbers verification required in every PR
- Cross-impact check required in every PR
- Removed: estimate-discussion (operator dismissed)

**Codex iter-2 fold**:

| ID | Source | Finding | Disposition |
|---|---|---|---|
| Codex-iter2-B1 | Codex 1 | #1225 escape hatch contradicts master Phase 0 ownership | **FOLDED** — §2.1 ships fix unconditionally; if audit conclusive-no-bug, still hardens path |
| Codex-iter2-B2 | Codex 1 | NEW-C seeder not implementation-grade | **FOLDED** — §2.8 design covers all 7 floor tables with per-table FK strategy; operator-chosen scaffold + `ownership_institutions_current` reference implementation; 6 others land when first needed |
| Codex-iter2-I1 | Codex 1 | Estimate-honesty | **DROPPED** (operator dismissed — "ticket counts don't matter; the work is the work") |
| Codex-iter2-I2 | Codex 1 | CLAUDE.md overlay missing per-PR sign-off | **FOLDED** — §4 step 6 now mandatory per-PR; plan-as-whole sign-off does NOT replace |
| Codex-iter2-I3 | Codex 1 | #1273 cohort table drift (count + S25 job_name) | **FOLDED** — §2.2 table corrected to 6 list + 1 streaming = 7; S25 stage_key=`fundamentals_sync`, job_name=`fundamentals_sync_bootstrap` documented |
| Codex-iter2-N1 | Codex 1 | Master plan §3 still says "3-5 days" | **FOLDED** — §6 close-out updates master plan §3 in same PR sequence |
| Codex-iter2-H1 | Codex 1 | `RAND() * 1e9` is PG `random()` | **FOLDED** — §2.8 uses `random()` (also: PK rewrite via `random()`-based offset in `_pk_offset` helper) |

**Plan v1.3 ready for Codex 1 iteration 3 + operator sign-off.**

### v1.3 → v1.4 (Codex 1 iter-3)

Codex iter-3 verdict: `#1225 unconditional fix: resolved. Cohort-fingerprint frontend wiring: resolved in plan. RESULT_A in-scope fix: resolved in plan. NEW-C is the remaining blocker.`

All findings in §2.8 NEW-C; folded:

| ID | Source | Finding | Disposition |
|---|---|---|---|
| Codex-iter3-B1 | Codex 1 | NEW-C writer-safety: synthetic rows get wiped by MERGE `WHEN NOT MATCHED BY SOURCE` regardless of filer_cik offset | **FOLDED** — §2.8 writer-safety strategy: sentinel `instrument_id >= 1_000_000_000` outside real range; refresh job iterates from `drifted` (real-instruments-with-observations per `ownership_observations_repair.py:181`); sentinel IDs never enter `drifted` (we seed `_current` directly, NOT `_observations`); seeder ships `_validate_no_refresh_leak()` test that asserts the strategy holds |
| Codex-iter3-B2 | Codex 1 | 7-table design has hallucinated schemas (financial_facts_raw PK + filing_events FK/partitioning) | **FOLDED** — §2.8 7-table plan rewritten with grep-verified real schemas (every claim cites `sql/NNN:line`); financial_facts_raw PK = `(fact_id, period_end)`; filing_events PK = `filing_event_id` BIGSERIAL, no manifest FK, no partitioning, column is `filing_date` not `filed_at`; sec_filing_manifest PK = `accession_number`, FK to instruments + self-FK |
| Codex-iter3-I1 | Codex 1 | `ownership_institutions_current` FK strategy overstated — no FK to instruments | **FOLDED** — §2.8 table updated: "Real FK: NONE"; validation reworded as "manual integrity check" via `JOIN instruments ON instrument_id` ; doc states no FK to instruments |
| Codex-iter3-I2 | Codex 1 | Implemented-table PK plan omits `ownership_nature` + `exposure_kind` | **FOLDED** — §2.8 table now states real PK `(instrument_id, filer_cik, ownership_nature, exposure_kind)` per `sql/114:134`; CHECK constraint values enumerated; synthetic strategy rotates through allowed values |
| Codex-iter3-N1 | Codex 1 | Per-PR sign-off in §4 step 6 workable | **NOTED** — no change |
| Codex-iter3-H1 | Codex 1 | `financial_facts_raw` PK shape hallucinated | **FOLDED** (= Codex-iter3-B2) |
| Codex-iter3-H2 | Codex 1 | `filing_events` manifest FK + `filed_at` partition strategy hallucinated | **FOLDED** (= Codex-iter3-B2) |

**Plan v1.4 ready for Codex 1 iteration 4.**

### v1.4 → v1.5 (Codex 1 iter-4)

Iter-4 NIT confirmed: "Schema corrections for original iter-3 blockers are mostly real. Spot-grep confirmed institutions _current PK/checks/no FK, financial_facts_raw PK/FKs/partitioning, filing_events PK/FK/filing_date/no partitioning, sec_filing_manifest PK/FKs/checks."

Remaining findings all in NEW-C; folded:

| ID | Source | Finding | Disposition |
|---|---|---|---|
| Codex-iter4-B1 | Codex 1 | `_sentinel_instrument_id` preflight assertion direction inverted (`< MAX(real) - 1e9` is false for normal DBs) | **FOLDED** — §2.8 corrected: preflight asserts `MAX(real) < 1_000_000_000` + emitted sentinels `> MAX(real)` |
| Codex-iter4-I1 | Codex 1 | `drifted` is STATE-anchored (not observations-anchored) — `ownership_observations_repair.py:152` selects from `ownership_refresh_state s LEFT JOIN obs_max`. Edge case: `sql/163` re-apply could backfill sentinel state row | **FOLDED** — §2.8 writer-safety section rewritten with precise drifted semantics + backfill hazard caveat (`sql/163` docstring warning + post-backfill sentinel-cleanup invariant) |
| Codex-iter4-I2 | Codex 1 | Real-number assertions incomplete; need observations-zero-sentinel + refresh-state-zero-sentinel | **FOLDED** — §2.8 real-numbers list expanded from 5 to 7 assertions (added the two requested) |
| Codex-iter4-I3 | Codex 1 | Stub plans say "verify at impl time" — contradicts grep-verified discipline; encode insiders + funds PKs now | **FOLDED** — §2.8 7-table table updated: insiders PK = `(instrument_id, holder_identity_key, ownership_nature, source, source_document_id, period_end)` per `sql/113:77`; funds PK = `(instrument_id, fund_series_id, period_end, source_document_id)` per `sql/123:89` |
| Codex-iter4-H1 | Codex 1 | `_validate_no_refresh_leak` claimed a `run_observations_repair_sweep` dry-run API that doesn't exist | **FOLDED** — §2.8 rewrites helper to import `_drifted_instruments` directly + assert no sentinel returned. Precedent at `tests/test_ownership_refresh_writer_merge.py:627` |
| Codex-iter4-N1 | Codex 1 | Iter-3 schema corrections solid (NIT only) | **NOTED** — no change |

**Plan v1.5 ready for Codex 1 iteration 5.**

---

## Per-PR addendum: #1327 review v1 (post-plan-sign-off)

Multi-agent + Codex review of §2.5 per-PR plan (2026-05-25 post-operator-sign-off) — pre-Codex per-PR iter-2.

| ID | Source | Finding | Disposition |
|---|---|---|---|
| 1327-B1 | data-engineer + Codex | Wrong test path: `tests/runbooks/test_safety.py` doesn't exist; actual is `tests/test_runbook_safety.py` + CLI default coverage belongs in `tests/test_stream_a_runbooks_cli.py:111` | **FOLDED** — §2.5 step 4 corrected to cite both real paths |
| 1327-B2 | data-engineer + Codex | Missing doc edit at `run-8-readiness.md:189` (parameter table also says default is 600) | **FOLDED** — §2.5 step 3 lists both lines |
| 1327-I1 | data-engineer + code-simplifier | "Defense-in-depth" framing misleading — `safety.py:175` default is unreachable (all 3 callers pass explicit `timeout_sec=`) | **FOLDED** — per code-simplifier SIMPLIFY: drop default entirely; make REQUIRED kwarg. Single source of truth = `DEFAULT_WAIT_FOR_JOBS_SEC` constant. v1.6 cleaner than dual edit |
| 1327-I2 | data-engineer | Stale spec reference at `stream-a-run-8-fixes.md:670` | **REBUTTED** — frozen historical proposal documenting state at original ship; PR description notes the drift. Editing a frozen archive doc is the wrong fix; the live source of truth is now the constant + `run-8-readiness.md` |
| 1327-N1 | Codex + code-simplifier | Test should use `inspect.signature` not slow timeout-path | **FOLDED** — §2.5 step 4 uses `inspect.signature` for REQUIRED-kwarg assertion |
| 1327-N2 | data-engineer | Plan acceptance grep too loose | **FOLDED** — §2.5 acceptance adds tighter grep for stray `600` literals in runbook context |
