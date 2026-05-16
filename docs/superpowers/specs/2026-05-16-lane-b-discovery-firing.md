# Lane B — Verify Layer 1/2/3 discovery firing + fix universal-gate supersession of Layer 2

**Issue:** #1181
**Branch:** `fix/1181-lane-b-layer-123-firing`
**Status:** spec — pre-Codex-1a
**Author:** Claude (autonomous)
**Date:** 2026-05-16

## 1. Problem

Two coupled defects discovered while verifying that #1155's Layer 1/2/3 discovery wiring actually fires steady-state on dev DB.

### 1.1 Defect A — universal bootstrap gate silently supersedes Layer 2's explicit no-prereq carve-out

`app/workers/scheduler.py:1010-1032` declares `sec_daily_index_reconcile` (Layer 2) with `prerequisite=None`. The docstring rationale:

> NO `_bootstrap_complete` prereq — JobRuntime evaluates `catch_up_on_boot` only at process start, so a prereq-blocked catch-up cannot re-fire when bootstrap completes later. Without this exception a stack that boots mid-bootstrap loses yesterday's reconcile permanently.

`app/services/processes/bootstrap_gate.py::check_bootstrap_state_gate` (added by PR1b-2 of #1064) is a UNIVERSAL gate that runs in `_wrap_invoker` (`app/jobs/runtime.py:1418-1437`), in catch-up (`runtime.py:1027`), and in the manual-queue listener (`listener.py:188`) BEFORE any per-job prereq. Scheduled fires cannot override (`bootstrap_gate.py:136-149`).

Net effect: Layer 2's design intent is silently neutered. Every scheduled fire of Layer 2 is blocked while `bootstrap_state.status != 'complete'` — exactly the failure mode the carve-out was designed to prevent.

**Live evidence (dev DB, 2026-05-16):**

```
job_name                  | total | succ | skip | fail | last
sec_atom_fast_lane        |   752 |    0 |  752 |    0 | 2026-05-16 12:55:00 UTC
sec_daily_index_reconcile |     6 |    0 |    6 |    0 | 2026-05-16 12:53:38 UTC
sec_per_cik_poll          |    62 |    0 |   62 |    0 | 2026-05-16 12:00:00 UTC
```

All skipped with reason `bootstrap_not_complete`. `bootstrap_state.status='partial_error'` since 2026-05-09 (operator intentionally holding bootstrap broken).

Layer 1 + Layer 3 being gated is BY DESIGN (their `prerequisite=_bootstrap_complete` was explicit). Layer 2 being gated is the bug.

### 1.2 Defect B — no end-to-end smoke confirms the Layer 1/2/3 path

Even when bootstrap completes, no test or operator runbook confirms that a real filing flows end-to-end:

```
SEC Atom/daily-index/submissions → discovery job → sec_filing_manifest row pending →
sec_manifest_worker → manifest_parsers/<source>.py → operator-visible table updated.
```

CLAUDE.md DoD clause 8-11 covers the parser half but not the discovery half. Operator cannot answer "is Layer 1/2/3 actually working?" without ad-hoc psql.

## 2. Goal

Eliminate the silent design-intent regression by making the per-job opt-out from the universal bootstrap gate explicit, and add a minimum-viable end-to-end smoke runbook + automated test so future regressions show up loudly.

## 3. Non-goals

- **Lane C** — `data_freshness_index` cadence-row population audit. Layer 3's `subjects_due_for_recheck` reader path currently has zero rows to drain (all 54,791 rows are `state='current'`). That is a cadence-seeding question, separate ticket if it surfaces.
- **Frontend ops dashboard** for discovery-layer health. Backend signal first.
- **Fixing bootstrap itself.** Operator is intentionally holding it broken; out of scope.
- **Generalising the carve-out** to other safety-net jobs proactively. Only Layer 2 has the documented carve-out today; if a second safety-net job arrives later, that PR adds itself to the carve-out list, not this one.

## 4. Settled-decisions impact

This spec adds one new settled decision and codifies an existing-but-undocumented one.

### 4.1 New settled decision — Universal bootstrap-state gate

The universal `check_bootstrap_state_gate` (PR1b-2 of #1064) is the install-state gate that runs BEFORE any per-job `ScheduledJob.prerequisite` in three call sites:

- `app/jobs/runtime.py::_wrap_invoker` (scheduled fire).
- `app/jobs/runtime.py::JobRuntime._run_catchup` (boot catch-up).
- `app/jobs/listener.py::_dispatch` (manual-queue).

Scheduled fires never override; manual-queue dispatches override via `{control:{override_bootstrap_gate:true}}` (writes `decision_audit` row).

This is a settled architectural invariant. Adding a new `ScheduledJob` is to opt-in to the gate by default; opting out requires an explicit carve-out (§4.2).

### 4.2 New settled decision — Carve-out for safety-net catch-up jobs

A `ScheduledJob` may set `exempt_from_universal_bootstrap_gate=True` IFF it satisfies ALL of:

1. **Catch-up correctness depends on no-skip:** `catch_up_on_boot=True` AND missed cadence windows are lost forever if not fired (e.g. yesterday's daily-index can never be re-fetched cleanly).
2. **No per-job prerequisite:** `prerequisite is None`. The carve-out rests on the body being safe-against-empty-DB; a non-None prereq would create two opinions on the same install-state question. If a future safety-net job needs both, the contract must be revised before exemption.
3. **Empty-universe behavior is a natural no-op:** body is idempotent + safe against an empty/partial DB. No destructive write, no expensive fetch loop.
4. **Cost is bounded:** one fetch per fire, sized in single-digit MB. Anything larger needs operator-intentional gating.

Layer 2 (`sec_daily_index_reconcile`) is the first and only such job today (one ~1MB fetch; subject_resolver filters every unknown CIK to natural no-op; `prerequisite=None`).

**Enforcement (Codex 1a WARNING 2):** §4.2 is enforced by registry-invariant tests (§5.5 tests 8/9/10) — an explicit allow-list AND `catch_up_on_boot is True` AND `prerequisite is None` assertions. Adding another carve-out requires:

- New spec entry + Codex 1a-equivalent review.
- Update to the allow-list assertion in `test_exempt_allowlist_is_explicit`.
- Update to `docs/settled-decisions.md` §"Safety-net catch-up gate carve-out".

Unilateral flag-flip is forbidden and caught by CI via the allow-list test.

## 5. Design

### 5.1 New field on `ScheduledJob`

```python
@dataclass(frozen=True)
class ScheduledJob:
    ...
    # Layer 2 safety-net carve-out (#1181). When True, the universal
    # check_bootstrap_state_gate is BYPASSED for every dispatch path
    # (scheduled fire, catch-up, manual-queue). Per-job prerequisite
    # still runs. See spec 2026-05-16-lane-b-discovery-firing.md §4.2
    # for the eligibility contract — a unilateral flag flip is forbidden;
    # additions require a new spec entry.
    exempt_from_universal_bootstrap_gate: bool = False
```

Default `False` preserves universal behavior for every existing job. Only Layer 2 flips to `True`.

### 5.2 Gate-wrapper changes (3 call sites)

The opt-out check happens BEFORE `check_bootstrap_state_gate`. If exempt, gate evaluation is skipped entirely (no `record_job_skip` row, no `decision_audit` write).

```python
job = self._job_registry.get(job_name)
if job is None or not job.exempt_from_universal_bootstrap_gate:
    allowed, reason = check_bootstrap_state_gate(conn, ...)
    if not allowed:
        record_job_skip(conn, job_name, reason, params_snapshot=...)
        return
```

All three call sites share the same SEMANTIC contract — "bypass the gate IFF the job is registered AND flagged exempt; otherwise fail-closed (gate the run)". Local condition shape differs by scope:

- `app/jobs/runtime.py::JobRuntime._wrap_invoker` (scheduled fire): `is_exempt = job is not None and job.exempt_from_universal_bootstrap_gate; if not is_exempt: <gate>`. `job is None` (registry-drift case) keeps the gate firing — fail-closed.
- `app/jobs/runtime.py::JobRuntime._run_catchup` (catch-up loop): bare `if not job.exempt_from_universal_bootstrap_gate: <gate>`. `job = catch_up_jobs[name]` is guaranteed non-None inside the overdue loop.
- `app/jobs/listener.py::_dispatch` (manual-queue): `if job is not None and not job.exempt_from_universal_bootstrap_gate: <gate>`. `job is None` here is the bootstrap-internal orchestrator/stage carve-out (no gate, no prereq).

**Listener change (Codex 1a WARNING 1):** today the listener does two separate registry scans — `job_in_registry = any(j.name == job_name for j in SCHEDULED_JOBS)` for the gate decision, then `job = next((j for j in SCHEDULED_JOBS if j.name == job_name), None)` later for the per-job prereq lookup. Collapse into ONE lookup at the top of the function:

```python
job = next((j for j in SCHEDULED_JOBS if j.name == job_name), None)
# Bootstrap-internal jobs (orchestrator + stages) are not registered;
# job is None and both the universal gate and per-job prereq skip.
if job is not None and not job.exempt_from_universal_bootstrap_gate:
    allowed, reason = check_bootstrap_state_gate(conn, ..., override_present=override_present)
    ...
```

This eliminates the risk of "kept `job_in_registry`, bolted on exemption later" leaving the gate call live for exempt jobs.

For listener: bootstrap-internal jobs (orchestrator + stages) are NOT registered → `job is None` → gate skipped today. The carve-out extends the same skip to exempt registered jobs. **For exempt jobs, `override_present` is meaningless** — the gate is bypassed by design (not by operator action), so no `decision_audit` row is written. This is an "unaudited design bypass" (Codex 1a NIT), distinct from the manual-queue "operator override" which writes audit + requires explicit `{control:{override_bootstrap_gate:true}}`.

### 5.3 Layer 2 flag flip

```python
ScheduledJob(
    name=JOB_SEC_DAILY_INDEX_RECONCILE,
    ...
    catch_up_on_boot=True,
    prerequisite=None,
    exempt_from_universal_bootstrap_gate=True,  # #1181
),
```

Docstring + comment block expand to capture the universal-gate context so a future reader sees the full picture.

### 5.4 Operator runbook — discovery-layer end-to-end smoke

New section `.claude/skills/data-engineer/SKILL.md` §11.6.1.

```markdown
## Discovery-layer end-to-end smoke (Lane B)

Pre-condition: jobs process running. Bootstrap state may be incomplete.

For each of (sec_atom_fast_lane, sec_daily_index_reconcile, sec_per_cik_poll):

### Step 1 — Snapshot baseline

Record the last run_id (any status) BEFORE firing so we can prove the
next row is OUR fire, not a coincidental scheduled fire. COALESCE
guards the first-fire case where the job has zero prior rows (e.g.
sec_daily_index_reconcile on a stack that has only ever skipped):

  SELECT COALESCE(MAX(run_id), 0) FROM job_runs WHERE job_name='<job_name>';

### Step 2 — Fire via manual queue

POST /jobs/<job_name>/run
body: {"control": {"override_bootstrap_gate": true}}

  (override flag required only if bootstrap_state.status != 'complete';
   Layer 2 with the carve-out fires without override regardless.)

### Step 3 — Confirm the fire succeeded

Within 60s:

  SELECT run_id, status, started_at, finished_at, row_count,
         params_snapshot, linked_request_id
  FROM job_runs
  WHERE job_name='<job_name>' AND run_id > <baseline_run_id_from_step_1>
  ORDER BY run_id DESC LIMIT 1;

Pass criteria: exactly one new row with status='success' and a populated
linked_request_id matching the manual-queue request from step 2.
row_count may be 0 (no new accessions to ingest is a valid success path
for atom/daily-index in steady state); the proof is status='success' +
finished_at populated.

### Step 4 — Discovery-attribution check (atom + daily-index only)

`sec_filing_manifest.source` records the parser-source enum, NOT the
discovery origin. To attribute new manifest rows to the fired run, use
the run's started_at/finished_at window:

  SELECT source, COUNT(*) AS new_rows
  FROM sec_filing_manifest
  WHERE created_at BETWEEN <run.started_at> AND <run.finished_at>
                            + INTERVAL '5 seconds'
  GROUP BY source;

A non-zero result during a known-active-day window proves the discovery
path wrote rows. A zero result is INCONCLUSIVE for atom (no new
accessions in the 5-min window is normal) — re-check the jobs-process
log line `sec_atom_fast_lane: feed=X matched=Y upserted=Z ...` for the
fired run's row_count attribution.

### Step 5 — Per-cik poll scheduler-write check (per_cik_poll only)

Confirm the poll updated freshness scheduler state:

  SELECT subject_type, source, last_polled_at, next_recheck_at
  FROM data_freshness_index
  WHERE last_polled_at BETWEEN <run.started_at> AND <run.finished_at>
                                  + INTERVAL '5 seconds'
  LIMIT 5;

Expect non-zero rows when the poll had subjects in-budget.

### Step 6 — Confirm scheduled-fire registration

Manual fire proves the invoker body works. Scheduled-fire registration
is proved by checking the registry's next_run_time is populated:

  GET /system/jobs  (or equivalent admin endpoint)

For sec_atom_fast_lane: next_run_time within ~5 min.
For sec_daily_index_reconcile: next_run_time at next 04:00 UTC.
For sec_per_cik_poll: next_run_time at top of next hour.

### Step 7 — Wait for natural scheduled fire (atom + per_cik only)

For atom (5 min cadence) and per_cik (hourly): wait one cadence and
re-run step 3. A scheduled fire's row has `linked_request_id IS NULL`
and confirms APScheduler is dispatching.

For daily-index (04:00 UTC): scheduled-fire confirmation is captured by
the registration check in step 6 — waiting 24h is not viable. The full
end-to-end is proved by (manual-fire success in step 3) + (registration
populated in step 6); the universal-gate carve-out (§5.3) is what makes
the next scheduled fire succeed regardless of bootstrap_state.
```

### 5.5 Automated test coverage

New `tests/test_universal_gate_carve_out.py`:

**Behavioral tests (gate-helper SHOULD-NOT-BE-CALLED — Codex 1a WARNING 3):**

For every "exempt bypasses" test, the gate helper is patched with a
strict `MagicMock()` and `assert_not_called()` after the dispatch.
A bare `return_value=(True, '')` mock would hide the case where the
exemption check is missing and the gate is consulted and happens to
return True. The whole point of the exemption is that the gate is
NOT consulted.

1. `test_exempt_job_bypasses_scheduled_fire_gate` —
   `bootstrap_state.status='partial_error'`, exempt job's
   `_wrap_invoker` runs the invoker. Assert `check_bootstrap_state_gate`
   was not called; no `record_job_skip` row.
2. `test_exempt_job_bypasses_catchup_gate` — same condition;
   `_run_catchup` includes the exempt job in `firing`, not `skipped`.
   Assert gate not called.
3. `test_exempt_job_bypasses_listener_gate_no_override` — manual-queue
   dispatch, no `override_bootstrap_gate` flag. Run dispatches; gate
   not called; no `mark_request_rejected`; no `decision_audit` row.
4. `test_exempt_job_bypasses_listener_gate_with_override` (Codex 1a
   WARNING 4) — manual-queue dispatch WITH
   `{control:{override_bootstrap_gate:true}}`. Run dispatches; gate
   not called; NO `decision_audit` row (override is meaningless for
   exempt jobs; carve-out is "unaudited design bypass" not "operator
   override").
5. `test_non_exempt_job_with_override_still_calls_gate` (Codex 1a
   WARNING 4) — non-exempt job + override flag still calls gate
   with `override_present=True` and writes the `decision_audit` row
   on bypass.
6. `test_non_exempt_job_still_gated_in_partial_error` — regression
   guard for the default path.
7. `test_exempt_job_with_failing_prereq_still_rejects` (Codex 1a
   WARNING 4) — SYNTHETIC test only: constructs a fake `ScheduledJob`
   with `exempt=True` AND `prerequisite=lambda c: (False, "test")`.
   The registry invariant (§5.5 test 10) forbids this combination in
   the real registry, so this test must build the fake `ScheduledJob`
   in-test rather than mutating SCHEDULED_JOBS. The test asserts that
   IF such a job were ever added (e.g. via a future contract revision
   per §4.2), the gate exemption does not also bypass the per-job
   prereq. Documents the layering contract: gate exemption ≠ prereq
   exemption.

**Registry-invariant tests (Codex 1a WARNING 2 — eligibility contract enforcement):**

8. `test_exempt_allowlist_is_explicit` — collect every
   `ScheduledJob` with `exempt_from_universal_bootstrap_gate=True`.
   Assert the set equals exactly `{JOB_SEC_DAILY_INDEX_RECONCILE}`.
   Adding a new exempt job requires updating this assertion + the
   spec §4.2 list + a new spec/Codex round per §4.2's contract.
9. `test_exempt_implies_catch_up_on_boot_true` — for every exempt
   job, assert `catch_up_on_boot is True`. The carve-out exists
   specifically for the catch-up boot-time-only evaluation trap;
   `catch_up_on_boot=False` + `exempt=True` is incoherent.
10. `test_exempt_implies_prerequisite_none_or_explicit` — for every
    exempt job, assert `prerequisite is None`. Layer 2's carve-out
    rests on natural-no-op-against-empty-DB; a non-None prereq would
    create two conflicting opinions on the same gate question. If
    a future exempt job needs both, the contract must be revised.

Plus extension to `tests/test_layer_123_wiring.py`:
- `test_layer1_not_exempt_from_universal_gate` — Layer 1 retains
  `exempt=False` (its `_bootstrap_complete` prereq is the right gate).
- `test_layer2_exempt_from_universal_gate` — Layer 2 has `exempt=True`.
- `test_layer3_not_exempt_from_universal_gate` — Layer 3 retains
  `exempt=False`.

### 5.6 Settled-decisions doc additions

Add two sections to `docs/settled-decisions.md`:

1. **"Universal bootstrap-state gate (#1064 PR1b-2)"** — codifies §4.1 of this spec.
2. **"Safety-net catch-up gate carve-out (#1181)"** — codifies §4.2; lists Layer 2 as the sole current member; restates the eligibility contract.

### 5.7 Prevention-log entry

Add to `docs/review-prevention-log.md`:

```
### Design intent in `ScheduledJob.prerequisite=None` can be silently neutered by a later-added universal gate
- Symptom: `sec_daily_index_reconcile` was declared `prerequisite=None`
  in #1155 to defend against the `catch_up_on_boot` boot-time-only
  evaluation trap (missed-yesterday daily-index lost forever otherwise).
  PR1b-2 of #1064 later added a universal `check_bootstrap_state_gate`
  that runs BEFORE any per-job prereq in every dispatch path. The
  Layer 2 carve-out was silently defeated for 9 days before live-DB
  smoke caught it (6 scheduled fires, all skipped `bootstrap_not_complete`).
- Prevention: When introducing a cross-cutting gate that runs BEFORE
  per-job `ScheduledJob.prerequisite`, audit every existing job whose
  `prerequisite=None` is a deliberate carve-out (grep
  `prerequisite=None.*\n.*explicit\|catch_up\|safety` in the registry).
  Either preserve the per-job opt-out via an explicit field
  (`exempt_from_universal_bootstrap_gate` pattern) or document the
  removal explicitly in the cross-cutting PR. Treat any
  `prerequisite=None` with a docstring rationale as a load-bearing
  signal, not a default.
```

## 6. Test plan

### 6.1 Unit / integration

Per §5.5. Pre-push gates (ruff/format/pyright/pytest impacted) must pass.

### 6.2 Operator-visible smoke (per DoD clause 8-11 for ETL changes)

This is not an ETL change per se (no parser, no schema migration affecting ownership/observations), so clauses 8-12 don't strictly apply. However, the spec touches the dispatch path that every ETL job runs through, so a parallel smoke is right.

**Smoke panel:** the 3 discovery jobs themselves, not instruments.

1. Run the operator runbook §5.4 for each of the 3 jobs against dev DB. Record `job_runs.run_id` + outcome in PR description.
2. Force `bootstrap_state.status='complete'` (or wait for one to occur) and confirm Layer 2 still fires normally (regression guard — exemption must not change behavior when gate would have allowed).
3. Cross-source check: not applicable here (no new data path; existing data path is preserved).

### 6.3 Pre-push gates

Per CLAUDE.md:

```
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv run pytest -n0 tests/test_universal_gate_carve_out.py tests/test_layer_123_wiring.py
                  tests/test_pr1b2_envelope_and_gate.py tests/test_jobs_runtime.py
```

Full pytest suite via pre-push hook (excluding pytest per `feedback_pre_push_xdist_postgres_locks.md` if env issues recur — impacted-files green gates push regardless).

## 7. Files touched

- `app/workers/scheduler.py` — add `ScheduledJob.exempt_from_universal_bootstrap_gate`; flip Layer 2.
- `app/jobs/runtime.py` — opt-out short-circuit in `_wrap_invoker` + `_run_catchup`.
- `app/jobs/listener.py` — collapse `job_in_registry` + later `next(...)` into one lookup at top of `_dispatch`; opt-out short-circuit before `check_bootstrap_state_gate` (see §5.2).
- `app/services/processes/bootstrap_gate.py` — module docstring + `check_bootstrap_state_gate` docstring cross-reference to the carve-out.
- `docs/settled-decisions.md` — two new sections (§4.1 universal gate, §4.2 carve-out).
- `docs/review-prevention-log.md` — one new entry (§5.7).
- `.claude/skills/data-engineer/SKILL.md` — §11.6.1 runbook (§5.4).
- `tests/test_universal_gate_carve_out.py` (new) — 7 behavioral tests + 3 registry-invariant tests (10 total per §5.5).
- `tests/test_layer_123_wiring.py` — extend with 3 per-layer exemption assertions.

Estimated diff: ~300 net lines (mostly docs + tests; ~40 lines code in 3 modules + 1 dataclass field).

## 8. Rollout

Single PR. No migration. No flag-flip in production (Layer 2 carve-out takes effect immediately on deploy; impact is "Layer 2 actually fires on schedule" which is the desired state).

Post-merge:

1. Wait one daily-index cadence (or trigger manual fire) on dev DB. Confirm Layer 2 succeeds.
2. Flip `[[us-source-coverage]]` + `[[legacy-cron-retirement]]` memory pre-condition lines from UNMET to MET (for Layer 2; Layer 1+3 remain bootstrap-gated by design, which is correctly captured).
3. Re-smoke Layer 1 + 3 once operator chooses to complete bootstrap (out of scope for this PR).

## 9. Codex 1a resolutions

- **BLOCKING 1 (runbook proof via `sec_filing_manifest.source`):** runbook §5.4 step 4 rewritten to use the run's `started_at`/`finished_at` window against `sec_filing_manifest.created_at`, with a note that zero-rows-in-window is INCONCLUSIVE for atom (no new accessions in a 5-min window is normal) and the authoritative attribution is the jobs-process log line + `job_runs.row_count`.
- **BLOCKING 2 (manual fire ≠ scheduled fire for daily-index):** runbook §5.4 step 6 (registration check via `/system/jobs` `next_run_time`) is now the scheduled-fire proof for Layer 2; step 7 waits for natural scheduled fire only for Layer 1 (5 min) and Layer 3 (hourly).
- **WARNING 1 (listener double registry lookup):** §5.2 explicitly collapses the existing `job_in_registry = any(...)` + later `job = next(...)` into one lookup at the top of `_dispatch`. Both the gate and the prereq paths consume the same `job` reference.
- **WARNING 2 (§4.2 policy-only):** §4.2 now includes an enforced contract (4 IFF conditions: `catch_up_on_boot=True`, `prerequisite is None`, no-op-safe, bounded-cost) AND a registry-invariant test allow-list (§5.5 tests 8/9/10) AND an unmissable-CI gate. Unilateral flag-flip is mechanically forbidden.
- **WARNING 3 (test gate-helper not called):** §5.5 explicitly uses a strict `MagicMock()` + `assert_not_called()` for every exempt-path test, not a `return_value=(True, '')` mock.
- **WARNING 4 (missing listener edge tests):** §5.5 adds tests 4 (exempt + override → no audit row), 5 (non-exempt + override → gate called with `override_present=True` + audit row), 7 (exempt + failing per-job prereq → still rejects).
- **NIT (audit-bypass vocabulary):** §5.2 + §4.2 + settled-decisions doc use "unaudited design bypass" for the exempt-job path and reserve "operator override" for the explicit `override_bootstrap_gate` envelope on non-exempt jobs.

## 9.1 Remaining open questions (deferred to Codex 1b plan review)

- Should `tests/test_layer_123_wiring.py` extension cover all 3 layers' exemption status or only the new field? Currently spec'd as all 3 for symmetry.
- Settled-decisions doc has no §"Universal bootstrap-state gate" today — should we land it as part of this PR (yes per §5.6) or split? Currently bundled because it's the cross-reference the carve-out doc points at.

## 10. Settled decisions preserved

- `Process topology (#719)` — jobs process owns scheduling. No change.
- `Provider strategy / SEC EDGAR` — preserved. No new feeds.
- `Universal bootstrap-state gate (#1064 PR1b-2)` — preserved; the carve-out is a documented EXCEPTION not a redesign.

## 11. References

- #1155 / PR #1157 — Layer 1/2/3 + sec_rebuild wiring (parent).
- #1064 — admin control hub rewrite (universal bootstrap-state gate origin).
- #1180 — manifest-worker drain fairness (just-merged; sibling Lane).
- `docs/superpowers/specs/2026-05-04-etl-coverage-model.md` §Layer 1/2/3.
- `docs/superpowers/specs/2026-05-13-layer-123-wiring.md` (#1155 spec).
- `app/services/processes/bootstrap_gate.py` — universal gate.
- Memory: `[[us-source-coverage]]`, `[[legacy-cron-retirement]]`, `[[etl-freshness-redesign]]`.
