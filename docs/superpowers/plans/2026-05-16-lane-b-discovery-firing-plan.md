# Implementation plan — Lane B discovery firing fix

**Spec:** `docs/superpowers/specs/2026-05-16-lane-b-discovery-firing.md`
**Issue:** #1181
**Branch:** `fix/1181-lane-b-layer-123-firing`
**Date:** 2026-05-16
**Status:** plan — pre-Codex-1b

## Task decomposition

Eight tasks, ordered. Each is independently verifiable; later tasks
depend on earlier ones.

### T1 — Add `ScheduledJob.exempt_from_universal_bootstrap_gate` field

**File:** `app/workers/scheduler.py`

**Change:** add one `bool = False` field to the `@dataclass(frozen=True) class ScheduledJob` after `prerequisite: PrerequisiteFn | None = None` (line 217). Docstring block:

```python
# #1181 — opt-out from the universal check_bootstrap_state_gate
# (PR1b-2 of #1064). When True, every dispatch path (scheduled fire,
# catch-up, manual-queue) BYPASSES the install-state gate. Per-job
# ``prerequisite`` still runs. The carve-out is an "unaudited design
# bypass" — no decision_audit row is written; the static registry
# allow-list is the audit trail.
#
# Eligibility (enforced by tests/test_universal_gate_carve_out.py
# allow-list + invariant assertions):
#   1. catch_up_on_boot=True (the carve-out exists for the
#      boot-time-only catch_up evaluation trap).
#   2. prerequisite is None.
#   3. Body is empty-DB safe (natural no-op).
#   4. Bounded cost per fire (single-digit MB fetch max).
# Adding a new exempt job requires a new spec + Codex review + update
# to test_exempt_allowlist_is_explicit. See spec
# docs/superpowers/specs/2026-05-16-lane-b-discovery-firing.md §4.2.
exempt_from_universal_bootstrap_gate: bool = False
```

**Verification:**
- `uv run ruff check app/workers/scheduler.py`
- `uv run pyright app/workers/scheduler.py`
- Field defaults to False so every existing `ScheduledJob(...)` call site continues to type-check unchanged.

### T2 — Flip Layer 2 to `exempt=True` + update docstring

**File:** `app/workers/scheduler.py` (lines 1010-1033)

**Change:** add `exempt_from_universal_bootstrap_gate=True,` to the `JOB_SEC_DAILY_INDEX_RECONCILE` `ScheduledJob(...)`. Replace the `prerequisite=None` comment block with a fuller explanation that names the universal gate and links to §4.2:

```python
# NO per-job prereq AND exempt from the universal bootstrap_state
# gate (#1181). Layer 2 is the safety-net against missed Atom
# windows; daily-04:00-UTC cadence + catch_up_on_boot means missing
# a fire = losing yesterday's reconcile permanently. The universal
# gate (PR1b-2 of #1064) would otherwise block every fire while
# bootstrap_state != 'complete'. Daily-index against an empty/partial
# universe is a natural no-op (subject_resolver filters every CIK).
# See spec docs/superpowers/specs/2026-05-16-lane-b-discovery-firing.md
# §4.2 for the carve-out eligibility contract.
prerequisite=None,
exempt_from_universal_bootstrap_gate=True,
```

**Verification:**
- T8 invariant tests run AFTER this — Layer 2 must satisfy `catch_up_on_boot=True`, `prerequisite is None`, and be in the exempt allow-list.

### T3 — Short-circuit in `_wrap_invoker` (scheduled fire)

**File:** `app/jobs/runtime.py` (lines 1418-1437)

**Change:** wrap the `check_bootstrap_state_gate` call in an exempt-bypass guard. `job` is already in scope from line 1377 (`self._job_registry.get(job_name)`).

Codex 1b WARNING (preserve current `job is None` semantics): today the gate ALWAYS runs in `_wrap_invoker` regardless of registry membership. The exempt bypass must apply ONLY when `job is not None AND job.exempt_from_universal_bootstrap_gate is True`. `job is None` keeps the gate firing (matches today). Pattern:

```python
# Bootstrap-state gate (#1064 PR1b-2). Bypassed only for jobs that are
# both registered AND flagged exempt (#1181 — Layer 2 safety-net
# carve-out; see spec §4.2 for the eligibility contract). The
# job-is-None case continues to gate (preserves today's registry-drift
# fail-closed posture).
is_exempt = job is not None and job.exempt_from_universal_bootstrap_gate
if not is_exempt:
    try:
        with psycopg.connect(database_url, autocommit=True) as conn:
            allowed, reason = check_bootstrap_state_gate(
                conn,
                job_name=job_name,
                invocation_path="scheduled",
                override_present=False,
            )
            if not allowed:
                record_job_skip(conn, job_name, reason, params_snapshot=dict(params))
                logger.info(
                    "scheduled fire of %r skipped — %s",
                    job_name,
                    reason,
                )
                return
    except Exception:
        logger.warning(
            "bootstrap_state gate for %r failed; running anyway",
            job_name,
            exc_info=True,
        )
```

**Verification:** T8 behavioral test `test_exempt_job_bypasses_scheduled_fire_gate` asserts gate not called.

### T4 — Short-circuit in `_run_catchup` (boot catch-up)

**File:** `app/jobs/runtime.py` (lines 1021-1037)

**Change (Codex 1b WARNING — correct variable source):** the `job` variable in the catch-up loop is `job = catch_up_jobs[name]` at line 994, NOT `self._job_registry.get(name)`. `catch_up_jobs` is keyed by job-name with `ScheduledJob` values, so `job` is always non-None here (the loop iterates `overdue` which is a subset of `catch_up_jobs.keys()`). The simpler `not job.exempt...` guard is correct in this scope.

```python
# PR1b-2 (#1064): universal bootstrap_state gate. Skipped for
# exempt jobs (#1181 — Layer 2 safety-net carve-out; see spec §4.2).
if not job.exempt_from_universal_bootstrap_gate:
    gate_allowed, gate_reason = check_bootstrap_state_gate(
        conn,
        job_name=name,
        invocation_path="scheduled",
        override_present=False,
    )
    if not gate_allowed:
        processed.add(name)
        record_job_skip(conn, name, gate_reason, params_snapshot=dict(params_dict))
        skipped.append((name, gate_reason))
        continue
```

**Verification:** T8 behavioral test `test_exempt_job_bypasses_catchup_gate`.

### T5 — Collapse listener double-lookup + short-circuit

**File:** `app/jobs/listener.py` (lines 184-218)

**Change (per spec §5.2 + Codex 1a WARNING 1):** replace the existing pattern:

```python
job_in_registry = any(j.name == job_name for j in SCHEDULED_JOBS)
if job_in_registry:
    ...check_bootstrap_state_gate(...)
...
# (later)
job = next((j for j in SCHEDULED_JOBS if j.name == job_name), None)
```

with ONE lookup at the top:

```python
job = next((j for j in SCHEDULED_JOBS if j.name == job_name), None)

# Bootstrap-state gate (#1064 PR1b-2). Skipped when:
#   - job is None: bootstrap-internal jobs (orchestrator + stages)
#     are not registered.
#   - job.exempt_from_universal_bootstrap_gate: #1181 carve-out for
#     safety-net jobs; see spec §4.2.
if job is not None and not job.exempt_from_universal_bootstrap_gate:
    try:
        with psycopg.connect(settings.database_url, autocommit=True) as conn:
            allowed, reason = check_bootstrap_state_gate(
                conn,
                job_name=job_name,
                invocation_path="manual_queue",
                override_present=override_present,
            )
    except Exception:
        logger.warning(
            "listener: bootstrap_state gate for %r failed; running anyway",
            job_name,
            exc_info=True,
        )
        allowed, reason = True, ""

    if not allowed:
        logger.info(
            "listener: rejecting manual_job request_id=%d for %r — %s",
            request_id,
            job_name,
            reason,
        )
        with psycopg.connect(settings.database_url, autocommit=True) as conn:
            mark_request_rejected(conn, request_id, error_msg=reason)
        return
```

Subsequent prereq lookup uses the same `job` variable (drops the second `next(...)` scan).

**Verification:** T8 behavioral tests:
- `test_exempt_job_bypasses_listener_gate_no_override`
- `test_exempt_job_bypasses_listener_gate_with_override`
- `test_non_exempt_job_with_override_still_calls_gate`

### T6 — Bootstrap-gate module docstring cross-reference

**File:** `app/services/processes/bootstrap_gate.py` (module docstring + `check_bootstrap_state_gate` docstring)

**Change (Codex 1b NIT — name the field):** add a "Carve-outs" subsection to the module docstring that names `ScheduledJob.exempt_from_universal_bootstrap_gate` explicitly so future callers find the contract via grep. Add a one-liner to `check_bootstrap_state_gate`'s docstring noting that callers in registered-job dispatch paths must check the carve-out flag BEFORE invoking the gate.

Carve-outs subsection draft:

```text
## Carve-outs

A registered ``ScheduledJob`` may opt out of this gate by setting
``exempt_from_universal_bootstrap_gate=True`` (added in #1181). When
exempt, all three dispatch paths (scheduled fire, catch-up,
manual-queue) MUST short-circuit BEFORE calling this helper — no
``decision_audit`` row is written. The carve-out is for safety-net
jobs whose missed cadence cannot be recovered (e.g.
``sec_daily_index_reconcile`` daily 04:00 UTC reconcile).

See docs/superpowers/specs/2026-05-16-lane-b-discovery-firing.md §4.2
for the eligibility contract.
```

No production code change in this file.

### T7 — Settled-decisions + prevention-log docs

**Files:**
- `docs/settled-decisions.md` — add §"Universal bootstrap-state gate (#1064 PR1b-2)" and §"Safety-net catch-up gate carve-out (#1181)".
- `docs/review-prevention-log.md` — add §"Design intent in `ScheduledJob.prerequisite=None` can be silently neutered by a later-added universal gate" (full text in spec §5.7).
- `.claude/skills/data-engineer/SKILL.md` — add §11.6.1 "Discovery-layer end-to-end smoke (Lane B)" runbook (spec §5.4).

### T8 — Test coverage

**File:** `tests/test_universal_gate_carve_out.py` (NEW)

7 behavioral tests + 3 registry-invariant tests per spec §5.5:

Behavioral (each uses strict `MagicMock()` + `assert_not_called()` for `check_bootstrap_state_gate` per Codex 1a WARNING 3):

1. `test_exempt_job_bypasses_scheduled_fire_gate`
2. `test_exempt_job_bypasses_catchup_gate`
3. `test_exempt_job_bypasses_listener_gate_no_override`
4. `test_exempt_job_bypasses_listener_gate_with_override`
5. `test_non_exempt_job_with_override_still_calls_gate`
6. `test_non_exempt_job_still_gated_in_partial_error`
7. `test_exempt_job_with_failing_prereq_still_rejects` (SYNTHETIC ScheduledJob built in-test; documented as such)

Registry-invariant:

8. `test_exempt_allowlist_is_explicit` — set == `{JOB_SEC_DAILY_INDEX_RECONCILE}`.
9. `test_exempt_implies_catch_up_on_boot_true`.
10. `test_exempt_implies_prerequisite_none`.

**Test helpers required (Codex 1b WARNING — drop DB fixture):**

- DROP `_partial_error_state()`. Behavioral tests patch `check_bootstrap_state_gate` directly (assert_not_called for exempt; return_value=(False, "...") for non-exempt). The point of every exempt test is "gate not called" — DB state is irrelevant. The point of every non-exempt test is "gate returns False"; controlling that via mock is cleaner than mutating a singleton DB row.
- `_fake_exempt_job(...)` factory — builds a synthetic `ScheduledJob` with arbitrary prereq for test 7. Keep.
- Patch path: `app.jobs.runtime.check_bootstrap_state_gate` for runtime tests, `app.jobs.listener.check_bootstrap_state_gate` for listener tests (matches existing `tests/test_pr1b2_envelope_and_gate.py` patch convention).
- **Reuse (Codex 1b NIT):** model new tests on existing patterns:
  - Listener tests reuse the shape of `tests/test_pr1b2_envelope_and_gate.py::_runtime_mock()` + `_job_no_prereq()` (private to that module today; copy the shape rather than refactor for export — single-PR scope).
  - Catch-up tests reuse `tests/test_jobs_runtime.py::_make_catchup_runtime()` (line ~530) which already provides the catch-up scaffolding without DB setup.

**Per-layer wiring assertions (implementation decision):**

Per-layer exemption assertions live in `tests/test_universal_gate_carve_out.py::TestLayer123ExemptionWiring` rather than `tests/test_layer_123_wiring.py` — keeps all carve-out test code (behavioral, invariant, per-layer) in one file so future readers find everything via grep on `exempt_from_universal_bootstrap_gate`:

- `test_layer1_atom_fast_lane_not_exempt` — `JOB_SEC_ATOM_FAST_LANE` → `exempt=False`.
- `test_layer2_daily_index_reconcile_exempt` — `JOB_SEC_DAILY_INDEX_RECONCILE` → `exempt=True`.
- `test_layer3_per_cik_poll_not_exempt` — `JOB_SEC_PER_CIK_POLL` → `exempt=False`.

**In-scope test repair (`feedback_fix_in_scope_default.md`):**

- `tests/test_pr1b2_envelope_and_gate.py::_job_no_prereq` — `MagicMock` auto-creates truthy attributes; set `exempt_from_universal_bootstrap_gate = False` explicitly so the listener's bypass check hits the non-exempt path.
- `tests/test_bootstrap_state_gate.py::test_sec_form3_has_bootstrap_complete_prereq` — `sec_form3_ingest` was retired 2026-05-14 (PR #1162); rename + repoint to `sec_atom_fast_lane`. Pre-existing drift unrelated to this PR but tripped by impacted-files smoke.

## Cross-cutting verification (after T1-T8)

### Pre-push gates (CLAUDE.md mandatory)

```bash
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv run pytest -n0 tests/test_universal_gate_carve_out.py \
                  tests/test_layer_123_wiring.py \
                  tests/test_pr1b2_envelope_and_gate.py \
                  tests/test_jobs_runtime.py
```

Full suite via pre-push hook (excluding pytest if env issues recur per `feedback_pre_push_xdist_postgres_locks.md`).

### Codex 2 (pre-push)

`codex exec review` against branch diff. Address findings; iterate to CLEAN.

### Operator-visible smoke (per spec §6.2)

1. Run spec §5.4 runbook against dev DB for each of the 3 layers. Record `job_runs.run_id` in PR description.
2. Verify Layer 2 carve-out specifically: confirm `sec_daily_index_reconcile` scheduled fire succeeds without override flag (post-merge or in ad-hoc test via cadence tweak).
3. No-regression check: confirm a non-exempt SEC job (e.g. `sec_per_cik_poll`) still skips with `bootstrap_not_complete` when no override flag is passed.

## Risk / blast radius

**Surface area:**
- 1 new dataclass field (default False, no impact on existing rows).
- 3 modified dispatch paths, each adding ONE conditional check before the existing gate call.
- 1 collapsed double-lookup in listener (pure refactor, no behavior change).
- 1 docstring update.
- Doc + test additions.

**Blast radius if wrong:**
- Code change to dispatch paths affects every scheduled/catch-up/manual-queue dispatch. If the conditional is malformed, exempt jobs continue to be gated (today's behavior — safe regression to baseline) OR non-exempt jobs bypass the gate (HIGH risk: lets every SEC job run against partial DB).
- The registry-invariant test allow-list catches accidental flag-flip on any non-Layer-2 job.
- The behavioral tests catch a malformed conditional that lets the gate run for exempt jobs OR bypasses for non-exempt.

**Mitigations:**

- All 3 dispatch paths share the same SEMANTIC contract — "bypass the gate IFF the job is registered AND flagged exempt; otherwise fail-closed (gate the run)". Local condition shape differs because the available `job` reference differs by scope: T3 (_wrap_invoker) needs `is_exempt = job is not None and job.exempt...; if not is_exempt:` to preserve `job is None` gating; T4 (catch-up) uses bare `if not job.exempt...` because `job = catch_up_jobs[name]` is guaranteed non-None; T5 (listener) uses `if job is not None and not job.exempt...` because the listener handles both registered + unregistered (orchestrator/internal) jobs in one path. The contract is what tests assert; the local phrasing is justified by scope.
- Behavioral tests use strict `MagicMock()` + `assert_not_called()` — cannot be satisfied by a happy-path bypass.
- Registry invariants are CI-enforced.

## Out of scope

- **Lane C** (data_freshness_index cadence audit) — separate ticket if it surfaces.
- **Bootstrap completion** — operator-intentional partial_error state stays untouched.
- **Frontend ops dashboard** for discovery-layer health — separate ticket.

## Rollback

Single PR; revert via `git revert`. No DB migration. No flag flip in production needed. Reverting restores the pre-fix behavior (Layer 2 still gated; missed-yesterday daily-index lost during bootstrap break — known degradation that operator has tolerated since 2026-05-09).

## Codex 1b resolutions

- **WARNING T4 wrong variable source:** corrected — `job = catch_up_jobs[name]` (line 994), not `self._job_registry.get(name)`. Simpler `not job.exempt...` guard applies.
- **WARNING `_wrap_invoker` job-is-None semantics:** preserved current fail-closed posture. Exempt bypass applies ONLY when `job is not None AND job.exempt_from_universal_bootstrap_gate is True`. `job is None` keeps the gate firing.
- **WARNING `_partial_error_state` fixture:** dropped. Behavioral tests patch `check_bootstrap_state_gate` directly. Cleaner + faster.
- **NIT helper reuse:** plan now points at existing `_runtime_mock()` / `_job_no_prereq()` shape in `tests/test_pr1b2_envelope_and_gate.py` and `_make_catchup_runtime()` in `tests/test_jobs_runtime.py`.
- **NIT bootstrap_gate.py docstring:** updated plan to name the field explicitly so grep finds the carve-out contract.

Open-question 1 (missed call-sites): Codex confirmed only the 3 documented production call-sites. Other hits are tests/docs only.
