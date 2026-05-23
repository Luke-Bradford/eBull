# Implementation plan — #1184 orchestrator inner JobLock re-entrancy

> Spec: [`2026-05-17-orchestrator-inner-lock-removal.md`](./2026-05-17-orchestrator-inner-lock-removal.md)
> (v4 CLEAN per Codex 1a 2026-05-17). Issue: **#1184**.
> Branch: `fix/1184-orchestrator-inner-lock-removal`.

## 1. Task graph (sequential — single PR)

```text
T1. JobLock re-entrancy (app/jobs/locks.py)
        │
        ├──▶ T2. morning_candidate_review source-registry entry (app/jobs/sources.py)
        │           │
        │           ├──▶ T3. Doc clean-up in sources.py (#1184 limitation removal)
        │
        ├──▶ T4. Tests
        │           ├── T4a. tests/test_job_lock_reentrancy.py (NEW, 6 tests)
        │           ├── T4b. tests/test_orchestrator_adapter_morning_candidate_review.py (NEW, 1 test)
        │           ├── T4c. tests/test_job_registry.py — pinned-list update
        │           └── T4d. tests/test_joblock_per_source.py + tests/test_db_lane_family_split.py — rewrite 4 same-context same-source cases to cross-thread (§2.5)
        │
        ├──▶ T5. Prevention-log update (docs/review-prevention-log.md — #1183 entry amendment)
        │
        └──▶ T6. Local gates (ruff / format / pyright / pytest impacted-files)
                    │
                    └──▶ T7. Codex 2 pre-push review
                                │
                                └──▶ T8. Push + PR + poll review/CI + merge
```

## 2. T1 — JobLock re-entrancy

### 2.1 Files touched

- `app/jobs/locks.py` — add ContextVar; modify `JobLock.__init__`,
  `__enter__`, `__exit__`; expose `_source` for tests.

### 2.2 Exact diff intent

Add (top of module, after imports):

```python
from contextvars import ContextVar, Token

from app.jobs.sources import Lane, source_for

# Per-call-context set of source-lock buckets currently held by a
# `JobLock` instance in this context. Used to make same-source nested
# acquisitions re-entrant (i.e. no-op) so the orchestrator's outer
# `JobLock(orchestrator_*_sync, source='db')` does not cause inner
# `JobLock(<db-lane-job>, source='db')` acquires to self-skip with
# `JobAlreadyRunning`.
#
# Scope is intentionally process-wide, not orchestrator-specific
# (spec §6.2.1). Today no non-orchestrator code path nests JobLock
# acquisitions; the re-entrancy mechanism is correct for ANY legitimate
# nesting against the same source, not just the orchestrator's.
_HELD_SOURCES: ContextVar[frozenset[Lane]] = ContextVar(
    "_joblock_held_sources", default=frozenset()
)
```

Modify `JobLock.__init__`:

```python
def __init__(self, database_url: str, job_name: str) -> None:
    self._database_url = database_url
    self._job_name = job_name
    # Lane | None — None ONLY for test_only_per_name acquires (the
    # raw-job_name escape hatch, which intentionally opts out of
    # re-entrancy). Production callers always have a non-None Lane.
    self._source: Lane | None = source_for(job_name)
    self._lock_key = f"job_source:{self._source}"
    self._conn: psycopg.Connection[object] | None = None
    self._reentrant: bool = False
    self._held_token: Token[frozenset[Lane]] | None = None
```

Drop `_lock_key_for(job_name)` static method body (was a one-liner;
move `f"job_source:{source}"` inline above). Modify `test_only_per_name`
classmethod to set `instance._source = None` and `instance._lock_key =
job_name` (raw, not source-prefixed). The `None` sentinel makes the
re-entrancy check in `__enter__` short-circuit via `is not None` —
`test_only_per_name` acquires are NEVER treated as re-entrant.
Document in the method docstring.

Replace `JobLock.__enter__`:

```python
def __enter__(self) -> JobLock:
    held = _HELD_SOURCES.get()
    if self._source is not None and self._source in held:
        # Re-entrant acquire — outer JobLock in this context already
        # holds the same source bucket. Postgres would reject the
        # second pg_try_advisory_lock from a different session; the
        # application-layer re-entrancy bypass treats the acquire as
        # a no-op (we already hold it). See spec §6.1.
        self._reentrant = True
        return self
    # Normal acquire path — unchanged behaviour below.
    conn = psycopg.connect(self._database_url, autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT pg_try_advisory_lock(hashtext(%s)::int)",
                (self._lock_key,),
            )
            row = cur.fetchone()
        if row is None or not row[0]:
            conn.close()
            raise JobAlreadyRunning(self._job_name)
    except Exception:
        with _suppress_close_errors():
            conn.close()
        raise
    self._conn = conn
    # Set the contextvar ONLY after the Postgres acquire succeeds so
    # an acquisition that raises does not leak a stale entry. Restored
    # by __exit__ via the saved token.
    if self._source is not None:
        self._held_token = _HELD_SOURCES.set(held | {self._source})
    return self
```

Replace `JobLock.__exit__`:

```python
def __exit__(self, exc_type, exc, tb) -> None:
    if self._reentrant:
        # No connection was opened, no contextvar mutation occurred.
        # Nothing to release.
        return
    if self._held_token is not None:
        try:
            _HELD_SOURCES.reset(self._held_token)
        finally:
            self._held_token = None
    conn = self._conn
    self._conn = None
    if conn is None:
        return
    # ... existing pg_advisory_unlock + close logic unchanged ...
```

### 2.3 Type of `_source`

`_source` typed as `Lane | None`. Production constructor sets
`self._source = source_for(job_name)` (always non-None — `source_for`
returns `Lane` or raises). `test_only_per_name` sets
`self._source = None`. Re-entrancy check becomes
`if self._source is not None and self._source in held:`. This is
pyright-clean (`Lane | None`) where the alternative empty-string
sentinel would fail the `Literal[...]` type assertion.

### 2.4 Risks

- `Lane` import in `app/jobs/locks.py` adds a dependency on
  `app/jobs/sources.py`. `source_for` is already imported at line 68;
  adding `Lane` to the same import is risk-free.
- `_source = source_for(job_name)` is now called from `__init__`
  rather than `__enter__` via `_lock_key_for`. Semantically identical
  — `source_for` is pure and side-effect-free. `KeyError` now raises
  at `JobLock(...)` construction instead of `__enter__`. Existing test
  `tests/test_joblock_per_source.py::TestJobLockUnknownJobName::test_unknown_raises_keyerror`
  asserts `pytest.raises(KeyError, match="unknown job_name"):
  JobLock(...)` — wraps the constructor itself, so the earlier raise
  is compatible. No production caller catches `KeyError` on
  `__enter__` (verified by repo-wide grep `grep -rn "JobLock(" --include="*.py"`).
- `test_only_per_name` gets `_source = None`. Document this in its
  docstring: "test_only_per_name acquires are NEVER treated as
  re-entrant — the per-name semantics is the whole point."

### 2.5 Existing tests this fix breaks (must be updated in same PR)

Codex 1b correctly flagged that the existing same-context same-source
serialisation tests become wrong post-fix because they exercise
re-entrancy:

| Test file | Tests | Pre-fix expectation | Post-fix expectation |
|-----------|-------|---------------------|----------------------|
| `tests/test_joblock_per_source.py` | `test_same_source_serialises` (`sec_form3_ingest` + `sec_def14a_ingest`, both sec_rate) | nested `with` raises | nested `with` BYPASSES (re-entrant) |
| `tests/test_joblock_per_source.py` | `test_db_source_serialises` (`orchestrator_full_sync` + `retry_deferred_recommendations`, both db) | nested `with` raises | nested `with` BYPASSES (re-entrant) |
| `tests/test_joblock_per_source.py` | `test_etoro_source_serialises` (`execute_approved_orders` + `etoro_lookups_refresh`, both etoro) | nested `with` raises | nested `with` BYPASSES (re-entrant) |
| `tests/test_db_lane_family_split.py` | `test_same_family_source_still_serialises` | nested `with` raises | nested `with` BYPASSES (re-entrant) |

Per spec §6.2.1 the new behaviour is intentional and correct (the
old expectation was a quirk of session-scoped Postgres locks, not a
real serialisation requirement against a re-entrant call from the
same call context). Update each test to assert
cross-context/process serialisation instead:

```python
def test_same_source_serialises(self) -> None:
    """sec_form3_ingest + sec_def14a_ingest share source=sec_rate.
    Cross-process / cross-thread acquires MUST still serialise.
    Same-context re-entrancy is intentional post-#1184 and is covered
    by tests/test_job_lock_reentrancy.py.

    Threads start with empty _HELD_SOURCES (Python ContextVar is NOT
    auto-propagated across threading.Thread), so the inner thread's
    JobLock acquire goes to the real Postgres advisory lock and
    collides with the outer thread's session — exactly the
    cross-process/cross-context contention this test gates.
    """
    import queue
    import threading

    outer_holding = threading.Event()
    inner_done = threading.Event()
    outer_errors: queue.Queue[BaseException] = queue.Queue()
    inner_results: queue.Queue[BaseException | str] = queue.Queue()

    def hold_outer() -> None:
        try:
            with JobLock(settings.database_url, "sec_form3_ingest"):
                outer_holding.set()
                # Wait for inner to complete its acquire attempt; bound
                # the wait so a bug in inner does not hang the suite.
                if not inner_done.wait(timeout=10.0):
                    raise TimeoutError("inner did not complete within 10s")
        except BaseException as exc:  # noqa: BLE001 — propagate
            outer_errors.put(exc)

    def try_inner() -> None:
        try:
            if not outer_holding.wait(timeout=10.0):
                raise TimeoutError("outer did not acquire within 10s")
            try:
                with JobLock(settings.database_url, "sec_def14a_ingest"):
                    inner_results.put("acquired unexpectedly")
            except JobAlreadyRunning as exc:
                inner_results.put(exc)
        finally:
            inner_done.set()

    t1 = threading.Thread(target=hold_outer, daemon=True)
    t2 = threading.Thread(target=try_inner, daemon=True)
    t1.start(); t2.start(); t1.join(timeout=15.0); t2.join(timeout=15.0)
    assert not t1.is_alive() and not t2.is_alive(), "threads hung"
    # Surface any outer-thread errors so failures don't get swallowed.
    if not outer_errors.empty():
        raise outer_errors.get()
    result = inner_results.get_nowait()
    assert isinstance(result, JobAlreadyRunning), f"expected JobAlreadyRunning, got {result!r}"
```

Apply analogous diff to the other three tests (`test_db_source_serialises`,
`test_etoro_source_serialises`, `test_same_family_source_still_serialises`).
Each follows the same `Event`/`Queue`/`join(timeout=...)` shape so a
hung or errored thread surfaces as a test failure rather than a
suite hang.

`test_cross_source_runs_concurrently` (line 39-45) and
`test_sec_rate_vs_sec_bulk_download_run_parallel` (line 61-65) are
UNAFFECTED — both use DIFFERENT sources, no re-entrancy triggered.

## 3. T2 — morning_candidate_review source-registry entry

### 3.1 Files touched

- `app/jobs/sources.py` — add one entry to `MANUAL_TRIGGER_JOB_SOURCES`.

### 3.2 Exact diff intent

```python
MANUAL_TRIGGER_JOB_SOURCES: dict[str, Lane] = {
    # ... existing entries ...
    "fx_rates_refresh": "db",
    # ... etc ...
    "monthly_report": "db",
    # NEW (#1184):
    # morning_candidate_review — heuristic ranking + recommendation build.
    # Reachable via composite orchestrator adapter
    # (refresh_scoring_and_recommendations) AND via manual-trigger queue.
    # DB-bound read + write; matches the existing db-lane sibling jobs.
    "morning_candidate_review": "db",
}
```

### 3.3 Risks

- Adds 1 entry to the registry; conflict-detection in
  `_build_job_name_to_source` only fires if the same job_name appears
  with a different effective lane elsewhere. `morning_candidate_review`
  is not in `SCHEDULED_JOBS` or `_BOOTSTRAP_STAGE_SPECS` → no conflict.

## 4. T3 — Doc clean-up in sources.py

### 4.1 Files touched

- `app/jobs/sources.py` — strip the "TWO classes" + "Known partial-fix
  limitation (#1184)" comment blocks; collapse to single-class
  description that's now accurate.

### 4.2 Diff intent

Replace the comment block (current lines ~147-167 + the per-entry
limitation paragraph lines ~199-213) with:

```python
# ---------------------------------------------------------------------------
# MANUAL_TRIGGER_JOB_SOURCES — source-lock coverage for jobs outside
# SCHEDULED_JOBS + _BOOTSTRAP_STAGE_SPECS.
# ---------------------------------------------------------------------------
#
# Every job_name in this map must resolve to a source via source_for()
# so that JobLock acquisition succeeds. Entries fall into two operational
# patterns, but the source-lookup contract is the same:
#
# 1. Operator manual-trigger-only jobs (e.g. sec_rebuild). Companion
#    param-metadata at app/services/processes/param_metadata.py
#    MANUAL_TRIGGER_JOB_METADATA; covered by tests/test_layer_123_wiring.py.
#
# 2. Jobs registered in app/jobs/runtime.py::_INVOKERS but not in
#    SCHEDULED_JOBS (cadence was moved into the orchestrator by #260).
#    These are reachable via the orchestrator's adapter inner-JobLock,
#    the /sync HTTP direct-call path, the boot sweep, and via manual
#    queue dispatch. The inner JobLock for the orchestrator scheduled-
#    cron path is no longer a self-skip hazard since #1184 — JobLock
#    detects same-source re-entrancy in the same call context and
#    bypasses the redundant Postgres acquire.
```

Drop the per-entry "Known partial-fix limitation (#1184)" paragraph.
Replace with a one-line comment per entry that names the resource
profile only.

## 5. T4 — Tests

### 5.1 T4a — `tests/test_job_lock_reentrancy.py` (NEW)

Six tests per spec §6.6.2 (a-e) + the test_only_per_name escape-
hatch invariant. All use real registered jobs:

- `orchestrator_full_sync` → source=`db` (ScheduledJob)
- `fx_rates_refresh` → source=`db` (MANUAL_TRIGGER_JOB_SOURCES)
- `daily_portfolio_sync` → source=`etoro` (MANUAL_TRIGGER_JOB_SOURCES)
- `execute_approved_orders` → source=`etoro` (ScheduledJob)

Fixture pattern (matches existing `tests/test_joblock_per_source.py`):

- Advisory-lock-only assertions use `settings.database_url` directly.
  Advisory locks are cluster-wide, not database-scoped, so dev DB is
  fine — no rows are written.
- Tests that exercise `_run_with_lock` + `_tracked_job` (which DO
  write `job_runs` rows) monkeypatch `settings.database_url` to
  `test_database_url()` from `tests/fixtures/ebull_test_db.py` and
  use the `ebull_test_conn` fixture for per-test cleanup. Pattern:

  ```python
  from tests.fixtures.ebull_test_db import test_database_url
  ...
  monkeypatch.setattr("app.config.settings.database_url", test_database_url())
  ```

  `_run_with_lock` reads `settings.database_url` directly (adapters.py
  line 103). `_tracked_job` reads it via `psycopg.connect(settings.database_url)`.
  Both follow the monkeypatched value.
- Add the same `pytestmark = pytest.mark.xdist_group(name="joblock_source_serial")`
  marker as `test_joblock_per_source.py` to serialise against other
  JobLock tests on a single xdist worker.

Skeleton:

```python
"""#1184 — JobLock per-source re-entrancy regression gate."""
from __future__ import annotations

import threading
from unittest.mock import patch

import psycopg
import pytest

from app.config import settings
from app.jobs.locks import _HELD_SOURCES, JobAlreadyRunning, JobLock

pytestmark = pytest.mark.xdist_group(name="joblock_source_serial")


def test_same_source_reentrant_bypasses_pg_lock() -> None:
    # outer = orchestrator_full_sync (db) + inner = fx_rates_refresh (db).
    # patch psycopg.connect at app.jobs.locks call site, count calls.
    # First JobLock acquires; second must not call connect (bypass).

def test_different_source_still_acquires_real_pg_lock() -> None:
    # outer = orchestrator_full_sync (db); inner = daily_portfolio_sync (etoro).
    # From a second raw psycopg.connect, run pg_try_advisory_lock on
    # hashtext('job_source:etoro')::int and expect False.

def test_orchestrator_outer_holds_db_inner_db_adapter_runs(
    monkeypatch, ebull_test_conn
) -> None:
    # monkeypatch settings.database_url -> test_database_url()
    # acquire outer JobLock(orchestrator_full_sync)
    # call adapters._run_with_lock(job_name="fx_rates_refresh", legacy_fn=fake_fn)
    # fake_fn uses _tracked_job to write status='success' row_count=1
    # assert returned tuple[0] == LayerOutcome.SUCCESS (not PREREQ_SKIP)

def test_sync_http_path_inner_lock_serialises_against_manual(
    monkeypatch, ebull_test_conn
) -> None:
    # NO outer JobLock acquired.
    # Open a SECOND raw psycopg.connect on test_database_url() and
    # SELECT pg_try_advisory_lock(hashtext('job_source:db')::int) → True
    # Call adapters._run_with_lock("fx_rates_refresh", fake_fn).
    # assert returned str startswith "legacy cron holder active"
    # assert fake_fn was never called.
    # Release the side lock with pg_advisory_unlock.

def test_reset_restores_prior_held_set_on_exception(
    monkeypatch
) -> None:
    # acquire outer JobLock(orchestrator_full_sync, db) → _HELD_SOURCES == {db}
    # patch psycopg.connect inside JobLock.__enter__ to raise for inner
    # try acquire inner JobLock(daily_portfolio_sync, etoro) → raises
    # outer __exit__ → assert _HELD_SOURCES.get() == frozenset()

def test_test_only_per_name_acquires_never_treated_as_reentrant() -> None:
    # JobLock.test_only_per_name(url, "fake_job_a") inside outer
    # JobLock(orchestrator_full_sync, db) — even though _HELD_SOURCES
    # contains 'db', the test_only_per_name acquire takes the real
    # raw-keyed advisory lock (its _source is None so bypass check
    # short-circuits). A second test_only_per_name(url, "fake_job_a")
    # acquire on a side connection raises JobAlreadyRunning.
```

### 5.2 T4b — `tests/test_orchestrator_adapter_morning_candidate_review.py` (NEW)

```python
"""#1184 — morning_candidate_review source-registry coverage.

The composite orchestrator adapter `refresh_scoring_and_recommendations`
acquires `JobLock(database_url, JOB_MORNING_CANDIDATE_REVIEW)` directly
(not via `_run_with_lock`). Without a source-registry entry the
acquisition KeyErrors at construction. This test pins the registry
entry so a future cleanup pass can't drop it silently.
"""
from app.jobs.sources import source_for


def test_morning_candidate_review_resolves_to_db_source() -> None:
    assert source_for("morning_candidate_review") == "db"
```

### 5.3 T4c — `tests/test_job_registry.py` — pinned-list update

`TestOrchestratorAdapterSourceCoverage::test_known_orchestrator_adapter_targets_covered`
expected dict gets one new entry. Bump the per-entry comment to note
that morning_candidate_review reaches via the composite adapter, not
`_run_with_lock`.

### 5.4 Test-only escape hatch handling

`test_only_per_name` keeps its existing behaviour (raw job_name keyed,
no re-entrancy). New documentation in its docstring + the test (f)
in §5.1 above pins the "test-only escape hatch does not pollute
production re-entrancy" contract.

The test (f) sketch in §5.1 acquires two `test_only_per_name` instances
on DIFFERENT psycopg sessions (each `JobLock.__enter__` opens its own
connection). Same raw key from a different session → real Postgres
collision → second raises `JobAlreadyRunning`. The acquire is also
verified to occur INSIDE an outer production `JobLock(orchestrator_full_sync, db)`
so the assertion is "even with `_HELD_SOURCES = {db}` in context, a
`test_only_per_name` acquire (whose `_source is None`) DOES go to
Postgres and DOES collide with a sibling `test_only_per_name` acquire
keyed on the same raw job_name."

## 6. T5 — Prevention-log update

### 6.1 Files touched

- `docs/review-prevention-log.md` — amend the existing #1183 entry
  (line 1329+).

### 6.2 Diff intent

Strike the "Limitation" paragraph at line 1333. Replace with:

```text
- Resolution (2026-05-17, #1184): JobLock now detects same-source
  re-entrancy via a process-local ContextVar (`_HELD_SOURCES` in
  `app/jobs/locks.py`). The outer orchestrator JobLock's source is
  recognised by inner adapter acquisitions in the same call context;
  the redundant Postgres acquire is bypassed (no self-skip). The
  source-lock contract is unchanged for cross-process / cross-context
  acquisitions — manual triggers in the same source still serialise.
  Tests: `tests/test_job_lock_reentrancy.py` (6 cases) +
  `tests/test_orchestrator_adapter_morning_candidate_review.py` +
  rewritten same-context tests in `tests/test_joblock_per_source.py`
  + `tests/test_db_lane_family_split.py` to assert cross-thread
  serialisation instead of same-context contention.
```

## 7. T6 — Local gates

```bash
uv run ruff check app/jobs/locks.py app/jobs/sources.py app/services/sync_orchestrator/adapters.py tests/test_job_lock_reentrancy.py tests/test_orchestrator_adapter_morning_candidate_review.py tests/test_job_registry.py tests/test_joblock_per_source.py tests/test_db_lane_family_split.py tests/test_jobs_locks.py
uv run ruff format --check app/jobs/locks.py app/jobs/sources.py tests/test_job_lock_reentrancy.py tests/test_orchestrator_adapter_morning_candidate_review.py tests/test_joblock_per_source.py tests/test_db_lane_family_split.py
uv run pyright app/jobs/locks.py app/jobs/sources.py app/services/sync_orchestrator/adapters.py
uv run pytest -n0 \
    tests/test_job_lock_reentrancy.py \
    tests/test_orchestrator_adapter_morning_candidate_review.py \
    tests/test_job_registry.py \
    tests/test_jobs_locks.py \
    tests/test_joblock_per_source.py \
    tests/test_db_lane_family_split.py \
    tests/test_universal_gate_carve_out.py \
    tests/test_layer_123_wiring.py
```

`tests/test_jobs_locks.py` (existing JobLock unit tests) +
`tests/test_joblock_per_source.py` (per-source contention semantics) +
`tests/test_db_lane_family_split.py` (db-lane family split serialisation)
are included because §2.2 modifies `JobLock.__enter__` / `__exit__`
directly AND §2.5 rewrites their same-context same-source serialisation
cases. Must not regress.

## 8. T7 — Codex 2 pre-push review

```bash
codex exec review
```

Standard pre-push diff review. Address findings before push.

## 9. T8 — Push + PR + poll

PR title: `fix(#1184): JobLock per-source re-entrancy via ContextVar`

PR body: spec link + spec §3 (Goals) + risk-table summary + test list.
Test plan section enumerates §6.6.2 (a-f).

Post-push: poll `gh pr view <n> --comments` + `gh pr checks <n>` until
Claude review posts + CI green. Resolve every comment with
`FIXED {sha}` / `DEFERRED #{num}` / `REBUTTED {reason}`.

## 10. T9 — Post-merge re-smoke (operator step)

Operator-side per `feedback_no_sleepy_claude.md` — DO NOT
ScheduleWakeup mid-cycle. Operator restarts jobs process, watches
next HF tick (5 min) + next FULL tick (03:00 UTC), confirms
`job_runs` rows land `status='success'` for the 4 db-lane targets.

## 11. Definition of done

- [ ] T1-T6 complete; all four pre-push gates green on impacted files.
- [ ] T7 Codex 2 review CLEAN or all findings addressed.
- [ ] PR opened; Claude review APPROVE on most recent commit; CI green.
- [ ] PR merged.
- [ ] T9 operator re-smoke confirms unblock.
- [ ] Memory updates: `[[us-source-coverage]]` +
  `[[1183-orchestrator-adapter-partial-fix]]` reflect the unblock;
  this PR closes the #1184 limitation note.
