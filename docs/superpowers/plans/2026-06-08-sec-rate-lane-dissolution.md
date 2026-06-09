# sec_rate Lane Dissolution Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the 1-wide pg-advisory `JobLock` `sec_rate` lane with an in-process bounded SEC-job concurrency gate (semaphore width 4 + per-job-name lock), ending the recurring lane-starvation (#1534/#1538/#1540) and closing #1536.

**Architecture:** `JobLock` keeps its pg-advisory path for every lane except `sec_rate`. For `sec_rate` it acquires an in-process `SecLaneGate` (a `threading.BoundedSemaphore(4)` + a per-job-name `threading.Lock`) instead of opening a Postgres connection — so the ~22 sec_rate jobs run up to 4-concurrent and each drops its per-job `JobLock` connection. A full gate raises `JobAlreadyRunning`, which the existing #1538 retry wrapper rides out for scheduled fires. A prerequisite (Task A) adds a per-accession advisory lock so the 13F live-ingest and CUSIP-rewash paths can't corrupt `institutional_holdings` when they hit the same accession concurrently (the operator-retry window, already reachable cross-lane today).

**Tech Stack:** Python 3.13, psycopg3, APScheduler (BackgroundScheduler, executor pool 10), Postgres 17, pytest (fast tier `-m "not db"`), `uv`.

**Spec:** `docs/specs/ops/2026-06-08-sec-rate-lane-dissolution.md`.

---

## File structure

| File | Responsibility | Action |
|---|---|---|
| `app/jobs/sec_lane_gate.py` | **NEW** — pure (threading-only) in-process gate: `SEC_LANE_MAX_CONCURRENCY`, `SecLaneGate`, the `SEC_LANE_GATE` singleton, `reset_for_tests`. Leaf module (no app imports) so it is fast-tier testable and importable by both `locks.py` and `pg_settings.py` without a cycle. | create |
| `app/jobs/locks.py` | `JobLock` — add the `sec_rate` branch that uses `SEC_LANE_GATE` instead of the pg-advisory connection. | modify |
| `app/db/pg_settings.py` | `_dev_profile_connection_demand` — charge the N concurrent sec_rate job bodies (they no longer charge a JobLock conn). | modify |
| `app/services/institutional_holdings.py` | **Task A** — `acquire_13f_accession_write_lock` helper + acquire it before the live-ingest holdings write. | modify |
| `app/services/rewash_filings.py` | **Task A** — acquire the same lock before the rewash `DELETE+INSERT`. | modify |
| `tests/jobs/test_sec_lane_gate.py` | **NEW** — fast-tier table-test of `SecLaneGate`. | create |
| `tests/jobs/test_job_lock_sec_rate.py` | **NEW** — `JobLock` sec_rate branch (db-tier by auto-marker; no real DB needed). | create |
| `tests/services/test_13f_accession_lock.py` | **NEW** — Task A helper SQL/key unit test. | create |
| `tests/db/test_connection_budget_sec_lane.py` | **NEW** — budget assertion at N=4. | create |
| `tests/jobs/test_sec_rate_membership.py` | **NEW** — pins the resolved sec_rate member set. | create |

Task order: **Task A first** (prerequisite write-safety guard), then the lane swap (Tasks 2–6). Two logical PRs are acceptable (Task A standalone, then the swap) or one PR with the commits in this order.

---

## Task 1: `SecLaneGate` — the pure in-process gate

**Files:**
- Create: `app/jobs/sec_lane_gate.py`
- Test: `tests/jobs/test_sec_lane_gate.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/jobs/test_sec_lane_gate.py
"""Fast-tier (no DB) table-test of the in-process sec_rate concurrency gate (#1542)."""
from app.jobs.sec_lane_gate import SecLaneGate


def test_free_slot_acquires():
    gate = SecLaneGate(2)
    assert gate.try_acquire("a") is True


def test_same_name_is_rejected_without_consuming_a_slot():
    gate = SecLaneGate(2)
    assert gate.try_acquire("a") is True
    # second acquire of the SAME name fails (per-job-name lock) ...
    assert gate.try_acquire("a") is False
    # ... and did NOT consume a count slot: a different name still gets in twice.
    assert gate.try_acquire("b") is True
    assert gate.try_acquire("c") is False  # now both of the 2 slots are held (a, b)


def test_all_slots_busy_rejects():
    gate = SecLaneGate(2)
    assert gate.try_acquire("a") is True
    assert gate.try_acquire("b") is True
    assert gate.try_acquire("c") is False  # full


def test_release_returns_the_slot_and_frees_the_name():
    gate = SecLaneGate(1)
    assert gate.try_acquire("a") is True
    assert gate.try_acquire("a") is False
    gate.release("a")
    assert gate.try_acquire("a") is True  # name + slot both freed


def test_release_of_unheld_name_does_not_raise_value_error_on_semaphore():
    # BoundedSemaphore over-release raises ValueError; guard against releasing
    # a gate that was never acquired (defensive — callers must pair acquire/release).
    gate = SecLaneGate(1)
    gate.try_acquire("a")
    gate.release("a")
    # re-acquire proves state is balanced
    assert gate.try_acquire("a") is True
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/jobs/test_sec_lane_gate.py -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'app.jobs.sec_lane_gate'`

- [ ] **Step 3: Write the module**

```python
# app/jobs/sec_lane_gate.py
"""In-process concurrency gate for the dissolved ``sec_rate`` JobLock lane (#1542).

The old lane was a 1-wide Postgres advisory mutex shared by ~22 SEC jobs — it
serialised them to one-at-a-time and starved the losers (#1534/#1538/#1540).
The SEC 10 req/s ceiling is enforced separately at the HTTP layer
(``_PROCESS_RATE_LIMIT_CLOCK``), so the lane gave zero rate protection.

This gate replaces it with two in-process primitives (zero Postgres
connections — all sec_rate execution is single-process; see the spec §3c):

  * a count semaphore — up to ``SEC_LANE_MAX_CONCURRENCY`` sec_rate jobs at once;
  * a per-job-name lock — never two instances of the SAME job_name (what the
    single shared lane gave incidentally; load-bearing for the Form-3
    DELETE+INSERT and the 13F rewash).

Both acquires are NON-blocking; a failure makes ``JobLock`` raise
``JobAlreadyRunning``, which ``_fire_scheduled_with_lane_retry`` (#1538) rides
out with its bounded backoff for scheduled fires.

Leaf module (threading only) so ``app/db/pg_settings.py`` can read
``SEC_LANE_MAX_CONCURRENCY`` for the connection-budget model and the gate is
fast-tier testable without importing psycopg.
"""

from __future__ import annotations

import threading
from typing import Final

SEC_LANE_MAX_CONCURRENCY: Final[int] = 4
"""Max concurrent ``sec_rate`` jobs. Bounded by the dev connection budget
(#1472): each running sec_rate job now holds ONE body connection (no JobLock
conn), and ``app/db/pg_settings.py`` charges N of these. Raising N requires
re-checking ``_dev_profile_connection_demand`` against ``max_connections``."""


class SecLaneGate:
    """Non-blocking count + per-name gate. Thread-safe."""

    def __init__(self, max_concurrency: int) -> None:
        self._slots = threading.BoundedSemaphore(max_concurrency)
        self._names_guard = threading.Lock()
        self._name_locks: dict[str, threading.Lock] = {}

    def _name_lock(self, job_name: str) -> threading.Lock:
        with self._names_guard:
            lock = self._name_locks.get(job_name)
            if lock is None:
                lock = threading.Lock()
                self._name_locks[job_name] = lock
            return lock

    def try_acquire(self, job_name: str) -> bool:
        """Acquire a slot for ``job_name``. Returns False (no state changed) if
        the same job_name is already running OR all slots are busy."""
        name_lock = self._name_lock(job_name)
        if not name_lock.acquire(blocking=False):
            return False  # same job_name already running
        if not self._slots.acquire(blocking=False):
            name_lock.release()  # give the name back — took no slot
            return False  # all slots busy
        return True

    def release(self, job_name: str) -> None:
        """Release a previously-acquired slot. Must pair 1:1 with a True
        ``try_acquire``; releasing the count slot first, then the name."""
        self._slots.release()
        self._name_lock(job_name).release()


SEC_LANE_GATE: Final[SecLaneGate] = SecLaneGate(SEC_LANE_MAX_CONCURRENCY)
"""Process-wide singleton used by ``JobLock`` for every ``sec_rate`` job."""


def reset_for_tests() -> None:
    """Test-only: rebuild the singleton's internal state. Production never calls this."""
    global SEC_LANE_GATE
    SEC_LANE_GATE = SecLaneGate(SEC_LANE_MAX_CONCURRENCY)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/jobs/test_sec_lane_gate.py -q`
Expected: PASS (5 passed)

- [ ] **Step 5: Commit**

```bash
git add app/jobs/sec_lane_gate.py tests/jobs/test_sec_lane_gate.py
git commit -m "feat(#1542): in-process SecLaneGate (semaphore + per-name lock)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task A (do BEFORE Task 2): per-accession 13F write lock

Prerequisite write-safety guard. Live 13F ingest (`_upsert_holding`, `ON CONFLICT DO NOTHING`) and the CUSIP rewash (`DELETE FROM institutional_holdings ...` then re-INSERT) can both mutate the same accession in the operator-retry window (ingest-log row deleted, `unresolved_13f_cusips` row left). Today the lane serialises them; after the swap it would not. The fix: a per-accession `pg_advisory_xact_lock` shared by both paths.

**Files:**
- Modify: `app/services/institutional_holdings.py` (add helper + acquire before the holdings-write loop in `_ingest_single_accession`)
- Modify: `app/services/rewash_filings.py:1031` (acquire before the rewash `DELETE`)
- Test: `tests/services/test_13f_accession_lock.py`

- [ ] **Step 1: Write the failing test (helper issues the exact lock SQL + key)**

```python
# tests/services/test_13f_accession_lock.py
"""The 13F per-accession advisory lock helper issues the canonical key (#1542 Task A)."""
from unittest.mock import MagicMock

from app.services.institutional_holdings import acquire_13f_accession_write_lock


def test_helper_issues_xact_lock_with_accession_keyed_hash():
    conn = MagicMock()
    acquire_13f_accession_write_lock(conn, "0001234567-25-000001")
    assert conn.execute.call_count == 1
    sql, params = conn.execute.call_args.args
    # transaction-scoped advisory lock (auto-released on commit)
    assert "pg_advisory_xact_lock" in sql
    # namespaced + accession-keyed via hashtextextended XOR — same key in both call sites
    assert "hashtextextended('ingest_13f_accession', 0)" in sql
    assert "hashtextextended(%s, 0)" in sql
    assert params == ("0001234567-25-000001",)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/services/test_13f_accession_lock.py -q`
Expected: FAIL — `ImportError: cannot import name 'acquire_13f_accession_write_lock'`

- [ ] **Step 3: Add the helper to `app/services/institutional_holdings.py`**

Place it near the other module-level write helpers (e.g. just above `_upsert_holding`). `Any` is already imported in that module; if not, add `from typing import Any`.

```python
def acquire_13f_accession_write_lock(conn: "psycopg.Connection[Any]", accession_number: str) -> None:
    """Serialise concurrent writers of ONE 13F accession's ``institutional_holdings``
    rows (#1542 Task A).

    Live ingest (``_ingest_single_accession`` → ``_upsert_holding``, ON CONFLICT
    DO NOTHING) and the CUSIP rewash (``rewash_filings._apply_13f_infotable``,
    DELETE+INSERT) can both target the same accession in the operator-retry
    window (ingest-log row deleted to force re-ingest, ``unresolved_13f_cusips``
    row left) — and already run concurrently cross-lane (``cusip_extid_sweep`` on
    ``db_cusip``). This advisory lock makes the two mutations mutually exclusive
    per accession.

    Transaction-scoped: auto-releases on COMMIT/ROLLBACK. MUST be called inside
    the same (non-autocommit) transaction as the ``institutional_holdings``
    write, AFTER any SEC fetches, so the lock is never held across network I/O.
    Key derivation MUST match the rewash call site exactly.
    """
    conn.execute(
        "SELECT pg_advisory_xact_lock("
        "(hashtextextended('ingest_13f_accession', 0) # hashtextextended(%s, 0)))",
        (accession_number,),
    )
```

- [ ] **Step 4: Run the helper test to verify it passes**

Run: `uv run pytest tests/services/test_13f_accession_lock.py -q`
Expected: PASS (1 passed)

- [ ] **Step 5: Acquire the lock at the live-ingest holdings write**

In `_ingest_single_accession` (`app/services/institutional_holdings.py`), the SEC fetches + raw-doc commits finish by line ~1516; `_upsert_filer` runs (~1532), then the empty-holdings early-return (~1534), then the `for ... _upsert_holding(...)` loop that writes `institutional_holdings`. Read that loop and insert the acquire on the line **immediately before the loop**:

```python
    # #1542 Task A — serialise this accession's holdings write against a
    # concurrent CUSIP-rewash DELETE+INSERT. Acquired here (after all SEC
    # fetches, inside the holdings-write transaction) so the lock is held only
    # across the institutional_holdings mutation, not the network fetches.
    acquire_13f_accession_write_lock(conn, ref.accession_number)
    for holding in holdings:                 # existing loop — do not change
        _upsert_holding(conn, ...)           # existing call — do not change
```

**Invariant to verify while editing:** no `conn.commit()` sits between this acquire and the loop's trailing `conn.commit()` — the lock must hold across the whole holdings mutation. (The `conn.commit()` calls at lines ~1454 and ~1516 are BEFORE this point and are fine.)

- [ ] **Step 6: Acquire the same lock before the rewash DELETE**

In `app/services/rewash_filings.py`, `_apply_13f_infotable`, the "All CUSIPs resolved — safe to replace-then-insert" block does the DELETE at line ~1031–1035. Add the import at the top of the file:

```python
from app.services.institutional_holdings import acquire_13f_accession_write_lock
```

Then insert the acquire immediately before the DELETE:

```python
    # All CUSIPs resolved — safe to replace-then-insert.
    # #1542 Task A — same per-accession lock as live ingest; serialises this
    # DELETE+INSERT against a concurrent _ingest_single_accession holdings write.
    acquire_13f_accession_write_lock(conn, raw_doc.accession_number)
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM institutional_holdings WHERE accession_number = %s",
            (raw_doc.accession_number,),
        )
```

Verify `app/services/rewash_filings.py` does not already import `institutional_holdings` in a way that would now cycle — it imports `_upsert_holding` FROM `institutional_holdings` already (so the dependency direction is rewash → institutional_holdings; institutional_holdings does NOT import rewash). No cycle.

- [ ] **Step 7: Run gates for the touched files**

Run: `uv run ruff check app/services/institutional_holdings.py app/services/rewash_filings.py && uv run pyright app/services/institutional_holdings.py app/services/rewash_filings.py && uv run pytest tests/services/test_13f_accession_lock.py -q`
Expected: clean + PASS

- [ ] **Step 8: Commit**

```bash
git add app/services/institutional_holdings.py app/services/rewash_filings.py tests/services/test_13f_accession_lock.py
git commit -m "fix(#1542): per-accession 13F advisory lock (ingest vs rewash) [Task A]

Closes the institutional_holdings race between live 13F ingest and the
CUSIP-rewash DELETE+INSERT in the operator-retry window (also reachable
cross-lane today via cusip_extid_sweep). Prerequisite for the sec_rate
lane dissolution.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: wire the `sec_rate` branch into `JobLock`

**Files:**
- Modify: `app/jobs/locks.py` (`__init__`, `__enter__`, `__exit__`)
- Test: `tests/jobs/test_job_lock_sec_rate.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/jobs/test_job_lock_sec_rate.py
"""JobLock sec_rate branch: in-process gate, no Postgres connection (#1542).

``source_for`` is monkeypatched so these tests do NOT build the job registry
(which has a pre-existing cold-import cycle, unrelated to #1542); the branch
under test only needs the resolved source to equal 'sec_rate'.
"""
import threading
from unittest.mock import patch

import pytest

from app.jobs import sec_lane_gate
from app.jobs.locks import JobAlreadyRunning, JobLock


@pytest.fixture(autouse=True)
def _sec_rate_source_and_fresh_gate(monkeypatch):
    # Avoid the registry build/cycle; route every job_name to source 'sec_rate'.
    monkeypatch.setattr("app.jobs.locks.source_for", lambda job_name: "sec_rate")
    sec_lane_gate.reset_for_tests()
    yield
    sec_lane_gate.reset_for_tests()


def test_sec_rate_job_opens_no_db_connection():
    with patch("app.jobs.locks.psycopg.connect") as mock_connect:
        with JobLock("postgresql://unused", "sec_atom_fast_lane"):
            pass
        mock_connect.assert_not_called()


def test_same_sec_job_name_from_another_thread_raises():
    # Same-context re-entry would hit the #1184 bypass and NOT raise; genuine
    # same-name contention must come from a second thread (its own _HELD_SOURCES).
    holding = threading.Event()
    may_release = threading.Event()

    def hold():
        with JobLock("postgresql://unused", "sec_x"):
            holding.set()
            may_release.wait(timeout=5)

    t = threading.Thread(target=hold)
    t.start()
    try:
        assert holding.wait(timeout=5)
        with pytest.raises(JobAlreadyRunning):
            with JobLock("postgresql://unused", "sec_x"):
                pass
    finally:
        may_release.set()
        t.join(timeout=5)


def test_release_frees_the_slot():
    with JobLock("postgresql://unused", "sec_atom_fast_lane"):
        pass
    with JobLock("postgresql://unused", "sec_atom_fast_lane"):  # name free again
        pass
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/jobs/test_job_lock_sec_rate.py -q`
Expected: FAIL — `test_sec_rate_job_opens_no_db_connection` fails: `psycopg.connect` WAS called (current code opens a conn for sec_rate).

- [ ] **Step 3: Add the import + the `__init__` flag in `app/jobs/locks.py`**

At the top of `app/jobs/locks.py`, with the other imports — import the **module**, not the singleton, so a test `reset_for_tests()` (which rebinds `sec_lane_gate.SEC_LANE_GATE`) is visible here:

```python
from app.jobs import sec_lane_gate
```

In `JobLock.__init__`, after `self._held_token: Token[frozenset[Lane]] | None = None` (~line 267), add:

```python
        # #1542 — set when this acquire took the in-process sec_rate gate
        # (the sec_rate lane no longer opens a Postgres advisory connection).
        self._sec_lane_held: bool = False
```

- [ ] **Step 4: Add the branch in `__enter__`**

In `JobLock.__enter__`, immediately AFTER the #1184 re-entrancy short-circuit (the block that ends `self._reentrant = True; return self`, ~line 307) and BEFORE `conn = psycopg.connect(...)` (~line 314), insert:

```python
        # #1542 — sec_rate is an in-process gate, NOT a pg-advisory lock.
        # Up to SEC_LANE_MAX_CONCURRENCY jobs run concurrently; a full gate (or a
        # same-name overlap) raises JobAlreadyRunning, which the #1538 retry
        # wrapper rides out for scheduled fires. No psycopg connection is opened.
        if self._source == "sec_rate":
            if not sec_lane_gate.SEC_LANE_GATE.try_acquire(self._job_name):
                raise JobAlreadyRunning(self._job_name)
            self._sec_lane_held = True
            new_held: frozenset[Lane] = held | frozenset[Lane]({self._source})
            self._held_token = _HELD_SOURCES.set(new_held)
            return self
```

(`held` is already in scope from the re-entrancy check above. `new_held` shadows the name used in the pg-advisory branch below — acceptable, both branches return before the other runs.)

- [ ] **Step 5: Add the release in `__exit__`**

In `JobLock.__exit__`, after the `_HELD_SOURCES` reset block (the `if self._held_token is not None:` block, ~line 356–360) and BEFORE `conn = self._conn` (~line 361), insert:

```python
        # #1542 — sec_rate gate release (no connection was opened).
        if self._sec_lane_held:
            self._sec_lane_held = False
            sec_lane_gate.SEC_LANE_GATE.release(self._job_name)
            return
```

- [ ] **Step 6: Run test to verify it passes**

Run: `uv run pytest tests/jobs/test_job_lock_sec_rate.py -q`
Expected: PASS (3 passed). (This test is `db`-marked by the collection auto-marker because `app/jobs/locks.py` imports `psycopg`; it needs no real DB but runs in the `-m db` tier. Run it explicitly as above.)

- [ ] **Step 7: Verify non-sec lanes are unchanged**

Run: `uv run pytest tests/ -q -k "job_lock or joblock" -m db`
Expected: existing JobLock tests still PASS (the pg-advisory path for non-sec lanes is untouched).

- [ ] **Step 8: Commit**

```bash
git add app/jobs/locks.py tests/jobs/test_job_lock_sec_rate.py
git commit -m "feat(#1542): JobLock routes sec_rate to the in-process gate

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: connection-budget accounting

**Files:**
- Modify: `app/db/pg_settings.py:256-265` (`_dev_profile_connection_demand`)
- Test: `tests/db/test_connection_budget_sec_lane.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/db/test_connection_budget_sec_lane.py
"""sec_rate dissolution charges N concurrent job bodies; still fits the dev budget (#1542).

Fast-tier (no DB): the auto-marker only marks a module `db` when its SOURCE
mentions psycopg.connect / TestClient / the test-DB URL — this one does not.
"""
from app.db import pg_settings
from app.jobs.sec_lane_gate import SEC_LANE_MAX_CONCURRENCY

_DEV_USABLE = 27  # dev box: max_connections=30 − superuser_reserved_connections=3


def test_demand_is_exactly_the_known_terms_plus_sec_lane_bodies():
    # Recompute from the same constants so the test fails BEFORE the edit
    # (function returns the 8-term baseline) and passes AFTER (+ the sec term).
    expected = (
        pg_settings.DB_POOL_MAX_SIZE
        + pg_settings.AUDIT_POOL_MAX_SIZE
        + pg_settings.API_FIXED_LONGLIVED_CONNS
        + pg_settings.JOBS_POOL_MAX_SIZE
        + pg_settings.BACKGROUND_POOL_MAX_SIZE
        + pg_settings.JOBS_FIXED_LONGLIVED_CONNS
        + pg_settings.JOBS_STEADY_STATE_EXEC_CONNS
        + pg_settings.ORCHESTRATOR_GATE_CHECK_CONN
        + SEC_LANE_MAX_CONCURRENCY
    )
    assert pg_settings._dev_profile_connection_demand() == expected


def test_demand_plus_reserve_fits_usable_with_margin():
    demand = pg_settings._dev_profile_connection_demand() + pg_settings.CONNECTION_BUDGET_RESERVE
    assert demand <= _DEV_USABLE, f"demand {demand} > usable {_DEV_USABLE}"
    assert _DEV_USABLE - demand >= 1, "no connection margin left"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/db/test_connection_budget_sec_lane.py -q`
Expected: FAIL — `test_demand_is_exactly_the_known_terms_plus_sec_lane_bodies` fails (function returns the 8-term baseline 17, expected 21).

- [ ] **Step 3: Edit `_dev_profile_connection_demand`**

In `app/db/pg_settings.py`, update the function (lines 256–265) to add the sec-lane body term. Import the constant lazily inside the function (defensive against any import-order edge):

```python
def _dev_profile_connection_demand() -> int:
    """... (keep the existing docstring; append:)

    #1542 — the ``sec_rate`` lane is now an in-process semaphore (no per-job
    JobLock connection). Up to ``SEC_LANE_MAX_CONCURRENCY`` sec_rate job BODIES
    run concurrently, each holding ONE raw body connection. Charge all N
    explicitly: before #1542 only one job ran at a time (its body conn absorbed
    by the reserve); now N>1 sec bodies are first-class demand.
    """
    from app.jobs.sec_lane_gate import SEC_LANE_MAX_CONCURRENCY

    return (
        DB_POOL_MAX_SIZE
        + AUDIT_POOL_MAX_SIZE
        + API_FIXED_LONGLIVED_CONNS
        + JOBS_POOL_MAX_SIZE
        + BACKGROUND_POOL_MAX_SIZE
        + JOBS_FIXED_LONGLIVED_CONNS
        + JOBS_STEADY_STATE_EXEC_CONNS
        + ORCHESTRATOR_GATE_CHECK_CONN
        + SEC_LANE_MAX_CONCURRENCY
    )
```

Expected resulting value: previous 17 + 4 = **21**; `21 + CONNECTION_BUDGET_RESERVE(3) = 24 ≤ 27`, margin 3. (The reserve still absorbs the ~2 non-sec body conns, so the real worst case ≈ 26 ≤ 27 — see spec §3b.)

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/db/test_connection_budget_sec_lane.py -q`
Expected: PASS (2 passed)

- [ ] **Step 5: Run the existing budget test (no regression)**

Run: `uv run pytest tests/ -q -k "connection_budget or pg_settings"`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add app/db/pg_settings.py tests/db/test_connection_budget_sec_lane.py
git commit -m "feat(#1542): charge N sec_rate bodies in the dev connection budget

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: pin the sec_rate membership set

**Files:**
- Test: `tests/jobs/test_sec_rate_membership.py`

- [ ] **Step 1: Write the test (a golden set — any future sec_rate addition fails until audited)**

```python
# tests/jobs/test_sec_rate_membership.py
"""Freeze the resolved sec_rate member set so a future addition is caught and
write-safety-audited before it silently inherits the new concurrency (#1542)."""
import pytest

import app.main  # noqa: F401 — full boot-order import resolves a pre-existing cold-import
#                  cycle in the registry (insider_transactions <-> insider_form3_ingest);
#                  unrelated to #1542. Building the registry standalone hits it.
from app.jobs.sources import get_job_name_to_source

# db-tier: builds the full registry (imports scheduler + bootstrap_orchestrator).
# The auto-marker does NOT mark this module (its source has no psycopg.connect /
# TestClient), so mark it explicitly.
pytestmark = pytest.mark.db

# Generated 2026-06-08 via: source_for over the full registry (spec §3d).
# 22 members. Adding/removing a sec_rate job MUST update this set AND re-run the
# write-safety audit for the new member (spec §3a).
EXPECTED_SEC_RATE_MEMBERS = frozenset({
    "cusip_universe_backfill",
    "daily_cik_refresh",
    "daily_research_refresh",
    "filings_history_seed",
    "mf_directory_sync",
    "ncen_classifier_yearly",
    "sec_13f_filer_directory_sync",
    "sec_13f_quarterly_sweep",
    "sec_8k_events_ingest",
    "sec_atom_fast_lane",
    "sec_business_summary_bootstrap",
    "sec_daily_index_reconcile",
    "sec_def14a_bootstrap",
    "sec_first_install_drain",
    "sec_form3_ingest",
    "sec_master_idx_gap_close",
    "sec_master_idx_quarterly_sweep",
    "sec_n_csr_bootstrap_drain",
    "sec_n_port_ingest",
    "sec_nport_filer_directory_sync",
    "sec_rebuild",
    "sec_submissions_files_walk",
})


def test_sec_rate_membership_is_frozen():
    resolved = {j for j, s in get_job_name_to_source().items() if s == "sec_rate"}
    assert resolved == EXPECTED_SEC_RATE_MEMBERS
```

- [ ] **Step 2: Run the test**

Run: `uv run pytest tests/jobs/test_sec_rate_membership.py -q` (db-tier; needs `docker compose --profile test up -d postgres-test` running — see Task 5 Step 2)
Expected: PASS (1 passed). If it FAILS, the registry changed — print the diff (`resolved ^ EXPECTED_SEC_RATE_MEMBERS`), audit the new member's write-set per spec §3a, then update the set.

- [ ] **Step 3: Commit**

```bash
git add tests/jobs/test_sec_rate_membership.py
git commit -m "test(#1542): pin the resolved sec_rate member set (22)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 5: full local gates + push prep

- [ ] **Step 1: Run the fast tier + format/lint/type gates**

```bash
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv run pytest -m "not db"
uv run pytest tests/smoke
```
Expected: all clean / PASS.

- [ ] **Step 2: Run the new DB-tier tests deliberately (they were excluded above)**

```bash
docker compose --profile test up -d postgres-test
uv run pytest tests/jobs/test_job_lock_sec_rate.py tests/db/test_connection_budget_sec_lane.py tests/jobs/test_sec_rate_membership.py -q
```
Expected: PASS. (These are `db`-marked by the collection auto-marker because their modules import psycopg / the registry; none needs real data, but they run in the `-m db` tier.)

- [ ] **Step 3: Codex checkpoint-2 (before first push — MANDATORY per CLAUDE.md)**

```bash
codex exec --skip-git-repo-check "Review the branch diff for #1542 (sec_rate lane dissolution). Verify: (1) JobLock sec_rate branch opens no psycopg connection and releases the gate on every exit path incl exceptions; (2) the SecLaneGate acquire/release pairs balance and BoundedSemaphore is never over-released; (3) Task A's pg_advisory_xact_lock uses an IDENTICAL key in institutional_holdings.py and rewash_filings.py, is held across the full institutional_holdings mutation in each path, and is never held across a SEC fetch; (4) the connection-budget edit keeps demand+reserve <= 27. Terse, findings only with severity + file:line + fix."
```
Fix anything real, re-run Step 1, then proceed.

- [ ] **Step 4: Push + open PR** (follow the CLAUDE.md branch/PR workflow — poll review + CI, resolve every comment).

---

## Post-merge operator follow-up (NOT a code step — operator-run, per spec §8)

1. Restart the jobs process onto the merged SHA (approved method): `kill -9` the `python -m app.jobs` child + uv parent, confirm `JOBS_PROCESS_LOCK` free, `nohup uv run python -m app.jobs >/tmp/jobs.log 2>&1 </dev/null &`. Confirm single scheduler (one child PID + one `ebull-jobs-singleton-fence` backend).
2. Dev-verify: trigger two *different* sec_rate producers (e.g. `sec_atom_fast_lane` + a manual `sec_13f_quarterly_sweep`) and confirm overlapping `job_runs` rows (pre-#1542 the second would skip). Confirm a same-name second dispatch skips with `JobAlreadyRunning`. Confirm `/system/postgres-health` connection count stays under budget during a 4-concurrent window.
3. Close #1536 (no lane to wedge on).

---

## Self-review (run before dispatching)

- **Spec coverage:** §4.1–4.2 → Tasks 1–2; §3b conn-budget → Task 3; §3a Task A → Task A; §3d membership → Task 4; §7 tests → each task's test + Task 5; §8 rollout → post-merge section; §11 Codex findings → folded (Task A exists; membership pinned at 22 incl the 3 fallback members; bootstrap needs no code change). No spec requirement is unimplemented.
- **Placeholders:** none — every step shows exact code/commands. (The one read-and-insert step, Task A Step 5, gives the exact helper call + precise location + the invariant to check.)
- **Type/name consistency:** `SecLaneGate` / `SEC_LANE_GATE` / `SEC_LANE_MAX_CONCURRENCY` / `reset_for_tests` / `acquire_13f_accession_write_lock` used identically across tasks and tests.

**Codex checkpoint-1 (on this plan) — findings folded in:**
- HIGH: same-name JobLock test rewritten to two-thread contention (same-context re-entry hits the #1184 bypass and would not raise).
- HIGH: `locks.py` imports the `sec_lane_gate` **module** and references `sec_lane_gate.SEC_LANE_GATE` (not the singleton directly) so `reset_for_tests()` rebinding is visible.
- MED: budget test now asserts the exact modeled demand (8 baseline terms + `SEC_LANE_MAX_CONCURRENCY`), so it fails before the edit and passes after.
- MED: tests avoid the pre-existing registry cold-import cycle — JobLock tests monkeypatch `app.jobs.locks.source_for`; the membership test does `import app.main` first.
- LOW: membership test marked `pytestmark = pytest.mark.db` explicitly (auto-marker scans source, not transitive imports).

**Out of scope (noted, not fixed here):** the pre-existing `insider_transactions` ↔ `insider_form3_ingest` cold-import cycle (registry builds fine during normal app boot; only bites isolated cold imports). Worth its own ticket; not a #1542 change.
