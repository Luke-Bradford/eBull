# tests/db/test_postgres_rate_gate.py
import threading
import time

import pytest
from psycopg_pool import ConnectionPool

from app.providers.postgres_rate_gate import PostgresFloorGate
from tests.fixtures.ebull_test_db import test_database_url, test_db_available

pytestmark = pytest.mark.db


@pytest.fixture
def sec_gate_pool():
    # No shared `db_pool` fixture exists (repo DB tests use `ebull_test_conn`,
    # a single conn). This gate test needs CONCURRENT conns, so open a small
    # pool against the worker test DB and ensure the seed row exists.
    if not test_db_available():
        pytest.skip("ebull_test DB unavailable")
    pool = ConnectionPool(test_database_url(), min_size=2, max_size=4, open=True)
    with pool.connection() as conn:
        conn.execute("INSERT INTO sec_rate_gate (budget) VALUES ('sec') ON CONFLICT (budget) DO NOTHING")
        conn.commit()
    try:
        yield pool
    finally:
        pool.close()


def _gate_ladder(pool) -> tuple[float, float]:
    """``(next_free_at, clock_timestamp())`` for the sec budget, one DB clock."""
    with pool.connection() as conn:
        row = conn.execute(
            "SELECT EXTRACT(EPOCH FROM next_free_at), EXTRACT(EPOCH FROM clock_timestamp()) "
            "FROM sec_rate_gate WHERE budget = 'sec'"
        ).fetchone()
        conn.commit()
    assert row is not None
    return float(row[0]), float(row[1])


def _pin_ladder(pool) -> float:
    """Reset ``next_free_at`` to now and return it, on the DB clock.

    ⚠ Not tidiness — the assertions below are VACUOUS without it. A ladder left
    stale by an earlier run sits in the past, so the first reservation jumps it
    forward to `now`, and that jump alone can satisfy any lower bound on the
    total advance. Caught by the #2648 revert-probe: a gate mutated to advance
    HALF a floor per grant still passed `after - before >= floor * n`.
    """
    with pool.connection() as conn:
        row = conn.execute(
            "UPDATE sec_rate_gate SET next_free_at = clock_timestamp() WHERE budget = 'sec' "
            "RETURNING EXTRACT(EPOCH FROM next_free_at)"
        ).fetchone()
        conn.commit()
    assert row is not None
    return float(row[0])


def test_two_threads_share_floor(sec_gate_pool):
    """Two threads reserve against ONE ladder, and every grant costs a floor.

    ⚠⚠ This asserted emission gaps (`min(gaps) >= floor * 0.5`) until #2648, and
    failed deterministically on an idle box at ~350x under the floor. The gate was
    never at fault: `acquire` = a DB-clock grant plus `time.sleep(wait)`, and the
    sleep on this box returns 37-105 ms late single-threaded, 129-148 ms late with
    two threads (measured, loadavg 3.4 / 10 CPUs) against a 50 ms floor. Two grants
    one floor apart therefore EMIT together whenever the earlier thread oversleeps
    by more than the later one — so an emission-gap assertion measures
    `time.sleep`, not `PostgresFloorGate`.

    What is asserted instead is the invariant the gate actually establishes, read
    from the Postgres clock so no local timing enters: each reservation advances
    the shared `next_free_at` by at least one floor, and never by more than one
    floor beyond where the ladder had to start. `_sleep` is stubbed out so the
    upper bound stays tight — with real sleeps the whole run outlasts the ladder
    and the bound goes slack (it would admit a 2x over-advance).
    """
    floor = 0.05
    per_thread, n_threads = 5, 2
    waits: list[float] = []
    lock = threading.Lock()

    def record(wait: float) -> None:
        with lock:
            waits.append(wait)

    gate = PostgresFloorGate(sec_gate_pool, budget="sec", floor_s=floor, _sleep=record)
    before = _pin_ladder(sec_gate_pool)

    def worker():
        for _ in range(per_thread):
            gate.acquire()

    threads = [threading.Thread(target=worker) for _ in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    after, now_after = _gate_ladder(sec_gate_pool)
    n = per_thread * n_threads

    # Lower bound: `next_free_at = GREATEST(now, next_free_at) + floor` advances by
    # at least a floor per reservation, whatever the interleaving. Both threads
    # charge the SAME row, so a gate that gave each thread its own ladder — or one
    # that let a concurrent reservation read a stale value — advances this by less.
    assert after - before >= floor * n
    # Upper bound: no reservation may cost more than its floor. The ladder can only
    # restart from `now` when it has fallen behind, so `max(before, now_after)` is
    # the highest base any of the n steps could have had.
    assert after - max(before, now_after) <= floor * n
    # Every caller was made to wait for its own slot. Only the FIRST reservation
    # can be free (the ladder may start stale, in which case the SQL clamps its
    # wait to 0 and `acquire` skips the sleep) — so at most one of the n is absent.
    assert n - 1 <= len(waits) <= n
    assert all(w > 0 for w in waits)
    # With the sleeps stubbed the n reservations land in one burst, so the last
    # grant is roughly (n-1) floors out: the ladder is handed out, not reset.
    assert max(waits) >= floor * (n - 1) * 0.5


def test_acquire_actually_sleeps_for_its_grant(sec_gate_pool):
    """The wait is not merely computed — `acquire` blocks the caller for it.

    Stubbing `_sleep` in the ladder test above removes the only thing that makes a
    reservation cost the caller anything, so one test still has to run the real
    sleep path. Asserted as a LOWER bound on elapsed time, which oversleep can
    only satisfy harder.
    """
    floor = 0.05
    gate = PostgresFloorGate(sec_gate_pool, budget="sec", floor_s=floor)
    gate.acquire()  # may return immediately off a stale ladder
    t0 = time.monotonic()
    gate.acquire()
    gate.acquire()
    assert time.monotonic() - t0 >= floor * 2 * 0.9


def test_fallback_on_db_error():
    # A pool whose .connection() raises -> gate must fall back to the
    # in-process floor (no exception, request still paced).
    class BoomPool:
        def connection(self):
            raise RuntimeError("pool down")

    gate = PostgresFloorGate(BoomPool(), budget="sec", floor_s=0.02)
    t0 = time.monotonic()
    gate.acquire()
    gate.acquire()
    assert time.monotonic() - t0 >= 0.02  # second call paced by fallback
