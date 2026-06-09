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


def test_two_threads_share_floor(sec_gate_pool):
    floor = 0.05
    gate = PostgresFloorGate(sec_gate_pool, budget="sec", floor_s=floor)
    fire_times: list[float] = []
    lock = threading.Lock()

    def worker():
        for _ in range(5):
            gate.acquire()
            with lock:
                fire_times.append(time.monotonic())

    threads = [threading.Thread(target=worker) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    fire_times.sort()
    gaps = [b - a for a, b in zip(fire_times, fire_times[1:])]
    # Reservation spacing is strict; allow a small jitter tolerance on the
    # observed emission gaps but assert the floor is broadly honoured.
    assert min(gaps) >= floor * 0.5
    assert sum(gaps) >= floor * (len(fire_times) - 1) * 0.8


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
