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
    # white-box: the gate released both the slot and the name on exit.
    assert not sec_lane_gate.SEC_LANE_GATE._held
    with JobLock("postgresql://unused", "sec_atom_fast_lane"):  # name free again
        pass


def test_two_different_sec_rate_jobs_hold_slots_concurrently():
    # The dissolved lane no longer serialises DIFFERENT sec_rate job_names.
    # Two THREADS (each its own _HELD_SOURCES contextvar) — a same-context
    # nested acquire would hit the #1184 re-entrancy bypass and never reach the
    # gate, so concurrency MUST be proven cross-thread.
    a_in = threading.Event()
    b_in = threading.Event()
    release = threading.Event()

    def run(job_name, entered):
        with JobLock("postgresql://unused", job_name):
            entered.set()
            assert release.wait(timeout=5)

    ta = threading.Thread(target=run, args=("sec_job_a", a_in))
    tb = threading.Thread(target=run, args=("sec_job_b", b_in))
    ta.start()
    tb.start()
    try:
        assert a_in.wait(timeout=5)
        assert b_in.wait(timeout=5)  # both inside at once → gate is >1-wide on distinct names
    finally:
        release.set()
        ta.join(timeout=5)
        tb.join(timeout=5)
