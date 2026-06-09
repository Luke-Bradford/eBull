"""Fast-tier (no DB) table-test of the in-process sec_rate concurrency gate (#1542)."""

import pytest

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


def test_release_of_never_acquired_name_raises_without_touching_slots():
    gate = SecLaneGate(1)
    with pytest.raises(RuntimeError):
        gate.release("never_acquired")
    # the bad release must NOT have loosened the semaphore: one acquire fills it.
    assert gate.try_acquire("a") is True
    assert gate.try_acquire("b") is False


def test_is_held_tracks_acquire_release():
    gate = SecLaneGate(2)
    assert gate.is_held("a") is False
    assert gate.try_acquire("a") is True
    assert gate.is_held("a") is True
    assert gate.is_held("b") is False
    gate.release("a")
    assert gate.is_held("a") is False
