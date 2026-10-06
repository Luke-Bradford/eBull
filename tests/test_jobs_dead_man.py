"""Pure-policy tests for the jobs dead-man (#3614 item 3).

Only ``step`` and the state-file round trip are exercised; no database, no
network, no osascript.
"""

from __future__ import annotations

from pathlib import Path

from scripts.jobs_dead_man import State, load_state, save_state, step

_NOW = 1_000_000.0
_STALE = 1800.0
_RENOTIFY = 7200.0


def _step(prior: State, *, observed: float | None, error: str | None = None, now: float = _NOW):
    return step(prior, observed_start=observed, read_error=error, now=now, stale_after_s=_STALE, renotify_s=_RENOTIFY)


def test_fresh_start_is_healthy() -> None:
    state, action = _step(State(), observed=_NOW - 300)
    assert action is None
    assert not state.alerting
    assert state.last_seen_start == _NOW - 300


def test_exactly_at_threshold_is_healthy() -> None:
    _, action = _step(State(), observed=_NOW - _STALE)
    assert action is None


def test_stale_start_alerts_once_then_holds_until_renotify() -> None:
    state, action = _step(State(), observed=_NOW - _STALE - 1)
    assert action == "alert"
    assert state.alerting
    assert state.reason is not None and "no job has started for 30 min" in state.reason

    state, action = _step(state, observed=_NOW - _STALE - 1, now=_NOW + 300)
    assert action is None
    assert state.alerting

    state, action = _step(state, observed=_NOW - _STALE - 1, now=_NOW + _RENOTIFY)
    assert action == "renotify"
    assert state.last_notified_at == _NOW + _RENOTIFY


def test_recovery_sends_one_notice() -> None:
    alerting, _ = _step(State(), observed=_NOW - 5000)
    state, action = _step(alerting, observed=_NOW + 60, now=_NOW + 120)
    assert action == "recover"
    assert not state.alerting
    assert state.reason is None

    _, action = _step(state, observed=_NOW + 360, now=_NOW + 420)
    assert action is None


def test_unreadable_db_uses_last_seen_start_as_evidence() -> None:
    seen, _ = _step(State(), observed=_NOW - 60)
    # DB goes away: one failed read must not page on its own...
    state, action = _step(seen, observed=None, error="OperationalError", now=_NOW + 60)
    assert action is None
    assert state.last_seen_start == _NOW - 60
    # ...but once the last known start is older than the threshold, it does.
    state, action = _step(state, observed=None, error="OperationalError", now=_NOW + _STALE)
    assert action == "alert"
    assert state.reason is not None
    assert "job_runs unreadable: OperationalError" in state.reason


def test_unreadable_with_no_history_pages_after_the_blind_streak() -> None:
    state, action = _step(State(), observed=None, error="OperationalError")
    assert action is None
    assert state.first_unread_at == _NOW

    state, action = _step(state, observed=None, error="OperationalError", now=_NOW + _STALE / 2)
    assert action is None
    assert state.first_unread_at == _NOW  # streak start is kept, not reset

    state, action = _step(state, observed=None, error="OperationalError", now=_NOW + _STALE + 1)
    assert action == "alert"
    assert state.reason is not None and state.reason.startswith("no job start observed")


def test_a_successful_read_clears_the_blind_streak() -> None:
    blind, _ = _step(State(), observed=None, error="OperationalError")
    state, _ = _step(blind, observed=_NOW + 30, now=_NOW + 60)
    assert state.first_unread_at is None


def test_state_round_trips_and_a_corrupt_file_resets(tmp_path: Path) -> None:
    path = tmp_path / "status.json"
    state, _ = _step(State(), observed=_NOW - 5000)
    save_state(path, state)
    assert load_state(path) == state

    path.write_text("{not json")
    assert load_state(path) == State()
    assert load_state(tmp_path / "missing.json") == State()
