"""Pure-policy tests for the jobs dead-man (#3614 item 3).

``step``/``settle`` are pure; ``main`` runs with the DB read, the push and
osascript stubbed. No database, no network.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from scripts import jobs_dead_man
from scripts.jobs_dead_man import State, load_state, save_state, settle, step

_NOW = 1_000_000.0
# Captured at import, before the autouse stub below replaces it per test.
_REAL_RUN_ALERT_WATCH = jobs_dead_man.run_alert_watch


@pytest.fixture(autouse=True)
def _no_alert_watch(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """``main`` runs the refusal-surface watch, which reads the real DB; stub it."""
    calls: list[str] = []
    monkeypatch.setattr(jobs_dead_man, "run_alert_watch", lambda: calls.append("watch"))
    return calls


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


def test_an_undelivered_alert_is_retried_next_run() -> None:
    prior = State(last_seen_start=_NOW - 5000)
    intended, action = _step(prior, observed=_NOW - 5000)
    assert action == "alert"
    kept = settle(prior, intended, delivered=False)
    assert not kept.alerting
    assert kept.last_notified_at is None
    _, action = _step(kept, observed=_NOW - 5000, now=_NOW + 300)
    assert action == "alert"  # not deferred to the 2 h re-alert


def test_an_undelivered_recovery_is_retried_next_run() -> None:
    alerting, _ = _step(State(), observed=_NOW - 5000)
    intended, action = _step(alerting, observed=_NOW + 60, now=_NOW + 120)
    assert action == "recover"
    kept = settle(alerting, intended, delivered=False)
    assert kept.alerting
    _, action = _step(kept, observed=_NOW + 360, now=_NOW + 420)
    assert action == "recover"


def test_main_persists_dedup_only_after_delivery(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    status = tmp_path / "status.json"
    monkeypatch.setenv("EBULL_NTFY_TOPIC", "t")
    monkeypatch.setattr(jobs_dead_man, "read_last_start", lambda: (1.0, None))
    monkeypatch.setattr(jobs_dead_man, "local_notify", lambda *_a: True)
    sent: list[str] = []

    def _push(**kw: object) -> bool:
        sent.append(str(kw["title"]))
        return len(sent) > 1  # first attempt fails, second succeeds

    monkeypatch.setattr(jobs_dead_man, "send_push", _push)
    assert jobs_dead_man.main(["--status-file", str(status)]) == 3  # dark, page undelivered
    assert not load_state(status).alerting
    assert jobs_dead_man.main(["--status-file", str(status)]) == 2
    assert load_state(status).alerting
    assert sent == ["eBull jobs daemon DARK", "eBull jobs daemon DARK"]
    assert jobs_dead_man.main(["--status-file", str(status)]) == 2
    assert len(sent) == 2  # delivered once: de-duplicated now


def test_an_unwritable_status_file_is_named_in_the_alert(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    blocker = tmp_path / "not-a-dir"
    blocker.write_text("")
    monkeypatch.delenv("EBULL_NTFY_TOPIC", raising=False)
    monkeypatch.setattr(jobs_dead_man, "read_last_start", lambda: (1.0, None))
    messages: list[str] = []
    monkeypatch.setattr(jobs_dead_man, "local_notify", lambda _t, m: messages.append(m) is None)
    assert jobs_dead_man.main(["--status-file", str(blocker / "status.json")]) == 2
    assert messages and "status file unwritable" in messages[0]


def test_without_push_a_failed_local_notification_is_retried(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    status = tmp_path / "status.json"
    monkeypatch.delenv("EBULL_NTFY_TOPIC", raising=False)
    monkeypatch.setattr(jobs_dead_man, "read_last_start", lambda: (1.0, None))
    monkeypatch.setattr(jobs_dead_man, "local_notify", lambda *_a: False)
    assert jobs_dead_man.main(["--status-file", str(status)]) == 3
    assert not load_state(status).alerting


def test_main_runs_the_alert_watch_only_when_job_runs_was_readable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, _no_alert_watch: list[str]
) -> None:
    monkeypatch.delenv("EBULL_NTFY_TOPIC", raising=False)
    monkeypatch.setattr(jobs_dead_man, "local_notify", lambda *_a: True)
    monkeypatch.setattr(jobs_dead_man, "read_last_start", lambda: (jobs_dead_man.time.time(), None))
    assert jobs_dead_man.main(["--status-file", str(tmp_path / "a.json")]) == 0
    assert _no_alert_watch == ["watch"]
    monkeypatch.setattr(jobs_dead_man, "read_last_start", lambda: (None, "db: OperationalError"))
    jobs_dead_man.main(["--status-file", str(tmp_path / "a.json")])
    assert _no_alert_watch == ["watch"]


def test_a_failing_alert_watch_is_contained(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    from scripts import operator_alert_watch

    def _boom(**_kw: object) -> int:
        raise RuntimeError("schema drift")

    monkeypatch.setattr(operator_alert_watch, "run", _boom)
    _REAL_RUN_ALERT_WATCH()
    assert "operator alert watch failed: RuntimeError('schema drift')" in capsys.readouterr().err
