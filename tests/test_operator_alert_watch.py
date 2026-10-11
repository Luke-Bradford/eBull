"""Pure-policy tests for the refusal-surface watch (#3614 item 3, slice B).

``plan``/``settle`` are pure; ``run`` runs with the DB read and the push
stubbed. No database, no network.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from app.services.kill_switch_drill import C2_EARLY_CODES, C3_EARLY_CODES
from app.services.kill_switch_drill_schedule import DRILL_OCCURRENCE_GRACE, DRILL_WATCH_FROM
from app.services.strategy_capital_sandbox import SANDBOX_EXCEEDED
from scripts import operator_alert_watch
from scripts.operator_alert_watch import (
    _BLOCK_PERSIST_S,
    _CONFIG_CORRUPT_CODES,
    _PAGED_BLOCKS,
    _PAGED_ENTRY_REFUSALS,
    _SEEN_TTL_S,
    Block,
    DrillChokepoint,
    DrillEvent,
    EntryRefusal,
    KillSwitchChange,
    Observation,
    WatchState,
    load_state,
    plan,
    save_state,
    settle,
)

_NOW = 2_000_000.0
_REPO = Path(__file__).resolve().parents[1]


def _block(source: str = "drawdown", *, active: bool = True, age: float = _BLOCK_PERSIST_S) -> Block:
    return Block(source, active, _NOW - age if active else None, "engine pot drawdown 12.0000% reached the limit 10%")


def _all_delivered(prior: WatchState, obs: Observation, now: float = _NOW) -> WatchState:
    return settle(prior, plan(prior, obs, now=now), now=now)


def test_a_held_block_pages_once_and_its_clear_pages_once() -> None:
    obs = Observation(blocks=[_block()])
    notices = plan(WatchState(), obs, now=_NOW)
    assert [n.title for n in notices] == ["eBull entries blocked: drawdown"]
    assert "reached the limit" in notices[0].message
    assert "Safe default" in notices[0].message

    state = _all_delivered(WatchState(), obs)
    assert state.paged_blocks == ("drawdown",)
    assert plan(state, obs, now=_NOW + 300) == []

    cleared = Observation(blocks=[_block(active=False)])
    notices = plan(state, cleared, now=_NOW + 600)
    assert [n.title for n in notices] == ["eBull drawdown block cleared"]
    state = _all_delivered(state, cleared, now=_NOW + 600)
    assert state.paged_blocks == ()
    assert plan(state, cleared, now=_NOW + 900) == []


def test_a_young_block_does_not_page_yet() -> None:
    assert plan(WatchState(), Observation(blocks=[_block(age=_BLOCK_PERSIST_S - 1)]), now=_NOW) == []


def test_a_reblock_while_paged_stays_paged_without_a_new_notice() -> None:
    state = WatchState(paged_blocks=("drawdown",))
    # Cleared and re-blocked between runs: blocked_at is fresh, the source is still active.
    assert plan(state, Observation(blocks=[_block(age=30)]), now=_NOW) == []


def test_freshness_blocks_never_page() -> None:
    obs = Observation(blocks=[_block("quote_freshness", age=86_400), _block("scan_freshness", age=86_400)])
    assert plan(WatchState(), obs, now=_NOW) == []


def test_a_missing_paged_block_row_counts_as_cleared() -> None:
    notices = plan(WatchState(paged_blocks=("broker_availability",)), Observation(), now=_NOW)
    assert [n.block_off for n in notices] == ["broker_availability"]


def test_each_kill_switch_change_pages_once_in_audit_order() -> None:
    obs = Observation(
        kill_switch=[
            KillSwitchChange(8, _NOW - 60, "operator", "drill done", new_active=False),
            KillSwitchChange(7, _NOW - 120, "operator", "drill", new_active=True),
        ]
    )
    notices = plan(WatchState(), obs, now=_NOW)
    assert [(n.title, n.priority) for n in notices] == [
        ("eBull kill switch ACTIVATED", 5),
        ("eBull kill switch deactivated", 4),
    ]
    assert "by operator" in notices[0].message and ": drill." in notices[0].message
    # Under kill, broker SL/TP and exact-owned exits run; recommendation EXIT and core sells do not.
    assert "New entries are refused; exits still run" not in notices[0].message
    assert "recommendation EXITs and core rebalance sells are refused" in notices[0].message
    assert plan(_all_delivered(WatchState(), obs), obs, now=_NOW + 300) == []


def test_entry_refusals_aggregate_into_one_notice_and_only_paged_codes_count() -> None:
    obs = Observation(
        refusals=[
            EntryRefusal(1, _NOW - 10, "sandbox_exceeded", "deployment 7, account equity 900.00"),
            EntryRefusal(2, _NOW - 5, "sandbox_exceeded", "deployment 7, account equity 1000.00"),
            EntryRefusal(3, _NOW - 5, "portfolio_daily_loss_limit"),
            EntryRefusal(4, _NOW - 5, "quote_spread_flagged"),
        ]
    )
    notices = plan(WatchState(), obs, now=_NOW)
    assert len(notices) == 1
    assert "3 engine entries were refused" in notices[0].message
    assert (
        "portfolio_daily_loss_limit x1; sandbox_exceeded x2 (latest: deployment 7, account equity 1000.00)"
        in notices[0].message
    )
    assert set(notices[0].seen) == {"entry_refusal:1", "entry_refusal:2", "entry_refusal:3"}

    state = _all_delivered(WatchState(), obs)
    later = Observation(refusals=[*obs.refusals, EntryRefusal(5, _NOW + 200, "sandbox_exceeded")])
    notices = plan(state, later, now=_NOW + 300)
    assert len(notices) == 1 and "1 engine entry was refused" in notices[0].message


def test_an_undelivered_notice_is_planned_again() -> None:
    obs = Observation(blocks=[_block()], kill_switch=[KillSwitchChange(1, _NOW, "op", "r", new_active=True)])
    state = settle(WatchState(), [], now=_NOW)
    assert len(plan(state, obs, now=_NOW + 300)) == 2


def test_seen_keys_expire_only_after_the_ttl() -> None:
    state = WatchState(seen={"kill_switch:1": _NOW})
    assert "kill_switch:1" in settle(state, [], now=_NOW + _SEEN_TTL_S - 1).seen
    assert settle(state, [], now=_NOW + _SEEN_TTL_S).seen == {}


def test_state_round_trips_and_a_corrupt_file_resets(tmp_path: Path) -> None:
    path = tmp_path / "s.json"
    state = WatchState(paged_blocks=("drawdown",), seen={"kill_switch:3": _NOW})
    assert save_state(path, state)
    assert load_state(path) == state
    path.write_text("[]")
    assert load_state(path) == WatchState()
    assert load_state(tmp_path / "missing.json") == WatchState()


def test_run_records_only_delivered_notices(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    status = tmp_path / "s.json"
    obs = Observation(blocks=[_block(age=86_400)])
    monkeypatch.setenv("EBULL_NTFY_TOPIC", "t")
    monkeypatch.setattr(operator_alert_watch, "local_notify", lambda *_a: True)
    monkeypatch.setattr(operator_alert_watch, "read_observation", lambda: obs)
    sent: list[str] = []

    def _push(**kw: object) -> bool:
        sent.append(str(kw["title"]))
        return len(sent) > 1  # first attempt fails

    monkeypatch.setattr(operator_alert_watch, "send_push", _push)
    assert operator_alert_watch.run(status_file=status) == 1
    assert load_state(status).paged_blocks == ()
    assert operator_alert_watch.run(status_file=status) == 0
    assert load_state(status).paged_blocks == ("drawdown",)
    assert operator_alert_watch.run(status_file=status) == 0
    assert len(sent) == 2


def test_with_push_off_the_local_notification_decides_delivery(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    status = tmp_path / "s.json"
    monkeypatch.delenv("EBULL_NTFY_TOPIC", raising=False)
    monkeypatch.setattr(operator_alert_watch, "read_observation", lambda: Observation(blocks=[_block(age=86_400)]))
    shown = [False, True]
    monkeypatch.setattr(operator_alert_watch, "local_notify", lambda *_a: shown.pop(0))
    assert operator_alert_watch.run(status_file=status) == 1  # not shown: retried next pass
    assert load_state(status).paged_blocks == ()
    assert operator_alert_watch.run(status_file=status) == 0
    assert load_state(status).paged_blocks == ("drawdown",)


def test_dry_run_neither_sends_nor_saves(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    status = tmp_path / "s.json"
    monkeypatch.setenv("EBULL_NTFY_TOPIC", "t")
    monkeypatch.setattr(operator_alert_watch, "local_notify", lambda *_a: True)
    monkeypatch.setattr(operator_alert_watch, "read_observation", lambda: Observation(blocks=[_block(age=86_400)]))
    monkeypatch.setattr(operator_alert_watch, "send_push", lambda **_kw: pytest.fail("dry run sent"))
    assert operator_alert_watch.run(status_file=status, dry_run=True) == 0
    assert not status.exists()


def test_paged_codes_and_sources_are_ones_the_engine_writes() -> None:
    """A renamed refusal code or block source would silently never page.

    Matched in the form the engine writes them, not as any string literal: a loss-limit
    code is a ``return`` from the executor's risk check, the sandbox code its imported
    constant, and a block source the ``source=`` keyword of the runtime's ``_set_block``.
    """
    executor = (_REPO / "app/services/strategy_paper_executor.py").read_text()
    assert "return SANDBOX_EXCEEDED" in executor
    for code in _PAGED_ENTRY_REFUSALS - {SANDBOX_EXCEEDED}:
        assert f'return "{code}"' in executor, code
    runtime = (_REPO / "app/services/strategy_paper_runtime.py").read_text()
    for source in _PAGED_BLOCKS:
        assert f'source="{source}"' in runtime, source


def test_refusal_evidence_names_each_persisted_figure_and_skips_nulls() -> None:
    assert operator_alert_watch._refusal_evidence(7, "1000.00", None, "50.00", "3.5") == (
        "deployment 7, account equity 1000.00, available cash 50.00, drawdown 3.5%"
    )
    assert operator_alert_watch._refusal_evidence(None, None, None, None, None) == ""


def test_an_unwritable_status_file_is_named_in_the_notice(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    blocker = tmp_path / "not-a-dir"
    blocker.write_text("")
    monkeypatch.setenv("EBULL_NTFY_TOPIC", "t")
    monkeypatch.setattr(operator_alert_watch, "local_notify", lambda *_a: True)
    monkeypatch.setattr(operator_alert_watch, "read_observation", lambda: Observation(blocks=[_block(age=86_400)]))
    messages: list[str] = []
    monkeypatch.setattr(operator_alert_watch, "send_push", lambda **kw: messages.append(str(kw["message"])) is None)
    assert operator_alert_watch.run(status_file=blocker / "s.json") == 0
    assert messages and messages[0].endswith("[alert watch status file unwritable: not de-duplicated]")


# ──────────────────────────── kill drills (#3614 slice 3) ────────────────────────────

# Just after the first watched slot's grace, with that slot satisfied, so nothing is overdue.
_SLOT = datetime(2026, 11, 3, 6, 12, tzinfo=UTC)
_DRILL_NOW = (_SLOT + DRILL_OCCURRENCE_GRACE).timestamp()
_PASSED = (DrillChokepoint("C1", "kill_refused", "kill_switch"), DrillChokepoint("C2", "kill_refused"))


def _drill(event_id: int = 3, **kw: object) -> DrillEvent:
    base: dict[str, object] = {
        "event_id": event_id,
        "started_at": _SLOT.timestamp() + 1,
        "trigger": "scheduled",
        "actor": "kill-drill",
        "entry_verdict": "passed",
        "book_verdict": "ok",
        "chokepoints": _PASSED,
        "code_commit": "e79ac4832f0c1d2e",
        "code_dirty": False,
    }
    base.update(kw)
    return DrillEvent(**base)  # type: ignore[arg-type]


def test_a_passed_drill_with_a_clean_book_sends_nothing() -> None:
    assert plan(WatchState(), Observation(drills=[_drill()]), now=_DRILL_NOW) == []


def test_an_allowed_chokepoint_pages_with_the_auto_trading_default() -> None:
    chokepoints = (DrillChokepoint("C1", "kill_refused", "kill_switch"), DrillChokepoint("C2", "allowed"))
    obs = Observation(drills=[_drill(entry_verdict="failed", chokepoints=chokepoints)])
    [notice] = plan(WatchState(), obs, now=_DRILL_NOW)
    assert (notice.title, notice.priority) == ("eBull kill drill FAILED", 5)
    assert "C1 kill_refused (kill_switch), C2 allowed" in notice.message
    assert "C2 may not stop under kill" in notice.message
    assert "build e79ac4832f0c" in notice.message
    assert notice.message.endswith("Safe default: set enable_auto_trading = false until a re-run passes.")
    assert plan(_all_delivered(WatchState(), obs, now=_DRILL_NOW), obs, now=_DRILL_NOW + 300) == []


@pytest.mark.parametrize(
    ("run_failure", "verify", "says", "default"),
    [
        ("kill_switch_row_missing", None, "kill_switch row is missing", "leave the switch as it is now."),
        ("sandbox_committed", True, "switch read on at verify", "leave the switch as it is now."),
        ("exception", None, "could not complete (exception: RuntimeError: boom)", "none needed for trading."),
    ],
)
def test_each_run_failure_pages_with_its_own_safe_default(
    run_failure: str, verify: bool | None, says: str, default: str
) -> None:
    # An exception before evaluation leaves the entry verdict ``not_run``; it still pages.
    event = _drill(
        entry_verdict="not_run",
        chokepoints=(),
        run_failure=run_failure,
        failure_detail="RuntimeError: boom",
        kill_active_at_verify=verify,
        code_dirty=True,
    )
    [notice] = plan(WatchState(), Observation(drills=[event]), now=_DRILL_NOW)
    assert says in notice.message
    assert "(dirty tree)" in notice.message
    assert notice.message.endswith(f"Safe default: {default}")


def test_a_corrupt_runtime_config_pages_but_auto_trading_off_does_not() -> None:
    corrupt = (DrillChokepoint("C1", "kill_refused"), DrillChokepoint("C2", "other_refusal", "runtime_config_corrupt"))
    off = (DrillChokepoint("C1", "kill_refused"), DrillChokepoint("C2", "other_refusal", "auto_trading_disabled"))
    obs = Observation(
        drills=[
            _drill(3, entry_verdict="incomplete", chokepoints=corrupt),
            _drill(4, entry_verdict="incomplete", chokepoints=off),
        ]
    )
    [notice] = plan(WatchState(), obs, now=_DRILL_NOW)
    assert notice.title == "eBull runtime config unreadable"
    assert "event 3" in notice.message and "runtime_config_corrupt" in notice.message


def test_book_defects_page_with_counts_and_ownerships() -> None:
    event = _drill(
        book_verdict="defects",
        unmapped=1,
        multiply_mapped=0,
        without_stop=2,
        without_target=0,
        defect_ownerships=(11, 12, 15),
    )
    [notice] = plan(WatchState(), Observation(drills=[event]), now=_DRILL_NOW)
    assert notice.title == "eBull book defects found by kill drill"
    assert "1 unmapped and 0 multiply mapped ownerships, 2 positions without a stop" in notice.message
    assert "(ownerships: 11, 12, 15)" in notice.message


def test_a_later_passing_drill_does_not_hide_an_earlier_failure() -> None:
    obs = Observation(
        drills=[_drill(3, entry_verdict="failed", run_failure="exception", failure_detail="x"), _drill(4)]
    )
    assert [n.title for n in plan(WatchState(), obs, now=_DRILL_NOW)] == ["eBull kill drill FAILED"]


def test_an_undrilled_slot_pages_once_at_the_end_of_its_grace() -> None:
    assert plan(WatchState(), Observation(), now=_DRILL_NOW - 1) == []
    [notice] = plan(WatchState(), Observation(), now=_DRILL_NOW)
    assert (notice.title, notice.priority) == ("eBull kill drill overdue", 3)
    assert "2026-11-03 06:12Z" in notice.message
    state = _all_delivered(WatchState(), Observation(), now=_DRILL_NOW)
    assert plan(state, Observation(), now=_DRILL_NOW + 300) == []
    # A drill run late does not satisfy the slot after the fact, and nothing is re-sent.
    late = Observation(drills=[_drill(started_at=_DRILL_NOW + 600)])
    assert plan(state, late, now=_DRILL_NOW + 900) == []


def test_no_slot_before_the_watch_start_is_overdue() -> None:
    assert plan(WatchState(), Observation(), now=(DRILL_WATCH_FROM + timedelta(days=20)).timestamp()) == []


def test_drill_keys_never_expire_but_others_still_do() -> None:
    state = WatchState(seen={"kill_drill:3:entry": _NOW, "kill_drill_overdue:x": _NOW, "kill_switch:1": _NOW})
    assert set(settle(state, [], now=_NOW + 100 * _SEEN_TTL_S).seen) == {"kill_drill:3:entry", "kill_drill_overdue:x"}


def test_config_corrupt_codes_are_codes_the_drill_records() -> None:
    assert _CONFIG_CORRUPT_CODES < C2_EARLY_CODES | C3_EARLY_CODES
