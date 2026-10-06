"""#3614 item 4 — the kill-switch drill's pure parts: classification, verdicts, the
time-to-flat estimate, the C2 decision split, and the entry-method census.

The database mechanisms (sandbox rollback, observe mode, loader neutrality, a
concurrent activation) are in ``test_3614_kill_switch_drill_db.py``.
"""

from __future__ import annotations

import ast
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

import app.services.kill_switch_drill as drill
from app.services.kill_switch_drill import (
    BookPosition,
    BookSnapshot,
    ChokepointResult,
    DrillRun,
    classify_c1,
    classify_c2,
    classify_c3,
    close_opportunity,
    entry_verdict,
    estimate_time_to_flat,
    last_close_created_at,
    mapping_defects,
)
from app.services.strategy_paper_executor import TradingEnabledState, decide_trading_enabled

REPO = Path(__file__).resolve().parents[1]
T = datetime(2026, 10, 6, 12, 0, tzinfo=UTC)


def _result(outcome: str, chokepoint: str = "C1") -> ChokepointResult:
    return ChokepointResult(chokepoint, outcome, None, (), True, "x" if outcome == "error" else None, T)  # type: ignore[arg-type]


def _pos(
    ownership_id: int,
    broker_position_id: int | None = None,
    *,
    broker_environment: str | None = "demo",
    us_listed: bool = True,
) -> BookPosition:
    """A mapped position (broker id defaults to 1000 + ownership id)."""
    broker_id = broker_position_id if broker_position_id is not None else 1000 + ownership_id
    return BookPosition(ownership_id, broker_id, 1, T, True, True, broker_environment, us_listed)


def _unmapped(ownership_id: int) -> BookPosition:
    return BookPosition(ownership_id, None, 1, None, None, None, None, True)


# --------------------------------------------------------------------------
# Classification
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("failed", "outcome"),
    [
        (("kill_switch",), "kill_refused"),
        (("kill_switch", "auto_trading"), "kill_refused"),
        (("auto_trading",), "allowed"),  # the revert-probe shape: kill rule removed, auto-trading off
        ((), "allowed"),
        (("kill_switch_config_corrupt", "auto_trading"), "error"),
    ],
)
def test_c1_is_classified_over_its_whole_rule_list(failed: tuple[str, ...], outcome: str) -> None:
    assert classify_c1(failed)[0] == outcome


@pytest.mark.parametrize(
    ("code", "outcome"),
    [
        ("kill_switch_active_or_missing", "kill_refused"),
        ("auto_trading_disabled", "other_refusal"),
        ("runtime_config_corrupt", "other_refusal"),
        (None, "allowed"),
        ("something_new", "error"),
    ],
)
def test_c2_classification(code: str | None, outcome: str) -> None:
    assert classify_c2(code)[0] == outcome


@pytest.mark.parametrize(
    ("admitted", "code", "outcome"),
    [
        (False, "core_kill_switch_active_or_missing", "kill_refused"),
        (False, "core_auto_trading_disabled", "other_refusal"),
        (False, "core_runtime_config_corrupt", "other_refusal"),
        (False, "core_execution_block_active", "allowed"),  # reached past the kill check
        (True, None, "allowed"),
    ],
)
def test_c3_classification(admitted: bool, code: str | None, outcome: str) -> None:
    assert classify_c3(admitted, code)[0] == outcome


def test_entry_verdict() -> None:
    kill = _result("kill_refused")
    assert entry_verdict([], failed_after_evaluation=False) == "not_run"
    assert entry_verdict([kill, kill, _result("not_applicable")], failed_after_evaluation=False) == "passed"
    assert entry_verdict([kill, _result("other_refusal")], failed_after_evaluation=False) == "incomplete"
    mixed = [kill, _result("other_refusal"), _result("allowed")]
    assert entry_verdict(mixed, failed_after_evaluation=False) == "failed"
    assert entry_verdict([kill, _result("error")], failed_after_evaluation=False) == "failed"
    assert entry_verdict([kill], failed_after_evaluation=True) == "failed"


# --------------------------------------------------------------------------
# C2 split: same codes, same precedence as the pre-split function
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("auto", "kill", "code"),
    [
        (False, True, "auto_trading_disabled"),
        (False, None, "auto_trading_disabled"),
        (True, True, "kill_switch_active_or_missing"),
        (True, None, "kill_switch_active_or_missing"),
        (True, False, None),
    ],
)
def test_c2_decision(auto: bool, kill: bool | None, code: str | None) -> None:
    assert decide_trading_enabled(TradingEnabledState(auto, kill)) == code


# --------------------------------------------------------------------------
# Time-to-flat
# --------------------------------------------------------------------------

# 2026-10-06 is a Tuesday; New York is on EDT (UTC-4): 09:30 ET = 13:30Z, 16:00 ET = 20:00Z.
OPEN_TUE = datetime(2026, 10, 6, 13, 30, tzinfo=UTC)
OPEN_WED = datetime(2026, 10, 7, 13, 30, tzinfo=UTC)
OPEN_MON = datetime(2026, 10, 12, 13, 30, tzinfo=UTC)


def test_opportunity_inside_the_session_is_now() -> None:
    t0 = datetime(2026, 10, 6, 15, 0, tzinfo=UTC)
    assert close_opportunity(t0) == t0


def test_opportunity_before_the_open_is_that_open() -> None:
    assert close_opportunity(datetime(2026, 10, 6, 6, 12, tzinfo=UTC)) == OPEN_TUE


def test_opportunity_after_the_close_is_the_next_open() -> None:
    assert close_opportunity(datetime(2026, 10, 6, 20, 0, tzinfo=UTC)) == OPEN_WED


def test_opportunity_at_the_weekend_is_monday() -> None:
    assert close_opportunity(datetime(2026, 10, 10, 15, 0, tzinfo=UTC)) == OPEN_MON


def test_opportunity_on_a_holiday_skips_it() -> None:
    # 2026-11-26 is Thanksgiving (closed); New York is on EST (UTC-5) by then.
    assert close_opportunity(datetime(2026, 11, 26, 15, 0, tzinfo=UTC)) == datetime(2026, 11, 27, 14, 30, tzinfo=UTC)


def test_twenty_one_closes_need_a_second_slot() -> None:
    assert last_close_created_at(OPEN_TUE, 20) == OPEN_TUE
    assert last_close_created_at(OPEN_TUE, 21) == OPEN_TUE + timedelta(minutes=1)


def test_a_slot_at_the_close_moves_to_the_next_open() -> None:
    last_minute = datetime(2026, 10, 6, 19, 59, tzinfo=UTC)
    assert last_close_created_at(last_minute, 21) == OPEN_WED


def test_estimate_adds_r_to_the_last_creation() -> None:
    t0 = datetime(2026, 10, 6, 6, 12, tzinfo=UTC)
    est = estimate_time_to_flat(t0, [_pos(i) for i in range(21)], outstanding_authority=0, close_resolution_max_s=874)
    assert est.null_reason is None
    assert est.seconds == (OPEN_TUE + timedelta(minutes=1) - t0).total_seconds() + 874


def test_no_exposure_is_zero_even_without_a_close_sample() -> None:
    est = estimate_time_to_flat(T, [], outstanding_authority=0, close_resolution_max_s=None)
    assert (est.seconds, est.null_reason) == (0.0, None)


@pytest.mark.parametrize(
    ("positions", "authority", "r", "reason"),
    [
        ([_pos(1)], 0, None, "no_close_observed"),
        ([_pos(1, us_listed=False)], 0, 874.0, "session_unknown"),
        ([_pos(1)], 1, 874.0, "outstanding_authority"),
        ([], 1, 874.0, "outstanding_authority"),
        ([_unmapped(1)], 0, 874.0, "mapping_defect"),
        ([_pos(1, 11), _pos(1, 12)], 0, 874.0, "mapping_defect"),
        ([_pos(1, broker_environment="real")], 0, 874.0, "unsupported_route"),
    ],
)
def test_estimate_null_reasons(positions: list[BookPosition], authority: int, r: float | None, reason: str) -> None:
    est = estimate_time_to_flat(T, positions, outstanding_authority=authority, close_resolution_max_s=r)
    assert (est.seconds, est.null_reason) == (None, reason)


def test_an_unrecorded_environment_is_not_a_real_route() -> None:
    """Today's book: every entry order predates #3189's column, and entries are demo-only."""
    positions = [_pos(1, broker_environment=None)]
    est = estimate_time_to_flat(T, positions, outstanding_authority=0, close_resolution_max_s=1)
    assert est.null_reason is None


@pytest.mark.parametrize(
    ("position", "verdict"),
    [
        (_pos(1), "ok"),
        (BookPosition(1, 11, 1, T, False, True, "demo", True), "defects"),
        (BookPosition(1, 11, 1, T, True, False, "demo", True), "defects"),  # SL AND TP on every position
        (_unmapped(1), "defects"),
    ],
)
def test_book_verdict(position: BookPosition, verdict: str) -> None:
    run = DrillRun(uuid.uuid4(), "manual", "test", T)
    run.snapshot = BookSnapshot(T, (position,), (), ())
    assert drill._book_fields(run)["book_verdict"] == verdict  # pyright: ignore[reportPrivateUsage]


def test_mapping_defects() -> None:
    assert mapping_defects([_pos(1), _unmapped(2), _pos(3, 31), _pos(3, 32)]) == (1, 1)


# --------------------------------------------------------------------------
# The census: the callers of the three broker entry methods are C1-C3
# --------------------------------------------------------------------------

ENTRY_METHODS = frozenset({"place_order", "place_demo_strategy_order", "place_demo_core_order"})

#: (file, enclosing function) -> chokepoint. Spec § Entry chokepoints.
CENSUS = {
    ("app/services/order_client.py", "_execute_under_key"): "C1",
    ("app/services/strategy_paper_executor.py", "_resume_uncertain_submission_locked"): "C2",
    ("app/services/strategy_paper_executor.py", "_submit_recorded_order"): "C2",
    ("app/services/strategy_core_executor.py", "_submit_core_authority_locked"): "C3",
}


def _entry_callers() -> set[tuple[str, str]]:
    found: set[tuple[str, str]] = set()
    for root in ("app", "scripts"):
        for path in sorted((REPO / root).rglob("*.py")):
            tree = ast.parse(path.read_text())
            for fn in ast.walk(tree):
                if not isinstance(fn, ast.FunctionDef | ast.AsyncFunctionDef):
                    continue
                for node in ast.walk(fn):
                    if (
                        isinstance(node, ast.Call)
                        and isinstance(node.func, ast.Attribute)
                        and node.func.attr in ENTRY_METHODS
                    ):
                        found.add((str(path.relative_to(REPO)), fn.name))
    return found


def test_every_caller_of_a_broker_entry_method_is_a_drilled_chokepoint() -> None:
    """A fourth entry caller fails here until the drill covers it (spec § Entry chokepoints).

    Nested functions are walked by both their own def and the enclosing one, so a call
    inside a closure reports both names; none of today's callers is nested.
    """
    assert _entry_callers() == set(CENSUS)
