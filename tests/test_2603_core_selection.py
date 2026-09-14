from datetime import datetime
from typing import Any, cast
from unittest.mock import MagicMock

import psycopg
import pytest

from app.services.strategy_core_selection import (
    CORE_SELECTION_EVIDENCE_NOT_BEFORE,
    CoreSelectionError,
    load_core_selection,
    require_selected_core_instrument,
)
from scripts.verify_2833_core_selection import load_declaration


def _empty_connection() -> psycopg.Connection[Any]:
    conn = MagicMock()
    conn.execute.return_value.fetchall.return_value = []
    return cast(psycopg.Connection[Any], conn)


def _candidate_connection() -> psycopg.Connection[Any]:
    conn = MagicMock()
    conn.execute.return_value.fetchall.return_value = [
        # Venue classes are the DEV-MEASURED ones (2026-09-14): SPY.RTH is exchange
        # `33` / `us_equity`; CSPX.L and IUSA.L are both exchange `7` = LSE /
        # `uk_equity`, which the core execution path has no session calendar for.
        (3417, "SPY.RTH", 5, None, None, 4, "us_equity"),
        (3434, "CSPX.L", 5, None, None, 4, "uk_equity"),
        (3075, "IUSA.L", 5, None, None, 4, "uk_equity"),
    ]
    return cast(psycopg.Connection[Any], conn)


def _selection_with_verdict(
    monkeypatch: pytest.MonkeyPatch,
    instrument_id: int,
    *,
    conn: psycopg.Connection[Any] | None = None,
) -> Any:
    """Load the selection as if #2833's verdict constants named ``instrument_id``."""
    monkeypatch.setattr("app.services.strategy_core_selection.SELECTED_CORE_INSTRUMENT_ID", instrument_id)
    monkeypatch.setattr("app.services.strategy_core_selection.SELECTED_CORE_EVIDENCE_REF", "#2833 verdict")
    return load_core_selection(conn if conn is not None else _candidate_connection())


def test_missing_candidates_are_unavailable_not_perpetually_collecting() -> None:
    selection = load_core_selection(_empty_connection())
    assert selection.state == "unavailable"
    assert selection.selected_instrument_id is None
    assert selection.observed_trading_days == 0
    assert selection.missing_candidate_ids == (3417, 3434, 3075)
    assert selection.configuration_error is None


def test_page_coverage_uses_the_frozen_prospective_boundary() -> None:
    declaration = load_declaration()
    assert CORE_SELECTION_EVIDENCE_NOT_BEFORE == datetime.fromisoformat(
        str(declaration["evidence_not_before"]).replace("Z", "+00:00")
    )


def test_overall_progress_counts_common_dates_not_the_minimum_individual_count() -> None:
    selection = load_core_selection(_candidate_connection())
    assert [candidate.observed_trading_days for candidate in selection.candidates] == [5, 5, 5]
    assert selection.observed_trading_days == 4


def test_mandate_enablement_refuses_before_2833_verdict() -> None:
    with pytest.raises(CoreSelectionError, match="five-trading-day cost verdict"):
        require_selected_core_instrument(_empty_connection(), instrument_id=3417)


@pytest.mark.parametrize("unexecutable_id", [3434, 3075])
def test_a_verdict_naming_a_venue_we_cannot_session_check_is_refused_at_declaration(
    monkeypatch: pytest.MonkeyPatch,
    unexecutable_id: int,
) -> None:
    """#2833 may only name a sleeve the core execution path can actually trade.

    Two of the three declared candidates are LSE / ``uk_equity``, and
    ``decide_core_preflight`` refuses those ``core_unsupported_market_session``
    with every other input healthy. Without this gate the refusal first appears
    in the operator-attended session, which is the most expensive place to find
    it; ``require_selected_core_instrument`` keys on ``ready``, so refusing here
    blocks the mandate writer and the executor together.
    """
    selection = _selection_with_verdict(monkeypatch, unexecutable_id)
    assert selection.state == "unavailable"
    assert selection.selected_instrument_id is None
    assert selection.selected_symbol is None
    assert selection.configuration_error is not None
    # The identity half is what the wrap exists for -- `session_support_reason` alone
    # names the CLASS, not which selection is at fault.
    assert str(unexecutable_id) in selection.configuration_error
    assert "uk_equity" in selection.configuration_error
    with pytest.raises(CoreSelectionError, match="five-trading-day cost verdict"):
        require_selected_core_instrument(_candidate_connection(), instrument_id=unexecutable_id)


def test_a_verdict_naming_an_executable_venue_is_ready(monkeypatch: pytest.MonkeyPatch) -> None:
    """The control arm — without it the test above passes on a gate that refuses
    everything, which is the failure mode a fail-closed check invites."""
    selection = _selection_with_verdict(monkeypatch, 3417)
    assert selection.state == "ready"
    assert selection.selected_instrument_id == 3417
    assert selection.selected_symbol == "SPY.RTH"
    assert selection.configuration_error is None


def test_a_candidate_with_no_exchanges_row_is_refused_not_admitted(monkeypatch: pytest.MonkeyPatch) -> None:
    """A NULL ``asset_class`` is an unknown venue, which is exactly what the
    allow-list exists to catch — it must not read as "nothing objected"."""
    conn = MagicMock()
    conn.execute.return_value.fetchall.return_value = [
        (3417, "SPY.RTH", 5, None, None, 4, None),
        (3434, "CSPX.L", 5, None, None, 4, "uk_equity"),
        (3075, "IUSA.L", 5, None, None, 4, "uk_equity"),
    ]
    selection = _selection_with_verdict(monkeypatch, 3417, conn=cast(psycopg.Connection[Any], conn))
    assert selection.state == "unavailable"
    assert selection.configuration_error is not None
    assert "asset_class=None" in selection.configuration_error


@pytest.mark.parametrize(
    ("instrument_id", "evidence_ref"),
    [(3417, None), (999999, "#2833 verdict")],
)
def test_partial_or_foreign_verdict_constants_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
    instrument_id: int,
    evidence_ref: str | None,
) -> None:
    monkeypatch.setattr("app.services.strategy_core_selection.SELECTED_CORE_INSTRUMENT_ID", instrument_id)
    monkeypatch.setattr("app.services.strategy_core_selection.SELECTED_CORE_EVIDENCE_REF", evidence_ref)
    selection = load_core_selection(_candidate_connection())
    assert selection.state == "unavailable"
    assert selection.selected_instrument_id is None
    assert selection.configuration_error is not None
