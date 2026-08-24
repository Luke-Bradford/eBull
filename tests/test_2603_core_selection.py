from typing import Any, cast
from unittest.mock import MagicMock

import psycopg
import pytest

from app.services.strategy_core_selection import (
    CoreSelectionError,
    load_core_selection,
    require_selected_core_instrument,
)


def _empty_connection() -> psycopg.Connection[Any]:
    conn = MagicMock()
    conn.execute.return_value.fetchall.return_value = []
    return cast(psycopg.Connection[Any], conn)


def _candidate_connection() -> psycopg.Connection[Any]:
    conn = MagicMock()
    conn.execute.return_value.fetchall.return_value = [
        (3417, "SPY.RTH", 5, None, None),
        (3434, "CSPX.L", 5, None, None),
        (3075, "IUSA.L", 5, None, None),
    ]
    return cast(psycopg.Connection[Any], conn)


def test_missing_candidates_are_unavailable_not_perpetually_collecting() -> None:
    selection = load_core_selection(_empty_connection())
    assert selection.state == "unavailable"
    assert selection.selected_instrument_id is None
    assert selection.observed_trading_days == 0
    assert selection.missing_candidate_ids == (3417, 3434, 3075)
    assert selection.configuration_error is None


def test_mandate_enablement_refuses_before_2833_verdict() -> None:
    with pytest.raises(CoreSelectionError, match="five-trading-day cost verdict"):
        require_selected_core_instrument(_empty_connection(), instrument_id=3417)


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
