"""#3007 part 2 — the pure booking checks 1-6, table-tested.

Spec: ``docs/proposals/execution/2026-09-23-late-exit-fill-booking.md``. The book
decision is taken on the VERBATIM whole-close lookup of close order ``383339190``
(attended GBX session, 2026-09-23), decoded with ``parse_float=Decimal`` exactly
as the adapter decodes it.
"""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from app.providers.broker import CloseOrderPositionFill
from app.services.late_exit_booking import (
    LateExitBooking,
    LateExitRefusal,
    decide_late_exit_booking,
)
from app.services.order_client import ExitLot, RecommendationSubmissionContext

_FIXTURE = Path(__file__).parent / "fixtures" / "etoro" / "attended_2026-09-23-02_partial_close_gbx.jsonl"
_WHOLE_CLOSE_STEP = "close_order_RAW_383339190"
_LOT_ID = 3_602_947_846
_LOT_UNITS = Decimal("0.32302400")  # `orders.recommendation_exit_units` is NUMERIC(20,8)
# The position's native open rate, in GBX: `openRate 5817.71` on the attended position.
_AVG_COST = Decimal("5817.710000")
_CREATED = datetime(2026, 9, 23, 8, 1, 53, tzinfo=UTC)
_BOOKED = datetime(2026, 9, 23, 9, 0, tzinfo=UTC)


def whole_close_body() -> dict[str, Any]:
    """The verbatim ``statusID 3`` body of the whole close, Decimal-decoded."""
    for line in _FIXTURE.read_text().splitlines():
        record = json.loads(line)
        if record["step"] == _WHOLE_CLOSE_STEP:
            return json.loads(json.dumps(record["payload"]), parse_float=Decimal)
    raise AssertionError(f"{_WHOLE_CLOSE_STEP} is not in {_FIXTURE.name}")


def _fills(body: dict[str, Any]) -> tuple[CloseOrderPositionFill, ...]:
    return tuple(
        CloseOrderPositionFill(
            position_id=int(row["positionID"]),
            rate=row.get("rate"),
            units=row.get("units"),
            occurred=row.get("occurred"),
        )
        for row in body["positions"]
    )


_CONTEXT = RecommendationSubmissionContext(
    recorded=True,
    order_params=None,
    exit_lot=ExitLot(position_id=_LOT_ID, units=_LOT_UNITS),
    exit_avg_cost=_AVG_COST,
)


def _decide(
    *,
    context: RecommendationSubmissionContext = _CONTEXT,
    fills: tuple[CloseOrderPositionFill, ...] | None = None,
    **overrides: object,
) -> LateExitBooking | LateExitRefusal:
    if fills is None:
        fills = _fills(whole_close_body())
        if overrides:
            fills = (replace(fills[0], **overrides),)  # type: ignore[arg-type]
    return decide_late_exit_booking(context=context, positions=fills, order_created_at=_CREATED, booking_at=_BOOKED)


def test_the_verbatim_whole_close_is_booked_in_native_currency() -> None:
    """Raw ``rate`` (GBX) against the native GBX ``avg_cost``; never ``× conversionRate``."""
    decision = _decide()

    assert decision == LateExitBooking(
        position_id=_LOT_ID,
        price=Decimal("5817.290000"),
        units=Decimal("0.323024"),
        filled_at=datetime(2026, 9, 23, 8, 1, 53, 830000, tzinfo=UTC),
        # 5817.29 × 0.323024 = 1879.12428496 GBX (≈ £18.79 ≈ $25, the history row's
        # `amount`), and (5817.29 − 5817.71) × 0.323024 = −0.13567008.
        gross_amount=Decimal("1879.124285"),
        realized_pnl_delta=Decimal("-0.135670"),
        exit_avg_cost=_AVG_COST,
    )


@pytest.mark.parametrize(
    ("context", "reason"),
    [
        (RecommendationSubmissionContext(recorded=False, order_params=None, exit_lot=None), "context_not_recorded"),
        (replace(_CONTEXT, exit_lot=None), "context_not_recorded"),
        # pre-413: a lot without its submission cost.
        (replace(_CONTEXT, exit_avg_cost=None), "context_not_recorded"),
    ],
)
def test_check_1_refuses_an_unrecorded_context(context: RecommendationSubmissionContext, reason: str) -> None:
    decision = _decide(context=context)
    assert isinstance(decision, LateExitRefusal) and decision.reason == reason


def test_check_2_refuses_an_execution_on_another_position() -> None:
    """A partial close reports a NEW slice id (``3602947861``), so it is not the lot."""
    decision = _decide(fills=(replace(_fills(whole_close_body())[0], position_id=3_602_947_861),))
    assert isinstance(decision, LateExitRefusal) and decision.reason == "not_the_lot"


@pytest.mark.parametrize("count", [0, 2])
def test_check_2_refuses_anything_but_exactly_one_execution(count: int) -> None:
    fills = _fills(whole_close_body()) * count
    decision = _decide(fills=fills)
    assert isinstance(decision, LateExitRefusal) and decision.reason == "not_the_lot"


@pytest.mark.parametrize(
    ("rate", "reason"),
    [
        (True, "unparseable"),
        ("5817.29", "unparseable"),
        (None, "unparseable"),
        (Decimal("NaN"), "unparseable"),
        (Decimal("Infinity"), "unparseable"),
        (Decimal("0"), "rate_not_positive"),
        (Decimal("-1"), "rate_not_positive"),
        # Positive, but ROUND_HALF_UP to 6 dp makes it 0.
        (Decimal("0.0000004"), "rate_rounds_to_zero"),
        (Decimal("1000000000000"), "rate_out_of_range"),
    ],
)
def test_check_3_refuses_an_unusable_rate(rate: object, reason: str) -> None:
    decision = _decide(rate=rate)
    assert isinstance(decision, LateExitRefusal) and decision.reason == reason


def test_check_3_books_the_quantised_price_everywhere() -> None:
    """``5817.2900005`` rounds HALF_UP to ``5817.290001``, and the P&L uses that."""
    decision = _decide(rate=Decimal("5817.2900005"))
    assert isinstance(decision, LateExitBooking)
    assert decision.price == Decimal("5817.290001")
    assert decision.gross_amount == (Decimal("5817.290001") * Decimal("0.323024")).quantize(Decimal("0.000001"))


def test_check_3_accepts_a_binary_float_through_its_string() -> None:
    """A fake that decodes plainly hands a float; ``Decimal(str())`` keeps its digits."""
    decision = _decide(rate=5817.29, units=0.323024)
    assert isinstance(decision, LateExitBooking) and decision.price == Decimal("5817.29")


@pytest.mark.parametrize(
    ("units", "reason"),
    [
        (False, "unparseable"),
        ("0.323024", "unparseable"),
        (Decimal("0.323023"), "units_not_the_lot"),
        # A partial execution of a whole close: fewer units, so NOT terminal.
        (Decimal("0.161512"), "units_not_the_lot"),
        (Decimal("0.646048"), "units_not_the_lot"),
    ],
)
def test_check_4_refuses_units_other_than_the_lot(units: object, reason: str) -> None:
    decision = _decide(units=units)
    assert isinstance(decision, LateExitRefusal) and decision.reason == reason


def test_check_4_refuses_units_not_exact_at_the_stored_scale() -> None:
    """Exactly the lot, but the lot has 8 dp and ``fills.units`` has 6: storing it would round."""
    context = replace(_CONTEXT, exit_lot=ExitLot(position_id=_LOT_ID, units=Decimal("0.32302401")))
    decision = _decide(context=context, units=Decimal("0.32302401"))
    assert isinstance(decision, LateExitRefusal) and decision.reason == "units_not_storable"


@pytest.mark.parametrize(
    ("occurred", "reason"),
    [
        (None, "occurred_not_a_string"),
        (1_758_614_513, "occurred_not_a_string"),
        ("yesterday", "unparseable"),
        ("2026-09-23T08:01:53.83", "occurred_naive"),
        # Outside [created_at::date − 1, booking date + 1].
        ("2026-09-21T23:59:59Z", "occurred_out_of_window"),
        ("2026-09-25T00:00:00Z", "occurred_out_of_window"),
    ],
)
def test_check_5_refuses_an_unusable_timestamp(occurred: object, reason: str) -> None:
    decision = _decide(occurred=occurred)
    assert isinstance(decision, LateExitRefusal) and decision.reason == reason


@pytest.mark.parametrize("occurred", ["2026-09-22T00:00:00Z", "2026-09-24T23:59:59+00:00"])
def test_check_5_window_edges_are_inclusive(occurred: str) -> None:
    assert isinstance(_decide(occurred=occurred), LateExitBooking)


def test_check_6_refuses_an_amount_the_column_cannot_hold() -> None:
    lot_units = Decimal("999999")
    context = replace(_CONTEXT, exit_lot=ExitLot(position_id=_LOT_ID, units=lot_units))
    decision = _decide(context=context, rate=Decimal("99999999"), units=lot_units)
    assert isinstance(decision, LateExitRefusal) and decision.reason == "amount_out_of_range"
