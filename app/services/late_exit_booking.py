"""Pure checks for booking a confirmed late EXIT fill (#3007 part 2).

Spec: ``docs/proposals/execution/2026-09-23-late-exit-fill-booking.md``, checks 1-6.
Everything here runs BEFORE the booking transaction opens and touches no database,
so each refusal is table-testable. The locked checks (7-9) and the writes live in
``order_client._book_late_exit_fill``.

Two rules shape the whole module:

* **This is the one parser of the close-order fill fields.** The provider carries
  ``rate`` / ``units`` / ``occurred`` unparsed (``CloseOrderPositionFill``), so no
  second place can disagree about what is bookable.
* **Every failure is a refusal, never an escape.** A parse error, an overflow or a
  bad timestamp returns :class:`LateExitRefusal`, and the caller parks the row for a
  human. A close the broker reports filled that we cannot book belongs to a person,
  whichever check failed.

⚠ The price is NATIVE (asset) currency and is booked raw, not ``× conversionRate``:
``positions`` is native-denominated (``portfolio_sync`` builds ``avg_cost`` from the
broker's native ``open_price``), so ``(rate − exit_avg_cost) × units`` is native on
both sides. Settled in the spec's "Which currency the booking uses" section.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from app.providers.broker import CloseOrderPositionFill
    from app.services.order_client import RecommendationSubmissionContext

#: The scale of ``fills.price`` / ``fills.units`` / ``fills.gross_amount`` and
#: ``positions.realized_pnl`` (all ``NUMERIC(18,6)``, ``sql/001``).
_SCALE: Decimal = Decimal("0.000001")
#: ``NUMERIC(18,6)`` holds 12 integer digits, so ``abs(x) < 10**12``.
NUMERIC_18_6_BOUND: Decimal = Decimal(10) ** 12
#: Check 5's plausibility window, in calendar days either side. Fixed by
#: construction (spec, check 5): the smallest calendar grain that absorbs any
#: sub-day skew between the broker's clock and ours. It gates nothing else.
_OCCURRED_SLACK = timedelta(days=1)


@dataclass(frozen=True)
class LateExitBooking:
    """What checks 1-6 license. Every Decimal is already at the stored scale."""

    position_id: int
    price: Decimal
    units: Decimal
    filled_at: datetime
    gross_amount: Decimal
    realized_pnl_delta: Decimal
    exit_avg_cost: Decimal


@dataclass(frozen=True)
class LateExitRefusal:
    """Why a filled close cannot be booked. ``reason`` is a stable key for the audit."""

    reason: str
    detail: str


def quantise(value: Decimal) -> Decimal:
    """Round to the stored 6 dp, ``ROUND_HALF_UP`` (spec r5 #37-38)."""
    return value.quantize(_SCALE, rounding=ROUND_HALF_UP)


def _json_number(value: object, field: str) -> Decimal:
    """A finite, non-bool JSON number, as a Decimal. Raises ``ValueError`` otherwise.

    ``Decimal(str(value))`` and never ``Decimal(float)``: the latter carries the
    binary expansion (``Decimal(0.1)`` is 55 digits), which would make check 4's
    exact comparison fail on a value the broker sent exactly.
    """
    if isinstance(value, bool) or not isinstance(value, (int, float, Decimal)):
        raise ValueError(f"{field} is not a JSON number: {value!r}")
    parsed = Decimal(str(value))
    if not parsed.is_finite():
        raise ValueError(f"{field} is not finite: {value!r}")
    return parsed


def _fits(value: Decimal) -> bool:
    return abs(value) < NUMERIC_18_6_BOUND


def decide_late_exit_booking(
    *,
    context: RecommendationSubmissionContext,
    positions: tuple[CloseOrderPositionFill, ...],
    order_created_at: datetime,
    booking_at: datetime,
) -> LateExitBooking | LateExitRefusal:
    """Apply the spec's checks 1-6 to one ``filled`` close-order answer."""
    try:
        return _decide(
            context=context,
            positions=positions,
            order_created_at=order_created_at,
            booking_at=booking_at,
        )
    except (ArithmeticError, ValueError, TypeError) as exc:
        # `InvalidOperation` and `Overflow` are `ArithmeticError`s. A parse failure
        # of any kind is a refusal, never an escape (spec, "Pure checks").
        return LateExitRefusal("unparseable", f"{type(exc).__name__}: {exc}")


def _decide(
    *,
    context: RecommendationSubmissionContext,
    positions: tuple[CloseOrderPositionFill, ...],
    order_created_at: datetime,
    booking_at: datetime,
) -> LateExitBooking | LateExitRefusal:
    # 1. Context. Pre-409 / pre-413 rows fail here; re-deriving the lot or the
    #    cost at poll time is the guess #2942 forbids.
    lot = context.exit_lot
    avg_cost = context.exit_avg_cost
    if not context.recorded or lot is None or avg_cost is None:
        return LateExitRefusal(
            "context_not_recorded",
            f"recorded={context.recorded} exit_lot={lot is not None} exit_avg_cost={avg_cost is not None}",
        )

    # 2. Exactly one execution, and it is the lot. A partial close reports a NEW
    #    slice id, so any other shape (split, re-identified) parks.
    if len(positions) != 1 or positions[0].position_id != lot.position_id:
        return LateExitRefusal(
            "not_the_lot",
            f"lot={lot.position_id} executions={[fill.position_id for fill in positions]}",
        )
    fill = positions[0]

    # 3. Rate: positive, still positive at 6 dp, and fits the column. The
    #    quantised value is used everywhere, so the stored price is the P&L's price.
    rate = _json_number(fill.rate, "rate")
    if rate <= 0:
        return LateExitRefusal("rate_not_positive", str(rate))
    price = quantise(rate)
    if price <= 0:
        return LateExitRefusal("rate_rounds_to_zero", str(rate))
    if not _fits(price):
        return LateExitRefusal("rate_out_of_range", str(rate))

    # 4. Units: EXACTLY the lot's, with no quantisation, and exact at 6 dp.
    #    Terminality comes from here, not from `statusID`: a partial execution of
    #    a whole close carries fewer units and parks.
    units = _json_number(fill.units, "units")
    if units != lot.units:
        return LateExitRefusal("units_not_the_lot", f"executed={units} lot={lot.units}")
    if quantise(units) != units or not _fits(units):
        return LateExitRefusal("units_not_storable", str(units))

    # 5. Occurred: an offset-aware ISO timestamp inside the calendar-grain window.
    if not isinstance(fill.occurred, str):
        return LateExitRefusal("occurred_not_a_string", repr(fill.occurred))
    occurred = datetime.fromisoformat(fill.occurred)
    if occurred.tzinfo is None or occurred.utcoffset() is None:
        return LateExitRefusal("occurred_naive", fill.occurred)
    occurred_date = occurred.astimezone(UTC).date()
    earliest = (order_created_at.astimezone(UTC) - _OCCURRED_SLACK).date()
    latest = (booking_at.astimezone(UTC) + _OCCURRED_SLACK).date()
    if not earliest <= occurred_date <= latest:
        return LateExitRefusal("occurred_out_of_window", f"{fill.occurred} not in [{earliest}, {latest}]")

    # 6. Arithmetic bound.
    gross = quantise(price * units)
    delta = quantise((price - avg_cost) * units)
    if not _fits(gross) or not _fits(delta):
        return LateExitRefusal("amount_out_of_range", f"gross={gross} delta={delta}")

    return LateExitBooking(
        position_id=lot.position_id,
        price=price,
        units=units,
        filled_at=occurred,
        gross_amount=gross,
        realized_pnl_delta=delta,
        exit_avg_cost=avg_cost,
    )
