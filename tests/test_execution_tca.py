from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest

from app.services.execution_tca import ARRIVAL_MAX_AGE_SECONDS, ArrivalFacts, ArrivalOutcome, entry_costs, entry_tca

AT = datetime(2026, 11, 2, 15, 10, 30, tzinfo=UTC)


def _arrival(
    bid: str = "99.9", ask: str = "100.1", *, age_s: float = 2, outcome: ArrivalOutcome = "quoted"
) -> ArrivalFacts:
    if outcome != "quoted":
        return ArrivalFacts(outcome, AT, None, None, None)
    return ArrivalFacts(outcome, AT, AT - timedelta(seconds=age_s), Decimal(bid), Decimal(ask))


def test_shortfall_splits_exactly_into_delay_and_execution_at_the_arrival_mid() -> None:
    c = entry_costs(decision=Decimal("99.5"), fill=Decimal("100.08"), arrival=_arrival())
    assert c.arrival_unmeasured is None
    assert c.delay == Decimal("100") / Decimal("99.5") - 1
    assert c.execution == Decimal("0.0008")
    assert c.effective_spread == Decimal("0.0016")
    assert c.quoted_spread == Decimal("0.002")
    assert c.delay is not None and c.execution is not None
    assert abs((1 + c.shortfall) - (1 + c.delay) * (1 + c.execution)) < Decimal("1e-20")


def test_a_fill_below_the_mid_is_a_negative_execution_cost_and_a_positive_effective_spread() -> None:
    c = entry_costs(decision=Decimal("100"), fill=Decimal("99.95"), arrival=_arrival())
    assert c.execution == Decimal("-0.0005")
    assert c.effective_spread == Decimal("0.001")


@pytest.mark.parametrize(
    ("arrival", "reason"),
    [
        (None, "arrival_missing"),
        (_arrival(outcome="failed"), "arrival_failed"),
        (_arrival(outcome="not_quoted"), "arrival_not_quoted"),
        (_arrival(age_s=ARRIVAL_MAX_AGE_SECONDS + 1), "arrival_stale"),
        (_arrival(age_s=-6), "arrival_future_dated"),
        (_arrival("100.2", "100.1"), "arrival_crossed"),
    ],
)
def test_an_unusable_arrival_keeps_the_shortfall_and_drops_the_split(arrival: ArrivalFacts | None, reason: str) -> None:
    c = entry_costs(decision=Decimal("100"), fill=Decimal("100.5"), arrival=arrival)
    assert c.shortfall == Decimal("0.005")
    assert (c.delay, c.execution, c.effective_spread, c.quoted_spread) == (None, None, None, None)
    assert c.arrival_unmeasured == reason


def test_an_arrival_exactly_at_the_age_bound_is_used() -> None:
    c = entry_costs(decision=Decimal("100"), fill=Decimal("100"), arrival=_arrival(age_s=ARRIVAL_MAX_AGE_SECONDS))
    assert c.arrival_unmeasured is None
    assert c.arrival_age_seconds == Decimal(ARRIVAL_MAX_AGE_SECONDS)


def _row(**overrides: Any) -> dict[str, Any]:
    row: dict[str, Any] = {
        "order_id": 1,
        "instrument_id": 7,
        "symbol": "X",
        "created_at": AT,
        "quote_at": AT - timedelta(seconds=90),
        "price_reference": "quote",
        "quote_ask": Decimal("100"),
        "stressed_cost_amount": Decimal("0.2"),
        "allocated_amount": Decimal("100"),
        "cost_stress_multiplier": Decimal("2"),
        "outcome": "quoted",
        "received_at": AT,
        "quoted_at": AT - timedelta(seconds=1),
        "bid": Decimal("99.9"),
        "ask": Decimal("100.1"),
        "n_opens": 1,
        "fill": Decimal("100.05"),
        "filled_at": AT + timedelta(seconds=4),
    }
    return row | overrides


def test_a_single_fill_is_measured_and_the_what_if_cost_is_unstressed() -> None:
    e = entry_tca(_row())
    assert e.unmeasured is None and e.costs is not None
    assert e.costs.execution == Decimal("0.0005")
    assert e.what_if_cost == Decimal("0.001")


@pytest.mark.parametrize(
    ("overrides", "reason"),
    [
        ({"n_opens": 0, "fill": None, "filled_at": None}, "no_fill"),
        ({"n_opens": 2}, "multiple_fills"),
        ({"fill": None}, "fill_price_unusable"),
    ],
)
def test_an_entry_without_one_usable_fill_is_not_measured(overrides: dict[str, Any], reason: str) -> None:
    e = entry_tca(_row(**overrides))
    assert e.costs is None and e.unmeasured == reason


def test_an_entry_without_an_arrival_row_still_reports_its_shortfall() -> None:
    e = entry_tca(_row(outcome=None, received_at=None, quoted_at=None, bid=None, ask=None))
    assert e.costs is not None and e.costs.shortfall == Decimal("0.0005")
    assert e.costs.arrival_unmeasured == "arrival_missing"
