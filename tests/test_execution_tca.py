from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest

from app.providers.broker import BrokerCostComponent, BrokerWhatIfCostResponse
from app.services.execution_tca import (
    ARRIVAL_MAX_AGE_SECONDS,
    ArrivalFacts,
    ArrivalOutcome,
    band_rows,
    classify_exit,
    close_cost,
    entry_costs,
    entry_tca,
    exit_tca,
)

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
        (ArrivalFacts("unusable", AT, AT, Decimal("0"), Decimal("100")), "arrival_unusable"),
        (_arrival("99.9", "Infinity"), "arrival_unusable"),
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


# --- #3611 slice 2: exits ---------------------------------------------------------------------------------------


def _cost(amount: str | None, value: str | None, currency: str = "USD") -> BrokerCostComponent:
    return BrokerCostComponent(
        "marketSpread",
        None if amount is None else Decimal(amount),
        None if value is None else Decimal(value),
        currency,
        {},
    )


def _response(*costs: BrokerCostComponent, instrument_id: int = 7) -> BrokerWhatIfCostResponse:
    return BrokerWhatIfCostResponse(instrument_id, "X", costs, AT, {})


def test_close_cost_sums_every_component_in_one_field_and_currency() -> None:
    assert close_cost(_response(_cost(None, "1.5"), _cost(None, "0.25")), instrument_id=7) == Decimal("1.75")


@pytest.mark.parametrize(
    "response",
    [
        _response(_cost(None, "1.5"), instrument_id=8),  # another instrument's quote
        _response(),  # no rows is not "no cost"
        _response(_cost("1", "1")),  # both fields: undocumented which wins
        _response(_cost(None, None)),  # neither field
        _response(_cost("1", None), _cost(None, "1")),  # mixed fields
        _response(_cost(None, "1"), _cost(None, "1", "EUR")),  # mixed currencies
        _response(_cost(None, "1", "EUR")),  # one currency, but not the ticket's
        _response(_cost(None, "-0.1")),
        _response(_cost(None, "NaN")),
    ],
)
def test_close_cost_refuses_what_it_cannot_decode(response: BrokerWhatIfCostResponse) -> None:
    assert close_cost(response, instrument_id=7) is None


@pytest.mark.parametrize(
    ("engine", "fill", "stop", "take", "kind"),
    [
        (True, "90", "95", "120", "engine"),  # the engine's close, wherever it filled
        (False, "95", "95", "120", "stop_loss"),  # at the stop
        (False, "93", "95", "120", "stop_loss"),  # through it
        (False, "120", "95", "120", "take_profit"),
        (False, "100", "95", "120", "unexplained"),  # inside both levels: manual
        (False, "90", "0", None, "unexplained"),  # a zero level is no level
        (False, "90", None, None, "unexplained"),
    ],
)
def test_an_exit_is_attributed_by_order_then_by_level(
    engine: bool, fill: str, stop: str | None, take: str | None, kind: str
) -> None:
    def d(x: str | None) -> Decimal | None:
        return None if x is None else Decimal(x)

    assert classify_exit(engine_order=engine, fill=Decimal(fill), stop=d(stop), take=d(take)) == kind


def _exit_row(**overrides: Any) -> dict[str, Any]:
    row: dict[str, Any] = {
        "strategy_trade_id": 1,
        "broker_position_id": 2,
        "instrument_id": 7,
        "symbol": "X",
        "n_closes": 1,
        "fill": Decimal("93"),
        "closed_at": AT,
        "stop_level": Decimal("95"),
        "take_level": Decimal("120"),
        "order_id": None,
        "created_at": None,
        "outcome": None,
        "received_at": None,
        "quoted_at": None,
        "bid": None,
        "ask": None,
        "what_if_outcome": None,
        "what_if_cost": None,
        "ticket_amount": None,
        "session_open": Decimal("94"),
        "session_low": Decimal("92"),
        "session_high": Decimal("96"),
        "entry_fill": Decimal("101"),
    }
    return row | overrides


def test_a_stop_hit_is_costed_against_its_level_and_flags_a_gap_through_it() -> None:
    e = exit_tca(_exit_row())
    assert (e.kind, e.level, e.gapped, e.gap_unmeasured, e.unmeasured) == ("stop_loss", Decimal("95"), True, None, None)
    assert e.level_cost == 1 - Decimal("93") / Decimal("95")
    assert e.execution is None and e.what_if_cost is None


def test_a_take_profit_without_a_gap_and_a_missing_candle() -> None:
    candle = {"session_open": Decimal("110"), "session_low": Decimal("109"), "session_high": Decimal("121")}
    assert exit_tca(_exit_row(fill=Decimal("120.5"), **candle)).gapped is False
    e = exit_tca(_exit_row(fill=Decimal("120.5"), session_open=None))
    assert (e.kind, e.gapped, e.gap_unmeasured) == ("take_profit", None, "session_candle_missing")
    assert e.level_cost == 1 - Decimal("120.5") / Decimal("120")  # negative: filled better than the target


def test_an_engine_close_is_measured_against_the_arrival_mid_and_the_close_what_if() -> None:
    e = exit_tca(
        _exit_row(
            order_id=5,
            created_at=AT,
            fill=Decimal("99.95"),
            outcome="quoted",
            received_at=AT,
            quoted_at=AT - timedelta(seconds=2),
            bid=Decimal("99.9"),
            ask=Decimal("100.1"),
            what_if_outcome="quoted",
            what_if_cost=Decimal("0.6"),
            ticket_amount=Decimal("1000"),
        )
    )
    assert (e.kind, e.unmeasured, e.arrival_unmeasured) == ("engine", None, None)
    assert e.execution == Decimal("0.0005")  # sold 5 cents under a 100 mid
    assert e.effective_spread == Decimal("0.001")
    assert e.quoted_spread == Decimal("0.002")
    assert e.what_if_cost == Decimal("0.0006")
    assert e.level is None and e.gapped is None


@pytest.mark.parametrize(
    ("overrides", "unmeasured", "arrival_unmeasured"),
    [
        ({"what_if_outcome": "failed"}, "what_if_failed", "arrival_missing"),
        ({"what_if_outcome": "undecodable"}, "what_if_undecodable", "arrival_missing"),
        ({}, "what_if_missing", "arrival_missing"),
    ],
)
def test_an_engine_close_names_what_it_could_not_measure(
    overrides: dict[str, Any], unmeasured: str, arrival_unmeasured: str
) -> None:
    e = exit_tca(_exit_row(order_id=5, created_at=AT, **overrides))
    assert (e.kind, e.unmeasured, e.arrival_unmeasured) == ("engine", unmeasured, arrival_unmeasured)


def test_several_close_events_or_a_bad_fill_are_not_measured() -> None:
    assert exit_tca(_exit_row(n_closes=2)).unmeasured == "multiple_closes"
    assert exit_tca(_exit_row(fill=Decimal("0"))).unmeasured == "fill_price_unusable"


def test_a_candle_whose_range_cannot_hold_the_fill_is_not_compared_with_the_level() -> None:
    # A later 4:1 split rebases price_daily: the session that opened at 105 now reads 26.25, which would look like a
    # gap through a nominal stop of 100. The fill of 99 lies outside the rebased range, so no gap is claimed.
    e = exit_tca(
        _exit_row(
            fill=Decimal("99"),
            stop_level=Decimal("100"),
            session_open=Decimal("26.25"),
            session_low=Decimal("24.5"),
            session_high=Decimal("26.5"),
        )
    )
    assert (e.kind, e.gapped, e.gap_unmeasured) == ("stop_loss", None, "session_candle_off_basis")
    assert e.level_cost == Decimal("0.01")


def test_an_engine_close_without_a_live_mark_is_unpriced() -> None:
    e = exit_tca(_exit_row(order_id=5, created_at=AT, what_if_outcome="unpriced"))
    assert (e.kind, e.unmeasured, e.what_if_cost) == ("engine", "what_if_unpriced", None)


def test_fills_are_grouped_by_week_side_and_the_entry_fills_band() -> None:
    # AT is Monday 2026-11-02; the second entry fills the next Monday, a different week.
    entries = [
        entry_tca(_row()),  # fill 100.05: >=$100, execution 5 bps, what-if 10 bps
        entry_tca(_row(fill=Decimal("50"), filled_at=AT + timedelta(days=1))),  # $20-100, same week
        entry_tca(_row(filled_at=AT + timedelta(days=7))),
        entry_tca(_row(n_opens=0, fill=None, filled_at=None)),  # no fill: not counted
    ]
    engine = exit_tca(
        _exit_row(
            order_id=5,
            created_at=AT,
            fill=Decimal("99.95"),
            entry_fill=Decimal("19"),  # banded by the ENTRY fill, as cost_model fixes it for the position's life
            outcome="quoted",
            received_at=AT,
            quoted_at=AT - timedelta(seconds=2),
            bid=Decimal("99.9"),
            ask=Decimal("100.1"),
        )
    )
    unbanded = exit_tca(_exit_row(order_id=6, created_at=AT, fill=Decimal("99.95"), entry_fill=None))
    stop = exit_tca(_exit_row())  # a stop close has no spread-comparable figure
    rows = band_rows(entries, [engine, unbanded, stop])
    keys = [(r.week.isoformat(), r.side, None if r.band is None else r.band.label, r.fills) for r in rows]
    assert keys == [
        ("2026-11-02", "entry", "$20-100", 1),
        ("2026-11-02", "entry", ">=$100", 1),
        ("2026-11-02", "exit", "$5-20", 1),
        ("2026-11-02", "exit", None, 1),
        ("2026-11-09", "entry", ">=$100", 1),
    ]
    assert rows[1].executions == (Decimal("0.0005"),)
    assert rows[1].what_ifs == (Decimal("0.001"),)
    assert rows[2].executions == (Decimal("0.0005"),) and rows[2].what_ifs == ()


def test_a_band_rows_model_figures_are_the_cost_models_fractions() -> None:
    (row,) = band_rows([entry_tca(_row())], [])  # fill 100.05: the >=$100 band, frozen p75 0.322 percent
    assert row.model_half_spread == Decimal("0.00161")  # per side: 0.161 percent
    assert row.model_spread == Decimal("0.00322")  # both sides: 0.322 percent = 32.2 bps


def test_a_naive_fill_time_is_refused_rather_than_read_as_local_time() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        band_rows([entry_tca(_row(filled_at=AT.replace(tzinfo=None)))], [])
