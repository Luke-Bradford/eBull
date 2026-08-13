from datetime import UTC, datetime, timedelta
from decimal import Decimal
from json import JSONDecodeError
from unittest.mock import MagicMock

from app.providers.broker import (
    BrokerCostComponent,
    BrokerInstrumentEligibility,
    BrokerLeverageConfig,
    BrokerWhatIfCostResponse,
)
from app.providers.implementations.etoro_broker import TradingPreflightParseError
from scripts.verify_2437_trading_preflight import (
    CENSUS_VERSION,
    MAX_COST_REQUESTS_PER_RUN,
    _bounded_cost_arms,
    _canonical_payload_bytes,
    _classify_scaling,
    _cost_orders,
    _cost_response_usable,
    _fetch_cost,
    _freshness,
    _interleave_cost_arms,
    _type_quotas,
)


def _config(settlement: str, direction: str, minimum: str = "10") -> BrokerLeverageConfig:
    return BrokerLeverageConfig(
        settlement_type=settlement,
        direction=direction,
        leverage_values=(1, 2),
        min_position_amount=Decimal(minimum),
        allow_edit_stop_loss=True,
        allow_edit_take_profit=True,
        allow_stop_loss_take_profit=True,
        raw_payload={},
    )


def _eligibility() -> BrokerInstrumentEligibility:
    return BrokerInstrumentEligibility(
        instrument_id=101,
        symbol="TEST",
        min_position_exposure=Decimal("50"),
        max_units_per_order=Decimal("1000"),
        allow_open_position=True,
        allow_close_position=True,
        allow_partial_close_position=True,
        allow_trailing_stop_loss=True,
        leverage_configs=(
            _config("REAL", "LONG"),
            _config("CFD", "SHORT", "250"),
            _config("CFD", "LONG"),
        ),
        raw_payload={},
    )


def _cost_response(*, amount: Decimal | None, value: Decimal | None, currency: str = "USD") -> BrokerWhatIfCostResponse:
    return BrokerWhatIfCostResponse(
        instrument_id=101,
        symbol="TEST",
        costs=(
            BrokerCostComponent(
                cost_type="marketSpread",
                amount=amount,
                value=value,
                currency=currency,
                raw_payload={},
            ),
        ),
        last_updated=datetime(2026, 8, 9, tzinfo=UTC),
        raw_payload={},
    )


def test_type_quotas_are_bounded_balanced_and_deterministic() -> None:
    assert _type_quotas(8) == {"Stocks": 4, "ETF": 4}
    assert _type_quotas(7) == {"Stocks": 4, "ETF": 3}


def test_cost_arms_are_versioned_scaled_and_restricted_to_authorised_shapes() -> None:
    arms = _cost_orders(_eligibility())

    assert len(arms) == 4
    assert [arm[2].transaction for arm in arms] == ["buy", "buy", "sellShort", "sellShort"]
    assert [arm[2].settlement_type for arm in arms] == ["real", "real", "cfd", "cfd"]
    assert [arm[2].amount for arm in arms] == [Decimal("100"), Decimal("1000"), Decimal("250"), Decimal("2500")]
    assert all(arm[0].startswith(f"{CENSUS_VERSION}:101:") for arm in arms)


def test_cost_arms_skip_refused_instruments_and_interleave_types_in_complete_pairs() -> None:
    refused = BrokerInstrumentEligibility(
        **{**_eligibility().__dict__, "instrument_id": 303, "allow_open_position": False}
    )
    etf = BrokerInstrumentEligibility(**{**_eligibility().__dict__, "instrument_id": 202})

    arms = _interleave_cost_arms((_eligibility(), refused, etf), {101: "Stocks", 202: "ETF", 303: "Stocks"})

    assert [arm[2].instrument_id for arm in arms] == [101, 101, 202, 202, 101, 101, 202, 202]
    assert all(arm[2].instrument_id != 303 for arm in arms)


def test_cost_request_budget_cannot_exceed_endpoint_limit_or_split_scaling_pair() -> None:
    arms = _cost_orders(_eligibility()) * 10

    assert len(_bounded_cost_arms(arms, MAX_COST_REQUESTS_PER_RUN)) == 20
    for invalid in (1, 3, MAX_COST_REQUESTS_PER_RUN + 2):
        try:
            _bounded_cost_arms(arms, invalid)
        except ValueError:
            pass
        else:  # pragma: no cover - assertion helper branch
            raise AssertionError(f"invalid request cap accepted: {invalid}")


def test_malformed_cost_arm_is_reported_without_aborting_census() -> None:
    order = _cost_orders(_eligibility())[0][2]
    for error in (TradingPreflightParseError("drift"), JSONDecodeError("not JSON", "", 0)):
        broker = MagicMock()
        broker.get_what_if_costs.side_effect = error

        result, error_type = _fetch_cost(broker, order)

        assert result is None
        assert error_type == type(error).__name__


def test_programming_value_error_is_not_hidden_as_an_arm_error() -> None:
    broker = MagicMock()
    broker.get_what_if_costs.side_effect = ValueError("local bug")
    order = _cost_orders(_eligibility())[0][2]

    try:
        _fetch_cost(broker, order)
    except ValueError as exc:
        assert str(exc) == "local bug"
    else:  # pragma: no cover - assertion helper branch
        raise AssertionError("unrelated ValueError was hidden as a provider arm error")


def test_scaling_equation_classifies_proportional_invariant_and_incomplete_fields() -> None:
    rows: list[dict[str, object]] = []
    for multiplier, proportional, invariant in (("1", "2", "7"), ("10", "20", "7")):
        rows.append(
            {
                "instrument_id": 101,
                "transaction": "buy",
                "settlement_type": "real",
                "multiplier": multiplier,
                "costs": [
                    {"cost_type": "spread", "currency": "USD", "amount": proportional, "value": None},
                    {"cost_type": "fee", "currency": "USD", "amount": None, "value": invariant},
                ],
            }
        )
    rows.append(
        {
            "instrument_id": 202,
            "transaction": "buy",
            "settlement_type": "real",
            "multiplier": "1",
            "costs": [{"cost_type": "fee", "currency": "USD", "amount": "1", "value": None}],
        }
    )

    classifications = _classify_scaling(rows)
    by_key = {(row["instrument_id"], row["cost_type"], row["field"]): row for row in classifications}

    assert by_key[(101, "spread", "amount")]["relationship"] == "ticket_proportional"
    assert by_key[(101, "spread", "amount")]["execution_semantics"] == "documented_order_currency_amount"
    assert by_key[(101, "fee", "value")]["relationship"] == "ticket_invariant"
    assert by_key[(101, "fee", "value")]["execution_semantics"] == "undocumented_blocking"
    assert by_key[(202, "fee", "amount")]["relationship"] == "incomplete_blocking"


def test_freshness_and_cost_semantics_fail_closed() -> None:
    observed_at = datetime(2026, 8, 9, 12, tzinfo=UTC)
    _, current_status = _freshness(observed_at - timedelta(minutes=5), observed_at)
    _, stale_status = _freshness(observed_at - timedelta(days=2), observed_at)
    _, future_status = _freshness(observed_at + timedelta(seconds=1), observed_at)

    assert current_status == "within_24h"
    assert stale_status == "stale_blocking"
    assert future_status == "future_timestamp_blocking"
    assert _cost_response_usable(_cost_response(amount=Decimal("1"), value=None), current_status) == (True, [])
    assert _cost_response_usable(_cost_response(amount=None, value=Decimal("1")), current_status) == (
        False,
        ["undocumented_value_semantics:marketSpread"],
    )
    assert _cost_response_usable(_cost_response(amount=Decimal("1"), value=None), stale_status) == (
        False,
        ["stale_blocking"],
    )
    assert _cost_response_usable(_cost_response(amount=Decimal("1"), value=None, currency="GBP"), current_status) == (
        False,
        ["unknown_currency:GBP"],
    )


def test_canonical_payload_size_is_key_order_independent() -> None:
    assert _canonical_payload_bytes({"b": 1, "a": 2}) == _canonical_payload_bytes({"a": 2, "b": 1})
