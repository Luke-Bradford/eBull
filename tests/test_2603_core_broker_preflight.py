"""#2603 step 3b-2: the BROKER half of the core submission refusal vocabulary.

Pure-logic tier -- no DB, no clock, no network.  The broker is a double whose only job
is to return a canned answer or raise, because every refusal under test is about what the
broker SAID, not about how it was reached.

Spec: ``docs/proposals/ta/2026-08-23-core-broker-preflight.md``.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest

from app.providers.broker import (
    BrokerAccountRiskSnapshot,
    BrokerCostComponent,
    BrokerInstrumentInvestment,
    BrokerWhatIfCostResponse,
    BrokerWhatIfOrder,
)
from app.services.broker_settlement_arms import (
    UNDERLYING_SETTLEMENT_TYPE,
    UNLEVERAGED_LEVERAGE,
)
from app.services.strategy_core_allocator import CoreRebalanceDecision, evaluate_core_rebalance
from app.services.strategy_core_broker_preflight import (
    CORE_MAX_ACCOUNT_RISK_AGE_SECONDS,
    StrategyCoreBrokerPreflightError,
    assess_core_broker_preflight,
)
from app.services.strategy_core_mandate import CORE_MANDATE_POLICY_VERSION, CoreMandate

CORE_ID = 4242
NOW = datetime(2026, 8, 23, 14, 30, tzinfo=UTC)

# 50% core on a 60+-5 mandate -> buy to the 55 edge: 0.55*1000 - 500 = 50.
_CORE_MV = "500"
_CASH = "500"
_EXPECTED_BUY = Decimal("50")


def mandate(*, base_currency: str = "USD", floor: str = "1") -> CoreMandate:
    return CoreMandate(
        event_id=1,
        revision=1,
        enabled=True,
        base_currency=base_currency,
        core_instrument_id=CORE_ID,
        core_target_pct=Decimal("60"),
        liquidity_reserve_pct=Decimal("5"),
        rebalance_band_pct=Decimal("5"),
        min_rebalance_amount=Decimal(floor),
        policy_version=CORE_MANDATE_POLICY_VERSION,
    )


def snapshot(
    *,
    core_market_value: str = _CORE_MV,
    cash: str = _CASH,
    observed_at: datetime = NOW,
    currency_id: int | None = 1,
    shorts: int = 0,
) -> BrokerAccountRiskSnapshot:
    return BrokerAccountRiskSnapshot(
        available_cash=Decimal(cash),
        total_invested=Decimal(core_market_value),
        unrealized_pnl=Decimal("0"),
        equity=Decimal(core_market_value) + Decimal(cash),
        instrument_investments=(
            BrokerInstrumentInvestment(CORE_ID, Decimal(core_market_value), Decimal(core_market_value), 1, shorts),
        ),
        observed_at=observed_at,
        raw_payload={},
        account_currency_id=currency_id,
    )


def cost_response(
    *,
    amount: str = "0.10",
    instrument_id: int = CORE_ID,
    last_updated: datetime = NOW,
) -> BrokerWhatIfCostResponse:
    return BrokerWhatIfCostResponse(
        instrument_id=instrument_id,
        symbol="IVV",
        costs=(
            BrokerCostComponent(
                cost_type="marketSpread", amount=None, value=Decimal(amount), currency="USD", raw_payload={}
            ),
        ),
        last_updated=last_updated,
        raw_payload={},
    )


class FakeBroker:
    """Records what it was asked; raises where a test wants the failure branch."""

    def __init__(
        self,
        *,
        snapshot_result: BrokerAccountRiskSnapshot | Exception | None = None,
        cost_result: BrokerWhatIfCostResponse | Exception | None = None,
    ) -> None:
        self._snapshot = snapshot() if snapshot_result is None else snapshot_result
        self._cost = cost_response() if cost_result is None else cost_result
        self.snapshot_calls = 0
        self.cost_orders: list[BrokerWhatIfOrder] = []

    def get_account_risk_snapshot(self) -> BrokerAccountRiskSnapshot:
        self.snapshot_calls += 1
        if isinstance(self._snapshot, Exception):
            raise self._snapshot
        return self._snapshot

    def get_what_if_costs(self, order: BrokerWhatIfOrder) -> BrokerWhatIfCostResponse:
        self.cost_orders.append(order)
        if isinstance(self._cost, Exception):
            raise self._cost
        return self._cost


def decision(*, amount: Decimal | None = None) -> CoreRebalanceDecision:
    """The verdict the intent recorded -- by default the one the world still yields."""
    fresh = evaluate_core_rebalance(mandate(), _state())
    if amount is None:
        return fresh
    return CoreRebalanceDecision(**{**fresh.__dict__, "amount": amount})


def _state() -> Any:
    from app.services.strategy_core_sleeve import observe_core_sleeve

    return observe_core_sleeve(snapshot(), core_instrument_id=CORE_ID)


def assess(broker: FakeBroker, **overrides: Any) -> Any:
    kwargs: dict[str, Any] = {
        "mandate": mandate(),
        "decision": decision(),
        "core_instrument_id": CORE_ID,
        "eligibility_response_currency": "USD",
        "eligibility_min_position_exposure": Decimal("10"),
        "eligibility_min_position_amount": Decimal("10"),
        "clock": lambda: NOW,
    }
    kwargs.update(overrides)
    return assess_core_broker_preflight(broker, **kwargs)  # type: ignore[arg-type]


# --- the happy path, and the request shape the response cannot echo ----------


def test_a_live_buy_is_admitted_and_sized_after_cost() -> None:
    verdict = assess(FakeBroker())

    assert verdict.admitted is True
    assert verdict.reason_code is None
    # Cost is charged, so the solved size is not the pre-cost 50 exactly.
    assert verdict.amount > Decimal("0")
    assert verdict.cost_rate is not None
    assert verdict.snapshot_observed_at == NOW


def test_the_what_if_request_is_bound_to_the_decision_field_by_field() -> None:
    """The response echoes none of these, so nothing downstream can check them."""
    broker = FakeBroker()
    assess(broker)

    assert len(broker.cost_orders) == 1
    order = broker.cost_orders[0]
    assert order.instrument_id == CORE_ID
    assert order.action == "open"
    assert order.transaction == "buy"
    assert order.settlement_type == UNDERLYING_SETTLEMENT_TYPE
    assert order.leverage == UNLEVERAGED_LEVERAGE
    assert order.amount == _EXPECTED_BUY
    assert order.units is None
    assert order.position_ids == ()


# --- the sell arm -----------------------------------------------------------


def test_a_sell_refuses_before_any_broker_call_is_spent() -> None:
    """#2712: the close arm needs position ids we do not hold, and an open quote
    under-states the close cost by up to 18.5x.  Refusing early also keeps a request
    we know returns 400 off the write lane."""
    broker = FakeBroker()
    sell = CoreRebalanceDecision(**{**decision().__dict__, "action": "sell_core"})

    verdict = assess(broker, decision=sell)

    assert verdict.reason_code == "core_close_side_cost_quote_unavailable"
    assert verdict.admitted is False
    assert broker.snapshot_calls == 0
    assert broker.cost_orders == []


@pytest.mark.parametrize("action", ["hold", "refused"])
def test_a_decision_that_is_not_a_trade_is_a_caller_bug_and_not_a_refusal(action: str) -> None:
    """Folding it into the vocabulary would make a caller learn about its own defect by
    catching a code that means the broker said something."""
    broker = FakeBroker()
    not_a_trade = CoreRebalanceDecision(**{**decision().__dict__, "action": action})

    with pytest.raises(StrategyCoreBrokerPreflightError, match="only a trade"):
        assess(broker, decision=not_a_trade)

    assert broker.snapshot_calls == 0


def test_an_instrument_that_is_not_the_mandates_keeps_the_allocators_own_code() -> None:
    """The sleeve is observed for the id the caller named, so a mismatch must surface
    as `sleeve_instrument_mismatch` rather than as a cost quote for the wrong name."""
    verdict = assess(FakeBroker(), core_instrument_id=CORE_ID + 1)

    assert verdict.reason_code == "sleeve_instrument_mismatch"


# --- account risk -----------------------------------------------------------


def test_an_unreachable_account_is_one_condition_however_it_failed() -> None:
    verdict = assess(FakeBroker(snapshot_result=RuntimeError("503")))

    assert verdict.reason_code == "core_account_risk_unavailable"
    assert verdict.snapshot_observed_at is None


@pytest.mark.parametrize(
    ("age_seconds", "expected"),
    [
        (CORE_MAX_ACCOUNT_RISK_AGE_SECONDS, None),
        (CORE_MAX_ACCOUNT_RISK_AGE_SECONDS + 1, "core_account_risk_stale"),
    ],
)
def test_the_age_bound_is_inclusive_at_the_bound_and_refuses_one_second_past(
    age_seconds: int, expected: str | None
) -> None:
    broker = FakeBroker(snapshot_result=snapshot(observed_at=NOW - timedelta(seconds=age_seconds)))

    verdict = assess(broker)

    assert verdict.reason_code == expected


def test_a_stamp_far_in_the_future_is_stale_and_not_maximally_fresh() -> None:
    """Without this a corrupted future timestamp never ages out."""
    broker = FakeBroker(snapshot_result=snapshot(observed_at=NOW + timedelta(seconds=6)))

    verdict = assess(broker)

    assert verdict.reason_code == "core_account_risk_stale"


def test_a_stamp_inside_the_skew_tolerance_is_still_usable() -> None:
    broker = FakeBroker(snapshot_result=snapshot(observed_at=NOW + timedelta(seconds=4)))

    assert assess(broker).admitted is True


def test_a_naive_stamp_refuses_rather_than_raising_on_the_subtraction() -> None:
    broker = FakeBroker(snapshot_result=snapshot(observed_at=NOW.replace(tzinfo=None)))

    verdict = assess(broker)

    assert verdict.reason_code == "core_account_risk_unobservable"


def test_a_snapshot_that_cannot_describe_the_sleeve_refuses() -> None:
    """A direct short on the core instrument: `observe_core_sleeve` raises, and this
    module turns a raise into a verdict rather than propagating it."""
    broker = FakeBroker(snapshot_result=snapshot(shorts=1))

    verdict = assess(broker)

    assert verdict.reason_code == "core_account_risk_unobservable"
    assert verdict.snapshot_observed_at == NOW


# --- drift ------------------------------------------------------------------


def test_a_decision_the_world_no_longer_yields_is_refused_as_drift() -> None:
    """The recorded amount is stale against a sleeve that has since moved.  Refusing
    here is what makes `_assert_decision_describes` unreachable from this path."""
    verdict = assess(FakeBroker(), decision=decision(amount=Decimal("49")))

    assert verdict.reason_code == "core_sleeve_moved_since_decision"


def test_the_broker_floor_biting_keeps_the_allocators_own_code() -> None:
    """`below_min_rebalance_amount` is the floor refusing, and it is passed through
    rather than re-coded -- two names on one condition loses which layer decided."""
    broker = FakeBroker()

    verdict = assess(broker, eligibility_min_position_exposure=Decimal("100"))

    assert verdict.reason_code == "below_min_rebalance_amount"
    assert broker.cost_orders == []


def test_an_unquoted_minimum_fails_closed_and_is_not_passed_on_as_no_minimum() -> None:
    """`effective_open_minimum` returning None means the broker quoted no usable
    threshold; the allocator's `broker_minimum=None` means the CALLER has none to
    supply.  Conflating them reads an unanswered question as an answered one."""
    verdict = assess(
        FakeBroker(),
        eligibility_min_position_exposure=None,
        eligibility_min_position_amount=None,
    )

    assert verdict.reason_code == "core_broker_open_minimum_unquoted"


def test_a_non_usd_eligibility_response_refuses_before_the_minimum_can_raise() -> None:
    verdict = assess(FakeBroker(), eligibility_response_currency="GBP")

    assert verdict.reason_code == "core_minimum_currency_unsupported"


def test_the_binding_age_check_runs_after_the_cost_call_not_before_it() -> None:
    """The regression this module's whole bound exists for.  An earlier draft tested the
    age once, on entry -- i.e. before the ONE call whose duration the bound covers -- so a
    request delayed by throttling, retries or an uncapped `Retry-After` still returned an
    ADMITTED verdict on an arbitrarily old snapshot.  The clock is read twice; this fake
    advances past the bound only on the second reading."""
    readings = iter([NOW, NOW + timedelta(seconds=CORE_MAX_ACCOUNT_RISK_AGE_SECONDS + 1)])
    broker = FakeBroker()

    verdict = assess(broker, clock=lambda: next(readings))

    assert verdict.reason_code == "core_account_risk_stale"
    assert verdict.admitted is False
    # The pre-check passed, so the request WAS spent -- which is what makes the second
    # check the one that binds.
    assert len(broker.cost_orders) == 1


def test_a_slow_cost_call_that_stays_inside_the_bound_is_still_admitted() -> None:
    """The bound must bite on a stall without refusing an ordinary round trip."""
    readings = iter([NOW, NOW + timedelta(seconds=CORE_MAX_ACCOUNT_RISK_AGE_SECONDS)])

    assert assess(FakeBroker(), clock=lambda: next(readings)).admitted is True


# --- cost -------------------------------------------------------------------


def test_an_unreachable_cost_endpoint_refuses_after_the_snapshot_was_taken() -> None:
    broker = FakeBroker(cost_result=RuntimeError("timeout"))

    verdict = assess(broker)

    assert verdict.reason_code == "core_cost_assessment_unavailable"
    assert verdict.snapshot_observed_at == NOW


def test_a_decode_refusal_is_passed_through_with_the_sizing_layers_own_code() -> None:
    """The response is for a different instrument -- the one identity the decoder can
    check.  `cost_quote_unusable` is the sizing layer's code and stays its code."""
    broker = FakeBroker(cost_result=cost_response(instrument_id=CORE_ID + 1))

    verdict = assess(broker)

    assert verdict.reason_code == "cost_quote_unusable"
    assert verdict.admitted is False


def test_a_cost_quote_older_than_the_sleeve_valuation_is_passed_through_as_stale() -> None:
    broker = FakeBroker(cost_result=cost_response(last_updated=NOW - timedelta(hours=2)))

    verdict = assess(broker)

    assert verdict.reason_code == "cost_quote_stale"


def test_a_sizing_refusal_with_no_code_raises_rather_than_returning_an_unnamed_one() -> None:
    """`python -O` strips asserts, and the stripped form of the guard this replaces would
    return `admitted=False` with `reason_code=None` -- an unnamed refusal, which is the one
    thing the closed vocabulary exists to prevent."""
    from app.services import strategy_core_broker_preflight as module
    from app.services.strategy_core_sizing import CoreSizingResult

    codeless = CoreSizingResult(
        sized=False,
        refusal_code=None,
        amount=Decimal("0"),
        pre_cost_amount=_EXPECTED_BUY,
        cost_rate=None,
        resulting_core_pct_at_bound=None,
        resulting_core_pct_at_zero_cost=None,
    )
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(module, "resolve_core_trade_size", lambda *_args, **_kwargs: codeless)

        with pytest.raises(StrategyCoreBrokerPreflightError, match="sizing contract is broken"):
            assess(FakeBroker())


@pytest.mark.parametrize(
    ("broker_kwargs", "message"),
    [
        ({"snapshot_result": RuntimeError("503")}, "account risk snapshot unavailable"),
        ({"cost_result": RuntimeError("timeout")}, "what-if cost quote unavailable"),
    ],
)
def test_a_swallowed_broker_exception_still_reaches_the_log_with_its_traceback(
    broker_kwargs: dict[str, Any], message: str, caplog: pytest.LogCaptureFixture
) -> None:
    """The refusal code cannot carry the cause -- a 503 and a TypeError in our own parse
    collapse to one code -- so a programming bug would otherwise be indistinguishable from
    a broker outage in production."""
    caplog.set_level("WARNING")

    assess(FakeBroker(**broker_kwargs))

    record = next(r for r in caplog.records if message in r.getMessage())
    assert record.exc_info is not None


# --- the derivation the age bound rests on ----------------------------------


def test_the_age_bound_still_matches_the_transport_it_was_derived_from() -> None:
    """A coupling test, not a restatement: the bound is one write-lane throttle wait
    plus one nominal HTTP round trip, and a provider retuning either must fail HERE
    rather than silently invalidating the derivation.

    ⚠ It deliberately does NOT assert a worst case.  The throttle is paid once per
    ATTEMPT and `Retry-After` overrides the backoff with no upper cap, so no worst case
    exists to bound -- see the constant's docstring.
    """
    from app.providers.implementations.etoro_broker import (
        _ETORO_HTTP_TIMEOUT_S,
        _ETORO_WRITE_INTERVAL_S,
    )

    assert _ETORO_WRITE_INTERVAL_S == 3.5
    assert _ETORO_HTTP_TIMEOUT_S == 30.0
    assert CORE_MAX_ACCOUNT_RISK_AGE_SECONDS == 34


def test_the_what_if_call_is_on_the_write_lane_the_bound_assumes() -> None:
    """The first draft of the bound used the 1.1s READ interval.  `get_what_if_costs`
    is an informational POST on `_http_write`, so it pays 3.5s."""
    import inspect

    from app.providers.implementations import etoro_broker

    source = inspect.getsource(etoro_broker.EtoroBrokerProvider.get_what_if_costs)
    assert "_http_write.post" in source
    assert "_http_read" not in source
