"""#2603 sell leg — pure halves: the step-6 outcome map and the sell refusals.

Spec: ``docs/proposals/ta/2026-09-23-core-sell-leg-close-rebuy.md`` §1.  The DB-backed
flow (close, resume, quarantine, link integrity) is ``test_2603_core_sell_leg_db.py``.
"""

from __future__ import annotations

from contextlib import nullcontext
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch
from uuid import UUID, uuid4

import pytest
from psycopg.pq import TransactionStatus

from app.providers.broker import BrokerCostComponent, BrokerDirectPositionInvestment, BrokerWhatIfCostResponse
from app.services.broker_closed_release import RELEASE_REASON
from app.services.strategy_core_allocator import CoreMandate, CoreSleeveState
from app.services.strategy_core_executor import (
    _CoreSellTarget,
    _execute_core_sell,
    _LinkedClose,
    map_core_close_outcome,
)
from app.services.strategy_core_mandate import CORE_MANDATE_POLICY_VERSION

_NOW = datetime(2026, 9, 11, 14, 30, tzinfo=UTC)
_POSITION_ID = 950001
_INSTRUMENT_ID = 3417
_CREDS = (UUID("ba39f751-d4bd-4553-ab25-d9acbb73fbe8"), UUID("f7306e0b-9494-415e-85fd-97874510cc83"))


@pytest.mark.parametrize(
    ("linked", "manager", "expected"),
    [
        (
            None,
            ("reconcile_required", "owned_position_missing", None),
            ("reconcile_required", "owned_position_missing"),
        ),
        (None, ("applied", RELEASE_REASON, None), ("refused", "core_position_closed_by_broker")),
        (None, ("applied", "position_protected", 4), ("refused", "core_rebalance_close_not_started")),
        (
            None,
            ("rejected", "core_rebalance_ownership_mismatch", None),
            ("refused", "core_rebalance_close_not_started"),
        ),
        (
            _LinkedClose(7, "submitted", None),
            ("submitted", "broker_close_accepted", 8),
            ("reconcile_required", "core_rebalance_result_mismatch"),
        ),
        (_LinkedClose(7, "submitted", None), ("submitted", "x", 7), ("submitted", "core_rebalance_close_submitted")),
        (_LinkedClose(7, "applied", None), ("applied", "x", 7), ("closed", "core_rebalance_close_applied")),
        (
            _LinkedClose(7, "rejected", "broker_close_rejected"),
            ("rejected", "x", 7),
            ("refused", "broker_close_rejected"),
        ),
        (
            _LinkedClose(7, "reconcile_required", "broker_close_uncertain"),
            ("reconcile_required", "x", 7),
            ("reconcile_required", "broker_close_uncertain"),
        ),
        (
            _LinkedClose(7, "submitting", None),
            ("submitted", "x", 7),
            ("reconcile_required", "core_rebalance_close_unresolved"),
        ),
        # The manager RAISED: no id to compare, the linked status alone decides.
        (_LinkedClose(7, "applied", None), (None, None, None), ("closed", "core_rebalance_close_applied")),
        (
            _LinkedClose(7, "intent_persisted", None),
            (None, None, None),
            ("reconcile_required", "core_rebalance_close_unresolved"),
        ),
    ],
)
def test_the_outcome_is_read_off_the_linked_operation(
    linked: _LinkedClose | None, manager: tuple[Any, Any, Any], expected: tuple[str, str]
) -> None:
    state, reason, operation_id = manager
    assert (
        map_core_close_outcome(
            linked=linked, manager_state=state, manager_reason=reason, manager_operation_id=operation_id
        )
        == expected
    )


class _Conn:
    def __init__(self) -> None:
        self.info = SimpleNamespace(transaction_status=TransactionStatus.IDLE)
        self.inserted: list[tuple[Any, ...]] = []

    def transaction(self) -> nullcontext[None]:
        return nullcontext()

    def execute(self, sql: str, params: tuple[Any, ...] = ()) -> None:
        assert "INSERT INTO strategy_core_rebalance_close_quotes" in sql
        self.inserted.append(params)


class _Broker:
    def __init__(self, cost: str = "0.10", *, raises: bool = False) -> None:
        self.cost = cost
        self.raises = raises
        self.orders: list[Any] = []

    def get_what_if_costs(self, order: Any) -> BrokerWhatIfCostResponse:
        self.orders.append(order)
        if self.raises:
            raise RuntimeError("what-if down")
        return BrokerWhatIfCostResponse(
            instrument_id=_INSTRUMENT_ID,
            symbol="SPY",
            costs=(BrokerCostComponent("marketSpread", None, Decimal(self.cost), "USD", {}),),
            last_updated=_NOW,
            raw_payload={},
        )


def _mandate(**overrides: Any) -> CoreMandate:
    base: dict[str, Any] = {
        "event_id": 1,
        "revision": 1,
        "policy_version": CORE_MANDATE_POLICY_VERSION,
        "enabled": True,
        "base_currency": "USD",
        "core_instrument_id": _INSTRUMENT_ID,
        "core_target_pct": Decimal("30"),
        "liquidity_reserve_pct": Decimal("5"),
        "rebalance_band_pct": Decimal("5"),
        "min_rebalance_amount": Decimal("25"),
    }
    base.update(overrides)
    return CoreMandate(**base)


def _sell(
    broker: _Broker,
    *,
    mandate: CoreMandate | None = None,
    lower_pct: Decimal = Decimal("25"),
    target: _CoreSellTarget | str | None = None,
    market_value: str = "550",
    remaining: str = "450",
    min_position_amount: Decimal = Decimal("10"),
) -> tuple[Any, _Conn, Any]:
    conn = _Conn()
    position = BrokerDirectPositionInvestment(
        position_id=_POSITION_ID,
        instrument_id=_INSTRUMENT_ID,
        is_buy=True,
        units=Decimal("1"),
        amount=Decimal(market_value),
        unrealized_pnl=Decimal("0"),
        market_value=Decimal(market_value),
        is_partially_altered=False,
        close_rate=Decimal(market_value),
        close_conversion_rate=Decimal("1"),
        asset_currency_id=1,
    )
    snapshot = SimpleNamespace(available_cash=Decimal("450"), observed_at=_NOW, direct_positions=(position,))
    usage = SimpleNamespace(
        core_market_value=Decimal(market_value), headroom=SimpleNamespace(remaining=Decimal(remaining))
    )
    state = CoreSleeveState(_INSTRUMENT_ID, Decimal(market_value), Decimal("450"), "USD", _NOW)
    intent = SimpleNamespace(core_rebalance_intent_id=11, decision=SimpleNamespace(lower_pct=lower_pct))
    proof = SimpleNamespace(
        response_currency="USD",
        min_position_exposure=Decimal("10"),
        min_position_amount=min_position_amount,
        api_key_credential_id=_CREDS[0],
        user_key_credential_id=_CREDS[1],
    )
    drive = SimpleNamespace(state="submitted", reason_code="core_rebalance_close_submitted")
    with (
        patch(
            "app.services.strategy_core_executor._core_sell_target",
            return_value=target or _CoreSellTarget(3, 21, _POSITION_ID, _CREDS),
        ),
        patch("app.services.strategy_core_executor._drive_core_close", return_value=drive) as driven,
    ):
        result = _execute_core_sell(
            conn,  # type: ignore[arg-type]
            broker=broker,  # type: ignore[arg-type]
            mandate=mandate or _mandate(),
            intent=intent,  # type: ignore[arg-type]
            snapshot=snapshot,  # type: ignore[arg-type]
            usage=usage,  # type: ignore[arg-type]
            state=state,
            proof=proof,  # type: ignore[arg-type]
            clock=lambda: _NOW + timedelta(seconds=1),
        )
    return result, conn, driven


def test_a_sell_quotes_the_close_arm_of_the_whole_position_then_hands_it_over() -> None:
    broker = _Broker()
    result, conn, driven = _sell(broker)

    assert result.reason_code == "core_rebalance_close_submitted"
    (order,) = broker.orders
    assert (order.action, order.transaction, order.position_ids, order.amount) == (
        "close",
        "sell",
        (_POSITION_ID,),
        Decimal("550"),
    )
    (quote,) = conn.inserted
    assert quote[:2] == (11, _POSITION_ID)
    assert driven.call_args.kwargs == {
        "broker": broker,
        "intent_id": 11,
        "strategy_trade_id": 21,
        "broker_position_id": _POSITION_ID,
        "ownership_id": 3,
        "submit": True,
    }


@pytest.mark.parametrize(
    ("kwargs", "code"),
    [
        ({"target": "core_sell_spans_positions"}, "core_sell_spans_positions"),
        ({"target": "core_operation_outstanding"}, "core_operation_outstanding"),
        ({"lower_pct": Decimal("0")}, "core_sell_would_strand_at_zero_lower"),
        ({"mandate": _mandate(rebalance_band_pct=Decimal("0"))}, "core_sell_zero_width_band"),
        ({"target": _CoreSellTarget(3, 21, 1, _CREDS)}, "core_sell_position_unobserved"),
        ({"target": _CoreSellTarget(3, 21, _POSITION_ID, (uuid4(), _CREDS[1]))}, "core_credential_provenance_changed"),
        ({"target": _CoreSellTarget(3, 21, _POSITION_ID, None)}, "core_credential_provenance_changed"),
        # The lower-edge rebuy (25% of a ~$100 post-close sleeve) is below a $50 floor.
        (
            {"market_value": "100", "remaining": "0", "mandate": _mandate(min_rebalance_amount=Decimal("50"))},
            "core_rebuy_below_minimum",
        ),
        # Over the bound, the UNCLAMPED headroom nets the deficit: -80 + 100 = $20 sleeve.
        ({"market_value": "100", "remaining": "-80"}, "core_rebuy_below_minimum"),
    ],
)
def test_every_sell_refusal_precedes_the_quote_row_and_the_manager(kwargs: dict[str, Any], code: str) -> None:
    result, conn, driven = _sell(_Broker(), **kwargs)

    assert (result.state, result.reason_code, result.intent_id) == ("refused", code, 11)
    assert conn.inserted == []
    driven.assert_not_called()


@pytest.mark.parametrize(
    ("broker", "code"),
    [
        (_Broker(raises=True), "core_close_side_cost_quote_unavailable"),
        (_Broker(cost="600"), "core_close_cost_implausible"),
    ],
)
def test_an_unusable_close_quote_refuses(broker: _Broker, code: str) -> None:
    result, conn, driven = _sell(broker)

    assert result.reason_code == code
    assert conn.inserted == []
    driven.assert_not_called()
