"""#3611 slice 2: exit evidence recorded before an engine close, and every close of an owned position measured."""

from datetime import timedelta
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo

import psycopg
import pytest
from psycopg.types.json import Jsonb

from app.providers.broker import BrokerCostComponent, BrokerPosition, BrokerWhatIfCostResponse
from app.providers.market_data import Quote
from app.services.execution_tca import exit_evidence, load_exit_tca
from app.services.strategy_position_manager import manage_owned_position
from tests.test_strategy_paper_executor import _NOW
from tests.test_strategy_position_manager import _POSITION_ID, _opened_trade

pytestmark = [pytest.mark.integration, pytest.mark.usefixtures("registered_strategy_test_candidates")]

Conn = psycopg.Connection[Any]

_INSTRUMENT_ID = 2449001  # `_opened_trade`'s instrument
_QUOTE = Quote(_INSTRUMENT_ID, _NOW - timedelta(seconds=1), Decimal("104.9"), Decimal("105.1"), None)


def _what_if(cost: str) -> BrokerWhatIfCostResponse:
    component = BrokerCostComponent("marketSpread", None, Decimal(cost), "USD", {"value": float(cost)})
    return BrokerWhatIfCostResponse(_INSTRUMENT_ID, "X", (component,), _NOW, {"costs": [{"value": float(cost)}]})


def _close_event(conn: Conn, *, price: str, stop: str = "0", take: str = "0") -> None:
    conn.execute(
        """
        INSERT INTO trade_events (position_id, etoro_instrument_id, instrument_id, event_kind, side, units, price,
                                  executed_at, source, raw_payload, environment)
        VALUES (%s, %s, %s, 'close', 'sell', 1, %s, %s, 'etoro_history', %s, 'demo')
        """,
        (
            _POSITION_ID,
            _INSTRUMENT_ID,
            _INSTRUMENT_ID,
            Decimal(price),
            _NOW + timedelta(seconds=5),
            Jsonb({"stopLossRate": float(stop), "takeProfitRate": float(take)}),
        ),
    )
    conn.commit()


def _close(conn: Conn, broker: Any, trade_id: int, *, quote: Quote | None, expected: list[str]) -> None:
    calls: list[str] = []
    original_close = broker.close_demo_strategy_position.side_effect
    broker.close_demo_strategy_position.side_effect = lambda **kw: (calls.append("close"), original_close(**kw))[1]
    what_if = broker.get_what_if_costs.side_effect

    def recorded_what_if(order: Any) -> Any:
        calls.append("what_if")
        return what_if(order)

    broker.get_what_if_costs.side_effect = recorded_what_if

    def arrival(_iid: int) -> Quote | None:
        calls.append("arrival")
        return quote

    result = manage_owned_position(
        conn,
        broker=broker,
        strategy_trade_id=trade_id,
        broker_position_id=_POSITION_ID,
        close_reason="strategy_exit",
        now=_NOW,
        exit_evidence=exit_evidence(conn, broker=broker, arrival_quote=arrival, clock=lambda: _NOW),
    )
    assert result.state == "submitted", result
    assert calls == expected


def test_an_engine_close_records_its_close_arm_cost_and_arrival_quote_then_is_measured(
    ebull_test_conn: Conn, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = ebull_test_conn
    trade_id, _, broker, _ = _opened_trade(conn, monkeypatch)
    broker.get_what_if_costs.side_effect = lambda _order: _what_if("0.63")
    # The quote sizes the what-if, and both are recorded before the close is sent.
    _close(conn, broker, trade_id, quote=_QUOTE, expected=["arrival", "what_if", "close"])

    order = broker.get_what_if_costs.call_args.args[0]
    assert (order.action, order.transaction, order.position_ids, order.amount) == (
        "close",
        "sell",
        (_POSITION_ID,),
        Decimal("104.90"),  # one unit at the arrival bid, not the portfolio's current_price (its open rate live)
    )
    row = conn.execute("SELECT outcome, cost, currency, ticket_amount FROM strategy_order_close_what_ifs").fetchone()
    assert row == ("quoted", Decimal("0.63"), "USD", Decimal("104.90"))

    _close_event(conn, price="104.95", stop="90", take="130")
    (e,) = load_exit_tca(conn, since=_NOW.date())
    assert (e.kind, e.unmeasured, e.arrival_unmeasured) == ("engine", None, None)
    assert e.execution == 1 - Decimal("104.95") / Decimal("105.0")
    assert e.what_if_cost == Decimal("0.63") / Decimal("104.90")


def test_a_failed_what_if_is_recorded_and_the_close_still_goes(
    ebull_test_conn: Conn, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = ebull_test_conn
    trade_id, _, broker, _ = _opened_trade(conn, monkeypatch)

    def down(_order: Any) -> Any:
        raise RuntimeError("what-if lane exhausted")

    broker.get_what_if_costs.side_effect = down
    _close(conn, broker, trade_id, quote=_QUOTE, expected=["arrival", "what_if", "close"])
    assert conn.execute("SELECT outcome, ticket_amount, raw_payload FROM strategy_order_close_what_ifs").fetchone() == (
        "failed",
        Decimal("104.90"),
        None,
    )


def test_without_a_live_bid_the_what_if_is_not_requested_and_the_close_still_goes(
    ebull_test_conn: Conn, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = ebull_test_conn
    trade_id, _, broker, _ = _opened_trade(conn, monkeypatch)
    broker.get_what_if_costs.reset_mock()
    _close(conn, broker, trade_id, quote=None, expected=["arrival", "close"])
    broker.get_what_if_costs.assert_not_called()
    assert conn.execute("SELECT outcome, ticket_amount FROM strategy_order_close_what_ifs").fetchone() == (
        "unpriced",
        None,
    )
    assert conn.execute("SELECT outcome FROM strategy_order_arrival_quotes").fetchone() == ("not_quoted",)


def test_a_hook_that_raises_cannot_stop_the_close(ebull_test_conn: Conn, monkeypatch: pytest.MonkeyPatch) -> None:
    conn = ebull_test_conn
    trade_id, _, broker, _ = _opened_trade(conn, monkeypatch)

    def hook(_order_id: int, _position: BrokerPosition) -> None:
        conn.execute("SELECT 1")  # leaves a transaction open, then raises
        raise RuntimeError("evidence broke")

    result = manage_owned_position(
        conn,
        broker=broker,
        strategy_trade_id=trade_id,
        broker_position_id=_POSITION_ID,
        close_reason="strategy_exit",
        now=_NOW,
        exit_evidence=hook,
    )
    assert result.state == "submitted"
    broker.close_demo_strategy_position.assert_called_once()
    status = conn.execute(
        "SELECT status FROM strategy_position_operations WHERE position_operation_id = %s",
        (result.position_operation_id,),
    ).fetchone()
    assert status == ("submitted",)


def test_a_broker_stop_close_is_costed_against_its_level_with_the_gap_flagged(
    ebull_test_conn: Conn, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = ebull_test_conn
    _opened_trade(conn, monkeypatch)
    session = (_NOW + timedelta(seconds=5)).astimezone(ZoneInfo("America/New_York")).date()
    conn.execute(
        "INSERT INTO price_daily (instrument_id, price_date, open, high, low, close) VALUES (%s, %s, 93, 96, 92, 94)",
        (_INSTRUMENT_ID, session),
    )
    _close_event(conn, price="94", stop="95", take="130")
    (e,) = load_exit_tca(conn, since=_NOW.date())
    assert (e.kind, e.order_id, e.level, e.gapped, e.gap_unmeasured) == ("stop_loss", None, Decimal("95"), True, None)
    assert e.level_cost == 1 - Decimal("94") / Decimal("95")
