"""Engine-pot NAV population and drawdown state against a real database (#3541 slice 1).

One test per new SQL mechanism: the exact-owned population read, and the state advance.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import timedelta
from decimal import Decimal
from typing import Any

import psycopg
import pytest

from app.providers.broker import BrokerAccountRiskSnapshot, BrokerDirectPositionInvestment
from app.services.engine_pot_risk import advance_pot_drawdown, observe_pot_nav, preview_pot_drawdown
from app.services.strategy_engine_capital import EngineCapitalObservationError
from tests.test_strategy_paper_executor import _NOW
from tests.test_strategy_position_manager import _MANUAL_POSITION_ID, _POSITION_ID, _opened_trade

pytestmark = [pytest.mark.integration, pytest.mark.usefixtures("registered_strategy_test_candidates")]


def _held(position_id: int, pnl: str, *, instrument_id: int = 2449001) -> BrokerDirectPositionInvestment:
    return BrokerDirectPositionInvestment(
        position_id=position_id,
        instrument_id=instrument_id,
        is_buy=True,
        units=Decimal("1"),
        amount=Decimal("100"),
        unrealized_pnl=Decimal(pnl),
        market_value=Decimal("100") + Decimal(pnl),
        is_partially_altered=False,
        close_rate=Decimal("100") + Decimal(pnl),
        close_conversion_rate=Decimal("1"),
        asset_currency_id=1,
    )


def _snapshot(*positions: BrokerDirectPositionInvestment, minutes: int = 0) -> BrokerAccountRiskSnapshot:
    return BrokerAccountRiskSnapshot(
        available_cash=Decimal("1000"),
        total_invested=Decimal("0"),
        unrealized_pnl=Decimal("0"),
        equity=Decimal("1000"),
        instrument_investments=(),
        observed_at=_NOW + timedelta(minutes=minutes),
        raw_payload={},
        direct_positions=positions,
        account_currency_id=1,
    )


def _close(conn: psycopg.Connection[Any], *, pnl: str, minutes: int) -> None:
    conn.execute(
        """
        INSERT INTO trade_events (
            position_id,etoro_instrument_id,instrument_id,event_kind,side,executed_at,units,price,
            realized_pnl_usd,source,raw_payload
        ) VALUES (%s,2449001,2449001,'close','sell',%s,1,100,%s,'etoro_sync','{}')
        """,
        (_POSITION_ID, _NOW + timedelta(minutes=minutes), Decimal(pnl)),
    )


def _refusal(conn: psycopg.Connection[Any], snapshot: BrokerAccountRiskSnapshot) -> str:
    with pytest.raises(EngineCapitalObservationError) as exc:
        observe_pot_nav(conn, snapshot)
    return exc.value.reason_code


def test_population_prices_only_the_exact_owned_book(
    ebull_test_conn: psycopg.Connection[Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = ebull_test_conn
    trade_id, _deployment, _broker, _manual = _opened_trade(conn, monkeypatch)
    # The fixture clock is in the past and `claimed_at` defaults to now(); membership is
    # point-in-time, so place the claim before the snapshots.
    conn.execute("UPDATE strategy_position_ownership SET claimed_at=%s", (_NOW - timedelta(days=1),))
    assert observe_pot_nav(conn, _snapshot(minutes=-24 * 60 - 1)).unrealised == Decimal("0")
    principal = conn.execute(
        "SELECT capital_limit FROM strategy_paper_pool_events ORDER BY strategy_paper_pool_event_id DESC LIMIT 1"
    ).fetchone()
    assert principal is not None

    # The operator's manual position moves by 500; only the owned one's 7 counts.
    pot = observe_pot_nav(conn, _snapshot(_held(_POSITION_ID, "7"), _held(_MANUAL_POSITION_ID, "-500")))
    assert (pot.principal, pot.realised, pot.unrealised) == (principal[0], Decimal("0"), Decimal("7"))

    # A partial close inside the snapshot is realised; one after it is still in the unrealised.
    _close(conn, pnl="3", minutes=-1)
    _close(conn, pnl="11", minutes=5)
    assert observe_pot_nav(conn, _snapshot(_held(_POSITION_ID, "7"))).realised == Decimal("3")

    assert _refusal(conn, _snapshot()) == "engine_capital_ownership_unwitnessed"
    assert _refusal(conn, _snapshot(_held(_POSITION_ID, "7", instrument_id=1))) == "engine_capital_ownership_mismatched"

    # Released at +6: before that the position is still held at its mark; after it, both
    # closes are realised and no mark is needed.
    conn.execute(
        "UPDATE strategy_position_ownership SET status='released',released_at=%s,release_reason='test' "
        "WHERE strategy_trade_id=%s",
        (_NOW + timedelta(minutes=6), trade_id),
    )
    assert observe_pot_nav(conn, _snapshot(_held(_POSITION_ID, "7"))).realised == Decimal("3")
    assert _refusal(conn, _snapshot()) == "engine_capital_ownership_unwitnessed"
    assert observe_pot_nav(conn, _snapshot(minutes=10)).realised == Decimal("14")
    conn.execute("DELETE FROM trade_events WHERE position_id=%s", (_POSITION_ID,))
    assert _refusal(conn, _snapshot(minutes=10)) == "engine_capital_population_incomplete"

    # A trade that reached the broker with no ownership row at all is unattributable P&L.
    conn.execute("DELETE FROM strategy_position_ownership WHERE strategy_trade_id=%s", (trade_id,))
    assert _refusal(conn, _snapshot()) == "engine_capital_population_incomplete"


def test_advance_bootstraps_links_and_refuses_stale_or_foreign_state(
    ebull_test_conn: psycopg.Connection[Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = ebull_test_conn
    _opened_trade(conn, monkeypatch)
    conn.execute("UPDATE strategy_position_ownership SET claimed_at=%s", (_NOW - timedelta(days=1),))
    base = observe_pot_nav(conn, _snapshot(_held(_POSITION_ID, "0")))
    assert advance_pot_drawdown(conn, base) == Decimal("0")

    # A loss of 10% of NAV: previewed without writing, then advanced.
    loss = replace(base, unrealised=-base.nav / 10, observed_at=base.observed_at + timedelta(minutes=1))
    assert preview_pot_drawdown(conn, loss) == Decimal("10")
    stored = conn.execute("SELECT last_drawdown_pct FROM strategy_engine_pot_risk_state").fetchone()
    assert stored == (Decimal("0"),)
    assert advance_pot_drawdown(conn, loss) == Decimal("10")

    assert advance_pot_drawdown(conn, base) == "engine_pot_risk_stale"
    conn.execute("UPDATE strategy_engine_pot_risk_state SET epoch_started_at=epoch_started_at - interval '1 day'")
    assert advance_pot_drawdown(conn, replace(loss, observed_at=loss.observed_at + timedelta(minutes=1))) == (
        "engine_pot_epoch_mismatch"
    )
