"""#3612 slice 2 — the paper executor's marked daily-loss check (spec Design 7 and 8).

The figure itself is tested in ``test_3612_marked_daily_loss`` (pure) and ``_db`` (its read); here it
is replaced by a stub so the executor's threshold, refusal routing and ordering are tested exactly.
"""

from __future__ import annotations

from datetime import timedelta
from decimal import Decimal
from typing import Any

import psycopg
import pytest

from app.services import strategy_paper_executor
from app.services.strategy_engine_capital import EngineCapitalObservationError
from tests.test_strategy_paper_executor import (
    _NOW,
    _broker,
    _existing_allocated_trade,
    _seed,
    execute_fired_paper_signal,
)

pytestmark = [pytest.mark.integration, pytest.mark.usefixtures("registered_strategy_test_candidates")]


def _limit(conn: psycopg.Connection[Any]) -> Decimal:
    row = conn.execute(
        "SELECT capital_limit, max_daily_loss_pct FROM strategy_paper_pool_events "
        "ORDER BY strategy_paper_pool_event_id DESC LIMIT 1"
    ).fetchone()
    assert row is not None
    return Decimal(str(row[0])) * Decimal(str(row[1])) / Decimal("100")


def _marked(monkeypatch: pytest.MonkeyPatch, outcome: Decimal | str) -> None:
    def observe(*_a: object, **_k: object) -> Decimal:
        if isinstance(outcome, str):
            raise EngineCapitalObservationError("stub refusal", outcome)  # type: ignore[arg-type]
        return outcome

    monkeypatch.setattr(strategy_paper_executor, "observe_daily_marked_pnl", observe)


def _no_realised(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "app.services.strategy_paper_executor.load_paper_realised_pnl",
        lambda _conn: {("S-ALLOC", "v1"): Decimal("0")},
    )


@pytest.mark.parametrize(("offset", "refused"), [("0", True), ("-0.01", True), ("0.01", False)])
def test_the_marked_limit_refuses_at_and_beyond_it(
    ebull_test_conn: psycopg.Connection[Any], monkeypatch: pytest.MonkeyPatch, offset: str, refused: bool
) -> None:
    conn = ebull_test_conn
    signal_id = _seed(conn)
    _no_realised(monkeypatch)
    _marked(monkeypatch, -_limit(conn) + Decimal(offset))
    conn.commit()
    broker = _broker()

    result = execute_fired_paper_signal(conn, broker=broker, signal_id=signal_id, now=_NOW)

    if refused:
        assert result.reason_code == "portfolio_daily_marked_loss_limit"
        broker.place_demo_strategy_order.assert_not_called()
    else:
        assert result.verdict == "submitted"


@pytest.mark.parametrize(
    "code",
    [
        "engine_pot_daily_anchor_missing",
        "engine_pot_daily_anchor_incomplete",
        "engine_pot_daily_close_in_window",
        "engine_pot_daily_event_unwitnessed",
        "engine_pot_daily_accounting_unverified",
        "engine_pot_daily_mark_stale",
    ],
)
def test_each_marked_refusal_reaches_the_recorded_reason_after_the_drawdown_advance(
    ebull_test_conn: psycopg.Connection[Any], monkeypatch: pytest.MonkeyPatch, code: str
) -> None:
    conn = ebull_test_conn
    signal_id = _seed(conn)
    _no_realised(monkeypatch)
    _marked(monkeypatch, code)
    broker = _broker()

    result = execute_fired_paper_signal(conn, broker=broker, signal_id=signal_id, now=_NOW)

    assert (result.verdict, result.reason_code) == ("rejected", code)
    broker.place_demo_strategy_order.assert_not_called()
    recorded = conn.execute(
        "SELECT reason_code FROM strategy_funding_decisions WHERE signal_id=%s", (signal_id,)
    ).fetchone()
    assert recorded == (code,)
    state = conn.execute("SELECT observed_at FROM strategy_engine_pot_risk_state").fetchone()
    assert state == (_NOW,)


def test_the_realised_check_still_refuses_when_the_marked_figure_is_flat(
    ebull_test_conn: psycopg.Connection[Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = ebull_test_conn
    signal_id = _seed(conn)
    _marked(monkeypatch, Decimal("0"))
    _released_close(conn, position_id=7443001, realised="-1000000", at=_NOW)
    _no_realised(monkeypatch)

    result = execute_fired_paper_signal(conn, broker=_broker(), signal_id=signal_id, now=_NOW)

    assert result.reason_code == "portfolio_daily_loss_limit"


def _released_close(conn: psycopg.Connection[Any], *, position_id: int, realised: str, at: Any) -> None:
    trade_id = _existing_allocated_trade(conn, index=position_id % 1000, amount=Decimal("100"), status="closed")
    conn.execute(
        """
        INSERT INTO strategy_position_ownership (
            strategy_trade_id,broker_position_id,status,claimed_at,released_at,release_reason
        ) VALUES (%s,%s,'released',%s,%s,'closed')
        """,
        (trade_id, position_id, at - timedelta(hours=1), at),
    )
    conn.execute(
        """
        INSERT INTO trade_events (
            position_id,etoro_instrument_id,instrument_id,event_kind,side,units,
            price,executed_at,fees_usd,realized_pnl_usd,investment_usd,source,raw_payload
        ) VALUES (%s,2449001,2449001,'close','sell',1,100,%s,0,%s::numeric,100,'etoro_history','{}')
        """,
        (position_id, at, realised),
    )
    conn.commit()


def test_a_non_finite_realised_day_refuses_instead_of_comparing(
    ebull_test_conn: psycopg.Connection[Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = ebull_test_conn
    signal_id = _seed(conn)
    _marked(monkeypatch, Decimal("0"))
    _released_close(conn, position_id=7443001, realised="NaN", at=_NOW)
    _no_realised(monkeypatch)

    result = execute_fired_paper_signal(conn, broker=_broker(), signal_id=signal_id, now=_NOW)

    assert result.reason_code == "engine_capital_population_incomplete"


def test_a_non_finite_realised_position_before_today_refuses_in_the_pot_nav(
    ebull_test_conn: psycopg.Connection[Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = ebull_test_conn
    signal_id = _seed(conn)
    _marked(monkeypatch, Decimal("0"))
    # Yesterday, outside the realised check's day, so only `_price_book` reads it.
    _released_close(conn, position_id=7443001, realised="NaN", at=_NOW - timedelta(days=1))
    _no_realised(monkeypatch)

    result = execute_fired_paper_signal(conn, broker=_broker(), signal_id=signal_id, now=_NOW)

    assert result.reason_code == "engine_capital_population_incomplete"
