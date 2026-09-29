"""#3471 §9 readout loader against real Postgres: a trial leg reached through its trade link, its
entry executions, its ownership and the broker's close row."""

from __future__ import annotations

from datetime import timedelta
from decimal import Decimal
from typing import Any

import psycopg
import pytest
from psycopg.types.json import Jsonb

from app.services.ai_trial_pair_lifecycle import previous_session, record_pair_lifecycle
from app.services.ai_trial_readout import load_pairs
from app.services.market_regime import Regime
from app.services.market_regime_provider import MarketRegimeProvider
from tests.test_ai_trial_deadline_db import _POSITION_ID, _opened_arm_leg
from tests.test_ai_trial_intent_db import ARM_INSTRUMENT, NOW

Conn = psycopg.Connection[Any]


def test_a_broker_closed_arm_leg_is_valued_from_its_close_row(
    ebull_test_conn: Conn, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = ebull_test_conn
    trade_id, _ = _opened_arm_leg(conn, monkeypatch)
    closed_at = NOW + timedelta(days=3)
    # The broker closed the position at its 116 target: +20 on 125 opened (1.25 units at 100).
    conn.execute(
        """
        INSERT INTO trade_events (position_id, etoro_instrument_id, instrument_id, event_kind, side, units,
                                  price, executed_at, fees_usd, realized_pnl_usd, investment_usd, source,
                                  raw_payload, recorded_at)
        VALUES (%s, %s, %s, 'close', 'sell', 1.25, 116, %s, 0, 20, 125, 'etoro_history', %s, %s)
        """,
        (
            _POSITION_ID,
            ARM_INSTRUMENT,
            ARM_INSTRUMENT,
            closed_at,
            Jsonb({"stopLossRate": 92, "takeProfitRate": 116}),
            closed_at + timedelta(minutes=5),
        ),
    )
    conn.execute(
        "UPDATE strategy_position_ownership SET status = 'released', released_at = %s, "
        "release_reason = 'broker_whole_close' WHERE strategy_trade_id = %s",
        (closed_at, trade_id),
    )
    conn.execute("UPDATE strategy_trades SET status = 'closed' WHERE strategy_trade_id = %s", (trade_id,))
    conn.commit()

    def regime(_: Conn) -> MarketRegimeProvider:
        return MarketRegimeProvider(regime_by_date={previous_session(NOW.date()): Regime.BULL_QUIET})

    record_pair_lifecycle(conn, now=closed_at, regime_loader=regime)
    row = conn.execute("SELECT declaration_id FROM ai_trial_pairs").fetchone()
    assert row is not None
    pairs, first_fill = load_pairs(conn, int(row[0]))
    conn.commit()

    assert first_fill == NOW.date()
    (pair,) = pairs
    # The control never ran, so the pair is open and not a unit; the exited arm is still valued.
    assert (pair.state, pair.regime_label, pair.control, pair.valued) == ("open", "bull_quiet", None, False)
    assert pair.arm is not None
    assert pair.arm.net_pct == pytest.approx(16.0)
    assert (pair.arm.open_amount, pair.arm.exit_label, pair.arm.unvalued_reason) == (125.0, "target", None)
    assert pair.arm.pnl_usd == pytest.approx(float(Decimal(20)))
