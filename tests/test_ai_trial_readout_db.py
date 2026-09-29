"""#3471 §9 readout loader against real Postgres: a trial leg reached through its trade link, its
entry executions, its ownership and the broker's close row."""

from __future__ import annotations

from datetime import timedelta
from decimal import Decimal
from typing import Any

import psycopg
import pytest
from psycopg.types.json import Jsonb

from app.services import ai_trial_readout
from app.services.ai_trial_pair_lifecycle import previous_session, record_pair_lifecycle
from app.services.ai_trial_readout import compute_readout, load_exposure_facts, load_pairs
from app.services.market_regime import Regime
from app.services.market_regime_provider import MarketRegimeProvider
from app.services.risk_metrics import RISK_METRICS_VERSION
from tests.test_ai_trial_deadline_db import _POSITION_ID, _opened_arm_leg
from tests.test_ai_trial_intent_db import ARM_INSTRUMENT, NOW

Conn = psycopg.Connection[Any]


def test_a_broker_closed_arm_leg_is_valued_from_its_close_row(
    ebull_test_conn: Conn, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = ebull_test_conn
    trade_id, _ = _opened_arm_leg(conn, monkeypatch)
    closed_at = NOW + timedelta(days=3)
    # The broker closed the position at its 116 target: +20 on 125 opened (1.25 units at 100), in
    # two slices. eToro books a partial slice under a NEW position id sharing the entry orderId.
    for position_id, order_id in ((_POSITION_ID, None), (_POSITION_ID + 1, 3471001)):
        conn.execute(
            """
            INSERT INTO trade_events (position_id, etoro_instrument_id, instrument_id, event_kind, side, units,
                                      price, executed_at, fees_usd, realized_pnl_usd, investment_usd, order_id,
                                      source, raw_payload, recorded_at)
            VALUES (%s, %s, %s, 'close', 'sell', 0.625, 116, %s, 0, 10, 62.5, %s, 'etoro_history', %s, %s)
            """,
            (
                position_id,
                ARM_INSTRUMENT,
                ARM_INSTRUMENT,
                closed_at + timedelta(seconds=position_id - _POSITION_ID),
                order_id,
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
    # Both slices counted: the owned position's and the unowned partial-close sibling's.
    # The exited arm is no longer live, its exit session anchors the pair, and §7's pool is reported.
    assert (pair.live_legs, pair.resolved_session) == (0, closed_at.date())
    # The SPY reference and turnover span: fill session to the broker's own close session.
    assert (pair.arm.entry_session, pair.arm.exit_session) == (NOW.date(), closed_at.date())
    assert pair.pool_size is not None and pair.pool_size >= 1
    # Filled at 100 against the preflight's stored 100 ask (sql/438); the control never filled.
    assert pair.fill_gaps == {"arm": 0.0}

    # The SPY reference is a due-readout input only: the 5-minute halt cycle runs this readout and
    # fails closed on any error, so a benchmark read must not reach it before the cohort is due.
    def _boom(*_: object) -> None:
        raise AssertionError("SPY loaded before the readout is due")

    monkeypatch.setattr(ai_trial_readout, "load_spy_reference", _boom)
    version = conn.execute("SELECT strategy_version FROM ai_trial_declarations").fetchone()
    assert version is not None
    readout = compute_readout(conn, strategy_version=str(version[0]), as_of=closed_at)
    conn.commit()
    assert (readout.cohort.status, readout.spy_capital) == ("not_due", None)


def test_exposure_facts_are_read_as_known_at_the_decision(
    ebull_test_conn: Conn, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = ebull_test_conn
    _opened_arm_leg(conn, monkeypatch)
    row = conn.execute(
        "SELECT p.declaration_id, p.pair_seq, r.session_date, d.atr14_pct, p.control_instrument_id "
        "FROM ai_trial_pairs p JOIN ai_trial_decisions d ON d.decision_id = p.arm_decision_id "
        "JOIN ai_trial_runs r ON r.run_id = d.run_id"
    ).fetchone()
    assert row is not None
    declaration_id, pair_seq, session, arm_atr, control_id = row
    # The house 1y beta: the observation dated before the entry session counts, the one ON it does not.
    for as_of_date, beta in ((session - timedelta(days=3), "1.25"), (session, "9.0")):
        conn.execute(
            "INSERT INTO instrument_risk_metrics_observations (instrument_id, as_of_date, metric_version, "
            "window_key, beta) VALUES (%s, %s, %s, '1y', %s)",
            (ARM_INSTRUMENT, as_of_date, RISK_METRICS_VERSION, Decimal(beta)),
        )
    conn.commit()

    facts = load_exposure_facts(conn, int(declaration_id))
    conn.commit()
    arm = facts[int(pair_seq)]["arm"]
    assert (arm.beta_1y, arm.atr14_pct) == (Decimal("1.25"), arm_atr)
    control = facts[int(pair_seq)]["control"]
    assert control.beta_1y is None
    assert set(facts[int(pair_seq)]) == {"arm", "control"} and control_id is not None
