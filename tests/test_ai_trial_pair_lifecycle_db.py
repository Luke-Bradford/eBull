"""#3471 slice 2c-iii-b — the pair lifecycle writer against real Postgres: events derived from the
executor's and reconciliation's own writes, appended once, with ``broken`` decided per pair."""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any
from uuid import uuid4

import psycopg
import pytest

from app.providers.broker import BrokerOrderSubmissionUncertain
from app.services.ai_trial_deadline import fill_session
from app.services.ai_trial_executor import execute_trial_signal
from app.services.ai_trial_pair_lifecycle import (
    LABEL_CLASSIFIER_VERSION,
    pair_unit_state,
    previous_session,
    record_pair_lifecycle,
)
from app.services.market_regime import Regime
from app.services.market_regime_provider import MarketRegimeProvider
from tests.test_ai_trial_deadline_db import _opened_arm_leg
from tests.test_ai_trial_executor_db import _broker, _control_instrument
from tests.test_ai_trial_intent_db import NOW, _published_pair

Conn = psycopg.Connection[Any]


def _events(conn: Conn) -> list[tuple[str | None, str, list[str] | None]]:
    rows = conn.execute("SELECT leg, event, reasons FROM ai_trial_pair_events ORDER BY event_id").fetchall()
    conn.commit()
    return [(row[0], row[1], row[2]) for row in rows]


def _control_signal(conn: Conn, monkeypatch: pytest.MonkeyPatch) -> int:
    # The arm fixture pins the request id; the control's order needs its own.
    monkeypatch.setattr("app.services.strategy_order_reconciliation.uuid4", uuid4)
    row = conn.execute("SELECT signal_id FROM ai_trial_leg_links WHERE leg = 'control'").fetchone()
    conn.commit()
    assert row is not None
    return int(row[0])


def test_a_filled_leg_and_an_uncertain_leg_block_the_pair(
    ebull_test_conn: Conn, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = ebull_test_conn
    _opened_arm_leg(conn, monkeypatch)
    control_signal = _control_signal(conn, monkeypatch)
    broker = _broker(_control_instrument(conn, control_signal))
    broker.place_demo_strategy_order.side_effect = BrokerOrderSubmissionUncertain("timeout")
    assert execute_trial_signal(conn, broker=broker, signal_id=control_signal, now=NOW).verdict == (
        "submission_uncertain"
    )

    assert record_pair_lifecycle(conn, now=NOW) == 4
    events = _events(conn)
    assert events == [
        ("arm", "submitted", None),
        ("arm", "filled", None),
        ("control", "submitted", None),
        ("control", "uncertain", None),
    ]
    # O11: excluded until reconciliation resolves the control leg; no `broken` while undetermined.
    assert pair_unit_state([(leg, event) for leg, event, _ in events]) == "blocked"
    # Idempotent: a second pass owes nothing.
    assert record_pair_lifecycle(conn, now=NOW) == 0

    # Still unresolved ten sessions after the target session → the pair breaks, once.
    unresolved_at = NOW.replace(day=19)
    assert record_pair_lifecycle(conn, now=unresolved_at - timedelta(seconds=1)) == 0
    assert record_pair_lifecycle(conn, now=unresolved_at) == 1
    assert _events(conn)[-1] == (None, "broken", ["unresolved"])
    assert record_pair_lifecycle(conn, now=unresolved_at) == 0


def test_a_refused_leg_breaks_the_pair_with_its_refusal_code(
    ebull_test_conn: Conn, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = ebull_test_conn
    _opened_arm_leg(conn, monkeypatch)
    control_signal = _control_signal(conn, monkeypatch)
    broker = _broker(_control_instrument(conn, control_signal))
    # The next day: the decision's target session has passed.
    refused = execute_trial_signal(conn, broker=broker, signal_id=control_signal, now=NOW + timedelta(days=1))
    assert refused.verdict == "rejected"

    assert record_pair_lifecycle(conn, now=NOW + timedelta(days=1)) == 3
    assert _events(conn) == [
        ("arm", "submitted", None),
        ("arm", "filled", None),
        (None, "broken", [refused.reason_code]),
    ]


def test_a_pair_with_every_leg_terminal_and_its_verdict_settled_is_no_longer_read(
    ebull_test_conn: Conn, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = ebull_test_conn
    _, signals = _published_pair(conn)
    late = NOW + timedelta(days=1)
    for leg in ("arm", "control"):
        broker = _broker(_control_instrument(conn, signals[leg]))
        assert execute_trial_signal(conn, broker=broker, signal_id=signals[leg], now=late).verdict == "rejected"

    assert record_pair_lifecycle(conn, now=late) == 1
    assert [event for _, event, _ in _events(conn)] == ["broken"]

    def _read(*_: object) -> int:
        raise AssertionError("a finished pair was read again")

    monkeypatch.setattr("app.services.ai_trial_pair_lifecycle._record_pair", _read)
    assert record_pair_lifecycle(conn, now=late) == 0


def _arm_fill_session(conn: Conn) -> date:
    row = conn.execute(
        """
        SELECT min(e.execution_time) FROM strategy_order_position_executions e
        JOIN strategy_trade_orders sto ON sto.order_id = e.order_id AND sto.purpose = 'entry'
        JOIN strategy_trades t ON t.strategy_trade_id = sto.strategy_trade_id
        JOIN strategy_funding_decisions fd ON fd.funding_decision_id = t.funding_decision_id
        JOIN ai_trial_leg_links l ON l.signal_id = fd.signal_id AND l.leg = 'arm'
        """
    ).fetchone()
    conn.commit()
    assert row is not None and row[0] is not None
    return fill_session(row[0])


def test_the_arm_fill_writes_one_regime_label_once_its_benchmark_bar_exists(
    ebull_test_conn: Conn, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = ebull_test_conn
    _opened_arm_leg(conn, monkeypatch)
    entry = _arm_fill_session(conn)
    loads: list[int] = []

    def no_bar_yet(_: Conn) -> MarketRegimeProvider:
        loads.append(1)
        return MarketRegimeProvider(regime_by_date={previous_session(previous_session(entry)): Regime.BULL_QUIET})

    assert record_pair_lifecycle(conn, now=NOW, regime_loader=no_bar_yet) == 2
    assert conn.execute("SELECT count(*) FROM ai_trial_pair_labels").fetchone() == (0,)
    conn.commit()

    def with_bar(_: Conn) -> MarketRegimeProvider:
        loads.append(2)
        return MarketRegimeProvider(regime_by_date={previous_session(entry): Regime.BEAR_QUIET})

    # The deferred label is retried on the next pass, and written once.
    assert record_pair_lifecycle(conn, now=NOW, regime_loader=with_bar) == 0
    assert record_pair_lifecycle(conn, now=NOW, regime_loader=with_bar) == 0
    rows = conn.execute("SELECT entry_session, regime_label, classifier_version FROM ai_trial_pair_labels").fetchall()
    conn.commit()
    assert rows == [(entry, "bear_quiet", LABEL_CLASSIFIER_VERSION)]
    # One benchmark load per pass that owed a label; none once the label exists.
    assert loads == [1, 2]
