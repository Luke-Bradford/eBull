"""#3514 against real Postgres: the readiness read names a published session's decision, each leg's
execution, the open leg and the loss-halt distance from the stored rows."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any

import psycopg
import pytest

from app.services.ai_trial_halts import TRIAL_LOSS_LIMIT_USD
from app.services.ai_trial_status import load_trial_status
from tests.test_ai_trial_deadline_db import _opened_arm_leg
from tests.test_ai_trial_intent_db import NOW

Conn = psycopg.Connection[Any]

SESSION = date(2026, 10, 5)


def test_a_published_session_with_one_leg_submitted(ebull_test_conn: Conn, monkeypatch: pytest.MonkeyPatch) -> None:
    conn = ebull_test_conn
    trade_id, _ = _opened_arm_leg(conn, monkeypatch)

    status = load_trial_status(conn, now=NOW + timedelta(hours=1))
    conn.commit()
    assert (status.state, status.declaration_id is not None) == ("active", True)
    # Other sessions may be listed as `not_run`, depending on when the fixture's genesis event
    # was written (wall clock); this session's row is what is under test.
    session = {s.session_date: s for s in status.sessions}[SESSION]
    assert session.decision.label == "legs_published:2"
    # The arm was submitted; the control has no funding decision and no in-window fire has run.
    assert [o.label for o in session.execution] == ["submitted:1", "awaiting_execution:1"]

    (leg,) = status.open_legs
    assert (leg.leg, leg.strategy_trade_id, leg.trade_status) == ("arm", trade_id, "open")
    assert leg.entry_price == Decimal("100")
    # The fixture writes no broker_positions row: nothing observed, not "no levels".
    assert (leg.broker_observed, leg.broker_stop, leg.broker_target) == (False, None, None)
    assert leg.requested_stop is not None and leg.requested_target is not None
    # 1.25 units at 100, bid 99.
    assert leg.pnl_usd == Decimal("-1.25")
    arm, control = status.loss
    assert (arm.leg, arm.pnl_usd, arm.unmeasured, arm.limit_usd) == ("arm", Decimal("-1.25"), 0, TRIAL_LOSS_LIMIT_USD)
    assert (control.leg, control.pnl_usd) == ("control", Decimal("0"))


def test_a_leg_no_fire_reached_before_its_close_is_not_run(
    ebull_test_conn: Conn, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = ebull_test_conn
    _opened_arm_leg(conn, monkeypatch)

    # Tuesday 23:45 UTC: Monday's session closed with the control leg never executed.
    status = load_trial_status(conn, now=datetime(2026, 10, 6, 23, 45, tzinfo=UTC))
    conn.commit()
    by_date = {s.session_date: s for s in status.sessions}
    assert by_date[SESSION].decision.label == "legs_published:2"
    assert [o.label for o in by_date[SESSION].execution] == ["submitted:1", "not_run:1"]
    # Every other listed session had no run row at all.
    assert {s.decision.label for d, s in by_date.items() if d != SESSION} <= {"not_run"}
