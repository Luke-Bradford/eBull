"""#3471 slice 2c-iv-c — the execute job's leg selection and the age backstop, on real Postgres.

The legs come from a real publish (``test_ai_trial_run_db``'s stubbed steps 0-2 and fake model).
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

import psycopg
import pytest

from app.services.ai_trial_deadline import TRIAL_MAX_POSITION_AGE_SECONDS
from app.services.ai_trial_jobs import TrialJobError, configure_trial_position_managers, due_trial_legs
from tests.test_ai_trial_run_db import _decisions, _invoke, _run, _seed, stubbed  # noqa: F401 (fixture)

Conn = psycopg.Connection[Any]


def _deployment(conn: Conn, strategy_id: str) -> int:
    row = conn.execute(
        """
        INSERT INTO strategy_deployments (
            strategy_id, strategy_version, mode, capital_limit, currency, enabled, updated_by, reason
        ) VALUES (%s, 'v1', 'paper', 1000, 'USD', TRUE, 'test', 'test')
        RETURNING deployment_id
        """,
        (strategy_id,),
    ).fetchone()
    assert row is not None
    return int(row[0])


def test_due_legs_are_published_undecided_and_not_in_the_future(
    ebull_test_conn: Conn,
    stubbed: dict[str, Any],  # noqa: F811
) -> None:
    conn = ebull_test_conn
    _seed(conn)
    outcome = _run(conn, _invoke(_decisions()))
    assert outcome.status == "decided" and outcome.session_date is not None
    session = outcome.session_date
    links = dict(conn.execute("SELECT leg, signal_id FROM ai_trial_leg_links").fetchall())

    # pair_seq 0 is even: the arm submits first (§7).
    assert conn.execute("SELECT pair_seq FROM ai_trial_pairs").fetchone() == (0,)
    assert due_trial_legs(conn, today=session) == [links["arm"], links["control"]]
    assert due_trial_legs(conn, today=session + timedelta(days=30)) == [links["arm"], links["control"]]
    # An odd pair_seq submits the control first. (Moved in-transaction only; rolled back below.)
    conn.execute("ALTER TABLE ai_trial_pairs DISABLE TRIGGER USER")
    conn.execute("UPDATE ai_trial_pairs SET pair_seq = 1")
    assert due_trial_legs(conn, today=session) == [links["control"], links["arm"]]
    conn.rollback()
    assert due_trial_legs(conn, today=session - timedelta(days=1)) == []

    # A funding decision, whatever its verdict, takes the leg out: the executor owns it now.
    conn.execute(
        "INSERT INTO strategy_funding_decisions (signal_id, verdict, reason_code) "
        "VALUES (%s, 'rejected', 'decision_expired')",
        (links["arm"],),
    )
    assert due_trial_legs(conn, today=session) == [links["control"]]

    # The age backstop needs both legs' paper deployments.
    _deployment(conn, "ai-discretionary-v1")
    with pytest.raises(TrialJobError, match="both trial legs"):
        configure_trial_position_managers(conn, updated_by="test")
    _deployment(conn, "ai-discretionary-v1-control")
    assert configure_trial_position_managers(conn, updated_by="test") == {
        "ai-discretionary-v1": 1,
        "ai-discretionary-v1-control": 1,
    }
    ages = conn.execute(
        """
        SELECT d.strategy_id, p.max_position_age_seconds, p.ratchet_variant_id
        FROM strategy_position_manager_policies p JOIN strategy_deployments d USING (deployment_id)
        ORDER BY d.strategy_id
        """
    ).fetchall()
    assert ages == [
        ("ai-discretionary-v1", TRIAL_MAX_POSITION_AGE_SECONDS, None),
        ("ai-discretionary-v1-control", TRIAL_MAX_POSITION_AGE_SECONDS, None),
    ]
