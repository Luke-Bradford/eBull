"""Regression: execute_order must re-check safety_layers_enabled at
execute time, not just at guard-evaluation time."""

from __future__ import annotations

from typing import Any

import psycopg
import pytest

from app.services.layer_enabled import set_layer_enabled
from app.services.order_client import SafetyLayerDisabledError, execute_order
from tests.fixtures.ebull_test_db import test_database_url as _test_database_url


def _seed_approved_buy(conn: psycopg.Connection[Any]) -> tuple[int, int]:
    """Insert a minimal approved BUY recommendation + decision_audit row.
    Returns (recommendation_id, decision_id).

    Uses real decision_audit column names:
      decision_time, stage, pass_fail, explanation, evidence_json, recommendation_id
    """
    cur = conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable)
        VALUES (999002, 'SAFE-EXEC', 'Safety exec test', TRUE)
        ON CONFLICT (instrument_id) DO UPDATE SET symbol = EXCLUDED.symbol
        RETURNING instrument_id
        """
    )
    inst_row = cur.fetchone()
    assert inst_row is not None

    cur = conn.execute(
        """
        INSERT INTO trade_recommendations
            (instrument_id, action, status, model_version, rationale, created_at, suggested_size_pct)
        VALUES
            (999002, 'BUY', 'approved', 'v1.1-balanced', 'test', now(), 0.01)
        RETURNING recommendation_id
        """
    )
    rec_row = cur.fetchone()
    assert rec_row is not None
    rec_id = int(rec_row[0])

    cur = conn.execute(
        """
        INSERT INTO decision_audit
            (decision_time, instrument_id, stage, pass_fail, explanation,
             evidence_json, recommendation_id)
        VALUES
            (now(), 999002, 'execution_guard', 'PASS', 'test',
             '[]'::jsonb, %s)
        RETURNING decision_id
        """,
        (rec_id,),
    )
    dec_row = cur.fetchone()
    assert dec_row is not None
    conn.commit()
    return rec_id, int(dec_row[0])


def _enable_all(conn: psycopg.Connection[Any]) -> None:
    for name in ("fx_rates", "portfolio_sync"):
        set_layer_enabled(conn, name, enabled=True)
    conn.commit()


@pytest.mark.integration
def test_execute_order_aborts_buy_when_fx_rates_disabled() -> None:
    with psycopg.connect(_test_database_url()) as conn:
        _enable_all(conn)
        rec_id, decision_id = _seed_approved_buy(conn)
        set_layer_enabled(conn, "fx_rates", enabled=False)
        conn.commit()
        try:
            with pytest.raises(SafetyLayerDisabledError) as exc:
                execute_order(conn, rec_id, decision_id, broker=None)
        finally:
            _enable_all(conn)
    assert "fx_rates" in str(exc.value).lower()


@pytest.mark.integration
def test_execute_order_aborts_buy_when_portfolio_sync_disabled() -> None:
    with psycopg.connect(_test_database_url()) as conn:
        _enable_all(conn)
        rec_id, decision_id = _seed_approved_buy(conn)
        set_layer_enabled(conn, "portfolio_sync", enabled=False)
        conn.commit()
        try:
            with pytest.raises(SafetyLayerDisabledError):
                execute_order(conn, rec_id, decision_id, broker=None)
        finally:
            _enable_all(conn)


@pytest.mark.integration
def test_execute_order_exit_allowed_even_when_safety_layers_disabled() -> None:
    """EXIT is the de-risk path — must pass through regardless."""
    with psycopg.connect(_test_database_url()) as conn:
        _enable_all(conn)
        conn.execute(
            """
            INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable)
            VALUES (999003, 'SAFE-EXIT', 'Safety exit test', TRUE)
            ON CONFLICT (instrument_id) DO UPDATE SET symbol = EXCLUDED.symbol
            """
        )
        # Open position so _load_position_units returns a non-null result.
        conn.execute(
            """
            INSERT INTO positions
                (instrument_id, open_date, avg_cost, current_units, cost_basis, source, updated_at)
            VALUES (999003, now()::date, 100, 1, 100, 'ebull', now())
            ON CONFLICT (instrument_id) DO NOTHING
            """
        )
        cur = conn.execute(
            """
            INSERT INTO trade_recommendations
                (instrument_id, action, status, model_version, rationale, created_at)
            VALUES
                (999003, 'EXIT', 'approved', 'v1.1-balanced', 'test', now())
            RETURNING recommendation_id
            """
        )
        rec_row = cur.fetchone()
        assert rec_row is not None
        rec_id = int(rec_row[0])

        cur = conn.execute(
            """
            INSERT INTO decision_audit
                (decision_time, instrument_id, stage, pass_fail, explanation,
                 evidence_json, recommendation_id)
            VALUES
                (now(), 999003, 'execution_guard', 'PASS', 'test',
                 '[]'::jsonb, %s)
            RETURNING decision_id
            """,
            (rec_id,),
        )
        dec_row = cur.fetchone()
        assert dec_row is not None
        decision_id = int(dec_row[0])
        conn.commit()

        # Disable both safety layers. EXIT must still go through —
        # SafetyLayerDisabledError should NOT be raised.
        set_layer_enabled(conn, "fx_rates", enabled=False)
        set_layer_enabled(conn, "portfolio_sync", enabled=False)
        conn.commit()
        try:
            # Must not raise SafetyLayerDisabledError. May raise other
            # errors from the demo-fill path; those are acceptable for
            # this test (we only assert the safety check passed).
            try:
                execute_order(conn, rec_id, decision_id, broker=None)
            except SafetyLayerDisabledError:
                pytest.fail("EXIT must not be blocked by safety_layers_enabled")
            except Exception:
                pass  # any other path error is orthogonal
        finally:
            _enable_all(conn)
