"""Execution-guard safety_layers_enabled rule (A.5 chunk 2)."""

from typing import Any

import psycopg
import pytest

from app.services.execution_guard import evaluate_recommendation
from app.services.layer_enabled import set_layer_enabled
from tests.fixtures.ebull_test_db import test_database_url as _test_database_url


def _seed_minimal_recommendation(
    conn: psycopg.Connection[Any],
    *,
    action: str,
) -> int:
    """Insert a minimal recommendation row + dependencies the guard needs.

    Returns the new recommendation_id. instruments uses an explicit
    integer PK (not BIGSERIAL), so we pick a large test-only value that
    won't collide with prod data. rationale is NOT NULL in the schema.
    """
    cur = conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable)
        VALUES (999001, 'SAFE-TEST', 'Safety test instrument', TRUE)
        ON CONFLICT (instrument_id) DO UPDATE SET company_name = EXCLUDED.company_name
        RETURNING instrument_id
        """,
    )
    inst_row = cur.fetchone()
    assert inst_row is not None
    instrument_id = int(inst_row[0])

    cur = conn.execute(
        """
        INSERT INTO trade_recommendations
            (instrument_id, action, rationale, model_version, status, created_at)
        VALUES
            (%s, %s, 'safety-layer test fixture', 'v1.1-balanced', 'proposed', now())
        RETURNING recommendation_id
        """,
        (instrument_id, action),
    )
    rec_row = cur.fetchone()
    assert rec_row is not None
    conn.commit()
    return int(rec_row[0])


def _enable_safety_layers(conn: psycopg.Connection[Any]) -> None:
    for name in ("fx_rates", "portfolio_sync"):
        set_layer_enabled(conn, name, enabled=True)
    conn.commit()


@pytest.mark.integration
def test_buy_blocked_when_fx_rates_disabled() -> None:
    with psycopg.connect(_test_database_url()) as conn:
        rec_id = _seed_minimal_recommendation(conn, action="BUY")
        set_layer_enabled(conn, "fx_rates", enabled=False)
        conn.commit()
        try:
            result = evaluate_recommendation(conn, rec_id)
        finally:
            _enable_safety_layers(conn)
    assert "safety_layers_enabled" in result.failed_rules


@pytest.mark.integration
def test_add_blocked_when_portfolio_sync_disabled() -> None:
    with psycopg.connect(_test_database_url()) as conn:
        rec_id = _seed_minimal_recommendation(conn, action="ADD")
        set_layer_enabled(conn, "portfolio_sync", enabled=False)
        conn.commit()
        try:
            result = evaluate_recommendation(conn, rec_id)
        finally:
            _enable_safety_layers(conn)
    assert "safety_layers_enabled" in result.failed_rules


@pytest.mark.integration
def test_exit_not_blocked_by_safety_layers() -> None:
    with psycopg.connect(_test_database_url()) as conn:
        rec_id = _seed_minimal_recommendation(conn, action="EXIT")
        set_layer_enabled(conn, "fx_rates", enabled=False)
        set_layer_enabled(conn, "portfolio_sync", enabled=False)
        conn.commit()
        try:
            result = evaluate_recommendation(conn, rec_id)
        finally:
            _enable_safety_layers(conn)
    # EXIT never hits the BUY/ADD-only block — safety_layers_enabled
    # must not appear in failed_rules regardless.
    assert "safety_layers_enabled" not in result.failed_rules


@pytest.mark.integration
def test_buy_not_blocked_by_safety_layers_when_enabled() -> None:
    with psycopg.connect(_test_database_url()) as conn:
        _enable_safety_layers(conn)
        rec_id = _seed_minimal_recommendation(conn, action="BUY")
        result = evaluate_recommendation(conn, rec_id)
    # The BUY may still fail on other rules (no coverage, etc.) but
    # NOT on safety_layers_enabled.
    assert "safety_layers_enabled" not in result.failed_rules
