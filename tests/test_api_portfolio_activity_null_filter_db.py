"""DB integration test for the get_activity nullable instrument_id filter (#1961).

The SQL mechanism under test is the optional ``instrument_id`` filter added in
#1926: ``%(instrument_id)s::bigint IS NULL OR instrument_id = %(instrument_id)s::bigint``.
When the caller omits ``instrument_id`` (the Portfolio-level Activity tab), the
param binds as an untyped NULL; without the ``::bigint`` cast, Postgres cannot
infer its type and aborts the whole statement with ``AmbiguousParameter`` — a
500 that the mocked-cursor tests in ``test_api_portfolio.py`` structurally
cannot catch (a mock ignores param types, so only a real planner reproduces it).
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import psycopg
import pytest

from app.api.portfolio import get_activity
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401  (fixture)

pytestmark = pytest.mark.integration

_IID_A = 90101
_IID_B = 90102


def _seed_two_instruments(conn: psycopg.Connection[Any]) -> None:
    """One own-portfolio open event on each of two instruments (USD display).

    ``display_currency='USD'`` short-circuits the FX path so the test does not
    depend on seeded ``fx_rates`` rows.
    """
    conn.execute("UPDATE runtime_config SET display_currency = 'USD' WHERE id = TRUE")
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, is_tradable)
        VALUES (%(a)s, 'ACTA', 'Act A', '4', 'USD', TRUE),
               (%(b)s, 'ACTB', 'Act B', '4', 'USD', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        {"a": _IID_A, "b": _IID_B},
    )
    conn.execute(
        """
        INSERT INTO trade_events (position_id, etoro_instrument_id, instrument_id, event_kind,
                                  side, units, price, executed_at, investment_usd,
                                  realized_pnl_usd, source, raw_payload)
        VALUES
          (%(a)s, %(a)s, %(a)s, 'open', 'buy', 10, 100, %(t)s, 1000, NULL, 'etoro_sync', '{}'::jsonb),
          (%(b)s, %(b)s, %(b)s, 'open', 'buy',  5,  50, %(t)s,  250, NULL, 'etoro_sync', '{}'::jsonb)
        ON CONFLICT DO NOTHING
        """,
        {"a": _IID_A, "b": _IID_B, "t": datetime(2026, 4, 1, 12, 0, tzinfo=UTC)},
    )


def test_get_activity_without_instrument_id_does_not_raise(
    ebull_test_conn: psycopg.Connection[Any],  # noqa: F811
) -> None:
    """The no-filter path (instrument_id=None) must execute and return all rows.

    Regression guard for #1961: the untyped-NULL param would raise
    ``psycopg.errors.AmbiguousParameter`` before the ``::bigint`` cast.
    """
    _seed_two_instruments(ebull_test_conn)
    resp = get_activity(limit=100, include_mirrors=False, instrument_id=None, conn=ebull_test_conn)
    assert resp.total == 2
    assert len(resp.events) == 2


def test_get_activity_with_instrument_id_filters_to_one(
    ebull_test_conn: psycopg.Connection[Any],  # noqa: F811
) -> None:
    """The per-instrument path returns only the matching instrument's events."""
    _seed_two_instruments(ebull_test_conn)
    resp = get_activity(limit=100, include_mirrors=False, instrument_id=_IID_A, conn=ebull_test_conn)
    assert resp.total == 1
    assert len(resp.events) == 1
    assert resp.events[0].etoro_instrument_id == _IID_A
