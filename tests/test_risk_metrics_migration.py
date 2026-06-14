"""Schema test for migration 198 — #591 PR-B Task B2 two-layer risk metrics.

The ``ebull_test_conn`` fixture applies all migrations (incl. sql/198) to the
template DB, so both tables already exist. Asserts:
  - both tables exist;
  - observations is RANGE-partitioned on as_of_date;
  - the _default partition exists and stays EMPTY after a current-quarter
    as_of_date insert (the row routes to a quarterly leaf, not _default);
  - a window_key='1y' row inserts into both tables;
  - an invalid window_key is rejected by the CHECK;
  - an invalid status value is rejected by the CHECK.
"""

from __future__ import annotations

from datetime import UTC, date, datetime

import psycopg
import pytest

from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export

pytestmark = pytest.mark.db

_IID = 591_000_001
_VERSION = "risk_v1"


def test_both_tables_exist(ebull_test_conn: psycopg.Connection[tuple]) -> None:  # noqa: F811
    conn = ebull_test_conn
    with conn.cursor() as cur:
        cur.execute(
            "SELECT to_regclass('instrument_risk_metrics_observations'), "
            "       to_regclass('instrument_risk_metrics_current')"
        )
        obs, cur_tbl = cur.fetchone()
    assert obs is not None
    assert cur_tbl is not None


def test_observations_is_range_partitioned(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    conn = ebull_test_conn
    with conn.cursor() as cur:
        cur.execute(
            "SELECT partstrat FROM pg_partitioned_table "
            "WHERE partrelid = 'instrument_risk_metrics_observations'::regclass"
        )
        row = cur.fetchone()
    assert row is not None
    assert row[0] == "r"  # RANGE


def test_current_quarter_row_routes_to_leaf_not_default(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    conn = ebull_test_conn
    today = date.today()
    conn.execute(
        """
        INSERT INTO instrument_risk_metrics_observations
            (instrument_id, as_of_date, metric_version, window_key, computed_at,
             cagr, vol_annualized, n_returns, cagr_status, vol_status)
        VALUES (%s, %s, %s, '1y', %s, 0.1234, 0.2200, 251, 'ok', 'ok')
        """,
        (_IID, today, _VERSION, datetime.now(UTC)),
    )
    conn.commit()
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM instrument_risk_metrics_observations_default")
        default_count = cur.fetchone()[0]
        cur.execute(
            "SELECT COUNT(*) FROM instrument_risk_metrics_observations "
            "WHERE instrument_id = %s",
            (_IID,),
        )
        total = cur.fetchone()[0]
    assert default_count == 0  # routed to a quarterly leaf
    assert total == 1


def test_current_row_inserts(ebull_test_conn: psycopg.Connection[tuple]) -> None:  # noqa: F811
    conn = ebull_test_conn
    conn.execute(
        """
        INSERT INTO instrument_risk_metrics_current
            (instrument_id, metric_version, window_key, as_of_date, computed_at,
             cagr, vol_annualized, beta, beta_r2, n_returns, cagr_status)
        VALUES (%s, %s, '1y', %s, %s, 0.1, 0.2, 1.05, 0.88, 251, 'ok')
        """,
        (_IID, _VERSION, date.today(), datetime.now(UTC)),
    )
    conn.commit()
    with conn.cursor() as cur:
        cur.execute(
            "SELECT window_key FROM instrument_risk_metrics_current WHERE instrument_id = %s",
            (_IID,),
        )
        row = cur.fetchone()
    assert row is not None
    assert row[0] == "1y"


def test_invalid_window_key_rejected(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    conn = ebull_test_conn
    with pytest.raises(psycopg.errors.CheckViolation):
        conn.execute(
            """
            INSERT INTO instrument_risk_metrics_observations
                (instrument_id, as_of_date, metric_version, window_key)
            VALUES (%s, %s, %s, '5y')
            """,
            (_IID, date.today(), _VERSION),
        )
    conn.rollback()


def test_invalid_status_rejected(ebull_test_conn: psycopg.Connection[tuple]) -> None:  # noqa: F811
    conn = ebull_test_conn
    with pytest.raises(psycopg.errors.CheckViolation):
        conn.execute(
            """
            INSERT INTO instrument_risk_metrics_observations
                (instrument_id, as_of_date, metric_version, window_key, cagr_status)
            VALUES (%s, %s, %s, '1y', 'totally_bogus')
            """,
            (_IID, date.today(), _VERSION),
        )
    conn.rollback()
