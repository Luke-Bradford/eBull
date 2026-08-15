"""sec_rate dissolution charges N concurrent job bodies; still fits the dev budget (#1542).

Fast-tier (no DB): the auto-marker only marks a module ``db`` when its source
mentions ``psycopg.connect`` / ``TestClient`` / the test-DB URL — this one does
not.
"""

from app.db import pg_settings
from app.jobs.sec_lane_gate import SEC_LANE_MAX_CONCURRENCY

_DEV_USABLE = 27  # dev box: max_connections=30 − superuser_reserved_connections=3


def test_demand_is_exactly_the_known_terms_plus_sec_lane_bodies():
    expected = (
        pg_settings.DB_POOL_MAX_SIZE
        + pg_settings.AUDIT_POOL_MAX_SIZE
        + pg_settings.API_FIXED_LONGLIVED_CONNS
        + pg_settings.JOBS_POOL_MAX_SIZE
        + pg_settings.BACKGROUND_POOL_MAX_SIZE
        + pg_settings.JOBS_FIXED_LONGLIVED_CONNS
        + pg_settings.JOBS_NON_SEC_MAX_CONCURRENCY * pg_settings.JOBS_NON_SEC_CONNECTIONS_PER_EXECUTION
        + pg_settings.JOBS_BACKTEST_PROGRESS_CONNECTIONS
        + SEC_LANE_MAX_CONCURRENCY
    )
    assert pg_settings._dev_profile_connection_demand() == expected


def test_demand_plus_reserve_exactly_fits_usable():
    demand = pg_settings._dev_profile_connection_demand() + pg_settings.CONNECTION_BUDGET_RESERVE
    assert demand == _DEV_USABLE


def test_execution_gates_fit_the_modeled_cadence_burst():
    assert pg_settings.JOBS_NON_SEC_MAX_CONCURRENCY == 2
    assert pg_settings.JOBS_GENERAL_NON_SEC_MAX_CONCURRENCY == 1
    assert pg_settings.JOBS_PAPER_LIFECYCLE_MAX_CONCURRENCY == 1
    assert pg_settings.JOBS_NON_SEC_MAX_CONCURRENCY == (
        pg_settings.JOBS_GENERAL_NON_SEC_MAX_CONCURRENCY + pg_settings.JOBS_PAPER_LIFECYCLE_MAX_CONCURRENCY
    )
    assert pg_settings.JOBS_NON_SEC_CONNECTIONS_PER_EXECUTION == 2
    assert pg_settings.JOBS_BACKTEST_PROGRESS_CONNECTIONS == 1
