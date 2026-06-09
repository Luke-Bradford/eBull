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
        + pg_settings.JOBS_STEADY_STATE_EXEC_CONNS
        + pg_settings.ORCHESTRATOR_GATE_CHECK_CONN
        + SEC_LANE_MAX_CONCURRENCY
    )
    assert pg_settings._dev_profile_connection_demand() == expected


def test_demand_plus_reserve_fits_usable_with_margin():
    demand = pg_settings._dev_profile_connection_demand() + pg_settings.CONNECTION_BUDGET_RESERVE
    assert demand <= _DEV_USABLE, f"demand {demand} > usable {_DEV_USABLE}"
    assert _DEV_USABLE - demand >= 1, "no connection margin left"
