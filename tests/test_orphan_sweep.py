"""Orphan-sweep regression tests for ``_drop_orphan_workers_older_than`` (#1208 Phase 2).

The sweep MUST:

1. Drop stale-named worker DBs whose backends have drained (the leak
   shape that motivated this PR — 45 leaked DBs on the dev cluster).
2. Leave fresh-named DBs alone (concurrent pytest invocation safety).
3. Leave OLD-but-active DBs alone (the Codex 1b BLOCKING invariant —
   sibling pytest run >1h that still holds a backend).
4. Refuse to ever target ``ebull`` / ``ebull_test_template`` /
   ``postgres`` regardless of name shape.

Each rail of the spec §4.3 safety model has a dedicated test.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta

import psycopg
import pytest
from psycopg import sql

from app.config import settings
from tests.fixtures.ebull_test_db import (
    _NEVER_DROP,
    TEMPLATE_DB_NAME,
    _admin_database_url,
    _create_empty_database,
    _drop_orphan_workers_older_than,
    _ensure_database,
    _swap_database,
    test_db_available,
)

_STALE_EPOCH_NAME = "ebull_test_0000000001_aaaaaa_gw99"
_STALE_ACTIVE_NAME = "ebull_test_0000000001_cccccc_gw97"


@pytest.fixture
def _admin_conn() -> Iterator[psycopg.Connection[object]]:
    if not test_db_available():
        pytest.skip("ebull_test DB stack unavailable")
    with psycopg.connect(_admin_database_url(), autocommit=True) as conn:
        yield conn


def _drop_if_exists(admin: psycopg.Connection[object], name: str) -> None:
    with admin.cursor() as cur:
        cur.execute(
            sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(
                sql.Identifier(name),
            )
        )


@pytest.mark.integration
def test_drops_stale_orphan_leaves_protected_dbs(
    _admin_conn: psycopg.Connection[object],
) -> None:
    """Rail 3 backstop — stale-named inactive DB is dropped; protected names survive."""
    fresh_name = f"ebull_test_{int(datetime.now(UTC).timestamp())}_bbbbbb_gw98"

    _drop_if_exists(_admin_conn, _STALE_EPOCH_NAME)
    _drop_if_exists(_admin_conn, fresh_name)
    _create_empty_database(_admin_conn, _STALE_EPOCH_NAME)
    _create_empty_database(_admin_conn, fresh_name)
    try:
        dropped = _drop_orphan_workers_older_than(timedelta(hours=1))
        assert _STALE_EPOCH_NAME in dropped, f"stale-named inactive DB must be swept; got dropped={dropped!r}"
        assert fresh_name not in dropped, f"fresh-named DB must survive age filter; got dropped={dropped!r}"
        # Rail 0 — protect set survives regardless.
        for protected in _NEVER_DROP:
            assert protected not in dropped, f"{protected!r} must never appear in sweep output"
        # Specifically pin the operator dev DB + template.
        assert _ensure_database(_admin_conn, "ebull"), "operator dev DB vanished"
        assert _ensure_database(_admin_conn, TEMPLATE_DB_NAME), "test-template DB vanished"
    finally:
        _drop_if_exists(_admin_conn, _STALE_EPOCH_NAME)
        _drop_if_exists(_admin_conn, fresh_name)


@pytest.mark.integration
def test_old_but_active_db_is_not_dropped(
    _admin_conn: psycopg.Connection[object],
) -> None:
    """Rail 2 — Codex 1b BLOCKING invariant.

    A DB old enough to pass the age filter MUST survive if any backend
    is still connected (e.g. a sibling pytest run holding its session
    keepalive). Plain DROP without FORCE plus the pg_stat_activity
    filter together guarantee this.
    """
    _drop_if_exists(_admin_conn, _STALE_ACTIVE_NAME)
    _create_empty_database(_admin_conn, _STALE_ACTIVE_NAME)

    keepalive_url = _swap_database(settings.database_url, _STALE_ACTIVE_NAME)
    keepalive: psycopg.Connection[object] | None = None
    try:
        keepalive = psycopg.connect(
            keepalive_url,
            autocommit=True,
            connect_timeout=2,
        )
        dropped = _drop_orphan_workers_older_than(timedelta(hours=1))
        assert _STALE_ACTIVE_NAME not in dropped, (
            "active-backend DB must survive even when older than min_age (Codex 1b BLOCKING invariant)"
        )
    finally:
        if keepalive is not None:
            keepalive.close()
        _drop_if_exists(_admin_conn, _STALE_ACTIVE_NAME)


@pytest.mark.integration
def test_non_matching_name_is_skipped(
    _admin_conn: psycopg.Connection[object],
) -> None:
    """Rail 1 — names that don't match the orphan regex are left alone.

    Catch-all rail: any operator-created DB whose name doesn't match
    ``ebull_test_<epoch>_<hex>_<suffix>`` must survive.
    """
    weird_name = "ebull_test_operator_handcrafted_dataset"
    _drop_if_exists(_admin_conn, weird_name)
    _create_empty_database(_admin_conn, weird_name)
    try:
        dropped = _drop_orphan_workers_older_than(timedelta(seconds=0))
        assert weird_name not in dropped, "regex-unmatched DB must survive even with min_age=0"
    finally:
        _drop_if_exists(_admin_conn, weird_name)


@pytest.mark.integration
def test_ci_short_circuit_returns_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    """When CI=true the sweep is a no-op (zero-cost in ephemeral CI containers)."""
    monkeypatch.setenv("CI", "true")
    dropped = _drop_orphan_workers_older_than(timedelta(seconds=0))
    assert dropped == []
