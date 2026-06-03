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
    _assert_worker_relations_under_ceiling,
    _create_empty_database,
    _drop_orphan_workers_older_than,
    _ensure_database,
    _force_drop_invalid_test_dbs,
    _swap_database,
    template_database_url,
    test_db_available,
)

# #1444 — these tests CREATE/DROP/UPDATE globally-named databases + invoke
# the global sweep, which takes cluster-wide CREATE/DROP DATABASE locks.
# Run concurrently across xdist workers they deadlock each other (observed:
# 3 workers wedged 40min on mutual DROP of the same fixed-name DBs). Pin the
# whole module to one xdist group (with --dist=loadgroup) so every real-DB
# reaper test — here and in test_dev_test_db_reaper.py — serialises onto a
# single worker. (The refined long-term shape is to unit-test the pure rail
# policy and keep real-DB tests minimal — see test_dev_test_db_reaper.py.)
pytestmark = pytest.mark.xdist_group("reaper_db_ops")

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


# ── #1401 — invalid-DB (datconnlimit=-2) force-reaper ────────────────
#
# A SIGKILL'd worker (or an interrupted DROP ... WITH (FORCE)) leaves a
# database marked ``datconnlimit = -2``: PG refuses all new connections,
# so it can only be force-dropped. The age-gated, plain-DROP orphan
# sweep above cannot clear it. ``UPDATE pg_database SET datconnlimit =
# -2`` faithfully reproduces the corpse (verified: connections are
# refused; ``DROP ... WITH (FORCE)`` still works).

_INVALID_TEST_NAME = "ebull_test_9999999999_dead01_gw95"
_INVALID_MIG_NAME = "ebull_mig156_deadbeef"
_VALID_TEST_NAME = "ebull_test_9999999999_dead02_gw94"


def _mark_invalid(admin: psycopg.Connection[object], name: str) -> None:
    """Mark a database ``datconnlimit = -2`` (interrupted-drop corpse)."""
    with admin.cursor() as cur:
        cur.execute("UPDATE pg_database SET datconnlimit = -2 WHERE datname = %s", (name,))


@pytest.mark.integration
def test_force_drops_invalid_worker_db(_admin_conn: psycopg.Connection[object]) -> None:
    """A ``datconnlimit=-2`` ``ebull_test_*`` corpse is force-dropped."""
    _drop_if_exists(_admin_conn, _INVALID_TEST_NAME)
    _create_empty_database(_admin_conn, _INVALID_TEST_NAME)
    _mark_invalid(_admin_conn, _INVALID_TEST_NAME)
    try:
        dropped = _force_drop_invalid_test_dbs()
        assert _INVALID_TEST_NAME in dropped, f"invalid worker corpse must be reaped; got {dropped!r}"
        assert not _ensure_database(_admin_conn, _INVALID_TEST_NAME), "corpse still present after reap"
    finally:
        _drop_if_exists(_admin_conn, _INVALID_TEST_NAME)


@pytest.mark.integration
def test_force_drops_invalid_mig_db(_admin_conn: psycopg.Connection[object]) -> None:
    """A ``datconnlimit=-2`` ``ebull_mig*`` corpse is force-dropped.

    The original sweep matched only ``ebull_test%`` — these mig DBs were
    the family that leaked uncovered (#1401).
    """
    _drop_if_exists(_admin_conn, _INVALID_MIG_NAME)
    _create_empty_database(_admin_conn, _INVALID_MIG_NAME)
    _mark_invalid(_admin_conn, _INVALID_MIG_NAME)
    try:
        dropped = _force_drop_invalid_test_dbs()
        assert _INVALID_MIG_NAME in dropped, f"invalid mig corpse must be reaped; got {dropped!r}"
        assert not _ensure_database(_admin_conn, _INVALID_MIG_NAME), "mig corpse still present after reap"
    finally:
        _drop_if_exists(_admin_conn, _INVALID_MIG_NAME)


@pytest.mark.integration
def test_invalid_reaper_leaves_valid_db(_admin_conn: psycopg.Connection[object]) -> None:
    """A VALID (datconnlimit=-1) test DB is never touched by the invalid reaper.

    Guards against the reaper widening into the live-worker-DB sweep's
    territory: only corpses (-2) are its concern.
    """
    _drop_if_exists(_admin_conn, _VALID_TEST_NAME)
    _create_empty_database(_admin_conn, _VALID_TEST_NAME)
    try:
        dropped = _force_drop_invalid_test_dbs()
        assert _VALID_TEST_NAME not in dropped, f"valid DB must survive invalid reaper; got {dropped!r}"
        assert _ensure_database(_admin_conn, _VALID_TEST_NAME), "valid DB wrongly dropped"
    finally:
        _drop_if_exists(_admin_conn, _VALID_TEST_NAME)


@pytest.mark.integration
def test_invalid_reaper_ci_short_circuit(monkeypatch: pytest.MonkeyPatch) -> None:
    """CI=true short-circuits the invalid reaper (ephemeral containers never accumulate corpses)."""
    monkeypatch.setenv("CI", "true")
    assert _force_drop_invalid_test_dbs() == []


# ── #1401 — worker-DB relation-count tripwire ────────────────────────


@pytest.mark.integration
def test_relation_ceiling_passes_for_template(
    _admin_conn: psycopg.Connection[object],
) -> None:
    """The fully-migrated template (≈9.6k relations) is well under the ceiling."""
    with psycopg.connect(template_database_url()) as conn:
        _assert_worker_relations_under_ceiling(conn)  # must not raise


@pytest.mark.integration
def test_relation_ceiling_tripwire_fires(
    _admin_conn: psycopg.Connection[object],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When the relation count exceeds the ceiling, the tripwire fails loudly."""
    monkeypatch.setattr(
        "tests.fixtures.ebull_test_db._WORKER_DB_RELATION_CEILING",
        1,
    )
    with psycopg.connect(template_database_url()) as conn:
        with pytest.raises(AssertionError, match="TRIPWIRE"):
            _assert_worker_relations_under_ceiling(conn)
