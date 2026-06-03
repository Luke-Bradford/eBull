"""Tests for the production-side orphan test-DB reaper (#1444).

Design note (the calculated/refined shape, #1444 review): the reaper's
RAIL POLICY — given ``(datname, has_active_backend)`` catalog rows + a
clock, which DBs qualify to drop — is a **pure function**
(``select_orphans_to_drop``). It is covered exhaustively here by fast,
DB-free, parallel-safe unit tests. Only the SQL MECHANISM (actually
issuing ``DROP DATABASE``) needs a real database, so there is exactly
ONE integration test for it, pinned to a single xdist group so the
cluster-wide ``CREATE``/``DROP DATABASE`` locks can't deadlock across
workers (the failure mode that wedged the full suite pre-refactor).

Rails (module docstring has the full safety model):
* Rail 2 — activity: a live backend → skip (protects a sibling run).
* Rail 1 — name regex.
* Rail 3 — age backstop.
* Rail 0 — ``NEVER_DROP`` protect-set.
Live-capable orphans use plain ``DROP`` (never FORCE — that is reserved
for ``datconnlimit=-2`` corpses, which have no live sibling to evict).
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import psycopg
import pytest
from psycopg import sql

from app.config import settings
from app.db.dev_test_db_reaper import (
    NEVER_DROP,
    run_orphan_test_db_reap,
    select_orphans_to_drop,
)
from tests.fixtures.ebull_test_db import (
    _admin_database_url,
    _create_empty_database,
    _ensure_database,
    test_db_available,
)

# A fixed "now" so the 10-digit epoch in the test names is deterministically
# old. Epoch 0000000002 = 1970; any sane min_age leaves it far in the past.
_NOW = datetime(2025, 1, 1, tzinfo=UTC)
_OLD = "ebull_test_0000000002_aaaaaa_gw0"  # matches regex, ancient epoch


# ── Pure rail-policy tests (no DB, parallel-safe) ────────────────────


def test_rail_drops_qualifying_orphan() -> None:
    assert select_orphans_to_drop([(_OLD, False)], min_age=timedelta(hours=1), now=_NOW) == [_OLD]


def test_rail_activity_skips_live_backend() -> None:
    # Rail 2 — a DB with a live backend is never dropped (sibling protection).
    assert select_orphans_to_drop([(_OLD, True)], min_age=timedelta(hours=1), now=_NOW) == []


def test_rail_regex_skips_nonmatching_name() -> None:
    # Rail 1 — operator-handcrafted / wrong-shape names survive.
    rows = [("ebull_test_operator_dataset", False), ("ebull", False), ("postgres", False)]
    assert select_orphans_to_drop(rows, min_age=timedelta(0), now=_NOW) == []


def test_rail_age_skips_recent() -> None:
    # Rail 3 — a fresh-epoch DB (now-ish) survives the age backstop.
    fresh_epoch = int((_NOW - timedelta(minutes=1)).timestamp())
    fresh = f"ebull_test_{fresh_epoch:010d}_bbbbbb_gw1"
    assert select_orphans_to_drop([(fresh, False)], min_age=timedelta(hours=1), now=_NOW) == []


def test_rail_age_boundary_inclusive() -> None:
    # epoch exactly at the threshold is NOT dropped (>= threshold skips).
    threshold = int((_NOW - timedelta(hours=1)).timestamp())
    at = f"ebull_test_{threshold:010d}_cccccc_gw2"
    assert select_orphans_to_drop([(at, False)], min_age=timedelta(hours=1), now=_NOW) == []
    one_older = f"ebull_test_{threshold - 1:010d}_cccccc_gw2"
    assert select_orphans_to_drop([(one_older, False)], min_age=timedelta(hours=1), now=_NOW) == [one_older]


def test_rail_never_drop_excluded() -> None:
    # Rail 0 — even if a protected name somehow matched the regex+age, the
    # NEVER_DROP set excludes it. (template name is the realistic case.)
    rows = [(name, False) for name in NEVER_DROP]
    assert select_orphans_to_drop(rows, min_age=timedelta(0), now=_NOW) == []


def test_rail_mixed_batch_selects_only_qualifying() -> None:
    rows = [
        (_OLD, False),  # drop
        (_OLD.replace("_gw0", "_gw9"), True),  # live → skip
        ("ebull_test_template", False),  # NEVER_DROP → skip
        ("ebull_handcrafted", False),  # regex → skip
    ]
    assert select_orphans_to_drop(rows, min_age=timedelta(hours=1), now=_NOW) == [_OLD]


def test_reap_skipped_outside_dev(monkeypatch: pytest.MonkeyPatch) -> None:
    """The jobs-process reaper is a hard no-op when app_env is not dev-like.

    Pure gate test — must NOT touch the database when skipped (a prod jobs
    process must never connect to the admin DB to reap test DBs).
    """
    monkeypatch.setattr(settings, "app_env", "prod")

    def _boom(*_a: object, **_k: object) -> list[str]:
        raise AssertionError("reaper touched the DB despite app_env=prod")

    monkeypatch.setattr("app.db.dev_test_db_reaper.sweep_orphan_test_databases", _boom)
    monkeypatch.setattr("app.db.dev_test_db_reaper.force_drop_invalid_test_dbs", _boom)

    result = run_orphan_test_db_reap()
    assert result.skipped is True
    assert result.total_reaped == 0


# ── Single integration test for the SQL mechanism (serialized) ───────


@pytest.mark.integration
@pytest.mark.xdist_group("reaper_db_ops")
def test_reap_drops_real_orphan_end_to_end(monkeypatch: pytest.MonkeyPatch) -> None:
    """ONE real-DB test: prove the sweep actually drops a stale orphan and
    leaves ``ebull`` untouched. Pinned to the ``reaper_db_ops`` xdist group
    (shared with test_orphan_sweep.py) so cluster-wide CREATE/DROP DATABASE
    locks never deadlock across workers.
    """
    if not test_db_available():
        pytest.skip("ebull_test DB stack unavailable")
    monkeypatch.setattr(settings, "app_env", "dev")
    name = "ebull_test_0000000002_eeeeee_gw0"  # ancient epoch, unique hex
    with psycopg.connect(_admin_database_url(), autocommit=True) as admin:
        with admin.cursor() as cur:
            cur.execute(sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(name)))
        _create_empty_database(admin, name)
        try:
            # admin_url pins the reap to the SEPARATE test cluster (C1 #1447)
            # — the same cluster the orphan was created on above. The
            # jobs-process default would target the dev ``ebull`` cluster.
            result = run_orphan_test_db_reap(admin_url=_admin_database_url())
            assert result.skipped is False
            assert name in result.orphans, f"got {result!r}"
            assert not _ensure_database(admin, name), "orphan still present after reap"
            # Protect-rail check: a non-``ebull_test_*`` DB (the maintenance
            # ``postgres`` DB) must survive — the sweep only targets the
            # test-DB name pattern. (Post-C1 the dev ``ebull`` DB does not
            # live on this cluster, so assert against ``postgres`` instead.)
            assert _ensure_database(admin, "postgres"), "maintenance DB vanished"
        finally:
            with admin.cursor() as cur:
                cur.execute(sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(name)))
