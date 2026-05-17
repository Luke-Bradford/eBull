"""#1187 — Boot guard call-site tests (lifespan + jobs entrypoint).

Pins the two production call sites that invoke
``enforce_max_locks_floor``:

1. FastAPI lifespan (``app/main.py``): hard-fail raise propagates
   through ``TestClient.__enter__`` so the smoke gate catches the
   misconfiguration.
2. Jobs entrypoint (``app/jobs/__main__.py::_enforce_pg_locks_with_cleanup``):
   hard-fail raise must close ``fence_conn`` + ``pool`` before
   re-raising — otherwise the next jobs-process boot is blocked by
   a stale advisory lock on the singleton key.

Lifespan test mirrors the dev-DB skip + xdist serialisation pattern
in ``tests/smoke/test_app_boots.py`` so concurrent pytest invocations
do not race the real lifespan's migrations.

Jobs cleanup test exercises the REAL extracted helper, not a copy of
the diff pattern (Codex 2 round-2 WARNING — copy would not catch a
production reorder regression).

Spec §6.3 + plan §5.3 (deferred originally; written in response to
Codex 2 pre-push).
"""

from __future__ import annotations

import contextlib
from collections.abc import Iterator
from unittest.mock import MagicMock

import psycopg
import pytest

from app.db.pg_settings import PG_LOCKS_FLOOR, PgLocksFloorBreached
from app.jobs.__main__ import _enforce_pg_locks_with_cleanup


def _db_reachable() -> bool:
    """Mirror ``tests/smoke/test_app_boots.py::_db_reachable`` — skip
    lifespan-touching tests when the dev DB is unreachable."""
    from app.config import settings

    try:
        with psycopg.connect(settings.database_url, connect_timeout=2) as conn:
            conn.execute("SELECT 1").fetchone()
    except Exception:
        return False
    return True


# ---------------------------------------------------------------------------
# Lifespan — driven through TestClient; dev-DB-required path
# ---------------------------------------------------------------------------


@contextlib.contextmanager
def _dev_db_lifespan_lock() -> Iterator[None]:
    """Serialise lifespan migrations across concurrent pytest invocations.

    Copied shape from ``tests/smoke/test_app_boots.py::_dev_db_lifespan_lock``
    so this test does not race the smoke test under concurrent
    invocations on the same cluster.
    """
    from urllib.parse import urlparse, urlunparse

    from app.config import settings
    from tests.fixtures.ebull_test_db import EBULL_SMOKE_LIFESPAN_LOCK

    parsed = urlparse(settings.database_url)
    admin_url = urlunparse(parsed._replace(path="/postgres"))

    try:
        admin = psycopg.connect(admin_url, autocommit=True)
    except Exception:
        yield
        return

    try:
        with admin.cursor() as cur:
            cur.execute("SELECT pg_advisory_lock(%s)", (EBULL_SMOKE_LIFESPAN_LOCK,))
        try:
            yield
        finally:
            with contextlib.suppress(Exception), admin.cursor() as cur:
                cur.execute("SELECT pg_advisory_unlock(%s)", (EBULL_SMOKE_LIFESPAN_LOCK,))
    finally:
        admin.close()


@pytest.mark.xdist_group("dev_db_smoke")
@pytest.mark.skipif(
    not _db_reachable(),
    reason="dev Postgres not reachable; lifespan test requires the real DB",
)
def test_lifespan_propagates_pg_locks_breach(monkeypatch: pytest.MonkeyPatch) -> None:
    """Patch ``enforce_max_locks_floor`` to raise; assert TestClient
    enter surfaces the same ``PgLocksFloorBreached`` (after migrations
    + source-registry validation succeed)."""
    from fastapi.testclient import TestClient

    from app import main as app_main

    def _raise(_conn: object) -> None:
        raise PgLocksFloorBreached(value=64, floor=PG_LOCKS_FLOOR)

    monkeypatch.setattr("app.db.pg_settings.enforce_max_locks_floor", _raise)

    with _dev_db_lifespan_lock():
        with pytest.raises(PgLocksFloorBreached) as exc:
            with TestClient(app_main.app):
                pytest.fail("lifespan should have raised before yield")
    assert exc.value.value == 64
    assert exc.value.floor == PG_LOCKS_FLOOR


# ---------------------------------------------------------------------------
# Jobs entrypoint cleanup — exercise the REAL extracted helper
# ---------------------------------------------------------------------------


def _patch_jobs_psycopg_connect(monkeypatch: pytest.MonkeyPatch) -> None:
    """Patch ``app.jobs.__main__.psycopg.connect`` to a fake context
    manager so the helper tests stay pure unit (no real Postgres
    required). Codex 2 round-3 BLOCKING.
    """
    fake_conn = MagicMock()
    fake_cm = MagicMock()
    fake_cm.__enter__ = MagicMock(return_value=fake_conn)
    fake_cm.__exit__ = MagicMock(return_value=None)
    monkeypatch.setattr("app.jobs.__main__.psycopg.connect", MagicMock(return_value=fake_cm))


def test_enforce_pg_locks_with_cleanup_closes_fence_and_pool_on_guard_raise(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The real ``_enforce_pg_locks_with_cleanup`` helper must close
    BOTH ``fence_conn`` and ``pool`` before re-raising. Reorder /
    deletion / regression in the helper itself fails this test.
    """
    _patch_jobs_psycopg_connect(monkeypatch)

    fence_close_called = {"flag": False}
    pool_close_called = {"flag": False}

    fence_conn = MagicMock(spec=psycopg.Connection)
    fence_conn.close.side_effect = lambda: fence_close_called.update(flag=True)

    pool = MagicMock()
    pool.close.side_effect = lambda: pool_close_called.update(flag=True)

    # Patch enforce_max_locks_floor at the module from which the helper
    # imports it (deferred import inside the helper body → patch the
    # canonical module path).
    def _raise(_conn: object) -> None:
        raise PgLocksFloorBreached(value=64, floor=PG_LOCKS_FLOOR)

    monkeypatch.setattr("app.db.pg_settings.enforce_max_locks_floor", _raise)

    with pytest.raises(PgLocksFloorBreached):
        _enforce_pg_locks_with_cleanup(fence_conn, pool)

    assert fence_close_called["flag"] is True, (
        "fence_conn.close() must run before re-raise so the singleton "
        "advisory lock is released and the next jobs-process boot can "
        "acquire it"
    )
    assert pool_close_called["flag"] is True, "pool.close() must also run before re-raise"


def test_enforce_pg_locks_with_cleanup_swallows_secondary_close_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If ``fence_conn.close()`` or ``pool.close()`` itself raises
    during cleanup, the original ``PgLocksFloorBreached`` must still
    propagate (cleanup errors are suppressed via
    ``contextlib.suppress``). Without this, the operator-facing error
    would be the secondary close failure instead of the actionable
    ``ALTER SYSTEM`` guidance.
    """
    _patch_jobs_psycopg_connect(monkeypatch)

    fence_conn = MagicMock(spec=psycopg.Connection)
    fence_conn.close.side_effect = RuntimeError("fence close failed during cleanup")

    pool = MagicMock()
    pool.close.side_effect = RuntimeError("pool close also failed")

    def _raise(_conn: object) -> None:
        raise PgLocksFloorBreached(value=64, floor=PG_LOCKS_FLOOR)

    monkeypatch.setattr("app.db.pg_settings.enforce_max_locks_floor", _raise)

    with pytest.raises(PgLocksFloorBreached):
        _enforce_pg_locks_with_cleanup(fence_conn, pool)


def test_enforce_pg_locks_with_cleanup_no_op_when_floor_satisfied(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Happy path: guard passes → no cleanup → fence + pool stay open
    so the rest of ``main()`` can use them.
    """
    _patch_jobs_psycopg_connect(monkeypatch)

    fence_conn = MagicMock(spec=psycopg.Connection)
    pool = MagicMock()

    def _no_op(_conn: object) -> None:
        return None

    monkeypatch.setattr("app.db.pg_settings.enforce_max_locks_floor", _no_op)

    _enforce_pg_locks_with_cleanup(fence_conn, pool)

    fence_conn.close.assert_not_called()
    pool.close.assert_not_called()
