"""Unit coverage for ``app.db.pool.open_pool`` (#717, extracted #719).

The dev stack went unresponsive after ~6h when a Docker port-forwarder
silently closed a pooled connection — without TCP keepalives or a
``check`` validator, every subsequent ``pool.connection()`` blocked
forever. The fix is config-only; this test pins the config so a
future refactor cannot regress it without breaking the smoke gate.

The helper was extracted to ``app.db.pool`` in #719 so both the FastAPI
process and the out-of-process jobs runtime share a single source of
truth.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import app.db.pool as _pool


def test_open_pool_applies_dead_conn_defences() -> None:
    """`open_pool` must hand `ConnectionPool` the keepalive kwargs,
    the per-checkout validator, and the recycle / timeout caps that
    together prevent the wedge described in #717.
    """
    fake_pool = MagicMock()
    with patch.object(_pool, "ConnectionPool", return_value=fake_pool) as ctor:
        result = _pool.open_pool("test_pool", min_size=1, max_size=10)

        assert result is fake_pool
        fake_pool.wait.assert_called_once()

        args, kwargs = ctor.call_args
        # First positional arg is the conninfo from settings.
        assert args[0]
        assert kwargs["min_size"] == 1
        assert kwargs["max_size"] == 10
        assert kwargs["name"] == "test_pool"

        # libpq TCP keepalives — OS-level dead-peer detection (#717).
        assert kwargs["kwargs"] == {
            "keepalives": 1,
            "keepalives_idle": 30,
            "keepalives_interval": 10,
            "keepalives_count": 3,
        }

        # Validator must come from the `ConnectionPool` symbol used by
        # `open_pool`, not a hand-rolled wrapper that could drift
        # from upstream's SELECT-1 contract. The comparison runs while
        # the patch is active so both references point at the same
        # mock attribute.
        assert kwargs["check"] is _pool.ConnectionPool.check_connection

        # Proactive recycle so a single bad conn cannot wedge the pool
        # for the remainder of uptime.
        assert kwargs["max_idle"] == 600.0
        assert kwargs["max_lifetime"] == 1800.0

        # Bounded checkout wait — surfaces a saturated/wedged pool as a
        # 503 instead of an indefinite event-loop block.
        assert kwargs["timeout"] == 15.0


def test_get_conn_maps_pool_timeout_to_503() -> None:
    """`Depends(get_conn)` routes must surface a wedged-pool checkout as
    a 503, not the FastAPI-default 500. Without this mapping, the only
    endpoint that returned a clean 503 on pool failure was `/health`
    (which catches Exception inline). Every other route raised a 500
    so operators couldn't distinguish "pool wedge" from "buggy handler".
    See #717.
    """
    from contextlib import contextmanager
    from typing import Any

    from fastapi.testclient import TestClient
    from psycopg_pool import PoolTimeout

    from app.main import app

    class _BrokenPool:
        @contextmanager
        def connection(self) -> Any:
            raise PoolTimeout("checkout timed out")
            yield None  # pragma: no cover — unreachable, satisfies generator protocol

    saved = getattr(app.state, "db_pool", None)
    app.state.db_pool = _BrokenPool()
    try:
        client = TestClient(app, raise_server_exceptions=False)
        # `/health/db` is the smallest endpoint that uses `Depends(get_conn)`.
        resp = client.get("/health/db")
        assert resp.status_code == 503, resp.text
    finally:
        if saved is None:
            if hasattr(app.state, "db_pool"):
                delattr(app.state, "db_pool")
        else:
            app.state.db_pool = saved


def test_get_conn_does_not_swallow_handler_pool_timeout() -> None:
    """Regression for PR #718 round 1 review: a `PoolTimeout` raised
    inside a route handler (i.e. AFTER successful checkout) must
    propagate untouched, not be silently rewritten as a 503. The
    only PoolTimeout that maps to 503 is the one raised by checkout
    itself.
    """
    from contextlib import contextmanager
    from typing import Any

    from fastapi import APIRouter, Depends
    from fastapi.testclient import TestClient
    from psycopg_pool import PoolTimeout

    from app.db import get_conn
    from app.main import app

    class _GoodPool:
        @contextmanager
        def connection(self) -> Any:
            yield object()  # checkout succeeds

    saved_pool = getattr(app.state, "db_pool", None)
    app.state.db_pool = _GoodPool()

    router = APIRouter()

    @router.get("/__pool_timeout_in_handler")
    def _broken(_: object = Depends(get_conn)) -> dict[str, str]:
        raise PoolTimeout("simulated handler-side raise")

    app.include_router(router)
    try:
        client = TestClient(app, raise_server_exceptions=False)
        resp = client.get("/__pool_timeout_in_handler")
        # 503 would mean the dependency caught + rewrote a handler-side
        # raise. We require 500 (or any non-503) to prove propagation.
        assert resp.status_code != 503, resp.text
    finally:
        # Strip the test route + restore pool.
        app.router.routes[:] = [
            r for r in app.router.routes if getattr(r, "path", None) != "/__pool_timeout_in_handler"
        ]
        if saved_pool is None:
            if hasattr(app.state, "db_pool"):
                delattr(app.state, "db_pool")
        else:
            app.state.db_pool = saved_pool


def test_db_pool_imports_real_connection_pool() -> None:
    """`app.db.pool.ConnectionPool` must be the real
    `psycopg_pool.ConnectionPool`, not an alias or shim. A future
    refactor that quietly swaps the import would break the dead-conn
    defences (the mock-based test above only proves the helper builds
    the right config — this one proves the helper builds it on top of
    the real upstream class).
    """
    from psycopg_pool import ConnectionPool

    assert _pool.ConnectionPool is ConnectionPool
