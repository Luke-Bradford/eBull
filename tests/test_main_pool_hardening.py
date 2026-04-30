"""Unit coverage for ``app.main._open_pool`` (#717).

The dev stack went unresponsive after ~6h when a Docker port-forwarder
silently closed a pooled connection — without TCP keepalives or a
``check`` validator, every subsequent ``pool.connection()`` blocked
forever. The fix is config-only; this test pins the config so a
future refactor cannot regress it without breaking the smoke gate.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import app.main as _main


def test_open_pool_applies_dead_conn_defences() -> None:
    """`_open_pool` must hand `ConnectionPool` the keepalive kwargs,
    the per-checkout validator, and the recycle / timeout caps that
    together prevent the wedge described in #717.
    """
    fake_pool = MagicMock()
    with patch.object(_main, "ConnectionPool", return_value=fake_pool) as ctor:
        result = _main._open_pool("test_pool", min_size=1, max_size=10)

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
        # `_open_pool`, not a hand-rolled wrapper that could drift
        # from upstream's SELECT-1 contract. The comparison runs while
        # the patch is active so both references point at the same
        # mock attribute.
        assert kwargs["check"] is _main.ConnectionPool.check_connection

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


def test_app_main_imports_real_connection_pool() -> None:
    """`app.main.ConnectionPool` must be the real `psycopg_pool.ConnectionPool`,
    not an alias or shim. A future refactor that quietly swaps the
    import would break the dead-conn defences (the mock-based test
    above only proves the helper builds the right config — this one
    proves the helper builds it on top of the real upstream class).
    """
    from psycopg_pool import ConnectionPool

    assert _main.ConnectionPool is ConnectionPool
