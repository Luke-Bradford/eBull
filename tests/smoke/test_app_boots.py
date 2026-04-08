"""Smoke test: the FastAPI app actually boots end-to-end.

Why this exists
---------------
On 2026-04-08 the backend failed to start because
``app/security/master_key.py`` had a SQL typo (``JOIN operators o ON
o.id = bc.operator_id`` -- the operators PK is ``operator_id``, not
``id``). The bug was inside the FastAPI lifespan, so it only fired on
real startup. Every existing unit test mocked the cursor, so 880
pytest checks were green while the running app was dead on its face.

The fix is this file: drive ``app.main.app`` through ``TestClient`` as
a context manager. ``TestClient.__enter__`` runs the lifespan against
the real database (migrations + pool open + master-key bootstrap), so
any SQL/import/config error in the lifespan path fails this test
loudly instead of being discovered by hand at the sign-in screen.

DB requirement and CI behaviour
-------------------------------
The smoke test requires a real Postgres at ``settings.database_url``.
On developer machines and any pipeline that brings up the dev DB this
just works. In a Postgres-less environment the module-level
``_db_reachable()`` probe runs once at collection time and the test
``skip``s with a clear reason instead of failing with an opaque
psycopg connection error. The skip is intentional: the smoke gate's
job is "did the lifespan come up against a real DB", and that
question is unanswerable without a DB -- a noisy failure there would
just train people to ignore the gate.

Keep this file fast and dependency-free: no fixtures, no mocks. The
in-test assertions are deliberately minimal -- the *real* test is
``TestClient.__enter__`` returning at all (that is what the original
master_key crash would have failed). The post-enter assertions are a
cheap coherence check that the lifespan finished setting the
``app.state`` flags it is contracted to set, so a future bug that
silently leaves the app in a half-initialised state (lifespan
returns but ``broker_key_loaded`` / ``boot_state`` never get
populated) also fails this test.
"""

from __future__ import annotations

import psycopg
import pytest
from fastapi.testclient import TestClient

from app.config import settings
from app.main import app


def _db_reachable() -> bool:
    """Probe the dev DB once at collection time.

    A short connect timeout keeps the skip path fast in CI envs that
    have no Postgres at all (the default psycopg connect timeout is
    long enough to feel like a hang). Any failure -- DNS, refused,
    auth, timeout -- is treated identically as "DB not available";
    the smoke gate is not the place to diagnose connection problems.
    """
    try:
        with psycopg.connect(settings.database_url, connect_timeout=2) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not _db_reachable(),
    reason="dev Postgres not reachable; smoke test requires the real DB",
)


def test_app_lifespan_boots_and_state_is_coherent() -> None:
    """Drive the real lifespan; fail loud if anything in startup breaks.

    The structural assertion is the ``with`` block returning at all:
    if ``master_key.bootstrap`` (or any other lifespan step) raises,
    ``TestClient.__enter__`` propagates the exception and the test
    fails before any assert runs. That is exactly the failure mode
    the original master_key SQL typo produced.

    The post-enter assertions then verify the lifespan actually
    populated the ``app.state`` contract documented in
    ``app/main.py::lifespan``. A future bug that swallows an error
    inside lifespan and yields with a half-initialised state would
    not be caught by the bare context-manager check; these asserts
    are the coherence backstop.
    """
    with TestClient(app) as client:
        # Lifespan must have populated every flag the rest of the app
        # reads off ``app.state``. Missing attributes here mean the
        # lifespan returned early or skipped its writes.
        assert hasattr(app.state, "boot_state")
        assert hasattr(app.state, "needs_setup")
        assert hasattr(app.state, "recovery_required")
        assert hasattr(app.state, "broker_key_loaded")
        assert hasattr(app.state, "db_pool")
        # boot_state must be one of the documented values; an unknown
        # string would mean the bootstrap returned a state the rest
        # of the codebase has no branch for.
        assert app.state.boot_state in {
            "clean_install",
            "normal",
            "recovery_required",
        }
        # /health is the cheapest end-to-end probe that the routing
        # layer is also wired up -- if it 500s, lifespan came up but
        # the app object itself is broken.
        resp = client.get("/health")
        assert resp.status_code == 200, resp.text
