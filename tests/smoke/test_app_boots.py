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

Requirements
------------
* Postgres reachable at ``settings.database_url`` (the same dev DB the
  rest of the suite already requires).
* Migrations apply cleanly. Lifespan will run them.

Keep this file fast and dependency-free: no fixtures, no mocks, no
asserts beyond "did it come up and answer /health". Anything fancier
belongs in the targeted suites.
"""

from __future__ import annotations

from fastapi.testclient import TestClient

from app.main import app


def test_app_lifespan_boots_and_health_responds() -> None:
    """Drive the real lifespan; fail loud if anything in startup breaks."""
    with TestClient(app) as client:
        resp = client.get("/health")
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["status"] == "ok"
