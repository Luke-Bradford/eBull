"""API tests for ``POST /jobs/{job_name}/run`` (issue #13, PR A).

Auth is overridden globally in ``conftest.py`` so these tests
exercise the routing + status mapping in isolation. The runtime
itself is replaced with a stub on ``app.state.job_runtime`` -- we
are not exercising APScheduler timing or the real Postgres advisory
lock here (those are covered by ``test_jobs_runtime.py`` and
``test_jobs_locks.py``).

The module-level ``TestClient(app)`` pattern matches the rest of
the API tests in this repo. Note that this construction does NOT
run the FastAPI lifespan, so ``app.state.job_runtime`` is unset
unless we set it explicitly -- which is what every test below does
(or deliberately skips, for the 503 path).
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from app.db import get_conn
from app.jobs.locks import JobAlreadyRunning
from app.jobs.runtime import UnknownJob
from app.main import app

client = TestClient(app)


class _StubRuntime:
    """Captures trigger calls and lets each test pick the outcome."""

    def __init__(self) -> None:
        self.triggered: list[str] = []
        self._raise: BaseException | None = None

    def will_raise(self, exc: BaseException) -> None:
        self._raise = exc

    def trigger(self, job_name: str) -> None:
        self.triggered.append(job_name)
        if self._raise is not None:
            raise self._raise


@pytest.fixture
def stub_runtime() -> Iterator[_StubRuntime]:
    rt = _StubRuntime()
    app.state.job_runtime = rt
    try:
        yield rt
    finally:
        app.state.job_runtime = None


class TestRunJob:
    def test_accepted_returns_202_and_calls_trigger(self, stub_runtime: _StubRuntime) -> None:
        resp = client.post("/jobs/nightly_universe_sync/run")
        assert resp.status_code == 202
        assert resp.content == b""
        assert stub_runtime.triggered == ["nightly_universe_sync"]

    def test_unknown_job_returns_404(self, stub_runtime: _StubRuntime) -> None:
        stub_runtime.will_raise(UnknownJob("not_a_real_job"))
        resp = client.post("/jobs/not_a_real_job/run")
        assert resp.status_code == 404
        assert "not_a_real_job" in resp.json()["detail"]

    def test_already_running_returns_409(self, stub_runtime: _StubRuntime) -> None:
        stub_runtime.will_raise(JobAlreadyRunning("nightly_universe_sync"))
        resp = client.post("/jobs/nightly_universe_sync/run")
        assert resp.status_code == 409
        assert "nightly_universe_sync" in resp.json()["detail"]

    def test_runtime_missing_returns_503(self) -> None:
        # Explicitly drop the runtime to simulate "lifespan never ran"
        # (which is the case for the module-level TestClient).
        app.state.job_runtime = None
        try:
            resp = client.post("/jobs/nightly_universe_sync/run")
            assert resp.status_code == 503
            assert "job runtime not started" in resp.json()["detail"]
        finally:
            app.state.job_runtime = None


def _override_conn(conn: MagicMock) -> None:
    def _gen() -> Iterator[MagicMock]:
        yield conn

    app.dependency_overrides[get_conn] = _gen


def _clear_conn_override() -> None:
    app.dependency_overrides.pop(get_conn, None)


def _make_conn(rows: list[dict[str, object]]) -> MagicMock:
    """Build a MagicMock psycopg connection that returns *rows* from a dict_row cursor."""
    conn = MagicMock()
    cur = MagicMock()
    cur.fetchall.return_value = rows
    conn.cursor.return_value.__enter__.return_value = cur
    conn.cursor.return_value.__exit__.return_value = None
    return conn


class TestListJobRuns:
    """Tests for ``GET /jobs/runs``.

    The DB connection is stubbed via ``app.dependency_overrides``; the
    SQL itself is not exercised here -- the round-trip query against
    real Postgres lives in the integration smoke path.
    """

    def teardown_method(self) -> None:
        _clear_conn_override()

    def test_returns_rows_in_response_shape(self) -> None:
        rows: list[dict[str, object]] = [
            {
                "run_id": 42,
                "job_name": "nightly_universe_sync",
                "started_at": datetime(2026, 4, 9, 2, 0, 0, tzinfo=UTC),
                "finished_at": datetime(2026, 4, 9, 2, 0, 12, tzinfo=UTC),
                "status": "success",
                "row_count": 1234,
                "error_msg": None,
            },
            {
                "run_id": 41,
                "job_name": "daily_news_refresh",
                "started_at": datetime(2026, 4, 9, 1, 0, 0, tzinfo=UTC),
                "finished_at": None,
                "status": "running",
                "row_count": None,
                "error_msg": None,
            },
        ]
        _override_conn(_make_conn(rows))

        resp = client.get("/jobs/runs")
        assert resp.status_code == 200
        body = resp.json()
        assert body["total"] == 2
        assert body["limit"] == 50
        assert body["job_name"] is None
        assert [item["run_id"] for item in body["items"]] == [42, 41]
        assert body["items"][0]["status"] == "success"
        assert body["items"][1]["finished_at"] is None

    def test_passes_filter_and_limit_to_query(self) -> None:
        conn = _make_conn([])
        _override_conn(conn)

        resp = client.get("/jobs/runs?job_name=nightly_universe_sync&limit=10")
        assert resp.status_code == 200
        body = resp.json()
        assert body["job_name"] == "nightly_universe_sync"
        assert body["limit"] == 10
        # Verify the query parameters were forwarded as bound params
        # (parameterised, not interpolated -- regression target).
        cur = conn.cursor.return_value.__enter__.return_value
        called_args = cur.execute.call_args
        params = called_args[0][1]
        assert params["job_name"] == "nightly_universe_sync"
        assert params["limit"] == 10

    def test_limit_out_of_range_returns_422(self) -> None:
        _override_conn(_make_conn([]))
        resp = client.get("/jobs/runs?limit=0")
        assert resp.status_code == 422
        resp = client.get("/jobs/runs?limit=999")
        assert resp.status_code == 422

    def test_db_failure_returns_503_with_fixed_detail(self) -> None:
        conn = MagicMock()
        cur = MagicMock()
        cur.execute.side_effect = RuntimeError("connection refused")
        conn.cursor.return_value.__enter__.return_value = cur
        conn.cursor.return_value.__exit__.return_value = None
        _override_conn(conn)

        resp = client.get("/jobs/runs")
        assert resp.status_code == 503
        # Fixed detail string -- no driver text leaked.
        assert resp.json()["detail"] == "job run history unavailable"


class TestRunJobAuth:
    """Structural check that the auth dependency is wired on the route.

    The conftest installs a global no-op override on
    ``require_session_or_service_token`` so every other test in this
    file silently 202s even if the dependency was forgotten on the
    route. The regression target is "someone removed the dependency
    from the decorator" -- we walk the FastAPI route registry and
    assert ``require_session_or_service_token`` is in the resolved
    dependency tree for ``POST /jobs/{job_name}/run``.

    Structural rather than runtime because a runtime auth check
    would also resolve ``get_conn``, which needs ``app.state.db_pool``,
    which is only set up by the lifespan. This test runs without
    lifespan -- and the structural assertion is the actual thing we
    care about anyway.
    """

    def test_route_declares_auth_dependency(self) -> None:
        from fastapi.routing import APIRoute

        from app.api.auth import require_session_or_service_token

        target_route: APIRoute | None = None
        for route in app.routes:
            if isinstance(route, APIRoute) and route.path == "/jobs/{job_name}/run":
                target_route = route
                break
        assert target_route is not None, "POST /jobs/{job_name}/run not found"

        # Walk the dependant tree (depth-first) and look for the auth
        # callable. APIRoute.dependant.dependencies contains the
        # `dependencies=[Depends(...)]` entries from the decorator.
        def _depends_on(dependant: object, target: object) -> bool:
            call = getattr(dependant, "call", None)
            if call is target:
                return True
            for sub in getattr(dependant, "dependencies", []):
                if _depends_on(sub, target):
                    return True
            return False

        assert _depends_on(target_route.dependant, require_session_or_service_token), (
            "POST /jobs/{job_name}/run is missing require_session_or_service_token"
        )
