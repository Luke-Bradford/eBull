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

import pytest
from fastapi.testclient import TestClient

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
