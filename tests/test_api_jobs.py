"""API tests for ``POST /jobs/{job_name}/run`` (rewritten for #719).

Pre-#719: the endpoint called ``runtime.trigger(job_name)`` against
an in-process JobRuntime. Tests stubbed that runtime on
``app.state.job_runtime``.

Post-#719: the endpoint validates the job name against
``VALID_JOB_NAMES`` and publishes a row to ``pending_job_requests``
via ``publish_manual_job_request``. The runtime no longer lives in
the API process. These tests patch the publisher and assert the
202 path returns the request_id and the 404 path is taken before
any publish.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import psycopg
from fastapi.testclient import TestClient

from app.db import get_conn
from app.main import app

client = TestClient(app)


class TestRunJob:
    def test_accepted_returns_202_and_publishes(self) -> None:
        with patch("app.api.jobs.publish_manual_job_request", return_value=42) as pub:
            resp = client.post("/jobs/nightly_universe_sync/run")
        assert resp.status_code == 202
        assert resp.json() == {"request_id": 42}
        assert pub.call_count == 1
        # The publish call should carry the validated job name.
        args, kwargs = pub.call_args
        assert args[0] == "nightly_universe_sync"

    def test_unknown_job_returns_404_without_publishing(self) -> None:
        with patch("app.api.jobs.publish_manual_job_request") as pub:
            resp = client.post("/jobs/not_a_real_job/run")
        assert resp.status_code == 404
        assert "not_a_real_job" in resp.json()["detail"]
        assert pub.call_count == 0


class TestRunJobEnvelope:
    """PR1b-2 (#1064) — envelope + control + override on POST /jobs/<name>/run."""

    def test_canonical_envelope_publishes_canonical_payload(self) -> None:
        with patch("app.api.jobs.publish_manual_job_request", return_value=11) as pub:
            resp = client.post(
                "/jobs/nightly_universe_sync/run",
                json={"params": {}, "control": {}},
            )
        assert resp.status_code == 202
        kwargs = pub.call_args.kwargs
        assert kwargs["payload"] == {"params": {}, "control": {}}

    def test_legacy_flat_dict_normalised_to_params(self) -> None:
        """Pre-PR1b-2 ergonomic shape — entire body becomes params."""
        # nightly_universe_sync has no declared ParamMetadata, so the only
        # legitimate flat-dict body is empty. Use an empty body to verify
        # the legacy → envelope normalisation produces ``{params: {}}``.
        with patch("app.api.jobs.publish_manual_job_request", return_value=12) as pub:
            resp = client.post("/jobs/nightly_universe_sync/run", json={})
        assert resp.status_code == 202
        kwargs = pub.call_args.kwargs
        assert kwargs["payload"] == {"params": {}, "control": {}}

    def test_no_body_treated_as_empty_params(self) -> None:
        with patch("app.api.jobs.publish_manual_job_request", return_value=13) as pub:
            resp = client.post("/jobs/nightly_universe_sync/run")
        assert resp.status_code == 202
        kwargs = pub.call_args.kwargs
        assert kwargs["payload"] == {"params": {}, "control": {}}

    def test_unknown_control_key_400(self) -> None:
        with patch("app.api.jobs.publish_manual_job_request") as pub:
            resp = client.post(
                "/jobs/nightly_universe_sync/run",
                json={"params": {}, "control": {"force_kill": True}},
            )
        assert resp.status_code == 400
        assert "force_kill" in resp.json()["detail"]
        pub.assert_not_called()

    def test_invalid_param_400(self) -> None:
        """ParamValidationError → 400 (not 500)."""
        with patch("app.api.jobs.publish_manual_job_request") as pub:
            resp = client.post(
                "/jobs/nightly_universe_sync/run",
                json={"params": {"unknown_field": "x"}},
            )
        assert resp.status_code == 400
        assert "unknown" in resp.json()["detail"].lower()
        pub.assert_not_called()

    def test_query_param_override_propagates_to_payload(self) -> None:
        with patch("app.api.jobs.publish_manual_job_request", return_value=14) as pub:
            resp = client.post(
                "/jobs/nightly_universe_sync/run?override_bootstrap_gate=true",
                json={"params": {}},
            )
        assert resp.status_code == 202
        kwargs = pub.call_args.kwargs
        assert kwargs["payload"]["control"]["override_bootstrap_gate"] is True

    def test_body_override_propagates_to_payload(self) -> None:
        with patch("app.api.jobs.publish_manual_job_request", return_value=15) as pub:
            resp = client.post(
                "/jobs/nightly_universe_sync/run",
                json={"params": {}, "control": {"override_bootstrap_gate": True}},
            )
        assert resp.status_code == 202
        kwargs = pub.call_args.kwargs
        assert kwargs["payload"]["control"]["override_bootstrap_gate"] is True

    def test_non_dict_body_400(self) -> None:
        with patch("app.api.jobs.publish_manual_job_request") as pub:
            resp = client.post("/jobs/nightly_universe_sync/run", json=[1, 2, 3])
        assert resp.status_code == 400
        pub.assert_not_called()

    def test_envelope_params_non_dict_400(self) -> None:
        with patch("app.api.jobs.publish_manual_job_request") as pub:
            resp = client.post(
                "/jobs/nightly_universe_sync/run",
                json={"params": "not an object", "control": {}},
            )
        assert resp.status_code == 400
        pub.assert_not_called()

    def test_override_must_be_strict_bool_400(self) -> None:
        """Codex pre-push round 2 BLOCKING — truthy strings cannot grant override."""
        with patch("app.api.jobs.publish_manual_job_request") as pub:
            resp = client.post(
                "/jobs/nightly_universe_sync/run",
                json={"params": {}, "control": {"override_bootstrap_gate": "true"}},
            )
        assert resp.status_code == 400
        assert "boolean" in resp.json()["detail"].lower()
        pub.assert_not_called()


class TestListJobRequests:
    """Smoke for the new GET /jobs/requests endpoint (#719)."""

    def teardown_method(self) -> None:
        app.dependency_overrides.pop(get_conn, None)

    def test_returns_rows_in_response_shape(self) -> None:
        rows: list[dict[str, object]] = [
            {
                "request_id": 7,
                "request_kind": "manual_job",
                "job_name": "nightly_universe_sync",
                "payload": None,
                "requested_at": datetime(2026, 4, 30, 12, 0, 0, tzinfo=UTC),
                "requested_by": "operator:1",
                "status": "completed",
                "claimed_at": datetime(2026, 4, 30, 12, 0, 1, tzinfo=UTC),
                "error_msg": None,
            },
        ]
        conn = MagicMock()
        cur = MagicMock()
        cur.fetchall.return_value = rows
        conn.cursor.return_value.__enter__.return_value = cur
        conn.cursor.return_value.__exit__.return_value = None

        def _gen():  # type: ignore[no-untyped-def]
            yield conn

        app.dependency_overrides[get_conn] = _gen
        resp = client.get("/jobs/requests")
        assert resp.status_code == 200
        body = resp.json()
        assert body["count"] == 1
        assert body["items"][0]["request_id"] == 7
        assert body["items"][0]["request_kind"] == "manual_job"


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
        assert body["count"] == 2
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
        cur.execute.side_effect = psycopg.OperationalError("connection refused")
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
