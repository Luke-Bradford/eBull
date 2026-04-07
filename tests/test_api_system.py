"""Tests for app.api.system — operator visibility endpoints (issue #57).

The DB connection is dependency-overridden to a MagicMock; service-layer
calls are patched at the route module's import boundary so we exercise the
HTTP shape, the overall_status derivation, and the next-run computation
without spinning up Postgres.

conftest.py installs a no-op override on
``require_session_or_service_token`` globally; real auth is exercised in
test_api_auth_session.py. The auth-on-route smoke test below clears that
override per-test using the same capture-and-restore pattern as
test_api_auth_session (prevention-log #81).
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient

from app.api.auth import require_session_or_service_token
from app.db import get_conn
from app.main import app
from app.services.ops_monitor import (
    LAYER_QUERY_FAILED_DETAIL_TEMPLATE,
    JobHealth,
    LayerHealth,
)
from app.workers.scheduler import SCHEDULED_JOBS

_NOW = datetime(2026, 4, 7, 12, 30, 0, tzinfo=UTC)


def _mock_conn() -> MagicMock:
    return MagicMock()


def _override_conn(conn: MagicMock) -> None:
    def _gen() -> Iterator[MagicMock]:
        yield conn

    app.dependency_overrides[get_conn] = _gen


def _clear_conn_override() -> None:
    app.dependency_overrides.pop(get_conn, None)


def _ok_layer(name: str) -> LayerHealth:
    return LayerHealth(
        layer=name,  # type: ignore[arg-type]  # tests use the literal set
        status="ok",
        latest=_NOW - timedelta(hours=1),
        max_age=timedelta(hours=4),
        age=timedelta(hours=1),
        detail="",
    )


def _stale_layer(name: str) -> LayerHealth:
    return LayerHealth(
        layer=name,  # type: ignore[arg-type]
        status="stale",
        latest=_NOW - timedelta(days=10),
        max_age=timedelta(days=2),
        age=timedelta(days=10),
        detail=f"{name}: stale",
    )


def _error_layer(name: str) -> LayerHealth:
    # Mirror the production format string from check_all_layers via the
    # shared constant so the fixture cannot drift from real error rows
    # (#86 round 3 review).
    return LayerHealth(
        layer=name,  # type: ignore[arg-type]
        status="error",
        detail=LAYER_QUERY_FAILED_DETAIL_TEMPLATE.format(layer=name),
    )


def _success_job_health(name: str) -> JobHealth:
    return JobHealth(
        job_name=name,
        last_status="success",
        last_started_at=_NOW - timedelta(minutes=30),
        last_finished_at=_NOW - timedelta(minutes=29),
        detail="",
    )


def _failed_job_health(name: str) -> JobHealth:
    return JobHealth(
        job_name=name,
        last_status="failure",
        last_started_at=_NOW - timedelta(minutes=30),
        last_finished_at=_NOW - timedelta(minutes=29),
        detail=f"{name}: last run failed — DB down",
    )


def _all_ok_layers() -> list[LayerHealth]:
    return [
        _ok_layer("universe"),
        _ok_layer("prices"),
        _ok_layer("quotes"),
        _ok_layer("fundamentals"),
        _ok_layer("filings"),
        _ok_layer("news"),
        _ok_layer("theses"),
        _ok_layer("scores"),
    ]


client = TestClient(app)


# ---------------------------------------------------------------------------
# /system/status
# ---------------------------------------------------------------------------


class TestSystemStatus:
    def teardown_method(self) -> None:
        _clear_conn_override()

    def test_all_green_returns_ok(self) -> None:
        _override_conn(_mock_conn())

        with (
            patch("app.api.system.check_all_layers", return_value=_all_ok_layers()),
            patch(
                "app.api.system.check_job_health",
                side_effect=lambda _conn, name: _success_job_health(name),
            ),
            patch(
                "app.api.system.get_kill_switch_status",
                return_value={
                    "is_active": False,
                    "activated_at": None,
                    "activated_by": None,
                    "reason": None,
                },
            ),
        ):
            resp = client.get("/system/status")

        assert resp.status_code == 200
        body = resp.json()
        assert body["overall_status"] == "ok"
        assert len(body["layers"]) == 8
        assert {layer["status"] for layer in body["layers"]} == {"ok"}
        assert len(body["jobs"]) == len(SCHEDULED_JOBS)
        assert body["kill_switch"]["active"] is False
        assert "checked_at" in body

    def test_stale_layer_degrades_overall(self) -> None:
        _override_conn(_mock_conn())

        layers = _all_ok_layers()
        layers[1] = _stale_layer("prices")

        with (
            patch("app.api.system.check_all_layers", return_value=layers),
            patch(
                "app.api.system.check_job_health",
                side_effect=lambda _conn, name: _success_job_health(name),
            ),
            patch(
                "app.api.system.get_kill_switch_status",
                return_value={
                    "is_active": False,
                    "activated_at": None,
                    "activated_by": None,
                    "reason": None,
                },
            ),
        ):
            resp = client.get("/system/status")

        assert resp.status_code == 200
        body = resp.json()
        assert body["overall_status"] == "degraded"
        prices = next(layer for layer in body["layers"] if layer["layer"] == "prices")
        assert prices["status"] == "stale"

    def test_layer_error_marks_overall_down(self) -> None:
        _override_conn(_mock_conn())

        layers = _all_ok_layers()
        layers[0] = _error_layer("universe")

        with (
            patch("app.api.system.check_all_layers", return_value=layers),
            patch(
                "app.api.system.check_job_health",
                side_effect=lambda _conn, name: _success_job_health(name),
            ),
            patch(
                "app.api.system.get_kill_switch_status",
                return_value={
                    "is_active": False,
                    "activated_at": None,
                    "activated_by": None,
                    "reason": None,
                },
            ),
        ):
            resp = client.get("/system/status")

        assert resp.status_code == 200
        body = resp.json()
        assert body["overall_status"] == "down"

    def test_failed_job_marks_overall_down(self) -> None:
        _override_conn(_mock_conn())

        first_job_name = SCHEDULED_JOBS[0].name

        def _job_health(_conn: object, name: str) -> JobHealth:
            if name == first_job_name:
                return _failed_job_health(name)
            return _success_job_health(name)

        with (
            patch("app.api.system.check_all_layers", return_value=_all_ok_layers()),
            patch("app.api.system.check_job_health", side_effect=_job_health),
            patch(
                "app.api.system.get_kill_switch_status",
                return_value={
                    "is_active": False,
                    "activated_at": None,
                    "activated_by": None,
                    "reason": None,
                },
            ),
        ):
            resp = client.get("/system/status")

        assert resp.status_code == 200
        body = resp.json()
        assert body["overall_status"] == "down"
        failed = next(job for job in body["jobs"] if job["name"] == first_job_name)
        assert failed["last_status"] == "failure"
        assert "DB down" in failed["detail"]

    def test_active_kill_switch_marks_overall_down_even_when_clean(self) -> None:
        _override_conn(_mock_conn())
        with (
            patch("app.api.system.check_all_layers", return_value=_all_ok_layers()),
            patch(
                "app.api.system.check_job_health",
                side_effect=lambda _conn, name: _success_job_health(name),
            ),
            patch(
                "app.api.system.get_kill_switch_status",
                return_value={
                    "is_active": True,
                    "activated_at": _NOW,
                    "activated_by": "op",
                    "reason": "halted",
                },
            ),
        ):
            resp = client.get("/system/status")

        assert resp.status_code == 200
        body = resp.json()
        assert body["overall_status"] == "down"
        assert body["kill_switch"]["active"] is True
        assert body["kill_switch"]["reason"] == "halted"

    def test_service_exception_returns_503_without_leaking_internals(self) -> None:
        # Detail must be a fixed string — no part of the underlying exception
        # message (which could carry DB schema, table names, or driver text)
        # may appear in the HTTP response. Full detail goes to logger.exception.
        _override_conn(_mock_conn())
        secret_marker = "secret-table-name-do-not-leak"
        with patch(
            "app.api.system.check_all_layers",
            side_effect=RuntimeError(secret_marker),
        ):
            resp = client.get("/system/status")
        assert resp.status_code == 503
        assert resp.json()["detail"] == "system status unavailable"
        assert secret_marker not in resp.text

    def test_fresh_deploy_no_job_runs_does_not_degrade_on_jobs_alone(self) -> None:
        # A freshly deployed system has no job_runs rows. The previous rule
        # treated `last_status is None` as degraded, which made every fresh
        # deploy report "degraded" purely on job state. The new rule excludes
        # None from the job degraded signal — empty data layers are the
        # meaningful fresh-deploy signal instead.
        _override_conn(_mock_conn())

        empty_jh = JobHealth(job_name="x", detail="x: no runs recorded")

        with (
            patch("app.api.system.check_all_layers", return_value=_all_ok_layers()),
            patch("app.api.system.check_job_health", return_value=empty_jh),
            patch(
                "app.api.system.get_kill_switch_status",
                return_value={
                    "is_active": False,
                    "activated_at": None,
                    "activated_by": None,
                    "reason": None,
                },
            ),
        ):
            resp = client.get("/system/status")

        assert resp.status_code == 200
        body = resp.json()
        # All layers ok + every job has no recorded runs => overall ok.
        assert body["overall_status"] == "ok"
        for job in body["jobs"]:
            assert job["last_status"] is None
            assert "no runs recorded" in job["detail"]

    def test_running_job_degrades_overall(self) -> None:
        # The "running" status remains a degraded signal even when everything
        # else is clean — it tells the operator a long-lived job is in flight
        # so health is not yet confirmed.
        _override_conn(_mock_conn())

        running = JobHealth(
            job_name="x",
            last_status="running",
            last_started_at=_NOW - timedelta(minutes=5),
            last_finished_at=None,
            detail="x: run still in progress",
        )

        with (
            patch("app.api.system.check_all_layers", return_value=_all_ok_layers()),
            patch("app.api.system.check_job_health", return_value=running),
            patch(
                "app.api.system.get_kill_switch_status",
                return_value={
                    "is_active": False,
                    "activated_at": None,
                    "activated_by": None,
                    "reason": None,
                },
            ),
        ):
            resp = client.get("/system/status")

        assert resp.status_code == 200
        assert resp.json()["overall_status"] == "degraded"


# ---------------------------------------------------------------------------
# /system/jobs
# ---------------------------------------------------------------------------


class TestSystemJobs:
    def teardown_method(self) -> None:
        _clear_conn_override()

    def test_returns_one_entry_per_registered_job(self) -> None:
        _override_conn(_mock_conn())
        with patch(
            "app.api.system.check_job_health",
            side_effect=lambda _conn, name: _success_job_health(name),
        ):
            resp = client.get("/system/jobs")

        assert resp.status_code == 200
        body = resp.json()
        assert len(body["jobs"]) == len(SCHEDULED_JOBS)
        names = [job["name"] for job in body["jobs"]]
        assert names == [job.name for job in SCHEDULED_JOBS]

        # Each entry carries the declared cadence + computed next_run_time.
        for job in body["jobs"]:
            assert job["cadence"]
            assert job["cadence_kind"] in ("hourly", "daily", "weekly")
            assert job["next_run_time"]
            assert job["next_run_time_source"] == "declared"
            assert job["description"]

    def test_no_runs_returns_null_last_status(self) -> None:
        _override_conn(_mock_conn())

        empty_health = JobHealth(job_name="x", detail="x: no runs recorded")

        with patch(
            "app.api.system.check_job_health",
            return_value=empty_health,
        ):
            resp = client.get("/system/jobs")

        assert resp.status_code == 200
        body = resp.json()
        for job in body["jobs"]:
            assert job["last_status"] is None
            assert job["last_started_at"] is None
            assert "no runs recorded" in job["detail"]

    def test_next_run_time_strictly_after_checked_at(self) -> None:
        # Pin _utcnow so the handler's `checked_at` and the cadence
        # computation use the exact same instant. This avoids a flake at
        # cadence boundaries (e.g. an hourly job firing exactly at the
        # second the test reads the clock) which could otherwise produce
        # next_run_time == checked_at.
        _override_conn(_mock_conn())
        with (
            patch("app.api.system._utcnow", return_value=_NOW),
            patch(
                "app.api.system.check_job_health",
                side_effect=lambda _conn, name: _success_job_health(name),
            ),
        ):
            resp = client.get("/system/jobs")

        assert resp.status_code == 200
        body = resp.json()
        checked_at = datetime.fromisoformat(body["checked_at"])
        assert checked_at == _NOW
        for job in body["jobs"]:
            next_run = datetime.fromisoformat(job["next_run_time"])
            assert next_run > checked_at, f"{job['name']} next_run not in future"

    def test_service_exception_returns_503_without_leaking_internals(self) -> None:
        _override_conn(_mock_conn())
        secret_marker = "secret-table-name-do-not-leak"
        with patch(
            "app.api.system.check_job_health",
            side_effect=RuntimeError(secret_marker),
        ):
            resp = client.get("/system/jobs")
        assert resp.status_code == 503
        assert resp.json()["detail"] == "job overview unavailable"
        assert secret_marker not in resp.text


# ---------------------------------------------------------------------------
# Auth wiring smoke test
# ---------------------------------------------------------------------------


class TestSystemAuthWiring:
    """Confirm the router enforces auth when the global no-op override is removed.

    Capture/restore pattern from prevention-log #81: capture the prior value
    in setup, restore from the captured reference in teardown — never re-fetch.
    """

    def setup_method(self) -> None:
        self._prior = app.dependency_overrides.get(require_session_or_service_token)
        app.dependency_overrides.pop(require_session_or_service_token, None)
        # The combined dep takes ``conn`` via Depends(get_conn); FastAPI
        # evaluates it before reaching the function body, so we must
        # provide a mock conn even though the no-cookie/no-bearer path
        # raises 401 before touching the DB.
        _override_conn(_mock_conn())

    def teardown_method(self) -> None:
        if self._prior is not None:
            app.dependency_overrides[require_session_or_service_token] = self._prior
        else:
            app.dependency_overrides.pop(require_session_or_service_token, None)
        _clear_conn_override()

    def test_status_requires_auth(self) -> None:
        # No bearer token + no session cookie + real combined dep => 401.
        # We patch settings.service_token to None so the fail-closed branch
        # runs without depending on env config.
        with patch("app.api.auth.settings") as mock_settings:
            mock_settings.service_token = None
            mock_settings.session_cookie_name = "ebull_session"
            mock_settings.session_idle_timeout_minutes = 60
            resp = client.get("/system/status")
        assert resp.status_code == 401

    def test_jobs_requires_auth(self) -> None:
        with patch("app.api.auth.settings") as mock_settings:
            mock_settings.service_token = None
            mock_settings.session_cookie_name = "ebull_session"
            mock_settings.session_idle_timeout_minutes = 60
            resp = client.get("/system/jobs")
        assert resp.status_code == 401
