"""Tests for app.api.system — operator visibility endpoints (issue #57).

The DB connection is dependency-overridden to a MagicMock; service-layer
calls are patched at the route module's import boundary so we exercise the
HTTP shape, the overall_status derivation, and the next-run computation
without spinning up Postgres.

conftest.py installs a no-op require_auth override globally; real auth is
exercised in test_api_auth.py. The auth-on-route smoke test below clears
that override per-test using the same capture-and-restore pattern as
test_api_auth (prevention-log #81).
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient

from app.api.auth import require_auth
from app.db import get_conn
from app.main import app
from app.services.ops_monitor import JobHealth, LayerHealth
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
    return LayerHealth(
        layer=name,  # type: ignore[arg-type]
        status="error",
        detail=f"{name}: query failed — boom",
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

    def test_service_exception_returns_503(self) -> None:
        _override_conn(_mock_conn())
        with patch(
            "app.api.system.check_all_layers",
            side_effect=RuntimeError("DB unreachable"),
        ):
            resp = client.get("/system/status")
        assert resp.status_code == 503
        assert "system status unavailable" in resp.json()["detail"]


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
        _override_conn(_mock_conn())
        with patch(
            "app.api.system.check_job_health",
            side_effect=lambda _conn, name: _success_job_health(name),
        ):
            resp = client.get("/system/jobs")

        assert resp.status_code == 200
        body = resp.json()
        checked_at = datetime.fromisoformat(body["checked_at"])
        for job in body["jobs"]:
            next_run = datetime.fromisoformat(job["next_run_time"])
            assert next_run > checked_at, f"{job['name']} next_run not in future"

    def test_service_exception_returns_503(self) -> None:
        _override_conn(_mock_conn())
        with patch(
            "app.api.system.check_job_health",
            side_effect=RuntimeError("DB unreachable"),
        ):
            resp = client.get("/system/jobs")
        assert resp.status_code == 503


# ---------------------------------------------------------------------------
# Auth wiring smoke test
# ---------------------------------------------------------------------------


class TestSystemAuthWiring:
    """Confirm the router enforces auth when the global no-op override is removed.

    Capture/restore pattern from prevention-log #81: capture the prior value
    in setup, restore from the captured reference in teardown — never re-fetch.
    """

    def setup_method(self) -> None:
        self._prior = app.dependency_overrides.get(require_auth)
        app.dependency_overrides.pop(require_auth, None)

    def teardown_method(self) -> None:
        if self._prior is not None:
            app.dependency_overrides[require_auth] = self._prior
        else:
            app.dependency_overrides.pop(require_auth, None)

    def test_status_requires_auth(self) -> None:
        # No bearer token + real auth dep => 401 from require_auth.
        # We patch settings.api_key to None so the fail-closed branch runs
        # without depending on env config.
        with patch("app.api.auth.settings") as mock_settings:
            mock_settings.api_key = None
            resp = client.get("/system/status")
        assert resp.status_code == 401

    def test_jobs_requires_auth(self) -> None:
        with patch("app.api.auth.settings") as mock_settings:
            mock_settings.api_key = None
            resp = client.get("/system/jobs")
        assert resp.status_code == 401
