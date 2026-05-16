"""#1181 — Universal bootstrap-state gate carve-out tests.

Two test groups:

1. **Behavioral** — every dispatch path (scheduled fire, catch-up,
   manual-queue) BYPASSES ``check_bootstrap_state_gate`` for jobs
   flagged ``exempt_from_universal_bootstrap_gate=True``. Non-exempt
   jobs are gated unchanged. The gate helper is patched with a strict
   ``MagicMock`` and ``assert_not_called()`` is used for exempt paths
   so a bare ``return_value=(True, '')`` mock cannot hide a missing
   exemption check.

2. **Registry invariant** — the static allow-list is the audit trail
   for the carve-out. ``test_exempt_allowlist_is_explicit`` pins the
   set to ``{sec_daily_index_reconcile}``; future exempt additions
   require updating the assertion + spec + Codex review per
   ``docs/superpowers/specs/2026-05-16-lane-b-discovery-firing.md`` §4.2.

Spec: docs/superpowers/specs/2026-05-16-lane-b-discovery-firing.md.
Plan: docs/superpowers/plans/2026-05-16-lane-b-discovery-firing-plan.md.
"""

from __future__ import annotations

import threading
from datetime import UTC, datetime
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from app.jobs.listener import _dispatch_manual_job
from app.jobs.locks import JobAlreadyRunning
from app.jobs.runtime import _INVOKERS, JobRuntime
from app.workers.scheduler import (
    JOB_SEC_ATOM_FAST_LANE,
    JOB_SEC_DAILY_INDEX_RECONCILE,
    JOB_SEC_PER_CIK_POLL,
    SCHEDULED_JOBS,
    Cadence,
    ScheduledJob,
)


class _FakeLock:
    """Drop-in for ``JobLock`` that skips ``source_for`` lookup.

    Synthetic job names used in this file are not in
    ``SCHEDULED_JOBS`` (production registry), so the real
    ``JobLock._lock_key_for`` raises ``KeyError`` when looking up
    the lock bucket. Tests don't need real locking — they need
    deterministic enter/exit so the wrapped invoker runs.
    """

    _held: dict[str, threading.Lock] = {}
    _registry_lock = threading.Lock()

    def __init__(self, _database_url: str, job_name: str) -> None:
        self._job_name = job_name
        with _FakeLock._registry_lock:
            if job_name not in _FakeLock._held:
                _FakeLock._held[job_name] = threading.Lock()
        self._lock = _FakeLock._held[job_name]
        self._acquired = False

    def __enter__(self) -> _FakeLock:
        if not self._lock.acquire(blocking=False):
            raise JobAlreadyRunning(self._job_name)
        self._acquired = True
        return self

    def __exit__(self, *_args: object) -> None:
        if self._acquired:
            self._lock.release()
            self._acquired = False


@pytest.fixture(autouse=True)
def _reset_fake_locks() -> Any:
    _FakeLock._held.clear()
    yield
    _FakeLock._held.clear()


# Frozen "now" — borrowed shape from tests/test_jobs_runtime.py so
# catch-up arithmetic is deterministic.
_NOW = datetime(2026, 5, 16, 13, 0, 0, tzinfo=UTC)


def _job_by_name(name: str) -> ScheduledJob | None:
    for job in SCHEDULED_JOBS:
        if job.name == name:
            return job
    return None


def _runtime_mock() -> MagicMock:
    runtime = MagicMock()
    runtime.submit_manual_with_request = MagicMock()
    return runtime


def _fake_job(
    name: str,
    *,
    prerequisite: Any = None,
    exempt: bool = False,
    catch_up: bool = True,
) -> ScheduledJob:
    """Build a synthetic ScheduledJob for in-test patching of SCHEDULED_JOBS."""
    return ScheduledJob(
        name=name,
        description="test fixture",
        cadence=Cadence.daily(hour=4, minute=0),
        source="sec_rate",
        catch_up_on_boot=catch_up,
        prerequisite=prerequisite,
        exempt_from_universal_bootstrap_gate=exempt,
    )


# ---------------------------------------------------------------------------
# 1. Behavioral tests
# ---------------------------------------------------------------------------


class TestExemptListenerPath:
    """Manual-queue dispatch path bypasses the universal gate for exempt jobs."""

    def test_exempt_job_bypasses_listener_gate_no_override(self) -> None:
        """Exempt + no override → gate not called; dispatch fires; no rejection."""
        runtime = _runtime_mock()
        exempt_job = _fake_job("layer2_test", exempt=True)
        gate_mock = MagicMock()  # strict — assert_not_called below
        with (
            patch("app.jobs.listener.check_bootstrap_state_gate", gate_mock),
            patch("app.jobs.listener.SCHEDULED_JOBS", [exempt_job]),
            patch("app.jobs.listener.VALID_JOB_NAMES", {"layer2_test"}),
            patch("app.jobs.listener.psycopg.connect") as mock_connect,
            patch("app.jobs.listener.mark_request_rejected") as mock_reject,
            patch("app.jobs.listener.validate_job_params", return_value={}),
        ):
            mock_connect.return_value.__enter__.return_value = MagicMock()
            _dispatch_manual_job(
                runtime=runtime,
                request_id=11,
                job_name="layer2_test",
                payload={"params": {}, "control": {}},
            )
        gate_mock.assert_not_called()
        mock_reject.assert_not_called()
        runtime.submit_manual_with_request.assert_called_once()

    def test_exempt_job_bypasses_listener_gate_with_override(self) -> None:
        """Exempt + override flag → still bypasses gate; NO decision_audit row.

        The carve-out is an "unaudited design bypass" — distinct from
        the operator override path, which writes audit. For exempt
        jobs, override_present is meaningless.
        """
        runtime = _runtime_mock()
        exempt_job = _fake_job("layer2_test", exempt=True)
        gate_mock = MagicMock()
        with (
            patch("app.jobs.listener.check_bootstrap_state_gate", gate_mock),
            patch("app.jobs.listener.SCHEDULED_JOBS", [exempt_job]),
            patch("app.jobs.listener.VALID_JOB_NAMES", {"layer2_test"}),
            patch("app.jobs.listener.psycopg.connect") as mock_connect,
            patch("app.jobs.listener.mark_request_rejected") as mock_reject,
            patch("app.jobs.listener.validate_job_params", return_value={}),
        ):
            mock_connect.return_value.__enter__.return_value = MagicMock()
            _dispatch_manual_job(
                runtime=runtime,
                request_id=12,
                job_name="layer2_test",
                payload={"params": {}, "control": {"override_bootstrap_gate": True}},
            )
        gate_mock.assert_not_called()
        mock_reject.assert_not_called()
        runtime.submit_manual_with_request.assert_called_once()

    def test_non_exempt_job_with_override_still_calls_gate(self) -> None:
        """Non-exempt + override → gate called with override_present=True."""
        runtime = _runtime_mock()
        non_exempt = _fake_job("non_exempt_test", exempt=False)
        gate_mock = MagicMock(return_value=(True, ""))
        with (
            patch("app.jobs.listener.check_bootstrap_state_gate", gate_mock),
            patch("app.jobs.listener.SCHEDULED_JOBS", [non_exempt]),
            patch("app.jobs.listener.VALID_JOB_NAMES", {"non_exempt_test"}),
            patch("app.jobs.listener.psycopg.connect") as mock_connect,
            patch("app.jobs.listener.mark_request_rejected"),
            patch("app.jobs.listener.validate_job_params", return_value={}),
        ):
            mock_connect.return_value.__enter__.return_value = MagicMock()
            _dispatch_manual_job(
                runtime=runtime,
                request_id=13,
                job_name="non_exempt_test",
                payload={"params": {}, "control": {"override_bootstrap_gate": True}},
            )
        assert gate_mock.call_args.kwargs["override_present"] is True
        runtime.submit_manual_with_request.assert_called_once()

    def test_non_exempt_job_still_gated_in_partial_error(self) -> None:
        """Default-path regression guard: non-exempt + gate-deny → reject."""
        runtime = _runtime_mock()
        non_exempt = _fake_job("non_exempt_test", exempt=False)
        with (
            patch(
                "app.jobs.listener.check_bootstrap_state_gate",
                return_value=(False, "bootstrap_not_complete"),
            ),
            patch("app.jobs.listener.SCHEDULED_JOBS", [non_exempt]),
            patch("app.jobs.listener.VALID_JOB_NAMES", {"non_exempt_test"}),
            patch("app.jobs.listener.psycopg.connect") as mock_connect,
            patch("app.jobs.listener.mark_request_rejected") as mock_reject,
            patch("app.jobs.listener.validate_job_params", return_value={}),
        ):
            mock_connect.return_value.__enter__.return_value = MagicMock()
            _dispatch_manual_job(
                runtime=runtime,
                request_id=14,
                job_name="non_exempt_test",
                payload={"params": {}, "control": {}},
            )
        mock_reject.assert_called_once()
        assert mock_reject.call_args.kwargs["error_msg"] == "bootstrap_not_complete"
        runtime.submit_manual_with_request.assert_not_called()

    def test_exempt_job_with_failing_prereq_still_rejects(self) -> None:
        """SYNTHETIC test only — registry invariant test 10 forbids this
        exempt+prereq combination in the real registry.

        Documents the layering contract: gate exemption does NOT bypass
        the per-job prerequisite. If a future contract revision allows
        exempt+prereq, this test guards against accidental "exempt also
        means no prereq" drift.
        """
        runtime = _runtime_mock()
        synthetic = _fake_job(
            "synthetic_exempt_with_prereq",
            exempt=True,
            prerequisite=lambda _conn: (False, "fake prereq fails"),
        )
        with (
            patch("app.jobs.listener.check_bootstrap_state_gate"),
            patch("app.jobs.listener.SCHEDULED_JOBS", [synthetic]),
            patch("app.jobs.listener.VALID_JOB_NAMES", {"synthetic_exempt_with_prereq"}),
            patch("app.jobs.listener.psycopg.connect") as mock_connect,
            patch("app.jobs.listener.mark_request_rejected") as mock_reject,
            patch("app.jobs.listener.validate_job_params", return_value={}),
        ):
            mock_connect.return_value.__enter__.return_value = MagicMock()
            _dispatch_manual_job(
                runtime=runtime,
                request_id=15,
                job_name="synthetic_exempt_with_prereq",
                payload={"params": {}, "control": {}},
            )
        mock_reject.assert_called_once()
        assert mock_reject.call_args.kwargs["error_msg"] == "fake prereq fails"
        runtime.submit_manual_with_request.assert_not_called()


class TestExemptRuntimePath:
    """Scheduled-fire and catch-up paths bypass the universal gate for exempt jobs."""

    def test_exempt_job_bypasses_scheduled_fire_gate(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Scheduled fire path: exempt job → gate not called."""
        fired: list[str] = []

        def invoker() -> None:
            fired.append("layer2_test")

        exempt_job = _fake_job("layer2_test", exempt=True)
        monkeypatch.setattr("app.jobs.runtime.SCHEDULED_JOBS", [exempt_job])
        monkeypatch.setattr("app.jobs.runtime.materialise_scheduled_params", lambda _name: {})
        monkeypatch.setattr(
            "app.jobs.runtime.validate_job_params",
            lambda _name, params, **_kw: dict(params),
        )
        monkeypatch.setattr(
            "app.jobs.runtime.run_with_prelude",
            lambda _url, _name, fn, **_kw: fn(_kw.get("params") or {}) or True,
        )
        gate_mock = MagicMock()
        monkeypatch.setattr("app.jobs.runtime.check_bootstrap_state_gate", gate_mock)
        # Stub record_job_skip so any unexpected gate hit would write a row.
        skips: list[Any] = []
        monkeypatch.setattr(
            "app.jobs.runtime.record_job_skip",
            lambda _conn, _n, _r, **kw: skips.append((_n, _r, kw)),
        )
        # Stub psycopg.connect.
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        monkeypatch.setattr("app.jobs.runtime.psycopg.connect", lambda _url, **_kw: mock_conn)
        monkeypatch.setattr("app.jobs.runtime.JobLock", _FakeLock)

        from app.jobs.runtime import _adapt_zero_arg

        rt = JobRuntime(
            database_url="postgresql://stub/stub",
            invokers={"layer2_test": _adapt_zero_arg(invoker)},
        )
        wrapped = rt._wrap_invoker("layer2_test", rt._invokers["layer2_test"])
        wrapped()

        gate_mock.assert_not_called()
        assert fired == ["layer2_test"]
        assert skips == []

    def test_non_exempt_scheduled_fire_calls_gate(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Regression guard: non-exempt scheduled fire still consults gate."""
        fired: list[str] = []

        def invoker() -> None:
            fired.append("non_exempt_test")

        non_exempt = _fake_job("non_exempt_test", exempt=False)
        monkeypatch.setattr("app.jobs.runtime.SCHEDULED_JOBS", [non_exempt])
        monkeypatch.setattr("app.jobs.runtime.materialise_scheduled_params", lambda _name: {})
        monkeypatch.setattr(
            "app.jobs.runtime.validate_job_params",
            lambda _name, params, **_kw: dict(params),
        )
        monkeypatch.setattr(
            "app.jobs.runtime.run_with_prelude",
            lambda _url, _name, fn, **_kw: fn(_kw.get("params") or {}) or True,
        )
        gate_mock = MagicMock(return_value=(False, "bootstrap_not_complete"))
        monkeypatch.setattr("app.jobs.runtime.check_bootstrap_state_gate", gate_mock)
        skips: list[Any] = []
        monkeypatch.setattr(
            "app.jobs.runtime.record_job_skip",
            lambda _conn, _n, _r, **kw: skips.append((_n, _r, kw)),
        )
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        monkeypatch.setattr("app.jobs.runtime.psycopg.connect", lambda _url, **_kw: mock_conn)
        monkeypatch.setattr("app.jobs.runtime.JobLock", _FakeLock)

        from app.jobs.runtime import _adapt_zero_arg

        rt = JobRuntime(
            database_url="postgresql://stub/stub",
            invokers={"non_exempt_test": _adapt_zero_arg(invoker)},
        )
        wrapped = rt._wrap_invoker("non_exempt_test", rt._invokers["non_exempt_test"])
        wrapped()

        gate_mock.assert_called_once()
        assert fired == []
        assert len(skips) == 1
        assert skips[0][1] == "bootstrap_not_complete"

    def test_scheduled_fire_with_unregistered_job_still_calls_gate(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Codex 2 WARNING — fail-closed contract for the `job is None` case.

        When an invoker is in ``_INVOKERS`` but the job-name is NOT in
        ``SCHEDULED_JOBS`` (registry-drift / hand-registration scenario),
        ``_wrap_invoker`` MUST still consult the universal gate. The
        exemption is opt-in and requires a registered ``ScheduledJob``;
        the unregistered case stays fail-closed (default behavior).
        """
        fired: list[str] = []

        def invoker() -> None:
            fired.append("orphan_job")

        # Empty SCHEDULED_JOBS so ``self._job_registry.get('orphan_job')``
        # returns None inside _wrap_invoker.
        monkeypatch.setattr("app.jobs.runtime.SCHEDULED_JOBS", [])
        monkeypatch.setattr("app.jobs.runtime.materialise_scheduled_params", lambda _name: {})
        monkeypatch.setattr(
            "app.jobs.runtime.validate_job_params",
            lambda _name, params, **_kw: dict(params),
        )
        monkeypatch.setattr(
            "app.jobs.runtime.run_with_prelude",
            lambda _url, _name, fn, **_kw: fn(_kw.get("params") or {}) or True,
        )
        gate_mock = MagicMock(return_value=(False, "bootstrap_not_complete"))
        monkeypatch.setattr("app.jobs.runtime.check_bootstrap_state_gate", gate_mock)
        skips: list[Any] = []
        monkeypatch.setattr(
            "app.jobs.runtime.record_job_skip",
            lambda _conn, _n, _r, **kw: skips.append((_n, _r, kw)),
        )
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        monkeypatch.setattr("app.jobs.runtime.psycopg.connect", lambda _url, **_kw: mock_conn)
        monkeypatch.setattr("app.jobs.runtime.JobLock", _FakeLock)

        from app.jobs.runtime import _adapt_zero_arg

        rt = JobRuntime(
            database_url="postgresql://stub/stub",
            invokers={"orphan_job": _adapt_zero_arg(invoker)},
        )
        wrapped = rt._wrap_invoker("orphan_job", rt._invokers["orphan_job"])
        wrapped()

        # Fail-closed: gate consulted, run rejected.
        gate_mock.assert_called_once()
        assert fired == []
        assert len(skips) == 1
        assert skips[0][1] == "bootstrap_not_complete"

    def test_exempt_job_bypasses_catchup_gate(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Catch-up path: exempt job → gate not called; job fires."""
        fired: list[str] = []

        def invoker() -> None:
            fired.append("layer2_test")

        # Layer-2 shape: daily, catch_up=True, exempt=True.
        exempt_job = _fake_job("layer2_test", exempt=True, catch_up=True)
        monkeypatch.setattr("app.jobs.runtime.SCHEDULED_JOBS", [exempt_job])
        # Make the job overdue.
        monkeypatch.setattr("app.jobs.runtime.fetch_latest_successful_runs", lambda _conn, _names: {})
        monkeypatch.setattr("app.jobs.runtime.materialise_scheduled_params", lambda _name: {})
        monkeypatch.setattr(
            "app.jobs.runtime.validate_job_params",
            lambda _name, params, **_kw: dict(params),
        )
        monkeypatch.setattr(
            "app.jobs.runtime.run_with_prelude",
            lambda _url, _name, fn, **_kw: fn(_kw.get("params") or {}) or True,
        )
        gate_mock = MagicMock()
        monkeypatch.setattr("app.jobs.runtime.check_bootstrap_state_gate", gate_mock)
        skips: list[Any] = []
        monkeypatch.setattr(
            "app.jobs.runtime.record_job_skip",
            lambda _conn, _n, _r, **kw: skips.append((_n, _r, kw)),
        )
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        monkeypatch.setattr("app.jobs.runtime.psycopg.connect", lambda _url, **_kw: mock_conn)
        monkeypatch.setattr(
            "app.jobs.runtime.datetime",
            type("_FakeDT", (), {"now": staticmethod(lambda tz: _NOW)}),
        )
        monkeypatch.setattr("app.jobs.runtime.JobLock", _FakeLock)

        from app.jobs.runtime import _adapt_zero_arg

        rt = JobRuntime(
            database_url="postgresql://stub/stub",
            invokers={"layer2_test": _adapt_zero_arg(invoker)},
        )
        rt._catch_up()
        # Wait for executor to drain the submitted future.
        rt._manual_executor.shutdown(wait=True)

        gate_mock.assert_not_called()
        assert skips == []
        assert fired == ["layer2_test"]


# ---------------------------------------------------------------------------
# 2. Registry-invariant tests
# ---------------------------------------------------------------------------


class TestExemptRegistryInvariants:
    """Static allow-list + structural assertions enforce §4.2 eligibility."""

    def _exempt_jobs(self) -> list[ScheduledJob]:
        return [j for j in SCHEDULED_JOBS if j.exempt_from_universal_bootstrap_gate]

    def test_exempt_allowlist_is_explicit(self) -> None:
        """Adding a new exempt job requires updating this assertion.

        See spec §4.2 — the static allow-list is the audit trail for
        the carve-out. Any unilateral flag flip without spec/Codex/
        settled-decisions update fails here.
        """
        actual_names = {j.name for j in self._exempt_jobs()}
        expected_names = {JOB_SEC_DAILY_INDEX_RECONCILE}
        assert actual_names == expected_names, (
            f"Exempt allow-list drifted. Expected exactly "
            f"{expected_names}, got {actual_names}. Adding a new exempt "
            "job requires a new spec entry + Codex review + update to "
            "this assertion. See spec §4.2."
        )

    def test_exempt_implies_catch_up_on_boot_true(self) -> None:
        """Carve-out exists for the catch_up_on_boot evaluation trap."""
        for job in self._exempt_jobs():
            assert job.catch_up_on_boot is True, (
                f"Exempt job {job.name!r} has catch_up_on_boot=False — "
                "incoherent with the carve-out (spec §4.2 contract #1)."
            )

    def test_exempt_implies_prerequisite_none(self) -> None:
        """Carve-out rests on body-safe-against-empty-DB."""
        for job in self._exempt_jobs():
            assert job.prerequisite is None, (
                f"Exempt job {job.name!r} has a per-job prerequisite — spec §4.2 contract #2 forbids this combination."
            )


# ---------------------------------------------------------------------------
# 3. Per-layer wiring assertions (extends test_layer_123_wiring.py)
# ---------------------------------------------------------------------------


class TestLayer123ExemptionWiring:
    """Layer 1/2/3 exemption flags are pinned to spec intent."""

    def test_layer1_atom_fast_lane_not_exempt(self) -> None:
        """Layer 1's _bootstrap_complete prereq IS the right gate."""
        job = _job_by_name(JOB_SEC_ATOM_FAST_LANE)
        assert job is not None
        assert job.exempt_from_universal_bootstrap_gate is False

    def test_layer2_daily_index_reconcile_exempt(self) -> None:
        """Layer 2 carve-out — spec §4.2 sole member."""
        job = _job_by_name(JOB_SEC_DAILY_INDEX_RECONCILE)
        assert job is not None
        assert job.exempt_from_universal_bootstrap_gate is True

    def test_layer3_per_cik_poll_not_exempt(self) -> None:
        """Layer 3's _bootstrap_complete prereq IS the right gate."""
        job = _job_by_name(JOB_SEC_PER_CIK_POLL)
        assert job is not None
        assert job.exempt_from_universal_bootstrap_gate is False


# Ensure the test module imports the runtime invokers registry without
# warnings if pytest collects it but doesn't run it.
_ = _INVOKERS
