"""Tests for the PR1b manual-queue prerequisite check.

Pre-PR1b: scheduled fires honoured ScheduledJob.prerequisite (e.g.
_bootstrap_complete on every SEC + fundamentals job); manual triggers
via the durable queue bypassed the prerequisite entirely. PR1b extends
the per-job prerequisite enforcement to the manual-queue dispatch path
in app/jobs/listener.py::_dispatch_manual_job.

Bootstrap-internal jobs (bootstrap_orchestrator + its stage jobs) are
NOT in SCHEDULED_JOBS, so the lookup returns no prerequisite and the
manual-queue path proceeds unchanged. This test pins both behaviours.

Codex pre-push BLOCKING #1 + #2 are addressed by this design — the gate
fires per-job from the existing prerequisite registry, not as a global
bootstrap_state check, so:
  - bootstrap_orchestrator (no prereq) is never gated, even with
    bootstrap_state.status='running'.
  - jobs without _bootstrap_complete prereq (orchestrator_high_frequency_sync,
    execute_approved_orders, monitor_positions, ...) keep pre-PR1b
    semantics.

PR1b-2 (#1064) layered the universal bootstrap_state gate ON TOP of
the per-job prereq. Tests in this file pre-date PR1b-2 and exercise
the prereq path in isolation — the bootstrap_state gate is patched to
always allow so the original assertions still hold. PR1b-2's gate
behaviour is covered in tests/test_bootstrap_gate_universal.py.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

from app.jobs.listener import _dispatch_manual_job
from app.workers.scheduler import SCHEDULED_JOBS


def _allow_gate() -> Any:
    """Patch ``check_bootstrap_state_gate`` to always allow (True, '').

    Tests in TestPrereqEnforcement exercise the per-job prereq path,
    not the universal bootstrap_state gate added in PR1b-2. With the
    gate patched to allow, dispatch reaches the per-job prereq check
    and the original assertions hold.
    """
    return patch("app.jobs.listener.check_bootstrap_state_gate", return_value=(True, ""))


def _runtime_mock() -> MagicMock:
    """Mock JobRuntime that records submit_manual_with_request calls."""
    runtime = MagicMock()
    runtime.submit_manual_with_request = MagicMock()
    return runtime


def _job_with_prereq(name: str, met: bool, reason: str) -> Any:
    """Build a ScheduledJob-like fake whose prerequisite returns (met, reason)."""
    job = MagicMock()
    job.name = name
    job.prerequisite = MagicMock(return_value=(met, reason))
    return job


class TestPrereqEnforcement:
    """Manual-queue path runs ScheduledJob.prerequisite if declared."""

    def test_unmet_prereq_rejects_with_reason(self) -> None:
        """Job with prereq returning (False, reason) → mark_request_rejected."""
        runtime = _runtime_mock()
        fake_job = _job_with_prereq(
            "sec_form3_ingest", met=False, reason="first-install bootstrap not complete; visit /admin to run"
        )
        with (
            _allow_gate(),
            patch("app.jobs.listener.SCHEDULED_JOBS", [fake_job]),
            patch("app.jobs.listener.VALID_JOB_NAMES", {"sec_form3_ingest"}),
            patch("app.jobs.listener.psycopg.connect") as mock_connect,
            patch("app.jobs.listener.mark_request_rejected") as mock_reject,
        ):
            mock_connect.return_value.__enter__.return_value = MagicMock()
            _dispatch_manual_job(
                runtime=runtime,
                request_id=42,
                job_name="sec_form3_ingest",
            )
        # Should have rejected with the prereq's reason string.
        mock_reject.assert_called_once()
        kwargs = mock_reject.call_args.kwargs
        assert kwargs["error_msg"] == "first-install bootstrap not complete; visit /admin to run"
        # Must NOT have submitted to the runtime executor.
        runtime.submit_manual_with_request.assert_not_called()

    def test_met_prereq_proceeds_to_runtime(self) -> None:
        """Job with prereq returning (True, '') → runtime.submit_manual_with_request fires."""
        runtime = _runtime_mock()
        fake_job = _job_with_prereq("sec_form3_ingest", met=True, reason="")
        with (
            _allow_gate(),
            patch("app.jobs.listener.SCHEDULED_JOBS", [fake_job]),
            patch("app.jobs.listener.VALID_JOB_NAMES", {"sec_form3_ingest"}),
            patch("app.jobs.listener.psycopg.connect") as mock_connect,
            patch("app.jobs.listener.mark_request_rejected") as mock_reject,
        ):
            mock_connect.return_value.__enter__.return_value = MagicMock()
            _dispatch_manual_job(
                runtime=runtime,
                request_id=42,
                job_name="sec_form3_ingest",
            )
        mock_reject.assert_not_called()
        runtime.submit_manual_with_request.assert_called_once_with(
            "sec_form3_ingest", request_id=42, mode=None, params={}
        )

    def test_no_prereq_proceeds_to_runtime(self) -> None:
        """Job with no prereq declared → runtime.submit_manual_with_request fires.

        Critical for bootstrap-internal jobs (bootstrap_orchestrator + stage
        jobs) which are NOT in SCHEDULED_JOBS and therefore have no prereq
        lookup — must not be gated by PR1b's manual-queue extension.
        """
        runtime = _runtime_mock()
        # Empty SCHEDULED_JOBS — bootstrap_orchestrator pattern.
        # Gate is bypassed by job_in_registry=False; no _allow_gate() needed.
        with (
            patch("app.jobs.listener.SCHEDULED_JOBS", []),
            patch("app.jobs.listener.VALID_JOB_NAMES", {"bootstrap_orchestrator"}),
            patch("app.jobs.listener.psycopg.connect") as mock_connect,
            patch("app.jobs.listener.mark_request_rejected") as mock_reject,
        ):
            mock_connect.return_value.__enter__.return_value = MagicMock()
            _dispatch_manual_job(
                runtime=runtime,
                request_id=42,
                job_name="bootstrap_orchestrator",
            )
        mock_reject.assert_not_called()
        runtime.submit_manual_with_request.assert_called_once()

    def test_prereq_check_failure_fails_open(self) -> None:
        """If prereq function itself raises, the job runs anyway (fail-open)."""
        runtime = _runtime_mock()
        bad_job = MagicMock()
        bad_job.name = "fundamentals_sync"
        bad_job.prerequisite = MagicMock(side_effect=RuntimeError("DB unavailable"))
        with (
            _allow_gate(),
            patch("app.jobs.listener.SCHEDULED_JOBS", [bad_job]),
            patch("app.jobs.listener.VALID_JOB_NAMES", {"fundamentals_sync"}),
            patch("app.jobs.listener.psycopg.connect") as mock_connect,
            patch("app.jobs.listener.mark_request_rejected") as mock_reject,
        ):
            mock_connect.return_value.__enter__.return_value = MagicMock()
            _dispatch_manual_job(
                runtime=runtime,
                request_id=42,
                job_name="fundamentals_sync",
            )
        mock_reject.assert_not_called()
        runtime.submit_manual_with_request.assert_called_once()

    def test_unmet_prereq_with_failing_rejection_write_does_not_dispatch(self) -> None:
        """Review-bot PR1b BLOCKING regression — if mark_request_rejected
        raises (transient write failure), the job MUST NOT fall through
        to submit_manual_with_request. A prerequisite that explicitly
        returned (False, reason) MUST NOT dispatch even if the rejection
        write fails."""
        runtime = _runtime_mock()
        fake_job = _job_with_prereq("sec_form3_ingest", met=False, reason="bootstrap not complete")
        with (
            _allow_gate(),
            patch("app.jobs.listener.SCHEDULED_JOBS", [fake_job]),
            patch("app.jobs.listener.VALID_JOB_NAMES", {"sec_form3_ingest"}),
            patch("app.jobs.listener.psycopg.connect") as mock_connect,
            patch(
                "app.jobs.listener.mark_request_rejected",
                side_effect=RuntimeError("write failed"),
            ),
        ):
            mock_connect.return_value.__enter__.return_value = MagicMock()
            try:
                _dispatch_manual_job(
                    runtime=runtime,
                    request_id=42,
                    job_name="sec_form3_ingest",
                )
            except RuntimeError:
                # Rejection write failure escapes — _route_claim's broad
                # except will mark the request rejected. That's correct.
                pass
        # The critical invariant: dispatch MUST NOT have fired.
        runtime.submit_manual_with_request.assert_not_called()

    def test_connect_failure_fails_open(self) -> None:
        """Codex round-2 BLOCKING regression — psycopg.connect raising MUST
        fail-open (not escape to _route_claim, which would silently
        mark the queue row rejected — divergence from scheduled-fire posture)."""
        runtime = _runtime_mock()
        fake_job = _job_with_prereq("sec_form3_ingest", met=False, reason="should not be reached")
        with (
            _allow_gate(),
            patch("app.jobs.listener.SCHEDULED_JOBS", [fake_job]),
            patch("app.jobs.listener.VALID_JOB_NAMES", {"sec_form3_ingest"}),
            patch("app.jobs.listener.psycopg.connect", side_effect=RuntimeError("connect failed")),
            patch("app.jobs.listener.mark_request_rejected") as mock_reject,
        ):
            _dispatch_manual_job(
                runtime=runtime,
                request_id=42,
                job_name="sec_form3_ingest",
            )
        mock_reject.assert_not_called()
        runtime.submit_manual_with_request.assert_called_once()


class TestUnknownJobName:
    """Pre-existing behaviour: unknown job_name → mark_request_rejected, no dispatch."""

    def test_unknown_rejects(self) -> None:
        runtime = _runtime_mock()
        with (
            patch("app.jobs.listener.psycopg.connect") as mock_connect,
            patch("app.jobs.listener.mark_request_rejected") as mock_reject,
        ):
            mock_connect.return_value.__enter__.return_value = MagicMock()
            _dispatch_manual_job(
                runtime=runtime,
                request_id=42,
                job_name="completely_made_up_xyz",
            )
        mock_reject.assert_called_once()
        kwargs = mock_reject.call_args.kwargs
        assert "unknown job name" in kwargs["error_msg"]
        runtime.submit_manual_with_request.assert_not_called()


class TestRealRegistryCoverage:
    """Coverage: real SCHEDULED_JOBS registry has _bootstrap_complete prereq
    on the SEC + fundamentals jobs (the ones that would have hit the gate
    in the original PR1b design). Spot-check that those jobs would gate."""

    def test_sec_atom_fast_lane_has_bootstrap_complete_prereq(self) -> None:
        """Pre-#1162: this test pinned ``sec_form3_ingest``. That cron
        was retired 2026-05-14 (PR #1162, commit baa3856) — moved to
        on-demand only. Sub in Layer 1 (``sec_atom_fast_lane``) as the
        representative SEC scheduled job with ``_bootstrap_complete``
        prereq.
        """
        from app.workers.scheduler import _bootstrap_complete

        job = next(j for j in SCHEDULED_JOBS if j.name == "sec_atom_fast_lane")
        assert job.prerequisite is _bootstrap_complete

    def test_fundamentals_sync_has_bootstrap_complete_in_compose(self) -> None:
        """fundamentals_sync uses _all_of(_bootstrap_complete, _has_any_coverage).
        The composed prereq fires the bootstrap check first; manual-queue path
        gets the same rejection."""
        job = next(j for j in SCHEDULED_JOBS if j.name == "fundamentals_sync")
        assert job.prerequisite is not None  # composed; identity check infeasible

    def test_orchestrator_high_frequency_sync_has_no_prereq(self) -> None:
        """Pre-PR1b non-gated job stays non-gated in PR1b. Critical for the
        BLOCKING #2 Codex caught — global gate would have broken this."""
        job = next(j for j in SCHEDULED_JOBS if j.name == "orchestrator_high_frequency_sync")
        assert job.prerequisite is None

    def test_execute_approved_orders_has_no_bootstrap_prereq(self) -> None:
        """execute_approved_orders has _has_actionable_recommendations prereq,
        NOT _bootstrap_complete. Pre-PR1b semantics preserved."""
        from app.workers.scheduler import _bootstrap_complete, _has_actionable_recommendations

        job = next(j for j in SCHEDULED_JOBS if j.name == "execute_approved_orders")
        assert job.prerequisite is _has_actionable_recommendations
        assert job.prerequisite is not _bootstrap_complete
