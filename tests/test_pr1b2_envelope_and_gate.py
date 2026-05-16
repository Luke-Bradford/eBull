"""PR1b-2 (#1064) — envelope, params_snapshot, and universal bootstrap_state gate.

Three behaviour groups:

1. **Envelope normalisation** — ``app/api/jobs.py`` accepts canonical
   ``{"params": ..., "control": ...}`` AND legacy flat-dict bodies.
   Both produce the same ``pending_job_requests.payload`` shape.
2. **Listener gate** — ``app/jobs/listener.py::_dispatch_manual_job``
   consults ``check_bootstrap_state_gate`` BEFORE the per-job prereq.
   Override flag bypasses + writes ``decision_audit``.
3. **Bootstrap gate helper** — ``check_bootstrap_state_gate`` returns
   the right tuple for each ``(invocation_path, status, override)``
   combination and writes the audit row only on actual override.

The PR1b prereq tests in ``test_bootstrap_state_gate.py`` cover the
per-job branch; this file is dedicated to the universal gate + envelope
plumbing PR1b-2 added.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, call, patch
from uuid import uuid4

import pytest

from app.jobs.listener import _dispatch_manual_job, _extract_envelope, _MalformedEnvelopeError
from app.services.processes.bootstrap_gate import check_bootstrap_state_gate

# ---------------------------------------------------------------------------
# 1. Envelope normalisation
# ---------------------------------------------------------------------------


class TestExtractEnvelope:
    """``_extract_envelope`` decodes durable payloads into (params, control)."""

    def test_canonical_envelope(self) -> None:
        params, control = _extract_envelope(
            {"params": {"start_date": "2026-01-01"}, "control": {"override_bootstrap_gate": True}}
        )
        assert params == {"start_date": "2026-01-01"}
        assert control == {"override_bootstrap_gate": True}

    def test_legacy_flat_dict(self) -> None:
        """Pre-PR1b-2 callers passed a flat dict — entire body becomes params."""
        params, control = _extract_envelope({"start_date": "2026-01-01"})
        assert params == {"start_date": "2026-01-01"}
        assert control == {}

    def test_none_payload(self) -> None:
        """Legacy queue rows have NULL payload — both sides empty."""
        params, control = _extract_envelope(None)
        assert params == {}
        assert control == {}

    def test_partial_envelope_params_only(self) -> None:
        params, control = _extract_envelope({"params": {"x": 1}})
        assert params == {"x": 1}
        assert control == {}

    def test_partial_envelope_control_only(self) -> None:
        params, control = _extract_envelope({"control": {"override_bootstrap_gate": True}})
        assert params == {}
        assert control == {"override_bootstrap_gate": True}

    def test_non_dict_payload_raises(self) -> None:
        """Codex pre-push round 2 — direct queue insert with list payload rejects."""
        with pytest.raises(_MalformedEnvelopeError):
            _extract_envelope([1, 2, 3])

    def test_envelope_params_non_dict_raises(self) -> None:
        """Codex pre-push round 1 — malformed inner params now rejects."""
        with pytest.raises(_MalformedEnvelopeError):
            _extract_envelope({"params": "not a dict", "control": {}})

    def test_envelope_control_non_dict_raises(self) -> None:
        with pytest.raises(_MalformedEnvelopeError):
            _extract_envelope({"params": {}, "control": [1, 2]})

    def test_unknown_control_key_raises(self) -> None:
        """Codex pre-push round 2 — direct insert with typo'd flag rejects."""
        with pytest.raises(_MalformedEnvelopeError) as exc:
            _extract_envelope({"params": {}, "control": {"override_bootstrap_gates": True}})
        assert "override_bootstrap_gates" in str(exc.value)

    def test_override_must_be_strict_bool(self) -> None:
        """Codex pre-push round 2 BLOCKING — truthy strings cannot grant override."""
        with pytest.raises(_MalformedEnvelopeError) as exc:
            _extract_envelope({"params": {}, "control": {"override_bootstrap_gate": "true"}})
        assert "boolean" in str(exc.value)


# ---------------------------------------------------------------------------
# 2. Listener gate (universal bootstrap_state)
# ---------------------------------------------------------------------------


def _runtime_mock() -> MagicMock:
    runtime = MagicMock()
    runtime.submit_manual_with_request = MagicMock()
    return runtime


def _job_no_prereq(name: str) -> Any:
    job = MagicMock()
    job.name = name
    job.prerequisite = None
    # #1181 — MagicMock auto-creates truthy attributes for any access;
    # set the carve-out flag to False explicitly so the listener's
    # gate-bypass check evaluates the non-exempt path.
    job.exempt_from_universal_bootstrap_gate = False
    return job


class TestListenerGate:
    """Universal bootstrap_state gate fires for jobs in SCHEDULED_JOBS."""

    def test_gate_blocks_without_override_marks_rejected(self) -> None:
        """status != 'complete', no override → mark_request_rejected with reason."""
        runtime = _runtime_mock()
        fake_job = _job_no_prereq("daily_cik_refresh")
        with (
            patch(
                "app.jobs.listener.check_bootstrap_state_gate",
                return_value=(False, "bootstrap_not_complete"),
            ),
            patch("app.jobs.listener.SCHEDULED_JOBS", [fake_job]),
            patch("app.jobs.listener.VALID_JOB_NAMES", {"daily_cik_refresh"}),
            patch("app.jobs.listener.psycopg.connect") as mock_connect,
            patch("app.jobs.listener.mark_request_rejected") as mock_reject,
        ):
            mock_connect.return_value.__enter__.return_value = MagicMock()
            _dispatch_manual_job(
                runtime=runtime,
                request_id=42,
                job_name="daily_cik_refresh",
                payload={"params": {}, "control": {}},
            )
        # Reject (NOT complete — PREVENTION-grade per data-engineer §6.5.7 step 8).
        mock_reject.assert_called_once()
        kwargs = mock_reject.call_args.kwargs
        assert kwargs["error_msg"] == "bootstrap_not_complete"
        runtime.submit_manual_with_request.assert_not_called()

    def test_gate_allows_with_override(self) -> None:
        """override_present=True + gate returns allowed → dispatch fires."""
        runtime = _runtime_mock()
        fake_job = _job_no_prereq("daily_cik_refresh")
        gate_mock = MagicMock(return_value=(True, ""))
        with (
            patch("app.jobs.listener.check_bootstrap_state_gate", gate_mock),
            patch("app.jobs.listener.SCHEDULED_JOBS", [fake_job]),
            patch("app.jobs.listener.VALID_JOB_NAMES", {"daily_cik_refresh"}),
            patch("app.jobs.listener.psycopg.connect") as mock_connect,
            patch("app.jobs.listener.mark_request_rejected") as mock_reject,
        ):
            mock_connect.return_value.__enter__.return_value = MagicMock()
            _dispatch_manual_job(
                runtime=runtime,
                request_id=42,
                job_name="daily_cik_refresh",
                payload={"params": {}, "control": {"override_bootstrap_gate": True}},
            )
        # Gate consulted with override_present=True.
        assert gate_mock.call_args.kwargs["override_present"] is True
        mock_reject.assert_not_called()
        runtime.submit_manual_with_request.assert_called_once()

    def test_gate_skipped_for_bootstrap_internal_jobs(self) -> None:
        """job not in SCHEDULED_JOBS → gate is NOT consulted (orchestrator self-deadlock guard)."""
        runtime = _runtime_mock()
        gate_mock = MagicMock()
        with (
            patch("app.jobs.listener.check_bootstrap_state_gate", gate_mock),
            patch("app.jobs.listener.SCHEDULED_JOBS", []),
            patch("app.jobs.listener.VALID_JOB_NAMES", {"bootstrap_orchestrator"}),
            patch("app.jobs.listener.psycopg.connect") as mock_connect,
            patch("app.jobs.listener.mark_request_rejected") as mock_reject,
        ):
            mock_connect.return_value.__enter__.return_value = MagicMock()
            _dispatch_manual_job(
                runtime=runtime,
                request_id=99,
                job_name="bootstrap_orchestrator",
                payload=None,
            )
        gate_mock.assert_not_called()
        mock_reject.assert_not_called()
        runtime.submit_manual_with_request.assert_called_once()

    def test_invalid_params_rejected_with_400_message(self) -> None:
        """Direct queue insert with invalid params → mark_request_rejected before gate."""
        runtime = _runtime_mock()
        fake_job = _job_no_prereq("sec_13f_quarterly_sweep")
        # job_name=sec_13f_quarterly_sweep allows internal key 'source_label'
        # ONLY when allow_internal_keys=True; the listener uses False, so an
        # operator-supplied source_label is rejected.
        with (
            patch("app.jobs.listener.SCHEDULED_JOBS", [fake_job]),
            patch("app.jobs.listener.VALID_JOB_NAMES", {"sec_13f_quarterly_sweep"}),
            patch("app.jobs.listener.psycopg.connect") as mock_connect,
            patch("app.jobs.listener.mark_request_rejected") as mock_reject,
        ):
            mock_connect.return_value.__enter__.return_value = MagicMock()
            _dispatch_manual_job(
                runtime=runtime,
                request_id=7,
                job_name="sec_13f_quarterly_sweep",
                payload={"params": {"source_label": "operator_attempt"}, "control": {}},
            )
        mock_reject.assert_called_once()
        kwargs = mock_reject.call_args.kwargs
        assert "invalid params" in kwargs["error_msg"]
        runtime.submit_manual_with_request.assert_not_called()

    def test_malformed_envelope_rejects_with_contract_message(self) -> None:
        """Direct queue insert with malformed envelope → mark_request_rejected."""
        runtime = _runtime_mock()
        fake_job = _job_no_prereq("daily_cik_refresh")
        with (
            patch("app.jobs.listener.SCHEDULED_JOBS", [fake_job]),
            patch("app.jobs.listener.VALID_JOB_NAMES", {"daily_cik_refresh"}),
            patch("app.jobs.listener.psycopg.connect") as mock_connect,
            patch("app.jobs.listener.mark_request_rejected") as mock_reject,
        ):
            mock_connect.return_value.__enter__.return_value = MagicMock()
            _dispatch_manual_job(
                runtime=runtime,
                request_id=42,
                job_name="daily_cik_refresh",
                payload={"params": "not a dict", "control": {}},
            )
        mock_reject.assert_called_once()
        kwargs = mock_reject.call_args.kwargs
        assert "malformed payload" in kwargs["error_msg"]
        runtime.submit_manual_with_request.assert_not_called()

    def test_validated_params_passed_to_runtime(self) -> None:
        """Valid params on the envelope flow to runtime.submit_manual_with_request."""
        runtime = _runtime_mock()
        fake_job = _job_no_prereq("daily_cik_refresh")
        # The job has no declared ParamMetadata, so the only valid params
        # dict is empty. The validator passes ``{}`` through unchanged.
        with (
            patch("app.jobs.listener.check_bootstrap_state_gate", return_value=(True, "")),
            patch("app.jobs.listener.SCHEDULED_JOBS", [fake_job]),
            patch("app.jobs.listener.VALID_JOB_NAMES", {"daily_cik_refresh"}),
            patch("app.jobs.listener.psycopg.connect") as mock_connect,
            patch("app.jobs.listener.mark_request_rejected"),
        ):
            mock_connect.return_value.__enter__.return_value = MagicMock()
            _dispatch_manual_job(
                runtime=runtime,
                request_id=42,
                job_name="daily_cik_refresh",
                payload={"params": {}, "control": {}},
            )
        runtime.submit_manual_with_request.assert_called_once_with(
            "daily_cik_refresh", request_id=42, mode=None, params={}
        )


# ---------------------------------------------------------------------------
# 3. check_bootstrap_state_gate helper unit tests
# ---------------------------------------------------------------------------


class _FakeBootstrapState:
    """Stand-in for app.services.bootstrap_state.BootstrapState."""

    def __init__(self, status: str) -> None:
        self.status = status


class TestBootstrapStateGate:
    """Unit-level coverage of the gate decision matrix + audit-write path."""

    def _patched_state(self, status: str) -> Any:
        return patch(
            "app.services.processes.bootstrap_gate.read_state",
            return_value=_FakeBootstrapState(status),
        )

    def test_complete_status_allows_no_audit(self) -> None:
        """Happy path: complete bootstrap → (True, '') with NO audit row."""
        conn = MagicMock()
        with self._patched_state("complete"):
            allowed, reason = check_bootstrap_state_gate(
                conn,
                job_name="daily_cik_refresh",
                invocation_path="manual_queue",
                override_present=False,
            )
        assert allowed is True
        assert reason == ""
        conn.execute.assert_not_called()

    def test_partial_error_blocks_scheduled(self) -> None:
        conn = MagicMock()
        with self._patched_state("partial_error"):
            allowed, reason = check_bootstrap_state_gate(
                conn,
                job_name="fundamentals_sync",
                invocation_path="scheduled",
                override_present=False,
            )
        assert allowed is False
        assert reason == "bootstrap_not_complete"
        conn.execute.assert_not_called()

    def test_running_blocks_manual_no_override(self) -> None:
        conn = MagicMock()
        with self._patched_state("running"):
            allowed, reason = check_bootstrap_state_gate(
                conn,
                job_name="daily_cik_refresh",
                invocation_path="manual_queue",
                override_present=False,
            )
        assert allowed is False
        assert reason == "bootstrap_not_complete"
        conn.execute.assert_not_called()

    def test_manual_override_allows_and_audits(self) -> None:
        conn = MagicMock()
        operator_id = uuid4()
        with self._patched_state("partial_error"):
            allowed, reason = check_bootstrap_state_gate(
                conn,
                job_name="daily_cik_refresh",
                invocation_path="manual_queue",
                override_present=True,
                operator_id=operator_id,
            )
        assert allowed is True
        assert reason == ""
        # Single decision_audit INSERT.
        assert conn.execute.call_count == 1
        sql = conn.execute.call_args.args[0]
        assert "INSERT INTO decision_audit" in sql
        assert "bootstrap_gate_override" in sql
        params = conn.execute.call_args.args[1]
        assert "manual override" in params["expl"]
        assert "partial_error" in params["expl"]
        # operator_id rides on evidence_json, not expl.
        assert str(operator_id) in params["evidence"]

    def test_scheduled_path_ignores_override(self) -> None:
        """Scheduled fires cannot override — override_present is meaningless there."""
        conn = MagicMock()
        with self._patched_state("partial_error"):
            allowed, reason = check_bootstrap_state_gate(
                conn,
                job_name="daily_cik_refresh",
                invocation_path="scheduled",
                override_present=True,  # ignored
            )
        assert allowed is False
        assert reason == "bootstrap_not_complete"
        conn.execute.assert_not_called()

    def test_audit_write_failure_does_not_block_run(self) -> None:
        """If decision_audit INSERT raises, the gate STILL returns allowed.

        Audit is desired but not load-bearing: we have already decided to
        allow the run on operator's explicit override; failing closed
        because the audit row failed to write would punish the operator
        for an unrelated DB hiccup.
        """
        conn = MagicMock()
        conn.execute.side_effect = RuntimeError("audit insert failed")
        with self._patched_state("partial_error"):
            allowed, reason = check_bootstrap_state_gate(
                conn,
                job_name="daily_cik_refresh",
                invocation_path="manual_queue",
                override_present=True,
                operator_id="svc-token",
            )
        assert allowed is True
        assert reason == ""
        # We tried once.
        assert conn.execute.call_args_list == [call(conn.execute.call_args.args[0], conn.execute.call_args.args[1])]
