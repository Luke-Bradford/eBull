"""Tests for the attribution summary scheduler job (Task 6).

Covers:
- attribution_summary_job: calls compute/persist for each SUMMARY_WINDOWS entry
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from app.services.return_attribution import SUMMARY_WINDOWS
from app.workers.scheduler import attribution_summary_job

# ---------------------------------------------------------------------------
# Patch paths
# ---------------------------------------------------------------------------

_PSYCOPG_CONNECT_PATCH = "app.workers.scheduler.psycopg.connect"
_RECORD_START_PATCH = "app.workers.scheduler.record_job_start"
_RECORD_FINISH_PATCH = "app.workers.scheduler.record_job_finish"
_SPIKE_PATCH = "app.workers.scheduler.check_row_count_spike"
_COMPUTE_PATCH = "app.workers.scheduler.compute_attribution_summary"
_PERSIST_PATCH = "app.workers.scheduler.persist_attribution_summary"


def _make_conn_ctx() -> MagicMock:
    """Return a MagicMock that acts as a psycopg connection context manager."""
    conn = MagicMock()
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    # transaction() must also behave as a context manager
    txn_ctx = MagicMock()
    txn_ctx.__enter__ = MagicMock(return_value=txn_ctx)
    txn_ctx.__exit__ = MagicMock(return_value=False)
    conn.transaction.return_value = txn_ctx
    return conn


def _make_summary_mock(positions: int = 5) -> MagicMock:
    from decimal import Decimal

    m = MagicMock()
    m.positions_attributed = positions
    m.avg_model_alpha_pct = Decimal("0.0123")
    return m


class TestAttributionSummaryJob:
    """Tests for attribution_summary_job."""

    @patch(_SPIKE_PATCH, return_value=MagicMock(flagged=False))
    @patch(_RECORD_FINISH_PATCH)
    @patch(_RECORD_START_PATCH, return_value=1)
    @patch(_PERSIST_PATCH)
    @patch(_COMPUTE_PATCH)
    @patch(_PSYCOPG_CONNECT_PATCH)
    def test_computes_summaries_for_all_windows(
        self,
        mock_connect: MagicMock,
        mock_compute: MagicMock,
        mock_persist: MagicMock,
        mock_start: MagicMock,
        mock_finish: MagicMock,
        mock_spike: MagicMock,
    ) -> None:
        """compute_attribution_summary must be called once per SUMMARY_WINDOWS entry."""
        conn_ctx = _make_conn_ctx()
        mock_connect.return_value = conn_ctx
        mock_compute.return_value = _make_summary_mock()

        attribution_summary_job()

        assert mock_compute.call_count == len(SUMMARY_WINDOWS)
        called_windows = [call.args[1] for call in mock_compute.call_args_list]
        assert called_windows == list(SUMMARY_WINDOWS)

    @patch(_SPIKE_PATCH, return_value=MagicMock(flagged=False))
    @patch(_RECORD_FINISH_PATCH)
    @patch(_RECORD_START_PATCH, return_value=1)
    @patch(_PERSIST_PATCH)
    @patch(_COMPUTE_PATCH)
    @patch(_PSYCOPG_CONNECT_PATCH)
    def test_persists_each_summary(
        self,
        mock_connect: MagicMock,
        mock_compute: MagicMock,
        mock_persist: MagicMock,
        mock_start: MagicMock,
        mock_finish: MagicMock,
        mock_spike: MagicMock,
    ) -> None:
        """persist_attribution_summary must be called once per window."""
        conn_ctx = _make_conn_ctx()
        mock_connect.return_value = conn_ctx

        summaries = [_make_summary_mock(i + 1) for i in range(len(SUMMARY_WINDOWS))]
        mock_compute.side_effect = summaries

        attribution_summary_job()

        assert mock_persist.call_count == len(SUMMARY_WINDOWS)
        for i, call in enumerate(mock_persist.call_args_list):
            # Second positional arg is the SummaryResult
            assert call.args[1] is summaries[i]
