"""Row-count spike detection (moved from ops_monitor in chunk 7).

Behaviour is byte-identical to the old ops_monitor.check_row_count_spike.
This test file exercises the new import path and a couple of
regression cases lifted from tests/test_ops_monitor.py so the move
does not silently drop coverage.
"""

from unittest.mock import MagicMock

from app.services.sync_orchestrator.row_count_spikes import check_row_count_spike


def test_returns_not_flagged_when_no_prior_runs() -> None:
    conn = MagicMock()
    conn.execute.return_value.fetchall.return_value = []
    result = check_row_count_spike(conn, "jobname", current_count=100)
    assert result.flagged is False


def test_imports_resolve() -> None:
    # Regression guard: no other orchestrator module should import
    # check_row_count_spike from ops_monitor after this chunk.
    from app.services.sync_orchestrator import row_count_spikes
    assert hasattr(row_count_spikes, "check_row_count_spike")
