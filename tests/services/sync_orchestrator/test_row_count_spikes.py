"""Row-count spike detection (moved from ops_monitor in chunk 7).

Behaviour is byte-identical to the old ops_monitor.check_row_count_spike.
This test file exercises the new import path and a couple of
regression cases lifted from tests/test_ops_monitor.py so the move
does not silently drop coverage.
"""

from unittest.mock import MagicMock

from app.services.sync_orchestrator.row_count_spikes import check_row_count_spike


def _mock_conn_with_prior(prior_row: dict | None) -> MagicMock:
    """Stub a psycopg connection whose cursor() context manager yields a
    cursor whose fetchone() returns `prior_row`.

    Matches the real call shape in row_count_spikes.py:
        with conn.cursor(row_factory=...) as cur:
            cur.execute(...)
            row = cur.fetchone()
    """
    conn = MagicMock()
    cur = MagicMock()
    cur.fetchone.return_value = prior_row
    # cursor() is a context manager.
    cm = MagicMock()
    cm.__enter__.return_value = cur
    cm.__exit__.return_value = False
    conn.cursor.return_value = cm
    return conn


def test_returns_not_flagged_when_no_prior_runs() -> None:
    # fetchone() returns None when job_runs has no prior successful row.
    conn = _mock_conn_with_prior(None)
    result = check_row_count_spike(conn, "jobname", current_count=100)
    assert result.flagged is False
    assert "no prior row_count" in result.detail


def test_returns_not_flagged_when_counts_match() -> None:
    conn = _mock_conn_with_prior({"row_count": 100})
    result = check_row_count_spike(conn, "jobname", current_count=100)
    assert result.flagged is False


def test_flags_when_current_drops_below_threshold() -> None:
    # 50% drop is well under the _SPIKE_RATIO_THRESHOLD.
    conn = _mock_conn_with_prior({"row_count": 100})
    result = check_row_count_spike(conn, "jobname", current_count=40)
    assert result.flagged is True


def test_imports_resolve_from_new_path() -> None:
    # Regression guard: the new module is importable without touching
    # ops_monitor.
    from app.services.sync_orchestrator import row_count_spikes

    assert hasattr(row_count_spikes, "check_row_count_spike")


# Boundary cases moved from tests/test_ops_monitor.py::TestCheckRowCountSpike
# when the ops_monitor back-compat shim was retired (#340). They guard the
# 50%-of-prior threshold edges (prevention-log §"check_row_count_spike compared
# the run against itself").


def test_count_above_threshold_not_flagged() -> None:
    # Previous: 100, current: 80 → ratio 0.8 > 0.5 threshold.
    conn = _mock_conn_with_prior({"row_count": 100})
    result = check_row_count_spike(conn, "jobname", current_count=80)
    assert result.flagged is False
    assert result.previous_count == 100


def test_drop_detail_names_the_drop() -> None:
    conn = _mock_conn_with_prior({"row_count": 100})
    result = check_row_count_spike(conn, "jobname", current_count=40)
    assert result.flagged is True
    assert "dropped" in result.detail


def test_zero_current_flagged() -> None:
    # Previous: 50, current: 0 → ratio 0.0 < 0.5 threshold.
    conn = _mock_conn_with_prior({"row_count": 50})
    result = check_row_count_spike(conn, "jobname", current_count=0)
    assert result.flagged is True


def test_zero_previous_not_flagged() -> None:
    # Previous: 0 → skip comparison (avoid divide by zero).
    conn = _mock_conn_with_prior({"row_count": 0})
    result = check_row_count_spike(conn, "jobname", current_count=0)
    assert result.flagged is False


def test_exactly_at_threshold_not_flagged() -> None:
    # Previous: 100, current: 50 → ratio 0.5 == threshold (not strictly less).
    conn = _mock_conn_with_prior({"row_count": 100})
    result = check_row_count_spike(conn, "jobname", current_count=50)
    assert result.flagged is False


def test_just_below_threshold_flagged() -> None:
    # Previous: 100, current: 49 → ratio 0.49 < 0.5 threshold.
    conn = _mock_conn_with_prior({"row_count": 100})
    result = check_row_count_spike(conn, "jobname", current_count=49)
    assert result.flagged is True
