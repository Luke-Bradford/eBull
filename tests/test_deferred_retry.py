"""Tests for app.services.deferred_retry.

Structure:
  - TestRetryDeferredRecommendations — full retry_deferred_recommendations
    with mocked DB and mocked evaluate_entry_conditions.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock, patch

from app.services.deferred_retry import (
    MAX_RETRY_ATTEMPTS,
    RETRY_EXPIRY_HOURS,
    RetryResult,
    retry_deferred_recommendations,
)
from app.services.entry_timing import EntryEvaluation

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_PATCH_TARGET = "app.services.deferred_retry.evaluate_entry_conditions"


def _make_cursor(rows: list[dict[str, Any]]) -> MagicMock:
    cur = MagicMock()
    cur.fetchall.return_value = rows
    cur.fetchone.return_value = rows[0] if rows else None
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    return cur


def _make_conn(cursor: MagicMock) -> MagicMock:
    """Return a connection mock whose cursor() context manager yields ``cursor``."""
    conn = MagicMock()
    conn.cursor.return_value = cursor
    # transaction() must work as a context manager
    txn_ctx = MagicMock()
    txn_ctx.__enter__ = MagicMock(return_value=txn_ctx)
    txn_ctx.__exit__ = MagicMock(return_value=False)
    conn.transaction.return_value = txn_ctx
    return conn


def _rec(
    recommendation_id: int = 1,
    instrument_id: int = 42,
    action: str = "BUY",
    timing_retry_count: int = 0,
    timing_deferred_at: datetime | None = None,
) -> dict[str, Any]:
    if timing_deferred_at is None:
        timing_deferred_at = datetime.now(tz=UTC) - timedelta(hours=1)
    return {
        "recommendation_id": recommendation_id,
        "instrument_id": instrument_id,
        "action": action,
        "timing_retry_count": timing_retry_count,
        "timing_deferred_at": timing_deferred_at,
    }


def _pass_evaluation() -> EntryEvaluation:
    return EntryEvaluation(
        verdict="pass",
        stop_loss_rate=Decimal("142.5"),
        take_profit_rate=Decimal("200.0"),
        rationale="PASS (all conditions favorable)",
        condition_details={"cond_0": "rsi: 55.0 (ok)"},
    )


def _defer_evaluation() -> EntryEvaluation:
    return EntryEvaluation(
        verdict="defer",
        stop_loss_rate=Decimal("142.5"),
        take_profit_rate=None,
        rationale="DEFER (1 unfavorable): rsi: 80.0 > 75.0 (overbought, defer)",
        condition_details={"cond_0": "rsi: 80.0 > 75.0 (overbought, defer)"},
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestRetryDeferredRecommendations:
    """retry_deferred_recommendations with mocked DB."""

    def test_no_deferred_recs_returns_zero_counts(self) -> None:
        """Empty fetchall → RetryResult with all zeros, no DB writes."""
        cur = _make_cursor([])
        conn = _make_conn(cur)

        result = retry_deferred_recommendations(conn)

        assert result == RetryResult(retried=0, re_proposed=0, re_deferred=0, expired=0, errors=0)
        conn.execute.assert_not_called()

    def test_deferred_rec_within_retry_limit_is_re_evaluated(self) -> None:
        """retry_count < MAX and not expired → evaluate, PASS → re_proposed=1."""
        rec = _rec(timing_retry_count=0)
        cur = _make_cursor([rec])
        conn = _make_conn(cur)

        with patch(_PATCH_TARGET, return_value=_pass_evaluation()) as mock_fn:
            result = retry_deferred_recommendations(conn)

        mock_fn.assert_called_once_with(conn, rec["recommendation_id"])
        assert result.re_proposed == 1
        assert result.retried == 1
        assert result.re_deferred == 0
        assert result.expired == 0
        assert result.errors == 0

    def test_deferred_rec_exceeding_retry_limit_is_expired(self) -> None:
        """retry_count == MAX_RETRY_ATTEMPTS → expired=1, no evaluation call."""
        rec = _rec(timing_retry_count=MAX_RETRY_ATTEMPTS)
        cur = _make_cursor([rec])
        conn = _make_conn(cur)

        with patch(_PATCH_TARGET) as mock_fn:
            result = retry_deferred_recommendations(conn)

        mock_fn.assert_not_called()
        assert result.expired == 1
        assert result.retried == 0
        assert result.errors == 0
        # Verify the UPDATE to timing_expired was issued
        update_calls = [str(c) for c in conn.execute.call_args_list]
        assert any("timing_expired" in c for c in update_calls)

    def test_deferred_rec_older_than_expiry_is_expired(self) -> None:
        """deferred_at older than RETRY_EXPIRY_HOURS → expired=1, no evaluation call."""
        old_time = datetime.now(tz=UTC) - timedelta(hours=RETRY_EXPIRY_HOURS + 1)
        rec = _rec(timing_retry_count=0, timing_deferred_at=old_time)
        cur = _make_cursor([rec])
        conn = _make_conn(cur)

        with patch(_PATCH_TARGET) as mock_fn:
            result = retry_deferred_recommendations(conn)

        mock_fn.assert_not_called()
        assert result.expired == 1
        assert result.retried == 0
        assert result.errors == 0

    def test_re_evaluation_still_unfavorable_increments_retry_count(self) -> None:
        """eval returns defer → re_deferred=1, retry_count incremented."""
        rec = _rec(timing_retry_count=1)
        cur = _make_cursor([rec])
        conn = _make_conn(cur)

        with patch(_PATCH_TARGET, return_value=_defer_evaluation()):
            result = retry_deferred_recommendations(conn)

        assert result.re_deferred == 1
        assert result.retried == 1
        assert result.re_proposed == 0
        assert result.expired == 0
        assert result.errors == 0
        # Verify retry_count was incremented (UPDATE with timing_retry_count)
        update_calls = [str(c) for c in conn.execute.call_args_list]
        assert any("timing_retry_count" in c for c in update_calls)

    def test_evaluation_exception_increments_errors_and_retry_count(self) -> None:
        """evaluate_entry_conditions raises → errors=1, retry_count increment attempted."""
        rec = _rec(timing_retry_count=0)
        cur = _make_cursor([rec])
        conn = _make_conn(cur)

        with patch(_PATCH_TARGET, side_effect=RuntimeError("TA data unavailable")):
            result = retry_deferred_recommendations(conn)

        assert result.errors == 1
        assert result.retried == 1
        assert result.re_proposed == 0
        assert result.re_deferred == 0
        # Best-effort retry_count increment: transaction() must have been called
        conn.transaction.assert_called()

    def test_multiple_recs_counted_independently(self) -> None:
        """Two recs: one passes, one defers → re_proposed=1, re_deferred=1."""
        rec_pass = _rec(recommendation_id=1, timing_retry_count=0)
        rec_defer = _rec(recommendation_id=2, timing_retry_count=1)
        cur = _make_cursor([rec_pass, rec_defer])
        conn = _make_conn(cur)

        def _dispatch(_conn: Any, rec_id: int) -> EntryEvaluation:
            if rec_id == 1:
                return _pass_evaluation()
            return _defer_evaluation()

        with patch(_PATCH_TARGET, side_effect=_dispatch):
            result = retry_deferred_recommendations(conn)

        assert result.re_proposed == 1
        assert result.re_deferred == 1
        assert result.retried == 2
        assert result.expired == 0
        assert result.errors == 0

    def test_expiry_check_age_wins_over_retry_count(self) -> None:
        """rec with retry_count < MAX but age > RETRY_EXPIRY_HOURS → expired (age wins)."""
        old_time = datetime.now(tz=UTC) - timedelta(hours=RETRY_EXPIRY_HOURS + 5)
        rec = _rec(timing_retry_count=MAX_RETRY_ATTEMPTS - 1, timing_deferred_at=old_time)
        cur = _make_cursor([rec])
        conn = _make_conn(cur)

        with patch(_PATCH_TARGET) as mock_fn:
            result = retry_deferred_recommendations(conn)

        mock_fn.assert_not_called()
        assert result.expired == 1

    def test_audit_row_written_on_pass(self) -> None:
        """On PASS, decision_audit INSERT includes stage=deferred_retry and pass_fail=PASS."""
        rec = _rec(timing_retry_count=0)
        cur = _make_cursor([rec])
        conn = _make_conn(cur)

        with patch(_PATCH_TARGET, return_value=_pass_evaluation()):
            retry_deferred_recommendations(conn)

        all_calls = [str(c) for c in conn.execute.call_args_list]
        assert any("deferred_retry" in c for c in all_calls)
        assert any("PASS" in c for c in all_calls)

    def test_audit_row_written_on_expiry(self) -> None:
        """On expiry, decision_audit INSERT includes pass_fail=FAIL."""
        rec = _rec(timing_retry_count=MAX_RETRY_ATTEMPTS)
        cur = _make_cursor([rec])
        conn = _make_conn(cur)

        with patch(_PATCH_TARGET):
            retry_deferred_recommendations(conn)

        all_calls = [str(c) for c in conn.execute.call_args_list]
        assert any("FAIL" in c for c in all_calls)

    def test_retry_result_is_frozen_dataclass(self) -> None:
        """RetryResult must be immutable (frozen=True)."""
        result = RetryResult(retried=1, re_proposed=1, re_deferred=0, expired=0)
        try:
            result.retried = 99  # type: ignore[misc]
            raise AssertionError("Should have raised FrozenInstanceError")
        except Exception as exc:
            assert "cannot assign" in str(exc).lower() or "frozen" in str(exc).lower()

    def test_none_timing_deferred_at_skips_age_check(self) -> None:
        """If timing_deferred_at is None, age check is skipped — rec is re-evaluated."""
        rec = _rec(timing_retry_count=0)
        rec["timing_deferred_at"] = None
        cur = _make_cursor([rec])
        conn = _make_conn(cur)

        with patch(_PATCH_TARGET, return_value=_pass_evaluation()) as mock_fn:
            result = retry_deferred_recommendations(conn)

        mock_fn.assert_called_once()
        assert result.re_proposed == 1
        assert result.expired == 0
