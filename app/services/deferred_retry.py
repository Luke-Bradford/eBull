"""Deferred retry service — re-evaluate timing_deferred recommendations.

Sits between the scheduler's execute_approved_orders job and the
entry_timing service.  BUY/ADD recommendations that were deferred by
Phase 0 (TA conditions unfavorable) are retried here up to
MAX_RETRY_ATTEMPTS times within RETRY_EXPIRY_HOURS.

Design:
  - Expire first (max retries or age) before re-evaluating.
  - Each status transition + audit row is atomic (one transaction).
  - Exceptions on a single rec are logged and counted; they never abort
    the rest of the batch (data-inconsistency case, not programmer error).
  - Pure service: caller provides the connection; no direct DB connects.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import psycopg
import psycopg.rows
from psycopg.types.json import Jsonb

from app.services.entry_timing import EntryEvaluation, evaluate_entry_conditions

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MAX_RETRY_ATTEMPTS: int = 3
RETRY_EXPIRY_HOURS: int = 24


# ---------------------------------------------------------------------------
# Public types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class RetryResult:
    """Counts from a single run of retry_deferred_recommendations()."""

    retried: int
    re_proposed: int
    re_deferred: int
    expired: int
    errors: int = 0


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _expire_rec(
    conn: psycopg.Connection[Any],
    rec: dict[str, Any],
    reason: str,
) -> None:
    """Atomically expire a rec: set status=timing_expired + write audit row.

    Must be called inside a ``with conn.transaction():`` block so the caller
    can group the commit after the context manager exits.
    """
    rec_id: int = rec["recommendation_id"]
    instrument_id: int = rec["instrument_id"]

    conn.execute(
        """
        INSERT INTO decision_audit
            (decision_time, instrument_id, recommendation_id,
             stage, pass_fail, explanation)
        VALUES
            (NOW(), %(iid)s, %(rid)s,
             'deferred_retry', 'FAIL', %(expl)s)
        """,
        {"iid": instrument_id, "rid": rec_id, "expl": reason},
    )
    conn.execute(
        """
        UPDATE trade_recommendations
        SET status = 'timing_expired',
            timing_rationale = %(rationale)s
        WHERE recommendation_id = %(rid)s
        """,
        {"rid": rec_id, "rationale": reason},
    )


def _write_retry_pass(
    conn: psycopg.Connection[Any],
    rec: dict[str, Any],
    evaluation: EntryEvaluation,
) -> None:
    """Atomically transition a rec to proposed after a PASS verdict."""
    rec_id: int = rec["recommendation_id"]
    instrument_id: int = rec["instrument_id"]
    new_retry_count: int = rec["timing_retry_count"] + 1

    conn.execute(
        """
        INSERT INTO decision_audit
            (decision_time, instrument_id, recommendation_id,
             stage, pass_fail, explanation, evidence_json)
        VALUES
            (NOW(), %(iid)s, %(rid)s,
             'deferred_retry', 'PASS', %(expl)s, %(ev)s)
        """,
        {
            "iid": instrument_id,
            "rid": rec_id,
            "expl": evaluation.rationale,
            "ev": Jsonb(evaluation.condition_details),
        },
    )
    conn.execute(
        """
        UPDATE trade_recommendations
        SET status = 'proposed',
            stop_loss_rate = %(sl)s,
            take_profit_rate = %(tp)s,
            timing_verdict = %(verdict)s,
            timing_rationale = %(rationale)s,
            timing_retry_count = %(retry_count)s
        WHERE recommendation_id = %(rid)s
        """,
        {
            "sl": evaluation.stop_loss_rate,
            "tp": evaluation.take_profit_rate,
            "verdict": evaluation.verdict,
            "rationale": evaluation.rationale,
            "retry_count": new_retry_count,
            "rid": rec_id,
        },
    )


def _write_retry_defer(
    conn: psycopg.Connection[Any],
    rec: dict[str, Any],
    evaluation: EntryEvaluation,
) -> None:
    """Atomically increment retry_count and stay timing_deferred."""
    rec_id: int = rec["recommendation_id"]
    instrument_id: int = rec["instrument_id"]
    new_retry_count: int = rec["timing_retry_count"] + 1

    conn.execute(
        """
        INSERT INTO decision_audit
            (decision_time, instrument_id, recommendation_id,
             stage, pass_fail, explanation, evidence_json)
        VALUES
            (NOW(), %(iid)s, %(rid)s,
             'deferred_retry', 'DEFER', %(expl)s, %(ev)s)
        """,
        {
            "iid": instrument_id,
            "rid": rec_id,
            "expl": evaluation.rationale,
            "ev": Jsonb(evaluation.condition_details),
        },
    )
    conn.execute(
        """
        UPDATE trade_recommendations
        SET timing_retry_count = %(retry_count)s,
            timing_verdict = %(verdict)s,
            timing_rationale = %(rationale)s
        WHERE recommendation_id = %(rid)s
        """,
        {
            "retry_count": new_retry_count,
            "verdict": evaluation.verdict,
            "rationale": evaluation.rationale,
            "rid": rec_id,
        },
    )


def _write_retry_error(
    conn: psycopg.Connection[Any],
    rec: dict[str, Any],
) -> None:
    """Increment retry_count and write audit row on the error path.

    The rec stays in timing_deferred so the next cycle can retry it.
    The audit row captures that a retry attempt was consumed by an error,
    so the trail is complete when the rec is eventually expired.
    """
    rec_id: int = rec["recommendation_id"]
    instrument_id: int = rec["instrument_id"]
    new_retry_count: int = rec["timing_retry_count"] + 1

    conn.execute(
        """
        INSERT INTO decision_audit
            (decision_time, instrument_id, recommendation_id,
             stage, pass_fail, explanation)
        VALUES
            (NOW(), %(iid)s, %(rid)s,
             'deferred_retry', 'FAIL',
             %(expl)s)
        """,
        {
            "iid": instrument_id,
            "rid": rec_id,
            "expl": f"evaluation raised an exception (retry_count={new_retry_count})",
        },
    )
    conn.execute(
        """
        UPDATE trade_recommendations
        SET timing_retry_count = %(retry_count)s
        WHERE recommendation_id = %(rid)s
        """,
        {"retry_count": new_retry_count, "rid": rec_id},
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def retry_deferred_recommendations(conn: psycopg.Connection[Any]) -> RetryResult:
    """Re-evaluate all timing_deferred BUY/ADD recommendations.

    For each deferred rec:
    - Expire if retry count >= MAX_RETRY_ATTEMPTS or age > RETRY_EXPIRY_HOURS.
    - Otherwise, call evaluate_entry_conditions():
        - pass   → transition to 'proposed' (re_proposed += 1)
        - defer  → stay 'timing_deferred', increment retry_count (re_deferred += 1)
        - exception → increment retry_count only, log error (errors += 1)

    Every status transition + decision_audit row is atomic.
    Exceptions on individual recs are logged and counted; they never abort
    the batch (partial results are better than a total failure).

    Returns a RetryResult with per-outcome counts.
    """
    # Load all deferred recs in a stable order.
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT recommendation_id, instrument_id, action,
                   timing_retry_count, timing_deferred_at
            FROM trade_recommendations
            WHERE status = 'timing_deferred'
              AND action IN ('BUY', 'ADD')
            ORDER BY recommendation_id
            """,
        )
        recs: list[dict[str, Any]] = cur.fetchall()

    if not recs:
        return RetryResult(retried=0, re_proposed=0, re_deferred=0, expired=0)

    now = datetime.now(tz=UTC)
    expiry_cutoff = now - timedelta(hours=RETRY_EXPIRY_HOURS)

    retried = 0
    re_proposed = 0
    re_deferred = 0
    expired = 0
    errors = 0

    for rec in recs:
        rec_id: int = rec["recommendation_id"]
        retry_count: int = rec["timing_retry_count"]
        deferred_at: datetime | None = rec["timing_deferred_at"]

        # --- Expiry check (age OR retry exhaustion) ---
        age_expired = deferred_at is not None and deferred_at < expiry_cutoff
        count_expired = retry_count >= MAX_RETRY_ATTEMPTS

        if age_expired or count_expired:
            if age_expired:
                assert deferred_at is not None  # narrowed by age_expired
                reason = (
                    f"deferred_retry: expired — deferred_at={deferred_at.isoformat()} "
                    f"older than {RETRY_EXPIRY_HOURS}h cutoff"
                )
            else:
                reason = (
                    f"deferred_retry: expired — retry_count={retry_count} >= MAX_RETRY_ATTEMPTS={MAX_RETRY_ATTEMPTS}"
                )
            try:
                with conn.transaction():
                    _expire_rec(conn, rec, reason)
                conn.commit()
                expired += 1
                logger.info("deferred_retry: expired rec=%d reason=%s", rec_id, reason)
            except Exception:
                logger.error(
                    "deferred_retry: failed to expire rec=%d",
                    rec_id,
                    exc_info=True,
                )
                errors += 1
            continue

        # --- Re-evaluate TA conditions ---
        retried += 1
        try:
            # I/O (evaluate_entry_conditions reads DB) happens BEFORE the
            # transaction so the lock window is minimised (sql-correctness).
            evaluation: EntryEvaluation = evaluate_entry_conditions(conn, rec_id)

            if evaluation.verdict == "pass":
                with conn.transaction():
                    _write_retry_pass(conn, rec, evaluation)
                conn.commit()
                re_proposed += 1
                logger.info(
                    "deferred_retry: PASS rec=%d retry_count=%d→%d",
                    rec_id,
                    retry_count,
                    retry_count + 1,
                )

            elif evaluation.verdict in ("defer", "skip"):
                # "skip" should not happen for a BUY/ADD rec but treat it as
                # defer rather than an error — stays deferred, retried later.
                with conn.transaction():
                    _write_retry_defer(conn, rec, evaluation)
                conn.commit()
                re_deferred += 1
                logger.info(
                    "deferred_retry: DEFER rec=%d retry_count=%d→%d rationale=%s",
                    rec_id,
                    retry_count,
                    retry_count + 1,
                    evaluation.rationale,
                )

            else:
                # Defensive: unknown verdict — treat as error.
                logger.error(
                    "deferred_retry: unexpected verdict=%r for rec=%d",
                    evaluation.verdict,
                    rec_id,
                )
                with conn.transaction():
                    _write_retry_error(conn, rec)
                conn.commit()
                errors += 1

        except Exception:
            logger.error(
                "deferred_retry: evaluation raised for rec=%d",
                rec_id,
                exc_info=True,
            )
            # The exception may have left the connection in an error state
            # (InFailedSqlTransaction). Roll back so subsequent operations
            # on the same connection can proceed.
            conn.rollback()
            # Best-effort: increment retry count + write audit row so the
            # error attempt is visible in the audit trail.
            try:
                with conn.transaction():
                    _write_retry_error(conn, rec)
                conn.commit()
            except Exception:
                logger.error(
                    "deferred_retry: could not write error audit for rec=%d",
                    rec_id,
                    exc_info=True,
                )
            errors += 1

    return RetryResult(
        retried=retried,
        re_proposed=re_proposed,
        re_deferred=re_deferred,
        expired=expired,
        errors=errors,
    )
