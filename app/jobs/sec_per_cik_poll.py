"""Per-CIK scheduled polling — Layer 3 (#870).

Issue #870 / spec §"Layer 3 — Per-CIK submissions.json".

Hourly job that reads ``data_freshness_index`` for subjects whose
``expected_next_at`` has elapsed, calls ``check_freshness`` per CIK,
UPSERTs new manifest rows, and updates the scheduler outcome.

Layer 3 is the per-CIK reconcile path — fires only at predicted-next-
filing windows. AAPL's DEF 14A poll fires once a year; AAPL's 13F
poll never fires (issuer subject; AAPL doesn't file 13F).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import psycopg

from app.providers.implementations.sec_submissions import HttpGet, check_freshness
from app.services.data_freshness import (
    FreshnessRow,
    cadence_for,
    record_poll_outcome,
    subjects_due_for_poll,
    subjects_due_for_recheck,
)
from app.services.sec_manifest import ManifestSource, record_manifest_entry

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PerCikPollStats:
    subjects_polled: int
    new_filings_recorded: int
    poll_errors: int
    # #1155 G13 — separate counters for the recheck reader path
    # (subjects_due_for_recheck) so operator can confirm both reader
    # paths are draining. recheck_* counts subjects whose state was
    # 'never_filed' or 'error' at the start of the tick.
    recheck_subjects_polled: int = 0
    recheck_new_filings_recorded: int = 0


def _probe_subject(
    conn: psycopg.Connection[Any],
    subject: FreshnessRow,
    *,
    http_get: HttpGet,
) -> tuple[int, bool]:
    """Probe one subject. Returns ``(new_filings_recorded, errored)``.

    Centralises the per-subject body so the poll and recheck paths
    share identical fetch + UPSERT + outcome-write logic. The caller
    increments its own stat counters based on the return.

    Caller MUST ensure ``subject.cik is not None`` (FINRA universe
    singleton has no submissions.json to poll). Asserted defensively
    to narrow the type for ``check_freshness``.
    """
    assert subject.cik is not None, "_probe_subject requires non-None cik"
    sources_to_check: set[ManifestSource] | None = {subject.source} if subject.source else None
    try:
        delta = check_freshness(
            http_get,
            cik=subject.cik,
            last_known_filing_id=subject.last_known_filing_id,
            sources=sources_to_check,
        )
    except Exception as exc:
        logger.warning(
            "per-cik poll: check_freshness raised for cik=%s source=%s: %s",
            subject.cik,
            subject.source,
            exc,
        )
        record_poll_outcome(
            conn,
            subject_type=subject.subject_type,
            subject_id=subject.subject_id,
            source=subject.source,
            outcome="error",
            error=f"{type(exc).__name__}: {exc}"[:500],
            cik=subject.cik,
            instrument_id=subject.instrument_id,
        )
        return (0, True)

    # UPSERT manifest rows for the new filings
    recorded = 0
    for row in delta.new_filings:
        if row.source is None:
            continue
        try:
            record_manifest_entry(
                conn,
                row.accession_number,
                cik=row.cik,
                form=row.form,
                source=row.source,
                subject_type=subject.subject_type,
                subject_id=subject.subject_id,
                instrument_id=subject.instrument_id,
                filed_at=row.filed_at,
                accepted_at=row.accepted_at,
                primary_document_url=row.primary_document_url,
                is_amendment=row.is_amendment,
            )
            recorded += 1
        except ValueError as exc:
            logger.warning("per-cik poll: rejected accession=%s: %s", row.accession_number, exc)

    # Update scheduler outcome. #1155 G13 — never_filed rows that
    # return no new filings must STAY in the recheck queue (state
    # never_filed) rather than transitioning to 'current'. Otherwise
    # the recheck path's whole point is defeated — every never_filed
    # row leaves the recheck lane on its first poll regardless of
    # whether the subject actually filed.
    outcome: str
    next_recheck_at: datetime | None = None
    if delta.new_filings:
        outcome = "new_data"
    elif subject.state == "never_filed":
        outcome = "never"
        # Keep the row in the recheck queue at the source's cadence.
        next_recheck_at = datetime.now(tz=UTC) + cadence_for(subject.source)
    else:
        # state in ('current', 'expected_filing_overdue', 'error') with
        # no new filings → row is 'current' (error recovers; overdue
        # still tracking until next predicted filing).
        outcome = "current"

    last_known = delta.new_filings[0].accession_number if delta.new_filings else subject.last_known_filing_id
    last_filed = delta.last_filed_at if delta.last_filed_at else subject.last_known_filed_at
    record_poll_outcome(
        conn,
        subject_type=subject.subject_type,
        subject_id=subject.subject_id,
        source=subject.source,
        outcome=outcome,  # type: ignore[arg-type]
        last_known_filing_id=last_known,
        last_known_filed_at=last_filed,
        new_filings_since=len(delta.new_filings),
        next_recheck_at=next_recheck_at,
        cik=subject.cik,
        instrument_id=subject.instrument_id,
    )
    return (recorded, False)


def run_per_cik_poll(
    conn: psycopg.Connection[Any],
    *,
    http_get: HttpGet,
    source: ManifestSource | None = None,
    max_subjects: int = 100,
) -> PerCikPollStats:
    """One per-CIK poll cycle. For each subject due, call submissions.json
    and UPSERT manifest + scheduler.

    Drains BOTH reader paths (#1155 G13):

    * ``subjects_due_for_poll`` — 'current' / 'expected_filing_overdue'
      rows past their ``expected_next_at``. Gets the dominant budget
      share so steady-state polls are never starved by error backlog.
    * ``subjects_due_for_recheck`` — 'never_filed' / 'error' rows past
      their ``next_recheck_at``. Gets the remaining ~1/3 budget so the
      recheck path drains at a guaranteed rate.

    Total subjects probed never exceeds ``max_subjects``. For
    ``max_subjects=100`` → ``poll=66, recheck=34``. For ``max_subjects=1``
    → ``poll=0, recheck=1`` (degenerate but bounded).

    Pagination (``has_more_in_files`` for first-install / rebuild
    paths) is NOT followed here — that lives in the dedicated drain
    + rebuild jobs (#871, #872) which have their own throughput
    budgets. The per-CIK steady-state path uses only the recent array.
    """
    # #1155 G13 — bounded total budget split: 2/3 to poll, ~1/3 to
    # recheck. No max(1, ...) floor so max_subjects=1 stays bounded
    # at total=1 (poll=0, recheck=1).
    poll_budget = max_subjects * 2 // 3
    recheck_budget = max_subjects - poll_budget

    subjects_polled = 0
    new_filings_recorded = 0
    poll_errors = 0
    recheck_subjects_polled = 0
    recheck_new_filings_recorded = 0

    poll_due = list(subjects_due_for_poll(conn, source=source, limit=poll_budget)) if poll_budget > 0 else []
    recheck_due = (
        list(subjects_due_for_recheck(conn, source=source, limit=recheck_budget)) if recheck_budget > 0 else []
    )

    for subject in poll_due:
        if subject.cik is None:
            # FINRA universe singleton — no submissions.json poll
            continue
        subjects_polled += 1
        recorded, errored = _probe_subject(conn, subject, http_get=http_get)
        new_filings_recorded += recorded
        if errored:
            poll_errors += 1

    for subject in recheck_due:
        if subject.cik is None:
            continue
        recheck_subjects_polled += 1
        recorded, errored = _probe_subject(conn, subject, http_get=http_get)
        recheck_new_filings_recorded += recorded
        if errored:
            poll_errors += 1

    logger.info(
        "per-cik poll: subjects=%d new_filings=%d errors=%d recheck_subjects=%d recheck_new_filings=%d",
        subjects_polled,
        new_filings_recorded,
        poll_errors,
        recheck_subjects_polled,
        recheck_new_filings_recorded,
    )
    return PerCikPollStats(
        subjects_polled=subjects_polled,
        new_filings_recorded=new_filings_recorded,
        poll_errors=poll_errors,
        recheck_subjects_polled=recheck_subjects_polled,
        recheck_new_filings_recorded=recheck_new_filings_recorded,
    )
