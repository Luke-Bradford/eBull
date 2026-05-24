"""Per-CIK scheduled polling — Layer 3 (#870).

Issue #870 / spec §"Layer 3 — Per-CIK submissions.json".

Hourly job that reads ``data_freshness_index`` for subjects whose
``expected_next_at`` has elapsed, calls ``check_freshness`` per CIK,
UPSERTs new manifest rows, and updates the scheduler outcome.

Layer 3 is the per-CIK reconcile path — fires only at predicted-next-
filing windows. AAPL's DEF 14A poll fires once a year; AAPL's 13F
poll never fires (issuer subject; AAPL doesn't file 13F).

Item 7 (#1233 ``docs/proposals/etl/run-8-readiness-fixes.md``):
when the caller supplies the richer ``http_get_with_meta`` callable
(see ``app/providers/implementations/sec_submissions.py:HttpGetWithMeta``),
this job rounds the SEC ``Last-Modified`` header through
``external_data_watermarks`` under source-key
``sec.last_modified.per_cik_poll`` and short-circuits on HTTP 304 —
skipping payload parse + scheduler UPSERT entirely while bumping
``watermark_at`` so the watermark row stays fresh.

Distinct source-key namespace from ``sec.submissions`` (which stores
top-accession at ``app/services/fundamentals/__init__.py:2030``) to
avoid corrupting two different fetch contracts.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import psycopg

from app.providers.implementations.sec_submissions import (
    HttpGet,
    HttpGetWithMeta,
    check_freshness,
    check_freshness_conditional,
)
from app.services.data_freshness import (
    FreshnessRow,
    cadence_for,
    record_poll_outcome,
    subjects_due_for_poll,
    subjects_due_for_recheck,
)
from app.services.sec_manifest import ManifestSource, record_manifest_entry
from app.services.watermarks import get_watermark, set_watermark

logger = logging.getLogger(__name__)


# Item 7 (#1233): dedicated source-key namespace for HTTP Last-Modified
# round-trip. MUST NOT collide with ``sec.submissions`` (top-accession
# semantics at ``app/services/fundamentals/__init__.py:2030``). See
# ``app/services/watermarks.py`` module docstring §Source-key
# namespaces in use.
_SOURCE_KEY_PER_CIK_POLL: str = "sec.last_modified.per_cik_poll"


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
    http_get: HttpGet | None = None,
    http_get_with_meta: HttpGetWithMeta | None = None,
) -> tuple[int, bool]:
    """Probe one subject. Returns ``(new_filings_recorded, errored)``.

    Centralises the per-subject body so the poll and recheck paths
    share identical fetch + UPSERT + outcome-write logic. The caller
    increments its own stat counters based on the return.

    Caller MUST ensure ``subject.cik is not None`` (FINRA universe
    singleton has no submissions.json to poll). Asserted defensively
    to narrow the type for ``check_freshness``.

    Item 7 (#1233): when ``http_get_with_meta`` is supplied, this
    function reads any prior ``sec.last_modified.per_cik_poll`` /
    ``<cik>`` watermark, sends it as ``If-Modified-Since``, and on
    HTTP 304 short-circuits without scheduler-outcome write (the
    payload didn't change so neither did the freshness state). On
    200 with a ``Last-Modified`` header the watermark is upserted
    inside the same transaction as the manifest writes so a crash
    mid-ingest cannot leave the watermark ahead of the data.

    Backwards compat: ``http_get`` legacy path is preserved for
    existing tests (``tests/test_sec_per_cik_poll.py``) that don't
    care about conditional-GET semantics. Exactly one of the two
    callables MUST be supplied.
    """
    if (http_get is None) == (http_get_with_meta is None):
        raise ValueError("_probe_subject requires exactly one of http_get / http_get_with_meta")
    assert subject.cik is not None, "_probe_subject requires non-None cik"
    sources_to_check: set[ManifestSource] | None = {subject.source} if subject.source else None
    cik_padded = subject.cik
    # CAVEMAN: read watermark BEFORE the fetch so we know whether to
    # inject If-Modified-Since. Watermark read is its own statement —
    # safe outside any open transaction at this point (the outer caller
    # opens a per-CIK ``with conn.transaction():`` only around the
    # writes below).
    if_modified_since: str | None = None
    if http_get_with_meta is not None:
        wm = get_watermark(conn, _SOURCE_KEY_PER_CIK_POLL, cik_padded)
        if_modified_since = wm.watermark if wm and wm.watermark else None
    try:
        if http_get_with_meta is not None:
            delta = check_freshness_conditional(
                http_get_with_meta,
                cik=subject.cik,
                last_known_filing_id=subject.last_known_filing_id,
                sources=sources_to_check,
                if_modified_since=if_modified_since,
            )
        else:
            assert http_get is not None  # narrowing for type checker
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

    # Item 7 (#1233): 304 short-circuit. Server says "nothing new
    # since your If-Modified-Since." Skip manifest writes (no new
    # filings) + bump watermark_at only (NOT watermark — the stored
    # Last-Modified is still the freshest the server has ever sent).
    # Scheduler outcome still writes ``current`` so expected_next_at
    # rolls forward and we don't re-poll this CIK immediately.
    if delta.not_modified:
        with conn.transaction():
            # CAVEMAN: re-stamp watermark_at by upserting the same
            # ``watermark`` string. set_watermark always touches
            # ``watermark_at`` via NOW() so we don't need a separate
            # UPDATE path — the upsert with identical watermark value
            # is the canonical "bump fetched_at" idiom for this
            # module.
            if if_modified_since is not None:
                set_watermark(
                    conn,
                    source=_SOURCE_KEY_PER_CIK_POLL,
                    key=cik_padded,
                    watermark=if_modified_since,
                    watermark_at=None,
                )
            # Scheduler outcome on 304: same logic as "200 with no new
            # filings" — current / never depending on prior state.
            if subject.state == "never_filed":
                outcome_304: str = "never"
                next_recheck_304: datetime | None = datetime.now(tz=UTC) + cadence_for(subject.source)
            else:
                outcome_304 = "current"
                next_recheck_304 = None
            record_poll_outcome(
                conn,
                subject_type=subject.subject_type,
                subject_id=subject.subject_id,
                source=subject.source,
                outcome=outcome_304,  # type: ignore[arg-type]
                last_known_filing_id=subject.last_known_filing_id,
                last_known_filed_at=subject.last_known_filed_at,
                new_filings_since=0,
                next_recheck_at=next_recheck_304,
                cik=subject.cik,
                instrument_id=subject.instrument_id,
            )
        return (0, False)

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

    # Item 7 (#1233): persist the fresh Last-Modified watermark. MUST
    # land in the same transaction as the manifest writes — set_watermark
    # asserts INTRANS. Only meaningful when the caller is on the
    # conditional path AND the server returned a Last-Modified header
    # (older SEC mirrors occasionally omit it; in that case skip
    # the upsert — next tick will refetch unconditionally).
    #
    # Codex 2 pre-push P1 fold 2026-05-24: gate the watermark write on
    # ``recorded == len(delta.new_filings)``. If ANY record_manifest_entry
    # raised ValueError above (caught + logged, not re-raised), the
    # accession was NOT persisted but ``last_known`` still advances at
    # line 246. Without this gate the next tick gets a 304 and the
    # unrecorded accession is hidden forever. Letting the watermark
    # stay stale forces a 200 re-fetch + retry. Retention-dropped
    # filings + new filings that all upserted cleanly still advance
    # the watermark (the common case).
    all_recorded = recorded == len(delta.new_filings)
    if http_get_with_meta is not None and delta.last_modified and all_recorded:
        with conn.transaction():
            set_watermark(
                conn,
                source=_SOURCE_KEY_PER_CIK_POLL,
                key=cik_padded,
                watermark=delta.last_modified,
                watermark_at=None,
            )
    return (recorded, False)


def run_per_cik_poll(
    conn: psycopg.Connection[Any],
    *,
    http_get: HttpGet | None = None,
    http_get_with_meta: HttpGetWithMeta | None = None,
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

    Item 7 (#1233): pass ``http_get_with_meta`` to enable conditional-
    GET via ``If-Modified-Since`` / ``Last-Modified`` watermarks
    (``sec.last_modified.per_cik_poll`` namespace). The scheduler
    invocation at ``app/workers/scheduler.py:_make_sec_http_get_with_meta``
    supplies it. The legacy ``http_get`` parameter remains for
    existing unit tests that fake a deterministic 200 body.
    """
    if (http_get is None) == (http_get_with_meta is None):
        raise ValueError("run_per_cik_poll requires exactly one of http_get / http_get_with_meta")
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
        recorded, errored = _probe_subject(
            conn,
            subject,
            http_get=http_get,
            http_get_with_meta=http_get_with_meta,
        )
        new_filings_recorded += recorded
        if errored:
            poll_errors += 1

    for subject in recheck_due:
        if subject.cik is None:
            continue
        recheck_subjects_polled += 1
        recorded, errored = _probe_subject(
            conn,
            subject,
            http_get=http_get,
            http_get_with_meta=http_get_with_meta,
        )
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
