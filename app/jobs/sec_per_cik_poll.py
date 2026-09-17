"""Per-CIK scheduled polling — Layer 3 (#870).

Issue #870 / spec §"Layer 3 — Per-CIK submissions.json".

Hourly job that reads ``data_freshness_index`` for subjects whose
``expected_next_at`` has elapsed, calls ``check_freshness`` per CIK,
UPSERTs new manifest rows, and updates the scheduler outcome.

Layer 3 is the per-CIK reconcile path — fires only at predicted-next-
filing windows. AAPL's DEF 14A poll fires once a year; AAPL's 13F
poll never fires (issuer subject; AAPL doesn't file 13F).

⚠⚠ **One fetch per CIK, not per (subject, source) triple** (#3109).
``submissions.json`` is an entity-wide response — SEC serves one
document carrying every form the entity filed, with no per-form
variant (`.claude/skills/data-sources/sec-edgar.md` §1). This job used
to fetch it once per due triple and discard every row outside that
one source, so a CIK with 9 due sources cost 9 identical responses.
The budget is therefore denominated in **CIKs** (``max_ciks``): the
same number of logical probes carries 1.92x the triples on the dev
corpus (66 probes → 127 triples across 66 CIKs, vs 66 triples across
48 CIKs before; ``scripts/measure_3109_batching.py``).

⚠ This does NOT fix the queue rotation the ticket is named for.
``record_poll_outcome`` derives ``expected_next_at`` from
``last_known_filed_at + cadence``, which for an inactive filer stays
in the past forever, so the head of the queue is reached more
completely — not replaced.

Item 7 (#1233 ``docs/proposals/etl/run-8-readiness-fixes.md``):
when the caller supplies the richer ``http_get_with_meta`` callable
(see ``app/providers/implementations/sec_submissions.py:HttpGetWithMeta``),
this job rounds the SEC ``Last-Modified`` header through
``external_data_watermarks`` under source-key
``sec.last_modified.per_cik_poll``, keyed ``<cik>:<source>`` (#3110 —
see ``_watermark_key``), and short-circuits on HTTP 304 —
skipping the manifest UPSERT + payload parse, but STILL writing a
scheduler outcome (``current``/``never``) and re-stamping
``fetched_at`` so the watermark row stays fresh. A 304 is a
budget-conserving success, not a noop.

⚠⚠ **It re-stamps ``fetched_at``, NOT ``watermark_at``** — four
docstring sites here claimed the latter and were wrong (#3109).
``set_watermark(..., watermark_at=None)`` binds NULL into
``watermark_at = EXCLUDED.watermark_at`` and sets ``fetched_at =
NOW()`` (``app/services/watermarks.py``). Nothing depended on the
false version; it was simply untrue in the place a reader trusts.

⚠ The whole conditional path is **inert in production**: #3110
measured that ``data.sec.gov`` returned no ``Last-Modified`` and no
``ETag`` on the submissions responses it probed, and 0 rows exist
under ``sec.last_modified.*`` across 1,949 runs. It is kept correct
because correctness does not depend on whether it fires.

Distinct source-key namespace from ``sec.submissions`` (which stores
top-accession at ``app/services/fundamentals/__init__.py:2030``) to
avoid corrupting two different fetch contracts.
"""

from __future__ import annotations

import logging
from collections.abc import Collection
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import psycopg

from app.providers.implementations.sec_submissions import (
    FreshnessDelta,
    HttpGet,
    HttpGetWithMeta,
    check_freshness,
    check_freshness_conditional,
    select_new_filings,
)
from app.services.data_freshness import (
    FreshnessRow,
    cadence_for,
    ciks_due_for_poll,
    ciks_due_for_recheck,
    record_poll_outcome,
)
from app.services.job_progress import JobProgress
from app.services.sec_manifest import ManifestSource, record_manifest_entry
from app.services.watermarks import get_watermark, set_watermark

logger = logging.getLogger(__name__)


# Item 7 (#1233): dedicated source-key namespace for HTTP Last-Modified
# round-trip. MUST NOT collide with ``sec.submissions`` (top-accession
# semantics at ``app/services/fundamentals/__init__.py:2030``). See
# ``app/services/watermarks.py`` module docstring §Source-key
# namespaces in use.
_SOURCE_KEY_PER_CIK_POLL: str = "sec.last_modified.per_cik_poll"


def _watermark_key(cik_padded: str, sources: Collection[ManifestSource] | None) -> str:
    """Watermark key for one probe — ``<cik>:<source>``, not ``<cik>`` (#3110).

    **The key must describe WHAT WAS PROCESSED, not what was fetched.**
    ``submissions.json`` is an entity-wide response (SEC's own API docs; see
    `.claude/skills/data-sources/sec-edgar.md` §1), but each subject's filings
    are taken out of it through its OWN source filter. Under the old CIK-only
    key, source A's poll stored the response validator and source B's next poll
    sent it as ``If-Modified-Since`` — so a 304 would certify B as current
    although B's filings had never been looked at by this path.

    ⚠⚠ **The invariant this docstring states is weaker than it reads, and
    #3109 measured why.** 963 duplicate ``(cik, source)`` row pairs exist, so
    SIBLING subjects with different accession watermarks already share one key.
    A validator stored under it therefore certifies "some subject with this
    (cik, source) was processed", not "this one was". That is why the batching
    path sends ``If-Modified-Since`` only for a single-subject batch, and why
    closing the hole properly is recorded on #3110 rather than here.

    ``<cik>:<page_name>`` is the existing convention for the sibling namespace
    ``sec.last_modified.submissions_files``; this mirrors it.

    ⚠⚠ Takes **the very object passed to the parser** (``sources_to_check``),
    not ``subject.source``, so the key and the filter provably cannot diverge —
    review NITPICK on PR #3130. The first cut derived the key separately and
    re-tested ``subject.source`` for truthiness; that is a second expression of
    one rule, which is how the two drift apart later.

    ``sources is None`` is not a fallback, it is the same rule: a probe with no
    filter processes the WHOLE response, so the CIK-wide key is the correct
    description of it. Unreachable today — ``data_freshness_index.source`` is
    ``NOT NULL`` with 0 null rows and ``FreshnessRow.source`` is non-optional.

    Sorted so a multi-source filter (none exists yet) yields a stable key
    rather than one that depends on set iteration order.
    """
    if sources is None:
        return cik_padded
    return f"{cik_padded}:{','.join(sorted(sources))}"


@dataclass(frozen=True)
class PerCikPollStats:
    subjects_polled: int
    new_filings_recorded: int
    poll_errors: int
    # #1155 G13 — separate counters for the recheck reader path
    # (ciks_due_for_recheck) so operator can confirm both reader
    # paths are draining. recheck_* counts subjects whose state was
    # 'never_filed' or 'error' at the start of the tick.
    recheck_subjects_polled: int = 0
    recheck_new_filings_recorded: int = 0
    # #3111 slice 6 — the counters the job's JobProgress is built from.
    #
    # ⚠⚠ THE TWO DENOMINATORS ARE DIFFERENT AND MUST NOT BE MIXED.
    #
    # ``poll_errors`` / ``recheck_poll_errors`` are SUBJECT-denominated: one
    # per due subject whose CIK fetch failed. They are subtracted from
    # ``subjects_polled`` / ``recheck_subjects_polled`` to give the ``polled``
    # outcome bucket, which is only sound because the two are the same unit.
    # ⚠ ``poll_errors`` was previously the COMBINED total for both lanes
    # (#3111 §7 recorded the missing split); it is now the poll lane alone.
    #
    # ``manifest_rejected`` is SUBJECT × ACCESSION write attempts: one subject
    # can reject many accessions, and two subjects of one CIK can each reject
    # the same accession. Never subtract it from a subject count, never sum it
    # with the two above — spec §10e.
    recheck_poll_errors: int = 0
    manifest_rejected: int = 0


def progress_for(stats: PerCikPollStats) -> JobProgress:
    """#2218 progress verdict for one tick — #3111 slice 6.

    ⚠⚠ **``polled`` is the outcome, NOT the discovery.** A CIK probed cleanly
    whose issuer has filed nothing is completed work, not a stall. Using
    ``new_filings_recorded`` here would have degraded **1,940 of this job's
    1,961 successful runs** (full population, ``job_runs`` 2026-06-05 →
    2026-09-17; spec §10c) — manufacturing exactly the alarm fatigue
    ``job_progress``'s module docstring exists to forbid: *"'Zero rows written'
    is NOT a degradation."*

    ⚠⚠ That rate is the PRE-BATCHING steady state and is temporarily false:
    #3109 landed 2026-09-17 and this job is draining a backlog at 1.92x triples
    per fetch, so 20 of its 21 runs since then DID discover filings. It reverts
    as the backlog drains. The choice does not depend on the rate — rule 2's
    premise is "produced no terminal outcome", so one legitimate no-discovery
    tick is enough to reject the discovery bucket, and every measured window
    contains them.

    ⚠ ``polled`` certifies **the probe stage** — the CIK's submissions response
    was obtained and applied to that subject. It does NOT certify that every
    downstream write for the subject succeeded, which is why
    ``manifest_rejected`` can be non-zero on a tick where ``polled == seen``.
    The paths on which a probe can succeed without doing the intended work are
    listed in ``docs/proposals/etl/2026-09-16-3109-per-cik-multi-source-batching.md``
    §4 and are deliberately NOT claimed as certified here.

    ⚠ Consequence, stated rather than hidden: ``degradation_reason``'s rule 2
    (saw candidates, produced no outcome) is UNREACHABLE for this job on a
    normal return, because ``polled`` is the complement of the subject-
    denominated errors. The effective verdict is rule 1 alone — any error
    bucket above zero. ``outcomes`` is here to keep rule 2 silent, which is its
    job; it is not a second layer of protection.

    Zero-valued error buckets are emitted rather than omitted: the verdict
    filters on ``n > 0`` so they are inert, and slice 5's read path
    distinguishes ``None`` (job does not report) from a measured zero.
    """
    seen = stats.subjects_polled + stats.recheck_subjects_polled
    return JobProgress(
        candidates_seen=seen,
        outcomes={"polled": seen - stats.poll_errors - stats.recheck_poll_errors},
        errors={
            "poll_fetch_failed": stats.poll_errors,
            "recheck_fetch_failed": stats.recheck_poll_errors,
            "manifest_rejected": stats.manifest_rejected,
        },
        context={
            "new_filings": stats.new_filings_recorded,
            "recheck_new_filings": stats.recheck_new_filings_recorded,
        },
    )


def _record_subject_error(conn: psycopg.Connection[Any], subject: FreshnessRow, exc: Exception) -> None:
    """Write the ``error`` scheduler outcome for one subject of a failed fetch."""
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


def _probe_cik(
    conn: psycopg.Connection[Any],
    subjects: list[FreshnessRow],
    *,
    http_get: HttpGet | None = None,
    http_get_with_meta: HttpGetWithMeta | None = None,
) -> tuple[int, int, int]:
    """Probe every due subject of ONE CIK with ONE fetch (#3109).

    Returns ``(new_filings_recorded, subjects_errored, manifest_rejected)``
    summed over the batch. ⚠ The last element is in a DIFFERENT unit from the
    other two — subject × accession write attempts, not subjects (#3111 §10e).

    ``subjects_errored`` is 0 or ``len(subjects)`` and nothing between: the
    fetch is entity-wide, so it either answered for the whole batch or for
    none of it. That is what makes ``polled = seen - errored`` well defined.

    Every element of ``subjects`` must share a zero-padded CIK — the
    CIK-grouped readers (``ciks_due_for_poll`` / ``ciks_due_for_recheck``)
    guarantee that, and it is re-checked here with a ``raise`` rather than an
    ``assert`` — this is the safeguard deciding which entity's URL is fetched,
    and ``python -O`` strips asserts.

    The response is fetched **unfiltered and untruncated**
    (``sources=None, last_known_filing_id=None``) because it is entity-wide;
    the per-triple half — source filter and accession watermark — is then
    applied per subject through ``select_new_filings``, the SAME function the
    provider's own ``check_freshness`` calls. A private copy of that rule
    here is exactly the drift the #3110 review nitpick warned about.

    ⚠⚠ **A fetch failure now fails EVERY subject of the CIK.** Before
    batching, three subjects on one CIK got three independent attempts and a
    transient failure cost one of them; now it costs all three, and they all
    land in ``error`` together. Accepted rather than claimed equivalent:
    ``ResilientClient`` already retries 429/5xx beneath this layer, and
    re-fetching per subject on failure would reinstate precisely the
    redundancy this change removes.

    **Write-failure boundary, unchanged:** each subject's writes stay in
    their own ``with conn.transaction():`` savepoint. An exception escaping
    one propagates out of ``run_per_cik_poll`` and ends the run — identical
    to the unbatched path, where a DB exception on the first subject also
    aborted before the rest were probed. No new catch is introduced here;
    adding one would change failure semantics under cover of a batching
    change.

    Item 7 (#1233) conditional GET, with #3110's key shape kept:

    * ``If-Modified-Since`` is sent **only when the batch holds exactly one
      subject**, which is byte-identical to the old per-subject probe. ⚠⚠ The
      tempting alternative — send it when every batched subject agrees on a
      validator — is unsound, because the ``<cik>:<source>`` key is SHARED by
      the 963 duplicate ``(cik, source)`` row pairs measured on 2026-09-16.
      Two subjects with different accession watermarks already read and write
      one key, so no batch-level agreement check can establish that each of
      them was processed. That hole predates batching and is recorded on
      #3109 §4 rather than papered over here.
    * A ``not_modified`` delta when no ``If-Modified-Since`` was sent is an
      ERROR, not a success. ``check_freshness_conditional`` takes the 304
      branch on status alone, so an unsolicited 304 would otherwise mark
      every batched subject ``current`` without any payload being examined.
    * The validator is persisted only on an actual 200. The 404 branch also
      returns a ``last_modified``, and ``recorded == len(new_filings)`` is
      trivially true at ``0 == 0``, so without the status check a 404 could
      store a validator.

    Backwards compat: the legacy ``http_get`` path is preserved for tests
    that do not care about conditional-GET semantics. Exactly one of the two
    callables MUST be supplied.
    """
    if (http_get is None) == (http_get_with_meta is None):
        raise ValueError("_probe_cik requires exactly one of http_get / http_get_with_meta")
    if not subjects:
        return (0, 0, 0)
    cik_padded = subjects[0].cik
    # ⚠ NOT ``assert`` — this is the safeguard deciding WHICH ENTITY'S URL gets
    # fetched, and ``python -O`` strips asserts. Under ``-O`` a mis-grouped
    # batch would silently fetch one CIK's submissions and write every other
    # subject's scheduler outcome from it. Review NITPICK on PR #3134; same
    # shape as the #3104 bot WARNING about an ``assert`` that kept an
    # accounting equality summing under ``-O``.
    if cik_padded is None or any(s.cik != cik_padded for s in subjects):
        # Bound to a local rather than inlined: a set comprehension inside an
        # f-string needs wrapping parens, which ``ruff format`` then pads to
        # ``{ (...)!r}``. Not sorted — the offending set can contain None,
        # which is unorderable against str, and a crash inside the error path
        # would replace a diagnosable failure with an opaque one.
        found_ciks = {s.cik for s in subjects}
        raise ValueError(f"_probe_cik requires exactly one non-None CIK per batch, got {found_ciks!r}")

    # Read the watermark BEFORE the fetch so we know whether to inject
    # If-Modified-Since. Single-subject batches only — see the docstring.
    if_modified_since: str | None = None
    solo_watermark_key: str | None = None
    if http_get_with_meta is not None and len(subjects) == 1:
        solo_watermark_key = _watermark_key(cik_padded, {subjects[0].source} if subjects[0].source else None)
        wm = get_watermark(conn, _SOURCE_KEY_PER_CIK_POLL, solo_watermark_key)
        if_modified_since = wm.watermark if wm and wm.watermark else None

    try:
        if http_get_with_meta is not None:
            delta = check_freshness_conditional(
                http_get_with_meta,
                cik=cik_padded,
                last_known_filing_id=None,
                sources=None,
                if_modified_since=if_modified_since,
            )
        else:
            assert http_get is not None  # narrowing for type checker
            delta = check_freshness(
                http_get,
                cik=cik_padded,
                last_known_filing_id=None,
                sources=None,
            )
        if delta.not_modified and if_modified_since is None:
            # Unsolicited 304. Certifying the batch off this would mean
            # marking subjects current from a response nobody looked at.
            raise RuntimeError(f"submissions.json returned 304 without a conditional request: cik={cik_padded}")
    except Exception as exc:
        logger.warning(
            "per-cik poll: fetch failed for cik=%s subjects=%d sources=%s: %s",
            cik_padded,
            len(subjects),
            ",".join(sorted(s.source for s in subjects)),
            exc,
        )
        for subject in subjects:
            _record_subject_error(conn, subject, exc)
        # Every subject of this batch is errored: the fetch is entity-wide, so
        # no subject of it was probed. This is the ONLY site that produces a
        # non-zero subject-error count, which is what keeps ``poll_errors``
        # subject-denominated and therefore subtractable (spec §10d).
        return (0, len(subjects), 0)

    recorded_total = 0
    rejected_total = 0
    for subject in subjects:
        recorded, rejected = _apply_delta_to_subject(
            conn,
            subject,
            delta,
            conditional=http_get_with_meta is not None,
            if_modified_since=if_modified_since,
            solo_watermark_key=solo_watermark_key,
        )
        recorded_total += recorded
        rejected_total += rejected
    return (recorded_total, 0, rejected_total)


def _apply_delta_to_subject(
    conn: psycopg.Connection[Any],
    subject: FreshnessRow,
    delta: FreshnessDelta,
    *,
    conditional: bool,
    if_modified_since: str | None,
    solo_watermark_key: str | None,
) -> tuple[int, int]:
    """Apply one CIK-wide response to one subject. Returns ``(recorded, rejected)``.

    The source filter and the accession watermark are this subject's own, so
    the result is identical to what the unbatched probe produced for it given
    the same response bytes.

    ``rejected`` (#3111 slice 6) counts ``record_manifest_entry`` calls this
    subject made that raised ``ValueError`` — previously logged and dropped, so
    a subject whose every accession was rejected was indistinguishable from one
    with nothing new. ⚠ It is an ATTEMPT count, not a distinct-accession count:
    see ``PerCikPollStats.manifest_rejected``.
    """
    # Pure type narrowing, not a safeguard: ``_probe_cik`` has already raised
    # if any batch member's cik is None, and this function is only reachable
    # from there. Left as an ``assert`` deliberately — under ``-O`` it narrows
    # nothing and costs nothing, because the real check upstream is a raise.
    assert subject.cik is not None, "_apply_delta_to_subject requires non-None cik"

    # Item 7 (#1233): 304 short-circuit. Server says "nothing new since your
    # If-Modified-Since." Skip manifest writes + bump fetched_at only (NOT
    # ``watermark`` — the stored Last-Modified is still the freshest the
    # server has ever sent). Scheduler outcome still writes ``current`` so
    # ``expected_next_at`` rolls forward and we don't re-poll immediately.
    if delta.not_modified:
        with conn.transaction():
            # Re-stamp by upserting the same ``watermark`` string:
            # ``set_watermark`` always sets ``fetched_at = NOW()``, so no
            # separate UPDATE path is needed. ⚠ It does NOT touch
            # ``watermark_at`` — passing ``watermark_at=None`` binds NULL
            # into that column. Four docstrings in this module used to claim
            # otherwise (#3109).
            if if_modified_since is not None and solo_watermark_key is not None:
                set_watermark(
                    conn,
                    source=_SOURCE_KEY_PER_CIK_POLL,
                    key=solo_watermark_key,
                    watermark=if_modified_since,
                    watermark_at=None,
                )
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
        return (0, 0)

    # The per-triple half: THIS subject's source filter and THIS subject's
    # accession watermark, applied to the shared CIK-wide parse.
    new_filings, last_filed_at = select_new_filings(
        delta.new_filings,
        sources={subject.source} if subject.source else None,
        last_known_filing_id=subject.last_known_filing_id,
    )

    # UPSERT manifest rows for the new filings
    recorded = 0
    rejected = 0
    for row in new_filings:
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
            # #3111 slice 6 — counted as well as logged. Swallowing stays
            # correct (one bad accession must not abandon the CIK's other
            # subjects), but the tick now degrades on it.
            rejected += 1
            logger.warning("per-cik poll: rejected accession=%s: %s", row.accession_number, exc)

    # Update scheduler outcome. #1155 G13 — never_filed rows that
    # return no new filings must STAY in the recheck queue (state
    # never_filed) rather than transitioning to 'current'. Otherwise
    # the recheck path's whole point is defeated — every never_filed
    # row leaves the recheck lane on its first poll regardless of
    # whether the subject actually filed.
    outcome: str
    next_recheck_at: datetime | None = None
    if new_filings:
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

    last_known = new_filings[0].accession_number if new_filings else subject.last_known_filing_id
    last_filed = last_filed_at if last_filed_at else subject.last_known_filed_at
    record_poll_outcome(
        conn,
        subject_type=subject.subject_type,
        subject_id=subject.subject_id,
        source=subject.source,
        outcome=outcome,  # type: ignore[arg-type]
        last_known_filing_id=last_known,
        last_known_filed_at=last_filed,
        new_filings_since=len(new_filings),
        next_recheck_at=next_recheck_at,
        cik=subject.cik,
        instrument_id=subject.instrument_id,
    )

    # Item 7 (#1233): persist the fresh Last-Modified watermark. MUST
    # land in the same transaction as the manifest writes — set_watermark
    # asserts INTRANS.
    #
    # Codex 2 pre-push P1 fold 2026-05-24: gate the watermark write on
    # ``recorded == len(new_filings)``. If ANY record_manifest_entry
    # raised ValueError above (caught + logged, not re-raised), the
    # accession was NOT persisted but ``last_known`` still advances.
    # Without this gate the next tick gets a 304 and the unrecorded
    # accession is hidden forever. Letting the watermark stay stale
    # forces a 200 re-fetch.
    #
    # ⚠⚠ **The re-fetch is NOT a retry, and an earlier version of this
    # comment said it was** (#3111 slice 6, Codex ckpt-1). ``last_known``
    # advanced at ``record_poll_outcome`` above regardless of whether any
    # write succeeded, so ``select_new_filings`` truncates the rejected
    # accession out of the next response and it is never seen again. This
    # gate protects only the HTTP validator. The loss is real, pre-existing
    # and recorded as NOT fixed in
    # ``docs/proposals/etl/2026-09-16-3109-per-cik-multi-source-batching.md``
    # §4.2, with the reason it is not a one-liner: gating the freshness
    # watermark too would wedge the row forever on a permanently-rejectable
    # accession. What slice 6 adds is only that the tick DEGRADES when it
    # happens instead of reporting a clean success.
    #
    # ⚠ ``solo_watermark_key`` is None for a multi-subject batch, which is
    # what withholds the write there: no If-Modified-Since was sent, so
    # storing a validator would advertise a certification this run never
    # performed for the other subjects sharing that key.
    all_recorded = recorded == len(new_filings)
    if conditional and delta.last_modified and all_recorded and solo_watermark_key is not None:
        with conn.transaction():
            set_watermark(
                conn,
                source=_SOURCE_KEY_PER_CIK_POLL,
                key=solo_watermark_key,
                watermark=delta.last_modified,
                watermark_at=None,
            )
    return (recorded, rejected)


def _triples_of(batches: list[list[FreshnessRow]]) -> set[tuple[str, str, str]]:
    """The ``(subject_type, subject_id, source)`` primary keys in ``batches``."""
    return {(r.subject_type, r.subject_id, r.source) for batch in batches for r in batch}


def _drop_claimed(
    batches: list[list[FreshnessRow]],
    *,
    claimed: set[tuple[str, str, str]],
) -> list[list[FreshnessRow]]:
    """Remove rows already claimed by the other lane, dropping empty batches.

    A batch that loses every member is dropped entirely rather than left as an
    empty list — ``_probe_cik`` would otherwise be handed a batch with no CIK
    to fetch, and ``len(poll_due)`` is the job's reported CIK-fetch count.
    """
    if not claimed:
        return batches
    kept: list[list[FreshnessRow]] = []
    for batch in batches:
        survivors = [r for r in batch if (r.subject_type, r.subject_id, r.source) not in claimed]
        if survivors:
            kept.append(survivors)
    return kept


def run_per_cik_poll(
    conn: psycopg.Connection[Any],
    *,
    http_get: HttpGet | None = None,
    http_get_with_meta: HttpGetWithMeta | None = None,
    source: ManifestSource | None = None,
    max_ciks: int = 100,
) -> PerCikPollStats:
    """One per-CIK poll cycle. For each CIK due, call submissions.json ONCE
    and UPSERT manifest + scheduler for every due subject of that CIK.

    Drains BOTH reader paths (#1155 G13):

    * ``ciks_due_for_poll`` — 'unknown' / 'current' /
      'expected_filing_overdue' rows past their ``next_poll_at`` (#3109: the
      poll-eligibility clock, NOT the filing-derived ``expected_next_at``).
      Gets the dominant budget share so steady-state polls are never starved
      by error backlog, plus whatever the recheck lane leaves unspent.
    * ``ciks_due_for_recheck`` — 'never_filed' / 'error' rows past their
      ``next_recheck_at``. Selected FIRST, capped at the remaining ~1/3, so
      the recheck path drains at a guaranteed rate.

    ⚠⚠ **The budget is denominated in CIKs, not subjects** (#3109, renamed
    from ``max_subjects``). ``submissions.json`` is entity-wide, so one
    fetch answers every due source of that CIK; the previous unit spent one
    identical fetch per ``(subject, source)`` triple. Total FETCHES never
    exceeds ``max_ciks``; the number of SUBJECTS processed is now
    unbounded by it — measured 1.92x on the dev corpus, max 8 per CIK
    (``scripts/measure_3109_batching.py``).

    For ``max_ciks=100`` → recheck capped at 34, poll gets ``100 - len(recheck)``
    — so 100 when the recheck lane is empty (its state on dev), 66 when it is
    full. For ``max_ciks=1`` → ``recheck=1, poll=0`` (degenerate but bounded).

    ⚠ Both lanes are read into lists BEFORE any write. Selecting rechecks
    after polling would immediately re-select rows the poll lane had just
    failed — which used to mean instantly, because a failed poll wrote a NULL
    ``next_recheck_at``. #3109 gave errors a finite ``ERROR_RECHECK_INTERVAL``
    backoff, so that is no longer true; the read-before-write ordering is kept
    anyway because it is the simpler invariant.

    ⚠ The lanes are NOT merged: a CIK with rows due in both costs two
    fetches, exactly as it did when they were separate probes. Merging them
    would change the budget contract the G13 split exists to enforce.

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
    # recheck. No max(1, ...) floor so max_ciks=1 stays bounded
    # at total=1 (poll=0, recheck=1).
    #
    # #3109 — the recheck lane's share is a CAP, not a reservation. It is
    # selected FIRST and the poll lane gets the residual, because the recheck
    # lane is usually empty (0 rows on dev, 2026-09-16) and 34 of 100 slots
    # per hour were buying nothing. G13's guarantee is untouched: recheck
    # still gets up to its full share and is still read before any write.
    #
    # ⚠ The rollover is ONE-WAY by design. Spare poll capacity is not returned
    # to recheck: with 15,483 CIKs in the poll lane against 0 in recheck, the
    # useful direction is the only one implemented, and the symmetric case
    # would need a second SELECT for a situation the measurement says does not
    # arise. ⚠ At max_ciks=1 with recheck work pending the poll lane still gets
    # 0 — degenerate but bounded, exactly as before.
    recheck_budget = max_ciks - (max_ciks * 2 // 3)

    subjects_polled = 0
    new_filings_recorded = 0
    poll_errors = 0
    recheck_subjects_polled = 0
    recheck_new_filings_recorded = 0
    # #3111 slice 6 — the recheck lane's errors used to fold into
    # ``poll_errors``, which made a per-lane success count uncomputable.
    recheck_poll_errors = 0
    manifest_rejected = 0

    recheck_due = ciks_due_for_recheck(conn, source=source, limit=recheck_budget) if recheck_budget > 0 else []
    poll_budget = max_ciks - len(recheck_due)
    poll_due = ciks_due_for_poll(conn, source=source, limit=poll_budget) if poll_budget > 0 else []

    # ⚠ The two lanes are separate statements under READ COMMITTED, so a
    # concurrent discovery producer can promote a row from 'never_filed' to
    # 'current' BETWEEN them and the same triple lands in both lists. That
    # matters because ``new_filings_since`` is additive in the UPSERT
    # (``data_freshness.py``: ``new_filings_since + EXCLUDED.new_filings_since``),
    # so applying one response twice double-counts it. Drop the duplicates from
    # the poll lane — the recheck lane already holds its claim, and its budget
    # was the one guaranteed.
    poll_due = _drop_claimed(poll_due, claimed=_triples_of(recheck_due))

    for batch in poll_due:
        subjects_polled += len(batch)
        recorded, errored, rejected = _probe_cik(
            conn,
            batch,
            http_get=http_get,
            http_get_with_meta=http_get_with_meta,
        )
        new_filings_recorded += recorded
        poll_errors += errored
        manifest_rejected += rejected

    for batch in recheck_due:
        recheck_subjects_polled += len(batch)
        recorded, errored, rejected = _probe_cik(
            conn,
            batch,
            http_get=http_get,
            http_get_with_meta=http_get_with_meta,
        )
        recheck_new_filings_recorded += recorded
        recheck_poll_errors += errored
        manifest_rejected += rejected

    logger.info(
        "per-cik poll: ciks=%d subjects=%d new_filings=%d errors=%d "
        "recheck_ciks=%d recheck_subjects=%d recheck_new_filings=%d "
        "recheck_errors=%d manifest_rejected=%d",
        len(poll_due),
        subjects_polled,
        new_filings_recorded,
        poll_errors,
        len(recheck_due),
        recheck_subjects_polled,
        recheck_new_filings_recorded,
        recheck_poll_errors,
        manifest_rejected,
    )
    return PerCikPollStats(
        subjects_polled=subjects_polled,
        new_filings_recorded=new_filings_recorded,
        poll_errors=poll_errors,
        recheck_subjects_polled=recheck_subjects_polled,
        recheck_new_filings_recorded=recheck_new_filings_recorded,
        recheck_poll_errors=recheck_poll_errors,
        manifest_rejected=manifest_rejected,
    )
