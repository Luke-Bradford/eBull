"""SEC change-driven fetch planner + executor (issue #272).

Replaces the 45-minute full-pull in ``daily_financial_facts`` with a
two-phase flow:

    plan_refresh(conn, provider, today)
        -> RefreshPlan { seeds, refreshes, submissions_only_advances }

    execute_refresh(conn, ..., plan, ...)   # Task 5
        -> RefreshOutcome

The planner fetches a 7-day window of SEC daily master-index files with
conditional GET, intersects filings with our covered-US cohort, and
compares each hit's top accession to a per-CIK watermark. Only CIKs
with genuinely new fundamentals filings (10-K / 10-Q / 20-F family)
land in ``refreshes``. CIKs with only non-fundamentals filings (8-K etc.)
advance the submissions watermark alone — no companyfacts pull.

A new covered CIK (fresh install or newly promoted ticker) has no
watermark row and is placed in ``seeds`` for a full initial backfill.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from typing import TYPE_CHECKING

import psycopg

from app.providers.implementations.sec_edgar import (
    MasterIndexEntry,
    SecFilingsProvider,
    parse_master_index,
)
from app.services.financial_facts import (
    finish_ingestion_run,
    start_ingestion_run,
    upsert_facts_for_instrument,
)
from app.services.sync_orchestrator.progress import report_progress
from app.services.watermarks import get_watermark, set_watermark

if TYPE_CHECKING:
    from app.providers.implementations.sec_fundamentals import SecFundamentalsProvider

logger = logging.getLogger(__name__)

LOOKBACK_DAYS = 7

# 6-K (foreign-private-issuer interim reports) is deliberately
# excluded — typically lacks structured XBRL, so refreshing
# companyfacts on 6-K yields no new fundamentals rows.
FUNDAMENTALS_FORMS: frozenset[str] = frozenset(
    {
        "10-K",
        "10-K/A",
        "10-Q",
        "10-Q/A",
        "20-F",
        "20-F/A",
        "40-F",
        "40-F/A",
    }
)


@dataclass(frozen=True)
class RefreshPlan:
    """One run's worth of work for ``daily_financial_facts``.

    - ``seeds`` — CIKs with no prior watermark row; full backfill.
    - ``refreshes`` — CIKs that filed a fundamentals form in the window
      with an accession newer than the stored watermark.
    - ``submissions_only_advances`` — CIKs that filed a non-fundamentals
      form (e.g. 8-K). Advance ``sec.submissions`` watermark only; no
      companyfacts pull.
    - ``pending_master_index_writes`` — per-day master-index watermarks
      that the planner parsed but has NOT yet committed. The executor
      commits each one only when every covered CIK whose filing appeared
      in that day's hits has been processed successfully. A failed CIK
      leaves its day's watermark un-advanced so the next run re-fetches
      the master-index on 200, re-parses, and re-plans the failed CIK.
    - ``ciks_by_day`` — ISO-date to list-of-hit-CIKs mapping used by
      the executor to decide which pending master-index writes are safe
      to commit.
    - ``new_filings_by_cik`` — per-CIK list of master-index entries
      that landed in this cycle. Populated for every covered CIK that
      had at least one master-index hit in the 7-day window (including
      seeds that happened to file this week). The executor only
      consumes this dict on the refresh + submissions-only paths; the
      seed path ignores it because seeds need full historical backfill
      (#268 Chunk E), not just this week's entries.
    """

    seeds: list[str] = field(default_factory=list)
    # refreshes carries (cik, top_accession) so the executor reuses
    # the accession the planner already fetched — no second
    # submissions.json request per refresh CIK.
    refreshes: list[tuple[str, str]] = field(default_factory=list)
    submissions_only_advances: list[tuple[str, str]] = field(default_factory=list)
    pending_master_index_writes: list[tuple[str, str, str]] = field(default_factory=list)
    ciks_by_day: dict[str, list[str]] = field(default_factory=dict)
    new_filings_by_cik: dict[str, list[MasterIndexEntry]] = field(default_factory=dict)
    # CIKs skipped during planning itself (fetch_submissions returned None
    # or filings.recent was empty). These never make it to
    # seeds/refreshes/submissions_only_advances, so the executor's
    # ``failed`` list does not capture them — but their master-index
    # day must still withhold. Executor unions this set into its
    # commit-gate so planner-phase transient skips block the watermark
    # advance exactly like executor-phase failures do.
    failed_plan_ciks: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class RefreshOutcome:
    """Per-category counters + per-CIK failure list for one run.

    ``failed`` is ``list[(cik, exception_class_name)]`` — a CIK appears
    here iff its per-CIK transaction was rolled back. Successful CIKs
    do not appear regardless of category.
    """

    seeded: int = 0
    refreshed: int = 0
    submissions_advanced: int = 0
    failed: list[tuple[str, str]] = field(default_factory=list)


def _load_covered_us_ciks(conn: psycopg.Connection[tuple]) -> list[str]:
    cur = conn.execute(
        """
        SELECT ei.identifier_value
        FROM instruments i
        JOIN external_identifiers ei
            ON ei.instrument_id = i.instrument_id
            AND ei.provider = 'sec'
            AND ei.identifier_type = 'cik'
            AND ei.is_primary = TRUE
        WHERE i.is_tradable = TRUE
        ORDER BY ei.identifier_value
        """
    )
    return [row[0] for row in cur.fetchall()]


def _lookback_dates(today: date) -> list[date]:
    return [today - timedelta(days=i) for i in range(LOOKBACK_DAYS)]


def _top_accession_from_submissions(
    submissions: dict[str, object],
) -> str | None:
    """Return the top accession number or None for empty submissions."""
    filings_block = submissions.get("filings")
    if not isinstance(filings_block, dict):
        return None
    recent = filings_block.get("recent")
    if not isinstance(recent, dict):
        return None
    accessions = recent.get("accessionNumber") or []
    if not accessions:
        return None
    return str(accessions[0])


def plan_refresh(
    conn: psycopg.Connection[tuple],
    provider: SecFilingsProvider,
    *,
    today: date,
) -> RefreshPlan:
    """Derive the work for a single daily_financial_facts run.

    Steps:

    1. Load covered-US CIKs (tradable instruments with a primary
       ``sec.cik`` external identifier).
    2. Fetch the 7-day master-index window with conditional GET. Each
       day has its own ``sec.master-index`` watermark keyed by ISO
       date. 304 and 404 both short-circuit (no watermark write).
    3. Intersect the master-index hits with the covered cohort.
    4. For each covered CIK, compare against its ``sec.submissions``
       watermark and bucket into seeds / refreshes /
       submissions_only_advances.

    The planner is pure (no data writes except watermark rows on the
    master-index). Actual companyfacts pulls happen in Task 5's
    ``execute_refresh``.
    """
    covered = _load_covered_us_ciks(conn)
    if not covered:
        return RefreshPlan()

    master_hits_by_cik: dict[str, list[MasterIndexEntry]] = {}
    # Per-day provenance so the executor can commit master-index
    # watermarks only when every CIK hit on that day was processed.
    ciks_by_day: dict[str, set[str]] = {}
    pending_master_index_writes: list[tuple[str, str, str]] = []

    for target in _lookback_dates(today):
        wm = get_watermark(conn, "sec.master-index", target.isoformat())
        if_modified_since = wm.watermark if wm else None
        result = provider.fetch_master_index(target, if_modified_since=if_modified_since)
        if result is None:
            # 304 Not Modified OR 404 (weekend / holiday): nothing to
            # parse, and no Last-Modified to persist on 404. The 304
            # path is safe — the stored watermark is still the correct
            # ``If-Modified-Since`` for next run.
            continue

        if wm is not None and wm.response_hash == result.body_hash:
            # Body identical to the last run but without a 304 —
            # watermark + hash are unchanged so no commit is required.
            # Skip re-parsing; next run still has a valid watermark.
            continue

        entries = parse_master_index(result.body)
        day_ciks: set[str] = set()
        for entry in entries:
            master_hits_by_cik.setdefault(entry.cik, []).append(entry)
            day_ciks.add(entry.cik)

        # Capture the watermark write as pending — executor commits it
        # only if every covered CIK on this day completes successfully.
        # A mid-run failure leaves the watermark un-advanced so the next
        # run re-fetches this day's master-index (200), re-parses, and
        # re-plans the missed CIK instead of 304-skipping it forever.
        iso = target.isoformat()
        pending_master_index_writes.append((iso, result.last_modified or "", result.body_hash))
        ciks_by_day[iso] = day_ciks

    seeds: list[str] = []
    refreshes: list[tuple[str, str]] = []
    submissions_only: list[tuple[str, str]] = []
    failed_plan_ciks: list[str] = []

    covered_set = set(covered)
    # Drop hits outside the cohort before the per-CIK loop so we never
    # issue a submissions fetch for a rogue master-index entry. The
    # ``.get(cik)`` lookup below would implicitly filter anyway, but
    # an explicit intersect documents intent.
    master_hits_by_cik = {cik: entries for cik, entries in master_hits_by_cik.items() if cik in covered_set}
    # Restrict per-day cohort tracking to covered CIKs too — the
    # executor's commit-if-all-succeeded check only cares about CIKs
    # that were actually planned this run.
    ciks_by_day_filtered: dict[str, list[str]] = {iso: sorted(ciks & covered_set) for iso, ciks in ciks_by_day.items()}

    for cik in covered:
        wm = get_watermark(conn, "sec.submissions", cik)
        if wm is None:
            seeds.append(cik)
            continue

        entries = master_hits_by_cik.get(cik)
        if not entries:
            continue

        submissions = provider.fetch_submissions(cik)
        if submissions is None:
            # Transient planner-phase skip — feed into failed_plan_ciks
            # so the executor's commit-gate withholds this day's
            # master-index watermark. Without this, the day would
            # commit, the next run would 304, and this CIK would be
            # permanently skipped.
            logger.warning(
                "plan_refresh: fetch_submissions returned None for cik=%s "
                "despite master-index hit — withholding master-index watermark",
                cik,
            )
            failed_plan_ciks.append(cik)
            continue
        top_accession = _top_accession_from_submissions(submissions)
        if top_accession is None:
            logger.warning(
                "plan_refresh: submissions.json for cik=%s has empty filings.recent "
                "despite master-index hit — withholding master-index watermark",
                cik,
            )
            failed_plan_ciks.append(cik)
            continue
        if top_accession == wm.watermark:
            # Amendment or re-listing of a filing we already have.
            continue

        hit_forms = {e.form_type for e in entries}
        if hit_forms & FUNDAMENTALS_FORMS:
            refreshes.append((cik, top_accession))
        else:
            submissions_only.append((cik, top_accession))

    return RefreshPlan(
        seeds=sorted(seeds),
        refreshes=sorted(refreshes),
        submissions_only_advances=sorted(submissions_only),
        pending_master_index_writes=pending_master_index_writes,
        ciks_by_day=ciks_by_day_filtered,
        failed_plan_ciks=sorted(failed_plan_ciks),
        # master_hits_by_cik has already been intersected with the
        # covered cohort above; pass it through so the executor can
        # upsert each hit into filing_events.
        new_filings_by_cik=master_hits_by_cik,
    )


def _instrument_for_cik(
    conn: psycopg.Connection[tuple],
    cik: str,
) -> tuple[int, str] | None:
    """Resolve a CIK to (instrument_id, symbol) via external_identifiers.

    Returns None if no tradable instrument has a primary sec.cik
    identifier for this CIK. A non-None result guarantees the
    instrument is currently tradable.
    """
    row = conn.execute(
        """
        SELECT i.instrument_id, i.symbol
        FROM instruments i
        JOIN external_identifiers ei
            ON ei.instrument_id = i.instrument_id
            AND ei.provider = 'sec'
            AND ei.identifier_type = 'cik'
            AND ei.identifier_value = %s
            AND ei.is_primary = TRUE
        WHERE i.is_tradable = TRUE
        """,
        (cik,),
    ).fetchone()
    if row is None:
        return None
    return int(row[0]), str(row[1])


def _upsert_filing_from_master_index(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
    entry: MasterIndexEntry,
    symbol: str,
) -> None:
    """Upsert a filing_events row from a master-index entry.

    Distinct from ``filings._upsert_filing`` on the ON CONFLICT path:
    when the row already exists, we DO NOT overwrite ``primary_document_url``
    or ``source_url``. Master-index only carries the generic
    ``{accession}-index.htm`` landing page, whereas the submissions-
    based ingest (``daily_research_refresh``) stores the specific
    primary document (e.g. ``aapl-20260330.htm``). A master-index
    upsert arriving after the richer ingest must not downgrade the URL.
    COALESCE preserves the existing value unless it is NULL.

    ``filing_date`` and ``filing_type`` still refresh on conflict —
    both are authoritative from either source and carry no loss-of-
    detail risk.
    """
    accession_no_dashes = entry.accession_number.replace("-", "")
    cik_int = int(entry.cik)
    master_index_url = (
        f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{accession_no_dashes}/{entry.accession_number}-index.htm"
    )
    try:
        filed_at = datetime.fromisoformat(entry.date_filed).replace(tzinfo=UTC)
    except ValueError:
        # Master-index dates are always ISO; ValueError here indicates
        # corrupt data. Log loudly so operators can investigate rather
        # than silently substituting now().
        logger.warning(
            "sec_incremental: malformed date_filed %r for accession %s (cik=%s) — "
            "falling back to now() so upsert proceeds",
            entry.date_filed,
            entry.accession_number,
            entry.cik,
        )
        filed_at = datetime.now(UTC)
    conn.execute(
        """
        INSERT INTO filing_events (
            instrument_id, filing_date, filing_type,
            provider, provider_filing_id, source_url, primary_document_url,
            raw_payload_json
        )
        VALUES (
            %(instrument_id)s, %(filing_date)s, %(filing_type)s,
            %(provider)s, %(provider_filing_id)s, %(source_url)s, %(primary_document_url)s,
            %(raw_payload_json)s
        )
        ON CONFLICT (provider, provider_filing_id) DO UPDATE SET
            filing_date          = EXCLUDED.filing_date,
            filing_type          = EXCLUDED.filing_type,
            source_url           = COALESCE(filing_events.source_url, EXCLUDED.source_url),
            primary_document_url = COALESCE(filing_events.primary_document_url, EXCLUDED.primary_document_url)
        """,
        {
            "instrument_id": instrument_id,
            "filing_date": filed_at.date(),
            "filing_type": entry.form_type,
            "provider": "sec",
            "provider_filing_id": entry.accession_number,
            "source_url": master_index_url,
            "primary_document_url": master_index_url,
            "raw_payload_json": json.dumps(
                {
                    "source": "master-index",
                    "provider_filing_id": entry.accession_number,
                    "symbol": symbol,
                    "filed_at": filed_at.isoformat(),
                    "filing_type": entry.form_type,
                    "company_name": entry.company_name,
                    "date_filed": entry.date_filed,
                }
            ),
        },
    )


def _run_cik_upsert(
    conn: psycopg.Connection[tuple],
    *,
    cik: str,
    filings_provider: SecFilingsProvider,
    fundamentals_provider: SecFundamentalsProvider,
    run_id: int,
    failed: list[tuple[str, str]],
    known_top_accession: str | None = None,
    new_filings: list[MasterIndexEntry] | None = None,
) -> int | None:
    """Per-CIK seed/refresh body.

    ``known_top_accession`` lets callers pass an accession the planner
    already fetched — avoids a second ``fetch_submissions`` call on the
    refresh path (planner fetches to decide refresh vs submissions-only;
    executor would otherwise re-fetch to read the top accession again).
    Seeds pass ``None`` because the planner has no prior watermark and
    doesn't call ``fetch_submissions`` for them.

    Returns the number of fact rows upserted on success (``int >= 0``)
    or ``None`` on skip or failure. Failures additionally append
    ``(cik, ExceptionName)`` to ``failed``; skips do not.

    All writes for one CIK happen inside one ``with conn.transaction()``
    block so on exception the facts upsert AND both watermark writes
    roll back together — watermarks never drift ahead of data.
    """
    try:
        inst = _instrument_for_cik(conn, cik)
        if inst is None:
            # Plan-time drift: CIK was covered during planning but no
            # longer resolves to a tradable instrument. Record as a
            # failure so the master-index watermark for this CIK's day
            # is WITHHELD — a future run re-checks after universe
            # reconciliation rather than 304-skipping forever.
            logger.warning(
                "sec_incremental: no tradable instrument found for cik=%s (plan drift?)",
                cik,
            )
            failed.append((cik, "InstrumentMissing"))
            return None
        instrument_id, symbol = inst
        # Close the implicit read transaction opened by _instrument_for_cik
        # before the HTTP calls below so the session is not idle-in-
        # transaction for multi-second windows × hundreds of CIKs.
        conn.commit()

        # Skip the second fetch_submissions round-trip if the planner
        # already captured the top accession (refresh / submissions-only
        # paths). Seeds have no prior watermark, so the planner never
        # fetched for them — executor still fetches once.
        if known_top_accession is not None:
            top_accession: str | None = known_top_accession
        else:
            submissions = filings_provider.fetch_submissions(cik)
            if submissions is None:
                # Transient: submissions endpoint unavailable (404 on a
                # CIK the master-index says filed today — private /
                # de-registered issuer, or a provider glitch). Record as
                # failure so the master-index watermark for this day is
                # NOT committed and the next run re-fetches + re-plans.
                logger.warning(
                    "sec_incremental: no submissions.json for cik=%s (private/de-registered?)",
                    cik,
                )
                failed.append((cik, "SubmissionsMissing"))
                return None
            top_accession = _top_accession_from_submissions(submissions)
            if top_accession is None:
                # Transient: submissions.json returned but filings.recent
                # is empty despite a master-index hit. Same invariant —
                # withhold the master-index watermark so next run retries.
                logger.warning(
                    "sec_incremental: submissions.json for cik=%s has empty filings.recent",
                    cik,
                )
                failed.append((cik, "EmptyFilingsRecent"))
                return None

        facts = fundamentals_provider.extract_facts(symbol, cik)
        facts_upserted = 0

        with conn.transaction():
            if facts:
                upserted, _skipped = upsert_facts_for_instrument(
                    conn,
                    instrument_id=instrument_id,
                    facts=facts,
                    ingestion_run_id=run_id,
                )
                facts_upserted = upserted
            # Upsert each master-index entry for this CIK into
            # filing_events so downstream event-driven triggers
            # (#273 thesis, #276 cascade) have a timestamped signal.
            # Idempotent: ON CONFLICT preserves richer URLs stored by
            # the submissions-based ingest path. Atomic with the facts
            # upsert and watermark writes below.
            if new_filings:
                for entry in new_filings:
                    _upsert_filing_from_master_index(
                        conn,
                        instrument_id=instrument_id,
                        entry=entry,
                        symbol=symbol,
                    )
            set_watermark(
                conn,
                source="sec.submissions",
                key=cik,
                watermark=top_accession,
            )
            set_watermark(
                conn,
                source="sec.companyfacts",
                key=cik,
                watermark=top_accession,
            )
        conn.commit()
        return facts_upserted
    except Exception as exc:
        # ``with conn.transaction()`` already rolled back on exception;
        # the explicit rollback here covers the pre-transaction path
        # (fetch_submissions raising, extract_facts raising) where no
        # transaction block had been entered yet.
        try:
            conn.rollback()
        except psycopg.Error:
            logger.debug("rollback suppressed after executor exception", exc_info=True)
        failed.append((cik, type(exc).__name__))
        logger.exception("sec_incremental per-CIK upsert failed for cik=%s", cik)
        return None


def execute_refresh(
    conn: psycopg.Connection[tuple],
    *,
    filings_provider: SecFilingsProvider,
    fundamentals_provider: SecFundamentalsProvider,
    plan: RefreshPlan,
) -> RefreshOutcome:
    """Execute a RefreshPlan against the database.

    Per-CIK isolation: each CIK's facts upsert + both watermark
    advances run inside a single ``with conn.transaction()`` block and
    commit atomically or roll back together. A per-CIK failure
    records the exception class name in ``RefreshOutcome.failed`` and
    continues — one bad CIK never aborts the layer. After each CIK's
    block we call ``conn.commit()`` so progress survives a later
    crash.

    The ``submissions_only_advances`` path skips both the submissions
    fetch AND the companyfacts fetch — the planner already decided
    that path is correct for 8-K-style hits where XBRL facts would
    not change.
    """
    total = len(plan.seeds) + len(plan.refreshes) + len(plan.submissions_only_advances)
    if total == 0:
        return RefreshOutcome()

    run_id = start_ingestion_run(
        conn,
        source="sec_edgar",
        endpoint="/api/xbrl/companyfacts",
        instrument_count=total,
    )
    conn.commit()

    seeded = 0
    refreshed = 0
    submissions_advanced = 0
    facts_upserted_total = 0
    failed: list[tuple[str, str]] = []
    done = 0
    catastrophic_error: str | None = None

    try:
        # Seeds + refreshes share one per-CIK body. _run_cik_upsert
        # returns the fact-row count (int >= 0) on success, or None
        # on skip / failure. Failures additionally append to `failed`.
        #
        # Seeds deliberately do NOT pass ``new_filings`` even if the
        # CIK has entries in ``plan.new_filings_by_cik`` — seeds need
        # full historical backfill (#268 Chunk E), not just this
        # cycle's master-index hits. Writing only this week's filings
        # for a seed would give downstream event triggers a misleading
        # signal ("look, a filing landed") when the instrument still
        # lacks most of its history. Chunk E owns the seed-time
        # filing_events population.
        for cik in plan.seeds:
            done += 1
            upserted = _run_cik_upsert(
                conn,
                cik=cik,
                filings_provider=filings_provider,
                fundamentals_provider=fundamentals_provider,
                run_id=run_id,
                failed=failed,
            )
            if upserted is not None:
                seeded += 1
                facts_upserted_total += upserted
            report_progress(done, total)

        for cik, top_accession in plan.refreshes:
            done += 1
            upserted = _run_cik_upsert(
                conn,
                cik=cik,
                filings_provider=filings_provider,
                fundamentals_provider=fundamentals_provider,
                run_id=run_id,
                failed=failed,
                known_top_accession=top_accession,
                new_filings=plan.new_filings_by_cik.get(cik),
            )
            if upserted is not None:
                refreshed += 1
                facts_upserted_total += upserted
            report_progress(done, total)

        for cik, accession in plan.submissions_only_advances:
            done += 1
            try:
                inst = _instrument_for_cik(conn, cik)
                conn.commit()  # close implicit read tx from the SELECT
                if inst is None:
                    # Plan-drift: CIK fell out of the tradable-with-SEC
                    # cohort between planning and execution. Record as
                    # failed so the master-index watermark for this
                    # day is withheld.
                    logger.warning(
                        "sec_incremental: submissions-only path — no tradable instrument for cik=%s (plan drift?)",
                        cik,
                    )
                    failed.append((cik, "InstrumentMissing"))
                    report_progress(done, total)
                    continue
                instrument_id, symbol = inst
                new_filings = plan.new_filings_by_cik.get(cik)
                with conn.transaction():
                    # Upsert filing_events for each master-index entry
                    # on this CIK so the 8-K (or similar) is visible to
                    # downstream event-driven triggers, even though we
                    # don't fetch companyfacts.
                    if new_filings:
                        for entry in new_filings:
                            _upsert_filing_from_master_index(
                                conn,
                                instrument_id=instrument_id,
                                entry=entry,
                                symbol=symbol,
                            )
                    set_watermark(
                        conn,
                        source="sec.submissions",
                        key=cik,
                        watermark=accession,
                    )
                conn.commit()
                submissions_advanced += 1
            except Exception as exc:
                try:
                    conn.rollback()
                except psycopg.Error:
                    logger.debug("rollback suppressed after executor exception", exc_info=True)
                failed.append((cik, type(exc).__name__))
                logger.exception(
                    "sec_incremental submissions-only advance failed for cik=%s",
                    cik,
                )
            report_progress(done, total)

        report_progress(done, total, force=True)

        # Commit pending master-index watermarks ONLY for days where
        # every covered CIK that appeared in that day's hits was
        # processed without failure. A failed CIK leaves its day's
        # watermark un-advanced so the next run re-fetches that day's
        # master-index on 200, re-parses, and re-plans the failed CIK
        # instead of 304-skipping it forever.
        # Union executor-phase failures with planner-phase skips so
        # both sources withhold the master-index watermark for their day.
        failed_ciks = {cik for cik, _ in failed} | set(plan.failed_plan_ciks)
        for iso_date, last_modified, body_hash in plan.pending_master_index_writes:
            day_ciks = set(plan.ciks_by_day.get(iso_date, []))
            if day_ciks & failed_ciks:
                logger.info(
                    "sec_incremental: withholding master-index watermark for %s due to failed CIKs in its hit set (%s)",
                    iso_date,
                    sorted(day_ciks & failed_ciks),
                )
                continue
            try:
                with conn.transaction():
                    set_watermark(
                        conn,
                        source="sec.master-index",
                        key=iso_date,
                        watermark=last_modified,
                        response_hash=body_hash,
                    )
                conn.commit()
            except Exception:
                try:
                    conn.rollback()
                except psycopg.Error:
                    logger.debug(
                        "rollback suppressed after master-index watermark commit failure",
                        exc_info=True,
                    )
                logger.exception(
                    "sec_incremental: master-index watermark commit failed for %s",
                    iso_date,
                )
    except Exception as exc:
        # Non-per-CIK failure escaped (per-CIK exceptions are caught
        # inside _run_cik_upsert and the submissions-only try block).
        # Typical triggers: DB connection drop, unhandled programming
        # error. Record it so the audit trail still has a terminal
        # status instead of orphaning the run row in 'running'.
        catastrophic_error = f"{type(exc).__name__}: {exc}"
        logger.exception("sec_incremental: catastrophic failure in execute_refresh")
        raise
    finally:
        # Always record a terminal status — required by the audit
        # non-negotiable (settled-decisions.md Auditability).
        progressed = seeded + refreshed + submissions_advanced
        if catastrophic_error is not None:
            status = "failed"
            error_msg: str | None = catastrophic_error
        elif failed and progressed == 0:
            status = "failed"
            error_msg = f"{len(failed)} CIKs failed"
        elif failed:
            status = "partial"
            error_msg = f"{len(failed)} CIKs failed"
        else:
            status = "success"
            error_msg = None
        try:
            # Clear any aborted transaction state left over from a
            # catastrophic psycopg error before the audit write — an
            # InFailedSqlTransaction on the next execute would orphan
            # the run row. Rollback on a clean connection is a no-op.
            try:
                conn.rollback()
            except psycopg.Error:
                logger.debug("pre-finish rollback suppressed", exc_info=True)
            finish_ingestion_run(
                conn,
                run_id=run_id,
                status=status,
                rows_upserted=facts_upserted_total,
                error=error_msg,
            )
            conn.commit()
        except Exception:
            # Roll back the aborted tx so the next caller gets a clean
            # session, and log regardless of outcome.
            try:
                conn.rollback()
            except psycopg.Error:
                logger.debug(
                    "rollback after finish_ingestion_run failure suppressed",
                    exc_info=True,
                )
            logger.exception("sec_incremental: finish_ingestion_run failed")
            # On a clean run path (no catastrophic exception already
            # being re-raised) we MUST surface the audit failure so the
            # scheduler's _tracked_job marks the job failed. Swallowing
            # here would report job success despite an orphaned run row.
            # On the catastrophic path the original exception is already
            # re-raised by the `except` above; don't mask it.
            if catastrophic_error is None:
                raise

    return RefreshOutcome(
        seeded=seeded,
        refreshed=refreshed,
        submissions_advanced=submissions_advanced,
        failed=failed,
    )
