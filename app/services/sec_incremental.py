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

import logging
from dataclasses import dataclass, field
from datetime import date, timedelta
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
    """

    seeds: list[str] = field(default_factory=list)
    refreshes: list[str] = field(default_factory=list)
    submissions_only_advances: list[tuple[str, str]] = field(default_factory=list)


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
    for target in _lookback_dates(today):
        wm = get_watermark(conn, "sec.master-index", target.isoformat())
        if_modified_since = wm.watermark if wm else None
        result = provider.fetch_master_index(target, if_modified_since=if_modified_since)
        if result is None:
            # 304 Not Modified OR 404 (weekend / holiday): nothing to
            # parse, and no Last-Modified to persist on 404. The 304
            # path is also a no-op because the stored watermark is
            # still the correct ``If-Modified-Since`` for next run.
            continue

        if wm is not None and wm.response_hash == result.body_hash:
            # Body identical to the last run but without a 304 — refresh
            # fetched_at only (watermark + hash unchanged) and skip
            # re-parsing. Secondary dedup for providers that don't
            # honour If-Modified-Since perfectly.
            with conn.transaction():
                set_watermark(
                    conn,
                    source="sec.master-index",
                    key=target.isoformat(),
                    watermark=result.last_modified or "",
                    response_hash=result.body_hash,
                )
            conn.commit()
            continue

        entries = parse_master_index(result.body)
        for entry in entries:
            master_hits_by_cik.setdefault(entry.cik, []).append(entry)

        with conn.transaction():
            set_watermark(
                conn,
                source="sec.master-index",
                key=target.isoformat(),
                watermark=result.last_modified or "",
                response_hash=result.body_hash,
            )
        conn.commit()

    seeds: list[str] = []
    refreshes: list[str] = []
    submissions_only: list[tuple[str, str]] = []

    covered_set = set(covered)
    # Drop hits outside the cohort before the per-CIK loop so we never
    # issue a submissions fetch for a rogue master-index entry. The
    # ``.get(cik)`` lookup below would implicitly filter anyway, but
    # an explicit intersect documents intent.
    master_hits_by_cik = {cik: entries for cik, entries in master_hits_by_cik.items() if cik in covered_set}

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
            continue
        top_accession = _top_accession_from_submissions(submissions)
        if top_accession is None:
            continue
        if top_accession == wm.watermark:
            # Amendment or re-listing of a filing we already have.
            continue

        hit_forms = {e.form_type for e in entries}
        if hit_forms & FUNDAMENTALS_FORMS:
            refreshes.append(cik)
        else:
            submissions_only.append((cik, top_accession))

    return RefreshPlan(
        seeds=sorted(seeds),
        refreshes=sorted(refreshes),
        submissions_only_advances=sorted(submissions_only),
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


def _run_cik_upsert(
    conn: psycopg.Connection[tuple],
    *,
    cik: str,
    filings_provider: SecFilingsProvider,
    fundamentals_provider: SecFundamentalsProvider,
    run_id: int,
    failed: list[tuple[str, str]],
) -> bool:
    """Per-CIK seed/refresh body.

    Returns True on success, False on skip (missing instrument,
    missing submissions, missing top accession) or failure. Appends
    ``(cik, ExceptionName)`` to ``failed`` on exception. All writes
    for one CIK happen inside one ``with conn.transaction()`` block
    so on exception the facts upsert AND both watermark writes roll
    back together — watermarks never drift ahead of data.
    """
    try:
        inst = _instrument_for_cik(conn, cik)
        if inst is None:
            return False
        instrument_id, symbol = inst

        submissions = filings_provider.fetch_submissions(cik)
        if submissions is None:
            return False
        top_accession = _top_accession_from_submissions(submissions)
        if top_accession is None:
            return False

        facts = fundamentals_provider.extract_facts(symbol, cik)

        with conn.transaction():
            if facts:
                upsert_facts_for_instrument(
                    conn,
                    instrument_id=instrument_id,
                    facts=facts,
                    ingestion_run_id=run_id,
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
        return True
    except Exception as exc:
        # ``with conn.transaction()`` already rolled back on exception;
        # the explicit rollback here covers the pre-transaction path
        # (fetch_submissions raising, extract_facts raising) where no
        # transaction block had been entered yet.
        try:
            conn.rollback()
        except Exception:
            pass
        failed.append((cik, type(exc).__name__))
        logger.exception("sec_incremental per-CIK upsert failed for cik=%s", cik)
        return False


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
    failed: list[tuple[str, str]] = []
    done = 0

    # Seeds + refreshes share one per-CIK body.
    for cik in plan.seeds:
        done += 1
        if _run_cik_upsert(
            conn,
            cik=cik,
            filings_provider=filings_provider,
            fundamentals_provider=fundamentals_provider,
            run_id=run_id,
            failed=failed,
        ):
            seeded += 1
        report_progress(done, total)

    for cik in plan.refreshes:
        done += 1
        if _run_cik_upsert(
            conn,
            cik=cik,
            filings_provider=filings_provider,
            fundamentals_provider=fundamentals_provider,
            run_id=run_id,
            failed=failed,
        ):
            refreshed += 1
        report_progress(done, total)

    for cik, accession in plan.submissions_only_advances:
        done += 1
        try:
            with conn.transaction():
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
            except Exception:
                pass
            failed.append((cik, type(exc).__name__))
            logger.exception(
                "sec_incremental submissions-only advance failed for cik=%s",
                cik,
            )
        report_progress(done, total)

    report_progress(done, total, force=True)

    progressed = seeded + refreshed + submissions_advanced
    if failed and progressed == 0:
        status = "failed"
    elif failed:
        status = "partial"
    else:
        status = "success"

    finish_ingestion_run(
        conn,
        run_id=run_id,
        status=status,
        rows_upserted=seeded + refreshed,
        error=f"{len(failed)} CIKs failed" if failed else None,
    )
    conn.commit()

    return RefreshOutcome(
        seeded=seeded,
        refreshed=refreshed,
        submissions_advanced=submissions_advanced,
        failed=failed,
    )
