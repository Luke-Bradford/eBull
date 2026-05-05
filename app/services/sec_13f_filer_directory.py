"""SEC 13F-HR filer directory ingest (#912 / #841 PR1).

Operator audit 2026-05-04 found ``institutional_filers`` holding 14
curated rows when the real US 13F-HR universe is ~5,000 filers per
quarter. AAPL institutional ownership rollup reports 5.94% against
gurufocus parity of ~62% — every downstream ingester runs against
the same 14 names, so 99% of the real institutional layer is
invisible to the rollup.

This module is the directory-discovery half. Walks SEC's quarterly
``form.idx`` for the last N closed quarters, harvests every distinct
13F-HR / 13F-HR/A / 13F-NT filer CIK + canonical name, and UPSERTs
into ``institutional_filers``. PR2 (#913) reads the populated
directory and ingests the actual holdings; PR3 (#914) closes the
CUSIP gap so the ingested holdings resolve to instruments.

Re-uses :mod:`app.services.top_filer_discovery` for fetch + parse so
the form.idx parser stays single-sourced.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime, time
from typing import Any

import psycopg
import psycopg.rows

from app.services.top_filer_discovery import (
    _THIRTEEN_F_FORM_TYPES,
    fetch_form_index,
    parse_form_index,
)

logger = logging.getLogger(__name__)


# Last 4 closed quarters covers every 13F-HR cycle (filers report
# quarterly; new filers surface within one quarter; 13F-HR/A
# amendments tail by 1-2 quarters). Wider windows fetch more
# bandwidth but don't add new filers — the tail is bounded.
DEFAULT_QUARTERS: int = 4


@dataclass(frozen=True)
class FilerDirectorySyncResult:
    """Counters from one ``sync_filer_directory`` invocation."""

    quarters_attempted: int
    quarters_failed: int
    filers_seen: int
    filers_inserted: int
    filers_refreshed: int
    skipped_empty_name: int


def _last_completed_quarter(today: date) -> tuple[int, int]:
    """Return the (year, quarter) of the most recent CLOSED quarter
    relative to ``today``. The current in-progress quarter's
    ``form.idx`` is incomplete, so the directory walk skips it."""
    cur_q = (today.month - 1) // 3 + 1
    if cur_q == 1:
        return today.year - 1, 4
    return today.year, cur_q - 1


def _last_n_quarters(today: date, n: int) -> list[tuple[int, int]]:
    """Return ``n`` (year, quarter) tuples newest-first ending at
    the quarter PRECEDING ``today``."""
    y, q = _last_completed_quarter(today)
    out: list[tuple[int, int]] = []
    for _ in range(n):
        out.append((y, q))
        q -= 1
        if q == 0:
            q = 4
            y -= 1
    return out


def _aggregate_filer_directory(
    quarters: list[tuple[int, int]],
    *,
    fetch: Callable[[int, int], str],
) -> tuple[dict[str, str], dict[str, date], int]:
    """Walk each quarter's form.idx and aggregate the latest 13F-HR
    company_name + filing date per CIK.

    Returns ``(latest_name_by_cik, latest_filed_by_cik, quarters_failed)``.
    Per-quarter fetch failures are isolated — a transient SEC outage
    on one quarter must not abort the whole sweep, partial coverage
    beats aborting. ``latest_name`` keys on the most recent
    ``date_filed``. When two filings share the same ``date_filed``
    (a single filer can file 13F-HR + 13F-HR/A on the same day with
    slightly different names), the lexicographically-greatest name
    wins so name selection is deterministic regardless of caller
    iteration order (Codex pre-push review #912).
    """
    latest_name: dict[str, str] = {}
    latest_filed: dict[str, date] = {}
    failed = 0
    for year, q in quarters:
        try:
            payload = fetch(year, q)
        except Exception:  # noqa: BLE001 — per-quarter failure isolation
            logger.exception(
                "sec_13f_filer_directory: form.idx fetch failed for %sQ%s",
                year,
                q,
            )
            failed += 1
            continue
        for entry in parse_form_index(payload):
            if entry.form_type not in _THIRTEEN_F_FORM_TYPES:
                continue
            prior_date = latest_filed.get(entry.cik)
            if prior_date is None or entry.date_filed > prior_date:
                latest_name[entry.cik] = entry.company_name
                latest_filed[entry.cik] = entry.date_filed
            elif entry.date_filed == prior_date and entry.company_name > latest_name[entry.cik]:
                # Same-day tiebreak — deterministic by name.
                latest_name[entry.cik] = entry.company_name
    return latest_name, latest_filed, failed


def _bulk_classify_filer_type(
    conn: psycopg.Connection[Any],
    ciks: list[str],
) -> dict[str, str]:
    """Bulk variant of :func:`app.services.ncen_classifier.compose_filer_type`.

    The single-CIK helper does two SELECTs per call; called per filer
    in a 5,000-row sweep that's 10,000 round-trips. This bulk version
    runs two ANY-array selects total. Same priority chain:

      1. Curated ETF seed list (``etf_filer_cik_seeds``) → ``ETF``
      2. N-CEN classification → ``INS`` / ``INV`` / ``OTHER`` / ``ETF``
      3. Default → ``INV``

    Steps 2 and 1 are applied in that order so step-1 always wins
    (same precedence as ``compose_filer_type``).
    """
    out: dict[str, str] = {cik: "INV" for cik in ciks}
    if not ciks:
        return out

    with conn.cursor(row_factory=psycopg.rows.tuple_row) as cur:
        cur.execute(
            """
            SELECT cik, derived_filer_type
            FROM ncen_filer_classifications
            WHERE cik = ANY(%(ciks)s)
            """,
            {"ciks": ciks},
        )
        for cik, derived in cur.fetchall():
            out[cik] = str(derived)

        cur.execute(
            """
            SELECT cik
            FROM etf_filer_cik_seeds
            WHERE cik = ANY(%(ciks)s) AND active = TRUE
            """,
            {"ciks": ciks},
        )
        for (cik,) in cur.fetchall():
            out[cik] = "ETF"

    return out


def sync_filer_directory(
    conn: psycopg.Connection[Any],
    *,
    quarters: int = DEFAULT_QUARTERS,
    today: date | None = None,
    fetch: Callable[[int, int], str] = fetch_form_index,
) -> FilerDirectorySyncResult:
    """Walk SEC quarterly form.idx, harvest distinct 13F-HR filer
    CIK + name, UPSERT into ``institutional_filers``.

    ``filer_type`` is bulk-resolved via the curated ETF list +
    N-CEN classifier (same priority chain as
    :func:`app.services.ncen_classifier.compose_filer_type`).
    Default for unknown filers is ``'INV'`` so the ≥95% non-NULL
    acceptance is structurally satisfied. ``filer_type`` is
    preserved on UPDATE — N-CEN classifier (#782) owns later
    refinement; this job only sets the floor on first INSERT.

    Idempotency: re-running on the same quarter set produces zero
    new rows but refreshes ``name`` + ``last_filing_at`` on
    existing rows. ``ON CONFLICT (cik) DO UPDATE`` is used directly
    rather than DO NOTHING so a filer rename or new filing date
    propagates without a separate UPDATE pass.
    """
    today_d = today if today is not None else datetime.now(tz=UTC).date()
    qs = _last_n_quarters(today_d, quarters)
    latest_name, latest_filed, failed = _aggregate_filer_directory(qs, fetch=fetch)

    skipped_empty_name = 0
    valid_ciks: list[str] = []
    for cik, name in latest_name.items():
        if not name.strip():
            logger.warning(
                "sec_13f_filer_directory: empty company_name for cik=%s — skipping",
                cik,
            )
            skipped_empty_name += 1
            continue
        valid_ciks.append(cik)

    filer_types = _bulk_classify_filer_type(conn, valid_ciks)

    inserted = 0
    refreshed = 0
    for cik in valid_ciks:
        name = latest_name[cik]
        filed_d = latest_filed[cik]
        # ``form.idx`` carries date-only; lift to midnight UTC for
        # the TIMESTAMPTZ column so GREATEST() comparisons against
        # later-arriving timestamps stay monotone.
        last_filing_ts = datetime.combine(filed_d, time(0, 0), tzinfo=UTC)
        row = conn.execute(
            """
            INSERT INTO institutional_filers (cik, name, filer_type, last_filing_at)
            VALUES (%(cik)s, %(name)s, %(filer_type)s, %(last_filing_at)s)
            ON CONFLICT (cik) DO UPDATE SET
                -- Only refresh name when the incoming filing is at
                -- least as recent as the stored one. Without this
                -- guard a newer filing date persisted by the
                -- holdings ingester (primary_doc.xml) would have its
                -- canonical name regress to whatever older row this
                -- form.idx walk happens to encounter. Codex pre-push
                -- review #912.
                name = CASE
                    WHEN institutional_filers.last_filing_at IS NULL
                      OR EXCLUDED.last_filing_at >= institutional_filers.last_filing_at
                    THEN EXCLUDED.name
                    ELSE institutional_filers.name
                END,
                last_filing_at = GREATEST(
                    COALESCE(institutional_filers.last_filing_at, '-infinity'),
                    COALESCE(EXCLUDED.last_filing_at, '-infinity')
                ),
                fetched_at = NOW()
            RETURNING (xmax = 0) AS was_inserted
            """,
            {
                "cik": cik,
                "name": name,
                "filer_type": filer_types[cik],
                "last_filing_at": last_filing_ts,
            },
        ).fetchone()
        # ``xmax = 0`` is true on a fresh INSERT, false when an UPDATE
        # ran (xmax then carries the txid that supersedes the prior
        # tuple). Same idiom as exchanges.py / dividend_calendar.py.
        if row is not None and bool(row[0]):
            inserted += 1
        else:
            refreshed += 1

    conn.commit()

    logger.info(
        "sec_13f_filer_directory: quarters=%d failed=%d seen=%d inserted=%d refreshed=%d skipped_empty_name=%d",
        len(qs),
        failed,
        len(latest_name),
        inserted,
        refreshed,
        skipped_empty_name,
    )

    return FilerDirectorySyncResult(
        quarters_attempted=len(qs),
        quarters_failed=failed,
        filers_seen=len(latest_name),
        filers_inserted=inserted,
        filers_refreshed=refreshed,
        skipped_empty_name=skipped_empty_name,
    )
