"""SEC N-PORT registered-investment-company (RIC) trust-CIK directory
sync (#963).

Sibling of :mod:`app.services.sec_13f_filer_directory` (#912). Walks
SEC's quarterly ``form.idx`` for the last N closed quarters, harvests
every distinct NPORT-P / NPORT-P/A filer CIK + canonical trust name,
and UPSERTs into ``sec_nport_filer_directory``. The N-PORT ingester
(:func:`app.workers.scheduler.sec_n_port_ingest`) reads this directory
instead of ``institutional_filers`` so it walks the right universe.

## Why a separate directory

13F-HR is filed by the MANAGER entity (`VANGUARD GROUP INC`,
``cik=0000102909``). N-PORT is filed by the RIC TRUST entity
(``VANGUARD INDEX FUNDS``, ``cik=0000036405``). These are distinct
SEC CIKs — walking the 13F-manager submissions endpoint for NPORT-P
returns nothing. Empirically discovered during #919 rollup integration
work: the standing ``sec_n_port_ingest`` job walked
``institutional_filers`` (11,206 rows on dev, all 13F managers) and
``n_port_ingest_log`` stayed empty. #919 worked around with a
hardcoded panel-targeted RIC CIK list at
``.claude/nport-panel-backfill.py``; this module replaces the
workaround with a proper standing directory.

## Form-type filter

Modern SEC filings use ``NPORT-P`` / ``NPORT-P/A``. Pre-2018 legacy
filings used ``N-PORT`` / ``N-PORT/A``; those don't appear in the
modern ``form.idx`` quarters this walker covers (default 4 quarters
back from today), but accepting them keeps the directory consistent
with :data:`app.services.n_port_ingest._NPORT_FORM_TYPES` so a
backfill script that ingests legacy quarters can still resolve to a
directory row.

## Re-uses #912's form-index fetcher

:mod:`app.services.top_filer_discovery` already encapsulates the
``form.idx`` fetch + parse. This module differs only in the
form-type frozenset it filters on and the destination table; the
``_aggregate_filer_directory``-shaped function is duplicated here
rather than parameterised in the 13F walker so each walker stays
self-contained. A future refactor PR can DRY the two if a third
walker arrives.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime, time
from typing import Any

import psycopg
import psycopg.rows

from app.services.top_filer_discovery import fetch_form_index, parse_form_index

logger = logging.getLogger(__name__)


# Mirrors :data:`app.services.n_port_ingest._NPORT_FORM_TYPES`.
# ``NPORT-P`` / ``NPORT-P/A`` are the canonical post-2018 spellings;
# ``N-PORT`` / ``N-PORT/A`` are the pre-2018 legacy spellings retained
# for shape-uniformity with the ingester (a backfill script ingesting
# legacy quarters can still resolve to a directory row).
_NPORT_FORM_TYPES: frozenset[str] = frozenset(
    {
        "NPORT-P",
        "NPORT-P/A",
        "N-PORT",
        "N-PORT/A",
    }
)


# 4 closed quarters covers every actively-filing RIC trust (NPORT-P
# is filed monthly per series, but the trust-CIK directory only needs
# one filing per quarter to surface the trust). Wider windows fetch
# more bandwidth but don't add new CIKs — the active-RIC tail is
# bounded by SEC's RIC registration roll, not by walk-window length.
DEFAULT_QUARTERS: int = 4


@dataclass(frozen=True)
class NportFilerDirectorySyncResult:
    """Counters from one :func:`sync_nport_filer_directory` invocation."""

    quarters_attempted: int
    quarters_failed: int
    filers_seen: int
    filers_inserted: int
    filers_refreshed: int
    skipped_empty_name: int


def _last_completed_quarter(today: date) -> tuple[int, int]:
    """Return the (year, quarter) of the most recent CLOSED quarter
    relative to ``today``. The current in-progress quarter's
    ``form.idx`` is incomplete, so the walk skips it.

    Identical helper to :mod:`app.services.sec_13f_filer_directory`
    — duplicated for self-containment so the two walkers can evolve
    independently."""
    cur_q = (today.month - 1) // 3 + 1
    if cur_q == 1:
        return today.year - 1, 4
    return today.year, cur_q - 1


def _last_n_quarters(today: date, n: int) -> list[tuple[int, int]]:
    """Return ``n`` (year, quarter) tuples newest-first ending at the
    quarter PRECEDING ``today``."""
    y, q = _last_completed_quarter(today)
    out: list[tuple[int, int]] = []
    for _ in range(n):
        out.append((y, q))
        q -= 1
        if q == 0:
            q = 4
            y -= 1
    return out


def _aggregate_nport_filer_directory(
    quarters: list[tuple[int, int]],
    *,
    fetch: Callable[[int, int], str],
) -> tuple[dict[str, str], dict[str, date], int]:
    """Walk each quarter's form.idx and aggregate the latest NPORT-P
    company_name + filing date per CIK.

    Returns ``(latest_name_by_cik, latest_filed_by_cik, quarters_failed)``.
    Per-quarter fetch failures are isolated — a transient SEC outage
    on one quarter must not abort the sweep; partial coverage beats
    aborting. ``latest_name`` keys on the most recent ``date_filed``;
    same-day ties resolve to the lex-greatest name for determinism
    (matches the pattern :func:`app.services.sec_13f_filer_directory.
    _aggregate_filer_directory` adopted after #912 Codex review)."""
    latest_name: dict[str, str] = {}
    latest_filed: dict[str, date] = {}
    failed = 0
    for year, q in quarters:
        try:
            payload = fetch(year, q)
        except Exception:  # noqa: BLE001 — per-quarter failure isolation
            logger.exception(
                "sec_nport_filer_directory: form.idx fetch failed for %sQ%s",
                year,
                q,
            )
            failed += 1
            continue
        for entry in parse_form_index(payload):
            if entry.form_type not in _NPORT_FORM_TYPES:
                continue
            prior_date = latest_filed.get(entry.cik)
            if prior_date is None or entry.date_filed > prior_date:
                latest_name[entry.cik] = entry.company_name
                latest_filed[entry.cik] = entry.date_filed
            elif entry.date_filed == prior_date and entry.company_name > latest_name[entry.cik]:
                # Same-day tiebreak — deterministic by name.
                latest_name[entry.cik] = entry.company_name
    return latest_name, latest_filed, failed


def sync_nport_filer_directory(
    conn: psycopg.Connection[Any],
    *,
    quarters: int = DEFAULT_QUARTERS,
    today: date | None = None,
    fetch: Callable[[int, int], str] = fetch_form_index,
) -> NportFilerDirectorySyncResult:
    """Walk SEC quarterly form.idx, harvest distinct NPORT-P filer
    CIK + name, UPSERT into ``sec_nport_filer_directory``.

    Idempotency: re-running on the same quarter set produces zero new
    rows but refreshes ``fund_trust_name`` + ``last_seen_filed_at`` on
    existing rows when the incoming filing is at least as recent as
    the stored one (defends against an older form.idx walk regressing
    a name a later filing already updated — same guard
    :mod:`app.services.sec_13f_filer_directory` adopted after #912
    Codex review).
    """
    today_d = today if today is not None else datetime.now(tz=UTC).date()
    qs = _last_n_quarters(today_d, quarters)
    latest_name, latest_filed, failed = _aggregate_nport_filer_directory(qs, fetch=fetch)

    skipped_empty_name = 0
    valid_ciks: list[tuple[str, str, date]] = []
    for cik, name in latest_name.items():
        if not name.strip():
            logger.warning(
                "sec_nport_filer_directory: empty company_name for cik=%s — skipping",
                cik,
            )
            skipped_empty_name += 1
            continue
        valid_ciks.append((cik, name, latest_filed[cik]))

    inserted = 0
    refreshed = 0
    for cik, name, filed_d in valid_ciks:
        # ``form.idx`` carries date-only; lift to midnight UTC for
        # the TIMESTAMPTZ column so GREATEST() comparisons against
        # later-arriving timestamps stay monotone. Mirrors #912.
        last_filing_ts = datetime.combine(filed_d, time(0, 0), tzinfo=UTC)
        row = conn.execute(
            """
            INSERT INTO sec_nport_filer_directory (
                cik, fund_trust_name, last_seen_period_end,
                last_seen_filed_at
            )
            VALUES (
                %(cik)s, %(name)s, %(period_end)s, %(filed_at)s
            )
            ON CONFLICT (cik) DO UPDATE SET
                fund_trust_name = CASE
                    WHEN sec_nport_filer_directory.last_seen_filed_at IS NULL
                      OR EXCLUDED.last_seen_filed_at >= sec_nport_filer_directory.last_seen_filed_at
                    THEN EXCLUDED.fund_trust_name
                    ELSE sec_nport_filer_directory.fund_trust_name
                END,
                last_seen_period_end = CASE
                    WHEN sec_nport_filer_directory.last_seen_period_end IS NULL
                      OR (EXCLUDED.last_seen_period_end IS NOT NULL
                          AND EXCLUDED.last_seen_period_end > sec_nport_filer_directory.last_seen_period_end)
                    THEN EXCLUDED.last_seen_period_end
                    ELSE sec_nport_filer_directory.last_seen_period_end
                END,
                last_seen_filed_at = GREATEST(
                    COALESCE(sec_nport_filer_directory.last_seen_filed_at, '-infinity'),
                    COALESCE(EXCLUDED.last_seen_filed_at, '-infinity')
                ),
                fetched_at = NOW()
            RETURNING (xmax = 0) AS was_inserted
            """,
            {
                "cik": cik,
                "name": name,
                # form.idx doesn't carry period_of_report; the per-CIK
                # NPORT-P submissions walker (in n_port_ingest)
                # threads that through later. Leave NULL on directory
                # insert; ingester layer is the source of truth for
                # per-accession period_end.
                "period_end": None,
                "filed_at": last_filing_ts,
            },
        ).fetchone()
        # ``xmax = 0`` is true on a fresh INSERT, false when an UPDATE
        # ran. Same idiom as sec_13f_filer_directory + exchanges.py.
        if row is not None and bool(row[0]):
            inserted += 1
        else:
            refreshed += 1

    conn.commit()

    logger.info(
        "sec_nport_filer_directory: quarters=%d failed=%d seen=%d inserted=%d refreshed=%d skipped_empty_name=%d",
        len(qs),
        failed,
        len(latest_name),
        inserted,
        refreshed,
        skipped_empty_name,
    )

    return NportFilerDirectorySyncResult(
        quarters_attempted=len(qs),
        quarters_failed=failed,
        filers_seen=len(latest_name),
        filers_inserted=inserted,
        filers_refreshed=refreshed,
        skipped_empty_name=skipped_empty_name,
    )
