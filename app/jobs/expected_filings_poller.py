"""Event-driven fundamentals catch-up — expected-filings seed + poller (#1788).

`daily_financial_facts` already detects new 10-Q/10-K filings (via
`plan_refresh` → submissions.json) and force-refreshes companyfacts +
`financial_periods` — but once a day over the whole universe. This module
adds a small, budget-bounded, high-frequency watchlist so the operator's
high-value instruments (held + watchlisted) get refreshed within minutes
of an expected filing appearing, ahead of the daily backstop.

Strictly additive: a mis-sized window or a missed filing just falls back
to the ≤24 h daily path. The poller acts ONLY on a strictly-newer,
exact-form, non-amendment accession (baseline-watermarked), so it can
never false-fulfil or write wrong data.

See docs/specs/etl/2026-06-28-expected-filings-poller.md.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta

import psycopg

from app.providers.implementations.sec_submissions import (
    FilingIndexRow,
    HttpGet,
    check_freshness,
)
from app.services.fundamentals.force_refresh import run_force_refresh
from app.services.sec_manifest import record_manifest_entry

logger = logging.getLogger(__name__)

# Quarterly cadence: the next fiscal period ends ~one quarter after the
# latest reported one (Rule 13a-13). 91d ≈ one quarter; the wide window
# below absorbs fiscal-calendar drift.
_QUARTER_DAYS = 91

# Poll window offsets from the predicted next period-end, padded around
# the widest statutory filing deadline (Form 10-Q A.(1): 40-45d after
# quarter-end; Form 10-K A.(2): 60-90d after FY-end). Start is offset to
# the earliest realistic filing date so we don't burn polls in the
# pre-deadline dead zone; an early filer caught late falls to the daily
# backstop.
_WINDOW_OFFSETS: dict[str, tuple[int, int]] = {
    "10-Q": (30, 55),
    "10-K": (50, 100),
}

# Manifest source code per expected form (sec_filing_manifest.source).
_SOURCE_FOR_FORM: dict[str, str] = {
    "10-Q": "sec_10q",
    "10-K": "sec_10k",
}


def next_form_and_window(
    latest_period_type: str,
    latest_period_end: date,
) -> tuple[str, date, date]:
    """Derive the next expected (form, window_start, window_end).

    Form is fixed by fiscal position, not by quarterly-cadence guessing
    (a domestic issuer files three 10-Qs then one 10-K — a Q3 issuer's
    next filing is the FY 10-K, NOT a phantom 10-Q):

      latest Q1, Q2 -> next 10-Q   (following quarter)
      latest Q3     -> next 10-K   (fiscal year-end period)
      latest Q4, FY -> next 10-Q   (first quarter of the new year)
      else          -> next 10-Q   (default for sparse/odd period types)

    The window is anchored on the predicted next period-end + the
    statutory-deadline-padded offsets.
    """
    expected = "10-K" if latest_period_type == "Q3" else "10-Q"
    next_period_end = latest_period_end + timedelta(days=_QUARTER_DAYS)
    start_off, end_off = _WINDOW_OFFSETS[expected]
    return (
        expected,
        next_period_end + timedelta(days=start_off),
        next_period_end + timedelta(days=end_off),
    )


def match_filing(
    new_filings: list[FilingIndexRow],
    expected_filing_type: str,
) -> FilingIndexRow | None:
    """Return the newest filing matching the expected form, or None.

    Exact-form + non-amendment match: ``10-Q/A`` / ``10-K/A`` map to the
    same manifest source as the original, so source-level matching would
    let an amendment false-fulfil an expected original filing. ``form``
    is matched verbatim and amendments are excluded.
    """
    for row in new_filings:
        if row.form == expected_filing_type and not row.is_amendment:
            return row
    return None


@dataclass(frozen=True)
class SeedStats:
    instruments_scoped: int
    rows_upserted: int


def _baseline_accession(
    conn: psycopg.Connection[tuple],
    instrument_id: int,
    source: str,
) -> str | None:
    """Last known non-amendment accession of ``source`` for the instrument."""
    row = conn.execute(
        """
        SELECT accession_number
        FROM sec_filing_manifest
        WHERE instrument_id = %s AND source = %s AND is_amendment = FALSE
        ORDER BY filed_at DESC
        LIMIT 1
        """,
        (instrument_id, source),
    ).fetchone()
    return str(row[0]) if row else None


def _scope_latest_periods(
    conn: psycopg.Connection[tuple],
    *,
    only_symbol: str | None,
) -> list[tuple[int, date, str]]:
    """Return (instrument_id, latest_period_end, latest_period_type).

    Scope is the operator high-value set — watchlist ∪ open positions —
    restricted to instruments with ≥1 financial_periods row. ``only_symbol``
    force-scopes one instrument (CLI / dev verification), bypassing
    membership.
    """
    if only_symbol is not None:
        scope_cte = "scope AS (SELECT instrument_id FROM instruments WHERE UPPER(symbol) = %(sym)s)"
        params: dict[str, object] = {"sym": only_symbol.upper()}
    else:
        scope_cte = (
            "scope AS ("
            " SELECT instrument_id FROM watchlist"
            " UNION SELECT instrument_id FROM positions WHERE current_units > 0"
            ")"
        )
        params = {}
    rows = conn.execute(
        f"""
        WITH {scope_cte}
        SELECT DISTINCT ON (fp.instrument_id)
               fp.instrument_id, fp.period_end_date, fp.period_type
        FROM financial_periods fp
        JOIN scope s ON s.instrument_id = fp.instrument_id
        WHERE fp.superseded_at IS NULL
        ORDER BY fp.instrument_id, fp.period_end_date DESC
        """,
        params,
    ).fetchall()
    return [(int(r[0]), r[1], str(r[2])) for r in rows]


@dataclass(frozen=True)
class SeedRow:
    instrument_id: int
    expected_filing_type: str
    anchor_period_end: date
    expected_window_start: date
    expected_window_end: date
    baseline_accession: str | None


def derive_seed_rows(
    conn: psycopg.Connection[tuple],
    *,
    only_symbol: str | None = None,
) -> list[SeedRow]:
    """Compute (no writes) the next expected filing per in-scope instrument."""
    scoped = _scope_latest_periods(conn, only_symbol=only_symbol)
    rows: list[SeedRow] = []
    for instrument_id, latest_period_end, latest_period_type in scoped:
        expected, win_start, win_end = next_form_and_window(latest_period_type, latest_period_end)
        baseline = _baseline_accession(conn, instrument_id, _SOURCE_FOR_FORM[expected])
        if baseline is None:
            # No known prior filing of the expected form → no watermark to
            # poll against. check_freshness(last_known_filing_id=None) would
            # return every recent same-source filing, so the poller would
            # false-fulfil on the existing last filing instead of waiting for
            # the new one. Skip — the daily backstop covers these instruments.
            logger.info(
                "seed_expected_filings: skipping instrument_id=%d %s — no manifest baseline",
                instrument_id,
                expected,
            )
            continue
        rows.append(
            SeedRow(
                instrument_id=instrument_id,
                expected_filing_type=expected,
                anchor_period_end=latest_period_end,
                expected_window_start=win_start,
                expected_window_end=win_end,
                baseline_accession=baseline,
            )
        )
    return rows


def seed_expected_filings(
    conn: psycopg.Connection[tuple],
    *,
    only_symbol: str | None = None,
) -> SeedStats:
    """Derive + upsert the next expected filing per in-scope instrument.

    Idempotent: one row per instrument (UNIQUE). The conditional upsert
    rolls a row forward and resets fulfilment ONLY when the cycle key
    (``anchor_period_end`` = latest reported period-end) advances, so a
    no-change re-seed leaves a fulfilled row untouched.
    """
    seed_rows = derive_seed_rows(conn, only_symbol=only_symbol)
    upserted = 0
    for sr in seed_rows:
        cur = conn.execute(
            """
            INSERT INTO expected_filings (
                instrument_id, expected_filing_type, anchor_period_end,
                expected_window_start, expected_window_end, baseline_accession)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (instrument_id) DO UPDATE
            SET expected_filing_type  = EXCLUDED.expected_filing_type,
                anchor_period_end     = EXCLUDED.anchor_period_end,
                expected_window_start = EXCLUDED.expected_window_start,
                expected_window_end   = EXCLUDED.expected_window_end,
                baseline_accession    = EXCLUDED.baseline_accession,
                last_polled_at        = NULL,
                fulfilled_at          = NULL,
                fulfilled_accession   = NULL
            WHERE expected_filings.anchor_period_end IS DISTINCT FROM EXCLUDED.anchor_period_end
            """,
            (
                sr.instrument_id,
                sr.expected_filing_type,
                sr.anchor_period_end,
                sr.expected_window_start,
                sr.expected_window_end,
                sr.baseline_accession,
            ),
        )
        upserted += cur.rowcount

    pruned = 0
    if only_symbol is None:
        # Full-scope re-seed prunes rows for instruments that have left the
        # high-value set (un-watchlisted / position closed) so the poller
        # doesn't keep spending SEC budget outside the watchlist ∪ open-positions
        # invariant. The --symbol path is additive and never prunes.
        cur = conn.execute(
            """
            DELETE FROM expected_filings
            WHERE instrument_id NOT IN (
                SELECT instrument_id FROM watchlist
                UNION SELECT instrument_id FROM positions WHERE current_units > 0
            )
            """
        )
        pruned = cur.rowcount
    conn.commit()
    logger.info(
        "seed_expected_filings: scoped=%d upserted=%d pruned=%d (only_symbol=%s)",
        len(seed_rows),
        upserted,
        pruned,
        only_symbol,
    )
    return SeedStats(instruments_scoped=len(seed_rows), rows_upserted=upserted)


@dataclass(frozen=True)
class PollStats:
    subjects_polled: int
    fulfilled: int
    poll_errors: int


@dataclass(frozen=True)
class _DueRow:
    expected_filings_id: int
    instrument_id: int
    symbol: str
    cik: str
    expected_filing_type: str
    baseline_accession: str | None


def _select_due(
    conn: psycopg.Connection[tuple],
    *,
    now: datetime,
    max_subjects: int,
) -> list[_DueRow]:
    """Unfulfilled rows whose window is open and poll interval elapsed.

    Most-stale-first (``last_polled_at NULLS FIRST``) so a large in-window
    set round-robins under the subject cap rather than starving the tail.
    """
    rows = conn.execute(
        """
        SELECT ef.id, ef.instrument_id, i.symbol, ei.identifier_value,
               ef.expected_filing_type, ef.baseline_accession
        FROM expected_filings ef
        JOIN instruments i ON i.instrument_id = ef.instrument_id
        JOIN external_identifiers ei
          ON ei.instrument_id = ef.instrument_id
         AND ei.provider = 'sec'
         AND ei.identifier_type = 'cik'
         AND ei.is_primary = TRUE
        WHERE ef.fulfilled_at IS NULL
          AND %(today)s BETWEEN ef.expected_window_start AND ef.expected_window_end
          AND (ef.last_polled_at IS NULL
               OR ef.last_polled_at < %(now)s - make_interval(mins => ef.poll_interval_minutes))
        ORDER BY ef.last_polled_at ASC NULLS FIRST, ef.id ASC
        LIMIT %(cap)s
        """,
        {"today": now.date(), "now": now, "cap": max_subjects},
    ).fetchall()
    return [
        _DueRow(
            expected_filings_id=int(r[0]),
            instrument_id=int(r[1]),
            symbol=str(r[2]),
            cik=str(r[3]),
            expected_filing_type=str(r[4]),
            baseline_accession=r[5],
        )
        for r in rows
    ]


def run_expected_filings_poller(
    conn: psycopg.Connection[tuple],
    *,
    http_get: HttpGet,
    now: datetime,
    user_agent: str,
    max_subjects: int = 100,
) -> PollStats:
    """Poll submissions.json for due rows; force-refresh on a matching filing.

    Commit discipline mirrors ``run_force_refresh``: the read tx (select)
    is committed before any HTTP-backed work so each per-subject mutation
    runs as a top-level transaction.
    """
    due = _select_due(conn, now=now, max_subjects=max_subjects)
    conn.commit()

    fulfilled = 0
    errors = 0
    for row in due:
        source = _SOURCE_FOR_FORM[row.expected_filing_type]
        try:
            delta = check_freshness(
                http_get,
                cik=row.cik,
                last_known_filing_id=row.baseline_accession,
                sources={source},  # type: ignore[arg-type]
                user_agent=user_agent,
            )
        except Exception:
            logger.exception(
                "expected_filings_poller: probe failed instrument_id=%d cik=%s",
                row.instrument_id,
                row.cik,
            )
            # Bump last_polled_at on failure too, so a bad CIK / network
            # error respects poll_interval_minutes instead of staying
            # most-stale and re-firing every tick (which would starve
            # healthy due rows under the subject cap).
            conn.execute(
                "UPDATE expected_filings SET last_polled_at = %s WHERE id = %s",
                (now, row.expected_filings_id),
            )
            conn.commit()
            errors += 1
            continue

        match = match_filing(delta.new_filings, row.expected_filing_type)
        if match is None or match.source is None:
            conn.execute(
                "UPDATE expected_filings SET last_polled_at = %s WHERE id = %s",
                (now, row.expected_filings_id),
            )
            conn.commit()
            continue

        # Record the discovered filing first (keeps the manifest invariant
        # and lets the next daily seed observe the advanced filing) — then
        # force-refresh fundamentals, then mark fulfilled.
        record_manifest_entry(
            conn,
            match.accession_number,
            cik=match.cik,
            form=match.form,
            source=match.source,
            subject_type="issuer",
            subject_id=str(row.instrument_id),
            instrument_id=row.instrument_id,
            filed_at=match.filed_at,
            accepted_at=match.accepted_at,
            primary_document_url=match.primary_document_url,
            is_amendment=match.is_amendment,
        )
        conn.commit()

        run_force_refresh(conn, [row.symbol])

        conn.execute(
            """
            UPDATE expected_filings
            SET fulfilled_at = %s, fulfilled_accession = %s, last_polled_at = %s
            WHERE id = %s
            """,
            (now, match.accession_number, now, row.expected_filings_id),
        )
        conn.commit()
        fulfilled += 1
        logger.info(
            "expected_filings_poller: fulfilled %s %s accession=%s",
            row.symbol,
            row.expected_filing_type,
            match.accession_number,
        )

    return PollStats(subjects_polled=len(due), fulfilled=fulfilled, poll_errors=errors)
