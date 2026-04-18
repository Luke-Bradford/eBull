"""Filings backfill (#268 Chunk E).

Drives every tradable SEC-covered instrument toward a terminal
``coverage.filings_status`` by paging SEC ``submissions.json``
history + verifying 8-K completeness inside the 365-day window,
with bounded retries only on recoverable failures.

Design doc: ``docs/superpowers/specs/2026-04-18-chunk-e-filings-backfill-design.md``.

Durability invariant: psycopg3 opens an implicit transaction on the
first ``execute`` against an idle connection. Any ``with
conn.transaction():`` that follows becomes a savepoint, not a durable
top-level commit. To keep per-page upserts durable against later
errors, this module's rule is: **before every ``with
conn.transaction():`` mutation block, call ``conn.commit()``**. It is
a no-op on an idle connection; on an implicit read-tx it commits
cheaply and puts the connection back in the idle state.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from enum import StrEnum
from typing import Any

import httpx
import psycopg

from app.providers.filings import FilingNotFound, FilingSearchResult
from app.providers.implementations.sec_edgar import (
    SecFilingsProvider,
    _normalise_submissions_block,
    _zero_pad_cik,
)
from app.services.coverage_audit import probe_status
from app.services.filings import _upsert_filing, _upsert_filing_event

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------
# Outcome enum + result dataclass
# ---------------------------------------------------------------------


class BackfillOutcome(StrEnum):
    """Terminal classification for one backfill pass.

    Values are persisted into ``coverage.filings_backfill_reason``.
    See design doc §BackfillOutcome for the semantics of each value.
    """

    COMPLETE_OK = "COMPLETE_OK"
    COMPLETE_FPI = "COMPLETE_FPI"
    STILL_INSUFFICIENT_EXHAUSTED = "STILL_INSUFFICIENT_EXHAUSTED"
    STILL_INSUFFICIENT_STRUCTURALLY_YOUNG = "STILL_INSUFFICIENT_STRUCTURALLY_YOUNG"
    STILL_INSUFFICIENT_HTTP_ERROR = "STILL_INSUFFICIENT_HTTP_ERROR"
    STILL_INSUFFICIENT_PARSE_ERROR = "STILL_INSUFFICIENT_PARSE_ERROR"
    SKIPPED_ATTEMPTS_CAP = "SKIPPED_ATTEMPTS_CAP"
    SKIPPED_BACKOFF_WINDOW = "SKIPPED_BACKOFF_WINDOW"


_RETRYABLE_REASONS: frozenset[str] = frozenset(
    {
        BackfillOutcome.STILL_INSUFFICIENT_HTTP_ERROR.value,
        BackfillOutcome.STILL_INSUFFICIENT_PARSE_ERROR.value,
    }
)


@dataclass(frozen=True)
class BackfillResult:
    instrument_id: int
    outcome: BackfillOutcome
    pages_fetched: int
    filings_upserted: int
    eight_k_gap_filled: int
    final_status: str


# Tunables (module-level for test override).
ATTEMPTS_CAP: int = 3
BACKOFF_DAYS: int = 7
EIGHT_K_WINDOW_DAYS: int = 365


# ---------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------


def _is_structurally_young(conn: psycopg.Connection[Any], instrument_id: int) -> bool:
    """True iff the instrument's earliest SEC filing is strictly
    newer than today - 18 months (calendar-correct via SQL INTERVAL).

    False when no filings exist at all — we can't prove youth
    without an earliest filing, so classify those as EXHAUSTED,
    not YOUNG (design doc v2-H3).

    Step 3 upserts every fetched filing to ``filing_events`` before
    step 5 calls this helper, so the DB query is the authoritative
    union of DB + just-fetched.
    """
    row = conn.execute(
        """
        SELECT MIN(filing_date) > (CURRENT_DATE - INTERVAL '18 months')
        FROM filing_events
        WHERE instrument_id = %s AND provider = 'sec'
        """,
        (instrument_id,),
    ).fetchone()
    conn.commit()  # M1 invariant.
    return bool(row[0]) if row is not None and row[0] is not None else False


# ---------------------------------------------------------------------
# Single coverage-write sink
# ---------------------------------------------------------------------


def _finalise(
    conn: psycopg.Connection[Any],
    instrument_id: int,
    *,
    outcome: BackfillOutcome,
    status: str | None,
    pages_fetched: int = 0,
    filings_upserted: int = 0,
    eight_k_gap_filled: int = 0,
) -> BackfillResult:
    """Single coverage-write path shared by all terminal outcomes.

    attempts delta by outcome:

    - ``COMPLETE_OK`` / ``COMPLETE_FPI``           -> set 0
    - ``HTTP_ERROR`` / ``PARSE_ERROR``             -> += 1
    - ``EXHAUSTED`` / ``STRUCTURALLY_YOUNG``       -> unchanged
    - ``SKIPPED_*``                                 -> no write at all

    ``status`` semantics:

    - ``None`` = preserve current ``filings_status``. Used by
      retryable errors so a correctly-classified
      ``structurally_young`` row is not demoted on transient
      failure (design doc v4-H2).
    - otherwise the UPDATE writes this value into ``filings_status``.

    Commits before the UPDATE (M1 invariant) and after (K.2/K.3
    durability pattern).
    """
    if outcome in (
        BackfillOutcome.SKIPPED_ATTEMPTS_CAP,
        BackfillOutcome.SKIPPED_BACKOFF_WINDOW,
    ):
        # Gating path — no mutation at all.
        return BackfillResult(
            instrument_id=instrument_id,
            outcome=outcome,
            pages_fetched=0,
            filings_upserted=0,
            eight_k_gap_filled=0,
            final_status="",
        )

    # attempts delta is one of three shapes — parameterising the
    # SQL keeps the query a ``LiteralString`` (pyright strict).
    reset_attempts = outcome in (
        BackfillOutcome.COMPLETE_OK,
        BackfillOutcome.COMPLETE_FPI,
    )
    increment_attempts = outcome in (
        BackfillOutcome.STILL_INSUFFICIENT_HTTP_ERROR,
        BackfillOutcome.STILL_INSUFFICIENT_PARSE_ERROR,
    )
    # EXHAUSTED / STRUCTURALLY_YOUNG leave attempts unchanged.

    conn.commit()  # M1 invariant before mutation.
    if status is not None and reset_attempts:
        result = conn.execute(
            """
            UPDATE coverage
            SET filings_status            = %s,
                filings_backfill_attempts = 0,
                filings_backfill_last_at  = NOW(),
                filings_backfill_reason   = %s,
                filings_audit_at          = NOW()
            WHERE instrument_id = %s
            RETURNING filings_status
            """,
            (status, outcome.value, instrument_id),
        )
    elif status is not None and increment_attempts:
        result = conn.execute(
            """
            UPDATE coverage
            SET filings_status            = %s,
                filings_backfill_attempts = filings_backfill_attempts + 1,
                filings_backfill_last_at  = NOW(),
                filings_backfill_reason   = %s,
                filings_audit_at          = NOW()
            WHERE instrument_id = %s
            RETURNING filings_status
            """,
            (status, outcome.value, instrument_id),
        )
    elif status is not None:
        # EXHAUSTED / STRUCTURALLY_YOUNG.
        result = conn.execute(
            """
            UPDATE coverage
            SET filings_status            = %s,
                filings_backfill_last_at  = NOW(),
                filings_backfill_reason   = %s,
                filings_audit_at          = NOW()
            WHERE instrument_id = %s
            RETURNING filings_status
            """,
            (status, outcome.value, instrument_id),
        )
    elif increment_attempts:
        # status=None preservation path for HTTP/PARSE errors
        # (design doc v4-H2 — never demote structurally_young on
        # transient failure).
        result = conn.execute(
            """
            UPDATE coverage
            SET filings_backfill_attempts = filings_backfill_attempts + 1,
                filings_backfill_last_at  = NOW(),
                filings_backfill_reason   = %s
            WHERE instrument_id = %s
            RETURNING filings_status
            """,
            (outcome.value, instrument_id),
        )
    else:
        # status=None and no attempts change — currently unused
        # but keep the branch explicit for future outcomes.
        result = conn.execute(
            """
            UPDATE coverage
            SET filings_backfill_last_at = NOW(),
                filings_backfill_reason  = %s
            WHERE instrument_id = %s
            RETURNING filings_status
            """,
            (outcome.value, instrument_id),
        )
    row = result.fetchone()
    final = str(row[0]) if row is not None and row[0] is not None else ""
    conn.commit()  # K.2/K.3 durability.

    return BackfillResult(
        instrument_id=instrument_id,
        outcome=outcome,
        pages_fetched=pages_fetched,
        filings_upserted=filings_upserted,
        eight_k_gap_filled=eight_k_gap_filled,
        final_status=final,
    )


# ---------------------------------------------------------------------
# Gating
# ---------------------------------------------------------------------


def _check_gating(conn: psycopg.Connection[Any], instrument_id: int) -> BackfillOutcome | None:
    """Return a gating outcome or ``None`` to proceed.

    Cap rule exempts ``structurally_young`` rows (design doc v5-H1)
    so an aged-out young issuer can be demoted to ``insufficient``
    once backfill completes cleanly.
    """
    row = conn.execute(
        """
        SELECT filings_backfill_attempts, filings_backfill_last_at,
               filings_backfill_reason, filings_status
        FROM coverage
        WHERE instrument_id = %s
        """,
        (instrument_id,),
    ).fetchone()
    conn.commit()  # M1 invariant.

    if row is None:
        # Bootstrap invariant violation — raise loudly.
        raise RuntimeError(f"backfill_filings: no coverage row for instrument_id={instrument_id}")

    attempts = int(row[0]) if row[0] is not None else 0
    last_at: datetime | None = row[1]
    last_reason: str | None = row[2]
    filings_status: str | None = row[3]

    if last_at is not None:
        # Backoff check. Use UTC-naive-aware comparison: psycopg3 returns
        # tz-aware datetime; compare against tz-aware now.
        cutoff = datetime.now(last_at.tzinfo) - timedelta(days=BACKOFF_DAYS)
        if last_at > cutoff:
            return BackfillOutcome.SKIPPED_BACKOFF_WINDOW

    if attempts >= ATTEMPTS_CAP and last_reason in _RETRYABLE_REASONS and filings_status != "structurally_young":
        return BackfillOutcome.SKIPPED_ATTEMPTS_CAP

    return None


# ---------------------------------------------------------------------
# Main entry
# ---------------------------------------------------------------------


def backfill_filings(
    conn: psycopg.Connection[Any],
    provider: SecFilingsProvider,
    cik: str,
    instrument_id: int,
) -> BackfillResult:
    """Page SEC submissions history for ``cik`` + reconcile 8-K gaps,
    then write one terminal ``coverage.filings_status`` row.

    See ``docs/superpowers/specs/2026-04-18-chunk-e-filings-backfill-design.md``
    for the full flow + outcome table.
    """
    gated = _check_gating(conn, instrument_id)
    if gated is not None:
        return _finalise(conn, instrument_id, outcome=gated, status=None)

    cik_padded = _zero_pad_cik(cik)

    # Step 2: fetch primary submissions.json.
    try:
        submissions = provider.fetch_submissions(cik_padded)
    except httpx.HTTPError:
        logger.warning(
            "backfill_filings: HTTP error on fetch_submissions cik=%s",
            cik_padded,
            exc_info=True,
        )
        return _finalise(
            conn,
            instrument_id,
            outcome=BackfillOutcome.STILL_INSUFFICIENT_HTTP_ERROR,
            status=None,
        )
    except json.JSONDecodeError, TypeError, KeyError:
        logger.warning(
            "backfill_filings: PARSE error on fetch_submissions cik=%s",
            cik_padded,
            exc_info=True,
        )
        return _finalise(
            conn,
            instrument_id,
            outcome=BackfillOutcome.STILL_INSUFFICIENT_PARSE_ERROR,
            status=None,
        )

    if submissions is None:
        # 404 — CIK valid in external_identifiers but SEC has no
        # submissions for it. Classify retryable.
        return _finalise(
            conn,
            instrument_id,
            outcome=BackfillOutcome.STILL_INSUFFICIENT_HTTP_ERROR,
            status=None,
        )

    window_cutoff = date.today() - timedelta(days=EIGHT_K_WINDOW_DAYS)
    pages_fetched = 0
    filings_upserted = 0
    eight_k_gap_filled = 0
    seen_filings: list[FilingSearchResult] = []
    bar_met = False
    eight_k_window_covered = False

    # Phase A: inline `recent` block.
    try:
        filings_outer = submissions["filings"]
        if not isinstance(filings_outer, dict):
            raise TypeError("filings block not a dict")
        recent_block = filings_outer["recent"]
        if not isinstance(recent_block, dict):
            raise TypeError("recent block not a dict")
        recent_results = _normalise_submissions_block(recent_block, cik_padded)
    except KeyError, TypeError, ValueError, AttributeError:
        logger.warning(
            "backfill_filings: PARSE error on recent block cik=%s",
            cik_padded,
            exc_info=True,
        )
        return _finalise(
            conn,
            instrument_id,
            outcome=BackfillOutcome.STILL_INSUFFICIENT_PARSE_ERROR,
            status=None,
        )

    conn.commit()  # M1 invariant before mutation block.
    with conn.transaction():
        for r in recent_results:
            _upsert_filing(conn, str(instrument_id), "sec", r)
    seen_filings.extend(recent_results)
    pages_fetched += 1
    filings_upserted += len(recent_results)

    bar_met = probe_status(conn, instrument_id) in ("analysable", "fpi")
    if recent_results:
        oldest_recent = min(r.filed_at.date() for r in recent_results)
        if oldest_recent <= window_cutoff:
            eight_k_window_covered = True

    # Phase B: files[] pagination.
    files_meta = filings_outer.get("files") or []
    if not isinstance(files_meta, list):
        files_meta = []

    try:
        entries = sorted(
            files_meta,
            key=lambda e: date.fromisoformat(str(e["filingTo"])),
            reverse=True,
        )
    except KeyError, TypeError, ValueError:
        # Missing/malformed filingTo — fall back to reversed original
        # order (SEC documents files[] as chronological oldest→newest).
        entries = list(reversed(files_meta))

    for entry in entries:
        if bar_met and eight_k_window_covered:
            break  # nothing further to fetch (design doc v4-H1)

        entry_name = entry.get("name") if isinstance(entry, dict) else None
        if not entry_name:
            continue

        try:
            page_raw = provider.fetch_submissions_page(str(entry_name))
        except httpx.HTTPError:
            logger.warning(
                "backfill_filings: HTTP error on page cik=%s name=%s",
                cik_padded,
                entry_name,
                exc_info=True,
            )
            return _finalise(
                conn,
                instrument_id,
                outcome=BackfillOutcome.STILL_INSUFFICIENT_HTTP_ERROR,
                status=None,
                pages_fetched=pages_fetched,
                filings_upserted=filings_upserted,
            )
        except json.JSONDecodeError, TypeError, KeyError:
            # ``fetch_submissions_page`` calls ``resp.json()`` internally
            # which can raise on malformed bytes. Classify retryable.
            logger.warning(
                "backfill_filings: PARSE error on page cik=%s name=%s",
                cik_padded,
                entry_name,
                exc_info=True,
            )
            return _finalise(
                conn,
                instrument_id,
                outcome=BackfillOutcome.STILL_INSUFFICIENT_PARSE_ERROR,
                status=None,
                pages_fetched=pages_fetched,
                filings_upserted=filings_upserted,
            )
        if page_raw is None:
            # 404 on a page the primary response claimed exists — data
            # integrity; classify retryable.
            logger.warning(
                "backfill_filings: 404 on page cik=%s name=%s",
                cik_padded,
                entry_name,
            )
            return _finalise(
                conn,
                instrument_id,
                outcome=BackfillOutcome.STILL_INSUFFICIENT_HTTP_ERROR,
                status=None,
                pages_fetched=pages_fetched,
                filings_upserted=filings_upserted,
            )

        try:
            page_results = _normalise_submissions_block(page_raw, cik_padded)
        except KeyError, TypeError, ValueError, AttributeError:
            logger.warning(
                "backfill_filings: PARSE error on page cik=%s name=%s",
                cik_padded,
                entry_name,
                exc_info=True,
            )
            return _finalise(
                conn,
                instrument_id,
                outcome=BackfillOutcome.STILL_INSUFFICIENT_PARSE_ERROR,
                status=None,
                pages_fetched=pages_fetched,
                filings_upserted=filings_upserted,
            )

        conn.commit()  # M1 invariant.
        with conn.transaction():
            for r in page_results:
                _upsert_filing(conn, str(instrument_id), "sec", r)
        seen_filings.extend(page_results)
        pages_fetched += 1
        filings_upserted += len(page_results)

        if not bar_met:
            bar_met = probe_status(conn, instrument_id) in ("analysable", "fpi")

        if page_results:
            page_oldest = min(r.filed_at.date() for r in page_results)
            if page_oldest <= window_cutoff:
                eight_k_window_covered = True

    # Step 4: 8-K gap reconciliation.
    conn.commit()  # M1 invariant.
    db_rows = conn.execute(
        """
        SELECT provider_filing_id
        FROM filing_events
        WHERE instrument_id = %s
          AND provider = 'sec'
          AND filing_type = '8-K'
          AND filing_date >= %s
        """,
        (instrument_id, window_cutoff),
    ).fetchall()
    conn.commit()  # M1 invariant.
    db_eight_ks = {str(r[0]) for r in db_rows}

    fetched_eight_ks = {
        r.provider_filing_id for r in seen_filings if r.filing_type == "8-K" and r.filed_at.date() >= window_cutoff
    }

    for missing_accession in sorted(fetched_eight_ks - db_eight_ks):
        try:
            event = provider.get_filing(missing_accession)
        except FilingNotFound:
            continue  # SEC deleted between pages; skip.
        except httpx.HTTPError:
            logger.warning(
                "backfill_filings: HTTP error on get_filing accession=%s",
                missing_accession,
                exc_info=True,
            )
            return _finalise(
                conn,
                instrument_id,
                outcome=BackfillOutcome.STILL_INSUFFICIENT_HTTP_ERROR,
                status=None,
                pages_fetched=pages_fetched,
                filings_upserted=filings_upserted,
                eight_k_gap_filled=eight_k_gap_filled,
            )

        conn.commit()  # M1 invariant.
        with conn.transaction():
            _upsert_filing_event(conn, instrument_id, "sec", event)
        eight_k_gap_filled += 1

    # Step 5: terminal classification + single coverage write.
    final_status = probe_status(conn, instrument_id)

    if final_status == "analysable":
        return _finalise(
            conn,
            instrument_id,
            outcome=BackfillOutcome.COMPLETE_OK,
            status="analysable",
            pages_fetched=pages_fetched,
            filings_upserted=filings_upserted,
            eight_k_gap_filled=eight_k_gap_filled,
        )
    if final_status == "fpi":
        return _finalise(
            conn,
            instrument_id,
            outcome=BackfillOutcome.COMPLETE_FPI,
            status="fpi",
            pages_fetched=pages_fetched,
            filings_upserted=filings_upserted,
            eight_k_gap_filled=eight_k_gap_filled,
        )
    if final_status == "insufficient":
        if _is_structurally_young(conn, instrument_id):
            return _finalise(
                conn,
                instrument_id,
                outcome=BackfillOutcome.STILL_INSUFFICIENT_STRUCTURALLY_YOUNG,
                status="structurally_young",
                pages_fetched=pages_fetched,
                filings_upserted=filings_upserted,
                eight_k_gap_filled=eight_k_gap_filled,
            )
        return _finalise(
            conn,
            instrument_id,
            outcome=BackfillOutcome.STILL_INSUFFICIENT_EXHAUSTED,
            status="insufficient",
            pages_fetched=pages_fetched,
            filings_upserted=filings_upserted,
            eight_k_gap_filled=eight_k_gap_filled,
        )
    if final_status == "no_primary_sec_cik":
        raise RuntimeError(
            f"backfill_filings: unexpected no_primary_sec_cik for "
            f"instrument_id={instrument_id}; eligibility filter should "
            f"have excluded this row"
        )
    raise RuntimeError(f"backfill_filings: unknown classifier status: {final_status!r}")
