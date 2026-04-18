"""Refresh cascade service (#276 Chunk K).

After ``daily_financial_facts`` commits new fundamentals + normalizes
periods, this service propagates the change to thesis and scoring:

1. Drain the durable retry outbox (K.2). Queued instruments bypass
   the stale gate — the outbox IS the signal that a thesis refresh
   is owed from a prior failure.
2. Map the refresh plan's successful CIKs (refreshes + submissions-
   only, minus per-CIK failures) to instrument_ids.
3. For each instrument, check ``find_stale_instruments`` — the event-
   driven predicate shipped in #273 flags any whose thesis lags a
   qualifying filing.
4. Generate a fresh thesis (Claude) for each queued retry + each
   stale instrument.
5. If any thesis refreshed this cycle, re-run ``compute_rankings``
   once for the full pool — scoring reads thesis fields so fresh
   theses can move every score, not just the cascade's subset.
6. Clear retry-queue rows for processed successes ONLY after the
   rerank succeeds. Rerank failure leaves the rows (and marks new-
   work successes with a RERANK_NEEDED marker) so the next cycle
   has a durable "rankings recompute needed" signal.

The full-pool rerank is the Option-α scoring approach from the
master plan — subset scoring was ruled out because ``compute_rankings``
assigns global rank and per-instrument score rows without the full
pool would have NULL / mismatched rank values.

Per-instrument thesis failures are isolated — one bad CIK does not
abort the loop or the subsequent rerank. Future K.3 adds session-
level advisory locking against ``daily_thesis_refresh``.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

import anthropic
import psycopg

from app.services.scoring import compute_rankings
from app.services.sec_incremental import RefreshOutcome, RefreshPlan
from app.services.thesis import find_stale_instruments, generate_thesis

logger = logging.getLogger(__name__)

ATTEMPT_CAP: int = 5
RERANK_MARKER: str = "RERANK_NEEDED"


@dataclass(frozen=True)
class CascadeOutcome:
    """Result of one ``cascade_refresh`` run.

    ``failed`` is stored as a tuple to preserve the ``frozen=True``
    immutability invariant — a ``list`` field would be attribute-
    immutable but value-mutable, which is a well-known dataclass
    footgun.
    """

    instruments_considered: int
    thesis_refreshed: int
    rankings_recomputed: bool
    retries_drained: int = 0
    failed: tuple[tuple[int, str], ...] = ()


def changed_instruments_from_outcome(
    conn: psycopg.Connection[Any],
    plan: RefreshPlan,
    outcome: RefreshOutcome,
) -> list[int]:
    """Map CIKs that succeeded this cycle to instrument_ids.

    Drops plan.seeds — seeds don't cascade (fresh-install Claude-call
    storm protection). Drops CIKs present in outcome.failed. Keeps
    refreshes (fundamentals-changing) and submissions_only_advances
    (8-K etc. — thesis context uses filings).
    """
    failed_ciks = {cik for cik, _reason in outcome.failed}
    seed_ciks = set(plan.seeds)
    excluded = failed_ciks | seed_ciks

    ciks = [cik for cik, _accession in plan.refreshes if cik not in excluded]
    ciks.extend(cik for cik, _accession in plan.submissions_only_advances if cik not in excluded)

    if not ciks:
        return []

    # Pad first, then de-dupe. Thesis staleness is keyed per
    # instrument, not per filing, so same-CIK double filings
    # collapse to one mapping; the event predicate in
    # find_stale_instruments picks up the newest filing regardless.
    # Pre-pad closes the gap where both "320193" and "0000320193"
    # would pass an unpadded seen-check and emit a duplicate after
    # padding. Invariant: every CIK reaching this function
    # originates from _zero_pad_cik (parse_master_index or the
    # external_identifiers store) and is already a 10-digit digit
    # string. str.zfill(10) is therefore a belt-and-braces pad for
    # any future caller that hands us a raw-integer CIK — it is
    # total (no ValueError) and is a no-op on already-padded input.
    padded = [cik.zfill(10) for cik in ciks]
    seen: set[str] = set()
    unique_ciks = [cik for cik in padded if not (cik in seen or seen.add(cik))]

    rows = conn.execute(
        """
        SELECT DISTINCT i.instrument_id
        FROM instruments i
        JOIN external_identifiers ei
            ON ei.instrument_id = i.instrument_id
           AND ei.provider = 'sec'
           AND ei.identifier_type = 'cik'
           AND ei.identifier_value = ANY(%s)
           AND ei.is_primary = TRUE
        WHERE i.is_tradable = TRUE
        ORDER BY i.instrument_id
        """,
        (unique_ciks,),
    ).fetchall()
    return [int(r[0]) for r in rows]


# ---------------------------------------------------------------------------
# Retry outbox helpers (K.2)
# ---------------------------------------------------------------------------


def enqueue_retry(
    conn: psycopg.Connection[Any],
    instrument_id: int,
    error_type: str,
) -> None:
    """UPSERT a retry row after a thesis failure.

    Caller MUST ``conn.rollback()`` before invoking this if the
    connection is in INERROR state from the failing thesis call.

    Transaction semantics: ``with conn.transaction():`` opens a
    new transaction when the connection has no open tx, or a
    savepoint when nested. In cascade_refresh's flow the outer
    conn has read transactions from drain_retry_queue /
    find_stale_instruments, so this write lands on a savepoint —
    durable only after the scheduler's ``conn.commit()`` that
    follows ``cascade_refresh`` returning. The scheduler commits
    immediately after cascade_refresh returns and before the
    failure-surfacing raise, so in practice the outbox write is
    durable whenever cascade_refresh exits cleanly.

    ``attempt_count`` semantics: first enqueue sets 1. Subsequent
    thesis failures increment by 1. A pre-existing RERANK_NEEDED
    marker (attempt_count=0) transitions into the thesis-failure
    path here — the UPDATE increments from 0 to 1 as expected.
    """
    with conn.transaction():
        conn.execute(
            """
            INSERT INTO cascade_retry_queue
                (instrument_id, attempt_count, last_error, last_attempted_at)
            VALUES (%s, 1, %s, NOW())
            ON CONFLICT (instrument_id) DO UPDATE SET
                attempt_count = cascade_retry_queue.attempt_count + 1,
                last_error = EXCLUDED.last_error,
                last_attempted_at = NOW()
            """,
            (instrument_id, error_type),
        )


def enqueue_rerank_marker(
    conn: psycopg.Connection[Any],
    instrument_id: int,
) -> None:
    """UPSERT a RERANK_NEEDED marker for a thesis-success-then-
    rerank-failure instrument.

    Sets ``attempt_count=0`` so rerank failures do NOT consume the
    thesis retry budget. On CONFLICT, resets any prior thesis-
    failure state (including at-cap rows) to RERANK_NEEDED /
    attempt_count=0 — a thesis success this cycle means the prior
    blocker is no longer current and the row must be drainable
    again for the next rerank attempt.
    """
    with conn.transaction():
        conn.execute(
            """
            INSERT INTO cascade_retry_queue
                (instrument_id, attempt_count, last_error, last_attempted_at)
            VALUES (%s, 0, %s, NOW())
            ON CONFLICT (instrument_id) DO UPDATE SET
                attempt_count = 0,
                last_error = EXCLUDED.last_error,
                last_attempted_at = NOW()
            """,
            (instrument_id, RERANK_MARKER),
        )


def clear_retry_success(
    conn: psycopg.Connection[Any],
    instrument_id: int,
) -> None:
    """DELETE the retry row for an instrument whose cascade succeeded
    this cycle (thesis refreshed + rerank succeeded). Idempotent —
    no-op if the row is absent. Wraps its own transaction."""
    with conn.transaction():
        conn.execute(
            "DELETE FROM cascade_retry_queue WHERE instrument_id = %s",
            (instrument_id,),
        )


def drain_retry_queue(
    conn: psycopg.Connection[Any],
    cap: int = ATTEMPT_CAP,
) -> list[int]:
    """Return instrument_ids eligible for retry — rows with
    ``attempt_count < cap``, ordered by ``enqueued_at`` ASC (oldest
    first). Rows at or above cap are left in place for admin
    inspection (surfaced in Chunk H / K.4)."""
    rows = conn.execute(
        """
        SELECT instrument_id
        FROM cascade_retry_queue
        WHERE attempt_count < %s
        ORDER BY enqueued_at ASC
        """,
        (cap,),
    ).fetchall()
    return [int(r[0]) for r in rows]


# ---------------------------------------------------------------------------
# cascade_refresh
# ---------------------------------------------------------------------------


def cascade_refresh(
    conn: psycopg.Connection[Any],
    client: anthropic.Anthropic,
    instrument_ids: list[int],
) -> CascadeOutcome:
    """Run the cascade.

    Flow:
    1. Drain the retry outbox — queued instrument_ids bypass the
       stale gate. Regenerating a thesis that another path already
       refreshed is idempotent-wasted, not incorrect.
    2. Run the event-driven stale predicate on ``instrument_ids``.
    3. For each (retry + stale) instrument, call ``generate_thesis``.
       Successes accumulate in ``processed_ok`` for deferred clear.
       Failures roll back first, then enqueue into the outbox in a
       fresh transaction.
    4. If any thesis refreshed, run ``compute_rankings`` once.
       - On rerank success: clear each ``processed_ok`` queue row.
       - On rerank failure: rollback, record (-1, ExcType) in
         ``failed``, and UPSERT a RERANK_NEEDED marker for each
         ``processed_ok`` id — the queue is the durable rankings-
         recompute signal for the next cycle.
    """
    retry_ids = drain_retry_queue(conn)
    if retry_ids:
        logger.info("cascade_refresh: drained %d retries from queue", len(retry_ids))

    stale = find_stale_instruments(conn, tier=None, instrument_ids=instrument_ids) if instrument_ids else []

    if not retry_ids and not stale:
        logger.info(
            "cascade_refresh: %d instruments considered, 0 stale, 0 retries — no thesis or score refresh",
            len(instrument_ids),
        )
        return CascadeOutcome(
            instruments_considered=len(instrument_ids),
            thesis_refreshed=0,
            rankings_recomputed=False,
            retries_drained=0,
        )

    thesis_refreshed = 0
    failed: list[tuple[int, str]] = []
    processed_ok: list[int] = []

    # Retry path — bypass stale gate. Outbox IS the signal.
    retry_set = set(retry_ids)
    for iid in retry_ids:
        try:
            generate_thesis(iid, conn, client)
            thesis_refreshed += 1
            processed_ok.append(iid)
            logger.info("cascade_refresh: retry thesis refreshed for instrument_id=%d", iid)
        except Exception as exc:
            try:
                conn.rollback()
            except psycopg.Error:
                logger.debug(
                    "cascade_refresh: rollback suppressed after retry thesis exception",
                    exc_info=True,
                )
            try:
                enqueue_retry(conn, iid, type(exc).__name__)
            except Exception:
                # Broad catch intentional: any exception from the helper
                # (psycopg error, programming bug, context manager
                # machinery) must NOT break the per-instrument
                # isolation guarantee and abort remaining siblings.
                # The retry signal for this iid is lost this cycle;
                # the instrument re-enters the cascade on the next run
                # via the #273 event predicate.
                logger.exception(
                    "cascade_refresh: enqueue_retry failed for instrument_id=%d — retry signal lost",
                    iid,
                )
            failed.append((iid, type(exc).__name__))
            logger.exception("cascade_refresh: retry thesis failed for instrument_id=%d", iid)

    # New-work (stale) path — skip any ids already processed in the
    # retry loop so a queued CIK that also surfaces as stale is only
    # generated once per cycle.
    for stale_instrument in stale:
        iid = stale_instrument.instrument_id
        if iid in retry_set:
            continue
        try:
            generate_thesis(iid, conn, client)
            thesis_refreshed += 1
            processed_ok.append(iid)
            logger.info(
                "cascade_refresh: thesis refreshed for instrument_id=%d symbol=%s reason=%s",
                iid,
                stale_instrument.symbol,
                stale_instrument.reason,
            )
        except Exception as exc:
            try:
                conn.rollback()
            except psycopg.Error:
                logger.debug(
                    "cascade_refresh: rollback suppressed after thesis exception",
                    exc_info=True,
                )
            try:
                enqueue_retry(conn, iid, type(exc).__name__)
            except Exception:
                # Broad catch intentional: any exception from the helper
                # (psycopg error, programming bug, context manager
                # machinery) must NOT break the per-instrument
                # isolation guarantee and abort remaining siblings.
                # The retry signal for this iid is lost this cycle;
                # the instrument re-enters the cascade on the next run
                # via the #273 event predicate.
                logger.exception(
                    "cascade_refresh: enqueue_retry failed for instrument_id=%d — retry signal lost",
                    iid,
                )
            failed.append((iid, type(exc).__name__))
            logger.exception(
                "cascade_refresh: thesis failed for instrument_id=%d symbol=%s",
                iid,
                stale_instrument.symbol,
            )

    rankings_recomputed = False
    if thesis_refreshed > 0:
        try:
            ranking_result = compute_rankings(conn)
            rankings_recomputed = True
            logger.info(
                "cascade_refresh: rankings recomputed — %d scored",
                len(ranking_result.scored),
            )
            # Clear processed queue rows AFTER successful rerank.
            for iid in processed_ok:
                try:
                    clear_retry_success(conn, iid)
                except Exception:
                    # Broad catch — see enqueue_retry rationale above.
                    # A failed clear leaves the row for the next
                    # cycle to re-process (idempotent / wasted-but-
                    # safe thesis call), not an incorrect state.
                    logger.exception("cascade_refresh: clear_retry_success failed for instrument_id=%d", iid)
        except Exception as exc:
            # Rollback FIRST — compute_rankings is SQL-heavy and
            # can leave the connection in INERROR; the subsequent
            # marker inserts would otherwise fail, losing the
            # durable signal for exactly the path it must preserve.
            try:
                conn.rollback()
            except psycopg.Error:
                logger.debug(
                    "cascade_refresh: rollback suppressed after compute_rankings exception",
                    exc_info=True,
                )
            failed.append((-1, type(exc).__name__))  # -1 sentinel for non-instrument failure
            logger.exception("cascade_refresh: compute_rankings failed after thesis refresh")
            # UPSERT RERANK_NEEDED markers for each processed_ok id
            # so the next cycle re-drains them even if there was no
            # pre-existing row (new-work success path).
            for iid in processed_ok:
                try:
                    enqueue_rerank_marker(conn, iid)
                except Exception:
                    # Broad catch — non-psycopg failures from the
                    # helper (programming bug, CM internals) must
                    # not abort the marker loop and lose the signal
                    # for the remaining processed_ok ids.
                    logger.exception(
                        "cascade_refresh: enqueue_rerank_marker failed for instrument_id=%d — "
                        "rankings-recompute signal lost for this instrument",
                        iid,
                    )

    logger.info(
        "cascade_refresh summary: considered=%d stale=%d retries_drained=%d thesis_refreshed=%d rankings=%s failed=%d",
        len(instrument_ids),
        len(stale),
        len(retry_ids),
        thesis_refreshed,
        rankings_recomputed,
        len(failed),
    )

    return CascadeOutcome(
        instruments_considered=len(instrument_ids),
        thesis_refreshed=thesis_refreshed,
        rankings_recomputed=rankings_recomputed,
        retries_drained=len(retry_ids),
        failed=tuple(failed),
    )
