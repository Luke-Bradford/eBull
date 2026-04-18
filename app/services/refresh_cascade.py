"""Refresh cascade service (#276 Chunk K).

After ``daily_financial_facts`` commits new fundamentals + normalizes
periods, this service propagates the change to thesis and scoring:

1. Map the refresh plan's successful CIKs (refreshes + submissions-
   only, minus per-CIK failures) to instrument_ids.
2. For each instrument, check ``find_stale_instruments`` — the event-
   driven predicate shipped in #273 flags any whose thesis lags a
   qualifying filing.
3. Generate a fresh thesis (Claude) for each stale instrument.
4. If any thesis refreshed this cycle, re-run ``compute_rankings``
   once for the full pool — scoring reads thesis fields so fresh
   theses can move every score, not just the cascade's subset.

The full-pool rerank is the Option-α scoring approach from the
master plan — subset scoring was ruled out because ``compute_rankings``
assigns global rank and per-instrument score rows without the full
pool would have NULL / mismatched rank values.

Per-instrument thesis failures are isolated — one bad CIK does not
abort the loop or the subsequent rerank. Future K.3 adds session-
level advisory locking against ``daily_thesis_refresh``; future K.2
adds a durable retry outbox. K.1 (this module) is the basic wiring.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

import anthropic
import psycopg

from app.services.scoring import compute_rankings
from app.services.sec_incremental import RefreshOutcome, RefreshPlan
from app.services.thesis import find_stale_instruments, generate_thesis

logger = logging.getLogger(__name__)


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
    failed: tuple[tuple[int, str], ...] = field(default_factory=tuple)


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

    # De-dupe by CIK (intentional — not by accession). Thesis
    # staleness is keyed per instrument, not per filing, so if the
    # same CIK filed twice in the window we still only need to map
    # it once; the event predicate in find_stale_instruments will
    # pick up the newest filing regardless.
    # CIK zero-padding: the planner (plan_refresh -> parse_master_index)
    # already pads via _zero_pad_cik, but we pad again defensively
    # here so a future caller that hands us a raw-integer CIK string
    # doesn't silently miss rows against the zero-padded storage in
    # external_identifiers.identifier_value.
    seen: set[str] = set()
    unique_ciks = [str(int(cik)).zfill(10) for cik in ciks if not (cik in seen or seen.add(cik))]

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


def cascade_refresh(
    conn: psycopg.Connection[Any],
    client: anthropic.Anthropic,
    instrument_ids: list[int],
) -> CascadeOutcome:
    """Run the cascade for the given instrument_ids.

    For each instrument: ``find_stale_instruments`` (scoped via
    ``tier=None`` + ``instrument_ids``) decides whether the thesis
    needs refresh per #273's event-driven predicate. If stale,
    ``generate_thesis`` is called — it commits its own read tx before
    Claude per #293 + writes its own thesis row atomically.

    After the thesis loop: if any thesis was refreshed, call
    ``compute_rankings`` once for the full analysable pool. Scoring
    reads thesis fields so any thesis change can move every score.

    Per-instrument failures recorded in the outcome; one failure
    does not abort siblings or the subsequent rerank.
    """
    if not instrument_ids:
        return CascadeOutcome(instruments_considered=0, thesis_refreshed=0, rankings_recomputed=False)

    stale = find_stale_instruments(conn, tier=None, instrument_ids=instrument_ids)
    if not stale:
        logger.info(
            "cascade_refresh: %d instruments considered, 0 stale — no thesis or score refresh",
            len(instrument_ids),
        )
        return CascadeOutcome(
            instruments_considered=len(instrument_ids),
            thesis_refreshed=0,
            rankings_recomputed=False,
        )

    thesis_refreshed = 0
    failed: list[tuple[int, str]] = []

    for stale_instrument in stale:
        try:
            generate_thesis(stale_instrument.instrument_id, conn, client)
            thesis_refreshed += 1
            logger.info(
                "cascade_refresh: thesis refreshed for instrument_id=%d symbol=%s reason=%s",
                stale_instrument.instrument_id,
                stale_instrument.symbol,
                stale_instrument.reason,
            )
        except Exception as exc:
            # Attempt to roll back any half-open tx from generate_thesis
            # before continuing to siblings. generate_thesis wraps its
            # DB write in its own `with conn.transaction():` so the
            # failing CIK's row was already rolled back — this is
            # belt-and-braces for the pre-transaction reads that
            # opened the implicit tx before the Claude call failed.
            try:
                conn.rollback()
            except psycopg.Error:
                logger.debug(
                    "cascade_refresh: rollback suppressed after thesis exception",
                    exc_info=True,
                )
            failed.append((stale_instrument.instrument_id, type(exc).__name__))
            logger.exception(
                "cascade_refresh: thesis failed for instrument_id=%d symbol=%s",
                stale_instrument.instrument_id,
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
        except Exception as exc:
            try:
                conn.rollback()
            except psycopg.Error:
                logger.debug(
                    "cascade_refresh: rollback suppressed after compute_rankings exception",
                    exc_info=True,
                )
            failed.append((-1, type(exc).__name__))  # -1 sentinel for non-instrument failure
            logger.exception("cascade_refresh: compute_rankings failed after thesis refresh")

    logger.info(
        "cascade_refresh summary: considered=%d stale=%d thesis_refreshed=%d rankings=%s failed=%d",
        len(instrument_ids),
        len(stale),
        thesis_refreshed,
        rankings_recomputed,
        len(failed),
    )

    return CascadeOutcome(
        instruments_considered=len(instrument_ids),
        thesis_refreshed=thesis_refreshed,
        rankings_recomputed=rankings_recomputed,
        failed=tuple(failed),
    )
