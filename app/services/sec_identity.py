"""Shared helpers for resolving SEC issuer CIKs to instrument siblings.

Per #1102, multiple instruments may share an SEC CIK — share-class
siblings (GOOG/GOOGL, BRK.A/BRK.B) co-bind the issuer CIK because the
CIK identifies the entity (issuer) and the CUSIP identifies the
security (per-share-class).

Per-filing parsers writing per-instrument observation rows must fan
out across all siblings, not collapse to one. The bulk-ingester
multimap pattern (`dict[str, list[int]]`) covers the bulk path; this
module covers the per-filing path. See
``docs/superpowers/specs/2026-05-10-1117-filings-fanout-complete.md``.

Cross-reference: data-engineer skill §12.B (canonical sibling
fan-out lookup pattern); §12.A documents the single-canonical-pick
variant for entity-level row writes.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

import psycopg


def siblings_for_issuer_cik(conn: psycopg.Connection[Any], cik: str) -> list[int]:
    """Return all instrument_ids sharing this issuer CIK.

    The CIK is normalised (strip + zero-pad to 10 digits) before
    lookup; callers may pass either form.

    Ordering is deterministic (instrument_id ASC), NOT semantically
    primary. Per-instrument fan-out callers iterate the full list.
    Callers that need a single canonical sibling for entity-level
    row writes use :func:`pick_entity_instrument` (the #828 policy,
    made final by the #2108 verdict). ``instruments.is_primary_listing``
    is NOT usable as an entity-primary marker — it is a per-symbol
    dedup flag and both siblings carry ``true`` in every ambiguous
    set (#2108 full-pop scan).

    Raises ``ValueError`` if ``cik`` is not numeric after
    normalisation. Empty string and obvious non-CIK input fail
    fast — silent zero-pad of garbage would mask data-quality
    issues upstream.
    """
    cik_padded = str(cik).strip().zfill(10)
    if not cik_padded.isdigit() or len(cik_padded) != 10:
        raise ValueError(f"non-numeric or wrong-length CIK: {cik!r}")
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT instrument_id
            FROM external_identifiers
            WHERE provider = 'sec'
              AND identifier_type = 'cik'
              AND identifier_value = %s
            ORDER BY instrument_id
            """,
            (cik_padded,),
        )
        return [int(row[0]) for row in cur.fetchall()]


def sibling_instruments_for_instrument(conn: psycopg.Connection[Any], instrument_id: int) -> list[int]:
    """Return the share-class sibling set containing this instrument.

    Instrument → its CURRENT primary sec/cik value → every instrument
    currently primary-bound to it. Falls back to ``[instrument_id]``
    when the instrument has no CIK binding (the result always contains
    the input, so ``instrument_id = ANY(result)`` reads never narrow).

    Both sides pin ``is_primary = TRUE``: the CIK upsert path demotes
    an instrument's superseded CIK rows to ``is_primary = FALSE``
    (#1173), and joining through a demoted value could union across
    ISSUER boundaries if a historical CIK were ever recycled or
    shared. Zero such rows exist today (2026-07-22 full-pop: 8
    demoted rows, none co-bound to another instrument — Codex ckpt-2
    on #2108), so the filter costs nothing and closes the structural
    hole.

    #2108: this is the read-side sibling-union used where an
    entity-level insider table is filtered per instrument at
    request time and the ``filing_events`` bridge is too expensive
    (measured 5× per resolver call on the worst-case instrument).
    Reach is a superset of the direct-key read because #828 PR-1
    keeps entity-row ``instrument_id`` inside the sibling set by
    construction.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT sib.instrument_id
            FROM external_identifiers own
            JOIN external_identifiers sib
              ON sib.identifier_value = own.identifier_value
             AND sib.provider = 'sec'
             AND sib.identifier_type = 'cik'
             AND sib.is_primary = TRUE
            WHERE own.provider = 'sec'
              AND own.identifier_type = 'cik'
              AND own.is_primary = TRUE
              AND own.instrument_id = %s
            ORDER BY sib.instrument_id
            """,
            (int(instrument_id),),
        )
        siblings = [int(row[0]) for row in cur.fetchall()]
    if int(instrument_id) not in siblings:
        siblings.append(int(instrument_id))
        siblings.sort()
    return siblings


@dataclass(frozen=True)
class InsiderWriterRouting:
    """Routing decision for one insider-filing (Form 3/4/5) write (#828 PR-1).

    ``entity_instrument_id`` — the instrument_id to stamp on entity-level
    rows (``insider_filings``, ``insider_transactions``,
    ``insider_initial_holdings``).

    ``sibling_instrument_ids`` — the issuer's share-class sibling set
    resolved from ``parsed.issuer_cik`` via the production resolver
    (``siblings_for_issuer_cik``). Empty when the issuer CIK is unknown,
    invalid, or maps to no in-universe instrument (the unroutable
    cohort — discovery linkage is kept).

    ``is_mislink`` — TRUE when the discovery-time instrument is NOT in a
    non-empty sibling set: the filing was discovered via the reporting
    OWNER's EDGAR stream (a Form 4 for BAC surfacing in Berkshire's
    submissions feed) and must not bind to the owner's instrument.
    """

    entity_instrument_id: int
    sibling_instrument_ids: list[int]
    is_mislink: bool


def pick_entity_instrument(
    *,
    discovery_instrument_id: int,
    sibling_instrument_ids: Sequence[int],
    history_instrument_ids: Sequence[int],
) -> tuple[int, bool]:
    """Pure interim entity-row policy (#828 PR-1; spec
    docs/proposals/etl/2026-07-22-828-insider-cik-routing.md).

    Returns ``(entity_instrument_id, is_mislink)``.

    - Empty sibling set, or discovery instrument already a sibling →
      keep the discovery linkage (not a mislink).
    - Mislink: prefer the unambiguous ``instrument_cik_history``
      instrument when exactly one exists AND it is inside the sibling
      set (stale history outside the set would break the PR-2
      ``instrument_id = ANY(siblings)`` invariant); else
      ``min(sibling set)``. Bookkeeping-grade only — read paths bridge
      per-instrument via ``filing_events``; the display-grade policy is
      the #828 sub-ticket.
    """
    siblings = [int(i) for i in sibling_instrument_ids]
    if not siblings or int(discovery_instrument_id) in siblings:
        return int(discovery_instrument_id), False
    history = {int(i) for i in history_instrument_ids}
    if len(history) == 1:
        (candidate,) = history
        if candidate in siblings:
            return candidate, True
    return min(siblings), True


def resolve_insider_writer_routing(
    conn: psycopg.Connection[Any],
    *,
    discovery_instrument_id: int,
    issuer_cik: str | None,
) -> InsiderWriterRouting:
    """Resolve the #828 PR-1 writer routing for one parsed insider filing.

    Fail-open on unusable CIKs (missing / non-numeric / no in-universe
    sibling): the discovery linkage is preserved exactly as pre-#828 —
    the 24.7k-row unroutable cohort keeps today's behaviour.
    """
    cik = (issuer_cik or "").strip()
    if not cik:
        return InsiderWriterRouting(int(discovery_instrument_id), [], False)
    try:
        siblings = siblings_for_issuer_cik(conn, cik)
    except ValueError:
        return InsiderWriterRouting(int(discovery_instrument_id), [], False)
    if not siblings or int(discovery_instrument_id) in siblings:
        return InsiderWriterRouting(int(discovery_instrument_id), siblings, False)
    history = _history_instruments_for_cik(conn, cik)
    entity, is_mislink = pick_entity_instrument(
        discovery_instrument_id=discovery_instrument_id,
        sibling_instrument_ids=siblings,
        history_instrument_ids=history,
    )
    return InsiderWriterRouting(entity, siblings, is_mislink)


def _history_instruments_for_cik(conn: psycopg.Connection[Any], cik: str) -> list[int]:
    """Distinct instruments recorded for this CIK in ``instrument_cik_history``.

    The history table stores the 10-digit zero-padded form (seeded from
    ``instrument_sec_profile.cik``); lpad defensively anyway.
    """
    cik_padded = str(cik).strip().zfill(10)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT instrument_id
            FROM instrument_cik_history
            WHERE lpad(btrim(cik), 10, '0') = %s
            ORDER BY instrument_id
            """,
            (cik_padded,),
        )
        return [int(row[0]) for row in cur.fetchall()]
