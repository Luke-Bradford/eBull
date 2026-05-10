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

from typing import Any

import psycopg


def siblings_for_issuer_cik(conn: psycopg.Connection[Any], cik: str) -> list[int]:
    """Return all instrument_ids sharing this issuer CIK.

    The CIK is normalised (strip + zero-pad to 10 digits) before
    lookup; callers may pass either form.

    Ordering is deterministic (instrument_id ASC), NOT semantically
    primary. Per-instrument fan-out callers iterate the full list.
    Callers that need a single canonical sibling for entity-level
    row writes should pick by explicit policy (e.g.
    ``instruments.is_primary_listing``) rather than relying on this
    ordering.

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
