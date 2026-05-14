"""classId → instrument_id resolver for N-CSR / N-CSRS fund metadata
(spec §7).

The resolver bridges the SEC `oef:ClassAxis` member (``C000NNNNNN``)
to ``instruments.instrument_id`` via ``external_identifiers``
(``provider='sec', identifier_type='class_id'``). A companion
``cik_refresh_mf_directory`` table (populated by the bundled Stage 6
``daily_cik_refresh`` extension, #1171) lets the parser discriminate
miss-reasons:

- ``PENDING_CIK_REFRESH`` — directory has no entry for this classId yet
  (a new fund posted to ``company_tickers_mf.json`` after our last
  refresh). Transient; parser retries with 24h backoff.
- ``EXT_ID_NOT_YET_WRITTEN`` — directory has the classId + the symbol
  maps to an instrument, but no ``external_identifiers`` row has been
  written yet (cik_refresh wrote the directory row but the in-universe
  bridge row is racing). Transient; retry next tick.
- ``INSTRUMENT_NOT_IN_UNIVERSE`` — directory has the classId, the
  symbol does NOT map to any instrument (mutual fund not in eToro
  universe). Deterministic; tombstone with reason.

No symbol-only fallback — tickers get reused, share-class symbols can
match non-fund instruments, ETF/mutual-fund tickers can collide across
exchanges. Resolution is via ``external_identifiers`` or retry-then-
tombstone, never via symbol heuristic (spec §7.4, Codex 1a BLOCKING-4).
"""

from __future__ import annotations

from enum import StrEnum
from typing import Any

import psycopg


class ResolverMissReason(StrEnum):
    """Spec §7.4 — three discriminated miss-reasons.

    The two transient reasons receive 24h retry-backoff at the parser
    level; the deterministic reason tombstones the per-class observation
    write immediately.
    """

    PENDING_CIK_REFRESH = "pending_cik_refresh"
    EXT_ID_NOT_YET_WRITTEN = "ext_id_not_yet_written"
    INSTRUMENT_NOT_IN_UNIVERSE = "instrument_not_in_universe"


def resolve_class_id_to_instrument(
    conn: psycopg.Connection[Any],
    class_id: str,
) -> int | None:
    """Resolve a SEC classId to an ``instrument_id``.

    Returns ``None`` for unknown classIds. The caller (per-class fan-out
    in ``sec_n_csr._parse_sec_n_csr``) is responsible for calling
    :func:`classify_resolver_miss` to decide retry-vs-tombstone.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT instrument_id
            FROM external_identifiers
            WHERE provider = 'sec'
              AND identifier_type = 'class_id'
              AND identifier_value = %(class_id)s
              AND is_primary = TRUE
            """,
            {"class_id": class_id},
        )
        row = cur.fetchone()
        if row is None:
            return None
        return int(row[0])


def classify_resolver_miss(
    conn: psycopg.Connection[Any],
    class_id: str,
) -> ResolverMissReason:
    """Discriminate the miss-reason for a classId that
    :func:`resolve_class_id_to_instrument` returned ``None`` for.

    Decision tree (spec §7.4):

    1. ``cik_refresh_mf_directory`` row absent → ``PENDING_CIK_REFRESH``.
    2. Directory row present + symbol matches an instrument → the
       external_identifiers row is racing → ``EXT_ID_NOT_YET_WRITTEN``.
    3. Directory row present + symbol does NOT match any instrument →
       ``INSTRUMENT_NOT_IN_UNIVERSE``.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                mf.symbol,
                EXISTS (
                    SELECT 1 FROM instruments i WHERE i.symbol = mf.symbol
                ) AS has_instrument
            FROM cik_refresh_mf_directory mf
            WHERE mf.class_id = %(class_id)s
            """,
            {"class_id": class_id},
        )
        row = cur.fetchone()

    if row is None:
        return ResolverMissReason.PENDING_CIK_REFRESH

    _symbol, has_instrument = row
    if has_instrument:
        return ResolverMissReason.EXT_ID_NOT_YET_WRITTEN
    return ResolverMissReason.INSTRUMENT_NOT_IN_UNIVERSE
