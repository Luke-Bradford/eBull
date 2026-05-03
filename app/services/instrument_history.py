"""Instrument CIK / symbol history helpers (#794 — Batch 1 schema).

The schema is designed to record the canonical chain of CIKs and
symbols for one instrument across rebrands, reorgs, and ticker
changes (FB → META, BBBY → BBBYQ at delisting). Every filings table
in the repo already keys on ``instrument_id``, so the read side
already survives a rename **if** the ingester resolved the
historical CIK to the same instrument_id at write time.

This module is the small public surface used by:

  * The Tier 0 ownership-rollup service (#789), which calls
    :func:`historical_ciks_for` so dedup and provenance code paths
    have a stable list of every CIK ever associated with an
    instrument.
  * The Batch 7 symbol-change ingester, which will use
    :func:`instrument_id_for_historical_cik` to resolve a filing
    under a historical CIK back to the current instrument_id at
    write time.

After Batch 1 the only data in these tables is one
``imported`` row per instrument seeded by
:func:`backfill_current_history` — which the migration script
``scripts/backfill_instrument_history.py`` runs idempotently on
deploy.

The Batch 7 ingester adds rows with ``source_event IN ('rebrand',
'reorg', 'merger', 'spinoff')`` and closes out the prior current
row by setting ``effective_to``. The DB-level invariants
(EXCLUDE no-overlap, partial UNIQUE on ``(instrument_id) WHERE
effective_to IS NULL``, CHECK ordered ranges) make a half-applied
ingest fail loud rather than silently produce two current chains.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date

import psycopg
import psycopg.rows


@dataclass(frozen=True)
class SymbolHistoryEntry:
    """One row from ``instrument_symbol_history`` shaped for the
    ownership-rollup payload (#794 frontend finish, Batch 7 of #788).

    The frontend renders a "Filed as X" callout when the historical
    chain includes a symbol other than the current one. Empty
    ``effective_to`` marks the current row."""

    symbol: str
    effective_from: date
    effective_to: date | None
    source_event: str


def historical_symbols_for(conn: psycopg.Connection[tuple], instrument_id: int) -> Sequence[SymbolHistoryEntry]:
    """Every symbol ever associated with this instrument, oldest-first.

    Returns ``[]`` for an instrument with no
    ``instrument_symbol_history`` row (pre-backfill stub). Callers
    can treat ``len(entries) > 1`` OR ``any(e.effective_to is not None)``
    as the trigger for the historical-symbol callout — both conditions
    imply the chain has more than just the imported current row.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT symbol, effective_from, effective_to, source_event
            FROM instrument_symbol_history
            WHERE instrument_id = %s
            ORDER BY effective_from ASC
            """,
            (instrument_id,),
        )
        return [
            SymbolHistoryEntry(
                symbol=str(row[0]),
                effective_from=row[1],
                effective_to=row[2],
                source_event=str(row[3]),
            )
            for row in cur.fetchall()
        ]


def historical_ciks_for(conn: psycopg.Connection[tuple], instrument_id: int) -> Sequence[str]:
    """Every CIK ever associated with this instrument, oldest-first.

    Returns ``[]`` for an instrument with no
    ``instrument_cik_history`` row (e.g. seeded but not yet
    backfilled). Callers should treat the empty case as "use the
    current ``instrument_sec_profile.cik``" — see Batch 7 for the
    full ingester wiring.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT cik
            FROM instrument_cik_history
            WHERE instrument_id = %s
            ORDER BY effective_from ASC
            """,
            (instrument_id,),
        )
        return [str(row[0]) for row in cur.fetchall()]


def current_cik_for(conn: psycopg.Connection[tuple], instrument_id: int) -> str | None:
    """The current (``effective_to IS NULL``) CIK for an instrument,
    or ``None`` when the instrument has no history row yet.

    The DB-level partial UNIQUE INDEX guarantees at most one current
    row per instrument, so this returns either zero or one match.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT cik
            FROM instrument_cik_history
            WHERE instrument_id = %s
              AND effective_to IS NULL
            """,
            (instrument_id,),
        )
        row = cur.fetchone()
    return str(row[0]) if row is not None else None


def instrument_id_for_historical_cik(conn: psycopg.Connection[tuple], cik: str) -> int | None:
    """Resolve a CIK (current or historical) to an instrument_id.

    Returns ``None`` when the CIK has never been associated with any
    instrument. Used by the Batch 7 ingester to route a filing under
    a historical CIK to the right instrument.

    A CIK can in principle map to multiple instruments at different
    times (e.g. a parent that spun off a subsidiary; the spinoff
    might briefly carry the parent's CIK before EDGAR assigns its
    own). The schema doesn't forbid this, so this helper returns the
    instrument whose history range contains the latest known
    ``effective_to`` (i.e. the most recent owner). Callers that need
    historical resolution by date should use
    :func:`instrument_id_for_cik_at_date`.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT instrument_id
            FROM instrument_cik_history
            WHERE cik = %s
            ORDER BY (effective_to IS NULL) DESC,
                     effective_to DESC NULLS LAST,
                     effective_from DESC
            LIMIT 1
            """,
            (cik,),
        )
        row = cur.fetchone()
    return int(row[0]) if row is not None else None


def backfill_current_history(conn: psycopg.Connection[tuple]) -> tuple[int, int]:
    """Seed one ``imported`` row per instrument in both history tables.

    Idempotent: re-running on a DB that already has the rows is a
    no-op via ``ON CONFLICT DO NOTHING``. Returns
    ``(cik_rows_inserted, symbol_rows_inserted)``.

    Caveats:

      * **CIK history** seeds from ``instrument_sec_profile.cik`` —
        the current CIK. Instruments with no SEC profile row (non-US
        listings, pre-ingest stubs) are skipped silently.
      * **Symbol history** seeds from ``instruments.symbol``. No
        synthesis from ``instrument_sec_profile.former_names`` —
        former_names is **company-name** history, not symbol history.
        Synthesising symbols from name changes would record fake
        chains (e.g. "Facebook, Inc." → "FB" before the actual rebrand
        date). The Batch 7 ingester writes real symbol-change events
        from the EDGAR per-accession ticker tagging.
      * ``effective_from`` is the instrument's ``first_seen_at::date``.
        That's a deploy-time hint; the actual SEC association may
        pre-date our ingestion. Acceptable because the only consumer
        in Batch 1 is :func:`historical_ciks_for`, which doesn't use
        ``effective_from`` for filtering.
    """
    cik_inserted = _backfill_cik_history(conn)
    sym_inserted = _backfill_symbol_history(conn)
    return cik_inserted, sym_inserted


def _backfill_cik_history(conn: psycopg.Connection[tuple]) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO instrument_cik_history (
                instrument_id, cik, effective_from, effective_to, source_event
            )
            SELECT i.instrument_id,
                   p.cik,
                   COALESCE(i.first_seen_at::date, CURRENT_DATE),
                   NULL,
                   'imported'
            FROM instruments i
            JOIN instrument_sec_profile p ON p.instrument_id = i.instrument_id
            WHERE p.cik IS NOT NULL
            ON CONFLICT DO NOTHING
            """,
        )
        return cur.rowcount if cur.rowcount is not None and cur.rowcount >= 0 else 0


def _backfill_symbol_history(conn: psycopg.Connection[tuple]) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO instrument_symbol_history (
                instrument_id, symbol, effective_from, effective_to, source_event
            )
            SELECT i.instrument_id,
                   i.symbol,
                   COALESCE(i.first_seen_at::date, CURRENT_DATE),
                   NULL,
                   'imported'
            FROM instruments i
            WHERE i.symbol IS NOT NULL
            ON CONFLICT DO NOTHING
            """,
        )
        return cur.rowcount if cur.rowcount is not None and cur.rowcount >= 0 else 0
