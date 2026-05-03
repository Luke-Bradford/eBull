"""Per-instrument ownership ingest drillthrough.

Codex's Chain 2.5 substrate: when the operator opens an instrument
and the ownership card is sparse, they need ONE place to see why.
Today the answer is scattered across 6 tables. This service folds
them into a single read keyed on instrument_id.

Surfaces five pipeline states:

  1. **13F-HR institutional holdings** — count of holdings rows,
     latest period, count of unresolved CUSIPs blocking
     resolution, count of partial / failed accessions in
     ``institutional_holdings_ingest_log``.
  2. **13D/G blockholders** — count of filings rows, latest
     filed_at, count of partial / failed accessions in
     ``blockholder_filings_ingest_log``.
  3. **Form 4 insider transactions** — count of typed rows,
     latest period, count of tombstoned filings.
  4. **Form 3 initial holdings** — count of typed rows, latest
     period, tombstone count.
  5. **DEF 14A proxy beneficial-ownership** — count of holders,
     latest as_of_date, count of partial / failed accessions in
     ``def14a_ingest_log``.

Each pipeline also reports raw-body coverage from
``filing_raw_documents`` so the operator can distinguish "we have
the body but parser didn't yield rows" (rewash candidate) from
"we never fetched the body" (queue / discovery gap).

Out of scope for this service:
  * Re-running the ingester (operator endpoint + queue is already
    in :mod:`app.api.operator_ingest`).
  * UI rendering — this returns structured data; the admin page
    consumes it.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Literal

import psycopg
import psycopg.rows

PipelineKey = Literal[
    "institutional_holdings",
    "blockholder_filings",
    "insider_transactions",
    "insider_initial_holdings",
    "def14a_beneficial_holdings",
]


@dataclass(frozen=True)
class PipelineState:
    """Per-pipeline state for a single instrument."""

    key: PipelineKey
    typed_row_count: int
    latest_event_at: date | datetime | None
    raw_body_count: int
    tombstone_count: int  # partial + failed log rows
    notes: tuple[str, ...]


@dataclass(frozen=True)
class InstrumentDrillthrough:
    instrument_id: int
    symbol: str
    pipelines: tuple[PipelineState, ...]


def get_instrument_drillthrough(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
) -> InstrumentDrillthrough | None:
    """Return per-pipeline state for one instrument, or None when
    the instrument doesn't exist."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            "SELECT instrument_id, symbol FROM instruments WHERE instrument_id = %s",
            (instrument_id,),
        )
        row = cur.fetchone()
    if row is None:
        return None

    pipelines = (
        _institutional_state(conn, instrument_id),
        _blockholder_state(conn, instrument_id),
        _form4_state(conn, instrument_id),
        _form3_state(conn, instrument_id),
        _def14a_state(conn, instrument_id),
    )
    return InstrumentDrillthrough(
        instrument_id=instrument_id,
        symbol=str(row["symbol"]),  # type: ignore[arg-type]
        pipelines=pipelines,
    )


def _institutional_state(conn: psycopg.Connection[Any], instrument_id: int) -> PipelineState:
    """13F-HR holdings: row count + latest period + tombstones in
    institutional_holdings_ingest_log + raw body count."""
    notes: list[str] = []
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT COUNT(*) AS row_count, MAX(period_of_report) AS latest_period
            FROM institutional_holdings
            WHERE instrument_id = %s
            """,
            (instrument_id,),
        )
        holdings = cur.fetchone() or {"row_count": 0, "latest_period": None}

        # Tombstones: rows in ingest_log scoped to accessions that
        # name this instrument's holdings. Two-step join because
        # the log table doesn't carry instrument_id directly.
        cur.execute(
            """
            SELECT COUNT(*) AS tombstone_count
            FROM institutional_holdings_ingest_log log
            WHERE log.status IN ('partial', 'failed')
              AND log.accession_number IN (
                  SELECT accession_number FROM institutional_holdings
                  WHERE instrument_id = %s
                  UNION
                  -- accessions that wrote zero rows (all unresolved
                  -- CUSIPs) won't be in institutional_holdings, but
                  -- the log row IS scoped to a filer that filed
                  -- against this instrument. Best-effort: any log
                  -- row whose filer also has at least one resolved
                  -- holding for this instrument.
                  SELECT log2.accession_number
                  FROM institutional_holdings_ingest_log log2
                  WHERE log2.filer_cik IN (
                      SELECT f.cik FROM institutional_filers f
                      JOIN institutional_holdings h ON h.filer_id = f.filer_id
                      WHERE h.instrument_id = %s
                  )
              )
            """,
            (instrument_id, instrument_id),
        )
        tomb_row = cur.fetchone() or {"tombstone_count": 0}

        # COUNT(DISTINCT accession_number): a dense 13F has one
        # raw body per accession but many institutional_holdings
        # rows. Plain COUNT(*) over the join would inflate the
        # body count by the number of resolved holdings — Codex
        # pre-push review caught the fanout.
        cur.execute(
            """
            SELECT COUNT(DISTINCT r.accession_number) AS body_count
            FROM filing_raw_documents r
            JOIN institutional_holdings h ON h.accession_number = r.accession_number
            WHERE r.document_kind = 'infotable_13f' AND h.instrument_id = %s
            """,
            (instrument_id,),
        )
        body_row = cur.fetchone() or {"body_count": 0}

        # Unresolved CUSIPs surface separately so the operator
        # knows there's a #740-backfill-shaped gap.
        cur.execute(
            """
            SELECT COUNT(*) AS unresolved_count
            FROM unresolved_13f_cusips u
            WHERE u.cusip IN (
                SELECT identifier_value FROM external_identifiers
                WHERE provider = 'sec' AND identifier_type = 'cusip'
                  AND instrument_id = %s
            )
            """,
            (instrument_id,),
        )
        unresolved_row = cur.fetchone() or {"unresolved_count": 0}

    if holdings["row_count"] == 0:
        notes.append("no holdings rows")
    if tomb_row["tombstone_count"]:
        notes.append(f"{tomb_row['tombstone_count']} tombstoned accession(s)")
    if unresolved_row["unresolved_count"]:
        notes.append(f"{unresolved_row['unresolved_count']} unresolved-CUSIP row(s) (#740 backfill gap)")
    if body_row["body_count"] and not holdings["row_count"]:
        notes.append("raw bodies on file but zero typed rows — rewash candidate")

    return PipelineState(
        key="institutional_holdings",
        typed_row_count=int(holdings["row_count"]),
        latest_event_at=holdings.get("latest_period"),
        raw_body_count=int(body_row["body_count"]),
        tombstone_count=int(tomb_row["tombstone_count"]),
        notes=tuple(notes),
    )


def _blockholder_state(conn: psycopg.Connection[Any], instrument_id: int) -> PipelineState:
    notes: list[str] = []
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT COUNT(*) AS row_count, MAX(filed_at) AS latest_filed
            FROM blockholder_filings
            WHERE instrument_id = %s
            """,
            (instrument_id,),
        )
        rows = cur.fetchone() or {"row_count": 0, "latest_filed": None}

        # 13D/G partials with unresolved CUSIPs persist with
        # instrument_id IS NULL — those accessions still represent
        # data we tried to ingest for THIS issuer. Match them via
        # issuer_cusip → external_identifiers as a fallback so the
        # tombstone count reflects the full state. Codex pre-push
        # review caught the gap.
        cur.execute(
            """
            SELECT COUNT(*) AS tombstone_count
            FROM blockholder_filings_ingest_log log
            WHERE log.status IN ('partial', 'failed')
              AND log.accession_number IN (
                  SELECT DISTINCT accession_number FROM blockholder_filings
                  WHERE instrument_id = %s
                  UNION
                  SELECT DISTINCT b2.accession_number
                  FROM blockholder_filings b2
                  JOIN external_identifiers ei
                    ON ei.identifier_value = b2.issuer_cusip
                   AND ei.provider = 'sec'
                   AND ei.identifier_type = 'cusip'
                  WHERE b2.instrument_id IS NULL
                    AND ei.instrument_id = %s
              )
            """,
            (instrument_id, instrument_id),
        )
        tomb = cur.fetchone() or {"tombstone_count": 0}

        # Same instrument_id-or-cusip union for raw body coverage.
        cur.execute(
            """
            SELECT COUNT(DISTINCT r.accession_number) AS body_count
            FROM filing_raw_documents r
            JOIN blockholder_filings b ON b.accession_number = r.accession_number
            LEFT JOIN external_identifiers ei
              ON ei.identifier_value = b.issuer_cusip
             AND ei.provider = 'sec'
             AND ei.identifier_type = 'cusip'
            WHERE r.document_kind = 'primary_doc_13dg'
              AND (b.instrument_id = %s OR (b.instrument_id IS NULL AND ei.instrument_id = %s))
            """,
            (instrument_id, instrument_id),
        )
        body = cur.fetchone() or {"body_count": 0}

    if rows["row_count"] == 0:
        notes.append("no blockholder filings")
    if tomb["tombstone_count"]:
        notes.append(f"{tomb['tombstone_count']} tombstoned accession(s)")
    if body["body_count"] and not rows["row_count"]:
        notes.append("raw bodies on file but zero typed rows — rewash candidate")

    return PipelineState(
        key="blockholder_filings",
        typed_row_count=int(rows["row_count"]),
        latest_event_at=rows.get("latest_filed"),
        raw_body_count=int(body["body_count"]),
        tombstone_count=int(tomb["tombstone_count"]),
        notes=tuple(notes),
    )


def _form4_state(conn: psycopg.Connection[Any], instrument_id: int) -> PipelineState:
    """typed_row_count comes from ``insider_transactions`` (the
    canonical child table — one row per Form 4 transaction). Codex
    pre-push review caught the prior version which counted
    ``insider_filings`` HEADERS (one per accession), giving the
    wrong "typed rows" count by orders of magnitude on busy
    insiders."""
    notes: list[str] = []
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT COUNT(*) AS row_count, MAX(f.period_of_report) AS latest_period
            FROM insider_transactions t
            JOIN insider_filings f ON f.accession_number = t.accession_number
            WHERE t.instrument_id = %s
              AND f.document_type = '4'
              AND f.is_tombstone = FALSE
            """,
            (instrument_id,),
        )
        rows = cur.fetchone() or {"row_count": 0, "latest_period": None}

        cur.execute(
            """
            SELECT COUNT(*) AS tombstone_count
            FROM insider_filings
            WHERE instrument_id = %s
              AND document_type = '4'
              AND is_tombstone = TRUE
            """,
            (instrument_id,),
        )
        tomb = cur.fetchone() or {"tombstone_count": 0}

        cur.execute(
            """
            SELECT COUNT(DISTINCT r.accession_number) AS body_count
            FROM filing_raw_documents r
            JOIN insider_filings i ON i.accession_number = r.accession_number
            WHERE r.document_kind = 'form4_xml'
              AND i.instrument_id = %s
              AND i.is_tombstone = FALSE
            """,
            (instrument_id,),
        )
        body = cur.fetchone() or {"body_count": 0}

    if rows["row_count"] == 0:
        notes.append("no Form 4 transactions")
    if tomb["tombstone_count"]:
        notes.append(f"{tomb['tombstone_count']} tombstoned filing(s)")
    if body["body_count"] and not rows["row_count"]:
        notes.append("raw bodies on file but zero typed rows — rewash candidate")

    return PipelineState(
        key="insider_transactions",
        typed_row_count=int(rows["row_count"]),
        latest_event_at=rows.get("latest_period"),
        raw_body_count=int(body["body_count"]),
        tombstone_count=int(tomb["tombstone_count"]),
        notes=tuple(notes),
    )


def _form3_state(conn: psycopg.Connection[Any], instrument_id: int) -> PipelineState:
    """typed_row_count comes from ``insider_initial_holdings`` —
    canonical Form 3 child table. Codex pre-push review caught
    the prior header-count bug here too."""
    notes: list[str] = []
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT COUNT(*) AS row_count, MAX(f.period_of_report) AS latest_period
            FROM insider_initial_holdings h
            JOIN insider_filings f ON f.accession_number = h.accession_number
            WHERE h.instrument_id = %s
              AND f.document_type LIKE '3%%'
              AND f.is_tombstone = FALSE
            """,
            (instrument_id,),
        )
        rows = cur.fetchone() or {"row_count": 0, "latest_period": None}

        cur.execute(
            """
            SELECT COUNT(*) AS tombstone_count
            FROM insider_filings
            WHERE instrument_id = %s
              AND document_type LIKE '3%%'
              AND is_tombstone = TRUE
            """,
            (instrument_id,),
        )
        tomb = cur.fetchone() or {"tombstone_count": 0}

        cur.execute(
            """
            SELECT COUNT(DISTINCT r.accession_number) AS body_count
            FROM filing_raw_documents r
            JOIN insider_filings i ON i.accession_number = r.accession_number
            WHERE r.document_kind = 'form3_xml'
              AND i.instrument_id = %s
              AND i.is_tombstone = FALSE
            """,
            (instrument_id,),
        )
        body = cur.fetchone() or {"body_count": 0}

    if rows["row_count"] == 0:
        notes.append("no Form 3 baseline filings")
    if tomb["tombstone_count"]:
        notes.append(f"{tomb['tombstone_count']} tombstoned filing(s)")
    if body["body_count"] and not rows["row_count"]:
        notes.append("raw bodies on file but zero typed rows — rewash candidate")

    return PipelineState(
        key="insider_initial_holdings",
        typed_row_count=int(rows["row_count"]),
        latest_event_at=rows.get("latest_period"),
        raw_body_count=int(body["body_count"]),
        tombstone_count=int(tomb["tombstone_count"]),
        notes=tuple(notes),
    )


def _def14a_state(conn: psycopg.Connection[Any], instrument_id: int) -> PipelineState:
    notes: list[str] = []
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT COUNT(*) AS row_count, MAX(as_of_date) AS latest_as_of
            FROM def14a_beneficial_holdings
            WHERE instrument_id = %s
            """,
            (instrument_id,),
        )
        rows = cur.fetchone() or {"row_count": 0, "latest_as_of": None}

        cur.execute(
            """
            SELECT COUNT(*) AS tombstone_count
            FROM def14a_ingest_log log
            WHERE log.status IN ('partial', 'failed')
              AND log.accession_number IN (
                  SELECT accession_number FROM def14a_beneficial_holdings
                  WHERE instrument_id = %s
                  UNION
                  SELECT fe.provider_filing_id FROM filing_events fe
                  WHERE fe.provider = 'sec' AND fe.instrument_id = %s
                    AND fe.filing_type = 'DEF 14A'
              )
            """,
            (instrument_id, instrument_id),
        )
        tomb = cur.fetchone() or {"tombstone_count": 0}

        # Filter by filing_type = 'DEF 14A' so DEFA14A amendments
        # whose provider_filing_id collides with a def14a_body raw
        # document don't inflate the body count. Codex pre-push
        # review caught the gap.
        cur.execute(
            """
            SELECT COUNT(DISTINCT r.accession_number) AS body_count
            FROM filing_raw_documents r
            JOIN filing_events fe ON fe.provider_filing_id = r.accession_number
            WHERE r.document_kind = 'def14a_body'
              AND fe.provider = 'sec'
              AND fe.filing_type = 'DEF 14A'
              AND fe.instrument_id = %s
            """,
            (instrument_id,),
        )
        body = cur.fetchone() or {"body_count": 0}

    if rows["row_count"] == 0:
        notes.append("no DEF 14A holders")
    if tomb["tombstone_count"]:
        notes.append(f"{tomb['tombstone_count']} tombstoned proxy filing(s)")
    if body["body_count"] and not rows["row_count"]:
        notes.append("raw bodies on file but zero typed rows — rewash candidate")

    return PipelineState(
        key="def14a_beneficial_holdings",
        typed_row_count=int(rows["row_count"]),
        latest_event_at=rows.get("latest_as_of"),
        raw_body_count=int(body["body_count"]),
        tombstone_count=int(tomb["tombstone_count"]),
        notes=tuple(notes),
    )


def iter_drillthrough_summaries(
    conn: psycopg.Connection[Any],
    *,
    instrument_ids: Iterable[int],
) -> list[InstrumentDrillthrough]:
    """Batch entry point for the admin "ownership coverage" page.
    Iterates instrument_ids and returns one drillthrough per
    instrument that exists. Skips unknown ids."""
    out: list[InstrumentDrillthrough] = []
    for iid in instrument_ids:
        result = get_instrument_drillthrough(conn, instrument_id=iid)
        if result is not None:
            out.append(result)
    return out
