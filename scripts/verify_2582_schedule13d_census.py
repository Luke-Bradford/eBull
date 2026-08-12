"""Read-only initial Schedule 13D source census for #2582.

This command deliberately reads no price bars or forward returns. It measures
event identity, document/timestamp availability and research-series coverage
metadata before a trading rule or outcome window is frozen.

Run from the repository root:

    PYTHONPATH=. uv run python scripts/verify_2582_schedule13d_census.py
    PYTHONPATH=. uv run python scripts/verify_2582_schedule13d_census.py --json
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime
from decimal import Decimal
from typing import Any, LiteralString, cast

import psycopg
from psycopg.rows import dict_row
from psycopg.sql import SQL

from app.config import settings

_VENDOR = "paperswithbacktest/Stocks-Daily-Price"

_YEARLY_COVERAGE: LiteralString = """
WITH accessions AS (
    SELECT accession_number,
           min(filed_at) AS filed_at,
           max(instrument_id) FILTER (WHERE instrument_id IS NOT NULL) AS instrument_id
    FROM blockholder_filings
    WHERE submission_type = 'SCHEDULE 13D'
    GROUP BY accession_number
), covered AS (
    SELECT a.*,
           s.series_id,
           s.first_bar,
           s.last_bar,
           s.first_bar <= a.filed_at::date - 60
               AND s.last_bar >= a.filed_at::date + 20 AS covered_60_20
    FROM accessions a
    LEFT JOIN research_price_series s
      ON s.instrument_id = a.instrument_id
     AND s.vendor = %(vendor)s
)
SELECT extract(year FROM filed_at)::int AS filing_year,
       count(*) AS accessions,
       count(instrument_id) AS instrument_mapped,
       count(series_id) AS research_series_mapped,
       count(*) FILTER (WHERE covered_60_20) AS covered_60_prior_20_later,
       count(*) FILTER (
           WHERE series_id IS NOT NULL AND last_bar < filed_at::date + 20
       ) AS outcome_window_incomplete,
       count(*) FILTER (
           WHERE series_id IS NOT NULL AND first_bar > filed_at::date - 60
       ) AS prior_window_incomplete
FROM covered
GROUP BY 1
ORDER BY 1
"""

_CHAIN_SHAPE: LiteralString = """
WITH accessions AS (
    SELECT accession_number,
           min(filer_id) AS filer_id,
           min(issuer_cik) AS issuer_cik,
           min(filed_at) AS filed_at,
           max(instrument_id) FILTER (WHERE instrument_id IS NOT NULL) AS instrument_id
    FROM blockholder_filings
    WHERE submission_type = 'SCHEDULE 13D'
    GROUP BY accession_number
), ranked AS (
    SELECT a.*,
           row_number() OVER (
               PARTITION BY filer_id, issuer_cik
               ORDER BY filed_at, accession_number
           ) AS chain_rank,
           EXISTS (
               SELECT 1
               FROM blockholder_filings prior
               WHERE prior.filer_id = a.filer_id
                 AND prior.issuer_cik = a.issuer_cik
                 AND prior.status = 'passive'
                 AND prior.filed_at < a.filed_at
           ) AS prior_13g
    FROM accessions a
)
SELECT count(*) AS accessions,
       count(*) FILTER (WHERE chain_rank = 1) AS first_13d_per_chain,
       count(*) FILTER (WHERE chain_rank > 1) AS repeated_initial_label,
       count(*) FILTER (WHERE prior_13g) AS prior_13g,
       count(DISTINCT instrument_id) AS mapped_instruments
FROM ranked
"""

_SOURCE_SHAPE: LiteralString = """
WITH accessions AS (
    SELECT accession_number,
           min(filed_at) AS filed_at,
           max(date_of_event) AS date_of_event
    FROM blockholder_filings
    WHERE submission_type = 'SCHEDULE 13D'
    GROUP BY accession_number
)
SELECT count(*) AS accessions,
       count(date_of_event) AS event_date_present,
       count(*) FILTER (WHERE filed_at::time <> time '00:00') AS precise_filing_time,
       count(*) FILTER (WHERE filed_at::time = time '00:00') AS date_only_filing_time,
       count(raw.payload) AS raw_document_present,
       count(*) FILTER (
           WHERE raw.payload ~* '<([[:alnum:]_]+:)?item4([ >])'
             AND raw.payload ~* '<([[:alnum:]_]+:)?transactionPurpose([ >])'
       ) AS structured_item4_present
FROM accessions a
LEFT JOIN filing_raw_documents raw
  ON raw.accession_number = a.accession_number
 AND raw.document_kind = 'primary_doc_13dg'
"""

_INGEST_STATUS: LiteralString = """
SELECT status, count(*) AS accessions
FROM blockholder_filings_ingest_log
GROUP BY status
ORDER BY status
"""

_RAW_STORAGE: LiteralString = """
SELECT count(*) AS documents,
       sum(byte_count) AS uncompressed_payload_bytes,
       round(avg(byte_count))::bigint AS average_document_bytes
FROM filing_raw_documents
WHERE document_kind = 'primary_doc_13dg'
"""


def _json_default(value: object) -> str:
    if isinstance(value, (date, datetime, Decimal)):
        return str(value)
    raise TypeError(f"cannot JSON-encode {type(value).__name__}")


def _one(conn: psycopg.Connection[dict[str, Any]], query: LiteralString) -> dict[str, Any]:
    row = conn.execute(SQL(query)).fetchone()
    if row is None:
        raise RuntimeError("Schedule 13D census aggregate returned no row")
    return row


def build_census(conn: psycopg.Connection[dict[str, Any]]) -> dict[str, object]:
    """Return bounded source metadata; never load a research price bar."""

    yearly = conn.execute(_YEARLY_COVERAGE, {"vendor": _VENDOR}).fetchall()
    return {
        "outcomes_read": False,
        "research_vendor": _VENDOR,
        "yearly_coverage": yearly,
        "chain_shape": _one(conn, _CHAIN_SHAPE),
        "source_shape": _one(conn, _SOURCE_SHAPE),
        "ingest_status": conn.execute(_INGEST_STATUS).fetchall(),
        "raw_storage": _one(conn, _RAW_STORAGE),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--json", action="store_true", help="emit machine-readable JSON")
    args = parser.parse_args(argv)

    raw_conn = psycopg.connect(
        settings.database_url,
        row_factory=dict_row,  # pyright: ignore[reportCallIssue, reportArgumentType]
    )
    with cast(psycopg.Connection[dict[str, Any]], raw_conn) as conn:
        conn.execute("SET TRANSACTION READ ONLY")
        report = build_census(conn)

    if args.json:
        print(json.dumps(report, default=_json_default, indent=2, sort_keys=True))
        return 0

    print("#2582 INITIAL SCHEDULE 13D SOURCE CENSUS — NO OUTCOMES READ")
    for row in report["yearly_coverage"]:  # type: ignore[union-attr]
        print(
            f"{row['filing_year']}: accessions={row['accessions']:,} "
            f"instrument_mapped={row['instrument_mapped']:,} "
            f"series_mapped={row['research_series_mapped']:,} "
            f"covered_60_20={row['covered_60_prior_20_later']:,}"
        )
    print("chain shape:", json.dumps(report["chain_shape"], default=_json_default, sort_keys=True))
    print("source shape:", json.dumps(report["source_shape"], default=_json_default, sort_keys=True))
    print("ingest status:", json.dumps(report["ingest_status"], default=_json_default, sort_keys=True))
    print("raw storage:", json.dumps(report["raw_storage"], default=_json_default, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
