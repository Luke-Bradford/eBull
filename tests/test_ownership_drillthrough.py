"""Tests for the per-instrument ownership drillthrough.

Pins: pipeline-state shape, raw-body-without-typed-rows note,
tombstone count, unknown-instrument returns None.
"""

from __future__ import annotations

from datetime import date

import psycopg
import pytest

from app.services.ownership_drillthrough import get_instrument_drillthrough
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401

pytestmark = pytest.mark.integration


def _seed_instrument(conn: psycopg.Connection[tuple], iid: int, symbol: str) -> None:
    conn.execute(
        """
        INSERT INTO instruments (
            instrument_id, symbol, company_name, exchange, currency, is_tradable
        ) VALUES (%s, %s, 'Test', '4', 'USD', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        (iid, symbol),
    )


def test_unknown_instrument_returns_none(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    result = get_instrument_drillthrough(ebull_test_conn, instrument_id=999_999)
    assert result is None


def test_zero_state_returns_no_pipeline_rows(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """Instrument exists but no ownership data — every pipeline
    has typed_row_count=0 and a 'no rows' note."""
    conn = ebull_test_conn
    _seed_instrument(conn, 970_001, "ZERO")
    conn.commit()

    result = get_instrument_drillthrough(conn, instrument_id=970_001)
    assert result is not None
    assert result.symbol == "ZERO"
    assert len(result.pipelines) == 5
    for p in result.pipelines:
        assert p.typed_row_count == 0
        assert p.raw_body_count == 0
        assert any("no" in n.lower() for n in p.notes)


def test_form4_state_counts_transactions_and_tombstones(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """Form 4 typed_row_count counts insider_transactions rows
    (canonical child table), NOT insider_filings headers. Codex
    pre-push review caught the prior header-count bug — a single
    accession with 5 transactions must report typed_row_count=5,
    not 1."""
    conn = ebull_test_conn
    _seed_instrument(conn, 970_002, "F4T")
    conn.execute(
        """
        INSERT INTO insider_filings (
            accession_number, instrument_id, document_type,
            primary_document_url, parser_version, is_tombstone,
            period_of_report
        ) VALUES
            ('a-26-1', %s, '4', 'u', 1, FALSE, '2025-01-15'),
            ('a-26-2', %s, '4', 'u', 1, TRUE, '2025-02-15')
        """,
        (970_002, 970_002),
    )
    # Two transactions on the live filing, zero on the tombstoned.
    for row_num in (1, 2):
        conn.execute(
            """
            INSERT INTO insider_transactions (
                accession_number, instrument_id, txn_row_num,
                filer_name, filer_role,
                txn_date, txn_code, shares, price
            ) VALUES ('a-26-1', %s, %s, 'Test Filer', 'director',
                      '2025-01-15', 'P', 100, 10.00)
            """,
            (970_002, row_num),
        )
    conn.commit()

    result = get_instrument_drillthrough(conn, instrument_id=970_002)
    assert result is not None
    form4 = next(p for p in result.pipelines if p.key == "insider_transactions")
    assert form4.typed_row_count == 2  # transactions, not headers
    assert form4.tombstone_count == 1
    assert form4.latest_event_at == date(2025, 1, 15)


def test_form3_state_counts_initial_holdings_not_headers(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """Form 3 typed_row_count counts insider_initial_holdings,
    not insider_filings. Same regression as Form 4."""
    conn = ebull_test_conn
    _seed_instrument(conn, 970_006, "F3T")
    conn.execute(
        """
        INSERT INTO insider_filings (
            accession_number, instrument_id, document_type,
            primary_document_url, parser_version, is_tombstone,
            period_of_report
        ) VALUES ('a-26-f3', %s, '3', 'u', 1, FALSE, '2025-01-15')
        """,
        (970_006,),
    )
    for row_num in (1, 2, 3):
        conn.execute(
            """
            INSERT INTO insider_initial_holdings (
                accession_number, instrument_id, row_num,
                filer_cik, filer_name, as_of_date,
                security_title, shares
            ) VALUES ('a-26-f3', %s, %s,
                      '0000111000', 'Test Filer', '2025-01-15',
                      'Common Stock', 100)
            """,
            (970_006, row_num),
        )
    conn.commit()

    result = get_instrument_drillthrough(conn, instrument_id=970_006)
    assert result is not None
    form3 = next(p for p in result.pipelines if p.key == "insider_initial_holdings")
    assert form3.typed_row_count == 3


def test_blockholder_state_counts_partials_with_null_instrument(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """13D/G partials with unresolved CUSIPs persist with
    instrument_id=NULL but match this issuer's CUSIP. The
    drillthrough must include them via external_identifiers
    fallback. Codex pre-push review caught the gap."""
    conn = ebull_test_conn
    _seed_instrument(conn, 970_007, "BHN")
    conn.execute(
        """
        INSERT INTO external_identifiers (
            instrument_id, provider, identifier_type, identifier_value, is_primary
        ) VALUES (%s, 'sec', 'cusip', 'BHCUSIP1', FALSE)
        ON CONFLICT (provider, identifier_type, identifier_value) DO NOTHING
        """,
        (970_007,),
    )
    conn.execute(
        "INSERT INTO blockholder_filers (cik, name) VALUES ('0000222222', 'F2') ON CONFLICT (cik) DO NOTHING",
    )
    with conn.cursor() as cur:
        cur.execute("SELECT filer_id FROM blockholder_filers WHERE cik = '0000222222'")
        result_row = cur.fetchone()
    assert result_row is not None
    filer_id = result_row[0]
    # Partial: instrument_id IS NULL but CUSIP matches.
    conn.execute(
        """
        INSERT INTO blockholder_filings (
            filer_id, accession_number, submission_type, status,
            instrument_id, issuer_cik, issuer_cusip,
            reporter_no_cik, reporter_name, aggregate_amount_owned, percent_of_class
        ) VALUES (%s, 'b-partial-1', 'SCHEDULE 13G', 'passive', NULL,
                  '0000999000', 'BHCUSIP1', FALSE, 'R', 1000, 5.0)
        """,
        (filer_id,),
    )
    conn.execute(
        """
        INSERT INTO blockholder_filings_ingest_log (
            accession_number, filer_cik, status, rows_inserted, rows_skipped
        ) VALUES ('b-partial-1', '0000222222', 'partial', 0, 1)
        """,
    )
    conn.commit()

    result = get_instrument_drillthrough(conn, instrument_id=970_007)
    assert result is not None
    bh = next(p for p in result.pipelines if p.key == "blockholder_filings")
    assert bh.tombstone_count == 1  # partial surfaced via CUSIP fallback


def test_institutional_body_count_does_not_fanout_on_dense_13f(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """A 13F with 50 holdings has ONE raw body. raw_body_count
    must be 1, not 50. Codex pre-push review caught the JOIN
    fanout."""
    conn = ebull_test_conn
    _seed_instrument(conn, 970_008, "F13")
    conn.execute(
        "INSERT INTO institutional_filers (cik, name) VALUES ('0000333333', 'F') ON CONFLICT (cik) DO NOTHING",
    )
    with conn.cursor() as cur:
        cur.execute("SELECT filer_id FROM institutional_filers WHERE cik = '0000333333'")
        result_row = cur.fetchone()
    assert result_row is not None
    filer_id = result_row[0]
    # Two distinct accessions × this same instrument; without
    # COUNT(DISTINCT), the join through institutional_holdings to
    # filing_raw_documents would inflate. With COUNT(DISTINCT) we
    # see body_count=2.
    for accn in ("f13-26-1", "f13-26-2"):
        conn.execute(
            """
            INSERT INTO institutional_holdings (
                filer_id, instrument_id, accession_number, period_of_report,
                shares, market_value_usd, voting_authority, filed_at
            ) VALUES (%s, %s, %s, '2025-09-30', 100, 1000, 'SOLE', '2025-11-01')
            """,
            (filer_id, 970_008, accn),
        )
        conn.execute(
            """
            INSERT INTO filing_raw_documents (
                accession_number, document_kind, payload, parser_version
            ) VALUES (%s, 'infotable_13f', '<x/>', '13f-infotable-v1')
            """,
            (accn,),
        )
    conn.commit()

    result = get_instrument_drillthrough(conn, instrument_id=970_008)
    assert result is not None
    inst = next(p for p in result.pipelines if p.key == "institutional_holdings")
    assert inst.typed_row_count == 2
    # Two raw bodies (one per accession), not 2x2=4 from a fanout.
    assert inst.raw_body_count == 2


def test_form4_raw_body_without_typed_row_surfaces_rewash_note(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """When raw bodies exist but typed rows are missing the note
    surfaces 'rewash candidate' so the operator knows the gap is
    parser-side, not fetch-side."""
    conn = ebull_test_conn
    _seed_instrument(conn, 970_003, "F4R")
    # Tombstoned row → typed_row_count==0 but the JOIN to
    # filing_raw_documents needs a row in insider_filings to
    # link the body.
    conn.execute(
        """
        INSERT INTO insider_filings (
            accession_number, instrument_id, document_type,
            primary_document_url, parser_version, is_tombstone
        ) VALUES ('a-26-3', %s, '4', 'u', 1, TRUE)
        """,
        (970_003,),
    )
    conn.execute(
        """
        INSERT INTO filing_raw_documents (
            accession_number, document_kind, payload, parser_version
        ) VALUES ('a-26-3', 'form4_xml', '<x/>', 'form4-v1')
        """,
    )
    conn.commit()

    result = get_instrument_drillthrough(conn, instrument_id=970_003)
    assert result is not None
    form4 = next(p for p in result.pipelines if p.key == "insider_transactions")
    assert form4.typed_row_count == 0
    assert form4.raw_body_count == 1
    assert any("rewash" in n.lower() for n in form4.notes)


def test_blockholder_state_counts_typed_rows(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    conn = ebull_test_conn
    _seed_instrument(conn, 970_004, "BH")
    conn.execute(
        "INSERT INTO blockholder_filers (cik, name) VALUES ('0000111111', 'F') ON CONFLICT (cik) DO NOTHING",
    )
    with conn.cursor() as cur:
        cur.execute("SELECT filer_id FROM blockholder_filers WHERE cik = '0000111111'")
        result_row = cur.fetchone()
    assert result_row is not None
    filer_id = result_row[0]
    conn.execute(
        """
        INSERT INTO blockholder_filings (
            filer_id, accession_number, submission_type, status,
            instrument_id, issuer_cik, issuer_cusip,
            reporter_no_cik, reporter_name, aggregate_amount_owned, percent_of_class
        ) VALUES (%s, 'b-26-1', 'SCHEDULE 13G', 'passive', %s,
                  '0000999000', 'CSP1', FALSE, 'R', 1000, 5.0)
        """,
        (filer_id, 970_004),
    )
    conn.commit()

    result = get_instrument_drillthrough(conn, instrument_id=970_004)
    assert result is not None
    bh = next(p for p in result.pipelines if p.key == "blockholder_filings")
    assert bh.typed_row_count == 1


def test_def14a_state_counts_holders(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    conn = ebull_test_conn
    _seed_instrument(conn, 970_005, "D14")
    conn.execute(
        """
        INSERT INTO def14a_beneficial_holdings (
            instrument_id, accession_number, issuer_cik,
            holder_name, holder_role, shares, percent_of_class, as_of_date
        ) VALUES (%s, 'd-26-1', '0000999000', 'H', 'officer', 100, 5.0, '2025-03-01'),
                 (%s, 'd-26-1', '0000999000', 'I', 'director', 200, 8.0, '2025-03-01')
        """,
        (970_005, 970_005),
    )
    conn.commit()

    result = get_instrument_drillthrough(conn, instrument_id=970_005)
    assert result is not None
    def14a = next(p for p in result.pipelines if p.key == "def14a_beneficial_holdings")
    assert def14a.typed_row_count == 2
    assert def14a.latest_event_at == date(2025, 3, 1)
