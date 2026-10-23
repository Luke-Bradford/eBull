"""#3227 tier (IM) against a real DB: ``_read_13f_im_evidence`` feeding ``_fold_13f_duplicates``.

Spec: docs/specs/ownership/2026-10-22-3227-13f-included-managers.md (Acceptance, DB tests). Auto-marked ``db`` (pulls
``ebull_test_conn``). A seeded 13F observation with captured segments, its capture record, its list rows and a Form 3
node fold through the real read under a non-UTC session; the read takes only § Rule 3's key.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

import psycopg
from psycopg.types.json import Jsonb

from app.services.ownership_rollup import (
    Holder,
    SliceCategory,
    SourceTag,
    _fold_13f_duplicates,
    _read_13f_im_evidence,
)

_IID = 3_227_801
_P = date(2026, 6, 30)
_T = 1_979_173
_M, _M_NAME = "0000200406", "JOHNSON & JOHNSON"
_K, _K_NAME = "0000924020", "Johnson & Johnson Innovation - JJDC, Inc."
_A = "0000904454-26-000435"
_CAPTURE = "6f1c7c1e-0000-4000-8000-000000000001"


def _holder(cik: str, name: str, source: SourceTag, accession: str) -> Holder:
    return Holder(
        filer_cik=cik,
        filer_name=name,
        shares=Decimal(_T),
        pct_outstanding=Decimal(0),
        winning_source=source,
        winning_accession=accession,
        winning_edgar_url=None,
        as_of_date=_P,
        filer_type=None,
        dropped_sources=(),
    )


def _observation(
    conn: psycopg.Connection[tuple], accession: str, shares: int, *, period: date = _P, exposure: str = "EQUITY"
) -> None:
    segments = {
        "capture_id": _CAPTURE,
        "cusips": ["15234Q207"],
        "lines": [{"discretion": "DFND", "other_managers": [1], "shares": str(shares)}],
    }
    conn.execute(
        """
        INSERT INTO ownership_institutions_observations (
            instrument_id, filer_cik, filer_name, ownership_nature, source, source_document_id, source_accession,
            filed_at, period_end, ingest_run_id, shares, exposure_kind, thirteen_f_segments
        ) VALUES (%s, %s, %s, 'economic', '13f', %s, %s, %s, %s, gen_random_uuid(), %s, %s, %s)
        """,
        (
            _IID,
            _M,
            _M_NAME,
            accession,
            accession,
            datetime(2026, 8, 14, tzinfo=UTC),
            period,
            shares,
            exposure,
            Jsonb(segments),
        ),
    )


def test_a_captured_13f_position_folds_through_the_real_read_under_a_non_utc_session(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    conn = ebull_test_conn
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, country, is_tradable)
        VALUES (%s, 'CTNMT', 'CTNM test co', '4', 'USD', 'US', TRUE)
        """,
        (_IID,),
    )
    _observation(conn, _A, _T)
    # Off § Rule 3's key, each in one way: another accession, another period, an option exposure.
    _observation(conn, "0000904454-26-000999", _T)
    _observation(conn, _A, 7, period=date(2026, 3, 31))
    _observation(conn, _A, 9, exposure="CALL")
    conn.execute(
        """
        INSERT INTO institutional_13f_captures (
            accession_number, capture_id, source, status, list_count, expected_list_count, line_count,
            expected_line_count, value_total, expected_value_total
        ) VALUES (%s, %s, 'xml', 'complete', 2, 2, 1, 1, 100, 100)
        """,
        (_A, _CAPTURE),
    )
    conn.execute(
        """
        INSERT INTO institutional_13f_other_managers (accession_number, entry_index, sequence_number_raw,
            sequence_number, cik, name)
        VALUES (%s, 0, 'X', NULL, '0000000001', 'Invalid entry'), (%s, 1, '1', 1, '0000924020', %s)
        """,
        (_A, _A, _K_NAME),
    )
    conn.execute("SET TIME ZONE 'Asia/Tokyo'")

    by_category: dict[SliceCategory, list[Holder]] = {
        "insiders": [_holder(_K, _K_NAME, "form3", "0000924020-24-000001")],
        "blockholders": [],
        "institutions": [_holder(_M, _M_NAME, "13f", _A)],
    }
    evidence = _read_13f_im_evidence(conn, _IID, by_category)
    (ev,) = evidence.values()
    assert ev.accession == _A
    assert [shares for shares, _ in ev.observations] == [Decimal(_T)]
    assert ev.capture == (_CAPTURE, "complete")
    assert ev.entries == ((1, _K),)

    _, corrections = _fold_13f_duplicates(by_category, [], {}, evidence)
    (c,) = corrections
    assert c.filer_cik == _K and c.shares_removed == Decimal(_T)
    assert f"its 13F {_A} reports every line of the position as shared discretion with included manager" in c.detail


def test_no_candidate_with_a_possible_twin_reads_nothing(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    by_category: dict[SliceCategory, list[Holder]] = {
        "insiders": [],
        "blockholders": [],
        "institutions": [_holder(_M, _M_NAME, "13f", _A)],
    }
    assert _read_13f_im_evidence(ebull_test_conn, _IID, by_category) == {}
