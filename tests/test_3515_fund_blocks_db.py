"""#3515 slice 2 — ``read_fund_blocks`` against real SQL: knowledge-time bounds on all three sources, the
claim-to-read window (§2), exact NUMERIC text, universe parity (§5) and the own-snapshot contract."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from typing import Any

import psycopg
import pytest

from app.services.ai_trial_fund_blocks import SnapshotNotFresh, read_fund_blocks

Conn = psycopg.Connection[Any]
EXT = "edgartools==5.30.2/mdna-1"
T0 = datetime(2026, 9, 1, tzinfo=UTC)
FUTURE = datetime(2030, 1, 1, tzinfo=UTC)
FY25 = date(2025, 12, 31)
SHA = "a" * 64


def _seed_instruments(conn: Conn) -> None:
    conn.execute("INSERT INTO exchanges (exchange_id, asset_class) VALUES ('us3515', 'us_equity')")
    for iid in (1, 2):
        conn.execute(
            "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable, exchange) "
            "VALUES (%s, %s, %s, TRUE, 'us3515')",
            (iid, f"S{iid}", f"S{iid} Inc"),
        )


def _event(conn: Conn, acc: str, form: str, filed: date, report: date, created: datetime) -> None:
    conn.execute(
        "INSERT INTO filing_events (instrument_id, filing_date, filing_type, provider, provider_filing_id, "
        "report_date, created_at) VALUES (1, %s, %s, 'sec', %s, %s, %s)",
        (filed, form, acc, report, created),
    )


def _fact(conn: Conn, acc: str, concept: str, val: str, fetched: datetime, unit: str = "USD") -> None:
    conn.execute(
        "INSERT INTO financial_facts_raw (instrument_id, taxonomy, concept, unit, period_end, val, "
        "accession_number, form_type, filed_date, fetched_at) "
        "VALUES (1, 'us-gaap', %s, %s, %s, %s::numeric, %s, '10-K', '2026-02-20', %s)",
        (concept, unit, FY25, val, acc, fetched),
    )


def _section(conn: Conn, acc: str, body: str) -> int:
    got = conn.execute(
        "INSERT INTO periodic_report_sections (instrument_id, accession_number, section_id, extractor, status, "
        "body, full_chars, retryable, source_url, source_text_sha256, source_chars) "
        "VALUES (1, %s, '10-K:Item 7', %s, 'extracted', %s, %s, FALSE, 'https://www.sec.gov/x.htm', %s, 10) "
        "RETURNING row_id",
        (acc, EXT, body, len(body), SHA),
    ).fetchone()
    assert got is not None
    return int(got[0])


def _now(conn: Conn) -> datetime:
    got = conn.execute("SELECT clock_timestamp()").fetchone()
    assert got is not None
    return got[0]


def test_read_fund_blocks_end_to_end(ebull_test_conn: Conn) -> None:
    conn = ebull_test_conn
    _seed_instruments(conn)
    _event(conn, "A-FY25", "10-K", date(2026, 2, 20), FY25, T0)
    _event(conn, "Q-Q2", "10-Q", date(2026, 8, 1), date(2026, 6, 30), FUTURE)  # known after as_of
    _fact(conn, "A-FY25", "Assets", "123456789012345678.90", T0)
    _fact(conn, "A-FY25", "Liabilities", "5", FUTURE)  # overwritten after as_of
    _fact(conn, "A-FY25", "Goodwill", "7", T0)  # outside K
    conn.execute(  # another provider's newer report: never a target
        "INSERT INTO filing_events (instrument_id, filing_date, filing_type, provider, provider_filing_id, "
        "report_date, created_at) VALUES (1, '2026-08-01', '10-Q', 'other', 'X-1', '2026-06-30', %s)",
        (T0,),
    )
    shown_row = _section(conn, "A-FY25", "Discussion and Analysis " + "x" * 1_200)
    conn.commit()
    as_of = _now(conn)
    conn.rollback()
    _section(conn, "A-FY25", "Discussion and Analysis " + "y" * 1_200)  # recorded after as_of
    conn.commit()

    got = read_fund_blocks(conn, [2, 1, 1], as_of=as_of, extractor=EXT)

    assert sorted(got.by_instrument) == [1, 2]  # §5: every name gets an entry, none dropped
    assert got.snapshot_at >= as_of
    one = got.by_instrument[1]
    assert one.fundamentals is not None
    [report] = one.fundamentals["reports"]
    assert report["accession_number"] == "A-FY25"
    # val is the stored NUMERIC(30, 6) as text, without the column scale's trailing zeros (§3): exact.
    assert [(g["concept"], [row[3] for row in g["rows"]]) for g in report["facts"]] == [
        ("us-gaap:Assets", ["123456789012345678.9"])
    ]
    assert len(one.audit.fact_ids["A-FY25"]) == 1
    assert one.fundamentals["newer_report_without_facts"] == []
    assert one.audit.withheld_after_as_of == {"A-FY25": 1}
    assert one.mdna is not None and one.mdna["row_id"] == shown_row and "x" * 50 in one.mdna["text"]
    two = got.by_instrument[2]
    assert two.fundamentals is None and two.fundamentals_absent is not None
    assert two.fundamentals_absent["reason"] == "no_candidate_report"
    assert two.mdna_absent == {"reason": "no_target_report"}


def test_claim_to_read_window_commit_is_visible(ebull_test_conn: Conn) -> None:
    """§2 snapshot-after-claim: a row stamped <= as_of but committed after the claim is visible."""
    conn = ebull_test_conn
    _seed_instruments(conn)
    _event(conn, "A-FY25", "10-K", date(2026, 2, 20), FY25, T0)
    conn.commit()
    as_of = _now(conn)
    conn.rollback()
    _fact(conn, "A-FY25", "Assets", "1", as_of - timedelta(seconds=1))
    conn.commit()

    got = read_fund_blocks(conn, [1], as_of=as_of, extractor=EXT)

    assert got.by_instrument[1].fundamentals is not None


def test_refuses_a_connection_already_in_a_transaction(ebull_test_conn: Conn) -> None:
    ebull_test_conn.execute("SELECT 1")
    with pytest.raises(SnapshotNotFresh):
        read_fund_blocks(ebull_test_conn, [1], as_of=T0, extractor=EXT)
