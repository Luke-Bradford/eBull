"""#2351 slice 2 — the suppression job against the real schema (views, refresh, cache)."""

from __future__ import annotations

from datetime import date

import httpx
import psycopg
import pytest

from app.services.def14a_recipients import run_recipient_suppressions
from app.services.ownership_observations import refresh_def14a_current
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export
from tests.test_def14a_recipients import cover_xml

pytestmark = pytest.mark.integration

CIK = "0001657853"
PROXY = "0001104659-26-044356"
COVER_OLD = "0001657853-25-000010"
COVER_NEW = "0001657853-26-000010"
URL_OLD = "https://www.sec.gov/Archives/edgar/data/1657853/000165785325000010/htz-20241231.htm"
URL_NEW = "https://www.sec.gov/Archives/edgar/data/1657853/000165785326000010/htz-20251231.htm"


def _seed(conn: psycopg.Connection[tuple]) -> None:
    for iid, symbol in ((1, "HTZ"), (2, "HTZWW")):
        conn.execute(
            """
            INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, is_tradable)
            VALUES (%s, %s, 'Hertz', '4', 'USD', TRUE)
            """,
            (iid, symbol),
        )
        conn.execute(
            """
            INSERT INTO external_identifiers (instrument_id, provider, identifier_type, identifier_value, is_primary)
            VALUES (%s, 'sec', 'cik', %s, TRUE)
            """,
            (iid, CIK),
        )
        conn.execute(
            """
            INSERT INTO def14a_beneficial_holdings (
                instrument_id, accession_number, issuer_cik, holder_name, holder_role,
                shares, percent_of_class, as_of_date)
            VALUES (%s, %s, %s, 'Knighthead Capital', 'principal', 100000, 12.5, '2026-03-01')
            """,
            (iid, PROXY, CIK),
        )
        conn.execute(
            """
            INSERT INTO ownership_def14a_observations (
                instrument_id, holder_name, holder_role, ownership_nature, source, source_document_id,
                source_accession, filed_at, period_end, known_from, ingest_run_id, shares, percent_of_class)
            VALUES (%s, 'Knighthead Capital', 'principal', 'beneficial', 'def14a', %s,
                    %s, '2026-04-01', '2026-03-01', '2026-04-01', gen_random_uuid(), 100000, 12.5)
            """,
            (iid, PROXY, PROXY),
        )
    rows = [
        (2, "DEF 14A", PROXY, date(2026, 4, 1), None),
        (1, "10-Q", COVER_OLD, date(2025, 11, 5), URL_OLD),
        (1, "10-K", COVER_NEW, date(2026, 2, 19), URL_NEW),
    ]
    for iid, form, acc, filed, url in rows:
        conn.execute(
            """
            INSERT INTO filing_events (instrument_id, filing_date, filing_type, provider, provider_filing_id,
                                       primary_document_url)
            VALUES (%s, %s, %s, 'sec', %s, %s)
            """,
            (iid, filed, form, acc, url),
        )
    for iid in (1, 2):
        refresh_def14a_current(conn, instrument_id=iid)


def _current_holders(conn: psycopg.Connection[tuple]) -> dict[int, int]:
    rows = conn.execute(
        "SELECT instrument_id, count(*) FROM ownership_def14a_current GROUP BY instrument_id"
    ).fetchall()
    return {int(r[0]): int(r[1]) for r in rows}


def _attributed(conn: psycopg.Connection[tuple]) -> set[int]:
    return {int(r[0]) for r in conn.execute("SELECT instrument_id FROM def14a_beneficial_holdings_attributed")}


def test_warrant_suppressed_walk_back_and_unresolved_keeps_rows(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    conn = ebull_test_conn
    conn.autocommit = True
    _seed(conn)
    pairs = [("Common Stock, par value $0.01", "HTZ"), ("Warrants to purchase Common Stock", "HTZWW")]
    calls: list[str] = []

    def fetch_404_then_cover(url: str) -> str | None:
        calls.append(url)
        return None if "000165785326000010" in url else cover_xml(pairs, cik=CIK)

    report = run_recipient_suppressions(conn, fetch_404_then_cover)

    # Newest cover 404 → walked back to the older one.
    assert len(calls) == 2, (calls, report)
    assert report.inserted == 1 and report.deleted == 0 and not report.instruments_failed
    row = conn.execute(
        "SELECT instrument_id, witness_instrument_id, cover_accession, cover_title FROM def14a_recipient_suppressions"
    ).fetchall()
    assert row == [(2, 1, COVER_OLD, "Warrants to purchase Common Stock")]
    assert _attributed(conn) == {1}
    assert _current_holders(conn) == {1: 1}
    assert conn.execute(
        "SELECT document_kind FROM filing_raw_documents WHERE accession_number = %s", (COVER_OLD,)
    ).fetchall() == [("xbrl_cover_instance",)]

    # Second run: all cache hits, no fetch, no change.
    calls.clear()
    report = run_recipient_suppressions(conn, fetch_404_then_cover)
    assert calls == [] and report.inserted == report.updated == report.deleted == 0

    # A transient failure on a NEW uncached cover never removes the stored suppression.
    conn.execute("DELETE FROM sec_cover_12b_pairs")
    conn.execute("DELETE FROM sec_cover_12b_fetches")

    def fetch_503(url: str) -> str | None:
        raise httpx.HTTPStatusError("503", request=httpx.Request("GET", url), response=httpx.Response(503))

    report = run_recipient_suppressions(conn, fetch_503)
    assert report.accessions_unresolved == 1 and report.deleted == 0
    assert _attributed(conn) == {1}

    # Reversal: delete the ledger row; the next evidence-free run restores attribution.
    def fetch_none(url: str) -> str | None:
        return None

    conn.execute("DELETE FROM sec_cover_12b_pairs")
    conn.execute("DELETE FROM sec_cover_12b_fetches")
    report = run_recipient_suppressions(conn, fetch_none)
    assert report.deleted == 1
    assert _attributed(conn) == {1, 2}
    assert _current_holders(conn) == {1: 1, 2: 1}
