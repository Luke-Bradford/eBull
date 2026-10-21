"""#2351 slices 2 / 3b — the suppression job against the real schema (views, refresh, cache)."""

from __future__ import annotations

from datetime import date
from typing import LiteralString

import httpx
import psycopg
import pytest

from app.services.def14a_recipients import (
    REASON_CLASS_ROW,
    RECIPIENT_RULE_VERSION,
    _apply_instrument,
    run_recipient_suppressions,
)
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


def test_row_suppression_withholds_one_holder_from_one_sibling(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """Slice 3b: a row ledger entry hides that (instrument, accession, holder) from all
    three views and ``_current``; the sibling and the other holder are untouched; deleting
    it restores the row."""
    conn = ebull_test_conn
    conn.autocommit = True
    _seed(conn)
    conn.execute(
        """
        INSERT INTO def14a_beneficial_holdings (
            instrument_id, accession_number, issuer_cik, holder_name, holder_role, shares, percent_of_class, as_of_date)
        VALUES (1, %s, %s, 'Plan', 'principal', 5, 1.0, '2026-03-01')
        """,
        (PROXY, CIK),
    )
    conn.execute(
        """
        INSERT INTO ownership_esop_observations (
            instrument_id, plan_name, ownership_nature, source, source_document_id, source_accession,
            filed_at, period_end, known_from, ingest_run_id, shares, percent_of_class)
        VALUES (1, 'Plan', 'beneficial', 'def14a', %s, %s, '2026-04-01', '2026-03-01', '2026-04-01',
                gen_random_uuid(), 5, 1.0)
        """,
        (PROXY, PROXY),
    )
    row = (
        1,
        PROXY,
        "Knighthead Capital",
        CIK,
        REASON_CLASS_ROW,
        RECIPIENT_RULE_VERSION,
        "c-1",
        "Class A Common Stock",
        "HTZ",
        2,
        "Class B Common Stock",
        "Class B Common Stock",
    )
    plan = (1, PROXY, "Plan", *row[3:])
    _apply_instrument(conn, 1, upserts=[], deletes=[], row_upserts=[row, plan], row_deletes=[])

    queries: dict[str, LiteralString] = {
        "holdings": "SELECT instrument_id, holder_name FROM def14a_beneficial_holdings_attributed",
        "def14a": "SELECT instrument_id, holder_name FROM ownership_def14a_observations_attributed",
        "esop": "SELECT instrument_id, plan_name FROM ownership_esop_observations_attributed",
    }

    def names(view: str) -> set[tuple[int, str]]:
        return {(int(r[0]), str(r[1])) for r in conn.execute(queries[view])}

    assert names("holdings") == {(2, "Knighthead Capital")}
    assert names("def14a") == {(2, "Knighthead Capital")}
    assert names("esop") == set()
    assert _current_holders(conn) == {2: 1}

    _apply_instrument(
        conn,
        1,
        upserts=[],
        deletes=[],
        row_upserts=[],
        row_deletes=[(PROXY, "Knighthead Capital"), (PROXY, "Plan")],
    )
    assert names("def14a") == {
        (1, "Knighthead Capital"),
        (2, "Knighthead Capital"),
    }
    assert names("esop") == {(1, "Plan")}
    assert _current_holders(conn) == {1: 1, 2: 1}


# ---------------------------------------------------------------------------
# #3899 — Item 403 table evidence without a retained body (spec D3)
# ---------------------------------------------------------------------------

_PROXY_BODY = """<html><body><p>Security Ownership of Certain Beneficial Owners and Management</p>
<table><tr><td>Name of Beneficial Owner</td><td>Class A Shares</td><td>Percent</td></tr>
<tr><td>Knighthead Capital</td><td>100,000</td><td>12.5%</td></tr></table></body></html>"""
_PROXY_URL = "https://www.sec.gov/Archives/edgar/data/1657853/000110465926044356/proxy.htm"


def _sha(text: str) -> str:
    import hashlib

    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _seed_proxy_body(conn: psycopg.Connection[tuple], *, swept: bool) -> None:
    conn.execute(
        """
        INSERT INTO filing_raw_documents
            (accession_number, document_kind, payload, payload_sha256, payload_swept_at, source_url)
        VALUES (%s, 'def14a_body', %s, %s, CASE WHEN %s THEN NOW() END, %s)
        """,
        (PROXY, None if swept else _PROXY_BODY, _sha(_PROXY_BODY) if swept else None, swept, _PROXY_URL),
    )


def test_item403_tables_cache_needs_no_retained_body(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    from app.providers.implementations.sec_def14a import item403_table_htmls
    from app.services.def14a_ingest import _PARSER_VERSION_DEF14A
    from app.services.def14a_recipients import item403_tables

    conn = ebull_test_conn
    conn.autocommit = True
    _seed_proxy_body(conn, swept=True)
    calls: list[str] = []

    def fetch(url: str) -> str | None:
        calls.append(url)
        return _PROXY_BODY

    def no_fetch(url: str) -> str | None:
        raise AssertionError(f"warm cache must not fetch {url}")

    expected = item403_table_htmls(_PROXY_BODY)
    # Miss on a swept body: one verified fetch, cache filled under the body's hash.
    assert item403_tables(conn, PROXY, fetch) == expected
    assert calls == [_PROXY_URL]
    cached = conn.execute("SELECT parser_version, body_sha256 FROM def14a_item403_tables").fetchall()
    assert cached == [(_PARSER_VERSION_DEF14A, _sha(_PROXY_BODY))]
    # Hit: looked up on the stored hash before any fetch.
    assert item403_tables(conn, PROXY, no_fetch) == expected

    # A row under an older parser version is a miss.
    conn.execute("UPDATE def14a_item403_tables SET parser_version = 'def14a-v0'")
    calls.clear()
    assert item403_tables(conn, PROXY, fetch) == expected and calls == [_PROXY_URL]

    # A swept body whose re-fetch no longer matches is unavailable, and not cached.
    conn.execute("DELETE FROM def14a_item403_tables")
    assert item403_tables(conn, PROXY, lambda _u: "<html>changed</html>") == "mismatch"
    assert conn.execute("SELECT count(*) FROM def14a_item403_tables").fetchone() == (0,)

    # A live body never fetches; it fills the cache under its own hash.
    conn.execute(
        "UPDATE filing_raw_documents SET payload = %s, payload_sha256 = NULL, payload_swept_at = NULL",
        (_PROXY_BODY,),
    )
    assert item403_tables(conn, PROXY, no_fetch) == expected
    assert conn.execute("SELECT body_sha256 FROM def14a_item403_tables").fetchall() == [(_sha(_PROXY_BODY),)]


def test_unreadable_swept_body_blocks_the_accession_and_keeps_its_rows(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> None:
    """A swept proxy body whose re-fetch fails verification is absence of evidence: the
    accession is blocked like an unresolved cover, so a stored suppression — here one
    the cover alone would rewrite — is kept untouched, and nothing is inserted."""
    conn = ebull_test_conn
    conn.autocommit = True
    _seed(conn)
    _seed_proxy_body(conn, swept=True)
    conn.execute(
        """
        INSERT INTO def14a_recipient_suppressions (
            instrument_id, accession_number, issuer_cik, reason, rule_version, cover_accession,
            cover_title, cover_symbol, witness_instrument_id, witness_title)
        VALUES (2, %s, %s, 'non_common_sibling', %s, %s, 'stale title', 'HTZWW', 1, 'Common Stock')
        """,
        (PROXY, CIK, RECIPIENT_RULE_VERSION, COVER_NEW),
    )
    pairs = [("Common Stock, par value $0.01", "HTZ"), ("Warrants to purchase Common Stock", "HTZWW")]

    def fetch(url: str) -> str | None:
        return "<html>SEC now serves other bytes</html>" if url == _PROXY_URL else cover_xml(pairs, cik=CIK)

    report = run_recipient_suppressions(conn, fetch)
    assert report.accessions_evidence_unavailable == 1
    assert report.inserted == report.updated == report.deleted == 0 and not report.instruments_failed
    assert conn.execute("SELECT cover_title FROM def14a_recipient_suppressions").fetchall() == [("stale title",)]

    # Once the body verifies, the accession is decided normally again.
    def fetch_ok(url: str) -> str | None:
        return _PROXY_BODY if url == _PROXY_URL else cover_xml(pairs, cik=CIK)

    report = run_recipient_suppressions(conn, fetch_ok)
    assert report.accessions_evidence_unavailable == 0
    assert report.updated == 1
    assert conn.execute("SELECT cover_title FROM def14a_recipient_suppressions").fetchall() == [
        ("Warrants to purchase Common Stock",)
    ]
