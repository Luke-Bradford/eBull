"""Tests for the 10-K manifest-worker parser adapter (#1151).

Covers:

- Happy path: HTML fetch → store_raw → parse → upsert blob + sections
  → ParseOutcome(parsed).
- Share-class fan-out: one manifest row writes both siblings.
- 10-K/A fallback: prior plain 10-K rescue when amendment misses Item 1.
- Empty / missing / parse-miss tombstones.
- ``filed_at`` gate suppression + tie-break by accession.
- NULL incumbent backwards compat.
- Section-extraction failure isolation.
- Partial fan-out rollback under deterministic upsert error.
- Fallback store_raw failure preserves original raw + returns failed.
- Transient vs deterministic upsert exceptions.
- ``record_parse_attempt`` not invoked from the manifest path (no
  corruption of an incumbent's body provenance).
- Registration via ``register_all_parsers``.
"""

from __future__ import annotations

from datetime import UTC, datetime

import psycopg
import psycopg.errors
import pytest

from app.jobs.sec_manifest_worker import (
    clear_registered_parsers,
    registered_parser_sources,
    run_manifest_worker,
)
from app.services.sec_manifest import get_manifest_row, record_manifest_entry
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export

# Minimal HTML body the real ``extract_business_section`` parser
# accepts: Item 1 heading + body + Item 1A boundary. The body length
# is comfortably above ``_MIN_BODY_LEN`` (120 chars) so the parse-miss
# branches must explicitly use shorter bodies or monkeypatching.
_FAKE_10K_HTML = (
    "<html><body>"
    "<p>UNITED STATES SECURITIES AND EXCHANGE COMMISSION</p>"
    "<p>FORM 10-K</p>"
    "<h2>Item 1. Business</h2>"
    "<p>Acme Corp is a fabricator of industrial widgets serving "
    "the global market. Acme operates manufacturing facilities in "
    "Ohio and Texas and sells through a network of distributors "
    "across North America, Europe, and Asia. Our customers include "
    "automotive OEMs and aerospace integrators. Strategic priorities "
    "include capacity expansion and supplier diversification.</p>"
    "<h2>Item 1A. Risk Factors</h2>"
    "<p>The risks of our business include cyclicality and tariffs.</p>"
    "</body></html>"
)

_FAKE_10KA_HTML_NO_ITEM_1 = (
    "<html><body>"
    "<p>UNITED STATES SECURITIES AND EXCHANGE COMMISSION</p>"
    "<p>FORM 10-K/A</p>"
    "<h2>Item 1A. Risk Factors</h2>"
    "<p>The risks of our business include cyclicality and tariffs.</p>"
    "<h2>Item 9B. Other Information</h2>"
    "<p>Part-III amendment.</p>"
    "</body></html>"
)


def _seed_instrument(
    conn: psycopg.Connection[tuple],
    *,
    iid: int,
    symbol: str,
    cik: str | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, is_tradable)
        VALUES (%s, %s, %s, '4', 'USD', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        (iid, symbol, f"{symbol} co"),
    )
    if cik is not None:
        conn.execute(
            """
            INSERT INTO external_identifiers (
                instrument_id, provider, identifier_type, identifier_value,
                is_primary, last_verified_at
            )
            VALUES (%s, 'sec', 'cik', %s, TRUE, NOW())
            ON CONFLICT (provider, identifier_type, identifier_value, instrument_id)
                WHERE provider = 'sec' AND identifier_type = 'cik'
            DO NOTHING
            """,
            (iid, cik),
        )


def _seed_pending_10k(
    conn: psycopg.Connection[tuple],
    *,
    accession: str,
    instrument_id: int,
    cik: str = "0000999990",
    form: str = "10-K",
    filed_at: datetime | None = None,
    primary_doc_url: str | None = None,
) -> None:
    if filed_at is None:
        filed_at = datetime(2026, 3, 15, tzinfo=UTC)
    if primary_doc_url is None:
        primary_doc_url = (
            f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession.replace('-', '')}/primary_doc.htm"
        )
    record_manifest_entry(
        conn,
        accession,
        cik=cik,
        form=form,
        source="sec_10k",
        subject_type="issuer",
        subject_id=str(instrument_id),
        instrument_id=instrument_id,
        filed_at=filed_at,
        primary_document_url=primary_doc_url,
    )


def _seed_filing_event(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
    accession: str,
    filing_type: str,
    filing_date,
    primary_doc_url: str,
) -> None:
    """Seed a filing_events row so _find_prior_plain_10k can locate the
    fallback target during 10-K/A rescue tests."""
    conn.execute(
        """
        INSERT INTO filing_events (
            provider, provider_filing_id, instrument_id, filing_type,
            filing_date, primary_document_url
        )
        VALUES ('sec', %s, %s, %s, %s, %s)
        ON CONFLICT (provider, provider_filing_id, instrument_id) DO NOTHING
        """,
        (accession, instrument_id, filing_type, filing_date, primary_doc_url),
    )


@pytest.fixture(autouse=True)
def _reset_registry_then_reload():
    from app.services.manifest_parsers import register_all_parsers

    clear_registered_parsers()
    register_all_parsers()
    yield
    clear_registered_parsers()
    register_all_parsers()


def _patch_fetch(monkeypatch, payload: str | None):
    from app.providers.implementations import sec_edgar

    monkeypatch.setattr(
        sec_edgar.SecFilingsProvider,
        "fetch_document_text",
        lambda self, url: payload,
    )


def _patch_fetch_map(monkeypatch, payloads: dict[str, str | None]):
    from app.providers.implementations import sec_edgar

    def _fake(self, url: str):  # noqa: ARG001
        return payloads.get(url)

    monkeypatch.setattr(sec_edgar.SecFilingsProvider, "fetch_document_text", _fake)


def _read_summary(conn, instrument_id):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT body, source_accession, filed_at FROM instrument_business_summary WHERE instrument_id = %s",
            (instrument_id,),
        )
        return cur.fetchone()


def _count_sections(conn, instrument_id, accession):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM instrument_business_summary_sections "
            "WHERE instrument_id = %s AND source_accession = %s",
            (instrument_id, accession),
        )
        return int(cur.fetchone()[0])


# ---------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------


def test_happy_path_parses_and_stores_raw(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Manifest worker drains a 10-K pending row: fetch → store_raw →
    parse → blob + sections upserted; manifest row reflects parsed +
    raw stored."""
    iid = 10100001
    _seed_instrument(ebull_test_conn, iid=iid, symbol="ACME", cik="0000999991")
    _seed_pending_10k(
        ebull_test_conn,
        accession="0000999991-26-000001",
        instrument_id=iid,
        cik="0000999991",
    )
    ebull_test_conn.commit()

    _patch_fetch(monkeypatch, _FAKE_10K_HTML)

    stats = run_manifest_worker(ebull_test_conn, source="sec_10k", max_rows=10)
    ebull_test_conn.commit()

    assert stats.parsed == 1
    assert stats.skipped_no_parser == 0

    row = get_manifest_row(ebull_test_conn, "0000999991-26-000001")
    assert row is not None
    assert row.ingest_status == "parsed"
    assert row.raw_status == "stored"

    body_row = _read_summary(ebull_test_conn, iid)
    assert body_row is not None
    assert "industrial widgets" in body_row[0]
    assert body_row[1] == "0000999991-26-000001"
    assert body_row[2] is not None  # filed_at populated

    # Raw stored under primary_doc.
    with ebull_test_conn.cursor() as cur:
        cur.execute(
            "SELECT byte_count FROM filing_raw_documents "
            "WHERE accession_number = '0000999991-26-000001' AND document_kind = 'primary_doc'"
        )
        raw = cur.fetchone()
    assert raw is not None and raw[0] > 0


def test_happy_path_fans_out_to_share_class_siblings(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Two instruments sharing one CIK both receive blob + sections
    from a single manifest row (10-K is entity-level; #1117 fan-out)."""
    iid_a = 10100100
    iid_b = 10100101
    cik = "0000999992"
    _seed_instrument(ebull_test_conn, iid=iid_a, symbol="ACMEA", cik=cik)
    _seed_instrument(ebull_test_conn, iid=iid_b, symbol="ACMEB", cik=cik)
    _seed_pending_10k(
        ebull_test_conn,
        accession="0000999992-26-000002",
        instrument_id=iid_a,
        cik=cik,
    )
    ebull_test_conn.commit()

    _patch_fetch(monkeypatch, _FAKE_10K_HTML)

    run_manifest_worker(ebull_test_conn, source="sec_10k", max_rows=10)
    ebull_test_conn.commit()

    for iid in (iid_a, iid_b):
        body_row = _read_summary(ebull_test_conn, iid)
        assert body_row is not None, f"sibling iid={iid} missing blob"
        assert body_row[1] == "0000999992-26-000002"
        assert _count_sections(ebull_test_conn, iid, "0000999992-26-000002") > 0


# ---------------------------------------------------------------------
# 10-K/A fallback
# ---------------------------------------------------------------------


def test_10ka_with_item1_present_no_fallback(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """10-K/A whose body carries Item 1 directly parses without
    triggering the prior-10-K fallback."""
    iid = 10100200
    cik = "0000999993"
    _seed_instrument(ebull_test_conn, iid=iid, symbol="AMNDR", cik=cik)
    _seed_pending_10k(
        ebull_test_conn,
        accession="0000999993-26-000003",
        instrument_id=iid,
        cik=cik,
        form="10-K/A",
    )
    ebull_test_conn.commit()

    _patch_fetch(monkeypatch, _FAKE_10K_HTML)

    run_manifest_worker(ebull_test_conn, source="sec_10k", max_rows=10)
    ebull_test_conn.commit()

    body_row = _read_summary(ebull_test_conn, iid)
    assert body_row is not None
    assert body_row[1] == "0000999993-26-000003"  # NOT a fallback acc


def test_10ka_falls_back_to_prior_plain_10k(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When 10-K/A misses Item 1, the adapter fetches the prior plain
    10-K from filing_events and persists the parent under the
    fallback's accession + filed_at."""
    iid = 10100300
    cik = "0000999994"
    _seed_instrument(ebull_test_conn, iid=iid, symbol="PTIII", cik=cik)

    amendment_acc = "0000999994-26-000004"
    fallback_acc = "0000999994-25-000010"
    amendment_url = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{amendment_acc.replace('-', '')}/amend.htm"
    fallback_url = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{fallback_acc.replace('-', '')}/original.htm"

    from datetime import date

    _seed_filing_event(
        ebull_test_conn,
        instrument_id=iid,
        accession=amendment_acc,
        filing_type="10-K/A",
        filing_date=date(2026, 4, 1),
        primary_doc_url=amendment_url,
    )
    _seed_filing_event(
        ebull_test_conn,
        instrument_id=iid,
        accession=fallback_acc,
        filing_type="10-K",
        filing_date=date(2025, 3, 1),
        primary_doc_url=fallback_url,
    )
    _seed_pending_10k(
        ebull_test_conn,
        accession=amendment_acc,
        instrument_id=iid,
        cik=cik,
        form="10-K/A",
        filed_at=datetime(2026, 4, 1, tzinfo=UTC),
        primary_doc_url=amendment_url,
    )
    ebull_test_conn.commit()

    _patch_fetch_map(
        monkeypatch,
        {
            amendment_url: _FAKE_10KA_HTML_NO_ITEM_1,
            fallback_url: _FAKE_10K_HTML,
        },
    )

    run_manifest_worker(ebull_test_conn, source="sec_10k", max_rows=10)
    ebull_test_conn.commit()

    body_row = _read_summary(ebull_test_conn, iid)
    assert body_row is not None
    assert body_row[1] == fallback_acc
    # Filed-at is fallback's date.
    assert body_row[2].year == 2025

    with ebull_test_conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM filing_raw_documents WHERE accession_number = %s AND document_kind = 'primary_doc'",
            (fallback_acc,),
        )
        assert cur.fetchone() is not None
        cur.execute(
            "SELECT 1 FROM filing_raw_documents WHERE accession_number = %s AND document_kind = 'primary_doc'",
            (amendment_acc,),
        )
        assert cur.fetchone() is not None  # original raw stored too


def test_10ka_no_prior_plain_10k_tombstones(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """10-K/A misses Item 1 and no prior plain 10-K exists →
    manifest row tombstoned with raw stored."""
    iid = 10100400
    cik = "0000999995"
    _seed_instrument(ebull_test_conn, iid=iid, symbol="NOPRI", cik=cik)
    _seed_pending_10k(
        ebull_test_conn,
        accession="0000999995-26-000005",
        instrument_id=iid,
        cik=cik,
        form="10-K/A",
    )
    ebull_test_conn.commit()

    _patch_fetch(monkeypatch, _FAKE_10KA_HTML_NO_ITEM_1)

    run_manifest_worker(ebull_test_conn, source="sec_10k", max_rows=10)
    ebull_test_conn.commit()

    row = get_manifest_row(ebull_test_conn, "0000999995-26-000005")
    assert row is not None
    assert row.ingest_status == "tombstoned"
    assert row.raw_status == "stored"


def test_10ka_fallback_fetch_error_returns_failed_with_raw_stored(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If the fallback fetch raises, the manifest returns ``failed``
    (worker retries) with ``raw_status='stored'`` reflecting the
    original 10-K/A raw that already landed."""
    iid = 10100500
    cik = "0000999996"
    _seed_instrument(ebull_test_conn, iid=iid, symbol="FBFAI", cik=cik)

    amendment_acc = "0000999996-26-000006"
    fallback_acc = "0000999996-25-000020"
    amendment_url = "https://example.test/amend.htm"
    fallback_url = "https://example.test/fallback.htm"

    from datetime import date

    _seed_filing_event(
        ebull_test_conn,
        instrument_id=iid,
        accession=amendment_acc,
        filing_type="10-K/A",
        filing_date=date(2026, 5, 1),
        primary_doc_url=amendment_url,
    )
    _seed_filing_event(
        ebull_test_conn,
        instrument_id=iid,
        accession=fallback_acc,
        filing_type="10-K",
        filing_date=date(2025, 4, 1),
        primary_doc_url=fallback_url,
    )
    _seed_pending_10k(
        ebull_test_conn,
        accession=amendment_acc,
        instrument_id=iid,
        cik=cik,
        form="10-K/A",
        primary_doc_url=amendment_url,
    )
    ebull_test_conn.commit()

    from app.providers.implementations import sec_edgar

    def _fake(self, url: str):  # noqa: ARG001
        if url == fallback_url:
            raise RuntimeError("synthetic fallback fetch error")
        return _FAKE_10KA_HTML_NO_ITEM_1

    monkeypatch.setattr(sec_edgar.SecFilingsProvider, "fetch_document_text", _fake)

    run_manifest_worker(ebull_test_conn, source="sec_10k", max_rows=10)
    ebull_test_conn.commit()

    row = get_manifest_row(ebull_test_conn, amendment_acc)
    assert row is not None
    assert row.ingest_status == "failed"
    assert row.raw_status == "stored"
    assert row.error is not None and "fallback fetch error" in row.error


def test_10ka_fallback_empty_body_tombstones(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    iid = 10100600
    cik = "0000999997"
    _seed_instrument(ebull_test_conn, iid=iid, symbol="FBEMP", cik=cik)
    amendment_acc = "0000999997-26-000007"
    fallback_acc = "0000999997-25-000030"
    amendment_url = "https://example.test/amend2.htm"
    fallback_url = "https://example.test/fallback2.htm"

    from datetime import date

    _seed_filing_event(
        ebull_test_conn,
        instrument_id=iid,
        accession=amendment_acc,
        filing_type="10-K/A",
        filing_date=date(2026, 4, 15),
        primary_doc_url=amendment_url,
    )
    _seed_filing_event(
        ebull_test_conn,
        instrument_id=iid,
        accession=fallback_acc,
        filing_type="10-K",
        filing_date=date(2025, 5, 15),
        primary_doc_url=fallback_url,
    )
    _seed_pending_10k(
        ebull_test_conn,
        accession=amendment_acc,
        instrument_id=iid,
        cik=cik,
        form="10-K/A",
        primary_doc_url=amendment_url,
    )
    ebull_test_conn.commit()

    _patch_fetch_map(
        monkeypatch,
        {
            amendment_url: _FAKE_10KA_HTML_NO_ITEM_1,
            fallback_url: None,
        },
    )

    run_manifest_worker(ebull_test_conn, source="sec_10k", max_rows=10)
    ebull_test_conn.commit()

    row = get_manifest_row(ebull_test_conn, amendment_acc)
    assert row is not None
    assert row.ingest_status == "tombstoned"


def test_10ka_fallback_parse_exception_returns_failed_with_raw_stored(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    iid = 10100700
    cik = "0000999998"
    _seed_instrument(ebull_test_conn, iid=iid, symbol="FBPAR", cik=cik)
    amendment_acc = "0000999998-26-000008"
    fallback_acc = "0000999998-25-000040"
    amendment_url = "https://example.test/amend3.htm"
    fallback_url = "https://example.test/fallback3.htm"

    from datetime import date

    _seed_filing_event(
        ebull_test_conn,
        instrument_id=iid,
        accession=amendment_acc,
        filing_type="10-K/A",
        filing_date=date(2026, 6, 1),
        primary_doc_url=amendment_url,
    )
    _seed_filing_event(
        ebull_test_conn,
        instrument_id=iid,
        accession=fallback_acc,
        filing_type="10-K",
        filing_date=date(2025, 6, 1),
        primary_doc_url=fallback_url,
    )
    _seed_pending_10k(
        ebull_test_conn,
        accession=amendment_acc,
        instrument_id=iid,
        cik=cik,
        form="10-K/A",
        primary_doc_url=amendment_url,
    )
    ebull_test_conn.commit()

    _patch_fetch_map(
        monkeypatch,
        {
            amendment_url: _FAKE_10KA_HTML_NO_ITEM_1,
            fallback_url: _FAKE_10K_HTML,
        },
    )

    from app.services.manifest_parsers import sec_10k as parser_module

    calls = {"count": 0}
    real_extract = parser_module.extract_business_section

    def _flaky(html):
        calls["count"] += 1
        if calls["count"] >= 2:  # the FALLBACK call raises
            raise RuntimeError("synthetic fallback parse crash")
        return real_extract(html)

    monkeypatch.setattr(parser_module, "extract_business_section", _flaky)

    run_manifest_worker(ebull_test_conn, source="sec_10k", max_rows=10)
    ebull_test_conn.commit()

    row = get_manifest_row(ebull_test_conn, amendment_acc)
    assert row is not None
    assert row.ingest_status == "failed"
    assert row.raw_status == "stored"
    assert row.error is not None and "fallback parse error" in row.error


def test_10ka_fallback_store_raw_failure_returns_failed_with_original_raw_stored(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fallback ``store_raw`` raise → ``failed`` with raw_status=stored;
    the original amendment's raw row still exists; no parent write."""
    iid = 10100800
    cik = "0000999920"
    _seed_instrument(ebull_test_conn, iid=iid, symbol="FBSTR", cik=cik)
    amendment_acc = "0000999920-26-000080"
    fallback_acc = "0000999920-25-000080"
    amendment_url = "https://example.test/amend4.htm"
    fallback_url = "https://example.test/fallback4.htm"

    from datetime import date

    _seed_filing_event(
        ebull_test_conn,
        instrument_id=iid,
        accession=amendment_acc,
        filing_type="10-K/A",
        filing_date=date(2026, 7, 1),
        primary_doc_url=amendment_url,
    )
    _seed_filing_event(
        ebull_test_conn,
        instrument_id=iid,
        accession=fallback_acc,
        filing_type="10-K",
        filing_date=date(2025, 7, 1),
        primary_doc_url=fallback_url,
    )
    _seed_pending_10k(
        ebull_test_conn,
        accession=amendment_acc,
        instrument_id=iid,
        cik=cik,
        form="10-K/A",
        primary_doc_url=amendment_url,
    )
    ebull_test_conn.commit()

    _patch_fetch_map(
        monkeypatch,
        {
            amendment_url: _FAKE_10KA_HTML_NO_ITEM_1,
            fallback_url: _FAKE_10K_HTML,
        },
    )

    from app.services.manifest_parsers import sec_10k as parser_module

    real_store = parser_module.store_raw

    def _flaky_store(conn, *, accession_number, **kwargs):
        if accession_number == fallback_acc:
            raise RuntimeError("synthetic fallback store_raw crash")
        return real_store(conn, accession_number=accession_number, **kwargs)

    monkeypatch.setattr(parser_module, "store_raw", _flaky_store)

    run_manifest_worker(ebull_test_conn, source="sec_10k", max_rows=10)
    ebull_test_conn.commit()

    row = get_manifest_row(ebull_test_conn, amendment_acc)
    assert row is not None
    assert row.ingest_status == "failed"
    assert row.raw_status == "stored"

    body_row = _read_summary(ebull_test_conn, iid)
    assert body_row is None  # no parent write

    # Original raw still present.
    with ebull_test_conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM filing_raw_documents WHERE accession_number = %s",
            (amendment_acc,),
        )
        assert cur.fetchone() is not None
        cur.execute(
            "SELECT 1 FROM filing_raw_documents WHERE accession_number = %s",
            (fallback_acc,),
        )
        assert cur.fetchone() is None


# ---------------------------------------------------------------------
# Failure paths on the original fetch / parse
# ---------------------------------------------------------------------


def test_fetch_error_returns_failed_outcome(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    iid = 10100900
    _seed_instrument(ebull_test_conn, iid=iid, symbol="FERR", cik="0000999921")
    _seed_pending_10k(
        ebull_test_conn,
        accession="0000999921-26-000009",
        instrument_id=iid,
        cik="0000999921",
    )
    ebull_test_conn.commit()

    from app.providers.implementations import sec_edgar

    def _boom(self, url):  # noqa: ARG001
        raise RuntimeError("network kaput")

    monkeypatch.setattr(sec_edgar.SecFilingsProvider, "fetch_document_text", _boom)

    before = datetime.now(tz=UTC)
    run_manifest_worker(ebull_test_conn, source="sec_10k", max_rows=10)
    ebull_test_conn.commit()

    row = get_manifest_row(ebull_test_conn, "0000999921-26-000009")
    assert row is not None
    assert row.ingest_status == "failed"
    assert row.error is not None and "fetch error" in row.error
    assert row.next_retry_at is not None
    delta = (row.next_retry_at - before).total_seconds()
    assert 3300 < delta < 3900


def test_empty_fetch_tombstones_without_raw_stored(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    iid = 10101000
    _seed_instrument(ebull_test_conn, iid=iid, symbol="EMTY", cik="0000999922")
    _seed_pending_10k(
        ebull_test_conn,
        accession="0000999922-26-000010",
        instrument_id=iid,
        cik="0000999922",
    )
    ebull_test_conn.commit()

    _patch_fetch(monkeypatch, None)

    run_manifest_worker(ebull_test_conn, source="sec_10k", max_rows=10)
    ebull_test_conn.commit()

    row = get_manifest_row(ebull_test_conn, "0000999922-26-000010")
    assert row is not None
    assert row.ingest_status == "tombstoned"

    with ebull_test_conn.cursor() as cur:
        cur.execute("SELECT 1 FROM filing_raw_documents WHERE accession_number = '0000999922-26-000010'")
        assert cur.fetchone() is None


def test_plain_10k_no_item1_tombstones_with_raw_stored(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A plain 10-K (not amendment) whose body lacks Item 1 tombstones
    immediately — the 10-K/A fallback path is reserved for the
    amendment form."""
    iid = 10101100
    _seed_instrument(ebull_test_conn, iid=iid, symbol="NOIT1", cik="0000999923")
    _seed_pending_10k(
        ebull_test_conn,
        accession="0000999923-26-000011",
        instrument_id=iid,
        cik="0000999923",
        form="10-K",
    )
    ebull_test_conn.commit()

    _patch_fetch(monkeypatch, _FAKE_10KA_HTML_NO_ITEM_1)

    run_manifest_worker(ebull_test_conn, source="sec_10k", max_rows=10)
    ebull_test_conn.commit()

    row = get_manifest_row(ebull_test_conn, "0000999923-26-000011")
    assert row is not None
    assert row.ingest_status == "tombstoned"
    assert row.raw_status == "stored"


# ---------------------------------------------------------------------
# filed_at gate (Option C)
# ---------------------------------------------------------------------


def _seed_incumbent_summary(
    conn,
    *,
    instrument_id: int,
    body: str,
    source_accession: str,
    filed_at: datetime | None,
) -> None:
    conn.execute(
        """
        INSERT INTO instrument_business_summary
            (instrument_id, body, source_accession, filed_at)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (instrument_id) DO UPDATE SET
            body = EXCLUDED.body,
            source_accession = EXCLUDED.source_accession,
            filed_at = EXCLUDED.filed_at
        """,
        (instrument_id, body, source_accession, filed_at),
    )


def test_filed_at_gate_suppresses_older_arrival(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Seed incumbent filed_at=2024 + accession=A1; manifest row
    filed_at=2020 + accession=A0 → adapter returns parsed but body +
    sections unchanged."""
    iid = 10101200
    cik = "0000999924"
    _seed_instrument(ebull_test_conn, iid=iid, symbol="GATE", cik=cik)

    _seed_incumbent_summary(
        ebull_test_conn,
        instrument_id=iid,
        body="incumbent newer body sentinel " * 6,
        source_accession="0000999924-24-000099",
        filed_at=datetime(2024, 6, 1, tzinfo=UTC),
    )
    _seed_pending_10k(
        ebull_test_conn,
        accession="0000999924-20-000099",
        instrument_id=iid,
        cik=cik,
        filed_at=datetime(2020, 6, 1, tzinfo=UTC),
    )
    ebull_test_conn.commit()

    _patch_fetch(monkeypatch, _FAKE_10K_HTML)

    run_manifest_worker(ebull_test_conn, source="sec_10k", max_rows=10)
    ebull_test_conn.commit()

    row = get_manifest_row(ebull_test_conn, "0000999924-20-000099")
    assert row is not None
    assert row.ingest_status == "parsed"  # drain succeeded
    body_row = _read_summary(ebull_test_conn, iid)
    assert body_row is not None
    # Body unchanged: the suppression kept the incumbent's body intact.
    assert body_row[1] == "0000999924-24-000099"
    assert body_row[0].startswith("incumbent newer body sentinel")


def test_filed_at_gate_allows_newer_arrival(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    iid = 10101300
    cik = "0000999925"
    _seed_instrument(ebull_test_conn, iid=iid, symbol="NEWR", cik=cik)
    _seed_incumbent_summary(
        ebull_test_conn,
        instrument_id=iid,
        body="incumbent older body sentinel " * 6,
        source_accession="0000999925-20-000099",
        filed_at=datetime(2020, 6, 1, tzinfo=UTC),
    )
    _seed_pending_10k(
        ebull_test_conn,
        accession="0000999925-24-000099",
        instrument_id=iid,
        cik=cik,
        filed_at=datetime(2024, 6, 1, tzinfo=UTC),
    )
    ebull_test_conn.commit()

    _patch_fetch(monkeypatch, _FAKE_10K_HTML)

    run_manifest_worker(ebull_test_conn, source="sec_10k", max_rows=10)
    ebull_test_conn.commit()

    body_row = _read_summary(ebull_test_conn, iid)
    assert body_row is not None
    assert body_row[1] == "0000999925-24-000099"
    assert "industrial widgets" in body_row[0]


def test_same_day_accession_tiebreaker_picks_later(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Two filings same filed_at but different accession numbers —
    higher accession wins the (filed_at, accession) tuple gate."""
    iid = 10101400
    cik = "0000999926"
    _seed_instrument(ebull_test_conn, iid=iid, symbol="TIEBR", cik=cik)
    _seed_incumbent_summary(
        ebull_test_conn,
        instrument_id=iid,
        body="incumbent same-day earlier accession " * 5,
        source_accession="0000999926-26-000001",
        filed_at=datetime(2026, 1, 1, tzinfo=UTC),
    )
    _seed_pending_10k(
        ebull_test_conn,
        accession="0000999926-26-000002",  # later accession
        instrument_id=iid,
        cik=cik,
        filed_at=datetime(2026, 1, 1, tzinfo=UTC),
    )
    ebull_test_conn.commit()

    _patch_fetch(monkeypatch, _FAKE_10K_HTML)

    run_manifest_worker(ebull_test_conn, source="sec_10k", max_rows=10)
    ebull_test_conn.commit()

    body_row = _read_summary(ebull_test_conn, iid)
    assert body_row is not None
    assert body_row[1] == "0000999926-26-000002"


def test_same_day_accession_tiebreaker_suppresses_lower(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Same filed_at, lower-accession arrival → suppressed; incumbent
    higher-accession body must survive. Pin the reverse direction of
    the (filed_at, accession) tuple gate."""
    iid = 10101450
    cik = "0000999926"
    _seed_instrument(ebull_test_conn, iid=iid, symbol="TIEBL", cik=cik)
    _seed_incumbent_summary(
        ebull_test_conn,
        instrument_id=iid,
        body="incumbent same-day higher accession sentinel " * 5,
        source_accession="0000999926-26-000050",
        filed_at=datetime(2026, 1, 1, tzinfo=UTC),
    )
    _seed_pending_10k(
        ebull_test_conn,
        accession="0000999926-26-000049",  # LOWER accession arrives next
        instrument_id=iid,
        cik=cik,
        filed_at=datetime(2026, 1, 1, tzinfo=UTC),
    )
    ebull_test_conn.commit()

    _patch_fetch(monkeypatch, _FAKE_10K_HTML)

    run_manifest_worker(ebull_test_conn, source="sec_10k", max_rows=10)
    ebull_test_conn.commit()

    body_row = _read_summary(ebull_test_conn, iid)
    assert body_row is not None
    assert body_row[1] == "0000999926-26-000050"  # incumbent retained
    assert body_row[0].startswith("incumbent same-day higher accession sentinel")


def test_null_incumbent_filed_at_allows_write(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    iid = 10101500
    cik = "0000999927"
    _seed_instrument(ebull_test_conn, iid=iid, symbol="NULLI", cik=cik)
    _seed_incumbent_summary(
        ebull_test_conn,
        instrument_id=iid,
        body="legacy row prior to filed_at column " * 5,
        source_accession="legacy-accession",
        filed_at=None,
    )
    _seed_pending_10k(
        ebull_test_conn,
        accession="0000999927-26-000050",
        instrument_id=iid,
        cik=cik,
    )
    ebull_test_conn.commit()

    _patch_fetch(monkeypatch, _FAKE_10K_HTML)

    run_manifest_worker(ebull_test_conn, source="sec_10k", max_rows=10)
    ebull_test_conn.commit()

    body_row = _read_summary(ebull_test_conn, iid)
    assert body_row is not None
    assert body_row[1] == "0000999927-26-000050"


def test_suppressed_parent_skips_sections_write(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Suppression branch must not DELETE+INSERT the incumbent's
    sections — the older accession has nothing to contribute."""
    iid = 10101600
    cik = "0000999928"
    _seed_instrument(ebull_test_conn, iid=iid, symbol="SKIPS", cik=cik)
    incumbent_acc = "0000999928-24-000099"
    _seed_incumbent_summary(
        ebull_test_conn,
        instrument_id=iid,
        body="incumbent body " * 10,
        source_accession=incumbent_acc,
        filed_at=datetime(2024, 6, 1, tzinfo=UTC),
    )
    # Seed a sections row under the incumbent accession.
    ebull_test_conn.execute(
        """
        INSERT INTO instrument_business_summary_sections
            (instrument_id, source_accession, section_order, section_key,
             section_label, body)
        VALUES (%s, %s, 1, 'general', 'Overview', 'sentinel section body')
        """,
        (iid, incumbent_acc),
    )
    _seed_pending_10k(
        ebull_test_conn,
        accession="0000999928-20-000099",
        instrument_id=iid,
        cik=cik,
        filed_at=datetime(2020, 6, 1, tzinfo=UTC),
    )
    ebull_test_conn.commit()

    _patch_fetch(monkeypatch, _FAKE_10K_HTML)

    run_manifest_worker(ebull_test_conn, source="sec_10k", max_rows=10)
    ebull_test_conn.commit()

    # Incumbent sections still present.
    with ebull_test_conn.cursor() as cur:
        cur.execute(
            "SELECT body FROM instrument_business_summary_sections WHERE instrument_id = %s AND source_accession = %s",
            (iid, incumbent_acc),
        )
        rows = cur.fetchall()
    assert len(rows) == 1
    assert rows[0][0] == "sentinel section body"

    # No sections under the older accession.
    assert _count_sections(ebull_test_conn, iid, "0000999928-20-000099") == 0


# ---------------------------------------------------------------------
# Upsert exception discrimination + partial-fanout rollback
# ---------------------------------------------------------------------


def test_transient_upsert_exception_retries(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    iid = 10101700
    cik = "0000999929"
    _seed_instrument(ebull_test_conn, iid=iid, symbol="TRANS", cik=cik)
    _seed_pending_10k(
        ebull_test_conn,
        accession="0000999929-26-000077",
        instrument_id=iid,
        cik=cik,
    )
    ebull_test_conn.commit()

    _patch_fetch(monkeypatch, _FAKE_10K_HTML)

    from app.services.manifest_parsers import sec_10k as parser_module

    def _raising(*args, **kwargs):  # noqa: ARG001
        raise psycopg.errors.SerializationFailure("synthetic serialisation failure")

    monkeypatch.setattr(parser_module, "upsert_business_summary", _raising)

    run_manifest_worker(ebull_test_conn, source="sec_10k", max_rows=10)
    ebull_test_conn.commit()

    row = get_manifest_row(ebull_test_conn, "0000999929-26-000077")
    assert row is not None
    assert row.ingest_status == "failed"
    assert row.raw_status == "stored"
    assert row.error is not None and "SerializationFailure" in row.error


def test_deterministic_upsert_exception_tombstones(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    iid = 10101800
    cik = "0000999930"
    _seed_instrument(ebull_test_conn, iid=iid, symbol="DETER", cik=cik)
    _seed_pending_10k(
        ebull_test_conn,
        accession="0000999930-26-000078",
        instrument_id=iid,
        cik=cik,
    )
    ebull_test_conn.commit()

    _patch_fetch(monkeypatch, _FAKE_10K_HTML)

    from app.services.manifest_parsers import sec_10k as parser_module

    def _raising(*args, **kwargs):  # noqa: ARG001
        raise RuntimeError("synthetic deterministic upsert error")

    monkeypatch.setattr(parser_module, "upsert_business_summary", _raising)

    run_manifest_worker(ebull_test_conn, source="sec_10k", max_rows=10)
    ebull_test_conn.commit()

    row = get_manifest_row(ebull_test_conn, "0000999930-26-000078")
    assert row is not None
    assert row.ingest_status == "tombstoned"
    assert row.raw_status == "stored"
    assert row.error is not None and "RuntimeError" in row.error


def test_partial_fanout_rollback_on_deterministic_error(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Sibling A upsert succeeds, sibling B raises a deterministic
    error → savepoint rolls back A's write too; neither sibling has a
    new blob, manifest tombstones."""
    cik = "0000999931"
    iid_a = 10101901
    iid_b = 10101902
    _seed_instrument(ebull_test_conn, iid=iid_a, symbol="FAA", cik=cik)
    _seed_instrument(ebull_test_conn, iid=iid_b, symbol="FBB", cik=cik)
    _seed_pending_10k(
        ebull_test_conn,
        accession="0000999931-26-000091",
        instrument_id=iid_a,
        cik=cik,
    )
    ebull_test_conn.commit()

    _patch_fetch(monkeypatch, _FAKE_10K_HTML)

    from app.services.manifest_parsers import sec_10k as parser_module

    real_upsert = parser_module.upsert_business_summary

    def _flaky(conn, *, instrument_id, **kwargs):
        if instrument_id == iid_b:
            raise RuntimeError("synthetic sibling B upsert error")
        return real_upsert(conn, instrument_id=instrument_id, **kwargs)

    monkeypatch.setattr(parser_module, "upsert_business_summary", _flaky)

    run_manifest_worker(ebull_test_conn, source="sec_10k", max_rows=10)
    ebull_test_conn.commit()

    row = get_manifest_row(ebull_test_conn, "0000999931-26-000091")
    assert row is not None
    assert row.ingest_status == "tombstoned"
    # Critical: sibling A's blob is NOT present (savepoint rolled back).
    for iid in (iid_a, iid_b):
        body_row = _read_summary(ebull_test_conn, iid)
        assert body_row is None, f"sibling iid={iid} should not have a blob after rollback"


def test_section_extraction_exception_does_not_break_parent_write(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A sections-extractor crash must not propagate; the parent blob
    still writes; sections table stays empty for this accession."""
    iid = 10102000
    cik = "0000999932"
    _seed_instrument(ebull_test_conn, iid=iid, symbol="SECRX", cik=cik)
    _seed_pending_10k(
        ebull_test_conn,
        accession="0000999932-26-000201",
        instrument_id=iid,
        cik=cik,
    )
    ebull_test_conn.commit()

    _patch_fetch(monkeypatch, _FAKE_10K_HTML)

    from app.services.manifest_parsers import sec_10k as parser_module

    def _crash(_html):
        raise RuntimeError("synthetic sections extractor crash")

    monkeypatch.setattr(parser_module, "extract_business_sections", _crash)

    run_manifest_worker(ebull_test_conn, source="sec_10k", max_rows=10)
    ebull_test_conn.commit()

    row = get_manifest_row(ebull_test_conn, "0000999932-26-000201")
    assert row is not None
    assert row.ingest_status == "parsed"
    assert row.raw_status == "stored"

    body_row = _read_summary(ebull_test_conn, iid)
    assert body_row is not None
    assert "industrial widgets" in body_row[0]

    assert _count_sections(ebull_test_conn, iid, "0000999932-26-000201") == 0


def test_tombstone_path_does_not_mutate_existing_body_summary_row(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When the manifest path tombstones (parse miss, empty fetch,
    deterministic upsert error), it MUST NOT call
    ``record_parse_attempt`` — that helper mutates ``source_accession``
    on an existing row and would corrupt the incumbent's provenance.
    Pin the invariant: pre-seed a healthy incumbent, tombstone an
    older accession's manifest row, and verify the incumbent body +
    source_accession survive."""
    iid = 10102100
    cik = "0000999933"
    _seed_instrument(ebull_test_conn, iid=iid, symbol="TMBS", cik=cik)
    _seed_incumbent_summary(
        ebull_test_conn,
        instrument_id=iid,
        body="healthy incumbent narrative " * 6,
        source_accession="0000999933-24-000001",
        filed_at=datetime(2024, 6, 1, tzinfo=UTC),
    )
    _seed_pending_10k(
        ebull_test_conn,
        accession="0000999933-20-000077",
        instrument_id=iid,
        cik=cik,
        filed_at=datetime(2020, 6, 1, tzinfo=UTC),
    )
    ebull_test_conn.commit()

    _patch_fetch(monkeypatch, None)  # empty body → tombstone

    run_manifest_worker(ebull_test_conn, source="sec_10k", max_rows=10)
    ebull_test_conn.commit()

    body_row = _read_summary(ebull_test_conn, iid)
    assert body_row is not None
    assert body_row[1] == "0000999933-24-000001"  # incumbent untouched
    assert body_row[0].startswith("healthy incumbent narrative")


# ---------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------


def test_parser_registered_via_register_all() -> None:
    from app.services.manifest_parsers import register_all_parsers

    assert "sec_10k" in registered_parser_sources()
    clear_registered_parsers()
    assert "sec_10k" not in registered_parser_sources()
    register_all_parsers()
    assert "sec_10k" in registered_parser_sources()
