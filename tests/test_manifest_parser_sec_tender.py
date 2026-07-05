"""Tests for the tender manifest-worker parser adapter (#1982).

Covers (ONE integration test per genuinely-new mechanism — lean-tier policy):
- Happy path incl. the new dual-party mechanism: body + header fetch →
  store_raw (tender_body, SWEPT/born-compacted) → role rows for BOTH header
  parties, independent of which instrument owns the manifest row.
- Header-fetch failure after a stored body → failed(retry) with
  ``raw_status='stored'`` honest (#938).
- Unusable header → tombstone with raw stored.

Fetch is monkeypatched at ``SecFilingsProvider.fetch_document_text`` (URL
dispatch: ``.hdr.sgml`` → header, else body) so tests run without touching
SEC.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import psycopg
import pytest

from app.jobs.sec_manifest_worker import clear_registered_parsers, run_manifest_worker
from app.services.sec_manifest import get_manifest_row, record_manifest_entry
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export

_BODY_URL = "https://www.sec.gov/Archives/edgar/data/1000001/000100000126000001/sctot.htm"

_FAKE_HDR = """<SEC-HEADER>0001000001-26-000001.hdr.sgml : 20260624
<SUBJECT-COMPANY>
<COMPANY-DATA>
<CONFORMED-NAME>Target Test Corp
<CIK>0009170001
</COMPANY-DATA>
</SUBJECT-COMPANY>
<FILED-BY>
<COMPANY-DATA>
<CONFORMED-NAME>Acquirer Test plc
<CIK>0009170002
</COMPANY-DATA>
</FILED-BY>
</SEC-HEADER>"""

_FAKE_BODY = """
<html><body>
SCHEDULE TO Tender Offer Statement under Section 14(d)(1) or 13(e)(1)
&#9746; Third-party tender offer subject to Rule 14d-1.
&#9744; Issuer tender offer subject to Rule 13e-4.
offer to purchase all Shares for $42.00 per Share, net to the seller in cash.
</body></html>
"""


def _seed_instrument(conn: psycopg.Connection[tuple], iid: int, symbol: str, cik: str) -> None:
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, is_tradable)
        VALUES (%s, %s, %s, '4', 'USD', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        (iid, symbol, f"{symbol} co"),
    )
    conn.execute(
        """
        INSERT INTO external_identifiers (instrument_id, provider, identifier_type, identifier_value, is_primary)
        VALUES (%s, 'sec', 'cik', %s, TRUE)
        ON CONFLICT DO NOTHING
        """,
        (iid, cik),
    )


def _seed_pending_tender(
    conn: psycopg.Connection[tuple],
    *,
    accession: str,
    instrument_id: int,
    form: str = "SC TO-T",
    cik: str = "0009170001",
) -> None:
    record_manifest_entry(
        conn,
        accession,
        cik=cik,
        form=form,
        source="sec_tender",
        subject_type="issuer",
        subject_id=str(instrument_id),
        instrument_id=instrument_id,
        filed_at=datetime(2026, 6, 24, tzinfo=UTC),
        primary_document_url=_BODY_URL,
    )


@pytest.fixture(autouse=True)
def _reset_registry_then_reload():
    from app.services.manifest_parsers import register_all_parsers

    clear_registered_parsers()
    register_all_parsers()
    yield
    clear_registered_parsers()
    register_all_parsers()


def _fetch_dispatch(body: str, header: str | None):
    def fetch(self, url: str) -> str:  # noqa: ANN001
        if url.endswith(".hdr.sgml"):
            if header is None:
                raise ConnectionError("header fetch boom")
            return header
        return body

    return fetch


def test_happy_path_dual_party_role_rows(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One manifest row (owned by the SUBJECT's instrument here — ownership is
    arbitrary between the parties) yields a typed row per header party, with
    roles from the SUBJECT-COMPANY / FILED-BY CIK blocks."""
    import app.services.manifest_parsers  # noqa: F401 — register

    _seed_instrument(ebull_test_conn, iid=9170001, symbol="TGT", cik="0009170001")
    _seed_instrument(ebull_test_conn, iid=9170002, symbol="ACQ", cik="0009170002")
    _seed_pending_tender(ebull_test_conn, accession="0001000001-26-000001", instrument_id=9170001)
    ebull_test_conn.commit()

    from app.providers.implementations import sec_edgar

    monkeypatch.setattr(
        sec_edgar.SecFilingsProvider,
        "fetch_document_text",
        _fetch_dispatch(_FAKE_BODY, _FAKE_HDR),
    )

    stats = run_manifest_worker(ebull_test_conn, source="sec_tender", max_rows=10)
    ebull_test_conn.commit()
    assert stats.parsed == 1

    row = get_manifest_row(ebull_test_conn, "0001000001-26-000001")
    assert row is not None
    assert row.ingest_status == "parsed"
    assert row.raw_status == "stored"

    with ebull_test_conn.cursor() as cur:
        cur.execute(
            """
            SELECT instrument_id, role, subject_company_name, subject_cik,
                   offeror_names, is_third_party_tender, is_issuer_tender,
                   offer_price_per_unit, unit_label, currency
            FROM tender_offer_events
            WHERE accession_number = '0001000001-26-000001'
            ORDER BY instrument_id
            """
        )
        events = cur.fetchall()
    assert len(events) == 2
    subject_row, offeror_row = events
    assert subject_row[0] == 9170001 and subject_row[1] == "subject"
    assert offeror_row[0] == 9170002 and offeror_row[1] == "offeror"
    for event in events:
        assert event[2] == "Target Test Corp"
        assert event[3] == "0009170001"
        assert event[4] == ["Acquirer Test plc"]
        assert event[5] is True  # third-party box checked
        assert event[6] is False  # issuer box unchecked
        assert event[7] == Decimal("42.00")
        assert event[8] == "Share"
        assert event[9] == "USD"

    # tender_body is SWEPT (born-compacted): row exists, sha only, no bytes.
    with ebull_test_conn.cursor() as cur:
        cur.execute(
            """
            SELECT payload, payload_sha256 FROM filing_raw_documents
            WHERE accession_number = '0001000001-26-000001' AND document_kind = 'tender_body'
            """
        )
        raw = cur.fetchone()
    assert raw is not None
    assert raw[0] is None
    assert raw[1] is not None


def test_header_fetch_failure_fails_with_retry(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A raised header fetch is transient: failed(retry), no typed rows.

    The raise happens before store_raw (both fetches share the try block), so
    ``raw_status`` is unset — the retry re-fetches both documents.
    """
    import app.services.manifest_parsers  # noqa: F401 — register

    _seed_instrument(ebull_test_conn, iid=9170003, symbol="HDRF", cik="0009170003")
    _seed_pending_tender(
        ebull_test_conn,
        accession="0001000001-26-000002",
        instrument_id=9170003,
        cik="0009170003",
    )
    ebull_test_conn.commit()

    from app.providers.implementations import sec_edgar

    monkeypatch.setattr(
        sec_edgar.SecFilingsProvider,
        "fetch_document_text",
        _fetch_dispatch(_FAKE_BODY, None),
    )

    run_manifest_worker(ebull_test_conn, source="sec_tender", max_rows=10)
    ebull_test_conn.commit()

    row = get_manifest_row(ebull_test_conn, "0001000001-26-000002")
    assert row is not None
    assert row.ingest_status == "failed"
    assert row.next_retry_at is not None
    with ebull_test_conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM tender_offer_events WHERE accession_number = '0001000001-26-000002'")
        count_row = cur.fetchone()
    assert count_row is not None and count_row[0] == 0


def test_unusable_header_tombstones_with_raw_stored(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A header without party blocks → tombstone, but raw IS stored (#938)."""
    import app.services.manifest_parsers  # noqa: F401 — register

    _seed_instrument(ebull_test_conn, iid=9170004, symbol="NOHD", cik="0009170004")
    _seed_pending_tender(
        ebull_test_conn,
        accession="0001000001-26-000003",
        instrument_id=9170004,
        cik="0009170004",
    )
    ebull_test_conn.commit()

    from app.providers.implementations import sec_edgar

    monkeypatch.setattr(
        sec_edgar.SecFilingsProvider,
        "fetch_document_text",
        _fetch_dispatch(_FAKE_BODY, "<SEC-HEADER>no blocks</SEC-HEADER>"),
    )

    run_manifest_worker(ebull_test_conn, source="sec_tender", max_rows=10)
    ebull_test_conn.commit()

    row = get_manifest_row(ebull_test_conn, "0001000001-26-000003")
    assert row is not None
    assert row.ingest_status == "tombstoned"
    assert row.raw_status == "stored"
