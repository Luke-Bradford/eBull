"""Tests for the 424B manifest-worker parser adapter (#1816).

Covers:
- Happy path: HTML fetch → store_raw (prospectus_body, SWEPT/born-compacted) →
  parse → upsert prospectus_offerings → ParseOutcome(parsed, raw_status=stored).
- Tombstone: fetch returns empty body.
- Tombstone with raw stored: body is not a recognizable prospectus.
- Failure: fetch raises (transient — worker retries).

Fetch is monkeypatched at ``SecFilingsProvider.fetch_document_text`` so tests
run without touching SEC.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import psycopg
import pytest

from app.jobs.sec_manifest_worker import clear_registered_parsers, run_manifest_worker
from app.services.sec_manifest import get_manifest_row, record_manifest_entry
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export


def _seed_instrument(conn: psycopg.Connection[tuple], iid: int, symbol: str) -> None:
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, is_tradable)
        VALUES (%s, %s, %s, '4', 'USD', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        (iid, symbol, f"{symbol} co"),
    )


def _seed_pending_424b(
    conn: psycopg.Connection[tuple],
    *,
    accession: str,
    instrument_id: int,
    form: str = "424B4",
    url: str = "https://www.sec.gov/Archives/edgar/data/2080126/000119312526294982/form424b4.htm",
    cik: str = "0002080126",
) -> None:
    record_manifest_entry(
        conn,
        accession,
        cik=cik,
        form=form,
        source="sec_424b",
        subject_type="issuer",
        subject_id=str(instrument_id),
        instrument_id=instrument_id,
        filed_at=datetime(2026, 6, 10, tzinfo=UTC),
        primary_document_url=url,
    )


# Minimal row-major Item 501(b)(3) cover the extractor accepts.
_FAKE_424B_HTML = """
<html><body>
PROSPECTUS 1,000,000 Shares of Common Stock
Per Share Total
Public offering price $ 10.00 $ 10,000,000
Underwriting discounts and commissions $ 0.70 $ 700,000
Proceeds, before expenses, to us $ 9.30 $ 9,300,000
</body></html>
"""


@pytest.fixture(autouse=True)
def _reset_registry_then_reload():
    from app.services.manifest_parsers import register_all_parsers

    clear_registered_parsers()
    register_all_parsers()
    yield
    clear_registered_parsers()
    register_all_parsers()


def test_happy_path_parses_and_born_compacts_raw(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import app.services.manifest_parsers  # noqa: F401 — register

    _seed_instrument(ebull_test_conn, iid=9160001, symbol="ACME")
    _seed_pending_424b(ebull_test_conn, accession="0002080126-26-000001", instrument_id=9160001)
    ebull_test_conn.commit()

    from app.providers.implementations import sec_edgar

    monkeypatch.setattr(
        sec_edgar.SecFilingsProvider,
        "fetch_document_text",
        lambda self, url: _FAKE_424B_HTML,
    )

    stats = run_manifest_worker(ebull_test_conn, source="sec_424b", max_rows=10)
    ebull_test_conn.commit()

    assert stats.parsed == 1
    assert stats.skipped_no_parser == 0

    row = get_manifest_row(ebull_test_conn, "0002080126-26-000001")
    assert row is not None
    assert row.ingest_status == "parsed"
    assert row.raw_status == "stored"

    with ebull_test_conn.cursor() as cur:
        cur.execute(
            """
            SELECT subtype, is_issuer_offering, price_per_unit, unit_label,
                   aggregate_offering_amount, underwriting_discount,
                   net_proceeds_to_issuer, currency, security_type
            FROM prospectus_offerings WHERE accession_number = '0002080126-26-000001'
            """
        )
        po = cur.fetchone()
    assert po is not None
    subtype, is_issuer, price, unit, aggregate, discount, net, currency, sec_type = po
    assert subtype == "424B4"
    assert is_issuer is True
    assert price == Decimal("10.00")
    assert unit == "Per Share"
    assert aggregate == Decimal("10000000")
    assert discount == Decimal("700000")
    assert net == Decimal("9300000")
    assert currency == "USD"
    assert sec_type == "Common Stock"

    # prospectus_body is SWEPT (born-compacted, like 8-K primary_doc):
    # the row exists with a sha but NO payload bytes.
    with ebull_test_conn.cursor() as cur:
        cur.execute(
            """
            SELECT payload, payload_sha256 FROM filing_raw_documents
            WHERE accession_number = '0002080126-26-000001' AND document_kind = 'prospectus_body'
            """
        )
        raw = cur.fetchone()
    assert raw is not None
    payload, sha = raw
    assert payload is None
    assert sha is not None


def test_empty_fetch_tombstones(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import app.services.manifest_parsers  # noqa: F401 — register

    _seed_instrument(ebull_test_conn, iid=9160002, symbol="DEAD")
    _seed_pending_424b(ebull_test_conn, accession="0002080126-26-000002", instrument_id=9160002)
    ebull_test_conn.commit()

    from app.providers.implementations import sec_edgar

    monkeypatch.setattr(sec_edgar.SecFilingsProvider, "fetch_document_text", lambda self, url: "")

    run_manifest_worker(ebull_test_conn, source="sec_424b", max_rows=10)
    ebull_test_conn.commit()

    row = get_manifest_row(ebull_test_conn, "0002080126-26-000002")
    assert row is not None
    assert row.ingest_status == "tombstoned"
    with ebull_test_conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM prospectus_offerings WHERE accession_number = '0002080126-26-000002'")
        count_row = cur.fetchone()
    assert count_row is not None and count_row[0] == 0


def test_unrecognized_body_tombstones_with_raw_stored(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A non-prospectus body → tombstone, but raw IS stored (#938 invariant)."""
    import app.services.manifest_parsers  # noqa: F401 — register

    _seed_instrument(ebull_test_conn, iid=9160003, symbol="WEIRD")
    _seed_pending_424b(ebull_test_conn, accession="0002080126-26-000003", instrument_id=9160003)
    ebull_test_conn.commit()

    from app.providers.implementations import sec_edgar

    monkeypatch.setattr(
        sec_edgar.SecFilingsProvider,
        "fetch_document_text",
        lambda self, url: "<html><body>not an offering document at all</body></html>",
    )

    run_manifest_worker(ebull_test_conn, source="sec_424b", max_rows=10)
    ebull_test_conn.commit()

    row = get_manifest_row(ebull_test_conn, "0002080126-26-000003")
    assert row is not None
    assert row.ingest_status == "tombstoned"
    assert row.raw_status == "stored"
    with ebull_test_conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM filing_raw_documents "
            "WHERE accession_number = '0002080126-26-000003' AND document_kind = 'prospectus_body'"
        )
        raw_count_row = cur.fetchone()
    assert raw_count_row is not None and raw_count_row[0] == 1


def test_fetch_exception_marks_failed(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import app.services.manifest_parsers  # noqa: F401 — register

    _seed_instrument(ebull_test_conn, iid=9160004, symbol="FLAKY")
    _seed_pending_424b(ebull_test_conn, accession="0002080126-26-000004", instrument_id=9160004)
    ebull_test_conn.commit()

    from app.providers.implementations import sec_edgar

    def _boom(self, url):  # noqa: ANN001, ANN201
        raise ConnectionError("transient")

    monkeypatch.setattr(sec_edgar.SecFilingsProvider, "fetch_document_text", _boom)

    run_manifest_worker(ebull_test_conn, source="sec_424b", max_rows=10)
    ebull_test_conn.commit()

    row = get_manifest_row(ebull_test_conn, "0002080126-26-000004")
    assert row is not None
    assert row.ingest_status == "failed"
    assert row.next_retry_at is not None
