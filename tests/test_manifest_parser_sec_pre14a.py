"""Tests for the PRE 14A / PRER14A manifest-worker parser adapter (#1892).

Covers:
- Happy path: HTML fetch -> store_raw (pre14a_body, RETAINED) -> parse ->
  upsert pre14a_proposal_signals -> ParseOutcome(parsed, raw_status=stored).
- Tombstone: fetch returns empty body.
- Tombstone with raw stored: body has no recognizable numbered proposals list.
- Failure: fetch raises (transient -- worker retries).

Fetch is monkeypatched at ``SecFilingsProvider.fetch_document_text`` so tests
run without touching SEC.
"""

from __future__ import annotations

from datetime import UTC, datetime

import psycopg
import pytest

from app.jobs.sec_manifest_worker import clear_registered_parsers, run_manifest_worker
from app.services.sec_manifest import get_manifest_row, record_manifest_entry
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 -- fixture re-export


def _seed_instrument(conn: psycopg.Connection[tuple], iid: int, symbol: str) -> None:
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, is_tradable)
        VALUES (%s, %s, %s, '4', 'USD', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        (iid, symbol, f"{symbol} co"),
    )


def _seed_pending_pre14a(
    conn: psycopg.Connection[tuple],
    *,
    accession: str,
    instrument_id: int,
    form: str = "PRE 14A",
    url: str = "https://www.sec.gov/Archives/edgar/data/1805521/000121390026074960/pre14a.htm",
    cik: str = "0001805521",
) -> None:
    record_manifest_entry(
        conn,
        accession,
        cik=cik,
        form=form,
        source="sec_pre14a",
        subject_type="issuer",
        subject_id=str(instrument_id),
        instrument_id=instrument_id,
        filed_at=datetime(2026, 7, 2, tzinfo=UTC),
        primary_document_url=url,
    )


# Minimal PRE 14A the extractor accepts (Rule 14a-4(a)(3) numbered agenda).
_FAKE_PRE14A_HTML = """
<html><body>
NOTICE OF ANNUAL MEETING OF STOCKHOLDERS
The meeting will be held for the purpose of considering and voting on the
following proposals:
1. To elect two directors to the Board.
2. To approve an amendment to increase the number of authorized shares of
common stock from 100,000,000 to 500,000,000.
3. To approve, on a non-binding advisory basis, executive compensation.
An advisory vote to approve executive compensation.
Each Proposal is more fully described in the accompanying proxy statement.
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


def test_happy_path_parses_and_retains_raw(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import app.services.manifest_parsers  # noqa: F401 -- register

    _seed_instrument(ebull_test_conn, iid=9160001, symbol="AGND")
    _seed_pending_pre14a(ebull_test_conn, accession="0001805521-26-000001", instrument_id=9160001)
    ebull_test_conn.commit()

    from app.providers.implementations import sec_edgar

    monkeypatch.setattr(
        sec_edgar.SecFilingsProvider,
        "fetch_document_text",
        lambda self, url: _FAKE_PRE14A_HTML,
    )

    stats = run_manifest_worker(ebull_test_conn, source="sec_pre14a", max_rows=10)
    ebull_test_conn.commit()

    assert stats.parsed == 1
    assert stats.skipped_no_parser == 0

    row = get_manifest_row(ebull_test_conn, "0001805521-26-000001")
    assert row is not None
    assert row.ingest_status == "parsed"
    assert row.raw_status == "stored"

    with ebull_test_conn.cursor() as cur:
        cur.execute(
            """
            SELECT proposal_count, reverse_stock_split_proposal,
                   authorized_share_increase_proposal, say_on_pay_advisory_vote,
                   agenda_items
            FROM pre14a_proposal_signals WHERE accession_number = '0001805521-26-000001'
            """
        )
        parsed = cur.fetchone()
    assert parsed is not None
    count, reverse_split, share_increase, say_on_pay, agenda_items = parsed
    assert count == 3
    assert reverse_split is False
    assert share_increase is True
    assert say_on_pay is True
    assert len(agenda_items) == 3

    # pre14a_body is RETAINED (not swept): payload present.
    with ebull_test_conn.cursor() as cur:
        cur.execute(
            """
            SELECT payload, byte_count FROM filing_raw_documents
            WHERE accession_number = '0001805521-26-000001' AND document_kind = 'pre14a_body'
            """
        )
        raw = cur.fetchone()
    assert raw is not None
    payload, byte_count = raw
    assert payload is not None and byte_count is not None and byte_count > 0


def test_empty_fetch_tombstones(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import app.services.manifest_parsers  # noqa: F401 -- register

    _seed_instrument(ebull_test_conn, iid=9160002, symbol="DEAD2")
    _seed_pending_pre14a(ebull_test_conn, accession="0001805521-26-000002", instrument_id=9160002)
    ebull_test_conn.commit()

    from app.providers.implementations import sec_edgar

    monkeypatch.setattr(sec_edgar.SecFilingsProvider, "fetch_document_text", lambda self, url: "")

    run_manifest_worker(ebull_test_conn, source="sec_pre14a", max_rows=10)
    ebull_test_conn.commit()

    row = get_manifest_row(ebull_test_conn, "0001805521-26-000002")
    assert row is not None
    assert row.ingest_status == "tombstoned"
    with ebull_test_conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM pre14a_proposal_signals WHERE accession_number = '0001805521-26-000002'")
        count_row = cur.fetchone()
    assert count_row is not None and count_row[0] == 0


def test_unrecognized_body_tombstones_with_raw_stored(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A body with no numbered proposals list -> tombstone, but raw IS
    stored (#938 invariant)."""
    import app.services.manifest_parsers  # noqa: F401 -- register

    _seed_instrument(ebull_test_conn, iid=9160003, symbol="WEIRD2")
    _seed_pending_pre14a(ebull_test_conn, accession="0001805521-26-000003", instrument_id=9160003)
    ebull_test_conn.commit()

    from app.providers.implementations import sec_edgar

    monkeypatch.setattr(
        sec_edgar.SecFilingsProvider,
        "fetch_document_text",
        lambda self, url: "<html><body>not a proxy notice at all</body></html>",
    )

    run_manifest_worker(ebull_test_conn, source="sec_pre14a", max_rows=10)
    ebull_test_conn.commit()

    row = get_manifest_row(ebull_test_conn, "0001805521-26-000003")
    assert row is not None
    assert row.ingest_status == "tombstoned"
    assert row.raw_status == "stored"
    with ebull_test_conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM filing_raw_documents "
            "WHERE accession_number = '0001805521-26-000003' AND document_kind = 'pre14a_body'"
        )
        raw_count_row = cur.fetchone()
    assert raw_count_row is not None and raw_count_row[0] == 1


def test_fetch_exception_marks_failed(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import app.services.manifest_parsers  # noqa: F401 -- register

    _seed_instrument(ebull_test_conn, iid=9160004, symbol="FLAKY2")
    _seed_pending_pre14a(ebull_test_conn, accession="0001805521-26-000004", instrument_id=9160004)
    ebull_test_conn.commit()

    from app.providers.implementations import sec_edgar

    def _boom(self, url):  # noqa: ANN001, ANN201
        raise ConnectionError("transient")

    monkeypatch.setattr(sec_edgar.SecFilingsProvider, "fetch_document_text", _boom)

    run_manifest_worker(ebull_test_conn, source="sec_pre14a", max_rows=10)
    ebull_test_conn.commit()

    row = get_manifest_row(ebull_test_conn, "0001805521-26-000004")
    assert row is not None
    assert row.ingest_status == "failed"
    assert row.next_retry_at is not None
