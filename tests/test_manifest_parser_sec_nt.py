"""Tests for the NT 10-K / NT 10-Q manifest-worker parser adapter (#1015).

Covers:
- Happy path: HTML fetch → store_raw (nt_body, RETAINED) → parse → upsert
  nt_filing_notices → ParseOutcome(parsed, raw_status=stored).
- Tombstone: fetch returns empty body.
- Tombstone with raw stored: body is not a recognizable Form 12b-25.
- Failure: fetch raises (transient — worker retries).

Fetch is monkeypatched at ``SecFilingsProvider.fetch_document_text`` so tests
run without touching SEC.
"""

from __future__ import annotations

from datetime import UTC, date, datetime

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


def _seed_pending_nt(
    conn: psycopg.Connection[tuple],
    *,
    accession: str,
    instrument_id: int,
    form: str = "NT 10-Q",
    url: str = "https://www.sec.gov/Archives/edgar/data/1505952/000162828026042197/nt10-q.htm",
    cik: str = "0001505952",
) -> None:
    record_manifest_entry(
        conn,
        accession,
        cik=cik,
        form=form,
        source="sec_nt",
        subject_type="issuer",
        subject_id=str(instrument_id),
        instrument_id=instrument_id,
        filed_at=datetime(2026, 6, 10, tzinfo=UTC),
        primary_document_url=url,
    )


# Minimal Form 12b-25 NT 10-Q the extractor accepts.
_FAKE_NT_HTML = """
<html><body>
UNITED STATES SECURITIES AND EXCHANGE COMMISSION FORM 12b-25
NOTIFICATION OF LATE FILING For Period Ended: April 30, 2026
PART I REGISTRANT INFORMATION Acme Corp
PART II RULES 12b-25(b) AND (c)
PART III NARRATIVE State below in reasonable detail why the report could not be
filed within the prescribed time period. Acme needs additional time to complete
its financial statements.
PART IV OTHER INFORMATION
(3) Is it anticipated that any significant change in results of operations from
the corresponding period for the last fiscal year will be reflected?
&#9744; Yes &#9746; No
If so, attach an explanation of the anticipated change.
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
    import app.services.manifest_parsers  # noqa: F401 — register

    _seed_instrument(ebull_test_conn, iid=9150001, symbol="ACME")
    _seed_pending_nt(ebull_test_conn, accession="0001505952-26-000001", instrument_id=9150001)
    ebull_test_conn.commit()

    from app.providers.implementations import sec_edgar

    monkeypatch.setattr(
        sec_edgar.SecFilingsProvider,
        "fetch_document_text",
        lambda self, url: _FAKE_NT_HTML,
    )

    stats = run_manifest_worker(ebull_test_conn, source="sec_nt", max_rows=10)
    ebull_test_conn.commit()

    assert stats.parsed == 1
    assert stats.skipped_no_parser == 0

    row = get_manifest_row(ebull_test_conn, "0001505952-26-000001")
    assert row is not None
    assert row.ingest_status == "parsed"
    assert row.raw_status == "stored"

    with ebull_test_conn.cursor() as cur:
        cur.execute(
            """
            SELECT late_form, period_of_report, grace_period_days,
                   results_change_anticipated, reason_text
            FROM nt_filing_notices WHERE accession_number = '0001505952-26-000001'
            """
        )
        nt = cur.fetchone()
    assert nt is not None
    late_form, period, grace, results_change, reason = nt
    assert late_form == "10-Q"
    assert period == date(2026, 4, 30)
    assert grace == 5
    assert results_change is False
    assert reason is not None and "additional time" in reason.lower()

    # nt_body is RETAINED (not swept like 8-K primary_doc): payload present.
    with ebull_test_conn.cursor() as cur:
        cur.execute(
            """
            SELECT payload, byte_count FROM filing_raw_documents
            WHERE accession_number = '0001505952-26-000001' AND document_kind = 'nt_body'
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
    import app.services.manifest_parsers  # noqa: F401 — register

    _seed_instrument(ebull_test_conn, iid=9150002, symbol="DEAD")
    _seed_pending_nt(ebull_test_conn, accession="0001505952-26-000002", instrument_id=9150002)
    ebull_test_conn.commit()

    from app.providers.implementations import sec_edgar

    monkeypatch.setattr(sec_edgar.SecFilingsProvider, "fetch_document_text", lambda self, url: "")

    run_manifest_worker(ebull_test_conn, source="sec_nt", max_rows=10)
    ebull_test_conn.commit()

    row = get_manifest_row(ebull_test_conn, "0001505952-26-000002")
    assert row is not None
    assert row.ingest_status == "tombstoned"
    with ebull_test_conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM nt_filing_notices WHERE accession_number = '0001505952-26-000002'")
        count_row = cur.fetchone()
    assert count_row is not None and count_row[0] == 0


def test_unrecognized_body_tombstones_with_raw_stored(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A non-Form-12b-25 body → tombstone, but raw IS stored (#938 invariant)."""
    import app.services.manifest_parsers  # noqa: F401 — register

    _seed_instrument(ebull_test_conn, iid=9150003, symbol="WEIRD")
    _seed_pending_nt(ebull_test_conn, accession="0001505952-26-000003", instrument_id=9150003)
    ebull_test_conn.commit()

    from app.providers.implementations import sec_edgar

    monkeypatch.setattr(
        sec_edgar.SecFilingsProvider,
        "fetch_document_text",
        lambda self, url: "<html><body>not a late-filing notice at all</body></html>",
    )

    run_manifest_worker(ebull_test_conn, source="sec_nt", max_rows=10)
    ebull_test_conn.commit()

    row = get_manifest_row(ebull_test_conn, "0001505952-26-000003")
    assert row is not None
    assert row.ingest_status == "tombstoned"
    assert row.raw_status == "stored"
    with ebull_test_conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM filing_raw_documents "
            "WHERE accession_number = '0001505952-26-000003' AND document_kind = 'nt_body'"
        )
        raw_count_row = cur.fetchone()
    assert raw_count_row is not None and raw_count_row[0] == 1


def test_fetch_exception_marks_failed(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import app.services.manifest_parsers  # noqa: F401 — register

    _seed_instrument(ebull_test_conn, iid=9150004, symbol="FLAKY")
    _seed_pending_nt(ebull_test_conn, accession="0001505952-26-000004", instrument_id=9150004)
    ebull_test_conn.commit()

    from app.providers.implementations import sec_edgar

    def _boom(self, url):  # noqa: ANN001, ANN201
        raise ConnectionError("transient")

    monkeypatch.setattr(sec_edgar.SecFilingsProvider, "fetch_document_text", _boom)

    run_manifest_worker(ebull_test_conn, source="sec_nt", max_rows=10)
    ebull_test_conn.commit()

    row = get_manifest_row(ebull_test_conn, "0001505952-26-000004")
    assert row is not None
    assert row.ingest_status == "failed"
    assert row.next_retry_at is not None
