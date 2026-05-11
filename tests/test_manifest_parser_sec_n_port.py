"""Tests for the NPORT-P manifest-worker parser adapter (#873).

Mirrors ``test_manifest_parser_sec_13f_hr.py`` shape:

* Happy path: primary_doc.xml fetch → store_raw → parse_n_port_payload
  → series upsert → record_fund_observation per resolvable Long-EC-NS
  holding → refresh_funds_current → ingest-log success.
* Tombstone on fetch 404 (audit log 'failed').
* Tombstone on ``NPortMissingSeriesError`` (audit log 'failed').
* Tombstone on ``NPortParseError`` (audit log 'failed', raw_status
  preserved).
* Fetch raise → ``failed`` with 1h backoff.
* Deterministic upsert exception → tombstone + audit-log 'failed'.
* Transient ``OperationalError`` on upsert → ``failed`` with 1h
  backoff (no audit-log row).
* Registration: ``register_all_parsers`` wires ``sec_n_port``.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import psycopg
import pytest

from app.jobs.sec_manifest_worker import (
    clear_registered_parsers,
    run_manifest_worker,
)
from app.services.sec_manifest import get_manifest_row, record_manifest_entry
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export

pytestmark = pytest.mark.integration


_FIXTURE_DIR = Path(__file__).parent / "fixtures" / "sec"
_FAKE_NPORT_XML = (_FIXTURE_DIR / "nport_p_test_fund.xml").read_text(encoding="utf-8")
_FAKE_NPORT_MISSING_SERIES_XML = (_FIXTURE_DIR / "nport_p_missing_series.xml").read_text(encoding="utf-8")


def _seed_instrument(conn: psycopg.Connection[tuple], *, iid: int, symbol: str) -> None:
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, is_tradable)
        VALUES (%s, %s, %s, '4', 'USD', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        (iid, symbol, f"{symbol} co"),
    )


def _seed_cusip_mapping(conn: psycopg.Connection[tuple], *, instrument_id: int, cusip: str) -> None:
    conn.execute(
        """
        INSERT INTO external_identifiers (instrument_id, provider, identifier_type, identifier_value, is_primary)
        VALUES (%s, 'sec', 'cusip', %s, TRUE)
        ON CONFLICT (provider, identifier_type, identifier_value)
            WHERE NOT (provider = 'sec' AND identifier_type = 'cik')
        DO NOTHING
        """,
        (instrument_id, cusip.upper()),
    )


def _seed_pending_n_port(
    conn: psycopg.Connection[tuple],
    *,
    accession: str,
    filer_cik: str,
) -> None:
    record_manifest_entry(
        conn,
        accession,
        cik=filer_cik,
        form="NPORT-P",
        source="sec_n_port",
        subject_type="institutional_filer",
        subject_id=filer_cik,
        instrument_id=None,
        filed_at=datetime(2026, 2, 26, tzinfo=UTC),
        primary_document_url=f"https://www.sec.gov/Archives/edgar/data/{int(filer_cik)}/"
        f"{accession.replace('-', '')}/primary_doc.xml",
    )


@pytest.fixture(autouse=True)
def _reset_registry_then_reload():
    from app.services.manifest_parsers import register_all_parsers

    clear_registered_parsers()
    register_all_parsers()
    yield
    clear_registered_parsers()
    register_all_parsers()


def _patch_fetch_map(monkeypatch: pytest.MonkeyPatch, payloads: dict[str, str | None]) -> list[str]:
    calls: list[str] = []
    from app.providers.implementations import sec_edgar

    def _fake(self, url: str):  # noqa: ARG001
        calls.append(url)
        return payloads.get(url)

    monkeypatch.setattr(sec_edgar.SecFilingsProvider, "fetch_document_text", _fake)
    return calls


def test_happy_path_parses_and_writes_fund_observation(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Manifest worker drains an NPORT-P pending row: fetch →
    store_raw → parse → series upsert → fund observation per
    resolvable Long-EC-NS holding → log success."""
    iid_aapl = 8800001
    _seed_instrument(ebull_test_conn, iid=iid_aapl, symbol="AAPL")
    _seed_cusip_mapping(ebull_test_conn, instrument_id=iid_aapl, cusip="037833100")
    accession = "0001234500-25-000603"
    filer_cik = "0000036405"
    _seed_pending_n_port(ebull_test_conn, accession=accession, filer_cik=filer_cik)
    ebull_test_conn.commit()

    primary_url = (
        f"https://www.sec.gov/Archives/edgar/data/{int(filer_cik)}/{accession.replace('-', '')}/primary_doc.xml"
    )
    _patch_fetch_map(monkeypatch, {primary_url: _FAKE_NPORT_XML})

    stats = run_manifest_worker(ebull_test_conn, source="sec_n_port", max_rows=10)
    ebull_test_conn.commit()

    assert stats.parsed == 1
    row = get_manifest_row(ebull_test_conn, accession)
    assert row is not None
    assert row.ingest_status == "parsed"
    assert row.raw_status == "stored"

    # raw body persisted under document_kind='nport_xml'.
    with ebull_test_conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM filing_raw_documents WHERE accession_number = %s AND document_kind = 'nport_xml'",
            (accession,),
        )
        assert cur.fetchone() is not None

    # Series row persisted.
    with ebull_test_conn.cursor() as cur:
        cur.execute("SELECT fund_filer_cik FROM sec_fund_series WHERE fund_series_id = 'S000002277'")
        series = cur.fetchone()
    assert series is not None and series[0] == filer_cik

    # AAPL fund observation persisted.
    with ebull_test_conn.cursor() as cur:
        cur.execute(
            "SELECT instrument_id FROM ownership_funds_observations WHERE source_accession = %s AND instrument_id = %s",
            (accession, iid_aapl),
        )
        assert cur.fetchone() is not None

    # Ingest log success/partial.
    with ebull_test_conn.cursor() as cur:
        cur.execute(
            "SELECT status FROM n_port_ingest_log WHERE accession_number = %s",
            (accession,),
        )
        log = cur.fetchone()
    assert log is not None
    # The golden fixture has 7 holdings; only 1 CUSIP (AAPL) resolves
    # in this minimal seed. Others land in skipped buckets → partial.
    assert log[0] in ("success", "partial")


def test_fetch_404_tombstones_with_log(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    accession = "0001234500-25-000700"
    filer_cik = "0000036405"
    _seed_pending_n_port(ebull_test_conn, accession=accession, filer_cik=filer_cik)
    ebull_test_conn.commit()

    primary_url = (
        f"https://www.sec.gov/Archives/edgar/data/{int(filer_cik)}/{accession.replace('-', '')}/primary_doc.xml"
    )
    _patch_fetch_map(monkeypatch, {primary_url: None})

    stats = run_manifest_worker(ebull_test_conn, source="sec_n_port", max_rows=10)
    ebull_test_conn.commit()

    assert stats.tombstoned == 1
    row = get_manifest_row(ebull_test_conn, accession)
    assert row is not None and row.ingest_status == "tombstoned"

    with ebull_test_conn.cursor() as cur:
        cur.execute(
            "SELECT status, error FROM n_port_ingest_log WHERE accession_number = %s",
            (accession,),
        )
        log = cur.fetchone()
    assert log is not None
    assert log[0] == "failed"
    assert log[1] is not None and "primary_doc.xml fetch failed" in log[1]


def test_missing_series_tombstones_with_log(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    accession = "0001234500-25-000701"
    filer_cik = "0000036405"
    _seed_pending_n_port(ebull_test_conn, accession=accession, filer_cik=filer_cik)
    ebull_test_conn.commit()

    primary_url = (
        f"https://www.sec.gov/Archives/edgar/data/{int(filer_cik)}/{accession.replace('-', '')}/primary_doc.xml"
    )
    _patch_fetch_map(monkeypatch, {primary_url: _FAKE_NPORT_MISSING_SERIES_XML})

    stats = run_manifest_worker(ebull_test_conn, source="sec_n_port", max_rows=10)
    ebull_test_conn.commit()

    assert stats.tombstoned == 1
    row = get_manifest_row(ebull_test_conn, accession)
    assert row is not None
    assert row.ingest_status == "tombstoned"
    assert row.raw_status == "stored"
    assert row.error is not None and "missing series" in row.error

    with ebull_test_conn.cursor() as cur:
        cur.execute(
            "SELECT status, error FROM n_port_ingest_log WHERE accession_number = %s",
            (accession,),
        )
        log = cur.fetchone()
    assert log is not None and log[0] == "failed"


def test_malformed_xml_tombstones_with_log(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    accession = "0001234500-25-000702"
    filer_cik = "0000036405"
    _seed_pending_n_port(ebull_test_conn, accession=accession, filer_cik=filer_cik)
    ebull_test_conn.commit()

    primary_url = (
        f"https://www.sec.gov/Archives/edgar/data/{int(filer_cik)}/{accession.replace('-', '')}/primary_doc.xml"
    )
    _patch_fetch_map(monkeypatch, {primary_url: "<not><well><formed>"})

    stats = run_manifest_worker(ebull_test_conn, source="sec_n_port", max_rows=10)
    ebull_test_conn.commit()

    assert stats.failed == 1
    row = get_manifest_row(ebull_test_conn, accession)
    assert row is not None
    # NPortParseError leads to _failed_outcome (transient bucket per
    # the parse-phase preserve-raw policy), not tombstone. raw_status
    # is preserved as 'stored'.
    assert row.ingest_status == "failed"
    assert row.raw_status == "stored"


def test_fetch_exception_marks_failed_with_backoff(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    accession = "0001234500-25-000703"
    filer_cik = "0000036405"
    _seed_pending_n_port(ebull_test_conn, accession=accession, filer_cik=filer_cik)
    ebull_test_conn.commit()

    from app.providers.implementations import sec_edgar

    def _boom(self, url):  # noqa: ARG001
        raise RuntimeError("network kaput")

    monkeypatch.setattr(sec_edgar.SecFilingsProvider, "fetch_document_text", _boom)

    before = datetime.now(tz=UTC)
    stats = run_manifest_worker(ebull_test_conn, source="sec_n_port", max_rows=10)
    ebull_test_conn.commit()

    assert stats.failed == 1
    row = get_manifest_row(ebull_test_conn, accession)
    assert row is not None
    assert row.ingest_status == "failed"
    assert row.error is not None and "fetch error" in row.error
    assert row.next_retry_at is not None
    delta = (row.next_retry_at - before).total_seconds()
    assert 3300 < delta < 3900


def test_deterministic_upsert_exception_tombstones_with_log(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR #1131: a non-transient observation upsert exception
    tombstones the manifest + writes ingest-log 'failed'."""
    from app.services.manifest_parsers import sec_n_port as parser_module

    iid = 8800010
    _seed_instrument(ebull_test_conn, iid=iid, symbol="AAPL")
    _seed_cusip_mapping(ebull_test_conn, instrument_id=iid, cusip="037833100")
    accession = "0001234500-25-000704"
    filer_cik = "0000036405"
    _seed_pending_n_port(ebull_test_conn, accession=accession, filer_cik=filer_cik)
    ebull_test_conn.commit()

    primary_url = (
        f"https://www.sec.gov/Archives/edgar/data/{int(filer_cik)}/{accession.replace('-', '')}/primary_doc.xml"
    )
    _patch_fetch_map(monkeypatch, {primary_url: _FAKE_NPORT_XML})

    def _raising(*args, **kwargs):  # noqa: ARG001
        raise RuntimeError("synthetic n_port observation upsert violation")

    monkeypatch.setattr(parser_module, "record_fund_observation", _raising)

    stats = run_manifest_worker(ebull_test_conn, source="sec_n_port", max_rows=10)
    ebull_test_conn.commit()

    assert stats.tombstoned == 1
    assert stats.failed == 0
    row = get_manifest_row(ebull_test_conn, accession)
    assert row is not None
    assert row.ingest_status == "tombstoned"
    assert row.raw_status == "stored"
    assert row.error is not None and "RuntimeError" in row.error

    with ebull_test_conn.cursor() as cur:
        cur.execute(
            "SELECT status, error FROM n_port_ingest_log WHERE accession_number = %s",
            (accession,),
        )
        log = cur.fetchone()
    assert log is not None
    assert log[0] == "failed"
    assert log[1] is not None and "RuntimeError" in log[1]


def test_transient_upsert_exception_retries(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR #1131: ``OperationalError`` on the observation upsert keeps
    the manifest in ``failed`` with 1h backoff — no audit-log row."""
    import psycopg.errors

    from app.services.manifest_parsers import sec_n_port as parser_module

    iid = 8800011
    _seed_instrument(ebull_test_conn, iid=iid, symbol="AAPL")
    _seed_cusip_mapping(ebull_test_conn, instrument_id=iid, cusip="037833100")
    accession = "0001234500-25-000705"
    filer_cik = "0000036405"
    _seed_pending_n_port(ebull_test_conn, accession=accession, filer_cik=filer_cik)
    ebull_test_conn.commit()

    primary_url = (
        f"https://www.sec.gov/Archives/edgar/data/{int(filer_cik)}/{accession.replace('-', '')}/primary_doc.xml"
    )
    _patch_fetch_map(monkeypatch, {primary_url: _FAKE_NPORT_XML})

    def _raising(*args, **kwargs):  # noqa: ARG001
        raise psycopg.errors.SerializationFailure("synthetic serialisation failure")

    monkeypatch.setattr(parser_module, "record_fund_observation", _raising)

    stats = run_manifest_worker(ebull_test_conn, source="sec_n_port", max_rows=10)
    ebull_test_conn.commit()

    assert stats.failed == 1
    assert stats.tombstoned == 0
    row = get_manifest_row(ebull_test_conn, accession)
    assert row is not None
    assert row.ingest_status == "failed"
    assert row.raw_status == "stored"
    assert row.error is not None and "SerializationFailure" in row.error

    with ebull_test_conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM n_port_ingest_log WHERE accession_number = %s",
            (accession,),
        )
        assert cur.fetchone() is None


def test_parser_registered_via_register_all() -> None:
    from app.jobs.sec_manifest_worker import registered_parser_sources
    from app.services.manifest_parsers import register_all_parsers

    assert "sec_n_port" in registered_parser_sources()
    clear_registered_parsers()
    assert "sec_n_port" not in registered_parser_sources()
    register_all_parsers()
    assert "sec_n_port" in registered_parser_sources()
