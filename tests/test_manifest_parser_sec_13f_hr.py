"""Tests for the 13F-HR manifest-worker parser adapter (#873).

The bulk path (``app/services/sec_13f_dataset_ingest.py``) handles
quarterly archive drains; this thin manifest adapter handles
atom-discovered freshness one accession at a time. Tests cover:

* Happy path: index.json walk → primary_doc.xml + infotable.xml
  fetch → store_raw twice → parse → upsert filer + holding +
  observations → ingest-log success.
* CUSIP unresolved: holding rows skipped, log status='partial'.
* Tombstone on index.json 404 / empty body.
* Tombstone on archive index missing primary_doc OR infotable.
* Tombstone on primary_doc.xml or infotable.xml empty body.
* Fetch raise → ``failed`` with 1h backoff.
* Parse-phase exception preserves ``raw_status='stored'``.
* Deterministic upsert exception tombstones the manifest + writes
  ingest-log 'failed'.
* Transient psycopg ``OperationalError`` on upsert retries via 1h
  backoff (no ingest-log row written so the retry sees a clean slate).
* Registration: ``register_all_parsers`` wires ``sec_13f_hr``.

The fetch boundary is monkeypatched at
``SecFilingsProvider.fetch_document_text`` so tests run without
touching SEC.
"""

from __future__ import annotations

import json
from collections.abc import Iterable
from datetime import UTC, datetime

import psycopg
import pytest

from app.jobs.sec_manifest_worker import (
    clear_registered_parsers,
    run_manifest_worker,
)
from app.services.sec_manifest import get_manifest_row, record_manifest_entry
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export

pytestmark = pytest.mark.integration


_FAKE_PRIMARY_XML = """<?xml version="1.0" encoding="UTF-8"?>
<edgarSubmission xmlns="http://www.sec.gov/edgar/thirteenffiler">
  <headerData>
    <filerInfo>
      <filer>
        <credentials>
          <cik>0001067983</cik>
        </credentials>
      </filer>
      <periodOfReport>09-30-2024</periodOfReport>
    </filerInfo>
  </headerData>
  <formData>
    <coverPage>
      <reportCalendarOrQuarter>09-30-2024</reportCalendarOrQuarter>
      <filingManager>
        <name>BERKSHIRE HATHAWAY INC</name>
        <address>
          <street1>3555 Farnam Street</street1>
          <city>Omaha</city>
          <stateOrCountry>NE</stateOrCountry>
          <zipCode>68131</zipCode>
        </address>
      </filingManager>
    </coverPage>
    <signatureBlock>
      <signatureDate>11-14-2024</signatureDate>
    </signatureBlock>
  </formData>
</edgarSubmission>
"""


def _infotable_xml(*, holdings: Iterable[dict[str, str]]) -> str:
    rows: list[str] = []
    for h in holdings:
        rows.append(
            f"""<infoTable>
  <nameOfIssuer>{h.get("name", "APPLE INC")}</nameOfIssuer>
  <titleOfClass>COM</titleOfClass>
  <cusip>{h["cusip"]}</cusip>
  <value>{h.get("value", "69900000")}</value>
  <shrsOrPrnAmt>
    <sshPrnamt>{h.get("shares", "300000000")}</sshPrnamt>
    <sshPrnamtType>{h.get("amt_type", "SH")}</sshPrnamtType>
  </shrsOrPrnAmt>
  <investmentDiscretion>SOLE</investmentDiscretion>
  <votingAuthority>
    <Sole>{h.get("sole", "300000000")}</Sole>
    <Shared>0</Shared>
    <None>0</None>
  </votingAuthority>
</infoTable>"""
        )
    body = "\n  ".join(rows)
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<informationTable xmlns="http://www.sec.gov/edgar/document/thirteenf/informationtable">
  {body}
</informationTable>
"""


def _index_json(*, primary: str | None = "primary_doc.xml", infotable: str | None = "infotable.xml") -> str:
    items: list[dict[str, str]] = []
    if primary:
        items.append({"name": primary, "type": "text", "size": "1234"})
    if infotable:
        items.append({"name": infotable, "type": "text", "size": "5678"})
    items.append({"name": "filing-index-headers.html", "type": "text", "size": "100"})
    return json.dumps({"directory": {"name": "/Archives/...", "item": items}})


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


def _seed_pending_13f_hr(
    conn: psycopg.Connection[tuple],
    *,
    accession: str,
    filer_cik: str,
) -> None:
    """Seed a pending manifest row. 13F-HR is filer-scoped — no
    instrument_id (issuer linkage resolves per-row by CUSIP)."""
    record_manifest_entry(
        conn,
        accession,
        cik=filer_cik,
        form="13F-HR",
        source="sec_13f_hr",
        subject_type="institutional_filer",
        subject_id=filer_cik,
        instrument_id=None,
        filed_at=datetime(2024, 11, 14, tzinfo=UTC),
        primary_document_url=f"https://www.sec.gov/Archives/edgar/data/{int(filer_cik)}/"
        f"{accession.replace('-', '')}/{accession}-index.htm",
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
    """Monkeypatch ``SecFilingsProvider.fetch_document_text`` to return
    payloads keyed by URL. Returns the URL-call list for assertions."""
    calls: list[str] = []

    from app.providers.implementations import sec_edgar

    def _fake(self, url: str):  # noqa: ARG001
        calls.append(url)
        return payloads.get(url)

    monkeypatch.setattr(sec_edgar.SecFilingsProvider, "fetch_document_text", _fake)
    return calls


def test_happy_path_parses_both_xmls_and_writes_observations(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Manifest worker drains a 13F-HR row end-to-end: index → primary
    → infotable → upsert filer + holding + observation → log success."""
    iid = 8790001
    cusip = "037833100"  # AAPL
    _seed_instrument(ebull_test_conn, iid=iid, symbol="AAPL")
    _seed_cusip_mapping(ebull_test_conn, instrument_id=iid, cusip=cusip)
    accession = "0001067983-25-000001"
    filer_cik = "0001067983"
    _seed_pending_13f_hr(ebull_test_conn, accession=accession, filer_cik=filer_cik)
    ebull_test_conn.commit()

    base = f"https://www.sec.gov/Archives/edgar/data/{int(filer_cik)}/{accession.replace('-', '')}/"
    _patch_fetch_map(
        monkeypatch,
        {
            base + "index.json": _index_json(),
            base + "primary_doc.xml": _FAKE_PRIMARY_XML,
            base + "infotable.xml": _infotable_xml(holdings=[{"cusip": cusip}]),
        },
    )

    stats = run_manifest_worker(ebull_test_conn, source="sec_13f_hr", max_rows=10)
    ebull_test_conn.commit()

    assert stats.parsed == 1
    row = get_manifest_row(ebull_test_conn, accession)
    assert row is not None
    assert row.ingest_status == "parsed"
    assert row.raw_status == "stored"

    # Both raw bodies persisted.
    with ebull_test_conn.cursor() as cur:
        cur.execute(
            "SELECT document_kind FROM filing_raw_documents WHERE accession_number = %s ORDER BY document_kind",
            (accession,),
        )
        kinds = [r[0] for r in cur.fetchall()]
    assert "primary_doc" in kinds
    assert "infotable_13f" in kinds

    # Holding row persisted.
    with ebull_test_conn.cursor() as cur:
        cur.execute(
            "SELECT instrument_id FROM institutional_holdings WHERE accession_number = %s",
            (accession,),
        )
        holdings = cur.fetchall()
    assert len(holdings) == 1
    assert holdings[0][0] == iid

    # Audit log success.
    with ebull_test_conn.cursor() as cur:
        cur.execute(
            "SELECT status, holdings_inserted FROM institutional_holdings_ingest_log WHERE accession_number = %s",
            (accession,),
        )
        log = cur.fetchone()
    assert log is not None
    assert log[0] == "success"
    assert log[1] == 1


def test_unresolved_cusip_logs_partial(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No CUSIP→instrument mapping → holding row skipped, log
    status='partial', tracking row in unresolved_13f_cusips."""
    accession = "0001067983-25-000002"
    filer_cik = "0001067983"
    _seed_pending_13f_hr(ebull_test_conn, accession=accession, filer_cik=filer_cik)
    ebull_test_conn.commit()

    base = f"https://www.sec.gov/Archives/edgar/data/{int(filer_cik)}/{accession.replace('-', '')}/"
    _patch_fetch_map(
        monkeypatch,
        {
            base + "index.json": _index_json(),
            base + "primary_doc.xml": _FAKE_PRIMARY_XML,
            base + "infotable.xml": _infotable_xml(holdings=[{"cusip": "999999999"}]),
        },
    )

    stats = run_manifest_worker(ebull_test_conn, source="sec_13f_hr", max_rows=10)
    ebull_test_conn.commit()

    assert stats.parsed == 1
    row = get_manifest_row(ebull_test_conn, accession)
    assert row is not None and row.ingest_status == "parsed"

    with ebull_test_conn.cursor() as cur:
        cur.execute(
            "SELECT status, holdings_inserted, holdings_skipped FROM institutional_holdings_ingest_log "
            "WHERE accession_number = %s",
            (accession,),
        )
        log = cur.fetchone()
    assert log is not None
    assert log[0] == "partial"
    assert log[1] == 0
    assert log[2] == 1


def test_index_404_tombstones_with_log(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    accession = "0001067983-25-000003"
    filer_cik = "0001067983"
    _seed_pending_13f_hr(ebull_test_conn, accession=accession, filer_cik=filer_cik)
    ebull_test_conn.commit()

    base = f"https://www.sec.gov/Archives/edgar/data/{int(filer_cik)}/{accession.replace('-', '')}/"
    _patch_fetch_map(monkeypatch, {base + "index.json": None})

    stats = run_manifest_worker(ebull_test_conn, source="sec_13f_hr", max_rows=10)
    ebull_test_conn.commit()

    assert stats.tombstoned == 1
    row = get_manifest_row(ebull_test_conn, accession)
    assert row is not None and row.ingest_status == "tombstoned"

    with ebull_test_conn.cursor() as cur:
        cur.execute(
            "SELECT status FROM institutional_holdings_ingest_log WHERE accession_number = %s",
            (accession,),
        )
        log = cur.fetchone()
    assert log is not None and log[0] == "failed"


def test_index_missing_primary_doc_tombstones(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Index found but primary_doc.xml entry absent — deterministic
    gap; tombstone the manifest + log 'failed'."""
    accession = "0001067983-25-000004"
    filer_cik = "0001067983"
    _seed_pending_13f_hr(ebull_test_conn, accession=accession, filer_cik=filer_cik)
    ebull_test_conn.commit()

    base = f"https://www.sec.gov/Archives/edgar/data/{int(filer_cik)}/{accession.replace('-', '')}/"
    _patch_fetch_map(monkeypatch, {base + "index.json": _index_json(primary=None)})

    stats = run_manifest_worker(ebull_test_conn, source="sec_13f_hr", max_rows=10)
    ebull_test_conn.commit()

    assert stats.tombstoned == 1
    row = get_manifest_row(ebull_test_conn, accession)
    assert row is not None and row.ingest_status == "tombstoned"
    assert row.error is not None and "archive index missing files" in row.error


def test_infotable_empty_body_tombstones_preserving_stored_raw(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """primary_doc fetched + stored; infotable returns None → tombstone
    with raw_status='stored' (primary_doc is already on disk)."""
    accession = "0001067983-25-000005"
    filer_cik = "0001067983"
    _seed_pending_13f_hr(ebull_test_conn, accession=accession, filer_cik=filer_cik)
    ebull_test_conn.commit()

    base = f"https://www.sec.gov/Archives/edgar/data/{int(filer_cik)}/{accession.replace('-', '')}/"
    _patch_fetch_map(
        monkeypatch,
        {
            base + "index.json": _index_json(),
            base + "primary_doc.xml": _FAKE_PRIMARY_XML,
            base + "infotable.xml": None,
        },
    )

    stats = run_manifest_worker(ebull_test_conn, source="sec_13f_hr", max_rows=10)
    ebull_test_conn.commit()

    assert stats.tombstoned == 1
    row = get_manifest_row(ebull_test_conn, accession)
    assert row is not None
    assert row.ingest_status == "tombstoned"
    assert row.raw_status == "stored"
    # primary_doc raw row exists.
    with ebull_test_conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM filing_raw_documents WHERE accession_number = %s AND document_kind = 'primary_doc'",
            (accession,),
        )
        assert cur.fetchone() is not None


def test_fetch_exception_marks_failed_with_backoff(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    accession = "0001067983-25-000006"
    filer_cik = "0001067983"
    _seed_pending_13f_hr(ebull_test_conn, accession=accession, filer_cik=filer_cik)
    ebull_test_conn.commit()

    from app.providers.implementations import sec_edgar

    def _boom(self, url):  # noqa: ARG001
        raise RuntimeError("network kaput")

    monkeypatch.setattr(sec_edgar.SecFilingsProvider, "fetch_document_text", _boom)

    before = datetime.now(tz=UTC)
    stats = run_manifest_worker(ebull_test_conn, source="sec_13f_hr", max_rows=10)
    ebull_test_conn.commit()

    assert stats.failed == 1
    row = get_manifest_row(ebull_test_conn, accession)
    assert row is not None and row.ingest_status == "failed"
    assert row.error is not None and "fetch error" in row.error
    assert row.next_retry_at is not None
    delta = (row.next_retry_at - before).total_seconds()
    assert 3300 < delta < 3900  # ~1h backoff ± slack


def test_parse_phase_exception_preserves_stored_raw_status(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Parse of primary_doc raises after store_raw committed → manifest
    must reflect raw_status='stored' so the next retry sees the stored
    body (no re-fetch from SEC)."""
    from app.services.manifest_parsers import sec_13f_hr as parser_module

    accession = "0001067983-25-000007"
    filer_cik = "0001067983"
    _seed_pending_13f_hr(ebull_test_conn, accession=accession, filer_cik=filer_cik)
    ebull_test_conn.commit()

    base = f"https://www.sec.gov/Archives/edgar/data/{int(filer_cik)}/{accession.replace('-', '')}/"
    _patch_fetch_map(
        monkeypatch,
        {
            base + "index.json": _index_json(),
            base + "primary_doc.xml": _FAKE_PRIMARY_XML,
        },
    )

    def _raising(*args, **kwargs):  # noqa: ARG001
        raise RuntimeError("synthetic primary_doc parse crash")

    monkeypatch.setattr(parser_module, "parse_primary_doc", _raising)

    stats = run_manifest_worker(ebull_test_conn, source="sec_13f_hr", max_rows=10)
    ebull_test_conn.commit()

    assert stats.failed == 1
    row = get_manifest_row(ebull_test_conn, accession)
    assert row is not None
    assert row.ingest_status == "failed"
    assert row.raw_status == "stored"


def test_deterministic_upsert_exception_tombstones_with_log(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR #1131: a non-transient upsert exception writes an audit-log
    row with status='failed' + tombstones the manifest."""
    from app.services.manifest_parsers import sec_13f_hr as parser_module

    iid = 8790010
    cusip = "037833100"
    _seed_instrument(ebull_test_conn, iid=iid, symbol="AAPL")
    _seed_cusip_mapping(ebull_test_conn, instrument_id=iid, cusip=cusip)
    accession = "0001067983-25-000008"
    filer_cik = "0001067983"
    _seed_pending_13f_hr(ebull_test_conn, accession=accession, filer_cik=filer_cik)
    ebull_test_conn.commit()

    base = f"https://www.sec.gov/Archives/edgar/data/{int(filer_cik)}/{accession.replace('-', '')}/"
    _patch_fetch_map(
        monkeypatch,
        {
            base + "index.json": _index_json(),
            base + "primary_doc.xml": _FAKE_PRIMARY_XML,
            base + "infotable.xml": _infotable_xml(holdings=[{"cusip": cusip}]),
        },
    )

    def _raising_upsert(*args, **kwargs):  # noqa: ARG001
        raise RuntimeError("synthetic 13F-HR upsert violation")

    monkeypatch.setattr(parser_module, "_upsert_holding", _raising_upsert)

    stats = run_manifest_worker(ebull_test_conn, source="sec_13f_hr", max_rows=10)
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
            "SELECT status, error FROM institutional_holdings_ingest_log WHERE accession_number = %s",
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
    """PR #1131: an ``OperationalError`` on upsert keeps the manifest
    in ``failed`` with 1h backoff — no ingest-log row, no tombstone."""
    import psycopg.errors

    from app.services.manifest_parsers import sec_13f_hr as parser_module

    iid = 8790011
    cusip = "037833100"
    _seed_instrument(ebull_test_conn, iid=iid, symbol="AAPL")
    _seed_cusip_mapping(ebull_test_conn, instrument_id=iid, cusip=cusip)
    accession = "0001067983-25-000009"
    filer_cik = "0001067983"
    _seed_pending_13f_hr(ebull_test_conn, accession=accession, filer_cik=filer_cik)
    ebull_test_conn.commit()

    base = f"https://www.sec.gov/Archives/edgar/data/{int(filer_cik)}/{accession.replace('-', '')}/"
    _patch_fetch_map(
        monkeypatch,
        {
            base + "index.json": _index_json(),
            base + "primary_doc.xml": _FAKE_PRIMARY_XML,
            base + "infotable.xml": _infotable_xml(holdings=[{"cusip": cusip}]),
        },
    )

    def _raising_upsert(*args, **kwargs):  # noqa: ARG001
        raise psycopg.errors.DeadlockDetected("synthetic deadlock")

    monkeypatch.setattr(parser_module, "_upsert_holding", _raising_upsert)

    stats = run_manifest_worker(ebull_test_conn, source="sec_13f_hr", max_rows=10)
    ebull_test_conn.commit()

    assert stats.failed == 1
    assert stats.tombstoned == 0
    row = get_manifest_row(ebull_test_conn, accession)
    assert row is not None
    assert row.ingest_status == "failed"
    assert row.raw_status == "stored"
    assert row.error is not None and "DeadlockDetected" in row.error

    with ebull_test_conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM institutional_holdings_ingest_log WHERE accession_number = %s",
            (accession,),
        )
        # No log row — transient must keep the retry path clean.
        assert cur.fetchone() is None


def test_prn_holdings_dropped_not_written_as_shares(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex pre-push (Finding 1): SSHPRNAMTTYPE='PRN' rows carry
    bond principal amounts in dollars, NOT share counts. The adapter
    MUST drop them before write so they don't silently land in
    ``institutional_holdings.shares``. Mirrors the bulk dataset
    path's filter at sec_13f_dataset_ingest.py:311."""
    iid = 8790020
    cusip_eq = "037833100"
    # Bond CUSIP intentionally unmapped — the PRN-drop filter rejects
    # the row before CUSIP resolution, so we don't need a mapping.
    cusip_bond = "037833555"
    _seed_instrument(ebull_test_conn, iid=iid, symbol="AAPL")
    _seed_cusip_mapping(ebull_test_conn, instrument_id=iid, cusip=cusip_eq)
    accession = "0001067983-25-000020"
    filer_cik = "0001067983"
    _seed_pending_13f_hr(ebull_test_conn, accession=accession, filer_cik=filer_cik)
    ebull_test_conn.commit()

    base = f"https://www.sec.gov/Archives/edgar/data/{int(filer_cik)}/{accession.replace('-', '')}/"
    _patch_fetch_map(
        monkeypatch,
        {
            base + "index.json": _index_json(),
            base + "primary_doc.xml": _FAKE_PRIMARY_XML,
            base + "infotable.xml": _infotable_xml(
                holdings=[
                    {"cusip": cusip_eq, "amt_type": "SH", "shares": "1000"},
                    {"cusip": cusip_bond, "amt_type": "PRN", "shares": "5000000"},
                ]
            ),
        },
    )

    stats = run_manifest_worker(ebull_test_conn, source="sec_13f_hr", max_rows=10)
    ebull_test_conn.commit()

    assert stats.parsed == 1
    # Only the SH row landed; PRN dropped.
    with ebull_test_conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM institutional_holdings WHERE accession_number = %s",
            (accession,),
        )
        rowcount = cur.fetchone()
    assert rowcount is not None
    assert rowcount[0] == 1

    # Log status='partial' with the PRN drop count surfaced.
    with ebull_test_conn.cursor() as cur:
        cur.execute(
            "SELECT status, holdings_inserted, holdings_skipped, error FROM "
            "institutional_holdings_ingest_log WHERE accession_number = %s",
            (accession,),
        )
        log = cur.fetchone()
    assert log is not None
    assert log[0] == "partial"
    assert log[1] == 1
    assert log[2] == 1  # 1 PRN row skipped
    assert log[3] is not None and "PRN rows dropped" in log[3]


def test_pre_2023_filed_at_scales_value_by_thousand(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex pre-push (Finding 2): pre-2023-01-03 filings reported
    Column 4 in thousands, post in dollars. SEC EDGAR Release 22.4.1
    is keyed by filed_at (not period_end) because a pre-cutover
    restatement filed late carries dollars.

    Test: fixture's signatureDate=11-14-2024 lands the filing
    post-cutover by default. Patch ``parse_primary_doc`` to return an
    info whose ``filed_at`` is pre-cutover; assert the value lands
    1000x the raw XML number."""
    from datetime import date as date_cls
    from decimal import Decimal as _Decimal

    from app.providers.implementations.sec_13f import ThirteenFFilerInfo
    from app.services.manifest_parsers import sec_13f_hr as parser_module

    iid = 8790030
    cusip = "037833100"
    _seed_instrument(ebull_test_conn, iid=iid, symbol="AAPL")
    _seed_cusip_mapping(ebull_test_conn, instrument_id=iid, cusip=cusip)
    accession = "0001067983-22-000030"
    filer_cik = "0001067983"
    _seed_pending_13f_hr(ebull_test_conn, accession=accession, filer_cik=filer_cik)
    ebull_test_conn.commit()

    base = f"https://www.sec.gov/Archives/edgar/data/{int(filer_cik)}/{accession.replace('-', '')}/"
    _patch_fetch_map(
        monkeypatch,
        {
            base + "index.json": _index_json(),
            base + "primary_doc.xml": _FAKE_PRIMARY_XML,
            base + "infotable.xml": _infotable_xml(
                holdings=[{"cusip": cusip, "value": "1234"}],  # raw value (would be thousands)
            ),
        },
    )

    pre_cutover_filed_at = datetime(2022, 11, 14, tzinfo=UTC)

    def _fake_primary_doc(_xml: str) -> ThirteenFFilerInfo:
        return ThirteenFFilerInfo(
            cik="0001067983",
            name="BERKSHIRE HATHAWAY INC",
            period_of_report=date_cls(2022, 9, 30),
            filed_at=pre_cutover_filed_at,
            table_value_total_usd=_Decimal("123"),
        )

    monkeypatch.setattr(parser_module, "parse_primary_doc", _fake_primary_doc)

    stats = run_manifest_worker(ebull_test_conn, source="sec_13f_hr", max_rows=10)
    ebull_test_conn.commit()

    assert stats.parsed == 1
    with ebull_test_conn.cursor() as cur:
        cur.execute(
            "SELECT market_value_usd FROM institutional_holdings WHERE accession_number = %s",
            (accession,),
        )
        row = cur.fetchone()
    assert row is not None
    # Pre-cutover raw value 1234 → scaled to 1234 * 1000 = 1_234_000.
    assert row[0] == _Decimal("1234000")


def test_ingest_log_preserves_period_on_failure_after_success(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex pre-push (Finding 3): a failure-row INSERT/UPDATE that
    carries period_of_report=NULL must NOT erase a previously-known
    good period_of_report. COALESCE on the ON CONFLICT path keeps the
    existing value when EXCLUDED is NULL.

    Direct SQL test of the ON CONFLICT semantics — fastest path to
    pinning the behaviour without staging an end-to-end retry
    interleave."""
    from datetime import date as date_cls

    from app.services.institutional_holdings import _record_ingest_attempt

    accession = "0001067983-25-000040"
    filer_cik = "0001067983"

    # First write: success row with a real period.
    _record_ingest_attempt(
        ebull_test_conn,
        filer_cik=filer_cik,
        accession_number=accession,
        period_of_report=date_cls(2024, 9, 30),
        status="success",
        holdings_inserted=10,
        holdings_skipped=0,
        error=None,
    )
    ebull_test_conn.commit()

    # Second write: failure row with period=NULL (e.g. pre-parse
    # 404 retry). Must not erase the period column.
    _record_ingest_attempt(
        ebull_test_conn,
        filer_cik=filer_cik,
        accession_number=accession,
        period_of_report=None,
        status="failed",
        error="primary_doc.xml fetch failed",
    )
    ebull_test_conn.commit()

    with ebull_test_conn.cursor() as cur:
        cur.execute(
            "SELECT status, period_of_report FROM institutional_holdings_ingest_log WHERE accession_number = %s",
            (accession,),
        )
        row = cur.fetchone()
    assert row is not None
    assert row[0] == "failed"
    # The critical assertion: period preserved despite a NULL write.
    assert row[1] == date_cls(2024, 9, 30)


def test_parser_registered_via_register_all() -> None:
    """``register_all_parsers()`` wires ``sec_13f_hr`` alongside the
    other manifest parsers."""
    from app.jobs.sec_manifest_worker import registered_parser_sources
    from app.services.manifest_parsers import register_all_parsers

    assert "sec_13f_hr" in registered_parser_sources()
    clear_registered_parsers()
    assert "sec_13f_hr" not in registered_parser_sources()
    register_all_parsers()
    assert "sec_13f_hr" in registered_parser_sources()
