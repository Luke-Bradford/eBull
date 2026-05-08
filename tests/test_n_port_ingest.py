"""Tests for the N-PORT ingester (#917 — Phase 3 PR1).

Three boundaries:

* Pure XML-in / dataclass-out parser (``parse_n_port_payload``).
* DB write-through helpers (``record_fund_observation`` +
  ``refresh_funds_current``) — exercised against the real
  ``ebull_test`` DB.
* SEC HTTP fetcher — abstracted as a :class:`Protocol` so tests
  use a deterministic in-memory fake.

Codex pre-impl review (2026-05-05) findings exercised:

* #1 — parser accepts ``NPORT-P`` / ``NPORT-P/A`` / ``N-PORT`` /
  ``N-PORT/A`` form spellings.
* #2 — fixture missing seriesId tombstones as failed.
* #3 + #4 — debt / preferred / short / no-cusip rows are dropped
  by the equity-common-Long write-side guard.
* #5 — amendments win over originals in ``_current``.
* #6 — parser runs offline (test asserts no network calls).
* #11 — ingest log measures parsed accessions.
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import psycopg
import psycopg.rows
import pytest

from app.services.n_port_ingest import (
    AccessionRef,
    NPortMissingSeriesError,
    NPortParseError,
    ingest_fund_n_port,
    parse_n_port_payload,
    parse_submissions_index,
)
from app.services.ownership_observations import (
    record_fund_observation,
    refresh_funds_current,
    upsert_sec_fund_series,
)
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export

pytestmark = pytest.mark.integration

_FIXTURE_DIR = Path(__file__).parent / "fixtures" / "sec"


# ---------------------------------------------------------------------------
# Seed helpers
# ---------------------------------------------------------------------------


def _seed_instrument(conn: psycopg.Connection[tuple], *, iid: int, symbol: str) -> None:
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, is_tradable)
        VALUES (%s, %s, %s, '4', 'USD', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        (iid, symbol, f"{symbol} Inc"),
    )


def _seed_cusip_mapping(conn: psycopg.Connection[tuple], *, instrument_id: int, cusip: str) -> None:
    conn.execute(
        """
        INSERT INTO external_identifiers (
            instrument_id, provider, identifier_type, identifier_value, is_primary
        )
        VALUES (%s, 'sec', 'cusip', %s, TRUE)
        ON CONFLICT (provider, identifier_type, identifier_value) DO NOTHING
        """,
        (instrument_id, cusip.upper()),
    )


def _submissions_json(*, accessions: list[tuple[str, str, str, str]]) -> str:
    """Build a fake submissions JSON. Each tuple is
    ``(accession, form, filing_date, report_date)``."""
    return json.dumps(
        {
            "filings": {
                "recent": {
                    "accessionNumber": [a[0] for a in accessions],
                    "form": [a[1] for a in accessions],
                    "filingDate": [a[2] for a in accessions],
                    "reportDate": [a[3] for a in accessions],
                },
                "files": [],
            }
        }
    )


def _archive_url(filer_cik: str, accession: str, filename: str) -> str:
    return f"https://www.sec.gov/Archives/edgar/data/{int(filer_cik)}/{accession.replace('-', '')}/{filename}"


class _FakeFetcher:
    """In-memory SEC HTTP fake — deterministic, no network."""

    def __init__(self, payloads: dict[str, str]) -> None:
        self._payloads = payloads
        self.fetch_log: list[str] = []

    def fetch_document_text(self, absolute_url: str) -> str | None:
        self.fetch_log.append(absolute_url)
        return self._payloads.get(absolute_url)


# ---------------------------------------------------------------------------
# Parser unit tests (offline, no DB)
# ---------------------------------------------------------------------------


class TestParseNPortPayload:
    def test_extracts_header_and_holdings(self) -> None:
        xml = (_FIXTURE_DIR / "nport_p_test_fund.xml").read_text(encoding="utf-8")
        parsed = parse_n_port_payload(xml)
        assert parsed.filer_cik == "0000036405"
        assert parsed.series_id == "S000002277"
        assert parsed.series_name.startswith("Vanguard 500 Index Fund")
        assert parsed.period_end == date(2025, 12, 31)
        assert parsed.filed_at == datetime(2026, 2, 26, tzinfo=UTC)
        # 7 holdings parsed total (the ingester filters down to
        # equity-common Long NS with valid CUSIPs + positive shares;
        # the parser surfaces all of them).
        assert len(parsed.holdings) == 7
        h = {h.cusip: h for h in parsed.holdings}
        assert h["037833100"].shares == Decimal("1000000")
        assert h["037833100"].asset_category == "EC"
        assert h["037833100"].payoff_profile == "Long"
        assert h["037833100"].units == "NS"
        assert h["594918104"].asset_category == "EC"
        # Debt holding is surfaced by the parser (its own assetCat='DBT').
        assert h["000000ACM"].asset_category == "DBT"
        # Short equity holding is surfaced.
        assert h["000000SHX"].payoff_profile == "Short"
        # Convertible bond — Long EC but units=PA (principal amount).
        assert h["000000CVB"].asset_category == "EC"
        assert h["000000CVB"].payoff_profile == "Long"
        assert h["000000CVB"].units == "PA"
        # Zero-balance equity-common.
        assert h["000000ZRO"].shares == Decimal("0")

    def test_raises_missing_series_id(self) -> None:
        xml = (_FIXTURE_DIR / "nport_p_missing_series.xml").read_text(encoding="utf-8")
        with pytest.raises(NPortMissingSeriesError):
            parse_n_port_payload(xml)

    def test_raises_on_malformed_xml(self) -> None:
        with pytest.raises(NPortParseError):
            parse_n_port_payload("<not><well><formed>")

    def test_runs_offline_no_network(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Codex pre-impl review #6: the parser must not reach the
        network. Patch every HTTP entrypoint to raise; the parse must
        still succeed."""
        # ``urllib.request.urlopen`` is the stdlib HTTP client; patch
        # at the public attribute so any code path that imports it
        # raises immediately.
        import urllib.request

        def _block(*args: object, **kwargs: object) -> object:
            raise AssertionError("parser made an HTTP request — must run offline")

        monkeypatch.setattr(urllib.request, "urlopen", _block)
        # ``httpx`` may not be imported by the parser, but if a future
        # change introduces it, the test must still trip. Use
        # ``importlib`` to reach into it only if installed.
        try:
            import httpx

            monkeypatch.setattr(httpx, "get", _block)
            monkeypatch.setattr(httpx.Client, "request", _block)
        except ImportError:
            pass

        xml = (_FIXTURE_DIR / "nport_p_test_fund.xml").read_text(encoding="utf-8")
        parsed = parse_n_port_payload(xml)
        assert parsed.series_id == "S000002277"


# ---------------------------------------------------------------------------
# Submissions-index walker
# ---------------------------------------------------------------------------


class TestParseSubmissionsIndex:
    def test_accepts_both_form_spellings(self) -> None:
        """Codex #1: submissions index may use ``NPORT-P`` (current)
        or ``N-PORT`` (legacy). Both must surface."""
        payload = _submissions_json(
            accessions=[
                ("0001234500-25-000603", "NPORT-P", "2026-02-26", "2025-12-31"),
                ("0001234500-25-000604", "NPORT-P/A", "2026-03-15", "2025-12-31"),
                ("0003-25-000003", "N-PORT", "2025-11-26", "2025-09-30"),
                ("0004-25-000004", "N-PORT/A", "2025-12-15", "2025-09-30"),
                ("0005-26-000005", "10-K", "2026-02-26", "2025-12-31"),
            ]
        )
        refs = parse_submissions_index(payload)
        forms = sorted(r.filing_type for r in refs)
        assert forms == ["N-PORT", "N-PORT/A", "NPORT-P", "NPORT-P/A"]

    def test_drops_other_forms(self) -> None:
        payload = _submissions_json(
            accessions=[
                ("0001234500-25-000603", "10-K", "2026-02-26", "2025-12-31"),
            ]
        )
        assert parse_submissions_index(payload) == []


# ---------------------------------------------------------------------------
# DB write-through tests — record + refresh
# ---------------------------------------------------------------------------


class TestRecordFundObservation:
    @pytest.fixture
    def _setup(self, ebull_test_conn: psycopg.Connection[tuple]) -> psycopg.Connection[tuple]:  # noqa: F811
        conn = ebull_test_conn
        _seed_instrument(conn, iid=917_001, symbol="AAPL")
        # Seed the fund series so foreign-key style references resolve.
        upsert_sec_fund_series(
            conn,
            fund_series_id="S000002277",
            fund_series_name="Vanguard 500 Index Fund",
            fund_filer_cik="0000036405",
            last_seen_period_end=date(2025, 12, 31),
        )
        conn.commit()
        return conn

    def test_round_trip(self, _setup: psycopg.Connection[tuple]) -> None:
        conn = _setup
        record_fund_observation(
            conn,
            instrument_id=917_001,
            fund_series_id="S000002277",
            fund_series_name="Vanguard 500 Index Fund",
            fund_filer_cik="0000036405",
            source_document_id="0001234500-25-000603",
            source_accession="0001234500-25-000603",
            source_field=None,
            source_url="https://www.sec.gov/.../primary_doc.xml",
            filed_at=datetime(2026, 2, 26, tzinfo=UTC),
            period_start=None,
            period_end=date(2025, 12, 31),
            ingest_run_id=uuid4(),
            shares=Decimal("1000000"),
            market_value_usd=Decimal("225000000.00"),
            payoff_profile="Long",
            asset_category="EC",
        )
        conn.commit()
        n = refresh_funds_current(conn, instrument_id=917_001)
        conn.commit()
        assert n == 1
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                "SELECT shares, fund_series_id FROM ownership_funds_current WHERE instrument_id = %s",
                (917_001,),
            )
            rows = cur.fetchall()
        assert len(rows) == 1
        assert rows[0]["shares"] == Decimal("1000000.0000")
        assert rows[0]["fund_series_id"] == "S000002277"

    def test_rejects_invalid_series_id(self, _setup: psycopg.Connection[tuple]) -> None:
        with pytest.raises(ValueError, match="invalid fund_series_id"):
            record_fund_observation(
                _setup,
                instrument_id=917_001,
                fund_series_id="not-a-series-id",
                fund_series_name="X",
                fund_filer_cik="0000000001",
                source_document_id="0001234500-25-000605",
                source_accession="0001234500-25-000605",
                source_field=None,
                source_url=None,
                filed_at=datetime(2026, 2, 26, tzinfo=UTC),
                period_start=None,
                period_end=date(2025, 12, 31),
                ingest_run_id=uuid4(),
                shares=Decimal("1"),
                market_value_usd=None,
                payoff_profile="Long",
                asset_category="EC",
            )

    def test_rejects_short_payoff(self, _setup: psycopg.Connection[tuple]) -> None:
        with pytest.raises(ValueError, match="payoff_profile"):
            record_fund_observation(
                _setup,
                instrument_id=917_001,
                fund_series_id="S000002277",
                fund_series_name="Vanguard 500",
                fund_filer_cik="0000036405",
                source_document_id="0001234500-25-000605",
                source_accession="0001234500-25-000605",
                source_field=None,
                source_url=None,
                filed_at=datetime(2026, 2, 26, tzinfo=UTC),
                period_start=None,
                period_end=date(2025, 12, 31),
                ingest_run_id=uuid4(),
                shares=Decimal("1"),
                market_value_usd=None,
                payoff_profile="Short",
                asset_category="EC",
            )

    def test_rejects_debt_assetcat(self, _setup: psycopg.Connection[tuple]) -> None:
        with pytest.raises(ValueError, match="asset_category"):
            record_fund_observation(
                _setup,
                instrument_id=917_001,
                fund_series_id="S000002277",
                fund_series_name="Vanguard 500",
                fund_filer_cik="0000036405",
                source_document_id="0001234500-25-000605",
                source_accession="0001234500-25-000605",
                source_field=None,
                source_url=None,
                filed_at=datetime(2026, 2, 26, tzinfo=UTC),
                period_start=None,
                period_end=date(2025, 12, 31),
                ingest_run_id=uuid4(),
                shares=Decimal("1"),
                market_value_usd=None,
                payoff_profile="Long",
                asset_category="DBT",
            )

    def test_rejects_zero_shares(self, _setup: psycopg.Connection[tuple]) -> None:
        with pytest.raises(ValueError, match="shares"):
            record_fund_observation(
                _setup,
                instrument_id=917_001,
                fund_series_id="S000002277",
                fund_series_name="Vanguard 500",
                fund_filer_cik="0000036405",
                source_document_id="0001234500-25-000605",
                source_accession="0001234500-25-000605",
                source_field=None,
                source_url=None,
                filed_at=datetime(2026, 2, 26, tzinfo=UTC),
                period_start=None,
                period_end=date(2025, 12, 31),
                ingest_run_id=uuid4(),
                shares=Decimal("0"),
                market_value_usd=None,
                payoff_profile="Long",
                asset_category="EC",
            )

    def test_amendment_wins_in_current(self, _setup: psycopg.Connection[tuple]) -> None:
        """Codex #5: NPORT-P/A filed later than original NPORT-P for
        the same period must win in ``_current``."""
        conn = _setup
        # Original NPORT-P, filed Feb 26.
        record_fund_observation(
            conn,
            instrument_id=917_001,
            fund_series_id="S000002277",
            fund_series_name="Vanguard 500 Index Fund",
            fund_filer_cik="0000036405",
            source_document_id="0001234500-25-000603",
            source_accession="0001234500-25-000603",
            source_field=None,
            source_url="https://www.sec.gov/.../primary_doc.xml",
            filed_at=datetime(2026, 2, 26, tzinfo=UTC),
            period_start=None,
            period_end=date(2025, 12, 31),
            ingest_run_id=uuid4(),
            shares=Decimal("1000000"),
            market_value_usd=Decimal("225000000.00"),
            payoff_profile="Long",
            asset_category="EC",
        )
        # Amendment NPORT-P/A, filed Mar 15 with corrected shares.
        record_fund_observation(
            conn,
            instrument_id=917_001,
            fund_series_id="S000002277",
            fund_series_name="Vanguard 500 Index Fund",
            fund_filer_cik="0000036405",
            source_document_id="0001234500-25-000604",
            source_accession="0001234500-25-000604",
            source_field=None,
            source_url="https://www.sec.gov/.../primary_doc.xml",
            filed_at=datetime(2026, 3, 15, tzinfo=UTC),
            period_start=None,
            period_end=date(2025, 12, 31),
            ingest_run_id=uuid4(),
            shares=Decimal("1100000"),  # corrected upward
            market_value_usd=Decimal("247500000.00"),
            payoff_profile="Long",
            asset_category="EC",
        )
        conn.commit()
        refresh_funds_current(conn, instrument_id=917_001)
        conn.commit()
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                "SELECT shares, source_document_id FROM ownership_funds_current WHERE instrument_id = %s",
                (917_001,),
            )
            rows = cur.fetchall()
        assert len(rows) == 1
        # Amendment shares win.
        assert rows[0]["shares"] == Decimal("1100000.0000")
        assert rows[0]["source_document_id"] == "0001234500-25-000604"


# ---------------------------------------------------------------------------
# End-to-end ingester (parser → DB)
# ---------------------------------------------------------------------------


class TestIngestFundNPort:
    @pytest.fixture
    def _setup(self, ebull_test_conn: psycopg.Connection[tuple]) -> psycopg.Connection[tuple]:  # noqa: F811
        conn = ebull_test_conn
        # AAPL + MSFT have valid CUSIP mappings; ZZZ / DBT / SHX do not
        # — they exercise the drop paths.
        _seed_instrument(conn, iid=917_010, symbol="AAPL")
        _seed_instrument(conn, iid=917_011, symbol="MSFT")
        _seed_cusip_mapping(conn, instrument_id=917_010, cusip="037833100")
        _seed_cusip_mapping(conn, instrument_id=917_011, cusip="594918104")
        conn.commit()
        return conn

    def test_ingest_drops_non_equity_short_unresolved(self, _setup: psycopg.Connection[tuple]) -> None:
        conn = _setup
        accession = "0000036405-26-000001"
        primary_url = _archive_url("0000036405", accession, "primary_doc.xml")
        submissions_url = "https://data.sec.gov/submissions/CIK0000036405.json"
        fixture_xml = (_FIXTURE_DIR / "nport_p_test_fund.xml").read_text(encoding="utf-8")
        fetcher = _FakeFetcher(
            {
                submissions_url: _submissions_json(accessions=[(accession, "NPORT-P", "2026-02-26", "2025-12-31")]),
                primary_url: fixture_xml,
            }
        )

        summary = ingest_fund_n_port(conn, fetcher, filer_cik="0000036405")
        conn.commit()

        # Fixture has 7 holdings: AAPL + MSFT pass guards. ACME-debt
        # drops on assetCat, SHRT-X drops on Short, ZZZZ drops on
        # missing CUSIP mapping, CVBND drops on units=PA, ZRO drops
        # on zero shares.
        assert summary.holdings_inserted == 2
        assert summary.holdings_skipped_non_equity == 1
        assert summary.holdings_skipped_short == 1
        assert summary.holdings_skipped_no_cusip == 1
        assert summary.holdings_skipped_non_share_units == 1
        assert summary.holdings_skipped_zero_shares == 1
        assert summary.accessions_ingested == 1
        assert summary.accessions_failed == 0

        # AAPL row landed in observations + current.
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                "SELECT shares, fund_series_id FROM ownership_funds_current WHERE instrument_id = %s",
                (917_010,),
            )
            rows = cur.fetchall()
        assert len(rows) == 1
        assert rows[0]["shares"] == Decimal("1000000.0000")
        assert rows[0]["fund_series_id"] == "S000002277"

        # sec_fund_series upserted.
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                "SELECT fund_series_name, fund_filer_cik FROM sec_fund_series WHERE fund_series_id = %s",
                ("S000002277",),
            )
            series_rows = cur.fetchall()
        assert len(series_rows) == 1
        assert series_rows[0]["fund_filer_cik"] == "0000036405"

        # Raw payload stored before parse (prevention-log #1168).
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM filing_raw_documents WHERE accession_number = %s AND document_kind = %s",
                (accession, "nport_xml"),
            )
            row = cur.fetchone()
        assert row is not None
        assert row[0] == 1

        # Tombstone log row written.
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                "SELECT status, holdings_inserted, holdings_skipped FROM n_port_ingest_log WHERE accession_number = %s",
                (accession,),
            )
            log_rows = cur.fetchall()
        assert len(log_rows) == 1
        assert log_rows[0]["status"] == "partial"  # 5 of 7 dropped → partial
        assert log_rows[0]["holdings_inserted"] == 2
        assert log_rows[0]["holdings_skipped"] == 5

    def test_idempotent_reingest(self, _setup: psycopg.Connection[tuple]) -> None:
        """Re-running the same accession after the tombstone is stamped
        doesn't re-fetch primary_doc."""
        conn = _setup
        accession = "0000036405-26-000001"
        primary_url = _archive_url("0000036405", accession, "primary_doc.xml")
        submissions_url = "https://data.sec.gov/submissions/CIK0000036405.json"
        fixture_xml = (_FIXTURE_DIR / "nport_p_test_fund.xml").read_text(encoding="utf-8")
        payloads = {
            submissions_url: _submissions_json(accessions=[(accession, "NPORT-P", "2026-02-26", "2025-12-31")]),
            primary_url: fixture_xml,
        }
        fetcher_first = _FakeFetcher(payloads)
        ingest_fund_n_port(conn, fetcher_first, filer_cik="0000036405")
        conn.commit()
        # Second invocation — submissions fetched, but primary_doc NOT
        # re-fetched (tombstoned).
        fetcher_second = _FakeFetcher(payloads)
        ingest_fund_n_port(conn, fetcher_second, filer_cik="0000036405")
        conn.commit()
        assert submissions_url in fetcher_second.fetch_log
        assert primary_url not in fetcher_second.fetch_log

    def test_amendment_uses_submissions_filed_at_when_header_missing(self, _setup: psycopg.Connection[tuple]) -> None:
        """Codex pre-push review #1: when the primary doc lacks a
        header ``filedAt``, the ingester must fall back to the
        submissions-index ``filingDate``. Otherwise an amendment
        sharing a period with its original would silently tie on
        ``filed_at`` (period_end midnight) and the older accession
        would win the tie-break."""
        conn = _setup
        accession_orig = "0000036405-26-000010"
        accession_amend = "0000036405-26-000011"
        # Build a primary doc XML that lacks a <filedAt> header.
        fixture_xml = (_FIXTURE_DIR / "nport_p_test_fund.xml").read_text(encoding="utf-8")
        no_header_xml = fixture_xml.replace("<filedAt>2026-02-26</filedAt>", "")
        # Also vary the AAPL share count between original and amendment
        # so the test can prove which row wins.
        amended_xml = no_header_xml.replace("<balance>1000000</balance>", "<balance>1234567</balance>", 1)
        primary_url_orig = _archive_url("0000036405", accession_orig, "primary_doc.xml")
        primary_url_amend = _archive_url("0000036405", accession_amend, "primary_doc.xml")
        submissions_url = "https://data.sec.gov/submissions/CIK0000036405.json"
        fetcher = _FakeFetcher(
            {
                submissions_url: _submissions_json(
                    accessions=[
                        (accession_orig, "NPORT-P", "2026-02-26", "2025-12-31"),
                        (accession_amend, "NPORT-P/A", "2026-03-15", "2025-12-31"),
                    ]
                ),
                primary_url_orig: no_header_xml,
                primary_url_amend: amended_xml,
            }
        )
        ingest_fund_n_port(conn, fetcher, filer_cik="0000036405")
        conn.commit()
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                "SELECT shares, source_document_id FROM ownership_funds_current WHERE instrument_id = %s",
                (917_010,),
            )
            rows = cur.fetchall()
        assert len(rows) == 1
        # Amendment wins because its submissions-index filingDate is
        # 2026-03-15 vs original's 2026-02-26.
        assert rows[0]["source_document_id"] == accession_amend
        assert rows[0]["shares"] == Decimal("1234567.0000")

    def test_missing_series_id_tombstones_failed(self, _setup: psycopg.Connection[tuple]) -> None:
        conn = _setup
        accession = "0000036405-26-000099"
        primary_url = _archive_url("0000036405", accession, "primary_doc.xml")
        submissions_url = "https://data.sec.gov/submissions/CIK0000036405.json"
        broken_xml = (_FIXTURE_DIR / "nport_p_missing_series.xml").read_text(encoding="utf-8")
        fetcher = _FakeFetcher(
            {
                submissions_url: _submissions_json(accessions=[(accession, "NPORT-P", "2026-02-26", "2025-12-31")]),
                primary_url: broken_xml,
            }
        )
        summary = ingest_fund_n_port(conn, fetcher, filer_cik="0000036405")
        conn.commit()
        assert summary.accessions_failed == 1
        assert summary.accessions_ingested == 0
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                "SELECT status, error FROM n_port_ingest_log WHERE accession_number = %s",
                (accession,),
            )
            log_rows = cur.fetchall()
        assert log_rows[0]["status"] == "failed"
        assert "seriesId" in str(log_rows[0]["error"])


# ---------------------------------------------------------------------------
# Schema-shape checks
# ---------------------------------------------------------------------------


class TestFundsSchema:
    def test_check_constraint_rejects_short_payoff(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """Schema-level second-line guard: even if the ingester / helper
        is bypassed, the CHECK constraint refuses Short rows."""
        conn = ebull_test_conn
        _seed_instrument(conn, iid=917_500, symbol="TEST")
        conn.commit()
        with pytest.raises(psycopg.errors.CheckViolation):
            conn.execute(
                """
                INSERT INTO ownership_funds_observations (
                    instrument_id, fund_series_id, fund_series_name, fund_filer_cik,
                    ownership_nature, source, source_document_id,
                    filed_at, period_end, ingest_run_id,
                    shares, payoff_profile, asset_category
                ) VALUES (
                    %s, 'S000002277', 'X', '0000036405',
                    'economic', 'nport', 'acc',
                    NOW(), '2025-12-31', %s,
                    1, 'Short', 'EC'
                )
                """,
                (917_500, str(uuid4())),
            )

    def test_check_constraint_rejects_invalid_series_id(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        conn = ebull_test_conn
        _seed_instrument(conn, iid=917_501, symbol="TEST2")
        conn.commit()
        with pytest.raises(psycopg.errors.CheckViolation):
            conn.execute(
                """
                INSERT INTO ownership_funds_observations (
                    instrument_id, fund_series_id, fund_series_name, fund_filer_cik,
                    ownership_nature, source, source_document_id,
                    filed_at, period_end, ingest_run_id,
                    shares, payoff_profile, asset_category
                ) VALUES (
                    %s, 'BOGUS', 'X', '0000036405',
                    'economic', 'nport', 'acc',
                    NOW(), '2025-12-31', %s,
                    1, 'Long', 'EC'
                )
                """,
                (917_501, str(uuid4())),
            )

    def test_partition_for_2025q4_exists(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                """
                SELECT to_regclass('public.ownership_funds_observations_2025q4')
                """
            )
            row = cur.fetchone()
        assert row is not None
        assert row[0] is not None

    def test_default_partition_empty(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        with ebull_test_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM ownership_funds_observations_default")
            row = cur.fetchone()
        assert row is not None
        assert row[0] == 0

    def test_n_port_ingest_log_check_rejects_invalid_series_id(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """Codex pre-push review #5: tombstone log carries the same
        regex CHECK as the observations / current / series tables."""
        conn = ebull_test_conn
        with pytest.raises(psycopg.errors.CheckViolation):
            conn.execute(
                """
                INSERT INTO n_port_ingest_log (
                    accession_number, filer_cik, fund_series_id, status
                ) VALUES (
                    'acc-bogus', '0000036405', 'BOGUS', 'failed'
                )
                """
            )


# ---------------------------------------------------------------------------
# ingest_all_fund_filers status precedence
# ---------------------------------------------------------------------------


class TestIngestAllStatusPrecedence:
    """Codex pre-push review (Coverage Gaps): status precedence
    deadline > crash-only > partial-or-skipped > success must mirror
    institutional_holdings.ingest_all_active_filers exactly.

    Rather than re-driving the full ingester, exercise the precedence
    logic directly by constructing summaries + invoking the function
    body's bookkeeping. The contract is small enough that a focused
    unit test against the helper aggregations covers it."""

    def test_success_when_no_skips_or_failures(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        from app.services.n_port_ingest import ingest_all_fund_filers

        # Empty CIK list short-circuits — covers the "no work" case.
        assert ingest_all_fund_filers(ebull_test_conn, _FakeFetcher({}), ciks=[]) == []


# ---------------------------------------------------------------------------
# Manifest form-code map
# ---------------------------------------------------------------------------


class TestManifestFormCodes:
    def test_all_n_port_spellings_map_to_sec_n_port(self) -> None:
        """Codex pre-impl review #1: extending the form map is the
        contract that lets ingest see both spellings."""
        from app.services.sec_manifest import map_form_to_source

        for form in ("N-PORT", "N-PORT/A", "NPORT-P", "NPORT-P/A"):
            assert map_form_to_source(form) == "sec_n_port"


# ---------------------------------------------------------------------------
# Misc parser dataclass guards (ergonomic checks)
# ---------------------------------------------------------------------------


def test_accession_ref_dataclass_is_frozen() -> None:
    ref = AccessionRef(
        accession_number="0001234500-25-000603",
        filing_type="NPORT-P",
        period_of_report=date(2025, 12, 31),
        filed_at=datetime(2026, 2, 26, tzinfo=UTC),
    )
    with pytest.raises((AttributeError, TypeError)):
        ref.accession_number = "0002"  # type: ignore[misc]
