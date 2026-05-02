"""API tests for /instruments/{symbol}/blockholders (#766 PR 3).

Pins:
  * 404 on unknown symbol.
  * Empty payload (200, totals=null, blockholders=[]) when no
    filings on file.
  * Latest-filing-per-primary-filer aggregation (older accessions
    superseded).
  * Joint-filing reporters collapse to one block row, not N (matches
    the operator's mental model of "5+% blocks", not "individual
    reporters").
  * additional_reporters surfaces joint-filing depth without
    inflating the totals.
  * 13D vs 13G partition by status — active_shares + passive_shares
    sum to blockholders_shares.
  * limit + ordering — top-N by aggregate_amount_owned DESC.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime
from decimal import Decimal

import psycopg
import pytest
from fastapi.testclient import TestClient

from app.db import get_conn
from app.main import app
from app.services.blockholders import seed_filer
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export

pytestmark = pytest.mark.integration


@pytest.fixture
def client(
    ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
) -> Iterator[TestClient]:
    """TestClient with get_conn overridden to yield the ebull_test
    connection. Restores the override on teardown so cross-test
    leaks cannot poison subsequent suites."""

    def _dep() -> Iterator[psycopg.Connection[tuple]]:
        yield ebull_test_conn

    app.dependency_overrides[get_conn] = _dep
    try:
        yield TestClient(app)
    finally:
        app.dependency_overrides.pop(get_conn, None)


def _seed_instrument(conn: psycopg.Connection[tuple], *, iid: int, symbol: str) -> None:
    conn.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, company_name, exchange, currency, is_tradable)
        VALUES (%s, %s, %s, '4', 'USD', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        (iid, symbol, f"{symbol} Inc"),
    )


def _seed_filer_row(
    conn: psycopg.Connection[tuple],
    *,
    filer_id: int,
    cik: str,
    name: str,
) -> None:
    conn.execute(
        """
        INSERT INTO blockholder_filers (filer_id, cik, name)
        VALUES (%s, %s, %s)
        ON CONFLICT (cik) DO NOTHING
        """,
        (filer_id, cik, name),
    )


def _seed_filing(
    conn: psycopg.Connection[tuple],
    *,
    filer_id: int,
    accession: str,
    submission_type: str,
    status: str,
    instrument_id: int | None,
    issuer_cik: str = "0000012345",
    issuer_cusip: str = "037833100",
    reporter_cik: str | None,
    reporter_no_cik: bool,
    reporter_name: str,
    aggregate_shares: str,
    percent: str,
    filed_at: datetime,
) -> None:
    conn.execute(
        """
        INSERT INTO blockholder_filings (
            filer_id, accession_number, submission_type, status,
            instrument_id, issuer_cik, issuer_cusip,
            reporter_cik, reporter_no_cik, reporter_name,
            aggregate_amount_owned, percent_of_class,
            filed_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """,
        (
            filer_id,
            accession,
            submission_type,
            status,
            instrument_id,
            issuer_cik,
            issuer_cusip,
            reporter_cik,
            reporter_no_cik,
            reporter_name,
            Decimal(aggregate_shares),
            Decimal(percent),
            filed_at,
        ),
    )


class TestBlockholdersEndpoint:
    def test_unknown_symbol_returns_404(self, client: TestClient) -> None:
        resp = client.get("/instruments/NOPE/blockholders")
        assert resp.status_code == 404

    def test_empty_when_no_filings(
        self,
        client: TestClient,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        _seed_instrument(ebull_test_conn, iid=766_300, symbol="NEW")
        ebull_test_conn.commit()
        resp = client.get("/instruments/NEW/blockholders")
        assert resp.status_code == 200
        body = resp.json()
        assert body["symbol"] == "NEW"
        assert body["totals"] is None
        assert body["blockholders"] == []

    def test_single_13d_block_renders(
        self,
        client: TestClient,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        conn = ebull_test_conn
        _seed_instrument(conn, iid=766_310, symbol="AAPL")
        seed_filer(conn, cik="0001234567", label="Test Activist")
        _seed_filer_row(conn, filer_id=1001, cik="0001234567", name="Test Activist Fund LP")
        _seed_filing(
            conn,
            filer_id=1001,
            accession="0001234567-25-000001",
            submission_type="SCHEDULE 13D",
            status="active",
            instrument_id=766_310,
            reporter_cik="0001234567",
            reporter_no_cik=False,
            reporter_name="Test Activist Fund LP",
            aggregate_shares="1500000",
            percent="5.5",
            filed_at=datetime(2025, 11, 6, tzinfo=UTC),
        )
        conn.commit()

        resp = client.get("/instruments/AAPL/blockholders")
        assert resp.status_code == 200
        body = resp.json()
        assert body["totals"] is not None
        assert Decimal(body["totals"]["blockholders_shares"]) == Decimal("1500000")
        assert Decimal(body["totals"]["active_shares"]) == Decimal("1500000")
        assert Decimal(body["totals"]["passive_shares"]) == Decimal(0)
        assert body["totals"]["total_filers"] == 1
        assert body["totals"]["as_of_date"] == "2025-11-06"
        assert len(body["blockholders"]) == 1
        block = body["blockholders"][0]
        assert block["status"] == "active"
        assert block["submission_type"] == "SCHEDULE 13D"
        assert Decimal(block["aggregate_amount_owned"]) == Decimal("1500000")
        assert Decimal(block["percent_of_class"]) == Decimal("5.5")
        assert block["additional_reporters"] == 0

    def test_joint_filing_renders_per_reporter_but_totals_dedupe(
        self,
        client: TestClient,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """Per-reporter rows in the response (matches the issue spec:
        ``blockholders list (per-reporter rows)``), but the totals
        slice dedupes by accession so a 1.5M-share block claimed by
        two joint reporters contributes 1.5M to the totals, not 3M.
        ``total_filers`` counts distinct blocks (accessions), not
        reporter rows."""
        conn = ebull_test_conn
        _seed_instrument(conn, iid=766_320, symbol="AAPL")
        seed_filer(conn, cik="0001234567", label="Joint Filing Test")
        _seed_filer_row(conn, filer_id=1010, cik="0001234567", name="Test Activist Fund LP")
        accession = "0001234567-25-000010"
        _seed_filing(
            conn,
            filer_id=1010,
            accession=accession,
            submission_type="SCHEDULE 13D",
            status="active",
            instrument_id=766_320,
            reporter_cik="0001234567",
            reporter_no_cik=False,
            reporter_name="Test Activist Fund LP",
            aggregate_shares="1500000",
            percent="5.5",
            filed_at=datetime(2025, 11, 6, tzinfo=UTC),
        )
        _seed_filing(
            conn,
            filer_id=1010,
            accession=accession,
            submission_type="SCHEDULE 13D",
            status="active",
            instrument_id=766_320,
            reporter_cik=None,
            reporter_no_cik=True,
            reporter_name="Jane Doe (managing member)",
            aggregate_shares="1500000",
            percent="5.5",
            filed_at=datetime(2025, 11, 6, tzinfo=UTC),
        )
        conn.commit()

        resp = client.get("/instruments/AAPL/blockholders")
        body = resp.json()
        # Per-reporter rows: 2 reporters in the joint filing yield
        # 2 rows in the response. Each carries
        # ``additional_reporters=1`` surfacing the joint-filing depth.
        assert len(body["blockholders"]) == 2
        for row in body["blockholders"]:
            assert row["additional_reporters"] == 1
        # Totals dedupe by accession — 1.5M (one block), not 3M.
        # total_filers counts distinct blocks (accessions), not rows.
        assert Decimal(body["totals"]["blockholders_shares"]) == Decimal("1500000")
        assert body["totals"]["total_filers"] == 1
        # Each per-reporter row carries the same 1.5M aggregate — the
        # SEC instructions require joint filers to claim the same
        # beneficial ownership.
        assert Decimal(body["blockholders"][0]["aggregate_amount_owned"]) == Decimal("1500000")
        assert Decimal(body["blockholders"][1]["aggregate_amount_owned"]) == Decimal("1500000")
        assert Decimal(body["blockholders"][0]["aggregate_amount_owned"]) == Decimal("1500000")

    def test_amendment_chain_supersession(
        self,
        client: TestClient,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """An older 13G/A is superseded by a later 13D from the same
        primary filer on the same issuer. Only the 13D appears."""
        conn = ebull_test_conn
        _seed_instrument(conn, iid=766_330, symbol="AAPL")
        seed_filer(conn, cik="0001234567", label="Convert")
        _seed_filer_row(conn, filer_id=1020, cik="0001234567", name="Converting Fund")

        _seed_filing(
            conn,
            filer_id=1020,
            accession="0001234567-25-000020",
            submission_type="SCHEDULE 13G/A",
            status="passive",
            instrument_id=766_330,
            reporter_cik="0001234567",
            reporter_no_cik=False,
            reporter_name="Converting Fund",
            aggregate_shares="500000",
            percent="2.5",
            filed_at=datetime(2025, 9, 15, tzinfo=UTC),
        )
        _seed_filing(
            conn,
            filer_id=1020,
            accession="0001234567-25-000021",
            submission_type="SCHEDULE 13D",
            status="active",
            instrument_id=766_330,
            reporter_cik="0001234567",
            reporter_no_cik=False,
            reporter_name="Converting Fund",
            aggregate_shares="2000000",
            percent="7.0",
            filed_at=datetime(2025, 11, 6, tzinfo=UTC),
        )
        conn.commit()

        resp = client.get("/instruments/AAPL/blockholders")
        body = resp.json()
        assert len(body["blockholders"]) == 1
        assert body["blockholders"][0]["status"] == "active"
        assert body["blockholders"][0]["submission_type"] == "SCHEDULE 13D"
        assert Decimal(body["blockholders"][0]["aggregate_amount_owned"]) == Decimal("2000000")
        assert Decimal(body["totals"]["blockholders_shares"]) == Decimal("2000000")
        assert Decimal(body["totals"]["active_shares"]) == Decimal("2000000")
        assert Decimal(body["totals"]["passive_shares"]) == Decimal(0)

    def test_supersession_across_different_submitters(
        self,
        client: TestClient,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """Same beneficial owner (same reporter_cik) files a 13D/A
        through a different EDGAR submitter than the original 13D.
        The chain identity is the reporter, not the submitter — so
        the later filing supersedes the earlier one even though
        ``filer_id`` differs. Codex pre-push review flagged the
        prior filer_id-keyed query for missing this case."""
        conn = ebull_test_conn
        _seed_instrument(conn, iid=766_360, symbol="AAPL")
        # Two distinct submitters, but they file on behalf of the
        # same beneficial owner (same reporter_cik).
        seed_filer(conn, cik="0001111111", label="Old Submitter")
        seed_filer(conn, cik="0002222222", label="New Submitter")
        _seed_filer_row(conn, filer_id=2001, cik="0001111111", name="Old Submitter LLC")
        _seed_filer_row(conn, filer_id=2002, cik="0002222222", name="New Submitter LLC")

        # First filing: old submitter, original 13D.
        _seed_filing(
            conn,
            filer_id=2001,
            accession="0001111111-25-000001",
            submission_type="SCHEDULE 13D",
            status="active",
            instrument_id=766_360,
            reporter_cik="0009999999",
            reporter_no_cik=False,
            reporter_name="Carl Icahn",
            aggregate_shares="1000000",
            percent="3.5",
            filed_at=datetime(2025, 6, 1, tzinfo=UTC),
        )
        # Second filing: same beneficial owner, NEW submitter, 13D/A
        # with updated position. Must supersede the original.
        _seed_filing(
            conn,
            filer_id=2002,
            accession="0002222222-25-000001",
            submission_type="SCHEDULE 13D/A",
            status="active",
            instrument_id=766_360,
            reporter_cik="0009999999",
            reporter_no_cik=False,
            reporter_name="Carl Icahn",
            aggregate_shares="2500000",
            percent="8.5",
            filed_at=datetime(2025, 11, 6, tzinfo=UTC),
        )
        conn.commit()

        resp = client.get("/instruments/AAPL/blockholders")
        body = resp.json()
        # Only the latest filing should appear — supersession via
        # reporter identity, not submitter identity.
        assert len(body["blockholders"]) == 1
        assert body["blockholders"][0]["accession_number"] == "0002222222-25-000001"
        assert body["blockholders"][0]["submission_type"] == "SCHEDULE 13D/A"
        assert Decimal(body["blockholders"][0]["aggregate_amount_owned"]) == Decimal("2500000")
        assert Decimal(body["totals"]["blockholders_shares"]) == Decimal("2500000")
        assert body["totals"]["total_filers"] == 1

    def test_distinct_holders_under_one_submitter_dont_collapse(
        self,
        client: TestClient,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """One EDGAR submitter (e.g. an investment adviser) files
        separately on behalf of two distinct beneficial owners (two
        different reporter_ciks, two different accessions). The
        response must expose both blocks — not collapse them under
        the shared submitter. Codex pre-push review flagged the
        prior filer_id-keyed query for collapsing this case."""
        conn = ebull_test_conn
        _seed_instrument(conn, iid=766_370, symbol="AAPL")
        seed_filer(conn, cik="0003333333", label="Shared Adviser")
        _seed_filer_row(conn, filer_id=2010, cik="0003333333", name="Shared Adviser LLC")

        # Two filings under the same submitter (filer_id=2010), but
        # for two different beneficial owners (distinct reporter_cik).
        _seed_filing(
            conn,
            filer_id=2010,
            accession="0003333333-25-000001",
            submission_type="SCHEDULE 13D",
            status="active",
            instrument_id=766_370,
            reporter_cik="0008000001",
            reporter_no_cik=False,
            reporter_name="Beneficial Owner A",
            aggregate_shares="1000000",
            percent="3.5",
            filed_at=datetime(2025, 11, 1, tzinfo=UTC),
        )
        _seed_filing(
            conn,
            filer_id=2010,
            accession="0003333333-25-000002",
            submission_type="SCHEDULE 13D",
            status="active",
            instrument_id=766_370,
            reporter_cik="0008000002",
            reporter_no_cik=False,
            reporter_name="Beneficial Owner B",
            aggregate_shares="2000000",
            percent="7.0",
            filed_at=datetime(2025, 11, 6, tzinfo=UTC),
        )
        conn.commit()

        resp = client.get("/instruments/AAPL/blockholders")
        body = resp.json()
        # Two distinct holders → two rows + two blocks in totals.
        assert len(body["blockholders"]) == 2
        names = {row["reporter_name"] for row in body["blockholders"]}
        assert names == {"Beneficial Owner A", "Beneficial Owner B"}
        assert Decimal(body["totals"]["blockholders_shares"]) == Decimal("3000000")
        assert body["totals"]["total_filers"] == 2

    def test_active_passive_partition(
        self,
        client: TestClient,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """Two filers — one 13D, one 13G — produce active + passive
        share counts that sum to the total."""
        conn = ebull_test_conn
        _seed_instrument(conn, iid=766_340, symbol="AAPL")
        seed_filer(conn, cik="0001234567", label="Active")
        seed_filer(conn, cik="0007654321", label="Passive")
        _seed_filer_row(conn, filer_id=1030, cik="0001234567", name="Activist Fund")
        _seed_filer_row(conn, filer_id=1031, cik="0007654321", name="Index Fund")

        _seed_filing(
            conn,
            filer_id=1030,
            accession="0001234567-25-000030",
            submission_type="SCHEDULE 13D",
            status="active",
            instrument_id=766_340,
            reporter_cik="0001234567",
            reporter_no_cik=False,
            reporter_name="Activist Fund",
            aggregate_shares="1500000",
            percent="5.5",
            filed_at=datetime(2025, 11, 6, tzinfo=UTC),
        )
        _seed_filing(
            conn,
            filer_id=1031,
            accession="0007654321-25-000001",
            submission_type="SCHEDULE 13G",
            status="passive",
            instrument_id=766_340,
            reporter_cik="0007654321",
            reporter_no_cik=False,
            reporter_name="Index Fund",
            aggregate_shares="3000000",
            percent="11.0",
            filed_at=datetime(2025, 10, 1, tzinfo=UTC),
        )
        conn.commit()

        resp = client.get("/instruments/AAPL/blockholders")
        body = resp.json()
        assert Decimal(body["totals"]["blockholders_shares"]) == Decimal("4500000")
        assert Decimal(body["totals"]["active_shares"]) == Decimal("1500000")
        assert Decimal(body["totals"]["passive_shares"]) == Decimal("3000000")
        assert body["totals"]["total_filers"] == 2

    def test_limit_truncates_drilldown_but_not_totals(
        self,
        client: TestClient,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """A small ``?limit=`` truncates the per-row list but the
        totals row spans every block — so the ownership card never
        mis-reports the slice when the operator filters the table."""
        conn = ebull_test_conn
        _seed_instrument(conn, iid=766_350, symbol="AAPL")
        for i in range(3):
            cik = f"00099999{i:02d}"
            seed_filer(conn, cik=cik, label=f"Filer {i}")
            _seed_filer_row(conn, filer_id=1100 + i, cik=cik, name=f"Filer {i}")
            _seed_filing(
                conn,
                filer_id=1100 + i,
                accession=f"00099999{i:02d}-25-000001",
                submission_type="SCHEDULE 13D",
                status="active",
                instrument_id=766_350,
                reporter_cik=cik,
                reporter_no_cik=False,
                reporter_name=f"Filer {i}",
                aggregate_shares=str(1000000 * (i + 1)),
                percent=str(i + 1),
                filed_at=datetime(2025, 11, 6, tzinfo=UTC),
            )
        conn.commit()

        resp = client.get("/instruments/AAPL/blockholders?limit=1")
        body = resp.json()
        assert len(body["blockholders"]) == 1
        # Largest block first.
        assert Decimal(body["blockholders"][0]["aggregate_amount_owned"]) == Decimal("3000000")
        # Totals span all 3 blocks.
        assert Decimal(body["totals"]["blockholders_shares"]) == Decimal("6000000")
        assert body["totals"]["total_filers"] == 3
