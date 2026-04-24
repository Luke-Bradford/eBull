"""Integration tests for ``ingest_insider_transactions`` (#429)."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import cast

import psycopg
import pytest

from app.services.insider_transactions import (
    get_insider_summary,
    ingest_insider_transactions,
)

pytestmark = pytest.mark.integration


class _StubFetcher:
    def __init__(self, by_url: dict[str, str | None]) -> None:
        self._by_url = by_url
        self.calls: list[str] = []

    def fetch_document_text(self, absolute_url: str) -> str | None:
        self.calls.append(absolute_url)
        return self._by_url.get(absolute_url)


def _seed_instrument(conn: psycopg.Connection[tuple], iid: int = 99, symbol: str = "AAPL") -> int:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO instruments (instrument_id, symbol, company_name) VALUES (%s, %s, %s) RETURNING instrument_id",
            (iid, symbol, "Test Co"),
        )
        row = cur.fetchone()
        assert row is not None
    conn.commit()
    return int(row[0])


def _seed_form_4(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
    accession: str,
    url: str,
    filing_type: str = "4",
    filing_date: str = "2024-06-01",
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO filing_events
                (instrument_id, filing_date, filing_type, provider,
                 provider_filing_id, primary_document_url)
            VALUES (%s, %s, %s, 'sec', %s, %s)
            """,
            (instrument_id, filing_date, filing_type, accession, url),
        )
    conn.commit()


_FORM_4_BUY = """<?xml version="1.0"?>
<ownershipDocument>
  <reportingOwner>
    <reportingOwnerId><rptOwnerName>CEO Name</rptOwnerName></reportingOwnerId>
    <reportingOwnerRelationship>
      <isOfficer>1</isOfficer>
      <officerTitle>CEO</officerTitle>
    </reportingOwnerRelationship>
  </reportingOwner>
  <nonDerivativeTable>
    <nonDerivativeTransaction>
      <transactionDate><value>2024-06-15</value></transactionDate>
      <transactionCoding><transactionCode>P</transactionCode></transactionCoding>
      <transactionAmounts>
        <transactionShares><value>500</value></transactionShares>
        <transactionPricePerShare><value>150.00</value></transactionPricePerShare>
        <transactionAcquiredDisposedCode><value>A</value></transactionAcquiredDisposedCode>
      </transactionAmounts>
      <ownershipNature><directOrIndirectOwnership><value>D</value></directOrIndirectOwnership></ownershipNature>
    </nonDerivativeTransaction>
  </nonDerivativeTable>
</ownershipDocument>
"""

_FORM_4_SELL = """<?xml version="1.0"?>
<ownershipDocument>
  <reportingOwner>
    <reportingOwnerId><rptOwnerName>CFO Name</rptOwnerName></reportingOwnerId>
    <reportingOwnerRelationship>
      <isOfficer>1</isOfficer>
      <officerTitle>CFO</officerTitle>
    </reportingOwnerRelationship>
  </reportingOwner>
  <nonDerivativeTable>
    <nonDerivativeTransaction>
      <transactionDate><value>2024-06-20</value></transactionDate>
      <transactionCoding><transactionCode>S</transactionCode></transactionCoding>
      <transactionAmounts>
        <transactionShares><value>200</value></transactionShares>
        <transactionPricePerShare><value>155.00</value></transactionPricePerShare>
        <transactionAcquiredDisposedCode><value>D</value></transactionAcquiredDisposedCode>
      </transactionAmounts>
      <ownershipNature><directOrIndirectOwnership><value>D</value></directOrIndirectOwnership></ownershipNature>
    </nonDerivativeTransaction>
  </nonDerivativeTable>
</ownershipDocument>
"""


class TestIngestInsiderTransactions:
    def test_happy_path_inserts_rows(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        iid = _seed_instrument(ebull_test_conn)
        _seed_form_4(
            ebull_test_conn,
            instrument_id=iid,
            accession="0000001-24-000001",
            url="https://www.sec.gov/Archives/form4-buy.xml",
        )
        fetcher = _StubFetcher({"https://www.sec.gov/Archives/form4-buy.xml": _FORM_4_BUY})

        result = ingest_insider_transactions(ebull_test_conn, cast("object", fetcher))  # type: ignore[arg-type]

        assert result.filings_scanned == 1
        assert result.filings_parsed == 1
        assert result.rows_inserted == 1
        with ebull_test_conn.cursor() as cur:
            cur.execute("SELECT filer_name, txn_code, shares FROM insider_transactions")
            row = cur.fetchone()
            assert row is not None
            assert row[0] == "CEO Name"
            assert row[1] == "P"
            assert row[2] == Decimal("500")

    def test_rerun_is_idempotent(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """Form 4 content is immutable per accession. Re-running the
        ingester must be a no-op — ON CONFLICT DO NOTHING on the
        (accession, row_num) UNIQUE."""
        iid = _seed_instrument(ebull_test_conn)
        _seed_form_4(
            ebull_test_conn,
            instrument_id=iid,
            accession="0000001-24-000001",
            url="https://www.sec.gov/Archives/form4.xml",
        )
        fetcher = _StubFetcher({"https://www.sec.gov/Archives/form4.xml": _FORM_4_BUY})
        ingest_insider_transactions(ebull_test_conn, cast("object", fetcher))  # type: ignore[arg-type]

        second_fetcher = _StubFetcher({"https://www.sec.gov/Archives/form4.xml": _FORM_4_BUY})
        second = ingest_insider_transactions(ebull_test_conn, cast("object", second_fetcher))  # type: ignore[arg-type]

        assert second.filings_scanned == 0
        assert second_fetcher.calls == []

    def test_form_4_amendment_included(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """4/A amendments are processed alongside plain 4."""
        iid = _seed_instrument(ebull_test_conn)
        _seed_form_4(
            ebull_test_conn,
            instrument_id=iid,
            accession="0000001-24-000002",
            url="https://www.sec.gov/Archives/amend.xml",
            filing_type="4/A",
        )
        fetcher = _StubFetcher({"https://www.sec.gov/Archives/amend.xml": _FORM_4_BUY})
        result = ingest_insider_transactions(ebull_test_conn, cast("object", fetcher))  # type: ignore[arg-type]
        assert result.rows_inserted == 1

    def test_non_form_4_skipped(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """10-K / 8-K / other forms must not be fetched."""
        iid = _seed_instrument(ebull_test_conn)
        _seed_form_4(
            ebull_test_conn,
            instrument_id=iid,
            accession="0000001-24-000003",
            url="https://www.sec.gov/Archives/other.xml",
            filing_type="10-K",
        )
        fetcher = _StubFetcher({})
        result = ingest_insider_transactions(ebull_test_conn, cast("object", fetcher))  # type: ignore[arg-type]
        assert result.filings_scanned == 0
        assert fetcher.calls == []

    def test_fetch_404_writes_tombstone_and_skips_next_run(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """Codex #429 H1 regression — a dead Form 4 URL must get a
        tombstone so the hourly ingester doesn't re-fetch it every
        run. Sentinel row with filer_name='__TOMBSTONE__' + row_num=-1.
        Second immediate pass must not call the fetcher."""
        iid = _seed_instrument(ebull_test_conn)
        _seed_form_4(
            ebull_test_conn,
            instrument_id=iid,
            accession="DEAD-ACC",
            url="https://www.sec.gov/dead.xml",
        )
        dead_fetcher = _StubFetcher({"https://www.sec.gov/dead.xml": None})
        ingest_insider_transactions(ebull_test_conn, cast("object", dead_fetcher))  # type: ignore[arg-type]

        second_fetcher = _StubFetcher({"https://www.sec.gov/dead.xml": None})
        second = ingest_insider_transactions(ebull_test_conn, cast("object", second_fetcher))  # type: ignore[arg-type]
        assert second.filings_scanned == 0
        assert second_fetcher.calls == []

    def test_tombstone_excluded_from_summary(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """Tombstones live in the same table but must not leak into
        the insider summary. Seed a tombstone + a real buy, assert
        the summary reflects only the real buy."""
        iid = _seed_instrument(ebull_test_conn)
        _seed_form_4(
            ebull_test_conn,
            instrument_id=iid,
            accession="DEAD-ACC",
            url="https://www.sec.gov/dead.xml",
        )
        _seed_form_4(
            ebull_test_conn,
            instrument_id=iid,
            accession="REAL-ACC",
            url="https://www.sec.gov/real.xml",
            filing_date=date.today().isoformat(),
        )
        fetcher = _StubFetcher(
            {
                "https://www.sec.gov/dead.xml": None,
                "https://www.sec.gov/real.xml": _FORM_4_BUY.replace("2024-06-15", date.today().isoformat()),
            }
        )
        ingest_insider_transactions(ebull_test_conn, cast("object", fetcher))  # type: ignore[arg-type]

        summary = get_insider_summary(ebull_test_conn, instrument_id=iid)
        # Only the real buy contributes — tombstone excluded.
        assert summary.net_shares_90d == Decimal("500")
        assert summary.buy_count_90d == 1
        assert summary.unique_filers_90d == 1

    def test_non_form_4_payload_counts_as_miss(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """The filing_events row says form='4' but the XML at the URL
        is a different document (URL rot / wrong upstream pointer).
        Parser returns None; ingester records parse_miss without
        inserting a phantom row."""
        iid = _seed_instrument(ebull_test_conn)
        _seed_form_4(
            ebull_test_conn,
            instrument_id=iid,
            accession="0000001-24-000004",
            url="https://www.sec.gov/Archives/wrong.xml",
        )
        fetcher = _StubFetcher({"https://www.sec.gov/Archives/wrong.xml": "<notForm4></notForm4>"})
        result = ingest_insider_transactions(ebull_test_conn, cast("object", fetcher))  # type: ignore[arg-type]
        assert result.parse_misses == 1
        assert result.rows_inserted == 0


class TestGetInsiderSummary:
    def test_net_buys_minus_sells(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """net_shares_90d = sum(buys) - sum(sells) over the window."""
        iid = _seed_instrument(ebull_test_conn)
        # Seed via the ingester so schema + parse path are both live.
        _seed_form_4(
            ebull_test_conn,
            instrument_id=iid,
            accession="BUY-1",
            url="https://www.sec.gov/buy.xml",
            filing_date=date.today().isoformat(),
        )
        _seed_form_4(
            ebull_test_conn,
            instrument_id=iid,
            accession="SELL-1",
            url="https://www.sec.gov/sell.xml",
            filing_date=date.today().isoformat(),
        )
        fetcher = _StubFetcher(
            {
                "https://www.sec.gov/buy.xml": _FORM_4_BUY.replace("2024-06-15", date.today().isoformat()),
                "https://www.sec.gov/sell.xml": _FORM_4_SELL.replace("2024-06-20", date.today().isoformat()),
            }
        )
        ingest_insider_transactions(ebull_test_conn, cast("object", fetcher))  # type: ignore[arg-type]

        summary = get_insider_summary(ebull_test_conn, instrument_id=iid)
        assert summary.net_shares_90d == Decimal("300")  # 500 bought - 200 sold
        assert summary.buy_count_90d == 1
        assert summary.sell_count_90d == 1
        assert summary.unique_filers_90d == 2

    def test_stale_transaction_excluded(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """Transactions > 90 days old drop out of the summary."""
        iid = _seed_instrument(ebull_test_conn)
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO insider_transactions
                    (instrument_id, accession_number, txn_row_num,
                     filer_name, txn_date, txn_code, shares, price, is_derivative)
                VALUES (%s, 'OLD', 0, 'X', CURRENT_DATE - INTERVAL '200 days',
                        'P', 1000, 50, FALSE)
                """,
                (iid,),
            )
        ebull_test_conn.commit()
        summary = get_insider_summary(ebull_test_conn, instrument_id=iid)
        assert summary.net_shares_90d == Decimal(0)
        assert summary.buy_count_90d == 0

    def test_derivative_excluded_from_summary(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """Option grants / RSU vests don't contribute to the net buy/
        sell signal — issue scope says the widget shows cash-buys vs
        sales, not stock-based comp."""
        iid = _seed_instrument(ebull_test_conn)
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO insider_transactions
                    (instrument_id, accession_number, txn_row_num,
                     filer_name, txn_date, txn_code, shares, price, is_derivative)
                VALUES (%s, 'GRANT', 0, 'X', CURRENT_DATE, 'A', 10000, NULL, TRUE)
                """,
                (iid,),
            )
        ebull_test_conn.commit()
        summary = get_insider_summary(ebull_test_conn, instrument_id=iid)
        assert summary.net_shares_90d == Decimal(0)


def test_fixture_imports_ok(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    """Sanity: the integration fixture fires + migration was applied."""
    with ebull_test_conn.cursor() as cur:
        cur.execute("SELECT 1 FROM information_schema.tables WHERE table_name = 'insider_transactions'")
        assert cur.fetchone() is not None
