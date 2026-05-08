"""Tests for the bulk Form N-PORT dataset ingester (#1025)."""

from __future__ import annotations

import csv
import io
import zipfile
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest

from app.services.sec_nport_dataset_ingest import (
    NPortIngestResult,
    ingest_nport_dataset_archive,
)
from tests.fixtures.ebull_test_db import ebull_test_conn
from tests.fixtures.ebull_test_db import test_db_available as _test_db_available

__all__ = ["ebull_test_conn"]


# ---------------------------------------------------------------------------
# Archive fixture
# ---------------------------------------------------------------------------


def _build_dataset_zip(
    *,
    submissions: list[dict[str, str]],
    registrants: list[dict[str, str]],
    fund_info: list[dict[str, str]],
    holdings: list[dict[str, str]],
) -> bytes:
    def _to_tsv(rows: list[dict[str, str]]) -> str:
        if not rows:
            return ""
        fieldnames = sorted({k for row in rows for k in row.keys()})
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=fieldnames, delimiter="\t")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
        return buf.getvalue()

    out = io.BytesIO()
    with zipfile.ZipFile(out, "w") as zf:
        zf.writestr("SUBMISSION.tsv", _to_tsv(submissions))
        zf.writestr("REGISTRANT.tsv", _to_tsv(registrants))
        zf.writestr("FUND_REPORTED_INFO.tsv", _to_tsv(fund_info))
        zf.writestr("FUND_REPORTED_HOLDING.tsv", _to_tsv(holdings))
    return out.getvalue()


# ---------------------------------------------------------------------------
# DB integration
# ---------------------------------------------------------------------------


_NEXT_IID: list[int] = [14000]


def _seed_universe_with_cusip(
    conn: psycopg.Connection[tuple],
    *,
    symbol: str,
    cusip: str,
) -> int:
    _NEXT_IID[0] += 1
    iid = _NEXT_IID[0]
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO instruments (instrument_id, symbol, company_name, currency, is_tradable) "
            "VALUES (%s, %s, %s, 'USD', TRUE)",
            (iid, symbol, f"{symbol} Inc."),
        )
        cur.execute(
            "INSERT INTO external_identifiers "
            "(instrument_id, provider, identifier_type, identifier_value, is_primary) "
            "VALUES (%s, 'sec', 'cusip', %s, TRUE)",
            (iid, cusip.upper()),
        )
    conn.commit()
    return iid


@pytest.mark.integration
@pytest.mark.skipif(not _test_db_available(), reason="ebull_test DB unavailable")
class TestIngestNPortDatasetArchive:
    def test_equity_long_holding_writes_observation(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
        tmp_path: Path,
    ) -> None:
        # AAPL CUSIP — 037833100.
        iid = _seed_universe_with_cusip(ebull_test_conn, symbol="AAPL", cusip="037833100")

        archive_bytes = _build_dataset_zip(
            submissions=[
                {
                    "ACCESSION_NUMBER": "ACCN1",
                    "FILING_DATE": "2025-11-30",
                    "SUB_TYPE": "NPORT-P",
                    "REPORT_DATE": "2025-09-30",
                },
            ],
            registrants=[
                {"ACCESSION_NUMBER": "ACCN1", "CIK": "1234567", "REGISTRANT_NAME": "Big Fund Trust"},
            ],
            fund_info=[
                {
                    "ACCESSION_NUMBER": "ACCN1",
                    "SERIES_ID": "S000004310",
                    "SERIES_NAME": "Big Fund Equity Series",
                },
            ],
            holdings=[
                {
                    "ACCESSION_NUMBER": "ACCN1",
                    "HOLDING_ID": "1",
                    "ISSUER_CUSIP": "037833100",
                    "BALANCE": "500000",
                    "UNIT": "NS",  # NOTE: required by ingester guard.
                    "CURRENCY_CODE": "USD",
                    "CURRENCY_VALUE": "75000000",
                    "PAYOFF_PROFILE": "Long",
                    "ASSET_CAT": "EC",
                },
            ],
        )
        archive_path = tmp_path / "nport.zip"
        archive_path.write_bytes(archive_bytes)

        result = ingest_nport_dataset_archive(
            conn=ebull_test_conn,
            archive_path=archive_path,
            ingest_run_id=uuid4(),
        )
        ebull_test_conn.commit()

        assert isinstance(result, NPortIngestResult)
        assert result.holdings_seen == 1
        assert result.rows_written == 1

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                """
                SELECT fund_series_id, fund_series_name, fund_filer_cik,
                       shares, market_value_usd, period_end, source
                FROM ownership_funds_observations
                WHERE instrument_id = %s
                """,
                (iid,),
            )
            row = cur.fetchone()
            assert row is not None
            (sid, sname, fcik, shares, mv, period, source) = row
            assert sid == "S000004310"
            assert sname == "Big Fund Equity Series"
            assert fcik == "0001234567"
            assert shares == Decimal("500000")
            assert mv == Decimal("75000000.00")
            assert period.isoformat() == "2025-09-30"
            assert source == "nport"

    def test_non_ec_asset_skipped(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
        tmp_path: Path,
    ) -> None:
        _seed_universe_with_cusip(ebull_test_conn, symbol="AAPL", cusip="037833100")
        archive_bytes = _build_dataset_zip(
            submissions=[{"ACCESSION_NUMBER": "ACCN_DBT", "FILING_DATE": "2025-11-30", "REPORT_DATE": "2025-09-30"}],
            registrants=[{"ACCESSION_NUMBER": "ACCN_DBT", "CIK": "1234567", "REGISTRANT_NAME": "X"}],
            fund_info=[{"ACCESSION_NUMBER": "ACCN_DBT", "SERIES_ID": "S000004310", "SERIES_NAME": "X"}],
            holdings=[
                {
                    "ACCESSION_NUMBER": "ACCN_DBT",
                    "HOLDING_ID": "1",
                    "ISSUER_CUSIP": "037833100",
                    "BALANCE": "1",
                    "PAYOFF_PROFILE": "Long",
                    "ASSET_CAT": "DBT",  # debt, not equity
                },
            ],
        )
        archive_path = tmp_path / "nport.zip"
        archive_path.write_bytes(archive_bytes)

        result = ingest_nport_dataset_archive(
            conn=ebull_test_conn,
            archive_path=archive_path,
            ingest_run_id=uuid4(),
        )
        ebull_test_conn.commit()

        assert result.rows_written == 0
        assert result.rows_skipped_non_equity == 1

    def test_short_position_skipped(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
        tmp_path: Path,
    ) -> None:
        _seed_universe_with_cusip(ebull_test_conn, symbol="AAPL", cusip="037833100")
        archive_bytes = _build_dataset_zip(
            submissions=[{"ACCESSION_NUMBER": "ACCN_S", "FILING_DATE": "2025-11-30", "REPORT_DATE": "2025-09-30"}],
            registrants=[{"ACCESSION_NUMBER": "ACCN_S", "CIK": "1234567", "REGISTRANT_NAME": "X"}],
            fund_info=[{"ACCESSION_NUMBER": "ACCN_S", "SERIES_ID": "S000004310", "SERIES_NAME": "X"}],
            holdings=[
                {
                    "ACCESSION_NUMBER": "ACCN_S",
                    "HOLDING_ID": "1",
                    "ISSUER_CUSIP": "037833100",
                    "BALANCE": "1",
                    "PAYOFF_PROFILE": "Short",
                    "ASSET_CAT": "EC",
                },
            ],
        )
        archive_path = tmp_path / "nport.zip"
        archive_path.write_bytes(archive_bytes)

        result = ingest_nport_dataset_archive(
            conn=ebull_test_conn,
            archive_path=archive_path,
            ingest_run_id=uuid4(),
        )
        ebull_test_conn.commit()

        assert result.rows_written == 0
        assert result.rows_skipped_non_long == 1

    def test_non_share_unit_skipped(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
        tmp_path: Path,
    ) -> None:
        # A convertible bond with UNIT='PA' (principal amount) but
        # categorised as EC + Long would silently land as shares
        # without the units guard.
        _seed_universe_with_cusip(ebull_test_conn, symbol="AAPL", cusip="037833100")
        archive_bytes = _build_dataset_zip(
            submissions=[{"ACCESSION_NUMBER": "ACCN_PA", "FILING_DATE": "2025-11-30", "REPORT_DATE": "2025-09-30"}],
            registrants=[{"ACCESSION_NUMBER": "ACCN_PA", "CIK": "1234567", "REGISTRANT_NAME": "X"}],
            fund_info=[{"ACCESSION_NUMBER": "ACCN_PA", "SERIES_ID": "S000004310", "SERIES_NAME": "X"}],
            holdings=[
                {
                    "ACCESSION_NUMBER": "ACCN_PA",
                    "HOLDING_ID": "1",
                    "ISSUER_CUSIP": "037833100",
                    "BALANCE": "100000",
                    "UNIT": "PA",  # principal amount, not shares
                    "PAYOFF_PROFILE": "Long",
                    "ASSET_CAT": "EC",
                },
            ],
        )
        archive_path = tmp_path / "nport.zip"
        archive_path.write_bytes(archive_bytes)
        result = ingest_nport_dataset_archive(
            conn=ebull_test_conn,
            archive_path=archive_path,
            ingest_run_id=uuid4(),
        )
        ebull_test_conn.commit()
        assert result.rows_written == 0
        assert result.rows_skipped_non_share_units == 1

    def test_unresolved_cusip_skipped(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
        tmp_path: Path,
    ) -> None:
        # Universe has only AAPL but the dataset references a different CUSIP.
        _seed_universe_with_cusip(ebull_test_conn, symbol="AAPL", cusip="037833100")
        archive_bytes = _build_dataset_zip(
            submissions=[{"ACCESSION_NUMBER": "ACCN_UN", "FILING_DATE": "2025-11-30", "REPORT_DATE": "2025-09-30"}],
            registrants=[{"ACCESSION_NUMBER": "ACCN_UN", "CIK": "1234567", "REGISTRANT_NAME": "X"}],
            fund_info=[{"ACCESSION_NUMBER": "ACCN_UN", "SERIES_ID": "S000004310", "SERIES_NAME": "X"}],
            holdings=[
                {
                    "ACCESSION_NUMBER": "ACCN_UN",
                    "HOLDING_ID": "1",
                    "ISSUER_CUSIP": "999999999",
                    "BALANCE": "1",
                    "UNIT": "NS",
                    "PAYOFF_PROFILE": "Long",
                    "ASSET_CAT": "EC",
                },
            ],
        )
        archive_path = tmp_path / "nport.zip"
        archive_path.write_bytes(archive_bytes)

        result = ingest_nport_dataset_archive(
            conn=ebull_test_conn,
            archive_path=archive_path,
            ingest_run_id=uuid4(),
        )
        ebull_test_conn.commit()

        assert result.rows_written == 0
        assert result.rows_skipped_unresolved_cusip == 1

    def test_missing_series_id_skipped(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
        tmp_path: Path,
    ) -> None:
        _seed_universe_with_cusip(ebull_test_conn, symbol="AAPL", cusip="037833100")
        archive_bytes = _build_dataset_zip(
            submissions=[{"ACCESSION_NUMBER": "ACCN_NS", "FILING_DATE": "2025-11-30", "REPORT_DATE": "2025-09-30"}],
            registrants=[{"ACCESSION_NUMBER": "ACCN_NS", "CIK": "1234567", "REGISTRANT_NAME": "X"}],
            fund_info=[{"ACCESSION_NUMBER": "ACCN_NS", "SERIES_ID": "", "SERIES_NAME": "X"}],
            holdings=[
                {
                    "ACCESSION_NUMBER": "ACCN_NS",
                    "HOLDING_ID": "1",
                    "ISSUER_CUSIP": "037833100",
                    "BALANCE": "1",
                    "UNIT": "NS",
                    "PAYOFF_PROFILE": "Long",
                    "ASSET_CAT": "EC",
                },
            ],
        )
        archive_path = tmp_path / "nport.zip"
        archive_path.write_bytes(archive_bytes)

        result = ingest_nport_dataset_archive(
            conn=ebull_test_conn,
            archive_path=archive_path,
            ingest_run_id=uuid4(),
        )
        ebull_test_conn.commit()

        assert result.rows_written == 0
        assert result.rows_skipped_missing_series == 1
