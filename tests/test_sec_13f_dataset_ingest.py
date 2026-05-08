"""Tests for the bulk Form 13F dataset ingester (#1023)."""

from __future__ import annotations

import io
import zipfile
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest

from app.services.sec_13f_dataset_ingest import (
    Form13FIngestResult,
    _map_putcall,
    _map_voting_authority,
    _parse_decimal,
    _parse_filing_date,
    _parse_period_end,
    ingest_13f_dataset_archive,
)
from tests.fixtures.ebull_test_db import ebull_test_conn
from tests.fixtures.ebull_test_db import test_db_available as _test_db_available

__all__ = ["ebull_test_conn"]


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------


class TestPureHelpers:
    def test_map_putcall_default_equity(self) -> None:
        assert _map_putcall(None) == "EQUITY"
        assert _map_putcall("") == "EQUITY"
        assert _map_putcall("PUT") == "PUT"
        assert _map_putcall("Put") == "PUT"
        assert _map_putcall("CALL") == "CALL"

    def test_map_voting_authority_priority_chain(self) -> None:
        assert _map_voting_authority({"VOTING_AUTH_SOLE": "100", "VOTING_AUTH_SHARED": "0"}) == "SOLE"
        assert _map_voting_authority({"VOTING_AUTH_SOLE": "0", "VOTING_AUTH_SHARED": "50"}) == "SHARED"
        assert _map_voting_authority({"VOTING_AUTH_NONE": "10"}) == "NONE"
        assert _map_voting_authority({}) is None

    def test_parse_decimal_handles_empty_strings(self) -> None:
        assert _parse_decimal(None) is None
        assert _parse_decimal("") is None
        assert _parse_decimal("   ") is None
        assert _parse_decimal("123.45") == Decimal("123.45")
        assert _parse_decimal("not a number") is None

    def test_parse_filing_date_iso_and_short(self) -> None:
        assert _parse_filing_date("2025-11-01") is not None
        assert _parse_filing_date("2025-11-01T00:00:00") is not None
        assert _parse_filing_date(None) is None

    def test_parse_period_end_iso_and_dmmy(self) -> None:
        assert _parse_period_end("2025-09-30") is not None
        assert _parse_period_end("30-Sep-2025") is not None


# ---------------------------------------------------------------------------
# Fixture builders
# ---------------------------------------------------------------------------


def _build_dataset_zip(
    *,
    submissions: list[dict[str, str]],
    coverpages: list[dict[str, str]],
    infotable: list[dict[str, str]],
) -> bytes:
    """Build a tiny in-memory 13F dataset ZIP."""
    import csv

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
        zf.writestr("COVERPAGE.tsv", _to_tsv(coverpages))
        zf.writestr("INFOTABLE.tsv", _to_tsv(infotable))
    return out.getvalue()


# ---------------------------------------------------------------------------
# DB integration
# ---------------------------------------------------------------------------


_NEXT_IID: list[int] = [12000]


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
class TestIngest13FDatasetArchive:
    def test_resolved_cusip_writes_observation_with_correct_fields(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
        tmp_path: Path,
    ) -> None:
        # AAPL CUSIP — 037833100.
        iid = _seed_universe_with_cusip(ebull_test_conn, symbol="AAPL", cusip="037833100")

        archive_bytes = _build_dataset_zip(
            submissions=[
                {
                    "ACCESSION_NUMBER": "0001234567-25-000001",
                    "CIK": "1234567",
                    "FILING_DATE": "2025-11-14",
                },
            ],
            coverpages=[
                {
                    "ACCESSION_NUMBER": "0001234567-25-000001",
                    "FILINGMANAGER_NAME": "Big Fund LLC",
                    "REPORTCALENDARORQUARTER": "2025-09-30",
                },
            ],
            infotable=[
                {
                    "ACCESSION_NUMBER": "0001234567-25-000001",
                    "CUSIP": "037833100",
                    "VALUE": "5000000",  # $thousands
                    "SSHPRNAMT": "100000",
                    "VOTING_AUTH_SOLE": "100000",
                    "VOTING_AUTH_SHARED": "0",
                    "VOTING_AUTH_NONE": "0",
                    "PUTCALL": "",
                },
            ],
        )
        archive_path = tmp_path / "form13f.zip"
        archive_path.write_bytes(archive_bytes)

        result = ingest_13f_dataset_archive(
            conn=ebull_test_conn,
            archive_path=archive_path,
            ingest_run_id=uuid4(),
        )
        ebull_test_conn.commit()

        assert isinstance(result, Form13FIngestResult)
        assert result.infotable_seen == 1
        assert result.rows_written == 1
        assert result.rows_skipped_unresolved_cusip == 0

        # Verify the observation row.
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                """
                SELECT filer_cik, filer_name, ownership_nature, source,
                       shares, market_value_usd, voting_authority, exposure_kind, period_end
                FROM ownership_institutions_observations
                WHERE instrument_id = %s
                """,
                (iid,),
            )
            row = cur.fetchone()
            assert row is not None
            (filer_cik, filer_name, nature, source, shares, mv, voting, exposure, period) = row
            assert filer_cik == "0001234567"
            assert filer_name == "Big Fund LLC"
            assert nature == "economic"
            assert source == "13f"
            assert shares == Decimal("100000.0000")
            # 5_000_000 thousands = 5_000_000_000 USD.
            assert mv == Decimal("5000000000.00")
            assert voting == "SOLE"
            assert exposure == "EQUITY"
            assert period.isoformat() == "2025-09-30"

    def test_unresolved_cusip_skipped_not_written(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
        tmp_path: Path,
    ) -> None:
        # Seed AAPL but the dataset references a different CUSIP.
        _seed_universe_with_cusip(ebull_test_conn, symbol="AAPL", cusip="037833100")

        archive_bytes = _build_dataset_zip(
            submissions=[{"ACCESSION_NUMBER": "ACCN1", "CIK": "1", "FILING_DATE": "2025-11-14"}],
            coverpages=[
                {
                    "ACCESSION_NUMBER": "ACCN1",
                    "FILINGMANAGER_NAME": "Some Fund",
                    "REPORTCALENDARORQUARTER": "2025-09-30",
                }
            ],
            infotable=[
                {"ACCESSION_NUMBER": "ACCN1", "CUSIP": "999999999", "VALUE": "1", "SSHPRNAMT": "1"},
            ],
        )
        archive_path = tmp_path / "form13f.zip"
        archive_path.write_bytes(archive_bytes)

        result = ingest_13f_dataset_archive(
            conn=ebull_test_conn,
            archive_path=archive_path,
            ingest_run_id=uuid4(),
        )
        ebull_test_conn.commit()

        assert result.rows_written == 0
        assert result.rows_skipped_unresolved_cusip == 1

    def test_putcall_split_writes_three_rows(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
        tmp_path: Path,
    ) -> None:
        # The 13F schema allows up to 3 rows per (accession, instrument):
        # equity + PUT + CALL via exposure_kind.
        iid = _seed_universe_with_cusip(ebull_test_conn, symbol="AAPL", cusip="037833100")

        archive_bytes = _build_dataset_zip(
            submissions=[{"ACCESSION_NUMBER": "ACCN1", "CIK": "1", "FILING_DATE": "2025-11-14"}],
            coverpages=[
                {
                    "ACCESSION_NUMBER": "ACCN1",
                    "FILINGMANAGER_NAME": "Some Fund",
                    "REPORTCALENDARORQUARTER": "2025-09-30",
                }
            ],
            infotable=[
                {
                    "ACCESSION_NUMBER": "ACCN1",
                    "CUSIP": "037833100",
                    "VALUE": "100",
                    "SSHPRNAMT": "10",
                    "PUTCALL": "",
                },
                {
                    "ACCESSION_NUMBER": "ACCN1",
                    "CUSIP": "037833100",
                    "VALUE": "50",
                    "SSHPRNAMT": "5",
                    "PUTCALL": "PUT",
                },
                {
                    "ACCESSION_NUMBER": "ACCN1",
                    "CUSIP": "037833100",
                    "VALUE": "75",
                    "SSHPRNAMT": "7",
                    "PUTCALL": "CALL",
                },
            ],
        )
        archive_path = tmp_path / "form13f.zip"
        archive_path.write_bytes(archive_bytes)

        result = ingest_13f_dataset_archive(
            conn=ebull_test_conn,
            archive_path=archive_path,
            ingest_run_id=uuid4(),
        )
        ebull_test_conn.commit()

        assert result.rows_written == 3
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT exposure_kind FROM ownership_institutions_observations "
                "WHERE instrument_id = %s ORDER BY exposure_kind",
                (iid,),
            )
            kinds = [r[0] for r in cur.fetchall()]
            assert kinds == ["CALL", "EQUITY", "PUT"]
