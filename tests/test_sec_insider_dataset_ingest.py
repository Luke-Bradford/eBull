"""Tests for the bulk Insider Transactions dataset ingester (#1024)."""

from __future__ import annotations

import csv
import io
import zipfile
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest

from app.services.sec_insider_dataset_ingest import (
    InsiderIngestResult,
    _map_form_to_source,
    _map_relationship,
    ingest_insider_dataset_archive,
)
from tests.fixtures.ebull_test_db import ebull_test_conn
from tests.fixtures.ebull_test_db import test_db_available as _test_db_available

__all__ = ["ebull_test_conn"]


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------


class TestPureHelpers:
    def test_map_form_to_source_form3_and_form4(self) -> None:
        assert _map_form_to_source("3") == "form3"
        assert _map_form_to_source("3/A") == "form3"
        assert _map_form_to_source("4") == "form4"
        assert _map_form_to_source("4/A") == "form4"
        assert _map_form_to_source("5") == "form4"  # Form 5 → form4 priority

    def test_map_relationship_officer_director_direct(self) -> None:
        assert _map_relationship({"RPTOWNER_RELATIONSHIP": "Officer"}) == "direct"
        assert _map_relationship({"IS_DIRECTOR": "1"}) == "direct"

    def test_map_relationship_ten_percent_beneficial(self) -> None:
        assert _map_relationship({"RPTOWNER_RELATIONSHIP": "TenPercentOwner"}) == "beneficial"

    def test_map_relationship_default_direct(self) -> None:
        assert _map_relationship({}) == "direct"


# ---------------------------------------------------------------------------
# Archive fixture
# ---------------------------------------------------------------------------


def _build_dataset_zip(
    *,
    submissions: list[dict[str, str]],
    owners: list[dict[str, str]],
    holdings: list[dict[str, str]],
    transactions: list[dict[str, str]] | None = None,
) -> bytes:
    """Build a tiny ZIP using SEC's documented underscore-free filenames."""

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
        # SEC primary filenames are underscore-free; ingester accepts
        # the underscored fork too for older quarters.
        zf.writestr("REPORTINGOWNER.tsv", _to_tsv(owners))
        zf.writestr("NONDERIV_HOLDING.tsv", _to_tsv(holdings))
        zf.writestr("NONDERIV_TRANS.tsv", _to_tsv(transactions or []))
    return out.getvalue()


# ---------------------------------------------------------------------------
# DB integration
# ---------------------------------------------------------------------------


_NEXT_IID: list[int] = [13000]


def _seed_universe(
    conn: psycopg.Connection[tuple],
    *,
    symbol: str,
    cik_padded: str,
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
            "VALUES (%s, 'sec', 'cik', %s, TRUE)",
            (iid, cik_padded),
        )
    conn.commit()
    return iid


@pytest.mark.integration
@pytest.mark.skipif(not _test_db_available(), reason="ebull_test DB unavailable")
class TestIngestInsiderDatasetArchive:
    def test_form4_writes_observation_with_post_trans_shares(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
        tmp_path: Path,
    ) -> None:
        iid = _seed_universe(ebull_test_conn, symbol="AAPL", cik_padded="0000320193")

        archive_bytes = _build_dataset_zip(
            submissions=[
                {
                    "ACCESSION_NUMBER": "ACCN1",
                    "ISSUERCIK": "320193",
                    "DOCUMENT_TYPE": "4",
                    "FILING_DATE": "2025-11-14",
                    "PERIOD_OF_REPORT": "2025-11-12",
                },
            ],
            owners=[
                {
                    "ACCESSION_NUMBER": "ACCN1",
                    "RPTOWNERCIK": "1234567",
                    "RPTOWNERNAME": "Cook Tim",
                    "RPTOWNER_RELATIONSHIP": "Officer",
                },
            ],
            # Form 4: post-trans shares-owned lives on the
            # transaction row (the canonical Form 4 shape per SEC).
            transactions=[
                {
                    "ACCESSION_NUMBER": "ACCN1",
                    "NONDERIV_TRANS_SK": "1",
                    "TRANS_DATE": "2025-11-12",
                    "SHRS_OWND_FOLWNG_TRANS": "3000000",
                },
            ],
            holdings=[],  # unused for normal Form 4 filings
        )
        archive_path = tmp_path / "form345.zip"
        archive_path.write_bytes(archive_bytes)

        result = ingest_insider_dataset_archive(
            conn=ebull_test_conn,
            archive_path=archive_path,
            ingest_run_id=uuid4(),
        )
        ebull_test_conn.commit()

        assert isinstance(result, InsiderIngestResult)
        assert result.rows_written == 1
        assert result.rows_skipped_unresolved_cik == 0

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                """
                SELECT holder_name, source, ownership_nature, shares, period_end, holder_cik
                FROM ownership_insiders_observations
                WHERE instrument_id = %s
                """,
                (iid,),
            )
            row = cur.fetchone()
            assert row is not None
            (holder_name, source, nature, shares, period_end, holder_cik) = row
            assert holder_name == "Cook Tim"
            assert source == "form4"
            assert nature == "direct"
            assert shares == Decimal("3000000.0000")
            assert period_end.isoformat() == "2025-11-12"
            assert holder_cik == "0001234567"

    def test_form3_uses_period_of_report_as_period_end(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
        tmp_path: Path,
    ) -> None:
        iid = _seed_universe(ebull_test_conn, symbol="AAPL", cik_padded="0000320193")

        archive_bytes = _build_dataset_zip(
            submissions=[
                {
                    "ACCESSION_NUMBER": "ACCN_F3",
                    "ISSUERCIK": "320193",
                    "DOCUMENT_TYPE": "3",
                    "FILING_DATE": "2025-10-01",
                    "PERIOD_OF_REPORT": "2025-09-30",
                },
            ],
            owners=[
                {
                    "ACCESSION_NUMBER": "ACCN_F3",
                    "RPTOWNERCIK": "1234567",
                    "RPTOWNERNAME": "Founder Eve",
                    "IS_DIRECTOR": "1",
                },
            ],
            holdings=[
                {
                    "ACCESSION_NUMBER": "ACCN_F3",
                    "SHRS_OWND_FOLWNG_TRANS": "100000",
                },
            ],
        )
        archive_path = tmp_path / "form345.zip"
        archive_path.write_bytes(archive_bytes)

        result = ingest_insider_dataset_archive(
            conn=ebull_test_conn,
            archive_path=archive_path,
            ingest_run_id=uuid4(),
        )
        ebull_test_conn.commit()

        assert result.rows_written == 1
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT source, period_end FROM ownership_insiders_observations WHERE instrument_id = %s",
                (iid,),
            )
            row = cur.fetchone()
            assert row is not None
            assert row[0] == "form3"
            assert row[1].isoformat() == "2025-09-30"

    def test_filing_date_dd_mon_yyyy_format_parses(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
        tmp_path: Path,
    ) -> None:
        # SEC publishes FILING_DATE as DD-MON-YYYY (e.g. 14-NOV-2025).
        iid = _seed_universe(ebull_test_conn, symbol="AAPL", cik_padded="0000320193")
        archive_bytes = _build_dataset_zip(
            submissions=[
                {
                    "ACCESSION_NUMBER": "ACCN_DD",
                    "ISSUERCIK": "320193",
                    "DOCUMENT_TYPE": "4",
                    "FILING_DATE": "14-NOV-2025",  # dataset format
                    "PERIOD_OF_REPORT": "12-NOV-2025",
                },
            ],
            owners=[
                {
                    "ACCESSION_NUMBER": "ACCN_DD",
                    "RPTOWNERCIK": "1234567",
                    "RPTOWNERNAME": "Insider X",
                    "RPTOWNER_RELATIONSHIP": "Officer",
                },
            ],
            transactions=[
                {
                    "ACCESSION_NUMBER": "ACCN_DD",
                    "NONDERIV_TRANS_SK": "1",
                    "TRANS_DATE": "12-NOV-2025",
                    "SHRS_OWND_FOLWNG_TRANS": "100",
                },
            ],
            holdings=[],
        )
        archive_path = tmp_path / "form345.zip"
        archive_path.write_bytes(archive_bytes)

        result = ingest_insider_dataset_archive(
            conn=ebull_test_conn,
            archive_path=archive_path,
            ingest_run_id=uuid4(),
        )
        ebull_test_conn.commit()

        assert result.rows_written == 1
        assert result.rows_skipped_bad_data == 0
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT period_end FROM ownership_insiders_observations WHERE instrument_id = %s",
                (iid,),
            )
            row = cur.fetchone()
            assert row is not None
            assert row[0].isoformat() == "2025-11-12"

    def test_multiple_transactions_per_accession_do_not_collide_on_pk(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
        tmp_path: Path,
    ) -> None:
        # Same accession + owner + nature + period, two distinct
        # transaction rows (separate trades on the same day) → both
        # must persist via NONDERIV_TRANS_SK in source_document_id.
        iid = _seed_universe(ebull_test_conn, symbol="AAPL", cik_padded="0000320193")
        archive_bytes = _build_dataset_zip(
            submissions=[
                {
                    "ACCESSION_NUMBER": "ACCN_DUP",
                    "ISSUERCIK": "320193",
                    "DOCUMENT_TYPE": "4",
                    "FILING_DATE": "14-NOV-2025",
                },
            ],
            owners=[
                {
                    "ACCESSION_NUMBER": "ACCN_DUP",
                    "RPTOWNERCIK": "1234567",
                    "RPTOWNERNAME": "Insider Y",
                    "RPTOWNER_RELATIONSHIP": "Officer",
                },
            ],
            transactions=[
                {
                    "ACCESSION_NUMBER": "ACCN_DUP",
                    "NONDERIV_TRANS_SK": "100",
                    "TRANS_DATE": "12-NOV-2025",
                    "SHRS_OWND_FOLWNG_TRANS": "10",
                },
                {
                    "ACCESSION_NUMBER": "ACCN_DUP",
                    "NONDERIV_TRANS_SK": "200",
                    "TRANS_DATE": "12-NOV-2025",
                    "SHRS_OWND_FOLWNG_TRANS": "20",
                },
            ],
            holdings=[],
        )
        archive_path = tmp_path / "form345.zip"
        archive_path.write_bytes(archive_bytes)

        result = ingest_insider_dataset_archive(
            conn=ebull_test_conn,
            archive_path=archive_path,
            ingest_run_id=uuid4(),
        )
        ebull_test_conn.commit()

        assert result.rows_written == 2
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM ownership_insiders_observations WHERE instrument_id = %s",
                (iid,),
            )
            row = cur.fetchone()
            assert row is not None
            assert row[0] == 2

    def test_unresolved_issuer_cik_skipped(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
        tmp_path: Path,
    ) -> None:
        # Universe has only AAPL; the dataset references an issuer CIK we do NOT track.
        _seed_universe(ebull_test_conn, symbol="AAPL", cik_padded="0000320193")
        archive_bytes = _build_dataset_zip(
            submissions=[
                {"ACCESSION_NUMBER": "X", "ISSUERCIK": "9999999", "DOCUMENT_TYPE": "4", "FILING_DATE": "2025-11-14"}
            ],
            owners=[
                {"ACCESSION_NUMBER": "X", "RPTOWNERCIK": "1", "RPTOWNERNAME": "X", "RPTOWNER_RELATIONSHIP": "Officer"}
            ],
            holdings=[],
            transactions=[
                {
                    "ACCESSION_NUMBER": "X",
                    "NONDERIV_TRANS_SK": "1",
                    "TRANS_DATE": "2025-11-13",
                    "SHRS_OWND_FOLWNG_TRANS": "1",
                }
            ],
        )
        archive_path = tmp_path / "form345.zip"
        archive_path.write_bytes(archive_bytes)

        result = ingest_insider_dataset_archive(
            conn=ebull_test_conn,
            archive_path=archive_path,
            ingest_run_id=uuid4(),
        )
        ebull_test_conn.commit()

        assert result.rows_written == 0
        assert result.rows_skipped_unresolved_cik == 1
