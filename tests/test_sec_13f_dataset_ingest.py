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
    _FIGI_RE,
    Form13FIngestResult,
    _map_putcall,
    _parse_decimal,
    _parse_filing_date,
    _parse_period_end,
    _persist_figi_external_identifiers,
    _read_voting_components,
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

    def test_read_voting_components_returns_raw_amounts(self) -> None:
        # #1567 — the drain SUMs the raw sub-amounts then derives the label,
        # so the helper now returns the three components (missing/blank → 0).
        assert _read_voting_components({"VOTING_AUTH_SOLE": "100", "VOTING_AUTH_SHARED": "0"}) == (
            Decimal(100),
            Decimal(0),
            Decimal(0),
        )
        assert _read_voting_components({"VOTING_AUTHORITY_SHARED": "50"}) == (
            Decimal(0),
            Decimal(50),
            Decimal(0),
        )
        assert _read_voting_components({"VOTING_AUTH_NONE": "10"}) == (Decimal(0), Decimal(0), Decimal(10))
        assert _read_voting_components({}) == (Decimal(0), Decimal(0), Decimal(0))

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
            # Post-2023-01-03 SEC reports VALUE in dollars (not
            # thousands) — period_end here is 2025-09-30 so no
            # multiplier applied. SEC FORM13F_metadata.json column
            # description: "Starting on January 3, 2023, market value
            # is reported rounded to the nearest dollar."
            assert mv == Decimal("5000000.00")
            assert voting == "SOLE"
            assert exposure == "EQUITY"
            assert period.isoformat() == "2025-09-30"

    def test_multi_submanager_rows_summed_and_prn_dropped(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
        tmp_path: Path,
    ) -> None:
        # #1567 — one AAPL position split across 3 sub-manager rows must SUM
        # to a single observation (was keep-last). #1566 — the PRN row is
        # bond principal, dropped (not summed into shares).
        iid = _seed_universe_with_cusip(ebull_test_conn, symbol="AAPL", cusip="037833100")
        common = {"ACCESSION_NUMBER": "0001234567-25-000002", "CUSIP": "037833100", "PUTCALL": ""}
        archive_bytes = _build_dataset_zip(
            submissions=[{"ACCESSION_NUMBER": "0001234567-25-000002", "CIK": "1234567", "FILING_DATE": "2025-11-14"}],
            coverpages=[
                {
                    "ACCESSION_NUMBER": "0001234567-25-000002",
                    "FILINGMANAGER_NAME": "Split Fund LLC",
                    "REPORTCALENDARORQUARTER": "2025-09-30",
                }
            ],
            infotable=[
                {**common, "VALUE": "10", "SSHPRNAMT": "100", "VOTING_AUTH_SOLE": "100"},
                {**common, "VALUE": "25", "SSHPRNAMT": "250", "VOTING_AUTH_SHARED": "250"},
                {**common, "VALUE": "1", "SSHPRNAMT": "3", "VOTING_AUTH_NONE": "3"},
                {**common, "VALUE": "999", "SSHPRNAMT": "999", "SSHPRNAMTTYPE": "PRN"},
            ],
        )
        archive_path = tmp_path / "form13f.zip"
        archive_path.write_bytes(archive_bytes)

        result = ingest_13f_dataset_archive(conn=ebull_test_conn, archive_path=archive_path, ingest_run_id=uuid4())
        ebull_test_conn.commit()

        assert result.infotable_seen == 4
        assert result.rows_skipped_bad_data == 1  # the PRN row

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT shares, market_value_usd, voting_authority "
                "FROM ownership_institutions_observations WHERE instrument_id = %s",
                (iid,),
            )
            rows = cur.fetchall()
        assert len(rows) == 1, "sub-manager split rows must collapse to one summed observation"
        shares, mv, voting = rows[0]
        assert shares == Decimal("353.0000")  # 100 + 250 + 3 (PRN's 999 excluded)
        assert mv == Decimal("36.00")  # 10 + 25 + 1
        assert voting == "SHARED"  # summed sole=100 / shared=250 / none=3 → SHARED dominates

    def test_unresolved_cusip_skipped_not_written(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
        tmp_path: Path,
    ) -> None:
        # Seed AAPL but the dataset references a different CUSIP.
        _seed_universe_with_cusip(ebull_test_conn, symbol="AAPL", cusip="037833100")

        archive_bytes = _build_dataset_zip(
            submissions=[{"ACCESSION_NUMBER": "0001234567-25-000001", "CIK": "1", "FILING_DATE": "2025-11-14"}],
            coverpages=[
                {
                    "ACCESSION_NUMBER": "0001234567-25-000001",
                    "FILINGMANAGER_NAME": "Some Fund",
                    "REPORTCALENDARORQUARTER": "2025-09-30",
                }
            ],
            infotable=[
                {"ACCESSION_NUMBER": "0001234567-25-000001", "CUSIP": "999999999", "VALUE": "1", "SSHPRNAMT": "1"},
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
            submissions=[{"ACCESSION_NUMBER": "0001234567-25-000001", "CIK": "1", "FILING_DATE": "2025-11-14"}],
            coverpages=[
                {
                    "ACCESSION_NUMBER": "0001234567-25-000001",
                    "FILINGMANAGER_NAME": "Some Fund",
                    "REPORTCALENDARORQUARTER": "2025-09-30",
                }
            ],
            infotable=[
                {
                    "ACCESSION_NUMBER": "0001234567-25-000001",
                    "CUSIP": "037833100",
                    "VALUE": "100",
                    "SSHPRNAMT": "10",
                    "PUTCALL": "",
                },
                {
                    "ACCESSION_NUMBER": "0001234567-25-000001",
                    "CUSIP": "037833100",
                    "VALUE": "50",
                    "SSHPRNAMT": "5",
                    "PUTCALL": "PUT",
                },
                {
                    "ACCESSION_NUMBER": "0001234567-25-000001",
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


@pytest.mark.integration
@pytest.mark.skipif(not _test_db_available(), reason="ebull_test DB unavailable")
class TestRealArchiveEdgeCases:
    """Pin behaviour against real-archive edge cases discovered
    2026-05-08 by ingesting form13f_01dec2025-28feb2026.zip
    end-to-end (#1054)."""

    def test_dd_mmm_yyyy_filing_date_format_parses(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
        tmp_path: Path,
    ) -> None:
        # Real SEC 13F dataset emits FILING_DATE as DD-MMM-YYYY
        # ('31-DEC-2025'), NOT ISO. Pre-fix every row was rejected
        # as bad_data — verified end-to-end produced 0 rows_written.
        _seed_universe_with_cusip(ebull_test_conn, symbol="AAPL", cusip="037833100")
        archive_bytes = _build_dataset_zip(
            submissions=[
                {
                    "ACCESSION_NUMBER": "0001234567-25-000001",
                    "CIK": "1234567",
                    "FILING_DATE": "31-DEC-2025",  # SEC dataset format
                },
            ],
            coverpages=[
                {
                    "ACCESSION_NUMBER": "0001234567-25-000001",
                    "FILINGMANAGER_NAME": "Big Fund LLC",
                    "REPORTCALENDARORQUARTER": "30-SEP-2025",
                },
            ],
            infotable=[
                {
                    "ACCESSION_NUMBER": "0001234567-25-000001",
                    "CUSIP": "037833100",
                    "VALUE": "1000",
                    "SSHPRNAMT": "100",
                    "SSHPRNAMTTYPE": "SH",
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
        assert result.rows_written == 1, f"expected 1, got {result}"

    def test_prn_rows_skipped_as_bad_data(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
        tmp_path: Path,
    ) -> None:
        # SSHPRNAMT carries shares (SH) OR principal-amount (PRN)
        # depending on SSHPRNAMTTYPE. Real archive 2026Q1 had 20k
        # PRN rows. Without filter they'd get stored as shares —
        # silent data corruption.
        _seed_universe_with_cusip(ebull_test_conn, symbol="AAPL", cusip="037833100")
        archive_bytes = _build_dataset_zip(
            submissions=[
                {"ACCESSION_NUMBER": "0001234567-25-000001", "CIK": "1", "FILING_DATE": "2025-11-14"},
            ],
            coverpages=[
                {
                    "ACCESSION_NUMBER": "0001234567-25-000001",
                    "FILINGMANAGER_NAME": "Bond Fund",
                    "REPORTCALENDARORQUARTER": "2025-09-30",
                },
            ],
            infotable=[
                {
                    "ACCESSION_NUMBER": "0001234567-25-000001",
                    "CUSIP": "037833100",
                    "VALUE": "1000",
                    "SSHPRNAMT": "1000000",
                    "SSHPRNAMTTYPE": "PRN",  # bond principal — must skip
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
        assert result.rows_written == 0
        assert result.rows_skipped_bad_data == 1

    @pytest.mark.parametrize("bad_shares", ["0", "0.0", "-5", "-100000", ""])
    def test_non_positive_or_null_shares_skipped(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
        tmp_path: Path,
        bad_shares: str,
    ) -> None:
        # #1433 — an SH-type holding with NULL / 0 / negative SSHPRNAMT is
        # malformed and must not be summed into the ownership rollup.
        _seed_universe_with_cusip(ebull_test_conn, symbol="AAPL", cusip="037833100")
        archive_bytes = _build_dataset_zip(
            submissions=[
                {"ACCESSION_NUMBER": "0001234567-25-000001", "CIK": "1", "FILING_DATE": "2025-11-14"},
            ],
            coverpages=[
                {
                    "ACCESSION_NUMBER": "0001234567-25-000001",
                    "FILINGMANAGER_NAME": "Mgr",
                    "REPORTCALENDARORQUARTER": "2025-09-30",
                },
            ],
            infotable=[
                {
                    "ACCESSION_NUMBER": "0001234567-25-000001",
                    "CUSIP": "037833100",
                    "VALUE": "1000",
                    "SSHPRNAMT": bad_shares,
                    "SSHPRNAMTTYPE": "SH",
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
        assert result.rows_written == 0
        assert result.rows_skipped_bad_data == 1

    @pytest.mark.parametrize("bad_period", ["6016-09-30", "1899-12-31", "2100-01-01"])
    def test_out_of_window_period_end_skipped(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
        tmp_path: Path,
        bad_period: str,
    ) -> None:
        # #1433 — a period_end outside [1900, 2100) (parser-bug year-6016,
        # pre-1900, or the exclusive 2100 ceiling) must be skipped before it
        # reaches the DEFAULT partition.
        _seed_universe_with_cusip(ebull_test_conn, symbol="AAPL", cusip="037833100")
        archive_bytes = _build_dataset_zip(
            submissions=[
                {"ACCESSION_NUMBER": "0001234567-25-000001", "CIK": "1", "FILING_DATE": "2025-11-14"},
            ],
            coverpages=[
                {
                    "ACCESSION_NUMBER": "0001234567-25-000001",
                    "FILINGMANAGER_NAME": "Mgr",
                    "REPORTCALENDARORQUARTER": bad_period,
                },
            ],
            infotable=[
                {
                    "ACCESSION_NUMBER": "0001234567-25-000001",
                    "CUSIP": "037833100",
                    "VALUE": "1000",
                    "SSHPRNAMT": "100000",
                    "SSHPRNAMTTYPE": "SH",
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
        assert result.rows_written == 0
        assert result.rows_skipped_bad_data == 1

    def test_value_pre_2023_cutover_multiplied_by_thousands(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
        tmp_path: Path,
    ) -> None:
        # Pre-2023-01-03 SEC reported VALUE in $thousands.
        # Post-cutover in dollars. SEC FORM13F_metadata.json:
        # "Starting on January 3, 2023, market value is reported
        # rounded to the nearest dollar."
        iid = _seed_universe_with_cusip(ebull_test_conn, symbol="AAPL", cusip="037833100")
        archive_bytes = _build_dataset_zip(
            submissions=[
                {"ACCESSION_NUMBER": "0001234567-22-000001", "CIK": "1", "FILING_DATE": "2022-12-15"},
            ],
            coverpages=[
                {
                    "ACCESSION_NUMBER": "0001234567-22-000001",
                    "FILINGMANAGER_NAME": "Old Fund",
                    "REPORTCALENDARORQUARTER": "2022-09-30",  # pre-cutover
                },
            ],
            infotable=[
                {
                    "ACCESSION_NUMBER": "0001234567-22-000001",
                    "CUSIP": "037833100",
                    "VALUE": "5000000",  # $thousands → 5B dollars
                    "SSHPRNAMT": "100000",
                    "SSHPRNAMTTYPE": "SH",
                },
            ],
        )
        archive_path = tmp_path / "form13f.zip"
        archive_path.write_bytes(archive_bytes)
        # PR6 #1233 §4.5 — pin ``now`` to early 2023 so the
        # ``thirteen_f_retention_cutoff`` admits the 2022-Q3 period
        # under test. The 8q cap is exercised separately in
        # ``tests/test_thirteen_f_retention_cap.py``; this test focuses
        # on the VALUE-cutover scaling.
        from datetime import UTC, datetime
        from unittest.mock import patch

        historical_now = datetime(2023, 1, 15, 12, 0, 0, tzinfo=UTC)
        with patch("app.services.institutional_holdings.datetime") as mock_dt:
            mock_dt.now.return_value = historical_now
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            result = ingest_13f_dataset_archive(
                conn=ebull_test_conn,
                archive_path=archive_path,
                ingest_run_id=uuid4(),
            )
        ebull_test_conn.commit()
        assert result.rows_written == 1
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT market_value_usd FROM ownership_institutions_observations WHERE instrument_id=%s",
                (iid,),
            )
            row = cur.fetchone()
            assert row is not None
            mv = row[0]
        # 5,000,000 thousands = 5B USD (pre-2023 multiplier applied).
        assert mv == Decimal("5000000000.00")


# ---------------------------------------------------------------------------
# #1302 — FIGI capture (the 13F INFOTABLE column added 2023-01-03; NOT LEI)
# ---------------------------------------------------------------------------


class TestFigiRegex:
    def test_accepts_valid_12char_figi(self) -> None:
        assert _FIGI_RE.match("BBG000B9XRY4")

    def test_rejects_wrong_length(self) -> None:
        assert _FIGI_RE.match("BBG000B9XRY") is None  # 11
        assert _FIGI_RE.match("BBG000B9XRY44") is None  # 13

    def test_rejects_lowercase_and_symbols(self) -> None:
        assert _FIGI_RE.match("bbg000b9xry4") is None
        assert _FIGI_RE.match("BBG000B9-RY4") is None


@pytest.mark.integration
@pytest.mark.skipif(not _test_db_available(), reason="ebull_test DB unavailable")
class TestPersistFigiExternalIdentifiers:
    def _seed_iid(self, conn: psycopg.Connection[tuple], symbol: str, cusip: str) -> int:
        return _seed_universe_with_cusip(conn, symbol=symbol, cusip=cusip)

    def test_inserts_new_figi_mapping(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        iid = self._seed_iid(ebull_test_conn, "FOO", "111111111")
        result = Form13FIngestResult()
        _persist_figi_external_identifiers(ebull_test_conn, {"BBG000B9XRY4": iid}, result=result)
        ebull_test_conn.commit()
        assert result.figi_identifiers_seen == 1
        assert result.figi_identifiers_written == 1
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT instrument_id, is_primary FROM external_identifiers "
                "WHERE provider='sec' AND identifier_type='figi' AND identifier_value='BBG000B9XRY4'"
            )
            row = cur.fetchone()
        assert row is not None
        assert row[0] == iid
        assert row[1] is False  # never claims primary

    def test_multiple_distinct_figis_counted_cumulatively(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """``figi_identifiers_written`` is the CUMULATIVE insert count across
        the whole ``executemany`` batch, not the last statement's rowcount.

        Empirically verified: psycopg 3.3.3 accumulates ``cur.rowcount`` over a
        non-returning ``executemany`` (3 distinct ON CONFLICT DO NOTHING
        inserts → rowcount 3; 1 conflict + 1 new → rowcount 1). This test pins
        that invariant so a driver-version bump that changes the semantic is
        caught here (rebuts the review WARNING claiming it reflects only the
        last statement)."""
        a = self._seed_iid(ebull_test_conn, "MUL1", "444444444")
        b = self._seed_iid(ebull_test_conn, "MUL2", "555555555")
        c = self._seed_iid(ebull_test_conn, "MUL3", "666666666")
        result = Form13FIngestResult()
        _persist_figi_external_identifiers(
            ebull_test_conn,
            {"BBG00000MUL1": a, "BBG00000MUL2": b, "BBG00000MUL3": c},
            result=result,
        )
        ebull_test_conn.commit()
        assert result.figi_identifiers_seen == 3
        assert result.figi_identifiers_written == 3  # cumulative, not 1

    def test_do_nothing_on_existing_figi(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """A FIGI already mapped (even to a DIFFERENT instrument) is never
        clobbered — DO NOTHING preserves the existing row."""
        iid_a = self._seed_iid(ebull_test_conn, "AAA", "222222222")
        iid_b = self._seed_iid(ebull_test_conn, "BBB", "333333333")
        r1 = Form13FIngestResult()
        _persist_figi_external_identifiers(ebull_test_conn, {"BBG00000FIG1": iid_a}, result=r1)
        ebull_test_conn.commit()
        # Second persist with the same FIGI pointed at a different instrument.
        r2 = Form13FIngestResult()
        _persist_figi_external_identifiers(ebull_test_conn, {"BBG00000FIG1": iid_b}, result=r2)
        ebull_test_conn.commit()
        assert r2.figi_identifiers_written == 0
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT instrument_id FROM external_identifiers "
                "WHERE provider='sec' AND identifier_type='figi' AND identifier_value='BBG00000FIG1'"
            )
            rows = cur.fetchall()
        assert len(rows) == 1
        assert rows[0][0] == iid_a  # original mapping preserved

    def test_empty_mapping_is_noop(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        result = Form13FIngestResult()
        _persist_figi_external_identifiers(ebull_test_conn, {}, result=result)
        assert result.figi_identifiers_seen == 0
        assert result.figi_identifiers_written == 0


@pytest.mark.integration
@pytest.mark.skipif(not _test_db_available(), reason="ebull_test DB unavailable")
class TestIngest13FFigiCapture:
    def test_figi_column_captured_during_archive_ingest(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
        tmp_path: Path,
    ) -> None:
        iid = _seed_universe_with_cusip(ebull_test_conn, symbol="AAPL", cusip="037833100")
        archive_bytes = _build_dataset_zip(
            submissions=[{"ACCESSION_NUMBER": "0001234567-25-000001", "CIK": "1234567", "FILING_DATE": "2025-11-14"}],
            coverpages=[
                {
                    "ACCESSION_NUMBER": "0001234567-25-000001",
                    "FILINGMANAGER_NAME": "Big Fund LLC",
                    "REPORTCALENDARORQUARTER": "2025-09-30",
                }
            ],
            infotable=[
                {
                    "ACCESSION_NUMBER": "0001234567-25-000001",
                    "CUSIP": "037833100",
                    "FIGI": "BBG000B9XRY4",
                    "VALUE": "5000000",
                    "SSHPRNAMT": "100000",
                    "PUTCALL": "",
                },
            ],
        )
        archive_path = tmp_path / "form13f.zip"
        archive_path.write_bytes(archive_bytes)

        result = ingest_13f_dataset_archive(conn=ebull_test_conn, archive_path=archive_path, ingest_run_id=uuid4())
        ebull_test_conn.commit()

        assert result.figi_identifiers_seen == 1
        assert result.figi_identifiers_written == 1
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT instrument_id FROM external_identifiers "
                "WHERE provider='sec' AND identifier_type='figi' AND identifier_value='BBG000B9XRY4'"
            )
            row = cur.fetchone()
        assert row is not None and row[0] == iid

    def test_malformed_or_missing_figi_not_captured(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
        tmp_path: Path,
    ) -> None:
        _seed_universe_with_cusip(ebull_test_conn, symbol="AAPL", cusip="037833100")
        archive_bytes = _build_dataset_zip(
            submissions=[{"ACCESSION_NUMBER": "0001234567-25-000001", "CIK": "1234567", "FILING_DATE": "2025-11-14"}],
            coverpages=[
                {
                    "ACCESSION_NUMBER": "0001234567-25-000001",
                    "FILINGMANAGER_NAME": "Big Fund LLC",
                    "REPORTCALENDARORQUARTER": "2025-09-30",
                }
            ],
            infotable=[
                # Pre-2023 row shape (no FIGI column) + a malformed FIGI row.
                {"ACCESSION_NUMBER": "0001234567-25-000001", "CUSIP": "037833100", "VALUE": "100", "SSHPRNAMT": "10"},
                {
                    "ACCESSION_NUMBER": "0001234567-25-000001",
                    "CUSIP": "037833100",
                    "FIGI": "NOTAFIGI",  # wrong length → rejected
                    "VALUE": "200",
                    "SSHPRNAMT": "20",
                    "PUTCALL": "Put",
                },
            ],
        )
        archive_path = tmp_path / "form13f.zip"
        archive_path.write_bytes(archive_bytes)

        result = ingest_13f_dataset_archive(conn=ebull_test_conn, archive_path=archive_path, ingest_run_id=uuid4())
        ebull_test_conn.commit()

        assert result.figi_identifiers_seen == 0
        assert result.figi_identifiers_written == 0


@pytest.mark.skipif(not _test_db_available(), reason="ebull_test DB unavailable")
class TestUnresolvedChunkedFlushParity:
    """#1436 — the unresolved-CUSIP buffer flushes in bounded chunks
    inside the INFOTABLE loop instead of one end-of-archive flush.

    Parity bar: the flush helper aggregates COUNT/MIN/MAX grouped by
    (cusip, source) with an ON CONFLICT that sums count + LEAST/GREATEST
    of the periods, so N chunked flushes within one archive transaction
    land byte-identical final rows to a single batch flush. This test
    drives a SMALL chunk size so the buffer flushes multiple times —
    including a (cusip) whose two holdings straddle a chunk boundary —
    and asserts the per-(cusip) aggregate is exactly what a single flush
    would produce.
    """

    def test_chunked_flush_matches_single_batch_aggregate(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from app.services import sec_13f_dataset_ingest as ingest_mod

        # Seed nothing in the universe for these CUSIPs → every INFOTABLE
        # row is unresolved and lands in the buffer.
        flush_calls = {"n": 0}
        real_flush = ingest_mod._flush_unresolved_buffer

        def _counting_flush(*args: object, **kwargs: object) -> None:
            flush_calls["n"] += 1
            real_flush(*args, **kwargs)  # type: ignore[arg-type]

        monkeypatch.setattr(ingest_mod, "_flush_unresolved_buffer", _counting_flush)
        # Chunk of 2 → buffer flushes at sizes 2, 2, then the remainder.
        monkeypatch.setattr(ingest_mod, "_UNRESOLVED_FLUSH_CHUNK", 2)

        # Two accessions; 5 unresolved holdings across 4 distinct CUSIPs.
        # Order is chosen so CHUNK0033's two holdings straddle a chunk
        # boundary (rows 1 + 3) — proving cross-chunk COUNT summing via
        # ON CONFLICT, not just within-chunk aggregation.
        subs = [
            {"ACCESSION_NUMBER": "0009990000-25-000001", "CIK": "9990000", "FILING_DATE": "2025-11-14"},
            {"ACCESSION_NUMBER": "0009990000-25-000002", "CIK": "9990000", "FILING_DATE": "2025-11-14"},
        ]
        covers = [
            {
                "ACCESSION_NUMBER": "0009990000-25-000001",
                "FILINGMANAGER_NAME": "Chunk Fund",
                "REPORTCALENDARORQUARTER": "2025-09-30",
            },
            {
                "ACCESSION_NUMBER": "0009990000-25-000002",
                "FILINGMANAGER_NAME": "Chunk Fund",
                "REPORTCALENDARORQUARTER": "2025-09-30",
            },
        ]
        infotable = [
            {"ACCESSION_NUMBER": "0009990000-25-000001", "CUSIP": "CHUNK0033", "VALUE": "1", "SSHPRNAMT": "1"},
            {"ACCESSION_NUMBER": "0009990000-25-000001", "CUSIP": "CHUNK0011", "VALUE": "1", "SSHPRNAMT": "1"},
            {"ACCESSION_NUMBER": "0009990000-25-000001", "CUSIP": "CHUNK0033", "VALUE": "1", "SSHPRNAMT": "1"},
            {"ACCESSION_NUMBER": "0009990000-25-000002", "CUSIP": "CHUNK0022", "VALUE": "1", "SSHPRNAMT": "1"},
            {"ACCESSION_NUMBER": "0009990000-25-000002", "CUSIP": "CHUNK0044", "VALUE": "1", "SSHPRNAMT": "1"},
        ]
        archive_bytes = _build_dataset_zip(submissions=subs, coverpages=covers, infotable=infotable)
        archive_path = tmp_path / "form13f_chunk.zip"
        archive_path.write_bytes(archive_bytes)

        result = ingest_13f_dataset_archive(conn=ebull_test_conn, archive_path=archive_path, ingest_run_id=uuid4())
        ebull_test_conn.commit()

        # 5 unresolved holdings counted; nothing written (no universe match).
        assert result.rows_written == 0
        assert result.rows_skipped_unresolved_cusip == 5
        # Chunking actually fired: flushes at 2, 2, remainder 1 = 3 calls.
        assert flush_calls["n"] == 3

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                """
                SELECT cusip, observation_count, first_period_end, last_period_end
                FROM unresolved_13f_cusips
                WHERE source = 'bulk_13f_dataset' AND cusip LIKE 'CHUNK00%'
                ORDER BY cusip
                """
            )
            rows = cur.fetchall()

        # Exactly the single-batch aggregate: one row per distinct CUSIP,
        # CHUNK0033 count=2 (summed ACROSS two separate chunk flushes).
        from datetime import date as _date

        assert rows == [
            ("CHUNK0011", 1, _date(2025, 9, 30), _date(2025, 9, 30)),
            ("CHUNK0022", 1, _date(2025, 9, 30), _date(2025, 9, 30)),
            ("CHUNK0033", 2, _date(2025, 9, 30), _date(2025, 9, 30)),
            ("CHUNK0044", 1, _date(2025, 9, 30), _date(2025, 9, 30)),
        ]
