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
                    "ACCESSION_NUMBER": "0001234567-25-000001",
                    "FILING_DATE": "2025-11-30",
                    "SUB_TYPE": "NPORT-P",
                    "REPORT_DATE": "2025-09-30",
                },
            ],
            registrants=[
                {"ACCESSION_NUMBER": "0001234567-25-000001", "CIK": "1234567", "REGISTRANT_NAME": "Big Fund Trust"},
            ],
            fund_info=[
                {
                    "ACCESSION_NUMBER": "0001234567-25-000001",
                    "SERIES_ID": "S000004310",
                    "SERIES_NAME": "Big Fund Equity Series",
                },
            ],
            holdings=[
                {
                    "ACCESSION_NUMBER": "0001234567-25-000001",
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

    @pytest.mark.parametrize("bad_report_date", ["6016-09-30", "1899-12-31", "2100-01-01"])
    def test_out_of_window_period_end_skipped(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
        tmp_path: Path,
        bad_report_date: str,
    ) -> None:
        # #1433 — a REPORT_DATE outside [1900, 2100) is rejected at the
        # submission level before any holding is processed, so it cannot
        # reach the DEFAULT partition (mirrors the #1218 XBRL guard).
        _seed_universe_with_cusip(ebull_test_conn, symbol="AAPL", cusip="037833100")
        archive_bytes = _build_dataset_zip(
            submissions=[
                {
                    "ACCESSION_NUMBER": "0001234567-25-000001",
                    "FILING_DATE": "2025-11-30",
                    "SUB_TYPE": "NPORT-P",
                    "REPORT_DATE": bad_report_date,
                },
            ],
            registrants=[
                {"ACCESSION_NUMBER": "0001234567-25-000001", "CIK": "1234567", "REGISTRANT_NAME": "Big Fund Trust"},
            ],
            fund_info=[
                {"ACCESSION_NUMBER": "0001234567-25-000001", "SERIES_ID": "S000004310", "SERIES_NAME": "S"},
            ],
            holdings=[
                {
                    "ACCESSION_NUMBER": "0001234567-25-000001",
                    "HOLDING_ID": "1",
                    "ISSUER_CUSIP": "037833100",
                    "BALANCE": "500000",
                    "UNIT": "NS",
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
        # The real invariant: an out-of-window period_end is never written.
        assert result.rows_written == 0
        # The #1433 bounds guard runs BEFORE the §4.6 retention gate, so every
        # out-of-window date — future (6016 / the exclusive 2100 ceiling) and
        # pre-1900 alike — is routed to rows_skipped_bad_data, not retention.
        assert result.rows_skipped_bad_data >= 1

    def test_marker_kept_when_cusip_stays_unresolved(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
        tmp_path: Path,
    ) -> None:
        """If the CUSIP never resolves (no instrument), the holding is
        skipped, no observation materialises, and the pending marker
        survives the ingest — the re-sighting upserts the SAME
        per-(cusip, source) row (#1349), never deletes it (ingest-time
        marker deletion was removed with the #1399 machinery)."""
        # Seed a DIFFERENT instrument; THIS holding's CUSIP stays unmapped.
        _seed_universe_with_cusip(ebull_test_conn, symbol="MSFT", cusip="594918104")
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "INSERT INTO unresolved_13f_cusips "
                "(cusip, source, observation_count, first_period_end, last_period_end) "
                "VALUES (%s, 'bulk_nport_dataset', 1, %s, %s)",
                ("000000001", "2025-09-30", "2025-09-30"),
            )
        ebull_test_conn.commit()

        archive_bytes = _build_dataset_zip(
            submissions=[
                {
                    "ACCESSION_NUMBER": "0001234567-25-000001",
                    "FILING_DATE": "2025-11-30",
                    "SUB_TYPE": "NPORT-P",
                    "REPORT_DATE": "2025-09-30",
                },
            ],
            registrants=[
                {"ACCESSION_NUMBER": "0001234567-25-000001", "CIK": "1234567", "REGISTRANT_NAME": "Big Fund Trust"},
            ],
            fund_info=[
                {
                    "ACCESSION_NUMBER": "0001234567-25-000001",
                    "SERIES_ID": "S000004310",
                    "SERIES_NAME": "Big Fund Equity Series",
                },
            ],
            holdings=[
                {
                    "ACCESSION_NUMBER": "0001234567-25-000001",
                    "HOLDING_ID": "1",
                    "ISSUER_CUSIP": "000000001",  # not in cusip_map → unresolved
                    "BALANCE": "500000",
                    "UNIT": "NS",
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
        assert result.rows_written == 0  # holding skipped — CUSIP unmapped

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT count(*), MAX(observation_count) FROM unresolved_13f_cusips "
                "WHERE source = 'bulk_nport_dataset' AND cusip = '000000001'"
            )
            remaining = cur.fetchone()
            # Still ONE row; the in-archive re-sighting bumped the count.
            assert remaining is not None and remaining[0] == 1
            assert remaining[1] == 2

    def test_bulk_seeds_n_port_ingest_log(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
        tmp_path: Path,
    ) -> None:
        # #1340 — every accession the bulk path loads gets a 'success'
        # n_port_ingest_log row so the per-CIK HTTP sweep (S23) skips it.
        _seed_universe_with_cusip(ebull_test_conn, symbol="AAPL", cusip="037833100")
        accn = "0001234567-25-000042"
        archive_bytes = _build_dataset_zip(
            submissions=[
                {
                    "ACCESSION_NUMBER": accn,
                    "FILING_DATE": "2025-11-30",
                    "SUB_TYPE": "NPORT-P",
                    "REPORT_DATE": "2025-09-30",
                },
            ],
            registrants=[
                {"ACCESSION_NUMBER": accn, "CIK": "1234567", "REGISTRANT_NAME": "Big Fund Trust"},
            ],
            fund_info=[
                {"ACCESSION_NUMBER": accn, "SERIES_ID": "S000004310", "SERIES_NAME": "Big Fund Equity Series"},
            ],
            holdings=[
                {
                    "ACCESSION_NUMBER": accn,
                    "HOLDING_ID": "1",
                    "ISSUER_CUSIP": "037833100",
                    "BALANCE": "500000",
                    "UNIT": "NS",
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

        assert result.ingest_log_rows_seeded == 1
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                """
                SELECT filer_cik, fund_series_id, period_of_report, status, holdings_inserted
                FROM n_port_ingest_log
                WHERE accession_number = %s
                """,
                (accn,),
            )
            row = cur.fetchone()
        assert row is not None, "bulk ingest must seed n_port_ingest_log for S23 skip"
        (filer_cik, series_id, period, status, inserted) = row
        assert filer_cik == "0001234567"
        assert series_id == "S000004310"
        assert period.isoformat() == "2025-09-30"
        assert status == "success"
        assert inserted == 1

    def test_bulk_skips_seeding_accession_with_unresolved_cusip(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
        tmp_path: Path,
    ) -> None:
        # #1340 — an accession with a valid-but-unmapped CUSIP holding is
        # RECOVERABLE (buffered for the S13 OpenFIGI sweep). It must NOT be
        # seeded into n_port_ingest_log, so S23 can re-fetch it after the
        # CUSIP resolves. Universe seeded for AAPL only; the holding's CUSIP
        # (a fake) is unmapped.
        _seed_universe_with_cusip(ebull_test_conn, symbol="AAPL", cusip="037833100")
        accn = "0009999999-25-000077"
        archive_bytes = _build_dataset_zip(
            submissions=[
                {
                    "ACCESSION_NUMBER": accn,
                    "FILING_DATE": "2025-11-30",
                    "SUB_TYPE": "NPORT-P",
                    "REPORT_DATE": "2025-09-30",
                },
            ],
            registrants=[
                {"ACCESSION_NUMBER": accn, "CIK": "9999999", "REGISTRANT_NAME": "Unmapped Fund Trust"},
            ],
            fund_info=[
                {"ACCESSION_NUMBER": accn, "SERIES_ID": "S000009999", "SERIES_NAME": "Unmapped Series"},
            ],
            holdings=[
                {
                    "ACCESSION_NUMBER": accn,
                    "HOLDING_ID": "1",
                    "ISSUER_CUSIP": "000000001",  # not in cusip_map
                    "BALANCE": "500000",
                    "UNIT": "NS",
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

        assert result.rows_skipped_unresolved_cusip == 1
        assert result.ingest_log_rows_seeded == 0
        with ebull_test_conn.cursor() as cur:
            cur.execute("SELECT 1 FROM n_port_ingest_log WHERE accession_number = %s", (accn,))
            assert cur.fetchone() is None, (
                "accession with an unresolved CUSIP must NOT be seeded — S23 retry path preserved"
            )

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


@pytest.mark.skipif(not _test_db_available(), reason="ebull_test DB unavailable")
class TestUnresolvedChunkedFlushParity:
    """#1436 — N-PORT unresolved-CUSIP buffer flushes in bounded chunks
    inside the holding loop (before the staging drain, matching the 13F
    sibling) instead of one end-of-archive flush. Same parity bar: the
    flush helper's (cusip, source) aggregation is a commutative monoid,
    so chunked flushes land byte-identical final rows to a single batch.
    """

    def test_chunked_flush_matches_single_batch_aggregate(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from datetime import date as _date

        from app.services import sec_nport_dataset_ingest as ingest_mod

        flush_calls = {"n": 0}
        real_flush = ingest_mod._flush_unresolved_buffer

        def _counting_flush(*args: object, **kwargs: object) -> None:
            flush_calls["n"] += 1
            real_flush(*args, **kwargs)  # type: ignore[arg-type]

        monkeypatch.setattr(ingest_mod, "_flush_unresolved_buffer", _counting_flush)
        monkeypatch.setattr(ingest_mod, "_UNRESOLVED_FLUSH_CHUNK", 2)

        accn1 = "0009990000-25-000001"
        accn2 = "0009990000-25-000002"
        subs = [
            {
                "ACCESSION_NUMBER": accn1,
                "FILING_DATE": "2025-11-30",
                "SUB_TYPE": "NPORT-P",
                "REPORT_DATE": "2025-09-30",
            },
            {
                "ACCESSION_NUMBER": accn2,
                "FILING_DATE": "2025-11-30",
                "SUB_TYPE": "NPORT-P",
                "REPORT_DATE": "2025-09-30",
            },
        ]
        regs = [
            {"ACCESSION_NUMBER": accn1, "CIK": "9990000", "REGISTRANT_NAME": "Chunk Trust"},
            {"ACCESSION_NUMBER": accn2, "CIK": "9990000", "REGISTRANT_NAME": "Chunk Trust"},
        ]
        funds = [
            {"ACCESSION_NUMBER": accn1, "SERIES_ID": "S000099000", "SERIES_NAME": "Chunk Series"},
            {"ACCESSION_NUMBER": accn2, "SERIES_ID": "S000099000", "SERIES_NAME": "Chunk Series"},
        ]

        def _holding(accn: str, hid: str, cusip: str) -> dict[str, str]:
            # Valid EC / Long / NS / positive-balance holding with an
            # UNMAPPED cusip → lands in the unresolved buffer.
            return {
                "ACCESSION_NUMBER": accn,
                "HOLDING_ID": hid,
                "ISSUER_CUSIP": cusip,
                "BALANCE": "500000",
                "UNIT": "NS",
                "CURRENCY_CODE": "USD",
                "CURRENCY_VALUE": "1000000",
                "PAYOFF_PROFILE": "Long",
                "ASSET_CAT": "EC",
            }

        # CHUNK0033 straddles a chunk boundary (rows 1 + 3) → cross-chunk
        # COUNT summing via ON CONFLICT.
        holdings = [
            _holding(accn1, "1", "CHUNK0033"),
            _holding(accn1, "2", "CHUNK0011"),
            _holding(accn1, "3", "CHUNK0033"),
            _holding(accn2, "4", "CHUNK0022"),
            _holding(accn2, "5", "CHUNK0044"),
        ]
        archive_bytes = _build_dataset_zip(submissions=subs, registrants=regs, fund_info=funds, holdings=holdings)
        archive_path = tmp_path / "nport_chunk.zip"
        archive_path.write_bytes(archive_bytes)

        result = ingest_nport_dataset_archive(conn=ebull_test_conn, archive_path=archive_path, ingest_run_id=uuid4())
        ebull_test_conn.commit()

        assert result.rows_written == 0
        assert result.rows_skipped_unresolved_cusip == 5
        # Flushes at 2, 2, remainder 1 = 3 calls.
        assert flush_calls["n"] == 3

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                """
                SELECT cusip, observation_count, first_period_end, last_period_end
                FROM unresolved_13f_cusips
                WHERE source = 'bulk_nport_dataset' AND cusip LIKE 'CHUNK00%'
                ORDER BY cusip
                """
            )
            rows = cur.fetchall()

        assert rows == [
            ("CHUNK0011", 1, _date(2025, 9, 30), _date(2025, 9, 30)),
            ("CHUNK0022", 1, _date(2025, 9, 30), _date(2025, 9, 30)),
            ("CHUNK0033", 2, _date(2025, 9, 30), _date(2025, 9, 30)),
            ("CHUNK0044", 1, _date(2025, 9, 30), _date(2025, 9, 30)),
        ]
