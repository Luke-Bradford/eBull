"""Tests for the bulk submissions.zip ingester (#1022)."""

from __future__ import annotations

import io
import json
import zipfile
from pathlib import Path

import psycopg
import pytest

from app.services.sec_submissions_ingest import (
    SubmissionsIngestResult,
    _cik_from_filename,
    ingest_submissions_archive,
)
from tests.fixtures.ebull_test_db import ebull_test_conn
from tests.fixtures.ebull_test_db import test_db_available as _test_db_available

__all__ = ["ebull_test_conn"]


# ---------------------------------------------------------------------------
# CIK filename parser — unit
# ---------------------------------------------------------------------------


class TestCikFromFilename:
    def test_valid_cik_filename(self) -> None:
        assert _cik_from_filename("CIK0000320193.json") == "0000320193"

    def test_unpadded_cik_rejected(self) -> None:
        assert _cik_from_filename("CIK320193.json") is None

    def test_non_cik_filename_rejected(self) -> None:
        assert _cik_from_filename("README.txt") is None
        assert _cik_from_filename("CIK0000320193-submissions-001.json") is None


# ---------------------------------------------------------------------------
# Archive fixture builder
# ---------------------------------------------------------------------------


def _build_archive(entries: dict[str, dict]) -> bytes:
    """Build an in-memory submissions.zip with the given CIK->payload map."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for cik, payload in entries.items():
            zf.writestr(f"CIK{cik}.json", json.dumps(payload))
    return buf.getvalue()


def _aapl_payload() -> dict:
    return {
        "cik": "320193",
        "name": "Apple Inc.",
        "sic": "3571",
        "sicDescription": "Electronic Computers",
        "exchanges": ["Nasdaq"],
        "category": "Large accelerated filer",
        "fiscalYearEnd": "0930",
        "stateOfIncorporation": "CA",
        "stateOfIncorporationDescription": "California",
        "filings": {
            "recent": {
                "accessionNumber": ["0000320193-25-000001", "0000320193-25-000002"],
                "filingDate": ["2025-11-01", "2025-08-01"],
                "form": ["10-K", "10-Q"],
                "primaryDocument": ["aapl-10-k.htm", "aapl-10-q.htm"],
                "reportDate": ["2025-09-30", "2025-06-30"],
            },
            "files": [],
        },
    }


def _msft_payload() -> dict:
    return {
        "cik": "789019",
        "name": "Microsoft Corp",
        "sic": "7372",
        "sicDescription": "Prepackaged Software",
        "exchanges": ["Nasdaq"],
        "category": "Large accelerated filer",
        "filings": {
            "recent": {
                "accessionNumber": ["0000789019-25-000010"],
                "filingDate": ["2025-07-30"],
                "form": ["10-K"],
                "primaryDocument": ["msft-10-k.htm"],
                "reportDate": ["2025-06-30"],
            },
            "files": [],
        },
    }


# ---------------------------------------------------------------------------
# DB integration
# ---------------------------------------------------------------------------


_NEXT_IID: list[int] = [10000]


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
class TestIngestSubmissionsArchive:
    def test_universe_match_writes_filings_and_profile(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
        tmp_path: Path,
    ) -> None:
        # Seed two universe instruments + one out-of-universe CIK.
        iid_aapl = _seed_universe(ebull_test_conn, symbol="AAPL", cik_padded="0000320193")
        iid_msft = _seed_universe(ebull_test_conn, symbol="MSFT", cik_padded="0000789019")

        archive_bytes = _build_archive(
            {
                "0000320193": _aapl_payload(),
                "0000789019": _msft_payload(),
                "9999999999": {
                    "cik": "9999999999",
                    "name": "Out of Universe",
                    "filings": {"recent": {}, "files": []},
                },
            }
        )
        archive_path = tmp_path / "submissions.zip"
        archive_path.write_bytes(archive_bytes)

        result = ingest_submissions_archive(
            conn=ebull_test_conn,
            archive_path=archive_path,
        )
        ebull_test_conn.commit()

        assert isinstance(result, SubmissionsIngestResult)
        assert result.archive_entries_seen == 3
        assert result.instruments_matched == 2
        assert result.archive_entries_skipped == 1  # the out-of-universe CIK
        assert result.parse_errors == 0
        # 2 AAPL filings + 1 MSFT filing = 3 upserted.
        assert result.filings_upserted == 3
        assert result.profiles_upserted == 2

        # Verify the rows actually landed.
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM filing_events WHERE instrument_id = %s AND provider = 'sec'",
                (iid_aapl,),
            )
            row = cur.fetchone()
            assert row is not None
            assert row[0] == 2

            cur.execute(
                "SELECT sic, sic_description FROM instrument_sec_profile WHERE instrument_id = %s",
                (iid_aapl,),
            )
            row = cur.fetchone()
            assert row is not None
            assert row[0] == "3571"
            assert row[1] == "Electronic Computers"

            # Codex review BLOCKING for PR #1030: ``raw_payload_json``
            # must carry the canonical ticker symbol (e.g. "AAPL"),
            # NOT a stringified instrument_id.
            cur.execute(
                "SELECT raw_payload_json->>'symbol' FROM filing_events "
                "WHERE instrument_id = %s ORDER BY filing_date DESC LIMIT 1",
                (iid_aapl,),
            )
            row = cur.fetchone()
            assert row is not None
            assert row[0] == "AAPL", f"expected ticker AAPL, got {row[0]!r}"

            # MSFT also landed.
            cur.execute(
                "SELECT COUNT(*) FROM filing_events WHERE instrument_id = %s AND provider = 'sec'",
                (iid_msft,),
            )
            row = cur.fetchone()
            assert row is not None
            assert row[0] == 1

    def test_corrupted_entry_increments_parse_errors_not_raise(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
        tmp_path: Path,
    ) -> None:
        iid = _seed_universe(ebull_test_conn, symbol="AAPL", cik_padded="0000320193")
        del iid

        # One bad JSON entry plus one good one.
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("CIK0000320193.json", "not valid json {")
        archive_path = tmp_path / "submissions.zip"
        archive_path.write_bytes(buf.getvalue())

        result = ingest_submissions_archive(
            conn=ebull_test_conn,
            archive_path=archive_path,
        )
        ebull_test_conn.commit()

        assert result.parse_errors == 1
        assert result.filings_upserted == 0

    def test_share_class_siblings_both_receive_filings_and_profile(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
        tmp_path: Path,
    ) -> None:
        """#1117 — GOOG + GOOGL co-bind one CIK; both must receive
        filings + entity profile.
        """
        iid_goog = _seed_universe(ebull_test_conn, symbol="GOOG", cik_padded="0001652044")
        iid_googl = _seed_universe(ebull_test_conn, symbol="GOOGL", cik_padded="0001652044")

        # Reuse AAPL submissions payload shape — _normalise_submissions_block
        # is content-agnostic; we care that the same archive entry
        # produces filings rows for both siblings.
        archive_bytes = _build_archive({"0001652044": _aapl_payload()})
        archive_path = tmp_path / "submissions.zip"
        archive_path.write_bytes(archive_bytes)

        result = ingest_submissions_archive(
            conn=ebull_test_conn,
            archive_path=archive_path,
        )
        ebull_test_conn.commit()

        assert result.archive_entries_seen == 1
        assert result.instruments_matched == 2
        assert result.parse_errors == 0
        # 2 filings × 2 siblings = 4 filings; 1 profile × 2 siblings = 2.
        assert result.filings_upserted == 4
        assert result.profiles_upserted == 2

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM filing_events WHERE instrument_id = %s AND provider = 'sec'",
                (iid_goog,),
            )
            row = cur.fetchone()
            assert row is not None
            assert row[0] == 2, f"GOOG expected 2 filings, got {row[0]}"

            cur.execute(
                "SELECT COUNT(*) FROM filing_events WHERE instrument_id = %s AND provider = 'sec'",
                (iid_googl,),
            )
            row = cur.fetchone()
            assert row is not None
            assert row[0] == 2, f"GOOGL expected 2 filings, got {row[0]}"

            # Each sibling carries its own ticker on the filing_events
            # row — Codex review BLOCKING for PR #1030 (the canonical
            # symbol must NOT be a stringified instrument_id, and must
            # NOT cross-contaminate between siblings).
            cur.execute(
                "SELECT raw_payload_json->>'symbol' FROM filing_events "
                "WHERE instrument_id = %s LIMIT 1",
                (iid_goog,),
            )
            row = cur.fetchone()
            assert row is not None
            assert row[0] == "GOOG", f"GOOG row carrying wrong symbol {row[0]!r}"

            cur.execute(
                "SELECT raw_payload_json->>'symbol' FROM filing_events "
                "WHERE instrument_id = %s LIMIT 1",
                (iid_googl,),
            )
            row = cur.fetchone()
            assert row is not None
            assert row[0] == "GOOGL", f"GOOGL row carrying wrong symbol {row[0]!r}"

            cur.execute(
                "SELECT COUNT(*) FROM instrument_sec_profile WHERE instrument_id IN (%s, %s)",
                (iid_goog, iid_googl),
            )
            row = cur.fetchone()
            assert row is not None
            assert row[0] == 2, f"expected 2 profiles, got {row[0]}"
