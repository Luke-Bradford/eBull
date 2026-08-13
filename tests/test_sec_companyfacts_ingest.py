"""Tests for the bulk companyfacts.zip ingester (#1022)."""

from __future__ import annotations

import io
import json
import zipfile
from pathlib import Path

import psycopg
import pytest

from app.services.sec_companyfacts_ingest import (
    CompanyFactsIngestResult,
    extract_facts_from_companyfacts_payload,
    ingest_companyfacts_archive,
)
from tests.fixtures.ebull_test_db import ebull_test_conn
from tests.fixtures.ebull_test_db import test_db_available as _test_db_available

__all__ = ["ebull_test_conn"]


# ---------------------------------------------------------------------------
# Payload fixtures
# ---------------------------------------------------------------------------


def _aapl_companyfacts_payload() -> dict:
    return {
        "cik": 320193,
        "entityName": "Apple Inc.",
        "facts": {
            "us-gaap": {
                "Revenues": {
                    "label": "Revenues",
                    "description": "Total revenue.",
                    "units": {
                        "USD": [
                            {
                                "start": "2024-10-01",
                                "end": "2025-09-30",
                                "val": 391_000_000_000,
                                "accn": "0000320193-25-000001",
                                "fy": 2025,
                                "fp": "FY",
                                "form": "10-K",
                                "filed": "2025-11-01",
                                "frame": "CY2025",
                            },
                        ]
                    },
                },
            },
            "dei": {
                "EntityCommonStockSharesOutstanding": {
                    "label": "Shares outstanding",
                    "units": {
                        "shares": [
                            {
                                "end": "2025-09-30",
                                "val": 15_000_000_000,
                                "accn": "0000320193-25-000001",
                                "fy": 2025,
                                "fp": "FY",
                                "form": "10-K",
                                "filed": "2025-11-01",
                            },
                        ]
                    },
                },
            },
        },
    }


# ---------------------------------------------------------------------------
# Pure-payload extractor
# ---------------------------------------------------------------------------


class TestExtractFactsFromCompanyFactsPayload:
    def test_extracts_both_taxonomies(self) -> None:
        payload = _aapl_companyfacts_payload()
        facts = extract_facts_from_companyfacts_payload(payload)
        assert len(facts) == 2
        taxonomies = {f.taxonomy for f in facts}
        assert taxonomies == {"us-gaap", "dei"}
        assert any(f.concept == "Revenues" for f in facts)
        assert any(f.concept == "EntityCommonStockSharesOutstanding" for f in facts)

    def test_empty_payload_returns_empty_list(self) -> None:
        assert extract_facts_from_companyfacts_payload({}) == []
        assert extract_facts_from_companyfacts_payload({"facts": {}}) == []


# ---------------------------------------------------------------------------
# DB integration
# ---------------------------------------------------------------------------


_NEXT_IID: list[int] = [11000]


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


def _build_archive(entries: dict[str, dict]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for cik, payload in entries.items():
            zf.writestr(f"CIK{cik}.json", json.dumps(payload))
    return buf.getvalue()


@pytest.mark.integration
@pytest.mark.skipif(not _test_db_available(), reason="ebull_test DB unavailable")
class TestIngestCompanyFactsArchive:
    def test_universe_match_writes_facts(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
        tmp_path: Path,
    ) -> None:
        iid_aapl = _seed_universe(ebull_test_conn, symbol="AAPL", cik_padded="0000320193")

        archive_bytes = _build_archive(
            {
                "0000320193": _aapl_companyfacts_payload(),
                "9999999999": {  # out-of-universe
                    "cik": 9999999999,
                    "entityName": "Out of Universe",
                    "facts": {"us-gaap": {}, "dei": {}},
                },
            }
        )
        archive_path = tmp_path / "companyfacts.zip"
        archive_path.write_bytes(archive_bytes)

        result = ingest_companyfacts_archive(
            conn=ebull_test_conn,
            archive_path=archive_path,
        )
        ebull_test_conn.commit()

        assert isinstance(result, CompanyFactsIngestResult)
        assert result.archive_entries_seen == 2
        assert result.instruments_matched == 1
        assert result.archive_entries_skipped_universe_gap == 1
        assert result.parse_errors == 0
        assert result.facts_upserted >= 2  # at least Revenues + Shares.
        assert result.ingestion_run_id is not None

        # Verify rows landed in financial_facts_raw.
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM financial_facts_raw WHERE instrument_id = %s",
                (iid_aapl,),
            )
            row = cur.fetchone()
            assert row is not None
            assert row[0] >= 2

            # Verify the ingestion run row is marked ok.
            cur.execute(
                "SELECT status, rows_upserted FROM data_ingestion_runs WHERE ingestion_run_id = %s",
                (result.ingestion_run_id,),
            )
            row = cur.fetchone()
            assert row is not None
            assert row[0] == "success"
            assert row[1] == result.facts_upserted

    def test_corrupted_entry_increments_parse_errors_not_raise(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
        tmp_path: Path,
    ) -> None:
        _seed_universe(ebull_test_conn, symbol="AAPL", cik_padded="0000320193")
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("CIK0000320193.json", "not valid json {")
        archive_path = tmp_path / "companyfacts.zip"
        archive_path.write_bytes(buf.getvalue())

        result = ingest_companyfacts_archive(
            conn=ebull_test_conn,
            archive_path=archive_path,
        )
        ebull_test_conn.commit()

        assert result.parse_errors == 1
        assert result.facts_upserted == 0

    def test_share_class_siblings_both_receive_facts(
        self,
        ebull_test_conn: psycopg.Connection[tuple],
        tmp_path: Path,
    ) -> None:
        """#1117 — GOOG + GOOGL co-bind one CIK; both must receive facts.

        Pre-#1117 the dict-collapse in ``_load_cik_to_instrument`` kept
        only the last-seen sibling, leaving the other without
        fundamentals on every bulk run. Multimap shape fans out one
        archive entry to N siblings.
        """
        iid_goog = _seed_universe(ebull_test_conn, symbol="GOOG", cik_padded="0001652044")
        iid_googl = _seed_universe(ebull_test_conn, symbol="GOOGL", cik_padded="0001652044")

        # Reuse AAPL payload shape — XBRL semantics are independent of
        # the issuer; the fan-out test cares about row presence per
        # instrument, not the specific concept set.
        archive_bytes = _build_archive({"0001652044": _aapl_companyfacts_payload()})
        archive_path = tmp_path / "companyfacts.zip"
        archive_path.write_bytes(archive_bytes)

        result = ingest_companyfacts_archive(
            conn=ebull_test_conn,
            archive_path=archive_path,
        )
        ebull_test_conn.commit()

        assert result.archive_entries_seen == 1
        assert result.instruments_matched == 2  # GOOG + GOOGL
        assert result.parse_errors == 0
        assert result.facts_upserted >= 4  # 2 facts × 2 siblings

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM financial_facts_raw WHERE instrument_id = %s",
                (iid_goog,),
            )
            row = cur.fetchone()
            assert row is not None
            goog_count = row[0]

            cur.execute(
                "SELECT COUNT(*) FROM financial_facts_raw WHERE instrument_id = %s",
                (iid_googl,),
            )
            row = cur.fetchone()
            assert row is not None
            googl_count = row[0]

        assert goog_count >= 2, f"GOOG expected >=2 facts, got {goog_count}"
        assert googl_count >= 2, f"GOOGL expected >=2 facts, got {googl_count}"
