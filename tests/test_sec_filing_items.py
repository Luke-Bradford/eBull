"""Tests for app.services.sec_filing_items (#431)."""

from __future__ import annotations

import psycopg
import pytest

from app.services.sec_filing_items import (
    _split_items,
    apply_8k_items_to_filing_events,
    parse_8k_items_by_accession,
)
from tests.fixtures.ebull_test_db import ebull_test_conn
from tests.fixtures.ebull_test_db import test_db_available as _test_db_available

__all__ = ["ebull_test_conn"]


# ---------------------------------------------------------------------------
# parse_8k_items_by_accession — unit
# ---------------------------------------------------------------------------


def _submissions_shell(
    *,
    accessions: list[str],
    forms: list[str],
    items_col: list[str],
) -> dict:
    return {
        "filings": {
            "recent": {
                "accessionNumber": accessions,
                "form": forms,
                "items": items_col,
            }
        }
    }


class TestParseItems:
    def test_extracts_items_from_8k_only(self) -> None:
        payload = _submissions_shell(
            accessions=["acc-1", "acc-2", "acc-3"],
            forms=["8-K", "10-Q", "8-K/A"],
            items_col=["1.01,2.03", "", "5.02"],
        )
        out = parse_8k_items_by_accession(payload)
        assert out == {
            "acc-1": ["1.01", "2.03"],
            "acc-3": ["5.02"],
        }

    def test_empty_items_string_yields_empty_list(self) -> None:
        payload = _submissions_shell(
            accessions=["acc-1"],
            forms=["8-K"],
            items_col=[""],
        )
        out = parse_8k_items_by_accession(payload)
        assert out == {"acc-1": []}

    def test_malformed_payload_yields_empty(self) -> None:
        assert parse_8k_items_by_accession({}) == {}
        assert parse_8k_items_by_accession({"filings": None}) == {}
        # Mismatched column lengths must not raise.
        bad = _submissions_shell(
            accessions=["a"],
            forms=["8-K", "extra"],
            items_col=[""],
        )
        assert parse_8k_items_by_accession(bad) == {}

    def test_deduplicates_repeated_codes(self) -> None:
        payload = _submissions_shell(
            accessions=["acc-1"],
            forms=["8-K"],
            items_col=["1.01, 1.01 ,2.03"],
        )
        assert parse_8k_items_by_accession(payload) == {"acc-1": ["1.01", "2.03"]}

    def test_drops_malformed_codes(self) -> None:
        payload = _submissions_shell(
            accessions=["acc-1"],
            forms=["8-K"],
            items_col=["1.01,FOO,1,9.01,,x.y"],
        )
        assert parse_8k_items_by_accession(payload) == {"acc-1": ["1.01", "9.01"]}


class TestSplitItems:
    def test_whitespace_tolerant(self) -> None:
        assert _split_items("  1.01 , 9.01  ") == ["1.01", "9.01"]

    def test_non_string_returns_empty(self) -> None:
        assert _split_items(None) == []
        assert _split_items(123) == []


# ---------------------------------------------------------------------------
# apply_8k_items_to_filing_events — integration
# ---------------------------------------------------------------------------


pytestmark_int = pytest.mark.integration

_NEXT_IID = [40_000]


def _seed_instrument(conn: psycopg.Connection[tuple], *, symbol: str) -> int:
    _NEXT_IID[0] += 1
    iid = _NEXT_IID[0]
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO instruments (instrument_id, symbol, company_name, currency, is_tradable) "
            "VALUES (%s, %s, %s, 'USD', TRUE)",
            (iid, symbol, f"{symbol} Inc."),
        )
    conn.commit()
    return iid


def _seed_filing(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
    accession: str,
    filing_type: str = "8-K",
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO filing_events (
                instrument_id, filing_date, filing_type,
                provider, provider_filing_id, source_url
            ) VALUES (%s, CURRENT_DATE, %s, 'sec', %s, 'https://example.com')
            """,
            (instrument_id, filing_type, accession),
        )
    conn.commit()


@pytest.mark.integration
@pytest.mark.skipif(not _test_db_available(), reason="ebull_test DB unavailable")
class TestApplyItems:
    def test_updates_existing_filing_events_row(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        iid = _seed_instrument(ebull_test_conn, symbol="ITM1")
        _seed_filing(ebull_test_conn, instrument_id=iid, accession="acc-itm1")

        applied = apply_8k_items_to_filing_events(
            ebull_test_conn,
            {"acc-itm1": ["1.01", "9.01"]},
        )
        ebull_test_conn.commit()
        assert applied == 1

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT items FROM filing_events WHERE provider_filing_id = %s",
                ("acc-itm1",),
            )
            row = cur.fetchone()
            assert row is not None
            assert row[0] == ["1.01", "9.01"]

    def test_missing_filing_row_is_noop(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        # No filing_events row with this accession — nothing to update.
        applied = apply_8k_items_to_filing_events(
            ebull_test_conn,
            {"acc-missing": ["1.01"]},
        )
        ebull_test_conn.commit()
        assert applied == 0

    def test_empty_list_still_writes_empty_array(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        iid = _seed_instrument(ebull_test_conn, symbol="ITM2")
        _seed_filing(ebull_test_conn, instrument_id=iid, accession="acc-itm2")
        apply_8k_items_to_filing_events(ebull_test_conn, {"acc-itm2": []})
        ebull_test_conn.commit()
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT items FROM filing_events WHERE provider_filing_id = %s",
                ("acc-itm2",),
            )
            row = cur.fetchone()
            assert row is not None
            assert row[0] == []


@pytest.mark.integration
@pytest.mark.skipif(not _test_db_available(), reason="ebull_test DB unavailable")
class TestLookupTableSeeded:
    def test_key_codes_present_with_severity(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT code, severity FROM sec_8k_item_codes "
                "WHERE code IN ('1.03', '2.02', '4.01', '5.02', '8.01') "
                "ORDER BY code"
            )
            rows = dict(cur.fetchall())
        # Bankruptcy, auditor change, change-of-control → critical
        assert rows["1.03"] == "critical"
        assert rows["4.01"] == "critical"
        # Earnings release, exec departure → material
        assert rows["2.02"] == "material"
        assert rows["5.02"] == "material"
        # Other events → informational
        assert rows["8.01"] == "informational"
