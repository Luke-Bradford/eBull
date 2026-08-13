"""Tests for eToro lookup-catalogue refresh (#515 PR 1).

Covers:

* Pure-unit normalisers for instrument-types + stocks-industries
  responses — pin live wrapper shapes, raise on schema drift, skip
  malformed rows.
* ``refresh_etoro_lookups`` semantics — insert + update + no-op
  branches, plus the empty-response no-clobber guard.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import psycopg
import pytest

from app.providers.implementations.etoro import (
    _normalise_instrument_types,
    _normalise_stocks_industries,
)
from app.providers.market_data import InstrumentTypeRecord, StocksIndustryRecord
from app.services.etoro_lookups import LookupRefreshSummary, refresh_etoro_lookups

# ---------------------------------------------------------------------------
# _normalise_instrument_types
# ---------------------------------------------------------------------------


class TestNormaliseInstrumentTypes:
    def test_live_wrapper_shape(self) -> None:
        raw = {
            "instrumentTypes": [
                {"instrumentTypeID": 1, "instrumentTypeDescription": "Forex"},
                {"instrumentTypeID": 5, "instrumentTypeDescription": "Stocks"},
            ]
        }
        records = _normalise_instrument_types(raw)
        assert len(records) == 2
        assert records[0] == InstrumentTypeRecord(type_id=1, description="Forex")
        assert records[1] == InstrumentTypeRecord(type_id=5, description="Stocks")

    def test_bare_list_fallback(self) -> None:
        """Accept the bare-list shape in case eToro aligns the
        live API with their portal docs in the future."""
        records = _normalise_instrument_types([{"instrumentTypeID": 6, "instrumentTypeDescription": "ETF"}])
        assert records == [InstrumentTypeRecord(type_id=6, description="ETF")]

    def test_unknown_wrapper_key_raises(self) -> None:
        with pytest.raises(ValueError, match="instrumentTypes"):
            _normalise_instrument_types({"types": [{"instrumentTypeID": 1}]})

    def test_unknown_top_shape_raises(self) -> None:
        with pytest.raises(ValueError, match="Expected dict"):
            _normalise_instrument_types("not a payload")

    def test_skips_malformed(self) -> None:
        raw = {
            "instrumentTypes": [
                {"instrumentTypeID": 1, "instrumentTypeDescription": "Forex"},
                {"instrumentTypeDescription": "Missing ID"},
                "not a dict",
                {"instrumentTypeID": "not-an-int", "instrumentTypeDescription": "Bad ID"},
                {"instrumentTypeID": 9, "instrumentTypeDescription": ""},
            ]
        }
        records = _normalise_instrument_types(raw)
        assert {r.type_id for r in records} == {1, 9}
        # Empty description normalises to None.
        rec_9 = next(r for r in records if r.type_id == 9)
        assert rec_9.description is None


# ---------------------------------------------------------------------------
# _normalise_stocks_industries
# ---------------------------------------------------------------------------


class TestNormaliseStocksIndustries:
    def test_live_wrapper_shape(self) -> None:
        raw = {
            "stocksIndustries": [
                {"industryID": 1, "industryName": "Basic Materials"},
                {"industryID": 8, "industryName": "Technology"},
            ]
        }
        records = _normalise_stocks_industries(raw)
        assert records == [
            StocksIndustryRecord(industry_id=1, name="Basic Materials"),
            StocksIndustryRecord(industry_id=8, name="Technology"),
        ]

    def test_unknown_wrapper_key_raises(self) -> None:
        with pytest.raises(ValueError, match="stocksIndustries"):
            _normalise_stocks_industries({"industries": [{"industryID": 1}]})


# ---------------------------------------------------------------------------
# refresh_etoro_lookups — DB integration
# ---------------------------------------------------------------------------


def _seed_existing_type(conn: psycopg.Connection[tuple], *, type_id: int, description: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO etoro_instrument_types (instrument_type_id, description)
            VALUES (%s, %s)
            ON CONFLICT (instrument_type_id) DO UPDATE SET description = EXCLUDED.description
            """,
            (type_id, description),
        )


def _read_type(conn: psycopg.Connection[tuple], type_id: int) -> str | None:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT description FROM etoro_instrument_types WHERE instrument_type_id = %s",
            (type_id,),
        )
        row = cur.fetchone()
    return row[0] if row else None


def _read_industry(conn: psycopg.Connection[tuple], industry_id: int) -> str | None:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT name FROM etoro_stocks_industries WHERE industry_id = %s",
            (industry_id,),
        )
        row = cur.fetchone()
    return row[0] if row else None


def _cleanup(conn: psycopg.Connection[tuple], *, type_ids: list[int], industry_ids: list[int]) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM etoro_instrument_types WHERE instrument_type_id = ANY(%s)",
            (type_ids,),
        )
        cur.execute(
            "DELETE FROM etoro_stocks_industries WHERE industry_id = ANY(%s)",
            (industry_ids,),
        )
    conn.commit()


@pytest.mark.integration
class TestRefreshEtoroLookups:
    def test_inserts_new_types_and_industries(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        provider = MagicMock()
        provider.get_instrument_types.return_value = [
            InstrumentTypeRecord(type_id=9001, description="Test Type"),
        ]
        provider.get_stocks_industries.return_value = [
            StocksIndustryRecord(industry_id=9001, name="Test Industry"),
        ]
        try:
            summary = refresh_etoro_lookups(provider, ebull_test_conn)
            ebull_test_conn.commit()
            assert summary.instrument_types_inserted == 1
            assert summary.industries_inserted == 1
            assert _read_type(ebull_test_conn, 9001) == "Test Type"
            assert _read_industry(ebull_test_conn, 9001) == "Test Industry"
        finally:
            _cleanup(ebull_test_conn, type_ids=[9001], industry_ids=[9001])

    def test_updates_changed_description(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        _seed_existing_type(ebull_test_conn, type_id=9002, description="Old Label")
        ebull_test_conn.commit()

        provider = MagicMock()
        provider.get_instrument_types.return_value = [
            InstrumentTypeRecord(type_id=9002, description="New Label"),
        ]
        provider.get_stocks_industries.return_value = []
        try:
            summary = refresh_etoro_lookups(provider, ebull_test_conn)
            ebull_test_conn.commit()
            assert summary.instrument_types_updated == 1
            assert summary.instrument_types_inserted == 0
            assert _read_type(ebull_test_conn, 9002) == "New Label"
        finally:
            _cleanup(ebull_test_conn, type_ids=[9002], industry_ids=[])

    def test_unchanged_description_is_noop(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        _seed_existing_type(ebull_test_conn, type_id=9003, description="Same")
        ebull_test_conn.commit()

        provider = MagicMock()
        provider.get_instrument_types.return_value = [
            InstrumentTypeRecord(type_id=9003, description="Same"),
        ]
        provider.get_stocks_industries.return_value = []
        try:
            summary = refresh_etoro_lookups(provider, ebull_test_conn)
            ebull_test_conn.commit()
            assert summary.instrument_types_inserted == 0
            assert summary.instrument_types_updated == 0
        finally:
            _cleanup(ebull_test_conn, type_ids=[9003], industry_ids=[])

    def test_blank_description_does_not_clobber(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """Partial response that omits the description for a row
        must NOT erase a previously-known label. COALESCE in the
        upsert preserves the existing value when EXCLUDED is NULL.
        Same shape as the exchanges-service guard from #503 PR 4."""
        _seed_existing_type(ebull_test_conn, type_id=9004, description="Real Label")
        ebull_test_conn.commit()

        provider = MagicMock()
        provider.get_instrument_types.return_value = [
            InstrumentTypeRecord(type_id=9004, description=None),
        ]
        provider.get_stocks_industries.return_value = []
        try:
            summary = refresh_etoro_lookups(provider, ebull_test_conn)
            ebull_test_conn.commit()
            assert summary.instrument_types_inserted == 0
            assert summary.instrument_types_updated == 0
            assert _read_type(ebull_test_conn, 9004) == "Real Label"
        finally:
            _cleanup(ebull_test_conn, type_ids=[9004], industry_ids=[])

    def test_empty_response_writes_nothing(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """Both endpoints returning zero rows must NOT clobber any
        existing label data."""
        _seed_existing_type(ebull_test_conn, type_id=9005, description="Persists")
        ebull_test_conn.commit()

        provider = MagicMock()
        provider.get_instrument_types.return_value = []
        provider.get_stocks_industries.return_value = []
        try:
            summary = refresh_etoro_lookups(provider, ebull_test_conn)
            ebull_test_conn.commit()
            assert summary == LookupRefreshSummary(0, 0, 0, 0, 0, 0)
            assert _read_type(ebull_test_conn, 9005) == "Persists"
        finally:
            _cleanup(ebull_test_conn, type_ids=[9005], industry_ids=[])
