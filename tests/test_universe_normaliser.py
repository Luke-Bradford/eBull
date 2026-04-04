"""
Unit tests for the eToro instrument normaliser and universe sync logic.

No network calls, no database — all tests use in-memory fixtures.
"""

import pytest

from app.providers.implementations.etoro import _normalise_instrument, _normalise_instruments
from app.providers.market_data import InstrumentRecord

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

FIXTURE_INSTRUMENT_DISPLAY_DATA = {
    "InstrumentID": 1001,
    "SymbolFull": "AAPL",
    "InstrumentDisplayName": "Apple Inc.",
    "ExchangeID": "NASDAQ",
    "PriceSource": "USD",
    "Sector": "Technology",
    "Industry": "Consumer Electronics",
    "Country": "US",
    "IsActive": True,
}

FIXTURE_INSTRUMENT_SNAKE = {
    "instrumentId": "2002",
    "symbol": "BP.L",
    "name": "BP plc",
    "exchange": "LSE",
    "currency": "GBP",
    "sector": "Energy",
    "industry": "Oil & Gas",
    "country": "UK",
}

FIXTURE_API_RESPONSE_DISPLAY_DATA = {
    "InstrumentDisplayDatas": [
        FIXTURE_INSTRUMENT_DISPLAY_DATA,
        {
            "InstrumentID": 1002,
            "SymbolFull": "MSFT",
            "InstrumentDisplayName": "Microsoft Corporation",
            "ExchangeID": "NASDAQ",
            "PriceSource": "USD",
            "Sector": "Technology",
            "Industry": "Software",
            "Country": "US",
            "IsActive": True,
        },
    ]
}

FIXTURE_API_RESPONSE_SNAKE = {
    "instruments": [
        FIXTURE_INSTRUMENT_SNAKE,
    ]
}


# ---------------------------------------------------------------------------
# _normalise_instrument
# ---------------------------------------------------------------------------


class TestNormaliseInstrument:
    def test_camel_case_fields(self) -> None:
        record = _normalise_instrument(FIXTURE_INSTRUMENT_DISPLAY_DATA)
        assert record is not None
        assert record.provider_id == "1001"
        assert record.symbol == "AAPL"
        assert record.company_name == "Apple Inc."
        assert record.exchange == "NASDAQ"
        assert record.currency == "USD"
        assert record.sector == "Technology"
        assert record.industry == "Consumer Electronics"
        assert record.country == "US"
        assert record.is_tradable is True

    def test_snake_case_fields(self) -> None:
        record = _normalise_instrument(FIXTURE_INSTRUMENT_SNAKE)
        assert record is not None
        assert record.provider_id == "2002"
        assert record.symbol == "BP.L"
        assert record.company_name == "BP plc"
        assert record.currency == "GBP"

    def test_missing_instrument_id_returns_none(self) -> None:
        item = {**FIXTURE_INSTRUMENT_DISPLAY_DATA}
        del item["InstrumentID"]
        assert _normalise_instrument(item) is None

    def test_missing_symbol_returns_none(self) -> None:
        item = {**FIXTURE_INSTRUMENT_DISPLAY_DATA}
        del item["SymbolFull"]
        assert _normalise_instrument(item) is None

    def test_optional_fields_can_be_none(self) -> None:
        item = {
            "InstrumentID": 9999,
            "SymbolFull": "XYZ",
        }
        record = _normalise_instrument(item)
        assert record is not None
        assert record.exchange is None
        assert record.sector is None
        assert record.industry is None
        assert record.country is None

    def test_empty_string_optional_fields_become_none(self) -> None:
        item = {**FIXTURE_INSTRUMENT_DISPLAY_DATA, "ExchangeID": "", "Sector": ""}
        record = _normalise_instrument(item)
        assert record is not None
        assert record.exchange is None
        assert record.sector is None

    def test_is_active_false(self) -> None:
        item = {**FIXTURE_INSTRUMENT_DISPLAY_DATA, "IsActive": False}
        record = _normalise_instrument(item)
        assert record is not None
        assert record.is_tradable is False

    def test_returns_instrument_record(self) -> None:
        record = _normalise_instrument(FIXTURE_INSTRUMENT_DISPLAY_DATA)
        assert isinstance(record, InstrumentRecord)


# ---------------------------------------------------------------------------
# _normalise_instruments (full response)
# ---------------------------------------------------------------------------


class TestNormaliseInstruments:
    def test_camel_case_response_shape(self) -> None:
        records = _normalise_instruments(FIXTURE_API_RESPONSE_DISPLAY_DATA)
        assert len(records) == 2
        symbols = {r.symbol for r in records}
        assert symbols == {"AAPL", "MSFT"}

    def test_snake_case_response_shape(self) -> None:
        records = _normalise_instruments(FIXTURE_API_RESPONSE_SNAKE)
        assert len(records) == 1
        assert records[0].symbol == "BP.L"

    def test_empty_instruments_list(self) -> None:
        records = _normalise_instruments({"InstrumentDisplayDatas": []})
        assert records == []

    def test_non_dict_response_raises(self) -> None:
        with pytest.raises(ValueError, match="Expected dict"):
            _normalise_instruments(["not", "a", "dict"])

    def test_bad_items_skipped(self) -> None:
        raw = {
            "InstrumentDisplayDatas": [
                FIXTURE_INSTRUMENT_DISPLAY_DATA,
                {"InstrumentID": None, "SymbolFull": None},  # both missing → skipped
                "not a dict",  # not a dict → skipped
            ]
        }
        records = _normalise_instruments(raw)
        assert len(records) == 1
        assert records[0].symbol == "AAPL"


# ---------------------------------------------------------------------------
# SyncSummary shape
# ---------------------------------------------------------------------------


class TestSyncSummary:
    def test_sync_summary_is_frozen(self) -> None:
        from app.services.universe import SyncSummary

        summary = SyncSummary(inserted=3, updated=1, deactivated=0)
        assert summary.inserted == 3
        assert summary.updated == 1
        assert summary.deactivated == 0

        with pytest.raises(Exception):
            summary.inserted = 99  # type: ignore[misc]
