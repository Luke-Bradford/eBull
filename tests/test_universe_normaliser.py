"""
Unit tests for the eToro instrument normaliser and universe sync logic.

No network calls, no database — all tests use in-memory fixtures.
Fixtures match the real eToro API response shape (instrumentDisplayDatas).
"""

import pytest

from app.providers.implementations.etoro import _normalise_instrument, _normalise_instruments
from app.providers.market_data import InstrumentRecord

# ---------------------------------------------------------------------------
# Fixtures — real eToro API field names
# ---------------------------------------------------------------------------

FIXTURE_INSTRUMENT = {
    "instrumentID": 1001,
    "symbolFull": "AAPL",
    "instrumentDisplayName": "Apple Inc.",
    "exchangeID": 10,
    "stocksIndustryId": 42,
    "priceSource": "Nasdaq",
    "isInternalInstrument": False,
}

FIXTURE_INSTRUMENT_INTERNAL = {
    **FIXTURE_INSTRUMENT,
    "instrumentID": 9999,
    "isInternalInstrument": True,
}

FIXTURE_API_RESPONSE = {
    "instrumentDisplayDatas": [
        FIXTURE_INSTRUMENT,
        {
            "instrumentID": 1002,
            "symbolFull": "MSFT",
            "instrumentDisplayName": "Microsoft Corporation",
            "exchangeID": 10,
            "stocksIndustryId": 42,
            "priceSource": "Nasdaq",
            "isInternalInstrument": False,
        },
    ]
}


# ---------------------------------------------------------------------------
# _normalise_instrument
# ---------------------------------------------------------------------------


class TestNormaliseInstrument:
    def test_real_api_fields(self) -> None:
        record = _normalise_instrument(FIXTURE_INSTRUMENT)
        assert record is not None
        assert record.provider_id == "1001"
        assert record.symbol == "AAPL"
        assert record.company_name == "Apple Inc."
        assert record.exchange == "10"
        assert record.sector == "42"
        assert record.is_tradable is True

    def test_currency_is_none_without_enrichment(self) -> None:
        """currency is None — eToro instruments endpoint does not expose
        a currency field. Enrichment (FMP profile) fills the real value."""
        record = _normalise_instrument(FIXTURE_INSTRUMENT)
        assert record is not None
        assert record.currency is None

    def test_missing_instrument_id_returns_none(self) -> None:
        item = {k: v for k, v in FIXTURE_INSTRUMENT.items() if k != "instrumentID"}
        assert _normalise_instrument(item) is None

    def test_missing_symbol_returns_none(self) -> None:
        item = {k: v for k, v in FIXTURE_INSTRUMENT.items() if k != "symbolFull"}
        assert _normalise_instrument(item) is None

    def test_optional_fields_can_be_none(self) -> None:
        item = {
            "instrumentID": 9999,
            "symbolFull": "XYZ",
            "isInternalInstrument": False,
        }
        record = _normalise_instrument(item)
        assert record is not None
        assert record.exchange is None
        assert record.sector is None
        assert record.industry is None
        assert record.country is None

    def test_internal_instrument_skipped(self) -> None:
        assert _normalise_instrument(FIXTURE_INSTRUMENT_INTERNAL) is None

    def test_is_tradable_always_true(self) -> None:
        """Only tradable instruments are returned by the API, so is_tradable
        is always True in normalised output."""
        record = _normalise_instrument(FIXTURE_INSTRUMENT)
        assert record is not None
        assert record.is_tradable is True

    def test_returns_instrument_record(self) -> None:
        record = _normalise_instrument(FIXTURE_INSTRUMENT)
        assert isinstance(record, InstrumentRecord)


# ---------------------------------------------------------------------------
# _normalise_instruments (full response)
# ---------------------------------------------------------------------------


class TestNormaliseInstruments:
    def test_response_shape(self) -> None:
        records = _normalise_instruments(FIXTURE_API_RESPONSE)
        assert len(records) == 2
        symbols = {r.symbol for r in records}
        assert symbols == {"AAPL", "MSFT"}

    def test_empty_instruments_list(self) -> None:
        records = _normalise_instruments({"instrumentDisplayDatas": []})
        assert records == []

    def test_non_dict_response_raises(self) -> None:
        with pytest.raises(ValueError, match="Expected dict"):
            _normalise_instruments(["not", "a", "dict"])

    def test_bad_items_skipped(self) -> None:
        raw = {
            "instrumentDisplayDatas": [
                FIXTURE_INSTRUMENT,
                {"instrumentID": None, "symbolFull": None},  # both missing → skipped
                "not a dict",  # not a dict → skipped
            ]
        }
        records = _normalise_instruments(raw)
        assert len(records) == 1
        assert records[0].symbol == "AAPL"

    def test_internal_instruments_filtered(self) -> None:
        raw = {
            "instrumentDisplayDatas": [
                FIXTURE_INSTRUMENT,
                FIXTURE_INSTRUMENT_INTERNAL,
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

        with pytest.raises(AttributeError):
            summary.inserted = 99  # type: ignore[misc]
