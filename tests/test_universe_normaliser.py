"""
Unit tests for the eToro instrument normaliser and universe sync logic.

Pure-unit normaliser tests use in-memory fixtures and run without a
database. The instrument-type COALESCE upsert test is integration
(marked) and exercises the real ``sync_universe`` SQL against the
``ebull_test`` Postgres.
"""

from unittest.mock import MagicMock

import psycopg
import pytest

from app.providers.implementations.etoro import _normalise_instrument, _normalise_instruments
from app.providers.market_data import InstrumentRecord
from app.services.universe import sync_universe

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
    "instrumentTypeName": "Stock",
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
        assert record.instrument_type is None

    def test_instrument_type_captured(self) -> None:
        """instrumentTypeName flows through so the universe upsert
        can persist eToro's classification (Stock / Crypto / ETF / …)
        for the cross-validation against exchanges.asset_class
        added in migration 068."""
        record = _normalise_instrument(FIXTURE_INSTRUMENT)
        assert record is not None
        assert record.instrument_type == "Stock"

    def test_instrument_type_empty_string_normalises_to_none(self) -> None:
        """Empty strings from eToro are treated as missing — the
        downstream upsert distinguishes 'unknown' from '' so a
        blank value would create a useless distinct row in
        ``instruments.instrument_type``."""
        item = {**FIXTURE_INSTRUMENT, "instrumentTypeName": ""}
        record = _normalise_instrument(item)
        assert record is not None
        assert record.instrument_type is None

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


# ---------------------------------------------------------------------------
# sync_universe — instrument_type COALESCE contract (#503 PR 4)
# ---------------------------------------------------------------------------


def _make_record(
    *,
    provider_id: str,
    symbol: str,
    instrument_type: str | None,
) -> InstrumentRecord:
    return InstrumentRecord(
        provider_id=provider_id,
        symbol=symbol,
        company_name=f"{symbol} Co.",
        exchange="2",
        currency="USD",
        sector=None,
        industry=None,
        country=None,
        is_tradable=True,
        instrument_type=instrument_type,
    )


def _read_instrument_type(conn: psycopg.Connection[tuple], instrument_id: int) -> str | None:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT instrument_type FROM instruments WHERE instrument_id = %s",
            (instrument_id,),
        )
        row = cur.fetchone()
    assert row is not None
    return row[0]


@pytest.mark.integration
class TestUniverseInstrumentTypeUpsert:
    """Pins the COALESCE behaviour of the ``sync_universe`` upsert
    against a real DB. The Codex round-1 WARNING fix: a refresh that
    omits ``instrument_type`` for an existing row must NOT overwrite
    the previously-known value with NULL."""

    def test_initial_insert_persists_instrument_type(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        provider = MagicMock()
        provider.get_tradable_instruments.return_value = [
            _make_record(provider_id="950001", symbol="ZZZ1", instrument_type="Stock"),
        ]
        sync_universe(provider, ebull_test_conn)
        ebull_test_conn.commit()
        assert _read_instrument_type(ebull_test_conn, 950001) == "Stock"

    def test_subsequent_null_does_not_clobber(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """First sync writes 'Stock'; a follow-up sync that omits
        ``instrument_type`` must leave 'Stock' in place."""
        provider = MagicMock()
        provider.get_tradable_instruments.return_value = [
            _make_record(provider_id="950002", symbol="ZZZ2", instrument_type="Crypto"),
        ]
        sync_universe(provider, ebull_test_conn)
        ebull_test_conn.commit()
        assert _read_instrument_type(ebull_test_conn, 950002) == "Crypto"

        # Second refresh — no instrument_type from provider.
        provider.get_tradable_instruments.return_value = [
            _make_record(provider_id="950002", symbol="ZZZ2", instrument_type=None),
        ]
        sync_universe(provider, ebull_test_conn)
        ebull_test_conn.commit()

        assert _read_instrument_type(ebull_test_conn, 950002) == "Crypto"

    def test_subsequent_value_change_overwrites(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """eToro reclassifying ``Stock`` → ``ETF`` must propagate.
        COALESCE only protects against NULL — a real change still wins."""
        provider = MagicMock()
        provider.get_tradable_instruments.return_value = [
            _make_record(provider_id="950003", symbol="ZZZ3", instrument_type="Stock"),
        ]
        sync_universe(provider, ebull_test_conn)
        ebull_test_conn.commit()

        provider.get_tradable_instruments.return_value = [
            _make_record(provider_id="950003", symbol="ZZZ3", instrument_type="ETF"),
        ]
        sync_universe(provider, ebull_test_conn)
        ebull_test_conn.commit()

        assert _read_instrument_type(ebull_test_conn, 950003) == "ETF"
