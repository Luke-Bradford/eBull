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
    "stocksIndustryID": 42,
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
            "stocksIndustryID": 42,
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

    def test_sector_zero_sentinel_maps_to_none(self) -> None:
        """eToro sends stocksIndustryID = 0 for FX/commodities (no
        industry). 0 must map to None, not the string "0" (#1598)."""
        record = _normalise_instrument({**FIXTURE_INSTRUMENT, "stocksIndustryID": 0})
        assert record is not None
        assert record.sector is None

    def test_currency_is_none_from_normaliser(self) -> None:
        """currency is None from the normaliser — the eToro instruments
        endpoint does not expose it. universe.py derives it from the
        operator-curated exchanges.currency join (sql/159, #1431)."""
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
        # #1464 dropped the instrument_type column + dataclass field (always
        # NULL — eToro's instruments endpoint never returns instrumentTypeName;
        # the type is captured as instrument_type_id instead).

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
# sync_universe — instrument_type_id COALESCE contract (#503 PR 4; the
# companion instrument_type TEXT column was dropped in #1464)
# ---------------------------------------------------------------------------


def _make_record(
    *,
    provider_id: str,
    symbol: str,
    instrument_type_id: int | None = None,
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
        instrument_type_id=instrument_type_id,
    )


def _read_instrument_type_id(conn: psycopg.Connection[tuple], instrument_id: int) -> int | None:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT instrument_type_id FROM instruments WHERE instrument_id = %s",
            (instrument_id,),
        )
        row = cur.fetchone()
    assert row is not None
    return row[0]


@pytest.mark.integration
class TestUniverseInstrumentTypeUpsert:
    """Pins the COALESCE behaviour of the ``sync_universe`` upsert against a
    real DB: a refresh that omits ``instrument_type_id`` for an existing row
    must NOT overwrite the previously-known value with NULL. (#1464 dropped the
    companion ``instrument_type`` TEXT column — always NULL — so only the
    ``instrument_type_id`` COALESCE remains.)"""

    def test_initial_insert_persists_instrument_type_id(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """instrument_type_id ships alongside the text label so the
        new lookup-table join works on a stable int key (#515 PR 1)."""
        provider = MagicMock()
        provider.get_tradable_instruments.return_value = [
            _make_record(
                provider_id="950010",
                symbol="ZZZ10",
                instrument_type_id=5,
            ),
        ]
        sync_universe(provider, ebull_test_conn)
        ebull_test_conn.commit()
        assert _read_instrument_type_id(ebull_test_conn, 950010) == 5

    def test_subsequent_null_id_does_not_clobber(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """COALESCE protects instrument_type_id — a transient response
        without instrumentTypeID must NOT erase a previously-known value."""
        provider = MagicMock()
        provider.get_tradable_instruments.return_value = [
            _make_record(
                provider_id="950011",
                symbol="ZZZ11",
                instrument_type_id=10,
            ),
        ]
        sync_universe(provider, ebull_test_conn)
        ebull_test_conn.commit()
        assert _read_instrument_type_id(ebull_test_conn, 950011) == 10

        provider.get_tradable_instruments.return_value = [
            _make_record(
                provider_id="950011",
                symbol="ZZZ11",
                instrument_type_id=None,
            ),
        ]
        sync_universe(provider, ebull_test_conn)
        ebull_test_conn.commit()
        assert _read_instrument_type_id(ebull_test_conn, 950011) == 10

    def test_subsequent_id_change_overwrites(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """A real id change (eToro reclassifies the instrument) still
        propagates — COALESCE only blocks NULL clobbers."""
        provider = MagicMock()
        provider.get_tradable_instruments.return_value = [
            _make_record(
                provider_id="950012",
                symbol="ZZZ12",
                instrument_type_id=5,
            ),
        ]
        sync_universe(provider, ebull_test_conn)
        ebull_test_conn.commit()

        provider.get_tradable_instruments.return_value = [
            _make_record(
                provider_id="950012",
                symbol="ZZZ12",
                instrument_type_id=6,
            ),
        ]
        sync_universe(provider, ebull_test_conn)
        ebull_test_conn.commit()
        assert _read_instrument_type_id(ebull_test_conn, 950012) == 6


# ---------------------------------------------------------------------------
# sync_universe — country derives from exchanges.country (#1233 §6.1)
# ---------------------------------------------------------------------------


def _read_country(conn: psycopg.Connection[tuple], instrument_id: int) -> str | None:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT country FROM instruments WHERE instrument_id = %s",
            (instrument_id,),
        )
        row = cur.fetchone()
    assert row is not None
    return row[0]


def _seed_exchange(conn: psycopg.Connection[tuple], *, exchange_id: str, country: str | None, asset_class: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO exchanges (exchange_id, country, asset_class)
            VALUES (%s, %s, %s)
            ON CONFLICT (exchange_id) DO UPDATE SET country = EXCLUDED.country, asset_class = EXCLUDED.asset_class
            """,
            (exchange_id, country, asset_class),
        )


def _make_record_with_exchange(
    *,
    provider_id: str,
    symbol: str,
    exchange: str | None,
) -> InstrumentRecord:
    return InstrumentRecord(
        provider_id=provider_id,
        symbol=symbol,
        company_name=f"{symbol} Co.",
        exchange=exchange,
        currency="USD",
        sector=None,
        industry=None,
        country=None,  # provider does not supply country (#1233 §6.1)
        is_tradable=True,
        instrument_type_id=None,
    )


@pytest.mark.integration
class TestUniverseCountryDerivesFromExchanges:
    """``instruments.country`` is derived from ``exchanges.country`` via
    the ``instruments.exchange = exchanges.exchange_id`` join — eToro
    does not expose country in the instruments endpoint (#1233 §6.1)."""

    def test_us_exchange_populates_country_us(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        # exchange_id='4' is curated as US equity per sql/067.
        _seed_exchange(ebull_test_conn, exchange_id="4", country="US", asset_class="us_equity")
        provider = MagicMock()
        provider.get_tradable_instruments.return_value = [
            _make_record_with_exchange(provider_id="970001", symbol="USEQ1", exchange="4"),
        ]
        sync_universe(provider, ebull_test_conn)
        ebull_test_conn.commit()
        assert _read_country(ebull_test_conn, 970001) == "US"

    def test_country_null_for_uncurated_exchange(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        # Crypto exchange has NULL country in seed (sql/067 line 92).
        _seed_exchange(ebull_test_conn, exchange_id="8", country=None, asset_class="crypto")
        provider = MagicMock()
        provider.get_tradable_instruments.return_value = [
            _make_record_with_exchange(provider_id="970002", symbol="CRYPTO1", exchange="8"),
        ]
        sync_universe(provider, ebull_test_conn)
        ebull_test_conn.commit()
        assert _read_country(ebull_test_conn, 970002) is None

    def test_country_refreshes_when_operator_recurates_exchange(
        self, ebull_test_conn: psycopg.Connection[tuple]
    ) -> None:
        """Operator re-classifies an exchange from NULL→'US'; the next
        sync_universe pass propagates the new country."""
        _seed_exchange(ebull_test_conn, exchange_id="970", country=None, asset_class="unknown")
        provider = MagicMock()
        provider.get_tradable_instruments.return_value = [
            _make_record_with_exchange(provider_id="970003", symbol="RECLASS", exchange="970"),
        ]
        sync_universe(provider, ebull_test_conn)
        ebull_test_conn.commit()
        assert _read_country(ebull_test_conn, 970003) is None

        # Operator curates the exchange.
        _seed_exchange(ebull_test_conn, exchange_id="970", country="GB", asset_class="uk_equity")
        ebull_test_conn.commit()

        # Next provider pass — country derives fresh from the join.
        sync_universe(provider, ebull_test_conn)
        ebull_test_conn.commit()
        assert _read_country(ebull_test_conn, 970003) == "GB"

    def test_missing_exchange_row_preserves_prior_country(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """Codex 2 pre-push edge case: if the exchange row is missing
        from ``exchanges`` (transient bootstrap order, brand-new
        exchange not yet seeded by sql/067), the upsert must preserve
        the existing ``instruments.country`` rather than wipe it to
        NULL."""
        # Seed a curated exchange + instrument with US country.
        _seed_exchange(ebull_test_conn, exchange_id="971", country="US", asset_class="us_equity")
        provider = MagicMock()
        provider.get_tradable_instruments.return_value = [
            _make_record_with_exchange(provider_id="970005", symbol="STALEXCH", exchange="971"),
        ]
        sync_universe(provider, ebull_test_conn)
        ebull_test_conn.commit()
        assert _read_country(ebull_test_conn, 970005) == "US"

        # Re-sync the same instrument, but with an exchange that does
        # NOT exist in exchanges. (Simulates: eToro reports a new
        # exchange the operator hasn't curated yet.)
        provider.get_tradable_instruments.return_value = [
            _make_record_with_exchange(provider_id="970005", symbol="STALEXCH", exchange="99999"),
        ]
        sync_universe(provider, ebull_test_conn)
        ebull_test_conn.commit()

        # Country must NOT have been wiped — the CASE branch preserves
        # the prior US value when the exchange row is missing.
        assert _read_country(ebull_test_conn, 970005) == "US"

    def test_record_country_field_ignored(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """The ``InstrumentRecord.country`` field is NOT wired through
        the upsert — the upsert sources country from ``exchanges`` only,
        so the dataclass field is irrelevant. This pins the behaviour:
        a provider that mistakenly populates ``country`` from
        instrument metadata cannot override the operator-curated
        exchanges.country."""
        _seed_exchange(ebull_test_conn, exchange_id="5", country="US", asset_class="us_equity")
        provider = MagicMock()
        rec = _make_record_with_exchange(provider_id="970004", symbol="OVERRIDE", exchange="5")
        # Mutate the frozen dataclass — can't, but mock-construct an
        # alternative by patching the dataclass replacement.
        from dataclasses import replace

        rec_with_bogus_country = replace(rec, country="XX")
        provider.get_tradable_instruments.return_value = [rec_with_bogus_country]
        sync_universe(provider, ebull_test_conn)
        ebull_test_conn.commit()
        # exchanges.country wins.
        assert _read_country(ebull_test_conn, 970004) == "US"


# ---------------------------------------------------------------------------
# sync_universe — currency derives from exchanges.currency (#1431)
# ---------------------------------------------------------------------------


def _read_currency(conn: psycopg.Connection[tuple], instrument_id: int) -> str | None:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT currency FROM instruments WHERE instrument_id = %s",
            (instrument_id,),
        )
        row = cur.fetchone()
    assert row is not None
    return row[0]


def _seed_exchange_currency(
    conn: psycopg.Connection[tuple], *, exchange_id: str, currency: str | None, asset_class: str
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO exchanges (exchange_id, currency, asset_class)
            VALUES (%s, %s, %s)
            ON CONFLICT (exchange_id) DO UPDATE SET currency = EXCLUDED.currency, asset_class = EXCLUDED.asset_class
            """,
            (exchange_id, currency, asset_class),
        )


@pytest.mark.integration
class TestUniverseCurrencyDerivesFromExchanges:
    """``instruments.currency`` is derived from ``exchanges.currency`` via
    the ``instruments.exchange = exchanges.exchange_id`` join — eToro
    does not expose currency in the instruments endpoint (#1431)."""

    def test_curated_exchange_populates_currency(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        _seed_exchange_currency(ebull_test_conn, exchange_id="4", currency="USD", asset_class="us_equity")
        provider = MagicMock()
        provider.get_tradable_instruments.return_value = [
            _make_record_with_exchange(provider_id="972001", symbol="USCCY1", exchange="4"),
        ]
        sync_universe(provider, ebull_test_conn)
        ebull_test_conn.commit()
        assert _read_currency(ebull_test_conn, 972001) == "USD"

    def test_currency_null_for_uncurated_exchange(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        # Crypto exchange has NULL currency — no single trading fiat.
        _seed_exchange_currency(ebull_test_conn, exchange_id="8", currency=None, asset_class="crypto")
        provider = MagicMock()
        provider.get_tradable_instruments.return_value = [
            _make_record_with_exchange(provider_id="972002", symbol="CRYPTOCCY", exchange="8"),
        ]
        sync_universe(provider, ebull_test_conn)
        ebull_test_conn.commit()
        assert _read_currency(ebull_test_conn, 972002) is None

    def test_currency_refreshes_when_operator_recurates_exchange(
        self, ebull_test_conn: psycopg.Connection[tuple]
    ) -> None:
        """Operator curates an exchange NULL→GBP; the next sync_universe
        propagates the new currency."""
        _seed_exchange_currency(ebull_test_conn, exchange_id="972", currency=None, asset_class="unknown")
        provider = MagicMock()
        provider.get_tradable_instruments.return_value = [
            _make_record_with_exchange(provider_id="972003", symbol="CCYRECLASS", exchange="972"),
        ]
        sync_universe(provider, ebull_test_conn)
        ebull_test_conn.commit()
        assert _read_currency(ebull_test_conn, 972003) is None

        _seed_exchange_currency(ebull_test_conn, exchange_id="972", currency="GBP", asset_class="uk_equity")
        ebull_test_conn.commit()

        sync_universe(provider, ebull_test_conn)
        ebull_test_conn.commit()
        assert _read_currency(ebull_test_conn, 972003) == "GBP"

    def test_missing_exchange_row_preserves_prior_currency(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """If the exchange row is missing (transient bootstrap order), the
        upsert preserves the existing ``instruments.currency`` rather than
        wiping it to NULL — same CASE shape as country."""
        _seed_exchange_currency(ebull_test_conn, exchange_id="973", currency="USD", asset_class="us_equity")
        provider = MagicMock()
        provider.get_tradable_instruments.return_value = [
            _make_record_with_exchange(provider_id="972005", symbol="STALECCY", exchange="973"),
        ]
        sync_universe(provider, ebull_test_conn)
        ebull_test_conn.commit()
        assert _read_currency(ebull_test_conn, 972005) == "USD"

        provider.get_tradable_instruments.return_value = [
            _make_record_with_exchange(provider_id="972005", symbol="STALECCY", exchange="99998"),
        ]
        sync_universe(provider, ebull_test_conn)
        ebull_test_conn.commit()
        assert _read_currency(ebull_test_conn, 972005) == "USD"

    def test_record_currency_field_ignored(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """``InstrumentRecord.currency`` is NOT wired through the upsert
        (binding removed in #1431) — the upsert sources currency from
        ``exchanges`` only. A record carrying USD cannot override the
        operator-curated exchanges.currency='GBP'."""
        _seed_exchange_currency(ebull_test_conn, exchange_id="974", currency="GBP", asset_class="uk_equity")
        provider = MagicMock()
        # _make_record_with_exchange hardcodes currency='USD' on the record.
        rec = _make_record_with_exchange(provider_id="972004", symbol="CCYOVERRIDE", exchange="974")
        assert rec.currency == "USD"
        provider.get_tradable_instruments.return_value = [rec]
        sync_universe(provider, ebull_test_conn)
        ebull_test_conn.commit()
        # exchanges.currency wins over the record's USD.
        assert _read_currency(ebull_test_conn, 972004) == "GBP"
