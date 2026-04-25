"""Tests for the exchanges metadata refresh (#503 PR 4).

Covers:

* The eToro exchanges normaliser — pins the bare-list contract per
  the eToro portal docs, raises ``ValueError`` on any dict-wrapped
  payload (Codex round 2 finding), and skips malformed rows.
* ``refresh_exchanges_metadata`` semantics:

  - inserts new rows with ``asset_class='unknown'``
  - updates ``description`` on existing rows (operator-curated
    ``country`` / ``asset_class`` are not touched)
  - no-op upsert when description matches (no row returned)
  - empty provider response → no DB writes (guards against an eToro
    blip wiping operator data)
"""

from __future__ import annotations

from unittest.mock import MagicMock

import psycopg
import pytest

from app.providers.implementations.etoro import _normalise_exchanges
from app.providers.market_data import ExchangeRecord
from app.services.exchanges import ExchangesRefreshSummary, refresh_exchanges_metadata

# Exchange ids used in DB tests. Synthetic prefix so a collision with
# the migration-067 seed (``1``..``20``) cannot mask a bug.
_TEST_ID_NEW = "test_new_e1"
_TEST_ID_EXISTING = "test_existing_e2"


# ---------------------------------------------------------------------------
# _normalise_exchanges — pure unit tests
# ---------------------------------------------------------------------------


class TestNormaliseExchanges:
    def test_bare_list_shape(self) -> None:
        raw = [
            {"exchangeID": 1, "exchangeDescription": "London Stock Exchange"},
            {"exchangeID": 2, "exchangeDescription": "XETRA"},
        ]
        records = _normalise_exchanges(raw)
        assert len(records) == 2
        assert all(isinstance(r, ExchangeRecord) for r in records)
        assert records[0].provider_id == "1"
        assert records[0].description == "London Stock Exchange"
        assert records[1].provider_id == "2"
        assert records[1].description == "XETRA"

    def test_dict_shape_raises(self) -> None:
        """A wrapped-dict response is NOT silently accepted — eToro's
        portal documents the bare-list shape, and Codex round 2 flagged
        that picking ``first list-typed value`` would silently mis-parse
        a future ``{"meta": [...], "exchanges": [...]}`` payload as an
        empty feed. Raise loudly so the weekly cron run surfaces the
        schema drift."""
        with pytest.raises(ValueError, match="Expected list"):
            _normalise_exchanges({"exchanges": [{"exchangeID": 8}]})

    def test_camelCase_id_variant_accepted(self) -> None:
        """eToro is inconsistent — some endpoints use ``exchangeId``,
        some ``exchangeID``. Accept both."""
        raw = [{"exchangeId": 5, "exchangeDescription": "NASDAQ"}]
        records = _normalise_exchanges(raw)
        assert len(records) == 1
        assert records[0].provider_id == "5"

    def test_missing_id_skipped(self) -> None:
        raw = [
            {"exchangeID": 1, "exchangeDescription": "OK"},
            {"exchangeDescription": "Missing ID — drop"},
            {"exchangeID": 2},  # missing description is fine; description optional
        ]
        records = _normalise_exchanges(raw)
        assert len(records) == 2
        assert {r.provider_id for r in records} == {"1", "2"}
        # Second record had no description → None (not "")
        rec2 = next(r for r in records if r.provider_id == "2")
        assert rec2.description is None

    def test_non_dict_items_skipped(self) -> None:
        raw = [
            {"exchangeID": 1, "exchangeDescription": "OK"},
            "not a dict",
            None,
        ]
        records = _normalise_exchanges(raw)
        assert len(records) == 1

    def test_empty_response_returns_empty(self) -> None:
        assert _normalise_exchanges([]) == []

    def test_unknown_shape_raises(self) -> None:
        with pytest.raises(ValueError, match="Expected list"):
            _normalise_exchanges("not a payload")


# ---------------------------------------------------------------------------
# refresh_exchanges_metadata — DB integration
# ---------------------------------------------------------------------------


def _seed_existing_exchange(
    conn: psycopg.Connection[tuple],
    *,
    exchange_id: str,
    description: str | None,
    country: str | None = "GB",
    asset_class: str = "uk_equity",
) -> None:
    """Pre-populate one ``exchanges`` row with an operator-curated
    ``country`` + ``asset_class``. The refresh service must not
    touch those columns even when it updates ``description``."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO exchanges (exchange_id, description, country, asset_class)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (exchange_id) DO UPDATE SET
                description = EXCLUDED.description,
                country     = EXCLUDED.country,
                asset_class = EXCLUDED.asset_class
            """,
            (exchange_id, description, country, asset_class),
        )


def _read_exchange(conn: psycopg.Connection[tuple], exchange_id: str) -> tuple[str | None, str | None, str] | None:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT description, country, asset_class FROM exchanges WHERE exchange_id = %s",
            (exchange_id,),
        )
        row = cur.fetchone()
    if row is None:
        return None
    return (row[0], row[1], row[2])


def _cleanup(conn: psycopg.Connection[tuple], ids: list[str]) -> None:
    with conn.cursor() as cur:
        cur.execute("DELETE FROM exchanges WHERE exchange_id = ANY(%s)", (ids,))
    conn.commit()


@pytest.mark.integration
class TestRefreshExchangesMetadata:
    def test_inserts_new_exchange_as_unknown(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        provider = MagicMock()
        provider.get_exchanges.return_value = [
            ExchangeRecord(provider_id=_TEST_ID_NEW, description="Test Exchange One"),
        ]
        try:
            summary = refresh_exchanges_metadata(provider, ebull_test_conn)
            ebull_test_conn.commit()

            assert summary.fetched == 1
            assert summary.inserted == 1
            assert summary.description_updated == 0

            row = _read_exchange(ebull_test_conn, _TEST_ID_NEW)
            assert row == ("Test Exchange One", None, "unknown")
        finally:
            _cleanup(ebull_test_conn, [_TEST_ID_NEW])

    def test_updates_description_only(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """Operator-curated ``country`` + ``asset_class`` survive an
        eToro-driven description refresh — those columns are the
        operator's source of truth."""
        _seed_existing_exchange(
            ebull_test_conn,
            exchange_id=_TEST_ID_EXISTING,
            description="Old description",
            country="GB",
            asset_class="uk_equity",
        )
        ebull_test_conn.commit()

        provider = MagicMock()
        provider.get_exchanges.return_value = [
            ExchangeRecord(provider_id=_TEST_ID_EXISTING, description="London Stock Exchange"),
        ]
        try:
            summary = refresh_exchanges_metadata(provider, ebull_test_conn)
            ebull_test_conn.commit()

            assert summary.fetched == 1
            assert summary.inserted == 0
            assert summary.description_updated == 1

            row = _read_exchange(ebull_test_conn, _TEST_ID_EXISTING)
            assert row == ("London Stock Exchange", "GB", "uk_equity")
        finally:
            _cleanup(ebull_test_conn, [_TEST_ID_EXISTING])

    def test_unchanged_description_is_noop(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """When description matches, neither ``inserted`` nor
        ``description_updated`` advances. Verifies the conditional
        ON CONFLICT WHERE actually filters out and the
        description_updated counter is meaningful (not just a count
        of every row processed)."""
        _seed_existing_exchange(
            ebull_test_conn,
            exchange_id=_TEST_ID_EXISTING,
            description="Same",
            country="DE",
            asset_class="eu_equity",
        )
        ebull_test_conn.commit()

        provider = MagicMock()
        provider.get_exchanges.return_value = [
            ExchangeRecord(provider_id=_TEST_ID_EXISTING, description="Same"),
        ]
        try:
            summary = refresh_exchanges_metadata(provider, ebull_test_conn)
            ebull_test_conn.commit()

            assert summary.fetched == 1
            assert summary.inserted == 0
            assert summary.description_updated == 0

            row = _read_exchange(ebull_test_conn, _TEST_ID_EXISTING)
            assert row == ("Same", "DE", "eu_equity")
        finally:
            _cleanup(ebull_test_conn, [_TEST_ID_EXISTING])

    def test_blank_description_does_not_clobber_existing(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """A partial eToro response that omits ``exchangeDescription``
        for a row must NOT erase a previously-known description.
        Pins the WARNING-fix from Codex round 1: COALESCE in the
        upsert preserves the existing value when EXCLUDED is NULL."""
        _seed_existing_exchange(
            ebull_test_conn,
            exchange_id=_TEST_ID_EXISTING,
            description="London Stock Exchange",
            country="GB",
            asset_class="uk_equity",
        )
        ebull_test_conn.commit()

        provider = MagicMock()
        provider.get_exchanges.return_value = [
            ExchangeRecord(provider_id=_TEST_ID_EXISTING, description=None),
        ]
        try:
            summary = refresh_exchanges_metadata(provider, ebull_test_conn)
            ebull_test_conn.commit()

            assert summary.fetched == 1
            # Neither inserted nor updated — the WHERE clause filters
            # out the no-op write.
            assert summary.inserted == 0
            assert summary.description_updated == 0

            row = _read_exchange(ebull_test_conn, _TEST_ID_EXISTING)
            assert row == ("London Stock Exchange", "GB", "uk_equity")
        finally:
            _cleanup(ebull_test_conn, [_TEST_ID_EXISTING])

    def test_empty_response_writes_nothing(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """An eToro hiccup that returns zero rows must NOT clobber
        existing operator data. Mirrors the ``sync_universe`` empty
        guard — same risk model."""
        _seed_existing_exchange(
            ebull_test_conn,
            exchange_id=_TEST_ID_EXISTING,
            description="Operator data",
            country="GB",
            asset_class="uk_equity",
        )
        ebull_test_conn.commit()

        provider = MagicMock()
        provider.get_exchanges.return_value = []
        try:
            summary = refresh_exchanges_metadata(provider, ebull_test_conn)
            ebull_test_conn.commit()

            assert summary == ExchangesRefreshSummary(fetched=0, inserted=0, description_updated=0)

            row = _read_exchange(ebull_test_conn, _TEST_ID_EXISTING)
            assert row == ("Operator data", "GB", "uk_equity")
        finally:
            _cleanup(ebull_test_conn, [_TEST_ID_EXISTING])
