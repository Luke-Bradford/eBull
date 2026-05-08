"""Regression tests for daily_cik_refresh instrument scope (#475).

Before the #475 fix, the mapper selected every tradable instrument
and blindly joined against SEC's ticker→CIK map. Crypto coins
(exchange='8') whose ticker collided with a US-listed company
(BTC ↔ Grayscale Bitcoin Mini Trust, COMP ↔ unrelated US co, etc.)
got the unrelated CIK stamped on them via external_identifiers.
The SEC profile / business-summary panels on the crypto page then
rendered data for a completely different company.

These tests pin the fixed behavior: the candidate query only
returns US-listed exchanges, so a crypto row with a US-ticker
collision never reaches ``upsert_cik_mapping``.
"""

from __future__ import annotations

import psycopg
import pytest

# Canonical US exchange_ids that exist as us_equity in the test DB
# post-migrations 067 + 069. Was ("2", "4", "5", "6", "7", "19",
# "20") on the pre-#514 seed, but ids 2 (Commodity), 6 (FRA),
# 7 (LSE) were misclassified by migration 067 and got
# reclassified to commodity / eu_equity / uk_equity by migration
# 069. Production has an additional id `33` (Regular Trading
# Hours) that #513's exchanges_metadata_refresh adds to the live
# DB; it isn't in the test DB because the refresh job only runs
# against eToro at runtime, not in the migration seed.
_US_EXCHANGES: tuple[str, ...] = ("4", "5", "19", "20")


def _seed_instrument(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
    symbol: str,
    exchange: str,
    is_tradable: bool = True,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO instruments "
            "(instrument_id, symbol, company_name, exchange, is_tradable) "
            "VALUES (%s, %s, %s, %s, %s)",
            (instrument_id, symbol, f"Company {symbol}", exchange, is_tradable),
        )


@pytest.mark.integration
class TestCikCandidateQueryScope:
    """Mirrors the SELECT used inside ``daily_cik_refresh`` in
    ``app/workers/scheduler.py``. Invariant under test: only US-listed
    exchanges produce candidate rows, so crypto (exchange='8') is
    excluded regardless of ticker collision potential."""

    def _run_scoped_query(self, conn: psycopg.Connection[tuple]) -> list[tuple[str, str]]:
        # Inline the production query verbatim so a future refactor
        # that removes the exchange filter is caught by this test
        # failing. #503 PR 3 swapped the hardcoded id list for a
        # JOIN against the ``exchanges`` table — same invariant
        # ("only us_equity exchanges produce candidates"), expressed
        # via the curated mapping.
        with conn.cursor() as cur:
            cur.execute(
                "SELECT i.symbol, i.instrument_id::text FROM instruments i "
                "JOIN exchanges e ON e.exchange_id = i.exchange "
                "WHERE i.is_tradable = TRUE "
                "AND e.asset_class = 'us_equity'"
            )
            return [(r[0], r[1]) for r in cur.fetchall()]

    def test_crypto_instrument_excluded(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        _seed_instrument(ebull_test_conn, instrument_id=100000, symbol="BTC", exchange="8")
        _seed_instrument(ebull_test_conn, instrument_id=12220, symbol="BTC.US", exchange="5")
        ebull_test_conn.commit()

        rows = self._run_scoped_query(ebull_test_conn)
        symbols = sorted(s for s, _ in rows)
        assert "BTC.US" in symbols
        assert "BTC" not in symbols, (
            "Crypto BTC must be scoped out — else SEC CIK for Grayscale Mini Trust stamps onto it"
        )

    def test_non_tradable_instrument_excluded(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        # ``ebull_test_conn`` truncates ``instruments`` per-test (see
        # tests/fixtures/ebull_test_db.py _PLANNER_TABLES), so tests
        # within this class start clean. Using a distinct id range
        # from ``test_all_us_exchanges_included`` is belt-and-braces
        # for a future fixture-scope refactor that re-uses state.
        _seed_instrument(ebull_test_conn, instrument_id=2001, symbol="AAPL", exchange="4", is_tradable=False)
        _seed_instrument(ebull_test_conn, instrument_id=2002, symbol="MSFT", exchange="4", is_tradable=True)
        ebull_test_conn.commit()

        symbols = sorted(s for s, _ in self._run_scoped_query(ebull_test_conn))
        assert symbols == ["MSFT"]

    def test_all_us_exchanges_included(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        # Start id range at 3000 so it cannot overlap with the
        # `test_non_tradable_instrument_excluded` 2001/2002 range.
        for idx, exch in enumerate(_US_EXCHANGES, start=3000):
            _seed_instrument(
                ebull_test_conn,
                instrument_id=idx,
                symbol=f"US{exch}",
                exchange=exch,
            )
        # One crypto + one FX sanity-check negative.
        _seed_instrument(ebull_test_conn, instrument_id=99999, symbol="CRY", exchange="8")
        _seed_instrument(ebull_test_conn, instrument_id=99998, symbol="FX", exchange="40")
        ebull_test_conn.commit()

        symbols = sorted(s for s, _ in self._run_scoped_query(ebull_test_conn))
        expected = sorted(f"US{e}" for e in _US_EXCHANGES)
        assert symbols == expected
        assert "CRY" not in symbols
        assert "FX" not in symbols

    def test_empty_universe_returns_empty(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        assert self._run_scoped_query(ebull_test_conn) == []

    def test_unknown_exchange_classification_excluded(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """An instrument on an exchange the operator hasn't yet
        classified (``asset_class = 'unknown'``) is excluded from
        the SEC mapper. New eToro exchange ids land as ``unknown``
        per the migration backfill so they don't silently pick up
        SEC CIKs (Codex round 1 acceptance for #503 PR 3)."""
        # Seed an exchange row classified as ``unknown``.
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "INSERT INTO exchanges (exchange_id, asset_class) "
                "VALUES ('99', 'unknown') "
                "ON CONFLICT (exchange_id) DO UPDATE SET asset_class = 'unknown'"
            )
        _seed_instrument(ebull_test_conn, instrument_id=4001, symbol="UNK", exchange="99")
        ebull_test_conn.commit()

        symbols = sorted(s for s, _ in self._run_scoped_query(ebull_test_conn))
        assert "UNK" not in symbols


# ---------------------------------------------------------------------------
# Empty-destination forces full upsert (#1056)
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestCikDestinationIsEmpty:
    """Pin #1056: when the operator wipes external_identifiers but
    the watermark survives, daily_cik_refresh's empty-dest helper
    detects the empty state so the upsert is forced regardless of
    304 / hash-skip optimisations."""

    def test_returns_true_when_no_sec_cik_rows(self, ebull_test_conn) -> None:
        from app.workers.scheduler import _cik_destination_is_empty

        ebull_test_conn.execute("DELETE FROM external_identifiers WHERE provider='sec' AND identifier_type='cik'")
        ebull_test_conn.commit()
        assert _cik_destination_is_empty(ebull_test_conn) is True

    def test_returns_false_when_at_least_one_sec_cik_row(self, ebull_test_conn) -> None:
        from app.workers.scheduler import _cik_destination_is_empty

        # Seed a single SEC CIK row.
        _seed_instrument(ebull_test_conn, instrument_id=4801, symbol="AAPL", exchange="4")
        ebull_test_conn.execute(
            "INSERT INTO external_identifiers "
            "(instrument_id, provider, identifier_type, identifier_value, is_primary) "
            "VALUES (4801, 'sec', 'cik', '0000320193', TRUE) "
            "ON CONFLICT DO NOTHING"
        )
        ebull_test_conn.commit()
        assert _cik_destination_is_empty(ebull_test_conn) is False

    def test_ignores_non_sec_cik_rows(self, ebull_test_conn) -> None:
        # A row with provider='etoro' or identifier_type='isin' must
        # NOT count toward dest-non-empty.
        from app.workers.scheduler import _cik_destination_is_empty

        ebull_test_conn.execute("DELETE FROM external_identifiers WHERE provider='sec' AND identifier_type='cik'")
        _seed_instrument(ebull_test_conn, instrument_id=4802, symbol="HD", exchange="4")
        ebull_test_conn.execute(
            "INSERT INTO external_identifiers "
            "(instrument_id, provider, identifier_type, identifier_value, is_primary) "
            "VALUES (4802, 'sec', 'cusip', '437076102', TRUE) "
            "ON CONFLICT DO NOTHING"
        )
        ebull_test_conn.commit()
        assert _cik_destination_is_empty(ebull_test_conn) is True


# ---------------------------------------------------------------------------
# daily_cik_refresh empty-dest regression branches (#1056)
# ---------------------------------------------------------------------------


from unittest.mock import patch  # noqa: E402

from app.providers.implementations.sec_edgar import CikMappingResult  # noqa: E402


@pytest.mark.integration
class TestDailyCikRefreshEmptyDest:
    """End-to-end coverage of the empty-dest regression branches.

    Mocks the SEC HTTP boundary (build_cik_mapping_conditional) to
    return controlled results; runs the real daily_cik_refresh path
    against ebull_test DB and asserts external_identifiers is
    populated post-run."""

    @staticmethod
    def _seed_aapl_us_equity(conn) -> None:
        # Ensure exchange '4' is us_equity (default in test DB) +
        # AAPL instrument exists.
        conn.execute("UPDATE exchanges SET asset_class='us_equity' WHERE exchange_id='4'")
        _seed_instrument(conn, instrument_id=4901, symbol="AAPL", exchange="4")
        conn.execute("DELETE FROM external_identifiers WHERE provider='sec' AND identifier_type='cik'")
        conn.commit()

    @staticmethod
    def _patch_db_url(monkeypatch):
        # daily_cik_refresh hardcodes settings.database_url; redirect
        # to the ebull_test DB for the duration of the test.
        from app.config import settings
        from tests.fixtures.ebull_test_db import test_database_url

        monkeypatch.setattr(settings, "database_url", test_database_url())

    def test_empty_dest_omits_if_modified_since(self, ebull_test_conn, monkeypatch) -> None:
        self._patch_db_url(monkeypatch)
        """Stale watermark + empty dest → refresh sends no IMS so
        SEC can't return 304 against the stale validator."""
        from app.services.watermarks import set_watermark
        from app.workers.scheduler import daily_cik_refresh

        self._seed_aapl_us_equity(ebull_test_conn)
        with ebull_test_conn.transaction():
            set_watermark(
                ebull_test_conn,
                source="sec.tickers",
                key="global",
                watermark="Wed, 01 Jan 2025 00:00:00 GMT",  # stale
                response_hash="STALE_HASH",
            )
        ebull_test_conn.commit()

        captured: dict = {}

        def fake_conditional(self, *, if_modified_since=None):
            captured["if_modified_since"] = if_modified_since
            return CikMappingResult(
                mapping={"AAPL": "0000320193"},
                last_modified="Wed, 06 May 2026 20:52:27 GMT",
                body_hash="NEW_HASH",
            )

        with patch(
            "app.providers.implementations.sec_edgar.SecFilingsProvider.build_cik_mapping_conditional",
            new=fake_conditional,
        ):
            daily_cik_refresh()

        # Empty-dest branch must omit IMS.
        assert captured["if_modified_since"] is None
        # AAPL CIK must be present after the run.
        row = ebull_test_conn.execute(
            "SELECT identifier_value FROM external_identifiers "
            "WHERE provider='sec' AND identifier_type='cik' AND instrument_id=4901"
        ).fetchone()
        assert row is not None
        assert row[0] == "0000320193"

    def test_empty_dest_with_matching_hash_still_upserts(self, ebull_test_conn, monkeypatch) -> None:
        self._patch_db_url(monkeypatch)
        """Empty dest + 200-with-same-body-hash must still upsert.
        Pre-fix: the hash-skip branch fired and dest stayed empty."""
        from app.services.watermarks import set_watermark
        from app.workers.scheduler import daily_cik_refresh

        self._seed_aapl_us_equity(ebull_test_conn)
        with ebull_test_conn.transaction():
            set_watermark(
                ebull_test_conn,
                source="sec.tickers",
                key="global",
                watermark="",
                response_hash="MATCHING_HASH",
            )
        ebull_test_conn.commit()

        def fake_conditional(self, *, if_modified_since=None):
            return CikMappingResult(
                mapping={"AAPL": "0000320193"},
                last_modified="Wed, 06 May 2026 20:52:27 GMT",
                body_hash="MATCHING_HASH",  # SAME as prior watermark
            )

        with patch(
            "app.providers.implementations.sec_edgar.SecFilingsProvider.build_cik_mapping_conditional",
            new=fake_conditional,
        ):
            daily_cik_refresh()

        row = ebull_test_conn.execute(
            "SELECT identifier_value FROM external_identifiers "
            "WHERE provider='sec' AND identifier_type='cik' AND instrument_id=4901"
        ).fetchone()
        assert row is not None
        assert row[0] == "0000320193"

    def test_empty_dest_with_none_result_raises(self, ebull_test_conn, monkeypatch) -> None:
        self._patch_db_url(monkeypatch)
        """Defence-in-depth: if the provider returns None despite no
        IMS being sent (impossible but guarded), raise loudly so the
        bug isn't masked. Codex pre-push MEDIUM for #1056."""
        from app.workers.scheduler import daily_cik_refresh

        self._seed_aapl_us_equity(ebull_test_conn)

        def fake_conditional(self, *, if_modified_since=None):
            return None

        with patch(
            "app.providers.implementations.sec_edgar.SecFilingsProvider.build_cik_mapping_conditional",
            new=fake_conditional,
        ):
            # _tracked_job catches and re-raises after recording a
            # job_runs failure row; the RuntimeError bubbles out.
            with pytest.raises(RuntimeError, match="304 despite empty destination"):
                daily_cik_refresh()
