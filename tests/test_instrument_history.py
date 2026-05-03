"""Integration tests for instrument CIK / symbol history (#794
schema piece, Batch 1 of #788).

Exercises the DB-level temporal invariants — overlap / inverted /
single-current — and the backfill helper's idempotency.
"""

from __future__ import annotations

from datetime import date

import psycopg
import pytest

from app.services import instrument_history
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export

pytestmark = pytest.mark.integration


def _seed_instrument(conn: psycopg.Connection[tuple], *, iid: int, symbol: str) -> None:
    conn.execute(
        """
        INSERT INTO instruments (
            instrument_id, symbol, company_name, exchange, currency, is_tradable
        ) VALUES (%s, %s, %s, '4', 'USD', TRUE)
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        (iid, symbol, f"{symbol} Inc"),
    )


def _seed_sec_profile(conn: psycopg.Connection[tuple], *, iid: int, cik: str) -> None:
    conn.execute(
        """
        INSERT INTO instrument_sec_profile (instrument_id, cik)
        VALUES (%s, %s)
        ON CONFLICT (instrument_id) DO UPDATE SET cik = EXCLUDED.cik
        """,
        (iid, cik),
    )


class TestCikHistoryConstraints:
    @pytest.fixture
    def _setup(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> psycopg.Connection[tuple]:
        conn = ebull_test_conn
        _seed_instrument(conn, iid=794_001, symbol="HIST")
        conn.commit()
        return conn

    def test_inverted_range_rejected(self, _setup: psycopg.Connection[tuple]) -> None:
        """``effective_to <= effective_from`` violates the CHECK."""
        conn = _setup
        with pytest.raises(psycopg.errors.CheckViolation):
            with conn.transaction():
                conn.execute(
                    """
                    INSERT INTO instrument_cik_history (
                        instrument_id, cik, effective_from, effective_to,
                        source_event
                    ) VALUES (%s, %s, %s, %s, %s)
                    """,
                    (794_001, "0000000001", date(2024, 1, 1), date(2024, 1, 1), "manual"),
                )

    def test_two_current_rows_rejected(self, _setup: psycopg.Connection[tuple]) -> None:
        """Partial UNIQUE INDEX forbids two ``effective_to IS NULL``
        rows for the same instrument."""
        conn = _setup
        with pytest.raises(psycopg.errors.UniqueViolation):
            with conn.transaction():
                conn.execute(
                    """
                    INSERT INTO instrument_cik_history (
                        instrument_id, cik, effective_from, effective_to,
                        source_event
                    ) VALUES (%s, %s, %s, NULL, %s)
                    """,
                    (794_001, "0000000002", date(2020, 1, 1), "manual"),
                )
                conn.execute(
                    """
                    INSERT INTO instrument_cik_history (
                        instrument_id, cik, effective_from, effective_to,
                        source_event
                    ) VALUES (%s, %s, %s, NULL, %s)
                    """,
                    (794_001, "0000000003", date(2022, 1, 1), "manual"),
                )

    def test_overlapping_ranges_rejected(self, _setup: psycopg.Connection[tuple]) -> None:
        """GIST EXCLUDE rejects overlapping date ranges per
        instrument (half-open ``[from, to)`` semantic)."""
        conn = _setup
        with pytest.raises(psycopg.errors.ExclusionViolation):
            with conn.transaction():
                conn.execute(
                    """
                    INSERT INTO instrument_cik_history (
                        instrument_id, cik, effective_from, effective_to,
                        source_event
                    ) VALUES (%s, %s, %s, %s, %s)
                    """,
                    (794_001, "0000000004", date(2020, 1, 1), date(2021, 1, 1), "rebrand"),
                )
                conn.execute(
                    """
                    INSERT INTO instrument_cik_history (
                        instrument_id, cik, effective_from, effective_to,
                        source_event
                    ) VALUES (%s, %s, %s, %s, %s)
                    """,
                    (794_001, "0000000005", date(2020, 6, 1), date(2021, 6, 1), "reorg"),
                )

    def test_adjacent_ranges_allowed(self, _setup: psycopg.Connection[tuple]) -> None:
        """Half-open ``[from, to)`` lets a chain ending on 2021-01-01
        sit next to a chain starting 2021-01-01 without overlap."""
        conn = _setup
        with conn.transaction():
            conn.execute(
                """
                INSERT INTO instrument_cik_history (
                    instrument_id, cik, effective_from, effective_to,
                    source_event
                ) VALUES (%s, %s, %s, %s, %s)
                """,
                (794_001, "0000000010", date(2020, 1, 1), date(2021, 1, 1), "rebrand"),
            )
            conn.execute(
                """
                INSERT INTO instrument_cik_history (
                    instrument_id, cik, effective_from, effective_to,
                    source_event
                ) VALUES (%s, %s, %s, %s, %s)
                """,
                (794_001, "0000000011", date(2021, 1, 1), date(2022, 1, 1), "reorg"),
            )


class TestSymbolHistoryClashGuard:
    def test_same_symbol_two_instruments_allowed(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """A symbol reused on a different instrument at a different
        time is a separate chain (PK is per-instrument). Inserts on
        both instruments succeed."""
        conn = ebull_test_conn
        _seed_instrument(conn, iid=794_010, symbol="ALPHA")
        _seed_instrument(conn, iid=794_011, symbol="BETA")
        conn.commit()

        with conn.transaction():
            conn.execute(
                """
                INSERT INTO instrument_symbol_history (
                    instrument_id, symbol, effective_from, effective_to,
                    source_event
                ) VALUES (%s, %s, %s, %s, %s)
                """,
                (794_010, "DEAD", date(2018, 1, 1), date(2019, 1, 1), "delisting"),
            )
            conn.execute(
                """
                INSERT INTO instrument_symbol_history (
                    instrument_id, symbol, effective_from, effective_to,
                    source_event
                ) VALUES (%s, %s, %s, %s, %s)
                """,
                (794_011, "DEAD", date(2024, 1, 1), date(2025, 1, 1), "relisting"),
            )


class TestBackfill:
    def test_backfill_idempotent(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        conn = ebull_test_conn
        _seed_instrument(conn, iid=794_020, symbol="BFIDEM")
        _seed_sec_profile(conn, iid=794_020, cik="0000099001")
        conn.commit()

        first_cik, first_sym = instrument_history.backfill_current_history(conn)
        conn.commit()
        second_cik, second_sym = instrument_history.backfill_current_history(conn)
        conn.commit()

        assert first_cik >= 1
        assert first_sym >= 1
        assert second_cik == 0
        assert second_sym == 0

    def test_historical_ciks_for_returns_seed_row(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        conn = ebull_test_conn
        _seed_instrument(conn, iid=794_021, symbol="HIST2")
        _seed_sec_profile(conn, iid=794_021, cik="0000099002")
        conn.commit()

        instrument_history.backfill_current_history(conn)
        conn.commit()

        ciks = instrument_history.historical_ciks_for(conn, 794_021)
        assert list(ciks) == ["0000099002"]
        assert instrument_history.current_cik_for(conn, 794_021) == "0000099002"

    def test_resolve_historical_cik_to_instrument(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        conn = ebull_test_conn
        _seed_instrument(conn, iid=794_022, symbol="HIST3")
        _seed_sec_profile(conn, iid=794_022, cik="0000099003")
        conn.commit()
        instrument_history.backfill_current_history(conn)
        conn.commit()

        assert instrument_history.instrument_id_for_historical_cik(conn, "0000099003") == 794_022
        assert instrument_history.instrument_id_for_historical_cik(conn, "0000099999") is None
