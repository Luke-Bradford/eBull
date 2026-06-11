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


class TestReconcileSymbolHistory:
    """#794 Batch 7 — the live symbol-change ingester."""

    def _seed_with_history(
        self,
        conn: psycopg.Connection[tuple],
        *,
        iid: int,
        symbol: str,
        opened_days_ago: int,
    ) -> None:
        """Instrument + an open history row opened ``opened_days_ago``."""
        _seed_instrument(conn, iid=iid, symbol=symbol)
        conn.execute(
            """
            INSERT INTO instrument_symbol_history (
                instrument_id, symbol, effective_from, effective_to, source_event
            ) VALUES (%s, %s, CURRENT_DATE - %s, NULL, 'imported')
            """,
            (iid, symbol, opened_days_ago),
        )

    def _chain(self, conn: psycopg.Connection[tuple], iid: int) -> list[tuple]:
        return conn.execute(
            """
            SELECT symbol, effective_from, effective_to, source_event
            FROM instrument_symbol_history
            WHERE instrument_id = %s
            ORDER BY effective_from
            """,
            (iid,),
        ).fetchall()

    def test_rename_closes_prior_and_opens_new(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        conn = ebull_test_conn
        self._seed_with_history(conn, iid=794001, symbol="FB", opened_days_ago=30)
        conn.execute("UPDATE instruments SET symbol = 'META' WHERE instrument_id = 794001")

        stats = instrument_history.reconcile_symbol_history(conn)
        conn.commit()

        assert stats.renamed == 1
        chain = self._chain(conn, 794001)
        assert len(chain) == 2
        assert chain[0][0] == "FB"
        assert chain[0][2] == date.today()  # closed at rename date
        assert chain[1][0] == "META"
        assert chain[1][2] is None  # current
        assert chain[1][3] == "rebrand"

    def test_delisted_suffix_classified_as_delisting(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        conn = ebull_test_conn
        self._seed_with_history(conn, iid=794002, symbol="BBBY", opened_days_ago=30)
        conn.execute("UPDATE instruments SET symbol = 'BBBY.delisted' WHERE instrument_id = 794002")

        stats = instrument_history.reconcile_symbol_history(conn)
        conn.commit()

        assert stats.renamed == 1
        chain = self._chain(conn, 794002)
        assert chain[-1][0] == "BBBY.delisted"
        assert chain[-1][3] == "delisting"

    def test_same_day_flip_updates_in_place(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """A flip on the open row's own effective_from day cannot close
        it (zero-duration range fails the ordered-ranges CHECK) — it is
        corrected in place instead."""
        conn = ebull_test_conn
        self._seed_with_history(conn, iid=794003, symbol="TYPO", opened_days_ago=0)
        conn.execute("UPDATE instruments SET symbol = 'FIXED' WHERE instrument_id = 794003")

        stats = instrument_history.reconcile_symbol_history(conn)
        conn.commit()

        assert stats.corrected_same_day == 1
        assert stats.renamed == 0
        chain = self._chain(conn, 794003)
        assert len(chain) == 1
        assert chain[0][0] == "FIXED"
        assert chain[0][2] is None

    def test_reconcile_is_idempotent(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        conn = ebull_test_conn
        self._seed_with_history(conn, iid=794004, symbol="FB", opened_days_ago=30)
        conn.execute("UPDATE instruments SET symbol = 'META' WHERE instrument_id = 794004")

        instrument_history.reconcile_symbol_history(conn)
        second = instrument_history.reconcile_symbol_history(conn)
        conn.commit()

        assert second.renamed == 0
        assert second.corrected_same_day == 0
        assert len(self._chain(conn, 794004)) == 2

    def test_seed_pass_covers_history_less_instrument(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        conn = ebull_test_conn
        _seed_instrument(conn, iid=794005, symbol="NEW")

        stats = instrument_history.reconcile_symbol_history(conn)
        conn.commit()

        assert stats.seeded >= 1
        chain = self._chain(conn, 794005)
        assert len(chain) == 1
        assert chain[0][0] == "NEW"
        assert chain[0][3] == "imported"

    def test_backfill_rerun_after_rename_does_not_violate_exclude(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """Regression: the old ON CONFLICT DO NOTHING backfill re-insert
        landed on a different PK after a rename and tripped the EXCLUDE
        no-overlap constraint. NOT EXISTS skips chained instruments."""
        conn = ebull_test_conn
        self._seed_with_history(conn, iid=794006, symbol="FB", opened_days_ago=30)
        conn.execute("UPDATE instruments SET symbol = 'META' WHERE instrument_id = 794006")
        instrument_history.reconcile_symbol_history(conn)

        cik_rows, sym_rows = instrument_history.backfill_current_history(conn)
        conn.commit()

        chain = self._chain(conn, 794006)
        assert len(chain) == 2  # backfill added nothing to the chained instrument

    def test_reused_symbol_chains_stay_separate(
        self,
        ebull_test_conn: psycopg.Connection[tuple],  # noqa: F811
    ) -> None:
        """#794 AC4 — a symbol abandoned by one instrument and later
        assigned to another must produce two unlinked chains."""
        conn = ebull_test_conn
        self._seed_with_history(conn, iid=794007, symbol="DEAD", opened_days_ago=30)
        conn.execute("UPDATE instruments SET symbol = 'ALIVE' WHERE instrument_id = 794007")
        instrument_history.reconcile_symbol_history(conn)
        # New issuer picks up the abandoned ticker.
        _seed_instrument(conn, iid=794008, symbol="DEAD")
        instrument_history.reconcile_symbol_history(conn)
        conn.commit()

        a = self._chain(conn, 794007)
        b = self._chain(conn, 794008)
        assert [r[0] for r in a] == ["DEAD", "ALIVE"]
        assert [r[0] for r in b] == ["DEAD"]
        # The two DEAD rows are keyed to different instruments — no join.
        owners = conn.execute(
            "SELECT DISTINCT instrument_id FROM instrument_symbol_history "
            "WHERE symbol = 'DEAD' AND instrument_id IN (794007, 794008)"
        ).fetchall()
        assert len(owners) == 2
