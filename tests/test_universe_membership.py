"""Integration tests for the append-only universe-membership record (#2290).

Two halves:

- the DB-level temporal invariants (ordered / single-current / no-overlap),
  plus the CHECK that is the whole point of the ticket — a closed row's
  ``effective_to`` must equal ``last_confirmed_on``, so a detection date
  can never be stamped there;
- ``reconcile_universe_membership``, which is a pure function of current
  table state and so is exercised by writing the state directly rather
  than by driving a provider.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

import psycopg
import pytest

from app.services.universe_membership import reconcile_universe_membership
from tests.fixtures.ebull_test_db import ebull_test_conn  # noqa: F401 — fixture re-export

pytestmark = pytest.mark.integration


def _seed_instrument(
    conn: psycopg.Connection[tuple[Any, ...]],
    *,
    iid: int,
    symbol: str,
    is_tradable: bool = True,
    first_seen_days_ago: int = 30,
) -> None:
    """Insert an instrument. ``first_seen_days_ago`` matters: the reconcile
    reads it to tell a genuinely-new listing from one that predates the
    membership table."""
    conn.execute(
        """
        INSERT INTO instruments (
            instrument_id, symbol, company_name, exchange, currency,
            is_tradable, first_seen_at, last_seen_at
        ) VALUES (
            %s, %s, %s, '4', 'USD', %s,
            NOW() - make_interval(days => %s), NOW()
        )
        ON CONFLICT (instrument_id) DO UPDATE SET is_tradable = EXCLUDED.is_tradable
        """,
        (iid, symbol, f"{symbol} Inc", is_tradable, first_seen_days_ago),
    )


def _seed_membership(
    conn: psycopg.Connection[tuple[Any, ...]],
    *,
    iid: int,
    from_days_ago: int,
    confirmed_days_ago: int,
    to_days_ago: int | None = None,
    source_event: str = "imported",
) -> None:
    conn.execute(
        """
        INSERT INTO instrument_universe_membership (
            instrument_id, effective_from, effective_to,
            last_confirmed_on, source_event
        ) VALUES (
            %s,
            CURRENT_DATE - %s::int,
            CASE WHEN %s::int IS NULL THEN NULL ELSE CURRENT_DATE - %s::int END,
            CURRENT_DATE - %s::int,
            %s
        )
        """,
        (iid, from_days_ago, to_days_ago, to_days_ago, confirmed_days_ago, source_event),
    )


def _rows(conn: psycopg.Connection[tuple[Any, ...]], iid: int) -> list[tuple[date, date | None, date, str]]:
    """Membership rows for one instrument, oldest first."""
    return [
        (r[0], r[1], r[2], r[3])
        for r in conn.execute(
            """
            SELECT effective_from, effective_to, last_confirmed_on, source_event
            FROM instrument_universe_membership
            WHERE instrument_id = %s
            ORDER BY effective_from
            """,
            (iid,),
        ).fetchall()
    ]


def _today(conn: psycopg.Connection[tuple[Any, ...]]) -> date:
    row = conn.execute("SELECT CURRENT_DATE").fetchone()
    assert row is not None
    return row[0]  # type: ignore[no-any-return]


class TestMembershipConstraints:
    @pytest.fixture
    def _setup(
        self,
        ebull_test_conn: psycopg.Connection[tuple[Any, ...]],  # noqa: F811
    ) -> psycopg.Connection[tuple[Any, ...]]:
        conn = ebull_test_conn
        _seed_instrument(conn, iid=2_290_001, symbol="MEMB")
        conn.commit()
        return conn

    def test_inverted_range_rejected(self, _setup: psycopg.Connection[tuple[Any, ...]]) -> None:
        """``effective_to < effective_from`` violates the ordered CHECK."""
        conn = _setup
        with pytest.raises(psycopg.errors.CheckViolation):
            with conn.transaction():
                _seed_membership(conn, iid=2_290_001, from_days_ago=5, confirmed_days_ago=9, to_days_ago=9)

    def test_single_day_range_accepted(self, _setup: psycopg.Connection[tuple[Any, ...]]) -> None:
        """``effective_to = effective_from`` is a ONE-DAY membership, not an
        inverted range — the inclusive-bound convention exists so this
        transition survives instead of being deleted."""
        conn = _setup
        _seed_membership(conn, iid=2_290_001, from_days_ago=5, confirmed_days_ago=5, to_days_ago=5)
        conn.commit()
        assert len(_rows(conn, 2_290_001)) == 1

    def test_closed_row_must_end_at_last_confirmed(self, _setup: psycopg.Connection[tuple[Any, ...]]) -> None:
        """⚠ The CHECK that IS the ticket. Closing at the DETECTION date
        (today) while the last confirmed presence was earlier is exactly the
        defect #2290 was filed for, and the constraint rejects it."""
        conn = _setup
        with pytest.raises(psycopg.errors.CheckViolation):
            with conn.transaction():
                conn.execute(
                    """
                    INSERT INTO instrument_universe_membership (
                        instrument_id, effective_from, effective_to,
                        last_confirmed_on, source_event
                    ) VALUES (
                        %s, CURRENT_DATE - 30, CURRENT_DATE,
                        CURRENT_DATE - 7, 'imported'
                    )
                    """,
                    (2_290_001,),
                )

    def test_two_open_rows_rejected(self, _setup: psycopg.Connection[tuple[Any, ...]]) -> None:
        """The partial unique index allows at most one open row."""
        conn = _setup
        _seed_membership(conn, iid=2_290_001, from_days_ago=30, confirmed_days_ago=0)
        conn.commit()
        with pytest.raises(psycopg.errors.UniqueViolation):
            with conn.transaction():
                _seed_membership(conn, iid=2_290_001, from_days_ago=10, confirmed_days_ago=0)

    def test_overlapping_ranges_rejected(self, _setup: psycopg.Connection[tuple[Any, ...]]) -> None:
        """The GIST EXCLUDE rejects two episodes covering a shared day."""
        conn = _setup
        _seed_membership(conn, iid=2_290_001, from_days_ago=30, confirmed_days_ago=10, to_days_ago=10)
        conn.commit()
        with pytest.raises(psycopg.errors.ExclusionViolation):
            with conn.transaction():
                # Opens ON the closed row's last membership day. Under an
                # inclusive upper bound that day belongs to both rows.
                _seed_membership(
                    conn,
                    iid=2_290_001,
                    from_days_ago=10,
                    confirmed_days_ago=0,
                    source_event="relisting",
                )

    def test_abutting_ranges_accepted(self, _setup: psycopg.Connection[tuple[Any, ...]]) -> None:
        """A relisting the DAY AFTER the last membership day is not an overlap."""
        conn = _setup
        _seed_membership(conn, iid=2_290_001, from_days_ago=30, confirmed_days_ago=10, to_days_ago=10)
        _seed_membership(
            conn,
            iid=2_290_001,
            from_days_ago=9,
            confirmed_days_ago=0,
            source_event="relisting",
        )
        conn.commit()
        assert len(_rows(conn, 2_290_001)) == 2

    def test_unknown_source_event_rejected(self, _setup: psycopg.Connection[tuple[Any, ...]]) -> None:
        conn = _setup
        with pytest.raises(psycopg.errors.CheckViolation):
            with conn.transaction():
                _seed_membership(
                    conn,
                    iid=2_290_001,
                    from_days_ago=5,
                    confirmed_days_ago=0,
                    source_event="delisting",
                )


class TestReconcile:
    @pytest.fixture
    def conn(
        self,
        ebull_test_conn: psycopg.Connection[tuple[Any, ...]],  # noqa: F811
    ) -> psycopg.Connection[tuple[Any, ...]]:
        return ebull_test_conn

    def test_seeds_pre_existing_tradable_as_imported(self, conn: psycopg.Connection[tuple[Any, ...]]) -> None:
        """An instrument already tradable when the table was created gets an
        ``imported`` row: its true membership start predates the record and is
        truncated here rather than invented."""
        _seed_instrument(conn, iid=2_290_010, symbol="IMPT", first_seen_days_ago=200)
        conn.commit()

        stats = reconcile_universe_membership(conn)
        conn.commit()

        assert stats.imported == 1
        assert _rows(conn, 2_290_010) == [(_today(conn), None, _today(conn), "imported")]

    def test_new_instrument_today_is_a_listing(self, conn: psycopg.Connection[tuple[Any, ...]]) -> None:
        """First seen today → ``listing``: the membership start is real, not truncated."""
        _seed_instrument(conn, iid=2_290_011, symbol="LIST", first_seen_days_ago=0)
        conn.commit()

        stats = reconcile_universe_membership(conn)
        conn.commit()

        assert stats.listed == 1
        assert _rows(conn, 2_290_011) == [(_today(conn), None, _today(conn), "listing")]

    def test_confirm_bumps_last_confirmed_on(self, conn: psycopg.Connection[tuple[Any, ...]]) -> None:
        _seed_instrument(conn, iid=2_290_012, symbol="CONF")
        _seed_membership(conn, iid=2_290_012, from_days_ago=30, confirmed_days_ago=1)
        conn.commit()

        stats = reconcile_universe_membership(conn)
        conn.commit()

        assert stats.confirmed == 1
        today = _today(conn)
        assert _rows(conn, 2_290_012) == [(today - timedelta(days=30), None, today, "imported")]

    def test_second_sync_same_day_does_not_rewrite(self, conn: psycopg.Connection[tuple[Any, ...]]) -> None:
        """Rows already confirmed today are skipped, so an operator-triggered
        second sync does not rewrite the whole universe."""
        _seed_instrument(conn, iid=2_290_013, symbol="TWCE")
        _seed_membership(conn, iid=2_290_013, from_days_ago=30, confirmed_days_ago=0)
        conn.commit()

        stats = reconcile_universe_membership(conn)
        conn.commit()

        assert stats.confirmed == 0

    def test_close_uses_last_confirmed_not_today(self, conn: psycopg.Connection[tuple[Any, ...]]) -> None:
        """⚠ Acceptance criterion 2. The instrument was last returned by the
        feed 7 days ago; we are noticing today. ``effective_to`` must be the
        former."""
        _seed_instrument(conn, iid=2_290_014, symbol="GONE", is_tradable=False)
        _seed_membership(conn, iid=2_290_014, from_days_ago=90, confirmed_days_ago=7)
        conn.commit()

        stats = reconcile_universe_membership(conn)
        conn.commit()

        assert stats.closed == 1
        today = _today(conn)
        assert _rows(conn, 2_290_014) == [
            (today - timedelta(days=90), today - timedelta(days=7), today - timedelta(days=7), "imported")
        ]

    def test_one_day_membership_closes_rather_than_vanishing(self, conn: psycopg.Connection[tuple[Any, ...]]) -> None:
        """Appeared on day D, gone by the next sync: ``last_confirmed_on ==
        effective_from``. The row must close as the single-day range [D, D],
        not be discarded — a one-day membership is exactly the kind of
        transition this table exists to record."""
        _seed_instrument(conn, iid=2_290_015, symbol="BLIP", is_tradable=False)
        _seed_membership(conn, iid=2_290_015, from_days_ago=3, confirmed_days_ago=3, source_event="listing")
        conn.commit()

        stats = reconcile_universe_membership(conn)
        conn.commit()

        assert stats.closed == 1
        d = _today(conn) - timedelta(days=3)
        assert _rows(conn, 2_290_015) == [(d, d, d, "listing")]

    def test_relisting_opens_a_second_row(self, conn: psycopg.Connection[tuple[Any, ...]]) -> None:
        """⚠ Acceptance criterion 3. The closed row is never mutated — ticker
        reuse means the two episodes are not necessarily the same business."""
        _seed_instrument(conn, iid=2_290_016, symbol="BACK")
        _seed_membership(conn, iid=2_290_016, from_days_ago=90, confirmed_days_ago=40, to_days_ago=40)
        conn.commit()

        stats = reconcile_universe_membership(conn)
        conn.commit()

        assert stats.relisted == 1
        today = _today(conn)
        assert _rows(conn, 2_290_016) == [
            (today - timedelta(days=90), today - timedelta(days=40), today - timedelta(days=40), "imported"),
            (today, None, today, "relisting"),
        ]

    def test_dormant_at_seed_time_returns_as_a_relisting(self, conn: psycopg.Connection[tuple[Any, ...]]) -> None:
        """⚠ An instrument already ``is_tradable = FALSE`` when the table was
        seeded has NO membership row, and its ``first_seen_at`` is old. When it
        comes back it must still be a ``relisting`` — labelling it ``imported``
        would claim it was tradable at seed time, which is exactly false, and
        would lose the return transition.

        Caught by Codex checkpoint 2; the earlier CASE fell through to
        ``imported`` for this shape."""
        # Another instrument seeded the table first, so this is not the seed run.
        _seed_instrument(conn, iid=2_290_021, symbol="INCUMB", first_seen_days_ago=300)
        _seed_membership(conn, iid=2_290_021, from_days_ago=100, confirmed_days_ago=0)
        # The dormant one: old, no membership row, now back in the feed.
        _seed_instrument(conn, iid=2_290_022, symbol="DORMANT", first_seen_days_ago=300)
        conn.commit()

        stats = reconcile_universe_membership(conn)
        conn.commit()

        assert stats.imported == 0
        assert stats.relisted == 1
        assert _rows(conn, 2_290_022) == [(_today(conn), None, _today(conn), "relisting")]

    def test_same_day_flip_flop_reopens_rather_than_splitting(self, conn: psycopg.Connection[tuple[Any, ...]]) -> None:
        """Present, absent, present within one day is a provider flip-flop, not
        a relisting: there is no day on which the instrument was absent, so the
        closed row reopens instead of a second row being minted."""
        _seed_instrument(conn, iid=2_290_017, symbol="FLIP")
        _seed_membership(conn, iid=2_290_017, from_days_ago=30, confirmed_days_ago=0, to_days_ago=0)
        conn.commit()

        stats = reconcile_universe_membership(conn)
        conn.commit()

        assert stats.reopened_same_day == 1
        assert stats.relisted == 0
        today = _today(conn)
        assert _rows(conn, 2_290_017) == [(today - timedelta(days=30), None, today, "imported")]

    def test_untradable_instrument_gets_no_row(self, conn: psycopg.Connection[tuple[Any, ...]]) -> None:
        """Nothing is invented for the instruments that were already
        deactivated before the table existed — #2290's no-backfill clause."""
        _seed_instrument(conn, iid=2_290_018, symbol="DEAD", is_tradable=False)
        conn.commit()

        reconcile_universe_membership(conn)
        conn.commit()

        assert _rows(conn, 2_290_018) == []

    def test_point_in_time_membership_query(self, conn: psycopg.Connection[tuple[Any, ...]]) -> None:
        """⚠ Acceptance criterion 4 — 'which instruments were tradable on D'
        is one range-containment query, in the form that uses the GIST index
        the no-overlap EXCLUDE already builds."""
        _seed_instrument(conn, iid=2_290_019, symbol="WASIN", is_tradable=False)
        _seed_instrument(conn, iid=2_290_020, symbol="ISNOW")
        _seed_membership(conn, iid=2_290_019, from_days_ago=60, confirmed_days_ago=20, to_days_ago=20)
        _seed_membership(conn, iid=2_290_020, from_days_ago=5, confirmed_days_ago=0)
        conn.commit()

        def members_on(days_ago: int) -> set[int]:
            return {
                int(r[0])
                for r in conn.execute(
                    """
                    SELECT instrument_id
                    FROM instrument_universe_membership
                    WHERE daterange(effective_from, effective_to, '[]')
                          @> (CURRENT_DATE - %s::int)
                      AND instrument_id IN (2290019, 2290020)
                    """,
                    (days_ago,),
                ).fetchall()
            }

        assert members_on(30) == {2_290_019}  # closed episode still answers
        assert members_on(20) == {2_290_019}  # inclusive: the last day counts
        assert members_on(19) == set()  # the day after it left
        assert members_on(0) == {2_290_020}
