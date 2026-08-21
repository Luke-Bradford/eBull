"""#2811 — the scan republishes the decision calendar it actually used.

Its own module because ``tests/conftest.py`` applies the ``db`` marker per MODULE:
putting these beside the pure window/frontier cases in
``test_strategy_signal_scan.py`` would drop every one of those off the fast push
gate.

The reason this table exists rather than the reader recomputing the calendar is in
``publish_decision_calendar``'s docstring: recomputing resolves a HISTORICAL
``strategy_version`` against today's universe, corpus and quarantine state.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import psycopg
import pytest

from app.services.strategy_monitoring import load_fire_rate
from app.services.strategy_signal_scan import publish_decision_calendar


def _published(conn: psycopg.Connection[tuple], strategy_id: str, version: str) -> date | None:
    """The publication header's frontier, or None if this version never published."""
    with conn.cursor() as cur:
        cur.execute(
            """SELECT frontier_date FROM strategy_decision_calendar_publications
               WHERE strategy_id = %s AND strategy_version = %s""",
            (strategy_id, version),
        )
        row = cur.fetchone()
        return row[0] if row else None


def _stored(conn: psycopg.Connection[tuple], strategy_id: str, version: str) -> set[date]:
    with conn.cursor() as cur:
        cur.execute(
            """SELECT decision_date FROM strategy_decision_calendar
               WHERE strategy_id = %s AND strategy_version = %s""",
            (strategy_id, version),
        )
        return {row[0] for row in cur.fetchall()}


@pytest.mark.db
class TestPublishReplaces:
    """REPLACE, not merge — the calendar is a function of the corpus.

    Safe here in a way it would not be for a signal: this table carries no verdict,
    no instrument and no fill, so nothing is a decision record and nothing is being
    withdrawn.
    """

    def test_a_date_that_stops_qualifying_stops_being_published(
        self, ebull_test_conn: psycopg.Connection[tuple]
    ) -> None:
        conn = ebull_test_conn
        version = "test-cal-v1+replace"
        with conn.cursor():
            publish_decision_calendar(
                conn,
                strategy_id="s2-cross-sectional-momentum",
                strategy_version=version,
                frontier_date=date(2026, 8, 19),
                decision_dates=frozenset({date(2026, 8, 1), date(2026, 9, 1)}),
            )
            assert _stored(conn, "s2-cross-sectional-momentum", version) == {
                date(2026, 8, 1),
                date(2026, 9, 1),
            }

            # #2797's shape: a weekend bar had taken the month, and the corrected
            # rule moves the rebalance to the Monday. A merge would leave BOTH,
            # and the reader would count a decision date the scan never used.
            publish_decision_calendar(
                conn,
                strategy_id="s2-cross-sectional-momentum",
                strategy_version=version,
                frontier_date=date(2026, 8, 20),
                decision_dates=frozenset({date(2026, 8, 3), date(2026, 9, 1)}),
            )
            assert _stored(conn, "s2-cross-sectional-momentum", version) == {
                date(2026, 8, 3),
                date(2026, 9, 1),
            }

    def test_an_empty_calendar_clears_rather_than_no_ops(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        conn = ebull_test_conn
        version = "test-cal-v1+empty"
        publish_decision_calendar(
            conn,
            strategy_id="s2-cross-sectional-momentum",
            strategy_version=version,
            frontier_date=date(2026, 8, 19),
            decision_dates=frozenset({date(2026, 8, 3)}),
        )
        publish_decision_calendar(
            conn,
            strategy_id="s2-cross-sectional-momentum",
            strategy_version=version,
            frontier_date=date(2026, 8, 19),
            decision_dates=frozenset(),
        )
        # ⚠ Genuinely EMPTY, which the reader must not confuse with ABSENT: an empty
        # published calendar is "this rule names no date in this corpus", absent is
        # "no calendar is known". The HEADER is what carries that — an earlier cut of
        # this test asserted the distinction while storing it in a row count, where
        # both sides are zero rows (Codex, checkpoint 2).
        assert _stored(conn, "s2-cross-sectional-momentum", version) == set()
        assert _published(conn, "s2-cross-sectional-momentum", version) == date(2026, 8, 19)

    def test_publishing_one_version_leaves_its_siblings_alone(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        # The rebalance rule is part of the identity and #2797 changed S-2's, so
        # two versions legitimately hold different calendars at the same time.
        conn = ebull_test_conn
        old, new = "test-cal-v1+old", "test-cal-v1+new"
        publish_decision_calendar(
            conn,
            strategy_id="s2-cross-sectional-momentum",
            strategy_version=old,
            frontier_date=date(2026, 8, 19),
            decision_dates=frozenset({date(2026, 8, 1)}),
        )
        publish_decision_calendar(
            conn,
            strategy_id="s2-cross-sectional-momentum",
            strategy_version=new,
            frontier_date=date(2026, 8, 19),
            decision_dates=frozenset({date(2026, 8, 3)}),
        )
        assert _stored(conn, "s2-cross-sectional-momentum", old) == {date(2026, 8, 1)}
        assert _stored(conn, "s2-cross-sectional-momentum", new) == {date(2026, 8, 3)}


@pytest.mark.db
class TestKnownEmptyIsNotUnknown:
    """The distinction the header exists for, asserted through the READER.

    Storing it is only half: `load_fire_rate` has to come back with `0` rather than
    `None`, or the header is a row nothing consults.
    """

    def test_a_published_empty_calendar_refuses_the_share(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        conn = ebull_test_conn
        version = "test-cal-v1+known-empty"
        sid = "s2-cross-sectional-momentum"
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO strategy_signal_daily_counts
                    (strategy_id, strategy_version, signal_bar_date,
                     signal_kind, verdict, reason_code, row_count)
                VALUES (%s, %s, DATE '2026-08-17', 'entry', 'not_fired', '', 3277)
                """,
                (sid, version),
            )
        publish_decision_calendar(
            conn,
            strategy_id=sid,
            strategy_version=version,
            frontier_date=date(2026, 8, 19),
            decision_dates=frozenset(),
        )
        rate = load_fire_rate(conn, versions=[version])[(sid, version)]
        assert rate.decision_days == 0, "published and empty is a KNOWN zero, not an unknown cadence"
        assert rate.share_unavailable_reason == "no_decision_date_scanned"

    def test_a_version_that_never_published_leaves_the_share_alone(
        self, ebull_test_conn: psycopg.Connection[tuple]
    ) -> None:
        conn = ebull_test_conn
        version = "test-cal-v1+unpublished"
        sid = "s2-cross-sectional-momentum"
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO strategy_signal_daily_counts
                    (strategy_id, strategy_version, signal_bar_date,
                     signal_kind, verdict, reason_code, row_count)
                VALUES (%s, %s, DATE '2026-08-17', 'entry', 'fired', '', 100),
                       (%s, %s, DATE '2026-08-17', 'entry', 'not_fired', '', 900)
                """,
                (sid, version, sid, version),
            )
        # Same strategy id, same census shape, no publication — and the ONLY
        # difference in the answer is the header row.
        rate = load_fire_rate(conn, versions=[version])[(sid, version)]
        assert rate.decision_days is None
        assert rate.fired_share_of_evaluable == Decimal("0.1000")
        assert rate.share_unavailable_reason is None
