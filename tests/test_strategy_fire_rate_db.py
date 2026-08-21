"""#2623 gap 2 — the fire-rate loader reads the census, not the sparse ledger.

Separated from ``test_strategy_fire_rate.py`` because the ``db`` marker is applied
per MODULE (``tests/conftest.py``), and the pure derivation cases belong on the
fast gate. Spec: ``docs/proposals/ta/2026-08-14-strategy-fire-rate.md``.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date
from decimal import Decimal

import psycopg
import pytest

from app.services import strategy_monitoring
from app.services.strategy_monitoring import StrategyFireRate, load_fire_rate
from app.services.strategy_signal_scan import publish_decision_calendar


@pytest.mark.db
class TestLoaderReadsTheCensus:
    """The one thing a pure test cannot prove: WHICH table is read.

    ``strategy_signals`` retains only fired rows durably, so it can see only the
    days on which something fired. The fixture below reproduces the shape measured
    on the full population — a version whose negatives outlive its fires — and the
    two sources disagree on the denominator by 5x.
    """

    def test_scanned_days_come_from_the_census_not_the_sparse_signal_table(
        self, ebull_test_conn: psycopg.Connection[tuple]
    ) -> None:
        conn = ebull_test_conn
        version = "test-fire-rate-v1+census"
        with conn.cursor() as cur:
            # One fired day, four further days on which the strategy was
            # evaluated and did not fire. `strategy_signals` would see 1 day.
            cur.execute(
                """
                INSERT INTO strategy_signal_daily_counts
                    (strategy_id, strategy_version, signal_bar_date,
                     signal_kind, verdict, reason_code, row_count)
                VALUES
                    ('s-test', %(v)s, DATE '2026-07-28', 'entry', 'fired',     '', 40),
                    ('s-test', %(v)s, DATE '2026-07-29', 'entry', 'not_fired', '', 10),
                    ('s-test', %(v)s, DATE '2026-07-30', 'entry', 'not_fired', '', 10),
                    ('s-test', %(v)s, DATE '2026-08-03', 'entry', 'not_fired', '', 10),
                    ('s-test', %(v)s, DATE '2026-08-04', 'entry', 'not_fired', '', 10),
                    -- An exit leg on a date the entry axis does not contain:
                    -- it must not extend the entry span or the rate is diluted.
                    ('s-test', %(v)s, DATE '2026-08-20', 'exit',  'fired',     '', 99)
                """,
                {"v": version},
            )
            rates = load_fire_rate(conn, versions=[version])

        rate = rates[("s-test", version)]
        assert rate.scanned_days == 5, "census days, not the 1 day strategy_signals retains"
        assert rate.fired_days == 1
        assert rate.fired_entry_signals == 40
        assert rate.evaluable_entry_decisions == 80
        assert rate.last_scanned_bar == date(2026, 8, 4), "the exit leg must not extend the entry axis"
        # 40 fires over a 7-day span.
        assert rate.entries_per_calendar_week == Decimal("40.00")
        assert rate.fired_share_of_evaluable == Decimal("0.5000")
        assert rate.share_unavailable_reason is None
        assert rate.weekly_rate_unavailable_reason is None

    def test_a_version_absent_from_the_census_is_absent_from_the_result(
        self, ebull_test_conn: psycopg.Connection[tuple]
    ) -> None:
        assert load_fire_rate(ebull_test_conn, versions=["test-fire-rate-v1+never-scanned"]) == {}


@pytest.mark.db
class TestDecisionCalendarPartition:
    """#2811 — the loader must read the SCAN's published calendar, not recompute it.

    ``s2-cross-sectional-momentum`` is used by id on purpose: ``_panel_floor``
    resolves ``min_participants`` from the manifest, and a made-up id would floor at
    1 and hide the sparse-instrument case below.
    """

    def _census(
        self,
        cur: psycopg.Cursor[tuple],
        version: str,
        rows: Sequence[tuple[str, date, str, int]],
    ) -> None:
        """(strategy_id, bar_date, verdict, row_count) -> census rows, entry leg."""
        cur.executemany(
            """
            INSERT INTO strategy_signal_daily_counts
                (strategy_id, strategy_version, signal_bar_date,
                 signal_kind, verdict, reason_code, row_count)
            VALUES (%s, %s, %s, 'entry', %s, '', %s)
            """,
            [(strategy_id, version, bar, verdict, count) for strategy_id, bar, verdict, count in rows],
        )

    def test_non_decision_bars_leave_the_denominator(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        conn = ebull_test_conn
        version = "test-fire-rate-v1+calendar"
        sid = "s2-cross-sectional-momentum"
        with conn.cursor() as cur:
            # One rebalance date that fired, three ordinary bars that are
            # `not_fired` because the CALENDAR excluded them, not the rule.
            self._census(
                cur,
                version,
                [
                    (sid, date(2026, 8, 3), "fired", 20),
                    (sid, date(2026, 8, 3), "not_fired", 180),
                    (sid, date(2026, 8, 4), "not_fired", 5000),
                    (sid, date(2026, 8, 5), "not_fired", 5000),
                    (sid, date(2026, 8, 6), "not_fired", 5000),
                ],
            )
        publish_decision_calendar(
            conn,
            strategy_id=sid,
            strategy_version=version,
            frontier_date=date(2026, 8, 7),
            decision_dates=frozenset({date(2026, 8, 3)}),
        )
        rates = load_fire_rate(conn, versions=[version])

        rate = rates[(sid, version)]
        assert rate.scanned_days == 4, "coverage is every census bar date, unrestricted"
        assert rate.decision_days == 1
        # 20/200 on the rebalance date. Pooling the other three bars would give
        # 20/15,200 = 0.0013 — a measurement of the calendar, not the strategy.
        assert rate.evaluable_entry_decisions == 200
        assert rate.fired_share_of_evaluable == Decimal("0.1000")
        assert rate.share_unavailable_reason is None

    def test_a_published_calendar_the_scan_never_reached_refuses_the_share(
        self, ebull_test_conn: psycopg.Connection[tuple]
    ) -> None:
        # The live 2026-08-21 shape: thousands of non-decision bars, no covered
        # rebalance date, and the old code reported a confident 0.0000.
        conn = ebull_test_conn
        version = "test-fire-rate-v1+uncovered"
        sid = "s2-cross-sectional-momentum"
        with conn.cursor() as cur:
            self._census(cur, version, [(sid, date(2026, 8, 17), "not_fired", 3277)])
        publish_decision_calendar(
            conn,
            strategy_id=sid,
            strategy_version=version,
            frontier_date=date(2026, 9, 3),
            decision_dates=frozenset({date(2026, 9, 1)}),
        )
        rates = load_fire_rate(conn, versions=[version])

        rate = rates[(sid, version)]
        assert rate.decision_days == 0
        assert rate.fired_share_of_evaluable is None
        assert rate.share_unavailable_reason == "no_decision_date_scanned"
        assert rate.scanned_days == 1, "it WAS scanned; it was never asked"

    def test_one_sparse_instrument_on_a_rebalance_date_is_not_an_opportunity(
        self, ebull_test_conn: psycopg.Connection[tuple]
    ) -> None:
        """The root cause of #2811, reproduced one layer up.

        A cold start writes each instrument's last bar strictly before the
        frontier, so a single sparse series lands ONE row on an arbitrary date.
        Without the ``min_participants`` floor that row would set
        ``decision_days = 1`` and re-enable the fake share.
        """
        conn = ebull_test_conn
        version = "test-fire-rate-v1+sparse"
        sid = "s2-cross-sectional-momentum"
        with conn.cursor() as cur:
            self._census(
                cur,
                version,
                [
                    (sid, date(2026, 9, 1), "not_fired", 1),
                    (sid, date(2026, 9, 2), "not_fired", 5000),
                ],
            )
        publish_decision_calendar(
            conn,
            strategy_id=sid,
            strategy_version=version,
            frontier_date=date(2026, 9, 3),
            decision_dates=frozenset({date(2026, 9, 1)}),
        )
        rates = load_fire_rate(conn, versions=[version])

        rate = rates[(sid, version)]
        assert rate.decision_days == 0, "1 name cannot form a decile; S-2 refuses below 10"
        assert rate.share_unavailable_reason == "no_decision_date_scanned"

    def test_no_published_calendar_leaves_the_share_exactly_as_it_was(
        self, ebull_test_conn: psycopg.Connection[tuple]
    ) -> None:
        # The arm that keeps the 8 per-series strategies byte-identical.
        conn = ebull_test_conn
        version = "test-fire-rate-v1+nocalendar"
        with conn.cursor() as cur:
            self._census(
                cur,
                version,
                [
                    ("s1-time-series-momentum", date(2026, 8, 17), "fired", 100),
                    ("s1-time-series-momentum", date(2026, 8, 18), "not_fired", 900),
                ],
            )
            rates = load_fire_rate(conn, versions=[version])

        rate = rates[("s1-time-series-momentum", version)]
        assert rate.decision_days is None, "absent is UNKNOWN cadence, never an empty calendar"
        assert rate.fired_share_of_evaluable == Decimal("0.1000")
        assert rate.share_unavailable_reason is None


@pytest.mark.db
class TestOneCorruptVersionDoesNotBlankThePage:
    """Review bot WARNING on PR #2812 — `derive_fire_rate` raises, by design.

    That is right for the derivation and wrong to let escape: this loop feeds an
    AGGREGATE endpoint, so an uncaught raise turns one bad row into ten empty
    cards. `_commit_strategy` already settles the shape for the writer — *"One
    strategy's failure does not stop the others."*

    ⚠⚠ THE RAISE IS UNREACHABLE FROM `load_fire_rate` TODAY, and saying so is the
    honest form. Every invariant holds by CONSTRUCTION in the current fold:
    `fired_days` and `decision_days` are both counted over `counted`, so the first
    cannot exceed the second; `counted` is a subset of the version's rows, so
    `decision_days <= scanned_days`; and `_FIRE_RATE_SQL` computes evaluable as
    `fired + not_fired`, so the numerator cannot exceed its denominator. The guard
    is defence against a FUTURE fold, which is exactly when nobody will be looking.
    That is why this patches the raise in rather than fabricating a row shape that
    cannot occur — a test that manufactures an impossible input to reach a branch
    proves the branch, not the isolation.
    """

    def test_a_violated_invariant_refuses_one_card_and_leaves_the_rest(
        self, ebull_test_conn: psycopg.Connection[tuple], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        conn = ebull_test_conn
        bad, good = "test-fire-rate-v1+corrupt", "test-fire-rate-v1+healthy"
        bad_id = "s2-cross-sectional-momentum"
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO strategy_signal_daily_counts
                    (strategy_id, strategy_version, signal_bar_date,
                     signal_kind, verdict, reason_code, row_count)
                VALUES (%s, %s, %s, 'entry', %s, '', %s)
                """,
                [
                    (bad_id, bad, date(2026, 8, 17), "fired", 5),
                    (bad_id, bad, date(2026, 8, 17), "not_fired", 95),
                    ("s1-time-series-momentum", good, date(2026, 8, 17), "fired", 100),
                    ("s1-time-series-momentum", good, date(2026, 8, 18), "not_fired", 900),
                ],
            )

        real = strategy_monitoring.derive_fire_rate

        def raise_for_the_corrupt_one(**kwargs: object) -> StrategyFireRate:
            if kwargs["decision_days"] is not None:
                raise ValueError("simulated producer-invariant violation")
            return real(**kwargs)  # type: ignore[arg-type]

        publish_decision_calendar(
            conn,
            strategy_id=bad_id,
            strategy_version=bad,
            frontier_date=date(2026, 8, 19),
            decision_dates=frozenset({date(2026, 8, 17)}),
        )
        monkeypatch.setattr(strategy_monitoring, "derive_fire_rate", raise_for_the_corrupt_one)
        rates = load_fire_rate(conn, versions=[bad, good])

        corrupt = rates[(bad_id, bad)]
        assert corrupt.share_unavailable_reason == "invariant_violated"
        assert corrupt.weekly_rate_unavailable_reason == "invariant_violated", (
            "a value is None iff its reason is not — refusing one and not the other is the same defect one field over"
        )
        assert corrupt.fired_share_of_evaluable is None
        assert corrupt.entries_per_calendar_week is None
        # Coverage is still reported: it came off the census, not the derivation.
        assert corrupt.scanned_days == 1

        # ⚠ The point of the test: the healthy sibling is still MEASURED. Before the
        # per-version catch this call raised and the endpoint returned nothing.
        healthy = rates[("s1-time-series-momentum", good)]
        assert healthy.fired_share_of_evaluable == Decimal("0.1000")
        assert healthy.share_unavailable_reason is None
