"""#2623 gap 2 — the fire-rate loader reads the census, not the sparse ledger.

Separated from ``test_strategy_fire_rate.py`` because the ``db`` marker is applied
per MODULE (``tests/conftest.py``), and the pure derivation cases belong on the
fast gate. Spec: ``docs/proposals/ta/2026-08-14-strategy-fire-rate.md``.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import psycopg
import pytest

from app.services.strategy_monitoring import load_fire_rate


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
