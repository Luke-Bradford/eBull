"""#3619 slice 2 — the month-end SQL against a real schema: last usable bar, quarantine, coverage."""

from __future__ import annotations

from datetime import date
from typing import Any

import psycopg

from app.services.price_quarantine import RULE_SET_VERSION
from app.services.total_return_reader import load_month_ends

# (bar_date, close, adj_close)
_BARS = [
    (date(2024, 1, 30), 10.0, 9.0),
    (date(2024, 1, 31), 11.0, 10.0),  # quarantined: January's level falls back to the 30th
    (date(2024, 2, 27), 12.0, 11.0),
    (date(2024, 2, 28), float("nan"), 11.5),  # non-finite: February falls back to the 27th
    (date(2024, 3, 27), 13.0, 12.0),
    (date(2024, 3, 28), 14.0, 13.0),  # a flag row that leaves the bar usable
    (date(2024, 4, 15), 15.0, 14.0),  # after the coverage range: April has no level
]
_COVERED_THROUGH = date(2024, 3, 28)


def _seed(conn: psycopg.Connection[Any]) -> int:
    row = conn.execute(
        """
        INSERT INTO research_price_series
            (vendor, vendor_symbol, upstream_source, licence, adjustment_basis, first_bar, last_bar,
             bar_count, corporate_action_stamps)
        VALUES ('test/3619', 'T3619', 'unknown', 'test-fixture', 'unadjusted', %s, %s, %s, 'vendor_supplied')
        RETURNING series_id
        """,
        (_BARS[0][0], _BARS[-1][0], len(_BARS)),
    ).fetchone()
    assert row is not None
    series_id = int(row[0])
    for bar_date, close, adj_close in _BARS:
        conn.execute(
            """
            INSERT INTO research_price_daily (series_id, bar_date, open, high, low, close, volume, adj_close)
            VALUES (%s, %s, %s, %s, %s, %s, 1000, %s)
            """,
            (series_id, bar_date, close, close, close, close, adj_close),
        )
    conn.execute(
        """
        INSERT INTO research_price_quarantine_coverage
            (series_id, rule_set_version, quarantine_as_of, first_bar, last_bar, bars_evaluated,
             transitions_evaluated)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        (series_id, RULE_SET_VERSION, date(2026, 1, 1), _BARS[0][0], _COVERED_THROUGH, len(_BARS) - 1, len(_BARS) - 2),
    )
    for bar_date, return_usable, range_usable in ((_BARS[1][0], False, True), (_BARS[5][0], True, False)):
        conn.execute(
            """
            INSERT INTO research_bar_quarantine
                (series_id, bar_date, return_usable, range_usable, provisional, rules, rule_set_version)
            VALUES (%s, %s, %s, %s, FALSE, ARRAY['test'], %s)
            """,
            (series_id, bar_date, return_usable, range_usable, RULE_SET_VERSION),
        )
    conn.commit()
    return series_id


def test_month_end_is_the_last_usable_bar_inside_current_coverage(ebull_test_conn: psycopg.Connection[Any]) -> None:
    series_id = _seed(ebull_test_conn)
    ends = load_month_ends(ebull_test_conn, [series_id])[series_id]
    assert sorted(ends) == [(2024, 1), (2024, 2), (2024, 3)]  # April lies outside coverage
    assert ends[(2024, 1)].bar_date == date(2024, 1, 30)  # the 31st is return-unusable
    assert ends[(2024, 2)].bar_date == date(2024, 2, 27)
    assert ends[(2024, 3)].bar_date == date(2024, 3, 28)  # range-only flag: still return-usable
    assert (ends[(2024, 3)].close, ends[(2024, 3)].adj_close) == (14.0, 13.0)


def test_a_stale_coverage_version_reads_nothing(ebull_test_conn: psycopg.Connection[Any]) -> None:
    series_id = _seed(ebull_test_conn)
    ebull_test_conn.execute(
        "UPDATE research_price_quarantine_coverage SET rule_set_version = 'old' WHERE series_id = %s", (series_id,)
    )
    ebull_test_conn.commit()
    assert load_month_ends(ebull_test_conn, [series_id]) == {}
