"""Phase 3c — the writer against a real database.

⚠ Deliberately thin. The table's constraint set is already exercised in
``test_strategy_signals_ledger.py`` and the fill arithmetic in
``test_signal_ledger.py``; what is left, and what only a real database can
show, is two things:

1. Acceptance 7 — the STORED fill price equals ``open(t+1)`` as ``price_daily``
   holds it. A pure-logic test compares the writer against its own fixture; this
   compares it against the table, with the series read back out of the database
   the way phase 5 will read it.
2. The writer refusing to upsert. This cannot be tested with a mocked cursor:
   asserting that the SQL text lacks ``ON CONFLICT`` proves nothing about what
   the database does with a second insert, which is the prevention log's
   "SQL-shape tests on single-path calls can't exercise the ON CONFLICT
   branch". The collision has to actually happen.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import psycopg
import pytest

from app.services.indicator_series import BarSeries
from app.services.signal_ledger import resolve_fills, store_signals
from app.services.strategy_observation_storage import store_strategy_observations
from app.services.strategy_registry import StrategyIdentity, StrategySignal
from app.services.technical_analysis import OHLCVRow

_IDENTITY = StrategyIdentity(
    strategy_id="S-TEST-DB",
    params={"period": 14},
    universe="survivor_only",
    cost_model_id="static-v1",
    source_hash="deadbeef",
)

# 2024-01-05 is a Friday, 2024-01-08 the following Monday. The fill for the
# Friday signal must be the Monday bar, not 2024-01-06.
_BARS = [
    (date(2024, 1, 4), Decimal("100.10")),
    (date(2024, 1, 5), Decimal("101.20")),
    (date(2024, 1, 8), Decimal("102.30")),
    (date(2024, 1, 9), Decimal("103.40")),
]


@pytest.fixture
def instrument_with_a_calendar_gap(ebull_test_conn: psycopg.Connection[tuple]) -> int:
    # ⚠ instruments.instrument_id is eToro's identifier, assigned upstream —
    # not a serial, so it must be supplied.
    ebull_test_conn.execute(
        "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable) "
        "VALUES (999002, 'GAPX', 'Gap Test Co', TRUE)"
    )
    with ebull_test_conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO price_daily (instrument_id, price_date, open, high, low, close, volume) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s)",
            [(999002, d, o, o + 1, o - 1, o, 1000) for d, o in _BARS],
        )
    ebull_test_conn.commit()
    return 999002


def _series_from_db(conn: psycopg.Connection[tuple], instrument_id: int) -> BarSeries:
    """Read the bars back the way a phase-5 runner would, rather than reusing
    the fixture tuples — otherwise the test compares the writer against itself."""
    rows = conn.execute(
        "SELECT price_date, open, high, low, close, volume FROM price_daily "
        "WHERE instrument_id = %s ORDER BY price_date",
        (instrument_id,),
    ).fetchall()
    bars: list[OHLCVRow] = [{"open": r[1], "high": r[2], "low": r[3], "close": r[4], "volume": r[5]} for r in rows]
    return BarSeries(dates=tuple(r[0] for r in rows), rows=tuple(bars))


def test_stored_fill_price_is_open_of_the_next_bar_in_price_daily(
    ebull_test_conn: psycopg.Connection[tuple], instrument_with_a_calendar_gap: int
) -> None:
    """Acceptance 7 and 2 together, resolved against the table itself."""
    instrument_id = instrument_with_a_calendar_gap
    series = _series_from_db(ebull_test_conn, instrument_id)
    rows = resolve_fills(
        [StrategySignal(verdict="fired", signal_index=i) for i in range(len(series))],
        series=series,
        identity=_IDENTITY,
        instrument_id=instrument_id,
    )
    report = store_strategy_observations(ebull_test_conn, rows)
    assert report.logical_rows == len(rows)
    assert report.fired_rows == 3
    assert report.retained_observation_rows == 1
    ebull_test_conn.commit()

    # The join is the assertion: every stored fill must equal price_daily's
    # OPEN on the fill bar, and that fill bar must be the next bar present.
    mismatches = ebull_test_conn.execute(
        """
        SELECT s.signal_bar_date, s.fill_bar_date, s.fill_price, p.open, expected.next_date, expected.next_open
        FROM strategy_signals s
        JOIN price_daily p ON p.instrument_id = s.instrument_id AND p.price_date = s.fill_bar_date
        JOIN (
            SELECT price_date,
                   lead(price_date) OVER (ORDER BY price_date) AS next_date,
                   lead(open)       OVER (ORDER BY price_date) AS next_open
            FROM price_daily WHERE instrument_id = %(instrument_id)s
        ) expected ON expected.price_date = s.signal_bar_date
        WHERE s.instrument_id = %(instrument_id)s
          AND (s.fill_price <> p.open OR s.fill_bar_date <> expected.next_date
               OR s.fill_price <> expected.next_open)
        """,
        {"instrument_id": instrument_id},
    ).fetchall()
    assert mismatches == []

    stored = ebull_test_conn.execute(
        "SELECT signal_bar_date, verdict, not_evaluable_reason, fill_bar_date, fill_price "
        "FROM strategy_signals WHERE instrument_id = %s ORDER BY signal_bar_date",
        (instrument_id,),
    ).fetchall()
    assert stored == [
        (date(2024, 1, 4), "fired", None, date(2024, 1, 5), Decimal("101.20")),
        # Friday → Monday. `signal_bar_date + 1 day` would be 2024-01-06.
        (date(2024, 1, 5), "fired", None, date(2024, 1, 8), Decimal("102.30")),
        (date(2024, 1, 8), "fired", None, date(2024, 1, 9), Decimal("103.40")),
    ]
    retained = ebull_test_conn.execute(
        "SELECT signal_bar_date, verdict, reason_code FROM strategy_signal_observations WHERE instrument_id = %s",
        (instrument_id,),
    ).fetchall()
    # Acceptance 8 — the last bar has no t+1, but routine detail is bounded.
    assert retained == [(date(2024, 1, 9), "not_evaluable", "no_fill_bar")]

    # #2333 — the indicator rule set the signals were computed under, stored on
    # every row and equal to the one hashed into `strategy_version`. The
    # round-trip through JSONB is the genuinely-new DB behaviour here: the
    # writer holds a read-only `MappingProxyType`, which psycopg cannot adapt
    # without the explicit `Jsonb` wrapper.
    rule_sets = ebull_test_conn.execute(
        "SELECT DISTINCT input_rule_set_versions FROM strategy_signals WHERE instrument_id = %s",
        (instrument_id,),
    ).fetchall()
    assert rule_sets == [(dict(_IDENTITY.input_rule_set_versions),)]


def test_the_writer_refuses_to_overwrite_a_recorded_signal(
    ebull_test_conn: psycopg.Connection[tuple], instrument_with_a_calendar_gap: int
) -> None:
    """No ON CONFLICT: a re-run must not silently rewrite what was decided.

    A deliberate re-run bumps ``strategy_version``, which is a different key.
    """
    instrument_id = instrument_with_a_calendar_gap
    series = _series_from_db(ebull_test_conn, instrument_id)
    rows = resolve_fills(
        [StrategySignal(verdict="fired", signal_index=0)],
        series=series,
        identity=_IDENTITY,
        instrument_id=instrument_id,
    )
    with ebull_test_conn.transaction():
        assert store_signals(ebull_test_conn, rows) == 1

    with pytest.raises(psycopg.errors.UniqueViolation), ebull_test_conn.transaction():
        store_signals(ebull_test_conn, rows)

    # ...but a genuinely different strategy is a different key and inserts
    # cleanly, so the old decision survives alongside it rather than under it.
    changed = resolve_fills(
        [StrategySignal(verdict="fired", signal_index=0)],
        series=series,
        identity=StrategyIdentity(
            strategy_id=_IDENTITY.strategy_id,
            params={"period": 21},  # criterion 11: different params, different strategy
            universe=_IDENTITY.universe,
            cost_model_id=_IDENTITY.cost_model_id,
            source_hash=_IDENTITY.source_hash,
        ),
        instrument_id=instrument_id,
    )
    assert changed[0].strategy_version != rows[0].strategy_version
    with ebull_test_conn.transaction():
        assert store_signals(ebull_test_conn, changed) == 1

    versions = ebull_test_conn.execute(
        "SELECT count(DISTINCT strategy_version) FROM strategy_signals "
        "WHERE instrument_id = %s AND signal_bar_date = %s",
        (instrument_id, date(2024, 1, 4)),
    ).fetchone()
    assert versions is not None and versions[0] == 2
