"""#2840 — the re-observation comparison.

These assert the behaviours the design exists for, and in several cases the exact
defect an earlier draft of it would have shipped:

* a bracket whose left edge is a real committed row, not an overwritten pointer;
* a ``Decimal`` that round-trips to the identical ``double precision`` is NOT a
  divergence (revision 1 compared ``Decimal`` to ``float`` directly, which is exact
  in Python and would have flagged almost every non-integer price);
* a reversion ``A -> B -> C -> B -> A`` records every leg, which a digest-keyed
  design loses;
* a comparison failure never costs us bars that were captured correctly.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock

import psycopg
import pytest

from app.providers.market_data import IntradayBar as ProviderBar
from app.providers.market_data import MarketDataProvider
from app.services import strategy_intraday_harvest
from app.services.strategy_intraday_harvest import run_intraday_harvest

from .test_strategy_intraday_harvest import _activate_test_universe, _bar

_OBSERVED = datetime.fromisoformat("2026-08-07T14:00:00+00:00")
_STAMPS = (
    "2026-08-07T13:30:00+00:00",
    "2026-08-07T13:35:00+00:00",
    "2026-08-07T13:40:00+00:00",
)


def _provider(close: str = "100") -> MagicMock:
    provider = MagicMock(spec=MarketDataProvider)
    provider.get_intraday_candles.return_value = [_bar(stamp, close) for stamp in _STAMPS]
    return provider


def _fire(conn: psycopg.Connection[tuple], provider: MagicMock) -> Any:
    return run_intraday_harvest(conn, provider, observed_at=_OBSERVED, max_requests=1)


def _calls(conn: psycopg.Connection[tuple]) -> list[tuple[Any, ...]]:
    return conn.execute(
        """
        SELECT reobservation_id, outcome, overlap_bars, compared_bars,
               agreed_bars, diverged_bars, instrument_id, failure_class
        FROM strategy_intraday_reobservations
        ORDER BY reobservation_id
        """
    ).fetchall()


def test_first_fire_has_nothing_to_re_observe_then_seeds_from_the_stored_bar(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _activate_test_universe(ebull_test_conn, instrument_id=2_840_001)
    provider = _provider()

    first = _fire(ebull_test_conn, provider)
    second = _fire(ebull_test_conn, provider)

    # Nothing is held before the first fire, so there is nothing to re-observe —
    # and that is a recorded outcome, not a silence.
    assert first.written == 3
    assert first.compared == 0
    assert second.written == 0
    assert second.compared == 3
    assert second.diverged == 0

    outcomes = [row[1] for row in _calls(ebull_test_conn)]
    assert outcomes == ["no_overlap", "compared"]

    sources = ebull_test_conn.execute(
        "SELECT DISTINCT baseline_source, price_changed FROM strategy_intraday_reobserved_bars"
    ).fetchall()
    assert sources == [("stored_bar", False)]


def test_the_third_fire_brackets_against_the_second_not_the_stored_bar(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """The left edge must be a prior OBSERVATION, whose requested_at is a real lower bound.

    A ``stored_bar`` baseline carries only ``captured_at``, which is an UPPER bound on
    when it was observed — so it can evidence a rewrite but cannot bracket one.
    """
    _activate_test_universe(ebull_test_conn, instrument_id=2_840_002)
    provider = _provider()

    _fire(ebull_test_conn, provider)
    _fire(ebull_test_conn, provider)
    _fire(ebull_test_conn, provider)

    ids = [row[0] for row in _calls(ebull_test_conn)]
    rows = ebull_test_conn.execute(
        """
        SELECT baseline_source, baseline_reobservation_id, baseline_captured_at
        FROM strategy_intraday_reobserved_bars
        WHERE reobservation_id = %s
        ORDER BY bar_time
        """,
        (ids[2],),
    ).fetchall()

    assert [row[0] for row in rows] == ["prior_reobservation"] * 3
    assert {row[1] for row in rows} == {ids[1]}
    assert all(row[2] is None for row in rows)


def test_a_perturbed_close_diverges_and_names_the_preceding_call(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _activate_test_universe(ebull_test_conn, instrument_id=2_840_003)

    _fire(ebull_test_conn, _provider("100"))
    _fire(ebull_test_conn, _provider("100"))
    third = _fire(ebull_test_conn, _provider("101"))

    assert third.diverged == 3
    ids = [row[0] for row in _calls(ebull_test_conn)]
    diverged = ebull_test_conn.execute(
        """
        SELECT baseline_close, observed_close, baseline_reobservation_id, volume_changed
        FROM strategy_intraday_reobserved_bars
        WHERE price_changed
        ORDER BY bar_time
        """
    ).fetchall()

    assert [(row[0], row[1]) for row in diverged] == [(100.0, 101.0)] * 3
    assert {row[2] for row in diverged} == {ids[1]}
    assert {row[3] for row in diverged} == {False}

    # The bar itself is never rewritten: its immutability is load-bearing for
    # captured_at semantics, and the evidence lives beside it.
    assert ebull_test_conn.execute(
        "SELECT DISTINCT close FROM strategy_intraday_bars WHERE instrument_id = 2840003"
    ).fetchall() == [(100.0,)]


def test_a_reversion_records_every_leg(ebull_test_conn: psycopg.Connection[tuple]) -> None:
    """A -> B -> C -> B -> A. A digest-keyed design loses the return to A."""
    _activate_test_universe(ebull_test_conn, instrument_id=2_840_004)

    for close in ("100", "100", "101", "102", "101", "100"):
        _fire(ebull_test_conn, _provider(close))

    legs = ebull_test_conn.execute(
        """
        SELECT baseline_close, observed_close
        FROM strategy_intraday_reobserved_bars
        WHERE price_changed AND bar_time = %s
        ORDER BY reobservation_id
        """,
        (datetime.fromisoformat(_STAMPS[0]),),
    ).fetchall()

    assert legs == [(100.0, 101.0), (101.0, 102.0), (102.0, 101.0), (101.0, 100.0)]


@pytest.mark.parametrize("redelivered", ["101.23", "101.230", "101.2300000"])
def test_a_decimal_that_round_trips_identically_is_not_a_divergence(
    ebull_test_conn: psycopg.Connection[tuple], redelivered: str
) -> None:
    """The defect revision 1 would have shipped.

    ``Decimal("101.23") == 101.23`` is False in Python — the float is not exactly
    101.23 — so comparing the delivered Decimal against the stored double directly
    would have reported a divergence on essentially every non-integer price.
    Equality is decided at storage precision, by PostgreSQL.
    """
    _activate_test_universe(ebull_test_conn, instrument_id=2_840_005)

    _fire(ebull_test_conn, _provider("101.23"))
    second = _fire(ebull_test_conn, _provider(redelivered))

    assert Decimal("101.23") != 101.23  # the premise, asserted rather than assumed
    assert second.compared == 3
    assert second.diverged == 0


def test_every_failing_path_still_records_a_denominator_row(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _activate_test_universe(ebull_test_conn, instrument_id=2_840_006, include_missing=True)
    failing = MagicMock(spec=MarketDataProvider)
    failing.get_intraday_candles.side_effect = RuntimeError("provider down")

    report = run_intraday_harvest(ebull_test_conn, failing, observed_at=_OBSERVED, max_requests=2)

    assert report.written == 0
    rows = {(row[1], row[6] is None, row[7]) for row in _calls(ebull_test_conn)}
    assert rows == {
        ("fetch_failed", False, "RuntimeError"),
        ("unresolved_member", True, None),
    }


def test_a_comparison_failure_never_costs_us_captured_bars(
    ebull_test_conn: psycopg.Connection[tuple], monkeypatch: pytest.MonkeyPatch
) -> None:
    """The one way this ticket could have damaged something that already works.

    Revision 1 ran the comparison before the write inside the same ``try``, where a
    defect in the new tables would have skipped bars that were collected correctly.
    """
    _activate_test_universe(ebull_test_conn, instrument_id=2_840_007)

    def _explode(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("comparison is broken")

    monkeypatch.setattr(strategy_intraday_harvest, "record_comparison", _explode)
    report = _fire(ebull_test_conn, _provider())

    assert report.written == 3
    assert [failure.reason for failure in report.failures] == ["reobservation: RuntimeError"]
    assert [(row[1], row[7]) for row in _calls(ebull_test_conn)] == [("comparison_skipped", "RuntimeError")]


def test_counters_reconcile_against_the_bar_rows_they_claim(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """``overlap_bars`` is a denominator only if every bar lands in exactly one bucket."""
    _activate_test_universe(ebull_test_conn, instrument_id=2_840_008)

    _fire(ebull_test_conn, _provider("100"))
    _fire(ebull_test_conn, _provider("101"))

    assert ebull_test_conn.execute(
        """
        SELECT count(*)
        FROM strategy_intraday_reobservations AS r
        WHERE r.compared_bars <> (SELECT count(*)
                                    FROM strategy_intraday_reobserved_bars AS b
                                   WHERE b.reobservation_id = r.reobservation_id)
           OR r.diverged_bars <> (SELECT count(*)
                                    FROM strategy_intraday_reobserved_bars AS b
                                   WHERE b.reobservation_id = r.reobservation_id
                                     AND b.price_changed)
        """
    ).fetchone() == (0,)


def _malformed(stamp: str) -> ProviderBar:
    """A candle the eToro normalizer accepts but our tables refuse: high < close."""
    return ProviderBar(
        timestamp=datetime.fromisoformat(stamp),
        open=Decimal("100"),
        high=Decimal("100"),
        low=Decimal("99"),
        close=Decimal("101"),
        volume=100,
    )


def test_one_malformed_candle_costs_one_bar_not_the_whole_call(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """The batch insert is one statement in one transaction.

    A candle that passes the Python guard and then fails a table CHECK would roll back
    every valid comparison in the call and report ``comparison_skipped`` — turning one
    bad candle into a lost call, which is what ``invalid_baseline_bars`` exists to stop.
    """
    _activate_test_universe(ebull_test_conn, instrument_id=2_840_009)
    _fire(ebull_test_conn, _provider("100"))

    mixed = MagicMock(spec=MarketDataProvider)
    mixed.get_intraday_candles.return_value = [
        _bar(_STAMPS[0], "100"),
        _malformed(_STAMPS[1]),
        _bar(_STAMPS[2], "100"),
    ]
    second = run_intraday_harvest(ebull_test_conn, mixed, observed_at=_OBSERVED, max_requests=1)

    assert second.failures == ()
    assert second.compared == 2
    assert _calls(ebull_test_conn)[1][1:6] == ("compared", 3, 2, 2, 0)
    assert ebull_test_conn.execute(
        "SELECT invalid_baseline_bars FROM strategy_intraday_reobservations ORDER BY reobservation_id"
    ).fetchall() == [(0,), (1,)]


def test_an_out_of_range_value_does_not_poison_the_transaction(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Catching the cast error is not enough — PostgreSQL leaves the transaction ABORTED.

    Without a savepoint every later statement in the call, including the outcome insert
    and the cursor advance, fails with ``InFailedSqlTransaction``.
    """
    _activate_test_universe(ebull_test_conn, instrument_id=2_840_010)
    _fire(ebull_test_conn, _provider("100"))

    overflowing = MagicMock(spec=MarketDataProvider)
    overflowing.get_intraday_candles.return_value = [
        ProviderBar(
            timestamp=datetime.fromisoformat(_STAMPS[0]),
            open=Decimal("1e1000"),
            high=Decimal("1e1000"),
            low=Decimal("1e1000"),
            close=Decimal("1e1000"),
            volume=100,
        ),
        _bar(_STAMPS[1], "100"),
        _bar(_STAMPS[2], "100"),
    ]
    second = run_intraday_harvest(ebull_test_conn, overflowing, observed_at=_OBSERVED, max_requests=1)

    assert second.failures == ()
    assert second.compared == 2
    assert _calls(ebull_test_conn)[1][1:6] == ("compared", 3, 2, 2, 0)
