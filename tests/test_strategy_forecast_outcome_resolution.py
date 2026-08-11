"""Forecast-owned target/stop/horizon outcomes are causal, compact and retryable."""

from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal
from typing import Any, cast

import psycopg

from app.services.indicator_series import BarSeries
from app.services.strategy_forecast_outcome_resolution import (
    ForecastOutcomeRow,
    PendingForecast,
    _read_cursor,
    _resolve_forecast,
    _select,
    _select_round_robin,
    _store,
    _write_cursor,
)


def _series(*ranges: tuple[Decimal | None, Decimal | None, Decimal | None]) -> BarSeries:
    dates = tuple(date(2026, 8, 1) + timedelta(days=index) for index in range(len(ranges)))
    rows = tuple(
        {
            "open": bar_open,
            "high": high,
            "low": low,
            "close": bar_open,
            "volume": Decimal("1000"),
        }
        for bar_open, high, low in ranges
    )
    return BarSeries(dates=dates, rows=rows)  # type: ignore[arg-type]


def _forecast(series: BarSeries, *, horizon: int = 2) -> PendingForecast:
    return PendingForecast(
        forecast_id=7,
        instrument_id=42,
        fill_bar_date=series.dates[0],
        fill_price=Decimal("100"),
        target_barrier_pct=Decimal("10"),
        stop_barrier_pct=Decimal("5"),
        horizon_market_days=horizon,
    )


def test_forecast_target_uses_its_exact_barrier() -> None:
    series = _series(
        (Decimal("100"), Decimal("111"), Decimal("99")),
        (Decimal("100"), Decimal("101"), Decimal("99")),
    )
    row = _resolve_forecast(_forecast(series), series=series, unresolved_breaks=())
    assert row is not None
    assert (row.outcome, row.exit_price, row.gross_return_pct, row.market_bars_held) == (
        "target_first",
        Decimal("110"),
        Decimal("0.1"),
        0,
    )


def test_forecast_stop_uses_its_exact_barrier() -> None:
    series = _series(
        (Decimal("100"), Decimal("101"), Decimal("94")),
        (Decimal("100"), Decimal("101"), Decimal("99")),
    )
    row = _resolve_forecast(_forecast(series), series=series, unresolved_breaks=())
    assert row is not None
    assert (row.outcome, row.exit_price, row.gross_return_pct) == (
        "stop_first",
        Decimal("95"),
        Decimal("-0.05"),
    )


def test_same_bar_target_and_stop_is_not_scored_in_either_direction() -> None:
    series = _series(
        (Decimal("100"), Decimal("111"), Decimal("94")),
        (Decimal("100"), Decimal("101"), Decimal("99")),
    )
    row = _resolve_forecast(_forecast(series), series=series, unresolved_breaks=())
    assert row is not None
    assert (row.outcome, row.exit_bar_date, row.exit_price, row.gross_return_pct) == (
        "ambiguous",
        series.dates[0],
        None,
        None,
    )
    assert row.reason is None


def test_timeout_exits_at_the_next_open_after_the_horizon() -> None:
    series = _series(
        (Decimal("100"), Decimal("101"), Decimal("99")),
        (Decimal("100"), Decimal("102"), Decimal("98")),
        (Decimal("103"), Decimal("104"), Decimal("102")),
    )
    row = _resolve_forecast(_forecast(series), series=series, unresolved_breaks=())
    assert row is not None
    assert (row.outcome, row.exit_bar_date, row.exit_price, row.gross_return_pct) == (
        "timeout",
        series.dates[2],
        Decimal("103"),
        Decimal("0.03"),
    )


def test_immature_horizon_writes_no_terminal_row() -> None:
    series = _series(
        (Decimal("100"), Decimal("101"), Decimal("99")),
        (Decimal("100"), Decimal("102"), Decimal("98")),
    )
    assert _resolve_forecast(_forecast(series), series=series, unresolved_breaks=()) is None


def test_quarantined_required_range_is_counted_as_unresolved() -> None:
    series = _series(
        (Decimal("100"), None, Decimal("99")),
        (Decimal("100"), Decimal("102"), Decimal("98")),
    )
    row = _resolve_forecast(_forecast(series), series=series, unresolved_breaks=())
    assert row is not None
    assert (row.outcome, row.reason, row.gross_return_pct) == ("unresolved", "quarantined_bar", None)


def test_scale_break_is_terminal_and_never_crosses_price_units() -> None:
    series = _series(
        (Decimal("100"), Decimal("101"), Decimal("99")),
        (Decimal("100"), Decimal("102"), Decimal("98")),
        (Decimal("10"), Decimal("12"), Decimal("8")),
    )
    row = _resolve_forecast(
        _forecast(series),
        series=series,
        unresolved_breaks=(series.dates[2],),
    )
    assert row is not None
    assert (row.outcome, row.reason, row.gross_return_pct) == ("unresolved", "series_break", None)


def test_round_robin_wraps_without_repeating(monkeypatch: object) -> None:
    from pytest import MonkeyPatch

    patcher = cast(MonkeyPatch, monkeypatch)
    seen: list[tuple[int, int | None, int]] = []

    def select(_conn: object, **kwargs: object) -> list[PendingForecast]:
        after = cast(int, kwargs["after_forecast_id"])
        ceiling = cast(int | None, kwargs.get("at_or_before_forecast_id"))
        limit = cast(int, kwargs["limit"])
        seen.append((after, ceiling, limit))
        ids = [8] if after == 7 else [2, 3]
        return [
            PendingForecast(
                forecast_id=forecast_id,
                instrument_id=42,
                fill_bar_date=date(2026, 8, 1),
                fill_price=Decimal("100"),
                target_barrier_pct=Decimal("10"),
                stop_barrier_pct=Decimal("5"),
                horizon_market_days=2,
            )
            for forecast_id in ids[:limit]
        ]

    patcher.setattr("app.services.strategy_forecast_outcome_resolution._select", select)
    rows = _select_round_robin(cast(psycopg.Connection[object], object()), cursor=7, limit=3)
    assert [row.forecast_id for row in rows] == [8, 2, 3]
    assert seen == [(7, None, 3), (0, 7, 2)]


def test_cursor_is_one_bounded_mutable_row(ebull_test_conn: psycopg.Connection[object]) -> None:
    assert _read_cursor(ebull_test_conn) == 0
    _write_cursor(ebull_test_conn, 10)
    _write_cursor(ebull_test_conn, 3)
    assert _read_cursor(ebull_test_conn) == 3
    assert ebull_test_conn.execute("SELECT count(*) FROM strategy_forecast_outcome_cursor").fetchone() == (1,)


def test_terminal_forecast_is_selected_and_stored_once(ebull_test_conn: psycopg.Connection[Any]) -> None:
    conn = ebull_test_conn
    conn.execute("INSERT INTO exchanges (exchange_id,country,asset_class) VALUES ('2553','US','us_equity')")
    conn.execute(
        "INSERT INTO instruments (instrument_id,symbol,company_name,exchange,currency,is_tradable) "
        "VALUES (2553001,'FOUT','Forecast outcome test','2553','USD',true)"
    )
    signal_id = conn.execute(
        """
        INSERT INTO strategy_signals (
            strategy_id,strategy_version,instrument_id,signal_bar_date,signal_kind,
            verdict,fill_bar_date,fill_price,universe,input_rule_set_versions
        ) VALUES (
            'S-FOUT','v1',2553001,'2026-08-01','entry','fired','2026-08-02',100,
            'survivor_only','{"indicator_series":"rules-v1"}'::jsonb
        ) RETURNING signal_id
        """
    ).fetchone()
    assert signal_id is not None
    conn.execute(
        """
        INSERT INTO strategy_forecast_calibrations (
            calibration_id,model_version,holdout_start,holdout_end,sample_size,
            brier_score,calibration_error,passed,evidence_ref
        ) VALUES ('cal-2553','model-v1','2026-01-01','2026-07-31',100,0.2,0.05,true,'test')
        """
    )
    forecast_id = conn.execute(
        """
        INSERT INTO strategy_opportunity_forecasts (
            signal_id,forecast_policy_version,decided_at,valid_through,side,
            horizon_market_days,target_barrier_pct,stop_barrier_pct,setup_version,
            exit_policy_version,calibration_id,target_probability,stop_probability,
            timeout_probability,target_net_return_pct,stop_net_return_pct,
            timeout_net_return_pct,expected_duration_hours,uncertainty_penalty_pct,
            tail_penalty_pct,correlation_penalty_pct,cost_stress_penalty_pct,
            conservative_net_expectancy_pct,cost_model_id
        ) VALUES (
            %s,'policy-v1','2026-08-01 20:00Z','2026-08-03 20:00Z','long',2,10,5,
            'setup-v1','exit-v1','cal-2553',0.5,0.25,0.25,10,-5,0,24,0,0,0,0,3.75,'cost-v1'
        ) RETURNING forecast_id
        """,
        (int(signal_id[0]),),
    ).fetchone()
    assert forecast_id is not None
    selected = _select(conn, after_forecast_id=0, limit=10)
    assert [row.forecast_id for row in selected] == [int(forecast_id[0])]

    outcomes = (
        ForecastOutcomeRow(
            int(forecast_id[0]), "target_first", None, date(2026, 8, 2), Decimal("110"), 0, Decimal("0.1")
        ),
        ForecastOutcomeRow(
            int(forecast_id[0]), "stop_first", None, date(2026, 8, 2), Decimal("95"), 0, Decimal("-0.05")
        ),
        ForecastOutcomeRow(int(forecast_id[0]), "timeout", None, date(2026, 8, 4), Decimal("103"), 2, Decimal("0.03")),
        ForecastOutcomeRow(int(forecast_id[0]), "ambiguous", None, date(2026, 8, 2), None, 0, None),
        ForecastOutcomeRow(int(forecast_id[0]), "unresolved", "series_break", None, None, None, None),
    )
    for terminal in outcomes:
        assert _store(conn, [terminal]) == 1
        assert _store(conn, [terminal]) == 0
        conn.execute("DELETE FROM strategy_opportunity_forecast_outcomes WHERE forecast_id=%s", (forecast_id[0],))
    assert _store(conn, [outcomes[0]]) == 1
    assert _select(conn, after_forecast_id=0, limit=10) == []
    assert conn.execute("SELECT count(*) FROM strategy_opportunity_forecast_outcomes").fetchone() == (1,)
