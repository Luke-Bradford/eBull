from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest

from app.services.etf_intraday_momentum_evaluation import (
    evaluate_candidate,
    evaluate_symbol,
    load_retained_bars,
)
from app.services.strategy_observation_storage import IntradayBar

_NY = ZoneInfo("America/New_York")


def _bar(
    day: date,
    hour: int,
    minute: int,
    *,
    open_: str,
    close: str,
    instrument_id: int = 11,
    source: str = "etoro/ETORO-RTH-V1/nyse_rth",
) -> IntradayBar:
    opening = Decimal(open_)
    closing = Decimal(close)
    return IntradayBar(
        timeframe="30m",
        bar_time=datetime(day.year, day.month, day.day, hour, minute, tzinfo=_NY).astimezone(UTC),
        instrument_id=instrument_id,
        open=opening,
        high=max(opening, closing),
        low=min(opening, closing),
        close=closing,
        volume=Decimal("1000"),
        source=source,
    )


def _two_outcome_sessions() -> list[IntradayBar]:
    monday = date(2026, 7, 13)
    tuesday = date(2026, 7, 14)
    wednesday = date(2026, 7, 15)
    return [
        _bar(monday, 15, 30, open_="99", close="100"),
        _bar(tuesday, 9, 30, open_="100", close="102"),
        _bar(tuesday, 15, 30, open_="100", close="101"),
        _bar(wednesday, 9, 30, open_="101", close="100"),
        _bar(wednesday, 15, 30, open_="100", close="98"),
    ]


def test_separates_fired_long_from_always_long_and_published_signed() -> None:
    result = evaluate_symbol("SPY", _two_outcome_sessions())

    assert result.complete_outcomes == 2
    assert result.fired_long == 1
    assert result.fired_cadence_pct == 50.0
    assert result.long_only.observations == 1
    assert result.long_only.expectancy_pct == pytest.approx(1.0)
    assert result.long_only.hit_rate_pct == 100.0
    assert result.always_long.observations == 2
    assert result.always_long.expectancy_pct == pytest.approx(-0.5)
    assert result.published_signed.expectancy_pct == pytest.approx(1.5)
    assert result.published_signed.hit_rate_pct == 100.0


def test_missing_required_interval_is_a_refusal_not_a_zero_return() -> None:
    bars = _two_outcome_sessions()
    bars = [
        bar
        for bar in bars
        if bar.bar_time.astimezone(_NY).date() != date(2026, 7, 15) or bar.bar_time.astimezone(_NY).hour != 15
    ]

    result = evaluate_symbol("SPY", bars)

    assert result.complete_outcomes == 1
    assert dict(result.refusals)["missing_last_half_hour_bar"] == 1
    assert result.long_only.observations == 1


def test_non_frozen_source_is_not_silently_used() -> None:
    bars = _two_outcome_sessions()
    tuesday = date(2026, 7, 14)
    bars.append(_bar(tuesday, 9, 30, open_="100", close="150", source="other"))

    result = evaluate_symbol("SPY", bars)

    assert result.complete_outcomes == 2
    assert result.long_only.expectancy_pct == pytest.approx(1.0)


def test_later_unregistered_bar_cannot_change_the_outcome() -> None:
    bars = _two_outcome_sessions()
    baseline = evaluate_symbol("SPY", bars)
    bars.append(_bar(date(2026, 7, 14), 14, 0, open_="1", close="10000"))

    observed = evaluate_symbol("SPY", bars)

    assert observed.long_only == baseline.long_only
    assert observed.always_long == baseline.always_long
    assert observed.published_signed == baseline.published_signed


def test_candidate_remains_immature_and_unpromotable() -> None:
    result = evaluate_candidate("universe-v1", {"SPY": _two_outcome_sessions()})

    assert "sample_immature" in result.promotion_refusals
    assert "historical_entry_exit_quotes_unavailable" in result.promotion_refusals
    assert "published_short_leg_not_executable" in result.promotion_refusals
    assert "prospective_outcome_interval_missing" in result.promotion_refusals
    assert result.symbols[1].complete_outcomes == 0


class _Rows:
    def __init__(self, rows: list[tuple[object, ...]]) -> None:
        self._rows = rows

    def fetchall(self) -> list[tuple[object, ...]]:
        return self._rows


class _Connection:
    def __init__(self, bar_row: tuple[object, ...]) -> None:
        self.bar_row = bar_row
        self.calls: list[tuple[str, object]] = []

    def execute(self, query: str, params: object = None) -> _Rows:
        self.calls.append((query, params))
        if "FROM strategy_intraday_universe_versions" in query:
            return _Rows([("active-v1",)])
        return _Rows([self.bar_row])


def test_loader_uses_exact_resolved_active_universe_member() -> None:
    bar = _bar(date(2026, 7, 13), 15, 30, open_="99", close="100")
    # PostgreSQL's retained OHLC columns decode as floats in production. The
    # candidate's Decimal arithmetic must not depend on Decimal-only fixtures.
    row = ("SPY", bar.bar_time, 11, 99.0, 100.0, 99.0, 100.0, 1000.0, bar.source)
    conn = _Connection(row)

    version, grouped = load_retained_bars(conn)  # type: ignore[arg-type]

    assert version == "active-v1"
    assert grouped["SPY"] == (bar,)
    sql, params = conn.calls[1]
    assert "cardinality(resolved.instrument_ids) = 1" in sql
    assert "bar.source LIKE 'etoro/%%/nyse_rth'" in sql
    assert params == {"version": "active-v1", "symbols": ["SPY", "QQQ", "IWM"]}
