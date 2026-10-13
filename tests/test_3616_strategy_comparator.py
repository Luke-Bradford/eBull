"""#3616 slice 1 — the investable UCITS comparator and the active hurdle (pure)."""

from __future__ import annotations

from dataclasses import replace
from datetime import date, timedelta
from decimal import Decimal

import pytest

from app.services.strategy_comparator import (
    COMPARATOR_SYMBOL,
    Close,
    ComparatorInstrument,
    Hurdle,
    PotWindow,
    compare,
    hurdle,
    hurdle_weights,
    pot_window,
)
from app.services.strategy_control_plane import PaperPool, mandate_for_profile
from app.services.strategy_core_mandate import CoreMandate
from app.services.strategy_wealth import StrategyWealthPoint, time_weighted_returns
from app.workers.scheduler import BENCHMARK_SYMBOLS

_D0 = date(2026, 9, 1)
_CSPX = ComparatorInstrument(instrument_id=3434, currency="USD", exchange="7")


def _day(index: int) -> date:
    return _D0 + timedelta(days=index)


def _series(*rows: tuple[str, str | None]) -> list[StrategyWealthPoint]:
    """``(principal, total_pnl)`` per day; ``None`` pnl = incomplete point."""
    points: list[StrategyWealthPoint] = []
    previous = Decimal("0")
    for index, (principal_text, pnl_text) in enumerate(rows):
        principal = Decimal(principal_text)
        pnl = None if pnl_text is None else Decimal(pnl_text)
        points.append(
            StrategyWealthPoint(
                date=_day(index),
                principal=principal,
                external_flow=principal - previous,
                realised_pnl=pnl,
                unrealised_pnl=None if pnl is None else Decimal("0"),
                total_pnl=pnl,
                pot_value=None if pnl is None else principal + pnl,
                complete=pnl is not None,
                incomplete_reasons=() if pnl is not None else ("owned_position_mark_missing",),
            )
        )
        previous = principal
    return points


def _window(*rows: tuple[str, str | None]) -> PotWindow | None:
    history = _series(*rows)
    return pot_window(history, time_weighted_returns(history))


def _core(target: str = "50", *, enabled: bool = True) -> CoreMandate:
    return CoreMandate(
        event_id=35,
        revision=9,
        enabled=enabled,
        base_currency="USD",
        core_instrument_id=1,
        core_target_pct=Decimal(target),
        liquidity_reserve_pct=Decimal("10"),
        rebalance_band_pct=Decimal("5"),
        min_rebalance_amount=Decimal("100"),
        policy_version="test",
    )


def _pool(active: str | None = "30", cash_reserve: str | None = "10", *, profile: str = "growth") -> PaperPool:
    mandate = replace(
        mandate_for_profile(profile),  # type: ignore[arg-type]
        active_risk_budget_pct=None if active is None else Decimal(active),
        cash_reserve_pct=None if cash_reserve is None else Decimal(cash_reserve),
    )
    return PaperPool(event_id=5, enabled=True, capital_limit=Decimal("1000"), mandate=mandate)


_LIVE = hurdle_weights(_core(), _pool())


def _close(day: int, value: str) -> Close:
    return Close(price_date=_day(day), close=Decimal(value))


# --- window ---------------------------------------------------------------


def test_window_from_unfunded_base_starts_at_the_base_not_return_since() -> None:
    history = _series(("0", "0"), ("1000", "0"), ("1000", "10"))
    returns = time_weighted_returns(history)
    window = pot_window(history, returns)
    assert window is not None
    assert returns.return_since == _day(1)
    assert window.start == _day(0)
    assert window.end == _day(2)
    assert window.pot_return == returns.points[-1].cumulative_return


def test_window_from_funded_base() -> None:
    window = _window(("1000", "0"), ("1000", "10"), ("1000", "20"))
    assert window == PotWindow(start=_day(0), end=_day(2), pot_return=Decimal("0.02"))


def test_trailing_incomplete_points_end_the_window_at_the_last_complete_point() -> None:
    window = _window(("1000", "0"), ("1000", "10"), ("1000", None), ("1000", None))
    assert window is not None
    assert window.end == _day(1)


def test_bridged_no_flow_gap_keeps_the_window() -> None:
    window = _window(("1000", "0"), ("1000", None), ("1000", "20"))
    assert window == PotWindow(start=_day(0), end=_day(2), pot_return=Decimal("0.02"))


def test_broken_chain_never_revives_an_earlier_return() -> None:
    # +10% then a flow inside an unvalued span breaks the chain.
    history = _series(("1000", "0"), ("1000", "100"), ("2000", None), ("2000", "100"))
    returns = time_weighted_returns(history)
    assert returns.total_return_available is False
    assert pot_window(history, returns) is None
    result = compare(_CSPX, None, None, None, _LIVE)
    assert result.unavailable_reason == "pot_return_unavailable"
    assert result.comparator_return is None
    assert result.window_start is None and result.window_end is None


def test_no_pot_return_is_pot_return_unavailable() -> None:
    assert _window(("1000", "0")) is None


def test_days_bound_moves_window_with_the_history() -> None:
    full = _series(("1000", "0"), ("1000", "10"), ("1000", "20"), ("1000", "30"))
    trimmed = full[1:]
    long_window = pot_window(full, time_weighted_returns(full))
    short_window = pot_window(trimmed, time_weighted_returns(trimmed))
    assert long_window is not None and short_window is not None
    assert (long_window.start, short_window.start) == (_day(0), _day(1))
    long = compare(_CSPX, long_window, _close(0, "100"), _close(3, "110"), _LIVE)
    short = compare(_CSPX, short_window, _close(1, "105"), _close(3, "110"), _LIVE)
    assert long.comparator_return != short.comparator_return


# --- endpoints ------------------------------------------------------------

_W = PotWindow(start=_day(10), end=_day(30), pot_return=Decimal("0.01"))


def test_worked_example_from_the_spec() -> None:
    # S is the unfunded base 2026-09-17, not return_since 2026-09-18.
    window = PotWindow(start=date(2026, 9, 17), end=date(2026, 10, 9), pot_return=Decimal("0.010937323"))
    result = compare(
        _CSPX,
        window,
        Close(date(2026, 9, 17), Decimal("822.86")),
        Close(date(2026, 10, 9), Decimal("841.59")),
        _LIVE,
    )
    assert result.comparator_return is not None and result.engine_excess is not None
    assert result.comparator_return == Decimal("841.59") / Decimal("822.86") - 1
    assert round(result.comparator_return, 6) == Decimal("0.022762")
    assert round(result.engine_excess, 6) == Decimal("-0.011825")
    assert result.hurdle.multiplier == Decimal("0.65") / Decimal("0.3")  # 13/6
    assert result.hurdle.hurdle_return is not None and result.hurdle.required_active_excess is not None
    assert round(result.hurdle.hurdle_return, 6) == Decimal("0.049318")
    assert round(result.hurdle.required_active_excess, 6) == Decimal("0.026556")
    assert (result.start_close_date, result.end_close_date) == (date(2026, 9, 17), date(2026, 10, 9))
    assert result.unavailable_reason is None and result.hurdle.unavailable_reason is None


def test_missing_instrument_wins_even_when_pot_return_is_unavailable() -> None:
    result = compare(None, None, None, None, _LIVE)
    assert result.unavailable_reason == "comparator_instrument_missing"
    assert result.instrument_id is None


@pytest.mark.parametrize(
    "instrument",
    [
        ComparatorInstrument(3434, "GBP", "7"),
        ComparatorInstrument(3434, "USD", "4"),
        ComparatorInstrument(3434, None, None),
    ],
)
def test_mismatched_instrument_keeps_its_id(instrument: ComparatorInstrument) -> None:
    result = compare(instrument, _W, _close(10, "100"), _close(30, "110"), _LIVE)
    assert result.unavailable_reason == "comparator_instrument_mismatch"
    assert result.instrument_id == 3434
    assert result.comparator_return is None and result.engine_excess is None


@pytest.mark.parametrize(
    ("start", "end"),
    [(None, _close(30, "110")), (_close(10, "100"), None), (None, None)],
)
def test_missing_endpoint_is_close_missing(start: Close | None, end: Close | None) -> None:
    result = compare(_CSPX, _W, start, end, _LIVE)
    assert result.unavailable_reason == "comparator_close_missing"
    assert result.comparator_return is None
    assert result.start_close_date == (None if start is None else start.price_date)
    assert result.end_close_date == (None if end is None else end.price_date)


@pytest.mark.parametrize(
    ("start_day", "end_day", "reason"),
    [
        (6, 30, None),  # start exactly 4 days old
        (5, 30, "comparator_stale"),  # start 5 days old, end fresh
        (10, 26, None),  # end exactly 4 days old
        (10, 25, "comparator_stale"),  # fresh start, end 5 days old
    ],
)
def test_four_day_coverage_boundary_on_each_end(start_day: int, end_day: int, reason: str | None) -> None:
    result = compare(_CSPX, _W, _close(start_day, "100"), _close(end_day, "110"), _LIVE)
    assert result.unavailable_reason == reason
    assert (result.comparator_return is None) == (reason is not None)


def test_uk_holiday_on_a_us_session_resolves_to_the_prior_close() -> None:
    # E is a US session the LSE was shut for; the prior LSE close is one day back.
    result = compare(_CSPX, _W, _close(10, "100"), _close(29, "110"), _LIVE)
    assert result.unavailable_reason is None
    assert result.end_close_date == _day(29)


def test_endpoints_collapsing_to_one_row_are_stale() -> None:
    window = PotWindow(start=_day(10), end=_day(12), pot_return=Decimal("0"))
    row = _close(9, "100")
    result = compare(_CSPX, window, row, row, _LIVE)
    assert result.unavailable_reason == "comparator_stale"


def test_distinct_rows_with_equal_closes_are_a_genuine_zero() -> None:
    result = compare(_CSPX, _W, _close(10, "100"), _close(30, "100"), _LIVE)
    assert result.comparator_return == 0
    assert result.engine_excess == Decimal("0.01")
    assert result.hurdle.hurdle_return == 0


# --- hurdle ---------------------------------------------------------------


def test_live_mandate_core_is_its_target_share_of_the_non_active_pot() -> None:
    # The allocator's sleeve is core + headroom, and headroom is net of the
    # active sleeves, so a 50% target is 35% of the pot at a 30% active cap.
    assert _LIVE.core_target_weight == Decimal("0.5")
    assert (_LIVE.core_weight, _LIVE.active_weight) == (Decimal("0.35"), Decimal("0.3"))
    assert _LIVE.multiplier == Decimal("0.65") / Decimal("0.3")
    assert (_LIVE.core_mandate_event_id, _LIVE.pool_event_id) == (35, 5)
    assert _LIVE.unavailable_reason is None


@pytest.mark.parametrize(
    ("core", "pool"),
    [
        (None, _pool()),
        (_core(enabled=False), _pool()),
        (_core(), PaperPool(None, False, Decimal("0"))),
        (_core(), _pool(profile="unconfigured")),
    ],
)
def test_mandate_unconfigured(core: CoreMandate | None, pool: PaperPool) -> None:
    weights = hurdle_weights(core, pool)
    assert weights.unavailable_reason == "mandate_unconfigured"
    assert weights.multiplier is None
    assert hurdle(weights, Decimal("0.1")).unavailable_reason == "mandate_unconfigured"


def test_weights_populated_whenever_the_rows_exist() -> None:
    weights = hurdle_weights(_core(enabled=False), _pool())
    assert (weights.core_weight, weights.active_weight) == (Decimal("0.35"), Decimal("0.3"))
    assert (weights.core_mandate_event_id, weights.pool_event_id) == (35, 5)


@pytest.mark.parametrize(
    ("core", "active", "cash_reserve"),
    [
        ("90", "50", "10"),  # 50 + 45 = 95 > 90 committable: the cash reserve forbids it
        ("120", "30", "10"),  # core target outside [0, 1]
        ("50", "-5", "10"),  # active outside [0, 1]
        ("50", "0", "60"),  # invalid wins over no_active_budget
    ],
)
def test_mandate_weights_invalid(core: str, active: str, cash_reserve: str) -> None:
    weights = hurdle_weights(_core(core), _pool(active, cash_reserve))
    assert weights.unavailable_reason == "mandate_weights_invalid"
    assert weights.multiplier is None


def test_no_active_budget() -> None:
    weights = hurdle_weights(_core(), _pool("0"))
    assert weights.unavailable_reason == "no_active_budget"
    assert weights.multiplier is None


def test_mandate_change_moves_only_the_hurdle() -> None:
    closes = (_close(10, "100"), _close(30, "110"))
    before = compare(_CSPX, _W, *closes, _LIVE)
    after = compare(_CSPX, _W, *closes, hurdle_weights(_core("40"), _pool("40")))
    assert before.comparator_return == after.comparator_return
    assert before.engine_excess == after.engine_excess
    assert after.hurdle.multiplier == Decimal("0.76") / Decimal("0.4")  # core 0.4 × 0.6 = 0.24
    assert after.hurdle.hurdle_return != before.hurdle.hurdle_return


@pytest.mark.parametrize("value", ["0", "-0.1"])
def test_zero_and_negative_comparator_returns(value: str) -> None:
    result = hurdle(_LIVE, Decimal(value))
    assert _LIVE.multiplier is not None
    assert result.hurdle_return == Decimal(value) * _LIVE.multiplier
    assert result.required_active_excess == Decimal(value) * _LIVE.multiplier - Decimal(value)


def test_infeasible_below_minus_one_keeps_engine_excess() -> None:
    result = compare(_CSPX, _W, _close(10, "100"), _close(30, "50"), _LIVE)  # −50% × 13/6 < −1
    assert result.hurdle.unavailable_reason == "hurdle_infeasible"
    assert result.hurdle.hurdle_return is None and result.hurdle.required_active_excess is None
    assert result.hurdle.multiplier is not None
    assert result.engine_excess == Decimal("0.01") - Decimal("-0.5")


def test_exactly_minus_one_stays_feasible() -> None:
    # Core 40 / active 50: core is 0.2 of the pot, multiplier 0.8 / 0.5 = 1.6.
    result = hurdle(hurdle_weights(_core("40"), _pool("50")), Decimal("-0.625"))
    assert result.unavailable_reason is None
    assert result.hurdle_return == Decimal("-1")


def test_multiplier_present_while_the_comparator_return_is_null() -> None:
    result = compare(_CSPX, None, None, None, _LIVE)
    assert result.hurdle.unavailable_reason == "comparator_return_unavailable"
    assert result.hurdle.multiplier == _LIVE.multiplier
    assert result.hurdle.hurdle_return is None


def test_mandate_reason_precedes_a_missing_comparator_return() -> None:
    result = compare(_CSPX, None, None, None, hurdle_weights(None, _pool()))
    assert result.hurdle.unavailable_reason == "mandate_unconfigured"


def test_null_hurdle_inputs_are_not_coerced() -> None:
    assert hurdle(Hurdle(), Decimal("0.1")) == Hurdle()


# --- binding --------------------------------------------------------------


def test_comparator_symbol_is_kept_fresh() -> None:
    assert COMPARATOR_SYMBOL in BENCHMARK_SYMBOLS
