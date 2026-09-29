"""#3471 spec v6 §16.3 — plan derivation, orders 8–12, and the §16.5 library plan rule."""

from __future__ import annotations

import math
import re
from decimal import Decimal
from fractions import Fraction
from pathlib import Path

import pytest

from app.services.ai_trial_decision import AtrMeasurement, measure_atr
from app.services.ai_trial_levels import LEVEL_IDS, Level
from app.services.ai_trial_plan import HORIZON_STOP_FLOOR_ATR, derive_plan, library_plan, plan_figures, qs

EXIT_BASE_RATES_OUT = Path(__file__).resolve().parents[1] / "scripts" / "ai_trial_exit_base_rates.out"


def _atr(atr14: str = "2", close: str = "100") -> AtrMeasurement:
    measurement = measure_atr(Decimal(atr14), Decimal(close))
    assert measurement is not None
    return measurement


def _lvl(price: str) -> Level:
    return Level(Fraction(Decimal(price)), None)


def test_qs_keeps_the_sign_and_never_emits_negative_zero() -> None:
    assert qs(Fraction(12345, 10**4)) == Decimal("1.2345")
    assert qs(Fraction(-123455, 10**5)) == Decimal("-1.2346")  # half away from zero
    assert str(qs(Fraction(-1, 10**6))) == "0.0000"


def test_figures_follow_the_derivation_exactly() -> None:
    figures = plan_figures(_atr(), _lvl("96"), _lvl("110"))
    assert figures.stop_price == Fraction(191, 2)  # 96 − ¼ × 2
    assert figures.stop_atr_multiple == Decimal("2.2500")
    assert figures.stop_pct == Decimal("4.5000")
    assert figures.target_pct == Decimal("10.0000")
    assert figures.r_multiple == Decimal("2.2222")


def test_figures_are_recorded_even_when_refused_and_can_be_negative() -> None:
    figures = plan_figures(_atr(), _lvl("104"), _lvl("99"))
    assert figures.stop_pct == Decimal("-3.5000") and figures.target_pct == Decimal("-1.0000")
    assert figures.r_multiple is None  # risk ≤ 0: undefined
    assert plan_figures(None, _lvl("96"), _lvl("110")).stop_price is None


@pytest.mark.parametrize(
    ("atr", "invalidation", "target", "horizon", "refusal"),
    [
        (_atr(), _lvl("96"), _lvl("110"), 10, None),
        (None, _lvl("96"), _lvl("110"), 10, "level_unavailable"),
        (_atr(), None, _lvl("110"), 10, "level_unavailable"),
        (_atr(), _lvl("96"), None, 10, "level_unavailable"),
        (_atr(), _lvl("100"), _lvl("110"), 10, "level_unavailable"),  # invalidation ≥ close
        (_atr(), _lvl("96"), _lvl("100"), 10, "level_unavailable"),  # target ≤ close
        (_atr(), _lvl("0.5"), _lvl("110"), 10, "level_unavailable"),  # stop price ≤ 0
        (_atr(), _lvl("98.5"), _lvl("110"), 10, "stop_below_horizon_floor"),  # 1.0 ATR < 1.5
        (_atr(), _lvl("98.5"), _lvl("110"), 5, None),  # 1.0 ATR meets the 5d floor
        (_atr(), _lvl("91"), _lvl("150"), 10, "stop_outside_atr_band"),  # 4.75 ATR
        (_atr(), _lvl("96"), _lvl("108"), 10, "reward_risk_below_min"),  # R 1.78
        (_atr("1"), _lvl("98.75"), _lvl("110"), 10, "plan_outside_bounds"),  # stop 1.5%
        (_atr(), _lvl("96"), _lvl("250"), 10, "plan_outside_bounds"),  # target 150%
    ],
)
def test_orders_8_to_12(
    atr: AtrMeasurement | None, invalidation: Level | None, target: Level | None, horizon: int, refusal: str | None
) -> None:
    assert derive_plan(atr, invalidation, target, horizon_days=horizon).refusal == refusal


def test_a_hand_built_invalid_measurement_refuses_instead_of_dividing_by_zero() -> None:
    zero = AtrMeasurement(Decimal(0), Decimal(100), Decimal(0))
    assert derive_plan(zero, _lvl("96"), _lvl("110"), horizon_days=10).refusal == "level_unavailable"
    assert plan_figures(zero, _lvl("96"), _lvl("110")).stop_atr_multiple is None


def test_reward_risk_also_binds_the_quantized_percentages_execution_uses() -> None:
    # risk 2.99995 → stop_pct q=3.0000; reward 5.9999 → R on prices is exactly 2, but the
    # executed ratio 5.9999 / 3.0000 is below 2 (r2-14).
    verdict = derive_plan(_atr(), _lvl("97.50005"), _lvl("105.9999"), horizon_days=5)
    assert verdict.figures.r_multiple == Decimal("2.0000")
    assert verdict.refusal == "reward_risk_below_min"


def _levels(**prices: str) -> dict[str, Level | None]:
    out: dict[str, Level | None] = dict.fromkeys(LEVEL_IDS)
    out.update({k: _lvl(v) for k, v in prices.items()})
    return out


def test_library_plan_takes_the_highest_feasible_support_and_the_lowest_feasible_target() -> None:
    levels = _levels(
        swing_low_1="99",  # below close but inside the 1.5 ATR floor
        sma20="96",
        donchian20_low="96",  # price tie with sma20: enum order picks donchian20_low
        sma200="92",
        swing_high_1="105",  # R 1.1
        donchian20_high="110",
        mm_up="110",  # price tie: enum order picks donchian20_high
    )
    plan = library_plan(levels, _atr(), horizon_days=10)
    assert plan is not None
    assert (plan.invalidation_level_id, plan.target_level_id) == ("donchian20_low", "donchian20_high")


def test_library_plan_is_none_without_a_feasible_pair() -> None:
    assert library_plan(_levels(sma20="96", swing_high_1="105"), _atr(), horizon_days=10) is None
    assert library_plan(_levels(sma20="96", donchian20_high="110"), None, horizon_days=10) is None


def test_floors_are_the_checked_in_mae_p50s_rounded_up_to_half_an_atr() -> None:
    """r2-34: the frozen §16.1 floors ARE the exit_map output, not a transcription of it."""
    measured = {
        int(h): Fraction(p50)
        for h, p50 in re.findall(r"^horizon (\d+)d .*MAE\(ATR\) p50=([\d.]+)", EXIT_BASE_RATES_OUT.read_text(), re.M)
    }
    assert set(measured) == set(HORIZON_STOP_FLOOR_ATR)
    for horizon, p50 in measured.items():
        assert Fraction(math.ceil(p50 * 2), 2) == HORIZON_STOP_FLOOR_ATR[horizon]
