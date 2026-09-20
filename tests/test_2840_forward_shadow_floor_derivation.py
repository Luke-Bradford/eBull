"""#2840 arm 2 — the forward-shadow floor is DERIVED, and the arithmetic is pinned here.

``docs/review-prevention-log.md``'s #2614 entry: *"derive by construction, keep the
arithmetic in a pure test that recomputes it from the named inputs, and put the
reproducing command next to the frozen literals so a stale rate fails loudly instead of
shipping."* This is that test.

Pure logic — no database, no corpus, no fixture file. ``derive`` takes a census mapping
and one stored row, so every case below is a hand-built input whose expected output is
recomputed from the formula rather than pasted from a run.
"""

from __future__ import annotations

import math
from decimal import Decimal
from statistics import NormalDist
from typing import Any

import pytest

from app.services.cost_model import BANDS
from app.services.strategies.s12_cheapest_band_price_gated_breakout import S12_STRATEGY_ID
from scripts.derive_2840_arm2_forward_shadow_floor import (
    ALPHA_TWO_SIDED,
    JOINT_POWER,
    POWERED_LEGS,
    S4_STRATEGY_ID,
    _stale_cost_inflation,
    derivation_string,
    derive,
)


def _mix(
    *,
    charge_pct: float,
    fill_dates: int,
    fires: int = 1_000,
) -> dict[str, Any]:
    return {
        "fires": fires,
        "charged_legs": fires,
        "unusable_fill_price": 0,
        "no_fill_bar": 0,
        "distinct_fill_dates": fill_dates,
        "concentration": {"max_fires_on_one_date": 1, "median_fires_per_date": 1, "dates_with_one_fire": fill_dates},
        "by_band": {">=$100": fires},
        "mean_round_trip_charge_pct": charge_pct,
        "cheapest_band_share": 1.0,
    }


def _census(
    *,
    s4_charge: dict[str, float] | None = None,
    s12_charge: dict[str, float] | None = None,
    s12_fill_dates: dict[str, int] | None = None,
    sd_all: float = 0.08,
    sd_gated: float = 0.04,
    s12_fired: int = 44_842,
    s12_dates: int = 4_916,
    s4_fired: int = 1_043_493,
    s4_dates: int = 11_616,
    span_days: int = 21_000,
    cost_price_basis: str = "as_traded",
    limited_to_series: int | None = None,
) -> dict[str, Any]:
    """One census report, shaped exactly as the census script prints it."""
    s4_charge = s4_charge or {"all_fires": 0.70, "max_hold_collapse": 0.74}
    s12_charge = s12_charge or {"all_fires": 0.33, "max_hold_collapse": 0.33}
    s12_fill_dates = s12_fill_dates or {"all_fires": 4_900, "max_hold_collapse": 2_000}
    mix: dict[str, Any] = {}
    for weighting in ("all_fires", "max_hold_collapse"):
        mix[f"{S4_STRATEGY_ID}/masked/{weighting}"] = _mix(charge_pct=s4_charge[weighting], fill_dates=10_000)
        mix[f"{S12_STRATEGY_ID}/masked/{weighting}"] = _mix(
            charge_pct=s12_charge[weighting], fill_dates=s12_fill_dates[weighting]
        )
    return {
        "cost_price_basis": cost_price_basis,
        "limited_to_series": limited_to_series,
        "charged_band_mix": mix,
        "bar_return_dispersion": {
            "masked": {
                "all_bars": {"count": 1_000, "mean": 0.0, "sd": sd_all},
                "gated_bars": {"count": 100, "mean": 0.0, "sd": sd_gated},
                "sd_ratio_gated_over_all": sd_gated / sd_all,
            }
        },
        "supply": {
            f"{S12_STRATEGY_ID}/masked": {
                "fired": s12_fired,
                "distinct_signal_dates": s12_dates,
                "span_days": span_days,
            },
            f"{S4_STRATEGY_ID}/masked": {
                "fired": s4_fired,
                "distinct_signal_dates": s4_dates,
                "span_days": span_days,
            },
        },
    }


def _borrowed(*, ci_low: str = "-0.8162", ci_high: str = "-0.0611", clusters: int = 10_691) -> dict[str, Any]:
    return {
        "trade_count": 372_385,
        "expectancy_ci_low_pct": Decimal(ci_low),
        "expectancy_ci_high_pct": Decimal(ci_high),
        "bootstrap_cluster_count": clusters,
        "bootstrap_block_length": 118,
        "bootstrap_design_effect": Decimal("63.02"),
        "cost_model_id": "static-p75-insession-v3+split-adjusted-max+carry-fx-structural-zero-long-x1-real-usd",
        "strategy_version": "strategy-registry-v1+3b9ed4d738fa",
        "corpus_version": "icyDenev/Intrader@2024-09-27",
    }


def _expected(census: dict[str, Any], borrowed: dict[str, Any]) -> dict[str, float | int]:
    """The formula, written out independently of the implementation."""
    normal = NormalDist()
    z_alpha = normal.inv_cdf(1 - ALPHA_TWO_SIDED / 2)
    z_power = normal.inv_cdf(JOINT_POWER ** (1 / POWERED_LEGS))
    half_width = float(borrowed["expectancy_ci_high_pct"] - borrowed["expectancy_ci_low_pct"]) / 2
    se = half_width / z_alpha

    charges = census["charged_band_mix"]
    deltas = {
        weighting: charges[f"{S4_STRATEGY_ID}/masked/{weighting}"]["mean_round_trip_charge_pct"]
        - charges[f"{S12_STRATEGY_ID}/masked/{weighting}"]["mean_round_trip_charge_pct"]
        for weighting in ("all_fires", "max_hold_collapse")
    }
    effect = min(deltas.values())

    s12, s4 = census["supply"][f"{S12_STRATEGY_ID}/masked"], census["supply"][f"{S4_STRATEGY_ID}/masked"]
    fan_12 = s12["fired"] / s12["distinct_signal_dates"]
    fan_4 = s4["fired"] / s4["distinct_signal_dates"]
    dispersion = census["bar_return_dispersion"]["masked"]
    ratio = (dispersion["gated_bars"]["sd"] / math.sqrt(fan_12)) / (dispersion["all_bars"]["sd"] / math.sqrt(fan_4))
    se_effective = se * _stale_cost_inflation() * max(1.0, ratio)

    clusters = int(borrowed["bootstrap_cluster_count"])
    required = math.ceil(clusters * (se_effective * (z_alpha + z_power) / effect) ** 2)
    conversion = max(
        s12["distinct_signal_dates"] / charges[f"{S12_STRATEGY_ID}/masked/{weighting}"]["distinct_fill_dates"]
        for weighting in ("all_fires", "max_hold_collapse")
    )
    dates = math.ceil(required * conversion)
    weeks = math.ceil(dates * (s12["span_days"] / (s12["distinct_signal_dates"] - 1)) / 7)
    return {"required_clusters": required, "min_independent_decision_dates": dates, "min_calendar_weeks": weeks}


def test_the_floor_is_the_formula_and_nothing_else() -> None:
    census, borrowed = _census(), _borrowed()
    result = derive(census, borrowed)
    expected = _expected(census, borrowed)
    assert result["required_clusters"] == expected["required_clusters"]
    assert result["min_independent_decision_dates"] == expected["min_independent_decision_dates"]
    assert result["min_calendar_weeks"] == expected["min_calendar_weeks"]
    # sql/333 CHECKs both > 0, and `ForwardShadowFloor` forbids a default.
    assert result["min_independent_decision_dates"] > 0
    assert result["min_calendar_weeks"] > 0


def test_the_effect_takes_the_weighting_that_makes_it_smallest() -> None:
    """A smaller effect needs MORE evidence, so the small one is the strict choice."""
    census = _census(
        s4_charge={"all_fires": 0.70, "max_hold_collapse": 0.90},
        s12_charge={"all_fires": 0.33, "max_hold_collapse": 0.33},
    )
    result = derive(census, _borrowed())
    assert result["effect_weighting"] == "all_fires"
    assert result["minimum_net_effect_pct"] == pytest.approx(0.37)


def test_the_date_conversion_takes_the_weighting_that_demands_most_dates() -> None:
    """Fewer fill-date clusters per signal date means more signal dates per cluster."""
    census = _census(s12_fill_dates={"all_fires": 4_900, "max_hold_collapse": 1_000})
    result = derive(census, _borrowed())
    assert result["conversion_weighting"] == "max_hold_collapse"
    assert result["signal_dates_per_cluster"] == pytest.approx(4_916 / 1_000)


def test_the_dispersion_inflation_is_floored_at_one_and_never_talks_the_floor_down() -> None:
    """A borrowed SE that is already conservative is used as it stands, not discounted."""
    # Gated bars a tenth as volatile and the SAME fan-out: the raw ratio is below 1.
    census = _census(sd_all=0.08, sd_gated=0.008, s12_fired=100, s12_dates=10, s4_fired=100, s4_dates=10)
    result = derive(census, _borrowed())
    assert result["dispersion_ratio"] < 1.0
    assert result["dispersion_inflation"] == 1.0


def test_thin_fan_out_raises_the_requirement_even_when_gated_bars_are_calmer() -> None:
    """The two measured terms pull opposite ways, and both are actually applied."""
    calm_and_broad = derive(_census(s12_fired=44_842, s12_dates=4_916), _borrowed())
    calm_and_thin = derive(_census(s12_fired=4_916, s12_dates=4_916), _borrowed())
    assert calm_and_thin["dispersion_inflation"] > calm_and_broad["dispersion_inflation"]
    assert calm_and_thin["required_clusters"] > calm_and_broad["required_clusters"]


def test_the_stale_cost_inflation_is_the_band_table_bound_and_exceeds_one() -> None:
    factors = [float((1 - band.half_spread) / (1 + band.half_spread)) for band in BANDS]
    assert _stale_cost_inflation() == pytest.approx(max(factors) / min(factors))
    assert _stale_cost_inflation() > 1.0


def test_a_limited_census_cannot_size_a_floor() -> None:
    with pytest.raises(RuntimeError, match="timing slice"):
        derive(_census(limited_to_series=50), _borrowed())


def test_a_corpus_that_failed_closed_to_the_maximum_band_is_refused() -> None:
    """`split_adjusted` means every leg took `UNKNOWN_NOMINAL_PRICE_BAND` — no mix."""
    with pytest.raises(RuntimeError, match="not 'as_traded'"):
        derive(_census(cost_price_basis="split_adjusted"), _borrowed())


def test_a_gate_that_buys_down_nothing_refuses_rather_than_inverting_the_floor() -> None:
    """Squaring hides a sign: a negative effect would produce a plausible finite floor."""
    census = _census(s4_charge={"all_fires": 0.30, "max_hold_collapse": 0.30})
    with pytest.raises(RuntimeError, match="does not buy down"):
        derive(census, _borrowed())


def test_a_cell_that_charged_no_leg_is_named_rather_than_crashing_on_a_none() -> None:
    census = _census()
    census["charged_band_mix"][f"{S12_STRATEGY_ID}/masked/all_fires"]["mean_round_trip_charge_pct"] = None
    with pytest.raises(RuntimeError, match="no charged leg on the all_fires weighting"):
        derive(census, _borrowed())


def test_a_single_decision_date_has_no_arrival_rate() -> None:
    with pytest.raises(RuntimeError, match="fewer than two dates"):
        derive(_census(s12_dates=1), _borrowed())


def test_the_derivation_string_fits_the_column_sql_333_declares() -> None:
    result = derive(_census(), _borrowed())
    assert len(derivation_string(result)) <= 1000
