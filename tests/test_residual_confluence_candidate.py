from __future__ import annotations

import math
from decimal import Decimal

import numpy as np
import pytest

from app.services.residual_confluence_candidate import (
    CANDIDATE_VERSION,
    DEFINITION,
    MODEL_FEATURE_NAMES,
    FeatureRefusal,
    FeatureStandardisation,
    build_exit_bracket,
    compute_features,
    definition_hash,
    definition_json,
    expected_net_value_from_net_payoffs_pct,
    expected_net_value_pct,
    fit_model,
)


def _inputs() -> dict[str, object]:
    market_stress = [0.001 + 0.0002 * math.sin(i / 7) for i in range(252)]
    market = market_stress[-126:]
    sector = [0.0005 + 0.0003 * math.cos(i / 9) for i in range(126)]
    # Make a non-zero residual exactly orthogonal to the OLS design so this
    # fixture can independently pin alpha/betas rather than merely repeat the
    # implementation's least-squares answer.
    design = np.column_stack((np.ones(126), market, sector))
    raw_residual = np.asarray([0.0004 * math.sin(i / 5) for i in range(126)])
    residual = raw_residual - design @ np.linalg.lstsq(design, raw_residual, rcond=None)[0]
    instrument = [0.0001 + 1.2 * m + 0.7 * s + e for m, s, e in zip(market, sector, residual, strict=True)]
    return {
        "prior_instrument_returns": instrument,
        "prior_market_returns": market_stress,
        "prior_sector_returns": sector,
        "prior_closes": [30.0 + i / 10 for i in range(20)],
        "prior_volumes": [1_000_000 + i * 1_000 for i in range(20)],
        "signal_instrument_return": -0.025,
        "signal_market_return": -0.004,
        "signal_sector_return": -0.006,
        "signal_open": 26.0,
        "signal_high": 27.0,
        "signal_low": 24.0,
        "signal_close": 24.5,
        "signal_volume": 2_500_000,
    }


def test_definition_is_complete_stable_and_contains_no_measured_result() -> None:
    payload = definition_json()
    # ⚠ Moved by #2720 (cost_model_id is a definition input; the carry/FX
    # structural closure is a new model, so the candidate version moves with
    # it — that is the pin doing its job, not collateral).
    assert definition_hash() == "6173442ec8ed4c8518b9e8f9baea9e3aaf5d92b2cb2f58f7fce776e9c800ccc8"
    assert CANDIDATE_VERSION == "residual-confluence-v1+6173442ec8ed"
    assert DEFINITION.market_vol_long_lookback == 252
    assert DEFINITION.model_features == MODEL_FEATURE_NAMES
    assert "expectancy" not in payload
    assert "coefficient" not in payload


def test_features_recover_prior_factor_model_and_use_current_completed_bar() -> None:
    features = compute_features(**_inputs())  # type: ignore[arg-type]
    assert features.alpha == pytest.approx(0.0001, abs=1e-9)
    assert features.beta_market == pytest.approx(1.2, abs=1e-9)
    assert features.beta_sector == pytest.approx(0.7, abs=1e-9)
    assert features.residual_return == pytest.approx(-0.0161, abs=1e-9)
    assert features.shock_z < 0
    assert features.close_location == pytest.approx(-2 / 3)
    assert features.abnormal_volume > 0
    assert features.market_stress > 0
    assert features.shock_x_location_x_volume == pytest.approx(
        features.shock_z * features.close_location * features.abnormal_volume
    )
    assert features.model_row == (
        features.shock_z,
        features.close_location,
        features.abnormal_volume,
        features.log_dollar_liquidity,
        features.market_stress,
        features.shock_x_location_x_volume,
    )


def test_appending_future_values_cannot_change_snapshot() -> None:
    inputs = _inputs()
    baseline = compute_features(**inputs)  # type: ignore[arg-type]
    # The API requires exact causal windows. Passing an appended outcome/future
    # value is refused instead of being silently truncated into the calculation.
    future = dict(inputs)
    future["prior_instrument_returns"] = [*inputs["prior_instrument_returns"], 99.0]  # type: ignore[misc]
    with pytest.raises(FeatureRefusal, match="exactly 126"):
        compute_features(**future)  # type: ignore[arg-type]
    assert compute_features(**inputs) == baseline  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("field", "replacement", "message"),
    [
        ("prior_market_returns", [0.01] * 251, "exactly 252"),
        ("prior_sector_returns", [0.0] * 126, "rank deficient"),
        ("prior_volumes", [0.0] * 20, "must be positive"),
        ("prior_closes", [0.9] * 20, "liquidity floor"),
        ("signal_high", 24.0, "OHLC envelope"),
        ("signal_volume", float("nan"), "non-finite"),
    ],
)
def test_missing_or_unsafe_feature_input_fails_closed(field: str, replacement: object, message: str) -> None:
    inputs = _inputs()
    inputs[field] = replacement
    with pytest.raises(FeatureRefusal, match=message):
        compute_features(**inputs)  # type: ignore[arg-type]


def test_price_floor_is_checked_after_a_valid_ohlc_envelope() -> None:
    inputs = _inputs()
    inputs.update(
        signal_open=Decimal("19.75"),
        signal_high=Decimal("20.5"),
        signal_low=Decimal("19"),
        signal_close=Decimal("19.99"),
    )
    with pytest.raises(FeatureRefusal, match="20 USD floor"):
        compute_features(**inputs)  # type: ignore[arg-type]


def test_price_floor_uses_original_decimal_without_float_round_trip() -> None:
    inputs = _inputs()
    inputs.update(
        signal_open=Decimal("20"),
        signal_high=Decimal("20.5"),
        signal_low=Decimal("19.5"),
        signal_close=Decimal("19.999999999999999999"),
    )
    with pytest.raises(FeatureRefusal, match="20 USD floor"):
        compute_features(**inputs)  # type: ignore[arg-type]


def test_zero_residual_volatility_is_refused() -> None:
    inputs = _inputs()
    market = inputs["prior_market_returns"][-126:]  # type: ignore[index]
    sector = inputs["prior_sector_returns"]
    inputs["prior_instrument_returns"] = [
        0.0001 + 1.2 * m + 0.7 * s
        for m, s in zip(market, sector, strict=True)  # type: ignore[arg-type]
    ]
    with pytest.raises(FeatureRefusal, match="residual volatility"):
        compute_features(**inputs)  # type: ignore[arg-type]


def test_bracket_is_fixed_from_signal_atr_and_refuses_unorderable_stop() -> None:
    bracket = build_exit_bracket(entry_price=Decimal("25"), signal_atr=Decimal("2"))
    assert bracket.target_price == Decimal("28.0")
    assert bracket.stop_price == Decimal("23.0")
    assert bracket.max_hold_sessions == 5
    with pytest.raises(FeatureRefusal, match="non-positive"):
        build_exit_bracket(entry_price=Decimal("1"), signal_atr=Decimal("2"))


def test_market_stress_is_sample_std_ratio_not_level_or_future_return() -> None:
    inputs = _inputs()
    features = compute_features(**inputs)  # type: ignore[arg-type]
    history = np.asarray(inputs["prior_market_returns"], dtype=float)
    expected = float(np.std(history[-20:], ddof=1) / np.std(history, ddof=1))
    assert features.market_stress == pytest.approx(expected)


def _model_population() -> tuple[list[tuple[float, ...]], list[str]]:
    rows: list[tuple[float, ...]] = []
    labels: list[str] = []
    for index in range(90):
        shock = -2.0 + index * 4.0 / 89.0
        location = math.sin(index / 7)
        abnormal_volume = math.cos(index / 11)
        row = (
            shock,
            location,
            abnormal_volume,
            17.0 + index / 100,
            0.8 + index / 200,
            shock * location * abnormal_volume,
        )
        rows.append(row)
        labels.append("target_first" if shock < -0.6 else "stop_first" if shock > 0.6 else "timeout")
    return rows, labels


def test_fitted_model_is_deterministic_training_only_and_probabilities_sum_to_one() -> None:
    rows, labels = _model_population()
    first = fit_model(rows, labels)  # type: ignore[arg-type]
    second = fit_model(rows, labels)  # type: ignore[arg-type]
    assert first == second
    probabilities = first.probabilities(rows[0])
    assert sum(probabilities.values()) == pytest.approx(1.0)
    assert probabilities["target_first"] > probabilities["stop_first"]
    # A future/holdout row is transformed by, but cannot alter, the stored
    # training-fold means and scales.
    baseline_standardisation = first.standardisation
    first.probabilities((99, 0, 0, 18, 1, 0))
    assert first.standardisation == baseline_standardisation


def test_model_refuses_zero_variation_missing_class_and_unknown_label() -> None:
    rows, labels = _model_population()
    flat = [tuple(1.0 for _ in MODEL_FEATURE_NAMES) for _ in labels]
    with pytest.raises(FeatureRefusal, match="zero or unavailable"):
        fit_model(flat, labels)  # type: ignore[arg-type]
    with pytest.raises(FeatureRefusal, match="missing outcome"):
        fit_model(rows[:20], ["target_first"] * 20)  # type: ignore[arg-type]
    bad_labels = list(labels)
    bad_labels[0] = "profit"
    with pytest.raises(FeatureRefusal, match="unknown outcome"):
        fit_model(rows, bad_labels)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("means", "sample_stds", "message"),
    [
        ((0.0,) * 5, (1.0,) * 6, "requires 6"),
        ((0.0,) * 6, (1.0,) * 5, "requires 6"),
        ((0.0,) * 5 + (float("nan"),), (1.0,) * 6, "non-finite"),
        ((0.0,) * 6, (1.0,) * 5 + (0.0,), "zero or unavailable"),
    ],
)
def test_standardisation_cannot_be_constructed_with_unsafe_scales(
    means: tuple[float, ...], sample_stds: tuple[float, ...], message: str
) -> None:
    with pytest.raises(FeatureRefusal, match=message):
        FeatureStandardisation(means, sample_stds)


def test_expected_net_value_uses_all_outcomes_and_costs() -> None:
    result = expected_net_value_pct(
        {"target_first": 0.55, "stop_first": 0.30, "timeout": 0.15},
        target_payoff_pct=3.0,
        stop_loss_pct=2.0,
        expected_timeout_payoff_pct=-0.2,
        total_cost_pct=0.25,
    )
    assert result == pytest.approx(0.77)
    with pytest.raises(FeatureRefusal, match="sum to one"):
        expected_net_value_pct(
            {"target_first": 0.8, "stop_first": 0.8, "timeout": 0.1},
            target_payoff_pct=3,
            stop_loss_pct=2,
            expected_timeout_payoff_pct=0,
            total_cost_pct=0.2,
        )


def test_expected_net_value_from_net_payoffs_uses_exact_class_payoffs() -> None:
    result = expected_net_value_from_net_payoffs_pct(
        {"target_first": 0.55, "stop_first": 0.30, "timeout": 0.15},
        target_net_payoff_pct=2.7,
        stop_net_payoff_pct=-2.2,
        mean_timeout_net_payoff_pct=-0.4,
    )
    assert result == pytest.approx(0.765)
    with pytest.raises(FeatureRefusal, match="stop net payoff negative"):
        expected_net_value_from_net_payoffs_pct(
            {"target_first": 0.55, "stop_first": 0.30, "timeout": 0.15},
            target_net_payoff_pct=2.7,
            stop_net_payoff_pct=0.1,
            mean_timeout_net_payoff_pct=-0.4,
        )
