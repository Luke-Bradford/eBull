"""Read-only recent-outcome construction for residual confluence candidate #2499.

This module is pure.  It does not select a profitable threshold, persist a
feature history, register a strategy, or reach an order path.  It turns one
already-masked price segment and exact comparator closes into the immutable
candidate observations consumed by the one-read verifier.
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Literal

import numpy as np

from app.services.cost_model import buy_price, half_spread_for, sell_price
from app.services.indicator_series import BarSeries, atr_series
from app.services.residual_confluence_candidate import (
    ATR_PERIOD,
    MARKET_VOL_LONG_LOOKBACK,
    MARKET_VOL_SHORT_LOOKBACK,
    MAX_HOLD_SESSIONS,
    MIN_MEDIAN_DOLLAR_VOLUME,
    MIN_SIGNAL_CLOSE,
    MODEL_FEATURE_NAMES,
    OLS_LOOKBACK,
    RESIDUAL_VOL_LOOKBACK,
    STOP_ATR_MULTIPLE,
    TARGET_ATR_MULTIPLE,
    VOLUME_LOOKBACK,
    OutcomeClass,
    expected_net_value_from_net_payoffs_pct,
    fit_model,
)

ObservedClass = Literal["target_first", "stop_first", "timeout", "ambiguous"]


@dataclass(frozen=True)
class CandidateObservation:
    instrument_id: int
    signal_date: date
    entry_date: date
    exit_date: date
    model_row: tuple[float, ...]
    observed_class: ObservedClass
    target_net_payoff_pct: float
    stop_net_payoff_pct: float
    realised_net_return_pct: float | None

    def label_for_arm(
        self, arm: Literal["best_case", "worst_case"]
    ) -> Literal["target_first", "stop_first", "timeout"]:
        if self.observed_class == "ambiguous":
            return "target_first" if arm == "best_case" else "stop_first"
        return self.observed_class

    def return_for_arm(self, arm: Literal["best_case", "worst_case"]) -> float:
        if self.observed_class == "ambiguous":
            return self.target_net_payoff_pct if arm == "best_case" else self.stop_net_payoff_pct
        if self.realised_net_return_pct is None:  # pragma: no cover - dataclass construction is internal
            raise RuntimeError("a resolved non-ambiguous observation has no return")
        return self.realised_net_return_pct


@dataclass(frozen=True)
class ExtractionCensus:
    bars_seen: int = 0
    eligible_features: int = 0
    non_negative_shock: int = 0
    incomplete_outcome: int = 0
    uneconomic_bracket: int = 0
    observations: int = 0


@dataclass(frozen=True)
class EvDecile:
    rank: int
    count: int
    mean_predicted_ev_pct: float
    realised_expectancy_pct: float
    win_rate_pct: float


@dataclass(frozen=True)
class FoldEvaluation:
    test_start: date
    test_end: date
    ambiguity_arm: Literal["best_case", "worst_case"]
    training_count: int
    test_candidate_count: int
    accepted_count: int
    overlap_suppressed: int
    win_rate_pct: float | None
    expectancy_pct: float | None
    profit_factor: float | None
    brier_score: float
    log_loss: float
    mean_predicted_ev_pct: float | None
    baseline_count: int
    baseline_expectancy_pct: float | None
    baseline_win_rate_pct: float | None
    max_entry_date_share_pct: float | None
    observed_class_counts: tuple[tuple[str, int], ...]
    feature_win_loss_standardised_differences: tuple[tuple[str, float], ...]
    ev_deciles: tuple[EvDecile, ...]
    returns: tuple[float, ...]
    entry_dates: tuple[date, ...]


def _log_returns(closes: Sequence[Decimal | None]) -> np.ndarray:
    result = np.full(len(closes), np.nan, dtype=float)
    for index in range(1, len(closes)):
        previous, current = closes[index - 1], closes[index]
        if previous is None or current is None or previous <= 0 or current <= 0:
            continue
        result[index] = math.log(float(current / previous))
    return result


def _net_return_pct(entry: Decimal, exit_: Decimal) -> float:
    half_spread = half_spread_for(entry)
    paid = buy_price(entry, half_spread=half_spread)
    received = sell_price(exit_, half_spread=half_spread)
    return float((received - paid) / paid * Decimal(100))


def _resolve(
    series: BarSeries,
    *,
    signal_index: int,
    atr: float,
) -> tuple[ObservedClass, int, Decimal, Decimal, Decimal | None] | None:
    fill_index = signal_index + 1
    timeout_index = fill_index + MAX_HOLD_SESSIONS - 1
    if timeout_index >= len(series):
        return None
    entry = series.rows[fill_index].get("open")
    if entry is None or entry <= 0:
        return None
    target = entry + TARGET_ATR_MULTIPLE * Decimal(str(atr))
    stop = entry - STOP_ATR_MULTIPLE * Decimal(str(atr))
    if stop <= 0:
        return None
    for index in range(fill_index, timeout_index + 1):
        row = series.rows[index]
        open_, high, low = row.get("open"), row.get("high"), row.get("low")
        if open_ is None or high is None or low is None or min(open_, high, low) <= 0:
            return None
        if open_ <= stop:
            return "stop_first", index, entry, target, open_
        if open_ >= target:
            return "target_first", index, entry, target, open_
        if low <= stop and high >= target:
            return "ambiguous", index, entry, target, None
        if low <= stop:
            # Daily OHLC cannot recover the stop-market execution price after
            # an intrabar trigger. The frozen development proxy is the stop
            # level; unobserved adverse slippage remains a promotion refusal.
            return "stop_first", index, entry, target, stop
        if high >= target:
            # A touched sell limit is valued at its limit, not the unknown
            # potentially better intrabar price. This is conservative.
            return "target_first", index, entry, target, target
    timeout_close = series.rows[timeout_index].get("close")
    if timeout_close is None or timeout_close <= 0:
        return None
    return "timeout", timeout_index, entry, target, timeout_close


def extract_segment_observations(
    *,
    instrument_id: int,
    series: BarSeries,
    market_closes: Mapping[date, Decimal],
    sector_closes: Mapping[date, Decimal],
) -> tuple[tuple[CandidateObservation, ...], ExtractionCensus]:
    """Evaluate exact causal windows in one scale-homogeneous segment."""
    if instrument_id <= 0:
        raise ValueError("instrument_id must be positive")
    # "Exact common sessions" means the intersection of the three observed
    # calendars, not requiring one provider to carry every date another does.
    # A masked instrument close remains on this axis as None and poisons every
    # return window that uses it; only a genuinely absent comparator session is
    # omitted from the shared calendar.
    common_indices = [
        index for index, bar_date in enumerate(series.dates) if bar_date in market_closes and bar_date in sector_closes
    ]
    common_instrument_closes = [series.rows[index].get("close") for index in common_indices]
    common_market_closes = [market_closes[series.dates[index]] for index in common_indices]
    common_sector_closes = [sector_closes[series.dates[index]] for index in common_indices]
    instrument_returns = _log_returns(common_instrument_closes)
    market_returns = _log_returns(common_market_closes)
    sector_returns = _log_returns(common_sector_closes)
    atr_values = atr_series(series, universe="survivor_only", period=ATR_PERIOD).values
    rows: list[CandidateObservation] = []
    eligible = non_negative = incomplete = uneconomic = 0

    # A signal at i needs market return i-252, which itself needs the aligned
    # close at i-253. Exact windows are refused rather than shortened.
    for common_position in range(MARKET_VOL_LONG_LOOKBACK + 1, len(common_indices)):
        index = common_indices[common_position]
        row = series.rows[index]
        open_, high, low, close, volume = (
            row.get("open"),
            row.get("high"),
            row.get("low"),
            row.get("close"),
            row.get("volume"),
        )
        if any(value is None for value in (open_, high, low, close, volume)):
            continue
        assert open_ is not None and high is not None and low is not None and close is not None and volume is not None
        if (
            min(open_, high, low, close, volume) <= 0
            or high < max(open_, close)
            or low > min(open_, close)
            or high <= low
        ):
            continue
        if close < MIN_SIGNAL_CLOSE:
            continue
        atr = atr_values[index]
        if atr is None or atr <= 0:
            continue

        market_history = market_returns[common_position - MARKET_VOL_LONG_LOOKBACK : common_position]
        market = market_history[-OLS_LOOKBACK:]
        sector = sector_returns[common_position - OLS_LOOKBACK : common_position]
        instrument = instrument_returns[common_position - OLS_LOOKBACK : common_position]
        current = (
            instrument_returns[common_position],
            market_returns[common_position],
            sector_returns[common_position],
        )
        if not all(np.isfinite(values).all() for values in (market_history, sector, instrument)) or not all(
            math.isfinite(value) for value in current
        ):
            continue
        design = np.column_stack((np.ones(OLS_LOOKBACK), market, sector))
        if int(np.linalg.matrix_rank(design)) != design.shape[1]:
            continue
        coefficients, *_ = np.linalg.lstsq(design, instrument, rcond=None)
        residual_history = instrument - design @ coefficients
        residual_vol = float(np.std(residual_history[-RESIDUAL_VOL_LOOKBACK:], ddof=1))
        if not math.isfinite(residual_vol) or residual_vol <= np.finfo(float).eps:
            continue
        residual = float(current[0] - coefficients[0] - coefficients[1] * current[1] - coefficients[2] * current[2])
        shock_z = residual / residual_vol

        prior = series.rows[index - VOLUME_LOOKBACK : index]
        prior_closes = [item.get("close") for item in prior]
        prior_volumes = [item.get("volume") for item in prior]
        if any(value is None or value <= 0 for value in (*prior_closes, *prior_volumes)):
            continue
        median_volume = float(np.median(np.asarray(prior_volumes, dtype=float)))
        dollar_volumes = np.asarray(prior_closes, dtype=float) * np.asarray(prior_volumes, dtype=float)
        median_dollar_volume = float(np.median(dollar_volumes))
        if Decimal(str(median_dollar_volume)) < MIN_MEDIAN_DOLLAR_VOLUME:
            continue
        short_vol = float(np.std(market_history[-MARKET_VOL_SHORT_LOOKBACK:], ddof=1))
        long_vol = float(np.std(market_history, ddof=1))
        if not math.isfinite(short_vol) or not math.isfinite(long_vol) or long_vol <= 0:
            continue
        close_location = float((Decimal(2) * close - high - low) / (high - low))
        abnormal_volume = math.log(float(volume) / median_volume)
        liquidity = math.log(median_dollar_volume)
        stress = short_vol / long_vol
        model_row = (
            shock_z,
            close_location,
            abnormal_volume,
            liquidity,
            stress,
            shock_z * close_location * abnormal_volume,
        )
        if not all(math.isfinite(value) for value in model_row):
            continue
        eligible += 1
        if shock_z >= 0:
            non_negative += 1
            continue
        resolution = _resolve(series, signal_index=index, atr=atr)
        if resolution is None:
            incomplete += 1
            continue
        outcome, exit_index, entry, target, exit_price = resolution
        stop = entry - STOP_ATR_MULTIPLE * Decimal(str(atr))
        target_payoff = _net_return_pct(entry, target)
        stop_payoff = _net_return_pct(entry, stop)
        if target_payoff <= 0 or stop_payoff >= 0:
            uneconomic += 1
            continue
        realised = None if exit_price is None else _net_return_pct(entry, exit_price)
        rows.append(
            CandidateObservation(
                instrument_id=instrument_id,
                signal_date=series.dates[index],
                entry_date=series.dates[index + 1],
                exit_date=series.dates[exit_index],
                model_row=model_row,
                observed_class=outcome,
                target_net_payoff_pct=target_payoff,
                stop_net_payoff_pct=stop_payoff,
                realised_net_return_pct=realised,
            )
        )
    return tuple(rows), ExtractionCensus(
        bars_seen=len(series),
        eligible_features=eligible,
        non_negative_shock=non_negative,
        incomplete_outcome=incomplete,
        uneconomic_bracket=uneconomic,
        observations=len(rows),
    )


def evaluate_anchored_fold(
    observations: tuple[CandidateObservation, ...],
    *,
    test_start: date,
    test_end: date,
    ambiguity_arm: Literal["best_case", "worst_case"],
) -> FoldEvaluation:
    """Fit on completed prior outcomes and apply the frozen positive-EV rule."""
    if test_end < test_start:
        raise ValueError("test interval is inverted")
    training = tuple(item for item in observations if item.exit_date < test_start)
    testing = tuple(item for item in observations if test_start <= item.signal_date <= test_end)
    if not training:
        raise ValueError(f"no completed training observations before {test_start}")
    if not testing:
        raise ValueError(f"no candidate observations in {test_start}/{test_end}")
    labels: tuple[OutcomeClass, ...] = tuple(item.label_for_arm(ambiguity_arm) for item in training)
    model = fit_model(tuple(item.model_row for item in training), labels)
    timeout_returns = [
        item.return_for_arm(ambiguity_arm) for item, label in zip(training, labels, strict=True) if label == "timeout"
    ]
    if not timeout_returns:
        raise ValueError("training fold has no timeout payoff")
    mean_timeout = sum(timeout_returns) / len(timeout_returns)

    squared_errors = 0.0
    log_loss = 0.0
    ordered = sorted(testing, key=lambda item: (item.signal_date, item.instrument_id))
    probabilities_by_id: dict[int, dict[Literal["target_first", "stop_first", "timeout"], float]] = {}
    predicted_ev_by_id: dict[int, float] = {}
    for item in ordered:
        probabilities = model.probabilities(item.model_row)
        probabilities_by_id[id(item)] = probabilities
        predicted_ev_by_id[id(item)] = expected_net_value_from_net_payoffs_pct(
            probabilities,
            target_net_payoff_pct=item.target_net_payoff_pct,
            stop_net_payoff_pct=item.stop_net_payoff_pct,
            mean_timeout_net_payoff_pct=mean_timeout,
        )
        actual = item.label_for_arm(ambiguity_arm)
        squared_errors += sum((probability - (label == actual)) ** 2 for label, probability in probabilities.items())
        log_loss -= math.log(max(probabilities[actual], np.finfo(float).tiny))

    baseline: list[CandidateObservation] = []
    baseline_available: dict[int, date] = {}
    for item in ordered:
        if item.signal_date <= baseline_available.get(item.instrument_id, date.min):
            continue
        baseline.append(item)
        baseline_available[item.instrument_id] = item.exit_date

    accepted: list[CandidateObservation] = []
    next_available: dict[int, date] = {}
    overlap_suppressed = 0
    for item in ordered:
        if item.signal_date <= next_available.get(item.instrument_id, date.min):
            overlap_suppressed += 1
            continue
        expected_value = predicted_ev_by_id[id(item)]
        if expected_value <= 0:
            continue
        accepted.append(item)
        next_available[item.instrument_id] = item.exit_date

    returns = tuple(item.return_for_arm(ambiguity_arm) for item in accepted)
    wins = sum(value > 0 for value in returns)
    gains = sum(value for value in returns if value > 0)
    losses = -sum(value for value in returns if value < 0)
    baseline_returns = tuple(item.return_for_arm(ambiguity_arm) for item in baseline)
    baseline_wins = sum(value > 0 for value in baseline_returns)
    accepted_predictions = tuple(predicted_ev_by_id[id(item)] for item in accepted)
    entry_date_counts: dict[date, int] = {}
    for entry_date in (item.entry_date for item in accepted):
        entry_date_counts[entry_date] = entry_date_counts.get(entry_date, 0) + 1
    class_counts: dict[str, int] = {}
    for item in accepted:
        label = item.label_for_arm(ambiguity_arm)
        class_counts[label] = class_counts.get(label, 0) + 1

    feature_differences: list[tuple[str, float]] = []
    if any(value > 0 for value in returns) and any(value <= 0 for value in returns):
        matrix = np.asarray([item.model_row for item in accepted], dtype=float)
        win_mask = np.asarray([value > 0 for value in returns], dtype=bool)
        pooled_std = np.std(matrix, axis=0, ddof=1)
        differences = np.divide(
            np.mean(matrix[win_mask], axis=0) - np.mean(matrix[~win_mask], axis=0),
            pooled_std,
            out=np.zeros_like(pooled_std),
            where=pooled_std > np.finfo(float).eps,
        )
        feature_differences = list(zip(MODEL_FEATURE_NAMES, (float(value) for value in differences), strict=True))

    deciles: list[EvDecile] = []
    # Rank the same non-overlapping opportunity set used by the unfiltered
    # challenger. Otherwise serial repeats from one instrument can manufacture
    # apparent model discrimination and make the deciles incomparable.
    ranked_indices = np.argsort([predicted_ev_by_id[id(item)] for item in baseline])
    for rank, indices in enumerate(np.array_split(ranked_indices, min(10, len(ranked_indices))), start=1):
        bucket = [baseline[int(index)] for index in indices]
        bucket_returns = [item.return_for_arm(ambiguity_arm) for item in bucket]
        deciles.append(
            EvDecile(
                rank=rank,
                count=len(bucket),
                mean_predicted_ev_pct=sum(predicted_ev_by_id[id(item)] for item in bucket) / len(bucket),
                realised_expectancy_pct=sum(bucket_returns) / len(bucket_returns),
                win_rate_pct=sum(value > 0 for value in bucket_returns) / len(bucket_returns) * 100,
            )
        )
    return FoldEvaluation(
        test_start=test_start,
        test_end=test_end,
        ambiguity_arm=ambiguity_arm,
        training_count=len(training),
        test_candidate_count=len(testing),
        accepted_count=len(accepted),
        overlap_suppressed=overlap_suppressed,
        win_rate_pct=None if not returns else wins / len(returns) * 100,
        expectancy_pct=None if not returns else sum(returns) / len(returns),
        profit_factor=None if losses == 0 else gains / losses,
        brier_score=squared_errors / len(testing),
        log_loss=log_loss / len(testing),
        mean_predicted_ev_pct=(
            None if not accepted_predictions else sum(accepted_predictions) / len(accepted_predictions)
        ),
        baseline_count=len(baseline_returns),
        baseline_expectancy_pct=None if not baseline_returns else sum(baseline_returns) / len(baseline_returns),
        baseline_win_rate_pct=None if not baseline_returns else baseline_wins / len(baseline_returns) * 100,
        max_entry_date_share_pct=(None if not accepted else max(entry_date_counts.values()) / len(accepted) * 100),
        observed_class_counts=tuple(sorted(class_counts.items())),
        feature_win_loss_standardised_differences=tuple(feature_differences),
        ev_deciles=tuple(deciles),
        returns=returns,
        entry_dates=tuple(item.entry_date for item in accepted),
    )


__all__ = [
    "CandidateObservation",
    "ExtractionCensus",
    "EvDecile",
    "FoldEvaluation",
    "ObservedClass",
    "evaluate_anchored_fold",
    "extract_segment_observations",
]
