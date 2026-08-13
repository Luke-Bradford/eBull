from __future__ import annotations

import math
from datetime import date, timedelta
from decimal import Decimal

import pytest

from app.services.indicator_series import BarSeries
from app.services.residual_confluence_candidate import compute_features
from app.services.residual_confluence_evaluation import (
    CandidateObservation,
    evaluate_anchored_fold,
    extract_segment_observations,
)
from app.services.technical_analysis import OHLCVRow


def _fixture(
    *, ambiguous: bool = False, signal_return: float = -0.05
) -> tuple[BarSeries, dict[date, Decimal], dict[date, Decimal], int]:
    count = 272
    signal_index = 260
    dates = tuple(date(2023, 1, 1) + timedelta(days=index) for index in range(count))
    market_returns = [0.0004 * math.sin(index / 7) + 0.0001 for index in range(count)]
    sector_returns = [0.0005 * math.cos(index / 11) - 0.0001 for index in range(count)]
    residuals = [0.0007 * math.sin(index / 5) for index in range(count)]
    instrument_returns = [
        0.00005 + 1.2 * market + 0.7 * sector + residual
        for market, sector, residual in zip(market_returns, sector_returns, residuals, strict=True)
    ]
    instrument_returns[signal_index] = signal_return

    def levels(returns: list[float], start: Decimal) -> list[Decimal]:
        values = [start]
        for value in returns[1:]:
            values.append(values[-1] * Decimal(str(math.exp(value))))
        return values

    market_levels = levels(market_returns, Decimal("400"))
    sector_levels = levels(sector_returns, Decimal("100"))
    instrument_levels = levels(instrument_returns, Decimal("50"))
    rows: list[OHLCVRow] = []
    for index, close in enumerate(instrument_levels):
        open_ = close
        rows.append(
            OHLCVRow(
                open=open_,
                high=open_ + Decimal("1"),
                low=open_ - Decimal("1"),
                close=close,
                volume=1_000_000 + index * 100,
            )
        )
    fill = instrument_levels[signal_index + 1]
    rows[signal_index + 1] = OHLCVRow(
        open=fill,
        high=fill + Decimal("10"),
        low=fill - (Decimal("10") if ambiguous else Decimal("0.5")),
        close=fill,
        volume=1_000_000,
    )
    return (
        BarSeries(dates=dates, rows=tuple(rows)),
        dict(zip(dates, market_levels, strict=True)),
        dict(zip(dates, sector_levels, strict=True)),
        signal_index,
    )


def test_extraction_matches_scalar_feature_contract_and_resolves_target() -> None:
    series, market_closes, sector_closes, signal_index = _fixture()
    observations, census = extract_segment_observations(
        instrument_id=42,
        series=series,
        market_closes=market_closes,
        sector_closes=sector_closes,
    )
    observation = next(item for item in observations if item.signal_date == series.dates[signal_index])
    instrument_returns = [
        math.log(float(series.rows[index]["close"] / series.rows[index - 1]["close"]))
        for index in range(1, len(series))
    ]
    market_returns = [
        math.log(float(market_closes[series.dates[index]] / market_closes[series.dates[index - 1]]))
        for index in range(1, len(series))
    ]
    sector_returns = [
        math.log(float(sector_closes[series.dates[index]] / sector_closes[series.dates[index - 1]]))
        for index in range(1, len(series))
    ]
    prior_volumes = [series.rows[index]["volume"] for index in range(signal_index - 20, signal_index)]
    signal_volume = series.rows[signal_index]["volume"]
    assert all(value is not None for value in prior_volumes) and signal_volume is not None
    scalar = compute_features(
        prior_instrument_returns=instrument_returns[signal_index - 127 : signal_index - 1],
        prior_market_returns=market_returns[signal_index - 253 : signal_index - 1],
        prior_sector_returns=sector_returns[signal_index - 127 : signal_index - 1],
        prior_closes=[float(series.rows[index]["close"]) for index in range(signal_index - 20, signal_index)],
        prior_volumes=[float(value) for value in prior_volumes if value is not None],
        signal_instrument_return=instrument_returns[signal_index - 1],
        signal_market_return=market_returns[signal_index - 1],
        signal_sector_return=sector_returns[signal_index - 1],
        signal_open=float(series.rows[signal_index]["open"]),
        signal_high=float(series.rows[signal_index]["high"]),
        signal_low=float(series.rows[signal_index]["low"]),
        signal_close=float(series.rows[signal_index]["close"]),
        signal_volume=float(signal_volume),
    )
    assert observation.model_row == pytest.approx(scalar.model_row)
    assert observation.observed_class == "target_first"
    assert observation.realised_net_return_pct is not None
    assert observation.target_net_payoff_pct > 0
    assert observation.stop_net_payoff_pct < 0
    assert census.observations == len(observations)


def test_ambiguous_bar_remains_two_explicit_arms() -> None:
    series, market_closes, sector_closes, signal_index = _fixture(ambiguous=True)
    observations, _ = extract_segment_observations(
        instrument_id=42,
        series=series,
        market_closes=market_closes,
        sector_closes=sector_closes,
    )
    observation = next(item for item in observations if item.signal_date == series.dates[signal_index])
    assert observation.observed_class == "ambiguous"
    assert observation.realised_net_return_pct is None
    assert observation.label_for_arm("best_case") == "target_first"
    assert observation.label_for_arm("worst_case") == "stop_first"
    assert observation.return_for_arm("best_case") > 0
    assert observation.return_for_arm("worst_case") < 0


def test_missing_exact_comparator_session_refuses_affected_window() -> None:
    series, market_closes, sector_closes, signal_index = _fixture()
    del market_closes[series.dates[signal_index]]
    observations, _ = extract_segment_observations(
        instrument_id=42,
        series=series,
        market_closes=market_closes,
        sector_closes=sector_closes,
    )
    assert all(item.signal_date != series.dates[signal_index] for item in observations)


def test_extra_instrument_session_is_omitted_without_poisoning_common_axis() -> None:
    series, market_closes, sector_closes, signal_index = _fixture()
    del market_closes[series.dates[signal_index - 10]]
    del sector_closes[series.dates[signal_index - 10]]
    observations, _ = extract_segment_observations(
        instrument_id=42,
        series=series,
        market_closes=market_closes,
        sector_closes=sector_closes,
    )
    assert any(item.signal_date == series.dates[signal_index] for item in observations)


def test_incomplete_frontier_outcomes_are_refused_and_counted() -> None:
    series, market_closes, sector_closes, _ = _fixture()
    observations, census = extract_segment_observations(
        instrument_id=42,
        series=BarSeries(dates=series.dates[:263], rows=series.rows[:263]),
        market_closes=market_closes,
        sector_closes=sector_closes,
    )
    assert census.incomplete_outcome > 0
    assert all(item.exit_date <= series.dates[262] for item in observations)


def test_target_that_cannot_clear_frozen_spread_is_refused() -> None:
    series, market_closes, sector_closes, _ = _fixture(signal_return=-0.001)
    narrow_rows = tuple(
        OHLCVRow(
            open=row["close"],
            high=row["close"] + Decimal("0.001"),
            low=row["close"] - Decimal("0.001"),
            close=row["close"],
            volume=row["volume"],
        )
        for row in series.rows
    )
    observations, census = extract_segment_observations(
        instrument_id=42,
        series=BarSeries(dates=series.dates, rows=narrow_rows),
        market_closes=market_closes,
        sector_closes=sector_closes,
    )
    assert not observations
    assert census.uneconomic_bracket > 0


def _model_observations() -> tuple[CandidateObservation, ...]:
    observations: list[CandidateObservation] = []
    classes = ("target_first", "stop_first", "timeout")
    for index in range(36):
        signal = date(2023, 1, 2) + timedelta(days=index * 7)
        outcome = classes[index % 3]
        realised = 2.0 if outcome == "target_first" else -1.5 if outcome == "stop_first" else -0.2
        observations.append(
            CandidateObservation(
                instrument_id=index % 5 + 1,
                signal_date=signal,
                entry_date=signal + timedelta(days=1),
                exit_date=signal + timedelta(days=3),
                model_row=(
                    -0.1 - index / 20,
                    math.sin(index),
                    math.cos(index / 2),
                    16.0 + index / 100,
                    0.8 + index / 200,
                    math.sin(index / 3),
                ),
                observed_class=outcome,  # type: ignore[arg-type]
                target_net_payoff_pct=2.0,
                stop_net_payoff_pct=-1.5,
                realised_net_return_pct=realised,
            )
        )
    for index in range(9):
        signal = date(2024, 1, 2) + timedelta(days=index // 3)
        outcome = classes[index % 3]
        realised = 2.0 if outcome == "target_first" else -1.5 if outcome == "stop_first" else -0.2
        observations.append(
            CandidateObservation(
                instrument_id=index % 3 + 1,
                signal_date=signal,
                entry_date=signal + timedelta(days=1),
                exit_date=signal + timedelta(days=3),
                model_row=(
                    -0.2 - index / 10,
                    math.sin(index + 1),
                    math.cos(index / 2 + 1),
                    16.5 + index / 100,
                    0.9 + index / 100,
                    math.sin(index / 3 + 1),
                ),
                observed_class=outcome,  # type: ignore[arg-type]
                target_net_payoff_pct=4.0,
                stop_net_payoff_pct=-1.0,
                realised_net_return_pct=realised,
            )
        )
    return tuple(observations)


def test_anchored_fold_trains_only_on_completed_prior_outcomes_and_suppresses_overlap() -> None:
    observations = _model_observations()
    evaluation = evaluate_anchored_fold(
        observations,
        test_start=date(2024, 1, 1),
        test_end=date(2024, 12, 31),
        ambiguity_arm="worst_case",
    )
    assert evaluation.training_count == 36
    assert evaluation.test_candidate_count == 9
    assert evaluation.accepted_count <= evaluation.test_candidate_count
    assert evaluation.overlap_suppressed > 0
    assert math.isfinite(evaluation.brier_score)
    assert math.isfinite(evaluation.log_loss)
    assert len(evaluation.returns) == len(evaluation.entry_dates) == evaluation.accepted_count
    assert evaluation.baseline_count > 0
    assert evaluation.baseline_expectancy_pct is not None
    assert evaluation.max_entry_date_share_pct is not None
    assert sum(count for _, count in evaluation.observed_class_counts) == evaluation.accepted_count
    assert len(evaluation.ev_deciles) == min(10, evaluation.baseline_count)
    assert sum(bucket.count for bucket in evaluation.ev_deciles) == evaluation.baseline_count
