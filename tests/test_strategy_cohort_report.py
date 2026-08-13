from __future__ import annotations

from dataclasses import replace
from datetime import date, timedelta

import pytest

from app.services.strategy_cohort_report import (
    UNPOOLED_KEY,
    CandidateCohortReport,
    CohortCell,
    CohortKey,
    CohortObservation,
    CohortPathMetrics,
    WindowEvidence,
    assess_recent_stability,
    build_cohort_report,
    cohort_keys_for,
)


def _observation(
    index: int, *, return_pct: float | None = None, mechanism: str = "liquidity_reversal"
) -> CohortObservation:
    entry = date(2026, 1, 1) + timedelta(days=index)
    realised = (0.7 + (index % 5) * 0.1) if return_pct is None else return_pct
    return CohortObservation(
        instrument_id=100 + index % 8,
        sector=("technology", "financials", "healthcare")[index % 3],
        entry_date=entry,
        exit_date=entry + timedelta(days=1),
        mechanism=mechanism,
        security_type="common_stock",
        primary_listing_market="nasdaq" if index % 2 else "nyse",
        price_band="20_to_50" if index % 2 else "50_to_150",
        dollar_volume_band="25m_to_100m" if index % 2 else "100m_plus",
        net_return_pct=realised,
        net_return_double_cost_pct=realised - 0.2,
        holding_minutes=390.0,
        turnover_fraction=2.0,
        max_adverse_excursion_pct=-0.4 - index % 4 * 0.1,
    )


def _report(observations: list[CohortObservation], *, start: date, end: date, seed: int = 7) -> CandidateCohortReport:
    paths = {
        key: CohortPathMetrics(
            max_drawdown_pct=-3.0,
            exposure_time_pct=25.0,
            turnover_annualised=4.0,
            path_model_id="event-time-path-v1",
        )
        for key in cohort_keys_for(observations)
    }
    return build_cohort_report(
        observations,
        strategy_id="candidate-1",
        strategy_version="strategy-sha",
        context_version="context-sha",
        outcome_version="outcome-sha",
        cost_model_id="cost-sha",
        window_start=start,
        window_end=end,
        root_seed=seed,
        path_metrics=paths,
    )


def test_report_emits_unpooled_declared_slices_and_failures() -> None:
    observations = [_observation(index) for index in range(40)]
    # A real losing mechanism stays in the same report rather than disappearing
    # behind selection of the positive mechanism.
    observations.extend(
        _observation(60 + index, return_pct=-1.0, mechanism="information_continuation") for index in range(10)
    )
    report = _report(observations, start=date(2026, 1, 1), end=date(2026, 3, 20))

    assert report.observation_count == 50
    assert report.cell(UNPOOLED_KEY) is not None
    winner = report.cell(CohortKey(("mechanism",), ("liquidity_reversal",)))
    loser = report.cell(CohortKey(("mechanism",), ("information_continuation",)))
    assert winner is not None and winner.verdict == "economically_positive"
    assert loser is not None and loser.verdict == "refused"
    assert "fewer_than_30_trades" in loser.refusals
    assert "double_cost_expectancy_not_positive" in loser.refusals


def test_report_is_reproducible_and_carries_concentration_and_tail() -> None:
    observations = [_observation(index) for index in range(40)]
    first = _report(observations, start=date(2026, 1, 1), end=date(2026, 2, 15), seed=17)
    second = _report(observations, start=date(2026, 1, 1), end=date(2026, 2, 15), seed=17)
    assert first == second
    cell = first.cell(UNPOOLED_KEY)
    assert cell is not None
    assert cell.entry_date_count == 40
    assert cell.largest_entry_date_share_pct == 2.5
    assert cell.largest_instrument_share_pct == 12.5
    assert cell.largest_sector_share_pct == 35.0
    assert cell.expected_shortfall_5pct <= cell.expectancy_pct
    assert cell.worst_mae_pct == pytest.approx(-0.7)
    assert cell.max_drawdown_pct == -3.0


def test_out_of_window_observation_refuses() -> None:
    with pytest.raises(ValueError, match="outside"):
        _report([_observation(40)], start=date(2026, 1, 1), end=date(2026, 1, 31))


def test_double_cost_failure_blocks_an_otherwise_positive_cell() -> None:
    observations = [replace(_observation(index), net_return_double_cost_pct=-0.01) for index in range(40)]
    report = _report(observations, start=date(2026, 1, 1), end=date(2026, 2, 15))
    cell = report.cell(UNPOOLED_KEY)
    assert cell is not None
    assert cell.verdict == "refused"
    assert cell.refusals == ("double_cost_expectancy_not_positive",)


def test_missing_portfolio_path_is_a_named_fail_closed_refusal() -> None:
    observations = [_observation(index) for index in range(40)]
    report = build_cohort_report(
        observations,
        strategy_id="candidate-1",
        strategy_version="strategy-sha",
        context_version="context-sha",
        outcome_version="outcome-sha",
        cost_model_id="cost-sha",
        window_start=date(2026, 1, 1),
        window_end=date(2026, 2, 15),
        root_seed=7,
    )
    cell = report.cell(UNPOOLED_KEY)
    assert cell is not None
    assert cell.refusals == ("path_metrics_not_computed",)


def _cell(verdict: str = "economically_positive") -> CohortCell:
    refusals = () if verdict == "economically_positive" else ("expectancy_ci_not_strictly_positive",)
    return CohortCell(
        key=UNPOOLED_KEY,
        trade_count=40,
        entry_date_count=40,
        effective_sample_size=35.0,
        expectancy_pct=0.5,
        expectancy_ci_low_pct=0.1,
        expectancy_ci_high_pct=0.9,
        hit_rate_pct=60.0,
        profit_factor=2.0,
        median_return_pct=0.4,
        double_cost_expectancy_pct=0.3,
        average_holding_minutes=390.0,
        average_turnover_fraction=2.0,
        worst_trade_pct=-1.0,
        expected_shortfall_5pct=-0.8,
        worst_mae_pct=-1.5,
        max_drawdown_pct=-3.0,
        exposure_time_pct=25.0,
        turnover_annualised=4.0,
        path_model_id="event-time-path-v1",
        largest_entry_date_share_pct=2.5,
        largest_instrument_share_pct=10.0,
        largest_sector_share_pct=30.0,
        bootstrap_model_id="c3-block-bootstrap-v1",
        verdict=verdict,  # type: ignore[arg-type]
        refusals=refusals,  # type: ignore[arg-type]
    )


def _window(start: date, end: date, *, verdict: str = "economically_positive") -> CandidateCohortReport:
    return CandidateCohortReport(
        strategy_id="candidate-1",
        strategy_version="strategy-sha",
        context_version="context-sha",
        outcome_version="outcome-sha",
        cost_model_id="cost-sha",
        window_start=start,
        window_end=end,
        observation_count=40,
        cells=(_cell(verdict),),
    )


def test_stability_requires_two_folds_and_later_terminal_interval() -> None:
    evidence = (
        WindowEvidence("walk_forward", _window(date(2026, 1, 1), date(2026, 1, 31))),
        WindowEvidence("walk_forward", _window(date(2026, 2, 1), date(2026, 2, 28))),
        WindowEvidence("untouched", _window(date(2026, 3, 1), date(2026, 3, 31))),
    )
    assert assess_recent_stability(evidence).stable is True


def test_one_failed_window_blocks_stability_and_remains_named() -> None:
    evidence = (
        WindowEvidence("walk_forward", _window(date(2026, 1, 1), date(2026, 1, 31))),
        WindowEvidence("walk_forward", _window(date(2026, 2, 1), date(2026, 2, 28), verdict="refused")),
        WindowEvidence("prospective", _window(date(2026, 3, 1), date(2026, 3, 31))),
    )
    result = assess_recent_stability(evidence)
    assert result.stable is False
    assert result.refusals == ("cohort_not_economically_positive_in_every_window",)


def test_overlap_and_missing_evidence_are_separate_refusals() -> None:
    evidence = (
        WindowEvidence("walk_forward", _window(date(2026, 1, 1), date(2026, 2, 1))),
        WindowEvidence("walk_forward", _window(date(2026, 2, 1), date(2026, 2, 28))),
    )
    result = assess_recent_stability(evidence)
    assert result.refusals == ("terminal_interval_missing", "window_order_overlaps")


def test_stability_rejects_a_different_strategy_version() -> None:
    first = _window(date(2026, 1, 1), date(2026, 1, 31))
    second = _window(date(2026, 2, 1), date(2026, 2, 28))
    terminal = replace(
        _window(date(2026, 3, 1), date(2026, 3, 31)),
        strategy_version="different-sha",
    )
    result = assess_recent_stability(
        (
            WindowEvidence("walk_forward", first),
            WindowEvidence("walk_forward", second),
            WindowEvidence("untouched", terminal),
        )
    )
    assert result.refusals == ("incompatible_report_identity",)


def test_terminal_interval_must_follow_the_folds() -> None:
    evidence = (
        WindowEvidence("untouched", _window(date(2025, 12, 1), date(2025, 12, 31))),
        WindowEvidence("walk_forward", _window(date(2026, 1, 1), date(2026, 1, 31))),
        WindowEvidence("walk_forward", _window(date(2026, 2, 1), date(2026, 2, 28))),
    )
    result = assess_recent_stability(evidence)
    assert result.refusals == ("terminal_interval_not_later",)
