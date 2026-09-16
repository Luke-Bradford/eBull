"""#3104 slice 1 — the ledger measurements behind #2505's promotion evidence."""

from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

import pytest

from app.services.strategy_promotion_evidence_measure import (
    LEDGER_MEASUREMENT_RULE_VERSION,
    RECENT_YEAR_HORIZON,
    LedgerMeasurements,
    RealisedLedger,
    RecentYearMeasurement,
    measure_ledger,
)

DAY = date(2024, 1, 1)


def _ledger(
    returns: list[float],
    *,
    entries: list[date] | None = None,
    exits: list[date] | None = None,
    names: list[int] | None = None,
    open_legs: tuple[tuple[date, date], ...] = (),
) -> RealisedLedger:
    """A ledger whose non-return columns are distinct unless a test cares."""
    count = len(returns)
    return RealisedLedger(
        net_return_pct=tuple(returns),
        entry_fill_date=tuple(entries or [DAY + timedelta(days=index) for index in range(count)]),
        exit_bar_date=tuple(exits or [DAY + timedelta(days=index + 1) for index in range(count)]),
        name_key=tuple(names or list(range(count))),
        open_legs=open_legs,
    )


#: The run root seed ``_measure_namespace`` passes (``BACKTEST_BOOTSTRAP_SEED``).
SEED = 20260808
#: ``ResultIdentity.window_end`` year, so ``DAY``'s legs sit inside the horizon.
ANCHOR = 2024


def _measure(ledger: RealisedLedger) -> LedgerMeasurements:
    """Slice 1's call shape, with slice 4's two required keywords supplied."""
    return measure_ledger(ledger, root_seed=SEED, anchor_year=ANCHOR)


# --------------------------------------------------------------------------
# Expected shortfall — Acerbi & Tasche's exact alpha-tail average
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("returns", "expected"),
    [
        # n=10, alpha*n=0.5, k=0 -> the single worst, weighted to one.
        pytest.param([float(value) for value in range(1, 11)], 1.0, id="k-zero-is-the-worst-leg"),
        # n=40, alpha*n=2.0, k=2 -> the exact mean of the two worst.
        pytest.param([float(value) for value in range(1, 41)], 1.5, id="integral-alpha-n"),
        # n=30, alpha*n=1.5, k=1 -> (x1 + 0.5*x2) / 1.5.
        pytest.param([float(value) for value in range(1, 31)], (1.0 + 0.5 * 2.0) / 1.5, id="fractional-boundary"),
    ],
)
def test_expected_shortfall_follows_the_published_estimator(returns: list[float], expected: float) -> None:
    measured = _measure(_ledger(returns))
    assert measured.expected_shortfall_5_pct == pytest.approx(Decimal(repr(expected)))


def test_expected_shortfall_is_not_ceil_of_alpha_n() -> None:
    """``ceil`` would average three of 41 legs — 7.32% of the population, not 5%."""
    measured = _measure(_ledger([float(value) for value in range(1, 42)]))
    ceil_estimator = (1.0 + 2.0 + 3.0) / 3.0
    assert measured.expected_shortfall_5_pct != pytest.approx(Decimal(repr(ceil_estimator)))
    # alpha*n = 2.05, k = 2 -> (1 + 2 + 0.05*3) / 2.05
    assert measured.expected_shortfall_5_pct == pytest.approx(Decimal(repr((1.0 + 2.0 + 0.05 * 3.0) / 2.05)))


def test_a_profitable_tail_is_reported_not_clamped() -> None:
    """A positive tail mean is a valid measurement; refusing it is the contract's job."""
    measured = _measure(_ledger([1.0, 2.0, 3.0, 4.0]))
    assert measured.expected_shortfall_5_pct > 0


# --------------------------------------------------------------------------
# Excluding the best 1%
# --------------------------------------------------------------------------


def test_excluding_best_uses_the_percentile_and_reports_what_it_dropped() -> None:
    measured = _measure(_ledger([float(value) for value in range(1, 101)]))
    # numpy's linear 99th percentile of 1..100 is 99.01, so 100 alone is dropped.
    assert measured.excluded_best_count == 1
    assert measured.excluding_best_1_expectancy_pct == pytest.approx(Decimal("50"))


def test_the_trim_drops_nothing_when_the_top_is_tied() -> None:
    """A threshold trim is not a fixed-count trim, which is why the count is reported."""
    measured = _measure(_ledger([5.0] * 50))
    assert measured.excluded_best_count == 0
    assert measured.excluding_best_1_expectancy_pct == pytest.approx(Decimal("5"))


def test_the_two_readings_of_best_1_diverge_above_a_hundred_legs() -> None:
    """576 of 580 stored results carry >=100 trades, so this is the live case."""
    returns = [float(value) for value in range(1, 201)]
    measured = _measure(_ledger(returns))
    assert measured.excluded_best_count == 2
    single_leg_excluded = sum(returns[:-1]) / 199
    assert measured.excluding_best_1_expectancy_pct != pytest.approx(Decimal(repr(single_leg_excluded)))


# --------------------------------------------------------------------------
# Outcome partition and concentration
# --------------------------------------------------------------------------


def test_outcome_counts_partition_the_population() -> None:
    measured = _measure(_ledger([1.0, -1.0, 0.0, 2.0, -0.5]))
    assert (measured.profitable_outcome_count, measured.losing_outcome_count, measured.flat_outcome_count) == (2, 2, 1)
    assert (
        measured.profitable_outcome_count + measured.losing_outcome_count + measured.flat_outcome_count
        == measured.outcome_count
    )


def test_concentration_is_a_count_share_of_the_population() -> None:
    measured = _measure(
        _ledger(
            [1.0, 2.0, 3.0, 4.0],
            entries=[DAY, DAY, DAY, DAY + timedelta(days=1)],
            exits=[DAY + timedelta(days=5)] * 4,
            names=[7, 7, 9, 9],
        )
    )
    assert measured.max_date_contribution_pct == pytest.approx(Decimal("75"))
    assert measured.max_name_contribution_pct == pytest.approx(Decimal("50"))


def test_a_profit_weighted_share_would_read_differently() -> None:
    """The contract's worked example is a TRADE-COUNT share, so the big winner must not dominate."""
    measured = _measure(
        _ledger(
            [1.0, 1.0, 1.0, 900.0],
            entries=[DAY, DAY, DAY, DAY + timedelta(days=1)],
            exits=[DAY + timedelta(days=5)] * 4,
        )
    )
    assert measured.max_date_contribution_pct == pytest.approx(Decimal("75"))


# --------------------------------------------------------------------------
# Concurrency
# --------------------------------------------------------------------------


def test_concurrency_intervals_are_half_open() -> None:
    """Spec §3.5 rule 4 — same-bar ordering is exit before entry."""
    measured = _measure(
        _ledger(
            [1.0, 1.0],
            entries=[DAY, DAY + timedelta(days=3)],
            exits=[DAY + timedelta(days=3), DAY + timedelta(days=5)],
        )
    )
    assert measured.max_concurrency == 1


def test_overlapping_legs_are_concurrent() -> None:
    measured = _measure(
        _ledger(
            [1.0, 1.0, 1.0],
            entries=[DAY, DAY + timedelta(days=1), DAY + timedelta(days=2)],
            exits=[DAY + timedelta(days=9)] * 3,
        )
    )
    assert measured.max_concurrency == 3


def test_open_legs_count_toward_concurrency() -> None:
    """They are in exposure and on the curve; omitting them understates the peak."""
    closed = _ledger([1.0], entries=[DAY], exits=[DAY + timedelta(days=2)])
    assert _measure(closed).max_concurrency == 1
    with_open = _ledger(
        [1.0],
        entries=[DAY],
        exits=[DAY + timedelta(days=2)],
        open_legs=((DAY + timedelta(days=1), DAY + timedelta(days=9)),),
    )
    assert _measure(with_open).max_concurrency == 2


def test_an_intraday_leg_was_still_held() -> None:
    """``bars_held = 0`` is legal — a tp/sl can be touched on the fill bar itself."""
    measured = _measure(_ledger([1.0], entries=[DAY], exits=[DAY]))
    assert measured.max_concurrency == 1


def test_an_all_intraday_population_reports_its_real_peak() -> None:
    """Erasing these legs would report 0, which ``PromotionEvidence`` rejects outright."""
    measured = _measure(_ledger([1.0, 2.0, 3.0], entries=[DAY] * 3, exits=[DAY] * 3))
    assert measured.max_concurrency == 3


def test_an_intraday_close_does_not_make_the_next_days_entry_concurrent() -> None:
    """The two ordering rules must not swallow each other."""
    measured = _measure(
        _ledger(
            [1.0, 1.0],
            entries=[DAY, DAY + timedelta(days=1)],
            exits=[DAY, DAY + timedelta(days=1)],
        )
    )
    assert measured.max_concurrency == 1


# --------------------------------------------------------------------------
# Input invariants
# --------------------------------------------------------------------------


def test_an_empty_ledger_is_refused_rather_than_zeroed() -> None:
    with pytest.raises(ValueError, match="no legs carries no measurement"):
        RealisedLedger(net_return_pct=(), entry_fill_date=(), exit_bar_date=(), name_key=())


def test_mismatched_columns_are_refused() -> None:
    with pytest.raises(ValueError, match="positionally parallel"):
        RealisedLedger(
            net_return_pct=(1.0, 2.0),
            entry_fill_date=(DAY,),
            exit_bar_date=(DAY, DAY),
            name_key=(1, 2),
        )


def test_a_non_finite_return_is_refused() -> None:
    with pytest.raises(ValueError, match="must be finite"):
        _ledger([1.0, float("nan")])


def test_a_leg_closing_before_it_opens_is_refused() -> None:
    with pytest.raises(ValueError, match="cannot close"):
        _ledger([1.0], entries=[DAY + timedelta(days=2)], exits=[DAY])


def test_the_rule_version_is_carried_on_the_measurement() -> None:
    assert _measure(_ledger([1.0])).rule_version == LEDGER_MEASUREMENT_RULE_VERSION


# --------------------------------------------------------------------------
# Slice 4 — the recent-year partition
# --------------------------------------------------------------------------


def _spread(year: int, count: int, *, start_return: float = 1.0) -> tuple[list[float], list[date]]:
    """``count`` legs on distinct dates inside ``year``, returns ascending."""
    return (
        [start_return + index for index in range(count)],
        [date(year, 1, 1) + timedelta(days=index * 3) for index in range(count)],
    )


def _multi_year(counts: dict[int, int]) -> RealisedLedger:
    returns: list[float] = []
    entries: list[date] = []
    for year, count in sorted(counts.items()):
        year_returns, year_entries = _spread(year, count)
        returns.extend(year_returns)
        entries.extend(year_entries)
    return _ledger(
        returns,
        entries=entries,
        exits=[when + timedelta(days=1) for when in entries],
        names=list(range(len(returns))),
    )


def test_the_horizon_is_five_trailing_years_anchored_on_the_result_window() -> None:
    """The cap is the contract's own (``len(years) > 5`` refuses), not a choice."""
    ledger = _multi_year({year: 40 for year in range(2018, 2025)})
    measured = measure_ledger(ledger, root_seed=SEED, anchor_year=2024)

    assert [item.year for item in measured.recent_years] == [2020, 2021, 2022, 2023, 2024]
    assert len(measured.recent_years) == RECENT_YEAR_HORIZON
    # 2018 and 2019 are outside the horizon and contribute no entry, so the
    # partition is a strict subset of the parent population.
    assert sum(item.observation_count for item in measured.recent_years) < measured.outcome_count


def test_the_anchor_moves_the_horizon_and_is_not_the_last_traded_date() -> None:
    """The in-sample case: legs stop years before the result's own window end."""
    ledger = _multi_year({2019: 30, 2020: 30})

    assert [item.year for item in measure_ledger(ledger, root_seed=SEED, anchor_year=2021).recent_years] == [
        2019,
        2020,
    ]
    # Anchored on the identity window end, every leg falls outside the horizon.
    assert measure_ledger(ledger, root_seed=SEED, anchor_year=2026).recent_years == ()


def test_a_horizon_year_with_no_realised_leg_is_absent_not_zeroed() -> None:
    measured = measure_ledger(_multi_year({2022: 30, 2024: 30}), root_seed=SEED, anchor_year=2024)

    assert [item.year for item in measured.recent_years] == [2022, 2024]
    # ⚠ Absent, and the gap is NOT filled — ``RecentYearEvidence`` refuses an
    # ``observation_count`` of zero, and an invented zero is the failure mode
    # the contract's "missing is not zero" rule names.
    assert all(item.observation_count > 0 for item in measured.recent_years)


def test_a_year_is_attributed_by_its_entry_and_not_its_exit() -> None:
    """A leg entered in 2023 and closed in 2025 belongs wholly to 2023."""
    entries = [date(2023, 12, 20) + timedelta(days=index) for index in range(4)]
    ledger = _ledger(
        [1.0, 2.0, 3.0, 4.0],
        entries=entries,
        exits=[date(2025, 6, 1)] * 4,
        names=[1, 2, 3, 4],
    )
    measured = measure_ledger(ledger, root_seed=SEED, anchor_year=2025)

    assert [(item.year, item.observation_count) for item in measured.recent_years] == [(2023, 4)]


def test_the_open_leg_census_is_carried_per_year() -> None:
    """Censoring concentrates in the recent years; it must not be invisible."""
    ledger = _ledger(
        [1.0, 2.0],
        entries=[date(2024, 3, 1), date(2024, 4, 1)],
        exits=[date(2024, 5, 1), date(2024, 6, 1)],
        names=[1, 2],
        open_legs=((date(2024, 7, 1), date(2024, 12, 31)), (date(2024, 8, 1), date(2024, 12, 31))),
    )
    measured = measure_ledger(ledger, root_seed=SEED, anchor_year=2024)

    (year,) = measured.recent_years
    assert (year.observation_count, year.open_leg_count) == (2, 2)


def test_a_year_whose_bootstrap_cannot_run_is_kept_with_nulls() -> None:
    """104 of 720 stored regime cohorts are this state — dropping it hides a year."""
    ledger = _ledger(
        [-5.0, -6.0, -7.0],
        entries=[date(2024, 2, 1)] * 3,
        exits=[date(2024, 3, 1)] * 3,
        names=[1, 2, 3],
    )
    measured = measure_ledger(ledger, root_seed=SEED, anchor_year=2024)

    (year,) = measured.recent_years
    # One cluster date, so ``block_bootstrap_expectancy`` returns None.
    assert year.expectancy_ci_low_pct is None
    assert year.bootstrap_seed is None
    # ⚠ The year survives, and its adverse expectancy with it.
    assert year.observation_count == 3
    assert year.after_cost_expectancy_pct == Decimal(repr(-6.0))


def test_a_measurable_year_carries_the_whole_bootstrap_provenance() -> None:
    measured = measure_ledger(_multi_year({2024: 120}), root_seed=SEED, anchor_year=2024)

    (year,) = measured.recent_years
    assert year.expectancy_ci_low_pct is not None
    assert year.expectancy_ci_high_pct is not None
    assert year.expectancy_ci_low_pct <= year.expectancy_ci_high_pct
    assert year.effective_sample_size is not None and year.effective_sample_size > 0
    assert year.bootstrap_block_length is not None and year.bootstrap_block_length >= 1
    assert year.bootstrap_cluster_count == 120
    assert year.bootstrap_design_effect is not None and year.bootstrap_design_effect > 0


def test_each_year_gets_its_own_derived_seed() -> None:
    """``sha256(f"{root}:{label}")[:4]`` — the repo's existing derivation."""
    measured = measure_ledger(_multi_year({2023: 120, 2024: 120}), root_seed=SEED, anchor_year=2024)

    seeds = [item.bootstrap_seed for item in measured.recent_years]
    assert all(seed is not None for seed in seeds)
    assert len(set(seeds)) == 2


def test_the_partition_cannot_exceed_the_population_it_partitions() -> None:
    with pytest.raises(ValueError, match="cannot exceed the population"):
        LedgerMeasurements(
            rule_version=LEDGER_MEASUREMENT_RULE_VERSION,
            outcome_count=1,
            profitable_outcome_count=1,
            losing_outcome_count=0,
            flat_outcome_count=0,
            expected_shortfall_5_pct=Decimal("-1"),
            excluding_best_1_expectancy_pct=Decimal("1"),
            excluded_best_count=0,
            max_date_contribution_pct=Decimal("100"),
            max_name_contribution_pct=Decimal("100"),
            max_concurrency=1,
            recent_years=(_year(2024, observation_count=2),),
        )


def _year(year: int, *, observation_count: int = 1) -> RecentYearMeasurement:
    return RecentYearMeasurement(
        year=year,
        observation_count=observation_count,
        open_leg_count=0,
        after_cost_expectancy_pct=Decimal("1"),
        expected_shortfall_5_pct=Decimal("-1"),
        max_date_contribution_pct=Decimal("100"),
        max_name_contribution_pct=Decimal("100"),
        expectancy_ci_low_pct=None,
        expectancy_ci_high_pct=None,
        effective_sample_size=None,
        bootstrap_seed=None,
        bootstrap_block_length=None,
        bootstrap_cluster_count=None,
        bootstrap_design_effect=None,
    )


def test_recent_years_must_be_unique_and_ascending() -> None:
    with pytest.raises(ValueError, match="unique and ascending"):
        LedgerMeasurements(
            rule_version=LEDGER_MEASUREMENT_RULE_VERSION,
            outcome_count=4,
            profitable_outcome_count=4,
            losing_outcome_count=0,
            flat_outcome_count=0,
            expected_shortfall_5_pct=Decimal("-1"),
            excluding_best_1_expectancy_pct=Decimal("1"),
            excluded_best_count=0,
            max_date_contribution_pct=Decimal("100"),
            max_name_contribution_pct=Decimal("100"),
            max_concurrency=1,
            recent_years=(_year(2024), _year(2023)),
        )


def test_a_partial_bootstrap_provenance_is_refused() -> None:
    """All-or-none: a half-recorded interval cannot be re-run."""
    with pytest.raises(ValueError, match="bootstrap fields"):
        RecentYearMeasurement(
            year=2024,
            observation_count=10,
            open_leg_count=0,
            after_cost_expectancy_pct=Decimal("1"),
            expected_shortfall_5_pct=Decimal("-1"),
            max_date_contribution_pct=Decimal("50"),
            max_name_contribution_pct=Decimal("50"),
            expectancy_ci_low_pct=Decimal("0.5"),
            expectancy_ci_high_pct=Decimal("1.5"),
            effective_sample_size=9.0,
            bootstrap_seed=7,
            bootstrap_block_length=None,
            bootstrap_cluster_count=None,
            bootstrap_design_effect=None,
        )


def test_an_interval_above_its_own_point_estimate_is_reported_not_repaired() -> None:
    """Efron & Tibshirani ch. 13 — a percentile interval need not contain it.

    ``RecentYearEvidence`` is the class that refuses the pair. Refusing it HERE
    would abort a backtest run over a measurement that is correct.
    """
    year = RecentYearMeasurement(
        year=2024,
        observation_count=10,
        open_leg_count=0,
        after_cost_expectancy_pct=Decimal("1"),
        expected_shortfall_5_pct=Decimal("-1"),
        max_date_contribution_pct=Decimal("50"),
        max_name_contribution_pct=Decimal("50"),
        expectancy_ci_low_pct=Decimal("2"),
        expectancy_ci_high_pct=Decimal("3"),
        effective_sample_size=9.0,
        bootstrap_seed=7,
        bootstrap_block_length=2,
        bootstrap_cluster_count=5,
        bootstrap_design_effect=1.1,
    )
    assert year.expectancy_ci_low_pct is not None
    assert year.expectancy_ci_low_pct > year.after_cost_expectancy_pct
