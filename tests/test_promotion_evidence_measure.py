"""#3104 slice 1 — the ledger measurements behind #2505's promotion evidence."""

from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

import pytest

from app.services.strategy_promotion_evidence_measure import (
    LEDGER_MEASUREMENT_RULE_VERSION,
    RealisedLedger,
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
    measured = measure_ledger(_ledger(returns))
    assert measured.expected_shortfall_5_pct == pytest.approx(Decimal(repr(expected)))


def test_expected_shortfall_is_not_ceil_of_alpha_n() -> None:
    """``ceil`` would average three of 41 legs — 7.32% of the population, not 5%."""
    measured = measure_ledger(_ledger([float(value) for value in range(1, 42)]))
    ceil_estimator = (1.0 + 2.0 + 3.0) / 3.0
    assert measured.expected_shortfall_5_pct != pytest.approx(Decimal(repr(ceil_estimator)))
    # alpha*n = 2.05, k = 2 -> (1 + 2 + 0.05*3) / 2.05
    assert measured.expected_shortfall_5_pct == pytest.approx(Decimal(repr((1.0 + 2.0 + 0.05 * 3.0) / 2.05)))


def test_a_profitable_tail_is_reported_not_clamped() -> None:
    """A positive tail mean is a valid measurement; refusing it is the contract's job."""
    measured = measure_ledger(_ledger([1.0, 2.0, 3.0, 4.0]))
    assert measured.expected_shortfall_5_pct > 0


# --------------------------------------------------------------------------
# Excluding the best 1%
# --------------------------------------------------------------------------


def test_excluding_best_uses_the_percentile_and_reports_what_it_dropped() -> None:
    measured = measure_ledger(_ledger([float(value) for value in range(1, 101)]))
    # numpy's linear 99th percentile of 1..100 is 99.01, so 100 alone is dropped.
    assert measured.excluded_best_count == 1
    assert measured.excluding_best_1_expectancy_pct == pytest.approx(Decimal("50"))


def test_the_trim_drops_nothing_when_the_top_is_tied() -> None:
    """A threshold trim is not a fixed-count trim, which is why the count is reported."""
    measured = measure_ledger(_ledger([5.0] * 50))
    assert measured.excluded_best_count == 0
    assert measured.excluding_best_1_expectancy_pct == pytest.approx(Decimal("5"))


def test_the_two_readings_of_best_1_diverge_above_a_hundred_legs() -> None:
    """576 of 580 stored results carry >=100 trades, so this is the live case."""
    returns = [float(value) for value in range(1, 201)]
    measured = measure_ledger(_ledger(returns))
    assert measured.excluded_best_count == 2
    single_leg_excluded = sum(returns[:-1]) / 199
    assert measured.excluding_best_1_expectancy_pct != pytest.approx(Decimal(repr(single_leg_excluded)))


# --------------------------------------------------------------------------
# Outcome partition and concentration
# --------------------------------------------------------------------------


def test_outcome_counts_partition_the_population() -> None:
    measured = measure_ledger(_ledger([1.0, -1.0, 0.0, 2.0, -0.5]))
    assert (measured.profitable_outcome_count, measured.losing_outcome_count, measured.flat_outcome_count) == (2, 2, 1)
    assert (
        measured.profitable_outcome_count + measured.losing_outcome_count + measured.flat_outcome_count
        == measured.outcome_count
    )


def test_concentration_is_a_count_share_of_the_population() -> None:
    measured = measure_ledger(
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
    measured = measure_ledger(
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
    measured = measure_ledger(
        _ledger(
            [1.0, 1.0],
            entries=[DAY, DAY + timedelta(days=3)],
            exits=[DAY + timedelta(days=3), DAY + timedelta(days=5)],
        )
    )
    assert measured.max_concurrency == 1


def test_overlapping_legs_are_concurrent() -> None:
    measured = measure_ledger(
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
    assert measure_ledger(closed).max_concurrency == 1
    with_open = _ledger(
        [1.0],
        entries=[DAY],
        exits=[DAY + timedelta(days=2)],
        open_legs=((DAY + timedelta(days=1), DAY + timedelta(days=9)),),
    )
    assert measure_ledger(with_open).max_concurrency == 2


def test_an_intraday_leg_was_still_held() -> None:
    """``bars_held = 0`` is legal — a tp/sl can be touched on the fill bar itself."""
    measured = measure_ledger(_ledger([1.0], entries=[DAY], exits=[DAY]))
    assert measured.max_concurrency == 1


def test_an_all_intraday_population_reports_its_real_peak() -> None:
    """Erasing these legs would report 0, which ``PromotionEvidence`` rejects outright."""
    measured = measure_ledger(_ledger([1.0, 2.0, 3.0], entries=[DAY] * 3, exits=[DAY] * 3))
    assert measured.max_concurrency == 3


def test_an_intraday_close_does_not_make_the_next_days_entry_concurrent() -> None:
    """The two ordering rules must not swallow each other."""
    measured = measure_ledger(
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
    assert measure_ledger(_ledger([1.0])).rule_version == LEDGER_MEASUREMENT_RULE_VERSION
