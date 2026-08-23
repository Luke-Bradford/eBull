"""S-H arm 1's frozen pass leg, table-tested with no database. Refs #2840.

The contract (`sh-volatile-regime-gate-2026-08-22`, §"Readout and abort bar") says
a pass needs `bear_volatile` positive in BOTH quarantine arms, and that a result
carried by `bull_volatile`-on-`masked` alone is a FAIL. These are the cases that
distinguish the frozen bar from the bars it would be easy to slide into — a
best-arm read, an any-regime read, and a missing cohort silently skipped.

⚠ Pure logic only. The database half of `scripts/measure_2840_sh_regime_gate.py`
is the #2599 gate and two SELECTs; exercising it needs the sealed outcomes it
exists to guard, which is the look the preregistration precedes.
"""

from __future__ import annotations

import pytest

from scripts.measure_2840_sh_regime_gate import Cell, cell_verdict, turnover_check


def _cell(regime: str, arm: str, expectancy: float | None, *, trades: int = 500, dates: int = 14) -> Cell:
    return Cell(
        regime=regime,
        quarantine_arm=arm,
        trade_count=trades,
        decision_date_count=dates,
        instrument_count=trades - 20,
        expectancy_pct=expectancy,
        profit_factor=None if expectancy is None else 1.0 + expectancy,
        expectancy_ci_low_pct=None if expectancy is None else expectancy - 0.5,
        expectancy_ci_high_pct=None if expectancy is None else expectancy + 0.5,
    )


def _cells(
    bear_masked: float | None,
    bear_admitted: float | None,
    bull_masked: float = -0.1,
) -> dict[tuple[str, str], Cell]:
    return {
        ("bear_volatile", "masked"): _cell("bear_volatile", "masked", bear_masked),
        ("bear_volatile", "admitted"): _cell("bear_volatile", "admitted", bear_admitted),
        ("bull_volatile", "masked"): _cell("bull_volatile", "masked", bull_masked, dates=18),
    }


def test_both_bear_arms_positive_is_the_only_pass() -> None:
    verdict = cell_verdict(_cells(0.12, 0.04))
    assert verdict.passed
    assert verdict.reasons == ()
    assert not verdict.carried_by_bull_volatile_masked_alone


@pytest.mark.parametrize(
    ("bear_masked", "bear_admitted"),
    [(0.12, -0.03), (-0.03, 0.12), (-0.03, -0.03)],
)
def test_one_negative_bear_arm_fails_however_strong_the_other_is(bear_masked: float, bear_admitted: float) -> None:
    """⚠ The arms are conjunctive. Reading the better arm is the defect the
    quarantine sensitivity exists to expose, not a tie-break."""

    verdict = cell_verdict(_cells(bear_masked, bear_admitted))
    assert not verdict.passed
    assert verdict.reasons


def test_exactly_zero_expectancy_is_not_positive() -> None:
    """`> 0`, not `>= 0`: a strictly break-even gate has no edge to promote."""

    verdict = cell_verdict(_cells(0.0, 0.5))
    assert not verdict.passed
    assert any("bear_volatile/masked" in reason for reason in verdict.reasons)


def test_a_strong_bull_volatile_cannot_carry_a_failing_bear_volatile() -> None:
    verdict = cell_verdict(_cells(-0.20, -0.20, bull_masked=3.0))
    assert not verdict.passed
    assert verdict.carried_by_bull_volatile_masked_alone


def test_bull_volatile_flag_is_off_when_the_deciding_regime_passes() -> None:
    """The flag names a FAILURE shape; on a pass it must not fire at all."""

    verdict = cell_verdict(_cells(0.2, 0.2, bull_masked=3.0))
    assert verdict.passed
    assert not verdict.carried_by_bull_volatile_masked_alone


def test_an_absent_bear_cohort_fails_rather_than_skipping() -> None:
    """A cohort that never traded and a cohort that traded and lost are different
    states; collapsing them lets an empty run read as anything at all."""

    verdict = cell_verdict({("bear_volatile", "masked"): _cell("bear_volatile", "masked", 0.5)})
    assert not verdict.passed
    assert any("cohort absent" in reason for reason in verdict.reasons)


def test_a_stored_null_expectancy_fails_and_says_so_distinctly() -> None:
    verdict = cell_verdict(_cells(None, 0.5))
    assert not verdict.passed
    assert any("expectancy not stored" in reason for reason in verdict.reasons)


def test_empty_cells_fail_on_both_arms() -> None:
    verdict = cell_verdict({})
    assert not verdict.passed
    assert len(verdict.reasons) == 2


def test_the_gate_must_cut_trade_count_to_hold_its_mechanism() -> None:
    assert turnover_check(300, 1000).mechanism_holds
    assert "0.300x" in turnover_check(300, 1000).note


def test_an_uncut_trade_count_fails_the_mechanism_even_though_expectancy_is_silent() -> None:
    """§"Readout and abort bar": *"reported as a fail of the stated mechanism even
    if expectancy improves"*. `turnover_check` never reads an expectancy."""

    assert not turnover_check(1000, 1000).mechanism_holds
    assert not turnover_check(1200, 1000).mechanism_holds


def test_a_control_that_never_traded_leaves_the_ratio_undefined_rather_than_dividing() -> None:
    check = turnover_check(0, 0)
    assert not check.mechanism_holds
    assert "undefined" in check.note
