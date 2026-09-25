"""#2901 PR B part 1: identity gate, statistics and verdict of the quality arm's monthly trial."""

from __future__ import annotations

import dataclasses
import math
import zipfile
from pathlib import Path

import pytest

from app.services.deflated_sharpe import DeflatedSharpeResult
from app.services.r6_exclusion_trial import CLASSIFIED_BEST, CLASSIFIED_WORST, ZERO_RECOVERY
from app.services.r6_monthly_trial import (
    MONTHLY_DSR_MODEL_ID,
    REFERENCE_MEMBER,
    STATISTIC_MONTHS,
    CohortTest,
    GateReadout,
    GateRefusal,
    HacEstimate,
    PolicyStatistics,
    TotalReturns,
    Verdict,
    cohort_active_returns,
    cohort_t,
    cohort_t_bar,
    decide_verdict,
    declared_trials,
    gate_passed,
    hac_t,
    haircut_pass,
    identity_gate,
    monthly_dsr,
    newey_west_lag,
    read_global_q_gpa,
    student_t_cdf,
    student_t_quantile,
    summarise_policy,
)


def test_statistic_window_is_134_full_months() -> None:
    assert len(STATISTIC_MONTHS) == 134
    assert (STATISTIC_MONTHS[0], STATISTIC_MONTHS[-1]) == ((2013, 7), (2024, 8))


# --- HAC ---------------------------------------------------------------------------------------------------


@pytest.mark.parametrize(("observations", "lag"), [(134, 4), (100, 4), (50, 3), (4, 1)])
def test_newey_west_lag_is_nw1994_rule_of_thumb(observations: int, lag: int) -> None:
    assert newey_west_lag(observations) == lag


def test_hac_t_hand_computed_negative_autocorrelation() -> None:
    # c = ±1, γ0 = 1, γ1 = −3/4, L = 1: Ω = 1 + 2·½·(−3/4) = 1/4; SE = √(Ω/4) = 1/4; T_eff = 16 capped at 4.
    estimate = hac_t([2.0, 0.0, 2.0, 0.0])
    assert estimate is not None
    assert (estimate.lag, estimate.variance, estimate.long_run_variance) == (1, 1.0, 0.25)
    assert estimate.t_stat == pytest.approx(4.0)
    assert estimate.effective_size == 4.0


def test_hac_t_hand_computed_positive_autocorrelation() -> None:
    # c = (2, 2, −2, −2): γ0 = 4, γ1 = 1, Ω = 5; T_eff = 4·4/5 = 3.2.
    estimate = hac_t([3.0, 3.0, -1.0, -1.0])
    assert estimate is not None
    assert estimate.long_run_variance == pytest.approx(5.0)
    assert estimate.t_stat == pytest.approx(1.0 / math.sqrt(5.0 / 4.0))
    assert estimate.effective_size == pytest.approx(3.2)


def test_hac_t_refuses_a_constant_series() -> None:
    assert hac_t([0.01] * 134) is None


# --- Student t / cohort t ----------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("probability", "df", "published"),
    [(0.975, 10, 2.228), (0.995, 10, 3.169), (0.975, 1, 12.706), (0.975, 2, 4.303), (0.975, 30, 2.042)],
)
def test_student_t_quantile_matches_published_tables(probability: float, df: int, published: float) -> None:
    assert student_t_quantile(probability, df) == pytest.approx(published, abs=5e-4)
    assert student_t_quantile(1.0 - probability, df) == pytest.approx(-published, abs=5e-4)


def test_student_t_cdf_closed_forms() -> None:
    assert student_t_cdf(0.0, 7) == 0.5
    assert student_t_cdf(1.0, 1) == pytest.approx(0.75)  # Cauchy
    assert student_t_cdf(1.5, 2) == pytest.approx(0.5 + 1.5 / (2 * math.sqrt(2 + 1.5**2)))


def test_cohort_bar_matches_the_z3_tail() -> None:
    bar = cohort_t_bar(10)
    assert bar > 3.169
    assert 2 * (1 - student_t_cdf(bar, 10)) == pytest.approx(2 * (1 - 0.9986501019683699), abs=1e-12)


def test_cohort_active_returns_keeps_only_complete_holding_years() -> None:
    arm = dict.fromkeys(STATISTIC_MONTHS, 0.01)
    control = dict.fromkeys(STATISTIC_MONTHS, 0.0)
    cohorts = cohort_active_returns(arm, control)
    assert list(cohorts) == list(range(2013, 2024))
    assert cohorts[2013] == pytest.approx(1.01**12 - 1.0)


def test_cohort_t_hand_computed() -> None:
    result = cohort_t({2013: 0.01, 2014: 0.03, 2015: 0.02})
    assert result is not None
    # mean 0.02, sd 0.01 → t = 0.02 / (0.01/√3) = 2√3, against t₂.
    assert (result.cohorts, result.df) == (3, 2)
    assert result.t_stat == pytest.approx(2 * math.sqrt(3))
    assert result.passed is (result.t_stat > cohort_t_bar(2))


def test_cohort_t_refuses_zero_spread() -> None:
    assert cohort_t(dict.fromkeys(range(2013, 2024), 0.02)) is None


# --- DSR ---------------------------------------------------------------------------------------------------


def _wiggle(level: float, amplitude: float) -> list[float]:
    return [level + amplitude * math.sin(1.7 * i) for i in range(134)]


def test_monthly_dsr_uses_the_floor_when_the_measured_pair_agrees() -> None:
    result = monthly_dsr(
        _wiggle(0.004, 0.02),
        effective_size=120.0,
        same_window_sharpes=(0.2, 0.2),
        declared_trials=declared_trials(0),
        trial_register_version="t",
    )
    assert result is not None
    assert result.trial_sharpe_variance == pytest.approx(1.0 / 119.0)
    assert (result.declared_trials, result.independent_trials, result.average_trial_correlation) == (5, 5.0, 0.0)
    assert result.model_id == MONTHLY_DSR_MODEL_ID


def test_monthly_dsr_refuses_rather_than_passing() -> None:
    kwargs = {"declared_trials": 5, "trial_register_version": "t"}
    assert monthly_dsr(_wiggle(0.004, 0.02), effective_size=120.0, same_window_sharpes=(0.2,), **kwargs) is None
    assert monthly_dsr(_wiggle(0.004, 0.02), effective_size=1.0, same_window_sharpes=(0.2, 0.3), **kwargs) is None
    assert monthly_dsr([0.01] * 134, effective_size=120.0, same_window_sharpes=(0.2, 0.3), **kwargs) is None


def test_declared_trials_counts_corrections() -> None:
    assert declared_trials(1) == 6
    with pytest.raises(ValueError):
        declared_trials(-1)


# --- Haircut -----------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("arm_gross", "arm_net", "control_gross", "control_net", "haircut", "expected"),
    [
        # edge 0.40 → 0.168 at d = 0.58; adjusted = 1.0 + 0.168 − 0.10 = 1.068 > control net 0.95.
        (1.40, 1.30, 1.00, 0.95, 0.58, True),
        # edge 0.10 → 0.042; adjusted = 1.0 + 0.042 − 0.10 = 0.942 < 0.95.
        (1.10, 1.00, 1.00, 0.95, 0.58, False),
        # same edge at d = 0.15: 1.0 + 0.085 − 0.10 = 0.985 > 0.95.
        (1.10, 1.00, 1.00, 0.95, 0.15, True),
        # negative edge carried in full; adjusted must also be > 0.
        (-0.20, -0.25, -0.10, -0.12, 0.15, False),
    ],
)
def test_haircut_pass(
    arm_gross: float, arm_net: float, control_gross: float, control_net: float, haircut: float, expected: bool
) -> None:
    assert (
        haircut_pass(
            arm_gross=arm_gross, arm_net=arm_net, control_gross=control_gross, control_net=control_net, haircut=haircut
        )
        is expected
    )


# --- Identity gate -----------------------------------------------------------------------------------------


def _reference_zip(tmp_path: Path, rows: list[str], header: str = "year,month,rank_GPA,nstocks,ret_vw") -> Path:
    path = tmp_path / "prof.zip"
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr(REFERENCE_MEMBER, "\n".join([header, *rows]) + "\n")
    return path


def test_read_global_q_gpa_spread_in_fractions(tmp_path: Path) -> None:
    path = _reference_zip(tmp_path, ["2013,7,1,300,2.0", "2013,7,5,300,9.0", "2013,7,10,300,3.5"])
    assert read_global_q_gpa(path) == {(2013, 7): pytest.approx(0.015)}


@pytest.mark.parametrize(
    ("rows", "header"),
    [
        (["2013,7,1,300,2.0"], "year,month,rank_GP,nstocks,ret_vw"),
        (["2013,7,1,300,2.0", "2013,7,1,300,2.1"], "year,month,rank_GPA,nstocks,ret_vw"),
        (["2013,7,1,300,150.0", "2013,7,10,300,1.0"], "year,month,rank_GPA,nstocks,ret_vw"),
        (["2013,7,1,300,nan", "2013,7,10,300,1.0"], "year,month,rank_GPA,nstocks,ret_vw"),
    ],
    ids=["header", "duplicate-rank", "percent-confusion", "non-finite"],
)
def test_read_global_q_gpa_refusals(tmp_path: Path, rows: list[str], header: str) -> None:
    with pytest.raises(GateRefusal):
        read_global_q_gpa(_reference_zip(tmp_path, rows, header))


def _aligned_pair() -> tuple[dict[tuple[int, int], float], dict[tuple[int, int], float]]:
    reference = {month: 0.02 * math.sin(1.3 * i) for i, month in enumerate(STATISTIC_MONTHS)}
    ours = {month: 0.8 * value + 0.002 * math.cos(2.9 * i) for i, (month, value) in enumerate(reference.items())}
    reference[(2013, 6)] = 0.5  # outside the window: ignored, not an extra key
    return ours, reference


def test_identity_gate_passes_an_aligned_correlated_spread_and_publishes_no_alpha() -> None:
    ours, reference = _aligned_pair()
    readout = identity_gate(ours, reference)
    assert readout.months == 134
    assert readout.passed
    assert not hasattr(readout, "alpha")


def test_identity_gate_refuses_a_missing_month_on_either_side() -> None:
    ours, reference = _aligned_pair()
    with pytest.raises(GateRefusal):
        identity_gate({k: v for k, v in ours.items() if k != (2020, 3)}, reference)
    with pytest.raises(GateRefusal):
        identity_gate(ours, {k: v for k, v in reference.items() if k != (2020, 3)})


def test_identity_gate_refuses_an_extra_month_in_ours() -> None:
    ours, reference = _aligned_pair()
    with pytest.raises(GateRefusal):
        identity_gate({**ours, (2024, 9): 0.0}, reference)


def _readout(passed: bool) -> GateReadout:
    return GateReadout(134, 0.5, 0.8, 0.1, 0.1, passed)


def test_gate_is_a_conjunct_over_policies() -> None:
    assert gate_passed({ZERO_RECOVERY.label: _readout(True), CLASSIFIED_BEST.label: _readout(True)})
    assert not gate_passed({ZERO_RECOVERY.label: _readout(True), CLASSIFIED_BEST.label: _readout(False)})
    with pytest.raises(ValueError):
        gate_passed({CLASSIFIED_BEST.label: _readout(True)})


# --- Summary and verdict -----------------------------------------------------------------------------------


def test_summarise_policy_runs_every_statistic_on_synthetic_series() -> None:
    control = {month: 0.01 * math.sin(0.9 * i) for i, month in enumerate(STATISTIC_MONTHS)}
    arm = {month: value + 0.004 + 0.01 * math.sin(2.3 * i) for i, (month, value) in enumerate(control.items())}
    complete_case = {month: value + 0.001 for month, value in control.items()}
    totals = TotalReturns(1.2, 1.1, 0.8, 0.75, 0.85, 0.8)
    result = summarise_policy(
        arm=arm, control=control, complete_case=complete_case, totals=totals, corrections=0, trial_register_version="t"
    )
    assert result.hac is not None and result.hac.lag == 4
    assert result.cohort is not None and result.cohort.cohorts == 11
    assert result.dsr is not None and result.dsr.declared_trials == 5
    assert result.mean_active == pytest.approx(0.004, abs=1e-3)
    assert set(result.haircut_pass) == {0.15, 0.58}


def test_summarise_policy_refuses_a_series_with_the_partial_month() -> None:
    series = dict.fromkeys(STATISTIC_MONTHS, 0.0)
    with pytest.raises(ValueError):
        summarise_policy(
            arm={**series, (2024, 9): 0.0},
            control=series,
            complete_case=series,
            totals=TotalReturns(0, 0, 0, 0, 0, 0),
            corrections=0,
            trial_register_version="t",
        )


def _hac(t_stat: float) -> HacEstimate:
    return HacEstimate(134, 4, 0.01, 1.0, 1.0, 0.01, t_stat, 120.0)


def _dsr(value: float) -> DeflatedSharpeResult:
    result = monthly_dsr(
        _wiggle(0.004, 0.02),
        effective_size=120.0,
        same_window_sharpes=(0.2, 0.2),
        declared_trials=5,
        trial_register_version="t",
    )
    assert result is not None
    return dataclasses.replace(result, deflated_sharpe=value)


def _stats(
    *,
    t_stat: float | None = 3.5,
    cohort_passed: bool = True,
    dsr: float | None = 0.97,
    haircut: tuple[bool, bool] = (True, True),
    cc_haircut: tuple[bool, bool] = (True, True),
    mean: float = 0.004,
    cc_mean: float = 0.003,
) -> PolicyStatistics:
    return PolicyStatistics(
        mean_active=mean,
        hac=None if t_stat is None else _hac(t_stat),
        cohort=CohortTest(11, 10, 0.05, 4.5 if cohort_passed else 2.0, 3.96, cohort_passed),
        dsr=None if dsr is None else _dsr(dsr),
        haircut_pass={0.15: haircut[0], 0.58: haircut[1]},
        complete_case_mean_active=cc_mean,
        complete_case_haircut_pass={0.15: cc_haircut[0], 0.58: cc_haircut[1]},
    )


def _policies(governing: PolicyStatistics, other: PolicyStatistics | None = None) -> dict[str, PolicyStatistics]:
    other = governing if other is None else other
    return {ZERO_RECOVERY.label: governing, CLASSIFIED_WORST.label: other, CLASSIFIED_BEST.label: governing}


@pytest.mark.parametrize(
    ("by_policy", "gate", "failed", "verdict"),
    [
        (None, False, False, Verdict.GATE_FAIL),
        (None, True, True, Verdict.SIMULATOR_INVARIANT),
        (_policies(_stats()), True, False, Verdict.PASS_ROBUST),
        (_policies(_stats(haircut=(True, False))), True, False, Verdict.PASS_CONTINGENT),
        (_policies(_stats(cc_haircut=(True, False))), True, False, Verdict.PASS_CONTINGENT),
        (
            _policies(_stats(t_stat=-3.4, mean=-0.004, haircut=(False, False))),
            True,
            False,
            Verdict.UNDERPERFORMS_CONTROL,
        ),
        (_policies(_stats(t_stat=-1.0, mean=-0.001)), True, False, Verdict.FAIL_CONTROL),
        (_policies(_stats(t_stat=2.0)), True, False, Verdict.UNDETERMINED_AT_THIS_POWER),
    ],
    ids=[
        "gate",
        "simulator",
        "robust",
        "contingent-headline",
        "contingent-diagnostic",
        "underperforms",
        "fail-control",
        "undetermined",
    ],
)
def test_verdict_table(
    by_policy: dict[str, PolicyStatistics] | None, gate: bool, failed: bool, verdict: Verdict
) -> None:
    assert decide_verdict(by_policy, gate_passed=gate, simulator_failed=failed).verdict is verdict


@pytest.mark.parametrize(
    "weak",
    [
        _stats(t_stat=2.9),
        _stats(cohort_passed=False),
        _stats(dsr=0.95),
        _stats(dsr=None),
        _stats(cc_mean=0.0),
        _stats(haircut=(False, False)),
    ],
    ids=["hac-t", "cohort", "dsr-strict", "dsr-refused", "diagnostic-mean", "haircut"],
)
def test_one_weak_policy_can_only_turn_a_pass_into_a_fail(weak: PolicyStatistics) -> None:
    result = decide_verdict(_policies(_stats(), weak), gate_passed=True, simulator_failed=False)
    assert result.verdict not in (Verdict.PASS_ROBUST, Verdict.PASS_CONTINGENT)


def test_binding_reports_the_least_favourable_policy_and_refusals() -> None:
    result = decide_verdict(
        _policies(_stats(), _stats(t_stat=None, mean=0.001)), gate_passed=True, simulator_failed=False
    )
    assert result.binding["hac_t"] == (CLASSIFIED_WORST.label, None)
    assert result.binding["mean_active"] == (CLASSIFIED_WORST.label, 0.001)
    assert result.verdict is Verdict.UNDETERMINED_AT_THIS_POWER


def test_verdict_requires_the_governing_policy() -> None:
    with pytest.raises(ValueError):
        decide_verdict({CLASSIFIED_BEST.label: _stats()}, gate_passed=True, simulator_failed=False)
