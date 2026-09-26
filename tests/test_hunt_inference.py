"""#3385 slice 3a — the hunt's inference, pure logic, hand-computed where a published answer exists."""

from __future__ import annotations

import hashlib
import math
from pathlib import Path

import numpy as np
import pytest

from app.services import deflated_sharpe as deflated_sharpe_module
from app.services import hunt_harness as hh
from app.services import hunt_inference as hi
from app.services import r6_monthly_trial
from app.services.deflated_sharpe import TradeMoments, deflated_sharpe, trade_moments
from app.services.r6_monthly_trial import HacEstimate, hac_t, newey_west_lag, student_t_cdf


def _noise(seed: int, count: int = 300, drift: float = 0.0) -> tuple[float, ...]:
    rng = np.random.default_rng(seed)
    return tuple(float(value) + drift for value in rng.normal(0.0, 0.01, count))


def _cell(active: tuple[float, ...], h: int = 1) -> hi.CellStatistics:
    cell = hi.cell_statistics(active, h=h, entered_formations=range(len(active)))
    assert isinstance(cell, hi.CellStatistics)
    return cell


# --- HAC at a caller lag ----------------------------------------------------------------------------------------


@pytest.mark.parametrize("seed", [1, 2, 3])
def test_hac_estimate_equals_r6_hac_t_at_the_r6_lag(seed: int) -> None:
    series = _noise(seed, count=134, drift=0.001)
    ours = hi.hac_estimate(series, newey_west_lag(len(series)))
    theirs = hac_t(series, ruined=False)
    assert isinstance(ours, HacEstimate) and isinstance(theirs, HacEstimate)
    assert ours == theirs


def test_hac_estimate_hand_computed_at_a_longer_lag() -> None:
    # c = (2, 2, −2, −2): γ0 = 4, γ1 = 1, γ2 = −2; L = 2: Ω = 4 + 2·(2/3)·1 + 2·(1/3)·(−2) = 4.
    estimate = hi.hac_estimate([3.0, 3.0, -1.0, -1.0], 2)
    assert isinstance(estimate, HacEstimate)
    assert estimate.long_run_variance == pytest.approx(4.0)
    assert estimate.t_stat == pytest.approx(1.0)


@pytest.mark.parametrize(
    ("observations", "h", "lag"),
    [(100, 1, 4), (100, 5, 10), (134, 1, 4), (4750, 5, 10), (4750, 1, 9)],
)
def test_hunt_lag_is_the_larger_of_2h_and_newey_west(observations: int, h: int, lag: int) -> None:
    assert hi.hunt_lag(observations, h) == lag


# --- refusals ------------------------------------------------------------------------------------------------


def test_short_sample_is_t_below_ten_lags() -> None:
    # h = 5 → L = 10 at T = 99 (NW gives 4): 10·L = 100 > 99.
    refused = hi.cell_statistics(_noise(1, 99), h=5, entered_formations=range(0, 99))
    assert isinstance(refused, hi.StatRefused) and refused.reason == "short_sample"


def test_sparse_arm_counts_greedily_h_apart() -> None:
    assert hi.sparse_arm_count([8, 0, 1, 2, 3, 5, 7, 7], 3) == 3  # 0, 3, 7
    assert hi.sparse_arm_count([], 1) == 0


@pytest.mark.parametrize(("formations", "refused"), [(29, True), (30, False)])
def test_sparse_arm_boundary(formations: int, refused: bool) -> None:
    result = hi.cell_statistics(_noise(4), h=1, entered_formations=range(formations))
    assert isinstance(result, hi.StatRefused) is refused
    if refused:
        assert isinstance(result, hi.StatRefused) and result.reason == "sparse_arm"


def test_constant_series_is_degenerate_variance() -> None:
    result = hi.cell_statistics((0.001,) * 300, h=1, entered_formations=range(300))
    assert isinstance(result, hi.StatRefused) and result.reason == "degenerate_variance"


@pytest.mark.parametrize("bad", [math.nan, math.inf])
def test_a_non_finite_active_return_refuses(bad: float) -> None:
    series = list(_noise(5))
    series[17] = bad
    result = hi.cell_statistics(series, h=1, entered_formations=range(300))
    assert isinstance(result, hi.StatRefused) and result.reason == "non_finite"


def test_overflowing_intermediates_refuse_rather_than_raise() -> None:
    result = hi.hac_estimate([1e308, -1e308, 1e308, -1e308], 1)
    assert isinstance(result, hi.StatRefused) and result.reason == "non_finite"


# --- p --------------------------------------------------------------------------------------------------------


def test_the_shared_cdf_goes_negative_in_the_tail_and_p_is_clamped() -> None:
    assert student_t_cdf(-100.0, 100) < 0.0  # v3 finding 42, reproduced
    assert hi.one_sided_p(100.0, 100) == 0.0
    assert hi.one_sided_p(0.0, 50) == pytest.approx(0.5)
    assert hi.one_sided_p(-100.0, 100) == 1.0


def test_an_underflowing_p_is_recorded_as_a_label_and_screens_as_zero() -> None:
    cell = _cell(_noise(6, drift=0.01))
    assert cell.p_underflow and cell.p == 0.0
    assert cell.form()["p"] == "< 1e-12"
    assert hi.screening_p({"a": cell}) == 0.0


def test_p_is_one_sided_in_the_arm_s_favour() -> None:
    cell = _cell(_noise(7, drift=-0.002))
    assert cell.t_stat < 0 and cell.p > 0.5


# --- Benjamini–Yekutieli ------------------------------------------------------------------------------------------

#: BY 2001, Example 3.4 (Steel & Torrie uterine weights): "Four hypotheses are rejected when
#: applying procedure (1) using FDR level of 0.05."
_EXAMPLE_3_4 = (0.183, 0.101, 0.028, 0.012, 0.003, 0.002)
#: BY 2001, Table 1 (Needleman et al.): p-values per family, procedure (1)'s rejection count.
_TABLE_1 = {
    "teacher": ((0.003, 0.05, 0.05, 0.14, 0.08, 0.01, 0.04, 0.01, 0.05, 0.003, 0.003), 5),
    "wisc": ((0.04, 0.05, 0.02, 0.49, 0.08, 0.36, 0.03, 0.38, 0.15, 0.90, 0.37, 0.54), 0),
    "verbal": ((0.002, 0.03, 0.07, 0.37, 0.90, 0.42, 0.05, 0.04, 0.32, 0.001, 0.001, 0.01), 4),
}


def _keyed(values: tuple[float, ...]) -> dict[int, float]:
    return dict(enumerate(values))


def test_step_up_reproduces_the_paper_s_example_3_4() -> None:
    flagged = hi.step_up(_keyed(_EXAMPLE_3_4), m=6, level=0.05)
    assert len(flagged) == 4


@pytest.mark.parametrize("family", sorted(_TABLE_1))
def test_step_up_reproduces_the_paper_s_table_1(family: str) -> None:
    values, rejected = _TABLE_1[family]
    assert len(hi.step_up(_keyed(values), m=len(values), level=0.05)) == rejected


def test_by_screen_hand_computed_on_example_3_4() -> None:
    # c(6) = 49/20 = 2.45; bars k·0.05/(6·2.45) = k·0.0034014: .002 ✓ (k=1), .003 ✓ (k=2), .012 > .0102 (k=3), …
    readout = hi.by_screen(_keyed(_EXAMPLE_3_4), m=6)
    assert readout.c_m == pytest.approx(2.45)
    assert readout.flagged == frozenset({4, 5})


def test_p_one_padding_adds_no_rejections() -> None:
    values = _keyed(_EXAMPLE_3_4)
    padded = {**values, **{100 + index: 1.0 for index in range(20)}}
    assert hi.step_up(values, m=26, level=0.05) == hi.step_up(padded, m=26, level=0.05)
    assert hi.by_screen(values, m=26).flagged == hi.by_screen(padded, m=26).flagged


def test_step_up_flags_nothing_when_no_k_qualifies_and_flags_ties() -> None:
    assert hi.step_up({"a": 0.2, "b": 0.3}, m=2, level=0.05) == frozenset()
    assert hi.step_up({"a": 0.01, "b": 0.01, "c": 0.9}, m=3, level=0.05) == frozenset({"a", "b"})


def test_step_up_rejects_a_bad_family() -> None:
    with pytest.raises(ValueError):
        hi.step_up({"a": 0.01, "b": 0.02}, m=1, level=0.05)
    with pytest.raises(ValueError):
        hi.step_up({"a": math.nan}, m=1, level=0.05)
    with pytest.raises(ValueError):
        hi.step_up({"a": -1e-16}, m=1, level=0.05)


def test_screening_p_is_the_worst_cell_and_one_on_any_refusal() -> None:
    low, high = _cell(_noise(8, drift=0.002)), _cell(_noise(9, drift=0.0005))
    assert hi.screening_p({"a": low, "b": high}) == max(low.p, high.p)
    assert hi.screening_p({"a": low, "b": hi.StatRefused("sparse_arm", "x")}) == 1.0
    assert hi.screening_p(None) == 1.0
    assert hi.screening_p({}) == 1.0


# --- V[SR] ----------------------------------------------------------------------------------------------------


def _members(count: int) -> list[hi.VPopulationMember]:
    return [hi.VPopulationMember(trial, _noise(100 + trial), ruined=False) for trial in range(1, count + 1)]


def test_v_population_counts_clones_once_and_excludes_the_unusable() -> None:
    members = _members(10)
    members += [
        hi.VPopulationMember(50, members[0].active_series, ruined=False),
        hi.VPopulationMember(51, None, ruined=False),
        hi.VPopulationMember(52, _noise(999), ruined=True),
        hi.VPopulationMember(53, (0.0,) * 300, ruined=False),
        hi.VPopulationMember(54, (math.nan,) * 300, ruined=False),
    ]
    variance = hi.trial_sharpe_variance(members)
    assert isinstance(variance, hi.TrialSharpeVariance)
    assert variance.trial_ids == tuple(range(1, 11))
    assert variance.excluded == {"no_series": 1, "ruined": 1, "non_finite": 1, "zero_variance": 1, "duplicate": 1}
    sharpes = []
    for member in members[:10]:
        assert member.active_series is not None
        moments = trade_moments(member.active_series)
        assert moments is not None
        sharpes.append(moments.sharpe)
    measured = float(np.var(sharpes, ddof=1))
    assert variance.measured_variance == pytest.approx(measured)
    assert variance.floor == pytest.approx(1 / 300)
    assert variance.variance == max(measured, 1 / 300)


def test_clones_cannot_collapse_v_below_the_floor() -> None:
    base = _noise(1)
    clones = [
        hi.VPopulationMember(trial, tuple(value * (1 + trial * 1e-9) for value in base), False) for trial in range(12)
    ]
    variance = hi.trial_sharpe_variance(clones)
    assert isinstance(variance, hi.TrialSharpeVariance)
    assert variance.measured_variance < 1e-12
    assert variance.variance == pytest.approx(1 / 300)


def test_fewer_than_ten_distinct_trials_refuses() -> None:
    result = hi.trial_sharpe_variance(_members(9))
    assert isinstance(result, hi.StatRefused) and result.reason == "trial_population_too_small"


# --- DSR -------------------------------------------------------------------------------------------------------


def test_n_hat_is_m_and_reproduces_the_paper_through_the_shared_functions() -> None:
    # Bailey & López de Prado 2014's worked answer (0.9004 at N = 100) with the hunt's convention
    # average_correlation = 0.0; tests/test_deflated_sharpe.py pins the other two published values.
    moments = TradeMoments(sharpe=2.5 / math.sqrt(250), skewness=-3.0, kurtosis=10.0, trade_count=1250)
    result = deflated_sharpe(
        moments,
        effective_sample_size=1250,
        trial_sharpe_variance=0.5 / 250,
        declared_trials=100,
        average_correlation=0.0,
        measured_trials=100,
        trial_register_version="test",
    )
    assert result is not None
    assert result.independent_trials == 100
    assert result.deflated_sharpe == pytest.approx(0.9004, abs=5e-5)


def test_hunt_dsr_is_the_shared_dsr_at_t_eff_and_n_hat_m() -> None:
    active = _noise(11, drift=0.002)
    cell = _cell(active)
    variance = hi.trial_sharpe_variance(_members(12))
    assert isinstance(variance, hi.TrialSharpeVariance)
    dsr = hi.hunt_dsr(active, cell, variance=variance, declared_trials=300, trial_register_version="r12")
    assert isinstance(dsr, hi.HuntDsr)
    assert dsr.axis_id == "hunt-calendar-active-dsr-v1"
    assert dsr.result.independent_trials == 300
    expected_t_eff = min(300.0, 300 * cell.variance / cell.long_run_variance)
    assert dsr.effective_sample_size == pytest.approx(expected_t_eff)
    moments = trade_moments(active)
    assert moments is not None
    direct = deflated_sharpe(
        moments,
        effective_sample_size=expected_t_eff,
        trial_sharpe_variance=variance.variance,
        declared_trials=300,
        average_correlation=0.0,
        measured_trials=12,
        trial_register_version="r12",
    )
    assert direct is not None and dsr.result.deflated_sharpe == pytest.approx(direct.deflated_sharpe)


def test_t_eff_shrinks_under_positive_dependence_and_is_capped_at_t() -> None:
    rng = np.random.default_rng(12)
    shocks = rng.normal(0.0, 0.01, 400)
    persistent = tuple(float(value) + 0.001 for value in np.convolve(shocks, np.ones(8) / 8, mode="valid")[:300])
    alternating = tuple(0.001 + (0.01 if index % 2 else -0.01) + float(shocks[index]) * 0.1 for index in range(300))
    variance = hi.trial_sharpe_variance(_members(12))
    assert isinstance(variance, hi.TrialSharpeVariance)
    for series, capped in ((persistent, False), (alternating, True)):
        cell = _cell(series)
        dsr = hi.hunt_dsr(series, cell, variance=variance, declared_trials=300, trial_register_version="r12")
        assert isinstance(dsr, hi.HuntDsr)
        assert (dsr.effective_sample_size == 300.0) is capped
        assert dsr.effective_sample_size <= 300.0


def test_a_short_effective_sample_refuses() -> None:
    rng = np.random.default_rng(13)
    shocks = rng.normal(0.0, 0.01, 400)
    # A 40-session moving average: T_eff ≈ T·γ0/Ω is far below 30.
    series = tuple(float(value) for value in np.convolve(shocks, np.ones(40) / 40, mode="valid")[:300])
    cell = _cell(series, h=10)
    variance = hi.trial_sharpe_variance(_members(12))
    assert isinstance(variance, hi.TrialSharpeVariance)
    result = hi.hunt_dsr(series, cell, variance=variance, declared_trials=300, trial_register_version="r12")
    assert isinstance(result, hi.StatRefused) and result.reason == "short_effective_sample"


def test_m_must_cover_the_measured_trials() -> None:
    active = _noise(11, drift=0.002)
    variance = hi.trial_sharpe_variance(_members(12))
    assert isinstance(variance, hi.TrialSharpeVariance)
    with pytest.raises(ValueError):
        hi.hunt_dsr(active, _cell(active), variance=variance, declared_trials=11, trial_register_version="r12")


# --- verdicts --------------------------------------------------------------------------------------------------

_PASS = hi.BaseReadout(t_stat=3.5, dsr=0.97, mean=0.001)
_STRESS_PASS = hi.StressReadout(t_stat=3.2, mean=0.0008)


def _verdict(base: dict, stress: dict | None = None) -> hi.VerdictResult:
    return hi.decide_verdict(base, stress if stress is not None else dict.fromkeys(base, _STRESS_PASS))


def test_pass_and_its_threshold_equalities() -> None:
    assert _verdict({"a": _PASS, "b": _PASS}).verdict is hi.Verdict.PASS
    assert _verdict({"a": hi.BaseReadout(3.5, 0.95, 0.001)}).verdict is hi.Verdict.PASS  # DSR ≥ 0.95
    assert _verdict({"a": hi.BaseReadout(3.0, 0.99, 0.001)}).verdict is hi.Verdict.UNDETERMINED  # t > 3.0 strict
    assert (
        _verdict({"a": _PASS}, {"a": hi.StressReadout(3.0, 0.001)}).verdict is hi.Verdict.PASS_CONTINGENT
    )  # stress t > 3.0 strict


def test_refused_base_wins_over_everything() -> None:
    result = _verdict({"a": _PASS, "b": hi.StatRefused("sparse_arm", "x")})
    assert result.verdict is hi.Verdict.NOT_PASS_REFUSED
    assert result.reasons == ("b: sparse_arm",)
    assert _verdict({"a": hi.BaseReadout(-4, 0.0, -0.01), "b": hi.StatRefused("non_finite", "x")}).verdict is (
        hi.Verdict.NOT_PASS_REFUSED
    )


def test_pass_contingent_when_a_stress_cell_fails_or_refuses() -> None:
    assert _verdict({"a": _PASS}, {"a": hi.StressReadout(3.5, 0.0)}).verdict is hi.Verdict.PASS_CONTINGENT
    refused = _verdict({"a": _PASS}, {"a": hi.StatRefused("book_ruin", "x")})
    assert refused.verdict is hi.Verdict.PASS_CONTINGENT
    assert refused.reasons == ("stress a: refused book_ruin",)
    assert not hi.Verdict.PASS_CONTINGENT.permits_holdout


def test_fail_control_needs_every_base_mean_at_or_below_zero() -> None:
    assert _verdict({"a": hi.BaseReadout(3.5, 0.99, 0.0), "b": hi.BaseReadout(-2, 0.0, -0.001)}).verdict is (
        hi.Verdict.FAIL_CONTROL
    )


def test_undetermined_is_positive_everywhere_but_short_of_t_or_dsr() -> None:
    result = _verdict({"a": hi.BaseReadout(2.0, 0.99, 0.001), "b": hi.BaseReadout(3.5, 0.94, 0.001)})
    assert result.verdict is hi.Verdict.UNDETERMINED
    assert len(result.reasons) == 2


def test_mixed_signs_are_not_pass_mixed() -> None:
    assert _verdict({"a": _PASS, "b": hi.BaseReadout(3.5, 0.99, 0.0)}).verdict is hi.Verdict.NOT_PASS_MIXED


@pytest.mark.parametrize("bad", [math.nan, math.inf])
def test_a_non_finite_readout_cannot_be_built(bad: float) -> None:
    with pytest.raises(ValueError):
        hi.BaseReadout(bad, 0.99, 0.001)
    with pytest.raises(ValueError):
        hi.StressReadout(3.5, bad)


def test_base_and_stress_must_be_the_same_cells() -> None:
    with pytest.raises(ValueError):
        hi.decide_verdict({"a": _PASS}, {"b": _STRESS_PASS})
    with pytest.raises(ValueError):
        hi.decide_verdict({}, {})


def test_no_pass_closes_the_hunt_with_no_demonstrated_edge() -> None:
    assert hi.validation_closure({"x": hi.Verdict.UNDETERMINED, "y": hi.Verdict.PASS_CONTINGENT}) is (
        hi.HuntClosure.NO_DEMONSTRATED_EDGE
    )
    assert hi.validation_closure({"x": hi.Verdict.UNDETERMINED, "y": hi.Verdict.PASS}) is None
    assert {verdict.value for verdict in hi.Verdict}.isdisjoint({closure.value for closure in hi.HuntClosure})


# --- regime readout and model identity ---------------------------------------------------------------------------


def test_regime_readout_is_mean_and_count_per_label() -> None:
    assert hi.regime_readout([1.0, 3.0, -1.0], ["bull", "bull", "bear"]) == {"bear": (-1.0, 1), "bull": (2.0, 2)}
    with pytest.raises(ValueError):
        hi.regime_readout([1.0], [])


def test_the_inference_code_and_its_shared_statistics_are_part_of_the_harness_model_id() -> None:
    expected = {
        module.__name__: hashlib.sha256(Path(str(module.__file__)).read_bytes()).hexdigest()
        for module in (hi, deflated_sharpe_module, r6_monthly_trial)
    }
    assert hh._model_constants()["model_code_sha256"] == expected


# --- Codex ckpt-2 findings -------------------------------------------------------------------------------------

#: Finite, passes ``cell_statistics``, but ``trade_moments`` raises on its numerical Pearson check.
_PEARSON_TRAP = tuple([0.001] + [0.001 * (1 + 1e-14)] * 29)


def test_the_pearson_trap_is_real() -> None:
    with pytest.raises(ValueError):
        trade_moments(_PEARSON_TRAP)


def test_a_trade_moments_failure_is_excluded_from_v_not_raised() -> None:
    members = [*_members(10), hi.VPopulationMember(99, _PEARSON_TRAP, ruined=False)]
    variance = hi.trial_sharpe_variance(members)
    assert isinstance(variance, hi.TrialSharpeVariance)
    assert variance.excluded["zero_variance"] == 1 and 99 not in variance.trial_ids


def test_a_trade_moments_failure_refuses_the_dsr() -> None:
    cell = hi.cell_statistics(_PEARSON_TRAP, h=1, entered_formations=range(30))
    assert isinstance(cell, hi.CellStatistics)
    variance = hi.trial_sharpe_variance(_members(12))
    assert isinstance(variance, hi.TrialSharpeVariance)
    result = hi.hunt_dsr(_PEARSON_TRAP, cell, variance=variance, declared_trials=300, trial_register_version="r12")
    assert isinstance(result, hi.StatRefused) and result.reason == "degenerate_variance"


def test_the_dsr_refuses_a_series_from_another_cell() -> None:
    first, second = _noise(11, drift=0.002), _noise(21, drift=0.002)
    variance = hi.trial_sharpe_variance(_members(12))
    assert isinstance(variance, hi.TrialSharpeVariance)
    with pytest.raises(ValueError):
        hi.hunt_dsr(second, _cell(first), variance=variance, declared_trials=300, trial_register_version="r12")
