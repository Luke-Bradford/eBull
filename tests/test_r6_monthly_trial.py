"""#2901 PR B part 1: identity gate, statistics, composition and verdict of the quality arm's monthly trial."""

from __future__ import annotations

import math
import zipfile
from pathlib import Path

import pytest

from app.services.r6_exclusion_trial import CLASSIFIED_BEST, CLASSIFIED_WORST, ZERO_RECOVERY
from app.services.r6_monthly_trial import (
    CONTINGENT_HAIRCUT,
    GATING_CONDITIONS,
    HAIRCUTS,
    REFERENCE_MEMBER,
    ROBUST_HAIRCUT,
    STATISTIC_MONTHS,
    CohortTest,
    Comparator,
    Condition,
    Direction,
    GateReadout,
    GateRefusal,
    HacEstimate,
    HaircutMargins,
    PolicyStatistics,
    Portfolio,
    Refused,
    TotalReturns,
    Tri,
    Verdict,
    bonferroni_t,
    cohort_active_returns,
    cohort_t,
    cohort_t_bar,
    compose_condition,
    compose_gate,
    decide_verdict,
    evaluate_conditions,
    family_size,
    hac_t,
    haircut_margins,
    identity_gate,
    margin_condition,
    mean_active,
    newey_west_lag,
    read_global_q_gpa,
    student_t_cdf,
    student_t_quantile,
    summarise_policy,
    two_sided_p,
    weak_kleene_and,
)

T, F, R = Tri.TRUE, Tri.FALSE, Tri.REFUSED


def test_statistic_window_is_134_full_months() -> None:
    assert len(STATISTIC_MONTHS) == 134
    assert (STATISTIC_MONTHS[0], STATISTIC_MONTHS[-1]) == ((2013, 7), (2024, 8))


# --- Weak Kleene -------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("left", "right", "expected"),
    [(T, T, T), (T, F, F), (F, F, F), (T, R, R), (F, R, R), (R, R, R), (F, T, F), (R, T, R), (R, F, R)],
)
def test_weak_kleene_truth_table(left: Tri, right: Tri, expected: Tri) -> None:
    assert weak_kleene_and((left, right)) is expected


def test_weak_kleene_needs_an_input() -> None:
    with pytest.raises(ValueError):
        weak_kleene_and(())


# --- Mean and HAC ------------------------------------------------------------------------------------------


@pytest.mark.parametrize(("observations", "lag"), [(134, 4), (100, 4), (50, 3), (4, 1)])
def test_newey_west_lag_is_nw1994_rule_of_thumb(observations: int, lag: int) -> None:
    assert newey_west_lag(observations) == lag


def test_hac_t_hand_computed_negative_autocorrelation() -> None:
    # c = ±1, γ0 = 1, γ1 = −3/4, L = 1: Ω = 1 + 2·½·(−3/4) = 1/4; SE = √(Ω/4) = 1/4.
    estimate = hac_t([2.0, 0.0, 2.0, 0.0], ruined=False)
    assert isinstance(estimate, HacEstimate)
    assert (estimate.lag, estimate.variance, estimate.long_run_variance) == (1, 1.0, 0.25)
    assert estimate.t_stat == pytest.approx(4.0)


def test_hac_t_hand_computed_positive_autocorrelation() -> None:
    # c = (2, 2, −2, −2): γ0 = 4, γ1 = 1, Ω = 5.
    estimate = hac_t([3.0, 3.0, -1.0, -1.0], ruined=False)
    assert isinstance(estimate, HacEstimate)
    assert estimate.long_run_variance == pytest.approx(5.0)
    assert estimate.t_stat == pytest.approx(1.0 / math.sqrt(5.0 / 4.0))


@pytest.mark.parametrize(
    ("series", "ruined"),
    [([0.01] * 134, False), ([0.01, math.nan, 0.02], False), ([0.01], False), ([0.01, 0.02, 0.03], True)],
    ids=["gamma0-zero", "non-finite", "one-observation", "ruin"],
)
def test_hac_t_refuses(series: list[float], ruined: bool) -> None:
    assert isinstance(hac_t(series, ruined=ruined), Refused)


def test_hac_t_refuses_a_non_finite_long_run_variance() -> None:
    # Finite inputs whose squared deviations overflow: γ̂₀ = inf and Ω̂ = nan.
    assert isinstance(hac_t([1e300, -1e300, 1e300, -1e300], ruined=False), Refused)


def test_mean_active_refusals_and_value() -> None:
    assert mean_active([0.01, 0.03], ruined=False) == pytest.approx(0.02)
    assert isinstance(mean_active([0.01], ruined=False), Refused)
    assert isinstance(mean_active([0.01, math.inf], ruined=False), Refused)
    assert isinstance(mean_active([0.01, 0.02], ruined=True), Refused)


# --- Bonferroni --------------------------------------------------------------------------------------------


def test_family_size_is_history_plus_first_run_plus_reserved() -> None:
    assert family_size(0) == 8
    assert family_size(10) == 18
    with pytest.raises(ValueError):
        family_size(-1)


def test_two_sided_p_is_a_survival_function_symmetric_in_t() -> None:
    assert two_sided_p(-3.0) == two_sided_p(3.0)
    assert two_sided_p(3.0) == pytest.approx(0.0026997960632601913, rel=1e-12)
    # 1 − Φ(10) is 0 in double precision; the survival function keeps the tail.
    assert 0.0 < two_sided_p(10.0) < 1e-22


def test_bonferroni_term_starts_to_bind_at_m_19() -> None:
    # α/M < p(3) = 0.0026998 ⇔ M > 18.52.
    assert bonferroni_t(18) == 3.0
    assert bonferroni_t(19) > 3.0
    assert two_sided_p(bonferroni_t(19)) == pytest.approx(0.05 / 19, rel=1e-9)
    for bad in (0, -1, True):
        with pytest.raises(ValueError):
            bonferroni_t(bad)


# --- Student t / cohort t ----------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("probability", "df", "published"),
    [(0.975, 10, 2.228), (0.995, 10, 3.169), (0.975, 1, 12.706), (0.975, 2, 4.303), (0.975, 30, 2.042)],
)
def test_student_t_quantile_matches_published_tables(probability: float, df: int, published: float) -> None:
    assert student_t_quantile(probability, df) == pytest.approx(published, abs=5e-4)
    assert student_t_quantile(1.0 - probability, df) == pytest.approx(-published, abs=5e-4)


def test_student_t_quantile_refuses_outside_its_bracket() -> None:
    with pytest.raises(ValueError):
        student_t_quantile(0.9999, 1)  # Cauchy quantile ≈ 3183 > 100


def test_student_t_cdf_closed_forms() -> None:
    assert student_t_cdf(0.0, 7) == 0.5
    assert student_t_cdf(1.0, 1) == pytest.approx(0.75)  # Cauchy
    assert student_t_cdf(1.5, 2) == pytest.approx(0.5 + 1.5 / (2 * math.sqrt(2 + 1.5**2)))


def _t_density(x: float, df: int) -> float:
    log_norm = math.lgamma((df + 1) / 2) - math.lgamma(df / 2) - 0.5 * math.log(df * math.pi)
    return math.exp(log_norm - (df + 1) / 2 * math.log1p(x * x / df))


def test_cohort_bar_is_3_957_and_matches_numerical_integration() -> None:
    q = cohort_t_bar(10)
    assert q == pytest.approx(3.957, abs=5e-4)
    # Independent cross-check: Simpson's rule on the t₁₀ density over [0, q] equals Φ(3) − ½.
    steps = 20_000
    width = q / steps
    total = _t_density(0.0, 10) + _t_density(q, 10)
    total += sum((4 if i % 2 else 2) * _t_density(i * width, 10) for i in range(1, steps))
    assert total * width / 3 == pytest.approx(0.5 - 0.5 * math.erfc(3.0 / math.sqrt(2.0)), abs=1e-11)


def test_cohort_active_returns_keeps_only_complete_holding_years() -> None:
    arm = dict.fromkeys(STATISTIC_MONTHS, 0.01)
    control = dict.fromkeys(STATISTIC_MONTHS, 0.0)
    cohorts = cohort_active_returns(arm, control)
    assert list(cohorts) == list(range(2013, 2024))
    assert cohorts[2013] == pytest.approx(1.01**12 - 1.0)


def test_cohort_t_hand_computed() -> None:
    result = cohort_t({2013: 0.01, 2014: 0.03, 2015: 0.02}, ruined=False)
    assert isinstance(result, CohortTest)
    # mean 0.02, sd 0.01 → t = 0.02 / (0.01/√3) = 2√3, against t₂.
    assert (result.cohorts, result.df) == (3, 2)
    assert result.t_stat == pytest.approx(2 * math.sqrt(3))
    assert result.bar == cohort_t_bar(2)


@pytest.mark.parametrize(
    ("cohorts", "ruined"),
    [
        (dict.fromkeys(range(2013, 2024), 0.02), False),
        ({2013: 0.02}, False),
        ({2013: 0.02, 2014: math.nan}, False),
        ({2013: 0.01, 2014: 0.02}, True),
    ],
    ids=["zero-spread", "one-cohort", "non-finite", "ruin"],
)
def test_cohort_t_refuses(cohorts: dict[int, float], ruined: bool) -> None:
    assert isinstance(cohort_t(cohorts, ruined=ruined), Refused)


# --- Haircut -----------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("arm_gross", "arm_net", "comparator_gross", "comparator_net", "haircut", "positive", "beats"),
    [
        # edge 0.40 → 0.168 at d = 0.58; adjusted = 1.0 + 0.168 − 0.10 = 1.068; − 0.95 = 0.118.
        (1.40, 1.30, 1.00, 0.95, 0.58, 1.068, 0.118),
        # edge 0.10 → 0.042; adjusted = 0.942; − 0.95 < 0.
        (1.10, 1.00, 1.00, 0.95, 0.58, 0.942, -0.008),
        # edge ≤ 0 is carried in full: adjusted = A_net.
        (-0.20, -0.25, -0.10, -0.12, 0.15, -0.25, -0.13),
        # ruin of A: an observed −1, still evaluable.
        (-1.0, -1.0, 0.2, 0.18, 0.15, -1.0, -1.18),
    ],
)
def test_haircut_margins(
    arm_gross: float,
    arm_net: float,
    comparator_gross: float,
    comparator_net: float,
    haircut: float,
    positive: float,
    beats: float,
) -> None:
    margins = haircut_margins(
        arm_gross=arm_gross,
        arm_net=arm_net,
        comparator_gross=comparator_gross,
        comparator_net=comparator_net,
        haircut=haircut,
    )
    assert isinstance(margins, HaircutMargins)
    assert (margins.positive, margins.beats_comparator) == (pytest.approx(positive), pytest.approx(beats))


def test_haircut_margins_refuse_a_non_finite_total() -> None:
    margins = haircut_margins(arm_gross=math.nan, arm_net=0.1, comparator_gross=0.1, comparator_net=0.1, haircut=0.15)
    assert isinstance(margins, Refused)


# --- Reference parsing -------------------------------------------------------------------------------------

HEADER = "year,month,rank_GPA,nstocks,ret_vw"


def _reference_rows(low: float = 1.0, high: float = 2.5) -> list[str]:
    rows = []
    for year, month in STATISTIC_MONTHS:
        rows += [f"{year},{month},1,300,{low}", f"{year},{month},5,300,9.0", f"{year},{month},10,300,{high}"]
    return rows


def _reference_zip(tmp_path: Path, rows: list[str], header: str = HEADER) -> Path:
    path = tmp_path / "prof.zip"
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr(REFERENCE_MEMBER, "\n".join([header, *rows]) + "\n")
    return path


def test_read_global_q_gpa_converts_each_leg_from_percent(tmp_path: Path) -> None:
    spread = read_global_q_gpa(_reference_zip(tmp_path, ["2013,6,1,0,-500", *_reference_rows(2.0, 3.5)]))
    assert list(spread) == list(STATISTIC_MONTHS)
    assert spread[(2013, 7)] == pytest.approx(0.035 - 0.02)


def test_read_global_q_gpa_accepts_a_total_loss(tmp_path: Path) -> None:
    rows = _reference_rows()
    rows[0] = "2013,7,1,300,-100"
    assert read_global_q_gpa(_reference_zip(tmp_path, rows))[(2013, 7)] == pytest.approx(0.025 + 1.0)


@pytest.mark.parametrize(
    ("change", "header"),
    [
        (None, "year,month,rank_GP,nstocks,ret_vw"),
        (("append", "2010,1,5,300,1.0\n2010,1,5,300,1.0"), HEADER),
        (("replace", "2013,7,1,0,1.0"), HEADER),
        (("replace", "2013,7,1,1.5,1.0"), HEADER),
        (("replace", "2013,7,1,300,-100.5"), HEADER),
        (("replace", "2013,7,1,300,nan"), HEADER),
        (("replace", "2013,7,1,300,"), HEADER),
        (("replace", "2013,x,1,300,1.0"), HEADER),
        (("drop", None), HEADER),
    ],
    ids=[
        "header",
        "duplicate-raw-row-outside-window",
        "nstocks-zero",
        "nstocks-not-integer",
        "below-total-loss",
        "non-finite",
        "missing-return",
        "malformed-key",
        "rank-1-misses-a-month",
    ],
)
def test_read_global_q_gpa_refusals(tmp_path: Path, change: tuple[str, str | None] | None, header: str) -> None:
    rows = _reference_rows()
    if change is not None:
        action, row = change
        if action == "append" and row is not None:
            rows += row.split("\n")
        elif action == "replace" and row is not None:
            rows[0] = row
        else:
            del rows[0]
    with pytest.raises(GateRefusal):
        read_global_q_gpa(_reference_zip(tmp_path, rows, header))


# --- Identity gate -----------------------------------------------------------------------------------------


def _aligned_pair() -> tuple[dict[tuple[int, int], float], dict[tuple[int, int], float]]:
    reference = {month: 0.02 * math.sin(1.3 * i) for i, month in enumerate(STATISTIC_MONTHS)}
    ours = {month: 0.8 * value + 0.002 * math.cos(2.9 * i) for i, (month, value) in enumerate(reference.items())}
    return ours, reference


def test_identity_gate_passes_an_aligned_correlated_spread_and_publishes_no_magnitude() -> None:
    ours, reference = _aligned_pair()
    readout = identity_gate(ours, reference, ruined=False)
    assert isinstance(readout, GateReadout)
    assert (readout.months, readout.beta_positive, readout.passed) == (134, True, True)
    assert not hasattr(readout, "alpha") and not hasattr(readout, "beta")


def test_identity_gate_fails_an_uncorrelated_spread() -> None:
    ours, reference = _aligned_pair()
    readout = identity_gate({m: 0.01 * math.cos(7.1 * i) for i, m in enumerate(ours)}, reference, ruined=False)
    assert isinstance(readout, GateReadout) and not readout.passed


def _drop(series: dict[tuple[int, int], float], month: tuple[int, int]) -> dict[tuple[int, int], float]:
    return {k: v for k, v in series.items() if k != month}


@pytest.mark.parametrize(
    "case", ["ours-missing", "reference-missing", "ours-extra", "ours-nan", "reference-nan", "ruin", "zero-variance"]
)
def test_identity_gate_refuses_this_policy(case: str) -> None:
    ours, reference = _aligned_pair()
    ruined = case == "ruin"
    if case == "ours-missing":
        ours = _drop(ours, (2020, 3))
    elif case == "reference-missing":
        reference = _drop(reference, (2020, 3))
    elif case == "ours-extra":
        ours[(2024, 9)] = 0.0
    elif case == "ours-nan":
        ours[(2020, 3)] = math.nan
    elif case == "reference-nan":
        reference[(2020, 3)] = math.nan
    elif case == "zero-variance":
        ours = dict.fromkeys(ours, 0.01)
    assert isinstance(identity_gate(ours, reference, ruined=ruined), Refused)


def _readout(passed: bool) -> GateReadout:
    return GateReadout(134, 0.5, True, 0.1, 0.1, passed)


@pytest.mark.parametrize(
    ("other", "expected"),
    [(_readout(True), T), (_readout(False), F), (Refused("ruin"), R)],
)
def test_gate_composes_across_policies(other: GateReadout | Refused, expected: Tri) -> None:
    assert compose_gate({ZERO_RECOVERY.label: _readout(True), CLASSIFIED_BEST.label: other}) is expected


def test_gate_requires_the_governing_policy() -> None:
    with pytest.raises(ValueError):
        compose_gate({CLASSIFIED_BEST.label: _readout(True)})


# --- Per-policy statistics ---------------------------------------------------------------------------------


def _synthetic() -> tuple[dict[tuple[int, int], float], ...]:
    control = {month: 0.01 * math.sin(0.9 * i) for i, month in enumerate(STATISTIC_MONTHS)}
    arm = {month: value + 0.004 + 0.01 * math.sin(2.3 * i) for i, (month, value) in enumerate(control.items())}
    complete_case = {month: value + 0.001 for month, value in control.items()}
    return arm, control, complete_case


TOTALS = TotalReturns(1.2, 1.1, 0.8, 0.75, 0.85, 0.8)


def test_summarise_policy_runs_every_statistic_on_synthetic_series() -> None:
    arm, control, complete_case = _synthetic()
    result = summarise_policy(arm=arm, control=control, complete_case=complete_case, totals=TOTALS, ruined=frozenset())
    assert isinstance(result.hac, HacEstimate) and result.hac.lag == 4
    assert isinstance(result.cohort, CohortTest) and result.cohort.cohorts == 11
    assert result.mean_active == pytest.approx(0.004, abs=1e-3)
    assert result.diagnostic_mean == pytest.approx(0.003, abs=1e-3)
    assert set(result.margins) == {(which, d) for which in Comparator for d in HAIRCUTS}


def test_summarise_policy_ruin_refuses_only_the_statistics_on_that_book() -> None:
    arm, control, complete_case = _synthetic()
    result = summarise_policy(
        arm=arm,
        control=control,
        complete_case=complete_case,
        totals=TOTALS,
        ruined=frozenset({Portfolio.COMPLETE_CASE}),
    )
    assert isinstance(result.diagnostic_mean, Refused)
    assert isinstance(result.hac, HacEstimate) and isinstance(result.cohort, CohortTest)
    assert all(isinstance(m, HaircutMargins) for m in result.margins.values())
    headline = summarise_policy(
        arm=arm, control=control, complete_case=complete_case, totals=TOTALS, ruined=frozenset({Portfolio.ARM})
    )
    assert all(isinstance(s, Refused) for s in (headline.mean_active, headline.hac, headline.cohort))
    assert isinstance(headline.diagnostic_mean, Refused)


def test_summarise_policy_refuses_a_series_with_the_partial_month() -> None:
    series = dict.fromkeys(STATISTIC_MONTHS, 0.0)
    with pytest.raises(ValueError):
        summarise_policy(
            arm={**series, (2024, 9): 0.0}, control=series, complete_case=series, totals=TOTALS, ruined=frozenset()
        )


# --- Composition and verdict -------------------------------------------------------------------------------

GOOD = HaircutMargins(0.5, 0.2)
BAD = HaircutMargins(0.5, -0.1)
Q = cohort_t_bar(10)
M = family_size(10)  # 18: the t > 3 term binds


def _stats(
    *,
    t: float | Refused = 3.5,
    cohort: float | Refused = 4.5,
    diag: float | Refused = 0.003,
    mean: float | Refused | None = None,
    margins: dict[tuple[Comparator, float], HaircutMargins | Refused] | None = None,
) -> PolicyStatistics:
    if mean is None:
        mean = t if isinstance(t, Refused) else 0.001 * t
    return PolicyStatistics(
        mean_active=mean,
        hac=t if isinstance(t, Refused) else HacEstimate(134, 4, 0.01, 1.0, 1.0, 0.01, t),
        cohort=cohort if isinstance(cohort, Refused) else CohortTest(11, 10, 0.05, cohort, Q),
        diagnostic_mean=diag,
        margins={(which, d): GOOD for which in Comparator for d in HAIRCUTS} | (margins or {}),
    )


def _policies(governing: PolicyStatistics, other: PolicyStatistics | None = None) -> dict[str, PolicyStatistics]:
    other = governing if other is None else other
    return {ZERO_RECOVERY.label: governing, CLASSIFIED_WORST.label: other, CLASSIFIED_BEST.label: governing}


def _verdict(by_policy: dict[str, PolicyStatistics], family: int = M) -> Verdict:
    conditions = evaluate_conditions(by_policy, family)
    return decide_verdict(conditions, refused_pre_gate=False, gate=T, simulator_failed=False).verdict


def _bad(
    haircut: float, which: Comparator = Comparator.CONTROL
) -> dict[tuple[Comparator, float], HaircutMargins | Refused]:
    return {(which, haircut): BAD}


@pytest.mark.parametrize(
    ("refused_pre_gate", "gate", "failed", "expected"),
    [
        (True, T, False, Verdict.REFUSED_PRE_GATE),
        (False, F, False, Verdict.GATE_FAIL),
        (False, R, False, Verdict.GATE_FAIL),
        (False, T, True, Verdict.SIMULATOR_INVARIANT),
    ],
)
def test_run_stage_verdicts(refused_pre_gate: bool, gate: Tri, failed: bool, expected: Verdict) -> None:
    result = decide_verdict(None, refused_pre_gate=refused_pre_gate, gate=gate, simulator_failed=failed)
    assert result.verdict is expected


@pytest.mark.parametrize(
    ("by_policy", "expected"),
    [
        (_policies(_stats()), Verdict.PASS_ROBUST),
        (_policies(_stats(margins=_bad(ROBUST_HAIRCUT))), Verdict.PASS_CONTINGENT),
        (_policies(_stats(margins=_bad(ROBUST_HAIRCUT, Comparator.COMPLETE_CASE))), Verdict.PASS_CONTINGENT),
        (_policies(_stats(margins={(Comparator.CONTROL, ROBUST_HAIRCUT): Refused("x")})), Verdict.NOT_PASS_REFUSED),
        (_policies(_stats(), _stats(t=Refused("ruin"))), Verdict.NOT_PASS_REFUSED),
        (_policies(_stats(t=-3.5, cohort=-4.5, diag=-0.003)), Verdict.UNDERPERFORMS_CONTROL_MONTHLY_HAC),
        (_policies(_stats(t=-1.0)), Verdict.FAIL_CONTROL_MONTHLY),
        (_policies(_stats(margins=_bad(CONTINGENT_HAIRCUT) | _bad(ROBUST_HAIRCUT))), Verdict.FAIL_ECONOMIC),
        (_policies(_stats(diag=-0.001)), Verdict.FAIL_DIAGNOSTIC),
        (_policies(_stats(t=2.0)), Verdict.UNDETERMINED_AT_THIS_POWER),
        (_policies(_stats(cohort=2.0)), Verdict.UNDETERMINED_AT_THIS_POWER),
        (_policies(_stats(t=2.0, margins=_bad(CONTINGENT_HAIRCUT))), Verdict.NOT_PASS_MIXED),
        (_policies(_stats(t=2.0, diag=-0.001)), Verdict.NOT_PASS_MIXED),
    ],
    ids=[
        "robust",
        "contingent-headline",
        "contingent-diagnostic",
        "robust-margin-refused",
        "one-policy-refused",
        "underperforms",
        "fail-control",
        "fail-economic",
        "fail-diagnostic",
        "undetermined-hac",
        "undetermined-cohort",
        "mixed-statistical-and-economic",
        "mixed-significance-and-diagnostic",
    ],
)
def test_verdict_table(by_policy: dict[str, PolicyStatistics], expected: Verdict) -> None:
    assert _verdict(by_policy) is expected


@pytest.mark.parametrize(
    ("stats", "expected"),
    [
        (_stats(t=3.0), Verdict.UNDETERMINED_AT_THIS_POWER),  # t > 3 is strict
        (_stats(cohort=Q), Verdict.UNDETERMINED_AT_THIS_POWER),  # cohort t > q is strict
        (_stats(diag=0.0), Verdict.FAIL_DIAGNOSTIC),  # mean(a′) > 0 is strict
        (_stats(t=0.0, mean=0.0, cohort=0.0), Verdict.FAIL_CONTROL_MONTHLY),  # mean(a) ≤ 0 includes 0
        (_stats(t=-3.0, cohort=-4.5), Verdict.FAIL_CONTROL_MONTHLY),  # t < −3 is strict
        (
            _stats(margins={(Comparator.CONTROL, ROBUST_HAIRCUT): HaircutMargins(0.0, 0.1)}),
            Verdict.PASS_CONTINGENT,
        ),  # margin > 0 is strict
    ],
    ids=["t-equals-3", "cohort-equals-q", "diagnostic-zero", "mean-zero", "t-equals-minus-3", "margin-zero"],
)
def test_equality_at_each_bar(stats: PolicyStatistics, expected: Verdict) -> None:
    assert _verdict(_policies(stats)) is expected


def test_bonferroni_is_evaluated_literally_where_its_term_binds() -> None:
    t_m = bonferroni_t(19)
    for family, t, expected in [
        (18, 3.004, Verdict.PASS_ROBUST),
        (19, 3.004, Verdict.UNDETERMINED_AT_THIS_POWER),
        (19, t_m - 1e-9, Verdict.UNDETERMINED_AT_THIS_POWER),
        (19, t_m + 1e-9, Verdict.PASS_ROBUST),
    ]:
        assert _verdict(_policies(_stats(t=t)), family) is expected, (family, t)


def test_an_invalid_family_size_refuses() -> None:
    conditions = evaluate_conditions(_policies(_stats()), 0)
    assert conditions[Condition.BONFERRONI].state is R
    assert conditions[Condition.BONFERRONI].binding is None
    assert _verdict(_policies(_stats()), 0) is Verdict.NOT_PASS_REFUSED


def test_all_policies_refused_leaves_the_binding_unavailable() -> None:
    by_policy = _policies(_stats(t=Refused("ruin"), cohort=Refused("ruin"), diag=Refused("ruin")))
    conditions = evaluate_conditions(by_policy, M)
    readout = conditions[Condition.BONFERRONI]
    assert (readout.state, readout.binding) == (R, None)
    assert set(readout.refusals) == set(by_policy)
    result = decide_verdict(conditions, refused_pre_gate=False, gate=T, simulator_failed=False)
    assert result.verdict is Verdict.NOT_PASS_REFUSED
    assert result.refused == (Condition.BONFERRONI, Condition.COHORT, Condition.DIAGNOSTIC_MEAN)


def test_false_plus_refused_composes_to_refused_and_excludes_the_refusal_from_the_extremum() -> None:
    by_policy = _policies(_stats(t=2.0), _stats(t=Refused("ruin")))
    readout = evaluate_conditions(by_policy, M)[Condition.BONFERRONI]
    assert readout.state is R
    assert readout.by_policy[CLASSIFIED_WORST.label] is R
    assert readout.binding == (CLASSIFIED_BEST.label, 2.0)  # ties go to the alphabetically first policy
    assert readout.refusals == {CLASSIFIED_WORST.label: "ruin"}
    assert _verdict(by_policy) is Verdict.NOT_PASS_REFUSED


def test_a_refused_governing_policy_still_reports_a_binding_value() -> None:
    by_policy = {
        ZERO_RECOVERY.label: _stats(t=Refused("ruin")),
        CLASSIFIED_WORST.label: _stats(t=3.2),
        CLASSIFIED_BEST.label: _stats(t=3.6),
    }
    readout = evaluate_conditions(by_policy, M)[Condition.BONFERRONI]
    assert readout.binding == (CLASSIFIED_WORST.label, 3.2)


def test_mixed_policy_signs_bind_in_each_predicate_direction() -> None:
    by_policy = _policies(_stats(t=-3.5), _stats(t=3.5))
    conditions = evaluate_conditions(by_policy, M)
    assert conditions[Condition.BONFERRONI].binding == (CLASSIFIED_BEST.label, -3.5)
    assert conditions[Condition.UNDERPERFORMS].binding == (CLASSIFIED_WORST.label, 3.5)
    assert conditions[Condition.UNDERPERFORMS].state is F
    assert conditions[Condition.MEAN_NOT_POSITIVE].state is F
    assert _verdict(by_policy) is Verdict.UNDETERMINED_AT_THIS_POWER


def test_compose_condition_direction() -> None:
    by_policy = _policies(_stats(t=3.1), _stats(t=4.0))

    def evaluate(stats: PolicyStatistics) -> tuple[float, bool] | Refused:
        assert isinstance(stats.hac, HacEstimate)
        return stats.hac.t_stat, True

    assert compose_condition(by_policy, evaluate, Direction.LOWER).binding == (CLASSIFIED_BEST.label, 3.1)
    assert compose_condition(by_policy, evaluate, Direction.UPPER).binding == (CLASSIFIED_WORST.label, 4.0)
    with pytest.raises(ValueError):
        compose_condition({CLASSIFIED_BEST.label: _stats()}, evaluate, Direction.LOWER)


@pytest.mark.parametrize(
    "weak",
    [
        _stats(t=2.9),
        _stats(cohort=3.0),
        _stats(diag=0.0),
        _stats(t=Refused("ruin")),
        _stats(
            margins=_bad(CONTINGENT_HAIRCUT, Comparator.COMPLETE_CASE) | _bad(ROBUST_HAIRCUT, Comparator.COMPLETE_CASE)
        ),
    ],
    ids=["hac-t", "cohort", "diagnostic-mean", "refused", "haircut"],
)
def test_one_weak_policy_can_only_remove_pass_eligibility(weak: PolicyStatistics) -> None:
    assert _verdict(_policies(_stats(), weak)) not in (Verdict.PASS_ROBUST, Verdict.PASS_CONTINGENT)


def test_verdict_lists_failed_gating_conditions() -> None:
    conditions = evaluate_conditions(_policies(_stats(t=2.0, margins=_bad(CONTINGENT_HAIRCUT))), M)
    result = decide_verdict(conditions, refused_pre_gate=False, gate=T, simulator_failed=False)
    assert result.failed == (
        Condition.BONFERRONI,
        margin_condition(Comparator.CONTROL, CONTINGENT_HAIRCUT, "beats_comparator"),
    )
    assert set(result.failed) <= set(GATING_CONDITIONS)


def test_verdict_requires_conditions_after_a_clean_gate() -> None:
    with pytest.raises(ValueError):
        decide_verdict(None, refused_pre_gate=False, gate=T, simulator_failed=False)
