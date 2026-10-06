"""#3609 step 1 slice 4: the fidelity comparison, registration gate, ledger and alignment diagnostics.

Spec: ``docs/research/2026-10-04-3609-step1-factor-panel.md`` §"Fidelity", §"Registration" and the slice 4 plan.
Every expected value is computed by hand in the test, never by the code under test.
"""

from __future__ import annotations

import json
import random
import statistics
from dataclasses import replace
from datetime import date
from pathlib import Path

import pytest

from app.services.factor_panel_artefact import import_closure
from app.services.factor_panel_fidelity import (
    ARMS,
    FIDELITY_SEARCHES,
    Bar,
    FidelityError,
    Form25Record,
    Holding,
    SeriesAccumulator,
    Verdict,
    append_ledger,
    characteristic_verdict,
    compare_arm,
    delayed_name_months,
    evidence_sha256,
    factor_month,
    fidelity_evidence,
    form25_table,
    read_ledger,
    register_gate,
    shift_month,
    version_history,
)
from app.services.trial_register import TRIAL_REGISTER, DeclaredTrial, TrialExactness, TrialRegister

CUT = 100.0  # micro cutoff (USD)
CAP = 1_000.0  # weight cap (USD)


def _h(key: int, value: float, me: float, best: float, worst: float | None = None, status: str = "observed") -> Holding:
    return Holding(key, value, me, status, {"best_case": best, "worst_case": best if worst is None else worst})


def _fifteen(low_returns: list[float], high_returns: list[float], mes: list[float]) -> list[Holding]:
    """15 non-micro names: values 1..15, so low = keys 1-5, middle = 6-10, high = 11-15."""
    out = [_h(i + 1, float(i + 1), mes[i], low_returns[i]) for i in range(5)]
    out += [_h(i + 6, float(i + 6), 500.0, 0.0) for i in range(5)]
    out += [_h(i + 11, float(i + 11), mes[i + 5], high_returns[i]) for i in range(5)]
    return out


# --------------------------------------------------------------------------- one formation


def test_capped_value_weights_and_the_sign_by_hand() -> None:
    # Low leg: MEs 200, 400, 600, 800, 3000 -> capped weights 200, 400, 600, 800, 1000 (sum 3000).
    # Returns 0.03, 0, 0, 0, 0.06 -> (200*0.03 + 1000*0.06) / 3000 = (6 + 60) / 3000 = 0.022.
    # High leg: MEs all 500 (sum 2500), returns 0.01, 0.02, 0.03, 0.04, 0.05 -> mean 0.03.
    mes = [200.0, 400.0, 600.0, 800.0, 3000.0] + [500.0] * 5
    holdings = _fifteen([0.03, 0, 0, 0, 0.06], [0.01, 0.02, 0.03, 0.04, 0.05], mes)
    got = factor_month(holdings, CUT, CAP, 1)
    assert (got.low_n, got.high_n) == (5, 5)
    assert got.returns is not None
    assert got.returns["best_case"] == pytest.approx(0.03 - 0.022)
    flipped = factor_month(holdings, CUT, CAP, -1)
    assert flipped.returns is not None and flipped.returns["best_case"] == pytest.approx(0.022 - 0.03)


def test_arms_are_computed_separately() -> None:
    holdings = _fifteen([0.0] * 5, [0.0] * 5, [500.0] * 10)
    holdings[14] = _h(15, 15.0, 500.0, 0.10, -0.50, "terminal")  # one high-leg name: best +10%, worst -50%
    got = factor_month(holdings, CUT, CAP, 1)
    assert got.returns is not None
    assert got.returns["best_case"] == pytest.approx(0.10 / 5)
    assert got.returns["worst_case"] == pytest.approx(-0.50 / 5)
    assert got.status_shares is not None
    assert got.status_shares["high"] == pytest.approx({"observed": 0.8, "terminal": 0.2, "coverage_exit": 0.0})
    assert got.status_shares["low"]["observed"] == 1.0


def test_a_four_name_leg_is_undersized_and_five_is_not() -> None:
    # 12 non-micro names: thirds of 4 -> undersized.
    twelve = [_h(i, float(i), 500.0, 0.0) for i in range(12)]
    got = factor_month(twelve, CUT, CAP, 1)
    assert (got.low_n, got.high_n, got.returns) == (4, 4, None)
    fifteen = [_h(i, float(i), 500.0, 0.0) for i in range(15)]
    assert factor_month(fifteen, CUT, CAP, 1).returns is not None


def test_micro_names_join_legs_on_the_breakpoints_and_all_micro_is_undersized() -> None:
    # 12 non-micro (thirds of 4) + 2 micro at the extremes: legs reach 5.
    names = [_h(i, float(i), 500.0, 0.0) for i in range(12)]
    names += [_h(100, -1.0, 50.0, 0.0), _h(101, 99.0, 50.0, 0.0)]
    got = factor_month(names, CUT, CAP, 1)
    assert (got.low_n, got.high_n) == (5, 5)
    all_micro = [_h(i, float(i), 50.0, 0.0) for i in range(30)]
    assert factor_month(all_micro, CUT, CAP, 1).returns is None


def test_a_cutoff_is_applied_in_usd_as_given() -> None:
    # The same names with a cutoff above every ME are all micro: no breakpoints, so undersized.
    names = [_h(i, float(i), 500.0, 0.0) for i in range(15)]
    assert factor_month(names, 600.0, CAP, 1).returns is None


@pytest.mark.parametrize(
    ("holding", "code"),
    [
        (_h(99, 1.0, 500.0, float("nan")), "arm_return"),
        (Holding(99, 1.0, 500.0, "observed", {"best_case": 0.0}), "arm_return"),
        (_h(99, 1.0, 0.0, 0.0), "me"),
        (_h(99, 1.0, 500.0, 0.0, status="mystery"), "holding_status"),
    ],
)
def test_a_bad_holding_refuses_without_its_value_in_the_message(holding: Holding, code: str) -> None:
    names = [_h(i, float(i), 500.0, 0.0123456) for i in range(15)] + [holding]
    with pytest.raises(FidelityError) as caught:
        factor_month(names, CUT, CAP, 1)
    assert caught.value.code == code
    assert "0.0123456" not in str(caught.value) and "nan" not in str(caught.value)


def test_a_missing_cutoff_or_sign_refuses() -> None:
    names = [_h(i, float(i), 500.0, 0.0) for i in range(15)]
    with pytest.raises(FidelityError, match="cutoff"):
        factor_month(names, float("nan"), CAP, 1)
    with pytest.raises(FidelityError, match="sign"):
        factor_month(names, CUT, CAP, 0)


# --------------------------------------------------------------------------- comparison

GRID = [shift_month("2014-10", i) for i in range(80)]


def _wave(n: int, seed: int = 1) -> list[float]:
    """A fixed pseudo-random monthly series: little autocorrelation, so lag and lead correlations stay small."""
    rng = random.Random(seed)
    return [rng.gauss(0.0, 0.01) for _ in range(n)]


def test_the_grid_is_the_80_stage_a_holding_months() -> None:
    assert (GRID[0], GRID[-1], len(GRID)) == ("2014-10", "2021-05", 80)


def test_identical_series_pass_with_unit_beta_and_zero_te() -> None:
    pub = dict(zip(GRID, _wave(80), strict=True))
    got = compare_arm(pub, pub, GRID, 0, 0.8)
    assert got.verdict is Verdict.PASS and got.failed_bars == ()
    assert got.beta == pytest.approx(1.0) and got.te_ratio == pytest.approx(0.0) and got.offset_ok is True
    assert got.pairs == {"contemporaneous": 80, "lag": 79, "lead": 79}


def test_lag_and_lead_pair_across_missing_months_and_stay_on_the_grid() -> None:
    pub = dict(zip(GRID, _wave(80), strict=True))
    ours = {m: v for m, v in pub.items() if m != "2016-03"}
    published = {m: v for m, v in pub.items() if m != "2018-07"}
    published["2014-09"] = 0.5  # off the grid: never a lag partner
    published["2021-06"] = 0.5  # off the grid: never a lead partner
    got = compare_arm(ours, published, GRID, 0, 0.8)
    # contemporaneous: 80 - 1 (ours) - 1 (published) = 78.
    # lag (m, m-1): 79 grid pairs; drop m=2016-03 (ours) and m=2018-08 (published 2018-07) -> 77.
    # lead (m, m+1): 79 grid pairs; drop m=2016-03 (ours) and m=2018-06 (published 2018-07) -> 77.
    assert got.pairs == {"contemporaneous": 78, "lag": 77, "lead": 77}


def test_fifty_nine_pairs_are_insufficient_and_sixty_are_not() -> None:
    pub = dict(zip(GRID, _wave(80), strict=True))
    sixty = dict(list(pub.items())[:61])  # ours on the first 61 months: lag pairs = 60 (the first has no m-1)
    assert compare_arm(sixty, pub, GRID, 0, 0.8).verdict is Verdict.PASS
    fifty_nine = dict(list(pub.items())[:60])  # lag pairs = 59; contemporaneous and lead stay at 60
    got = compare_arm(fifty_nine, pub, GRID, 0, 0.8)
    assert got.verdict is Verdict.INSUFFICIENT
    assert got.insufficient == ("lag_pairs",)
    assert got.pairs == {"contemporaneous": 60, "lag": 59, "lead": 60}
    assert got.offset_ok is None and got.beta is None


def test_zero_variance_is_insufficient() -> None:
    pub = dict(zip(GRID, _wave(80), strict=True))
    flat = dict.fromkeys(GRID, 0.01)
    got = compare_arm(flat, pub, GRID, 0, 0.8)
    assert got.verdict is Verdict.INSUFFICIENT and "contemporaneous_zero_variance" in got.insufficient


def _shifted(scale: float, shift: float, noise: float = 0.0) -> dict[str, float]:
    base = _wave(80)
    other = _wave(80, seed=2)
    return {m: scale * b + shift + noise * o for m, b, o in zip(GRID, base, other, strict=True)}


def test_each_bar_failing_alone_names_only_itself() -> None:
    pub = _shifted(1.0, 0.0)
    assert compare_arm(_shifted(1.5, 0.0), pub, GRID, 0, 0.8).failed_bars == (Bar.BETA,)
    assert compare_arm(_shifted(1.0, 0.003), pub, GRID, 0, 0.8).failed_bars == (Bar.OFFSET,)
    assert compare_arm(_shifted(1.0, 0.0), pub, GRID, 3, 0.8).failed_bars == (Bar.UNDERSIZED,)
    # Noise orthogonal to the published series and demeaned lowers the correlation below 0.80 while beta stays 1
    # and the offset 0: e = other - mean(other) - (cov(other, pub) / var(pub)) x (pub - mean(pub)).
    p = [pub[m] for m in GRID]
    other = _wave(80, seed=2)
    slope = statistics.covariance(other, p) / statistics.variance(p)
    e = [o - statistics.fmean(other) - slope * (x - statistics.fmean(p)) for o, x in zip(other, p, strict=True)]
    noisy = {m: x + 1.2 * n for m, x, n in zip(GRID, p, e, strict=True)}
    got = compare_arm(noisy, pub, GRID, 0, 0.8)
    assert got.beta == pytest.approx(1.0) and got.offset_ok is True
    assert got.failed_bars == (Bar.CORRELATION,)


def test_lead_lag_fails_when_ours_is_published_a_month_late() -> None:
    pub = _shifted(1.0, 0.0)
    late = {m: pub[shift_month(m, 1)] for m in GRID if shift_month(m, 1) in pub}
    late[GRID[-1]] = 0.0
    got = compare_arm(late, pub, GRID, 0, 0.0)
    assert Bar.LEAD_LAG in got.failed_bars


def test_values_exactly_at_the_bars_pass() -> None:
    pub = _shifted(1.0, 0.0)
    assert compare_arm(_shifted(1.3, 0.0), pub, GRID, 2, 0.8).failed_bars == ()
    assert compare_arm(_shifted(0.7, 0.0), pub, GRID, 2, 0.8).failed_bars == ()
    edge = compare_arm(_shifted(1.0, 0.0025), pub, GRID, 0, 0.8)  # |12 x 0.0025| = 0.03 exactly
    assert edge.offset_ok is True
    same = compare_arm(pub, pub, GRID, 0, 1.0)  # correlation 1.0 against a bar of 1.0
    assert same.failed_bars == ()


def test_a_constant_shift_moves_only_the_offset_boolean() -> None:
    pub = _shifted(1.0, 0.0, noise=0.2)
    ours = _shifted(1.1, 0.0)
    plain = compare_arm(ours, pub, GRID, 0, 0.8)
    moved = compare_arm({m: v + 0.01 for m, v in ours.items()}, pub, GRID, 0, 0.8)
    assert moved.correlation == pytest.approx(plain.correlation)
    assert moved.beta == pytest.approx(plain.beta) and moved.te_ratio == pytest.approx(plain.te_ratio)
    assert (plain.offset_ok, moved.offset_ok) == (True, False)


def test_the_offset_magnitude_is_never_in_the_result() -> None:
    pub = _shifted(1.0, 0.0)
    got = compare_arm(_shifted(1.0, 0.004), pub, GRID, 0, 0.8).as_json()
    assert set(got) == {
        "verdict", "failed_bars", "insufficient", "pairs", "correlation", "beta", "te_ratio", "offset_ok", "undersized"
    }  # fmt: skip
    assert "0.048" not in json.dumps(got)  # 12 x 0.004


def test_one_failing_or_insufficient_arm_decides_the_characteristic() -> None:
    pub = _shifted(1.0, 0.0)
    passing = compare_arm(pub, pub, GRID, 0, 0.8)
    failing = compare_arm(_shifted(2.0, 0.0), pub, GRID, 0, 0.8)
    short = compare_arm(dict(list(pub.items())[:10]), pub, GRID, 0, 0.8)
    assert characteristic_verdict({"best_case": passing, "worst_case": passing}) is Verdict.PASS
    assert characteristic_verdict({"best_case": passing, "worst_case": failing}) is Verdict.FAIL
    assert characteristic_verdict({"best_case": failing, "worst_case": short}) is Verdict.INSUFFICIENT


def test_the_accumulator_counts_empty_and_undersized_months_on_the_grid() -> None:
    acc = SeriesAccumulator(GRID)
    pub = _shifted(1.0, 0.0)
    names = [_h(i, float(i), 500.0, 0.0) for i in range(15)]
    for m in GRID[:77]:
        acc.add(m, replace(factor_month(names, CUT, CAP, 1), returns={arm: pub[m] for arm in ARMS}))
    acc.add(GRID[77], factor_month(names[:12], CUT, CAP, 1))  # undersized
    result = acc.result(pub, "be_me")  # GRID[78], GRID[79] never added: no names at all
    assert result["undersized"] == 3
    assert result["missing_ours"] == GRID[77:]
    assert result["arms"]["best_case"]["failed_bars"] == ["undersized"]
    with pytest.raises(FidelityError, match="grid"):
        acc.add(GRID[0], factor_month(names, CUT, CAP, 1))


# --------------------------------------------------------------------------- registration

SPEC = "a" * 64
DIGEST = "b" * 64


def _trial(trial_id: str = "3609-step1-fidelity-v1", digest: str = DIGEST, **changes: object) -> DeclaredTrial:
    fields: dict[str, object] = {
        "trial_id": trial_id,
        "description": "fidelity",
        "evidence": fidelity_evidence(SPEC, digest),
        "exactness": TrialExactness.EXACT,
        "searches": FIDELITY_SEARCHES,
        **changes,
    }
    return DeclaredTrial(**fields)  # type: ignore[arg-type]


def _register(*trials: DeclaredTrial) -> TrialRegister:
    return TrialRegister(version="test", trials=trials)


def test_the_gate_returns_the_one_matching_entry() -> None:
    trial = _trial()
    assert register_gate(_register(trial), SPEC, DIGEST, []) is trial


@pytest.mark.parametrize(
    ("trial", "spec", "digest"),
    [
        (_trial(), "c" * 64, DIGEST),  # another spec
        (_trial(), SPEC, "c" * 64),  # another versions digest
        (_trial(searches=8), SPEC, DIGEST),
        (_trial(exactness=TrialExactness.FLOOR), SPEC, DIGEST),
        (_trial(trial_id="something-else"), SPEC, DIGEST),
    ],
)
def test_the_gate_refuses_a_wrong_entry(trial: DeclaredTrial, spec: str, digest: str) -> None:
    with pytest.raises(FidelityError):
        register_gate(_register(trial), spec, digest, [])


def test_the_gate_refuses_a_changed_past_entry_and_a_reused_trial() -> None:
    v1 = _trial()
    ran = {"trial_id": v1.trial_id, "trial_evidence_sha256": evidence_sha256(v1), "versions_digest": DIGEST}
    assert register_gate(_register(v1), SPEC, DIGEST, [ran]) is v1  # an identical re-run is the same search
    moved = _trial(digest="d" * 64)  # v1's evidence rewritten to name a new version
    with pytest.raises(FidelityError, match="moved"):
        register_gate(_register(moved), SPEC, "d" * 64, [ran])
    v2 = _trial("3609-step1-fidelity-v2", digest="d" * 64)
    assert register_gate(_register(v1, v2), SPEC, "d" * 64, [ran]) is v2
    v2_reusing = _trial("3609-step1-fidelity-v2")  # a second entry naming v1's digest
    with pytest.raises(FidelityError, match="2 register entries"):
        register_gate(_register(v1, v2_reusing), SPEC, DIGEST, [ran])
    two_digests = _trial(evidence=fidelity_evidence(SPEC, DIGEST) + fidelity_evidence(SPEC, "d" * 64))
    with pytest.raises(FidelityError, match="exactly one"):
        register_gate(_register(two_digests), SPEC, DIGEST, [])


def test_the_real_register_holds_one_fidelity_entry_of_sixteen_searches() -> None:
    (entry,) = [t for t in TRIAL_REGISTER.trials if t.trial_id.startswith("3609-step1-fidelity-v")]
    assert (entry.trial_id, entry.searches, entry.exactness, entry.declared_for) == (
        "3609-step1-fidelity-v1",
        16,
        TrialExactness.EXACT,
        None,
    )
    assert "spec_sha256=" in entry.evidence and "construction_versions_sha256=" in entry.evidence


def test_three_versions_without_a_pass_reject_and_insufficient_counts() -> None:
    rows = [
        {"versions_digest": d, "verdicts": {"be_me": v, "gp_at": "PASS"}}
        for d, v in (("v1", "FAIL"), ("v2", "INSUFFICIENT"), ("v2", "INSUFFICIENT"), ("v3", "FAIL"))
    ]
    got = version_history(rows)
    assert got["be_me"] == {"versions": 3, "rejected": True}
    assert got["gp_at"] == {"versions": 3, "rejected": False}
    assert version_history(rows[:3])["be_me"] == {"versions": 2, "rejected": False}


# --------------------------------------------------------------------------- ledger


def test_the_ledger_appends_whole_rows_and_dedups_a_committed_copy(tmp_path: Path) -> None:
    local = tmp_path / "ledger.jsonl"
    append_ledger(local, {"run_id": "r1", "event": "started"})
    append_ledger(local, {"run_id": "r1", "event": "completed"})
    committed = tmp_path / "committed.jsonl"
    committed.write_text(local.read_text().splitlines()[0] + "\n")
    rows = read_ledger(committed, local, tmp_path / "absent.jsonl")
    assert [(r["run_id"], r["event"]) for r in rows] == [("r1", "started"), ("r1", "completed")]


def test_a_run_records_started_before_its_gates_and_failed_without_the_message(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import argparse

    import scripts.report_3609_fidelity as report

    monkeypatch.setattr(report, "is_dirty", lambda: False)
    monkeypatch.setattr(report, "head_commit", lambda: "f" * 40)

    def refuse(*_: object) -> None:
        raise FidelityError("artefact_versions", "secret 0.0421")

    monkeypatch.setattr(report, "verify_artefact", refuse)
    ledger = tmp_path / "ledger.jsonl"
    args = argparse.Namespace(artefact=tmp_path, manifest_sha256="0" * 64, census_form25=False, revision_reason=None)
    with pytest.raises(FidelityError):
        report.run(args, ["report"], ledger)
    started, failed = (json.loads(line) for line in ledger.read_text().splitlines())
    assert started["event"] == "started" and started["run_id"] == failed["run_id"]
    spec_sha256, versions = report.current_versions()
    assert (started["spec_sha256"], started["versions"]) == (spec_sha256, versions)  # pinned even when a gate fails
    assert failed == {
        "run_id": started["run_id"],
        "event": "failed",
        "error_class": "FidelityError",
        "code": "artefact_versions",
        "evaluation_began": False,
    }
    assert "0.0421" not in ledger.read_text()


def test_a_dirty_checkout_refuses_before_any_ledger_row(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import argparse

    import scripts.report_3609_fidelity as report

    monkeypatch.setattr(report, "is_dirty", lambda: True)
    args = argparse.Namespace(artefact=tmp_path, manifest_sha256="0" * 64, census_form25=False, revision_reason=None)
    with pytest.raises(FidelityError, match="dirty"):
        report.run(args, ["report"], tmp_path / "ledger.jsonl")
    assert not (tmp_path / "ledger.jsonl").exists()


# --------------------------------------------------------------------------- closure and diagnostics


def test_the_unhashed_file_keeps_its_imports_in_the_closure(tmp_path: Path) -> None:
    for relative, text in {
        "app/__init__.py": "",
        "app/services/__init__.py": "",
        "app/services/register.py": "from app.services.only_via_register import x\n",
        "app/services/only_via_register.py": "",
        "scripts/__init__.py": "",
        "scripts/helper.py": "",
        "scripts/root.py": "from app.services import register\nfrom scripts.helper import y\n",
    }.items():
        (tmp_path / relative).parent.mkdir(parents=True, exist_ok=True)
        (tmp_path / relative).write_text(text)
    got = import_closure([tmp_path / "scripts/root.py"], tmp_path, unhashed=frozenset({"app/services/register.py"}))
    assert sorted(got) == [
        "app/__init__.py",
        "app/services/__init__.py",
        "app/services/only_via_register.py",
        "scripts/__init__.py",
        "scripts/helper.py",
        "scripts/root.py",
    ]


def test_the_real_construction_sources_hash_the_report_and_not_the_register() -> None:
    from scripts.build_3609_factor_panel import construction_sources

    got = construction_sources()
    assert "scripts/report_3609_fidelity.py" in got and "app/services/factor_panel_fidelity.py" in got
    assert "app/services/trial_register.py" not in got
    assert "app/services/deflated_sharpe.py" in got  # reached through the register, still hashed


def test_delayed_name_months_is_inferred_from_later_use() -> None:
    periods = {
        # At 2019-06-30 the 2019-03-31 quarter was used; 2019-01-31... a later row uses 2019-02-28, which was
        # lag-eligible at 2019-06-30 (2019-02-28 + 4 months = 2019-06-28 <= 2019-06-30): delayed.
        1: [(date(2019, 6, 30), date(2018, 12, 31)), (date(2019, 7, 31), date(2019, 2, 28))],
        # 2019-03-31 + 4 months = 2019-07-31 > 2019-06-30: not yet eligible, so not delayed.
        2: [(date(2019, 6, 30), date(2018, 12, 31)), (date(2019, 7, 31), date(2019, 3, 31))],
        # Missing at M, a later row uses an eligible period: delayed.
        3: [(date(2019, 6, 30), None), (date(2019, 8, 31), date(2019, 1, 31))],
        # The later row uses the same period: not delayed.
        4: [(date(2019, 6, 30), date(2019, 1, 31)), (date(2019, 7, 31), date(2019, 1, 31))],
    }
    assert delayed_name_months(periods) == 2


def test_form25_matching_strips_a_trailing_q_run_and_buckets_by_filing_year() -> None:
    records = [
        Form25Record("1", "CRCQQ", date(2020, 7, 31), date(2020, 7, 31)),  # CRC, 10 days off
        Form25Record("2", "ABC", date(2020, 3, 2), date(2020, 2, 28)),  # exact ABC and stripped ABC: same
        Form25Record("3", "NONE", date(2021, 1, 4), date(2021, 1, 4)),  # no series
        Form25Record("4", "XYZQ", date(2021, 6, 1), date(2021, 6, 1)),  # XYZ ends 90 days off
    ]
    last_bars = {"CRC": [date(2020, 8, 10)], "ABC": [date(2020, 2, 27)], "XYZ": [date(2021, 3, 3)]}
    assert form25_table(records, last_bars) == [
        {"year": 2020, "records": 2, "with_series": 2, "within_window": 2},
        {"year": 2021, "records": 2, "with_series": 1, "within_window": 0},
    ]


def test_a_reason_absent_from_a_formation_counts_as_zero_me_share() -> None:
    from scripts.report_3609_fidelity import mean_me_share

    by_formation = {
        "2019-05-31": {"reit": {"count": 3, "me_share": 0.04}},
        "2019-06-30": {"admitted": {"count": 9, "me_share": 0.96}},
        "2019-07-31": {"reit": {"count": 1, "me_share": 0.02}, "not_priced": {"count": 5, "me_share": None}},
    }
    assert mean_me_share(by_formation, "reit") == pytest.approx((0.04 + 0.0 + 0.02) / 3)
    assert mean_me_share(by_formation, "not_priced") is None
