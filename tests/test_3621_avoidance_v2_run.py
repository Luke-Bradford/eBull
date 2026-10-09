"""#3621 slice 5b-2b: v2's declaration gate, run ledger, stage-B gate, SI diagnostics and evaluation, on synthetic
inputs only. No artefact is read."""

from __future__ import annotations

import dataclasses
import gzip
import json
from datetime import date
from pathlib import Path
from typing import Any

import pytest

from app.services.avoidance_filters import FILTER_SETS_V2, Filter, MaxReading, NameFlags
from app.services.factor_book_path import HoldingReturn
from app.services.factor_book_series import ARMS
from app.services.factor_panel_fidelity import Verdict, read_ledger
from app.services.factor_panel_prices import HoldingStatus
from app.services.result_ledger import HoldoutAccess
from app.services.short_interest_flag import Reading, SiState, read_calendar_csv
from app.services.trial_register import TrialRegister
from scripts.load_3621_si import CALENDAR_PATH, FormationSi
from scripts.report_3609_step2 import PanelMonth, PanelName
from scripts.report_3609_step2_assembly import jsonable
from scripts.report_3621_fidelity import MaxFidelity
from scripts.report_3621_si import CONDITION_5, with_si
from scripts.run_3621_avoidance import RunError, Stage, verdict_lines
from scripts.run_3621_avoidance_v2 import (
    SEARCHES,
    STEP2_SUB,
    TRIAL_ID,
    audit_list,
    check_declaration,
    declaration_evidence,
    declared_row,
    declared_trial,
    evaluate,
    evidence_value,
    no_si,
    open_runs,
    require_undeclared,
    run,
    si_set_identity,
    stage_b_gate,
    unreported_zero,
)

LABELS = {
    "spec_sha256": "a" * 64,
    "construction_sha256": "b" * 64,
    "register_policy_sha256": "c" * 64,
    "python": "3.14",
    "stage_a_manifest_sha256": "d" * 64,
    "si_counts_sha256": "e" * 64,
}
V1_ROWS = [
    {"event": "declared", "trial_id": "3621-avoidance-filters-v1", "payload_sha256": "f" * 64},
    {"run_id": "v1run", "event": "started"},
    {"run_id": "v1run", "event": "completed", "trial_id": "3621-avoidance-filters-v1"},
]


def _declared(labels: dict[str, str] = LABELS) -> tuple[TrialRegister, list[dict[str, Any]]]:
    trial = declared_trial(declaration_evidence(labels))
    return TrialRegister("test", (trial,)), [*V1_ROWS, declared_row(trial)]


# --------------------------------------------------------------------------- the declaration


def test_searches_are_the_addendums_86_and_the_evidence_reads_back() -> None:
    assert SEARCHES == 86
    evidence = declaration_evidence(LABELS)
    assert all(evidence_value(evidence, label) == value for label, value in LABELS.items())


def test_the_gate_reads_only_v2s_rows_and_refuses_every_mismatch() -> None:
    register, committed = _declared()
    # v1's completed run shares the committed ledger and does not end v2.
    assert check_declaration(register, committed, LABELS).trial_id == TRIAL_ID
    with pytest.raises(RunError, match="construction_sha256"):
        check_declaration(register, committed, {**LABELS, "construction_sha256": "0" * 64})
    with pytest.raises(RunError, match="'declared' row"):
        check_declaration(register, V1_ROWS, LABELS)
    edited = dataclasses.replace(register.trials[0], description="edited after the pin")
    with pytest.raises(RunError, match="'declared' row"):
        check_declaration(TrialRegister("test", (edited,)), committed, LABELS)
    wrong = dataclasses.replace(register.trials[0], searches=62)
    with pytest.raises(RunError, match="86-search"):
        check_declaration(TrialRegister("test", (wrong,)), committed, LABELS)
    done = [
        *committed,
        {"run_id": "v2", "event": "started", "trial_id": TRIAL_ID},
        {"run_id": "v2", "event": "completed"},
    ]
    with pytest.raises(RunError, match="completed run"):
        check_declaration(register, done, LABELS)


def test_a_second_declaration_refuses() -> None:
    register, committed = _declared()
    require_undeclared(TrialRegister("test", ()), V1_ROWS)
    with pytest.raises(RunError, match="already declared"):
        require_undeclared(register, [])
    with pytest.raises(RunError, match="already declared"):
        require_undeclared(TrialRegister("test", ()), committed)


def test_open_runs_ignore_v1s_runs() -> None:
    rows = [{"run_id": "old", "event": "started"}, {"run_id": "v2", "event": "started", "trial_id": TRIAL_ID}]
    assert open_runs(rows) == ["v2"]
    assert open_runs([*rows, {"run_id": "v2", "event": "failed"}]) == []


def test_the_stage_b_gate_binds_step_2s_sub_and_records_v2s_code_hashes() -> None:
    register, committed = _declared()
    gate = stage_b_gate(register, lambda: dict(LABELS))
    assert gate.sub == STEP2_SUB
    assert gate.versions() == {
        k: LABELS[k] for k in ("spec_sha256", "construction_sha256", "register_policy_sha256", "python")
    }
    gate.check_declaration(committed)
    with pytest.raises(RunError):
        gate.check_declaration(V1_ROWS)


# --------------------------------------------------------------------------- the run's ledger


def _ledgers(tmp_path: Path) -> tuple[Path, Path, TrialRegister]:
    register, committed = _declared()
    tmp_path.mkdir(parents=True, exist_ok=True)
    committed_path = tmp_path / "committed.jsonl"
    committed_path.write_text("".join(json.dumps(row) + "\n" for row in committed))
    return tmp_path / "local" / "ledger.jsonl", committed_path, register


def _run(tmp_path: Path, build: Any, evaluate_run: Any, labels: dict[str, str] = LABELS, run_id: str = "r1") -> Any:
    ledger, committed, register = _ledgers(tmp_path)
    return ledger, lambda: run(
        run_id,
        head="abc",
        command=["x"],
        accessed_by="loop",
        record_access=lambda _: 41,
        build_stage_b=build,
        evaluate_run=evaluate_run,
        register=register,
        ledger=ledger,
        committed_ledger=committed,
        labels=lambda: dict(labels),
    )


def test_run_writes_started_access_stage_b_report_and_completed_in_order(tmp_path: Path) -> None:
    accesses: list[HoldoutAccess] = []
    ledger, committed, register = _ledgers(tmp_path)

    def record(access: HoldoutAccess) -> int:
        accesses.append(access)
        return 41

    def build(run_id: str) -> tuple[Path, str]:
        assert [r["event"] for r in read_ledger(ledger)] == ["started", "access_recorded"]  # access before any read
        with ledger.open("a") as handle:  # the builder writes its own row
            handle.write(json.dumps({"run_id": run_id, "event": "stage_b_published", "manifest_sha256": "9"}) + "\n")
        return tmp_path / "stageB", "9" * 64

    def evaluate_run(artefact: tuple[Path, str]) -> tuple[dict[str, Any], bytes]:
        assert artefact == (tmp_path / "stageB", "9" * 64)
        return {"verdict_lines": ["si|micro: ELIGIBLE"]}, gzip.compress(b"{}\n", mtime=0)

    row = run(
        "r1",
        head="abc",
        command=["x"],
        accessed_by="loop",
        record_access=record,
        build_stage_b=build,
        evaluate_run=evaluate_run,
        register=register,
        ledger=ledger,
        committed_ledger=committed,
        labels=lambda: dict(LABELS),
    )
    assert row["verdicts"] == ["si|micro: ELIGIBLE"] and row["stage_b_manifest_sha256"] == "9" * 64
    rows = read_ledger(ledger)
    assert [r["event"] for r in rows] == [
        "started",
        "access_recorded",
        "stage_b_published",
        "report_written",
        "completed",
    ]
    assert rows[0]["trial_id"] == TRIAL_ID and rows[1]["access_id"] == 41
    assert (accesses[0].strategy_version, accesses[0].result_version) == ("v2", "r1")
    assert "v2 declared run r1" in accesses[0].purpose


def test_a_gate_refusal_writes_nothing(tmp_path: Path) -> None:
    def never(_: str) -> tuple[Path, str]:
        raise AssertionError("no stage-B build before the gate passes")

    ledger, attempt = _run(tmp_path, never, lambda _: ({}, b""), labels={**LABELS, "python": "3.13"})
    with pytest.raises(RunError):
        attempt()
    assert not ledger.exists()


def test_a_builder_that_ended_the_run_is_not_ended_twice_and_one_that_did_not_is_ended(tmp_path: Path) -> None:
    def ended(run_id: str) -> tuple[Path, str]:
        with ledger.open("a") as handle:
            handle.write(json.dumps({"run_id": run_id, "event": "failed", "step": "stage_b_published"}) + "\n")
        raise RuntimeError("build failed")

    ledger, attempt = _run(tmp_path, ended, lambda _: ({}, b""))
    with pytest.raises(RuntimeError, match="build failed"):
        attempt()
    assert [r["event"] for r in read_ledger(ledger)] == ["started", "access_recorded", "failed"]

    def refused(run_id: str) -> tuple[Path, str]:
        raise RunError("the builder's gate refused")

    ledger2, attempt2 = _run(tmp_path / "b", refused, lambda _: ({}, b""))
    with pytest.raises(RunError, match="refused"):
        attempt2()
    rows = read_ledger(ledger2)
    assert [r["event"] for r in rows] == ["started", "access_recorded", "failed"]
    assert rows[-1]["step"] == "stage_b_published"


# --------------------------------------------------------------------------- the SI diagnostics


JUNE_15 = date(2021, 6, 15)


def test_unreported_zero_pads_unmatched_names_into_n() -> None:
    readings = {
        1: Reading(SiState.VALID, JUNE_15, 0.5),
        2: Reading(SiState.VALID, JUNE_15, 0.3),
        **{k: Reading(SiState.UNMATCHED) for k in range(3, 23)},
    }
    # Two values: q = the 2nd smallest (0.5). With 20 zeros in N = 22: q = the 20th smallest = 0.0, so 0.3 flags too.
    assert unreported_zero(readings, 0.5) == (0.0, 1)


def test_stage_a_resolving_to_a_settlement_refuses() -> None:
    calendar = read_calendar_csv(CALENDAR_PATH.read_text())
    month = PanelMonth(date(2021, 4, 30), date(2021, 4, 30), {}, {}, {})
    no_si(calendar, [month])
    with pytest.raises(RunError, match="resolve to an SI settlement"):
        no_si(calendar, [dataclasses.replace(month, formation=date(2021, 6, 30), session=date(2021, 6, 30))])


def _panel(series: dict[int, int]) -> PanelMonth:
    def name(sid: int) -> PanelName:
        return PanelName(sid, 1.0, "OTHER", {}, HoldingReturn(HoldingStatus.OBSERVED, dict.fromkeys(ARMS, 0.0)), None)

    return PanelMonth(date(2021, 6, 30), date(2021, 6, 30), {k: name(s) for k, s in series.items()}, {}, {})


def test_the_audit_list_keeps_series_whose_matched_issue_name_changes() -> None:
    m1, m2 = _panel({1: 10, 2: 20}), _panel({1: 10, 2: 20})
    later = date(2021, 7, 15)
    si = [
        FormationSi(
            JUNE_15,
            {
                1: Reading(SiState.VALID, JUNE_15, 0.1, issue_name="Radius Health", log_ratio=0.0),
                2: Reading(SiState.VALID, JUNE_15, 0.1, issue_name="Stable Inc", log_ratio=0.0),
            },
            0.1,
            frozenset(),
        ),
        FormationSi(
            later,
            {
                1: Reading(SiState.IDENTITY_FAIL, later, issue_name="Schnitzer Steel", log_ratio=1.0),
                2: Reading(SiState.VALID, later, 0.1, issue_name="STABLE INC.", log_ratio=0.0),
            },
            0.1,
            frozenset(),
        ),
    ]
    (entry,) = audit_list([m1, m2], si)
    assert entry["series_id"] == 10 and entry["accepted_change"] is False
    assert [(n["name"], n["first"], n["state"]) for n in entry["names"]] == [
        ("RADIUSHEALTH", JUNE_15, "valid"),
        ("SCHNITZERSTEEL", later, "identity_fail"),
    ]


# --------------------------------------------------------------------------- the evaluation


FORMATIONS = [date(2021, 4, 30), date(2021, 5, 28), date(2021, 6, 30)]
CUTOFFS = (300.0, 600.0, 900.0)
N = 1000
READING = MaxReading(0.01, 20, 0, None)


def _flags_for(name: int) -> NameFlags:
    flagged = {Filter.MAX} if name % 10 == 0 else set()
    if name % 7 == 0:
        flagged.add(Filter.SUB5)
    return NameFlags(READING, frozenset(flagged))


def _si(*, unmatched: range = range(0)) -> FormationSi:
    """Formation 2021-06: name n has SIR n / N; the top decile (n >= 900) flags, except ``unmatched`` names."""
    readings = {
        n: Reading(SiState.UNMATCHED) if n in unmatched else Reading(SiState.VALID, JUNE_15, n / N) for n in range(N)
    }
    values = sorted(r.sir for r in readings.values() if r.sir is not None)
    q = values[-(-9 * len(values) // 10) - 1]
    return FormationSi(
        JUNE_15, readings, q, frozenset(n for n, r in readings.items() if r.sir is not None and r.sir >= q)
    )


def _stage(si: FormationSi) -> Stage:
    def name(n: int) -> PanelName:
        r = -0.3 if n >= 900 else 0.01  # the SI-flagged names lose
        return PanelName(n, 1.0 + n, "OTHER", {}, HoldingReturn(HoldingStatus.OBSERVED, dict.fromkeys(ARMS, r)), None)

    months = [
        PanelMonth(
            f,
            f,
            {n: name(n) for n in range(N)},
            dict.fromkeys(range(N), 20.0),
            dict.fromkeys(range(N), date(2010, 1, 1)),
        )
        for f in FORMATIONS
    ]
    flags = {n: _flags_for(n) for n in range(N)}
    return Stage(
        months=months,
        flags=[flags, flags, with_si(flags, si.flagged)],
        readings=[{n: f.reading for n, f in flags.items()} for _ in months],
        cutoffs=dict.fromkeys(FORMATIONS, CUTOFFS),
        symbols={n: f"S{n}" for n in range(N)},
    )


def test_evaluate_gives_42_verdicts_condition_5_and_the_si_increment() -> None:
    fid = MaxFidelity(Verdict.PASS, {}, 0)
    si = _si()
    result = evaluate([_stage(si)], [[None, None, si]], fid)
    pairs = result["pairs"]
    assert len(pairs) == len(FILTER_SETS_V2) * 7
    verdicts = {k: b["verdict"] for k, b in pairs.items() if b["verdict"] is not None}
    assert len(verdicts) == 42
    assert verdicts["si|top1000"].delta_g[("whole", "worst_case", "stress_2x")] > 0
    assert pairs["si|micro"]["condition_5_shortfalls"] == [] and CONDITION_5 not in verdicts["si|top1000"].failed
    assert set(pairs["si|top1000"]["valueless_weight"]) == {date(2021, 6, 30)}
    assert set(result["si_increment"]) == {"micro", "small", "large", "mega", "top1000", "rest", "all"}
    assert len(verdict_lines(pairs, fid)) == 43
    json.dumps(jsonable({k: v for k, v in result.items() if k != "excluded"}), allow_nan=False)


def test_a_population_under_the_coverage_floor_is_not_eligible_on_its_si_sets() -> None:
    fid = MaxFidelity(Verdict.PASS, {}, 0)
    si = _si(unmatched=range(0, 60))  # 60 of micro's 299 names (ME = 1 + n < 300) have no value: 79.9% < 90%
    pairs = evaluate([_stage(si)], [[None, None, si]], fid)["pairs"]
    assert pairs["si|micro"]["condition_5_shortfalls"] == [date(2021, 6, 30)]
    micro = pairs["si|micro"]["verdict"]
    assert micro.verdict.value in ("NOT ELIGIBLE", "NO_EFFECT", "REFUSED")
    if micro.verdict.value == "NOT ELIGIBLE":
        assert CONDITION_5 in micro.failed
    assert pairs["max|micro"].get("condition_5_shortfalls") is None


def test_the_si_set_refuses_an_exclusion_before_2021_06() -> None:
    si = _si()
    stage = _stage(si)
    early = with_si(stage.flags[0], frozenset({5}))
    members = [frozenset(range(N))] * 3
    si_set_identity(stage.months, members, stage.flags)
    with pytest.raises(RunError, match="before"):
        si_set_identity(stage.months, members, [early, *stage.flags[1:]])
