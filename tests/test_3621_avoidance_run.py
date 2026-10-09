"""#3621 slice 4: the declaration gate, the run's ledger, the reproduction check and the evaluation, on synthetic inputs
only. No artefact is read."""

from __future__ import annotations

import dataclasses
import gzip
import json
from datetime import date
from pathlib import Path
from typing import Any

import pytest

from app.services.avoidance_filters import FILTER_SETS, Filter, MaxMissing, MaxReading, MaxSeries, NameFlags
from app.services.factor_book_path import HoldingReturn
from app.services.factor_book_series import ARMS
from app.services.factor_panel_fidelity import Verdict, read_ledger
from app.services.factor_panel_prices import DailyBar, HoldingStatus
from app.services.result_ledger import HoldoutAccess
from app.services.trial_register import TrialRegister
from scripts.report_3609_step2 import PanelMonth, PanelName
from scripts.report_3609_step2_assembly import jsonable
from scripts.report_3621_fidelity import MaxFidelity
from scripts.run_3621_avoidance import (
    SEARCHES,
    TRIAL_ID,
    RunError,
    Stage,
    check_declaration,
    declaration_evidence,
    declared_row,
    declared_trial,
    evaluate,
    evidence_value,
    max_readings,
    module_counts,
    read_nyse_cutoffs,
    read_published_max,
    reproduce,
    require_undeclared,
    run,
    verdict_lines,
)

LABELS = {
    "spec_sha256": "a" * 64,
    "construction_sha256": "b" * 64,
    "register_policy_sha256": "c" * 64,
    "python": "3.14",
    "stage_a_manifest_sha256": "d" * 64,
    "jkp_code_commit": "67174c7f",
}


def _declared(labels: dict[str, str] = LABELS) -> tuple[TrialRegister, list[dict[str, Any]]]:
    trial = declared_trial(declaration_evidence(labels))
    return TrialRegister("test", (trial,)), [declared_row(trial)]


# --------------------------------------------------------------------------- the declaration


def test_searches_are_the_specs_62_and_the_evidence_reads_back() -> None:
    assert SEARCHES == 62
    evidence = declaration_evidence(LABELS)
    assert all(evidence_value(evidence, label) == value for label, value in LABELS.items())
    with pytest.raises(RunError, match="0 times"):
        evidence_value(evidence, "missing_label")


def test_the_gate_accepts_the_declared_row_and_refuses_every_mismatch() -> None:
    register, committed = _declared()
    assert check_declaration(register, committed, committed, LABELS).trial_id == TRIAL_ID
    with pytest.raises(RunError, match="construction_sha256"):
        check_declaration(register, committed, committed, {**LABELS, "construction_sha256": "e" * 64})
    with pytest.raises(RunError, match="'declared' row"):
        check_declaration(register, [], [], LABELS)
    edited = dataclasses.replace(register.trials[0], description="edited after the pin")
    with pytest.raises(RunError, match="'declared' row"):
        check_declaration(TrialRegister("test", (edited,)), committed, committed, LABELS)
    wrong = dataclasses.replace(register.trials[0], searches=61)
    with pytest.raises(RunError, match="62-search"):
        check_declaration(TrialRegister("test", (wrong,)), committed, committed, LABELS)
    with pytest.raises(RunError, match="0 register rows"):
        check_declaration(TrialRegister("test", ()), committed, committed, LABELS)
    with pytest.raises(RunError, match="completed run"):
        check_declaration(register, committed, [*committed, {"event": "completed"}], LABELS)


def test_a_second_declaration_refuses() -> None:
    register, committed = _declared()
    require_undeclared(TrialRegister("test", ()), [])
    with pytest.raises(RunError, match="already declared"):
        require_undeclared(register, [])
    with pytest.raises(RunError, match="already declared"):
        require_undeclared(TrialRegister("test", ()), committed)


# --------------------------------------------------------------------------- the run's ledger


def _ledgers(tmp_path: Path) -> tuple[Path, Path, TrialRegister]:
    register, committed = _declared()
    committed_path = tmp_path / "committed.jsonl"
    committed_path.write_text("".join(json.dumps(row) + "\n" for row in committed))
    return tmp_path / "local" / "ledger.jsonl", committed_path, register


def test_run_writes_started_access_report_and_completed_in_order(tmp_path: Path) -> None:
    ledger, committed, register = _ledgers(tmp_path)
    accesses: list[HoldoutAccess] = []

    def record(access: HoldoutAccess) -> int:
        accesses.append(access)
        return 41

    def evaluate_run() -> tuple[dict[str, Any], bytes]:
        assert [r["event"] for r in read_ledger(ledger)] == ["started", "access_recorded"]  # access before any read
        return {"verdict_lines": ["max|micro: ELIGIBLE"]}, gzip.compress(b"{}\n", mtime=0)

    row = run(
        "r1",
        head="abc",
        command=["x"],
        accessed_by="loop",
        record_access=record,
        evaluate_run=evaluate_run,
        register=register,
        ledger=ledger,
        committed_ledger=committed,
        labels=lambda: dict(LABELS),
    )
    assert row["verdicts"] == ["max|micro: ELIGIBLE"]
    rows = read_ledger(ledger)
    assert [r["event"] for r in rows] == ["started", "access_recorded", "report_written", "completed"]
    assert rows[1]["access_id"] == 41
    assert (accesses[0].access_kind, accesses[0].result_version) == ("evaluate", "r1")
    assert (ledger.parent / "r1.json").is_file() and (ledger.parent / "r1-names.jsonl.gz").is_file()


def test_a_gate_refusal_writes_nothing_and_a_failed_read_ends_the_run(tmp_path: Path) -> None:
    ledger, committed, register = _ledgers(tmp_path)

    def never(_: HoldoutAccess) -> int:
        raise AssertionError("no access before the gate passes")

    with pytest.raises(RunError):
        run(
            "r0",
            head="abc",
            command=[],
            accessed_by="loop",
            record_access=never,
            evaluate_run=lambda: ({}, b""),
            register=register,
            ledger=ledger,
            committed_ledger=committed,
            labels=lambda: {**LABELS, "python": "3.13"},
        )
    assert not ledger.exists()

    def broken() -> tuple[dict[str, Any], bytes]:
        raise RunError("REPRODUCTION: differs")

    with pytest.raises(RunError, match="REPRODUCTION"):
        run(
            "r2",
            head="abc",
            command=[],
            accessed_by="loop",
            record_access=lambda _: 7,
            evaluate_run=broken,
            register=register,
            ledger=ledger,
            committed_ledger=committed,
            labels=lambda: dict(LABELS),
        )
    rows = read_ledger(ledger)
    assert [r["event"] for r in rows] == ["started", "access_recorded", "failed"]
    assert rows[-1]["step"] == "report_written"


# --------------------------------------------------------------------------- readers


def _gz(lines: list[Any]) -> bytes:
    return gzip.compress("".join(json.dumps(line) + "\n" for line in lines).encode(), mtime=0)


def test_readers_check_units_and_month_ends() -> None:
    cutoffs = _gz(
        [
            ["nyse_p20", "2021-05-31", 1.0, "usd_millions"],
            ["nyse_p50", "2021-05-31", 2.0, "usd_millions"],
            ["nyse_p80", "2021-05-31", 3.0, "usd_millions"],
            ["nyse_p80", "2021-06-30", 3.0, "usd_millions"],
        ]
    )
    assert read_nyse_cutoffs(cutoffs) == {date(2021, 5, 31): (1e6, 2e6, 3e6)}
    with pytest.raises(RunError, match="usd_millions"):
        read_nyse_cutoffs(_gz([["nyse_p20", "2021-05-31", 1.0, "usd"]]))
    published = _gz([["rmax1_21d", "2021-05-31", 0.01, "decimal_return"], ["age", "2021-05-30", 0.02, "x"]])
    assert read_published_max(published) == {"2021-05": 0.01}
    with pytest.raises(RunError, match="month-end"):
        read_published_max(_gz([["rmax1_21d", "2021-05-30", 0.01, "decimal_return"]]))


SESSIONS = [date(2021, 4, 30), *(date(2021, 5, d) for d in range(3, 29) if date(2021, 5, d).weekday() < 5)]


def _bars(prices: list[float]) -> list[list[Any]]:
    return [[d.isoformat(), p, p, 100, False, True] for d, p in zip(SESSIONS, prices, strict=True)]


def _panel_month(series: dict[int, int]) -> PanelMonth:
    def name(series_id: int) -> PanelName:
        return PanelName(series_id, 500.0, "OTHER", {}, HoldingReturn(HoldingStatus.OBSERVED, {}), None)

    return PanelMonth(SESSIONS[-1], SESSIONS[-1], {n: name(s) for n, s in series.items()}, {}, {})


def test_max_readings_read_each_admitted_name_at_its_session_and_refuse_missing_bars() -> None:
    prices = [10.0 * (1.0 + 0.01 * i) for i in range(len(SESSIONS))]
    daily = _gz([[5, _bars(prices)], [6, _bars([10.0] * len(SESSIONS))], [9, _bars(prices)]])
    got = max_readings(daily, SESSIONS, [_panel_month({1: 5, 2: 6})])
    expected = MaxSeries(
        [DailyBar(d, p, p, 100, False, True) for d, p in zip(SESSIONS, prices, strict=True)], SESSIONS
    ).at(len(SESSIONS) - 1)
    assert got[SESSIONS[-1]][1] == expected and expected.value is not None
    assert got[SESSIONS[-1]][2].missing is MaxMissing.ZERO_HEAVY
    with pytest.raises(RunError, match="no frozen daily bars"):
        max_readings(daily, SESSIONS, [_panel_month({1: 5, 3: 8})])


# --------------------------------------------------------------------------- reproduction and evaluation

FORMATIONS = [date(2021, 4, 30), date(2021, 5, 28), date(2021, 6, 30)]
CUTOFFS = (300.0, 600.0, 900.0)
N = 1000


def _flags_for(name: int) -> NameFlags:
    missing = MaxMissing.SCREENED if name % 50 == 0 else MaxMissing.SHORT if name % 50 == 1 else None
    reading = MaxReading(None if missing else 0.01, 20, 0, missing)
    flagged = {Filter.MAX} if name % 10 == 0 else set()
    if name % 7 == 0:
        flagged.add(Filter.SUB5)
    if name % 11 == 0:
        flagged.add(Filter.YOUNG)
    return NameFlags(reading, frozenset(flagged))


def _stage() -> Stage:
    def name(n: int, r: float) -> PanelName:
        return PanelName(n, 1.0 + n, "OTHER", {}, HoldingReturn(HoldingStatus.OBSERVED, dict.fromkeys(ARMS, r)), None)

    months = [
        PanelMonth(
            formation=f,
            session=f,
            admitted={n: name(n, -0.2 if n % 10 == 0 else 0.01) for n in range(N)},
            close=dict.fromkeys(range(N), 20.0),
            first_bar=dict.fromkeys(range(N), date(2010, 1, 1)),
        )
        for f in FORMATIONS
    ]
    flags = {n: _flags_for(n) for n in range(N)}
    return Stage(
        months=months,
        flags=[flags for _ in months],
        readings=[{n: f.reading for n, f in flags.items()} for _ in months],
        cutoffs=dict.fromkeys(FORMATIONS, CUTOFFS),
        symbols={n: f"S{n}" for n in range(N)},
    )


def test_module_counts_by_hand_and_reproduce_refuses_any_difference() -> None:
    stage = _stage()
    counts = module_counts(stage.months[0], stage.flags[0], CUTOFFS)
    micro = range(299)  # ME = 1 + n < 300
    assert counts["micro"]["admitted"] == len(micro)
    assert counts["micro"]["max"] == sum(1 for n in micro if n % 10 == 0)
    assert counts["micro"]["max_screened"] == sum(1 for n in micro if n % 50 == 0)
    assert counts["micro"]["max_short"] == sum(1 for n in micro if n % 50 == 1)
    assert counts["micro"]["any"] == sum(1 for n in micro if n % 10 == 0 or n % 7 == 0 or n % 11 == 0)
    assert counts["top1000"]["admitted"] == N and counts["rest"]["admitted"] == 0

    grid = [f.isoformat() for f in FORMATIONS]
    table = [module_counts(m, f, CUTOFFS) for m, f in zip(stage.months, stage.flags, strict=True)]
    assert reproduce((grid, table), stage) == len(FORMATIONS)
    table[1]["small"]["sub5"] += 1
    with pytest.raises(RunError, match=r"REPRODUCTION: .* at 1 formations: \['2021-05-28'\]"):
        reproduce((grid, table), stage)


def test_evaluate_gives_30_verdicts_a_pooled_diagnostic_and_strict_json() -> None:
    fid = MaxFidelity(Verdict.PASS, {}, 0)
    result = evaluate([_stage()], fid)
    pairs = result["pairs"]
    assert len(pairs) == len(FILTER_SETS) * 7
    verdicts = {k: b["verdict"] for k, b in pairs.items() if b["verdict"] is not None}
    assert len(verdicts) == 30 and all(k.split("|")[1] != "all" for k in verdicts)
    # Excluding the MAX-flagged losers (n % 10 == 0) lifts growth in the top 1,000.
    assert verdicts["max|top1000"].delta_g[("whole", "worst_case", "stress_2x")] > 0
    lines = verdict_lines(pairs, fid)
    assert len(lines) == 31 and lines[-1] == "MAX fidelity: PASS"
    assert {e.population for e in result["excluded"] if e.name_key == 0} == {"micro", "top1000", "all"}
    json.dumps(jsonable({k: v for k, v in result.items() if k != "excluded"}), allow_nan=False)


def test_a_run_left_open_refuses_the_next_until_it_is_ended(tmp_path: Path) -> None:
    ledger, committed, register = _ledgers(tmp_path)
    ledger.parent.mkdir(parents=True)
    ledger.write_text(json.dumps({"run_id": "dead", "event": "started"}) + "\n")

    def attempt() -> dict[str, Any]:
        return run(
            "r3",
            head="abc",
            command=[],
            accessed_by="loop",
            record_access=lambda _: 8,
            evaluate_run=lambda: ({"verdict_lines": []}, b""),
            register=register,
            ledger=ledger,
            committed_ledger=committed,
            labels=lambda: dict(LABELS),
        )

    with pytest.raises(RunError, match=r"\['dead'\] started and never ended"):
        attempt()
    with ledger.open("a") as handle:
        handle.write(json.dumps({"run_id": "dead", "event": "failed"}) + "\n")
    assert attempt()["event"] == "completed"
