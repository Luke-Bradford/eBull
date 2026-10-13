"""#3621 slice 5b: v2's SI run pieces (``scripts/report_3621_si.py``), fixture-only."""

from __future__ import annotations

import csv
import json
from datetime import date
from pathlib import Path

import pytest

from app.services.avoidance_filters import FILTER_SETS, FILTER_SETS_V2, Filter, MaxReading, NameFlags
from app.services.short_interest_flag import Reading, SiState
from scripts.report_3621_books import PairResult, PairVerdict, Population
from scripts.report_3621_si import (
    CONDITION_5,
    COUNTED,
    STAGE_B_ADDED_INPUT,
    SiRunError,
    counts_rows,
    coverage_shortfalls,
    si_counts,
    si_name_lines,
    si_overlap,
    si_pair_result,
    stage_b_identity,
    valueless_weight,
    with_si,
)

PREMISE_COUNTS = Path(__file__).resolve().parents[1] / "docs" / "research" / "3621-si-premise-counts.csv"
M = date(2021, 6, 30)
USED = date(2021, 6, 15)
READING = MaxReading(0.05, 20, 0, None)


def _flags(**flagged: frozenset[Filter]) -> dict[int, NameFlags]:
    return {int(k[1:]): NameFlags(READING, v) for k, v in flagged.items()}


def _valid(sir: float, *, carried: bool = False) -> Reading:
    return Reading(SiState.VALID, USED, sir, carried)


def test_v2_adds_si_and_all_four_after_v1s_five() -> None:
    assert len(FILTER_SETS) == 5
    assert FILTER_SETS_V2[:5] == FILTER_SETS
    assert FILTER_SETS_V2[5:] == (frozenset({Filter.SI}), frozenset({Filter.MAX, Filter.SUB5, Filter.YOUNG, Filter.SI}))


def test_with_si_adds_the_filter_only_to_flagged_names() -> None:
    flags = _flags(n1=frozenset({Filter.MAX}), n2=frozenset())
    out = with_si(flags, frozenset({1}))
    assert out[1].flagged == {Filter.MAX, Filter.SI} and out[2].flagged == frozenset()
    assert out[1].removed_by(frozenset({Filter.SI})) and not out[2].removed_by(FILTER_SETS_V2[-1])
    with pytest.raises(SiRunError):
        with_si(flags, frozenset({3}))


def test_counts_columns_and_population_order_are_premise_twos() -> None:
    with PREMISE_COUNTS.open() as f:
        reader = csv.reader(f)
        header = next(reader)
        first_rows = [next(reader) for _ in Population]
    assert header == ["M", "population", *COUNTED]
    assert [r[1] for r in first_rows] == [str(p) for p in Population]


def test_si_counts_tallies_each_population() -> None:
    readings = {
        1: _valid(0.30, carried=True),
        2: _valid(0.01),
        3: Reading(SiState.UNMATCHED),
        4: Reading(SiState.IDENTITY_FAIL, USED),
    }
    pops = {p: frozenset() for p in Population} | {
        Population.MICRO: frozenset({1, 2, 3}),
        Population.MEGA: frozenset({4}),
        Population.ALL: frozenset({1, 2, 3, 4}),
    }
    counts = si_counts(pops, readings, frozenset({1}))
    assert counts[Population.MICRO] == {
        "admitted": 3,
        "ambiguous": 0,
        "unmatched": 1,
        "unverifiable": 0,
        "identity_fail": 0,
        "valid": 2,
        "flagged": 1,
        "split_carried": 1,
    }
    assert counts[Population.ALL]["identity_fail"] == 1 and counts[Population.SMALL]["admitted"] == 0
    assert counts_rows(M, counts)[0] == ["2021-06-30", "micro", "3", "0", "1", "0", "0", "2", "1", "1"]
    with pytest.raises(SiRunError, match="no_settlement"):
        si_counts(pops, {**readings, 4: Reading(SiState.NO_SETTLEMENT)}, frozenset())


def test_name_lines_order_and_serialisation_match_the_premise_file() -> None:
    sir = 0.0065280651879227195
    me = {1004: 5.0e9, 1001: 7.0e9, 1002: 5.0e9}
    readings = {1001: _valid(sir), 1004: Reading(SiState.UNMATCHED), 1002: _valid(0.5)}
    lines = si_name_lines(M, me, readings, frozenset({1002}))
    # The premise file's first line (docs/research §"Premises", per-name sha256 c005ca8a…) has exactly this shape.
    assert lines[0] == '["2021-06-30",1001,"valid","2021-06-15",0.0065280651879227195,false]'
    assert [json.loads(line)[1] for line in lines] == [1001, 1002, 1004]  # ME desc, ties by name_key
    assert lines[2] == '["2021-06-30",1004,"unmatched",null,null,false]'
    assert json.loads(lines[1])[5] is True
    with pytest.raises(SiRunError):
        si_name_lines(M, {1: 1.0}, readings, frozenset())


def test_coverage_floor_is_ninety_percent_of_u_at_covered_formations() -> None:
    at_floor = {i: _valid(0.1) for i in range(9)} | {9: Reading(SiState.UNMATCHED)}
    below = {i: _valid(0.1) for i in range(8)} | {8: Reading(SiState.UNMATCHED), 9: Reading(SiState.AMBIGUOUS)}
    u = frozenset(range(10))
    days = [date(2021, 5, 28), date(2021, 6, 30), date(2021, 7, 30), date(2021, 8, 31)]
    assert coverage_shortfalls([u, u, u, frozenset()], [None, at_floor, below, {}], days) == days[2:]
    with pytest.raises(SiRunError):
        coverage_shortfalls([u], [None, None], days[:1])


def test_condition_5_downgrades_only_eligible_or_not_eligible() -> None:
    short = [date(2021, 7, 30)]
    eligible = si_pair_result(PairResult(PairVerdict.ELIGIBLE), short)
    assert eligible.verdict is PairVerdict.NOT_ELIGIBLE and eligible.failed == (CONDITION_5,)
    failing = si_pair_result(PairResult(PairVerdict.NOT_ELIGIBLE, ("2 stage B",)), short)
    assert failing.failed == ("2 stage B", CONDITION_5)
    for verdict in (PairVerdict.REFUSED, PairVerdict.NO_EFFECT):
        assert si_pair_result(PairResult(verdict), short) == PairResult(verdict)
    assert si_pair_result(PairResult(PairVerdict.ELIGIBLE), []) == PairResult(PairVerdict.ELIGIBLE)


def test_valueless_weight_and_overlap() -> None:
    readings = {1: _valid(0.1), 2: Reading(SiState.UNMATCHED), 3: Reading(SiState.UNVERIFIABLE)}
    assert valueless_weight(frozenset({1, 2, 3}), readings) == pytest.approx(2 / 3)
    assert valueless_weight(frozenset(), readings) is None
    flags = _flags(
        n1=frozenset({Filter.SI, Filter.MAX}),
        n2=frozenset({Filter.SI, Filter.SUB5, Filter.YOUNG}),
        n3=frozenset({Filter.MAX}),
    )
    assert si_overlap(flags) == {"si": 2, "max": 1, "sub5": 1, "young": 1}


# --------------------------------------------------------------------------- the rebuilt stage B


def _row(m: str, key: int, holding: float, **extra: object) -> dict[str, object]:
    return {"M": m, "name_key": key, "exclusion": None, "prices": {"holding": {"r": holding}, "close": 10.0}, **extra}


V1_INPUTS = {"inputs/daily.jsonl.gz": "aa", "inputs/sessions.jsonl.gz": "bb"}
V2_INPUTS = {**V1_INPUTS, STAGE_B_ADDED_INPUT: "cc"}


def test_rebuilt_stage_b_may_differ_only_in_holding_returns() -> None:
    v1 = [_row("2021-06-30", 1, 3.5), _row("2021-06-30", 2, 0.1, exclusion="no_me")]
    v2 = [_row("2021-06-30", 1, 0.9), _row("2021-06-30", 2, 0.1, exclusion="no_me")]
    assert stage_b_identity(v1, v2, V1_INPUTS, V2_INPUTS) == 2


@pytest.mark.parametrize(
    ("v2", "inputs", "match"),
    [
        pytest.param([_row("2021-06-30", 1, 0.9)], V2_INPUTS, "keys differ", id="missing-row"),
        pytest.param(
            [_row("2021-06-30", 1, 0.9), _row("2021-06-30", 2, 0.1, exclusion="no_me", added=1)],
            V2_INPUTS,
            "differ outside",
            id="added-field",
        ),
        pytest.param(
            [_row("2021-06-30", 1, 0.9), _row("2021-06-30", 2, 0.1)], V2_INPUTS, "differ outside", id="changed-field"
        ),
        pytest.param(
            [_row("2021-06-30", 1, 0.9), _row("2021-06-30", 1, 0.9)], V2_INPUTS, "repeats", id="duplicate-key"
        ),
        pytest.param(
            [_row("2021-06-30", 1, 0.9, terminating=0), _row("2021-06-30", 2, 0.1, exclusion="no_me")],
            V2_INPUTS,
            "differ outside",
            id="bool-became-int",
        ),
        pytest.param(
            [_row("2021-06-30", 1, 0.9), _row("2021-06-30", 2, 0.1, exclusion="no_me")],
            {**V2_INPUTS, "inputs/daily.jsonl.gz": "zz"},
            "inputs changed",
            id="input-changed",
        ),
        pytest.param(
            [_row("2021-06-30", 1, 0.9), _row("2021-06-30", 2, 0.1, exclusion="no_me")],
            {**V2_INPUTS, "inputs/other.jsonl.gz": "dd"},
            "added inputs",
            id="extra-input",
        ),
        pytest.param(
            [_row("2021-06-30", 1, 0.9), _row("2021-06-30", 2, 0.1, exclusion="no_me")],
            V1_INPUTS,
            "added inputs",
            id="cutoffs-input-missing",
        ),
    ],
)
def test_rebuilt_stage_b_refusals(v2: list[dict[str, object]], inputs: dict[str, str], match: str) -> None:
    v1 = [_row("2021-06-30", 1, 3.5, terminating=False), _row("2021-06-30", 2, 0.1, exclusion="no_me")]
    v2 = [r if r["name_key"] != 1 or "terminating" in r else {**r, "terminating": False} for r in v2]
    with pytest.raises(SiRunError, match=match):
        stage_b_identity(v1, v2, V1_INPUTS, inputs)
