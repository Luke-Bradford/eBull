"""#3609 step 2 report assembly: the verdict and every diagnostic over one synthetic panel, and the output payload.

Synthetic inputs only (no corpus, no stage B). The book universe is shrunk to 120 names so 40 formations score in a
fast-tier test; nothing else about the construction changes.
"""

from __future__ import annotations

import calendar
import json
import math
import random
from dataclasses import dataclass, fields, replace
from datetime import date
from enum import StrEnum

import pytest

import app.services.factor_book as factor_book
import scripts.report_3609_step2_assembly as assembly
from app.services.factor_book import CHARACTERISTICS, BookRefusal
from app.services.factor_book_path import HoldingReturn, Month, month_of
from app.services.factor_book_series import ARMS
from app.services.factor_panel_prices import HoldingStatus
from scripts.report_3609_baselines import FACTOR_REGRESSORS
from scripts.report_3609_step2 import PanelMonth, PanelName
from scripts.report_3609_step2_assembly import (
    LABELS,
    SURVIVORSHIP,
    boundary_formation,
    evaluate,
    jsonable,
    payload,
    verdict_line,
)
from scripts.report_3609_step2_verdict import BASE, Verdict, verdict

COSTS = {"gross": 0.0, "net": 1.0, "stress_2x": 2.0}
INDUSTRIES = ("Manuf", "HiTec", "Shops")
NAMES = 130
UNIVERSE = 120
SEASONED = date(2000, 1, 3)


def _month_end(year: int, month: int) -> date:
    return date(year, month, calendar.monthrange(year, month)[1])


def _formations() -> list[date]:
    """2021-04..2024-07: one stage-A formation, then stage B's 2021-05..2024-07."""
    out, year, month = [], 2021, 4
    while (year, month) <= (2024, 7):
        out.append(_month_end(year, month))
        year, month = (year + 1, 1) if month == 12 else (year, month + 1)
    return out


def _panel() -> list[PanelMonth]:
    rng = random.Random(3609)
    panel = []
    for formation in _formations():
        admitted = {}
        for name in range(NAMES):
            r = rng.gauss(0.01, 0.06)
            admitted[name] = PanelName(
                series_id=1000 + name,
                me=1e9 + rng.random() * 1e9,
                industry=INDUSTRIES[name % len(INDUSTRIES)],
                signed={c: rng.gauss(0.0, 1.0) for c in CHARACTERISTICS},
                holding=HoldingReturn(HoldingStatus.OBSERVED, {"best_case": r, "worst_case": r - 0.001}),
                sic=3720,
            )
        close = {name: 6.0 + name % 60 for name in range(NAMES)}
        panel.append(PanelMonth(formation, formation, admitted, close, dict.fromkeys(range(NAMES), SEASONED)))
    return panel


def _months() -> list[Month]:
    out = [(2021, 5)]
    while out[-1] < (2024, 8):
        year, month = out[-1]
        out.append((year + 1, 1) if month == 12 else (year, month + 1))
    return out


def _factors() -> dict[str, dict[Month, float]]:
    rng = random.Random(7)
    return {name: {m: rng.gauss(0.0, 0.03) for m in _months()} for name in (*FACTOR_REGRESSORS, "RF")}


def _b1() -> dict[str, list[object]]:
    rng = random.Random(11)
    months = _months()
    return {
        "months": [f"{y:04d}-{m:02d}" for y, m in months],
        "continuing": [rng.gauss(0.008, 0.04) for _ in months],
        "rebalance_cost": [0.0] * len(months),
    }


#: Published for every formation but the last two, which print ``unavailable``.
CUTOFFS = {f: 1.5e9 for f in _formations()[:-2]}


def _evaluate(panel: list[PanelMonth]) -> assembly.Report:
    return evaluate(panel, factors=_factors(), cutoffs=CUTOFFS, b1_saved=_b1(), b1_close=400.0, costs=COSTS, draws=3)


@pytest.fixture(scope="module")
def report() -> assembly.Report:
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(factor_book, "UNIVERSE_SIZE", UNIVERSE)
        return _evaluate(_panel())


# --------------------------------------------------------------------------- evaluate


def test_the_run_captures_the_stage_b_boundary_and_the_verdict_is_the_verdict_modules(
    report: assembly.Report,
) -> None:
    run = report.run
    assert run.months == tuple(_months())
    boundary = date(2021, 5, 31)
    assert all(p.boundary is not None and p.boundary.formation == boundary for p in run.book.values())
    assert report.verdict == verdict(run, _factors())
    assert report.verdict.status in {"INSUFFICIENT", "G1_REFUSED", "FAIL", "PASS"}


def test_the_reference_is_cap_weighted_over_the_universe_and_industries_come_from_the_panel(
    report: assembly.Report,
) -> None:
    panel = _panel()
    first = panel[0]
    universe = sorted(first.admitted, key=lambda n: (-first.admitted[n].me, n))[:UNIVERSE]
    total = math.fsum(first.admitted[n].me for n in universe)
    expected = {
        industry: math.fsum(first.admitted[n].me for n in universe if first.admitted[n].industry == industry) / total
        for industry in INDUSTRIES
    }
    weights = report.attribution.industry_monthly[month_of(first.formation)]
    assert set(weights) == set(INDUSTRIES)
    for industry, weight in weights.items():
        assert weight.reference == pytest.approx(expected[industry], rel=1e-12)


def test_every_block_covers_the_run(report: assembly.Report) -> None:
    formations = [month_of(f) for f in _formations()]
    stage_labels = {"stage A", "stage B", "pooled"}
    for arm in ARMS:
        assert set(report.operations.book[arm]) >= stage_labels | {"2021", "2024"}
        for summary in report.signals[arm].values():
            assert set(summary.ic) == stage_labels
        for band in report.sub_book_windows[arm].values():
            assert set(band) == stage_labels
    assert [month_of(r.formation) for r in report.universe.rows] == formations
    assert sum(r.cutoff is not None for r in report.universe.rows) == len(CUTOFFS)
    assert list(report.segments.price_unavailable) == formations
    assert set(report.sub_books) == {(arm, cost) for arm in ARMS for cost in COSTS}


def test_a_diagnostic_refusal_leaves_no_verdict(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verdict order step 1: a diagnostic input refusal stops the run like any data refusal."""

    def refuse(*_: object) -> None:
        raise BookRefusal("ME_INVALID", "an ME sum in the share is not finite")

    monkeypatch.setattr(factor_book, "UNIVERSE_SIZE", UNIVERSE)
    monkeypatch.setattr(assembly, "universe_diagnostics", refuse)
    with pytest.raises(BookRefusal) as caught:
        _evaluate(_panel())
    assert caught.value.code == "ME_INVALID"


def test_a_panel_without_the_boundary_formation_is_not_the_declared_run() -> None:
    panel = _panel()
    assert boundary_formation(panel) == date(2021, 5, 31)
    with pytest.raises(ValueError, match="boundary month"):
        boundary_formation([m for m in panel if month_of(m.formation) != (2021, 5)])
    with pytest.raises(ValueError, match="boundary month"):
        boundary_formation([*panel, replace(panel[1], formation=date(2021, 5, 30))])


# --------------------------------------------------------------------------- output


def test_the_verdict_line_names_the_survivorship_regimes_and_what_qualifies_the_status() -> None:
    assert verdict_line(Verdict("FAIL", "G1 best_case")) == f"FAIL: G1 best_case. {SURVIVORSHIP}."
    insufficient = verdict_line(Verdict("INSUFFICIENT", insufficient=(date(2022, 3, 31),)))
    assert insufficient.startswith("INSUFFICIENT (formations 2022-03-31). ")
    annotations = ("fails at stress cost (best_case vs B1)", "depends on 2023 (x)")
    passed = verdict_line(Verdict("PASS", annotations=annotations))
    assert passed.startswith("PASS [fails at stress cost (best_case vs B1); depends on 2023 (x)]. ")
    assert "stage B lies wholly in the Form 25-checked regime" in passed


def test_the_payload_is_strict_json_with_labels_months_and_judgements(report: assembly.Report) -> None:
    out = payload(report)
    text = json.dumps(out, allow_nan=False, sort_keys=True)
    assert json.loads(text) == out
    assert out["labels"] == dict(LABELS)
    assert out["verdict"]["line"] == verdict_line(report.verdict)
    assert out["verdict"]["status"] == report.verdict.status
    assert out["path"]["months"][0] == "2021-05" and out["path"]["months"][-1] == "2024-08"
    arm = ARMS[0]
    book = out["path"]["book"][f"{arm}|{BASE}"]
    assert book["returns"]["2021-06"] == report.run.book[(arm, BASE)].returns[(2021, 6)]
    # Properties carry the judgements a reader needs.
    window = out["attribution"]["attribution"][arm]["stage B"]
    assert window["selection"] == report.attribution.attribution[arm]["stage B"].selection
    assert "passed" in window["regression"]
    summary = out["signals"][arm]["composite"]["ic"]["pooled"]
    assert summary["insufficient"] == report.signals[arm]["composite"].ic["pooled"].insufficient
    assert out["signals"][arm]["composite"]["monthly"]["ic"].keys() == {f"{y:04d}-{m:02d}" for y, m in _months()}


class _Colour(StrEnum):
    RED = "red"


@dataclass(frozen=True)
class _Row:
    value: float
    when: date

    @property
    def doubled(self) -> float:
        return 2 * self.value

    @property
    def _hidden(self) -> float:
        return 0.0


def test_jsonable_formats_every_output_shape() -> None:
    got = jsonable(
        {
            (2021, 6): [_Row(1.5, date(2021, 6, 30))],
            ("best_case", "net"): {_Colour.RED: math.nan, "up": math.inf, "down": -math.inf},
            "names": frozenset({3, 1, 2}),
            "pair": (7, 40),
        }
    )
    assert got == {
        "2021-06": [{"value": 1.5, "when": "2021-06-30", "doubled": 3.0}],
        "best_case|net": {"red": "nan", "up": "inf", "down": "-inf"},
        "names": [1, 2, 3],
        "pair": [7, 40],
    }


def test_jsonable_refuses_colliding_keys_and_unknown_types() -> None:
    with pytest.raises(ValueError, match="2021-06"):
        jsonable({(2021, 6): 1, "2021-06": 2})
    with pytest.raises(TypeError, match="object"):
        jsonable(object())
    with pytest.raises(TypeError, match="no output key"):
        jsonable({1.5: 0})


def test_the_payload_covers_every_report_field(report: assembly.Report) -> None:
    out = payload(report)
    names = {f.name for f in fields(assembly.Report)}
    assert names - {"run", "sub_books", "sub_book_windows"} <= out.keys()
    assert set(out["sub_books"]) == {"monthly", "windows"}
