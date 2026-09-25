"""#2901 runner, the run (part 2c-ii): stage order, sealing and what each verdict publishes.

Spec: ``docs/proposals/ta/2026-09-25-2901-quality-declaration-spec.md`` ("Parity with the frozen simulator",
"Identity gate", "Verdict", "Descriptive readouts"). Every price here is synthetic: one formation (X = 2013-07-01)
and a window ending 2016-09-30, so the statistic months are 2013-07 … 2016-08: the gate needs 24, and the
cohort bar needs df ≥ 2 to lie inside its bisection bracket. S3 terminates on 2013-12-31, so the
censuses carry a recognition.
"""

from __future__ import annotations

import hashlib
import json
import math
import stat
from dataclasses import replace
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pytest

import scripts.run_2901_quality_trial as runner
from app.services.market_calendar import us_market_status
from app.services.r6_exclusion_trial import (
    HALF_SPREAD,
    PROGRAMME_POLICIES,
    ZERO_RECOVERY,
    PriceBar,
    PriceSeries,
    SeriesEvidence,
    month_pairs,
)
from app.services.r6_monthly_trial import GateRefusal, Portfolio, Refused, Tri, Unavailable, Verdict
from app.services.series_termination import TerminationEvidence
from scripts.run_2901_quality_trial import (
    SPY,
    FormationBooks,
    appended_hac,
    book_symbols,
    jsonable,
    load_prices,
    run_trial,
    schedules,
    sealer,
    simulate_books,
    spy_regime,
)

WINDOW_END = date(2016, 9, 30)
MONTHS = month_pairs((2013, 7), (2016, 8))
BOOKS = (
    FormationBooks(
        formation=date(2013, 6, 28),
        x_date=date(2013, 7, 1),
        arm=frozenset({"S1"}),
        control=frozenset({"S1", "S2", "S3"}),
        complete_case=frozenset({"S1", "S2", "S3", "S4"}),
        gate_control=frozenset({"S2"}),
        gate_boundary_ties=0,
    ),
)


def _sessions(start: date, end: date) -> list[date]:
    days, day = [], start
    while day <= end:
        if us_market_status(day) != "closed":
            days.append(day)
        day += timedelta(days=1)
    return days


def _series(symbol: str, seed: int, *, last: date = WINDOW_END, drop: frozenset[date] = frozenset()) -> PriceSeries:
    bars = tuple(
        PriceBar(
            day,
            10.0 * (1 + 0.3 * math.sin(seed * index / 7.0)) * 1.001,
            10.0 * (1 + 0.3 * math.sin(seed * index / 7.0)),
        )
        for index, day in enumerate(_sessions(date(2013, 6, 3), last))
        if day not in drop
    )
    return PriceSeries(symbol, bars, 0)


def _evidence(prices: dict[str, PriceSeries]) -> dict[str, SeriesEvidence]:
    return {
        symbol: SeriesEvidence(
            index,
            series.bars[0].day,
            series.bars[-1].day,
            TerminationEvidence(linked=False, provision=None, q_suffix=False),
        )
        for index, (symbol, series) in enumerate(sorted(prices.items()), start=1)
    }


def _prices() -> dict[str, PriceSeries]:
    return {
        "S1": _series("S1", 1),
        "S2": _series("S2", 2),
        "S3": _series("S3", 3, last=date(2013, 12, 31)),
        "S4": _series("S4", 4),
    }


def _ours(prices: dict[str, PriceSeries], evidence: dict[str, SeriesEvidence]) -> dict[tuple[int, int], float]:
    paths = simulate_books(schedules(BOOKS), BOOKS, prices, evidence, window_end=WINDOW_END)
    gross = paths[(ZERO_RECOVERY.label, 0.0)]
    return {m: gross[Portfolio.ARM].factors[m] - gross[Portfolio.GATE_CONTROL].factors[m] for m in MONTHS}


def _run(
    tmp_path: Path,
    *,
    prices: dict[str, PriceSeries] | None = None,
    spy: PriceSeries | Unavailable | None = None,
    reference: Any = None,
) -> dict[str, Any]:
    prices = _prices() if prices is None else prices
    spy = _series(SPY, 5) if spy is None else spy
    evidence = _evidence({**prices, **({SPY: spy} if isinstance(spy, PriceSeries) else {})})
    ours = _ours(prices, _evidence(prices)) if reference is None else None

    def read_reference() -> dict[tuple[int, int], float]:
        if reference is None:
            assert ours is not None
            return ours
        return reference()

    return run_trial(
        books=BOOKS,
        prices=prices,
        evidence=evidence,
        spy_series=spy,
        read_reference=read_reference,
        family=10,
        seal=sealer(tmp_path / "sealed"),
        window_end=WINDOW_END,
        months=MONTHS,
    )


def _published(output: dict[str, Any]) -> str:
    return json.dumps(jsonable(output), allow_nan=False, sort_keys=True)


def test_a_passed_gate_publishes_the_statistics_and_descriptives(tmp_path: Path) -> None:
    output = _run(tmp_path)
    assert output["stage"] == "post_gate"
    assert output["gate"]["state"] is Tri.TRUE
    assert output["verdict"] not in {Verdict.REFUSED_PRE_GATE, Verdict.GATE_FAIL, Verdict.SIMULATOR_INVARIANT}
    assert output["family_size"] == 10
    assert set(output["statistics"]) == {policy.label for policy in PROGRAMME_POLICIES}
    described = output["descriptives"]
    spy = described["spy"]
    assert spy["spread_drag"] == pytest.approx(spy["gross_multiple"] - spy["net_multiple"], rel=1e-12)
    assert described["spy_regime"] == {date(2013, 6, 28): "unavailable"}  # no SPY bar a year before D
    governing = described["by_policy"][ZERO_RECOVERY.label]
    assert set(governing["cohorts"]["complete"]) == {2013, 2014, 2015}
    assert governing["cohorts"]["partial"]["formation"] == 2016
    assert governing["hac_with_partial_month"].observations == len(MONTHS) + 1
    assert described["buy_hold_unavailable"] is None
    # Every (policy, h) path is reported, gross included, with the buy-and-hold beside the four books.
    assert set(described["paths"]) == {f"{p.label}:{h}" for p in PROGRAMME_POLICIES for h in (0.0, HALF_SPREAD)}
    gross = described["paths"][f"{ZERO_RECOVERY.label}:0.0"]
    assert set(gross) == {*runner.GATING_BOOKS, runner.BUY_HOLD}
    recognised = gross[Portfolio.CONTROL]["realisation_census"]
    assert recognised["terminated:unknown_termination"]["count"] == 1  # S3, recognised at its first missing mark
    assert not isinstance(gross[Portfolio.CONTROL]["stale_marks"], Unavailable)
    _published(output)


def test_a_failed_gate_publishes_only_the_gate(tmp_path: Path) -> None:
    ours = _ours(_prices(), _evidence(_prices()))
    output = _run(tmp_path, reference=lambda: {month: -value for month, value in ours.items()})
    assert output["verdict"] is Verdict.GATE_FAIL
    assert set(output) == {"verdict", "stage", "gate"}
    assert output["gate"]["state"] is Tri.FALSE
    assert all(not readout.passed for readout in output["gate"]["by_policy"].values())
    _published(output)


def test_an_unreadable_reference_refuses_every_policy(tmp_path: Path) -> None:
    def refuse() -> dict[tuple[int, int], float]:
        raise GateRefusal("unexpected global-q GP/A header")

    output = _run(tmp_path, reference=refuse)
    assert output["verdict"] is Verdict.GATE_FAIL
    assert output["gate"]["state"] is Tri.REFUSED
    assert all(isinstance(readout, Refused) for readout in output["gate"]["by_policy"].values())


def _assert_sealed_refusal(output: dict[str, Any], tmp_path: Path, verdict: Verdict, stage: str) -> None:
    assert set(output) == {"verdict", "stage", "exception_type", "sealed_traceback_sha256"}
    assert (output["verdict"], output["stage"]) == (verdict, stage)
    sealed = tmp_path / "sealed" / f"{stage}-{output['sealed_traceback_sha256']}.txt"
    assert hashlib.sha256(sealed.read_bytes()).hexdigest() == output["sealed_traceback_sha256"]
    assert stat.S_IMODE(sealed.stat().st_mode) == 0o600


def test_a_pre_gate_raise_publishes_only_its_stage_and_type(tmp_path: Path) -> None:
    prices = _prices()
    prices["S4"] = _series("S4", 4, drop=frozenset({date(2013, 7, 1)}))  # a C′ target with no bar on X
    output = _run(tmp_path, prices=prices, reference=lambda: {})
    _assert_sealed_refusal(output, tmp_path, Verdict.REFUSED_PRE_GATE, "pre_gate")


def test_a_post_gate_raise_is_a_simulator_invariant(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def broken(*_args: Any, **_kwargs: Any) -> None:
        raise ValueError("a figure that must not be published: 0.1234")

    monkeypatch.setattr(runner, "policy_statistics", broken)
    output = _run(tmp_path)
    _assert_sealed_refusal(output, tmp_path, Verdict.SIMULATOR_INVARIANT, "post_gate")
    assert "0.1234" not in _published(output)


def test_spy_and_the_buy_hold_are_non_gating(tmp_path: Path) -> None:
    missing_final = _series(SPY, 5, drop=frozenset({WINDOW_END}))
    output = _run(tmp_path, spy=missing_final)
    assert output["stage"] == "post_gate"
    assert isinstance(output["descriptives"]["spy"], Unavailable)
    output = _run(tmp_path, spy=Unavailable("RuntimeError"))
    assert output["descriptives"]["spy"] == Unavailable("RuntimeError")
    assert output["descriptives"]["spy_regime"] == {date(2013, 6, 28): "unavailable"}


def test_the_spy_regime_needs_a_bar_on_each_endpoint_session() -> None:
    bars = (
        PriceBar(date(2012, 6, 28), 10.0, 10.0),
        PriceBar(date(2012, 7, 3), 11.0, 11.0),
        PriceBar(date(2013, 6, 28), 12.0, 12.0),
        PriceBar(date(2013, 7, 4), 1.0, 1.0),  # closure-dated: never a session
    )
    series = PriceSeries(SPY, bars, 0)
    assert spy_regime(series, [date(2013, 6, 28)]) == {date(2013, 6, 28): "positive"}
    # D = 2013-07-01 has no bar: the 06-28 bar never substitutes for it.
    assert spy_regime(series, [date(2013, 7, 1)]) == {date(2013, 7, 1): "unavailable"}
    # D = 2013-07-04 is a closure: its session is 07-03, which has no bar; the closure-dated bar is not used.
    assert spy_regime(series, [date(2013, 7, 4)]) == {date(2013, 7, 4): "unavailable"}
    assert spy_regime(Unavailable("x"), [date(2013, 6, 28)]) == {date(2013, 6, 28): "unavailable"}


def test_the_appended_hac_is_unavailable_on_a_september_only_ruin() -> None:
    prices = _prices()
    net = simulate_books(schedules(BOOKS), BOOKS, prices, _evidence(prices), window_end=WINDOW_END)[
        (ZERO_RECOVERY.label, HALF_SPREAD)
    ]
    arm, control = net[Portfolio.ARM], net[Portfolio.CONTROL]
    assert appended_hac(arm, control, MONTHS).observations == len(MONTHS) + 1
    assert isinstance(appended_hac(replace(arm, terminal_wealth=0.0), control, MONTHS), Unavailable)


def test_load_prices_matches_the_builder_and_refuses_ambiguity(tmp_path: Path) -> None:
    row = "2013-07-01,10,11,9,10,100,0,0,10\n"
    (tmp_path / "BRK-B.csv").write_text(row)
    assert set(load_prices(tmp_path, {"BRK.B"})) == {"BRK.B"}
    (tmp_path / "brk.b.csv").write_text(row)
    with pytest.raises(RuntimeError, match="2 mirror files"):
        load_prices(tmp_path, {"BRK.B"})
    with pytest.raises(RuntimeError, match="0 mirror files"):
        load_prices(tmp_path, {"MSFT"})


def test_book_symbols_refuse_a_spelling_the_evidence_loader_would_change() -> None:
    assert book_symbols(BOOKS) == {"S1", "S2", "S3", "S4"}
    lower = (replace(BOOKS[0], complete_case=BOOKS[0].complete_case | {"s5"}),)
    with pytest.raises(runner.ArtefactRefusal):
        book_symbols(lower)


def test_the_sealer_never_overwrites(tmp_path: Path) -> None:
    seal = sealer(tmp_path)
    try:
        raise RuntimeError("boom")
    except RuntimeError as exc:
        error = exc
    first = seal("gate", error)
    assert seal("gate", error) == first
    assert len(list(tmp_path.iterdir())) == 1


def test_jsonable_encodes_every_result_type() -> None:
    value = {
        (Portfolio.ARM, HALF_SPREAD): Unavailable("r"),
        date(2013, 7, 1): frozenset({"b", "a"}),
        "tri": Tri.REFUSED,
        "nan": math.nan,
        (2013, 7): (1.0, Refused("why")),
    }
    assert json.loads(json.dumps(jsonable(value), allow_nan=False)) == {
        f"A:{HALF_SPREAD}": {"reason": "r"},
        "2013-07-01": ["a", "b"],
        "tri": "R",
        "nan": "nan",
        "2013:7": [1.0, {"reason": "why"}],
    }
