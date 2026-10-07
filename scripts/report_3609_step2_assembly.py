"""#3609 step 2's report assembly: the verdict and every diagnostic over one panel, and the output payload.

Spec: ``docs/research/2026-10-06-3609-step2-factor-book.md`` (PR #3666) §"Decision rule" (verdict order),
§"Registration" ("Labels on every output") and §"Diagnostics". Pure functions over the loader's
:class:`~scripts.report_3609_step2.PanelMonth` list (stage A then stage B, consecutive formations); the ledger, the
pin checks and the command line wrap :func:`evaluate` in a later slice. A sibling of ``report_3609_step2`` because
the segment and universe modules import its ``PanelMonth``.

* **Verdict order.** :func:`evaluate` computes the run, the verdict and every diagnostic before it returns. A data
  refusal anywhere (the run's, the verdict's ``COMPARATOR_INVALID``, or a diagnostic input refusal such as
  ``ME_INVALID``) raises, so a ``REFUSED`` run yields no verdict, path or diagnostic (verdict order step 1). Every
  other status (``INSUFFICIENT``, ``G1_REFUSED``, ``FAIL``, ``PASS``) carries the path and the diagnostics, which are
  printed, never gated.
* **Inputs per formation:** the book's scores and universe (:func:`~scripts.report_3609_step2.score`); the
  cap-weighted reference's ME, the universe names' ME at s(M); the FF-12 industry of each universe name, which covers
  every name the book or the reference holds (both hold universe names only).
* **The boundary** is the stage-B boundary formation (2021-05), whose pre-trade state every path captures.
* **Windows** for the signal and segment blocks are the run's stage windows, as the operations and attribution
  blocks' are.
* **Not yet here:** §"Source rules"' construction counts (uninformative groups, membership patterns and
  identifier-decided selections, each with its book weight) are the next slice's block.
"""

from __future__ import annotations

import inspect
import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, fields, is_dataclass
from datetime import date
from enum import Enum
from typing import Any, Final, TypeGuard

import numpy as np

from app.services.factor_book_control import DRAWS
from app.services.factor_book_path import BoundaryState, Month, PathResult, book_decisions, month_of
from app.services.factor_book_references import reference_decisions
from app.services.factor_book_series import Scenario, SeriesRun, run_series, summarise
from app.services.strategy_result import AmbiguityArm
from scripts.report_3609_step2 import PanelMonth, formation_inputs, score
from scripts.report_3609_step2_attribution import Diagnostics as AttributionDiagnostics
from scripts.report_3609_step2_attribution import diagnostics as attribution_diagnostics
from scripts.report_3609_step2_operations import Operations, operations, stage_windows
from scripts.report_3609_step2_segments import Segments, segment_month, segments
from scripts.report_3609_step2_signals import SignalSummary, signals
from scripts.report_3609_step2_subbooks import SubBook, WindowMetrics, all_sub_books, all_window_metrics
from scripts.report_3609_step2_universe import UniverseDiagnostics
from scripts.report_3609_step2_universe import diagnostics as universe_diagnostics
from scripts.report_3609_step2_verdict import STAGE_B_FORMATIONS, Verdict, verdict

#: §"Registration", "Labels on every output".
LABELS: Final[Mapping[str, str]] = {
    "construction": "retrospectively filtered construction data",
    "stage A": "development",
    "stage B": "reused validation; integrity masks retrospective, not strictly point-in-time",
}
#: Premise 5 (step 1 premise 1): the survivorship regime of each sample, named on the verdict line.
SURVIVORSHIP: Final = (
    "survivorship: stage A spans 2014-09..2018 (terminations recorded, coverage unverified) and 2019 on (checked "
    "against a selected Form 25 set); stage B lies wholly in the Form 25-checked regime, which is not full-population "
    "survivorship-free"
)
#: The labels §"Diagnostics" puts on individual blocks.
NOTES: Final[Mapping[str, str]] = {
    "signals": "selection-conditioned: the families were chosen on stage A",
    "segments": "a proxy for market-segments.md's NYSE cells; selection-conditioned like the signal block",
    "sub_books": "attribution series: not tradable portfolios, no cash, and they do not sum to the book",
    "n_eff": "an estimate that captures dependence only up to the Newey-West bandwidth",
    "turnover": "realised, from one history: no expected-benefit model exists here",
    "industry": "the 2x flag is a same-universe diagnostic, not the benchmark cap",
    "operations": (
        "assumptions carried from step 0: real settlement, fractional holdings, zero slippage, gaps and fees, and "
        "pre-withholding returns for the book and B1 alike"
    ),
}


@dataclass(frozen=True)
class Report:
    run: SeriesRun
    verdict: Verdict
    operations: Operations
    attribution: AttributionDiagnostics
    #: Per arm, then signal.
    signals: Mapping[str, Mapping[str, SignalSummary]]
    universe: UniverseDiagnostics
    segments: Segments
    #: Per scenario, then band: the monthly series and trades by band.
    sub_books: Mapping[Scenario, Mapping[str, SubBook]]
    #: Per arm, band and window.
    sub_book_windows: Mapping[AmbiguityArm, Mapping[str, Mapping[str, WindowMetrics]]]


def boundary_formation(panel: Sequence[PanelMonth]) -> date:
    """The stage-B boundary formation's date; a panel without it is not the declared run."""
    found = [m.formation for m in panel if month_of(m.formation) == STAGE_B_FORMATIONS[0]]
    if len(found) != 1:
        raise ValueError(f"the panel holds {len(found)} formations in the boundary month {STAGE_B_FORMATIONS[0]}")
    return found[0]


def evaluate(
    panel: Sequence[PanelMonth],
    *,
    factors: Mapping[str, Mapping[Month, float]],
    cutoffs: Mapping[date, float],
    b1_saved: Mapping[str, Sequence[object]],
    b1_close: float,
    costs: Mapping[str, float],
    draws: int = DRAWS,
) -> Report:
    """The run, its verdict and every diagnostic. ``factors`` are G1's (FF5, momentum and RF), ``cutoffs`` are
    ``report_3609_step2_universe.read_cutoffs``'s, and ``b1_saved``, ``b1_close`` and ``costs`` are
    :func:`run_series`'s. A non-finite or non-positive ME never reaches ``me``: the loader and
    :func:`~app.services.factor_book.book_universe` (inside :func:`score`) refuse it ``ME_INVALID``."""
    boundary = boundary_formation(panel)
    scored = [score(month) for month in panel]
    formations = [formation_inputs(month, s) for month, s in zip(panel, scored, strict=True)]
    me = [{name: month.admitted[name].me for name in s.universe} for month, s in zip(panel, scored, strict=True)]
    run = run_series(formations, me, b1_saved=b1_saved, b1_close=b1_close, costs=costs, draws=draws, boundary=boundary)
    outcome = verdict(run, factors)
    book = book_decisions(formations).decisions
    reference = reference_decisions(formations, me)
    industries = [
        {name: month.admitted[name].industry for name in s.universe} for month, s in zip(panel, scored, strict=True)
    ]
    windows = stage_windows(run)
    universes = [s.universe for s in scored]
    return Report(
        run=run,
        verdict=outcome,
        operations=operations(run, book),
        attribution=attribution_diagnostics(run, factors, book, reference, industries),
        signals=signals(formations, [s.scores for s in scored], windows),
        universe=universe_diagnostics(panel, universes, book, run, cutoffs),
        segments=segments(
            [segment_month(month, s.universe, s.scores) for month, s in zip(panel, scored, strict=True)], windows
        ),
        sub_books=all_sub_books(book, run),
        sub_book_windows=all_window_metrics(book, run, factors),
    )


# --------------------------------------------------------------------------- output


def verdict_line(outcome: Verdict) -> str:
    """The status, its reason, the insufficient formations or a pass's annotations, and the survivorship regimes."""
    line = outcome.status if outcome.reason is None else f"{outcome.status}: {outcome.reason}"
    if outcome.insufficient:
        line += f" (formations {', '.join(d.isoformat() for d in outcome.insufficient)})"
    if outcome.annotations:
        line += f" [{'; '.join(outcome.annotations)}]"
    return f"{line}. {SURVIVORSHIP}."


def _is_month(value: object) -> TypeGuard[Month]:
    return (
        isinstance(value, tuple)
        and len(value) == 2
        and all(type(v) is int for v in value)
        and 1000 <= value[0]
        and 1 <= value[1] <= 12
    )


def _key(key: object) -> str:
    if isinstance(key, Enum):
        return str(key.value)
    if isinstance(key, date):
        return key.isoformat()
    if _is_month(key):
        year, month = key
        return f"{year:04d}-{month:02d}"
    if isinstance(key, tuple):
        return "|".join(_key(part) for part in key)
    if isinstance(key, str | int):
        return str(key)
    # No block keys by float (the strict-JSON payload test walks every block of a real run); a new key type must
    # choose its printed form here rather than fall through to ``str``.
    raise TypeError(f"no output key for {type(key).__name__}")


def jsonable(value: object) -> Any:
    """JSON-ready output: a dataclass becomes its fields plus its public properties (``passed``, ``failing``,
    ``insufficient``, ``flag`` …); months print ``YYYY-MM``, dates ISO, enums their value; a tuple key joins its parts
    with ``|``; a non-finite float prints as its ``repr`` (``nan``, ``inf``), never as a JSON ``NaN``. Two keys that
    print alike refuse."""
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, np.generic):
        return jsonable(value.item())
    if isinstance(value, float):
        return value if math.isfinite(value) else repr(value)
    if isinstance(value, int | str):
        return value
    if isinstance(value, date):
        return value.isoformat()
    if _is_month(value):
        return _key(value)
    if is_dataclass(value) and not isinstance(value, type):
        out = {f.name: jsonable(getattr(value, f.name)) for f in fields(value)}
        for name, member in inspect.getmembers(type(value)):
            if isinstance(member, property) and not name.startswith("_"):
                out[name] = jsonable(getattr(value, name))
        return out
    if isinstance(value, Mapping):
        mapped: dict[str, Any] = {}
        for k, v in value.items():
            key = _key(k)
            if key in mapped:
                raise ValueError(f"two output keys print as {key!r}")
            mapped[key] = jsonable(v)
        return mapped
    if isinstance(value, set | frozenset):
        return [jsonable(v) for v in sorted(value)]
    if isinstance(value, list | tuple):
        return [jsonable(v) for v in value]
    raise TypeError(f"no output form for {type(value).__name__}")


def _record(boundary: BoundaryState | None) -> dict[str, Any] | None:
    return None if boundary is None else boundary.record()


def payload(report: Report) -> dict[str, Any]:
    """The output file: the verdict line and its fields, the labels, the path, and every diagnostic block with the
    notes §"Diagnostics" requires. The monthly series (IC and spread per signal and cell, sub-book months) are in
    their blocks. Each path carries its trades, its imputed realisations (listed apart: no order, no cost) and its
    order notional and cost per trade category; each control draw carries its summary, categories included
    (§"References and the control", trade categories). Boundary states print as their canonical record."""
    run = report.run

    def path(paths: Mapping[Scenario, PathResult]) -> dict[str, Any]:
        return {
            _key(s): jsonable(
                {
                    "returns": p.returns,
                    "nav": p.nav,
                    "turnover": p.turnover,
                    "holdings": p.holdings,
                    "nonpositive": p.nonpositive,
                    "boundary": _record(p.boundary),
                    "categories": summarise(p).categories,
                    "trades": p.trades,
                    "realisations": p.realisations,
                }
            )
            for s, p in paths.items()
        }

    return {
        "verdict": {"line": verdict_line(report.verdict), **jsonable(report.verdict)},
        "labels": dict(LABELS),
        "notes": dict(NOTES),
        "path": {
            "months": jsonable(run.months),
            "book": path(run.book),
            "equal_weight": path(run.equal_weight),
            "cap_weighted": path(run.cap_weighted),
            "control": {
                _key(s): [
                    jsonable({f.name: getattr(d, f.name) for f in fields(d)} | {"boundary": _record(d.boundary)})
                    for d in draws
                ]
                for s, draws in run.control.items()
            },
            "b1": jsonable(run.b1),
            "pools": jsonable(run.pools),
            "insufficient": jsonable(run.insufficient),
        },
        "operations": jsonable(report.operations),
        "attribution": jsonable(report.attribution),
        "signals": jsonable(report.signals),
        "universe": jsonable(report.universe),
        "segments": jsonable(report.segments),
        "sub_books": {"monthly": jsonable(report.sub_books), "windows": jsonable(report.sub_book_windows)},
    }


__all__ = [
    "LABELS",
    "NOTES",
    "SURVIVORSHIP",
    "Report",
    "boundary_formation",
    "evaluate",
    "jsonable",
    "payload",
    "verdict_line",
]
