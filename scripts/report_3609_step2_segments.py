"""#3609 step 2's segment signal diagnostics (slice 3c-v(f)): printed, never gated.

Spec: ``docs/research/2026-10-06-3609-step2-factor-book.md`` §"Diagnostics" (PR #3666): "Two size cells",
"Segments" ("Each cell: the signal block above, on its own names") and "FF-12 industry". The per-band sub-books are
a separate slice. Labelled a proxy for ``market-segments.md``'s NYSE cells, and selection-conditioned like the signal
block (the families were chosen on stage A).

* **Two size cells:** the book universe, and the admitted names outside it. Scores are standardised within (size
  cell, FF-12 industry, M) by the book's own scorer (:func:`~app.services.factor_book.composite_scores`), among the
  names with the inputs, whatever their price. The universe cell's scores are the book's.
* **Cost-band reporting cells** are assigned afterwards, from each name's raw close at s(M) by step 0's band
  (:func:`~app.services.factor_book_path.entry_band`). A missing, non-finite or non-positive close puts the name in a
  ``price_unavailable`` cell, counted per formation; it never refuses. A name's cell is set at each M.
* **Each (band, size) cell** gets the signal block of :mod:`scripts.report_3609_step2_signals` (IC, quintile spread,
  their summaries and n_eff), per arm and signal, on its own names: those in the cell with the signal's score and a
  holding-month return in the arm.
* **FF-12 industry:** a marginal partition of the book universe (``UNCLASSIFIED`` is its own), not crossed with the
  cells; the IC block only, on the book's scores.
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from typing import Final

from app.services.factor_book import CHARACTERISTICS, Exact, Scores, composite_scores
from app.services.factor_book_path import HoldingReturn, Month, entry_band, month_of, next_month
from app.services.factor_book_series import ARMS
from app.services.strategy_result import AmbiguityArm
from scripts.report_3609_step2 import PanelMonth
from scripts.report_3609_step2_operations import Window
from scripts.report_3609_step2_signals import SIGNALS, Monthly, SeriesSummary, SignalSummary, ic, quintile_spread
from scripts.report_3609_step2_signals import summarise as summarise_series

UNIVERSE_CELL: Final = "universe"
OUTSIDE_CELL: Final = "outside"
SIZE_CELLS: Final = (UNIVERSE_CELL, OUTSIDE_CELL)
PRICE_UNAVAILABLE: Final = "price_unavailable"
#: (cost band, size cell).
Cell = tuple[str, str]


def band_of(close: float | None) -> str:
    """Step 0's band label for a raw close at s(M), or ``PRICE_UNAVAILABLE``."""
    if close is None or not (math.isfinite(close) and close > 0):
        return PRICE_UNAVAILABLE
    return entry_band(close)[0]


@dataclass(frozen=True)
class SegmentMonth:
    """One formation's cells: each admitted name's cell and return, each size cell's scores."""

    formation: date
    scores: Mapping[str, Scores]
    cells: Mapping[int, Cell]
    #: FF-12 industry of every admitted name.
    industry: Mapping[int, str]
    returns: Mapping[int, HoldingReturn]

    @property
    def price_unavailable(self) -> int:
        return sum(1 for band, _ in self.cells.values() if band == PRICE_UNAVAILABLE)


def segment_month(month: PanelMonth, universe: Sequence[int], book_scores: Scores) -> SegmentMonth:
    """``universe`` and ``book_scores`` are the book's at this formation (``report_3609_step2.score``)."""
    inside = set(universe)
    if not inside <= month.admitted.keys():
        raise ValueError(f"{month.formation}: the universe holds a name outside the admitted population")
    outside = sorted(month.admitted.keys() - inside)
    signed = {
        c: {n: month.admitted[n].signed[c] for n in outside if c in month.admitted[n].signed} for c in CHARACTERISTICS
    }
    industry = {name: panel.industry for name, panel in month.admitted.items()}
    scores = {UNIVERSE_CELL: book_scores, OUTSIDE_CELL: composite_scores(outside, industry, signed)}
    cells = {
        name: (band_of(month.close.get(name)), UNIVERSE_CELL if name in inside else OUTSIDE_CELL)
        for name in month.admitted
    }
    returns = {name: panel.holding for name, panel in month.admitted.items()}
    return SegmentMonth(month.formation, scores, cells, industry, returns)


def _population(
    names: Sequence[int], scores: Mapping[int, Exact], returns: Mapping[int, HoldingReturn], arm: AmbiguityArm
) -> tuple[dict[int, Exact], dict[int, float]]:
    chosen = sorted(n for n in names if n in scores and arm in returns[n].by_arm)
    return {n: scores[n] for n in chosen}, {n: returns[n].by_arm[arm] for n in chosen}


def cell_monthly(months: Sequence[SegmentMonth], cell: Cell, signal: str, arm: AmbiguityArm) -> Monthly:
    """The signal block's monthly IC and spread on ``cell``'s names, keyed by holding month M + 1."""
    ics: dict[Month, float | None] = {}
    spreads: dict[Month, float | None] = {}
    sizes: dict[Month, int] = {}
    for month in months:
        held = next_month(month_of(month.formation))
        names = [n for n, c in month.cells.items() if c == cell]
        s, r = _population(names, month.scores[cell[1]].by_operation.get(signal, {}), month.returns, arm)
        ics[held], spreads[held], sizes[held] = ic(s, r), quintile_spread(s, r), len(s)
    return Monthly(ics, spreads, sizes)


@dataclass(frozen=True)
class IndustryIc:
    #: Per holding month: the IC (``None`` where undefined) and the population size.
    ic: Mapping[Month, float | None]
    names: Mapping[Month, int]
    #: Per window label.
    summaries: Mapping[str, SeriesSummary]


def industry_monthly(
    months: Sequence[SegmentMonth], industry: str, signal: str, arm: AmbiguityArm
) -> tuple[dict[Month, float | None], dict[Month, int]]:
    """The IC on the book universe's names in ``industry``, on the book's scores."""
    ics: dict[Month, float | None] = {}
    sizes: dict[Month, int] = {}
    for month in months:
        held = next_month(month_of(month.formation))
        names = [n for n, (_, size) in month.cells.items() if size == UNIVERSE_CELL and month.industry[n] == industry]
        s, r = _population(names, month.scores[UNIVERSE_CELL].by_operation.get(signal, {}), month.returns, arm)
        ics[held], sizes[held] = ic(s, r), len(s)
    return ics, sizes


@dataclass(frozen=True)
class Segments:
    #: Per arm, then cell, then signal.
    cells: Mapping[AmbiguityArm, Mapping[Cell, Mapping[str, SignalSummary]]]
    #: Per arm, then FF-12 industry, then signal.
    industries: Mapping[AmbiguityArm, Mapping[str, Mapping[str, IndustryIc]]]
    #: Per formation month: names in the ``price_unavailable`` cell.
    price_unavailable: Mapping[Month, int]


def segments(months: Sequence[SegmentMonth], windows: Sequence[Window]) -> Segments:
    """Every figure in this module, over every cell and industry seen at any formation."""
    cells = sorted({c for m in months for c in m.cells.values()})
    industries = sorted({m.industry[n] for m in months for n, (_, s) in m.cells.items() if s == UNIVERSE_CELL})
    by_cell: dict[AmbiguityArm, dict[Cell, dict[str, SignalSummary]]] = {}
    by_industry: dict[AmbiguityArm, dict[str, dict[str, IndustryIc]]] = {}
    for arm in ARMS:
        by_cell[arm] = {}
        for cell in cells:
            by_cell[arm][cell] = {}
            for signal in SIGNALS:
                series = cell_monthly(months, cell, signal, arm)
                by_cell[arm][cell][signal] = SignalSummary(
                    series,
                    {w.label: summarise_series(series.ic, w, ic_ir=True) for w in windows},
                    {w.label: summarise_series(series.spread, w, ic_ir=False) for w in windows},
                )
        by_industry[arm] = {}
        for industry in industries:
            by_industry[arm][industry] = {}
            for signal in SIGNALS:
                ics, sizes = industry_monthly(months, industry, signal, arm)
                summaries = {w.label: summarise_series(ics, w, ic_ir=True) for w in windows}
                by_industry[arm][industry][signal] = IndustryIc(ics, sizes, summaries)
    unavailable = {month_of(m.formation): m.price_unavailable for m in months}
    return Segments(by_cell, by_industry, unavailable)


__all__ = [
    "OUTSIDE_CELL",
    "PRICE_UNAVAILABLE",
    "SIZE_CELLS",
    "UNIVERSE_CELL",
    "Cell",
    "IndustryIc",
    "SegmentMonth",
    "Segments",
    "band_of",
    "cell_monthly",
    "industry_monthly",
    "segment_month",
    "segments",
]
