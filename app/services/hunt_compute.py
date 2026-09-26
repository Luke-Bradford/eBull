"""The pattern-hunt harness's computation over a loaded panel: eligibility, cohorts, cells.

#3385 slice 3c-iii, spec ``docs/proposals/ta/2026-09-26-3385-hunt-harness.md`` (v5):
"Eligibility", "Signals", "The evaluator" and "Inference". Pure: ``hunt_harness`` loads
the panel (the only hunt module that may import a research price reader) and calls
:func:`compute_panel`. This module's code is part of ``HUNT_HARNESS_MODEL_ID``.

Per formation t (spec "A trial" and "The evaluator"):

1. **Eligible** = admitted series with a bar on t whose AS-TRADED bar is valid (O, H, L,
   C finite and > 0; L ≤ min(O, C); H ≥ max(O, C); volume finite and > 0; H > L). Each
   exclusion reason is counted per formation. The reason is decided by the loader
   (:func:`bar_exclusion`) on the printed bar, because a derived ratio-basis level is a
   rounded quotient and can flip an equality.
2. The signal sees :func:`hunt_view.rebased_view` of the eligible set. A missing or
   non-finite score makes the name **unscored**; a key outside the eligible set, or a
   score that is not a real number, is a signal defect and raises (an infrastructure
   error: no outcome, the registration is retried).
3. C(t) = the scored names; A(t) = :func:`hunt_evaluator.select_arm`. An empty C(t) is
   an idle slot.

Cells = ``PROGRAMME_POLICIES`` × {with, without dividends} × {base, stress}. Base cost is
the frozen half-spread band at the as-traded entry price plus the proportional commission;
stress charges ARM positions max(base band, ``UNKNOWN_NOMINAL_PRICE_BAND``). The canonical
cell is base, with dividends, ``zero_recovery``: its active series is stored, and the
outcome is ``refused`` exactly when its statistics refused.
"""

from __future__ import annotations

import math
import platform
from array import array
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from typing import Any, Final, Literal

import numpy as np

from app.services import hunt_books, hunt_evaluator, hunt_inference
from app.services.cost_model import UNKNOWN_NOMINAL_PRICE_BAND, cost_band_for
from app.services.hunt_evaluator import BookSeries, Cohort, Point, SeriesPrices
from app.services.hunt_inference import StatRefused
from app.services.hunt_view import Bars, Dividends, SessionDate, SignalView, rebased_view
from app.services.r6_exclusion_trial import PROGRAMME_POLICIES, TerminationPolicy
from app.services.series_termination import TerminationClass

Cost = Literal["base", "stress"]
COSTS: Final[tuple[Cost, ...]] = ("base", "stress")

#: Why an as-traded bar is not eligible, in the order they are tested. Code 0 = valid.
BAR_EXCLUSIONS: Final[tuple[str, ...]] = (
    "invalid_price",
    "low_above_body",
    "high_below_body",
    "non_positive_volume",
    "zero_range",
)
#: A series whose loaded life spans t but which printed no bar on t.
NO_BAR: Final = "no_bar"

CANONICAL_POLICY: Final = "zero_recovery"
COST_REGIME: Final = "tariff-2026-counterfactual"
#: The descriptive volatility tilt's window (spec "Descriptive tilts").
TILT_VOLATILITY_SESSIONS: Final = 63

Signal = Callable[[int, SignalView, Mapping[str, Any]], Mapping[int, float]]


def _finite_positive(value: float) -> bool:
    return math.isfinite(value) and value > 0.0


def bar_exclusion(o: float, h: float, low: float, c: float, volume: float) -> int:
    """0 when the as-traded bar is eligible, else 1 + the index of its first failing rule."""
    if not all(_finite_positive(value) for value in (o, h, low, c)):
        return 1
    if low > min(o, c):
        return 2
    if h < max(o, c):
        return 3
    if not _finite_positive(volume):
        return 4
    if not h > low:
        return 5
    return 0


def cell_key(policy: str, with_dividends: bool, cost: Cost) -> str:
    return f"{policy}|{'with' if with_dividends else 'without'}_dividends|{cost}"


CANONICAL_CELL: Final = cell_key(CANONICAL_POLICY, True, "base")


# ---------------------------------------------------------------------------
# The panel
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PanelSeries:
    """One admitted series, keyed by session ordinals.

    ``ratio`` holds the ratio-basis bars (returns and the signal view); ``traded_open`` /
    ``traded_close`` the printed levels (cost bands, the rebase, the log-price tilt),
    aligned one-for-one. ``exclusion`` is :func:`bar_exclusion` of each printed bar.
    ``dividends`` maps an ex-date ordinal to the cash amount on the ratio basis.
    """

    series_id: int
    ratio: Bars
    traded_open: Sequence[float]
    traded_close: Sequence[float]
    exclusion: bytes
    dividends: Mapping[int, float]
    terminal_ordinal: int | None
    termination_class: TerminationClass | None
    _index: array[int] = field(init=False, repr=False, compare=False)
    #: ``dividends`` as the view's parallel arrays, built once here, never per formation (#3386).
    dividend_arrays: Dividends = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        count = len(self.ratio.ordinals)
        if not count:
            raise ValueError(f"series {self.series_id} has no bars; the loader drops it")
        if not (len(self.traded_open) == len(self.traded_close) == len(self.exclusion) == count):
            raise ValueError(f"series {self.series_id}: the printed columns do not align with the ratio bars")
        if (self.terminal_ordinal is None) != (self.termination_class is None):
            raise ValueError(f"series {self.series_id}: a terminal bar needs a class, and only a terminal bar has one")
        if self.terminal_ordinal is not None and self.ratio.ordinals[-1] > self.terminal_ordinal:
            raise ValueError(f"series {self.series_id} has a bar after its terminal bar")
        first = self.ratio.ordinals[0]
        index = array("l", [-1]) * (self.ratio.ordinals[-1] - first + 1)
        for position, ordinal in enumerate(self.ratio.ordinals):
            index[ordinal - first] = position
        object.__setattr__(self, "_index", index)
        paid = sorted(self.dividends.items())
        object.__setattr__(
            self,
            "dividend_arrays",
            Dividends(array("l", [o for o, _ in paid]), array("d", [float(a) for _, a in paid])),
        )

    @property
    def first(self) -> int:
        return self.ratio.ordinals[0]

    @property
    def last(self) -> int:
        return self.ratio.ordinals[-1]

    def position(self, ordinal: int) -> int:
        """The bar's position on ``ordinal``, or −1 when no bar printed that session."""
        offset = ordinal - self.first
        return self._index[offset] if 0 <= offset < len(self._index) else -1


class _Column(Mapping[int, float]):
    """One ratio-basis price column as ``ordinal → price``, without copying it."""

    __slots__ = ("_series", "_values")

    def __init__(self, series: PanelSeries, values: Sequence[float]) -> None:
        self._series = series
        self._values = values

    def __getitem__(self, ordinal: int) -> float:
        position = self._series.position(ordinal)
        if position < 0:
            raise KeyError(ordinal)
        return self._values[position]

    def __iter__(self) -> Iterator[int]:
        return iter(self._series.ratio.ordinals)

    def __len__(self) -> int:
        return len(self._series.ratio.ordinals)


@dataclass(frozen=True)
class HuntPanel:
    """Everything one computation reads, loaded before the first formation."""

    sessions: tuple[date, ...]
    series: Mapping[int, PanelSeries]
    #: The descriptive regime label of each session (``market_regime`` on the research benchmark).
    regime_labels: Sequence[str]
    #: What the loader dropped or could not place, by reason (recorded on the outcome).
    load_counts: Mapping[str, int]

    def __post_init__(self) -> None:
        if len(self.regime_labels) != len(self.sessions):
            raise ValueError(f"{len(self.regime_labels)} regime labels for {len(self.sessions)} sessions")


@dataclass(frozen=True)
class ComputeParams:
    """The spec fields the computation reads, plus the split's calendar bounds."""

    split_start: date
    split_end: date
    embargo: int
    lag: int
    h: int
    entry_point: Point
    exit_point: Point
    sign: int
    selection: float
    constants: Mapping[str, Any]
    commission: float

    def __post_init__(self) -> None:
        if not (math.isfinite(self.commission) and 0.0 <= self.commission < 1.0):
            raise ValueError(f"commission must be in [0, 1), got {self.commission}")


@dataclass(frozen=True)
class PanelOutcome:
    status: Literal["computed", "refused"]
    statistics: Mapping[str, Any]
    active_series: tuple[float, ...] | None


# ---------------------------------------------------------------------------
# Formations
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _Formations:
    cohorts: dict[int, Cohort]
    counts: dict[str, list[int]]
    #: Per formation with a cohort: (arm − control) mean log printed close, and mean trailing volatility.
    log_price_tilts: list[float]
    volatility_tilts: list[float]


def _trailing_volatility(series: PanelSeries, position: int, t: int) -> float | None:
    """ddof=1 stdev of close-to-close log returns over the bars in [t − 63, t] (descriptive)."""
    ordinals, closes = series.ratio.ordinals, series.ratio.close
    start = position
    while start > 0 and ordinals[start - 1] >= t - TILT_VOLATILITY_SESSIONS:
        start -= 1
    window = [closes[i] for i in range(start, position + 1)]
    if len(window) < 3 or not all(_finite_positive(value) for value in window):
        return None
    returns = np.diff(np.log(np.asarray(window, dtype=np.float64)))
    return float(np.std(returns, ddof=1))


def _mean_difference(arm: list[float], control: list[float]) -> float | None:
    if not arm or not control:
        return None
    return math.fsum(arm) / len(arm) - math.fsum(control) / len(control)


def _form_cohorts(panel: HuntPanel, grid: hunt_evaluator.Grid, params: ComputeParams, signal: Signal) -> _Formations:
    formation_set = set(grid.formations)
    trading: dict[int, list[int]] = {t: [] for t in grid.formations}
    # Series whose loaded life [first, last] spans t, by a difference array over sessions.
    life_edges = [0] * (len(panel.sessions) + 1)
    for series_id, series in panel.series.items():
        for ordinal in series.ratio.ordinals:
            if ordinal in formation_set:
                trading[ordinal].append(series_id)
        life_edges[series.first] += 1
        life_edges[series.last + 1] -= 1
    alive = 0
    spanning: dict[int, int] = {}
    for ordinal, edge in enumerate(life_edges[:-1]):
        alive += edge
        if ordinal in formation_set:
            spanning[ordinal] = alive

    reasons = (*BAR_EXCLUSIONS, NO_BAR)
    # The view's dates, once per computation (#3386): int tuples, never date objects.
    calendar: tuple[SessionDate, ...] = tuple((d.year, d.month, d.day, d.weekday()) for d in panel.sessions)
    counts: dict[str, list[int]] = {name: [] for name in ("eligible", "scored", "unscored", "arm", *reasons)}
    cohorts: dict[int, Cohort] = {}
    log_price_tilts: list[float] = []
    volatility_tilts: list[float] = []
    for t in grid.formations:
        excluded = dict.fromkeys(BAR_EXCLUSIONS, 0)
        eligible: dict[int, Bars] = {}
        traded_close: dict[int, float] = {}
        paid: dict[int, Dividends] = {}
        positions: dict[int, int] = {}
        for series_id in trading[t]:
            series = panel.series[series_id]
            position = series.position(t)
            code = series.exclusion[position]
            if code:
                excluded[BAR_EXCLUSIONS[code - 1]] += 1
                continue
            eligible[series_id] = series.ratio
            traded_close[series_id] = series.traded_close[position]
            paid[series_id] = series.dividend_arrays
            positions[series_id] = position
        view = rebased_view(t, eligible, traded_close, sessions=calendar, dividends=paid)
        scores = signal(t, view, params.constants)
        if not isinstance(scores, Mapping):
            raise TypeError(f"signal returned {type(scores).__name__}, not a mapping")
        outside = set(scores) - set(eligible)
        if outside:
            raise ValueError(
                f"signal scored {len(outside)} names outside the eligible set on {t}: {sorted(outside)[:3]}"
            )
        scored: dict[int, float] = {}
        for series_id, score in scores.items():
            if isinstance(score, bool) or not isinstance(score, int | float):
                raise TypeError(f"signal score for {series_id} on {t} is {type(score).__name__}, not a real number")
            if math.isfinite(score):
                scored[series_id] = float(score)
        arm = hunt_evaluator.select_arm(scored, sign=params.sign, fraction=params.selection) if scored else frozenset()
        for reason in BAR_EXCLUSIONS:
            counts[reason].append(excluded[reason])
        counts[NO_BAR].append(spanning[t] - len(trading[t]))
        counts["eligible"].append(len(eligible))
        counts["scored"].append(len(scored))
        counts["unscored"].append(len(eligible) - len(scored))
        counts["arm"].append(len(arm))
        if not scored:
            continue
        control = frozenset(scored)
        cohorts[t] = Cohort(control=control, arm=arm)

        log_price = {sid: math.log(traded_close[sid]) for sid in control}
        tilt = _mean_difference([log_price[s] for s in arm], list(log_price.values()))
        if tilt is not None:
            log_price_tilts.append(tilt)
        volatility = {sid: _trailing_volatility(panel.series[sid], positions[sid], t) for sid in control}
        tilt = _mean_difference(
            [v for s in arm if (v := volatility[s]) is not None],
            [v for v in volatility.values() if v is not None],
        )
        if tilt is not None:
            volatility_tilts.append(tilt)
    return _Formations(cohorts, counts, log_price_tilts, volatility_tilts)


# ---------------------------------------------------------------------------
# Cells
# ---------------------------------------------------------------------------


def _half_spreads(panel: HuntPanel, params: ComputeParams) -> Callable[[Cost], hunt_evaluator.HalfSpread]:
    base_cache: dict[tuple[int, int], float] = {}
    stress_band = float(UNKNOWN_NOMINAL_PRICE_BAND.half_spread)

    def base_band(series_id: int, e: int) -> float:
        key = (series_id, e)
        cached = base_cache.get(key)
        if cached is None:
            series = panel.series[series_id]
            position = series.position(e)
            prices = series.traded_open if params.entry_point == "open" else series.traded_close
            price = prices[position] if position >= 0 else math.nan
            if not _finite_positive(price):
                # The evaluator charges only an entered position, whose ratio price is valid,
                # and a valid ratio price is a valid printed price over a positive scale.
                raise ValueError(f"series {series_id} has no valid printed {params.entry_point} on {e} to price")
            cached = base_cache[key] = float(cost_band_for(Decimal(repr(price)), price_basis="as_traded").half_spread)
        return cached

    def for_cost(cost: Cost) -> hunt_evaluator.HalfSpread:
        def half_spread(series_id: int, e: int, book: hunt_evaluator.Book) -> float:
            band = base_band(series_id, e)
            if cost == "stress" and book == "arm":
                band = max(band, stress_band)
            return band + params.commission

        return half_spread

    return for_cost


def _ols_slope(active: Sequence[float], control: Sequence[float]) -> float | None:
    """Slope of a on r_C (descriptive)."""
    if len(active) < 2:
        return None
    x = np.asarray(control, dtype=np.float64)
    y = np.asarray(active, dtype=np.float64)
    dx = x - x.mean()
    denominator = float(dx @ dx)
    if denominator <= 0.0 or not math.isfinite(denominator):
        return None
    slope = float(dx @ (y - y.mean())) / denominator
    return slope if math.isfinite(slope) else None


def _describe(values: list[float]) -> dict[str, Any]:
    return {"mean": math.fsum(values) / len(values) if values else None, "formations": len(values)}


def compute_panel(panel: HuntPanel, params: ComputeParams, signal: Signal) -> PanelOutcome:
    """Every cell's statistics, the canonical active series and the descriptive readouts."""
    grid = hunt_evaluator.formation_grid(
        panel.sessions,
        start=params.split_start,
        end=params.split_end,
        lag=params.lag,
        h=params.h,
        embargo=params.embargo,
    )
    base_statistics: dict[str, Any] = {
        "cost_regime": COST_REGIME,
        "canonical_cell": CANONICAL_CELL,
        "load": dict(panel.load_counts),
        "environment": {"python": platform.python_version(), "numpy": np.__version__},
    }
    if isinstance(grid, StatRefused):
        return PanelOutcome("refused", {**base_statistics, "grid": grid.form()}, None)

    formations = _form_cohorts(panel, grid, params, signal)
    names = {name for cohort in formations.cohorts.values() for name in cohort.control}
    prices = {
        name: SeriesPrices(
            opens=_Column(panel.series[name], panel.series[name].ratio.open),
            closes=_Column(panel.series[name], panel.series[name].ratio.close),
            dividends=panel.series[name].dividends,
            terminal_ordinal=panel.series[name].terminal_ordinal,
        )
        for name in names
    }
    packed = hunt_books.pack_prices(prices)
    half_spreads = _half_spreads(panel, params)

    cells: dict[str, Any] = {}
    canonical_books: BookSeries | None = None
    canonical_refused = True
    policy: TerminationPolicy
    for policy in PROGRAMME_POLICIES:
        fractions = {
            name: policy.terminal_fraction(termination_class)
            for name in names
            if (termination_class := panel.series[name].termination_class) is not None
        }
        for with_dividends in (True, False):
            for cost in COSTS:
                key = cell_key(policy.label, with_dividends, cost)
                timeline: dict[str, Any] = {
                    "lag": params.lag,
                    "h": params.h,
                    "entry_point": params.entry_point,
                    "exit_point": params.exit_point,
                    "half_spread": half_spreads(cost),
                    "terminal_fractions": fractions,
                    "with_dividends": with_dividends,
                }
                books = hunt_books.evaluate_books_fast(grid, formations.cohorts, packed, **timeline)
                if books is None:
                    # A refusal condition was met: the reference evaluator names it.
                    books = hunt_evaluator.evaluate_books(grid, formations.cohorts, prices, **timeline)
                if isinstance(books, StatRefused):
                    cells[key] = books.form()
                    continue
                statistics = hunt_inference.cell_statistics(
                    books.active, h=params.h, entered_formations=books.entered_formations
                )
                cells[key] = statistics.form()
                if key == CANONICAL_CELL:
                    canonical_books = books
                    canonical_refused = isinstance(statistics, StatRefused)

    sessions = panel.sessions
    grid_form = {
        "first_session": sessions[grid.first].isoformat(),
        "last_session": sessions[grid.last].isoformat(),
        "observations": len(grid.sessions),
        "formations": len(grid.formations),
        "embargo": params.embargo,
    }
    per_formation = {"formation_dates": [sessions[t].isoformat() for t in grid.formations], **formations.counts}
    statistics_out: dict[str, Any] = {
        **base_statistics,
        "grid": grid_form,
        "per_formation": per_formation,
        "cells": cells,
        "tilts": {
            "log_price": _describe(formations.log_price_tilts),
            "volatility_63": _describe(formations.volatility_tilts),
        },
    }
    if canonical_books is None:
        return PanelOutcome("refused", statistics_out, None)
    labels = [panel.regime_labels[d] for d in grid.sessions]
    statistics_out["regimes"] = {
        label: {"mean": mean, "sessions": count}
        for label, (mean, count) in hunt_inference.regime_readout(canonical_books.active, labels).items()
    }
    statistics_out["tilts"]["slope_on_control"] = _ols_slope(canonical_books.active, canonical_books.control)
    return PanelOutcome("refused" if canonical_refused else "computed", statistics_out, canonical_books.active)


__all__ = [
    "BAR_EXCLUSIONS",
    "CANONICAL_CELL",
    "COSTS",
    "COST_REGIME",
    "NO_BAR",
    "TILT_VOLATILITY_SESSIONS",
    "ComputeParams",
    "HuntPanel",
    "PanelOutcome",
    "PanelSeries",
    "Signal",
    "bar_exclusion",
    "cell_key",
    "compute_panel",
]
