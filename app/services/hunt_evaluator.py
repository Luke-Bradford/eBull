"""The pattern-hunt harness's evaluator: arm minus matched control, on a calendar-time grid.

#3385 slice 3b, spec ``docs/proposals/ta/2026-09-26-3385-hunt-harness.md`` (v5), "Splits
and purge" (the grid) and "The evaluator". Pure: no database and no reader. Slice 3c
loads the ratio-basis prices, dividends, half-spreads and termination for one cell and
calls :func:`evaluate_books`; ``hunt_inference`` turns the active series into statistics.

Construction, in brief (every rule is the spec's; nothing here is new):

- **Grid.** Calendar only: a formation t is usable when t is in the split, past the
  embargo, and its exit x = t + lag + h − 1 is in the split too. The grid runs from the
  first usable entry to the last usable exit; idle sessions return 0 in both books.
- **Slots (Jegadeesh & Titman 1993 §I).** h slots of weight 1/h; slot k on session d holds
  the cohort formed at d − lag − k. A book's return is the mean slot gross factor − 1.
- **Within a cohort, buy-and-hold on value factors.** Each position starts at V = 1;
  nothing moves between positions after formation, so the cohort factor on d is
  ΣV(d) / ΣV(d − 1). A position never entered stays cash at V = 1.
- **A position** is tracked as a value and a reference price: V is always units × ref, so
  a mark q multiplies V by (q + D) / ref, where D is the dividend cash per unit ex-dated
  since the last mark. Costs enter as fill prices (``buy`` = p·(1 + h_s), ``sell`` =
  p·(1 − h_s)), so every factor is a ratio of non-negative prices.

⚠ Order within a session: entry (at its point) → dividend accrual (ex-date after e only:
a buyer on the ex-date does not receive it) → exit if d = x ≤ ``last_bar`` (exit wins
over termination; obligations 21/23) → close mark → terminal haircut if d = ``last_bar``
< x. An open exit uses open(x) only, never close(x); with no valid open(x) it exits at
the last available close, never a later price (obligation 26).
"""

from __future__ import annotations

import math
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from fractions import Fraction
from typing import Final, Literal

from app.services.hunt_inference import StatRefused

Point = Literal["open", "close"]
Book = Literal["arm", "control"]
BOOKS: Final[tuple[Book, ...]] = ("arm", "control")

#: A book whose gross factor on one session is at or below this refuses ``book_ruin``.
BOOK_RUIN_FACTOR: Final = 1e-9


def _valid_price(value: float | None) -> bool:
    return value is not None and math.isfinite(value) and value > 0.0


# ---------------------------------------------------------------------------
# Inputs
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SeriesPrices:
    """One series on the ratio basis, keyed by session ordinal.

    A price point that is absent, non-finite or ≤ 0 is not a price (spec "Contract
    decisions", bar validity after formation). ``dividends`` maps an ex-date ordinal to
    the cash amount already on the ratio basis. ``terminal_ordinal`` is the stored
    ``last_bar`` of a terminating series and ``None`` for a live one.
    """

    opens: Mapping[int, float]
    closes: Mapping[int, float]
    dividends: Mapping[int, float]
    terminal_ordinal: int | None

    def __post_init__(self) -> None:
        if self.terminal_ordinal is not None:
            late = [d for d in (*self.opens, *self.closes) if d > self.terminal_ordinal]
            if late:
                raise ValueError(f"a price after the terminal bar {self.terminal_ordinal}: {sorted(late)[:3]}")

    def price(self, ordinal: int, point: Point) -> float | None:
        value = (self.opens if point == "open" else self.closes).get(ordinal)
        return value if _valid_price(value) else None


@dataclass(frozen=True)
class Cohort:
    """C(t), every eligible scored name, and A(t) ⊆ C(t), the selected names."""

    control: frozenset[int]
    arm: frozenset[int]

    def __post_init__(self) -> None:
        if not self.control:
            raise ValueError("an empty control cohort is an idle slot, not a cohort")
        if not self.arm or not self.arm <= self.control:
            raise ValueError("the arm must be a non-empty subset of the control")


@dataclass(frozen=True)
class Grid:
    """Calendar-usable formations and the fixed grid of sessions they can hold."""

    formations: tuple[int, ...]
    first: int
    last: int

    @property
    def sessions(self) -> range:
        return range(self.first, self.last + 1)


def formation_grid(
    sessions: Sequence[date], *, start: date, end: date, lag: int, h: int, embargo: int
) -> Grid | StatRefused:
    """The fixed grid of one split, from the calendar alone (no price is read).

    Ordinals index ``sessions``. Purge: t, e = t + lag and x = e + h − 1 all fall in
    [start, end]. Embargo: formations in the first ``embargo`` sessions of the split are
    dropped (0 for discovery, 63 for validation and holdout).
    """
    if lag < 1 or h < 1 or embargo < 0:
        raise ValueError(f"bad timeline lag={lag} h={h} embargo={embargo}")
    if any(later <= earlier for earlier, later in zip(sessions, sessions[1:], strict=False)):
        raise ValueError("sessions must be strictly increasing")
    inside = [ordinal for ordinal, day in enumerate(sessions) if start <= day <= end]
    if not inside:
        return StatRefused("empty_grid", f"no session in [{start}, {end}]")
    first_in, last_in = inside[0], inside[-1]
    formations = tuple(range(first_in + embargo, last_in - lag - h + 2))
    if not formations:
        return StatRefused("empty_grid", f"no calendar-usable formation in [{start}, {end}]")
    return Grid(formations=formations, first=formations[0] + lag, last=formations[-1] + lag + h - 1)


def selection_count(fraction: float, scored: int) -> int:
    """⌈f · N⌉, computed on the decimal ``repr`` of f: the stored spec's canonical form is
    that ``repr``, and the float product 0.07 * 100 is 7.000000000000001, whose ceiling
    would be 8."""
    if scored < 0:
        raise ValueError(f"scored must be >= 0, got {scored}")
    return math.ceil(Fraction(repr(fraction)) * scored)


def select_arm(scores: Mapping[int, float], *, sign: int, fraction: float) -> frozenset[int]:
    """The ⌈f · N⌉ names at the top of sign · score; every name tied at the cut is included."""
    if sign not in (1, -1):
        raise ValueError(f"sign must be +1 or -1, got {sign}")
    if any(not math.isfinite(score) for score in scores.values()):
        raise ValueError("an unscored name must be dropped before selection, not passed as a non-finite score")
    count = selection_count(fraction, len(scores))
    if count == 0:
        return frozenset()
    ordered = sorted((sign * score for score in scores.values()), reverse=True)
    cut = ordered[count - 1]
    return frozenset(name for name, score in scores.items() if sign * score >= cut)


# ---------------------------------------------------------------------------
# One position
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PositionPath:
    #: V on sessions e … x; V(e − 1) = 1.
    values: tuple[float, ...]
    entered: bool


def position_path(
    prices: SeriesPrices,
    *,
    e: int,
    x: int,
    entry_point: Point,
    exit_point: Point,
    half_spread: float,
    terminal_fraction: float | None,
    with_dividends: bool,
) -> PositionPath | StatRefused:
    """One position's value factors on e … x (spec "A position's value factor")."""
    if not (math.isfinite(half_spread) and 0.0 < half_spread < 1.0):
        raise ValueError(f"half_spread must be in (0, 1), got {half_spread}")
    if x < e:
        raise ValueError(f"exit {x} before entry {e}")
    terminal = prices.terminal_ordinal
    if (terminal is None) != (terminal_fraction is None):
        raise ValueError("a terminating series needs a terminal fraction, and only a terminating series has one")
    if terminal_fraction is not None and not (math.isfinite(terminal_fraction) and 0.0 <= terminal_fraction <= 1.0):
        raise ValueError(f"terminal fraction must be in [0, 1], got {terminal_fraction}")
    span = x - e + 1
    entry = prices.price(e, entry_point)
    if entry is None:
        # Never entered, never charged: its allocation stays cash in both books alike.
        return PositionPath(values=(1.0,) * span, entered=False)

    value = 1.0
    ref = entry * (1.0 + half_spread)
    last_price = entry
    pending = 0.0
    frozen = False
    values: list[float] = []
    for d in range(e, x + 1):
        if frozen:
            values.append(value)
            continue
        if with_dividends and d > e:
            amount = prices.dividends.get(d, 0.0)
            if not (math.isfinite(amount) and amount >= 0.0):
                return StatRefused("bad_dividend", f"dividend {amount} ex-dated at session {d}")
            pending += amount
        if d == x and (terminal is None or x <= terminal):
            # A missing exit price exits on x at the last available close (a stale mark, no
            # future information), or at the entry fill when no close has printed since.
            fill = prices.price(x, exit_point)
            mark = fill if fill is not None else last_price
            value *= (mark * (1.0 - half_spread) + pending) / ref
            values.append(value)
            break
        close = prices.price(d, "close")
        if close is not None:
            value *= (close + pending) / ref
            ref = close
            last_price = close
            pending = 0.0
        if terminal is not None and d == terminal:
            assert terminal_fraction is not None
            # Credit any dividend still pending (no later mark exists), then haircut the whole value.
            value *= (ref + pending) / ref
            value *= terminal_fraction
            pending = 0.0
            frozen = True
        if not (math.isfinite(value) and value >= 0.0):
            return StatRefused("non_finite", f"position value {value} at session {d}")
        values.append(value)
    if not all(math.isfinite(v) and v >= 0.0 for v in values):
        return StatRefused("non_finite", "a position value is not a finite non-negative factor")
    return PositionPath(values=tuple(values), entered=True)


# ---------------------------------------------------------------------------
# The books
# ---------------------------------------------------------------------------

HalfSpread = Callable[[int, int, Book], float]


@dataclass(frozen=True)
class BookSeries:
    """One cell's books on the fixed grid."""

    sessions: range
    arm: tuple[float, ...]
    control: tuple[float, ...]
    active: tuple[float, ...]
    #: Formations with at least one ENTERED arm position (``sparse_arm``'s input).
    entered_formations: tuple[int, ...]


def _cohort_sums(paths: Sequence[PositionPath], span: int) -> list[float]:
    """ΣV on e − 1 … x (the leading entry is n: every position starts at 1)."""
    sums = [float(len(paths))]
    for offset in range(span):
        sums.append(math.fsum(path.values[offset] for path in paths))
    return sums


def evaluate_books(
    grid: Grid,
    cohorts: Mapping[int, Cohort],
    prices: Mapping[int, SeriesPrices],
    *,
    lag: int,
    h: int,
    entry_point: Point,
    exit_point: Point,
    half_spread: HalfSpread,
    terminal_fractions: Mapping[int, float],
    with_dividends: bool,
) -> BookSeries | StatRefused:
    """Arm and control book returns and a(d) = r_A(d) − r_C(d) on every grid session.

    ``cohorts`` holds only formations whose control cohort is non-empty; any other grid
    formation is an idle slot. ``half_spread(series, e, book)`` is the per-side charge.
    ``terminal_fractions`` is the cell's policy applied to every terminating series.
    """
    if h == 1 and (entry_point, exit_point) != ("open", "close"):
        raise ValueError("h = 1 needs an open -> close hold")
    unknown = set(cohorts) - set(grid.formations)
    if unknown:
        raise ValueError(f"cohorts at formations outside the grid: {sorted(unknown)[:3]}")

    # book → formation → ΣV on e − 1 … x
    sums: dict[Book, dict[int, list[float]]] = {"arm": {}, "control": {}}
    entered: list[int] = []
    for t, cohort in sorted(cohorts.items()):
        e, x = t + lag, t + lag + h - 1
        by_book: dict[Book, list[PositionPath]] = {"arm": [], "control": []}
        #: An arm position at the control's half-spread is the same path; compute it once.
        computed: dict[tuple[int, float], PositionPath] = {}
        arm_entered = False
        for book in BOOKS:
            names = cohort.arm if book == "arm" else cohort.control
            for name in sorted(names):
                series = prices.get(name)
                if series is None:
                    raise ValueError(f"no prices for series {name}")
                charge = half_spread(name, e, book)
                path = computed.get((name, charge))
                if path is None:
                    result = position_path(
                        series,
                        e=e,
                        x=x,
                        entry_point=entry_point,
                        exit_point=exit_point,
                        half_spread=charge,
                        terminal_fraction=terminal_fractions.get(name) if series.terminal_ordinal is not None else None,
                        with_dividends=with_dividends,
                    )
                    if isinstance(result, StatRefused):
                        return result
                    path = computed[(name, charge)] = result
                by_book[book].append(path)
                if book == "arm" and path.entered:
                    arm_entered = True
        for book in BOOKS:
            sums[book][t] = _cohort_sums(by_book[book], h)
        if arm_entered:
            entered.append(t)

    returns: dict[Book, list[float]] = {"arm": [], "control": []}
    for d in grid.sessions:
        for book in BOOKS:
            factors = []
            for k in range(h):
                cohort_sums = sums[book].get(d - lag - k)
                if cohort_sums is None:
                    factors.append(1.0)  # idle slot
                    continue
                before, now = cohort_sums[k], cohort_sums[k + 1]
                factors.append(1.0 if before == 0.0 else now / before)
            gross = math.fsum(factors) / h
            if not math.isfinite(gross):
                return StatRefused("non_finite", f"{book} gross factor {gross} at session {d}")
            if gross <= BOOK_RUIN_FACTOR:
                return StatRefused("book_ruin", f"{book} gross factor {gross} at session {d}")
            returns[book].append(gross - 1.0)
    active = tuple(a - c for a, c in zip(returns["arm"], returns["control"], strict=True))
    return BookSeries(
        sessions=grid.sessions,
        arm=tuple(returns["arm"]),
        control=tuple(returns["control"]),
        active=active,
        entered_formations=tuple(entered),
    )


__all__ = [
    "BOOK_RUIN_FACTOR",
    "BookSeries",
    "Cohort",
    "Grid",
    "PositionPath",
    "SeriesPrices",
    "evaluate_books",
    "formation_grid",
    "position_path",
    "select_arm",
    "selection_count",
]
