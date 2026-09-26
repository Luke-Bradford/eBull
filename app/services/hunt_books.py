"""The hunt evaluator's books, vectorised across names (#3385).

``hunt_evaluator.evaluate_books`` is the oracle: this module computes the same books
from the same inputs, stepping every position of one cohort through its sessions as
numpy arrays instead of one Python loop per position. Pure: no database and no reader.

What is identical and what is not, stated so a reader does not have to infer it:

- **Position values are bitwise identical.** Each step applies the oracle's operations
  in the oracle's order (``value * ((close + pending) / ref)``, the fill prices, the
  terminal credit then haircut), and IEEE float64 arithmetic is the same elementwise.
- **Cohort sums are not.** The oracle sums a cohort with ``math.fsum`` (exactly rounded);
  this module uses ``np.add.reduceat`` (sequential), within ``n · eps`` relative of it. The
  randomized oracle test bounds the books at 1e-12 relative. Slot factors are then
  summed per session with ``math.fsum``, as in the oracle.
- **Refusals are the oracle's.** When any refusal condition is met this module returns
  ``None`` and the caller runs the oracle, which names the refusal in its own order.

Positions of a run of formations (both books) are stepped together, so the Python loop
is O(h) per block of up to ``BLOCK_POSITIONS`` positions; the work is
O(formations × names × h) elementwise, in C.
"""

from __future__ import annotations

import math
from collections.abc import Iterator, Mapping
from dataclasses import dataclass

import numpy as np
import numpy.typing as npt

from app.services.hunt_evaluator import (
    BOOK_RUIN_FACTOR,
    BOOKS,
    Book,
    BookSeries,
    Cohort,
    Grid,
    HalfSpread,
    Point,
    SeriesPrices,
)

Floats = npt.NDArray[np.float64]
Ints = npt.NDArray[np.int64]


@dataclass(frozen=True)
class PackedPrices:
    """Every series on one ragged float64 layout: row r covers ordinals ``first[r]`` onwards.

    A missing price is NaN and an invalid one is kept as stored, so validity is the
    oracle's (finite and > 0). A missing dividend is 0.0; a stored one is kept raw, so a
    bad amount still refuses. ``terminal`` is −1 for a live series.
    """

    rows: Mapping[int, int]
    first: Ints
    offset: Ints
    length: Ints
    opens: Floats
    closes: Floats
    dividends: Floats
    terminal: Ints


def pack_prices(prices: Mapping[int, SeriesPrices]) -> PackedPrices:
    """Lay the oracle's inputs out once per trial; every cell reuses the result."""
    names = sorted(prices)
    firsts: list[int] = []
    lengths: list[int] = []
    for name in names:
        series = prices[name]
        keys = [*series.opens, *series.closes, *series.dividends]
        low, high = (min(keys), max(keys)) if keys else (0, -1)
        firsts.append(low)
        lengths.append(high - low + 1)
    length = np.asarray(lengths, dtype=np.int64)
    offset = np.zeros(len(names), dtype=np.int64)
    if names:
        offset[1:] = np.cumsum(length)[:-1]
    total = int(length.sum())
    opens = np.full(total, np.nan)
    closes = np.full(total, np.nan)
    dividends = np.zeros(total)
    for row, name in enumerate(names):
        series = prices[name]
        base = int(offset[row]) - firsts[row]
        for column, values in ((opens, series.opens), (closes, series.closes), (dividends, series.dividends)):
            for ordinal, value in values.items():
                column[base + ordinal] = value
    return PackedPrices(
        rows={name: row for row, name in enumerate(names)},
        first=np.asarray(firsts, dtype=np.int64),
        offset=offset,
        length=length,
        opens=opens,
        closes=closes,
        dividends=dividends,
        terminal=np.asarray(
            [-1 if prices[name].terminal_ordinal is None else prices[name].terminal_ordinal for name in names],
            dtype=np.int64,
        ),
    )


def _valid(values: Floats) -> npt.NDArray[np.bool_]:
    return np.isfinite(values) & (values > 0.0)


#: Positions stepped together; bounds a block's working set (about 10 float64 arrays each).
BLOCK_POSITIONS = 1 << 20
#: A session gross within this relative distance of ``BOOK_RUIN_FACTOR`` is left to the
#: oracle: the sequential cohort sums differ from ``math.fsum`` by at most ``n · eps``
#: relative, which is below 1e-9 for any cohort under 9M names.
RUIN_MARGIN = 1e-9


@dataclass(frozen=True)
class _Block:
    """Every position of a run of formations, both books, grouped by (formation, book)."""

    rows: Ints
    entry: Ints  # e per position
    names: list[int]
    books: list[Book]
    #: Start of each (formation, book) group in position order, and its (t, book).
    group_starts: Ints
    groups: list[tuple[int, Book]]


def _blocks(cohorts: Mapping[int, Cohort], packed: PackedPrices, lag: int) -> Iterator[_Block]:
    """Yielded one at a time, so only one block's positions are resident."""
    pending: list[tuple[int, Cohort]] = []
    size = 0

    def build() -> _Block:
        rows: list[int] = []
        entry: list[int] = []
        names: list[int] = []
        books: list[Book] = []
        starts: list[int] = []
        groups: list[tuple[int, Book]] = []
        for t, cohort in pending:
            for book in BOOKS:
                members = sorted(cohort.arm if book == "arm" else cohort.control)
                starts.append(len(rows))
                groups.append((t, book))
                rows.extend(packed.rows[name] for name in members)
                entry.extend([t + lag] * len(members))
                names.extend(members)
                books.extend(book for _ in members)
        return _Block(
            rows=np.asarray(rows, dtype=np.int64),
            entry=np.asarray(entry, dtype=np.int64),
            names=names,
            books=books,
            group_starts=np.asarray(starts, dtype=np.int64),
            groups=groups,
        )

    for t, cohort in sorted(cohorts.items()):
        pending.append((t, cohort))
        size += len(cohort.control) + len(cohort.arm)
        if size >= BLOCK_POSITIONS:
            yield build()
            pending, size = [], 0
    if pending:
        yield build()


def session_gross(factors: Floats, h: int) -> float | None:
    """The oracle's slot mean, or ``None`` where the oracle must decide: a non-finite or
    overflowing sum, book ruin, or a value within ``RUIN_MARGIN`` of the ruin threshold."""
    try:
        total = math.fsum(factors.tolist())
    except OverflowError, ValueError:
        return None
    gross = total / h
    if not math.isfinite(gross) or gross <= BOOK_RUIN_FACTOR * (1.0 + RUIN_MARGIN):
        return None
    return gross


def _block_values(
    packed: PackedPrices,
    block: _Block,
    *,
    h: int,
    entry_point: Point,
    exit_point: Point,
    half_spread: HalfSpread,
    terminal_fractions: Mapping[int, float],
    with_dividends: bool,
) -> tuple[Floats, npt.NDArray[np.bool_]] | None:
    """ΣV per (group, offset 0 … h − 1) and each position's entered flag; ``None`` on a refusal."""
    first = packed.first[block.rows]
    end = first + packed.length[block.rows]
    base = packed.offset[block.rows] - first

    def gather(column: Floats, d: Ints, missing: float) -> Floats:
        inside = (d >= first) & (d < end)
        return np.where(inside, column[np.where(inside, base + d, 0)], missing)

    e = block.entry
    entry = gather(packed.opens if entry_point == "open" else packed.closes, e, math.nan)
    entered = _valid(entry)
    count = len(block.names)
    charges = np.full(count, 0.5)
    for position in np.flatnonzero(entered).tolist():
        charge = half_spread(block.names[position], int(e[position]), block.books[position])
        if not (math.isfinite(charge) and 0.0 < charge < 1.0):
            raise ValueError(f"half_spread must be in (0, 1), got {charge}")
        charges[position] = charge
    terminal = packed.terminal[block.rows]
    fractions = np.ones(count)
    for position in np.flatnonzero(entered & (terminal >= 0)).tolist():
        name = block.names[position]
        fraction = terminal_fractions.get(name)
        if fraction is None or not (math.isfinite(fraction) and 0.0 <= fraction <= 1.0):
            raise ValueError(f"terminal fraction for {name} must be in [0, 1], got {fraction}")
        fractions[position] = fraction

    sums = np.empty((len(block.groups), h))
    value = np.ones(count)
    ref = entry * (1.0 + charges)
    if not np.isfinite(ref[entered]).all():
        return None
    last_price = entry.copy()
    pending = np.zeros(count)
    frozen = ~entered
    for k in range(h):
        d = e + k
        active = ~frozen
        if with_dividends and k > 0:
            amount = gather(packed.dividends, d, 0.0)
            if (active & ~(np.isfinite(amount) & (amount >= 0.0))).any():
                return None
            pending = np.where(active, pending + amount, pending)
        if k == h - 1:
            fill = gather(packed.opens if exit_point == "open" else packed.closes, d, math.nan)
            mark = np.where(_valid(fill), fill, last_price)
            value = np.where(active, value * ((mark * (1.0 - charges) + pending) / ref), value)
        else:
            close = gather(packed.closes, d, math.nan)
            marked = active & _valid(close)
            value = np.where(marked, value * ((close + pending) / ref), value)
            ref = np.where(marked, close, ref)
            last_price = np.where(marked, close, last_price)
            pending = np.where(marked, 0.0, pending)
            ending = active & (terminal == d)
            value = np.where(ending, value * ((ref + pending) / ref), value)
            value = np.where(ending, value * fractions, value)
            pending = np.where(ending, 0.0, pending)
            frozen = frozen | ending
        if (active & ~(np.isfinite(value) & (value >= 0.0))).any():
            return None
        sums[:, k] = np.add.reduceat(value, block.group_starts)
    return sums, entered


def evaluate_books_fast(
    grid: Grid,
    cohorts: Mapping[int, Cohort],
    packed: PackedPrices,
    *,
    lag: int,
    h: int,
    entry_point: Point,
    exit_point: Point,
    half_spread: HalfSpread,
    terminal_fractions: Mapping[int, float],
    with_dividends: bool,
) -> BookSeries | None:
    """``hunt_evaluator.evaluate_books`` on packed prices; ``None`` means run the oracle for its refusal."""
    if h == 1 and (entry_point, exit_point) != ("open", "close"):
        raise ValueError("h = 1 needs an open -> close hold")
    unknown = set(cohorts) - set(grid.formations)
    if unknown:
        raise ValueError(f"cohorts at formations outside the grid: {sorted(unknown)[:3]}")
    # arm ⊆ control is Cohort's invariant; both are checked so a bypassed one still fails cleanly.
    missing = {name for cohort in cohorts.values() for name in cohort.control | cohort.arm} - set(packed.rows)
    if missing:
        raise ValueError(f"no prices for series {min(missing)}")

    with np.errstate(all="ignore"):
        first_formation = grid.formations[0]
        # Slot factors ΣV(e − 1 + k + 1) / ΣV(e − 1 + k) per formation and offset; 1 when idle.
        factors: dict[Book, Floats] = {book: np.ones((len(grid.formations), h)) for book in BOOKS}
        entered: list[int] = []
        for block in _blocks(cohorts, packed, lag):
            result = _block_values(
                packed,
                block,
                h=h,
                entry_point=entry_point,
                exit_point=exit_point,
                half_spread=half_spread,
                terminal_fractions=terminal_fractions,
                with_dividends=with_dividends,
            )
            if result is None:
                return None
            sums, position_entered = result
            if not np.isfinite(sums).all():
                return None
            sizes = np.diff(np.append(block.group_starts, len(block.names)))
            arm_entered = np.add.reduceat(position_entered.astype(np.int64), block.group_starts) > 0
            for group, (t, book) in enumerate(block.groups):
                chain = np.empty(h + 1)
                chain[0] = float(sizes[group])
                chain[1:] = sums[group]
                before, now = chain[:-1], chain[1:]
                factors[book][t - first_formation] = np.where(before == 0.0, 1.0, now / before)
                if book == "arm" and arm_entered[group]:
                    entered.append(t)

        returns: dict[Book, list[float]] = {"arm": [], "control": []}
        slots = np.arange(h)
        for j in range(len(grid.sessions)):
            index = j - slots  # formation index of slot k on this session
            inside = (index >= 0) & (index < len(grid.formations))
            for book in BOOKS:
                row = np.where(inside, factors[book][np.where(inside, index, 0), slots], 1.0)
                gross = session_gross(row, h)
                if gross is None:
                    return None
                returns[book].append(gross - 1.0)
    active = tuple(a - c for a, c in zip(returns["arm"], returns["control"], strict=True))
    return BookSeries(
        sessions=grid.sessions,
        arm=tuple(returns["arm"]),
        control=tuple(returns["control"]),
        active=active,
        entered_formations=tuple(entered),
    )


__all__ = ["BLOCK_POSITIONS", "RUIN_MARGIN", "PackedPrices", "evaluate_books_fast", "pack_prices", "session_gross"]
