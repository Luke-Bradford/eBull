"""Phase 5d — the sleeve equity curve, and the sizing rule that produces it.

Spec: ``docs/proposals/ta/2026-08-07-bounded-backtester.md`` §5.4 (exposure,
cash and the return denominator; sizing as a result input), §2.1 (the fill
contract), §3.2 rule 5 (the open-position mark) and §4 (the ``vectorbt``
adoption decision). Parent
``docs/proposals/ta/strategy-catalogue-and-backtest-validity.md`` criteria 2 and
7. Refs #2240.

⚠⚠ ``vectorbt`` WAS MEASURED AND REJECTED. THE MEASUREMENTS ARE HERE SO THE
DECISION CAN BE RE-OPENED WITH EVIDENCE RATHER THAN RE-ARGUED.

§4 defers the adoption decision to this stage — *"the adoption decision is taken
against a working trade list rather than up front"* — and names portfolio-level
path-dependent drawdown plus Sharpe / Sortino / exposure / turnover as what the
library is worth buying. Re-tested against ``vectorbt==1.1.0`` on Python 3.14.4,
2026-08-07:

1. **The metrics §4 wanted REFUSE on our index.** ``sharpe_ratio()``,
   ``sortino_ratio()``, ``annualized_volatility()`` and ``annualized_return()``
   all raise ``ValueError: Index frequency is None`` on a ``DatetimeIndex`` of
   real trading dates, which is what a 16,236-date calendar with holidays and
   halts is. The only way through is declaring a fixed ``freq``, and
   ``freq="1D"`` imposes an annualisation factor of **exactly 365.0** —
   measured, by dividing the library's Sharpe by the per-period one — against an
   index carrying ~196 observations per calendar year. That inflates Sharpe by
   ``sqrt(365/196) = 1.37x``. ⚠ It is the KILLER, and not a packaging
   objection: the one thing the library was to be adopted FOR is the thing that
   does not work on this data.
2. **It wants a dense date x instrument panel; ours is 27.3% dense.** Measured
   against the dev corpus this run: 5,266 series, 23,339,583 bars, 16,236
   distinct trading dates — 85,498,776 dense cells, so 72.7% is NaN padding.
   ``Portfolio.from_signals`` at that exact shape completes in 5.9 s but peaks
   at **4.05 GiB RSS**.
3. **Its default fill semantics import the look-ahead §3.5 rule 1 forbids.**
   Reproduced on the current install: ``from_signals(close, entries, exits)``
   fills a bar-1 signal at that bar's OWN close (101.0). Spec §2 measured the
   same thing; it still holds.
4. **58 packages**, including ``numba`` 0.66.0 and ``llvmlite``,
   ``scikit-learn``, ``scipy``, ``matplotlib``, ``plotly``, ``ipywidgets``,
   ``requests`` and ``tqdm``, against a repo that runs ``pip-audit --strict`` in
   CI. ⚠ Spec §4 states *"It pulls no numba"* — that is FALSIFIED on this
   resolution, so the packaging residual is larger than the spec recorded, not
   smaller.

So the engine below is hand-rolled, which parent §8's *"Do not hand-roll"* is
against. The counter-argument is not "we prefer our own": it is that the library
cannot compute four of criterion 7's twelve metrics on an irregular trading-date
index without a declared annualisation this module derives from the axis itself.

⚠⚠ WHAT THIS MODULE DOES NOT DO, AND WHY THAT IS THE WHOLE DESIGN.

It never reads a price series, never resolves a fill and never sees a
``signal_bar_date``. §2.1: *"The simulator is handed events indexed on the FILL
bar, priced at the stored ``fill_price``."* A ``Leg`` carries an entry index, an
exit index and two already-costed prices; there is no field in which a
recomputed fill could be expressed. The caller resolves them from the ledger.
"""

from __future__ import annotations

import hashlib
from array import array
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Final

import numpy as np
import numpy.typing as npt

#: How a leg's notional is decided. ⚠ FROZEN AND HASHED — it is
#: ``strategy_result.SIZING_RULE``'s payload, and §5.4 is explicit that naming
#: it *"is what stops a later sizing change reading as a performance
#: improvement"*. Changing any rule below is a new sizing rule id.
#:
#: ⚠⚠ §5.4 declares *"equal weight across concurrent positions, rebalanced only
#: on position open/close"* and stops there. Three sub-decisions it does not
#: take are taken HERE, by construction, because no published rule fixes them
#: and each changes every number downstream:
#:
#:   1. **WHEN the equal weight is re-imposed.** Only on an EVENT DATE — a date
#:      on which at least one leg opens or closes — which is §5.4's clause read
#:      literally. Between event dates the weights DRIFT; they are not restored
#:      daily. ⚠ The rejected reading is "rebalance every bar", which is a
#:      different (and busier) strategy, and would charge turnover the declared
#:      rule never incurs.
#:   2. **AT WHAT PRICE the rebalance trades.** At the event date's CLOSE.
#:      Entries and exits — the only ledger-derived orders — transact at their
#:      stored fill prices at the OPEN, which is what keeps §2.1's equality
#:      exact. A rebalance trade is produced by the SIZING RULE and not by a
#:      ledger row, so it has no stored fill price to equal, and the close is
#:      the only price on the bar this module is given.
#:   3. **SELLS BEFORE BUYS, and buys capped by cash.** A single-pass rebalance
#:      to ``equity / n`` leaves cash at MINUS the costs it just charged, which
#:      is leverage — arithmetically small and forbidden outright by the project
#:      posture. Selling first and then spending only what is on hand makes
#:      ``cash >= 0`` hold by construction rather than by tolerance, and the
#:      under-investment that leaves is exactly the cost charged.
SIZING_RULE_ID: Final = "equal_weight_concurrent_v1"


def _code_hash() -> str:
    return hashlib.sha256(Path(__file__).read_bytes()).hexdigest()[:12]


# MT-1 depends on both curve constructors in this module. This source-derived
# version prevents either engine changing while its four-arm book identity is
# silently reused; it is intentionally broader than the public sizing-rule ID.
EQUITY_CURVE_ENGINE_VERSION: Final = f"equity-curve-engine-v1+{_code_hash()}"

#: Research arm for #2430. A new position receives the same causal entry-time
#: target as v1 (equity divided by the post-entry concurrent count), but every
#: holding is then left to drift until its ledger exit. It is deliberately not
#: the production ``SIZING_RULE_ID`` and cannot inherit production evidence.
ENTRY_WEIGHT_DRIFT_RULE_ID: Final = "entry_weight_drift_v1"

#: #2430's implementable middle arm: positions still enter and exit at their
#: ledger fills, but the synthetic equalisation trade is allowed only at a
#: panel-calendar month end. It separates ordinary portfolio maintenance from
#: v1's accidental coupling of turnover to how often any name opens or closes.
MONTH_END_REBALANCE_RULE_ID: Final = "calendar_month_end_equal_weight_v1"

#: MT-1's holdings-level overlay engine. The exposure decision is known before
#: the declared monthly bar, but the synthetic portfolio trade is applied only
#: AFTER that bar's mark; consequently the new target affects the following
#: close-to-close return and cannot consume the decision bar's outcome. Source
#: entries/exits retain their stored open fills. Overlay and event rebalances
#: share one closing step 4 over the post-fill holdings; source fills remain
#: distinct and every synthetic trade pays the existing holding-specific
#: half-spread.
CAPPED_TARGET_EXPOSURE_RULE_ID: Final = "capped_target_exposure_after_decision_close_v1"

#: How criterion 7's buy-and-hold BENCHMARK is composed. ⚠ FROZEN AND HASHED —
#: it is ``ResultIdentity.benchmark_rule``, and it exists as a separate id for
#: the reason ``SIZING_RULE_ID`` exists: a comparator that can change without the
#: result identity moving is a comparator that can be tuned invisibly. Until
#: #2426 the benchmark silently inherited ``SIZING_RULE_ID``.
#:
#: ⚠⚠ THE DEFINING PROPERTY IS THAT IT NEVER REBALANCES, and that is a SOURCE
#: RULE, not a preference. Blume & Stambaugh, *"Biases in computed returns: An
#: application to the size effect"*, Journal of Financial Economics 12 (1983),
#: 387-404: rebalancing trades into the bid-ask noise in each closing print, and
#: *"returns computed for buy-and-hold portfolios largely avoid the bias induced
#: by closing prices"*. They measure the bias at 0.056%/day on the small-firm
#: decile against 0.001% on the large-firm decile — fifty times as great — which
#: is why it matters here specifically: our panel is predominantly small and
#: delisted US names. Measured on our own full population (#2426), running this
#: benchmark under ``equal_weight_concurrent_v1`` instead added **23.2 points of
#: annual return** and turned 137,477,862x the starting pot over on a portfolio
#: that is supposed never to trade.
#:
#: What the panel's shape forces, fixed BY CONSTRUCTION because no published rule
#: covers an unbalanced panel where names list and delist inside the window
#: (CRSP's equal-weighted index redistributes to survivors, which is a rebalance;
#: its delisting-return rule needs a field our corpus does not carry):
#:
#:   1. **Each leg is committed exactly ``starting_equity / n``** at its own
#:      entry, and held to its own exit.
#:   2. **Proceeds go to cash and stay there; cash earns 0.** §5.4's own rule for
#:      the strategy — *"define cash return as zero, report return on the full
#:      allocated pot"* — applied to the benchmark, which is what keeps the two
#:      denominators comparable.
#:   3. **No rebalance, ever**, so no rebalance trade and no rebalance cost. The
#:      entry/exit round trip is charged exactly as the strategy's is, because
#:      those prices arrive already net from ``_benchmark_book``.
BENCHMARK_RULE_ID: Final = "equal_weight_buy_and_hold_v1"

#: ⚠ A leg opening on a date whose cash cannot fund ``equity / n`` is admitted
#: at whatever cash allows and TOPPED UP by that date's close rebalance. It is
#: never refused, and the count of short-funded entries is reported.
#:
#: The alternative — refuse the entry — was rejected: it would make the trade
#: population depend on the sizing rule, so two sizing rules could not be
#: compared on the same trades, and criterion 9's census would have to carry a
#: narrowing whose size depends on the order positions happened to open in.
_MIN_ALLOCATION: Final = 0.0


@dataclass
class LegBook:
    """The trade list, columnar, because the row form does not fit in memory.

    ⚠ COLUMNAR IS NOT PREMATURE OPTIMISATION HERE. S-1 produced 3,135,355
    positions over the validated universe (measured at stage 5b), and one frozen
    dataclass per position with a tuple of marks is several gigabytes for a
    structure that is read once, in index order. The flat ``marks`` array with
    per-leg offsets is the same data at ~186 MB.

    ⚠ ``marks`` HOLDS ONE CLOSE PER BAR OF THE LEG'S LIFE, INCLUSIVE AT BOTH
    ENDS, and ``nan`` for a date on which the instrument's own series has no
    bar. Spec §3.3: a halted name *"stays open to the next date on which its own
    series has a bar"*, so the gap is real and must not be interpolated —
    ``build_equity_curve`` carries the last known mark forward and counts it.

    ⚠ ``marks[0]`` IS used and ``marks[-1]`` is NOT, which is not an
    inconsistency: the leg fills at the ENTRY bar's open and is then marked at
    that same bar's close, so ``marks[0]`` is a live valuation; it exits at the
    EXIT bar's open and is gone before that bar's close is read. The last slot
    is still required, because ``offset = mark_offset + (day - entry_index)``
    is what makes the lookup O(1), and a ragged array would put a bounds test in
    the innermost loop.
    """

    entry_index: list[int] = field(default_factory=list)
    exit_index: list[int] = field(default_factory=list)
    entry_price: list[float] = field(default_factory=list)
    exit_price: list[float] = field(default_factory=list)
    half_spread: list[float] = field(default_factory=list)
    #: ``False`` for a position open at the window end, priced at its mark
    #: (§3.2 rule 5). ⚠ Kept because §3.4 keeps such a position OUT of the win
    #: rate and expectancy while keeping it IN exposure and on the equity curve,
    #: and only this flag can tell the two apart downstream.
    realised: list[bool] = field(default_factory=list)
    mark_offset: list[int] = field(default_factory=list)
    marks: array[float] = field(default_factory=lambda: array("d"))

    def add(
        self,
        *,
        entry_index: int,
        exit_index: int,
        entry_price: float,
        exit_price: float,
        half_spread: float,
        realised: bool,
        marks: list[float],
    ) -> None:
        """Append one leg. Refuses a shape the simulator could not price.

        ⚠ THIS RAISES. It is a writer-side shape, like ``StrategyResult`` and
        unlike ``check_promotable``: a caller assembling a leg whose marks do
        not span its life has a bug, and a silent truncation would shorten a
        hold rather than fail.
        """
        span = exit_index - entry_index + 1
        if exit_index < entry_index:
            raise ValueError(f"leg closes at index {exit_index} before it opens at {entry_index}")
        if len(marks) != span:
            raise ValueError(
                f"leg spanning indices {entry_index}..{exit_index} needs {span} marks, got {len(marks)} — a short "
                "mark array would silently shorten the hold"
            )
        if entry_price <= 0.0 or exit_price <= 0.0:
            raise ValueError(f"leg prices must be positive, got entry {entry_price} exit {exit_price}")
        if half_spread < 0.0:
            raise ValueError(f"half_spread must be non-negative, got {half_spread}")
        self.entry_index.append(entry_index)
        self.exit_index.append(exit_index)
        self.entry_price.append(entry_price)
        self.exit_price.append(exit_price)
        self.half_spread.append(half_spread)
        self.realised.append(realised)
        self.mark_offset.append(len(self.marks))
        self.marks.extend(marks)

    def __len__(self) -> int:
        return len(self.entry_index)

    def rebased(self, offset: int) -> LegBook:
        """The same legs, re-based onto an axis starting ``offset`` bars later.

        ⚠ Every caller of ``build_equity_curve`` measures on a TRUNCATED axis —
        §5's rule that an equity axis is the evaluation axis cut to the closed
        span of its own positions — so re-basing is part of the curve contract
        and not one caller's private step. It lives here because two now need
        it: ``backtest_run`` for a namespace book, ``synthetic_control_run`` for
        each cohort member's own span.

        ⚠ The price, spread, ``realised`` and ``marks`` arrays are SHARED rather
        than copied. They are read-only from here on and the marks array is the
        large one (hundreds of MB on a full-corpus arm); copying it to change
        two integer columns would double the peak for nothing.
        """
        return LegBook(
            entry_index=[index - offset for index in self.entry_index],
            exit_index=[index - offset for index in self.exit_index],
            entry_price=self.entry_price,
            exit_price=self.exit_price,
            half_spread=self.half_spread,
            realised=self.realised,
            mark_offset=self.mark_offset,
            marks=self.marks,
        )


@dataclass(frozen=True)
class SharedMarkLegBook:
    """Columnar legs whose marks reference immutable per-series arrays.

    Synthetic-control members redraw millions of entries over the same price
    series.  Flattening each selected leg's mark span into ``LegBook.marks``
    duplicates the corpus once per member and was the dominant memory cost of
    full controls.  This representation stores only one source id per leg; the
    equity walker below remains the single sizing/cost implementation.
    """

    entry_index: npt.NDArray[np.int64]
    exit_index: npt.NDArray[np.int64]
    entry_price: npt.NDArray[np.float64]
    exit_price: npt.NDArray[np.float64]
    half_spread: npt.NDArray[np.float64]
    realised: npt.NDArray[np.bool_]
    mark_source: npt.NDArray[np.int32]
    marks_by_source: tuple[npt.NDArray[np.float64], ...]
    marks_first_by_source: npt.NDArray[np.int64]

    def __post_init__(self) -> None:
        for name in (
            "entry_index",
            "exit_index",
            "entry_price",
            "exit_price",
            "half_spread",
            "realised",
            "mark_source",
            "marks_first_by_source",
        ):
            value = getattr(self, name)
            if value.ndim != 1:
                raise ValueError(f"{name} must be one-dimensional, got shape {value.shape}")
        for source, marks in enumerate(self.marks_by_source):
            if marks.ndim != 1:
                raise ValueError(f"mark source {source} must be one-dimensional, got shape {marks.shape}")
        size = int(self.entry_index.size)
        for name in ("exit_index", "entry_price", "exit_price", "half_spread", "realised", "mark_source"):
            actual = int(getattr(self, name).size)
            if actual != size:
                raise ValueError(f"{name} has {actual} entries against {size} legs")
        if int(self.marks_first_by_source.size) != len(self.marks_by_source):
            raise ValueError(
                f"{self.marks_first_by_source.size} mark starts against {len(self.marks_by_source)} mark sources"
            )
        if size and (int(self.mark_source.min()) < 0 or int(self.mark_source.max()) >= len(self.marks_by_source)):
            raise ValueError("a shared-mark leg references a missing mark source")
        if size and bool(np.any(self.exit_index < self.entry_index)):
            leg = int(np.flatnonzero(self.exit_index < self.entry_index)[0])
            raise ValueError(
                f"shared-mark leg {leg} closes at index {int(self.exit_index[leg])} before it opens at "
                f"{int(self.entry_index[leg])}"
            )
        if size:
            # Fail at construction rather than allowing NumPy's negative-index
            # wrap or an IndexError part-way through a multi-million-leg walk.
            # The flat-mark ``LegBook.add`` validates the equivalent span on
            # every leg; changing storage layout must not weaken that boundary.
            source_starts = self.marks_first_by_source[self.mark_source]
            source_lengths = np.fromiter(
                (marks.size for marks in self.marks_by_source),
                dtype=np.int64,
                count=len(self.marks_by_source),
            )
            source_ends = source_starts + source_lengths[self.mark_source]
            invalid_span = (self.entry_index < source_starts) | (self.exit_index >= source_ends)
            if bool(np.any(invalid_span)):
                leg = int(np.flatnonzero(invalid_span)[0])
                source = int(self.mark_source[leg])
                raise ValueError(
                    f"shared-mark leg {leg} spans indices {int(self.entry_index[leg])}.."
                    f"{int(self.exit_index[leg])} outside source {source}'s mark range "
                    f"{int(source_starts[leg])}..{int(source_ends[leg]) - 1}"
                )

    def __len__(self) -> int:
        return int(self.entry_index.size)


@dataclass(frozen=True)
class EquityCurve:
    """The daily path, and every narrowing that produced it.

    ⚠ THE COUNTERS ARE CRITERION 9'S REPORT, not diagnostics — the same posture
    as ``PositionSet``. *"A narrowing that is not counted is a narrowing
    asserted safe."*
    """

    #: One entry per date on the axis. Starts at ``starting_equity``.
    equity: npt.NDArray[np.float64]
    #: Notional at work at each date's close. ⚠ NOT ``equity - cash`` computed
    #: by the reader: cash is what is left after costs, so the two differ by the
    #: charge, and exposure must be measured on what was actually invested.
    invested: npt.NDArray[np.float64]
    #: Legs open at each date's close. ⚠ §5.4: exposure is *"invested
    #: capital-days / allocated capital-days"* and *"is NOT sum(bars_held);
    #: sql/256 says bars_held is a bar count and NOT exposure time, and the
    #: difference is concurrency"*. This array is the concurrency.
    open_count: npt.NDArray[np.int32]
    #: Notional changing hands at each date, entries + exits + rebalance trades.
    traded_notional: npt.NDArray[np.float64]
    #: Total half-spread charged on REBALANCE trades only. ⚠ Entry and exit
    #: costs are already inside ``entry_price`` / ``exit_price`` (they are the
    #: NET prices ``position_costing`` produced), so adding them here would
    #: double-count.
    rebalance_costs: float
    #: Dates on which at least one leg opened or closed.
    event_dates: int
    #: Entries whose cash at the open could not fund ``equity / n``, topped up
    #: by that date's close rebalance.
    short_funded_entries: int
    #: Dates on which a leg's own series had no bar and the previous mark was
    #: carried forward (§3.3's halt).
    stale_marks: int
    #: Legs still open at the end of the axis, held at a frozen mark because
    #: their instrument had no further usable bar (§3.2 rule 5). ⚠ Reported
    #: because they are IN exposure and IN the final equity while being out of
    #: the win rate and out of expectancy (§3.4) — a reader comparing the two
    #: populations needs the size of the difference.
    unrealised_held: int

    def __post_init__(self) -> None:
        n = len(self.equity)
        for name in ("invested", "open_count", "traded_notional"):
            other = getattr(self, name)
            if len(other) != n:
                raise ValueError(f"{name} has {len(other)} entries against {n} dates")


def _build_strategy_curve(
    book: LegBook | SharedMarkLegBook,
    *,
    date_count: int,
    starting_equity: float = 1.0,
    rebalance_events: bool,
    scheduled_rebalance_indices: frozenset[int] = frozenset(),
    scheduled_exposure_by_index: Mapping[int, float] | None = None,
) -> EquityCurve:
    """Shared strategy walk; ``rebalance_events`` is the #2430 A/B switch.

    ⚠⚠ ORDER WITHIN A DATE IS FIXED AND IS §3.2 RULE 4: **exits, then entries,
    then the mark, then the rebalance.** *"An exit row whose fill bar equals a
    new entry's fill bar closes the OLDER position; it never closes the position
    opened that bar."* Reversing the first two would fund an entry out of cash
    that a same-day exit had not released yet, which shows up as a spurious
    short-funded entry rather than as an error.

    ⚠ CASH EARNS 0 (§5.4), and the denominator is the FULL allocated pot, never
    capital at work: *"a strategy invested 10% of the time can post a
    spectacular return on almost no capital at work"*.

    Pure. Reads no database, mutates no argument.
    """
    if date_count < 1:
        raise ValueError(f"date_count must be >= 1, got {date_count}")
    if starting_equity <= 0.0:
        raise ValueError(f"starting_equity must be positive, got {starting_equity}")

    n_legs = len(book)
    entry_index = np.asarray(book.entry_index, dtype=np.int64)
    exit_index = np.asarray(book.exit_index, dtype=np.int64)
    entry_price = np.asarray(book.entry_price, dtype=np.float64)
    exit_price = np.asarray(book.exit_price, dtype=np.float64)
    half_spread = np.asarray(book.half_spread, dtype=np.float64)
    realised = list(book.realised)
    # The walk below is scalar and path-dependent. Native Python scalars avoid
    # constructing millions of NumPy scalar wrappers while preserving the exact
    # operation order; the arrays above remain the vectorised shape validator.
    entry_indices: list[int] = entry_index.tolist()
    exit_indices: list[int] = exit_index.tolist()
    entry_prices: list[float] = entry_price.tolist()
    exit_prices: list[float] = exit_price.tolist()
    half_spreads: list[float] = half_spread.tolist()
    all_realised = all(realised)
    if isinstance(book, SharedMarkLegBook):
        mark_source = book.mark_source
        marks_by_source = book.marks_by_source
        marks_first_by_source = book.marks_first_by_source
        mark_sources: list[int] = mark_source.tolist()
        marks_first_by_leg: list[int] = marks_first_by_source[mark_source].tolist()
        mark_offset = np.empty(0, dtype=np.int64)
        marks = np.empty(0, dtype=np.float64)
        mark_offsets: list[int] = []
    else:
        mark_source = np.empty(0, dtype=np.int32)
        marks_by_source = ()
        marks_first_by_source = np.empty(0, dtype=np.int64)
        mark_sources = []
        marks_first_by_leg = []
        mark_offset = np.asarray(book.mark_offset, dtype=np.int64)
        marks = np.frombuffer(book.marks, dtype=np.float64) if len(book.marks) else np.empty(0, dtype=np.float64)
        mark_offsets = mark_offset.tolist()

    if n_legs and int(exit_index.max()) >= date_count:
        raise ValueError(
            f"a leg closes at index {int(exit_index.max())} on a {date_count}-date axis — the axis is short, and "
            "silently truncating it would drop the tail of the curve"
        )
    if n_legs and int(entry_index.min()) < 0:
        raise ValueError(f"a leg opens at index {int(entry_index.min())}; indices are positions on the date axis")

    # Legs bucketed by the date they open and the date they close. ⚠ Built from
    # the arrays rather than assumed sorted: `build_positions` emits per
    # instrument, so the concatenated book is in instrument order, not date
    # order, and a loop that assumed otherwise would open legs late.
    opening: list[list[int]] = [[] for _ in range(date_count)]
    closing: list[list[int]] = [[] for _ in range(date_count)]
    for leg in range(n_legs):
        opening[entry_indices[leg]].append(leg)
        closing[exit_indices[leg]].append(leg)

    equity_path = np.zeros(date_count, dtype=np.float64)
    invested_path = np.zeros(date_count, dtype=np.float64)
    open_path = np.zeros(date_count, dtype=np.int32)
    traded_path = np.zeros(date_count, dtype=np.float64)

    units = [0.0] * n_legs
    last_price = [0.0] * n_legs

    cash = starting_equity
    open_legs: list[int] = []
    #: Legs past their last tradeable bar — open, marked, and unsellable.
    frozen: set[int] = set()
    rebalance_costs = 0.0
    event_dates = 0
    short_funded = 0
    stale_marks = 0
    target_exposure = 1.0 if scheduled_exposure_by_index is None else 0.0

    for day in range(date_count):
        opened_today = opening[day]
        # ⚠⚠ A LEG THAT OPENS AND CLOSES ON THE SAME DATE IS SPLIT OUT, and
        # §3.2 rule 4 is the reason it must be. "Exit before entry" is about
        # DIFFERENT positions — *"an exit row whose fill bar equals a new
        # entry's fill bar closes the OLDER position"* — and applying it to a
        # leg's OWN exit would close it before it opened, leaving it open
        # forever. ``sql/256`` records ``bars_held = 0`` as legal (a tp/sl can
        # be touched on the fill bar itself), so this is a real population, not
        # a defensive branch.
        #
        # ⚠⚠ AN UNREALISED LEG IS NOT AN EXIT, AND TREATING IT AS ONE WAS A REAL
        # DEFECT (caught at Codex checkpoint 2). A position open at the window
        # end has no sale — §3.2 rule 5 gives it *"an unrealised mark taken at
        # the last usable close of the evaluation window for that instrument"*
        # and §3.4 keeps it IN exposure and ON the equity curve. Liquidating it
        # at its mark bar would drop it from ``open_count`` and ``invested``
        # from that date on, and worse, would hand its notional to cash where it
        # could fund a same-day entry — buying with money nobody received.
        # So it FREEZES instead: it stays open at its net mark for the rest of
        # the axis, and it is excluded from the rebalance because there is no
        # bar on which it could be traded.
        closing_now = closing[day]
        if all_realised:
            closed_today = [leg for leg in closing_now if entry_indices[leg] < day]
            same_bar = [leg for leg in closing_now if entry_indices[leg] == day]
            freezing_today: list[int] = []
        else:
            closed_today = [leg for leg in closing_now if realised[leg] and entry_indices[leg] < day]
            same_bar = [leg for leg in closing_now if realised[leg] and entry_indices[leg] == day]
            freezing_today = [leg for leg in closing_now if not realised[leg]]
        event = bool(closed_today or opened_today)

        # 1. EXITS of positions opened EARLIER, at the stored net fill price.
        for leg in closed_today:
            proceeds = units[leg] * exit_prices[leg]
            cash += proceeds
            traded_path[day] += proceeds
            units[leg] = 0.0
        if closed_today:
            done = set(closed_today)
            open_legs = [leg for leg in open_legs if leg not in done]

        # 2. ENTRIES, at the stored net fill price. ⚠ The valuation reference is
        #    the PREVIOUS close (`last_price`), which is the newest price this
        #    module has at the open of `day`. Using today's close here would be
        #    look-ahead on the sizing decision.
        #    ⚠ ``equity_ref`` is computed ONCE and reused across every entry on
        #    the date, which is not a shortcut: an entry moves value from cash
        #    into a holding and leaves total equity unchanged, so recomputing it
        #    per entry returns the same number for O(open) work each time. On
        #    S-1's 3.1 M legs the naive form is quadratic in the open set.
        #    ⚠ THE DENOMINATOR COUNTS TODAY'S WHOLE BASKET, not one entry at a
        #    time. Both entries on a date are decided at the same instant on the
        #    same bar, so an allocator sizing today's basket knows how many
        #    names it is opening; sizing them sequentially would give the first
        #    100% of a flat pot and report every sibling as short-funded, which
        #    is an artefact of the loop rather than a capital constraint.
        equity_ref = cash
        if opened_today:
            for leg in open_legs:
                equity_ref += units[leg] * last_price[leg]
        basket = len(open_legs) + len(opened_today)
        for leg in opened_today:
            target = target_exposure * equity_ref / basket
            allocation = min(target, cash)
            if allocation < target:
                short_funded += 1
            if allocation > _MIN_ALLOCATION:
                units[leg] = allocation / entry_prices[leg]
                cash -= allocation
                traded_path[day] += allocation
            last_price[leg] = entry_prices[leg]
            open_legs.append(leg)

        # 2b. SAME-BAR EXITS, after their own entry. ⚠ These legs never reach
        #     the close: they hold for the bar and are gone, so they take no
        #     part in the mark or the rebalance below.
        if same_bar:
            for leg in same_bar:
                proceeds = units[leg] * exit_prices[leg]
                cash += proceeds
                traded_path[day] += proceeds
                units[leg] = 0.0
            done = set(same_bar)
            open_legs = [leg for leg in open_legs if leg not in done]

        # 2c. FREEZE the unrealised legs whose mark bar is today. ⚠ AFTER the
        #     entries, so a position whose fill bar IS its instrument's last
        #     usable bar opens and freezes on the same date rather than being
        #     frozen before it exists.
        for leg in freezing_today:
            last_price[leg] = exit_prices[leg]
            frozen.add(leg)

        if scheduled_exposure_by_index is not None and frozen:
            raise ValueError(
                "a capped target-exposure curve cannot carry an untradeable frozen leg — "
                "uniform portfolio scaling would be unreachable"
            )

        # 3. MARK TO THE CLOSE. A leg whose series has no bar today keeps its
        #    previous mark (§3.3's halt) and the carry-forward is counted.
        #    ⚠ A FROZEN leg is skipped: its series has no further bar at all, so
        #    counting each remaining date as a "stale mark" would report the
        #    window's tail as thousands of halts.
        if all_realised:
            for leg in open_legs:
                if marks_by_source:
                    source = mark_sources[leg]
                    offset = day - marks_first_by_leg[leg]
                    mark = marks_by_source[source][offset]
                else:
                    offset = mark_offsets[leg] + (day - entry_indices[leg])
                    mark = marks[offset]
                if np.isnan(mark):
                    stale_marks += 1
                else:
                    last_price[leg] = mark
        else:
            for leg in open_legs:
                if leg in frozen:
                    continue
                if marks_by_source:
                    source = mark_sources[leg]
                    offset = day - marks_first_by_leg[leg]
                    mark = marks_by_source[source][offset]
                else:
                    offset = mark_offsets[leg] + (day - entry_indices[leg])
                    mark = marks[offset]
                if np.isnan(mark):
                    stale_marks += 1
                else:
                    last_price[leg] = mark

        # 4. REBALANCE, sells first then buys, capped by cash so it can never
        #    go negative (see SIZING_RULE_ID note 3).
        #    ⚠ ``event_dates`` counts the DATE, not the rebalance: a date whose
        #    last position closed is an event with nothing left to rebalance,
        #    and counting only rebalances would under-report the concurrency
        #    changes criterion 8 asks for.
        if event:
            event_dates += 1
        #    ⚠⚠ FROZEN LEGS ARE EXCLUDED FROM THE TARGET AND FROM THE TRADING.
        #    Equal weight across the concurrent set is unreachable when one
        #    member cannot be sold, so the TRADEABLE sleeve is equalised among
        #    itself over the capital that is actually available
        #    (``cash + tradeable_held``), and a frozen leg keeps its mark. The
        #    alternative — dividing total equity by the total count — would set
        #    a target the frozen leg can never move to and force every tradeable
        #    leg to absorb the shortfall, which is a different sizing rule.
        tradeable = open_legs if all_realised else [leg for leg in open_legs if leg not in frozen]
        exposure_changes = False
        if scheduled_exposure_by_index is not None and day in scheduled_exposure_by_index:
            exposure_changes = True
            target_exposure = scheduled_exposure_by_index[day]
        rebalance_now = (rebalance_events and event) or day in scheduled_rebalance_indices or exposure_changes
        if rebalance_now and tradeable:
            held = 0.0
            for leg in tradeable:
                held += units[leg] * last_price[leg]
            target = target_exposure * (cash + held) / len(tradeable)
            buyers: list[int] = []
            for leg in tradeable:
                value = units[leg] * last_price[leg]
                if value > target:
                    sold = value - target
                    charge = sold * half_spreads[leg]
                    cash += sold - charge
                    units[leg] = target / last_price[leg]
                    traded_path[day] += sold
                    rebalance_costs += charge
                elif value < target:
                    buyers.append(leg)
            for leg in buyers:
                value = units[leg] * last_price[leg]
                wanted = target - value
                spend = min(wanted, cash / (1.0 + half_spreads[leg]))
                if spend <= 0.0:
                    continue
                charge = spend * half_spreads[leg]
                cash -= spend + charge
                units[leg] += spend / last_price[leg]
                traded_path[day] += spend
                rebalance_costs += charge

        held = 0.0
        for leg in open_legs:
            held += units[leg] * last_price[leg]
        invested_path[day] = held
        equity_path[day] = cash + held
        open_path[day] = len(open_legs)

    return EquityCurve(
        equity=equity_path,
        invested=invested_path,
        open_count=open_path,
        traded_notional=traded_path,
        rebalance_costs=rebalance_costs,
        event_dates=event_dates,
        short_funded_entries=short_funded,
        stale_marks=stale_marks,
        unrealised_held=len(frozen),
    )


def build_equity_curve(
    book: LegBook | SharedMarkLegBook,
    *,
    date_count: int,
    starting_equity: float = 1.0,
) -> EquityCurve:
    """Walk the date axis once, applying production ``SIZING_RULE_ID``."""
    return _build_strategy_curve(
        book,
        date_count=date_count,
        starting_equity=starting_equity,
        rebalance_events=True,
    )


def build_entry_weight_drift_curve(
    book: LegBook,
    *,
    date_count: int,
    starting_equity: float = 1.0,
) -> EquityCurve:
    """#2430 arm: equal entry-time targets, then no synthetic rebalance trades.

    Entries and exits keep their exact stored net fills and ordering. New
    positions use the same causal target and cash cap as production v1. The
    only removed operation is step 4's event-date equalisation, so the result
    isolates how much return, drawdown and turnover that rule contributes.
    """
    return _build_strategy_curve(
        book,
        date_count=date_count,
        starting_equity=starting_equity,
        rebalance_events=False,
    )


def build_month_end_rebalanced_curve(
    book: LegBook,
    *,
    dates: Sequence[date],
    starting_equity: float = 1.0,
) -> EquityCurve:
    """#2430 arm: restore equal weight only at each panel-calendar month end."""
    if any(later <= earlier for earlier, later in zip(dates, dates[1:], strict=False)):
        raise ValueError("dates must be strictly increasing for a causal month-end calendar")
    # A boundary is observable only when the next panel date belongs to a new
    # month.  The final date may merely be a truncated evaluation boundary
    # (for example 8 July), so treating it as a month-end would add a synthetic
    # trade with no forward holding period and contaminate the comparison.
    month_ends = frozenset(
        index
        for index, when in enumerate(dates[:-1])
        if (dates[index + 1].year, dates[index + 1].month) != (when.year, when.month)
    )
    return _build_strategy_curve(
        book,
        date_count=len(dates),
        starting_equity=starting_equity,
        rebalance_events=False,
        scheduled_rebalance_indices=month_ends,
    )


def build_capped_target_exposure_curve(
    book: LegBook,
    *,
    dates: Sequence[date],
    target_exposure_by_date: Mapping[date, float],
    starting_equity: float = 1.0,
) -> EquityCurve:
    """Run one source book under a causal, unlevered exposure schedule.

    Before the first supplied decision the sleeve remains in cash. On a
    decision date the existing source holdings are marked first, then rebalanced
    uniformly to the new aggregate target; that target therefore applies to the
    *next* close-to-close return. Source entry/exit events between decisions
    rebalance the book under the last target without changing it. All synthetic
    trades use the same per-leg half-spread and sell-before-buy cash cap as the
    production curve.

    The caller must derive the dates and exposures from the frozen causal
    history. This function validates shape and mechanics only; it deliberately
    cannot invent a missing decision or exposure.
    """
    if any(later <= earlier for earlier, later in zip(dates, dates[1:], strict=False)):
        raise ValueError("dates must be strictly increasing for a causal exposure schedule")
    if not target_exposure_by_date:
        raise ValueError("target exposure schedule is empty")
    date_index = {when: index for index, when in enumerate(dates)}
    scheduled: dict[int, float] = {}
    for when, raw_target in target_exposure_by_date.items():
        if when not in date_index:
            raise ValueError(f"exposure decision {when} is outside the curve date axis")
        if isinstance(raw_target, bool):
            raise ValueError(f"exposure target on {when} must be numeric, not boolean")
        target = float(raw_target)
        if not np.isfinite(target) or not 0.0 <= target <= 1.0:
            raise ValueError(f"exposure target on {when} must be finite and in [0, 1], got {raw_target!r}")
        scheduled[date_index[when]] = target
    return _build_strategy_curve(
        book,
        date_count=len(dates),
        starting_equity=starting_equity,
        rebalance_events=True,
        scheduled_exposure_by_index=scheduled,
    )


def build_buy_and_hold_curve(
    book: LegBook,
    *,
    date_count: int,
    starting_equity: float = 1.0,
) -> EquityCurve:
    """Walk the date axis once, applying ``BENCHMARK_RULE_ID``.

    The same axis, the same marks and the same already-netted prices as
    ``build_equity_curve`` — and no rebalance. That single difference is the
    whole function; see ``BENCHMARK_RULE_ID`` for the source rule that requires
    it and for what it measured on our corpus.

    ⚠ ORDER WITHIN A DATE MIRRORS ``build_equity_curve``: exits of positions
    opened earlier, then entries, then same-bar exits, then the mark. Kept
    identical deliberately — the benchmark and the strategy must not differ in
    any respect the comparison is not about.

    ⚠⚠ THE ALLOCATION IS FIXED AT ``starting_equity / n`` AND NEVER RECOMPUTED,
    which is what makes cash provably sufficient: total commitment is exactly
    ``n * (starting_equity / n)``, entries only debit and exits only credit, so
    no leg can be short-funded and no reserved-cash accounting is needed. The
    strategy curve cannot make that guarantee — its target moves with equity —
    which is why it carries a cash cap and a short-funded counter and this does
    not.

    ⚠ COST SHAPE, because the ``leg not in done`` filters look worse than they
    measure. The dominant term is the per-day mark-and-value loop, which is
    ``sum(open_count)`` — one iteration per open position per day, and that is
    simply what marking a portfolio daily IS. The filters run only on a date
    where something closes, so they are bounded by the open set on closing dates
    alone. At full-corpus shape they are the minority term by roughly four to
    one. ⚠ Re-measure rather than trusting that ratio, which is a property of
    the corpus and moves with it::

        PYTHONPATH=. uv run python scripts/verify_2426_benchmark.py --profile

    ⚠ REFUSES AN UNREALISED LEG. ``_benchmark_book`` closes every leg at its last
    usable bar and marks all of them ``realised``, so an unrealised one means the
    caller built the book some other way. The strategy engine handles that case
    by FREEZING the leg and excluding it from the rebalance; with no rebalance to
    exclude it from there is no defined treatment here, and inventing one
    silently would price a position nobody could sell.

    Pure. Reads no database, mutates no argument.
    """
    if date_count < 1:
        raise ValueError(f"date_count must be >= 1, got {date_count}")
    if starting_equity <= 0.0:
        raise ValueError(f"starting_equity must be positive, got {starting_equity}")

    n_legs = len(book)
    equity_path = np.full(date_count, starting_equity, dtype=np.float64)
    invested_path = np.zeros(date_count, dtype=np.float64)
    open_path = np.zeros(date_count, dtype=np.int32)
    traded_path = np.zeros(date_count, dtype=np.float64)
    if n_legs == 0:
        return EquityCurve(
            equity=equity_path,
            invested=invested_path,
            open_count=open_path,
            traded_notional=traded_path,
            rebalance_costs=0.0,
            event_dates=0,
            short_funded_entries=0,
            stale_marks=0,
            unrealised_held=0,
        )

    if not all(book.realised):
        raise ValueError(
            f"{sum(1 for value in book.realised if not value)} of {n_legs} benchmark legs are unrealised — a "
            "buy-and-hold leg is held to its instrument's last usable bar by construction, and there is no "
            "rebalance here from which an unsellable position could be excluded"
        )

    entry_index = np.asarray(book.entry_index, dtype=np.int64)
    exit_index = np.asarray(book.exit_index, dtype=np.int64)
    entry_price = np.asarray(book.entry_price, dtype=np.float64)
    exit_price = np.asarray(book.exit_price, dtype=np.float64)
    mark_offset = np.asarray(book.mark_offset, dtype=np.int64)
    marks = np.frombuffer(book.marks, dtype=np.float64) if len(book.marks) else np.empty(0, dtype=np.float64)

    if int(exit_index.max()) >= date_count:
        raise ValueError(
            f"a leg closes at index {int(exit_index.max())} on a {date_count}-date axis — the axis is short, and "
            "silently truncating it would drop the tail of the curve"
        )
    if int(entry_index.min()) < 0:
        raise ValueError(f"a leg opens at index {int(entry_index.min())}; indices are positions on the date axis")

    opening: list[list[int]] = [[] for _ in range(date_count)]
    closing: list[list[int]] = [[] for _ in range(date_count)]
    for leg in range(n_legs):
        opening[int(entry_index[leg])].append(leg)
        closing[int(exit_index[leg])].append(leg)

    #: ⚠ THE constant of this rule. Computed once, never revisited.
    allocation = starting_equity / n_legs
    units = np.zeros(n_legs, dtype=np.float64)
    last_price = np.zeros(n_legs, dtype=np.float64)

    cash = starting_equity
    open_legs: list[int] = []
    event_dates = 0
    stale_marks = 0

    for day in range(date_count):
        opened_today = opening[day]
        closing_now = closing[day]
        closed_today = [leg for leg in closing_now if int(entry_index[leg]) < day]
        same_bar = [leg for leg in closing_now if int(entry_index[leg]) == day]
        if closed_today or opened_today:
            event_dates += 1

        for leg in closed_today:
            proceeds = units[leg] * exit_price[leg]
            cash += proceeds
            traded_path[day] += proceeds
            units[leg] = 0.0
        if closed_today:
            done = set(closed_today)
            open_legs = [leg for leg in open_legs if leg not in done]

        for leg in opened_today:
            units[leg] = allocation / entry_price[leg]
            cash -= allocation
            traded_path[day] += allocation
            last_price[leg] = entry_price[leg]
            open_legs.append(leg)

        # ⚠ Same-bar legs take no part in the mark: they are gone before the
        # close is read, exactly as in ``build_equity_curve``.
        if same_bar:
            for leg in same_bar:
                proceeds = units[leg] * exit_price[leg]
                cash += proceeds
                traded_path[day] += proceeds
                units[leg] = 0.0
            done = set(same_bar)
            open_legs = [leg for leg in open_legs if leg not in done]

        for leg in open_legs:
            offset = int(mark_offset[leg]) + (day - int(entry_index[leg]))
            mark = marks[offset]
            if np.isnan(mark):
                stale_marks += 1
            else:
                last_price[leg] = mark

        held = 0.0
        for leg in open_legs:
            held += units[leg] * last_price[leg]
        invested_path[day] = held
        equity_path[day] = cash + held
        open_path[day] = len(open_legs)

    return EquityCurve(
        equity=equity_path,
        invested=invested_path,
        open_count=open_path,
        traded_notional=traded_path,
        rebalance_costs=0.0,
        event_dates=event_dates,
        short_funded_entries=0,
        stale_marks=stale_marks,
        unrealised_held=0,
    )


__all__ = [
    "BENCHMARK_RULE_ID",
    "CAPPED_TARGET_EXPOSURE_RULE_ID",
    "EQUITY_CURVE_ENGINE_VERSION",
    "ENTRY_WEIGHT_DRIFT_RULE_ID",
    "MONTH_END_REBALANCE_RULE_ID",
    "SIZING_RULE_ID",
    "EquityCurve",
    "LegBook",
    "SharedMarkLegBook",
    "build_buy_and_hold_curve",
    "build_capped_target_exposure_curve",
    "build_entry_weight_drift_curve",
    "build_equity_curve",
    "build_month_end_rebalanced_curve",
]
