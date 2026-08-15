"""§9's random-entry synthetic control, ORCHESTRATED against the shipped run.

Spec: ``docs/proposals/ta/2026-08-07-bounded-backtester.md`` §9 (*the harness
itself*). Construction and thresholds: ``app/services/random_entry_cohort.py``
(stage 5e-5b), which this module does not restate. Caller:
``app/services/backtest_run.py``. Refs #2240, #2601.

WHAT WAS MISSING, IN ONE SENTENCE
---------------------------------
``random_entry_cohort`` owns the PLACEMENT (``place_entries``) and the VERDICT
(``evaluate_control``); ``result_ledger`` owns the PERSISTENCE; ``strategy_result``
owns the three refusals. Nothing owned the middle — turning a placement into a
member's equity curve — so ``synthetic_control_not_run`` stood on every stored
row and no invocation could clear it. That middle is this module.

⚠⚠ THE MATCHING AXES ARE NOT REDEFINED HERE. ``random_entry_cohort``'s header
fixes them: *"a member differs from the real sleeve in the ENTRY BARS and in
nothing else"*, the universe / date axis / cost model / sizing rule /
quarantine arm / exit-side accounting all held fixed. This module supplies those
same fixtures from the run that is already computing them, and adds no axis of
its own. The one thing it must DECIDE is the placement SPACE — which bars count
as "a bar this strategy could have opened on" — and that decision is measured
rather than declared; see below.

THE PLACEMENT SPACE IS MEASURED FROM THE RUN'S OWN VERDICTS
-----------------------------------------------------------
``verify_2240_random_entry_cohort.py`` imports each strategy's ``WARMUP_BARS``
constant and starts the eligible space there. That does not generalise: S-2
(``cross_sectional``) exports no such constant, and a manifest field for it
would be the *"per-strategy tuning"* ``strategy_manifest``'s header refuses.

It is also unnecessary, because the run already emits the answer. §3.1 makes
evaluability a property of the STRATEGY decided before any condition runs, so
``strategy_registry.evaluate`` emits ONE verdict PER BAR and stamps the cold
ones ``not_evaluable("insufficient_warmup")``. A bar therefore carries its own
"could this strategy have decided here" flag, for every strategy class, with no
constant to keep in step.

So the eligible FILL bars of a series are, exactly:

1. the ``t+1`` of every ENTRY-leg row whose verdict is not ``not_evaluable`` —
   ``signal_ledger.resolve_fills``' own rule, *"``fill_index = signal_index + 1``,
   always"*, applied to the bars the strategy was live on rather than only to
   the ones it fired on;
2. that carry a usable open (present and ``> 0``) — the ledger's
   ``unusable_fill_price`` refusal;
3. inside the evaluation window and on the panel axis;
4. whose raw and total-return closes are both finite and positive, which is
   ``_absorb``'s ``total_return_price_missing`` exclusion applied to the
   endpoint a permuted leg would be priced at.

⚠ This is a NARROWING relative to the verify script's space (which admitted any
in-window bar with an open, past a fixed warm-up) and it is the correct
direction: every clause above is a condition the real strategy was under, and a
member placing a position where the real one structurally could not is a member
drawn from a wider null than §9 asks for.

⚠⚠ THE CONTROL IS COMPUTED FOR THE ``in_sample`` NAMESPACE ONLY, AND THAT IS A
REQUIREMENT RATHER THAN A SAVING. ``backtest_run``'s header: *"criterion 5's
whole mechanism is that looking at [the hold-out] is rare, deliberate and
logged"*. A cohort is 1,000 evaluations; running one over the withheld side is
1,000 looks at it, which is the audit trail measuring its own automation at a
thousand times the rate §4 already refused at one. A hold-out row therefore
keeps ``synthetic_control_not_run`` — DECLARED (``HOLDOUT_CONTROL_REASON``), not
silently absent.

⚠ The in-sample restriction also makes the placement exact rather than
approximate. ``namespace_for_position`` sends a position SPANNING the boundary
to the hold-out, so an in-sample position has both ends before it; the eligible
ordinals are therefore in-sample bars, and ``entry_ordinal + hold`` cannot leave
the set. A hold-out cohort would have to reproduce a population that MIXES
spanning and wholly-withheld positions, which is a second matching question §9
does not settle.

WHAT THE COHORT MATCHES, AND WHAT IT CANNOT
-------------------------------------------
The permuted population is the strategy's REALISED, COSTED, PLACEABLE positions
— the ones with a close bar, no ``uncosted_reason``, and both endpoints on the
eligible table. Everything else is counted in ``unmatchable`` and reported. That
is stage 5e-5b's own contract (*"REALISED CLOSES ONLY … the cohort matches the
realised population"*) and not a new exclusion: a position marked out at the
window end exits at a CLOSE with one side costed, so a permuted twin of it would
be a differently-priced trade.

⚠ ``MatchResidual.strategy_trade_count`` is therefore the MATCHABLE count and
not ``metrics.trade_count``. The two coincide only when nothing was unmatchable,
and ``CohortResult.unmatchable`` is on the report so the gap is visible rather
than inferred from a residual that looks exact.

THE SEED HIERARCHY
------------------
Root: ``COHORT_ROOT_SEED``, the declared constant stage 5e-5b froze and
``sql/268`` stores. Per member: ``member_seed(index)``, a ``SeedSequence`` keyed
by ``spawn_key=(index,)`` so member ``m``'s stream is a pure function of
``(root, m)`` and no sharding or ordering can move a draw. ⚠ NO SECOND ROOT IS
MINTED PER RUN. A root derived from run identity would make two evaluations of
the same strategy draw different nulls, and §9's *"with the seed recorded"* is
satisfied by a seed that is the same one next time.
"""

from __future__ import annotations

import logging
import time
from array import array
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date
from typing import Final

import numpy as np
import numpy.typing as npt

from app.services.cost_model import UNKNOWN_NOMINAL_PRICE_BAND
from app.services.equity_curve import LegBook, build_equity_curve
from app.services.indicator_series import BarSeries
from app.services.position_builder import Window
from app.services.position_costing import CostedPosition
from app.services.random_entry_cohort import (
    SPEC_COHORT_SIZE,
    MatchResidual,
    MemberOutcome,
    SyntheticControl,
    evaluate_control,
    match_residual,
    member_seed,
    net_entry_prices,
    net_exit_prices,
    place_entries,
    slack,
)
from app.services.signal_ledger import LedgerRow
from app.services.strategy_result import ResultNamespace, namespace_for_position
from app.services.strategy_statistics import DatedEquityCurve, StrategyMetrics, TradeReturns, compute_metrics

logger = logging.getLogger(__name__)

#: How the eligible fill bars were selected, frozen. ⚠ A member is only
#: comparable with another member placed into the SAME space, and this space is
#: ours (no source rule fixes it) — so it is named, versioned, and reported
#: beside ``COHORT_MODEL_ID`` rather than left implicit in a loop body.
PLACEMENT_SPACE_ID: Final = "evaluable-entry-fill-bars-v1"

#: The only namespace a control is computed for. See the header.
CONTROL_NAMESPACE: Final[ResultNamespace] = "in_sample"

#: What a hold-out row's ``synthetic_control_not_run`` MEANS, so the refusal is
#: a declaration and not an omission.
HOLDOUT_CONTROL_REASON: Final = (
    "a 1,000-member cohort over the hold-out namespace is 1,000 looks at withheld data, which is what criterion 5's "
    "access log exists to keep rare; the control is computed for the in_sample namespace only"
)

#: The half-spread every leg of this corpus carries. ⚠ NOT a simplification:
#: ``cost_band_for(..., price_basis="split_adjusted")`` returns
#: ``UNKNOWN_NOMINAL_PRICE_BAND`` for EVERY price, because a split-adjusted
#: historical price cannot select a nominal band. The real sleeve is costed on
#: exactly that constant, so reading it here is sharing the cost model rather
#: than assuming one.
_HALF_SPREAD: Final = float(UNKNOWN_NOMINAL_PRICE_BAND.half_spread)


# ---------------------------------------------------------------------------
# The placement space, accumulated during the run's own corpus pass
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SeriesPlacement:
    """One instrument's eligible fill bars, and the holds to permute into them.

    ⚠ ONE ``adjusted_open`` ARRAY AND NOT TWO PRICE ARRAYS. The net buy and sell
    prices are the same number times ``(1 ± h)`` with ``h`` constant over this
    corpus, so storing both would double a per-eligible-bar array over 5,266
    series to hold a scalar multiple. The multiply happens per member, where it
    is vectorised anyway.

    ⚠ ``marks`` IS THE RUN'S OWN ARRAY, SHARED NOT COPIED. ``evaluate_arm``
    already retains the total-return close array per instrument to build the
    benchmark; a second copy is ~200 MB of the same floats. It is read-only from
    here on.
    """

    #: Panel-axis index of each eligible fill bar, strictly ascending.
    panel: npt.NDArray[np.int64]
    #: That bar's open, already carried onto the total-return basis by the same
    #: ``wealth_close / raw_close`` factor ``_absorb`` applies to a real leg.
    adjusted_open: npt.NDArray[np.float64]
    #: The realised holds to permute, in ELIGIBLE-ORDINAL units.
    holds: npt.NDArray[np.int64]
    #: Total-return closes, panel-aligned from ``marks_first``.
    #:
    #: ⚠⚠ AN ndarray AND NOT THE CALLER'S ``list``. ``evaluate_arm`` builds this
    #: span as a Python list and then keeps only a compact ``array("d")`` copy of
    #: it; retaining the LIST here would pin ~25M boxed Python floats across the
    #: whole corpus — gigabytes, for the same numbers the run already holds in
    #: 200 MB. Converted at collection, once per series.
    marks: npt.NDArray[np.float64]
    marks_first: int

    def __post_init__(self) -> None:
        if self.panel.size != self.adjusted_open.size:
            raise ValueError(f"{self.panel.size} eligible bars against {self.adjusted_open.size} prices")


@dataclass
class CohortCollector:
    """The cohort's inputs, gathered from the corpus pass the run already makes.

    ⚠⚠ NO SECOND CORPUS READ. §9's control was costed at stage 5e-5b as
    *"~5.5 CPU hours of ARITHMETIC sitting behind ~1.5 hours of DATABASE READ
    that is identical for every member"*, and the verify script paid the read
    once by caching to disk. A job cannot depend on a developer cache, so the
    read is paid by riding the pass ``evaluate_arm`` is making anyway — the
    collector is fed inside its loop and holds only what a member needs.
    """

    window: Window
    placements: list[SeriesPlacement] = field(default_factory=list)
    #: Realised positions the permutation cannot reproduce, by reason.
    unmatchable: Counter[str] = field(default_factory=Counter)
    #: Series whose realised holds do not fit their own eligible space. ⚠ A
    #: CONTRADICTION rather than a rare shape — the real positions are
    #: non-overlapping in this same ordinal space — so the series is refused
    #: whole and counted, never trimmed. Trimming would change the match.
    no_slack_series: int = 0

    def collect(
        self,
        *,
        rows: Sequence[LedgerRow],
        series: BarSeries,
        costed: Sequence[CostedPosition],
        axis_pos: Mapping[date, int],
        raw_closes: Sequence[float],
        wealth_closes: Sequence[float],
        first_axis_index: int,
    ) -> None:
        """Add one instrument's placement space. Called once per series."""
        # ⚠ BUILT ONCE AND PASSED DOWN. Both halves of this method need the
        # date -> bar-index map and the corpus pass calls this ~5,266 times per
        # arm, so building it twice is a second full dict per series for nothing
        # (review bot NITPICK, PR #2619).
        bar_of = {when: index for index, when in enumerate(series.dates)}
        eligible_bar, panel, adjusted = _eligible_fill_bars(
            rows=rows,
            series=series,
            bar_of=bar_of,
            axis_pos=axis_pos,
            window=self.window,
            raw_closes=raw_closes,
            wealth_closes=wealth_closes,
            first_axis_index=first_axis_index,
        )
        if not eligible_bar:
            _count_unmatchable(costed, self.unmatchable)
            return
        ordinal_of = {bar: ordinal for ordinal, bar in enumerate(eligible_bar)}
        holds = _matchable_holds(costed, bar_of=bar_of, ordinal_of=ordinal_of, unmatchable=self.unmatchable)
        if not holds:
            return
        held = np.asarray(holds, dtype=np.int64)
        if slack(eligible=len(eligible_bar), holds=held) < 0:
            self.no_slack_series += 1
            self.unmatchable["series_cannot_carry_its_own_holds"] += len(holds)
            return
        self.placements.append(
            SeriesPlacement(
                panel=np.asarray(panel, dtype=np.int64),
                adjusted_open=np.asarray(adjusted, dtype=np.float64),
                holds=held,
                marks=np.asarray(wealth_closes, dtype=np.float64),
                marks_first=first_axis_index,
            )
        )

    @property
    def matchable_trade_count(self) -> int:
        return sum(int(placement.holds.size) for placement in self.placements)


def _eligible_fill_bars(
    *,
    rows: Sequence[LedgerRow],
    series: BarSeries,
    bar_of: Mapping[date, int],
    axis_pos: Mapping[date, int],
    window: Window,
    raw_closes: Sequence[float],
    wealth_closes: Sequence[float],
    first_axis_index: int,
) -> tuple[list[int], list[int], list[float]]:
    """The four clauses in the header, applied in order. Returns parallel lists.

    ⚠ KEYED ON THE ENTRY LEG ONLY. An exit-leg verdict says nothing about
    whether a position could be OPENED on the following bar, and S-1/S-3 emit
    both legs on every bar — so counting exits would double the space and let a
    member open where the strategy was cold.

    ⚠ ``bar_of`` IS SUPPLIED, not rebuilt. The caller already needs the same
    ``date -> bar index`` map to place the realised holds.
    """
    fills: set[int] = set()
    for row in rows:
        if row.signal_kind != "entry" or row.not_evaluable_reason is not None:
            continue
        signal_index = bar_of.get(row.signal_bar_date)
        if signal_index is None:  # pragma: no cover - rows are resolved from this series
            continue
        fill_index = signal_index + 1
        if fill_index >= len(series):
            # `no_fill_bar` — the series ended, so no decision on that bar could
            # have been acted on. `resolve_fills` refuses it for the real leg.
            continue
        fills.add(fill_index)

    eligible_bar: list[int] = []
    panel: list[int] = []
    adjusted: list[float] = []
    for fill_index in sorted(fills):
        when = series.dates[fill_index]
        if not window.contains(when):
            continue
        slot = axis_pos.get(when)
        if slot is None:
            continue
        if namespace_for_position(when, when) != CONTROL_NAMESPACE:
            continue
        bar_open = series.rows[fill_index].get("open")
        if bar_open is None or bar_open <= 0:
            continue
        offset = slot - first_axis_index
        if not 0 <= offset < len(raw_closes):  # pragma: no cover - slot is inside this series' span
            continue
        raw_close = raw_closes[offset]
        wealth_close = wealth_closes[offset]
        if not _usable(raw_close) or not _usable(wealth_close):
            continue
        eligible_bar.append(fill_index)
        panel.append(slot)
        # The same carry ``_absorb`` applies to a real leg: the net price is
        # scaled by that bar's own total-return factor, so the two are on one
        # basis and the cohort is not comparing an adjusted sleeve with an
        # unadjusted null.
        adjusted.append(float(bar_open) * wealth_close / raw_close)
    return eligible_bar, panel, adjusted


def _usable(value: float) -> bool:
    return bool(np.isfinite(value)) and value > 0.0


def _count_unmatchable(costed: Sequence[CostedPosition], unmatchable: Counter[str]) -> None:
    """Charge every realised in-sample position to ``no_eligible_bar``."""
    for row in costed:
        if namespace_for_position(row.position.entry_fill_bar_date, row.position.close_bar_date) == CONTROL_NAMESPACE:
            unmatchable["no_eligible_bar"] += 1


def _matchable_holds(
    costed: Sequence[CostedPosition],
    *,
    bar_of: Mapping[date, int],
    ordinal_of: Mapping[int, int],
    unmatchable: Counter[str],
) -> list[int]:
    """This series' realised in-sample holds, in eligible-ordinal units.

    ⚠ EVERY REJECTION IS COUNTED UNDER ITS OWN CODE. A silently shorter hold
    list is a cohort matched to a population nobody named, and the residual
    would still read as exact because the permutation preserves whatever it was
    given.
    """
    holds: list[int] = []
    for row in costed:
        position = row.position
        if namespace_for_position(position.entry_fill_bar_date, position.close_bar_date) != CONTROL_NAMESPACE:
            continue
        if row.uncosted_reason is not None:
            unmatchable[row.uncosted_reason] += 1
            continue
        if position.close_bar_date is None:  # pragma: no cover - an in-sample position is closed by construction
            unmatchable["open_at_window_end"] += 1
            continue
        entry_bar = bar_of.get(position.entry_fill_bar_date)
        exit_bar = bar_of.get(position.close_bar_date)
        if entry_bar is None or exit_bar is None:  # pragma: no cover - both are bars of this series
            unmatchable["bar_off_series"] += 1
            continue
        entry_ordinal = ordinal_of.get(entry_bar)
        exit_ordinal = ordinal_of.get(exit_bar)
        if entry_ordinal is None or exit_ordinal is None or exit_ordinal < entry_ordinal:
            # The exit bar is not itself an eligible ENTRY bar — its open is
            # unusable, or its closes are missing. A permuted twin would have to
            # exit on a bar the cohort cannot price, so it is not matched.
            unmatchable["endpoint_not_eligible"] += 1
            continue
        holds.append(exit_ordinal - entry_ordinal)
    return holds


# ---------------------------------------------------------------------------
# Running the cohort
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CohortResult:
    """§9's control, its match residual, and what the run had to exclude."""

    control: SyntheticControl
    residual: MatchResidual
    placement_space_id: str
    #: Realised in-sample positions the permutation could not reproduce.
    unmatchable: Mapping[str, int]
    no_slack_series: int
    series_placed: int
    elapsed_s: float

    @property
    def seconds_per_member(self) -> float:
        return self.elapsed_s / self.control.cohort_size


def run_cohort(
    collector: CohortCollector,
    *,
    axis: Sequence[date],
    strategy_metrics: StrategyMetrics,
    benchmark: DatedEquityCurve | None,
    cohort_size: int = SPEC_COHORT_SIZE,
) -> CohortResult:
    """Place, price and measure ``cohort_size`` members. Pure; reads no database.

    ``axis`` is the real strategy's complete fixed namespace tuple. Every member
    uses it unchanged; random placement may change the path but cannot select a
    more flattering annualisation interval.

    ⚠⚠ ``benchmark`` IS ``None`` ON EVERY REAL CALL AND THAT IS FORCED, not a
    shortcut. ``compute_metrics`` refuses a benchmark whose curve length differs
    from the strategy's (*"the benchmark curve has N points against the
    strategy's M"*), and each member's span is its own — so the sleeve's
    benchmark cannot be reused, and building one per member would compute
    ``return_vs_buy_and_hold_pct``, which NEITHER of §9's two cuts consumes,
    1,000 times. ``compute_metrics`` already documents the null case as an
    explicit absence rather than a silent 0% benchmark. The parameter stays so a
    test with a matching span can exercise the populated path.

    ⚠ NO BLOCK BOOTSTRAP PER MEMBER. §9 reads the cohort's Sharpe and net
    return; criterion 3's correction is a property of the REAL sleeve's trade
    population, and running it 1,000 times would add hours to compute a number
    no threshold consumes. Stage 5e-5b's ``--cohort`` arm made the same call.

    ⚠⚠ THERE IS NO ``root_seed`` PARAMETER, AND ITS ABSENCE IS THE POINT. An
    earlier draft took one, recorded it on the row, and still drew every member
    from ``member_seed(index)`` — which is keyed on ``COHORT_ROOT_SEED`` and
    nothing else. The stored seed would then have described the bootstrap and
    NOT the cohort it sits beside, so §9's *"with the seed recorded"* would be
    satisfied by a number that reproduces neither. (Codex checkpoint 2.) The
    root is the module constant, one value, used for both.
    """
    if cohort_size < 1:
        raise ValueError(f"cohort size must be positive, got {cohort_size}")
    if not collector.placements:
        raise ValueError(
            "no series carries a placeable in-sample position — §9's control cannot be built against an empty "
            "placement space, and a control computed from nothing would read as a passed threshold"
        )
    started = time.monotonic()
    expected = collector.matchable_trade_count
    members: list[MemberOutcome] = []
    for index in range(cohort_size):
        rng = np.random.Generator(np.random.PCG64(member_seed(index)))
        book, returns, entry_dates, exit_dates = _place_member(rng, collector.placements, axis=axis)
        if len(book) != expected:
            # ⚠ EQUALITY, per member. The permutation preserves the trade count
            # by construction, so a mismatch is the one failure mode it can have
            # — a series whose holds were silently dropped — and a tolerance
            # here would hide exactly that.
            raise RuntimeError(
                f"cohort member {index} placed {len(book):,} legs against the strategy's {expected:,} matchable "
                "positions — the permutation is supposed to preserve the count per series"
            )
        dates = tuple(axis)
        curve = build_equity_curve(book, date_count=len(dates))
        metrics = compute_metrics(
            DatedEquityCurve(dates=dates, curve=curve),
            trades=TradeReturns(
                net_return_pct=tuple(returns),
                entry_fill_date=tuple(entry_dates),
                exit_bar_date=tuple(exit_dates),
                open_count=0,
                unpriced_count=0,
            ),
            buy_and_hold=benchmark,
            bootstrap_seed=None,
        )
        members.append(
            MemberOutcome(
                index=index,
                sharpe=metrics.sharpe,
                total_return_pct=metrics.total_return_pct,
                exposure_time_pct=metrics.exposure_time_pct,
                turnover_annualised=metrics.turnover_annualised,
                trade_count=metrics.trade_count,
            )
        )
    frozen = tuple(members)
    return CohortResult(
        control=evaluate_control(
            frozen,
            strategy_sharpe=strategy_metrics.sharpe,
            strategy_return_pct=strategy_metrics.total_return_pct,
            # ⚠ NOT PASSED, so it takes ``evaluate_control``'s own default —
            # which is ``COHORT_ROOT_SEED``, the same constant ``member_seed``
            # keys on. One root, both uses, no way for them to disagree.
        ),
        residual=match_residual(
            frozen,
            # ⚠ THE MATCHABLE COUNT, not ``metrics.trade_count``. See the header:
            # the cohort is permuted from the realised, costed, placeable
            # population, and comparing it against a wider one would report a
            # residual for a match nobody attempted.
            strategy_trade_count=expected,
            strategy_exposure_time_pct=strategy_metrics.exposure_time_pct,
            strategy_turnover_annualised=strategy_metrics.turnover_annualised,
        ),
        placement_space_id=PLACEMENT_SPACE_ID,
        unmatchable=dict(collector.unmatchable),
        no_slack_series=collector.no_slack_series,
        series_placed=len(collector.placements),
        elapsed_s=time.monotonic() - started,
    )


def _place_member(
    rng: np.random.Generator,
    placements: Sequence[SeriesPlacement],
    *,
    axis: Sequence[date],
) -> tuple[LegBook, array[float], list[date], list[date]]:
    """One member: the same holds, redrawn entry bars, everything else fixed."""
    book = LegBook()
    returns: array[float] = array("d")
    entry_dates: list[date] = []
    exit_dates: list[date] = []
    for placement in placements:
        entries, permuted = place_entries(rng, eligible=int(placement.panel.size), holds=placement.holds)
        entry_price = net_entry_prices(placement.adjusted_open[entries], np.full(entries.size, _HALF_SPREAD))
        exit_slot = entries + permuted
        exit_price = net_exit_prices(placement.adjusted_open[exit_slot], np.full(entries.size, _HALF_SPREAD))
        entry_panel = placement.panel[entries]
        exit_panel = placement.panel[exit_slot]
        mark_base = -placement.marks_first
        for leg in range(int(placement.holds.size)):
            entry_index = int(entry_panel[leg])
            exit_index = int(exit_panel[leg])
            book.add(
                entry_index=entry_index,
                exit_index=exit_index,
                entry_price=float(entry_price[leg]),
                exit_price=float(exit_price[leg]),
                half_spread=_HALF_SPREAD,
                realised=True,
                marks=placement.marks[mark_base + entry_index : mark_base + exit_index + 1].tolist(),
            )
            returns.append(float((exit_price[leg] - entry_price[leg]) / entry_price[leg] * 100.0))
            entry_dates.append(axis[entry_index])
            # #2623 gap 1 — the permuted exit bar, already in scope above.
            exit_dates.append(axis[exit_index])
    return book, returns, entry_dates, exit_dates


__all__ = [
    "CONTROL_NAMESPACE",
    "HOLDOUT_CONTROL_REASON",
    "PLACEMENT_SPACE_ID",
    "CohortCollector",
    "CohortResult",
    "SeriesPlacement",
    "run_cohort",
]
