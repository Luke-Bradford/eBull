"""§3.2 — the deliberately-triggered backtest run.

Spec: ``docs/proposals/ta/2026-08-08-strategy-backtest-run.md`` (which settles
§3.2 of ``2026-08-08-strategy-runner-and-manifest.md``). Sibling job:
``app/services/strategy_signal_scan.py`` (§3.1). Writer:
``app/services/result_ledger.py``. Row shape: ``sql/262``-``sql/269``. Frozen
literals and the gate: ``app/services/strategy_result.py``. Refs #2240, #2394.

⚠⚠ THE INVOCATION UNIT IS THE STRATEGY **SET**, NOT ONE STRATEGY (spec §2).
``deflated_sharpe.MIN_MEASURED_TRIALS`` is 2, so a single strategy has no
``V[SR_n]`` and every row it writes carries ``deflated_sharpe = NULL`` — a
refusal no amount of re-running that one strategy can clear. One invocation
therefore evaluates every runnable manifest entry and deflates once across them.
A ``strategy_id`` param narrows the set for debugging, and the run then DECLARES
the Deflated Sharpe absent and says why rather than writing a row that looks
incomplete for an unexplained reason.

⚠⚠ THE HOLD-OUT ARM IS FREE, AND THAT IS THE PROBLEM (spec §4).
``strategy_result.namespace_for_position`` is a FILTER over positions one corpus
sweep has already produced, so computing the withheld side costs nothing. But
criterion 5's whole mechanism is that looking at it is rare, deliberate and
logged; a job that computes and writes both every time turns
``strategy_holdout_accesses`` into a count of its own automation. So the hold-out
partition is **counted and discarded** unless the invocation supplies both
``holdout_purpose`` and ``holdout_accessed_by`` — and it is not merely left
unwritten, it is not measured at all. The count is the only hold-out figure an
in-sample run emits.

⚠ WHY THE TRIGGER IS MANUAL, and it is NOT "because it is expensive". Half an
hour is a scheduled job's workload. The reasons are governance: criterion 5
requires a purpose a cron fire cannot supply (§4), and a stored row is
meaningless if its identity moved between runs (§10). Neither is affected by
making the run faster.

⚠ S-4 IS EXCLUDED, NOT SKIPPED. It is the only ``level_based`` manifest entry,
``build_positions`` refuses a level-based entry with no outcome at the pinned
version pair, and nothing in ``app/`` constructs an ``ExitLevels`` to produce
one. ``runnable_strategies`` DEMONSTRATES that refusal by calling the builder
rather than quoting its docstring, and the run reports the exclusion with the
message — criterion 9's *"exclusion is visible rather than assumed harmless"*.

⚠ IT READS THE FROZEN RESEARCH CORPUS ONLY. ``load_masked_series`` at
``CORPUS_VERSION``; ``strategy_signal_scan`` reads live ``price_daily`` through
``price_masked_bars``. The two sources are deliberately different and their rows
are not comparable. S-2 is where that could slip — assembling a panel is the one
place a ``price_daily`` read would look like a convenience rather than a corpus
change.
"""

from __future__ import annotations

import logging
import math
import time
from array import array
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from typing import Any, Final

import numpy as np
import psycopg

from app.services.cost_model import CARRY_UNMODELLED, COST_MODEL_ID, half_spread_for
from app.services.deflated_sharpe import (
    MIN_MEASURED_TRIALS,
    DeflatedSharpeResult,
    TradeMoments,
    average_trial_correlation,
    deflated_sharpe,
    implied_independent_trials,
    trade_moments,
)
from app.services.equity_curve import (
    BENCHMARK_RULE_ID,
    SIZING_RULE_ID,
    LegBook,
    build_buy_and_hold_curve,
    build_equity_curve,
)
from app.services.indicator_series import BarSeries, Universe
from app.services.outcome_resolver import RULE_SET_VERSION as OUTCOME_RULE_SET_VERSION
from app.services.position_builder import RULE_SET_VERSION as POSITION_RULE_SET_VERSION
from app.services.position_builder import (
    EntryFill,
    ExitFill,
    ExitRegime,
    OutcomePin,
    Window,
    build_positions,
)
from app.services.position_costing import CostedPosition, cost_positions
from app.services.price_structure import StructureBar
from app.services.research_price_structure_store import (
    QUARANTINE_ARMS,
    QUARANTINE_RULE_SET_VERSION,
    QuarantineArm,
    load_masked_series,
)
from app.services.result_ledger import (
    holdout_access_counts,
    store_holdout_arm_pair,
    store_in_sample_arm_pair,
)
from app.services.signal_ledger import LedgerRow, resolve_fills
from app.services.strategies.validated_universe import load_validated_universe
from app.services.strategy_manifest import STRATEGY_MANIFEST, StrategyEntry
from app.services.strategy_registry import StrategyIdentity, StrategySignal, stage_cross_sectional_member
from app.services.strategy_result import (
    AMBIGUITY_ARMS,
    CORPUS_VERSION,
    EVALUATION_WINDOW_END,
    EVALUATION_WINDOW_START,
    HOLDOUT_BOUNDARY,
    AmbiguityArm,
    PromotionCandidate,
    PromotionRefusal,
    ResultIdentity,
    ResultNamespace,
    StrategyResult,
    check_promotable,
    namespace_for_position,
)
from app.services.strategy_statistics import StrategyMetrics, TradeReturns, compute_metrics
from app.services.technical_analysis import OHLCVRow
from app.services.trial_register import TRIAL_REGISTER

logger = logging.getLogger(__name__)

#: The research corpus is survivor-only (#2284) and every row this job writes
#: inherits that label (#2288). ⚠ It is BOTH the ``Universe`` hashed into
#: ``StrategyIdentity`` and the ``universe_basis`` stamped on the result, and
#: they must not drift apart: the identity says what the strategy was told, the
#: basis says what the gate refuses on.
BACKTEST_UNIVERSE: Universe = "survivor_only"

#: §6 — ``portfolio`` scope is out. It is a statement about a cross-strategy
#: allocator and nothing in ``app/`` allocates across strategies.
RESULT_SCOPE: Final = "sleeve"

#: Criterion 3's block-bootstrap RNG seed, frozen HERE in ``app/``.
#:
#: ⚠⚠ NOT ``scripts/verify_2240_statistics.BOOTSTRAP_SEED``. That literal lives
#: in a verify script no production path imports, and inheriting it would make
#: every stored ``effective_sample_size`` a function of a file that is not part
#: of the application. ``compute_metrics`` computes the bootstrap ONLY when a
#: seed is passed (*"⚠⚠ bootstrap_seed IS REQUIRED FOR CRITERION 3 AND DEFAULTS
#: TO OFF"*), and criterion 6's Deflated Sharpe consumes its output — so a run
#: with no seed cannot produce a promotable row at all.
#:
#: ⚠ Any fixed value is as good as any other; what matters is that it is
#: DECLARED and does not drift. Changing it changes every stored interval, so it
#: is frozen rather than defaulted, exactly as criterion 11 treats a declared
#: input.
BACKTEST_BOOTSTRAP_SEED: Final = 20260808

#: §9 — the three refusals no invocation of this job can close, whatever it
#: measures. ``universe_basis_not_survivorship_free`` is blocked on #2284's
#: corpus purchase, ``carry_unmodelled`` on #2277's carry measurement, and
#: ``synthetic_control_not_run`` on a stage this cut does not contain (§9: the
#: control is 1,000 full-corpus evaluations PER STRATEGY, and the only cohort
#: that exists lives in a developer cache no job may depend on).
#:
#: ⚠ The job cannot make anything promotable, and that is correct rather than a
#: shortfall — §6 of the bounded-backtester spec states the intended initial
#: state in those words. What it changes is that the refusals become SPECIFIC
#: AND FEW instead of eight-of-eight.
STANDING_REFUSALS: Final[frozenset[PromotionRefusal]] = frozenset(
    {
        "universe_basis_not_survivorship_free",
        "carry_unmodelled",
        "synthetic_control_not_run",
    }
)

#: §6's two arm vocabularies, ITERATED IN A DECLARED ORDER rather than by
#: sorting the frozensets.
#:
#: ⚠ Not style. ``AMBIGUITY_ARMS`` and ``QUARANTINE_ARMS`` are ``frozenset[str]``
#: (the runtime companions to the ``Literal`` aliases), so ``sorted()`` of either
#: yields plain ``str`` — which type-checks into a ``ResultIdentity`` field that
#: is supposed to be closed, and would let a widened vocabulary reach the row
#: unchecked. The tuples below are typed, and the completeness assertion under
#: them is what stops a THIRD arm being added to the vocabulary and silently
#: never written: the job would otherwise keep producing 24 rows while the arm
#: space said 36.
AMBIGUITY_ARM_ORDER: Final[tuple[AmbiguityArm, ...]] = ("best_case", "worst_case")
QUARANTINE_ARM_ORDER: Final[tuple[QuarantineArm, ...]] = ("admitted", "masked")

if set(AMBIGUITY_ARM_ORDER) != AMBIGUITY_ARMS or set(QUARANTINE_ARM_ORDER) != QUARANTINE_ARMS:  # pragma: no cover
    raise RuntimeError(
        f"backtest_run iterates {sorted(AMBIGUITY_ARM_ORDER)} x {sorted(QUARANTINE_ARM_ORDER)} against the declared "
        f"{sorted(AMBIGUITY_ARMS)} x {sorted(QUARANTINE_ARMS)} — an arm the run never iterates is an arm the run "
        "never writes, and the row count would keep looking complete"
    )


#: The pin ``runnable_strategies`` hands ``build_positions`` when demonstrating
#: the level-based refusal. ⚠ REAL values, not stand-ins: the builder refuses a
#: level-based regime with a NULL pin one check earlier, so a stand-in that
#: failed construction would produce the wrong raise and demonstrate nothing.
_OUTCOME_PIN: Final = OutcomePin(
    rule_set_version=OUTCOME_RULE_SET_VERSION,
    input_rule_set_version=QUARANTINE_RULE_SET_VERSION,
)

#: A calendar to hand ``decision_calendar`` so every manifest entry's
#: ``exit_regime`` CONSTRUCTS during the runnable probe. ⚠ Not cosmetic:
#: ``ExitRegime`` refuses an EMPTY ``rebalance_dates`` — "no calendar" and "a
#: calendar with no dates" stay distinguishable — so S-2's factory raises on
#: ``()``. The regime SHAPE, which is all the probe reads, does not depend on
#: which dates are in it; the real run builds the regime from the real axis.
_PROBE_CALENDAR: Final = tuple(
    date.fromordinal(n) for n in range(date(2020, 1, 1).toordinal(), date(2021, 1, 1).toordinal())
)

_AXIS_SQL = """
    SELECT DISTINCT d.bar_date
    FROM research_price_series s
    JOIN research_price_daily d ON d.series_id = s.series_id
    WHERE s.instrument_id = ANY(%(ids)s)
      AND d.bar_date BETWEEN %(start)s AND %(end)s
    ORDER BY 1
"""

_SERIES_SQL = """
    SELECT instrument_id, series_id
    FROM research_price_series
    WHERE instrument_id = ANY(%(ids)s)
    ORDER BY instrument_id, series_id
"""

#: ⚠ Reads the STORE, not the ``strategy_results`` view: the view is filtered to
#: ``namespace = 'in_sample'``, so a collision check through it would be blind to
#: every hold-out row and the run would discover the duplicate at INSERT — after
#: the corpus pass §8 measures in minutes.
_EXISTING_RESULT_VERSIONS = """
    SELECT result_version
    FROM strategy_results_store
    WHERE result_version = ANY(%(result_versions)s)
"""


# ---------------------------------------------------------------------------
# What the run reports about itself (§11)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ExcludedStrategy:
    """A manifest entry that cannot produce a result row, and the reason.

    ⚠ The reason is the message ``build_positions`` RAISED, not a paraphrase of
    it. §3: the entire exclusion of S-4 rests on that refusal, and a paraphrase
    goes stale the day the builder's rule changes.
    """

    strategy_id: str
    reason: str


@dataclass(frozen=True)
class NamespaceMeasurement:
    """One ``(strategy, quarantine arm, namespace)`` measurement, before writing.

    ⚠ The equity AXIS here is the namespace's own truncated span (§5) while
    ``window_start`` / ``window_end`` on the row stay the full evaluation window.
    ``sql/262`` is explicit that no CHECK ties the two — *"the window is the
    EVALUATION window and the namespace selects within it"* — and storing the
    truncated span in the window columns would make two rows over one corpus
    look like two corpora.
    """

    namespace: ResultNamespace
    metrics: StrategyMetrics
    moments: TradeMoments | None
    #: Mean realised trade return per ENTRY date. Criterion 3's cluster key, so
    #: this phase's two correlation constructions agree about "the same day".
    daily_returns: Mapping[date, float]
    evaluated_instrument_ids: frozenset[int]
    position_count: int
    axis_first: date
    axis_last: date


@dataclass(frozen=True)
class ArmMeasurement:
    """Everything one ``(strategy, quarantine arm)`` corpus pass produced."""

    strategy_id: str
    strategy_version: str
    quarantine_arm: QuarantineArm
    namespaces: Mapping[ResultNamespace, NamespaceMeasurement]
    #: §4 — the hold-out side of the partition on an in-sample invocation: the
    #: only hold-out figure such a run emits.
    holdout_positions_discarded: int
    close_sources: Mapping[str, int]
    series_evaluated: int
    elapsed_s: float


@dataclass(frozen=True)
class WrittenRow:
    """One stored row, with the refusal list re-measured on it (criterion 8)."""

    strategy_id: str
    result_version: str
    namespace: ResultNamespace
    ambiguity_arm: AmbiguityArm
    quarantine_arm: QuarantineArm
    result_id: int
    evaluated_instrument_count: int
    refusals: tuple[PromotionRefusal, ...]


@dataclass(frozen=True)
class BacktestRunReport:
    """Spec §11's observability contract, as data rather than as log lines."""

    runnable: tuple[str, ...]
    excluded: tuple[ExcludedStrategy, ...]
    holdout_requested: bool
    arms: tuple[ArmMeasurement, ...]
    rows: tuple[WrittenRow, ...] = ()
    #: Why the Deflated Sharpe is absent, per ``(namespace, quarantine arm)``
    #: group that produced none. ⚠ Empty means every group deflated; a group
    #: named here wrote rows with ``deflated_sharpe = NULL`` FOR A STATED
    #: REASON, which is the difference between a refusal an operator can act on
    #: and one they cannot.
    deflation_refusals: Mapping[str, str] = field(default_factory=dict)
    trial_register_version: str = TRIAL_REGISTER.version
    declared_trials: int = TRIAL_REGISTER.declared_count

    @property
    def rows_written(self) -> int:
        return len(self.rows)


# ---------------------------------------------------------------------------
# Which strategies can produce a result row (§3)
# ---------------------------------------------------------------------------


def _regime_for(entry: StrategyEntry, calendar: Sequence[date]) -> ExitRegime:
    return entry.exit_regime(entry.decision_calendar(calendar))


def _demonstrate_level_refusal(entry: StrategyEntry, regime: ExitRegime) -> str | None:
    """Call ``build_positions`` on a level-based entry carrying no outcome.

    Returns the refusal message, or ``None`` if it did NOT refuse — which would
    mean §3.2's blocker has gone away and both this job and the spec are stale.

    ⚠ DEMONSTRATED, NOT QUOTED. ``position_builder``'s docstring says a
    level-based entry needs an outcome; a docstring cannot be wrong in CI, and
    the whole exclusion of S-4 rests on this raise.
    """
    if not regime.level_based:
        return None
    when = date(2020, 1, 2)
    later = date(2020, 1, 3)
    rows: tuple[OHLCVRow, ...] = (
        {"open": 10.0, "high": 11.0, "low": 9.0, "close": 10.5, "volume": 1000},  # type: ignore[typeddict-item]
        {"open": 10.5, "high": 11.5, "low": 9.5, "close": 11.0, "volume": 1000},  # type: ignore[typeddict-item]
    )
    try:
        build_positions(
            strategy_id=entry.strategy_id,
            strategy_version="probe",
            entries=[
                EntryFill(
                    signal_id=1,
                    instrument_id=1,
                    signal_bar_date=when,
                    fill_bar_date=later,
                    fill_price=Decimal("10.5"),
                )
            ],
            exits=[],
            outcomes=[],
            outcome_pin=_OUTCOME_PIN,
            series={1: BarSeries(dates=(when, later), rows=rows)},
            regime=regime,
            window=Window(start=when, end=later),
        )
    except ValueError as exc:
        return str(exc)
    return None


def runnable_strategies(
    manifest: Mapping[str, StrategyEntry] = STRATEGY_MANIFEST,
    *,
    calendar: Sequence[date] = _PROBE_CALENDAR,
) -> tuple[tuple[str, ...], tuple[ExcludedStrategy, ...]]:
    """Which manifest entries can produce a stored result TODAY, and why not.

    Derived from the manifest rather than from a hand-written list, so a fifth
    strategy landing is evaluated rather than forgotten — which is the defect
    ``strategy_manifest`` exists to close, and it would reappear one layer up
    here if this function enumerated by hand.

    ⚠ A strategy that is BLOCKED is returned as a named exclusion, never
    dropped. A three-of-four run reporting "3 strategies evaluated" is exactly
    the silent narrowing criterion 9 forbids.

    ⚠ A ``level_based`` entry whose builder did NOT refuse raises. That is the
    one state this function must not paper over: the exclusion is derived from
    the refusal, so a missing refusal means the derivation no longer holds and
    the honest answer is to stop rather than to guess which way it went.
    """
    runnable: list[str] = []
    excluded: list[ExcludedStrategy] = []
    for strategy_id in sorted(manifest):
        entry = manifest[strategy_id]
        regime = _regime_for(entry, calendar)
        if not regime.level_based:
            runnable.append(strategy_id)
            continue
        refusal = _demonstrate_level_refusal(entry, regime)
        if refusal is None:
            raise RuntimeError(
                f"{strategy_id} is level_based but build_positions did NOT refuse an entry with no outcome — "
                "§3.2 excludes it BECAUSE of that refusal, so the exclusion can no longer be derived and the "
                "spec has to be re-measured rather than trusted"
            )
        excluded.append(ExcludedStrategy(strategy_id=strategy_id, reason=refusal))
    return tuple(runnable), tuple(excluded)


# ---------------------------------------------------------------------------
# Corpus → positions, for one (strategy, quarantine arm)
# ---------------------------------------------------------------------------


def _to_series(bars: Sequence[StructureBar]) -> BarSeries:
    """The loader's bars, projected onto the strategies' input shape.

    ⚠ NO EXTRA MASKING HERE, and that is deliberate rather than an omission.
    ``load_masked_series`` already applies ``price_quarantine.rule_b1``'s
    non-positive-open clause under the ``masked`` arm (#2354); re-applying it
    would ALSO mask it under ``admitted``, and criterion 9's arm is defined as
    *"quarantined bars admitted at their stored values"*. A bar reaching the
    fill path with a bad open is refused independently by
    ``signal_ledger.resolve_fills`` as ``unusable_fill_price``, which is what
    makes the admitted arm safe to run at all.
    """
    rows: list[OHLCVRow] = [
        {"open": bar.open, "high": bar.high, "low": bar.low, "close": bar.close, "volume": bar.volume}  # type: ignore[typeddict-item]
        for bar in bars
    ]
    return BarSeries(dates=tuple(bar.bar_date for bar in bars), rows=tuple(rows))


def _fills(rows: Sequence[LedgerRow], instrument_id: int) -> tuple[list[EntryFill], list[ExitFill]]:
    """Project the writer's own output onto the builder's inputs.

    ⚠ ``verdict == "fired"`` alone, exactly as ``outcome_ledger.select_pending_fills``
    filters: a ``not_fired`` or ``not_evaluable`` row has no fill and the builder
    must never see one.
    """
    entries: list[EntryFill] = []
    exits: list[ExitFill] = []
    for index, row in enumerate(rows):
        if row.verdict != "fired":
            continue
        assert row.fill_bar_date is not None and row.fill_price is not None
        if row.signal_kind == "entry":
            entries.append(
                EntryFill(
                    # A stand-in for the BIGSERIAL the ledger would assign.
                    # Nothing is stored by this job, so the id only has to be
                    # unique within this instrument's batch.
                    signal_id=index,
                    instrument_id=instrument_id,
                    signal_bar_date=row.signal_bar_date,
                    fill_bar_date=row.fill_bar_date,
                    fill_price=row.fill_price,
                )
            )
        else:
            exits.append(
                ExitFill(
                    instrument_id=instrument_id,
                    signal_bar_date=row.signal_bar_date,
                    fill_bar_date=row.fill_bar_date,
                    fill_price=row.fill_price,
                )
            )
    return entries, exits


def _mark_index(series: BarSeries, *, window: Window, not_before: date) -> int | None:
    """Where ``position_builder``'s open-position mark was taken (§3.2 rule 5).

    The last bar at or before the window end, at or after the entry fill, that
    carries a close. A leg with no locatable mark cannot be placed on the axis
    and is counted rather than dropped silently.
    """
    for index in range(len(series) - 1, -1, -1):
        when = series.dates[index]
        if when > window.end:
            continue
        if when < not_before:
            return None
        if series.rows[index].get("close") is not None:
            return index
    return None


@dataclass
class _NamespaceBook:
    """One namespace's legs and trades, accumulated on the FULL evaluation axis.

    ⚠ Absolute axis indices, re-based only at report time. The namespace's own
    axis (§5) is the closed span of ITS OWN positions, which is not knowable
    until the corpus pass has finished — so the legs are collected against the
    one axis every instrument shares and shifted once at the end.
    """

    book: LegBook = field(default_factory=LegBook)
    returns: array[float] = field(default_factory=lambda: array("d"))
    entry_dates: list[date] = field(default_factory=list)
    instruments: set[int] = field(default_factory=set)
    positions: int = 0
    open_at_end: int = 0
    excluded: Counter[str] = field(default_factory=Counter)
    first_index: int | None = None
    last_index: int | None = None

    def add_leg(
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
        self.book.add(
            entry_index=entry_index,
            exit_index=exit_index,
            entry_price=entry_price,
            exit_price=exit_price,
            half_spread=half_spread,
            realised=realised,
            marks=marks,
        )
        if self.first_index is None or entry_index < self.first_index:
            self.first_index = entry_index
        if self.last_index is None or exit_index > self.last_index:
            self.last_index = exit_index

    def daily_trade_returns(self) -> dict[date, float]:
        """Mean realised trade return per ENTRY date — this trial's return series."""
        totals: dict[date, list[float]] = {}
        for value, day in zip(self.returns, self.entry_dates, strict=True):
            totals.setdefault(day, []).append(value)
        return {day: sum(values) / len(values) for day, values in totals.items()}


def _shifted(book: LegBook, offset: int) -> LegBook:
    """The same legs, re-based onto an axis starting ``offset`` bars later.

    ⚠ The price, spread, ``realised`` and ``marks`` arrays are SHARED rather
    than copied. They are read-only from here on and the marks array is the
    large one (hundreds of MB on a full-corpus arm); copying it to change two
    integer columns would double the peak for nothing.
    """
    return LegBook(
        entry_index=[index - offset for index in book.entry_index],
        exit_index=[index - offset for index in book.exit_index],
        entry_price=book.entry_price,
        exit_price=book.exit_price,
        half_spread=book.half_spread,
        realised=book.realised,
        mark_offset=book.mark_offset,
        marks=book.marks,
    )


def _benchmark_book(
    *,
    instruments: frozenset[int],
    closes_by_instrument: Mapping[int, tuple[int, array[float]]],
    lo: int,
    hi: int,
) -> LegBook:
    """Criterion 7's buy-and-hold arm, on ONE namespace's truncated axis.

    ⚠⚠ THE BENCHMARK RUNS THROUGH THE SAME ENGINE, and criterion 7's twelfth
    metric is why. *"Return relative to buy-and-hold"* has no published
    definition on an unbalanced panel where instruments list and delist inside
    the window, so it is fixed by construction — and computing it with different
    machinery would attribute the machinery's difference to the strategy. It is
    charged the same cost model: one round trip at the entry band's half-spread.

    ⚠ ONE LEG PER INSTRUMENT THE NAMESPACE ACTUALLY EVALUATED, not per corpus
    series. The row's ``evaluated_instrument_count`` is the namespace's own set
    (§0: the two namespaces differ by 23.6%), and a benchmark over a wider
    population would be a comparison against names this row does not claim to
    have measured.

    ⚠ CLIPPED TO ``[lo, hi]``, which is the namespace's axis and not the
    evaluation window. A leg outside it is dropped; one straddling it opens at
    the first usable close inside and closes at the last. That is the same
    "first usable bar in the window to its last" rule the whole-window benchmark
    applies, with the namespace's axis as the window — the alternative, holding
    the full-window benchmark against a truncated strategy curve, compares two
    different spans.
    """
    book = LegBook()
    one = Decimal(1)
    for instrument_id in sorted(instruments):
        located = closes_by_instrument.get(instrument_id)
        if located is None:  # pragma: no cover - every evaluated instrument was loaded
            continue
        first_axis_index, closes = located
        start = max(lo, first_axis_index)
        end = min(hi, first_axis_index + len(closes) - 1)
        if end <= start:
            continue
        window = np.frombuffer(closes, dtype=np.float64)[start - first_axis_index : end - first_axis_index + 1]
        usable = np.flatnonzero(~np.isnan(window))
        if usable.size < 2:
            continue
        entry_offset = int(usable[0])
        exit_offset = int(usable[-1])
        entry_close = float(window[entry_offset])
        exit_close = float(window[exit_offset])
        if entry_close <= 0.0 or exit_close <= 0.0:
            continue
        half = half_spread_for(Decimal(repr(entry_close)))
        book.add(
            entry_index=start + entry_offset - lo,
            exit_index=start + exit_offset - lo,
            entry_price=float(Decimal(repr(entry_close)) * (one + half)),
            exit_price=float(Decimal(repr(exit_close)) * (one - half)),
            half_spread=float(half),
            realised=True,
            marks=[float(value) for value in window[entry_offset : exit_offset + 1]],
        )
    return book


def _absorb(
    costed: Sequence[CostedPosition],
    *,
    series: BarSeries,
    window: Window,
    axis_pos: Mapping[date, int],
    closes: Sequence[float],
    first_axis_index: int,
    instrument_id: int,
    books: Mapping[ResultNamespace, _NamespaceBook],
    close_sources: Counter[str],
    discarded: Counter[str],
) -> None:
    """Route one instrument's costed positions into their namespace books.

    ⚠ The §5.2 partition is applied HERE, on the positions one corpus sweep
    produced, because ``namespace_for_position`` is a filter and not a second
    evaluation. A namespace absent from ``books`` is COUNTED and discarded —
    which is §4's rule that an in-sample invocation must not so much as compute
    the withheld side's metrics.
    """
    for row in costed:
        position = row.position
        side = namespace_for_position(position.entry_fill_bar_date, position.close_bar_date)
        close_sources[position.close_source or "open_at_window_end"] += 1
        book = books.get(side)
        if book is None:
            discarded[side] += 1
            continue
        book.positions += 1
        book.instruments.add(instrument_id)

        entry_index = axis_pos.get(position.entry_fill_bar_date)
        if entry_index is None:  # pragma: no cover - every fill bar is a corpus bar
            book.excluded["entry_bar_off_axis"] += 1
            continue
        if row.uncosted_reason is not None:
            # §3.4 — excluded and COUNTED, never dropped silently. Neither
            # reachable state carries an exit price, so neither can be placed on
            # the curve.
            book.excluded[row.uncosted_reason] += 1
            continue
        assert row.exit_price_net is not None and row.net_return_pct is not None

        if position.close_bar_date is not None:
            exit_index = axis_pos.get(position.close_bar_date)
            realised = True
        else:
            book.open_at_end += 1
            realised = False
            located = _mark_index(series, window=window, not_before=position.entry_fill_bar_date)
            if located is None:
                book.excluded["mark_bar_unlocatable"] += 1
                continue
            exit_index = axis_pos.get(series.dates[located])
        if exit_index is None or exit_index < entry_index:  # pragma: no cover - builder orders its own closes
            book.excluded["close_bar_off_axis"] += 1
            continue

        span_from = entry_index - first_axis_index
        book.add_leg(
            entry_index=entry_index,
            exit_index=exit_index,
            entry_price=float(row.entry_price_net),
            exit_price=float(row.exit_price_net),
            half_spread=float(row.half_spread),
            realised=realised,
            marks=list(closes[span_from : exit_index - first_axis_index + 1]),
        )
        if realised:
            book.returns.append(float(row.net_return_pct))
            book.entry_dates.append(position.entry_fill_bar_date)


@dataclass(frozen=True)
class _Corpus:
    """The evaluation axis and the series to stream, read once per invocation."""

    universe: tuple[int, ...]
    axis: tuple[date, ...]
    axis_pos: Mapping[date, int]
    pairs: tuple[tuple[int, int], ...]

    @property
    def window(self) -> Window:
        return Window(start=EVALUATION_WINDOW_START, end=EVALUATION_WINDOW_END)


def load_corpus(conn: psycopg.Connection[Any], *, limit: int | None = None) -> _Corpus:
    """The corpus ∩ §4.0 validated-universe slice, and its union calendar.

    ⚠ ``limit`` exists for a smoke run and the caller must say so in its report.
    A limited pass is not a full-population figure and no row written from one
    describes the population its ``evaluated_instrument_count`` claims.
    """
    universe = load_validated_universe(conn)
    bounds = {"ids": list(universe), "start": EVALUATION_WINDOW_START, "end": EVALUATION_WINDOW_END}
    axis = tuple(row[0] for row in conn.execute(_AXIS_SQL, bounds).fetchall())
    pairs = [(int(row[0]), int(row[1])) for row in conn.execute(_SERIES_SQL, {"ids": list(universe)}).fetchall()]
    if limit is not None:
        pairs = pairs[:limit]
    return _Corpus(
        universe=universe,
        axis=axis,
        axis_pos={when: index for index, when in enumerate(axis)},
        pairs=tuple(pairs),
    )


def _measure_namespace(
    namespace: ResultNamespace,
    book: _NamespaceBook,
    *,
    corpus: _Corpus,
    closes_by_instrument: Mapping[int, tuple[int, array[float]]],
) -> NamespaceMeasurement | None:
    """Build this namespace's curve on its own axis and compute criterion 7's set.

    §5's rule, fixed by construction because no published formulation covers it:
    *a namespace's equity axis is the evaluation axis truncated to the closed
    span of that namespace's own positions.* Both ends are MEASURED — the
    in-sample start is that namespace's earliest entry fill and NOT the corpus
    start, because a strategy's warm-up means its first position lands well
    after the first bar and an axis padded back to 1962 would dilute exactly the
    CAGR this rule exists to protect.

    ⚠ THE IN-SAMPLE BOUND IS ASSERTED AGAINST THE FROZEN BOUNDARY. A violation
    means ``namespace_for_position`` mis-classified a position; it never means
    the axis needs widening, so it raises rather than adjusting.
    """
    if book.first_index is None or book.last_index is None:
        return None
    lo, hi = book.first_index, book.last_index
    if hi - lo < 1:
        return None
    if namespace == "in_sample" and corpus.axis[hi] >= HOLDOUT_BOUNDARY:
        raise RuntimeError(
            f"an in-sample position closes {corpus.axis[hi]}, on or after the frozen boundary {HOLDOUT_BOUNDARY} — "
            "namespace_for_position must have mis-classified it, and widening the axis would import a withheld bar "
            "into a training number"
        )

    dates = corpus.axis[lo : hi + 1]
    curve = build_equity_curve(_shifted(book.book, lo), date_count=len(dates))
    instruments = frozenset(book.instruments)
    # ⚠⚠ NOT ``build_equity_curve`` — #2426. The benchmark shares this engine's
    # cost model and fill contract deliberately, but NOT its sizing rule:
    # ``equal_weight_concurrent_v1`` re-imposes equal weight on every event date,
    # and a comparator that rebalances is not buy-and-hold. Measured on the full
    # population, that inheritance added 23.2 points of annual return and turned
    # over 137,477,862x the pot. See ``BENCHMARK_RULE_ID``.
    benchmark = build_buy_and_hold_curve(
        _benchmark_book(
            instruments=instruments,
            closes_by_instrument=closes_by_instrument,
            lo=lo,
            hi=hi,
        ),
        date_count=len(dates),
    )
    metrics = compute_metrics(
        curve,
        dates=dates,
        trades=TradeReturns(
            net_return_pct=tuple(book.returns),
            entry_fill_date=tuple(book.entry_dates),
            open_count=book.open_at_end,
            unpriced_count=sum(book.excluded.values()),
        ),
        buy_and_hold=benchmark,
        bootstrap_seed=BACKTEST_BOOTSTRAP_SEED,
    )
    # ⚠ ACCEPTANCE 7 — every stored row carries a non-null effective sample
    # size. ``compute_metrics`` was given a seed, so a null here means the block
    # bootstrap could not run over this namespace's cluster axis (too few dates
    # carrying a trade), and criterion 3 forbids reporting a nominal *n* in its
    # place. Refused HERE, where the message names the namespace, rather than
    # three layers later as an unexplained refusal-list mismatch.
    if metrics.effective_sample_size is None:
        raise RuntimeError(
            f"the {namespace} namespace produced {metrics.trade_count} realised trade(s) over "
            f"{len(dates)} dates and the block bootstrap computed no effective sample size — criterion 3 forbids "
            "reporting a nominal n in its place, so no row can be written for it"
        )
    return NamespaceMeasurement(
        namespace=namespace,
        metrics=metrics,
        # ⚠ On the TRADE axis, so the moments are commensurable with the block
        # bootstrap's effective sample size — which is the `T` criterion 6's
        # deflation consumes.
        moments=trade_moments(list(book.returns)),
        daily_returns=book.daily_trade_returns(),
        evaluated_instrument_ids=instruments,
        position_count=book.positions,
        axis_first=dates[0],
        axis_last=dates[-1],
    )


def evaluate_arm(
    conn: psycopg.Connection[Any],
    entry: StrategyEntry,
    *,
    corpus: _Corpus,
    quarantine_arm: QuarantineArm,
    identity: StrategyIdentity,
    namespaces: Sequence[ResultNamespace],
) -> ArmMeasurement:
    """One ``(strategy, quarantine arm)`` corpus pass, end to end.

    ⚠ BOTH NAMESPACES COME OUT OF THIS ONE PASS when both are requested (§4);
    the caller decides whether the hold-out side is one of them, and when it is
    not the hold-out positions are counted and thrown away rather than measured.

    ⚠ A ``cross_sectional`` entry costs TWO sub-passes over the corpus and it is
    not an optimisation gap: "hold the top decile" is a statement about the
    cross-section, so no member's verdict at a rebalance date is decidable until
    every member has been staged — and holding 5,266 members' bars resident to
    avoid the second read is the full-corpus materialisation the §3.1 spec
    already refused.
    """
    started = time.monotonic()
    regime = _regime_for(entry, corpus.axis)
    books: dict[ResultNamespace, _NamespaceBook] = {name: _NamespaceBook() for name in namespaces}
    close_sources: Counter[str] = Counter()
    discarded: Counter[str] = Counter()
    closes_by_instrument: dict[int, tuple[int, array[float]]] = {}
    ranking: _CrossSection | None = None

    if entry.strategy_class == "cross_sectional":
        ranking = _rank_cross_section(conn, entry, corpus=corpus, quarantine_arm=quarantine_arm)

    evaluated = 0
    for instrument_id, series_id in corpus.pairs:
        masked = load_masked_series(conn, series_id, arm=quarantine_arm)
        if not masked.bars:
            continue
        series = _to_series(masked.bars)
        indices = [corpus.axis_pos[when] for when in series.dates if when in corpus.axis_pos]
        if len(indices) < 2:
            continue
        evaluated += 1
        first_axis_index, last_axis_index = indices[0], indices[-1]
        # ⚠ One dense close array per INSTRUMENT, spanning its own first to last
        # axis index with `nan` in between. That is what makes a leg's mark slice
        # O(1) to cut, and it is ~25M floats over the corpus rather than the 85M
        # a full dense panel would need.
        closes = [float("nan")] * (last_axis_index - first_axis_index + 1)
        for when, row in zip(series.dates, series.rows, strict=True):
            slot = corpus.axis_pos.get(when)
            close = row.get("close")
            if slot is not None and close is not None:
                closes[slot - first_axis_index] = float(close)
        closes_by_instrument[instrument_id] = (first_axis_index, array("d", closes))

        signals = _signals_for(entry, series, instrument_id=instrument_id, ranking=ranking)
        rows = resolve_fills(signals, series=series, identity=identity, instrument_id=instrument_id)
        entries, exits = _fills(rows, instrument_id)
        built = build_positions(
            strategy_id=entry.strategy_id,
            strategy_version=identity.version,
            entries=entries,
            exits=exits,
            outcomes=[],
            outcome_pin=None,
            series={instrument_id: series},
            regime=regime,
            window=corpus.window,
        )
        _absorb(
            list(cost_positions(built.positions)),
            series=series,
            window=corpus.window,
            axis_pos=corpus.axis_pos,
            closes=closes,
            first_axis_index=first_axis_index,
            instrument_id=instrument_id,
            books=books,
            close_sources=close_sources,
            discarded=discarded,
        )

    measured: dict[ResultNamespace, NamespaceMeasurement] = {}
    for name in namespaces:
        outcome = _measure_namespace(
            name,
            books[name],
            corpus=corpus,
            closes_by_instrument=closes_by_instrument,
        )
        if outcome is not None:
            measured[name] = outcome
    return ArmMeasurement(
        strategy_id=entry.strategy_id,
        strategy_version=identity.version,
        quarantine_arm=quarantine_arm,
        namespaces=measured,
        holdout_positions_discarded=discarded.get("hold_out", 0),
        close_sources=dict(close_sources),
        series_evaluated=evaluated,
        elapsed_s=time.monotonic() - started,
    )


@dataclass(frozen=True)
class _CrossSection:
    """Sub-pass A's output: the panel calendar and every date's ranking verdict.

    ⚠⚠ ``decision_dates`` IS CARRIED, NOT RE-DERIVED FROM ``winners | thin``, and
    the difference is a real verdict change. A decision date at which NO member
    was evaluable appears in neither map, so re-deriving the calendar from them
    would hand sub-pass B a set that no longer contains it — and
    ``stage_cross_sectional_member`` would then return ``not_fired`` for that bar
    (*"a non-decision bar did not fire"*) where sub-pass A returned
    ``not_evaluable``. Two sub-passes of one evaluation must see one calendar.
    """

    decision_dates: frozenset[date]
    winners: Mapping[date, frozenset[int]]
    thin: frozenset[date]


def _signals_for(
    entry: StrategyEntry,
    series: BarSeries,
    *,
    instrument_id: int,
    ranking: _CrossSection | None,
) -> list[StrategySignal]:
    """One instrument's whole-series verdicts, per-series or cross-sectional."""
    if entry.signals is not None:
        return entry.signals(series, universe=BACKTEST_UNIVERSE, masked_reason="quarantined_bar")
    assert entry.member is not None and ranking is not None
    staged = stage_cross_sectional_member(
        entry.member(
            series,
            panel_decision_dates=ranking.decision_dates,
            universe=BACKTEST_UNIVERSE,
            masked_reason="quarantined_bar",
        )
    )
    signals: list[StrategySignal] = []
    for index, verdict in enumerate(staged.verdicts):
        if verdict is not None:
            signals.append(verdict)
            continue
        when = series.dates[index]
        if when in ranking.thin:
            # ⚠ ``min_participants`` is the RUNNER's call, mirroring
            # ``evaluate_cross_sectional``: an empty return from ``select``
            # cannot be told apart from "the panel was too thin", and criterion
            # 8 exists to keep that distinction countable.
            signals.append(
                StrategySignal(verdict="not_evaluable", signal_index=index, kind="entry", reason="thin_cross_section")
            )
            continue
        fired = instrument_id in ranking.winners.get(when, frozenset())
        signals.append(StrategySignal(verdict="fired" if fired else "not_fired", signal_index=index, kind="entry"))
    return signals


def _rank_cross_section(
    conn: psycopg.Connection[Any],
    entry: StrategyEntry,
    *,
    corpus: _Corpus,
    quarantine_arm: QuarantineArm,
) -> _CrossSection:
    """Sub-pass A: stage every member and rank each decision date's cross-section.

    ⚠ ONLY THE SCORES ARE KEPT. ``stage_cross_sectional_member`` is public for
    exactly this — *"a full-corpus census cannot hold every member's bars in
    memory at once, so it stages one series at a time and keeps only
    ``scores``"* — and the per-bar verdicts are recomputed in sub-pass B from
    the same function rather than retained, which is ~25M objects avoided.

    ⚠ The panel calendar is the corpus's UNION calendar, not the frontier-
    eligible subset: a name that stopped trading in 1998 still contributed the
    sessions on which the panel rebalanced.
    """
    assert entry.member is not None and entry.select is not None and entry.min_participants is not None
    decision_dates = entry.decision_calendar(corpus.axis)
    if decision_dates is None:  # pragma: no cover - the manifest guarantees one
        raise RuntimeError(f"{entry.strategy_id} is cross_sectional but returned no decision calendar")

    scores: dict[date, dict[int, float]] = {}
    for instrument_id, series_id in corpus.pairs:
        masked = load_masked_series(conn, series_id, arm=quarantine_arm)
        if not masked.bars:
            continue
        series = _to_series(masked.bars)
        if len(series) < 2:
            continue
        staged = stage_cross_sectional_member(
            entry.member(
                series,
                panel_decision_dates=decision_dates,
                universe=BACKTEST_UNIVERSE,
                masked_reason="quarantined_bar",
            )
        )
        for when, value in staged.scores.items():
            scores.setdefault(when, {})[instrument_id] = value

    winners: dict[date, frozenset[int]] = {}
    thin: set[date] = set()
    for when in sorted(scores):
        at_date = scores[when]
        if len(at_date) < entry.min_participants:
            thin.add(when)
            continue
        selected = frozenset(entry.select(when, at_date))
        unknown = selected - at_date.keys()
        if unknown:
            raise ValueError(
                f"{entry.strategy_id} select returned {sorted(unknown)} on {when}, which did not participate in "
                "that cross-section — every winner must be one of the members offered"
            )
        winners[when] = selected
    return _CrossSection(decision_dates=decision_dates, winners=winners, thin=frozenset(thin))


# ---------------------------------------------------------------------------
# Criterion 6 — the Deflated Sharpe, across the measured trials (§2)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _Deflation:
    """One ``(namespace, quarantine arm)`` group's shared deflation inputs."""

    variance: float
    correlation: float
    measured_trials: int


def deflate_group(
    measurements: Mapping[str, NamespaceMeasurement],
) -> tuple[_Deflation | None, str | None]:
    """``V[SR_n]`` and the inter-trial correlation over one group of trials.

    ⚠⚠ THE TRIALS ARE THE STRATEGIES, MEASURED UNDER THE SAME CONDITIONS.
    Grouping is per ``(namespace, quarantine arm)`` because a Sharpe measured
    on the withheld side under admitted bars is not a draw from the same search
    as one measured in-sample under masked bars, and equation (1)'s expected
    maximum is over draws from ONE distribution.

    ⚠ Equation (8)'s correlation is measured off the trials' realised
    per-entry-date return series on the dates they BOTH traded. The dates are
    INTERSECTED, never unioned: a trial that did not trade on a date carries no
    pairwise information, and padding it with a zero would manufacture agreement.

    Returns ``(None, reason)`` in every state where the deflation does not
    exist. Each is real and each leaves ``deflated_sharpe`` NULL with the gate
    refusing on ``deflated_sharpe_not_computed`` — a stated refusal rather than
    a silent one.
    """
    usable = {
        strategy_id: outcome
        for strategy_id, outcome in measurements.items()
        if outcome.moments is not None and outcome.metrics.effective_sample_size is not None
    }
    if len(usable) < MIN_MEASURED_TRIALS:
        return None, (
            f"{len(usable)} measured trial(s) against MIN_MEASURED_TRIALS={MIN_MEASURED_TRIALS} — V[SR_n] does not "
            "exist below it, so no amount of re-running this set produces a Deflated Sharpe"
        )
    # ⚠ ``strategy_id`` IS the register's trial id, checked rather than mapped.
    # ``TrialRegister.sharpe_variance`` raises on an undeclared key, and a
    # measured trial the register does not declare under-counts the search and
    # RAISES the DSR — the favourable direction, which is why it must fail here.
    undeclared = sorted(set(usable) - TRIAL_REGISTER.trial_ids)
    if undeclared:
        return None, (
            f"manifest strategies {undeclared} are not declared trials in {TRIAL_REGISTER.version} — their Sharpes "
            "would be measured but uncounted in M, which under-counts the search and raises the DSR"
        )
    sharpes = {strategy_id: outcome.moments.sharpe for strategy_id, outcome in usable.items()}  # type: ignore[union-attr]
    variance = TRIAL_REGISTER.sharpe_variance(sharpes)
    if variance is None or variance <= 0.0 or not math.isfinite(variance):
        return None, "V[SR_n] is zero or undefined over the measured trials"

    labels = sorted(usable)
    series = {label: usable[label].daily_returns for label in labels}
    common = sorted(set.intersection(*(set(series[label]) for label in labels)))
    if len(common) < 2:
        return None, "the trials share fewer than 2 active dates, so no correlation exists"
    panel = np.array([[series[label][day] for day in common] for label in labels])
    # ⚠⚠ THE DEGENERATE TRIAL IS DETECTED ON THE INPUT, NOT ON THE MATRIX, AND
    # THE VERSION THAT CHECKED THE MATRIX IS DEAD CODE ON THIS NUMPY.
    # ``verify_2240_statistics.py``'s P11 comment says *"np.corrcoef returns NaN
    # for a CONSTANT series"* — measured on **numpy 2.4.4** (2026-08-08),
    # ``np.corrcoef([[0.1, 0.1, 0.1], [0.1, -0.2, 0.3]])`` returns
    # ``[[1., 0.], [0., 1.]]``: a finite **0.0**, not NaN. So an
    # ``isfinite`` guard never fires and a trial carrying no pairwise
    # information is silently read as UNCORRELATED — which pushes ``N_hat``
    # toward ``M`` rather than refusing. A zero-variance row is a trial with no
    # correlation to measure, and that is what is tested.
    #
    # ⚠ The ``isfinite`` check is kept BELOW as a backstop, not as the mechanism:
    # a future numpy could go back to NaN, and either way a non-finite
    # correlation must not reach ``average_trial_correlation``, which raises on
    # its range check rather than reporting.
    # ⚠ ``ptp`` (max - min) AND NOT ``std == 0.0``. The first draft tested the
    # standard deviation and did not fire on ``[0.1, 0.1, 0.1]``: the mean of
    # three binary 0.1s is 0.10000000000000002, so the deviations are ~1e-17 and
    # the std is a denormal rather than a zero. "All values equal" is the
    # property that actually means "no variation", it is exact in floating point,
    # and it invents no tolerance.
    degenerate = sorted(label for index, label in enumerate(labels) if float(np.ptp(panel[index])) == 0.0)
    if degenerate:
        return None, (
            f"trials {degenerate} have a constant return series over the {len(common)} shared dates, so there is no "
            "correlation to measure — reading them as uncorrelated would push the implied independent trial count "
            "toward M on evidence that does not exist"
        )
    matrix = np.corrcoef(panel)
    if not np.all(np.isfinite(matrix)):  # pragma: no cover - the zero-variance check above is the live guard
        return None, "the correlation matrix is not finite, so no average correlation exists"
    rho = average_trial_correlation(matrix)
    # ⚠ Appendix A.3 bounds rho at ``-1/(M-1)`` for a positive-definite matrix
    # and ``implied_independent_trials`` RAISES outside it. Reported as a refusal
    # rather than allowed to crash the invocation after the corpus passes.
    floor = -1.0 / (TRIAL_REGISTER.declared_count - 1)
    if not floor < rho <= 1.0:
        return None, (
            f"measured correlation {rho} is outside A.3's ({floor}, 1] bound for M = "
            f"{TRIAL_REGISTER.declared_count} — the matrix it came from is not positive-definite"
        )
    independent = implied_independent_trials(rho, TRIAL_REGISTER.declared_count)
    if not 1.0 < independent <= TRIAL_REGISTER.declared_count:
        return None, (
            f"implied independent trials {independent} is outside (1, {TRIAL_REGISTER.declared_count}] — A.3's "
            "interpolation is undefined there and clamping it would invent a treatment the paper does not give"
        )
    return _Deflation(variance=variance, correlation=rho, measured_trials=len(sharpes)), None


def _deflated_for(outcome: NamespaceMeasurement, deflation: _Deflation | None) -> DeflatedSharpeResult | None:
    if deflation is None or outcome.moments is None or outcome.metrics.effective_sample_size is None:
        return None
    return deflated_sharpe(
        outcome.moments,
        effective_sample_size=outcome.metrics.effective_sample_size,
        trial_sharpe_variance=deflation.variance,
        declared_trials=TRIAL_REGISTER.declared_count,
        average_correlation=deflation.correlation,
        measured_trials=deflation.measured_trials,
        trial_register_version=TRIAL_REGISTER.version,
    )


# ---------------------------------------------------------------------------
# Assembling and storing the rows
# ---------------------------------------------------------------------------


def build_result(
    outcome: NamespaceMeasurement,
    *,
    strategy_id: str,
    strategy_version: str,
    ambiguity_arm: AmbiguityArm,
    quarantine_arm: QuarantineArm,
    deflated: DeflatedSharpeResult | None,
) -> StrategyResult:
    """One ``strategy_results`` row. §7's fourteen identity members, all pinned.

    ⚠ THIRTEEN OF THE FOURTEEN ARE READ FROM A MODULE THAT FROZE THEM, and that
    is the design working. The only operator-facing degree of freedom is whether
    the hold-out arm runs at all; a job that let an operator pass a sizing rule
    or a window would be minting result identities no code path can reproduce.

    ⚠⚠ ``input_rule_set_version`` IS THE QUARANTINE RULE SET, and it is NOT
    ``StrategyIdentity.input_rule_set_versions`` (which is indicator-only and is
    already inside ``strategy_version``, so folding it in here would hash it
    twice). ``sql/262``'s own comment names its purpose: *re-run the quarantine
    under a changed rule set and the same signal resolves differently with the
    resolver byte-identical.*

    ⚠ ``outcome_rule_set_version`` is stamped even though no runnable strategy
    produces an outcome. It is a member of the hash and ``sql/262`` declares it
    ``NOT NULL`` with a non-empty CHECK, so the resolver's LIVE version is read
    rather than a placeholder written — a blank would pass ``NOT NULL`` and
    silently merge two results (the #2286 shape).
    """
    return StrategyResult(
        identity=ResultIdentity(
            strategy_id=strategy_id,
            strategy_version=strategy_version,
            result_scope=RESULT_SCOPE,
            namespace=outcome.namespace,
            ambiguity_arm=ambiguity_arm,
            quarantine_arm=quarantine_arm,
            sizing_rule=SIZING_RULE_ID,
            benchmark_rule=BENCHMARK_RULE_ID,
            cost_model_id=COST_MODEL_ID,
            corpus_version=CORPUS_VERSION,
            window_start=EVALUATION_WINDOW_START,
            window_end=EVALUATION_WINDOW_END,
            position_rule_set_version=POSITION_RULE_SET_VERSION,
            outcome_rule_set_version=OUTCOME_RULE_SET_VERSION,
            input_rule_set_version=QUARANTINE_RULE_SET_VERSION,
        ),
        metrics=outcome.metrics,
        universe_basis=BACKTEST_UNIVERSE,
        # ⚠ ``CARRY_UNMODELLED`` AS AT COMPUTE TIME, stamped per row. When carry
        # is finally measured every row computed before that measurement must
        # STAY unpromotable, which a gate reading today's module constant would
        # silently undo.
        carry_unmodelled=CARRY_UNMODELLED,
        evaluated_instrument_count=len(outcome.evaluated_instrument_ids),
        trial_count=None if deflated is None else deflated.declared_trials,
        deflated_sharpe=None if deflated is None else Decimal(repr(deflated.deflated_sharpe)),
        deflated=deflated,
    )


def _candidate(
    result: StrategyResult,
    *,
    validated: frozenset[int],
    evaluated: frozenset[int],
    holdout_evaluations: int,
    recorded_accesses: int,
) -> PromotionCandidate:
    """The gate's inputs, supplied DIRECTLY rather than read back.

    ⚠⚠ COMPUTING THE REFUSAL LIST MUST NOT TOUCH THE HOLD-OUT ACCESS LOG.
    ``result_ledger.quarantine_arms_compared`` records a ``read`` access on a
    ``hold_out`` identity — deliberately, because *looking is the event criterion
    5 governs* — so a job that gated every hold-out row it had just written
    would add one read record per row and turn the audit trail into a count of
    its own automation. This job wrote both arms in the same transaction, so it
    knows, and it must not ask the database a question it is the answer to.

    ⚠ ``ambiguity_material`` is ``False`` for the same reason and is not an
    assumption: the two ambiguity arms of a runnable strategy are ONE
    measurement (§6 — ``ambiguous`` is unreachable without a ``level_based``
    regime, asserted by ``runnable_strategies`` and corroborated by the
    close-source census), so their Sharpe gap is exactly zero and cannot exceed
    any non-negative threshold §3.4 could set.
    """
    return PromotionCandidate(
        result=result,
        evaluated_instrument_ids=evaluated,
        validated_universe_ids=validated,
        holdout_evaluations=holdout_evaluations,
        recorded_accesses=recorded_accesses,
        ambiguity_material=False,
        quarantine_arms_compared=True,
    )


def _expected_refusals(*, holdout_requested: bool, deflated: bool) -> frozenset[PromotionRefusal]:
    """What §9's table says a row from THIS invocation must still refuse on."""
    expected = set(STANDING_REFUSALS)
    if not holdout_requested:
        expected.add("holdout_never_evaluated")
    if not deflated:
        expected.update({"deflated_sharpe_not_computed", "trial_count_undeclared"})
    return frozenset(expected)


def assert_no_existing_results(conn: psycopg.Connection[Any], identities: Sequence[ResultIdentity]) -> None:
    """Refuse a colliding ``result_version`` BEFORE the corpus is touched (§10).

    ⚠ The identity is fully determined by module constants and the namespace
    (§7), so every version this invocation intends to write is knowable up
    front. Discovering the collision at INSERT time would throw away the whole
    multi-pass sweep, and the operator's remedy — delete the row deliberately —
    is the same either way.

    ⚠ NO ``ON CONFLICT`` EXISTS AND NONE IS TO BE ADDED. ``DO NOTHING`` would
    hide corpus drift behind a silent no-op, which is the state a stored
    backtest result must never be able to reach.
    """
    versions = [identity.version for identity in identities]
    if len(set(versions)) != len(versions):
        raise RuntimeError(
            "the invocation plans two rows with the same result_version — the identity does not separate the arms "
            "it is supposed to, and the second write would collide on strategy_results_unique"
        )
    rows = conn.execute(_EXISTING_RESULT_VERSIONS, {"result_versions": versions}).fetchall()
    existing = {str(row[0]) for row in rows}
    if existing:
        raise RuntimeError(
            f"{len(existing)} of the {len(versions)} result_version(s) this run would write already exist "
            f"({sorted(existing)[:3]}…) — an unchanged configuration re-run raises rather than overwriting, because "
            "a silently replaced backtest result is a track record nobody can audit"
        )


# ---------------------------------------------------------------------------
# The run
# ---------------------------------------------------------------------------


def run_backtest(
    conn: psycopg.Connection[Any],
    *,
    strategy_id: str | None = None,
    holdout_purpose: str | None = None,
    holdout_accessed_by: str | None = None,
    trial_register_version: str | None = None,
    limit: int | None = None,
    manifest: Mapping[str, StrategyEntry] = STRATEGY_MANIFEST,
) -> BacktestRunReport:
    """Evaluate every runnable strategy and persist criterion 9's arm pairs.

    ⚠⚠ THE TRANSACTION BOUNDARY IS NOT PER STRATEGY, AND THAT INVERTS §3.1's
    "one strategy's failure does not stop the others". §2 puts the Deflated
    Sharpe across the whole strategy set, so ``V[SR_n]`` is a function of WHICH
    trials were measured: a run that loses S-2 during evaluation and proceeds
    would deflate S-1 and S-3 against a two-trial variance while reporting
    itself complete — a MORE CONFIDENT number obtained by losing evidence. A
    strategy failing in the evaluation phase aborts the invocation, and nothing
    has been written at that point because the phases are ordered evaluate →
    deflate → write for exactly this reason.

    ⚠ Concurrency is NOT handled here. The caller holds ``app/jobs/locks.py``'s
    session-scoped ``JobLock`` on the lock manager's own connection — an
    ``pg_advisory_xact_lock`` taken inside this function's write phase would be
    absorbed by the parent transaction and held until the last arm pair
    committed (prevention log).
    """
    if trial_register_version is not None and trial_register_version != TRIAL_REGISTER.version:
        raise ValueError(
            f"invocation asserts trial register {trial_register_version!r} but the live register is "
            f"{TRIAL_REGISTER.version!r} — deflating against a register that has moved would put an M on the row "
            "that does not describe the search"
        )
    holdout_requested = _check_holdout_pairing(purpose=holdout_purpose, accessed_by=holdout_accessed_by)
    namespaces: tuple[ResultNamespace, ...] = ("in_sample", "hold_out") if holdout_requested else ("in_sample",)

    runnable, excluded = runnable_strategies(manifest)
    if strategy_id is not None:
        if strategy_id not in runnable:
            raise ValueError(f"{strategy_id!r} is not a runnable manifest strategy; runnable today: {list(runnable)}")
        runnable = (strategy_id,)
    if not runnable:
        raise RuntimeError("no manifest strategy is runnable — every entry is blocked, so there is nothing to store")

    corpus = load_corpus(conn, limit=limit)
    identities = {
        entry_id: manifest[entry_id].identity(universe=BACKTEST_UNIVERSE, cost_model_id=COST_MODEL_ID)
        for entry_id in runnable
    }
    planned = [
        ResultIdentity(
            strategy_id=entry_id,
            strategy_version=identities[entry_id].version,
            result_scope=RESULT_SCOPE,
            namespace=namespace,
            ambiguity_arm=ambiguity,
            quarantine_arm=quarantine,
            sizing_rule=SIZING_RULE_ID,
            benchmark_rule=BENCHMARK_RULE_ID,
            cost_model_id=COST_MODEL_ID,
            corpus_version=CORPUS_VERSION,
            window_start=EVALUATION_WINDOW_START,
            window_end=EVALUATION_WINDOW_END,
            position_rule_set_version=POSITION_RULE_SET_VERSION,
            outcome_rule_set_version=OUTCOME_RULE_SET_VERSION,
            input_rule_set_version=QUARANTINE_RULE_SET_VERSION,
        )
        for entry_id in runnable
        for namespace in namespaces
        for ambiguity in AMBIGUITY_ARM_ORDER
        for quarantine in QUARANTINE_ARM_ORDER
    ]
    assert_no_existing_results(conn, planned)

    logger.info(
        "strategy_backtest_run: %d runnable strategy(ies) %s x %d quarantine arm(s) over %d series, "
        "namespaces %s, %d rows planned",
        len(runnable),
        list(runnable),
        len(QUARANTINE_ARMS),
        len(corpus.pairs),
        list(namespaces),
        len(planned),
    )
    for entry in excluded:
        logger.info("strategy_backtest_run: EXCLUDED %s — %s", entry.strategy_id, entry.reason)

    # 1. Evaluate every strategy x quarantine arm, holding metrics in memory.
    arms: list[ArmMeasurement] = []
    for entry_id in runnable:
        for quarantine in QUARANTINE_ARM_ORDER:
            measurement = evaluate_arm(
                conn,
                manifest[entry_id],
                corpus=corpus,
                quarantine_arm=quarantine,
                identity=identities[entry_id],
                namespaces=namespaces,
            )
            _assert_ambiguity_unreachable(measurement)
            arms.append(measurement)
            logger.info(
                "strategy_backtest_run: %s/%s evaluated %d series in %.1fs — %s, hold-out discarded %d",
                entry_id,
                quarantine,
                measurement.series_evaluated,
                measurement.elapsed_s,
                {name: outcome.position_count for name, outcome in measurement.namespaces.items()},
                measurement.holdout_positions_discarded,
            )

    # 2. Deflate once per (namespace, quarantine arm) group.
    by_group: dict[tuple[ResultNamespace, QuarantineArm], dict[str, NamespaceMeasurement]] = {}
    for measurement in arms:
        for name, outcome in measurement.namespaces.items():
            by_group.setdefault((name, measurement.quarantine_arm), {})[measurement.strategy_id] = outcome
    deflations: dict[tuple[ResultNamespace, QuarantineArm], _Deflation | None] = {}
    refusals: dict[str, str] = {}
    for group, measurements in by_group.items():
        deflation, reason = deflate_group(measurements)
        deflations[group] = deflation
        if reason is not None:
            refusals[f"{group[0]}/{group[1]}"] = reason
            logger.warning("strategy_backtest_run: no Deflated Sharpe for %s/%s — %s", group[0], group[1], reason)

    # 3. Write, one transaction per arm pair.
    validated = frozenset(corpus.universe)
    written = _write_rows(
        conn,
        arms=arms,
        deflations=deflations,
        validated=validated,
        holdout_requested=holdout_requested,
        holdout_purpose=holdout_purpose,
        holdout_accessed_by=holdout_accessed_by,
    )
    report = BacktestRunReport(
        runnable=runnable,
        excluded=excluded,
        holdout_requested=holdout_requested,
        arms=tuple(arms),
        rows=written,
        deflation_refusals=refusals,
    )
    _assert_every_runnable_produced_rows(report, namespaces=namespaces)
    log_report(report)
    return report


def _check_holdout_pairing(*, purpose: str | None, accessed_by: str | None) -> bool:
    """Exactly one of "neither supplied" and "both supplied, both non-empty".

    ⚠ "Required together" IS NOT EXPRESSIBLE IN ``ParamMetadata`` — it declares
    per-key type and requiredness and has no conditional model — so the pairing
    is checked HERE, first thing, before any corpus work. Leaving it to the
    metadata layer would let a hold-out run start with a blank ``purpose``,
    which is the #2286 shape: a present-but-empty field passing a presence check.
    """
    # ⚠ ``.strip()`` AND NOT BARE TRUTHINESS. ``"   "`` is truthy, so a
    # whitespace-only purpose would pass a presence check and land in
    # ``strategy_holdout_accesses.purpose`` as an audit record that records
    # nothing — the #2286 shape exactly. ``sql/264``'s CHECK and ``HoldoutAccess``
    # both refuse an empty string, so the blank would fail at the WRITE, after
    # the corpus passes; caught here it costs nothing. Found by a table test.
    supplied = [
        name
        for name, value in (("holdout_purpose", purpose), ("holdout_accessed_by", accessed_by))
        if value is not None and value.strip()
    ]
    if not supplied:
        return False
    if len(supplied) != 2:
        raise ValueError(
            f"a hold-out invocation needs holdout_purpose AND holdout_accessed_by, both non-empty; got {supplied} — "
            "criterion 5's log is an audit and a record with no intent attached answers 'how many times' and never "
            "'should that have happened'"
        )
    return True


def _assert_ambiguity_unreachable(measurement: ArmMeasurement) -> None:
    """§6 — a runnable strategy cannot close ``ambiguous``, and this measures it.

    ``position_builder`` assigns ``close_source == "ambiguous"`` only inside its
    ``if regime.level_based:`` branch and ``runnable_strategies`` has already
    refused every level-based entry, so the census is the corroboration on real
    data. If it ever moves, the two ambiguity arms are NOT one measurement and
    writing the second as a relabelled copy of the first would be a fabrication.
    """
    count = measurement.close_sources.get("ambiguous", 0)
    if count:
        raise RuntimeError(
            f"{measurement.strategy_id}/{measurement.quarantine_arm} closed {count} position(s) 'ambiguous' on a "
            "non-level regime — §6's claim that the two ambiguity arms are one measurement is falsified, and the "
            "second arm may no longer be written as the first under another label"
        )


def _write_rows(
    conn: psycopg.Connection[Any],
    *,
    arms: Sequence[ArmMeasurement],
    deflations: Mapping[tuple[ResultNamespace, QuarantineArm], _Deflation | None],
    validated: frozenset[int],
    holdout_requested: bool,
    holdout_purpose: str | None,
    holdout_accessed_by: str | None,
) -> tuple[WrittenRow, ...]:
    """Criterion 9's arm pairs, ``masked`` and ``admitted`` in one transaction each.

    ⚠ THE PAIR WRITER IS THE POINT. A lone ``admitted`` row is a number nobody
    may quote (``sql/267``) and a lone ``masked`` row is the state the gate
    refuses as ``quarantine_arms_not_compared``; ``store_*_arm_pair`` makes the
    half-written state unreachable rather than merely discouraged.

    ⚠ THE PRE-FLIGHT GATE RUNS BEFORE THE FIRST INSERT. Criterion 8 requires the
    refusal list re-measured on every written row and equal to §9's, and the
    projected hold-out counts are knowable before the write — so a deviation
    refuses the whole invocation instead of leaving rows behind that nobody
    predicted.
    """
    by_strategy: dict[str, dict[QuarantineArm, ArmMeasurement]] = {}
    for measurement in arms:
        by_strategy.setdefault(measurement.strategy_id, {})[measurement.quarantine_arm] = measurement

    pending: list[tuple[str, ResultNamespace, AmbiguityArm, StrategyResult, StrategyResult]] = []
    for strategy_id in sorted(by_strategy):
        by_arm = by_strategy[strategy_id]
        masked_arm, admitted_arm = by_arm.get("masked"), by_arm.get("admitted")
        if masked_arm is None or admitted_arm is None:  # pragma: no cover - both arms are always evaluated
            raise RuntimeError(f"{strategy_id} produced {sorted(by_arm)} rather than both quarantine arms")
        for namespace in sorted(set(masked_arm.namespaces) & set(admitted_arm.namespaces)):
            for ambiguity in AMBIGUITY_ARM_ORDER:
                masked = build_result(
                    masked_arm.namespaces[namespace],
                    strategy_id=strategy_id,
                    strategy_version=masked_arm.strategy_version,
                    ambiguity_arm=ambiguity,
                    quarantine_arm="masked",
                    deflated=_deflated_for(masked_arm.namespaces[namespace], deflations.get((namespace, "masked"))),
                )
                admitted = build_result(
                    admitted_arm.namespaces[namespace],
                    strategy_id=strategy_id,
                    strategy_version=admitted_arm.strategy_version,
                    ambiguity_arm=ambiguity,
                    quarantine_arm="admitted",
                    deflated=_deflated_for(admitted_arm.namespaces[namespace], deflations.get((namespace, "admitted"))),
                )
                pending.append((strategy_id, namespace, ambiguity, masked, admitted))

    _preflight_gate(pending, validated=validated, holdout_requested=holdout_requested)

    stored: list[tuple[int, StrategyResult]] = []
    for _strategy_id, namespace, _ambiguity, masked, admitted in pending:
        if namespace == "hold_out":
            assert holdout_purpose is not None and holdout_accessed_by is not None
            ids = store_holdout_arm_pair(
                conn,
                masked,
                admitted,
                accessed_by=holdout_accessed_by,
                purpose=holdout_purpose,
            )
        else:
            ids = store_in_sample_arm_pair(conn, masked, admitted)
        stored.extend(zip(ids, (masked, admitted), strict=True))

    # Criterion 8 — RE-MEASURED on every written row, with the hold-out counts
    # read back from the database rather than projected. ⚠ The counts are per
    # ``(strategy_id, strategy_version)`` and every row of one strategy reads the
    # same pair, so they are cached rather than re-queried 8 times per strategy.
    written: list[WrittenRow] = []
    counts_cache: dict[tuple[str, str], tuple[int, int]] = {}
    for result_id, result in stored:
        identity = result.identity
        key = (identity.strategy_id, identity.strategy_version)
        if key not in counts_cache:
            counts = holdout_access_counts(conn, *key)
            counts_cache[key] = (counts.holdout_evaluations, counts.recorded_accesses)
        evaluations, accesses = counts_cache[key]
        outcome = check_promotable(
            _candidate(
                result,
                validated=validated,
                evaluated=_evaluated_ids(arms, result),
                holdout_evaluations=evaluations,
                recorded_accesses=accesses,
            )
        )
        expected = _expected_refusals(holdout_requested=holdout_requested, deflated=result.deflated is not None)
        if set(outcome) != expected:
            raise RuntimeError(
                f"{identity.strategy_id} {identity.namespace}/{identity.quarantine_arm} stored with refusals "
                f"{sorted(outcome)} against the expected {sorted(expected)} — §9's table and the gate disagree, and "
                "a row whose refusal list nobody predicted is not auditable"
            )
        written.append(
            WrittenRow(
                strategy_id=identity.strategy_id,
                result_version=identity.version,
                namespace=identity.namespace,
                ambiguity_arm=identity.ambiguity_arm,
                quarantine_arm=identity.quarantine_arm,
                result_id=result_id,
                evaluated_instrument_count=result.evaluated_instrument_count,
                refusals=outcome,
            )
        )
    return tuple(written)


def _evaluated_ids(arms: Sequence[ArmMeasurement], result: StrategyResult) -> frozenset[int]:
    for measurement in arms:
        if (
            measurement.strategy_id == result.identity.strategy_id
            and measurement.quarantine_arm == result.identity.quarantine_arm
        ):
            outcome = measurement.namespaces.get(result.identity.namespace)
            if outcome is not None:
                return outcome.evaluated_instrument_ids
    raise RuntimeError(  # pragma: no cover - every stored row came from a measurement
        f"no measurement matches the stored row {result.identity.version}"
    )


def _preflight_gate(
    pending: Sequence[tuple[str, ResultNamespace, AmbiguityArm, StrategyResult, StrategyResult]],
    *,
    validated: frozenset[int],
    holdout_requested: bool,
) -> None:
    """The refusal list every row WILL carry, checked before anything is stored.

    The hold-out counts are projected: one ``evaluate`` record per hold-out row
    (``store_holdout_arm_pair`` writes one per arm, and ``sql/264``'s trigger
    refuses a row without one), so the two counts are equal by construction and
    the gate's ``recorded_accesses < holdout_evaluations`` clause cannot fire on
    a row this job wrote.
    """
    projected: Counter[tuple[str, str]] = Counter()
    for _strategy_id, namespace, _ambiguity, masked, admitted in pending:
        if namespace == "hold_out":
            for result in (masked, admitted):
                projected[(result.identity.strategy_id, result.identity.strategy_version)] += 1
    for _strategy_id, _namespace, _ambiguity, masked, admitted in pending:
        for result in (masked, admitted):
            count = projected[(result.identity.strategy_id, result.identity.strategy_version)]
            outcome = check_promotable(
                _candidate(
                    result,
                    validated=validated,
                    # ⚠ The row's OWN evaluated set is not needed to predict the
                    # refusal list — only whether it is empty and inside the
                    # validated universe — and both hold for any namespace this
                    # job measured. A non-empty subset is supplied so the two
                    # universe clauses are exercised rather than skipped.
                    evaluated=frozenset({next(iter(validated))}) if validated else frozenset(),
                    holdout_evaluations=count,
                    recorded_accesses=count,
                )
            )
            expected = _expected_refusals(holdout_requested=holdout_requested, deflated=result.deflated is not None)
            if set(outcome) != expected:
                raise RuntimeError(
                    f"{result.identity.strategy_id} {result.identity.namespace}/"
                    f"{result.identity.quarantine_arm} would store with refusals {sorted(outcome)} against the "
                    f"expected {sorted(expected)} — refusing before the first INSERT rather than leaving rows "
                    "behind whose gate verdict nobody predicted"
                )


def _assert_every_runnable_produced_rows(
    report: BacktestRunReport,
    *,
    namespaces: Sequence[ResultNamespace],
) -> None:
    """§11 — a RUNNABLE strategy that produced no row FAILS the run.

    ⚠ Against the RUNNABLE set, never against the manifest. S-4 is expected to
    be absent and its exclusion is a reported reason, not a failure; the two are
    distinguished by the runnable computation itself. Same construction §3.1
    landed after the review bot found its population gate anchored on the wrong
    side.
    """
    expected = len(report.runnable) * len(namespaces) * len(AMBIGUITY_ARMS) * len(QUARANTINE_ARMS)
    produced = Counter(row.strategy_id for row in report.rows)
    missing = sorted(strategy_id for strategy_id in report.runnable if not produced[strategy_id])
    if missing:
        raise RuntimeError(
            f"runnable strategies {missing} produced no result row — an absent row indexes to nothing, so a "
            "shortfall is only detectable against the runnable set the run planned"
        )
    if report.rows_written != expected:
        raise RuntimeError(
            f"{report.rows_written} rows written against {expected} planned "
            f"({len(report.runnable)} strategies x {len(namespaces)} namespaces x {len(AMBIGUITY_ARMS)} ambiguity "
            f"x {len(QUARANTINE_ARMS)} quarantine) — a short write is a namespace or an arm silently missing"
        )


def log_report(report: BacktestRunReport) -> None:
    """Spec §11's per-run report, emitted by the job rather than by a script."""
    logger.info(
        "strategy_backtest_run: %d row(s) over %s; hold-out %s; trial register %s (M = %d)",
        report.rows_written,
        list(report.runnable),
        "EVALUATED" if report.holdout_requested else "not evaluated",
        report.trial_register_version,
        report.declared_trials,
    )
    # ⚠ M is the register's whole count, not this run's. Criterion 6 counts
    # abandoned branches, manual eyeballing and discarded parameter values, so a
    # 3-strategy run legitimately reports M = 11 — said here beside the number
    # because a reader would otherwise take it for an error.
    logger.info(
        "  M = %d counts the whole declared search, not the %d strategies this run measured",
        report.declared_trials,
        len(report.runnable),
    )
    for excluded in report.excluded:
        logger.info("  EXCLUDED %s: %s", excluded.strategy_id, excluded.reason)
    for measurement in report.arms:
        logger.info(
            "  %s/%s %.1fs series=%d close_sources=%s holdout_discarded=%d",
            measurement.strategy_id,
            measurement.quarantine_arm,
            measurement.elapsed_s,
            measurement.series_evaluated,
            dict(sorted(measurement.close_sources.items())),
            measurement.holdout_positions_discarded,
        )
        for namespace, outcome in sorted(measurement.namespaces.items()):
            logger.info(
                "    %s axis %s…%s positions=%d instruments=%d sharpe=%.4f ess=%s",
                namespace,
                outcome.axis_first,
                outcome.axis_last,
                outcome.position_count,
                len(outcome.evaluated_instrument_ids),
                outcome.metrics.sharpe,
                outcome.metrics.effective_sample_size,
            )
    for group, reason in sorted(report.deflation_refusals.items()):
        logger.warning("  no Deflated Sharpe for %s: %s", group, reason)
    for row in report.rows:
        logger.info(
            "    stored %s %s/%s/%s %s instruments=%d refusals=%s",
            row.strategy_id,
            row.namespace,
            row.ambiguity_arm,
            row.quarantine_arm,
            row.result_version,
            row.evaluated_instrument_count,
            list(row.refusals),
        )


__all__ = [
    "BACKTEST_BOOTSTRAP_SEED",
    "BACKTEST_UNIVERSE",
    "RESULT_SCOPE",
    "STANDING_REFUSALS",
    "ArmMeasurement",
    "BacktestRunReport",
    "ExcludedStrategy",
    "NamespaceMeasurement",
    "WrittenRow",
    "assert_no_existing_results",
    "build_result",
    "deflate_group",
    "evaluate_arm",
    "load_corpus",
    "log_report",
    "run_backtest",
    "runnable_strategies",
]
