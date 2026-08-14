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

⚠ S-4 IS RUNNABLE ONLY BECAUSE ITS MANIFEST ENTRY OWNS A CAUSAL LEVEL FACTORY.
The runner computes ATR at signal bar ``t``, fixes the bracket around the
``t+1`` open, and resolves the daily-OHLC uncertainty twice. A future
``level_based`` entry with no factory remains a named exclusion; it is never
silently skipped.

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
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from typing import Any, Final, Literal, cast

import numpy as np
import psycopg

from app.services.cost_model import (
    CARRY_UNMODELLED,
    COST_MODEL_ID,
    FX_UNMODELLED,
    UNKNOWN_NOMINAL_PRICE_BAND,
)
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
    ENTRY_WEIGHT_DRIFT_RULE_ID,
    MONTH_END_REBALANCE_RULE_ID,
    SIZING_RULE_ID,
    LegBook,
    build_buy_and_hold_curve,
    build_entry_weight_drift_curve,
    build_equity_curve,
    build_month_end_rebalanced_curve,
)
from app.services.indicator_series import BarSeries, Universe
from app.services.market_regime_provider import MarketRegimeProvider
from app.services.outcome_resolver import RULE_SET_VERSION as OUTCOME_RULE_SET_VERSION
from app.services.outcome_resolver import ExitLevels, UnresolvedReason, resolve_outcome
from app.services.position_builder import RULE_SET_VERSION as POSITION_RULE_SET_VERSION
from app.services.position_builder import (
    EntryFill,
    ExitFill,
    ExitRegime,
    OutcomePin,
    ResolvedOutcome,
    Window,
    build_positions,
)
from app.services.position_costing import CostedPosition, cost_positions
from app.services.price_segments import (
    load_unresolved_breaks,
    segment_end_index,
    series_segment_bounds,
)
from app.services.price_structure import StructureBar
from app.services.random_entry_cohort import SPEC_COHORT_SIZE, SyntheticControl
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
    store_walk_forward_folds,
)
from app.services.signal_ledger import LedgerRow, resolve_fills
from app.services.strategies.validated_universe import (
    VALIDATED_UNIVERSE_RULE_VERSION,
    load_validated_universe,
)
from app.services.strategy_manifest import STRATEGY_MANIFEST, StrategyEntry, StrategyPurpose
from app.services.strategy_registry import (
    SignalKind,
    StrategyIdentity,
    StrategySignal,
    resolve_participating_bar,
)
from app.services.strategy_result import (
    AMBIGUITY_ARMS,
    CORPUS_VERSION,
    EVALUATION_WINDOW_END,
    EVALUATION_WINDOW_START,
    HOLDOUT_BOUNDARY,
    LEGACY_RETURN_BASIS,
    TOTAL_RETURN_BASIS,
    AmbiguityArm,
    PromotionCandidate,
    PromotionRefusal,
    ResultIdentity,
    ResultNamespace,
    StrategyResult,
    check_promotable,
    namespace_for_position,
)
from app.services.strategy_result_ambiguity import (
    AMBIGUITY_RULE_VERSION,
    AmbiguityRecord,
    ambiguity_verdict,
    load_result_ambiguity,
    store_result_ambiguity,
)
from app.services.strategy_result_universe import (
    ResultUniverseRecord,
    load_result_universe,
    store_result_universe,
)
from app.services.strategy_segmented_evaluation import segmented_member, segmented_signals
from app.services.strategy_statistics import StrategyMetrics, TradeReturns, compute_metrics
from app.services.synthetic_control_run import (
    CONTROL_NAMESPACE,
    HOLDOUT_CONTROL_REASON,
    CohortCollector,
    CohortResult,
    run_cohort,
)
from app.services.technical_analysis import OHLCVRow
from app.services.trial_register import TRIAL_REGISTER
from app.services.walk_forward import (
    FOLD_COUNT,
    WALK_FORWARD_MODEL_ID,
    FoldRecord,
    WalkForwardFolds,
    bar_weighted_folds,
    census,
    training_embargo_bars,
)

logger = logging.getLogger(__name__)

BacktestPhase = Literal["corpus", "ranking", "evaluation", "deflation", "write"]


@dataclass(frozen=True)
class BacktestProgressEvent:
    """One transient checkpoint; never evidence completion or a result row."""

    phase: BacktestPhase
    strategy_id: str | None = None
    quarantine_arm: QuarantineArm | None = None
    ambiguity_arm: AmbiguityArm | None = None
    series_seen: int = 0
    series_total: int | None = None


ProgressCallback = Callable[[BacktestProgressEvent], None]


def _emit_progress(callback: ProgressCallback | None, event: BacktestProgressEvent) -> None:
    if callback is not None:
        callback(event)


def _emit_series_progress(
    callback: ProgressCallback | None,
    *,
    phase: Literal["ranking", "evaluation"],
    entry: StrategyEntry,
    quarantine_arm: QuarantineArm,
    ambiguity_arm: AmbiguityArm | None,
    series_seen: int,
    series_total: int,
) -> None:
    """Bound callback overhead while guaranteeing an early and final tick."""
    if series_seen == 1 or series_seen == series_total or series_seen % 25 == 0:
        _emit_progress(
            callback,
            BacktestProgressEvent(
                phase=phase,
                strategy_id=entry.strategy_id,
                quarantine_arm=quarantine_arm,
                ambiguity_arm=ambiguity_arm,
                series_seen=series_seen,
                series_total=series_total,
            ),
        )


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

#: §9/#2505 — the three refusals no invocation of this job can close, whatever
#: it measures. ``universe_basis_not_survivorship_free`` is blocked on #2284's
#: corpus purchase, ``carry_unmodelled`` on #2277's carry measurement and
#: ``fx_unmodelled`` on #2363's FX measurement — SEPARATE members since #2363,
#: because they close on unrelated evidence and one will be live while the
#: other is not.
#:
#: ⚠⚠ ``synthetic_control_not_run`` WAS A FOURTH MEMBER HERE AND IS NOT ANY MORE
#: (#2601). It stood on the stated grounds that *"the only cohort that exists
#: lives in a developer cache no job may depend on"* — which was true of the
#: cohort, not of the control: ``synthetic_control_run`` now builds the cohort's
#: inputs by riding the corpus pass this job already makes, so the refusal is
#: closable by an invocation that asks for it. It is still added by
#: ``_expected_refusals`` for a run that does not, and for every hold-out row
#: (``HOLDOUT_CONTROL_REASON``) — a CONDITIONAL refusal rather than a standing
#: one, which is the whole difference.
#:
#: ⚠ The job still cannot make anything promotable on this corpus, and that is
#: correct rather than a shortfall — §6 of the bounded-backtester spec states
#: the intended initial state in those words. What it changes is that the
#: refusals become SPECIFIC AND FEW instead of a generic failure.
STANDING_REFUSALS: Final[frozenset[PromotionRefusal]] = frozenset(
    {
        "universe_basis_not_survivorship_free",
        "carry_unmodelled",
        "fx_unmodelled",
        "promotion_evidence_missing",
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
#: never written: the job would otherwise keep producing 32 rows while the arm
#: space said 48.
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

#: The IN-SAMPLE axis and its per-date bar count — criterion 5's fold cut is
#: bar-weighted, and ``_AXIS_SQL`` carries no counts because nothing else needs
#: them.
#:
#: ⚠ ``< %(boundary)s`` and never ``<=``: ``HOLDOUT_BOUNDARY`` is the FIRST
#: HOLD-OUT BAR (``namespace_for_bar``), so including it would cut folds over a
#: withheld date. Deliberately the same predicate as
#: ``scripts/verify_2240_walk_forward.py``'s own axis query, so the split this
#: job stores and the split that script asserts are cut over one axis.
#:
#: ⚠ The counts are the STORED bars and are therefore arm-invariant, which is
#: what makes criterion 9's two arms comparable — see ``load_corpus``.
_INSAMPLE_AXIS_SQL = """
    SELECT d.bar_date, count(*)
    FROM research_price_series s
    JOIN research_price_daily d ON d.series_id = s.series_id
    WHERE s.instrument_id = ANY(%(ids)s)
      AND d.bar_date >= %(start)s
      AND d.bar_date <= %(end)s
      AND d.bar_date < %(boundary)s
    GROUP BY d.bar_date
    ORDER BY 1
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
    #: Criterion 5's label windows, on the panel axis — populated for the
    #: ``in_sample`` namespace and EMPTY for ``hold_out``, which has no split.
    #: ⚠ These are the legs that reached the CURVE, so the census describes the
    #: same observations every metric on the row was computed from. It is
    #: therefore ``<= position_count``, which also counts the positions §3.4
    #: excluded as uncosted; ``_cut_splits`` logs both so the gap is visible
    #: rather than inferred. Measured 2026-08-08 the two are EQUAL on every
    #: in-sample arm — ``unpriced_trade_count`` and ``open_trade_count`` are 0
    #: on all 12 stored rows — but that is a property of this corpus, not a
    #: guarantee, which is why the bound is stated as ``<=``.
    label_starts: array[int] = field(default_factory=lambda: array("i"))
    label_ends: array[int] = field(default_factory=lambda: array("i"))
    #: Sizing-path diagnostics retained for rule attribution (#2430). These are
    #: not promotion metrics; they explain whether a return changed because a
    #: rule traded more or could not fund otherwise-identical entries.
    rebalance_costs: float = 0.0
    short_funded_entries: int = 0
    traded_notional_total: float = 0.0


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
    #: ``None`` means the strategy cannot produce an ambiguous level touch, so
    #: one measured population is shared by both stored ambiguity identities.
    ambiguity_arm: AmbiguityArm | None = None
    #: §9's random-entry control for this arm's ``in_sample`` namespace, when the
    #: invocation asked for one. ⚠ ``None`` is the DEFAULT and means the run did
    #: not compute it — never that a computed control was lost: ``run_cohort``
    #: raises rather than returning a partial one.
    cohort: CohortResult | None = None


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
    #: Criterion 5's fold rows attached to this result — ``FOLD_COUNT`` on an
    #: in-sample row and 0 on a hold-out one, which ``sql/269``'s trigger
    #: refuses folds for.
    folds_written: int = 0


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
            if entry.exit_levels is not None or entry.exit_levels_batch is not None:
                raise RuntimeError(
                    f"{strategy_id} supplies scalar or batch exit levels for a non-level regime — the declared "
                    "close source and its consumer disagree"
                )
            runnable.append(strategy_id)
            continue
        if entry.exit_levels is not None:
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


def _resolved_level_outcomes(
    entry: StrategyEntry,
    entries: Sequence[EntryFill],
    *,
    series: BarSeries,
    ambiguity_arm: AmbiguityArm,
    quarantine_arm: QuarantineArm,
    unresolved_breaks: Sequence[date],
) -> list[ResolvedOutcome]:
    """Resolve a level strategy with causal levels and a declared OHLC bound.

    Daily OHLC cannot reveal which resting order touched first when one bar
    spans both. The resolver therefore emits ``ambiguous`` and this adapter
    converts only those rows to the declared best/worst sensitivity price.
    """
    return _resolved_level_outcomes_for_arms(
        entry,
        entries,
        series=series,
        ambiguity_arms=(ambiguity_arm,),
        quarantine_arm=quarantine_arm,
        unresolved_breaks=unresolved_breaks,
    )[ambiguity_arm]


def _resolved_level_outcomes_for_arms(
    entry: StrategyEntry,
    entries: Sequence[EntryFill],
    *,
    series: BarSeries,
    ambiguity_arms: Sequence[AmbiguityArm],
    quarantine_arm: QuarantineArm,
    unresolved_breaks: Sequence[date],
) -> dict[AmbiguityArm, list[ResolvedOutcome]]:
    """Resolve one filled population once, then project declared OHLC arms.

    Only a genuinely ambiguous daily bar differs between ``best_case`` and
    ``worst_case``. Signal fills, causal levels, gap outcomes, expiry and
    unresolved reasons are shared facts. Each returned list is nevertheless a
    distinct mutable container so later position/book accumulation cannot leak
    between arms.
    """
    requested = tuple(ambiguity_arms)
    if not requested or len(set(requested)) != len(requested):
        raise ValueError("ambiguity_arms must be a non-empty sequence without duplicates")
    unknown = {arm for arm in requested if arm not in AMBIGUITY_ARMS}
    if unknown:
        raise ValueError(f"unknown ambiguity arms {sorted(unknown)}")
    levels_by_entry = _exit_levels_for_entries(
        entry,
        entries,
        series=series,
        unresolved_breaks=unresolved_breaks,
    )
    bar_index = {when: index for index, when in enumerate(series.dates)}
    missing_reason: UnresolvedReason = "quarantined_bar" if quarantine_arm == "masked" else "missing_bar_data"
    masked_reasons: dict[int, UnresolvedReason] = {
        index: missing_reason
        for index, row in enumerate(series.rows)
        if any(row.get(field) is None for field in ("open", "high", "low", "close"))
    }
    resolved: dict[AmbiguityArm, list[ResolvedOutcome]] = {arm: [] for arm in requested}
    for fill, levels in zip(entries, levels_by_entry, strict=True):
        if isinstance(levels, str):
            for ambiguity_arm in requested:
                resolved[ambiguity_arm].append(
                    ResolvedOutcome(
                        signal_id=fill.signal_id,
                        rule_set_version=OUTCOME_RULE_SET_VERSION,
                        input_rule_set_version=QUARANTINE_RULE_SET_VERSION,
                        outcome="unresolved",
                        exit_bar_date=None,
                        exit_price=None,
                        reason=levels,
                    )
                )
            continue
        fill_index = bar_index[fill.fill_bar_date]
        position_segment_end = segment_end_index(
            series,
            fill_index=fill_index,
            unresolved_breaks=unresolved_breaks,
        )
        outcome = resolve_outcome(
            series=series,
            fill_index=fill_index,
            entry_price=fill.fill_price,
            levels=levels,
            masked_bar_reasons=masked_reasons,
            segment_end_index=position_segment_end,
        )
        for ambiguity_arm in requested:
            outcome_name = outcome.outcome
            exit_price = outcome.exit_price
            if outcome_name == "ambiguous":
                # An ambiguous outcome needs BOTH levels touched in one bar, so a
                # stop-only bracket (take_profit=None, S-7) can never reach here —
                # the resolver's rule 3 tests a target that does not exist.
                assert levels.take_profit is not None
                outcome_name = "tp_hit" if ambiguity_arm == "best_case" else "sl_hit"
                exit_price = levels.take_profit if ambiguity_arm == "best_case" else levels.stop_loss
            resolved[ambiguity_arm].append(
                ResolvedOutcome(
                    signal_id=fill.signal_id,
                    rule_set_version=OUTCOME_RULE_SET_VERSION,
                    input_rule_set_version=QUARANTINE_RULE_SET_VERSION,
                    outcome=outcome_name,
                    exit_bar_date=outcome.exit_bar_date,
                    exit_price=exit_price,
                    reason=outcome.reason,
                    unresolved_until_bar_date=(
                        series.dates[position_segment_end + 1]
                        if outcome.reason == "series_break" and position_segment_end is not None
                        else None
                    ),
                ),
            )
    return resolved


def _exit_levels_for_entries(
    entry: StrategyEntry,
    entries: Sequence[EntryFill],
    *,
    series: BarSeries,
    unresolved_breaks: Sequence[date],
) -> tuple[ExitLevels | UnresolvedReason, ...]:
    """Build level objects in entry order, batching only within one segment.

    Indicators restart at every unresolved scale break. Grouping by the exact
    half-open segment preserves that reset while avoiding one full ATR pass per
    entry. The scalar manifest factory remains the mandatory fallback and test
    oracle.
    """
    if entry.exit_levels is None:
        raise ValueError(f"{entry.strategy_id} is level-based but declares no exit-level factory")
    if not entries:
        return ()
    bar_index = {when: index for index, when in enumerate(series.dates)}
    indexed = [(position, fill, bar_index[fill.signal_bar_date]) for position, fill in enumerate(entries)]
    unassigned = object()
    built: list[ExitLevels | UnresolvedReason | object] = [unassigned] * len(entries)
    for start, end in series_segment_bounds(series, unresolved_breaks=unresolved_breaks):
        segment_entries = [(position, fill, index - start) for position, fill, index in indexed if start <= index < end]
        if not segment_entries:
            continue
        signal_series = BarSeries(dates=series.dates[start:end], rows=series.rows[start:end])
        requests = tuple((local_index, fill.fill_price) for _, fill, local_index in segment_entries)
        if entry.exit_levels_batch is not None:
            levels = tuple(
                entry.exit_levels_batch(
                    signal_series,
                    requests=requests,
                    universe=BACKTEST_UNIVERSE,
                )
            )
        else:
            levels = tuple(
                entry.exit_levels(
                    signal_series,
                    signal_index=local_index,
                    entry_price=fill.fill_price,
                    universe=BACKTEST_UNIVERSE,
                )
                for _, fill, local_index in segment_entries
            )
        if len(levels) != len(segment_entries):
            raise RuntimeError(
                f"{entry.strategy_id} batch exit factory returned {len(levels)} levels for "
                f"{len(segment_entries)} requests"
            )
        for (position, _fill, _local_index), level in zip(segment_entries, levels, strict=True):
            built[position] = level
    if any(level is unassigned for level in built):
        raise RuntimeError(f"{entry.strategy_id} could not assign an exit bracket to every filled entry")
    return cast(tuple[ExitLevels | UnresolvedReason, ...], tuple(built))


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
    #: Positionally parallel to `entry_dates`; `TradeReturns` enforces that.
    exit_dates: list[date] = field(default_factory=list)
    instruments: set[int] = field(default_factory=set)
    positions: int = 0
    open_at_end: int = 0
    excluded: Counter[str] = field(default_factory=Counter)
    first_index: int | None = None
    last_index: int | None = None
    #: Whether to accumulate criterion 5's label windows off this book. Set for
    #: the in-sample namespace ONLY — ``walk_forward``'s header: *"the hold-out
    #: is not an input to any function in this module and never becomes one"*,
    #: and ``sql/269``'s trigger refuses a fold row on a hold-out result. On a
    #: hold-out book the two arrays would be ~20 MB of what nothing may read.
    records_label_windows: bool = False
    #: Parallel arrays: the panel-axis index of the entry fill and of the close.
    #: ⚠ Same construction as ``verify_2240_walk_forward._Observations``, which
    #: is what the split this job stores has to agree with.
    label_starts: array[int] = field(default_factory=lambda: array("i"))
    label_ends: array[int] = field(default_factory=lambda: array("i"))

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
        # ⚠⚠ REALISED LEGS ONLY, and on an in-sample book that is every leg —
        # ``namespace_for_position`` returns ``in_sample`` only for a CLOSED
        # position, so an unrealised one cannot reach here (asserted by
        # ``_measure_namespace``). The guard stays because an open position's
        # label window is UNRESOLVED: its end index is the mark bar, not a
        # close, and feeding that to ``training_embargo_bars`` would report a
        # span the strategy never realised — on an early fold, most of the
        # corpus. ``verify_2240_walk_forward._Observations`` excludes them for
        # the same reason and counts the exclusion.
        if self.records_label_windows and realised:
            self.label_starts.append(entry_index)
            self.label_ends.append(exit_index)

    def daily_trade_returns(self) -> dict[date, float]:
        """Mean realised trade return per ENTRY date — this trial's return series."""
        totals: dict[date, list[float]] = {}
        for value, day in zip(self.returns, self.entry_dates, strict=True):
            totals.setdefault(day, []).append(value)
        return {day: sum(values) / len(values) for day, values in totals.items()}


def build_in_sample_split(
    starts: Sequence[int],
    ends: Sequence[int],
    *,
    axis: Sequence[date],
    bar_counts: Sequence[int],
) -> WalkForwardFolds:
    """Criterion 5's purged split over ONE in-sample population. Pure.

    ``starts`` and ``ends`` are panel-axis indices of the entry fill and the
    close, both inclusive — the label window ``role`` classifies.

    ⚠⚠ THE GEOMETRY DOES NOT DEPEND ON THE POPULATION, AND THAT IS LOAD-BEARING
    FOR CRITERION 9. ``bar_weighted_folds`` reads only the axis, so every result
    row of one run is cut at the same four boundaries; only the measured embargo
    and the census move between arms. If the geometry moved with the arm, the
    masked and admitted censuses would be counts over differently-cut folds and
    no delta between them would be interpretable — the argument
    ``QuarantineCensus`` makes for its own two arms, one grain down.

    ⚠ THE ORDER IS THE RULE: embargo first, census second. The embargo is
    measured off the fold's POST-PURGE training side, so it cannot be computed
    from a census that already needed it. ``training_embargo_bars``' own header
    records why that ordering is what keeps it non-circular.

    ⚠ NO *CALLER* SELECTS THE FOLD COUNT. This function passes the module
    constant and takes no ``fold_count`` parameter of its own: ``FOLD_COUNT``'s
    comment is that *a fold count which can be passed in is a fold count that
    can be swept, and a swept validity gate is a search over validity gates*.
    ``bar_weighted_folds`` still accepts one so a unit test can draw a two-fold
    axis, and ``WalkForwardFolds`` refuses any other count on construction.
    """
    if len(starts) != len(ends):
        raise ValueError(f"{len(starts)} label-window starts against {len(ends)} ends")
    if not starts:
        raise ValueError(
            "no closed in-sample observation to cut folds over — a split counting nothing would record the validity "
            "gate as having run over a population that does not exist"
        )
    folds = bar_weighted_folds(bar_counts, fold_count=FOLD_COUNT)
    records: list[FoldRecord] = []
    for fold in folds:
        embargo = training_embargo_bars(starts, ends, fold=fold)
        records.append(
            FoldRecord(
                fold=fold,
                first_date=axis[fold.first_index],
                last_date=axis[fold.last_index],
                bar_count=sum(bar_counts[fold.first_index : fold.last_index + 1]),
                embargo_bars=embargo,
                census=census(starts, ends, fold=fold, embargo_bars=embargo),
            )
        )
    return WalkForwardFolds(model_id=WALK_FORWARD_MODEL_ID, folds=tuple(records))


def _shifted(book: LegBook, offset: int) -> LegBook:
    """``LegBook.rebased``, kept as a name this module already reads by."""
    return book.rebased(offset)


def _benchmark_book(
    *,
    instruments: frozenset[int],
    raw_closes_by_instrument: Mapping[int, tuple[int, array[float]]],
    wealth_closes_by_instrument: Mapping[int, tuple[int, array[float]]],
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
        located = raw_closes_by_instrument.get(instrument_id)
        wealth_located = wealth_closes_by_instrument.get(instrument_id)
        if located is None or wealth_located is None:  # pragma: no cover - every evaluated instrument was loaded
            continue
        first_axis_index, closes = located
        wealth_first_axis_index, wealth_closes = wealth_located
        if wealth_first_axis_index != first_axis_index or len(wealth_closes) != len(closes):
            raise RuntimeError(f"raw and wealth price axes disagree for instrument {instrument_id}")
        start = max(lo, first_axis_index)
        end = min(hi, first_axis_index + len(closes) - 1)
        if end <= start:
            continue
        window = np.frombuffer(closes, dtype=np.float64)[start - first_axis_index : end - first_axis_index + 1]
        wealth_window = np.frombuffer(wealth_closes, dtype=np.float64)[
            start - first_axis_index : end - first_axis_index + 1
        ]
        usable = np.flatnonzero(
            np.isfinite(window) & np.isfinite(wealth_window) & (window > 0.0) & (wealth_window > 0.0)
        )
        if usable.size < 2:
            continue
        entry_offset = int(usable[0])
        exit_offset = int(usable[-1])
        raw_span = window[entry_offset : exit_offset + 1]
        wealth_span = wealth_window[entry_offset : exit_offset + 1]
        raw_observed = ~np.isnan(raw_span)
        if np.any(
            raw_observed
            & ((~np.isfinite(raw_span)) | (~np.isfinite(wealth_span)) | (raw_span <= 0.0) | (wealth_span <= 0.0))
        ):
            continue
        entry_close = float(window[entry_offset])
        entry_wealth = float(wealth_window[entry_offset])
        exit_wealth = float(wealth_window[exit_offset])
        if entry_close <= 0.0 or entry_wealth <= 0.0 or exit_wealth <= 0.0:
            continue
        # The corpus OHLC is split-adjusted and has no point-in-time split
        # factors.  It cannot honestly choose a nominal-price cost band (#2400).
        half = UNKNOWN_NOMINAL_PRICE_BAND.half_spread
        book.add(
            entry_index=start + entry_offset - lo,
            exit_index=start + exit_offset - lo,
            entry_price=float(Decimal(repr(entry_wealth)) * (one + half)),
            exit_price=float(Decimal(repr(exit_wealth)) * (one - half)),
            half_spread=float(half),
            realised=True,
            marks=[float(value) for value in wealth_span],
        )
    return book


def _absorb(
    costed: Sequence[CostedPosition],
    *,
    series: BarSeries,
    window: Window,
    axis_pos: Mapping[date, int],
    raw_closes: Sequence[float],
    wealth_closes: Sequence[float],
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

        # ⚠ `close_bar_date` is nullable but `realised` is set True ONLY in this
        # branch, so a realised trade provably has one. Bound here rather than
        # re-narrowed at the append below: the guarantee is structural, and
        # re-deriving it there would let the two drift apart (#2623 gap 1).
        close_bar_date: date | None = None
        if position.close_bar_date is not None:
            close_bar_date = position.close_bar_date
            exit_index = axis_pos.get(close_bar_date)
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
        exit_slot = exit_index - first_axis_index
        raw_span = raw_closes[span_from : exit_slot + 1]
        wealth_span = wealth_closes[span_from : exit_slot + 1]
        if any(
            math.isfinite(raw_value) and (not math.isfinite(wealth_value) or wealth_value <= 0.0)
            for raw_value, wealth_value in zip(raw_span, wealth_span, strict=True)
        ):
            book.excluded["total_return_price_missing"] += 1
            continue
        entry_raw = raw_closes[span_from]
        exit_raw = raw_closes[exit_slot]
        entry_wealth_mark = wealth_closes[span_from]
        exit_wealth_mark = wealth_closes[exit_slot]
        if any(
            not math.isfinite(value) or value <= 0.0
            for value in (entry_raw, exit_raw, entry_wealth_mark, exit_wealth_mark)
        ):
            book.excluded["total_return_price_missing"] += 1
            continue
        entry_price = float(row.entry_price_net) * entry_wealth_mark / entry_raw
        exit_price = float(row.exit_price_net) * exit_wealth_mark / exit_raw
        book.add_leg(
            entry_index=entry_index,
            exit_index=exit_index,
            entry_price=entry_price,
            exit_price=exit_price,
            half_spread=float(row.half_spread),
            realised=realised,
            marks=list(wealth_span),
        )
        if realised:
            assert close_bar_date is not None  # realised is set only where it is bound
            book.returns.append((exit_price - entry_price) / entry_price * 100.0)
            book.entry_dates.append(position.entry_fill_bar_date)
            # #2623 gap 1. The exit bar was never lost here — the namespace split
            # above already reads it — only never carried into the metric set.
            book.exit_dates.append(close_bar_date)


@dataclass(frozen=True)
class _Corpus:
    """The evaluation axis and the series to stream, read once per invocation.

    ⚠⚠ ``in_sample_axis`` IS A PREFIX OF ``axis``, NOT A SECOND AXIS, and
    ``load_corpus`` asserts it rather than trusting the two queries to agree.
    Every index this module stores on a fold row is a position on ``axis``,
    while criterion 5's split is defined on the in-sample side — so if the two
    ever stopped coinciding, every stored ``first_index`` would silently point
    at a different date than the ``first_date`` beside it. That is the defect
    class ``walk_forward``'s own header records from 5e-3: *two individually
    correct numbers joined on an axis neither of them names.*
    """

    universe: tuple[int, ...]
    axis: tuple[date, ...]
    axis_pos: Mapping[date, int]
    pairs: tuple[tuple[int, int], ...]
    evaluation_start: date = EVALUATION_WINDOW_START
    evaluation_end: date = EVALUATION_WINDOW_END
    #: The pre-boundary prefix of ``axis``, and how many bars the panel carries
    #: on each of its dates. Criterion 5's fold cut is weighted by the latter.
    in_sample_axis: tuple[date, ...] = ()
    in_sample_bar_counts: tuple[int, ...] = ()
    #: Unresolved scale transitions keyed by instrument. Each date is the first
    #: bar at the new scale and therefore bounds a position opened before it.
    unresolved_breaks: Mapping[int, tuple[date, ...]] = field(default_factory=dict)

    @property
    def window(self) -> Window:
        return Window(start=self.evaluation_start, end=self.evaluation_end)


def load_corpus(
    conn: psycopg.Connection[Any],
    *,
    limit: int | None = None,
    evaluation_window: Window | None = None,
) -> _Corpus:
    """The corpus ∩ §4.0 validated-universe slice, and its union calendar.

    ⚠ ``limit`` exists for a smoke run and the caller must say so in its report.
    A limited pass is not a full-population figure and no row written from one
    describes the population its ``evaluated_instrument_count`` claims.
    """
    window = evaluation_window or Window(start=EVALUATION_WINDOW_START, end=EVALUATION_WINDOW_END)
    if window.start < EVALUATION_WINDOW_START or window.end > EVALUATION_WINDOW_END:
        raise ValueError(
            f"evaluation window {window.start} -> {window.end} lies outside the frozen corpus window "
            f"{EVALUATION_WINDOW_START} -> {EVALUATION_WINDOW_END}"
        )
    universe = load_validated_universe(conn)
    # ⚠ THE PER-RUN FX GATE (#2720). The cost model's ``fx_unmodelled = False``
    # stamp rests on "no conversion event occurs", whose universe half is a
    # MUTABLE table property — #2605's script asserts it full-population, but a
    # later ``sync_universe`` could break it without moving the frozen
    # ``COST_MODEL_ID``. So the run that stamps the flag re-asserts it on its
    # own evaluated set, against the instrument's OWN quote currency rather
    # than the ``exchanges.currency`` proxy, and refuses loudly rather than
    # stamping a claim it did not check. NULL is a violation, not a pass.
    non_usd = conn.execute(
        "SELECT instrument_id, currency FROM instruments "
        "WHERE instrument_id = ANY(%(ids)s) AND (currency IS NULL OR upper(currency) <> 'USD')",
        {"ids": list(universe)},
    ).fetchall()
    if non_usd:
        sample = ", ".join(f"{int(row[0])}={row[1]!r}" for row in non_usd[:5])
        raise RuntimeError(
            f"{len(non_usd)} validated-universe instruments are not USD-quoted ({sample}…) — the cost model "
            f"({COST_MODEL_ID}) closes FX as structurally zero for an all-USD lane, so stamping "
            "fx_unmodelled=false over a non-USD instrument would clear a promotion refusal the run did not "
            "earn. Fix the universe or ship a cost model that prices conversion."
        )
    bounds = {"ids": list(universe), "start": window.start, "end": window.end}
    axis = tuple(row[0] for row in conn.execute(_AXIS_SQL, bounds).fetchall())
    pairs = [(int(row[0]), int(row[1])) for row in conn.execute(_SERIES_SQL, {"ids": list(universe)}).fetchall()]
    if limit is not None:
        pairs = pairs[:limit]
    included_ids = sorted({instrument_id for instrument_id, _series_id in pairs})
    unresolved_breaks = load_unresolved_breaks(conn, included_ids)

    in_sample = conn.execute(
        _INSAMPLE_AXIS_SQL,
        {"ids": list(universe), "start": window.start, "end": window.end, "boundary": HOLDOUT_BOUNDARY},
    ).fetchall()
    in_sample_axis = tuple(row[0] for row in in_sample)
    # ⚠⚠ THE PREFIX INVARIANT, ASSERTED AND NOT ASSUMED. Both queries run over
    # the same validated universe and the same two tables, and
    # ``EVALUATION_WINDOW_END`` is after ``HOLDOUT_BOUNDARY``, so the in-sample
    # axis SHOULD be exactly the dates of ``axis`` before the boundary — which
    # is what lets a fold index and a leg index be the same integer with no
    # re-basing step. Measured true on the dev corpus 2026-08-08 (14,975 of
    # 16,236 dates), but a universe or window change could break it silently and
    # every stored fold row would then carry an index/date pair that disagree.
    expected_prefix = tuple(when for when in axis if when < HOLDOUT_BOUNDARY)
    if in_sample_axis != expected_prefix:
        raise RuntimeError(
            f"the in-sample axis carries {len(in_sample_axis):,} dates against {len(expected_prefix):,} dates of the "
            "evaluation axis before the frozen boundary — a fold index is a position on the evaluation axis, so a "
            "split cut over a different axis would store indices and dates that do not describe each other"
        )
    return _Corpus(
        universe=universe,
        axis=axis,
        axis_pos={when: index for index, when in enumerate(axis)},
        pairs=tuple(pairs),
        evaluation_start=window.start,
        evaluation_end=window.end,
        in_sample_axis=in_sample_axis,
        in_sample_bar_counts=tuple(int(row[1]) for row in in_sample),
        unresolved_breaks=unresolved_breaks,
    )


def _measure_namespace(
    namespace: ResultNamespace,
    book: _NamespaceBook,
    *,
    corpus: _Corpus,
    raw_closes_by_instrument: Mapping[int, tuple[int, array[float]]],
    wealth_closes_by_instrument: Mapping[int, tuple[int, array[float]]],
    sizing_rule: str = SIZING_RULE_ID,
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
    # ⚠ ``namespace_for_position`` returns ``in_sample`` only for a position
    # with a close date, so an in-sample book can hold no unrealised leg and
    # every one of them carries a RESOLVED label window. Asserted rather than
    # commented, because criterion 5's embargo is measured off those windows and
    # a single open leg would contribute the span from its entry to the mark bar
    # — a hold the strategy never realised.
    if namespace == "in_sample" and book.open_at_end:
        raise RuntimeError(
            f"the in-sample namespace holds {book.open_at_end} position(s) open at the window end — "
            "namespace_for_position sends every open position to the hold-out, so criterion 5's label windows "
            "would carry an unresolved span"
        )

    dates = corpus.axis[lo : hi + 1]
    shifted = _shifted(book.book, lo)
    if sizing_rule == SIZING_RULE_ID:
        curve = build_equity_curve(shifted, date_count=len(dates))
    elif sizing_rule == ENTRY_WEIGHT_DRIFT_RULE_ID:
        curve = build_entry_weight_drift_curve(shifted, date_count=len(dates))
    elif sizing_rule == MONTH_END_REBALANCE_RULE_ID:
        curve = build_month_end_rebalanced_curve(shifted, dates=dates)
    else:
        raise ValueError(f"unknown sizing rule {sizing_rule!r}")
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
            raw_closes_by_instrument=raw_closes_by_instrument,
            wealth_closes_by_instrument=wealth_closes_by_instrument,
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
            exit_bar_date=tuple(book.exit_dates),
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
        label_starts=book.label_starts,
        label_ends=book.label_ends,
        rebalance_costs=curve.rebalance_costs,
        short_funded_entries=curve.short_funded_entries,
        traded_notional_total=float(curve.traded_notional.sum()),
    )


def evaluate_arm(
    conn: psycopg.Connection[Any],
    entry: StrategyEntry,
    *,
    corpus: _Corpus,
    quarantine_arm: QuarantineArm,
    ambiguity_arm: AmbiguityArm | None,
    identity: StrategyIdentity,
    namespaces: Sequence[ResultNamespace],
    progress: ProgressCallback | None = None,
    return_basis: str = TOTAL_RETURN_BASIS,
    sizing_rule: str = SIZING_RULE_ID,
    cohort_size: int | None = None,
    regime_provider: MarketRegimeProvider | None = None,
) -> ArmMeasurement:
    """One ``(strategy, quarantine arm)`` corpus pass, end to end.

    ⚠ ``cohort_size`` OPTS IN TO §9's CONTROL AND RIDES THIS SAME PASS. The
    cohort's inputs — the eligible fill bars, their total-return-adjusted opens,
    and the realised in-sample holds — are all derivable from what the loop
    below already holds, so asking for a control costs no second corpus read.
    ``None`` means the invocation did not ask, and the row keeps
    ``synthetic_control_not_run``.

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
    # ⚠ ONE benchmark classification per arm pass, built before the instrument
    # loop. Sourced from the RESEARCH corpus (`spy_chain_v1` — see
    # `MarketRegimeProvider.load_research`), the same source universe the bars
    # come from, so the regime reaches 1993-11-11 rather than `price_daily`'s
    # 2023-02 ceiling. Bars before SPY's 1993 inception still carry `None`; see
    # `_signals_for`.
    #
    # ⚠ INJECTABLE, defaulting to the research read from `conn`, for a harness
    # with no real connection.
    if regime_provider is None:
        regime_provider = MarketRegimeProvider.load_research(conn)
    started = time.monotonic()
    if return_basis not in {LEGACY_RETURN_BASIS, TOTAL_RETURN_BASIS}:
        raise ValueError(f"unknown return basis {return_basis!r}")
    if sizing_rule not in {SIZING_RULE_ID, ENTRY_WEIGHT_DRIFT_RULE_ID, MONTH_END_REBALANCE_RULE_ID}:
        raise ValueError(f"unknown sizing rule {sizing_rule!r}")
    regime = _regime_for(entry, corpus.axis)
    if regime.level_based != (ambiguity_arm is not None):
        raise ValueError(
            f"{entry.strategy_id} level_based={regime.level_based} received ambiguity arm {ambiguity_arm!r}"
        )
    books: dict[ResultNamespace, _NamespaceBook] = {
        name: _NamespaceBook(records_label_windows=(name == "in_sample")) for name in namespaces
    }
    collector = _collector_for(cohort_size, corpus=corpus, namespaces=namespaces)
    close_sources: Counter[str] = Counter()
    discarded: Counter[str] = Counter()
    raw_closes_by_instrument: dict[int, tuple[int, array[float]]] = {}
    wealth_closes_by_instrument: dict[int, tuple[int, array[float]]] = {}
    ranking: dict[SignalKind, _CrossSection] | None = None

    if entry.strategy_class == "cross_sectional":
        ranking = _rank_cross_sections(
            conn,
            entry,
            corpus=corpus,
            quarantine_arm=quarantine_arm,
            regime_provider=regime_provider,
            progress=progress,
        )

    evaluated = 0
    total = len(corpus.pairs)
    for series_seen, (instrument_id, series_id) in enumerate(corpus.pairs, start=1):
        masked = load_masked_series(conn, series_id, arm=quarantine_arm)
        if not masked.bars:
            _emit_series_progress(
                progress,
                phase="evaluation",
                entry=entry,
                quarantine_arm=quarantine_arm,
                ambiguity_arm=ambiguity_arm,
                series_seen=series_seen,
                series_total=total,
            )
            continue
        series = _to_series(masked.bars)
        indices = [corpus.axis_pos[when] for when in series.dates if when in corpus.axis_pos]
        if len(indices) < 2:
            _emit_series_progress(
                progress,
                phase="evaluation",
                entry=entry,
                quarantine_arm=quarantine_arm,
                ambiguity_arm=ambiguity_arm,
                series_seen=series_seen,
                series_total=total,
            )
            continue
        evaluated += 1
        first_axis_index, last_axis_index = indices[0], indices[-1]
        # ⚠ One dense close array per INSTRUMENT, spanning its own first to last
        # axis index with `nan` in between. That is what makes a leg's mark slice
        # O(1) to cut, and it is ~25M floats over the corpus rather than the 85M
        # a full dense panel would need.
        if len(masked.wealth_closes) != len(series):
            raise RuntimeError(f"series {series_id} has misaligned raw and wealth observations")
        raw_closes = [float("nan")] * (last_axis_index - first_axis_index + 1)
        wealth_closes = [float("nan")] * (last_axis_index - first_axis_index + 1)
        for when, row, wealth_close in zip(series.dates, series.rows, masked.wealth_closes, strict=True):
            slot = corpus.axis_pos.get(when)
            close = row.get("close")
            if slot is not None and close is not None:
                raw_closes[slot - first_axis_index] = float(close)
            selected_wealth_close = close if return_basis == LEGACY_RETURN_BASIS else wealth_close
            if slot is not None and selected_wealth_close is not None:
                wealth_closes[slot - first_axis_index] = float(selected_wealth_close)
        raw_closes_by_instrument[instrument_id] = (first_axis_index, array("d", raw_closes))
        wealth_closes_by_instrument[instrument_id] = (first_axis_index, array("d", wealth_closes))

        signals = _signals_for(
            entry,
            series,
            instrument_id=instrument_id,
            ranking=ranking,
            unresolved_breaks=corpus.unresolved_breaks.get(instrument_id, ()),
            regime_provider=regime_provider,
        )
        rows = resolve_fills(signals, series=series, identity=identity, instrument_id=instrument_id)
        entries, exits = _fills(rows, instrument_id)
        outcomes = (
            _resolved_level_outcomes(
                entry,
                entries,
                series=series,
                ambiguity_arm=ambiguity_arm,
                quarantine_arm=quarantine_arm,
                unresolved_breaks=corpus.unresolved_breaks.get(instrument_id, ()),
            )
            if ambiguity_arm is not None
            else []
        )
        built = build_positions(
            strategy_id=entry.strategy_id,
            strategy_version=identity.version,
            entries=entries,
            exits=exits,
            outcomes=outcomes,
            outcome_pin=_OUTCOME_PIN if regime.level_based else None,
            series={instrument_id: series},
            regime=regime,
            window=corpus.window,
        )
        costed = list(cost_positions(built.positions, price_basis="split_adjusted"))
        _absorb(
            costed,
            series=series,
            window=corpus.window,
            axis_pos=corpus.axis_pos,
            raw_closes=raw_closes,
            wealth_closes=wealth_closes,
            first_axis_index=first_axis_index,
            instrument_id=instrument_id,
            books=books,
            close_sources=close_sources,
            discarded=discarded,
        )
        if collector is not None:
            collector.collect(
                rows=rows,
                series=series,
                costed=costed,
                axis_pos=corpus.axis_pos,
                raw_closes=raw_closes,
                wealth_closes=wealth_closes,
                first_axis_index=first_axis_index,
            )
        _emit_series_progress(
            progress,
            phase="evaluation",
            entry=entry,
            quarantine_arm=quarantine_arm,
            ambiguity_arm=ambiguity_arm,
            series_seen=series_seen,
            series_total=total,
        )

    measured: dict[ResultNamespace, NamespaceMeasurement] = {}
    for name in namespaces:
        outcome = _measure_namespace(
            name,
            books[name],
            corpus=corpus,
            raw_closes_by_instrument=raw_closes_by_instrument,
            wealth_closes_by_instrument=wealth_closes_by_instrument,
            sizing_rule=sizing_rule,
        )
        if outcome is not None:
            measured[name] = outcome
    return ArmMeasurement(
        strategy_id=entry.strategy_id,
        strategy_version=identity.version,
        ambiguity_arm=ambiguity_arm,
        quarantine_arm=quarantine_arm,
        namespaces=measured,
        holdout_positions_discarded=discarded.get("hold_out", 0),
        close_sources=dict(close_sources),
        series_evaluated=evaluated,
        elapsed_s=time.monotonic() - started,
        cohort=_run_cohort_for(
            collector,
            measured=measured,
            corpus=corpus,
            cohort_size=cohort_size,
            label=f"{entry.strategy_id}/{ambiguity_arm or 'shared'}/{quarantine_arm}",
        ),
    )


def _collector_for(
    cohort_size: int | None,
    *,
    corpus: _Corpus,
    namespaces: Sequence[ResultNamespace],
) -> CohortCollector | None:
    """A collector when this pass measures the namespace a control is defined for.

    ⚠ A hold-out-only invocation gets NONE rather than an empty collector, and
    the reason is ``HOLDOUT_CONTROL_REASON``: the control is an in-sample
    construction, so an arm that measures no in-sample namespace has nothing to
    build one against and must say ``synthetic_control_not_run`` rather than
    produce a control over the withheld side.
    """
    if cohort_size is None or CONTROL_NAMESPACE not in namespaces:
        return None
    return CohortCollector(window=corpus.window)


def _run_cohort_for(
    collector: CohortCollector | None,
    *,
    measured: Mapping[ResultNamespace, NamespaceMeasurement],
    corpus: _Corpus,
    cohort_size: int | None,
    label: str,
) -> CohortResult | None:
    """§9's control for this arm, once the sleeve it is compared against exists.

    ⚠ THE STRATEGY-SIDE FIGURES COME FROM THE MEASUREMENT THIS RUN JUST MADE,
    not from a stored row. Criterion 11's argument applies to the control as
    much as to the result — a Sharpe compared with a cohort built for a
    different arm is two measurements joined on nothing — and the measurement is
    already in hand here.
    """
    if collector is None or cohort_size is None:
        return None
    outcome = measured.get(CONTROL_NAMESPACE)
    if outcome is None:
        raise RuntimeError(
            f"{label} asked for §9's control but produced no {CONTROL_NAMESPACE} measurement to compare it against — "
            "a cohort with no strategy Sharpe beside it is a null distribution nobody can read"
        )
    result = run_cohort(
        collector,
        axis=corpus.axis,
        strategy_metrics=outcome.metrics,
        benchmark=None,
        cohort_size=cohort_size,
    )
    logger.info(
        "strategy_backtest_run: %s synthetic control — %d members over %d series in %.1fs (%.3fs/member), "
        "cohort mean %.3f%% CI [%.3f, %.3f], cohort Sharpe p%.0f %.4f against %.4f, passed=%s, unmatchable %s",
        label,
        result.control.cohort_size,
        result.series_placed,
        result.elapsed_s,
        result.seconds_per_member,
        result.control.mean_return_pct,
        result.control.mean_return_ci_low_pct,
        result.control.mean_return_ci_high_pct,
        result.control.sharpe_percentile,
        result.control.cohort_sharpe_threshold,
        result.control.strategy_sharpe,
        result.control.passed,
        result.unmatchable,
    )
    return result


def evaluate_level_arms(
    conn: psycopg.Connection[Any],
    entry: StrategyEntry,
    *,
    corpus: _Corpus,
    quarantine_arm: QuarantineArm,
    identity: StrategyIdentity,
    namespaces: Sequence[ResultNamespace],
    progress: ProgressCallback | None = None,
    return_basis: str = TOTAL_RETURN_BASIS,
    sizing_rule: str = SIZING_RULE_ID,
    cohort_size: int | None = None,
    regime_provider: MarketRegimeProvider | None = None,
) -> tuple[ArmMeasurement, ...]:
    """Evaluate both daily-OHLC ambiguity projections from one corpus pass.

    ⚠ ONE COLLECTOR PER AMBIGUITY ARM, not one shared. The arms differ in which
    level a bar resolved to, which moves the CLOSE bar of a position and
    therefore its hold — so a shared placement space would permute one arm's
    holds into the other's null and report a control for a population neither
    arm measured.

    The two arms differ only where one daily bar spans both fixed levels. They
    therefore share immutable series reads, signal rows, fill ids, causal level
    construction and the raw resolver outcome. Namespace books, close-source
    counters and discarded-position counters remain arm-local, so the two
    measurements cannot influence one another after that common evidence.
    """
    # ⚠ ONE benchmark classification per arm pass, built before the instrument
    # loop. Sourced from the RESEARCH corpus (`spy_chain_v1` — see
    # `MarketRegimeProvider.load_research`), the same source universe the bars
    # come from, so the regime reaches 1993-11-11 rather than `price_daily`'s
    # 2023-02 ceiling. Bars before SPY's 1993 inception still carry `None`; see
    # `_signals_for`.
    #
    # ⚠ INJECTABLE, defaulting to the research read from `conn`, for a harness
    # with no real connection.
    if regime_provider is None:
        regime_provider = MarketRegimeProvider.load_research(conn)
    started = time.monotonic()
    if return_basis not in {LEGACY_RETURN_BASIS, TOTAL_RETURN_BASIS}:
        raise ValueError(f"unknown return basis {return_basis!r}")
    if sizing_rule not in {SIZING_RULE_ID, ENTRY_WEIGHT_DRIFT_RULE_ID, MONTH_END_REBALANCE_RULE_ID}:
        raise ValueError(f"unknown sizing rule {sizing_rule!r}")
    regime = _regime_for(entry, corpus.axis)
    if not regime.level_based:
        raise ValueError(f"{entry.strategy_id} is not level-based and has no ambiguity arms to share")
    books: dict[AmbiguityArm, dict[ResultNamespace, _NamespaceBook]] = {
        ambiguity: {name: _NamespaceBook(records_label_windows=(name == "in_sample")) for name in namespaces}
        for ambiguity in AMBIGUITY_ARM_ORDER
    }
    close_sources: dict[AmbiguityArm, Counter[str]] = {ambiguity: Counter() for ambiguity in AMBIGUITY_ARM_ORDER}
    discarded: dict[AmbiguityArm, Counter[str]] = {ambiguity: Counter() for ambiguity in AMBIGUITY_ARM_ORDER}
    collectors: dict[AmbiguityArm, CohortCollector | None] = {
        ambiguity: _collector_for(cohort_size, corpus=corpus, namespaces=namespaces)
        for ambiguity in AMBIGUITY_ARM_ORDER
    }
    raw_closes_by_instrument: dict[int, tuple[int, array[float]]] = {}
    wealth_closes_by_instrument: dict[int, tuple[int, array[float]]] = {}
    ranking = (
        _rank_cross_sections(
            conn,
            entry,
            corpus=corpus,
            quarantine_arm=quarantine_arm,
            regime_provider=regime_provider,
            progress=progress,
        )
        if entry.strategy_class == "cross_sectional"
        else None
    )

    evaluated = 0
    total = len(corpus.pairs)
    for series_seen, (instrument_id, series_id) in enumerate(corpus.pairs, start=1):
        masked = load_masked_series(conn, series_id, arm=quarantine_arm)
        if not masked.bars:
            _emit_series_progress(
                progress,
                phase="evaluation",
                entry=entry,
                quarantine_arm=quarantine_arm,
                ambiguity_arm=None,
                series_seen=series_seen,
                series_total=total,
            )
            continue
        series = _to_series(masked.bars)
        indices = [corpus.axis_pos[when] for when in series.dates if when in corpus.axis_pos]
        if len(indices) < 2:
            _emit_series_progress(
                progress,
                phase="evaluation",
                entry=entry,
                quarantine_arm=quarantine_arm,
                ambiguity_arm=None,
                series_seen=series_seen,
                series_total=total,
            )
            continue
        evaluated += 1
        first_axis_index, last_axis_index = indices[0], indices[-1]
        if len(masked.wealth_closes) != len(series):
            raise RuntimeError(f"series {series_id} has misaligned raw and wealth observations")
        raw_closes = [float("nan")] * (last_axis_index - first_axis_index + 1)
        wealth_closes = [float("nan")] * (last_axis_index - first_axis_index + 1)
        for when, row, wealth_close in zip(series.dates, series.rows, masked.wealth_closes, strict=True):
            slot = corpus.axis_pos.get(when)
            close = row.get("close")
            if slot is not None and close is not None:
                raw_closes[slot - first_axis_index] = float(close)
            selected_wealth_close = close if return_basis == LEGACY_RETURN_BASIS else wealth_close
            if slot is not None and selected_wealth_close is not None:
                wealth_closes[slot - first_axis_index] = float(selected_wealth_close)
        # Read-only after construction. Both namespace measurements must mark
        # against the same observations; no arm mutates this array.
        raw_closes_by_instrument[instrument_id] = (first_axis_index, array("d", raw_closes))
        wealth_closes_by_instrument[instrument_id] = (first_axis_index, array("d", wealth_closes))

        signals = _signals_for(
            entry,
            series,
            instrument_id=instrument_id,
            ranking=ranking,
            unresolved_breaks=corpus.unresolved_breaks.get(instrument_id, ()),
            regime_provider=regime_provider,
        )
        rows = resolve_fills(signals, series=series, identity=identity, instrument_id=instrument_id)
        entries, exits = _fills(rows, instrument_id)
        outcomes = _resolved_level_outcomes_for_arms(
            entry,
            entries,
            series=series,
            ambiguity_arms=AMBIGUITY_ARM_ORDER,
            quarantine_arm=quarantine_arm,
            unresolved_breaks=corpus.unresolved_breaks.get(instrument_id, ()),
        )
        for ambiguity in AMBIGUITY_ARM_ORDER:
            built = build_positions(
                strategy_id=entry.strategy_id,
                strategy_version=identity.version,
                entries=entries,
                exits=exits,
                outcomes=outcomes[ambiguity],
                outcome_pin=_OUTCOME_PIN,
                series={instrument_id: series},
                regime=regime,
                window=corpus.window,
            )
            costed = list(cost_positions(built.positions, price_basis="split_adjusted"))
            _absorb(
                costed,
                series=series,
                window=corpus.window,
                axis_pos=corpus.axis_pos,
                raw_closes=raw_closes,
                wealth_closes=wealth_closes,
                first_axis_index=first_axis_index,
                instrument_id=instrument_id,
                books=books[ambiguity],
                close_sources=close_sources[ambiguity],
                discarded=discarded[ambiguity],
            )
            arm_collector = collectors[ambiguity]
            if arm_collector is not None:
                arm_collector.collect(
                    rows=rows,
                    series=series,
                    costed=costed,
                    axis_pos=corpus.axis_pos,
                    raw_closes=raw_closes,
                    wealth_closes=wealth_closes,
                    first_axis_index=first_axis_index,
                )
        _emit_series_progress(
            progress,
            phase="evaluation",
            entry=entry,
            quarantine_arm=quarantine_arm,
            ambiguity_arm=None,
            series_seen=series_seen,
            series_total=total,
        )

    elapsed = time.monotonic() - started
    measurements: list[ArmMeasurement] = []
    for ambiguity in AMBIGUITY_ARM_ORDER:
        measured: dict[ResultNamespace, NamespaceMeasurement] = {}
        for name in namespaces:
            outcome = _measure_namespace(
                name,
                books[ambiguity][name],
                corpus=corpus,
                raw_closes_by_instrument=raw_closes_by_instrument,
                wealth_closes_by_instrument=wealth_closes_by_instrument,
                sizing_rule=sizing_rule,
            )
            if outcome is not None:
                measured[name] = outcome
        measurements.append(
            ArmMeasurement(
                strategy_id=entry.strategy_id,
                strategy_version=identity.version,
                ambiguity_arm=ambiguity,
                quarantine_arm=quarantine_arm,
                namespaces=measured,
                holdout_positions_discarded=discarded[ambiguity].get("hold_out", 0),
                close_sources=dict(close_sources[ambiguity]),
                series_evaluated=evaluated,
                elapsed_s=elapsed,
                cohort=_run_cohort_for(
                    collectors[ambiguity],
                    measured=measured,
                    corpus=corpus,
                    cohort_size=cohort_size,
                    label=f"{entry.strategy_id}/{ambiguity}/{quarantine_arm}",
                ),
            )
        )
    return tuple(measurements)


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
    ranking: Mapping[SignalKind, _CrossSection] | None,
    unresolved_breaks: Sequence[date] = (),
    regime_provider: MarketRegimeProvider,
) -> list[StrategySignal]:
    """One instrument's whole-series verdicts, per-series or cross-sectional.

    ⚠ THE REGIME NOW COMES FROM THE RESEARCH CORPUS (``spy_chain_v1``, see
    ``MarketRegimeProvider.load_research``) — the same source universe as the
    bars — and is classifiable 1993-11-11 → the corpus end. The residual bound
    is SPY's own 1993-01-22 inception: bars before it carry ``None``, so a
    regime-gated strategy (S-5…S-10) still cannot fire 1962–1992 and that span
    is an empty sample rather than a bad one. Defaulting those decades to a
    permissive regime would fabricate market conditions; a pre-1993 regime
    would need a different benchmark (the S&P 500 index itself) and a new
    ``spy_chain`` version.
    """
    if entry.signals is not None:
        return segmented_signals(
            entry,
            series,
            universe=BACKTEST_UNIVERSE,
            masked_reason="quarantined_bar",
            unresolved_breaks=unresolved_breaks,
            regime=regime_provider.for_dates(series.dates),
        )
    assert entry.member is not None and ranking is not None
    signals: list[StrategySignal] = []
    for leg, leg_ranking in sorted(ranking.items()):
        staged = segmented_member(
            entry,
            series,
            panel_decision_dates=leg_ranking.decision_dates,
            universe=BACKTEST_UNIVERSE,
            masked_reason="quarantined_bar",
            unresolved_breaks=unresolved_breaks,
            regime=regime_provider.for_dates(series.dates),
            leg=leg,
        )
        for index, verdict in enumerate(staged.verdicts):
            if verdict is not None:
                signals.append(verdict)
                continue
            when = series.dates[index]
            if when in leg_ranking.thin:
                # ⚠ ``min_participants`` is the RUNNER's call, mirroring
                # ``evaluate_cross_sectional``: an empty return from ``select``
                # cannot be told apart from "the panel was too thin", and criterion
                # 8 exists to keep that distinction countable.
                signals.append(
                    StrategySignal(verdict="not_evaluable", signal_index=index, kind=leg, reason="thin_cross_section")
                )
                continue
            signals.append(
                resolve_participating_bar(
                    when=when,
                    index=index,
                    kind=leg,
                    selected=instrument_id in leg_ranking.winners.get(when, frozenset()),
                    admissible_dates=staged.admissible_dates,
                    mandatory_dates=staged.mandatory_dates,
                )
            )
    return signals


def _rank_cross_sections(
    conn: psycopg.Connection[Any],
    entry: StrategyEntry,
    *,
    corpus: _Corpus,
    quarantine_arm: QuarantineArm,
    regime_provider: MarketRegimeProvider,
    progress: ProgressCallback | None = None,
) -> dict[SignalKind, _CrossSection]:
    """Sub-pass A per ranked leg — one corpus read per leg, S-2 one, S-10 two.

    ⚠ NOT one read shared: S-10's legs rank DIFFERENT panels (the entry panel
    carries the $1 floor, the exit panel does not), so their score sets
    genuinely differ and a shared read would silently rank one leg on the
    other's denominator.
    """
    legs: tuple[SignalKind, ...] = ("entry", "exit") if entry.exit_leg is not None else ("entry",)
    return {
        leg: _rank_cross_section(
            conn,
            entry,
            corpus=corpus,
            quarantine_arm=quarantine_arm,
            regime_provider=regime_provider,
            leg=leg,
            progress=progress,
        )
        for leg in legs
    }


def _rank_cross_section(
    conn: psycopg.Connection[Any],
    entry: StrategyEntry,
    *,
    corpus: _Corpus,
    quarantine_arm: QuarantineArm,
    regime_provider: MarketRegimeProvider,
    leg: SignalKind = "entry",
    progress: ProgressCallback | None = None,
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
    if leg == "entry":
        select, min_participants = entry.select, entry.min_participants
    else:
        assert entry.exit_leg is not None
        select, min_participants = entry.exit_leg.select, entry.exit_leg.min_participants
    assert entry.member is not None and select is not None and min_participants is not None
    decision_dates = entry.decision_calendar(corpus.axis)
    if decision_dates is None:  # pragma: no cover - the manifest guarantees one
        raise RuntimeError(f"{entry.strategy_id} is cross_sectional but returned no decision calendar")

    scores: dict[date, dict[int, float]] = {}
    total = len(corpus.pairs)
    for series_seen, (instrument_id, series_id) in enumerate(corpus.pairs, start=1):
        masked = load_masked_series(conn, series_id, arm=quarantine_arm)
        if not masked.bars:
            _emit_series_progress(
                progress,
                phase="ranking",
                entry=entry,
                quarantine_arm=quarantine_arm,
                ambiguity_arm=None,
                series_seen=series_seen,
                series_total=total,
            )
            continue
        series = _to_series(masked.bars)
        if len(series) < 2:
            _emit_series_progress(
                progress,
                phase="ranking",
                entry=entry,
                quarantine_arm=quarantine_arm,
                ambiguity_arm=None,
                series_seen=series_seen,
                series_total=total,
            )
            continue
        staged = segmented_member(
            entry,
            series,
            panel_decision_dates=decision_dates,
            universe=BACKTEST_UNIVERSE,
            masked_reason="quarantined_bar",
            unresolved_breaks=corpus.unresolved_breaks.get(instrument_id, ()),
            regime=regime_provider.for_dates(series.dates),
            leg=leg,
        )
        for when, value in staged.scores.items():
            scores.setdefault(when, {})[instrument_id] = value
        _emit_series_progress(
            progress,
            phase="ranking",
            entry=entry,
            quarantine_arm=quarantine_arm,
            ambiguity_arm=None,
            series_seen=series_seen,
            series_total=total,
        )

    winners: dict[date, frozenset[int]] = {}
    thin: set[date] = set()
    for when in sorted(scores):
        at_date = scores[when]
        if len(at_date) < min_participants:
            thin.add(when)
            continue
        selected = frozenset(select(when, at_date))
        unknown = selected - at_date.keys()
        if unknown:
            raise ValueError(
                f"{entry.strategy_id} {leg} select returned {sorted(unknown)} on {when}, which did not participate "
                "in that cross-section — every winner must be one of the members offered"
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
    purpose: StrategyPurpose,
    ambiguity_arm: AmbiguityArm,
    quarantine_arm: QuarantineArm,
    deflated: DeflatedSharpeResult | None,
    evaluation_window: Window | None = None,
    synthetic_control: SyntheticControl | None = None,
) -> StrategyResult:
    """One ``strategy_results`` row. §7's fourteen identity members, all pinned.

    ⚠ THIRTEEN OF THE FOURTEEN ARE READ FROM A MODULE THAT FROZE THEM, and that
    is the design working. Recent-evidence windows are the sole exception: they
    are selected from ``strategy_recent_evidence.RECENT_EVIDENCE_WINDOWS`` by
    the caller and remain part of the result hash. Raw dates are not exposed as
    job parameters, so an operator still cannot mint an unregistered window.

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
    window = evaluation_window or Window(start=EVALUATION_WINDOW_START, end=EVALUATION_WINDOW_END)
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
            window_start=window.start,
            window_end=window.end,
            position_rule_set_version=POSITION_RULE_SET_VERSION,
            outcome_rule_set_version=OUTCOME_RULE_SET_VERSION,
            input_rule_set_version=QUARANTINE_RULE_SET_VERSION,
            return_basis=TOTAL_RETURN_BASIS,
        ),
        purpose=purpose,
        metrics=outcome.metrics,
        universe_basis=BACKTEST_UNIVERSE,
        # ⚠ ``CARRY_UNMODELLED`` AS AT COMPUTE TIME, stamped per row. When carry
        # is finally measured every row computed before that measurement must
        # STAY unpromotable, which a gate reading today's module constant would
        # silently undo.
        carry_unmodelled=CARRY_UNMODELLED,
        fx_unmodelled=FX_UNMODELLED,
        evaluated_instrument_count=len(outcome.evaluated_instrument_ids),
        trial_count=None if deflated is None else deflated.declared_trials,
        deflated_sharpe=None if deflated is None else Decimal(repr(deflated.deflated_sharpe)),
        deflated=deflated,
        # ⚠ NOT PART OF ``ResultIdentity`` (§7 lists fourteen members and this is
        # none of them), so attaching a control does NOT mint a new
        # ``result_version``. A row already stored without one therefore blocks
        # its own re-run through ``assert_no_existing_results`` — deliberately:
        # a stored result silently gaining a passed threshold is exactly the
        # "track record nobody can audit" that assertion exists to prevent.
        synthetic_control=synthetic_control,
    )


def _candidate(
    result: StrategyResult,
    *,
    validated: frozenset[int],
    evaluated: frozenset[int],
    holdout_evaluations: int,
    recorded_accesses: int,
    ambiguity_material: bool | None,
) -> PromotionCandidate:
    """The gate's inputs, supplied DIRECTLY rather than read back.

    ⚠⚠ COMPUTING THE REFUSAL LIST MUST NOT TOUCH THE HOLD-OUT ACCESS LOG.
    ``result_ledger.quarantine_arms_compared`` records a ``read`` access on a
    ``hold_out`` identity — deliberately, because *looking is the event criterion
    5 governs* — so a job that gated every hold-out row it had just written
    would add one read record per row and turn the audit trail into a count of
    its own automation. This job wrote both arms in the same transaction, so it
    knows, and it must not ask the database a question it is the answer to.

    ``ambiguity_material`` is computed from the measurements already in memory.
    It is ``False`` for a shared non-level measurement, ``False`` for two
    numerically equal level arms, and ``None`` when the arms differ but this run
    has no synthetic-control threshold with which §3.4 can judge materiality.
    The last state deliberately adds ``ambiguity_arms_not_compared``.
    """
    return PromotionCandidate(
        result=result,
        evaluated_instrument_ids=evaluated,
        validated_universe_ids=validated,
        holdout_evaluations=holdout_evaluations,
        recorded_accesses=recorded_accesses,
        ambiguity_material=ambiguity_material,
        quarantine_arms_compared=True,
    )


def _ambiguity_record_for(
    arms: Sequence[ArmMeasurement],
    result: StrategyResult,
) -> AmbiguityRecord:
    """§3.4's comparison INPUTS for this row, for freezing under #2625.

    Split out of ``_ambiguity_material_for`` so the verdict has exactly one
    definition: this function gathers what was compared, and
    ``ambiguity_verdict`` decides. The stored record is then provably the thing
    the write-time gate judged, rather than a second description of it.

    ⚠ THE SHORT-CIRCUIT IS PRESERVED, and it is observable. The original
    returned ``None`` on the FIRST unpriced arm without validating the later
    arm's namespace, so collecting both eagerly would raise where the runner
    used to return a verdict. The ``break`` keeps that exact behaviour.

    ⚠ A NON-FINITE SHARPE IS TREATED AS UNPRICED, which is what the original
    did by accident and this does on purpose. ``sharpes[0] == sharpes[1]`` is
    ``False`` for NaN, so the old code fell through to ``None``; ``AmbiguityRecord``
    refuses a non-finite value outright, so without this branch a degenerate
    zero-volatility measurement would turn a "not compared" verdict into a
    crashed run.
    """
    matching = [
        measurement
        for measurement in arms
        if measurement.strategy_id == result.identity.strategy_id
        and measurement.quarantine_arm == result.identity.quarantine_arm
    ]
    if any(measurement.ambiguity_arm is None for measurement in matching):
        # ⚠ PRECEDENCE: presence of a shared measurement decides the record
        # before any Sharpe is read, and regardless of their values.
        return AmbiguityRecord(
            ambiguity_rule_version=AMBIGUITY_RULE_VERSION,
            comparison_basis="shared_measurement",
        )
    by_arm = {measurement.ambiguity_arm: measurement for measurement in matching}
    if set(by_arm) != set(AMBIGUITY_ARM_ORDER):
        raise RuntimeError(
            f"{result.identity.strategy_id}/{result.identity.quarantine_arm} has ambiguity measurements "
            f"{sorted(arm for arm in by_arm if arm is not None)} rather than both declared arms"
        )
    sharpes: dict[str, float | None] = {}
    for ambiguity in AMBIGUITY_ARM_ORDER:
        outcome = by_arm[ambiguity].namespaces.get(result.identity.namespace)
        if outcome is None:
            raise RuntimeError(
                f"{result.identity.strategy_id}/{ambiguity}/{result.identity.quarantine_arm} has no "
                f"{result.identity.namespace} measurement for a row built from that namespace"
            )
        sharpe = outcome.metrics.sharpe
        if sharpe is None or not math.isfinite(sharpe):
            sharpes[ambiguity] = None
            break
        sharpes[ambiguity] = sharpe
    return AmbiguityRecord(
        ambiguity_rule_version=AMBIGUITY_RULE_VERSION,
        comparison_basis="arm_sharpes",
        best_case_sharpe=sharpes.get("best_case"),
        worst_case_sharpe=sharpes.get("worst_case"),
    )


def _ambiguity_material_for(
    arms: Sequence[ArmMeasurement],
    result: StrategyResult,
) -> bool | None:
    """Return §3.4's pair verdict without inventing a comparison threshold.

    Non-level strategies carry one shared measurement which is copied to the
    two stored identities, so its gap is provably zero. A level strategy has two
    real measurements. Equal numeric Sharpes also prove a zero gap; unequal or
    unpriced Sharpes need the random cohort's 95th-percentile gap. This runner
    does not attach that cohort yet, so the only honest verdict is ``None`` and
    the promotion gate stays closed.

    ⚠ ONE DEFINITION, SHARED WITH THE TRANSITION (#2625). The verdict is derived
    from the same record that gets frozen, by the same pure function
    ``promote_strategy`` calls on the way back out. A second hand-written copy
    of §3.4 here is exactly the drift the extraction prevents.
    """
    return ambiguity_verdict(_ambiguity_record_for(arms, result))


def _expected_refusals(
    *,
    holdout_requested: bool,
    deflated: bool,
    purpose: StrategyPurpose = "capital_candidate",
    ambiguity_material: bool | None = False,
    prior_holdout_evaluations: int = 0,
    synthetic_control: SyntheticControl | None = None,
) -> frozenset[PromotionRefusal]:
    """What §9's table says a row from this run must still refuse on.

    ⚠⚠ THE THREE SYNTHETIC-CONTROL CODES ARE PREDICTED HERE AND DECIDED IN
    ``check_promotable``, AND THE DUPLICATION IS THE POINT. Criterion 8 requires
    the refusal list re-measured on every stored row and equal to §9's table;
    that check is only worth running while the two sides are computed
    independently. A helper shared with the gate would make the comparison
    vacuous — it would agree with itself.

    ⚠⚠ ``holdout_never_evaluated`` IS A PROPERTY OF THE STRATEGY VERSION, NOT OF
    THIS INVOCATION (#2433). ``check_promotable`` derives it from the LEDGER —
    stored hold-out rows and recorded ``evaluate`` accesses for the
    ``(strategy_id, strategy_version)`` pair — so predicting it from
    ``holdout_requested`` alone is only right on a FIRST run. Once a version has
    been hold-out evaluated, an in-sample-only re-run legitimately does not carry
    the refusal, and this function used to insist it did.

    It was unreachable until #2426: ``assert_no_existing_results`` refused any
    re-run whose ``result_version`` already existed, so a second invocation never
    reached the check. Adding ``benchmark_rule`` to the identity hash moved every
    version, made a re-run possible for the first time, and the corrected
    buy-and-hold run rejected here after a full corpus pass.
    """
    expected = set(STANDING_REFUSALS)
    if purpose == "harness_validation":
        expected.add("harness_validation_only")
    if not holdout_requested and prior_holdout_evaluations <= 0:
        expected.add("holdout_never_evaluated")
    if not deflated:
        expected.update({"deflated_sharpe_not_computed", "trial_count_undeclared"})
    if ambiguity_material is None:
        expected.add("ambiguity_arms_not_compared")
    elif ambiguity_material:
        expected.add("ambiguity_material")
    if synthetic_control is None:
        expected.add("synthetic_control_not_run")
    else:
        if not synthetic_control.mean_return_ci_contains_zero:
            expected.add("synthetic_control_cohort_shows_edge")
        if not synthetic_control.sharpe_exceeds_cohort:
            expected.add("synthetic_control_sharpe_below_cohort")
    return frozenset(expected)


def _end_read_phase(conn: psycopg.Connection[Any]) -> None:
    """Close the implicit read transaction so the next phase holds no locks.

    ⚠ ROLLBACK AND NOT COMMIT, AND ONLY WHERE NOTHING HAS BEEN WRITTEN. Every
    call site is a boundary between two read-only phases, so there is no work to
    keep; ``rollback`` says that, and it also cannot flush a caller's pending
    write if the opt-in guard in ``run_backtest`` is ever weakened.

    ⚠ A ``psycopg`` connection with ``autocommit=False`` opens a transaction on
    its FIRST statement and holds every lock that statement took until the
    transaction ends — so a single ``SELECT`` on ``strategy_results_store``
    keeps an ``AccessShareLock`` on it for the rest of the run. Measured on the
    dev database 2026-08-13: the lock is present after the read and gone after
    the rollback (#2628).
    """
    if conn.info.transaction_status != psycopg.pq.TransactionStatus.IDLE:
        conn.rollback()


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
    evaluation_window: Window | None = None,
    manifest: Mapping[str, StrategyEntry] = STRATEGY_MANIFEST,
    progress: ProgressCallback | None = None,
    synthetic_control: bool = False,
    release_read_locks: bool = False,
) -> BacktestRunReport:
    """Evaluate every runnable strategy and persist criterion 9's arm pairs.

    ⚠⚠ ``synthetic_control`` IS A BOOLEAN AND NOT A COHORT SIZE (#2601).
    ``SPEC_COHORT_SIZE`` is §9's own literal — *"a ``SPEC_`` literal and not a
    tuning knob: the 95th percentile of a 1,000-member sample is its 950th order
    statistic"* — so an invocation may choose whether to run the control and may
    NOT choose how big it is. A 200-member cohort stored under the same
    ``model_id`` would be a different estimator wearing the same name.

    ⚠ IT DEFAULTS OFF, AND THE COST IS WHY (§9, and see
    ``SYNTHETIC_CONTROL_BUDGET``). Every other phase of this job is one corpus
    pass; the control is ``SPEC_COHORT_SIZE`` equity curves PER ARM on top of
    it. An operator who wants promotable evidence asks for it; a run that only
    wants the metric set does not pay for it and says ``synthetic_control_not_run``.

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

    ⚠⚠ ``release_read_locks`` DEFAULTS OFF AND IS AN OPT-IN FOR A REASON (#2628).
    A ``psycopg`` connection with ``autocommit=False`` opens one transaction at
    its first statement, so without this flag the ``AccessShareLock`` that §10's
    pre-flight takes on ``strategy_results_store`` is held for the WHOLE run.
    Measured 2026-08-12: a migration on that relation waited behind a run for ten
    minutes having burned 0.31s of CPU, and a *pending* ``AccessExclusiveLock``
    queues ahead of new readers — so a waiting ``ALTER TABLE`` stalls the running
    dev stack rather than politely taking its turn, which is why
    ``tests/smoke/test_app_boots.py`` (the FastAPI lifespan runs migrations at
    boot) fails for purely environmental reasons while this job runs.

    Passing it asserts something the caller alone knows: **the connection is this
    invocation's to manage and carries no uncommitted work**. That is checked, not
    trusted — an open transaction at entry raises. It is off by default because
    ``scripts/verify_2429_total_return.py`` and
    ``scripts/benchmark_2488_evidence_refresh.py`` deliberately run the whole
    thing inside their OWN transaction and ``rollback`` it so the measurement
    never charges the trial register; a release would commit their rows.

    ⚠ WHAT IT DOES NOT CHANGE: the atomic grain. The write phase is wrapped in
    one ``conn.transaction()`` on both paths, so it stays a savepoint under a
    caller's transaction and becomes a single top-level transaction under the
    job — matching the invocation-atomicity ``refresh_recent`` already relies on
    (a partially written pinned window is refused as needing operator repair).
    """
    if release_read_locks and conn.info.transaction_status != psycopg.pq.TransactionStatus.IDLE:
        # ⚠ The guard, not a comment, is what makes the opt-in safe: this
        # function rolls back at its phase boundaries, and a caller that had
        # pending work on the connection would silently lose it.
        raise RuntimeError(
            "release_read_locks was asked for on a connection that is already in a transaction — this run rolls "
            "back at its phase boundaries, so any work the caller has not committed would be discarded"
        )
    if trial_register_version is not None and trial_register_version != TRIAL_REGISTER.version:
        raise ValueError(
            f"invocation asserts trial register {trial_register_version!r} but the live register is "
            f"{TRIAL_REGISTER.version!r} — deflating against a register that has moved would put an M on the row "
            "that does not describe the search"
        )
    holdout_requested = _check_holdout_pairing(purpose=holdout_purpose, accessed_by=holdout_accessed_by)
    namespaces = _namespaces_for_window(
        holdout_requested=holdout_requested,
        evaluation_window=evaluation_window,
    )

    cohort_size = SPEC_COHORT_SIZE if synthetic_control else None
    if cohort_size is not None and holdout_requested:
        # ⚠ DECLARED IN THE RUN LOG, not left as a comment. An operator who
        # asked for §9's control and then reads `synthetic_control_not_run` on
        # half the rows is owed the reason at the moment it is decided.
        logger.info(
            "strategy_backtest_run: hold-out rows will carry synthetic_control_not_run — %s",
            HOLDOUT_CONTROL_REASON,
        )
    runnable, excluded = runnable_strategies(manifest)
    if strategy_id is not None:
        if strategy_id not in runnable:
            raise ValueError(f"{strategy_id!r} is not a runnable manifest strategy; runnable today: {list(runnable)}")
        runnable = (strategy_id,)
    if not runnable:
        raise RuntimeError("no manifest strategy is runnable — every entry is blocked, so there is nothing to store")

    _emit_progress(progress, BacktestProgressEvent(phase="corpus"))
    corpus = load_corpus(conn, limit=limit, evaluation_window=evaluation_window)
    _emit_progress(
        progress,
        BacktestProgressEvent(
            phase="corpus",
            series_seen=len(corpus.pairs),
            series_total=len(corpus.pairs),
        ),
    )
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
            window_start=corpus.window.start,
            window_end=corpus.window.end,
            position_rule_set_version=POSITION_RULE_SET_VERSION,
            outcome_rule_set_version=OUTCOME_RULE_SET_VERSION,
            input_rule_set_version=QUARANTINE_RULE_SET_VERSION,
            return_basis=TOTAL_RETURN_BASIS,
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

    if release_read_locks:
        # ⚠ THE ONE BOUNDARY THAT MATTERS. Everything above reads the strategy
        # result relations (§10's pre-flight); nothing below does until the write
        # phase. Measured 2026-08-13 on dev: the evaluation phase's only reads are
        # ``research_price_daily``, ``research_bar_quarantine`` and
        # ``research_price_quarantine_coverage``, so releasing here leaves the
        # multi-hour pass holding no lock on any strategy table.
        _end_read_phase(conn)

    # 1. Evaluate every strategy x quarantine arm, holding metrics in memory.
    arms: list[ArmMeasurement] = []
    for entry_id in runnable:
        regime = _regime_for(manifest[entry_id], corpus.axis)
        for quarantine in QUARANTINE_ARM_ORDER:
            if regime.level_based:
                measurements = evaluate_level_arms(
                    conn,
                    manifest[entry_id],
                    corpus=corpus,
                    quarantine_arm=quarantine,
                    identity=identities[entry_id],
                    namespaces=namespaces,
                    progress=progress,
                    cohort_size=cohort_size,
                )
            else:
                measurements = (
                    evaluate_arm(
                        conn,
                        manifest[entry_id],
                        corpus=corpus,
                        quarantine_arm=quarantine,
                        ambiguity_arm=None,
                        identity=identities[entry_id],
                        namespaces=namespaces,
                        progress=progress,
                        cohort_size=cohort_size,
                    ),
                )
            for measurement in measurements:
                _assert_ambiguity_contract(measurement)
                arms.append(measurement)
                logger.info(
                    "strategy_backtest_run: %s/%s/%s evaluated %d series in %.1fs — %s, hold-out discarded %d",
                    entry_id,
                    measurement.ambiguity_arm or "shared",
                    quarantine,
                    measurement.series_evaluated,
                    measurement.elapsed_s,
                    {name: outcome.position_count for name, outcome in measurement.namespaces.items()},
                    measurement.holdout_positions_discarded,
                )
            if release_read_locks:
                # An arm is a full corpus pass, and the arms are independent —
                # so this bounds the corpus-table read transaction to one pass
                # instead of the whole run, and lets vacuum advance between them.
                _end_read_phase(conn)

    # 2. Deflate once per (namespace, quarantine arm) group.
    _emit_progress(progress, BacktestProgressEvent(phase="deflation"))
    by_group: dict[tuple[ResultNamespace, AmbiguityArm, QuarantineArm], dict[str, NamespaceMeasurement]] = {}
    for ambiguity in AMBIGUITY_ARM_ORDER:
        for measurement in arms:
            if measurement.ambiguity_arm not in {None, ambiguity}:
                continue
            for name, outcome in measurement.namespaces.items():
                by_group.setdefault((name, ambiguity, measurement.quarantine_arm), {})[measurement.strategy_id] = (
                    outcome
                )
    deflations: dict[tuple[ResultNamespace, AmbiguityArm, QuarantineArm], _Deflation | None] = {}
    refusals: dict[str, str] = {}
    for group, measurements in by_group.items():
        deflation, reason = deflate_group(measurements)
        deflations[group] = deflation
        if reason is not None:
            refusals[f"{group[0]}/{group[1]}/{group[2]}"] = reason
            logger.warning(
                "strategy_backtest_run: no Deflated Sharpe for %s/%s/%s — %s",
                group[0],
                group[1],
                group[2],
                reason,
            )

    # 3. Write, one transaction per arm pair.
    _emit_progress(progress, BacktestProgressEvent(phase="write"))
    validated = frozenset(corpus.universe)
    # ⚠ THE WRITE PHASE IS ONE EXPLICIT TRANSACTION, on both paths. Under a
    # caller's transaction this is the SAVEPOINT it always was; under
    # ``release_read_locks`` the connection is idle here, so without it each pair
    # writer's ``conn.transaction()`` would become top-level and a run that died
    # part-way would leave a PARTIAL pinned window committed — which
    # ``_recent_evidence_completion`` refuses as needing operator repair. The
    # completeness assertion is inside for the same reason: it must be able to
    # discard the rows it rejects.
    with conn.transaction():
        written = _write_rows(
            conn,
            arms=arms,
            deflations=deflations,
            validated=validated,
            holdout_requested=holdout_requested,
            holdout_purpose=holdout_purpose,
            holdout_accessed_by=holdout_accessed_by,
            corpus=corpus,
            strategy_purposes={strategy_id: entry.purpose for strategy_id, entry in manifest.items()},
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


def _namespaces_for_window(
    *,
    holdout_requested: bool,
    evaluation_window: Window | None,
) -> tuple[ResultNamespace, ...]:
    """Bind custom windows to audited hold-out evidence and nothing else.

    The frozen 1962–2026 run keeps its original namespace behaviour. A custom
    window is deliberately narrower: it may only inspect dates from the hold-out
    side of the frozen boundary and cannot produce an ``in_sample`` row under a
    different date range. The public runner only supplies registered windows.
    """
    if evaluation_window is None:
        return ("in_sample", "hold_out") if holdout_requested else ("in_sample",)
    if evaluation_window.start < HOLDOUT_BOUNDARY:
        raise ValueError(
            f"a custom recent-evidence window must start on or after the frozen hold-out boundary "
            f"{HOLDOUT_BOUNDARY}; got {evaluation_window.start}"
        )
    if not holdout_requested:
        raise ValueError("a custom recent-evidence window is hold-out evidence and requires an audited access")
    return ("hold_out",)


def _assert_ambiguity_contract(measurement: ArmMeasurement) -> None:
    """Shared measurements cannot contain ambiguity; bounded arms must resolve it."""
    count = measurement.close_sources.get("ambiguous", 0)
    if count:
        raise RuntimeError(
            f"{measurement.strategy_id}/{measurement.ambiguity_arm or 'shared'}/{measurement.quarantine_arm} "
            f"closed {count} position(s) as ambiguous after arm resolution — each bounded arm must carry an "
            "explicit stop or target price, while a shared non-level measurement cannot reach ambiguity"
        )


def _control_for(measurement: ArmMeasurement, namespace: ResultNamespace) -> SyntheticControl | None:
    """This arm's control, on the ONE namespace it was built for.

    ⚠ A hold-out row gets ``None`` even on a run that computed a control, and
    that is not a gap: the cohort was placed into in-sample bars against the
    in-sample sleeve's holds, so attaching it to a hold-out row would put an
    in-sample null beside a withheld Sharpe and let the pair pass a threshold
    neither side measured. ``HOLDOUT_CONTROL_REASON`` records why no hold-out
    cohort is built at all.
    """
    if namespace != CONTROL_NAMESPACE or measurement.cohort is None:
        return None
    return measurement.cohort.control


def _write_rows(
    conn: psycopg.Connection[Any],
    *,
    arms: Sequence[ArmMeasurement],
    deflations: Mapping[tuple[ResultNamespace, AmbiguityArm, QuarantineArm], _Deflation | None],
    validated: frozenset[int],
    holdout_requested: bool,
    holdout_purpose: str | None,
    holdout_accessed_by: str | None,
    corpus: _Corpus,
    strategy_purposes: Mapping[str, StrategyPurpose],
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

    ⚠⚠ EVERY SPLIT IS CUT BEFORE THE FIRST INSERT, for the same reason. Cutting
    folds is pure, so a construction that cannot produce one — an empty
    population, an axis too short to carry ``FOLD_COUNT`` blocks — must fail
    with ZERO rows written rather than half way through, leaving results whose
    folds can never be attached: ``assert_no_existing_results`` refuses the
    re-run that would fix them, and there is no repair path.

    ⚠ THE FOLDS GO IN THE PAIR'S OWN TRANSACTION. A nested ``conn.transaction()``
    is a SAVEPOINT (measured on psycopg 3.3.3 — an inner failure unwinds the
    whole outer block), so a stored in-sample result and its split stand or fall
    together and "a result row whose split silently never landed" is not
    reachable.

    ⚠ THE PAIR IS THE UNWIND GRAIN, NOT THE COMMIT GRAIN — corrected 2026-08-13
    (#2628). This once read *"a failure at pair 7 still leaves 6 committed"*.
    That was never true: ``run_backtest`` wraps this call in a transaction of its
    own, so every ``conn.transaction()`` here is a savepoint and a failure at
    pair 7 discards all seven. The invocation is the commit grain, which is what
    ``refresh_recent`` relies on — it treats a window as the restart boundary and
    refuses a partially written one as needing operator repair.
    """
    by_strategy: dict[str, dict[tuple[AmbiguityArm | None, QuarantineArm], ArmMeasurement]] = {}
    for measurement in arms:
        key = (measurement.ambiguity_arm, measurement.quarantine_arm)
        if key in by_strategy.setdefault(measurement.strategy_id, {}):
            raise RuntimeError(f"{measurement.strategy_id}/{key} produced two measurements")
        by_strategy[measurement.strategy_id][key] = measurement

    pending: list[tuple[str, ResultNamespace, AmbiguityArm, StrategyResult, StrategyResult]] = []
    for strategy_id in sorted(by_strategy):
        measured = by_strategy[strategy_id]
        for ambiguity in AMBIGUITY_ARM_ORDER:
            masked_arm = measured.get((ambiguity, "masked")) or measured.get((None, "masked"))
            admitted_arm = measured.get((ambiguity, "admitted")) or measured.get((None, "admitted"))
            if masked_arm is None or admitted_arm is None:
                raise RuntimeError(
                    f"{strategy_id}/{ambiguity} produced {sorted(measured)} rather than both quarantine arms"
                )
            for namespace in sorted(set(masked_arm.namespaces) & set(admitted_arm.namespaces)):
                masked = build_result(
                    masked_arm.namespaces[namespace],
                    strategy_id=strategy_id,
                    strategy_version=masked_arm.strategy_version,
                    purpose=strategy_purposes[strategy_id],
                    ambiguity_arm=ambiguity,
                    quarantine_arm="masked",
                    deflated=_deflated_for(
                        masked_arm.namespaces[namespace],
                        deflations.get((namespace, ambiguity, "masked")),
                    ),
                    evaluation_window=corpus.window,
                    synthetic_control=_control_for(masked_arm, namespace),
                )
                admitted = build_result(
                    admitted_arm.namespaces[namespace],
                    strategy_id=strategy_id,
                    strategy_version=admitted_arm.strategy_version,
                    purpose=strategy_purposes[strategy_id],
                    ambiguity_arm=ambiguity,
                    quarantine_arm="admitted",
                    deflated=_deflated_for(
                        admitted_arm.namespaces[namespace],
                        deflations.get((namespace, ambiguity, "admitted")),
                    ),
                    evaluation_window=corpus.window,
                    synthetic_control=_control_for(admitted_arm, namespace),
                )
                pending.append((strategy_id, namespace, ambiguity, masked, admitted))

    # ⚠ Read ONCE, before anything is written, so the preflight prediction and
    # the post-write re-measure share a source (#2433).
    prior_holdout: dict[tuple[str, str], int] = {}
    for _strategy_id, _namespace, _ambiguity, masked, _admitted in pending:
        key = (masked.identity.strategy_id, masked.identity.strategy_version)
        if key not in prior_holdout:
            prior_holdout[key] = holdout_access_counts(conn, *key).holdout_evaluations
    _preflight_gate(
        pending,
        arms=arms,
        validated=validated,
        holdout_requested=holdout_requested,
        prior_holdout=prior_holdout,
    )
    splits = _cut_splits(arms, corpus=corpus)
    _assert_every_in_sample_row_has_a_split(pending, splits)

    stored: list[tuple[int, StrategyResult, int]] = []
    for strategy_id, namespace, ambiguity, masked, admitted in pending:
        if namespace == "hold_out":
            assert holdout_purpose is not None and holdout_accessed_by is not None
            # ⚠ The universe record joins the pair's own transaction as a
            # savepoint member (the pair writer's ``conn.transaction()`` nests),
            # so "a result row without its frozen universe" is unreachable
            # through this writer rather than merely refused later (#2621).
            with conn.transaction():
                ids = store_holdout_arm_pair(
                    conn,
                    masked,
                    admitted,
                    accessed_by=holdout_accessed_by,
                    purpose=holdout_purpose,
                )
                for result_id, result in zip(ids, (masked, admitted), strict=True):
                    _store_universe_record(conn, result_id, result, arms=arms, validated=validated)
                    _store_ambiguity_record(conn, result_id, result, arms=arms)
            stored.extend((result_id, result, 0) for result_id, result in zip(ids, (masked, admitted), strict=True))
            continue
        # ⚠ ONE SPLIT PER ARM, NOT PER PAIR. The two rows of a pair differ in
        # exactly the quarantine arm, and the arm is what moves the population —
        # so each row is cut over its OWN observations. Both ambiguity arms of
        # one quarantine arm share a split because they share a measurement
        # (``build_result`` is handed the same ``NamespaceMeasurement`` twice),
        # which is why the four in-sample rows of a strategy carry two distinct
        # censuses and not four.
        with conn.transaction():
            ids = store_in_sample_arm_pair(conn, masked, admitted)
            for result_id, result in zip(ids, (masked, admitted), strict=True):
                split_key = (strategy_id, ambiguity, result.identity.quarantine_arm)
                shared_key = (strategy_id, None, result.identity.quarantine_arm)
                folds = store_walk_forward_folds(
                    conn,
                    result_id,
                    splits.get(split_key) or splits[shared_key],
                )
                _store_universe_record(conn, result_id, result, arms=arms, validated=validated)
                _store_ambiguity_record(conn, result_id, result, arms=arms)
                stored.append((result_id, result, folds))

    # Criterion 8 — RE-MEASURED on every written row, with the hold-out counts
    # read back from the database rather than projected. ⚠ The counts are per
    # ``(strategy_id, strategy_version)`` and every row of one strategy reads the
    # same pair, so they are cached rather than re-queried 8 times per strategy.
    written: list[WrittenRow] = []
    counts_cache: dict[tuple[str, str], tuple[int, int]] = {}
    #: One §3.4 comparison per (strategy_id, quarantine_arm, namespace); both
    #: ambiguity arms of that key must freeze the same record. See the check below.
    ambiguity_by_comparison: dict[tuple[str, str, str], AmbiguityRecord] = {}
    for result_id, result, folds_written in stored:
        identity = result.identity
        key = (identity.strategy_id, identity.strategy_version)
        if key not in counts_cache:
            counts = holdout_access_counts(conn, *key)
            counts_cache[key] = (counts.holdout_evaluations, counts.recorded_accesses)
        evaluations, accesses = counts_cache[key]
        # ⚠ READ BACK, not taken from memory — the same argument as the frozen
        # universe record directly below. Deriving the write-time verdict from
        # the row the TRANSITION will load is what proves the two agree; a
        # divergence (a constraint that rejected a value, a hash that does not
        # round-trip) then fails this run loudly instead of surfacing months
        # later as a result nobody can promote.
        ambiguity_record = load_result_ambiguity(conn, result_id)
        if ambiguity_record is None:
            raise RuntimeError(
                f"{identity.strategy_id} {identity.namespace}/{identity.quarantine_arm} stored without its frozen "
                "ambiguity record — the writer must freeze the gate's inputs in the pair's own transaction"
            )
        # ⚠⚠ SIBLING CONSISTENCY, ENFORCED AT WRITE TIME AND NOT ONLY IN A TEST.
        # The verdict is a function of (strategy_id, quarantine_arm, namespace)
        # and NOT of `ambiguity_arm`, so the two rows differing solely in their
        # ambiguity arm are two views of ONE §3.4 comparison. If they ever froze
        # different records, `promote_strategy` would admit whichever of the pair
        # happened to pass — so the divergence must fail the run that caused it,
        # not wait to be noticed at a promotion months later.
        comparison_key = (identity.strategy_id, identity.quarantine_arm, identity.namespace)
        seen_record = ambiguity_by_comparison.setdefault(comparison_key, ambiguity_record)
        if seen_record != ambiguity_record:
            raise RuntimeError(
                f"{identity.strategy_id} {identity.namespace}/{identity.quarantine_arm} froze ambiguity record "
                f"{ambiguity_record} against {seen_record} for the same comparison — the two ambiguity arms of one "
                "quarantine arm share a §3.4 verdict and cannot disagree"
            )
        ambiguity_material = ambiguity_verdict(ambiguity_record)
        # ⚠ The universe inputs are READ BACK from the frozen record, not taken
        # from memory — the same argument that reads the hold-out counts off the
        # database. The re-measure then verifies exactly what the promotion
        # transition will load (#2621); a missing or divergent record makes the
        # refusal cross-check below fail loudly instead of surfacing months
        # later as an unpromotable row.
        record = load_result_universe(conn, result_id)
        if record is None:
            raise RuntimeError(
                f"{identity.strategy_id} {identity.namespace}/{identity.quarantine_arm} stored without its frozen "
                "universe record — the writer must freeze the gate's inputs in the pair's own transaction"
            )
        outcome = check_promotable(
            _candidate(
                result,
                validated=record.validated_universe_ids,
                evaluated=record.evaluated_instrument_ids,
                holdout_evaluations=evaluations,
                recorded_accesses=accesses,
                ambiguity_material=ambiguity_material,
            )
        )
        # ⚠ The SAME ``evaluations`` the outcome was derived from. Predicting the
        # expectation from a different source than the actual is exactly the
        # mismatch #2433 was.
        expected = _expected_refusals(
            holdout_requested=holdout_requested,
            deflated=result.deflated is not None,
            purpose=result.purpose,
            ambiguity_material=ambiguity_material,
            prior_holdout_evaluations=evaluations,
            synthetic_control=result.synthetic_control,
        )
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
                folds_written=folds_written,
            )
        )
    return tuple(written)


def _assert_every_in_sample_row_has_a_split(
    pending: Sequence[tuple[str, ResultNamespace, AmbiguityArm, StrategyResult, StrategyResult]],
    splits: Mapping[tuple[str, AmbiguityArm | None, QuarantineArm], WalkForwardFolds],
) -> None:
    """Every in-sample row about to be written has a split waiting for it.

    ⚠⚠ THE COVERAGE IS COMPLETE BY CONSTRUCTION AND CHECKED ANYWAY, BEFORE THE
    FIRST INSERT. ``pending`` and ``splits`` are both derived from ``arms`` under
    the same condition — a row exists only where BOTH arms carry an
    ``in_sample`` namespace, which is exactly when ``_cut_splits`` keys a split
    for each of them — so a miss is unreachable today.

    It is asserted because the invariant spans two functions that a later change
    could move apart, and because of WHERE the failure would otherwise land: the
    lookup sits inside the per-pair transaction, after ``store_in_sample_arm_pair``
    has already inserted. The savepoint means no half-written pair survives, but
    the run would still abort with earlier pairs committed and no way to attach
    their folds afterwards (#2423). Checking here turns that into a refusal with
    ZERO rows written — the same argument ``_preflight_gate`` above makes for
    criterion 8's refusal list.
    """
    missing = sorted(
        {
            (strategy_id, ambiguity, result.identity.quarantine_arm)
            for strategy_id, namespace, ambiguity, masked, admitted in pending
            if namespace == "in_sample"
            for result in (masked, admitted)
            if (strategy_id, ambiguity, result.identity.quarantine_arm) not in splits
            and (strategy_id, None, result.identity.quarantine_arm) not in splits
        }
    )
    if missing:
        raise RuntimeError(
            f"no walk-forward split was cut for {missing} — every in-sample row is written with its split in one "
            "transaction, so a missing one would surface after the pair had already been inserted and could never "
            "be attached afterwards"
        )


def _cut_splits(
    arms: Sequence[ArmMeasurement],
    *,
    corpus: _Corpus,
) -> dict[tuple[str, AmbiguityArm | None, QuarantineArm], WalkForwardFolds]:
    """Criterion 5's split for each ``(strategy, quarantine arm)`` in-sample population.

    ⚠ A hold-out measurement contributes nothing and is skipped rather than
    refused: an invocation that requested both namespaces still cuts folds only
    on the in-sample side, which is the whole of ``walk_forward``'s scope.
    """
    splits: dict[tuple[str, AmbiguityArm | None, QuarantineArm], WalkForwardFolds] = {}
    for measurement in arms:
        outcome = measurement.namespaces.get("in_sample")
        if outcome is None:
            continue
        split = build_in_sample_split(
            outcome.label_starts,
            outcome.label_ends,
            axis=corpus.in_sample_axis,
            bar_counts=corpus.in_sample_bar_counts,
        )
        logger.info(
            "strategy_backtest_run: %s/%s split %s over %d observation(s) of %d in-sample position(s) — "
            "embargo %s, purged %s, embargoed %s",
            measurement.strategy_id,
            measurement.quarantine_arm,
            split.model_id,
            split.observation_count,
            outcome.position_count,
            [record.embargo_bars for record in split.folds],
            [record.census.purged for record in split.folds],
            [record.census.embargoed for record in split.folds],
        )
        # ⚠⚠ A DUPLICATE KEY IS REFUSED, NOT OVERWRITTEN. ``run_backtest`` builds
        # ``arms`` one per ``(strategy, quarantine arm)`` so this cannot fire
        # today, but a plain assignment makes "two measurements of one arm" a
        # SILENT last-write-wins: the surviving split would be cut over one
        # population while the rows it lands on were measured over the other,
        # and nothing downstream could tell — the census would simply describe
        # observations the metrics beside it never saw. Same argument
        # ``WalkForwardFolds`` makes for a split assembled from more than one
        # run, one level up. Found by a Codex pass on the rebuttal, not by the
        # diff review.
        key = (measurement.strategy_id, measurement.ambiguity_arm, measurement.quarantine_arm)
        if key in splits:
            raise RuntimeError(
                f"{key} produced a second in-sample measurement — one arm is one population, and silently keeping "
                "the later split would attach a census to rows whose metrics were computed over the earlier one"
            )
        splits[key] = split
    return splits


def _evaluated_ids(arms: Sequence[ArmMeasurement], result: StrategyResult) -> frozenset[int]:
    for measurement in arms:
        if (
            measurement.strategy_id == result.identity.strategy_id
            and measurement.quarantine_arm == result.identity.quarantine_arm
            and measurement.ambiguity_arm in {None, result.identity.ambiguity_arm}
        ):
            outcome = measurement.namespaces.get(result.identity.namespace)
            if outcome is not None:
                return outcome.evaluated_instrument_ids
    raise RuntimeError(  # pragma: no cover - every stored row came from a measurement
        f"no measurement matches the stored row {result.identity.version}"
    )


def _store_ambiguity_record(
    conn: psycopg.Connection[Any],
    result_id: int,
    result: StrategyResult,
    *,
    arms: Sequence[ArmMeasurement],
) -> None:
    """Freeze the row's §3.4 comparison inputs in the pair's own transaction (#2625).

    ⚠ THE RECORD IS A FUNCTION OF (strategy_id, quarantine_arm, namespace) AND
    NOT OF ``ambiguity_arm`` — ``_ambiguity_record_for`` filters on the first
    three only. So the two result rows that differ solely in their ambiguity arm
    MUST receive identical records, and do, by construction. That is enforced
    twice, because the consequence of divergence is a promotion: the criterion-8
    loop in ``_write_rows`` compares every pair it reads back and raises, and
    ``tests/test_backtest_run.py`` pins the purity property that makes it hold.
    A future edit making the record depend on the arm would give one comparison
    two verdicts, and ``promote_strategy`` would admit whichever half passed.
    """
    store_result_ambiguity(
        conn,
        result_id=result_id,
        record=_ambiguity_record_for(arms, result),
    )


def _store_universe_record(
    conn: psycopg.Connection[Any],
    result_id: int,
    result: StrategyResult,
    *,
    arms: Sequence[ArmMeasurement],
    validated: frozenset[int],
) -> None:
    """Freeze the row's universe inputs in the pair's own transaction (#2621).

    The evaluated set comes from ``_evaluated_ids`` — the same source the
    write-time gate consumes — and the universe is the run's single
    ``load_validated_universe`` read (``corpus.universe``). The count assertion
    pins the record to the row it describes BEFORE the insert: the two are equal
    by construction today (both are views of one ``NamespaceMeasurement``), and
    a drift would otherwise surface only at the promotion transition, as an
    ``evaluated_universe_count_mismatch`` refusal on a row already committed.
    """
    evaluated = _evaluated_ids(arms, result)
    if len(evaluated) != result.evaluated_instrument_count:
        raise RuntimeError(
            f"{result.identity.version} would freeze {len(evaluated)} evaluated instruments against a row "
            f"claiming {result.evaluated_instrument_count} — the universe record must describe its own row"
        )
    store_result_universe(
        conn,
        result_id=result_id,
        record=ResultUniverseRecord(
            universe_rule_version=VALIDATED_UNIVERSE_RULE_VERSION,
            evaluated_instrument_ids=evaluated,
            validated_universe_ids=validated,
        ),
    )


def _preflight_gate(
    pending: Sequence[tuple[str, ResultNamespace, AmbiguityArm, StrategyResult, StrategyResult]],
    *,
    arms: Sequence[ArmMeasurement],
    validated: frozenset[int],
    holdout_requested: bool,
    prior_holdout: Mapping[tuple[str, str], int],
) -> None:
    """The refusal list every row WILL carry, checked before anything is stored.

    The hold-out counts are projected: one ``evaluate`` record per hold-out row
    (``store_holdout_arm_pair`` writes one per arm, and ``sql/264``'s trigger
    refuses a row without one), so the two counts are equal by construction and
    the gate's ``recorded_accesses < holdout_evaluations`` clause cannot fire on
    a row this job wrote.

    ⚠⚠ THE PROJECTION STARTS FROM WHAT THE LEDGER ALREADY HOLDS (#2433), not
    from zero. A strategy version hold-out evaluated by an EARLIER run carries
    those rows into ``check_promotable`` at write time, so a projection counting
    only this run's pending rows predicts a refusal set the stored rows will not
    have — and a gate whose prediction is wrong cannot catch the mismatch it
    exists for.
    """
    projected: Counter[tuple[str, str]] = Counter(prior_holdout)
    for _strategy_id, namespace, _ambiguity, masked, admitted in pending:
        if namespace == "hold_out":
            for result in (masked, admitted):
                projected[(result.identity.strategy_id, result.identity.strategy_version)] += 1
    for _strategy_id, _namespace, _ambiguity, masked, admitted in pending:
        for result in (masked, admitted):
            count = projected[(result.identity.strategy_id, result.identity.strategy_version)]
            ambiguity_material = _ambiguity_material_for(arms, result)
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
                    ambiguity_material=ambiguity_material,
                )
            )
            expected = _expected_refusals(
                holdout_requested=holdout_requested,
                deflated=result.deflated is not None,
                purpose=result.purpose,
                ambiguity_material=ambiguity_material,
                prior_holdout_evaluations=prior_holdout.get(
                    (result.identity.strategy_id, result.identity.strategy_version), 0
                ),
                synthetic_control=result.synthetic_control,
            )
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

    ⚠ Against the RUNNABLE set, never against the manifest. A future level
    strategy lacking an outcome producer is an exclusion, not a failure; the
    two are distinguished by the runnable computation itself. Same construction
    §3.1 landed after the review bot found its population gate anchored on the
    wrong side.
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
    # ⚠ Criterion 5's split, checked on BOTH sides. An in-sample row without its
    # ``FOLD_COUNT`` folds is a validity gate that did not run, and a hold-out
    # row WITH folds is a cross-validation of the withheld side that nobody ran
    # — the claim ``sql/269``'s trigger exists to make unstorable. Neither is
    # refused by ``check_promotable`` (§8's "no promotion refusal is added"), so
    # the run has to be what notices.
    for row in report.rows:
        wanted = FOLD_COUNT if row.namespace == "in_sample" else 0
        if row.folds_written != wanted:
            raise RuntimeError(
                f"{row.strategy_id} {row.namespace}/{row.ambiguity_arm}/{row.quarantine_arm} stored "
                f"{row.folds_written} fold(s) against {wanted} — an in-sample result carries the whole split or the "
                "gate did not run, and a hold-out result carries none"
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
        cohort = measurement.cohort
        if cohort is None:
            # ⚠ SAID, not omitted. §11's report is what an operator reads to
            # find out why a row refuses, and "the control does not appear in
            # the report" is indistinguishable from "the control was lost".
            logger.info("    synthetic control: NOT RUN — the invocation did not ask for one")
        else:
            logger.info(
                "    synthetic control %s/%s: %d members, %d series, %.1fs (%.3fs/member); "
                "mean %.3f%% CI [%.3f, %.3f] contains_zero=%s; sharpe p%.0f %.4f vs strategy %.4f exceeds=%s; "
                "passed=%s; exposure %+.2fpp turnover %+.3f; unmatchable=%s no_slack_series=%d",
                cohort.control.model_id,
                cohort.placement_space_id,
                cohort.control.cohort_size,
                cohort.series_placed,
                cohort.elapsed_s,
                cohort.seconds_per_member,
                cohort.control.mean_return_pct,
                cohort.control.mean_return_ci_low_pct,
                cohort.control.mean_return_ci_high_pct,
                cohort.control.mean_return_ci_contains_zero,
                cohort.control.sharpe_percentile,
                cohort.control.cohort_sharpe_threshold,
                cohort.control.strategy_sharpe,
                cohort.control.sharpe_exceeds_cohort,
                cohort.control.passed,
                cohort.residual.exposure_delta_pct_points,
                cohort.residual.turnover_delta,
                dict(sorted(cohort.unmatchable.items())),
                cohort.no_slack_series,
            )
    for group, reason in sorted(report.deflation_refusals.items()):
        logger.warning("  no Deflated Sharpe for %s: %s", group, reason)
    for row in report.rows:
        logger.info(
            "    stored %s %s/%s/%s %s instruments=%d folds=%d refusals=%s",
            row.strategy_id,
            row.namespace,
            row.ambiguity_arm,
            row.quarantine_arm,
            row.result_version,
            row.evaluated_instrument_count,
            row.folds_written,
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
    "build_in_sample_split",
    "build_result",
    "deflate_group",
    "evaluate_arm",
    "load_corpus",
    "log_report",
    "run_backtest",
    "runnable_strategies",
]
