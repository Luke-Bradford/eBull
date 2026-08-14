"""The strategy manifest — enumeration that cannot be forgotten (#2394 §2).

Spec: ``docs/proposals/ta/2026-08-08-strategy-runner-and-manifest.md`` §2.
Registry contract: ``app/services/strategy_registry.py`` (phase 3a). Refs #2240.

THE DEFECT THIS CLOSES
----------------------
There was no enumeration of strategies anywhere — no ``__all__``, no registry
list. Every caller imported modules by name, so the strategy set was whatever a
given script happened to import. Measured on this tree by
``scripts/verify_2394_strategy_manifest.py --census`` (nothing here is
hardcoded): the per-module import counts are lopsided, which is why every
phase-5 run report carries S-1 and S-3 figures and not S-2 and S-4. **Not a
decision — an import list.** Adding S-5 means finding every one of those sites,
and forgetting one produces a silently smaller population rather than an error,
which is criterion 9's *"exclusion is visible rather than assumed harmless"*
failing one layer up from where it was fixed.

⚠⚠ THIS IS A SEPARATE MODULE AND **NOT** ``strategy_registry.py``, WHICH IS
WHERE SPEC §2 PUT IT. Three reasons, the first of which is measurable:

1. ``StrategyIdentity.version`` hashes ``_module_hash()`` — the bytes of
   ``strategy_registry.py``. A manifest living there would move **every stored
   strategy version every time a strategy is added**, so registering S-5 would
   invalidate S-1's entire track record for no reason of S-1's. This module
   leaves those bytes untouched, and that is checkable rather than asserted:
   ``verify_2394_strategy_manifest.py --identity`` diffs the file against
   ``origin/main`` and re-derives all four versions.
2. The registry is imported **by** every strategy module, so it cannot import
   them back. A bottom-of-file import would work only by accident of ordering.
3. ``ExitRegime`` lives in ``position_builder``. Putting the manifest in the
   registry would couple the pure signal contract to the position layer, and
   every strategy module would then import the backtester transitively.

⚠⚠ AND IT IS NOT INSIDE ``app/services/strategies/`` EITHER, WHICH IS WHERE IT
WAS FIRST WRITTEN. ``tests/test_strategy_registry.py::TestInputRuleSetsAre
Complete`` walks every module in that package and requires any imported
``app.services`` module carrying a ``RULE_SET_VERSION`` to be inside
``INPUT_RULE_SETS``. This module imports ``position_builder``, which has one — so
the guard failed, correctly, on the first run of the fast tier.

The two obvious answers are both wrong. Adding ``position_builder`` to
``INPUT_RULE_SETS`` would move every strategy identity, which is reason 1 above
happening by another route. Excluding this file from the walk would weaken a
guard that is working. The manifest is not a strategy — it computes no verdict —
so it belongs outside the package the guard scopes to, and the guard's reach is
left exactly as it was.

⚠ A CORRECTION TO SPEC §1, which says *"``STRATEGY_SET_ID`` … Adding or removing
a strategy changes the set, so the manifest is versioned by it."* It does not,
and must not: ``STRATEGY_SET_ID`` is a literal in the registry, and no field of
``StrategyIdentity`` is a function of manifest membership. S-1's signals are not
changed by S-5 existing, so S-1's version must not move when S-5 lands. The set
id names the **contract** version, not the membership.

⚠ WHAT THE MANIFEST DOES NOT CARRY: per-strategy *tuning*. No thresholds, no
windows, no cost overrides. That is #2333's stance inherited verbatim — *"a
field they must remember to fill is the same omission with a nicer name"*.

⚠ WHAT IT DOES CARRY, AND WHY THAT IS NOT TUNING: the OPERATIONAL CONTRACT. A
runner cannot invoke a strategy at all without knowing whether it is
``per_series`` or ``cross_sectional``, which ``signal_kind`` legs it emits, and
which exit regime resolves its outcomes. Omitting those forces the runner back
into per-strategy ``if`` branches — the same defect in a new location.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence, Set
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from types import MappingProxyType
from typing import Literal, Protocol, get_args

from app.services.indicator_series import BarSeries, Universe
from app.services.market_regime import RegimeSeries
from app.services.outcome_resolver import ExitLevels, UnresolvedReason
from app.services.position_builder import ExitRegime
from app.services.strategies.s1_time_series_momentum import S1_STRATEGY_ID, s1_identity, s1_signals
from app.services.strategies.s2_cross_sectional_momentum import (
    MIN_CROSS_SECTION,
    S2_STRATEGY_ID,
    rebalance_dates,
    s2_identity,
    s2_member,
    s2_select,
)
from app.services.strategies.s3_mean_reversion_in_trend import MAX_HOLD_BARS as S3_MAX_HOLD_BARS
from app.services.strategies.s3_mean_reversion_in_trend import S3_STRATEGY_ID, s3_identity, s3_signals
from app.services.strategies.s4_volatility_compression_breakout import MAX_HOLD_BARS as S4_MAX_HOLD_BARS
from app.services.strategies.s4_volatility_compression_breakout import (
    S4_STRATEGY_ID,
    s4_exit_bracket,
    s4_identity,
    s4_signals,
)
from app.services.strategies.s5_support_bounce import (
    MAX_HOLD_BARS as S5_MAX_HOLD_BARS,
)
from app.services.strategies.s5_support_bounce import (
    S5_STRATEGY_ID,
    s5_exit_bracket,
    s5_identity,
    s5_signals,
)
from app.services.strategies.s6_resistance_breakout import (
    MAX_HOLD_BARS as S6_MAX_HOLD_BARS,
)
from app.services.strategies.s6_resistance_breakout import (
    S6_STRATEGY_ID,
    s6_exit_bracket,
    s6_identity,
    s6_signals,
)
from app.services.strategies.s7_trend_pullback import (
    MAX_HOLD_BARS as S7_MAX_HOLD_BARS,
)
from app.services.strategies.s7_trend_pullback import (
    S7_STRATEGY_ID,
    s7_exit_bracket,
    s7_identity,
    s7_signals,
)
from app.services.strategies.s8_range_mean_reversion import (
    MAX_HOLD_BARS as S8_MAX_HOLD_BARS,
)
from app.services.strategies.s8_range_mean_reversion import (
    S8_STRATEGY_ID,
    s8_exit_bracket,
    s8_identity,
    s8_signals,
)
from app.services.strategies.s9_squeeze_expansion import (
    MAX_HOLD_BARS as S9_MAX_HOLD_BARS,
)
from app.services.strategies.s9_squeeze_expansion import (
    S9_STRATEGY_ID,
    s9_exit_bracket,
    s9_identity,
    s9_signals,
)
from app.services.strategies.s10_relative_strength_leader import (
    MIN_CROSS_SECTION as S10_MIN_CROSS_SECTION,
)
from app.services.strategies.s10_relative_strength_leader import (
    S10_STRATEGY_ID,
    s10_entry_member,
    s10_entry_select,
    s10_exit_member,
    s10_exit_select,
    s10_identity,
    s10_rebalance_dates,
)
from app.services.strategy_exit_levels_batch import s4_exit_levels_batch
from app.services.strategy_registry import (
    SIGNAL_KINDS,
    CrossSectionalMember,
    CrossSectionalSelect,
    NotEvaluableReason,
    SignalKind,
    StrategyIdentity,
    StrategySignal,
)

#: How the runner has to invoke this strategy. ``per_series`` takes one
#: instrument's bars; ``cross_sectional`` needs a whole panel on a date, because
#: "hold the top decile" is a statement about the cross-section and not about
#: any one member (see ``strategy_registry.evaluate_cross_sectional``).
StrategyClass = Literal["per_series", "cross_sectional"]
StrategyPurpose = Literal["harness_validation", "capital_candidate"]

#: ⚠ DERIVED, never restated — the closed-vocabulary-in-N-places defect the
#: registry already fixed with ``get_args`` (#2218).
STRATEGY_CLASSES: frozenset[str] = frozenset(get_args(StrategyClass))
STRATEGY_PURPOSES: frozenset[str] = frozenset(get_args(StrategyPurpose))


class IdentityFactory(Protocol):
    """``s*_identity``. Both arguments are required — criterion 11 puts the
    universe and the cost model *inside* the identity."""

    def __call__(self, *, universe: Universe, cost_model_id: str) -> StrategyIdentity: ...


class PerSeriesSignals(Protocol):
    """The uniform per-series call. Every leg the strategy emits, in one list.

    ⚠ ``masked_reason`` is one name for what the catalogue spells two ways:
    ``s1_signals``/``s3_signals`` take ``close_reason`` (they read only closes),
    ``s4_signals`` takes ``masked_reason`` (it reads all four OHLC fields). The
    adapters below are exactly where that difference is absorbed — a runner
    passing ``close_reason=`` to S-4 is a ``TypeError`` at call time, which is
    the 19-call-site problem wearing a keyword.
    """

    def __call__(
        self,
        series: BarSeries,
        *,
        universe: Universe,
        masked_reason: NotEvaluableReason,
        regime: RegimeSeries,
    ) -> list[StrategySignal]: ...

    # ⚠⚠ ``regime`` IS ON THE UNIFORM CALL, NOT ON THE STRATEGIES THAT USE IT.
    # S-1..S-4 ignore it; S-5..S-10 gate on it. Putting it here rather than
    # branching per strategy is the same choice this module already made for
    # ``masked_reason``: the adapters absorb the difference, and a runner cannot
    # forget to supply it for the one strategy that needs it.
    #
    # ⚠ The alternative — a ``requires_regime`` flag with a runner branch —
    # reintroduces exactly the per-strategy ``if`` this module exists to delete,
    # and the branch would be the thing nobody updates when S-11 arrives.
    #
    # ⚠ Safe for identities: ``StrategyIdentity.version`` hashes
    # ``strategy_registry.py``, NOT this module (see the module docstring,
    # reason 1). Editing the manifest moves no stored strategy version.


class MemberStager(Protocol):
    """The uniform cross-sectional call: one member's contribution to the panel.

    ⚠ This is the MEMBER entry point, not ``s2_signals``. ``s2_signals`` holds
    the whole panel in memory and says so; a full-corpus runner must stream one
    series at a time through this and ``select``, which is what
    ``StagedMember`` is public for.

    ⚠⚠ ``regime`` IS ON THE UNIFORM CALL — the same decision
    ``PerSeriesSignals`` records above, for the same reason: S-2 ignores it,
    S-10's entry leg gates on it, and the adapters absorb the difference so a
    runner cannot forget to supply it for the one member that needs it.
    """

    def __call__(
        self,
        series: BarSeries,
        *,
        panel_decision_dates: Set[date],
        universe: Universe,
        masked_reason: NotEvaluableReason,
        regime: RegimeSeries,
    ) -> CrossSectionalMember: ...


class DecisionCalendar(Protocol):
    """The panel dates this strategy may act on, from the panel's union calendar.

    ⚠ Returns ``None`` for a strategy with no calendar, rather than being absent
    for one. That is what makes the runner branch-free::

        regime = entry.exit_regime(entry.decision_calendar(calendar))

    "No calendar" and "a calendar with no dates" stay distinguishable, which is
    the same distinction ``ExitRegime.__post_init__`` already refuses to lose.
    """

    def __call__(self, calendar: Iterable[date]) -> frozenset[date] | None: ...


class ExitRegimeFactory(Protocol):
    """Which close sources this strategy declares — spec §3's table, executable.

    ⚠ The table exists today only as prose in ``ExitRegime``'s docstring, and
    every caller hand-builds the dataclass from it (``verify_2240_position_
    builder.py`` builds S-1's and S-3's; nothing builds S-2's or S-4's). A
    docstring cannot be iterated and cannot be wrong-in-CI, which is the same
    reason this manifest exists at all.
    """

    def __call__(self, decision_dates: frozenset[date] | None) -> ExitRegime: ...


class ExitLevelsFactory(Protocol):
    """A level strategy's causal bracket construction at one filled entry."""

    def __call__(
        self,
        series: BarSeries,
        *,
        signal_index: int,
        entry_price: Decimal,
        universe: Universe,
    ) -> ExitLevels | UnresolvedReason: ...


class ExitLevelsBatchFactory(Protocol):
    """A level strategy's brackets for several fills in one immutable series."""

    def __call__(
        self,
        series: BarSeries,
        *,
        requests: Sequence[tuple[int, Decimal]],
        universe: Universe,
    ) -> Sequence[ExitLevels | UnresolvedReason]: ...


@dataclass(frozen=True)
class CrossSectionalLeg:
    """One additional ranked leg of a cross-sectional strategy — S-10's exit.

    A LEG OBJECT rather than parallel ``exit_member`` / ``exit_select`` /
    ``exit_min_participants`` fields on ``StrategyEntry`` (Codex ckpt-1): the
    three are meaningless apart, and parallel optional columns are an
    increasingly unvalidated two-column structure the tagged-union
    ``__post_init__`` below would have to keep re-learning.

    ``min_participants`` is PER LEG deliberately — S-10 sets both legs to the
    same floor, but an exit band and an entry decile protect different things
    and a future strategy may need them apart.
    """

    member: MemberStager
    select: CrossSectionalSelect
    min_participants: int

    def __post_init__(self) -> None:
        if self.min_participants < 1:
            raise ValueError(f"min_participants must be at least 1, got {self.min_participants}")


@dataclass(frozen=True)
class StrategyEntry:
    """One registered strategy: how to identify it, how to invoke it, how it exits.

    ⚠ A TAGGED UNION, checked rather than documented. ``strategy_class`` selects
    which invocation fields must be present, and ``__post_init__`` refuses an
    entry that populates the other arm — an entry declaring ``per_series`` while
    carrying a ``select`` is a registration whose two halves disagree, and the
    runner would honour whichever half it happened to read.
    """

    strategy_id: str
    purpose: StrategyPurpose
    identity: IdentityFactory
    strategy_class: StrategyClass
    #: The legs this strategy emits. S-1/S-3 emit entry AND exit; S-2/S-4 entry
    #: only. A runner reading this knows whether an absent exit row is a missing
    #: write or the strategy's design.
    signal_kinds: frozenset[SignalKind]
    exit_regime: ExitRegimeFactory
    decision_calendar: DecisionCalendar
    #: ``per_series`` only.
    signals: PerSeriesSignals | None = None
    #: ``cross_sectional`` only, all three together.
    member: MemberStager | None = None
    select: CrossSectionalSelect | None = None
    min_participants: int | None = None
    #: ``cross_sectional`` only, optional — a SECOND ranked leg (S-10's exit).
    #: Present iff ``"exit"`` is among ``signal_kinds``: a cross-sectional
    #: strategy's exit verdicts can only come from a ranked leg, so declaring
    #: one without the other is a registration whose halves disagree.
    exit_leg: CrossSectionalLeg | None = None
    #: Level-based only. Its absence is a named runner exclusion rather than a
    #: silent max-hold fallback.
    exit_levels: ExitLevelsFactory | None = None
    #: Optional result-equivalent batch form. It may share immutable indicator
    #: work, never entry-specific prices or signal indices.
    exit_levels_batch: ExitLevelsBatchFactory | None = None

    def __post_init__(self) -> None:
        if not self.strategy_id.strip():
            raise ValueError("strategy_id must be a non-empty declaration")
        if self.purpose not in STRATEGY_PURPOSES:
            raise ValueError(f"unknown strategy purpose {self.purpose!r}; must be one of {sorted(STRATEGY_PURPOSES)}")
        if self.strategy_class not in STRATEGY_CLASSES:
            raise ValueError(
                f"unknown strategy class {self.strategy_class!r}; must be one of {sorted(STRATEGY_CLASSES)}"
            )
        # ⚠ A comprehension rather than set difference: ``signal_kinds`` is
        # typed ``frozenset[SignalKind]`` and ``SIGNAL_KINDS`` is the runtime
        # ``frozenset[str]``, and set operators are invariant in the element
        # type. The membership test is what is actually wanted — an untyped
        # caller can pass any string, which is the case being refused.
        unknown = {kind for kind in self.signal_kinds if kind not in SIGNAL_KINDS}
        if unknown:
            raise ValueError(f"unknown signal kinds {sorted(unknown)}; must be within {sorted(SIGNAL_KINDS)}")
        if "entry" not in self.signal_kinds:
            raise ValueError(
                f"{self.strategy_id} declares {sorted(self.signal_kinds)} and no entry leg — an exit-only "
                "strategy has no position to close, and outcome resolution consumes fired ENTRIES"
            )
        cross_sectional_fields = (self.member, self.select, self.min_participants)
        if self.strategy_class == "per_series":
            if self.signals is None:
                raise ValueError(f"{self.strategy_id} is per_series and declares no signals function")
            if any(field is not None for field in cross_sectional_fields) or self.exit_leg is not None:
                raise ValueError(
                    f"{self.strategy_id} is per_series but carries cross-sectional fields — the runner would "
                    "invoke whichever half it read first"
                )
        else:
            if self.signals is not None:
                raise ValueError(f"{self.strategy_id} is cross_sectional but carries a per-series signals function")
            if any(field is None for field in cross_sectional_fields):
                raise ValueError(
                    f"{self.strategy_id} is cross_sectional and must declare member, select and "
                    "min_participants together — a panel runner needs all three to rank anything"
                )
            if (self.exit_leg is not None) != ("exit" in self.signal_kinds):
                raise ValueError(
                    f"{self.strategy_id} declares signal_kinds {sorted(self.signal_kinds)} with "
                    f"exit_leg {'present' if self.exit_leg is not None else 'absent'} — a cross-sectional "
                    "exit verdict can only come from a ranked exit leg, so the two must agree"
                )
        if self.min_participants is not None and self.min_participants < 1:
            raise ValueError(f"min_participants must be at least 1, got {self.min_participants}")
        if self.exit_levels_batch is not None and self.exit_levels is None:
            raise ValueError(
                f"{self.strategy_id} declares batch exit levels without the scalar factory used as its oracle"
            )


def _no_decision_calendar(calendar: Iterable[date]) -> frozenset[date] | None:
    """A per-series strategy acts on every bar it is evaluable at, not on a calendar."""
    return None


def _s1_signals(
    series: BarSeries,
    *,
    universe: Universe,
    masked_reason: NotEvaluableReason,
    regime: RegimeSeries,  # noqa: ARG001 - uniform call; S-1 does not gate on regime
) -> list[StrategySignal]:
    return s1_signals(series, universe=universe, close_reason=masked_reason)


def _s3_signals(
    series: BarSeries,
    *,
    universe: Universe,
    masked_reason: NotEvaluableReason,
    regime: RegimeSeries,  # noqa: ARG001 - uniform call; S-3 does not gate on regime
) -> list[StrategySignal]:
    return s3_signals(series, universe=universe, close_reason=masked_reason)


def _s4_signals(
    series: BarSeries,
    *,
    universe: Universe,
    masked_reason: NotEvaluableReason,
    regime: RegimeSeries,  # noqa: ARG001 - uniform call; S-4 does not gate on regime
) -> list[StrategySignal]:
    return s4_signals(series, universe=universe, masked_reason=masked_reason)


def _s2_member(
    series: BarSeries,
    *,
    panel_decision_dates: Set[date],
    universe: Universe,
    masked_reason: NotEvaluableReason,
    regime: RegimeSeries,  # noqa: ARG001 - uniform call; S-2 does not gate on regime
) -> CrossSectionalMember:
    return s2_member(
        series,
        panel_rebalance_dates=panel_decision_dates,
        universe=universe,
        close_reason=masked_reason,
    )


def _s10_entry_member(
    series: BarSeries,
    *,
    panel_decision_dates: Set[date],
    universe: Universe,
    masked_reason: NotEvaluableReason,
    regime: RegimeSeries,
) -> CrossSectionalMember:
    """S-10's entry leg is the first cross-sectional member that reads ``regime``."""
    return s10_entry_member(
        series,
        panel_decision_dates=panel_decision_dates,
        universe=universe,
        close_reason=masked_reason,
        regime=regime,
    )


def _s10_exit_member(
    series: BarSeries,
    *,
    panel_decision_dates: Set[date],
    universe: Universe,
    masked_reason: NotEvaluableReason,
    regime: RegimeSeries,  # noqa: ARG001 - deliberate: a missing benchmark must never refuse an exit (S-7's rule)
) -> CrossSectionalMember:
    return s10_exit_member(
        series,
        panel_decision_dates=panel_decision_dates,
        universe=universe,
        close_reason=masked_reason,
    )


def _reject_decision_dates(strategy_id: str, decision_dates: frozenset[date] | None) -> None:
    """A per-series strategy handed a calendar is a runner bug, not a no-op.

    Silently ignoring it would let a caller believe S-1 rebalances monthly.
    """
    if decision_dates is not None:
        raise ValueError(
            f"{strategy_id} declares no decision calendar, so it cannot be given {len(decision_dates)} "
            "decision dates — pass the result of its own decision_calendar()"
        )


def _s1_exit_regime(decision_dates: frozenset[date] | None) -> ExitRegime:
    """§4 gives S-1 no holding bound, so ``signal_pair`` is its only close source.

    ⚠ Spec §5.3 flags that as a genuine open problem for the walk-forward
    embargo and recommends giving S-1 a declared bound. That would be a NEW
    strategy version and is not this ticket's to make — the manifest records
    what S-1 is today, not what it should become.
    """
    _reject_decision_dates(S1_STRATEGY_ID, decision_dates)
    return ExitRegime(signal_pair=True, level_based=False, max_hold_bars=None, rebalance_dates=None)


def _s2_exit_regime(decision_dates: frozenset[date] | None) -> ExitRegime:
    """S-2 closes on the calendar: its hold is *"until the next rebalance"*.

    ⚠ It declares NO ``max_hold_bars`` on purpose — approximating a month as 21
    bars would invent a parameter §4 does not give.
    """
    if decision_dates is None:
        raise ValueError(
            f"{S2_STRATEGY_ID} closes on its rebalance calendar and cannot be built without one — "
            "pass rebalance_dates(panel calendar)"
        )
    return ExitRegime(signal_pair=False, level_based=False, max_hold_bars=None, rebalance_dates=decision_dates)


def _s3_exit_regime(decision_dates: frozenset[date] | None) -> ExitRegime:
    """S-3 exits on its own exit leg OR on the hold cap.

    ⚠ ``level_based=False``: S-3 has no stop and no target — its exit is
    ``rsi_14 > 50`` — so ``ExitLevels`` cannot be constructed for it. That is
    ``ExitRegime``'s own correction to the catalogue's prose, carried here.
    """
    _reject_decision_dates(S3_STRATEGY_ID, decision_dates)
    return ExitRegime(signal_pair=True, level_based=False, max_hold_bars=S3_MAX_HOLD_BARS, rebalance_dates=None)


def _s4_exit_regime(decision_dates: frozenset[date] | None) -> ExitRegime:
    """S-4 exits on an ATR stop/target fixed at signal time, or on the hold cap.

    ``level_based=True`` and ``StrategyEntry.exit_levels`` move together: the
    regime tells the position builder an outcome is mandatory, while the
    manifest supplies the strategy-owned causal factory that can produce one.
    """
    _reject_decision_dates(S4_STRATEGY_ID, decision_dates)
    return ExitRegime(signal_pair=False, level_based=True, max_hold_bars=S4_MAX_HOLD_BARS, rebalance_dates=None)


def _s4_exit_levels(
    series: BarSeries,
    *,
    signal_index: int,
    entry_price: Decimal,
    universe: Universe,
) -> ExitLevels | UnresolvedReason:
    """Adapt S-4's hashed scalar oracle to the outcome reason contract.

    The batch adapter owns the typed refusal because changing the hashed S-4
    module merely to add an exception class would mint a new strategy version.
    For an orderable bracket the original scalar factory is still called and
    compared exactly, so the optimisation cannot silently become the formula.
    """
    (batched,) = s4_exit_levels_batch(
        series,
        requests=((signal_index, entry_price),),
        universe=universe,
    )
    if batched == "unorderable_exit_levels":
        return batched

    target, stop, max_hold = s4_exit_bracket(
        series,
        signal_index=signal_index,
        entry_price=entry_price,
        universe=universe,
    )
    scalar = ExitLevels(take_profit=target, stop_loss=stop, max_hold_bars=max_hold)
    if scalar != batched:
        raise RuntimeError("S-4 scalar and batch exit-level factories disagree")
    return scalar


def _s9_signals(
    series: BarSeries,
    *,
    universe: Universe,
    masked_reason: NotEvaluableReason,
    regime: RegimeSeries,
) -> list[StrategySignal]:
    return s9_signals(series, universe=universe, masked_reason=masked_reason, regime=regime)


def _s9_exit_regime(decision_dates: frozenset[date] | None) -> ExitRegime:
    """S-9 exits on an entry-anchored ATR bracket fixed at signal time, or the hold cap."""
    _reject_decision_dates(S9_STRATEGY_ID, decision_dates)
    return ExitRegime(signal_pair=False, level_based=True, max_hold_bars=S9_MAX_HOLD_BARS, rebalance_dates=None)


def _s9_exit_levels(
    series: BarSeries,
    *,
    signal_index: int,
    entry_price: Decimal,
    universe: Universe,
) -> ExitLevels | UnresolvedReason:
    """Adapt S-9's bracket to the outcome reason contract."""
    try:
        target, stop, max_hold = s9_exit_bracket(
            series, signal_index=signal_index, entry_price=entry_price, universe=universe
        )
    except ValueError, IndexError:
        return "unorderable_exit_levels"
    if target <= stop:
        return "unorderable_exit_levels"
    return ExitLevels(take_profit=target, stop_loss=stop, max_hold_bars=max_hold)


def _s7_signals(
    series: BarSeries,
    *,
    universe: Universe,
    masked_reason: NotEvaluableReason,
    regime: RegimeSeries,
) -> list[StrategySignal]:
    return s7_signals(series, universe=universe, masked_reason=masked_reason, regime=regime)


def _s7_exit_regime(decision_dates: frozenset[date] | None) -> ExitRegime:
    """S-7 exits on its own exit leg, an entry-anchored ATR stop, or the hold cap.

    ⚠⚠ THE FIRST REGIME DECLARING ``signal_pair`` AND ``level_based`` TOGETHER.
    That is S-7's §3 shape — a stop with no target plus a ``close < 50-SMA``
    exit rule — not a contradiction: ``build_positions`` evaluates every
    declared close source together and the earliest wins, so the two compose
    exactly as S-3's signal pair composes with its hold cap.
    """
    _reject_decision_dates(S7_STRATEGY_ID, decision_dates)
    return ExitRegime(signal_pair=True, level_based=True, max_hold_bars=S7_MAX_HOLD_BARS, rebalance_dates=None)


def _s7_exit_levels(
    series: BarSeries,
    *,
    signal_index: int,
    entry_price: Decimal,
    universe: Universe,
) -> ExitLevels | UnresolvedReason:
    """Adapt S-7's stop-only bracket to the outcome reason contract.

    ⚠ ``take_profit=None`` is the STOP-ONLY bracket ``outcome_resolver``
    documents — rules 2/3/5 of its precedence table are unreachable and the
    target-side close comes from the exit leg or the hold cap instead.
    ⚠ ``stop <= 0`` replaces the siblings' ``target <= stop`` orderability
    check: with no target the only unorderable state is a stop at or below
    zero, which a low-priced high-ATR name genuinely produces (``ExitLevels``
    itself refuses it, so it must be a typed refusal here, not a raise that
    aborts the whole batch).
    """
    try:
        target, stop, max_hold = s7_exit_bracket(
            series, signal_index=signal_index, entry_price=entry_price, universe=universe
        )
    except ValueError, IndexError:
        # ⚠ BOTH, deliberately — `ValueError` is the bracket's own refusal (no
        # ATR); `IndexError` is an out-of-range `signal_index` inside
        # `atr_series`. A narrow except that reads as exhaustive is worse than
        # none (S-5's recorded lesson).
        return "unorderable_exit_levels"
    if stop <= 0:
        return "unorderable_exit_levels"
    return ExitLevels(take_profit=target, stop_loss=stop, max_hold_bars=max_hold)


def _s8_signals(
    series: BarSeries,
    *,
    universe: Universe,
    masked_reason: NotEvaluableReason,
    regime: RegimeSeries,
) -> list[StrategySignal]:
    return s8_signals(series, universe=universe, masked_reason=masked_reason, regime=regime)


def _s8_exit_regime(decision_dates: frozenset[date] | None) -> ExitRegime:
    """S-8 exits on the signal bar's middle band / an entry-anchored ATR stop, or the hold cap."""
    _reject_decision_dates(S8_STRATEGY_ID, decision_dates)
    return ExitRegime(signal_pair=False, level_based=True, max_hold_bars=S8_MAX_HOLD_BARS, rebalance_dates=None)


def _s8_exit_levels(
    series: BarSeries,
    *,
    signal_index: int,
    entry_price: Decimal,
    universe: Universe,
) -> ExitLevels | UnresolvedReason:
    """Adapt S-8's bracket to the outcome reason contract.

    ⚠ ``target <= stop`` is MORE reachable here than for the entry-anchored
    strategies and is still not a bug. S-8's target is the signal bar's middle
    band while its stop is anchored to the fill, so a gap up through the band on
    the open of ``t+1`` inverts them — the rule's thesis was consumed before the
    position existed, and an unresolved outcome is the truthful record of that.
    """
    try:
        target, stop, max_hold = s8_exit_bracket(
            series, signal_index=signal_index, entry_price=entry_price, universe=universe
        )
    except ValueError, IndexError:
        return "unorderable_exit_levels"
    if target <= stop:
        return "unorderable_exit_levels"
    return ExitLevels(take_profit=target, stop_loss=stop, max_hold_bars=max_hold)


def _s5_signals(
    series: BarSeries,
    *,
    universe: Universe,
    masked_reason: NotEvaluableReason,
    regime: RegimeSeries,
) -> list[StrategySignal]:
    return s5_signals(series, universe=universe, masked_reason=masked_reason, regime=regime)


def _s5_exit_regime(decision_dates: frozenset[date] | None) -> ExitRegime:
    """S-5 exits on a level-anchored stop / ATR target fixed at signal time, or the hold cap."""
    _reject_decision_dates(S5_STRATEGY_ID, decision_dates)
    return ExitRegime(signal_pair=False, level_based=True, max_hold_bars=S5_MAX_HOLD_BARS, rebalance_dates=None)


def _s5_exit_levels(
    series: BarSeries,
    *,
    signal_index: int,
    entry_price: Decimal,
    universe: Universe,
) -> ExitLevels | UnresolvedReason:
    """Adapt S-5's bracket to the outcome reason contract. No ``regime`` — see `_s6_exit_levels`."""
    try:
        target, stop, max_hold = s5_exit_bracket(
            series, signal_index=signal_index, entry_price=entry_price, universe=universe
        )
    except ValueError, IndexError:
        # ⚠ BOTH, deliberately. `ValueError` is the bracket's own refusal (no ATR,
        # no level); `IndexError` is what an out-of-range `signal_index` produces
        # inside `atr_series` / `levels_at`. Catching only the first let a
        # per-bar failure abort the WHOLE outcome batch — a narrow except that
        # reads as exhaustive is worse than no except at all.
        return "unorderable_exit_levels"
    if target <= stop:
        return "unorderable_exit_levels"
    return ExitLevels(take_profit=target, stop_loss=stop, max_hold_bars=max_hold)


def _s6_signals(
    series: BarSeries,
    *,
    universe: Universe,
    masked_reason: NotEvaluableReason,
    regime: RegimeSeries,
) -> list[StrategySignal]:
    """S-6 is the first strategy that actually reads ``regime`` — passed through."""
    return s6_signals(series, universe=universe, masked_reason=masked_reason, regime=regime)


def _s6_exit_regime(decision_dates: frozenset[date] | None) -> ExitRegime:
    """S-6 exits on a level-anchored stop / ATR target fixed at signal time, or the hold cap."""
    _reject_decision_dates(S6_STRATEGY_ID, decision_dates)
    return ExitRegime(signal_pair=False, level_based=True, max_hold_bars=S6_MAX_HOLD_BARS, rebalance_dates=None)


def _s6_exit_levels(
    series: BarSeries,
    *,
    signal_index: int,
    entry_price: Decimal,
    universe: Universe,
) -> ExitLevels | UnresolvedReason:
    """Adapt S-6's bracket to the outcome reason contract.

    ⚠ NO ``regime`` PARAMETER, and that is deliberate rather than an omission.
    A bracket is only ever requested for a signal that ALREADY FIRED, so the
    regime was permitted by construction at signal time. Threading it here would
    let a regime that has since moved refuse to produce a stop for a position
    that is already open — a gate that can retroactively remove an open
    position's exit is not a safety control. See `_resistance_below`.
    """
    try:
        target, stop, max_hold = s6_exit_bracket(
            series, signal_index=signal_index, entry_price=entry_price, universe=universe
        )
    except ValueError, IndexError:
        # The level or ATR is unevaluable at the signal bar. Typed refusal rather
        # than a raise: the outcome runner records an unresolved outcome, which is
        # the truthful state, and a raise would abort the whole batch for one bar.
        #
        # ⚠ BOTH exception types, deliberately. `ValueError` is the bracket's own
        # refusal; `IndexError` is what an out-of-range `signal_index` produces
        # inside `atr_series` / `levels_at`. A narrow except that reads as
        # exhaustive is worse than none.
        return "unorderable_exit_levels"
    if target <= stop:
        # Reachable, and not a bug to prevent: the stop is anchored to the LEVEL
        # while the target is anchored to the ENTRY, so a fill far below the
        # level can invert them. That asymmetry is the rule (see the S-6 module
        # docstring), so an inverted bracket is a real state to refuse.
        return "unorderable_exit_levels"
    return ExitLevels(take_profit=target, stop_loss=stop, max_hold_bars=max_hold)


def _s10_exit_regime(decision_dates: frozenset[date] | None) -> ExitRegime:
    """S-10 closes on its own ranked exit leg — ``signal_pair`` alone.

    ⚠ ``rebalance_dates`` stays ``None`` even though S-10 has a calendar: C4
    closes at "the next rebalance NOT RESELECTED", which is ENTRY-set
    retention, and S-10's retention is the wider top-three-decile band the
    exit leg carries. Declaring both would close band-surviving positions a
    month early. The calendar argument is still accepted (and ignored)
    because the runner hands every cross-sectional strategy its own
    ``decision_calendar`` output.
    """
    if decision_dates is None:
        raise ValueError(
            f"{S10_STRATEGY_ID} is cross_sectional and its runner must pass its decision calendar — "
            "a None here means the runner skipped decision_calendar()"
        )
    return ExitRegime(signal_pair=True, level_based=False, max_hold_bars=None, rebalance_dates=None)


#: Every strategy in the catalogue, keyed by ``strategy_id``.
#:
#: ⚠⚠ COMPLETENESS IS A TEST, NOT A CONVENTION.
#: ``tests/test_strategy_manifest.py::TestManifestIsComplete`` walks every module
#: under ``app.services.strategies`` and fails if one exposing a
#: ``*_STRATEGY_ID`` is missing from here — the same pattern
#: ``TestInputRuleSetsAreComplete`` already proves for ``INPUT_RULE_SETS``. So a
#: strategy present in the tree but absent here **fails CI** rather than quietly
#: not running.
STRATEGY_MANIFEST: Mapping[str, StrategyEntry] = MappingProxyType(
    {
        S1_STRATEGY_ID: StrategyEntry(
            strategy_id=S1_STRATEGY_ID,
            purpose="harness_validation",
            identity=s1_identity,
            strategy_class="per_series",
            signal_kinds=frozenset({"entry", "exit"}),
            exit_regime=_s1_exit_regime,
            decision_calendar=_no_decision_calendar,
            signals=_s1_signals,
        ),
        S2_STRATEGY_ID: StrategyEntry(
            strategy_id=S2_STRATEGY_ID,
            purpose="harness_validation",
            identity=s2_identity,
            strategy_class="cross_sectional",
            signal_kinds=frozenset({"entry"}),
            exit_regime=_s2_exit_regime,
            decision_calendar=rebalance_dates,
            member=_s2_member,
            select=s2_select,
            min_participants=MIN_CROSS_SECTION,
        ),
        S3_STRATEGY_ID: StrategyEntry(
            strategy_id=S3_STRATEGY_ID,
            purpose="harness_validation",
            identity=s3_identity,
            strategy_class="per_series",
            signal_kinds=frozenset({"entry", "exit"}),
            exit_regime=_s3_exit_regime,
            decision_calendar=_no_decision_calendar,
            signals=_s3_signals,
        ),
        S4_STRATEGY_ID: StrategyEntry(
            strategy_id=S4_STRATEGY_ID,
            purpose="harness_validation",
            identity=s4_identity,
            strategy_class="per_series",
            signal_kinds=frozenset({"entry"}),
            exit_regime=_s4_exit_regime,
            decision_calendar=_no_decision_calendar,
            signals=_s4_signals,
            exit_levels=_s4_exit_levels,
            exit_levels_batch=s4_exit_levels_batch,
        ),
        S5_STRATEGY_ID: StrategyEntry(
            strategy_id=S5_STRATEGY_ID,
            purpose="harness_validation",
            identity=s5_identity,
            strategy_class="per_series",
            signal_kinds=frozenset({"entry"}),
            exit_regime=_s5_exit_regime,
            decision_calendar=_no_decision_calendar,
            signals=_s5_signals,
            exit_levels=_s5_exit_levels,
        ),
        S6_STRATEGY_ID: StrategyEntry(
            strategy_id=S6_STRATEGY_ID,
            purpose="harness_validation",
            identity=s6_identity,
            strategy_class="per_series",
            signal_kinds=frozenset({"entry"}),
            exit_regime=_s6_exit_regime,
            decision_calendar=_no_decision_calendar,
            signals=_s6_signals,
            exit_levels=_s6_exit_levels,
        ),
        S7_STRATEGY_ID: StrategyEntry(
            strategy_id=S7_STRATEGY_ID,
            purpose="harness_validation",
            identity=s7_identity,
            strategy_class="per_series",
            signal_kinds=frozenset({"entry", "exit"}),
            exit_regime=_s7_exit_regime,
            decision_calendar=_no_decision_calendar,
            signals=_s7_signals,
            exit_levels=_s7_exit_levels,
        ),
        S8_STRATEGY_ID: StrategyEntry(
            strategy_id=S8_STRATEGY_ID,
            purpose="harness_validation",
            identity=s8_identity,
            strategy_class="per_series",
            signal_kinds=frozenset({"entry"}),
            exit_regime=_s8_exit_regime,
            decision_calendar=_no_decision_calendar,
            signals=_s8_signals,
            exit_levels=_s8_exit_levels,
        ),
        S9_STRATEGY_ID: StrategyEntry(
            strategy_id=S9_STRATEGY_ID,
            purpose="harness_validation",
            identity=s9_identity,
            strategy_class="per_series",
            signal_kinds=frozenset({"entry"}),
            exit_regime=_s9_exit_regime,
            decision_calendar=_no_decision_calendar,
            signals=_s9_signals,
            exit_levels=_s9_exit_levels,
        ),
        S10_STRATEGY_ID: StrategyEntry(
            strategy_id=S10_STRATEGY_ID,
            purpose="harness_validation",
            identity=s10_identity,
            strategy_class="cross_sectional",
            signal_kinds=frozenset({"entry", "exit"}),
            exit_regime=_s10_exit_regime,
            decision_calendar=s10_rebalance_dates,
            member=_s10_entry_member,
            select=s10_entry_select,
            min_participants=S10_MIN_CROSS_SECTION,
            exit_leg=CrossSectionalLeg(
                member=_s10_exit_member,
                select=s10_exit_select,
                min_participants=S10_MIN_CROSS_SECTION,
            ),
        ),
    }
)


__all__ = [
    "STRATEGY_CLASSES",
    "STRATEGY_MANIFEST",
    "CrossSectionalLeg",
    "DecisionCalendar",
    "ExitRegimeFactory",
    "ExitLevelsBatchFactory",
    "ExitLevelsFactory",
    "IdentityFactory",
    "MemberStager",
    "PerSeriesSignals",
    "StrategyClass",
    "StrategyEntry",
    "StrategyPurpose",
]
