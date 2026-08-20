"""Phase 3a — the strategy registry contract.

Spec: ``docs/proposals/ta/2026-08-05-strategy-registry-and-signal-ledger.md``.
Refs #2240, #2245, #2288.

WHAT A STRATEGY IS HERE
----------------------
A pure function over a ``BarSeries`` plus its indicator series, returning a
verdict per bar. Code, not rows: a rules table needs an interpreter, and an
interpreter is a second place for the fill-timing rule to be got wrong.

⚠⚠ THE FILL RULE IS ENFORCED BY THE SHAPE OF THIS API, NOT BY A CONSTRAINT.

Parent §3.5: *"Signal on the close of bar t → fill at the OPEN of bar t+1. No
exceptions… The backtester must make same-bar fills structurally impossible
rather than merely discouraged."*

A ``StrategySignal`` carries a bar INDEX and nothing else. There is no field
through which a strategy could request a fill price, a fill date, or a fill
bar. The writer resolves the fill from the series. **A same-bar fill is not
expressible**, which is what "structurally impossible" has to mean — removing
the capability rather than detecting its misuse.

⚠ An earlier draft of the spec claimed a ``CHECK (fill_bar_date >
signal_bar_date)`` was "the whole mechanism". It is not: a writer can record
``signal_bar_date = t-1``, fill on ``t``, and use bar ``t``'s data with every
constraint passing. That CHECK is a backstop against a buggy writer and is
described as one.

⚠⚠ EVALUABILITY IS DECIDED BEFORE THE CONDITION RUNS. See ``evaluate``.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping, Sequence, Set
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from types import MappingProxyType
from typing import Literal, Protocol, get_args

from app.services.indicator_series import RULE_SET_VERSION as INDICATOR_SERIES_RULE_SET_VERSION
from app.services.indicator_series import IndicatorSeries, MultiIndicatorSeries, Universe
from app.services.market_regime_provider import RULE_SET_VERSION as BENCHMARK_SOURCE_RULE_SET_VERSION
from app.services.series_termination import TERMINATION_RULE_VERSION
from app.services.universe_selection import UNIVERSE_SELECTION_RULE_VERSION

STRATEGY_SET_ID = "strategy-registry-v1"


def _module_hash() -> str:
    return hashlib.sha256(Path(__file__).read_bytes()).hexdigest()[:12]


#: Every versioned rule set whose OUTPUT a strategy reads, keyed by the module
#: that owns it. Hashed into ``StrategyIdentity.version`` and stored beside
#: every ledger row.
#:
#: ⚠⚠ THIS IS A REGISTRY-WIDE CONSTANT, NOT A PER-STRATEGY DECLARATION, AND
#: THAT IS THE POINT (#2333).
#:
#: The defect it closes: a strategy is `sma_series(fast) > sma_series(slow)` and
#: has no other content, so a change to how the SMA is COMPUTED produces
#: different signals — under an unchanged ``strategy_version``, whose key then
#: treats the old and new rows as the same row. Same shape as the phase-4b
#: prevention entry (*"a derived artefact's key must carry the version of every
#: pipeline that can change it, not just the deriving module's own"*), which was
#: fixed for ``strategy_outcomes`` and left unfixed one layer up.
#:
#: A per-strategy ``inputs=[...]`` field would be more precise and is rejected:
#: the failure being fixed is an author not thinking about indicator versions at
#: all, and a field they must remember to fill is the same omission with a
#: nicer name. ``tests/test_strategy_registry.py::TestInputRuleSetsAreComplete``
#: walks every module in ``app.services.strategies`` and fails if it imports a
#: versioned rule set missing from here — so the coverage is checked, not
#: promised.
#:
#: ⚠ The cost is OVER-INVALIDATION, twice over, and it is accepted knowingly
#: rather than inherited: ``RULE_SET_VERSION`` hashes a module's whole source,
#: so a comment edit in ``indicator_series.py`` moves every strategy's identity;
#: and because this set is registry-wide, a strategy reading none of these
#: series still moves with them. Both make stored signals visibly stale instead
#: of silently mixed, which is the trade this epic has already taken three times
#: (``price_quarantine``, ``price_structure``, ``indicator_series``).
INPUT_RULE_SETS: Mapping[str, str] = MappingProxyType(
    {
        "indicator_series": INDICATOR_SERIES_RULE_SET_VERSION,
        # ⚠ Not import-detectable by ``TestInputRuleSetsAreComplete``'s walk —
        # strategies receive the regime as a constructed ``RegimeSeries``, they
        # never import the provider — so this entry is maintained by hand and
        # pinned by ``test_the_stored_mapping_is_the_hashed_one``. It names the
        # benchmark SOURCE rules (live ``price_daily`` vs the backtest's
        # ``spy_chain_v1`` research chain): switching the backtest source flips
        # every pre-2023 bar of a regime-gated strategy from ``not_evaluable``
        # to a real verdict, which is a changed input under an unchanged
        # strategy_version unless it is hashed here.
        "market_regime_provider": BENCHMARK_SOURCE_RULE_SET_VERSION,
        # ⚠ #2721 step 3 — BOTH hand-maintained, like the entry above
        # (strategies import neither module; the ENGINE consumes them), and
        # both pinned by ``test_the_stored_mapping_is_the_hashed_one``.
        #
        # ``series_termination``: what a held position realises when its
        # series stops — the survivorship treatment itself. Joined the hashed
        # set at the SAME commit that first wired it into the backtest, per
        # its own module docstring's freeze.
        #
        # ``universe_selection``: the vendor pins, admission rule, alive cut
        # and capture date. The bare ``universe`` label on the identity does
        # not version any of those (ckpt-1), and a changed admission is a
        # changed universe under criterion 11. This over-invalidates
        # survivor-only identities on a survivorship-free rule change —
        # accepted deliberately, the same global-rule-set over-invalidation
        # every entry in this mapping makes.
        "series_termination": TERMINATION_RULE_VERSION,
        "universe_selection": UNIVERSE_SELECTION_RULE_VERSION,
    }
)


Verdict = Literal["fired", "not_fired", "not_evaluable"]

SignalKind = Literal["entry", "exit"]

#: ⚠ CLOSED vocabulary. Seven codes are parent criterion 8 verbatim:
#: *"`not_evaluable` carries a reason code … These have different bias
#: implications and collapsing them loses the ability to tell a data gap from a
#: real absence."* Free text cannot be counted, so it cannot support criterion
#: 9's "measure what you reject".
#:
#: ⚠ ``no_fill_bar`` is an EIGHTH, added here and flagged as an addition rather
#: than smuggled in: the last bar of any series has no ``t+1``, so a signal
#: there can never be filled, and none of the seven describes that. It is not a
#: data gap — it is the edge of the series. If the parent's vocabulary is the
#: authority, this needs adopting there too.
#:
#: ⚠ ``thin_cross_section`` is a NINTH, and the same flagging applies (sql/260
#: widens the CHECK). It is the first code that is a property of the PANEL
#: rather than of the bar: a strategy ranking within a cross-section of six
#: names has no decile to be in the top of. The alternatives were both worse —
#: rounding the decile up silently becomes "best of six", and reporting
#: ``not_fired`` is criterion 8's exact prohibition, a data-availability fact
#: wearing a rule verdict's clothes. See ``evaluate_cross_sectional``.
#:
#: ⚠ ``unusable_fill_price`` is a TENTH (#2354, sql/270 widens the CHECK), and
#: it is a SPLIT of ``no_fill_bar`` rather than a new situation: bar ``t+1``
#: exists and its OPEN is not a usable price. ``resolve_fills`` used to report
#: that as ``no_fill_bar`` and said so, conditionally — *"if the measured count
#: ever leaves zero, split it"*. It has (170 bars across the two corpora,
#: measured in sql/270), so this is the split it pre-registered. The pair is
#: exactly criterion 8's distinction: the edge of the series is a real absence,
#: an unpriceable bar is a data gap.
#:
#: ⚠ ``missing_market_context`` is an ELEVENTH (#2437, sql/351 widens the three
#: CHECKs), and it is the first code that is a property of a DIFFERENT
#: INSTRUMENT than the one being judged. S-5…S-10 gate on a market regime
#: classified from the benchmark; when the benchmark contributed no bar at all
#: on a date the instrument traded, the strategy cannot judge that bar.
#: ``thin_cross_section`` is the nearest relative and is still wrong — that
#: describes a panel which EXISTS and is too small, not a series that is absent.
#:
#: ⚠ Measured before it was minted, full validated universe (dev DB,
#: 2026-08-14): 9,688 bars over 360 dates, worst 2026-02-06 with 1,735
#: instruments trading against no SPY bar. Every one was stored as
#: ``not_fired`` — a bar the strategy could not judge, recorded as one it judged
#: and declined. ⚠ A benchmark bar that EXISTS and is merely unclassifiable
#: (200-SMA still warming) stays ``insufficient_warmup``; only an absent
#: benchmark observation earns this code. See ``market_regime.RegimeSeries``.
NotEvaluableReason = Literal[
    "missing_volume",
    "missing_spread",
    "insufficient_warmup",
    "quarantined_bar",
    "series_break",
    "not_listed",
    "ambiguous_intrabar",
    "no_fill_bar",
    "thin_cross_section",
    "unusable_fill_price",
    "missing_market_context",
]

# ⚠ DERIVED from the Literals above, never restated. Review flagged the
# vocabulary being written out three times here — and sql/255's CHECK makes a
# fourth — which is precisely the closed-vocabulary-in-N-places defect the
# prevention log carries from #2218 (a member added in one place and missed in
# the others writes rows nothing reads). `get_args` makes drift impossible in
# Python; tests/test_strategy_registry.py pins the SQL CHECK against these.
VERDICTS: frozenset[str] = frozenset(get_args(Verdict))
SIGNAL_KINDS: frozenset[str] = frozenset(get_args(SignalKind))
NOT_EVALUABLE_REASONS: frozenset[str] = frozenset(get_args(NotEvaluableReason))

#: The seven from parent criterion 8. `no_fill_bar`, `thin_cross_section`,
#: `unusable_fill_price` and `missing_market_context` are OURS and are excluded
#: deliberately — see NotEvaluableReason. Kept as an explicit subtraction so
#: adding a parent code later cannot silently land on our side of the line.
OUR_ADDITIONAL_REASON_CODES: frozenset[str] = frozenset(
    {"no_fill_bar", "thin_cross_section", "unusable_fill_price", "missing_market_context"}
)
PARENT_REASON_CODES: frozenset[str] = NOT_EVALUABLE_REASONS - OUR_ADDITIONAL_REASON_CODES


@dataclass(frozen=True)
class StrategySignal:
    """One bar's verdict.

    ⚠ ``signal_index`` is an index into the series the strategy was given, and
    it is the ONLY positional information a strategy emits. No fill date, no
    fill price, no fill bar — see the module docstring.
    """

    verdict: Verdict
    signal_index: int
    kind: SignalKind = "entry"
    #: Required when ``verdict == "not_evaluable"``, forbidden otherwise.
    reason: NotEvaluableReason | None = None

    def __post_init__(self) -> None:
        # ⚠ `Literal` is a TYPE-CHECK annotation and enforces nothing at
        # runtime — an untyped caller can pass `reason="free text"` or an
        # unknown verdict straight through. This class exists to keep verdicts
        # and reason codes COUNTABLE (criterion 9 has to count them), so the
        # closed sets are checked here rather than assumed.
        if self.verdict not in VERDICTS:
            raise ValueError(f"unknown verdict {self.verdict!r}; must be one of {sorted(VERDICTS)}")
        if self.kind not in SIGNAL_KINDS:
            raise ValueError(f"unknown signal kind {self.kind!r}; must be one of {sorted(SIGNAL_KINDS)}")
        if self.reason is not None and self.reason not in NOT_EVALUABLE_REASONS:
            raise ValueError(f"unknown reason code {self.reason!r}; must be one of {sorted(NOT_EVALUABLE_REASONS)}")
        if self.verdict == "not_evaluable" and self.reason is None:
            raise ValueError("not_evaluable requires a reason code (parent criterion 8)")
        if self.verdict != "not_evaluable" and self.reason is not None:
            raise ValueError(f"reason {self.reason!r} is meaningless on verdict {self.verdict!r}")
        if self.signal_index < 0:
            raise ValueError(f"signal_index must be non-negative, got {self.signal_index}")


@dataclass(frozen=True)
class StrategyIdentity:
    """Everything that makes this a distinct strategy.

    ⚠ Parent criterion 11: *"Strategy identity must cover code, not just
    parameters — same params with a changed filter, universe or cost model is a
    different strategy."* So the version hashes ALL of it. An earlier draft
    hashed only the defining module's source (copying ``indicator_series``),
    which misses the universe and the cost model entirely — two genuinely
    different strategies would then share a version and their signals would
    collide on the ledger's uniqueness key.

    ⚠ This is also why ``universe`` is NOT a separate column in that key:
    criterion 11 puts it *inside* the identity, so one identity spanning two
    universes is not one strategy.

    ⚠ The hash also covers ``INPUT_RULE_SETS`` (#2333). The strategy's filter
    logic is not only the module below — it is that module *plus the definition
    of every indicator it reads*, and criterion 11 says changed filter logic is
    a different strategy.
    """

    strategy_id: str
    params: Mapping[str, object]
    universe: Universe
    cost_model_id: str
    #: Source of the module DEFINING the strategy, not of this registry.
    source_hash: str

    @property
    def input_rule_set_versions(self) -> Mapping[str, str]:
        """The rule sets this identity's version covers — see ``INPUT_RULE_SETS``.

        ⚠ A PROPERTY, not a field, deliberately. A field is something a caller
        can pass wrongly or a strategy author can forget; there is exactly one
        correct value per process, so it is read rather than accepted.

        ⚠ Consequence, stated because it is surprising: two identities with
        equal FIELDS compare equal (``dataclass`` ``__eq__`` does not see a
        property) while their ``version`` differs across processes running
        different indicator code. That is the intended direction — the version
        is what the ledger keys on, and it is the thing that must move.

        The ledger writer reads it from HERE rather than importing the constant
        itself, so the stored column and the hash it sits beside cannot
        disagree — the argument ``LedgerRow`` already makes for ``universe``.
        """
        return INPUT_RULE_SETS

    @property
    def version(self) -> str:
        payload = json.dumps(
            {
                "strategy_id": self.strategy_id,
                "params": self.params,
                "universe": self.universe,
                "cost_model_id": self.cost_model_id,
                "source_hash": self.source_hash,
                "registry": _module_hash(),
                "input_rule_sets": dict(self.input_rule_set_versions),
            },
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        )
        return f"{STRATEGY_SET_ID}+{hashlib.sha256(payload.encode()).hexdigest()[:12]}"


#: A strategy body. Invoked ONLY on bars where every declared input is
#: evaluable — see ``evaluate``.
StrategyBody = Callable[[int], bool]


class EvaluableSeries(Protocol):
    """What ``evaluate`` actually needs from a declared input.

    ⚠⚠ A PROTOCOL RATHER THAN A UNION, AND THE ALTERNATIVE IS WHY (#2437).

    ``StrategyInput`` was typed ``IndicatorSeries | MultiIndicatorSeries``, so a
    ``market_regime.RegimeSeries`` — the one input S-5…S-10 gate on — could not
    be declared at all, and the regime check ended up INSIDE each strategy body
    instead. That is not a style difference: ``evaluate``'s whole guarantee is
    that evaluability is decided before the body runs, so an input checked
    inside the body has no way to report ``not_evaluable`` and reports
    ``not_fired`` instead. 9,688 bars were stored that way.

    The two ways to let a regime in were both worse:

    * **Widen the union.** ``strategy_registry`` is the generic contract and
      ``market_regime`` is one strategy family's rule; importing the second into
      the first inverts the layering, and every future family would add another
      arm.
    * **Adapt to an ``IndicatorSeries``.** That object carries
      ``indicator_series.RULE_SET_VERSION``, which the regime did not come from,
      so the adapter would have to stamp a provenance that is false — and
      ``market_regime`` deliberately does not import ``indicator_series`` (see
      its ``_trailing_sma``) precisely to keep the two versions uncoupled.

    A structural protocol needs neither. Any object exposing per-bar values plus
    the indices it could not support satisfies the contract, and nothing has to
    know about anything else.

    ⚠ ``values`` is ``Sequence[object | None]``, not ``float | None`` — a regime
    value is a ``Regime`` enum member. ``evaluate`` only ever asks whether it
    ``is None``, never what it is.

    ⚠ ``MultiIndicatorSeries`` does NOT satisfy this (it exposes ``components``,
    not ``values``) and is kept as an explicit union arm below.
    """

    @property
    def values(self) -> Sequence[object | None]: ...

    @property
    def not_evaluable_indices(self) -> tuple[int, ...]: ...

    def __len__(self) -> int: ...


@dataclass(frozen=True)
class StrategyInput:
    """One indicator series a strategy depends on, WITH the reason code to
    record when it cannot support a value.

    ⚠ The reason has to come from the caller, and this pairing is the fix for a
    real defect Codex found at checkpoint 2. ``evaluate`` originally recorded a
    single ``warmup_reason`` for every unevaluable bar, which collapsed
    quarantined bars, series breaks and genuine data gaps all into
    ``insufficient_warmup`` — destroying precisely what parent criterion 8
    exists for: *"These have different bias implications and collapsing them
    loses the ability to tell a data gap from a real absence."*

    ``indicator_series`` knows THAT a value is unevaluable but not WHY — it has
    no database access and cannot know whether a NULL came from a quarantined
    bar or a missing volume field. The caller assembling the inputs does. So
    the knowledge is supplied where it exists rather than guessed where it does
    not.

    ⚠ Warm-up is distinguished structurally, not by the caller: a leading
    ``None`` that is NOT in ``not_evaluable_indices`` is the indicator warming
    up, and is always ``insufficient_warmup``.
    """

    series: EvaluableSeries | MultiIndicatorSeries
    #: Recorded when this input is unevaluable for a data reason.
    reason: NotEvaluableReason


def _unevaluable_reason_at(inputs: Sequence[StrategyInput], index: int) -> NotEvaluableReason | None:
    """The reason this bar cannot be judged, or None if every input is fine.

    Data reasons win over warm-up: a bar that is BOTH inside an indicator's
    warm-up and quarantined is reported as quarantined, because that is the one
    with a bias implication worth counting.
    """
    warming = False
    for declared in inputs:
        series = declared.series
        if index in series.not_evaluable_indices:
            return declared.reason
        # ⚠ The isinstance test is on the MULTI arm, not the single one. It used
        # to read `isinstance(series, IndicatorSeries)` with the multi case in
        # `else`, which silently treated any non-`IndicatorSeries` as having
        # `.components` — so the `EvaluableSeries` protocol widening (#2437)
        # would have sent a `RegimeSeries` down the multi branch and raised
        # `AttributeError` on the first warm-up bar. Testing the arm that has
        # the distinctive shape leaves the protocol as the default.
        if isinstance(series, MultiIndicatorSeries):
            if any(component[index] is None for component in series.components.values()):
                warming = True
        elif series.values[index] is None:
            warming = True
    return "insufficient_warmup" if warming else None


def evaluate(
    body: StrategyBody,
    *,
    inputs: Sequence[StrategyInput],
    n_bars: int,
    kind: SignalKind = "entry",
) -> list[StrategySignal]:
    """Run ``body`` over every bar, returning one verdict each.

    ⚠⚠ EVALUABILITY IS CHECKED BEFORE ``body`` IS CALLED, AND THAT IS THE WHOLE
    POINT OF THIS FUNCTION.

    Python's ``and`` / ``or`` short-circuit. A strategy written as::

        close[i] > sma[i] and volume[i] > vol_sma[i] * 1.5

    returns False the moment ``close <= sma``, WITHOUT ever touching
    ``volume``. If ``volume`` was unevaluable at that bar, the strategy has
    reported ``not_fired`` for a bar it could not actually judge — which is
    design-doc decision 5's corruption ("could not evaluate" indistinguishable
    from "did not fire", silently corrupting the win-rate denominator)
    re-entering through the back door after being closed at the indicator
    layer.

    Checking every declared input first makes short-circuit ordering
    irrelevant, rather than something each strategy author has to remember not
    to get wrong. ``body`` is only ever invoked on bars where all of its inputs
    are evaluable, so inside it a ``None`` is impossible by construction.

    ⚠ The LAST bar is ``no_fill_bar``, not a fire. A signal on the final bar of
    a series has no ``t+1`` to fill at, and reporting it as ``fired`` would
    hand the backtester a trade that cannot be entered. Parent criterion 8's
    seven codes do not cover this case; see ``NotEvaluableReason``.
    """
    signals: list[StrategySignal] = []
    for index in range(n_bars):
        if index == n_bars - 1:
            signals.append(StrategySignal(verdict="not_evaluable", signal_index=index, kind=kind, reason="no_fill_bar"))
            continue
        reason = _unevaluable_reason_at(inputs, index)
        if reason is not None:
            signals.append(StrategySignal(verdict="not_evaluable", signal_index=index, kind=kind, reason=reason))
            continue
        fired = body(index)
        signals.append(StrategySignal(verdict="fired" if fired else "not_fired", signal_index=index, kind=kind))
    return signals


# ---------------------------------------------------------------------------
# The CROSS-SECTIONAL contract
# ---------------------------------------------------------------------------
#
# ⚠⚠ WHY `evaluate` COULD NOT BE REUSED, AND WHAT MUST SURVIVE THE EXTENSION.
#
# `evaluate` runs a per-bar predicate over ONE series, because S-1, S-3 and S-4
# read only their own instrument's bars. S-2 does not: "hold the top decile" is
# a statement about the cross-section on a DATE, so the verdict for instrument A
# at date D depends on B..Z at D. Nothing above can express that.
#
# The extension is a second runner, not a second contract. All three guarantees
# `evaluate` buys are re-derived below rather than re-argued per strategy:
# evaluability is decided before any score is read, no fill is expressible, and
# the reason vocabulary stays the closed set at the top of this file.
#
# Spec: docs/proposals/ta/2026-08-06-cross-sectional-contract-and-s2.md.


@dataclass(frozen=True)
class CrossSectionalMember:
    """One instrument's contribution to a ranked panel.

    ⚠ ``score`` MUST ALSO BE DECLARED IN ``inputs`` and that is checked, not
    documented. Codex found the hole at checkpoint 1: without it,
    ``_unevaluable_reason_at`` can pass a bar whose ``score.values[i]`` is
    ``None``, and the runner would then rank a member on a value it does not
    have — precisely the "evaluability precedes the condition" guarantee this
    contract exists to keep.

    ⚠ ``dates`` are this member's OWN bar dates and are what the ranking groups
    on. Grouping on the bar INDEX would rank a 2019 bar against a 2007 one,
    because index ``i`` is a different date on every member.

    ``decision_indices`` are the bars at which this member ranks — a rebalance
    bar it is eligible for. Everything else is an ordinary ``not_fired``: the
    rule is *"fire iff a decision bar AND selected"*, so a non-decision bar did
    not fire. It is a verdict, not an absence.

    TWO OPTIONAL REFINEMENTS OF THE DECISION RULE (#2437 S-10), both ``None``
    for a strategy that does not use them — ``None`` is exactly today's
    behaviour and S-2 passes it implicitly:

    - ``admissible_indices`` — decision bars allowed to fire WHEN SELECTED.
      A selected bar outside it is ``not_fired``; its score still entered the
      ranking, so the panel denominator is unchanged. This is what makes
      *"the top decile that ALSO closes above its own 50-SMA"* expressible:
      the decile is cut on the whole panel, the SMA condition then filters
      the winners without backfilling the slots.
    - ``mandatory_indices`` — decision bars that fire REGARDLESS of
      selection. S-10's *"closes below 50-SMA"* exit fires whether or not the
      name also left the retention band.

    The one resolution rule, applied AFTER every refusal (``no_fill_bar``,
    unevaluable inputs, non-decision, ``thin_cross_section`` — precedence is
    unchanged and mandatory does NOT beat a refusal):

        fired iff mandatory OR (selected AND admissible)

    ⚠ Both are constrained to ``decision_indices`` and that is checked: an
    admissibility or mandate on a bar that never ranks is a contradiction the
    author should hear about, not a key silently ignored.
    """

    dates: tuple[date, ...]
    inputs: tuple[StrategyInput, ...]
    score: IndicatorSeries
    decision_indices: frozenset[int]
    admissible_indices: frozenset[int] | None = None
    mandatory_indices: frozenset[int] | None = None

    def __post_init__(self) -> None:
        n = len(self.dates)
        if len(self.score) != n:
            raise ValueError(
                f"score has {len(self.score)} values for {n} bars — an offset series is how an off-by-one "
                "enters a backtest"
            )
        for declared in self.inputs:
            if len(declared.series) != n:
                raise ValueError(f"declared input has {len(declared.series)} values for {n} bars")
        if not any(declared.series is self.score for declared in self.inputs):
            raise ValueError(
                "the ranking score must be DECLARED among inputs — otherwise a bar whose score is None "
                "passes the evaluability check and gets ranked on a value it does not have"
            )
        for index in self.decision_indices:
            if not 0 <= index < n:
                raise ValueError(f"decision index {index} is outside the {n}-bar series")
        refinements = (("admissible_indices", self.admissible_indices), ("mandatory_indices", self.mandatory_indices))
        for name, refined in refinements:
            if refined is None:
                continue
            stray = refined - self.decision_indices
            if stray:
                raise ValueError(
                    f"{name} contains {sorted(stray)[:5]} outside decision_indices — a refinement of the "
                    "decision rule on a bar that never ranks is a contradiction, not a no-op"
                )
        for i in range(1, n):
            if self.dates[i] <= self.dates[i - 1]:
                raise ValueError(
                    f"member dates are not strictly ascending at index {i}: {self.dates[i - 1]} then {self.dates[i]}"
                )


@dataclass(frozen=True)
class StagedMember:
    """One member's per-bar verdicts, with the ranked bars still undecided.

    ``verdicts[i] is None`` means "this bar participates in the ranking at
    ``dates[i]`` and cannot be decided until the whole cross-section is known".

    ⚠ This intermediate is PUBLIC on purpose. A full-corpus census cannot hold
    every member's bars in memory at once, so it stages one series at a time and
    keeps only ``scores``. Without this split it would re-implement the staging
    pass, which is how a census and the strategy it measures come to disagree.
    """

    verdicts: tuple[StrategySignal | None, ...]
    #: Ranking score per participating bar, keyed by that bar's DATE.
    scores: Mapping[date, float]
    #: The member's refinements, converted to DATES so that a consumer that
    #: re-slices series (``segmented_member``) merges them without the
    #: index-remapping every index-keyed field owes at a reslice — the silent,
    #: type-checking trap the 08-14 S-8 session committed to memory. ``None``
    #: preserves the member's ``None`` (= unrefined).
    admissible_dates: frozenset[date] | None = None
    mandatory_dates: frozenset[date] | None = None


def resolve_participating_bar(
    *,
    when: date,
    index: int,
    kind: SignalKind,
    selected: bool,
    admissible_dates: frozenset[date] | None,
    mandatory_dates: frozenset[date] | None,
) -> StrategySignal:
    """THE one resolution rule for a staged bar that survived every refusal.

    ⚠ Three resolvers rank cross-sections independently — ``evaluate_cross_
    sectional`` here, the scan's ``_resolve_cross_section``, and the
    backtest's ``_signals_for`` — and each used to inline ``fired iff
    selected``. With admissibility and mandates in the rule, three inlined
    copies is three chances for one of them to drift; they all call this.
    """
    admissible = admissible_dates is None or when in admissible_dates
    mandatory = mandatory_dates is not None and when in mandatory_dates
    fired = mandatory or (selected and admissible)
    return StrategySignal(verdict="fired" if fired else "not_fired", signal_index=index, kind=kind)


def stage_cross_sectional_member(member: CrossSectionalMember, *, kind: SignalKind = "entry") -> StagedMember:
    """Everything decidable about one member without seeing the others.

    Same refusal order as ``evaluate``, for the same reasons:

    1. the LAST bar is ``no_fill_bar`` — there is no ``t+1`` to fill at;
    2. an unevaluable declared input refuses the bar with the caller's reason;
    3. a non-decision bar is ``not_fired``;
    4. otherwise the bar participates.

    ⚠ (1) applies even to a non-decision bar, which looks over-strict on a
    monthly calendar and is not: ``signal_ledger.resolve_fills`` re-stamps the
    final bar ``no_fill_bar`` unconditionally, so any other verdict here would
    be unstorable. One rule, in both places.
    """
    n = len(member.dates)
    verdicts: list[StrategySignal | None] = []
    scores: dict[date, float] = {}
    for index in range(n):
        if index == n - 1:
            verdicts.append(
                StrategySignal(verdict="not_evaluable", signal_index=index, kind=kind, reason="no_fill_bar")
            )
            continue
        reason = _unevaluable_reason_at(member.inputs, index)
        if reason is not None:
            verdicts.append(StrategySignal(verdict="not_evaluable", signal_index=index, kind=kind, reason=reason))
            continue
        if index not in member.decision_indices:
            verdicts.append(StrategySignal(verdict="not_fired", signal_index=index, kind=kind))
            continue
        value = member.score.values[index]
        # Unreachable: `score` is a declared input, so a None was refused above.
        # Present to narrow the type and to fail loudly for a direct caller.
        assert value is not None
        verdicts.append(None)
        scores[member.dates[index]] = value
    return StagedMember(
        verdicts=tuple(verdicts),
        scores=scores,
        admissible_dates=(
            None
            if member.admissible_indices is None
            else frozenset(member.dates[index] for index in member.admissible_indices)
        ),
        mandatory_dates=(
            None
            if member.mandatory_indices is None
            else frozenset(member.dates[index] for index in member.mandatory_indices)
        ),
    )


#: Given a date and the scores of everyone ranking on it, the winners.
#: ⚠ It receives scores and a date, so it CANNOT name a bar, a price or a fill.
#: That is a narrower claim than "look-ahead is impossible": ``select`` is
#: ordinary code and could close over anything. What is structural is that every
#: score reaching it is a causal per-bar value, and that the runner hands it no
#: route to the future.
CrossSectionalSelect = Callable[[date, Mapping[int, float]], Set[int]]


def evaluate_cross_sectional(
    *,
    members: Mapping[int, CrossSectionalMember],
    select: CrossSectionalSelect,
    min_participants: int,
    kind: SignalKind = "entry",
) -> dict[int, list[StrategySignal]]:
    """Run a ranked strategy over a panel: one verdict per member per bar.

    ``min_participants`` is the smallest cross-section the ranking rule is
    defined on. Below it, every participant at that date is
    ``not_evaluable("thin_cross_section")`` — the runner's call, not
    ``select``'s, because an empty return from ``select`` cannot be told apart
    from "the panel was too thin", and criterion 8 exists to keep exactly that
    distinction countable.

    ⚠ ``select`` returning a key that did not participate RAISES. Ignoring it
    would hide a selector bug behind a plausible-looking ledger, and honouring
    it would fire a signal on a bar the runner already judged unevaluable.
    """
    if min_participants < 1:
        raise ValueError(f"min_participants must be at least 1, got {min_participants}")

    staged = {key: stage_cross_sectional_member(member, kind=kind) for key, member in members.items()}

    by_date: dict[date, dict[int, float]] = {}
    for key, member_staged in staged.items():
        for when, value in member_staged.scores.items():
            by_date.setdefault(when, {})[key] = value

    winners_by_date: dict[date, frozenset[int]] = {}
    thin_dates: set[date] = set()
    # Sorted so a `select` with any state sees dates in time order, and so two
    # runs over the same panel are identical.
    for when in sorted(by_date):
        scores = by_date[when]
        if len(scores) < min_participants:
            thin_dates.add(when)
            continue
        winners = frozenset(select(when, scores))
        unknown = winners - scores.keys()
        if unknown:
            raise ValueError(
                f"select returned {sorted(unknown)} on {when}, which did not participate in that "
                "cross-section — every winner must be one of the members offered"
            )
        winners_by_date[when] = winners

    resolved: dict[int, list[StrategySignal]] = {}
    for key, member_staged in staged.items():
        member = members[key]
        signals: list[StrategySignal] = []
        for index, verdict in enumerate(member_staged.verdicts):
            if verdict is not None:
                signals.append(verdict)
                continue
            when = member.dates[index]
            if when in thin_dates:
                signals.append(
                    StrategySignal(verdict="not_evaluable", signal_index=index, kind=kind, reason="thin_cross_section")
                )
            else:
                signals.append(
                    resolve_participating_bar(
                        when=when,
                        index=index,
                        kind=kind,
                        selected=key in winners_by_date[when],
                        admissible_dates=member_staged.admissible_dates,
                        mandatory_dates=member_staged.mandatory_dates,
                    )
                )
        resolved[key] = signals
    return resolved
