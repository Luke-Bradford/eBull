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
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, get_args

from app.services.indicator_series import IndicatorSeries, MultiIndicatorSeries, Universe

STRATEGY_SET_ID = "strategy-registry-v1"


def _module_hash() -> str:
    return hashlib.sha256(Path(__file__).read_bytes()).hexdigest()[:12]


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
NotEvaluableReason = Literal[
    "missing_volume",
    "missing_spread",
    "insufficient_warmup",
    "quarantined_bar",
    "series_break",
    "not_listed",
    "ambiguous_intrabar",
    "no_fill_bar",
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

#: The seven from parent criterion 8. `no_fill_bar` is OURS and is excluded
#: deliberately — see NotEvaluableReason. Kept as an explicit subtraction so
#: adding a parent code later cannot silently land on our side of the line.
OUR_ADDITIONAL_REASON_CODES: frozenset[str] = frozenset({"no_fill_bar"})
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
    """

    strategy_id: str
    params: Mapping[str, object]
    universe: Universe
    cost_model_id: str
    #: Source of the module DEFINING the strategy, not of this registry.
    source_hash: str

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
            },
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        )
        return f"{STRATEGY_SET_ID}+{hashlib.sha256(payload.encode()).hexdigest()[:12]}"


#: A strategy body. Invoked ONLY on bars where every declared input is
#: evaluable — see ``evaluate``.
StrategyBody = Callable[[int], bool]


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

    series: IndicatorSeries | MultiIndicatorSeries
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
        if isinstance(series, IndicatorSeries):
            if series.values[index] is None:
                warming = True
        else:
            if any(component[index] is None for component in series.components.values()):
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
