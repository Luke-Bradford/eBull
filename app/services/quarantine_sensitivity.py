"""Phase 5e-5a — criterion 9's census, and the delta its sensitivity arm reports.

Spec: ``docs/proposals/ta/2026-08-07-bounded-backtester.md`` §9 acceptance C9 and
§8 (stage 5e-5a). Parent
``docs/proposals/ta/strategy-catalogue-and-backtest-validity.md`` criterion 9 —
*"Report the count and share of bars/trades excluded per strategy, and run one
sensitivity arm with conservative handling, so exclusion is visible rather than
assumed harmless."* Refs #2240.

PURE. No database, no corpus, no strategy. It takes what a run measured and
turns it into the two objects C9 names — a census and a delta — so the arithmetic
is table-testable and the full-population sweep
(``scripts/verify_2240_quarantine_sensitivity.py``) owns only the reading.

⚠⚠ THE DELTA IS SIGNED, AND ITS DIRECTION IS NOT KNOWN IN ADVANCE.
---------------------------------------------------------------------------
It is tempting to assert that admitting quarantined bars can only ADD trades —
a masked close suppresses an indicator, so restoring it should restore signals.
It does not follow. A restored close also feeds the exit side: a position that
closes earlier under the admitted arm frees its instrument for a different
entry, and one that closes later blocks one (§3.1's pyramiding collapse). So the
admitted arm can hold FEWER trades than the masked arm on the same series, and
an assertion of direction would fire on correct code. What is asserted instead
is that both arms read the SAME bars — see ``QuarantineCensus``.

⚠⚠ A METRIC NULL IN ONE ARM HAS NO DELTA, AND IT IS NOT ZERO.
---------------------------------------------------------------------------
``profit_factor``, ``sortino`` and ``effective_sample_size`` are legitimately
``None`` (``strategy_statistics``' header gives each one's reason). A comparison
that subtracted a missing number from a present one, or quietly emitted ``0.0``
for "unchanged", would report the most interesting case — an arm where the
denominator population appeared or vanished — as the least interesting one.
``MetricDelta`` carries a state instead, and ``delta`` is ``None`` unless both
sides are present.

⚠ WHAT THIS MODULE DOES NOT DECIDE. There is no materiality threshold here and
none anywhere else in phase 5 for the quarantine arm. §3.4's ambiguity pair has
one because the spec declares it; criterion 9 asks only that the exclusion be
*visible*, and inventing a "delta below X is fine" cut would be exactly the
made-up constant `.claude/CLAUDE.md` forbids. The promotion gate refuses on the
comparison being ABSENT, never on its size.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Final, Literal, get_args

from app.services.research_price_structure_store import QUARANTINE_ARMS, QuarantineArm
from app.services.strategy_registry import NOT_EVALUABLE_REASONS
from app.services.strategy_statistics import StrategyMetrics

#: ⚠⚠ TRANSCRIBED from parent criterion 7's own sentence, NOT derived from
#: ``StrategyMetrics``. C9 says the delta covers *"every C7 metric"*, so this
#: list is the claim being made and a list built by reflection over the
#: dataclass would agree with itself no matter what the dataclass lost — the
#: tautology the #2282 lesson names. ``tests/test_quarantine_sensitivity.py``
#: bridges the two: every name here must be a field, and there must be twelve.
SPEC_CRITERION7_METRICS: Final[tuple[str, ...]] = (
    "expectancy_per_trade_pct",
    "profit_factor",
    "cagr_pct",
    "annualised_volatility_pct",
    "sharpe",
    "sortino",
    "max_drawdown_pct",
    "exposure_time_pct",
    "turnover_annualised",
    "trade_count",
    "effective_sample_size",
    "return_vs_buy_and_hold_pct",
)

#: The id of this comparison's construction, stamped on a reported run in the
#: same spirit as ``METRIC_SET_ID``: which quantities were compared, and how a
#: one-sided null was handled, is part of what a printed delta MEANS.
SENSITIVITY_MODEL_ID: Final = "criterion9-sensitivity-v1"

#: Why a delta is absent. ⚠ Four states and not a bare ``None``: "the admitted
#: arm gained a losing trade so ``profit_factor`` became computable" and "both
#: arms had none" are opposite findings and would otherwise print identically.
DeltaState = Literal["measured", "masked_null", "admitted_null", "both_null"]
DELTA_STATES: Final[frozenset[str]] = frozenset(get_args(DeltaState))


@dataclass(frozen=True)
class MetricDelta:
    """One criterion-7 metric under both arms."""

    metric: str
    masked: float | None
    admitted: float | None
    delta: float | None
    state: DeltaState

    def __post_init__(self) -> None:
        if self.metric not in SPEC_CRITERION7_METRICS:
            raise ValueError(f"{self.metric!r} is not one of criterion 7's twelve: {SPEC_CRITERION7_METRICS}")
        expected: DeltaState
        if self.masked is None and self.admitted is None:
            expected = "both_null"
        elif self.masked is None:
            expected = "masked_null"
        elif self.admitted is None:
            expected = "admitted_null"
        else:
            expected = "measured"
        if self.state != expected:
            raise ValueError(
                f"{self.metric}: state {self.state!r} against masked={self.masked!r} admitted={self.admitted!r}, "
                f"which is {expected!r}"
            )
        if (self.delta is None) != (expected != "measured"):
            raise ValueError(
                f"{self.metric}: delta {self.delta!r} is present exactly when both arms are, and is never a "
                "stand-in for an absent comparison"
            )

    @property
    def relative_pct(self) -> float | None:
        """The delta as a share of the masked value.

        ⚠ ``None`` on a zero base rather than an infinity or a large number: a
        metric that was 0.0 under masking and non-zero under admission has no
        meaningful percentage change, and printing one invites a reader to
        compare it with the others.
        """
        if self.delta is None or self.masked in (None, 0.0):
            return None
        assert self.masked is not None  # narrowed by the guard above, for pyright
        return 100.0 * self.delta / abs(self.masked)


def compare_metrics(masked: StrategyMetrics, admitted: StrategyMetrics) -> tuple[MetricDelta, ...]:
    """Criterion 7's twelve, under both arms, in the criterion's own order.

    ⚠ Raises rather than skipping when a name is missing from
    ``StrategyMetrics``. A comparison that silently covered eleven of twelve
    would satisfy C9's wording in a report and not in fact.
    """
    deltas: list[MetricDelta] = []
    for metric in SPEC_CRITERION7_METRICS:
        if not hasattr(masked, metric) or not hasattr(admitted, metric):
            raise AttributeError(
                f"StrategyMetrics carries no {metric!r}: criterion 7's twelve and the metric set have diverged"
            )
        left = getattr(masked, metric)
        right = getattr(admitted, metric)
        left_f = None if left is None else float(left)
        right_f = None if right is None else float(right)
        if left_f is None and right_f is None:
            state: DeltaState = "both_null"
        elif left_f is None:
            state = "masked_null"
        elif right_f is None:
            state = "admitted_null"
        else:
            state = "measured"
        deltas.append(
            MetricDelta(
                metric=metric,
                masked=left_f,
                admitted=right_f,
                delta=None if state != "measured" else right_f - left_f,  # type: ignore[operator]
                state=state,
            )
        )
    return tuple(deltas)


@dataclass(frozen=True)
class ArmCensus:
    """What one arm read, and what it refused.

    ``series_fail_closed`` is the OTHER exclusion channel and is deliberately
    counted beside the flagged bars: a series with no coverage row contributes
    zero bars under BOTH arms, so it never appears in a delta, and a census
    reporting only the masked fields would describe the smaller of the two
    exclusions as the whole of it.
    """

    arm: QuarantineArm
    series_evaluated: int
    series_fail_closed: int
    bars: int
    bars_flagged: int
    range_flagged: int
    return_flagged: int
    #: ``not_evaluable`` reason → count, over every signal the strategy emitted
    #: under this arm. Criterion 8's closed vocabulary; C8 requires it reported
    #: per strategy rather than collapsed to a total.
    not_evaluable: Mapping[str, int]
    trades: int

    def __post_init__(self) -> None:
        if self.arm not in QUARANTINE_ARMS:
            raise ValueError(f"unknown quarantine arm {self.arm!r}; must be one of {sorted(QUARANTINE_ARMS)}")
        counts = {
            "series_evaluated": self.series_evaluated,
            "series_fail_closed": self.series_fail_closed,
            "bars": self.bars,
            "bars_flagged": self.bars_flagged,
            "range_flagged": self.range_flagged,
            "return_flagged": self.return_flagged,
            "trades": self.trades,
        }
        for name, value in counts.items():
            if value < 0:
                raise ValueError(f"{name} must be non-negative, got {value}")
        if self.bars_flagged > self.bars:
            raise ValueError(f"bars_flagged {self.bars_flagged} exceeds bars {self.bars}")
        if max(self.range_flagged, self.return_flagged) > self.bars_flagged:
            raise ValueError(
                f"a field count ({self.range_flagged}, {self.return_flagged}) exceeds the flagged bar count "
                f"{self.bars_flagged}"
            )
        unknown = set(self.not_evaluable) - NOT_EVALUABLE_REASONS
        if unknown:
            raise ValueError(f"unknown not_evaluable reason codes {sorted(unknown)}")
        if any(count < 0 for count in self.not_evaluable.values()):
            raise ValueError(f"negative not_evaluable counts in {dict(self.not_evaluable)}")

    @property
    def flagged_bar_share_pct(self) -> float | None:
        """Criterion 9's *share* of bars excluded — ``None`` on an empty read."""
        return None if self.bars == 0 else 100.0 * self.bars_flagged / self.bars

    @property
    def quarantined_bar_signals(self) -> int:
        return self.not_evaluable.get("quarantined_bar", 0)


@dataclass(frozen=True)
class QuarantineCensus:
    """One strategy's two arms, and the exclusion they differ over.

    ⚠⚠ THE CONTROLLED-EXPERIMENT CHECK LIVES HERE. Both arms are produced from
    the same fetched rows (``research_price_structure_store.load_arms``), so the
    bar and series counts and every flag count MUST be identical — the arm
    changes what is masked, not what is read. If they differ, the two metric
    sets are not a comparison of handling, they are a comparison of populations,
    and every delta below is uninterpretable. Refused rather than reported.
    """

    strategy: str
    masked: ArmCensus
    admitted: ArmCensus

    def __post_init__(self) -> None:
        if self.masked.arm != "masked" or self.admitted.arm != "admitted":
            raise ValueError(f"arms are mislabelled: {self.masked.arm!r} / {self.admitted.arm!r}")
        shared = ("series_evaluated", "series_fail_closed", "bars", "bars_flagged", "range_flagged", "return_flagged")
        for field in shared:
            left = getattr(self.masked, field)
            right = getattr(self.admitted, field)
            if left != right:
                raise ValueError(
                    f"{self.strategy}: the arms disagree on {field} ({left} vs {right}). Both read the same rows, "
                    "so a difference means the populations differ and no delta between them is interpretable"
                )

    @property
    def trade_delta(self) -> int:
        """⚠ SIGNED — see the module header; admitting bars can lose trades."""
        return self.admitted.trades - self.masked.trades

    @property
    def trade_delta_share_pct(self) -> float | None:
        """The trade delta as a share of the masked arm's trades."""
        return None if self.masked.trades == 0 else 100.0 * self.trade_delta / self.masked.trades


__all__ = [
    "DELTA_STATES",
    "SENSITIVITY_MODEL_ID",
    "SPEC_CRITERION7_METRICS",
    "ArmCensus",
    "DeltaState",
    "MetricDelta",
    "QuarantineCensus",
    "compare_metrics",
]
