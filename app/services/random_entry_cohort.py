"""Phase 5e-5b — the 1,000-strategy random-entry synthetic control.

Spec: ``docs/proposals/ta/2026-08-07-bounded-backtester.md`` §9 (*the harness
itself*) and §8 (stage 5e-5b). Parent
``docs/proposals/ta/strategy-catalogue-and-backtest-validity.md`` §5, the block
headed *"Acceptance for the harness itself"*. Refs #2240.

⚠⚠ WHAT THIS IS FOR, IN ONE SENTENCE: it is the null distribution against which
a strategy's number is read, so that *"a harness that finds edge in noise is
broken regardless of what else it explains"* becomes a measurement rather than a
hope.

THE CONSTRUCTION IS A PERMUTATION, AND THAT IS WHAT MAKES THE MATCH EXACT
------------------------------------------------------------------------
§9 requires the cohort *"matched to each real strategy's universe, dates,
exposure and turnover, under the same cost model, with the seed recorded"*, and
fixes nothing about HOW the randomisation is done. Two constructions were
available:

1. **Calibrate a Bernoulli entry rate** per series until the cohort's exposure
   and turnover land near the real sleeve's. Rejected: the match is then an
   optimisation with a tolerance nobody can source, and the tolerance becomes a
   free parameter of the null.
2. **Permute the entries this strategy actually made** — keep, per series, the
   realised trade COUNT and the multiset of HOLDING PERIODS exactly, and redraw
   only WHERE each position opens. Adopted.

Under (2) the trade count matches by construction and is asserted, not
tolerated; the holding-period distribution matches by construction; and the only
residual is exposure, which drifts because equal-weight sizing makes a
position's capital-days depend on how many siblings are open beside it. That
residual is MEASURED and reported (``MatchResidual``) rather than assumed small.

⚠ The null this encodes is precisely *"the entry timing carries no information
about the returns that follow it"*. It is the Monte-Carlo permutation test
familiar from Aronson, *Evidence-Based Technical Analysis* (2006) ch. 6 and
Masters, *Permutation and Randomization Tests for Trading System Development*
(2018) — not a re-derivation, but neither book fixes the placement measure, so
that piece is OURS and is frozen in ``COHORT_MODEL_ID``.

⚠⚠ WHAT IS DELIBERATELY *NOT* RANDOMISED, because randomising it would change
the null being tested: the universe, the date axis, the cost model, the sizing
rule, the quarantine arm and the exit-side accounting. A member differs from the
real sleeve in the ENTRY BARS and in nothing else.

TWO THRESHOLDS, AND BOTH ARE THE PARENT'S OWN WORDS
---------------------------------------------------
> *"the mean net return of the random cohort must lie within its own 95%
> bootstrap CI of zero, and each real strategy's Sharpe must exceed the 95th
> percentile of the random cohort's to count as evidence at all"*

Both are implemented literally in ``evaluate_control``, and ``passed`` is their
conjunction. ⚠ A third quantity — where the strategy's own RETURN falls in the
cohort's return distribution — is computed and reported beside them because it
is the statistic the permutation-test literature above actually uses, and
because a reader comparing the two thresholds needs it to tell "no edge" from
"the threshold is measuring the wrong thing". It is reported and it does NOT
gate; changing what gates is a spec amendment, not a module decision.

⚠ THE COHORT IS NOT A TRIAL COUNT. ``trial_register`` counts searches of price
data for an edge to ship. A control distribution is not such a search — no
member is a candidate for promotion, and none can be selected into one — so the
1,000 members do not enter criterion 6's ``M``. Recorded here rather than left
implicit, because the register's own header says an undercounted ``M`` raises
the Deflated Sharpe.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from decimal import Decimal
from typing import Final

import numpy as np
import numpy.typing as npt

from app.services.cost_model import buy_price, sell_price

#: The construction this module implements, frozen. ⚠ It names the PLACEMENT
#: MEASURE and the matching rule, both of which are ours (see the header), so a
#: cohort number computed under a different placement is not comparable with one
#: computed under this. Stored on the result row for that reason.
COHORT_MODEL_ID: Final = "permuted-entry-uniform-gap-v1"

#: §9's cohort size, verbatim. ⚠ A ``SPEC_`` literal and not a tuning knob: the
#: 95th percentile of a 1,000-member sample is its 950th order statistic, and a
#: smaller cohort makes that percentile a coarser estimate of the same quantity
#: while the acceptance wording stays the same.
SPEC_COHORT_SIZE: Final = 1000

#: §9's Sharpe threshold, verbatim.
SPEC_SHARPE_PERCENTILE: Final = 95.0

#: §9's interval, verbatim — *"its own 95% bootstrap CI"*.
SPEC_CI_PERCENT: Final = 95.0

#: The seed §9 requires recorded. ⚠ DECLARED, not drawn: *"with a recorded
#: seed"* is unsatisfiable by a seed nobody wrote down, and a default drawn from
#: the clock would make two runs of the same evaluation differ in a number
#: nobody chose. Same posture as ``compute_metrics``'s ``bootstrap_seed``.
COHORT_ROOT_SEED: Final = 20260808

#: Resamples for the cohort-mean interval. ⚠ Efron & Tibshirani (1993) ch. 13
#: put the floor for an INTERVAL at 1,000; stage 5e-2 chose 2,000 over the same
#: floor and this matches it so the two intervals in one result are not drawn
#: from differently-sized bootstraps.
COHORT_BOOTSTRAP_RESAMPLES: Final = 2000


def member_seed(index: int) -> np.random.SeedSequence:
    """This member's independent stream, derived from the recorded root.

    ⚠ ``spawn_key`` and not ``SeedSequence.spawn()``: ``spawn`` is STATEFUL on
    the parent, so a run split across shards would hand member 700 a different
    stream depending on which shard drew it. Keying the child by index makes the
    member's stream a pure function of ``(COHORT_ROOT_SEED, index)``, which is
    what "recorded seed" has to mean if the record is to reproduce the run.
    """
    if index < 0:
        raise ValueError(f"member index must be non-negative, got {index}")
    return np.random.SeedSequence(entropy=COHORT_ROOT_SEED, spawn_key=(index,))


# ---------------------------------------------------------------------------
# Placement
# ---------------------------------------------------------------------------


def slack(*, eligible: int, holds: npt.NDArray[np.int64]) -> int:
    """Eligible bars left over once every hold is laid end to end.

    Negative means the series cannot carry its own realised trade population in
    the eligible space — which is a CONTRADICTION rather than a rare shape, and
    the caller counts it instead of quietly shortening a hold.
    """
    if eligible < 1:
        return -1
    return (eligible - 1) - int(holds.sum())


def place_entries(
    rng: np.random.Generator,
    *,
    eligible: int,
    holds: npt.NDArray[np.int64],
) -> tuple[npt.NDArray[np.int64], npt.NDArray[np.int64]]:
    """Random non-overlapping entry ordinals for one series, and their holds.

    Ordinals index the series' ELIGIBLE FILL BARS — the bars on which this
    strategy could have opened a position at all (a usable open, inside the
    evaluation window, past the strategy's declared warm-up). Working in that
    space rather than in raw bar indices is what stops a random member trading
    on a bar the real strategy was structurally unable to trade on.

    ⚠⚠ THE HOLDS ARE IN THE SAME ORDINAL SPACE, so a member's position occupies
    the same NUMBER of tradeable bars as the real one it was permuted from, and
    its span in calendar dates follows from wherever it landed. §5.3 and §8.4
    record what happens when an instrument-axis count is read onto a
    panel-axis window; the ordinal space here is the instrument's own, and the
    panel span is derived from it rather than assumed equal to it.

    The construction, which is OURS (no source rule fixes it) and frozen in
    ``COHORT_MODEL_ID``:

    1. **Permute the holds.** Their ORDER carries information — a long hold
       following a short one is a fact about the signal — and only their
       multiset is being matched.
    2. **Draw the leading gap of each position** as ``m`` iid uniform draws on
       ``{0 … slack}``, SORTED. Sorting makes the gaps non-negative by
       construction, so positions never overlap and no rejection loop is needed.
       ⚠ This is uniform over the sorted DRAW, not over the placements
       themselves (the two differ in the same way a multiset differs from a
       composition). Neither is fixed by any source; this one is declared, and
       the alternative is named here so a later reader can see it was a choice.
    3. **Offset by the exclusive prefix sum of the permuted holds**, which turns
       leading gaps into absolute entry ordinals.

    ⚠ TOUCHING IS PERMITTED — a position may open on the ordinal a previous one
    closed on. That is ``position_builder``'s own rule 4 (*"a closed position
    whose close date equals a later entry's fill bar does NOT suppress it"*), so
    forbidding it here would make the cohort's placement space strictly smaller
    than the real strategy's.

    Raises when the series cannot carry its holds; the caller counts it.
    """
    count = int(holds.size)
    if count == 0:
        empty = np.empty(0, dtype=np.int64)
        return empty, empty
    free = slack(eligible=eligible, holds=holds)
    if free < 0:
        raise ValueError(
            f"{count} holds totalling {int(holds.sum())} bars do not fit in {eligible} eligible bars — the series "
            "cannot carry its own realised trade population, and shortening a hold would change the match"
        )
    permuted = rng.permutation(holds)
    leading = np.sort(rng.integers(0, free + 1, size=count))
    # Exclusive prefix sum: position i starts after every hold before it.
    consumed = np.concatenate(([0], np.cumsum(permuted[:-1])))
    return (leading + consumed).astype(np.int64), permuted.astype(np.int64)


# ---------------------------------------------------------------------------
# Pricing — the vectorised form of the cost model, pinned to the cost model
# ---------------------------------------------------------------------------


def net_entry_prices(
    opens: npt.NDArray[np.float64],
    half_spreads: npt.NDArray[np.float64],
) -> npt.NDArray[np.float64]:
    """``cost_model.buy_price`` over an array. Pinned to it by test.

    ⚠⚠ THIS IS A SECOND IMPLEMENTATION OF ARITHMETIC THAT ALREADY EXISTS, and
    the reason is scale, not preference: the cohort prices ~3.1 billion legs and
    ``buy_price`` takes ``Decimal``. A ``Decimal`` round trip per leg is a
    multi-day run. The mitigation is a BRIDGE TEST rather than a comment —
    ``tests/test_random_entry_cohort.py`` asserts these two functions agree with
    ``buy_price``/``sell_price`` to float tolerance across the whole band table,
    so a change to the cost model's arithmetic that is not mirrored here fails a
    test instead of silently re-pricing the null.
    """
    return opens * (1.0 + half_spreads)


def net_exit_prices(
    opens: npt.NDArray[np.float64],
    half_spreads: npt.NDArray[np.float64],
) -> npt.NDArray[np.float64]:
    """``cost_model.sell_price`` over an array. Pinned to it by test."""
    return opens * (1.0 - half_spreads)


def decimal_net_prices(open_price: Decimal, half_spread: Decimal) -> tuple[Decimal, Decimal]:
    """The same pair through the Decimal path, for the bridge test to compare."""
    return buy_price(open_price, half_spread=half_spread), sell_price(open_price, half_spread=half_spread)


# ---------------------------------------------------------------------------
# The cohort's own statistics
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class MemberOutcome:
    """One cohort member, reduced to what the acceptance and the match read."""

    index: int
    sharpe: float
    total_return_pct: float
    exposure_time_pct: float
    turnover_annualised: float
    trade_count: int

    def __post_init__(self) -> None:
        if self.index < 0:
            raise ValueError(f"member index must be non-negative, got {self.index}")
        if self.trade_count < 0:
            raise ValueError(f"trade count must be non-negative, got {self.trade_count}")


@dataclass(frozen=True)
class MatchResidual:
    """How closely the cohort reproduced what §9 asked it to match.

    ⚠ TRADE COUNT IS EXACT AND IS ASSERTED; the other two are REPORTED. The
    permutation preserves the count by construction, so a mismatch is a bug and
    is refused. Exposure and turnover are not preserved by construction — equal
    weight makes a position's capital-days depend on how many siblings are open
    beside it, and the rebalance charges turnover the ledger never emitted — so
    a threshold on either would be an invented constant. The size of the drift
    is the number a reader needs, and it is here.
    """

    strategy_trade_count: int
    cohort_mean_trade_count: float
    strategy_exposure_time_pct: float
    cohort_mean_exposure_time_pct: float
    strategy_turnover_annualised: float
    cohort_mean_turnover_annualised: float

    @property
    def trade_count_matches(self) -> bool:
        """Exact, because the permutation preserves the count per series."""
        return math.isclose(self.cohort_mean_trade_count, float(self.strategy_trade_count), rel_tol=0.0, abs_tol=1e-9)

    @property
    def exposure_delta_pct_points(self) -> float:
        return self.cohort_mean_exposure_time_pct - self.strategy_exposure_time_pct

    @property
    def turnover_delta(self) -> float:
        return self.cohort_mean_turnover_annualised - self.strategy_turnover_annualised


def percentile_bootstrap_mean(
    values: npt.NDArray[np.float64],
    *,
    seed: int,
    resamples: int = COHORT_BOOTSTRAP_RESAMPLES,
    interval_pct: float = SPEC_CI_PERCENT,
) -> tuple[float, float, float]:
    """Mean, and its percentile-bootstrap interval. Returns ``(mean, low, high)``.

    ⚠ IID, NOT CLUSTERED, and the contrast with stage 5e-2 is the point. The
    block bootstrap exists because TRADES are correlated across instruments on
    the same day. COHORT MEMBERS are not: each is drawn from its own
    ``SeedSequence`` child and its entries are independent of every other
    member's, so the resampling unit is the member and no block structure
    applies.

    ⚠ What the interval measures is the sampling variability OF THE
    RANDOMISATION, conditional on the price path every member trades. It is not
    a confidence interval for "the return of a random strategy in general", and
    reading it as one would attribute the corpus's own drift to sampling noise.

    Efron & Tibshirani (1993) ch. 13, percentile method — the same citation and
    the same first-order-accuracy caveat as ``block_bootstrap``.
    """
    if values.size == 0:
        raise ValueError("no cohort members: an empty cohort has no mean and no interval")
    if resamples < 1:
        raise ValueError(f"resamples must be positive, got {resamples}")
    if not 0.0 < interval_pct < 100.0:
        raise ValueError(f"interval_pct must be inside (0, 100), got {interval_pct}")
    rng = np.random.default_rng(seed)
    draws = rng.integers(0, values.size, size=(resamples, values.size))
    means = values[draws].mean(axis=1)
    tail = (100.0 - interval_pct) / 2.0
    low, high = np.percentile(means, [tail, 100.0 - tail])
    return float(values.mean()), float(low), float(high)


def cohort_threshold(values: npt.NDArray[np.float64], *, percentile: float) -> float:
    """The cohort's ``percentile`` as an ORDER STATISTIC, never an interpolation.

    ⚠⚠ ``method="inverted_cdf"`` AND NOT NUMPY'S DEFAULT, and the difference is
    a decision, not a rounding. NumPy's default is linear interpolation on the
    ``(n-1)`` grid, which for a 1,000-member cohort puts the 95th percentile
    between the **950th and 951st** sorted members — a value **no member
    achieved**. §9 asks a real strategy to exceed *"the 95th percentile of the
    random cohort's"* Sharpe, and a cut that sits between two draws refuses a
    strategy that beat every draw at or below the declared rank, for a value the
    null never produced.

    ``inverted_cdf`` is the empirical-CDF inverse — Hyndman & Fan (1996) type 1,
    the nearest-rank definition — so at ``n = 1000`` and 95% the threshold is
    exactly the 950th order statistic, which is what this module's header claims
    and what a permutation test's *"at most 5% of the null lies strictly
    above"* means. (Caught at Codex checkpoint 2; the code and its own docstring
    disagreed.)

    ⚠ THE BOOTSTRAP INTERVAL DELIBERATELY DOES **NOT** MOVE WITH THIS.
    ``percentile_bootstrap_mean`` keeps NumPy's default, matching stage 5e-2's
    shipped ``BOOTSTRAP_MODEL_ID`` convention: the two are different quantities —
    an interval estimator over bootstrap replications (Efron & Tibshirani ch. 13,
    where interpolation is standard) against a decision cut over a finite cohort
    of strategies — and silently re-estimating 5e-2's intervals from here would
    change a shipped number for a reason that has nothing to do with it.
    """
    if values.size == 0:
        raise ValueError("no cohort members: an empty null distribution has no percentile")
    return float(np.percentile(values, percentile, method="inverted_cdf"))


@dataclass(frozen=True)
class SyntheticControl:
    """§9's synthetic control for ONE strategy, as it is stored and gated on.

    ⚠ Every field is a DECLARED INPUT or a MEASURED OUTPUT — there is nothing
    here a reader has to re-derive, because criterion 11's argument applies to a
    control as much as to a result: the same strategy against a cohort built
    under a different model id, size or seed is a different measurement.
    """

    model_id: str
    cohort_size: int
    root_seed: int
    #: The cohort's mean total net return and its percentile-bootstrap interval.
    mean_return_pct: float
    mean_return_ci_low_pct: float
    mean_return_ci_high_pct: float
    #: The cohort's Sharpe at ``SPEC_SHARPE_PERCENTILE``, and the strategy's own.
    sharpe_percentile: float
    cohort_sharpe_threshold: float
    strategy_sharpe: float
    #: ⚠ REPORTED, NOT GATED. Where the strategy's own return falls in the
    #: cohort's return distribution — the statistic the permutation-test
    #: literature uses. See the module header for why it does not gate.
    cohort_return_threshold_pct: float
    strategy_return_pct: float

    def __post_init__(self) -> None:
        if self.cohort_size < 1:
            raise ValueError(f"cohort size must be positive, got {self.cohort_size}")
        if not self.model_id:
            raise ValueError("model_id is required: a control with no declared construction cannot be compared")
        if self.mean_return_ci_low_pct > self.mean_return_ci_high_pct:
            raise ValueError(
                f"cohort mean interval [{self.mean_return_ci_low_pct}, {self.mean_return_ci_high_pct}] is inverted"
            )
        if not 0.0 < self.sharpe_percentile < 100.0:
            raise ValueError(f"sharpe_percentile must be inside (0, 100), got {self.sharpe_percentile}")

    @property
    def mean_return_ci_contains_zero(self) -> bool:
        """§9's FIRST threshold, literally: *"within its own 95% bootstrap CI of zero"*."""
        return self.mean_return_ci_low_pct <= 0.0 <= self.mean_return_ci_high_pct

    @property
    def sharpe_exceeds_cohort(self) -> bool:
        """§9's SECOND threshold, literally. Strict — *"must exceed"*."""
        return self.strategy_sharpe > self.cohort_sharpe_threshold

    @property
    def return_exceeds_cohort(self) -> bool:
        """The reported permutation-test form. ⚠ Does not gate."""
        return self.strategy_return_pct > self.cohort_return_threshold_pct

    @property
    def passed(self) -> bool:
        """BOTH parent thresholds. ⚠ Conjunction, and §9 says so: *"acceptance is BOTH"*."""
        return self.mean_return_ci_contains_zero and self.sharpe_exceeds_cohort


def evaluate_control(
    members: tuple[MemberOutcome, ...],
    *,
    strategy_sharpe: float,
    strategy_return_pct: float,
    root_seed: int = COHORT_ROOT_SEED,
    percentile: float = SPEC_SHARPE_PERCENTILE,
) -> SyntheticControl:
    """Assemble §9's control from a finished cohort. Pure; reads no database.

    ⚠ ``members`` IS THE WHOLE COHORT, and the size is stored rather than
    assumed: a run that lost members to a refusal produces a control whose
    percentile is estimated from fewer order statistics, and the reader must be
    able to see that on the row rather than infer it from the absence of a
    complaint.
    """
    if not members:
        raise ValueError("no cohort members: §9's control cannot be evaluated against an empty null distribution")
    seen = {member.index for member in members}
    if len(seen) != len(members):
        raise ValueError(
            f"{len(members)} members carry {len(seen)} distinct indices — a duplicated member is one draw counted "
            "twice, which narrows the null distribution it is supposed to widen"
        )
    returns = np.asarray([member.total_return_pct for member in members], dtype=np.float64)
    sharpes = np.asarray([member.sharpe for member in members], dtype=np.float64)
    mean, low, high = percentile_bootstrap_mean(returns, seed=root_seed)
    return SyntheticControl(
        model_id=COHORT_MODEL_ID,
        cohort_size=len(members),
        root_seed=root_seed,
        mean_return_pct=mean,
        mean_return_ci_low_pct=low,
        mean_return_ci_high_pct=high,
        sharpe_percentile=percentile,
        cohort_sharpe_threshold=cohort_threshold(sharpes, percentile=percentile),
        strategy_sharpe=strategy_sharpe,
        # ⚠ THE SAME PERCENTILE, DELIBERATELY, and it is not independently
        # configurable. §9 names one cut — *"the 95th percentile of the random
        # cohort's"* — and the return figure exists so a reader can locate the
        # strategy in the SAME place in a second marginal distribution. Two
        # different percentiles here would make the pair incomparable while
        # looking like a generalisation. (Review bot NITPICK, PR #2395.)
        cohort_return_threshold_pct=cohort_threshold(returns, percentile=percentile),
        strategy_return_pct=strategy_return_pct,
    )


def match_residual(
    members: tuple[MemberOutcome, ...],
    *,
    strategy_trade_count: int,
    strategy_exposure_time_pct: float,
    strategy_turnover_annualised: float,
) -> MatchResidual:
    """What §9's *"matched … on exposure and turnover"* actually came out at."""
    if not members:
        raise ValueError("no cohort members: nothing to compare the strategy against")
    return MatchResidual(
        strategy_trade_count=strategy_trade_count,
        cohort_mean_trade_count=float(np.mean([member.trade_count for member in members])),
        strategy_exposure_time_pct=strategy_exposure_time_pct,
        cohort_mean_exposure_time_pct=float(np.mean([member.exposure_time_pct for member in members])),
        strategy_turnover_annualised=strategy_turnover_annualised,
        cohort_mean_turnover_annualised=float(np.mean([member.turnover_annualised for member in members])),
    )


__all__ = [
    "COHORT_BOOTSTRAP_RESAMPLES",
    "COHORT_MODEL_ID",
    "COHORT_ROOT_SEED",
    "SPEC_CI_PERCENT",
    "SPEC_COHORT_SIZE",
    "SPEC_SHARPE_PERCENTILE",
    "MatchResidual",
    "MemberOutcome",
    "SyntheticControl",
    "cohort_threshold",
    "decimal_net_prices",
    "evaluate_control",
    "match_residual",
    "member_seed",
    "net_entry_prices",
    "net_exit_prices",
    "percentile_bootstrap_mean",
    "place_entries",
    "slack",
]
