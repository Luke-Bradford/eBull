"""Pure evaluator for S-H arm 2's paired acceptance test (#2840).

Spec: ``docs/proposals/ta/2026-09-20-sh-top-band-price-gated-breakout.md``
§"Statistical contract". This module holds the statistic the contract declares,
and nothing else: it loads no price, reads no ledger row, opens no outcome and
authorises no hold-out access. The caller supplies two already-built after-cost
books per quarantine arm — S-12 (treatment) and S-4 (control).

⚠⚠ WHY THE STATISTIC LIVES HERE AND NOT IN THE RUN.

``strategy_mt1_trial``'s header states the reason for its own existence:
*"Keeping the statistic here allows the implementation to be reviewed before the
trial register and sealed-outcome gate are opened."* Arm 2 is in exactly that
position — the declaration is not frozen, ``sql/333`` bars UPDATE and DELETE,
and a contract that declares a statistic no code can compute is a contract whose
first look would have to invent one.

WHAT THIS REPLACES
------------------
The contract's first pass bar was two point-estimate inequalities:
``expectancy_per_trade_pct > 0`` for S-12, and S-12's figure above S-4's. Two
successive attempts to derive a forward-shadow floor against it were refused at
Codex checkpoint 1, and the cause sat above both: the bar carried no justified
error control, so there was nothing to power.
``docs/proposals/ta/2026-08-11-portfolio-alpha-viability-plan.md`` §5 rejects it
on its own terms too — admission needs *"a preregistered date-clustered/block or
studentized bootstrap lower bound on the portfolio net return distribution, not
a z interval on per-trade means"*, and *"A positive mean alone cannot pass."*

⚠⚠ WHICH OF THE CHOICES BELOW ARE DERIVED, AND WHICH ARE DECLARED.

A third checkpoint-1 pass (2026-09-20, 54 findings) refused this module's first
cut for presenting two DECLARED choices as derivations — a block length "fixed
by construction" at the hold cap, and an axis floor obtained by inverting a
selector's tuning cap. Both were the defect that killed the two earlier drafts,
in a new costume. ``.claude/CLAUDE.md`` permits a choice where no published
formulation exists, on the condition that it is *said* to be one and frozen in a
version hash. So the split is stated here rather than blurred:

**DERIVED — the FORMULATIONS are published; SELECTING them is still a choice.**
⚠ That distinction is finding 19 of the third pass and it is not pedantry: circular
rather than moving blocks, a percentile rather than a studentized interval, 95%
rather than another coverage, and this particular set of conjuncts are all
selections. What is derived is each method's arithmetic, not its appointment.

- **Block length** — ``block_bootstrap.optimal_block_length``: Politis & White
  (2004) with the Patton, Politis & White (2009) ``4/3`` circular correction,
  measured off the series' own autocovariance. ⚠ The first cut fixed it at
  ``MAX_HOLD_BARS`` instead, arguing that a preregistered declaration cannot
  measure a block at the look. That argument was wrong twice over: freezing a
  SELECTOR freezes the algorithm, not its output, which is the same standing the
  bootstrap's own covariance estimate has; and a fixed block does not merely
  "contain" the dependence — an ordinary block scheme attenuates lag-``k``
  covariance by roughly ``1 - k/b``, so pinning ``b`` at the dependence length
  leaves the long-run variance UNDERSTATED, which reports an interval narrower
  than the truth and errs in the passing direction. ⚠ The size of that shortfall
  depends on the covariance shape and is NOT a universal fraction; a draft of this
  header said "about two thirds", which holds for one particular shape and can
  even reverse under negative autocovariances.
  ⚠ ``block_bootstrap``'s own header attributes the ``4/3`` constant to the 2009
  correction "for the circular scheme". That attribution is contested (the 2009
  note concerns the stationary bootstrap's variance constant); the constant used
  for circular blocks is not in question, only the provenance sentence, and it is
  that module's to correct rather than this one's.
  ⚠ "Used as published" overstates the IMPLEMENTATION: ``optimal_block_length``
  makes its own choices about autocorrelation normalisation, rounding and the
  degenerate fallbacks. The variant this trial freezes is that function as it
  stands, hash included, not the paper in the abstract.
- **Circular blocks** — Politis & Romano (1992), for the reason
  ``block_bootstrap``'s header gives on this same axis shape: a moving scheme
  under-samples the first and last ``b-1`` observations, biasing a statistic
  computed over a window whose ends are a strategy's earliest and latest trades.
  ⚠ ``strategy_mt1_trial`` chose MOVING blocks; that is not overridden here, it
  is a different trial with its own frozen construction.
- **Percentile interval, two-sided 95%** — Efron & Tibshirani (1993) ch. 13, at
  ``block_bootstrap.CONFIDENCE`` so the two constructions cannot drift apart.
  ⚠ First-order accurate only; BCa is not computed, exactly as
  ``block_bootstrap`` records. The lower endpoint of an equal-tailed two-sided
  95% interval is a nominal ONE-SIDED 0.025, and it is nominal: a percentile
  bootstrap's actual tail error is approximate, and no calibration evidence is
  offered here.
- **The pairing** — identical resampled indices across both strategies in every
  replication, ``strategy_mt1_trial``'s construction.
- **The conjunction** — an intersection-union test, Berger (1982),
  *Multiparameter hypothesis testing and acceptance sampling*, Technometrics
  24(4):295-300. Requiring every component to reject at level alpha gives the
  combination size at most alpha, under no assumption about dependence between
  components. ⚠ That is conditional on each component being a VALID level-alpha
  test; the IUT confers nothing on a miscalibrated component.

**DECLARED — choices, frozen in ``TRIAL_EVALUATOR_VERSION``, not derivations.**

- **One block length for both axes**, measured on the portfolio paired-difference
  series because that is the trial's PRIMARY estimand. ⚠ A draft justified this as
  "the only series defined on every panel date"; that is false — both marginal
  return series are defined there too, and the real reason is precedence among
  estimands, which is a choice. ⚠⚠ The transfer is NOT validated, in two
  directions the third pass reproduced: common serial dependence cancels in the
  difference, so a difference-selected block can be far SHORTER than the treatment
  series alone would select (``b=1`` against ``42`` in one probe) and the own-return
  leg is then under-blocked; and the expectancy ratio's relevant linearised series
  is ``(return_sum - mean x count)/E[count]``, whose dependence the portfolio
  difference need not capture at all. ⚠ The cluster axis also holds ACTIVE dates
  only, so ``b`` clusters span at least ``b`` panel dates for a non-wrapping run of
  distinct dates — and a wrapping block has no ordinary chronological span at all.
  None of this establishes that over-spanning is safe; longer blocks are not
  automatically conservative.
- **10,000 replications** — ``strategy_mt1_trial``'s value, above Efron &
  Tibshirani's 1,000 floor. It REDUCES Monte Carlo noise in the 2.5% tail (250
  draws rather than 50 at ``block_bootstrap.RESAMPLES``). ⚠ It is not an accuracy
  guarantee: endpoint uncertainty also depends on the resampling distribution's
  shape near the quantile, and none of it says anything about the bootstrap's own
  calibration.
- **The frozen seed**, date-derived, the idiom ``BACKTEST_BOOTSTRAP_SEED`` uses.
- **The union cluster axis** rather than the intersection, so the control keeps
  its own full-population estimand.
- **The estimand starts at the first axis date's CLOSE.** ``equity[0]`` is that
  close, so ``equity[1]/equity[0] - 1`` is the first return and any date-zero
  activity sits inside the base rather than in a return. Declared because the
  input contract cannot verify that date zero was inert.

⚠⚠ THE PAIRING IS WHAT MAKES THE COVARIANCE UNNECESSARY BEFORE THE LOOK.

The binding leg compares ``mean_12 - mean_4``, whose variance needs S-12's
dispersion AND the covariance. Neither exists before S-12 runs, and S-12 must not
run before the freeze. Borrowing S-4's marginal standard error bounds the
difference's SE in neither direction, because the covariance is unsigned — a
refused draft's error, recorded in ``docs/review-prevention-log.md``.

Resampling the same block indices for both strategies removes the problem rather
than estimating around it: the difference is formed inside each replication, so
its sampling distribution is produced directly and the covariance is induced by
the data at the look.

⚠ IT DOES NOT MAKE A POWER CALCULATION AVAILABLE, and the two were conflated in
both refused drafts. Sizing a study still needs a variance in advance. ⚠ Nor is
the reverse true: an ABSOLUTE economic minimum effect can be DECLARED without any
variance — the variance is what converts it into a sample size. The contract's
§"Relevance horizon" says so; the first cut of this module claimed the opposite.

TWO AXES, BECAUSE TWO DOCUMENTS REQUIRE DIFFERENT UNITS
-------------------------------------------------------
1. **Portfolio** — daily after-cost sleeve returns on the run's shared
   ``in_sample_axis``. §5's literal unit. Both strategies in one ``run_backtest``
   invocation are built on the same predeclared panel axis (``backtest_run``
   ``_measure``: ``dates = corpus.in_sample_axis``), so the two curves are
   element-wise comparable. ⚠ THIS MODULE CANNOT VERIFY THAT PROVENANCE — it
   checks shapes and date membership, and equal-length curves from two different
   runs would pass. The caller that wires this to a run owns the identity
   checks (strategy versions, run id, namespace, ambiguity arm, cost basis,
   sizing rule, corpus version); see the contract's §"What this does not
   resolve".
2. **Per-trade expectancy, clustered by entry fill date** — the decision metric
   ``.claude/skills/quant/cost-aware-viability.md`` mandates. Cluster key and its
   reason are ``block_bootstrap.cluster_by_date``'s, unchanged. ⚠ It is NOT the
   unit ``ForwardShadowFloor`` counts: that counts one strategy's resolved
   SIGNAL-BAR dates, this is two strategies' union of ENTRY-FILL dates after
   trade selection. The two are not interchangeable and the contract says so.

⚠ A SPARSE-AXIS REFUSAL IS PARTLY A MATTER OF DRAW LUCK, and that is stated
rather than hidden. A replication that happens to select no trade for one
strategy is refused as 0/0. Whether such a replication occurs depends on the seed
and the replication count, so the absence of one does not establish that the
axis adequately supports both books. ``MIN_SUPPORTING_CLUSTERS`` below is a hard
per-strategy precondition checked before any draw, so the deterministic part of
that guard does not depend on the RNG.
"""

from __future__ import annotations

import hashlib
import math
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Final

import numpy as np
import numpy.typing as npt

from app.services.block_bootstrap import CONFIDENCE, MIN_CLUSTERS, DateClusters, optimal_block_length

#: The contract string the frozen declaration will carry. ⚠ Bumped from
#: ``sh-cheapest-band-price-gate-2026-09-20`` by the amendment this module
#: implements: the pass bar changed, so the contract changed, and a digest over
#: the old string would attest a rule that no longer exists.
TRIAL_CONTRACT_VERSION: Final = "sh-cheapest-band-price-gate-2026-09-20-v2"

#: ⚠⚠ BOTH ARE REQUIRED, AND THE NAMES ARE CHECKED. The contract makes ``masked``
#: and ``admitted`` conjuncts; evaluating one, or evaluating two arms under any
#: other names, would report a pass for an experiment the declaration does not
#: describe. The first cut checked only that the supplied names were distinct,
#: which a singleton or a pair of invented names passed.
DECLARED_ARMS: Final[tuple[str, ...]] = ("masked", "admitted")

#: Replications. A DECLARED choice; see the header.
RESAMPLES: Final = 10_000

#: Frozen before the look. Date-derived, as ``BACKTEST_BOOTSTRAP_SEED`` is.
BOOTSTRAP_SEED: Final = 2_026_092_000

#: ⚠ The only published floor available, and it is ``block_bootstrap``'s own:
#: fewer than two clusters and there is no serial structure to measure. The first
#: cut carried a floor of 170, obtained by inverting ``optimal_block_length``'s
#: ``b_max`` tuning cap. That inversion is a design choice, not a theorem — a cap
#: that clamps a selector says nothing about which sample sizes admit valid
#: inference — and it is gone. **No sample-size floor certifying 2.5%-tail
#: coverage is derivable here, and none is invented.**
MIN_SUPPORTING_CLUSTERS: Final[int] = MIN_CLUSTERS

#: Resamples evaluated per batch — bounds peak memory exactly as
#: ``block_bootstrap._BATCH`` does, and does not affect the result: the batches
#: are concatenated and the RNG stream is continuous.
_BATCH: Final = 200


def _code_hash() -> str:
    return hashlib.sha256(Path(__file__).read_bytes()).hexdigest()[:12]


TRIAL_EVALUATOR_VERSION: Final = f"s12-paired-date-block-iut-v2+{_code_hash()}"


class S12PairedTrialRefused(ValueError):
    """The declared statistic cannot be computed from the supplied evidence.

    ⚠ A refusal is an outcome, not an error to route around. Every branch below
    is a state in which a number could be produced but would not mean what the
    contract says it means.
    """


@dataclass(frozen=True)
class OneSidedBound:
    """A point estimate and the two-sided 95% interval around it.

    ⚠ Only ``lower`` decides. ``high`` is carried so a reader can see the width
    that produced the decision, which a bare bound hides.
    """

    point: float
    lower: float
    high: float

    @property
    def positive(self) -> bool:
        return self.lower > 0.0

    #: Variance of the replications this bound was taken from. ⚠ Carried rather
    #: than recomputed from the endpoints: ``lower == high`` is neither necessary
    #: nor sufficient for "the statistic did not vary". A distribution of 9,800
    #: zeros and 200 ones has equal 2.5%/97.5% endpoints and positive variance,
    #: and a genuinely constant statistic can still show endpoints differing in
    #: the last bit from summation order. Both were reproduced at checkpoint 1.
    replication_variance: float


@dataclass(frozen=True)
class PairedBooks:
    """One quarantine arm's two after-cost books, on the run's shared axis.

    ``treatment_equity`` and ``control_equity`` are the sleeve equity paths
    ``equity_curve.EquityCurve.equity`` produces, both indexed on ``dates``.
    ``treatment_clusters`` and ``control_clusters`` are each strategy's trades
    reduced by ``block_bootstrap.cluster_by_date``.
    """

    arm: str
    dates: tuple[date, ...]
    treatment_equity: npt.NDArray[np.float64]
    control_equity: npt.NDArray[np.float64]
    treatment_clusters: DateClusters
    control_clusters: DateClusters


@dataclass(frozen=True)
class ArmVerdict:
    """The four bounds one quarantine arm contributes to the conjunction."""

    arm: str
    portfolio_own: OneSidedBound
    portfolio_difference: OneSidedBound
    expectancy_own: OneSidedBound
    expectancy_difference: OneSidedBound
    portfolio_observations: int
    expectancy_clusters: int
    #: Measured off the portfolio paired-difference series and applied to both
    #: axes — a declared transfer, see the header.
    block_length: int
    #: ⚠ The selector's RAW output, before any clipping. Reported beside
    #: ``block_length`` so a reader can tell a measured block from a clipped one;
    #: without it several distinct failures are indistinguishable in the result.
    selector_block_length: int
    #: Decision dates on which each book actually traded. ⚠ NOT the union — the
    #: union hides a starved book behind a dense one.
    treatment_supporting_dates: int
    control_supporting_dates: int

    @property
    def passes(self) -> bool:
        return (
            self.portfolio_own.positive
            and self.portfolio_difference.positive
            and self.expectancy_own.positive
            and self.expectancy_difference.positive
        )


@dataclass(frozen=True)
class S12PairedTrialResult:
    """The frozen historical statistic. Not a promotion and not a capital decision."""

    arms: tuple[ArmVerdict, ...]
    conjuncts_pass: bool
    contract_version: str = TRIAL_CONTRACT_VERSION
    evaluator_version: str = TRIAL_EVALUATOR_VERSION
    resamples: int = RESAMPLES
    bootstrap_seed: int = BOOTSTRAP_SEED
    confidence: float = CONFIDENCE


def portfolio_returns(equity: npt.NDArray[np.float64], *, label: str) -> npt.NDArray[np.float64]:
    """Daily after-cost simple returns off a sleeve equity path.

    ⚠ REFUSES A NON-POSITIVE MARK rather than producing a return from it. A zero
    or negative equity point is ruin or a broken path; dividing by it yields a
    finite number with no meaning, and the bootstrap would carry it silently.
    """
    path = np.asarray(equity, dtype=np.float64)
    if path.ndim != 1 or path.size < 2:
        raise S12PairedTrialRefused(
            f"{label} equity path has shape {path.shape}; a one-dimensional path of at least two marks is required"
        )
    if not np.all(np.isfinite(path)):
        raise S12PairedTrialRefused(f"{label} equity path holds a non-finite mark")
    if not np.all(path > 0.0):
        raise S12PairedTrialRefused(f"{label} equity path reaches zero or below; a simple return is undefined there")
    returns = path[1:] / path[:-1] - 1.0
    if not np.all(np.isfinite(returns)):
        raise S12PairedTrialRefused(f"{label} equity path produces a non-finite return; the marks are not comparable")
    # ⚠ THE POSITIVITY CHECK ABOVE DOES NOT CATCH RUIN BY UNDERFLOW. Two finite
    # positive marks can still divide to a total loss — ``[1e300, 1e-300]``
    # returns exactly -1.0 and passes every check above (reproduced at
    # checkpoint 1). A return at or below -1 is ruin whatever produced it.
    if not np.all(returns > -1.0):
        raise S12PairedTrialRefused(f"{label} equity path produces a return at or below -100%; that is ruin")
    return returns


def _validate_axis(books: PairedBooks) -> None:
    """Every structural precondition the pairing rests on, checked before any draw.

    ⚠ Shape alone is NOT the pairing. Two equal-length curves from different
    periods satisfy every length check and are not paired at all; the date axis
    is what binds them, so the clusters must live on it and it must be a real
    axis (ascending, distinct). Provenance beyond that — same run, same
    namespace, same ambiguity arm — is the caller's and is named in the header.
    """
    count = len(books.dates)
    if count < 2:
        raise S12PairedTrialRefused(f"{books.arm}: a {count}-date axis cannot carry a return")
    if len(set(books.dates)) != count:
        raise S12PairedTrialRefused(f"{books.arm}: the date axis repeats a date, so a block is not a contiguous span")
    if list(books.dates) != sorted(books.dates):
        raise S12PairedTrialRefused(f"{books.arm}: the date axis is not ascending, so a block is not a contiguous span")
    for label, equity in (("treatment", books.treatment_equity), ("control", books.control_equity)):
        # ⚠ SHAPE BEFORE ``len``. A scalar array has no ``len`` and raised
        # ``TypeError: len() of unsized object`` from here — the refusal protocol
        # escaped through its own validator (reproduced at checkpoint 1; the
        # earlier test exercised only the helper, which checks shape itself).
        path = np.asarray(equity, dtype=np.float64)
        if path.ndim != 1:
            raise S12PairedTrialRefused(
                f"{books.arm}: the {label} equity path has shape {path.shape}; a one-dimensional path is required"
            )
        if path.shape[0] != count:
            raise S12PairedTrialRefused(
                f"{books.arm}: {count} dates against {path.shape[0]} {label} marks — the paired statistic needs "
                "one shared axis"
            )
    on_axis = set(books.dates)
    for label, clusters in (("treatment", books.treatment_clusters), ("control", books.control_clusters)):
        stray = sorted(set(clusters.dates) - on_axis)
        if stray:
            raise S12PairedTrialRefused(
                f"{books.arm}: {len(stray)} {label} cluster date(s) are absent from the shared axis, first "
                f"{stray[0]} — the two books are not describing one experiment"
            )


def _union_axis(
    treatment: DateClusters, control: DateClusters
) -> tuple[
    tuple[date, ...],
    npt.NDArray[np.int64],
    npt.NDArray[np.float64],
    npt.NDArray[np.int64],
    npt.NDArray[np.float64],
]:
    """Both strategies' clusters on one ascending axis, zero-filled where absent.

    ⚠ UNION, NOT INTERSECTION. A date on which only the control traded is a date
    the control's own estimand includes, and dropping it would re-scope the
    comparator to the treatment's calendar.
    """
    axis = tuple(sorted(set(treatment.dates) | set(control.dates)))
    if not axis:
        raise S12PairedTrialRefused("neither strategy produced a trade, so there is no cluster axis")
    position = {day: index for index, day in enumerate(axis)}
    counts = [np.zeros(len(axis), dtype=np.int64), np.zeros(len(axis), dtype=np.int64)]
    sums = [np.zeros(len(axis), dtype=np.float64), np.zeros(len(axis), dtype=np.float64)]
    for slot, clusters in enumerate((treatment, control)):
        if not np.all(np.isfinite(clusters.return_sums)):
            raise S12PairedTrialRefused("a cluster carries a non-finite return sum")
        # ⚠⚠ RE-CHECK ``DateClusters``'s OWN INVARIANTS, DO NOT TRUST THEM. It is a
        # frozen dataclass wrapped around WRITABLE NumPy arrays, so every check in
        # its ``__post_init__`` describes the moment of construction and nothing
        # later. Codex checkpoint 2 reproduced the consequence: setting
        # ``trade_counts[0] = 101`` after construction leaves ``trade_count`` at
        # 250 while the array sums to 350, and the pooled point estimate moved from
        # 0.50 to 0.357 with the arm still PASSING. The class itself records why the
        # two counts must agree — *"the design effect and the point estimate would
        # then divide by different nominal counts"*.
        if len(clusters.trade_counts) != len(clusters.dates) or len(clusters.return_sums) != len(clusters.dates):
            raise S12PairedTrialRefused(
                f"a cluster axis is ragged: {len(clusters.dates)} dates, {len(clusters.trade_counts)} counts, "
                f"{len(clusters.return_sums)} sums"
            )
        pooled = int(clusters.trade_counts.sum())
        if pooled != clusters.trade_count:
            raise S12PairedTrialRefused(
                f"a cluster axis holds {pooled} trades but declares {clusters.trade_count} — the point estimate "
                "and the declared population would divide by different nominal counts"
            )
        for day, count, total in zip(clusters.dates, clusters.trade_counts, clusters.return_sums):
            # ⚠ ``DateClusters`` is a frozen dataclass around MUTABLE arrays, and
            # its own validation accepts a fractional count such as 1.5 — which
            # this loop silently truncated to 1, turning a supplied pooled mean of
            # 0.30 into a reported 0.45 (reproduced at checkpoint 1). Re-checked
            # here rather than trusted, because post-construction mutation also
            # bypasses that validation entirely.
            whole = int(count)
            if whole != count or whole < 1:
                raise S12PairedTrialRefused(
                    f"a cluster on {day} carries a trade count of {count!r}; a cluster holds a whole number of "
                    "trades, at least one"
                )
            counts[slot][position[day]] = whole
            sums[slot][position[day]] = float(total)
    return axis, counts[0], sums[0], counts[1], sums[1]


def _block_indices(
    axis_length: int,
    *,
    block_length: int,
    generator: np.random.Generator,
    batch: int,
) -> npt.NDArray[np.int64]:
    """One batch of circular block resample indices over ``axis_length``.

    ⚠ Each resample draws ``ceil(n/b)`` starts uniformly, wraps with ``% n`` and
    TRUNCATES back to ``n``, so every resample carries the same number of
    observations as the original — ``block_bootstrap``'s construction, and its
    reason: without the truncation the last block would extend the axis and a
    resample would be longer than the sample it estimates.

    ⚠ Written here rather than imported. ``block_bootstrap``'s equivalent is
    private, welded to its ratio estimator, and its RNG stream is the provenance
    of ledger rows already stored under ``c3-block-bootstrap-v1`` with no
    value-pinning test protecting it. ``strategy_mt1_trial`` carries its own for
    the same reason: a preregistered evaluator freezes its resampling scheme, and
    sharing one would make a future edit to either trial's construction a silent
    edit to the other's.
    """
    blocks = math.ceil(axis_length / block_length)
    offsets = np.arange(block_length, dtype=np.int64)
    starts = generator.integers(0, axis_length, size=(batch, blocks), dtype=np.int64)
    index = (starts[:, :, None] + offsets) % axis_length
    return index.reshape(batch, blocks * block_length)[:, :axis_length]


def _axis_block_length(selected: int, axis_length: int, *, arm: str, axis: str) -> int:
    """Clip the selected block to an axis, refusing the degenerate clip.

    ⚠⚠ ``b >= n`` IS A FALSE-PASS PATH, NOT A CLIP. Under the circular scheme a
    block of the axis's own length contains every observation exactly once in
    every replication, so the statistic is constant, the interval collapses onto
    the point estimate, and ANY positive point estimate passes automatically. The
    first cut clipped with ``min(selected, portfolio_n, cluster_n)`` and relied on
    a ``lower == high`` degeneracy check to catch the result — which
    summation-order noise escaped, at an interval width of 2.2e-16 (reproduced at
    checkpoint 1, on a three-cluster axis). Refusing the configuration is the
    structural fix; a tolerance on the width is not.

    ⚠ A block merely CLOSE to the axis length is not guarded, and no derivable
    threshold separates the two. Stated rather than papered over.
    """
    block = min(selected, axis_length)
    if block >= axis_length:
        raise S12PairedTrialRefused(
            f"{arm}: the {axis} axis holds {axis_length} observation(s) and the block is {block} — a circular "
            "block that long draws the whole axis in every replication, so the statistic cannot vary"
        )
    return block


def _bound(statistics: npt.NDArray[np.float64], point: float, *, label: str) -> OneSidedBound:
    if not math.isfinite(point):
        raise S12PairedTrialRefused(f"{label} has a non-finite point estimate")
    if not np.all(np.isfinite(statistics)):
        raise S12PairedTrialRefused(f"{label} produced a non-finite bootstrap statistic")
    tail = (1.0 - CONFIDENCE) / 2.0 * 100.0
    low, high = (float(value) for value in np.percentile(statistics, [tail, 100.0 - tail]))
    if not (math.isfinite(low) and math.isfinite(high)):
        raise S12PairedTrialRefused(f"{label} produced a non-finite interval endpoint")
    variance = float(np.var(statistics, ddof=1)) if statistics.size > 1 else 0.0
    return OneSidedBound(point=point, lower=low, high=high, replication_variance=variance)


def evaluate_arm(books: PairedBooks) -> ArmVerdict:
    """The four bounds for one quarantine arm, on one shared RNG stream.

    ⚠ ONE GENERATOR, DRAWN IN A FIXED ORDER — portfolio axis first, then the
    cluster axis — so the whole arm reproduces from ``BOOTSTRAP_SEED`` alone.
    That is a reproducibility property and NOT a statistical one: sharing a
    generator does not make the two axes' resamples a joint sample of one
    experiment, and their bounds must not be pooled or averaged. The IUT does not
    need them to be independent, which is why the sharing is harmless here.
    """
    _validate_axis(books)

    treatment_returns = portfolio_returns(books.treatment_equity, label=f"{books.arm} treatment")
    control_returns = portfolio_returns(books.control_equity, label=f"{books.arm} control")
    difference_returns = treatment_returns - control_returns
    portfolio_n = len(treatment_returns)
    if portfolio_n < MIN_SUPPORTING_CLUSTERS:
        raise S12PairedTrialRefused(
            f"{books.arm}: {portfolio_n} portfolio observation(s); fewer than {MIN_SUPPORTING_CLUSTERS} carries no "
            "serial structure to measure"
        )

    axis, treatment_counts, treatment_sums, control_counts, control_sums = _union_axis(
        books.treatment_clusters, books.control_clusters
    )
    cluster_n = len(axis)
    # ⚠⚠ PER STRATEGY, NOT ON THE UNION. The first cut checked the union's size,
    # which a treatment with six trades against a dense control passed. The
    # support that matters is each book's own.
    for label, counts in (("treatment", treatment_counts), ("control", control_counts)):
        supporting = int(np.count_nonzero(counts))
        if supporting < MIN_SUPPORTING_CLUSTERS:
            raise S12PairedTrialRefused(
                f"{books.arm}: the {label} book is supported by {supporting} decision date(s); fewer than "
                f"{MIN_SUPPORTING_CLUSTERS} cannot be resampled as a cluster axis"
            )

    # ⚠ ONE block length, measured on the primary estimand's own series and
    # applied to both axes. A DECLARED transfer — see the header — and the clip
    # to each axis REFUSES rather than silently degenerating.
    selected_block = optimal_block_length(difference_returns)
    portfolio_block = _axis_block_length(selected_block, portfolio_n, arm=books.arm, axis="portfolio")
    cluster_block = _axis_block_length(selected_block, cluster_n, arm=books.arm, axis="cluster")

    generator = np.random.default_rng(BOOTSTRAP_SEED)
    own_portfolio = np.empty(RESAMPLES, dtype=np.float64)
    difference_portfolio = np.empty(RESAMPLES, dtype=np.float64)
    written = 0
    while written < RESAMPLES:
        batch = min(_BATCH, RESAMPLES - written)
        index = _block_indices(portfolio_n, block_length=portfolio_block, generator=generator, batch=batch)
        own_portfolio[written : written + batch] = treatment_returns[index].mean(axis=1)
        # ⚠ The difference is formed INSIDE the replication, on the same indices.
        # Differencing two separately-resampled means would destroy the pairing
        # and silently assume independence — the covariance is exactly what this
        # construction exists to capture.
        difference_portfolio[written : written + batch] = difference_returns[index].mean(axis=1)
        written += batch

    own_expectancy = np.empty(RESAMPLES, dtype=np.float64)
    difference_expectancy = np.empty(RESAMPLES, dtype=np.float64)
    written = 0
    while written < RESAMPLES:
        batch = min(_BATCH, RESAMPLES - written)
        index = _block_indices(cluster_n, block_length=cluster_block, generator=generator, batch=batch)
        treatment_trades = treatment_counts[index].sum(axis=1)
        control_trades = control_counts[index].sum(axis=1)
        # ⚠ A resample can miss one strategy entirely on a sparse axis. That is
        # 0/0, not zero expectancy, and it is refused rather than imputed. ⚠ Its
        # occurrence is seed-dependent; the deterministic guard is the
        # per-strategy support check above.
        if int(treatment_trades.min()) < 1 or int(control_trades.min()) < 1:
            raise S12PairedTrialRefused(
                f"{books.arm}: a resample drew no trade for one strategy, so its pooled expectancy is 0/0"
            )
        treatment_mean = treatment_sums[index].sum(axis=1) / treatment_trades
        control_mean = control_sums[index].sum(axis=1) / control_trades
        own_expectancy[written : written + batch] = treatment_mean
        difference_expectancy[written : written + batch] = treatment_mean - control_mean
        written += batch

    treatment_point = float(treatment_sums.sum()) / int(treatment_counts.sum())
    control_point = float(control_sums.sum()) / int(control_counts.sum())
    verdict = ArmVerdict(
        arm=books.arm,
        portfolio_own=_bound(own_portfolio, float(treatment_returns.mean()), label=f"{books.arm} portfolio own"),
        portfolio_difference=_bound(
            difference_portfolio, float(difference_returns.mean()), label=f"{books.arm} portfolio difference"
        ),
        expectancy_own=_bound(own_expectancy, treatment_point, label=f"{books.arm} expectancy own"),
        expectancy_difference=_bound(
            difference_expectancy, treatment_point - control_point, label=f"{books.arm} expectancy difference"
        ),
        portfolio_observations=portfolio_n,
        expectancy_clusters=cluster_n,
        block_length=portfolio_block,
        selector_block_length=selected_block,
        treatment_supporting_dates=int(np.count_nonzero(treatment_counts)),
        control_supporting_dates=int(np.count_nonzero(control_counts)),
    )
    # ⚠⚠ A MARGINAL WITH NO REPLICATION VARIANCE IS A REFUSAL; A DIFFERENCE WITH
    # NONE IS NOT. ``block_bootstrap`` refuses a zero bootstrap variance because
    # the design effect is then 0/0, and the same state on an OWN leg means the
    # marginal was not sampled. On a DIFFERENCE leg it is reachable legitimately —
    # a constant per-observation advantage has no EMPIRICAL sampling variability
    # under the pairing — so it is allowed through.
    #
    # ⚠ Two limits, both stated because the first cut asserted past them. This
    # tests the REPLICATIONS' variance, not ``lower == high``, which is neither
    # necessary nor sufficient (see ``OneSidedBound.replication_variance``). And
    # allowing a degenerate DIFFERENCE is blanket: collapse can also come from
    # periodic block sums or quantile collapse, and this cannot tell those from a
    # constant advantage. The structural false-pass route — a block as long as its
    # axis — is closed in ``_axis_block_length`` instead, which is where it
    # belongs.
    for label, bound in (("portfolio own", verdict.portfolio_own), ("expectancy own", verdict.expectancy_own)):
        if bound.replication_variance <= 0.0:
            raise S12PairedTrialRefused(
                f"{books.arm}: the {label} bootstrap has zero variance across replications, so its marginal was "
                "not sampled"
            )
    return verdict


def evaluate_s12_paired_trial(arms: Sequence[PairedBooks]) -> S12PairedTrialResult:
    """Evaluate the two declared quarantine arms and combine them as an IUT.

    ⚠⚠ THE ARM SET IS CHECKED AGAINST ``DECLARED_ARMS``, not merely counted.
    The contract names ``masked`` and ``admitted`` and makes both conjuncts;
    reporting a pass from one arm, or from two arms under other names, would be
    selecting an experiment after the look.
    """
    names = tuple(book.arm for book in arms)
    if sorted(names) != sorted(DECLARED_ARMS):
        raise S12PairedTrialRefused(
            f"the contract declares the arms {DECLARED_ARMS} and requires both; received {names}"
        )
    verdicts = tuple(evaluate_arm(book) for book in arms)
    return S12PairedTrialResult(arms=verdicts, conjuncts_pass=all(verdict.passes for verdict in verdicts))


__all__ = [
    "BOOTSTRAP_SEED",
    "DECLARED_ARMS",
    "MIN_SUPPORTING_CLUSTERS",
    "RESAMPLES",
    "TRIAL_CONTRACT_VERSION",
    "TRIAL_EVALUATOR_VERSION",
    "ArmVerdict",
    "OneSidedBound",
    "PairedBooks",
    "S12PairedTrialRefused",
    "S12PairedTrialResult",
    "evaluate_arm",
    "evaluate_s12_paired_trial",
    "portfolio_returns",
]
