"""Phase 5e-2 — criterion 3's block bootstrap, clustered by date.

Spec: ``docs/proposals/ta/2026-08-07-bounded-backtester.md`` §8 (stage 5e-2) and
§9's C3. Parent ``docs/proposals/ta/strategy-catalogue-and-backtest-validity.md``
criterion 3 — *"Use a block bootstrap over calendar blocks with errors clustered
by date, and report the effective sample size and confidence interval — not a
bare percentage."* Refs #2240.

⚠⚠ THE CRITERION NAMES THE METHOD AND EXPLICITLY FORBIDS THE SHORTCUT.

Criterion 3 opens by rejecting a rule it could have adopted: *"'Effective n ≈
nominal/20' is too crude"*. Two distinct correlations are in play and a single
divisor collapses them —

1. **serial** — a strategy holding for weeks produces overlapping returns, so
   consecutive dates are not independent draws;
2. **cross-sectional** — *"signals are correlated across instruments on the same
   day"*, so twenty entries fired by one market-wide move are nearer to one
   observation than to twenty.

The construction below is the criterion's own, and it separates the two: (2) is
handled by making the **date** the resampling unit, so every trade sharing a
date moves together or not at all; (1) is handled by resampling **contiguous
blocks** of dates rather than single dates, so the serial structure inside a
block survives into the resample.

WHERE EACH CONSTANT COMES FROM
------------------------------
Picking a block length is the *"am I about to pick a threshold, ratio or
window"* trigger in ``.claude/CLAUDE.md``, and the precedent it cites (#2279, an
invented 20th/80th-percentile band where Bollinger's published rule is six
months) is exactly the error available here — "20 days, about a holding period"
would have looked reasonable and been invented. A published rule exists, so it
is used and cited rather than reasoned out:

- **Block length** — Politis & White (2004), *Automatic Block-Length Selection
  for the Dependent Bootstrap*, Econometric Reviews 23(1):53-70, with the
  correction in Patton, Politis & White (2009), Econometric Reviews
  28(4):372-375. Implemented in ``optimal_block_length`` below, including the
  2009 paper's ``4/3`` constant for the circular scheme. ⚠ The block length is
  therefore MEASURED off the series' own autocovariance, not declared.
- **Circular blocks** — Politis & Romano (1992). The wrap-around is not
  cosmetic: a moving block bootstrap under-samples the first and last ``b-1``
  observations, biasing a statistic computed over a window whose ends are a
  strategy's earliest and latest trades. ⚠ It is also what makes the ``4/3``
  constant the right one — ``2`` is the stationary bootstrap's, and pairing
  either constant with the other scheme mis-sizes the block silently.
- **Effective sample size** — Kish (1965), *Survey Sampling* §8.2: the design
  effect ``deff = Var_actual / Var_srs``, and ``ESS = n / deff``. This is what
  makes the reported number commensurable with ``trade_count`` beside it in
  criterion 7's metric set: both are in units of trades.
- **Percentile interval** — Efron & Tibshirani (1993), *An Introduction to the
  Bootstrap*, ch. 13. ⚠ Percentile intervals are first-order accurate only; BCa
  is the second-order correction and is NOT computed here, because it needs a
  cluster jackknife over the full population. Stated rather than silently
  omitted: a reported interval that is narrower than the truth is the failure
  mode criterion 3 exists to prevent.
- **Resample count** — 2,000. Efron & Tibshirani recommend at least 1,000 for
  interval estimation (a point estimate's standard error tolerates far fewer),
  and no published rule fixes a single value above that floor. So this one IS a
  free constant, and it is frozen in ``BOOTSTRAP_MODEL_ID`` with the others per
  the same file's rule for the case where no published formulation exists.

⚠⚠ THE RESAMPLE COSTS TWO NUMBERS PER DATE, NOT ONE PER TRADE.

The statistic is a RATIO — pooled expectancy is ``sum(returns) /
count(returns)`` over the trades of the resampled dates — so a cluster enters a
resample fully described by its ``(trade count, return sum)`` pair. Resampling
therefore gathers over ~10^4 dates rather than ~10^6 trades, and the result is
EXACT, not an approximation of a per-trade resample: the two are the same
arithmetic. Without this the full-population arm would not run.

⚠ BLOCKS ARE CONTIGUOUS IN OBSERVED-DATE ORDER, NOT IN CALENDAR DAYS.

The cluster axis holds only dates that carry at least one trade, so a block of
``b`` clusters spans ``b`` ACTIVE dates and an unknown number of calendar days —
more of them in a sparse period. The alternative, padding the axis with the full
trading calendar, would make a block a fixed calendar span but would fill it
with zero-trade dates that carry no error to cluster, diluting the very
correlation being corrected for. The active-date axis is the choice, it is
frozen in ``BOOTSTRAP_MODEL_ID``, and it is stated here because it is the
assumption a later reader is most likely to make silently.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date
from typing import Final

import numpy as np
import numpy.typing as npt

#: The identity of this construction. Same role as ``METRIC_SET_ID`` and
#: ``COST_MODEL_ID``: the free choices above (resample count, confidence level,
#: circular scheme, percentile interval, active-date axis, entry-date
#: clustering) are frozen behind it, so a stored ``effective_sample_size``
#: cannot silently change meaning. Bumping it is a new evaluation.
BOOTSTRAP_MODEL_ID: Final = "c3-block-bootstrap-v1"

#: Bootstrap replications. See the header — a free constant above Efron &
#: Tibshirani's 1,000 floor for interval estimation.
RESAMPLES: Final = 2_000

#: Two-sided coverage of the reported interval.
CONFIDENCE: Final = 0.95

#: Fewer clusters than this and there is no serial structure to measure: a
#: single date cannot be autocorrelated with anything, and the design effect
#: would be computed from one draw.
MIN_CLUSTERS: Final = 2

#: Resamples evaluated per batch. ⚠ Bounds peak memory: the index matrix is
#: ``batch x clusters`` int64, so a 16k-cluster axis costs ~26 MB per batch at
#: 200 rather than ~256 MB for all 2,000 at once. Does not affect the result —
#: the batches are concatenated and the RNG stream is continuous.
_BATCH: Final = 200


@dataclass(frozen=True)
class DateClusters:
    """Trades reduced to criterion 3's resampling unit: one row per date.

    ⚠ ``dates`` is ascending and DISTINCT — it is the cluster axis, and a
    repeated date would mean the same cross-section entered twice as two
    independent draws, which is the error the clustering exists to prevent.
    """

    dates: tuple[date, ...]
    #: Trades attributed to each date. Every entry is >= 1: a date with no
    #: trade is not a cluster and is absent from the axis (see the header).
    trade_counts: npt.NDArray[np.int64]
    #: Sum of net return percent over that date's trades. ⚠ SUM, not mean —
    #: the pooled statistic is a ratio of sums, and storing means here would
    #: need the counts back to re-weight, which is how a cluster bootstrap
    #: silently becomes an unweighted one.
    return_sums: npt.NDArray[np.float64]
    #: The trade population these clusters were built from, kept whole because
    #: the design effect's denominator is the IID variance of the mean over the
    #: NOMINAL trade count.
    trade_variance: float
    trade_count: int

    def __post_init__(self) -> None:
        if not (len(self.dates) == len(self.trade_counts) == len(self.return_sums)):
            raise ValueError(
                f"cluster axis is ragged: {len(self.dates)} dates, {len(self.trade_counts)} counts, "
                f"{len(self.return_sums)} sums"
            )
        if len(self.dates) != len(set(self.dates)):
            raise ValueError("cluster dates are not distinct — a date is one cluster, not several")
        if list(self.dates) != sorted(self.dates):
            raise ValueError("cluster dates are not ascending, so a contiguous block is not a contiguous span")
        if len(self.trade_counts) and int(self.trade_counts.min()) < 1:
            raise ValueError("a cluster with no trades is not a cluster and must be absent from the axis")
        # ⚠⚠ THE TWO NOMINAL COUNTS MUST BE THE SAME COUNT.
        # ``trade_count`` feeds Kish's denominator (``iid_variance`` and hence
        # the design effect), while the point estimate divides by
        # ``trade_counts.sum()``. ``cluster_by_date`` always sets them
        # consistently, but a caller constructing this directly could not — and
        # the result would mix two different nominal counts inside one
        # ``BootstrapResult`` with nothing downstream able to tell.
        pooled = int(self.trade_counts.sum())
        if pooled != self.trade_count:
            raise ValueError(
                f"the cluster axis holds {pooled} trades but trade_count declares {self.trade_count} — the design "
                "effect and the point estimate would then divide by different nominal counts"
            )

    @property
    def cluster_count(self) -> int:
        return len(self.dates)


@dataclass(frozen=True)
class BootstrapResult:
    """Criterion 3's two required outputs, plus the provenance to re-run them.

    ⚠ The interval is on ``point_estimate_pct`` — expectancy per trade — which
    is criterion 7's trade-level headline and therefore the *"bare percentage"*
    criterion 3 refuses to let stand alone.
    """

    effective_sample_size: float
    ci_low_pct: float
    ci_high_pct: float
    point_estimate_pct: float
    #: Measured off the cluster axis by ``optimal_block_length``, never declared.
    block_length: int
    cluster_count: int
    trade_count: int
    resamples: int
    seed: int
    #: ``Var_bootstrap(mean) / Var_iid(mean)`` — Kish's design effect. ⚠ Reported
    #: rather than folded away because its DIRECTION is the finding: above 1 the
    #: overlap cost sample size, which is the expected case and the reason
    #: criterion 3 exists; below 1 it did not, and a reader should see that
    #: rather than an ESS above the nominal count with no explanation.
    design_effect: float
    model_id: str = BOOTSTRAP_MODEL_ID

    def __post_init__(self) -> None:
        if self.effective_sample_size <= 0.0:
            raise ValueError(f"effective_sample_size must be positive, got {self.effective_sample_size}")
        if self.ci_low_pct > self.ci_high_pct:
            raise ValueError(f"interval [{self.ci_low_pct}, {self.ci_high_pct}] is inverted")
        if self.block_length < 1:
            raise ValueError(f"block_length must be at least 1, got {self.block_length}")
        if self.block_length > self.cluster_count:
            raise ValueError(
                f"block_length {self.block_length} exceeds the {self.cluster_count}-cluster axis it was measured on"
            )
        if self.design_effect <= 0.0:
            raise ValueError(f"design_effect must be positive, got {self.design_effect}")


def cluster_by_date(
    net_return_pct: Sequence[float],
    cluster_dates: Sequence[date],
) -> DateClusters:
    """Group realised trade returns into one cluster per date.

    ⚠⚠ THE CLUSTER KEY IS THE ENTRY FILL DATE, AND THE CRITERION PICKS IT.

    Criterion 3's stated reason for clustering is that *"signals are correlated
    across instruments on the same day"* — that is a statement about the day the
    signal fired, so the cross-section that must move together is the one that
    ENTERED together. Clustering on the exit date instead would group trades
    that share nothing but an outcome, and would scatter a single market-wide
    entry across as many clusters as it had holding periods.
    """
    if len(net_return_pct) != len(cluster_dates):
        raise ValueError(f"{len(net_return_pct)} returns against {len(cluster_dates)} dates — they must be parallel")

    returns = np.asarray(net_return_pct, dtype=np.float64)
    order = sorted(range(len(cluster_dates)), key=lambda index: cluster_dates[index])

    dates: list[date] = []
    counts: list[int] = []
    sums: list[float] = []
    for index in order:
        day = cluster_dates[index]
        if dates and dates[-1] == day:
            counts[-1] += 1
            sums[-1] += float(returns[index])
        else:
            dates.append(day)
            counts.append(1)
            sums.append(float(returns[index]))

    # ⚠ ddof=1. The design effect compares two estimates of the SAME quantity —
    # the variance of the mean — and the bootstrap side is a sample variance over
    # replications. A population variance here would inflate the ratio by
    # n/(n-1) on one side only.
    variance = float(np.var(returns, ddof=1)) if len(returns) > 1 else 0.0

    return DateClusters(
        dates=tuple(dates),
        trade_counts=np.asarray(counts, dtype=np.int64),
        return_sums=np.asarray(sums, dtype=np.float64),
        trade_variance=variance,
        trade_count=len(returns),
    )


def optimal_block_length(series: npt.NDArray[np.float64]) -> int:
    """Politis & White (2004) with the Patton, Politis & White (2009) correction.

    Returns the CIRCULAR-bootstrap block length ``b_cb``, rounded up to a whole
    number of clusters and clamped to ``[1, len(series)]``.

    The published arithmetic, reproduced from the two papers::

        h(x)   = min(1, 2(1 - |x|))                    the flat-top lag window
        g      = sum_{k=-m}^{m} h(k/m) |k| gamma_k
        sigma2 = sum_{k=-m}^{m} h(k/m) gamma_k
        d_cb   = (4/3) sigma2^2                        4/3 is the 2009 correction
        b_cb   = ((2 g^2) / d_cb)^(1/3) n^(1/3)

    with ``m`` chosen as the first lag after which ``k_n`` consecutive
    autocorrelations all sit inside the conservative band
    ``+/- 2 sqrt(log10(n)/n)``, ``k_n = max(5, log10(n))``, and ``m`` capped at
    ``ceil(sqrt(n)) + k_n``. ``b`` is capped at ``min(3 sqrt(n), n/3)``.

    ⚠ ``g == 0`` (no measurable serial dependence) drives ``b_cb`` to 0, and
    ``sigma2 == 0`` divides by zero. Both floor to a block length of 1, which is
    the honest degenerate answer: blocks of one cluster ARE the plain cluster
    bootstrap, which is what an uncorrelated series should get. Neither case is
    silently rewritten into a larger block.
    """
    nobs = int(series.shape[0])
    if nobs < 2:
        return 1

    eps = series - series.mean()
    b_max = math.ceil(min(3.0 * math.sqrt(nobs), nobs / 3.0))
    kn = max(5, int(math.log10(nobs)))
    m_max = int(math.ceil(math.sqrt(nobs))) + kn
    m_max = min(m_max, nobs - 1)
    cv = 2.0 * math.sqrt(math.log10(nobs) / nobs)

    acv = np.zeros(m_max + 1, dtype=np.float64)
    abs_acorr = np.zeros(m_max + 1, dtype=np.float64)
    opt_m: int | None = None
    for lag in range(m_max + 1):
        head = eps[lag:]
        tail = eps[: nobs - lag]
        cross = float(head @ tail)
        acv[lag] = cross / nobs
        # ⚠ Normalised by the two half-samples actually multiplied together, not
        # by gamma_0 — this is the published band's own scaling.
        v1 = float(eps[lag + 1 :] @ eps[lag + 1 :]) if lag + 1 < nobs else 0.0
        v2 = float(eps[: -(lag + 1)] @ eps[: -(lag + 1)]) if lag + 1 < nobs else 0.0
        denominator = math.sqrt(v1 * v2)
        abs_acorr[lag] = abs(cross) / denominator if denominator > 0.0 else 0.0
        if lag >= kn and opt_m is None and bool(np.all(abs_acorr[lag - kn : lag] < cv)):
            opt_m = lag - kn

    m = 2 * max(opt_m, 1) if opt_m is not None else m_max
    m = min(m, m_max)

    g = 0.0
    lr_acv = float(acv[0])
    for k in range(1, m + 1):
        lam = 1.0 if k / m <= 0.5 else 2.0 * (1.0 - k / m)
        g += 2.0 * lam * k * float(acv[k])
        lr_acv += 2.0 * lam * float(acv[k])

    d_cb = 4.0 / 3.0 * lr_acv**2
    if d_cb <= 0.0 or g == 0.0:
        return 1
    b_cb = ((2.0 * g**2) / d_cb) ** (1.0 / 3.0) * nobs ** (1.0 / 3.0)
    if not math.isfinite(b_cb):
        return 1
    return int(min(max(math.ceil(b_cb), 1), b_max, nobs))


def _circular_block_statistics(
    clusters: DateClusters,
    *,
    block_length: int,
    resamples: int,
    seed: int,
) -> npt.NDArray[np.float64]:
    """Pooled expectancy under ``resamples`` circular block resamples of the axis.

    ⚠ Each resample draws ``ceil(n/b)`` block starts uniformly over the axis and
    wraps with ``% n``, then TRUNCATES the concatenation back to ``n`` clusters,
    so every resample carries the same number of dates as the original. Without
    the truncation the last block would extend the axis and a resample would be
    longer than the sample it estimates.
    """
    n = clusters.cluster_count
    counts = clusters.trade_counts
    sums = clusters.return_sums
    # ⚠ The index matrix is `batch x blocks_per_resample x block_length`, so its
    # width scales with `n / block_length` — NOT with `n`. A very small block on
    # a large axis is therefore the expensive case, not a large one; `_BATCH` is
    # what bounds it, and it is the knob to turn if that case ever shows up.
    blocks_per_resample = math.ceil(n / block_length)
    offsets = np.arange(block_length, dtype=np.int64)

    rng = np.random.default_rng(seed)
    statistics = np.empty(resamples, dtype=np.float64)
    for start in range(0, resamples, _BATCH):
        batch = min(_BATCH, resamples - start)
        starts = rng.integers(0, n, size=(batch, blocks_per_resample), dtype=np.int64)
        index = (starts[:, :, None] + offsets) % n
        index = index.reshape(batch, blocks_per_resample * block_length)[:, :n]
        # The ratio estimator: pooled sum over pooled count. ⚠ Every cluster has
        # at least one trade (``DateClusters`` enforces it), so the denominator
        # is >= n and cannot be zero.
        statistics[start : start + batch] = sums[index].sum(axis=1) / counts[index].sum(axis=1)
    return statistics


def block_bootstrap_expectancy(
    clusters: DateClusters,
    *,
    seed: int,
    resamples: int = RESAMPLES,
    confidence: float = CONFIDENCE,
) -> BootstrapResult | None:
    """Criterion 3's effective sample size and interval. Pure; reads no database.

    ⚠⚠ RETURNS ``None`` IN EXACTLY THREE STATES, AND EACH IS A REAL ONE.

    1. **Fewer than ``MIN_CLUSTERS`` dates.** One date has no serial structure,
       so no design effect can be measured from it.
    2. **Zero trade variance** — fewer than two trades, or every trade returning
       the same number. Kish's denominator ``Var_srs = s^2/n`` is then zero and
       the design effect is ``0/0``.
    3. **Zero bootstrap variance** — every resample returned the same statistic.
       The design effect is zero and the ESS infinite.

    In all three the caller leaves ``effective_sample_size`` NULL and the
    promotion gate refuses on ``effective_sample_size_not_computed``. ⚠ That is
    the point: criterion 3 says *"no bare percentage and no nominal n is
    reported anywhere"*, so a degenerate bootstrap must not fall back to the
    trade count. A refusal is the correct output of a measurement that could not
    be made.
    """
    if not 0.0 < confidence < 1.0:
        raise ValueError(f"confidence must be strictly inside (0, 1), got {confidence}")
    if resamples < 1:
        raise ValueError(f"resamples must be positive, got {resamples}")

    if clusters.cluster_count < MIN_CLUSTERS:
        return None
    if clusters.trade_count < 2 or clusters.trade_variance <= 0.0:
        return None

    block_length = optimal_block_length(clusters.return_sums / clusters.trade_counts)
    block_length = min(block_length, clusters.cluster_count)

    statistics = _circular_block_statistics(
        clusters,
        block_length=block_length,
        resamples=resamples,
        seed=seed,
    )
    bootstrap_variance = float(np.var(statistics, ddof=1)) if resamples > 1 else 0.0
    if bootstrap_variance <= 0.0 or not math.isfinite(bootstrap_variance):
        return None

    # Kish's design effect, and the ESS it defines. ⚠ ``iid_variance`` is the
    # variance the mean WOULD have under independent sampling of the nominal
    # trade count — the "n" criterion 3 forbids reporting — so the ratio is
    # exactly how much that n overstates the evidence.
    iid_variance = clusters.trade_variance / clusters.trade_count
    design_effect = bootstrap_variance / iid_variance
    effective_sample_size = clusters.trade_count / design_effect
    if not math.isfinite(effective_sample_size) or effective_sample_size <= 0.0:
        return None

    tail = (1.0 - confidence) / 2.0 * 100.0
    ci_low, ci_high = (float(value) for value in np.percentile(statistics, [tail, 100.0 - tail]))

    pooled_count = int(clusters.trade_counts.sum())
    point_estimate = float(clusters.return_sums.sum()) / pooled_count

    return BootstrapResult(
        effective_sample_size=effective_sample_size,
        ci_low_pct=ci_low,
        ci_high_pct=ci_high,
        point_estimate_pct=point_estimate,
        block_length=block_length,
        cluster_count=clusters.cluster_count,
        trade_count=clusters.trade_count,
        resamples=resamples,
        seed=seed,
        design_effect=design_effect,
    )


__all__ = [
    "BOOTSTRAP_MODEL_ID",
    "CONFIDENCE",
    "MIN_CLUSTERS",
    "RESAMPLES",
    "BootstrapResult",
    "DateClusters",
    "block_bootstrap_expectancy",
    "cluster_by_date",
    "optimal_block_length",
]
