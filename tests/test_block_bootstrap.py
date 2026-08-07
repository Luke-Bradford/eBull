"""Phase 5e-2 — criterion 3's block bootstrap clustered by date.

Spec: ``docs/proposals/ta/2026-08-07-bounded-backtester.md`` §8 (stage 5e-2), C3.
Parent criterion 3. Refs #2240.

⚠ PURE-LOGIC TIER. No database — the bootstrap reads a trade list and nothing
else, so a DB fixture here would buy nothing and cost the push gate.

⚠⚠ THE SPEC CONSTANTS ARE LITERALS BELOW, NOT IMPORTS.

Prevention-log lesson (2026-08-06): *a reference that IMPORTS the constant it
validates is a tautology*. ``assert RESAMPLES == RESAMPLES`` passes after
somebody edits the module to 50. So the published constants and the frozen free
ones are written out by hand here, and ``TestSpecConstants`` is the one bridge
between the literals and the module.
"""

from __future__ import annotations

import math
from datetime import date, timedelta

import numpy as np
import numpy.typing as npt
import pytest

from app.services.block_bootstrap import (
    BOOTSTRAP_MODEL_ID,
    CONFIDENCE,
    MIN_CLUSTERS,
    RESAMPLES,
    BootstrapResult,
    DateClusters,
    block_bootstrap_expectancy,
    cluster_by_date,
    optimal_block_length,
)

#: Hand-written from the spec and the papers. See the module header.
SPEC_RESAMPLES = 2_000
SPEC_CONFIDENCE = 0.95
SPEC_MIN_CLUSTERS = 2
SPEC_MODEL_ID = "c3-block-bootstrap-v1"
#: Patton, Politis & White (2009)'s constant for the CIRCULAR scheme. The
#: stationary bootstrap's is 2, and the two are easy to cross.
SPEC_CIRCULAR_CONSTANT = 4.0 / 3.0

_DAY_ZERO = date(2020, 1, 6)


def _days(count: int, *, start: int = 0) -> list[date]:
    return [_DAY_ZERO + timedelta(days=start + i) for i in range(count)]


def _reference_block_length(series: npt.NDArray[np.float64], *, constant: float) -> int:
    """Politis & White (2004) + Patton, Politis & White (2009), transcribed here.

    ⚠ An INDEPENDENT transcription, not an import: it takes the scheme constant
    as an argument (4/3 circular, 2 stationary) and builds the autocovariances
    with ``np.correlate`` rather than the module's explicit dot products. If
    this shared the module's code the assertion would be a tautology.
    """
    n = int(series.shape[0])
    eps = series - series.mean()
    kn = max(5, int(math.log10(n)))
    m_max = min(int(math.ceil(math.sqrt(n))) + kn, n - 1)
    cv = 2.0 * math.sqrt(math.log10(n) / n)

    full = np.correlate(eps, eps, mode="full")
    acv = full[n - 1 : n + m_max] / n

    acorr = np.empty(m_max + 1, dtype=np.float64)
    for lag in range(m_max + 1):
        head, tail = eps[lag:], eps[: n - lag]
        v1 = float(eps[lag + 1 :] @ eps[lag + 1 :]) if lag + 1 < n else 0.0
        v2 = float(eps[: -(lag + 1)] @ eps[: -(lag + 1)]) if lag + 1 < n else 0.0
        denominator = math.sqrt(v1 * v2)
        acorr[lag] = abs(float(head @ tail)) / denominator if denominator > 0.0 else 0.0

    opt_m: int | None = None
    for lag in range(kn, m_max + 1):
        if opt_m is None and bool(np.all(acorr[lag - kn : lag] < cv)):
            opt_m = lag - kn
    m = min(2 * max(opt_m, 1), m_max) if opt_m is not None else m_max

    lags = np.arange(1, m + 1, dtype=np.float64)
    weights = np.where(lags / m <= 0.5, 1.0, 2.0 * (1.0 - lags / m))
    g = float(2.0 * np.sum(weights * lags * acv[1 : m + 1]))
    sigma2 = float(acv[0] + 2.0 * np.sum(weights * acv[1 : m + 1]))

    d = constant * sigma2**2
    if d <= 0.0 or g == 0.0:
        return 1
    b = ((2.0 * g**2) / d) ** (1.0 / 3.0) * n ** (1.0 / 3.0)
    b_max = math.ceil(min(3.0 * math.sqrt(n), n / 3.0))
    return int(min(max(math.ceil(b), 1), b_max, n))


class TestSpecConstants:
    """The one bridge between the hand-written literals and the module."""

    def test_the_frozen_constants_match_the_spec(self) -> None:
        assert RESAMPLES == SPEC_RESAMPLES
        assert CONFIDENCE == SPEC_CONFIDENCE
        assert MIN_CLUSTERS == SPEC_MIN_CLUSTERS
        assert BOOTSTRAP_MODEL_ID == SPEC_MODEL_ID

    def test_the_resample_count_clears_the_interval_floor(self) -> None:
        """⚠ Efron & Tibshirani's floor for INTERVAL estimation is 1,000, and
        ``sql/265`` enforces it as a CHECK. A default below it would make every
        write fail rather than degrade quietly, which is the right direction —
        but the default should not be the thing that trips it."""
        assert RESAMPLES >= 1_000


class TestClusterByDate:
    def test_a_date_becomes_one_cluster_carrying_the_sum_not_the_mean(self) -> None:
        """⚠ SUM. The pooled statistic is a ratio of sums; storing means here
        would silently re-weight every date to equal importance regardless of
        how many trades it fired."""
        day = _DAY_ZERO
        clusters = cluster_by_date([1.0, 3.0, 10.0], [day, day, day + timedelta(days=1)])

        assert clusters.dates == (day, day + timedelta(days=1))
        assert list(clusters.trade_counts) == [2, 1]
        assert list(clusters.return_sums) == [4.0, 10.0]
        assert clusters.trade_count == 3

    def test_unsorted_input_is_ordered_onto_an_ascending_axis(self) -> None:
        """A contiguous block is only a contiguous span if the axis is sorted,
        so the grouping sorts rather than trusting the caller."""
        days = _days(3)
        clusters = cluster_by_date([5.0, 1.0, 3.0], [days[2], days[0], days[1]])

        assert clusters.dates == tuple(days)
        assert list(clusters.return_sums) == [1.0, 3.0, 5.0]

    def test_ragged_input_is_refused(self) -> None:
        with pytest.raises(ValueError, match="parallel"):
            cluster_by_date([1.0, 2.0], _days(1))

    def test_the_trade_variance_is_the_sample_variance_of_the_raw_trades(self) -> None:
        """⚠ ddof=1, and over the TRADES — it is Kish's ``s^2`` and the design
        effect's denominator. A cluster-level variance here would compare two
        different quantities and the ratio would mean nothing."""
        returns = [1.0, 2.0, 6.0, 3.0]
        clusters = cluster_by_date(returns, _days(4))
        assert clusters.trade_variance == pytest.approx(float(np.var(returns, ddof=1)))


class TestDateClustersInvariants:
    def test_a_repeated_date_is_refused(self) -> None:
        """Two rows for one date means the same cross-section entered twice as
        two independent draws — the exact error the clustering prevents."""
        with pytest.raises(ValueError, match="distinct"):
            DateClusters(
                dates=(_DAY_ZERO, _DAY_ZERO),
                trade_counts=np.asarray([1, 1], dtype=np.int64),
                return_sums=np.asarray([1.0, 2.0], dtype=np.float64),
                trade_variance=0.5,
                trade_count=2,
            )

    def test_a_descending_axis_is_refused(self) -> None:
        with pytest.raises(ValueError, match="ascending"):
            DateClusters(
                dates=(_DAY_ZERO + timedelta(days=1), _DAY_ZERO),
                trade_counts=np.asarray([1, 1], dtype=np.int64),
                return_sums=np.asarray([1.0, 2.0], dtype=np.float64),
                trade_variance=0.5,
                trade_count=2,
            )

    def test_an_empty_cluster_is_refused(self) -> None:
        """A zero-trade date carries no error to cluster, and admitting one
        would dilute the correlation being corrected for."""
        with pytest.raises(ValueError, match="no trades"):
            DateClusters(
                dates=tuple(_days(2)),
                trade_counts=np.asarray([1, 0], dtype=np.int64),
                return_sums=np.asarray([1.0, 0.0], dtype=np.float64),
                trade_variance=0.5,
                trade_count=1,
            )

    def test_a_declared_trade_count_disagreeing_with_the_axis_is_refused(self) -> None:
        """⚠⚠ TWO NOMINAL COUNTS IN ONE RESULT.

        ``trade_count`` is Kish's denominator (via ``iid_variance``, and hence
        the design effect); the point estimate divides by
        ``trade_counts.sum()``. If they diverge, one ``BootstrapResult`` reports
        an effective sample size and a point estimate computed against different
        populations, and nothing downstream can tell. ``cluster_by_date`` cannot
        produce the state — a direct construction can, which is why the guard is
        on the dataclass rather than on the factory.
        """
        with pytest.raises(ValueError, match="declares"):
            DateClusters(
                dates=tuple(_days(2)),
                trade_counts=np.asarray([2, 3], dtype=np.int64),
                return_sums=np.asarray([1.0, 2.0], dtype=np.float64),
                trade_variance=0.5,
                trade_count=4,
            )


class TestOptimalBlockLength:
    def test_a_persistent_series_gets_a_longer_block_than_white_noise(self) -> None:
        """⚠⚠ THE PROPERTY THE WHOLE STAGE RESTS ON. If the block length does
        not grow with serial dependence then it is not measuring dependence, and
        every effective sample size downstream is a fixed-window number wearing
        a measured name."""
        rng = np.random.default_rng(11)
        noise = rng.standard_normal(600)

        persistent = np.empty(600, dtype=np.float64)
        persistent[0] = noise[0]
        for i in range(1, 600):
            persistent[i] = 0.9 * persistent[i - 1] + noise[i]

        assert optimal_block_length(persistent) > optimal_block_length(noise)

    def test_a_constant_series_floors_at_one(self) -> None:
        """``g == 0`` and ``sigma2 == 0`` both arise here. Blocks of one cluster
        ARE the plain cluster bootstrap, which is the honest answer for a series
        with no measurable dependence — not a silently larger block."""
        assert optimal_block_length(np.zeros(200, dtype=np.float64)) == 1

    def test_a_degenerate_axis_floors_at_one(self) -> None:
        assert optimal_block_length(np.asarray([1.0], dtype=np.float64)) == 1
        assert optimal_block_length(np.asarray([], dtype=np.float64)) == 1

    @pytest.mark.parametrize("size", [2, 7, 40, 250])
    def test_the_block_never_leaves_the_axis(self, size: int) -> None:
        rng = np.random.default_rng(3)
        series = rng.standard_normal(size)
        block = optimal_block_length(series)
        assert 1 <= block <= size

    def test_it_matches_an_independent_transcription_of_the_published_formula(self) -> None:
        """⚠⚠ THE PIN ON THE ARITHMETIC, INCLUDING THE 4/3.

        ``_reference_block_length`` below is transcribed from Politis & White
        (2004) / Patton, Politis & White (2009) directly, in a different shape
        from the module (``np.correlate`` for the autocovariances, an explicit
        constant argument). It does NOT import anything from the module, which
        is what stops this being a restatement of the code under test — the
        repo's standing rule after the #2240 S-3 tautology.

        The second assertion is the one that pins the constant specifically: the
        stationary bootstrap's 2 produces a DIFFERENT block on this same series,
        so a crossed constant cannot pass both.
        """
        rng = np.random.default_rng(5)
        noise = rng.standard_normal(1_000)
        persistent = np.empty(1_000, dtype=np.float64)
        persistent[0] = noise[0]
        for i in range(1, 1_000):
            persistent[i] = 0.8 * persistent[i - 1] + noise[i]

        b_max = math.ceil(min(3.0 * math.sqrt(1_000), 1_000 / 3.0))
        circular = _reference_block_length(persistent, constant=SPEC_CIRCULAR_CONSTANT)
        stationary = _reference_block_length(persistent, constant=2.0)
        assert 1 < circular < b_max, "the series must land on an interior block for this pin to bite"
        assert circular != stationary, "the two constants must differ here or the pin proves nothing"

        assert optimal_block_length(persistent) == circular


class TestBlockBootstrap:
    def test_clustering_a_population_onto_fewer_dates_costs_sample_size(self) -> None:
        """⚠⚠ CRITERION 3'S ACTUAL CLAIM, TESTED DIRECTLY.

        *"Signals are correlated across instruments on the same day"* — so the
        SAME 400 trade returns, packed onto 20 dates instead of spread over 400,
        must yield a smaller effective sample size. The trade count is identical
        in both arms and the nominal ``n`` cannot tell them apart; that is the
        whole reason a nominal ``n`` is forbidden.
        """
        rng = np.random.default_rng(17)
        # A common shock per day plus per-trade noise: the packed arm's 20 trades
        # a day share their shock, which is the cross-sectional correlation.
        shocks = rng.standard_normal(20) * 3.0
        returns = [float(shocks[d] + rng.standard_normal()) for d in range(20) for _ in range(20)]

        packed_dates = [_DAY_ZERO + timedelta(days=d) for d in range(20) for _ in range(20)]
        spread_dates = _days(400)

        packed = block_bootstrap_expectancy(cluster_by_date(returns, packed_dates), seed=1)
        spread = block_bootstrap_expectancy(cluster_by_date(returns, spread_dates), seed=1)
        assert packed is not None and spread is not None

        assert packed.trade_count == spread.trade_count == 400
        assert packed.cluster_count == 20
        assert spread.cluster_count == 400
        assert packed.effective_sample_size < spread.effective_sample_size
        # And the clustered arm's design effect exceeds 1 — the overlap COST
        # evidence, which is the direction criterion 3 exists to expose.
        assert packed.design_effect > 1.0

    def test_the_ratio_estimator_equals_a_pooled_trade_mean(self) -> None:
        """⚠ The module claims the cluster-level ``(count, sum)`` resample is
        EXACT rather than an approximation of a per-trade resample. That rests on
        pooled expectancy being ``sum(sums) / sum(counts)``, so it is asserted
        against the raw trade list rather than trusted."""
        returns = [1.0, 3.0, -2.0, 4.0, 0.5]
        dates = [_DAY_ZERO, _DAY_ZERO, _DAY_ZERO + timedelta(days=1), _DAY_ZERO + timedelta(days=5), _DAY_ZERO]
        result = block_bootstrap_expectancy(cluster_by_date(returns, dates), seed=2, resamples=1_000)
        assert result is not None
        assert result.point_estimate_pct == pytest.approx(sum(returns) / len(returns))

    def test_the_same_seed_reproduces_and_a_different_seed_does_not(self) -> None:
        """Criterion 11 makes the seed a declared input; if it did not move the
        answer it would not be one, and storing it would be decoration."""
        rng = np.random.default_rng(23)
        returns = [float(v) for v in rng.standard_normal(300)]
        dates = _days(300)

        first = block_bootstrap_expectancy(cluster_by_date(returns, dates), seed=7)
        again = block_bootstrap_expectancy(cluster_by_date(returns, dates), seed=7)
        other = block_bootstrap_expectancy(cluster_by_date(returns, dates), seed=8)
        assert first is not None and again is not None and other is not None

        assert first == again
        assert first.effective_sample_size != other.effective_sample_size

    def test_the_interval_brackets_the_point_estimate_on_a_symmetric_population(self) -> None:
        rng = np.random.default_rng(29)
        returns = [float(v) for v in rng.standard_normal(400)]
        result = block_bootstrap_expectancy(cluster_by_date(returns, _days(400)), seed=3)
        assert result is not None
        assert result.ci_low_pct < result.point_estimate_pct < result.ci_high_pct

    def test_a_wider_confidence_gives_a_wider_interval(self) -> None:
        rng = np.random.default_rng(31)
        returns = [float(v) for v in rng.standard_normal(300)]
        clusters = cluster_by_date(returns, _days(300))

        narrow = block_bootstrap_expectancy(clusters, seed=4, confidence=0.80)
        wide = block_bootstrap_expectancy(clusters, seed=4, confidence=0.99)
        assert narrow is not None and wide is not None
        assert (wide.ci_high_pct - wide.ci_low_pct) > (narrow.ci_high_pct - narrow.ci_low_pct)

    def test_a_single_cluster_is_not_measurable(self) -> None:
        """⚠ None, NOT a fallback to the nominal count. Criterion 3: *"no bare
        percentage and no nominal n is reported anywhere"* — so a bootstrap that
        could not run must leave the gate refusing, never fill the column."""
        returns = [1.0, 2.0, 3.0]
        assert block_bootstrap_expectancy(cluster_by_date(returns, [_DAY_ZERO] * 3), seed=1) is None

    def test_a_zero_variance_population_is_not_measurable(self) -> None:
        """Kish's denominator ``s^2/n`` is zero, so the design effect is 0/0."""
        assert block_bootstrap_expectancy(cluster_by_date([2.0] * 50, _days(50)), seed=1) is None

    def test_a_single_trade_is_not_measurable(self) -> None:
        assert block_bootstrap_expectancy(cluster_by_date([1.5], _days(1)), seed=1) is None

    def test_a_nonsense_confidence_or_resample_count_raises(self) -> None:
        """⚠ These RAISE rather than returning None, and the split is deliberate:
        None means *the measurement could not be made from this data*, whereas a
        confidence of 1.5 is a caller bug and must not be reported as a data
        state the gate then refuses."""
        clusters = cluster_by_date([1.0, 2.0, 3.0], _days(3))
        with pytest.raises(ValueError, match="confidence"):
            block_bootstrap_expectancy(clusters, seed=1, confidence=1.5)
        with pytest.raises(ValueError, match="resamples"):
            block_bootstrap_expectancy(clusters, seed=1, resamples=0)

    def test_the_block_length_is_reported_and_fits_its_axis(self) -> None:
        rng = np.random.default_rng(37)
        returns = [float(v) for v in rng.standard_normal(250)]
        result = block_bootstrap_expectancy(cluster_by_date(returns, _days(250)), seed=5)
        assert result is not None
        assert 1 <= result.block_length <= result.cluster_count == 250
        assert result.model_id == SPEC_MODEL_ID
        assert result.resamples == SPEC_RESAMPLES

    def test_every_resample_gathers_exactly_the_axis_length(self) -> None:
        """⚠⚠ The concatenation is TRUNCATED back to ``n`` clusters. Without the
        truncation the final block overhangs and each resample estimates a
        LONGER sample than the one in hand, biasing the variance downward —
        i.e. narrowing the interval, the flattering direction.

        Detected deterministically rather than statistically. Every cluster
        holds one trade, and only cluster 0 carries a non-zero return, so the
        statistic is ``(times cluster 0 was drawn) / (clusters gathered)``. With
        ``n = 50`` and ``b = 7`` the untruncated gather is ``ceil(50/7) * 7 =
        56``, so the denominator is 50 or 56 and nothing in between: multiplying
        by 50 yields whole numbers under truncation and almost never otherwise.
        """
        from app.services.block_bootstrap import _circular_block_statistics

        returns = [1.0] + [0.0] * 49
        clusters = cluster_by_date(returns, _days(50))
        stats = _circular_block_statistics(clusters, block_length=7, resamples=64, seed=9)

        assert stats.shape == (64,)
        scaled = stats * 50.0
        assert np.allclose(scaled, np.round(scaled)), (
            "a statistic that is not a multiple of 1/50 means the resample gathered a number of clusters "
            "other than the 50 on the axis"
        )
        # And cluster 0 really was drawn a varying number of times, so the
        # assertion above is not passing on a constant column of zeros.
        assert len(set(np.round(scaled).astype(int).tolist())) > 1


class TestCircularityAndCoverage:
    """The two properties the other tests structurally cannot see."""

    def test_every_cluster_is_drawn_with_equal_frequency(self) -> None:
        """⚠⚠ THE WRAP, and why it is not cosmetic.

        Politis & Romano's circular scheme wraps a block past the end of the
        axis so every cluster is equally likely to be drawn. The obvious
        alternative — clamping a block to the last index — over-samples the tail
        and starves the head, which biases the statistic toward whatever the
        final dates did. ⚠ Neither the truncation test nor any ESS test can see
        it: the resample still gathers exactly ``n`` clusters, so only the
        FREQUENCIES differ.

        Measured 2026-08-07 on this fixture: the wrap gives first 0.01941 /
        last 0.01953 (a ratio of 1.006), and clamping gives 0.00311 / 0.07562
        (a ratio of 24). The tolerance below sits far inside that gap.
        """
        from app.services.block_bootstrap import _circular_block_statistics

        def drawn_frequency(which: int) -> float:
            returns = [0.0] * 50
            returns[which] = 1.0
            clusters = cluster_by_date(returns, _days(50))
            return float(_circular_block_statistics(clusters, block_length=7, resamples=4_000, seed=9).mean())

        first, last = drawn_frequency(0), drawn_frequency(49)
        assert first == pytest.approx(last, rel=0.15), (
            f"cluster 0 drawn at {first} against cluster 49 at {last} — a block that does not wrap over-samples "
            "the end of the axis and starves the start"
        )

    @pytest.mark.parametrize("confidence", [0.80, 0.95])
    def test_the_interval_covers_the_declared_share_of_the_distribution(self, confidence: float) -> None:
        """⚠⚠ THE TAIL IS HALVED BETWEEN THE TWO SIDES.

        A two-sided 95% interval runs from the 2.5th to the 97.5th percentile.
        Forgetting the halving gives the 5th-to-95th — a 90% interval reported
        as 95%, i.e. NARROWER than its own label, which is the flattering
        direction. ⚠ The wider-confidence-gives-a-wider-interval test cannot see
        this: the un-halved form is still monotone in ``confidence``.

        Asserted as COVERAGE against the same resample stream, which is what the
        percentile method means.
        """
        from app.services.block_bootstrap import _circular_block_statistics

        rng = np.random.default_rng(29)
        clusters = cluster_by_date([float(v) for v in rng.standard_normal(400)], _days(400))
        result = block_bootstrap_expectancy(clusters, seed=3, confidence=confidence)
        assert result is not None

        statistics = _circular_block_statistics(
            clusters, block_length=result.block_length, resamples=result.resamples, seed=3
        )
        inside = float(((statistics >= result.ci_low_pct) & (statistics <= result.ci_high_pct)).mean())
        assert inside == pytest.approx(confidence, abs=0.01)


class TestBootstrapResultInvariants:
    def _valid(self, **overrides: object) -> dict[str, object]:
        base: dict[str, object] = {
            "effective_sample_size": 40.0,
            "ci_low_pct": -1.0,
            "ci_high_pct": 1.0,
            "point_estimate_pct": 0.0,
            "block_length": 5,
            "cluster_count": 100,
            "trade_count": 400,
            "resamples": SPEC_RESAMPLES,
            "seed": 1,
            "design_effect": 10.0,
        }
        base.update(overrides)
        return base

    def test_the_valid_shape_constructs(self) -> None:
        assert BootstrapResult(**self._valid()).model_id == SPEC_MODEL_ID  # type: ignore[arg-type]

    @pytest.mark.parametrize(
        ("overrides", "match"),
        [
            ({"effective_sample_size": 0.0}, "positive"),
            ({"ci_low_pct": 2.0}, "inverted"),
            ({"block_length": 0}, "at least 1"),
            ({"block_length": 101}, "exceeds"),
            ({"design_effect": 0.0}, "design_effect"),
        ],
    )
    def test_the_broken_shapes_are_refused(self, overrides: dict, match: str) -> None:
        with pytest.raises(ValueError, match=match):
            BootstrapResult(**self._valid(**overrides))  # type: ignore[arg-type]
