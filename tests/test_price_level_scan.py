"""``LevelScan`` — whole-series pivot detection hoisted out of the per-bar call.

Module under test: ``app/services/price_levels.py``.

⚠ THE HOIST IS A PERFORMANCE CHANGE AND MUST NOT BE A BEHAVIOURAL ONE. S-5 and
S-6 both ask for levels at every bar of a series, so the cost is real; the
verdicts must be identical to the form this replaced, and that is asserted here
rather than argued.

Pure tier: no database, no fixtures, no IO.
"""

from __future__ import annotations

import numpy as np
import pytest

from app.services.price_levels import (
    _PAIRWISE_SUMMATION_BLOCK,
    CLUSTER_ATR_TOLERANCE,
    MAX_TOUCH_AGE_BARS,
    MIN_TOUCHES,
    LevelScan,
    PriceLevel,
    _cluster,
    levels_at,
    swing_pivots,
)


class TestLevelScanReproducesTheScalarForm:
    """⚠ The hoist is a PERFORMANCE change and must not be a behavioural one.

    ``levels_at`` now builds a ``LevelScan`` and calls ``at``, so there is one
    code path — but the equivalence that made the hoist legal (a pivot's verdict
    depends only on its own +/- 5 bars, never on where the observer stands) is
    the claim, and it is asserted rather than argued.
    """

    @staticmethod
    def _wavy(n: int = 120) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        i = np.arange(n, dtype=float)
        highs = 100.0 + 5.0 * np.sin(i / 3.0) + 0.4 * np.sin(i / 11.0)
        lows = highs - 2.0
        volumes = np.full(n, 1_000.0)
        return highs, lows, volumes

    def test_the_hoisted_and_scalar_forms_agree_at_every_index(self) -> None:
        highs, lows, volumes = self._wavy()
        scan = LevelScan.build(highs=highs, lows=lows, volumes=volumes)
        seen = 0
        for index in range(highs.size):
            hoisted = scan.at(atr=1.5, index=index)
            scalar = levels_at(highs=highs, lows=lows, volumes=volumes, atr=1.5, index=index)
            assert hoisted == scalar
            seen += len(hoisted)
        assert seen > 0, "a fixture with no levels would make this vacuous"

    def test_a_pivot_is_never_reported_before_it_is_confirmed(self) -> None:
        """⚠⚠ The lookahead this whole construction exists to prevent. The last
        candidate is ``index - 5``, so a pivot at ``index`` is unknowable."""
        highs, lows, _ = self._wavy()
        pivots = swing_pivots(highs, lows)
        assert pivots.high_indices, "no pivots detected — the fixture proves nothing"
        for index in range(20, highs.size):
            live = LevelScan.build(highs=highs, lows=lows, volumes=None).at(atr=1.5, index=index)
            for level in live:
                assert level.last_touch_index <= index - pivots.half_window

    def test_ragged_inputs_raise_rather_than_returning_nothing(self) -> None:
        with pytest.raises(ValueError, match="must align"):
            LevelScan.build(highs=np.zeros(5), lows=np.zeros(4), volumes=None)


def _reference_cluster(
    indices: list[int],
    prices: np.ndarray,
    volumes: np.ndarray | None,
    *,
    tolerance: float,
) -> list[tuple[float, list[int]]]:
    """The pre-#2780 clustering, transcribed so the fast path is checked against it.

    ⚠ A COPY ON PURPOSE. Importing the implementation to test the implementation
    is the tautology the repo's #2240 S-3 lesson names; this is the arithmetic the
    optimisation must reproduce, written out, and it must not be refactored to
    share code with the thing it validates.
    """
    if not indices:
        return []
    order = sorted(indices, key=lambda i: prices[i])
    clusters: list[list[int]] = [[order[0]]]
    for idx in order[1:]:
        if abs(prices[idx] - prices[clusters[-1][-1]]) <= tolerance:
            clusters[-1].append(idx)
        else:
            clusters.append([idx])
    out: list[tuple[float, list[int]]] = []
    for cluster in clusters:
        if volumes is None:
            price = float(np.mean([prices[i] for i in cluster]))
        else:
            weights = np.array([max(volumes[i], 0.0) for i in cluster])
            values = np.array([prices[i] for i in cluster])
            price = float(np.average(values, weights=weights)) if weights.sum() > 0 else float(values.mean())
        out.append((price, cluster))
    return out


class TestClusteringIsBitIdenticalAfterVectorisation:
    """#2780 — ``_cluster`` was 77% of s5's runtime; the rewrite must not move a bit.

    ⚠⚠ A LEVEL PRICE FEEDS A THRESHOLD COMPARISON, so "close enough" is not a
    standard this change may be held to: a last-bit difference can flip which
    side of a support level a close sits on, and therefore which trades exist.
    Equality below is `==` on floats deliberately, never `approx`.
    """

    def test_the_pairwise_boundary_the_fast_path_hands_back_at_is_real(self) -> None:
        """⚠ The constant is justified as NumPy's own blocksize, so verify that
        rather than trusting the comment. Below it a sequential accumulation
        equals ``ndarray.sum``; at it, NumPy switches to pairwise and they part.
        """
        rng = np.random.default_rng(20260820)
        values = rng.uniform(1.0, 1e6, 64)

        def sequential(view: np.ndarray) -> float:
            total = 0.0
            for value in view:
                total += float(value)
            return total

        assert all(sequential(values[:n]) == float(values[:n].sum()) for n in range(1, _PAIRWISE_SUMMATION_BLOCK))
        assert any(
            sequential(values[:n]) != float(values[:n].sum()) for n in range(_PAIRWISE_SUMMATION_BLOCK, values.size + 1)
        ), "no divergence at or above the boundary — the fallback may be guarding nothing"

    def test_it_matches_the_previous_arithmetic_bit_for_bit(self) -> None:
        rng = np.random.default_rng(2026)
        compared = 0
        for trial in range(1500):
            count = int(rng.integers(1, 180))
            prices = rng.uniform(1.0, 900.0, count + 5)
            mode = trial % 3
            volumes = None if mode == 0 else rng.uniform(0.0, 1e7, prices.size)
            if mode == 2 and volumes is not None:
                # Zero-weight clusters take the unweighted fallback branch.
                volumes[rng.integers(0, volumes.size, size=max(1, volumes.size // 4))] = 0.0
            indices = sorted({int(i) for i in rng.integers(0, prices.size, size=count)})
            tolerance = float(rng.choice([0.01, 0.5, 2.0, 10.0, 60.0, 400.0]))

            expected = _reference_cluster(indices, prices, volumes, tolerance=tolerance)
            actual = _cluster(indices, prices, volumes, tolerance=tolerance)

            assert [members for _, members in actual] == [members for _, members in expected], (
                "cluster membership and its ORDER must be preserved — `at` sums volumes over it"
            )
            assert [price for price, _ in actual] == [price for price, _ in expected]
            compared += 1
        assert compared == 1500

    def test_an_empty_pivot_set_is_still_empty(self) -> None:
        assert _cluster([], np.zeros(3), None, tolerance=1.0) == []


def _reference_at(
    scan: LevelScan,
    *,
    atr: float,
    index: int,
) -> tuple[PriceLevel, ...]:
    """The pre-filter-hoist ``LevelScan.at``, transcribed.

    ⚠ A COPY ON PURPOSE, and built on ``_reference_cluster`` rather than on
    ``_cluster``, so the whole chain is checked against hand-written arithmetic
    instead of against another part of the implementation. The #2240 S-3 lesson
    is that a reference which imports what it validates is a tautology; that
    applies to a reference which imports the validated thing's *helper* too.

    This form materialised every cluster and asked ``len``/``max`` of the list.
    ``at`` now answers both from ``_segment``'s arrays and never builds the
    list. The two must return equal tuples — ``==``, never ``approx``.
    """
    if index < 0 or index >= scan.highs.size:
        return ()
    if not np.isfinite(atr) or atr <= 0:
        return ()
    tolerance = CLUSTER_ATR_TOLERANCE * atr
    last_confirmed = index - scan.pivots.half_window
    hi_idx = [i for i in scan.pivots.high_indices if i <= last_confirmed]
    lo_idx = [i for i in scan.pivots.low_indices if i <= last_confirmed]

    total = 0.0 if scan.volume_cumsum is None else float(scan.volume_cumsum[index])
    out: list[PriceLevel] = []
    for kind, idxs, prices in (("resistance", hi_idx, scan.highs), ("support", lo_idx, scan.lows)):
        for price, cluster in _reference_cluster(idxs, prices, scan.volumes, tolerance=tolerance):
            touches = len(cluster)
            last_touch = max(cluster)
            if touches < MIN_TOUCHES:
                continue
            if index - last_touch > MAX_TOUCH_AGE_BARS:
                continue
            if scan.volumes is None:
                share = 1.0
            else:
                share = float(np.nansum([scan.volumes[i] for i in cluster])) / total if total > 0 else 0.0
            strength = touches * float(np.log1p(share))
            out.append(
                PriceLevel(
                    price=price,
                    kind=kind,  # type: ignore[arg-type]
                    touches=touches,
                    last_touch_index=last_touch,
                    strength=strength,
                )
            )
    return tuple(sorted(out, key=lambda level: level.strength, reverse=True))


class TestAtFiltersBeforeMaterialisingWithoutMovingAVerdict:
    """#2780 — ``at`` discarded 94.1% of the clusters ``_cluster`` built for it.

    ⚠⚠ ``touches`` AND ``last_touch`` ARE THE ONLY THINGS THE MEMBER LIST WAS
    EVER READ FOR, and ``PriceLevel`` stores neither list nor anything derived
    from its order except the volume share. The hoist is therefore legal only if
    every surviving level is bit-identical, which is asserted here over
    randomised series rather than argued from the shape of the change.

    Pure tier: no database, no fixtures, no IO.
    """

    @staticmethod
    def _series(rng: np.random.Generator, n: int) -> tuple[np.ndarray, np.ndarray, np.ndarray | None]:
        """A wandering series, so pivots recur near the same prices and cluster."""
        steps = rng.normal(0.0, 1.0, n).cumsum()
        mid = 100.0 + 6.0 * np.sin(np.arange(n) / 7.0) + 0.6 * steps
        highs = mid + rng.uniform(0.05, 1.2, n)
        lows = mid - rng.uniform(0.05, 1.2, n)
        volumes: np.ndarray | None
        mode = int(rng.integers(0, 3))
        if mode == 0:
            volumes = None
        else:
            volumes = rng.uniform(0.0, 5e6, n)
            if mode == 2:
                # Zero-weight and NaN volumes reach the fallback and `nansum`.
                volumes[rng.integers(0, n, size=max(1, n // 5))] = 0.0
                volumes[rng.integers(0, n, size=max(1, n // 20))] = np.nan
        return highs, lows, volumes

    def test_it_matches_the_materialising_form_at_every_bar(self) -> None:
        rng = np.random.default_rng(20260820)
        levels_seen = 0
        bars_compared = 0
        for _ in range(25):
            n = int(rng.integers(200, 950))
            highs, lows, volumes = self._series(rng, n)
            scan = LevelScan.build(highs=highs, lows=lows, volumes=volumes)
            atr = float(rng.choice([0.75, 1.5, 4.0, 12.0]))
            for index in range(n):
                actual = scan.at(atr=atr, index=index)
                expected = _reference_at(scan, atr=atr, index=index)
                assert actual == expected
                levels_seen += len(actual)
                bars_compared += 1
        assert bars_compared > 5_000, "too few bars walked for the comparison to mean much"
        # ⚠ A fixture that refuses everything proves nothing: equality against
        # the oracle passes trivially when both sides are empty at every bar.
        assert levels_seen > 500, f"only {levels_seen} levels survived — the filters are being tested on nothing"

    def test_both_filters_actually_reject_something_in_the_fixture(self) -> None:
        """⚠ The hoist moves ``MIN_TOUCHES`` and ``MAX_TOUCH_AGE_BARS`` into the
        vectorised pass. If neither ever rejected a cluster here, the test above
        would be comparing two unfiltered paths and the hoist would be unproven.
        """
        rng = np.random.default_rng(4)
        highs, lows, volumes = self._series(rng, 900)
        scan = LevelScan.build(highs=highs, lows=lows, volumes=volumes)
        thin = 0
        stale = 0
        built = 0
        for index in range(scan.highs.size):
            last_confirmed = index - scan.pivots.half_window
            for idxs, prices in ((scan.pivots.high_indices, scan.highs), (scan.pivots.low_indices, scan.lows)):
                live = [i for i in idxs if i <= last_confirmed]
                for _, cluster in _reference_cluster(live, prices, scan.volumes, tolerance=CLUSTER_ATR_TOLERANCE * 1.5):
                    built += 1
                    if len(cluster) < MIN_TOUCHES:
                        thin += 1
                    elif index - max(cluster) > MAX_TOUCH_AGE_BARS:
                        stale += 1
        assert thin > 0, "no cluster was ever rejected for too few touches"
        assert stale > 0, "no cluster was ever rejected for staleness — the age filter is untested"
        assert built > thin + stale, "every cluster was rejected; nothing survived to compare"

    def test_a_series_with_no_confirmed_pivots_yet_returns_nothing(self) -> None:
        """``_segment`` returns ``None`` for an empty pivot prefix; ``at`` must
        treat that as no levels rather than raising on a zero-length reduceat."""
        highs = np.linspace(10.0, 40.0, 60)
        lows = highs - 1.0
        scan = LevelScan.build(highs=highs, lows=lows, volumes=None)
        assert scan.at(atr=1.0, index=0) == ()
        assert scan.at(atr=1.0, index=3) == ()

    def test_a_nonfinite_or_nonpositive_atr_still_refuses(self) -> None:
        highs, lows, volumes = self._series(np.random.default_rng(9), 120)
        scan = LevelScan.build(highs=highs, lows=lows, volumes=volumes)
        assert scan.at(atr=float("nan"), index=100) == ()
        assert scan.at(atr=0.0, index=100) == ()
        assert scan.at(atr=-1.0, index=100) == ()
        assert scan.at(atr=1.5, index=scan.highs.size) == ()
        assert scan.at(atr=1.5, index=-1) == ()
