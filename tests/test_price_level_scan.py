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
    LevelScan,
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
