"""#3471 §9 — the cluster sign-flip randomisation p, shared by the planning table and the readout.

Spec: ``docs/proposals/execution/2026-09-28-3471-ai-discretionary-v1.md`` §9 "Primary statistic".

- A cluster is every pair entered on one ``session_date``; a flip negates every *d* in it.
- The statistic is the equal-weight mean of *d* over units, so a flip moves a cluster by its SUM.
- With ≤ ``EXACT_MAX_CLUSTERS`` clusters every flip is enumerated (the identity included) and
  p = #{T_b ≥ T_obs} / 2^K.
- Above that, B flips come from a declared seed and p = (1 + #{T_b ≥ T_obs}) / (1 + B): the
  identity is counted once, explicitly, and ties count as ≥.

⚠ Pure numpy, no DB and no model. The null is approximate (§9 lists why); nothing here makes it
exact.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Final, Literal

import numpy as np
import numpy.typing as npt

EXACT_MAX_CLUSTERS: Final = 16
MONTE_CARLO_FLIPS: Final = 99_999

Alternative = Literal["greater", "less"]

#: Relative tolerance for "T_b ≥ T_obs". A flip that reproduces T_obs up to float summation order
#: is a tie, and ties count as ≥ (§9) — without the tolerance a reordered sum could drop one.
_TIE_RTOL: Final = 1e-12
#: Upper bound on the (flips × rows) T_b block held at once, in elements (~64 MB of float64).
_BLOCK_ELEMENTS: Final = 8_000_000


@dataclass(frozen=True)
class FlipSet:
    """±1 flip rows, one column per cluster. ``exact`` rows are all 2^K sign vectors (row 0 is the
    identity); Monte-Carlo rows carry no forced identity — the p formula adds it."""

    signs: npt.NDArray[np.float64]
    exact: bool


def flip_set(n_clusters: int, *, seed: int, flips: int = MONTE_CARLO_FLIPS) -> FlipSet:
    """All 2^K flips when K ≤ ``EXACT_MAX_CLUSTERS``, else ``flips`` Monte-Carlo rows from ``seed``."""
    if n_clusters < 1:
        raise ValueError(f"need at least one cluster, got {n_clusters}")
    if n_clusters <= EXACT_MAX_CLUSTERS:
        codes = np.arange(2**n_clusters, dtype=np.int64)[:, None]
        bits = (codes >> np.arange(n_clusters, dtype=np.int64)) & 1
        return FlipSet((1 - 2 * bits).astype(np.float64), exact=True)
    if flips < 1:
        raise ValueError(f"flips must be >= 1, got {flips}")
    rng = np.random.default_rng(seed)
    return FlipSet(rng.choice(np.array([-1.0, 1.0]), size=(flips, n_clusters)), exact=False)


def sign_flip_p(
    cluster_sums: Sequence[float] | npt.NDArray[np.float64],
    flips: FlipSet,
    *,
    alternative: Alternative = "greater",
) -> float:
    """One-sided p for the mean of *d*, given each cluster's sum of *d*.

    The unit count divides T_obs and every T_b alike, so the test runs on the sums. ``alternative``
    "greater" tests mean(d) > 0 (the readout); "less" tests mean(d) < 0 (the harm stop).
    """
    sums = np.asarray(cluster_sums, dtype=np.float64)
    if sums.ndim != 1:
        raise ValueError(f"cluster sums must be 1-D, got shape {sums.shape}")
    return float(sign_flip_p_batch(sums[None, :], flips, alternative=alternative)[0])


def sign_flip_p_batch(
    cluster_sums: npt.NDArray[np.float64],
    flips: FlipSet,
    *,
    alternative: Alternative = "greater",
) -> npt.NDArray[np.float64]:
    """``sign_flip_p`` for each row of an (R × K) array of cluster sums, against one flip set."""
    sums = np.asarray(cluster_sums, dtype=np.float64)
    signs = flips.signs
    if sums.ndim != 2 or signs.ndim != 2 or signs.shape[1] != sums.shape[1]:
        raise ValueError(f"shape mismatch: sums {sums.shape}, signs {signs.shape}")
    if not np.all(np.isfinite(sums)):
        raise ValueError("cluster sums must be finite")
    orient = 1.0 if alternative == "greater" else -1.0
    t_obs = orient * sums.sum(axis=1)  # (R,)
    floor = t_obs - _TIE_RTOL * np.abs(sums).sum(axis=1)  # purely relative: scale-invariant
    hits = np.empty(sums.shape[0], dtype=np.float64)
    step = max(1, _BLOCK_ELEMENTS // signs.shape[0])
    for lo in range(0, sums.shape[0], step):
        t_b = orient * (signs @ sums[lo : lo + step].T)  # (B, rows)
        hits[lo : lo + step] = np.count_nonzero(t_b >= floor[None, lo : lo + step], axis=0)
    if flips.exact:
        return hits / signs.shape[0]
    return (1.0 + hits) / (1.0 + signs.shape[0])
