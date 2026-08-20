"""Support and resistance levels (S-5…S-10 §2). Pure, versioned, no I/O.

Spec: ``docs/proposals/ta/2026-08-14-strategy-set-s5-s10.md`` §2.

⚠⚠ THERE IS NO PUBLISHED FORMULATION FOR PRICE-LEVEL CLUSTERING, AND THIS MODULE
SAYS SO RATHER THAN INVENTING A CITATION.

The repo rule for that case is explicit: where a published formulation genuinely
does not exist, state it, fix the rule BY CONSTRUCTION, and freeze the constants
in a version hash. Every number below is arbitrary-but-declared. None is tuned,
none is fitted, and none should be described anywhere as derived.

⚠ This is the honest counterpart to ``market_regime``, where both legs DO have
published rules (the 200-SMA and Bollinger's six-month Squeeze/Bulge). The two
modules are deliberately different in provenance, and conflating them — treating
these constants as equally grounded — is the error to avoid.

WHAT A LEVEL IS HERE
--------------------
A price where the market has repeatedly turned. Operationally: a cluster of swing
pivots, close together relative to the instrument's own volatility, touched
several times, recently enough to still matter.

⚠ ATR-RELATIVE, NOT PERCENT. A $400 stock and a $4 stock do not have the same
notion of "close together", and a fixed percentage gets that wrong in opposite
directions at the two ends of the universe. Clustering in ATR units makes the
tolerance the instrument's own.
"""

from __future__ import annotations

import bisect
import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Final, Literal

import numpy as np
import numpy.typing as npt
from numpy.lib.stride_tricks import sliding_window_view

#: Bars either side of a pivot that it must exceed. FROZEN BY CONSTRUCTION.
#: Larger = fewer, more significant pivots; smaller = noise. 5 is a choice.
PIVOT_HALF_WINDOW: Final[int] = 5

#: Cluster tolerance in ATR(14) units. FROZEN BY CONSTRUCTION.
CLUSTER_ATR_TOLERANCE: Final[float] = 0.5

#: Minimum touches before a cluster is a level at all. FROZEN BY CONSTRUCTION.
#: Two points define a line through any two points; three is the smallest count
#: that is not automatically satisfiable.
MIN_TOUCHES: Final[int] = 3

#: A level whose most recent touch is older than this is stale. FROZEN.
MAX_TOUCH_AGE_BARS: Final[int] = 120

_RULE_SET_ID: Final[str] = "price-levels-v1"


def _code_hash() -> str:
    return hashlib.sha256(Path(__file__).read_bytes()).hexdigest()[:12]


#: ⚠ Every constant above is hashed in. Moving ANY of them is a new version —
#: they were frozen together and are only meaningful together, so bumping one
#: while claiming continuity with a stored track record is not available.
LEVEL_RULE_VERSION: Final[str] = f"{_RULE_SET_ID}+{_code_hash()}"

LevelKind = Literal["support", "resistance"]


@dataclass(frozen=True)
class PriceLevel:
    """One clustered level, as of a stated bar index.

    ``strength`` ranks levels against each other and is NEVER compared to a
    threshold — it has no units and no calibration, so a cut on it would be a
    fitted parameter wearing a score's clothing.
    """

    price: float
    kind: LevelKind
    touches: int
    last_touch_index: int
    strength: float
    rule_set_version: str = LEVEL_RULE_VERSION


@dataclass(frozen=True)
class PivotSet:
    """Every confirmed swing pivot in one series, detected once.

    ⚠⚠ A PIVOT IS ONLY CONFIRMED ``half_window`` BARS AFTER IT HAPPENS. Whether
    index ``i`` is a swing high is decided ENTIRELY by bars ``i - half_window ..
    i + half_window`` and by nothing else, so the verdict is the same whether it
    is computed while standing at bar ``i + half_window`` or at the end of the
    series. That independence is what makes precomputing the whole series safe,
    and it is why ``LevelScan.at`` filters by ``index - half_window`` rather than
    re-detecting: the filter reproduces "confirmed by bar ``index``" exactly.

    Reading a pivot at ``index`` itself would be lookahead of the most seductive
    kind — it reads as "today is a swing high", which cannot be known today. It
    needs the next ``half_window`` bars to fail to exceed it. Every level built
    from such a pivot would be fitted to bars the strategy has not seen, and the
    resulting backtest would be silently, spectacularly optimistic.
    """

    #: Ascending, so ``LevelScan.at`` can bisect rather than filter.
    high_indices: tuple[int, ...]
    low_indices: tuple[int, ...]
    half_window: int


def swing_pivots(
    highs: npt.NDArray[np.float64],
    lows: npt.NDArray[np.float64],
    *,
    half_window: int = PIVOT_HALF_WINDOW,
) -> PivotSet:
    """Detect every confirmed swing high and low in one vectorised pass.

    ⚠ THE TIE RULE IS ``argmax == half_window``, NOT ``high == max``, AND THE
    DIFFERENCE IS LOAD-BEARING. ``argmax`` returns the FIRST occurrence, so when
    two bars in a window share the maximum only the earlier one is a pivot. The
    redundant-looking ``>=`` comparison is kept beside it because the two
    together are the rule as written, and dropping either changes which of two
    equal highs is called the turn.

    ⚠ A non-finite value in EITHER window disqualifies BOTH kinds at that index.
    A masked high and a masked low are equally "this bar's shape is unknown", and
    treating them separately would let a bar with a hidden low be called a swing
    high. The consequence is stated rather than hidden: a masked bar silently
    removes pivot candidates, so a level that should exist may not, and a caller
    that needs that distinction must gate on the mask itself — this function
    reports pivots, not evaluability.
    """
    if half_window < 1:
        raise ValueError(f"half_window must be at least 1, got {half_window}")
    if highs.size != lows.size:
        raise ValueError(f"highs/lows must align: {highs.size} highs against {lows.size} lows")
    width = 2 * half_window + 1
    if highs.size < width:
        return PivotSet(high_indices=(), low_indices=(), half_window=half_window)

    window_hi = sliding_window_view(highs, width)
    window_lo = sliding_window_view(lows, width)
    # ⚠ Row `k` of the view covers bars `k .. k + width - 1`, so its CENTRE is
    # bar `k + half_window`. Reading row `k` as bar `k` is the off-by-five this
    # alignment array exists to make impossible to write by accident.
    centres = np.arange(half_window, highs.size - half_window)
    finite = np.all(np.isfinite(window_hi), axis=1) & np.all(np.isfinite(window_lo), axis=1)
    is_high = finite & (np.argmax(window_hi, axis=1) == half_window) & (highs[centres] >= np.max(window_hi, axis=1))
    is_low = finite & (np.argmin(window_lo, axis=1) == half_window) & (lows[centres] <= np.min(window_lo, axis=1))
    return PivotSet(
        high_indices=tuple(int(i) for i in centres[is_high]),
        low_indices=tuple(int(i) for i in centres[is_low]),
        half_window=half_window,
    )


#: NumPy's reduction runs sequentially below this many elements and switches to
#: PAIRWISE summation at or above it. ⚠ Not a tuned constant and not ours: it is
#: the boundary at which a sequential accumulation stops being bit-identical to
#: ``ndarray.sum``, so it is the exact point where the fast path below must hand
#: back to ``np.average``. Verified empirically in
#: ``tests/test_price_levels.py`` rather than trusted from the NumPy source.
_PAIRWISE_SUMMATION_BLOCK = 8


def _cluster(
    indices: list[int],
    prices: npt.NDArray[np.float64],
    volumes: npt.NDArray[np.float64] | None,
    *,
    tolerance: float,
) -> list[tuple[float, list[int]]]:
    """Group pivots whose prices lie within ``tolerance`` of each other.

    Single-linkage on a price-sorted list: walk in price order and start a new
    cluster whenever the gap to the previous pivot exceeds the tolerance.

    ⚠ Single-linkage can chain — a dense ladder of pivots each within tolerance
    of the next merges into one wide cluster. Accepted deliberately: the
    alternative (a fixed number of clusters, or a centroid method) needs a
    parameter that IS fitted, and a chained cluster in a tight range is arguably
    one level. The consequence is that a level's price is a volume-weighted mean
    that may sit where no pivot actually is; ``touches`` remains honest.
    """
    if not indices:
        return []

    # ⚠⚠ VECTORISED, AND BIT-IDENTICAL TO THE LOOP IT REPLACES BY CONSTRUCTION
    # (#2780). This was 77% of s5-support-bounce's runtime — ``np.average`` alone
    # was called 30,056,519 times in one 300-series profile, a third of the whole
    # evaluation — because it built two NumPy arrays per cluster and averaged
    # them, on clusters that are typically a handful of pivots. Every step below
    # reproduces the previous arithmetic exactly rather than approximately:
    #
    #   * ``kind="stable"`` reproduces ``sorted(..., key=...)``, which is stable,
    #     so equal prices keep their original index order and the cluster member
    #     lists are unchanged. Callers depend on that order — ``at`` sums
    #     ``volumes`` over a cluster, and float addition is not commutative.
    #   * the walk compared each pivot with the LAST MEMBER APPENDED, which in
    #     price order is simply its predecessor, so ``diff`` is the same test.
    #     ``> tolerance`` starts a cluster exactly where ``<= tolerance`` failed
    #     to continue one; ``abs`` was redundant on an ascending sort.
    #   * ``bincount`` accumulates in array order, which IS the sorted order the
    #     old per-cluster arrays carried, so the summation order is preserved.
    #
    # ⚠⚠ THE ONE PLACE THEY WOULD DIVERGE IS HANDLED, NOT ASSUMED AWAY. NumPy's
    # reduction switches from sequential to PAIRWISE summation at 8 elements, so
    # for a cluster that large ``bincount``'s sequential accumulation differs
    # from ``np.average`` in the last bits. Measured on the prototype: max
    # absolute difference 7.1e-15. That is not negligible here — a level price
    # feeds a threshold comparison, so a last-bit change can flip which trades
    # exist. Clusters of 8 or more therefore fall back to ``np.average`` on
    # exactly the old inputs. The boundary is NumPy's own blocksize, not a fitted
    # constant.
    idx = np.asarray(indices, dtype=np.int64)
    order = idx[np.argsort(prices[idx], kind="stable")]
    ordered_prices = prices[order]
    if order.size == 1:
        starts_cluster = np.zeros(0, dtype=np.bool_)
    else:
        starts_cluster = np.diff(ordered_prices) > tolerance
    group = np.concatenate((np.zeros(1, dtype=np.int64), np.cumsum(starts_cluster, dtype=np.int64)))
    sizes = np.bincount(group)
    bounds = np.concatenate((np.zeros(1, dtype=np.int64), np.cumsum(sizes, dtype=np.int64)))

    # ⚠ Bound unconditionally so the fallback below narrows on THIS name rather
    # than re-testing ``volumes``. Two separate ``volumes is None`` checks are
    # equivalent at runtime but not to a type checker, and the pre-push gate was
    # right to refuse the version that had them.
    ordered_weights = None if volumes is None else np.maximum(volumes[order], 0.0)

    if ordered_weights is None:
        totals = np.bincount(group, weights=ordered_prices)
        fast_prices = totals / sizes
    else:
        weight_totals = np.bincount(group, weights=ordered_weights)
        value_totals = np.bincount(group, weights=ordered_prices * ordered_weights)
        with np.errstate(invalid="ignore", divide="ignore"):
            fast_prices = value_totals / weight_totals
        # ``weights.sum() > 0`` fell back to the unweighted mean; reproduced here.
        unweighted = np.bincount(group, weights=ordered_prices) / sizes
        fast_prices = np.where(weight_totals > 0.0, fast_prices, unweighted)

    out: list[tuple[float, list[int]]] = []
    for gid in range(int(sizes.size)):
        lo, hi = int(bounds[gid]), int(bounds[gid + 1])
        cluster = [int(i) for i in order[lo:hi]]
        if hi - lo < _PAIRWISE_SUMMATION_BLOCK:
            price = float(fast_prices[gid])
        elif ordered_weights is None:
            price = float(np.mean(ordered_prices[lo:hi]))
        else:
            weights = ordered_weights[lo:hi]
            values = ordered_prices[lo:hi]
            price = float(np.average(values, weights=weights)) if weights.sum() > 0 else float(values.mean())
        out.append((price, cluster))
    return out


@dataclass(frozen=True)
class LevelScan:
    """One series, prepared once, so levels can be asked for at EVERY bar.

    ⚠⚠ THIS EXISTS FOR A MEASURED REASON, NOT A STYLISTIC ONE. The strategy
    runner evaluates a strategy over a whole series and filters its OUTPUT
    (``strategy_signal_scan._scan_per_series``: *"The strategy sees the WHOLE
    series and the filter is applied to its output"*), so a level-based strategy
    needs levels at every bar, not just the last one. **S-5 and S-6 both run in
    exactly that shape**, so this is not a hypothetical access pattern.

    The ORIGINAL form detected pivots inside every call with a per-index Python
    loop, which makes a full-series walk quadratic. Both figures below are
    measured, and each names the population it was measured over — a bare
    speedup with an implied subject is the shape the repo rule about hand-written
    statistics exists to stop:

    * **3.61 ms/bar** — the replaced form, timed over SPY's 844 evaluable indices.
    * **0.1532 ms/bar** — this form, timed over the **3,107,697 bars** of the
      3,854 validated-universe instruments with enough history to be scanned.

    So the full-population walk takes **476 seconds** here, against roughly
    **187 minutes** (3,107,697 x 3.61 ms) for the form it replaces. Reproduce::

        uv run python scripts/verify_2437_level_scan.py --census

    ⚠ ``--equivalence`` reports a speedup of only ~2.3x, and that is NOT a
    contradiction: it times ``LevelScan.at`` against the CURRENT ``levels_at``,
    which builds one of these per call and therefore already gets the vectorised
    detection. The ~24x is against the per-index loop. Two different comparisons,
    and a reader who conflates them will think the hoist barely paid — hence both
    subjects being named rather than one number quoted.

    ⚠ IT IS NOT A SECOND IMPLEMENTATION, and that is deliberate. ``levels_at``
    below builds one of these and calls ``at``, so there is exactly one code
    path and no oracle to keep in agreement — the ``s4_exit_levels_batch``
    shape, without the scalar-vs-batch divergence that shape has to test for.

    ⚠ Alignment is VALIDATED, not assumed — matching ``classify_regimes``, which
    raises on mismatched inputs. Misaligned arrays do not fail loudly:
    ``swing_pivots`` reads ``highs[i]`` and ``lows[i]`` as the same bar, so a
    shorter ``lows`` silently pairs each high with the WRONG bar's low and the
    detector returns confident, wrong pivots. Raising rather than returning ()
    because a caller handing in ragged arrays has a bug, and an empty result
    reads as "no levels here".
    """

    highs: npt.NDArray[np.float64]
    lows: npt.NDArray[np.float64]
    volumes: npt.NDArray[np.float64] | None
    pivots: PivotSet
    #: ``nancumsum`` of ``volumes`` — the running denominator of a cluster's
    #: volume share. ⚠ ``nancumsum`` and ``nansum`` agree by construction (both
    #: read a NaN as a zero contribution), so this is the same number the
    #: per-call ``nansum(volumes[: index + 1])`` produced, not an approximation.
    volume_cumsum: npt.NDArray[np.float64] | None
    rule_set_version: str = LEVEL_RULE_VERSION

    @classmethod
    def build(
        cls,
        *,
        highs: npt.NDArray[np.float64],
        lows: npt.NDArray[np.float64],
        volumes: npt.NDArray[np.float64] | None,
    ) -> LevelScan:
        if highs.size != lows.size:
            raise ValueError(f"highs/lows must align: {highs.size} highs against {lows.size} lows")
        if volumes is not None and volumes.size != highs.size:
            raise ValueError(f"volumes must align with prices: {volumes.size} volumes against {highs.size} bars")
        return cls(
            highs=highs,
            lows=lows,
            volumes=volumes,
            pivots=swing_pivots(highs, lows, half_window=PIVOT_HALF_WINDOW),
            volume_cumsum=None if volumes is None else np.nancumsum(volumes),
        )

    def at(self, *, atr: float, index: int) -> tuple[PriceLevel, ...]:
        """Live support/resistance levels as known at bar ``index``.

        ⚠⚠ CAUSAL. Reads bars ``<= index`` and only pivots CONFIRMED by
        ``index`` (see ``PivotSet``). ``atr`` must be the value at ``index`` —
        passing a later ATR would size the clustering tolerance with information
        from after the decision, which is the same leak in a subtler place.
        """
        if index < 0 or index >= self.highs.size:
            return ()
        if not np.isfinite(atr) or atr <= 0:
            return ()
        tolerance = CLUSTER_ATR_TOLERANCE * atr
        last_confirmed = index - self.pivots.half_window
        hi_idx = list(self.pivots.high_indices[: bisect.bisect_right(self.pivots.high_indices, last_confirmed)])
        lo_idx = list(self.pivots.low_indices[: bisect.bisect_right(self.pivots.low_indices, last_confirmed)])

        total = 0.0 if self.volume_cumsum is None else float(self.volume_cumsum[index])
        out: list[PriceLevel] = []
        for kind, idxs, prices in (("resistance", hi_idx, self.highs), ("support", lo_idx, self.lows)):
            for price, cluster in _cluster(idxs, prices, self.volumes, tolerance=tolerance):
                touches = len(cluster)
                last_touch = max(cluster)
                if touches < MIN_TOUCHES:
                    continue
                if index - last_touch > MAX_TOUCH_AGE_BARS:
                    continue
                if self.volumes is None:
                    share = 1.0
                else:
                    share = float(np.nansum([self.volumes[i] for i in cluster])) / total if total > 0 else 0.0
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


def levels_at(
    *,
    highs: npt.NDArray[np.float64],
    lows: npt.NDArray[np.float64],
    volumes: npt.NDArray[np.float64] | None,
    atr: float,
    index: int,
) -> tuple[PriceLevel, ...]:
    """Live support/resistance levels at ONE bar — the convenience form.

    ⚠ Prepares a whole-series ``LevelScan`` per call, so asking for N bars this
    way is N times the preparation. Use ``LevelScan.build`` once and ``at`` per
    bar for anything that walks a series; this form is for a single lookup and
    for the tests that pin the two against each other.
    """
    return LevelScan.build(highs=highs, lows=lows, volumes=volumes).at(atr=atr, index=index)


def nearest_level(levels: tuple[PriceLevel, ...], *, price: float, kind: LevelKind, atr: float) -> PriceLevel | None:
    """The closest live level of ``kind`` within the clustering tolerance."""
    if not np.isfinite(price) or not np.isfinite(atr) or atr <= 0:
        return None
    candidates = [lvl for lvl in levels if lvl.kind == kind and abs(lvl.price - price) <= CLUSTER_ATR_TOLERANCE * atr]
    if not candidates:
        return None
    return min(candidates, key=lambda lvl: abs(lvl.price - price))


__all__ = [
    "CLUSTER_ATR_TOLERANCE",
    "LEVEL_RULE_VERSION",
    "MAX_TOUCH_AGE_BARS",
    "MIN_TOUCHES",
    "PIVOT_HALF_WINDOW",
    "LevelKind",
    "LevelScan",
    "PivotSet",
    "PriceLevel",
    "levels_at",
    "nearest_level",
    "swing_pivots",
]
