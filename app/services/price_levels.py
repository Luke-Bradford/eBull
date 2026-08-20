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
#: ``tests/test_price_level_scan.py`` rather than trusted from the NumPy source.
_PAIRWISE_SUMMATION_BLOCK = 8


@dataclass(frozen=True)
class _ClusterSegments:
    """Price-sorted pivots cut into single-linkage clusters, before any filter.

    ⚠⚠ THE FILTERS RUN ON THESE ARRAYS, NOT ON MATERIALISED CLUSTERS (#2780).
    ``at`` discards the overwhelming majority of what clustering produces:
    measured over one 300-series ``s5-support-bounce`` profile, 30,756,116
    clusters reached ``len``/``max`` and 1,822,327 survived ``MIN_TOUCHES`` and
    ``MAX_TOUCH_AGE_BARS`` — 94.1% built to be thrown away. Reproduce with::

        uv run python -m cProfile -o /tmp/s5.prof -m scripts.verify_2697_metric_axis_ab \
            --limit 300 --strategy s5-support-bounce

    and read the caller counts of ``builtins.max`` and ``nansum`` against
    ``price_levels.at``.

    The list was built to answer two questions — how many members, and which is
    the latest bar — and ``PriceLevel`` stores neither. Both are ``sizes`` and a
    ``maximum.reduceat`` over ``order``, which the vectorised pass has already
    computed, so the Python list is pure cost.
    """

    #: Pivot indices in stable price order, laid out cluster by cluster.
    order: npt.NDArray[np.int64]
    #: ``order`` offsets: cluster ``g`` is ``order[bounds[g] : bounds[g + 1]]``.
    bounds: npt.NDArray[np.int64]
    #: Members per cluster. This IS ``touches``; no list needed to count it.
    sizes: npt.NDArray[np.int64]
    ordered_prices: npt.NDArray[np.float64]
    ordered_weights: npt.NDArray[np.float64] | None
    #: ``bincount``-derived cluster means. ⚠ VALID ONLY BELOW THE PAIRWISE
    #: BOUNDARY — see ``price``, which is the only thing allowed to read it.
    fast_prices: npt.NDArray[np.float64]

    def price(self, gid: int) -> float:
        """One cluster's level price, on exactly the pre-#2780 arithmetic.

        ⚠⚠ THE PAIRWISE HAND-BACK LIVES HERE AND NOWHERE ELSE. NumPy's reduction
        switches from sequential to pairwise summation at
        ``_PAIRWISE_SUMMATION_BLOCK`` elements, so ``bincount``'s sequential
        accumulation parts from ``np.average`` in the last bits at that size
        (7.1e-15 measured on the #2780 prototype). A level price feeds a
        threshold comparison, so a last-bit change flips which trades exist.
        """
        lo, hi = int(self.bounds[gid]), int(self.bounds[gid + 1])
        if hi - lo < _PAIRWISE_SUMMATION_BLOCK:
            return float(self.fast_prices[gid])
        if self.ordered_weights is None:
            return float(np.mean(self.ordered_prices[lo:hi]))
        weights = self.ordered_weights[lo:hi]
        values = self.ordered_prices[lo:hi]
        return float(np.average(values, weights=weights)) if weights.sum() > 0 else float(values.mean())

    def last_touch(self) -> npt.NDArray[np.int64]:
        """``max(cluster)`` for every cluster at once.

        ⚠ ``reduceat`` is exact here rather than approximately so: ``order`` is
        int64, and integer maximum has no accumulation order to get wrong. Every
        cluster has at least one member (``bincount`` emits no empty group), so
        the degenerate ``reduceat`` case where a segment is empty cannot arise.
        """
        return np.maximum.reduceat(self.order, self.bounds[:-1])


def _segment(
    idx: npt.NDArray[np.int64],
    prices: npt.NDArray[np.float64],
    volumes: npt.NDArray[np.float64] | None,
    *,
    tolerance: float,
) -> _ClusterSegments | None:
    """Single-linkage clustering, vectorised, stopping short of materialising.

    Group pivots whose prices lie within ``tolerance`` of each other: walk in
    price order and start a new cluster whenever the gap to the previous pivot
    exceeds the tolerance.

    ⚠ Single-linkage can chain — a dense ladder of pivots each within tolerance
    of the next merges into one wide cluster. Accepted deliberately: the
    alternative (a fixed number of clusters, or a centroid method) needs a
    parameter that IS fitted, and a chained cluster in a tight range is arguably
    one level. The consequence is that a level's price is a volume-weighted mean
    that may sit where no pivot actually is; ``touches`` remains honest.

    ⚠⚠ BIT-IDENTICAL TO THE PYTHON LOOP IT REPLACED BY CONSTRUCTION (#2780):
    ``kind="stable"`` reproduces ``sorted(..., key=...)``, so equal prices keep
    their original index order and cluster membership AND ITS ORDER are
    unchanged — ``at`` sums volumes over that order, and float addition is not
    commutative. The walk compared each pivot with the last member appended,
    which in price order is simply its predecessor, so ``diff`` is the same
    test; ``> tolerance`` starts a cluster exactly where ``<= tolerance`` failed
    to continue one, and ``abs`` was redundant on an ascending sort.
    ``bincount`` accumulates in that same sorted order.

    Returns ``None`` for an empty pivot set rather than empty arrays, so callers
    branch once instead of guarding every array op against size zero.

    ⚠ TAKES AN ARRAY, NOT A LIST, and that is a measured choice (#2780). ``at``
    holds its confirmed-pivot prefix as an ndarray slice — a view, costing
    nothing — whereas the previous shape built a Python list of the whole prefix
    on every bar and handed it straight back to ``np.asarray``, which the
    profile counted 918,324 times over 300 series.
    """
    if idx.size == 0:
        return None

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

    return _ClusterSegments(
        order=order,
        bounds=bounds,
        sizes=sizes,
        ordered_prices=ordered_prices,
        ordered_weights=ordered_weights,
        fast_prices=fast_prices,
    )


def _cluster(
    indices: list[int],
    prices: npt.NDArray[np.float64],
    volumes: npt.NDArray[np.float64] | None,
    *,
    tolerance: float,
) -> list[tuple[float, list[int]]]:
    """Every cluster, membership included — the declarative form of a cluster.

    ⚠ ``at`` DELIBERATELY DOES NOT CALL THIS, and that is the #2780 point rather
    than an oversight: materialising the member lists is the cost, and ``at``
    needs them for nothing. This is the reference shape — the scalar side of the
    same scalar-vs-batch pairing ``strategy_exit_levels_batch`` uses — kept so
    ``tests/test_price_level_scan.py`` can pin the arithmetic against a
    hand-transcribed oracle, and so "what is a cluster" has one readable answer.
    """
    segments = _segment(np.asarray(indices, dtype=np.int64), prices, volumes, tolerance=tolerance)
    if segments is None:
        return []
    out: list[tuple[float, list[int]]] = []
    for gid in range(int(segments.sizes.size)):
        lo, hi = int(segments.bounds[gid]), int(segments.bounds[gid + 1])
        out.append((segments.price(gid), [int(i) for i in segments.order[lo:hi]]))
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

    * **3.61 ms/bar** — the replaced form, timed over SPY's 844 evaluable indices,
      against **187 minutes** for a 3,107,697-bar walk at that rate.

    ⚠ THE per-bar FIGURE FOR THIS FORM IS NOT WRITTEN HERE, deliberately. It has
    already moved twice (#2437's hoist, then #2780's filter hoist), and a
    hand-copied statistic goes stale silently in the place a reader trusts most.
    ``--census`` computes it at run time over the validated universe::

        uv run python -m scripts.verify_2437_level_scan --census

    ⚠⚠ THERE ARE NOW TWO IMPLEMENTATIONS AND THAT IS THE POINT (#2780). ``at``
    filters on ``_segment``'s arrays and never materialises a cluster;
    ``_cluster`` materialises every one and is what the tests pin against a
    hand-transcribed oracle. That is exactly the ``s4_exit_levels_batch``
    scalar-vs-batch shape, and it carries the same obligation: the divergence
    has to be tested for, not argued away. ⚠ ``levels_at`` is NOT that oracle —
    it builds one of these and calls ``at``, so it shares the code path and
    cannot disagree. ``--equivalence`` compares against ``_reference_at``
    instead, for that reason.

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
    #: ``pivots.high_indices`` / ``low_indices`` as int64 arrays, so ``at`` can
    #: take a confirmed-pivot prefix as a VIEW instead of rebuilding a Python
    #: list of it on every bar. ⚠ Same values in the same ascending order — the
    #: tuples remain the declared form and these are a representation of them,
    #: not a second source.
    high_index_array: npt.NDArray[np.int64]
    low_index_array: npt.NDArray[np.int64]
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
        pivots = swing_pivots(highs, lows, half_window=PIVOT_HALF_WINDOW)
        return cls(
            highs=highs,
            lows=lows,
            volumes=volumes,
            pivots=pivots,
            high_index_array=np.asarray(pivots.high_indices, dtype=np.int64),
            low_index_array=np.asarray(pivots.low_indices, dtype=np.int64),
            volume_cumsum=None if volumes is None else np.nancumsum(volumes),
        )

    def at(self, *, atr: float, index: int) -> tuple[PriceLevel, ...]:
        """Live support/resistance levels as known at bar ``index``.

        ⚠⚠ CAUSAL. Reads bars ``<= index`` and only pivots CONFIRMED by
        ``index`` (see ``PivotSet``). ``atr`` must be the value at ``index`` —
        passing a later ATR would size the clustering tolerance with information
        from after the decision, which is the same leak in a subtler place.

        ⚠⚠ THE TWO FILTERS RUN VECTORISED, BEFORE ANY CLUSTER IS MATERIALISED
        (#2780). ``touches`` is a cluster's size and ``last_touch`` is the
        largest bar index in it, so both are answerable from ``_segment``'s
        arrays; the pre-#2780 form built a Python list of ints per cluster to
        ask them and then discarded 94.1% of the answers. The surviving
        clusters take exactly the arithmetic they took before — ``price`` is
        unchanged, and the volume share is still one ``nansum`` per level over
        the same members in the same order, because float addition is not
        commutative and this feeds ``strength``.
        """
        if index < 0 or index >= self.highs.size:
            return ()
        if not np.isfinite(atr) or atr <= 0:
            return ()
        tolerance = CLUSTER_ATR_TOLERANCE * atr
        last_confirmed = index - self.pivots.half_window
        # ⚠ Views over the prefix, not copies. ``bisect`` still reads the tuples
        # because it is C-speed on them and the arrays hold the same ascending
        # values, so the cut index is the same one.
        hi_idx = self.high_index_array[: bisect.bisect_right(self.pivots.high_indices, last_confirmed)]
        lo_idx = self.low_index_array[: bisect.bisect_right(self.pivots.low_indices, last_confirmed)]

        total = 0.0 if self.volume_cumsum is None else float(self.volume_cumsum[index])
        out: list[PriceLevel] = []
        for kind, idxs, prices in (("resistance", hi_idx, self.highs), ("support", lo_idx, self.lows)):
            segments = _segment(idxs, prices, self.volumes, tolerance=tolerance)
            if segments is None:
                continue
            last_touches = segments.last_touch()
            # ⚠ ``flatnonzero`` is ascending, so the surviving clusters are
            # appended in the same order the per-cluster loop appended them.
            # ``sorted`` below is stable, so equal strengths keep that order and
            # the returned tuple is unchanged.
            live = np.flatnonzero((segments.sizes >= MIN_TOUCHES) & (index - last_touches <= MAX_TOUCH_AGE_BARS))
            for raw_gid in live:
                gid = int(raw_gid)
                touches = int(segments.sizes[gid])
                last_touch = int(last_touches[gid])
                if self.volumes is None:
                    share = 1.0
                elif total > 0:
                    lo, hi = int(segments.bounds[gid]), int(segments.bounds[gid + 1])
                    share = float(np.nansum(self.volumes[segments.order[lo:hi]])) / total
                else:
                    share = 0.0
                strength = touches * float(np.log1p(share))
                out.append(
                    PriceLevel(
                        price=segments.price(gid),
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
