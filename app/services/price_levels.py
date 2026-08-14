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

import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Final, Literal

import numpy as np
import numpy.typing as npt

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


def _swing_indices(
    highs: npt.NDArray[np.float64],
    lows: npt.NDArray[np.float64],
    *,
    upto: int,
    half_window: int,
) -> tuple[list[int], list[int]]:
    """Confirmed swing highs and lows in bars ``<= upto``.

    ⚠⚠ A PIVOT IS ONLY CONFIRMED ``half_window`` BARS AFTER IT HAPPENS, and this
    function refuses to return unconfirmed ones. The last candidate index is
    ``upto - half_window``, never ``upto``.

    Returning a pivot at ``upto`` would be lookahead of the most seductive kind:
    it reads as "today is a swing high", which cannot be known today — it needs
    the next ``half_window`` bars to fail to exceed it. Every level built from
    such a pivot would be fitted to bars the strategy has not seen, and the
    resulting backtest would be silently, spectacularly optimistic.
    """
    highs_out: list[int] = []
    lows_out: list[int] = []
    last = upto - half_window
    for i in range(half_window, last + 1):
        window_hi = highs[i - half_window : i + half_window + 1]
        window_lo = lows[i - half_window : i + half_window + 1]
        if not np.all(np.isfinite(window_hi)) or not np.all(np.isfinite(window_lo)):
            continue
        if highs[i] >= np.max(window_hi) and np.argmax(window_hi) == half_window:
            highs_out.append(i)
        if lows[i] <= np.min(window_lo) and np.argmin(window_lo) == half_window:
            lows_out.append(i)
    return highs_out, lows_out


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


def levels_at(
    *,
    highs: npt.NDArray[np.float64],
    lows: npt.NDArray[np.float64],
    volumes: npt.NDArray[np.float64] | None,
    atr: float,
    index: int,
) -> tuple[PriceLevel, ...]:
    """Live support/resistance levels as known at bar ``index``.

    ⚠⚠ CAUSAL. Reads bars ``<= index`` and only CONFIRMED pivots (see
    ``_swing_indices``). ``atr`` must be the value at ``index`` — passing a
    later ATR would size the clustering tolerance with information from after
    the decision, which is the same leak in a subtler place.
    """
    if index < 0 or index >= highs.size:
        return ()
    if not np.isfinite(atr) or atr <= 0:
        return ()
    tolerance = CLUSTER_ATR_TOLERANCE * atr

    hi_idx, lo_idx = _swing_indices(highs, lows, upto=index, half_window=PIVOT_HALF_WINDOW)
    out: list[PriceLevel] = []
    for kind, idxs, prices in (("resistance", hi_idx, highs), ("support", lo_idx, lows)):
        for price, cluster in _cluster(idxs, prices, volumes, tolerance=tolerance):
            touches = len(cluster)
            last_touch = max(cluster)
            if touches < MIN_TOUCHES:
                continue
            if index - last_touch > MAX_TOUCH_AGE_BARS:
                continue
            if volumes is None:
                share = 1.0
            else:
                total = float(np.nansum(volumes[: index + 1]))
                share = float(np.nansum([volumes[i] for i in cluster])) / total if total > 0 else 0.0
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
    "PriceLevel",
    "levels_at",
    "nearest_level",
]
