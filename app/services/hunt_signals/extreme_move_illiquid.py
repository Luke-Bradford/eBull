"""Hunt 1 (route A): the illiquid third's 5-session relative losers (#3387).

Spec ``docs/proposals/ta/2026-09-26-3387-hunt-1-route-a-spec.md``, "The trial"
(``extreme_loser_reversal_illiquid_v1``). This module scores; the harness selects
(``sign = -1``, ``f = 0.05``), enters at close t+1 (``lag = 1``, the skip day) and holds
``h = 5``.

Scored domain on t, every constant in the spec's ``constants`` mapping:

1. the name's last ``history`` bars sit on the ``history`` most recent sessions ≤ t (no
   gap), with every close and volume finite and > 0, and close_t ≥ ``min_close`` (the
   view's close on t is the as-traded one);
2. of those N names, the most illiquid ⌈N / ``illiquid_divisor``⌉ by Amihud illiquidity,
   the mean of |close_d / close_{d−1} − 1| / (close_d × volume_d) over the
   ``amihud_sessions`` sessions ending t, sorted descending, ties by ``series_id``
   ascending. The rest are unscored, so the control is that illiquid set.

Score: close_t / close_{t−``formation``} − 1. Every ratio here is invariant to the view's
rebase k, and close × volume is too (the view divides volume by k).
"""

from __future__ import annotations

import math

# ``typing.Mapping``, not ``collections.abc``: only ``typing`` is on the signal import allowlist.
from typing import Any, Mapping  # noqa: UP035

from app.services.hunt_view import SignalView

#: The constants a spec must carry, exactly (a missing or extra key is a spec defect).
CONSTANT_KEYS = frozenset({"history", "amihud_sessions", "formation", "min_close", "illiquid_divisor"})


def _positive(value: float) -> bool:
    return math.isfinite(value) and value > 0.0


def _read_constants(constants: Mapping[str, Any]) -> tuple[int, int, int, float, int]:
    if set(constants) != CONSTANT_KEYS:
        raise ValueError(f"constants must be exactly {sorted(CONSTANT_KEYS)}, got {sorted(constants)}")
    history, amihud, formation, divisor = (
        constants[key] for key in ("history", "amihud_sessions", "formation", "illiquid_divisor")
    )
    min_close = constants["min_close"]
    for name, value in (("history", history), ("amihud_sessions", amihud), ("formation", formation)):
        if isinstance(value, bool) or not isinstance(value, int) or value < 1:
            raise ValueError(f"{name} must be a positive int, got {value!r}")
    if isinstance(divisor, bool) or not isinstance(divisor, int) or divisor < 1:
        raise ValueError(f"illiquid_divisor must be a positive int, got {divisor!r}")
    if not (isinstance(min_close, float) and _positive(min_close)):
        raise ValueError(f"min_close must be a positive float, got {min_close!r}")
    if history < max(amihud, formation) + 1:
        raise ValueError(f"history {history} is too short for {amihud} Amihud and {formation} formation returns")
    return history, amihud, formation, min_close, divisor


def score(t: int, view: SignalView, constants: Mapping[str, Any]) -> dict[int, float]:
    """The 5-session return of every name in the illiquid third of the domain on t."""
    history, amihud_sessions, formation, min_close, divisor = _read_constants(constants)
    illiquidity: dict[int, float] = {}
    returns: dict[int, float] = {}
    for series_id, bars in view.series.items():
        if len(bars.ordinals) < history or bars.ordinals[-history] != t - history + 1:
            continue
        closes = bars.close[-history:]
        volumes = bars.volume[-history:]
        if not (all(_positive(c) for c in closes) and all(_positive(v) for v in volumes)):
            continue
        if closes[-1] < min_close:
            continue
        impact = math.fsum(
            abs(closes[i] / closes[i - 1] - 1.0) / (closes[i] * volumes[i])
            for i in range(history - amihud_sessions, history)
        )
        illiquidity[series_id] = impact / amihud_sessions
        returns[series_id] = closes[-1] / closes[-1 - formation] - 1.0
    ordered = sorted(illiquidity, key=lambda series_id: (-illiquidity[series_id], series_id))
    scored = ordered[: -(-len(ordered) // divisor)]
    return {series_id: returns[series_id] for series_id in scored}
