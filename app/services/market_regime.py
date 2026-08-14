"""The market-regime classifier (S-5…S-10 §1). Pure, versioned, no I/O.

Spec: ``docs/proposals/ta/2026-08-14-strategy-set-s5-s10.md`` §1.

WHY THIS EXISTS
---------------
Operator direction 2026-08-14: *"the markets change regularly so one test from 20
years ago is unlikely to behave the same today and there is nothing in place or
even possible to give you any variance factor to get it in line."*

That is correct, and this module is the structural answer rather than a caveat.
S-1…S-4 were each judged on ONE pooled statistic over the whole span — the wrong
instrument for a non-stationary series. Regime becomes an INPUT: a strategy
declares the regimes it may fire in, and firing outside that domain is the defect
rather than evidence the rule is broken.

⚠⚠ BOTH LEGS USE A PUBLISHED FORMULATION. NEITHER IS A PERCENTILE CUT.

*Trend* — close vs the 200-day SMA. The conventional long-horizon trend reference;
not ours to invent, and not tuned here.

*Volatility* — Bollinger **BandWidth** at its lowest / highest reading in **126
trading days (six months)**: the Squeeze and the Bulge, *Bollinger on Bollinger
Bands* ch. 21.

⚠ A 20th/80th-percentile BandWidth cut was INVENTED for this exact purpose on
#2279 and caught at review. It is named here so the next reader does not
re-derive it: the published rule is a six-month EXTREME, not a distributional
quantile, and the two disagree in precisely the quiet-market conditions the
Squeeze exists to identify.

⚠ ``SQUEEZE_LOOKBACK_BARS = 126`` is "six months" in TRADING days. Bollinger
states the rule in months; 126 is the standard trading-day count for six months
and is frozen rather than recomputed per-calendar, so the boundary cannot drift
with holiday schedules.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Final

import numpy as np
import numpy.typing as npt

#: Bollinger's six-month Squeeze/Bulge window, in trading days.
SQUEEZE_LOOKBACK_BARS: Final[int] = 126

#: The conventional long-horizon trend reference.
TREND_SMA_PERIOD: Final[int] = 200

#: Bollinger's own defaults for the band the BandWidth is derived from.
BOLLINGER_PERIOD: Final[int] = 20
BOLLINGER_NUM_STD: Final[float] = 2.0

_RULE_SET_ID: Final[str] = "market-regime-v1"


def _code_hash() -> str:
    """Hash this module's source, per ``indicator_series._code_hash``'s idiom.

    ⚠ Reads the FILE rather than ``inspect.getsource`` — the repo's existing
    convention, and it types cleanly (``inspect.getmodule`` returns
    ``ModuleType | None``). Matching the idiom matters more than the mechanism:
    two hash schemes in one codebase produce version strings that look
    comparable and are not.
    """
    return hashlib.sha256(Path(__file__).read_bytes()).hexdigest()[:12]


#: ⚠ Hashed into every strategy identity that consumes a regime. Moving a
#: boundary is a NEW VERSION, never a redefinition of this one — a strategy's
#: stored track record is only interpretable against the regime rule it ran under.
REGIME_RULE_VERSION: Final[str] = f"{_RULE_SET_ID}+{_code_hash()}"


class Regime(StrEnum):
    """The four states. Deliberately a closed set — an unknown regime must be
    representable as absence (``None``), never as a fifth silent member."""

    BULL_QUIET = "bull_quiet"
    BULL_VOLATILE = "bull_volatile"
    BEAR_QUIET = "bear_quiet"
    BEAR_VOLATILE = "bear_volatile"


@dataclass(frozen=True)
class RegimeSeries:
    """Per-bar regime, aligned to the input bars.

    ``None`` at an index means NOT CLASSIFIABLE — insufficient warmup for either
    leg. ⚠ It does not mean "neutral": a strategy gated on a regime must refuse
    to fire on ``None`` rather than treat it as permissive. That is the same
    fail-closed posture as an unevaluable indicator, and the reason this is
    ``Regime | None`` rather than a fifth enum member.
    """

    values: tuple[Regime | None, ...]
    rule_set_version: str = REGIME_RULE_VERSION

    def __len__(self) -> int:
        return len(self.values)

    def permits(self, index: int, allowed: frozenset[Regime]) -> bool:
        """True only when the bar is classifiable AND inside ``allowed``."""
        if index < 0 or index >= len(self.values):
            return False
        current = self.values[index]
        return current is not None and current in allowed


def bandwidth(
    upper: npt.NDArray[np.float64], middle: npt.NDArray[np.float64], lower: npt.NDArray[np.float64]
) -> npt.NDArray[np.float64]:
    """Bollinger BandWidth: ``(upper - lower) / middle``.

    ⚠ Normalised by the middle band, which is what makes it comparable across
    price levels and across time — the raw band spread is not. A zero or
    non-finite middle yields NaN rather than an exception: a degenerate bar is
    unclassifiable, not fatal.
    """
    with np.errstate(divide="ignore", invalid="ignore"):
        out = (upper - lower) / middle
    out[~np.isfinite(out)] = np.nan
    return out


def _is_extreme_low(window: npt.NDArray[np.float64]) -> bool:
    """True when the LAST value is the minimum of a fully-populated window.

    ⚠ Requires the whole window to be finite. A partially-warmed window would
    make "lowest in six months" mean "lowest in however much data happened to
    exist", which is a different and much weaker claim — and it would fire most
    readily early in a series, exactly where it is least meaningful.
    """
    if window.size < SQUEEZE_LOOKBACK_BARS or not np.all(np.isfinite(window)):
        return False
    return bool(window[-1] <= np.min(window))


def _is_extreme_high(window: npt.NDArray[np.float64]) -> bool:
    if window.size < SQUEEZE_LOOKBACK_BARS or not np.all(np.isfinite(window)):
        return False
    return bool(window[-1] >= np.max(window))


def classify_regimes(
    *,
    closes: npt.NDArray[np.float64],
    band_upper: npt.NDArray[np.float64],
    band_middle: npt.NDArray[np.float64],
    band_lower: npt.NDArray[np.float64],
) -> RegimeSeries:
    """Classify every bar of a benchmark series.

    ⚠⚠ CAUSAL AT EVERY INDEX. Bar ``t`` is classified using bars ``<= t`` only.
    The trend leg reads the SMA computed to ``t``; the volatility leg reads the
    trailing 126-bar BandWidth window ending at ``t``. Nothing here may consult
    ``t+1`` — a regime that peeks is worse than no regime, because every
    strategy gated on it inherits the leak silently.

    ⚠ The BULGE, not the Squeeze, decides `volatile`. The two are independent
    extremes and a bar can be neither; "not in Bulge" is therefore the quiet
    state and covers the ordinary middle of the distribution, which is where
    most bars live. Squeeze is consumed separately by S-9 and is NOT a regime.
    """
    n = closes.size
    if not (band_upper.size == band_middle.size == band_lower.size == n):
        raise ValueError("regime inputs must be aligned to the close series")

    bw = bandwidth(band_upper, band_middle, band_lower)
    sma = _trailing_sma(closes, TREND_SMA_PERIOD)

    out: list[Regime | None] = []
    for i in range(n):
        trend_ref = sma[i]
        if not np.isfinite(trend_ref) or not np.isfinite(closes[i]):
            out.append(None)
            continue
        window = bw[max(0, i - SQUEEZE_LOOKBACK_BARS + 1) : i + 1]
        if window.size < SQUEEZE_LOOKBACK_BARS or not np.all(np.isfinite(window)):
            out.append(None)
            continue
        bullish = bool(closes[i] > trend_ref)
        volatile = _is_extreme_high(window)
        if bullish:
            out.append(Regime.BULL_VOLATILE if volatile else Regime.BULL_QUIET)
        else:
            out.append(Regime.BEAR_VOLATILE if volatile else Regime.BEAR_QUIET)
    return RegimeSeries(values=tuple(out))


def is_squeeze(
    band_upper: npt.NDArray[np.float64],
    band_middle: npt.NDArray[np.float64],
    band_lower: npt.NDArray[np.float64],
    index: int,
) -> bool:
    """Bollinger's Squeeze at ``index``: BandWidth lowest in 126 bars.

    Separate from :func:`classify_regimes` on purpose — the Squeeze is a SETUP
    condition (S-9), not a market state. Folding it into the regime enum would
    force every strategy to reason about a condition only one of them uses.
    """
    bw = bandwidth(band_upper, band_middle, band_lower)
    if index < 0 or index >= bw.size:
        return False
    return _is_extreme_low(bw[max(0, index - SQUEEZE_LOOKBACK_BARS + 1) : index + 1])


def _trailing_sma(values: npt.NDArray[np.float64], period: int) -> npt.NDArray[np.float64]:
    """Simple moving average, NaN until the window is fully populated.

    ⚠ Written here rather than reusing ``indicator_series.sma_series`` because
    that returns an ``IndicatorSeries`` carrying its own ``rule_set_version``,
    and importing it would fold that version into this module's hash — coupling
    every regime identity to unrelated indicator edits. The arithmetic is four
    lines; the coupling would be permanent.
    """
    n = values.size
    out = np.full(n, np.nan)
    if period <= 0:
        raise ValueError("period must be positive")
    if n < period:
        return out
    finite = np.isfinite(values)
    cumsum = np.cumsum(np.where(finite, values, 0.0))
    cumcount = np.cumsum(finite.astype(np.int64))
    for i in range(period - 1, n):
        lo = i - period
        total = cumsum[i] - (cumsum[lo] if lo >= 0 else 0.0)
        count = cumcount[i] - (cumcount[lo] if lo >= 0 else 0)
        if count == period:
            out[i] = total / period
    return out


def unconstrained_regime(n_bars: int, *, regime: Regime = Regime.BULL_QUIET) -> RegimeSeries:
    """A uniform regime for callers that must SUPPLY one but do not GATE on one.

    ⚠⚠ NOT A PRODUCTION SHORTCUT, AND THE NAME IS THE WARNING. S-1…S-4 predate
    the regime and ignore the argument, so a harness exercising them still has to
    pass something; this is that something. Using it on a path that scans a
    regime-GATED strategy (S-5…S-10) would assert a market condition nobody
    measured and let the strategy fire in conditions it declares it avoids.

    The real scan builds its regime from the benchmark via
    ``market_regime_provider.MarketRegimeProvider``. If you are reaching for this
    in ``app/``, you are almost certainly reaching for that instead.
    """
    if n_bars < 0:
        raise ValueError(f"n_bars must be non-negative, got {n_bars}")
    return RegimeSeries(values=(regime,) * n_bars)


__all__ = [
    "BOLLINGER_NUM_STD",
    "BOLLINGER_PERIOD",
    "REGIME_RULE_VERSION",
    "SQUEEZE_LOOKBACK_BARS",
    "TREND_SMA_PERIOD",
    "Regime",
    "RegimeSeries",
    "bandwidth",
    "classify_regimes",
    "unconstrained_regime",
    "is_squeeze",
]
