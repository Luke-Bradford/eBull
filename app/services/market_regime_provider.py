"""Load the benchmark once, classify it, align it to any instrument (S-5…S-10 §1).

``market_regime`` is PURE — arrays in, verdicts out, no I/O, hashed into every
strategy identity that consumes it. This module is the impure half: it reads the
benchmark from the database and aligns the result to an instrument's own bar
dates. The split is deliberate, so a regime edit that changes verdicts moves the
hash while a change to *how the benchmark is fetched* does not.

⚠⚠ ALIGNED BY DATE, NEVER BY POSITION. An instrument and the benchmark do not
share a bar count — different listing dates, different halts, different holidays
on a non-US venue. Zipping two arrays positionally would pair an instrument's
2024 bar with the benchmark's 2022 bar and every regime verdict downstream would
be silently wrong in a way no length check catches. The date map is the whole
point of this module.

⚠ A bar with no benchmark bar on that date gets ``None`` — not the previous
regime, and not a default. ``RegimeSeries.permits`` refuses ``None``, so an
instrument trading on a day the benchmark did not simply does not fire. Carrying
the last known regime forward would be a guess presented as an observation, and
it would be most wrong exactly when the market is closed for an unusual reason.
"""

from __future__ import annotations

from datetime import date
from typing import Any, Final

import numpy as np
import psycopg

from app.services.market_regime import (
    BOLLINGER_NUM_STD,
    BOLLINGER_PERIOD,
    Regime,
    RegimeSeries,
    classify_regimes,
)

#: The benchmark the regime is measured on.
#:
#: ⚠⚠ SPY AND SPY.RTH ARE THE SAME FUND and no column separates them — only
#: ``.RTH`` carries a ``real`` arm. Pinned by exact symbol rather than resolved
#: by a LIKE or a prefix match, because a resolver that returns both would pick
#: one arbitrarily and the regime would change identity between runs.
BENCHMARK_SYMBOL: Final[str] = "SPY"


class BenchmarkUnavailableError(RuntimeError):
    """The benchmark series cannot be loaded or is too short to classify.

    ⚠ Raised rather than degrading to "no regime". A scan that silently produced
    an all-``None`` regime would refuse every gated strategy and look exactly
    like a quiet market — a total outage rendering as a legitimate verdict.
    """


def _bollinger(closes: np.ndarray) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """SMA(20) ± 2 POPULATION sigma, matching ``indicator_series.bollinger_series``.

    ⚠ POPULATION sigma (``/n``), not sample (``/n-1``) — inherited from
    ``technical_analysis.bollinger_bands``, whose test asserts ``/ 20``. Using
    the sample form here would put the regime on a slightly different band than
    every other Bollinger consumer in the codebase, and the disagreement would
    only show at the Squeeze/Bulge boundary, which is precisely where it matters.
    """
    n = closes.size
    upper = np.full(n, np.nan)
    middle = np.full(n, np.nan)
    lower = np.full(n, np.nan)
    for i in range(BOLLINGER_PERIOD - 1, n):
        window = closes[i - BOLLINGER_PERIOD + 1 : i + 1]
        if not np.all(np.isfinite(window)):
            continue
        mean = float(window.mean())
        sigma = float(window.std())
        middle[i] = mean
        upper[i] = mean + BOLLINGER_NUM_STD * sigma
        lower[i] = mean - BOLLINGER_NUM_STD * sigma
    return upper, middle, lower


class MarketRegimeProvider:
    """One classification of the benchmark, reusable across a whole scan.

    ⚠ Built ONCE per scan and shared. Re-classifying per instrument would be
    thousands of redundant passes over the same benchmark, and — worse — would
    let two instruments in one cycle disagree about what the market was doing if
    anything about the read changed between them.
    """

    def __init__(self, *, regime_by_date: dict[date, Regime | None]) -> None:
        self._by_date = regime_by_date

    @classmethod
    def load(cls, conn: psycopg.Connection[Any], *, symbol: str = BENCHMARK_SYMBOL) -> MarketRegimeProvider:
        rows = conn.execute(
            """
            SELECT p.price_date, p.close
            FROM price_daily p
            JOIN instruments i ON i.instrument_id = p.instrument_id
            WHERE i.symbol = %s AND p.close IS NOT NULL
            ORDER BY p.price_date
            """,
            (symbol,),
        ).fetchall()
        if not rows:
            raise BenchmarkUnavailableError(f"benchmark {symbol!r} has no priced bars; the regime cannot be classified")
        # ⚠⚠ DUPLICATE DATES ARE A LIVE RISK HERE, NOT A HYPOTHETICAL. This repo
        # already records that SPY and SPY.RTH are the SAME FUND with no column
        # separating them, so a symbol match can return two instrument_ids and
        # two rows per date. Building the map without this check keeps whichever
        # row survives the dict insert — silently, and the corrupted regime is
        # then SHARED by every strategy that gates on it.
        #
        # Raising rather than de-duplicating: picking one of two candidate
        # benchmarks is a decision about which series the regime means, and it
        # belongs to whoever pins the symbol, not to a tie-break here.
        dates = [row[0] for row in rows]
        if len(set(dates)) != len(dates):
            duplicated = sorted({day for day in dates if dates.count(day) > 1})[:5]
            raise BenchmarkUnavailableError(
                f"benchmark {symbol!r} returned multiple rows for the same date "
                f"(first few: {duplicated}); more than one instrument matches this symbol, "
                "so the regime series is ambiguous — pin the instrument_id rather than the symbol"
            )
        closes = np.array([float(row[1]) for row in rows])
        upper, middle, lower = _bollinger(closes)
        series = classify_regimes(closes=closes, band_upper=upper, band_middle=middle, band_lower=lower)
        if all(value is None for value in series.values):
            raise BenchmarkUnavailableError(
                f"benchmark {symbol!r} has {len(rows)} bars but none are classifiable; "
                "the 200-SMA and a full 126-bar BandWidth window need ~326 bars"
            )
        return cls(regime_by_date=dict(zip(dates, series.values, strict=True)))

    def for_dates(self, dates: tuple[date, ...]) -> RegimeSeries:
        """The regime on each of ``dates``, ``None`` where the benchmark has no bar."""
        return RegimeSeries(values=tuple(self._by_date.get(day) for day in dates))

    @property
    def classified_days(self) -> int:
        """How many benchmark days carry a regime — for run reporting."""
        return sum(1 for value in self._by_date.values() if value is not None)


__all__ = [
    "BENCHMARK_SYMBOL",
    "BenchmarkUnavailableError",
    "MarketRegimeProvider",
]
