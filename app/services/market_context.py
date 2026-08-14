"""The benchmark's regime, per DATE, as every regime-gated strategy reads it.

Spec: ``docs/proposals/ta/2026-08-14-strategy-set-s5-s10.md`` §1. Refs #2437.

WHY THIS IS DATE-KEYED AND NOT INDEX-ALIGNED
--------------------------------------------
The regime is a property of the MARKET on a day, not of the instrument being
judged. Every S-5…S-10 strategy reads the same classification, computed once on
one benchmark series, and each instrument then has to find its own bars in it.

Bar INDEX cannot carry that. Index ``i`` is a different date on every
instrument, so an index-aligned regime array would silently pair a 2026 bar of
one name with a 2023 bar of another — the identical argument
``CrossSectionalMember`` already makes for its own ranking key (*"Grouping on
the bar INDEX would rank a 2019 bar against a 2007 one"*). It also survives
``strategy_segmented_evaluation``, which slices a series into price-scale
segments: a date key needs no re-basing, an index key needs re-basing at every
segment boundary and is wrong the first time somebody forgets.

⚠⚠ THE THREE STATES ARE DISTINCT AND STAY DISTINCT.

1. **Classified** — the benchmark has a session and both legs are warm. The gate
   is a real ``True``/``False`` and the strategy decides.
2. **Before the benchmark's first classifiable bar** — the 200-SMA or the
   126-bar BandWidth window is still filling. That is warm-up, and
   ``gate_series`` leaves a bare ``None`` so the registry reports
   ``insufficient_warmup``.
3. **After it, with no session** — a hole in the benchmark series. That is a
   DATA GAP in a series the strategy does not own, and it is reported as
   ``missing_market_context`` (the registry's eleventh code).

Measured on the validated universe before the code was written, because the
whole point of the split is that the third class is not empty: of 3,650,325
loadable bars, 85.26% are state 1, 14.48% state 2, and **9,405 bars over 353
dates** are state 3 — worst single date 2026-02-06, on which 1,735 instruments
traded and SPY has no bar. Collapsing 3 into 2 would report a permanent hole as
a series that had not started yet.

⚠ THE BENCHMARK IS PINNED BY SYMBOL AND ITS FRESHNESS IS SOMEBODY'S JOB.
Prevention log, #1818: *"when a read path pins a specific symbol/entity, that
symbol must appear in … the ingest scope that maintains its data — and a test
should pin the cross-reference"*. SPX500 froze for weeks precisely because no
refresh scope named it. ``BENCHMARK_SYMBOL`` is in
``scheduler.BENCHMARK_SYMBOLS`` and ``tests/test_strategy_s6.py`` binds the two.

⚠ SPY AND SPY.RTH ARE THE SAME FUND and only ``.RTH`` carries a ``real`` arm, so
the spec requires the series to be pinned rather than resolved loosely. The
lookup below is exact symbol equality and refuses anything but a unique match —
a ``LIKE`` or a canonical-redirect hop is what would land on the wrong one.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from typing import Any, Final

import numpy as np
import psycopg

from app.services.indicator_series import BarSeries, IndicatorSeries, Universe
from app.services.market_regime import (
    BOLLINGER_NUM_STD,
    BOLLINGER_PERIOD,
    REGIME_RULE_VERSION,
    Regime,
    classify_regimes,
)

#: ⚠ Pinned, and it is part of every regime-gated strategy's identity — a
#: strategy gated on QQQ's regime is a different strategy from one gated on
#: SPY's. Strategy modules hash this into their params rather than assuming it.
BENCHMARK_SYMBOL: Final[str] = "SPY"


class MarketContextUnavailable(RuntimeError):
    """The benchmark could not be resolved or classified at all.

    ⚠ Raised rather than returning an empty context, because an empty context is
    indistinguishable from "the market was never classifiable", and a strategy
    handed one would refuse every bar of every instrument while reporting a
    perfectly ordinary reason code. A scan that cannot build its benchmark has a
    configuration problem, and it should say so where the operator will see it.
    """


@dataclass(frozen=True)
class MarketContext:
    """One benchmark's classified regime, keyed by session date."""

    #: Only CLASSIFIABLE sessions appear. A benchmark bar whose regime is
    #: ``None`` is absent from this map, which is why ``first_classified`` is
    #: carried separately — the map alone cannot tell warm-up from a hole.
    regime_by_date: Mapping[date, Regime]
    #: The earliest classified session. Everything before it is benchmark
    #: warm-up; everything after it and absent is a gap.
    first_classified: date
    benchmark_instrument_id: int
    benchmark_symbol: str
    rule_set_version: str = REGIME_RULE_VERSION

    def gate_series(
        self,
        dates: Sequence[date],
        *,
        allowed: frozenset[Regime],
        universe: Universe,
    ) -> IndicatorSeries:
        """Per-bar permission for one instrument, in the shape ``evaluate`` checks.

        ``1.0`` permitted, ``0.0`` refused by the rule, ``None`` undecidable.

        ⚠ PERMISSION, NOT THE REGIME ITSELF. Encoding which of the four states a
        bar is in would put a categorical value in a ``float`` series and invite
        a caller to compare it to a number. Every strategy reads the same
        question — *may I fire here* — so that is the question the series
        answers, and ``allowed`` is what makes it per-strategy.

        ⚠ A refused regime is ``0.0``, NOT ``None``. Firing outside a strategy's
        declared domain is the defect (spec §0 rule 2), so a bar in
        ``bear_quiet`` is a bar the strategy CONSIDERED and declined — a real
        ``not_fired`` verdict. Reporting it as unevaluable would delete the
        denominator that shows the regime gate doing its job.
        """
        if not allowed:
            raise ValueError("a regime gate with no permitted regimes can never fire; declare at least one")
        values: list[float | None] = []
        unevaluable: list[int] = []
        for index, when in enumerate(dates):
            regime = self.regime_by_date.get(when)
            if regime is not None:
                values.append(1.0 if regime in allowed else 0.0)
                continue
            values.append(None)
            # ⚠ Bare `None` BEFORE the first classified session (the registry
            # reads that as warm-up); a listed refusal at or after it, because
            # by then the benchmark has started and its absence is a hole.
            if when >= self.first_classified:
                unevaluable.append(index)
        return IndicatorSeries(tuple(values), universe, tuple(unevaluable))


def classify_benchmark(series: BarSeries, *, universe: Universe) -> dict[date, Regime]:
    """Classify one benchmark series into ``{date: regime}``, dropping the rest.

    Split out from the loader so the classification can be exercised without a
    database, and so a caller measuring the corpus uses the same code the scan
    does rather than a second copy of the band plumbing.
    """
    from app.services.indicator_series import bollinger_series

    bands = bollinger_series(series, universe=universe, period=BOLLINGER_PERIOD, num_std=BOLLINGER_NUM_STD)

    def band(name: str) -> np.ndarray:
        return np.array([np.nan if value is None else value for value in bands.components[name]], dtype=float)

    regimes = classify_regimes(
        closes=series.array_closes,
        band_upper=band("upper"),
        band_middle=band("middle"),
        band_lower=band("lower"),
    )
    return {when: regime for when, regime in zip(series.dates, regimes.values, strict=True) if regime is not None}


def load_market_context(
    conn: psycopg.Connection[Any],
    *,
    universe: Universe,
    symbol: str = BENCHMARK_SYMBOL,
) -> MarketContext:
    """Resolve the benchmark, load its masked bars, and classify every session.

    ⚠ Goes through ``load_masked_bars`` like every other series, not through a
    bespoke query. The benchmark is subject to the same quarantine and the same
    fail-closed coverage rule as the instruments it gates: a benchmark loaded by
    a laxer path would let a bar the corpus rejected decide whether 6,774 other
    instruments may trade.
    """
    from app.services.price_masked_bars import load_masked_bars

    rows = conn.execute(
        "SELECT instrument_id FROM instruments WHERE symbol = %(symbol)s ORDER BY instrument_id",
        {"symbol": symbol},
    ).fetchall()
    if len(rows) != 1:
        raise MarketContextUnavailable(
            f"benchmark symbol {symbol!r} resolves to {len(rows)} instruments, expected exactly 1 — "
            "the regime series must be pinned, not chosen (SPY and SPY.RTH are the same fund)"
        )
    instrument_id = int(rows[0][0])
    series = load_masked_bars(conn, instrument_id).series
    by_date = classify_benchmark(series, universe=universe)
    if not by_date:
        raise MarketContextUnavailable(
            f"benchmark {symbol!r} (instrument {instrument_id}) has {len(series)} loadable bars and not one "
            f"classifiable session — every regime-gated strategy would refuse every bar"
        )
    return MarketContext(
        regime_by_date=by_date,
        first_classified=min(by_date),
        benchmark_instrument_id=instrument_id,
        benchmark_symbol=symbol,
    )


__all__ = [
    "BENCHMARK_SYMBOL",
    "MarketContext",
    "MarketContextUnavailable",
    "classify_benchmark",
    "load_market_context",
]
