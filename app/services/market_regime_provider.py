"""Load the benchmark once, classify it, align it to any instrument (S-5…S-10 §1).

``market_regime`` is PURE — arrays in, verdicts out, no I/O, hashed into every
strategy identity that consumes it. This module is the impure half: it reads the
benchmark from the database and aligns the result to an instrument's own bar
dates. The split is deliberate, so a regime edit that changes verdicts moves the
hash while a change to *how the benchmark is fetched* does not.

⚠ One refinement to that split (#2437 walk-forward prerequisite): the fetch
*mechanics* stay outside the hash, but the source *semantics* — which closes
feed the classifier in each context — are identity-bearing, carried by this
module's ``RULE_SET_VERSION`` through ``strategy_registry.INPUT_RULE_SETS``.
``load`` (live scan, ``price_daily``) and ``load_research`` (backtest,
``spy_chain_v1`` over the research corpus) are the two declared sources.

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

import math
from collections.abc import Sequence
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

#: The benchmark SOURCE rules, hashed into every strategy identity via
#: ``strategy_registry.INPUT_RULE_SETS`` (#2333's mechanism).
#:
#: ⚠ A DECLARED STRING, NOT A MODULE HASH — deliberately unlike
#: ``REGIME_RULE_VERSION``. This module's header freezes the design that fetch
#: *mechanics* stay outside the hash; what joins the hash is the source
#: *semantics*: which closes feed the classifier in each context. Changing
#: either source rule (the live table, or any frozen chain constant below) bumps
#: this string by hand; an edit to how the rows are fetched does not.
#:
#: Why the source is identity-bearing at all: switching the backtest benchmark
#: from ``price_daily`` (first SPY bar 2022-05-10) to the research chain flips
#: every pre-2023 bar of a regime-gated strategy from ``not_evaluable`` to a
#: real verdict. Same signals table, same strategy code, different verdicts —
#: the exact "changed input under an unchanged version" defect
#: ``INPUT_RULE_SETS`` exists to make visible.
RULE_SET_VERSION: Final[str] = "benchmark-source-v1:live=price_daily_spy;research=spy_chain_v1"

# --- The research chain (``spy_chain_v1``), all constants frozen together. ---
# Two segments, ONE seam. There is no published formulation for splicing two
# vendor series into a benchmark, so the construction is fixed BY CONSTRUCTION
# (the repo rule for that case) and every constant below is part of
# ``RULE_SET_VERSION``'s ``spy_chain_v1`` — moving any of them is a new chain
# version, never a redefinition.

#: Primary segment: the eToro comparator snapshot. Chosen because it is
#: numerically identical to the live scan's benchmark — ``close IS DISTINCT
#: FROM`` returns 0 rows against ``price_daily`` SPY across all 1,018 shared
#: dates — so the backtest's recent-window regime IS the live regime.
CHAIN_PRIMARY: Final[tuple[str, str]] = ("etoro/etoro-comparators-2026-07-08-v1", "SPY")
#: What the primary's ``adjustment_basis`` must be. Provenance drift under an
#: unchanged vendor name (a re-ingested series on a different basis) is refused,
#: not absorbed.
CHAIN_PRIMARY_BASIS: Final[str] = "split_adjusted"

#: Fallback segment: the Intrader archive's SPY, used strictly BEFORE the seam.
#: ``unadjusted`` vs the primary's ``split_adjusted`` is not a step source: SPY
#: has no split history, which the 585-date overlap agreement (max |diff| $1.76,
#: ~0.3% — vendor close-mark difference) itself evidences.
CHAIN_FALLBACK: Final[tuple[str, str]] = ("icyDenev/Intrader", "SPY")
CHAIN_FALLBACK_BASIS: Final[str] = "unadjusted"

#: ⚠ THE SEAM IS FROZEN, NOT DERIVED. It equals the primary's first bar at
#: freeze time; deriving it from live coverage would let a corpus refresh that
#: extends either series silently move the seam and redefine ``spy_chain_v1``
#: under an unchanged version. ``load_research`` asserts the primary actually
#: has a bar on this date and refuses otherwise.
CHAIN_SEAM: Final[date] = date(2022, 5, 10)

#: The fallback's last bar must fall within this many calendar days before the
#: seam. 7 = the maximum intra-series gap measured on BOTH segments' full
#: population (a holiday week); a larger gap means the fallback segment eroded
#: and the chain would classify across a hole it believes is continuous.
CHAIN_MAX_SEAM_GAP_DAYS: Final[int] = 7


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


def _chain_closes(
    primary: Sequence[tuple[date, float]],
    fallback: Sequence[tuple[date, float]],
    *,
    seam: date = CHAIN_SEAM,
    max_seam_gap_days: int = CHAIN_MAX_SEAM_GAP_DAYS,
) -> list[tuple[date, float]]:
    """``spy_chain_v1``'s merge — pure, so the construction is testable without a DB.

    ⚠ ONE SEAM ONLY. The fallback contributes strictly-before-seam bars and the
    primary contributes on-or-after-seam bars; a hole in the primary after the
    seam STAYS A HOLE even when the fallback has that date. Per-date
    interleaving would flip vendors mid-SMA-window on every vendor hole and
    turn the one declared seam into unbounded micro-seams.

    Refusals (``BenchmarkUnavailableError``) rather than degradation, matching
    ``load``'s contract: a silently short chain would classify a different span
    than ``RULE_SET_VERSION`` declares.
    """
    trimmed_fallback = [(day, close) for day, close in fallback if day < seam]
    trimmed_primary = [(day, close) for day, close in primary if day >= seam]
    if not trimmed_primary or trimmed_primary[0][0] != seam:
        raise BenchmarkUnavailableError(
            f"chain primary has no bar on the frozen seam {seam} — the series no longer "
            "matches the coverage spy_chain_v1 was frozen against"
        )
    if not trimmed_fallback:
        raise BenchmarkUnavailableError(f"chain fallback has no bars before the seam {seam}")
    gap = (seam - trimmed_fallback[-1][0]).days
    if gap > max_seam_gap_days:
        raise BenchmarkUnavailableError(
            f"chain fallback's last bar {trimmed_fallback[-1][0]} is {gap} calendar days before "
            f"the seam {seam} (limit {max_seam_gap_days}) — the fallback segment has eroded"
        )
    chained = trimmed_fallback + trimmed_primary
    for index, (day, close) in enumerate(chained):
        if index and day <= chained[index - 1][0]:
            raise BenchmarkUnavailableError(f"chain dates are not strictly increasing at {day}")
        if not math.isfinite(close) or close <= 0:
            raise BenchmarkUnavailableError(f"chain close on {day} is {close!r} — not a positive finite price")
    return chained


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
        return cls._classify(dates, [float(row[1]) for row in rows], label=repr(symbol))

    @classmethod
    def _classify(cls, dates: Sequence[date], close_values: Sequence[float], *, label: str) -> MarketRegimeProvider:
        """The one classification path, shared by both constructors.

        ⚠ Shared deliberately: if ``load`` and ``load_research`` each owned a
        Bollinger + ``classify_regimes`` call, the two could drift apart and the
        live scan and the backtest would disagree about what the market was
        doing on a date both can classify — the exact property the chain's
        strict-extension check (0 disagreements on all 819 live-classifiable
        dates) exists to preserve.
        """
        closes = np.array(close_values)
        upper, middle, lower = _bollinger(closes)
        series = classify_regimes(closes=closes, band_upper=upper, band_middle=middle, band_lower=lower)
        if all(value is None for value in series.values):
            raise BenchmarkUnavailableError(
                f"benchmark {label} has {len(close_values)} bars but none are classifiable; "
                "the 200-SMA and a full 126-bar BandWidth window need ~326 bars"
            )
        return cls(regime_by_date=dict(zip(dates, series.values, strict=True)))

    @classmethod
    def load_research(cls, conn: psycopg.Connection[Any]) -> MarketRegimeProvider:
        """The BACKTEST's benchmark: ``spy_chain_v1`` over the research corpus.

        The live scan keeps ``load`` (``price_daily``); this constructor exists
        because the backtest's bars come from the research corpus and its axis
        reaches decades before ``price_daily``'s first SPY bar (2022-05-10) —
        the benchmark must come from the same source universe as the bars it
        contextualises (`backtest_run._signals_for`'s own prescription).

        ⚠ Quarantine treatment, declared: reads ``research_price_daily.close``
        without consulting ``research_bar_quarantine``. The regime is market
        context, not a return computation, so the quarantine's ``return_usable``
        / ``range_usable`` verdicts do not define "close usable". Verified on
        the full population at freeze time: the only flagged rows on either
        pinned series are 5 archive-tail ``provisional`` marks on the fallback
        (2024-09-23→27, empty rule list, both flags true) — all AFTER the seam,
        so the chain never reads them.

        ⚠ Both segment reads happen on the one connection passed in, inside its
        current transaction, so the two queries observe one corpus snapshot.
        """
        segments: dict[str, list[tuple[date, float]]] = {}
        for role, (vendor, vendor_symbol), expected_basis in (
            ("primary", CHAIN_PRIMARY, CHAIN_PRIMARY_BASIS),
            ("fallback", CHAIN_FALLBACK, CHAIN_FALLBACK_BASIS),
        ):
            series_rows = conn.execute(
                """
                SELECT series_id, adjustment_basis
                FROM research_price_series
                WHERE vendor = %s AND vendor_symbol = %s
                """,
                (vendor, vendor_symbol),
            ).fetchall()
            # ``(vendor, vendor_symbol)`` is DB-unique, so >1 cannot happen today;
            # kept because this refusal message beats a downstream shape error.
            if len(series_rows) != 1:
                raise BenchmarkUnavailableError(
                    f"chain {role} pin {(vendor, vendor_symbol)!r} resolved to {len(series_rows)} series; "
                    "spy_chain_v1 requires exactly one"
                )
            series_id, basis = series_rows[0]
            if basis != expected_basis:
                raise BenchmarkUnavailableError(
                    f"chain {role} {(vendor, vendor_symbol)!r} has adjustment_basis {basis!r}, "
                    f"expected {expected_basis!r} — provenance drifted under an unchanged vendor name"
                )
            bar_rows = conn.execute(
                """
                SELECT bar_date, close
                FROM research_price_daily
                WHERE series_id = %s AND close IS NOT NULL
                ORDER BY bar_date
                """,
                (series_id,),
            ).fetchall()
            segments[role] = [(row[0], float(row[1])) for row in bar_rows]
        chained = _chain_closes(segments["primary"], segments["fallback"])
        return cls._classify([day for day, _ in chained], [close for _, close in chained], label="spy_chain_v1")

    def for_dates(self, dates: tuple[date, ...]) -> RegimeSeries:
        """The regime on each of ``dates``, ``None`` where the benchmark has no bar.

        ⚠⚠ THE TWO KINDS OF ``None`` ARE SEPARATED HERE, AND THIS IS THE ONLY
        PLACE THAT CAN SEPARATE THEM. ``dict.get`` returns ``None`` both for a
        date the benchmark never traded and for a date it traded but could not
        yet be classified — and only this map knows which. By the time a
        strategy sees the ``RegimeSeries`` the distinction is gone.

        Membership in ``self._by_date`` is the discriminator: present means the
        benchmark contributed an observation (warm-up if its value is ``None``),
        absent means it contributed nothing (``not_evaluable``).

        ⚠ ``in`` rather than ``get() is None`` — a benchmark bar that classified
        to ``None`` is IN the map, so the two tests disagree exactly on the
        population this method exists to split.
        """
        values = tuple(self._by_date.get(day) for day in dates)
        unobserved = tuple(index for index, day in enumerate(dates) if day not in self._by_date)
        return RegimeSeries(values=values, not_evaluable_indices=unobserved)

    @property
    def classified_days(self) -> int:
        """How many benchmark days carry a regime — for run reporting."""
        return sum(1 for value in self._by_date.values() if value is not None)


__all__ = [
    "BENCHMARK_SYMBOL",
    "CHAIN_FALLBACK",
    "CHAIN_FALLBACK_BASIS",
    "CHAIN_MAX_SEAM_GAP_DAYS",
    "CHAIN_PRIMARY",
    "CHAIN_PRIMARY_BASIS",
    "CHAIN_SEAM",
    "RULE_SET_VERSION",
    "BenchmarkUnavailableError",
    "MarketRegimeProvider",
]
