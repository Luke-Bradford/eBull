"""Ledger measurements for #2505's promotion evidence — slice 1 of #3104.

#2505 shipped the refusal contract and the immutable store; nothing produces a
record. This module produces the part of it that is pure arithmetic over the
realised-trade ledger, and nothing else: no verdicts, no costs, no challengers.

⚠ IT HAS NO CALLER YET, AND THAT IS THE SLICE BOUNDARY. Slice 2 calls it from
``backtest_run._measure_namespace`` and carries the result on
``NamespaceMeasurement`` — the same way ``regime_cohorts`` and
``termination_census`` already outlive the book. It cannot run later than that:
``_measure_namespace`` returns an aggregate and the book is discarded before any
row is written, so "compute it at write time" is not implementable.

⚠ NOTHING HERE IS CLAMPED OR DEFAULTED. ``PromotionEvidence`` refuses several of
these values (a positive tail mean, for one); the refusal is the contract's job
and is not this module's to pre-empt. "Missing is not zero" is the contract's own
rule and it applies to measurement too.

Design: ``docs/proposals/ta/2026-09-16-promotion-evidence-producer.md``.
"""

from __future__ import annotations

import math
from collections import Counter
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Final

import numpy as np

#: Frozen because the estimators below are choices, and a stored record must be
#: attributable to the rule that produced it. ⚠ Bump on a RULE change, never on
#: a comment. The version is returned on the measurement rather than written to
#: a column today; carrying it into the stored payload is an open item on the
#: design doc, because ``PromotionEvidence`` has nowhere to put it.
LEDGER_MEASUREMENT_RULE_VERSION: Final = "promotion-evidence-ledger-measure-2026-09-16-v1"

#: Acerbi & Tasche's alpha for the α-tail average. #2505's field is the 5% one.
EXPECTED_SHORTFALL_ALPHA: Final = 0.05

#: The trim for "expectancy excluding the best 1%". Expressed as a PERCENTILE
#: rather than a count because that is the only in-repo formulation of this
#: statistic (``scripts/verify_2437_short_stops.py:163``).
BEST_TRIM_PERCENTILE: Final = 99.0


@dataclass(frozen=True)
class RealisedLedger:
    """One namespace's realised legs, plus the open legs still in exposure.

    ⚠⚠ TWO POPULATIONS, DELIBERATELY DIFFERENT SIZES. This is the split
    ``TradeReturns`` already documents (``strategy_statistics.py:84``): the
    trade-level statistics take the realised legs, and anything describing
    EXPOSURE must also see the legs open at the window end. Concurrency is the
    second kind — counting realised legs alone understates it, which is the
    direction that flatters a candidate.

    ``name_key`` is the book's key and NOT always an instrument id: the
    survivorship-free path uses ``-series_id`` for a series admitted without a
    live link (#2721 step 3, ``backtest_run.py:1540``). Concentration only needs
    identity, so that is harmless here — but a later slice joining this key to a
    sector must say what it does with a negative one.
    """

    #: Net return per realised leg, in percent, positionally parallel to the
    #: three tuples below.
    net_return_pct: tuple[float, ...]
    entry_fill_date: tuple[date, ...]
    exit_bar_date: tuple[date, ...]
    name_key: tuple[int, ...]
    #: ``(entry_fill_date, window_end)`` for each leg still open at the end.
    #: An open leg never closes inside the window, so it contributes an opening
    #: event and no closing one.
    open_legs: tuple[tuple[date, date], ...] = ()

    def __post_init__(self) -> None:
        count = len(self.net_return_pct)
        if count == 0:
            # ⚠ REFUSED, NOT ZEROED. Every statistic below is undefined on an
            # empty population, and an invented zero is exactly what the
            # contract's "missing is not zero and does not reduce a score" rule
            # exists to prevent.
            raise ValueError("a realised ledger with no legs carries no measurement")
        for name in ("entry_fill_date", "exit_bar_date", "name_key"):
            if len(getattr(self, name)) != count:
                raise ValueError(
                    f"{name} carries {len(getattr(self, name))} entries against {count} returns — "
                    "the realised columns are positionally parallel"
                )
        if any(not math.isfinite(value) for value in self.net_return_pct):
            raise ValueError("every realised return must be finite")
        for entry, exit_bar in zip(self.entry_fill_date, self.exit_bar_date, strict=True):
            if exit_bar < entry:
                raise ValueError(f"a leg entered {entry} cannot close {exit_bar}")
        for entry, window_end in self.open_legs:
            if window_end < entry:
                raise ValueError(f"an open leg entered {entry} cannot be marked at {window_end}")


@dataclass(frozen=True)
class LedgerMeasurements:
    """The #2505 fields that are arithmetic over the ledger, and only those."""

    rule_version: str
    outcome_count: int
    profitable_outcome_count: int
    losing_outcome_count: int
    flat_outcome_count: int
    #: The α-tail MEAN in return units — the negative of Acerbi & Tasche's ES,
    #: because #2505 reports tail losses as non-positive percentages.
    expected_shortfall_5_pct: Decimal
    excluding_best_1_expectancy_pct: Decimal
    #: How many legs the percentile trim actually dropped. ⚠ A threshold trim is
    #: not a fixed-count trim: under ties it removes more or fewer than 1%, so
    #: the number is reported rather than assumed to be ``ceil(0.01 n)``.
    excluded_best_count: int
    max_date_contribution_pct: Decimal
    max_name_contribution_pct: Decimal
    max_concurrency: int


def _alpha_tail_mean(sorted_ascending: np.ndarray, *, alpha: float) -> float:
    """Acerbi & Tasche (2002) §4's α-tail average, exactly.

    *On the coherence of expected shortfall*, JBF 26(7). With ``k = floor(αn)``
    the estimator is ``(sum of the k smallest + (αn - k) · x_(k+1)) / (αn)`` —
    the boundary observation carries a FRACTIONAL weight.

    ⚠ ``ceil(αn)`` is a different estimator and overstates the tail: at ``n=41``
    it averages three observations, which is 7.32% of the population and not 5%.

    ⚠ For ``αn < 1`` the formula reduces to the single worst observation, which
    is the honest answer at that sample size rather than a degenerate one.
    """
    n = int(sorted_ascending.size)
    alpha_n = alpha * n
    k = int(math.floor(alpha_n))
    head = float(sorted_ascending[:k].sum()) if k else 0.0
    # k < n for every alpha < 1, so the boundary observation always exists.
    boundary = float(sorted_ascending[k])
    return (head + (alpha_n - k) * boundary) / alpha_n


def measure_ledger(ledger: RealisedLedger) -> LedgerMeasurements:
    """Measure one realised ledger. Pure; reads no database."""
    returns = np.asarray(ledger.net_return_pct, dtype=float)
    count = int(returns.size)
    ordered = np.sort(returns)

    # The percentile trim, and the count it actually removed.
    threshold = float(np.percentile(returns, BEST_TRIM_PERCENTILE))
    kept = returns[returns <= threshold]
    if kept.size == 0:  # pragma: no cover - a percentile is always >= the minimum
        raise RuntimeError("the best-1% trim removed the whole population")

    date_counts = Counter(ledger.entry_fill_date)
    name_counts = Counter(ledger.name_key)

    return LedgerMeasurements(
        rule_version=LEDGER_MEASUREMENT_RULE_VERSION,
        outcome_count=count,
        profitable_outcome_count=int(np.count_nonzero(returns > 0.0)),
        losing_outcome_count=int(np.count_nonzero(returns < 0.0)),
        flat_outcome_count=int(np.count_nonzero(returns == 0.0)),
        expected_shortfall_5_pct=_decimal(_alpha_tail_mean(ordered, alpha=EXPECTED_SHORTFALL_ALPHA)),
        excluding_best_1_expectancy_pct=_decimal(float(kept.mean())),
        excluded_best_count=count - int(kept.size),
        max_date_contribution_pct=_share_pct(max(date_counts.values()), count),
        max_name_contribution_pct=_share_pct(max(name_counts.values()), count),
        max_concurrency=_max_concurrency(ledger),
    )


#: Within one date, the order in which boundary events are applied. Two
#: DIFFERENT rules meet here and neither may swallow the other:
#:
#: ⚠ ``_CLOSE_EARLIER`` before ``_OPEN`` is spec §3.5 rule 4 — *"same-bar
#: ordering is exit before entry"* (``position_builder.py:661``) — so a leg
#: closing on a date and a DIFFERENT leg opening on it are not concurrent.
#:
#: ⚠⚠ ``_CLOSE_SAME_DAY`` comes after ``_OPEN``, because a leg that opens and
#: closes on one bar WAS held. ``bars_held = 0`` is legal — a tp/sl can be
#: touched on the fill bar itself (``position_builder.Position.__post_init__``)
#: — and a single ordering would either erase those legs (reporting 0 for an
#: all-intraday population, which ``PromotionEvidence`` then rejects outright)
#: or make yesterday's exit concurrent with today's entry. Caught at Codex
#: checkpoint 2; the first version had only the first rule.
_CLOSE_EARLIER: Final = 0
_OPEN: Final = 1
_CLOSE_SAME_DAY: Final = 2


def _max_concurrency(ledger: RealisedLedger) -> int:
    """The most positions held at once, over realised AND open legs.

    A sweep over ``2n`` boundary events, because the stored population reaches
    4,228,628 realised legs and a date x position grid is not an option.
    """
    events: list[tuple[date, int, int]] = []
    for entry, exit_bar in zip(ledger.entry_fill_date, ledger.exit_bar_date, strict=True):
        events.append((entry, _OPEN, 1))
        events.append((exit_bar, _CLOSE_SAME_DAY if exit_bar == entry else _CLOSE_EARLIER, -1))
    for entry, _window_end in ledger.open_legs:
        # ⚠ NO CLOSING EVENT. A leg open at the window end never closes inside
        # it, so it is held from its entry to the end of the sweep.
        events.append((entry, _OPEN, 1))
    events.sort()
    held = 0
    peak = 0
    for _event_date, _phase, delta in events:
        held += delta
        peak = max(peak, held)
    return peak


def _share_pct(largest: int, total: int) -> Decimal:
    """One bucket's share of the population, in percent.

    ⚠ COUNT SHARE, NOT PROFIT SHARE. #2505's worked example is *"36.1% of
    accepted 2025 trades enter on one date"*.
    """
    return _decimal(largest / total * 100.0)


def _decimal(value: float) -> Decimal:
    """``repr`` and not ``str(float)`` rounding — the repo's conversion idiom."""
    return Decimal(repr(value))


__all__ = [
    "BEST_TRIM_PERCENTILE",
    "EXPECTED_SHORTFALL_ALPHA",
    "LEDGER_MEASUREMENT_RULE_VERSION",
    "LedgerMeasurements",
    "RealisedLedger",
    "measure_ledger",
]
