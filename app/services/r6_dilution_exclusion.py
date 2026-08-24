"""Frozen signal construction for R6 arm #2908.

The Nsi portfolio shape follows global-q's published testing-portfolio rule:
negative Nsi occupies portfolios 1-2, zero portfolio 3, and positive Nsi
portfolios 4-10, using NYSE breakpoints. The exact finite-sample order statistic
and tie rule are not published; this module fixes them by construction.
"""

from __future__ import annotations

import bisect
import math
from dataclasses import dataclass
from decimal import Decimal
from typing import Final

RED_FLAG_THRESHOLD: Final = 0.60


@dataclass(frozen=True)
class NsiInput:
    symbol: str
    exchange: str
    current_shares: Decimal | None
    prior_shares: Decimal | None
    red_flag_scores: tuple[float, ...]
    red_flag_history_complete: bool

    @property
    def nsi(self) -> float | None:
        if self.current_shares is None or self.prior_shares is None:
            return None
        value = math.log(float(self.current_shares / self.prior_shares))
        if not math.isfinite(value):
            raise ValueError(f"non-finite Nsi for {self.symbol}")
        return value


def _nearest_rank_breakpoints(values: list[float], groups: int) -> tuple[float, ...]:
    """Return lower-inclusive cut points at k/groups, fixed by construction."""
    if groups < 2:
        raise ValueError("groups must be at least two")
    ordered = sorted(values)
    if len(ordered) < groups:
        raise ValueError(f"need at least {groups} NYSE observations, got {len(ordered)}")
    return tuple(ordered[math.ceil(k * len(ordered) / groups) - 1] for k in range(1, groups))


def assign_nsi_portfolios(
    rows: tuple[NsiInput, ...],
    *,
    nyse_exchange_names: frozenset[str],
) -> dict[str, int | None]:
    """Assign published Nsi portfolios; missing share pairs remain unranked."""
    if not nyse_exchange_names:
        raise ValueError("NYSE exchange-name set must be non-empty")
    values = {row.symbol: row.nsi for row in rows}
    nyse_negative = [
        value
        for row in rows
        if row.exchange in nyse_exchange_names and (value := values[row.symbol]) is not None and value < 0
    ]
    nyse_positive = [
        value
        for row in rows
        if row.exchange in nyse_exchange_names and (value := values[row.symbol]) is not None and value > 0
    ]
    negative_breaks = _nearest_rank_breakpoints(nyse_negative, 2)
    positive_breaks = _nearest_rank_breakpoints(nyse_positive, 7)

    assigned: dict[str, int | None] = {}
    for row in rows:
        value = values[row.symbol]
        if value is None:
            assigned[row.symbol] = None
        elif value < 0:
            assigned[row.symbol] = 1 + bisect.bisect_left(negative_breaks, value)
        elif value == 0:
            assigned[row.symbol] = 3
        else:
            assigned[row.symbol] = 4 + bisect.bisect_left(positive_breaks, value)
    return assigned


def has_filing_red_flag(row: NsiInput) -> bool:
    """Mirror the existing 90-day AVG(non-null score) > 0.60 rule."""
    if not row.red_flag_history_complete or not row.red_flag_scores:
        return False
    return sum(row.red_flag_scores) / len(row.red_flag_scores) > RED_FLAG_THRESHOLD


def exclusions(
    rows: tuple[NsiInput, ...],
    portfolios: dict[str, int | None],
) -> dict[str, frozenset[str]]:
    dilution = frozenset(row.symbol for row in rows if portfolios[row.symbol] == 10)
    red_flag = frozenset(row.symbol for row in rows if has_filing_red_flag(row))
    return {
        "dilution": dilution,
        "red_flag": red_flag,
        "union": dilution | red_flag,
    }


__all__ = [
    "RED_FLAG_THRESHOLD",
    "NsiInput",
    "assign_nsi_portfolios",
    "exclusions",
    "has_filing_red_flag",
]
