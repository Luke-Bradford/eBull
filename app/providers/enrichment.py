"""
Enrichment provider interface.

Supplies supplemental instrument data beyond core fundamentals: company profile
metadata, forward-looking earnings calendar, and analyst consensus estimates.

FMP is the v1 implementation. All domain code imports this interface only —
never the concrete provider.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date
from decimal import Decimal


@dataclass(frozen=True)
class InstrumentProfileData:
    """
    Supplemental company profile metadata for an instrument.

    None means the data was not available from the provider for this symbol.
    """

    symbol: str
    beta: Decimal | None
    public_float: int | None  # shares, not monetary — use BIGINT in DB
    avg_volume_30d: int | None
    market_cap: Decimal | None
    employees: int | None
    ipo_date: date | None
    is_actively_trading: bool | None


@dataclass(frozen=True)
class EarningsEvent:
    """
    A single earnings release event for a company.

    fiscal_date_ending is the period end date; reporting_date is when results
    are/were announced. Either may be None if not yet confirmed by the provider.
    """

    symbol: str
    fiscal_date_ending: date
    reporting_date: date | None
    eps_estimate: Decimal | None
    eps_actual: Decimal | None
    revenue_estimate: Decimal | None
    revenue_actual: Decimal | None
    surprise_pct: Decimal | None  # (actual - estimate) / |estimate| * 100, percentage


@dataclass(frozen=True)
class AnalystEstimates:
    """
    Analyst consensus estimates for a symbol as of a given date.

    fq = forward quarter; fy = forward year.
    None means the provider returned no consensus for that metric.
    """

    symbol: str
    as_of_date: date
    consensus_eps_fq: Decimal | None
    consensus_eps_fy: Decimal | None
    consensus_rev_fq: Decimal | None
    consensus_rev_fy: Decimal | None
    analyst_count: int | None
    buy_count: int | None
    hold_count: int | None
    sell_count: int | None
    price_target_mean: Decimal | None
    price_target_high: Decimal | None
    price_target_low: Decimal | None


class EnrichmentProvider(ABC):
    """
    Interface for supplemental instrument enrichment data.

    Covers company profile metadata, earnings calendar, and analyst estimates.
    v1 implementation: FmpEnrichmentProvider.
    """

    @abstractmethod
    def get_profile_enrichment(self, symbol: str) -> InstrumentProfileData | None:
        """
        Return supplemental profile metadata for a symbol.
        Returns None if the provider has no data for this symbol.
        """

    @abstractmethod
    def get_earnings_calendar(
        self,
        symbol: str,
        limit: int = 8,
    ) -> list[EarningsEvent]:
        """
        Return upcoming and recent earnings events for a symbol, oldest-first,
        up to limit entries.

        limit defaults to 8 (approximately two years of quarterly results).
        """

    @abstractmethod
    def get_analyst_estimates(self, symbol: str) -> AnalystEstimates | None:
        """
        Return the latest analyst consensus estimates for a symbol.
        Returns None if the provider has no coverage for this symbol.
        """
