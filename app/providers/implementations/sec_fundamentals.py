"""
SEC EDGAR XBRL fundamentals provider.

Implements FundamentalsProvider against the SEC Company Facts API
(https://data.sec.gov/api/xbrl/companyfacts/).  Completely free, no API key
required, 10 req/s rate limit.

This is the primary fundamentals source for US-listed companies.  FMP remains
as a fallback for non-US equities.

Data extraction strategy:
  - Income / cash-flow items (flow over a period): take the most recent 10-K
    entry (``fp=FY``) for a TTM figure.  If no 10-K exists, sum the 4 most
    recent distinct single-quarter values.
  - Balance-sheet items (point-in-time): take the most recent entry regardless
    of form type.
  - Margins and FCF are derived from the raw values.

XBRL tag priority for revenue (companies use different tags):
  1. RevenueFromContractWithCustomerExcludingAssessedTax  (ASC 606, post-2018)
  2. Revenues
  3. SalesRevenueNet  (older filings)
  4. RevenueFromContractWithCustomerIncludingAssessedTax

Provider contract:
  - Providers are pure HTTP clients — no DB access.
  - The service layer resolves instrument_id → CIK via external_identifiers
    and passes the CIK when calling this provider.
"""

from __future__ import annotations

import logging
from datetime import date
from decimal import Decimal
from types import TracebackType
from typing import Any

import httpx

from app.providers.fundamentals import FundamentalsProvider, FundamentalsSnapshot, XbrlFact
from app.providers.resilient_client import ResilientClient
from app.services import raw_persistence

logger = logging.getLogger(__name__)

_BASE_URL = "https://data.sec.gov"

# SEC rate-limit: 10 req/s.  Shared with the filings provider if both are
# running, but in practice they run in separate phases of daily_research_refresh
# so no cross-provider coordination is needed.
_MIN_REQUEST_INTERVAL_S = 0.11

# XBRL tags, in priority order, for each concept.
# Companies use different tags depending on accounting standard and vintage.
_REVENUE_TAGS = (
    "RevenueFromContractWithCustomerExcludingAssessedTax",
    "Revenues",
    "SalesRevenueNet",
    "RevenueFromContractWithCustomerIncludingAssessedTax",
)
_GROSS_PROFIT_TAGS = ("GrossProfit",)
_OPERATING_INCOME_TAGS = ("OperatingIncomeLoss",)
_OPERATING_CF_TAGS = ("NetCashProvidedByUsedInOperatingActivities",)
_CAPEX_TAGS = (
    "PaymentsToAcquirePropertyPlantAndEquipment",
    "CapitalExpenditures",
)
_CASH_TAGS = (
    "CashAndCashEquivalentsAtCarryingValue",
    "CashCashEquivalentsAndShortTermInvestments",
)
_DEBT_TAGS = ("LongTermDebt", "LongTermDebtNoncurrent")
_EQUITY_TAGS = ("StockholdersEquity", "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest")
_EPS_TAGS = ("EarningsPerShareDiluted",)
_SHARES_TAGS = ("CommonStockSharesOutstanding", "WeightedAverageNumberOfDilutedSharesOutstanding")

# ── Expanded XBRL tags for financial_facts_raw pipeline ──────────
TRACKED_CONCEPTS: dict[str, tuple[str, ...]] = {
    "revenue": (
        "RevenueFromContractWithCustomerExcludingAssessedTax",
        "Revenues",
        "SalesRevenueNet",
        "RevenueFromContractWithCustomerIncludingAssessedTax",
    ),
    "cost_of_revenue": ("CostOfGoodsAndServicesSold", "CostOfRevenue"),
    "gross_profit": ("GrossProfit",),
    "operating_income": ("OperatingIncomeLoss",),
    "net_income": ("NetIncomeLoss",),
    "eps_basic": ("EarningsPerShareBasic",),
    "eps_diluted": ("EarningsPerShareDiluted",),
    "research_and_dev": ("ResearchAndDevelopmentExpense",),
    "sga_expense": ("SellingGeneralAndAdministrativeExpense",),
    "depreciation_amort": (
        "DepreciationDepletionAndAmortization",
        "DepreciationAndAmortization",
    ),
    "interest_expense": ("InterestExpense", "InterestExpenseDebt"),
    "income_tax": ("IncomeTaxExpenseBenefit",),
    "shares_basic": ("WeightedAverageNumberOfSharesOutstandingBasic",),
    "shares_diluted": ("WeightedAverageNumberOfDilutedSharesOutstanding",),
    "sbc_expense": ("AllocatedShareBasedCompensationExpense", "ShareBasedCompensation"),
    "total_assets": ("Assets",),
    "total_liabilities": ("Liabilities",),
    "shareholders_equity": (
        "StockholdersEquity",
        "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
    ),
    "cash": (
        "CashAndCashEquivalentsAtCarryingValue",
        "CashCashEquivalentsAndShortTermInvestments",
    ),
    "long_term_debt": ("LongTermDebt", "LongTermDebtNoncurrent"),
    "short_term_debt": ("ShortTermBorrowings", "CommercialPaper"),
    "shares_outstanding": ("CommonStockSharesOutstanding",),
    "inventory": ("InventoryNet",),
    "receivables": ("AccountsReceivableNetCurrent",),
    "payables": ("AccountsPayableCurrent",),
    "goodwill": ("Goodwill",),
    "ppe_net": ("PropertyPlantAndEquipmentNet",),
    "operating_cf": ("NetCashProvidedByUsedInOperatingActivities",),
    "investing_cf": ("NetCashProvidedByUsedInInvestingActivities",),
    "financing_cf": ("NetCashProvidedByUsedInFinancingActivities",),
    "capex": ("PaymentsToAcquirePropertyPlantAndEquipment", "CapitalExpenditures"),
    "dividends_paid": ("PaymentsOfDividends",),
    "dps_declared": ("CommonStockDividendsPerShareDeclared",),
    "buyback_spend": ("PaymentsForRepurchaseOfCommonStock",),
}

_ALL_TRACKED_TAGS: frozenset[str] = frozenset(tag for tags in TRACKED_CONCEPTS.values() for tag in tags)


def _zero_pad_cik(cik: str | int) -> str:
    return str(int(cik)).zfill(10)


_UNIT_PRIORITY = ("USD", "USD/shares", "shares", "pure")


def _extract_facts_from_gaap(gaap: dict[str, Any]) -> list[XbrlFact]:
    """Extract all tracked XBRL facts from a companyfacts us-gaap section."""
    facts: list[XbrlFact] = []
    for tag_name, fact_data in gaap.items():
        if tag_name not in _ALL_TRACKED_TAGS:
            continue
        units = fact_data.get("units", {})
        for unit_key in _UNIT_PRIORITY:
            entries = units.get(unit_key)
            if not entries:
                continue
            for entry in entries:
                try:
                    end_str = entry["end"]
                    val = entry["val"]
                    accn = entry["accn"]
                    form = entry["form"]
                    filed_str = entry["filed"]
                except KeyError:
                    logger.debug("Skipping XBRL entry for %s: missing required field", tag_name)
                    continue

                start_str = entry.get("start")
                try:
                    period_end = date.fromisoformat(end_str)
                    period_start = date.fromisoformat(start_str) if start_str else None
                    filed_date = date.fromisoformat(filed_str)
                except ValueError, TypeError:
                    logger.debug("Skipping XBRL entry for %s: bad date format", tag_name)
                    continue

                # Guard against NaN/Infinity values in SEC data
                try:
                    decimal_val = Decimal(str(val))
                    if not decimal_val.is_finite():
                        logger.debug("Skipping XBRL entry for %s: non-finite val %s", tag_name, val)
                        continue
                except Exception:
                    logger.debug("Skipping XBRL entry for %s: unparseable val %s", tag_name, val)
                    continue

                facts.append(
                    XbrlFact(
                        concept=tag_name,
                        taxonomy="us-gaap",
                        unit=unit_key,
                        period_start=period_start,
                        period_end=period_end,
                        val=decimal_val,
                        frame=entry.get("frame"),
                        accession_number=accn,
                        form_type=form,
                        filed_date=filed_date,
                        fiscal_year=entry.get("fy"),
                        fiscal_period=entry.get("fp"),
                        decimals=str(entry["decimals"]) if "decimals" in entry else None,
                    )
                )
    return facts


class SecFundamentalsProvider(FundamentalsProvider):
    """
    Fetches normalised fundamentals from SEC EDGAR XBRL Company Facts API.

    Use as a context manager:

        with SecFundamentalsProvider(user_agent=...) as provider:
            snap = provider.get_latest_snapshot("AAPL", cik="0000320193")

    The ``symbol`` parameter in the interface methods is the ticker symbol
    for the FundamentalsSnapshot.  The ``cik`` must be passed separately
    via ``get_latest_snapshot_by_cik`` / ``get_snapshot_history_by_cik``.
    The standard interface methods (``get_latest_snapshot``, etc.) are
    implemented but require the symbol to already exist in the CIK cache
    (populated by the service layer at the start of the refresh run).
    """

    def __init__(self, user_agent: str) -> None:
        self._user_agent = user_agent
        self._client = httpx.Client(
            base_url=_BASE_URL,
            headers={"User-Agent": user_agent, "Accept": "application/json"},
            timeout=30.0,
        )
        self._http = ResilientClient(
            self._client,
            min_request_interval_s=_MIN_REQUEST_INTERVAL_S,
        )
        # symbol → CIK cache, populated by caller before bulk refresh
        self._cik_cache: dict[str, str] = {}

    def __enter__(self) -> SecFundamentalsProvider:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self._client.close()

    def set_cik_cache(self, mapping: dict[str, str]) -> None:
        """Populate the symbol→CIK cache for bulk operations."""
        self._cik_cache = {k.upper(): v for k, v in mapping.items()}

    # ------------------------------------------------------------------
    # FundamentalsProvider interface
    # ------------------------------------------------------------------

    def get_latest_snapshot(self, symbol: str) -> FundamentalsSnapshot | None:
        cik = self._cik_cache.get(symbol.upper())
        if not cik:
            return None
        return self.get_latest_snapshot_by_cik(symbol, cik)

    def get_snapshot_history(
        self,
        symbol: str,
        from_date: date,
        to_date: date,
        limit: int = 40,
    ) -> list[FundamentalsSnapshot]:
        cik = self._cik_cache.get(symbol.upper())
        if not cik:
            return []
        return self.get_snapshot_history_by_cik(symbol, cik, from_date, to_date, limit)

    # ------------------------------------------------------------------
    # CIK-based methods (called directly by service layer)
    # ------------------------------------------------------------------

    def get_latest_snapshot_by_cik(
        self,
        symbol: str,
        cik: str,
    ) -> FundamentalsSnapshot | None:
        """Fetch the most recent fundamentals for a company by CIK."""
        facts = self._fetch_company_facts(cik)
        if facts is None:
            return None

        gaap = facts.get("facts", {}).get("us-gaap", {})
        if not gaap:
            logger.info("SEC fundamentals: no us-gaap facts for CIK %s (%s)", cik, symbol)
            return None

        return _build_latest_snapshot(symbol, gaap)

    def get_snapshot_history_by_cik(
        self,
        symbol: str,
        cik: str,
        from_date: date,
        to_date: date,
        limit: int = 40,
    ) -> list[FundamentalsSnapshot]:
        """Fetch historical fundamentals snapshots from 10-K filings."""
        facts = self._fetch_company_facts(cik)
        if facts is None:
            return []

        gaap = facts.get("facts", {}).get("us-gaap", {})
        if not gaap:
            return []

        return _build_history_snapshots(symbol, gaap, from_date, to_date, limit)

    def extract_facts(self, symbol: str, cik: str) -> list[XbrlFact]:
        """Extract all tracked XBRL facts from SEC companyfacts."""
        raw = self._fetch_company_facts(cik)
        if raw is None:
            return []
        gaap = raw.get("facts", {}).get("us-gaap", {})
        if not gaap:
            logger.info("No us-gaap facts for %s (CIK %s)", symbol, cik)
            return []
        return _extract_facts_from_gaap(gaap)

    # ------------------------------------------------------------------
    # Private HTTP
    # ------------------------------------------------------------------

    def _fetch_company_facts(self, cik: str) -> dict[str, Any] | None:
        cik_padded = _zero_pad_cik(cik)
        path = f"/api/xbrl/companyfacts/CIK{cik_padded}.json"
        resp = self._http.get(path)
        if resp.status_code == 404:
            logger.info("SEC fundamentals: no company facts for CIK %s", cik)
            return None
        # Persist raw response before raise — non-negotiable for auditability.
        raw = resp.json()
        raw_persistence.persist_raw_if_new("sec_fundamentals", f"sec_facts_{cik_padded}", raw)
        resp.raise_for_status()
        return raw  # type: ignore[no-any-return]


# ------------------------------------------------------------------
# Normalisers — pure functions, no I/O
# ------------------------------------------------------------------


def _get_entries(gaap: dict[str, Any], tags: tuple[str, ...]) -> list[dict[str, Any]]:
    """Return entries for the first matching XBRL tag.

    Tries unit types in order: USD (monetary), USD/shares (per-share),
    shares (share counts).
    """
    for tag in tags:
        fact = gaap.get(tag)
        if fact is None:
            continue
        units = fact.get("units", {})
        entries = units.get("USD") or units.get("USD/shares") or units.get("shares") or []
        if entries:
            return entries  # type: ignore[no-any-return]
    return []


def _latest_annual_value(entries: list[dict[str, Any]]) -> tuple[float | None, date | None]:
    """
    Extract the most recent full-year (10-K, fp=FY) value.

    Returns (value, period_end_date) or (None, None) if no annual entry found.
    """
    annual = [e for e in entries if e.get("form") == "10-K" and e.get("fp") == "FY" and e.get("end")]
    if not annual:
        return None, None

    # Sort by end date descending to get most recent
    annual.sort(key=lambda e: e["end"], reverse=True)
    best = annual[0]
    try:
        end_date = date.fromisoformat(best["end"])
    except ValueError, TypeError:
        return None, None
    return best.get("val"), end_date


def _latest_point_in_time(entries: list[dict[str, Any]]) -> tuple[float | None, date | None]:
    """
    Extract the most recent value from any filing form.

    For balance-sheet items that are point-in-time snapshots.
    Returns (value, as_of_date) or (None, None).
    """
    valid = [e for e in entries if e.get("end") and e.get("val") is not None]
    if not valid:
        return None, None

    valid.sort(key=lambda e: e["end"], reverse=True)
    best = valid[0]
    try:
        end_date = date.fromisoformat(best["end"])
    except ValueError, TypeError:
        return None, None
    return best["val"], end_date


def _ttm_from_quarters(entries: list[dict[str, Any]]) -> float | None:
    """
    Compute TTM by summing the 4 most recent single-quarter values.

    Single-quarter entries have both ``start`` and ``end``, with a span of
    roughly 90 days (one quarter).  We filter to entries with a duration
    between 60 and 120 days to avoid picking up YTD or annual values.
    """
    quarterly = []
    for e in entries:
        start_str = e.get("start")
        end_str = e.get("end")
        val = e.get("val")
        if not start_str or not end_str or val is None:
            continue
        try:
            start = date.fromisoformat(start_str)
            end = date.fromisoformat(end_str)
        except ValueError, TypeError:
            continue
        days = (end - start).days
        if 60 <= days <= 120:
            quarterly.append((end, val))

    if len(quarterly) < 4:
        return None

    # Sort by end date descending, take 4 most recent
    quarterly.sort(key=lambda x: x[0], reverse=True)
    return sum(v for _, v in quarterly[:4])


def _get_ttm_value(gaap: dict[str, Any], tags: tuple[str, ...]) -> float | None:
    """Get TTM value: prefer annual 10-K, fall back to sum of 4 quarters."""
    entries = _get_entries(gaap, tags)
    if not entries:
        return None

    val, _ = _latest_annual_value(entries)
    if val is not None:
        return val

    return _ttm_from_quarters(entries)


def _get_balance_sheet_value(gaap: dict[str, Any], tags: tuple[str, ...]) -> float | None:
    """Get the most recent point-in-time balance sheet value."""
    entries = _get_entries(gaap, tags)
    if not entries:
        return None
    val, _ = _latest_point_in_time(entries)
    return val


def _safe_decimal(val: float | None) -> Decimal | None:
    if val is None:
        return None
    return Decimal(str(val))


def _safe_int(val: float | None) -> int | None:
    if val is None:
        return None
    result = int(val)
    return result if result != 0 else None


def _build_latest_snapshot(
    symbol: str,
    gaap: dict[str, Any],
) -> FundamentalsSnapshot | None:
    """Build a FundamentalsSnapshot from the most recent XBRL data."""

    # Income / cash flow — TTM values
    revenue = _get_ttm_value(gaap, _REVENUE_TAGS)
    gross_profit = _get_ttm_value(gaap, _GROSS_PROFIT_TAGS)
    operating_income = _get_ttm_value(gaap, _OPERATING_INCOME_TAGS)
    operating_cf = _get_ttm_value(gaap, _OPERATING_CF_TAGS)
    capex = _get_ttm_value(gaap, _CAPEX_TAGS)

    # Balance sheet — point in time
    cash = _get_balance_sheet_value(gaap, _CASH_TAGS)
    debt = _get_balance_sheet_value(gaap, _DEBT_TAGS)
    equity = _get_balance_sheet_value(gaap, _EQUITY_TAGS)
    shares = _get_balance_sheet_value(gaap, _SHARES_TAGS)

    # EPS — prefer TTM
    eps_entries = _get_entries(gaap, _EPS_TAGS)
    eps_val, _ = _latest_annual_value(eps_entries) if eps_entries else (None, None)
    if eps_val is None and eps_entries:
        # Fall back to most recent quarterly EPS (not summed — EPS is already per-share)
        eps_val, _ = _latest_point_in_time(eps_entries)

    # Determine as_of_date from the most recent balance sheet entry
    # (canonical anchor per settled decisions)
    as_of = _determine_as_of_date(gaap)
    if as_of is None:
        logger.info("SEC fundamentals: cannot determine as_of_date for %s", symbol)
        return None

    # Derived fields
    gross_margin: float | None = None
    if gross_profit is not None and revenue is not None and revenue != 0:
        gross_margin = gross_profit / revenue

    operating_margin: float | None = None
    if operating_income is not None and revenue is not None and revenue != 0:
        operating_margin = operating_income / revenue

    fcf: float | None = None
    if operating_cf is not None:
        # CapEx is typically a positive outflow in XBRL.  Some filers
        # report it as negative (cash outflow sign convention) — normalise
        # to positive before subtracting.
        fcf = operating_cf - abs(capex) if capex is not None else operating_cf

    net_debt: float | None = None
    if debt is not None and cash is not None:
        net_debt = debt - cash
    # If cash is unknown, leave net_debt as None rather than assuming zero

    book_value: float | None = None
    if equity is not None and shares is not None and shares != 0:
        book_value = equity / shares

    return FundamentalsSnapshot(
        symbol=symbol,
        as_of_date=as_of,
        revenue_ttm=_safe_decimal(revenue),
        gross_margin=_safe_decimal(gross_margin),
        operating_margin=_safe_decimal(operating_margin),
        fcf=_safe_decimal(fcf),
        cash=_safe_decimal(cash),
        debt=_safe_decimal(debt),
        net_debt=_safe_decimal(net_debt),
        shares_outstanding=_safe_int(shares),
        book_value=_safe_decimal(book_value),
        eps=_safe_decimal(eps_val),
    )


def _determine_as_of_date(gaap: dict[str, Any]) -> date | None:
    """
    Determine the canonical as_of_date from the most recent balance-sheet entry.

    Tries cash first (most universal), then equity, then debt.
    """
    for tags in (_CASH_TAGS, _EQUITY_TAGS, _DEBT_TAGS):
        entries = _get_entries(gaap, tags)
        if entries:
            _, d = _latest_point_in_time(entries)
            if d is not None:
                return d
    return None


def _build_history_snapshots(
    symbol: str,
    gaap: dict[str, Any],
    from_date: date,
    to_date: date,
    limit: int,
) -> list[FundamentalsSnapshot]:
    """
    Build historical snapshots from 10-K filings within a date range.

    Each 10-K filing represents one snapshot.  Balance-sheet items are taken
    from the 10-K's period end date; income/CF items are the FY values.
    """
    # Find all 10-K period end dates from revenue entries
    rev_entries = _get_entries(gaap, _REVENUE_TAGS)
    annual_entries = [e for e in rev_entries if e.get("form") == "10-K" and e.get("fp") == "FY" and e.get("end")]

    if not annual_entries:
        return []

    # Get unique end dates within range
    end_dates: list[date] = []
    seen: set[str] = set()
    for e in annual_entries:
        end_str = e["end"]
        if end_str in seen:
            continue
        seen.add(end_str)
        try:
            d = date.fromisoformat(end_str)
        except ValueError, TypeError:
            continue
        if from_date <= d <= to_date:
            end_dates.append(d)

    end_dates.sort()
    if limit:
        end_dates = end_dates[-limit:]

    snapshots: list[FundamentalsSnapshot] = []
    for end_d in end_dates:
        snap = _build_snapshot_for_date(symbol, gaap, end_d)
        if snap is not None:
            snapshots.append(snap)

    return snapshots


def _get_value_at_date(
    entries: list[dict[str, Any]],
    target_date: date,
    *,
    annual_only: bool = False,
) -> float | None:
    """Get the value closest to target_date from entries."""
    candidates = []
    for e in entries:
        end_str = e.get("end")
        val = e.get("val")
        if not end_str or val is None:
            continue
        if annual_only and (e.get("form") != "10-K" or e.get("fp") != "FY"):
            continue
        try:
            end = date.fromisoformat(end_str)
        except ValueError, TypeError:
            continue
        if end == target_date:
            return val  # type: ignore[no-any-return]
        candidates.append((abs((end - target_date).days), val))

    if not candidates:
        return None
    # Return the value from the entry closest to target date
    candidates.sort(key=lambda x: x[0])
    # Only accept entries within 45 days of target (same quarter)
    if candidates[0][0] <= 45:
        return candidates[0][1]
    return None


def _build_snapshot_for_date(
    symbol: str,
    gaap: dict[str, Any],
    target_date: date,
) -> FundamentalsSnapshot | None:
    """Build a snapshot using values at or near a specific date."""
    revenue = _get_value_at_date(_get_entries(gaap, _REVENUE_TAGS), target_date, annual_only=True)
    gross_profit = _get_value_at_date(_get_entries(gaap, _GROSS_PROFIT_TAGS), target_date, annual_only=True)
    operating_income = _get_value_at_date(_get_entries(gaap, _OPERATING_INCOME_TAGS), target_date, annual_only=True)
    operating_cf = _get_value_at_date(_get_entries(gaap, _OPERATING_CF_TAGS), target_date, annual_only=True)
    capex = _get_value_at_date(_get_entries(gaap, _CAPEX_TAGS), target_date, annual_only=True)

    cash = _get_value_at_date(_get_entries(gaap, _CASH_TAGS), target_date)
    debt = _get_value_at_date(_get_entries(gaap, _DEBT_TAGS), target_date)
    equity = _get_value_at_date(_get_entries(gaap, _EQUITY_TAGS), target_date)
    shares = _get_value_at_date(_get_entries(gaap, _SHARES_TAGS), target_date)
    eps_val = _get_value_at_date(_get_entries(gaap, _EPS_TAGS), target_date, annual_only=True)

    gross_margin: float | None = None
    if gross_profit is not None and revenue is not None and revenue != 0:
        gross_margin = gross_profit / revenue

    operating_margin: float | None = None
    if operating_income is not None and revenue is not None and revenue != 0:
        operating_margin = operating_income / revenue

    fcf: float | None = None
    if operating_cf is not None:
        fcf = operating_cf - abs(capex) if capex is not None else operating_cf

    net_debt: float | None = None
    if debt is not None and cash is not None:
        net_debt = debt - cash

    book_value: float | None = None
    if equity is not None and shares is not None and shares != 0:
        book_value = equity / shares

    return FundamentalsSnapshot(
        symbol=symbol,
        as_of_date=target_date,
        revenue_ttm=_safe_decimal(revenue),
        gross_margin=_safe_decimal(gross_margin),
        operating_margin=_safe_decimal(operating_margin),
        fcf=_safe_decimal(fcf),
        cash=_safe_decimal(cash),
        debt=_safe_decimal(debt),
        net_debt=_safe_decimal(net_debt),
        shares_outstanding=_safe_int(shares),
        book_value=_safe_decimal(book_value),
        eps=_safe_decimal(eps_val),
    )
