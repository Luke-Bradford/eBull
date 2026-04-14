"""
Financial Modelling Prep (FMP) fundamentals provider.

Implements FundamentalsProvider and EnrichmentProvider against the FMP Premium API.
Requires FMP_API_KEY with at least Premium tier access.

Endpoints used:
  /v3/income-statement/{symbol}?period=quarter&limit=N&apikey=...
  /v3/balance-sheet-statement/{symbol}?period=quarter&limit=N&apikey=...
  /v3/cash-flow-statement/{symbol}?period=quarter&limit=N&apikey=...
  /v3/income-statement-ttm/{symbol}?apikey=...
  /v3/balance-sheet-statement-ttm/{symbol}?apikey=...   (not a real FMP endpoint;
      TTM balance sheet is not published — balance sheet is always point-in-time)
  /v3/cash-flow-statement-ttm/{symbol}?apikey=...
  /v3/profile/{symbol}?apikey=...
  /v3/historical/earning_calendar/{symbol}?limit=N&apikey=...
  /v3/analyst-estimates/{symbol}?period=quarter&limit=1&apikey=...
  /v4/analyst-stock-recommendations/{symbol}?apikey=...
  /v4/price-target-consensus/{symbol}?apikey=...

as_of_date = latest balance sheet period end date (canonical period anchor).
TTM income/cashflow figures are sourced from the TTM endpoints where available;
if TTM endpoint returns nothing, the most recent quarterly value is used as a
fallback and the mismatch is logged.

Raw responses are persisted before normalisation.
"""

import json
import logging
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from types import TracebackType

import httpx

from app.providers.enrichment import (
    AnalystEstimates,
    EarningsEvent,
    EnrichmentProvider,
    InstrumentProfileData,
)
from app.providers.fundamentals import FundamentalsProvider, FundamentalsSnapshot
from app.providers.resilient_client import ResilientClient

logger = logging.getLogger(__name__)

_FMP_BASE_URL = "https://financialmodelingprep.com/api"
_RAW_PAYLOAD_DIR = Path("data/raw/fmp")

# FMP rate limit: ~250 req/min on Premium (plan-dependent).
# 0.25s interval ≈ 240/min — ~4% headroom.
_FMP_REQUEST_INTERVAL_S = 0.25


def _persist_raw(tag: str, payload: object) -> None:
    """Write raw API response to disk before normalisation."""
    try:
        _RAW_PAYLOAD_DIR.mkdir(parents=True, exist_ok=True)
        ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        path = _RAW_PAYLOAD_DIR / f"{tag}_{ts}.json"
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    except Exception:
        logger.warning("Failed to persist raw FMP payload for tag=%s", tag, exc_info=True)


@dataclass(frozen=True)
class InstrumentProfile:
    """Currency and exchange info from FMP /profile endpoint."""

    symbol: str
    currency: str
    exchange: str | None
    sector: str | None
    industry: str | None


class FmpFundamentalsProvider(FundamentalsProvider, EnrichmentProvider):
    """
    Fetches normalised fundamentals from FMP (Premium tier required).

    Use as a context manager to ensure the HTTP client is closed:

        with FmpFundamentalsProvider(api_key=...) as provider:
            snap = provider.get_latest_snapshot("AAPL")
    """

    def __init__(self, api_key: str) -> None:
        self._api_key = api_key
        self._client = httpx.Client(
            base_url=_FMP_BASE_URL,
            timeout=30.0,
        )
        self._http = ResilientClient(
            self._client,
            min_request_interval_s=_FMP_REQUEST_INTERVAL_S,
        )

    def __enter__(self) -> FmpFundamentalsProvider:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self._client.close()

    def get_latest_snapshot(self, symbol: str) -> FundamentalsSnapshot | None:
        """
        Fetch the most recent fundamentals snapshot for a symbol.

        Combines:
          - TTM income-statement for revenue, margins, EPS
          - TTM cash-flow-statement for FCF
          - Most recent quarterly balance-sheet for cash, debt, shares, book value
          - as_of_date = latest balance-sheet period end

        Returns None if FMP has no data for this symbol.
        """
        bs_rows = self._fetch_balance_sheet(symbol, limit=1)
        if not bs_rows:
            logger.info("FMP: no balance sheet data for %s", symbol)
            return None

        bs = bs_rows[0]
        ttm_income = self._fetch_ttm_income(symbol)
        ttm_cf = self._fetch_ttm_cashflow(symbol)

        # Fall back to most recent quarterly if TTM endpoint returns nothing
        if ttm_income is None:
            logger.warning("FMP: no TTM income data for %s, falling back to quarterly", symbol)
            inc_rows = self._fetch_income(symbol, limit=1)
            ttm_income = inc_rows[0] if inc_rows else None

        if ttm_cf is None:
            logger.warning("FMP: no TTM cash flow data for %s, falling back to quarterly", symbol)
            cf_rows = self._fetch_cashflow(symbol, limit=1)
            ttm_cf = cf_rows[0] if cf_rows else None

        return _build_snapshot(symbol, bs, ttm_income, ttm_cf)

    def get_snapshot_history(
        self,
        symbol: str,
        from_date: date,
        to_date: date,
        limit: int = 40,
    ) -> list[FundamentalsSnapshot]:
        """
        Return historical fundamentals snapshots oldest-first within [from_date, to_date].

        Each snapshot is keyed to a quarterly balance-sheet period end date.
        Corresponding quarterly income and cash-flow rows are matched by date.
        If no matching income/CF row exists for a given BS period, those fields
        are None and the mismatch is logged.
        """
        bs_rows = self._fetch_balance_sheet(symbol, limit=limit)
        inc_rows = self._fetch_income(symbol, limit=limit)
        cf_rows = self._fetch_cashflow(symbol, limit=limit)

        if not bs_rows:
            logger.info("FMP: no balance sheet history for %s", symbol)
            return []

        # Index income and CF rows by period end date for O(1) lookup
        inc_by_date: dict[str, dict[str, object]] = {r["date"]: r for r in inc_rows if "date" in r}  # type: ignore[index]
        cf_by_date: dict[str, dict[str, object]] = {r["date"]: r for r in cf_rows if "date" in r}  # type: ignore[index]

        snapshots: list[FundamentalsSnapshot] = []
        for bs in bs_rows:
            raw_date = bs.get("date")
            if not raw_date:
                continue
            try:
                period_end = date.fromisoformat(str(raw_date)[:10])
            except ValueError:
                logger.warning("FMP: unparseable balance sheet date '%s' for %s", raw_date, symbol)
                continue

            if not (from_date <= period_end <= to_date):
                continue

            date_key = str(raw_date)[:10]
            inc = inc_by_date.get(date_key)
            cf = cf_by_date.get(date_key)

            if inc is None:
                logger.debug("FMP: no income row for %s on %s", symbol, date_key)
            if cf is None:
                logger.debug("FMP: no cash flow row for %s on %s", symbol, date_key)

            snap = _build_snapshot(symbol, bs, inc, cf)
            if snap is not None:
                snapshots.append(snap)

        # Return oldest-first
        snapshots.sort(key=lambda s: s.as_of_date)
        return snapshots

    def get_instrument_profile(self, symbol: str) -> InstrumentProfile | None:
        """Fetch instrument profile for currency enrichment.

        Uses GET /api/v3/profile/{symbol}.
        Returns None if the symbol is not found in FMP.
        """
        item = self._fetch_profile(symbol)
        if item is None:
            return None
        raw_currency = str(item.get("currency", ""))
        # FMP returns "GBp" (pence) for LSE stocks — normalise to "GBP"
        currency = raw_currency.upper() if raw_currency else None
        raw_exchange = item.get("exchangeShortName")
        raw_sector = item.get("sector")
        raw_industry = item.get("industry")
        return InstrumentProfile(
            symbol=symbol,
            currency=currency or "USD",
            exchange=str(raw_exchange) if raw_exchange is not None else None,
            sector=str(raw_sector) if raw_sector is not None else None,
            industry=str(raw_industry) if raw_industry is not None else None,
        )

    def get_profile_enrichment(self, symbol: str) -> InstrumentProfileData | None:
        """Fetch supplemental profile metadata for a symbol.

        Returns None if the provider has no data for this symbol.
        """
        item = self._fetch_profile(symbol)
        if item is None:
            return None
        return _build_profile_data(symbol, item)

    def get_earnings_calendar(
        self,
        symbol: str,
        limit: int = 8,
    ) -> list[EarningsEvent]:
        """Return earnings events for a symbol, oldest-first, up to limit entries."""
        resp = self._http.get(
            f"/v3/historical/earning_calendar/{symbol}",
            params={"limit": limit, "apikey": self._api_key},
        )
        if resp.status_code != 200:
            logger.warning("FMP earnings calendar fetch failed for %s: %s", symbol, resp.status_code)
            return []
        raw = resp.json()
        _persist_raw(f"fmp_earnings_{symbol}", raw)
        if not isinstance(raw, list):
            return []
        events = [_build_earnings_event(symbol, row) for row in raw if isinstance(row, dict)]
        events.sort(key=lambda e: e.fiscal_date_ending)
        return events

    def get_analyst_estimates(self, symbol: str) -> AnalystEstimates | None:
        """Return the latest analyst consensus estimates for a symbol.

        Combines quarterly analyst estimates, stock recommendations, and price
        target consensus from three FMP endpoints.
        Returns None if the provider has no coverage for this symbol.
        """
        est_resp = self._http.get(
            f"/v3/analyst-estimates/{symbol}",
            params={"period": "quarter", "limit": 1, "apikey": self._api_key},
        )
        estimates: list[dict[str, object]] = []
        if est_resp.status_code == 200:
            raw_est = est_resp.json()
            _persist_raw(f"fmp_analyst_est_{symbol}", raw_est)
            if isinstance(raw_est, list):
                estimates = [row for row in raw_est if isinstance(row, dict)]

        rec_resp = self._http.get(
            f"/v4/analyst-stock-recommendations/{symbol}",
            params={"apikey": self._api_key},
        )
        consensus: dict[str, object] | None = None
        if rec_resp.status_code == 200:
            raw_rec = rec_resp.json()
            _persist_raw(f"fmp_analyst_rec_{symbol}", raw_rec)
            if isinstance(raw_rec, list) and raw_rec and isinstance(raw_rec[0], dict):
                consensus = raw_rec[0]

        pt_resp = self._http.get(
            f"/v4/price-target-consensus/{symbol}",
            params={"apikey": self._api_key},
        )
        price_target: dict[str, object] | None = None
        if pt_resp.status_code == 200:
            raw_pt = pt_resp.json()
            _persist_raw(f"fmp_price_target_{symbol}", raw_pt)
            if isinstance(raw_pt, list) and raw_pt and isinstance(raw_pt[0], dict):
                price_target = raw_pt[0]

        return _build_analyst_estimates(symbol, estimates, consensus, price_target)

    # ------------------------------------------------------------------
    # Private HTTP helpers
    # ------------------------------------------------------------------

    def _fetch_profile(self, symbol: str) -> dict[str, object] | None:
        """Fetch raw profile dict from FMP /v3/profile/{symbol}.

        Returns None if the symbol is not found or the response is not 200.
        Shared by get_instrument_profile and get_profile_enrichment.
        """
        resp = self._http.get(
            f"/v3/profile/{symbol}",
            params={"apikey": self._api_key},
        )
        if resp.status_code != 200:
            logger.warning("FMP profile fetch failed for %s: %s", symbol, resp.status_code)
            return None
        data = resp.json()
        _persist_raw(f"fmp_profile_{symbol}", data)
        if not isinstance(data, list) or not data:
            return None
        item = data[0]
        return item if isinstance(item, dict) else None

    def _fetch_balance_sheet(self, symbol: str, limit: int) -> list[dict[str, object]]:
        resp = self._http.get(
            f"/v3/balance-sheet-statement/{symbol}",
            params={"period": "quarter", "limit": limit, "apikey": self._api_key},
        )
        resp.raise_for_status()
        raw = resp.json()
        _persist_raw(f"fmp_bs_{symbol}", raw)
        return raw if isinstance(raw, list) else []

    def _fetch_income(self, symbol: str, limit: int) -> list[dict[str, object]]:
        resp = self._http.get(
            f"/v3/income-statement/{symbol}",
            params={"period": "quarter", "limit": limit, "apikey": self._api_key},
        )
        resp.raise_for_status()
        raw = resp.json()
        _persist_raw(f"fmp_inc_{symbol}", raw)
        return raw if isinstance(raw, list) else []

    def _fetch_cashflow(self, symbol: str, limit: int) -> list[dict[str, object]]:
        resp = self._http.get(
            f"/v3/cash-flow-statement/{symbol}",
            params={"period": "quarter", "limit": limit, "apikey": self._api_key},
        )
        resp.raise_for_status()
        raw = resp.json()
        _persist_raw(f"fmp_cf_{symbol}", raw)
        return raw if isinstance(raw, list) else []

    def _fetch_ttm_income(self, symbol: str) -> dict[str, object] | None:
        resp = self._http.get(
            f"/v3/income-statement-ttm/{symbol}",
            params={"apikey": self._api_key},
        )
        resp.raise_for_status()
        raw = resp.json()
        _persist_raw(f"fmp_inc_ttm_{symbol}", raw)
        if isinstance(raw, list) and raw:
            return raw[0]  # type: ignore[return-value]
        return None

    def _fetch_ttm_cashflow(self, symbol: str) -> dict[str, object] | None:
        resp = self._http.get(
            f"/v3/cash-flow-statement-ttm/{symbol}",
            params={"apikey": self._api_key},
        )
        resp.raise_for_status()
        raw = resp.json()
        _persist_raw(f"fmp_cf_ttm_{symbol}", raw)
        if isinstance(raw, list) and raw:
            return raw[0]  # type: ignore[return-value]
        return None


# ------------------------------------------------------------------
# Normalisers — pure functions, no I/O, unit tested with fixture data
# ------------------------------------------------------------------


def _build_snapshot(
    symbol: str,
    bs: Mapping[str, object],
    income: Mapping[str, object] | None,
    cashflow: Mapping[str, object] | None,
) -> FundamentalsSnapshot | None:
    """
    Combine balance-sheet, income, and cash-flow rows into a FundamentalsSnapshot.

    as_of_date is the balance-sheet period end date (canonical anchor).
    Returns None if as_of_date cannot be parsed from the balance sheet row.
    """
    raw_date = bs.get("date")
    if not raw_date:
        logger.warning("FMP: balance sheet row missing 'date' for %s: %s", symbol, dict(bs))
        return None

    try:
        as_of_date = date.fromisoformat(str(raw_date)[:10])
    except ValueError:
        logger.warning("FMP: unparseable date '%s' for %s", raw_date, symbol)
        return None

    return FundamentalsSnapshot(
        symbol=symbol,
        as_of_date=as_of_date,
        # TTM income fields
        revenue_ttm=_decimal_or_none(income.get("revenue") if income else None),
        gross_margin=_margin_or_none(income.get("grossProfitRatio") if income else None),
        operating_margin=_margin_or_none(income.get("operatingIncomeRatio") if income else None),
        eps=_decimal_or_none(income.get("epsdiluted") if income else None),
        # TTM cash flow
        fcf=_decimal_or_none(cashflow.get("freeCashFlow") if cashflow else None),
        # Balance sheet (point-in-time)
        cash=_decimal_or_none(bs.get("cashAndCashEquivalents")),
        debt=_decimal_or_none(bs.get("totalDebt")),
        net_debt=_decimal_or_none(bs.get("netDebt")),
        shares_outstanding=_int_or_none(bs.get("commonStock")),
        book_value=_decimal_or_none(bs.get("bookValuePerShare")),
    )


def _decimal_or_none(value: object) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def _margin_or_none(value: object) -> Decimal | None:
    """FMP returns margins as ratios (0–1). Return as-is."""
    return _decimal_or_none(value)


def _int_or_none(value: object) -> int | None:
    if value is None:
        return None
    try:
        result = int(float(str(value)))
        if result == 0:
            logger.warning("FMP: _int_or_none received zero value (%r) — returning None; verify source data", value)
            return None
        return result
    except ValueError, ArithmeticError:
        return None


def _build_profile_data(
    symbol: str,
    item: Mapping[str, object],
) -> InstrumentProfileData:
    """
    Build InstrumentProfileData from a raw FMP /v3/profile response dict.

    Pure function — no I/O.
    """
    ipo_date: date | None = None
    raw_ipo = item.get("ipoDate")
    if raw_ipo is not None:
        try:
            ipo_date = date.fromisoformat(str(raw_ipo)[:10])
        except ValueError:
            ipo_date = None

    raw_active = item.get("isActivelyTrading")
    is_actively_trading: bool | None = raw_active if isinstance(raw_active, bool) else None

    return InstrumentProfileData(
        symbol=symbol,
        beta=_decimal_or_none(item.get("beta")),
        public_float=_int_or_none(item.get("floatShares")),
        avg_volume_30d=_int_or_none(item.get("volAvg")),
        market_cap=_decimal_or_none(item.get("mktCap")),
        employees=_int_or_none(item.get("fullTimeEmployees")),
        ipo_date=ipo_date,
        is_actively_trading=is_actively_trading,
    )


def _build_earnings_event(
    symbol: str,
    row: Mapping[str, object],
) -> EarningsEvent:
    """
    Build an EarningsEvent from a single FMP earnings calendar response row.

    Pure function — no I/O.
    FMP fields: 'fiscalDateEnding' = fiscal period end, 'date' = announcement date.
    Raises ValueError if 'fiscalDateEnding' cannot be parsed.
    """
    # FMP 'fiscalDateEnding' is the fiscal quarter end; 'date' is announcement date
    raw_fiscal = row.get("fiscalDateEnding") or row.get("date")
    fiscal_date_ending = date.fromisoformat(str(raw_fiscal)[:10])

    reporting_date: date | None = None
    raw_reported = row.get("date")  # announcement/reporting date
    if raw_reported is not None:
        try:
            reporting_date = date.fromisoformat(str(raw_reported)[:10])
        except ValueError:
            reporting_date = None

    eps_estimate = _decimal_or_none(row.get("epsEstimated"))
    eps_actual = _decimal_or_none(row.get("eps"))
    revenue_estimate = _decimal_or_none(row.get("revenueEstimated"))
    revenue_actual = _decimal_or_none(row.get("revenue"))

    surprise_pct: Decimal | None = None
    if eps_estimate is not None and eps_actual is not None and eps_estimate != Decimal(0):
        surprise_pct = (eps_actual - eps_estimate) / abs(eps_estimate) * Decimal(100)

    return EarningsEvent(
        symbol=symbol,
        fiscal_date_ending=fiscal_date_ending,
        reporting_date=reporting_date,
        eps_estimate=eps_estimate,
        eps_actual=eps_actual,
        revenue_estimate=revenue_estimate,
        revenue_actual=revenue_actual,
        surprise_pct=surprise_pct,
    )


def _build_analyst_estimates(
    symbol: str,
    estimates: list[dict[str, object]],
    consensus: dict[str, object] | None,
    price_target: dict[str, object] | None,
) -> AnalystEstimates | None:
    """
    Build AnalystEstimates from FMP analyst estimate, recommendation, and price
    target consensus responses.

    Pure function — no I/O.
    Returns None if all three inputs are empty/None (no data available).
    """
    if not estimates and consensus is None and price_target is None:
        return None

    # -- from estimates[0] --
    as_of_date: date | None = None
    consensus_eps_fq: Decimal | None = None
    consensus_rev_fq: Decimal | None = None
    analyst_count_from_est: int | None = None

    if estimates:
        est = estimates[0]
        raw_date = est.get("date")
        if raw_date is not None:
            try:
                as_of_date = date.fromisoformat(str(raw_date)[:10])
            except ValueError:
                as_of_date = None
        consensus_eps_fq = _decimal_or_none(est.get("estimatedEpsAvg"))
        consensus_rev_fq = _decimal_or_none(est.get("estimatedRevenueAvg"))
        analyst_count_from_est = _int_or_none(est.get("numberAnalystEstimatedEps"))

    # -- from consensus (analyst stock recommendations) --
    buy_count: int | None = None
    hold_count: int | None = None
    sell_count: int | None = None

    if consensus is not None:
        buy_count = _int_or_none(consensus.get("buy"))
        hold_count = _int_or_none(consensus.get("hold"))
        sell_count = _int_or_none(consensus.get("sell"))

    # -- from price_target --
    price_target_mean: Decimal | None = None
    price_target_high: Decimal | None = None
    price_target_low: Decimal | None = None
    analyst_count: int | None = analyst_count_from_est

    if price_target is not None:
        price_target_mean = _decimal_or_none(price_target.get("targetConsensus") or price_target.get("targetMean"))
        price_target_high = _decimal_or_none(price_target.get("targetHigh"))
        price_target_low = _decimal_or_none(price_target.get("targetLow"))
        pt_analyst_count = _int_or_none(price_target.get("numberOfAnalysts"))
        if pt_analyst_count is not None:
            analyst_count = pt_analyst_count

    # as_of_date is required for the dataclass — use today as a fallback if
    # the estimates list was empty but consensus/price_target data is present
    if as_of_date is None:
        as_of_date = date.today()

    return AnalystEstimates(
        symbol=symbol,
        as_of_date=as_of_date,
        consensus_eps_fq=consensus_eps_fq,
        consensus_eps_fy=None,
        consensus_rev_fq=consensus_rev_fq,
        consensus_rev_fy=None,
        analyst_count=analyst_count,
        buy_count=buy_count,
        hold_count=hold_count,
        sell_count=sell_count,
        price_target_mean=price_target_mean,
        price_target_high=price_target_high,
        price_target_low=price_target_low,
    )
