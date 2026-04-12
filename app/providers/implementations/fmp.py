"""
Financial Modelling Prep (FMP) fundamentals provider.

Implements FundamentalsProvider against the FMP Premium API.
Requires FMP_API_KEY with at least Premium tier access.

Endpoints used:
  /v3/income-statement/{symbol}?period=quarter&limit=N&apikey=...
  /v3/balance-sheet-statement/{symbol}?period=quarter&limit=N&apikey=...
  /v3/cash-flow-statement/{symbol}?period=quarter&limit=N&apikey=...
  /v3/income-statement-ttm/{symbol}?apikey=...
  /v3/balance-sheet-statement-ttm/{symbol}?apikey=...   (not a real FMP endpoint;
      TTM balance sheet is not published — balance sheet is always point-in-time)
  /v3/cash-flow-statement-ttm/{symbol}?apikey=...

as_of_date = latest balance sheet period end date (canonical period anchor).
TTM income/cashflow figures are sourced from the TTM endpoints where available;
if TTM endpoint returns nothing, the most recent quarterly value is used as a
fallback and the mismatch is logged.

Raw responses are persisted before normalisation.
"""

import json
import logging
from collections.abc import Mapping
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from types import TracebackType

import httpx

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


class FmpFundamentalsProvider(FundamentalsProvider):
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

    # ------------------------------------------------------------------
    # Private HTTP helpers
    # ------------------------------------------------------------------

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
