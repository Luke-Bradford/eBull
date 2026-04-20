"""yfinance provider — thin wrapper around yahooquery-style public data.

Used for non-US tickers where SEC XBRL doesn't apply (LSE, Euronext, HK,
ASX) and as a gap-filler for US tickers needing current price, analyst
estimates, major holders, etc.

yfinance scrapes Yahoo Finance's public pages. It is MIT-licensed, has
no API key, and no rate limit, but it CAN break without notice when
Yahoo changes layout. This module's contract: every method returns
either a typed dataclass on success or ``None`` on any failure. Never
raise to callers — the research page must stay interactive even when
yfinance is down.

Methods surface:

- :meth:`get_profile` — company identity + headline fundamentals.
- :meth:`get_quote` — last price + 52w range + day change.
- :meth:`get_key_stats` — valuation ratios + growth metrics.
- :meth:`get_financials` — income / balance / cashflow statement history.
- :meth:`get_dividends` — historical dividends per share.
- :meth:`get_analyst_estimates` — consensus targets + EPS forecast.
- :meth:`get_major_holders` — institutional + insider holdings pct.

All numeric values are ``Decimal`` where they represent money or ratios,
to match the rest of the codebase's money discipline.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any, Literal

import yfinance

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Return types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class YFinanceProfile:
    symbol: str
    display_name: str | None
    sector: str | None
    industry: str | None
    exchange: str | None
    country: str | None
    currency: str | None
    market_cap: Decimal | None
    employees: int | None
    website: str | None
    long_business_summary: str | None


@dataclass(frozen=True)
class YFinanceQuote:
    symbol: str
    price: Decimal | None
    day_change: Decimal | None
    day_change_pct: Decimal | None
    week_52_high: Decimal | None
    week_52_low: Decimal | None
    currency: str | None


@dataclass(frozen=True)
class YFinanceKeyStats:
    symbol: str
    pe_ratio: Decimal | None
    pb_ratio: Decimal | None
    dividend_yield: Decimal | None  # 0-1 ratio
    payout_ratio: Decimal | None
    roe: Decimal | None
    roa: Decimal | None
    debt_to_equity: Decimal | None
    revenue_growth_yoy: Decimal | None
    earnings_growth_yoy: Decimal | None


Statement = Literal["income", "balance", "cashflow"]
Period = Literal["quarterly", "annual"]


@dataclass(frozen=True)
class YFinanceFinancialRow:
    """One column of a financial statement (one fiscal period)."""

    period_end: date
    values: dict[str, Decimal]  # concept -> value


@dataclass(frozen=True)
class YFinanceFinancials:
    symbol: str
    statement: Statement
    period: Period
    currency: str | None
    rows: list[YFinanceFinancialRow]


@dataclass(frozen=True)
class YFinanceDividend:
    ex_date: date
    amount: Decimal


@dataclass(frozen=True)
class YFinanceAnalystEstimates:
    symbol: str
    target_mean: Decimal | None
    target_high: Decimal | None
    target_low: Decimal | None
    recommendation_mean: Decimal | None  # 1=Strong Buy, 5=Strong Sell
    num_analysts: int | None


@dataclass(frozen=True)
class YFinanceMajorHolders:
    symbol: str
    insiders_pct: Decimal | None
    institutions_pct: Decimal | None
    institutional_holders_count: int | None


@dataclass(frozen=True)
class YFinancePriceBar:
    """One bar of OHLCV history."""

    bar_date: date
    open: Decimal | None
    high: Decimal | None
    low: Decimal | None
    close: Decimal | None
    volume: int | None


# ---------------------------------------------------------------------------
# Coercion helpers
# ---------------------------------------------------------------------------


def _to_decimal(raw: Any) -> Decimal | None:
    """Coerce a yfinance value to Decimal; return None for NaN/missing."""
    if raw is None:
        return None
    # yfinance returns numpy float NaN for missing values; math.isnan is
    # unsafe on non-numeric input, so catch via Decimal construction.
    try:
        value = Decimal(str(raw))
    except InvalidOperation, ValueError, TypeError:
        return None
    if value.is_nan():
        return None
    return value


def _to_int(raw: Any) -> int | None:
    if raw is None:
        return None
    try:
        value = int(raw)
    except ValueError, TypeError:
        return None
    return value


def _to_str(raw: Any) -> str | None:
    if raw is None:
        return None
    # Yahoo sometimes returns float NaN for missing string fields (e.g.
    # sector, industry). str(float('nan')) == 'nan', which would render
    # the word "nan" in the UI — filter these out first.
    try:
        if isinstance(raw, float) and raw != raw:  # NaN check
            return None
    except TypeError:
        pass
    text = str(raw).strip()
    if not text or text.lower() == "nan":
        return None
    return text


# ---------------------------------------------------------------------------
# Provider
# ---------------------------------------------------------------------------


class YFinanceProvider:
    """Thin wrapper around :class:`yfinance.Ticker`.

    Every method catches all exceptions and returns ``None`` on failure.
    Yahoo occasionally breaks; the research UI must stay interactive.
    Failures are logged at WARNING level so operators notice drift.

    The provider is stateless beyond an optional ``session`` override
    for tests that want to inject a fake HTTP client.
    """

    def __init__(self, *, session: Any | None = None) -> None:
        self._session = session

    def _ticker(self, symbol: str) -> yfinance.Ticker:
        if self._session is not None:
            return yfinance.Ticker(symbol, session=self._session)
        return yfinance.Ticker(symbol)

    # -- Profile --------------------------------------------------------

    def get_profile(self, symbol: str) -> YFinanceProfile | None:
        try:
            info = self._ticker(symbol).info
        except Exception:
            logger.warning("yfinance.get_profile failed for %s", symbol, exc_info=True)
            return None
        if not info:
            return None
        return YFinanceProfile(
            symbol=symbol,
            display_name=_to_str(info.get("longName")) or _to_str(info.get("shortName")),
            sector=_to_str(info.get("sector")),
            industry=_to_str(info.get("industry")),
            exchange=_to_str(info.get("exchange")),
            country=_to_str(info.get("country")),
            currency=_to_str(info.get("currency")),
            market_cap=_to_decimal(info.get("marketCap")),
            employees=_to_int(info.get("fullTimeEmployees")),
            website=_to_str(info.get("website")),
            long_business_summary=_to_str(info.get("longBusinessSummary")),
        )

    # -- Quote ----------------------------------------------------------

    def get_quote(self, symbol: str) -> YFinanceQuote | None:
        try:
            info = self._ticker(symbol).info
        except Exception:
            logger.warning("yfinance.get_quote failed for %s", symbol, exc_info=True)
            return None
        if not info:
            return None
        price = _to_decimal(info.get("regularMarketPrice") or info.get("currentPrice"))
        prev_close = _to_decimal(info.get("regularMarketPreviousClose") or info.get("previousClose"))
        day_change: Decimal | None = None
        day_change_pct: Decimal | None = None
        if price is not None and prev_close is not None and prev_close != 0:
            day_change = price - prev_close
            day_change_pct = day_change / prev_close
        return YFinanceQuote(
            symbol=symbol,
            price=price,
            day_change=day_change,
            day_change_pct=day_change_pct,
            week_52_high=_to_decimal(info.get("fiftyTwoWeekHigh")),
            week_52_low=_to_decimal(info.get("fiftyTwoWeekLow")),
            currency=_to_str(info.get("currency")),
        )

    # -- Key stats ------------------------------------------------------

    def get_key_stats(self, symbol: str) -> YFinanceKeyStats | None:
        try:
            info = self._ticker(symbol).info
        except Exception:
            logger.warning("yfinance.get_key_stats failed for %s", symbol, exc_info=True)
            return None
        if not info:
            return None
        return YFinanceKeyStats(
            symbol=symbol,
            pe_ratio=_to_decimal(info.get("trailingPE")),
            pb_ratio=_to_decimal(info.get("priceToBook")),
            dividend_yield=_to_decimal(info.get("dividendYield")),
            payout_ratio=_to_decimal(info.get("payoutRatio")),
            roe=_to_decimal(info.get("returnOnEquity")),
            roa=_to_decimal(info.get("returnOnAssets")),
            debt_to_equity=_to_decimal(info.get("debtToEquity")),
            revenue_growth_yoy=_to_decimal(info.get("revenueGrowth")),
            earnings_growth_yoy=_to_decimal(info.get("earningsGrowth")),
        )

    # -- Financials -----------------------------------------------------

    def get_financials(
        self,
        symbol: str,
        *,
        statement: Statement,
        period: Period = "quarterly",
    ) -> YFinanceFinancials | None:
        """Fetch an income / balance / cashflow statement.

        Returns ``None`` on any error so the UI can render "no data" rather
        than 500. Empty statements (valid but with zero columns) also
        return ``None`` — there is no useful render for them.
        """
        if period not in ("quarterly", "annual"):
            logger.warning(
                "yfinance.get_financials: invalid period=%r for %s (expected 'quarterly' or 'annual')",
                period,
                symbol,
            )
            return None
        try:
            ticker = self._ticker(symbol)
            if period == "quarterly":
                frame = {
                    "income": ticker.quarterly_financials,
                    "balance": ticker.quarterly_balance_sheet,
                    "cashflow": ticker.quarterly_cashflow,
                }[statement]
            else:
                frame = {
                    "income": ticker.financials,
                    "balance": ticker.balance_sheet,
                    "cashflow": ticker.cashflow,
                }[statement]
            info = ticker.info
        except Exception:
            logger.warning(
                "yfinance.get_financials(statement=%s, period=%s) failed for %s",
                statement,
                period,
                symbol,
                exc_info=True,
            )
            return None
        if frame is None or frame.empty:
            return None
        currency = _to_str(info.get("financialCurrency")) if info else None
        rows: list[YFinanceFinancialRow] = []
        for col in frame.columns:
            # yfinance columns are pandas Timestamp; .date() extracts date.
            try:
                period_end = col.date()  # type: ignore[union-attr]
            except AttributeError:
                # Already a date, or malformed — skip.
                continue
            values: dict[str, Decimal] = {}
            for concept, raw in frame[col].items():
                decimal_value = _to_decimal(raw)
                if decimal_value is None:
                    continue
                values[str(concept)] = decimal_value
            if values:
                rows.append(YFinanceFinancialRow(period_end=period_end, values=values))
        if not rows:
            return None
        rows.sort(key=lambda r: r.period_end, reverse=True)
        return YFinanceFinancials(
            symbol=symbol,
            statement=statement,
            period=period,
            currency=currency,
            rows=rows,
        )

    # -- Dividends ------------------------------------------------------

    def get_dividends(self, symbol: str) -> list[YFinanceDividend] | None:
        try:
            series = self._ticker(symbol).dividends
        except Exception:
            logger.warning("yfinance.get_dividends failed for %s", symbol, exc_info=True)
            return None
        if series is None or series.empty:
            return []
        results: list[YFinanceDividend] = []
        for ts, amount_raw in series.items():
            try:
                ex_date = ts.date()  # type: ignore[union-attr]
            except AttributeError:
                continue
            decimal_amount = _to_decimal(amount_raw)
            if decimal_amount is None:
                continue
            results.append(YFinanceDividend(ex_date=ex_date, amount=decimal_amount))
        results.sort(key=lambda d: d.ex_date, reverse=True)
        return results

    # -- Analyst estimates ---------------------------------------------

    def get_analyst_estimates(self, symbol: str) -> YFinanceAnalystEstimates | None:
        try:
            info = self._ticker(symbol).info
        except Exception:
            logger.warning("yfinance.get_analyst_estimates failed for %s", symbol, exc_info=True)
            return None
        if not info:
            return None
        return YFinanceAnalystEstimates(
            symbol=symbol,
            target_mean=_to_decimal(info.get("targetMeanPrice")),
            target_high=_to_decimal(info.get("targetHighPrice")),
            target_low=_to_decimal(info.get("targetLowPrice")),
            recommendation_mean=_to_decimal(info.get("recommendationMean")),
            num_analysts=_to_int(info.get("numberOfAnalystOpinions")),
        )

    # -- Price history --------------------------------------------------

    def get_price_history(
        self,
        symbol: str,
        *,
        period: str = "1y",
        interval: str = "1d",
    ) -> list[YFinancePriceBar] | None:
        """Fetch OHLCV price history.

        ``period`` / ``interval`` follow yfinance's own string vocabulary
        (e.g. ``1d``, ``5d``, ``1mo``, ``3mo``, ``6mo``, ``1y``, ``2y``,
        ``5y``, ``ytd``, ``max``). Returns bars sorted oldest-first to
        match what a chart renderer expects.

        Returns ``None`` on yfinance raise; empty list on valid-but-empty
        result (e.g. delisted ticker, pre-IPO window).
        """
        try:
            frame = self._ticker(symbol).history(period=period, interval=interval)
        except Exception:
            logger.warning(
                "yfinance.get_price_history(period=%s, interval=%s) failed for %s",
                period,
                interval,
                symbol,
                exc_info=True,
            )
            return None
        if frame is None or frame.empty:
            return []
        bars: list[YFinancePriceBar] = []
        for ts, row in frame.iterrows():
            try:
                bar_date = ts.date()  # type: ignore[union-attr]
            except AttributeError:
                continue
            bars.append(
                YFinancePriceBar(
                    bar_date=bar_date,
                    open=_to_decimal(row.get("Open")),
                    high=_to_decimal(row.get("High")),
                    low=_to_decimal(row.get("Low")),
                    close=_to_decimal(row.get("Close")),
                    volume=_to_int(row.get("Volume")),
                )
            )
        bars.sort(key=lambda b: b.bar_date)
        return bars

    # -- Major holders --------------------------------------------------

    def get_major_holders(self, symbol: str) -> YFinanceMajorHolders | None:
        try:
            ticker = self._ticker(symbol)
            info = ticker.info
        except Exception:
            logger.warning("yfinance.get_major_holders failed for %s", symbol, exc_info=True)
            return None
        if not info:
            return None
        return YFinanceMajorHolders(
            symbol=symbol,
            insiders_pct=_to_decimal(info.get("heldPercentInsiders")),
            institutions_pct=_to_decimal(info.get("heldPercentInstitutions")),
            institutional_holders_count=_to_int(info.get("numberOfInstitutionalHolders")),
        )
