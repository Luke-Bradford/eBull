"""Instrument list and detail API endpoints.

Reads from:
  - instruments          (core instrument metadata)
  - quotes               (1:1 current snapshot per instrument, overwritten each refresh)
  - coverage             (1:1 coverage tier per instrument)
  - external_identifiers (1:N provider-native identifiers per instrument)

No writes (DB-side). No schema changes.

**Carve-out — intraday candles (#600).** The
``GET /instruments/{symbol}/intraday-candles`` endpoint is a
provider-backed pass-through: it loads eToro broker credentials,
calls the live eToro REST endpoint via ``EtoroMarketDataProvider``,
and serves bars through an in-process TTL cache (no DB persistence).
This is the one endpoint in this module that consumes external API
quota and writes an audit row per request (via
``load_credential_for_provider_use``). All other endpoints stay
DB-only.
"""

from __future__ import annotations

import csv
import io
import logging
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Literal, get_args

import httpx
import psycopg
import psycopg.rows
from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel, Field

from app.api._helpers import resolve_quote_price
from app.api.auth import require_session_or_service_token
from app.config import settings
from app.db import get_conn
from app.db.snapshot import snapshot_read
from app.providers.implementations.etoro import EtoroMarketDataProvider
from app.providers.market_data import IntradayInterval
from app.services import ownership_history, ownership_rollup
from app.services.broker_credentials import (
    CredentialNotFound,
    load_credential_for_provider_use,
)
from app.services.dimensional_facts import DimensionalAxis
from app.services.dimensional_facts_store import read_segments
from app.services.fcf_yield import fcf_yield_series
from app.services.fx import FxRateNotFound, convert, load_live_fx_rates
from app.services.intraday_candles import fetch_intraday_candles
from app.services.operators import (
    AmbiguousOperatorError,
    NoOperatorError,
    sole_operator_id,
)
from app.services.peer_comparison import (
    FACTOR_BETTER_WHEN,
    FACTOR_KEYS,
    FACTOR_LABELS,
    compute_peer_comparison,
    is_factor_thin,
)
from app.services.portfolio_risk import (
    PortfolioRiskStatus,
    compute_portfolio_relative_risk,
)
from app.services.risk_metrics import (
    RISK_METRICS_VERSION,
    annualized_vol,
    drawdown_curve,
    load_close_series,
    ols_beta,
    simple_returns,
)
from app.services.runtime_config import get_runtime_config
from app.services.sector_classification import resolve_sector_spdr, sector_spdr_case_sql

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/instruments", tags=["instruments"])


@contextmanager
def _short_lived_conn(request: Request) -> Iterator[psycopg.Connection[object]]:
    """Borrow a pooled conn for the duration of the ``with`` block, then
    release it — WITHOUT ``Depends(get_conn)``, so the conn is NOT pinned
    for the whole request.

    #1472 PR2 (V3/V4): the lazy-fill 8-K body + business-sections routes
    must NOT hold a pooled conn across the external SEC fetch. The fetch
    happens inside the service on its own short pool borrow; the route
    does its own DB reads on these scoped borrows and releases each one
    around the service call. Reuses ``get_conn``'s pool-from-state + 503
    mapping (#717). Hand-driving bypasses ``app.dependency_overrides`` —
    tests inject via ``app.state.db_pool`` (prevention-log #265).

    READ-ONLY by contract: ``gen.close()`` injects ``GeneratorExit`` at
    ``get_conn``'s yield, so ``pool.connection()``'s ``with conn:`` exits
    via exception → ROLLS BACK. Do not write through this helper — a write
    would be silently discarded. Writers own their connection lifecycle
    (the lazy-fill services borrow + commit their own pool conns).
    """
    gen = get_conn(request)
    conn = next(gen)
    try:
        yield conn
    finally:
        gen.close()


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------

MAX_PAGE_LIMIT = 200


class QuoteSnapshot(BaseModel):
    """Latest quote for an instrument.

    The ``quotes`` table is a 1:1 current-snapshot table keyed by
    ``instrument_id``.  Each market-data refresh overwrites the single row
    for a given instrument, so there is never more than one quote row per
    instrument.  A LEFT JOIN on ``quotes`` is therefore fan-out-safe.
    """

    bid: float
    ask: float
    last: float | None
    spread_pct: float | None
    quoted_at: datetime


class ExternalIdentifier(BaseModel):
    provider: str
    identifier_type: str
    identifier_value: str


class InstrumentListItem(BaseModel):
    instrument_id: int
    symbol: str
    company_name: str
    exchange: str | None
    currency: str | None
    sector: str | None
    # #1675: real GICS sector + its sector-SPDR, resolved on-read from the SEC
    # SIC (same crosswalk as the identity payload). NULL for ETFs / non-filers /
    # unmapped SIC — never a guessed sector. The opaque ``sector`` 1-9 code above
    # is retained for back-compat but is no longer the operator-facing grouping
    # dimension. Required-nullable to mirror the TS ``string | null`` exactly.
    gics_sector: str | None
    sector_spdr: str | None
    is_tradable: bool
    coverage_tier: int | None
    latest_quote: QuoteSnapshot | None


class InstrumentListResponse(BaseModel):
    items: list[InstrumentListItem]
    total: int
    offset: int
    limit: int


class InstrumentIdentity(BaseModel):
    symbol: str
    display_name: str | None
    sector: str | None  # eToro numeric industry id, as text (provider contract)
    # Resolved eToro industry name via etoro_stocks_industries (sql/070); None when
    # sector is NULL or unmapped. The FE uses this as the fallback sector label for
    # non-SEC instruments that have no GICS sector (resolved from SIC below).
    sector_name: str | None
    industry: str | None
    # #1634: real GICS sector + its sector-SPDR, resolved on-read from the SEC
    # SIC code (instruments.sector is an opaque 1-9 code). NULL when the
    # instrument has no SIC (ETFs / non-filers) or its SIC has no confident
    # mapping — never a guessed sector. Required-nullable (no default) to mirror
    # the TS `string | null` exactly (Codex ckpt-2).
    gics_sector: str | None
    sector_spdr: str | None
    exchange: str | None
    country: str | None
    currency: str | None
    market_cap: Decimal | None
    # #1665: per-class FLOAT value of THIS instrument's own share class — its
    # FSDS shares × its price (GOOGL Class A ≈ $2.15T), a SEPARATE stat from
    # ``market_cap`` (the whole company, ≈ $4.45T, identical across siblings).
    # Non-null ONLY for a curated dual-class issuer where this instrument is a
    # priced per-class leg; null for single-class issuers (``market_cap`` already
    # IS the sole class value) and where no clean per-class leg exists.
    # Required-nullable (no default) to mirror the TS ``string | null`` exactly.
    class_market_value: Decimal | None
    # #819: when set, this instrument is an operational duplicate
    # (e.g. ``AAPL.RTH``) of the named canonical symbol
    # (``AAPL``). The frontend should redirect to the canonical
    # symbol's page so chart / ownership / fundamentals render
    # under the security with the actual SEC filings. NULL = this
    # instrument IS canonical (the default). See sql/145 +
    # docs/settled-decisions.md "Canonical-instrument redirect".
    canonical_symbol: str | None = None


class InstrumentPrice(BaseModel):
    """``current`` + ``currency`` are the instrument's NATIVE listing price —
    the tradable number — and never flip by which path (REST snapshot vs SSE
    live tick) answers first (#1906, operator decision 2026-07-04: native is
    PRIMARY everywhere). ``display_current`` + ``display_currency`` carry the
    FX-converted companion in the operator's display currency (a secondary,
    muted figure in the UI); both are ``None`` when no FX rate is available or
    the native currency already equals the display currency — the native price
    still renders. Matches ``InstrumentIdentity.currency`` (also native, the
    price chart's axis per the #1845 convention)."""

    current: Decimal | None
    day_change: Decimal | None
    day_change_pct: Decimal | None
    week_52_high: Decimal | None
    week_52_low: Decimal | None
    currency: str | None
    display_current: Decimal | None = None
    display_currency: str | None = None


# Closed set of values for `InstrumentKeyStats.field_source` entries. Mirror
# in frontend/src/api/types.ts — consumers rely on this being exhaustive.
# Per settled decision (eToro = market data, SEC = official filings),
# yfinance has no role; #498/#499 retired it.
#
# ``sec_dividend_summary`` distinguishes values sourced from
# ``instrument_dividend_summary.ttm_yield_pct`` (#426) from values
# computed against XBRL concepts directly — the dividend summary is
# *not* a raw XBRL field and conflating the two would mislead any
# audit trail that filters on provenance.
KeyStatsFieldSource = Literal[
    "sec_xbrl",
    "sec_dividend_summary",
    "unavailable",
    "sec_xbrl_price_missing",
]


class InstrumentKeyStats(BaseModel):
    pe_ratio: Decimal | None
    pb_ratio: Decimal | None
    dividend_yield: Decimal | None
    payout_ratio: Decimal | None
    roe: Decimal | None
    roa: Decimal | None
    debt_to_equity: Decimal | None
    revenue_growth_yoy: Decimal | None
    earnings_growth_yoy: Decimal | None
    field_source: dict[str, KeyStatsFieldSource] | None = None


class InstrumentFinancialRow(BaseModel):
    period_end: date
    period_type: str  # Q1/Q2/Q3/Q4/FY (from financial_periods)
    values: dict[str, Decimal | None]


class InstrumentFinancials(BaseModel):
    symbol: str
    statement: str  # "income" | "balance" | "cashflow"
    period: str  # "quarterly" | "annual"
    currency: str | None
    source: str  # "financial_periods" (local SEC XBRL) | "unavailable"
    rows: list[InstrumentFinancialRow]


class FcfYieldPoint(BaseModel):
    period_end: date
    period_type: str  # Q1/Q2/Q3/Q4 (quarterly) | FY (annual)
    fcf_ttm: Decimal | None  # trailing-4Q (quarterly) or FY (annual); ABS(SUM capex)
    market_cap: Decimal | None  # period_end shares × period_end close
    fcf_yield_pct: Decimal | None  # fcf_ttm / market_cap × 100; negative preserved
    price: Decimal | None  # close at/before period_end
    price_as_of: date | None


class FcfYieldSeries(BaseModel):
    symbol: str
    # Set → instrument is fail-closed suppressed (multi-class cap distortion
    # #1662, or cross-currency FCF/price); ``points`` is then empty.
    suppressed_reason: Literal["multiclass", "currency_mismatch"] | None
    points: list[FcfYieldPoint]


class PeerFactor(BaseModel):
    """One radar factor: the instrument's value vs its sector median (#1751)."""

    key: str
    label: str
    instrument_value: float | None
    sector_median: float | None
    sector_n: int  # # sector members with a non-null value for this factor
    dev_limited: bool  # True when thin: price-gated (P/E) OR sector coverage <20% (#1836)
    better_when: Literal["higher", "lower"]


class PeerInstrument(BaseModel):
    """A sector peer with its factor row (for the #594 heatmap)."""

    instrument_id: int
    symbol: str
    company_name: str | None
    size_proxy: float | None  # total_assets (peer-proximity ranking key)
    factors: dict[str, float | None]


class PeerComparison(BaseModel):
    symbol: str
    instrument_id: int
    sector: str  # raw code "1".."9" (instruments.sector is TEXT; no lookup table)
    sector_member_count: int  # complete-TTM members in the sector (median base)
    factors: list[PeerFactor]
    peers: list[PeerInstrument]


class CandleBar(BaseModel):
    """One daily OHLCV bar — the minimal shape a chart library needs."""

    date: date
    open: Decimal | None
    high: Decimal | None
    low: Decimal | None
    close: Decimal | None
    volume: Decimal | None


class InstrumentCandles(BaseModel):
    symbol: str
    # Range token the caller asked for; echoed so the client can cache
    # by range key. `days` is the resolved lookback the server applied.
    # Field named `range` even though it shadows the Python builtin at
    # class scope — pydantic v2's alias+populate_by_name wiring kept
    # tripping pyright, and the shadow is safe inside a BaseModel
    # (the builtin is still reachable as `builtins.range`).
    range: Literal["1w", "1m", "3m", "6m", "ytd", "1y", "5y", "max"]  # noqa: A003
    days: int | None  # None when range="max"
    rows: list[CandleBar]


class IntradayBarPayload(BaseModel):
    """One intraday OHLCV bar.

    ``timestamp`` is a UTC ISO-8601 datetime — distinct from
    ``CandleBar.date`` which is YYYY-MM-DD only. Lightweight-charts
    consumers feed ``timestamp`` straight into the time scale via
    ``new Date(...).getTime() / 1000``.
    """

    timestamp: datetime
    open: Decimal | None
    high: Decimal | None
    low: Decimal | None
    close: Decimal | None
    volume: int | None


class InstrumentIntradayCandles(BaseModel):
    """Response shape for /instruments/{symbol}/intraday-candles.

    ``persisted`` is always False so the caller knows these bars are
    not in any DB table — they came directly from the provider via the
    in-process cache. Useful for ops dashboards and a future
    persistence-status flag if we ever cache to disk.
    """

    symbol: str
    interval: IntradayInterval
    count: int
    persisted: Literal[False] = False
    rows: list[IntradayBarPayload]


# Accepted interval tokens — derived from the IntradayInterval Literal
# so this list cannot drift from the provider contract.
_VALID_INTERVALS: frozenset[str] = frozenset(get_args(IntradayInterval))

# Hard cap mirrors eToro's documented 1000-candle ceiling. Leaving a
# small headroom so we never touch the limit.
_MAX_INTRADAY_COUNT = 1000


class CapabilityCellPayload(BaseModel):
    """One (capability × instrument) cell in the summary response.

    Mirrors ``app.services.capabilities.CapabilityCell`` — the API
    layer translates the dataclass to this Pydantic model so the
    OpenAPI schema is generated correctly.
    """

    providers: list[str]
    data_present: dict[str, bool]


# Session-shading profile for the intraday chart (#609 Phase A). Derived
# from the instrument's exchange + ``exchanges.asset_class``; drives which
# session bands the frontend paints. Single source of truth for the four
# values — the frontend mirrors this Literal.
#   us_equity      — full PM/RTH/AH bands + NYSE holiday calendar
#   us_equity_rth  — eToro RTH-only duplicate (exchange 33), no PM/AH
#   foreign_equity — non-US listing: open/closed only, no PM/AH tint
#   continuous     — fx / commodity / index / crypto: no bands
SessionProfile = Literal["us_equity", "us_equity_rth", "foreign_equity", "continuous"]

# SQL fragment deriving ``session_profile`` from a row that has ``i.exchange``
# and a ``LEFT JOIN exchanges e``. Exchange-33's RTH-only nature is NOT
# encoded in ``asset_class`` (it is ``us_equity``) so the exchange-id check
# must come first. Total + default-bearing: any unrecognised / NULL
# asset_class falls through to ``continuous`` (no session bands) — never
# errors on a new enum, and never paints NYSE PM/AH bands on an unclassified
# *foreign* exchange (Tokyo / Toronto / Tadawul etc. carry asset_class
# ``unknown``). US instruments only live on exchanges 4/5/33/19/20, which are
# all classified, so a US instrument never reaches the ELSE branch.
_SESSION_PROFILE_SQL = """
        CASE
            WHEN i.exchange = '33' THEN 'us_equity_rth'
            WHEN e.asset_class = 'us_equity' THEN 'us_equity'
            WHEN e.asset_class IN ('eu_equity', 'uk_equity', 'asia_equity', 'mena_equity')
                THEN 'foreign_equity'
            WHEN e.asset_class IN ('commodity', 'fx', 'index', 'crypto') THEN 'continuous'
            ELSE 'continuous'
        END AS session_profile
"""


class InstrumentSummary(BaseModel):
    """Per-ticker research summary.

    Sources, per the settled provider strategy (#498/#499):
      - Identity: local ``instruments`` row (sourced from eToro
        universe sync + SEC profile enrichment).
      - Price: ``quotes`` table (eToro WS / market data refresh).
      - Market cap: SEC XBRL share count × eToro close.
      - Key stats: SEC XBRL via ``financial_periods_ttm`` +
        ``instrument_dividend_summary``.

    All leaf fields are nullable so the UI can render partial data
    when SEC coverage is missing. ``source`` reports which provider
    populated each section.
    """

    instrument_id: int
    is_tradable: bool
    coverage_tier: int | None
    # Intraday-chart session-shading profile (#609). See SessionProfile.
    session_profile: SessionProfile
    identity: InstrumentIdentity
    price: InstrumentPrice | None
    key_stats: InstrumentKeyStats | None
    source: dict[str, str]
    # Coverage gates — frontend uses these to hide irrelevant
    # tabs / panels rather than render an empty state for an
    # instrument the source does not cover (#503 PR 2).
    #
    # ``has_sec_cik``: True iff the instrument has a primary SEC
    # CIK in ``external_identifiers``. Gates SEC-specific panels:
    # SecProfilePanel, InsiderActivityPanel, DividendsPanel,
    # business-summary section.
    #
    # ``has_filings_coverage``: True iff any provider has filed
    # filings for the instrument (today: SEC; tomorrow:
    # Companies House / regional sources). Gates the
    # source-agnostic Filings tab + right-rail "recent filings"
    # widget. Wider than ``has_sec_cik`` so adding a non-SEC
    # provider later doesn't bake in a follow-up.
    has_sec_cik: bool
    has_filings_coverage: bool
    # Per-capability resolution (#515 PR 3). Keyed by capability
    # name (filings / fundamentals / dividends / …); each value
    # carries the operator-decided ``providers`` list and a
    # per-provider ``data_present`` dict. Frontend renders a
    # panel iff providers is non-empty AND any data_present
    # value is true. has_sec_cik / has_filings_coverage above are
    # kept for now as a thin shim during the migration window;
    # PR 3b retires them once frontend reads ``capabilities``
    # directly.
    capabilities: dict[str, CapabilityCellPayload]


class InstrumentDetail(BaseModel):
    instrument_id: int
    symbol: str
    company_name: str
    exchange: str | None
    currency: str | None
    sector: str | None
    industry: str | None
    country: str | None
    is_tradable: bool
    first_seen_at: datetime
    last_seen_at: datetime
    coverage_tier: int | None
    session_profile: SessionProfile
    latest_quote: QuoteSnapshot | None
    external_identifiers: list[ExternalIdentifier]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_quote(row: dict[str, object]) -> QuoteSnapshot | None:
    """Extract a QuoteSnapshot from a joined row, or None if no quote exists.

    Guards on ``quoted_at``, ``bid``, and ``ask`` — all three must be non-None
    to produce a valid snapshot.  A partially-written quote row (e.g. quoted_at
    set but bid/ask NULL) returns None rather than crashing on ``float(None)``.
    """
    if row.get("quoted_at") is None or row.get("bid") is None or row.get("ask") is None:
        return None
    return QuoteSnapshot(
        bid=float(row["bid"]),  # type: ignore[arg-type]
        ask=float(row["ask"]),  # type: ignore[arg-type]
        last=float(row["last"]) if row.get("last") is not None else None,  # type: ignore[arg-type]
        spread_pct=float(row["spread_pct"]) if row.get("spread_pct") is not None else None,  # type: ignore[arg-type]
        quoted_at=row["quoted_at"],  # type: ignore[arg-type]
    )


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get("", response_model=InstrumentListResponse)
def list_instruments(
    conn: psycopg.Connection[object] = Depends(get_conn),
    search: str | None = Query(default=None, max_length=100),
    sector: str | None = Query(default=None),
    sector_spdr: str | None = Query(default=None),
    coverage_tier: int | None = Query(default=None, ge=1, le=3),
    exchange: str | None = Query(default=None),
    has_dividend: bool | None = Query(default=None),
    offset: int = Query(default=0, ge=0),
    limit: int = Query(default=50, ge=1, le=MAX_PAGE_LIMIT),
) -> InstrumentListResponse:
    """Paginated instrument list with optional filters.

    Filters:
      - search: prefix match on symbol or case-insensitive substring on
        instrument_display_name (company_name)
      - sector_spdr: exact match on the real GICS sector-SPDR resolved from the
        SEC SIC (#1675; e.g. ``XLK``). The operator-facing sector dimension.
      - sector: DEPRECATED — exact match on the opaque ``instruments.sector``
        1-9 code (no GICS meaning; SPY/XLF/JPM all share ``4``). Retained for
        back-compat; use ``sector_spdr`` instead.
      - coverage_tier: exact match (1/2/3); untiered instruments excluded
      - exchange: exact match on instruments.exchange
      - has_dividend: True matches instruments with a positive TTM dividend
        signal (dps_declared or dividends_paid > 0 in the last four reported
        quarters); False matches instruments with no such signal. Backed by
        the ``instrument_dividend_summary`` view (sql/050).

    Ordering: symbol ASC, instrument_id ASC (deterministic tiebreak).
    """
    # -- WHERE clause fragments (parameterised) ----------------------------
    where_clauses: list[str] = []
    filter_params: dict[str, object] = {}

    if search is not None:
        search = search.strip()
        if search:
            where_clauses.append("(i.symbol ILIKE %(search_prefix)s OR i.company_name ILIKE %(search_contains)s)")
            filter_params["search_prefix"] = f"{search}%"
            filter_params["search_contains"] = f"%{search}%"
    if sector is not None:
        where_clauses.append("i.sector = %(sector)s")
        filter_params["sector"] = sector
    if sector_spdr is not None:
        # SQL-side resolve (faithful mirror of resolve_sector_spdr) so the
        # filter paginates correctly. The items query already joins p; the
        # COUNT join is added below when this filter is active.
        where_clauses.append(f"({sector_spdr_case_sql()}) = %(sector_spdr)s")
        filter_params["sector_spdr"] = sector_spdr
    if coverage_tier is not None:
        where_clauses.append("c.coverage_tier = %(coverage_tier)s")
        filter_params["coverage_tier"] = coverage_tier
    if exchange is not None:
        where_clauses.append("i.exchange = %(exchange)s")
        filter_params["exchange"] = exchange
    if has_dividend is not None:
        if has_dividend:
            where_clauses.append("COALESCE(ds.has_dividend, FALSE) = TRUE")
        else:
            where_clauses.append("COALESCE(ds.has_dividend, FALSE) = FALSE")

    where_sql = (" WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

    # -- COUNT query -------------------------------------------------------
    # Only join tables that the active filters require.
    # Uses filter_params only — no limit/offset keys that the COUNT has no placeholders for.
    count_needs_coverage = coverage_tier is not None
    count_needs_dividend = has_dividend is not None
    count_join = "LEFT JOIN coverage c USING (instrument_id)" if count_needs_coverage else ""
    count_dividend_join = (
        "LEFT JOIN instrument_dividend_summary ds USING (instrument_id)" if count_needs_dividend else ""
    )
    # The sector_spdr filter's WHERE references p.sic, so the COUNT must join the
    # SEC profile when that filter is active (the items query always joins it).
    count_sec_join = "LEFT JOIN instrument_sec_profile p USING (instrument_id)" if sector_spdr is not None else ""
    count_sql = (  # noqa: S608  — hardcoded fragments only
        f"SELECT COUNT(*) AS cnt FROM instruments i {count_join} {count_dividend_join} {count_sec_join}{where_sql}"
    )

    # -- Items query -------------------------------------------------------
    # Only join the dividend-summary view when the filter needs it. The view
    # scans every instrument with any dividend row, and adding it to an
    # unrelated query (e.g. a plain ``/instruments`` call) is pure overhead.
    items_dividend_join = count_dividend_join
    items_params: dict[str, object] = {**filter_params, "limit": limit, "offset": offset}
    # Always join the SEC profile + select p.sic so every row can resolve its
    # real GICS sector for display (#1675), independent of whether the
    # sector_spdr filter is active. PK join — trivial cost.
    items_sql = f"""SELECT i.instrument_id, i.symbol, i.company_name, i.exchange,
               i.currency, i.sector, p.sic, i.is_tradable,
               c.coverage_tier,
               q.bid, q.ask, q.last, q.spread_pct, q.quoted_at
        FROM instruments i
        LEFT JOIN quotes q USING (instrument_id)
        LEFT JOIN coverage c USING (instrument_id)
        LEFT JOIN instrument_sec_profile p USING (instrument_id)
        {items_dividend_join}
        {where_sql}
        ORDER BY i.symbol, i.instrument_id
        LIMIT %(limit)s OFFSET %(offset)s"""  # noqa: S608  — hardcoded fragments only

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(count_sql, filter_params)  # type: ignore[arg-type]  # SQL built from hardcoded fragments
        count_row = cur.fetchone()
        total: int = count_row["cnt"] if count_row else 0  # type: ignore[index]

        cur.execute(items_sql, items_params)  # type: ignore[arg-type]  # SQL built from hardcoded fragments
        rows = cur.fetchall()

    items: list[InstrumentListItem] = []
    for r in rows:
        sc = resolve_sector_spdr(r.get("sic"))  # type: ignore[arg-type]
        items.append(
            InstrumentListItem(
                instrument_id=r["instrument_id"],  # type: ignore[arg-type]
                symbol=r["symbol"],  # type: ignore[arg-type]
                company_name=r["company_name"],  # type: ignore[arg-type]
                exchange=r["exchange"],  # type: ignore[arg-type]
                currency=r["currency"],  # type: ignore[arg-type]
                sector=r["sector"],  # type: ignore[arg-type]
                gics_sector=sc.gics_sector if sc is not None else None,
                sector_spdr=sc.spdr_symbol if sc is not None else None,
                is_tradable=r["is_tradable"],  # type: ignore[arg-type]
                coverage_tier=r["coverage_tier"],  # type: ignore[arg-type]
                latest_quote=_parse_quote(r),
            )
        )

    return InstrumentListResponse(items=items, total=total, offset=offset, limit=limit)


# Column sets per statement for the local financial_periods read path.
# Tuple order is the preferred display order for each statement.
_INCOME_COLUMNS: tuple[str, ...] = (
    "revenue",
    "cost_of_revenue",
    "gross_profit",
    "operating_income",
    "net_income",
    "eps_basic",
    "eps_diluted",
    "research_and_dev",
    "sga_expense",
    "depreciation_amort",
    "interest_expense",
    "income_tax",
    "shares_basic",
    "shares_diluted",
    "sbc_expense",
    # Tier 1 + Tier 2 expansion (#732). Comprehensive income (true-up
    # vs net income via OCI), separated intangible amortisation,
    # deferred-tax bridge, other non-operating, antidilutive share
    # equivalents excluded from EPS.
    "comprehensive_income",
    "intangible_amortization",
    "deferred_income_tax",
    "other_nonoperating_income",
    "antidilutive_securities",
)

_BALANCE_COLUMNS: tuple[str, ...] = (
    "total_assets",
    "total_liabilities",
    "shareholders_equity",
    "cash",
    "long_term_debt",
    "short_term_debt",
    "shares_outstanding",
    "inventory",
    "receivables",
    "payables",
    "goodwill",
    "ppe_net",
    # Ownership / capital structure (#731). Surfaced after migration
    # 088 + the matching projection in app/services/fundamentals.py.
    "treasury_shares",
    "shares_authorized",
    "shares_issued",
    "retained_earnings",
    # Tier 1 + Tier 2 expansion (#732). Working-capital + liquidity
    # additions (assets_current, liabilities_current, cash_restricted)
    # plus equity-section components (additional_paid_in_capital,
    # accumulated_oci).
    "assets_current",
    "liabilities_current",
    "cash_restricted",
    "additional_paid_in_capital",
    "accumulated_oci",
)

_CASHFLOW_COLUMNS: tuple[str, ...] = (
    "operating_cf",
    "investing_cf",
    "financing_cf",
    "capex",
    "dividends_paid",
    "dps_declared",
    "buyback_spend",
)

_STATEMENT_COLUMNS: dict[str, tuple[str, ...]] = {
    "income": _INCOME_COLUMNS,
    "balance": _BALANCE_COLUMNS,
    "cashflow": _CASHFLOW_COLUMNS,
}


def _fetch_local_financials(
    conn: psycopg.Connection[object],
    instrument_id: int,
    columns: tuple[str, ...],
    period: str,
) -> tuple[list[InstrumentFinancialRow], str | None]:
    """Read rows from ``financial_periods`` for the given statement's columns.

    ``period == 'quarterly'`` matches period_type IN ('Q1','Q2','Q3','Q4').
    ``period == 'annual'`` matches period_type = 'FY'.

    Returns (rows, currency). Empty rows = no local data; the caller
    returns an empty payload (no yfinance fallback per #498/#499 —
    SEC XBRL is the only fundamentals source for US instruments).
    """
    period_types: list[str] = ["Q1", "Q2", "Q3", "Q4"] if period == "quarterly" else ["FY"]

    # Columns whitelisted above — safe to format into SQL. period_types
    # is bound as a parameter, not formatted, so a future value added to
    # the CHECK constraint won't silently match.
    select_cols = ", ".join(columns)
    # Ordering: ``period_end_date DESC`` is the primary signal so the
    # rendered columns walk backwards through real fiscal time. The
    # endpoint is called separately for ``period_types ∈ {Q1..Q4}``
    # (quarterly view) vs ``{FY}`` (annual view) — see the call sites
    # below — so a within-row mix of FY and Q4 does not happen at the
    # API level. Tie-breakers are added defensively so a leftover
    # duplicate row that slips past the migration 076 dedupe (e.g. a
    # provider-side restatement that arrives after the one-shot purge)
    # still renders deterministically: latest ``filed_date`` wins,
    # then ``filing_event_id`` only as a final pin against a tied
    # filed_date.
    sql = f"""
        SELECT period_end_date, period_type, reported_currency, {select_cols}
        FROM financial_periods
        WHERE instrument_id = %(iid)s
          AND superseded_at IS NULL
          AND period_type = ANY(%(types)s::text[])
        ORDER BY period_end_date DESC,
                 filed_date DESC NULLS LAST,
                 fiscal_year DESC NULLS LAST,
                 fiscal_quarter DESC NULLS LAST
        LIMIT 20
    """  # noqa: S608 — columns are a hardcoded whitelist; period_types is bound

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(sql, {"iid": instrument_id, "types": period_types})  # type: ignore[arg-type]  # SQL built from hardcoded whitelist
        db_rows = cur.fetchall()

    if not db_rows:
        return [], None

    currency = db_rows[0].get("reported_currency") if db_rows else None  # type: ignore[union-attr]
    rows: list[InstrumentFinancialRow] = [
        InstrumentFinancialRow(
            period_end=r["period_end_date"],  # type: ignore[arg-type]
            period_type=str(r["period_type"]),  # type: ignore[index]
            values={col: r.get(col) for col in columns},  # type: ignore[union-attr]
        )
        for r in db_rows
    ]
    return rows, str(currency) if currency is not None else None


@router.get("/{symbol}/financials", response_model=InstrumentFinancials)
def get_instrument_financials(
    symbol: str,
    period: Literal["quarterly", "annual"] = Query(default="quarterly"),
    statement: Literal["income", "balance", "cashflow"] = Query(default="income"),
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> InstrumentFinancials:
    """Per-ticker financial statement.

    Sourced exclusively from local ``financial_periods`` rows (SEC
    XBRL-derived). Per the settled provider strategy (#498/#499 +
    #532), yfinance and paid third-party providers have no role.
    Non-US issuers without regulated-source coverage in this repo
    return an empty row list — per-region integration PRs add free
    regulated providers (Companies House, ESMA, etc.).

    Returns an empty row list (not 500, not 404) when no SEC data
    exists — the UI shows "no statement data available".
    """
    symbol_clean = symbol.strip().upper()
    if not symbol_clean:
        raise HTTPException(status_code=400, detail="symbol is required")

    columns = _STATEMENT_COLUMNS[statement]

    # Resolve symbol -> instrument_id for the local read. `symbol` is
    # not UNIQUE across exchanges (see migration 043), so order by
    # `is_primary_listing DESC, instrument_id ASC` to make the winner
    # deterministic on collisions.
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT instrument_id, symbol FROM instruments
            WHERE UPPER(symbol) = %(s)s
            ORDER BY is_primary_listing DESC, instrument_id ASC
            LIMIT 1
            """,
            {"s": symbol_clean},
        )
        inst_row = cur.fetchone()

    if inst_row is None:
        raise HTTPException(status_code=404, detail=f"Instrument {symbol} not found")

    local_rows, local_currency = _fetch_local_financials(
        conn,
        int(inst_row["instrument_id"]),  # type: ignore[arg-type]
        columns,
        period,
    )
    if local_rows:
        return InstrumentFinancials(
            symbol=str(inst_row["symbol"]),  # type: ignore[index]
            statement=statement,
            period=period,
            currency=local_currency,
            source="financial_periods",
            rows=local_rows,
        )

    # No SEC coverage → empty payload. Frontend renders the empty-
    # state hint; no fallback to a non-canonical source.
    return InstrumentFinancials(
        symbol=str(inst_row["symbol"]),  # type: ignore[index]
        statement=statement,
        period=period,
        currency=None,
        source="unavailable",
        rows=[],
    )


@router.get("/{symbol}/fcf-yield", response_model=FcfYieldSeries)
def get_instrument_fcf_yield(
    symbol: str,
    period: Literal["quarterly", "annual"] = Query(default="quarterly"),
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> FcfYieldSeries:
    """Per-period FCF-yield series for the fundamentals drill overlay (#671).

    Single-class, currency-coherent issuers get the TTM-FCF / period-end-cap
    trend. Multi-class issuers (the retired dual-class distortion #1662) and
    cross-currency issuers (no FX normaliser) are fail-closed SUPPRESSED
    (``suppressed_reason`` set, ``points`` empty); the FE keeps the absolute
    FCF line + a caveat. Policy lives in app/services/fcf_yield.py.
    """
    symbol_clean = symbol.strip().upper()
    if not symbol_clean:
        raise HTTPException(status_code=400, detail="symbol is required")

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT instrument_id, symbol FROM instruments
            WHERE UPPER(symbol) = %(s)s
            ORDER BY is_primary_listing DESC, instrument_id ASC
            LIMIT 1
            """,
            {"s": symbol_clean},
        )
        inst_row = cur.fetchone()

    if inst_row is None:
        raise HTTPException(status_code=404, detail=f"Instrument {symbol} not found")

    result = fcf_yield_series(
        conn,
        instrument_id=int(inst_row["instrument_id"]),  # type: ignore[arg-type]
        period=period,
    )
    return FcfYieldSeries(
        symbol=str(inst_row["symbol"]),  # type: ignore[index]
        suppressed_reason=result.suppressed_reason,
        points=[
            FcfYieldPoint(
                period_end=row.period_end,
                period_type=row.period_type,
                fcf_ttm=row.fcf_ttm,
                market_cap=row.market_cap,
                fcf_yield_pct=row.fcf_yield_pct,
                price=row.price,
                price_as_of=row.price_as_of,
            )
            for row in result.rows
        ],
    )


@router.get("/{symbol}/peer-comparison", response_model=PeerComparison)
def get_instrument_peer_comparison(
    symbol: str,
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> PeerComparison:
    """Peer-comparison data for the #594 radar + sector heatmap (#1751).

    Per instrument: the radar factors (P/E, ROE, revenue growth YoY, operating
    margin, debt/equity, net margin), their sector medians, and a size-proximity
    peer set — all derived server-side from existing fundamentals (no new
    ingest). A factor is ``dev_limited`` (thin: greyed + ⚠) when price-gated
    (P/E) OR its sector coverage is <20% (#1836). 404 when the instrument has no
    sector classification or no complete-TTM fundamentals. Policy lives in
    app/services/peer_comparison.py (``is_factor_thin``).
    """
    symbol_clean = symbol.strip().upper()
    if not symbol_clean:
        raise HTTPException(status_code=400, detail="symbol is required")

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT instrument_id FROM instruments
            WHERE UPPER(symbol) = %(s)s
            ORDER BY is_primary_listing DESC, instrument_id ASC
            LIMIT 1
            """,
            {"s": symbol_clean},
        )
        inst_row = cur.fetchone()

    if inst_row is None:
        raise HTTPException(status_code=404, detail=f"Instrument {symbol} not found")

    result = compute_peer_comparison(conn, instrument_id=int(inst_row["instrument_id"]))  # type: ignore[arg-type]
    if result is None:
        raise HTTPException(
            status_code=404,
            detail=f"No peer-comparison data for {symbol} (no sector classification or no complete-TTM fundamentals)",
        )

    return PeerComparison(
        symbol=result.symbol,
        instrument_id=result.instrument_id,
        sector=result.sector,
        sector_member_count=result.sector_member_count,
        factors=[
            PeerFactor(
                key=key,
                label=FACTOR_LABELS[key],
                instrument_value=result.self_factors.get(key),
                sector_median=result.medians[key].median,
                sector_n=result.medians[key].n,
                dev_limited=is_factor_thin(key, result.medians[key].n, result.sector_member_count),
                better_when=FACTOR_BETTER_WHEN[key],  # type: ignore[arg-type]
            )
            for key in FACTOR_KEYS
        ],
        peers=[
            PeerInstrument(
                instrument_id=p.instrument_id,
                symbol=p.symbol,
                company_name=p.company_name,
                size_proxy=p.total_assets,
                factors=p.factors,
            )
            for p in result.peers
        ],
    )


_CANDLE_RANGE_DAYS: dict[str, int | None] = {
    "1w": 7,
    "1m": 30,
    "3m": 90,
    "6m": 180,
    "1y": 365,
    "5y": 365 * 5,
    "max": None,
}


def _resolve_range_days(range_: str, today: date) -> int | None:
    """Map a range token to a calendar-day lookback, or None for max.

    YTD is computed dynamically from ``today`` rather than living in
    the static dict so the lookback shrinks every Jan 1. The static
    dict still holds the fixed-window tokens.
    """
    if range_ == "ytd":
        # Days from Jan 1 of `today`'s year to `today` (inclusive of
        # today's bar). Clamp to ≥1 so January 1 returns at least
        # yesterday's bar — without the clamp, Jan 1 returns 0 and the
        # chart shows an empty state on what should be a sensible
        # "single-day-into-the-year" view.
        return max(1, (today - date(today.year, 1, 1)).days)
    return _CANDLE_RANGE_DAYS[range_]


@router.get("/{symbol}/candles", response_model=InstrumentCandles)
def get_instrument_candles(
    symbol: str,
    range_: Literal["1w", "1m", "3m", "6m", "ytd", "1y", "5y", "max"] = Query(default="1m", alias="range"),
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> InstrumentCandles:
    """Daily OHLCV bars for `symbol` over the requested lookback.

    Reads from `price_daily` (populated by the market-data refresh
    job). No provider fallback here — if we don't have local bars,
    return an empty row list and let the chart render an empty state.
    A 404 is reserved for an unknown symbol.

    Range resolution is the server's responsibility: the caller passes
    a range token (`1m`, `1y`, ...) and the server maps it to a day
    lookback. `max` returns every stored bar.

    Bars are returned oldest-first so chart libraries can feed the
    array directly without re-sorting.
    """
    symbol_clean = symbol.strip().upper()
    if not symbol_clean:
        raise HTTPException(status_code=400, detail="symbol is required")

    # Symbol → instrument_id (primary-listing tiebreaker, see #357).
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT instrument_id, symbol FROM instruments
            WHERE UPPER(symbol) = %(s)s
            ORDER BY is_primary_listing DESC, instrument_id ASC
            LIMIT 1
            """,
            {"s": symbol_clean},
        )
        inst_row = cur.fetchone()

    if inst_row is None:
        raise HTTPException(status_code=404, detail=f"Instrument {symbol} not found")

    days = _resolve_range_days(range_, date.today())
    # Two fixed queries rather than f-string-composing a WHERE clause,
    # so there's no structural-injection footgun if the range-token
    # set grows later. `max` omits the date filter entirely.
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        if days is None:
            cur.execute(
                """
                SELECT price_date, open, high, low, close, volume
                FROM price_daily
                WHERE instrument_id = %(iid)s
                ORDER BY price_date ASC
                """,
                {"iid": inst_row["instrument_id"]},
            )
        else:
            cur.execute(
                """
                SELECT price_date, open, high, low, close, volume
                FROM price_daily
                WHERE instrument_id = %(iid)s
                  AND price_date >= CURRENT_DATE - make_interval(days => %(days)s)
                ORDER BY price_date ASC
                """,
                {"iid": inst_row["instrument_id"], "days": days},
            )
        rows = cur.fetchall()

    bars = [
        CandleBar(
            date=r["price_date"],  # type: ignore[arg-type]
            open=r["open"],  # type: ignore[arg-type]
            high=r["high"],  # type: ignore[arg-type]
            low=r["low"],  # type: ignore[arg-type]
            close=r["close"],  # type: ignore[arg-type]
            volume=r["volume"],  # type: ignore[arg-type]
        )
        for r in rows
    ]
    return InstrumentCandles(
        symbol=str(inst_row["symbol"]),  # type: ignore[arg-type]
        range=range_,
        days=days,
        rows=bars,
    )


@router.get(
    "/{symbol}/intraday-candles",
    response_model=InstrumentIntradayCandles,
    dependencies=[Depends(require_session_or_service_token)],
)
def get_instrument_intraday_candles(
    request: Request,
    symbol: str,
    interval: IntradayInterval = Query(default="OneMinute"),
    count: int = Query(default=390, ge=1, le=_MAX_INTRADAY_COUNT),
) -> InstrumentIntradayCandles:
    """Provider-backed intraday OHLCV bars.

    **Not** read from the local DB. Each call resolves the symbol to
    an instrument id, loads eToro broker credentials, fetches bars at
    the requested interval through the in-process TTL cache, and
    returns them. Daily / longer-horizon ranges should continue to
    use ``/candles?range=...`` which reads from ``price_daily``.

    Error mapping:
      * Unknown symbol → 404
      * Missing eToro credentials → 503 (operator must run setup)
      * Provider 429 (rate limit) → 503 with Retry-After
      * Provider 5xx / network failure → 502

    The frontend owns the range → (interval, count) translation
    table. This endpoint is intentionally count-based to mirror the
    eToro REST shape exactly.
    """
    if interval not in _VALID_INTERVALS:
        # FastAPI already validates the Literal, but keep this as a
        # belt-and-braces guard against drift between the type and
        # the validator's accepted set.
        raise HTTPException(status_code=400, detail=f"Unsupported interval {interval!r}")

    symbol_clean = symbol.strip().upper()
    if not symbol_clean:
        raise HTTPException(status_code=400, detail="symbol is required")

    # #111: dedicated audit pool from lifespan; falls back to None
    # for tests that don't set up app.state (legacy caller-conn audit).
    audit_pool = getattr(request.app.state, "audit_pool", None)

    # #1472 PR2: the pooled connection MUST NOT be held across the eToro
    # REST call below — a conn pinned across external I/O stalls a small
    # pool (block-then-PoolTimeout, max_waiting=0). Drive get_conn by hand
    # so its pool-from-state + 503 mapping (#717) is reused, do all DB
    # reads inside this scope, then release the conn via gen.close() BEFORE
    # the external call. Materialized reads (inst_row, plaintext keys)
    # survive the release — they are plain data, not cursor-backed.
    gen = get_conn(request)
    conn = next(gen)
    try:
        # Symbol → instrument_id (primary-listing tiebreaker, matches
        # the daily endpoint).
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                """
                SELECT instrument_id, symbol FROM instruments
                WHERE UPPER(symbol) = %(s)s
                ORDER BY is_primary_listing DESC, instrument_id ASC
                LIMIT 1
                """,
                {"s": symbol_clean},
            )
            inst_row = cur.fetchone()

        if inst_row is None:
            raise HTTPException(status_code=404, detail=f"Instrument {symbol} not found")

        # Load eToro credentials. #111: pass the pool so the audit row
        # is written on a side connection — durable independent of this
        # handler's transaction state, so network failures on the
        # external eToro call cannot drop the audit trail.
        try:
            op_id = sole_operator_id(conn)
        except (NoOperatorError, AmbiguousOperatorError) as exc:
            logger.warning("intraday-candles: operator lookup failed: %s", exc)
            raise HTTPException(status_code=503, detail="No operator configured") from exc

        try:
            api_key = load_credential_for_provider_use(
                conn,
                operator_id=op_id,
                provider="etoro",
                label="api_key",
                environment=settings.etoro_env,
                caller="intraday_candles_endpoint",
                audit_pool=audit_pool,
            )
            user_key = load_credential_for_provider_use(
                conn,
                operator_id=op_id,
                provider="etoro",
                label="user_key",
                environment=settings.etoro_env,
                caller="intraday_candles_endpoint",
                audit_pool=audit_pool,
            )
        except CredentialNotFound as exc:
            logger.warning("intraday-candles: %s", exc)
            raise HTTPException(
                status_code=503,
                detail="eToro credentials not configured",
            ) from exc

        symbol_out = str(inst_row["symbol"])  # type: ignore[arg-type]
        instrument_id = int(inst_row["instrument_id"])  # type: ignore[arg-type]

        # Commit the caller-conn write before releasing. load_credential_
        # for_provider_use UPDATEs broker_credentials.last_used_at on this
        # conn (caller owns the commit). gen.close() injects GeneratorExit
        # at get_conn's yield, so pool.connection()'s `with conn:` exits
        # via exception → ROLLS BACK — which would silently drop last_used_at
        # (Codex ckpt-2). Mirror validate-stored: commit before close.
        conn.commit()
    finally:
        gen.close()

    try:
        with EtoroMarketDataProvider(api_key=api_key, user_key=user_key, env=settings.etoro_env) as provider:
            bars = fetch_intraday_candles(
                provider,
                instrument_id=instrument_id,
                interval=interval,
                count=count,
            )
    except httpx.HTTPStatusError as exc:
        status = exc.response.status_code
        if status == 429:
            retry_after = exc.response.headers.get("Retry-After", "30")
            logger.warning(
                "intraday-candles: eToro rate-limited for %s, retry-after=%s",
                symbol_clean,
                retry_after,
            )
            raise HTTPException(
                status_code=503,
                detail="Rate limited upstream",
                headers={"Retry-After": retry_after},
            ) from exc
        logger.warning("intraday-candles: eToro returned %d for %s", status, symbol_clean)
        raise HTTPException(status_code=502, detail="Upstream provider error") from exc
    except httpx.RequestError as exc:
        logger.warning("intraday-candles: network error fetching %s: %s", symbol_clean, exc)
        raise HTTPException(status_code=502, detail="Upstream provider unreachable") from exc

    return InstrumentIntradayCandles(
        symbol=symbol_out,
        interval=interval,
        # Echo the actual number of bars returned, not the operator's
        # request — eToro can return fewer than `count` near market
        # open, on thinly-traded instruments, or after a fresh listing.
        # Callers reading `body.count` must see what they actually got.
        count=len(bars),
        rows=[
            IntradayBarPayload(
                timestamp=b.timestamp,
                open=b.open,
                high=b.high,
                low=b.low,
                close=b.close,
                volume=b.volume,
            )
            for b in bars
        ],
    )


# ---------------------------------------------------------------------------
# SEC entity profile (#427) — extracted from submissions.json
# ---------------------------------------------------------------------------


class FormerNameModel(BaseModel):
    name: str
    from_: str | None = None
    to: str | None = None


class InstrumentSecProfile(BaseModel):
    symbol: str
    cik: str
    sic: str | None
    sic_description: str | None
    owner_org: str | None
    description: str | None
    website: str | None
    investor_website: str | None
    ein: str | None
    lei: str | None
    state_of_incorporation: str | None
    state_of_incorporation_desc: str | None
    fiscal_year_end: str | None
    category: str | None
    exchanges: list[str]
    former_names: list[FormerNameModel]
    has_insider_issuer: bool | None
    has_insider_owner: bool | None


@router.get("/{symbol}/sec_profile", response_model=InstrumentSecProfile)
def get_instrument_sec_profile(
    symbol: str,
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> InstrumentSecProfile:
    """Return the SEC-sourced entity metadata for an instrument (#427).

    Surfaces sic / sic_description / description / website / exchanges
    / former_names / insider-activity flags from the daily submissions
    fetch. Populated for US-mapped tickers after the first
    ``fundamentals_sync`` seeds the row.

    404 when the instrument itself is unknown. 404 + ``{"detail": "no
    SEC profile"}`` when the instrument exists but no profile row has
    been seeded yet (pre-first-seed or non-US ticker without a primary
    CIK).
    """
    from app.services.sec_entity_profile import get_entity_profile

    symbol_clean = symbol.strip().upper()
    if not symbol_clean:
        raise HTTPException(status_code=400, detail="symbol is required")

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT instrument_id, symbol FROM instruments
            WHERE UPPER(symbol) = %(s)s
            ORDER BY is_primary_listing DESC, instrument_id ASC
            LIMIT 1
            """,
            {"s": symbol_clean},
        )
        inst_row = cur.fetchone()

    if inst_row is None:
        raise HTTPException(status_code=404, detail=f"Instrument {symbol} not found")

    instrument_id = int(inst_row["instrument_id"])  # type: ignore[arg-type]
    profile = get_entity_profile(conn, instrument_id=instrument_id)
    if profile is None:
        raise HTTPException(
            status_code=404,
            detail="no SEC profile on file for this instrument",
        )

    # Description = SEC submissions.json blurb only. Pre-#552 this
    # path preferred the authoritative 10-K Item 1 body and the
    # SecProfilePanel rendered the full multi-paragraph narrative
    # inline — that produced a wall-of-text on the instrument page
    # that pushed all other panels off-screen. The 10-K narrative now
    # lives behind the BusinessSectionsTeaser + drilldown route
    # (#552); the SecProfile description reverts to the short
    # submissions blurb that fits the instrument page panel.
    description = profile.description

    return InstrumentSecProfile(
        symbol=str(inst_row["symbol"]),  # type: ignore[arg-type]
        cik=profile.cik,
        sic=profile.sic,
        sic_description=profile.sic_description,
        owner_org=profile.owner_org,
        description=description,
        website=profile.website,
        investor_website=profile.investor_website,
        ein=profile.ein,
        lei=profile.lei,
        state_of_incorporation=profile.state_of_incorporation,
        state_of_incorporation_desc=profile.state_of_incorporation_desc,
        fiscal_year_end=profile.fiscal_year_end,
        category=profile.category,
        exchanges=profile.exchanges,
        former_names=[
            FormerNameModel(
                name=str(fn["name"]),
                from_=fn.get("from"),
                to=fn.get("to"),
            )
            for fn in profile.former_names
            if fn.get("name")
        ],
        has_insider_issuer=profile.has_insider_issuer,
        has_insider_owner=profile.has_insider_owner,
    )


# ---------------------------------------------------------------------------
# Headcount endpoint (#551 — XBRL ``dei:EntityNumberOfEmployees``)
# ---------------------------------------------------------------------------


class InstrumentHeadcount(BaseModel):
    """Most-recent reported employee count for one instrument.

    Sourced from the SEC iXBRL ``dei:EntityNumberOfEmployees`` fact
    via ``financial_facts_raw``. Block-tagging mandate post-2020 means
    near-complete coverage on US issuers — the panel is silent (404)
    when the instrument has no SEC CIK or no DEI fact ingested yet.
    """

    symbol: str
    employees: int
    period_end_date: date
    source_accession: str


@router.get("/{symbol}/employees", response_model=InstrumentHeadcount | None)
def get_instrument_employees(
    symbol: str,
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> InstrumentHeadcount | None:
    """Latest ``dei:EntityNumberOfEmployees`` fact for an instrument.

    Returns **200 with a null body** when the instrument exists but has
    no headcount fact on file — which is the common case: only ~16 of
    ~5,200 instruments XBRL-tag ``dei:EntityNumberOfEmployees`` (it is an
    optional concept; AAPL/GME and most majors report headcount as 10-K
    narrative text, not a structured fact — verified against SEC
    companyfacts, #1813). A 404 is reserved for an *unknown* symbol, so
    the FE can fetch this unconditionally without polluting the browser
    console with an error on every instrument page (#1813).
    """
    symbol_clean = symbol.strip().upper()
    if not symbol_clean:
        raise HTTPException(status_code=400, detail="symbol is required")

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT instrument_id, symbol FROM instruments
            WHERE UPPER(symbol) = %(s)s
            ORDER BY is_primary_listing DESC, instrument_id ASC
            LIMIT 1
            """,
            {"s": symbol_clean},
        )
        inst_row = cur.fetchone()
    if inst_row is None:
        raise HTTPException(status_code=404, detail=f"Instrument {symbol} not found")
    instrument_id = int(inst_row["instrument_id"])  # type: ignore[arg-type]

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT period_end, val, accession_number
              FROM financial_facts_raw
             WHERE instrument_id = %s
               AND concept = 'EntityNumberOfEmployees'
             ORDER BY period_end DESC, fetched_at DESC
             LIMIT 1
            """,
            (instrument_id,),
        )
        row = cur.fetchone()

    if row is None:
        # Known instrument, no headcount fact — absent optional datum, not
        # an error. 200 + null keeps the instrument page console clean
        # (#1813). Unknown-symbol still 404s above (line ~1439).
        return None

    return InstrumentHeadcount(
        symbol=str(inst_row["symbol"]),  # type: ignore[arg-type]
        employees=int(row["val"]),  # type: ignore[arg-type]
        period_end_date=row["period_end"],  # type: ignore[arg-type]
        source_accession=str(row["accession_number"]),
    )


# ---------------------------------------------------------------------------
# Segments endpoint (#554 — dimensional XBRL facts)
# ---------------------------------------------------------------------------


# API axis param → storage enum (pinned by test so storage values never
# leak into the URL surface).
SEGMENT_AXIS_PARAM_TO_ENUM: dict[str, DimensionalAxis] = {
    "business": "business_segment",
    "product": "product_service",
    "geographic": "geographic",
}

SegmentAxisParam = Literal["business", "product", "geographic"]


class SegmentRowModel(BaseModel):
    member_qname: str
    member_label: str
    revenue: float | None
    operating_income: float | None
    assets: float | None
    # Share of the summed leaf revenue in this response. Leaf rows sum
    # to the consolidated figure (subtotal members are excluded at the
    # reader), so this is internally consistent by construction.
    pct_of_total: float | None


class InstrumentSegments(BaseModel):
    """Latest-fiscal-year dimensional breakdown for one instrument.

    ``sources`` maps metric → winning accession: the reader selects the
    winning filing per (axis, metric) independently, so a 10-K/A that
    restates revenue but omits operating income pairs amendment revenue
    with original-filing operating income (spec §D4/D6).
    """

    symbol: str
    axis: SegmentAxisParam
    period_end: date
    filed_at: datetime
    sources: dict[str, str]
    total_revenue: float | None
    rows: list[SegmentRowModel]


@router.get("/{symbol}/segments", response_model=InstrumentSegments)
def get_instrument_segments(
    symbol: str,
    axis: SegmentAxisParam = "business",
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> InstrumentSegments:
    """Latest-FY segment / product / geographic breakdown (#554).

    Returns 404 when no dimensional facts are on file for the axis —
    non-SEC issuer, the 10-K predates the XBRL mandate, or the filer
    discloses nothing on the axis (e.g. banks often emit no
    revenue-alias facts on the product axis).
    """
    symbol_clean = symbol.strip().upper()
    if not symbol_clean:
        raise HTTPException(status_code=400, detail="symbol is required")

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT instrument_id, symbol FROM instruments
            WHERE UPPER(symbol) = %(s)s
            ORDER BY is_primary_listing DESC, instrument_id ASC
            LIMIT 1
            """,
            {"s": symbol_clean},
        )
        inst_row = cur.fetchone()
    if inst_row is None:
        raise HTTPException(status_code=404, detail=f"Instrument {symbol} not found")
    instrument_id = int(inst_row["instrument_id"])  # type: ignore[arg-type]

    result = read_segments(
        conn,  # type: ignore[arg-type] — reader opens its own dict_row cursors
        instrument_id=instrument_id,
        axis=SEGMENT_AXIS_PARAM_TO_ENUM[axis],
    )
    if not result.rows or result.period_end is None or result.filed_at is None:
        raise HTTPException(
            status_code=404,
            detail=f"No {axis} breakdown on file for {symbol}",
        )

    total_revenue = sum(
        (row["revenue"] for row in result.rows if row["revenue"] is not None),
        start=Decimal(0),
    )
    rows = [
        SegmentRowModel(
            member_qname=row["member_qname"],
            member_label=row["member_label"],
            revenue=float(row["revenue"]) if row["revenue"] is not None else None,
            operating_income=float(row["operating_income"]) if row["operating_income"] is not None else None,
            assets=float(row["assets"]) if row["assets"] is not None else None,
            pct_of_total=(
                float(row["revenue"] / total_revenue) if row["revenue"] is not None and total_revenue > 0 else None
            ),
        )
        for row in result.rows
    ]
    return InstrumentSegments(
        symbol=str(inst_row["symbol"]),  # type: ignore[arg-type]
        axis=axis,
        period_end=result.period_end,
        filed_at=result.filed_at,
        sources=dict(result.sources),
        total_revenue=float(total_revenue) if total_revenue > 0 else None,
        rows=rows,
    )


# ---------------------------------------------------------------------------
# 8-K structured-events endpoint (#450)
# ---------------------------------------------------------------------------


class EightKItemModel(BaseModel):
    item_code: str
    item_label: str
    severity: str | None
    body: str


class EightKExhibitModel(BaseModel):
    exhibit_number: str
    description: str | None


class EightKFilingModel(BaseModel):
    accession_number: str
    document_type: str
    is_amendment: bool
    date_of_report: date | None
    reporting_party: str | None
    signature_name: str | None
    signature_title: str | None
    signature_date: date | None
    primary_document_url: str | None
    body_deferred: bool = False
    items: list[EightKItemModel]
    exhibits: list[EightKExhibitModel]


class EightKFilingsResponse(BaseModel):
    symbol: str
    filings: list[EightKFilingModel]


# Per-endpoint allowed `provider` values. Frontend hooks pass the
# capability's resolved provider tag so the endpoint can route to the
# right backend once non-SEC sources land. Today every endpoint only
# wires a single provider; per-region integration PRs add more.
_EIGHT_K_PROVIDERS: tuple[str, ...] = ("sec_8k_events",)
_DIVIDEND_PROVIDERS: tuple[str, ...] = ("sec_dividend_summary",)
_INSIDER_PROVIDERS: tuple[str, ...] = ("sec_form4",)
# Form 3 baseline (#768 PR 4) — separate tuple so a stray
# ``?provider=sec_form3`` on the Form 4 endpoint fails validation
# rather than silently passing through to a reader that doesn't
# consume it. PR #774 review caught the cross-contamination risk.
_INSIDER_BASELINE_PROVIDERS: tuple[str, ...] = ("sec_form3",)
_DEF14A_PROVIDERS: tuple[str, ...] = ("sec_def14a",)


def _validate_provider(provider: str | None, allowed: tuple[str, ...]) -> None:
    """Reject an unknown ``?provider=`` value.

    ``provider=None`` (param omitted) falls back to the endpoint's
    historical default behaviour for backward compatibility — every
    existing caller pre-#515 PR 3b passes nothing. New frontend hooks
    always pass the capability-resolved provider tag explicitly.
    """
    if provider is None:
        return
    if provider not in allowed:
        raise HTTPException(
            status_code=400,
            detail=f"unsupported provider {provider!r}; allowed: {list(allowed)}",
        )


@router.get("/{symbol}/eight_k_filings", response_model=EightKFilingsResponse)
def get_instrument_8k_filings(
    symbol: str,
    limit: int = 50,
    provider: str | None = Query(
        default=None,
        description="Capability provider tag. Today only 'sec_8k_events'.",
    ),
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> EightKFilingsResponse:
    """Return recent 8-K filings for an instrument with full structured
    item bodies + exhibit pointers (#450)."""
    from app.services.eight_k_events import list_8k_filings

    _validate_provider(provider, _EIGHT_K_PROVIDERS)

    symbol_clean = symbol.strip().upper()
    if not symbol_clean:
        raise HTTPException(status_code=400, detail="symbol is required")
    if limit <= 0 or limit > 500:
        raise HTTPException(status_code=400, detail="limit must be between 1 and 500")

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT instrument_id, symbol FROM instruments
            WHERE UPPER(symbol) = %(s)s
            ORDER BY is_primary_listing DESC, instrument_id ASC
            LIMIT 1
            """,
            {"s": symbol_clean},
        )
        inst_row = cur.fetchone()

    if inst_row is None:
        raise HTTPException(status_code=404, detail=f"Instrument {symbol} not found")

    instrument_id = int(inst_row["instrument_id"])  # type: ignore[arg-type]
    if not _has_sec_cik(conn, instrument_id):
        # No SEC CIK means no canonical 8-K source — return empty
        # rather than render orphan rows from a prior bad CIK link
        # (see migration 066 / spec PR 2).
        raise HTTPException(status_code=404, detail=f"Instrument {symbol} has no SEC coverage")
    filings = list_8k_filings(conn, instrument_id=instrument_id, limit=limit)
    return EightKFilingsResponse(
        symbol=str(inst_row["symbol"]),  # type: ignore[arg-type]
        filings=[
            EightKFilingModel(
                accession_number=f.accession_number,
                document_type=f.document_type,
                is_amendment=f.is_amendment,
                date_of_report=f.date_of_report,
                reporting_party=f.reporting_party,
                signature_name=f.signature_name,
                signature_title=f.signature_title,
                signature_date=f.signature_date,
                primary_document_url=f.primary_document_url,
                body_deferred=f.body_deferred,
                items=[
                    EightKItemModel(
                        item_code=i.item_code,
                        item_label=i.item_label,
                        severity=i.severity,
                        body=i.body,
                    )
                    for i in f.items
                ],
                exhibits=[
                    EightKExhibitModel(
                        exhibit_number=e.exhibit_number,
                        description=e.description,
                    )
                    for e in f.exhibits
                ],
            )
            for f in filings
        ],
    )


@router.get(
    "/{symbol}/eight_k_filings/{accession_number}/body",
    response_model=EightKFilingModel,
)
def get_instrument_8k_filing_body(
    symbol: str,
    accession_number: str,
    response: Response,
    request: Request,
) -> EightKFilingModel:
    """Lazily fetch + return one 8-K filing's bodies + exhibits (#1343).

    The events rail seeds 8-K metadata (item codes + dates) at bootstrap
    with the body deferred. Opening a filing's detail calls this: if the
    row is ``body_deferred``, fetch the primary document, parse, cache,
    and return the now-complete filing; an already-fetched filing returns
    immediately (idempotent). Side-effecting GET by design (#1343).

    Error contract: a deterministic failure (404 / parse-miss) tombstones
    and returns the (empty) filing with 200 so the FE renders an empty
    state, not an error toast; a transient fetch error → 503.

    Connection discipline (#1472 PR2 V3): drives ``get_conn`` by hand via
    :func:`_short_lived_conn` so the pooled conn is released BEFORE the SEC
    fetch — the lazy-fill service borrows its own short-lived pool conns
    and holds none across the external I/O.
    """
    from app.providers.implementations.sec_edgar import SecFilingsProvider
    from app.services.eight_k_events import fetch_eight_k_body_now, get_8k_filing

    response.headers["Cache-Control"] = "no-store"
    symbol_clean = symbol.strip().upper()
    if not symbol_clean:
        raise HTTPException(status_code=400, detail="symbol is required")
    accession_clean = accession_number.strip()
    if not accession_clean:
        raise HTTPException(status_code=400, detail="accession_number is required")

    # Pre-reads on a short borrow, released BEFORE the SEC fetch: resolve
    # the instrument + scope the accession to it via the filing_events
    # bridge so a URL-manipulated cross-issuer accession can't be fetched
    # or returned (Codex ckpt2). 404 if the accession isn't this issuer's.
    with _short_lived_conn(request) as conn:
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                """
                SELECT instrument_id FROM instruments
                WHERE UPPER(symbol) = %(s)s
                ORDER BY is_primary_listing DESC, instrument_id ASC
                LIMIT 1
                """,
                {"s": symbol_clean},
            )
            inst_row = cur.fetchone()
        if inst_row is None:
            raise HTTPException(status_code=404, detail=f"Instrument {symbol} not found")
        instrument_id = int(inst_row["instrument_id"])  # type: ignore[arg-type]

        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM filing_events WHERE provider = 'sec' "
                "AND provider_filing_id = %s AND instrument_id = %s "
                "AND filing_type IN ('8-K', '8-K/A') LIMIT 1",
                (accession_clean, instrument_id),
            )
            if cur.fetchone() is None:
                raise HTTPException(
                    status_code=404,
                    detail=f"8-K filing {accession_clean} not found for {symbol}",
                )

    # Lazy-fill holds NO pooled conn across the SEC fetch — the service
    # borrows its own short-lived conns from the pool (#1472 PR2 V3).
    try:
        with SecFilingsProvider(user_agent=settings.sec_user_agent) as provider:
            fetch_eight_k_body_now(request.app.state.db_pool, provider, accession_number=accession_clean)
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001 — transient fetch / network → 503
        raise HTTPException(
            status_code=503,
            detail="8-K body temporarily unavailable; try again",
        ) from exc

    with _short_lived_conn(request) as conn:
        filing = get_8k_filing(conn, accession_number=accession_clean)
    if filing is None:
        raise HTTPException(status_code=404, detail=f"8-K filing {accession_clean} not found")
    return EightKFilingModel(
        accession_number=filing.accession_number,
        document_type=filing.document_type,
        is_amendment=filing.is_amendment,
        date_of_report=filing.date_of_report,
        reporting_party=filing.reporting_party,
        signature_name=filing.signature_name,
        signature_title=filing.signature_title,
        signature_date=filing.signature_date,
        primary_document_url=filing.primary_document_url,
        body_deferred=filing.body_deferred,
        items=[
            EightKItemModel(
                item_code=i.item_code,
                item_label=i.item_label,
                severity=i.severity,
                body=i.body,
            )
            for i in filing.items
        ],
        exhibits=[
            EightKExhibitModel(
                exhibit_number=e.exhibit_number,
                description=e.description,
            )
            for e in filing.exhibits
        ],
    )


# ---------------------------------------------------------------------------
# 10-K Item 1 subsection breakdown (#449)
# ---------------------------------------------------------------------------


class BusinessCrossReferenceModel(BaseModel):
    reference_type: str
    target: str
    context: str


class BusinessTableModel(BaseModel):
    order: int
    headers: list[str]
    rows: list[list[str]]


class BusinessSectionModel(BaseModel):
    section_order: int
    section_key: str
    section_label: str
    body: str
    cross_references: list[BusinessCrossReferenceModel]
    tables: list[BusinessTableModel] = []


class BusinessSectionsParseStatus(BaseModel):
    """Why ``sections`` is empty (#648).

    Populated only when ``sections`` is empty. Lets the operator UI
    distinguish "the parser hasn't run yet" from "the parser tried
    and failed" from "the filing genuinely has no Item 1" — all of
    which used to render as the same opaque "No 10-K Item 1 on file"
    empty state.

    ``state``:
      * ``not_attempted`` — no parent ``instrument_business_summary``
        row exists. The ingester hasn't visited this instrument yet.
      * ``parse_failed`` — parent row body is empty + an explicit
        ``last_failure_reason`` from the FailureReason taxonomy is
        set. Includes ``failure_reason``, ``next_retry_at``, and
        ``last_attempted_at``.
      * ``no_item_1`` — parent row body is empty AND the failure
        reason was ``no_item_1_marker`` (or a body-too-short slice
        from the same Item 1 absence). The 10-K filed by this issuer
        doesn't have a parseable Item 1 — common for 10-K/A Part-III
        amendments. Distinct from generic ``parse_failed`` so the
        operator doesn't waste time investigating a fix.
      * ``sections_pending`` — parent row body is non-empty (Item 1
        was extracted) but the section splitter hasn't written
        children yet. Should be transient.
    """

    state: Literal["not_attempted", "parse_failed", "no_item_1", "sections_pending", "deferred"]
    failure_reason: str | None = None
    next_retry_at: datetime | None = None
    last_attempted_at: datetime | None = None


class BusinessSectionsResponse(BaseModel):
    """Response payload for ``/instruments/{symbol}/business_sections``.

    ``sections`` is ordered by the source 10-K layout (section_order).
    ``source_accession`` identifies the 10-K the sections were extracted
    from, so the UI can link back to the SEC filing. Empty list when
    no sections are on file (first-time instruments or no 10-K filed).

    ``cik`` is the SEC entity CIK for the instrument, plumbed through
    so the frontend can build direct iXBRL viewer URLs
    (``cgi-bin/viewer?cik=...&accession_number=...``) without an
    EDGAR search redirect (#563). NULL for instruments without a
    primary SEC CIK link (non-US tickers, crypto, etc.).

    ``parse_status`` (#648) explains WHY ``sections`` is empty when
    it is. NULL when ``sections`` has any content.
    """

    symbol: str
    source_accession: str | None
    cik: str | None
    sections: list[BusinessSectionModel]
    parse_status: BusinessSectionsParseStatus | None = None


@router.get(
    "/{symbol}/business_sections",
    response_model=BusinessSectionsResponse,
)
def get_instrument_business_sections(
    symbol: str,
    response: Response,
    request: Request,
    accession: str | None = Query(
        None,
        description="Specific 10-K accession; omit for the latest filing.",
    ),
) -> BusinessSectionsResponse:
    """Return the 10-K Item 1 subsection breakdown for an instrument (#449).

    Every subsection from the latest 10-K lands as its own row with a
    canonical ``section_key`` (stable identifier) + the verbatim
    ``section_label`` from the filing + the body text + cross-references
    to other items / exhibits / notes. Headings that don't match a known
    canonical key surface as ``section_key='other'`` with the original
    heading preserved — no silent drops.

    Pass ``?accession=<accession_number>`` to retrieve sections for a
    specific historical filing. Returns 404 when no sections exist for
    the requested accession (#559).

    Connection discipline (#1472 PR2 V4): drives ``get_conn`` by hand via
    :func:`_short_lived_conn` so the pooled conn is released BEFORE the SEC
    fetch — the lazy-fill service borrows its own short-lived pool conns
    and holds none across the external I/O. The route reads on two scoped
    borrows (decide → fill → re-read/classify) bracketing the fill.
    """
    from app.services.business_summary import (
        fetch_business_summary_body_now,
        get_business_sections,
        get_parse_status,
    )

    symbol_clean = symbol.strip().upper()
    if not symbol_clean:
        raise HTTPException(status_code=400, detail="symbol is required")

    # Borrow #1 (released BEFORE any SEC fetch): resolve the instrument,
    # read current sections, decide whether a lazy fill is needed.
    with _short_lived_conn(request) as conn:
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                """
                SELECT instrument_id, symbol FROM instruments
                WHERE UPPER(symbol) = %(s)s
                ORDER BY is_primary_listing DESC, instrument_id ASC
                LIMIT 1
                """,
                {"s": symbol_clean},
            )
            inst_row = cur.fetchone()
        if inst_row is None:
            raise HTTPException(status_code=404, detail=f"Instrument {symbol} not found")
        instrument_id = int(inst_row["instrument_id"])  # type: ignore[arg-type]
        symbol_out = str(inst_row["symbol"])  # type: ignore[arg-type]
        sections = get_business_sections(conn, instrument_id=instrument_id, accession=accession)
        if accession is not None and not sections:
            raise HTTPException(
                status_code=404,
                detail=f"no 10-K sections for {symbol} accession {accession}",
            )
        # #1343 — lazy fill on first view when the latest-filing path is
        # empty because the 10-K Item 1 is a deferred bootstrap placeholder.
        need_fill = False
        if not sections and accession is None:
            ps_pre = get_parse_status(conn, instrument_id=instrument_id)
            need_fill = ps_pre is not None and ps_pre.state == "deferred"

    # Lazy fill (~0.5-1s first load; instant + cached after) holds NO pooled
    # conn across the SEC fetch — the service borrows its own short-lived
    # conns from the pool (#1472 PR2 V4). A transient fetch error → 503 (the
    # row stays deferred, so a retry / next view re-attempts); a
    # deterministic miss does NOT raise (exits deferred via the backoff
    # path) and re-reads as parse_failed / no_item_1 below — a 200 empty.
    if need_fill:
        from app.providers.implementations.sec_edgar import SecFilingsProvider

        # Side-effecting fill — don't let an intermediary cache the
        # transient deferred/empty state (Codex ckpt2).
        response.headers["Cache-Control"] = "no-store"
        try:
            with SecFilingsProvider(user_agent=settings.sec_user_agent) as provider:
                fetch_business_summary_body_now(request.app.state.db_pool, provider, instrument_id=instrument_id)
        except HTTPException:
            raise
        except Exception as exc:  # noqa: BLE001 — transient fetch / network → 503
            raise HTTPException(
                status_code=503,
                detail="10-K Item 1 temporarily unavailable; try again",
            ) from exc

    # Borrow #2 (post-fill): re-read sections if we filled, classify the
    # empty case (#648), and plumb the CIK (#563).
    parse_status_model: BusinessSectionsParseStatus | None = None
    with _short_lived_conn(request) as conn:
        if need_fill:
            sections = get_business_sections(conn, instrument_id=instrument_id, accession=accession)
        # #648 — classify why the latest-filing path is empty so the UI can
        # render distinct empty states (only on the accession=None path —
        # an explicit-accession miss already 404'd above).
        if not sections and accession is None:
            ps = get_parse_status(conn, instrument_id=instrument_id)
            if ps is not None:
                parse_status_model = BusinessSectionsParseStatus(
                    # The Literal narrows it for the response model — the
                    # service-layer dataclass uses str so the service
                    # tests don't pull in fastapi.
                    state=ps.state,  # type: ignore[arg-type]
                    failure_reason=ps.failure_reason,
                    next_retry_at=ps.next_retry_at,  # type: ignore[arg-type]
                    last_attempted_at=ps.last_attempted_at,  # type: ignore[arg-type]
                )

        # #563: plumb CIK so the frontend can build direct iXBRL viewer
        # URLs. Single SELECT against the existing primary SEC link;
        # returns None for instruments without one (non-US tickers).
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                "SELECT identifier_value FROM external_identifiers "
                "WHERE instrument_id = %(iid)s AND provider = 'sec' "
                "AND identifier_type = 'cik' AND is_primary = TRUE "
                "LIMIT 1",
                {"iid": instrument_id},
            )
            cik_row = cur.fetchone()
        cik = str(cik_row["identifier_value"]) if cik_row is not None else None  # type: ignore[index]

    source_accession = sections[0].source_accession if sections else None

    return BusinessSectionsResponse(
        symbol=symbol_out,
        source_accession=source_accession,
        cik=cik,
        sections=[
            BusinessSectionModel(
                section_order=s.section_order,
                section_key=s.section_key,
                section_label=s.section_label,
                body=s.body,
                cross_references=[
                    BusinessCrossReferenceModel(
                        reference_type=ref.reference_type,
                        target=ref.target,
                        context=ref.context,
                    )
                    for ref in s.cross_references
                ],
                tables=[
                    BusinessTableModel(
                        order=tbl.order,
                        headers=list(tbl.headers),
                        rows=[list(row) for row in tbl.rows],
                    )
                    for tbl in s.tables
                ],
            )
            for s in sections
        ],
        parse_status=parse_status_model,
    )


# ---------------------------------------------------------------------------
# 10-K filing history (#559)
# ---------------------------------------------------------------------------


class TenKHistoryFilingModel(BaseModel):
    accession_number: str
    filing_date: date
    filing_type: str


class TenKHistoryResponse(BaseModel):
    symbol: str
    filings: list[TenKHistoryFilingModel]


@router.get(
    "/{symbol}/filings/10-k/history",
    response_model=TenKHistoryResponse,
)
def get_instrument_tenk_history(
    symbol: str,
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> TenKHistoryResponse:
    """Return the list of 10-K and 10-K/A filings for an instrument in
    reverse chronological order (#559).

    Used by the prior-10-Ks rail on the drilldown page so the user can
    navigate to any historical annual report.
    """
    from app.services.business_summary import list_10k_history

    symbol_clean = symbol.strip().upper()
    if not symbol_clean:
        raise HTTPException(status_code=400, detail="symbol is required")

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT instrument_id, symbol FROM instruments
            WHERE UPPER(symbol) = %(s)s
            ORDER BY is_primary_listing DESC, instrument_id ASC
            LIMIT 1
            """,
            {"s": symbol_clean},
        )
        inst_row = cur.fetchone()

    if inst_row is None:
        raise HTTPException(status_code=404, detail=f"Instrument {symbol} not found")

    instrument_id = int(inst_row["instrument_id"])  # type: ignore[arg-type]
    filings = list_10k_history(conn, instrument_id=instrument_id)
    return TenKHistoryResponse(
        symbol=str(inst_row["symbol"]),  # type: ignore[arg-type]
        filings=[
            TenKHistoryFilingModel(
                accession_number=f.accession_number,
                filing_date=f.filing_date,
                filing_type=f.filing_type,
            )
            for f in filings
        ],
    )


# ---------------------------------------------------------------------------
# Dilution + share-count history (#435)
# ---------------------------------------------------------------------------


class ShareCountPeriodModel(BaseModel):
    period_end: date
    fiscal_year: int | None
    fiscal_period: str | None
    shares_outstanding: Decimal | None
    shares_issued_new: Decimal | None
    buyback_shares: Decimal | None


class DilutionSummaryModel(BaseModel):
    latest_shares: Decimal | None
    latest_as_of: date | None
    yoy_shares: Decimal | None
    net_dilution_pct_yoy: Decimal | None
    ttm_shares_issued: Decimal | None
    ttm_buyback_shares: Decimal | None
    ttm_net_share_change: Decimal | None
    dilution_posture: Literal["dilutive", "buyback_heavy", "stable"]


class InstrumentDilution(BaseModel):
    symbol: str
    summary: DilutionSummaryModel
    history: list[ShareCountPeriodModel]


@router.get("/{symbol}/dilution", response_model=InstrumentDilution)
def get_instrument_dilution(
    symbol: str,
    limit: int = Query(default=40, ge=1, le=200),
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> InstrumentDilution:
    """Per-period share count + TTM dilution summary (#435).

    Source: SEC XBRL facts already ingested via the daily
    ``fundamentals_sync`` path (``StockIssuedDuringPeriodSharesNewIssues``,
    ``StockRepurchasedDuringPeriodShares``, ``CommonStockSharesOutstanding``,
    ``dei:EntityCommonStockSharesOutstanding``). Returns the
    ``stable`` empty shape for never-seeded / non-US tickers — UI
    renders an empty state without 404 handling.

    Default ``limit=40`` covers ten years of quarterly history.
    """
    from app.services.dilution import get_dilution_summary, get_share_count_history

    symbol_clean = symbol.strip().upper()
    if not symbol_clean:
        raise HTTPException(status_code=400, detail="symbol is required")

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT instrument_id, symbol FROM instruments
            WHERE UPPER(symbol) = %(s)s
            ORDER BY is_primary_listing DESC, instrument_id ASC
            LIMIT 1
            """,
            {"s": symbol_clean},
        )
        inst_row = cur.fetchone()

    if inst_row is None:
        raise HTTPException(status_code=404, detail=f"Instrument {symbol} not found")

    instrument_id = int(inst_row["instrument_id"])  # type: ignore[arg-type]
    summary = get_dilution_summary(conn, instrument_id=instrument_id)
    history = get_share_count_history(conn, instrument_id=instrument_id, limit=limit)

    return InstrumentDilution(
        symbol=str(inst_row["symbol"]),  # type: ignore[arg-type]
        summary=DilutionSummaryModel(
            latest_shares=summary.latest_shares,
            latest_as_of=summary.latest_as_of,
            yoy_shares=summary.yoy_shares,
            net_dilution_pct_yoy=summary.net_dilution_pct_yoy,
            ttm_shares_issued=summary.ttm_shares_issued,
            ttm_buyback_shares=summary.ttm_buyback_shares,
            ttm_net_share_change=summary.ttm_net_share_change,
            dilution_posture=summary.dilution_posture,
        ),
        history=[
            ShareCountPeriodModel(
                period_end=p.period_end,
                fiscal_year=p.fiscal_year,
                fiscal_period=p.fiscal_period,
                shares_outstanding=p.shares_outstanding,
                shares_issued_new=p.shares_issued_new,
                buyback_shares=p.buyback_shares,
            )
            for p in history
        ],
    )


# ---------------------------------------------------------------------------
# Dividend history + summary (#414 follow-up, operator ask 2026-04-24)
# ---------------------------------------------------------------------------


class DividendPeriodModel(BaseModel):
    period_end_date: date
    period_type: str
    fiscal_year: int
    fiscal_quarter: int | None
    dps_declared: Decimal | None
    dividends_paid: Decimal | None
    reported_currency: str | None


class DividendSummaryModel(BaseModel):
    has_dividend: bool
    ttm_dps: Decimal | None
    ttm_dividends_paid: Decimal | None
    ttm_yield_pct: Decimal | None
    latest_dps: Decimal | None
    latest_dividend_at: date | None
    dividend_streak_q: int
    dividend_currency: str | None


class UpcomingDividendModel(BaseModel):
    source_accession: str
    declaration_date: date | None
    ex_date: date | None
    record_date: date | None
    pay_date: date | None
    dps_declared: Decimal | None
    currency: str


class InstrumentDividends(BaseModel):
    symbol: str
    summary: DividendSummaryModel
    history: list[DividendPeriodModel]
    upcoming: list[UpcomingDividendModel]


@router.get("/{symbol}/dividends", response_model=InstrumentDividends)
def get_instrument_dividends(
    symbol: str,
    limit: int = Query(default=40, ge=1, le=400),
    provider: str | None = Query(
        default=None,
        description="Capability provider tag. Today only 'sec_dividend_summary'.",
    ),
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> InstrumentDividends:
    """Dividend history + TTM yield summary for a single instrument.

    Source: SEC XBRL ``us-gaap:CommonStockDividendsPerShareDeclared`` +
    ``us-gaap:PaymentsOfDividends``, already ingested via the daily
    companyfacts path. Returns ``has_dividend=False`` with an empty
    history array for instruments that have never reported a dividend
    — the UI can render an empty state without 404 handling.

    Default ``limit=40`` covers ten years of quarterly history.
    """
    from app.services.dividends import (
        get_dividend_history,
        get_dividend_summary,
        get_upcoming_dividends,
    )

    _validate_provider(provider, _DIVIDEND_PROVIDERS)

    symbol_clean = symbol.strip().upper()
    if not symbol_clean:
        raise HTTPException(status_code=400, detail="symbol is required")

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT instrument_id, symbol FROM instruments
            WHERE UPPER(symbol) = %(s)s
            ORDER BY is_primary_listing DESC, instrument_id ASC
            LIMIT 1
            """,
            {"s": symbol_clean},
        )
        inst_row = cur.fetchone()

    if inst_row is None:
        raise HTTPException(status_code=404, detail=f"Instrument {symbol} not found")

    instrument_id = int(inst_row["instrument_id"])  # type: ignore[arg-type]
    if not _has_sec_cik(conn, instrument_id):
        # No SEC CIK = no canonical dividend source. 404 instead
        # of returning orphan rows from a prior bad CIK link
        # (see migration 066 / spec PR 2).
        raise HTTPException(status_code=404, detail=f"Instrument {symbol} has no SEC coverage")
    summary = get_dividend_summary(conn, instrument_id=instrument_id)
    history = get_dividend_history(conn, instrument_id=instrument_id, limit=limit)
    upcoming = get_upcoming_dividends(conn, instrument_id=instrument_id)

    return InstrumentDividends(
        symbol=str(inst_row["symbol"]),  # type: ignore[arg-type]
        summary=DividendSummaryModel(
            has_dividend=summary.has_dividend,
            ttm_dps=summary.ttm_dps,
            ttm_dividends_paid=summary.ttm_dividends_paid,
            ttm_yield_pct=summary.ttm_yield_pct,
            latest_dps=summary.latest_dps,
            latest_dividend_at=summary.latest_dividend_at,
            dividend_streak_q=summary.dividend_streak_q,
            dividend_currency=summary.dividend_currency,
        ),
        history=[
            DividendPeriodModel(
                period_end_date=p.period_end_date,
                period_type=p.period_type,
                fiscal_year=p.fiscal_year,
                fiscal_quarter=p.fiscal_quarter,
                dps_declared=p.dps_declared,
                dividends_paid=p.dividends_paid,
                reported_currency=p.reported_currency,
            )
            for p in history
        ],
        upcoming=[
            UpcomingDividendModel(
                source_accession=u.source_accession,
                declaration_date=u.declaration_date,
                ex_date=u.ex_date,
                record_date=u.record_date,
                pay_date=u.pay_date,
                dps_declared=u.dps_declared,
                currency=u.currency,
            )
            for u in upcoming
        ],
    )


class InsiderSummaryModel(BaseModel):
    """90-day insider-activity summary with two lenses (#458).

    Open-market fields (``open_market_*``) capture discretionary P/S
    trading — the strongest sentiment signal. Total-activity fields
    (``total_acquired_*`` / ``total_disposed_*``) capture every
    non-derivative transaction classified by
    ``acquired_disposed_code`` (or ``txn_code`` when SEC omitted the
    A/D flag). Operators need both: an RSU-vest month can show zero
    open-market buys alongside a large grant, and a summary that
    only reports open-market activity would misleadingly imply the
    insider disposed of shares on balance.
    """

    symbol: str
    open_market_net_shares_90d: Decimal
    open_market_buy_count_90d: int
    open_market_sell_count_90d: int
    total_acquired_shares_90d: Decimal
    total_disposed_shares_90d: Decimal
    acquisition_count_90d: int
    disposition_count_90d: int
    unique_filers_90d: int
    latest_txn_date: date | None
    # Back-compat aliases for callers built against the pre-#458 shape.
    # Mirror the primary open-market fields so existing consumers keep
    # rendering. Remove after all callers migrate.
    net_shares_90d: Decimal
    buy_count_90d: int
    sell_count_90d: int


@router.get("/{symbol}/insider_summary", response_model=InsiderSummaryModel)
def get_instrument_insider_summary(
    symbol: str,
    provider: str | None = Query(
        default=None,
        description="Capability provider tag. Today only 'sec_form4'.",
    ),
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> InsiderSummaryModel:
    """Return the 90-day insider-transaction summary (#429 / #458).

    Two lenses: open-market (discretionary P/S) and total-activity
    (every non-derivative transaction classified by
    ``acquired_disposed_code``). Only non-derivative trades
    contribute; derivative grants / option exercises are excluded.
    """
    from app.services.insider_transactions import get_insider_summary

    _validate_provider(provider, _INSIDER_PROVIDERS)

    symbol_clean = symbol.strip().upper()
    if not symbol_clean:
        raise HTTPException(status_code=400, detail="symbol is required")

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT instrument_id, symbol FROM instruments
            WHERE UPPER(symbol) = %(s)s
            ORDER BY is_primary_listing DESC, instrument_id ASC
            LIMIT 1
            """,
            {"s": symbol_clean},
        )
        inst_row = cur.fetchone()

    if inst_row is None:
        raise HTTPException(status_code=404, detail=f"Instrument {symbol} not found")

    instrument_id = int(inst_row["instrument_id"])  # type: ignore[arg-type]
    if not _has_sec_cik(conn, instrument_id):
        # Insider transactions are SEC Form 4 — no SEC CIK = no
        # canonical source. 404 instead of returning orphan rows
        # from a prior bad CIK link (see migration 066 / spec PR 2).
        raise HTTPException(status_code=404, detail=f"Instrument {symbol} has no SEC coverage")
    summary = get_insider_summary(conn, instrument_id=instrument_id)
    return InsiderSummaryModel(
        symbol=str(inst_row["symbol"]),  # type: ignore[arg-type]
        open_market_net_shares_90d=summary.open_market_net_shares_90d,
        open_market_buy_count_90d=summary.open_market_buy_count_90d,
        open_market_sell_count_90d=summary.open_market_sell_count_90d,
        total_acquired_shares_90d=summary.total_acquired_shares_90d,
        total_disposed_shares_90d=summary.total_disposed_shares_90d,
        acquisition_count_90d=summary.acquisition_count_90d,
        disposition_count_90d=summary.disposition_count_90d,
        unique_filers_90d=summary.unique_filers_90d,
        latest_txn_date=summary.latest_txn_date,
        net_shares_90d=summary.open_market_net_shares_90d,
        buy_count_90d=summary.open_market_buy_count_90d,
        sell_count_90d=summary.open_market_sell_count_90d,
    )


class InsiderTransactionDetailModel(BaseModel):
    """Wide-shape payload for one Form 4 transaction row.

    Mirrors :class:`app.services.insider_transactions.InsiderTransactionDetail`
    one-to-one. Every structured field the XML carries is surfaced;
    the frontend picks what to render, the API does not editorialise.
    Footnote bodies are attached as a dict keyed by the XML field
    they qualify (``transactionShares``, ``transactionPricePerShare``,
    etc.) so the UI can render the explanatory text next to the
    specific cell.
    """

    accession_number: str
    txn_row_num: int
    document_type: str
    txn_date: date
    deemed_execution_date: date | None
    filer_cik: str | None
    filer_name: str
    filer_role: str | None
    security_title: str | None
    txn_code: str
    acquired_disposed_code: str | None
    shares: Decimal | None
    price: Decimal | None
    post_transaction_shares: Decimal | None
    direct_indirect: str | None
    nature_of_ownership: str | None
    is_derivative: bool
    equity_swap_involved: bool | None
    transaction_timeliness: str | None
    conversion_exercise_price: Decimal | None
    exercise_date: date | None
    expiration_date: date | None
    underlying_security_title: str | None
    underlying_shares: Decimal | None
    underlying_value: Decimal | None
    footnotes: dict[str, str]


class InsiderTransactionsListModel(BaseModel):
    symbol: str
    rows: list[InsiderTransactionDetailModel]


@router.get(
    "/{symbol}/insider_transactions",
    response_model=InsiderTransactionsListModel,
)
def get_instrument_insider_transactions(
    symbol: str,
    limit: int = 100,
    provider: str | None = Query(
        default=None,
        description="Capability provider tag. Today only 'sec_form4'.",
    ),
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> InsiderTransactionsListModel:
    """Return recent Form 4 insider transactions for an instrument.

    The operator-facing detail view. Covers both non-derivative
    (open-market buys/sells) and derivative (option exercises / grants)
    rows, most-recent first, up to ``limit``. Tombstoned filings
    (failed fetch / parse) are excluded.

    Every meaningful Form 4 XML field lands on the response. The UI
    decides what's worth rendering — we don't drop fields at the API
    layer (per the "every structured field queryable in SQL" rule).
    """
    from app.services.insider_transactions import list_insider_transactions

    _validate_provider(provider, _INSIDER_PROVIDERS)

    symbol_clean = symbol.strip().upper()
    if not symbol_clean:
        raise HTTPException(status_code=400, detail="symbol is required")
    if limit <= 0 or limit > 500:
        raise HTTPException(status_code=400, detail="limit must be between 1 and 500")

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT instrument_id, symbol FROM instruments
            WHERE UPPER(symbol) = %(s)s
            ORDER BY is_primary_listing DESC, instrument_id ASC
            LIMIT 1
            """,
            {"s": symbol_clean},
        )
        inst_row = cur.fetchone()

    if inst_row is None:
        raise HTTPException(status_code=404, detail=f"Instrument {symbol} not found")

    instrument_id = int(inst_row["instrument_id"])  # type: ignore[arg-type]
    if not _has_sec_cik(conn, instrument_id):
        # Form 4 is an SEC filing — no SEC CIK = no canonical
        # source. 404 instead of returning orphan rows from a
        # prior bad CIK link (see migration 066 / spec PR 2).
        raise HTTPException(status_code=404, detail=f"Instrument {symbol} has no SEC coverage")
    detail_rows = list_insider_transactions(conn, instrument_id=instrument_id, limit=limit)
    return InsiderTransactionsListModel(
        symbol=str(inst_row["symbol"]),  # type: ignore[arg-type]
        rows=[
            InsiderTransactionDetailModel(
                accession_number=d.accession_number,
                txn_row_num=d.txn_row_num,
                document_type=d.document_type,
                txn_date=d.txn_date,
                deemed_execution_date=d.deemed_execution_date,
                filer_cik=d.filer_cik,
                filer_name=d.filer_name,
                filer_role=d.filer_role,
                security_title=d.security_title,
                txn_code=d.txn_code,
                acquired_disposed_code=d.acquired_disposed_code,
                shares=d.shares,
                price=d.price,
                post_transaction_shares=d.post_transaction_shares,
                direct_indirect=d.direct_indirect,
                nature_of_ownership=d.nature_of_ownership,
                is_derivative=d.is_derivative,
                equity_swap_involved=d.equity_swap_involved,
                transaction_timeliness=d.transaction_timeliness,
                conversion_exercise_price=d.conversion_exercise_price,
                exercise_date=d.exercise_date,
                expiration_date=d.expiration_date,
                underlying_security_title=d.underlying_security_title,
                underlying_shares=d.underlying_shares,
                underlying_value=d.underlying_value,
                footnotes=d.footnotes,
            )
            for d in detail_rows
        ],
    )


# ---------------------------------------------------------------------
# Form 3 baseline-only insider holdings (#768 PR 4)
# ---------------------------------------------------------------------
#
# Surfaces insiders who hold a Form 3 baseline grant but have no Form 4
# activity on file — the "invisible insiders" the ownership card
# currently misses (RSU on appointment, never traded after). Frontend
# merges these rows with the Form 4 holders to produce the per-officer
# ring 3 wedges.


class InsiderBaselineHoldingModel(BaseModel):
    filer_cik: str
    filer_name: str
    filer_role: str | None
    security_title: str | None
    is_derivative: bool
    direct_indirect: str | None  # 'D' / 'I' / None
    shares: Decimal | None
    value_owned: Decimal | None
    as_of_date: date


class InsiderBaselineListModel(BaseModel):
    symbol: str
    rows: list[InsiderBaselineHoldingModel]


@router.get(
    "/{symbol}/insider_baseline",
    response_model=InsiderBaselineListModel,
)
def get_instrument_insider_baseline(
    symbol: str,
    provider: str | None = Query(
        default=None,
        description="Capability provider tag. Today only 'sec_form3'.",
    ),
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> InsiderBaselineListModel:
    """Return Form 3 baseline holdings for filers with no Form 4
    activity on file (#768 PR 4).

    Operationally meaningful slice: officers who received an RSU /
    initial grant on appointment and never traded after are invisible
    to the per-filer ring 3 today (no Form 4 events for them). The
    ownership panel merges these rows with the Form 4 holders to
    surface the complete current insider population.

    Filers with any non-tombstoned Form 4 row are excluded — their
    cumulative balance is already derivable from the latest
    ``post_transaction_shares`` observation on the
    ``insider_transactions`` reader. Including them here would
    double-count the per-filer wedge.

    Empty-state contract: a non-covered or pre-ingest instrument
    returns ``200`` with ``rows=[]``. Reserved 404 for unknown
    symbol or no SEC coverage (matches the existing
    ``/insider_transactions`` endpoint).
    """
    from app.services.insider_form3_ingest import list_baseline_only_insider_holdings

    _validate_provider(provider, _INSIDER_BASELINE_PROVIDERS)

    symbol_clean = symbol.strip().upper()
    if not symbol_clean:
        raise HTTPException(status_code=400, detail="symbol is required")

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT instrument_id, symbol FROM instruments
            WHERE UPPER(symbol) = %(s)s
            ORDER BY is_primary_listing DESC, instrument_id ASC
            LIMIT 1
            """,
            {"s": symbol_clean},
        )
        inst_row = cur.fetchone()

    if inst_row is None:
        raise HTTPException(status_code=404, detail=f"Instrument {symbol} not found")

    instrument_id = int(inst_row["instrument_id"])  # type: ignore[arg-type]
    if not _has_sec_cik(conn, instrument_id):
        # Form 3 is an SEC filing — no SEC CIK = no source.
        raise HTTPException(status_code=404, detail=f"Instrument {symbol} has no SEC coverage")

    holdings = list_baseline_only_insider_holdings(conn, instrument_id=instrument_id)
    return InsiderBaselineListModel(
        symbol=str(inst_row["symbol"]),  # type: ignore[arg-type]
        rows=[
            InsiderBaselineHoldingModel(
                filer_cik=h.filer_cik,
                filer_name=h.filer_name,
                filer_role=h.filer_role,
                security_title=h.security_title,
                is_derivative=h.is_derivative,
                direct_indirect=h.direct_indirect,
                shares=h.shares,
                value_owned=h.value_owned,
                as_of_date=h.as_of_date,
            )
            for h in holdings
        ],
    )


# ---------------------------------------------------------------------
# Form 3 baseline drillthrough + CSV export (#788 Chain 2.6)
# ---------------------------------------------------------------------


class InsiderBaselineDrillModel(BaseModel):
    """Drillthrough payload pairing the baseline holdings list with
    the Form 3 pipeline state from ownership_drillthrough.

    Lets the operator distinguish "no baseline rows because the
    issuer has no Form 3 filings" from "no baseline rows because
    the parser missed / tombstoned them" without flipping between
    pages.
    """

    symbol: str
    instrument_id: int
    rows: list[InsiderBaselineHoldingModel]
    pipeline_typed_row_count: int
    pipeline_raw_body_count: int
    pipeline_tombstone_count: int
    pipeline_notes: list[str]


@router.get(
    "/{symbol}/insider_baseline/drill",
    response_model=InsiderBaselineDrillModel,
)
def get_insider_baseline_drill(
    symbol: str,
    provider: str | None = Query(
        default=None,
        description="Capability provider tag. Today only 'sec_form3'.",
    ),
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> InsiderBaselineDrillModel:
    """Form 3 baseline list + per-instrument pipeline state.

    Empty-state contract matches ``/insider_baseline``: unknown
    symbol → 404, no SEC coverage → 404, ingest-not-yet-run →
    200 with empty rows + pipeline notes saying "no Form 3
    baseline filings"."""
    from app.services.insider_form3_ingest import list_baseline_only_insider_holdings

    _validate_provider(provider, _INSIDER_BASELINE_PROVIDERS)

    symbol_clean = symbol.strip().upper()
    if not symbol_clean:
        raise HTTPException(status_code=400, detail="symbol is required")

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT instrument_id, symbol FROM instruments
            WHERE UPPER(symbol) = %(s)s
            ORDER BY is_primary_listing DESC, instrument_id ASC
            LIMIT 1
            """,
            {"s": symbol_clean},
        )
        inst_row = cur.fetchone()

    if inst_row is None:
        raise HTTPException(status_code=404, detail=f"Instrument {symbol} not found")

    instrument_id = int(inst_row["instrument_id"])  # type: ignore[arg-type]
    if not _has_sec_cik(conn, instrument_id):
        raise HTTPException(status_code=404, detail=f"Instrument {symbol} has no SEC coverage")

    holdings = list_baseline_only_insider_holdings(conn, instrument_id=instrument_id)

    # Inline Form 3 pipeline state — typed-row count, raw bodies,
    # tombstones, informational notes. Same shape as the
    # ownership_drillthrough service (Chain 2.5, PR #830);
    # inlined here so this PR is independent of #830's merge order.
    notes: list[str] = []
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        # Per-#1117 PR-B: insider_initial_holdings + insider_filings are
        # entity-level (PK accession or accession+row_num); the
        # per-instrument denormalised instrument_id column points at
        # the canonical sibling, NOT every share-class sibling. Reads
        # for share-class siblings (GOOG/GOOGL) route through
        # filing_events bridge so both render the same Form 3
        # baseline.
        cur.execute(
            """
            SELECT COUNT(*) AS row_count
            FROM insider_initial_holdings h
            JOIN insider_filings f ON f.accession_number = h.accession_number
            WHERE f.document_type LIKE '3%%'
              AND f.is_tombstone = FALSE
              AND EXISTS (
                  SELECT 1 FROM filing_events fe
                  WHERE fe.provider_filing_id = h.accession_number
                    AND fe.provider = 'sec'
                    AND fe.instrument_id = %s
              )
            """,
            (instrument_id,),
        )
        typed = cur.fetchone() or {"row_count": 0}
        cur.execute(
            """
            SELECT COUNT(*) AS tombstone_count
            FROM insider_filings i
            WHERE i.document_type LIKE '3%%'
              AND i.is_tombstone = TRUE
              AND EXISTS (
                  SELECT 1 FROM filing_events fe
                  WHERE fe.provider_filing_id = i.accession_number
                    AND fe.provider = 'sec'
                    AND fe.instrument_id = %s
              )
            """,
            (instrument_id,),
        )
        tomb = cur.fetchone() or {"tombstone_count": 0}
        cur.execute(
            """
            SELECT COUNT(DISTINCT r.accession_number) AS body_count
            FROM filing_raw_documents r
            JOIN insider_filings i ON i.accession_number = r.accession_number
            WHERE r.document_kind = 'form3_xml'
              AND EXISTS (
                  SELECT 1 FROM filing_events fe
                  WHERE fe.provider_filing_id = i.accession_number
                    AND fe.provider = 'sec'
                    AND fe.instrument_id = %s
              )
            """,
            (instrument_id,),
        )
        body = cur.fetchone() or {"body_count": 0}

    typed_row_count = int(typed["row_count"])
    tombstone_count = int(tomb["tombstone_count"])
    raw_body_count = int(body["body_count"])
    # "No filings" is the LITERAL no-coverage case: zero typed
    # rows, zero tombstones, zero raw bodies. The other zero-typed
    # cases (rewash candidate / tombstoned-only) get more specific
    # notes below — Codex pre-push review caught the prior
    # mislabelling.
    if typed_row_count == 0 and tombstone_count == 0 and raw_body_count == 0:
        notes.append("no Form 3 baseline filings")
    if tombstone_count:
        notes.append(f"{tombstone_count} tombstoned filing(s)")
    if raw_body_count and not typed_row_count:
        notes.append("raw bodies on file but zero typed rows — rewash candidate")

    return InsiderBaselineDrillModel(
        symbol=str(inst_row["symbol"]),  # type: ignore[arg-type]
        instrument_id=instrument_id,
        rows=[
            InsiderBaselineHoldingModel(
                filer_cik=h.filer_cik,
                filer_name=h.filer_name,
                filer_role=h.filer_role,
                security_title=h.security_title,
                is_derivative=h.is_derivative,
                direct_indirect=h.direct_indirect,
                shares=h.shares,
                value_owned=h.value_owned,
                as_of_date=h.as_of_date,
            )
            for h in holdings
        ],
        pipeline_typed_row_count=typed_row_count,
        pipeline_raw_body_count=raw_body_count,
        pipeline_tombstone_count=tombstone_count,
        pipeline_notes=notes,
    )


@router.get(
    "/{symbol}/insider_baseline/export.csv",
    response_class=PlainTextResponse,
)
def get_insider_baseline_csv(
    symbol: str,
    provider: str | None = Query(
        default=None,
        description="Capability provider tag. Today only 'sec_form3'.",
    ),
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> PlainTextResponse:
    """Operator-friendly CSV export of the same baseline list.

    Always 200 (with a single header row when no data) so an
    automation script can pipe this into a spreadsheet without
    branch-on-status. Intentionally streams as text/csv with
    ``Content-Disposition: attachment`` so the browser saves the
    file rather than rendering the raw text."""
    from app.services.insider_form3_ingest import list_baseline_only_insider_holdings

    _validate_provider(provider, _INSIDER_BASELINE_PROVIDERS)

    symbol_clean = symbol.strip().upper()
    if not symbol_clean:
        raise HTTPException(status_code=400, detail="symbol is required")

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT instrument_id, symbol FROM instruments
            WHERE UPPER(symbol) = %(s)s
            ORDER BY is_primary_listing DESC, instrument_id ASC
            LIMIT 1
            """,
            {"s": symbol_clean},
        )
        inst_row = cur.fetchone()

    if inst_row is None:
        raise HTTPException(status_code=404, detail=f"Instrument {symbol} not found")
    instrument_id = int(inst_row["instrument_id"])  # type: ignore[arg-type]
    if not _has_sec_cik(conn, instrument_id):
        raise HTTPException(status_code=404, detail=f"Instrument {symbol} has no SEC coverage")

    holdings = list_baseline_only_insider_holdings(conn, instrument_id=instrument_id)

    # Build CSV via csv.writer to a StringIO so we get the
    # canonical quoting + line terminator. Plain str-join would
    # mishandle commas / quotes inside filer names.
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerow(
        [
            "filer_cik",
            "filer_name",
            "filer_role",
            "security_title",
            "is_derivative",
            "direct_indirect",
            "shares",
            "value_owned",
            "as_of_date",
        ]
    )
    for h in holdings:
        writer.writerow(
            [
                h.filer_cik,
                h.filer_name,
                h.filer_role or "",
                h.security_title or "",
                "true" if h.is_derivative else "false",
                h.direct_indirect or "",
                str(h.shares) if h.shares is not None else "",
                str(h.value_owned) if h.value_owned is not None else "",
                h.as_of_date.isoformat(),
            ]
        )
    return PlainTextResponse(
        content=buf.getvalue(),
        media_type="text/csv",
        headers={
            "Content-Disposition": f'attachment; filename="{symbol_clean}_insider_baseline.csv"',
        },
    )


# ---------------------------------------------------------------------
# DEF 14A beneficial-ownership drillthrough + CSV export (#788 Chain 2.7)
# ---------------------------------------------------------------------


class Def14AHolderModel(BaseModel):
    holder_name: str
    holder_role: str | None
    shares: Decimal | None
    percent_of_class: Decimal | None
    as_of_date: date | None
    accession_number: str
    issuer_cik: str


class Def14ADrillModel(BaseModel):
    """Holders + Form 14A pipeline state.

    ``holders`` are from the latest TYPED-ROW filing
    (def14a_beneficial_holdings → latest as_of_date). When a
    newer filing exists in filing_events but didn't produce
    typed rows (parser failed / not yet ingested),
    ``pipeline_notes`` surfaces the gap so the operator doesn't
    silently see stale holders. Codex pre-push review caught
    the prior version which served stale holders without
    surfacing the newer filing's existence.
    """

    symbol: str
    instrument_id: int
    holders: list[Def14AHolderModel]
    pipeline_typed_row_count: int
    pipeline_raw_body_count: int
    pipeline_tombstone_count: int
    pipeline_notes: list[str]
    # Newest DEF 14A filing date observed in filing_events, regardless
    # of whether typed rows exist for it. Lets the operator see at a
    # glance whether the holders shown are from the latest known
    # filing or an older one.
    latest_known_filing_date: date | None
    # Date of the filing the holders are from (the typed-row
    # filing). NULL when no typed rows exist.
    holders_as_of_date: date | None


@router.get(
    "/{symbol}/def14a_holdings/drill",
    response_model=Def14ADrillModel,
)
def get_def14a_drill(
    symbol: str,
    provider: str | None = Query(
        default=None,
        description="Capability provider tag. Today only 'sec_def14a'.",
    ),
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> Def14ADrillModel:
    """Latest-filing DEF 14A beneficial-ownership holders + Form
    14A pipeline state (typed row count, raw body count,
    tombstones, notes). Same gates as /insider_baseline."""
    _validate_provider(provider, _DEF14A_PROVIDERS)

    symbol_clean = symbol.strip().upper()
    if not symbol_clean:
        raise HTTPException(status_code=400, detail="symbol is required")

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT instrument_id, symbol FROM instruments
            WHERE UPPER(symbol) = %(s)s
            ORDER BY is_primary_listing DESC, instrument_id ASC
            LIMIT 1
            """,
            {"s": symbol_clean},
        )
        inst_row = cur.fetchone()

    if inst_row is None:
        raise HTTPException(status_code=404, detail=f"Instrument {symbol} not found")
    instrument_id = int(inst_row["instrument_id"])  # type: ignore[arg-type]
    if not _has_sec_cik(conn, instrument_id):
        raise HTTPException(status_code=404, detail=f"Instrument {symbol} has no SEC coverage")

    # Latest filing's holders. ``ORDER BY as_of_date DESC NULLS
    # LAST, accession_number DESC`` so a tie-breaks on accession
    # rather than picking arbitrarily; NULLS LAST so a non-null
    # ``as_of_date`` always beats a null one.
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            WITH latest AS (
                SELECT accession_number, as_of_date AS holders_as_of
                FROM def14a_beneficial_holdings
                WHERE instrument_id = %s
                ORDER BY as_of_date DESC NULLS LAST, accession_number DESC
                LIMIT 1
            )
            SELECT holder_name, holder_role, shares, percent_of_class,
                   h.as_of_date, h.accession_number, issuer_cik,
                   latest.holders_as_of
            FROM def14a_beneficial_holdings h
            JOIN latest USING (accession_number)
            WHERE instrument_id = %s
            ORDER BY shares DESC NULLS LAST, holder_name
            """,
            (instrument_id, instrument_id),
        )
        holder_rows = cur.fetchall()
        holders_as_of_date = holder_rows[0].get("holders_as_of") if holder_rows else None

        # Newest DEF 14A filing observed in filing_events
        # (regardless of whether typed rows exist for it). Track
        # ``provider_filing_id`` (= accession_number) so the
        # stale check below can compare like-for-like against the
        # holders' accession. ``filing_date`` and the holders'
        # ``as_of_date`` are different business dates for the same
        # filing — Codex pre-push review caught a prior version
        # that compared them and false-positived.
        cur.execute(
            """
            -- DEF14A-CAP-EXEMPT: read-side coverage/diagnostic
            -- query — reports what's been ingested; not a writer
            -- chokepoint. #1233 PR5 cap applies only to ingest
            -- decisions.
            SELECT provider_filing_id AS accession, filing_date
            FROM filing_events
            WHERE provider = 'sec'
              AND filing_type = 'DEF 14A'
              AND instrument_id = %s
            -- Tie-break on accession (provider_filing_id), not
            -- filing_event_id, so the choice is deterministic
            -- regardless of insert order. Codex pre-push review
            -- caught a same-day false-stale where event-id tie-
            -- break picked acc-2 over acc-1.
            ORDER BY filing_date DESC, provider_filing_id DESC
            LIMIT 1
            """,
            (instrument_id,),
        )
        latest_event_row = cur.fetchone()
        latest_known_filing_date = latest_event_row.get("filing_date") if latest_event_row else None
        latest_known_accession = latest_event_row.get("accession") if latest_event_row else None
        holders_accession = holder_rows[0].get("accession_number") if holder_rows else None

        # Pipeline state (mirrors ownership_drillthrough's def14a
        # query — inlined to keep this PR independent of #830's
        # merge order).
        cur.execute(
            "SELECT COUNT(*) AS row_count FROM def14a_beneficial_holdings WHERE instrument_id = %s",
            (instrument_id,),
        )
        typed = cur.fetchone() or {"row_count": 0}
        # COUNT(DISTINCT log.accession_number): a single filing
        # retried multiple times produces multiple log rows, so a
        # bare COUNT(*) inflates tombstone_count and misleads
        # operator triage. Claude PR #833 review caught this as
        # WARNING — filing-level counts are the canonical operator
        # signal, not row-level.
        cur.execute(
            """
            -- DEF14A-CAP-EXEMPT: read-side coverage/diagnostic
            -- (tombstone-count). Not an ingest chokepoint.
            SELECT COUNT(DISTINCT log.accession_number) AS tombstone_count
            FROM def14a_ingest_log log
            WHERE log.status IN ('partial', 'failed')
              AND log.accession_number IN (
                  SELECT accession_number FROM def14a_beneficial_holdings
                  WHERE instrument_id = %s
                  UNION
                  SELECT fe.provider_filing_id FROM filing_events fe
                  WHERE fe.provider = 'sec' AND fe.instrument_id = %s
                    AND fe.filing_type = 'DEF 14A'
              )
            """,
            (instrument_id, instrument_id),
        )
        tomb = cur.fetchone() or {"tombstone_count": 0}
        cur.execute(
            """
            -- DEF14A-CAP-EXEMPT: read-side coverage/diagnostic
            -- (raw-body count). Not an ingest chokepoint.
            SELECT COUNT(DISTINCT r.accession_number) AS body_count
            FROM filing_raw_documents r
            JOIN filing_events fe ON fe.provider_filing_id = r.accession_number
            WHERE r.document_kind = 'def14a_body'
              AND fe.provider = 'sec'
              AND fe.filing_type = 'DEF 14A'
              AND fe.instrument_id = %s
            """,
            (instrument_id,),
        )
        body = cur.fetchone() or {"body_count": 0}

    typed_count = int(typed["row_count"])
    tombstone_count = int(tomb["tombstone_count"])
    raw_body_count = int(body["body_count"])

    # Discovered-but-unparsed: filings exist in filing_events but
    # haven't produced typed rows / raw bodies / tombstones. The
    # ingest queue should pick them up; surface as info so the
    # operator knows the gap is queue-side, not pipeline-side.
    discovered_unparsed_count = 0
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            -- DEF14A-CAP-EXEMPT: read-side coverage/diagnostic
            -- (discovered-unparsed count). Not an ingest chokepoint.
            SELECT COUNT(*) AS c
            FROM filing_events fe
            WHERE fe.provider = 'sec'
              AND fe.filing_type = 'DEF 14A'
              AND fe.instrument_id = %s
              AND NOT EXISTS (
                  SELECT 1 FROM def14a_ingest_log log
                  WHERE log.accession_number = fe.provider_filing_id
              )
            """,
            (instrument_id,),
        )
        d_row = cur.fetchone()
        if d_row is not None:
            discovered_unparsed_count = int(d_row["c"])

    notes: list[str] = []
    if typed_count == 0 and tombstone_count == 0 and raw_body_count == 0 and discovered_unparsed_count == 0:
        notes.append("no DEF 14A holders")
    if tombstone_count:
        notes.append(f"{tombstone_count} tombstoned proxy filing(s)")
    if raw_body_count and not typed_count:
        notes.append("raw bodies on file but zero typed rows — rewash candidate")
    if discovered_unparsed_count:
        notes.append(f"{discovered_unparsed_count} filing(s) discovered but not yet ingested")
    # Stale-holders surface: the latest filing in filing_events is
    # a DIFFERENT accession than the one the holders came from.
    # Compare by accession (not date) — filing_date and as_of_date
    # are different business dates of the same filing, so a date
    # comparison would false-positive on a healthy single-filing
    # case. Codex pre-push review caught.
    if (
        latest_known_accession is not None
        and holders_accession is not None
        and latest_known_accession != holders_accession
    ):
        date_str = latest_known_filing_date.isoformat() if latest_known_filing_date is not None else "unknown"
        notes.append(
            f"holders shown are from accession {holders_accession}; "
            f"newer DEF 14A {latest_known_accession} (filed {date_str}) "
            f"is missing typed rows"
        )

    return Def14ADrillModel(
        symbol=str(inst_row["symbol"]),  # type: ignore[arg-type]
        instrument_id=instrument_id,
        holders=[
            Def14AHolderModel(
                holder_name=str(r["holder_name"]),  # type: ignore[arg-type]
                holder_role=(str(r["holder_role"]) if r.get("holder_role") else None),
                shares=r.get("shares"),  # type: ignore[arg-type]
                percent_of_class=r.get("percent_of_class"),  # type: ignore[arg-type]
                as_of_date=r.get("as_of_date"),  # type: ignore[arg-type]
                accession_number=str(r["accession_number"]),  # type: ignore[arg-type]
                issuer_cik=str(r["issuer_cik"]),  # type: ignore[arg-type]
            )
            for r in holder_rows
        ],
        pipeline_typed_row_count=typed_count,
        pipeline_raw_body_count=raw_body_count,
        pipeline_tombstone_count=tombstone_count,
        pipeline_notes=notes,
        latest_known_filing_date=latest_known_filing_date,
        holders_as_of_date=holders_as_of_date,
    )


@router.get(
    "/{symbol}/def14a_holdings/export.csv",
    response_class=PlainTextResponse,
)
def get_def14a_csv(
    symbol: str,
    provider: str | None = Query(
        default=None,
        description="Capability provider tag. Today only 'sec_def14a'.",
    ),
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> PlainTextResponse:
    """CSV export of all DEF 14A holders across all on-file
    accessions for the instrument. Operator-friendly: header
    always emitted; downloaded file uses
    ``Content-Disposition: attachment``."""
    _validate_provider(provider, _DEF14A_PROVIDERS)

    symbol_clean = symbol.strip().upper()
    if not symbol_clean:
        raise HTTPException(status_code=400, detail="symbol is required")

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT instrument_id, symbol FROM instruments
            WHERE UPPER(symbol) = %(s)s
            ORDER BY is_primary_listing DESC, instrument_id ASC
            LIMIT 1
            """,
            {"s": symbol_clean},
        )
        inst_row = cur.fetchone()
    if inst_row is None:
        raise HTTPException(status_code=404, detail=f"Instrument {symbol} not found")
    instrument_id = int(inst_row["instrument_id"])  # type: ignore[arg-type]
    if not _has_sec_cik(conn, instrument_id):
        raise HTTPException(status_code=404, detail=f"Instrument {symbol} has no SEC coverage")

    # Export ALL holdings (every accession) — the CSV is for
    # historical analysis, not the latest snapshot. Ordered so a
    # spreadsheet groups by filing year naturally.
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT accession_number, issuer_cik, holder_name, holder_role,
                   shares, percent_of_class, as_of_date
            FROM def14a_beneficial_holdings
            WHERE instrument_id = %s
            ORDER BY as_of_date DESC NULLS LAST, accession_number DESC,
                     shares DESC NULLS LAST, holder_name
            """,
            (instrument_id,),
        )
        rows = cur.fetchall()

    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerow(
        [
            "accession_number",
            "issuer_cik",
            "holder_name",
            "holder_role",
            "shares",
            "percent_of_class",
            "as_of_date",
        ]
    )
    for r in rows:
        writer.writerow(
            [
                str(r["accession_number"]),
                str(r["issuer_cik"]),
                str(r["holder_name"]),
                str(r["holder_role"] or ""),
                str(r["shares"]) if r.get("shares") is not None else "",
                str(r["percent_of_class"]) if r.get("percent_of_class") is not None else "",
                r["as_of_date"].isoformat() if r.get("as_of_date") else "",
            ]
        )
    return PlainTextResponse(
        content=buf.getvalue(),
        media_type="text/csv",
        headers={
            "Content-Disposition": f'attachment; filename="{symbol_clean}_def14a_holdings.csv"',
        },
    )


def _has_sec_cik(conn: psycopg.Connection[object], instrument_id: int) -> bool:
    """True if the instrument has a primary SEC CIK — the US-ticker signal
    that gates the local-SEC-XBRL preference path."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM external_identifiers "
            "WHERE instrument_id = %(iid)s AND provider = 'sec' "
            "AND identifier_type = 'cik' AND is_primary = TRUE "
            "LIMIT 1",
            {"iid": instrument_id},
        )
        return cur.fetchone() is not None


def _has_filings_coverage(conn: psycopg.Connection[object], instrument_id: int) -> bool:
    """True if any filings provider has filed filings for the instrument.

    Provider-agnostic coverage gate (#503 PR 2). Today the only
    populated provider is SEC, so this is currently equivalent to
    ``EXISTS row in filing_events``. Once Companies House / other
    regional providers are wired, the same gate keeps working
    without a frontend change.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM filing_events WHERE instrument_id = %(iid)s LIMIT 1",
            {"iid": instrument_id},
        )
        return cur.fetchone() is not None


def _fetch_local_fundamentals(
    conn: psycopg.Connection[object],
    instrument_id: int,
) -> dict[str, Decimal | None]:
    """Return the latest fundamentals_snapshot row as a dict of derivable
    fields. Missing rows yield an empty dict — callers treat it as
    'no local fundamentals'.

    Pulls:
      - eps (for PE ratio with current price)
      - book_value (per-share, for PB ratio with current price)
      - net_debt / cash / debt (for debt_to_equity with equity from
        financial_periods)

    Complemented by the latest financial_periods row (TTM-ish) for ROE,
    ROA, revenue growth — data the snapshot table doesn't carry.
    """
    out: dict[str, Decimal | None] = {}
    try:
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                """
                SELECT eps, book_value, shares_outstanding, cash, debt, net_debt, revenue_ttm
                FROM fundamentals_snapshot
                WHERE instrument_id = %(iid)s
                ORDER BY as_of_date DESC
                LIMIT 1
                """,
                {"iid": instrument_id},
            )
            snap = cur.fetchone()
            cur.execute(
                """
                SELECT net_income, shareholders_equity, total_assets, total_liabilities, revenue
                FROM financial_periods
                WHERE instrument_id = %(iid)s
                  AND superseded_at IS NULL
                  AND period_type IN ('Q1', 'Q2', 'Q3', 'Q4', 'FY')
                ORDER BY period_end_date DESC
                LIMIT 1
                """,
                {"iid": instrument_id},
            )
            fp = cur.fetchone()
    except psycopg.Error:
        # DB errors masquerading as "no local data" would be invisible
        # in the source map. Log the failure loudly; caller treats the
        # empty dict as "stats unavailable".
        logger.warning(
            "_fetch_local_fundamentals: DB query failed for instrument_id=%d",
            instrument_id,
            exc_info=True,
        )
        return {}
    if snap is not None:
        out["eps"] = snap.get("eps")  # type: ignore[union-attr]
        out["book_value"] = snap.get("book_value")  # type: ignore[union-attr]
        out["shares_outstanding"] = snap.get("shares_outstanding")  # type: ignore[union-attr]
        out["net_debt"] = snap.get("net_debt")  # type: ignore[union-attr]
        out["debt"] = snap.get("debt")  # type: ignore[union-attr]
    if fp is not None:
        out["net_income"] = fp.get("net_income")  # type: ignore[union-attr]
        out["shareholders_equity"] = fp.get("shareholders_equity")  # type: ignore[union-attr]
        out["total_assets"] = fp.get("total_assets")  # type: ignore[union-attr]
        out["total_liabilities"] = fp.get("total_liabilities")  # type: ignore[union-attr]
        out["revenue"] = fp.get("revenue")  # type: ignore[union-attr]
    return out


def _build_local_stats(
    local: dict[str, Decimal | None],
    current_price: Decimal | None,
    dividend_yield: Decimal | None,
) -> InstrumentKeyStats | None:
    """Build an :class:`InstrumentKeyStats` from SEC XBRL-derived data only.

    Per the settled provider strategy (#498/#499), yfinance is no
    longer consulted; key stats are computed exclusively from local
    SEC data:

      - pe_ratio        = current_price / eps
      - pb_ratio        = current_price / book_value
      - debt_to_equity  = debt / shareholders_equity
      - roe             = net_income / shareholders_equity
      - roa             = net_income / total_assets
      - dividend_yield  = passed in from
        ``instrument_dividend_summary.ttm_yield_pct`` (#426)

    Fields the local SEC pipeline doesn't yet produce (payout ratio,
    revenue / earnings growth YoY) return ``None`` with
    ``field_source = "unavailable"`` until those derivations land.
    """
    if not local and current_price is None and dividend_yield is None:
        return None

    field_source: dict[str, KeyStatsFieldSource] = {}

    def _pick(field: str, local_value: Decimal | None, *, source: KeyStatsFieldSource = "sec_xbrl") -> Decimal | None:
        if local_value is not None:
            field_source[field] = source
            return local_value
        field_source[field] = "unavailable"
        return None

    def _safe_div(num: Decimal | None, denom: Decimal | None) -> Decimal | None:
        if num is None or denom is None or denom == 0:
            return None
        try:
            return num / denom
        except ArithmeticError, InvalidOperation:
            return None

    local_pe = _safe_div(current_price, local.get("eps"))
    local_pb = _safe_div(current_price, local.get("book_value"))
    local_de = _safe_div(local.get("debt"), local.get("shareholders_equity"))
    local_roe = _safe_div(local.get("net_income"), local.get("shareholders_equity"))
    local_roa = _safe_div(local.get("net_income"), local.get("total_assets"))

    # When local EPS / book_value are present but current_price is not,
    # pe/pb remain unresolvable — but that's "waiting on price", not
    # "no local data". Surface that distinction so the UI can render a
    # "price missing" hint rather than an ambiguous em-dash.
    price_blocked_pe = local_pe is None and current_price is None and local.get("eps") is not None
    price_blocked_pb = local_pb is None and current_price is None and local.get("book_value") is not None

    pe_final = _pick("pe_ratio", local_pe)
    pb_final = _pick("pb_ratio", local_pb)
    div_final = _pick("dividend_yield", dividend_yield, source="sec_dividend_summary")
    payout_final = _pick("payout_ratio", None)
    roe_final = _pick("roe", local_roe)
    roa_final = _pick("roa", local_roa)
    de_final = _pick("debt_to_equity", local_de)
    rev_growth_final = _pick("revenue_growth_yoy", None)
    earn_growth_final = _pick("earnings_growth_yoy", None)

    # Rewrite pe/pb source when SEC data exists but the current price
    # is missing — distinguishes "waiting on price" from "genuinely
    # unavailable". Must run BEFORE Pydantic copies the dict into the
    # model field.
    if price_blocked_pe and pe_final is None:
        field_source["pe_ratio"] = "sec_xbrl_price_missing"
    if price_blocked_pb and pb_final is None:
        field_source["pb_ratio"] = "sec_xbrl_price_missing"

    return InstrumentKeyStats(
        pe_ratio=pe_final,
        pb_ratio=pb_final,
        dividend_yield=div_final,
        payout_ratio=payout_final,
        roe=roe_final,
        roa=roa_final,
        debt_to_equity=de_final,
        revenue_growth_yoy=rev_growth_final,
        earnings_growth_yoy=earn_growth_final,
        field_source=field_source,
    )


@router.get("/{symbol}/summary", response_model=InstrumentSummary)
def get_instrument_summary(
    symbol: str,
    conn: psycopg.Connection[object] = Depends(get_conn),
    instrument_id: int | None = Query(default=None, ge=1, alias="id"),
) -> InstrumentSummary:
    """Per-ticker research summary.

    Sources, per the settled provider strategy (#498/#499 — yfinance
    retired):

    - Identity: local ``instruments`` row.
    - Price: ``quotes`` table (eToro WS / market data refresh).
    - Market cap: SEC XBRL share count × ``quotes.last``.
    - Key stats: SEC XBRL via ``financial_periods_ttm`` +
      ``instrument_dividend_summary``.

    Fields with no canonical eToro / SEC source today (52-week range,
    day change, payout ratio, growth YoY) return ``None`` rather
    than reaching for an unsanctioned provider. The frontend renders
    "—" for those until a follow-up wires SEC-derived computations.

    A missing symbol returns 404. ``?id=<instrument_id>`` override:
    when a symbol collides across exchanges, the caller can pin a
    specific instrument_id. The server verifies that id's symbol
    matches the path symbol — a mismatch is a 404, not a silent
    wrong-instrument response. See
    docs/superpowers/specs/2026-04-20-per-stock-research-page.md §2.
    """
    symbol_clean = symbol.strip().upper()
    if not symbol_clean:
        raise HTTPException(status_code=400, detail="symbol is required")

    # `symbol` is not UNIQUE across exchanges (see migration 043);
    # ORDER BY `is_primary_listing DESC, instrument_id ASC` makes the
    # winner deterministic when two listings share a ticker.
    # #819: LEFT JOIN canonical row so the response can advertise the
    # canonical symbol for operational-duplicate variants (e.g.
    # ``AAPL.RTH`` -> ``AAPL``). NULL when this row IS canonical.
    if instrument_id is not None:
        # instrument_id is the PK so the lookup is already unique; no
        # ORDER BY / LIMIT needed.
        lookup_sql = f"""
            SELECT i.instrument_id, i.symbol, i.company_name, i.exchange,
                   i.currency, i.sector, esi.name AS sector_name,
                   i.industry, i.country,
                   i.is_tradable, c.coverage_tier,
                   q.bid, q.ask, q.last,
                   p.sic,
                   {_SESSION_PROFILE_SQL},
                   canonical.symbol AS canonical_symbol
            FROM instruments i
            LEFT JOIN coverage c USING (instrument_id)
            LEFT JOIN quotes q USING (instrument_id)
            LEFT JOIN instrument_sec_profile p USING (instrument_id)
            LEFT JOIN exchanges e ON e.exchange_id = i.exchange
            LEFT JOIN etoro_stocks_industries esi
              ON esi.industry_id::text = i.sector
            LEFT JOIN instruments canonical
              ON canonical.instrument_id = i.canonical_instrument_id
            WHERE i.instrument_id = %(id)s AND UPPER(i.symbol) = %(symbol)s
        """
        params: dict[str, object] = {"id": instrument_id, "symbol": symbol_clean}
    else:
        lookup_sql = f"""
            SELECT i.instrument_id, i.symbol, i.company_name, i.exchange,
                   i.currency, i.sector, esi.name AS sector_name,
                   i.industry, i.country,
                   i.is_tradable, c.coverage_tier,
                   q.bid, q.ask, q.last,
                   p.sic,
                   {_SESSION_PROFILE_SQL},
                   canonical.symbol AS canonical_symbol
            FROM instruments i
            LEFT JOIN coverage c USING (instrument_id)
            LEFT JOIN quotes q USING (instrument_id)
            LEFT JOIN instrument_sec_profile p USING (instrument_id)
            LEFT JOIN exchanges e ON e.exchange_id = i.exchange
            LEFT JOIN etoro_stocks_industries esi
              ON esi.industry_id::text = i.sector
            LEFT JOIN instruments canonical
              ON canonical.instrument_id = i.canonical_instrument_id
            WHERE UPPER(i.symbol) = %(symbol)s
            ORDER BY i.is_primary_listing DESC, i.instrument_id ASC
            LIMIT 1
        """
        params = {"symbol": symbol_clean}
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(lookup_sql, params)
        row = cur.fetchone()

    if row is None:
        raise HTTPException(status_code=404, detail=f"Instrument {symbol} not found")

    # Identity: local DB only. Fields the operator's universe sync
    # left null stay null — no third-party fill-in.
    # #1634: resolve the real GICS sector + sector-SPDR from the SEC SIC
    # (fail-closed None when no SIC / no confident mapping).
    sector_cls = resolve_sector_spdr(row.get("sic"))  # type: ignore[arg-type]
    identity = InstrumentIdentity(
        symbol=row["symbol"],  # type: ignore[arg-type]
        display_name=row["company_name"] or None,  # type: ignore[arg-type]
        sector=row["sector"],  # type: ignore[arg-type]
        sector_name=row["sector_name"],  # type: ignore[arg-type]
        industry=row["industry"],  # type: ignore[arg-type]
        gics_sector=sector_cls.gics_sector if sector_cls is not None else None,
        sector_spdr=sector_cls.spdr_symbol if sector_cls is not None else None,
        exchange=row["exchange"],  # type: ignore[arg-type]
        country=row["country"],  # type: ignore[arg-type]
        currency=row["currency"],  # type: ignore[arg-type]
        market_cap=None,
        class_market_value=None,
        canonical_symbol=row.get("canonical_symbol"),  # type: ignore[arg-type]
    )

    # Price: ``quotes`` row. Mark hierarchy is the shared #1428 contract —
    # live trade ``last`` (>0) → bid/ask mid → none — so the instrument
    # summary current price matches the position mark on the same
    # instrument. A non-positive last is not a valid price (eToro persists
    # last=0.00 for un-freshly-traded instruments). Day change + 52w range
    # stay null until a SEC-derived computation lands; the frontend renders "—".
    mark = resolve_quote_price(
        float(row["last"]) if row.get("last") is not None else None,
        float(row["bid"]) if row.get("bid") is not None else None,
        float(row["ask"]) if row.get("ask") is not None else None,
    )
    current_price: Decimal | None = Decimal(str(mark)) if mark is not None else None
    native_ccy = row["currency"]  # type: ignore[assignment]
    # #1906 (operator decision 2026-07-04): NATIVE price is primary — the
    # tradable number — and never flips by which path (REST snapshot vs SSE
    # live tick) answers first. ``current``/``currency`` therefore stay
    # native. ``display_current``/``display_currency`` carry the FX-converted
    # companion in the operator's display currency (rendered secondary +
    # muted). The companion stays ``None`` when no FX rate is available (the
    # native price still shows — never a converted number under a wrong
    # label) or when native already equals the display currency. Mirrors the
    # SSE live-tick path, which emits both a native triple and a ``display``
    # block (app/api/sse_quotes.py ``_format_tick``).
    display_price: Decimal | None = None
    display_ccy_for_price: str | None = None
    if current_price is not None and native_ccy is not None:
        display_currency = get_runtime_config(conn).display_currency
        if native_ccy != display_currency:
            try:
                rates = load_live_fx_rates(conn)
                display_price = convert(current_price, native_ccy, display_currency, rates)
                display_ccy_for_price = display_currency
            except FxRateNotFound:
                logger.warning(
                    "get_instrument_summary: FX rate %s->%s not found for %s; showing native price only",
                    native_ccy,
                    display_currency,
                    symbol_clean,
                )
    price_block = (
        InstrumentPrice(
            current=current_price,
            day_change=None,
            day_change_pct=None,
            week_52_high=None,
            week_52_low=None,
            currency=native_ccy,  # type: ignore[arg-type]
            display_current=display_price,
            display_currency=display_ccy_for_price,
        )
        if current_price is not None
        else None
    )

    instrument_id_int = int(row["instrument_id"])  # type: ignore[arg-type]
    local_fundamentals: dict[str, Decimal | None] = {}
    use_local_sec = _has_sec_cik(conn, instrument_id_int)
    if use_local_sec:
        local_fundamentals = _fetch_local_fundamentals(conn, instrument_id_int)

    # Market cap. For a CURATED multi-class issuer (GOOG/GOOGL, …) the combined-shares
    # × this-class-price product is structurally wrong (one company → two different
    # "caps"), so use the total-company cap Σ(class shares × class price) from the
    # #1623 per-class FSDS table. `resolve_market_cap_basis` returns:
    #   - total_company → use the total
    #   - multiclass_unavailable → fail closed (null), never publish the broken
    #     combined×price for a known dual-class issuer
    #   - not_multiclass → legacy single-class product (exact; also the better total
    #     when only one class of a multi-class issuer is in our universe)
    # Returns None for instruments without SEC coverage; market cap then stays null
    # on the identity rather than reaching for a non-canonical source.
    try:
        from app.services.xbrl_derived_stats import compute_market_cap, resolve_market_cap_basis

        cap_resolution = resolve_market_cap_basis(conn, instrument_id=instrument_id_int)
        if cap_resolution.basis == "total_company" and cap_resolution.total is not None:
            computed_cap_value: Decimal | None = cap_resolution.total.value
        elif cap_resolution.basis == "multiclass_unavailable":
            computed_cap_value = None  # fail closed: known dual-class, no clean total
        else:
            single_cap = compute_market_cap(conn, instrument_id=instrument_id_int)
            computed_cap_value = single_cap.value if single_cap is not None else None
        # #1665: per-class float value of THIS instrument's own share class.
        # Travels on the same resolution (no extra query); set only on the
        # total_company basis, so it is null for single-class issuers.
        class_market_value: Decimal | None = cap_resolution.class_market_value
    except Exception:
        logger.warning("compute_market_cap failed", exc_info=True)
        computed_cap_value = None
        class_market_value = None
    identity_update: dict[str, Decimal | None] = {}
    if computed_cap_value is not None:
        identity_update["market_cap"] = computed_cap_value
    if class_market_value is not None:
        identity_update["class_market_value"] = class_market_value
    if identity_update:
        identity = identity.model_copy(update=identity_update)

    # Dividend yield: SEC dividend summary (#426). Empty when the
    # instrument has never paid a dividend; key stats path falls
    # through cleanly.
    dividend_yield: Decimal | None = None
    try:
        from app.services.dividends import get_dividend_summary

        div_summary = get_dividend_summary(conn, instrument_id=instrument_id_int)
        dividend_yield = div_summary.ttm_yield_pct
    except Exception:
        logger.warning("get_dividend_summary failed", exc_info=True)

    has_local_values = any(v is not None for v in local_fundamentals.values())
    # Build the stats block when ANY input is available — SEC
    # fundamentals, dividend yield, or both. Without this gate, an
    # instrument with a dividend yield but no SEC fundamentals row
    # would silently drop the yield (Codex round 2 finding on PR for
    # #498/#499).
    if (use_local_sec and has_local_values) or dividend_yield is not None:
        stats_block = _build_local_stats(
            local_fundamentals,
            current_price=current_price,
            dividend_yield=dividend_yield,
        )
        # Block-level source label reflects which inputs actually
        # contributed: combined when both SEC fundamentals and the
        # dividend summary are populated, individual when only one
        # provided values. Avoids mislabelling a dividend-only block
        # as ``sec_xbrl`` (Codex review on PR for #499).
        if use_local_sec and has_local_values and dividend_yield is not None:
            key_stats_source = "sec_xbrl+sec_dividend_summary"
        elif use_local_sec and has_local_values:
            key_stats_source = "sec_xbrl"
        else:
            key_stats_source = "sec_dividend_summary"
    else:
        stats_block = None
        key_stats_source = "unavailable"

    source = {
        "identity": "local_db",
        "price": "quotes" if current_price is not None else "unavailable",
        "key_stats": key_stats_source,
    }

    # Coverage gates (#503 PR 2). Resolve via dedicated calls — do
    # NOT alias ``use_local_sec`` here. Today the two flags are the
    # same boolean (both come from ``_has_sec_cik``), but
    # ``use_local_sec`` is named for the local-XBRL preference path
    # and could narrow in future (e.g. "has CIK AND has ingested
    # XBRL"). The frontend gate must follow ``_has_sec_cik`` exactly,
    # not whatever predicate the local-pref path wants. Codex
    # review on PR #506 caught the aliasing risk.
    has_sec_cik = _has_sec_cik(conn, instrument_id_int)
    has_filings_coverage = _has_filings_coverage(conn, instrument_id_int)

    # Per-capability resolution (#515 PR 3). Frontend gates panels
    # on providers + data_present rather than the older has_sec_cik
    # / has_filings_coverage shim. Latter kept in the response for
    # the migration window — PR 3b retires them once frontend reads
    # ``capabilities`` directly.
    capabilities = _resolve_capabilities_payload(
        conn,
        instrument_id=instrument_id_int,
        exchange_id=identity.exchange,
    )

    return InstrumentSummary(
        instrument_id=row["instrument_id"],  # type: ignore[arg-type]
        is_tradable=row["is_tradable"],  # type: ignore[arg-type]
        coverage_tier=row["coverage_tier"],  # type: ignore[arg-type]
        session_profile=row["session_profile"],  # type: ignore[arg-type]
        identity=identity,
        price=price_block,
        key_stats=stats_block,
        source=source,
        has_sec_cik=has_sec_cik,
        has_filings_coverage=has_filings_coverage,
        capabilities=capabilities,
    )


def _resolve_capabilities_payload(
    conn: psycopg.Connection[object],
    *,
    instrument_id: int,
    exchange_id: str | None,
) -> dict[str, CapabilityCellPayload]:
    """Translate the resolver dataclass into the Pydantic payload.

    When the instrument's exchange is unknown (e.g. NULL exchange
    column from a partial universe sync), every capability comes
    back with empty ``providers`` — the resolver still runs so the
    response shape is uniform.
    """
    from app.services.capabilities import resolve_capabilities

    # Pass an unmatchable sentinel for NULL-exchange rows so the
    # resolver still runs — per-instrument augmentation via
    # ``external_identifiers`` (e.g. a SEC CIK) must flow through
    # even when the exchange row is missing. Codex round-1 finding
    # on PR #5XX: returning empty cells short-circuited the SEC
    # CIK augment and contradicted has_sec_cik on partially-synced
    # instruments.
    resolved = resolve_capabilities(
        conn,
        instrument_id=instrument_id,
        exchange_id=exchange_id if exchange_id is not None else "",
    )
    return {
        cap: CapabilityCellPayload(
            providers=list(cell.providers),
            data_present=cell.data_present,
        )
        for cap, cell in resolved.cells.items()
    }


@router.get("/{instrument_id}", response_model=InstrumentDetail)
def get_instrument(
    instrument_id: int,
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> InstrumentDetail:
    """Single instrument with latest quote, coverage tier, and external identifiers."""
    instrument_sql = f"""
        SELECT i.instrument_id, i.symbol, i.company_name, i.exchange,
               i.currency, i.sector, i.industry, i.country,
               i.is_tradable, i.first_seen_at, i.last_seen_at,
               c.coverage_tier,
               {_SESSION_PROFILE_SQL},
               q.bid, q.ask, q.last, q.spread_pct, q.quoted_at
        FROM instruments i
        LEFT JOIN quotes q USING (instrument_id)
        LEFT JOIN coverage c USING (instrument_id)
        LEFT JOIN exchanges e ON e.exchange_id = i.exchange
        WHERE i.instrument_id = %(instrument_id)s
    """

    identifiers_sql = """
        SELECT provider, identifier_type, identifier_value
        FROM external_identifiers
        WHERE instrument_id = %(instrument_id)s
        ORDER BY provider, identifier_type, identifier_value
    """

    params = {"instrument_id": instrument_id}

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(instrument_sql, params)
        row = cur.fetchone()

        if row is None:
            raise HTTPException(status_code=404, detail=f"Instrument {instrument_id} not found")

        cur.execute(identifiers_sql, params)
        id_rows = cur.fetchall()

    ext_ids = [
        ExternalIdentifier(
            provider=r["provider"],  # type: ignore[arg-type]
            identifier_type=r["identifier_type"],  # type: ignore[arg-type]
            identifier_value=r["identifier_value"],  # type: ignore[arg-type]
        )
        for r in id_rows
    ]

    return InstrumentDetail(
        instrument_id=row["instrument_id"],  # type: ignore[arg-type]
        symbol=row["symbol"],  # type: ignore[arg-type]
        company_name=row["company_name"],  # type: ignore[arg-type]
        exchange=row["exchange"],  # type: ignore[arg-type]
        currency=row["currency"],  # type: ignore[arg-type]
        sector=row["sector"],  # type: ignore[arg-type]
        industry=row["industry"],  # type: ignore[arg-type]
        country=row["country"],  # type: ignore[arg-type]
        is_tradable=row["is_tradable"],  # type: ignore[arg-type]
        first_seen_at=row["first_seen_at"],  # type: ignore[arg-type]
        last_seen_at=row["last_seen_at"],  # type: ignore[arg-type]
        coverage_tier=row["coverage_tier"],  # type: ignore[arg-type]
        session_profile=row["session_profile"],  # type: ignore[arg-type]
        latest_quote=_parse_quote(row),
        external_identifiers=ext_ids,
    )


# ---------------------------------------------------------------------------
# Institutional holdings reader (#730 PR 4)
# ---------------------------------------------------------------------------


class InstitutionalFilerHolding(BaseModel):
    """One filer's stake in this instrument as of a recent quarter."""

    filer_cik: str
    filer_name: str
    filer_type: str  # 'ETF' | 'INV' | 'INS' | 'BD' | 'OTHER'
    accession_number: str
    period_of_report: date
    shares: Decimal
    market_value_usd: Decimal | None
    voting_authority: str | None  # 'SOLE' | 'SHARED' | 'NONE' | None
    is_put_call: str | None  # 'PUT' | 'CALL' | None (None = underlying equity)


class InstitutionalHoldingsTotals(BaseModel):
    """Per-slice rollups for the ownership card consumer (#729).

    Slices follow the card's percentage-derivation contract. Only
    underlying-equity rows participate (``is_put_call IS NULL``);
    PUT / CALL exposure rows ship in ``filers`` for the drilldown
    table but do NOT contribute to the slice totals — counting an
    option position as ownership double-counts the underlying.
    """

    period_of_report: date
    institutions_shares: Decimal  # sum across filer_type IN ('INV','INS','BD','OTHER')
    etfs_shares: Decimal  # sum across filer_type = 'ETF'
    total_filers: int
    total_institutions_filers: int
    total_etfs_filers: int


class InstitutionalHoldingsResponse(BaseModel):
    symbol: str
    totals: InstitutionalHoldingsTotals | None  # None when no holdings on file
    filers: list[InstitutionalFilerHolding]


_DEFAULT_HOLDINGS_LIMIT = 50
_MAX_HOLDINGS_LIMIT = 500


@router.get("/{symbol}/institutional-holdings", response_model=InstitutionalHoldingsResponse)
def get_instrument_institutional_holdings(
    symbol: str,
    limit: int = Query(default=_DEFAULT_HOLDINGS_LIMIT, ge=1, le=_MAX_HOLDINGS_LIMIT),
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> InstitutionalHoldingsResponse:
    """13F-HR institutional + ETF holdings for one instrument.

    Returns the most recent quarter's holdings — the per-instrument
    ``period_of_report = MAX(period_of_report)`` cohort. Ownership
    card (#729) consumers use:

      * ``totals.institutions_shares`` / ``shares_outstanding`` for
        the Institutions slice.
      * ``totals.etfs_shares`` / ``shares_outstanding`` for the ETFs
        slice.
      * ``filers`` for the per-filer drilldown table (top-N by
        share count, defaulting to 50 — bump via ``?limit=`` up to
        500).

    Empty state: a non-covered or pre-ingest instrument returns
    ``200`` with ``totals=null`` and ``filers=[]``. The card
    consumer renders the empty-per-slice fallback. 404 is reserved
    for an unknown symbol.

    Per-slice semantics:
      * Only underlying-equity rows (``is_put_call IS NULL``) feed
        the totals — option exposure is excluded so a PUT position
        does not mis-attribute as long ownership.
      * PUT / CALL rows DO appear in ``filers`` for audit /
        drilldown.
      * 'INS' (insurance) and 'BD' (broker-dealer) labels are
        rolled into the institutions slice (their members file 13F
        as institutional managers).
    """
    symbol_clean = symbol.strip().upper()
    if not symbol_clean:
        raise HTTPException(status_code=400, detail="symbol is required")

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT instrument_id, symbol FROM instruments
            WHERE UPPER(symbol) = %(s)s
            ORDER BY is_primary_listing DESC, instrument_id ASC
            LIMIT 1
            """,
            {"s": symbol_clean},
        )
        inst_row = cur.fetchone()
    if inst_row is None:
        raise HTTPException(status_code=404, detail=f"Instrument {symbol} not found")
    instrument_id = int(inst_row["instrument_id"])  # type: ignore[arg-type]

    # Latest period_of_report on file. NULL means no holdings ingested
    # yet — return the empty payload rather than 404 so the card can
    # render its no-coverage fallback uniformly.
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT MAX(period_of_report) AS latest
            FROM institutional_holdings
            WHERE instrument_id = %s
            """,
            (instrument_id,),
        )
        latest_row = cur.fetchone()
    latest_period = latest_row["latest"] if latest_row is not None else None  # type: ignore[index]
    if latest_period is None:
        return InstitutionalHoldingsResponse(
            symbol=str(inst_row["symbol"]),  # type: ignore[arg-type]
            totals=None,
            filers=[],
        )

    # Per-slice totals + filer counts. Two distinct cohorts in one
    # query so the response is internally consistent:
    #
    #   * ``etfs_shares`` / ``institutions_shares`` and the slice
    #     filer counts (``total_etfs_filers`` /
    #     ``total_institutions_filers``) sum over the EQUITY-only
    #     subset (``is_put_call IS NULL``). Option exposure is
    #     excluded from the ownership-percentage rollup; counting
    #     a protective put as long ownership double-counts the
    #     underlying.
    #
    #   * ``total_filers`` counts EVERY distinct filer reporting
    #     this instrument in the latest quarter — equity + PUT +
    #     CALL — so it matches the drilldown ``filers`` list shown
    #     to the operator. Pre-fix this was equity-only too, which
    #     produced ``total_filers < len(filers)`` for any
    #     instrument with option-only filers. Codex caught this on
    #     the PR review.
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT
                COALESCE(SUM(h.shares) FILTER (
                    WHERE f.filer_type = 'ETF' AND h.is_put_call IS NULL
                ), 0) AS etfs_shares,
                COALESCE(SUM(h.shares) FILTER (
                    WHERE (f.filer_type IN ('INV','INS','BD','OTHER') OR f.filer_type IS NULL)
                    AND h.is_put_call IS NULL
                ), 0) AS institutions_shares,
                COUNT(DISTINCT f.filer_id) AS total_filers,
                COUNT(DISTINCT f.filer_id) FILTER (
                    WHERE f.filer_type = 'ETF' AND h.is_put_call IS NULL
                ) AS total_etfs_filers,
                COUNT(DISTINCT f.filer_id) FILTER (
                    WHERE (f.filer_type IN ('INV','INS','BD','OTHER') OR f.filer_type IS NULL)
                    AND h.is_put_call IS NULL
                ) AS total_institutions_filers
            FROM institutional_holdings h
            JOIN institutional_filers f USING (filer_id)
            WHERE h.instrument_id = %(iid)s
              AND h.period_of_report = %(period)s
            """,
            {"iid": instrument_id, "period": latest_period},
        )
        totals_row = cur.fetchone()
    if totals_row is None:
        # Defensive: the aggregate returns one row even when no
        # input rows exist (zeros + counts), so ``None`` here is
        # an unreachable invariant violation. Use HTTPException
        # rather than ``assert`` so the guard survives ``python -O``.
        raise HTTPException(status_code=500, detail="aggregate produced no row")

    totals = InstitutionalHoldingsTotals(
        period_of_report=latest_period,  # type: ignore[arg-type]
        institutions_shares=Decimal(totals_row["institutions_shares"] or 0),  # type: ignore[arg-type]
        etfs_shares=Decimal(totals_row["etfs_shares"] or 0),  # type: ignore[arg-type]
        total_filers=int(totals_row["total_filers"] or 0),  # type: ignore[arg-type]
        total_institutions_filers=int(totals_row["total_institutions_filers"] or 0),  # type: ignore[arg-type]
        total_etfs_filers=int(totals_row["total_etfs_filers"] or 0),  # type: ignore[arg-type]
    )

    # Top-N filers by share count for the drilldown table. Includes
    # PUT / CALL rows so the operator can see option exposure
    # alongside underlying equity. Tie-break: market value descending,
    # then accession_number ascending so a deterministic order
    # survives same-share filings.
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT f.cik AS filer_cik, f.name AS filer_name,
                   COALESCE(f.filer_type, 'OTHER') AS filer_type,
                   h.accession_number, h.period_of_report,
                   h.shares, h.market_value_usd,
                   h.voting_authority, h.is_put_call
            FROM institutional_holdings h
            JOIN institutional_filers f USING (filer_id)
            WHERE h.instrument_id = %(iid)s
              AND h.period_of_report = %(period)s
            ORDER BY h.shares DESC,
                     h.market_value_usd DESC NULLS LAST,
                     h.accession_number ASC
            LIMIT %(limit)s
            """,
            {"iid": instrument_id, "period": latest_period, "limit": limit},
        )
        rows = cur.fetchall()

    filers = [
        InstitutionalFilerHolding(
            filer_cik=str(r["filer_cik"]),  # type: ignore[arg-type]
            filer_name=str(r["filer_name"]),  # type: ignore[arg-type]
            filer_type=str(r["filer_type"]),  # type: ignore[arg-type]
            accession_number=str(r["accession_number"]),  # type: ignore[arg-type]
            period_of_report=r["period_of_report"],  # type: ignore[arg-type]
            shares=r["shares"],  # type: ignore[arg-type]
            market_value_usd=r["market_value_usd"],  # type: ignore[arg-type]
            voting_authority=r["voting_authority"],  # type: ignore[arg-type]
            is_put_call=r["is_put_call"],  # type: ignore[arg-type]
        )
        for r in rows
    ]

    return InstitutionalHoldingsResponse(
        symbol=str(inst_row["symbol"]),  # type: ignore[arg-type]
        totals=totals,
        filers=filers,
    )


# ---------------------------------------------------------------------------
# 13D/G blockholders — #766 PR 3 of 3
# ---------------------------------------------------------------------------


class BlockholderRow(BaseModel):
    """One block on the cap table — the latest non-superseded 13D / 13G
    filing for a (primary filer, issuer) pair.

    Joint filings have N reporting persons under one accession, all
    typically claiming the same beneficial ownership. The reader
    collapses them to one row per primary filer (the EDGAR
    submitter), picking the largest-aggregate reporter as the
    canonical "block representative". The ``additional_reporters``
    count surfaces the joint-filing depth so the operator knows the
    block is held jointly without inflating the totals.
    """

    filer_cik: str  # primary filer's CIK (the EDGAR submitter)
    filer_name: str
    reporter_cik: str | None  # representative reporter's CIK
    reporter_name: str
    submission_type: str  # 'SCHEDULE 13D' | 'SCHEDULE 13D/A' | 'SCHEDULE 13G' | 'SCHEDULE 13G/A'
    status: str  # 'active' | 'passive'
    accession_number: str
    aggregate_amount_owned: Decimal | None
    percent_of_class: Decimal | None
    additional_reporters: int  # joint-filing co-reporters omitted from this row
    date_of_event: date | None
    filed_at: datetime | None


class BlockholdersTotals(BaseModel):
    """Per-instrument blockholders rollup for the ownership card.

    ``blockholders_shares`` sums the per-block ``aggregate_amount_owned``
    across every block (one block per primary filer). The reader
    deduplicates joint-filing reporters via the per-filer DISTINCT ON
    so two indirect beneficial owners of the same 1.5M-share block
    contribute 1.5M, not 3M.

    ``active_shares`` and ``passive_shares`` partition the same total
    by ``status`` (13D = active, 13G = passive) so the card can show
    a stacked "engaged vs index" sub-bar if the operator wants it.

    ``as_of_date`` is the latest ``filed_at`` across the included
    blocks — drives the per-category freshness chip (#767).
    """

    blockholders_shares: Decimal
    active_shares: Decimal
    passive_shares: Decimal
    total_filers: int
    as_of_date: date | None


class BlockholdersResponse(BaseModel):
    symbol: str
    totals: BlockholdersTotals | None
    blockholders: list[BlockholderRow]


_DEFAULT_BLOCKHOLDERS_LIMIT = 50
_MAX_BLOCKHOLDERS_LIMIT = 500


@router.get("/{symbol}/blockholders", response_model=BlockholdersResponse)
def get_instrument_blockholders(
    symbol: str,
    limit: int = Query(default=_DEFAULT_BLOCKHOLDERS_LIMIT, ge=1, le=_MAX_BLOCKHOLDERS_LIMIT),
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> BlockholdersResponse:
    """13D / 13G blockholders for one instrument (#766 PR 3).

    Returns the latest non-superseded filing per primary-filer per
    issuer, with joint-filing reporters collapsed to a single
    representative row. Each row corresponds to one ≥5% block on the
    cap table.

    Empty state: a non-covered or pre-ingest instrument returns
    ``200`` with ``totals=null`` and ``blockholders=[]``. Card
    consumers render the empty-per-slice fallback. 404 is reserved
    for an unknown symbol.

    Aggregation semantics match :func:`app.services.blockholders.
    latest_blockholder_positions` but at the *primary filer* grain
    rather than per-reporter-identity, so the ownership card's
    blockholders slice does not double-count joint filers.
    """
    symbol_clean = symbol.strip().upper()
    if not symbol_clean:
        raise HTTPException(status_code=400, detail="symbol is required")

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT instrument_id, symbol FROM instruments
            WHERE UPPER(symbol) = %(s)s
            ORDER BY is_primary_listing DESC, instrument_id ASC
            LIMIT 1
            """,
            {"s": symbol_clean},
        )
        inst_row = cur.fetchone()
    if inst_row is None:
        raise HTTPException(status_code=404, detail=f"Instrument {symbol} not found")
    instrument_id = int(inst_row["instrument_id"])  # type: ignore[arg-type]

    # Per-reporter chain: pick the latest filing per
    # ``(reporter_identity, issuer_cik)`` where ``reporter_identity =
    # COALESCE(reporter_cik, reporter_name)``. Matches the schema's
    # hot-path index from migration 095 and the PR 2 aggregator's
    # supersession semantic — a 13D filed after a prior 13G/A by the
    # same reporter wins regardless of which submitter (filer_id)
    # routed it.
    #
    # ``additional_reporters`` is computed per-accession to surface
    # joint-filing depth on each row (e.g. "Carl Icahn + 2 others").
    # The totals query downstream dedupes by accession to avoid
    # double-counting joint filers.
    #
    # Codex pre-push review caught the prior version of this query
    # which deduped on ``filer_id`` (the EDGAR submitter, not the
    # beneficial-owner identity) — that broke supersession across
    # submitter changes and conflated distinct holders that shared
    # one submitter.
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            WITH per_reporter_chain AS (
                SELECT DISTINCT ON (COALESCE(bf.reporter_cik, bf.reporter_name), bf.issuer_cik)
                    bf.filer_id,
                    bf.accession_number,
                    bf.submission_type,
                    bf.status,
                    bf.reporter_cik,
                    bf.reporter_name,
                    bf.aggregate_amount_owned,
                    bf.percent_of_class,
                    bf.date_of_event,
                    bf.filed_at
                FROM blockholder_filings bf
                WHERE bf.instrument_id = %(iid)s
                ORDER BY
                    COALESCE(bf.reporter_cik, bf.reporter_name),
                    bf.issuer_cik,
                    bf.filed_at DESC NULLS LAST,
                    bf.accession_number DESC
            ),
            accession_reporter_count AS (
                SELECT
                    accession_number,
                    COUNT(*) AS reporter_count
                FROM per_reporter_chain
                GROUP BY accession_number
            )
            SELECT
                f.cik AS filer_cik,
                f.name AS filer_name,
                prc.accession_number,
                prc.submission_type,
                prc.status,
                prc.reporter_cik,
                prc.reporter_name,
                prc.aggregate_amount_owned,
                prc.percent_of_class,
                prc.date_of_event,
                prc.filed_at,
                COALESCE(arc.reporter_count, 1) - 1 AS additional_reporters
            FROM per_reporter_chain prc
            JOIN blockholder_filers f ON f.filer_id = prc.filer_id
            LEFT JOIN accession_reporter_count arc
              ON arc.accession_number = prc.accession_number
            ORDER BY prc.aggregate_amount_owned DESC NULLS LAST,
                     prc.filed_at DESC NULLS LAST,
                     prc.accession_number DESC,
                     prc.reporter_name
            LIMIT %(limit)s
            """,
            {"iid": instrument_id, "limit": limit},
        )
        block_rows = cur.fetchall()

    if not block_rows:
        return BlockholdersResponse(
            symbol=str(inst_row["symbol"]),  # type: ignore[arg-type]
            totals=None,
            blockholders=[],
        )

    # Totals span every block, not just the per-page slice. Compute
    # in a separate query so a small ``limit`` does not truncate the
    # rollup.
    #
    # Two-step rollup so joint-filing reporters (multiple per-reporter
    # chain rows that share an accession) do not double-count:
    #
    #   1. ``per_reporter_chain`` — same shape as the drilldown query
    #      above. One row per reporter chain.
    #   2. ``per_accession_block`` — collapse to one row per
    #      accession by picking the largest aggregate_amount_owned
    #      among the joint reporters. SEC instructions require all
    #      joint filers to claim the same beneficial ownership, so
    #      MAX is canonical (and tolerates the rare misfiling where
    #      one reporter's row defers to the prior cover page with
    #      NULL aggregate). ``MAX(filed_at)`` so the per-block
    #      filed_at is the most recent in the chain.
    #   3. Sum across blocks — that is the slice total.
    #
    # ``total_filers`` counts distinct *blocks* (= accessions in the
    # latest-per-reporter set), not reporter rows — matches the
    # operator's "how many ≥5% blocks are on the cap table" mental
    # model. Codex pre-push review caught the prior filer_id-keyed
    # approach for the same reason.
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            WITH per_reporter_chain AS (
                SELECT DISTINCT ON (COALESCE(bf.reporter_cik, bf.reporter_name), bf.issuer_cik)
                    bf.accession_number,
                    bf.status,
                    bf.aggregate_amount_owned,
                    bf.filed_at
                FROM blockholder_filings bf
                WHERE bf.instrument_id = %(iid)s
                ORDER BY
                    COALESCE(bf.reporter_cik, bf.reporter_name),
                    bf.issuer_cik,
                    bf.filed_at DESC NULLS LAST,
                    bf.accession_number DESC
            ),
            per_accession_block AS (
                SELECT
                    accession_number,
                    -- ``BOOL_OR(status = 'active')`` so an accession
                    -- with any active reporter row counts as active
                    -- in the partition (typical: all reporters of
                    -- one accession share the same status, but the
                    -- check survives an edge-case mixed-status
                    -- joint filing without raising).
                    BOOL_OR(status = 'active') AS is_active,
                    MAX(aggregate_amount_owned) AS aggregate_amount_owned,
                    MAX(filed_at) AS filed_at
                FROM per_reporter_chain
                GROUP BY accession_number
            )
            SELECT
                COALESCE(SUM(aggregate_amount_owned), 0) AS blockholders_shares,
                COALESCE(SUM(aggregate_amount_owned) FILTER (WHERE is_active), 0) AS active_shares,
                COALESCE(SUM(aggregate_amount_owned) FILTER (WHERE NOT is_active), 0) AS passive_shares,
                COUNT(*) AS total_filers,
                MAX(filed_at) AS latest_filed_at
            FROM per_accession_block
            """,
            {"iid": instrument_id},
        )
        totals_row = cur.fetchone()
    if totals_row is None:
        # Defensive: aggregate always returns one row even on empty
        # input. ``None`` here would be an invariant violation.
        raise HTTPException(status_code=500, detail="aggregate produced no row")

    latest_filed_at = totals_row["latest_filed_at"]  # type: ignore[index]
    as_of_date = latest_filed_at.date() if isinstance(latest_filed_at, datetime) else None

    totals = BlockholdersTotals(
        blockholders_shares=Decimal(totals_row["blockholders_shares"] or 0),  # type: ignore[arg-type]
        active_shares=Decimal(totals_row["active_shares"] or 0),  # type: ignore[arg-type]
        passive_shares=Decimal(totals_row["passive_shares"] or 0),  # type: ignore[arg-type]
        total_filers=int(totals_row["total_filers"] or 0),  # type: ignore[arg-type]
        as_of_date=as_of_date,
    )

    blockholders = [
        BlockholderRow(
            filer_cik=str(r["filer_cik"]),  # type: ignore[arg-type]
            filer_name=str(r["filer_name"]),  # type: ignore[arg-type]
            reporter_cik=(str(r["reporter_cik"]) if r["reporter_cik"] is not None else None),  # type: ignore[arg-type]
            reporter_name=str(r["reporter_name"]),  # type: ignore[arg-type]
            submission_type=str(r["submission_type"]),  # type: ignore[arg-type]
            status=str(r["status"]),  # type: ignore[arg-type]
            accession_number=str(r["accession_number"]),  # type: ignore[arg-type]
            aggregate_amount_owned=r["aggregate_amount_owned"],  # type: ignore[arg-type]
            percent_of_class=r["percent_of_class"],  # type: ignore[arg-type]
            additional_reporters=int(r["additional_reporters"] or 0),  # type: ignore[arg-type]
            date_of_event=r["date_of_event"],  # type: ignore[arg-type]
            filed_at=r["filed_at"],  # type: ignore[arg-type]
        )
        for r in block_rows
    ]

    return BlockholdersResponse(
        symbol=str(inst_row["symbol"]),  # type: ignore[arg-type]
        totals=totals,
        blockholders=blockholders,
    )


# ---------------------------------------------------------------------------
# Cross-channel deduped ownership rollup — Tier 0 (#789, parent #788)
# ---------------------------------------------------------------------------


class _DroppedSourceModel(BaseModel):
    source: Literal["form4", "form3", "13d", "13g", "def14a", "13f", "nport"]
    accession_number: str
    shares: Decimal
    as_of_date: date | None
    edgar_url: str | None


class _CorrectionAppliedModel(BaseModel):
    """A figure-changing correction applied at read time (#1639 / #1644 / #1647).

    First-class structured JSON so a machine consumer (or operator) sees WHY
    the institutions total changed, not just the corrected number. ``kind`` is a
    closed vocabulary:
      * ``suppressed_by_13f_nt`` (#1639) — stale 13F-HR removed (13F-NT for a later
        quarter). NT-specific fields set.
      * ``def14a_restates_institution`` (#1644) — proxy 5%-holder figure folded
        under a larger 13F family sum.
      * ``institutional_family_collapse`` (#1649) — a 13F shell figure folded under
        a larger consolidated proxy/13G family figure (gap-fill).
      * ``blockholder_group_collapse`` (#1645) — a Rule 13d-5 group counted once in the
        blockholders wedge.
      * ``insider_control_group_collapse`` (#1652) — a sponsor's GP/LP chain deemed block
        counted once across Form 4 / Form 3 / 13D / 13G (insiders slice).

    The NT-specific fields are null for the #1644/#1649 kinds; ``family_id`` /
    ``source_channel`` / ``winning_source`` / ``winning_accession`` / ``detail``
    carry the generic non-lossy provenance. ``filer_cik`` is null for a
    proxy-name-only fold."""

    kind: Literal[
        "suppressed_by_13f_nt",
        "def14a_restates_institution",
        "institutional_family_collapse",
        "blockholder_group_collapse",
        "insider_control_group_collapse",
    ]
    filer_cik: str | None
    filer_name: str
    shares_removed: Decimal
    superseded_period: date | None = None
    winning_nt_period: date | None = None
    winning_nt_accession: str | None = None
    family_id: str | None = None
    source_channel: Literal["form4", "form3", "13d", "13g", "def14a", "13f", "nport"] | None = None
    winning_source: Literal["form4", "form3", "13d", "13g", "def14a", "13f", "nport"] | None = None
    winning_accession: str | None = None
    detail: str = ""


class _FamilyMemberModel(BaseModel):
    """One constituent 13F sub-CIK row inside a collapsed institutional family
    (#1644/#1649). Display-only breakdown; NOT additive (the family holder's
    ``shares`` already counts it)."""

    filer_cik: str | None
    filer_name: str
    shares: Decimal
    source: Literal["form4", "form3", "13d", "13g", "def14a", "13f", "nport"]
    accession_number: str
    edgar_url: str | None
    as_of_date: date | None


class _HolderModel(BaseModel):
    filer_cik: str | None
    filer_name: str
    shares: Decimal
    pct_outstanding: Decimal
    winning_source: Literal["form4", "form3", "13d", "13g", "def14a", "13f", "nport"]
    winning_accession: str
    winning_edgar_url: str | None
    as_of_date: date | None
    filer_type: str | None
    dropped_sources: list[_DroppedSourceModel]
    family_members: list[_FamilyMemberModel] = []


class _SliceModel(BaseModel):
    category: Literal["insiders", "blockholders", "institutions", "etfs", "def14a_unmatched", "funds", "esop"]
    label: str
    total_shares: Decimal
    pct_outstanding: Decimal
    filer_count: int
    dominant_source: Literal["form4", "form3", "13d", "13g", "def14a", "13f", "nport"] | None
    holders: list[_HolderModel]
    # Tag added with #919 funds slice. ``pie_wedge`` slices contribute
    # to residual / concentration math; ``institution_subset`` (funds) and
    # ``proxy_disclosure`` (DEF 14A, #1659 — Rule 13d-3 deemed/overlapping
    # beneficial ownership, not additive) are memo overlays. Frontend filters
    # on this to decide whether to render in the pie vs as a memo panel.
    denominator_basis: Literal["pie_wedge", "institution_subset", "proxy_disclosure"] = "pie_wedge"
    # As-of coherence envelope (#1647 part 1). The as-of span of this slice's
    # deduped holders (incl. collapsed-family members) so a machine consumer
    # sees the figure sums across quarters. NULL-as_of-only slice → None/0/False.
    as_of_min: date | None = None
    as_of_max: date | None = None
    distinct_quarters: int = 0
    mixed_period: bool = False


class _ResidualModel(BaseModel):
    shares: Decimal
    pct_outstanding: Decimal
    label: str
    tooltip: str
    oversubscribed: bool


class _CategoryCoverageModel(BaseModel):
    known_filers: int
    estimated_universe: int | None
    pct_universe: Decimal | None
    state: Literal["no_data", "red", "unknown_universe", "amber", "green"]
    # Honest machine completeness flag (#1647 part 2). True ⇔ no real
    # filer-universe estimate for this category (figure is a floor). Real
    # coverage_ratio gate DEFERRED #790. Default True (back-compat with
    # pre-envelope payloads, which were all unknown-universe).
    is_estimate: bool = True


class _CoverageModel(BaseModel):
    state: Literal["no_data", "red", "unknown_universe", "amber", "green"]
    categories: dict[str, _CategoryCoverageModel]


class _ConcentrationModel(BaseModel):
    pct_outstanding_known: Decimal
    info_chip: str


class _BannerModel(BaseModel):
    state: Literal["no_data", "red", "unknown_universe", "amber", "green"]
    variant: Literal["error", "warning", "info", "success"]
    headline: str
    body: str


class _HistoricalSymbolModel(BaseModel):
    symbol: str
    effective_from: date
    effective_to: date | None
    source_event: str


class _SharesOutstandingSourceModel(BaseModel):
    accession_number: str | None
    concept: str | None
    form_type: str | None
    edgar_url: str | None


class _DualClassDenominatorModel(BaseModel):
    """Multi-class denominator caveat (#1646). Present only when this instrument
    shares its SEC CIK with another traded share class, so every percentage in
    the rollup is a combined-basis lower bound. ``note`` is server-owned copy the
    FE renders verbatim."""

    cik: str
    sibling_symbols: list[str]
    note: str


class _PerClassDenominatorModel(BaseModel):
    """Per-class denominator applied (#788). Present only when a verified FSDS
    per-class share count replaced the issuer's combined all-class count, so every
    percentage is per-class-true and the #1646 caveat is superseded (the two are
    mutually exclusive). ``note`` is server-owned copy the FE renders verbatim."""

    cik: str
    class_member: str
    period_end: date
    per_class_shares: Decimal
    combined_shares: Decimal
    source_adsh: str
    source_fsds_qtr: str
    note: str


class _SanityChecksModel(BaseModel):
    """Raw plausibility facts over the pie-wedge slices (#1647 part 4). NOT
    pass/fail — measurements a decision agent can reason over to catch the next
    silent inflation (the existing ``residual.oversubscribed`` guard cannot).
    Memo-overlay slices excluded. ``shares_outstanding <= 0`` → zeroed."""

    max_distinct_quarters: int = 0
    institutions_pct: Decimal = Decimal(0)
    institutions_over_100pct: bool = False
    largest_single_holder_pct: Decimal = Decimal(0)
    any_pie_slice_over_100pct: bool = False


class _DenominatorCrossCheckModel(BaseModel):
    """Independent denominator tie-out (#1647 part 5). Facts, not a gate — it never
    changes a share count. ``method`` encodes the comparison's STRENGTH:
    ``independent_concept`` (single-class dei cover-page vs us-gaap balance-sheet — a
    real independent cross-source); ``per_class_subset_bound`` (dual-class structural
    backstop — only flags the impossible sibling-sum > combined; not independent);
    ``unavailable``. ``primary_value`` / ``comparison_value`` are the two figures THIS
    check compares (``primary_concept`` names which); ``pct_diff`` =
    (primary - comparison) / comparison. The aggregate ownership PERCENTAGES have no
    independent source (all vendors sum the same SEC filings + disagree by method) —
    documented in the metrics-analyst skill, not a field. ``note`` is server-owned FE copy."""

    method: Literal["independent_concept", "per_class_subset_bound", "unavailable"] = "unavailable"
    primary_value: Decimal | None = None
    primary_concept: str | None = None
    comparison_value: Decimal | None = None
    comparison_concept: str | None = None
    primary_as_of: date | None = None
    comparison_as_of: date | None = None
    as_of_delta_days: int | None = None
    pct_diff: Decimal | None = None
    status: Literal["agrees", "minor_skew", "diverges", "plausible", "unavailable"] = "unavailable"
    note: str = "No independent SEC figure on file to cross-check the share-count denominator."


class OwnershipRollupResponse(BaseModel):
    """Cross-channel deduped ownership snapshot (#789).

    The single denominator is ``shares_outstanding`` (XBRL DEI).
    Treasury renders as an additive top wedge — not part of the
    denominator, not deduped against. The ``residual`` is the
    explicit ``Public / unattributed`` block; ``coverage`` drives the
    banner state machine via universe coverage (NOT float
    concentration); ``concentration`` is shown separately as an info
    chip. See ``docs/superpowers/specs/2026-05-03-ownership-tier0-and-cik-history-design.md``
    for the contract."""

    symbol: str
    instrument_id: int
    shares_outstanding: Decimal | None
    shares_outstanding_as_of: date | None
    shares_outstanding_source: _SharesOutstandingSourceModel
    treasury_shares: Decimal | None
    treasury_as_of: date | None
    slices: list[_SliceModel]
    residual: _ResidualModel
    concentration: _ConcentrationModel
    coverage: _CoverageModel
    banner: _BannerModel
    # Symbol chain from instrument_symbol_history (Batch 7 of #788).
    # Empty for instruments without a backfilled chain. Frontend
    # renders a "Filed as X" callout when the chain has any symbol
    # other than the current one.
    historical_symbols: list[_HistoricalSymbolModel]
    # Figure-changing corrections applied at read time (#1639 / #1647). Today:
    # 13F-NT supersessions. ``suppressed_by_notice`` is the convenience count of
    # the ``suppressed_by_13f_nt`` kind so a consumer can branch without
    # iterating. Empty list / 0 when no correction fired.
    corrections_applied: list[_CorrectionAppliedModel]
    suppressed_by_notice: int
    # Multi-class denominator caveat (#1646). Non-null only for one share class of
    # a multi-class issuer (GOOG/GOOGL, BRK.A/BRK.B); null for single-class issuers
    # and the no_data path. When set, the FE renders the caveat callout and every
    # percentage should be read as a combined-basis lower bound.
    dual_class_denominator: _DualClassDenominatorModel | None
    # Per-class denominator applied (#788). Non-null only when a verified FSDS
    # per-class share count replaced the combined denominator (mutually exclusive
    # with ``dual_class_denominator``). When set, percentages are per-class-true and
    # the FE renders the per-class info note instead of the #1646 caveat.
    per_class_denominator: _PerClassDenominatorModel | None = None
    # Sanity-invariant facts over the pie-wedge slices (#1647 part 4). Always
    # present; zeroed on the no_data path. Default so older callers/tests need
    # no change.
    sanity: _SanityChecksModel = Field(default_factory=_SanityChecksModel)
    # Independent denominator tie-out (#1647 part 5). Always present; ``unavailable``
    # on the no_data path / when no comparison figure is on file. Default so older
    # callers/tests need no change.
    denominator_cross_check: _DenominatorCrossCheckModel = Field(default_factory=_DenominatorCrossCheckModel)
    computed_at: datetime


def _rollup_to_response(
    rollup: ownership_rollup.OwnershipRollup,
) -> OwnershipRollupResponse:
    return OwnershipRollupResponse(
        symbol=rollup.symbol,
        instrument_id=rollup.instrument_id,
        shares_outstanding=rollup.shares_outstanding,
        shares_outstanding_as_of=rollup.shares_outstanding_as_of,
        shares_outstanding_source=_SharesOutstandingSourceModel(
            accession_number=rollup.shares_outstanding_source.accession_number,
            concept=rollup.shares_outstanding_source.concept,
            form_type=rollup.shares_outstanding_source.form_type,
            edgar_url=rollup.shares_outstanding_source.edgar_url,
        ),
        treasury_shares=rollup.treasury_shares,
        treasury_as_of=rollup.treasury_as_of,
        slices=[
            _SliceModel(
                category=s.category,
                label=s.label,
                total_shares=s.total_shares,
                pct_outstanding=s.pct_outstanding,
                filer_count=s.filer_count,
                dominant_source=s.dominant_source,
                denominator_basis=s.denominator_basis,
                as_of_min=s.as_of_min,
                as_of_max=s.as_of_max,
                distinct_quarters=s.distinct_quarters,
                mixed_period=s.mixed_period,
                holders=[
                    _HolderModel(
                        filer_cik=h.filer_cik,
                        filer_name=h.filer_name,
                        shares=h.shares,
                        pct_outstanding=h.pct_outstanding,
                        winning_source=h.winning_source,
                        winning_accession=h.winning_accession,
                        winning_edgar_url=h.winning_edgar_url,
                        as_of_date=h.as_of_date,
                        filer_type=h.filer_type,
                        dropped_sources=[
                            _DroppedSourceModel(
                                source=d.source,
                                accession_number=d.accession_number,
                                shares=d.shares,
                                as_of_date=d.as_of_date,
                                edgar_url=d.edgar_url,
                            )
                            for d in h.dropped_sources
                        ],
                        family_members=[
                            _FamilyMemberModel(
                                filer_cik=m.filer_cik,
                                filer_name=m.filer_name,
                                shares=m.shares,
                                source=m.source,
                                accession_number=m.accession_number,
                                edgar_url=m.edgar_url,
                                as_of_date=m.as_of_date,
                            )
                            for m in h.family_members
                        ],
                    )
                    for h in s.holders
                ],
            )
            for s in rollup.slices
        ],
        residual=_ResidualModel(
            shares=rollup.residual.shares,
            pct_outstanding=rollup.residual.pct_outstanding,
            label=rollup.residual.label,
            tooltip=rollup.residual.tooltip,
            oversubscribed=rollup.residual.oversubscribed,
        ),
        concentration=_ConcentrationModel(
            pct_outstanding_known=rollup.concentration.pct_outstanding_known,
            info_chip=rollup.concentration.info_chip,
        ),
        coverage=_CoverageModel(
            state=rollup.coverage.state,
            categories={
                k: _CategoryCoverageModel(
                    known_filers=c.known_filers,
                    estimated_universe=c.estimated_universe,
                    pct_universe=c.pct_universe,
                    state=c.state,
                    is_estimate=c.is_estimate,
                )
                for k, c in rollup.coverage.categories.items()
            },
        ),
        banner=_BannerModel(
            state=rollup.banner.state,
            variant=rollup.banner.variant,
            headline=rollup.banner.headline,
            body=rollup.banner.body,
        ),
        historical_symbols=[
            _HistoricalSymbolModel(
                symbol=h.symbol,
                effective_from=h.effective_from,
                effective_to=h.effective_to,
                source_event=h.source_event,
            )
            for h in rollup.historical_symbols
        ],
        corrections_applied=[
            _CorrectionAppliedModel(
                kind=c.kind,  # type: ignore[arg-type]  # closed vocab, validated by Pydantic Literal
                filer_cik=c.filer_cik,
                filer_name=c.filer_name,
                shares_removed=c.shares_removed,
                superseded_period=c.superseded_period,
                winning_nt_period=c.winning_nt_period,
                winning_nt_accession=c.winning_nt_accession,
                family_id=c.family_id,
                source_channel=c.source_channel,
                winning_source=c.winning_source,
                winning_accession=c.winning_accession,
                detail=c.detail,
            )
            for c in rollup.corrections_applied
        ],
        suppressed_by_notice=sum(1 for c in rollup.corrections_applied if c.kind == "suppressed_by_13f_nt"),
        dual_class_denominator=(
            _DualClassDenominatorModel(
                cik=rollup.dual_class_denominator.cik,
                sibling_symbols=list(rollup.dual_class_denominator.sibling_symbols),
                note=rollup.dual_class_denominator.note,
            )
            if rollup.dual_class_denominator is not None
            else None
        ),
        per_class_denominator=(
            _PerClassDenominatorModel(
                cik=rollup.per_class_denominator.cik,
                class_member=rollup.per_class_denominator.class_member,
                period_end=rollup.per_class_denominator.period_end,
                per_class_shares=rollup.per_class_denominator.per_class_shares,
                combined_shares=rollup.per_class_denominator.combined_shares,
                source_adsh=rollup.per_class_denominator.source_adsh,
                source_fsds_qtr=rollup.per_class_denominator.source_fsds_qtr,
                note=rollup.per_class_denominator.note,
            )
            if rollup.per_class_denominator is not None
            else None
        ),
        sanity=_SanityChecksModel(
            max_distinct_quarters=rollup.sanity.max_distinct_quarters,
            institutions_pct=rollup.sanity.institutions_pct,
            institutions_over_100pct=rollup.sanity.institutions_over_100pct,
            largest_single_holder_pct=rollup.sanity.largest_single_holder_pct,
            any_pie_slice_over_100pct=rollup.sanity.any_pie_slice_over_100pct,
        ),
        denominator_cross_check=_DenominatorCrossCheckModel(
            method=rollup.denominator_cross_check.method,
            primary_value=rollup.denominator_cross_check.primary_value,
            primary_concept=rollup.denominator_cross_check.primary_concept,
            comparison_value=rollup.denominator_cross_check.comparison_value,
            comparison_concept=rollup.denominator_cross_check.comparison_concept,
            primary_as_of=rollup.denominator_cross_check.primary_as_of,
            comparison_as_of=rollup.denominator_cross_check.comparison_as_of,
            as_of_delta_days=rollup.denominator_cross_check.as_of_delta_days,
            pct_diff=rollup.denominator_cross_check.pct_diff,
            status=rollup.denominator_cross_check.status,
            note=rollup.denominator_cross_check.note,
        ),
        computed_at=rollup.computed_at,
    )


class OwnershipHistoryPointResponse(BaseModel):
    """One point on a holder's history series (#840.F).

    ``holder_count`` (#922): filers contributing to an aggregate
    bucket; ``None`` on per-holder series and issuer-level treasury
    points."""

    period_end: date
    ownership_nature: str
    shares: Decimal | None
    source: str
    source_accession: str | None
    filed_at: datetime | None
    holder_count: int | None = None


class AggregateCoverageResponse(BaseModel):
    """Coverage-coherence envelope for an aggregate series (#1648).

    Mirrors :class:`ownership_history.AggregateCoverage` field-for-field
    (keep in sync with ``frontend/src/api/ownershipHistory.ts``). Facts a
    consumer reads to tell coverage-driven slope from real flow; ``None`` on
    per-holder responses (coverage spread is meaningless for one filer)."""

    bucket_count: int
    as_of_min: date | None
    as_of_max: date | None
    holder_count_min: int | None
    holder_count_max: int | None
    holder_count_latest: int | None


class OwnershipHistoryResponse(BaseModel):
    """Time-bucketed deduped ownership history (#840.F).

    Per Codex plan-review #6: each point is the dedup winner for
    ``(period_end, ownership_nature)`` — NOT raw observations. The
    chart consumer renders one line per nature, with provenance
    fields driving click-through to the source filing."""

    symbol: str
    instrument_id: int
    category: str
    holder_id: str | None
    points: list[OwnershipHistoryPointResponse]
    # Coverage-coherence envelope (#1648), populated only for
    # ``aggregate=true`` requests; ``None`` on per-holder series.
    coverage: AggregateCoverageResponse | None = None


@router.get(
    "/{symbol}/ownership-history",
    response_model=OwnershipHistoryResponse,
)
def get_instrument_ownership_history(
    symbol: str,
    category: str,
    holder_id: str | None = None,
    aggregate: bool = False,
    from_date: date | None = None,
    to_date: date | None = None,
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> OwnershipHistoryResponse:
    """Time-bucketed deduped ownership history for one instrument ×
    category × optional holder.

    Operator question this answers: *"how has Vanguard's AAPL
    position shifted over the last 8 quarters?"* — call with
    ``category=institutions, holder_id=0000102909``.

    ``aggregate=true`` (#922) answers the CATEGORY-level question
    ("how have institutions in total trended?"): per-quarter sums
    over the per-filer dedup winners. Only ``institutions`` and
    ``treasury`` aggregate honestly — event-driven categories
    (insiders / blockholders / def14a) would only count holders who
    happened to file in each period.

    Categories: ``insiders``, ``blockholders``, ``institutions``,
    ``treasury``, ``def14a``. ``holder_id`` semantics depend on
    category (see :func:`ownership_history.get_ownership_history`).

    Date filters are inclusive valid-time bounds on ``period_end``.
    Reads run inside ``snapshot_read`` so the timeseries reconciles
    against one consistent snapshot."""
    symbol_clean = symbol.strip().upper()
    if not symbol_clean:
        raise HTTPException(status_code=400, detail="symbol is required")
    if category not in ("insiders", "blockholders", "institutions", "treasury", "def14a"):
        raise HTTPException(status_code=400, detail=f"unknown category {category!r}")
    if aggregate:
        # ANY supplied holder_id (including blank) conflicts — a blank
        # value slipping through would echo holder_id="" in the
        # response and mask caller bugs (Codex ckpt-2 S3).
        if holder_id is not None:
            raise HTTPException(
                status_code=400,
                detail="aggregate=true and holder_id are mutually exclusive",
            )
        if category not in ownership_history.AGGREGATE_CATEGORIES:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"category {category!r} has no honest aggregate series: "
                    "event-driven filings would only count holders who filed "
                    "in each period (carry-forward not implemented); query "
                    "per-holder instead"
                ),
            )
    # Codex pre-push review for #840.F: holder-scoped categories
    # MUST be called with a holder_id. Without one, DISTINCT ON
    # ``(period_end, ownership_nature)`` returns one arbitrary
    # winning holder per period and silently drops the rest — that
    # would mislead the chart consumer. Treasury is issuer-level so
    # holder_id is ignored there.
    holder_scoped = ("insiders", "blockholders", "institutions", "def14a")
    if not aggregate and category in holder_scoped and (holder_id is None or not holder_id.strip()):
        raise HTTPException(
            status_code=400,
            detail=f"holder_id is required for category {category!r}",
        )

    with snapshot_read(conn):
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                """
                SELECT instrument_id, symbol FROM instruments
                WHERE UPPER(symbol) = %(s)s
                ORDER BY is_primary_listing DESC, instrument_id ASC
                LIMIT 1
                """,
                {"s": symbol_clean},
            )
            inst_row = cur.fetchone()
        if inst_row is None:
            raise HTTPException(status_code=404, detail=f"Instrument {symbol} not found")
        coverage_resp: AggregateCoverageResponse | None = None
        if aggregate:
            points = ownership_history.get_ownership_category_totals(
                conn,
                instrument_id=int(inst_row["instrument_id"]),  # type: ignore[arg-type]
                category=category,  # type: ignore[arg-type]
                from_date=from_date,
                to_date=to_date,
            )
            # Coverage-coherence envelope (#1648) — aggregate only; the spread
            # tells a consumer when a Q/Q change is filing coverage, not flow.
            cov = ownership_history.summarise_aggregate_coverage(points)
            coverage_resp = AggregateCoverageResponse(
                bucket_count=cov.bucket_count,
                as_of_min=cov.as_of_min,
                as_of_max=cov.as_of_max,
                holder_count_min=cov.holder_count_min,
                holder_count_max=cov.holder_count_max,
                holder_count_latest=cov.holder_count_latest,
            )
        else:
            points = ownership_history.get_ownership_history(
                conn,
                instrument_id=int(inst_row["instrument_id"]),  # type: ignore[arg-type]
                category=category,  # type: ignore[arg-type]
                holder_id=holder_id,
                from_date=from_date,
                to_date=to_date,
            )
    return OwnershipHistoryResponse(
        symbol=str(inst_row["symbol"]),  # type: ignore[arg-type]
        instrument_id=int(inst_row["instrument_id"]),  # type: ignore[arg-type]
        category=category,
        holder_id=holder_id,
        points=[
            OwnershipHistoryPointResponse(
                period_end=p.period_end,
                ownership_nature=p.ownership_nature,
                shares=p.shares,
                source=p.source,
                source_accession=p.source_accession,
                filed_at=p.filed_at,
                holder_count=p.holder_count,
            )
            for p in points
        ],
        coverage=coverage_resp,
    )


@router.get(
    "/{symbol}/ownership-rollup",
    response_model=OwnershipRollupResponse,
)
def get_instrument_ownership_rollup(
    symbol: str,
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> OwnershipRollupResponse:
    """Cross-channel deduped ownership rollup. Tier 0 of #788.

    Reads run inside one ``snapshot_read`` block so the per-slice
    totals, residual, and coverage banner all reconcile against a
    single REPEATABLE READ snapshot. Codex spec review caught a
    prior anti-pattern that would have produced a SAVEPOINT instead
    of a fresh snapshot on the pooled connection.

    Empty / pre-ingest state: ``slices=[]``, banner state = either
    ``no_data`` (no XBRL outstanding) or ``unknown_universe``
    (outstanding present but no per-category universe estimates yet).
    Both render 200 OK with the appropriate banner. 404 is reserved
    for unknown symbols.
    """
    symbol_clean = symbol.strip().upper()
    if not symbol_clean:
        raise HTTPException(status_code=400, detail="symbol is required")

    with snapshot_read(conn):
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                """
                SELECT instrument_id, symbol FROM instruments
                WHERE UPPER(symbol) = %(s)s
                ORDER BY is_primary_listing DESC, instrument_id ASC
                LIMIT 1
                """,
                {"s": symbol_clean},
            )
            inst_row = cur.fetchone()
        if inst_row is None:
            raise HTTPException(status_code=404, detail=f"Instrument {symbol} not found")
        rollup = ownership_rollup.get_ownership_rollup(
            conn,
            symbol=str(inst_row["symbol"]),  # type: ignore[arg-type]
            instrument_id=int(inst_row["instrument_id"]),  # type: ignore[arg-type]
        )
    return _rollup_to_response(rollup)


# Slice categories present on ``OwnershipSlice.category``. The
# frontend's ``CATEGORY_LABELS`` set also includes ``treasury`` —
# treasury is a memo row in the CSV (additive wedge on the chart),
# not a holders slice. Treated as a valid filter value below: it
# scopes the CSV to the treasury memo + residual rows only.
_ROLLUP_CSV_SLICE_CATEGORIES: frozenset[str] = frozenset(
    {"insiders", "blockholders", "institutions", "etfs", "def14a_unmatched", "funds", "esop"},
)
_ROLLUP_CSV_CATEGORIES: frozenset[str] = _ROLLUP_CSV_SLICE_CATEGORIES | {"treasury"}


def rollup_csv_slice_filter(category: str) -> frozenset[str]:
    """Slice categories a ``?category=`` CSV filter keeps.

    ``treasury`` keeps no slices (memo + residual rows only). Every
    other category maps to its own slice — including
    ``def14a_unmatched``, which since #1659 is a NON-ADDITIVE memo
    overlay (``denominator_basis=proxy_disclosure``) but keeps its own
    L2 filer category + CSV scope so the proxy holders stay inspectable
    / exportable as a cross-check. The CSV scope matches the chart/table
    1:1: ``?category=insiders&view=raw`` carries insiders only, and
    ``?category=def14a_unmatched`` carries the proxy-only holders.
    (Pre-#1627 the chart/table folded DEF 14A into insiders and this
    filter mirrored that fold — prevention-log
    #1767; un-folding one surface requires un-folding all three.)
    """
    if category == "treasury":
        return frozenset()
    return frozenset({category})


@router.get(
    "/{symbol}/ownership-rollup/export.csv",
    response_class=PlainTextResponse,
)
def get_instrument_ownership_rollup_csv(
    symbol: str,
    category: str | None = Query(
        default=None,
        description=(
            "Optional category filter: insiders | blockholders | institutions "
            "| etfs | def14a_unmatched | funds | esop | treasury. Slice "
            "categories scope the export to that slice's holders; ``treasury`` "
            "drops all slice holders and emits only the treasury + residual "
            "memo rows. ``funds`` and ``esop`` are memo-overlay slices — their "
            "rows render with the ``__memo:<category>__`` prefix in the output "
            "CSV so they are outside the additive (treasury + residual + "
            "Σ pie-wedge) reconciliation. Without ``category``, every slice "
            "is exported."
        ),
    ),
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> PlainTextResponse:
    """CSV export of the canonical deduped ownership rollup
    (Chain 2.8 of #788).

    Same data shape the operator sees in the L2 ownership card,
    flattened to one row per surviving holder + treasury memo +
    residual memo. ``Content-Disposition: attachment`` so the
    browser saves rather than rendering. Header always emitted so
    an automation pipe is branchless on empty rollups.

    ``?category=`` scopes the export to one slice — drives the L2
    page's "download CSV" button when the operator has drilled into
    a single category. Without it, every slice is exported.

    Reads run inside ``snapshot_read`` so the per-slice totals,
    treasury, and residual all reconcile against one REPEATABLE
    READ snapshot. Same isolation contract as the JSON rollup
    endpoint."""
    symbol_clean = symbol.strip().upper()
    if not symbol_clean:
        raise HTTPException(status_code=400, detail="symbol is required")
    if category is not None and category not in _ROLLUP_CSV_CATEGORIES:
        raise HTTPException(
            status_code=400,
            detail=(f"Unknown category {category!r}; expected one of {sorted(_ROLLUP_CSV_CATEGORIES)}"),
        )

    with snapshot_read(conn):
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                """
                SELECT instrument_id, symbol FROM instruments
                WHERE UPPER(symbol) = %(s)s
                ORDER BY is_primary_listing DESC, instrument_id ASC
                LIMIT 1
                """,
                {"s": symbol_clean},
            )
            inst_row = cur.fetchone()
        if inst_row is None:
            raise HTTPException(status_code=404, detail=f"Instrument {symbol} not found")
        rollup = ownership_rollup.get_ownership_rollup(
            conn,
            symbol=str(inst_row["symbol"]),  # type: ignore[arg-type]
            instrument_id=int(inst_row["instrument_id"]),  # type: ignore[arg-type]
        )

    if category is not None:
        # Server-side slice filter so the FE's L2 ``?category=`` filter
        # state can flow into the CSV without a client-side build.
        # ``dataclasses.replace`` keeps every other field
        # (residual, treasury, banner, computed_at) intact — the
        # operator still sees the canonical residual + treasury memo
        # rows even when the slice list is filtered.
        #
        # ``category=treasury`` is a valid filter even though treasury
        # is a memo row (not a slice). Drop ALL slices and keep the
        # treasury + residual memo rows — preserves the prior
        # ``buildCsv(filteredRows)`` behavior where ``?category=treasury``
        # only emitted the treasury row. Codex Chain 2.8 follow-up
        # caught the prior version 400ing on ``treasury``.
        from dataclasses import replace

        wanted = rollup_csv_slice_filter(category)
        filtered_slices = tuple(s for s in rollup.slices if s.category in wanted)
        rollup = replace(rollup, slices=filtered_slices)

    return PlainTextResponse(
        content=ownership_rollup.build_rollup_csv(rollup),
        media_type="text/csv",
        headers={
            "Content-Disposition": f'attachment; filename="{symbol_clean}_ownership_rollup.csv"',
        },
    )


# ---------------------------------------------------------------------------
# Risk metrics (#591 PR-B, Task B6) — read-through of the persisted two-layer
# risk-metrics tables plus on-read display series.
# ---------------------------------------------------------------------------


class RiskWindowMetrics(BaseModel):
    """All persisted risk scalars + per-metric statuses for ONE window.

    Every numeric field is a FRACTION (0.10 == 10%) or dimensionless, and
    nullable — a thin / invalid history yields NULL plus a flagging status,
    never a fabricated zero. Statuses pass through verbatim from the persisted
    columns.
    """

    window_key: str
    cagr: Decimal | None
    excess_cagr_vs_spy: Decimal | None
    max_drawdown: Decimal | None
    current_drawdown: Decimal | None
    vol_annualized: Decimal | None
    beta: Decimal | None
    beta_r2: Decimal | None
    calmar: Decimal | None
    skew: Decimal | None
    excess_kurtosis: Decimal | None
    var_5: Decimal | None
    worst_day: Decimal | None
    best_day: Decimal | None
    trailing_1m: Decimal | None
    trailing_3m: Decimal | None
    trailing_6m: Decimal | None
    trailing_1y: Decimal | None
    excess_trailing_1m: Decimal | None
    excess_trailing_3m: Decimal | None
    excess_trailing_6m: Decimal | None
    excess_trailing_1y: Decimal | None
    n_returns: int | None
    beta_n_obs: int | None
    window_days: int | None
    cagr_status: str | None
    vol_status: str | None
    beta_status: str | None
    drawdown_status: str | None
    distribution_status: str | None
    calmar_status: str | None
    trailing_status: str | None
    excess_cagr_status: str | None
    # Sector-relative beta/excess (#1674): a 2nd OLS + excess-CAGR vs the
    # instrument's sector SPDR ETF (top-level ``sector_benchmark_symbol`` names
    # which SPDR). Null + ``benchmark_missing`` status when no sector resolved.
    sector_beta: Decimal | None
    sector_beta_r2: Decimal | None
    sector_beta_n_obs: int | None
    sector_beta_status: str | None
    sector_excess_cagr: Decimal | None
    sector_excess_cagr_status: str | None
    # Total return (#1635): price return + reinvested per-share dividends. tr_calmar
    # = tr_cagr / |max_drawdown|. tr_status (own axis) is {ok, tr_incomplete,
    # no_dividends}; tr_n_periods = dividend periods reinvested in the window.
    tr_cagr: Decimal | None
    tr_calmar: Decimal | None
    tr_status: str | None
    tr_n_periods: int | None


class DrawdownPoint(BaseModel):
    """One point on the running-peak underwater curve (dd ≤ 0)."""

    date: date
    drawdown: Decimal


class RollingVolPoint(BaseModel):
    """One point on the trailing rolling-volatility line (annualized fraction)."""

    date: date
    vol: Decimal


class HistogramBin(BaseModel):
    """One bin of the daily-return distribution histogram."""

    lower: Decimal
    upper: Decimal
    count: int


class BetaScatterPoint(BaseModel):
    """One date-aligned (SPY return, instrument return) pair for the beta scatter."""

    spy_return: Decimal
    inst_return: Decimal


class RiskSeries(BaseModel):
    """On-read display series for the risk drill charts.

    Computed at request time from the price series cut at the persisted
    ``as_of_date`` (NOT persisted) — purely presentational. ``beta`` /
    ``beta_r2`` here are the FULL-series fit shown alongside the scatter; the
    per-window betas live on :class:`RiskWindowMetrics`. Beta fields are null and
    ``beta_scatter`` empty when no benchmark series is available.
    """

    drawdown_curve: list[DrawdownPoint]
    rolling_vol: list[RollingVolPoint]
    return_histogram: list[HistogramBin]
    beta_scatter: list[BetaScatterPoint]
    beta: Decimal | None
    beta_r2: Decimal | None


class InstrumentRiskMetrics(BaseModel):
    """Response shape for GET /instruments/{symbol}/risk-metrics.

    ``windows`` is empty and ``series``/``as_of_date`` null when the instrument
    has no persisted risk rows (never computed). ``benchmark_symbol`` is null
    when no benchmark was resolved at compute time.
    """

    symbol: str
    as_of_date: date | None
    benchmark_symbol: str | None
    sector_benchmark_symbol: str | None
    metric_version: str
    windows: list[RiskWindowMetrics]
    series: RiskSeries | None


class PortfolioRelativeRiskResponse(BaseModel):
    """Response shape for GET /instruments/{symbol}/portfolio-risk (#1636).

    The candidate's risk relative to the operator's CURRENT book — a
    current-exposure covariance estimate (today's weights over past returns), NOT
    realized book history. Every scalar is nullable; ``status`` carries the
    degraded cases (``empty_book`` / ``book_history_unavailable`` /
    ``single_holding_is_candidate`` / ``insufficient_history``). NULLs are never
    coerced to 0.

    - ``portfolio_beta`` = cov(candidate, book) / var(book): >1 amplifies book
      risk, <1 dampens, <0 hedges.
    - ``marginal_risk_contribution`` = portfolio_beta × portfolio_vol: the
      candidate's annualized risk per unit weight at the margin (a small add
      funded pro-rata from the book reduces per-unit risk iff this < portfolio_vol).
    All return/vol figures are fractions; vols are annualized.
    """

    symbol: str
    as_of_date: date | None
    status: PortfolioRiskStatus
    holdings_count: int
    already_held: bool
    current_weight: Decimal | None
    portfolio_beta: Decimal | None
    correlation: Decimal | None
    candidate_vol: Decimal | None
    portfolio_vol: Decimal | None
    marginal_risk_contribution: Decimal | None
    n_obs: int


# Display-series tuning. The rolling-vol window is in RETURNS (≈ trading days).
_ROLLING_VOL_WINDOW = 30
_HISTOGRAM_BINS = 30
# Windows surfaced shortest → full.
_RISK_WINDOW_ORDER: dict[str, int] = {"1y": 0, "3y": 1, "full": 2}


def _rolling_vol_series(
    inst_returns: list[tuple[date, Decimal]],
    window: int = _ROLLING_VOL_WINDOW,
) -> list[RollingVolPoint]:
    """Trailing annualized-vol line: ``annualized_vol`` over the last ``window`` returns.

    Keyed to the latest return's date in each trailing window. Empty until at
    least ``window`` returns exist. Reuses the service ``annualized_vol`` so the
    line and the scalar use identical math.
    """
    out: list[RollingVolPoint] = []
    if len(inst_returns) < window:
        return out
    for i in range(window - 1, len(inst_returns)):
        slice_vals = [r for _, r in inst_returns[i - window + 1 : i + 1]]
        vol = annualized_vol(slice_vals)
        if vol is not None:
            out.append(RollingVolPoint(date=inst_returns[i][0], vol=vol))
    return out


def _return_histogram(
    inst_returns: list[tuple[date, Decimal]],
    bins: int = _HISTOGRAM_BINS,
) -> list[HistogramBin]:
    """Equal-width histogram of daily returns over [min, max].

    A degenerate (constant) series collapses to a single unit-width bin around
    the value so the chart still renders. Returns empty when there are no
    returns.
    """
    vals = [r for _, r in inst_returns]
    if not vals:
        return []
    lo = min(vals)
    hi = max(vals)
    if hi <= lo:
        # Constant series — one bin centred on the value.
        return [HistogramBin(lower=lo - Decimal("0.5"), upper=lo + Decimal("0.5"), count=len(vals))]
    span = hi - lo
    width = span / Decimal(bins)
    counts = [0] * bins
    for v in vals:
        idx = int((v - lo) / width)
        if idx >= bins:
            idx = bins - 1  # the max value lands in the last bin
        if idx < 0:
            idx = 0
        counts[idx] += 1
    return [
        HistogramBin(lower=lo + width * Decimal(i), upper=lo + width * Decimal(i + 1), count=counts[i])
        for i in range(bins)
    ]


def _beta_scatter(
    inst_returns: list[tuple[date, Decimal]],
    spy_returns: list[tuple[date, Decimal]],
) -> list[BetaScatterPoint]:
    """Date-aligned (spy, inst) return pairs over the sorted date intersection.

    Mirrors :func:`ols_beta`'s alignment (intersection, never positional-zip).
    Empty when either side is empty.
    """
    inst_map = dict(inst_returns)
    spy_map = dict(spy_returns)
    shared = sorted(set(inst_map) & set(spy_map))
    return [BetaScatterPoint(spy_return=spy_map[d], inst_return=inst_map[d]) for d in shared]


@router.get("/{symbol}/risk-metrics", response_model=InstrumentRiskMetrics)
def get_instrument_risk_metrics(
    symbol: str,
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> InstrumentRiskMetrics:
    """Persisted risk metrics + on-read display series for ``symbol``.

    Reads ``instrument_risk_metrics_current`` (latest write-through, one row per
    window). The honest-status contract is preserved end-to-end: every scalar
    and its status pass through verbatim — NULLs are never coerced to zero, and
    a flagging status (insufficient_history / partial_window / benchmark_*) is
    surfaced as-is.

    404 for an unknown symbol. 200 with empty ``windows`` / null ``as_of_date``
    / null ``series`` when the instrument exists but has no persisted risk rows
    (never computed). Otherwise the display series are recomputed at request
    time from the price series cut at the persisted ``as_of_date``.
    """
    symbol_clean = symbol.strip().upper()
    if not symbol_clean:
        raise HTTPException(status_code=400, detail="symbol is required")

    # All reads — symbol→id resolution, current rows, benchmark lookup, and the
    # price series — run inside ONE snapshot so a concurrent universe /
    # primary-listing change cannot mix two committed views (Codex ckpt-2 LOW;
    # snapshot_read commits the pending txn on entry, so a pre-block lookup would
    # be a different snapshot — prevention-log §snapshot_read).
    with snapshot_read(conn):
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                """
                SELECT instrument_id, symbol FROM instruments
                WHERE UPPER(symbol) = %(s)s
                ORDER BY is_primary_listing DESC, instrument_id ASC
                LIMIT 1
                """,
                {"s": symbol_clean},
            )
            inst_row = cur.fetchone()

        if inst_row is None:
            raise HTTPException(status_code=404, detail=f"Instrument {symbol} not found")

        instrument_id = int(inst_row["instrument_id"])  # type: ignore[arg-type]
        out_symbol = str(inst_row["symbol"])  # type: ignore[arg-type]

        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                """
                SELECT
                    window_key, as_of_date, benchmark_instrument_id,
                    cagr, excess_cagr_vs_spy, max_drawdown, current_drawdown,
                    vol_annualized, beta, beta_r2, calmar,
                    skew, excess_kurtosis, var_5, worst_day, best_day,
                    trailing_1m, trailing_3m, trailing_6m, trailing_1y,
                    excess_trailing_1m, excess_trailing_3m,
                    excess_trailing_6m, excess_trailing_1y,
                    n_returns, beta_n_obs, window_days,
                    cagr_status, vol_status, beta_status, drawdown_status,
                    distribution_status, calmar_status, trailing_status,
                    excess_cagr_status,
                    sector_benchmark_instrument_id,
                    sector_beta, sector_beta_r2, sector_beta_n_obs,
                    sector_beta_status, sector_excess_cagr, sector_excess_cagr_status,
                    tr_cagr, tr_calmar, tr_status, tr_n_periods
                FROM instrument_risk_metrics_current
                WHERE instrument_id = %(iid)s
                  AND metric_version = %(ver)s
                """,
                {"iid": instrument_id, "ver": RISK_METRICS_VERSION},
            )
            risk_rows = cur.fetchall()

        if not risk_rows:
            # Never computed — honest empty payload, not a 404.
            return InstrumentRiskMetrics(
                symbol=out_symbol,
                as_of_date=None,
                benchmark_symbol=None,
                sector_benchmark_symbol=None,
                metric_version=RISK_METRICS_VERSION,
                windows=[],
                series=None,
            )

        # All windows share one as_of_date (the rebuild writes them together).
        as_of_date: date = risk_rows[0]["as_of_date"]  # type: ignore[assignment]

        # Resolve the benchmark symbol from the persisted benchmark_instrument_id.
        benchmark_symbol: str | None = None
        bench_id: int | None = None
        for r in risk_rows:
            if r["benchmark_instrument_id"] is not None:
                bench_id = int(r["benchmark_instrument_id"])  # type: ignore[arg-type]
                break
        if bench_id is not None:
            with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
                cur.execute(
                    "SELECT symbol FROM instruments WHERE instrument_id = %(id)s",
                    {"id": bench_id},
                )
                bench_row = cur.fetchone()
            if bench_row is not None:
                benchmark_symbol = str(bench_row["symbol"])  # type: ignore[arg-type]

        # Resolve the sector benchmark symbol (#1674) from the persisted
        # sector_benchmark_instrument_id (one sector per instrument — shared
        # across windows; null when the instrument has no resolvable sector).
        sector_benchmark_symbol: str | None = None
        sector_bench_id: int | None = None
        for r in risk_rows:
            if r["sector_benchmark_instrument_id"] is not None:
                sector_bench_id = int(r["sector_benchmark_instrument_id"])  # type: ignore[arg-type]
                break
        if sector_bench_id is not None:
            with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
                cur.execute(
                    "SELECT symbol FROM instruments WHERE instrument_id = %(id)s",
                    {"id": sector_bench_id},
                )
                sector_bench_row = cur.fetchone()
            if sector_bench_row is not None:
                sector_benchmark_symbol = str(sector_bench_row["symbol"])  # type: ignore[arg-type]

        # Load the price series cut at the persisted as_of_date for the series.
        inst_closes = load_close_series(conn, instrument_id, as_of_date)
        spy_closes = load_close_series(conn, bench_id, as_of_date) if bench_id is not None else []

    windows = [
        RiskWindowMetrics(
            window_key=str(r["window_key"]),
            cagr=r["cagr"],
            excess_cagr_vs_spy=r["excess_cagr_vs_spy"],
            max_drawdown=r["max_drawdown"],
            current_drawdown=r["current_drawdown"],
            vol_annualized=r["vol_annualized"],
            beta=r["beta"],
            beta_r2=r["beta_r2"],
            calmar=r["calmar"],
            skew=r["skew"],
            excess_kurtosis=r["excess_kurtosis"],
            var_5=r["var_5"],
            worst_day=r["worst_day"],
            best_day=r["best_day"],
            trailing_1m=r["trailing_1m"],
            trailing_3m=r["trailing_3m"],
            trailing_6m=r["trailing_6m"],
            trailing_1y=r["trailing_1y"],
            excess_trailing_1m=r["excess_trailing_1m"],
            excess_trailing_3m=r["excess_trailing_3m"],
            excess_trailing_6m=r["excess_trailing_6m"],
            excess_trailing_1y=r["excess_trailing_1y"],
            n_returns=r["n_returns"],
            beta_n_obs=r["beta_n_obs"],
            window_days=r["window_days"],
            cagr_status=r["cagr_status"],
            vol_status=r["vol_status"],
            beta_status=r["beta_status"],
            drawdown_status=r["drawdown_status"],
            distribution_status=r["distribution_status"],
            calmar_status=r["calmar_status"],
            trailing_status=r["trailing_status"],
            excess_cagr_status=r["excess_cagr_status"],
            sector_beta=r["sector_beta"],
            sector_beta_r2=r["sector_beta_r2"],
            sector_beta_n_obs=r["sector_beta_n_obs"],
            sector_beta_status=r["sector_beta_status"],
            sector_excess_cagr=r["sector_excess_cagr"],
            sector_excess_cagr_status=r["sector_excess_cagr_status"],
            tr_cagr=r["tr_cagr"],
            tr_calmar=r["tr_calmar"],
            tr_status=r["tr_status"],
            tr_n_periods=r["tr_n_periods"],
        )
        for r in risk_rows
    ]
    windows.sort(key=lambda w: _RISK_WINDOW_ORDER.get(w.window_key, 99))

    # On-read display series over the full series (cut at as_of_date).
    inst_returns = simple_returns(inst_closes)
    spy_returns = simple_returns(spy_closes)
    dd_points = [DrawdownPoint(date=d, drawdown=dd) for d, dd in drawdown_curve(inst_closes)]
    rolling = _rolling_vol_series(inst_returns)
    histogram = _return_histogram(inst_returns)
    scatter = _beta_scatter(inst_returns, spy_returns)
    fit = ols_beta(inst_returns, spy_returns) if spy_returns else None

    series = RiskSeries(
        drawdown_curve=dd_points,
        rolling_vol=rolling,
        return_histogram=histogram,
        beta_scatter=scatter,
        beta=fit.beta if fit is not None else None,
        beta_r2=fit.r2 if fit is not None else None,
    )

    return InstrumentRiskMetrics(
        symbol=out_symbol,
        as_of_date=as_of_date,
        benchmark_symbol=benchmark_symbol,
        sector_benchmark_symbol=sector_benchmark_symbol,
        metric_version=RISK_METRICS_VERSION,
        windows=windows,
        series=series,
    )


@router.get("/{symbol}/portfolio-risk", response_model=PortfolioRelativeRiskResponse)
def get_instrument_portfolio_risk(
    symbol: str,
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> PortfolioRelativeRiskResponse:
    """Candidate-vs-current-book risk (#1636) — marginal risk contribution.

    How much the candidate moves the operator's existing book's risk: portfolio
    beta / correlation / marginal-risk-contribution vs the current holdings.
    ON-READ (the book is dynamic) — a current-exposure covariance estimate, not
    realized book history. 404 for an unknown symbol; 200 with
    ``status="empty_book"`` when the operator holds nothing.
    """
    symbol_clean = symbol.strip().upper()
    if not symbol_clean:
        raise HTTPException(status_code=400, detail="symbol is required")

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT instrument_id, symbol FROM instruments
            WHERE UPPER(symbol) = %(s)s
            ORDER BY is_primary_listing DESC, instrument_id ASC
            LIMIT 1
            """,
            {"s": symbol_clean},
        )
        row = cur.fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail=f"Instrument {symbol} not found")

    result = compute_portfolio_relative_risk(
        conn,
        int(row["instrument_id"]),  # type: ignore[arg-type]
        str(row["symbol"]),  # type: ignore[arg-type]
        date.today(),
    )
    return PortfolioRelativeRiskResponse(
        symbol=result.symbol,
        as_of_date=result.as_of_date,
        status=result.status,
        holdings_count=result.holdings_count,
        already_held=result.already_held,
        current_weight=result.current_weight,
        portfolio_beta=result.portfolio_beta,
        correlation=result.correlation,
        candidate_vol=result.candidate_vol,
        portfolio_vol=result.portfolio_vol,
        marginal_risk_contribution=result.marginal_risk_contribution,
        n_obs=result.n_obs,
    )
