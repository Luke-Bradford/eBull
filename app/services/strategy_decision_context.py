"""Versioned, compact point-in-time context for strategy candidates (#2508).

Rolling series remain in their bounded source stores. This module snapshots
only the values that existed at one fired/refused decision so later cohort
analysis cannot silently resolve security type, listing or liquidity from
today's metadata.
"""

from __future__ import annotations

import hashlib
import inspect
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any, Final, Literal
from zoneinfo import ZoneInfo

import psycopg
import psycopg.rows

from app.services.cboe_vix import SOURCE_VERSION as CBOE_VIX_SOURCE_VERSION
from app.services.cboe_vix import load_vix_close_as_known
from app.services.market_calendar import us_market_status

SecurityType = Literal["common_stock", "etf", "other", "unknown"]
ListingMarket = Literal["nyse", "nasdaq", "other", "unknown"]
CandidateVerdict = Literal["eligible", "refused"]
PriceBand = Literal["under_5", "5_to_20", "20_to_50", "50_to_150", "150_plus"]
DollarVolumeBand = Literal["under_1m", "1m_to_10m", "10m_to_25m", "25m_to_100m", "100m_plus"]
AsTradedPriceBasis = Literal["observed_unadjusted", "reconstructed_unadjusted", "unknown"]
AS_TRADED_PRICE_BASES: Final = frozenset({"observed_unadjusted", "reconstructed_unadjusted", "unknown"})


@dataclass(frozen=True)
class ContextDefinition:
    price_edges: tuple[str, ...] = ("5", "20", "50", "150")
    dollar_volume_edges: tuple[str, ...] = ("1000000", "10000000", "25000000", "100000000")
    trailing_volume_statistic: str = "causal_median"
    volume_lookback_sessions: int = 20
    volume_capacity_statistic: str = "causal_mean"
    listing_semantics: str = "primary_listing_not_execution_venue"
    sector_semantics: str = "provider_stocks_industry_not_gics"
    sector_source: str = "prospective_instrument_market_classification_history"
    eligible_price_bases: tuple[str, ...] = ("observed_unadjusted", "reconstructed_unadjusted")
    vix_source_version: str = CBOE_VIX_SOURCE_VERSION
    vix_availability: str = "prior_new_york_session_only"


DEFINITION: Final = ContextDefinition()


def _version() -> str:
    payload = repr(DEFINITION) + inspect.getsource(price_band_for) + inspect.getsource(dollar_volume_band_for)
    return "decision-context-v3:" + hashlib.sha256(payload.encode()).hexdigest()[:16]


def price_band_for(price: Decimal) -> PriceBand:
    if price <= 0:
        raise ValueError("as-traded price must be positive")
    if price < 5:
        return "under_5"
    if price < 20:
        return "5_to_20"
    if price < 50:
        return "20_to_50"
    if price < 150:
        return "50_to_150"
    return "150_plus"


def dollar_volume_band_for(value: Decimal) -> DollarVolumeBand:
    if value < 0:
        raise ValueError("trailing median dollar volume must be non-negative")
    if value < 1_000_000:
        return "under_1m"
    if value < 10_000_000:
        return "1m_to_10m"
    if value < 25_000_000:
        return "10m_to_25m"
    if value < 100_000_000:
        return "25m_to_100m"
    return "100m_plus"


CONTEXT_VERSION: Final = _version()
_NEW_YORK: Final = ZoneInfo("America/New_York")


@dataclass(frozen=True)
class MarketClassification:
    effective_from: date
    security_type: SecurityType
    primary_listing_market: ListingMarket
    provider_exchange_id: str | None
    instrument_type_id: int | None
    provider_industry_id: int | None


@dataclass(frozen=True)
class DecisionVix:
    close: Decimal | None
    bar_date: date | None
    source_version: str | None
    refusal_reason: str | None = None

    def __post_init__(self) -> None:
        complete = self.close is not None and self.bar_date is not None and bool(self.source_version)
        if complete == (self.refusal_reason is not None):
            raise ValueError("VIX context must be either complete or carry one refusal reason")
        if self.close is not None and (not self.close.is_finite() or self.close <= 0):
            raise ValueError("VIX close must be finite and positive")


@dataclass(frozen=True)
class DecisionInputs:
    as_traded_price: Decimal | None
    as_traded_price_basis: AsTradedPriceBasis | None
    trailing_mean_share_volume: Decimal | None
    trailing_median_share_volume: Decimal | None
    trailing_mean_dollar_volume: Decimal | None
    trailing_median_dollar_volume: Decimal | None
    zero_volume_frequency: Decimal | None
    intraday_coverage: Decimal | None
    relative_volume: Decimal | None
    spread_bps: Decimal | None
    realised_volatility: Decimal | None
    gap_pct: Decimal | None
    market_sector_residual_z: Decimal | None
    vix: DecisionVix | None


@dataclass(frozen=True)
class DecisionContext:
    strategy_id: str
    strategy_version: str
    instrument_id: int
    decision_at: datetime
    signal_id: int | None
    candidate_verdict: CandidateVerdict
    refusal_reason: str | None
    context_version: str
    classification_effective_from: date | None
    security_type: SecurityType | None
    primary_listing_market: ListingMarket | None
    provider_exchange_id: str | None
    instrument_type_id: int | None
    provider_industry_id: int | None
    as_traded_price: Decimal | None
    as_traded_price_basis: AsTradedPriceBasis | None
    price_band: PriceBand | None
    volume_lookback_sessions: int
    trailing_mean_share_volume: Decimal | None
    trailing_median_share_volume: Decimal | None
    trailing_mean_dollar_volume: Decimal | None
    trailing_median_dollar_volume: Decimal | None
    dollar_volume_band: DollarVolumeBand | None
    zero_volume_frequency: Decimal | None
    intraday_coverage: Decimal | None
    relative_volume: Decimal | None
    spread_bps: Decimal | None
    realised_volatility: Decimal | None
    gap_pct: Decimal | None
    market_sector_residual_z: Decimal | None
    vix: Decimal | None
    vix_bar_date: date | None
    vix_source_version: str | None


_REQUIRED_INPUTS: Final = (
    "as_traded_price",
    "as_traded_price_basis",
    "trailing_mean_share_volume",
    "trailing_median_share_volume",
    "trailing_mean_dollar_volume",
    "trailing_median_dollar_volume",
    "zero_volume_frequency",
    "intraday_coverage",
    "relative_volume",
    "spread_bps",
    "realised_volatility",
    "gap_pct",
    "market_sector_residual_z",
)


def _prior_us_session(decision_at: datetime) -> date:
    candidate = decision_at.astimezone(_NEW_YORK).date() - timedelta(days=1)
    while us_market_status(candidate) == "closed":
        candidate -= timedelta(days=1)
    return candidate


def load_decision_vix(conn: psycopg.Connection[Any], *, decision_at: datetime) -> DecisionVix:
    """Resolve the exact prior-session Cboe close or a typed refusal."""
    if decision_at.tzinfo is None or decision_at.utcoffset() is None:
        raise ValueError("decision_at must be timezone-aware")
    expected = _prior_us_session(decision_at)
    bar = load_vix_close_as_known(conn, decision_at=decision_at)
    if bar is None:
        return DecisionVix(None, None, None, "missing_source")
    if bar.bar_date != expected:
        return DecisionVix(None, None, None, f"stale_source:{bar.bar_date.isoformat()}<expected:{expected.isoformat()}")
    return DecisionVix(bar.close, bar.bar_date, CBOE_VIX_SOURCE_VERSION)


def load_market_classification(
    conn: psycopg.Connection[Any], *, instrument_id: int, decision_at: datetime
) -> MarketClassification | None:
    """Resolve only a classification observed on the decision date.

    Imported current metadata does not apply before its effective_from; a
    historical backtest therefore refuses instead of leaking today's venue or
    type into old trades.
    """
    if decision_at.tzinfo is None:
        raise ValueError("decision_at must be timezone-aware")
    with conn.cursor(row_factory=psycopg.rows.tuple_row) as cur:
        row = cur.execute(
            """
            SELECT effective_from, security_type, primary_listing_market,
                   provider_exchange_id, instrument_type_id,
                   provider_industry_id
              FROM instrument_market_classification_history
             WHERE instrument_id = %(instrument_id)s
               AND daterange(effective_from, effective_to, '[]') @> %(decision_date)s::date
            """,
            {
                "instrument_id": instrument_id,
                "decision_date": decision_at.astimezone(_NEW_YORK).date(),
            },
        ).fetchone()
    if row is None:
        return None
    return MarketClassification(
        effective_from=row[0],
        security_type=row[1],
        primary_listing_market=row[2],
        provider_exchange_id=row[3],
        instrument_type_id=row[4],
        provider_industry_id=row[5],
    )


def build_decision_context(
    *,
    strategy_id: str,
    strategy_version: str,
    instrument_id: int,
    decision_at: datetime,
    signal_id: int | None,
    classification: MarketClassification | None,
    inputs: DecisionInputs,
) -> DecisionContext:
    if not strategy_id or not strategy_version:
        raise ValueError("strategy identity must be non-empty")
    if instrument_id <= 0:
        raise ValueError("instrument_id must be positive")
    if decision_at.tzinfo is None:
        raise ValueError("decision_at must be timezone-aware")
    if inputs.as_traded_price_basis is not None and inputs.as_traded_price_basis not in AS_TRADED_PRICE_BASES:
        raise ValueError(f"unknown as_traded_price_basis {inputs.as_traded_price_basis!r}")
    for name in (
        "trailing_mean_share_volume",
        "trailing_median_share_volume",
        "trailing_mean_dollar_volume",
        "trailing_median_dollar_volume",
        "relative_volume",
        "spread_bps",
        "realised_volatility",
    ):
        value = getattr(inputs, name)
        if value is not None and value < 0:
            raise ValueError(f"{name} must be non-negative")
    for name in ("zero_volume_frequency", "intraday_coverage"):
        value = getattr(inputs, name)
        if value is not None and not 0 <= value <= 1:
            raise ValueError(f"{name} must be inside 0-1")

    missing: list[str] = [name for name in _REQUIRED_INPUTS if getattr(inputs, name) is None]
    if inputs.vix is None:
        missing.append("vix")
    elif inputs.vix.refusal_reason is not None:
        missing.append(f"vix_{inputs.vix.refusal_reason}")
    if inputs.as_traded_price_basis == "unknown":
        missing.append("as_traded_price_basis")
    if classification is None:
        missing.append("point_in_time_classification")
    else:
        if classification.security_type == "unknown":
            missing.append("security_type")
        if classification.primary_listing_market == "unknown":
            missing.append("primary_listing_market")
        if classification.provider_industry_id is None:
            missing.append("provider_industry_id")

    price_band = None if inputs.as_traded_price is None else price_band_for(inputs.as_traded_price)
    dollar_band = (
        None
        if inputs.trailing_median_dollar_volume is None
        else dollar_volume_band_for(inputs.trailing_median_dollar_volume)
    )
    verdict: CandidateVerdict = "refused" if missing else "eligible"
    refusal = None if not missing else "missing:" + ",".join(sorted(missing))

    return DecisionContext(
        strategy_id=strategy_id,
        strategy_version=strategy_version,
        instrument_id=instrument_id,
        decision_at=decision_at,
        signal_id=signal_id,
        candidate_verdict=verdict,
        refusal_reason=refusal,
        context_version=CONTEXT_VERSION,
        classification_effective_from=None if classification is None else classification.effective_from,
        security_type=None if classification is None else classification.security_type,
        primary_listing_market=None if classification is None else classification.primary_listing_market,
        provider_exchange_id=None if classification is None else classification.provider_exchange_id,
        instrument_type_id=None if classification is None else classification.instrument_type_id,
        provider_industry_id=None if classification is None else classification.provider_industry_id,
        as_traded_price=inputs.as_traded_price,
        as_traded_price_basis=inputs.as_traded_price_basis,
        price_band=price_band,
        volume_lookback_sessions=DEFINITION.volume_lookback_sessions,
        trailing_mean_share_volume=inputs.trailing_mean_share_volume,
        trailing_median_share_volume=inputs.trailing_median_share_volume,
        trailing_mean_dollar_volume=inputs.trailing_mean_dollar_volume,
        trailing_median_dollar_volume=inputs.trailing_median_dollar_volume,
        dollar_volume_band=dollar_band,
        zero_volume_frequency=inputs.zero_volume_frequency,
        intraday_coverage=inputs.intraday_coverage,
        relative_volume=inputs.relative_volume,
        spread_bps=inputs.spread_bps,
        realised_volatility=inputs.realised_volatility,
        gap_pct=inputs.gap_pct,
        market_sector_residual_z=inputs.market_sector_residual_z,
        vix=None if inputs.vix is None else inputs.vix.close,
        vix_bar_date=None if inputs.vix is None else inputs.vix.bar_date,
        vix_source_version=None if inputs.vix is None else inputs.vix.source_version,
    )


_INSERT_CONTEXT = """
    INSERT INTO strategy_decision_contexts (
        strategy_id, strategy_version, instrument_id, decision_at, signal_id,
        candidate_verdict, refusal_reason, context_version,
        classification_effective_from, security_type, primary_listing_market,
        provider_exchange_id, instrument_type_id, provider_industry_id,
        as_traded_price,
        as_traded_price_basis, price_band,
        volume_lookback_sessions, trailing_mean_share_volume,
        trailing_median_share_volume, trailing_mean_dollar_volume,
        trailing_median_dollar_volume, dollar_volume_band,
        zero_volume_frequency, intraday_coverage,
        relative_volume, spread_bps, realised_volatility,
        gap_pct, market_sector_residual_z, vix, vix_bar_date,
        vix_source_version
    ) VALUES (
        %(strategy_id)s, %(strategy_version)s, %(instrument_id)s, %(decision_at)s, %(signal_id)s,
        %(candidate_verdict)s, %(refusal_reason)s, %(context_version)s,
        %(classification_effective_from)s, %(security_type)s, %(primary_listing_market)s,
        %(provider_exchange_id)s, %(instrument_type_id)s, %(provider_industry_id)s,
        %(as_traded_price)s,
        %(as_traded_price_basis)s, %(price_band)s,
        %(volume_lookback_sessions)s, %(trailing_mean_share_volume)s,
        %(trailing_median_share_volume)s, %(trailing_mean_dollar_volume)s,
        %(trailing_median_dollar_volume)s, %(dollar_volume_band)s,
        %(zero_volume_frequency)s, %(intraday_coverage)s,
        %(relative_volume)s, %(spread_bps)s, %(realised_volatility)s,
        %(gap_pct)s, %(market_sector_residual_z)s, %(vix)s, %(vix_bar_date)s,
        %(vix_source_version)s
    )
    RETURNING context_id
"""


def store_decision_context(conn: psycopg.Connection[Any], context: DecisionContext) -> int:
    """Insert immutably; a duplicate decision is a loud research defect."""
    row = conn.execute(_INSERT_CONTEXT, asdict(context)).fetchone()
    if row is None:  # pragma: no cover - INSERT RETURNING always returns
        raise RuntimeError("decision context insert returned no identity")
    return int(row[0])


__all__ = [
    "AS_TRADED_PRICE_BASES",
    "CONTEXT_VERSION",
    "DecisionContext",
    "DecisionInputs",
    "DecisionVix",
    "MarketClassification",
    "build_decision_context",
    "dollar_volume_band_for",
    "load_market_classification",
    "load_decision_vix",
    "price_band_for",
    "store_decision_context",
]
