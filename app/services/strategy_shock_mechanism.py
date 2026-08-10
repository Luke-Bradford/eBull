"""Point-in-time mechanism routing for short-horizon price-shock research.

This module is deliberately outcome-blind.  It separates a known material SEC
event, a move explained by prior-fitted market/sector context, and a fully
observable no-known-catalyst residual shock before any continuation/reversal
candidate sees the row.  Missing event coverage is an ``unknown`` refusal, not
evidence that no catalyst existed.

Refs #2507.  No result from a backtest enters this definition.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from typing import Any, Final, Literal
from zoneinfo import ZoneInfo

import psycopg

from app.services.market_calendar import us_market_status

Mechanism = Literal[
    "known_fundamental_catalyst",
    "known_market_or_sector_move",
    "no_known_catalyst_liquidity_candidate",
    "unknown",
]
CoverageStatus = Literal["complete", "known_incomplete"]

RESIDUAL_SHOCK_Z: Final = 2.0
MAX_HALT_FEED_AGE_SECONDS: Final = 300
MAX_HALT_FEED_AGE: Final = timedelta(seconds=MAX_HALT_FEED_AGE_SECONDS)
REQUIRED_EVENT_SOURCES: Final = ("sec_filings", "issuer_releases", "market_news")
_NY = ZoneInfo("America/New_York")
_MARKET_OPEN = time(9, 30)


@dataclass(frozen=True)
class MechanismDefinition:
    version: int = 1
    outputs: tuple[str, ...] = (
        "known_fundamental_catalyst",
        "known_market_or_sector_move",
        "no_known_catalyst_liquidity_candidate",
        "unknown",
    )
    precedence: tuple[str, ...] = (
        "known_material_catalyst",
        "prior_fitted_market_sector_explanation",
        "complete_event_coverage",
        "complete_liquidity_and_halt_context",
    )
    residual_shock_z: float = RESIDUAL_SHOCK_Z
    max_halt_feed_age_seconds: int = MAX_HALT_FEED_AGE_SECONDS
    required_event_sources: tuple[str, ...] = REQUIRED_EVENT_SOURCES
    missing_sec_acceptance_policy: str = "next-regular-session-open-after-filed-date"
    no_catalyst_semantics: str = "all-required-sources-complete-over-decision-window"
    direction_semantics: str = "none-provenance-only"


DEFINITION: Final = MechanismDefinition()


def definition_json() -> str:
    return json.dumps(asdict(DEFINITION), sort_keys=True, separators=(",", ":"))


def definition_hash() -> str:
    return hashlib.sha256(definition_json().encode()).hexdigest()


CLASSIFIER_VERSION: Final = f"shock-mechanism-v1+{definition_hash()[:12]}"


@dataclass(frozen=True)
class SecCatalyst:
    accession_number: str
    form: str
    knowledge_at: datetime
    item_codes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.accession_number or not self.form:
            raise ValueError("SEC catalyst identity is incomplete")
        _require_aware("knowledge_at", self.knowledge_at)


@dataclass(frozen=True)
class EventSourceCoverage:
    source: str
    status: CoverageStatus
    covered_from: datetime | None
    covered_through: datetime | None

    def __post_init__(self) -> None:
        if not self.source:
            raise ValueError("event coverage source is empty")
        if self.covered_from is not None:
            _require_aware("covered_from", self.covered_from)
        if self.covered_through is not None:
            _require_aware("covered_through", self.covered_through)
        if (
            self.covered_from is not None
            and self.covered_through is not None
            and self.covered_from > self.covered_through
        ):
            raise ValueError("event coverage begins after it ends")

    def covers(self, start: datetime, end: datetime) -> bool:
        return (
            self.status == "complete"
            and self.covered_from is not None
            and self.covered_through is not None
            and self.covered_from <= start
            and self.covered_through >= end
        )


@dataclass(frozen=True)
class FactorContext:
    """One decision-time return decomposition fitted only on prior bars."""

    raw_return: float
    expected_market_sector_return: float
    residual_return: float
    prior_residual_volatility: float
    fitted_through: datetime
    model_version: str
    market_series_id: str
    sector_series_id: str

    def __post_init__(self) -> None:
        values = (
            self.raw_return,
            self.expected_market_sector_return,
            self.residual_return,
            self.prior_residual_volatility,
        )
        if not all(math.isfinite(value) for value in values):
            raise ValueError("factor context contains a non-finite value")
        if self.prior_residual_volatility <= 0:
            raise ValueError("prior residual volatility must be positive")
        _require_aware("fitted_through", self.fitted_through)
        if not self.model_version or not self.market_series_id or not self.sector_series_id:
            raise ValueError("factor context provenance is incomplete")
        reconstructed = self.raw_return - self.expected_market_sector_return
        if not math.isclose(reconstructed, self.residual_return, rel_tol=1e-10, abs_tol=1e-12):
            raise ValueError("factor context residual is inconsistent with raw minus expected return")

    @property
    def residual_z(self) -> float:
        return self.residual_return / self.prior_residual_volatility


@dataclass(frozen=True)
class LiquidityContext:
    as_traded_price: Decimal | None
    trailing_median_dollar_volume: Decimal | None
    relative_volume: Decimal | None
    realised_volatility: Decimal | None
    spread_bps: Decimal | None
    confirmation_completed_at: datetime | None
    halt_feed_at: datetime | None
    active_halt: bool | None

    def missing_at(self, decision_at: datetime) -> tuple[str, ...]:
        missing: list[str] = []
        positive = {
            "as_traded_price": self.as_traded_price,
            "trailing_median_dollar_volume": self.trailing_median_dollar_volume,
            "relative_volume": self.relative_volume,
            "realised_volatility": self.realised_volatility,
        }
        for name, value in positive.items():
            if value is None or not value.is_finite() or value <= 0:
                missing.append(name)
        if self.spread_bps is None or not self.spread_bps.is_finite() or self.spread_bps < 0:
            missing.append("spread_bps")
        if self.confirmation_completed_at is None:
            missing.append("completed_intraday_confirmation")
        elif self.confirmation_completed_at.tzinfo is None or self.confirmation_completed_at > decision_at:
            missing.append("causal_intraday_confirmation")
        if self.halt_feed_at is None:
            missing.append("halt_feed")
        elif self.halt_feed_at.tzinfo is None or not (
            decision_at - MAX_HALT_FEED_AGE <= self.halt_feed_at <= decision_at
        ):
            missing.append("fresh_halt_feed")
        if self.active_halt is None:
            missing.append("active_halt_state")
        return tuple(missing)


@dataclass(frozen=True)
class MechanismDecision:
    mechanism: Mechanism
    reason_code: str
    classifier_version: str
    residual_z: float | None
    catalyst_accessions: tuple[str, ...]
    missing_inputs: tuple[str, ...]


def _require_aware(name: str, value: datetime) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")


def _coverage_missing(coverage: Sequence[EventSourceCoverage], *, start: datetime, end: datetime) -> tuple[str, ...]:
    by_source: Mapping[str, EventSourceCoverage] = {item.source: item for item in coverage}
    duplicates = tuple(
        sorted({item.source for item in coverage if sum(row.source == item.source for row in coverage) > 1})
    )
    missing = tuple(
        source
        for source in REQUIRED_EVENT_SOURCES
        if source not in by_source or not by_source[source].covers(start, end)
    )
    return tuple(f"duplicate_event_coverage:{source}" for source in duplicates) + missing


def classify_shock_mechanism(
    *,
    decision_at: datetime,
    event_window_start: datetime,
    catalysts: Sequence[SecCatalyst],
    event_coverage: Sequence[EventSourceCoverage],
    factor_context: FactorContext | None,
    liquidity_context: LiquidityContext | None,
) -> MechanismDecision:
    """Route one observation without reading or predicting an outcome."""
    _require_aware("decision_at", decision_at)
    _require_aware("event_window_start", event_window_start)
    if event_window_start > decision_at:
        raise ValueError("event window begins after the decision")

    known = tuple(
        sorted(
            (catalyst for catalyst in catalysts if event_window_start <= catalyst.knowledge_at <= decision_at),
            key=lambda item: (item.knowledge_at, item.accession_number),
        )
    )
    if known:
        return MechanismDecision(
            "known_fundamental_catalyst",
            "material_sec_event_known_before_decision",
            CLASSIFIER_VERSION,
            (
                factor_context.residual_z
                if factor_context is not None and factor_context.fitted_through < decision_at
                else None
            ),
            tuple(item.accession_number for item in known),
            (),
        )

    factor_is_causal = factor_context is not None and factor_context.fitted_through < decision_at
    if factor_is_causal and factor_context is not None and abs(factor_context.residual_z) < RESIDUAL_SHOCK_Z:
        return MechanismDecision(
            "known_market_or_sector_move",
            "prior_fitted_market_sector_context_explains_move",
            CLASSIFIER_VERSION,
            factor_context.residual_z,
            (),
            (),
        )

    coverage_missing = _coverage_missing(event_coverage, start=event_window_start, end=decision_at)
    if coverage_missing:
        return MechanismDecision(
            "unknown",
            "event_coverage_incomplete",
            CLASSIFIER_VERSION,
            factor_context.residual_z if factor_is_causal and factor_context is not None else None,
            (),
            coverage_missing,
        )
    if not factor_is_causal or factor_context is None:
        return MechanismDecision(
            "unknown",
            "factor_context_incomplete" if factor_context is None else "factor_context_not_causal",
            CLASSIFIER_VERSION,
            None,
            (),
            ("prior_fitted_market_sector_context",),
        )
    if liquidity_context is None:
        return MechanismDecision(
            "unknown",
            "liquidity_context_incomplete",
            CLASSIFIER_VERSION,
            factor_context.residual_z,
            (),
            ("liquidity_context",),
        )
    liquidity_missing = liquidity_context.missing_at(decision_at)
    if liquidity_missing:
        return MechanismDecision(
            "unknown",
            "liquidity_context_incomplete",
            CLASSIFIER_VERSION,
            factor_context.residual_z,
            (),
            liquidity_missing,
        )
    if liquidity_context.active_halt:
        return MechanismDecision(
            "unknown",
            "active_market_halt",
            CLASSIFIER_VERSION,
            factor_context.residual_z,
            (),
            (),
        )
    return MechanismDecision(
        "no_known_catalyst_liquidity_candidate",
        "residual_shock_with_complete_no_known_catalyst_context",
        CLASSIFIER_VERSION,
        factor_context.residual_z,
        (),
        (),
    )


def _next_regular_session_open(after_date: date) -> datetime:
    candidate = after_date + timedelta(days=1)
    while us_market_status(candidate) == "closed":
        candidate += timedelta(days=1)
    return datetime.combine(candidate, _MARKET_OPEN, tzinfo=_NY).astimezone(UTC)


def sec_knowledge_at(*, filed_at: datetime, accepted_at: datetime | None) -> datetime:
    """Exact acceptance when known; otherwise the following session open."""
    _require_aware("filed_at", filed_at)
    if accepted_at is not None:
        _require_aware("accepted_at", accepted_at)
        return accepted_at.astimezone(UTC)
    # Date-only SEC discovery is stored as a UTC-midnight timestamptz. Mapping
    # that instant to New York would incorrectly move it onto the prior civil
    # day; its UTC date is the provider's filed-date identity.
    return _next_regular_session_open(filed_at.astimezone(UTC).date())


_MATERIAL_SEC_SQL = """
    SELECT m.accession_number, m.form, m.filed_at, m.accepted_at,
           COALESCE(
             array_agg(DISTINCT i.item_code ORDER BY i.item_code)
               FILTER (WHERE c.severity IN ('material', 'critical')),
             ARRAY[]::text[]
           ) AS material_items
    FROM sec_filing_manifest m
    LEFT JOIN eight_k_items i ON i.accession_number = m.accession_number
    LEFT JOIN sec_8k_item_codes c ON c.code = i.item_code
    WHERE m.instrument_id = %(instrument_id)s
      AND m.form IN ('10-K', '10-K/A', '10-Q', '10-Q/A', '8-K', '8-K/A')
      AND m.filed_at >= %(coarse_start)s
      AND m.filed_at <= %(decision_at)s
    GROUP BY m.accession_number, m.form, m.filed_at, m.accepted_at
    ORDER BY m.filed_at, m.accession_number
"""


def load_material_sec_catalysts(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
    event_window_start: datetime,
    decision_at: datetime,
) -> tuple[SecCatalyst, ...]:
    """Load only material issuer events causally known in the window.

    The coarse SQL floor reaches back seven calendar days because a date-only
    filing can become known on a later market session. Exact inclusion is made
    from ``accepted_at`` or the conservative next-session rule in Python.
    """
    _require_aware("event_window_start", event_window_start)
    _require_aware("decision_at", decision_at)
    rows = conn.execute(
        _MATERIAL_SEC_SQL,
        {
            "instrument_id": instrument_id,
            "coarse_start": event_window_start - timedelta(days=7),
            "decision_at": decision_at,
        },
    ).fetchall()
    catalysts: list[SecCatalyst] = []
    for accession, form, filed_at, accepted_at, item_codes in rows:
        codes = tuple(str(item) for item in item_codes)
        if str(form).startswith("8-K") and not codes:
            continue
        knowledge_at = sec_knowledge_at(filed_at=filed_at, accepted_at=accepted_at)
        if event_window_start <= knowledge_at <= decision_at:
            catalysts.append(SecCatalyst(str(accession), str(form), knowledge_at, codes))
    catalysts.sort(key=lambda item: (item.knowledge_at, item.accession_number))
    return tuple(catalysts)


__all__ = [
    "CLASSIFIER_VERSION",
    "DEFINITION",
    "EventSourceCoverage",
    "FactorContext",
    "LiquidityContext",
    "MechanismDecision",
    "SecCatalyst",
    "classify_shock_mechanism",
    "definition_hash",
    "definition_json",
    "load_material_sec_catalysts",
    "sec_knowledge_at",
]
