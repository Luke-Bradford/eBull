"""Frozen published ORB / Stocks-in-Play candidate contract (#2485).

This module deliberately contains no evaluator and no catalogue entry.  It
exists to prevent a small-panel breakout from being mistaken for the published
cross-sectional strategy while its required evidence is still unavailable.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from decimal import Decimal
from typing import Final, Literal


@dataclass(frozen=True)
class OrbStocksInPlayDefinition:
    paper_revision: str = "2025-04-29"
    security_type: str = "common_stock"
    security_type_boundary: str = "ebull_adaptation_pending_paper_crsp_share_code_confirmation"
    listing_markets: tuple[str, ...] = ("nyse", "nasdaq")
    opening_range_minutes: int = 5
    minimum_open_price_usd: Decimal = Decimal("5")
    minimum_prior_mean_share_volume: Decimal = Decimal("1000000")
    minimum_prior_atr_usd: Decimal = Decimal("0.50")
    lookback_sessions: int = 14
    minimum_opening_relative_volume: Decimal = Decimal("1")
    maximum_daily_rank: int = 20
    direction: str = "opening_candle_direction"
    entry: str = "stop_at_opening_range_high_or_low"
    stop_atr_fraction: Decimal = Decimal("0.10")
    exit: str = "stop_or_regular_session_close"
    profit_target: None = None
    gap_filter: None = None
    paper_risk_fraction: Decimal = Decimal("0.01")
    ebull_maximum_leverage: Decimal = Decimal("1")


DEFINITION: Final = OrbStocksInPlayDefinition()
CANDIDATE_VERSION: Final = "orb-stocks-in-play-v1:" + hashlib.sha256(repr(DEFINITION).encode()).hexdigest()[:16]


@dataclass(frozen=True)
class ReplicationReadiness:
    """Coverage known before any retained outcome is opened."""

    point_in_time_membership: bool
    security_type_mapping_verified: bool
    atr_implementation_verified: bool
    expected_prefilter_names: int
    scanned_prefilter_names: int
    opening_volume_names: int
    expected_selected_names: int
    selected_names: int
    selected_paths: int
    as_traded_price_basis_complete: bool
    decision_quotes_complete: bool
    shortability_complete: bool


ReadinessVerdict = Literal["ready", "refused"]


def assess_replication_readiness(value: ReplicationReadiness) -> tuple[ReadinessVerdict, tuple[str, ...]]:
    """Refuse partial-universe and non-executable approximations by name."""

    for field in ("expected_prefilter_names", "scanned_prefilter_names", "opening_volume_names"):
        if getattr(value, field) <= 0:
            raise ValueError(f"{field} must be positive")
    for field in ("expected_selected_names", "selected_names", "selected_paths"):
        if not 0 <= getattr(value, field) <= DEFINITION.maximum_daily_rank:
            raise ValueError(f"{field} must be inside 0-{DEFINITION.maximum_daily_rank}")
    if value.expected_selected_names > value.expected_prefilter_names:
        raise ValueError("expected_selected_names cannot exceed expected_prefilter_names")

    reasons: list[str] = []
    if not value.point_in_time_membership:
        reasons.append("missing_point_in_time_membership")
    if not value.security_type_mapping_verified:
        reasons.append("ambiguous_published_security_type_universe")
    if not value.atr_implementation_verified:
        reasons.append("ambiguous_atr_implementation")
    if value.scanned_prefilter_names != value.expected_prefilter_names:
        reasons.append("incomplete_prefilter_cross_section")
    if value.opening_volume_names != value.expected_prefilter_names:
        reasons.append("incomplete_opening_volume_cross_section")
    if value.selected_names != value.expected_selected_names:
        reasons.append("incomplete_top20_selection")
    if value.selected_paths != value.selected_names:
        reasons.append("incomplete_selected_intraday_paths")
    if not value.as_traded_price_basis_complete:
        reasons.append("missing_as_traded_price_basis")
    if not value.decision_quotes_complete:
        reasons.append("missing_decision_time_etoro_quotes")
    if not value.shortability_complete:
        reasons.append("missing_decision_time_shortability")

    return ("ready" if not reasons else "refused", tuple(reasons))
