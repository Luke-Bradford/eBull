"""
Scoring and ranking engine.

Responsibilities:
  - Compute six family scores (quality, value, turnaround, momentum, sentiment,
    confidence) from raw DB signals using deterministic, clipped 0-1 formulas.
  - Apply additive penalties (stale thesis, missing data, wide spread, etc.).
  - Compute a weighted total score per the active weight mode.
  - Assign rank and rank_delta within a scoring run.
  - Persist each result as an immutable row in the scores table (never overwrite).

Score families and weight modes are defined in docs/scoring-model.md.
All formula constants are explicit in this file — no hidden logic.

Model version convention:  "<version>-<mode>"  e.g. "v1-balanced"

Versioning contract:
  score rows are append-only. A scoring run produces one row per instrument.
  Prior rows are never mutated. rank_delta is computed by comparing against
  the most recent prior run with the same model_version.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

import psycopg
import psycopg.rows
from psycopg.types.json import Jsonb

from app.services.instrument_analytics import (
    assemble_instrument_analytics,
    compute_peer_grades,
)
from app.services.sector_classification import resolve_sector_spdr
from app.services.xbrl_derived_stats import (
    MarketCapResolution,
    resolve_market_cap_basis,
)

logger = logging.getLogger(__name__)

_DEFAULT_MODEL_VERSION = "v1.3-balanced"

# Model-version prefix gates (single source — the next version is a one-line add,
# not a scattered string edit). TA-enhanced momentum applies from v1.1; the
# realized-risk penalty from v1.2; both carry forward to v1.3 (which only ADDS the
# Calmar reward on top — Codex ckpt-1 HIGH: v1.3 must inherit v1.2 behavior).
_TA_MOMENTUM_PREFIXES: tuple[str, ...] = ("v1.1", "v1.2", "v1.3")
_RISK_PENALTY_PREFIXES: tuple[str, ...] = ("v1.2", "v1.3")
_CALMAR_REWARD_PREFIXES: tuple[str, ...] = ("v1.3",)

# ---------------------------------------------------------------------------
# Weight modes  (must sum to 1.0)
#
# v1   — return-only momentum (3 return windows, no TA)
# v1.1 — TA-enhanced momentum (returns + trend/quality/volatility subcomponents)
#         Same family weights as v1; the difference is inside _momentum_score.
# v1.2 — v1.1 momentum/families + an additive realized-risk penalty (#1633).
#         Same family weights as v1.1; the difference is the penalty block.
# ---------------------------------------------------------------------------

_WEIGHT_MODES: dict[str, dict[str, float]] = {
    "v1-balanced": {
        "quality": 0.25,
        "value": 0.25,
        "turnaround": 0.20,
        "confidence": 0.15,
        "momentum": 0.10,
        "sentiment": 0.05,
    },
    "v1-conservative": {
        "quality": 0.35,
        "value": 0.25,
        "confidence": 0.20,
        "momentum": 0.10,
        "sentiment": 0.05,
        "turnaround": 0.05,
    },
    "v1-speculative": {
        "turnaround": 0.30,
        "value": 0.25,
        "momentum": 0.15,
        "confidence": 0.15,
        "sentiment": 0.10,
        "quality": 0.05,
    },
    "v1.1-balanced": {
        "quality": 0.25,
        "value": 0.25,
        "turnaround": 0.20,
        "confidence": 0.15,
        "momentum": 0.10,
        "sentiment": 0.05,
    },
    "v1.1-conservative": {
        "quality": 0.35,
        "value": 0.25,
        "confidence": 0.20,
        "momentum": 0.10,
        "sentiment": 0.05,
        "turnaround": 0.05,
    },
    "v1.1-speculative": {
        "turnaround": 0.30,
        "value": 0.25,
        "momentum": 0.15,
        "confidence": 0.15,
        "sentiment": 0.10,
        "quality": 0.05,
    },
    # v1.2 — identical family weights to v1.1 (the realized-risk penalty is
    # additive and does not touch family weights).
    "v1.2-balanced": {
        "quality": 0.25,
        "value": 0.25,
        "turnaround": 0.20,
        "confidence": 0.15,
        "momentum": 0.10,
        "sentiment": 0.05,
    },
    "v1.2-conservative": {
        "quality": 0.35,
        "value": 0.25,
        "confidence": 0.20,
        "momentum": 0.10,
        "sentiment": 0.05,
        "turnaround": 0.05,
    },
    "v1.2-speculative": {
        "turnaround": 0.30,
        "value": 0.25,
        "momentum": 0.15,
        "confidence": 0.15,
        "sentiment": 0.10,
        "quality": 0.05,
    },
    # v1.3 — identical family weights to v1.2 (the Calmar TR-reward is additive,
    # like the v1.2 penalty; family weights untouched so v1/v1.1/v1.2 score
    # history is preserved). v1.3 = v1.2 penalties + the Calmar reward.
    "v1.3-balanced": {
        "quality": 0.25,
        "value": 0.25,
        "turnaround": 0.20,
        "confidence": 0.15,
        "momentum": 0.10,
        "sentiment": 0.05,
    },
    "v1.3-conservative": {
        "quality": 0.35,
        "value": 0.25,
        "confidence": 0.20,
        "momentum": 0.10,
        "sentiment": 0.05,
        "turnaround": 0.05,
    },
    "v1.3-speculative": {
        "turnaround": 0.30,
        "value": 0.25,
        "momentum": 0.15,
        "confidence": 0.15,
        "sentiment": 0.10,
        "quality": 0.05,
    },
}

# ---------------------------------------------------------------------------
# Penalty constants  (additive deductions from total_score)
# ---------------------------------------------------------------------------

_PENALTY_STALE_THESIS: float = 0.15
_PENALTY_MISSING_CRITICAL_DATA: float = 0.10
_PENALTY_WIDE_SPREAD: float = 0.05
_PENALTY_HIGH_RED_FLAG: float = 0.10  # avg recent red_flag_score > threshold
_PENALTY_EXTREME_DILUTION: float = 0.10  # shares_outstanding grew > threshold
_PENALTY_LOW_CONFIDENCE: float = 0.10  # thesis confidence_score < 0.4

_RED_FLAG_PENALTY_THRESHOLD: float = 0.60  # avg red_flag_score above this triggers penalty
_DILUTION_GROWTH_THRESHOLD: float = 0.20  # 20% share count growth triggers penalty
_LOW_CONFIDENCE_THRESHOLD: float = 0.40

# ---------------------------------------------------------------------------
# Realized-risk penalty (#1633, scoring model v1.2 only)
#
# Reads risk_v1 realized metrics (instrument_risk_metrics_current, 3y window)
# and penalises persistently high realized volatility / deep drawdowns. Beta is
# deliberately NOT used: a full-population scan (2026-06-18, dev) found beta_r2
# >= 0.30 for only ~3.4% of instruments, so a market-beta-vs-SPY penalty would
# be statistical noise for this universe (its value is portfolio/sector-relative
# — #1636 / #1674). Calmar was excluded from THIS penalty block because it is
# materially total-return-sensitive; the TR series shipped in #1635 and the Calmar
# return-ratio REWARD now lives in v1.3 (see _calmar_reward). Vol and drawdown
# shape are only marginally TR-moved, so this vol/drawdown penalty stays on the
# price-return basis.
#
# Thresholds are calibrated to the eToro universe's own tail (median annual vol
# ~0.56, median max-drawdown ~-0.50 — SPY-like thresholds would flag ~90% and
# discriminate nothing). They are explicit constants, applied identically every
# run — NOT a cohort-relative normalization (banned in v1); reviewable at every
# version bump like the other penalty constants. Comparators are strict.
# Spec: docs/specs/ranking/2026-06-18-realized-risk-penalty-v1.2.md
# ---------------------------------------------------------------------------

_RISK_PENALTY_WINDOW: str = "3y"

_VOL_HIGH_THRESHOLD: float = 0.90  # ~ universe p75 of vol_annualized
_VOL_EXTREME_THRESHOLD: float = 1.45  # ~ universe p90
_DD_HIGH_THRESHOLD: float = -0.70  # ~ universe p25 (worst quartile)
_DD_EXTREME_THRESHOLD: float = -0.85  # ~ universe p10 (worst decile)

_PENALTY_RISK_HIGH_TIER: float = 0.04
_PENALTY_RISK_EXTREME_TIER: float = 0.08

# ---------------------------------------------------------------------------
# Calmar return-ratio reward (#1635 / #1633-vnext, scoring model v1.3 only)
#
# Reads the risk_v1 3y total-return Calmar (tr_cagr / |max_drawdown|) and rewards
# persistently strong risk-adjusted return. TR is the SEC-derived total return
# (price + reinvested dividends, #1635); the reward fires from tr_calmar only when
# tr_status is trustworthy ({ok, no_dividends}); for tr_incomplete it falls back to
# the price-return calmar (dividend-blind but correct on its own terms) + a caveat
# note. Additive (settled-decisions: additive not multiplicative); EXTREME tested
# first so the extreme tier is reachable. Strict comparators. Thresholds are
# calibrated to the universe's own tr_calmar tail (post-backfill — like the v1.2
# penalty), NOT cohort-relative normalization (banned).
# Spec: docs/specs/ranking/2026-06-19-sec-total-return-calmar-v1.3.md
# ---------------------------------------------------------------------------

# Calibrated to the full-population 3y tr_calmar tail (dev backfill 2026-06-19,
# trustworthy rows n=4,512): p75≈0.73, p90≈2.05. HIGH = top quartile, EXTREME =
# top decile — mirrors the v1.2 penalty's p75/p90 tail basis. Reviewable at every
# version bump like the other constants.
_CALMAR_HIGH_THRESHOLD: float = 0.75  # ~ universe p75 of tr_calmar (top quartile)
_CALMAR_EXTREME_THRESHOLD: float = 2.00  # ~ universe p90 (top decile)

_REWARD_CALMAR_HIGH_TIER: float = 0.04
_REWARD_CALMAR_EXTREME_TIER: float = 0.08

# Mode-scaled risk appetite: conservative weights risk-adjusted return most
# (full reward); speculative least. Applied as a multiplier on the tier size.
_CALMAR_REWARD_MODE_SCALE: dict[str, float] = {
    "conservative": 1.0,
    "balanced": 0.75,
    "speculative": 0.50,
}

# Thesis is stale if it was created more than this many days ago and no
# fresher one exists. In practice the thesis service enforces review_frequency
# per instrument, but the scoring engine applies its own staleness check as
# a defensive second layer.
_THESIS_STALE_DAYS: int = 90

# Lookback for news sentiment (days)
_NEWS_LOOKBACK_DAYS: int = 30

# ---------------------------------------------------------------------------
# Public result types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class FamilyScores:
    quality: float
    value: float
    turnaround: float
    momentum: float
    sentiment: float
    confidence: float


@dataclass(frozen=True)
class PenaltyRecord:
    name: str
    deduction: float
    reason: str


@dataclass(frozen=True)
class RewardRecord:
    """An additive bonus to total_score (v1.3 Calmar reward).

    Kept SEPARATE from PenaltyRecord (Codex ckpt-1 LOW): a negative penalty would
    corrupt ``total_penalty`` / "penalties fired" / the JSON. ``addition`` is a
    positive magnitude added to the score.
    """

    name: str
    addition: float
    reason: str


@dataclass(frozen=True)
class ScoreResult:
    instrument_id: int
    model_version: str
    family_scores: FamilyScores
    penalties: list[PenaltyRecord]
    total_penalty: float
    raw_total: float
    total_score: float
    explanation: str
    # Additive rewards (v1.3 Calmar). Empty for v1/v1.1/v1.2.
    rewards: list[RewardRecord] = field(default_factory=list)
    total_reward: float = 0.0
    # Data-completeness evidence (#1820 §4). Additive — does NOT affect
    # total_score; surfaced to the action layer + LLM record as a gate.
    data_completeness: float | None = None
    completeness_tier: str | None = None
    # IAR evidence block (#1823, P2 of #1815) — Piotroski/Altman/positioning +
    # (injected by compute_rankings) the cross-sectional peer grade. Persisted to
    # scores.analytics_json. EVIDENCE-ONLY: never enters raw_total/total_score.
    analytics: dict[str, Any] | None = None
    # eToro sector code, carried for the compute_rankings peer-grade pass.
    sector: str | None = None
    # Set after ranking pass
    rank: int | None = None
    rank_delta: int | None = None


@dataclass
class RankingResult:
    scored: list[ScoreResult]
    model_version: str
    run_at: datetime = field(default_factory=lambda: datetime.now(tz=UTC))


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _clip(value: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, value))


def _utcnow() -> datetime:
    return datetime.now(tz=UTC)


def _to_float(val: object) -> float | None:
    if val is None:
        return None
    try:
        return float(val)  # type: ignore[arg-type]
    except TypeError, ValueError:
        return None


def _apply_market_cap_basis(
    valuation_row: dict[str, Any] | None,
    resolution: MarketCapResolution,
) -> dict[str, Any] | None:
    """Overlay the #1662 total-company market cap onto the ranking view's row (#1664).

    The ``instrument_valuation`` view (sql/201) NULLs the shares-distorted columns
    (``market_cap_live``, ``fcf_yield``, …) for a curated dual-class issuer because it
    cannot build the correct total-company cap in SQL (that needs per-class prices +
    residual imputation + fail-closed guards — ``_assemble_total_company_cap``). This
    restores the two figures the scorer consumes and can recompute company-wide-correctly:

    - ``market_cap_live`` = the total-company cap (Σ class×price, identical across siblings).
    - ``fcf_yield`` = company TTM free cash flow / total-company cap. ``fcf_ttm`` is the
      issuer-level (combined-company) TTM FCF — dual-class siblings share one CIK's
      fundamentals — so it is the right numerator against the total-company cap.

    ``multiclass_unavailable`` leaves the view's NULLs (honest graceful degrade);
    ``not_multiclass`` leaves the legacy single-class product untouched. Pure —
    table-tested without a DB."""
    if valuation_row is None:
        return None
    if resolution.basis != "total_company" or resolution.total is None:
        return valuation_row
    total_cap = _to_float(resolution.total.value)
    valuation_row["market_cap_live"] = resolution.total.value
    fcf_ttm = _to_float(valuation_row.get("fcf_ttm"))
    if fcf_ttm is not None and total_cap is not None and total_cap > 0:
        valuation_row["fcf_yield"] = fcf_ttm / total_cap
    else:
        valuation_row["fcf_yield"] = None
    return valuation_row


# ---------------------------------------------------------------------------
# Data-completeness score C (#1815 §4 / #1820)
# ---------------------------------------------------------------------------

# §4 weights — must sum to 1.0.
_C_WEIGHT_FUND = 0.30
_C_WEIGHT_FILING = 0.30
_C_WEIGHT_THESIS = 0.15
_C_WEIGHT_PRICE = 0.15
_C_WEIGHT_NEWS = 0.10

# §4 thresholds.
_C_FILING_FULL_MONTHS = 15.0  # 10-K/10-Q filed within this window → full credit
_C_FILING_HALF_MONTHS = 27.0  # within this window → half credit
_C_THESIS_FRESH_DAYS = 90  # thesis newer than this → full credit
_C_PRICE_FULL_TD = 252  # ~1y of trading days → full credit
_C_PRICE_HALF_TD = 63  # ~1 quarter → half credit
_C_NEWS_FULL = 3  # items in last 90d → full credit
_C_NEWS_HALF = 1  # ≥1 item → half credit

# §4 tier cut-points. C < 0.40 is unclearable on price alone (price .15 +
# news .10 = .25), which is exactly the thin-data bug class the gate kills.
_C_TIER_INSUFFICIENT = 0.40
_C_TIER_THIN = 0.70


def _data_completeness(
    fund_present: bool,
    filing_age_months: float | None,
    thesis_present: bool,
    thesis_age_days: int | None,
    price_td_count: int,
    news_90d_count: int,
) -> tuple[float, str]:
    """Compute the #1815 §4 data-completeness score C (0-1) and its tier.

    Pure — table-tested without a DB. Surfaces missingness as missingness; it
    never neutral-fills. Tiers: ``insufficient_data`` (C<0.40 — action layer
    caps at HOLD), ``thin_data`` (<0.70), ``full``.
    """
    fund = 1.0 if fund_present else 0.0

    if filing_age_months is None:
        filing = 0.0
    elif filing_age_months <= _C_FILING_FULL_MONTHS:
        filing = 1.0
    elif filing_age_months <= _C_FILING_HALF_MONTHS:
        filing = 0.5
    else:
        filing = 0.0

    if not thesis_present or thesis_age_days is None:
        thesis = 0.0
    elif thesis_age_days <= _C_THESIS_FRESH_DAYS:
        thesis = 1.0
    else:
        thesis = 0.5

    if price_td_count >= _C_PRICE_FULL_TD:
        price = 1.0
    elif price_td_count >= _C_PRICE_HALF_TD:
        price = 0.5
    else:
        price = 0.0

    if news_90d_count >= _C_NEWS_FULL:
        news = 1.0
    elif news_90d_count >= _C_NEWS_HALF:
        news = 0.5
    else:
        news = 0.0

    c = (
        _C_WEIGHT_FUND * fund
        + _C_WEIGHT_FILING * filing
        + _C_WEIGHT_THESIS * thesis
        + _C_WEIGHT_PRICE * price
        + _C_WEIGHT_NEWS * news
    )

    if c < _C_TIER_INSUFFICIENT:
        tier = "insufficient_data"
    elif c < _C_TIER_THIN:
        tier = "thin_data"
    else:
        tier = "full"

    return c, tier


# ---------------------------------------------------------------------------
# Family score computations
# ---------------------------------------------------------------------------


def _quality_score(
    operating_margin: float | None,
    gross_margin: float | None,
    fcf: float | None,
    net_debt: float | None,
    debt: float | None,
) -> tuple[float, list[str]]:
    """
    Weighted blend of operating margin, gross margin, FCF sign, and debt.

    Returns (score, missing_components).
    """
    notes: list[str] = []

    if operating_margin is not None:
        op_score = _clip((operating_margin - 0.00) / 0.20)
    else:
        op_score = 0.25
        notes.append("operating_margin missing")

    if gross_margin is not None:
        gm_score = _clip((gross_margin - 0.10) / 0.50)
    else:
        gm_score = 0.25
        notes.append("gross_margin missing")

    if fcf is not None:
        fcf_score = 1.0 if fcf > 0 else 0.0
    else:
        fcf_score = 0.25
        notes.append("fcf missing")

    if net_debt is not None:
        debt_score = 1.0 if net_debt <= 0 else 0.5
    elif debt is not None:
        debt_score = 0.5
    else:
        debt_score = 0.25
        notes.append("debt/net_debt missing")

    score = 0.35 * op_score + 0.25 * gm_score + 0.20 * fcf_score + 0.20 * debt_score
    return _clip(score), notes


def _value_score(
    base_value: float | None,
    bear_value: float | None,
    current_price: float | None,
    *,
    pe_ratio: float | None = None,
    fcf_yield: float | None = None,
    price_target_mean: float | None = None,
    thesis_stance: str | None = None,
) -> tuple[float, list[str]]:
    """
    Thesis valuation upside as the primary value proxy.

    Primary path (thesis-based): when base_value is available.
      upside_to_base  = (base_value - current_price) / current_price
      downside_to_bear = (current_price - bear_value) / current_price

    Fallback path (fundamentals-derived): when base_value is None.
      Blends up to three signals — P/E attractiveness (35%), FCF yield (35%),
      and price-target upside (30%) — re-normalised across available components.

    ``thesis_stance`` is the latest thesis' stance when one exists (None = no
    thesis row). A thesis may legitimately decline per-share targets (e.g. an
    ``avoid`` stance), so the fallback note must not report it as absent
    (#2005).

    Returns (score, missing_components).
    """
    notes: list[str] = []

    if current_price is None or current_price <= 0:
        notes.append("current_price missing or zero")

    if base_value is not None:
        # ------------------------------------------------------------------
        # Primary path: thesis-based
        # ------------------------------------------------------------------
        if bear_value is None:
            notes.append("bear_value missing")

        if current_price is None or current_price <= 0:
            return 0.5, notes  # neutral-by-absence

        upside_to_base = (base_value - current_price) / current_price
        upside_score = _clip(upside_to_base / 0.50)  # 50% upside => 1.0

        if bear_value is not None:
            downside_to_bear = (current_price - bear_value) / current_price
            downside_penalty = _clip(downside_to_bear / 0.50)
        else:
            downside_penalty = 0.5  # unknown downside — assume moderate risk
            notes.append("bear_value missing; assuming 0.5 downside penalty")

        score = 0.75 * upside_score + 0.25 * (1.0 - downside_penalty)
        return _clip(score), notes

    # ----------------------------------------------------------------------
    # Fallback path: fundamentals-derived (no thesis, or thesis without
    # per-share targets)
    # ----------------------------------------------------------------------
    fallback_note = (
        "fundamentals fallback (no thesis)"
        if thesis_stance is None
        # "without targets", not "declined": legacy/DQ rows also carry null
        # targets — absence of targets is the fact, intent is not observable.
        else f"fundamentals fallback (thesis without targets, stance: {thesis_stance})"
    )
    notes.append("base_value missing")
    if bear_value is None:
        notes.append("bear_value missing")

    if current_price is None or current_price <= 0:
        return 0.5, notes  # neutral-by-absence

    components: list[tuple[float, float]] = []  # (score, weight)

    if pe_ratio is not None and pe_ratio > 0:
        pe_score = _clip(1.0 - (pe_ratio - 10.0) / 40.0)
        components.append((pe_score, 0.35))

    if fcf_yield is not None:
        fy_score = _clip(fcf_yield / 0.08)
        components.append((fy_score, 0.35))

    if price_target_mean is not None:
        pt_upside = (price_target_mean - current_price) / current_price
        pt_score = _clip(pt_upside / 0.50)
        components.append((pt_score, 0.30))

    if not components:
        notes.append(fallback_note)
        return 0.5, notes

    total_weight = sum(w for _, w in components)
    score = sum(s * w / total_weight for s, w in components)
    notes.append(fallback_note)
    return _clip(score), notes


def _momentum_score(
    return_1m: float | None,
    return_3m: float | None,
    return_6m: float | None,
    *,
    ta_indicators: Mapping[str, float | None] | None = None,
) -> tuple[float, list[str]]:
    """
    Blended momentum score combining return-based signals with TA indicators.

    When ta_indicators is None or empty, falls back to pure return scoring
    (backward compatible).

    TA blending (when available):
      Return-based:      40%
      Trend confirmation: 25% (price vs SMA200, MACD histogram)
      Momentum quality:  20% (RSI regime, Stochastic position)
      Volatility regime: 15% (Bollinger position, ATR context)

    Returns (score, notes_about_missing_components).
    """
    notes: list[str] = []

    # --- Return-based component (original logic) ---
    return_components: list[tuple[float, float]] = []

    if return_1m is not None:
        s1m = _clip((return_1m + 0.10) / 0.30)
        return_components.append((s1m, 0.20))
    else:
        notes.append("return_1m missing")

    if return_3m is not None:
        s3m = _clip((return_3m + 0.15) / 0.45)
        return_components.append((s3m, 0.50))
    else:
        notes.append("return_3m missing")

    if return_6m is not None:
        s6m = _clip((return_6m + 0.20) / 0.60)
        return_components.append((s6m, 0.30))
    else:
        notes.append("return_6m missing")

    if not return_components:
        return_score: float | None = None
    else:
        total_w = sum(w for _, w in return_components)
        return_score = sum(s * w / total_w for s, w in return_components)

    # --- If no TA data, fall back to return-only scoring ---
    if not ta_indicators or not any(
        ta_indicators.get(k) is not None
        for k in ("sma_200", "macd_histogram", "rsi_14", "stoch_k", "bb_upper", "atr_14")
    ):
        if return_score is None:
            return 0.5, notes
        return _clip(return_score), notes

    current_close = ta_indicators.get("current_close")

    # --- Trend confirmation (25%): SMA200 + MACD histogram ---
    trend_parts: list[tuple[float, float]] = []

    sma_200 = ta_indicators.get("sma_200")
    if sma_200 is not None and current_close is not None and sma_200 != 0:
        pct_from_sma = (current_close - sma_200) / sma_200
        trend_parts.append((_clip(0.5 + pct_from_sma * 2.5), 0.60))
    else:
        notes.append("TA: sma_200 unavailable")

    macd_hist = ta_indicators.get("macd_histogram")
    if macd_hist is not None and current_close is not None and current_close != 0:
        # Normalise histogram to percentage of price so signal is
        # comparable across price levels.  Scale factor 20 maps a ±2.5 %
        # histogram to the 0-1 clip range (moderate signal ≈ 0.7/0.3).
        macd_pct = macd_hist / current_close
        trend_parts.append((_clip(0.5 + macd_pct * 20.0), 0.40))
    else:
        notes.append("TA: macd_histogram unavailable")

    trend_score: float | None = None
    if trend_parts:
        tw = sum(w for _, w in trend_parts)
        trend_score = sum(s * w / tw for s, w in trend_parts)

    # --- Momentum quality (20%): RSI + Stochastic ---
    mq_parts: list[tuple[float, float]] = []

    rsi_val = ta_indicators.get("rsi_14")
    if rsi_val is not None:
        # RSI ramp: 30→50 is recovery, 50→70 is healthy uptrend.
        # Oversold (<30) and overbought (>70) are warning zones.
        if rsi_val < 30:
            rsi_score = rsi_val / 60.0
        elif rsi_val <= 70:
            rsi_score = 0.5 + (rsi_val - 30) / 80.0
        else:
            rsi_score = max(0.0, 1.0 - (rsi_val - 70) / 30.0)
        mq_parts.append((_clip(rsi_score), 0.60))
    else:
        notes.append("TA: rsi_14 unavailable")

    stoch_k = ta_indicators.get("stoch_k")
    if stoch_k is not None:
        if stoch_k < 20:
            stoch_score = stoch_k / 40.0
        elif stoch_k <= 80:
            stoch_score = 0.5 + (stoch_k - 20) / 120.0
        else:
            stoch_score = max(0.0, 1.0 - (stoch_k - 80) / 20.0)
        mq_parts.append((_clip(stoch_score), 0.40))
    else:
        notes.append("TA: stoch_k unavailable")

    mq_score: float | None = None
    if mq_parts:
        mw = sum(w for _, w in mq_parts)
        mq_score = sum(s * w / mw for s, w in mq_parts)

    # --- Volatility regime (15%): Bollinger position + ATR ---
    vol_parts: list[tuple[float, float]] = []

    bb_upper = ta_indicators.get("bb_upper")
    bb_lower = ta_indicators.get("bb_lower")
    if bb_upper is not None and bb_lower is not None and current_close is not None:
        bb_width = bb_upper - bb_lower
        if bb_width > 0:
            # Position within band measures trend strength (high = strong
            # uptrend), intentionally opposite to RSI/stoch overbought
            # treatment which measures exhaustion risk.
            vol_parts.append((_clip((current_close - bb_lower) / bb_width), 0.60))
        else:
            vol_parts.append((0.5, 0.60))
    else:
        notes.append("TA: bollinger unavailable")

    atr_val = ta_indicators.get("atr_14")
    if atr_val is not None and current_close is not None and current_close > 0:
        atr_pct = atr_val / current_close
        vol_parts.append((_clip(1.0 - atr_pct * 10.0), 0.40))
    else:
        notes.append("TA: atr_14 unavailable")

    vol_score: float | None = None
    if vol_parts:
        vw = sum(w for _, w in vol_parts)
        vol_score = sum(s * w / vw for s, w in vol_parts)

    # --- Final blend ---
    final_parts: list[tuple[float, float]] = []
    if return_score is not None:
        final_parts.append((return_score, 0.40))
    if trend_score is not None:
        final_parts.append((trend_score, 0.25))
    if mq_score is not None:
        final_parts.append((mq_score, 0.20))
    if vol_score is not None:
        final_parts.append((vol_score, 0.15))

    if not final_parts:
        return 0.5, notes

    total_w = sum(w for _, w in final_parts)
    blended = sum(s * w / total_w for s, w in final_parts)
    return _clip(blended), notes


def _sentiment_score(
    rows: Sequence[tuple[float | None, float | None]],  # [(sentiment_score, importance_score), ...]
) -> tuple[float, list[str]]:
    """
    Importance-weighted mean of signed sentiment scores over the news lookback.

    Signed sentiment is in [-1, 1]; map to [0, 1] via (raw + 1) / 2.

    Returns (score, notes).
    """
    notes: list[str] = []
    out_of_range = [s for s, _ in rows if s is not None and not (-1.0 <= s <= 1.0)]
    if out_of_range:
        logger.warning(
            "_sentiment_score: %d sentiment value(s) outside [-1, 1]: %s — clipping will suppress distortion",
            len(out_of_range),
            out_of_range[:5],  # cap log length
        )
    valid = [(s, w) for s, w in rows if s is not None]

    if not valid:
        notes.append("no recent news events; defaulting to neutral 0.5")
        return 0.5, notes

    total_weight = sum((w if w is not None else 1.0) for _, w in valid)
    if total_weight <= 0:
        total_weight = float(len(valid))

    weighted_sum = sum(s * (w if w is not None else 1.0) for s, w in valid)
    raw_mean = weighted_sum / total_weight
    score = _clip((raw_mean + 1.0) / 2.0)
    return score, notes


def _turnaround_score(
    # Sequence of (operating_margin, revenue_ttm) pairs, newest-first
    snapshots: Sequence[tuple[float | None, float | None]],
    avg_red_flag_score: float | None,
    net_debt: float | None,
) -> tuple[float, list[str]]:
    """
    Blend of margin trend, revenue trend, filing red flags, and debt stress.

    Returns (score, missing_components).
    """
    notes: list[str] = []

    # Margin trend: compare latest vs prior average (2-4 snapshots)
    margins: list[float] = [v for s in snapshots if (v := _to_float(s[0])) is not None]
    if len(margins) >= 2:
        latest_margin = margins[0]
        prior_avg = sum(margins[1:]) / len(margins[1:])
        margin_trend_score = 1.0 if latest_margin > prior_avg else 0.0
    elif len(margins) == 1:
        margin_trend_score = 0.5
        notes.append("only one margin snapshot; trend unknown")
    else:
        margin_trend_score = 0.5
        notes.append("operating_margin missing; margin trend unknown")

    # Revenue trend
    revenues: list[float] = [v for s in snapshots if (v := _to_float(s[1])) is not None]
    if len(revenues) >= 2:
        latest_rev = revenues[0]
        prior_avg_rev = sum(revenues[1:]) / len(revenues[1:])
        revenue_trend_score = 1.0 if latest_rev > prior_avg_rev else 0.0
    elif len(revenues) == 1:
        revenue_trend_score = 0.5
        notes.append("only one revenue snapshot; trend unknown")
    else:
        revenue_trend_score = 0.5
        notes.append("revenue_ttm missing; revenue trend unknown")

    # Red flag component
    if avg_red_flag_score is not None:
        red_flag_component = 1.0 - _clip(avg_red_flag_score)
    else:
        red_flag_component = 0.5
        notes.append("red_flag_score missing; defaulting to neutral")

    # Debt stress
    if net_debt is not None:
        debt_stress_component = 1.0 if net_debt <= 0 else 0.5
    else:
        debt_stress_component = 0.5
        notes.append("net_debt missing; defaulting to neutral")

    score = (
        0.30 * margin_trend_score
        + 0.20 * revenue_trend_score
        + 0.30 * red_flag_component
        + 0.20 * debt_stress_component
    )
    return _clip(score), notes


# ---------------------------------------------------------------------------
# Penalty computation
# ---------------------------------------------------------------------------


def _compute_penalties(
    thesis_created_at: datetime | None,
    confidence_score: float | None,
    has_missing_critical_data: bool,
    spread_flag: bool,
    avg_red_flag_score: float | None,
    shares_outstanding_latest: float | None,
    shares_outstanding_prior: float | None,
    now: datetime,
) -> list[PenaltyRecord]:
    penalties: list[PenaltyRecord] = []

    # Stale thesis — only penalise when a thesis exists but is outdated.
    # Missing thesis is NOT penalised: T3→T2 promotion is based on
    # deterministic signals alone (per #169), and T2→T1 promotion
    # enforces thesis existence in coverage.py.  Penalising missing
    # thesis here would prevent instruments from ever reaching the score
    # threshold needed for promotion.
    if thesis_created_at is not None and (now - thesis_created_at).days > _THESIS_STALE_DAYS:
        age_days = (now - thesis_created_at).days
        penalties.append(
            PenaltyRecord(
                name="stale_thesis",
                deduction=_PENALTY_STALE_THESIS,
                reason=f"thesis is {age_days} days old (threshold: {_THESIS_STALE_DAYS})",
            )
        )

    # Missing critical data
    if has_missing_critical_data:
        penalties.append(
            PenaltyRecord(
                name="missing_critical_data",
                deduction=_PENALTY_MISSING_CRITICAL_DATA,
                reason="one or more critical data components absent",
            )
        )

    # Wide spread
    if spread_flag:
        penalties.append(
            PenaltyRecord(
                name="wide_spread",
                deduction=_PENALTY_WIDE_SPREAD,
                reason="spread_flag is set for current quote",
            )
        )

    # High red flag score (legal/regulatory proxy)
    if avg_red_flag_score is not None and avg_red_flag_score > _RED_FLAG_PENALTY_THRESHOLD:
        penalties.append(
            PenaltyRecord(
                name="high_red_flag",
                deduction=_PENALTY_HIGH_RED_FLAG,
                reason=f"avg recent red_flag_score={avg_red_flag_score:.2f} > threshold {_RED_FLAG_PENALTY_THRESHOLD}",
            )
        )

    # Extreme dilution risk
    if shares_outstanding_latest is not None and shares_outstanding_prior is not None and shares_outstanding_prior > 0:
        dilution_growth = (shares_outstanding_latest - shares_outstanding_prior) / shares_outstanding_prior
        if dilution_growth > _DILUTION_GROWTH_THRESHOLD:
            penalties.append(
                PenaltyRecord(
                    name="extreme_dilution",
                    deduction=_PENALTY_EXTREME_DILUTION,
                    reason=(
                        f"shares outstanding grew {dilution_growth:.1%} > threshold {_DILUTION_GROWTH_THRESHOLD:.0%}"
                    ),
                )
            )

    # Low confidence thesis
    if confidence_score is not None and confidence_score < _LOW_CONFIDENCE_THRESHOLD:
        penalties.append(
            PenaltyRecord(
                name="low_confidence",
                deduction=_PENALTY_LOW_CONFIDENCE,
                reason=f"thesis confidence_score={confidence_score:.2f} < threshold {_LOW_CONFIDENCE_THRESHOLD}",
            )
        )

    return penalties


def _realized_risk_penalties(
    vol_annualized: float | None,
    vol_status: str | None,
    max_drawdown: float | None,
    drawdown_status: str | None,
) -> tuple[list[PenaltyRecord], list[str]]:
    """Realized-risk penalties from risk_v1 3y metrics (#1633, v1.2 only).

    Returns ``(penalties, notes)``. Notes carry the no-metric / non-ok / None
    explanations (which are NOT penalties) so the caller can fold them into the
    score explanation, exactly like the family notes.

    Honest absence (prevention-log: a ``None`` / non-``ok`` status must not be
    folded into a signal bucket): absence is never a risk signal. A missing row,
    a non-``ok`` status, or a NULL value yields NO penalty and a note — never a
    deduction. Only an ``ok`` status with a present value can trigger a penalty.

    Comparators are strict (``>`` vol, ``<`` drawdown); a value exactly on a
    threshold falls to the lower tier (or none). Tiers are additive deductions
    sized by severity — NOT multiplicative (settled-decision compliant).
    """
    penalties: list[PenaltyRecord] = []
    notes: list[str] = []

    # Volatility
    if vol_status != "ok":
        notes.append(f"realized-risk: vol status={vol_status or 'no metrics'}")
    elif vol_annualized is None:
        notes.append("realized-risk: vol value missing despite ok status")
    elif vol_annualized > _VOL_EXTREME_THRESHOLD:
        penalties.append(
            PenaltyRecord(
                name="high_realized_volatility",
                deduction=_PENALTY_RISK_EXTREME_TIER,
                reason=(f"3y annualized vol={vol_annualized:.2f} > extreme threshold {_VOL_EXTREME_THRESHOLD}"),
            )
        )
    elif vol_annualized > _VOL_HIGH_THRESHOLD:
        penalties.append(
            PenaltyRecord(
                name="high_realized_volatility",
                deduction=_PENALTY_RISK_HIGH_TIER,
                reason=(f"3y annualized vol={vol_annualized:.2f} > high threshold {_VOL_HIGH_THRESHOLD}"),
            )
        )

    # Max drawdown (negative fraction; more negative = worse)
    if drawdown_status != "ok":
        notes.append(f"realized-risk: drawdown status={drawdown_status or 'no metrics'}")
    elif max_drawdown is None:
        notes.append("realized-risk: drawdown value missing despite ok status")
    elif max_drawdown < _DD_EXTREME_THRESHOLD:
        penalties.append(
            PenaltyRecord(
                name="deep_drawdown",
                deduction=_PENALTY_RISK_EXTREME_TIER,
                reason=(f"3y max drawdown={max_drawdown:.2f} < extreme threshold {_DD_EXTREME_THRESHOLD}"),
            )
        )
    elif max_drawdown < _DD_HIGH_THRESHOLD:
        penalties.append(
            PenaltyRecord(
                name="deep_drawdown",
                deduction=_PENALTY_RISK_HIGH_TIER,
                reason=(f"3y max drawdown={max_drawdown:.2f} < high threshold {_DD_HIGH_THRESHOLD}"),
            )
        )

    return penalties, notes


def _calmar_reward(
    model_version: str,
    tr_calmar: float | None,
    tr_status: str | None,
    price_calmar: float | None,
) -> tuple[list[RewardRecord], list[str]]:
    """Calmar return-ratio reward from risk_v1 3y metrics (#1635, v1.3 only).

    Returns ``(rewards, notes)``. The reward basis is the TOTAL-RETURN Calmar when
    ``tr_status`` is trustworthy (``ok`` / ``no_dividends`` — for a non-payer
    tr_calmar == price calmar, exact); for ``tr_incomplete`` the TR series is
    untrusted so it falls back to the price-return ``calmar`` (dividend-blind but
    correct on its own terms) + a caveat note. Absence (no metrics / no usable
    Calmar) yields a note, never a reward (honest-absence — prevention-log).

    Additive, mode-scaled. EXTREME tier tested first so it is reachable. Comparators
    strict; a value exactly on a threshold falls to the lower tier (or none).
    """
    rewards: list[RewardRecord] = []
    notes: list[str] = []

    if tr_status in ("ok", "no_dividends"):
        basis, basis_calmar = "total-return", tr_calmar
    elif tr_status == "tr_incomplete":
        basis, basis_calmar = "price-return (tr_incomplete)", price_calmar
        notes.append("calmar reward: tr_incomplete — using dividend-blind price-return Calmar")
    else:
        notes.append(f"calmar reward: tr status={tr_status or 'no metrics'}")
        return rewards, notes

    if basis_calmar is None:
        notes.append(f"calmar reward: {basis} Calmar value missing")
        return rewards, notes

    # Mode-scaled tier size. Mode is the suffix after the version (v1.3-balanced).
    mode = model_version.split("-", 1)[1] if "-" in model_version else "balanced"
    scale = _CALMAR_REWARD_MODE_SCALE.get(mode, _CALMAR_REWARD_MODE_SCALE["balanced"])

    if basis_calmar > _CALMAR_EXTREME_THRESHOLD:
        rewards.append(
            RewardRecord(
                name="strong_calmar",
                addition=_REWARD_CALMAR_EXTREME_TIER * scale,
                reason=(
                    f"3y {basis} Calmar={basis_calmar:.2f} > extreme threshold "
                    f"{_CALMAR_EXTREME_THRESHOLD} (mode scale {scale})"
                ),
            )
        )
    elif basis_calmar > _CALMAR_HIGH_THRESHOLD:
        rewards.append(
            RewardRecord(
                name="strong_calmar",
                addition=_REWARD_CALMAR_HIGH_TIER * scale,
                reason=(
                    f"3y {basis} Calmar={basis_calmar:.2f} > high threshold "
                    f"{_CALMAR_HIGH_THRESHOLD} (mode scale {scale})"
                ),
            )
        )

    return rewards, notes


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------


def _load_instrument_data(
    conn: psycopg.Connection[Any],
    instrument_id: int,
    now: datetime,
) -> dict[str, Any]:
    """
    Load all signals required for scoring a single instrument.

    Returns a flat dict of typed sub-results. Each query uses dict_row so
    callers reference columns by name, not position — a schema change that
    adds or reorders columns will raise a KeyError rather than silently
    producing wrong scores.

    All DB access is read-only — no writes.
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        # Latest fundamentals snapshot (+ up to 4 prior for trend)
        cur.execute(
            """
            SELECT operating_margin, gross_margin, fcf, net_debt, debt,
                   revenue_ttm, shares_outstanding
            FROM fundamentals_snapshot
            WHERE instrument_id = %(id)s
            ORDER BY as_of_date DESC
            LIMIT 5
            """,
            {"id": instrument_id},
        )
        fund_rows: list[dict[str, Any]] = cur.fetchall()

        # Latest price features
        cur.execute(
            """
            SELECT return_1m, return_3m, return_6m, close,
                   sma_200, macd_histogram, rsi_14,
                   stoch_k, stoch_d,
                   bb_upper, bb_lower, atr_14
            FROM price_daily
            WHERE instrument_id = %(id)s
              AND close IS NOT NULL
            ORDER BY price_date DESC
            LIMIT 1
            """,
            {"id": instrument_id},
        )
        price_row: dict[str, Any] | None = cur.fetchone()

        # Current quote (spread flag + last price).
        # quotes is keyed on instrument_id (PRIMARY KEY), so at most one row
        # exists per instrument. The ORDER BY is included defensively in case
        # the schema ever relaxes that constraint.
        cur.execute(
            """
            SELECT spread_flag, last, bid, ask
            FROM quotes
            WHERE instrument_id = %(id)s
            ORDER BY quoted_at DESC
            LIMIT 1
            """,
            {"id": instrument_id},
        )
        quote_row: dict[str, Any] | None = cur.fetchone()

        # Latest thesis (confidence + valuation bands + stance + created_at)
        cur.execute(
            """
            SELECT confidence_score, base_value, bear_value, stance, created_at
            FROM theses
            WHERE instrument_id = %(id)s
            ORDER BY thesis_version DESC
            LIMIT 1
            """,
            {"id": instrument_id},
        )
        thesis_row: dict[str, Any] | None = cur.fetchone()

        # Recent news sentiment (last 30 days).
        # Cutoff is a full TIMESTAMPTZ to match the event_time column type.
        cutoff = now - timedelta(days=_NEWS_LOOKBACK_DAYS)
        cur.execute(
            """
            SELECT sentiment_score, importance_score
            FROM news_events
            WHERE instrument_id = %(id)s
              AND event_time >= %(cutoff)s
              AND sentiment_score IS NOT NULL
            ORDER BY event_time DESC
            """,
            {"id": instrument_id, "cutoff": cutoff},
        )
        news_rows: list[dict[str, Any]] = cur.fetchall()

        # Average red flag score from filing events over the last 90 days.
        # filing_date is a DATE column, so the cutoff is passed as date to
        # avoid implicit cast ambiguity.
        rf_cutoff = now - timedelta(days=90)
        cur.execute(
            """
            SELECT AVG(red_flag_score) AS avg_red_flag
            FROM filing_events
            WHERE instrument_id = %(id)s
              AND filing_date >= %(cutoff)s
              AND red_flag_score IS NOT NULL
            """,
            {"id": instrument_id, "cutoff": rf_cutoff.date()},
        )
        rf_row: dict[str, Any] | None = cur.fetchone()

        # --- Data-completeness inputs (#1820 §4) ---------------------------
        # Each feeds the C score; all are EVIDENCE/safety-gate inputs, so they
        # query the full population, not the LIMIT-5 fund sample.
        #
        # fund_present: any snapshot with revenue_ttm AND (op OR gross) margin.
        cur.execute(
            """
            SELECT EXISTS (
                SELECT 1 FROM fundamentals_snapshot
                WHERE instrument_id = %(id)s
                  AND revenue_ttm IS NOT NULL
                  AND (operating_margin IS NOT NULL OR gross_margin IS NOT NULL)
            ) AS fund_present
            """,
            {"id": instrument_id},
        )
        fund_present_row: dict[str, Any] | None = cur.fetchone()

        # Latest 10-K / 10-Q filed date. Source rule: SEC annual (10-K, Reg S-K)
        # / quarterly (10-Q, Exchange Act §13) reports; filing_events is our
        # ingested EDGAR filing-event record (column is filing_type, NOT
        # form_type). Amendments included.
        cur.execute(
            """
            SELECT MAX(filing_date) AS last_10kq
            FROM filing_events
            WHERE instrument_id = %(id)s
              AND filing_type IN ('10-K', '10-Q', '10-K/A', '10-Q/A')
            """,
            {"id": instrument_id},
        )
        filing_recency_row: dict[str, Any] | None = cur.fetchone()

        # Price-history depth (trading days with a close).
        cur.execute(
            """
            SELECT COUNT(*) AS price_td
            FROM price_daily
            WHERE instrument_id = %(id)s
              AND close IS NOT NULL
            """,
            {"id": instrument_id},
        )
        price_count_row: dict[str, Any] | None = cur.fetchone()

        # News coverage in the last 90 days (§4 uses a 90d window, distinct
        # from the 30d sentiment lookback above).
        news_cutoff_90 = now - timedelta(days=90)
        cur.execute(
            """
            SELECT COUNT(*) AS news_90d
            FROM news_events
            WHERE instrument_id = %(id)s
              AND event_time >= %(cutoff)s
            """,
            {"id": instrument_id, "cutoff": news_cutoff_90},
        )
        news_count_row: dict[str, Any] | None = cur.fetchone()

        # Valuation multiples from view (enrichment).
        # Degrade gracefully if the view or table does not exist yet
        # (pre-migration environment, partial test setup).
        # Wrapped in a savepoint so UndefinedTable only rolls back
        # the enrichment query, not the entire transaction.
        valuation_row: dict[str, Any] | None = None
        try:
            with conn.transaction():
                cur.execute(
                    """
                    SELECT pe_ratio, pb_ratio, p_fcf_ratio, fcf_yield,
                           debt_equity_ratio, market_cap_live, current_price,
                           fcf_ttm
                    FROM instrument_valuation
                    WHERE instrument_id = %(id)s
                    """,
                    {"id": instrument_id},
                )
                valuation_row = cur.fetchone()
        except psycopg.errors.UndefinedTable, psycopg.errors.UndefinedColumn:
            pass  # savepoint already rolled back; prior queries intact

        # Realized-risk metrics (#1633, risk_v1 3y window). Read-only enrichment;
        # consumed only by v1.2+ models. RISK_METRICS_VERSION is imported lazily
        # to avoid the risk_metrics → scheduler → refresh_cascade module-scope
        # import chain (mirrors #1632 thesis ingestion). Own savepoint: the table
        # may be absent in a partial test DB → degrade to None (no penalty).
        risk_row: dict[str, Any] | None = None
        try:
            from app.services.risk_metrics import RISK_METRICS_VERSION

            with conn.transaction():
                cur.execute(
                    """
                    SELECT vol_annualized, vol_status,
                           max_drawdown, drawdown_status,
                           calmar, tr_calmar, tr_status
                    FROM instrument_risk_metrics_current
                    WHERE instrument_id = %(id)s
                      AND metric_version = %(mv)s
                      AND window_key = %(win)s
                    """,
                    {"id": instrument_id, "mv": RISK_METRICS_VERSION, "win": _RISK_PENALTY_WINDOW},
                )
                risk_row = cur.fetchone()
        except psycopg.errors.UndefinedTable, psycopg.errors.UndefinedColumn:
            # Catches BOTH (PEP 758 bare-tuple except, valid on 3.14; ruff format
            # normalises away the parens). Mirrors the valuation savepoint above:
            # a partial schema where the table exists but a selected column is
            # absent raises UndefinedColumn, which must also degrade to
            # risk_row = None (no penalty), not propagate.
            pass  # savepoint already rolled back; prior queries intact

    # #1664: for a curated dual-class issuer the view NULLs the shares-distorted
    # valuation columns (combined-shares × one-class-price is structurally wrong;
    # the correct total-company cap cannot be built in SQL). Overlay the #1662
    # total-company figure via the single policy helper so the value score uses
    # the correct market cap / FCF yield rather than a degraded NULL. Own savepoint:
    # resolve_market_cap_basis reads instrument_class_shares_outstanding (#1623),
    # which may be absent in a partial test DB → contain the rollback.
    if valuation_row is not None:
        try:
            with conn.transaction():
                resolution = resolve_market_cap_basis(conn, instrument_id=instrument_id)
        except (psycopg.errors.UndefinedTable,):
            resolution = MarketCapResolution(basis="not_multiclass")
        valuation_row = _apply_market_cap_basis(valuation_row, resolution)

    # Sector code (eToro, for the peer-grade cohort) + SEC SIC (→ GICS sector,
    # for the F/Z financials suppression). #1823. Own cursor — the dict_row
    # cursor above is closed by this point.
    # Savepoint-guarded: instrument_sec_profile may be absent in a partial schema
    # (e.g. a thin test DB) → degrade to no sector/sic rather than failing the
    # whole score, mirroring the valuation/risk savepoint pattern above.
    sector_code: str | None = None
    sic: str | None = None
    try:
        with conn.transaction():
            with conn.cursor() as sec_cur:
                sec_cur.execute(
                    """
                    SELECT i.sector, p.sic
                    FROM instruments i
                    LEFT JOIN instrument_sec_profile p ON p.instrument_id = i.instrument_id
                    WHERE i.instrument_id = %(id)s
                    """,
                    {"id": instrument_id},
                )
                sec_row = sec_cur.fetchone()
                if sec_row is not None:
                    sector_code = sec_row[0]
                    sic = sec_row[1]
    except psycopg.errors.UndefinedTable, psycopg.errors.UndefinedColumn:
        sector_code = None
        sic = None

    return {
        "sector_code": sector_code,
        "sic": sic,
        "fund_rows": fund_rows,
        "price_row": price_row,
        "quote_row": quote_row,
        "thesis_row": thesis_row,
        "news_rows": news_rows,
        # AVG() always returns one row, even when no matching rows exist (returns NULL).
        # rf_row is therefore never None; avg_red_flag may be None if no filings matched.
        "avg_red_flag_score": _to_float(rf_row["avg_red_flag"]) if rf_row is not None else None,
        "valuation_row": valuation_row,
        "risk_row": risk_row,
        # Data-completeness inputs (#1820 §4)
        "fund_present": bool(fund_present_row["fund_present"]) if fund_present_row is not None else False,
        "last_10kq_date": filing_recency_row["last_10kq"] if filing_recency_row is not None else None,
        "price_td_count": int(price_count_row["price_td"]) if price_count_row is not None else 0,
        "news_90d_count": int(news_count_row["news_90d"]) if news_count_row is not None else 0,
    }


# ---------------------------------------------------------------------------
# Single-instrument scoring
# ---------------------------------------------------------------------------


def compute_score(
    instrument_id: int,
    conn: psycopg.Connection[Any],
    model_version: str = _DEFAULT_MODEL_VERSION,
) -> ScoreResult:
    """
    Compute a scored result for a single instrument.

    Does not persist — callers are responsible for writing to the DB.
    Raises KeyError if model_version is not recognised.
    """
    weights = _WEIGHT_MODES.get(model_version)
    if weights is None:
        raise KeyError(f"Unknown model_version: {model_version!r}. Known: {list(_WEIGHT_MODES)}")

    now = _utcnow()
    data = _load_instrument_data(conn, instrument_id, now)

    fund_rows = data["fund_rows"]
    price_row = data["price_row"]
    quote_row = data["quote_row"]
    thesis_row = data["thesis_row"]
    news_rows = data["news_rows"]
    avg_red_flag_score: float | None = data["avg_red_flag_score"]  # type: ignore[assignment]

    # ------------------------------------------------------------------
    # Extract raw signals
    # ------------------------------------------------------------------

    # Fundamentals — latest row (dict_row: access by column name)
    if fund_rows:
        latest_fund = fund_rows[0]
        operating_margin = _to_float(latest_fund["operating_margin"])
        gross_margin = _to_float(latest_fund["gross_margin"])
        fcf = _to_float(latest_fund["fcf"])
        net_debt = _to_float(latest_fund["net_debt"])
        debt = _to_float(latest_fund["debt"])
        shares_latest = _to_float(latest_fund["shares_outstanding"])
    else:
        operating_margin = gross_margin = fcf = net_debt = debt = shares_latest = None

    # Shares outstanding prior (use oldest available snapshot for dilution check)
    if len(fund_rows) >= 2:
        shares_prior = _to_float(fund_rows[-1]["shares_outstanding"])
    else:
        shares_prior = None

    # Fundamentals snapshots for trend (newest-first list of (op_margin, revenue))
    snapshots: list[tuple[float | None, float | None]] = [
        (_to_float(r["operating_margin"]), _to_float(r["revenue_ttm"])) for r in fund_rows
    ]

    # Price features
    if price_row:
        return_1m = _to_float(price_row["return_1m"])
        return_3m = _to_float(price_row["return_3m"])
        return_6m = _to_float(price_row["return_6m"])
        close_price = _to_float(price_row["close"])
    else:
        return_1m = return_3m = return_6m = close_price = None

    # Quote — prefer last price from quote, fall back to close
    if quote_row:
        spread_flag: bool = bool(quote_row["spread_flag"])
        last_price = _to_float(quote_row["last"])
        # Best available price: quote last > quote mid > daily close
        if last_price and last_price > 0:
            current_price: float | None = last_price
        else:
            bid = _to_float(quote_row["bid"])
            ask = _to_float(quote_row["ask"])
            if bid and ask and bid > 0 and ask > 0:
                current_price = (bid + ask) / 2.0
            else:
                current_price = close_price
    else:
        spread_flag = False
        current_price = close_price

    # Thesis
    if thesis_row:
        thesis_confidence = _to_float(thesis_row["confidence_score"])
        thesis_created_at: datetime | None = thesis_row["created_at"]
    else:
        thesis_confidence = None
        thesis_created_at = None

    # News sentiment rows: [(sentiment_score, importance_score), ...]
    sentiment_rows: list[tuple[float | None, float | None]] = [
        (_to_float(r["sentiment_score"]), _to_float(r["importance_score"])) for r in news_rows
    ]

    # ------------------------------------------------------------------
    # Missing critical data flag
    # Critical = no fundamentals at all AND no thesis valuation AND no price
    # ------------------------------------------------------------------
    has_missing_critical = not fund_rows and thesis_row is None and price_row is None

    # ------------------------------------------------------------------
    # Family scores
    # ------------------------------------------------------------------
    explanation_parts: list[str] = []

    q_score, q_notes = _quality_score(operating_margin, gross_margin, fcf, net_debt, debt)
    if q_notes:
        explanation_parts.append("quality: " + "; ".join(q_notes))

    val_row = data.get("valuation_row")

    v_score, v_notes = _value_score(
        base_value=_to_float(thesis_row["base_value"]) if thesis_row else None,
        bear_value=_to_float(thesis_row["bear_value"]) if thesis_row else None,
        current_price=current_price,
        pe_ratio=_to_float(val_row["pe_ratio"]) if val_row else None,
        fcf_yield=_to_float(val_row["fcf_yield"]) if val_row else None,
        # #539: analyst price targets sourced from FMP retired; the
        # value-score branch falls back to thesis base/bear and
        # multiples when this is None.
        price_target_mean=None,
        thesis_stance=str(thesis_row["stance"]) if thesis_row and thesis_row["stance"] is not None else None,
    )
    if v_notes:
        explanation_parts.append("value: " + "; ".join(v_notes))

    # Build TA indicators dict for momentum score.
    # Only v1.1+ models use TA-enhanced momentum; v1 models preserve
    # the original return-only formula for score history compatibility.
    ta_indicators: dict[str, float | None] | None = None
    if price_row is not None and model_version.startswith(_TA_MOMENTUM_PREFIXES):
        ta_keys = [
            "sma_200",
            "macd_histogram",
            "rsi_14",
            "stoch_k",
            "stoch_d",
            "bb_upper",
            "bb_lower",
            "atr_14",
        ]
        ta_raw = {k: _to_float(price_row.get(k)) for k in ta_keys}
        if any(v is not None for v in ta_raw.values()):
            ta_indicators = ta_raw
            ta_indicators["current_close"] = current_price

    m_score, m_notes = _momentum_score(
        return_1m,
        return_3m,
        return_6m,
        ta_indicators=ta_indicators,
    )
    if m_notes:
        explanation_parts.append("momentum: " + "; ".join(m_notes))

    s_score, s_notes = _sentiment_score(sentiment_rows)
    if s_notes:
        explanation_parts.append("sentiment: " + "; ".join(s_notes))

    t_score, t_notes = _turnaround_score(snapshots, avg_red_flag_score, net_debt)
    if t_notes:
        explanation_parts.append("turnaround: " + "; ".join(t_notes))

    c_score = _clip(thesis_confidence) if thesis_confidence is not None else 0.5
    if thesis_confidence is None:
        explanation_parts.append("confidence: no thesis; defaulting to 0.5")

    family = FamilyScores(
        quality=q_score,
        value=v_score,
        turnaround=t_score,
        momentum=m_score,
        sentiment=s_score,
        confidence=c_score,
    )

    # ------------------------------------------------------------------
    # Weighted total (raw, pre-penalty)
    # ------------------------------------------------------------------
    raw_total = (
        weights["quality"] * family.quality
        + weights["value"] * family.value
        + weights["turnaround"] * family.turnaround
        + weights["momentum"] * family.momentum
        + weights["sentiment"] * family.sentiment
        + weights["confidence"] * family.confidence
    )

    # ------------------------------------------------------------------
    # Penalties
    # ------------------------------------------------------------------
    penalties = _compute_penalties(
        thesis_created_at=thesis_created_at,
        confidence_score=thesis_confidence,
        has_missing_critical_data=has_missing_critical,
        spread_flag=spread_flag,
        avg_red_flag_score=avg_red_flag_score,
        shares_outstanding_latest=shares_latest,
        shares_outstanding_prior=shares_prior,
        now=now,
    )

    # Realized-risk penalty — v1.2+ only (#1633). v1 / v1.1 score history is
    # unchanged. Absence (no row / non-ok status / NULL) yields a note, never a
    # deduction (handled inside _realized_risk_penalties).
    if model_version.startswith(_RISK_PENALTY_PREFIXES):
        risk_row = data.get("risk_row")
        risk_penalties, risk_notes = _realized_risk_penalties(
            vol_annualized=_to_float(risk_row["vol_annualized"]) if risk_row else None,
            vol_status=risk_row["vol_status"] if risk_row else None,
            max_drawdown=_to_float(risk_row["max_drawdown"]) if risk_row else None,
            drawdown_status=risk_row["drawdown_status"] if risk_row else None,
        )
        penalties = penalties + risk_penalties
        explanation_parts.extend(risk_notes)

    total_penalty = sum(p.deduction for p in penalties)

    if penalties:
        penalty_names = ", ".join(p.name for p in penalties)
        explanation_parts.append(f"penalties fired: {penalty_names} (total deduction: {total_penalty:.2f})")

    # Calmar TR-reward — v1.3+ only (#1635). Additive bonus from the risk_v1 3y
    # total-return Calmar. Absence yields a note, never a reward.
    rewards: list[RewardRecord] = []
    if model_version.startswith(_CALMAR_REWARD_PREFIXES):
        risk_row = data.get("risk_row")
        rewards, reward_notes = _calmar_reward(
            model_version=model_version,
            tr_calmar=_to_float(risk_row["tr_calmar"]) if risk_row else None,
            tr_status=risk_row["tr_status"] if risk_row else None,
            price_calmar=_to_float(risk_row["calmar"]) if risk_row else None,
        )
        explanation_parts.extend(reward_notes)

    total_reward = sum(r.addition for r in rewards)
    if rewards:
        reward_names = ", ".join(r.name for r in rewards)
        explanation_parts.append(f"rewards fired: {reward_names} (total bonus: {total_reward:.2f})")

    total_score = _clip(raw_total - total_penalty + total_reward)

    explanation = "; ".join(explanation_parts) if explanation_parts else "all signals present"

    # ------------------------------------------------------------------
    # Data-completeness evidence (#1820 §4). Additive — independent of the
    # total_score math above; consumed by the portfolio action layer (caps
    # BUY/ADD when insufficient) and the LLM record.
    # ------------------------------------------------------------------
    last_10kq = data.get("last_10kq_date")
    filing_age_months = ((now.date() - last_10kq).days / 30.4375) if last_10kq is not None else None
    thesis_age_days = (now - thesis_created_at).days if thesis_created_at is not None else None
    data_completeness, completeness_tier = _data_completeness(
        fund_present=bool(data.get("fund_present", False)),
        filing_age_months=filing_age_months,
        thesis_present=thesis_row is not None,
        thesis_age_days=thesis_age_days,
        price_td_count=int(data.get("price_td_count", 0)),
        news_90d_count=int(data.get("news_90d_count", 0)),
    )

    # ------------------------------------------------------------------
    # IAR evidence signals (#1823 §P2). Additive — never enters the
    # total_score math above. Piotroski/Altman + positioning are per-
    # instrument; the cross-sectional peer_grade is injected by
    # compute_rankings from the run population.
    # ------------------------------------------------------------------
    fund_rows_for_shares = data["fund_rows"]
    shares_out = _to_float(fund_rows_for_shares[0]["shares_outstanding"]) if fund_rows_for_shares else None
    sic_cls = resolve_sector_spdr(data.get("sic"))  # type: ignore[arg-type]
    gics_sector = sic_cls.gics_sector if sic_cls is not None else None
    analytics = assemble_instrument_analytics(
        instrument_id, conn, gics_sector=gics_sector, shares_outstanding=shares_out
    )

    return ScoreResult(
        instrument_id=instrument_id,
        model_version=model_version,
        family_scores=family,
        penalties=penalties,
        total_penalty=total_penalty,
        raw_total=raw_total,  # pre-penalty weighted sum; always [0,1] by construction
        total_score=total_score,
        explanation=explanation,
        rewards=rewards,
        total_reward=total_reward,
        data_completeness=data_completeness,
        completeness_tier=completeness_tier,
        analytics=analytics,
        sector=data.get("sector_code"),  # type: ignore[arg-type]
    )


# ---------------------------------------------------------------------------
# Ranking pass
# ---------------------------------------------------------------------------


def _fetch_prior_ranks(
    conn: psycopg.Connection[Any],
    instrument_ids: list[int],
    model_version: str,
) -> dict[int, int]:
    """
    Return the most recent rank for each instrument_id under this model_version.

    Only looks at the most recent scoring run (identified by MAX(scored_at)
    per instrument). Returns an empty dict if no prior rows exist.
    """
    if not instrument_ids:
        return {}

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT DISTINCT ON (instrument_id)
                instrument_id,
                rank
            FROM scores
            WHERE instrument_id = ANY(%(ids)s)
              AND model_version = %(mv)s
              AND rank IS NOT NULL
            ORDER BY instrument_id, scored_at DESC
            """,
            {"ids": instrument_ids, "mv": model_version},
        )
        rows: list[dict[str, Any]] = cur.fetchall()

    return {int(r["instrument_id"]): int(r["rank"]) for r in rows if r["rank"] is not None}


def compute_rankings(
    conn: psycopg.Connection[Any],
    model_version: str = _DEFAULT_MODEL_VERSION,
) -> RankingResult:
    """
    Score all eligible Tier 1 instruments and produce a ranked list.

    Eligibility: instrument is Tier 1 coverage, is_tradable=TRUE, and has
    at least one of: a thesis row, a fundamentals snapshot, or price data.

    Steps:
      1. Load eligible instrument_ids.
      2. compute_score() for each.
      3. Sort descending by total_score, assign rank.
      4. Compute rank_delta vs most recent prior run (same model_version).
      5. Persist all rows to scores table atomically.

    Returns RankingResult. Raises on unknown model_version.
    """
    if model_version not in _WEIGHT_MODES:
        raise KeyError(f"Unknown model_version: {model_version!r}")

    # Eligible instruments — all tradable instruments with a coverage row
    # and at least some data.  No tier gate: scoring runs for every tier
    # (including T3) so the weekly coverage review can promote on
    # deterministic signals alone.  The coverage JOIN ensures scores are
    # only created for instruments that review_coverage can see.
    with conn.cursor(row_factory=psycopg.rows.dict_row) as elig_cur:
        elig_cur.execute(
            """
            SELECT DISTINCT i.instrument_id
            FROM instruments i
            JOIN coverage c ON c.instrument_id = i.instrument_id
            WHERE i.is_tradable = TRUE
              AND c.filings_status = 'analysable'
              AND (
                  EXISTS (SELECT 1 FROM theses t WHERE t.instrument_id = i.instrument_id)
                  OR EXISTS (SELECT 1 FROM fundamentals_snapshot f WHERE f.instrument_id = i.instrument_id)
                  OR EXISTS (SELECT 1 FROM price_daily p WHERE p.instrument_id = i.instrument_id)
              )
            ORDER BY i.instrument_id
            """,
        )
        rows = elig_cur.fetchall()

    instrument_ids = [int(r["instrument_id"]) for r in rows]
    if not instrument_ids:
        logger.info("compute_rankings: no eligible instruments found")
        return RankingResult(scored=[], model_version=model_version)

    logger.info("compute_rankings: scoring %d eligible instrument(s) [model=%s]", len(instrument_ids), model_version)

    # Score each instrument, skipping failures
    results: list[ScoreResult] = []
    for iid in instrument_ids:
        try:
            result = compute_score(iid, conn, model_version)
            results.append(result)
        except Exception:
            logger.warning("compute_rankings: scoring failed for instrument_id=%d, skipping", iid, exc_info=True)

    # Sort descending by total_score, assign rank (1 = best)
    results.sort(key=lambda r: r.total_score, reverse=True)

    # Cross-sectional hybrid peer grade (#1823 §P2) — computed from this run's
    # absolute family scores grouped by eToro sector. Evidence-only; injected into
    # each instrument's analytics block. Percentile cohort is the run-eligible
    # population (basis records this).
    peer_run_items = [
        (
            r.instrument_id,
            r.sector,
            {
                "quality": r.family_scores.quality,
                "value": r.family_scores.value,
                "turnaround": r.family_scores.turnaround,
                "momentum": r.family_scores.momentum,
                "sentiment": r.family_scores.sentiment,
                "confidence": r.family_scores.confidence,
            },
        )
        for r in results
    ]
    peer_grades = compute_peer_grades(peer_run_items)

    # Read prior ranks and write new rows inside a single transaction.
    # Keeping _fetch_prior_ranks inside the transaction prevents a TOCTOU race
    # where a concurrent scoring run commits between the prior-rank read and our
    # own insert, causing rank_delta to be computed against the just-committed run
    # instead of the true prior run.
    run_at = _utcnow()
    ranked: list[ScoreResult] = []

    with conn.transaction():
        prior_ranks = _fetch_prior_ranks(conn, [r.instrument_id for r in results], model_version)

        for position, result in enumerate(results, start=1):
            prior_rank = prior_ranks.get(result.instrument_id)
            rank_delta = (prior_rank - position) if prior_rank is not None else None
            analytics = dict(result.analytics) if result.analytics is not None else {}
            analytics["peer_grade"] = peer_grades.get(result.instrument_id)
            scored = ScoreResult(
                instrument_id=result.instrument_id,
                model_version=result.model_version,
                family_scores=result.family_scores,
                penalties=result.penalties,
                total_penalty=result.total_penalty,
                raw_total=result.raw_total,
                total_score=result.total_score,
                explanation=result.explanation,
                rewards=result.rewards,
                total_reward=result.total_reward,
                data_completeness=result.data_completeness,
                completeness_tier=result.completeness_tier,
                analytics=analytics,
                sector=result.sector,
                rank=position,
                rank_delta=rank_delta,
            )
            ranked.append(scored)
            _insert_score(conn, scored, run_at)

    logger.info(
        "compute_rankings: persisted %d score rows [model=%s]",
        len(ranked),
        model_version,
    )

    return RankingResult(scored=ranked, model_version=model_version, run_at=run_at)


# ---------------------------------------------------------------------------
# DB persistence
# ---------------------------------------------------------------------------


def _insert_score(
    conn: psycopg.Connection[Any],
    result: ScoreResult,
    scored_at: datetime,
) -> None:
    """
    Insert a single score row. Append-only — never updates prior rows.
    Must be called inside an open transaction.
    """
    # penalties_json carries BOTH penalties and rewards, disambiguated by `kind`
    # (additive to the JSON shape — non-breaking; #1635). Penalties keep
    # `deduction`; rewards use `addition`. total_penalty stays penalties-only.
    penalties_payload: list[dict[str, object]] = [
        {"name": p.name, "deduction": p.deduction, "reason": p.reason, "kind": "penalty"} for p in result.penalties
    ]
    penalties_payload += [
        {"name": r.name, "addition": r.addition, "reason": r.reason, "kind": "reward"} for r in result.rewards
    ]

    conn.execute(
        """
        INSERT INTO scores (
            instrument_id, scored_at,
            quality_score, value_score, turnaround_score,
            momentum_score, sentiment_score, confidence_score,
            raw_total, total_score, model_version,
            penalties_json, explanation,
            rank, rank_delta,
            data_completeness, completeness_tier,
            analytics_json
        )
        VALUES (
            %(instrument_id)s, %(scored_at)s,
            %(quality_score)s, %(value_score)s, %(turnaround_score)s,
            %(momentum_score)s, %(sentiment_score)s, %(confidence_score)s,
            %(raw_total)s, %(total_score)s, %(model_version)s,
            %(penalties_json)s, %(explanation)s,
            %(rank)s, %(rank_delta)s,
            %(data_completeness)s, %(completeness_tier)s,
            %(analytics_json)s
        )
        """,
        {
            "instrument_id": result.instrument_id,
            "scored_at": scored_at,
            "quality_score": result.family_scores.quality,
            "value_score": result.family_scores.value,
            "turnaround_score": result.family_scores.turnaround,
            "momentum_score": result.family_scores.momentum,
            "sentiment_score": result.family_scores.sentiment,
            "confidence_score": result.family_scores.confidence,
            "raw_total": result.raw_total,
            "total_score": result.total_score,
            "model_version": result.model_version,
            "penalties_json": Jsonb(penalties_payload),
            "explanation": result.explanation,
            "rank": result.rank,
            "rank_delta": result.rank_delta,
            "data_completeness": result.data_completeness,
            "completeness_tier": result.completeness_tier,
            "analytics_json": Jsonb(result.analytics) if result.analytics is not None else None,
        },
    )
