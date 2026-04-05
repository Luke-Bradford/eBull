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
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any, Literal

import psycopg
from psycopg.types.json import Jsonb

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Domain literals
# ---------------------------------------------------------------------------

WeightMode = Literal["balanced", "conservative", "speculative"]

_DEFAULT_MODEL_VERSION = "v1-balanced"

# ---------------------------------------------------------------------------
# Weight modes  (must sum to 1.0)
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
class ScoreResult:
    instrument_id: int
    model_version: str
    family_scores: FamilyScores
    penalties: list[PenaltyRecord]
    total_penalty: float
    raw_total: float
    total_score: float
    explanation: str
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
    except (TypeError, ValueError):
        return None


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
) -> tuple[float, list[str]]:
    """
    Thesis valuation upside as the primary value proxy.

    upside_to_base  = (base_value - current_price) / current_price
    downside_to_bear = (current_price - bear_value) / current_price

    Returns (score, missing_components).
    """
    notes: list[str] = []

    if base_value is None:
        notes.append("base_value missing")
    if bear_value is None:
        notes.append("bear_value missing")
    if current_price is None or current_price <= 0:
        notes.append("current_price missing or zero")

    if base_value is None or current_price is None or current_price <= 0:
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


def _momentum_score(
    return_1m: float | None,
    return_3m: float | None,
    return_6m: float | None,
) -> tuple[float, list[str]]:
    """
    Blended return score.  3m return is dominant (50% weight).

    Returns (score, missing_components).
    """
    notes: list[str] = []
    components: list[tuple[float, float]] = []  # (score, weight)

    if return_1m is not None:
        s1m = _clip((return_1m + 0.10) / 0.30)
        components.append((s1m, 0.20))
    else:
        notes.append("return_1m missing")

    if return_3m is not None:
        s3m = _clip((return_3m + 0.15) / 0.45)
        components.append((s3m, 0.50))
    else:
        notes.append("return_3m missing")

    if return_6m is not None:
        s6m = _clip((return_6m + 0.20) / 0.60)
        components.append((s6m, 0.30))
    else:
        notes.append("return_6m missing")

    if not components:
        return 0.5, notes  # no momentum data — neutral-by-absence

    # Re-normalise weights across available components
    total_weight = sum(w for _, w in components)
    score = sum(s * w / total_weight for s, w in components)
    return _clip(score), notes


def _sentiment_score(
    rows: Sequence[tuple[float | None, float | None]],  # [(sentiment_score, importance_score), ...]
) -> tuple[float, list[str]]:
    """
    Importance-weighted mean of signed sentiment scores over the news lookback.

    Signed sentiment is in [-1, 1]; map to [0, 1] via (raw + 1) / 2.

    Returns (score, notes).
    """
    notes: list[str] = []
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

    # Stale thesis
    if thesis_created_at is None:
        penalties.append(
            PenaltyRecord(
                name="stale_thesis",
                deduction=_PENALTY_STALE_THESIS,
                reason="no thesis exists",
            )
        )
    elif (now - thesis_created_at).days > _THESIS_STALE_DAYS:
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

    Returns a flat dict of raw values; callers convert to float as needed.
    All DB access is read-only — no writes.
    """
    # Latest fundamentals snapshot (+ up to 4 prior for trend)
    fund_rows = conn.execute(
        """
        SELECT operating_margin, gross_margin, fcf, net_debt, debt,
               revenue_ttm, shares_outstanding
        FROM fundamentals_snapshot
        WHERE instrument_id = %(id)s
        ORDER BY as_of_date DESC
        LIMIT 5
        """,
        {"id": instrument_id},
    ).fetchall()

    # Latest price features
    price_row = conn.execute(
        """
        SELECT return_1m, return_3m, return_6m, close
        FROM price_daily
        WHERE instrument_id = %(id)s
          AND close IS NOT NULL
        ORDER BY price_date DESC
        LIMIT 1
        """,
        {"id": instrument_id},
    ).fetchone()

    # Current quote (spread flag + last price)
    quote_row = conn.execute(
        """
        SELECT spread_flag, last, bid, ask
        FROM quotes
        WHERE instrument_id = %(id)s
        """,
        {"id": instrument_id},
    ).fetchone()

    # Latest thesis (confidence + valuation bands + created_at)
    thesis_row = conn.execute(
        """
        SELECT confidence_score, base_value, bear_value, created_at
        FROM theses
        WHERE instrument_id = %(id)s
        ORDER BY thesis_version DESC
        LIMIT 1
        """,
        {"id": instrument_id},
    ).fetchone()

    # Recent news sentiment (last 30 days)
    cutoff = now - timedelta(days=_NEWS_LOOKBACK_DAYS)
    news_rows = conn.execute(
        """
        SELECT sentiment_score, importance_score
        FROM news_events
        WHERE instrument_id = %(id)s
          AND event_time >= %(cutoff)s
          AND sentiment_score IS NOT NULL
        ORDER BY event_time DESC
        """,
        {"id": instrument_id, "cutoff": cutoff},
    ).fetchall()

    # Recent red flag scores from filing events (last 90 days)
    rf_cutoff = now - timedelta(days=90)
    rf_row = conn.execute(
        """
        SELECT AVG(red_flag_score)
        FROM filing_events
        WHERE instrument_id = %(id)s
          AND filing_date >= %(cutoff)s
          AND red_flag_score IS NOT NULL
        """,
        {"id": instrument_id, "cutoff": rf_cutoff.date()},
    ).fetchone()

    return {
        "fund_rows": fund_rows,
        "price_row": price_row,
        "quote_row": quote_row,
        "thesis_row": thesis_row,
        "news_rows": news_rows,
        "avg_red_flag_score": _to_float(rf_row[0]) if rf_row else None,
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

    # Fundamentals — latest row
    if fund_rows:
        latest_fund = fund_rows[0]
        operating_margin = _to_float(latest_fund[0])
        gross_margin = _to_float(latest_fund[1])
        fcf = _to_float(latest_fund[2])
        net_debt = _to_float(latest_fund[3])
        debt = _to_float(latest_fund[4])
        shares_latest = _to_float(latest_fund[6])
    else:
        operating_margin = gross_margin = fcf = net_debt = debt = shares_latest = None

    # Shares outstanding prior (use oldest available snapshot for dilution check)
    if len(fund_rows) >= 2:
        shares_prior = _to_float(fund_rows[-1][6])
    else:
        shares_prior = None

    # Fundamentals snapshots for trend (newest-first list of (op_margin, revenue))
    snapshots: list[tuple[float | None, float | None]] = [(_to_float(r[0]), _to_float(r[5])) for r in fund_rows]

    # Price features
    if price_row:
        return_1m = _to_float(price_row[0])
        return_3m = _to_float(price_row[1])
        return_6m = _to_float(price_row[2])
        close_price = _to_float(price_row[3])
    else:
        return_1m = return_3m = return_6m = close_price = None

    # Quote — prefer last price from quote, fall back to close
    if quote_row:
        spread_flag: bool = bool(quote_row[0])
        last_price = _to_float(quote_row[1])
        # Best available price: quote last > quote mid > daily close
        if last_price and last_price > 0:
            current_price: float | None = last_price
        else:
            bid = _to_float(quote_row[2])
            ask = _to_float(quote_row[3])
            if bid and ask and bid > 0 and ask > 0:
                current_price = (bid + ask) / 2.0
            else:
                current_price = close_price
    else:
        spread_flag = False
        current_price = close_price

    # Thesis
    if thesis_row:
        thesis_confidence = _to_float(thesis_row[0])
        base_value = _to_float(thesis_row[1])
        bear_value = _to_float(thesis_row[2])
        thesis_created_at: datetime | None = thesis_row[3]
    else:
        thesis_confidence = base_value = bear_value = None
        thesis_created_at = None

    # News sentiment rows: [(sentiment_score, importance_score), ...]
    sentiment_rows: list[tuple[float | None, float | None]] = [(_to_float(r[0]), _to_float(r[1])) for r in news_rows]

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

    v_score, v_notes = _value_score(base_value, bear_value, current_price)
    if v_notes:
        explanation_parts.append("value: " + "; ".join(v_notes))

    m_score, m_notes = _momentum_score(return_1m, return_3m, return_6m)
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
    total_penalty = sum(p.deduction for p in penalties)

    if penalties:
        penalty_names = ", ".join(p.name for p in penalties)
        explanation_parts.append(f"penalties fired: {penalty_names} (total deduction: {total_penalty:.2f})")

    total_score = _clip(raw_total - total_penalty)

    explanation = "; ".join(explanation_parts) if explanation_parts else "all signals present"

    return ScoreResult(
        instrument_id=instrument_id,
        model_version=model_version,
        family_scores=family,
        penalties=penalties,
        total_penalty=total_penalty,
        raw_total=_clip(raw_total),
        total_score=total_score,
        explanation=explanation,
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

    rows = conn.execute(
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
    ).fetchall()

    return {int(r[0]): int(r[1]) for r in rows if r[1] is not None}


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

    # Eligible instruments
    rows = conn.execute(
        """
        SELECT DISTINCT i.instrument_id
        FROM instruments i
        JOIN coverage c ON c.instrument_id = i.instrument_id
        WHERE i.is_tradable = TRUE
          AND c.coverage_tier = 1
          AND (
              EXISTS (SELECT 1 FROM theses t WHERE t.instrument_id = i.instrument_id)
              OR EXISTS (SELECT 1 FROM fundamentals_snapshot f WHERE f.instrument_id = i.instrument_id)
              OR EXISTS (SELECT 1 FROM price_daily p WHERE p.instrument_id = i.instrument_id)
          )
        ORDER BY i.instrument_id
        """,
    ).fetchall()

    instrument_ids = [int(r[0]) for r in rows]
    if not instrument_ids:
        logger.info("compute_rankings: no eligible Tier 1 instruments found")
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

    # Prior ranks for delta computation
    prior_ranks = _fetch_prior_ranks(conn, [r.instrument_id for r in results], model_version)

    ranked: list[ScoreResult] = []
    for position, result in enumerate(results, start=1):
        prior_rank = prior_ranks.get(result.instrument_id)
        rank_delta = (prior_rank - position) if prior_rank is not None else None
        ranked.append(
            ScoreResult(
                instrument_id=result.instrument_id,
                model_version=result.model_version,
                family_scores=result.family_scores,
                penalties=result.penalties,
                total_penalty=result.total_penalty,
                raw_total=result.raw_total,
                total_score=result.total_score,
                explanation=result.explanation,
                rank=position,
                rank_delta=rank_delta,
            )
        )

    # Persist all rows in a single transaction
    run_at = _utcnow()
    with conn.transaction():
        for r in ranked:
            _insert_score(conn, r, run_at)

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
    penalties_payload = [{"name": p.name, "deduction": p.deduction, "reason": p.reason} for p in result.penalties]

    conn.execute(
        """
        INSERT INTO scores (
            instrument_id, scored_at,
            quality_score, value_score, turnaround_score,
            momentum_score, sentiment_score, confidence_score,
            total_score, model_version,
            penalties_json, explanation,
            rank, rank_delta
        )
        VALUES (
            %(instrument_id)s, %(scored_at)s,
            %(quality_score)s, %(value_score)s, %(turnaround_score)s,
            %(momentum_score)s, %(sentiment_score)s, %(confidence_score)s,
            %(total_score)s, %(model_version)s,
            %(penalties_json)s, %(explanation)s,
            %(rank)s, %(rank_delta)s
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
            "total_score": result.total_score,
            "model_version": result.model_version,
            "penalties_json": Jsonb(penalties_payload),
            "explanation": result.explanation,
            "rank": result.rank,
            "rank_delta": result.rank_delta,
        },
    )
