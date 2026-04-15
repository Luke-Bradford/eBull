"""Entry timing service — TA-informed entry condition evaluation.

Sits between the portfolio manager (which decides WHAT to trade) and
the execution guard (which checks hard rules).  For BUY/ADD
recommendations, evaluates whether current TA conditions support
entering now vs. deferring to the next cycle.

EXIT recommendations always pass through — the timing layer must
never add gates that block protective exits (settled decision).

Key design choices:
  - NULL TA indicators are neutral/pass — never block on missing data.
  - ATR-based stop-loss with a 5% floor to prevent negative/too-tight SL.
  - Take-profit from thesis base_value, guarded against stale/below-entry values.
  - Pure service module: reads DB, returns a result. No side effects.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Literal

import psycopg
import psycopg.rows

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# ATR multiplier for stop-loss: entry_price - ATR_SL_MULTIPLIER * ATR(14)
ATR_SL_MULTIPLIER = Decimal("2.0")

# Floor: SL cannot be more than this fraction below entry price.
# Prevents negative/absurdly wide SL on volatile low-price stocks.
SL_FLOOR_PCT = Decimal("0.05")  # 5% below entry

# Minimum SL distance: SL must be at least this fraction below entry.
# Prevents being stopped out by normal spread noise on low-volatility stocks.
SL_MIN_DISTANCE_PCT = Decimal("0.02")  # 2% below entry

# RSI threshold: above this = overbought, defer entry.
RSI_OVERBOUGHT = 75.0

# Bollinger band proximity: if price is within this fraction of the upper
# band range (upper - lower), consider overextended and defer.
BB_OVEREXTENDED_PCT = 0.95

# Verdicts returned by evaluate_entry_conditions.  The DB CHECK
# constraint also permits 'error' (written by the scheduler for
# error-deferred recs), but the service itself never produces it.
TimingVerdict = Literal["pass", "defer", "skip"]


# ---------------------------------------------------------------------------
# Public types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class EntryEvaluation:
    """Result of evaluating entry timing conditions for a recommendation."""

    verdict: TimingVerdict
    stop_loss_rate: Decimal | None
    take_profit_rate: Decimal | None
    rationale: str
    condition_details: dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# DB loaders
# ---------------------------------------------------------------------------


def _load_recommendation_with_thesis(
    conn: psycopg.Connection[Any],
    recommendation_id: int,
) -> dict[str, Any] | None:
    """Load recommendation joined with the latest thesis for the instrument.

    Returns None if the recommendation doesn't exist.
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT tr.recommendation_id, tr.instrument_id, tr.action,
                   tr.target_entry, tr.suggested_size_pct, tr.status,
                   t.base_value, t.buy_zone_low, t.buy_zone_high,
                   t.confidence_score
            FROM trade_recommendations tr
            LEFT JOIN LATERAL (
                SELECT base_value, buy_zone_low, buy_zone_high, confidence_score
                FROM theses
                WHERE instrument_id = tr.instrument_id
                ORDER BY created_at DESC
                LIMIT 1
            ) t ON TRUE
            WHERE tr.recommendation_id = %(rid)s
            """,
            {"rid": recommendation_id},
        )
        return cur.fetchone()


def _load_latest_ta(
    conn: psycopg.Connection[Any],
    instrument_id: int,
) -> dict[str, Any] | None:
    """Load the latest price_daily row with TA indicators.

    Returns None if no TA data exists for this instrument.
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT close, sma_200, rsi_14, macd_histogram,
                   bb_upper, bb_lower, atr_14
            FROM price_daily
            WHERE instrument_id = %(iid)s
              AND close IS NOT NULL
            ORDER BY price_date DESC
            LIMIT 1
            """,
            {"iid": instrument_id},
        )
        return cur.fetchone()


# ---------------------------------------------------------------------------
# Condition evaluators
# ---------------------------------------------------------------------------


def _eval_rsi(rsi_14: float | None) -> tuple[str, bool]:
    """Evaluate RSI condition.

    Returns (description, is_favorable).
    NULL RSI = neutral/pass (never block on missing data).
    """
    if rsi_14 is None:
        return ("rsi: NULL (neutral)", True)
    if rsi_14 > RSI_OVERBOUGHT:
        return (f"rsi: {rsi_14:.1f} > {RSI_OVERBOUGHT} (overbought, defer)", False)
    return (f"rsi: {rsi_14:.1f} (ok)", True)


def _eval_macd(macd_histogram: float | None) -> tuple[str, bool]:
    """Evaluate MACD histogram condition.

    Positive histogram = momentum is favorable.
    Negative = momentum weakening, defer.
    NULL = neutral/pass.
    """
    if macd_histogram is None:
        return ("macd: NULL (neutral)", True)
    if macd_histogram >= 0:
        return (f"macd_hist: {macd_histogram:.4f} >= 0 (favorable)", True)
    return (f"macd_hist: {macd_histogram:.4f} < 0 (weak momentum, defer)", False)


def _eval_bollinger(
    close: float,
    bb_upper: float | None,
    bb_lower: float | None,
) -> tuple[str, bool]:
    """Evaluate Bollinger band position.

    Price near upper band = overextended, defer.
    NULL bands = neutral/pass.
    """
    if bb_upper is None or bb_lower is None:
        return ("bollinger: NULL (neutral)", True)
    band_width = bb_upper - bb_lower
    if band_width <= 0:
        return ("bollinger: zero-width band (neutral)", True)
    # Position within band: 0 = lower, 1 = upper
    position = (close - bb_lower) / band_width
    if position >= BB_OVEREXTENDED_PCT:
        return (f"bollinger: position={position:.2f} >= {BB_OVEREXTENDED_PCT} (overextended, defer)", False)
    return (f"bollinger: position={position:.2f} (ok)", True)


def _eval_trend(close: float, sma_200: float | None) -> tuple[str, bool]:
    """Evaluate trend confirmation via SMA-200.

    Price above SMA-200 = favorable.
    Price below = unfavorable but not a hard defer (informational).
    NULL SMA-200 = neutral/pass.
    """
    if sma_200 is None:
        return ("trend: SMA-200 NULL (neutral)", True)
    if close >= sma_200:
        return (f"trend: close={close:.2f} >= SMA-200={sma_200:.2f} (favorable)", True)
    return (f"trend: close={close:.2f} < SMA-200={sma_200:.2f} (below trend)", True)


# ---------------------------------------------------------------------------
# SL/TP computation
# ---------------------------------------------------------------------------


def _compute_stop_loss(
    entry_price: Decimal,
    atr_14: Decimal | None,
) -> Decimal:
    """Compute ATR-based stop-loss with floor and minimum distance clamps.

    Formula: entry_price - ATR_SL_MULTIPLIER * ATR(14)
    Floor:   max(ATR-derived, entry_price * (1 - SL_FLOOR_PCT))
    Minimum: min(result, entry_price * (1 - SL_MIN_DISTANCE_PCT))

    Units: entry_price is USD, ATR is USD price-range. Result is USD.

    If ATR is NULL, uses the floor (5% below entry) as default.
    """
    floor_sl = entry_price * (Decimal("1") - SL_FLOOR_PCT)
    min_distance_sl = entry_price * (Decimal("1") - SL_MIN_DISTANCE_PCT)

    if atr_14 is None or atr_14 <= 0:
        # No ATR data: use the floor as a reasonable default
        return floor_sl

    # ATR-derived SL
    atr_sl = entry_price - ATR_SL_MULTIPLIER * atr_14

    # Clamp: not below floor (prevents negative/absurdly wide SL)
    sl = max(atr_sl, floor_sl)

    # Clamp: at least SL_MIN_DISTANCE_PCT below entry (prevents too-tight SL)
    sl = min(sl, min_distance_sl)

    return sl


def _compute_take_profit(
    entry_price: Decimal,
    base_value: Decimal | None,
) -> Decimal | None:
    """Compute take-profit from thesis base_value (target price).

    Returns None if base_value is missing or at/below entry price.
    A TP at or below entry would trigger immediately — nonsensical.
    """
    if base_value is None:
        return None
    if base_value <= entry_price:
        # base_value at/below current entry — don't set a TP that
        # would trigger immediately. The portfolio manager already
        # handles valuation-target-achieved as an EXIT condition.
        return None
    return base_value


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def evaluate_entry_conditions(
    conn: psycopg.Connection[Any],
    recommendation_id: int,
) -> EntryEvaluation:
    """Evaluate TA-based entry timing for a single recommendation.

    For BUY/ADD: checks RSI, MACD, Bollinger, trend; computes SL/TP.
    For EXIT: always returns pass (never defers protective exits).
    For HOLD: returns skip (no timing evaluation needed).
    """
    rec = _load_recommendation_with_thesis(conn, recommendation_id)
    if rec is None:
        return EntryEvaluation(
            verdict="skip",
            stop_loss_rate=None,
            take_profit_rate=None,
            rationale=f"recommendation_id={recommendation_id} not found",
        )

    action = str(rec["action"])
    instrument_id = int(rec["instrument_id"])

    # EXIT recs always pass — never defer protective exits (settled decision).
    if action == "EXIT":
        return EntryEvaluation(
            verdict="skip",
            stop_loss_rate=None,
            take_profit_rate=None,
            rationale="EXIT recommendation: timing evaluation skipped (always pass through)",
        )

    # HOLD recs don't need timing evaluation.
    if action == "HOLD":
        return EntryEvaluation(
            verdict="skip",
            stop_loss_rate=None,
            take_profit_rate=None,
            rationale="HOLD recommendation: no timing evaluation needed",
        )

    # --- BUY/ADD: evaluate TA conditions ---
    ta = _load_latest_ta(conn, instrument_id)

    # Extract TA values, guarding every nullable field before float() cast.
    # Prevention log: float(None) crash in nullable columns (#73).
    close_raw = ta["close"] if ta is not None else None
    rsi_raw = ta["rsi_14"] if ta is not None else None
    macd_hist_raw = ta["macd_histogram"] if ta is not None else None
    bb_upper_raw = ta["bb_upper"] if ta is not None else None
    bb_lower_raw = ta["bb_lower"] if ta is not None else None
    sma_200_raw = ta["sma_200"] if ta is not None else None
    atr_raw = ta["atr_14"] if ta is not None else None

    close_f = float(close_raw) if close_raw is not None else None
    rsi_f = float(rsi_raw) if rsi_raw is not None else None
    macd_f = float(macd_hist_raw) if macd_hist_raw is not None else None
    bb_upper_f = float(bb_upper_raw) if bb_upper_raw is not None else None
    bb_lower_f = float(bb_lower_raw) if bb_lower_raw is not None else None
    sma_200_f = float(sma_200_raw) if sma_200_raw is not None else None
    atr_dec = Decimal(str(atr_raw)) if atr_raw is not None else None

    # Evaluate each condition independently.  RSI and MACD handle NULL
    # inputs as neutral.  Bollinger and trend need a close price, so they
    # are neutral when close is NULL.  This preserves the distinction
    # between "no price row" (ta is None, caught above) and "price row
    # with NULL indicators" in the condition log.
    conditions: list[tuple[str, bool]] = []
    conditions.append(_eval_rsi(rsi_f))
    conditions.append(_eval_macd(macd_f))
    if close_f is not None:
        conditions.append(_eval_bollinger(close_f, bb_upper_f, bb_lower_f))
        conditions.append(_eval_trend(close_f, sma_200_f))
    else:
        conditions.append(("bollinger: close NULL (neutral)", True))
        conditions.append(("trend: close NULL (neutral)", True))

    # Count unfavorable conditions
    unfavorable = [desc for desc, ok in conditions if not ok]
    all_details = {f"cond_{i}": desc for i, (desc, _ok) in enumerate(conditions)}

    # Compute SL/TP regardless of verdict (auditable even on defers).
    # entry_price = target_entry from recommendation (buy zone midpoint, USD).
    entry_price_raw = rec["target_entry"]
    entry_price = Decimal(str(entry_price_raw)) if entry_price_raw is not None else None

    base_value_raw = rec["base_value"]
    base_value = Decimal(str(base_value_raw)) if base_value_raw is not None else None

    sl: Decimal | None = None
    tp: Decimal | None = None

    if entry_price is not None and entry_price > 0:
        sl = _compute_stop_loss(entry_price, atr_dec)
        tp = _compute_take_profit(entry_price, base_value)

    # Verdict: defer if any condition is unfavorable.
    # Entry timing is conservative — one red flag is enough to defer.
    if unfavorable:
        rationale_parts = [f"DEFER ({len(unfavorable)} unfavorable): "] + unfavorable
        if sl is not None:
            rationale_parts.append(f"SL={sl:.6f}")
        if tp is not None:
            rationale_parts.append(f"TP={tp:.6f}")
        return EntryEvaluation(
            verdict="defer",
            stop_loss_rate=sl,
            take_profit_rate=tp,
            rationale="; ".join(rationale_parts),
            condition_details=all_details,
        )

    # All conditions favorable — pass.
    rationale_parts = ["PASS (all conditions favorable)"]
    for desc, _ in conditions:
        rationale_parts.append(desc)
    if sl is not None:
        rationale_parts.append(f"SL={sl:.6f}")
    if tp is not None:
        rationale_parts.append(f"TP={tp:.6f}")
    return EntryEvaluation(
        verdict="pass",
        stop_loss_rate=sl,
        take_profit_rate=tp,
        rationale="; ".join(rationale_parts),
        condition_details=all_details,
    )
