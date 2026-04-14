"""
Portfolio manager.

Responsibilities:
  - Load the latest scored and ranked candidate list for Tier 1 instruments.
  - Load current portfolio state (positions, cash).
  - Evaluate each instrument and produce one of: BUY, ADD, HOLD, EXIT.
  - Enforce position-sizing and sector-concentration rules from trading-policy.md.
  - Persist recommendations to trade_recommendations (append-only, deduped HOLDs).

Action priority (highest wins):
  1. EXIT  — thesis break / severe red flag / valuation target reached
  2. ADD   — held, below full size, conviction improved, no new red flags
  3. BUY   — unowned, passes entry checks, concentration limits clear
  4. HOLD  — default fallback

Policy limits (trading-policy.md):
  - max_active_positions:    20
  - max_initial_position_pct: 5 %  of AUM
  - max_full_position_pct:   10 %  of AUM
  - max_sector_exposure_pct: 25 %  of AUM

AUM basis: mark-to-market (current_units × latest_quote_price).
Fallback when no quote: cost_basis for that position.
Cash: SUM(cash_ledger.amount). Empty ledger → cash unknown; BUY allowed
but rationale notes cash_check_deferred (enforcement owned by execution guard).

This service produces *recommendations only*. Nothing is sent to eToro here.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Literal

import psycopg
import psycopg.rows

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Policy constants — single source of truth for trading-policy.md limits
# ---------------------------------------------------------------------------

MAX_ACTIVE_POSITIONS: int = 20
MAX_INITIAL_POSITION_PCT: float = 0.05  # 5 %
MAX_FULL_POSITION_PCT: float = 0.10  # 10 %
# Sector cap uses strict greater-than (> not >=): landing exactly at 25% is
# permitted; exceeding 25% is not. The ceiling is the maximum post-action exposure.
MAX_SECTOR_EXPOSURE_PCT: float = 0.25  # 25 %

# Minimum total_score for a BUY recommendation
MIN_BUY_SCORE: float = 0.35
# Score threshold for buying without a thesis.  When score exceeds this,
# the deterministic signals alone (fundamentals, momentum, filings) are
# strong enough that a thesis is not required.  This allows the pipeline
# to operate autonomously without AI spend for clear-cut opportunities.
MIN_SCORE_ONLY_BUY: float = 0.55

# Minimum improvement thresholds for ADD conviction check
ADD_MIN_CONFIDENCE_DELTA: float = 0.05
ADD_MIN_SCORE_DELTA: float = 0.05

# Red flag score above this triggers EXIT consideration
EXIT_RED_FLAG_THRESHOLD: float = 0.80

# ---------------------------------------------------------------------------
# Public types
# ---------------------------------------------------------------------------

Action = Literal["BUY", "ADD", "HOLD", "EXIT"]


@dataclass(frozen=True)
class PositionState:
    instrument_id: int
    symbol: str
    sector: str | None
    current_units: float
    cost_basis: float
    market_value: float  # mark-to-market; cost_basis fallback if no quote
    quote_is_fallback: bool  # True when cost_basis was used instead of a live price


@dataclass(frozen=True)
class Recommendation:
    instrument_id: int
    symbol: str
    action: Action
    target_entry: float | None  # midpoint of buy zone, or current price
    suggested_size_pct: float | None  # target % of AUM; None for HOLD/EXIT with no resize
    rationale: str
    score_id: int | None
    model_version: str | None
    cash_balance_known: bool | None


@dataclass
class PortfolioReviewResult:
    recommendations: list[Recommendation]
    run_at: datetime = field(default_factory=lambda: datetime.now(tz=UTC))
    total_aum: float = 0.0
    cash: float | None = None  # None = unknown
    active_positions: int = 0


# ---------------------------------------------------------------------------
# DB loaders
# ---------------------------------------------------------------------------


def _load_cash(conn: psycopg.Connection[Any]) -> float | None:
    """
    Return current cash balance, or None if the ledger is empty (unknown).

    cash_ledger.amount sign convention: positive = inflow, negative = outflow.
    SUM(amount) gives the current balance.
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute("SELECT SUM(amount) AS balance FROM cash_ledger")
        row = cur.fetchone()
    if row is None or row["balance"] is None:
        return None
    return float(row["balance"])


def _load_positions(conn: psycopg.Connection[Any]) -> dict[int, PositionState]:
    """
    Load all open positions with mark-to-market values.

    Uses latest quote price where available; falls back to cost_basis and
    sets quote_is_fallback=True so callers can note this in explanations.
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT
                p.instrument_id,
                i.symbol,
                i.sector,
                p.current_units,
                p.cost_basis,
                q.last                   AS quote_price,
                q.last IS NULL           AS quote_is_fallback
            FROM positions p
            JOIN instruments i ON i.instrument_id = p.instrument_id
            LEFT JOIN LATERAL (
                SELECT last
                FROM quotes
                WHERE instrument_id = p.instrument_id
                  AND last IS NOT NULL
                ORDER BY quoted_at DESC
                LIMIT 1
            ) q ON TRUE
            WHERE p.current_units > 0
            """
        )
        rows = cur.fetchall()

    positions: dict[int, PositionState] = {}
    for r in rows:
        units = float(r["current_units"])
        cost_basis = float(r["cost_basis"])
        quote_price = float(r["quote_price"]) if r["quote_price"] is not None else None
        is_fallback = bool(r["quote_is_fallback"])
        market_value = (units * quote_price) if quote_price is not None else cost_basis
        positions[int(r["instrument_id"])] = PositionState(
            instrument_id=int(r["instrument_id"]),
            symbol=str(r["symbol"]),
            sector=r["sector"],
            current_units=units,
            cost_basis=cost_basis,
            market_value=market_value,
            quote_is_fallback=is_fallback,
        )
    return positions


# ---------------------------------------------------------------------------
# Mirror breakdown types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class MirrorBreakdown:
    """Per-mirror aggregate for the portfolio endpoint.

    All monetary values are in USD (eToro copy-trading native currency).
    Callers convert to display currency.
    """

    mirror_id: int
    parent_username: str
    active: bool
    funded_usd: float  # initial_investment + deposits - withdrawals
    mirror_equity_usd: float  # available_amount + sum(position market values)
    unrealized_pnl_usd: float  # mirror_equity - funded
    position_count: int
    started_copy_date: datetime


def load_mirror_breakdowns(conn: psycopg.Connection[Any]) -> list[MirrorBreakdown]:
    """Return per-mirror equity breakdowns for active mirrors.

    Uses the same MTM pricing hierarchy as ``_load_mirror_equity``:
    quote.last → open_rate fallback.  Returns one row per active mirror.

    Values are in USD — the caller converts to display currency.
    """
    sql = """
        SELECT ct.parent_username,
               m.mirror_id, m.active,
               m.initial_investment, m.deposit_summary,
               m.withdrawal_summary, m.available_amount,
               m.started_copy_date,
               COALESCE(p.mv, 0) AS positions_mv,
               COALESCE(p.pos_count, 0) AS position_count
        FROM copy_mirrors m
        JOIN copy_traders ct USING (parent_cid)
        LEFT JOIN LATERAL (
            SELECT SUM(
                      cmp.amount
                    + (CASE WHEN cmp.is_buy THEN 1 ELSE -1 END)
                      * cmp.units
                      * (COALESCE(q.last, cmp.open_rate) - cmp.open_rate)
                      * cmp.open_conversion_rate
                   ) AS mv,
                   COUNT(*) AS pos_count
            FROM copy_mirror_positions cmp
            LEFT JOIN LATERAL (
                SELECT last
                FROM quotes
                WHERE instrument_id = cmp.instrument_id
                ORDER BY quoted_at DESC
                LIMIT 1
            ) q ON TRUE
            WHERE cmp.mirror_id = m.mirror_id
        ) p ON TRUE
        WHERE m.active
        ORDER BY m.mirror_id
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(sql)
        rows = cur.fetchall()

    breakdowns: list[MirrorBreakdown] = []
    for r in rows:
        available = float(r["available_amount"])
        positions_mv = float(r["positions_mv"])
        mirror_equity = available + positions_mv

        funded = float(r["initial_investment"]) + float(r["deposit_summary"]) - float(r["withdrawal_summary"])

        breakdowns.append(
            MirrorBreakdown(
                mirror_id=r["mirror_id"],
                parent_username=r["parent_username"],
                active=r["active"],
                funded_usd=funded,
                mirror_equity_usd=mirror_equity,
                unrealized_pnl_usd=mirror_equity - funded,
                position_count=int(r["position_count"]),
                started_copy_date=r["started_copy_date"],
            )
        )
    return breakdowns


def _load_mirror_equity(conn: psycopg.Connection[Any]) -> float:
    """Return the summed mirror_equity across all active mirrors.

    Runs the §3.4 mirror-equity CTE against the caller's
    connection and returns a float. The value is `0.0` when
    `copy_mirrors` is empty or every row is `active = FALSE` —
    `COALESCE(SUM(...), 0)` in the SQL turns an empty result set
    into `0.0`, never `NULL`, so the return type is `float` and
    not `float | None`. See spec §6.0 and §6.4 for rationale.

    The value is usually non-negative but is NOT mathematically
    floored at zero: a leveraged position with a large adverse
    MTM delta could push a per-mirror contribution negative, and
    the aggregate could go negative too. Callers sum this
    directly into `total_aum` without assuming positivity.

    This helper does NOT open its own transaction; it reads
    under the caller's scope, matching `_load_cash` /
    `_load_positions`.
    """
    sql = """
        WITH mirror_equity AS (
            SELECT COALESCE(SUM(
                m.available_amount + COALESCE(p.mv, 0)
            ), 0) AS total
            FROM copy_mirrors m
            LEFT JOIN LATERAL (
                SELECT SUM(
                      cmp.amount
                    + (CASE WHEN cmp.is_buy THEN 1 ELSE -1 END)
                      * cmp.units
                      * (COALESCE(q.last, cmp.open_rate) - cmp.open_rate)
                      * cmp.open_conversion_rate
                ) AS mv
                FROM copy_mirror_positions cmp
                LEFT JOIN LATERAL (
                    SELECT last
                    FROM quotes
                    WHERE instrument_id = cmp.instrument_id
                    ORDER BY quoted_at DESC
                    LIMIT 1
                ) q ON TRUE
                WHERE cmp.mirror_id = m.mirror_id
            ) p ON TRUE
            WHERE m.active
        )
        SELECT total FROM mirror_equity
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(sql)
        row = cur.fetchone()
    # COALESCE(SUM(...), 0) guarantees exactly one row with a
    # non-NULL numeric, so row should never be None. Use an
    # explicit RuntimeError (not `assert`) so the guard survives
    # `python -O` — see prevention log entry "`assert` as a
    # runtime guard in service code" (#109).
    if row is None:  # pragma: no cover — driver/CTE invariant violation
        raise RuntimeError("_load_mirror_equity: COALESCE(SUM(...), 0) CTE returned no rows; driver invariant violated")
    return float(row["total"])


def _load_ranked_scores(
    conn: psycopg.Connection[Any],
    model_version: str,
) -> list[dict[str, Any]]:
    """
    Load the most recent score row per instrument for model_version,
    returned in rank order (rank 1 first). Instruments without a rank
    (unscored this run) are excluded.
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT DISTINCT ON (instrument_id)
                instrument_id,
                score_id,
                total_score,
                confidence_score,
                rank,
                model_version,
                scored_at
            FROM scores
            WHERE model_version = %(mv)s
              AND rank IS NOT NULL
            ORDER BY instrument_id, scored_at DESC
            """,
            {"mv": model_version},
        )
        rows: list[dict[str, Any]] = cur.fetchall()
    return sorted(rows, key=lambda r: int(r["rank"]))


def _load_instrument_details(
    conn: psycopg.Connection[Any],
    instrument_ids: list[int],
) -> dict[int, dict[str, Any]]:
    """
    Load per-instrument data needed for action evaluation:
      - symbol, sector
      - latest thesis (stance, confidence_score, buy_zone, base_value, break_conditions_json)
      - previous thesis confidence_score (for ADD conviction delta)
      - previous score total_score for same model_version (for ADD score delta)
      - max recent red_flag_score (90 days)
    """
    if not instrument_ids:
        return {}

    details: dict[int, dict[str, Any]] = {iid: {} for iid in instrument_ids}

    # Symbol and sector
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT instrument_id, symbol, sector
            FROM instruments
            WHERE instrument_id = ANY(%(ids)s)
            """,
            {"ids": instrument_ids},
        )
        for r in cur.fetchall():
            iid = int(r["instrument_id"])
            details[iid]["symbol"] = str(r["symbol"])
            details[iid]["sector"] = r["sector"]

    # Latest thesis
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT DISTINCT ON (instrument_id)
                instrument_id,
                thesis_version,
                stance,
                confidence_score,
                buy_zone_low,
                buy_zone_high,
                base_value,
                break_conditions_json
            FROM theses
            WHERE instrument_id = ANY(%(ids)s)
            ORDER BY instrument_id, created_at DESC
            """,
            {"ids": instrument_ids},
        )
        for r in cur.fetchall():
            iid = int(r["instrument_id"])
            details[iid]["thesis"] = dict(r)

    # Previous thesis (second most recent) — for confidence delta
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT DISTINCT ON (instrument_id)
                instrument_id,
                confidence_score
            FROM theses
            WHERE instrument_id = ANY(%(ids)s)
              AND thesis_version < (
                  SELECT MAX(thesis_version)
                  FROM theses t2
                  WHERE t2.instrument_id = theses.instrument_id
              )
            ORDER BY instrument_id, created_at DESC
            """,
            {"ids": instrument_ids},
        )
        for r in cur.fetchall():
            iid = int(r["instrument_id"])
            details[iid]["prev_thesis_confidence"] = (
                float(r["confidence_score"]) if r["confidence_score"] is not None else None
            )

    # Max recent red_flag_score (last 90 days)
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT instrument_id, MAX(red_flag_score) AS max_red_flag
            FROM filing_events
            WHERE instrument_id = ANY(%(ids)s)
              AND filing_date >= CURRENT_DATE - INTERVAL '90 days'
              AND red_flag_score IS NOT NULL
            GROUP BY instrument_id
            """,
            {"ids": instrument_ids},
        )
        for r in cur.fetchall():
            iid = int(r["instrument_id"])
            details[iid]["max_red_flag"] = float(r["max_red_flag"])

    return details


def _load_prev_scores(
    conn: psycopg.Connection[Any],
    instrument_ids: list[int],
    model_version: str,
    latest_score_ids: dict[int, int],
) -> dict[int, float]:
    """
    Return the previous (second most recent) total_score per instrument
    for the given model_version, excluding the already-loaded latest row.

    Used for the ADD score-delta conviction check.

    Caller must ensure every id in instrument_ids has an entry in latest_score_ids;
    this function asserts that invariant to catch data-inconsistency bugs early.
    """
    if not instrument_ids:
        return {}

    missing = [iid for iid in instrument_ids if iid not in latest_score_ids]
    if missing:
        raise ValueError(
            f"_load_prev_scores: instrument_ids not found in latest_score_ids: {missing}. "
            "Caller must pass only ids that have a corresponding latest score row."
        )

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT DISTINCT ON (instrument_id)
                instrument_id,
                total_score
            FROM scores
            WHERE instrument_id = ANY(%(ids)s)
              AND model_version = %(mv)s
              AND score_id != ALL(%(exclude_ids)s)
            ORDER BY instrument_id, scored_at DESC
            """,
            {
                "ids": instrument_ids,
                "mv": model_version,
                "exclude_ids": list(latest_score_ids.values()),
            },
        )
        rows = cur.fetchall()

    return {int(r["instrument_id"]): float(r["total_score"]) for r in rows if r["total_score"] is not None}


def _load_prior_recommendations(
    conn: psycopg.Connection[Any],
    instrument_ids: list[int],
) -> dict[int, dict[str, Any]]:
    """
    Return the most recent recommendation row per instrument.
    Used to deduplicate redundant HOLD rows.
    """
    if not instrument_ids:
        return {}

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT DISTINCT ON (instrument_id)
                instrument_id,
                action,
                rationale
            FROM trade_recommendations
            WHERE instrument_id = ANY(%(ids)s)
            ORDER BY instrument_id, created_at DESC
            """,
            {"ids": instrument_ids},
        )
        return {int(r["instrument_id"]): dict(r) for r in cur.fetchall()}


# ---------------------------------------------------------------------------
# Action evaluators
# ---------------------------------------------------------------------------


def _sector_pct(
    positions: dict[int, PositionState],
    sector: str | None,
    total_aum: float,
) -> float:
    """Current sector exposure as a fraction of AUM."""
    if total_aum <= 0 or sector is None:
        return 0.0
    return sum(p.market_value for p in positions.values() if p.sector == sector) / total_aum


def _evaluate_exit(
    pos: PositionState,
    details: dict[str, Any],
    current_price: float | None,
) -> tuple[bool, str]:
    """
    Return (should_exit, reason).

    EXIT if any of:
      1. break_conditions present AND max_red_flag >= EXIT_RED_FLAG_THRESHOLD
         (thesis break / severe risk event)
      2. current_price >= thesis.base_value (valuation target achieved)
    """
    thesis = details.get("thesis")
    if thesis is None:
        return False, ""

    # Rule 1 — thesis break / severe red flag
    max_red_flag: float | None = details.get("max_red_flag")
    break_conditions = thesis.get("break_conditions_json")
    if break_conditions and max_red_flag is not None and max_red_flag >= EXIT_RED_FLAG_THRESHOLD:
        return (
            True,
            f"Thesis break triggered: max_red_flag={max_red_flag:.2f} "
            f">= threshold={EXIT_RED_FLAG_THRESHOLD}; break conditions present",
        )

    # Rule 2 — valuation target achieved
    base_value = float(thesis["base_value"]) if thesis.get("base_value") is not None else None
    if base_value is not None and current_price is not None and current_price >= base_value:
        return (
            True,
            f"Valuation target reached: current_price={current_price:.4f} >= base_value={base_value:.4f}",
        )

    return False, ""


def _evaluate_add(
    pos: PositionState,
    details: dict[str, Any],
    latest_score: dict[str, Any],
    prev_score_total: float | None,
    total_aum: float,
    positions: dict[int, PositionState],
) -> tuple[bool, str]:
    """
    Return (should_add, reason).

    ADD requires:
      - position below max_full_position_pct
      - stance == "buy"
      - no thesis break (max_red_flag below threshold)
      - conviction improved: confidence_score delta >= 0.05 OR score delta >= 0.05
      - sector concentration passes after proposed add
    """
    if total_aum <= 0:
        return False, ""

    current_pct = pos.market_value / total_aum
    if current_pct >= MAX_FULL_POSITION_PCT:
        return False, ""

    thesis = details.get("thesis")
    if thesis is None or thesis.get("stance") != "buy":
        return False, ""

    max_red_flag: float | None = details.get("max_red_flag")
    if max_red_flag is not None and max_red_flag >= EXIT_RED_FLAG_THRESHOLD:
        return False, ""

    # Conviction check — need at least one delta to be positive
    latest_confidence = (
        float(latest_score["confidence_score"]) if latest_score.get("confidence_score") is not None else None
    )
    prev_confidence: float | None = details.get("prev_thesis_confidence")
    confidence_improved = (
        latest_confidence is not None
        and prev_confidence is not None
        and (latest_confidence - prev_confidence) >= ADD_MIN_CONFIDENCE_DELTA
    )

    latest_total = float(latest_score["total_score"]) if latest_score.get("total_score") is not None else None
    score_improved = (
        latest_total is not None
        and prev_score_total is not None
        and (latest_total - prev_score_total) >= ADD_MIN_SCORE_DELTA
    )

    if not confidence_improved and not score_improved:
        return False, ""

    # Sector check: would adding breach the cap?
    headroom = MAX_FULL_POSITION_PCT - current_pct
    add_pct = min(headroom, MAX_INITIAL_POSITION_PCT)
    sector_after = _sector_pct(positions, pos.sector, total_aum) + add_pct
    if pos.sector is not None and sector_after > MAX_SECTOR_EXPOSURE_PCT:
        return (
            False,
            f"ADD blocked: sector {pos.sector!r} would reach {sector_after:.1%} > max {MAX_SECTOR_EXPOSURE_PCT:.0%}",
        )

    signals: list[str] = []
    if confidence_improved and score_improved:
        signals.append("confidence and score both improved")
    elif confidence_improved:
        signals.append("confidence improved")
    else:
        signals.append("score improved")

    return (
        True,
        f"Conviction strengthened ({signals[0]}); position at {current_pct:.1%}, room to {MAX_FULL_POSITION_PCT:.0%}",
    )


def _evaluate_buy(
    instrument_id: int,
    symbol: str,
    sector: str | None,
    details: dict[str, Any],
    latest_score: dict[str, Any],
    positions: dict[int, PositionState],
    total_aum: float,
    cash: float | None,
    pending_buy_count: int,
    pending_sector_pct: dict[str, float],
) -> tuple[bool, str]:
    """
    Return (should_buy, reason).

    BUY requires:
      - portfolio (held + already-approved BUYs this run) below max_active_positions
      - total_score >= MIN_BUY_SCORE
      - thesis with stance == "buy", OR total_score >= MIN_SCORE_ONLY_BUY
      - no severe red flags
      - sector concentration passes, accounting for BUYs already approved this run
      - cash sufficient if known; if unknown, note cash_check_deferred

    pending_buy_count: number of BUYs approved so far in this evaluation pass.
    pending_sector_pct: accumulated sector exposure from BUYs approved so far,
        keyed by sector name. Callers must update this after each approval.
    """
    if len(positions) + pending_buy_count >= MAX_ACTIVE_POSITIONS:
        return (
            False,
            f"BUY blocked: portfolio at max_active_positions={MAX_ACTIVE_POSITIONS}",
        )

    total_score = float(latest_score["total_score"]) if latest_score.get("total_score") is not None else 0.0
    if total_score < MIN_BUY_SCORE:
        return False, f"Score {total_score:.3f} below min_buy_score={MIN_BUY_SCORE}"

    thesis = details.get("thesis")
    if thesis is None:
        # Allow BUY on strong deterministic score alone (no AI spend needed).
        # Below this threshold, thesis validation is required.
        if total_score < MIN_SCORE_ONLY_BUY:
            return False, f"No thesis and score {total_score:.3f} below score-only threshold={MIN_SCORE_ONLY_BUY}"
    elif thesis.get("stance") != "buy":
        return False, f"Thesis stance {thesis.get('stance')!r} is not 'buy'"

    max_red_flag: float | None = details.get("max_red_flag")
    if max_red_flag is not None and max_red_flag >= EXIT_RED_FLAG_THRESHOLD:
        return False, f"Severe red flag: red_flag_score={max_red_flag:.2f}"

    if sector is not None and total_aum > 0:
        # Combine held exposure with exposure from BUYs already approved this run
        held_pct = _sector_pct(positions, sector, total_aum)
        pending_pct = pending_sector_pct.get(sector, 0.0)
        sector_after = held_pct + pending_pct + MAX_INITIAL_POSITION_PCT
        if sector_after > MAX_SECTOR_EXPOSURE_PCT:
            return (
                False,
                f"BUY blocked: sector {sector!r} would reach {sector_after:.1%} > max {MAX_SECTOR_EXPOSURE_PCT:.0%}",
            )

    if cash is not None and total_aum > 0:
        required = MAX_INITIAL_POSITION_PCT * total_aum
        if cash < required:
            return (
                False,
                f"Insufficient cash: {cash:.2f} < required {required:.2f}",
            )

    rank = latest_score.get("rank")
    cash_note = "" if cash is not None else "; cash_check_deferred (ledger empty)"
    thesis_note = "" if thesis is not None else "; score-only entry (no thesis)"
    return (
        True,
        f"Entry candidate: score={total_score:.3f} rank={rank}; "
        f"initial allocation {MAX_INITIAL_POSITION_PCT:.0%} of AUM{cash_note}{thesis_note}",
    )


# ---------------------------------------------------------------------------
# Target entry price
# ---------------------------------------------------------------------------


def _target_entry(thesis: dict[str, Any] | None, current_price: float | None) -> float | None:
    """Buy zone midpoint if available, otherwise current price."""
    if thesis:
        low = float(thesis["buy_zone_low"]) if thesis.get("buy_zone_low") is not None else None
        high = float(thesis["buy_zone_high"]) if thesis.get("buy_zone_high") is not None else None
        if low is not None and high is not None and high > low:
            return (low + high) / 2.0
    return current_price


# ---------------------------------------------------------------------------
# Rationale builders
# ---------------------------------------------------------------------------


def _hold_rationale(latest_score: dict[str, Any] | None, quote_is_fallback: bool) -> str:
    """
    Build the HOLD rationale string.

    Extracted so the test suite can derive the expected string from the same
    format rather than hardcoding it, preventing brittle string-match failures
    when the format changes.
    """
    if latest_score is not None:
        reason = f"No action trigger met; score={float(latest_score['total_score']):.3f} rank={latest_score['rank']}"
    else:
        reason = "Held position; not in current ranked list (no fresh score)"
    if quote_is_fallback:
        reason += "; market value estimated from cost_basis (no live quote)"
    return reason


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------


def _insert_recommendation(
    conn: psycopg.Connection[Any],
    rec: Recommendation,
    run_at: datetime,
) -> None:
    conn.execute(
        """
        INSERT INTO trade_recommendations
            (instrument_id, created_at, action, target_entry,
             suggested_size_pct, rationale, status,
             score_id, model_version, cash_balance_known)
        VALUES
            (%(instrument_id)s, %(created_at)s, %(action)s, %(target_entry)s,
             %(suggested_size_pct)s, %(rationale)s, 'proposed',
             %(score_id)s, %(model_version)s, %(cash_balance_known)s)
        """,
        {
            "instrument_id": rec.instrument_id,
            "created_at": run_at,
            "action": rec.action,
            "target_entry": rec.target_entry,
            "suggested_size_pct": rec.suggested_size_pct,
            "rationale": rec.rationale,
            "score_id": rec.score_id,
            "model_version": rec.model_version,
            "cash_balance_known": rec.cash_balance_known,
        },
    )


def _should_persist_hold(
    instrument_id: int,
    rationale: str,
    prior_recs: dict[int, dict[str, Any]],
) -> bool:
    """
    Return True if this HOLD should be written to the DB.

    Suppress redundant HOLDs: only write when there is no prior row,
    the prior action was not HOLD, or the rationale materially changed.
    """
    prior = prior_recs.get(instrument_id)
    if prior is None:
        return True
    if prior["action"] != "HOLD":
        return True
    # Rationale changed (e.g. instrument fell out of ranking)
    if prior["rationale"] != rationale:
        return True
    return False


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def run_portfolio_review(
    conn: psycopg.Connection[Any],
    model_version: str = "v1-balanced",
) -> PortfolioReviewResult:
    """
    Evaluate the Tier 1 ranked candidate list against current portfolio state
    and produce action recommendations.

    Steps:
      1. Load latest scores (ranked, model_version).
      2. Load positions (mark-to-market) and cash balance.
      3. Load per-instrument thesis, conviction, red-flag data.
      4. Evaluate held instruments: EXIT → ADD → HOLD.
      5. Evaluate unowned ranked candidates: BUY.
      6. Persist actionable recommendations + non-redundant HOLDs atomically.

    Returns PortfolioReviewResult. Does not raise on partial data — instruments
    with missing data are held or skipped with a note in the rationale.
    """
    run_at = datetime.now(tz=UTC)

    # --- Load state ---
    positions = _load_positions(conn)
    cash = _load_cash(conn)
    cash_known = cash is not None

    ranked_scores = _load_ranked_scores(conn, model_version)
    ranked_ids = [int(r["instrument_id"]) for r in ranked_scores]
    score_by_id: dict[int, dict[str, Any]] = {int(r["instrument_id"]): r for r in ranked_scores}
    latest_score_ids: dict[int, int] = {int(r["instrument_id"]): int(r["score_id"]) for r in ranked_scores}

    # All instruments to evaluate: ranked + any held not in ranked list
    all_ids = list(ranked_ids)
    for iid in positions:
        if iid not in score_by_id:
            all_ids.append(iid)

    # Load mirror_equity eagerly so the early-return path below still
    # reports an honest total_aum (§6.3 contract: total_aum MUST include
    # mirror_equity at every call site, even when no recommendations run).
    # Without this hoist, a run with active copy_mirrors but no scores
    # and no positions would return total_aum=0.0 — factually wrong.
    mirror_equity = _load_mirror_equity(conn)

    if not all_ids:
        logger.info(
            "run_portfolio_review: no ranked candidates and no open positions (mirror_equity=%.2f)",
            mirror_equity,
        )
        return PortfolioReviewResult(
            recommendations=[],
            run_at=run_at,
            total_aum=mirror_equity + (cash if cash_known else 0.0),
            cash=cash,
            active_positions=0,
        )

    details_map = _load_instrument_details(conn, all_ids)

    # Previous scores for ADD delta check (only needed for held instruments in ranked list)
    held_ranked_ids = [iid for iid in ranked_ids if iid in positions]
    prev_scores = _load_prev_scores(conn, held_ranked_ids, model_version, latest_score_ids)

    prior_recs = _load_prior_recommendations(conn, all_ids)

    # AUM — positions + cash + mirror_equity (§6.3).
    # mirror_equity was loaded above (pre-early-return) so the contract
    # holds for both the recommendation path and the no-work path.
    total_market_value = sum(p.market_value for p in positions.values())
    total_aum = total_market_value + (cash if cash_known else 0.0) + mirror_equity

    logger.info(
        "run_portfolio_review: positions=%d cash=%s mirror_equity=%.2f aum=%.2f ranked=%d model=%s",
        len(positions),
        f"{cash:.2f}" if cash_known else "unknown",
        mirror_equity,
        total_aum,
        len(ranked_ids),
        model_version,
    )

    recommendations: list[Recommendation] = []

    # Evaluation order: held instruments (EXIT/ADD/HOLD) BEFORE unowned candidates (BUY).
    # This ordering is load-bearing: the pending_sector_pct accumulator used in BUY
    # evaluation only captures in-flight BUYs, not in-flight ADDs. Because ADDs are
    # evaluated here — before any BUY accumulation begins — the ADD sector check
    # (_sector_pct against held positions only) is always correct. If this order
    # is ever changed so BUYs are evaluated before or alongside ADDs, the ADD sector
    # check must also receive the pending_sector_pct accumulator.

    # --- Evaluate held instruments first (EXIT / ADD / HOLD) ---
    for iid, pos in positions.items():
        details = details_map.get(iid, {})
        latest_score = score_by_id.get(iid)
        score_id = int(latest_score["score_id"]) if latest_score else None
        mv = str(latest_score["model_version"]) if latest_score else None

        # Current price for EXIT valuation check
        current_price = (
            pos.market_value / pos.current_units if pos.current_units > 0 and not pos.quote_is_fallback else None
        )

        # 1. EXIT
        should_exit, exit_reason = _evaluate_exit(pos, details, current_price)
        if should_exit:
            recommendations.append(
                Recommendation(
                    instrument_id=iid,
                    symbol=pos.symbol,
                    action="EXIT",
                    target_entry=current_price,
                    suggested_size_pct=None,
                    rationale=exit_reason,
                    score_id=score_id,
                    model_version=mv,
                    cash_balance_known=cash_known,
                )
            )
            continue

        # 2. ADD (only if ranked and has a score)
        if latest_score is not None:
            prev_score_total = prev_scores.get(iid)
            should_add, add_reason = _evaluate_add(pos, details, latest_score, prev_score_total, total_aum, positions)
            if should_add:
                current_pct = pos.market_value / total_aum if total_aum > 0 else 0.0
                add_target = min(current_pct + MAX_INITIAL_POSITION_PCT, MAX_FULL_POSITION_PCT)
                thesis = details.get("thesis")
                recommendations.append(
                    Recommendation(
                        instrument_id=iid,
                        symbol=pos.symbol,
                        action="ADD",
                        target_entry=_target_entry(thesis, current_price),
                        suggested_size_pct=add_target,
                        rationale=add_reason,
                        score_id=score_id,
                        model_version=mv,
                        cash_balance_known=cash_known,
                    )
                )
                continue

        # 3. HOLD
        hold_reason = _hold_rationale(latest_score, pos.quote_is_fallback)

        recommendations.append(
            Recommendation(
                instrument_id=iid,
                symbol=pos.symbol,
                action="HOLD",
                target_entry=None,
                suggested_size_pct=None,
                rationale=hold_reason,
                score_id=score_id,
                model_version=mv,
                cash_balance_known=cash_known,
            )
        )

    # --- Evaluate unowned ranked candidates (BUY) ---
    # Accumulators track resource consumption from BUYs approved earlier in
    # this same pass so each candidate is checked against the true post-approval
    # state, not just the held-positions baseline.
    pending_buy_count: int = 0
    pending_sector_pct: dict[str, float] = {}

    for iid in ranked_ids:
        if iid in positions:
            continue  # already evaluated above
        details = details_map.get(iid, {})
        latest_score = score_by_id[iid]
        score_id = int(latest_score["score_id"])
        mv = str(latest_score["model_version"])
        symbol = details.get("symbol", str(iid))
        sector = details.get("sector")

        should_buy, buy_reason = _evaluate_buy(
            iid,
            symbol,
            sector,
            details,
            latest_score,
            positions,
            total_aum,
            cash,
            pending_buy_count,
            pending_sector_pct,
        )
        if should_buy:
            thesis = details.get("thesis")
            recommendations.append(
                Recommendation(
                    instrument_id=iid,
                    symbol=symbol,
                    action="BUY",
                    target_entry=_target_entry(thesis, None),
                    suggested_size_pct=MAX_INITIAL_POSITION_PCT,
                    rationale=buy_reason,
                    score_id=score_id,
                    model_version=mv,
                    cash_balance_known=cash_known,
                )
            )
            # Update accumulators so the next candidate sees the correct state
            pending_buy_count += 1
            if sector is not None and total_aum > 0:
                pending_sector_pct[sector] = pending_sector_pct.get(sector, 0.0) + MAX_INITIAL_POSITION_PCT

    # --- Persist atomically ---
    written = 0
    with conn.transaction():
        for rec in recommendations:
            if rec.action == "HOLD" and not _should_persist_hold(rec.instrument_id, rec.rationale, prior_recs):
                continue
            _insert_recommendation(conn, rec, run_at)
            written += 1

    # Log counts of generated recommendations; written may be less due to HOLD dedup
    counts = {a: sum(1 for r in recommendations if r.action == a) for a in ("BUY", "ADD", "HOLD", "EXIT")}
    logger.info(
        "run_portfolio_review complete: generated=%d written=%d BUY=%d ADD=%d HOLD=%d EXIT=%d",
        len(recommendations),
        written,
        counts["BUY"],
        counts["ADD"],
        counts["HOLD"],
        counts["EXIT"],
    )

    return PortfolioReviewResult(
        recommendations=recommendations,
        run_at=run_at,
        total_aum=total_aum,
        cash=cash,
        active_positions=len(positions),
    )
