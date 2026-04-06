"""
Execution guard.

Responsibilities:
  - Consume a recommendation_id and re-evaluate it against current DB state.
  - Enforce all non-negotiable hard rules before any order can be staged.
  - Write exactly one decision_audit row per invocation, regardless of verdict.
  - Update trade_recommendations.status to 'approved' or 'rejected' atomically
    with the audit write.
  - Never trust stale recommendation state — all constraints are re-checked
    against live DB data.

Rule application by action:

  All actions (BUY, ADD, EXIT, HOLD):
    - kill_switch           — DB-backed runtime flag; missing row = config corrupt
    - auto_trading_disabled — settings.enable_auto_trading must be True
    - live_trading_disabled — settings.enable_live_trading must be True

  BUY / ADD only:
    - coverage_not_tier1    — instrument must have coverage_tier = 1
    - no_coverage_row       — no coverage row exists at all
    - thesis_stale          — latest thesis older than freshness window
    - no_thesis             — no thesis row exists
    - spread_wide           — quotes.spread_flag is TRUE
    - spread_unavailable    — no quotes row or spread_flag is NULL
    - cash_unknown          — cash_ledger is empty (cannot verify affordability)
    - concentration_breach  — post-action sector exposure would exceed 25% AUM

  EXIT:
    - above kill switch / config rules only; thesis, coverage, spread, and
      cash checks are intentionally skipped (do not block a protective exit)

This service produces PASS / FAIL verdicts only.
Nothing is sent to eToro here.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Literal

import psycopg
import psycopg.rows
from psycopg.types.json import Jsonb

from app.config import Settings

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

STAGE: str = "execution_guard"

# Sector cap mirrors portfolio manager policy
_MAX_SECTOR_EXPOSURE_PCT: float = 0.25
_MAX_INITIAL_POSITION_PCT: float = 0.05

# Freshness windows per review_frequency value
_FRESHNESS_DAYS: dict[str, int] = {
    "daily": 1,
    "weekly": 7,
    "monthly": 30,
}

# ---------------------------------------------------------------------------
# Public types
# ---------------------------------------------------------------------------

Verdict = Literal["PASS", "FAIL"]

RuleName = Literal[
    "kill_switch",
    "kill_switch_config_corrupt",
    "auto_trading",
    "live_trading",
    "coverage_not_tier1",
    "no_coverage_row",
    "thesis_stale",
    "no_thesis",
    "spread_wide",
    "spread_unavailable",
    "cash_unknown",
    "instrument_missing",
    "sector_missing",
    "concentration_breach",
]


@dataclass(frozen=True)
class RuleResult:
    rule: RuleName
    passed: bool
    detail: str = ""


@dataclass(frozen=True)
class GuardResult:
    recommendation_id: int
    instrument_id: int
    verdict: Verdict
    failed_rules: list[RuleName]
    explanation: str
    decision_id: int  # PK of the written decision_audit row


# ---------------------------------------------------------------------------
# DB loaders (all read-only; called before any transaction is opened)
# ---------------------------------------------------------------------------


def _load_recommendation(
    conn: psycopg.Connection[Any],
    recommendation_id: int,
) -> dict[str, Any]:
    """
    Load the recommendation row.  Raises ValueError if not found — this is a
    programmer error (the caller passed a non-existent ID).
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT recommendation_id, instrument_id, action, model_version
            FROM trade_recommendations
            WHERE recommendation_id = %(rid)s
            """,
            {"rid": recommendation_id},
        )
        row = cur.fetchone()
    if row is None:
        raise ValueError(f"evaluate_recommendation: recommendation_id={recommendation_id} not found")
    return dict(row)


def _load_kill_switch(conn: psycopg.Connection[Any]) -> dict[str, Any] | None:
    """
    Load the kill_switch row.  Returns None if the row is missing (treated as
    configuration corruption by the caller — fail closed).
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute("SELECT is_active, activated_at, reason FROM kill_switch ORDER BY id DESC LIMIT 1")
        row = cur.fetchone()
    return dict(row) if row is not None else None


def _load_coverage(
    conn: psycopg.Connection[Any],
    instrument_id: int,
) -> dict[str, Any] | None:
    # coverage.instrument_id is PRIMARY KEY — at most one row per instrument.
    # ORDER BY instrument_id LIMIT 1 is stated for consistency with the
    # fetchone invariant; the PK guarantees determinism regardless.
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT coverage_tier, review_frequency
            FROM coverage
            WHERE instrument_id = %(iid)s
            ORDER BY instrument_id
            LIMIT 1
            """,
            {"iid": instrument_id},
        )
        row = cur.fetchone()
    return dict(row) if row is not None else None


def _load_latest_thesis(
    conn: psycopg.Connection[Any],
    instrument_id: int,
) -> dict[str, Any] | None:
    """Return the most recent thesis row or None."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT created_at
            FROM theses
            WHERE instrument_id = %(iid)s
            ORDER BY created_at DESC
            LIMIT 1
            """,
            {"iid": instrument_id},
        )
        row = cur.fetchone()
    return dict(row) if row is not None else None


def _load_quote(
    conn: psycopg.Connection[Any],
    instrument_id: int,
) -> dict[str, Any] | None:
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT spread_flag
            FROM quotes
            WHERE instrument_id = %(iid)s
            ORDER BY quoted_at DESC
            LIMIT 1
            """,
            {"iid": instrument_id},
        )
        row = cur.fetchone()
    return dict(row) if row is not None else None


def _load_cash(conn: psycopg.Connection[Any]) -> float | None:
    """Return current cash balance, or None if the ledger is empty."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute("SELECT SUM(amount) AS balance FROM cash_ledger")
        row = cur.fetchone()
    if row is None or row["balance"] is None:
        return None
    return float(row["balance"])


def _load_sector_exposure(
    conn: psycopg.Connection[Any],
    instrument_id: int,
) -> tuple[bool, str | None, float, float]:
    """
    Return (instrument_found, sector, current_sector_pct, total_aum).

    instrument_found is False when the instrument has no row in instruments —
    the caller must treat this as a hard failure, not a pass.
    current_sector_pct is the fraction of AUM currently in the same sector as
    instrument_id (excluding the instrument itself, since it is unowned for BUY).
    total_aum = SUM(position mark-to-market) + cash.  Returns 0.0 when unknown.
    """
    # Instrument sector
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            "SELECT sector FROM instruments WHERE instrument_id = %(iid)s",
            {"iid": instrument_id},
        )
        row = cur.fetchone()
    if row is None:
        return False, None, 0.0, 0.0
    sector: str | None = row["sector"]

    # Portfolio mark-to-market
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT
                i.sector,
                SUM(
                    COALESCE(q.last, p.cost_basis / NULLIF(p.current_units, 0)) * p.current_units
                ) AS market_value
            FROM positions p
            JOIN instruments i ON i.instrument_id = p.instrument_id
            LEFT JOIN LATERAL (
                SELECT last FROM quotes
                WHERE instrument_id = p.instrument_id
                ORDER BY quoted_at DESC
                LIMIT 1
            ) q ON TRUE
            WHERE p.current_units > 0
            GROUP BY i.sector
            """,
        )
        rows = cur.fetchall()

    sector_values: dict[str | None, float] = {}
    total_positions: float = 0.0
    for r in rows:
        mv = float(r["market_value"]) if r["market_value"] is not None else 0.0
        sector_values[r["sector"]] = mv
        total_positions += mv

    # Cash
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute("SELECT SUM(amount) AS balance FROM cash_ledger")
        row = cur.fetchone()
    cash = float(row["balance"]) if row is not None and row["balance"] is not None else 0.0

    total_aum = total_positions + cash
    current_sector_pct = (sector_values.get(sector, 0.0) / total_aum) if total_aum > 0 else 0.0

    return True, sector, current_sector_pct, total_aum


# ---------------------------------------------------------------------------
# Rule evaluators
# ---------------------------------------------------------------------------


def _check_kill_switch(ks_row: dict[str, Any] | None) -> RuleResult:
    if ks_row is None:
        return RuleResult(
            rule="kill_switch_config_corrupt",
            passed=False,
            detail="kill_switch row missing — configuration corrupt",
        )
    if ks_row["is_active"]:
        activated_at = ks_row.get("activated_at")
        reason = ks_row.get("reason") or ""
        detail = "kill switch active"
        if activated_at:
            detail += f" since {activated_at}"
        if reason:
            detail += f"; reason: {reason}"
        return RuleResult(rule="kill_switch", passed=False, detail=detail)
    return RuleResult(rule="kill_switch", passed=True)


def _check_auto_trading(enabled: bool) -> RuleResult:
    if not enabled:
        return RuleResult(
            rule="auto_trading",
            passed=False,
            detail="settings.enable_auto_trading is False",
        )
    return RuleResult(rule="auto_trading", passed=True)


def _check_live_trading(enabled: bool) -> RuleResult:
    if not enabled:
        return RuleResult(
            rule="live_trading",
            passed=False,
            detail="settings.enable_live_trading is False",
        )
    return RuleResult(rule="live_trading", passed=True)


def _check_coverage(coverage: dict[str, Any] | None) -> RuleResult:
    if coverage is None:
        return RuleResult(
            rule="no_coverage_row",
            passed=False,
            detail="no coverage row for instrument",
        )
    tier = coverage.get("coverage_tier")
    if tier != 1:
        return RuleResult(
            rule="coverage_not_tier1",
            passed=False,
            detail=f"coverage_tier={tier}",
        )
    return RuleResult(rule="coverage_not_tier1", passed=True)


def _check_thesis_freshness(
    thesis: dict[str, Any] | None,
    coverage: dict[str, Any] | None,
    now: datetime,
) -> RuleResult:
    if thesis is None:
        return RuleResult(rule="no_thesis", passed=False, detail="no thesis row found")

    review_frequency: str | None = coverage.get("review_frequency") if coverage else None
    max_age_days = _FRESHNESS_DAYS.get(review_frequency or "", None)  # type: ignore[arg-type]
    if max_age_days is None:
        # Unknown frequency — treat as stale (conservative)
        return RuleResult(
            rule="thesis_stale",
            passed=False,
            detail=f"review_frequency={review_frequency!r} unknown; treating as stale",
        )

    created_at: datetime = thesis["created_at"]
    # Ensure timezone-aware comparison
    if created_at.tzinfo is None:
        created_at = created_at.replace(tzinfo=UTC)
    age = now - created_at
    if age > timedelta(days=max_age_days):
        return RuleResult(
            rule="thesis_stale",
            passed=False,
            detail=f"thesis age={age.days}d > max={max_age_days}d (frequency={review_frequency!r})",
        )
    return RuleResult(rule="thesis_stale", passed=True)


def _check_spread(quote: dict[str, Any] | None) -> RuleResult:
    if quote is None:
        return RuleResult(
            rule="spread_unavailable",
            passed=False,
            detail="no quotes row for instrument",
        )
    spread_flag = quote.get("spread_flag")
    if spread_flag is None:
        return RuleResult(
            rule="spread_unavailable",
            passed=False,
            detail="quotes.spread_flag is NULL",
        )
    if spread_flag:
        return RuleResult(rule="spread_wide", passed=False, detail="spread_flag=TRUE")
    return RuleResult(rule="spread_wide", passed=True)


def _check_cash(cash: float | None) -> RuleResult:
    if cash is None:
        return RuleResult(
            rule="cash_unknown",
            passed=False,
            detail="cash_ledger is empty; cannot verify affordability",
        )
    if cash <= 0:
        return RuleResult(
            rule="cash_unknown",
            passed=False,
            detail=f"cash_balance={cash}; no buying power",
        )
    return RuleResult(rule="cash_unknown", passed=True)


def _check_concentration(
    instrument_found: bool,
    sector: str | None,
    current_sector_pct: float,
    total_aum: float,
) -> RuleResult:
    if not instrument_found:
        return RuleResult(
            rule="instrument_missing",
            passed=False,
            detail="instrument_id not found in instruments table",
        )
    if sector is None:
        return RuleResult(
            rule="sector_missing",
            passed=False,
            detail="instruments.sector is NULL; cannot verify concentration",
        )
    if total_aum <= 0:
        return RuleResult(rule="concentration_breach", passed=True)
    post_action_pct = current_sector_pct + _MAX_INITIAL_POSITION_PCT
    if post_action_pct > _MAX_SECTOR_EXPOSURE_PCT:
        return RuleResult(
            rule="concentration_breach",
            passed=False,
            detail=(
                f"sector={sector!r}: current={current_sector_pct:.1%} "
                f"+ alloc={_MAX_INITIAL_POSITION_PCT:.0%} "
                f"= {post_action_pct:.1%} > max={_MAX_SECTOR_EXPOSURE_PCT:.0%}"
            ),
        )
    return RuleResult(rule="concentration_breach", passed=True)


# ---------------------------------------------------------------------------
# Explanation builder
# ---------------------------------------------------------------------------


def _build_explanation(results: Sequence[RuleResult]) -> str:
    failed = [r for r in results if not r.passed]
    if not failed:
        return "All rules passed"
    parts = [f"{r.rule}: {r.detail}" if r.detail else r.rule for r in failed]
    return "FAIL — " + "; ".join(parts)


# ---------------------------------------------------------------------------
# Audit writer
# ---------------------------------------------------------------------------


def _write_audit(
    conn: psycopg.Connection[Any],
    recommendation_id: int,
    instrument_id: int,
    model_version: str | None,
    verdict: Verdict,
    explanation: str,
    rule_results: Sequence[RuleResult],
    now: datetime,
) -> int:
    """
    Write one decision_audit row and update trade_recommendations.status.
    Both writes are inside a single transaction.
    Returns the decision_id of the inserted audit row.
    """
    evidence = [{"rule": r.rule, "passed": r.passed, "detail": r.detail} for r in rule_results]
    status = "approved" if verdict == "PASS" else "rejected"

    with conn.transaction():
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                """
                INSERT INTO decision_audit
                    (decision_time, instrument_id, recommendation_id, stage,
                     model_version, pass_fail, explanation, evidence_json)
                VALUES
                    (%(dt)s, %(iid)s, %(rid)s, %(stage)s,
                     %(mv)s, %(pf)s, %(expl)s, %(ev)s)
                RETURNING decision_id
                """,
                {
                    "dt": now,
                    "iid": instrument_id,
                    "rid": recommendation_id,
                    "stage": STAGE,
                    "mv": model_version,
                    "pf": verdict,
                    "expl": explanation,
                    "ev": Jsonb(evidence),
                },
            )
            audit_row = cur.fetchone()

        conn.execute(
            """
            UPDATE trade_recommendations
            SET status = %(status)s
            WHERE recommendation_id = %(rid)s
            """,
            {"status": status, "rid": recommendation_id},
        )

    if audit_row is None:  # pragma: no cover — INSERT … RETURNING always returns a row
        raise RuntimeError("decision_audit INSERT returned no row — this should never happen")
    return int(audit_row["decision_id"])


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def _utcnow() -> datetime:
    return datetime.now(tz=UTC)


def evaluate_recommendation(
    conn: psycopg.Connection[Any],
    recommendation_id: int,
    settings: Settings,
) -> GuardResult:
    """
    Evaluate a trade recommendation against all hard rules and write one
    decision_audit row.

    Raises ValueError if recommendation_id is not found (programmer error).
    Never raises for rule failures — those produce a FAIL verdict.

    Steps:
      1. Load the recommendation row.
      2. Load all required state from DB (kill switch, coverage, thesis,
         quote, cash, sector exposure).
      3. Evaluate rules in order; collect results.
      4. Write audit row + update recommendation status atomically.
      5. Return GuardResult.

    No external I/O is performed inside any DB transaction.
    """
    now = _utcnow()

    # --- Step 1: load recommendation (raises if missing) ---
    rec = _load_recommendation(conn, recommendation_id)
    instrument_id: int = int(rec["instrument_id"])
    action: str = str(rec["action"])
    model_version: str | None = rec.get("model_version")

    # --- Step 2: load all state (no transaction open yet) ---
    # Always load kill switch (applies to every action).
    ks_row = _load_kill_switch(conn)

    # BUY / ADD require additional state; EXIT intentionally skips these checks
    # (do not block a protective exit on stale thesis, off-tier coverage, etc.).
    coverage: dict[str, Any] | None = None
    thesis: dict[str, Any] | None = None
    quote: dict[str, Any] | None = None
    cash: float | None = None
    instrument_found: bool = True
    sector: str | None = None
    current_sector_pct: float = 0.0
    total_aum: float = 0.0

    if action in ("BUY", "ADD"):
        coverage = _load_coverage(conn, instrument_id)
        thesis = _load_latest_thesis(conn, instrument_id)
        quote = _load_quote(conn, instrument_id)
        cash = _load_cash(conn)
        instrument_found, sector, current_sector_pct, total_aum = _load_sector_exposure(conn, instrument_id)

    # --- Step 3: evaluate rules ---
    rule_results: list[RuleResult] = []

    # Rules that apply to every action
    rule_results.append(_check_kill_switch(ks_row))
    rule_results.append(_check_auto_trading(settings.enable_auto_trading))
    rule_results.append(_check_live_trading(settings.enable_live_trading))

    # Rules that apply to BUY / ADD only
    if action in ("BUY", "ADD"):
        coverage_result = _check_coverage(coverage)
        rule_results.append(coverage_result)

        # Skip thesis freshness when coverage is absent — review_frequency is
        # unknowable without a coverage row, and no_coverage_row is already a
        # FAIL.  Emitting thesis_stale alongside it would be misleading noise
        # in the audit trail.
        if coverage is not None:
            rule_results.append(_check_thesis_freshness(thesis, coverage, now))
        rule_results.append(_check_spread(quote))
        rule_results.append(_check_cash(cash))
        rule_results.append(_check_concentration(instrument_found, sector, current_sector_pct, total_aum))

    # --- Step 4: derive verdict ---
    failed = [r for r in rule_results if not r.passed]
    verdict: Verdict = "FAIL" if failed else "PASS"
    failed_rule_names: list[RuleName] = [r.rule for r in failed]
    explanation = _build_explanation(rule_results)

    logger.info(
        "execution_guard: recommendation_id=%d instrument_id=%d action=%s verdict=%s failed=%s",
        recommendation_id,
        instrument_id,
        action,
        verdict,
        failed_rule_names or "none",
    )

    # --- Step 5: write audit (always, regardless of verdict) ---
    decision_id = _write_audit(
        conn,
        recommendation_id=recommendation_id,
        instrument_id=instrument_id,
        model_version=model_version,
        verdict=verdict,
        explanation=explanation,
        rule_results=rule_results,
        now=now,
    )

    return GuardResult(
        recommendation_id=recommendation_id,
        instrument_id=instrument_id,
        verdict=verdict,
        failed_rules=failed_rule_names,
        explanation=explanation,
        decision_id=decision_id,
    )
