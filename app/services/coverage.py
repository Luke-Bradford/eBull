"""
Coverage tier management service.

Responsibilities:
  - Evaluate instruments for promotion (T3→T2, T2→T1) and demotion (T1→T2, T2→T3).
  - Enforce Tier 1 hard cap (50).
  - Record all tier changes and blocked promotions in coverage_audit.
  - Provide manual override with rationale.

Tiering is deterministic and rule-based — not AI-decided.

Hysteresis:
  Promotion and demotion score thresholds have a 0.10 gap to prevent flapping:
    - T3→T2 promote at ≥0.55,  T2→T3 demote at <0.45
    - T2→T1 promote at ≥0.70,  T1→T2 demote at <0.60

Tier 1 required data (all must be present):
  - current thesis
  - latest score row
  - latest fundamentals snapshot
  - tradable instrument
  - current quote available

Promotion is one step per review cycle (T3→T2 or T2→T1, never T3→T1).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Literal

import psycopg
import psycopg.rows
from psycopg.types.json import Jsonb

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Tunable thresholds
# ---------------------------------------------------------------------------

# Promotion
PROMOTE_T3_TO_T2_SCORE: float = 0.55
PROMOTE_T2_TO_T1_SCORE: float = 0.70
PROMOTE_T2_TO_T1_CONFIDENCE: float = 0.60
PROMOTE_T2_TO_T1_STANCES: frozenset[str] = frozenset({"buy", "watch"})

# Demotion (hysteresis: 0.10 below promotion thresholds)
DEMOTE_T1_TO_T2_SCORE: float = 0.60
DEMOTE_T2_TO_T3_SCORE: float = 0.45

# Tier 1 hard cap
TIER_1_CAP: int = 50

# Review frequency mapping — duplicated from thesis.py; extract when touched next
_REVIEW_FREQUENCY_DAYS: dict[str, int] = {
    "daily": 1,
    "weekly": 7,
    "monthly": 30,
}

# ---------------------------------------------------------------------------
# Domain types
# ---------------------------------------------------------------------------

ChangeType = Literal["promotion", "demotion", "override", "blocked_promotion"]


@dataclass(frozen=True)
class InstrumentSnapshot:
    """Point-in-time data for a single instrument, used by the tier evaluator."""

    instrument_id: int
    symbol: str
    is_tradable: bool
    current_tier: int
    review_frequency: str | None
    total_score: float | None
    thesis_stance: str | None
    thesis_confidence: float | None
    thesis_created_at: datetime | None
    has_fundamentals: bool
    has_quote: bool
    spread_flag: bool | None


@dataclass(frozen=True)
class TierChange:
    instrument_id: int
    old_tier: int
    new_tier: int
    change_type: ChangeType
    rationale: str
    evidence: dict[str, object]


@dataclass(frozen=True)
class ReviewResult:
    promotions: list[TierChange]
    demotions: list[TierChange]
    blocked: list[TierChange]
    unchanged: int


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _utcnow() -> datetime:
    """Return the current UTC datetime. Extracted for testability."""
    return datetime.now(tz=UTC)


def _thesis_staleness_label(
    thesis_created_at: datetime | None,
    review_frequency: str | None,
    now: datetime,
) -> str | None:
    """
    Return a human-readable staleness label if the thesis is not fresh,
    or None if the thesis is fresh.

    Single source of truth for the ``_REVIEW_FREQUENCY_DAYS`` lookup —
    ``_is_thesis_fresh`` delegates here.
    """
    if thesis_created_at is None:
        return "no thesis"
    days = _REVIEW_FREQUENCY_DAYS.get(review_frequency) if review_frequency is not None else None
    if days is None:
        return f"thesis freshness unknown (review_frequency={review_frequency!r})"
    if now >= thesis_created_at + timedelta(days=days):
        return "thesis stale"
    return None


def _is_thesis_fresh(
    thesis_created_at: datetime | None,
    review_frequency: str | None,
    now: datetime,
) -> bool:
    """
    A thesis is fresh if now < thesis.created_at + interval(review_frequency).
    Returns False if thesis is absent, created_at is None, or frequency is unrecognised.
    """
    return _thesis_staleness_label(thesis_created_at, review_frequency, now) is None


def _has_tier1_required_data(snap: InstrumentSnapshot) -> tuple[bool, list[str]]:
    """
    Check whether all Tier 1 required data is present.
    Returns (passes, list_of_missing_items).
    """
    missing: list[str] = []
    if snap.thesis_created_at is None:
        missing.append("thesis")
    if snap.total_score is None:
        missing.append("score")
    if not snap.has_fundamentals:
        missing.append("fundamentals")
    if not snap.is_tradable:
        missing.append("tradable")
    if not snap.has_quote:
        missing.append("quote")
    return len(missing) == 0, missing


def _build_evidence(snap: InstrumentSnapshot) -> dict[str, object]:
    """Build a serialisable evidence dict from an instrument snapshot."""
    return {
        "symbol": snap.symbol,
        "is_tradable": snap.is_tradable,
        "current_tier": snap.current_tier,
        "total_score": snap.total_score,
        "thesis_stance": snap.thesis_stance,
        "thesis_confidence": snap.thesis_confidence,
        "thesis_created_at": snap.thesis_created_at.isoformat() if snap.thesis_created_at else None,
        "has_fundamentals": snap.has_fundamentals,
        "has_quote": snap.has_quote,
        "spread_flag": snap.spread_flag,
    }


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------


def _load_instruments_for_review(
    conn: psycopg.Connection[Any],
) -> list[InstrumentSnapshot]:
    """
    Load all instruments that have a coverage row, along with their latest
    score, thesis, fundamentals, and quote state.

    Uses LATERAL subqueries to select the latest row from each table without
    fan-out (each subquery returns at most one row). The quotes table is
    keyed on instrument_id (PRIMARY KEY), so a plain LEFT JOIN is safe.
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT
                i.instrument_id,
                i.symbol,
                i.is_tradable,
                c.coverage_tier,
                c.review_frequency,
                ls.total_score,
                lt.stance          AS thesis_stance,
                lt.confidence_score AS thesis_confidence,
                lt.created_at       AS thesis_created_at,
                (lf.as_of_date IS NOT NULL) AS has_fundamentals,
                (lq.instrument_id IS NOT NULL) AS has_quote,
                lq.spread_flag
            FROM instruments i
            JOIN coverage c ON c.instrument_id = i.instrument_id
            LEFT JOIN LATERAL (
                SELECT total_score
                FROM scores
                WHERE instrument_id = i.instrument_id
                ORDER BY scored_at DESC
                LIMIT 1
            ) ls ON TRUE
            LEFT JOIN LATERAL (
                SELECT stance, confidence_score, created_at
                FROM theses
                WHERE instrument_id = i.instrument_id
                ORDER BY thesis_version DESC
                LIMIT 1
            ) lt ON TRUE
            LEFT JOIN LATERAL (
                SELECT as_of_date
                FROM fundamentals_snapshot
                WHERE instrument_id = i.instrument_id
                ORDER BY as_of_date DESC
                LIMIT 1
            ) lf ON TRUE
            LEFT JOIN quotes lq ON lq.instrument_id = i.instrument_id
            ORDER BY i.symbol
            """
        )
        rows = cur.fetchall()

    return [
        InstrumentSnapshot(
            instrument_id=int(r["instrument_id"]),
            symbol=r["symbol"],
            is_tradable=bool(r["is_tradable"]),
            current_tier=int(r["coverage_tier"]),
            review_frequency=r["review_frequency"],
            total_score=float(r["total_score"]) if r["total_score"] is not None else None,
            thesis_stance=r["thesis_stance"],
            thesis_confidence=float(r["thesis_confidence"]) if r["thesis_confidence"] is not None else None,
            thesis_created_at=r["thesis_created_at"],
            has_fundamentals=bool(r["has_fundamentals"]),
            has_quote=bool(r["has_quote"]),
            spread_flag=r["spread_flag"],
        )
        for r in rows
    ]


# ---------------------------------------------------------------------------
# Tier evaluation — pure functions, no DB access
# ---------------------------------------------------------------------------


def _evaluate_promotion(snap: InstrumentSnapshot, now: datetime) -> TierChange | None:
    """
    Evaluate whether an instrument qualifies for promotion.
    Returns a TierChange if promotion is warranted, None otherwise.
    Promotion is one step per cycle (T3→T2 or T2→T1, never T3→T1).
    """
    evidence = _build_evidence(snap)

    if snap.current_tier == 3:
        # T3 → T2: total_score >= 0.55, thesis exists, instrument tradable
        reasons: list[str] = []
        if snap.total_score is None or snap.total_score < PROMOTE_T3_TO_T2_SCORE:
            return None
        if snap.thesis_created_at is None:
            return None
        if not snap.is_tradable:
            return None
        reasons.append(f"score={snap.total_score:.3f} >= {PROMOTE_T3_TO_T2_SCORE}")
        reasons.append("thesis exists")
        reasons.append("instrument tradable")
        return TierChange(
            instrument_id=snap.instrument_id,
            old_tier=3,
            new_tier=2,
            change_type="promotion",
            rationale=f"T3→T2: {'; '.join(reasons)}",
            evidence=evidence,
        )

    if snap.current_tier == 2:
        # T2 → T1: all conditions must pass
        if snap.total_score is None or snap.total_score < PROMOTE_T2_TO_T1_SCORE:
            return None
        if snap.thesis_created_at is None:
            return None
        if snap.thesis_stance not in PROMOTE_T2_TO_T1_STANCES:
            return None
        if snap.thesis_confidence is None or snap.thesis_confidence < PROMOTE_T2_TO_T1_CONFIDENCE:
            return None
        if not _is_thesis_fresh(snap.thesis_created_at, snap.review_frequency, now):
            return None
        # Liquidity: spread_flag must be False (not wide), quote must exist
        if not snap.has_quote:
            return None
        if snap.spread_flag is None or snap.spread_flag:
            return None
        # All Tier 1 required data
        data_ok, missing = _has_tier1_required_data(snap)
        if not data_ok:
            return None

        reasons_t1: list[str] = [
            f"score={snap.total_score:.3f} >= {PROMOTE_T2_TO_T1_SCORE}",
            f"stance={snap.thesis_stance}",
            f"confidence={snap.thesis_confidence:.2f} >= {PROMOTE_T2_TO_T1_CONFIDENCE}",
            "thesis fresh",
            "liquidity passes (spread_flag=False)",
            "all required data present",
        ]
        return TierChange(
            instrument_id=snap.instrument_id,
            old_tier=2,
            new_tier=1,
            change_type="promotion",
            rationale=f"T2→T1: {'; '.join(reasons_t1)}",
            evidence=evidence,
        )

    return None


def _evaluate_demotion(snap: InstrumentSnapshot, now: datetime) -> TierChange | None:
    """
    Evaluate whether an instrument should be demoted.
    Returns a TierChange if demotion is warranted, None otherwise.
    Demotion triggers are OR conditions — any single trigger fires the demotion.
    """
    evidence = _build_evidence(snap)

    if snap.current_tier == 1:
        # T1 → T2: any of these triggers demotion
        triggers: list[str] = []

        if snap.total_score is not None and snap.total_score < DEMOTE_T1_TO_T2_SCORE:
            triggers.append(f"score={snap.total_score:.3f} < {DEMOTE_T1_TO_T2_SCORE}")

        label = _thesis_staleness_label(snap.thesis_created_at, snap.review_frequency, now)
        if label is not None:
            triggers.append(label)

        if snap.thesis_stance == "avoid":
            triggers.append("stance=avoid")

        if snap.has_quote and snap.spread_flag:
            triggers.append("liquidity fails (spread_flag=True)")
        elif not snap.has_quote:
            triggers.append("no quote available")

        data_ok, missing = _has_tier1_required_data(snap)
        if not data_ok:
            triggers.append(f"critical data missing: {', '.join(missing)}")

        if triggers:
            return TierChange(
                instrument_id=snap.instrument_id,
                old_tier=1,
                new_tier=2,
                change_type="demotion",
                rationale=f"T1→T2: {'; '.join(triggers)}",
                evidence=evidence,
            )

    if snap.current_tier == 2:
        # T2 → T3: any of these triggers demotion
        triggers_t3: list[str] = []

        if snap.total_score is not None and snap.total_score < DEMOTE_T2_TO_T3_SCORE:
            triggers_t3.append(f"score={snap.total_score:.3f} < {DEMOTE_T2_TO_T3_SCORE}")

        if snap.thesis_created_at is None:
            triggers_t3.append("no thesis")

        if not snap.is_tradable:
            triggers_t3.append("instrument not tradable")

        # Severe data absence: no fundamentals AND no quote
        if not snap.has_fundamentals and not snap.has_quote:
            triggers_t3.append("missing fundamentals and quote")

        if triggers_t3:
            return TierChange(
                instrument_id=snap.instrument_id,
                old_tier=2,
                new_tier=3,
                change_type="demotion",
                rationale=f"T2→T3: {'; '.join(triggers_t3)}",
                evidence=evidence,
            )

    return None


# ---------------------------------------------------------------------------
# Cap enforcement
# ---------------------------------------------------------------------------


def _enforce_tier1_cap(
    current_tier1_count: int,
    proposed_promotions: list[TierChange],
) -> tuple[list[TierChange], list[TierChange]]:
    """
    Enforce the Tier 1 hard cap. If current T1 count + proposed promotions
    exceeds TIER_1_CAP, block the lowest-ranked candidates.

    Ranking for cap tiebreaking: total_score DESC, confidence DESC,
    thesis freshness (created_at DESC).

    Returns (approved, blocked).
    """
    available_slots = max(0, TIER_1_CAP - current_tier1_count)

    if len(proposed_promotions) <= available_slots:
        return proposed_promotions, []

    # Sort by priority: total_score DESC, confidence DESC, thesis_created_at DESC
    def _sort_key(tc: TierChange) -> tuple[float, float, float]:
        ev = tc.evidence
        raw_score = ev.get("total_score")
        score = float(raw_score) if isinstance(raw_score, (int, float)) else 0.0
        raw_conf = ev.get("thesis_confidence")
        conf = float(raw_conf) if isinstance(raw_conf, (int, float)) else 0.0
        # Use timestamp as float for sorting; more recent = higher priority
        created = ev.get("thesis_created_at")
        if isinstance(created, str):
            try:
                ts = datetime.fromisoformat(created).timestamp()
            except (ValueError, TypeError):
                ts = 0.0
        else:
            ts = 0.0
        return (score, conf, ts)

    ranked = sorted(proposed_promotions, key=_sort_key, reverse=True)

    approved = ranked[:available_slots]
    blocked_changes = [
        TierChange(
            instrument_id=tc.instrument_id,
            old_tier=tc.old_tier,
            new_tier=tc.old_tier,  # tier unchanged
            change_type="blocked_promotion",
            rationale=(
                f"Tier 1 cap ({TIER_1_CAP}) reached; "
                f"ranked below cutoff (score={tc.evidence.get('total_score')}, "
                f"confidence={tc.evidence.get('thesis_confidence')})"
            ),
            evidence=tc.evidence,
        )
        for tc in ranked[available_slots:]
    ]

    return approved, blocked_changes


# ---------------------------------------------------------------------------
# DB writes
# ---------------------------------------------------------------------------


def _apply_tier_change(
    conn: psycopg.Connection[Any],
    change: TierChange,
) -> None:
    """Update the coverage row and insert an audit record for a single tier change."""
    if change.old_tier != change.new_tier:
        conn.execute(
            "UPDATE coverage SET coverage_tier = %(tier)s WHERE instrument_id = %(id)s",
            {"tier": change.new_tier, "id": change.instrument_id},
        )

    conn.execute(
        """
        INSERT INTO coverage_audit
            (instrument_id, old_tier, new_tier, change_type, rationale, evidence_json)
        VALUES
            (%(instrument_id)s, %(old_tier)s, %(new_tier)s,
             %(change_type)s, %(rationale)s, %(evidence_json)s)
        """,
        {
            "instrument_id": change.instrument_id,
            "old_tier": change.old_tier,
            "new_tier": change.new_tier,
            "change_type": change.change_type,
            "rationale": change.rationale,
            "evidence_json": Jsonb(change.evidence),
        },
    )


# ---------------------------------------------------------------------------
# Public entry points
# ---------------------------------------------------------------------------


def review_coverage(
    conn: psycopg.Connection[Any],
) -> ReviewResult:
    """
    Run a deterministic coverage tier review for all instruments.

    Steps:
      1. Load all instruments with coverage data.
      2. Evaluate demotions first (frees T1 slots).
      3. Evaluate promotions.
      4. Enforce Tier 1 cap on proposed T2→T1 promotions.
      5. Apply all changes and record audit trail atomically.

    Returns ReviewResult summarising all changes.
    """
    now = _utcnow()
    snapshots = _load_instruments_for_review(conn)

    if not snapshots:
        logger.info("review_coverage: no instruments with coverage found")
        return ReviewResult(promotions=[], demotions=[], blocked=[], unchanged=0)

    # Phase 1: evaluate demotions
    demotions: list[TierChange] = []
    demoted_ids: set[int] = set()
    for snap in snapshots:
        demotion = _evaluate_demotion(snap, now)
        if demotion is not None:
            demotions.append(demotion)
            demoted_ids.add(snap.instrument_id)

    # Phase 2: evaluate promotions (skip instruments already being demoted)
    t1_promotions: list[TierChange] = []
    other_promotions: list[TierChange] = []
    for snap in snapshots:
        if snap.instrument_id in demoted_ids:
            continue
        promotion = _evaluate_promotion(snap, now)
        if promotion is not None:
            if promotion.new_tier == 1:
                t1_promotions.append(promotion)
            else:
                other_promotions.append(promotion)

    # Phase 3: enforce T1 cap
    # Current T1 count = instruments at T1 minus those being demoted from T1
    current_t1 = sum(1 for s in snapshots if s.current_tier == 1 and s.instrument_id not in demoted_ids)
    approved_t1, blocked = _enforce_tier1_cap(current_t1, t1_promotions)

    all_promotions = other_promotions + approved_t1

    # Phase 4: apply atomically
    with conn.transaction():
        for change in demotions:
            _apply_tier_change(conn, change)
        for change in all_promotions:
            _apply_tier_change(conn, change)
        for change in blocked:
            _apply_tier_change(conn, change)

    # Each instrument falls into exactly one bucket:
    #   promotions + demotions + blocked + unchanged == len(snapshots)
    unchanged = len(snapshots) - len(demotions) - len(all_promotions) - len(blocked)

    logger.info(
        "review_coverage: promotions=%d demotions=%d blocked=%d unchanged=%d",
        len(all_promotions),
        len(demotions),
        len(blocked),
        unchanged,
    )

    return ReviewResult(
        promotions=all_promotions,
        demotions=demotions,
        blocked=blocked,
        unchanged=unchanged,
    )


def override_tier(
    conn: psycopg.Connection[Any],
    instrument_id: int,
    new_tier: int,
    rationale: str,
) -> TierChange:
    """
    Manually override an instrument's coverage tier.

    Opens its own ``conn.transaction()`` block — do not call inside an
    existing transaction. A ``ValueError`` raised during validation will
    roll back the inner transaction (or savepoint) and propagate to the caller.

    Validates:
      - instrument exists and has a coverage row
      - new_tier is 1, 2, or 3
      - rationale is non-empty
      - Tier 1 cap is respected (even for manual overrides)

    Raises ValueError on validation failure.
    Returns the TierChange record.
    """
    if new_tier not in (1, 2, 3):
        raise ValueError(f"new_tier must be 1, 2, or 3; got {new_tier}")

    if not rationale or not rationale.strip():
        raise ValueError("rationale must be non-empty")

    # All reads (old_tier lookup, T1 cap count) and the write are inside
    # a single transaction so the audit record cannot record a stale old_tier
    # and the cap check cannot race with a concurrent override or weekly review.
    with conn.transaction():
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                """
                SELECT c.coverage_tier, i.symbol
                FROM coverage c
                JOIN instruments i ON i.instrument_id = c.instrument_id
                WHERE c.instrument_id = %(id)s
                """,
                {"id": instrument_id},
            )
            row = cur.fetchone()

        if row is None:
            raise ValueError(f"No coverage row for instrument_id={instrument_id}")

        old_tier = int(row["coverage_tier"])
        symbol = row["symbol"]

        if old_tier == new_tier:
            raise ValueError(f"instrument_id={instrument_id} ({symbol}) is already at Tier {new_tier}")

        if new_tier == 1:
            with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
                cur.execute("SELECT COUNT(*) AS cnt FROM coverage WHERE coverage_tier = 1")
                count_row = cur.fetchone()
            current_t1 = int(count_row["cnt"]) if count_row else 0  # type: ignore[index]
            if current_t1 >= TIER_1_CAP:
                raise ValueError(
                    f"Tier 1 cap ({TIER_1_CAP}) reached; current count={current_t1}. Demote another instrument first."
                )

        change = TierChange(
            instrument_id=instrument_id,
            old_tier=old_tier,
            new_tier=new_tier,
            change_type="override",
            rationale=f"Manual override ({symbol}): {rationale.strip()}",
            evidence={
                "symbol": symbol,
                "old_tier": old_tier,
                "new_tier": new_tier,
                "operator_rationale": rationale.strip(),
            },
        )
        _apply_tier_change(conn, change)

    logger.info(
        "override_tier: instrument_id=%d (%s) Tier %d→%d reason=%s",
        instrument_id,
        symbol,
        old_tier,
        new_tier,
        rationale.strip(),
    )

    return change


# ---------------------------------------------------------------------------
# First-run seeding
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SeedResult:
    """Outcome of ``seed_coverage``."""

    seeded: int
    already_populated: bool


def seed_coverage(
    conn: psycopg.Connection[Any],
) -> SeedResult:
    """Seed initial Tier 3 coverage rows for all tradable instruments.

    This is a first-run bootstrap helper: it inserts coverage rows
    only when the coverage table is completely empty.  Once seeded, the
    weekly coverage review promotes instruments through 3→2→1 on its
    normal schedule.

    Uses ``ON CONFLICT DO NOTHING`` so a concurrent call is safe (the
    second caller inserts zero rows rather than racing).  In practice,
    concurrency cannot arise because this is only called from
    ``nightly_universe_sync`` which holds an advisory lock.

    Opens its own ``conn.transaction()`` (a savepoint when nested inside
    a caller-managed transaction) so the read (empty check) and the
    write (bulk INSERT) are atomic.
    """
    with conn.transaction():
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute("SELECT COUNT(*) AS cnt FROM coverage")
            row = cur.fetchone()
            # COUNT(*) always returns exactly one row; the value is 0 when empty.
            count = int(row["cnt"]) if row is not None else 0

        # Note: the COUNT and INSERT run as separate statements within the
        # same savepoint.  A concurrent transaction could theoretically
        # insert rows between them, causing us to return seeded=0 with
        # already_populated=False.  This is cosmetic — the only caller
        # (nightly_universe_sync) holds an advisory lock, so concurrency
        # cannot arise.  ON CONFLICT DO NOTHING is defence-in-depth.
        if count > 0:
            logger.info("seed_coverage: table already has %d rows, skipping", count)
            return SeedResult(seeded=0, already_populated=True)

        result = conn.execute(
            """
            INSERT INTO coverage (instrument_id, coverage_tier)
            SELECT instrument_id, 3
            FROM instruments
            WHERE is_tradable = TRUE
            ON CONFLICT DO NOTHING
            """
        )
        seeded = max(result.rowcount, 0) if result.rowcount is not None else 0
        logger.info("seed_coverage: seeded %d instruments at Tier 3", seeded)
        return SeedResult(seeded=seeded, already_populated=False)
