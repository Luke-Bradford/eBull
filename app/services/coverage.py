"""Coverage pipeline — consolidated service module.

Per the 2026-04-19 research-tool refocus §1.1 (Chunk 4), this module merges:

- coverage.py — tier promote/demote rules + review_coverage() (Section 1)
- coverage_audit.py — filings_status classifier (Section 2)
- filings_backfill.py — backfill_filings() pager + 8-K gap fill (Section 3)

External import contract: everything previously importable from the three
retired modules is now importable from ``app.services.coverage``.

---

Section 1 — Coverage tier management:

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

Section 2 — filings_status classifier:

Outputs (one of four; ``unknown`` is a pre-audit placeholder written
elsewhere and ``structurally_young`` is assigned by Section 3, not here):

- ``analysable`` — US domestic issuer, 10-K count >= 2 in 3y AND
  10-Q count >= 4 in 18mo.
- ``insufficient`` — has primary SEC CIK but below the bar.
- ``fpi`` — Foreign Private Issuer.
- ``no_primary_sec_cik`` — no primary ``sec``/``cik`` row.

Section 3 — Filings backfill:

Drives every tradable SEC-covered instrument toward a terminal
``coverage.filings_status`` by paging SEC ``submissions.json`` history
plus 8-K gap-fill inside the 365-day window.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from enum import StrEnum
from typing import Any, Literal

import httpx
import psycopg
import psycopg.rows
from psycopg.types.json import Jsonb

from app.providers.filings import FilingNotFound, FilingSearchResult
from app.providers.implementations.sec_edgar import (
    SecFilingsProvider,
    _normalise_submissions_block,
    _zero_pad_cik,
)
from app.services.filings import _upsert_filing, _upsert_filing_event

logger = logging.getLogger(__name__)


# ============================================================================
# Section 1: Tier management (was coverage.py)
# ============================================================================

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
                -- #268 Chunk J: only surface the latest score if the
                -- instrument is currently analysable. A tier-3
                -- instrument whose filings_status regressed to
                -- insufficient / fpi / no_primary_sec_cik must not
                -- get promoted on a pre-regression score.
                SELECT total_score
                FROM scores
                WHERE instrument_id = i.instrument_id
                  AND c.filings_status = 'analysable'
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
        # T3 → T2: total_score >= 0.55, instrument tradable.
        # Thesis is NOT required — deterministic signals (fundamentals,
        # price action) are sufficient for T3→T2.  Thesis kicks in at T2
        # to enable T2→T1 promotion.
        reasons: list[str] = []
        if snap.total_score is None or snap.total_score < PROMOTE_T3_TO_T2_SCORE:
            return None
        if not snap.is_tradable:
            return None
        reasons.append(f"score={snap.total_score:.3f} >= {PROMOTE_T3_TO_T2_SCORE}")
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
            except ValueError, TypeError:
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
        if result.rowcount == -1:
            raise RuntimeError("seed_coverage INSERT INTO coverage: server did not report a command tag (rowcount=-1)")
        seeded = result.rowcount
        logger.info("seed_coverage: seeded %d instruments at Tier 3", seeded)
        return SeedResult(seeded=seeded, already_populated=False)


@dataclass(frozen=True)
class BootstrapResult:
    """Outcome of ``bootstrap_missing_coverage_rows``."""

    bootstrapped: int


def bootstrap_missing_coverage_rows(
    conn: psycopg.Connection[Any],
) -> BootstrapResult:
    """Insert Tier 3 coverage rows for tradable instruments that lack one.

    Unlike ``seed_coverage`` which is a first-run-only bootstrap (no-ops
    if the table is non-empty), this function targets the post-bootstrap
    gap: newly-added tradable instruments that joined the universe
    after the initial seed. It performs a set-difference insert keyed
    on ``NOT EXISTS`` so existing rows are untouched and no tier is
    ever clobbered.

    Called from ``nightly_universe_sync`` after ``seed_coverage`` so
    every tradable instrument has a coverage row going into the
    downstream audit / thesis / scoring passes.

    New rows land with ``filings_status = 'unknown'`` (#268 Chunk G)
    so the weekly coverage audit (Chunk F) picks them up on its next
    run and classifies them into one of the four terminal outputs.
    Without this, the audit would leave them as NULL
    filings_status and the ``null_anomalies`` counter would flag
    every new instrument as a data-integrity warning until the audit
    could catch up.

    Opens its own ``conn.transaction()`` (savepoint when nested) so
    the INSERT is atomic. ``ON CONFLICT DO NOTHING`` is defence-in-
    depth; the ``NOT EXISTS`` predicate already guarantees no
    conflict on non-concurrent callers.
    """
    with conn.transaction():
        result = conn.execute(
            """
            INSERT INTO coverage (instrument_id, coverage_tier, filings_status)
            SELECT i.instrument_id, 3, 'unknown'
            FROM instruments i
            WHERE i.is_tradable = TRUE
              AND NOT EXISTS (
                  SELECT 1 FROM coverage c WHERE c.instrument_id = i.instrument_id
              )
            ON CONFLICT DO NOTHING
            """
        )
        if result.rowcount == -1:
            raise RuntimeError(
                "bootstrap_missing_coverage_rows INSERT INTO coverage: "
                "server did not report a command tag (rowcount=-1)"
            )
        bootstrapped = result.rowcount
        logger.info(
            "bootstrap_missing_coverage_rows: inserted %d missing coverage rows at Tier 3",
            bootstrapped,
        )
        return BootstrapResult(bootstrapped=bootstrapped)


# ============================================================================
# Section 2: filings_status classifier (was coverage_audit.py)
# ============================================================================


@dataclass(frozen=True)
class AuditCounts:
    """Per-instrument row returned by the audit aggregate."""

    instrument_id: int
    ten_k_in_3y: int
    ten_q_in_18m: int
    us_base_or_amend_total: int
    fpi_total: int


@dataclass(frozen=True)
class AuditSummary:
    """Run result for ``audit_all_instruments``.

    ``null_anomalies`` is the count from a post-update query over
    tradable instruments left-joined to ``coverage`` — captures both
    missing coverage rows (Chunk B regression) AND rows whose
    ``filings_status`` remained NULL after the bulk UPDATE. Either is
    a data-integrity bug and is logged at WARNING.
    """

    analysable: int
    insufficient: int
    fpi: int
    no_primary_sec_cik: int
    total_updated: int
    null_anomalies: int


def _classify(agg: AuditCounts | None, has_sec_cik: bool) -> str:
    """Pure classification — windows/counts pre-computed in SQL.

    ``agg`` is ``None`` when the instrument has zero SEC filings in the
    filing_events table. ``has_sec_cik`` comes from the cohort query.
    """
    if not has_sec_cik:
        return "no_primary_sec_cik"

    if agg is None:
        # SEC CIK present but no SEC filings yet — typical for a
        # newly-added cohort member before Chunk E backfills history.
        return "insufficient"

    # FPI check FIRST: SEC CIK, zero US base-or-amend filings, at
    # least one 20-F/40-F/6-K family filing.
    if agg.us_base_or_amend_total == 0 and agg.fpi_total > 0:
        return "fpi"

    # US history bar — base forms only. Amendments do NOT count
    # toward distinct-period depth (a 10-K/A restates the same year,
    # not an additional one).
    if agg.ten_k_in_3y >= 2 and agg.ten_q_in_18m >= 4:
        return "analysable"

    return "insufficient"


def _load_aggregates(conn: psycopg.Connection[Any]) -> dict[int, AuditCounts]:
    """Return per-instrument SEC filing counts for the four dimensions
    the classifier needs.

    Windows computed in SQL via ``COUNT(*) FILTER`` so the Python
    side doesn't re-derive dates. Amendments are counted toward
    ``us_base_or_amend_total`` (for FPI detection) but NOT toward the
    history-depth thresholds ``ten_k_in_3y`` / ``ten_q_in_18m``.

    Only covered SEC CIKs (primary) are joined in, so rows for
    instruments without a primary ``sec``/``cik`` row are naturally
    excluded.
    """
    rows = conn.execute(
        """
        SELECT
            fe.instrument_id,
            COUNT(*) FILTER (
                WHERE fe.filing_type = '10-K'
                  AND fe.filing_date >= (CURRENT_DATE - INTERVAL '3 years')
            ) AS ten_k_in_3y,
            COUNT(*) FILTER (
                WHERE fe.filing_type = '10-Q'
                  AND fe.filing_date >= (CURRENT_DATE - INTERVAL '18 months')
            ) AS ten_q_in_18m,
            COUNT(*) FILTER (
                WHERE fe.filing_type IN ('10-K','10-K/A','10-Q','10-Q/A')
            ) AS us_base_or_amend_total,
            COUNT(*) FILTER (
                WHERE fe.filing_type IN ('20-F','20-F/A','40-F','40-F/A','6-K','6-K/A')
            ) AS fpi_total
        FROM filing_events fe
        JOIN external_identifiers ei
            ON ei.instrument_id = fe.instrument_id
           AND ei.provider = 'sec'
           AND ei.identifier_type = 'cik'
           AND ei.is_primary = TRUE
        WHERE fe.provider = 'sec'
        GROUP BY fe.instrument_id
        """
    ).fetchall()

    return {
        int(r[0]): AuditCounts(
            instrument_id=int(r[0]),
            ten_k_in_3y=int(r[1]),
            ten_q_in_18m=int(r[2]),
            us_base_or_amend_total=int(r[3]),
            fpi_total=int(r[4]),
        )
        for r in rows
    }


def _load_cohort(
    conn: psycopg.Connection[Any],
) -> list[tuple[int, bool]]:
    """Every tradable instrument + whether it has a primary SEC CIK."""
    rows = conn.execute(
        """
        SELECT
            i.instrument_id,
            EXISTS (
                SELECT 1 FROM external_identifiers ei
                WHERE ei.instrument_id = i.instrument_id
                  AND ei.provider = 'sec'
                  AND ei.identifier_type = 'cik'
                  AND ei.is_primary = TRUE
            ) AS has_sec_cik
        FROM instruments i
        WHERE i.is_tradable = TRUE
        ORDER BY i.instrument_id
        """
    ).fetchall()
    return [(int(r[0]), bool(r[1])) for r in rows]


def _count_null_anomalies(conn: psycopg.Connection[Any]) -> int:
    """Tradable instruments missing a coverage row OR with NULL filings_status."""
    row = conn.execute(
        """
        SELECT COUNT(*)
        FROM instruments i
        LEFT JOIN coverage c ON c.instrument_id = i.instrument_id
        WHERE i.is_tradable = TRUE
          AND (c.instrument_id IS NULL OR c.filings_status IS NULL)
        """
    ).fetchone()
    return int(row[0]) if row is not None else 0


def audit_all_instruments(conn: psycopg.Connection[Any]) -> AuditSummary:
    """Full-universe audit. Writes ``filings_status`` for every tradable
    instrument via a single bulk UPDATE. Wrapped in ``conn.transaction()``
    so a mid-flight failure rolls back the whole audit rather than
    leaving the table in a partial state.

    Does NOT touch ``filings_backfill_*`` columns (Chunk E owns those)
    and never assigns ``structurally_young`` (Chunk E owns that too).
    """
    with conn.transaction():
        aggregates = _load_aggregates(conn)
        cohort = _load_cohort(conn)

        classifications: list[tuple[int, str]] = []
        counts = {
            "analysable": 0,
            "insufficient": 0,
            "fpi": 0,
            "no_primary_sec_cik": 0,
        }
        for instrument_id, has_sec_cik in cohort:
            status = _classify(aggregates.get(instrument_id), has_sec_cik)
            classifications.append((instrument_id, status))
            counts[status] += 1

        total_updated = 0
        if classifications:
            instrument_ids = [c[0] for c in classifications]
            statuses = [c[1] for c in classifications]
            # Demote-guard: preserve ``structurally_young`` on an
            # ``insufficient`` classifier output. Chunk E owns that
            # value — the post-backfill signal must survive until
            # either backfill itself demotes (issuer aged out, clean
            # EXHAUSTED run) or promotes (enough base forms now).
            result = conn.execute(
                """
                UPDATE coverage c
                SET filings_status = CASE
                        WHEN c.filings_status = 'structurally_young'
                             AND v.status = 'insufficient'
                        THEN c.filings_status
                        ELSE v.status
                    END,
                    filings_audit_at = NOW()
                FROM unnest(%s::bigint[], %s::text[]) AS v(instrument_id, status)
                WHERE c.instrument_id = v.instrument_id
                """,
                (instrument_ids, statuses),
            )
            if result.rowcount == -1:
                raise RuntimeError("audit_all_instruments UPDATE: server did not report a command tag (rowcount=-1)")
            total_updated = result.rowcount

    # Null-anomaly check runs AFTER the transaction commits so the
    # count reflects durable state. Running it inside the `with` block
    # would count uncommitted rows; on commit failure those counts
    # would be stale. Post-commit makes the check unambiguous.
    null_anomalies = _count_null_anomalies(conn)

    if null_anomalies > 0:
        logger.warning(
            "coverage_audit: %d null_anomalies detected — either tradable "
            "instruments without a coverage row (Chunk B regression) or "
            "coverage rows whose filings_status remained NULL after UPDATE. "
            "Investigate before the next thesis/scoring cycle.",
            null_anomalies,
        )

    logger.info(
        "coverage_audit: analysable=%d insufficient=%d fpi=%d no_primary_sec_cik=%d total_updated=%d",
        counts["analysable"],
        counts["insufficient"],
        counts["fpi"],
        counts["no_primary_sec_cik"],
        total_updated,
    )

    return AuditSummary(
        analysable=counts["analysable"],
        insufficient=counts["insufficient"],
        fpi=counts["fpi"],
        no_primary_sec_cik=counts["no_primary_sec_cik"],
        total_updated=total_updated,
        null_anomalies=null_anomalies,
    )


def audit_instrument(conn: psycopg.Connection[Any], instrument_id: int) -> str:
    """Single-instrument version of ``audit_all_instruments``.

    Returns the classified status. Used by Chunk G's universe-sync
    hook when a new instrument needs an immediate audit pass, and by
    Chunk E's post-backfill re-audit loop.

    Wrapped in a savepoint so the SELECT + UPDATE are atomic even
    when called inside an outer transaction.
    """
    with conn.transaction():
        agg_rows = conn.execute(
            """
            SELECT
                COUNT(*) FILTER (
                    WHERE fe.filing_type = '10-K'
                      AND fe.filing_date >= (CURRENT_DATE - INTERVAL '3 years')
                ),
                COUNT(*) FILTER (
                    WHERE fe.filing_type = '10-Q'
                      AND fe.filing_date >= (CURRENT_DATE - INTERVAL '18 months')
                ),
                COUNT(*) FILTER (
                    WHERE fe.filing_type IN ('10-K','10-K/A','10-Q','10-Q/A')
                ),
                COUNT(*) FILTER (
                    WHERE fe.filing_type IN ('20-F','20-F/A','40-F','40-F/A','6-K','6-K/A')
                )
            FROM filing_events fe
            JOIN external_identifiers ei
                ON ei.instrument_id = fe.instrument_id
               AND ei.provider = 'sec'
               AND ei.identifier_type = 'cik'
               AND ei.is_primary = TRUE
            WHERE fe.provider = 'sec'
              AND fe.instrument_id = %s
            """,
            (instrument_id,),
        ).fetchone()

        has_cik_row = conn.execute(
            """
            SELECT EXISTS (
                SELECT 1 FROM external_identifiers ei
                WHERE ei.instrument_id = %s
                  AND ei.provider = 'sec'
                  AND ei.identifier_type = 'cik'
                  AND ei.is_primary = TRUE
            )
            """,
            (instrument_id,),
        ).fetchone()
        has_sec_cik = bool(has_cik_row[0]) if has_cik_row is not None else False

        agg: AuditCounts | None
        if agg_rows is None or all(v == 0 for v in agg_rows):
            # No SEC filings for this instrument.
            agg = None
        else:
            agg = AuditCounts(
                instrument_id=instrument_id,
                ten_k_in_3y=int(agg_rows[0]),
                ten_q_in_18m=int(agg_rows[1]),
                us_base_or_amend_total=int(agg_rows[2]),
                fpi_total=int(agg_rows[3]),
            )

        status = _classify(agg, has_sec_cik)

        # Demote-guard: preserve ``structurally_young`` on an
        # ``insufficient`` classifier output. See module docstring.
        # Use named params so the guard's two references to ``status``
        # can't silently desynchronise under a future refactor.
        result = conn.execute(
            """
            UPDATE coverage
            SET filings_status = CASE
                    WHEN filings_status = 'structurally_young'
                         AND %(status)s = 'insufficient'
                    THEN filings_status
                    ELSE %(status)s
                END,
                filings_audit_at = NOW()
            WHERE instrument_id = %(instrument_id)s
            RETURNING filings_status
            """,
            {"status": status, "instrument_id": instrument_id},
        )
        row = result.fetchone()
        if row is None:
            # No coverage row for this instrument. Post-#292 this
            # should never happen — universe sync + the weekly backfill
            # together guarantee coverage rows for every tradable
            # instrument. Raise loudly rather than return a status
            # string that was never persisted.
            raise RuntimeError(
                f"audit_instrument: no coverage row for instrument_id={instrument_id}; "
                f"classifier returned {status!r} but UPDATE matched zero rows. "
                f"Check coverage bootstrap (#292) + universe sync wiring."
            )
        # Return what the DB actually holds post-guard, not the raw
        # classifier output — otherwise a preserved young row would
        # report as 'insufficient' to callers (Chunk E's probe path).
        return str(row[0])


def probe_status(conn: psycopg.Connection[Any], instrument_id: int) -> str:
    """Read-only classifier probe (#268 Chunk E).

    Identical aggregate + ``_classify`` logic to ``audit_instrument``,
    but does NOT UPDATE coverage. Backfill uses this inside its
    pagination loop so a later retryable error cannot leave a
    premature ``'analysable'`` in coverage (Chunk E design v3 C1).

    Returns the classifier output (never reads current
    ``filings_status``). Commits after the SELECTs to close the
    implicit transaction per the backfill durability invariant.
    """
    agg_rows = conn.execute(
        """
        SELECT
            COUNT(*) FILTER (
                WHERE fe.filing_type = '10-K'
                  AND fe.filing_date >= (CURRENT_DATE - INTERVAL '3 years')
            ),
            COUNT(*) FILTER (
                WHERE fe.filing_type = '10-Q'
                  AND fe.filing_date >= (CURRENT_DATE - INTERVAL '18 months')
            ),
            COUNT(*) FILTER (
                WHERE fe.filing_type IN ('10-K','10-K/A','10-Q','10-Q/A')
            ),
            COUNT(*) FILTER (
                WHERE fe.filing_type IN ('20-F','20-F/A','40-F','40-F/A','6-K','6-K/A')
            )
        FROM filing_events fe
        JOIN external_identifiers ei
            ON ei.instrument_id = fe.instrument_id
           AND ei.provider = 'sec'
           AND ei.identifier_type = 'cik'
           AND ei.is_primary = TRUE
        WHERE fe.provider = 'sec'
          AND fe.instrument_id = %s
        """,
        (instrument_id,),
    ).fetchone()

    has_cik_row = conn.execute(
        """
        SELECT EXISTS (
            SELECT 1 FROM external_identifiers ei
            WHERE ei.instrument_id = %s
              AND ei.provider = 'sec'
              AND ei.identifier_type = 'cik'
              AND ei.is_primary = TRUE
        )
        """,
        (instrument_id,),
    ).fetchone()
    has_sec_cik = bool(has_cik_row[0]) if has_cik_row is not None else False

    agg: AuditCounts | None
    if agg_rows is None or all(v == 0 for v in agg_rows):
        agg = None
    else:
        agg = AuditCounts(
            instrument_id=instrument_id,
            ten_k_in_3y=int(agg_rows[0]),
            ten_q_in_18m=int(agg_rows[1]),
            us_base_or_amend_total=int(agg_rows[2]),
            fpi_total=int(agg_rows[3]),
        )

    conn.commit()  # close implicit tx per backfill durability invariant.
    return _classify(agg, has_sec_cik)


# ============================================================================
# Section 3: Filings backfill (was filings_backfill.py)
# ============================================================================


# ---------------------------------------------------------------------
# Outcome enum + result dataclass
# ---------------------------------------------------------------------


class BackfillOutcome(StrEnum):
    """Terminal classification for one backfill pass.

    Values are persisted into ``coverage.filings_backfill_reason``.
    See design doc §BackfillOutcome for the semantics of each value.
    """

    COMPLETE_OK = "COMPLETE_OK"
    COMPLETE_FPI = "COMPLETE_FPI"
    STILL_INSUFFICIENT_EXHAUSTED = "STILL_INSUFFICIENT_EXHAUSTED"
    STILL_INSUFFICIENT_STRUCTURALLY_YOUNG = "STILL_INSUFFICIENT_STRUCTURALLY_YOUNG"
    STILL_INSUFFICIENT_HTTP_ERROR = "STILL_INSUFFICIENT_HTTP_ERROR"
    STILL_INSUFFICIENT_PARSE_ERROR = "STILL_INSUFFICIENT_PARSE_ERROR"
    SKIPPED_ATTEMPTS_CAP = "SKIPPED_ATTEMPTS_CAP"
    SKIPPED_BACKOFF_WINDOW = "SKIPPED_BACKOFF_WINDOW"


_RETRYABLE_REASONS: frozenset[str] = frozenset(
    {
        BackfillOutcome.STILL_INSUFFICIENT_HTTP_ERROR.value,
        BackfillOutcome.STILL_INSUFFICIENT_PARSE_ERROR.value,
    }
)


@dataclass(frozen=True)
class BackfillResult:
    instrument_id: int
    outcome: BackfillOutcome
    pages_fetched: int
    filings_upserted: int
    eight_k_gap_filled: int
    final_status: str


# Tunables (module-level for test override).
ATTEMPTS_CAP: int = 3
BACKOFF_DAYS: int = 7
EIGHT_K_WINDOW_DAYS: int = 365


# ---------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------


def _is_structurally_young(conn: psycopg.Connection[Any], instrument_id: int) -> bool:
    """True iff the instrument's earliest SEC filing is strictly
    newer than today - 18 months (calendar-correct via SQL INTERVAL).

    False when no filings exist at all — we can't prove youth
    without an earliest filing, so classify those as EXHAUSTED,
    not YOUNG (design doc v2-H3).

    Step 3 upserts every fetched filing to ``filing_events`` before
    step 5 calls this helper, so the DB query is the authoritative
    union of DB + just-fetched.
    """
    row = conn.execute(
        """
        SELECT MIN(filing_date) > (CURRENT_DATE - INTERVAL '18 months')
        FROM filing_events
        WHERE instrument_id = %s AND provider = 'sec'
        """,
        (instrument_id,),
    ).fetchone()
    conn.commit()  # M1 invariant.
    return bool(row[0]) if row is not None and row[0] is not None else False


# ---------------------------------------------------------------------
# Single coverage-write sink
# ---------------------------------------------------------------------


def _finalise(
    conn: psycopg.Connection[Any],
    instrument_id: int,
    *,
    outcome: BackfillOutcome,
    status: str | None,
    pages_fetched: int = 0,
    filings_upserted: int = 0,
    eight_k_gap_filled: int = 0,
) -> BackfillResult:
    """Single coverage-write path shared by all terminal outcomes.

    attempts delta by outcome:

    - ``COMPLETE_OK`` / ``COMPLETE_FPI``           -> set 0
    - ``HTTP_ERROR`` / ``PARSE_ERROR``             -> += 1
    - ``EXHAUSTED`` / ``STRUCTURALLY_YOUNG``       -> unchanged
    - ``SKIPPED_*``                                 -> no write at all

    ``status`` semantics:

    - ``None`` = preserve current ``filings_status``. Used by
      retryable errors so a correctly-classified
      ``structurally_young`` row is not demoted on transient
      failure (design doc v4-H2).
    - otherwise the UPDATE writes this value into ``filings_status``.

    Commits before the UPDATE (M1 invariant) and after (K.2/K.3
    durability pattern).
    """
    if outcome in (
        BackfillOutcome.SKIPPED_ATTEMPTS_CAP,
        BackfillOutcome.SKIPPED_BACKOFF_WINDOW,
    ):
        # Gating path — no mutation at all.
        return BackfillResult(
            instrument_id=instrument_id,
            outcome=outcome,
            pages_fetched=0,
            filings_upserted=0,
            eight_k_gap_filled=0,
            final_status="",
        )

    # attempts delta is one of three shapes — parameterising the
    # SQL keeps the query a ``LiteralString`` (pyright strict).
    reset_attempts = outcome in (
        BackfillOutcome.COMPLETE_OK,
        BackfillOutcome.COMPLETE_FPI,
    )
    increment_attempts = outcome in (
        BackfillOutcome.STILL_INSUFFICIENT_HTTP_ERROR,
        BackfillOutcome.STILL_INSUFFICIENT_PARSE_ERROR,
    )
    # EXHAUSTED / STRUCTURALLY_YOUNG leave attempts unchanged.

    conn.commit()  # M1 invariant before mutation.
    if status is not None and reset_attempts:
        result = conn.execute(
            """
            UPDATE coverage
            SET filings_status            = %s,
                filings_backfill_attempts = 0,
                filings_backfill_last_at  = NOW(),
                filings_backfill_reason   = %s,
                filings_audit_at          = NOW()
            WHERE instrument_id = %s
            RETURNING filings_status
            """,
            (status, outcome.value, instrument_id),
        )
    elif status is not None and increment_attempts:
        result = conn.execute(
            """
            UPDATE coverage
            SET filings_status            = %s,
                filings_backfill_attempts = filings_backfill_attempts + 1,
                filings_backfill_last_at  = NOW(),
                filings_backfill_reason   = %s,
                filings_audit_at          = NOW()
            WHERE instrument_id = %s
            RETURNING filings_status
            """,
            (status, outcome.value, instrument_id),
        )
    elif status is not None:
        # EXHAUSTED / STRUCTURALLY_YOUNG.
        result = conn.execute(
            """
            UPDATE coverage
            SET filings_status            = %s,
                filings_backfill_last_at  = NOW(),
                filings_backfill_reason   = %s,
                filings_audit_at          = NOW()
            WHERE instrument_id = %s
            RETURNING filings_status
            """,
            (status, outcome.value, instrument_id),
        )
    elif increment_attempts:
        # status=None preservation path for HTTP/PARSE errors
        # (design doc v4-H2 — never demote structurally_young on
        # transient failure).
        result = conn.execute(
            """
            UPDATE coverage
            SET filings_backfill_attempts = filings_backfill_attempts + 1,
                filings_backfill_last_at  = NOW(),
                filings_backfill_reason   = %s
            WHERE instrument_id = %s
            RETURNING filings_status
            """,
            (outcome.value, instrument_id),
        )
    else:
        # status=None and no attempts change — currently unused
        # but keep the branch explicit for future outcomes.
        result = conn.execute(
            """
            UPDATE coverage
            SET filings_backfill_last_at = NOW(),
                filings_backfill_reason  = %s
            WHERE instrument_id = %s
            RETURNING filings_status
            """,
            (outcome.value, instrument_id),
        )
    row = result.fetchone()
    final = str(row[0]) if row is not None and row[0] is not None else ""
    conn.commit()  # K.2/K.3 durability.

    return BackfillResult(
        instrument_id=instrument_id,
        outcome=outcome,
        pages_fetched=pages_fetched,
        filings_upserted=filings_upserted,
        eight_k_gap_filled=eight_k_gap_filled,
        final_status=final,
    )


# ---------------------------------------------------------------------
# Gating
# ---------------------------------------------------------------------


def _check_gating(conn: psycopg.Connection[Any], instrument_id: int) -> BackfillOutcome | None:
    """Return a gating outcome or ``None`` to proceed.

    Cap rule exempts ``structurally_young`` rows (design doc v5-H1)
    so an aged-out young issuer can be demoted to ``insufficient``
    once backfill completes cleanly.
    """
    row = conn.execute(
        """
        SELECT filings_backfill_attempts, filings_backfill_last_at,
               filings_backfill_reason, filings_status
        FROM coverage
        WHERE instrument_id = %s
        """,
        (instrument_id,),
    ).fetchone()
    conn.commit()  # M1 invariant.

    if row is None:
        # Bootstrap invariant violation — raise loudly.
        raise RuntimeError(f"backfill_filings: no coverage row for instrument_id={instrument_id}")

    attempts = int(row[0]) if row[0] is not None else 0
    last_at: datetime | None = row[1]
    last_reason: str | None = row[2]
    filings_status: str | None = row[3]

    if last_at is not None:
        # Backoff check. Use UTC-naive-aware comparison: psycopg3 returns
        # tz-aware datetime; compare against tz-aware now.
        cutoff = datetime.now(last_at.tzinfo) - timedelta(days=BACKOFF_DAYS)
        if last_at > cutoff:
            return BackfillOutcome.SKIPPED_BACKOFF_WINDOW

    if attempts >= ATTEMPTS_CAP and last_reason in _RETRYABLE_REASONS and filings_status != "structurally_young":
        return BackfillOutcome.SKIPPED_ATTEMPTS_CAP

    return None


# ---------------------------------------------------------------------
# Main entry
# ---------------------------------------------------------------------


def backfill_filings(
    conn: psycopg.Connection[Any],
    provider: SecFilingsProvider,
    cik: str,
    instrument_id: int,
) -> BackfillResult:
    """Page SEC submissions history for ``cik`` + reconcile 8-K gaps,
    then write one terminal ``coverage.filings_status`` row.

    See ``docs/superpowers/specs/2026-04-18-chunk-e-filings-backfill-design.md``
    for the full flow + outcome table.
    """
    gated = _check_gating(conn, instrument_id)
    if gated is not None:
        return _finalise(conn, instrument_id, outcome=gated, status=None)

    cik_padded = _zero_pad_cik(cik)

    # Step 2: fetch primary submissions.json.
    try:
        submissions = provider.fetch_submissions(cik_padded)
    except httpx.HTTPError:
        logger.warning(
            "backfill_filings: HTTP error on fetch_submissions cik=%s",
            cik_padded,
            exc_info=True,
        )
        return _finalise(
            conn,
            instrument_id,
            outcome=BackfillOutcome.STILL_INSUFFICIENT_HTTP_ERROR,
            status=None,
        )
    except json.JSONDecodeError, TypeError:
        # `fetch_submissions` only decodes JSON (`resp.json()`) and returns
        # the dict — it does no dict-key access here. `KeyError` would mean
        # a dict-access bug in the provider code that raised during decode;
        # misclassifying that as a retryable PARSE error would hide it behind
        # backoff. Let any `KeyError` propagate so operators see the
        # traceback instead of silent retry (#355).
        logger.warning(
            "backfill_filings: PARSE error on fetch_submissions cik=%s",
            cik_padded,
            exc_info=True,
        )
        return _finalise(
            conn,
            instrument_id,
            outcome=BackfillOutcome.STILL_INSUFFICIENT_PARSE_ERROR,
            status=None,
        )

    if submissions is None:
        # 404 — CIK valid in external_identifiers but SEC has no
        # submissions for it. Classify retryable.
        return _finalise(
            conn,
            instrument_id,
            outcome=BackfillOutcome.STILL_INSUFFICIENT_HTTP_ERROR,
            status=None,
        )

    window_cutoff = date.today() - timedelta(days=EIGHT_K_WINDOW_DAYS)
    pages_fetched = 0
    filings_upserted = 0
    eight_k_gap_filled = 0
    seen_filings: list[FilingSearchResult] = []
    bar_met = False
    eight_k_window_covered = False

    # Phase A: inline `recent` block.
    try:
        filings_outer = submissions["filings"]
        if not isinstance(filings_outer, dict):
            raise TypeError("filings block not a dict")
        recent_block = filings_outer["recent"]
        if not isinstance(recent_block, dict):
            raise TypeError("recent block not a dict")
        recent_results = _normalise_submissions_block(recent_block, cik_padded)
    except KeyError, TypeError, ValueError, AttributeError:
        logger.warning(
            "backfill_filings: PARSE error on recent block cik=%s",
            cik_padded,
            exc_info=True,
        )
        return _finalise(
            conn,
            instrument_id,
            outcome=BackfillOutcome.STILL_INSUFFICIENT_PARSE_ERROR,
            status=None,
        )

    conn.commit()  # M1 invariant before mutation block.
    with conn.transaction():
        for r in recent_results:
            _upsert_filing(conn, str(instrument_id), "sec", r)
    seen_filings.extend(recent_results)
    pages_fetched += 1
    filings_upserted += len(recent_results)

    bar_met = probe_status(conn, instrument_id) in ("analysable", "fpi")
    if recent_results:
        oldest_recent = min(r.filed_at.date() for r in recent_results)
        if oldest_recent <= window_cutoff:
            eight_k_window_covered = True

    # Phase B: files[] pagination.
    files_meta = filings_outer.get("files") or []
    if not isinstance(files_meta, list):
        files_meta = []

    def _entry_filing_to(e: object) -> date:
        """Per-entry key resolver. Returns ``date.min`` for entries
        whose ``filingTo`` is missing/malformed so a single bad entry
        sinks to the back of the sort rather than aborting the whole
        ordering (pre-v4 fallback silently demoted all pages to
        oldest-first, wasting HTTP budget on unnecessary old pages).
        """
        if not isinstance(e, dict):
            return date.min
        raw = e.get("filingTo")
        try:
            return date.fromisoformat(str(raw))
        except TypeError, ValueError:
            return date.min

    entries = sorted(files_meta, key=_entry_filing_to, reverse=True)

    for entry in entries:
        if bar_met and eight_k_window_covered:
            break  # nothing further to fetch (design doc v4-H1)

        entry_name = entry.get("name") if isinstance(entry, dict) else None
        if not entry_name:
            continue

        try:
            page_raw = provider.fetch_submissions_page(str(entry_name))
        except httpx.HTTPError:
            logger.warning(
                "backfill_filings: HTTP error on page cik=%s name=%s",
                cik_padded,
                entry_name,
                exc_info=True,
            )
            return _finalise(
                conn,
                instrument_id,
                outcome=BackfillOutcome.STILL_INSUFFICIENT_HTTP_ERROR,
                status=None,
                pages_fetched=pages_fetched,
                filings_upserted=filings_upserted,
            )
        except json.JSONDecodeError, TypeError:
            # `fetch_submissions_page` calls `resp.json()` internally — JSON
            # decode + basic type coercion are the only failure modes. A
            # `KeyError` at this scope would signal a dict-access bug inside
            # the provider, not a malformed payload; let it propagate so
            # operators see the traceback instead of silent retry (#355).
            logger.warning(
                "backfill_filings: PARSE error on page cik=%s name=%s",
                cik_padded,
                entry_name,
                exc_info=True,
            )
            return _finalise(
                conn,
                instrument_id,
                outcome=BackfillOutcome.STILL_INSUFFICIENT_PARSE_ERROR,
                status=None,
                pages_fetched=pages_fetched,
                filings_upserted=filings_upserted,
            )
        if page_raw is None:
            # 404 on a page the primary response claimed exists — data
            # integrity; classify retryable.
            logger.warning(
                "backfill_filings: 404 on page cik=%s name=%s",
                cik_padded,
                entry_name,
            )
            return _finalise(
                conn,
                instrument_id,
                outcome=BackfillOutcome.STILL_INSUFFICIENT_HTTP_ERROR,
                status=None,
                pages_fetched=pages_fetched,
                filings_upserted=filings_upserted,
            )

        try:
            page_results = _normalise_submissions_block(page_raw, cik_padded)
        except KeyError, TypeError, ValueError, AttributeError:
            logger.warning(
                "backfill_filings: PARSE error on page cik=%s name=%s",
                cik_padded,
                entry_name,
                exc_info=True,
            )
            return _finalise(
                conn,
                instrument_id,
                outcome=BackfillOutcome.STILL_INSUFFICIENT_PARSE_ERROR,
                status=None,
                pages_fetched=pages_fetched,
                filings_upserted=filings_upserted,
            )

        conn.commit()  # M1 invariant.
        with conn.transaction():
            for r in page_results:
                _upsert_filing(conn, str(instrument_id), "sec", r)
        seen_filings.extend(page_results)
        pages_fetched += 1
        filings_upserted += len(page_results)

        if not bar_met:
            bar_met = probe_status(conn, instrument_id) in ("analysable", "fpi")

        if page_results:
            page_oldest = min(r.filed_at.date() for r in page_results)
            if page_oldest <= window_cutoff:
                eight_k_window_covered = True

    # Step 4: 8-K gap reconciliation.
    conn.commit()  # M1 invariant.
    db_rows = conn.execute(
        """
        SELECT provider_filing_id
        FROM filing_events
        WHERE instrument_id = %s
          AND provider = 'sec'
          AND filing_type = '8-K'
          AND filing_date >= %s
        """,
        (instrument_id, window_cutoff),
    ).fetchall()
    conn.commit()  # M1 invariant.
    db_eight_ks = {str(r[0]) for r in db_rows}

    fetched_eight_ks = {
        r.provider_filing_id for r in seen_filings if r.filing_type == "8-K" and r.filed_at.date() >= window_cutoff
    }

    for missing_accession in sorted(fetched_eight_ks - db_eight_ks):
        try:
            event = provider.get_filing(missing_accession)
        except FilingNotFound:
            continue  # SEC deleted between pages; skip.
        except httpx.HTTPError:
            logger.warning(
                "backfill_filings: HTTP error on get_filing accession=%s",
                missing_accession,
                exc_info=True,
            )
            return _finalise(
                conn,
                instrument_id,
                outcome=BackfillOutcome.STILL_INSUFFICIENT_HTTP_ERROR,
                status=None,
                pages_fetched=pages_fetched,
                filings_upserted=filings_upserted,
                eight_k_gap_filled=eight_k_gap_filled,
            )

        conn.commit()  # M1 invariant.
        with conn.transaction():
            _upsert_filing_event(conn, instrument_id, "sec", event)
        eight_k_gap_filled += 1

    # Step 5: terminal classification + single coverage write.
    final_status = probe_status(conn, instrument_id)

    if final_status == "analysable":
        return _finalise(
            conn,
            instrument_id,
            outcome=BackfillOutcome.COMPLETE_OK,
            status="analysable",
            pages_fetched=pages_fetched,
            filings_upserted=filings_upserted,
            eight_k_gap_filled=eight_k_gap_filled,
        )
    if final_status == "fpi":
        return _finalise(
            conn,
            instrument_id,
            outcome=BackfillOutcome.COMPLETE_FPI,
            status="fpi",
            pages_fetched=pages_fetched,
            filings_upserted=filings_upserted,
            eight_k_gap_filled=eight_k_gap_filled,
        )
    if final_status == "insufficient":
        if _is_structurally_young(conn, instrument_id):
            return _finalise(
                conn,
                instrument_id,
                outcome=BackfillOutcome.STILL_INSUFFICIENT_STRUCTURALLY_YOUNG,
                status="structurally_young",
                pages_fetched=pages_fetched,
                filings_upserted=filings_upserted,
                eight_k_gap_filled=eight_k_gap_filled,
            )
        return _finalise(
            conn,
            instrument_id,
            outcome=BackfillOutcome.STILL_INSUFFICIENT_EXHAUSTED,
            status="insufficient",
            pages_fetched=pages_fetched,
            filings_upserted=filings_upserted,
            eight_k_gap_filled=eight_k_gap_filled,
        )
    if final_status == "no_primary_sec_cik":
        raise RuntimeError(
            f"backfill_filings: unexpected no_primary_sec_cik for "
            f"instrument_id={instrument_id}; eligibility filter should "
            f"have excluded this row"
        )
    raise RuntimeError(f"backfill_filings: unknown classifier status: {final_status!r}")
