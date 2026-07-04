"""Scores and rankings API endpoints.

Reads from:
  - scores      (per-instrument per-run score breakdown, rank, rank_delta)
  - instruments  (symbol, company_name, sector for display)
  - coverage     (coverage_tier)
  - theses       (stance — only when stance filter is active)

No writes. No schema changes.

Latest-run semantics:
  ``compute_rankings`` writes all rows for a single run with one coherent
  ``scored_at`` timestamp inside a single transaction.  The rankings
  endpoint identifies the latest run via ``MAX(scored_at)`` for the
  requested ``model_version``.  If no run exists, both endpoints return
  an empty list with ``total=0``.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from datetime import datetime
from typing import Literal

import psycopg
import psycopg.rows
from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel

from app.db import get_conn

# Single source of truth for the default model version (#1633: v1.2-balanced). The
# read endpoints must default to the same version the scoring pass writes, or the
# rankings page shows stale rows of a version no longer produced.
from app.services.scoring import _DEFAULT_MODEL_VERSION
from app.services.sector_classification import resolve_sector_spdr, sector_spdr_case_sql

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/rankings", tags=["rankings"])

MAX_PAGE_LIMIT = 200

Stance = Literal["buy", "hold", "watch", "avoid"]

# Server-side sort allowlist (#1825). The request value is a Literal (FastAPI
# 422s anything off-list) AND only ever used as a dict KEY here — the SQL column
# fragment comes exclusively from this hardcoded map, so the user value never
# reaches the ORDER BY string. `gics_sector` is deliberately absent: the column
# displays a Python-resolved GICS label while the only SQL expression is the
# SPDR-symbol CASE, so a SQL sort would not match the visible order (sector is a
# filter, not a sort).
SortField = Literal[
    "rank",
    "rank_delta",
    "symbol",
    "coverage_tier",
    "total_score",
    "quality_score",
    "value_score",
    "turnaround_score",
    "momentum_score",
    "sentiment_score",
    "confidence_score",
    "data_completeness",
]

_SORT_COLUMNS: dict[str, str] = {
    "rank": "s.rank",
    "rank_delta": "s.rank_delta",
    "symbol": "i.symbol",
    "coverage_tier": "c.coverage_tier",
    "total_score": "s.total_score",
    "quality_score": "s.quality_score",
    "value_score": "s.value_score",
    "turnaround_score": "s.turnaround_score",
    "momentum_score": "s.momentum_score",
    "sentiment_score": "s.sentiment_score",
    "confidence_score": "s.confidence_score",
    "data_completeness": "s.data_completeness",
}

_SORT_DIR: dict[str, str] = {"asc": "ASC", "desc": "DESC"}

# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------


class RankingItem(BaseModel):
    """Single instrument's score from the latest scoring run."""

    instrument_id: int
    symbol: str
    company_name: str
    sector: str | None
    # #1675: real GICS sector resolved on-read from the SEC SIC (same crosswalk
    # as the instrument identity payload). NULL for ETFs / non-filers / unmapped
    # SIC. The opaque ``sector`` 1-9 code above is retained for back-compat.
    gics_sector: str | None
    coverage_tier: int | None
    rank: int | None
    rank_delta: int | None
    total_score: float | None
    raw_total: float | None
    quality_score: float | None
    value_score: float | None
    turnaround_score: float | None
    momentum_score: float | None
    sentiment_score: float | None
    confidence_score: float | None
    # #1825: data-completeness surfaced so a high-ranked thin-coverage name is
    # visibly flagged (on `scores` since #1820).
    data_completeness: float | None
    completeness_tier: str | None
    penalties_json: list[dict[str, object]] | None
    explanation: str | None
    model_version: str
    scored_at: datetime


class RankingsListResponse(BaseModel):
    items: list[RankingItem]
    total: int
    offset: int
    limit: int
    model_version: str
    scored_at: datetime | None


class RankingsCoverageBucket(BaseModel):
    """One rank-exclusion cause with its operator label and instrument count."""

    reason: str
    label: str
    count: int


class RankingsCoverage(BaseModel):
    """Explicit ranked-vs-universe denominator for the Rankings header (#1918).

    ``universe`` = tradable instruments. ``ranked`` = the exact count the
    ``GET /rankings`` list returns unfiltered for this run. ``not_ranked`` is
    a MECE breakdown of ``universe - ranked`` by cause, so an operator can tell
    "not ranked" from "ranked low" instead of reading absence as a bug.
    """

    model_version: str
    scored_at: datetime | None
    universe: int
    ranked: int
    not_ranked: list[RankingsCoverageBucket]


# Operator labels per rank-exclusion reason. The non-`analysable` keys are
# `coverage.filings_status` values whose meaning is the classifier's own rule
# (app/services/coverage.py — probe_status / _finalise / _is_structurally_young,
# coverage.py:1844-1893), not inferred here. `analysable_unranked` and `other`
# are computed buckets. Labels avoid the stronger "US-GAAP" claim: the
# classifier keys on SEC form families/counts, not accounting basis.
_NOT_RANKED_LABELS: dict[str, str] = {
    "no_primary_sec_cik": "No SEC filer (non-US listing)",
    "fpi": "Foreign private issuer (20-F/6-K filer)",
    "insufficient": "Insufficient filing history",
    "structurally_young": "Recently listed — too little history",
    "analysable_unranked": "Analysable — not in latest ranking run",
    "other": "Unclassified coverage",
}

# Emission order for not_ranked buckets: raw filing-status causes first (largest
# classes), computed buckets last.
_NOT_RANKED_ORDER: tuple[str, ...] = (
    "no_primary_sec_cik",
    "fpi",
    "insufficient",
    "structurally_young",
    "analysable_unranked",
    "other",
)


def build_coverage(
    *,
    model_version: str,
    scored_at: datetime | None,
    universe: int,
    ranked: int,
    status_counts: Mapping[str, int],
) -> RankingsCoverage:
    """Assemble the coverage breakdown from raw full-population counts.

    Pure (no DB) so it is table-testable. ``status_counts`` maps
    ``coverage.filings_status`` -> count over the *tradable* universe (a NULL /
    missing status is simply absent from the map — it falls into ``other``).

    Buckets are mutually exclusive; ``other`` is the residual
    (``universe - Σ known statuses``, i.e. tradable rows with a NULL/unknown
    coverage status), so ``ranked + Σ not_ranked == universe`` holds
    definitionally. ``analysable`` splits into ``ranked`` +
    ``analysable_unranked``. A negative computed bucket signals a data anomaly
    (e.g. duplicate score rows making ranked > analysable); it is clamped to 0
    and logged rather than shown as a nonsense negative.
    """
    analysable = status_counts.get("analysable", 0)
    known_status_sum = sum(
        status_counts.get(s, 0)
        for s in ("analysable", "no_primary_sec_cik", "fpi", "insufficient", "structurally_young")
    )

    counts: dict[str, int] = {
        "no_primary_sec_cik": status_counts.get("no_primary_sec_cik", 0),
        "fpi": status_counts.get("fpi", 0),
        "insufficient": status_counts.get("insufficient", 0),
        "structurally_young": status_counts.get("structurally_young", 0),
        "analysable_unranked": analysable - ranked,
        "other": universe - known_status_sum,
    }

    for key in ("analysable_unranked", "other"):
        if counts[key] < 0:
            logger.warning(
                "rankings coverage: negative %s bucket (%d) — clamping to 0; universe=%d ranked=%d status_counts=%s",
                key,
                counts[key],
                universe,
                ranked,
                dict(status_counts),
            )
            counts[key] = 0

    not_ranked = [
        RankingsCoverageBucket(reason=r, label=_NOT_RANKED_LABELS[r], count=counts[r])
        for r in _NOT_RANKED_ORDER
        if counts[r] > 0
    ]
    return RankingsCoverage(
        model_version=model_version,
        scored_at=scored_at,
        universe=universe,
        ranked=ranked,
        not_ranked=not_ranked,
    )


class ScoreHistoryItem(BaseModel):
    """One point in an instrument's score history."""

    scored_at: datetime
    total_score: float | None
    raw_total: float | None
    quality_score: float | None
    value_score: float | None
    turnaround_score: float | None
    momentum_score: float | None
    sentiment_score: float | None
    confidence_score: float | None
    penalties_json: list[dict[str, object]] | None
    explanation: str | None
    rank: int | None
    rank_delta: int | None
    model_version: str


class ScoreHistoryResponse(BaseModel):
    instrument_id: int
    items: list[ScoreHistoryItem]


class VerdictScore(BaseModel):
    scored_at: datetime
    model_version: str
    rank: int | None
    rank_delta: int | None
    total_score: float | None
    raw_total: float | None
    quality_score: float | None
    value_score: float | None
    turnaround_score: float | None
    momentum_score: float | None
    sentiment_score: float | None
    confidence_score: float | None
    data_completeness: float | None
    completeness_tier: str | None
    penalties_json: list[dict[str, object]] | None
    explanation: str | None
    analytics_json: dict[str, object] | None


class VerdictResponse(BaseModel):
    """Latest score row for a single instrument — the per-instrument Verdict
    payload (#1824, P3 of #1815).

    ``score`` is ``None`` when the instrument has never been scored (200 +
    null, mirroring the instrument-404→200+null convention of #1813). When
    present it carries the full headline breakdown plus the Instrument
    Analytical Record (``analytics_json``, #1823) passed through verbatim.

    ``analytics_json`` is typed loosely as a passthrough dict: the ``iar_v1``
    shape is self-describing and every signal is independently nullable/sparse
    (a suppressed F/Z emits only ``{suppressed, reason}``; a standalone
    ``peer_grade`` omits ``peer_key``/``peer_n``). Strict Pydantic modelling
    would hit the validation cliff (#932) and couple the read endpoint to the
    evidence schema; the FE owns the display-side typing. Pre-#1823 score rows
    keep ``analytics_json = null`` — the headline still renders.
    """

    instrument_id: int
    score: VerdictScore | None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_optional_float(row: dict[str, object], key: str) -> float | None:
    """Safely cast a nullable numeric DB column to float."""
    val = row.get(key)
    if val is None:
        return None
    return float(val)  # type: ignore[arg-type]


def _parse_optional_int(row: dict[str, object], key: str) -> int | None:
    """Safely cast a nullable integer DB column to int."""
    val = row.get(key)
    if val is None:
        return None
    return int(val)  # type: ignore[arg-type]


def _parse_ranking_item(row: dict[str, object]) -> RankingItem:
    return RankingItem(
        instrument_id=row["instrument_id"],  # type: ignore[arg-type]
        symbol=row["symbol"],  # type: ignore[arg-type]
        company_name=row["company_name"],  # type: ignore[arg-type]
        sector=row["sector"],  # type: ignore[arg-type]
        gics_sector=(_sc.gics_sector if (_sc := resolve_sector_spdr(row.get("sic"))) is not None else None),  # type: ignore[arg-type]
        coverage_tier=_parse_optional_int(row, "coverage_tier"),
        rank=_parse_optional_int(row, "rank"),
        rank_delta=_parse_optional_int(row, "rank_delta"),
        total_score=_parse_optional_float(row, "total_score"),
        raw_total=_parse_optional_float(row, "raw_total"),
        quality_score=_parse_optional_float(row, "quality_score"),
        value_score=_parse_optional_float(row, "value_score"),
        turnaround_score=_parse_optional_float(row, "turnaround_score"),
        momentum_score=_parse_optional_float(row, "momentum_score"),
        sentiment_score=_parse_optional_float(row, "sentiment_score"),
        confidence_score=_parse_optional_float(row, "confidence_score"),
        data_completeness=_parse_optional_float(row, "data_completeness"),
        completeness_tier=row.get("completeness_tier"),  # type: ignore[arg-type]
        penalties_json=row["penalties_json"],  # type: ignore[arg-type]
        explanation=row["explanation"],  # type: ignore[arg-type]
        model_version=row["model_version"],  # type: ignore[arg-type]
        scored_at=row["scored_at"],  # type: ignore[arg-type]
    )


def _parse_history_item(row: dict[str, object]) -> ScoreHistoryItem:
    return ScoreHistoryItem(
        scored_at=row["scored_at"],  # type: ignore[arg-type]
        total_score=_parse_optional_float(row, "total_score"),
        raw_total=_parse_optional_float(row, "raw_total"),
        quality_score=_parse_optional_float(row, "quality_score"),
        value_score=_parse_optional_float(row, "value_score"),
        turnaround_score=_parse_optional_float(row, "turnaround_score"),
        momentum_score=_parse_optional_float(row, "momentum_score"),
        sentiment_score=_parse_optional_float(row, "sentiment_score"),
        confidence_score=_parse_optional_float(row, "confidence_score"),
        penalties_json=row["penalties_json"],  # type: ignore[arg-type]
        explanation=row["explanation"],  # type: ignore[arg-type]
        rank=_parse_optional_int(row, "rank"),
        rank_delta=_parse_optional_int(row, "rank_delta"),
        model_version=row["model_version"],  # type: ignore[arg-type]
    )


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get("", response_model=RankingsListResponse)
def list_rankings(
    conn: psycopg.Connection[object] = Depends(get_conn),
    model_version: str = Query(default=_DEFAULT_MODEL_VERSION),
    coverage_tier: int | None = Query(default=None, ge=1, le=3),
    sector: str | None = Query(default=None),
    sector_spdr: str | None = Query(default=None),
    stance: Stance | None = Query(default=None),
    q: str | None = Query(default=None),
    min_total_score: float | None = Query(default=None),
    sort: SortField = Query(default="rank"),
    sort_dir: Literal["asc", "desc"] = Query(default="asc"),
    offset: int = Query(default=0, ge=0),
    limit: int = Query(default=50, ge=1, le=MAX_PAGE_LIMIT),
) -> RankingsListResponse:
    """Latest scored and ranked instruments for a given model version.

    Identifies the most recent scoring run by ``MAX(scored_at)`` for the
    requested ``model_version``.  All rows in a single run share the same
    ``scored_at`` timestamp (written atomically by ``compute_rankings``).

    If no scoring run exists, returns an empty list with ``total=0``.

    Filters:
      - coverage_tier: exact match (1/2/3); untiered instruments excluded
      - sector_spdr: exact match on the real GICS sector-SPDR resolved from the
        SEC SIC (#1675; e.g. ``XLF``). The operator-facing peer dimension.
      - sector: DEPRECATED — exact match on the opaque ``instruments.sector``
        1-9 code (no GICS meaning). Retained for back-compat; use ``sector_spdr``.
      - stance: latest thesis stance for each instrument (adds LATERAL join only when used)
      - q: case-insensitive substring match on symbol OR company_name (#1825 search)
      - min_total_score: keep only rows with ``total_score >= min_total_score`` (#1825)

    Sorting (#1825): ``sort`` (column allowlist) + ``sort_dir`` (asc/desc), both
    Literals resolved through hardcoded maps so the user value never reaches the
    SQL string. A stable ``s.rank, s.instrument_id`` tiebreak makes every page a
    deterministic, drift-free slice. Server-authoritative pagination
    (``offset``/``limit``/``total``) lets the client page the WHOLE filtered,
    sorted population — no client-side truncation.
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        # Step 1: find the latest run timestamp for this model_version.
        cur.execute(
            "SELECT MAX(scored_at) AS latest FROM scores WHERE model_version = %(mv)s",
            {"mv": model_version},
        )
        # MAX() always returns exactly one row; the value is None when no rows match.
        ts_row = cur.fetchone()
        latest_scored_at = ts_row["latest"]  # type: ignore[index]

        if latest_scored_at is None:
            return RankingsListResponse(
                items=[],
                total=0,
                offset=offset,
                limit=limit,
                model_version=model_version,
                scored_at=None,
            )

        # Step 2: build dynamic WHERE / JOIN fragments.
        # #268 Chunk J gate: always require coverage.filings_status =
        # 'analysable' so the list endpoint can't surface stale score
        # rows for instruments that fell out of the analysable pool
        # between the last scoring run and now (e.g. audit regressed
        # them to insufficient / fpi / no_primary_sec_cik).
        # is_tradable gate (#1918): a delisted/non-tradable instrument with a
        # stale analysable score must never surface, and this makes the list
        # `total` exactly equal the /rankings/coverage `ranked` count (both gate
        # tradable+analysable over the latest run). Full-pop check 2026-07-04: 0
        # ranked rows are non-tradable today, so a no-op now — a forward guard.
        where_clauses: list[str] = [
            "s.model_version = %(mv)s",
            "s.scored_at = %(scored_at)s",
            "i.is_tradable = TRUE",
            "c.filings_status = 'analysable'",
        ]
        filter_params: dict[str, object] = {
            "mv": model_version,
            "scored_at": latest_scored_at,
        }

        # Always join the SEC profile so every ranking row can resolve its real
        # GICS sector for display (#1675) and the sector_spdr filter can resolve
        # p.sic SQL-side. PK join — trivial cost; shared by COUNT + items.
        extra_joins: list[str] = ["LEFT JOIN instrument_sec_profile p USING (instrument_id)"]

        if coverage_tier is not None:
            where_clauses.append("c.coverage_tier = %(coverage_tier)s")
            filter_params["coverage_tier"] = coverage_tier

        if sector is not None:
            where_clauses.append("i.sector = %(sector)s")
            filter_params["sector"] = sector

        if sector_spdr is not None:
            where_clauses.append(f"({sector_spdr_case_sql()}) = %(sector_spdr)s")
            filter_params["sector_spdr"] = sector_spdr

        # Stance filter: LATERAL join to latest thesis only when needed.
        if stance is not None:
            extra_joins.append(
                """LEFT JOIN LATERAL (
                    SELECT th.stance
                    FROM theses th
                    WHERE th.instrument_id = s.instrument_id
                    ORDER BY th.created_at DESC
                    LIMIT 1
                ) lt ON TRUE"""
            )
            where_clauses.append("lt.stance = %(stance)s")
            filter_params["stance"] = stance

        # Search: case-insensitive substring on symbol OR company_name (#1825).
        if q is not None and q.strip() != "":
            where_clauses.append("(i.symbol ILIKE %(q)s OR i.company_name ILIKE %(q)s)")
            filter_params["q"] = f"%{q.strip()}%"

        # Minimum total score (#1825). NULL total_score rows are excluded by the
        # >= comparison, which is correct — an unscored row has no score to clear
        # the bar.
        if min_total_score is not None:
            where_clauses.append("s.total_score >= %(min_total_score)s")
            filter_params["min_total_score"] = min_total_score

        where_sql = " WHERE " + " AND ".join(where_clauses)
        joins_sql = "\n".join(extra_joins)

        # Step 3: COUNT query — separate params dict (no limit/offset keys).
        count_sql = f"""SELECT COUNT(*) AS cnt
            FROM scores s
            JOIN instruments i USING (instrument_id)
            LEFT JOIN coverage c USING (instrument_id)
            {joins_sql}
            {where_sql}"""  # noqa: S608  — hardcoded fragments only

        cur.execute(count_sql, filter_params)  # type: ignore[arg-type]
        count_row = cur.fetchone()
        total: int = count_row["cnt"] if count_row else 0  # type: ignore[index]

        # Step 4: data query — adds limit/offset in a separate params dict.
        items_params: dict[str, object] = {
            **filter_params,
            "limit": limit,
            "offset": offset,
        }
        # ORDER BY: column + direction come ONLY from the hardcoded allowlist
        # maps (the request values are Literals, used as dict keys), so the
        # user-controlled value never reaches the SQL string. The
        # ``s.rank, s.instrument_id`` suffix is a stable, unique tiebreak so
        # every page is a deterministic slice even when the sort value (and
        # rank) tie (#1825 / Codex ckpt-1).
        order_col = _SORT_COLUMNS[sort]
        order_dir = _SORT_DIR[sort_dir]
        order_sql = f"ORDER BY {order_col} {order_dir} NULLS LAST, s.rank ASC NULLS LAST, s.instrument_id ASC"
        items_sql = f"""SELECT s.instrument_id, i.symbol, i.company_name, i.sector, p.sic,
                   c.coverage_tier,
                   s.rank, s.rank_delta,
                   s.total_score, s.raw_total,
                   s.quality_score, s.value_score, s.turnaround_score,
                   s.momentum_score, s.sentiment_score, s.confidence_score,
                   s.data_completeness, s.completeness_tier,
                   s.penalties_json, s.explanation,
                   s.model_version, s.scored_at
            FROM scores s
            JOIN instruments i USING (instrument_id)
            LEFT JOIN coverage c USING (instrument_id)
            {joins_sql}
            {where_sql}
            {order_sql}
            LIMIT %(limit)s OFFSET %(offset)s"""  # noqa: S608  — hardcoded fragments only

        cur.execute(items_sql, items_params)  # type: ignore[arg-type]
        rows = cur.fetchall()

    items = [_parse_ranking_item(r) for r in rows]
    return RankingsListResponse(
        items=items,
        total=total,
        offset=offset,
        limit=limit,
        model_version=model_version,
        scored_at=latest_scored_at,  # type: ignore[arg-type]
    )


@router.get("/coverage", response_model=RankingsCoverage)
def get_rankings_coverage(
    conn: psycopg.Connection[object] = Depends(get_conn),
    model_version: str = Query(default=_DEFAULT_MODEL_VERSION),
) -> RankingsCoverage:
    """Ranked-vs-universe denominator + why-not-ranked breakdown (#1918).

    Makes the Rankings surface honest: it renders only the scored subset of a
    ~12.6k tradable universe, and without this an operator reads the ~8.7k
    absent instruments as a bug rather than a correct exclusion (no SEC
    fundamentals to score).

    ``ranked`` is gated identically to ``GET /rankings`` (latest run of
    ``model_version``, ``is_tradable AND filings_status='analysable'``,
    ``COUNT(DISTINCT instrument_id)``), so the header count equals the table
    ``total``. The whole payload is computed in one SQL statement (atomic
    snapshot — two READ COMMITTED reads would not be). If no scoring run exists,
    ``scored_at`` is null and ``ranked`` is 0.
    """
    sql = """
        WITH latest AS (
            SELECT MAX(scored_at) AS scored_at
            FROM scores
            WHERE model_version = %(mv)s
        ),
        uni AS (
            SELECT c.filings_status
            FROM instruments i
            LEFT JOIN coverage c ON c.instrument_id = i.instrument_id
            WHERE i.is_tradable = TRUE
        ),
        ranked AS (
            -- COUNT(DISTINCT instrument_id): defensive. Within one run
            -- (a single scored_at) compute_rankings writes exactly one row per
            -- distinct instrument, so this equals the list endpoint's COUNT(*)
            -- today (full-pop check: 0 dup triples across all score rows). The
            -- DB-level UNIQUE(instrument_id, model_version, scored_at) that would
            -- make the equality unbreakable is tracked in #1933.
            SELECT COUNT(DISTINCT s.instrument_id) AS n
            FROM scores s
            JOIN instruments i USING (instrument_id)
            JOIN coverage c USING (instrument_id)
            WHERE s.model_version = %(mv)s
              AND s.scored_at = (SELECT scored_at FROM latest)
              AND i.is_tradable = TRUE
              AND c.filings_status = 'analysable'
        )
        SELECT
            (SELECT scored_at FROM latest) AS scored_at,
            (SELECT COUNT(*) FROM uni) AS universe,
            (SELECT n FROM ranked) AS ranked,
            COALESCE(
                (
                    SELECT jsonb_object_agg(filings_status, cnt)
                    FROM (
                        SELECT filings_status, COUNT(*) AS cnt
                        FROM uni
                        WHERE filings_status IS NOT NULL
                        GROUP BY filings_status
                    ) g
                ),
                '{}'::jsonb
            ) AS status_counts
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(sql, {"mv": model_version})
        row = cur.fetchone()

    # The SELECT always returns exactly one row (scalar subqueries).
    assert row is not None  # noqa: S101 — narrows the dict_row Optional for type checkers
    raw_status = row["status_counts"] or {}
    status_counts = {str(k): int(v) for k, v in raw_status.items()}

    return build_coverage(
        model_version=model_version,
        scored_at=row["scored_at"],
        universe=int(row["universe"] or 0),
        ranked=int(row["ranked"] or 0),
        status_counts=status_counts,
    )


@router.get("/history/{instrument_id}", response_model=ScoreHistoryResponse)
def get_score_history(
    instrument_id: int,
    conn: psycopg.Connection[object] = Depends(get_conn),
    model_version: str = Query(default=_DEFAULT_MODEL_VERSION),
    limit: int = Query(default=30, ge=1, le=MAX_PAGE_LIMIT),
) -> ScoreHistoryResponse:
    """Score trend over time for a single instrument.

    Returns the most recent ``limit`` score rows (newest first) for the
    requested ``model_version``.  If the instrument has never been scored,
    returns an empty list.
    """
    sql = """
        SELECT s.scored_at,
               s.total_score, s.raw_total,
               s.quality_score, s.value_score, s.turnaround_score,
               s.momentum_score, s.sentiment_score, s.confidence_score,
               s.penalties_json, s.explanation,
               s.rank, s.rank_delta,
               s.model_version
        FROM scores s
        WHERE s.instrument_id = %(instrument_id)s
          AND s.model_version = %(mv)s
        ORDER BY s.scored_at DESC
        LIMIT %(limit)s
    """
    params = {"instrument_id": instrument_id, "mv": model_version, "limit": limit}

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()

    items = [_parse_history_item(r) for r in rows]
    return ScoreHistoryResponse(instrument_id=instrument_id, items=items)


@router.get("/verdict/{instrument_id}", response_model=VerdictResponse)
def get_verdict(
    instrument_id: int,
    conn: psycopg.Connection[object] = Depends(get_conn),
    model_version: str = Query(default=_DEFAULT_MODEL_VERSION),
) -> VerdictResponse:
    """Latest score row for a single instrument — the Verdict tab payload (#1824).

    Returns *this instrument's* most recent score (``ORDER BY scored_at DESC
    LIMIT 1``), including the IAR (``analytics_json``, #1823). This deliberately
    diverges from the run-coherent ``MAX(scored_at)`` semantics of the rankings
    list: the list is a cross-sectional ranking that must be coherent within one
    run, whereas a single-instrument verdict wants this instrument's most recent
    analysis even if it was dropped from the very latest run. Staleness is made
    visible by returning ``scored_at``.

    200 with ``score = null`` when the instrument has never been scored
    (mirrors the instrument-404→200+null convention of #1813). Pre-#1823 rows
    keep ``analytics_json = null`` — the headline still renders.
    """
    sql = """
        SELECT s.scored_at, s.model_version,
               s.rank, s.rank_delta,
               s.total_score, s.raw_total,
               s.quality_score, s.value_score, s.turnaround_score,
               s.momentum_score, s.sentiment_score, s.confidence_score,
               s.data_completeness, s.completeness_tier,
               s.penalties_json, s.explanation, s.analytics_json
        FROM scores s
        WHERE s.instrument_id = %(instrument_id)s
          AND s.model_version = %(mv)s
        ORDER BY s.scored_at DESC
        LIMIT 1
    """
    params = {"instrument_id": instrument_id, "mv": model_version}

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(sql, params)
        row = cur.fetchone()

    if row is None:
        return VerdictResponse(instrument_id=instrument_id, score=None)

    return VerdictResponse(
        instrument_id=instrument_id,
        score=VerdictScore(
            scored_at=row["scored_at"],  # type: ignore[arg-type]
            model_version=row["model_version"],  # type: ignore[arg-type]
            rank=_parse_optional_int(row, "rank"),
            rank_delta=_parse_optional_int(row, "rank_delta"),
            total_score=_parse_optional_float(row, "total_score"),
            raw_total=_parse_optional_float(row, "raw_total"),
            quality_score=_parse_optional_float(row, "quality_score"),
            value_score=_parse_optional_float(row, "value_score"),
            turnaround_score=_parse_optional_float(row, "turnaround_score"),
            momentum_score=_parse_optional_float(row, "momentum_score"),
            sentiment_score=_parse_optional_float(row, "sentiment_score"),
            confidence_score=_parse_optional_float(row, "confidence_score"),
            data_completeness=_parse_optional_float(row, "data_completeness"),
            completeness_tier=row["completeness_tier"],  # type: ignore[arg-type]
            penalties_json=row["penalties_json"],  # type: ignore[arg-type]
            explanation=row["explanation"],  # type: ignore[arg-type]
            analytics_json=row["analytics_json"],  # type: ignore[arg-type]
        ),
    )
