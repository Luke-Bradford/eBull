"""Thesis API endpoints.

Reads from:
  - theses       (append-only versioned thesis rows per instrument)
  - instruments   (existence check for 404 on history endpoint)

Writes from POST /instruments/{symbol}/thesis (Phase 2.4) via the
existing ``generate_thesis`` service — 24h-cached per-ticker unless
``?force=true`` (#1919).

Auth (#1919): both routers require a session or service token. Before
this, the LLM-spending POST and every memo read were unauthenticated —
anyone with reach to :8000 could enumerate memos and burn LLM spend.

Note: the issue (#52) mentions ``conviction_score`` but the theses table
has ``confidence_score``.  This module uses the actual schema column name.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta

import psycopg
import psycopg.rows
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from app.api.auth import require_session_or_service_token
from app.db import get_conn
from app.services.llm_client import LLMClient, LLMProviderNotConfigured, make_llm_client
from app.services.runtime_config import RuntimeConfigCorrupt
from app.services.scoring import _DEFAULT_MODEL_VERSION
from app.services.thesis import find_stale_instruments, generate_thesis

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/theses",
    tags=["theses"],
    dependencies=[Depends(require_session_or_service_token)],
)


# Separate router for the symbol-based POST; kept under /instruments so
# the research page can POST to a single resource prefix.
instrument_thesis_router = APIRouter(
    prefix="/instruments",
    tags=["instruments"],
    dependencies=[Depends(require_session_or_service_token)],
)


def get_llm_client(conn: psycopg.Connection[object] = Depends(get_conn)) -> LLMClient:
    """FastAPI dependency: resolves the configured LLM provider per request.

    All config resolution goes through ``make_llm_client`` (#1919 —
    replaces the direct ``os.environ`` read this module used to do).
    503 when the provider cannot be constructed (anthropic configured
    without a key) or runtime_config is corrupt — the thesis endpoint is
    the only caller that needs an LLM, so failing here keeps the rest of
    the API unaffected.
    """
    try:
        return make_llm_client(conn)
    except LLMProviderNotConfigured as exc:
        # Fixed string — never echo internal exception text (#87).
        raise HTTPException(
            status_code=503,
            detail="LLM provider not configured — thesis generation unavailable",
        ) from exc
    except RuntimeConfigCorrupt as exc:
        raise HTTPException(status_code=503, detail="runtime config unavailable") from exc


MAX_PAGE_LIMIT = 200

# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------


class ThesisDetail(BaseModel):
    """Single thesis row with all columns including critic output.

    ``is_stale`` / ``stale_reason`` are populated ONLY by the latest-thesis
    GET (#1902 staleness single-source — the canonical predicate is
    ``find_stale_instruments``, coverage.review_frequency-based, NOT a
    client-side day constant). History rows and the POST response leave
    them null: a history row is inherently historical and the POST just
    generated the thesis.
    """

    thesis_id: int
    instrument_id: int
    thesis_version: int
    thesis_type: str
    stance: str
    confidence_score: float | None
    buy_zone_low: float | None
    buy_zone_high: float | None
    base_value: float | None
    bull_value: float | None
    bear_value: float | None
    break_conditions_json: list[str] | None
    memo_markdown: str
    critic_json: dict[str, object] | None
    created_at: datetime
    # Provenance (#2000): stamped at insert since #1919 PR-A; nullable —
    # pre-#1919 rows have no attribution. Surfaced so the operator can
    # tell an anchored v2 memo from a blind-priced v1 on the page.
    prompt_version: str | None = None
    model: str | None = None
    provider: str | None = None
    is_stale: bool | None = None
    stale_reason: str | None = None


class ThesisHistoryResponse(BaseModel):
    instrument_id: int
    items: list[ThesisDetail]
    total: int
    offset: int
    limit: int


class ThesisLibraryItem(BaseModel):
    """One row of the Theses library (#1902): latest thesis per instrument
    plus display context (held flag, latest score, latest generation-run
    status, server-computed staleness).

    HELD instruments WITHOUT any thesis also get a row (thesis fields
    null, ``stale_reason='no_thesis'`` when analysable) — the dashboard
    staleness alert includes them, so the library it links to must too
    (Codex ckpt-2 finding 1). Unheld instruments without theses stay out:
    the library is a thesis surface, not the instrument universe.

    ``run_status`` is the latest ``thesis_runs`` row for the instrument —
    'running' | 'ok' | 'failed' (DB CHECK-constrained), None when the
    instrument predates thesis_runs (#1919). ``stale_reason`` is None when
    the thesis is fresh OR the instrument is outside the refresh engine's
    scope (not tradable / not analysable — regeneration would never fire,
    so flagging it stale would nag forever with no cure).
    ``critic_verdict`` stays an open string: validated at write time
    against the critic enum, but the column is free JSON — no response
    Literal over open text (#1808 class).
    """

    instrument_id: int
    symbol: str
    company_name: str
    thesis_id: int | None
    thesis_version: int | None
    thesis_type: str | None
    stance: str | None
    confidence_score: float | None
    buy_zone_low: float | None
    buy_zone_high: float | None
    created_at: datetime | None
    critic_verdict: str | None
    stale_reason: str | None
    is_held: bool
    latest_score: float | None
    latest_rank: int | None
    run_status: str | None
    run_error: str | None
    run_trigger: str | None
    run_started_at: datetime | None


class ThesisLibraryResponse(BaseModel):
    items: list[ThesisLibraryItem]
    total: int
    offset: int
    limit: int


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_optional_float(row: dict[str, object], key: str) -> float | None:
    """Safely cast a nullable numeric DB column to float."""
    val = row.get(key)
    if val is None:
        return None
    return float(val)  # type: ignore[arg-type]


def _parse_thesis(row: dict[str, object]) -> ThesisDetail:
    return ThesisDetail(
        thesis_id=row["thesis_id"],  # type: ignore[arg-type]
        instrument_id=row["instrument_id"],  # type: ignore[arg-type]
        thesis_version=row["thesis_version"],  # type: ignore[arg-type]
        thesis_type=row["thesis_type"],  # type: ignore[arg-type]
        stance=row["stance"],  # type: ignore[arg-type]
        confidence_score=_parse_optional_float(row, "confidence_score"),
        buy_zone_low=_parse_optional_float(row, "buy_zone_low"),
        buy_zone_high=_parse_optional_float(row, "buy_zone_high"),
        base_value=_parse_optional_float(row, "base_value"),
        bull_value=_parse_optional_float(row, "bull_value"),
        bear_value=_parse_optional_float(row, "bear_value"),
        break_conditions_json=row["break_conditions_json"],  # type: ignore[arg-type]
        memo_markdown=row["memo_markdown"],  # type: ignore[arg-type]
        critic_json=row["critic_json"],  # type: ignore[arg-type]
        created_at=row["created_at"],  # type: ignore[arg-type]
        prompt_version=row.get("prompt_version"),  # type: ignore[arg-type]
        model=row.get("model"),  # type: ignore[arg-type]
        provider=row.get("provider"),  # type: ignore[arg-type]
    )


_THESIS_COLUMNS = """
    t.thesis_id, t.instrument_id, t.thesis_version,
    t.thesis_type, t.stance, t.confidence_score,
    t.buy_zone_low, t.buy_zone_high,
    t.base_value, t.bull_value, t.bear_value,
    t.break_conditions_json, t.memo_markdown, t.critic_json,
    t.created_at, t.prompt_version, t.model, t.provider
"""


def _critic_verdict(critic_json: object) -> str | None:
    """Extract the critic's verdict string from a critic_json payload."""
    if not isinstance(critic_json, dict):
        return None
    verdict = critic_json.get("verdict")
    return verdict if isinstance(verdict, str) else None


def library_order_key(row: dict[str, object]) -> tuple[int, float, int]:
    """Deterministic library ordering, independent of per-query ORDER BYs.

    Gap rows (held, no thesis — ``created_at`` is None) sort first: a
    missing memo on money is the most actionable row. Thesis rows follow
    newest-first, thesis_id tiebreak. One sort key in one place so the
    page order can't silently change if either SQL constant's ORDER BY
    is edited independently (review NITPICK).
    """
    created = row.get("created_at")
    if not isinstance(created, datetime):
        return (0, 0.0, 0)
    thesis_id = row.get("thesis_id")
    return (1, -created.timestamp(), -int(thesis_id) if isinstance(thesis_id, int) else 0)


def filter_and_page_library(
    rows: list[dict[str, object]],
    stale_reasons: dict[int, str],
    *,
    held_only: bool,
    stale_only: bool,
    stance: str | None,
    offset: int,
    limit: int,
) -> tuple[int, list[dict[str, object]]]:
    """Apply library filters + pagination to the latest-per-instrument rows.

    Pure (table-tested without a DB). All filtering is post-SQL so the
    query stays one static shape and staleness — which only exists as
    the canonical Python predicate ``find_stale_instruments`` — composes
    with the other filters in one place. Row volume is bounded by the
    number of instruments that have a thesis (hundreds under the #1919
    batch-bounded refresh), so in-memory filtering is fine.

    Returns ``(total_after_filters, page_slice)``. Mutates each row with
    its ``stale_reason`` so callers get one enriched shape.
    """
    filtered: list[dict[str, object]] = []
    for row in rows:
        # int() cast, not assert — asserts are stripped under `python -O`
        # so they can't gate response shape (review WARNING; same rule as
        # python-hygiene.md "never assert production invariants").
        instrument_id = int(row["instrument_id"])  # type: ignore[arg-type]
        row["stale_reason"] = stale_reasons.get(instrument_id)
        if held_only and not row["is_held"]:
            continue
        if stale_only and row["stale_reason"] is None:
            continue
        if stance is not None and row["stance"] != stance:
            continue
        filtered.append(row)
    return len(filtered), filtered[offset : offset + limit]


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


# Latest thesis per instrument + display context. DISTINCT ON picks the
# newest (created_at, thesis_version) row per instrument — same tiebreak
# as the per-instrument latest read. The scores LATERAL is deliberately
# per-instrument-latest, NOT run-coherent MAX(scored_at): this is a
# per-row display column, the same divergence get_verdict documents
# (app/api/scores.py) — an instrument dropped from the very latest run
# still shows its most recent analysis, with staleness visible via rank.
# The thesis_runs LATERAL rides idx_thesis_runs_instrument_started;
# run_id DESC breaks same-timestamp ties deterministically.
_LIBRARY_SQL = """
    SELECT
        t.thesis_id, t.instrument_id, t.thesis_version, t.thesis_type,
        t.stance, t.confidence_score, t.buy_zone_low, t.buy_zone_high,
        t.critic_json, t.created_at,
        i.symbol, i.company_name,
        EXISTS (
            SELECT 1 FROM positions p
            WHERE p.instrument_id = t.instrument_id
              AND p.current_units > 0
        ) AS is_held,
        s.total_score AS latest_score,
        s.rank        AS latest_rank,
        r.status      AS run_status,
        r.error       AS run_error,
        r.trigger     AS run_trigger,
        r.started_at  AS run_started_at
    FROM (
        SELECT DISTINCT ON (instrument_id)
            thesis_id, instrument_id, thesis_version, thesis_type,
            stance, confidence_score, buy_zone_low, buy_zone_high,
            critic_json, created_at
        FROM theses
        ORDER BY instrument_id, created_at DESC, thesis_version DESC
    ) t
    JOIN instruments i ON i.instrument_id = t.instrument_id
    LEFT JOIN LATERAL (
        SELECT s.total_score, s.rank
        FROM scores s
        WHERE s.instrument_id = t.instrument_id
          AND s.model_version = %(mv)s
        ORDER BY s.scored_at DESC
        LIMIT 1
    ) s ON TRUE
    LEFT JOIN LATERAL (
        SELECT r.status, r.error, r.trigger, r.started_at
        FROM thesis_runs r
        WHERE r.instrument_id = t.instrument_id
        ORDER BY r.started_at DESC, r.run_id DESC
        LIMIT 1
    ) r ON TRUE
    ORDER BY t.created_at DESC, t.thesis_id DESC
"""

# Held instruments with NO thesis at all — the actionable gap the dashboard
# staleness alert surfaces, so the library must show them too (Codex ckpt-2).
# Same LATERAL context as _LIBRARY_SQL; thesis columns are typed NULLs so both
# result sets share one row shape. Prepended above the thesis rows by the
# endpoint: a missing memo on money outranks any existing memo's age.
_HELD_NO_THESIS_SQL = """
    SELECT
        NULL::bigint      AS thesis_id,
        i.instrument_id,
        NULL::int         AS thesis_version,
        NULL::text        AS thesis_type,
        NULL::text        AS stance,
        NULL::numeric     AS confidence_score,
        NULL::numeric     AS buy_zone_low,
        NULL::numeric     AS buy_zone_high,
        NULL::jsonb       AS critic_json,
        NULL::timestamptz AS created_at,
        i.symbol, i.company_name,
        TRUE AS is_held,
        s.total_score AS latest_score,
        s.rank        AS latest_rank,
        r.status      AS run_status,
        r.error       AS run_error,
        r.trigger     AS run_trigger,
        r.started_at  AS run_started_at
    FROM instruments i
    LEFT JOIN LATERAL (
        SELECT s.total_score, s.rank
        FROM scores s
        WHERE s.instrument_id = i.instrument_id
          AND s.model_version = %(mv)s
        ORDER BY s.scored_at DESC
        LIMIT 1
    ) s ON TRUE
    LEFT JOIN LATERAL (
        SELECT r.status, r.error, r.trigger, r.started_at
        FROM thesis_runs r
        WHERE r.instrument_id = i.instrument_id
        ORDER BY r.started_at DESC, r.run_id DESC
        LIMIT 1
    ) r ON TRUE
    WHERE EXISTS (
            SELECT 1 FROM positions p
            WHERE p.instrument_id = i.instrument_id
              AND p.current_units > 0
          )
      AND NOT EXISTS (
            SELECT 1 FROM theses t
            WHERE t.instrument_id = i.instrument_id
          )
    ORDER BY i.symbol
"""


@router.get("", response_model=ThesisLibraryResponse)
def list_theses(
    conn: psycopg.Connection[object] = Depends(get_conn),
    held_only: bool = Query(default=False),
    stale: bool = Query(default=False),
    stance: str | None = Query(default=None, pattern="^(buy|hold|watch|avoid)$"),
    offset: int = Query(default=0, ge=0),
    limit: int = Query(default=50, ge=1, le=MAX_PAGE_LIMIT),
) -> ThesisLibraryResponse:
    """Theses library (#1902): latest thesis per instrument, newest first.

    Staleness is computed server-side via ``find_stale_instruments`` —
    the single canonical predicate (coverage.review_frequency cadence +
    #273 filing-event triggers) shared with the ``thesis_refresh``
    scheduler, so the library column, the instrument-page chip and the
    refresh engine can never disagree. Row volume is bounded by the
    number of instruments holding a thesis (unpaginated scan, then
    in-memory filter + slice — see ``filter_and_page_library``).
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        # Held-but-thesis-less gap rows + latest-thesis rows (Codex ckpt-2),
        # merged under ONE explicit sort key (gap rows first, then newest
        # thesis) so page order never depends on concat order of the two
        # queries. Stable sort preserves each query's own tiebreak order.
        cur.execute(_HELD_NO_THESIS_SQL, {"mv": _DEFAULT_MODEL_VERSION})
        rows: list[dict[str, object]] = list(cur.fetchall())
        cur.execute(_LIBRARY_SQL, {"mv": _DEFAULT_MODEL_VERSION})
        rows.extend(cur.fetchall())
        rows.sort(key=library_order_key)

    stale_reasons: dict[int, str] = {}
    if rows:
        instrument_ids = [int(r["instrument_id"]) for r in rows]  # type: ignore[arg-type]
        stale_reasons = {
            s.instrument_id: s.reason for s in find_stale_instruments(conn, tier=None, instrument_ids=instrument_ids)
        }

    total, page = filter_and_page_library(
        rows,
        stale_reasons,
        held_only=held_only,
        stale_only=stale,
        stance=stance,
        offset=offset,
        limit=limit,
    )

    items = [
        ThesisLibraryItem(
            instrument_id=row["instrument_id"],  # type: ignore[arg-type]
            symbol=row["symbol"],  # type: ignore[arg-type]
            company_name=row["company_name"],  # type: ignore[arg-type]
            thesis_id=row["thesis_id"],  # type: ignore[arg-type]
            thesis_version=row["thesis_version"],  # type: ignore[arg-type]
            thesis_type=row["thesis_type"],  # type: ignore[arg-type]
            stance=row["stance"],  # type: ignore[arg-type]
            confidence_score=_parse_optional_float(row, "confidence_score"),
            buy_zone_low=_parse_optional_float(row, "buy_zone_low"),
            buy_zone_high=_parse_optional_float(row, "buy_zone_high"),
            created_at=row["created_at"],  # type: ignore[arg-type]
            critic_verdict=_critic_verdict(row["critic_json"]),
            stale_reason=row["stale_reason"],  # type: ignore[arg-type]
            is_held=bool(row["is_held"]),
            latest_score=_parse_optional_float(row, "latest_score"),
            latest_rank=row["latest_rank"],  # type: ignore[arg-type]
            run_status=row["run_status"],  # type: ignore[arg-type]
            run_error=row["run_error"],  # type: ignore[arg-type]
            run_trigger=row["run_trigger"],  # type: ignore[arg-type]
            run_started_at=row["run_started_at"],  # type: ignore[arg-type]
        )
        for row in page
    ]
    return ThesisLibraryResponse(items=items, total=total, offset=offset, limit=limit)


@router.get("/{instrument_id}", response_model=ThesisDetail | None)
def get_latest_thesis(
    instrument_id: int,
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> ThesisDetail | None:
    """Latest thesis for an instrument, ordered by created_at then version.

    Returns **200 with a null body** when no thesis exists yet — the
    normal pre-thesis state, not an error. The instrument page fetches
    this on every load, so a 404 here meant a console error on every
    not-yet-analysed instrument (#1813)."""
    sql = f"""
        SELECT {_THESIS_COLUMNS}
        FROM theses t
        WHERE t.instrument_id = %(instrument_id)s
        ORDER BY t.created_at DESC, t.thesis_version DESC
        LIMIT 1
    """  # safe: _THESIS_COLUMNS is a module-level constant, not user input
    params = {"instrument_id": instrument_id}

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(sql, params)
        row = cur.fetchone()

        if row is None:
            # Distinguish "unknown instrument" (404) from "known
            # instrument, no thesis yet" (200 + null) — same contract as
            # get_thesis_history. The null case is the normal pre-analysis
            # state and lets the research page render its Generate-thesis
            # affordance without a console error on every un-analysed
            # instrument (#1813). Existence check only on the no-thesis
            # path, so the common (thesis-present) read stays single-query.
            cur.execute(
                "SELECT 1 FROM instruments WHERE instrument_id = %(instrument_id)s",
                {"instrument_id": instrument_id},
            )
            if cur.fetchone() is None:
                raise HTTPException(
                    status_code=404,
                    detail=f"Instrument {instrument_id} not found",
                )
            return None

    thesis = _parse_thesis(row)
    # Staleness single-source (#1902): the FE used to duplicate a 30-day
    # constant; the canonical predicate is find_stale_instruments
    # (coverage cadence + #273 filing-event triggers). reason=None means
    # fresh OR outside refresh scope (not tradable / not analysable) —
    # both render as not-stale because regeneration would never fire.
    stale = find_stale_instruments(conn, tier=None, instrument_ids=[instrument_id])
    thesis.stale_reason = stale[0].reason if stale else None
    thesis.is_stale = bool(stale)
    return thesis


@router.get("/{instrument_id}/history", response_model=ThesisHistoryResponse)
def get_thesis_history(
    instrument_id: int,
    conn: psycopg.Connection[object] = Depends(get_conn),
    offset: int = Query(default=0, ge=0),
    limit: int = Query(default=50, ge=1, le=MAX_PAGE_LIMIT),
) -> ThesisHistoryResponse:
    """Paginated thesis history for an instrument, newest first.

    Returns 404 if the instrument does not exist.
    Returns 200 with empty items if the instrument exists but has no theses.
    """
    # Check instrument existence first.
    exists_sql = """
        SELECT 1 FROM instruments WHERE instrument_id = %(instrument_id)s
    """
    exists_params = {"instrument_id": instrument_id}

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(exists_sql, exists_params)
        if cur.fetchone() is None:
            raise HTTPException(
                status_code=404,
                detail=f"Instrument {instrument_id} not found",
            )

        # COUNT then SELECT is a TOCTOU window, but theses is append-only
        # so total can only grow between queries — never shrink.
        # Separate params dict (prevention log: shared params).
        count_sql = """
            SELECT COUNT(*) AS cnt
            FROM theses t
            WHERE t.instrument_id = %(instrument_id)s
        """
        count_params = {"instrument_id": instrument_id}
        cur.execute(count_sql, count_params)
        # Aggregate SELECT always returns exactly one row; guard the column.
        count_row = cur.fetchone()
        total: int = int(count_row["cnt"])  # type: ignore[index,arg-type]

        if total == 0:
            return ThesisHistoryResponse(
                instrument_id=instrument_id,
                items=[],
                total=0,
                offset=offset,
                limit=limit,
            )

        # Data query — separate params dict with limit/offset.
        data_sql = f"""
            SELECT {_THESIS_COLUMNS}
            FROM theses t
            WHERE t.instrument_id = %(instrument_id)s
            ORDER BY t.created_at DESC, t.thesis_version DESC
            LIMIT %(limit)s OFFSET %(offset)s
        """  # safe: _THESIS_COLUMNS is a module-level constant, not user input
        data_params = {"instrument_id": instrument_id, "limit": limit, "offset": offset}
        cur.execute(data_sql, data_params)
        rows = cur.fetchall()

    items = [_parse_thesis(r) for r in rows]
    return ThesisHistoryResponse(
        instrument_id=instrument_id,
        items=items,
        total=total,
        offset=offset,
        limit=limit,
    )


# ---------------------------------------------------------------------------
# Symbol-keyed thesis endpoint (Phase 2.4)
# ---------------------------------------------------------------------------


THESIS_CACHE_WINDOW = timedelta(hours=24)


class GenerateThesisResponse(BaseModel):
    """Result of POST /instruments/{symbol}/thesis.

    ``cached`` reports whether the returned thesis came from the 24h
    cache (no Anthropic spend for this request) or was freshly
    generated this call.
    """

    cached: bool
    thesis: ThesisDetail


@instrument_thesis_router.post("/{symbol}/thesis", response_model=GenerateThesisResponse)
def generate_instrument_thesis(
    symbol: str,
    force: bool = Query(default=False),
    conn: psycopg.Connection[object] = Depends(get_conn),
    client: LLMClient = Depends(get_llm_client),
) -> GenerateThesisResponse:
    """Generate or return the cached thesis for a ticker.

    Phase 2.4 of the 2026-04-19 research-tool refocus. Cache window is
    24h per ticker: a POST within 24h of the last thesis returns the
    cached row without calling the LLM; after 24h the endpoint
    regenerates. ``?force=true`` (#1919) bypasses the cache — local-first
    provider config makes the spend implication negligible; the attempt
    is recorded in thesis_runs with trigger='manual' either way.

    Returns:
      - 404 if the symbol is not in the local instruments table
      - 503 if the LLM provider is not configured
      - 200 with the thesis (cached or fresh)
    """
    symbol_clean = symbol.strip().upper()
    if not symbol_clean:
        raise HTTPException(status_code=400, detail="symbol is required")

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            "SELECT instrument_id FROM instruments WHERE UPPER(symbol) = %(s)s LIMIT 1",
            {"s": symbol_clean},
        )
        inst_row = cur.fetchone()

    if inst_row is None:
        raise HTTPException(status_code=404, detail=f"Instrument {symbol} not found")
    instrument_id = int(inst_row["instrument_id"])  # type: ignore[arg-type]

    # Cache check: latest thesis for this instrument within 24h.
    # Skipped entirely under force=true.
    if not force:
        latest_sql = f"""
            SELECT {_THESIS_COLUMNS}
            FROM theses t
            WHERE t.instrument_id = %(iid)s
              AND t.created_at >= %(since)s
            ORDER BY t.created_at DESC, t.thesis_version DESC
            LIMIT 1
        """  # noqa: S608 — _THESIS_COLUMNS is a module-level constant
        now = datetime.now(UTC)
        cache_cutoff = now - THESIS_CACHE_WINDOW
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(latest_sql, {"iid": instrument_id, "since": cache_cutoff})
            cached_row = cur.fetchone()

        if cached_row is not None:
            logger.info(
                "POST /instruments/%s/thesis: cache hit (created_at=%s)",
                symbol_clean,
                cached_row["created_at"],  # type: ignore[index]
            )
            return GenerateThesisResponse(cached=True, thesis=_parse_thesis(cached_row))

    # Cache miss (or force) — call the existing generate_thesis service.
    # It handles its own DB transaction + LLM calls. We must NOT wrap
    # this in our own transaction (see generate_thesis caller contract).
    logger.info("POST /instruments/%s/thesis: %s, generating", symbol_clean, "forced" if force else "cache miss")
    try:
        generate_thesis(instrument_id, conn, client, trigger="manual")
    except Exception as exc:
        logger.exception("POST /instruments/%s/thesis: generation failed", symbol_clean)
        raise HTTPException(
            status_code=502,
            detail=f"thesis generation failed: {type(exc).__name__}",
        ) from exc

    # Re-read the just-inserted thesis via the same columns shape so the
    # response format is stable.
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            f"""
            SELECT {_THESIS_COLUMNS}
            FROM theses t
            WHERE t.instrument_id = %(iid)s
            ORDER BY t.created_at DESC, t.thesis_version DESC
            LIMIT 1
            """,  # noqa: S608
            {"iid": instrument_id},
        )
        fresh_row = cur.fetchone()

    if fresh_row is None:
        # Shouldn't happen — generate_thesis just inserted. Defensive.
        raise HTTPException(status_code=500, detail="thesis row missing after generation")

    return GenerateThesisResponse(cached=False, thesis=_parse_thesis(fresh_row))
