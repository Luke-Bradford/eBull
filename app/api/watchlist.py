"""Watchlist API (Phase 3.2 of the 2026-04-19 research-tool refocus).

Per-operator list of instruments the user is tracking. V1 assumes a
single operator — ``sole_operator_id`` resolves the current operator
rather than a session-scoped one, matching how other operator-scoped
endpoints in this codebase work today. When multi-operator sessions
land, swap to a session-backed dependency.

Routes:
  GET    /watchlist                     — list tracked instruments (newest-first)
  POST   /watchlist                     — add {symbol, notes?}
  DELETE /watchlist/{symbol}            — remove
"""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

import psycopg
import psycopg.rows
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from app.api.auth import require_session_or_service_token
from app.db import get_conn
from app.services.operators import AmbiguousOperatorError, NoOperatorError, sole_operator_id

router = APIRouter(
    prefix="/watchlist",
    tags=["watchlist"],
    dependencies=[Depends(require_session_or_service_token)],
)


class WatchlistItem(BaseModel):
    instrument_id: int
    symbol: str
    company_name: str
    exchange: str | None
    currency: str | None
    sector: str | None
    added_at: datetime
    notes: str | None


class WatchlistListResponse(BaseModel):
    items: list[WatchlistItem]
    total: int


class WatchlistAddRequest(BaseModel):
    symbol: str = Field(min_length=1, max_length=32)
    notes: str | None = None


def _resolve_operator(conn: psycopg.Connection[object]) -> UUID:
    try:
        return sole_operator_id(conn)
    except NoOperatorError as exc:
        raise HTTPException(status_code=503, detail="no operator configured") from exc
    except AmbiguousOperatorError as exc:
        raise HTTPException(
            status_code=501,
            detail="multiple operators present — watchlist requires a per-session operator context",
        ) from exc


@router.get("", response_model=WatchlistListResponse)
def list_watchlist(
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> WatchlistListResponse:
    operator_id = _resolve_operator(conn)
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT i.instrument_id, i.symbol, i.company_name, i.exchange,
                   i.currency, i.sector,
                   w.added_at, w.notes
            FROM watchlist w
            JOIN instruments i USING (instrument_id)
            WHERE w.operator_id = %(op)s
            ORDER BY w.added_at DESC
            """,
            {"op": operator_id},
        )
        rows = cur.fetchall()
    items = [
        WatchlistItem(
            instrument_id=int(r["instrument_id"]),  # type: ignore[arg-type]
            symbol=str(r["symbol"]),  # type: ignore[index]
            company_name=str(r["company_name"]),  # type: ignore[index]
            exchange=r["exchange"],  # type: ignore[union-attr]
            currency=r["currency"],  # type: ignore[union-attr]
            sector=r["sector"],  # type: ignore[union-attr]
            added_at=r["added_at"],  # type: ignore[arg-type]
            notes=r["notes"],  # type: ignore[union-attr]
        )
        for r in rows
    ]
    return WatchlistListResponse(items=items, total=len(items))


@router.post("", response_model=WatchlistItem, status_code=201)
def add_to_watchlist(
    req: WatchlistAddRequest,
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> WatchlistItem:
    operator_id = _resolve_operator(conn)
    symbol_clean = req.symbol.strip().upper()
    if not symbol_clean:
        raise HTTPException(status_code=400, detail="symbol is required")

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            "SELECT instrument_id, symbol, company_name, exchange, currency, sector "
            "FROM instruments WHERE UPPER(symbol) = %(s)s LIMIT 1",
            {"s": symbol_clean},
        )
        inst = cur.fetchone()
    if inst is None:
        raise HTTPException(status_code=404, detail=f"Instrument {req.symbol} not found")

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            INSERT INTO watchlist (operator_id, instrument_id, notes)
            VALUES (%(op)s, %(iid)s, %(notes)s)
            ON CONFLICT (operator_id, instrument_id)
            DO UPDATE SET
                -- Preserve existing notes when the caller omits notes
                -- on a re-add; only overwrite on an explicit new note.
                notes = COALESCE(EXCLUDED.notes, watchlist.notes)
                -- added_at intentionally NOT updated — 'first added'
                -- semantics must survive idempotent re-adds.
            RETURNING added_at, notes
            """,
            {"op": operator_id, "iid": int(inst["instrument_id"]), "notes": req.notes},  # type: ignore[arg-type]
        )
        wl_row = cur.fetchone()
    conn.commit()
    if wl_row is None:
        # RETURNING must produce a row — if it didn't, the INSERT/UPDATE
        # path is broken and we should surface that instead of crashing
        # under python -O (where bare ``assert`` is compiled away).
        raise HTTPException(status_code=500, detail="watchlist upsert returned no row")

    return WatchlistItem(
        instrument_id=int(inst["instrument_id"]),  # type: ignore[arg-type]
        symbol=str(inst["symbol"]),  # type: ignore[index]
        company_name=str(inst["company_name"]),  # type: ignore[index]
        exchange=inst["exchange"],  # type: ignore[union-attr]
        currency=inst["currency"],  # type: ignore[union-attr]
        sector=inst["sector"],  # type: ignore[union-attr]
        added_at=wl_row["added_at"],  # type: ignore[arg-type]
        notes=wl_row["notes"],  # type: ignore[union-attr]
    )


@router.delete("/{symbol}", status_code=204)
def remove_from_watchlist(
    symbol: str,
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> None:
    operator_id = _resolve_operator(conn)
    symbol_clean = symbol.strip().upper()
    if not symbol_clean:
        raise HTTPException(status_code=400, detail="symbol is required")

    with conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM watchlist w
            USING instruments i
            WHERE w.operator_id = %(op)s
              AND w.instrument_id = i.instrument_id
              AND UPPER(i.symbol) = %(s)s
            """,
            {"op": operator_id, "s": symbol_clean},
        )
        rows_deleted = cur.rowcount

    # Check rowcount BEFORE commit — a 404 (nothing deleted) must not
    # commit a no-op transaction (cosmetic, but noisy in audit logs
    # and confusing to clients that retry on 404).
    if rows_deleted == 0:
        raise HTTPException(
            status_code=404,
            detail=f"Instrument {symbol} not on watchlist",
        )
    conn.commit()
