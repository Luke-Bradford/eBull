"""Calendar-of-events API (#1754 Phase B).

A portfolio/watchlist-wide view: this week's market status (trading day /
half-day / closed) per session profile, plus the real upcoming corporate
events we actually ingest — ex-dividends.

Scope of the data (premise-checked on dev, #1754):
  * **Market status** — US is precise via the in-house ``market_calendar``
    (NYSE). ``foreign_equity`` degrades to weekday-open / weekend-closed
    (``holidays_modelled=false``) — no foreign exchange holiday set is
    modelled (the operator's "no new deps" decision; #609 posture).
    ``continuous`` (crypto/fx/commodity/index) is ``not_modelled`` — we do
    not assert an open/closed for it.
  * **Upcoming ex-dividends** — ``dividend_events.ex_date >= today``. Real
    (sparse) data.
  * Forward earnings + filing dates are deliberately NOT here: we ingest no
    forward earnings calendar and no filing-due dates (verified absent).

``is_open_now`` / the current intraday session is computed on the FRONTEND
via the existing ``classifySession`` over these rows + the market specials
it already fetches — no server-side duplication of the session-window logic.

Auth: single-operator / global semantics matching the rest of the app —
``positions`` is global; ``watchlist`` is resolved via ``sole_operator_id``.
A service-token caller sees instance-wide scope.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Literal
from zoneinfo import ZoneInfo

import psycopg
import psycopg.rows
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from app.api.auth import require_session_or_service_token
from app.api.instruments import _SESSION_PROFILE_SQL
from app.db import get_conn
from app.services.market_calendar import us_market_reason, us_market_status
from app.services.operators import AmbiguousOperatorError, NoOperatorError, sole_operator_id

router = APIRouter(
    prefix="/calendar",
    tags=["calendar"],
    dependencies=[Depends(require_session_or_service_token)],
)

_NY = ZoneInfo("America/New_York")
_DEFAULT_HORIZON_DAYS = 7
_MAX_HORIZON_DAYS = 28

CalendarScope = Literal["portfolio", "watchlist", "all"]
DayType = Literal["open", "half_day", "closed", "not_modelled"]

# Human label + degradation metadata per session profile.
_PROFILE_META: dict[str, tuple[str, str, bool]] = {
    # profile -> (label, timezone, holidays_modelled)
    "us_equity": ("US equity", "America/New_York", True),
    "us_equity_rth": ("US equity (regular hours)", "America/New_York", True),
    "foreign_equity": ("Foreign equity", "exchange-local (approx)", False),
    "continuous": ("Continuous (crypto / FX / commodity / index)", "UTC", False),
}


class MarketStatusDay(BaseModel):
    date: date
    day_type: DayType
    # Why the day is not a regular session (holiday / early-close occasion /
    # "Weekend"), or None on a normal open day. US is precise; foreign carries
    # only "Weekend" (holidays not modelled); continuous is always None.
    reason: str | None = None


class MarketStatusRow(BaseModel):
    profile: str
    label: str
    timezone: str
    holidays_modelled: bool
    week: list[MarketStatusDay]


class UpcomingExDividend(BaseModel):
    symbol: str
    instrument_id: int
    ex_date: date
    pay_date: date | None


class CalendarEvents(BaseModel):
    scope: CalendarScope
    as_of: date
    market_status: list[MarketStatusRow]
    ex_dividends: list[UpcomingExDividend]


def _day_type(profile: str, d: date) -> DayType:
    """Trading-day classification for a session profile on a civil date.

    US is precise (NYSE). Foreign degrades to weekday/weekend. Continuous is
    not modelled. ``d`` is the relevant civil date (NY for US; the caller uses
    NY 'today' as the anchor uniformly — foreign weekend-vs-weekday is robust
    to the ~hours of NY/local skew)."""
    if profile in ("us_equity", "us_equity_rth"):
        return us_market_status(d)
    if profile == "foreign_equity":
        return "closed" if d.weekday() >= 5 else "open"
    if profile == "continuous":
        # crypto / FX / commodity / index — eBull's settled stance (#609
        # classifySession) is "no session concept, always trading"; report it
        # the same way the chart + the live now-badge do, not as not_modelled.
        return "open"
    return "not_modelled"  # unknown/future profile — safe default


def _day_reason(profile: str, d: date) -> str | None:
    """Operator-facing reason a day is non-open, paired with ``_day_type``.

    US profiles are NYSE-precise (holiday/early-close names + "Weekend").
    ``foreign_equity`` carries only "Weekend" — its holidays are not modelled,
    so naming a weekday closure we cannot detect would be misleading.
    ``continuous`` / unknown profiles have no reason."""
    if profile in ("us_equity", "us_equity_rth"):
        return us_market_reason(d)
    if profile == "foreign_equity":
        return "Weekend" if d.weekday() >= 5 else None
    return None


def _scope_instruments(conn: psycopg.Connection[object], scope: CalendarScope) -> list[tuple[int, str, str]]:
    """``(instrument_id, symbol, session_profile)`` for the scope. Empty list is
    a valid result (no holdings / empty watchlist).

    ``watchlist`` is operator-scoped via ``sole_operator_id`` (matching the
    watchlist endpoint's single-operator posture); ``all`` applies the SAME
    operator filter to its watchlist leg so one operator's view never unions
    another's watchlist (Codex ckpt-2). ``positions`` is global."""
    params: dict[str, object] = {}
    if scope in ("watchlist", "all"):
        try:
            params["op"] = sole_operator_id(conn)
        except NoOperatorError as exc:
            raise HTTPException(status_code=503, detail="no operator configured") from exc
        except AmbiguousOperatorError as exc:
            raise HTTPException(
                status_code=409,
                detail="multiple operators present — calendar requires a per-session operator context",
            ) from exc

    if scope == "watchlist":
        source = "JOIN watchlist src ON src.instrument_id = i.instrument_id AND src.operator_id = %(op)s"
    elif scope == "portfolio":
        source = "JOIN positions src ON src.instrument_id = i.instrument_id"
    else:  # all — positions (global) ∪ this operator's watchlist
        source = (
            "JOIN (SELECT instrument_id FROM positions "
            "UNION SELECT instrument_id FROM watchlist WHERE operator_id = %(op)s"
            ") src ON src.instrument_id = i.instrument_id"
        )

    sql = f"""
        SELECT DISTINCT i.instrument_id, i.symbol, {_SESSION_PROFILE_SQL}
        FROM instruments i
        {source}
        LEFT JOIN exchanges e ON e.exchange_id = i.exchange
        ORDER BY i.symbol
    """  # noqa: S608 — _SESSION_PROFILE_SQL is a static module constant, params are bound
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(sql, params)
        return [(int(r["instrument_id"]), str(r["symbol"]), str(r["session_profile"])) for r in cur.fetchall()]


@router.get("/events", response_model=CalendarEvents)
def calendar_events(
    conn: psycopg.Connection[object] = Depends(get_conn),
    scope: CalendarScope = Query(default="portfolio"),
    days: int = Query(
        default=_DEFAULT_HORIZON_DAYS, ge=1, le=_MAX_HORIZON_DAYS, description="Horizon in days (incl. today)."
    ),
) -> CalendarEvents:
    instruments = _scope_instruments(conn, scope)
    today_ny = datetime.now(tz=_NY).date()

    # One market-status row per DISTINCT profile present in the scope.
    profiles = sorted({p for _, _, p in instruments})
    horizon_dates = [today_ny + timedelta(days=i) for i in range(days)]
    market_status = [
        MarketStatusRow(
            profile=p,
            label=_PROFILE_META.get(p, (p, "UTC", False))[0],
            timezone=_PROFILE_META.get(p, (p, "UTC", False))[1],
            holidays_modelled=_PROFILE_META.get(p, (p, "UTC", False))[2],
            week=[MarketStatusDay(date=d, day_type=_day_type(p, d), reason=_day_reason(p, d)) for d in horizon_dates],
        )
        for p in profiles
    ]

    ex_dividends: list[UpcomingExDividend] = []
    instrument_ids = [iid for iid, _, _ in instruments]
    if instrument_ids:
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                """
                SELECT i.symbol, d.instrument_id, d.ex_date, d.pay_date
                FROM dividend_events d
                JOIN instruments i ON i.instrument_id = d.instrument_id
                WHERE d.instrument_id = ANY(%(ids)s)
                  AND d.ex_date IS NOT NULL
                  AND d.ex_date >= %(today)s
                ORDER BY d.ex_date, i.symbol, d.instrument_id
                """,
                {"ids": instrument_ids, "today": today_ny},
            )
            ex_dividends = [
                UpcomingExDividend(
                    symbol=str(r["symbol"]),
                    instrument_id=int(r["instrument_id"]),
                    ex_date=r["ex_date"],
                    pay_date=r["pay_date"],
                )
                for r in cur.fetchall()
            ]

    return CalendarEvents(scope=scope, as_of=today_ny, market_status=market_status, ex_dividends=ex_dividends)
