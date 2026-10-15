"""Momentum book (#3822) — the monthly formation: read the universe, form the book, emit the month's entry signals.

``form_book`` runs on a NYSE session. When no formation exists for month t (the month before the session's month),
it reads every tradable US stock's daily closes from ``price_daily``, forms the book with
``app.services.momentum_book.target_book``, and in one transaction writes the formation, a fired ``entry``
``strategy_signals`` row for each book name not already held, and the exits for held names that left the book.
A formation that already exists is returned unchanged, so the call is idempotent and restart-safe.

The executor (``momentum_book_executor``) trades the entries; exits are closed through the position manager. This
module places no orders.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any, Final

import psycopg
from psycopg.types.json import Jsonb

from app.services import momentum_book as mb
from app.services.market_calendar import us_market_status
from app.services.price_quarantine import RULE_SET_VERSION as QUARANTINE_RULE_SET_VERSION
from app.services.strategies.validated_universe import US_EQUITY_ASSET_CLASS, resolve_stocks_type_id

logger = logging.getLogger(__name__)

Conn = psycopg.Connection[Any]

#: Recorded on every entry signal (``strategy_signals.input_rule_set_versions``).
RULE_SET_VERSIONS: Final = {"momentum_book": f"{mb.STRATEGY_ID}@{mb.STRATEGY_VERSION}"}

_CANDIDATES_SQL: Final = """
    WITH us AS (
        SELECT i.instrument_id
        FROM instruments i
        JOIN exchanges e ON e.exchange_id = i.exchange
        WHERE i.is_tradable AND e.asset_class = %(asset_class)s AND i.instrument_type_id = %(type_id)s
          AND i.canonical_instrument_id IS NULL
    )
    SELECT p.instrument_id, p.price_date, p.close::float8, COALESCE(p.volume, 0)::float8,
           row_number() OVER (PARTITION BY p.instrument_id ORDER BY p.price_date DESC) AS rn
    FROM price_daily p
    JOIN us USING (instrument_id)
    -- Fail-closed quarantine, as `price_masked_bars._LOAD_SQL`: only bars inside an evaluated range, and never a
    -- close the quarantine marks unusable.
    JOIN price_quarantine_coverage cov
      ON cov.instrument_id = p.instrument_id
     AND cov.rule_set_version = %(quarantine_version)s
     AND p.price_date BETWEEN cov.first_bar AND cov.last_bar
    LEFT JOIN price_bar_quarantine q
      ON q.instrument_id = p.instrument_id AND q.price_date = p.price_date
     AND q.rule_set_version = %(quarantine_version)s
    WHERE p.price_date >= %(start)s AND p.price_date < %(end)s
      AND p.close IS NOT NULL AND p.close > 0 AND COALESCE(q.return_usable, TRUE)
"""

# The latest usable close on or before the signal day, per entry name (the signal's fill price).
_ENTRY_CLOSES_SQL: Final = """
    SELECT DISTINCT ON (instrument_id) instrument_id, close
    FROM price_daily
    WHERE instrument_id = ANY(%s) AND price_date <= %s AND price_date > %s AND close IS NOT NULL AND close > 0
    ORDER BY instrument_id, price_date DESC
"""

# Open trades the momentum book owns, through the ownership chain (funding decision -> paper deployment).
_HELD_SQL: Final = """
    SELECT t.strategy_trade_id, t.instrument_id
    FROM strategy_trades t
    JOIN strategy_funding_decisions fd ON fd.funding_decision_id = t.funding_decision_id AND fd.verdict = 'allocated'
    JOIN strategy_deployments d ON d.deployment_id = fd.deployment_id AND d.mode = 'paper'
    WHERE d.strategy_id = %(strategy_id)s AND d.strategy_version = %(strategy_version)s
      AND t.status NOT IN ('closed', 'failed')
    ORDER BY t.strategy_trade_id
"""


def month_start(day: date) -> date:
    return day.replace(day=1)


def formation_month_for(session: date) -> date:
    """Month t for a session: the month before the session's month, as its first day."""
    return month_start(month_start(session) - timedelta(days=1))


def load_candidates(conn: Conn, *, formation_month: date) -> list[mb.Candidate]:
    """Every tradable canonical US stock with its month-end closes for months t-12..t and its dollar volume over
    the last ``ADV_SESSIONS`` sessions to the end of month t."""
    end = month_start(formation_month + timedelta(days=32))  # first day after month t
    start = month_start(formation_month - timedelta(days=13 * 31))  # covers month t-12
    rows = conn.execute(
        _CANDIDATES_SQL,
        {
            "asset_class": US_EQUITY_ASSET_CLASS,
            "type_id": resolve_stocks_type_id(conn),
            "start": start,
            "end": end,
            "quarantine_version": QUARANTINE_RULE_SET_VERSION,
        },
    ).fetchall()
    closes: dict[int, dict[int, tuple[date, float]]] = defaultdict(dict)
    dollar: dict[int, list[float]] = defaultdict(list)
    for instrument_id, day, close, volume, rn in rows:
        key = mb.month_key(day)
        prior = closes[instrument_id].get(key)
        if prior is None or day > prior[0]:
            closes[instrument_id][key] = (day, close)
        if rn <= mb.ADV_SESSIONS:
            dollar[instrument_id].append(close * volume)
    return [
        mb.Candidate(
            instrument_id=iid,
            adv=sum(dollar[iid]) / len(dollar[iid]),
            month_closes={k: v[1] for k, v in by_month.items()},
        )
        for iid, by_month in closes.items()
        if dollar[iid]
    ]


@dataclass(frozen=True)
class Formation:
    formation_id: int
    formation_month: date
    target_session: date
    book: tuple[int, ...]
    entry_signal_ids: tuple[int, ...]
    exit_trade_ids: tuple[int, ...]
    created: bool


def _existing(conn: Conn, formation_month: date) -> Formation | None:
    row = conn.execute(
        """
        SELECT formation_id, target_session, book FROM momentum_book_formations
        WHERE strategy_version = %s AND formation_month = %s
        """,
        (mb.STRATEGY_VERSION, formation_month),
    ).fetchone()
    if row is None:
        return None
    formation_id = int(row[0])
    entries = conn.execute(
        "SELECT signal_id FROM momentum_book_entries WHERE formation_id = %s ORDER BY rank", (formation_id,)
    ).fetchall()
    exits = conn.execute(
        "SELECT strategy_trade_id FROM momentum_book_exits WHERE formation_id = %s ORDER BY strategy_trade_id",
        (formation_id,),
    ).fetchall()
    return Formation(
        formation_id=formation_id,
        formation_month=formation_month,
        target_session=row[1],
        book=tuple(int(x["instrument_id"]) for x in row[2]),
        entry_signal_ids=tuple(int(r[0]) for r in entries),
        exit_trade_ids=tuple(int(r[0]) for r in exits),
        created=False,
    )


def _last_session_before(day: date) -> date:
    d = day - timedelta(days=1)
    while us_market_status(d) == "closed":
        d -= timedelta(days=1)
    return d


def form_book(conn: Conn, *, session: date) -> Formation | None:
    """Form month t's book for ``session`` (a NY date), or return the existing formation. ``None`` when ``session``
    is not a NYSE session or no candidate has the history the rule needs."""
    if us_market_status(session) == "closed":
        return None
    formation_month = formation_month_for(session)
    existing = _existing(conn, formation_month)
    conn.commit()
    if existing is not None:
        return existing

    candidates = load_candidates(conn, formation_month=formation_month)
    conn.commit()
    t = mb.month_key(formation_month)
    book = mb.target_book(candidates, t)
    if not book:
        logger.warning("momentum book: no scorable candidate for %s (%d candidates)", formation_month, len(candidates))
        return None
    scores = {
        c.instrument_id: mb.score(r)
        for c in candidates
        if c.instrument_id in book and (r := mb.formation_returns(c.month_closes, t)) is not None
    }
    signal_day = _last_session_before(session)
    # Every book name's fill price is resolved BEFORE the formation is written: a formation is immutable, so an
    # entry dropped here would stay dropped for the month (Codex ckpt-2 P1). A missing close forms nothing; the
    # next fire retries.
    closes = {
        int(iid): close
        for iid, close in conn.execute(
            _ENTRY_CLOSES_SQL, (list(book), signal_day, signal_day - timedelta(days=7))
        ).fetchall()
    }
    conn.commit()
    missing = [iid for iid in book if iid not in closes]
    if missing:
        logger.warning("momentum book: no close by %s for %s; formation deferred", signal_day, missing)
        return None

    with conn.transaction():
        held = conn.execute(
            _HELD_SQL, {"strategy_id": mb.STRATEGY_ID, "strategy_version": mb.STRATEGY_VERSION}
        ).fetchall()
        held_ids = [int(r[1]) for r in held]
        plan = mb.rebalance_plan(book, held_ids)
        row = conn.execute(
            """
            INSERT INTO momentum_book_formations
                (strategy_version, formation_month, target_session, candidates, book, held)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (strategy_version, formation_month) DO NOTHING
            RETURNING formation_id
            """,
            (
                mb.STRATEGY_VERSION,
                formation_month,
                session,
                len(candidates),
                Jsonb([{"instrument_id": iid, "rank": k, "score": scores[iid]} for k, iid in enumerate(book, start=1)]),
                Jsonb(sorted(held_ids)),
            ),
        ).fetchone()
        if row is None:  # a concurrent formation won the race
            return _existing(conn, formation_month)
        formation_id = int(row[0])
        signal_ids: list[int] = []
        for instrument_id in plan.entries:
            sid = conn.execute(
                """
                INSERT INTO strategy_signals (
                    strategy_id, strategy_version, instrument_id, signal_bar_date, signal_kind, verdict,
                    fill_bar_date, fill_price, universe, input_rule_set_versions
                ) VALUES (%s, %s, %s, %s, 'entry', 'fired', %s, %s, 'survivor_only', %s)
                RETURNING signal_id
                """,
                (
                    mb.STRATEGY_ID,
                    mb.STRATEGY_VERSION,
                    instrument_id,
                    signal_day,
                    session,
                    closes[instrument_id],
                    Jsonb(RULE_SET_VERSIONS),
                ),
            ).fetchone()
            assert sid is not None
            conn.execute(
                "INSERT INTO momentum_book_entries (signal_id, formation_id, instrument_id, rank) "
                "VALUES (%s, %s, %s, %s)",
                (int(sid[0]), formation_id, instrument_id, book.index(instrument_id) + 1),
            )
            signal_ids.append(int(sid[0]))
        exit_trades = [(int(tid), int(iid)) for tid, iid in held if int(iid) in set(plan.exits)]
        for trade_id, instrument_id in exit_trades:
            conn.execute(
                "INSERT INTO momentum_book_exits (formation_id, strategy_trade_id, instrument_id) VALUES (%s, %s, %s)",
                (formation_id, trade_id, instrument_id),
            )
    logger.info(
        "momentum book: formed %s for session %s — %d names, %d entries, %d exits",
        formation_month,
        session,
        len(book),
        len(signal_ids),
        len(exit_trades),
    )
    return Formation(
        formation_id=formation_id,
        formation_month=formation_month,
        target_session=session,
        book=book,
        entry_signal_ids=tuple(signal_ids),
        exit_trade_ids=tuple(t for t, _ in exit_trades),
        created=True,
    )


__all__ = ["Formation", "form_book", "formation_month_for", "load_candidates", "month_start"]
