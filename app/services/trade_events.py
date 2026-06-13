"""Trade-events ledger ingest (#1593).

Append-only ``trade_events`` ledger of broker-observed position opens
and closes. Spec: docs/proposals/etl/2026-06-13-etoro-trade-ledger.md.

Writer topology: ``sync_portfolio`` is the ONLY writer — open events
from the portfolio-payload positions, open+close events from the
trade-history rows its callers fetch and pass in. The eBull order path
never writes here (its broker_positions rows carry synthetic negative
position ids, #227); it enqueues an immediate sync instead.

Units contract (spec §1.7): an open event's ``units`` is the position's
ORIGINAL opened size; each close event's ``units`` is that slice's
delta. eToro partial closes reduce the same positionId, so a position
may carry one open and N closes. ``conflict_anomaly`` counts every
observation that violates this model — loud, never silently merged.

Rows are immutable: conflicts are ``ON CONFLICT DO NOTHING`` against
the two partial unique indexes (sql/194); first observation wins.
Portfolio-sourced opens are preferred within a batch because they carry
``initialUnits`` (history-derived opens are best-effort Σ-of-slices).
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any, Literal

import httpx
import psycopg
import psycopg.rows
from psycopg.types.json import Jsonb

from app.providers.broker import BrokerClosedTrade, BrokerPosition, BrokerProvider
from app.providers.implementations.etoro_broker import TradeHistoryParseError
from app.services.sync_orchestrator.dispatcher import publish_manual_job_request_with_conn

logger = logging.getLogger(__name__)

EventKind = Literal["open", "close"]
EventSide = Literal["buy", "sell"]
EventSource = Literal["etoro_sync", "etoro_history"]

# Pre-dates any eToro retail account we could hold; the deep-backfill
# minDate for an empty ledger.
HISTORY_EPOCH = datetime(2017, 1, 1, tzinfo=UTC)

# Re-fetch window behind the watermark — absorbs clock skew and
# late-arriving rows; the unique indexes make the overlap idempotent.
_WATERMARK_OVERLAP = timedelta(days=7)


@dataclass(frozen=True)
class TradeEvent:
    """One broker-observed open or close, ready for insertion."""

    position_id: int
    etoro_instrument_id: int
    event_kind: EventKind
    side: EventSide
    units: Decimal
    price: Decimal | None
    executed_at: datetime
    source: EventSource
    raw_payload: dict[str, Any]
    fees_usd: Decimal | None = None
    realized_pnl_usd: Decimal | None = None
    investment_usd: Decimal | None = None
    order_id: int | None = None
    social_trade_id: int | None = None
    parent_position_id: int | None = None


@dataclass
class TradeEventCounters:
    """Closed-set ingest counters (spec §15). Logged by sync_portfolio."""

    inserted: int = 0
    duplicate: int = 0
    unresolved_instrument: int = 0
    null_price: int = 0
    conflict_anomaly: int = 0
    skipped_other: int = 0
    skip_reasons: list[str] = field(default_factory=list)

    def log_line(self) -> str:
        line = (
            f"inserted={self.inserted} duplicate={self.duplicate} "
            f"unresolved_instrument={self.unresolved_instrument} "
            f"null_price={self.null_price} "
            f"conflict_anomaly={self.conflict_anomaly} "
            f"other={self.skipped_other}"
        )
        if self.skip_reasons:
            line += f" reasons={self.skip_reasons}"
        return line


def _side(kind: EventKind, is_buy: bool) -> EventSide:
    """Open leg of a long is a buy and its close a sell; shorts invert."""
    if kind == "open":
        return "buy" if is_buy else "sell"
    return "sell" if is_buy else "buy"


def _positive_or_none(value: Decimal | None) -> Decimal | None:
    """Sentinel guard: a non-positive rate is NOT a valid mark (prevention log).

    Pure mapping — the ``null_price`` counter is incremented at INGEST
    time for rows that actually land, so merge-dropped duplicates never
    inflate it (Codex ckpt-2 MED).
    """
    if value is None or value <= 0:
        return None
    return value


def open_events_from_positions(
    positions: Sequence[BrokerPosition],
    counters: TradeEventCounters,
) -> list[TradeEvent]:
    """Open events for every real-id position in the portfolio payload.

    Idempotent against the ledger (DO NOTHING on the per-position open
    key), so every sync re-emits all opens rather than diffing —
    simpler, and heals any gap. Synthetic negative ids (#227) and
    id-less legacy rows are excluded by construction.

    Fees are NOT stamped on open events: the position's ``totalFees``
    accumulates over its whole life and the authoritative per-trade fee
    arrives on the history close row — stamping both would double-count
    for consumers. The raw payload keeps the running figure.
    """
    events: list[TradeEvent] = []
    for bp in positions:
        if bp.position_id is None or bp.position_id < 0:
            continue
        if bp.open_date_time is None:
            # Cannot fabricate an executed_at (external timestamps must
            # come from the API response, never now() — prevention log).
            counters.skipped_other += 1
            counters.skip_reasons.append(f"open_event position {bp.position_id}: no openDateTime in payload")
            continue
        units = bp.initial_units if bp.initial_units is not None and bp.initial_units > 0 else bp.units
        if units <= 0:
            counters.skipped_other += 1
            counters.skip_reasons.append(f"open_event position {bp.position_id}: non-positive units")
            continue
        events.append(
            TradeEvent(
                position_id=bp.position_id,
                etoro_instrument_id=bp.instrument_id,
                event_kind="open",
                side=_side("open", bp.is_buy),
                units=units,
                price=_positive_or_none(bp.open_price),
                executed_at=bp.open_date_time,
                source="etoro_sync",
                raw_payload=bp.raw_payload,
                investment_usd=bp.initial_amount_in_dollars if bp.initial_amount_in_dollars > 0 else None,
            )
        )
    return events


def events_from_history(
    trades: Sequence[BrokerClosedTrade],
    counters: TradeEventCounters,
) -> list[TradeEvent]:
    """Transform history rows into events: one close per slice plus one
    synthesized open per position (Σ slice units, earliest open leg).

    The synthesized open is only a fallback for positions the ledger
    never saw open in a portfolio payload — merge_events() prefers a
    portfolio-sourced open, and the DB open key keeps whichever landed
    first thereafter.
    """
    by_position: dict[int, list[BrokerClosedTrade]] = {}
    for t in trades:
        if t.position_id < 0:
            # Never observed in probes; guard matches the ledger CHECK.
            counters.skipped_other += 1
            counters.skip_reasons.append(f"history row position {t.position_id}: negative id")
            continue
        by_position.setdefault(t.position_id, []).append(t)

    events: list[TradeEvent] = []
    for position_id, rows in by_position.items():
        rows.sort(key=lambda t: t.open_timestamp)
        earliest = rows[0]
        total_units = sum((t.units for t in rows), Decimal("0"))
        if total_units > 0:
            events.append(
                TradeEvent(
                    position_id=position_id,
                    etoro_instrument_id=earliest.instrument_id,
                    event_kind="open",
                    side=_side("open", earliest.is_buy),
                    units=total_units,
                    price=_positive_or_none(earliest.open_rate),
                    executed_at=earliest.open_timestamp,
                    source="etoro_history",
                    raw_payload=earliest.raw_payload,
                    investment_usd=earliest.initial_investment,
                    order_id=earliest.order_id,
                    social_trade_id=earliest.social_trade_id,
                    parent_position_id=earliest.parent_position_id,
                )
            )
        for t in rows:
            if t.units <= 0:
                counters.skipped_other += 1
                counters.skip_reasons.append(f"history row position {position_id}: non-positive units")
                continue
            events.append(
                TradeEvent(
                    position_id=position_id,
                    etoro_instrument_id=t.instrument_id,
                    event_kind="close",
                    side=_side("close", t.is_buy),
                    units=t.units,
                    price=_positive_or_none(t.close_rate),
                    executed_at=t.close_timestamp,
                    source="etoro_history",
                    raw_payload=t.raw_payload,
                    fees_usd=t.fees,
                    realized_pnl_usd=t.net_profit,
                    investment_usd=t.investment,
                    order_id=t.order_id,
                    social_trade_id=t.social_trade_id,
                    parent_position_id=t.parent_position_id,
                )
            )
    return events


def merge_events(
    portfolio_opens: Sequence[TradeEvent],
    history_events: Sequence[TradeEvent],
) -> list[TradeEvent]:
    """Combine the two streams, preferring a portfolio-sourced open for
    any position present in both (it carries initialUnits; the
    history-derived open is Σ-of-slices best-effort)."""
    open_positions_covered = {e.position_id for e in portfolio_opens}
    merged = list(portfolio_opens)
    for e in history_events:
        if e.event_kind == "open" and e.position_id in open_positions_covered:
            continue
        merged.append(e)
    return merged


def compute_history_min_date(conn: psycopg.Connection[Any]) -> datetime:
    """minDate for the next trade-history fetch.

    The endpoint filters on closeTimestamp (probed, spec §0), so the
    max ingested close is the correct watermark; the overlap re-walks a
    window the unique indexes dedupe. Empty ledger → deep backfill.
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        row = cur.execute(
            """
            SELECT MAX(executed_at) AS watermark
            FROM trade_events
            WHERE event_kind = 'close' AND source = 'etoro_history'
            """
        ).fetchone()
    watermark = row["watermark"] if row else None
    if watermark is None:
        return HISTORY_EPOCH
    return watermark - _WATERMARK_OVERLAP


_INSERT_OPEN_SQL = """
    INSERT INTO trade_events
        (position_id, etoro_instrument_id, instrument_id, event_kind, side,
         units, price, executed_at, fees_usd, realized_pnl_usd,
         investment_usd, order_id, social_trade_id, parent_position_id,
         source, raw_payload)
    VALUES
        (%(position_id)s, %(etoro_instrument_id)s, %(instrument_id)s,
         %(event_kind)s, %(side)s, %(units)s, %(price)s, %(executed_at)s,
         %(fees_usd)s, %(realized_pnl_usd)s, %(investment_usd)s,
         %(order_id)s, %(social_trade_id)s, %(parent_position_id)s,
         %(source)s, %(raw_payload)s)
    ON CONFLICT (position_id) WHERE event_kind = 'open' DO NOTHING
"""

_INSERT_CLOSE_SQL = """
    INSERT INTO trade_events
        (position_id, etoro_instrument_id, instrument_id, event_kind, side,
         units, price, executed_at, fees_usd, realized_pnl_usd,
         investment_usd, order_id, social_trade_id, parent_position_id,
         source, raw_payload)
    VALUES
        (%(position_id)s, %(etoro_instrument_id)s, %(instrument_id)s,
         %(event_kind)s, %(side)s, %(units)s, %(price)s, %(executed_at)s,
         %(fees_usd)s, %(realized_pnl_usd)s, %(investment_usd)s,
         %(order_id)s, %(social_trade_id)s, %(parent_position_id)s,
         %(source)s, %(raw_payload)s)
    ON CONFLICT (position_id, executed_at) WHERE event_kind = 'close' DO NOTHING
"""


def ingest_trade_events(
    conn: psycopg.Connection[Any],
    events: Sequence[TradeEvent],
    counters: TradeEventCounters,
) -> TradeEventCounters:
    """Insert events; immutable semantics (DO NOTHING + loud anomalies).

    Runs on the caller's transaction — never commits (sync_portfolio's
    contract). All I/O happened before this call; pure DB writes here.
    """
    if not events:
        return counters

    distinct_ids = sorted({e.etoro_instrument_id for e in events})
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        rows = cur.execute(
            "SELECT instrument_id FROM instruments WHERE instrument_id = ANY(%s)",
            [distinct_ids],
        ).fetchall()
    known_instruments = {row["instrument_id"] for row in rows}

    touched_positions: set[int] = set()
    for event in events:
        resolved = event.etoro_instrument_id if event.etoro_instrument_id in known_instruments else None
        if resolved is None:
            counters.unresolved_instrument += 1
        params = {
            "position_id": event.position_id,
            "etoro_instrument_id": event.etoro_instrument_id,
            "instrument_id": resolved,
            "event_kind": event.event_kind,
            "side": event.side,
            "units": event.units,
            "price": event.price,
            "executed_at": event.executed_at,
            "fees_usd": event.fees_usd,
            "realized_pnl_usd": event.realized_pnl_usd,
            "investment_usd": event.investment_usd,
            "order_id": event.order_id,
            "social_trade_id": event.social_trade_id,
            "parent_position_id": event.parent_position_id,
            "source": event.source,
            "raw_payload": Jsonb(event.raw_payload),
        }
        sql = _INSERT_OPEN_SQL if event.event_kind == "open" else _INSERT_CLOSE_SQL
        result = conn.execute(sql, params)
        if result.rowcount == 1:
            counters.inserted += 1
            if event.price is None:
                # Landed with NULL price (absent or sentinel rate) —
                # counted here, not at transform time, so merge-dropped
                # duplicates never inflate the figure (spec §15).
                counters.null_price += 1
            touched_positions.add(event.position_id)
            continue
        # Conflict: expected duplicate unless the stored row disagrees
        # on the financial figures — that is an anomaly worth shouting
        # about (advisory check; race-tolerant, logging only).
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            existing = cur.execute(
                """
                SELECT units, realized_pnl_usd
                FROM trade_events
                WHERE position_id = %(pid)s AND event_kind = %(kind)s
                  AND (%(kind)s = 'open' OR executed_at = %(executed_at)s)
                ORDER BY event_id
                LIMIT 1
                """,
                {"pid": event.position_id, "kind": event.event_kind, "executed_at": event.executed_at},
            ).fetchone()
        if existing is None:
            # Row vanished between INSERT and SELECT — impossible for an
            # append-only table outside test truncation; count loudly.
            counters.conflict_anomaly += 1
            logger.warning(
                "trade_events: conflict with no existing row (position %d %s) — investigate",
                event.position_id,
                event.event_kind,
            )
            continue
        same_units = Decimal(str(existing["units"])) == event.units
        same_pnl = (existing["realized_pnl_usd"] is None and event.realized_pnl_usd is None) or (
            existing["realized_pnl_usd"] is not None
            and event.realized_pnl_usd is not None
            and Decimal(str(existing["realized_pnl_usd"])) == event.realized_pnl_usd
        )
        if same_units and (event.event_kind == "open" or same_pnl):
            counters.duplicate += 1
        else:
            counters.conflict_anomaly += 1
            logger.warning(
                "trade_events: %s event for position %d conflicts with stored figures "
                "(stored units=%s pnl=%s, incoming units=%s pnl=%s) — first observation kept; "
                "incoming payload: %s",
                event.event_kind,
                event.position_id,
                existing["units"],
                existing["realized_pnl_usd"],
                event.units,
                event.realized_pnl_usd,
                event.raw_payload,
            )

    _check_close_sum_invariant(conn, touched_positions, counters)
    return counters


def _check_close_sum_invariant(
    conn: psycopg.Connection[Any],
    position_ids: set[int],
    counters: TradeEventCounters,
) -> None:
    """Σ(close units) must not exceed the open's units (spec §4)."""
    if not position_ids:
        return
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        rows = cur.execute(
            """
            SELECT o.position_id, o.units AS open_units,
                   SUM(c.units) AS closed_units
            FROM trade_events o
            JOIN trade_events c
              ON c.position_id = o.position_id AND c.event_kind = 'close'
            WHERE o.event_kind = 'open'
              AND o.position_id = ANY(%s)
            GROUP BY o.position_id, o.units
            HAVING SUM(c.units) > o.units
            """,
            [sorted(position_ids)],
        ).fetchall()
    for row in rows:
        counters.conflict_anomaly += 1
        logger.warning(
            "trade_events: position %d closes sum to %s > open units %s — "
            "partial-close model violation, investigate (spec §22.1)",
            row["position_id"],
            row["closed_units"],
            row["open_units"],
        )


# Must equal scheduler.JOB_DAILY_PORTFOLIO_SYNC — duplicated here because
# importing the scheduler would be circular (it imports this module).
# Pinned by tests/test_trade_events.py::test_post_trade_sync_job_name.
POST_TRADE_SYNC_JOB = "daily_portfolio_sync"


def enqueue_post_trade_sync(conn: psycopg.Connection[Any], *, requested_by: str) -> None:
    """Queue an immediate portfolio sync after a successful fill so the
    trade lands in the ledger within seconds instead of the next 5-min
    tick (#1593 spec §1.3). Runs on the caller's transaction — NOTIFY
    fires at commit, so the jobs process wakes only once the fill row
    is durable. If the enqueue is lost, the scheduled tick covers it.
    """
    publish_manual_job_request_with_conn(conn, POST_TRADE_SYNC_JOB, requested_by=requested_by)


def fetch_trade_history_safely(
    broker: BrokerProvider,
    min_date: datetime,
) -> Sequence[BrokerClosedTrade] | None:
    """Fetch history with the spec §7 error posture; None = skip ingest.

    A failed fetch must never block the position sync: 4xx request-shape
    or permission regressions are loud ERRORs (deterministic — fix code
    or key scopes); transient network/429/5xx are WARNINGs. Either way
    the watermark is untouched, so the next sync re-covers the window.
    """
    try:
        return broker.get_trade_history(min_date)
    except httpx.HTTPStatusError as exc:
        log = logger.error if exc.response.status_code in (400, 403) else logger.warning
        log(
            "trade history fetch failed (HTTP %d) — positions still sync, watermark unchanged",
            exc.response.status_code,
            exc_info=True,
        )
        return None
    except httpx.HTTPError:
        logger.warning(
            "trade history fetch failed (network) — positions still sync, watermark unchanged",
            exc_info=True,
        )
        return None
    except TradeHistoryParseError:
        logger.error(
            "trade history response failed to parse — deterministic, fix the parser; "
            "positions still sync, watermark unchanged",
            exc_info=True,
        )
        return None


def record_trade_events(
    conn: psycopg.Connection[Any],
    positions: Sequence[BrokerPosition],
    trade_history: Sequence[BrokerClosedTrade] | None,
) -> TradeEventCounters:
    """Single entry point used by sync_portfolio."""
    counters = TradeEventCounters()
    opens = open_events_from_positions(positions, counters)
    # None = fetch failed (skip history ingest); [] = fetch succeeded
    # with no rows. Identical output today, but the distinction is the
    # §7 contract — keep it explicit.
    if trade_history is not None:
        merged = merge_events(opens, events_from_history(trade_history, counters))
    else:
        merged = opens
    return ingest_trade_events(conn, merged, counters)
