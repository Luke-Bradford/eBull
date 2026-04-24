"""eToro WebSocket live-price subscriber (#274 Slice 1).

Connects to ``wss://ws.etoro.com/ws``, authenticates with the
operator's eToro API + user keys, subscribes to ``instrument:<id>``
topics for every instrument the operator currently holds OR has on
their watchlist, and upserts each ``Trading.Instrument.Rate`` push
into the existing ``quotes`` table.

**Slice 1 scope** (deliberately tight per operator simplicity ask):

- Single-instance dev assumption: one process owns the WS connection.
  No advisory-lock multi-worker dance — that's a Slice 4 concern when
  the app actually runs multi-worker in prod.
- Quotes-only. ``private`` topic / position-event handling is Slice 2.
- ``quotes`` table writes only. SSE / Redis fan-out is Slice 3.
- Frontend continues to poll the existing ``/quotes`` endpoints with
  React Query. The 5-second client cadence + WS-driven SQL freshness
  combine into the "few-second live price" experience the operator
  asked for.

Reconnect policy: any I/O error or close triggers a 5-second backoff
then re-authenticate + re-subscribe. The set of topics is recomputed
on every reconnect so a freshly-opened position / watchlist add is
picked up after at most one reconnect cycle, even if the first ever
connect happened before that change.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any

import psycopg
import psycopg_pool
import websockets
from websockets.asyncio.client import ClientConnection

logger = logging.getLogger(__name__)


_WS_URL = "wss://ws.etoro.com/ws"
_RECONNECT_BACKOFF_S = 5.0


# ---------------------------------------------------------------------
# Pure helpers — unit tested without WS mocks
# ---------------------------------------------------------------------


@dataclass(frozen=True)
class QuoteUpdate:
    """Normalised tick — what the rate-message parser emits and the
    DB upsert consumes."""

    instrument_id: int
    bid: Decimal
    ask: Decimal
    last: Decimal | None
    quoted_at: datetime


def build_auth_message(api_key: str, user_key: str) -> str:
    """Compose the ``Authenticate`` op JSON sent on every (re)connect."""
    return json.dumps(
        {
            "id": str(uuid.uuid4()),
            "operation": "Authenticate",
            "data": {"apiKey": api_key, "userKey": user_key},
        }
    )


def build_subscribe_message(instrument_ids: list[int]) -> str | None:
    """Compose the ``Subscribe`` op JSON for a list of instrument IDs.

    Returns ``None`` when the list is empty so callers don't send a
    no-op subscription that eToro might reject.
    """
    if not instrument_ids:
        return None
    topics = [f"instrument:{iid}" for iid in instrument_ids]
    return json.dumps(
        {
            "id": str(uuid.uuid4()),
            "operation": "Subscribe",
            "data": {"topics": topics, "snapshot": True},
        }
    )


def parse_rate_message(raw: str) -> QuoteUpdate | None:
    """Parse a ``Trading.Instrument.Rate`` push.

    eToro's WS protocol wraps each push in an envelope:
    ``{type: "Trading.Instrument.Rate", data: {InstrumentID, Bid,
    Ask, LastExecution, Date, ...}}``. Returns ``None`` for any other
    message type or any field-shape failure — callers continue
    listening rather than failing the whole connection.
    """
    try:
        msg = json.loads(raw)
    except json.JSONDecodeError:
        return None
    if not isinstance(msg, dict):
        return None
    if msg.get("type") != "Trading.Instrument.Rate":
        return None
    data = msg.get("data")
    if not isinstance(data, dict):
        return None
    try:
        instrument_id = int(data["InstrumentID"])
        bid = Decimal(str(data["Bid"]))
        ask = Decimal(str(data["Ask"]))
        last_raw = data.get("LastExecution")
        last = Decimal(str(last_raw)) if last_raw is not None else None
        date_str = str(data["Date"])
        # eToro's ISO date includes 'Z' suffix; normalise to UTC.
        if date_str.endswith("Z"):
            date_str = date_str[:-1] + "+00:00"
        quoted_at = datetime.fromisoformat(date_str)
    except KeyError, TypeError, ValueError:
        return None
    return QuoteUpdate(
        instrument_id=instrument_id,
        bid=bid,
        ask=ask,
        last=last,
        quoted_at=quoted_at,
    )


def _compute_spread_pct(bid: Decimal, ask: Decimal) -> Decimal | None:
    """Mid-spread percentage. Matches the existing service-layer
    convention so quotes from the WS path stay comparable to the
    REST-poll path."""
    if bid <= 0 or ask <= 0:
        return None
    mid = (bid + ask) / Decimal(2)
    if mid <= 0:
        return None
    return ((ask - bid) / mid) * Decimal(100)


# ---------------------------------------------------------------------
# DB upsert
# ---------------------------------------------------------------------


_UPSERT_SQL = """
INSERT INTO quotes (instrument_id, quoted_at, bid, ask, last, spread_pct, spread_flag)
VALUES (%(instrument_id)s, %(quoted_at)s, %(bid)s, %(ask)s, %(last)s, %(spread_pct)s, FALSE)
ON CONFLICT (instrument_id) DO UPDATE SET
    quoted_at  = EXCLUDED.quoted_at,
    bid        = EXCLUDED.bid,
    ask        = EXCLUDED.ask,
    last       = EXCLUDED.last,
    spread_pct = EXCLUDED.spread_pct
WHERE quotes.quoted_at IS NULL OR EXCLUDED.quoted_at >= quotes.quoted_at
"""


def upsert_quote(conn: psycopg.Connection[Any], update: QuoteUpdate) -> None:
    """Upsert one tick into the ``quotes`` table.

    The WHERE clause guards against an out-of-order arrival
    overwriting a fresher tick that beat it through the network —
    rare but possible across reconnects when the WS replay overlaps
    the live stream.
    """
    spread_pct = _compute_spread_pct(update.bid, update.ask)
    conn.execute(
        _UPSERT_SQL,
        {
            "instrument_id": update.instrument_id,
            "quoted_at": update.quoted_at,
            "bid": update.bid,
            "ask": update.ask,
            "last": update.last,
            "spread_pct": spread_pct,
        },
    )


# ---------------------------------------------------------------------
# Watched-instruments selector
# ---------------------------------------------------------------------


def fetch_watched_instrument_ids(conn: psycopg.Connection[Any]) -> list[int]:
    """Return the set of instrument IDs the WS subscriber should
    subscribe to: held positions ∪ watchlist.

    The eBull schema stores eToro's native integer instrument id
    directly in ``instruments.instrument_id`` (see the universe
    upsert in ``app.services.universe`` which writes
    ``INSERT ... VALUES (%(provider_id)s, ...)`` into the
    ``instrument_id`` column). So the same integer that the WS
    ``instrument:<id>`` topic expects is what's already on the
    parent + child tables — no JOIN to ``external_identifiers``
    needed.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT instrument_id FROM (
                SELECT instrument_id FROM broker_positions
                UNION
                SELECT instrument_id FROM watchlist
            ) AS u
            """,
        )
        return [int(row[0]) for row in cur.fetchall() if row[0] is not None]


# ---------------------------------------------------------------------
# Subscriber lifecycle
# ---------------------------------------------------------------------


class EtoroWebSocketSubscriber:
    """Lifespan-managed coroutine that holds the WS connection.

    ``start()`` launches the listen loop as an asyncio task; ``stop()``
    cancels it. The internal loop reconnects on any error after a
    short backoff.
    """

    def __init__(
        self,
        *,
        api_key: str,
        user_key: str,
        pool: psycopg_pool.ConnectionPool[Any],
        watched_ids_provider: Callable[[], list[int]] | None = None,
    ) -> None:
        self._api_key = api_key
        self._user_key = user_key
        self._pool = pool
        # Default selector hits the DB; tests inject a stub.
        self._watched_ids_provider = watched_ids_provider or self._default_watched_ids
        self._task: asyncio.Task[None] | None = None
        self._stop_event = asyncio.Event()

    def _default_watched_ids(self) -> list[int]:
        with self._pool.connection() as conn:
            return fetch_watched_instrument_ids(conn)

    def _sync_upsert(self, update: QuoteUpdate) -> None:
        """Sync helper offloaded to a worker thread per tick so the
        event loop never blocks on a DB round-trip. Both
        ``pool.connection()`` (a sync context manager) and the
        ``conn.execute`` it yields run inside ``asyncio.to_thread``.
        """
        with self._pool.connection() as conn:
            upsert_quote(conn, update)
            conn.commit()

    async def start(self) -> None:
        if self._task is not None:
            return
        self._stop_event.clear()
        self._task = asyncio.create_task(self._run(), name="etoro-ws-subscriber")
        logger.info("EtoroWebSocketSubscriber: started")

    async def stop(self) -> None:
        if self._task is None:
            return
        self._stop_event.set()
        self._task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._task
        self._task = None
        logger.info("EtoroWebSocketSubscriber: stopped")

    async def _run(self) -> None:
        while not self._stop_event.is_set():
            try:
                await self._connect_and_listen()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.warning(
                    "EtoroWebSocketSubscriber: connection error — backoff %.1fs then reconnect",
                    _RECONNECT_BACKOFF_S,
                    exc_info=True,
                )
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=_RECONNECT_BACKOFF_S)
                # Stop signalled during backoff — exit cleanly.
                return
            except TimeoutError:
                continue

    async def _connect_and_listen(self) -> None:
        async with websockets.connect(_WS_URL) as ws:
            await ws.send(build_auth_message(self._api_key, self._user_key))
            # Drain the auth response — eToro replies with
            # {"success": true} or an error envelope. We wait for one
            # frame so a bad key surfaces immediately rather than
            # silently looping subscribe attempts.
            auth_reply = await asyncio.wait_for(ws.recv(), timeout=10.0)
            if not _is_auth_success(auth_reply):
                raise RuntimeError(f"eToro WS auth failed: {auth_reply!r}")

            # Selector hits the DB; offload to a worker thread so
            # the connect path doesn't block the event loop.
            ids = await asyncio.to_thread(self._watched_ids_provider)
            sub_msg = build_subscribe_message(ids)
            if sub_msg is not None:
                await ws.send(sub_msg)
                logger.info(
                    "EtoroWebSocketSubscriber: subscribed to %d instrument topics",
                    len(ids),
                )
            else:
                logger.info(
                    "EtoroWebSocketSubscriber: no watched instruments — "
                    "connection will idle until a position / watchlist add"
                )

            await self._listen(ws)

    async def _listen(self, ws: ClientConnection) -> None:
        async for raw in ws:
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8", errors="ignore")
            update = parse_rate_message(raw)
            if update is None:
                continue
            try:
                # ``pool.connection()`` is sync — calling it from the
                # event loop would block the loop for the full DB
                # round-trip on every tick. Offload to a worker
                # thread so the WS read loop stays hot.
                await asyncio.to_thread(self._sync_upsert, update)
            except Exception:
                logger.warning(
                    "EtoroWebSocketSubscriber: upsert failed instrument_id=%d",
                    update.instrument_id,
                    exc_info=True,
                )


def _is_auth_success(raw: str | bytes) -> bool:
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", errors="ignore")
    try:
        msg = json.loads(raw)
    except json.JSONDecodeError, TypeError:
        return False
    return isinstance(msg, dict) and bool(msg.get("success"))
