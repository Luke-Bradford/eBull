"""eToro WebSocket live-price subscriber.

Connects to ``wss://ws.etoro.com/ws``, authenticates with the
operator's eToro API + user keys, and subscribes to
``instrument:<id>`` topics on demand from page-view SSE streams.
Each ``Trading.Instrument.Rate`` push is upserted into the
``quotes`` table and fanned out via :class:`QuoteBus` to any
subscribed SSE consumer.

**Visibility-driven subscription model** (#498):
The subscriber holds no opinion about which instruments to stream.
``add_instruments`` and ``remove_instruments`` callers (the SSE
endpoint at :func:`app.api.sse_quotes._event_stream`) bump and drop
ref counts on ``_topic_refs``; a topic is sent to eToro iff its
refcount > 0. Held positions and watchlist entries are **not**
auto-subscribed — what the operator has on screen drives the
upstream subscription, nothing else. Boots quiet: a fresh process
authenticates the WS but sends no Subscribe frame until an SSE
stream lands.

**Private channel + reconcile**:
The subscriber also subscribes to the ``private`` topic. eToro
pushes ``Trading.OrderFor*`` / ``Trading.Position*`` /
``Trading.Credit*`` envelopes here whenever the operator's
portfolio state changes. Each private push schedules a debounced
REST reconcile — ``EtoroBrokerProvider.get_portfolio()`` followed
by ``sync_portfolio()`` against the live DB. Multi-leg trades and
rapid order bursts collapse into one reconcile per
``_RECONCILE_DEBOUNCE_S`` window so the public REST limit
(60 GET/min) is respected even when the private firehose is noisy.

**Reconnect policy**: any I/O error or close triggers a 5-second
backoff then re-authenticate + re-subscribe. The reconnect's
batched Subscribe replays whatever ``_topic_refs.keys()`` currently
holds, so an SSE stream that survived the outage continues to
receive ticks once auth completes.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import threading
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

from app.services.quote_stream import QuoteBus

logger = logging.getLogger(__name__)


_WS_URL = "wss://ws.etoro.com/ws"
_RECONNECT_BACKOFF_S = 5.0
# Debounce window for portfolio reconcile after a private-channel
# event. Multi-leg trades produce a burst of order/position pushes;
# we collapse them into one REST reconcile so the broker endpoint
# isn't hammered. 3 seconds is short enough that the operator sees
# fresh state inside a "feel alive" window without churn.
_RECONCILE_DEBOUNCE_S = 3.0


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


def build_unsubscribe_message(instrument_ids: list[int]) -> str | None:
    """Compose the ``Unsubscribe`` op JSON for a list of instrument IDs.

    Mirrors eToro's documented Subscribe envelope (same id/operation
    structure, ``topics`` array payload) per
    https://api-portal.etoro.com/api-reference/websocket/example-code.
    Returns ``None`` on empty input so the caller skips a no-op frame.
    """
    if not instrument_ids:
        return None
    topics = [f"instrument:{iid}" for iid in instrument_ids]
    return json.dumps(
        {
            "id": str(uuid.uuid4()),
            "operation": "Unsubscribe",
            "data": {"topics": topics},
        }
    )


_PRIVATE_TOPIC = "private"


def build_private_subscribe_message() -> str:
    """Compose the ``Subscribe`` op JSON for the ``private`` topic.

    The private channel carries order / position / credit events for
    the authenticated operator. Always sent — there's no "empty list"
    case as with instrument topics, since there's exactly one private
    channel per session. ``snapshot=False`` because we want only
    forward-going events; the REST reconcile owns the snapshot.
    """
    return json.dumps(
        {
            "id": str(uuid.uuid4()),
            "operation": "Subscribe",
            "data": {"topics": [_PRIVATE_TOPIC], "snapshot": False},
        }
    )


# Private-channel message types that signal a portfolio state change
# worth reconciling. eToro's WS docs list at least
# Trading.OrderForCloseMultiple.Update; we accept any
# Trading.OrderFor* / Trading.Position* / Trading.Credit* type as a
# reconcile trigger so we don't have to enumerate every variant up
# front. Debouncing means duplicates collapse anyway.
_PRIVATE_EVENT_PREFIXES: tuple[str, ...] = (
    "Trading.OrderFor",
    "Trading.Position",
    "Trading.Credit",
)

_RATE_MESSAGE_TYPE = "Trading.Instrument.Rate"


def _iter_inner_messages(raw: str) -> list[dict[str, object]]:
    """Normalise an eToro WS frame into the list of inner messages it
    carries.

    Per the official documentation
    (https://api-portal.etoro.com/api-reference/websocket/topics.md),
    each frame is wrapped in a ``{"messages": [...]}`` envelope; each
    inner message has the shape
    ``{"topic": ..., "content": "<json-string>", "id": ..., "type": ...}``.
    The ``content`` field is itself a JSON-encoded string, NOT a
    parsed object — callers must ``json.loads`` it to get the
    actual rate payload.

    We also accept a top-level inner-message shape (no outer
    ``messages`` wrapper) for backwards compatibility with our
    historical test fixtures and any future framing change.
    Returns ``[]`` for malformed JSON, non-dict envelopes, or
    envelopes that carry neither shape.
    """
    try:
        envelope = json.loads(raw)
    except json.JSONDecodeError:
        return []
    if not isinstance(envelope, dict):
        return []
    inner = envelope.get("messages")
    if isinstance(inner, list):
        return [m for m in inner if isinstance(m, dict)]
    # Top-level inner-message shape: {"type": "...", ...}.
    if isinstance(envelope.get("type"), str):
        return [envelope]
    return []


def _parse_rate_content(msg: dict[str, object]) -> QuoteUpdate | None:
    """Parse one inner ``Trading.Instrument.Rate`` message into a
    :class:`QuoteUpdate`.

    Handles both the documented envelope shape — where ``content``
    is a JSON-encoded string carrying the actual fields — and the
    legacy ``data`` shape (parsed object directly under ``data``)
    used by older test fixtures. Returns ``None`` on any field-
    shape failure so the listener loop drops the bad frame and
    keeps reading.
    """
    if msg.get("type") != _RATE_MESSAGE_TYPE:
        return None

    payload: object | None
    raw_content = msg.get("content")
    if isinstance(raw_content, str):
        try:
            payload = json.loads(raw_content)
        except json.JSONDecodeError:
            return None
    else:
        payload = msg.get("data")
    if not isinstance(payload, dict):
        return None

    # InstrumentID lives on the payload (eToro's official shape) but
    # for the legacy fixtures it may also live alongside on the
    # outer message; topic parsing covers the documented case where
    # InstrumentID is absent from the content.
    instrument_id_raw: object = payload.get("InstrumentID")
    if instrument_id_raw is None:
        topic = msg.get("topic")
        if isinstance(topic, str) and topic.startswith("instrument:"):
            instrument_id_raw = topic.removeprefix("instrument:")
    try:
        instrument_id = int(str(instrument_id_raw))
        bid = Decimal(str(payload["Bid"]))
        ask = Decimal(str(payload["Ask"]))
        last_raw = payload.get("LastExecution")
        last = Decimal(str(last_raw)) if last_raw is not None else None
        date_str = str(payload["Date"])
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


def is_private_event(raw: str) -> bool:
    """True if ``raw`` carries any private-channel push that should
    trigger a portfolio reconcile. Operates on the
    ``{"messages": [...]}`` envelope (the eToro v1 shape) as well
    as the top-level inner-message shape used by older fixtures.
    """
    for msg in _iter_inner_messages(raw):
        msg_type = msg.get("type")
        if isinstance(msg_type, str) and any(msg_type.startswith(p) for p in _PRIVATE_EVENT_PREFIXES):
            return True
    return False


def parse_rate_message(raw: str) -> QuoteUpdate | None:
    """Parse the *first* ``Trading.Instrument.Rate`` push in a raw WS
    frame. For frames carrying multiple ticks (eToro batches), use
    :func:`parse_rate_messages` to receive every update.

    Kept for backward-compat with existing single-tick test fixtures.
    """
    for msg in _iter_inner_messages(raw):
        update = _parse_rate_content(msg)
        if update is not None:
            return update
    return None


def parse_rate_messages(raw: str) -> list[QuoteUpdate]:
    """Extract every ``Trading.Instrument.Rate`` push in a raw WS
    frame. eToro's WS may batch multiple rates into one frame; the
    listener loop must process all of them or the rate-stream will
    silently drop ticks for high-frequency instruments.
    """
    updates: list[QuoteUpdate] = []
    for msg in _iter_inner_messages(raw):
        update = _parse_rate_content(msg)
        if update is not None:
            updates.append(update)
    return updates


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
        env: str,
        pool: psycopg_pool.ConnectionPool[Any],
        bus: QuoteBus | None = None,
        watched_ids_provider: Callable[[], list[int]] | None = None,
        reconcile_runner: Callable[[], None] | None = None,
    ) -> None:
        self._api_key = api_key
        self._user_key = user_key
        self._env = env
        self._pool = pool
        # Optional pub/sub fan-out for sub-second UI delivery (Slice 3).
        # When None, ticks are still upserted to ``quotes`` but no SSE
        # consumer is notified — useful for the daemon-only deploy
        # mode and for tests that exercise only the upsert path.
        self._bus = bus
        # Default selector hits the DB; tests inject a stub.
        self._watched_ids_provider = watched_ids_provider or self._default_watched_ids
        # Default reconcile runner builds an EtoroBrokerProvider and
        # calls sync_portfolio. Tests inject a no-op or counter to
        # avoid hitting the real REST API + DB.
        self._reconcile_runner = reconcile_runner or self._default_reconcile_runner
        self._task: asyncio.Task[None] | None = None
        self._stop_event = asyncio.Event()
        # Single dedicated worker coroutine owns reconciliation:
        # ``_schedule_reconcile`` only ``set()``s the event, the
        # worker waits on it, drains the burst, then runs at most one
        # reconcile at a time. This pattern (vs cancel-and-replace
        # debounce tasks) avoids the race where a cancel arrives
        # while ``asyncio.to_thread(self._reconcile_runner)`` is in
        # flight — Task.cancel cancels the *coroutine*, not the
        # worker thread, so the prior reconcile would otherwise keep
        # writing to DB while a new task starts a second concurrent
        # one. Single worker = guaranteed serialisation.
        self._reconcile_signal = asyncio.Event()
        self._reconcile_worker_task: asyncio.Task[None] | None = None
        # ``_reconcile_idle`` is a *thread-side* signal: the runner
        # wrapper clears it before invoking the user-supplied runner
        # and sets it back after. ``stop()`` waits on this before
        # returning so the FastAPI lifespan can't close the
        # ConnectionPool while a reconcile thread is still inside
        # ``sync_portfolio``. ``Task.cancel()`` on the worker
        # coroutine does *not* wait for an in-flight ``to_thread``
        # worker — the coroutine raises CancelledError immediately
        # while the OS thread keeps running. So we need an explicit
        # thread-completion barrier separate from the asyncio
        # cancellation chain.
        self._reconcile_idle = threading.Event()
        self._reconcile_idle.set()

        # Visibility-driven topic registry. Every page-view SSE stream
        # bumps a ref on its visible instrument ids; the topic is sent
        # to eToro iff its refcount > 0 (#498). No DB-backed selector
        # auto-pins held positions or watchlist — what the operator
        # has on screen drives the upstream subscription, nothing else.
        self._topic_refs: dict[int, int] = {}
        # Live WS connection, set inside ``_connect_and_listen`` once
        # the auth handshake succeeds and cleared on disconnect. The
        # add/remove path reads this to send frames from external
        # request handlers; ``None`` means the connection is in a
        # reconnect window and the request-handler path updates only
        # the in-memory ref counts — the next connect seeds the
        # subscribe set from ``_topic_refs.keys()``.
        self._ws: ClientConnection | None = None
        # Serialises concurrent add_instruments / remove_instruments
        # calls that can arrive from multiple SSE clients on the
        # same event loop. Small lock, held briefly.
        self._topic_lock = asyncio.Lock()

    def _default_watched_ids(self) -> list[int]:
        with self._pool.connection() as conn:
            return fetch_watched_instrument_ids(conn)

    def _default_reconcile_runner(self) -> None:
        """Sync helper: REST snapshot via EtoroBrokerProvider, then
        ``sync_portfolio`` against a fresh DB connection. Runs in a
        worker thread (see ``_perform_reconcile``) so the WS event
        loop stays hot. Mirrors the daily_portfolio_sync pattern in
        ``app.workers.scheduler`` so the two reconcile paths agree on
        broker construction + sync semantics.
        """
        # Local imports avoid pulling provider stack into module load
        # (the REST provider has heavy httpx + retry deps that the
        # WS-only test path doesn't need).
        from app.providers.implementations.etoro_broker import EtoroBrokerProvider
        from app.services.portfolio_sync import sync_portfolio

        with EtoroBrokerProvider(
            api_key=self._api_key,
            user_key=self._user_key,
            env=self._env,
        ) as broker:
            portfolio = broker.get_portfolio()

        with self._pool.connection() as conn:
            # ``ConnectionPool.connection()`` already commits on clean
            # exit / rolls back on error via ``with conn:`` — no
            # explicit commit needed here.
            sync_portfolio(conn, portfolio)

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
        self._reconcile_signal.clear()
        self._reconcile_worker_task = asyncio.create_task(self._reconcile_worker(), name="etoro-ws-reconcile-worker")
        # Subscriber boots with an empty ``_topic_refs``; the WS
        # connection comes up but doesn't subscribe to anything until
        # an SSE stream opens and calls ``add_instruments`` for the
        # ids on screen. Visibility drives the upstream subscription,
        # not held / watchlist state (#498).
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
        # Cancel the reconcile worker. The worker coroutine may be
        # awaiting ``asyncio.to_thread`` — the cancel raises
        # CancelledError out of the await, but the OS thread running
        # ``self._reconcile_runner`` keeps going. We then wait on
        # ``_reconcile_idle`` (set from inside the thread by the
        # wrapper, see ``_run_reconcile_in_thread``) so the lifespan
        # caller can safely close the DB pool right after this stop()
        # returns.
        if self._reconcile_worker_task is not None:
            self._reconcile_worker_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._reconcile_worker_task
            self._reconcile_worker_task = None
        if not self._reconcile_idle.is_set():
            # Bounded wait — sync_portfolio is fast, but if a thread
            # is somehow stuck we'd rather log + proceed than hang
            # shutdown forever.
            done = await asyncio.to_thread(self._reconcile_idle.wait, 30.0)
            if not done:
                logger.warning(
                    "EtoroWebSocketSubscriber: reconcile thread still "
                    "running after 30s shutdown wait — proceeding anyway"
                )
        logger.info("EtoroWebSocketSubscriber: stopped")

    def _schedule_reconcile(self) -> None:
        """Signal the reconcile worker that a private event landed.

        Idempotent: setting an already-set ``Event`` is a no-op, so a
        burst of N events ahead of the worker still results in one
        debounce window and one reconcile — the burst-collapse
        invariant comes from the worker's wait-then-drain loop, not
        from cancelling per-event timers.
        """
        self._reconcile_signal.set()

    async def _reconcile_worker(self) -> None:
        """Owner coroutine for portfolio reconciliation.

        Loop:
          1. Wait for a reconcile signal.
          2. Drain the debounce window: keep clearing+waiting up to
             ``_RECONCILE_DEBOUNCE_S`` for further signals; any new
             signal restarts the window so a long burst collapses
             into a single reconcile fired only after a quiet gap.
          3. Run the reconcile via ``asyncio.to_thread`` so the
             event loop stays responsive.
          4. Re-iterate. If a signal arrived *during* the reconcile,
             ``_reconcile_signal.is_set()`` is true at the top of the
             next loop, so the next reconcile fires after another
             debounce window — guaranteeing the latest broker state
             is reflected without ever running two reconciles at
             once.

        Cancellation is the only exit path; ``stop()`` cancels the
        task. CancelledError raised mid-``to_thread`` waits for the
        worker thread to finish before propagating, so the DB write
        never gets torn mid-flight.
        """
        while not self._stop_event.is_set():
            await self._reconcile_signal.wait()
            # Debounce drain: collect a quiet gap before firing.
            while True:
                self._reconcile_signal.clear()
                try:
                    await asyncio.wait_for(
                        self._reconcile_signal.wait(),
                        timeout=_RECONCILE_DEBOUNCE_S,
                    )
                except TimeoutError:
                    break
                # Another signal arrived inside the window — drain
                # again so the reconcile reflects the latest event.
            # Clear the idle barrier synchronously *before* handing
            # work to the executor. If we cleared inside the worker
            # thread instead, ``stop()`` could fire between
            # ``asyncio.to_thread`` submission and the thread
            # actually starting, observe ``is_set() is True``, and
            # return while the queued thread is about to run a
            # reconcile against the soon-to-close pool. Synchronous
            # clear + thread-side ``set()`` in a ``finally`` removes
            # that submit-not-yet-running window.
            self._reconcile_idle.clear()
            try:
                await asyncio.to_thread(self._run_reconcile_in_thread)
                logger.info("EtoroWebSocketSubscriber: reconcile complete")
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.warning(
                    "EtoroWebSocketSubscriber: reconcile failed",
                    exc_info=True,
                )

    def _run_reconcile_in_thread(self) -> None:
        """Wrapper executed inside the worker thread.

        The asyncio side clears ``_reconcile_idle`` before submitting
        this; the thread's ``finally`` sets it again. The set() runs
        *inside the thread*, so ``stop()`` can wait on this Event to
        know the actual OS thread has exited — independent of
        whatever the asyncio side did with cancellation.
        """
        try:
            self._reconcile_runner()
        finally:
            self._reconcile_idle.set()

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
            auth_reply = await _await_auth_envelope(ws, timeout_s=10.0)
            if not _is_auth_success(auth_reply):
                raise RuntimeError(f"eToro WS auth failed: {auth_reply!r}")

            # Hold the lock across the ``_ws`` publish + batched
            # initial Subscribe send. This serialises against every
            # concurrent add/remove: a remove that would Unsubscribe
            # a topic present in our batched snapshot cannot fire
            # until we finish sending, so eToro never sees an
            # out-of-order ``Unsubscribe(T) → Subscribe(..., T)``
            # that would strand T subscribed despite no source
            # wanting it. Sending under the lock also means the
            # batched frame and any concurrent delta frames are
            # serialised in wire order.
            #
            # ``_topic_refs`` is the single source of truth for what
            # to subscribe to. Page-view SSE streams populate it via
            # ``add_instruments`` / ``remove_instruments`` as the
            # operator opens and closes pages; refs accumulated during
            # a reconnect window survive here. Re-subscribing from
            # ``_topic_refs.keys()`` replays the current visibility
            # set in one frame so any SSE that survived the outage
            # keeps receiving ticks.
            #
            # The private subscribe + listen loop run outside the
            # lock but inside the ``_ws``-clearing try/finally, so
            # any failure after startup-subscribe completes still
            # clears ``_ws`` before the outer reconnect backoff.
            try:
                async with self._topic_lock:
                    topics_to_send = sorted(self._topic_refs.keys())
                    self._ws = ws
                    sub_msg = build_subscribe_message(topics_to_send)
                    if sub_msg is not None:
                        await ws.send(sub_msg)
                        logger.info(
                            "EtoroWebSocketSubscriber: subscribed to %d instrument topics",
                            len(topics_to_send),
                        )
                    else:
                        logger.info(
                            "EtoroWebSocketSubscriber: no tracked instruments — "
                            "connection will idle until a page-view subscribe"
                        )

                # Always subscribe to the private channel — even if
                # the operator has no instruments yet, opening a
                # position will emit a private event that triggers
                # reconcile, which in turn picks up the new
                # watched-IDs set on the next reconnect cycle.
                await ws.send(build_private_subscribe_message())
                logger.info("EtoroWebSocketSubscriber: subscribed to private channel")

                await self._listen(ws)
            finally:
                # Clear ``_ws`` under the lock so any in-flight
                # add/remove either completed its frame send (not
                # holding the lock while sending, see those methods)
                # or queues here on the lock and, on re-entry,
                # observes ``_ws = None`` — deferring to the next
                # reconnect cycle rather than sending on a dead
                # socket.
                async with self._topic_lock:
                    self._ws = None

    async def add_instruments(self, instrument_ids: list[int]) -> None:
        """Bump ref counts for the given instrument ids; send a
        Subscribe frame for any topics whose refcount just went 0→1.

        Single mechanism for every page-view ref — the SSE endpoint
        in :mod:`app.api.sse_quotes` calls this on stream open with
        the ids the operator currently has on screen. The Nth caller's
        add does not change wire state if N-1 callers already hold a
        ref (multi-tab on the same instrument shares one Subscribe).

        Safe to call from FastAPI request handlers. If the ws is
        mid-reconnect the counts are still updated and the next
        connect cycle re-subscribes from ``_topic_refs.keys()``.

        Cancellation-safety: the ref-count update is pure-Python
        under the lock with no ``await`` in the critical section
        before the wire send, so a CancelledError during the awaited
        send leaves the counts committed. Callers pair this with a
        ``remove_instruments`` in their finally to guarantee no
        leaked refs.
        """
        if not instrument_ids:
            return
        async with self._topic_lock:
            newly_tracked: list[int] = []
            for iid in instrument_ids:
                prior = self._topic_refs.get(iid, 0)
                self._topic_refs[iid] = prior + 1
                if prior == 0:
                    newly_tracked.append(iid)
            # Send under the lock so wire ordering matches state
            # ordering. Releasing the lock between state-mutation
            # and send would let a concurrent ``remove_instruments``
            # queue an Unsubscribe(T) frame that overtakes our
            # Subscribe(T) on the wire — eToro would end up with T
            # subscribed despite ``_topic_refs[T] == 1``, tearing
            # down a held-position feed (Codex review on PR for
            # #490). The cost is that other callers wait for the
            # send to flush; ws.send() is non-blocking on a healthy
            # socket so contention is small in practice.
            if newly_tracked and self._ws is not None:
                msg = build_subscribe_message(newly_tracked)
                if msg is not None:
                    try:
                        await self._ws.send(msg)
                        logger.info(
                            "EtoroWebSocketSubscriber: subscribe %d topics",
                            len(newly_tracked),
                        )
                    except Exception:
                        logger.warning(
                            "EtoroWebSocketSubscriber: Subscribe send failed; "
                            "next reconnect will resubscribe from ref counts",
                            exc_info=True,
                        )

    async def remove_instruments(self, instrument_ids: list[int]) -> None:
        """Decrement ref counts; send Unsubscribe for topics that
        hit zero.

        Symmetric with :meth:`add_instruments` — the SSE endpoint
        calls this on stream close with the ids the page was viewing.
        When two tabs share the same id the first close drops the
        refcount from 2→1 (no Unsubscribe), the second from 1→0
        (Unsubscribe goes out). No wire teardown until every page-view
        ref has dropped.
        """
        if not instrument_ids:
            return
        async with self._topic_lock:
            to_unsubscribe: list[int] = []
            for iid in instrument_ids:
                if iid not in self._topic_refs:
                    continue
                self._topic_refs[iid] -= 1
                if self._topic_refs[iid] <= 0:
                    del self._topic_refs[iid]
                    to_unsubscribe.append(iid)
            # See ``add_instruments`` for the rationale on sending
            # under the lock — same wire-ordering invariant.
            if to_unsubscribe and self._ws is not None:
                msg = build_unsubscribe_message(to_unsubscribe)
                if msg is not None:
                    try:
                        await self._ws.send(msg)
                        logger.info(
                            "EtoroWebSocketSubscriber: unsubscribe %d topics",
                            len(to_unsubscribe),
                        )
                    except Exception:
                        logger.warning(
                            "EtoroWebSocketSubscriber: Unsubscribe send failed",
                            exc_info=True,
                        )

    async def _listen(self, ws: ClientConnection) -> None:
        async for raw in ws:
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8", errors="ignore")
            # Private events come first because they're cheap to test
            # for and we never want a reconcile-trigger to be
            # confused with a rate push (the type prefix check
            # already disambiguates, but ordering keeps the dispatch
            # readable).
            if is_private_event(raw):
                self._schedule_reconcile()
                continue
            # eToro batches multiple ticks per WS frame inside the
            # ``messages: [...]`` envelope (#503 fix). Drop any
            # frame that yields zero rates; otherwise iterate every
            # update so high-frequency instruments don't lose ticks
            # to batching.
            updates = parse_rate_messages(raw)
            for update in updates:
                # Publish first, on the event loop, before the DB
                # offload. SSE subscribers see the tick within the
                # same async tick the WS read finished on; the DB
                # round-trip only gates persistence (which the
                # page-load path reads to bootstrap before SSE
                # takes over). Loop-affinity on
                # ``QuoteBus.publish`` requires this be called
                # from the event loop, so doing it before
                # ``to_thread`` is the only correct ordering —
                # calling it from inside the worker thread would
                # race the asyncio.Queue internals.
                if self._bus is not None:
                    self._bus.publish(update)
                try:
                    # ``pool.connection()`` is sync — calling it
                    # from the event loop would block the loop for
                    # the full DB round-trip on every tick.
                    # Offload to a worker thread so the WS read
                    # loop stays hot.
                    await asyncio.to_thread(self._sync_upsert, update)
                except Exception:
                    logger.warning(
                        "EtoroWebSocketSubscriber: upsert failed instrument_id=%d",
                        update.instrument_id,
                        exc_info=True,
                    )


def _looks_like_json_envelope(raw: str | bytes) -> bool:
    """Coarse pre-filter for the auth-handshake drain loop.

    eToro's WS occasionally emits a leading control byte (observed
    ``b'\\x00'`` in dev, likely an internal heartbeat / keepalive
    prelude) before the actual auth response. ``_is_auth_success``
    parses JSON and rejects on non-success, so the noise frame
    would tip us into a 5-second reconnect loop forever.

    Strip whitespace + control bytes and check whether the first
    real character is ``{``. JSON envelopes always start there;
    anything else is noise we should keep reading past.
    """
    if isinstance(raw, bytes):
        text = raw.decode("utf-8", errors="ignore")
    else:
        text = raw
    # Single-pass strip across whitespace + null so any interleaving
    # (``\x00 {``, `` \x00{``, ``\x00\x00 {``) is handled. A two-pass
    # ``.lstrip().lstrip("\x00")`` would miss ``\x00 {`` because the
    # leading null blocks the whitespace strip.
    stripped = text.lstrip("\x00 \t\r\n\v\f")
    return stripped.startswith("{")


async def _await_auth_envelope(ws: ClientConnection, *, timeout_s: float) -> str | bytes:
    """Drain non-JSON frames during the auth handshake.

    Reads frames until one looks like a JSON envelope or the
    cumulative ``timeout_s`` deadline elapses. Returns the first
    JSON-envelope frame so the caller can run ``_is_auth_success``
    on it.

    Why this matters: a single ``recv()`` with a strict JSON parse
    treats *any* leading frame as the auth ack. eToro emits a
    control-byte prelude on some connections (dev observation:
    ``b'\\x00'``); without draining we reconnect-loop every
    backoff window and never authenticate. See #474.
    """
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_s
    while True:
        remaining = deadline - loop.time()
        if remaining <= 0:
            raise TimeoutError("eToro WS auth: no JSON envelope within deadline")
        frame = await asyncio.wait_for(ws.recv(), timeout=remaining)
        if _looks_like_json_envelope(frame):
            return frame
        # Log at DEBUG so this is visible when investigating but
        # silent in production. Frame may be bytes; repr keeps the
        # control characters readable.
        logger.debug("EtoroWebSocketSubscriber: skipping noise frame %r during auth", frame)


def _is_auth_success(raw: str | bytes) -> bool:
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", errors="ignore")
    try:
        msg = json.loads(raw)
    except json.JSONDecodeError, TypeError:
        return False
    return isinstance(msg, dict) and bool(msg.get("success"))
