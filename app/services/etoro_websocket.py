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
import ssl
import threading
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any
from uuid import UUID

import psycopg
import psycopg_pool
import websockets
from websockets.asyncio.client import ClientConnection
from websockets.exceptions import ConnectionClosed

from app.services.credential_health_cache import CredentialHealthCache
from app.services.quote_stream import QuoteBus

logger = logging.getLogger(__name__)


_WS_URL = "wss://ws.etoro.com/ws"
_RECONNECT_BACKOFF_S = 5.0

# Auth-failure exponential backoff (#978 / #974/D). Applied when the
# eToro WS auth handshake itself fails (401 / Unauthorized) — distinct
# from generic connection errors which still use _RECONNECT_BACKOFF_S.
# Spec-locked sequence + 600s cap; reset to index 0 on first
# successful auth. Codex pre-push r2.11 (#974).
_AUTH_FAILURE_BACKOFF_S: tuple[float, ...] = (5.0, 30.0, 120.0, 600.0, 600.0)
# Debounce window for portfolio reconcile after a private-channel
# event. Multi-leg trades produce a burst of order/position pushes;
# we collapse them into one REST reconcile so the broker endpoint
# isn't hammered. 3 seconds is short enough that the operator sees
# fresh state inside a "feel alive" window without churn.
_RECONCILE_DEBOUNCE_S = 3.0
# REST live-rate poll cadence. eToro's WS is bursty; this poll
# guarantees a freshness floor so the chart's in-progress bar updates
# at least every _RATE_POLL_INTERVAL_S regardless of WS push state.
# 5s × 12 polls/min = 12 GET/min — well under the 60 GET/min budget.
# Each poll batch-fetches every visible instrument in one rates call.
_RATE_POLL_INTERVAL_S = 5.0

# Hard per-frame ceiling on the eToro WS, MEASURED — the portal
# documents no limit of any kind (#2241, bracketed 25,529 B accepted /
# 25,719 B fatal). Over it the socket is DROPPED: close code 1006 with
# an empty reason, i.e. no close frame at all, no ack, no error
# envelope, subscription not applied.
_WS_FRAME_LIMIT_BYTES = 25_600
# What we actually pack to. The headroom absorbs any envelope drift on
# eToro's side and keeps frames near the 500-topic/~9.4 KB shape proven
# stable at full scale in #2241 — there is no benefit to sailing close
# to a limit whose breach is silent and kills the connection.
_WS_FRAME_BUDGET_BYTES = 20_480
# How long an op frame may sit un-acked before it is reported. eToro
# acks Subscribe/Unsubscribe within milliseconds; a missing ack is the
# ONLY signal that a frame was dropped, so it must not pass unnoticed.
_ACK_TIMEOUT_S = 10.0


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


@dataclass(frozen=True)
class RateDelta:
    """One ``Trading.Instrument.Rate`` push, parsed but NOT yet merged.

    eToro's rate push is a **field-level sparse delta**, not a
    complete snapshot (#2243, measured over 180,666 messages; see
    ``.claude/skills/data-sources/etoro-api.md`` §"WS rate semantics").
    Any subset of ``Bid`` / ``Ask`` / ``LastExecution`` can arrive
    alone — only 16.8% of messages carry ``Bid``+``Ask`` together,
    and requiring both discarded 58.1% of *price-changing* messages
    (#2252).

    So the wire shape cannot be normalised to a :class:`QuoteUpdate`
    in isolation: a bid-only push is meaningful, but only against the
    last known ask. This type is the honest intermediate — what the
    frame actually said — and :class:`RateStateStore` merges it onto
    per-instrument state to produce a complete tick.

    Presence is tracked separately from value because the two carry
    different meanings for ``last``: an *absent* ``LastExecution``
    means "unchanged, keep prior", whereas a *present* one that is
    non-positive means "not a real trade → NULL" (#1429). A bare
    ``Decimal | None`` cannot express both.
    """

    instrument_id: int
    quoted_at: datetime
    bid: Decimal | None = None
    ask: Decimal | None = None
    last: Decimal | None = None
    # True iff the payload carried the field at all. ``has_last`` with
    # ``last is None`` is the #1429 "non-positive → NULL" case.
    has_bid: bool = False
    has_ask: bool = False
    has_last: bool = False

    @property
    def carries_price(self) -> bool:
        """True if this push moves any price field.

        59.8% of rate messages are pure heartbeats (``Date`` +
        ``PriceRateID``, no price field). Merging one changes nothing,
        and emitting on one would advance ``quoted_at`` without any
        price behind it — reporting freshness we do not have.
        """
        return self.has_bid or self.has_ask or self.has_last

    def to_quote_update(self) -> QuoteUpdate | None:
        """Stateless promotion — only a delta already carrying BOTH
        sides is a complete tick. Returns ``None`` otherwise.

        This is the pre-#2252 behaviour, preserved for the
        stateless :func:`parse_rate_messages` API.
        """
        if self.bid is None or self.ask is None:
            return None
        return QuoteUpdate(
            instrument_id=self.instrument_id,
            bid=self.bid,
            ask=self.ask,
            last=self.last,
            quoted_at=self.quoted_at,
        )


def build_auth_message(api_key: str, user_key: str) -> str:
    """Compose the ``Authenticate`` op JSON sent on every (re)connect."""
    return json.dumps(
        {
            "id": str(uuid.uuid4()),
            "operation": "Authenticate",
            "data": {"apiKey": api_key, "userKey": user_key},
        }
    )


@dataclass(frozen=True)
class WsFrame:
    """One op frame ready to send, with the id needed to match its ack."""

    frame_id: str
    operation: str
    payload: str
    topic_count: int


def _topic_frames(
    instrument_ids: list[int],
    operation: str,
    extra_data: dict[str, object] | None = None,
) -> list[WsFrame]:
    """Split ``instrument_ids`` into frames that fit the WS byte limit.

    **Pack by BYTES, never by topic count** (#2241): instrument-id
    width varies from 2 to 6 digits, so a count-based cap does not
    bound frame size. Over the limit eToro does not reject — it drops
    the socket with `1006` and an empty reason, no ack and no error
    envelope, and the subscription is simply not applied (#2249).

    Sizing is exact rather than iterative: the envelope is serialised
    once with an empty topic list to get its overhead, and each topic
    costs its own JSON encoding plus one separator byte. Both are pure
    ASCII here, so byte length equals character length.
    """
    if not instrument_ids:
        return []

    data: dict[str, object] = {"topics": [], **(extra_data or {})}
    # Overhead of everything except the topics themselves. The uuid is
    # a fixed 36 chars, so any id stands in for the real one.
    overhead = len(json.dumps({"id": str(uuid.uuid4()), "operation": operation, "data": data}))

    frames: list[WsFrame] = []
    batch: list[str] = []
    size = overhead
    for iid in instrument_ids:
        topic = f"instrument:{iid}"
        # +1 for the comma joining it to the previous topic. Charging
        # it on the first topic too simply leaves one spare byte.
        cost = len(json.dumps(topic)) + 1
        if batch and size + cost > _WS_FRAME_BUDGET_BYTES:
            frames.append(_seal_frame(batch, operation, extra_data))
            batch = []
            size = overhead
        batch.append(topic)
        size += cost
    if batch:
        frames.append(_seal_frame(batch, operation, extra_data))
    return frames


def _seal_frame(topics: list[str], operation: str, extra_data: dict[str, object] | None) -> WsFrame:
    frame_id = str(uuid.uuid4())
    payload = json.dumps(
        {
            "id": frame_id,
            "operation": operation,
            "data": {"topics": topics, **(extra_data or {})},
        }
    )
    # Belt and braces: the accounting above is exact, but a silent
    # over-limit frame costs the whole connection, so assert rather
    # than trust the arithmetic.
    encoded = len(payload.encode("utf-8"))
    if encoded > _WS_FRAME_LIMIT_BYTES:
        raise ValueError(f"{operation} frame is {encoded} bytes, over the {_WS_FRAME_LIMIT_BYTES}-byte WS limit")
    return WsFrame(frame_id=frame_id, operation=operation, payload=payload, topic_count=len(topics))


def build_subscribe_frames(instrument_ids: list[int]) -> list[WsFrame]:
    """``Subscribe`` frames for ``instrument_ids``, chunked to fit the
    WS byte limit. Empty list for empty input."""
    return _topic_frames(instrument_ids, "Subscribe", {"snapshot": True})


def build_unsubscribe_frames(instrument_ids: list[int]) -> list[WsFrame]:
    """``Unsubscribe`` frames for ``instrument_ids``, chunked to fit the
    WS byte limit. Empty list for empty input."""
    return _topic_frames(instrument_ids, "Unsubscribe")


def build_subscribe_message(instrument_ids: list[int]) -> str | None:
    """Compose a SINGLE ``Subscribe`` op JSON for a list of instrument IDs.

    Returns ``None`` when the list is empty so callers don't send a
    no-op subscription that eToro might reject.

    ⚠ **Not safe for an unbounded id set** — over 25 KiB the frame is
    dropped silently and takes the connection with it (#2249). Callers
    that cannot bound their input must use :func:`build_subscribe_frames`.
    Retained for fixtures and for call sites with a known-small set.
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


def _parse_rate_content(msg: dict[str, object]) -> RateDelta | None:
    """Parse one inner ``Trading.Instrument.Rate`` message into a
    :class:`RateDelta`.

    Handles both the documented envelope shape — where ``content``
    is a JSON-encoded string carrying the actual fields — and the
    legacy ``data`` shape (parsed object directly under ``data``)
    used by older test fixtures.

    ``None`` means **"not a rate message"** — wrong type, unparseable
    content, or no usable identity/timestamp. It does NOT mean
    "rate message with a partial payload": that is a
    :class:`RateDelta` whose ``has_*`` flags say which fields
    arrived, and collapsing the two is exactly the #2252 defect
    (58.1% of price-changing pushes discarded as if they were
    malformed).
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
        # Identity + timestamp are the only REQUIRED fields. Every
        # price field is optional (#2243): the heartbeat shape is
        # ``{Date, PriceRateID}`` and carries no price at all.
        date_str = str(payload["Date"])
        if date_str.endswith("Z"):
            date_str = date_str[:-1] + "+00:00"
        quoted_at = datetime.fromisoformat(date_str)

        has_bid = "Bid" in payload
        bid = Decimal(str(payload["Bid"])) if has_bid else None
        has_ask = "Ask" in payload
        ask = Decimal(str(payload["Ask"])) if has_ask else None

        has_last = "LastExecution" in payload
        last_raw = payload.get("LastExecution")
        last = Decimal(str(last_raw)) if last_raw is not None else None
        # #1429: a non-positive last is not a real trade (eToro pushes 0 for
        # un-freshly-traded instruments) — persist NULL, never a fake 0.
        if last is not None and last <= 0:
            last = None
    except KeyError, TypeError, ValueError, InvalidOperation:
        return None
    return RateDelta(
        instrument_id=instrument_id,
        quoted_at=quoted_at,
        bid=bid,
        ask=ask,
        last=last,
        has_bid=has_bid,
        has_ask=has_ask,
        has_last=has_last,
    )


@dataclass(frozen=True)
class OpAck:
    """eToro's acknowledgement of a Subscribe / Unsubscribe frame."""

    frame_id: str
    operation: str
    success: bool
    error_code: str | None = None


def parse_op_acks(raw: str) -> list[OpAck]:
    """Extract every Subscribe / Unsubscribe ack in a raw WS frame.

    Shape: ``{"id": …, "success": true, "operation": "Subscribe"}``
    (#2241). Parsed separately from :func:`_iter_inner_messages`
    because an ack carries **no** ``type`` field, so that helper's
    top-level branch does not return it.

    Reading acks is not cosmetic: an oversize frame produces no error
    envelope and no close frame, so the *absence* of an ack is the only
    evidence it was dropped.
    """
    # Substring pre-filter before the decode. This runs on every
    # inbound frame, alongside the decodes that `is_private_event` and
    # `parse_rate_deltas` already do, and the overwhelming majority of
    # frames are rate pushes carrying no `operation` key at all. No
    # semantic shortcut: an ack cannot exist without the literal key.
    if '"operation"' not in raw:
        return []
    try:
        envelope = json.loads(raw)
    except json.JSONDecodeError:
        return []

    candidates: list[object]
    if isinstance(envelope, dict) and isinstance(envelope.get("messages"), list):
        candidates = list(envelope["messages"])
    else:
        candidates = [envelope]

    acks: list[OpAck] = []
    for item in candidates:
        if not isinstance(item, dict):
            continue
        frame_id = item.get("id")
        operation = item.get("operation")
        if not isinstance(frame_id, str) or operation not in ("Subscribe", "Unsubscribe"):
            continue
        error_code = item.get("errorCode")
        acks.append(
            OpAck(
                frame_id=frame_id,
                operation=operation,
                success=bool(item.get("success")),
                error_code=error_code if isinstance(error_code, str) else None,
            )
        )
    return acks


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
    Stateless, so — like :func:`parse_rate_messages` — it sees only
    pushes that are complete on their own (#2252).
    """
    for msg in _iter_inner_messages(raw):
        delta = _parse_rate_content(msg)
        if delta is None:
            continue
        update = delta.to_quote_update()
        if update is not None:
            return update
    return None


def parse_rate_deltas(raw: str) -> list[RateDelta]:
    """Extract every ``Trading.Instrument.Rate`` push in a raw WS
    frame as a :class:`RateDelta`, complete or partial.

    eToro's WS may batch multiple rates into one frame; the listener
    loop must process all of them or the rate-stream will silently
    drop ticks for high-frequency instruments.

    This is the parse the subscriber uses. Feed the results through
    a :class:`RateStateStore` to merge them into complete ticks.
    """
    deltas: list[RateDelta] = []
    for msg in _iter_inner_messages(raw):
        delta = _parse_rate_content(msg)
        if delta is not None:
            deltas.append(delta)
    return deltas


def parse_rate_messages(raw: str) -> list[QuoteUpdate]:
    """Extract every rate push that is complete **on its own**.

    Stateless view over :func:`parse_rate_deltas`, kept for callers
    and fixtures that predate #2252. Partial deltas are not visible
    here — on the live wire that is 58.1% of price-changing pushes,
    so anything ingesting the real feed must use
    :func:`parse_rate_deltas` + :class:`RateStateStore` instead.
    """
    updates: list[QuoteUpdate] = []
    for delta in parse_rate_deltas(raw):
        update = delta.to_quote_update()
        if update is not None:
            updates.append(update)
    return updates


class RateStateStore:
    """Per-instrument last-known rate state, merged across sparse deltas.

    Exists because :data:`_UPSERT_SQL` writes ``bid``, ``ask``,
    ``last`` and ``spread_pct`` in one statement, so a partial delta
    cannot be applied without prior state — and eToro only sends
    partials (#2252). Holding the last known value per field turns a
    bid-only push into a complete tick against the standing ask.

    State is seeded for free: :func:`build_subscribe_message` already
    requests ``snapshot: True``, and the snapshot arrives as an
    ordinary rate message carrying every field, so a fresh
    subscription is complete from its first push.

    Not thread-safe by design — the subscriber applies deltas on the
    event loop in :meth:`EtoroWebSocketSubscriber._listen`, and only
    the resulting :class:`QuoteUpdate` is handed to a worker thread.
    """

    __slots__ = ("_state",)

    def __init__(self) -> None:
        # instrument_id -> (bid, ask, last, quoted_at)
        self._state: dict[int, tuple[Decimal | None, Decimal | None, Decimal | None, datetime]] = {}

    def apply(self, delta: RateDelta) -> QuoteUpdate | None:
        """Merge ``delta`` onto stored state; return a complete tick, or None.

        Returns ``None`` — meaning "nothing to publish", not "error" — when:

        * the delta carries no price field (a heartbeat; 59.8% of the
          wire). Emitting would advance ``quoted_at`` with no price
          behind it, overstating freshness;
        * the merged state still lacks a bid or an ask, i.e. we have
          never seen one side for this instrument. Only possible
          before the snapshot lands or if it was missed;
        * the delta is older than the state it would overwrite. The
          in-memory guard mirrors the ``quoted_at`` guard already in
          :data:`_UPSERT_SQL`, and matters more here: an out-of-order
          push that merely lost a race used to affect one row, but
          against merged state it would corrupt every subsequent tick
          for that instrument.
        """
        # A heartbeat is inert: nothing to merge, and — critically —
        # it must NOT advance the ordering watermark. Heartbeats are
        # the majority of the wire and arrive continuously, so letting
        # one set `quoted_at` would make the guard below reject the
        # next genuine price delta stamped anywhere behind it. The
        # watermark exists to order PRICE data against price data.
        if not delta.carries_price:
            return None

        prev = self._state.get(delta.instrument_id)
        if prev is not None and delta.quoted_at < prev[3]:
            return None

        bid, ask, last = (prev[0], prev[1], prev[2]) if prev is not None else (None, None, None)
        if delta.has_bid:
            bid = delta.bid
        if delta.has_ask:
            ask = delta.ask
        if delta.has_last:
            # Presence, not truthiness: a present-but-non-positive
            # LastExecution clears `last` to NULL per #1429, which is
            # a real state change and not the same as "unchanged".
            last = delta.last
        self._state[delta.instrument_id] = (bid, ask, last, delta.quoted_at)

        if bid is None or ask is None:
            return None
        return QuoteUpdate(
            instrument_id=delta.instrument_id,
            bid=bid,
            ask=ask,
            last=last,
            quoted_at=delta.quoted_at,
        )

    def forget(self, instrument_ids: list[int]) -> None:
        """Drop state for instruments no longer subscribed.

        Called on Unsubscribe so the store tracks live subscriptions
        rather than growing for the lifetime of the process — which
        matters once #2240's collector holds a universe-scale
        subscription rather than the handful of ids on screen.

        This is the tidy path, not the guarantee: ``_listen`` also
        refuses to create state for an unsubscribed id, which is what
        actually bounds the store when an Unsubscribe never lands.
        """
        for iid in instrument_ids:
            self._state.pop(iid, None)


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
        # #978 / #974/D — credential-aware mode. When all three are
        # supplied, the subscriber:
        #   * Records every auth outcome through to credential health
        #     via `record_health_outcome` (source='incidental').
        #   * Pre-checks the cache before opening a connection — if
        #     operator health != VALID, skips the connect entirely
        #     (no auth flood while keys are known-bad).
        #   * Uses exponential backoff on consecutive auth failures
        #     (5, 30, 120, 600, 600 cap) instead of fixed 5s.
        # Legacy callers omit them and get the original static-key
        # behavior unchanged.
        operator_id: UUID | None = None,
        credential_cache: CredentialHealthCache | None = None,
        audit_pool: psycopg_pool.ConnectionPool[Any] | None = None,
    ) -> None:
        self._api_key = api_key
        self._user_key = user_key
        self._env = env
        self._pool = pool
        self._operator_id = operator_id
        self._credential_cache = credential_cache
        self._audit_pool = audit_pool
        self._consecutive_auth_failures: int = 0
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

        # REST-polled live-rate fallback (#602 follow-up). eToro WS
        # is bursty / silent for stretches in demo; per-second polling
        # of /instruments/rates guarantees a freshness floor regardless
        # of WS state. Every _RATE_POLL_INTERVAL_S the poll loop
        # snapshots `_topic_refs.keys()` (visible instrument ids),
        # batch-calls the rates endpoint, and publishes synthesised
        # ticks to the bus exactly the way the WS path does. Skipped
        # when no SSE stream has visible ids.
        self._rest_poll_task: asyncio.Task[None] | None = None
        # Timer-driven un-acked-frame reporter (#2249). Separate from
        # the receive loop on purpose — see ``_ack_reaper_loop``.
        self._ack_reaper_task: asyncio.Task[None] | None = None

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

        # Per-instrument merged rate state (#2252). eToro's rate push
        # is a field-level sparse delta, so a bid-only message is only
        # a usable quote against the standing ask. Mutated solely on
        # the event loop (``_listen`` / ``remove_instruments``), and
        # ``_listen`` admits only ids present in ``_topic_refs``, so
        # its key set is bounded by the live subscription set.
        # Deliberately NOT cleared on reconnect: Subscribe replays
        # with ``snapshot: True``, so a stale entry is overwritten by
        # the snapshot before any partial can be merged onto it, and
        # keeping it means the reconnect window does not regress to
        # "no quote at all" for instruments whose snapshot is slow.
        self._rate_state = RateStateStore()

        # Op frames sent and not yet acked (#2249):
        # frame_id -> (operation, topic_count, monotonic sent_at).
        # An oversize frame yields no ack, no error envelope and no
        # close frame, so a missing ack is the ONLY evidence it was
        # dropped — without this the log reads "subscribed to N topics"
        # immediately before every death.
        self._pending_acks: dict[str, tuple[str, int, float]] = {}

    def _default_watched_ids(self) -> list[int]:
        with self._pool.connection() as conn:
            return fetch_watched_instrument_ids(conn)

    def _record_auth_outcome(self, *, success: bool, error_detail: str | None) -> None:
        """Write-through to credential health for both label rows.

        Legacy mode (no operator_id / cache / audit_pool): no-op.
        Credential-aware mode: looks up the operator's two label rows
        (api_key + user_key) and calls record_health_outcome with
        source='incidental' for each. The validate-stored probe path
        is the only thing that can clear REJECTED — incidental success
        only promotes from UNTESTED to VALID per the locked stickiness
        contract (#975).
        """
        if self._operator_id is None or self._audit_pool is None or self._credential_cache is None:
            return

        # Local imports avoid module-load circular paths and keep
        # startup fast — credential_health pulls in psycopg_pool /
        # config which is fine but unnecessary on the read-only
        # legacy path.
        from app.services.credential_health import record_health_outcome

        try:
            with self._pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT id
                          FROM broker_credentials
                         WHERE operator_id = %s
                           AND provider = 'etoro'
                           AND environment = %s
                           AND revoked_at IS NULL
                        """,
                        (self._operator_id, self._env),
                    )
                    cred_ids = [row[0] for row in cur.fetchall()]
        except Exception:
            logger.exception(
                "EtoroWebSocketSubscriber: credential id lookup failed; auth outcome write-through skipped"
            )
            return

        for cred_id in cred_ids:
            try:
                record_health_outcome(
                    credential_id=cred_id,
                    success=success,
                    source="incidental",
                    error_detail=error_detail,
                    pool=self._audit_pool,
                )
            except Exception:
                logger.warning(
                    "EtoroWebSocketSubscriber: credential health write-through failed for %s",
                    cred_id,
                    exc_info=True,
                )

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
        from app.services.trade_events import compute_history_min_date, fetch_trade_history_safely

        # Watermark read on a briefly-held pool conn BEFORE the provider
        # session — never hold a pooled conn across HTTP (#1472 class).
        with self._pool.connection() as conn:
            history_min_date = compute_history_min_date(conn)

        with EtoroBrokerProvider(
            api_key=self._api_key,
            user_key=self._user_key,
            env=self._env,
        ) as broker:
            portfolio = broker.get_portfolio()
            trade_history = fetch_trade_history_safely(broker, history_min_date)

        with self._pool.connection() as conn:
            # ``ConnectionPool.connection()`` already commits on clean
            # exit / rolls back on error via ``with conn:`` — no
            # explicit commit needed here.
            sync_portfolio(conn, portfolio, trade_history=trade_history)

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
        self._rest_poll_task = asyncio.create_task(self._rest_poll_loop(), name="etoro-ws-rest-poll")
        self._ack_reaper_task = asyncio.create_task(self._ack_reaper_loop(), name="etoro-ws-ack-reaper")
        logger.info("EtoroWebSocketSubscriber: started")

    async def stop(self) -> None:
        if self._task is None:
            return
        self._stop_event.set()
        self._task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._task
        self._task = None
        if self._rest_poll_task is not None:
            self._rest_poll_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._rest_poll_task
            self._rest_poll_task = None
        if self._ack_reaper_task is not None:
            self._ack_reaper_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._ack_reaper_task
            self._ack_reaper_task = None
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
            # Credential-health pre-flight (#978 / #974/D). When the
            # cache is wired, skip the connect attempt entirely while
            # the operator's aggregate health is anything other than
            # VALID. Avoids the 5s/loop auth-fail spam observed pre-
            # #978 when keys were bad. The cache is wake-up + 5s poll
            # so a VALID transition is observed within one cycle.
            if self._credential_cache is not None and self._operator_id is not None:
                health = self._credential_cache.get(operator_id=self._operator_id, environment=self._env)
                if health.value != "valid":
                    logger.debug(
                        "EtoroWebSocketSubscriber: skipping connect — operator health=%s",
                        health.value,
                    )
                    try:
                        await asyncio.wait_for(self._stop_event.wait(), timeout=_RECONNECT_BACKOFF_S)
                        return
                    except TimeoutError:
                        continue

            try:
                await self._connect_and_listen()
            except asyncio.CancelledError:
                raise
            except ssl.SSLError:
                # SSLError is an OSError subclass but signals TLS/cert
                # config trouble, not idle churn — keep the traceback.
                logger.warning(
                    "EtoroWebSocketSubscriber: connection error — backoff then reconnect",
                    exc_info=True,
                )
            except (ConnectionClosed, OSError, TimeoutError) as exc:
                # Expected churn (#1548): idle WS reaped by an LB, TCP
                # reset, auth-envelope timeout. Handled by reconnect —
                # no traceback, or real bugs drown in the noise.
                logger.info(
                    "EtoroWebSocketSubscriber: connection closed (%s) — reconnecting in %.0fs",
                    type(exc).__name__,
                    self._current_backoff(),
                )
            except Exception:
                logger.warning(
                    "EtoroWebSocketSubscriber: connection error — backoff then reconnect",
                    exc_info=True,
                )
            backoff = self._current_backoff()
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=backoff)
                # Stop signalled during backoff — exit cleanly.
                return
            except TimeoutError:
                continue

    def _current_backoff(self) -> float:
        """Return the next reconnect-backoff delay in seconds.

        Legacy mode (no cache): always _RECONNECT_BACKOFF_S.
        Credential-aware mode: exponential by consecutive_auth_failures.
        """
        if self._credential_cache is None or self._operator_id is None:
            return _RECONNECT_BACKOFF_S
        idx = min(self._consecutive_auth_failures, len(_AUTH_FAILURE_BACKOFF_S) - 1)
        return _AUTH_FAILURE_BACKOFF_S[idx]

    async def _connect_and_listen(self) -> None:
        async with websockets.connect(_WS_URL) as ws:
            await ws.send(build_auth_message(self._api_key, self._user_key))
            auth_reply = await _await_auth_envelope(ws, timeout_s=10.0)
            if not _is_auth_success(auth_reply):
                # Credential-aware mode (#978 / #974/D): write the
                # auth failure through to credential health. The
                # orchestrator gate, admin UI, and any future
                # subscribers see the rejected state on their next
                # cache poll.
                self._consecutive_auth_failures += 1
                self._record_auth_outcome(success=False, error_detail=str(auth_reply)[:240])
                raise RuntimeError(f"eToro WS auth failed: {auth_reply!r}")
            # Auth succeeded — reset the failure counter so the next
            # connection error backs off from the base interval.
            if self._consecutive_auth_failures > 0:
                logger.info(
                    "EtoroWebSocketSubscriber: auth recovered after %d consecutive failures",
                    self._consecutive_auth_failures,
                )
                self._consecutive_auth_failures = 0
            self._record_auth_outcome(success=True, error_detail=None)

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
                    # Frames pending on the dead connection can never
                    # be acked now. Report them rather than clearing
                    # silently — a reconnect that happened BECAUSE an
                    # oversize frame killed the socket is exactly when
                    # this evidence matters.
                    self._reap_unacked(reason="connection was re-established before the ack arrived")
                    # #2249: chunk the replay. A single frame over the
                    # ref set is what turns a large subscription into a
                    # connect → oversize frame → 1006 → reconnect loop
                    # that cannot self-heal, because the failure never
                    # drains ``_topic_refs``.
                    frames = build_subscribe_frames(topics_to_send)
                    if frames:
                        await self._send_frames(ws, frames)
                        logger.info(
                            "EtoroWebSocketSubscriber: subscribed to %d instrument topics in %d frame(s)",
                            len(topics_to_send),
                            len(frames),
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
                try:
                    # Built INSIDE the try: `_seal_frame` raises on a
                    # sizing bug, and this runs on the SSE request path
                    # — an uncaught raise would 500 the operator's
                    # stream, where the existing contract is "log and
                    # let the next reconnect resubscribe from refs".
                    frames = build_subscribe_frames(newly_tracked)
                    await self._send_frames(self._ws, frames)
                    logger.info(
                        "EtoroWebSocketSubscriber: subscribe %d topics in %d frame(s)",
                        len(newly_tracked),
                        len(frames),
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
            # Drop merged rate state alongside the topic (#2252) — the
            # next Subscribe re-seeds it from the snapshot.
            self._rate_state.forget(to_unsubscribe)
            # See ``add_instruments`` for the rationale on sending
            # under the lock — same wire-ordering invariant.
            if to_unsubscribe and self._ws is not None:
                try:
                    # Built inside the try — see `add_instruments`.
                    frames = build_unsubscribe_frames(to_unsubscribe)
                    await self._send_frames(self._ws, frames)
                    logger.info(
                        "EtoroWebSocketSubscriber: unsubscribe %d topics in %d frame(s)",
                        len(to_unsubscribe),
                        len(frames),
                    )
                except Exception:
                    logger.warning(
                        "EtoroWebSocketSubscriber: Unsubscribe send failed",
                        exc_info=True,
                    )

    def _register_pending(self, frame: WsFrame) -> None:
        """Record a sent op frame so its ack can be correlated (#2249)."""
        self._pending_acks[frame.frame_id] = (frame.operation, frame.topic_count, time.monotonic())

    async def _send_frames(self, ws: ClientConnection, frames: list[WsFrame]) -> None:
        """Send op frames, registering each for ack correlation FIRST.

        Order matters and is not cosmetic: ``ws.send`` awaits, which
        yields to the event loop, so the receive loop can process this
        very frame's ack before control returns here. Registering
        afterwards would insert an entry that the ack has already been
        and gone for — nothing would ever clear it, and the reaper
        would later report a genuinely-acked frame as NEVER ACKED,
        making the silent-drop detector cry wolf.

        A frame that fails to send is de-registered: nothing will ack
        what never reached the wire, and the caller already logs the
        send failure. ``BaseException`` so a cancellation mid-send
        cleans up too.
        """
        for frame in frames:
            self._register_pending(frame)
            try:
                await ws.send(frame.payload)
            except BaseException:
                self._pending_acks.pop(frame.frame_id, None)
                raise

    def _resolve_acks(self, raw: str) -> None:
        """Clear pending entries for acked frames and log rejections."""
        for ack in parse_op_acks(raw):
            pending = self._pending_acks.pop(ack.frame_id, None)
            if ack.success:
                continue
            # An explicit rejection (e.g. the 4,999-topic session cap,
            # #2241) does NOT poison the session — the connection keeps
            # serving already-subscribed topics. Log and carry on; do
            # not tear down.
            logger.warning(
                "EtoroWebSocketSubscriber: %s REJECTED for %s topics (errorCode=%s) — "
                "connection still live, those topics are not subscribed",
                ack.operation,
                pending[1] if pending else "?",
                ack.error_code,
            )

    def _reap_unacked(self, *, reason: str | None = None) -> None:
        """Report op frames that were never acknowledged.

        The silent-drop failure mode has no other detector: `ws.send()`
        returns normally, no error envelope arrives, and the close (if
        any) is a bare 1006. Reported once per frame, then forgotten so
        the warning does not repeat.

        ``reason`` set → drain EVERY pending frame regardless of age,
        for the case where they can no longer possibly be acked (the
        connection is gone).
        """
        now = time.monotonic()
        expired = reason is not None
        stale = [(fid, meta) for fid, meta in self._pending_acks.items() if expired or now - meta[2] > _ACK_TIMEOUT_S]
        for fid, (operation, topic_count, _) in stale:
            del self._pending_acks[fid]
            logger.warning(
                "EtoroWebSocketSubscriber: %s frame %s (%d topics) NEVER ACKED (%s) — those topics are NOT subscribed",
                operation,
                fid,
                topic_count,
                reason or f"no ack in {_ACK_TIMEOUT_S:.0f}s; eToro silently dropped it",
            )

    async def _ack_reaper_loop(self) -> None:
        """Time-driven un-acked-frame reporter.

        Deliberately NOT piggybacked on the receive loop: the failure
        this detects is an oversize frame that gets the socket dropped,
        which means **no further inbound message ever arrives** — so a
        reaper riding inbound traffic would miss precisely the case it
        exists for (Codex checkpoint 2). Ticks on a timer instead.
        """
        # Floor guards a pathologically small timeout only; in
        # production _ACK_TIMEOUT_S is 10s so the interval is 5s.
        interval = max(0.05, _ACK_TIMEOUT_S / 2)
        while not self._stop_event.is_set():
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=interval)
                return
            except TimeoutError:
                pass
            if self._pending_acks:
                self._reap_unacked()

    async def _listen(self, ws: ClientConnection) -> None:
        async for raw in ws:
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8", errors="ignore")
            # eToro batches inner messages of mixed types in a single
            # ``messages: [...]`` frame (#503/#504 fix). A frame may
            # carry one private event AND several rate ticks at once;
            # an early ``continue`` after the private check would
            # silently drop those rates. Dispatch BOTH paths on
            # every frame: schedule a reconcile if any inner message
            # is a private event, AND publish every rate tick the
            # frame carries.
            # #2249: match acks to sent op frames, and report any that
            # never arrive. Cheap on the hot path — the ack shape is
            # rejected on the first key check for a rate frame.
            self._resolve_acks(raw)

            if is_private_event(raw):
                self._schedule_reconcile()
            # #2252: parse to sparse deltas and merge onto per-instrument
            # state. Requiring a complete Bid+Ask payload — as this loop
            # did — discarded 58.1% of price-CHANGING pushes and left
            # ``quotes`` 1.5-2.6x staler than the feed allows.
            #
            # Gate on ``_topic_refs`` so ``_rate_state`` can only ever
            # hold instruments we are currently subscribed to. Without
            # it the merge state is repopulated by any frame for an
            # unsubscribed id — and ``remove_instruments`` forgets
            # BEFORE the wire Unsubscribe lands, so a dropped, rejected,
            # cancelled or simply ignored Unsubscribe would let
            # already-buffered frames refill it indefinitely. The gate
            # turns "bounded by the universe" from a claim about the
            # data into an invariant the code enforces.
            #
            # Unlocked read: ``_topic_refs`` is only mutated by
            # ``add_instruments`` / ``remove_instruments``, which run on
            # this same event loop, and there is no await between the
            # read and its use — so the lock buys nothing here and
            # taking it would serialise every frame against every
            # page-view change.
            updates = [
                update
                for delta in parse_rate_deltas(raw)
                if delta.instrument_id in self._topic_refs and (update := self._rate_state.apply(delta)) is not None
            ]
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

    async def _rest_poll_loop(self) -> None:
        """Periodic REST poll of /instruments/rates to guarantee a
        freshness floor for the visible-id set.

        eToro's WS is bursty: tens of seconds of silence are normal,
        even on demo with held positions. Pure-WS charts go stale in
        those windows. This loop runs alongside the WS, polling the
        REST rates endpoint every _RATE_POLL_INTERVAL_S for whatever
        ids have a refcount, and synthesises QuoteUpdate objects that
        feed the same QuoteBus the WS path uses. SSE consumers see
        identical tick payloads regardless of source.

        Why a loop here, not per-SSE-stream: the rates endpoint
        batches up to 100 ids in one call. One loop covering the
        union of all visible ids is strictly cheaper than N parallel
        per-stream polls. Same scaling argument as
        LiveQuoteProvider's "one stream per page" rule.

        Rate budget: 12 polls/min × 1 GET = 12 GET/min/key. 60 GET/min
        is the eToro ceiling, leaving ample headroom for the WS-side
        Subscribe / Unsubscribe traffic.
        """
        # Local import keeps the WS module's startup cycle independent
        # of the REST provider's heavy httpx + retry deps; only fires
        # when subscriber.start() is called.
        from app.providers.implementations.etoro import EtoroMarketDataProvider

        provider = EtoroMarketDataProvider(
            api_key=self._api_key,
            user_key=self._user_key,
            env=self._env,
        )
        try:
            while not self._stop_event.is_set():
                try:
                    await asyncio.wait_for(
                        self._stop_event.wait(),
                        timeout=_RATE_POLL_INTERVAL_S,
                    )
                    return
                except TimeoutError:
                    pass
                async with self._topic_lock:
                    ids = sorted(self._topic_refs.keys())
                if not ids:
                    continue
                try:
                    quotes = await asyncio.to_thread(provider.get_quotes, ids)
                except Exception:
                    logger.warning(
                        "EtoroWebSocketSubscriber: REST rate poll failed for %d ids — will retry next interval",
                        len(ids),
                        exc_info=True,
                    )
                    continue
                if self._bus is None:
                    continue
                for q in quotes:
                    update = QuoteUpdate(
                        instrument_id=q.instrument_id,
                        bid=q.bid,
                        ask=q.ask,
                        last=q.last,
                        quoted_at=q.timestamp,
                    )
                    self._bus.publish(update)
                    # Also persist to quotes table so the operator's
                    # next page-load sees the same fresh value the
                    # chart is already displaying. Same offload pattern
                    # as the WS path.
                    try:
                        await asyncio.to_thread(self._sync_upsert, update)
                    except Exception:
                        logger.warning(
                            "EtoroWebSocketSubscriber: rate-poll upsert failed instrument_id=%d",
                            update.instrument_id,
                            exc_info=True,
                        )
        finally:
            # Best-effort cleanup of the long-lived REST client. Most
            # of the time the subscriber outlives a single eBull
            # session so this only runs on lifespan shutdown.
            try:
                provider._client.close()  # noqa: SLF001 — provider context-manager only
            except Exception:
                pass


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
