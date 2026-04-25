"""Server-Sent Events endpoint for live quote ticks (#274 Slices 3+4).

Operator UI uses ``EventSource`` to subscribe to a filtered live feed
of WebSocket-driven quote updates. Each tick from the eToro WS
``Trading.Instrument.Rate`` push lands on the in-process
``QuoteBus`` (see ``app.services.quote_stream``); this endpoint
wraps a per-connection subscriber in a ``StreamingResponse`` that
emits ``data: {json}\\n\\n`` frames.

**Why SSE, not WebSockets:** the channel is one-way (server → UI),
EventSource is built into every browser, auto-reconnects on drop,
and goes through proxies / corporate firewalls without protocol
upgrades. WebSockets would buy bidirectional traffic that the
quote-stream use-case has no need for.

**Auth:** reuses the existing operator-session dependency. SSE
connections inherit the same Cookie that the rest of the API uses,
so no separate token plumbing is needed.

**Slice 4 (display currency):** ticks land on the bus in the
instrument's *native* currency (eToro's quote currency, captured in
``instruments.currency``). At stream open we snapshot the
instrument-currency map, the live FX rate table, and the operator's
``runtime_config.display_currency``; per-tick we attach a
``display`` block with the converted bid/ask/last so the UI can
render the operator's preferred currency without doing FX maths.
The native triple is preserved alongside the display triple for
clients that want both. If no FX rate exists for a pair, the
display block is ``null`` and the UI falls back to the native
values.

A snapshot at stream open is acceptable because (a) FX rates change
every 5 min via ``fx_rates_refresh`` — slower than typical session
length, (b) ``display_currency`` rarely changes, (c) EventSource
auto-reconnects so a refresh on the next connect cycle picks up
any change.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncGenerator
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal

import psycopg
from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import StreamingResponse

from app.api.auth import require_session_or_service_token
from app.services.etoro_websocket import QuoteUpdate
from app.services.fx import convert_quote_fields, load_live_fx_rates
from app.services.quote_stream import QuoteBus
from app.services.runtime_config import get_runtime_config

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/sse",
    tags=["sse"],
    dependencies=[Depends(require_session_or_service_token)],
)


# Heartbeat interval: SSE comment frames keep proxies / load
# balancers from idle-killing the connection. 15s is well under the
# typical 60s idle timeout while staying clear of the per-tick rate.
_HEARTBEAT_INTERVAL_S = 15.0


@dataclass(frozen=True)
class _DisplayContext:
    """Per-stream snapshot of FX + currency data.

    Captured once at stream open; reused on every tick. See module
    docstring for why a snapshot is acceptable here.
    """

    display_ccy: str
    instrument_ccys: dict[int, str]
    rates: dict[tuple[str, str], Decimal]


def _format_tick(update: QuoteUpdate, ctx: _DisplayContext) -> str:
    """Serialise one tick as a single SSE ``data:`` frame.

    Always emits the native triple (``bid``/``ask``/``last``) so
    clients keep a stable contract. The ``display`` block carries
    the converted triple plus the target currency code; it is
    ``null`` when conversion isn't possible (unknown native currency
    or missing FX rate). Decimal precision is preserved via
    ``str(Decimal)`` — float would round-trip lossy and break
    downstream spread-pct invariants.
    """
    native_ccy = ctx.instrument_ccys.get(update.instrument_id)
    display_block: dict[str, object] | None = None
    if native_ccy is not None:
        converted = convert_quote_fields(
            update.bid,
            update.ask,
            update.last,
            native_ccy=native_ccy,
            display_ccy=ctx.display_ccy,
            rates=ctx.rates,
        )
        if converted is not None:
            d_bid, d_ask, d_last = converted
            display_block = {
                "currency": ctx.display_ccy,
                "bid": str(d_bid),
                "ask": str(d_ask),
                "last": None if d_last is None else str(d_last),
            }

    payload: dict[str, object] = {
        "instrument_id": update.instrument_id,
        "native_currency": native_ccy,
        "bid": str(update.bid),
        "ask": str(update.ask),
        "last": None if update.last is None else str(update.last),
        "quoted_at": update.quoted_at.isoformat(),
        "display": display_block,
    }
    return f"data: {json.dumps(payload)}\n\n"


def _load_display_context(conn: psycopg.Connection[object], instrument_ids: frozenset[int]) -> _DisplayContext:
    """Build the per-stream FX + currency snapshot.

    Restricts the instrument-currency lookup to ``instrument_ids``
    so a watchlist of 5 doesn't drag the entire universe (5k+ rows)
    into memory per SSE connection.
    """
    display_ccy = get_runtime_config(conn).display_currency
    instrument_ccys: dict[int, str] = {}
    if instrument_ids:
        import psycopg.rows

        with conn.cursor(row_factory=psycopg.rows.tuple_row) as cur:
            cur.execute(
                "SELECT instrument_id, currency FROM instruments WHERE instrument_id = ANY(%s)",
                (list(instrument_ids),),
            )
            for iid, ccy in cur.fetchall():
                if ccy is not None:
                    instrument_ccys[int(iid)] = str(ccy)
    rates = load_live_fx_rates(conn)
    return _DisplayContext(
        display_ccy=display_ccy,
        instrument_ccys=instrument_ccys,
        rates=rates,
    )


def _parse_instrument_ids(raw: str) -> frozenset[int]:
    """Parse the ``ids`` query string (comma-separated ints).

    Anything that doesn't parse as an int is silently dropped — the
    UI is the only client and supplies its own list, so a typo
    becomes "I see no ticks" rather than a 400 that breaks the
    connection retry loop.
    """
    out: set[int] = set()
    for token in raw.split(","):
        token = token.strip()
        if not token:
            continue
        try:
            out.add(int(token))
        except ValueError:
            continue
    return frozenset(out)


async def _event_stream(
    request: Request,
    bus: QuoteBus,
    instrument_ids: frozenset[int],
    ctx: _DisplayContext,
) -> AsyncGenerator[str]:
    """Generator yielded by StreamingResponse.

    Pulls ticks from a per-connection subscriber queue with a
    timeout race against the heartbeat interval — every iteration
    we either deliver a tick OR send a comment heartbeat. Either
    way we then check ``request.is_disconnected()`` so a tab close
    tears down the subscription within at most one heartbeat
    cycle.
    """
    async with bus.subscribe(instrument_ids) as queue:
        # Open frame is yielded *after* subscribe so any tick
        # published between this point and the next iteration
        # actually reaches the subscriber. If the open frame went
        # first, a fast publish could land before the bus knows
        # about us — fine in production where ticks arrive over
        # seconds, but a real bug for tests and for any caller
        # that publishes synchronously after opening the stream.
        yield f": stream open at {datetime.now(UTC).isoformat()} display={ctx.display_ccy}\n\n"

        while True:
            if await request.is_disconnected():
                return
            try:
                update = await asyncio.wait_for(queue.get(), timeout=_HEARTBEAT_INTERVAL_S)
            except TimeoutError:
                # Heartbeat: SSE comments are lines starting with ':'
                # and are ignored by EventSource clients but keep
                # the connection alive through proxies.
                yield ": heartbeat\n\n"
                continue
            yield _format_tick(update, ctx)


@router.get("/quotes")
async def quotes_stream(
    request: Request,
    ids: str = Query(..., description="Comma-separated instrument IDs to stream"),
) -> StreamingResponse:
    """Stream live quote ticks for the requested instrument IDs.

    Example: ``GET /sse/quotes?ids=1001,1002,1003``

    The bus is read off ``request.app.state.quote_bus``, which is
    populated by the FastAPI lifespan. Reading from app state (vs
    a module global) keeps tests in control: a test app fixture
    can inject its own QuoteBus without monkey-patching the import.
    """
    bus: QuoteBus | None = getattr(request.app.state, "quote_bus", None)
    if bus is None:
        # Lifespan hasn't installed a bus — every other route still
        # works, but live ticks are not available. Surface as a
        # 503 so the UI can fall back to its 5s polling cadence.
        return StreamingResponse(
            iter([": no quote bus available\n\n"]),
            media_type="text/event-stream",
            status_code=503,
        )

    instrument_ids = _parse_instrument_ids(ids)

    # Snapshot the FX + currency context before the stream starts so
    # the per-tick path is a pure dict lookup. ``pool.connection()``
    # is sync and would block the event loop for the full DB round-
    # trip — offload to a worker thread so a slow DB / reconnect
    # burst can't stall unrelated async work. Mirrors the WS
    # subscriber's ``_sync_upsert`` offload pattern.
    pool = request.app.state.db_pool

    def _load() -> _DisplayContext:
        with pool.connection() as conn:
            return _load_display_context(conn, instrument_ids)

    ctx = await asyncio.to_thread(_load)

    return StreamingResponse(
        _event_stream(request, bus, instrument_ids, ctx),
        media_type="text/event-stream",
        headers={
            # Disable proxy buffering so each tick flushes immediately.
            # Without this, nginx will hold ticks until its buffer
            # fills (8KB+) — at <200B per tick that's 40+ ticks of
            # delay, defeating the whole point of SSE.
            "X-Accel-Buffering": "no",
            "Cache-Control": "no-cache",
        },
    )
