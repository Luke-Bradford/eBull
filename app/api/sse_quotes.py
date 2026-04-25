"""Server-Sent Events endpoint for live quote ticks (#274 Slice 3).

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
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncGenerator
from datetime import UTC, datetime

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import StreamingResponse

from app.api.auth import require_session_or_service_token
from app.services.etoro_websocket import QuoteUpdate
from app.services.quote_stream import QuoteBus

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


def _format_tick(update: QuoteUpdate) -> str:
    """Serialise one tick as a single SSE ``data:`` frame.

    Uses ``str(Decimal)`` to preserve full precision rather than
    casting to float. Frontend parses these strings back into the
    Decimal-equivalent representation; lossy float conversion in
    transit would defeat the spread-pct invariants.
    """
    payload = {
        "instrument_id": update.instrument_id,
        "bid": str(update.bid),
        "ask": str(update.ask),
        "last": None if update.last is None else str(update.last),
        "quoted_at": update.quoted_at.isoformat(),
    }
    return f"data: {json.dumps(payload)}\n\n"


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
        yield f": stream open at {datetime.now(UTC).isoformat()}\n\n"

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
            yield _format_tick(update)


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
    return StreamingResponse(
        _event_stream(request, bus, instrument_ids),
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
