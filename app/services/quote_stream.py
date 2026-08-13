"""In-process quote-tick fan-out bus (#274 Slice 3).

The eToro WebSocket subscriber writes every rate push to the
``quotes`` table (Slices 1+2). This module adds a parallel
*real-time* delivery channel: the same ``QuoteUpdate`` is also
``publish()``ed onto a ``QuoteBus``, and any number of asyncio
subscribers can ``subscribe()`` for the instrument IDs they care
about. The SSE endpoint in ``app.api.sse_quotes`` is the one
in-tree consumer; tests inject their own.

**Why in-process, not Redis:** the operator's eBull deploys as a
single uvicorn instance — there is no fan-out across workers in v1.
Redis pub/sub would solve the multi-worker case but adds an
infra dep and a 5-10ms hop per tick. Slice 4 adds a
Postgres-advisory-lock arbitrator if/when multi-worker becomes
real; that's the point at which a cross-process bus is justified.

Backpressure: each subscriber gets a bounded ``asyncio.Queue``. A
slow consumer (e.g. a tab the operator left open in a sleeping
laptop) cannot block the publish hot-path — when the queue is full
we *drop* the tick and bump a per-subscriber counter. The next
delivered tick still has the latest bid/ask, so a temporarily-slow
consumer recovers gracefully.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from app.services.etoro_websocket import QuoteUpdate

logger = logging.getLogger(__name__)


# Per-subscriber queue size. Each tick is small (<200 bytes when
# serialized), so 100 covers any realistic burst without holding
# meaningful memory. A subscriber consistently lagging by >100 ticks
# is broken — drop policy is the right answer, not unbounded growth.
_QUEUE_MAXSIZE = 100


@dataclass(eq=False)
class _Subscriber:
    """Internal handle the bus tracks per active subscription.

    ``eq=False`` keeps identity-based hashing (the dataclass default
    sets ``__hash__`` to ``None`` when ``eq=True``, which is the
    dataclass default — that breaks ``set.add``). Each subscriber is
    a unique session, so identity is exactly the equality we want.
    """

    instrument_ids: frozenset[int]
    queue: asyncio.Queue[QuoteUpdate]
    drops: int = field(default=0)


class QuoteBus:
    """Fan-out bus: ``publish(update)`` → every matching subscriber.

    Loop-affinity: bus must be created and driven from a single
    asyncio event loop. ``publish`` is sync but **must** be called
    from the loop thread — ``asyncio.Queue.put_nowait`` is only
    safe in-loop because the queue's internal scheduler primitives
    (Future creation for waiters, callback chaining) are
    loop-bound. The WS subscriber publishes from its async
    ``_listen`` coroutine for exactly this reason; the DB upsert
    that goes to ``asyncio.to_thread`` does *not* touch the bus.
    """

    def __init__(self) -> None:
        self._subscribers: set[_Subscriber] = set()

    @asynccontextmanager
    async def subscribe(self, instrument_ids: frozenset[int]) -> AsyncIterator[asyncio.Queue[QuoteUpdate]]:
        """Async context manager: yields a queue that receives ticks
        for the requested instrument IDs.

        On exit, the subscriber is removed from the fan-out set. The
        SSE endpoint wraps this in its event-stream generator so a
        client disconnect tears down the subscription cleanly.

        Empty ``instrument_ids`` is allowed but useless — the queue
        will simply never receive anything; callers should filter
        empties out themselves.
        """
        sub = _Subscriber(
            instrument_ids=instrument_ids,
            queue=asyncio.Queue(maxsize=_QUEUE_MAXSIZE),
        )
        # Add/remove are sync because publish() is sync and must not
        # await — both share single-threaded loop invariants. ``set``
        # mutation in CPython is atomic per the GIL but we still
        # snapshot to a tuple in publish() so subscribe/unsubscribe
        # mid-fan-out cannot raise ``RuntimeError: set changed size
        # during iteration``.
        self._subscribers.add(sub)
        try:
            yield sub.queue
        finally:
            self._subscribers.discard(sub)
            if sub.drops > 0:
                logger.info(
                    "QuoteBus subscriber removed: dropped %d ticks during session",
                    sub.drops,
                )

    def publish(self, update: QuoteUpdate) -> None:
        """Fan out one tick. **Must be called from the event loop
        thread** — see the class docstring on loop-affinity.

        Slow-consumer policy: if a subscriber's queue is full,
        increment its drop counter and continue — never block the
        publisher. The dropped tick is gone (we don't replay the
        latest), but every subscriber that was reading at normal
        cadence still saw the most recent bid/ask.
        """
        # Snapshot to a tuple so subscribe/unsubscribe cannot mutate
        # the iteration target. Both sides run on the same loop so
        # this races nothing in practice; the snapshot is belt-and-
        # braces for any future change that adds re-entrancy.
        for sub in tuple(self._subscribers):
            if update.instrument_id not in sub.instrument_ids:
                continue
            try:
                sub.queue.put_nowait(update)
            except asyncio.QueueFull:
                sub.drops += 1
                # Don't log every drop — under sustained backpressure
                # we'd flood logs. The session-end log records the
                # total.
