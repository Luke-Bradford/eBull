# app/providers/rate_gate.py
"""Rate-gate abstraction shared by every SEC HTTP consumer (#1484).

A ``RateGate`` enforces an inter-request floor. ``InProcessFloorGate`` is
the legacy per-process monotonic floor (default + DB-failure fallback);
``PostgresFloorGate`` (separate module, DB-touching) makes the floor
cross-process. Providers stay DB-free by holding only a ``RateGate``.
"""

from __future__ import annotations

import asyncio
import threading
import time
from collections.abc import Awaitable, Callable
from typing import Protocol, runtime_checkable

# Canonical SEC inter-request floor (9.09 req/s). Defined HERE in the DB-free
# module so both `sec_edgar` and `sec_rate_gate_holder` import it from a leaf
# with no provider/holder dependency (Codex ckpt-1 MED: avoids the
# holder<->sec_edgar import cycle). `sec_edgar` re-exports it as the legacy
# `_MIN_REQUEST_INTERVAL_S` name.
SEC_MIN_REQUEST_INTERVAL_S: float = 0.11


def compute_wait(*, now: float, next_free_at: float, floor: float) -> float:
    """Seconds to wait before firing: ``max(0, next_free_at - now)``.

    Mirrors the GCRA SQL in PostgresFloorGate so the arithmetic is unit-
    testable in isolation. ``floor`` is accepted for symmetry with the SQL
    signature (the advance adds it); the wait itself does not use it.
    """
    return max(0.0, next_free_at - now)


@runtime_checkable
class RateGate(Protocol):
    def acquire(self) -> None: ...
    async def acquire_async(self) -> None: ...


class InProcessFloorGate:
    """Monotonic inter-request floor over a single in-process timestamp.

    ``_next_free_at`` is the next-allowed fire time. Both ``acquire`` and
    ``acquire_async`` reserve under one ``threading.Lock`` (advance the
    timestamp), then sleep OUTSIDE the lock — identical semantics to the
    legacy ``_throttle_and_stamp`` / ``_AsyncRateLimiter`` pair.
    """

    def __init__(
        self,
        *,
        floor: float,
        _monotonic: Callable[[], float] = time.monotonic,
        _sleep: Callable[[float], None] = time.sleep,
        _async_sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._floor = floor
        self._next_free_at = 0.0
        self._lock = threading.Lock()
        self._monotonic = _monotonic
        self._sleep = _sleep
        self._async_sleep = _async_sleep

    def _reserve(self) -> float:
        with self._lock:
            now = self._monotonic()
            fire_at = max(now, self._next_free_at)
            self._next_free_at = fire_at + self._floor
            return compute_wait(now=now, next_free_at=fire_at, floor=self._floor)

    def acquire(self) -> None:
        wait = self._reserve()
        self._sleep(wait)

    async def acquire_async(self) -> None:
        wait = self._reserve()
        await self._async_sleep(wait)
