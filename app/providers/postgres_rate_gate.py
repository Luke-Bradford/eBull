# app/providers/postgres_rate_gate.py
"""Cross-process SEC rate gate backed by one Postgres row (#1484, §3a).

GCRA virtual-floor: a single UPDATE advances ``sec_rate_gate.next_free_at``
under the row lock and returns the wait. Borrow a pooled conn for ~1 ms
under ONE threading.Lock (sync + async share it -> <=1 gate conn/process),
release, then sleep. DB error / zero-row -> in-process fallback (§3e).
"""

from __future__ import annotations

import asyncio
import logging
import threading
import time
from collections.abc import Callable
from typing import Any

from app.providers.rate_gate import SEC_MIN_REQUEST_INTERVAL_S, InProcessFloorGate

logger = logging.getLogger(__name__)

# Spec §3e: ONE process-global in-process fallback shared by every
# PostgresFloorGate instance (not one per instance), so a DB outage degrades
# to a single per-process floor — never fragmented per gate.
_PROCESS_FALLBACK_GATE = InProcessFloorGate(floor=SEC_MIN_REQUEST_INTERVAL_S)

# Single CTE statement: capture clock_timestamp() once (volatile), reuse it
# for both the advance and the returned wait (§3a; Codex ckpt-1 MED).
_GCRA_SQL = """
WITH t AS (SELECT clock_timestamp() AS now)
UPDATE sec_rate_gate g
SET next_free_at = GREATEST((SELECT now FROM t), g.next_free_at)
                   + make_interval(secs => %(floor)s)
FROM t
WHERE g.budget = %(budget)s
RETURNING EXTRACT(EPOCH FROM (g.next_free_at
          - make_interval(secs => %(floor)s) - t.now)) AS wait_s
"""


class PostgresFloorGate:
    """Reservation pacer. ``acquire`` = a DB-clock slot grant, then a local sleep.

    ⚠ The two halves have different guarantees, and only the first is this
    class's. The GCRA UPDATE spaces GRANTS by ``floor_s`` on the Postgres clock;
    what a caller does with the returned wait is a local sleep, whose accuracy is
    the OS scheduler's. Measured on the dev box at #2648: ``time.sleep(0.05)``
    returned 37-105 ms late single-threaded and 129-148 ms late with two threads
    (loadavg 3.4, 10 CPUs). Oversleep only ever DELAYS a request, so the grant
    ladder still bounds the request rate — but two threads whose grants are one
    floor apart can EMIT within a millisecond of each other when the earlier one
    oversleeps by more than the later one, so emission spacing is not a property
    to assert on. See tests/db/test_postgres_rate_gate.py.

    ``_sleep`` is injectable for exactly that reason, mirroring the seam
    ``InProcessFloorGate`` already carries (app/providers/rate_gate.py:55) — it
    lets a test observe the grant ladder without the scheduler in the way. The
    async path keeps ``asyncio.sleep``; nothing needs to seam it yet.
    """

    def __init__(
        self,
        pool: Any,
        *,
        budget: str = "sec",
        floor_s: float,
        _sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._pool = pool
        self._budget = budget
        self._floor = floor_s
        self._sleep = _sleep
        self._lock = threading.Lock()
        # §3e fallback is the process-global singleton above (shared by every
        # PostgresFloorGate instance), NOT a per-instance gate.
        self._fallback = _PROCESS_FALLBACK_GATE

    def _reserve_sync(self) -> float:
        """Borrow a conn, run the GCRA UPDATE, release; return wait seconds.

        Raises on DB error / zero-row so callers route to the fallback.
        """
        with self._lock:
            with self._pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(_GCRA_SQL, {"floor": self._floor, "budget": self._budget})
                    row = cur.fetchone()
                conn.commit()
        if row is None:
            raise RuntimeError(f"sec_rate_gate row missing for budget={self._budget!r}")
        return max(0.0, float(row[0]))

    def acquire(self) -> None:
        try:
            wait = self._reserve_sync()
        except Exception:
            logger.warning("PostgresFloorGate: DB acquire failed; in-process fallback", exc_info=True)
            self._fallback.acquire()
            return
        if wait > 0:
            self._sleep(wait)

    async def acquire_async(self) -> None:
        loop = asyncio.get_running_loop()
        try:
            wait = await loop.run_in_executor(None, self._reserve_sync)
        except Exception:
            logger.warning("PostgresFloorGate: DB acquire failed; in-process fallback", exc_info=True)
            await self._fallback.acquire_async()
            return
        if wait > 0:
            await asyncio.sleep(wait)
