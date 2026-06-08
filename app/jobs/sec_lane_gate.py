"""In-process concurrency gate for the dissolved ``sec_rate`` JobLock lane (#1542).

The old lane was a 1-wide Postgres advisory mutex shared by ~22 SEC jobs — it
serialised them to one-at-a-time and starved the losers (#1534/#1538/#1540).
The SEC 10 req/s ceiling is enforced separately at the HTTP layer
(``_PROCESS_RATE_LIMIT_CLOCK``), so the lane gave zero rate protection.

This gate replaces it with two in-process primitives (zero Postgres
connections — all sec_rate execution is single-process; see the spec §3c):

  * a count semaphore — up to ``SEC_LANE_MAX_CONCURRENCY`` sec_rate jobs at once;
  * a per-job-name lock — never two instances of the SAME job_name (what the
    single shared lane gave incidentally; load-bearing for the Form-3
    DELETE+INSERT and the 13F rewash).

Both acquires are NON-blocking; a failure makes ``JobLock`` raise
``JobAlreadyRunning``, which ``_fire_scheduled_with_lane_retry`` (#1538) rides
out with its bounded backoff for scheduled fires.

Leaf module (threading only) so ``app/db/pg_settings.py`` can read
``SEC_LANE_MAX_CONCURRENCY`` for the connection-budget model and the gate is
fast-tier testable without importing psycopg.
"""

from __future__ import annotations

import threading
from typing import Final

SEC_LANE_MAX_CONCURRENCY: Final[int] = 4
"""Max concurrent ``sec_rate`` jobs. Bounded by the dev connection budget
(#1472): each running sec_rate job now holds ONE body connection (no JobLock
conn), and ``app/db/pg_settings.py`` charges N of these. Raising N requires
re-checking ``_dev_profile_connection_demand`` against ``max_connections``."""


class SecLaneGate:
    """Non-blocking count + per-name gate. Thread-safe.

    A ``BoundedSemaphore`` bounds total concurrency; a set of currently-held
    job names enforces "no two instances of the same job_name". Both are
    mutated under one guard lock, so ``try_acquire`` / ``release`` are atomic.
    ``release`` of a name that is not currently held raises ``RuntimeError``
    (loud misuse) WITHOUT touching the semaphore — so a mispaired call can
    never loosen the count. The held set is bounded by the number of
    concurrent holders (<= max_concurrency), not by distinct names ever seen.
    """

    def __init__(self, max_concurrency: int) -> None:
        self._slots = threading.BoundedSemaphore(max_concurrency)
        self._guard = threading.Lock()
        self._held: set[str] = set()

    def try_acquire(self, job_name: str) -> bool:
        """Acquire a slot for ``job_name``. Returns False (no state changed) if
        the same job_name is already running OR all slots are busy."""
        with self._guard:
            if job_name in self._held:
                return False  # same job_name already running
            if not self._slots.acquire(blocking=False):
                return False  # all slots busy
            self._held.add(job_name)
            return True

    def release(self, job_name: str) -> None:
        """Release a slot previously taken by ``try_acquire``. Raises
        RuntimeError if ``job_name`` is not currently held (mispaired release),
        WITHOUT touching the semaphore, so the count cannot be corrupted."""
        with self._guard:
            if job_name not in self._held:
                raise RuntimeError(f"SecLaneGate.release: {job_name!r} was not acquired")
            self._held.discard(job_name)
            self._slots.release()

    def is_held(self, job_name: str) -> bool:
        """True if ``job_name`` currently holds a slot in THIS process.

        Liveness signal for the bootstrap orphan reaper (#1542): sec_rate jobs
        hold no Postgres advisory lock, so the reaper (which runs in the jobs
        process) checks the in-process held set instead of ``pg_locks``.
        """
        with self._guard:
            return job_name in self._held


SEC_LANE_GATE: SecLaneGate = SecLaneGate(SEC_LANE_MAX_CONCURRENCY)
"""Process-wide singleton used by ``JobLock`` for every ``sec_rate`` job."""


def reset_for_tests() -> None:
    """Test-only: rebuild the singleton's internal state. Production never calls this."""
    global SEC_LANE_GATE
    SEC_LANE_GATE = SecLaneGate(SEC_LANE_MAX_CONCURRENCY)
