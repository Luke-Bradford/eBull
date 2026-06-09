# SEC cross-process rate limiter (shared GCRA gate) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the per-PROCESS in-memory SEC 10 req/s throttle with a cross-process shared GCRA rate gate over one Postgres row, so all SEC HTTP traffic (every process/thread/future #479 worker) draws from one global ≤9 req/s reservation budget.

**Architecture:** A `RateGate` (sync `acquire()` + async `acquire_async()`) is accessed via a process-global getter `get_sec_rate_gate()` set once per composition root. `PostgresFloorGate` runs a single GCRA `UPDATE … RETURNING wait_s` against `sec_rate_gate(budget, next_free_at)`, borrows a pooled conn for ~1 ms under one `threading.Lock` (≤1 gate conn/process), releases, then sleeps `wait_s`. `InProcessFloorGate` (today's monotonic floor) is the default + the DB-failure fallback. The providers stay DB-free — the DB lives only in the injected gate.

**Tech Stack:** Python 3.12, psycopg3 + psycopg_pool, FastAPI lifespan, APScheduler jobs process, pytest (`-m "not db"` fast tier + `-m db` integration).

**Spec:** `docs/specs/ops/2026-06-09-sec-cross-process-rate-limiter.md`. Read §3a (GCRA SQL), §3b (getter wiring), §3d (conn bound), §3e (fallback), §3f (full consumer coverage), §3g (reservation-vs-emission).

**Key decisions baked in:**
- The unified global floor is `_MIN_REQUEST_INTERVAL_S = 0.11` (9.09 req/s). The bulk paths' old per-process `target_rps=7.0` self-limit is **subsumed** by the global gate — `target_rps` becomes advisory (ignored) on the gate path. This is a net safety gain (previously bulk-7 + per-filing-9 per process could sum >10; now everything shares one 9.09 floor).
- Sync and async share **one** `_reserve_sync()` under a single `threading.Lock` → ≤1 gate conn/process (tighter than the spec's ≤2).

---

## File Structure

- **Create** `app/providers/rate_gate.py` — `RateGate` protocol, `compute_wait` pure helper, `InProcessFloorGate` (DB-free). One responsibility: the rate-gate algorithm + in-process impl.
- **Create** `app/providers/postgres_rate_gate.py` — `PostgresFloorGate` (the only DB-touching gate; imports pool types + the in-process fallback). Kept separate so `rate_gate.py` stays DB-free and importable anywhere.
- **Create** `app/providers/sec_rate_gate_holder.py` — `get_sec_rate_gate()` / `set_sec_rate_gate()` + `_reset_sec_rate_gate_for_tests()`. Authoritative singleton holder.
- **Create** `sql/187_sec_rate_gate.sql` — the table + seed row.
- **Modify** `app/providers/resilient_client.py` — optional `gate` param; `_throttle` delegates.
- **Modify** `app/providers/implementations/sec_edgar.py` — provider constructors pass `gate=get_sec_rate_gate()`.
- **Modify** `app/providers/implementations/sec_fundamentals.py` — same.
- **Modify** `app/services/sec_pipelined_fetcher.py` — `_AsyncRateLimiter` delegates to the gate.
- **Modify** `app/services/sec_bulk_refresh.py` + `app/services/sec_bulk_download.py` — drop the clock-pair plumbing; rely on the gate default.
- **Modify** `app/main.py` (API lifespan) + `app/jobs/__main__.py` (jobs) — `set_sec_rate_gate(PostgresFloorGate(pool))` after pool open.
- **Modify** `app/db/pg_settings.py` — document the transient gate-conn term (no `max` change).
- **Modify** `.claude/skills/data-sources/sec-edgar.md` + `.claude/skills/data-engineer/etl-endpoint-coverage.md` — correct the per-process-clock claims.
- **Tests:** `tests/providers/test_rate_gate.py` (pure), `tests/providers/test_sec_rate_gate_holder.py` (pure), `tests/db/test_postgres_rate_gate.py` (`-m db`).

---

## Task 1: Schema migration `sql/187_sec_rate_gate.sql`

**Files:**
- Create: `sql/187_sec_rate_gate.sql`

- [ ] **Step 1: Write the migration**

```sql
-- 187_sec_rate_gate.sql
--
-- #1484 — cross-process SEC 10 req/s rate limiter. The in-process
-- _PROCESS_RATE_LIMIT_CLOCK (app/providers/implementations/sec_edgar.py)
-- paces each PROCESS to <=9 req/s independently; the API + jobs processes
-- together can sum >10 req/s against SEC's single per-IP counter -> UA-ban
-- risk. This table backs a shared GCRA "virtual floor": a single
-- advanceable next_free_at timestamp all processes reserve against, so the
-- global reservation rate stays under the SEC ceiling regardless of
-- process count. Keyed by `budget` so other per-IP limiters (FINRA, etc.)
-- can adopt the same primitive later by inserting another row.
--
-- See docs/specs/ops/2026-06-09-sec-cross-process-rate-limiter.md.

CREATE TABLE IF NOT EXISTS sec_rate_gate (
    budget        TEXT PRIMARY KEY,
    next_free_at  TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

INSERT INTO sec_rate_gate (budget) VALUES ('sec') ON CONFLICT (budget) DO NOTHING;
```

- [ ] **Step 2: Apply the migration to dev DB**

Run: `uv run python -c "from app.db.migrations import run_migrations; print(run_migrations())"`
Expected: output list includes `187_sec_rate_gate.sql`.

- [ ] **Step 3: Verify the row exists**

Run: `uv run python -c "import psycopg, os; c=psycopg.connect(os.environ['DATABASE_URL']); print(c.execute(\"SELECT budget, next_free_at FROM sec_rate_gate\").fetchone())"`
Expected: `('sec', datetime(...))`.

- [ ] **Step 4: Commit**

```bash
git add sql/187_sec_rate_gate.sql
git commit -m "feat(#1484): sec_rate_gate table for cross-process GCRA limiter"
```

---

## Task 2: `RateGate` protocol + `compute_wait` + `InProcessFloorGate`

**Files:**
- Create: `app/providers/rate_gate.py`
- Test: `tests/providers/test_rate_gate.py`

- [ ] **Step 1: Write the failing tests**

```python
# tests/providers/test_rate_gate.py
import time
import asyncio
import pytest
from app.providers.rate_gate import compute_wait, InProcessFloorGate


def test_compute_wait_idle_returns_zero():
    # next_free_at in the past -> fire immediately
    assert compute_wait(now=100.0, next_free_at=99.0, floor=0.11) == 0.0


def test_compute_wait_backlogged_returns_remaining():
    # next_free_at ahead of now -> wait the remainder
    assert compute_wait(now=100.0, next_free_at=100.05, floor=0.11) == pytest.approx(0.05)


def test_inprocess_floor_spaces_sync_calls():
    clock = [0.0]
    sleeps: list[float] = []
    gate = InProcessFloorGate(floor=0.11, _monotonic=lambda: clock[0], _sleep=lambda s: (sleeps.append(s), clock.__setitem__(0, clock[0] + s)))
    gate.acquire()          # idle -> no sleep
    gate.acquire()          # immediately after -> must sleep ~floor
    assert sleeps[0] == 0.0
    assert sleeps[1] == pytest.approx(0.11, abs=1e-9)


def test_inprocess_floor_async_shares_clock_with_sync():
    clock = [0.0]
    sleeps: list[float] = []

    async def fake_sleep(s):
        sleeps.append(s)
        clock[0] += s

    gate = InProcessFloorGate(floor=0.11, _monotonic=lambda: clock[0], _sleep=lambda s: clock.__setitem__(0, clock[0] + s), _async_sleep=fake_sleep)
    gate.acquire()                     # sync stamps the shared clock
    asyncio.run(gate.acquire_async())  # async sees it -> waits ~floor
    assert sleeps[-1] == pytest.approx(0.11, abs=1e-9)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/providers/test_rate_gate.py -v`
Expected: FAIL — `ModuleNotFoundError: app.providers.rate_gate`.

- [ ] **Step 3: Write the implementation**

```python
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
from typing import Awaitable, Callable, Protocol, runtime_checkable


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
        if wait > 0:
            self._sleep(wait)

    async def acquire_async(self) -> None:
        wait = self._reserve()
        if wait > 0:
            await self._async_sleep(wait)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/providers/test_rate_gate.py -v`
Expected: PASS (4 tests).

- [ ] **Step 5: Lint + typecheck**

Run: `uv run ruff check app/providers/rate_gate.py tests/providers/test_rate_gate.py && uv run pyright app/providers/rate_gate.py`
Expected: no errors.

- [ ] **Step 6: Commit**

```bash
git add app/providers/rate_gate.py tests/providers/test_rate_gate.py
git commit -m "feat(#1484): RateGate protocol + InProcessFloorGate + compute_wait"
```

---

## Task 3: `PostgresFloorGate` (GCRA, sync + async, fallback)

**Files:**
- Create: `app/providers/postgres_rate_gate.py`
- Test: `tests/db/test_postgres_rate_gate.py` (DB tier — written here, run in Task 9 too)

- [ ] **Step 1: Write the failing DB test**

```python
# tests/db/test_postgres_rate_gate.py
import threading
import time
import pytest
from app.providers.postgres_rate_gate import PostgresFloorGate

pytestmark = pytest.mark.db


def test_two_threads_share_floor(db_pool):
    # db_pool: existing test fixture yielding a psycopg_pool ConnectionPool
    # against the test DB with a seeded sec_rate_gate('sec') row.
    floor = 0.05
    gate = PostgresFloorGate(db_pool, budget="sec", floor_s=floor)
    fire_times: list[float] = []
    lock = threading.Lock()

    def worker():
        for _ in range(5):
            gate.acquire()
            with lock:
                fire_times.append(time.monotonic())

    threads = [threading.Thread(target=worker) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    fire_times.sort()
    gaps = [b - a for a, b in zip(fire_times, fire_times[1:])]
    # Reservation spacing is strict; allow a small jitter tolerance on the
    # observed emission gaps (§3g) but assert the floor is broadly honoured.
    assert min(gaps) >= floor * 0.5
    assert sum(gaps) >= floor * (len(fire_times) - 1) * 0.8


def test_fallback_on_db_error(monkeypatch):
    # A pool whose .connection() raises -> gate must fall back to the
    # in-process floor (no exception, request still paced).
    class BoomPool:
        def connection(self):
            raise RuntimeError("pool down")

    gate = PostgresFloorGate(BoomPool(), budget="sec", floor_s=0.02)
    t0 = time.monotonic()
    gate.acquire()
    gate.acquire()
    assert time.monotonic() - t0 >= 0.02  # second call paced by fallback
```

- [ ] **Step 2: Run to verify it fails**

Run: `docker compose --profile test up -d postgres-test && uv run pytest tests/db/test_postgres_rate_gate.py -v -m db`
Expected: FAIL — `ModuleNotFoundError: app.providers.postgres_rate_gate`.

- [ ] **Step 3: Write the implementation**

```python
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
from typing import Any

from app.providers.rate_gate import InProcessFloorGate

logger = logging.getLogger(__name__)

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
    def __init__(self, pool: Any, *, budget: str = "sec", floor_s: float) -> None:
        self._pool = pool
        self._budget = budget
        self._floor = floor_s
        self._lock = threading.Lock()
        # Shared process-global fallback (§3e): one in-process floor for all
        # SEC paths in this process, so a DB outage degrades to per-process
        # pacing — never unthrottled, never fragmented per gate instance.
        self._fallback = InProcessFloorGate(floor=floor_s)

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
            import time
            time.sleep(wait)

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
```

- [ ] **Step 4: Run to verify it passes**

Run: `uv run pytest tests/db/test_postgres_rate_gate.py -v -m db`
Expected: PASS (2 tests).

- [ ] **Step 5: Lint + typecheck + commit**

```bash
uv run ruff check app/providers/postgres_rate_gate.py tests/db/test_postgres_rate_gate.py
uv run pyright app/providers/postgres_rate_gate.py
git add app/providers/postgres_rate_gate.py tests/db/test_postgres_rate_gate.py
git commit -m "feat(#1484): PostgresFloorGate GCRA gate with in-process fallback"
```

---

## Task 4: Holder module `get_sec_rate_gate` / `set_sec_rate_gate`

**Files:**
- Create: `app/providers/sec_rate_gate_holder.py`
- Test: `tests/providers/test_sec_rate_gate_holder.py`

- [ ] **Step 1: Write the failing tests**

```python
# tests/providers/test_sec_rate_gate_holder.py
from app.providers.rate_gate import InProcessFloorGate
from app.providers import sec_rate_gate_holder as holder


def test_default_is_inprocess_floor():
    holder._reset_sec_rate_gate_for_tests()
    assert isinstance(holder.get_sec_rate_gate(), InProcessFloorGate)


def test_set_then_get_returns_set_gate():
    holder._reset_sec_rate_gate_for_tests()
    sentinel = InProcessFloorGate(floor=0.5)
    holder.set_sec_rate_gate(sentinel)
    assert holder.get_sec_rate_gate() is sentinel


def test_getter_reflects_swap_for_late_importer():
    # Simulates §3b: a module that imports the holder (not the gate value)
    # still sees a gate set AFTER it imported.
    holder._reset_sec_rate_gate_for_tests()
    from app.providers import sec_rate_gate_holder as late_import
    swapped = InProcessFloorGate(floor=0.9)
    holder.set_sec_rate_gate(swapped)
    assert late_import.get_sec_rate_gate() is swapped
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run pytest tests/providers/test_sec_rate_gate_holder.py -v`
Expected: FAIL — `ModuleNotFoundError`.

- [ ] **Step 3: Write the implementation**

```python
# app/providers/sec_rate_gate_holder.py
"""Authoritative process-global SEC rate gate (#1484, §3b).

Accessed via get_sec_rate_gate() at construction/acquire time — NEVER
value-imported — so set_sec_rate_gate() at the composition root propagates
to every consumer module. Default is the in-process floor (correct, just
single-process) so tests / CLI / pool-less callers work without wiring.
"""

from __future__ import annotations

from app.providers.implementations.sec_edgar import _MIN_REQUEST_INTERVAL_S
from app.providers.rate_gate import InProcessFloorGate, RateGate

_sec_rate_gate: RateGate = InProcessFloorGate(floor=_MIN_REQUEST_INTERVAL_S)


def get_sec_rate_gate() -> RateGate:
    return _sec_rate_gate


def set_sec_rate_gate(gate: RateGate) -> None:
    global _sec_rate_gate
    _sec_rate_gate = gate


def _reset_sec_rate_gate_for_tests() -> None:
    global _sec_rate_gate
    _sec_rate_gate = InProcessFloorGate(floor=_MIN_REQUEST_INTERVAL_S)
```

> Note: importing `_MIN_REQUEST_INTERVAL_S` from `sec_edgar` is a constant value-import (fine — it never changes). Only the *gate* must be getter-accessed.

- [ ] **Step 4: Run to verify it passes**

Run: `uv run pytest tests/providers/test_sec_rate_gate_holder.py -v`
Expected: PASS (3 tests).

- [ ] **Step 5: Lint + typecheck + commit**

```bash
uv run ruff check app/providers/sec_rate_gate_holder.py tests/providers/test_sec_rate_gate_holder.py
uv run pyright app/providers/sec_rate_gate_holder.py
git add app/providers/sec_rate_gate_holder.py tests/providers/test_sec_rate_gate_holder.py
git commit -m "feat(#1484): sec_rate_gate_holder getter/setter singleton"
```

---

## Task 5: `ResilientClient` optional `gate` delegation

**Files:**
- Modify: `app/providers/resilient_client.py:58-88` (`__init__`), `:118-139` (`_throttle_and_stamp`), `:158` (call site)
- Test: `tests/providers/test_rate_gate.py` (append)

- [ ] **Step 1: Append the failing test**

```python
# tests/providers/test_rate_gate.py  (append)
def test_resilient_client_delegates_to_gate():
    import httpx
    from app.providers.resilient_client import ResilientClient

    calls = {"n": 0}

    class StubGate:
        def acquire(self): calls["n"] += 1
        async def acquire_async(self): ...

    transport = httpx.MockTransport(lambda req: httpx.Response(200, text="ok"))
    client = httpx.Client(transport=transport)
    rc = ResilientClient(client, gate=StubGate())
    rc.get("https://example.test/x")
    assert calls["n"] == 1
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run pytest tests/providers/test_rate_gate.py::test_resilient_client_delegates_to_gate -v`
Expected: FAIL — `ResilientClient.__init__() got an unexpected keyword argument 'gate'`.

- [ ] **Step 3: Edit `ResilientClient.__init__`** — add the `gate` param

In `app/providers/resilient_client.py`, add to the constructor signature (after `shared_throttle_lock`):

```python
        gate: "RateGate | None" = None,
```

and at the end of `__init__`:

```python
        # #1484: when a RateGate is injected, the throttle delegates to it
        # (cross-process gate) and the legacy shared-clock path is bypassed.
        self._gate = gate
```

Add the import near the top (guarded to avoid a cycle):

```python
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from app.providers.rate_gate import RateGate
```

- [ ] **Step 4: Edit `_throttle_and_stamp`** — delegate when a gate is present

Rename the call at `_request` (line ~158) stays `self._throttle_and_stamp()`. At the top of `_throttle_and_stamp`, before the `_min_interval` logic:

```python
        if self._gate is not None:
            self._gate.acquire()
            return
```

- [ ] **Step 5: Run the test + the existing throttle tests**

Run: `uv run pytest tests/providers/test_rate_gate.py -v && uv run pytest tests/providers -k resilient -v`
Expected: PASS (new test + existing ResilientClient tests unchanged).

- [ ] **Step 6: Lint + typecheck + commit**

```bash
uv run ruff check app/providers/resilient_client.py && uv run pyright app/providers/resilient_client.py
git add app/providers/resilient_client.py tests/providers/test_rate_gate.py
git commit -m "feat(#1484): ResilientClient delegates throttle to injected RateGate"
```

---

## Task 6: SEC providers inject the gate

**Files:**
- Modify: `app/providers/implementations/sec_edgar.py:264-275` (both `ResilientClient(...)` builds)
- Modify: `app/providers/implementations/sec_fundamentals.py:611-613`

- [ ] **Step 1: Edit `sec_edgar.py`** — pass `gate=get_sec_rate_gate()` to both clients

Add a lazy import inside `__init__` (avoid an import cycle — the holder imports `_MIN_REQUEST_INTERVAL_S` from this module):

```python
        from app.providers.sec_rate_gate_holder import get_sec_rate_gate
        _gate = get_sec_rate_gate()
```

Then add `gate=_gate,` to BOTH `ResilientClient(...)` constructions (the `self._http` and `self._http_tickers` builds at lines 264-275). Keep the existing `shared_last_request` / `shared_throttle_lock` args (harmless — ignored when a gate is present; preserves the in-process fallback path for any caller that constructs before wiring).

- [ ] **Step 2: Edit `sec_fundamentals.py`** — same

Add the lazy import + `gate=get_sec_rate_gate()` to the `ResilientClient(...)` at lines 611-613.

- [ ] **Step 3: Verify no import cycle at boot**

Run: `uv run python -c "import app.providers.implementations.sec_edgar; import app.providers.implementations.sec_fundamentals; import app.providers.sec_rate_gate_holder; print('ok')"`
Expected: `ok`.

- [ ] **Step 4: Lint + typecheck + commit**

```bash
uv run ruff check app/providers/implementations/sec_edgar.py app/providers/implementations/sec_fundamentals.py
uv run pyright app/providers/implementations/sec_edgar.py app/providers/implementations/sec_fundamentals.py
git add app/providers/implementations/sec_edgar.py app/providers/implementations/sec_fundamentals.py
git commit -m "feat(#1484): SEC providers inject get_sec_rate_gate() into ResilientClient"
```

---

## Task 7: Async + bulk paths route through the gate

**Files:**
- Modify: `app/services/sec_pipelined_fetcher.py:115-153` (`_AsyncRateLimiter`)
- Modify: `app/services/sec_bulk_refresh.py:308-320`
- Modify: `app/services/sec_bulk_download.py:1322-1335`

- [ ] **Step 1: Edit `_AsyncRateLimiter`** — delegate to the gate by default

In `app/services/sec_pipelined_fetcher.py`, change `_AsyncRateLimiter.__init__` to accept an optional `gate` and resolve it:

```python
    def __init__(
        self,
        target_rps: float,
        *,
        gate: "RateGate | None" = None,
        shared_clock: list[float] | None = None,
        shared_lock: threading.Lock | None = None,
    ) -> None:
        if target_rps <= 0:
            raise ValueError("target_rps must be > 0")
        # #1484: default to the process-global cross-process gate. An
        # explicit shared_clock (tests) keeps the legacy in-process floor for
        # isolation; target_rps is advisory on the gate path (the gate's own
        # floor governs).
        if gate is None and shared_clock is None:
            from app.providers.sec_rate_gate_holder import get_sec_rate_gate
            gate = get_sec_rate_gate()
        self._gate = gate
        self._min_interval = 1.0 / target_rps
        self._clock = shared_clock if shared_clock is not None else [0.0]
        self._lock = shared_lock if shared_lock is not None else threading.Lock()
```

And in `acquire`, delegate first:

```python
    async def acquire(self) -> None:
        if self._gate is not None:
            await self._gate.acquire_async()
            return
        # ... existing in-process two-phase floor (unchanged) ...
```

Add the TYPE_CHECKING import for `RateGate`.

- [ ] **Step 2: Edit `sec_bulk_refresh.py`** — drop the clock-pair plumbing

Replace the lazy clock import + `_AsyncRateLimiter(target_rps=7.0, shared_clock=..., shared_lock=...)` (lines 308-320) with:

```python
    from app.services.sec_pipelined_fetcher import _AsyncRateLimiter

    # #1484: no shared_clock -> _AsyncRateLimiter defaults to the
    # cross-process gate. The old per-process 7 rps self-limit is subsumed
    # by the global gate floor.
    rate_limiter = _AsyncRateLimiter(target_rps=7.0)
```

- [ ] **Step 3: Edit `sec_bulk_download.py`** — same replacement at lines 1322-1335

```python
    from app.services.sec_pipelined_fetcher import _AsyncRateLimiter

    rate_limiter = _AsyncRateLimiter(target_rps=7.0)
```

- [ ] **Step 4: Run the existing pipelined-fetcher tests (must still pass via explicit-clock isolation path)**

Run: `uv run pytest tests/ -k "pipelined or rate_limiter or bulk_refresh or bulk_download" -v -m "not db"`
Expected: PASS (tests that pass an explicit `shared_clock` keep the in-process path; others delegate to the default gate, which in tests is the in-process default).

- [ ] **Step 5: Lint + typecheck + commit**

```bash
uv run ruff check app/services/sec_pipelined_fetcher.py app/services/sec_bulk_refresh.py app/services/sec_bulk_download.py
uv run pyright app/services/sec_pipelined_fetcher.py app/services/sec_bulk_refresh.py app/services/sec_bulk_download.py
git add app/services/sec_pipelined_fetcher.py app/services/sec_bulk_refresh.py app/services/sec_bulk_download.py
git commit -m "feat(#1484): async + bulk SEC paths route through the shared gate"
```

---

## Task 8: Wire the gate at both composition roots + conn-budget note

**Files:**
- Modify: `app/main.py:251-253` (after `pool = open_pool("db_pool", ...)`)
- Modify: `app/jobs/__main__.py:895` (after `pool = open_pool("jobs_pool", ...)`)
- Modify: `app/db/pg_settings.py:255-272` (`_dev_profile_connection_demand` docstring/comment)

- [ ] **Step 1: Edit `app/main.py`** — set the gate right after the request pool opens

Immediately after `app.state.db_pool = pool` (line 253):

```python
    # #1484: install the cross-process SEC rate gate, backed by the request
    # pool, BEFORE any SEC provider is constructed in this process.
    from app.providers.implementations.sec_edgar import _MIN_REQUEST_INTERVAL_S
    from app.providers.postgres_rate_gate import PostgresFloorGate
    from app.providers.sec_rate_gate_holder import set_sec_rate_gate

    set_sec_rate_gate(PostgresFloorGate(pool, budget="sec", floor_s=_MIN_REQUEST_INTERVAL_S))
    logger.info("SEC cross-process rate gate installed (API process).")
```

- [ ] **Step 2: Edit `app/jobs/__main__.py`** — set the gate right after the jobs pool opens (line 895)

```python
    from app.providers.implementations.sec_edgar import _MIN_REQUEST_INTERVAL_S
    from app.providers.postgres_rate_gate import PostgresFloorGate
    from app.providers.sec_rate_gate_holder import set_sec_rate_gate

    set_sec_rate_gate(PostgresFloorGate(pool, budget="sec", floor_s=_MIN_REQUEST_INTERVAL_S))
    logger.info("SEC cross-process rate gate installed (jobs process).")
```

- [ ] **Step 3: Edit `pg_settings.py`** — document the transient gate-conn term

In the `_dev_profile_connection_demand` docstring (lines 255-272), append a paragraph:

```python
    #1484 — the SEC rate gate (PostgresFloorGate) borrows ONE pooled conn per
    acquire for ~1 ms under a process-local lock (<=1 gate conn/process), then
    releases before sleeping. This is transient pool usage, not steady-state
    demand: it does not raise any pool max, so the returned total is
    unchanged. Worst-case concurrent transient is <=1/process (<=2 across
    API+jobs), well within the reserve.
```

- [ ] **Step 4: Boot both processes against dev to confirm wiring (smoke)**

Run: `uv run pytest tests/smoke/test_app_boots.py -v`
Expected: PASS — lifespan installs the gate without error.

- [ ] **Step 5: Lint + typecheck + commit**

```bash
uv run ruff check app/main.py app/jobs/__main__.py app/db/pg_settings.py
uv run pyright app/main.py app/jobs/__main__.py app/db/pg_settings.py
git add app/main.py app/jobs/__main__.py app/db/pg_settings.py
git commit -m "feat(#1484): wire PostgresFloorGate at API + jobs composition roots"
```

---

## Task 9: 429 / UA-throttle counter + health surface (§4.4)

**Files:**
- Modify: `app/providers/resilient_client.py:184-197` (the retry-warning block)
- Modify: the SEC health surface (locate at implementation: `grep -rn "postgres-health\|/system/" app/api | head`); add a `sec_throttle_429_total` field sourced from a process counter.
- Test: `tests/providers/test_rate_gate.py` (append a counter-increment assertion)

- [ ] **Step 1: Add a module-level counter to `resilient_client.py`**

```python
import itertools
_SEC_429_COUNTER = itertools.count()
def sec_throttle_429_count() -> int:
    # next() on a fresh count() starts at 0; expose the consumed total.
    return next(_SEC_429_COUNTER) - _consumed  # see Step 2 note
```

> Implementation note: use a simple `threading.Lock`-guarded int instead of `itertools.count` for a readable total:

```python
import threading
_sec_429_lock = threading.Lock()
_sec_429_total = 0

def _incr_sec_429() -> None:
    global _sec_429_total
    with _sec_429_lock:
        _sec_429_total += 1

def sec_throttle_429_total() -> int:
    with _sec_429_lock:
        return _sec_429_total
```

- [ ] **Step 2: Increment on a 429 in the retry block**

In `_request`, inside the `if response.status_code == 429 or ... in _RETRYABLE_5XX:` branch, guard on 429 specifically:

```python
                if response.status_code == 429:
                    _incr_sec_429()
```

- [ ] **Step 3: Append the test**

```python
# tests/providers/test_rate_gate.py (append)
def test_429_increments_counter():
    import httpx
    from app.providers import resilient_client as rc_mod
    from app.providers.resilient_client import ResilientClient, sec_throttle_429_total

    before = sec_throttle_429_total()
    seq = iter([httpx.Response(429, headers={"retry-after": "0.1"}), httpx.Response(200, text="ok")])
    transport = httpx.MockTransport(lambda req: next(seq))
    rc = ResilientClient(httpx.Client(transport=transport), max_retries=1, backoff_schedule=(0.0,))
    rc.get("https://example.test/x")
    assert sec_throttle_429_total() == before + 1
```

- [ ] **Step 4: Surface the counter on the health endpoint**

Locate the system-health response builder (`grep -rn "postgres-health" app/api`). Add `sec_throttle_429_total` to its payload, importing `sec_throttle_429_total` from `resilient_client`. (Exact file/field naming follows the existing health-payload pattern — mirror a sibling counter field.)

- [ ] **Step 5: Run + lint + commit**

```bash
uv run pytest tests/providers/test_rate_gate.py -v
uv run ruff check app/providers/resilient_client.py && uv run pyright app/providers/resilient_client.py
git add app/providers/resilient_client.py tests/providers/test_rate_gate.py app/api
git commit -m "feat(#1484): SEC 429 counter + health surface for throttle visibility"
```

---

## Task 10: Skill-doc corrections (skill-ownership rule)

**Files:**
- Modify: `.claude/skills/data-sources/sec-edgar.md` §4 (the `_PROCESS_RATE_LIMIT_CLOCK` "across every ingest job" claim)
- Modify: `.claude/skills/data-engineer/etl-endpoint-coverage.md` lines 23 + 163

- [ ] **Step 1: Edit `sec-edgar.md` §4**

Change the sentence claiming the in-process clock enforces 10 r/s "across every ingest job" to note it bounds a single PROCESS, and that #1484's `sec_rate_gate` GCRA gate makes the **reservation rate** global per-IP across processes. Cite `docs/specs/ops/2026-06-09-sec-cross-process-rate-limiter.md`.

- [ ] **Step 2: Edit `etl-endpoint-coverage.md`**

Line 23 ("Rate-limit pool — shared per-IP budget | per-host clock + lock") and line 163 ("10 req/s shared per-IP … `_PROCESS_RATE_LIMIT_CLOCK`"): annotate that the in-process clock is PER-PROCESS and the cross-process budget is enforced by `sec_rate_gate` (#1484).

- [ ] **Step 3: Commit**

```bash
git add .claude/skills/data-sources/sec-edgar.md .claude/skills/data-engineer/etl-endpoint-coverage.md
git commit -m "docs(#1484): correct skill claims — in-process clock is per-process, gate is global"
```

---

## Task 11: Full pre-push gate + dev-verify

- [ ] **Step 1: Run the fast tier + smoke**

Run:
```bash
uv run ruff check . && uv run ruff format --check .
uv run pyright
uv run pytest -m "not db"
uv run pytest tests/smoke
```
Expected: all green.

- [ ] **Step 2: Run the DB tier for the gate**

Run: `docker compose --profile test up -d postgres-test && uv run pytest -m db -k "rate_gate or postgres_rate"`
Expected: PASS.

- [ ] **Step 3: Dev-verify (operator) — record outputs in the PR description (§8 of the spec)**

1. Restart both processes onto the branch SHA (jobs via the operator `kill -9` + `nohup uv run python -m app.jobs` method; API via `stack-restart.sh`).
2. Two-process hammer: a side script opening two processes that each call `PostgresFloorGate(pool).acquire()` in a loop; confirm combined observed fire-rate ≤ ~9 req/s.
3. Drive a manual SEC sweep (jobs at ceiling) + a `GET /instruments/{sym}/8-k/{accession}` lazy fill (API); confirm the combined second never exceeds 10 (via the §4.4 counter / SEC request logs) and the click returns ≤ ~1 s.
4. `/system/postgres-health` connection count stays under budget during a 4-concurrent-SEC-job + API-click window.

- [ ] **Step 4: Codex checkpoint-2 (before first push)**

Run: `codex exec "review"` on the branch diff. Fix anything real before pushing.

- [ ] **Step 5: Push + open PR**

```bash
git push -u origin fix/1484-sec-cross-process-rate-limiter
gh pr create --fill
```

Then poll `gh pr view <n> --comments` + `gh pr checks <n>` per the branch/PR workflow until APPROVE on the latest commit with CI green.

---

## Self-Review notes

- **Spec coverage:** §3a→T1+T3 (SQL/gate), §3b→T4+T6 (getter/wiring), §3d→T3+T8 (conn bound), §3e→T3 (fallback), §3f→T6+T7 (all three consumer families), §3g→T3 test tolerance + spec wording, §4.1→T2+T3 (sync+async), §4.4→T9 (counter), §5 skill fix→T10, §7 tests→T2/T3/T4/T5/T9, §8 rollout→T11.
- **Reservation-vs-emission (§3g):** the DB test (T3) asserts the floor with jitter tolerance, not strict equality — matching the honest guarantee.
- **Type consistency:** `RateGate.acquire`/`acquire_async`, `PostgresFloorGate(pool, *, budget, floor_s)`, `get_sec_rate_gate()`/`set_sec_rate_gate()`/`_reset_sec_rate_gate_for_tests()`, `sec_throttle_429_total()` — names identical across tasks.
- **Open item for executor:** confirm the exact system-health payload file/field in T9 Step 4 (mirror an existing counter field; do not invent a new endpoint).
