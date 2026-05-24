# Cascade advisory lock (#276 K.3) — design v2

**Goal:** prevent `cascade_refresh` and `daily_thesis_refresh` from concurrently generating a thesis for the same instrument. Duplicate Claude calls + race on thesis row commit.

**Scope:** K.3 core + a K.2 durability fix that Codex surfaced while reviewing K.3 (helper writes on savepoint under implicit tx can be lost by a later cascade rollback).

**Revision history:**
- v1 — initial.
- v2 — Codex-driven: (a) helpers now commit explicitly so later rollbacks can't destroy durable queue state; (b) `instrument_lock` unlock handles INERROR conn; (c) `daily_thesis_refresh` on success resolves the pending cascade row.
- v3/v4 — Codex-driven: daily success demotes pending thesis-failure / LOCKED_BY_SIBLING rows to RERANK_NEEDED via `demote_to_rerank_needed` rather than deleting them, so the rankings-recompute signal survives until cascade's own rerank succeeds. Only cascade's post-rerank-success path DELETEs via `clear_retry_success`.

---

## Problem

After K.2:

- `cascade_refresh` runs inside `daily_financial_facts` and calls `generate_thesis` per queued + stale instrument.
- `daily_thesis_refresh` runs on its own schedule and also calls `generate_thesis` per T1/T2 stale instrument.
- Overlap = two Claude calls + commit race on thesis row. Wasted budget + audit muddle.

Transaction-level locking is wrong — `generate_thesis` commits before Claude (#293), so an xact lock would release mid-call.

Separately, Codex K.3 review surfaced a K.2 durability gap: queue helpers wrap writes in `with conn.transaction():` which creates savepoints under the implicit outer tx. A later `conn.rollback()` inside `cascade_refresh` rolls back the outer tx and discards the savepoint writes. The scheduler commits AFTER `cascade_refresh` returns, so post-cascade state is durable — but mid-cascade rollbacks (thesis exception handler, rerank exception handler) can erase prior enqueue writes from earlier loop iterations. K.3 fixes this.

## Solution

### Core: session-level advisory lock

`pg_try_advisory_lock(bigint)` keyed on `instrument_id`. Held for the duration of the `generate_thesis` call (including Claude round-trip) and released in `finally` via `pg_advisory_unlock`. Non-blocking — `try_lock` returns immediately.

Skip semantics:

- `cascade_refresh`: write/preserve a `cascade_retry_queue` row with `last_error='LOCKED_BY_SIBLING'`, `attempt_count=0`. Does NOT consume retry budget.
- `daily_thesis_refresh`: log informational, `skipped += 1`. No queue write on skip.
- **New (fix for Codex v1 HIGH — at-cap stuck-row path + Codex v2/v3 HIGH — RERANK_NEEDED preservation)**: `daily_thesis_refresh` on per-instrument SUCCESS calls a NEW helper `demote_to_rerank_needed(conn, iid)` that UPDATEs any pending thesis-failure / LOCKED_BY_SIBLING row to `last_error='RERANK_NEEDED'`, `attempt_count=0`. Daily's thesis write resolves the thesis-level signal but does NOT run `compute_rankings`, so the row must persist as a durable rankings-recompute signal for the next cascade cycle. Demote-rather-than-delete. Pre-existing RERANK_NEEDED rows are left untouched. Cascade's own post-rerank-success path continues to use the unconditional `clear_retry_success` because cascade at that point resolved BOTH thesis and rerank.

### K.2 durability fix

Helpers drop `with conn.transaction():` and instead `conn.execute(...)` then `conn.commit()`. The commit is durable regardless of prior implicit-tx state; caller rollbacks can no longer erase prior queue writes. All four existing helpers (`enqueue_retry`, `enqueue_rerank_marker`, `clear_retry_success`, and new `enqueue_locked_by_sibling`) adopt this pattern.

Trade-off: explicit `conn.commit()` calls incompatible with any future caller that wraps cascade in an outer `with conn.transaction():`. Cascade callers today don't do that. Documented in each helper's docstring. Compatible with future K.4 observability which will wrap around cascade, not inside it.

## Helpers — `app/services/refresh_cascade.py`

```python
from contextlib import contextmanager
from collections.abc import Iterator

LOCKED_BY_SIBLING: str = "LOCKED_BY_SIBLING"


@contextmanager
def instrument_lock(
    conn: psycopg.Connection[Any],
    instrument_id: int,
) -> Iterator[bool]:
    """Session-level advisory lock on instrument_id. Yields True if
    acquired, False if a sibling session holds it.

    Session-level (NOT xact-level) so the lock spans generate_thesis
    including its internal commit-before-Claude (#293). Unlock in
    finally handles conn-INERROR by attempting rollback before the
    unlock SQL, and tolerates unlock failure (session close
    guarantees Postgres releases the lock).
    """
    acquired_row = conn.execute(
        "SELECT pg_try_advisory_lock(%s)", (instrument_id,)
    ).fetchone()
    acquired = bool(acquired_row[0]) if acquired_row else False
    try:
        yield acquired
    finally:
        if not acquired:
            return
        try:
            conn.execute("SELECT pg_advisory_unlock(%s)", (instrument_id,))
        except psycopg.Error:
            # Conn may be INERROR from the protected block. Roll back
            # and retry unlock once. If retry still fails, session
            # close will eventually free the lock — log and continue
            # rather than masking the caller's original exception.
            try:
                conn.rollback()
                conn.execute("SELECT pg_advisory_unlock(%s)", (instrument_id,))
            except psycopg.Error:
                logger.exception(
                    "instrument_lock: unlock failed for instrument_id=%d — "
                    "session close will release",
                    instrument_id,
                )


def enqueue_locked_by_sibling(
    conn: psycopg.Connection[Any],
    instrument_id: int,
) -> None:
    """INSERT ... ON CONFLICT DO NOTHING — preserves any existing
    queue row (at-cap or RERANK_NEEDED). Fresh insert records
    last_error='LOCKED_BY_SIBLING' and attempt_count=0 so the
    next cycle re-drains without consuming thesis budget.
    Commits explicitly so later cascade rollbacks cannot erase
    the skip marker (see module-level durability note).
    """
    conn.execute(
        """
        INSERT INTO cascade_retry_queue
            (instrument_id, attempt_count, last_error, last_attempted_at)
        VALUES (%s, 0, 'LOCKED_BY_SIBLING', NOW())
        ON CONFLICT (instrument_id) DO NOTHING
        """,
        (instrument_id,),
    )
    conn.commit()
```

Also refactor existing helpers (`enqueue_retry`, `enqueue_rerank_marker`, `clear_retry_success`) to the same pattern: single `conn.execute` + explicit `conn.commit()`, dropping `with conn.transaction():`.

New demote helper for `daily_thesis_refresh`:

```python
def demote_to_rerank_needed(
    conn: psycopg.Connection[Any],
    instrument_id: int,
) -> None:
    """On daily_thesis_refresh success, convert any pending
    thesis-failure / LOCKED_BY_SIBLING row to RERANK_NEEDED /
    attempt_count=0. Daily's write resolves the pending thesis
    signal but does NOT run compute_rankings, so the row must
    persist (demoted) as a durable rankings-recompute signal
    until a real cascade rerank succeeds and the unconditional
    clear_retry_success path deletes it.

    Codex v3 HIGH: an earlier design version DELETE'd non-
    RERANK_NEEDED rows here. That dropped the rerank signal
    whenever daily succeeded on a thesis-failure / lock-skipped
    instrument, leaving rankings stale. Demote-rather-than-
    delete preserves the signal.

    Pre-existing RERANK_NEEDED rows are left untouched — they
    already carry the correct signal.
    """
    conn.execute(
        """
        UPDATE cascade_retry_queue
           SET attempt_count = 0,
               last_error = 'RERANK_NEEDED',
               last_attempted_at = NOW()
         WHERE instrument_id = %s
           AND last_error != 'RERANK_NEEDED'
        """,
        (instrument_id,),
    )
    conn.commit()
```

## cascade_refresh changes

Per-instrument loop (both retry path and stale path) wrapped with `instrument_lock`:

```python
with instrument_lock(conn, iid) as acquired:
    if not acquired:
        enqueue_locked_by_sibling(conn, iid)
        logger.info("cascade_refresh: LOCKED_BY_SIBLING instrument_id=%d", iid)
        locked_skipped += 1
        continue
    # existing generate_thesis + processed_ok / failed / enqueue_retry logic
```

`CascadeOutcome` gains `locked_skipped: int = 0`. Locked-skips are NOT in `failed`, do NOT trigger the scheduler raise, and do NOT count as thesis attempts.

## daily_thesis_refresh changes — `app/workers/scheduler.py`

Per-instrument loop wraps with `instrument_lock`:

```python
with psycopg.connect(settings.database_url) as conn:
    with instrument_lock(conn, item.instrument_id) as acquired:
        if not acquired:
            logger.info(
                "daily_thesis_refresh: LOCKED_BY_SIBLING symbol=%s instrument_id=%d",
                item.symbol, item.instrument_id,
            )
            skipped += 1
            report_progress(idx, total)
            continue
        generate_thesis(
            instrument_id=item.instrument_id,
            conn=conn,
            client=claude_client,
        )
        # On success, demote any pending thesis-failure /
        # LOCKED_BY_SIBLING row to RERANK_NEEDED — daily's thesis
        # write subsumes cascade's pending thesis signal but does
        # not run compute_rankings, so the row must persist as a
        # rankings-recompute signal. Pre-existing RERANK_NEEDED
        # rows are untouched. Only cascade's post-rerank-success
        # path DELETEs via clear_retry_success.
        demote_to_rerank_needed(conn, item.instrument_id)
```

Each per-instrument `psycopg.connect(...)` gives its own session. Cross-connection advisory-lock contention blocks cascade's long-lived conn against daily's per-instrument conn — Postgres advisory locks are global-keyed across sessions.

## Tests

**Unit (mock conn):**

1. `instrument_lock` acquired: executes `pg_try_advisory_lock`, yields True, executes `pg_advisory_unlock` in finally.
2. `instrument_lock` not acquired: yields False, no unlock call.
3. `instrument_lock` unlocks even if body raises.
4. `instrument_lock` unlock path tolerates psycopg.Error on the first attempt: rollback + retry.
5. `enqueue_locked_by_sibling` issues INSERT ... ON CONFLICT DO NOTHING with count=0 literal and 'LOCKED_BY_SIBLING' + explicit conn.commit().
6. All existing helpers now call `conn.commit()` directly (`enqueue_retry`, `enqueue_rerank_marker`, `clear_retry_success`). Test assertions on `conn.transaction.assert_called_once()` replaced with `conn.commit.assert_called()`.
7. `cascade_refresh` per-instrument: lock acquired → normal path, no locked_skipped.
8. `cascade_refresh` per-instrument: lock not acquired → `enqueue_locked_by_sibling` called, `generate_thesis` NOT called, no entry in `failed`, `locked_skipped += 1`.
9. `cascade_refresh` outcome summary includes `locked_skipped`.
10. `cascade_refresh` lock-skip does NOT trigger scheduler raise (outcome.failed stays empty for locked rows).
11. Regression: mid-cascade rollback does NOT destroy prior enqueue_retry writes (covered by the commit-after-execute pattern; integration-level test below).

**Integration (real DB):**

12. Two connections: A acquires lock for iid=1, B gets False on same iid. After A releases, B can acquire.
13. Acquire + run invalid SQL inside lock body + exit: a second connection can acquire. Exercises the INERROR-recovery unlock path.
14. `enqueue_locked_by_sibling` on empty queue inserts count=0 / LOCKED_BY_SIBLING.
15. `enqueue_locked_by_sibling` on at-cap row is a no-op — preserves count and last_error.
16. Durability: `enqueue_retry` on a conn with implicit read tx → mid-function call to `conn.rollback()` does NOT erase the enqueue row. Exercises the K.2 durability fix.
17. `daily_thesis_refresh` path integration (scheduler-mocked Claude): success demotes a thesis-failure row to RERANK_NEEDED; skip leaves row untouched.
18. `demote_to_rerank_needed` on thesis-failure row sets last_error='RERANK_NEEDED' + attempt_count=0 and preserves the row (Codex v3 HIGH regression cover — row must survive for cascade's next rerank).
19. `demote_to_rerank_needed` on LOCKED_BY_SIBLING row demotes similarly — daily success + cascade lock-skip both resolve to a rankings-recompute signal.
20. `demote_to_rerank_needed` on an existing RERANK_NEEDED row is a no-op (WHERE clause filters it out).

## Risks

- **Lock leaks on crash**: session-level locks persist until connection closes. `finally` unlock + connection-cleanup guarantees release. Postgres cleans up on connection death.
- **Claude-call hold time**: lock held ~10-30s per thesis. Acceptable — this is exactly the mutual-exclusion window we want.
- **`conn.commit()` vs outer explicit tx**: callers that wrap cascade in `with conn.transaction():` would break the commit-inside-helper pattern. No caller does this today. Documented in docstrings.
- **LOCKED_BY_SIBLING benign race** (Codex LOW): holder can finish and clear the queue row before the loser inserts the marker. Resurrects a fresh count-0 row, next cycle does one wasted Claude call. Accepted — correctness holds, only efficiency is bounded.

## Shipping order

1. Helper refactor (commit-after-execute) + new `instrument_lock` / `enqueue_locked_by_sibling` / `LOCKED_BY_SIBLING`.
2. `cascade_refresh` wiring.
3. `daily_thesis_refresh` wiring (with post-success `demote_to_rerank_needed` — daily does not DELETE queue rows; only cascade's post-rerank-success path does).
4. Unit tests (refactored + new) + integration tests.
5. Codex pre-push + PR.
