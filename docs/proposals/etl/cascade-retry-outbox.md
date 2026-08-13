# Cascade retry outbox (#276 K.2) — design v2

**Goal:** durable retry queue for per-instrument cascade failures. Closes the post-K.1 gap where SEC watermarks commit before cascade, so a cascade failure never triggers a re-plan of the same CIK on the next `daily_financial_facts` run.

**Scope:** K.2 only. K.3 (advisory lock) and K.4 (observability) are separate follow-ups.

**Revision history:**
- v1: initial proposal.
- v2: Codex-driven rewrite. Changes: retry path bypasses stale gate, enqueue happens in a fresh tx after rollback, clear-on-success is non-atomic-but-idempotent, scheduler always invokes cascade on api-key-set days, attempt_count semantics pinned.

---

## Problem

Post-K.1 state:

- `daily_financial_facts` commits SEC facts + normalization before calling `cascade_refresh`.
- Cascade per-instrument thesis failure → `CascadeOutcome.failed` entry → scheduler raises RuntimeError so `_tracked_job` records failure.
- Next run's `plan_refresh`: watermarks already advanced, same CIK is not in `plan.refreshes`.
- Re-entry via the event predicate (`filing_events.created_at > thesis_generated_at`) works **only** when `filing_events` row for this CIK remains newer than thesis. True on the next run — but fragile:
  - If thesis row was partially written (unlikely given #293's commit-before-Claude + atomic write, but possible on unexpected DB error), the predicate comparison can miss.
  - If another path (manual thesis refresh, `daily_thesis_refresh`) wrote a thesis after the failure, the event predicate drops the CIK from the stale set — cascade never retries the failure.
  - No visibility / retry cap — a broken CIK silently retries forever or silently drops forever, with no ops signal.

Master plan K.2 specifies a durable outbox to make retries deterministic and observable.

## Non-goals

- Advisory locking against `daily_thesis_refresh` concurrent writes (K.3).
- Admin UI surface for queue rows at attempt cap (K.4 or Chunk H).
- Retry queue observability / freshness predicate integration (K.4).

---

## Schema — migration 037

```sql
CREATE TABLE cascade_retry_queue (
    instrument_id     BIGINT PRIMARY KEY REFERENCES instruments(instrument_id) ON DELETE CASCADE,
    enqueued_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_attempted_at TIMESTAMPTZ,
    attempt_count     INTEGER NOT NULL DEFAULT 0,
    last_error        TEXT NOT NULL,
    CONSTRAINT cascade_retry_queue_attempt_count_nonneg
        CHECK (attempt_count >= 0)
);

CREATE INDEX idx_cascade_retry_queue_enqueued
    ON cascade_retry_queue(enqueued_at);
```

`ON DELETE CASCADE` per Codex LOW — eBull does not hard-delete instruments today, but the defensive FK is trivial. No backfill migration — queue starts empty.

## Attempt-count semantics (fixed per Codex HIGH)

`attempt_count` = **number of failed attempts observed so far**.

| Event | `attempt_count` after |
|---|---|
| No prior row, failure → INSERT | 1 |
| Existing row, subsequent failure → UPDATE | +1 |
| K.3 LOCKED_BY_SIBLING, no prior row | 0 (fresh row, no attempt consumed) |
| K.3 LOCKED_BY_SIBLING, existing row | unchanged |
| Success → DELETE | row gone |

Eligible for drain: `attempt_count < ATTEMPT_CAP (5)`.

## Module changes — `app/services/refresh_cascade.py`

New constants and helpers:

```python
ATTEMPT_CAP: int = 5

def enqueue_retry(
    conn: psycopg.Connection[Any],
    instrument_id: int,
    error_type: str,
) -> None:
    """UPSERT a retry row in its OWN transaction. Safe to call on a
    connection that was just rolled back from a failed generate_thesis
    — this function wraps its write in ``with conn.transaction():``
    so it commits independently.

    INSERT ... ON CONFLICT (instrument_id) DO UPDATE SET
        attempt_count = cascade_retry_queue.attempt_count + 1,
        last_error = EXCLUDED.last_error,
        last_attempted_at = NOW()
    — on first enqueue ``DEFAULT 0 + 1 = 1`` via an INSERT-side
    ``attempt_count = 1``. INSERT sets count=1, UPDATE increments.
    """

def clear_retry_success(
    conn: psycopg.Connection[Any],
    instrument_id: int,
) -> None:
    """DELETE the retry row — idempotent (no-op if row absent).
    Wraps in its own ``with conn.transaction():`` so it commits
    independently of generate_thesis's internal commit."""

def drain_retry_queue(
    conn: psycopg.Connection[Any],
    cap: int = ATTEMPT_CAP,
) -> list[int]:
    """SELECT instrument_id FROM cascade_retry_queue
    WHERE attempt_count < cap
    ORDER BY enqueued_at ASC
    — oldest first. Rows at or above cap are left for admin
    inspection (surfaced in Chunk H / K.4)."""

def enqueue_rerank_marker(
    conn: psycopg.Connection[Any],
    instrument_id: int,
) -> None:
    """Insert a RERANK_NEEDED marker for an instrument whose
    thesis refreshed successfully this cycle but whose rerank then
    failed. attempt_count stays 0 — rerank failures do not consume
    the thesis retry budget.

    INSERT ... ON CONFLICT (instrument_id) DO UPDATE SET
        last_error = 'RERANK_NEEDED',
        attempt_count = 0,
        last_attempted_at = NOW()
    — thesis succeeded this cycle, so any prior thesis-failure
    state (including at-cap rows) is no longer the current blocker;
    resetting to RERANK_NEEDED / attempt_count=0 makes the row
    drainable again so the next cycle can re-attempt rerank.
    Wraps in its own ``with conn.transaction():``."""
```

### cascade_refresh flow changes

`cascade_refresh` signature unchanged. Body reorganized:

1. **Entry**: `retry_ids = drain_retry_queue(conn)`. Log `cascade_refresh: drained N retries`.
2. **Processed-success tracking**: local `processed_ok: list[int] = []`. This defers all queue-clear operations to AFTER `compute_rankings` succeeds (Codex v2 HIGH 2 — otherwise a thesis-succeeds-then-rerank-fails run loses the durable retry signal).
3. **Retry path** (bypasses stale gate, per Codex v1 BLOCKER 2):
   - For each `iid` in `retry_ids`:
     - Call `generate_thesis(iid, conn, client)` directly — the outbox IS the signal that this instrument needs a thesis refresh.
     - On success: `processed_ok.append(iid)`, increment `thesis_refreshed`. (Queue not yet cleared.)
     - On failure: `conn.rollback()` first (Codex v1 HIGH 1), then `enqueue_retry(conn, iid, exc_type)` in a fresh tx, append to `failed`.
4. **New-work path** (existing stale-gated path):
   - `stale = find_stale_instruments(conn, tier=None, instrument_ids=instrument_ids)` — unchanged.
   - For each stale row: same try/except as today.
   - On per-instrument success: `processed_ok.append(iid)`.
   - On per-instrument failure: `conn.rollback()` first, then `enqueue_retry(conn, iid, exc_type)` in a fresh tx, then `failed.append(...)`.
5. **Rerank**:
   - On rerank success: for each `iid` in `processed_ok`, call `clear_retry_success(conn, iid)`. Any pre-existing queue rows and any new-work successes resolve together.
   - On rerank failure: first `conn.rollback()` (mirrors the thesis-failure pattern — `compute_rankings` is SQL-heavy and can leave the connection INERROR; without this, the subsequent marker inserts would fail and the durable signal would be lost for exactly the path it is meant to preserve). Then record `(-1, ExcType)` in `failed`. Then iterate `processed_ok` and call `enqueue_rerank_marker(conn, iid)` for each. The helper UPSERTs with `ON CONFLICT DO UPDATE` to RERANK_NEEDED / `attempt_count=0`, which resets any pre-existing at-cap thesis-failure state — a thesis success this cycle means the prior thesis-failure blocker is no longer current. `attempt_count=0` keeps the retry budget scoped to thesis failures; rerank failures do not consume it.
6. **Short-circuit**: if `retry_ids` and `instrument_ids` are both empty AND `stale` is empty, return noop outcome (no-op run).

### Why retry path bypasses stale gate (Codex BLOCKER 2 resolution)

Stale gate exists to avoid spurious thesis calls for instruments with no new events. Queued retries are already known to need a thesis — the enqueue happened *because* the thesis failed. Re-checking stale state on a queued retry would drop instruments whose thesis was subsequently generated by another path (manual / `daily_thesis_refresh`) while leaving the queue row in place, causing both (a) missed ranking refresh and (b) permanently stuck queue row.

Trade-off: occasionally we regenerate a thesis that was just generated elsewhere. That's a bounded wasted Claude call, not a correctness issue. Thesis writes are per-cycle idempotent.

## Transaction semantics (fixed per Codex HIGH 1, HIGH 2)

- **Enqueue after failure**: rollback the outer connection first to clear any `INERROR` state from the failed thesis, THEN call `enqueue_retry` which opens its own fresh transaction via `with conn.transaction():`. This guarantees the outbox write is durable even if the pre-thesis read tx was aborted.
- **Clear on success**: not atomic with the thesis write. `generate_thesis` commits internally per #293; `clear_retry_success` commits in its own subsequent transaction. Crash between the two leaves the queue row in place. **Idempotent recovery**: next cycle drains the row, regenerates thesis (already written, idempotent per the UPSERT pattern in thesis storage), clears the row. Worst case is one extra Claude call, not incorrect state.
- **Rollback-first, enqueue-second ordering** removes the "discarded outbox write" hazard Codex flagged.

## Scheduler — `app/workers/scheduler.py`

Three changes:

1. **Always invoke cascade when API key is set** (Codex v1 BLOCKER 1 — spec v1 skipped cascade on no-new-SEC-work days, starving the retry queue). Current code:

   ```python
   changed_ids = changed_instruments_from_outcome(conn, plan, outcome)
   if changed_ids:
       cascade_refresh(conn, cascade_client, changed_ids)
   ```

   Becomes:

   ```python
   changed_ids = changed_instruments_from_outcome(conn, plan, outcome)
   cascade_outcome = cascade_refresh(conn, cascade_client, changed_ids)
   # drain path runs inside even when changed_ids is empty
   ```

   `cascade_refresh` returns the empty-noop `CascadeOutcome` when both the retry queue and `instrument_ids` are empty.

2. **Preserve the post-cascade `conn.commit()`** (Codex v2 HIGH 1). K.1 placed the commit inside `if changed_ids:` — now that cascade fires unconditionally on api-key-set days, the commit moves outside that gate too. Shape:

   ```python
   cascade_outcome = cascade_refresh(conn, cascade_client, changed_ids)
   conn.commit()  # persist cascade-side writes (rankings, queue clears)
                  # before the failure-surfacing raise below discards
                  # them via psycopg.connect()'s CM rollback.
   logger.info("cascade_refresh outcome: ...", ...)
   if cascade_outcome.failed:
       raise RuntimeError(...)
   ```

3. Log message unchanged. Failure-raise logic unchanged.

## Tests

Unit (mock conn) covering:

1. `enqueue_retry` inserts row with `attempt_count=1` on empty queue.
2. `enqueue_retry` on existing row increments `attempt_count` and updates `last_error` + `last_attempted_at`.
3. `clear_retry_success` deletes existing row.
4. `clear_retry_success` is idempotent on empty queue.
5. `drain_retry_queue` returns rows with `attempt_count < cap`, skipping at-cap rows.
6. `drain_retry_queue` returns empty list when queue is empty.
7. `drain_retry_queue` orders by `enqueued_at` ASC.
8. `cascade_refresh` drains queue and processes retry path BYPASSING `find_stale_instruments`.
9. `cascade_refresh` retry-path success plus rerank success → `clear_retry_success` called after rerank.
10. `cascade_refresh` retry-path failure calls `enqueue_retry` (after rollback).
11. `cascade_refresh` new-work success on an instrument with an existing queue row → queue cleared after rerank (cross-path clearing).
12. `cascade_refresh` new-work failure calls `enqueue_retry`.
13. `cascade_refresh` rerank failure does NOT enqueue; processed_ok rows are NOT cleared (durable rankings-needed signal — Codex v2 HIGH 2).
14. `cascade_refresh` with empty retry queue AND empty `instrument_ids` returns noop outcome — scheduler no-changed-ids drain test.
15. `cascade_refresh` with empty `instrument_ids` but non-empty queue processes retries.
16. `cascade_refresh` with thesis success AND rerank failure: queue rows for processed_ok stay, so next run re-processes.
17. `cascade_refresh` new-work thesis success (no prior queue row) AND rerank failure: `enqueue_rerank_marker` inserts a fresh RERANK_NEEDED row with `attempt_count=0`; next cycle re-drains, rerank succeeds, row cleared (Codex v3 HIGH regression cover).
18. `cascade_refresh` pre-existing at-cap row AND thesis success this cycle AND rerank failure: `enqueue_rerank_marker` upserts the row to RERANK_NEEDED with `attempt_count=0`, making it drainable again — confirms Codex v4 at-cap-conflict fix.
19. `cascade_refresh` rerank failure path calls `conn.rollback()` BEFORE any `enqueue_rerank_marker` call; verified by setting the mock conn into a simulated aborted state and confirming the markers still write.

Integration (real DB, `tests/test_cascade_retry_queue_integration.py`):

20. Run one cascade cycle that fails for one CIK → queue row present with `attempt_count=1`.
21. Second cycle with no new `changed_ids`: drain runs, retry processed, rerank success clears row.
22. Attempt cap: 5 consecutive failures → row has `attempt_count=5`, 6th drain skips it.
23. Thesis success + rerank failure (new-work or retry): queue carries markers/rows → next cycle recovers and clears.
24. Enqueue in fresh tx after simulated conn error (manually put conn into aborted state, call rollback-first then enqueue_retry, verify row exists after full commit).
25. Scheduler end-to-end: `daily_financial_facts` with zero `plan.refreshes` but non-empty queue still invokes cascade and drains.

## Migration application

Standard `sql/037_cascade_retry_queue.sql`. Applied by existing migration runner in `app/main.py` lifespan.

## Out of scope / deferrals

- **K.3 advisory lock**: `LOCKED_BY_SIBLING` write semantics already specified in the attempt-count table.
- **Admin UI**: rows at or above cap left for inspection; surfacing them is Chunk H / K.4 scope.
- **Metrics export**: queue depth / at-cap counts for Grafana — K.4.
- **Concurrent enqueue/clear race**: prevented by K.3 advisory lock, not K.2. Within K.2, the PK + `ON CONFLICT` UPSERT handles concurrent enqueues from multiple cascade invocations safely at the DB level; a concurrent clear-followed-by-enqueue could theoretically resurrect a cleared row, but that is exactly the desired behavior (failure after success should re-enqueue).

## Risks

- **Stuck rows**: if `last_error` is transient (network), the instrument retries every cycle until cap; if permanent (schema mismatch), cap prevents infinite retry. Five attempts is conservative — K.4 can tune based on observed failure modes.
- **Queue growth**: bounded by the universe size (PK on instrument_id). Worst case one row per analysable instrument, cleared on next successful cycle.
- **Wasted Claude calls on cross-path refresh**: bounded by queue size. Accepted trade-off per the stale-gate-bypass rationale.

## Shipping order

1. Migration 037.
2. Three DB helper functions with unit tests (1-7).
3. Wire into `cascade_refresh` with retry-first path (tests 8-15).
4. Scheduler edit: always invoke cascade on api-key-set days (test 21).
5. Integration tests 16-20.
6. Gates + Codex pre-push review + PR.
