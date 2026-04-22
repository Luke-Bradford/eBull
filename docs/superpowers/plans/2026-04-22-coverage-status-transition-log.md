# Coverage Status Transition Log Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the coverage.filings_status transition event log + `GET|POST /alerts/coverage-status-drops` endpoint trio so the dashboard alerts strip (#399) has a third alert feed.

**Architecture:** Append-only Postgres event table populated by a trigger on `coverage`. Writer serialization via `pg_advisory_xact_lock` inside trigger closes the cursor-skip race. Endpoint trio mirrors `/alerts/position-alerts` (#396) 1:1.

**Tech Stack:** PostgreSQL 15 (psycopg3), FastAPI + pydantic, pytest (real-DB integration via `tests/fixtures/ebull_test_db.py`).

**Spec:** `docs/superpowers/specs/2026-04-22-coverage-status-transition-log.md`

**Ticket:** #397. Branch `feature/397-coverage-status-transition-log` is already created; spec is already committed on it.

---

## File Structure

| Path | Responsibility | Action |
| --- | --- | --- |
| `.claude/CLAUDE.md` | Project instructions; Branch-and-PR-workflow step 3 indent polish | Modify (3-space indent on lines 77-87) |
| `sql/047_coverage_status_events.sql` | Event table + dual partial indexes + trigger fn + trigger + operator cursor col | Create |
| `app/api/alerts.py` | Alert endpoints; extend with 3 new routes + pydantic models + module docstring | Modify |
| `tests/fixtures/ebull_test_db.py` | Test DB truncation list; extend with `coverage_status_events` + `coverage` | Modify |
| `tests/test_coverage_status_transition_trigger.py` | Structural + trigger-behaviour + concurrent-writer tests (real DB) | Create |
| `tests/test_api_alerts.py` | Extend with `TestCoverageStatusDropsEndpoint` + integration tests for new routes | Modify |

---

## Task 1: CLAUDE.md indent polish (bundled from pending_polish memory)

**Files:**

- Modify: `.claude/CLAUDE.md` lines 77-87 (the prose block under step 3 of `## Branch and PR workflow`)

**Why first:** Trivial change; gets it out of the way. Bundling per memory `project_pending_polish.md`.

- [ ] **Step 1: Read the current state**

Run: `sed -n '72,95p' .claude/CLAUDE.md`

Verify lines 77-87 contain the unindented prose block + bullet list.

- [ ] **Step 2: Edit — indent by 3 spaces**

Exact change:

From:

```
3. Push and open a PR.
After every push, poll:
- `gh pr view {pr_number} --comments`
- `gh pr checks {pr_number}`

Do not push again until:
- the Claude review has posted
- CI results are visible
- all review comments have been read

Do not push a follow-up commit for CI alone without first reading the review comments on the latest commit.
If the review has not posted yet, wait and poll again rather than continuing blindly.
4. Wait for Claude review and CI on the latest commit.
```

To:

```
3. Push and open a PR.
   After every push, poll:
   - `gh pr view {pr_number} --comments`
   - `gh pr checks {pr_number}`

   Do not push again until:
   - the Claude review has posted
   - CI results are visible
   - all review comments have been read

   Do not push a follow-up commit for CI alone without first reading the review comments on the latest commit.
   If the review has not posted yet, wait and poll again rather than continuing blindly.
4. Wait for Claude review and CI on the latest commit.
```

- [ ] **Step 3: Verify render**

Run: `sed -n '72,95p' .claude/CLAUDE.md` — confirm 3-space indent on the prose + bullet block under step 3.

- [ ] **Step 4: Commit**

```bash
git add .claude/CLAUDE.md
git commit -m "docs(claude): indent step 3 prose in Branch and PR workflow

Steps 4-7 were collapsing into step 3 because the polling commands
and 'Do not push again until' prose block were unindented, breaking
the numbered list.

Bundled with #397."
```

---

## Task 2: Migration 047 — schema + cursor col (red test first)

**Files:**

- Create: `sql/047_coverage_status_events.sql`
- Create: `tests/test_coverage_status_transition_trigger.py` (structural-assertion tests — trigger tests added in Task 3-5)

**Why:** Schema first per CLAUDE.md working order. TDD at the structural level — assert migration shape before writing it.

- [ ] **Step 1: Write the failing structural-assertion tests**

Create `tests/test_coverage_status_transition_trigger.py`:

```python
"""Migration 047 + trigger behaviour tests against real ``ebull_test``.

Structural assertions (table columns, FK, dual partial indexes, trigger,
cursor column) + trigger behaviour (transitions, no-op UPDATEs, INSERT
not covered, advisory-lock serialization) + concurrent-writer test.
"""

from __future__ import annotations

from collections.abc import Iterator

import psycopg
import pytest

from tests.fixtures.ebull_test_db import ebull_test_conn as ebull_test_conn  # noqa: F401


def _fetch_one_scalar(conn: psycopg.Connection[tuple], sql: str, params: tuple = ()) -> object:
    with conn.cursor() as cur:
        cur.execute(sql, params)  # type: ignore[call-overload]
        row = cur.fetchone()
    conn.commit()
    assert row is not None, f"expected one row from: {sql}"
    return row[0]


class TestMigration047Structure:
    def test_coverage_status_events_table_exists(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        exists = _fetch_one_scalar(
            ebull_test_conn,
            "SELECT EXISTS(SELECT 1 FROM information_schema.tables "
            "WHERE table_name = 'coverage_status_events')",
        )
        assert exists is True

    def test_coverage_status_events_columns(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT column_name, data_type, is_nullable "
                "FROM information_schema.columns "
                "WHERE table_name = 'coverage_status_events' "
                "ORDER BY ordinal_position"
            )
            cols = cur.fetchall()
        ebull_test_conn.commit()
        by_name = {c[0]: (c[1], c[2]) for c in cols}
        assert by_name["event_id"] == ("bigint", "NO")
        assert by_name["instrument_id"] == ("bigint", "NO")
        assert by_name["changed_at"] == ("timestamp with time zone", "NO")
        assert by_name["old_status"] == ("text", "YES")
        assert by_name["new_status"] == ("text", "YES")

    def test_instrument_id_fk_present(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        # pg_constraint: check FK from coverage_status_events.instrument_id to instruments.
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*)
                FROM pg_constraint c
                JOIN pg_class t ON t.oid = c.conrelid
                JOIN pg_class r ON r.oid = c.confrelid
                WHERE c.contype = 'f'
                  AND t.relname = 'coverage_status_events'
                  AND r.relname = 'instruments'
                """
            )
            row = cur.fetchone()
        ebull_test_conn.commit()
        assert row is not None
        assert row[0] == 1

    def test_drops_partial_index_on_event_id(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        indexdef = _fetch_one_scalar(
            ebull_test_conn,
            "SELECT indexdef FROM pg_indexes "
            "WHERE indexname = 'idx_coverage_status_events_drops'",
        )
        assert indexdef is not None
        assert "event_id DESC" in str(indexdef)
        assert "old_status = 'analysable'" in str(indexdef)
        assert "new_status IS DISTINCT FROM 'analysable'" in str(indexdef)

    def test_drops_partial_index_on_changed_at(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        indexdef = _fetch_one_scalar(
            ebull_test_conn,
            "SELECT indexdef FROM pg_indexes "
            "WHERE indexname = 'idx_coverage_status_events_drops_changed_at'",
        )
        assert indexdef is not None
        assert "changed_at DESC" in str(indexdef)
        assert "old_status = 'analysable'" in str(indexdef)
        assert "new_status IS DISTINCT FROM 'analysable'" in str(indexdef)

    def test_operators_cursor_column_exists(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT data_type, is_nullable FROM information_schema.columns "
                "WHERE table_name = 'operators' "
                "  AND column_name = 'alerts_last_seen_coverage_event_id'"
            )
            row = cur.fetchone()
        ebull_test_conn.commit()
        assert row is not None, "alerts_last_seen_coverage_event_id column missing from operators"
        assert row[0] == "bigint"
        assert row[1] == "YES"

    def test_trigger_exists_after_update_of_filings_status(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                """
                SELECT t.tgname, pg_get_triggerdef(t.oid)
                FROM pg_trigger t
                JOIN pg_class c ON c.oid = t.tgrelid
                WHERE c.relname = 'coverage'
                  AND t.tgname = 'trg_coverage_filings_status_transition'
                """
            )
            row = cur.fetchone()
        ebull_test_conn.commit()
        assert row is not None, "trigger trg_coverage_filings_status_transition missing"
        triggerdef = str(row[1])
        assert "AFTER UPDATE OF filings_status" in triggerdef
        assert "FOR EACH ROW" in triggerdef

    def test_trigger_function_takes_advisory_lock(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        prosrc = _fetch_one_scalar(
            ebull_test_conn,
            "SELECT prosrc FROM pg_proc WHERE proname = 'log_coverage_status_transition'",
        )
        assert prosrc is not None
        src = str(prosrc)
        assert "pg_advisory_xact_lock" in src
        assert "coverage_status_events_writer" in src
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_coverage_status_transition_trigger.py::TestMigration047Structure -v`

Expected: all 8 tests FAIL — table / column / index / trigger / function do not exist yet.

- [ ] **Step 3: Write the migration**

Create `sql/047_coverage_status_events.sql`:

```sql
-- Migration 047: coverage.filings_status transition log + operator read cursor
--
-- 1. coverage_status_events — append-only row per filings_status transition.
--    event_id BIGSERIAL PK for strict-> cursor semantics. Advisory xact lock
--    in the trigger (below) serializes concurrent writers so commit order
--    matches event_id order — required because coverage.filings_status has
--    multiple writer paths (audit_all_instruments, audit_instrument,
--    _apply_backfill_outcome) that can overlap.
-- 2. Trigger logs ALL UPDATE transitions (including NULL->terminal first
--    audit). Endpoint filters to drops-from-analysable. Other slices
--    reserved for future audit UIs without schema change.
-- 3. INSERT path NOT covered — rows land via seed_coverage / bootstrap
--    with NULL or 'unknown'; first subsequent UPDATE fires the trigger.
--    Moot for drops scope (no INSERT writes 'analysable' directly).
-- 4. Dual partial indexes mirror sql/046_position_alerts_opened_at_index.sql
--    — one on event_id DESC for cursor walks, one on changed_at DESC for
--    the 7-day window filter. Same partial predicate on both.
--    No now()-based predicate (STABLE, not IMMUTABLE; Postgres rejects —
--    same rationale as sql/045_position_alerts.sql).
-- 5. operators.alerts_last_seen_coverage_event_id — parallel cursor to
--    existing alerts_last_seen_decision_id + alerts_last_seen_position_alert_id
--    columns. NULL = never acknowledged.

CREATE TABLE IF NOT EXISTS coverage_status_events (
    event_id      BIGSERIAL    PRIMARY KEY,
    instrument_id BIGINT       NOT NULL REFERENCES instruments(instrument_id),
    changed_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),
    old_status    TEXT         NULL,
    new_status    TEXT         NULL
);

CREATE INDEX IF NOT EXISTS idx_coverage_status_events_drops
    ON coverage_status_events (event_id DESC)
    WHERE old_status = 'analysable' AND new_status IS DISTINCT FROM 'analysable';

CREATE INDEX IF NOT EXISTS idx_coverage_status_events_drops_changed_at
    ON coverage_status_events (changed_at DESC)
    WHERE old_status = 'analysable' AND new_status IS DISTINCT FROM 'analysable';

CREATE OR REPLACE FUNCTION log_coverage_status_transition()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.filings_status IS DISTINCT FROM OLD.filings_status THEN
        -- Xact-scoped advisory lock serializes concurrent coverage writers so
        -- commit order matches event_id order (#396-style cursor safety).
        -- Idempotent within a single txn (bulk UPDATE takes it once per
        -- transitioning row — stacking is harmless; all refs release on commit).
        PERFORM pg_advisory_xact_lock(hashtext('coverage_status_events_writer'));
        INSERT INTO coverage_status_events (instrument_id, old_status, new_status)
        VALUES (NEW.instrument_id, OLD.filings_status, NEW.filings_status);
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_coverage_filings_status_transition ON coverage;

CREATE TRIGGER trg_coverage_filings_status_transition
    AFTER UPDATE OF filings_status ON coverage
    FOR EACH ROW
    EXECUTE FUNCTION log_coverage_status_transition();

ALTER TABLE operators
    ADD COLUMN IF NOT EXISTS alerts_last_seen_coverage_event_id BIGINT;
```

Note on `DROP TRIGGER IF EXISTS`: CREATE TRIGGER has no `IF NOT EXISTS` form in Postgres 15. Drop-first keeps the migration idempotent across re-runs in the same test DB.

- [ ] **Step 4: Run tests — all pass**

Run: `uv run pytest tests/test_coverage_status_transition_trigger.py::TestMigration047Structure -v`

Expected: all 8 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add sql/047_coverage_status_events.sql tests/test_coverage_status_transition_trigger.py
git commit -m "feat(#397): migration 047 — coverage_status_events + trigger

- coverage_status_events append-only table with BIGSERIAL PK.
- Dual partial indexes on (event_id DESC) and (changed_at DESC) for
  drops-from-analysable slice.
- Trigger log_coverage_status_transition with pg_advisory_xact_lock
  to serialize concurrent coverage writers (commit order matches
  event_id order, #396 cursor safety).
- operators.alerts_last_seen_coverage_event_id cursor column."
```

---

## Task 3: Trigger behaviour tests — transitions + no-ops

**Files:**

- Modify: `tests/test_coverage_status_transition_trigger.py` (extend with `TestTriggerBehaviour` class)

**Why:** Verify the trigger actually detects transitions correctly before building any code that consumes the event table.

Seed helper caveat: the test DB's migrations populate `instruments` + `coverage` via bootstrap only if the universe has been synced. The tests run against a fresh `ebull_test`, so `instruments` may be empty. Tests must seed an instrument + its coverage row explicitly.

- [ ] **Step 1: Add the test class — write failing trigger-behaviour tests**

Append to `tests/test_coverage_status_transition_trigger.py`:

```python
def _seed_instrument_with_coverage(
    conn: psycopg.Connection[tuple],
    *,
    initial_status: str | None = None,
) -> int:
    """Insert one tradable instrument + its coverage row; return instrument_id.

    initial_status=None leaves coverage.filings_status NULL (pre-audit). Otherwise
    the coverage row lands with that status (via UPDATE after INSERT — see below
    for why INSERT-direct does not fire the trigger).
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO instruments (symbol, name, instrument_type, is_tradable)
            VALUES ('TRG_' || nextval('instruments_instrument_id_seq')::text,
                    'Trigger test instrument', 'STOCK', TRUE)
            RETURNING instrument_id
            """
        )
        row = cur.fetchone()
        assert row is not None
        instrument_id = int(row[0])

        # INSERT the coverage row with filings_status NULL (mirrors seed_coverage
        # behaviour). Trigger does NOT fire on INSERT — this is deliberate per
        # spec scope.
        cur.execute(
            "INSERT INTO coverage (instrument_id, coverage_tier, filings_status) "
            "VALUES (%s, 3, NULL)",
            (instrument_id,),
        )
        if initial_status is not None:
            # Second step: UPDATE to initial_status. This fires the trigger, so
            # tests that want a clean slate must TRUNCATE coverage_status_events
            # after seeding. Callers responsible.
            cur.execute(
                "UPDATE coverage SET filings_status = %s WHERE instrument_id = %s",
                (initial_status, instrument_id),
            )
    conn.commit()
    return instrument_id


def _count_events(conn: psycopg.Connection[tuple], instrument_id: int | None = None) -> int:
    with conn.cursor() as cur:
        if instrument_id is None:
            cur.execute("SELECT COUNT(*) FROM coverage_status_events")
        else:
            cur.execute(
                "SELECT COUNT(*) FROM coverage_status_events WHERE instrument_id = %s",
                (instrument_id,),
            )
        row = cur.fetchone()
    conn.commit()
    assert row is not None
    return int(row[0])


class TestTriggerBehaviour:
    def test_null_to_analysable_logs_event(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        iid = _seed_instrument_with_coverage(ebull_test_conn)  # filings_status NULL
        assert _count_events(ebull_test_conn, iid) == 0

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "UPDATE coverage SET filings_status = 'analysable' WHERE instrument_id = %s",
                (iid,),
            )
        ebull_test_conn.commit()

        assert _count_events(ebull_test_conn, iid) == 1
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT old_status, new_status FROM coverage_status_events "
                "WHERE instrument_id = %s",
                (iid,),
            )
            row = cur.fetchone()
        ebull_test_conn.commit()
        assert row is not None
        assert row[0] is None
        assert row[1] == "analysable"

    def test_analysable_to_insufficient_logs_event(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        iid = _seed_instrument_with_coverage(ebull_test_conn, initial_status="analysable")
        # Seeding fired one event (NULL -> 'analysable'); start from that baseline.
        baseline = _count_events(ebull_test_conn, iid)

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "UPDATE coverage SET filings_status = 'insufficient' WHERE instrument_id = %s",
                (iid,),
            )
        ebull_test_conn.commit()

        assert _count_events(ebull_test_conn, iid) == baseline + 1

    def test_no_op_update_same_value_writes_nothing(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        iid = _seed_instrument_with_coverage(ebull_test_conn, initial_status="analysable")
        baseline = _count_events(ebull_test_conn, iid)

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "UPDATE coverage SET filings_status = 'analysable' WHERE instrument_id = %s",
                (iid,),
            )
        ebull_test_conn.commit()

        assert _count_events(ebull_test_conn, iid) == baseline

    def test_case_expression_preserving_same_value_writes_nothing(
        self, ebull_test_conn: psycopg.Connection[tuple]
    ) -> None:
        """Mirrors audit_all_instruments demote-guard pattern."""
        iid = _seed_instrument_with_coverage(ebull_test_conn, initial_status="structurally_young")
        baseline = _count_events(ebull_test_conn, iid)

        # CASE expression that resolves to the same value — simulates
        # audit_all_instruments preserving structurally_young when classifier
        # returns 'insufficient'.
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                """
                UPDATE coverage
                SET filings_status = CASE
                    WHEN filings_status = 'structurally_young' AND %s = 'insufficient'
                    THEN filings_status
                    ELSE %s
                END
                WHERE instrument_id = %s
                """,
                ("insufficient", "insufficient", iid),
            )
        ebull_test_conn.commit()

        assert _count_events(ebull_test_conn, iid) == baseline

    def test_update_of_unrelated_column_does_not_fire(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        iid = _seed_instrument_with_coverage(ebull_test_conn, initial_status="analysable")
        baseline = _count_events(ebull_test_conn, iid)

        with ebull_test_conn.cursor() as cur:
            cur.execute("UPDATE coverage SET filings_audit_at = now() WHERE instrument_id = %s", (iid,))
        ebull_test_conn.commit()

        assert _count_events(ebull_test_conn, iid) == baseline

    def test_insert_with_filings_status_does_not_fire(self, ebull_test_conn: psycopg.Connection[tuple]) -> None:
        """Documented scope limit: INSERT path not covered by trigger."""
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO instruments (symbol, name, instrument_type, is_tradable)
                VALUES ('TRG_INS_' || nextval('instruments_instrument_id_seq')::text,
                        'Insert test', 'STOCK', TRUE)
                RETURNING instrument_id
                """
            )
            row = cur.fetchone()
        assert row is not None
        iid = int(row[0])

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "INSERT INTO coverage (instrument_id, coverage_tier, filings_status) "
                "VALUES (%s, 3, 'unknown')",
                (iid,),
            )
        ebull_test_conn.commit()

        assert _count_events(ebull_test_conn, iid) == 0

    def test_bulk_update_mixed_transitioning_and_static_rows(
        self, ebull_test_conn: psycopg.Connection[tuple]
    ) -> None:
        iid_a = _seed_instrument_with_coverage(ebull_test_conn, initial_status="analysable")
        iid_b = _seed_instrument_with_coverage(ebull_test_conn, initial_status="insufficient")
        baseline = _count_events(ebull_test_conn)

        # A transitions, B stays the same via the CASE-demote-guard-style pattern.
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                """
                UPDATE coverage
                SET filings_status = CASE
                    WHEN instrument_id = %s THEN 'insufficient'
                    ELSE 'insufficient'
                END
                WHERE instrument_id IN (%s, %s)
                """,
                (iid_a, iid_a, iid_b),
            )
        ebull_test_conn.commit()

        # Exactly one new event (for iid_a). iid_b was already 'insufficient'.
        assert _count_events(ebull_test_conn) == baseline + 1
```

- [ ] **Step 2: Run the new tests — first run will FAIL on the `instruments` INSERT if `is_tradable` / `instrument_type` / `symbol` columns differ**

Run: `uv run pytest tests/test_coverage_status_transition_trigger.py::TestTriggerBehaviour -v`

If seed helper fails on missing columns / NOT NULL violations, adjust `_seed_instrument_with_coverage` against `instruments` schema. Check with:

Run: `uv run python -c "import psycopg; from tests.fixtures.ebull_test_db import test_database_url; conn=psycopg.connect(test_database_url()); cur=conn.cursor(); cur.execute(\"SELECT column_name, is_nullable, column_default FROM information_schema.columns WHERE table_name='instruments' ORDER BY ordinal_position\"); [print(r) for r in cur.fetchall()]"`

Adjust helper until seed succeeds.

- [ ] **Step 3: Run — all 7 tests PASS**

Run: `uv run pytest tests/test_coverage_status_transition_trigger.py::TestTriggerBehaviour -v`

- [ ] **Step 4: Commit**

```bash
git add tests/test_coverage_status_transition_trigger.py
git commit -m "test(#397): trigger behaviour — transitions, no-ops, INSERT not covered"
```

---

## Task 4: Concurrent-writer serialization test

**Files:**

- Modify: `tests/test_coverage_status_transition_trigger.py` (add `TestConcurrentWriters`)

**Why:** Prove the advisory lock makes commit order match event_id order. This is the most important correctness property — without the lock the cursor can silently skip rows.

- [ ] **Step 1: Add the test**

Append to `tests/test_coverage_status_transition_trigger.py`:

```python
class TestConcurrentWriters:
    def test_advisory_lock_serializes_commits_to_event_id_order(
        self, ebull_test_conn: psycopg.Connection[tuple]
    ) -> None:
        """Two connections update different coverage rows. Second blocks on the
        advisory lock until the first commits. event_id order matches commit
        order — no way for a later-committing lower event_id to be silently
        skipped by the dashboard cursor.
        """
        from tests.fixtures.ebull_test_db import test_database_url

        iid_a = _seed_instrument_with_coverage(ebull_test_conn, initial_status="analysable")
        iid_b = _seed_instrument_with_coverage(ebull_test_conn, initial_status="analysable")

        # Clear baseline.
        with ebull_test_conn.cursor() as cur:
            cur.execute("DELETE FROM coverage_status_events WHERE instrument_id IN (%s, %s)", (iid_a, iid_b))
        ebull_test_conn.commit()

        url = test_database_url()

        # Open conn A, begin txn, UPDATE iid_a — advisory lock acquired by trigger
        # but txn NOT yet committed.
        conn_a = psycopg.connect(url)
        conn_a.autocommit = False
        with conn_a.cursor() as cur_a:
            cur_a.execute(
                "UPDATE coverage SET filings_status = 'insufficient' WHERE instrument_id = %s",
                (iid_a,),
            )

        # Open conn B. Its UPDATE will block on the advisory lock held by A's
        # trigger. Use a short statement_timeout so the test fails loudly if the
        # lock isn't acquired as expected.
        conn_b = psycopg.connect(url)
        conn_b.autocommit = False
        with conn_b.cursor() as cur_b:
            cur_b.execute("SET LOCAL statement_timeout = '3s'")

        # Fire B's UPDATE in a thread so we can commit A while B is blocked.
        import threading

        b_error: list[BaseException] = []

        def _b_update() -> None:
            try:
                with conn_b.cursor() as cur_b:
                    cur_b.execute(
                        "UPDATE coverage SET filings_status = 'insufficient' WHERE instrument_id = %s",
                        (iid_b,),
                    )
                conn_b.commit()
            except BaseException as exc:  # noqa: BLE001
                b_error.append(exc)

        thread_b = threading.Thread(target=_b_update)
        thread_b.start()

        # Give B time to reach the lock wait.
        import time

        time.sleep(0.5)

        # Commit A — releases the advisory lock.
        conn_a.commit()
        conn_a.close()

        thread_b.join(timeout=5)
        if b_error:
            raise b_error[0]
        conn_b.close()

        # Assert event_id order matches commit order: A committed first, so A's
        # event_id < B's event_id.
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT instrument_id, event_id FROM coverage_status_events "
                "WHERE instrument_id IN (%s, %s) ORDER BY event_id",
                (iid_a, iid_b),
            )
            rows = cur.fetchall()
        ebull_test_conn.commit()
        assert len(rows) == 2
        assert rows[0][0] == iid_a, f"expected A ({iid_a}) first, got {rows}"
        assert rows[1][0] == iid_b, f"expected B ({iid_b}) second, got {rows}"
        assert rows[0][1] < rows[1][1]
```

- [ ] **Step 2: Run — PASS**

Run: `uv run pytest tests/test_coverage_status_transition_trigger.py::TestConcurrentWriters -v`

Expected: PASS. If it fails with a statement_timeout, the advisory lock logic is off — verify migration matches Task 2's trigger function body.

- [ ] **Step 3: Commit**

```bash
git add tests/test_coverage_status_transition_trigger.py
git commit -m "test(#397): advisory lock serializes concurrent coverage writers"
```

---

## Task 5: Test-fixture truncation list update

**Files:**

- Modify: `tests/fixtures/ebull_test_db.py` — extend `_PLANNER_TABLES` with `coverage_status_events` + `coverage`

**Why:** Without this, events leak across tests. `coverage` already has FKs from many tables (CASCADE handles it); need `coverage` in list so `coverage_status_events` can be truncated without partial-state.

- [ ] **Step 1: Read the list**

Run: `grep -n "_PLANNER_TABLES" tests/fixtures/ebull_test_db.py`

Find the tuple definition.

- [ ] **Step 2: Edit — add two entries**

Insert `coverage_status_events` and `coverage` into `_PLANNER_TABLES`. Order matters for readability; group with coverage-related entries:

```python
_PLANNER_TABLES: tuple[str, ...] = (
    "cascade_retry_queue",
    "financial_facts_raw",
    "data_ingestion_runs",
    "external_identifiers",
    "external_data_watermarks",
    "coverage_status_events",  # #397 transition log (child of coverage)
    "coverage",  # #397 truncate needed to reset coverage_status_events trigger state
    "position_alerts",  # #396 position-alert episodes
    "instruments",
    "job_runs",
    "financial_periods_raw",
    "financial_periods",
    "filing_events",
    "decision_audit",  # #315 Phase 3 alerts
    "trade_recommendations",  # #315 Phase 3 alerts (FK parent of decision_audit)
    "operators",  # #315 Phase 3 alerts (cursor column)
)
```

- [ ] **Step 3: Run the full trigger test file — still green**

Run: `uv run pytest tests/test_coverage_status_transition_trigger.py -v`

Expected: all tests still PASS. Tests should now also run cleanly on repeat (no leaked events).

Run twice to confirm:

Run: `uv run pytest tests/test_coverage_status_transition_trigger.py -v && uv run pytest tests/test_coverage_status_transition_trigger.py -v`

- [ ] **Step 4: Regression check — run full pytest to confirm no other test broke from truncation changes**

Run: `uv run pytest -x`

Expected: previously-green suite stays green.

- [ ] **Step 5: Commit**

```bash
git add tests/fixtures/ebull_test_db.py
git commit -m "test(#397): truncate coverage_status_events + coverage between tests"
```

---

## Task 6: Pydantic models + `GET /alerts/coverage-status-drops` (TDD)

**Files:**

- Modify: `app/api/alerts.py` — add pydantic models + GET route
- Modify: `tests/test_api_alerts.py` — add `TestCoverageStatusDropsGet` class

**Why:** API is the consumer of the event log. TDD endpoint-first so SQL shape is pinned by tests.

- [ ] **Step 1: Write failing tests — unit + integration**

Append to `tests/test_api_alerts.py` (end of file):

```python
# ---------------------------------------------------------------------
# #397: coverage-status-drops endpoints
# ---------------------------------------------------------------------


def _seed_coverage_status_event(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
    old_status: str = "analysable",
    new_status: str = "insufficient",
    changed_at_offset: str = "-1 hour",
) -> int:
    """Insert one coverage_status_events row with controlled offset; return event_id.

    ``changed_at_offset`` is a SQL interval literal (``'-1 hour'``, ``'-8 days'``).
    F-string composition as in _seed_position_alert — test-controlled constants
    only.
    """
    sql = f"""
            INSERT INTO coverage_status_events
                (instrument_id, old_status, new_status, changed_at)
            VALUES (
                %s, %s, %s,
                now() + INTERVAL '{changed_at_offset}'
            )
            RETURNING event_id
            """
    with conn.cursor() as cur:
        cur.execute(sql, (instrument_id, old_status, new_status))  # type: ignore[call-overload]
        row = cur.fetchone()
        assert row is not None
    conn.commit()
    return int(row[0])


class TestCoverageStatusDropsGet:
    def test_get_returns_503_when_no_operator(self, client: TestClient) -> None:
        with patch("app.api.alerts.sole_operator_id", side_effect=NoOperatorError()):
            _install_conn()
            resp = client.get("/alerts/coverage-status-drops")
        assert resp.status_code == 503

    def test_get_returns_501_when_multiple_operators(self, client: TestClient) -> None:
        with patch("app.api.alerts.sole_operator_id", side_effect=AmbiguousOperatorError()):
            _install_conn()
            resp = client.get("/alerts/coverage-status-drops")
        assert resp.status_code == 501

    def test_get_empty_state(self, client: TestClient) -> None:
        cur = _install_conn(
            fetchone_returns=[
                {"alerts_last_seen_coverage_event_id": None},
                {"unseen_count": 0},
            ],
            fetchall_returns=[],
        )
        with patch("app.api.alerts.sole_operator_id", return_value=UUID("00000000-0000-0000-0000-000000000001")):
            resp = client.get("/alerts/coverage-status-drops")
        assert resp.status_code == 200
        assert resp.json() == {
            "alerts_last_seen_coverage_event_id": None,
            "unseen_count": 0,
            "drops": [],
        }
        # SQL shape pin: predicate references BOTH old_status = 'analysable'
        # AND new_status IS DISTINCT FROM 'analysable' on the list query.
        list_sql = cur.execute.call_args_list[-1][0][0]
        assert "old_status = 'analysable'" in list_sql
        assert "new_status IS DISTINCT FROM 'analysable'" in list_sql
        assert "ORDER BY e.event_id DESC" in list_sql
        assert "LIMIT 500" in list_sql


@pytest.mark.skipif("not test_db_available()")
class TestCoverageStatusDropsGetIntegration:
    def test_get_returns_drops_in_window(
        self, ebull_test_conn: psycopg.Connection[tuple]
    ) -> None:
        _seed_operator(ebull_test_conn)
        iid = _seed_instrument(ebull_test_conn)
        _seed_coverage_status_event(ebull_test_conn, instrument_id=iid, changed_at_offset="-1 hour")

        client = _bind_test_client(ebull_test_conn)
        try:
            with patch("app.api.alerts.sole_operator_id", return_value=_INT_OP_ID):
                resp = client.get("/alerts/coverage-status-drops")
            assert resp.status_code == 200
            body = resp.json()
            assert body["alerts_last_seen_coverage_event_id"] is None
            assert body["unseen_count"] == 1
            assert len(body["drops"]) == 1
            assert body["drops"][0]["old_status"] == "analysable"
            assert body["drops"][0]["new_status"] == "insufficient"
        finally:
            from app.db import get_conn

            app.dependency_overrides.pop(get_conn, None)

    def test_get_excludes_non_drops(
        self, ebull_test_conn: psycopg.Connection[tuple]
    ) -> None:
        """Promotions (insufficient -> analysable) + first audit (NULL -> terminal)
        must not appear on strip."""
        _seed_operator(ebull_test_conn)
        iid = _seed_instrument(ebull_test_conn)
        # Promotion — excluded.
        _seed_coverage_status_event(
            ebull_test_conn, instrument_id=iid, old_status="insufficient", new_status="analysable"
        )
        # NULL -> 'analysable' first-audit — excluded (old_status IS NULL does
        # not match 'analysable' predicate).
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "INSERT INTO coverage_status_events (instrument_id, old_status, new_status) "
                "VALUES (%s, NULL, 'analysable')",
                (iid,),
            )
        ebull_test_conn.commit()

        client = _bind_test_client(ebull_test_conn)
        try:
            with patch("app.api.alerts.sole_operator_id", return_value=_INT_OP_ID):
                resp = client.get("/alerts/coverage-status-drops")
            assert resp.json()["drops"] == []
            assert resp.json()["unseen_count"] == 0
        finally:
            from app.db import get_conn

            app.dependency_overrides.pop(get_conn, None)

    def test_get_excludes_rows_older_than_7_days(
        self, ebull_test_conn: psycopg.Connection[tuple]
    ) -> None:
        _seed_operator(ebull_test_conn)
        iid = _seed_instrument(ebull_test_conn)
        _seed_coverage_status_event(ebull_test_conn, instrument_id=iid, changed_at_offset="-8 days")

        client = _bind_test_client(ebull_test_conn)
        try:
            with patch("app.api.alerts.sole_operator_id", return_value=_INT_OP_ID):
                resp = client.get("/alerts/coverage-status-drops")
            assert resp.json()["drops"] == []
            assert resp.json()["unseen_count"] == 0
        finally:
            from app.db import get_conn

            app.dependency_overrides.pop(get_conn, None)

    def test_get_respects_cursor_on_unseen_count(
        self, ebull_test_conn: psycopg.Connection[tuple]
    ) -> None:
        _seed_operator(ebull_test_conn)
        iid = _seed_instrument(ebull_test_conn)
        e1 = _seed_coverage_status_event(ebull_test_conn, instrument_id=iid, changed_at_offset="-2 hours")
        _seed_coverage_status_event(ebull_test_conn, instrument_id=iid, changed_at_offset="-1 hour")

        # Set operator cursor to e1 — one unseen (the later event).
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "UPDATE operators SET alerts_last_seen_coverage_event_id = %s "
                "WHERE operator_id = %s",
                (e1, _INT_OP_ID),
            )
        ebull_test_conn.commit()

        client = _bind_test_client(ebull_test_conn)
        try:
            with patch("app.api.alerts.sole_operator_id", return_value=_INT_OP_ID):
                resp = client.get("/alerts/coverage-status-drops")
            body = resp.json()
            assert body["unseen_count"] == 1
            assert len(body["drops"]) == 2  # list is uncapped by cursor
        finally:
            from app.db import get_conn

            app.dependency_overrides.pop(get_conn, None)
```

If existing helpers (`_seed_operator`, `_seed_instrument`, `_bind_test_client`, `_INT_OP_ID`, `test_db_available`) don't exist in `tests/test_api_alerts.py`, reuse or lift from neighbouring integration tests (`test_integration_position_alerts_*`). Lift, don't duplicate — put them at module scope if they aren't already.

- [ ] **Step 2: Run tests — FAIL (endpoint missing)**

Run: `uv run pytest tests/test_api_alerts.py::TestCoverageStatusDropsGet tests/test_api_alerts.py::TestCoverageStatusDropsGetIntegration -v`

Expected: all fail with 404 (route not registered) or integration-only failures.

- [ ] **Step 3: Update `app/api/alerts.py` module docstring**

Find the existing docstring header. Extend the numbered list to include:

```python
"""Alerts API — dashboard strip read + cursor endpoints.

Provides three independent alert feeds sharing the same dashboard strip shape:

1. Execution-guard rejections (#315 Phase 3 / PR #394):
   - GET  /alerts/guard-rejections
   - POST /alerts/seen               (body: {seen_through_decision_id})
   - POST /alerts/dismiss-all

2. Position alerts (SL/TP/thesis breach episodes, #396):
   - GET  /alerts/position-alerts
   - POST /alerts/position-alerts/seen          (body: {seen_through_position_alert_id})
   - POST /alerts/position-alerts/dismiss-all

3. Coverage status drops from 'analysable' (#397):
   - GET  /alerts/coverage-status-drops
   - POST /alerts/coverage-status-drops/seen    (body: {seen_through_event_id})
   - POST /alerts/coverage-status-drops/dismiss-all

Each feed maintains its own BIGSERIAL cursor column on ``operators`` and a
7-day window. Cursor semantics are identical across feeds: strict ``>``
comparison, GREATEST+COALESCE monotonicity, LEAST clamp on /seen, MAX
advance on /dismiss-all, and ``m.max_id IS NOT NULL`` empty-window guard.
See specs at ``docs/superpowers/specs/2026-04-21-alerts-strip-guard-rejections.md``
(guard), ``docs/superpowers/specs/2026-04-21-position-alert-persistence.md``
(position), and ``docs/superpowers/specs/2026-04-22-coverage-status-transition-log.md``
(coverage).

Known divergence between the guard /seen endpoint and the other two: guard
``/alerts/seen`` writes ``0`` as the cursor on an empty window + NULL cursor
(see #395 tech-debt). Position and coverage /seen endpoints do not — they
use the ``m.max_id IS NOT NULL`` guard as dismiss-all to preserve
``NULL = never acknowledged``.
"""
```

- [ ] **Step 4: Add pydantic models**

In `app/api/alerts.py`, below the existing `PositionAlertsMarkSeenRequest` model:

```python
class CoverageStatusDrop(BaseModel):
    event_id: int
    instrument_id: int
    symbol: str
    changed_at: datetime
    old_status: str           # always 'analysable' by endpoint filter
    new_status: str | None    # nullable defensive (CHECK permits NULL)


class CoverageStatusDropsResponse(BaseModel):
    alerts_last_seen_coverage_event_id: int | None
    unseen_count: int
    drops: list[CoverageStatusDrop]


class CoverageStatusDropsMarkSeenRequest(BaseModel):
    seen_through_event_id: int = Field(gt=0)
```

- [ ] **Step 5: Add the GET route**

Below the existing position-alerts GET handler:

```python
@router.get("/coverage-status-drops", response_model=CoverageStatusDropsResponse)
def get_coverage_status_drops(
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> CoverageStatusDropsResponse:
    operator_id = _resolve_operator(conn)
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        # 1. Read operator's cursor.
        cur.execute(
            "SELECT alerts_last_seen_coverage_event_id FROM operators WHERE operator_id = %(op)s",
            {"op": operator_id},
        )
        op_row = cur.fetchone()
        last_seen: int | None = op_row["alerts_last_seen_coverage_event_id"] if op_row else None

        # 2. Count unseen in-window drops (uncapped).
        cur.execute(
            """
            SELECT COUNT(*) AS unseen_count
            FROM coverage_status_events
            WHERE old_status = 'analysable'
              AND new_status IS DISTINCT FROM 'analysable'
              AND changed_at >= now() - INTERVAL '7 days'
              AND (%(last_id)s::BIGINT IS NULL OR event_id > %(last_id)s::BIGINT)
            """,
            {"last_id": last_seen},
        )
        count_row = cur.fetchone()
        assert count_row is not None, "COUNT(*) always returns a row"
        unseen_count: int = int(count_row["unseen_count"])

        # 3. Fetch list capped at 500. ORDER BY event_id DESC — BIGSERIAL PK is
        # race-safe (advisory xact lock in migration 047's trigger serializes
        # concurrent coverage writers, matching #396 rationale).
        cur.execute(
            """
            SELECT
                e.event_id,
                e.instrument_id,
                i.symbol,
                e.changed_at,
                e.old_status,
                e.new_status
            FROM coverage_status_events e
            JOIN instruments i ON i.instrument_id = e.instrument_id
            WHERE e.old_status = 'analysable'
              AND e.new_status IS DISTINCT FROM 'analysable'
              AND e.changed_at >= now() - INTERVAL '7 days'
            ORDER BY e.event_id DESC
            LIMIT 500
            """
        )
        rows = cur.fetchall()

    return CoverageStatusDropsResponse(
        alerts_last_seen_coverage_event_id=last_seen,
        unseen_count=unseen_count,
        drops=[CoverageStatusDrop.model_validate(r) for r in rows],
    )
```

- [ ] **Step 6: Run — tests PASS**

Run: `uv run pytest tests/test_api_alerts.py::TestCoverageStatusDropsGet tests/test_api_alerts.py::TestCoverageStatusDropsGetIntegration -v`

Expected: all PASS.

- [ ] **Step 7: Commit**

```bash
git add app/api/alerts.py tests/test_api_alerts.py
git commit -m "feat(#397): GET /alerts/coverage-status-drops + pydantic models"
```

---

## Task 7: `POST /alerts/coverage-status-drops/seen` (TDD)

**Files:**

- Modify: `app/api/alerts.py`
- Modify: `tests/test_api_alerts.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_api_alerts.py`:

```python
@pytest.mark.skipif("not test_db_available()")
class TestCoverageStatusDropsSeen:
    def test_seen_advances_cursor_monotonically(
        self, ebull_test_conn: psycopg.Connection[tuple]
    ) -> None:
        _seed_operator(ebull_test_conn)
        iid = _seed_instrument(ebull_test_conn)
        e1 = _seed_coverage_status_event(ebull_test_conn, instrument_id=iid, changed_at_offset="-2 hours")
        _seed_coverage_status_event(ebull_test_conn, instrument_id=iid, changed_at_offset="-1 hour")

        client = _bind_test_client(ebull_test_conn)
        try:
            with patch("app.api.alerts.sole_operator_id", return_value=_INT_OP_ID):
                resp = client.post(
                    "/alerts/coverage-status-drops/seen",
                    json={"seen_through_event_id": e1},
                )
            assert resp.status_code == 204
            # Cursor advanced to e1.
            with ebull_test_conn.cursor() as cur:
                cur.execute(
                    "SELECT alerts_last_seen_coverage_event_id FROM operators "
                    "WHERE operator_id = %s",
                    (_INT_OP_ID,),
                )
                row = cur.fetchone()
            ebull_test_conn.commit()
            assert row is not None
            assert row[0] == e1

            # Second call with smaller value — cursor does NOT regress.
            with patch("app.api.alerts.sole_operator_id", return_value=_INT_OP_ID):
                resp = client.post(
                    "/alerts/coverage-status-drops/seen",
                    json={"seen_through_event_id": 1},
                )
            assert resp.status_code == 204
            with ebull_test_conn.cursor() as cur:
                cur.execute(
                    "SELECT alerts_last_seen_coverage_event_id FROM operators "
                    "WHERE operator_id = %s",
                    (_INT_OP_ID,),
                )
                row = cur.fetchone()
            ebull_test_conn.commit()
            assert row is not None
            assert row[0] == e1
        finally:
            from app.db import get_conn

            app.dependency_overrides.pop(get_conn, None)

    def test_seen_empty_window_is_noop_and_preserves_null(
        self, ebull_test_conn: psycopg.Connection[tuple]
    ) -> None:
        """No in-window drops + NULL cursor → /seen does NOT materialize a cursor.
        Mirrors position-alerts /seen behaviour (no #395 divergence)."""
        _seed_operator(ebull_test_conn)

        client = _bind_test_client(ebull_test_conn)
        try:
            with patch("app.api.alerts.sole_operator_id", return_value=_INT_OP_ID):
                resp = client.post(
                    "/alerts/coverage-status-drops/seen",
                    json={"seen_through_event_id": 99999},
                )
            assert resp.status_code == 204
            with ebull_test_conn.cursor() as cur:
                cur.execute(
                    "SELECT alerts_last_seen_coverage_event_id FROM operators "
                    "WHERE operator_id = %s",
                    (_INT_OP_ID,),
                )
                row = cur.fetchone()
            ebull_test_conn.commit()
            assert row is not None
            assert row[0] is None
        finally:
            from app.db import get_conn

            app.dependency_overrides.pop(get_conn, None)

    def test_seen_clamps_to_in_window_max(
        self, ebull_test_conn: psycopg.Connection[tuple]
    ) -> None:
        _seed_operator(ebull_test_conn)
        iid = _seed_instrument(ebull_test_conn)
        e1 = _seed_coverage_status_event(ebull_test_conn, instrument_id=iid, changed_at_offset="-1 hour")
        # Request far beyond the in-window max — cursor clamps to e1.
        client = _bind_test_client(ebull_test_conn)
        try:
            with patch("app.api.alerts.sole_operator_id", return_value=_INT_OP_ID):
                resp = client.post(
                    "/alerts/coverage-status-drops/seen",
                    json={"seen_through_event_id": e1 + 999_999},
                )
            assert resp.status_code == 204
            with ebull_test_conn.cursor() as cur:
                cur.execute(
                    "SELECT alerts_last_seen_coverage_event_id FROM operators "
                    "WHERE operator_id = %s",
                    (_INT_OP_ID,),
                )
                row = cur.fetchone()
            ebull_test_conn.commit()
            assert row is not None
            assert row[0] == e1
        finally:
            from app.db import get_conn

            app.dependency_overrides.pop(get_conn, None)

    def test_seen_requires_positive_integer(self, client: TestClient) -> None:
        _install_conn()
        with patch("app.api.alerts.sole_operator_id", return_value=UUID("00000000-0000-0000-0000-000000000001")):
            resp = client.post(
                "/alerts/coverage-status-drops/seen",
                json={"seen_through_event_id": 0},
            )
        assert resp.status_code == 422
```

- [ ] **Step 2: Run — FAIL (route missing)**

Run: `uv run pytest tests/test_api_alerts.py::TestCoverageStatusDropsSeen -v`

Expected: 404 (route not registered) + 422 test may pass for the wrong reason.

- [ ] **Step 3: Implement the route**

Append to `app/api/alerts.py`:

```python
@router.post("/coverage-status-drops/seen", status_code=status.HTTP_204_NO_CONTENT)
def mark_coverage_status_drops_seen(
    body: CoverageStatusDropsMarkSeenRequest,
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> None:
    operator_id = _resolve_operator(conn)
    with conn.cursor() as cur:
        # m.max_id IS NOT NULL guard preserves NULL cursor on empty window.
        # Matches /alerts/position-alerts/seen (post-#395 correct shape) rather
        # than guard /alerts/seen (pre-#395 divergent shape).
        cur.execute(
            """
            UPDATE operators AS op
            SET alerts_last_seen_coverage_event_id = GREATEST(
                COALESCE(op.alerts_last_seen_coverage_event_id, 0),
                LEAST(%(seen_through_event_id)s, m.max_id)
            )
            FROM (
                SELECT MAX(event_id) AS max_id
                FROM coverage_status_events
                WHERE old_status = 'analysable'
                  AND new_status IS DISTINCT FROM 'analysable'
                  AND changed_at >= now() - INTERVAL '7 days'
            ) AS m
            WHERE op.operator_id = %(op)s
              AND m.max_id IS NOT NULL
            """,
            {
                "seen_through_event_id": body.seen_through_event_id,
                "op": operator_id,
            },
        )
    conn.commit()
```

- [ ] **Step 4: Run — PASS**

Run: `uv run pytest tests/test_api_alerts.py::TestCoverageStatusDropsSeen -v`

Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add app/api/alerts.py tests/test_api_alerts.py
git commit -m "feat(#397): POST /alerts/coverage-status-drops/seen"
```

---

## Task 8: `POST /alerts/coverage-status-drops/dismiss-all` (TDD)

**Files:**

- Modify: `app/api/alerts.py`
- Modify: `tests/test_api_alerts.py`

- [ ] **Step 1: Write failing tests**

Append to `tests/test_api_alerts.py`:

```python
@pytest.mark.skipif("not test_db_available()")
class TestCoverageStatusDropsDismissAll:
    def test_dismiss_all_advances_to_max(
        self, ebull_test_conn: psycopg.Connection[tuple]
    ) -> None:
        _seed_operator(ebull_test_conn)
        iid = _seed_instrument(ebull_test_conn)
        _seed_coverage_status_event(ebull_test_conn, instrument_id=iid, changed_at_offset="-2 hours")
        e2 = _seed_coverage_status_event(ebull_test_conn, instrument_id=iid, changed_at_offset="-1 hour")

        client = _bind_test_client(ebull_test_conn)
        try:
            with patch("app.api.alerts.sole_operator_id", return_value=_INT_OP_ID):
                resp = client.post("/alerts/coverage-status-drops/dismiss-all")
            assert resp.status_code == 204
            with ebull_test_conn.cursor() as cur:
                cur.execute(
                    "SELECT alerts_last_seen_coverage_event_id FROM operators "
                    "WHERE operator_id = %s",
                    (_INT_OP_ID,),
                )
                row = cur.fetchone()
            ebull_test_conn.commit()
            assert row is not None
            assert row[0] == e2
        finally:
            from app.db import get_conn

            app.dependency_overrides.pop(get_conn, None)

    def test_dismiss_all_empty_window_preserves_null_cursor(
        self, ebull_test_conn: psycopg.Connection[tuple]
    ) -> None:
        _seed_operator(ebull_test_conn)
        client = _bind_test_client(ebull_test_conn)
        try:
            with patch("app.api.alerts.sole_operator_id", return_value=_INT_OP_ID):
                resp = client.post("/alerts/coverage-status-drops/dismiss-all")
            assert resp.status_code == 204
            with ebull_test_conn.cursor() as cur:
                cur.execute(
                    "SELECT alerts_last_seen_coverage_event_id FROM operators "
                    "WHERE operator_id = %s",
                    (_INT_OP_ID,),
                )
                row = cur.fetchone()
            ebull_test_conn.commit()
            assert row is not None
            assert row[0] is None
        finally:
            from app.db import get_conn

            app.dependency_overrides.pop(get_conn, None)

    def test_dismiss_all_does_not_regress_cursor(
        self, ebull_test_conn: psycopg.Connection[tuple]
    ) -> None:
        _seed_operator(ebull_test_conn)
        iid = _seed_instrument(ebull_test_conn)
        e1 = _seed_coverage_status_event(ebull_test_conn, instrument_id=iid, changed_at_offset="-1 hour")
        # Pre-advance cursor past e1.
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "UPDATE operators SET alerts_last_seen_coverage_event_id = %s "
                "WHERE operator_id = %s",
                (e1 + 999, _INT_OP_ID),
            )
        ebull_test_conn.commit()

        client = _bind_test_client(ebull_test_conn)
        try:
            with patch("app.api.alerts.sole_operator_id", return_value=_INT_OP_ID):
                resp = client.post("/alerts/coverage-status-drops/dismiss-all")
            assert resp.status_code == 204
            with ebull_test_conn.cursor() as cur:
                cur.execute(
                    "SELECT alerts_last_seen_coverage_event_id FROM operators "
                    "WHERE operator_id = %s",
                    (_INT_OP_ID,),
                )
                row = cur.fetchone()
            ebull_test_conn.commit()
            assert row is not None
            assert row[0] == e1 + 999  # GREATEST preserves the larger existing cursor
        finally:
            from app.db import get_conn

            app.dependency_overrides.pop(get_conn, None)
```

- [ ] **Step 2: Run — FAIL**

Run: `uv run pytest tests/test_api_alerts.py::TestCoverageStatusDropsDismissAll -v`

- [ ] **Step 3: Implement the route**

Append to `app/api/alerts.py`:

```python
@router.post("/coverage-status-drops/dismiss-all", status_code=status.HTTP_204_NO_CONTENT)
def dismiss_all_coverage_status_drops(
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> None:
    operator_id = _resolve_operator(conn)
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE operators AS op
            SET alerts_last_seen_coverage_event_id = GREATEST(
                COALESCE(op.alerts_last_seen_coverage_event_id, 0),
                m.max_id
            )
            FROM (
                SELECT MAX(event_id) AS max_id
                FROM coverage_status_events
                WHERE old_status = 'analysable'
                  AND new_status IS DISTINCT FROM 'analysable'
                  AND changed_at >= now() - INTERVAL '7 days'
            ) AS m
            WHERE op.operator_id = %(op)s
              AND m.max_id IS NOT NULL
            """,
            {"op": operator_id},
        )
    conn.commit()
```

- [ ] **Step 4: Run — PASS**

Run: `uv run pytest tests/test_api_alerts.py::TestCoverageStatusDropsDismissAll -v`

- [ ] **Step 5: Commit**

```bash
git add app/api/alerts.py tests/test_api_alerts.py
git commit -m "feat(#397): POST /alerts/coverage-status-drops/dismiss-all"
```

---

## Task 9: Pre-push gates + Codex checkpoint 2 + push + PR

**Files:** (none; gate + review only)

- [ ] **Step 1: Run all four pre-push gates**

```bash
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv run pytest
```

All four must pass. If ruff format fails, run `uv run ruff format .` and re-add the changed files to a new commit.

- [ ] **Step 2: Smoke gate**

Run: `uv run pytest tests/smoke/test_app_boots.py -v`

Expected: PASS. The smoke test drives app lifespan against the real dev DB; migration 047 applies as part of startup.

- [ ] **Step 3: Manual /health probe against the live stack**

Per memory `feedback_validate_running_server.md`: after editing lifespan-adjacent code (anything hit by migration apply), confirm the running server actually boots. Migration 047 runs in lifespan. If the dev stack is running via VS Code tasks:

Run: `curl -fsS http://127.0.0.1:8000/health` — expect 200 + JSON body.

If stack is not running, skip — do not start or restart it (memory `feedback_keep_stack_running.md`).

- [ ] **Step 4: Codex checkpoint 2 — diff review before first push**

```bash
git diff main...HEAD | head -c 50000 > /tmp/pr397_diff.txt
codex.cmd exec "Review this diff for PR #397 (coverage status transition log). Diff at /tmp/pr397_diff.txt. Focus: correctness of trigger SQL, advisory-lock serialization behaviour, endpoint SQL predicates match partial-index predicates, test coverage of the scope limits called out in docs/superpowers/specs/2026-04-22-coverage-status-transition-log.md, any new issues I missed. Reply terse."
```

Fix any real findings before pushing; rebut unsound findings in the PR description.

- [ ] **Step 5: Push + open PR**

```bash
git push -u origin feature/397-coverage-status-transition-log
gh pr create --title "feat(#397): coverage status transition log" --body "$(cat <<'EOF'
## What

- New migration `sql/047_coverage_status_events.sql` — append-only event table, dual partial indexes on `(event_id DESC)` + `(changed_at DESC)` for drops-from-analysable, row-level `AFTER UPDATE OF filings_status` trigger with `pg_advisory_xact_lock` writer serialization, `operators.alerts_last_seen_coverage_event_id` cursor column.
- Three routes in `app/api/alerts.py` — `GET /alerts/coverage-status-drops`, `POST /alerts/coverage-status-drops/seen`, `POST /alerts/coverage-status-drops/dismiss-all`. Mirror position-alerts pattern (#396).
- CLAUDE.md Branch-and-PR-workflow step 3 indent polish bundled.

## Why

Prereq for #399 (AlertsStrip UI wire-up). Completes the third alert feed; guard-rejection + position-alert shipped in #394 + #401.

## Test plan

- Migration structural assertions (columns, FK, dual partial indexes, trigger, advisory lock in function body).
- Trigger behaviour — transitions logged, no-op UPDATEs / CASE-preserve / unrelated-column / INSERT do not fire.
- Concurrent-writer serialization — commit order matches event_id order.
- Endpoint integration tests (GET filters drops, cursor math, empty-window preserves NULL, dismiss-all advance, auth).

## Called out

- `instrument_id` FK has no `ON DELETE CASCADE` — instrument delete blocks if history exists. Deliberate; instrument deletes are rare and manual.
- Trigger takes `pg_advisory_xact_lock(hashtext('coverage_status_events_writer'))` to serialize concurrent coverage writers. Matches #396's single-writer prerequisite. Cheap (one lock per txn; stacking harmless).
- Event log covers UPDATE transitions only — INSERT-created rows (`seed_coverage`, `bootstrap_missing_coverage_rows`) don't fire the trigger. Orthogonal to drops-from-analysable scope (no INSERT path writes `'analysable'`). Documented in spec for future full-history UI needs.
- Inherits #395 multi-query GET snapshot drift.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

- [ ] **Step 6: Start polling review + CI immediately**

Per memory `feedback_post_push_cycle.md` + CLAUDE.md — the loop is not optional. Start polling without asking:

```bash
gh pr checks --watch
gh pr view --comments
```

Resolve every comment as FIXED / DEFERRED / REBUTTED per the review comment resolution contract. Re-run Task 9 steps 1-3 before every follow-up push. Merge only after APPROVE on the most recent commit + CI green.

---

## Self-review

**1. Spec coverage:**

- Schema table + cursor col → Task 2
- Advisory lock serialization → Task 2 (implemented) + Task 4 (tested)
- Dual partial indexes → Task 2 (implemented) + Task 2 (structural tests) + Task 6 (runtime query matches partial predicate)
- Trigger behaviour + no-ops + INSERT scope limit → Task 3
- GET endpoint + cursor semantics + 7-day window → Task 6
- /seen endpoint + empty-window NULL preservation → Task 7
- /dismiss-all endpoint + monotonicity → Task 8
- CLAUDE.md polish → Task 1
- Test DB isolation → Task 5
- Pre-push gates + Codex ckpt 2 + PR body → Task 9

All spec sections covered.

**2. Placeholder scan:** No "TBD", "TODO", "implement later". Every step shows either exact code or exact command.

**3. Type consistency:**

- `CoverageStatusDrop.new_status: str | None` consistent with partial-index predicate `new_status IS DISTINCT FROM 'analysable'` (permits NULL).
- `seen_through_event_id` used consistently in model + endpoint + tests.
- `alerts_last_seen_coverage_event_id` column name matches across migration, model field, endpoint SQL, tests.
- Pydantic response field `drops` (not `alerts`) consistent across model + endpoint return + test assertions.

**4. Known risks:**

- Task 3 seed helper may fail against actual `instruments` columns — plan explicitly calls this out + gives the introspection command to fix.
- Task 5 truncation change could break other tests — explicit regression run included.
