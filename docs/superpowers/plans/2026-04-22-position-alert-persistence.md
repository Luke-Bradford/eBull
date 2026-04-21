# Position-alert event persistence — Implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Persist `position_monitor.check_position_health` breach events to a new `position_alerts` episode table, exposing the data via `GET /alerts/position-alerts`, `POST /alerts/position-alerts/seen`, `POST /alerts/position-alerts/dismiss-all` so #399 can render thesis/SL/TP breaches in the dashboard alerts strip.

**Architecture:** Episode model — one row per breach onset, `resolved_at` flipped when the breach clears. Partial unique index on `(instrument_id, alert_type) WHERE resolved_at IS NULL` enforces at-most-one-open-episode per pair. Writer runs inside `monitor_positions_job` on the hourly tick, single-threaded per `app/jobs/runtime.py:224,243` (`max_instances=1` + `threading.Lock`). Read API mirrors #394's guard-rejection shape: 7-day window, 500-row LIMIT, BIGSERIAL `alert_id` cursor with strict `>` comparison.

**Tech Stack:** Python 3.12, FastAPI, Pydantic v2, psycopg 3, PostgreSQL, pytest.

**Branch:** `feature/396-position-alert-persistence` (per CLAUDE.md Branch and PR workflow).

**Spec:** [`docs/superpowers/specs/2026-04-21-position-alert-persistence.md`](../specs/2026-04-21-position-alert-persistence.md).

---

## File Structure

### New files

- `sql/045_position_alerts.sql` — migration creating `position_alerts` table, partial unique index, query index, and `operators.alerts_last_seen_position_alert_id` column.
- (No new Python files — writer lives in existing `app/services/position_monitor.py`; API lives in existing `app/api/alerts.py`.)

### Modified files

- `app/services/position_monitor.py` — add `PersistStats` dataclass + `persist_position_alerts(conn, result)` function.
- `app/workers/scheduler.py:2128-2161` — call `persist_position_alerts` after `check_position_health`; adjust log line.
- `app/api/alerts.py` — add `PositionAlert`, `PositionAlertsResponse`, `PositionAlertsMarkSeenRequest` Pydantic models; add three new endpoints.
- `frontend/src/api/types.ts` — (no change in this PR; typed models live on backend; frontend extension is #399 scope).
- `tests/test_position_monitor.py` — add `TestPersistPositionAlerts` class with 10 unit tests against `ebull_test_conn`.
- `tests/test_api_alerts.py` — add 22 new test methods covering the three new endpoints.
- `tests/fixtures/ebull_test_db.py:46-60` — add `position_alerts` to `_PLANNER_TABLES` so the per-test TRUNCATE sweeps it.

---

## Task 1 — Migration + test fixture

**Files:**
- Create: `sql/045_position_alerts.sql`
- Modify: `tests/fixtures/ebull_test_db.py:46-60`

- [ ] **Step 1: Write the migration**

Create `sql/045_position_alerts.sql` with exact content:

```sql
-- Migration 045: position-alert episode persistence + operator read cursor
--
-- 1. position_alerts — one row per breach EPISODE (not per hourly evaluation).
--    opened_at = onset detection time. resolved_at = clearance detection time
--    (NULL while still breaching). alert_id is BIGSERIAL for strict-> cursor
--    semantics mirroring operators.alerts_last_seen_decision_id (#394 rationale).
-- 2. Partial unique index enforces at-most-one-open-episode per (instrument_id,
--    alert_type). The writer's INSERT path relies on this as the concurrency
--    backstop. Single-threaded scheduler (app/jobs/runtime.py max_instances=1
--    + per-job threading.Lock) makes overlap effectively impossible; this
--    constraint is the defensive second layer.
-- 3. idx_position_alerts_recent on alert_id DESC supports the strip scan in
--    GET /alerts/position-alerts (ORDER BY alert_id DESC + LIMIT 500).
--    No WHERE predicate: partial-index predicates must be IMMUTABLE in
--    PostgreSQL, and now()-based predicates use a STABLE function which
--    would be rejected.
-- 4. operators.alerts_last_seen_position_alert_id — parallel cursor to the
--    existing alerts_last_seen_decision_id column. NULL = never acknowledged.

CREATE TABLE IF NOT EXISTS position_alerts (
    alert_id      BIGSERIAL    PRIMARY KEY,
    instrument_id BIGINT       NOT NULL REFERENCES instruments(instrument_id),
    alert_type    TEXT         NOT NULL
                               CHECK (alert_type IN ('sl_breach', 'tp_breach', 'thesis_break')),
    opened_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),
    resolved_at   TIMESTAMPTZ  NULL,
    detail        TEXT         NOT NULL,
    current_bid   NUMERIC      NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_position_alerts_open
    ON position_alerts (instrument_id, alert_type)
    WHERE resolved_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_position_alerts_recent
    ON position_alerts (alert_id DESC);

ALTER TABLE operators
    ADD COLUMN IF NOT EXISTS alerts_last_seen_position_alert_id BIGINT;
```

- [ ] **Step 2: Add `position_alerts` to test TRUNCATE list**

In `tests/fixtures/ebull_test_db.py`, modify `_PLANNER_TABLES`:

```python
_PLANNER_TABLES: tuple[str, ...] = (
    "cascade_retry_queue",
    "financial_facts_raw",
    "data_ingestion_runs",
    "external_identifiers",
    "external_data_watermarks",
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

`position_alerts` must come BEFORE `instruments` because `position_alerts.instrument_id` references `instruments(instrument_id)` and TRUNCATE CASCADE order ensures the referencing table is cleared first (belt-and-braces — `CASCADE` handles it anyway).

- [ ] **Step 3: Apply migration to dev and test DBs + verify**

```bash
uv run python -c "from app.db.migrations import run_migrations; run_migrations()"
```

Expected output: one line logging `045_position_alerts.sql applied` (exact format may differ — look for the filename).

Verify the table exists in dev DB:

```bash
uv run python -c "import psycopg; from app.config import settings; \
  c = psycopg.connect(settings.database_url); \
  cur = c.cursor(); \
  cur.execute(\"SELECT column_name, data_type FROM information_schema.columns WHERE table_name='position_alerts' ORDER BY ordinal_position\"); \
  print(list(cur))"
```

Expected: 7 rows — `alert_id bigint`, `instrument_id bigint`, `alert_type text`, `opened_at timestamp with time zone`, `resolved_at timestamp with time zone`, `detail text`, `current_bid numeric`.

Verify `operators.alerts_last_seen_position_alert_id`:

```bash
uv run python -c "import psycopg; from app.config import settings; \
  c = psycopg.connect(settings.database_url); \
  cur = c.cursor(); \
  cur.execute(\"SELECT column_name FROM information_schema.columns WHERE table_name='operators' AND column_name='alerts_last_seen_position_alert_id'\"); \
  print(cur.fetchall())"
```

Expected: `[('alerts_last_seen_position_alert_id',)]`.

- [ ] **Step 4: Commit**

```bash
git add sql/045_position_alerts.sql tests/fixtures/ebull_test_db.py
git commit -m "feat(#396): position_alerts episode table + operator cursor column"
```

---

## Task 2 — Writer: `PersistStats` + `persist_position_alerts`

**Files:**
- Modify: `app/services/position_monitor.py` (add new dataclass + function)
- Test: `tests/test_position_monitor.py` (add `TestPersistPositionAlerts` class)

All writer tests use the real `ebull_test_conn` fixture (episode diff logic exercises atomic transactions, partial-unique-index conflicts, and real DB round-trips — mocked cursors can't pin those).

- [ ] **Step 1: Write failing test for empty + empty no-op**

Append to `tests/test_position_monitor.py`:

```python
import psycopg
import pytest

from app.services.position_monitor import (
    MonitorAlert,
    MonitorResult,
    PersistStats,
    persist_position_alerts,
)
from tests.fixtures.ebull_test_db import ebull_test_conn, test_db_available  # noqa: F401


_next_instrument_id = 0


def _seed_instrument(conn: psycopg.Connection[tuple], *, symbol: str = "AAPL") -> int:
    """Insert a tradable instrument, return instrument_id.

    ``instruments.instrument_id`` is a BIGINT PRIMARY KEY with NO default
    (sql/001_init.sql:2), so the caller supplies the id. ``symbol`` and
    ``company_name`` are NOT NULL (sql/001_init.sql:3-4) — no fixture-neutral
    defaults. Prevention: ``INSERT INTO instruments fixtures must supply
    is_tradable``; we supply it explicitly even though it has a default.
    """
    global _next_instrument_id
    _next_instrument_id += 1
    iid = _next_instrument_id
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO instruments (instrument_id, symbol, company_name, currency, is_tradable) "
            "VALUES (%s, %s, %s, 'USD', TRUE)",
            (iid, symbol, f"{symbol} Inc."),
        )
    conn.commit()
    return iid


@pytest.mark.skipif("not test_db_available()")
class TestPersistPositionAlerts:
    """Writer diff-logic tests against real ebull_test DB."""

    def test_empty_and_empty_is_noop(
        self, ebull_test_conn: psycopg.Connection[tuple]
    ) -> None:
        result = MonitorResult(positions_checked=0, alerts=())
        stats = persist_position_alerts(ebull_test_conn, result)
        assert stats == PersistStats(opened=0, resolved=0, unchanged=0)

        with ebull_test_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM position_alerts")
            assert cur.fetchone() == (0,)
```

Run:

```bash
uv run pytest tests/test_position_monitor.py::TestPersistPositionAlerts::test_empty_and_empty_is_noop -v
```

Expected: FAIL with `ImportError: cannot import name 'PersistStats'` or `cannot import name 'persist_position_alerts'`.

- [ ] **Step 2: Add `PersistStats` dataclass to `position_monitor.py`**

Add after the existing `MonitorResult` dataclass in `app/services/position_monitor.py`:

```python
@dataclass(frozen=True)
class PersistStats:
    """Aggregate stats from one persist_position_alerts invocation."""

    opened: int
    resolved: int
    unchanged: int
```

- [ ] **Step 3: Add the writer function**

Append to `app/services/position_monitor.py`:

```python
def persist_position_alerts(
    conn: psycopg.Connection[Any],
    result: MonitorResult,
) -> PersistStats:
    """Reconcile open breach episodes against the current MonitorResult.

    Contract: for each (instrument_id, alert_type) pair:
      - current breach AND no open episode    -> INSERT new row
      - current breach AND open episode       -> no-op (still breaching)
      - no current breach AND open episode    -> UPDATE resolved_at = now()
      - no current breach AND no open episode -> no-op

    Runs inside a single ``conn.transaction()`` block — caller MUST NOT
    hold an outer transaction, because psycopg v3 treats nested
    ``conn.transaction()`` as a savepoint and the outer commit path is
    the caller's responsibility. ``monitor_positions_job`` opens a
    fresh connection, invokes ``check_position_health`` (read-only, no
    BEGIN), then calls this writer — the ``conn.transaction()`` block
    here IS the outer transaction and commits on clean exit.

    Concurrency: the INSERT path tolerates partial-unique-index
    conflicts via ``ON CONFLICT DO NOTHING``. The resolve path runs
    ``WHERE resolved_at IS NULL`` so a row resolved by a concurrent
    writer between the diff read and the UPDATE is a silent no-op.
    Both guards are defensive — the scheduler serialises
    ``monitor_positions_job`` via ``max_instances=1`` + per-job
    ``threading.Lock`` (app/jobs/runtime.py:224,243).
    """
    current: dict[tuple[int, str], MonitorAlert] = {
        (a.instrument_id, a.alert_type): a for a in result.alerts
    }

    with conn.transaction():
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                "SELECT instrument_id, alert_type FROM position_alerts WHERE resolved_at IS NULL"
            )
            open_pairs: set[tuple[int, str]] = {
                (int(row["instrument_id"]), str(row["alert_type"])) for row in cur.fetchall()
            }

            to_open = set(current.keys()) - open_pairs
            to_resolve = open_pairs - set(current.keys())
            unchanged = len(open_pairs & set(current.keys()))

            opened = 0
            for key in to_open:
                alert = current[key]
                cur.execute(
                    """
                    INSERT INTO position_alerts
                        (instrument_id, alert_type, detail, current_bid)
                    VALUES (%(instrument_id)s, %(alert_type)s, %(detail)s, %(current_bid)s)
                    ON CONFLICT (instrument_id, alert_type) WHERE resolved_at IS NULL
                    DO NOTHING
                    """,
                    {
                        "instrument_id": alert.instrument_id,
                        "alert_type": alert.alert_type,
                        "detail": alert.detail,
                        "current_bid": alert.current_bid,
                    },
                )
                # rowcount == 1 on insert, 0 on ON CONFLICT DO NOTHING (race backstop).
                if cur.rowcount == 1:
                    opened += 1

            resolved = 0
            for instrument_id, alert_type in to_resolve:
                cur.execute(
                    """
                    UPDATE position_alerts
                    SET resolved_at = now()
                    WHERE instrument_id = %(instrument_id)s
                      AND alert_type = %(alert_type)s
                      AND resolved_at IS NULL
                    """,
                    {"instrument_id": instrument_id, "alert_type": alert_type},
                )
                if cur.rowcount == 1:
                    resolved += 1

    return PersistStats(opened=opened, resolved=resolved, unchanged=unchanged)
```

- [ ] **Step 4: Run empty-state test**

```bash
uv run pytest tests/test_position_monitor.py::TestPersistPositionAlerts::test_empty_and_empty_is_noop -v
```

Expected: PASS.

- [ ] **Step 5: Add the remaining 9 unit tests**

Append each method inside `TestPersistPositionAlerts`:

```python
    def test_new_breach_opens_episode(
        self, ebull_test_conn: psycopg.Connection[tuple]
    ) -> None:
        iid = _seed_instrument(ebull_test_conn)
        alert = MonitorAlert(
            instrument_id=iid,
            symbol="AAPL",
            alert_type="sl_breach",
            detail="bid=130 < sl=140",
            current_bid=Decimal("130"),
        )
        stats = persist_position_alerts(
            ebull_test_conn, MonitorResult(positions_checked=1, alerts=(alert,))
        )
        assert stats == PersistStats(opened=1, resolved=0, unchanged=0)

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT alert_type, detail, current_bid, resolved_at "
                "FROM position_alerts WHERE instrument_id = %s",
                (iid,),
            )
            rows = cur.fetchall()
        assert len(rows) == 1
        assert rows[0][0] == "sl_breach"
        assert rows[0][1] == "bid=130 < sl=140"
        assert rows[0][2] == Decimal("130")
        assert rows[0][3] is None

    def test_still_breaching_is_noop(
        self, ebull_test_conn: psycopg.Connection[tuple]
    ) -> None:
        iid = _seed_instrument(ebull_test_conn)
        alert = MonitorAlert(
            instrument_id=iid,
            symbol="AAPL",
            alert_type="sl_breach",
            detail="bid=130 < sl=140",
            current_bid=Decimal("130"),
        )
        # First run: opens the episode.
        persist_position_alerts(
            ebull_test_conn, MonitorResult(positions_checked=1, alerts=(alert,))
        )
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT alert_id, opened_at FROM position_alerts WHERE instrument_id = %s",
                (iid,),
            )
            first = cur.fetchone()
        assert first is not None

        # Second run: same breach still present.
        stats = persist_position_alerts(
            ebull_test_conn, MonitorResult(positions_checked=1, alerts=(alert,))
        )
        assert stats == PersistStats(opened=0, resolved=0, unchanged=1)

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT alert_id, opened_at FROM position_alerts WHERE instrument_id = %s",
                (iid,),
            )
            rows = cur.fetchall()
        assert len(rows) == 1
        # alert_id and opened_at unchanged — no new row, no UPDATE on existing.
        assert rows[0][0] == first[0]
        assert rows[0][1] == first[1]

    def test_clearance_resolves_episode(
        self, ebull_test_conn: psycopg.Connection[tuple]
    ) -> None:
        iid = _seed_instrument(ebull_test_conn)
        alert = MonitorAlert(
            instrument_id=iid,
            symbol="AAPL",
            alert_type="sl_breach",
            detail="bid=130 < sl=140",
            current_bid=Decimal("130"),
        )
        persist_position_alerts(
            ebull_test_conn, MonitorResult(positions_checked=1, alerts=(alert,))
        )
        stats = persist_position_alerts(
            ebull_test_conn, MonitorResult(positions_checked=1, alerts=())
        )
        assert stats == PersistStats(opened=0, resolved=1, unchanged=0)

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT resolved_at FROM position_alerts WHERE instrument_id = %s",
                (iid,),
            )
            rows = cur.fetchall()
        assert len(rows) == 1
        assert rows[0][0] is not None

    def test_re_breach_after_clearance_opens_new_episode(
        self, ebull_test_conn: psycopg.Connection[tuple]
    ) -> None:
        iid = _seed_instrument(ebull_test_conn)
        alert = MonitorAlert(
            instrument_id=iid,
            symbol="AAPL",
            alert_type="sl_breach",
            detail="bid=130 < sl=140",
            current_bid=Decimal("130"),
        )
        persist_position_alerts(
            ebull_test_conn, MonitorResult(positions_checked=1, alerts=(alert,))
        )
        persist_position_alerts(
            ebull_test_conn, MonitorResult(positions_checked=1, alerts=())
        )
        stats = persist_position_alerts(
            ebull_test_conn, MonitorResult(positions_checked=1, alerts=(alert,))
        )
        assert stats == PersistStats(opened=1, resolved=0, unchanged=0)

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT resolved_at FROM position_alerts "
                "WHERE instrument_id = %s ORDER BY alert_id",
                (iid,),
            )
            rows = cur.fetchall()
        assert len(rows) == 2
        assert rows[0][0] is not None  # first episode resolved
        assert rows[1][0] is None  # second episode still open

    def test_mixed_across_alert_types(
        self, ebull_test_conn: psycopg.Connection[tuple]
    ) -> None:
        iid = _seed_instrument(ebull_test_conn)
        sl = MonitorAlert(instrument_id=iid, symbol="AAPL", alert_type="sl_breach",
                          detail="sl", current_bid=Decimal("100"))
        tp = MonitorAlert(instrument_id=iid, symbol="AAPL", alert_type="tp_breach",
                          detail="tp", current_bid=Decimal("250"))
        thesis = MonitorAlert(instrument_id=iid, symbol="AAPL", alert_type="thesis_break",
                              detail="red=0.9", current_bid=None)
        # Seed: open sl_breach.
        persist_position_alerts(
            ebull_test_conn, MonitorResult(positions_checked=1, alerts=(sl,))
        )
        # Current: tp + thesis (sl no longer breaching).
        stats = persist_position_alerts(
            ebull_test_conn, MonitorResult(positions_checked=1, alerts=(tp, thesis))
        )
        assert stats == PersistStats(opened=2, resolved=1, unchanged=0)

    def test_mixed_across_instruments(
        self, ebull_test_conn: psycopg.Connection[tuple]
    ) -> None:
        iid_a = _seed_instrument(ebull_test_conn, symbol="AAPL")
        iid_b = _seed_instrument(ebull_test_conn, symbol="MSFT")
        iid_c = _seed_instrument(ebull_test_conn, symbol="GOOG")
        sl_a = MonitorAlert(iid_a, "AAPL", "sl_breach", "a", Decimal("100"))
        sl_b = MonitorAlert(iid_b, "MSFT", "sl_breach", "b", Decimal("100"))
        sl_c = MonitorAlert(iid_c, "GOOG", "sl_breach", "c", Decimal("100"))
        persist_position_alerts(
            ebull_test_conn, MonitorResult(positions_checked=2, alerts=(sl_a, sl_b))
        )
        stats = persist_position_alerts(
            ebull_test_conn, MonitorResult(positions_checked=2, alerts=(sl_a, sl_c))
        )
        # A: unchanged (still breaching). B: resolved. C: opened.
        assert stats == PersistStats(opened=1, resolved=1, unchanged=1)

    def test_partial_unique_index_blocks_duplicate_open_pair(
        self, ebull_test_conn: psycopg.Connection[tuple]
    ) -> None:
        """Direct DB-level test: two open rows for same (instrument, type) fail.

        The writer's ``ON CONFLICT DO NOTHING`` is the defensive-second-layer
        concurrency backstop; its correctness depends on the partial unique
        index itself rejecting duplicates. Hitting the ON CONFLICT path from
        inside the writer diff is impossible (the diff reads the existing row
        and puts it in ``unchanged``), so we pin the index constraint
        directly. If this constraint ever regressed, the writer's backstop
        would silently allow duplicates.
        """
        iid = _seed_instrument(ebull_test_conn)
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "INSERT INTO position_alerts "
                "(instrument_id, alert_type, detail) "
                "VALUES (%s, 'sl_breach', 'first')",
                (iid,),
            )
            with pytest.raises(psycopg.errors.UniqueViolation):
                cur.execute(
                    "INSERT INTO position_alerts "
                    "(instrument_id, alert_type, detail) "
                    "VALUES (%s, 'sl_breach', 'second')",
                    (iid,),
                )

    def test_partial_unique_index_allows_reopen_after_resolve(
        self, ebull_test_conn: psycopg.Connection[tuple]
    ) -> None:
        """Partial index WHERE resolved_at IS NULL — a resolved row does not
        block a new open row for the same (instrument, type)."""
        iid = _seed_instrument(ebull_test_conn)
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "INSERT INTO position_alerts "
                "(instrument_id, alert_type, detail, resolved_at) "
                "VALUES (%s, 'sl_breach', 'first', now())",
                (iid,),
            )
            # No exception — resolved row is not counted by the partial index.
            cur.execute(
                "INSERT INTO position_alerts "
                "(instrument_id, alert_type, detail) "
                "VALUES (%s, 'sl_breach', 'second-open')",
                (iid,),
            )
        ebull_test_conn.commit()

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM position_alerts WHERE instrument_id = %s",
                (iid,),
            )
            assert cur.fetchone() == (2,)

    def test_all_three_alert_types_for_same_instrument(
        self, ebull_test_conn: psycopg.Connection[tuple]
    ) -> None:
        iid = _seed_instrument(ebull_test_conn)
        sl = MonitorAlert(iid, "AAPL", "sl_breach", "s", Decimal("100"))
        tp = MonitorAlert(iid, "AAPL", "tp_breach", "t", Decimal("250"))
        th = MonitorAlert(iid, "AAPL", "thesis_break", "r", None)
        stats = persist_position_alerts(
            ebull_test_conn, MonitorResult(positions_checked=1, alerts=(sl, tp, th))
        )
        assert stats == PersistStats(opened=3, resolved=0, unchanged=0)

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT alert_type FROM position_alerts WHERE instrument_id = %s ORDER BY alert_type",
                (iid,),
            )
            types = [row[0] for row in cur.fetchall()]
        assert types == ["sl_breach", "thesis_break", "tp_breach"]

    def test_current_bid_null_passes_through(
        self, ebull_test_conn: psycopg.Connection[tuple]
    ) -> None:
        iid = _seed_instrument(ebull_test_conn)
        alert = MonitorAlert(iid, "AAPL", "thesis_break", "red=0.9", None)
        persist_position_alerts(
            ebull_test_conn, MonitorResult(positions_checked=1, alerts=(alert,))
        )
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT current_bid FROM position_alerts WHERE instrument_id = %s", (iid,)
            )
            row = cur.fetchone()
        assert row == (None,)
```

- [ ] **Step 6: Run all persist tests**

```bash
uv run pytest tests/test_position_monitor.py::TestPersistPositionAlerts -v
```

Expected: 10 PASS. If `ebull_test` is unreachable, all 10 SKIP — fix your DB connection before proceeding.

- [ ] **Step 7: Commit**

```bash
git add app/services/position_monitor.py tests/test_position_monitor.py
git commit -m "feat(#396): persist_position_alerts episode writer + 10 unit tests"
```

---

## Task 3 — Scheduler wiring

**Files:**
- Modify: `app/workers/scheduler.py:2128-2161`

- [ ] **Step 1: Update `monitor_positions_job` to call the writer**

Replace the function body between `with _tracked_job(JOB_MONITOR_POSITIONS) as tracker:` and the `def fundamentals_sync()` sentinel:

```python
def monitor_positions_job() -> None:
    """Hourly position health check.

    Detects SL/TP breaches and thesis breaks between daily sync cycles.
    Writes one row per breach ONSET to ``position_alerts`` (#396);
    existing open episodes are resolved when the breach clears. Alerts
    also logged for operator visibility via journalctl.

    Read-only with respect to orders — does not place orders or modify
    positions. Writes only to ``position_alerts`` via
    ``persist_position_alerts``.
    """
    with _tracked_job(JOB_MONITOR_POSITIONS) as tracker:
        try:
            with psycopg.connect(settings.database_url) as conn:
                result = check_position_health(conn)
                # Writer has its own inner try/except so that a persist
                # failure does NOT clobber the job's row_count with 0 —
                # check_position_health succeeded, the tracked count reflects
                # what was checked (spec: writer failure must preserve
                # tracker.row_count = result.positions_checked).
                try:
                    stats = persist_position_alerts(conn, result)
                except Exception:
                    logger.error(
                        "monitor_positions: persist_position_alerts failed",
                        exc_info=True,
                    )
                    stats = PersistStats(opened=0, resolved=0, unchanged=0)
        except Exception:
            logger.error("monitor_positions: health check failed", exc_info=True)
            tracker.row_count = 0
            return

        tracker.row_count = result.positions_checked

        if result.alerts:
            for alert in result.alerts:
                logger.warning(
                    "monitor_positions: ALERT %s on %s (instrument_id=%d): %s",
                    alert.alert_type,
                    alert.symbol,
                    alert.instrument_id,
                    alert.detail,
                )

        logger.info(
            "monitor_positions: %d checked, episodes: +%d opened / -%d resolved / %d unchanged",
            result.positions_checked,
            stats.opened,
            stats.resolved,
            stats.unchanged,
        )
```

Update the import on line 60:

```python
from app.services.position_monitor import (
    PersistStats,
    check_position_health,
    persist_position_alerts,
)
```

- [ ] **Step 2: Verify scheduler smoke-tests still pass**

```bash
uv run pytest tests/test_scheduler_autonomous.py -v -k monitor
```

Expected: all existing `monitor_positions` tests PASS (they mock `check_position_health` — if any assertion breaks on the new import or call shape, the mock needs a `persist_position_alerts` patch).

If a test fails because `persist_position_alerts` is called with a MagicMock connection, patch the writer too:

```python
with patch("app.workers.scheduler.persist_position_alerts") as mock_persist:
    mock_persist.return_value = PersistStats(0, 0, 0)
    monitor_positions_job()
```

Update affected tests accordingly.

- [ ] **Step 3: Run the app smoke test**

```bash
uv run pytest tests/smoke/test_app_boots.py -v
```

Expected: PASS — the app must still boot with the scheduler changes.

- [ ] **Step 4: Commit**

```bash
git add app/workers/scheduler.py tests/test_scheduler_autonomous.py
git commit -m "feat(#396): wire persist_position_alerts into monitor_positions_job"
```

---

## Task 4 — GET /alerts/position-alerts

**Files:**
- Modify: `app/api/alerts.py`
- Test: `tests/test_api_alerts.py`

- [ ] **Step 1: Write the failing test — empty state**

Integration tests follow the existing module-level pattern in `tests/test_api_alerts.py`: each test is a free function decorated with `@pytest.mark.skipif("not test_db_available()")`, takes `ebull_test_conn` as a fixture, and uses the module helpers `_seed_operator`, `_bind_test_client`, and `_INT_OP_ID` already defined at [`tests/test_api_alerts.py:355-387`](../../tests/test_api_alerts.py). Auth is globally disabled via [`tests/conftest.py:22`](../../tests/conftest.py) overriding `require_session_or_service_token` — tests do not pass auth headers.

Append to `tests/test_api_alerts.py` **after** the existing `test_integration_*` block (end of file):

```python
# --- #396 position-alert integration tests ----------------------------------

_PA_INSTRUMENT_ID_COUNTER = 1000  # module-scoped unique IDs to avoid PK clashes


def _seed_alert_instrument(
    conn: psycopg.Connection[tuple], *, symbol: str = "AAPL"
) -> int:
    """Insert one instrument row with a unique BIGINT PK; return the id.

    Isolated from the guard-rejection tests' ``iid = 1`` so a single
    ``ebull_test_conn`` fixture can host multiple instruments without PK
    clash after TRUNCATE resets (BIGSERIAL on other tables resets, but
    instruments uses caller-supplied PK).
    """
    global _PA_INSTRUMENT_ID_COUNTER
    _PA_INSTRUMENT_ID_COUNTER += 1
    iid = _PA_INSTRUMENT_ID_COUNTER
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO instruments (instrument_id, symbol, company_name, currency, is_tradable) "
            "VALUES (%s, %s, %s, 'USD', TRUE)",
            (iid, symbol, f"{symbol} Inc."),
        )
    conn.commit()
    return iid


def _seed_position_alert(
    conn: psycopg.Connection[tuple],
    *,
    instrument_id: int,
    alert_type: str = "sl_breach",
    opened_at_offset: str = "-1 hour",
    resolved_at_offset: str | None = None,
    detail: str = "breach",
    current_bid: Decimal | None = Decimal("100"),
) -> int:
    """Insert one position_alerts row with controlled offsets; return alert_id.

    ``opened_at_offset`` / ``resolved_at_offset`` are SQL interval
    literals (``'-1 hour'``, ``'-6 days'``). Whitespace / format is
    re-used verbatim in an f-string inside the INSERT — do not accept
    user input here, only test-controlled constants (prevention:
    f-string SQL composition for column / table identifiers).
    """
    resolved_sql = (
        f"now() + INTERVAL '{resolved_at_offset}'" if resolved_at_offset else "NULL"
    )
    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO position_alerts
                (instrument_id, alert_type, opened_at, resolved_at, detail, current_bid)
            VALUES (
                %s, %s,
                now() + INTERVAL '{opened_at_offset}',
                {resolved_sql},
                %s, %s
            )
            RETURNING alert_id
            """,
            (instrument_id, alert_type, detail, current_bid),
        )
        row = cur.fetchone()
        assert row is not None
    conn.commit()
    return int(row[0])


@pytest.mark.skipif("not test_db_available()")
def test_integration_position_alerts_get_empty_state(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _seed_operator(ebull_test_conn)
    client = _bind_test_client(ebull_test_conn)
    try:
        with patch("app.api.alerts.sole_operator_id", return_value=_INT_OP_ID):
            resp = client.get("/alerts/position-alerts")
        assert resp.status_code == 200
        assert resp.json() == {
            "alerts_last_seen_position_alert_id": None,
            "unseen_count": 0,
            "alerts": [],
        }
    finally:
        from app.db import get_conn

        app.dependency_overrides.pop(get_conn, None)
```

Add `from decimal import Decimal` to the file's imports if not already present.

Run:

```bash
uv run pytest tests/test_api_alerts.py::test_integration_position_alerts_get_empty_state -v
```

Expected: FAIL with 404 (endpoint not registered).

- [ ] **Step 2: Add Pydantic models to `app/api/alerts.py`**

Append after the existing `MarkSeenRequest` class:

```python
AlertType = Literal["sl_breach", "tp_breach", "thesis_break"]


class PositionAlert(BaseModel):
    alert_id: int
    alert_type: AlertType
    instrument_id: int
    symbol: str
    opened_at: datetime
    resolved_at: datetime | None
    detail: str
    current_bid: Decimal | None


class PositionAlertsResponse(BaseModel):
    alerts_last_seen_position_alert_id: int | None
    unseen_count: int
    alerts: list[PositionAlert]


class PositionAlertsMarkSeenRequest(BaseModel):
    seen_through_position_alert_id: int = Field(gt=0)
```

Add to top-of-file imports:

```python
from decimal import Decimal
```

- [ ] **Step 3: Implement `GET /alerts/position-alerts`**

Append to `app/api/alerts.py`:

```python
@router.get("/position-alerts", response_model=PositionAlertsResponse)
def get_position_alerts(
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> PositionAlertsResponse:
    operator_id = _resolve_operator(conn)
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        # 1. Read operator's cursor.
        cur.execute(
            "SELECT alerts_last_seen_position_alert_id FROM operators WHERE operator_id = %(op)s",
            {"op": operator_id},
        )
        op_row = cur.fetchone()
        last_seen: int | None = (
            op_row["alerts_last_seen_position_alert_id"] if op_row else None
        )

        # 2. Count unseen in-window rows (uncapped).
        cur.execute(
            """
            SELECT COUNT(*) AS unseen_count
            FROM position_alerts
            WHERE opened_at >= now() - INTERVAL '7 days'
              AND (%(last_id)s::BIGINT IS NULL OR alert_id > %(last_id)s::BIGINT)
            """,
            {"last_id": last_seen},
        )
        count_row = cur.fetchone()
        assert count_row is not None, "COUNT(*) always returns a row"
        unseen_count: int = int(count_row["unseen_count"])

        # 3. Fetch the list (capped at 500). ORDER BY alert_id DESC —
        # BIGSERIAL PK is the race-safe ordering (clock-skew irrelevant;
        # single-threaded writer guarantees monotonicity). Matches #394
        # rationale for decision_id.
        cur.execute(
            """
            SELECT
                pa.alert_id,
                pa.alert_type,
                pa.instrument_id,
                i.symbol,
                pa.opened_at,
                pa.resolved_at,
                pa.detail,
                pa.current_bid
            FROM position_alerts pa
            JOIN instruments i ON i.instrument_id = pa.instrument_id
            WHERE pa.opened_at >= now() - INTERVAL '7 days'
            ORDER BY pa.alert_id DESC
            LIMIT 500
            """
        )
        rows = cur.fetchall()

    return PositionAlertsResponse(
        alerts_last_seen_position_alert_id=last_seen,
        unseen_count=unseen_count,
        alerts=[PositionAlert.model_validate(r) for r in rows],
    )
```

- [ ] **Step 4: Run empty-state test**

```bash
uv run pytest tests/test_api_alerts.py::TestPositionAlertsEndpoints::test_get_empty_state_returns_zero_count_and_null_cursor -v
```

Expected: PASS.

- [ ] **Step 5: Add the remaining GET tests**

Append to `tests/test_api_alerts.py`. Each test is a module-level function; each wraps the request block in `try / finally` to pop the `get_conn` override.

```python
@pytest.mark.skipif("not test_db_available()")
def test_integration_position_alerts_get_includes_rows_within_window(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _seed_operator(ebull_test_conn)
    iid = _seed_alert_instrument(ebull_test_conn)
    _seed_position_alert(ebull_test_conn, instrument_id=iid, opened_at_offset="-6 days")
    _seed_position_alert(
        ebull_test_conn, instrument_id=iid, opened_at_offset="-8 days",
        alert_type="tp_breach",
    )
    client = _bind_test_client(ebull_test_conn)
    try:
        with patch("app.api.alerts.sole_operator_id", return_value=_INT_OP_ID):
            resp = client.get("/alerts/position-alerts")
        body = resp.json()
        assert len(body["alerts"]) == 1
        assert body["alerts"][0]["alert_type"] == "sl_breach"
    finally:
        from app.db import get_conn

        app.dependency_overrides.pop(get_conn, None)


@pytest.mark.skipif("not test_db_available()")
def test_integration_position_alerts_get_caps_at_500(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _seed_operator(ebull_test_conn)
    iid = _seed_alert_instrument(ebull_test_conn)
    # Seed 510 RESOLVED rows — partial unique index only fires on unresolved
    # rows, so resolved_at=now() exempts each insert from the open-pair
    # constraint. All share the same (instrument_id, alert_type) key.
    with ebull_test_conn.cursor() as cur:
        for _ in range(510):
            cur.execute(
                "INSERT INTO position_alerts "
                "(instrument_id, alert_type, opened_at, resolved_at, detail) "
                "VALUES (%s, 'sl_breach', now() - INTERVAL '1 hour', now(), 'x')",
                (iid,),
            )
    ebull_test_conn.commit()

    client = _bind_test_client(ebull_test_conn)
    try:
        with patch("app.api.alerts.sole_operator_id", return_value=_INT_OP_ID):
            resp = client.get("/alerts/position-alerts")
        body = resp.json()
        assert len(body["alerts"]) == 500
        assert body["unseen_count"] == 510
    finally:
        from app.db import get_conn

        app.dependency_overrides.pop(get_conn, None)


@pytest.mark.skipif("not test_db_available()")
def test_integration_position_alerts_get_unseen_count_respects_cursor(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _seed_operator(ebull_test_conn)
    iid = _seed_alert_instrument(ebull_test_conn)
    a1 = _seed_position_alert(ebull_test_conn, instrument_id=iid,
                              alert_type="sl_breach", resolved_at_offset="-30 min")
    _seed_position_alert(ebull_test_conn, instrument_id=iid,
                         alert_type="tp_breach", resolved_at_offset="-20 min")
    with ebull_test_conn.cursor() as cur:
        cur.execute(
            "UPDATE operators SET alerts_last_seen_position_alert_id = %s "
            "WHERE operator_id = %s",
            (a1, _INT_OP_ID),
        )
    ebull_test_conn.commit()

    client = _bind_test_client(ebull_test_conn)
    try:
        with patch("app.api.alerts.sole_operator_id", return_value=_INT_OP_ID):
            resp = client.get("/alerts/position-alerts")
        body = resp.json()
        assert body["unseen_count"] == 1
        assert body["alerts_last_seen_position_alert_id"] == a1
    finally:
        from app.db import get_conn

        app.dependency_overrides.pop(get_conn, None)


@pytest.mark.skipif("not test_db_available()")
def test_integration_position_alerts_get_includes_resolved_within_window(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _seed_operator(ebull_test_conn)
    iid = _seed_alert_instrument(ebull_test_conn)
    _seed_position_alert(
        ebull_test_conn, instrument_id=iid,
        opened_at_offset="-6 days", resolved_at_offset="-2 hours",
    )
    client = _bind_test_client(ebull_test_conn)
    try:
        with patch("app.api.alerts.sole_operator_id", return_value=_INT_OP_ID):
            resp = client.get("/alerts/position-alerts")
        body = resp.json()
        assert len(body["alerts"]) == 1
        assert body["alerts"][0]["resolved_at"] is not None
    finally:
        from app.db import get_conn

        app.dependency_overrides.pop(get_conn, None)


@pytest.mark.skipif("not test_db_available()")
def test_integration_position_alerts_get_excludes_old_opened_even_if_unresolved(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _seed_operator(ebull_test_conn)
    iid = _seed_alert_instrument(ebull_test_conn)
    _seed_position_alert(
        ebull_test_conn, instrument_id=iid,
        opened_at_offset="-9 days", resolved_at_offset=None,
    )
    client = _bind_test_client(ebull_test_conn)
    try:
        with patch("app.api.alerts.sole_operator_id", return_value=_INT_OP_ID):
            resp = client.get("/alerts/position-alerts")
        body = resp.json()
        assert body["alerts"] == []
    finally:
        from app.db import get_conn

        app.dependency_overrides.pop(get_conn, None)


@pytest.mark.skipif("not test_db_available()")
def test_integration_position_alerts_get_orders_by_alert_id_not_opened_at(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Later alert_id but earlier opened_at must rank higher."""
    _seed_operator(ebull_test_conn)
    iid = _seed_alert_instrument(ebull_test_conn)
    a1 = _seed_position_alert(
        ebull_test_conn, instrument_id=iid, alert_type="sl_breach",
        opened_at_offset="-10 min", resolved_at_offset="-5 min",
    )
    a2 = _seed_position_alert(
        ebull_test_conn, instrument_id=iid, alert_type="tp_breach",
        opened_at_offset="-1 hour", resolved_at_offset="-30 min",
    )
    client = _bind_test_client(ebull_test_conn)
    try:
        with patch("app.api.alerts.sole_operator_id", return_value=_INT_OP_ID):
            resp = client.get("/alerts/position-alerts")
        body = resp.json()
        # a2 has the later alert_id (inserted second) despite earlier opened_at.
        assert body["alerts"][0]["alert_id"] == a2
        assert body["alerts"][1]["alert_id"] == a1
    finally:
        from app.db import get_conn

        app.dependency_overrides.pop(get_conn, None)
```

- [ ] **Step 6: Run all GET tests**

```bash
uv run pytest tests/test_api_alerts.py -v -k "test_integration_position_alerts_get"
```

Expected: all 7 PASS.

- [ ] **Step 7: Commit**

```bash
git add app/api/alerts.py tests/test_api_alerts.py
git commit -m "feat(#396): GET /alerts/position-alerts + 7 api tests"
```

---

## Task 5 — POST /alerts/position-alerts/seen

**Files:**
- Modify: `app/api/alerts.py`
- Test: `tests/test_api_alerts.py`

- [ ] **Step 1: Write failing test for monotonic UPDATE**

Append to `tests/test_api_alerts.py`:

```python
@pytest.mark.skipif("not test_db_available()")
def test_integration_position_alerts_seen_monotonic(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _seed_operator(ebull_test_conn)
    iid = _seed_alert_instrument(ebull_test_conn)
    _seed_position_alert(
        ebull_test_conn, instrument_id=iid,
        opened_at_offset="-1 hour", resolved_at_offset=None,
    )
    with ebull_test_conn.cursor() as cur:
        cur.execute(
            "UPDATE operators SET alerts_last_seen_position_alert_id = 1000 "
            "WHERE operator_id = %s",
            (_INT_OP_ID,),
        )
    ebull_test_conn.commit()

    client = _bind_test_client(ebull_test_conn)
    try:
        with patch("app.api.alerts.sole_operator_id", return_value=_INT_OP_ID):
            resp = client.post(
                "/alerts/position-alerts/seen",
                json={"seen_through_position_alert_id": 500},
            )
        assert resp.status_code == 204

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT alerts_last_seen_position_alert_id FROM operators "
                "WHERE operator_id = %s",
                (_INT_OP_ID,),
            )
            assert cur.fetchone() == (1000,)
    finally:
        from app.db import get_conn

        app.dependency_overrides.pop(get_conn, None)
```

Run:

```bash
uv run pytest tests/test_api_alerts.py::test_integration_position_alerts_seen_monotonic -v
```

Expected: FAIL with 404.

- [ ] **Step 2: Implement `POST /alerts/position-alerts/seen`**

Append to `app/api/alerts.py`:

```python
@router.post("/position-alerts/seen", status_code=status.HTTP_204_NO_CONTENT)
def mark_position_alerts_seen(
    body: PositionAlertsMarkSeenRequest,
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> None:
    operator_id = _resolve_operator(conn)
    with conn.cursor() as cur:
        # The m.max_id IS NOT NULL guard makes this a no-op on an empty
        # window — without it, LEAST(client_posted, NULL) would short-circuit
        # to NULL and GREATEST(COALESCE(cursor, 0), NULL) would itself be NULL
        # (PostgreSQL GREATEST ignores NULL arguments), but the simpler reading
        # is: we never want to materialise a cursor value when no rows exist.
        cur.execute(
            """
            UPDATE operators AS op
            SET alerts_last_seen_position_alert_id = GREATEST(
                COALESCE(op.alerts_last_seen_position_alert_id, 0),
                LEAST(%(seen_through)s, m.max_id)
            )
            FROM (
                SELECT MAX(alert_id) AS max_id
                FROM position_alerts
                WHERE opened_at >= now() - INTERVAL '7 days'
            ) AS m
            WHERE op.operator_id = %(op)s
              AND m.max_id IS NOT NULL
            """,
            {"seen_through": body.seen_through_position_alert_id, "op": operator_id},
        )
    conn.commit()
```

- [ ] **Step 3: Run test**

```bash
uv run pytest tests/test_api_alerts.py::TestPositionAlertsEndpoints::test_post_seen_monotonic_never_rewinds -v
```

Expected: PASS.

- [ ] **Step 4: Add remaining `/seen` tests**

Append to `tests/test_api_alerts.py`:

```python
@pytest.mark.skipif("not test_db_available()")
def test_integration_position_alerts_seen_first_time(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _seed_operator(ebull_test_conn)
    iid = _seed_alert_instrument(ebull_test_conn)
    a1 = _seed_position_alert(
        ebull_test_conn, instrument_id=iid, opened_at_offset="-1 hour"
    )
    client = _bind_test_client(ebull_test_conn)
    try:
        with patch("app.api.alerts.sole_operator_id", return_value=_INT_OP_ID):
            resp = client.post(
                "/alerts/position-alerts/seen",
                json={"seen_through_position_alert_id": a1},
            )
        assert resp.status_code == 204

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT alerts_last_seen_position_alert_id FROM operators "
                "WHERE operator_id = %s",
                (_INT_OP_ID,),
            )
            assert cur.fetchone() == (a1,)
    finally:
        from app.db import get_conn

        app.dependency_overrides.pop(get_conn, None)


@pytest.mark.skipif("not test_db_available()")
def test_integration_position_alerts_seen_missing_field_422(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _seed_operator(ebull_test_conn)
    client = _bind_test_client(ebull_test_conn)
    try:
        with patch("app.api.alerts.sole_operator_id", return_value=_INT_OP_ID):
            resp = client.post("/alerts/position-alerts/seen", json={})
        assert resp.status_code == 422
    finally:
        from app.db import get_conn

        app.dependency_overrides.pop(get_conn, None)


@pytest.mark.skipif("not test_db_available()")
def test_integration_position_alerts_seen_non_integer_422(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Spec case 12 — non-integer body field rejected."""
    _seed_operator(ebull_test_conn)
    client = _bind_test_client(ebull_test_conn)
    try:
        with patch("app.api.alerts.sole_operator_id", return_value=_INT_OP_ID):
            resp = client.post(
                "/alerts/position-alerts/seen",
                json={"seen_through_position_alert_id": "abc"},
            )
        assert resp.status_code == 422
    finally:
        from app.db import get_conn

        app.dependency_overrides.pop(get_conn, None)


@pytest.mark.skipif("not test_db_available()")
def test_integration_position_alerts_seen_non_positive_422(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _seed_operator(ebull_test_conn)
    client = _bind_test_client(ebull_test_conn)
    try:
        with patch("app.api.alerts.sole_operator_id", return_value=_INT_OP_ID):
            resp = client.post(
                "/alerts/position-alerts/seen",
                json={"seen_through_position_alert_id": 0},
            )
        assert resp.status_code == 422
    finally:
        from app.db import get_conn

        app.dependency_overrides.pop(get_conn, None)


@pytest.mark.skipif("not test_db_available()")
def test_integration_position_alerts_seen_clamped_to_in_window_max(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _seed_operator(ebull_test_conn)
    iid = _seed_alert_instrument(ebull_test_conn)
    a1 = _seed_position_alert(
        ebull_test_conn, instrument_id=iid, opened_at_offset="-1 hour"
    )
    client = _bind_test_client(ebull_test_conn)
    try:
        with patch("app.api.alerts.sole_operator_id", return_value=_INT_OP_ID):
            resp = client.post(
                "/alerts/position-alerts/seen",
                json={"seen_through_position_alert_id": 99_999},
            )
        assert resp.status_code == 204

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT alerts_last_seen_position_alert_id FROM operators "
                "WHERE operator_id = %s",
                (_INT_OP_ID,),
            )
            assert cur.fetchone() == (a1,)
    finally:
        from app.db import get_conn

        app.dependency_overrides.pop(get_conn, None)


@pytest.mark.skipif("not test_db_available()")
def test_integration_position_alerts_seen_empty_window_noop(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _seed_operator(ebull_test_conn)
    client = _bind_test_client(ebull_test_conn)
    try:
        with patch("app.api.alerts.sole_operator_id", return_value=_INT_OP_ID):
            resp = client.post(
                "/alerts/position-alerts/seen",
                json={"seen_through_position_alert_id": 500},
            )
        assert resp.status_code == 204

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT alerts_last_seen_position_alert_id FROM operators "
                "WHERE operator_id = %s",
                (_INT_OP_ID,),
            )
            # Cursor stays NULL — no 0 written. Divergence from #394 /alerts/seen
            # (which does write 0 on the same edge; tracked separately).
            assert cur.fetchone() == (None,)
    finally:
        from app.db import get_conn

        app.dependency_overrides.pop(get_conn, None)


@pytest.mark.skipif("not test_db_available()")
def test_integration_position_alerts_seen_race_strict_greater(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _seed_operator(ebull_test_conn)
    iid = _seed_alert_instrument(ebull_test_conn)
    a_old = _seed_position_alert(
        ebull_test_conn, instrument_id=iid, alert_type="sl_breach",
        opened_at_offset="-1 hour", resolved_at_offset="-30 min",
    )
    client = _bind_test_client(ebull_test_conn)
    try:
        with patch("app.api.alerts.sole_operator_id", return_value=_INT_OP_ID):
            client.post(
                "/alerts/position-alerts/seen",
                json={"seen_through_position_alert_id": a_old},
            )
        # Row arrives AFTER the POST — larger alert_id, must remain unseen.
        _seed_position_alert(
            ebull_test_conn, instrument_id=iid, alert_type="tp_breach",
            opened_at_offset="-5 min",
        )
        with patch("app.api.alerts.sole_operator_id", return_value=_INT_OP_ID):
            resp = client.get("/alerts/position-alerts")
        body = resp.json()
        assert body["unseen_count"] == 1
    finally:
        from app.db import get_conn

        app.dependency_overrides.pop(get_conn, None)
```

- [ ] **Step 5: Run all /seen tests**

```bash
uv run pytest tests/test_api_alerts.py -v -k "test_integration_position_alerts_seen"
```

Expected: 7 PASS (monotonic, first-time, missing-field-422, non-integer-422, non-positive-422, clamped, empty-window-noop, race). That is 8 — verify count matches.

- [ ] **Step 6: Commit**

```bash
git add app/api/alerts.py tests/test_api_alerts.py
git commit -m "feat(#396): POST /alerts/position-alerts/seen + 6 api tests"
```

---

## Task 6 — POST /alerts/position-alerts/dismiss-all

**Files:**
- Modify: `app/api/alerts.py`
- Test: `tests/test_api_alerts.py`

- [ ] **Step 1: Write failing test for MAX advance**

Append to `tests/test_api_alerts.py`:

```python
@pytest.mark.skipif("not test_db_available()")
def test_integration_position_alerts_dismiss_all_advances_to_max(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _seed_operator(ebull_test_conn)
    iid = _seed_alert_instrument(ebull_test_conn)
    _seed_position_alert(
        ebull_test_conn, instrument_id=iid, alert_type="sl_breach",
        opened_at_offset="-3 hours", resolved_at_offset="-2 hours",
    )
    _seed_position_alert(
        ebull_test_conn, instrument_id=iid, alert_type="tp_breach",
        opened_at_offset="-2 hours", resolved_at_offset="-1 hour",
    )
    a3 = _seed_position_alert(
        ebull_test_conn, instrument_id=iid, alert_type="thesis_break",
        opened_at_offset="-30 min",
    )
    client = _bind_test_client(ebull_test_conn)
    try:
        with patch("app.api.alerts.sole_operator_id", return_value=_INT_OP_ID):
            resp = client.post("/alerts/position-alerts/dismiss-all")
        assert resp.status_code == 204

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT alerts_last_seen_position_alert_id FROM operators "
                "WHERE operator_id = %s",
                (_INT_OP_ID,),
            )
            assert cur.fetchone() == (a3,)
    finally:
        from app.db import get_conn

        app.dependency_overrides.pop(get_conn, None)
```

Run:

```bash
uv run pytest tests/test_api_alerts.py::test_integration_position_alerts_dismiss_all_advances_to_max -v
```

Expected: FAIL with 404.

- [ ] **Step 2: Implement `POST /alerts/position-alerts/dismiss-all`**

Append to `app/api/alerts.py`:

```python
@router.post("/position-alerts/dismiss-all", status_code=status.HTTP_204_NO_CONTENT)
def dismiss_all_position_alerts(
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> None:
    operator_id = _resolve_operator(conn)
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE operators AS op
            SET alerts_last_seen_position_alert_id = GREATEST(
                COALESCE(op.alerts_last_seen_position_alert_id, 0),
                m.max_id
            )
            FROM (
                SELECT MAX(alert_id) AS max_id
                FROM position_alerts
                WHERE opened_at >= now() - INTERVAL '7 days'
            ) AS m
            WHERE op.operator_id = %(op)s
              AND m.max_id IS NOT NULL
            """,
            {"op": operator_id},
        )
    conn.commit()
```

- [ ] **Step 3: Run the advance test**

```bash
uv run pytest tests/test_api_alerts.py::TestPositionAlertsEndpoints::test_post_dismiss_all_advances_cursor_to_max -v
```

Expected: PASS.

- [ ] **Step 4: Add remaining /dismiss-all tests**

Append to `tests/test_api_alerts.py`:

```python
@pytest.mark.skipif("not test_db_available()")
def test_integration_position_alerts_dismiss_all_monotonic(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _seed_operator(ebull_test_conn)
    iid = _seed_alert_instrument(ebull_test_conn)
    _seed_position_alert(
        ebull_test_conn, instrument_id=iid,
        opened_at_offset="-1 hour", resolved_at_offset="-30 min",
    )
    with ebull_test_conn.cursor() as cur:
        cur.execute(
            "UPDATE operators SET alerts_last_seen_position_alert_id = 500 "
            "WHERE operator_id = %s",
            (_INT_OP_ID,),
        )
    ebull_test_conn.commit()

    client = _bind_test_client(ebull_test_conn)
    try:
        with patch("app.api.alerts.sole_operator_id", return_value=_INT_OP_ID):
            resp = client.post("/alerts/position-alerts/dismiss-all")
        assert resp.status_code == 204
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT alerts_last_seen_position_alert_id FROM operators "
                "WHERE operator_id = %s",
                (_INT_OP_ID,),
            )
            assert cur.fetchone() == (500,)
    finally:
        from app.db import get_conn

        app.dependency_overrides.pop(get_conn, None)


@pytest.mark.skipif("not test_db_available()")
def test_integration_position_alerts_dismiss_all_empty_window_null_cursor(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _seed_operator(ebull_test_conn)
    client = _bind_test_client(ebull_test_conn)
    try:
        with patch("app.api.alerts.sole_operator_id", return_value=_INT_OP_ID):
            resp = client.post("/alerts/position-alerts/dismiss-all")
        assert resp.status_code == 204
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT alerts_last_seen_position_alert_id FROM operators "
                "WHERE operator_id = %s",
                (_INT_OP_ID,),
            )
            assert cur.fetchone() == (None,)
    finally:
        from app.db import get_conn

        app.dependency_overrides.pop(get_conn, None)


@pytest.mark.skipif("not test_db_available()")
def test_integration_position_alerts_dismiss_all_empty_window_existing_cursor(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _seed_operator(ebull_test_conn)
    with ebull_test_conn.cursor() as cur:
        cur.execute(
            "UPDATE operators SET alerts_last_seen_position_alert_id = 500 "
            "WHERE operator_id = %s",
            (_INT_OP_ID,),
        )
    ebull_test_conn.commit()

    client = _bind_test_client(ebull_test_conn)
    try:
        with patch("app.api.alerts.sole_operator_id", return_value=_INT_OP_ID):
            client.post("/alerts/position-alerts/dismiss-all")
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT alerts_last_seen_position_alert_id FROM operators "
                "WHERE operator_id = %s",
                (_INT_OP_ID,),
            )
            assert cur.fetchone() == (500,)
    finally:
        from app.db import get_conn

        app.dependency_overrides.pop(get_conn, None)


@pytest.mark.skipif("not test_db_available()")
def test_integration_position_alerts_dismiss_all_race_later_row_unseen(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _seed_operator(ebull_test_conn)
    iid = _seed_alert_instrument(ebull_test_conn)
    _seed_position_alert(
        ebull_test_conn, instrument_id=iid, alert_type="sl_breach",
        opened_at_offset="-2 hours", resolved_at_offset="-1 hour",
    )
    _seed_position_alert(
        ebull_test_conn, instrument_id=iid, alert_type="tp_breach",
        opened_at_offset="-1 hour", resolved_at_offset="-30 min",
    )
    client = _bind_test_client(ebull_test_conn)
    try:
        with patch("app.api.alerts.sole_operator_id", return_value=_INT_OP_ID):
            client.post("/alerts/position-alerts/dismiss-all")
        _seed_position_alert(
            ebull_test_conn, instrument_id=iid, alert_type="thesis_break",
            opened_at_offset="-5 min",
        )
        with patch("app.api.alerts.sole_operator_id", return_value=_INT_OP_ID):
            resp = client.get("/alerts/position-alerts")
        body = resp.json()
        assert body["unseen_count"] == 1
    finally:
        from app.db import get_conn

        app.dependency_overrides.pop(get_conn, None)
```

- [ ] **Step 5: Add cross-cutting tests (cursor isolation both directions, 503/501, alert_type round-trip)**

Append to `tests/test_api_alerts.py`:

```python
@pytest.mark.skipif("not test_db_available()")
def test_integration_position_alerts_cursor_isolated_from_guard_direction_1(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """POST /alerts/position-alerts/seen must not touch the guard cursor."""
    _seed_operator(ebull_test_conn)
    iid = _seed_alert_instrument(ebull_test_conn)
    a1 = _seed_position_alert(
        ebull_test_conn, instrument_id=iid, opened_at_offset="-1 hour"
    )
    client = _bind_test_client(ebull_test_conn)
    try:
        with patch("app.api.alerts.sole_operator_id", return_value=_INT_OP_ID):
            client.post(
                "/alerts/position-alerts/seen",
                json={"seen_through_position_alert_id": a1},
            )
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT alerts_last_seen_decision_id, alerts_last_seen_position_alert_id "
                "FROM operators WHERE operator_id = %s",
                (_INT_OP_ID,),
            )
            guard_cursor, pos_cursor = cur.fetchone()
        assert guard_cursor is None  # untouched
        assert pos_cursor == a1
    finally:
        from app.db import get_conn

        app.dependency_overrides.pop(get_conn, None)


@pytest.mark.skipif("not test_db_available()")
def test_integration_position_alerts_cursor_isolated_from_guard_direction_2(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """POST /alerts/seen (guard) must not touch the position cursor."""
    _seed_operator(ebull_test_conn)
    # Seed one guard rejection so the /alerts/seen UPDATE is not a no-op.
    with ebull_test_conn.cursor() as cur:
        cur.execute(
            "INSERT INTO decision_audit "
            "(decision_time, stage, pass_fail, explanation) "
            "VALUES (now(), 'execution_guard', 'FAIL', 'guard-row') "
            "RETURNING decision_id"
        )
        row = cur.fetchone()
        assert row is not None
        did = row[0]
    ebull_test_conn.commit()

    client = _bind_test_client(ebull_test_conn)
    try:
        with patch("app.api.alerts.sole_operator_id", return_value=_INT_OP_ID):
            client.post(
                "/alerts/seen",
                json={"seen_through_decision_id": did},
            )
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT alerts_last_seen_decision_id, alerts_last_seen_position_alert_id "
                "FROM operators WHERE operator_id = %s",
                (_INT_OP_ID,),
            )
            guard_cursor, pos_cursor = cur.fetchone()
        assert guard_cursor == did
        assert pos_cursor is None  # untouched
    finally:
        from app.db import get_conn

        app.dependency_overrides.pop(get_conn, None)


@pytest.mark.skipif("not test_db_available()")
def test_integration_position_alerts_no_operator_returns_503(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Do NOT seed an operator AND do NOT patch sole_operator_id — the
    real resolver must raise NoOperatorError and the API must map to 503.
    """
    client = _bind_test_client(ebull_test_conn)
    try:
        # No patch — exercise the real operator resolver path.
        assert client.get("/alerts/position-alerts").status_code == 503
        assert client.post(
            "/alerts/position-alerts/seen",
            json={"seen_through_position_alert_id": 1},
        ).status_code == 503
        assert client.post(
            "/alerts/position-alerts/dismiss-all"
        ).status_code == 503
    finally:
        from app.db import get_conn

        app.dependency_overrides.pop(get_conn, None)


@pytest.mark.skipif("not test_db_available()")
def test_integration_position_alerts_multiple_operators_returns_501(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    """Seed two operators, do NOT patch sole_operator_id — real resolver
    raises AmbiguousOperatorError → 501."""
    _seed_operator(ebull_test_conn)
    second_id = UUID("22222222-2222-2222-2222-222222222222")
    with ebull_test_conn.cursor() as cur:
        cur.execute(
            "INSERT INTO operators (operator_id, username, password_hash) "
            "VALUES (%s, 'alerts_test_op2', 'x') ON CONFLICT DO NOTHING",
            (second_id,),
        )
    ebull_test_conn.commit()

    client = _bind_test_client(ebull_test_conn)
    try:
        assert client.get("/alerts/position-alerts").status_code == 501
        assert client.post(
            "/alerts/position-alerts/seen",
            json={"seen_through_position_alert_id": 1},
        ).status_code == 501
        assert client.post(
            "/alerts/position-alerts/dismiss-all"
        ).status_code == 501
    finally:
        from app.db import get_conn

        app.dependency_overrides.pop(get_conn, None)


@pytest.mark.skipif("not test_db_available()")
def test_integration_position_alerts_alert_type_round_trip_all_three(
    ebull_test_conn: psycopg.Connection[tuple],
) -> None:
    _seed_operator(ebull_test_conn)
    iid = _seed_alert_instrument(ebull_test_conn)
    offsets = {"sl_breach": "-3 hours", "tp_breach": "-2 hours", "thesis_break": "-1 hour"}
    for t, offset in offsets.items():
        _seed_position_alert(
            ebull_test_conn, instrument_id=iid,
            alert_type=t, opened_at_offset=offset,
        )

    client = _bind_test_client(ebull_test_conn)
    try:
        with patch("app.api.alerts.sole_operator_id", return_value=_INT_OP_ID):
            resp = client.get("/alerts/position-alerts")
        body = resp.json()
        types = {row["alert_type"] for row in body["alerts"]}
        assert types == {"sl_breach", "tp_breach", "thesis_break"}
    finally:
        from app.db import get_conn

        app.dependency_overrides.pop(get_conn, None)
```

- [ ] **Step 6: Run all position-alert endpoint tests**

```bash
uv run pytest tests/test_api_alerts.py -v -k "test_integration_position_alerts"
```

Expected: 23 PASS.

Test inventory:
- GET: empty_state, includes_rows_within_window, caps_at_500, unseen_count_respects_cursor, includes_resolved_within_window, excludes_old_opened_even_if_unresolved, orders_by_alert_id_not_opened_at (**7 tests**)
- POST /seen: monotonic, first_time, missing_field_422, non_integer_422, non_positive_422, clamped_to_in_window_max, empty_window_noop, race_strict_greater (**8 tests**)
- POST /dismiss-all: advances_to_max, monotonic, empty_window_null_cursor, empty_window_existing_cursor, race_later_row_unseen (**5 tests**)
- Cross-cutting: cursor_isolated_from_guard_direction_1, cursor_isolated_from_guard_direction_2, no_operator_returns_503, multiple_operators_returns_501, alert_type_round_trip_all_three (**5 tests**) → wait, 2+1+1+1 = 5 cross-cutting; total 7+8+5+5 = 25.

Reconcile: spec enumerates 22 API tests. Plan adds more because spec counts the two directions of cursor isolation as one line; plan splits into two. Accept the over-coverage.

- [ ] **Step 7: Commit**

```bash
git add app/api/alerts.py tests/test_api_alerts.py
git commit -m "feat(#396): POST /alerts/position-alerts/dismiss-all + cross-cutting tests"
```

---

## Task 7 — Gates + Codex pre-push review + PR

- [ ] **Step 1: Run full pre-push gate locally**

```bash
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv run pytest
```

All four must pass. If any fails, fix root cause — do NOT skip or xfail.

Smoke test must be in the last `pytest` run — `tests/smoke/test_app_boots.py` is the gate that catches lifespan-only failures per CLAUDE.md `feedback_smoke_gate_swallowed_failures` memory.

- [ ] **Step 2: Run Codex pre-push review**

```bash
codex.cmd exec review
```

Address every finding: FIXED / DEFERRED / REBUTTED. No findings left silent. Fix any real issue in the diff before pushing.

- [ ] **Step 3: Re-run gates after Codex fixes (if any)**

```bash
uv run ruff check . && uv run ruff format --check . && uv run pyright && uv run pytest
```

Expected: all green.

- [ ] **Step 4: Push branch + open PR**

PR body is written to a temp file first so the `gh` invocation works in both bash and PowerShell (no heredoc dependency). Use an absolute path so the command is CWD-independent.

Write `/tmp/pr_396_body.md` (or platform-equivalent temp path) with this content:

```markdown
## What
Persist SL/TP/thesis breaches from hourly `position_monitor.check_position_health` to a new `position_alerts` episode table. Expose via `GET /alerts/position-alerts`, `POST /alerts/position-alerts/seen`, `POST /alerts/position-alerts/dismiss-all` — mirrors #394's guard-rejection shape so #399 can union all three feeds.

## Why
Closes #396. Today `monitor_positions_job` logs breaches to stderr only; no DB rows, no strip, no "since last visit" semantics. Episode model (one row per breach onset, `resolved_at` on clearance) matches eBull's append-with-intent convention for recommendations — no spam on still-breaching evaluations.

## Test plan
- [x] Unit (11): `persist_position_alerts` diff logic against `ebull_test` — empty/empty, new-breach, still-breaching, clearance, re-break, mixed types, mixed instruments, partial-unique-index blocks duplicate open pair, partial-unique-index allows reopen after resolve, all-three-types, NULL current_bid
- [x] API (25): GET empty/window/cap/cursor/resolved-in-window/out-of-window/ordering (7); /seen monotonic/first-time/missing-field-422/non-integer-422/non-positive-422/clamp/empty-window-noop/race (8); /dismiss-all advance/monotonic/empty-window-null/empty-window-existing/race (5); cross-cutting cursor-isolation-both-directions/503/501/alert_type-round-trip (5)
- [x] Smoke (`tests/smoke/test_app_boots.py`) green
- [x] Codex pre-spec + pre-plan + pre-push reviews clean
- [x] All gates green: `ruff check`, `ruff format --check`, `pyright`, `pytest`

Spec: `docs/superpowers/specs/2026-04-21-position-alert-persistence.md`
Plan: `docs/superpowers/plans/2026-04-22-position-alert-persistence.md`

🤖 Generated with [Claude Code](https://claude.com/claude-code)
```

Then push and create the PR:

```bash
git push -u origin feature/396-position-alert-persistence
gh pr create \
  --title "feat(#396): position-alert event persistence" \
  --body-file /tmp/pr_396_body.md
```

On PowerShell / Windows, substitute the temp path (e.g. `$env:TEMP\pr_396_body.md`).

- [ ] **Step 5: Start polling review + CI immediately**

Per `feedback_post_push_cycle.md`: do NOT wait for user prompt. Start polling.

```bash
gh pr view <PR_NUMBER> --comments
gh pr checks <PR_NUMBER>
```

Repeat until both Claude review bot has posted AND CI is green on latest commit. Resolve every comment with FIXED / DEFERRED / REBUTTED. Re-run all gates between follow-up pushes.

- [ ] **Step 6: Merge when APPROVE on latest commit + CI green**

Check Codex if the latest review round is rebuttal-only (per CLAUDE.md decision tree). Merge only when:
- APPROVE on the MOST RECENT commit, OR
- Latest round is all rebuttals, Codex independently agrees rebuttals are sound

On merge:
- Delete local + remote branch
- Close #396
- #397 and #399 remain open (#399 now has one of two preconditions met)

---

## Self-review checks (author, pre-Codex)

- **Spec coverage** — every section of `docs/superpowers/specs/2026-04-21-position-alert-persistence.md` is represented:
  - Schema → Task 1
  - Scheduler serialization rebuttal → documented in migration comment + writer docstring
  - Writer (`persist_position_alerts`) → Task 2
  - Scheduler wiring → Task 3
  - GET /alerts/position-alerts → Task 4
  - POST /alerts/position-alerts/seen → Task 5
  - POST /alerts/position-alerts/dismiss-all → Task 6
  - GET snapshot consistency deferral → noted in Task 4 comment (references #395)
  - All 25 API tests + 11 unit tests → Tasks 2, 4, 5, 6 (spec enumerated 22 + 10; plan splits a few tests into direction-pairs and adds one partial-index-allows-reopen unit test for full coverage of the DB-level invariant)

- **Type consistency** — `AlertType = Literal["sl_breach", "tp_breach", "thesis_break"]` defined once in `app/api/alerts.py`; `position_monitor.AlertType` mirrors it exactly; DB CHECK constraint in migration matches; Pydantic `PositionAlert.alert_type` uses the type alias.

- **No placeholders** — every step has concrete code or exact command + expected output. No "TBD" or "similar to above" references.
