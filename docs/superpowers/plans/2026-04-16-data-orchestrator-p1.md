# Data Orchestrator — Phase 1: Adapters + Registry (behind ORCHESTRATOR_ENABLED flag)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the sync orchestrator core — schema, registry, planner, crash-guarded executor, 13 layer adapters — behind feature flag `ORCHESTRATOR_ENABLED=false`. Existing `/system/*` and `/jobs/*` endpoints stay untouched. `POST /sync` returns 503 until Phase 4 flips the flag.

**Architecture:** One new package (`app/services/sync_orchestrator/` — NOT a single `.py` file) that owns the DAG, split into focused modules (`types.py`, `freshness.py`, `registry.py`, `planner.py`, `executor.py`, `adapters.py`, `reaper.py`, `__init__.py`). One new migration (`033_sync_orchestrator.sql`), and thin adapters that wrap existing scheduler job functions per the §2.3 refresh contract. Existing scheduler.py early-return holes (10 lines) get rewritten to raise-or-PREREQ_SKIP so `_tracked_job` records truthful `job_runs` rows.

**Tech Stack:** Python 3.14, psycopg v3 (autocommit + `with conn.transaction()`), FastAPI, PostgreSQL 17

**Spec:** `docs/superpowers/specs/2026-04-16-data-orchestrator-and-observability-design.md` (Codex-approved round 15)

**Status:** READY TO IMPLEMENT — Codex approved round 9 (2026-04-16)

**Settled decisions preserved:**

- **Provider design rule** — adapters are thin wrappers, no domain logic in orchestrator.
- **Guard auditability** — every layer run writes `job_runs` + `sync_layer_progress` rows. No silent success.
- **No leverage/shorting in v1** — orchestrator does not change execution policy; just schedules data refreshes.
- **psycopg3 correctness** — every orchestrator-owned connection uses `autocommit=True` + `with conn.transaction()` so the context manager issues a real BEGIN/COMMIT, not a SAVEPOINT.

**Prevention log entries checked:**

- **`with conn.transaction()` on non-autocommit** → SAVEPOINT, not commit. Rule enforced throughout planner and audit writers.
- **`record_job_skip` requires autocommit** per [ops_monitor.py:322](app/services/ops_monitor.py#L322). Adapters honour this.
- **JOIN fan-out** — planner queries filter on `job_runs` with `ORDER BY started_at DESC LIMIT 1` to avoid fan-out.
- **Single-row UPDATE silent no-op** — `_finalize_sync_run` asserts 1 row updated.

---

## Task Dependency Graph

Tasks are **sequential**, not independently parallelizable. Each task's output feeds the next. Agentic workers should execute tasks in order. Parallel execution is not supported in Phase 1 because:

- Task 2 (types) exports symbols imported by Tasks 3–11.
- Task 3 (freshness) is imported by Task 4 (registry).
- Task 4 (registry) is imported by Task 5 (planner).
- Task 5 (planner) is called by Task 6 (executor).
- Task 6 (executor) wraps Task 7 (entry points).
- Task 8 (scheduler early-returns) and Task 9 (morning extract) both edit [scheduler.py](app/workers/scheduler.py) in overlapping regions (the morning_candidate_review body); they share write ownership of the same file and must run sequentially. Task 9 must finish before Task 10 (the morning adapter imports `compute_morning_recommendations`).
- Task 10 (adapters) depends on Tasks 2, 4, 8, 9.
- Task 11 (reaper + flag + API) depends on Tasks 2, 6, 7.
- Task 12 (pre-push) depends on every prior task.

Dependency chain: `1 → 2 → 3 → 4 → 5 → 6 → 7 → 8 → 9 → 10 → 11 → 12`.

Merge strategy: one PR per Phase 1, not one PR per task. Tasks commit to a single feature branch `feature/{issue}-data-orchestrator-phase-1`; the PR opens after Task 12 self-review passes.

---

## File Map

| File | Action | Purpose |
|------|--------|---------|
| `sql/033_sync_orchestrator.sql` | Create | Migration: `sync_runs`, `sync_layer_progress`, `idx_sync_runs_single_running` partial unique index, `idx_sync_runs_started` |
| `app/services/sync_orchestrator/__init__.py` | Create | Package entrypoint — re-exports public API (types, entry points, registries). **This is a package, not a module file** — no `app/services/sync_orchestrator.py`. |
| `app/services/sync_orchestrator/types.py` | Create | Types + constants (LayerOutcome, RefreshResult, LayerPlan, ExecutionPlan, LayerRefresh protocol, SyncScope, SyncTrigger, SyncResult, SyncAlreadyRunning, PREREQ_SKIP_MARKER, prereq_skip_reason). No I/O. |
| `app/services/sync_orchestrator/freshness.py` | Create | Per-layer is_fresh predicates (Task 3) |
| `app/services/sync_orchestrator/registry.py` | Create | LAYERS and JOB_TO_LAYERS registries (Task 4) |
| `app/services/sync_orchestrator/planner.py` | Create | build_execution_plan (Task 5) |
| `app/services/sync_orchestrator/executor.py` | Create | _start_sync_run, _run_layers_loop, _safe_run_and_finalize, audit writers (Task 6) |
| `app/services/sync_orchestrator/adapters.py` | Create | 13 layer adapters (Task 10) |
| `app/services/sync_orchestrator/reaper.py` | Create | Boot-time orphaned-sync reaper (Task 11) |
| `app/workers/scheduler.py` | Modify | Close 10 early-return holes; extract `compute_morning_recommendations` from `morning_candidate_review` |
| `app/main.py` | Modify | Call reaper before scheduler start; register `POST /sync` (gated 503 while `ORCHESTRATOR_ENABLED=false`) |
| `app/api/sync.py` | Create | Stub `POST /sync` + `GET /sync/status` + `GET /sync/layers` + `GET /sync/runs` endpoints (Phase 1: return 503 for POST, real reads for GET) |
| `app/config.py` | Modify | Add `orchestrator_enabled: bool = False` setting |
| `tests/test_sync_orchestrator_types.py` | Create | Types + constants unit tests |
| `tests/test_sync_orchestrator_freshness.py` | Create | Freshness predicate tests, one class per layer |
| `tests/test_sync_orchestrator_planner.py` | Create | `build_execution_plan` + dependency derivation tests |
| `tests/test_sync_orchestrator_executor.py` | Create | `_run_layers_loop`, `_safe_run_and_finalize`, composite tests, crash path |
| `tests/test_sync_orchestrator_concurrency.py` | Create | Partial unique index 409 path, orphan reaper |
| `tests/test_sync_orchestrator_adapters.py` | Create | Per-adapter tests (one class per layer) |
| `tests/test_scheduler_early_return_holes.py` | Create | Regression tests that the 10 rewritten early-returns now raise or PREREQ_SKIP as expected |

---

### Task 1: Migration — sync_runs and sync_layer_progress

**Files:**

- Create: `sql/033_sync_orchestrator.sql`

Creates the two new tables and the partial unique index that serves as the authoritative concurrency gate. Schema matches spec §2.8 exactly.

- [ ] **Step 1: Write the migration**

```sql
-- 033_sync_orchestrator.sql
--
-- Phase 1 of Data Orchestrator (spec: 2026-04-16-data-orchestrator-and-observability-design.md)
--
-- Creates:
--   sync_runs                       — one row per orchestrator invocation
--   sync_layer_progress             — one row per emitted layer per sync run
--   idx_sync_runs_single_running    — authoritative concurrency gate (partial unique)
--   idx_sync_runs_started           — lookup index for recent runs

CREATE TABLE IF NOT EXISTS sync_runs (
    sync_run_id    BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    scope          TEXT NOT NULL CHECK (scope IN ('full', 'layer', 'high_frequency', 'job')),
    scope_detail   TEXT,  -- layer name if scope='layer', legacy job name if scope='job', NULL for full/high_frequency
    trigger        TEXT NOT NULL CHECK (trigger IN ('manual', 'scheduled', 'catch_up')),
    started_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at    TIMESTAMPTZ,
    status         TEXT NOT NULL DEFAULT 'running'
                   CHECK (status IN ('running', 'complete', 'partial', 'failed')),
    layers_planned INTEGER NOT NULL,
    layers_done    INTEGER NOT NULL DEFAULT 0,
    layers_failed  INTEGER NOT NULL DEFAULT 0,
    layers_skipped INTEGER NOT NULL DEFAULT 0,
    error_category TEXT  -- sanitized category when status='failed' (§3.4)
);

CREATE INDEX IF NOT EXISTS idx_sync_runs_started
    ON sync_runs(started_at DESC);

-- Authoritative concurrency gate: at most one 'running' sync_runs row
-- across all scopes. Duplicate INSERT → UniqueViolation → SyncAlreadyRunning.
CREATE UNIQUE INDEX IF NOT EXISTS idx_sync_runs_single_running
    ON sync_runs((TRUE))
    WHERE status = 'running';

CREATE TABLE IF NOT EXISTS sync_layer_progress (
    sync_run_id    BIGINT NOT NULL REFERENCES sync_runs(sync_run_id),
    layer_name     TEXT NOT NULL,
    status         TEXT NOT NULL DEFAULT 'pending'
                   CHECK (status IN ('pending', 'running', 'complete', 'failed', 'skipped', 'partial')),
    started_at     TIMESTAMPTZ,
    finished_at    TIMESTAMPTZ,
    items_total    INTEGER,
    items_done     INTEGER,
    row_count      INTEGER,
    error_category TEXT,   -- sanitized category only; full error in logs (§3.4)
    skip_reason    TEXT,
    PRIMARY KEY (sync_run_id, layer_name)
);
```

- [ ] **Step 2: Verify migration runs**

Run: `uv run python -c "import psycopg; from app.config import settings; conn = psycopg.connect(settings.database_url); conn.execute(open('sql/033_sync_orchestrator.sql').read()); conn.commit(); print('OK')"`
Expected: `OK`.

- [ ] **Step 3: Verify partial unique index**

```bash
uv run python -c "
import psycopg
from app.config import settings
with psycopg.connect(settings.database_url, autocommit=True) as conn:
    with conn.transaction():
        conn.execute(\"INSERT INTO sync_runs (scope, trigger, layers_planned) VALUES ('full', 'manual', 3)\")
    try:
        with conn.transaction():
            conn.execute(\"INSERT INTO sync_runs (scope, trigger, layers_planned) VALUES ('high_frequency', 'scheduled', 2)\")
        print('FAIL: second running row allowed')
    except psycopg.errors.UniqueViolation:
        print('OK: second running row blocked')
    conn.execute(\"UPDATE sync_runs SET status='failed', finished_at=now() WHERE status='running'\")
"
```

Expected: `OK: second running row blocked`.

- [ ] **Step 4: Commit**

```bash
git commit -m "feat(#TBD): migration 033 — sync_runs + sync_layer_progress tables (#TBD)"
```

---

### Task 2: Core types, constants, and exceptions

**Files:**

- Create: `app/services/sync_orchestrator/__init__.py` (empty, marks package)
- Create: `app/services/sync_orchestrator/types.py`
- Create: `tests/test_sync_orchestrator_types.py`

Defines `LayerOutcome`, `RefreshResult`, `LayerPlan`, `ExecutionPlan`, `LayerRefresh` protocol, `SyncScope`, `SyncTrigger`, `SyncResult`, `SyncAlreadyRunning`, `PREREQ_SKIP_MARKER`, `prereq_skip_reason`. No I/O in this file.

- [ ] **Step 1: Write failing tests**

```python
# tests/test_sync_orchestrator_types.py
"""Tests for sync orchestrator types and constants."""

from __future__ import annotations

import pytest

from app.services.sync_orchestrator.types import (
    ExecutionPlan,
    LayerOutcome,
    LayerPlan,
    PREREQ_SKIP_MARKER,
    RefreshResult,
    SyncAlreadyRunning,
    SyncScope,
    SyncTrigger,
    prereq_skip_reason,
)


class TestLayerOutcome:
    def test_enum_values(self) -> None:
        assert LayerOutcome.SUCCESS.value == "success"
        assert LayerOutcome.NO_WORK.value == "no_work"
        assert LayerOutcome.PARTIAL.value == "partial"
        assert LayerOutcome.FAILED.value == "failed"
        assert LayerOutcome.DEP_SKIPPED.value == "dep_skipped"
        assert LayerOutcome.PREREQ_SKIP.value == "prereq_skip"


class TestPrereqSkipReason:
    def test_marker_prefix(self) -> None:
        reason = prereq_skip_reason("no provider configured")
        assert reason.startswith(PREREQ_SKIP_MARKER)
        assert "no provider configured" in reason

    def test_marker_constant(self) -> None:
        assert PREREQ_SKIP_MARKER == "prereq_missing:"


class TestSyncScope:
    def test_full(self) -> None:
        scope = SyncScope.full()
        assert scope.kind == "full"
        assert scope.detail is None
        assert scope.force is False

    def test_layer(self) -> None:
        scope = SyncScope.layer("candles")
        assert scope.kind == "layer"
        assert scope.detail == "candles"

    def test_job_forces_by_default(self) -> None:
        scope = SyncScope.job("daily_candle_refresh")
        assert scope.kind == "job"
        assert scope.detail == "daily_candle_refresh"
        assert scope.force is True

    def test_high_frequency(self) -> None:
        scope = SyncScope.high_frequency()
        assert scope.kind == "high_frequency"


class TestSyncAlreadyRunning:
    def test_carries_active_id(self) -> None:
        exc = SyncAlreadyRunning(SyncScope.full(), active_sync_run_id=42)
        assert exc.active_sync_run_id == 42
        assert "42" in str(exc)


class TestLayerPlanDepsDerivation:
    """Spec §2.6: external = emit_deps - set(emits), topo-sorted. No closure."""

    def test_single_layer_passes_through(self) -> None:
        plan = LayerPlan(
            name="daily_candle_refresh",
            emits=("candles",),
            reason="stale",
            dependencies=("universe",),
            is_blocking=True,
            estimated_items=50,
        )
        assert plan.dependencies == ("universe",)

    def test_composite_plan_accepts_explicit_dependency_tuple(self) -> None:
        # Pure dataclass shape test — _build_layer_plan's derivation
        # logic is tested in test_sync_orchestrator_planner (Task 5).
        plan = LayerPlan(
            name="morning_candidate_review",
            emits=("scoring", "recommendations"),
            reason="stale",
            dependencies=("thesis", "candles"),
            is_blocking=True,
            estimated_items=0,
        )
        assert plan.emits == ("scoring", "recommendations")
        assert plan.dependencies == ("thesis", "candles")
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_sync_orchestrator_types.py -v`
Expected: all fail (module not found).

- [ ] **Step 3: Implement types**

```python
# app/services/sync_orchestrator/types.py
"""Type and constant definitions for the sync orchestrator.

Pure data — no I/O, no DB, no dependencies on other orchestrator modules.
Importable from planner, executor, adapters without cycles.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import timedelta
from enum import StrEnum
from typing import Literal, Protocol

# ---------------------------------------------------------------------------
# Outcome enumeration (spec §2.3)
# ---------------------------------------------------------------------------


class LayerOutcome(StrEnum):
    """What happened to a layer during this sync run."""

    SUCCESS = "success"            # ran, did work, wrote rows
    NO_WORK = "no_work"            # ran successfully, nothing to do
    PARTIAL = "partial"            # ran, some items succeeded and some failed
    FAILED = "failed"              # refresh aborted; blocking for dependents
    DEP_SKIPPED = "dep_skipped"    # never ran; upstream blocking layer failed
    PREREQ_SKIP = "prereq_skip"    # never produced useful work; prerequisite missing


# ---------------------------------------------------------------------------
# PREREQ_SKIP marker (spec §1.3)
# ---------------------------------------------------------------------------


PREREQ_SKIP_MARKER = "prereq_missing:"


def prereq_skip_reason(detail: str) -> str:
    """Return the canonical `record_job_skip(reason=...)` string for a
    PREREQ_SKIP outcome. fresh_by_audit counts job_runs skipped rows ONLY
    when their error_msg starts with PREREQ_SKIP_MARKER — legacy skip
    reasons without the marker do not count."""
    return f"{PREREQ_SKIP_MARKER} {detail}"


# ---------------------------------------------------------------------------
# RefreshResult + LayerRefresh protocol (spec §2.3)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class RefreshResult:
    """Outcome of one layer's refresh call."""

    outcome: LayerOutcome
    row_count: int
    items_processed: int
    items_total: int | None
    detail: str
    error_category: str | None = None


class ProgressCallback(Protocol):
    def __call__(
        self, items_done: int, items_total: int | None = None
    ) -> None: ...


class LayerRefresh(Protocol):
    """Canonical adapter signature — one declaration, used by both
    single-layer and composite adapters.

    Single-layer adapters return [(layer_name, result)] — one element.
    Composite adapters return one element per emitted layer, in declared
    emit order. See spec §2.3 + §2.3.1."""

    def __call__(
        self,
        *,
        sync_run_id: int,
        progress: ProgressCallback,
        upstream_outcomes: Mapping[str, LayerOutcome],
    ) -> Sequence[tuple[str, RefreshResult]]: ...


# ---------------------------------------------------------------------------
# LayerPlan + ExecutionPlan (spec §2.6)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class LayerPlan:
    name: str                         # legacy job name (matches _INVOKERS key)
    emits: tuple[str, ...]            # layer names this plan produces
    reason: str                       # human-readable staleness reason
    dependencies: tuple[str, ...]     # derived per §2.6 external-only rule
    is_blocking: bool
    estimated_items: int


@dataclass(frozen=True)
class LayerSkip:
    name: str
    reason: str  # "already fresh"


@dataclass(frozen=True)
class ExecutionPlan:
    layers_to_refresh: tuple[LayerPlan, ...]
    layers_skipped: tuple[LayerSkip, ...]
    estimated_duration: timedelta | None


# ---------------------------------------------------------------------------
# SyncScope, SyncTrigger, SyncResult (spec §2.1)
# ---------------------------------------------------------------------------


SyncTrigger = Literal["manual", "scheduled", "catch_up"]


@dataclass(frozen=True)
class SyncScope:
    kind: Literal["full", "layer", "high_frequency", "job"]
    detail: str | None = None
    force: bool = False

    @classmethod
    def full(cls) -> "SyncScope":
        return cls(kind="full")

    @classmethod
    def layer(cls, name: str) -> "SyncScope":
        return cls(kind="layer", detail=name)

    @classmethod
    def job(cls, legacy_job_name: str, force: bool = True) -> "SyncScope":
        return cls(kind="job", detail=legacy_job_name, force=force)

    @classmethod
    def high_frequency(cls) -> "SyncScope":
        return cls(kind="high_frequency")


@dataclass(frozen=True)
class SyncResult:
    sync_run_id: int
    outcomes: Mapping[str, LayerOutcome]


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class SyncAlreadyRunning(RuntimeError):
    """Raised when the partial unique index gate denies a new sync.

    The HTTP layer maps this to 409 Conflict with the active sync_run_id
    in the body so the client can poll it."""

    def __init__(
        self, scope: SyncScope, active_sync_run_id: int | None = None
    ) -> None:
        super().__init__(
            f"sync already running (scope={scope.kind}, active_id={active_sync_run_id})"
        )
        self.scope = scope
        self.active_sync_run_id = active_sync_run_id
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_sync_orchestrator_types.py -v`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git commit -m "feat(#TBD): sync orchestrator types, constants, exceptions"
```

---

### Task 3: Freshness predicates (15 layers)

**Files:**

- Create: `app/services/sync_orchestrator/freshness.py`
- Create: `tests/test_sync_orchestrator_freshness.py`

Each layer has a `*_is_fresh(conn) -> (bool, str)` predicate. All use `job_runs` as primary watermark per spec §1.3. Counting rows = status='success' OR (status='skipped' AND error_msg LIKE 'prereq_missing:%').

- [ ] **Step 1: Write tests (one class per layer)**

Write one `TestXxxIsFresh` class per layer. Test both fresh and stale branches. Mock `conn.execute` + `fetchone` returns. Example pattern:

```python
# tests/test_sync_orchestrator_freshness.py excerpt
class TestUniverseIsFresh:
    def test_fresh_when_job_runs_success_within_24h(self) -> None:
        from datetime import datetime, timezone, timedelta
        from app.services.sync_orchestrator.freshness import universe_is_fresh

        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
        conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        cursor.fetchone.return_value = (
            datetime.now(timezone.utc) - timedelta(hours=2),  # started_at
            "success",
            None,
        )
        fresh, detail = universe_is_fresh(conn)
        assert fresh is True

    def test_stale_when_last_run_older_than_24h(self) -> None:
        # ... older than window
        ...

    def test_fresh_when_last_run_prereq_skip(self) -> None:
        # status='skipped' AND error_msg LIKE 'prereq_missing:%' → counts
        ...

    def test_stale_when_last_run_failure(self) -> None:
        # Even if an earlier success exists, a newer failure = stale.
        ...

    def test_stale_when_no_runs_ever(self) -> None:
        ...

    def test_newer_failure_invalidates_older_success(self) -> None:
        """Regression: predicate must query latest row first, not filter
        for counting rows before ordering. A success at t-12h followed by
        a failure at t-2h must read as stale."""
        # cursor.fetchone → (t-2h, 'failure', 'boom')
        # assert fresh is False
        ...
```

Write equivalent classes for: `cik_mapping`, `candles`, `financial_facts`, `financial_normalization`, `fundamentals`, `news`, `thesis`, `scoring`, `recommendations`, `portfolio_sync`, `fx_rates`, `cost_models`, `weekly_reports`, `monthly_reports`.

**Content-check branches required** (beyond the audit-row check):

- `candles` — stale when any T1/T2 instrument lacks a row with `price_date >= most_recent_trading_day(today)` (per §1.3 and the implementation pseudocode above).
- `fundamentals` — stale when any tradable instrument lacks a `fundamentals_snapshot` row with `as_of_date >= current_quarter_start` (per §1.3).
- `thesis` — stale when `find_stale_instruments(conn, tier=1)` returns non-empty (per §1.3).
- `scoring` — stale when `MAX(scores.scored_at) WHERE model_version = <default>` is older than the latest `theses.created_at` OR older than the latest `price_daily.price_date` (per §1.3: "newer than latest thesis write and latest candle write").
- `recommendations` — per spec §1.3 bullet: fresh iff `MAX(trade_recommendations.created_at) > MAX(scores.scored_at) WHERE model_version = <default>` OR latest successful `job_runs` for `morning_candidate_review` within 24h. Implementer writes `TestRecommendationsIsFresh` covering both branches: test_fresh_by_newer_recommendation_vs_score, test_fresh_by_recent_morning_review, test_stale_when_scores_newer_than_recs_and_morning_review_old.

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_sync_orchestrator_freshness.py -v`
Expected: all fail.

- [ ] **Step 3: Implement freshness predicates**

Structure:

```python
# app/services/sync_orchestrator/freshness.py
"""Per-layer freshness predicates.

All predicates return (fresh: bool, detail: str). Predicates are pure
reads from the planning connection — no writes, no I/O outside SELECTs.

Source of truth per spec §1.3:
  fresh_by_audit   = latest counting job_runs row within freshness window
  fresh_by_content = per-layer content check (optional)
  layer fresh iff (fresh_by_audit AND fresh_by_content)

Counting rows = status='success' OR (status='skipped' AND
  error_msg LIKE 'prereq_missing:%').
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import psycopg

from app.services.sync_orchestrator.types import PREREQ_SKIP_MARKER


def _fresh_by_audit(
    conn: psycopg.Connection[Any],
    job_name: str,
    window: timedelta,
) -> tuple[bool, str]:
    """Return (True, "last sync Xh ago") if the LATEST job_runs row for
    `job_name` is a counting row (status='success' OR status='skipped' with
    PREREQ_SKIP_MARKER) within `window`.

    **Critical:** the latest row must be a counting row — a newer failure
    invalidates an older success. Do NOT filter by counting-status before
    ORDER BY, or a stale success will hide a recent failure."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT started_at, status, error_msg
            FROM job_runs
            WHERE job_name = %(job_name)s
            ORDER BY started_at DESC
            LIMIT 1
            """,
            {"job_name": job_name},
        )
        row = cur.fetchone()
    if row is None:
        return False, f"no job_runs row for {job_name}"
    started_at, status, error_msg = row
    # Check status counts (matches §1.3 rule: success OR PREREQ_SKIP-marked skipped)
    is_counting = (
        status == "success"
        or (status == "skipped" and error_msg and error_msg.startswith(PREREQ_SKIP_MARKER))
    )
    if not is_counting:
        return False, f"latest {job_name} has status={status}, not a counting row"
    age = datetime.now(timezone.utc) - started_at
    if age > window:
        return False, f"last {job_name} {_format_age(age)} ago (window {_format_age(window)})"
    return True, f"last {job_name} {_format_age(age)} ago"


def _format_age(delta: timedelta) -> str:
    # "2h 14m", "3d 4h", etc.
    total_seconds = int(delta.total_seconds())
    days, rem = divmod(total_seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, _ = divmod(rem, 60)
    if days > 0:
        return f"{days}d {hours}h"
    if hours > 0:
        return f"{hours}h {minutes}m"
    return f"{minutes}m"


# ---------------------------------------------------------------------------
# Per-layer predicates (one function per layer)
# ---------------------------------------------------------------------------


def universe_is_fresh(conn: psycopg.Connection[Any]) -> tuple[bool, str]:
    return _fresh_by_audit(conn, "nightly_universe_sync", timedelta(hours=24))


def cik_mapping_is_fresh(conn: psycopg.Connection[Any]) -> tuple[bool, str]:
    return _fresh_by_audit(conn, "daily_cik_refresh", timedelta(hours=24))


def candles_is_fresh(conn: psycopg.Connection[Any]) -> tuple[bool, str]:
    audit_fresh, audit_detail = _fresh_by_audit(
        conn, "daily_candle_refresh", timedelta(hours=24)
    )
    if not audit_fresh:
        return False, audit_detail
    # Per-instrument check: each T1/T2 instrument's latest price_date must
    # equal or exceed most_recent_trading_day(today). Using the GLOBAL
    # MAX(price_date) would false-pass when the whole table is stale.
    from datetime import date
    from app.services.market_data import _most_recent_trading_day  # line 149
    trading_day = _most_recent_trading_day(date.today())
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*) AS missing
            FROM instruments i
            JOIN coverage c USING (instrument_id)
            WHERE c.coverage_tier IN (1, 2)
              AND COALESCE(
                  (SELECT MAX(price_date) FROM price_daily p
                   WHERE p.instrument_id = i.instrument_id),
                  DATE '1900-01-01'
              ) < %(trading_day)s
            """,
            {"trading_day": trading_day},
        )
        (missing,) = cur.fetchone() or (0,)
    if missing > 0:
        return False, f"{missing} T1/T2 instruments missing candle for {trading_day.isoformat()}"
    return True, audit_detail


# ... and so on for all 15 layers. Pattern is consistent; the implementer
# fills in per-layer content checks where the spec §1.3 bullet calls for them
# (fundamentals uses fundamentals_snapshot.as_of_date >= current_quarter_start;
# thesis uses find_stale_instruments; scoring uses MAX(scores.scored_at) vs
# latest thesis and latest candle; etc.)
```

Implementer completes all 15 layer predicates. Each matches the §1.3 bullet exactly.

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_sync_orchestrator_freshness.py -v`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git commit -m "feat(#TBD): freshness predicates for 15 layers (job_runs-based)"
```

---

### Task 4: LAYERS and JOB_TO_LAYERS registries

**Files:**

- Create: `app/services/sync_orchestrator/registry.py`
- Create: `tests/test_sync_orchestrator_registry.py`

Registers all 15 `DataLayer` entries with their freshness predicate + (stub) refresh callable + dependencies + is_blocking. Also `JOB_TO_LAYERS` mapping verified against `_INVOKERS` keys. Refresh callables are populated in Task 10 (adapters); here we use `NotImplementedError` stubs so the registry structure is testable first.

- [ ] **Step 1: Write failing tests**

```python
# tests/test_sync_orchestrator_registry.py
class TestLayerRegistry:
    def test_all_15_layers_present(self) -> None:
        from app.services.sync_orchestrator.registry import LAYERS
        expected = {
            "universe", "cik_mapping", "candles", "financial_facts",
            "financial_normalization", "fundamentals", "news", "thesis",
            "scoring", "recommendations", "portfolio_sync", "fx_rates",
            "cost_models", "weekly_reports", "monthly_reports",
        }
        assert set(LAYERS.keys()) == expected

    def test_blocking_defaults_per_spec(self) -> None:
        from app.services.sync_orchestrator.registry import LAYERS
        non_blocking = {"news", "portfolio_sync", "fx_rates", "weekly_reports", "monthly_reports"}
        for name, layer in LAYERS.items():
            expected_blocking = name not in non_blocking
            assert layer.is_blocking == expected_blocking, name

    def test_every_dep_is_a_known_layer(self) -> None:
        from app.services.sync_orchestrator.registry import LAYERS
        for name, layer in LAYERS.items():
            for dep in layer.dependencies:
                assert dep in LAYERS, f"{name} depends on unknown {dep}"


class TestJobToLayers:
    def test_every_key_is_a_real_invoker(self) -> None:
        from app.jobs.runtime import _INVOKERS
        from app.services.sync_orchestrator.registry import JOB_TO_LAYERS
        for job_name in JOB_TO_LAYERS:
            assert job_name in _INVOKERS, job_name

    def test_every_emitted_layer_is_in_registry(self) -> None:
        from app.services.sync_orchestrator.registry import LAYERS, JOB_TO_LAYERS
        for job_name, emits in JOB_TO_LAYERS.items():
            for layer in emits:
                assert layer in LAYERS, f"{job_name} emits unknown layer {layer}"

    def test_expected_mappings(self) -> None:
        from app.services.sync_orchestrator.registry import JOB_TO_LAYERS
        assert JOB_TO_LAYERS["daily_financial_facts"] == ("financial_facts", "financial_normalization")
        assert JOB_TO_LAYERS["morning_candidate_review"] == ("scoring", "recommendations")
        assert JOB_TO_LAYERS["weekly_report"] == ("weekly_reports",)
        assert JOB_TO_LAYERS["monthly_report"] == ("monthly_reports",)
        # Outside-DAG entries should be empty tuples
        assert JOB_TO_LAYERS["execute_approved_orders"] == ()
        assert JOB_TO_LAYERS["monitor_positions"] == ()
        assert JOB_TO_LAYERS["retry_deferred_recommendations"] == ()
        assert JOB_TO_LAYERS["weekly_coverage_review"] == ()
        assert JOB_TO_LAYERS["attribution_summary"] == ()
        assert JOB_TO_LAYERS["daily_tax_reconciliation"] == ()
```

- [ ] **Step 2: Implement registry**

```python
# app/services/sync_orchestrator/registry.py
"""LAYERS and JOB_TO_LAYERS registries.

Adapter functions are imported from .adapters (Task 10); the registry
here declares the DAG structure. is_blocking, dependencies, and
display_name all come from the spec §1.1 + §2.4.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import psycopg

from app.services.sync_orchestrator.freshness import (
    candles_is_fresh, cik_mapping_is_fresh, cost_models_is_fresh,
    financial_facts_is_fresh, financial_normalization_is_fresh,
    fundamentals_is_fresh, fx_rates_is_fresh, monthly_reports_is_fresh,
    news_is_fresh, portfolio_sync_is_fresh, recommendations_is_fresh,
    scoring_is_fresh, thesis_is_fresh, universe_is_fresh,
    weekly_reports_is_fresh,
)
from app.services.sync_orchestrator.types import LayerRefresh


@dataclass(frozen=True)
class DataLayer:
    name: str
    display_name: str
    tier: int
    is_fresh: Callable[[psycopg.Connection[Any]], tuple[bool, str]]
    refresh: LayerRefresh
    dependencies: tuple[str, ...]
    is_blocking: bool = True
    cadence: str = "daily"


# Temporary stub — real adapters wired in Task 10.
def _not_implemented_adapter(**kwargs: Any) -> Any:
    raise NotImplementedError("adapter wired in Task 10")


LAYERS: dict[str, DataLayer] = {
    "universe": DataLayer(
        name="universe",
        display_name="Tradable Universe",
        tier=0,
        is_fresh=universe_is_fresh,
        refresh=_not_implemented_adapter,
        dependencies=(),
    ),
    "cik_mapping": DataLayer(
        name="cik_mapping",
        display_name="SEC CIK Mapping",
        tier=0,
        is_fresh=cik_mapping_is_fresh,
        refresh=_not_implemented_adapter,
        dependencies=("universe",),
    ),
    "candles": DataLayer(
        name="candles",
        display_name="Daily Price Candles",
        tier=1,
        is_fresh=candles_is_fresh,
        refresh=_not_implemented_adapter,
        dependencies=("universe",),
    ),
    "financial_facts": DataLayer(
        name="financial_facts",
        display_name="SEC EDGAR XBRL Facts",
        tier=1,
        is_fresh=financial_facts_is_fresh,
        refresh=_not_implemented_adapter,
        dependencies=("cik_mapping",),
    ),
    "financial_normalization": DataLayer(
        name="financial_normalization",
        display_name="Financial Period Normalization",
        tier=2,
        is_fresh=financial_normalization_is_fresh,
        refresh=_not_implemented_adapter,
        dependencies=("financial_facts",),
    ),
    "fundamentals": DataLayer(
        name="fundamentals",
        display_name="Fundamentals Snapshot",
        tier=1,
        is_fresh=fundamentals_is_fresh,
        refresh=_not_implemented_adapter,
        dependencies=("universe",),
        cadence="quarterly",
    ),
    "news": DataLayer(
        name="news",
        display_name="News & Sentiment",
        tier=1,
        is_fresh=news_is_fresh,
        refresh=_not_implemented_adapter,
        dependencies=("universe",),
        is_blocking=False,
        cadence="4h",
    ),
    "thesis": DataLayer(
        name="thesis",
        display_name="Investment Thesis",
        tier=2,
        is_fresh=thesis_is_fresh,
        refresh=_not_implemented_adapter,
        dependencies=("fundamentals", "financial_normalization", "news"),
    ),
    "scoring": DataLayer(
        name="scoring",
        display_name="Ranking Scores",
        tier=3,
        is_fresh=scoring_is_fresh,
        refresh=_not_implemented_adapter,
        dependencies=("thesis", "candles"),
    ),
    "recommendations": DataLayer(
        name="recommendations",
        display_name="Trade Recommendations",
        tier=3,
        is_fresh=recommendations_is_fresh,
        refresh=_not_implemented_adapter,
        dependencies=("scoring",),
    ),
    "portfolio_sync": DataLayer(
        name="portfolio_sync",
        display_name="Portfolio Sync",
        tier=0,
        is_fresh=portfolio_sync_is_fresh,
        refresh=_not_implemented_adapter,
        dependencies=(),
        is_blocking=False,
        cadence="5m",
    ),
    "fx_rates": DataLayer(
        name="fx_rates",
        display_name="FX Rates",
        tier=0,
        is_fresh=fx_rates_is_fresh,
        refresh=_not_implemented_adapter,
        dependencies=(),
        is_blocking=False,
        cadence="5m",
    ),
    "cost_models": DataLayer(
        name="cost_models",
        display_name="Transaction Cost Models",
        tier=2,
        is_fresh=cost_models_is_fresh,
        refresh=_not_implemented_adapter,
        dependencies=("universe",),
    ),
    "weekly_reports": DataLayer(
        name="weekly_reports",
        display_name="Weekly Performance Report",
        tier=3,
        is_fresh=weekly_reports_is_fresh,
        refresh=_not_implemented_adapter,
        dependencies=(),
        is_blocking=False,
        cadence="weekly",
    ),
    "monthly_reports": DataLayer(
        name="monthly_reports",
        display_name="Monthly Performance Report",
        tier=3,
        is_fresh=monthly_reports_is_fresh,
        refresh=_not_implemented_adapter,
        dependencies=(),
        is_blocking=False,
        cadence="monthly",
    ),
}


JOB_TO_LAYERS: dict[str, tuple[str, ...]] = {
    # In-DAG (13 entries, non-empty tuples):
    "nightly_universe_sync":          ("universe",),
    "daily_cik_refresh":              ("cik_mapping",),
    "daily_candle_refresh":           ("candles",),
    "daily_financial_facts":          ("financial_facts", "financial_normalization"),
    "daily_research_refresh":         ("fundamentals",),
    "daily_news_refresh":             ("news",),
    "daily_thesis_refresh":           ("thesis",),
    "daily_portfolio_sync":           ("portfolio_sync",),
    "morning_candidate_review":       ("scoring", "recommendations"),
    "seed_cost_models":               ("cost_models",),
    "weekly_report":                  ("weekly_reports",),
    "monthly_report":                 ("monthly_reports",),
    "fx_rates_refresh":               ("fx_rates",),
    # Outside-DAG (6 entries, empty tuples):
    "execute_approved_orders":        (),
    "weekly_coverage_review":         (),
    "retry_deferred_recommendations": (),
    "monitor_positions":              (),
    "attribution_summary":            (),
    "daily_tax_reconciliation":       (),
}
```

- [ ] **Step 3: Run tests**

Run: `uv run pytest tests/test_sync_orchestrator_registry.py -v`
Expected: all pass.

- [ ] **Step 4: Commit**

```bash
git commit -m "feat(#TBD): LAYERS + JOB_TO_LAYERS registries (adapter stubs)"
```

---

### Task 5: Planner — `build_execution_plan` + dep derivation

**Files:**

- Create: `app/services/sync_orchestrator/planner.py`
- Create: `tests/test_sync_orchestrator_planner.py`

Builds `ExecutionPlan` from `SyncScope`. Applies freshness filtering (unless `force=True`), derives `LayerPlan.dependencies` per the external-only rule, produces topologically-sorted `layers_to_refresh`.

- [ ] **Step 1: Write failing tests**

Test cases:

1. `test_full_scope_with_all_fresh` → `layers_to_refresh == ()`
2. `test_full_scope_with_all_stale` → all 15 layers included
3. `test_layer_scope_includes_only_layer_and_stale_deps` → `SyncScope.layer("candles")` when candles stale and universe fresh → just candles
4. `test_layer_scope_deps_stale` → candles + universe both stale → both included, topo order
5. `test_high_frequency_scope_includes_only_portfolio_sync_and_fx_rates`
6. `test_job_scope_force_true_includes_composite_emit_set_even_when_fresh` — `SyncScope.job("daily_financial_facts")` when facts + normalization fresh → still planned (force)
7. `test_composite_plan_dependencies_drops_intra_emits` — `set(morning_candidate_review.dependencies) == {"thesis", "candles"}`; same-depth ordering is implementation-defined, test on set.
8. `test_composite_plan_dependencies_drops_intra_emits_facts` — `daily_financial_facts.dependencies == ("cik_mapping",)` (single element, order trivial).
9. `test_topo_order_respects_dag` — universe before candles before scoring in `layers_to_refresh`

**`force=True` scope semantics (spec §2.1 JOB scope):** force-run applies **only** to the target job, not its dependencies. Example: `SyncScope.job("daily_candle_refresh", force=True)` includes `daily_candle_refresh` in `layers_to_refresh` even if candles is fresh, but `universe` (a dependency) is included only if universe itself is stale. Test cases:

- `test_job_scope_force_runs_target_when_fresh` — target's emits are fresh; target still planned.
- `test_job_scope_does_not_force_fresh_dependency` — target stale but universe fresh; only target planned, universe omitted.
- `test_job_scope_includes_stale_dependency` — target stale and universe stale; both planned, topo order.

- [ ] **Step 2: Implement planner**

Structure:

```python
# app/services/sync_orchestrator/planner.py
from __future__ import annotations

from typing import Any

import psycopg

from app.services.sync_orchestrator.registry import LAYERS, JOB_TO_LAYERS
from app.services.sync_orchestrator.types import (
    ExecutionPlan,
    LayerPlan,
    LayerSkip,
    SyncScope,
)


def build_execution_plan(
    conn: psycopg.Connection[Any],
    scope: SyncScope,
) -> ExecutionPlan:
    """Build the plan for a sync run. See spec §2.6."""
    # 1. Determine target layers based on scope.
    candidates = _scope_to_candidate_jobs(scope)  # returns list of legacy_job_name

    # 2. For each candidate job, decide inclusion. force=True applies ONLY
    #    to the scope's target job (scope.detail for JOB scope); dependencies
    #    are always evaluated on freshness, never force-run. Spec §2.1.
    target_job = scope.detail if scope.kind == "job" else None
    layers_to_refresh: list[LayerPlan] = []
    layers_skipped: list[LayerSkip] = []
    for job_name in candidates:
        emits = JOB_TO_LAYERS[job_name]
        if not emits:  # outside-DAG — should not appear in candidates
            continue
        is_target = (job_name == target_job)
        if is_target and scope.force:
            include = True
            reason = f"forced by scope={scope.kind}"
        else:
            fresh, reason = _all_emits_fresh(conn, emits)
            include = not fresh
        if include:
            layers_to_refresh.append(_build_layer_plan(job_name, emits, reason))
        else:
            for emit in emits:
                layers_skipped.append(LayerSkip(name=emit, reason=f"fresh: {reason}"))

    # 3. Topologically sort layers_to_refresh by emit-DAG order.
    layers_to_refresh = _topo_sort(layers_to_refresh)

    return ExecutionPlan(
        layers_to_refresh=tuple(layers_to_refresh),
        layers_skipped=tuple(layers_skipped),
        estimated_duration=None,  # Phase 2 enhancement
    )


def _build_layer_plan(
    job_name: str, emits: tuple[str, ...], reason: str
) -> LayerPlan:
    """Derive LayerPlan.dependencies per §2.6 external-only rule."""
    emit_set = set(emits)
    emit_deps: set[str] = set()
    for emit in emits:
        emit_deps.update(LAYERS[emit].dependencies)
    external_deps = emit_deps - emit_set

    # Blocking = any emit is blocking. A composite that produces at least
    # one blocking emit must block dependents on failure, so use `any`.
    # For all current composites both emits share is_blocking=True, so
    # all() and any() agree in practice; using any() matches the English
    # spec semantics in §2.4.
    is_blocking = any(LAYERS[emit].is_blocking for emit in emits)

    return LayerPlan(
        name=job_name,
        emits=emits,
        reason=reason,
        # Topologically sort via the LAYERS DAG, NOT alphabetically. For
        # morning_candidate_review, external={thesis, candles}; `thesis`
        # has dep (fundamentals, financial_normalization, news) and
        # `candles` has dep (universe,), so neither depends on the other
        # — both are at the same topo depth and either order is valid.
        # For determinism we sort by topo depth (descending: shallower
        # first) then by layer name within the same depth. This gives
        # (thesis, candles) for morning_candidate_review since thesis
        # is deeper in the DAG — reversed to put the deepest dep first.
        # Implementer: use a stable topo sort keyed on layer depth.
        dependencies=_topo_sort_by_depth(external_deps),
        is_blocking=is_blocking,
        estimated_items=0,  # Phase 2: query historical items_total
    )


# _scope_to_candidate_jobs, _all_emits_fresh, _topo_sort — standard helpers
```

- [ ] **Step 3: Run tests**

Run: `uv run pytest tests/test_sync_orchestrator_planner.py -v`
Expected: all pass.

- [ ] **Step 4: Commit**

```bash
git commit -m "feat(#TBD): build_execution_plan + composite dep derivation"
```

---

### Task 6: Executor — `_run_layers_loop`, `_safe_run_and_finalize`, audit writers

**Files:**

- Create: `app/services/sync_orchestrator/executor.py`
- Create: `tests/test_sync_orchestrator_executor.py`

Implements the exact pseudocode from spec §2.2. Every helper that writes to the DB opens its own `autocommit=True` connection and uses `with conn.transaction()` so commits are real.

- [ ] **Step 1: Write failing tests**

Test cases (split into focused classes):

- `TestStartSyncRun`: inserts sync_runs row + all sync_layer_progress rows; returns (id, plan); raises `SyncAlreadyRunning(scope, active_sync_run_id=N)` on duplicate running row (constructor takes `scope` first — see Task 2 types).
- `TestRunLayersLoop`: happy path (all SUCCESS); composite happy path (two emits both SUCCESS); blocking dep failure → downstream DEP_SKIPPED; adapter raises → FAILED for all emits; adapter returns mismatched emit set → FAILED with contract-violation marker; adapter returns empty list → FAILED; adapter returns PARTIAL for one emit → downstream still runs (PARTIAL doesn't block); PREREQ_SKIP on blocking → downstream DEP_SKIPPED.
- `TestSafeRunAndFinalize`: preserves committed outcomes when loop crashes; calls `_fail_unfinished_layers` in finally; calls `_finalize_sync_run`; swallows finalize crashes and logs.
- `TestBuildUpstreamOutcomes`: planned deps use in-memory outcomes; unplanned deps resolved from `job_runs` via `_last_counting_outcome_from_job_runs`.
- `TestBlockingDependencyFailed`: FAILED and DEP_SKIPPED on blocking dep → skip reason; PREREQ_SKIP on blocking dep → skip reason; PARTIAL on blocking dep → does NOT skip; any outcome on non-blocking dep → does NOT skip.
- `TestFinalizeSyncRun`: reads sync_layer_progress as authoritative; picks terminal status (complete/partial/failed); logs drift when in-memory outcomes disagree with DB.

- [ ] **Step 2: Implement executor**

Key helpers (follow spec §2.2 pseudocode verbatim):

- `_start_sync_run` — wraps planning + insert in `with conn.transaction():`; catches UniqueViolation, re-queries active id, raises `SyncAlreadyRunning(scope, active_sync_run_id=...)` (positional scope first per Task 2 types).
- `_run_layers_loop(sync_run_id, plan, outcomes)` — mutates outcomes in place; calls `_build_upstream_outcomes` then `_blocking_dependency_failed` (resolved map); validates adapter return (`set(names) == set(emits)`, no duplicates); records per-emit progress rows.
- `_safe_run_and_finalize(sync_run_id, plan)` — try/finally wrapping `_run_layers_loop` + `_fail_unfinished_layers` + `_finalize_sync_run`. Outcomes dict shared by reference.
- `_invoke_layer_refresh(layer_plan, sync_run_id, upstream_outcomes)` — looks up `LAYERS[layer_plan.emits[0]].refresh` and calls it with `progress=_make_progress_callback(sync_run_id, layer_plan.emits)`.
- `_make_progress_callback` — returns callback that opens short-lived autocommit connection, updates `items_done` on `sync_layer_progress`, closes. Debounced (every N items or 10s).
- `_record_layer_started/_result/_failed/_skipped` — each opens its own `autocommit=True` connection, uses `with conn.transaction()`, updates `sync_layer_progress` row for (sync_run_id, layer_name).
- `_fail_unfinished_layers(sync_run_id)` — UPDATE any `status IN ('pending', 'running')` rows to `status='failed'` with `error_category='orchestrator_crash'`; returns {name: LayerOutcome.FAILED} dict.
- `_finalize_sync_run(sync_run_id, outcomes)` — reads `sync_layer_progress` as authoritative, computes counts, sets terminal status + finished_at; logs drift if memory `outcomes` disagrees with DB.
- `_last_counting_outcome_from_job_runs(layer_name)` — resolves via `JOB_TO_LAYERS` reverse lookup to find the job_name for the layer, reads `job_runs`, maps to LayerOutcome.

- [ ] **Step 3: Run tests**

Run: `uv run pytest tests/test_sync_orchestrator_executor.py -v`
Expected: all pass.

- [ ] **Step 4: Commit**

```bash
git commit -m "feat(#TBD): executor + crash guard + audit writers"
```

---

### Task 7: Public entry points — `run_sync`, `submit_sync`

**Files:**

- Modify: `app/services/sync_orchestrator/executor.py` (add `run_sync`, `submit_sync` at bottom)
- Modify: `app/services/sync_orchestrator/__init__.py` (re-export)
- Create: `tests/test_sync_orchestrator_entrypoints.py`

Wires the two public entry points inside `executor.py` alongside the private helpers they call (`_start_sync_run`, `_safe_run_and_finalize`). `submit_sync` submits to `JobRuntime._manual_executor` — but that's an **instance attribute**, not module-level importable. The orchestrator gets a reference to the live `JobRuntime` via a setter called from `app/main.py` at startup. In test contexts, tests inject their own `ThreadPoolExecutor` via the same setter.

- [ ] **Step 1: Write failing tests**

- `TestRunSync`: calls `_start_sync_run` then `_safe_run_and_finalize` synchronously; returns `SyncResult`.
- `TestSubmitSync`: calls `_start_sync_run` synchronously; submits `_safe_run_and_finalize` to executor; returns `(sync_run_id, plan)` before loop runs. Assert the sync_runs row + pending progress rows are durable before return.
- `TestSubmitSyncSyncAlreadyRunning`: raises `SyncAlreadyRunning` synchronously when gate denies.

- [ ] **Step 2: Implement entry points and executor setter**

Add to `app/services/sync_orchestrator/executor.py`:

```python
from typing import Protocol

class _ExecutorLike(Protocol):
    def submit(self, fn, *args, **kwargs): ...

# Module-global set by app/main.py or tests. Raises if submit_sync is
# called before the setter has run — enforces "register executor at app
# boot" discipline.
_executor_ref: _ExecutorLike | None = None


def set_executor(executor: _ExecutorLike) -> None:
    """Called once at app startup to register the worker pool. In prod,
    pass `job_runtime._manual_executor`. In tests, pass a ThreadPoolExecutor
    (or a synchronous-inline stub)."""
    global _executor_ref
    _executor_ref = executor


def run_sync(scope: SyncScope, trigger: SyncTrigger) -> SyncResult:
    """Synchronous entry: plan + execute + finalize in caller's thread."""
    sync_run_id, plan = _start_sync_run(scope, trigger)
    outcomes = _safe_run_and_finalize(sync_run_id, plan)
    return SyncResult(sync_run_id=sync_run_id, outcomes=outcomes)


def submit_sync(
    scope: SyncScope, trigger: SyncTrigger
) -> tuple[int, ExecutionPlan]:
    """Async entry: plan + submit to worker; return before layers run."""
    if _executor_ref is None:
        raise RuntimeError(
            "sync orchestrator executor not set — app startup must call "
            "set_executor(job_runtime._manual_executor) before submit_sync"
        )
    sync_run_id, plan = _start_sync_run(scope, trigger)
    _executor_ref.submit(_safe_run_and_finalize, sync_run_id, plan)
    return sync_run_id, plan
```

Add to `app/services/sync_orchestrator/__init__.py`:

```python
from app.services.sync_orchestrator.types import (
    LayerOutcome, RefreshResult, LayerPlan, ExecutionPlan,
    LayerRefresh, SyncScope, SyncTrigger, SyncResult,
    SyncAlreadyRunning, PREREQ_SKIP_MARKER, prereq_skip_reason,
)
from app.services.sync_orchestrator.registry import LAYERS, JOB_TO_LAYERS, DataLayer
from app.services.sync_orchestrator.planner import build_execution_plan
from app.services.sync_orchestrator.executor import (
    run_sync, submit_sync, set_executor,
)

__all__ = [
    "LayerOutcome", "RefreshResult", "LayerPlan", "ExecutionPlan",
    "LayerRefresh", "SyncScope", "SyncTrigger", "SyncResult",
    "SyncAlreadyRunning", "PREREQ_SKIP_MARKER", "prereq_skip_reason",
    "LAYERS", "JOB_TO_LAYERS", "DataLayer",
    "build_execution_plan", "run_sync", "submit_sync", "set_executor",
]
```

- [ ] **Step 3: Run tests**

Run: `uv run pytest tests/test_sync_orchestrator_entrypoints.py -v`
Expected: all pass.

- [ ] **Step 4: Commit**

```bash
git commit -m "feat(#TBD): public entry points run_sync + submit_sync"
```

---

### Task 8: Close early-return holes in scheduler.py

**Files:**

- Modify: `app/workers/scheduler.py`
- Create: `tests/test_scheduler_early_return_holes.py`

Rewrites the 10 paths called out in spec §2.3 + §8.1. Each becomes either `raise` (for real errors that should propagate) or `record_job_skip(reason=prereq_skip_reason(...))` + `return` (for PREREQ_SKIP cases).

**Specific lines to rewrite** (verify file:line before editing — these drift):

1. [scheduler.py:652-653](app/workers/scheduler.py#L652) `nightly_universe_sync` creds None → `record_job_skip + return` with `prereq_skip_reason("etoro credentials missing")`.
2. [scheduler.py:750-751](app/workers/scheduler.py#L750) `daily_candle_refresh` creds None → same pattern.
3. [scheduler.py:1082](app/workers/scheduler.py#L1082) `daily_news_refresh` Anthropic key missing → `record_job_skip + return` with `prereq_skip_reason("anthropic api key missing")`. (This one IS already outside `_tracked_job`; simple rewrite.)
4. [scheduler.py:1105](app/workers/scheduler.py#L1105) `daily_news_refresh` no provider wired → **restructure required**: the current no-provider block sits INSIDE `_tracked_job` which would cause a naive `record_job_skip + return` to write two job_runs rows (one skipped + one success-from-tracked-job). Fix: move the provider-availability guard BEFORE the `_tracked_job` context (before line 1083's `with _tracked_job(...)` entry), mirroring the credential-check pattern. Pattern:

```python
def daily_news_refresh() -> None:
    if settings.anthropic_api_key is None:
        with psycopg.connect(settings.database_url, autocommit=True) as conn:
            record_job_skip(conn, JOB_DAILY_NEWS_REFRESH,
                            prereq_skip_reason("anthropic api key missing"))
        return
    if _news_provider() is None:  # new helper checking wiring
        with psycopg.connect(settings.database_url, autocommit=True) as conn:
            record_job_skip(conn, JOB_DAILY_NEWS_REFRESH,
                            prereq_skip_reason("news provider not configured"))
        return
    with _tracked_job(JOB_DAILY_NEWS_REFRESH) as tracker:
        # ... rest unchanged
```

Regression test: `test_news_refresh_no_provider_writes_exactly_one_skipped_job_runs_row`.
5. [scheduler.py:1137](app/workers/scheduler.py#L1137) `daily_thesis_refresh` Anthropic key missing return → `prereq_skip_reason("anthropic api key missing")` (also outside `_tracked_job`).
6. [scheduler.py:1152](app/workers/scheduler.py#L1152) and [scheduler.py:1157](app/workers/scheduler.py#L1157) `daily_thesis_refresh` query failure / no-stale-instruments silent returns → review each: one is legitimate "no work" (`NO_WORK` outcome via normal return with `tracker.row_count=0`), the other is a query failure that should **raise**. Grep the surrounding context to distinguish. Write separate tests for each case.
7. [scheduler.py:1208](app/workers/scheduler.py#L1208) `daily_portfolio_sync` creds None → `prereq_skip_reason("etoro credentials missing")`.
8. [scheduler.py:1268](app/workers/scheduler.py#L1268) `morning_candidate_review` scoring **exception-catch** silent return → **raise**. (This is the `except Exception: return` after `compute_rankings`.)
9. [scheduler.py:1292](app/workers/scheduler.py#L1292) `morning_candidate_review` portfolio-review **exception-catch** silent return → **raise**. (This is the `except Exception: return` after portfolio review, NOT line 1273 — that one is the legitimate `if not score_result.scored: return` no-work path which stays as-is so Task 9's `compute_morning_recommendations` can return `review_result=None`.)
10. [scheduler.py:1329](app/workers/scheduler.py#L1329) `morning_candidate_review` `execute_approved_orders()` side-effect call → **extract** per Task 9 (larger refactor).

`_tracked_job` already records failure on raise; these changes turn silent-success-on-failure into honest `status='failure'` rows.

**Important:** `record_job_skip` requires autocommit per [ops_monitor.py:322](app/services/ops_monitor.py#L322). The existing scheduler early-returns happen BEFORE entering `_tracked_job`, so they need to open their own autocommit connection to call `record_job_skip`. Pattern:

```python
def nightly_universe_sync() -> None:
    creds = _load_etoro_credentials("nightly_universe_sync")
    if creds is None:
        with psycopg.connect(settings.database_url, autocommit=True) as conn:
            record_job_skip(conn, JOB_NIGHTLY_UNIVERSE_SYNC,
                            prereq_skip_reason("etoro credentials missing"))
        return
    # ... rest unchanged
```

- [ ] **Step 1: Write regression tests**

Per hole, write one test that confirms the new behaviour:

```python
class TestNightlyUniverseSyncCredsMissing:
    def test_writes_prereq_skip_job_run(self, monkeypatch) -> None:
        """creds None → one job_runs row with status='skipped' and
        error_msg starting with PREREQ_SKIP_MARKER."""
        monkeypatch.setattr("app.workers.scheduler._load_etoro_credentials",
                            lambda _: None)
        # ... spy on record_job_skip
        ...
```

Do this for all 10 holes.

- [ ] **Step 2: Implement rewrites**

Sequentially edit each scheduler.py line range. Re-run `rg 'creds is None: return'` etc. after edits to verify no instances remain.

- [ ] **Step 3: Run tests**

Run: `uv run pytest tests/test_scheduler_early_return_holes.py -v`
Expected: all pass. Also: `uv run pytest tests/test_scheduler*.py -v` to catch regressions.

- [ ] **Step 4: Commit**

```bash
git commit -m "fix(#TBD): close 10 early-return holes in scheduler.py (raise or PREREQ_SKIP)"
```

---

### Task 9: Extract `compute_morning_recommendations` from `morning_candidate_review`

**Files:**

- Modify: `app/workers/scheduler.py`
- Create: `tests/test_compute_morning_recommendations.py` (or add to existing test file)

Preferred option from spec §2.3.1: split out the scoring+recommendations logic so the orchestrator adapter can call it **without** `execute_approved_orders()` side effect. Legacy `morning_candidate_review` job body becomes `compute_morning_recommendations()` followed by the existing `execute_approved_orders()` call — preserving Phase 1–3 scheduled behaviour.

- [ ] **Step 1: Identify the split point**

Read `morning_candidate_review` body. The `execute_approved_orders()` call is at [scheduler.py:1329](app/workers/scheduler.py#L1329). Everything above it is scoring + recommendations; below it is the trigger decision.

- [ ] **Step 2: Write failing test**

```python
class TestComputeMorningRecommendations:
    def test_returns_score_and_rec_summaries_without_executing_orders(self) -> None:
        """Orchestrator path: runs scoring + recommendations, returns summary,
        never calls execute_approved_orders."""
        # Spy on execute_approved_orders
        ...

    def test_morning_candidate_review_still_calls_execute_after(self) -> None:
        """Legacy scheduled path: unchanged behaviour, still triggers execution."""
        ...
```

- [ ] **Step 3: Extract function**

```python
from app.services.scoring import RankingResult  # existing at scoring.py:160
from app.services.portfolio import PortfolioReviewResult  # existing at portfolio.py:101


@dataclass(frozen=True)
class MorningComputeResult:
    ranking_result: RankingResult
    review_result: PortfolioReviewResult | None  # None when scoring returned no eligible instruments


def compute_morning_recommendations() -> MorningComputeResult:
    """Run scoring + recommendations. Does NOT call execute_approved_orders.

    Used by the sync orchestrator's morning_candidate_review adapter, which
    must not trigger order execution as a side effect of a data refresh.
    The legacy `morning_candidate_review` scheduled job retains its execute
    trigger during Phase 1–3; Phase 4 removes that scheduled path.

    Opens TWO separate psycopg.connect() blocks — one per phase — so a
    recommendation failure cannot roll back the completed scoring run.
    Matches the existing discipline at [scheduler.py:1259](app/workers/scheduler.py#L1259).

    Preserves the no-score path from the legacy body: if scoring produces
    an empty `scored` list, portfolio review does NOT run and
    `review_result` is None. The adapter surfaces this as NO_WORK for the
    recommendations layer; fresh_by_audit still counts the layer because
    the job_runs row is SUCCESS (scoring ran correctly).
    """
    # Move body from morning_candidate_review (scheduler.py:1256+) to here,
    # stopping BEFORE the execute_approved_orders() call. Two connect blocks:
    # one for compute_rankings, one for portfolio review. Return None for
    # review_result when score_result.scored is empty.
    ...
    return MorningComputeResult(ranking_result=..., review_result=...)


def morning_candidate_review() -> None:
    with _tracked_job(JOB_MORNING_CANDIDATE_REVIEW) as tracker:
        try:
            result = compute_morning_recommendations()
        except Exception:
            # compute_morning_recommendations logs + raises. _tracked_job
            # records the failure. No partial cleanup required because
            # each phase owns its own connection.
            raise
        tracker.row_count = (
            len(result.ranking_result.scored)
            + (len(result.review_result.recommendations)
               if result.review_result is not None else 0)
        )
        # Preserve existing execute trigger logic from scheduler.py:1313+
        # (kill switch check, enable_auto_trading check, then
        # execute_approved_orders() if green). Skip when review_result is
        # None (no eligible instruments; nothing to execute). Copy
        # verbatim with that one added guard.
        if result.review_result is None:
            return
        ...
```

- [ ] **Step 4: Run tests**

Run: `uv run pytest tests/test_compute_morning_recommendations.py tests/test_scheduler*.py -v`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git commit -m "refactor(#TBD): extract compute_morning_recommendations for orchestrator use"
```

---

### Task 10: Layer adapters (13 adapters)

**Files:**

- Create: `app/services/sync_orchestrator/adapters.py`
- Modify: `app/services/sync_orchestrator/registry.py` (replace `_not_implemented_adapter` refs with real adapters)
- Create: `tests/test_sync_orchestrator_adapters.py`

One adapter per in-DAG `JOB_TO_LAYERS` entry. Each follows the §2.3 contract exactly: opens own connection, returns `Sequence[tuple[str, RefreshResult]]` (1 or N elements), writes `job_runs` row via `_tracked_job` or explicit pair, raises on internal failure.

**Critical invariant — read outcome inside the JobLock context.** Every adapter must call `_latest_job_outcome(job_name)` **before** exiting the `with JobLock(...)` block. A concurrent legacy cron fire can acquire the advisory lock the instant our block exits and write a newer `job_runs` row; reading outcome after exit would attribute the wrong outcome. Templates below demonstrate the pattern (single-layer, composite, morning). Every adapter must match.

**Adapter template** (single-layer) — uses `JobLock` wrapper so concurrent legacy cron fires in Phase 1–3 serialize with orchestrator-triggered runs:

```python
from app.jobs.locks import JobAlreadyRunning, JobLock
from app.config import settings


def refresh_universe(
    *,
    sync_run_id: int,
    progress: ProgressCallback,
    upstream_outcomes: Mapping[str, LayerOutcome],
) -> Sequence[tuple[str, RefreshResult]]:
    """Adapter for universe layer → calls nightly_universe_sync logic.

    Wraps the legacy job call in `JobLock` to serialize with any
    still-scheduled cron fire of the same job during Phase 1–3 (spec §8.1).
    Contention → PREREQ_SKIP with prereq_skip_reason marker so the
    fresh_by_audit predicate counts the skip and the next sync retries
    naturally."""
    from app.workers.scheduler import nightly_universe_sync

    outcome: LayerOutcome
    row_count: int
    try:
        with JobLock(settings.database_url, "nightly_universe_sync"):
            try:
                nightly_universe_sync()  # writes job_runs row via _tracked_job
            except Exception:
                # _tracked_job recorded failure; re-raise so the orchestrator
                # records FAILED for this emit.
                raise
            # Read outcome INSIDE the lock so a concurrent legacy-cron fire
            # cannot race a newer job_runs row in between our run and our read.
            outcome, row_count = _latest_job_outcome("nightly_universe_sync")
    except JobAlreadyRunning:
        # Another holder (legacy cron fire) has the advisory lock. Record a
        # PREREQ_SKIP row so fresh_by_audit still counts this layer as
        # ran-to-its-prerequisite-check, not stale-forever.
        with psycopg.connect(settings.database_url, autocommit=True) as conn:
            record_job_skip(
                conn,
                "nightly_universe_sync",
                prereq_skip_reason("legacy cron holder active"),
            )
        return [("universe", RefreshResult(
            outcome=LayerOutcome.PREREQ_SKIP,
            row_count=0,
            items_processed=0,
            items_total=None,
            detail="legacy cron holder active (JobLock busy)",
            error_category=None,
        ))]

    # Lock released. outcome + row_count already captured inside the lock.
    return [("universe", RefreshResult(
        outcome=outcome,
        row_count=row_count,
        items_processed=row_count,
        items_total=None,
        detail=f"universe refresh: {outcome.value}",
        error_category=None,
    ))]
```

**Composite adapter template** (`daily_financial_facts`) — same `JobLock` wrapping:

```python
def refresh_financial_facts_and_normalization(
    *, sync_run_id, progress, upstream_outcomes,
) -> Sequence[tuple[str, RefreshResult]]:
    """Composite adapter: emits (financial_facts, financial_normalization).
    Atomic per §2.3.1: either both emit rows succeed or both fail/PREREQ_SKIP."""
    from app.workers.scheduler import daily_financial_facts

    outcome: LayerOutcome
    row_count: int
    try:
        with JobLock(settings.database_url, "daily_financial_facts"):
            daily_financial_facts()
            outcome, row_count = _latest_job_outcome("daily_financial_facts")
    except JobAlreadyRunning:
        with psycopg.connect(settings.database_url, autocommit=True) as conn:
            record_job_skip(
                conn, "daily_financial_facts",
                prereq_skip_reason("legacy cron holder active"),
            )
        skip = RefreshResult(
            outcome=LayerOutcome.PREREQ_SKIP, row_count=0, items_processed=0,
            items_total=None, detail="legacy cron holder active",
            error_category=None,
        )
        return [("financial_facts", skip), ("financial_normalization", skip)]

    # outcome + row_count captured inside the lock; emit two layer rows
    # both reporting the same outcome (atomic composite per §2.3.1).
    return [
        ("financial_facts", RefreshResult(
            outcome=outcome, row_count=row_count, items_processed=row_count,
            items_total=None, detail="xbrl fetch", error_category=None,
        )),
        ("financial_normalization", RefreshResult(
            outcome=outcome, row_count=0, items_processed=0,
            items_total=None, detail="normalization pass", error_category=None,
        )),
    ]
```

**Morning composite adapter** (uses `compute_morning_recommendations` from Task 9 to avoid executing orders). Uses the existing [`_tracked_job`](app/workers/scheduler.py) helper so the audit-write pattern matches every other legacy job exactly (single code path, single test of the pattern):

```python
def refresh_scoring_and_recommendations(
    *, sync_run_id, progress, upstream_outcomes,
) -> Sequence[tuple[str, RefreshResult]]:
    """Composite adapter: emits (scoring, recommendations). Does NOT call
    execute_approved_orders — that side effect stays on the legacy
    scheduled trigger only (spec §2.3.1)."""
    from app.workers.scheduler import (
        _tracked_job,
        JOB_MORNING_CANDIDATE_REVIEW,
        MorningComputeResult,           # extracted in Task 9
        compute_morning_recommendations,  # extracted in Task 9
    )

    result: MorningComputeResult
    outcome: LayerOutcome
    try:
        with JobLock(settings.database_url, JOB_MORNING_CANDIDATE_REVIEW):
            # _tracked_job opens its own connection, writes start+finish
            # rows, re-raises on body exception. Same audit path as every
            # other legacy job — no new DB-write pattern invented here.
            #
            # CRITICAL: compute_morning_recommendations must preserve the
            # existing scheduler.py pattern of ONE CONNECTION PER PHASE so
            # recommendation failure cannot roll back completed scoring
            # (see scheduler.py:1259 "Each phase opens its own connection
            # so a failure in recommendations does not roll back the
            # completed scoring run"). The extracted function in Task 9
            # owns that discipline; the adapter just calls it.
            with _tracked_job(JOB_MORNING_CANDIDATE_REVIEW) as tracker:
                result = compute_morning_recommendations()  # no conn arg — owns its own
                tracker.row_count = (
                    len(result.ranking_result.scored)
                    + (len(result.review_result.recommendations)
                       if result.review_result is not None else 0)
                )
            # Read outcome inside the lock to avoid race with legacy cron.
            outcome, _ = _latest_job_outcome(JOB_MORNING_CANDIDATE_REVIEW)
    except JobAlreadyRunning:
        with psycopg.connect(settings.database_url, autocommit=True) as conn:
            record_job_skip(
                conn, JOB_MORNING_CANDIDATE_REVIEW,
                prereq_skip_reason("legacy cron holder active"),
            )
        skip = RefreshResult(
            outcome=LayerOutcome.PREREQ_SKIP, row_count=0, items_processed=0,
            items_total=None, detail="legacy cron holder active",
            error_category=None,
        )
        return [("scoring", skip), ("recommendations", skip)]

    # outcome captured inside the lock above. Per-layer item counts
    # come from `result` returned by compute_morning_recommendations.
    # The no-score path (empty scoring.scored → review_result is None)
    # yields NO_WORK for recommendations.
    scoring_count = len(result.ranking_result.scored)
    if result.review_result is None:
        rec_outcome = LayerOutcome.NO_WORK
        rec_count = 0
    else:
        rec_outcome = outcome
        rec_count = len(result.review_result.recommendations)
    return [
        ("scoring", RefreshResult(
            outcome=outcome,
            row_count=scoring_count,
            items_processed=scoring_count,
            items_total=None, detail="scoring pass", error_category=None,
        )),
        ("recommendations", RefreshResult(
            outcome=rec_outcome,
            row_count=rec_count,
            items_processed=rec_count,
            items_total=None,
            detail="recommendations pass" if rec_count
                   else "no eligible instruments to score",
            error_category=None,
        )),
    ]
```

**Note on types:** the extracted `compute_morning_recommendations` returns a `MorningComputeResult` dataclass (defined in Task 9) with two fields: `ranking_result: RankingResult` (from [app/services/scoring.py:160](app/services/scoring.py#L160)) and `review_result: PortfolioReviewResult | None` (from [app/services/portfolio.py:101](app/services/portfolio.py#L101); **nullable** — None when scoring produced no eligible instruments, preserving the legacy no-score path). These are the **actual existing type names** — the earlier draft of this plan mis-named them `ScoreRunResult` / `RecommendationResult`. Implementer grep-verifies before writing Task 9.

- [ ] **Step 1: Write per-adapter tests**

One class per adapter. Each tests: (a) successful refresh returns correct emit tuples; (b) underlying job raises → adapter re-raises; (c) PREREQ_SKIP outcome detected when the job called `record_job_skip`; (d) composite adapters return both emits with same outcome.

- [ ] **Step 2: Implement 13 adapters**

Each adapter is a thin wrapper. Implementer follows the templates above. The `_latest_job_outcome` helper reads `job_runs` to determine what the wrapped job did:

```python
def _latest_job_outcome(job_name: str) -> tuple[LayerOutcome, int]:
    """Read the most recent job_runs row and convert to LayerOutcome."""
    with psycopg.connect(settings.database_url) as conn:
        row = conn.execute(
            """
            SELECT status, row_count, error_msg
            FROM job_runs
            WHERE job_name = %s
            ORDER BY started_at DESC
            LIMIT 1
            """,
            (job_name,),
        ).fetchone()
    if row is None:
        return LayerOutcome.FAILED, 0
    status, row_count, error_msg = row
    if status == "success":
        return (LayerOutcome.SUCCESS if row_count else LayerOutcome.NO_WORK), row_count or 0
    if status == "skipped" and error_msg and error_msg.startswith(PREREQ_SKIP_MARKER):
        return LayerOutcome.PREREQ_SKIP, 0
    return LayerOutcome.FAILED, row_count or 0
```

- [ ] **Step 3: Replace stubs in registry**

In `registry.py`, swap each `refresh=_not_implemented_adapter` for the real adapter import.

- [ ] **Step 4: Run tests**

Run: `uv run pytest tests/test_sync_orchestrator_adapters.py -v`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git commit -m "feat(#TBD): 13 layer adapters + _latest_job_outcome helper"
```

---

### Task 11: Boot-time orphan reaper + feature flag + API stubs

**Files:**

- Create: `app/services/sync_orchestrator/reaper.py`
- Create: `tests/test_sync_orchestrator_reaper.py`
- Modify: `app/config.py`
- Create: `app/api/sync.py`
- Modify: `app/main.py`

Reaper runs at app startup (before scheduler start) to transition stale `running` rows to `failed` with `error_category='orchestrator_crash'`. Feature flag `ORCHESTRATOR_ENABLED` gates `POST /sync`. GET endpoints work regardless (useful for inspection).

- [ ] **Step 1: Write reaper AND router tests**

Route tests must exist BEFORE implementation per TDD. Full test classes required for Task 11:

- `TestOrphanedSyncReaper` (below)
- `TestPostSyncDisabled` — `ORCHESTRATOR_ENABLED=false` → 503.
- `TestPostSyncEnabled` — flag on, full scope → 202 + `sync_run_id` + plan JSON; layer scope missing `layer` field → 422; job scope missing `job` field → 422.
- `TestPostSyncConflict` — second POST while one is running → 409 with body `{"error": "sync_already_running", "sync_run_id": <active_id>}` (matches spec §4.4 response shape).
- `TestGetSyncStatus` — returns running sync or `{"is_running": false}`; schema matches §4.3 `GET /sync/status` example.
- `TestGetSyncLayers` — returns 15 layers; each entry has `name`, `display_name`, `tier`, `is_fresh`, `freshness_detail`, `dependencies`, `is_blocking`.
- `TestGetSyncRunsLimitBounds` — `limit=0` → 422, `limit=101` → 422, `limit=50` → 200.
- `TestSyncRoutesAuthRequired` — unauthenticated request → 401/403 (matches /jobs auth behaviour).

```python
class TestOrphanedSyncReaper:
    def test_transitions_stale_running_to_failed(self, db_conn) -> None:
        # Insert a sync_runs row with started_at > 1h ago, status='running'
        # Run reaper
        # Assert row is now status='failed', error_category='orchestrator_crash',
        # finished_at set
        ...

    def test_does_not_touch_recent_running_rows(self, db_conn) -> None:
        # Insert row started 30m ago
        # Run reaper
        # Assert row unchanged
        ...

    def test_does_not_touch_already_terminal_rows(self, db_conn) -> None:
        # Insert row with status='complete' and old started_at
        # Run reaper
        # Assert row unchanged
        ...

    def test_recomputes_aggregate_counts_from_progress_rows(self, db_conn) -> None:
        """After reaping a stale 'running' sync, sync_runs.layers_done/
        layers_failed/layers_skipped must reflect the actual
        sync_layer_progress row statuses — not the stale values from
        when the run crashed."""
        # Insert sync_runs row with status='running', started_at = now-2h,
        # layers_planned=5, layers_done=0, layers_failed=0, layers_skipped=0.
        # Insert 5 sync_layer_progress rows with mixed statuses:
        #   2x 'complete', 1x 'failed', 1x 'skipped', 1x 'running'.
        # Run reaper.
        # Assert sync_runs row now has:
        #   layers_done=2, layers_failed=2 (1 failed + 1 running→failed),
        #   layers_skipped=1. Running row also flipped to failed.
        ...
```

- [ ] **Step 2: Implement reaper**

```python
# app/services/sync_orchestrator/reaper.py
def reap_orphaned_syncs(timeout: timedelta = timedelta(hours=1)) -> int:
    """Transition sync_runs rows stuck in 'running' longer than timeout
    to 'failed' with error_category='orchestrator_crash'. Returns count.

    Called at app startup before the scheduler begins. Uses its own
    autocommit connection."""
    with psycopg.connect(settings.database_url, autocommit=True) as conn:
        with conn.transaction():
            # Step 1: reap stale running syncs and their pending/running
            # progress rows in a single statement. Returns the reaped ids.
            reaped_rows = conn.execute(
                """
                WITH reaped AS (
                    UPDATE sync_runs
                    SET status = 'failed',
                        finished_at = now(),
                        error_category = 'orchestrator_crash'
                    WHERE status = 'running'
                      AND started_at < now() - %s::interval
                    RETURNING sync_run_id
                ),
                _progress_cleanup AS (
                    UPDATE sync_layer_progress slp
                    SET status = 'failed',
                        finished_at = now(),
                        error_category = 'orchestrator_crash'
                    FROM reaped r
                    WHERE slp.sync_run_id = r.sync_run_id
                      AND slp.status IN ('pending', 'running')
                    RETURNING 1
                )
                SELECT sync_run_id FROM reaped
                """,
                (timeout,),
            ).fetchall()
            reaped_ids = [r[0] for r in reaped_rows]

            # Step 2: recompute aggregate counts on each reaped sync_runs row
            # from its sync_layer_progress rows so GET /sync/runs shows truthful
            # layers_done/failed/skipped for crash-reaped runs.
            if reaped_ids:
                conn.execute(
                    """
                    UPDATE sync_runs sr
                    SET layers_done    = agg.done,
                        layers_failed  = agg.failed,
                        layers_skipped = agg.skipped
                    FROM (
                        SELECT sync_run_id,
                               COUNT(*) FILTER (WHERE status IN ('complete','partial')) AS done,
                               COUNT(*) FILTER (WHERE status = 'failed') AS failed,
                               COUNT(*) FILTER (WHERE status = 'skipped') AS skipped
                        FROM sync_layer_progress
                        WHERE sync_run_id = ANY(%s)
                        GROUP BY sync_run_id
                    ) agg
                    WHERE sr.sync_run_id = agg.sync_run_id
                    """,
                    (reaped_ids,),
                )
    return len(reaped_ids)
```

- [ ] **Step 3: Add config flag**

```python
# app/config.py
orchestrator_enabled: bool = False
```

- [ ] **Step 4: Implement API stubs**

```python
# app/api/sync.py
from typing import Any, Literal
from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel

from app.api.auth import require_session_or_service_token  # same dep used by /jobs/*
from app.config import settings
from app.db import get_conn
from app.services.sync_orchestrator import (
    ExecutionPlan, SyncAlreadyRunning, SyncScope, submit_sync,
)

router = APIRouter(
    prefix="/sync",
    tags=["sync"],
    dependencies=[Depends(require_session_or_service_token)],  # match /jobs auth
)


class SyncRequest(BaseModel):
    """Request body for POST /sync."""
    scope: Literal["full", "layer", "high_frequency", "job"] = "full"
    layer: str | None = None    # required when scope='layer'
    job: str | None = None      # required when scope='job'


def _scope_from(body: SyncRequest) -> SyncScope:
    """Map API request to internal SyncScope. Validates that scope-specific
    fields are present (e.g. layer name when scope='layer')."""
    if body.scope == "full":
        return SyncScope.full()
    if body.scope == "high_frequency":
        return SyncScope.high_frequency()
    if body.scope == "layer":
        if not body.layer:
            raise HTTPException(422, detail="layer required when scope='layer'")
        return SyncScope.layer(body.layer)
    if body.scope == "job":
        if not body.job:
            raise HTTPException(422, detail="job required when scope='job'")
        return SyncScope.job(body.job, force=True)
    raise HTTPException(422, detail=f"unknown scope {body.scope!r}")


def _plan_to_json(plan: ExecutionPlan) -> dict[str, Any]:
    """Serialize ExecutionPlan for JSON response."""
    return {
        "layers_to_refresh": [
            {
                "name": lp.name,
                "emits": list(lp.emits),
                "reason": lp.reason,
                "dependencies": list(lp.dependencies),
                "is_blocking": lp.is_blocking,
                "estimated_items": lp.estimated_items,
            }
            for lp in plan.layers_to_refresh
        ],
        "layers_skipped": [
            {"name": s.name, "reason": s.reason}
            for s in plan.layers_skipped
        ],
    }


@router.post("", status_code=status.HTTP_202_ACCEPTED)
def post_sync(body: SyncRequest):
    if not settings.orchestrator_enabled:
        raise HTTPException(503, detail="sync orchestrator disabled (Phase 1)")
    try:
        sync_run_id, plan = submit_sync(_scope_from(body), trigger="manual")
    except SyncAlreadyRunning as exc:
        # Must use JSONResponse (not HTTPException) to get TOP-LEVEL body
        # shape per spec §4.4. FastAPI's HTTPException wraps `detail` under
        # a "detail" key by default, which we don't want here.
        from fastapi.responses import JSONResponse
        return JSONResponse(
            status_code=409,
            content={
                "error": "sync_already_running",
                "sync_run_id": exc.active_sync_run_id,
            },
        )
    return {"sync_run_id": sync_run_id, "plan": _plan_to_json(plan)}


@router.get("/status")
def get_sync_status(conn = Depends(get_conn)) -> dict[str, Any]:
    """Return current running sync (if any) + its active layer row.

    Shape matches spec §4.3 example. When no sync is running, returns
    `{"is_running": false, "current_run": null, "active_layer": null}`."""
    row = conn.execute(
        """
        SELECT sync_run_id, scope, trigger, started_at,
               layers_planned, layers_done, layers_failed, layers_skipped
        FROM sync_runs
        WHERE status = 'running'
        ORDER BY started_at DESC
        LIMIT 1
        """,
    ).fetchone()
    if row is None:
        return {"is_running": False, "current_run": None, "active_layer": None}
    sync_run_id = row[0]
    active = conn.execute(
        """
        SELECT layer_name, started_at, items_total, items_done
        FROM sync_layer_progress
        WHERE sync_run_id = %s AND status = 'running'
        ORDER BY started_at DESC
        LIMIT 1
        """,
        (sync_run_id,),
    ).fetchone()
    return {
        "is_running": True,
        "current_run": {
            "sync_run_id": row[0], "scope": row[1], "trigger": row[2],
            "started_at": row[3].isoformat(),
            "layers_planned": row[4], "layers_done": row[5],
            "layers_failed": row[6], "layers_skipped": row[7],
        },
        "active_layer": None if active is None else {
            "name": active[0],
            "started_at": active[1].isoformat() if active[1] else None,
            "items_total": active[2], "items_done": active[3],
        },
    }


@router.get("/layers")
def get_sync_layers(conn = Depends(get_conn)) -> dict[str, Any]:
    """Return all 15 layers with current freshness status + last job_runs row.

    Schema matches spec §4.3 GET /sync/layers example — name, display_name,
    tier, is_fresh, freshness_detail, last_success_at, last_duration_seconds,
    last_error_category, consecutive_failures, dependencies."""
    from app.services.sync_orchestrator import LAYERS
    from app.services.sync_orchestrator.registry import JOB_TO_LAYERS

    # Reverse-lookup: layer_name → legacy job_name for job_runs reads.
    layer_to_job = {
        emit: job for job, emits in JOB_TO_LAYERS.items() for emit in emits
    }
    out = []
    for name, layer in LAYERS.items():
        fresh, detail = layer.is_fresh(conn)
        job_name = layer_to_job[name]
        # Two queries: last_success_at tracks only SUCCESS rows (never a
        # newer failure). consecutive_failures counts the latest run-of-
        # failures from most-recent backwards.
        last_success = conn.execute(
            """
            SELECT started_at, finished_at
            FROM job_runs
            WHERE job_name = %s AND status = 'success'
            ORDER BY started_at DESC
            LIMIT 1
            """,
            (job_name,),
        ).fetchone()
        out.append({
            "name": name,
            "display_name": layer.display_name,
            "tier": layer.tier,
            "is_fresh": fresh,
            "freshness_detail": detail,
            "last_success_at": (
                last_success[1].isoformat()
                if last_success and last_success[1] else None
            ),
            "last_duration_seconds": (
                int((last_success[1] - last_success[0]).total_seconds())
                if last_success and last_success[0] and last_success[1] else None
            ),
            "last_error_category": None,  # Phase 1 does not derive category
            "consecutive_failures": 0,  # Phase 2 enhancement
            "dependencies": list(layer.dependencies),
            "is_blocking": layer.is_blocking,
        })
    return {"layers": out}


@router.get("/runs")
def get_sync_runs(
    conn = Depends(get_conn),
    limit: int = Query(20, ge=1, le=100),
) -> dict[str, Any]:
    """Return recent sync runs, newest first (bounded limit per prevention-log rule)."""
    rows = conn.execute(
        """
        SELECT sync_run_id, scope, scope_detail, trigger, started_at,
               finished_at, status, layers_planned, layers_done,
               layers_failed, layers_skipped
        FROM sync_runs
        ORDER BY started_at DESC
        LIMIT %s
        """,
        (limit,),
    ).fetchall()
    return {"runs": [
        {
            "sync_run_id": r[0], "scope": r[1], "scope_detail": r[2],
            "trigger": r[3],
            "started_at": r[4].isoformat(),
            "finished_at": r[5].isoformat() if r[5] else None,
            "status": r[6],
            "layers_planned": r[7], "layers_done": r[8],
            "layers_failed": r[9], "layers_skipped": r[10],
        }
        for r in rows
    ]}
```

- [ ] **Step 5: Wire reaper + executor setter + router in main.py**

Two distinct edits in [app/main.py](app/main.py):

**1. Module-level router registration** alongside the existing `app.include_router(...)` block (around [main.py:119-127](app/main.py#L119)) — matches the existing project convention (routers are registered at module import, NOT inside lifespan):

```python
# app/main.py — TOP of file alongside other `from app.api.* import router as ..._router`
# imports (avoids Ruff E402 module-level-import-not-at-top):
from app.api.sync import router as sync_router

# ... later in the file, beside the other app.include_router(...) calls:
app.include_router(sync_router)
```

**2. Inside lifespan** — must match the existing `try/except` around `start_runtime()` at [main.py:94-104](app/main.py#L94) which tolerates runtime-start failure by setting `job_runtime = None`. Reap BEFORE `start_runtime()`; register executor AFTER, guarded for None:

```python
# app/main.py lifespan body — ADD lines marked [NEW]:
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    # [NEW] Reap orphaned sync_runs BEFORE runtime starts. Uses its own
    # short-lived connection; safe even if start_runtime() later fails.
    from app.services.sync_orchestrator.reaper import reap_orphaned_syncs
    try:
        reaped = reap_orphaned_syncs()
        logger.info("orchestrator reaper: transitioned %d orphaned rows", reaped)
    except Exception:
        logger.exception("orchestrator reaper failed — continuing startup")

    # existing block (main.py:94-104) — unchanged:
    job_runtime: JobRuntime | None
    try:
        job_runtime = start_runtime()
    except Exception:
        logger.exception("start_runtime failed — running without scheduler")
        job_runtime = None
    app.state.job_runtime = job_runtime

    # [NEW] Register executor only when runtime startup succeeded. If
    # job_runtime is None, submit_sync will raise RuntimeError on call —
    # that's correct because the orchestrator cannot dispatch without
    # the worker pool. POST /sync remains 503 via ORCHESTRATOR_ENABLED
    # flag in Phase 1 anyway.
    if job_runtime is not None:
        from app.services.sync_orchestrator import set_executor
        set_executor(job_runtime._manual_executor)

    yield

    # Existing shutdown order (main.py:107-115) unchanged — runtime down
    # before pool close so in-flight jobs can still write job_runs rows.
    shutdown_runtime(job_runtime)
    app.state.job_runtime = None
    pool.close()
    logger.info("Connection pool closed.")
```

- [ ] **Step 6: Run tests**

Run pre-push checks:

```bash
uv run pytest tests/test_sync_orchestrator*.py -v
uv run pytest tests/smoke/test_app_boots.py -v
```

Expected: all pass; smoke test exercises lifespan + reaper + router registration.

- [ ] **Step 7: Commit**

```bash
git commit -m "feat(#TBD): orphan reaper + ORCHESTRATOR_ENABLED flag + /sync/* endpoints (503 until Phase 4)"
```

---

### Task 12: Pre-push + self-review + PR

**Files:** All modified/created files.

- [ ] **Step 1: Run full pre-push gate**

```bash
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv run pytest
```

All four must pass.

- [ ] **Step 2: Re-read diff as an adversary**

`git diff main...HEAD`. Walk through:

- Schema: every NOT NULL has DEFAULT or is populated before INSERT.
- SQL: parameterized queries everywhere; no string interpolation.
- Transactions: every DB-write function uses `autocommit=True` + `with conn.transaction()`.
- Types: every `LayerRefresh` adapter matches `Sequence[tuple[str, RefreshResult]]`.
- Early-returns: no new silent-return-on-error paths introduced.
- Feature flag: `POST /sync` returns 503 when `ORCHESTRATOR_ENABLED=false`; GET endpoints work.
- No references to removed/invented identifiers. Grep for: `JobLock.trigger`, `JobLockBusy`, `score_run_id`, `sec_xbrl`. (Do NOT grep for `cost_models` — that is a legitimate layer name in this diff. Grep for the specific mistake pattern `"cost_models".*created_at` instead.)

- [ ] **Step 3: Verify dev stack boots and smoke-tests pass**

```bash
# /sync/* requires require_session_or_service_token like /jobs/*.
# Set TOKEN to a valid service token or cookie before running.
TOKEN="$(cat ~/.ebull-service-token)"  # or however tokens are stored locally

curl -sf http://localhost:8000/health && echo OK
curl -sf -H "Authorization: Bearer $TOKEN" http://localhost:8000/sync/layers | head -c 200
# POST is EXPECTED to return 503 while ORCHESTRATOR_ENABLED=false, so drop -f
# (which would treat 503 as a curl failure and suppress the body) and print
# the status code + response body explicitly.
curl -s -o /tmp/sync-post-body -w 'HTTP %{http_code}\n' \
     -H "Authorization: Bearer $TOKEN" -X POST http://localhost:8000/sync \
     -d '{"scope":"full"}' -H 'content-type: application/json'
cat /tmp/sync-post-body
# expect HTTP 503 with detail "sync orchestrator disabled (Phase 1)"
```

- [ ] **Step 4: Open PR**

```bash
git push -u origin feature/<N>-data-orchestrator-phase-1
gh pr create --title "feat: data orchestrator Phase 1 (gated behind ORCHESTRATOR_ENABLED flag)" --body "$(cat <<'EOF'
## Summary
- Migration 033: `sync_runs`, `sync_layer_progress`, `idx_sync_runs_single_running` partial unique index
- `app/services/sync_orchestrator/` module: types, constants, freshness predicates, planner, executor, 13 layer adapters, orphan reaper
- Closed 10 early-return holes in `app/workers/scheduler.py` — missing creds / missing API keys now write `status='skipped'` with `PREREQ_SKIP_MARKER`, silent failures now raise
- Extracted `compute_morning_recommendations` so orchestrator adapter runs scoring+recommendations WITHOUT triggering `execute_approved_orders`
- Added `POST /sync`, `GET /sync/status|layers|runs` endpoints (POST returns 503 until `ORCHESTRATOR_ENABLED=true` in Phase 4)
- Feature flag `ORCHESTRATOR_ENABLED=false` by default — zero behavioural change for operators

## Security model
- `POST /sync` gated behind `ORCHESTRATOR_ENABLED` feature flag.
- `idx_sync_runs_single_running` prevents concurrent syncs at DB level.
- `record_job_skip` requires autocommit — asserted in callers.
- `morning_candidate_review` adapter cannot trigger order execution; `execute_approved_orders` stays on separate scheduled trigger.
- Boot-time reaper transitions orphaned `running` rows to `failed` with `error_category='orchestrator_crash'`.

## Tradeoffs
- Adapters inspect `job_runs` AFTER the underlying job runs to determine outcome (vs capturing return value). Chosen because legacy jobs return None and writing `_tracked_job` output capture would be a separate refactor.
- `_last_counting_outcome_from_job_runs` opens a fresh connection per lookup during `_build_upstream_outcomes`. Acceptable for Phase 1; can be batched in Phase 2 if profiling warrants.
- `weekly_reports` + `monthly_reports` are separate layers that share no content; they have no dependencies and always run when stale, since neither depends on anything else.

## Test plan
- [ ] `uv run pytest tests/test_sync_orchestrator*.py -v` — all new tests pass
- [ ] `uv run pytest tests/test_scheduler_early_return_holes.py -v` — regression coverage for 10 rewrites
- [ ] `uv run pytest tests/smoke/test_app_boots.py -v` — reaper + router wire up cleanly in lifespan
- [ ] Manual: `curl -X POST /sync` returns 503; `curl /sync/layers` returns 15 layers with freshness
- [ ] Manual: insert a stale `status='running'` row, restart app, verify reaper transitions it

Phase 2-5 follow-ups: progress callbacks, observability UI, flag flip + cron removal. Tracked separately.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

- [ ] **Step 5: Poll review and CI**

Per `.claude/CLAUDE.md` branch-and-PR workflow: `gh pr view {n} --comments` and `gh pr checks {n}` until APPROVE on latest commit + CI green.

---

## Post-Phase-1 follow-ups (out of scope for this plan)

- **Phase 2:** Add `progress: ProgressCallback | None = None` param to long-running legacy jobs (`daily_candle_refresh`, `daily_financial_facts`, `daily_thesis_refresh`). Instrument `items_done` updates.
- **Phase 3:** Replace AdminPage jobs section with system health dashboard; auto-refresh; error drill-down.
- **Phase 4:** Flip `ORCHESTRATOR_ENABLED=true`; remove 12 cron schedules whose `JOB_TO_LAYERS` value is non-empty; add FULL + HIGH_FREQUENCY orchestrator triggers; rewrite tests on `SCHEDULED_JOBS` counts.
- **Phase 5:** Remove legacy `/system/jobs` UI tab; mark `POST /jobs/{name}/run` deprecated in OpenAPI.
