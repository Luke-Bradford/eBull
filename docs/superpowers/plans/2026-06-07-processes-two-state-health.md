# Processes Two-State Health Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the admin Processes page binary — 🟢 green (no action) vs 🔴 red (act) — by de-noising the existing health signals instead of deleting them, so a recent success clears the slate while a genuinely-behind / stalled / dead-engine state still turns red.

**Architecture:** Backend keeps `compute_verdict`'s 4-value vocabulary and precedence (the #1509/#1510 invariants stay verbatim). We change (a) *when* `schedule_missed` fires — only when overdue by a full cadence cycle, anchored on `finished_at`; (b) *what* `watermark_gap` means — a positive source-level "behind" predicate over `data_freshness_index.new_filings_since`; (c) the FE colour mapping — fold `working`/`self_healing` into one calm green; plus three correctness closers (orchestrator_full_sync overdue, never-started bound, operator-cancel benign, dead-engine banner).

**Tech Stack:** Python (FastAPI, psycopg3), pytest (pure table-tests, `-m "not db"` fast tier), React/TypeScript (Vitest), SQL migrations under `sql/`.

**Spec:** `docs/specs/ui/2026-06-07-process-health-two-state.md`. Umbrella #1508.

**Shipping:** May land as TWO PRs — PR-A backend (Tasks 1–6) and PR-B frontend (Tasks 7–8) — but the FE depends on PR-A's verdict behaviour, so sequence A→B. File one tracking issue under #1508 before PR-A.

---

## File Structure

**Backend (PR-A):**
- `app/services/processes/stale_detection.py` — MODIFY: `compute()` gains `cadence_period_s`; `schedule_missed` threshold `60s → max(cadence_period_s, FLOOR)`. Pure logic — the core change.
- `app/services/processes/scheduled_adapter.py` — MODIFY: anchor `expected_fire_at` on `max(started_at, finished_at)`; pass `cadence_period_s`; add `_source_watermark_behind()` + wire it into the `watermark_gap` input (replacing `_has_data_freshness_gap`); add `orchestrator_full_sync` to `_ORCHESTRATOR_SYNC_SCOPE`; compute `never_started`; compute `cancel_was_operator_initiated`.
- `app/services/processes/__init__.py` — MODIFY: add `never_started` + `cancel_was_operator_initiated` to `ProcessRow`.
- `app/services/processes/health_verdict.py` — MODIFY: `pending_first_run` + `never_started` → attention "never started"; operator-cancel `cancelled` → benign.
- `app/api/processes.py` — MODIFY: pass the two new fields through `_convert_row` into `compute_verdict`.
- `sql/NNN_job_first_seen.sql` — CREATE: persisted per-job first-seen anchor for C6.
- `app/workers/scheduler.py` (or jobs bootstrap) — MODIFY: upsert `job_first_seen` on registry load.

**Frontend (PR-B):**
- `frontend/src/components/admin/processStatus.ts` — MODIFY: `VERDICT_VISUAL` two-colour; `VERDICT_SORT_PRIORITY` working→current group.
- `frontend/src/components/admin/ProcessesTable.tsx` — MODIFY: `isCollapsible` includes `working`; pin only `attention`.
- Processes header (StaleBanner) — MODIFY: global red when jobs engine `down`.

---

## Task 1: C1 — `schedule_missed` fires only when overdue by a full cadence cycle, anchored on `finished_at`

**Files:**
- Modify: `app/services/processes/stale_detection.py` (`compute`, ~line 60-128; `SCHEDULE_MISS_TOLERANCE_S` line 49)
- Modify: `app/services/processes/scheduled_adapter.py:803-831` (anchor + pass cadence period)
- Test: `tests/test_process_stale_detection.py` (existing file for this module — confirm name; else create)

- [ ] **Step 1: Write failing tests for the new threshold + anchor**

```python
# tests/test_process_stale_detection.py
from datetime import UTC, datetime, timedelta
from app.services.processes.stale_detection import compute

_DAILY_S = 86_400

def _base(**kw):
    args = dict(
        mechanism="scheduled_job", status="ok", expected_fire_at=None,
        has_data_freshness_gap=False, has_dispatched_queue_age=False,
        last_progress_at=None, active_run_started_at=None,
        process_id="x", now=datetime(2026, 6, 7, 12, 0, tzinfo=UTC),
        cadence_period_s=_DAILY_S,
    )
    args.update(kw)
    return args

def test_within_one_cycle_is_not_schedule_missed():
    now = datetime(2026, 6, 7, 12, 0, tzinfo=UTC)
    # expected fire was 2h ago — well under a daily cycle → NOT missed
    assert "schedule_missed" not in compute(**_base(now=now, expected_fire_at=now - timedelta(hours=2)))

def test_overdue_by_more_than_a_cycle_is_schedule_missed():
    now = datetime(2026, 6, 7, 12, 0, tzinfo=UTC)
    # expected fire was 25h ago — past a full daily cycle → missed
    assert "schedule_missed" in compute(**_base(now=now, expected_fire_at=now - timedelta(hours=25)))

def test_floor_protects_every_5min_jobs():
    now = datetime(2026, 6, 7, 12, 0, tzinfo=UTC)
    # 5-min cadence, expected 3 min ago — under the FLOOR → not missed
    assert "schedule_missed" not in compute(**_base(now=now, cadence_period_s=300, expected_fire_at=now - timedelta(minutes=3)))
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest tests/test_process_stale_detection.py -k "schedule_missed or floor or within_one_cycle" -v`
Expected: FAIL — `compute()` got an unexpected keyword argument `cadence_period_s`.

- [ ] **Step 3: Add `cadence_period_s` + FLOOR to `compute()`**

In `stale_detection.py`, add near `SCHEDULE_MISS_TOLERANCE_S`:

```python
# C1 (#1508 two-state): schedule_missed fires only when overdue by a WHOLE
# cadence cycle, not a single late tick. FLOOR protects sub-cycle jobs
# (every-5-min) from flapping when their cadence is shorter than the floor.
SCHEDULE_MISS_FLOOR_S: Final[int] = 300
```

Change the `compute()` signature to add `cadence_period_s: int` (after `expected_fire_at`), and replace Rule 1:

```python
    # Rule 1: schedule_missed — overdue by more than a full cadence cycle
    # (C1). The adapter anchors expected_fire_at on the terminal run's
    # max(started_at, finished_at), so a run that just finished resets the
    # clock. A single late tick no longer fires; an entire skipped cycle does.
    overdue_threshold = max(cadence_period_s, SCHEDULE_MISS_FLOOR_S)
    if (
        mechanism == "scheduled_job"
        and status != "running"
        and expected_fire_at is not None
        and expected_fire_at < now - _seconds(overdue_threshold)
    ):
        reasons.append("schedule_missed")
```

(Leave `SCHEDULE_MISS_TOLERANCE_S` for any other reference; the new threshold supersedes its use in Rule 1.)

- [ ] **Step 4: Run the new tests to verify they pass**

Run: `uv run pytest tests/test_process_stale_detection.py -k "schedule_missed or floor or within_one_cycle" -v`
Expected: PASS.

- [ ] **Step 5: Anchor on `finished_at` + pass cadence in the adapter**

In `scheduled_adapter.py`, replace the `expected_fire_at` block (lines 803-805) and the `compute_stale_reasons(...)` call args:

```python
    expected_fire_at: datetime | None = None
    if terminal_row is not None and terminal_row.get("started_at") is not None:
        # C1: anchor on the LATER of started_at / finished_at so a run that
        # just completed resets the overdue clock (a long run that finishes
        # after a nominal slot must not read overdue the instant it ends).
        anchor = terminal_row["started_at"]
        finished = terminal_row.get("finished_at")
        if finished is not None and finished > anchor:
            anchor = finished
        expected_fire_at = compute_next_run(job.cadence, anchor)
```

Add `cadence_period_s=int(job.cadence.period.total_seconds())` to the `compute_stale_reasons(...)` call. (Confirm the cadence period accessor: grep `class Cadence` in `app/workers/scheduler.py`; if the interval is exposed differently, e.g. `job.cadence.interval_seconds`, use that. Add a one-line step to confirm before writing.)

- [ ] **Step 6: Update existing stale-detection callers/tests for the new required arg, run fast tier**

Run: `uv run pytest tests/test_process_stale_detection.py -q && uv run pytest -m "not db" -q`
Expected: PASS (fix any other `compute(` call site that now needs `cadence_period_s`).

- [ ] **Step 7: Commit**

```bash
git add app/services/processes/stale_detection.py app/services/processes/scheduled_adapter.py tests/test_process_stale_detection.py
git commit -m "feat(#1508): schedule_missed fires only when overdue by a full cadence cycle (C1)"
```

---

## Task 2: C2 — `watermark_gap` = positive source-level "behind" predicate

**Files:**
- Modify: `app/services/processes/scheduled_adapter.py` (add `_source_watermark_behind`; replace the `has_data_freshness_gap` input at lines 807-815)
- Test: `tests/test_scheduled_adapter_watermark.py` (db-tier — needs `data_freshness_index` rows)

- [ ] **Step 0: Confirm `new_filings_since` semantics**

Read the writer of `data_freshness_index.new_filings_since` (grep `new_filings_since` across `app/`). Confirm it is "count of upstream filings known-but-not-yet-ingested for this subject" and is reset to 0 after ingest. If it instead means "filings seen in the last poll" (not necessarily un-ingested), fall back to comparing `last_known_filed_at` (upstream) against the ingested watermark column the writer uses. Record the confirmed column in the test.

- [ ] **Step 1: Write failing db-tier test**

```python
# tests/test_scheduled_adapter_watermark.py
import pytest
from app.services.processes.scheduled_adapter import _source_watermark_behind

pytestmark = pytest.mark.db

def test_source_behind_when_uningested_filings_exist(ebull_test_conn):
    conn = ebull_test_conn
    conn.execute(
        "INSERT INTO data_freshness_index (subject_type, subject_id, source, new_filings_since) "
        "VALUES ('cik','0000320193','sec_form4', 3)"
    )
    assert _source_watermark_behind(conn, source="sec_form4") is True

def test_source_not_behind_when_all_ingested(ebull_test_conn):
    conn = ebull_test_conn
    conn.execute(
        "INSERT INTO data_freshness_index (subject_type, subject_id, source, new_filings_since) "
        "VALUES ('cik','0000320193','sec_form4', 0)"
    )
    assert _source_watermark_behind(conn, source="sec_form4") is False

def test_quiet_source_with_no_rows_is_not_behind(ebull_test_conn):
    assert _source_watermark_behind(ebull_test_conn, source="sec_form4") is False
```

- [ ] **Step 2: Run to verify failure**

Run: `docker compose --profile test up -d postgres-test && uv run pytest tests/test_scheduled_adapter_watermark.py -v`
Expected: FAIL — `_source_watermark_behind` not defined.

- [ ] **Step 3: Implement the predicate**

Add to `scheduled_adapter.py` next to `_source_watermark_fresh`:

```python
def _source_watermark_behind(conn: psycopg.Connection[Any], *, source: str) -> bool:
    """True when ``source`` has at least one subject with upstream filings we
    have NOT yet ingested (``new_filings_since > 0``) — a POSITIVE,
    source-level "we are behind" signal (C2 / #1508).

    Distinct from ``not _source_watermark_fresh`` (which is false for quiet
    sources / fresh installs and would false-RED them) and from the old
    per-subject ``_has_data_freshness_gap`` (event-form jitter). LIMIT 1.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1 FROM data_freshness_index
             WHERE source = %(source)s AND new_filings_since > 0
             LIMIT 1
            """,
            {"source": source},
        )
        return cur.fetchone() is not None
```

- [ ] **Step 4: Wire it as the `watermark_gap` input (replace `_has_data_freshness_gap`)**

Replace lines 807-815 (`has_data_freshness_gap = (... _has_data_freshness_gap(...))`) with:

```python
    has_data_freshness_gap = (
        freshness_source is not None
        and process_status != "running"
        and _source_watermark_behind(conn, source=freshness_source)
    )
```

(`_has_data_freshness_gap` becomes unused — delete it and its `WATERMARK_GAP_TOLERANCE_S` import if nothing else uses them; grep first. The `compute_stale_reasons` arg name `has_data_freshness_gap` stays — only its meaning sharpened.)

- [ ] **Step 5: Run tests + fast tier**

Run: `uv run pytest tests/test_scheduled_adapter_watermark.py -v && uv run pytest -m "not db" -q`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add app/services/processes/scheduled_adapter.py tests/test_scheduled_adapter_watermark.py
git commit -m "feat(#1508): watermark_gap = positive source-level behind predicate (C2)"
```

---

## Task 3: C5 — `orchestrator_full_sync` honest overdue via `sync_runs`

**Files:**
- Modify: `app/services/processes/scheduled_adapter.py:306-308` (`_ORCHESTRATOR_SYNC_SCOPE`)
- Test: `tests/test_scheduled_adapter_orchestrator.py` (db-tier)

- [ ] **Step 1: Write failing test**

```python
# tests/test_scheduled_adapter_orchestrator.py
import pytest
from app.services.processes.scheduled_adapter import _ORCHESTRATOR_SYNC_SCOPE

def test_full_sync_resolves_from_sync_runs():
    assert _ORCHESTRATOR_SYNC_SCOPE.get("orchestrator_full_sync") == "full"
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest tests/test_scheduled_adapter_orchestrator.py -v`
Expected: FAIL — key absent (returns None).

- [ ] **Step 3: Add the entry**

Confirm the `sync_runs.scope` value for the full DAG (grep `scope=` in `app/services/sync_orchestrator/`; the high-frequency one is `"high_frequency"`, full is likely `"full"`). Then:

```python
_ORCHESTRATOR_SYNC_SCOPE: Final[dict[str, str]] = {
    "orchestrator_high_frequency_sync": "high_frequency",
    "orchestrator_full_sync": "full",
}
```

- [ ] **Step 4: Run + fast tier**

Run: `uv run pytest tests/test_scheduled_adapter_orchestrator.py -v && uv run pytest -m "not db" -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add app/services/processes/scheduled_adapter.py tests/test_scheduled_adapter_orchestrator.py
git commit -m "feat(#1508): orchestrator_full_sync honest overdue via sync_runs (C5)"
```

---

## Task 4: C6 — never-started bound on a persisted first-seen anchor

**Files:**
- Create: `sql/NNN_job_first_seen.sql` (use next free number — `ls sql/ | tail`)
- Modify: jobs bootstrap (`app/workers/scheduler.py` registry load, or the jobs-process startup that already iterates `SCHEDULED_JOBS`) — upsert first-seen
- Modify: `app/services/processes/scheduled_adapter.py` — compute `never_started`
- Modify: `app/services/processes/__init__.py` (`ProcessRow`), `health_verdict.py`, `app/api/processes.py`
- Test: `tests/test_health_verdict.py` (pure), `tests/test_job_first_seen.py` (db)

- [ ] **Step 1: Write the migration**

`sql/NNN_job_first_seen.sql`:

```sql
-- #1508 C6 — persisted per-job first-seen anchor for the never-started verdict.
-- Volatile process-start cannot anchor it (resets the grace window every
-- restart → long-cadence never-run jobs lie green). One row per job_name,
-- written once on first registry load.
CREATE TABLE IF NOT EXISTS job_first_seen (
    job_name   text PRIMARY KEY,
    first_seen timestamptz NOT NULL DEFAULT now()
);
```

- [ ] **Step 2: Write failing pure verdict test**

```python
# tests/test_health_verdict.py  (add)
from app.services.processes.health_verdict import compute_verdict

def test_never_started_past_grace_is_attention():
    v, _, reason = compute_verdict(status="pending_first_run", stale_reasons=(), never_started=True)
    assert v == "attention" and reason == "never started"

def test_pending_first_run_within_grace_stays_working():
    v, _, _ = compute_verdict(status="pending_first_run", stale_reasons=(), never_started=False)
    assert v == "working"
```

- [ ] **Step 3: Run to verify failure**

Run: `uv run pytest tests/test_health_verdict.py -k never_started -v`
Expected: FAIL — `compute_verdict` got unexpected kwarg `never_started`.

- [ ] **Step 4: Add `never_started` to `compute_verdict`**

In `health_verdict.py`, add `never_started: bool = False` to the signature, and change the `pending_first_run` branch (lines 173-184):

```python
    if status == "pending_first_run":
        if never_started:
            # C6: overdue past its first expected fire with still zero rows —
            # broken-from-day-one, not merely awaiting its first slot.
            return ("attention", False, "never started")
        if watermark_is_fresh:
            return ("current", False, "")
        return ("working", False, "first run pending")
```

- [ ] **Step 5: Run pure test to verify pass**

Run: `uv run pytest tests/test_health_verdict.py -k never_started -v`
Expected: PASS.

- [ ] **Step 6: Add `never_started` to `ProcessRow` + thread through `_convert_row`**

Add `never_started: bool = False` to `ProcessRow` (`app/services/processes/__init__.py`). In `app/api/processes.py::_convert_row`, pass `never_started=row.never_started` into `compute_verdict(...)`.

- [ ] **Step 7: Compute `never_started` in the adapter + upsert first-seen**

In the jobs-process startup that iterates `SCHEDULED_JOBS`, upsert once:

```python
conn.execute(
    "INSERT INTO job_first_seen (job_name) VALUES (%(n)s) ON CONFLICT (job_name) DO NOTHING",
    {"n": job.name},
)
```

In `scheduled_adapter.py` `build_row`, after `terminal_row` is resolved:

```python
    never_started = False
    if terminal_row is None:  # lifetime-zero
        first_seen = _job_first_seen(conn, job_name=job.name)  # SELECT first_seen ... LIMIT 1
        if first_seen is not None:
            first_expected = compute_next_run(job.cadence, first_seen)
            grace = max(int(job.cadence.period.total_seconds()), SCHEDULE_MISS_FLOOR_S)
            never_started = first_expected < now - timedelta(seconds=grace)
```

Pass `never_started=never_started` into the `ProcessRow(...)` constructor.

- [ ] **Step 8: Write + run a db-tier test for the anchor, then fast tier**

```python
# tests/test_job_first_seen.py
import pytest
pytestmark = pytest.mark.db
# insert a job_first_seen row far in the past for a never-run daily job,
# assert build_row(...).never_started is True; insert one within grace, assert False.
```

Run: `uv run pytest tests/test_job_first_seen.py -v && uv run pytest -m "not db" -q`
Expected: PASS.

- [ ] **Step 9: Commit**

```bash
git add sql/NNN_job_first_seen.sql app/services/processes/ app/api/processes.py app/workers/scheduler.py tests/test_health_verdict.py tests/test_job_first_seen.py
git commit -m "feat(#1508): never-started verdict on persisted first-seen anchor (C6)"
```

---

## Task 5: Operator-cancel reads benign (green), system/crash cancel stays red

**Files:**
- Modify: `app/services/processes/scheduled_adapter.py` (compute `cancel_was_operator_initiated` via `process_stop_requests` join; note line 697 placeholder `cancelled_by_operator_id=None  # PR4 wires the join`)
- Modify: `app/services/processes/__init__.py` (`ProcessRow`), `health_verdict.py`, `app/api/processes.py`
- Test: `tests/test_health_verdict.py` (pure)

- [ ] **Step 1: Write failing pure verdict tests**

```python
# tests/test_health_verdict.py  (add)
def test_operator_cancel_is_benign_green():
    v, _, _ = compute_verdict(status="cancelled", stale_reasons=(), cancel_was_operator_initiated=True)
    assert v == "current"

def test_system_cancel_stays_attention():
    v, _, reason = compute_verdict(status="cancelled", stale_reasons=(), cancel_was_operator_initiated=False)
    assert v == "attention" and reason == "last run cancelled"
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest tests/test_health_verdict.py -k cancel -v`
Expected: FAIL — unexpected kwarg.

- [ ] **Step 3: Add `cancel_was_operator_initiated` to `compute_verdict`**

Add `cancel_was_operator_initiated: bool = False` to the signature; change the `cancelled` branch (line 171):

```python
    if status == "cancelled":
        if cancel_was_operator_initiated:
            # Deliberate operator cancel — benign until the next fire.
            return ("current", False, "")
        return ("attention", False, "last run cancelled")
```

- [ ] **Step 4: Run pure test**

Run: `uv run pytest tests/test_health_verdict.py -k cancel -v`
Expected: PASS.

- [ ] **Step 5: Wire the join in the adapter + thread through**

Add `cancel_was_operator_initiated: bool = False` to `ProcessRow`. In the adapter, when `terminal_row["cancelled_at"]` is set, probe `process_stop_requests` for a matching row (operator-initiated) and set the flag. Pass through `_convert_row` into `compute_verdict`.

- [ ] **Step 6: Run fast tier**

Run: `uv run pytest -m "not db" -q`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add app/services/processes/ app/api/processes.py tests/test_health_verdict.py
git commit -m "feat(#1508): operator-cancel reads benign; crash-cancel stays red"
```

---

## Task 6: Invariant regression test (wedge never masked)

**Files:**
- Test: `tests/test_health_verdict.py` (pure)

- [ ] **Step 1: Add the explicit invariant table-test**

```python
# tests/test_health_verdict.py  (add)
import pytest

@pytest.mark.parametrize("wedge", ["watermark_gap", "queue_stuck", "mid_flight_stuck"])
@pytest.mark.parametrize("recover", ["retry_in_flight", "liveness_kick_in_flight"])
def test_recovery_signal_never_masks_a_wedge(wedge, recover):
    kw = {"status": "failed", "stale_reasons": ("schedule_missed", wedge), recover: True}
    if recover == "retry_in_flight":
        kw["retry_at_display"] = "21:20"
    verdict, _, _ = compute_verdict(**kw)
    assert verdict == "attention", f"{recover} masked {wedge}"
```

- [ ] **Step 2: Run — must already PASS (invariant preserved)**

Run: `uv run pytest tests/test_health_verdict.py -k never_masks -v`
Expected: PASS (compute_verdict's precedence is unchanged; this pins it).

- [ ] **Step 3: Commit**

```bash
git add tests/test_health_verdict.py
git commit -m "test(#1508): pin wedge-never-masked invariant across retry+kick"
```

---

## Task 7: C3 — collapse the FE to two colours

**Files:**
- Modify: `frontend/src/components/admin/processStatus.ts:150-186` (`VERDICT_VISUAL`, `VERDICT_SORT_PRIORITY`)
- Modify: `frontend/src/components/admin/ProcessesTable.tsx:386` (`isCollapsible`) + pinning
- Test: `frontend/src/components/admin/processStatus.test.ts` (or the existing ProcessesTable test)

- [ ] **Step 1: Write failing unit test**

```ts
// processStatus.test.ts
import { VERDICT_VISUAL, VERDICT_SORT_PRIORITY } from "./processStatus";

test("working and self_healing share the calm (non-attention) tone", () => {
  expect(VERDICT_VISUAL.working.tone).toBe(VERDICT_VISUAL.current.tone);
  expect(VERDICT_VISUAL.self_healing.tone).toBe(VERDICT_VISUAL.current.tone);
  expect(VERDICT_VISUAL.attention.tone).not.toBe(VERDICT_VISUAL.current.tone);
});

test("only attention sorts to the pinned region", () => {
  expect(VERDICT_SORT_PRIORITY.attention).toBe(0);
  expect(VERDICT_SORT_PRIORITY.working).toBe(VERDICT_SORT_PRIORITY.current);
  expect(VERDICT_SORT_PRIORITY.self_healing).toBe(VERDICT_SORT_PRIORITY.current);
});
```

(Adapt `.tone` to the actual `StatusVisual` field that drives colour — read `processStatus.ts` for the field name, e.g. `dotClass` / `colorClass`. Assert green-equality on that field.)

- [ ] **Step 2: Run to verify failure**

Run: `pnpm --dir frontend test:unit -- processStatus`
Expected: FAIL.

- [ ] **Step 3: Map `working`/`self_healing` to the calm green visual + sort with current**

Edit `VERDICT_VISUAL` so `working` and `self_healing` reuse `current`'s colour (keep their distinct `label` text — `updating…` / sub-label — but the dot/tone is the calm green; drop the blue/amber pulse-as-alarm). Edit `VERDICT_SORT_PRIORITY`: `attention: 0`, and `working`/`self_healing`/`current` all `1` (one calm group).

- [ ] **Step 4: Make `working` collapsible + pin only attention**

`ProcessesTable.tsx`: change `isCollapsible` (line 386) to `verdict !== "attention"` (i.e. `current || working || self_healing`). Update the pinned/collapsed `useMemo` split (lines 132-149) and `collapsedLabel` so the disclosure counts include working. Update the comment block at 124-129.

- [ ] **Step 5: Run FE unit + typecheck**

Run: `pnpm --dir frontend test:unit && pnpm --dir frontend typecheck`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add frontend/src/components/admin/processStatus.ts frontend/src/components/admin/ProcessesTable.tsx frontend/src/components/admin/processStatus.test.ts
git commit -m "feat(#1508): collapse Processes page to two colours (C3)"
```

---

## Task 8: C4 — global red banner when the jobs engine is down

**Files:**
- Modify: the Processes header (`StaleBanner` — grep `StaleBanner` under `frontend/src/components/admin/`) + its data source
- Possibly modify: `app/api/system.py:281` (`_derive_overall_status`) to fold `jobs_process.state == "down"`, OR have the header read `/system/jobs.jobs_process.state`
- Test: FE unit on the banner + a backend test if `_derive_overall_status` changes

- [ ] **Step 1: Decide the wiring (investigation)**

Confirm what the Processes header already fetches. `jobs_process.state` (`healthy`/`degraded`/`down`) lives on `/system/jobs` (`JobsListResponse:213`), and `_build_jobs_process_health` returns `down` when the heartbeat table is empty (jobs process not running). Choose the minimal path:
- (a) If the header already consumes `/system/status`, fold `jobs_process.state == "down" → overall_status="down"` in `_derive_overall_status` (line 281) and surface it; OR
- (b) have the header fetch `/system/jobs` `jobs_process.state` directly.
Record the choice in the commit message.

- [ ] **Step 2: Write the failing FE test for the banner**

```ts
// StaleBanner.test.tsx (or wherever the header renders)
test("renders a hard-red engine-down banner when jobs engine is down", () => {
  render(<StaleBanner enginedown={true} /* + minimal props */ />);
  expect(screen.getByText(/jobs engine not running/i)).toBeInTheDocument();
});
```

- [ ] **Step 3: Run to verify failure**

Run: `pnpm --dir frontend test:unit -- StaleBanner`
Expected: FAIL.

- [ ] **Step 4: Implement the banner branch**

Add a top-priority red banner state: when engine is down, render "⚠ Jobs engine not running — processes are not updating" ABOVE the per-row clean-bill/attention summary, regardless of per-row verdicts. Wire the `enginedown` input from the chosen data source (Step 1).

- [ ] **Step 5: Run FE unit + typecheck (+ backend test if `_derive_overall_status` changed)**

Run: `pnpm --dir frontend test:unit && pnpm --dir frontend typecheck`
(If backend changed: `uv run pytest tests/test_system_status.py -q`.)
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add frontend/src/components/admin/ app/api/system.py
git commit -m "feat(#1508): global red banner when jobs engine is down (C4)"
```

---

## Task 9: Full gate + dev-verify

- [ ] **Step 1: Backend gate**

Run:
```bash
uv run ruff check . && uv run ruff format --check . && uv run pyright
uv run pytest -m "not db" && uv run pytest tests/smoke
docker compose --profile test up -d postgres-test && uv run pytest -m db -k "watermark or first_seen or orchestrator or stale_detection or health_verdict"
```
Expected: all green.

- [ ] **Step 2: Frontend gate**

Run: `pnpm --dir frontend typecheck && pnpm --dir frontend test:unit`
Expected: PASS.

- [ ] **Step 3: Codex ckpt-2 (before first push)**

Run `codex exec review` on the branch. Fix anything real before pushing.

- [ ] **Step 4: Dev-verify (operator-visible, after restart)**

Read the live verdicts via the in-process path (memory `project_db_lane_starvation.md`):
```python
import app.main, psycopg
from app.config import settings
from app.api import processes as P
with psycopg.connect(settings.database_url) as c:
    rows = [P._convert_row(r) for r in P._gather_snapshot(c).rows]
```
Confirm: (1) `orchestrator_high_frequency_sync` / `monitor_positions` no longer read attention purely from `schedule_missed` (C1); (2) a genuinely-overdue daily job still reads attention; (3) a source with `new_filings_since > 0` reads attention via C2; (4) FE shows two colours with only attention pinned. Record the figures in the PR description (clauses 8/11 of CLAUDE.md DoD).

- [ ] **Step 5: Open PR, poll review + CI, resolve, merge** per CLAUDE.md branch/PR workflow.

---

## Self-Review

- **Spec coverage:** C1 → Task 1; C2 → Task 2; C3 → Task 7; C4 → Task 8; C5 → Task 3; C6 → Task 4; operator-cancel edge → Task 5; invariant → Task 6; dev-verify/DoD → Task 9. All spec sections mapped.
- **Placeholders:** the two genuine unknowns (C2 `new_filings_since` semantics, C4 header data source, plus the `Cadence` period accessor) are explicit *investigation steps* (Task 2 Step 0, Task 8 Step 1, Task 1 Step 5) that must resolve before their code lands — not hand-waves in code steps.
- **Type consistency:** new `compute_verdict` kwargs (`never_started`, `cancel_was_operator_initiated`) and new `ProcessRow` fields (`never_started`, `cancel_was_operator_initiated`) are defined in Tasks 4/5 and consumed via `_convert_row`; `_source_watermark_behind(conn, *, source)` signature is consistent across Task 2; `cadence_period_s` added in Task 1 and used nowhere else.
