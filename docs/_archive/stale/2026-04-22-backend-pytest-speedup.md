# Backend Pytest Speedup Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an `EBULL_SKIP_CATCH_UP` env-var gate to `JobRuntime.start()` so the pytest suite's `TestClient(app)` lifespan entries stop firing real overdue APScheduler jobs. Sets the var in `tests/conftest.py`.

**Architecture:** Single env-var check wrapping the `self._catch_up()` call at `app/jobs/runtime.py:302`. Test conftest sets the var at import time with `setdefault`. No changes to `_catch_up()` body (preserves direct unit tests). No changes to production behaviour. No xdist, no deps changes.

**Tech Stack:** Python 3.14, FastAPI, APScheduler, pytest.

**Spec:** `docs/superpowers/specs/2026-04-22-backend-pytest-speedup.md`
**Ticket:** #327 (narrow subset). Branch `fix/327a-backend-pytest-speedup` exists; spec committed.

---

## File Structure

| Path | Responsibility | Action |
| --- | --- | --- |
| `app/jobs/runtime.py` | Job runtime — env-var gate around `_catch_up()` call in `start()` | Modify (2 lines + import) |
| `tests/conftest.py` | Global pytest config — set `EBULL_SKIP_CATCH_UP=1` | Modify (3 lines) |
| `tests/test_jobs_runtime.py` | Direct catch-up tests — add coverage for the gate | Modify (1 test added) |

No other files touched.

---

## Task 1: Env-var gate in `JobRuntime.start()` (TDD)

**Files:**

- Modify: `tests/test_jobs_runtime.py` — add test class asserting gate behaviour
- Modify: `app/jobs/runtime.py:302` — wrap `self._catch_up()` in the env-var check

- [ ] **Step 1: Read existing `TestStartWiring` structure**

Run: `grep -n "class TestStartWiring\|class TestCatchUpOnBoot" tests/test_jobs_runtime.py`

Confirm both classes exist + find where `TestStartWiring` ends so the new class lands nearby.

- [ ] **Step 2: Write the failing test for the env-gate behaviour**

Append a new test class to `tests/test_jobs_runtime.py` near the existing `TestStartWiring` + `TestCatchUpOnBoot` classes:

```python
class TestStartCatchUpEnvGate:
    """Tests for the ``EBULL_SKIP_CATCH_UP`` env-var gate on ``start()``.

    The gate wraps the ``self._catch_up()`` call at the end of ``start()``
    so pytest sessions can enter the FastAPI lifespan without firing real
    overdue APScheduler jobs. Direct calls to ``rt._catch_up()`` are
    NOT gated (covered in ``TestCatchUpOnBoot``).
    """

    def test_env_var_set_skips_catch_up_in_start(
        self,
        patched_runtime: None,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from app.jobs import runtime as rt_mod

        calls: list[str] = []
        monkeypatch.setattr(
            rt_mod.JobRuntime,
            "_catch_up",
            lambda self: calls.append("called"),
        )
        monkeypatch.setenv("EBULL_SKIP_CATCH_UP", "1")

        rt = rt_mod.JobRuntime()
        try:
            rt.start()
            assert calls == [], "start() must skip _catch_up() when EBULL_SKIP_CATCH_UP=1"
        finally:
            rt.shutdown()

    def test_env_var_unset_runs_catch_up_in_start(
        self,
        patched_runtime: None,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from app.jobs import runtime as rt_mod

        calls: list[str] = []
        monkeypatch.setattr(
            rt_mod.JobRuntime,
            "_catch_up",
            lambda self: calls.append("called"),
        )
        monkeypatch.delenv("EBULL_SKIP_CATCH_UP", raising=False)

        rt = rt_mod.JobRuntime()
        try:
            rt.start()
            assert calls == ["called"], "start() must invoke _catch_up() when env var unset"
        finally:
            rt.shutdown()

    def test_env_var_zero_runs_catch_up_in_start(
        self,
        patched_runtime: None,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Exact-match gate: only '1' skips. '0' / 'false' / any other value fires catch-up.

        Lets a developer override the conftest.py default with
        EBULL_SKIP_CATCH_UP=0 pytest to reproduce catch-up bugs.
        """
        from app.jobs import runtime as rt_mod

        calls: list[str] = []
        monkeypatch.setattr(
            rt_mod.JobRuntime,
            "_catch_up",
            lambda self: calls.append("called"),
        )
        monkeypatch.setenv("EBULL_SKIP_CATCH_UP", "0")

        rt = rt_mod.JobRuntime()
        try:
            rt.start()
            assert calls == ["called"], "EBULL_SKIP_CATCH_UP=0 must still fire catch-up"
        finally:
            rt.shutdown()
```

Make sure the existing `patched_runtime` fixture is importable at this location. (It's already used by `TestStartWiring` + `TestCatchUpOnBoot` — same file, same import scope.)

- [ ] **Step 3: Run the new tests — expect the first test to FAIL (no gate yet)**

Run: `uv run pytest tests/test_jobs_runtime.py::TestStartCatchUpEnvGate -v`

Expected: `test_env_var_set_skips_catch_up_in_start` FAILS with `assert ["called"] == []`. The other two tests pass (they match current behaviour of always calling `_catch_up()`).

- [ ] **Step 4: Add the env-var gate in `JobRuntime.start()`**

Open `app/jobs/runtime.py`. Add `import os` to the imports block. The file already has `import logging`, `import threading` — place `import os` alongside. Current imports (lines 39–50):

```python
from __future__ import annotations

import logging
import os
import threading
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from typing import Final

import psycopg
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
```

Then replace line 302 (`self._catch_up()`) with:

```python
        # Gate: EBULL_SKIP_CATCH_UP=1 skips the catch-up loop. Used by
        # tests/conftest.py so pytest's TestClient(app) lifespan entries
        # don't fire overdue APScheduler jobs against the dev DB (300s+
        # teardown waits previously).
        #
        # Gated at the call site in start() (not inside _catch_up() body)
        # so direct unit tests in TestCatchUpOnBoot that call
        # rt._catch_up() bypass this gate and continue to exercise the
        # catch-up loop in isolation.
        if os.environ.get("EBULL_SKIP_CATCH_UP") == "1":
            logger.debug("EBULL_SKIP_CATCH_UP=1; skipping catch-up on boot")
        else:
            self._catch_up()
```

- [ ] **Step 5: Run the new tests — expect all three to PASS**

Run: `uv run pytest tests/test_jobs_runtime.py::TestStartCatchUpEnvGate -v`

Expected: 3 tests PASS.

- [ ] **Step 6: Run existing `TestCatchUpOnBoot` tests — expect no regression**

Run: `uv run pytest tests/test_jobs_runtime.py::TestCatchUpOnBoot -v`

Expected: all existing tests still PASS. Direct calls to `rt._catch_up()` aren't gated.

- [ ] **Step 7: Commit**

```bash
git add app/jobs/runtime.py tests/test_jobs_runtime.py
git commit -m "feat(#327): EBULL_SKIP_CATCH_UP env gate in JobRuntime.start()

Wraps the self._catch_up() call at the end of start() in an env-var
check so pytest sessions can enter the FastAPI lifespan without firing
overdue APScheduler jobs against the dev DB. Gated at the call site
in start() rather than _catch_up() body to preserve direct unit tests
in TestCatchUpOnBoot that call rt._catch_up() in isolation."
```

---

## Task 2: Set the env var in `tests/conftest.py`

**Files:**

- Modify: `tests/conftest.py` — add `os.environ.setdefault("EBULL_SKIP_CATCH_UP", "1")` at import time

- [ ] **Step 1: Read current `tests/conftest.py`**

Run: `cat tests/conftest.py`

Should be short (~12 lines): installs auth override, imports `ebull_test_conn` fixture. Confirm shape before editing.

- [ ] **Step 2: Capture baseline pytest time for comparison (optional but recommended)**

Run: `uv run pytest tests/api/test_sync_scope_behind.py -v --durations=5` with `EBULL_SKIP_CATCH_UP=0` to reproduce the slow teardowns. Skip if baseline already known (1116s full suite; 300s+ teardowns).

- [ ] **Step 3: Edit `tests/conftest.py` — add env var at top**

Replace the file contents with:

```python
"""Shared pytest configuration for eBull API tests.

The protected routes use ``require_session_or_service_token`` (issue #98).
We install a no-op override on it so the broad set of pre-existing API
tests can hit protected endpoints without managing bearer tokens or
session cookies. The dedicated auth tests
(``test_api_auth_session.py``) clear this override per-test to exercise
the real dependency.
"""

from __future__ import annotations

import os

# Skip lifespan catch-up in every TestClient(app) enter/exit cycle.
# Without this, each test that enters the FastAPI lifespan fires real
# overdue APScheduler jobs against the dev DB, which then block the
# shutdown(wait=True) path for hundreds of seconds per test. Gated at
# the start() call site in app/jobs/runtime.py so direct catch-up unit
# tests in tests/test_jobs_runtime.py::TestCatchUpOnBoot are unaffected.
# setdefault (not hard-set) lets a developer run
# EBULL_SKIP_CATCH_UP=0 pytest to reproduce catch-up bugs.
os.environ.setdefault("EBULL_SKIP_CATCH_UP", "1")

from app.api.auth import require_session_or_service_token  # noqa: E402
from app.main import app  # noqa: E402
from tests.fixtures.ebull_test_db import ebull_test_conn as ebull_test_conn  # noqa: F401, E402


def _noop_auth() -> None:  # pragma: no cover - trivial override
    return None


app.dependency_overrides[require_session_or_service_token] = _noop_auth
```

- [ ] **Step 4: Run the slow test file — expect fast teardowns now**

Run: `uv run pytest tests/api/test_sync_scope_behind.py -v --durations=10`

Expected: teardown times drop from 300s+ to <100ms. Previously-slow tests pass in a few seconds each. Wall time for this file alone drops from ~630s to a few seconds.

- [ ] **Step 5: Run the smoke test — expect green**

Run: `uv run pytest tests/smoke/test_app_boots.py -v`

Expected: PASS. Smoke invariants (lifespan enters, migrations apply, pool open, master-key bootstrap, app.state coherent) are orthogonal to the catch-up loop. The smoke test asserts app state shape, not that catch-up ran.

If smoke fails: inspect the failure. If an assertion references `app.state.job_runtime._catch_up_ran` or similar, the spec's assumption about orthogonality is wrong — stop and re-scope. Expected case is green.

- [ ] **Step 6: Run the full suite — expect ~200s + 2325 passed**

Run: `uv run pytest 2>&1 | tail -5`

Expected: `2325 passed, 1 skipped` with wall time ~200s (vs 1116s baseline). Smaller machines may take longer; the win is relative to the per-test teardown collapse.

- [ ] **Step 7: Commit**

```bash
git add tests/conftest.py
git commit -m "test(#327): set EBULL_SKIP_CATCH_UP=1 in pytest conftest

Kills the per-test 3-4s lifespan teardown cost from APScheduler
shutdown(wait=True) blocking on overdue catch-up jobs. Two tests in
tests/api/test_sync_scope_behind.py previously paid 300s+ teardowns
each; those drop to <100ms. Expected full-suite wall time: 1116s -> ~200s."
```

---

## Task 3: Pre-push gates + Codex checkpoint 2 + push + PR

**Files:** (none; gate + review + publish)

- [ ] **Step 1: Run all backend gates**

```bash
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv run pytest
```

All four must pass. If ruff format flags the new `import os` line or the comment blocks, run `uv run ruff format .` then re-stage with a separate `style(#327): ruff format` commit.

- [ ] **Step 2: Smoke gate**

Run: `uv run pytest tests/smoke/test_app_boots.py -v`

Expected: PASS. The whole point of the change — don't let the smoke gate fail silently.

- [ ] **Step 3: Verify direct-catch-up test coverage stayed green**

Run: `uv run pytest tests/test_jobs_runtime.py -v`

Expected: all `TestCatchUpOnBoot` tests PASS (they call `rt._catch_up()` directly; env var is irrelevant at that call site). Plus the three new `TestStartCatchUpEnvGate` tests PASS.

- [ ] **Step 4: Frontend gates (unchanged by this PR but required by CLAUDE.md)**

```bash
pnpm --dir frontend typecheck
pnpm --dir frontend test --pool=forks --poolOptions.forks.singleFork=true
```

Expected: both green. This PR does not touch frontend, but the gate is mandatory.

- [ ] **Step 5: Codex checkpoint 2 — diff review before push**

```bash
git diff main...HEAD > /tmp/pr327a_diff.txt
codex.cmd exec "Checkpoint 2 diff review for PR (narrow #327 subset) — backend pytest speedup via EBULL_SKIP_CATCH_UP env gate. Diff at /tmp/pr327a_diff.txt. Spec at d:/Repos/eBull/docs/superpowers/specs/2026-04-22-backend-pytest-speedup.md.

Focus: gate placement in start() (not _catch_up body), conftest setdefault import ordering, smoke-test invariant preservation, any missed test that calls start() directly and would be affected. Reply terse."
```

Fix any real findings before pushing.

- [ ] **Step 6: Push + open PR**

```bash
git push -u origin fix/327a-backend-pytest-speedup
gh pr create --title "fix(#327): skip catch-up in test lifespan — 5.6x backend pytest speedup" --body "$(cat <<'EOF'
## What

- `JobRuntime.start()` wraps the `self._catch_up()` call at `app/jobs/runtime.py:302` in an `EBULL_SKIP_CATCH_UP` env-var gate.
- `tests/conftest.py` sets the env var via `os.environ.setdefault` at import time.
- New test class `TestStartCatchUpEnvGate` in `tests/test_jobs_runtime.py` covers the gate (set / unset / `=0` variants).

## Why

Backend pytest regressed from 25s (2026-04-19 baseline per #327) to 1116s. Every `with TestClient(app)` entry fires the FastAPI lifespan, which calls `JobRuntime.start()` → `_catch_up()` → runs real overdue APScheduler jobs against the dev DB. Teardown blocks on `scheduler.shutdown(wait=True)` waiting for those to finish. Two tests in `tests/api/test_sync_scope_behind.py` paid 300s+ teardowns each; the rest paid ~3s each.

Gating in `start()` (not `_catch_up()` body) preserves direct unit tests in `tests/test_jobs_runtime.py::TestCatchUpOnBoot` that call `rt._catch_up()` in isolation.

## Test plan

- Full suite: 2325 passed, ~200s (vs 1116s).
- Slow file: `tests/api/test_sync_scope_behind.py` teardowns drop from 300s+ to <100ms.
- Smoke: `tests/smoke/test_app_boots.py` green.
- Direct catch-up tests: `tests/test_jobs_runtime.py::TestCatchUpOnBoot` green.
- New gate tests: `TestStartCatchUpEnvGate` 3/3 green.

## Called out

- Production behaviour unchanged. Env var unset → catch-up fires.
- Scheduler still starts in tests. Only the catch-up loop is skipped. `app.state.job_runtime` non-None.
- pytest-xdist + frontend items from #327 deferred. Codex ckpt 1 surfaced that many tests use `ebull_test` without the `ebull_test_conn` fixture, and collection-time DB bootstrap races under multi-worker xdist. Follow-up PR after audit.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

- [ ] **Step 7: Start polling review + CI immediately**

```bash
gh pr checks --watch
gh pr view --comments
```

Resolve every comment as FIXED / DEFERRED / REBUTTED. Merge after APPROVE on the most recent commit + CI green (or Codex-agreed rebuttal-only round per memory `feedback_merge_rules`).

---

## Self-review

**1. Spec coverage:**

- Env gate at `start()` call site → Task 1 (step 4).
- `tests/conftest.py` sets env var with `setdefault` → Task 2.
- Direct catch-up tests stay green → Task 1 (step 6) asserts.
- Smoke test stays green → Task 2 (step 5) + Task 3 (step 2).
- `_catch_up()` body unchanged → confirmed in Task 1 (only wraps the call, doesn't edit the method body).
- No xdist / no deps changes → out of scope per spec.

All spec sections covered.

**2. Placeholder scan:** No "TBD", "TODO", "implement later". Every code step shows the full diff.

**3. Type consistency:**

- Env var name `EBULL_SKIP_CATCH_UP` identical across spec + gate + conftest + tests.
- Gate uses exact-match `== "1"` — tests assert `"0"` and unset both fire catch-up.
- `patched_runtime` fixture name matches existing `TestCatchUpOnBoot` pattern in same file.
