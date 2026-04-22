# Backend pytest speedup — design spec

**Ticket:** #327 (narrow subset — backend catch-up regression only. xdist + frontend items deferred per Codex ckpt 1 review to separate PR with proper DB-race audit.)

## Problem

Backend `uv run pytest` currently takes **1116s (18m36s)** on local runs, up from a **25s** baseline recorded in #327 on 2026-04-19. Profiling (2026-04-22) isolates it to two tests with 311s and 309s **teardown** times (not call time), plus a long tail of ~3–4s teardowns on every test using `with TestClient(app)`:

```text
311.99s teardown tests/api/test_sync_scope_behind.py::test_post_sync_behind_includes_non_healthy_upstream
309.27s teardown tests/api/test_sync_scope_behind.py::test_post_sync_behind_bypasses_legacy_freshness_filter
```

## Root cause

`TestClient(app) as client:` triggers FastAPI lifespan. Lifespan calls `start_runtime()` at `app/main.py:123` before `yield`. `start_runtime()` → `JobRuntime.start()` → `_catch_up()` at `app/jobs/runtime.py:302` — fires every overdue `SCHEDULED_JOBS` entry with `catch_up_on_boot=True` (universe sync, market data, filings ingestion, etc.) against the real dev DB.

Teardown blocks at `app/jobs/runtime.py:438` on `self._scheduler.shutdown(wait=True)` — deliberate per lines 425–433 (preserves `job_runs` write integrity). Shutdown waits for those catch-up jobs to complete real work.

Two tests hit the 300s worst case when catch-up has multiple overdue jobs to execute end-to-end. Every other TestClient-using test pays ~3s for runtime startup + shutdown on its own.

Blast radius: only 4 files use `with TestClient(app)` — `tests/api/conftest.py` (the `clean_client` fixture), `tests/test_sync_orchestrator_api.py`, `tests/test_main_health.py`, `tests/smoke/test_app_boots.py`.

## Scope decision — after Codex ckpt 1 review

Codex flagged three issues with the initial broader scope (xdist + DB auto-grouping):

1. `tests/test_jobs_runtime.py::TestCatchUpOnBoot` calls `rt._catch_up()` directly — env gate on `_catch_up` would early-return and break those tests.
2. Many non-`ebull_test_conn` tests touch `ebull_test` (via direct connect, local fixtures). xdist `loadgroup` on the fixture alone wouldn't serialise all of them.
3. Collection-time `test_db_available()` + migration bootstrap races under multi-worker xdist.

Narrowing scope accordingly: **skip-catch-up only in this PR.** xdist comes later when we can properly audit DB-touching tests + gate collection-time bootstrap. Expected perf: 1116s → ~200s (5.6× win) from the catch-up fix alone. Good enough to unblock.

## Decisions

| # | Decision | Reason |
| --- | --- | --- |
| 1 | **Gate in `JobRuntime.start()` after scheduler init, around the `_catch_up()` call — NOT inside `_catch_up()` itself.** | Preserves direct `rt._catch_up()` unit tests in `tests/test_jobs_runtime.py::TestCatchUpOnBoot`. Those bypass `start()` and test the loop in isolation. Gating in `start()` skips only the lifespan-path invocation. |
| 2 | **Env var `EBULL_SKIP_CATCH_UP=1`, set via `tests/conftest.py` at import time with `os.environ.setdefault`.** | Narrow, explicit opt-in. `setdefault` lets a developer reproduce catch-up bugs locally with `EBULL_SKIP_CATCH_UP=0 pytest`. Production unset → current behaviour preserved. |
| 3 | **Smoke test `tests/smoke/test_app_boots.py` runs with catch-up skipped.** | Smoke invariants are: lifespan enters cleanly, migrations applied, pool open, master-key bootstrap ran, `app.state` coherent. Catch-up is orthogonal to those — production catch-up fires inside the lifespan but its outcome is not an invariant of `test_app_lifespan_boots_and_state_is_coherent`. |
| 4 | **No `pytest-xdist`, no `pyproject.toml` dep changes, no collection hooks in this PR.** | Per Codex ckpt 1: multiple non-`ebull_test_conn` tests touch `ebull_test`; collection-time bootstrap races under xdist. Each needs its own audit + fix. Separate PR. Ticket #327 stays open for that + frontend items. |

## Architecture

Two surfaces touched:

| Path | Change |
| --- | --- |
| `app/jobs/runtime.py` (`JobRuntime.start` method) | Wrap the `self._catch_up()` call in an env-var check |
| `tests/conftest.py` | Add `os.environ.setdefault("EBULL_SKIP_CATCH_UP", "1")` at the top |

No changes to `app/main.py`. No changes to `_catch_up()` body. No changes to `JobRuntime.shutdown()` or any test file.

### Code shapes

**`app/jobs/runtime.py` — `JobRuntime.start()` diff around line 302:**

```python
# existing start() body up through scheduler registration ...
self._started = True
if os.environ.get("EBULL_SKIP_CATCH_UP") == "1":
    logger.debug("EBULL_SKIP_CATCH_UP=1; skipping catch-up on boot")
else:
    self._catch_up()
```

Add `import os` at top of file if not already present.

The env check is at the call site in `start()`, not inside `_catch_up()`. Direct unit tests in `tests/test_jobs_runtime.py::TestCatchUpOnBoot` that call `rt._catch_up()` bypass this gate and continue to exercise the catch-up loop.

**`tests/conftest.py` — insertion at top:**

```python
"""Shared pytest configuration for eBull API tests."""

from __future__ import annotations

import os

# Skip lifespan catch-up in every TestClient(app) enter/exit cycle.
# Without this, each test that enters the FastAPI lifespan fires real
# overdue APScheduler jobs against the dev DB, which then block the
# shutdown(wait=True) path for hundreds of seconds per test.
# Direct catch-up unit tests in tests/test_jobs_runtime.py bypass this
# gate because JobRuntime.start() reads the env var, not _catch_up()
# itself.
os.environ.setdefault("EBULL_SKIP_CATCH_UP", "1")

# ... existing imports + auth override
```

`setdefault` rather than hard-set: a developer running `EBULL_SKIP_CATCH_UP=0 pytest` to reproduce a catch-up bug locally still sees the full behaviour.

## Edge cases

| Case | Behaviour |
| --- | --- |
| Production (`EBULL_SKIP_CATCH_UP` unset) | `start()` calls `_catch_up()` exactly as today. No behaviour change. |
| Developer runs `EBULL_SKIP_CATCH_UP=0 pytest` | `start()` calls `_catch_up()` → catch-up fires → test suite slow as today. Useful for reproducing catch-up bugs. |
| Smoke test `test_app_lifespan_boots_and_state_is_coherent` | Lifespan enters, migrations apply, pool opens, master-key bootstrap runs, scheduler starts with empty catch-up. `app.state.job_runtime` is non-None and functional. Asserted invariants unchanged. |
| `tests/test_jobs_runtime.py::TestCatchUpOnBoot` direct tests | Call `rt._catch_up()` directly; env var is irrelevant at that call site. Unchanged green. |
| System-status / scheduler-introspection endpoints during tests | `app.state.job_runtime` is set to a live `JobRuntime` with an empty catch-up. Introspection reads unaffected. |

## Verification

- `EBULL_SKIP_CATCH_UP=0 uv run pytest -q` → baseline ~1116s, 2325 passed. (Manual sanity.)
- `uv run pytest -q` (default, var set via conftest) → expected ~200s, 2325 passed.
- `uv run pytest tests/smoke/test_app_boots.py -v` → green.
- `uv run pytest tests/test_jobs_runtime.py::TestCatchUpOnBoot -v` → green.

## Rollback

Revert the commit. Env var default absent → catch-up fires → production behaviour. No migration; no data impact.

## Follow-up work (not in this PR)

- pytest-xdist adoption once `ebull_test` usage is audited across non-`ebull_test_conn` tests + collection-time DB bootstrap is idempotent under concurrent workers. File as separate ticket or update #327.
- Frontend items from #327 (vitest worker cap, SetupPage refactor, `test:unit`/`test:integration` split). Separate PR.

## PR description skeleton

Title: `fix(#327): skip catch-up in test lifespan — 5.6x backend pytest speedup`

Body:

> **What**
>
> - `JobRuntime.start()` wraps the `_catch_up()` call in an `EBULL_SKIP_CATCH_UP` env-var gate.
> - `tests/conftest.py` sets the var via `os.environ.setdefault` at import time.
>
> **Why**
>
> Backend pytest regressed from 25s (2026-04-19 baseline per #327) to 1116s. Every `with TestClient(app)` entry fires the full lifespan, which calls `_catch_up()` and runs real overdue APScheduler jobs against the dev DB. Teardown blocks on `scheduler.shutdown(wait=True)` waiting for those to finish. Two tests hit 300s teardowns; the rest pay ~3s each. Skipping catch-up in the test lifespan returns ~916s to the suite.
>
> **Test plan**
>
> - Full suite: 2325 passed, ~200s (vs 1116s).
> - Smoke: `tests/smoke/test_app_boots.py` green.
> - Direct catch-up unit tests: `tests/test_jobs_runtime.py::TestCatchUpOnBoot` green (gated in `start()`, not `_catch_up()` body).
>
> **Called out**
>
> - Production behaviour unchanged. Env var unset → catch-up fires.
> - Scheduler still starts in tests. Only the catch-up loop is skipped. `app.state.job_runtime` non-None.
> - pytest-xdist + frontend items from #327 deferred. Codex ckpt 1 surfaced that many tests use `ebull_test` without the `ebull_test_conn` fixture, and collection-time DB bootstrap races under multi-worker xdist. Follow-up PR after audit.
