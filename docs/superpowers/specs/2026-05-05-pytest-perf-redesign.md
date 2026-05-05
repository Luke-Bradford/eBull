# Pytest perf redesign — concurrent isolation + xdist + template DB

**Date:** 2026-05-05
**Issue:** #893 — tech-debt: pytest infra slowness + integration test contention
**Status:** brainstorm + 2x Codex-reviewed (8 findings folded); awaiting operator sign-off

## Problem

Operator-observed pain on `main` as of 2026-05-05:

- Full pytest suite is the pre-push gate (`.githooks/pre-push:34-35`). Single
  shared `ebull_test` Postgres database is consumed serially.
- `tests/fixtures/ebull_test_db.py::ebull_test_conn` truncates **73 tables**
  (`_PLANNER_TABLES`) before and after every test that opts into the fixture.
  ~260 fixture invocations across ~130 test files.
- 121 SQL migrations under `sql/`. Migrations are skipped per-process if
  already recorded in `schema_migrations` (cheap path), but a fresh DB pays
  the full 121-file replay cost.
- Concurrent pytest invocations (developer running multiple shells, or
  pre-push hook firing while another suite is mid-flight) **block on row
  and table locks** in the shared DB. Operator has observed 10+ minute
  hangs from a stuck process holding locks while siblings pile up.
- No `pytest-xdist` configured. Sequential-only execution.
- Some tests write directly to `settings.database_url` — i.e. the
  **operator's dev DB** — bypassing the `ebull_test` fixture entirely.
  Audit (grep `psycopg.connect(settings.database_url`) finds 6 writer
  files: `tests/test_jobs_listener.py`,
  `tests/test_jobs_heartbeat.py`,
  `tests/test_jobs_queue_recovery.py`,
  `tests/test_jobs_queue_boot_drain.py`,
  `tests/test_sync_orchestrator_dispatcher.py`, and one read-only
  reachability probe in `tests/test_jobs_locks.py` (the test itself
  is read-only against the DB; the lock writes happen inside
  `JobLock` which receives a URL parameter). These violate
  `feedback_test_db_isolation.md` and would deadlock under concurrent
  pytest invocations.

Operator goal: full suite **≤ 5–10 min wall-clock** while preserving
coverage. Concurrent pytest must not deadlock.

## Out of scope

- **Truncate-by-need / opt-in fixture API rewrite** — high blast radius
  across ~130 files. Defer to follow-up after worker isolation lands.
- **`pg_dump` / `pg_restore` snapshot-restore path** — Postgres native
  `CREATE DATABASE … TEMPLATE` is faster (page-level copy) and removes
  the round trip through a dump file.
- **Pre-push hook scope reduction** — operator decision 2026-05-04 was
  explicitly to keep full pytest at push time. This spec speeds it up,
  not removes it.
- **Pre-existing failures #875 + #876** — handled in their own tickets;
  this spec must not touch them. Pre-push currently blocks on them and
  developers bypass with `--no-verify` per pickup notes.
- **Replacing `_PLANNER_TABLES` truncation order** — list stays as-is.
_(Migrating dev-DB writers to test fixture is **in scope** — see §6.)_

## Success criteria

1. `time uv run pytest -q` on operator's dev box: full suite finishes in
   **≤ 10 min** (target: 5–7 min).
2. Two concurrent `uv run pytest -q` invocations both complete without
   either deadlocking, hanging > 60 s on each other, or corrupting the
   other's working DB.
3. No regressions: every previously green test still green under
   xdist (modulo #875 + #876 which were already failing).
4. `pytest --durations=20` always-on so future regressions surface
   loudly.
5. `_assert_test_db` invariant remains intact: no test writes to a DB
   other than its assigned `ebull_test_<run>_<worker>`.
   **Documented exception:** `tests/smoke/test_app_boots.py` exists
   specifically to drive FastAPI lifespan against the real dev DB
   (CLAUDE.md "smoke gate" clause). It stays on `settings.database_url`
   by design. To satisfy SC#2 (concurrent invocations must not corrupt
   each other), it is wrapped in a Postgres **session-scoped advisory
   lock** acquired on the maintenance `postgres` DB
   (`EBULL_SMOKE_LIFESPAN_LOCK = 0x65427554534D4B`) before
   `TestClient(app)` enters, released after exit. This lock serialises
   the smoke test across **all pytest invocations** on the same
   Postgres cluster — only one process can drive lifespan migrations
   at a time. The lock acquisition wraps the test body in a
   `try/finally` so a `TestClient` exit-or-raise still releases the
   lock connection (Codex v5 implementation note). xdist_group is
   also applied as a within-invocation pin (defence in depth, makes
   wait-time deterministic). This is the only allowed exception.

## Design

### 1. Add `pytest-xdist` and configure default parallelism

`pyproject.toml`:

- Add `pytest-xdist>=3.5` to `[dependency-groups].dev`.
- Add to `[tool.pytest.ini_options]`:
  - `addopts = "-q --tb=short -n 4 --durations=20 --dist=loadgroup"`
  - `--dist=loadgroup` is needed so the single `xdist_group("dev_db_smoke")`
    marker on `tests/smoke/test_app_boots.py` (the documented
    dev-DB exception, see SC #5) pins it to one worker.
  - **Worker count is `4`, not `auto`.** Codex finding #7: `-n auto`
    saturates Postgres on a high-core-count box and is slower than a
    fixed cap. Empirically tune via the benchmark loop in §8 before
    raising.
- **Do not** use shell-style env interpolation in `addopts` (Codex
  finding #4 — pytest does not expand `${VAR:-default}`). Configure
  `--basetemp` programmatically in the controller-only hook (see §4).

### 2. Per-worker, per-invocation private database

Codex finding #1: two concurrent `pytest` invocations both have `gw0`,
both compute the same DB name, run B's `DROP ... FORCE` evicts run A
mid-flight. Need a **per-invocation run id** mixed into the name.

`tests/fixtures/ebull_test_db.py` rework:

```python
import os, time
from secrets import token_hex

# Set once per pytest invocation in the controller. Workers inherit
# via xdist's worker-spawn env propagation.
_RUN_ID_ENV = "EBULL_PYTEST_RUN_ID"

def _run_id() -> str:
    rid = os.environ.get(_RUN_ID_ENV)
    if rid is None:
        # First call in the controller (before pytest_sessionstart fires).
        # Use a short, filesystem-safe id.
        rid = f"{int(time.time())}_{token_hex(3)}"
        os.environ[_RUN_ID_ENV] = rid
    return rid

def _worker_id() -> str:
    return os.environ.get("PYTEST_XDIST_WORKER", "main")

def test_db_name() -> str:
    return f"ebull_test_{_run_id()}_{_worker_id()}"
```

`test_database_url()` derives from `test_db_name()`. The
~30 test modules calling `test_database_url()` directly pick this up
automatically.

**`TEST_DB_NAME` constant is removed.** All importers move to
`test_db_name()`. Pyright + ruff fail any leftover.

**Postgres database name limit is 63 bytes.** `ebull_test_<10 digits>_<6 hex>_gw15` ≈ 33 bytes — well under cap.

### 3. Migrate `tests/test_operator_setup_race.py` onto the shared fixture

Codex finding #2: this file inlines its own `_TEST_DB_NAME = "ebull_test"`
at line 67 and rebuilds the connect/migrate/truncate path locally. Under
xdist or concurrent invocations, every worker still hits the literal
`ebull_test` and races every other test process.

Action: replace the file's local helpers with imports from
`tests.fixtures.ebull_test_db`. The file's per-test logic (the race-
condition assertions themselves) is preserved; only the DB-bootstrap
plumbing is shared. This was already flagged as a follow-up in the
fixture's own docstring (lines 22-24) and is now the right time.

### 4. Template database with migration-hash invalidation

A new `ebull_test_template` is the master copy. **Build runs only in
the xdist controller process**, never in workers (Codex finding #5).

Controller-only detection in `tests/conftest.py`:

```python
def pytest_configure(config: pytest.Config) -> None:
    is_worker = hasattr(config, "workerinput")
    if not is_worker:
        _build_template_if_stale()
        _set_basetemp(config)
```

Bootstrap sequence inside `_build_template_if_stale()` (controller only):

1. Acquire Postgres advisory lock on the **maintenance `postgres` DB**
   (key `EBULL_TEMPLATE_LOCK = 0x65427554455354` ≈ "eBuTEST"; valid
   Python int literal, Codex finding #8). This serialises template
   build across pytest invocations on the same Postgres cluster.
2. Compute migration hash. Codex finding #6: include filename + bytes,
   sorted by filename, so renames or order changes invalidate the
   template:
   ```python
   import hashlib
   h = hashlib.sha256()
   for path in sorted(SQL_DIR.glob("*.sql"), key=lambda p: p.name):
       h.update(path.name.encode("utf-8"))
       h.update(b"\0")
       h.update(path.read_bytes())
       h.update(b"\0")
   migration_hash = h.hexdigest()
   ```
3. Read stored hash from a small file at
   `~/.cache/ebull/test_template_hash` (cross-platform via
   `platformdirs.user_cache_dir("ebull")`).
4. If `ebull_test_template` does not exist OR stored hash ≠ current
   hash: drop and recreate template, apply all migrations, store new
   hash. Drop must use `WITH (FORCE)` to evict stale connections.
5. Release advisory lock.

Per-worker DB creation (each xdist worker, on first use of the
fixture or on first call to `test_database_url()`):

1. Acquire advisory lock on `postgres` DB (per-worker key derived
   deterministically — `int.from_bytes(blake2b(f"{run_id}:{worker_id}"
   .encode(), digest_size=8).digest(), "big", signed=True)`. Codex v2
   finding #5: Python's built-in `hash()` is salted across processes,
   so workers would compute different keys for the same string. blake2b
   is stable.) Prevents a worker re-running itself from racing.
2. If `ebull_test_<run>_<worker>` exists: `DROP DATABASE … WITH (FORCE)`
   (defensive — should not exist, but cleanup of crashed prior runs).
3. `CREATE DATABASE ebull_test_<run>_<worker> TEMPLATE ebull_test_template`.
   Postgres copies pages directly. Sub-second on local SSD.
4. Release lock.

**Why advisory lock on `postgres` and not `ebull_test_template`:**
PostgreSQL refuses `CREATE DATABASE ... TEMPLATE foo` if **any
connection** is open on `foo`. The advisory lock must be held on a DB
nobody else is connecting to — `postgres` is the natural choice and is
already used by `_admin_database_url()` in the existing fixture.

### 5. Per-worker `basetemp` outside the repo

Codex finding #4: pytest ini does not expand `${VAR:-default}`.
Configure programmatically:

```python
# tests/conftest.py — controller-only path inside pytest_configure
def _set_basetemp(config: pytest.Config) -> None:
    base = pathlib.Path(tempfile.gettempdir()) / "ebull_pytest" / _run_id()
    base.mkdir(parents=True, exist_ok=True)
    config.option.basetemp = str(base)
```

xdist creates per-worker subdirs `gw0/`, `gw1/`, … under `basetemp`.
At session end, Python's `tempfile.gettempdir()` cleans up nothing —
add a session-finish hook that prunes the `_run_id()` subtree.

Old `tmp_pytest/` in repo root: leave gitignored entry, one-shot
`rm -rf tmp_pytest/` cleanup commit.

### 6. Migrate dev-DB-writing tests onto the per-worker test DB

Codex finding #3 (v2): success criterion #2 ("two concurrent pytest
invocations must not corrupt each other") cannot be satisfied while
6 test files still write to `settings.database_url`. xdist groups
only serialise within a single invocation, not across invocations.

Migration is straightforward because the test-DB schema is identical
to the dev-DB schema (the template applies every `sql/*.sql` file).
Two patterns:

**Pattern A — direct connection in the test file.** Replace each
`psycopg.connect(settings.database_url, ...)` with
`psycopg.connect(test_database_url(), ...)` and pass
`test_database_url()` instead of `settings.database_url` into any
class under test that takes a URL string (`HeartbeatWriter`,
`JobLock`).

**Pattern B — connection inside a tested helper.** Codex v3
finding #1: `publish_manual_job_request()` and `publish_sync_request()`
at `app/services/sync_orchestrator/dispatcher.py:83,117` hard-code
`psycopg.connect(settings.database_url, ...)`. They read
`settings.database_url` at call time (verified — module-level
`from app.config import settings`), so tests can `monkeypatch.setattr(
"app.config.settings.database_url", test_database_url())` for the
duration of the test. Apply this monkeypatch via a per-file
autouse fixture in the affected test files. Pattern is the standard
pytest monkeypatch idiom; no production code change needed.

In-scope file list (verified by grep):

- `tests/test_jobs_listener.py`
- `tests/test_jobs_heartbeat.py`
- `tests/test_jobs_queue_recovery.py`
- `tests/test_jobs_queue_boot_drain.py`
- `tests/test_sync_orchestrator_dispatcher.py`
- `tests/test_jobs_locks.py` (read-only probe + URL pass-through into
  `JobLock`; same substitution).

Cleanup blocks (e.g. `DELETE FROM job_runtime_heartbeat WHERE …`) move
to the test DB too, so no rows persist into the operator's dev DB.

Add to `_PLANNER_TABLES` if any of these files write to tables not
yet in the truncate set (audit at implementation time:
`job_runtime_heartbeat`, `pending_job_requests`, anything else
touched by these files).

Extend the existing structural guard at
`tests/smoke/test_no_settings_url_in_destructive_paths.py` to fail if
any test file under `tests/` (excluding smoke tests, the fixture
module itself, and explicit reachability-probe blocks marked with a
recognised comment) calls `psycopg.connect(settings.database_url`.
Re-greppable canary so future contributors don't reintroduce the
violation.

After this PR, **no test writes to the operator's dev DB** except
the documented `test_app_boots.py` lifespan smoke gate, which is
pinned to a single worker.

### 7. Always-on durations + perf gate (deferred)

`addopts` includes `--durations=20`. This prints the 20 slowest
tests every run. Enables the operator to spot regressions visually.

A **soft** perf gate (custom plugin recording per-test wall-clock to
JSON, compared against baseline) is **out of scope here**; tracked as
a follow-up tech-debt ticket.

### 8. Audit pass for serial-order assumptions

Before merging:

- **Global advisory locks** — `app/jobs/locks.py::JOBS_PROCESS_LOCK_KEY`
  is session-scoped on `settings.database_url`. After §6 migration,
  `tests/test_jobs_locks.py` exercises this against the per-worker
  test DB. Per-worker isolation eliminates cross-worker contention
  automatically.
- **Module-globals mutation** — xdist runs each worker as a separate
  process; module globals are per-worker. No action required.
- **`PYTEST_XDIST_WORKER` not set in controller** — `_worker_id()`
  returns `"main"` in the controller; no test should call
  `test_db_name()` from controller-only code (controller has no
  `ebull_test_main_*` DB).
- **Empirical xdist worker count tune** — start with `-n 4`, run the
  full suite, record wall-clock. Step 6 → 8 → auto and pick the knee.
  Numbers go in PR description.

Audit report goes in PR description, not the spec.

### 9. Cleanup of leaked databases and basetemp dirs

Add session-finish teardown that:
- Drops `ebull_test_<run>_<worker>` for the workers this invocation
  owned. The template stays.
- Removes `${TEMP}/ebull_pytest/<run>/` subtree.

Do **not** drop the template at session end — the next invocation
re-uses it for free if the migration hash matches.

A manual cleanup helper (`python -m tests.fixtures.cleanup_test_dbs`)
drops every database matching `ebull_test_*` except the template, for
operator-driven recovery from a crashed run.

## Rollout

1. Feature branch `fix/893-pytest-perf-redesign`.
2. Single PR. Squash-merge OK; this branch has no stacked dependencies.
3. After merge: operator runs `time uv run pytest -q` on dev box and
   records new wall-clock in PR description (Definition of Done clause
   8 from CLAUDE.md).

## Risks and mitigations

| Risk | Mitigation |
|---|---|
| `CREATE DATABASE … TEMPLATE` fails because a connection on `ebull_test_template` is still open | Use `WITH (FORCE)` on DROP; controller closes its admin connection before workers fire. |
| Postgres on operator's box doesn't support `WITH (FORCE)` (PG13+) | Operator confirmed cluster is PG15+. If a teammate has older, fall back to `pg_terminate_backend()` loop. |
| `pg_advisory_lock` numeric key collision with application code | Use a documented constant range; constants live in `tests/fixtures/ebull_test_db.py` and are off-limits to app code. |
| xdist workers race on `schema_migrations` insert | Workers don't migrate — only the controller does. Workers receive a fully-migrated template. |
| New migrations during a long-lived dev session | Hash check at session start triggers template rebuild. Cost is one full migration apply on first run after a new file lands; subsequent runs hit the cache. |
| Tests that used `TEST_DB_NAME` as a string constant | Replaced with `test_db_name()` function. Imports auto-fail at pyright/ruff stage. Audit pass before push. |
| Pre-existing failures #875 + #876 still flake under xdist | Out of scope. Document in PR. |
| Cross-worker schema drift if a test mutates schema (DDL inside a test) | TRUNCATE is not DDL. Audit confirms no test issues `CREATE TABLE` against `ebull_test_*`. |
| Concurrent invocations collide on per-worker DB names | `_run_id()` mixes seconds + 6 hex chars; collision probability ≈ 1 / 16M per second. Acceptable. |
| §6 file migration breaks tests that depend on dev-DB-only state (e.g. existing rows) | Audit each file: every observed pattern is "set up your own row, write, assert, clean up". No file reads pre-existing dev-DB state, so swapping URLs is safe. Verified by reading the 6 files at implementation time. |

## Open questions

None — both Codex passes folded.

## References

- Issue #893: tech-debt ticket.
- `tests/fixtures/ebull_test_db.py:1-363` — current fixture.
- `tests/test_operator_setup_race.py:67` — inlined `_TEST_DB_NAME` to be
  migrated onto shared fixture.
- 6 dev-DB writer files migrated to `test_database_url()` in §6:
  `tests/test_jobs_listener.py`, `tests/test_jobs_heartbeat.py`,
  `tests/test_jobs_queue_recovery.py`,
  `tests/test_jobs_queue_boot_drain.py`,
  `tests/test_sync_orchestrator_dispatcher.py`,
  `tests/test_jobs_locks.py`.
- `.githooks/pre-push:23-46` — current pre-push gate.
- `pyproject.toml:50-54` — current pytest config.
- Codex review session 019df563 (2026-05-04 23:46 UTC) — initial 5
  findings folded.
- Codex review session 019df567 (2026-05-04 23:51 UTC) — 8 findings on
  spec v1, all folded into v2 (this version).
- CLAUDE.md "Codex second-opinion — mandatory checkpoints" section.
