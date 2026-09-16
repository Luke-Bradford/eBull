# Dev API wedge — instrumentation before diagnosis (#3119)

Status: proposed (slice 1 of #3119)
Revision: 2 — rewritten after Codex checkpoint 1 returned 50 findings, several of
which falsified claims in revision 1. The corrections are marked ⛔ below.

## Problem

2026-09-16: the dev API accepted connections on `:8000` and answered none.
`POST /auth/login` returned nothing in 90 s, `GET /health` nothing in 10 s.
Postgres was idle and healthy throughout. The uvicorn reloader had also stopped
restarting, so the worker had been serving **stale code for ~10 hours** before it
stopped serving at all. `SIGTERM` was ignored; `SIGKILL` recovered it, destroying
the state that would have identified the blocking frame.

The ticket is explicit: **do not guess at a cause in the fix; instrument first.**
This spec is instrumentation only. Naming the blocking frame is the ticket's
acceptance and needs a captured stack that does not yet exist.

## What is known, and how strongly

Checked against uvicorn 0.43.0 / CPython 3.14.4 as installed in
`/Users/lukebradford/Dev/eBull/.venv`.

1. **Both probed endpoints are synchronous.** `app/main.py:600 def health(...)`
   and `app/api/auth_session.py:193 def login(...)` are `def`, so FastAPI
   dispatches both through the AnyIO worker threadpool. The operator's probe set
   therefore cannot separate *sync-path starvation* (loop alive, threadpool
   tokens exhausted) from *a blocked event loop*.

   ⛔ **Revision 1 said no async dependency-free endpoint exists. That is false.**
   `app/main.py` passes no `docs_url`/`openapi_url` override, so FastAPI's
   defaults are live — verified against the running dev API: `GET /openapi.json`
   → 200 in 0.47 s, `GET /docs` → 200 in 0.0008 s. Both are async and take no
   dependency, and Codex reproduced `/openapi.json` answering while the AnyIO
   limiter was saturated. The discriminator already exists **by accident**. What
   it is not is cheap (`/openapi.json` rebuilds a large schema), stable (either
   URL may be disabled for exactly the fingerprint reasons in §5) or documented.
   `/health/live` makes it explicit, constant-time and contractual — that, not
   novelty, is its justification.

2. **The reloader's unbounded join is a *candidate* mechanism, not the
   established cause.** `uvicorn/supervisors/basereload.py:97-98` is
   `self.process.terminate()` then `self.process.join()` with no timeout, and
   `uvicorn/server.py:286-296` bounds `_wait_tasks_to_complete()` by
   `config.timeout_graceful_shutdown`, which the dev command never sets, so it is
   `None`. A child that cannot finish graceful shutdown would therefore block the
   parent for ever and no later file change would restart it.

   ⛔ **Revision 1 asserted this as the mechanism of the 10 h window. It does not
   survive its own timeline.** uvicorn closes the worker's listeners *before*
   draining requests, so a worker stuck mid-drain would not keep answering new
   requests for ten hours. And the reload parent owns the listening socket, so
   "accepted connections but answered none" is equally consistent with *no live
   worker at all* — the kernel accept backlog absorbs connects regardless. Other
   readings not excluded: the watcher never fired, watched a path that did not
   change, or died. **No parent stack was captured, so this stays a hypothesis.**
   Capturing the parent's stack is part of the acceptance below.

3. **A pool wedge is *not* excluded.** ⛔ Revision 1 claimed `app/db/pool.py:132`
   `timeout=15.0` excluded pool checkout by construction because the observed
   hang was 90 s. It does not: psycopg_pool takes an untimed internal lock and
   invokes its connection-check callback without passing the remaining deadline,
   so either can exceed the nominal bound. Waiting for an AnyIO token also
   *precedes* checkout, and the query, transaction and connection return follow
   it. Pool-related causes remain open.

4. **Stuck teardown during reload races is a recorded, observed class.**
   `app/main.py:405-412` bounds `EtoroWebSocketSubscriber.stop()` at 35 s, and the
   comment cites "a stuck WS task cancel (rare but observed during watcher-driven
   reload races)". Recorded as a **hypothesis for the instrumentation to test**,
   not claimed as the cause. ⚠ `asyncio.wait_for` is not a hard deadline — it
   waits for cancellation to complete, and `stop()` suppresses cancellation around
   several awaits, so even that 35 s is an envelope rather than a guarantee.

5. **The API reports no build identity.** ⛔ Revision 1 said nothing in the tree
   reports the commit it was loaded from. False: `app/jobs/dev_reload.py:327`
   `running_commit()` does exactly that for the **jobs** child (#2666), including
   the `GIT_*` environment scrub that a git hook otherwise uses to redirect
   `rev-parse` at the hook's repository (#2658). The correct narrow claim is that
   **no equivalent exists for the API worker**, and that the new helper should be
   modelled on `running_commit`, not invented.

   `rg faulthandler` over the tree still returns nothing.

## Non-goals

- **Fixing the wedge.** The cause is unknown and the ticket forbids guessing.
- **Bounding graceful shutdown** (`--timeout-graceful-shutdown`). Revision 1
  proposed it; ⛔ **dropped.** Two reasons, the second decisive: the derivation was
  invalid (connection-acquisition limits do not bound request execution, and the
  app serves intentionally long-lived SSE streams via `sse_quotes_router`), and
  more importantly a timeout makes the worker **exit before its stack is
  captured**. That is the same evidence destruction `SIGKILL` caused on
  2026-09-16, automated. It cannot ship in the slice whose purpose is capture.
  Revisit once a dump exists.
- **A new scheduled job.** The dev cluster's connection budget is at zero headroom
  (#1472); a new lane costs two connections. The prober is therefore an external
  script with no DB and no app import.
- **A new alert feed.** Every feed in `app/api/alerts.py` carries a table, an
  `operators` cursor column and frontend work. Disproportionate to one signal.

## Design

### 1. `app/system/git_identity.py` — bounded, hook-safe git reads

Two functions, both of which **never raise** and degrade to `None`:

- `head_commit()` → full `HEAD` sha.
- `app_tree_hash()` → `git rev-parse HEAD:app`, the tree hash of `app/` alone.

Both scrub `GIT_*` from the subprocess environment and pass an explicit `timeout`,
copying `dev_reload.running_commit()`'s hard-won handling of #2658.

**Why the app subtree and not `HEAD`.** The watcher's scope is `--reload-dir app`,
so a docs-only, test-only or spec-only commit moves `HEAD` and correctly produces
no reload. A `HEAD`-based staleness test would flag every such merge as stale —
which in this repo is most of them. `HEAD:app` changes exactly when the watched
tree changes. Verified: `HEAD` `1f53de30…` vs `HEAD:app` `34c73357…` on a commit
touching `app/` and `scripts/`.

⚠ Both are proxies for "which bytes did this worker import", not identities.
Uncommitted edits, a revert to the same tree, and per-module import timing all
break the equivalence. `dirty` (from `git status --porcelain`, same scrub and
bound) is carried so an uncommitted dev-verify checkout is visible rather than
silently reported as fresh.

⛔ **Success-with-empty-output must not collapse into failure** (Codex
checkpoint 2, P3). Revision 2's helper returned `None` for both, which for
`status --porcelain` is exactly wrong: empty output *is* the clean answer, so a
`git status` that timed out reported the checkout as clean — the failure
direction that hides the case `dirty` exists to expose. `_git` now returns `""`
on success-empty and `None` only on failure; `is_dirty()` returns `None` for
failure and never `False`.

⚠ `dev_reload.running_commit()` is deliberately **not** refactored to delegate to
this module in this slice. It is imported by the jobs supervisor, which cannot
reload itself — changing it costs a `launchctl kickstart`, and #2833's hourly
`strategy_core_quote_observations` buckets are mid-flight until the 09-18 verdict
with SPY.RTH already the fragile candidate. The delegation is a two-line follow-up
once that verdict is in; noted on the PR so it is not lost.

### 2. `app/system/served_build.py` — identity + the signal handler

Executed once at import, all of it fail-soft:

- capture `commit`, `app_tree`, `dirty`, `started_at` (UTC), `pid`;
- register `faulthandler` on `SIGUSR1`, `all_threads=True`, writing to an
  append-mode dump file under `resolve_data_dir()`;
- write a **sidecar JSON** next to it recording `{pid, commit, app_tree, dirty,
  started_at, dump_path, faulthandler: bool}`.

⛔ Two claims from revision 1 deleted as false, both verified against CPython
3.14.4: `faulthandler.register` does **not** require the main thread (that is
`signal.signal`), and the module-global file handle is **not** needed to defeat
garbage collection (CPython keeps a strong reference to the file object). The
handle is still module-global, for the reason that does hold: an explicit `close()`
or fd reuse elsewhere would leave the handler writing to a recycled descriptor.

⚠ **One registration writes to one fd.** File *and* stderr is not available
without an explicit tee, so the dump goes to the **file** only — stderr on this
stack is a VS Code task buffer that does not survive.

⚠ **Guarded, not unconditional.** `faulthandler.register` and `SIGUSR1` are POSIX
only, and `.vscode/tasks.json` carries a pwsh arm for Windows. Registration is
skipped when either is absent and the sidecar records `faulthandler: false`.

⚠ Import-time file opening can fail (missing dir, permissions, full disk). Every
failure is caught, recorded in the sidecar, and never raised — an observability
feature must not become a boot failure.

**Why a sidecar rather than an HTTP field.** It solves three problems at once:
a wedged API cannot report its own identity over HTTP; publishing commit, pid and
start time on a public endpoint expands the fingerprint surface that `health_db`'s
docstring (#240) explicitly narrowed; and the prober needs the worker's **pid**
before it can signal anything (§4).

### 3. `GET /health/live` — async, dependency-free, identity-free

`async def`, no DB, no `Depends`, no threadpool hop. Returns `{"alive": true,
"uptime_s": N}` and nothing else, so it adds no fingerprint beyond the liveness
`/health` and `/health/db` already give. Its only job is the loop-alive bit:

| `/health/live` | `/health` | reading |
| --- | --- | --- |
| answers | answers | both paths live (⚠ `/health` may answer **503** and still count) |
| answers | hangs | loop alive, **sync path not completing** |
| hangs | hangs | loop blocked, **or no live worker** (the reload parent's backlog still accepts) |
| hangs | answers | probe straddled a restart, or an intermittent stall — re-probe |

⚠ This table narrows the candidate set; it does not identify a cause. Row 2 is
consistent with AnyIO token exhaustion *and* with `/health`'s own DB work being
slow — `/health` takes a pool connection and runs queries, so it is not a pure
sync-dispatch probe. Row 3 is consistent with a blocked loop *and* with startup,
drain, process suspension or GIL starvation. The dump is what discriminates
within a row; this table only says which dump to go looking for.

### 4. `scripts/probe_api_wedge.py` — the bounded probe

One invocation, no DB, no app import:

1. Probe `/health/live` and `/health`, each with its own **total** deadline,
   no retries, no redirects.

   ⛔ `urlopen(timeout=…)` does **not** give that (Codex checkpoint 2, P2): it
   bounds each individual blocking socket operation, so a peer that accepts and
   then dribbles can overrun the nominal deadline by a multiple. For a wedge
   detector that is the one affordable-looking failure that is not — an overrun
   delays the dump, which is the evidence the exercise exists to capture. The
   fetch therefore runs on a daemon thread bounded by `join`. Covered by a test
   that binds a listening socket with no accept loop, which is the 2026-09-16
   shape exactly.
2. Read the sidecar; compare its `app_tree` against a live `app_tree_hash()`.
   Report `fresh` / `STALE` / `unknown`. ⚠ **Unknown is never fresh** — two
   `None`s do not match, and a missing sidecar, unreadable JSON or failed git
   read all report `unknown` and exit non-zero.
3. On a hang: send `SIGUSR1` **to the sidecar's pid only**, and only after an
   identity check passes. Then print the dump path.

   ⚠⚠ **`SIGUSR1`'s default disposition terminates the process.** Signalling a
   process that did not register — the reload *parent*, an old worker, a reused
   pid — kills it. `pgrep uvicorn` is not an acceptable identification, because
   it matches the parent too.

   ⛔ **Liveness is not identity** (Codex checkpoint 2, P1). The sidecar outlives
   the process it names, so a worker that exits and has its pid recycled leaves
   a record still claiming `faulthandler: true` for a pid that is alive again.
   Two independent checks, both against the sidecar's own record: the
   executable must match, **and** the process must have been running at least as
   long as the record has existed — a recycled pid is necessarily younger than
   the record naming it. Age comes from `ps -o etime=`, a **duration**, chosen
   over `ps -o lstart=` because `lstart` renders in local time and comparing it
   to a UTC field is a standing trap in this repo.
4. Never sends `SIGTERM` or `SIGKILL`. `SIGKILL` is what destroyed the evidence
   on 2026-09-16.
5. Exits non-zero on hang, staleness or unknown, so a caller can gate on it.

⚠ The script is a single bounded invocation. "Periodic" is the caller's job: the
operator runs it, or a loop session does, before trusting a dev-verify result.
No cadence is wired in this slice, deliberately — that would need the job
substrate and therefore a connection lane.

## Acceptance

Slice 1 is done when, on a healthy stack:

- `/health/live` answers and the sidecar's `app_tree` equals `git rev-parse HEAD:app`;
- `SIGUSR1` to the sidecar pid produces a thread dump at the recorded path;
- the probe reports `fresh` and exits 0.

Plus one property asserted rather than claimed: a test that **saturates the AnyIO
limiter** and shows an async handler answering while a sync handler does not —
row 2 of the table above, which is the only row this slice can manufacture.

⛔ The ticket's own acceptance — "capture one wedge with a Python-level stack and
name the blocking frame" — is **open after this slice**. It needs a wedge to
recur, which is wall-clock, not work. ⚠ And one dump may not be enough:
`faulthandler` prints thread stacks, so a *suspended asyncio task* shows only an
idle loop, and a native blocker shows no Python frame at all. Slice 2 is the
diagnosis and starts from whatever the first dump does show — including, per §2,
the reload **parent's** stack, which nothing has ever captured.
