# Jobs boot — liveness before blocking work + bounded outbound calls

**Status:** proposed (2026-06-05). Codex checkpoint-1 reviewed (8 findings folded). Issue: #1479.
**Goal:** a slow/hung outbound call during boot must NOT wedge the whole jobs-process startup (heartbeat threads dark, scheduler silent for ~43 min). Bring liveness up before blocking boot work, make the boot freshness sweep non-blocking-on-main, and bound the one remaining unbounded outbound primitive.

## RCA (code-grounded, 2026-06-04 freeze)

Symptom: after a clean restart the jobs process **wedged in boot ~43 min** — heartbeat threads never started (`beat_age` grew, stale pids retained), no scheduled jobs fired, discovery dark. Self-recovered when the blocking call returned. `sample <pid>` showed the boot thread 100% in `_ssl__SSLSocket_read → PySSL_select` (outbound HTTPS **read** — matches the SDK 600s *read* timeout, not the 5s connect timeout).

Scope note (Codex ckpt-1): `scheduler.start()` (:1068) runs BEFORE the sweep (:1072), so a sweep hang does **not** stop the APScheduler thread itself. The sweep hang **definitively** explains heartbeat-threads-dark + listener-dark (both start after :1072). "No scheduled jobs fired" is a **secondary** effect, not caused by the sweep directly: a `catch_up_on_boot` fire of `daily_research_refresh` hitting the same black-holed Anthropic host occupies the single-worker `_manual_executor`, so subsequent scheduled fires that route through it queue behind the hung call. This spec does not over-assert the exact scheduler mechanism; PR2 (bounding the call) removes the saturation source regardless, and PR1 (liveness-first) removes the heartbeat-dark symptom regardless.

Boot order in `app/jobs/__main__.py::serve()` (actual, not the docstring's 1-13):
- guards (fence, pg_locks, singleton re-seeds, operator, master-key) — all DB-only.
- `try:` body: fence-heartbeat thread → reaper → bootstrap recovery → process_stop sweep → queue stale-row reset → **boot-drain (`_drain_pending_at_boot`, :1054)** → **`runtime.start()` → `_catch_up` (:1067)** → **`run_boot_freshness_sweep()` (:1072)** → test-DB reap → credential-health thread → **heartbeat threads (:1104)** → listener via supervisor.

Dispatch-blocking analysis (verified, not assumed):
- boot-drain `_route_claim` → `_dispatch_manual_job`/`_dispatch_sync_request` both **`.submit()` to executors** (`listener.py:303,446`) — non-blocking on main. ✔
- `_catch_up` is **fire-and-forget** — `_manual_executor.submit()` (`runtime.py:1153`). ✔
- **`run_boot_freshness_sweep` runs `run_sync(SyncScope.behind())` SYNCHRONOUSLY on the main boot thread** (`boot_sweep.py:29-31,37` — its own docstring: "Runs synchronously on the caller's thread"). ✘ This is the inline blocker.

Outbound-timeout audit (whole repo):
- All `httpx.Client(...)` carry `timeout=30.0` (eToro/eToro-broker/SEC-edgar/SEC-fundamentals/companies-house/FINRA ×2/frankfurter/openfigi/broker-credentials/sec-bulk). ✔
- All `urllib.request.urlopen(...)` carry explicit `timeout=` (30–120s). ✔
- eToro WS `websockets.connect(_WS_URL)` (`etoro_websocket.py:806`) bounds auth via `_await_auth_envelope(..., timeout_s=10.0)`; **not reachable from `scope=behind` sync** (no caller in `sync_orchestrator/`/`workers/`). ✔
- **4× `anthropic.Anthropic(api_key=...)` with NO explicit timeout**: `app/api/theses.py:52`, `app/services/sentiment.py:101`, `app/workers/scheduler.py:2510` (`cascade_refresh`), `:2639` (`daily_thesis_refresh`). SDK 0.89.0 default per-request timeout (verified locally): **connect 5s, read/write/pool 600s**, with **auto-retry `max_retries=2`** → ~30 min effective on a black-holed read, approaching the observed 43 min. ✘ **Boot-reachable subset:** only `:2510` (via `daily_research_refresh` → `cascade_refresh`, selectable by `scope=behind`). `theses.py:52` is API-process only; `sentiment.py:101` and `:2639` are not on the verified jobs-boot path. PR2 bounds all 4 anyway (defense-in-depth + single lint rule — any unbounded LLM call hangs its worker thread).

Reachability closing the loop: `sync_orchestrator/registry.py:236` maps `"daily_research_refresh": ("fundamentals",)`; `daily_research_refresh` (`scheduler.py`) drives `cascade_refresh` → unbounded `anthropic.Anthropic(...)`. `SyncScope.behind()` selects DEGRADED/ACTION_NEEDED layers (research is degraded on a fresh dashboard) → `daily_research_refresh` fires → unbounded Anthropic read, **on the main boot thread** (synchronous `run_sync`). → heartbeat threads (:1104) never start.

**Root cause (two independent defects, both required for the freeze):**
1. **Liveness is gated behind blocking boot work.** Heartbeat threads start at :1104, AFTER an inline `run_sync(behind)` that can block arbitrarily long. Process-alive signalling must not depend on boot work completing.
2. **An unbounded outbound primitive exists on a boot-reachable path.** The Anthropic SDK's 600s-×-retries default is the outbound analogue of the DB-side gap the merged `PGCONNECT_TIMEOUT` guard (`a76593d`, #1475) closed.

## Decision

Fix BOTH defects (either alone is insufficient: bounding the call alone still leaves liveness gated behind a legitimately-slow multi-minute behind-sync; reordering alone still lets a worker thread hang indefinitely on the unbounded call). Do NOT reorder the deliberate **drain-before-`scheduler.start()`** invariant (settled-decisions: "queue boot-drain must run before scheduler.start so prior-boot work is replayed, not lost"). Heartbeats are independent of both drain and scheduler — they only need the `HeartbeatWriter` + `stop_event`, both constructed before the `try:` body — so they can move to the top of the body without touching the drain→scheduler ordering.

## Workstreams (each its own PR; ordered by ROI-vs-risk)

### PR1 — liveness before blocking boot work  *(do first; the core fix; low runtime risk)*
**Heartbeat reorder — truthful-health shape (Codex ckpt-1 fold).** Do NOT blindly move all 4 subsystem beats to the top: starting `scheduler`/`manual_listener`/`queue_drainer` beats before those subsystems exist writes *false* subsystem-health. Instead:
- Start the **`main` (process-alive) heartbeat at the top of the `try:` body** (right after the fence-heartbeat, before the reaper). `main` is truthfully alive the moment the process is running — this is the liveness signal the operator stale-detector keys on. (Verify which beat key the Processes stale-detection actually reads before finalising; if it aggregates per-subsystem, the `scheduler` beat must also move to right after `scheduler.start()`.)
- Relocate each **subsystem** beat to immediately after that subsystem starts: `scheduler` beat right after `scheduler.start()` (:1068, still before the sweep); `manual_listener`/`queue_drainer` beats after the listener/executors are wired (near the supervisor at :1138). No beat claims health for a subsystem that isn't up.
- `heartbeat_loop` only writes DB rows via `HeartbeatWriter` (`heartbeat.py:59`) — mechanically safe to start early; the constraint is *semantic* (don't lie), not a dependency.

**Boot-drain → `scheduler.start()` ordering unchanged** (drain-before-scheduler invariant preserved).

**Make the boot freshness sweep non-blocking-on-main.**
- Dispatch the sweep **fire-and-forget onto its OWN dedicated `daemon=True` thread** (NOT inline, NOT the shared `sync_executor`, NOT a `ThreadPoolExecutor`). Rationale folded from Codex ckpt-2:
  - NOT `sync_executor`: it's the listener's sync-request queue (`listener.py:446`) + runs boot-drained sync work — a 43-min sweep there blocks real sync requests. (`_catch_up` uses the *separate* `_manual_executor`, `runtime.py:1153`, so it was never at risk — Codex confirmed.)
  - NOT a `ThreadPoolExecutor`: `shutdown(wait=False)` can't interrupt a running task, and `concurrent.futures`' atexit join would block interpreter exit on a hung sweep — trading boot-liveness for shutdown-liveness. A `daemon=True` thread blocks neither boot nor exit (daemon threads are not joined at interpreter shutdown).
- The thread target wraps `run_boot_freshness_sweep` (which already catches `SyncAlreadyRunning` + honours `EBULL_SKIP_BOOT_SWEEP`) in a broad guard so a residual escape lands in the log, not an unraisable-thread traceback. No future / no `add_done_callback` (avoids the `Future.exception()`-raises-`CancelledError`-on-cancel hazard Codex flagged).
- **Abandon-and-reap safety:** a daemon sweep killed mid-write leaves an orphaned `sync_runs` 'running' row, which the next boot's `reap_orphaned_syncs(reap_all=True)` (step 4) transitions to terminal — the exact recovery path a SIGKILL mid-sweep already relied on. No new finally-block teardown needed.
- Tests: a boot harness where the sweep blocks on an `threading.Event` asserts (1) the `main` heartbeat row advances and (2) `serve()` reaches the listener-supervisor stage without waiting on the sweep. Extend `tests/smoke/test_jobs_process_boots.py` (already drives `serve()` under a controlled stop event).
- **Acceptance:** with an artificially-hung outbound call on the behind-sync path, the `main` heartbeat is alive and the listener/supervisor is up within seconds of boot; the process never presents as dead; no subsystem beat claims health before its subsystem is running.

### PR2 — bound the Anthropic SDK clients  *(small; the outbound-timeout half; independent of PR1)*
- Add an explicit bounded `timeout=` **and** an explicit `max_retries=` to all 4 `anthropic.Anthropic(...)` constructions. Single source of truth: `app/config`-level constants (e.g. `ANTHROPIC_REQUEST_TIMEOUT` as an `httpx.Timeout`, `ANTHROPIC_MAX_RETRIES`) imported at each site — no magic numbers (connection-discipline named-constant rule).
- **These are non-streaming `messages.create` calls (Codex ckpt-1), not streaming** — so a too-short *read* timeout would truncate a legitimate long thesis/research generation. Set a **generous read timeout** (enough for the longest legitimate completion) with a **bounded connect** (the connect phase already defaults to 5s; the failure was the 600s read), and an explicit `max_retries` (0 or 1, not the default 2) so a black-holed read fails in *one* generous window instead of ~3×600s. Per the `claude-api` skill: `anthropic.Anthropic(timeout=httpx.Timeout(connect=5.0, read=R, write=30.0, pool=...), max_retries=N)`. Migrating these calls to streaming + `get_final_message` (the skill's recommended pattern for long output, which gives timeout protection without a hard read ceiling) is a **reasonable alternative** — flag for committee, but the timeout+retries bound is the minimal safe fix and ships PR2 without a behavioural refactor.
- Add a chokepoint lint (mirror `scripts/check_*.sh`): every `anthropic.Anthropic(` construction **under `app/`** must pass `timeout=`. Wire into `.githooks/pre-push`.
- Tests: the lint catches a timeout-less construction; the constants are honoured at each call site.
- **Acceptance:** no outbound LLM call under `app/` can hang a worker thread beyond the bounded read window × `max_retries`; a regression that drops the timeout fails the hook; no legitimate long generation is truncated by the chosen read ceiling.

### PR3 — (optional, committee call) widen the audit to a general "bounded outbound" guard
- Generalise PR2's lint to *every* outbound client constructor (httpx/urllib/anthropic/websockets) asserting an explicit timeout, so a future provider added without one fails pre-push.
- **Scope to `app/` only, with an allowlist** (Codex ckpt-1): `scripts/`, `app/runbooks/` (e.g. `stream_a_run_8_verify.py:484` `httpx.Client(follow_redirects=True)`, no timeout), `app/api/_debug_ws.py`, and `tests/` contain intentionally-unbounded or operator-only outbound calls that are NOT production request/boot paths. A repo-wide guard would false-positive on these. Audit scope claim corrected: the "every `app/` provider/service client carries `timeout=`" finding holds for the production paths; runbooks/debug/tests are out of the boot path and out of the guard.
- Defer entirely if PR2's narrow guard + the existing 30s-everywhere convention is judged sufficient.

## Sequencing
PR1 (liveness — ships the operator-visible fix) → PR2 (bound the call — closes the root outbound gap) → PR3 (optional hardening). PR1 and PR2 are independent and may land in either order; PR1 first because it's the symptom the operator saw.

## Out of scope
- #1478 (sec_rate JobLock mutex starvation) — separate root cause, separate PR.
- #1474 (job_runs telemetry) — separate.
- Raising executor `max_workers` — the unbounded-call fix removes the saturation pressure; sizing is a separate question.

## Discipline checklist (CLAUDE.md)
- Settled-decisions applied: drain-before-`scheduler.start()` invariant (preserved); boot freshness sweep is best-effort recovery (`settled-decisions.md` boot-sweep entries). No settled decision is changed.
- Prevention-log: `prevention-log:1120` (pre-#719 API coupling — "a long-running job that hung an outbound HTTP call left a stranded advisory lock"; #719 moved the work to the jobs process but the *unbounded outbound call* class remained — this PR closes it for the boot path).
- ETL DoD clauses 8-12: N/A — no filings ETL / parser / schema change. (PR1/PR2 touch boot ordering + client timeouts only.)
