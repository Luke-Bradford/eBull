# Cross-process SEC 10 req/s rate limiter (shared GCRA gate)

Issue: #1484 (`bug` / P1 / `area: ops`). Closes the deferred PR2 of
`docs/proposals/infra/2026-06-05-sec-rate-lane-starvation.md`.
Date: 2026-06-09. Status: spec, pre-implementation.
Predecessor: #1542 (`docs/specs/ops/2026-06-08-sec-rate-lane-dissolution.md`) — dissolved the
jobs-side `sec_rate` JobLock lane and confirmed the **real** rate gate is the per-process HTTP throttle.
Memory: `[[project-sec-rate-lane-wrong-model]]`, `[[project-1542-sec-rate-lane-dissolution]]`.

## 1. Problem

The SEC fair-access rule is **10 requests/second per IP, "regardless of the number of machines"**
(`.claude/skills/data-sources/sec-edgar.md` §4, quoting <https://www.sec.gov/about/developer-resources>).
The budget is **one rolling counter per IP / User-Agent identity** — horizontal scaling buys **zero**
headroom; the only correct enforcement is **one shared counter** across all request sources.

eBull enforces the floor at the HTTP layer via `ResilientClient`
([app/providers/resilient_client.py:118-139](../../../app/providers/resilient_client.py#L118)): a shared
`min_request_interval_s` floor (0.11s → ≤9.09 req/s) over an injected timestamp
`shared_last_request: list[float]` + `shared_throttle_lock`
([resilient_client.py:65-66](../../../app/providers/resilient_client.py#L65)). For SEC, every provider
instance injects the **same process-global** clock `_PROCESS_RATE_LIMIT_CLOCK` + `_PROCESS_RATE_LIMIT_LOCK`
([app/providers/implementations/sec_edgar.py:72-81](../../../app/providers/implementations/sec_edgar.py#L72)),
imported by `sec_fundamentals.py`, `sec_bulk_refresh.py`, `sec_pipelined_fetcher.py`, `sec_bulk_download.py`,
`concurrent_fetch.py`.

**The bug: the clock is a module global, so it is per-PROCESS, not per-IP.** The deploy runs **two**
processes that each make SEC calls (confirmed single-uvicorn-worker API + single jobs process; see §3c):

- **jobs** — the sustained consumer; sweeps run at the ~9 req/s ceiling continuously.
- **API** — `instruments.py` lazy 8-K / 10-K body fills
  ([instruments.py:1410](../../../app/api/instruments.py#L1410), [:1624](../../../app/api/instruments.py#L1624)):
  **exactly one SEC GET per operator click** (`fetch_eight_k_body_now` /
  `fetch_business_summary_body_now` each call `fetcher.fetch_document_text(url)` once —
  [eight_k_events.py:664](../../../app/services/eight_k_events.py#L664),
  [business_summary.py:1081](../../../app/services/business_summary.py#L1081)). Sporadic, operator-driven.

Each process paces to ≤9 req/s **independently**, so the combined rate can momentarily reach ~10–18 req/s
against SEC's single per-IP counter. Escalation (`sec-edgar.md` §4): >10 r/s for minutes → IP rate-limit page
10–30 min; repeated soft-blocks → **IP ban until manual review**. This is the scenario the in-code comment
([sec_edgar.py:69-71](../../../app/providers/implementations/sec_edgar.py#L69), "revive when #479 lands…
Postgres advisory-lock token bucket") and #479 (multi-worker, which would add MORE processes and break even
the API-internal pacing) target.

#1542 made this **more** acute, not less: dissolving the jobs-side lane lets up to 4 SEC jobs run
concurrently, so the jobs process now sits **nearer** the 9 req/s ceiling more of the time → the API's stray
clicks are more likely to tip a given second over 10.

## 2. Goal

Replace the per-PROCESS SEC clock with a **cross-process shared rate gate** so all SEC HTTP requests — from
every process, thread, and any future #479 worker — draw from **one** global ≤9 req/s budget. Correctness
property: at no instant does the IP exceed the SEC 10 req/s ceiling, regardless of process count.

Constraints:
- **Preserve provider DB-purity.** The SEC providers are pure HTTP clients
  ([sec_edgar.py:19-23](../../../app/providers/implementations/sec_edgar.py#L19)). The DB must live in the
  injected gate, never in the provider.
- **No churn at the ~50 provider construction sites** (`grep -c SecFilingsProvider(` ≈ 50, almost all in
  [app/workers/scheduler.py](../../../app/workers/scheduler.py)). A missed site = silent per-process floor =
  bug persists. The wiring must be **set-once at the composition root**, exactly like the current module
  global.
- **No regression to the #1472 connection budget** (27 usable; tight margin).

## 3. Audit findings (the grounding)

### 3a. Correct primitive — GCRA virtual-floor over one shared row

The current algorithm is an **inter-request floor** (`_throttle_and_stamp`: sleep until `last_request_at +
floor`, then stamp `now`). Its cross-process generalization is **GCRA** (generic cell rate algorithm /
"virtual scheduling") — a single advanceable timestamp `next_free_at` instead of a sleep-holding mutex. One
atomic statement per request, row-lock serializes ALL processes + threads, **no sleep under lock, connection
held ~1 ms**:

```sql
UPDATE sec_rate_gate
SET next_free_at = GREATEST(clock_timestamp(), next_free_at) + make_interval(secs => :floor_s)
WHERE budget = 'sec'
RETURNING EXTRACT(EPOCH FROM (next_free_at - make_interval(secs => :floor_s) - clock_timestamp())) AS wait_s;
```

- `next_free_at` (post-UPDATE) = `GREATEST(now, old_next_free_at) + floor`. The returned `wait_s` =
  `GREATEST(now, old) - now` = `max(0, old - now)` — **0 when the gate is idle** (fire immediately),
  positive only when requests are backlogged.
- Caller sleeps `max(0, wait_s)` **after releasing the connection**, then fires the HTTP request.
- **All time math is server-side** (`clock_timestamp()`), so there is no cross-process clock skew: every
  process reads/advances the same DB clock. (`clock_timestamp()`, not `now()`/`transaction_timestamp()`,
  because the value must advance within a transaction and reflect wall-time at statement execution.)
- Strict floor (burst tolerance τ = 0) reproduces today's behaviour exactly. GCRA generalizes to a burst
  allowance trivially (`next_free_at - now ≤ τ`) but YAGNI — keep τ = 0.

This is the "Postgres advisory-lock token bucket" the in-code comment and `[[project-sec-rate-lane-wrong-model]]`
predicted, in its minimal correct form (a single timestamp, no capacity/refill pair to tune).

### 3b. Wiring — process-global settable singleton, zero call-site churn

The provider already reads `_PROCESS_RATE_LIMIT_CLOCK` as a **module global** — no gate is threaded through
the ~50 construction sites today. Mirror that exactly:

- Introduce a module-level injectable `_sec_rate_gate: RateGate` (alongside the existing clock in
  `sec_edgar.py`, or a small sibling module both `sec_edgar` and `sec_fundamentals` import — same pattern as
  the shared clock import today), defaulting to `InProcessFloorGate(_MIN_REQUEST_INTERVAL_S)` (current
  behaviour).
- A `set_sec_rate_gate(gate)` setter, called **once** per process at its composition root, **before any SEC
  provider is constructed**:
  - **API:** `app/main.py` lifespan, immediately after `pool = open_pool(...)`
    ([main.py:251-253](../../../app/main.py#L251)).
  - **jobs:** `app/jobs/__main__.py`, after the jobs pool is opened
    ([__main__.py:68](../../../app/jobs/__main__.py#L68)).
- The provider constructor injects `_sec_rate_gate` into `ResilientClient` instead of the clock list+lock
  ([sec_edgar.py:264-275](../../../app/providers/implementations/sec_edgar.py#L264);
  [sec_fundamentals.py:611-613](../../../app/providers/implementations/sec_fundamentals.py#L611)).

Set-once-at-boot, read-many: a module-global rebind is atomic in CPython; no lock needed. **Zero changes at
the ~50 call sites.** Any unwired process / test / CLI keeps the in-process default and stays correct
single-process.

### 3c. Process topology — two processes today, more with #479

- API = a **single uvicorn worker** (no `--workers`: [Makefile:63](../../../Makefile#L63),
  [stack-restart.sh:141](../../../stack-restart.sh#L141)) → one process.
- jobs = a single process (singleton-fenced `JOBS_PROCESS_LOCK`).
- Both hold a DB pool at every SEC call site (API: `request.app.state.db_pool`; jobs: the jobs pool). So a
  PG gate is reachable in both.
- #479 (multi-worker WS subscriber) would add processes/workers → the in-process clock breaks even
  *within* a tier. The shared gate is **topology-proof**: every worker shares the one `sec_rate_gate` row.

### 3d. Connection budget — borrow-per-acquire adds no steady-state demand

- The gate **serializes globally**: at most ~1 request fires per 0.11s across BOTH processes, so
  gate-connection concurrency is **~1**, even under #1542's 4 concurrent SEC jobs (they are *paced by* the
  gate, not racing it).
- Therefore: **borrow a pooled connection per `acquire()`, held ~1 ms** (UPDATE only; sleep happens after
  release). This adds **no steady-state** demand to `_dev_profile_connection_demand`
  ([app/db/pg_settings.py:255](../../../app/db/pg_settings.py#L255)) — pools are already charged at
  `max_size`; a 1 ms borrow does not raise the max. A **dedicated** limiter conn (rejected) would burn 2 of
  the scarce ~7 headroom conns for a thing used 1 ms at a time.
- **Caveat to verify at implementation (Codex ckpt-1):** in the jobs process the SEC call happens inside a
  job body that already holds 1 body conn; the gate borrow is a transient +1. Because gate concurrency is
  ~1, worst-case transient demand is +1 conn, within margin — but confirm `check_connection_budget`
  ([pg_settings.py:314](../../../app/db/pg_settings.py#L314)) still passes and decide whether the model needs
  a documented +1 transient note (no `max` change expected).

### 3e. Failure mode — gate must fail toward SAFETY, but not wedge

If the gate's DB op raises (transient pool exhaustion, etc.), the request must not (a) burst SEC unthrottled,
nor (b) hard-fail the SEC call. Decision: on gate-DB error, **fall back to a brief in-process floor sleep**
(the `InProcessFloorGate` behaviour) for that single request and log at WARNING. This degrades to today's
per-process pacing for the duration of the outage (no *worse* than current), never to unthrottled. (Pinned in
§6.)

## 4. Design

### 4.1 `RateGate` protocol + two implementations

```python
class RateGate(Protocol):
    def acquire(self) -> None: ...   # blocks (sleeps) until the caller may fire one request

class InProcessFloorGate:            # today's monotonic floor + lock, refactored out of _throttle_and_stamp
    # default; used by tests, CLI, pool-less callers, and as the §3e fallback

class PostgresFloorGate:             # GCRA UPDATE (§3a); borrows a conn per acquire, sleeps post-release
    def __init__(self, pool, *, budget: str = "sec", floor_s: float = _MIN_REQUEST_INTERVAL_S): ...
```

- `RateGate` protocol + `InProcessFloorGate`: DB-free module (e.g. `app/providers/rate_gate.py`).
- `PostgresFloorGate`: the only DB-touching part (imports pool types). On `acquire()`: borrow conn → run the
  §3a UPDATE → release conn → `time.sleep(max(0, wait_s))`. On DB error → §3e fallback.

### 4.2 `ResilientClient` change

Add an optional `gate: RateGate | None` param. When present, `_throttle_and_stamp` (rename → `_throttle`)
delegates to `gate.acquire()` and the legacy `shared_last_request`/`shared_throttle_lock` path is bypassed.
When absent, the existing in-memory floor path is unchanged (back-compat for FINRA / CH / eToro / openfigi,
which keep their own per-process clocks until they adopt the gate — out of scope here). The SEC provider
constructors pass `gate=_sec_rate_gate`.

### 4.3 Schema

`sql/187_sec_rate_gate.sql` — minimal, generalizable by `budget`:

```sql
CREATE TABLE IF NOT EXISTS sec_rate_gate (
    budget        TEXT PRIMARY KEY,
    next_free_at  TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
INSERT INTO sec_rate_gate (budget) VALUES ('sec') ON CONFLICT (budget) DO NOTHING;
```

One row, keyed by `budget`, so FINRA / Companies House (same latent per-process-clock bug) can adopt the same
primitive later by inserting another row — no schema change. The UPDATE is idempotent on re-run; the seed is
`ON CONFLICT DO NOTHING`.

### 4.4 Observability (the memory note's "429/UA-throttle alerting")

`ResilientClient` already logs WARNING on each 429/5xx retry with `Retry-After`
([resilient_client.py:184-196](../../../app/providers/resilient_client.py#L184)). Add a **counter** of SEC
429 / UA-throttle responses surfaced through `/system/...` health (mirror an existing counter surface) and/or
a log-greppable marker, so an actual breach is **visible** rather than silent. Scope: a single counter +
surface; not a new alerting subsystem.

## 5. What changes / what does not

**Changes:**
- New `app/providers/rate_gate.py` — `RateGate` protocol + `InProcessFloorGate` (DB-free).
- `PostgresFloorGate` — GCRA gate (placement: `rate_gate.py` or `app/db/`; decide at plan time).
- `sec_edgar.py` — module-global `_sec_rate_gate` + `set_sec_rate_gate`; provider injects the gate into
  `ResilientClient`. `sec_fundamentals.py` — same injection.
- `resilient_client.py` — optional `gate` param; throttle delegates to it when present.
- `app/main.py` (API lifespan) + `app/jobs/__main__.py` (jobs) — call `set_sec_rate_gate(PostgresFloorGate(pool))`
  once after pool open.
- `sql/187_sec_rate_gate.sql` — the table + seed row.
- 429/throttle counter + health surface (§4.4).
- Tests (§7); this spec; skill fix (§5 below); memory + prevention-log if a lesson surfaces.

**Skill correction (skill-ownership rule — same PR).** `sec-edgar.md` §4 currently states the in-process
`_PROCESS_RATE_LIMIT_CLOCK` enforces 10 r/s "across every ingest job" — that is **per-process only**. Correct
it to: the per-process clock bounds a single process; #1484 adds the cross-process `sec_rate_gate` GCRA gate
that makes the budget truly global per-IP.

**Does NOT change:** the SEC providers stay pure HTTP clients (DB lives in the injected gate); the ~50
`SecFilingsProvider(...)` call sites are **untouched** (§3b); FINRA / Companies House / eToro / openfigi keep
their own per-process clocks (their cross-process exposure is lower-volume and out of scope — file follow-ups
if needed); no change to `_MIN_REQUEST_INTERVAL_S = 0.11` (now genuinely global, safely under 10 r/s);
no change to the #1542 in-process SEC-job semaphore (orthogonal — it bounds *jobs*, this bounds *requests*).

## 6. Edge cases / invariants

- **Fire-immediately when idle:** `wait_s = max(0, old_next_free_at - now)` = 0 on an idle gate → no added
  latency to the rare API click when jobs are quiet. The 8-K/10-K lazy fill stays ~0.5–1 s.
- **No sleep under lock / no held conn:** the conn is released before `time.sleep(wait_s)`. The row lock is
  held only for the single UPDATE.
- **Gate-DB failure (§3e):** fall back to a one-shot in-process floor sleep + WARNING; never burst, never
  hard-fail the SEC request.
- **Set-once ordering:** `set_sec_rate_gate` must run before the first provider construction in each process
  (API lifespan after pool open; jobs after pool open). A provider built before the setter (or in a test)
  uses the `InProcessFloorGate` default — correct, just single-process.
- **Backlog fairness:** under contention the jobs process's continuous UPDATEs and the API's single UPDATE
  interleave at row-lock granularity; the API request waits ≤ a few × floor (sub-second), never starves.
- **`clock_timestamp()` vs `now()`:** must use `clock_timestamp()` (advances within a tx, real wall-time);
  `now()`/`transaction_timestamp()` are fixed at tx start and would mis-compute under a long-lived tx.

## 7. Testing (lean — pure-logic + one DB-concurrency check + smoke)

Pure-logic (no DB):
- `InProcessFloorGate.acquire` enforces the floor (monotonic-clock injected/faked) — spacing ≥ floor across N
  calls; idle gate → no sleep.
- The GCRA `wait_s` algebra as a **pure function** (`compute_wait(now, next_free_at, floor)`), table-tested:
  idle → 0; backlogged → positive; advance is monotonic. (Separates the arithmetic from the psycopg path.)
- `ResilientClient` delegates to the injected gate when present; uses the legacy floor when absent.

DB-backed (one only, `-m db`):
- Two threads sharing one `PostgresFloorGate` over the same row issue M acquires; assert the **observed inter-
  fire spacing ≥ floor** (the cross-process property the in-process test cannot cover). One mechanism, one
  test — per the lean-tests rule.

Plus:
- `_dev_profile_connection_demand` / `check_connection_budget` assertion stays `≤ usable` with margin ≥ 1
  (no `max` change expected; pin it).
- Smoke (`tests/smoke/test_app_boots.py`) — lifespan wires the gate without error.

## 8. Rollout + dev-verify

No data migration; one additive table. Land → run `sql/187` → restart **both** processes onto the new SHA
(jobs via the operator-approved `kill -9` + `nohup uv run python -m app.jobs` method; API via `stack-restart.sh`).

**Dev-verify (operator-visible):**
1. From a side script, hammer the `PostgresFloorGate` from **two processes** at once; confirm observed
   global fire-rate ≤ ~9 req/s (not ~18).
2. Drive jobs at the ceiling (a manual SEC sweep) **and** trigger an API 8-K lazy fill
   (`GET /instruments/{sym}/8-k/{accession}` on a `body_deferred` filing); confirm via SEC request logging /
   the §4.4 counter that the combined second never exceeds 10, and the click still returns ~≤1 s.
3. Kill the gate's DB access (point the pool at a closed conn) for one request; confirm §3e fallback logs
   WARNING and the request still completes (degraded to in-process floor), not a 503 burst.
4. `/system/postgres-health` connection count stays under budget during a 4-concurrent-SEC-job + API-click
   window.

## 9. Out of scope (YAGNI)

- Migrating FINRA / Companies House / eToro / openfigi to the shared gate — lower-volume, separate per-IP
  budgets; file follow-ups if their cross-process overlap is ever shown to matter.
- A burst allowance (GCRA τ > 0) — strict floor matches today; trivially added later if steady-state shows
  the floor is too coarse.
- Distributed/multi-host coordination beyond one Postgres — the gate row already covers "regardless of
  machines" as long as all processes share the one DB (they do).
- #479 itself (multi-worker subscriber) — this spec only ensures the gate is topology-proof for when it lands.

## 10. Open verification items for implementation (Codex ckpt-1 targets)

1. Confirm the exact GCRA UPDATE returns the intended `wait_s` (sign, units) and that `clock_timestamp()` is
   correct vs `now()`; verify on PG17 against a 2-thread harness.
2. Confirm `set_sec_rate_gate` runs before the first SEC provider construction in BOTH composition roots
   (re-grep for any earlier provider build in API lifespan / jobs boot).
3. Confirm `_dev_profile_connection_demand` + `check_connection_budget` margin ≥ 1 holds with borrow-per-
   acquire (no `max` change); decide whether to document the +1 transient.
4. Confirm the legacy `shared_last_request` path stays byte-for-byte unchanged for non-SEC providers (no
   accidental behaviour change to FINRA/CH/eToro).
5. Re-grep that no non-jobs/non-API process constructs a SEC provider expecting the in-process clock in a way
   the global-gate swap would change (scripts under `scripts/`).
