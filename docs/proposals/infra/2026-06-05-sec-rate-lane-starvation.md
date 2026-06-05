# SEC discovery starvation — de-conflate the JobLock lane from the rate budget

**Status:** proposed (2026-06-05). Codex ckpt-1 + **8-lens committee** reviewed. **Verdict: REJECT the full Option-B split; ship the minimal `sec_manifest_worker`-only lane extraction** (see "Decision" + the consolidated findings memo `project_1478_sec_rate_consolidated_findings.md`). Issue: #1478.
**Goal:** stop `sec_manifest_worker` perpetually starving the SEC discovery/producer jobs (`sec_atom_fast_lane`, `sec_per_cik_poll`, `sec_filing_documents_ingest`, `sec_insider_transactions_backfill`, …) — WITHOUT raising the SEC 10 req/s per-IP request rate.

## RCA (code-grounded — corrects the issue's premise)

Symptom (issue #1478, verified from jobs stdout): every `:X0/:X5` boundary, `sec_atom_fast_lane` (and the other SEC producers) skip with `another instance is already running (lock held by … earlier overrunning fire)`. Since 2026-06-04 restart: `sec_manifest_worker` 7/7, `sec_atom_fast_lane` 0/7.

`JobLock` is **lane/source-keyed**, not job-name-keyed: `pg_try_advisory_lock(hashtext('job_source:{lane}')::int)` (`app/jobs/locks.py:248,306`). ALL SEC-hitting jobs share lane `sec_rate` (`app/jobs/sources.py`: `sec_rebuild`, `daily_research_refresh`, `filings_history_seed`, `sec_submissions_files_walk`, `sec_form3_ingest`, `sec_13f_quarterly_sweep`, `sec_n_port_ingest`, `sec_n_csr_bootstrap_drain`, plus the scheduled `sec_manifest_worker` / `sec_atom_fast_lane` / `sec_per_cik_poll` / `sec_filing_documents_ingest` / `sec_insider_transactions_backfill` / `sec_daily_index_reconcile`). `sec_manifest_worker` fires the same `:X0/:X5` boundary, runs 20-37s (per the log, **mostly DB tombstoning — not SEC calls**), acquires the shared lock ~10ms before the producers, and holds it for its **entire** run → the producers find it held and skip. The drainer perpetually starves the producers.

**The premise correction (the pivotal finding):** the issue frames the `sec_rate` mutex as existing "purely to bound SEC request rate." **It is not the rate enforcer and cannot be** — a job-granularity advisory lock counts *jobs*, not *requests*. The actual 10 req/s budget is enforced at the **HTTP layer**, independently of the JobLock:

- `app/providers/implementations/sec_edgar.py:55-81`: `_MIN_REQUEST_INTERVAL_S = 0.11` (≈9 req/s, conservative) + a **process-wide shared timestamp** `_PROCESS_RATE_LIMIT_CLOCK` + companion `threading.Lock` `_PROCESS_RATE_LIMIT_LOCK` (#537 / #726). Every `SecFilingsProvider` / `SecFundamentalsProvider` in the process funnels its `ResilientClient(min_request_interval_s=…)` through this one atomic read-modify-write floor. The docstring is explicit: it exists so **"8-thread concurrent fetchers"** cannot burst past the ceiling — i.e. concurrent SEC fetching within one process is *already a designed, rate-safe mode*.

So two mechanisms got conflated:
- **Lane (`sec_rate`)** = JobLock key → currently a *job-overlap mutex* across all SEC jobs.
- **`_PROCESS_RATE_LIMIT_LOCK`** = the *real* 10 req/s budget, per-HTTP-call, held only microseconds, concurrency-safe.

**Removing the cross-job mutex cannot raise the request rate** — the HTTP throttle is untouched and bounds the jobs-process aggregate to ≤9 req/s regardless of how many SEC jobs run concurrently. (Codex ckpt-1 verified TRUE in-process: `SecFilingsProvider` + `SecFundamentalsProvider` pass the *same* `_PROCESS_RATE_LIMIT_CLOCK` + `_PROCESS_RATE_LIMIT_LOCK` into `ResilientClient`, and `_throttle_and_stamp()` (`resilient_client.py:135`) holds that lock across the read→sleep→write of the timestamp, so the 0.11s floor is genuinely global across threads. Precise wording: it is the sole *inter-request floor*; retry/backoff also sleeps but only *slows* traffic — neither bursts.)

## Decision (8-lens committee): ship the minimal `sec_manifest_worker`-only lane extraction

**The full N-way per-job split (Option B) is REJECTED as over-scoped/risky.** Unanimous committee + Codex-CTO verdict: extract ONLY `sec_manifest_worker` — the proven hog — into a new dedicated lane `sec_manifest`, leaving the producers on `sec_rate`. Why:

- `sec_manifest_worker` is *the* blocker: 7/7 fires vs producers 0/7; it holds the lock 20-37s on **mostly DB tombstoning** (barely uses SEC) yet sits in the SEC lane → it is mis-laned. Removing it un-starves every producer.
- **Verified: `sec_manifest_worker` is NOT a bootstrap stage** (zero hits in `bootstrap_orchestrator.py`). Its new lane never lands in `bootstrap_stages.lane`, so the extraction **avoids every BLOCKING blast-radius gap the full split would hit** (see below).
- Blast radius: 1 lane literal + 1 `SCHEDULED_JOBS.source` change + Lane-type mirrors + stale-text fixes + 4 tests. The full split would touch ~22 jobs, 13 `scheduler.py` `source="sec_rate"` sites, a SQL CHECK migration, `_LANE_MAX_CONCURRENCY` (a hidden 2nd serializer), `_STAGE_LANE_OVERRIDES`, 3 non-aliased `Lane` Literal types, and ~20 tests.
- Operator has 8 SEC jobs dark NOW → time-to-relief dominates.

The per-job lock semantics for `sec_manifest_worker` become "no two instances of `sec_manifest_worker`" (the correct overrun/double-dispatch guard) instead of "no SEC job while the worker runs." Its SEC fetches still go through the unchanged 0.11s HTTP throttle.

**Accepted residual (committee + Codex-CTO):** this does NOT prevent producer-vs-producer contention — the producers still share `sec_rate` and can occasionally collide at shared `:X0/:X5` boundaries. But they're short discovery jobs, not the perpetual manifest-caused freeze. If producer starvation surfaces post-fix (detectable via #1474 telemetry), split them in a follow-up.

### BLOCKING gaps the full split would hit — AVOIDED by the minimal extraction (committee)

- **`_LANE_MAX_CONCURRENCY={'sec_rate':1}`** (`bootstrap_orchestrator.py:245,2052`) is a SECOND, load-bearing serializer for SEC *bootstrap* stages, independent of JobLock. A full split propagating new lanes into bootstrap would let SEC stages run concurrently for the first time and/or trip the boot-time conflict guard `_build_job_name_to_source` (requires `SCHEDULED_JOBS.source == _effective_lane(bootstrap stage)` → split one without the other = **process won't boot**). `sec_manifest_worker` isn't a bootstrap stage → N/A.
- **`bootstrap_stages.lane` CHECK constraint** (`sql/165…`) needs an `ALTER` migration for any lane WRITTEN to that table. `sec_manifest` is never written there → **no migration**.
- **Three non-aliased `Lane` Literal types** (`sources.py:62`, `api/bootstrap.py:69 LaneApi`, `bootstrap_state.py`) drift silently (independent `Literal`s, not imports). Minimal extraction still adds `sec_manifest` to `sources.py::Lane` + `_ALLOWED_SOURCES`; verify whether `LaneApi`/`bootstrap_state.Lane` must mirror a non-bootstrap lane (likely only the bootstrap-written lanes need them — confirm at impl).

**NOT in scope (deferred, separate issue):** a **cross-process** rate limiter. The API process also makes SEC HTTP calls, and `_PROCESS_RATE_LIMIT_*` is **per-process** — so API SEC calls + jobs SEC calls are each independently throttled to ≤9 req/s and could sum >10 req/s. This is the scenario the issue's "Postgres advisory-lock token bucket" instinct (and the `sec_edgar.py:69-71` "revive when #479 lands" comment) actually targets. **But it is pre-existing, independent of the JobLock (which never coordinated with the API), and NOT the cause of the discovery freeze.** Codex ckpt-1 bounded the API surface: `capability_overrides_admin.py` is **metadata-only (no SEC HTTP)**; the real API SEC calls are `instruments.py` **lazy 8-K / 10-K body fills** (`instruments.py:1350,1552`) — user/request-driven, not sustained sweeps. So the cross-process overlap is bursty-at-worst and deferring is not a regression. File a follow-up for the cross-process token bucket.

## De-risks confirmed by Codex + committee (do NOT re-investigate)

- **Re-entrancy: no blocker.** No `sec_rate` job dispatches another `sec_rate` job synchronously in-context relying on the `_HELD_SOURCES` same-lane bypass (sync orchestrator maps only `daily_research_refresh`; adapters call a single `JobLock(job_name)` at `adapters.py:103`). Moving `sec_manifest_worker` to its own lane cannot break any sibling-dispatch path.
- **Manifest producer/worker race: safe.** `sec_atom_fast_lane` writes 'pending' via `record_manifest_entry` (no `ingest_status` update on conflict); `sec_manifest_worker` drains via `transition_status` (row-locked, `sec_manifest.py:374`). No clobber.
- **Single-`sec_manifest_worker`-instance is load-bearing (Codex spec-bug catch):** the drains (`iter_pending`/`iter_retryable`, `sec_manifest.py:537`) do **NOT** use `FOR UPDATE SKIP LOCKED`. The one-instance guarantee comes from the **per-job self-lock**. The new `sec_manifest` lane has exactly one member → one lock → one instance, preserving it. **Test (ii) below pins this.**
- **`acquire_shared_source_locks`** (`watermarks.py:703-735`) — a source-keyed xact advisory lock INDEPENDENT of `Lane` — already serializes concurrent SEC freshness/manifest RMW. Other data races (`sec_rebuild` vs `sec_per_cik_poll` on `data_freshness_index`; `institutional_filers.last_13f_hr_at`) are atomic-single-statement / monotonic-UPSERT / disjoint → benign.
- **Retry backoff does NOT hold the throttle lock** (`resilient_client.py:135-139` vs the send/sleep at `:167,:197`) → a 429 on the worker can't stall producers. **Conditional-GET premise dropped:** the mutex never dedup'd duplicate fetches, it serialized them — no regression there.
- **Rollback clean:** lanes are `Literal` members + string map entries; advisory keys are `hashtext`-computed at runtime, never stored. Revert = restore the literal. No migration/orphan.

## Workstreams

**PR1 — extract `sec_manifest_worker` into lane `sec_manifest`:**
1. `app/jobs/sources.py`: add `"sec_manifest"` to `Lane` + `_ALLOWED_SOURCES`; correct the stale "rate bucket" docstrings — `sources.py:84-86` ("every per-CIK fetch competes here") and `app/jobs/locks.py` module docstring — so the next reader doesn't re-add the mutex believing it's the rate gate.
   - **`api/bootstrap.py::LaneApi` + `bootstrap_state.py::Lane` do NOT get `sec_manifest` — VERIFIED at impl (pyright-clean + empirically).** Those two `Literal`s are the **`bootstrap_stages.lane` row-shape** vocabulary (legacy `sec` catch-all + bootstrap-relevant family lanes); they already intentionally diverge from `sources.py::Lane` — they OMIT the non-bootstrap scheduled lanes `finra`, `bootstrap`, `openfigi`, and carry a legacy `sec` that `Lane` does not. `sec_manifest_worker` is a scheduled (non-bootstrap) job, so `sec_manifest` is never written to `bootstrap_stages.lane` → it correctly does NOT belong in those two types. (This also means a `set(LaneApi.__args__) == set(Lane.__args__)` equality test would FALSE-FAIL today on `finra`/`bootstrap`/`openfigi` — they are deliberately distinct axes, not copies. The genuine drift guard — every *bootstrap-stage* lane ∈ `LaneApi` ∩ `bootstrap_state.Lane` ∩ the sql CHECK — is pre-existing and broader than #1478; deferred to a tech-debt issue **#1486**.)
2. Re-point `sec_manifest_worker`'s `SCHEDULED_JOBS` entry: `source` `sec_rate` → `sec_manifest`.
3. Tests (reuse `tests/test_joblock_per_source.py` template; CI runs NO pytest → see DB-free note): (i) `sec_manifest_worker` + `sec_atom_fast_lane` acquire CONCURRENTLY (different lanes, no mutual `JobAlreadyRunning`); (ii) two `sec_manifest_worker` mutually exclude — **must be CROSS-THREAD** (same-context same-lane hits the #1184 re-entrancy bypass → false-pass); (iii) the HTTP throttle floor still holds ≤9 req/s across N threads (reuse `test_sec_rate_limit_clock.py` import + drive `_throttle_and_stamp`, assert inter-arrival ≥ ~0.10s floor-with-slack); (iv) **DB-free** `test_sec_manifest_distinct_lane` regression pin (registry is in-memory → runs even when dev PG is down / under `--no-verify`, the only always-on gate since CI has no pytest).
4. Update the breaking registry tests preserving power (assert the EXACT new lane, not a wildcard): `test_workers_scheduler_registry.py:341`, `test_layer_123_wiring.py`, `test_job_registry.py:169/282` + `_ALLOWED_SOURCES` (`:37`). Plus the doc table `docs/wiki/job-registry-audit.md` (Codex: `sec_manifest_worker` row).
5. File the **cross-process SEC token-bucket follow-up issue (P1)** — filed as **#1484**; cite its number in this spec + the `sec_edgar.py:69-71` comment. (Codex CTO: P1, not ship-blocking for the minimal extraction, but the split makes the jobs process sit nearer the 9-req/s ceiling so the API+jobs cross-process overlap matters more — defer only WITH the filed follow-up.)

**Optional / committee suggestion (decide at impl):** bump `_MIN_REQUEST_INTERVAL_S` 0.11 → ~0.12s (≤8.3 req/s) for headroom, since the floor is release-timed not receive-timed (~9% headroom is thin for an IP-ban-risk source). Low-cost insurance; only if it doesn't materially slow discovery.

**PR2 (deferred follow-up issue)** — cross-process SEC token bucket (Postgres advisory-lock or shared limiter) so API + jobs share one 10 req/s budget. The #479 / `sec_edgar.py:69-71` concern.

## Sequencing (committee + Codex CTO)
Ship PR1 (minimal extraction) **before #1474** — #1478 is the root-cause fix for #1474's false-"SCHEDULE MISSED" SEC cohort (a skipped scheduled fire writes NO `job_runs` row → `expected_fire_at` recedes → false stale). Land #1474 telemetry immediately after to (a) prove the fix and (b) detect any residual producer-vs-producer starvation.

## Discipline (CLAUDE.md)
- Settled-decisions: the `sec_rate` shared-budget invariant is PRESERVED (HTTP throttle unchanged); only `sec_manifest_worker`'s lane membership changes. `settled-decisions.md:610-612` is about OpenFIGI not cannibalising the budget (disputed-but-likely NOT stale) — read + decide at impl; do not assume it needs editing.
- ETL DoD 8-12: job-concurrency change, no parser/schema/ownership data change → clauses 8-12 N/A. The same-job-mutex test (ii) is the guard against dup fetch/parse; no smoke panel unless a drain codepath changes.
- Prevention-log: #1226 (JobLock contention), #678 (lock contention) — same family; extract the **"a job-granularity lock is NOT a request-rate limiter; the HTTP throttle is the rate gate"** lesson into the data-engineer skill on merge.
- Codex ckpt-2 before first push (per CLAUDE.md checkpoint 2).
