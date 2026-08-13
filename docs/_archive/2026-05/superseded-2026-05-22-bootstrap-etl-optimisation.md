# Bootstrap ETL — comprehensive optimisation spec

**Date:** 2026-05-22
**Author:** Claude Opus 4.7
**Status:** DRAFT — pending clean-agent adversarial review
**Baseline:** run #4 cancelled at ~110 min, projected 4-5 h end-to-end. Three concurrent audits (data-engineer + edgar + adversarial) confirmed pathologies.

---

## 1. Problem statement (operator's words)

> 2-3 hours is nuts. Powerful machine, fast internet. The only bottleneck should be SEC rate limits. Cold backfill AND regular daily updates. Robust. Complete coverage. Minimum code clutter. Don't settle for first idea — refine.

Translated to engineering targets:

| Target | Today | Acceptance |
|---|---|---|
| Cold-install wall-clock | ~4-5 h | **≤ 45 min** |
| Daily-refresh wall-clock | N/A (re-runs cold path) | **≤ 5 min** |
| CUSIP coverage at completion | 33.82% (silent) | **≥ 80% (floor-gated)** |
| Ownership categories populated at "complete" | 3 of 10 | **10 of 10** |
| Operator-visible drop telemetry | INFO log only | **Admin panel + per-category floors** |
| Process-crash recovery | Manual remediation | **Auto-reap stuck `running` stages** |
| Wasted re-download on re-bootstrap | Full 5.7 GB | **0 bytes if SHA matches** |

---

## 2. Findings (audit synthesis)

Convergent across all 3 audits:

1. **`wait(all_futures)` at `bootstrap_orchestrator.py:1720` serialises lanes.** `sec_rate` lane idle 80+ min while `db` lane churned. The single biggest wall-clock waste.
2. **Per-row INSERT + per-row SAVEPOINT in `sec_13f_dataset_ingest.py:352-385` + `sec_nport_dataset_ingest.py:419-447`.** Caps throughput at ~1500 rows/sec when COPY+ON CONFLICT yields 50,000+ rows/sec. Defends against malformed rows that are already pre-filtered upstream.
3. **CUSIP coverage 33.82% silently proceeds.** `BOOTSTRAP_MIN_CUSIP_COVERAGE_RATIO` defaults to 0.0. Run #4 dropped 12.7M rows across 13F + N-PORT. OpenFIGI fallback unwired despite settled-decisions allowance.
4. **Bulk ingester write-through is broken.** `record_institution_observation` writes only to `_observations`, never calls `refresh_*_current`. Operator UI panels are empty until Phase E (`ownership_observations_backfill`) runs at the end.
5. **`_preflight_cleanup_stale_partials` deletes complete .zips on every run.** No SHA-keyed reuse. Costs operator a full re-download (5.7 GB) on every bootstrap.
6. **`sec_filing_manifest` carries 1.18M rows from cancelled run #3.** No reset of `failed` rows; `next_retry_at` backoffs survive into the new run.
7. **Stages 14, 16, 17, 18, 19, 20, 21 redundant with bulk path + manifest worker.** Coverage provided by S7-S12 + steady-state manifest drain.
8. **`_CAPABILITY_MIN_ROWS` has no entry for `cik_mapping_ready`.** A CIK refresh that wrote zero rows still satisfies the cap → every downstream CIK-keyed write silently drops.
9. **No `If-Modified-Since` on bulk-archive HEAD.** Daily refresh of submissions.zip + companyfacts.zip not scheduled.
10. **No reaper for orphaned-`running` bootstrap stages.** Process crash mid-stage leaves the run stuck; operator must manually intervene.

Divergent (resolved):

- Data-engineer claimed `cik_refresh` is per-CIK serial. **Edgar correct: it's 3 JSON pulls (~30s).** Not a bottleneck.
- Adversarial claimed filer directory syncs missing as cap providers. **Edgar correct: they already cover the use case.** Add cap explicitly anyway for defensiveness.

---

## 3. Acceptance criteria — measurable

Cold bootstrap from clean DB on operator's machine (host stats: ~111 Mbps download, ample RAM, PG17 local docker):

```
ASSERT bootstrap_runs.status = 'complete'
ASSERT bootstrap_runs elapsed ≤ 45 min
ASSERT all 26 stages.status IN ('success', 'skipped')
ASSERT NO stage.status = 'cancelled' UNLESS operator triggered
ASSERT external_identifiers WHERE provider IN ('sec','openfigi') AND identifier_type='cusip' ≥ 0.80 × COUNT(instruments WHERE is_tradable)
ASSERT ownership_institutions_current rows > 0
ASSERT ownership_funds_current rows > 0
ASSERT ownership_insiders_current rows > 0
ASSERT ownership_blockholders_current rows > 0
ASSERT ownership_def14a_current rows > 0
ASSERT ownership_esop_current rows > 0
ASSERT ownership_treasury_current rows > 0
ASSERT financial_facts_raw rows > 0 (companyfacts ingest)
ASSERT instrument_share_count_latest has rows for 5-instrument smoke panel (AAPL/GME/MSFT/JPM/HD)
ASSERT GET /instruments/AAPL/ownership-rollup returns non-empty within 1s
ASSERT bootstrap_archive_results aggregates show drop rate < 25% per archive
```

Daily refresh (already-bootstrapped DB, simulate 100 new accessions):

```
ASSERT scheduled atom + daily-index + per-CIK poll discovers all 100
ASSERT manifest worker drains them within 5 min
ASSERT no bulk archive re-download triggered
```

Robustness:

```
ASSERT mid-archive cancel observed within 30s (not 15 min)
ASSERT process-crash mid-stage: next jobs restart auto-resumes the stage
ASSERT operator wipes DB but keeps /sec/bulk/*.zip: bootstrap skips re-download (SHA match)
ASSERT CUSIP coverage at 30% pre-flight: bootstrap refuses to start with structured reason
```

---

## 4. Plan — ordered PRs (Tier 1)

Each PR is independent. CI must pass + Codex 2 review + bot APPROVE before merge. Order is by safety + dependency, not size.

### PR-1: CUSIP coverage hardening + OpenFIGI fallback resolver

**Scope (~500 LoC):**

- New service: `app/services/openfigi_resolver.py`
  - Adapter over `https://api.openfigi.org/v3/mapping`
  - Public API: 25 req/min unkeyed, 250 keyed. Read `OPENFIGI_API_KEY` env var; degrade to 25 req/min if absent.
  - Batch endpoint accepts ≤100 mappings per POST. Maps `(idType: TICKER, idValue: <ticker>, exchCode: <US|UN|UQ>)` → CUSIP.
  - Writes resolved CUSIPs to `external_identifiers (provider='openfigi', identifier_type='cusip', is_primary=FALSE, instrument_id, identifier_value)`.
  - Idempotent ON CONFLICT.
- New bootstrap stage `S3.5: openfigi_cusip_fallback`
  - Runs after `cusip_universe_backfill` (S3), before bulk ingest.
  - Targets only instruments with `is_tradable=TRUE` AND no row in `external_identifiers WHERE identifier_type='cusip'`.
  - Provides capability `cusip_mapping_lifted` (new) — orthogonal to existing `cusip_mapping_ready`.
- New preflight `assert_cusip_coverage` enforcement
  - Change `bootstrap_preconditions.DEFAULT_MIN_CUSIP_COVERAGE_RATIO` default `0.0` → `0.80`.
  - Move the check from log-only to a true pre-flight in `_assert_phase_C_preconditions` so S10/S12 refuse to dispatch if floor not met.
  - Structured reason: `"CUSIP coverage X% below floor 80%; openfigi_cusip_fallback ran but lifted only Y CUSIPs; investigate ticker→FIGI gap for top-N missing tickers (list)"`.
- Skill update: `.claude/skills/data-sources/sec-edgar.md` §3.2 — OpenFIGI added as documented fallback path.
- Skill update: `.claude/skills/data-engineer/SKILL.md` — `_CAPABILITY_MIN_ROWS` discipline + CUSIP-cliff quantification.

**Why first:** without CUSIP coverage, every subsequent perf win produces 33% of the truth faster. Floor-gate is the contract.

**Risk:** OpenFIGI rate-limit / availability. Mitigation: degrade gracefully — log warning if OpenFIGI is down, proceed to pre-flight check; if coverage at 80%+ from existing SEC List, proceed; if below, block with operator-actionable error.

**Test plan:**
- Unit: `openfigi_resolver` against recorded HTTPS fixtures.
- Integration: stage runs against the 12k-instrument dev DB; asserts coverage lifts from 33% to ≥80%.
- Smoke: AAPL, GME, MSFT, JPM, HD all have CUSIPs post-stage.

### PR-2: Dispatcher parallelism — replace `wait(ALL_COMPLETED)` with `as_completed` poll

**Scope (~250 LoC):**

- `app/services/bootstrap_orchestrator.py:1665-1730` — replace the `lane_executors + wait(all_futures)` pattern with:
  - One persistent `ThreadPoolExecutor` per lane, sized to `_LANE_MAX_CONCURRENCY[lane]`.
  - A single `as_completed(in_flight_futures)` loop that processes one completed future at a time.
  - After each completion: update `statuses`/`rows_processed`, recompute `caps`, find newly-ready stages, dispatch them onto their lane's executor.
  - Cancel checkpoint between every completion (not just every batch).
  - Per-lane in-flight counter respects `max_concurrency`.
- New invariant test `tests/test_bootstrap_dispatcher_cross_lane_parallelism.py`:
  - Fixture: 2 lanes (`fast`, `slow`), each with cap=1.
  - `slow` lane has one stage running 5s.
  - `fast` lane has 5 stages each running 100ms.
  - ASSERT: all 5 fast-lane stages complete BEFORE the slow-lane stage finishes (proves cross-lane parallelism).
- Skill update: data-engineer SKILL §6.5 — document `as_completed` pattern.

**Why second:** unblocks every subsequent PR. The CUSIP fix is gated by S3 → as-completed lets S3.5 dispatch into `sec_rate` immediately after S3 completes.

**Risk:** dispatcher loop is load-bearing. Subtle bugs around cap-eval timing, lane concurrency tracking, cancel observation cadence. Mitigation: rigorous tests + Codex 2 pre-push.

**Test plan:**
- Unit: invariant test above.
- Integration: snapshot `bootstrap_orchestrator_phase_c_test.py` extended to assert `(s8.completed_at - s10.completed_at) < 60s` (proves S8 doesn't wait on S10's 80 min).
- Smoke: empirical wall-clock against a 12k-instrument dev DB.

### PR-3: 13F + N-PORT bulk ingester — COPY + ON CONFLICT refactor

**Scope (~600 LoC):**

- `app/services/sec_13f_dataset_ingest.py`
  - Replace per-row loop at lines 277-397 with:
    1. Pre-validate every row in Python (the per-row gates already exist at lines 286-330). Build a generator of valid rows.
    2. `psycopg.Cursor.copy()` into a per-archive UNLOGGED staging table `_staging_13f_archive_<run_id>_<archive_basename>`.
    3. Single `INSERT INTO ownership_institutions_observations SELECT ... FROM staging ON CONFLICT (...) DO UPDATE SET ...`.
    4. `DROP TABLE staging` post-archive.
    5. `INSERT INTO unresolved_13f_cusips` similarly batched.
  - Preserve idempotent UPSERT semantics (line 377-390).
  - Preserve per-archive commit boundary (so a later archive failing doesn't roll back prior archive success).
- `app/services/sec_nport_dataset_ingest.py` — same pattern.
- `app/services/sec_insider_dataset_ingest.py` — same pattern (lower urgency, smaller archives, but consistent shape).
- Lint: `scripts/check_bulk_ingest_no_per_row_savepoint.sh` — greps for `with conn.transaction()` inside `_iter_tsv` loops; fails CI on regression.

**Why third:** depends on PR-2 (so the time saved isn't masked by other-lane idle). Independent of PR-1 (CUSIP gate is upstream).

**Risk:** schema constraint violations bypass the per-row savepoint. Mitigation: pre-validation in Python covers every documented case; chunk-replay strategy for the rare unexpected violation.

**Test plan:**
- Unit: `tests/test_sec_13f_bulk_copy_path.py` — fixture archive with 1000 valid rows + 10 deliberately-malformed rows. Asserts 1000 written + 10 surfaced in `rows_skipped_bad_data`.
- Benchmark: same fixture run pre-/post-PR. Asserts ≥10× speedup.
- Integration: full bootstrap dev-DB run; S10 + S12 + S11 wall-clock should drop from 80+46+~10 min to ~5+3+1 min.

### PR-4: Bulk ingester write-through — batched MERGE post-archive

**Scope (~150 LoC):**

- `app/services/sec_bulk_orchestrator_jobs.py:408-440` — replace serial `for instrument_id in sorted(touched_ids): refresh_institutions_current(...)` with:
  - Single `refresh_institutions_current_batch(conn, instrument_ids=touched_ids)` helper that wraps the existing `refresh_institutions_current` MERGE in a `USING (SELECT … FROM observations WHERE instrument_id = ANY(%(ids)s) …)`.
  - Same pattern for `refresh_funds_current`, `refresh_insiders_current`.
- `app/services/ownership_observations.py` — add `refresh_*_current_batch` siblings to existing single-instrument helpers. Share the MERGE SQL via a Jinja-style template or hand-roll with `ANY(%(ids)s::bigint[])`.
- The existing PR12 `_CATEGORIES` 7-category MERGE writer covers the diff-aware logic — batch path just changes the predicate.
- Test: assert `refresh_institutions_current_batch([1,2,3])` produces the same `_current` state as 3× `refresh_institutions_current(1/2/3)`.

**Why fourth:** unblocks operator-visible `_current` data appearing within minutes of the bulk archive committing, not at Phase E end.

**Risk:** advisory lock semantics — batch path needs to take per-instrument advisory locks in deterministic order to avoid deadlock. Mitigation: sort `instrument_ids` ascending before locking.

### PR-5: Manifest reset at bootstrap start + SHA-keyed .zip reuse

**Scope (~400 LoC):**

- New helper `app/services/bootstrap_orchestrator.py::reset_manifest_for_run` invoked at top of `run_bootstrap_orchestrator`:
  - For each manifest source the run is about to (re)process: `UPDATE sec_filing_manifest SET ingest_status='pending', next_retry_at=NULL, error=NULL WHERE source=ANY(...) AND ingest_status='failed'`.
  - Operator opt-out: param `reset_failed_manifest_rows=False` for the (rare) case of "preserve prior backoff state".
- Replace `_preflight_cleanup_stale_partials` (`sec_bulk_download.py:271-298`) with `_preflight_sha_keyed_reuse`:
  - For each expected archive: if `<archive>.zip.sha256` exists on disk AND matches the manifest URL's `ETag`/`Last-Modified` (HEAD), skip re-download.
  - HEAD request includes `If-Modified-Since: <local mtime>`.
  - On HEAD 304: reuse the existing .zip. Verify SHA-256.
  - On HEAD 200 + same Content-Length + same ETag: reuse.
  - Otherwise: re-download.
- Write `.zip.sha256` sidecar after every successful download.

**Risk:** SHA-keyed reuse must NEVER promote stale data. Mitigation: ETag is the SEC's own version stamp; trust it.

**Test plan:**
- Unit: SHA-mismatch forces re-download.
- Integration: re-bootstrap on a freshly-wiped dev DB but with `.zip` files intact → bootstrap completes WITHOUT re-downloading.

### PR-6: Reaper for orphaned `running` bootstrap stages

**Scope (~200 LoC):**

- New helper `app/services/bootstrap_orchestrator.py::reap_orphaned_running_stages` invoked at top of `run_bootstrap_orchestrator`:
  - `SELECT stage_key, started_at FROM bootstrap_stages WHERE bootstrap_run_id = $run_id AND status = 'running'`
  - For each: check if the corresponding `JobLock` advisory lock is held (via `pg_try_advisory_lock` non-blocking). If unlocked → process is dead → reset stage to `pending`. Log with structured reason.
- Wire into the same prelude as `reset_manifest_for_run`.

**Test plan:**
- Unit: orchestrator entry with one `running` stage + lock NOT held → reset to `pending`. Lock held → leave alone.
- Integration: trigger bootstrap, kill jobs process mid-stage 9, restart jobs → stage 9 auto-resumes.

### PR-7: Capability min_rows + filer_directory_seeded cap

**Scope (~80 LoC):**

- `bootstrap_orchestrator.py:370` — add `_CAPABILITY_MIN_ROWS["cik_mapping_ready"] = 1` (per data-engineer audit §1.5).
- Add new capability `filer_directory_seeded` provided by S4 (`sec_13f_filer_directory_sync`) + S5 (`sec_nport_filer_directory_sync`) — required by S10/S12 (`OR` semantics: either directory satisfies).
- Catalogue-invariant test gain: assert every CIK-keyed downstream stage has a min_rows entry on its required cap.

**Why last:** structural correctness gate. No wall-clock saving. Cheap insurance against silent-fail futures.

### PR-8: Daily-refresh schedule for bulk archives

**Scope (~200 LoC):**

- Two new `SCHEDULED_JOBS` entries:
  - `JOB_SEC_SUBMISSIONS_BULK_REFRESH` — daily 04:00 ET / 08:00 UTC. Adapter wraps `_head_size_and_type` with conditional `If-Modified-Since`. Only re-downloads if SEC's `Last-Modified` advances.
  - `JOB_SEC_COMPANYFACTS_BULK_REFRESH` — daily 04:30 ET / 08:30 UTC.
- Both write to the same `/sec/bulk/<name>.zip` and `.sha256` sidecar.
- Add monthly refresh for 13F / N-PORT / Insider datasets via similar pattern (5th of every month, after SEC's 1st-business-day publication).

**Why:** without this, bulk archives stale 24h post-bootstrap. Daily mode falls back to slower per-CIK paths.

---

## 5. What's explicitly out of scope for v1

Per audit synthesis + iterative-refinement memory:

- Form D, NPORT EX, NRSRO datasets (per edgar §1.3 — out of universe).
- Per-form Atom polling (per edgar §1.4 — budget-blow).
- 26-stage → 8-stage collapse (per adversarial §1 — too big for v1; defer to Tier 2).
- Lane abstraction deletion (per adversarial §2 — defer to Tier 2).
- Stages 14, 16, 17, 18, 19, 20, 21 deletion (per adversarial §17 — defer to Tier 2 after Tier 1 proves the bulk-path-alone hypothesis).

These are correct moves but each requires its own coordinated refactor and ships AFTER Tier 1 proves the architecture out.

---

## 6. Implementation order rationale

```
PR-1 CUSIP floor + OpenFIGI  →  PR-2 dispatcher parallelism  →  PR-3 COPY refactor
                                          ↓                                ↓
                                  PR-4 write-through batch       PR-5 SHA reuse
                                          ↓
                                  PR-6 reaper + PR-7 cap min_rows + PR-8 daily refresh
```

Each PR carries:
- Codex 2 pre-push review
- Pre-flight review skill applied
- Engineering-skill self-review checklist
- Integration test that exercises the relevant pipe in isolation
- Smoke against 5-instrument panel (AAPL/GME/MSFT/JPM/HD)
- Bot APPROVE on latest commit
- Memory update if a new pattern emerges

---

## 7. Per-pipe isolation tests

User mandated: "test each pipe in isolation". Each PR ships with a dedicated integration test that proves the pipe works without depending on other stages. Test fixtures:

| Pipe | Test fixture | Pass criterion |
|---|---|---|
| CUSIP OpenFIGI | 100 instruments without CUSIP, OpenFIGI HTTPS fixtures | Coverage lifts ≥80% |
| Dispatcher parallelism | 2-lane synthetic stages | Fast-lane stages complete before slow-lane |
| 13F COPY ingest | 1000-row dataset zip fixture | Throughput ≥10× baseline; idempotent on rerun |
| NPORT COPY ingest | 1000-row dataset zip fixture | Same |
| Bulk write-through | Mock observations + check `_current` deltas | `_current` updated within 1s of archive commit |
| SHA-keyed reuse | Same .zip with same SHA across two runs | Second run zero-byte download |
| Manifest reset | Pre-seeded failed rows + run | Failed rows flip to pending at start |
| Reaper | Pre-seeded running rows + no lock | Rows flip to pending at orchestrator entry |

---

## 8. Risk register

| Risk | Mitigation |
|---|---|
| OpenFIGI rate limit hits during initial 13k-instrument resolve | Batch 100 per POST; sleep between batches; 250 req/min keyed gives ~52 min wall-clock; gate as one-time stage with 1h budget |
| Dispatcher refactor introduces lane-counter race | Use `threading.Lock` per lane; rigorous concurrency test in tests/test_bootstrap_dispatcher_cross_lane_parallelism.py |
| COPY path silently mis-formats Decimal/None | Pre-validate every type at parse stage; staging table column types match target |
| SHA-keyed reuse promotes stale data | Trust SEC's ETag; HEAD always issued; reuse only on Last-Modified match |
| Reaper races with a slow-starting stage | Add 60s grace period before reap; only reap stages with `started_at > 5 min ago` |
| OpenFIGI key absent in production | Degrade to 25 req/min; bootstrap still completes (slower) but doesn't fail |
| Manifest reset clobbers in-flight parser progress | Only resets `failed` status, not `pending`/`fetched`/`parsed` |

---

## 9. Rollout strategy

1. **Land PRs 1+2 first.** These are the operator-visible "first run is fast and complete" PRs.
2. **Smoke on dev DB** between every PR — 5-instrument panel must pass.
3. **Operator triggers a fresh bootstrap** after PR-3 lands (the COPY refactor). Measure wall-clock against this spec's targets.
4. **If wall-clock ≥ 45 min**, hold PR-4 onwards; profile and iterate.
5. **Once Tier 1 ships**, declare Tier 1 complete in memory + skill updates.
6. **Tier 2 (stage collapse, lane deletion) opens a separate epic.**

---

## 10. Open questions for adversarial review

- Is the PR ordering safe? What happens if PR-2 (parallelism) ships before PR-1 (CUSIP floor) and a fast bootstrap silently produces 33% data?
  - **My answer:** ship PR-1 first. The floor gate REFUSES to start Phase C until coverage is ≥80%. So PR-2 (parallelism) being faster doesn't matter if Phase C is blocked.
- Is the OpenFIGI legal posture OK? They're a Bloomberg-owned public API with explicit free-tier terms.
  - **My answer:** settled-decisions.md §"approved data sources" lists OpenFIGI explicitly; reuse with the same posture.
- Should PR-3 (COPY) include a fallback to row-by-row if COPY fails?
  - **My answer:** no — COPY failure is a programmer error (schema mismatch), not a data issue. Fail loud and fix the schema.
- Is `as_completed` actually safe for cancel observation?
  - **My answer:** cancel checkpoint runs between every completion, not every batch. Worst-case latency = duration of single completing stage (≤ archive ingest ~5 min after PR-3). Acceptable.
- Are there other settled-decisions this conflicts with?
  - **My answer:** I'll check explicitly in next-pass refinement.

---

## 11. What I'd love to be wrong about

This plan is the synthesis of three audits. Before any code lands, a clean agent will rip it apart in §13. If they find:

- A safer ordering of PRs.
- A correctness gap I missed.
- A simpler way to achieve the same wall-clock without the COPY refactor.
- A reason any PR is bigger than it needs to be.

— refine and re-review until convergence.

---

## 12. Decision log (running)

| Decision | Rationale | Date |
|---|---|---|
| Tier 1 = 8 PRs, not one mega-refactor | smaller PRs reduce review risk + enable measure-after-each | 2026-05-22 |
| CUSIP floor 80% (was 0%) | settled-decisions data integrity invariant | 2026-05-22 |
| Use OpenFIGI public API | free, no auth required for basic use, settled-decisions-approved | 2026-05-22 |
| Keep 26-stage catalogue in Tier 1 | Tier 2 will collapse; minimise blast radius in Tier 1 | 2026-05-22 |
| Replace `wait(ALL_COMPLETED)` with `as_completed` | minimal diff, preserves cap-eval semantics | 2026-05-22 |
| COPY into unlogged staging then INSERT...SELECT into target | preserves UPSERT semantics; SEC archives are immutable so synchronous_commit=OFF is safe | 2026-05-22 |

---

## 13. Adversarial-review pass (PENDING)

Clean agent dispatch — instructions in this spec's §10 + the §15 below. Agent must NOT see this section before completing.

---

## 14. Skill drift to fix in same PRs

Per audit synthesis:

- `.claude/skills/data-sources/sec-edgar.md` §3.2, §4, §9 — OpenFIGI fallback, dataset cadences, daily-refresh hooks.
- `.claude/skills/data-engineer/SKILL.md` §3.5, §6.5 — bulk ingester pattern, capability min_rows discipline, write-through batching.
- New: `.claude/skills/data-engineer/bootstrap-runtime.md` — runtime invariants, cancel semantics, reaper pattern.

---

## 15. Closing

The bootstrap is functionally correct today but architecturally expensive in three independent ways: per-row writes, lane-blocking wait, and silent CUSIP cliff. Tier 1 fixes all three without revolutionising the architecture. Tier 2 (deferred) will simplify the stage catalogue once Tier 1 proves the bulk-path-alone hypothesis.

The pride-on-the-line answer to "are we doing all we can": no, not yet — but this plan, executed in Tier 1, brings cold-bootstrap from ~4-5 h to ≤ 45 min with 80%+ data coverage and operator-visible drop telemetry. Daily mode becomes a sub-5-minute affair under #1155 + steady-state.

No declaring done before each PR profiles end-to-end against the baseline.
