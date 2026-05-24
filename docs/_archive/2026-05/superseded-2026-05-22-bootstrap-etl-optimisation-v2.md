# Bootstrap ETL — comprehensive optimisation spec **v2**

**Date:** 2026-05-22 (v2 after rip-apart + empirical probes)
**Author:** Claude Opus 4.7
**Status:** DRAFT v2 — addresses every CRITICAL + HIGH from `/tmp/spec-rip-apart.md`. Awaiting second adversarial pass + Codex 2 spec-gate.
**Baseline:** run #4 cancelled at ~110 min, projected 4-5 h end-to-end.
**Precedent:** spec v1 at `2026-05-22-bootstrap-etl-optimisation.md`. v2 supersedes.

---

## Changelog v1 → v2

| Finding | v1 claim | v2 reality |
|---|---|---|
| C1 write-through "broken" | claimed bulk path doesn't refresh `_current` | **wrong.** `sec_bulk_orchestrator_jobs.py:410-431` (13F), `:569-585` (insider), `:751-767` (NPORT) DO call `refresh_*_current` per touched_instrument. PR-4 reframed as **batching** the serial loop, not "fixing" write-through. |
| C2 PR-3 unresolved CUSIP writeback | listed as preserved behavior | **wrong.** Bulk path doesn't write to `unresolved_13f_cusips` today; only the legacy `institutional_holdings.py:646-662` path does. v2: this becomes explicit NEW behaviour in a separate split PR (PR-1b). |
| C3 staging table fixture leak | new `_staging_<run>_<archive>` tables | v2: `CREATE TEMP TABLE ... ON COMMIT DROP` — session-scoped, no `_PLANNER_TABLES` invariant break. |
| C4 SHA-reuse contract conflict | "trust SEC ETag" | **empirical truth:** SEC ignores `If-None-Match` and `If-Modified-Since` (probed 2026-05-22; both return 200 + full body). Stable ETag IS returned (`504b124e9474334e889e9e525db95c14-184`). v2: client-side HEAD → ETag-compare → conditional GET. Plus PR-5 split: 5a manifest reset (no contract change), 5b SHA-reuse (requires settled-decisions delta). |
| C5 dispatcher tests break | LoC 250 only | v2: 250 service + 150-250 test rework, explicit. |
| H1 OpenFIGI fake settled-decision | "approved data sources" cited | v2: NEW settled-decision entry must land BEFORE PR-1. |
| H2 OpenFIGI rate limits wrong | 25/min unkeyed; 250/min keyed | **empirical truth via OpenFIGI docs:** unkeyed 25/min × 10 jobs/POST = 250 mappings/min; keyed 25/6s × 100 jobs/POST = 25,000 mappings/min. |
| **OpenFIGI does NOT return CUSIP** (v2 discovery via API doc probe) | implicit in v1 | **kills** ticker→CUSIP proactive seeding. OpenFIGI viable only as REVERSE resolver (`idType=ID_CUSIP, idValue=<x>` → returns FIGI+ticker). |
| H3 PR-3 not independent of PR-1 | claimed independent | v2: explicit blocking dependency — PR-3 cannot ship in operator-visible mode without PR-1. |
| H4 floor breaks cascade-skip | floor in C-precondition | v2: floor moved to POST-SWEEP (end of Phase D), not pre-Phase C. Bulk ingest accepts whatever coverage exists; sweep recovers; floor enforces at run completion. |
| H5 reaper acquire side effect | `pg_try_advisory_lock` | v2: probe via `pg_locks` system view (NO acquire). Source lookup via `source_for(job_name)`. Heartbeat row check for re-entrancy. |
| H6 PR-5+PR-8 interaction | undefined | v2: PR-8 runs against bootstrap-not-in-progress only; uses `bootstrap_state` fence to skip on conflict. |
| H7 45-min budget undefensible | one-line claim | v2: explicit stage-by-stage budget in §10. Honest ETA on Tier 1 alone: 60-90 min. 45 min target deferred to Tier 2 (catalogue collapse + lane deletion). |
| H8 capability orphan | `cusip_mapping_lifted` cap | v2: no new capability. S1.b writes to same `external_identifiers` table that satisfies existing `cusip_mapping_ready`. |
| H9 share-class siblings | smoke missing | v2: 5-instrument panel extended to **AAPL/GME/MSFT/JPM/HD + GOOGL** (share-class sibling case). |
| H10 PR-7 no evidence | speculative cap floor | v2: PR-7 conditional on producing one concrete failure trace OR deferred to operator triage. |
| M9 COPY fail-loud | "fix the schema" | v2: use Postgres 17 `COPY ... ON_ERROR ignore` (verified PG17 feature). Per-row tolerance preserved without per-row savepoint. |

---

## 1. Goals (unchanged from v1)

| Target | Today | Acceptance (v2) |
|---|---|---|
| Cold-install wall-clock | ~4-5 h | Tier 1: **≤ 90 min**. Tier 2 (catalogue collapse): **≤ 45 min**. |
| Daily-refresh wall-clock | N/A | **≤ 5 min** (steady-state path, depends on #1155 firing) |
| CUSIP coverage at run COMPLETION (post-sweep) | 33.82% | **≥ 80% (floor-gated at run end)** |
| CUSIP coverage at BULK INGEST START | 33.82% (silent) | **measured + recorded; bulk proceeds** |
| Ownership categories populated at "complete" | 3 of 10 | **10 of 10** |
| Operator-visible drop telemetry | INFO log only | **Admin panel + per-category drop summary** |
| Process-crash recovery | Manual | **Auto-reap stuck `running` stages on jobs restart** |
| Wasted re-download on re-bootstrap | Full 5.7 GB | **0 bytes if ETag matches** (PR-5b) |

---

## 2. Pre-requisite settled-decisions deltas (must land FIRST)

### SD-1: OpenFIGI as approved external data source

New entry in `docs/settled-decisions.md`:

> **2026-05-22 — OpenFIGI v3 as approved fallback CUSIP→ticker resolver.**
> OpenFIGI is Bloomberg-operated, free at 25 req/min unkeyed + 25 req/6s keyed,
> ToS permits programmatic use. The free API does NOT return CUSIP in responses
> — it only accepts CUSIP as input via `idType=ID_CUSIP`. Bootstrap usage is
> CUSIP → FIGI → ticker → instrument_id resolution, applied to the
> `unresolved_13f_cusips` buffer. Operator opt-in via `OPENFIGI_API_KEY` env
> var; unkeyed mode is the default and is sufficient for the universe size.
> Forbidden: any flow that takes ticker → OpenFIGI → CUSIP (the response
> shape does not return CUSIP; this would be a coding error).

### SD-2: Bulk archive ETag-keyed reuse permitted (REQUIRED for PR-5b)

New entry in `docs/settled-decisions.md`:

> **2026-05-22 — Bulk archive reuse keyed on SEC ETag.**
> The Codex review BLOCKING for #1020 (`sec_bulk_download.py:271-298`) banned
> reusing a prior-run .zip because the run-manifest provenance contract
> required each archive to be downloaded in THIS run. With SEC's stable
> S3-backed ETag (probed 2026-05-22: `submissions.zip` returns
> `etag: "504b124e9474334e889e9e525db95c14-184"`), an unchanged ETag
> guarantees byte-equivalent content. Reuse is permitted when:
> (1) the local `.zip.etag` sidecar matches SEC's HEAD response, AND
> (2) the SHA-256 of the local file matches the sidecar's recorded SHA.
> The run-manifest records `reuse_reason: 'etag_match'` for audit.
> If the operator wants forced re-download, `BOOTSTRAP_FORCE_REDOWNLOAD=1`
> bypasses the reuse path.

If SD-2 is rejected, PR-5b drops and the operator pays a full 5.7 GB redownload on every bootstrap.

---

## 3. Revised PR ordering + LoC budgets

```
SD-1 + SD-2 settled-decisions  → PR-1a CUSIP resolver capture
                                          ↓
                                  PR-1b OpenFIGI reactive sweep + bootstrap stage
                                          ↓
                                  PR-2 dispatcher parallelism
                                          ↓
                                  PR-3 COPY refactor (13F, NPORT, insider)
                                          ↓
                                  PR-4 batched _current refresh
                                          ↓
                                  PR-5a manifest reset + PR-5b SHA-reuse (parallel)
                                          ↓
                                  PR-6 reaper + PR-8 daily refresh
                                          ↓
                                  PR-7 cap floor (conditional)
```

Each PR carries Codex 2 pre-push + bot APPROVE + integration test.

| PR | Service LoC | Test LoC | Skill LoC | Total | Critical-path? |
|---|---|---|---|---|---|
| PR-1a | 200 | 100 | 50 | ~350 | yes |
| PR-1b | 400 | 200 | 100 | ~700 | yes |
| PR-2 | 250 | 250 | 50 | ~550 | yes |
| PR-3 | 600 | 400 | 50 | ~1050 | yes |
| PR-4 | 150 | 150 | 50 | ~350 | no |
| PR-5a | 150 | 100 | 30 | ~280 | no |
| PR-5b | 300 | 200 | 50 | ~550 | no |
| PR-6 | 200 | 150 | 30 | ~380 | no |
| PR-7 | 30 | 50 | 20 | ~100 | no |
| PR-8 | 200 | 150 | 30 | ~380 | no |
| **Total** | **2480** | **1750** | **460** | **~4690** | |

---

## 4. PR-1a — Bulk-path unresolved-CUSIP capture

**Replaces v1 PR-1 step "unresolved_13f_cusips writeback".**

### Scope (service ~200 LoC)

- `app/services/sec_13f_dataset_ingest.py:294-297` — replace `result.rows_skipped_unresolved_cusip += 1` with INSERT into `unresolved_13f_cusips` (currently only the legacy per-filing path writes).
- `app/services/sec_nport_dataset_ingest.py:341-345` — same.
- Both write via `_record_unresolved_cusip(conn, cusip, filer_cik, period_end, source='bulk_13f_dataset' | 'bulk_nport_dataset')`.
- Idempotent ON CONFLICT (cusip, filer_cik, period_end, source).
- Batched: accumulate ~1000 rows then single INSERT (COPY in PR-3 phase).

### Test

Unit fixture: 10-row archive with 5 unresolved CUSIPs → assert 5 rows in `unresolved_13f_cusips`.

### Why first

Without this, PR-1b (the resolver) has no buffer to sweep. The capture must precede the sweep semantically.

---

## 5. PR-1b — OpenFIGI reactive resolver + Phase D sweep stage

### Scope (service ~400 LoC)

- New module `app/services/openfigi_resolver.py`:
  - Adapter over `https://api.openfigi.com/v3/mapping` (verified base URL).
  - Batch builder: chunks ≤10 (unkeyed) or ≤100 (keyed) `(idType=ID_CUSIP, idValue=<cusip>)` requests per POST.
  - Rate limiter: shared with no other client (OpenFIGI ≠ SEC budget). Unkeyed = 25/min; keyed = 25/6s. Honour `ratelimit-remaining` / `ratelimit-reset` response headers (per OpenFIGI docs).
  - 429 backoff: exponential up to 3 retries; abandon batch with structured reason.
  - Reads `OPENFIGI_API_KEY` env var; degrades to unkeyed on absence.
- Extend `app/services/cusip_resolver.py::sweep_resolvable_unresolved_cusips`:
  - Currently: name-fuzzy via SEC 13F List at 0.92 threshold.
  - Add: for each unresolved row not name-fuzzy-matched, query OpenFIGI for CUSIP→FIGI→ticker.
  - If ticker matches `instruments.symbol` (exact), write `external_identifiers (provider='openfigi', identifier_type='cusip', instrument_id, identifier_value=<cusip>, is_primary=FALSE)`.
  - Trigger existing rewash path (`_rewash_originating_filings`) for the now-resolved CUSIP.
- New bootstrap stage `S12.5: cusip_resolver_post_bulk_sweep`
  - Lane: `sec_rate` (not `sec_bulk_download` — OpenFIGI is a separate rate budget).
  - Requires: `institutional_inputs_seeded` AND `funds_inputs_seeded` (so unresolved buffer is full).
  - Provides: NO new cap. Writes to existing `external_identifiers` which already satisfies `cusip_mapping_ready` — but cap is already provided by S3.
  - **Floor gate (post-stage):** compute `cusip_coverage` after S12.5 completes. Demote run to `partial_complete` if coverage < 80%. NOT a hard refuse — the bulk data is already on disk + observations are written + UI works; the demote tells the operator "follow-up needed" without blocking.

### Test

- Unit: `openfigi_resolver` with recorded HTTPS fixtures (5 sample CUSIPs → known tickers). Verify rate-limit backoff on synthetic 429.
- Integration: pre-seed `unresolved_13f_cusips` with 20 rows, run sweep, assert ≥10 promoted to `external_identifiers`.
- Smoke: AAPL/GME/MSFT/JPM/HD + **GOOGL** (share-class sibling) all have CUSIPs after sweep.

### Risk

- OpenFIGI down for 48h → sweep returns no new mappings; coverage stays at 33%; operator sees `partial_complete`. Document operator escalation path.
- ToS / legal posture for high-volume use → SD-1 covers; operator approval pre-merge required.

---

## 6. PR-2 — Dispatcher parallelism (replaces `wait(ALL_COMPLETED)`)

### Scope (service 250 + test 250)

`app/services/bootstrap_orchestrator.py` `_phase_batched_dispatch`:

- Replace `wait([f for _, f in all_futures])` at line 1720 with `as_completed` poll.
- One persistent `ThreadPoolExecutor` per lane (lifetime = dispatcher entry → exit).
- After each completion: update `statuses` + `rows_processed`, **recompute `caps`**, find newly-ready stages, submit to their lane executors.
- Per-lane in-flight counter (defended by `threading.Lock`) enforces `_LANE_MAX_CONCURRENCY[lane]`.
- Cancel checkpoint between every completion (not every batch).
- On dispatcher exit: shutdown all executors with `wait=True`.

### Critical test additions (per rip-apart C5)

- `tests/test_bootstrap_dispatcher_cross_lane_parallelism.py`:
  - 2-lane synthetic: slow-lane single stage 5s; fast-lane 5 stages × 100ms. Assert fast-lane all done before slow-lane.
  - **Per-completion cap recomputation:** stage A (cap-provider) finishes at t=1.0s; stage B (cap-requirer) is in same batch but blocked on A's cap until A completes. Assert B starts within 100ms of A's completion.
  - **Cancel observation:** cancel after stage A completes mid-batch; assert stage B never starts.

### Test fixture rework (per rip-apart M1)

- Existing `tests/test_bootstrap_orchestrator*.py` use synchronous dispatch + ordered `bootstrap_archive_results` pre-seeds. v2 estimate: 100-200 LoC of fixture rework (mock the `as_completed` iteration via deterministic future completion).

### Risk

- Dispatcher is load-bearing. Mitigation: pre-flight review skill + Codex 2 pre-push + 3 distinct integration test scenarios.

---

## 7. PR-3 — 13F + NPORT + insider COPY refactor

### Scope (service ~600 LoC + tests 400)

For each of `sec_13f_dataset_ingest.py`, `sec_nport_dataset_ingest.py`, `sec_insider_dataset_ingest.py`:

1. Pre-validate every row in Python (per-row gates at `sec_13f_dataset_ingest.py:286-330` already exist).
2. `CREATE TEMP TABLE _stg_<category>_archive ( ... ) ON COMMIT DROP` (session-scoped, no fixture leak).
3. `psycopg.Cursor.copy()` validated rows into staging.
4. Single `INSERT INTO <observations> SELECT ... FROM _stg_<category>_archive ON CONFLICT (...) DO UPDATE SET ...` — preserves UPSERT semantics.
5. Same pattern for `unresolved_*` table writes (depends on PR-1a).
6. Per-archive `conn.commit()` boundary preserved (later-archive failure doesn't roll back prior archives).
7. Use Postgres 17 `COPY ... WITH (ON_ERROR ignore, LOG_VERBOSITY verbose)` for residual schema-drift tolerance (per rip-apart M9). Verified PG17 feature.

### Lint

`scripts/check_bulk_ingest_no_per_row_savepoint.sh`:
- Path whitelist: `app/services/sec_*_dataset_ingest.py`.
- Empty-grep guard: return 1 on no match (file moved).
- Shellcheck-clean (per `feedback_iterative_refinement` memory).

### Test (per rip-apart T2)

- Multi-million-row fixture archive (lazy-generated, not stored). Assert ≥10× throughput.
- Inject deliberate schema-drift row (NUMERIC overflow). Assert `ON_ERROR ignore` skips + surfaces in `rows_skipped_bad_data`.

### Cancel observation cost (per rip-apart M4)

Today: per-INFOTABLE-row cancel checkpoint (ms-latency). COPY: one atomic op per archive (~10-60s latency on multi-million rows). Acknowledge trade-off in spec; operator-cancel latency degrades from sub-second to ~10-60s. Document in skill.

---

## 8. PR-4 — Batched `refresh_*_current` post-archive

### Scope (service ~150 LoC)

`app/services/sec_bulk_orchestrator_jobs.py:410-431` (13F), `:569-585` (insider), `:751-767` (NPORT):

- Replace serial `for instrument_id in sorted(touched_ids): refresh_X_current(conn, instrument_id)` with `refresh_X_current_batch(conn, instrument_ids=touched_ids)`.
- New `refresh_*_current_batch` helpers in `app/services/ownership_observations.py`:
  - Wrap the existing PR12 MERGE writer in `USING (... WHERE instrument_id = ANY(%(ids)s::bigint[]))`.
  - Take all advisory locks sorted by HASH KEY (not raw instrument_id, per rip-apart M5).
- Test: assert `refresh_X_current_batch([1,2,3])` ≡ `refresh_X_current(1) + refresh_X_current(2) + refresh_X_current(3)`.

### Reframing (per rip-apart C1)

This is NOT "fix write-through" — write-through ALREADY exists. This is **batch the serial refresh loop**. Expected wall-clock saving: 2-10 min per category at peak.

---

## 9. PR-5a — Manifest reset at bootstrap start

### Scope (service ~150 LoC)

New helper `app/services/bootstrap_orchestrator.py::reset_manifest_for_run` invoked at top of `run_bootstrap_orchestrator`:

```sql
UPDATE sec_filing_manifest
   SET ingest_status='pending',
       next_retry_at=NULL,
       error=NULL
 WHERE source = ANY(%(sources)s::text[])
   AND ingest_status='failed'
   AND last_attempted_at < %(this_run_started_at)s;
```

- `sources` = the set the run is about to (re)process.
- Operator opt-out via `bootstrap_runs.params {'reset_failed_manifest': False}`.

### Test

Pre-seed 5 failed rows, run reset, assert all flip to pending.

### Why split from v1 PR-5

This is contract-neutral (failed→pending is legitimate retry semantics). PR-5b (SHA-reuse) requires SD-2 settled-decision delta first.

---

## 10. PR-5b — ETag-keyed bulk archive reuse (depends on SD-2)

### Scope (service ~300 LoC)

`app/services/sec_bulk_download.py`:

- Replace `_preflight_cleanup_stale_partials` with `_preflight_etag_keyed_reuse`:
  1. For each expected archive: HEAD against SEC with retry (no `If-Modified-Since` — confirmed empirically ignored).
  2. Read returned `ETag` header (e.g. `"504b124e9474334e889e9e525db95c14-184"`).
  3. Read local `.zip.etag` sidecar.
  4. If sidecars match: verify local `.zip` SHA-256 against `.zip.sha256` sidecar. If matches: reuse. Record `reuse_reason='etag_match'` in `.run_manifest.json`.
  5. Otherwise: delete local + re-download.
- On every successful download: write `.zip.etag` + `.zip.sha256` sidecars.
- Operator override: `BOOTSTRAP_FORCE_REDOWNLOAD=1` env var bypasses reuse.

### Interaction with PR-8 (per rip-apart H6)

PR-8 daily refresh re-downloads only if SEC ETag advances. PR-5b reuse only if local ETag matches SEC ETag. If PR-8 ran overnight and wrote a fresher .zip, PR-5b reuse sees fresh local ETag matches fresh SEC ETag — bootstrap reuses correctly.

Pathological case: PR-8 starts mid-bootstrap. Defended by: PR-8 checks `bootstrap_state.status='running'` and skips if true. Add fence check at top of daily-refresh job.

### Test

- HEAD fixture: ETag changes → forces re-download.
- HEAD fixture: ETag matches → 0-byte reuse.
- Operator override: `BOOTSTRAP_FORCE_REDOWNLOAD=1` → always re-download.

---

## 11. PR-6 — Orphaned-`running` stage reaper (probe-only)

### Scope (service ~200 LoC)

New helper `app/services/bootstrap_orchestrator.py::reap_orphaned_running_stages`:

- Query orphans:

  ```sql
  SELECT bs.stage_key, bs.started_at
    FROM bootstrap_stages bs
   WHERE bs.bootstrap_run_id = %(run_id)s
     AND bs.status = 'running'
     AND bs.started_at < NOW() - INTERVAL '60 seconds';  -- grace window
  ```

- For each: probe lock held via `pg_locks` system view (NO acquire side effect):

  ```sql
  SELECT 1 FROM pg_locks
   WHERE locktype = 'advisory'
     AND classid = 0
     AND objid = hashtextextended('job_source:' || %(source)s, 0)::int;
  ```

- Source lookup via `app.jobs.sources.source_for(job_name)` (per rip-apart H5).
- If lock NOT held AND `job_runtime_heartbeat.last_heartbeat_at < NOW() - INTERVAL '5 minutes'` (defends against re-entrancy where outer holder is still alive): reset stage to `pending` with structured reason in `last_error`.
- Invoked at top of `run_bootstrap_orchestrator`.

### Test

- Stage `running` + no lock + no recent heartbeat → reset.
- Stage `running` + lock held → leave alone.
- Stage `running` + lock NOT held BUT heartbeat recent → leave alone (re-entrancy case).
- Stage `running` + started_at within grace window → leave alone.

---

## 12. PR-7 — Capability min_rows + filer directory cap (conditional)

### Scope (service ~30 LoC)

- `bootstrap_orchestrator.py:370` — add `_CAPABILITY_MIN_ROWS["cik_mapping_ready"] = 1`.
- Add new capability `filer_directory_seeded` provided by S4 OR S5; required by S10/S12.
- Catalogue-invariant test gain.

### Defer condition

Per rip-apart H10: only ship if a concrete failure trace exists where `cik_mapping_ready` was advertised on zero rows. If no trace by Tier 1 completion: defer to operator triage. Spec doesn't pre-commit.

---

## 13. PR-8 — Daily refresh schedule for bulk archives

### Scope (service ~200 LoC)

- Two new `SCHEDULED_JOBS` entries:
  - `JOB_SEC_SUBMISSIONS_BULK_REFRESH` — daily 08:00 UTC (after SEC's 03:00 ET nightly rebuild). Adapter calls SEC HEAD, ETag-compare, conditional GET.
  - `JOB_SEC_COMPANYFACTS_BULK_REFRESH` — daily 08:30 UTC.
- Both honour `_PROCESS_RATE_LIMIT_CLOCK` (per rip-apart M10).
- Both fence-check `bootstrap_state.status != 'running'` (per H6).
- Monthly job for 13F / NPORT / Insider (5th of month).

### Test

- Mock SEC HEAD returning unchanged ETag → 0-byte refresh.
- Mock SEC HEAD returning new ETag → triggers download.
- Fence-check: `bootstrap_state.status='running'` → job skips.

---

## 14. Honest wall-clock budget (per rip-apart H7)

Stage-by-stage best-case post-Tier-1:

| Stage | Wall-clock | Notes |
|---|---|---|
| S1 universe_sync | 10s | unchanged |
| S2 candle_refresh | 4 min | 200 instruments × ~1s each |
| S3 cusip_universe_backfill | 30s | unchanged |
| S4+S5 filer directories | ~30s parallel | unchanged |
| S6 cik_refresh | 30s | 3 JSON pulls (per edgar audit) |
| S7 sec_bulk_download | 5 min | 5.7 GB @ 100 Mbps; PR-5b reuse → ~30s if cached |
| S8 sec_submissions_ingest | 10 min | 12k CIKs filtered from 1.5 GB zip |
| S9 sec_companyfacts_ingest | 10 min | per-fact insert (own optimization is Tier 2) |
| S10 sec_13f_ingest_from_dataset | ~5 min | post PR-3 COPY (was 80 min) |
| S11 sec_insider_ingest_from_dataset | ~2 min | post PR-3 COPY |
| S12 sec_nport_ingest_from_dataset | ~4 min | post PR-3 COPY (was 46 min) |
| **S12.5 cusip_resolver_post_bulk_sweep** | up to 30 min | OpenFIGI unkeyed @ 250 mappings/min on 7-12k unresolved CUSIPs |
| S13 sec_submissions_files_walk | 9 min | sec_rate-bound; runs in parallel with S8/S9 via PR-2 |
| S14 filings_history_seed | 2 min | mostly no-op post-bulk |
| S15 sec_first_install_drain | ~20 min | non-issuer HTTP for institutional/blockholder filers |
| S16-S22 legacy chain | ~10 min combined | mostly no-ops at ~5-10s per stage dispatch overhead |
| S23 ownership_observations_backfill | ~2 min | mostly no-op post PR-4 |
| S24 fundamentals_sync | ~5 min | XBRL sync |
| S25 mf_directory_sync | ~30s | |
| S26 n_csr_drain | ~5 min | |

**Critical-path sum (with cross-lane parallelism via PR-2):**
- Phase A (S1-S7): 10s + 5 min + 30s + 30s + 30s + 5 min = **~12 min**
- Phase C (S8-S12 parallel): max(10, 10, 5, 2, 4) = **~10 min**
- Phase D (S12.5 sweep): up to **30 min** OpenFIGI unkeyed (could be much faster with key)
- Phase E (S13 + legacy chain + S23-S26): **~25 min** with parallelism, dominated by S15

**Tier 1 honest cold-bootstrap ETA: 75-90 min.**

**45-min target is Tier 2 work:**
- Stage catalogue collapse (delete S14, S16-S21) → -10 min
- S15 non-issuer drain replaced by bulk submissions.zip parsing → -15 min
- S12.5 with OpenFIGI key → -25 min
- Combined: 45-60 min realistic Tier 2.

**v2 acceptance: ≤ 90 min Tier 1 / ≤ 45 min Tier 2.**

---

## 15. Acceptance criteria (revised per rip-apart A1-A4)

```
ASSERT bootstrap_runs.status = 'complete'
ASSERT bootstrap_runs elapsed ≤ 90 min (Tier 1) / ≤ 45 min (Tier 2)
ASSERT every stage IN ('success', 'skipped')
ASSERT NO stage = 'cancelled' UNLESS operator triggered
ASSERT external_identifiers (provider IN ('sec','openfigi') identifier_type='cusip') ≥ 0.80 × tradable instruments
ASSERT ownership_*_current rows ≥ _CAPABILITY_MIN_ROWS[corresponding cap]  -- per A1
ASSERT bootstrap_archive_results rows = COUNT(C-stages * archives_per_stage)  -- per A2
ASSERT financial_facts_raw rows > 0
ASSERT instrument_share_count_latest has rows for AAPL/GME/MSFT/JPM/HD/GOOGL  -- per H9
ASSERT GET /instruments/AAPL/ownership-rollup non-empty within 1s
ASSERT GET /instruments/GOOGL/ownership-rollup non-empty within 1s  -- share-class sibling
ASSERT bootstrap_archive_results aggregate drop rate < 25% per archive
ASSERT data_freshness_index has rows for all (subject, source) triples touched  -- per A4
ASSERT downstream ranking/thesis engine smoke test passes (depends on ownership_*_current)  -- per A3
```

Daily refresh:

```
ASSERT scheduled atom + daily-index + per-CIK poll discovers all new accessions
ASSERT manifest worker drains within 5 min
ASSERT no bulk archive re-download triggered (PR-5b reuse on ETag match)
ASSERT PR-8 daily refresh on unchanged ETag = 0 bytes
ASSERT PR-8 fence-check skips when bootstrap.status='running'
```

Robustness:

```
ASSERT operator cancel mid-archive observed within 60s (per PR-3 trade-off in §7)
ASSERT process crash mid-stage: jobs restart auto-resumes via PR-6 reaper
ASSERT operator deletes only .zip files but keeps .sha256: PR-5b correctly forces re-download
ASSERT OpenFIGI down: bootstrap completes with `partial_complete` status + structured operator action
ASSERT two operators trigger bootstrap simultaneously: partial-unique index blocks second; clear error message
ASSERT SEC re-publishes mid-bootstrap: SHA verification at C-stage entry detects drift (PR-5b extension)
ASSERT disk runs out mid-COPY: PR-3 transaction rollback + bootstrap_archive_results retains drop record
```

---

## 16. Migration safety (per rip-apart MS1-MS4)

- **MS1 column-shape parity**: PR-3 staging tables auto-match target via `LIKE <target> INCLUDING DEFAULTS`. CI lint guards against drift.
- **MS2 SCHEDULED_JOBS registry**: PR-8 + PR-1b new entries must add `_INVOKERS[]` + `source_for()` rows. Test invariant.
- **MS3 SQL touch**: PR-1b new stage requires `sql/<N>_bootstrap_stages_lane_family_split.sql` migration. Spec includes the migration step.
- **MS4 mid-bootstrap deploy**: pre-deploy `bootstrap_runs.stages` rows don't have S12.5. After restart, dispatcher's `spec_by_stage_key.get(stage.stage_key)` returns None → stale-name failure. **Mitigation**: PR-1b ships with a backfill migration that INSERTs S12.5 into all in-flight `bootstrap_stages` rows on first deploy.

---

## 17. Out-of-scope for Tier 1 (per rip-apart S1-S2)

- Stage catalogue collapse (delete S14, S16-S21).
- Lane abstraction deletion.
- `_PARAM_DYNAMIC_BOOTSTRAP_*_CUTOFF` sentinel consolidation.
- COPY refactor for S9 companyfacts ingest (separate per-fact pattern).
- Insider Form 144 ingest.
- Schedule 13E ingest.
- Real-time per-CIK atom polling.

These are Tier 2 — each requires its own coordinated refactor and ships AFTER Tier 1 proves the bulk-path-alone hypothesis.

---

## 18. Memory + skill updates per Tier 1

Land in same PRs:

- `.claude/skills/data-sources/sec-edgar.md` — OpenFIGI fallback (§3.2 sub-section), conditional fetch reality (SEC ignores If-*), bulk archive cadences (§4 new), daily-refresh hooks (§9), ETag reuse pattern.
- `.claude/skills/data-engineer/SKILL.md` — `_CAPABILITY_MIN_ROWS` discipline (§6.5.4 new), bulk ingester COPY pattern (§3.5 new), batched refresh pattern (§3.7 new).
- Add settled-decisions SD-1 + SD-2 entries.
- Update prevention log: COPY error tolerance via PG17 ON_ERROR (lessons from this iteration).

---

## 19. Decision log (running)

| Decision | Rationale | Date |
|---|---|---|
| OpenFIGI reactive (CUSIP→ticker), NOT proactive | empirical API doc probe: response doesn't contain CUSIP | 2026-05-22 |
| Floor gate post-sweep, not pre-bulk | preserves cascade-skip cleanly, doesn't block legit slow-connection installs | 2026-05-22 |
| Tier 1 honest ETA: 90 min not 45 | stage-by-stage budget showed S15 + S12.5 + legacy chain still dominate | 2026-05-22 |
| ETag-keyed reuse via client-side compare | empirical probe: SEC ignores If-* headers, but returns stable ETag | 2026-05-22 |
| PR-5 split into 5a (manifest reset) + 5b (ETag reuse) | 5b requires settled-decisions delta; 5a doesn't | 2026-05-22 |
| Smoke panel adds GOOGL (share-class sibling) | settled-decision PR-B deferred for fan-out; explicit test of the gap | 2026-05-22 |
| PR-7 conditional on concrete failure trace | rip-apart H10: defensive cap floor without evidence is speculative | 2026-05-22 |

---

## 20. Round 3 review gate

After v2 lands, before any implementation:

1. **Codex 2 spec review** — `codex.cmd exec "Review this spec at docs/superpowers/specs/2026-05-22-bootstrap-etl-optimisation-v2.md. Focus on correctness gaps, missing pre-conditions, ordering bugs. Reply terse."`
2. **Second clean-agent adversarial pass** — like the rip-apart, but on v2. Verify every CRITICAL+HIGH from v1 rip-apart is addressed.
3. **Operator sign-off** — present v2 to user with Tier 1 plan + Tier 2 deferred list + 90-min ETA honest budget.

Only after all three converge: start implementation.

---

## Closing

v2 incorporates every CRITICAL + HIGH finding from `/tmp/spec-rip-apart.md` plus two empirical probes (OpenFIGI API behaviour + SEC bulk archive conditional fetch). The major direction change is OpenFIGI's role: reactive REVERSE resolver, not proactive seeder. The 45-min target moves to Tier 2; Tier 1 honestly delivers ~90 min.

Iterative refinement memory honoured: first plan was wrong on 5 critical points; v2 fixes them; v3 will fix what v2 missed.
