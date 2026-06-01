# Bootstrap ETL Redesign — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax. Design rationale + every grounded symbol live in the spec: `docs/proposals/etl/2026-06-01-bootstrap-etl-redesign-design.md` (read its §0 grep-proof before touching code).

**Goal:** A fresh/empty `ebull` install bootstraps correct SEC + universe data in minutes (today ~1h) with zero per-CIK HTTP, bounded memory (no OOM at a 4GB executor budget), load-time validation gates, data-event-date watermarks, and a live timeline with ETA/heartbeat.

**Architecture:** Collapse the per-CIK SEC `sec_rate` lane (bootstrap stages S14/S15/S22/S23) onto the already-downloaded bulk archives (the operator-visible ownership rollup already reads bulk-backed observation tables); defer document bodies via the existing #1343 lazy-on-view mechanism; seed data-event-date watermarks from the bulk archives' own `filed_at`; gate completion on a load-time validation stage; surface live per-stage rate/ETA/heartbeat. The in-process per-lane ThreadPool dispatcher is **unchanged** — this is a `_BOOTSTRAP_STAGE_SPECS` + capability-graph + validation/observability change.

**Tech Stack:** Python 3.14 / psycopg3 / FastAPI / APScheduler (jobs process, settled #719) / PostgreSQL 17 (partitioned ownership) / React+TS frontend / pytest+xdist. SEC EDGAR bulk artifacts only.

**Phase = PR.** Each phase is an independent branch (`fix/<issue>-…` or `feature/<issue>-…`), self-reviewed, Codex checkpoint-2 before first push, bot-reviewed, merged on APPROVE + green. Codex checkpoint-1 already run on the spec AND on this plan.

**Phase dependency order (why this sequence):**
1. **P1 memory floor** — de-risks the OOM pillar independently, before any stage change. No behaviour change to data.
2. **P2 collapse the per-CIK lane** — the core win (minutes not hours). Depends on nothing but P0 (#1410, shipped).
3. **P3 watermark hardening** — invariants on the bulk-seed path; small, can land beside P2.
4. **P4 validation stage** — needs the P2 stage-set settled (validates the bulk-only output).
5. **P5 live timeline** — independent of P2–P4 data correctness; UI + payload.
6. **P6 clean-bootstrap drive** — runs the real fresh bootstrap; **calibrates P4's numeric floors** from the first clean run, verifies DoD 8–12.

---

## Phase 1 — Memory floor (no OOM at a 4GB executor budget)

**Issue:** file a `fix/<n>-bootstrap-memory-floor`. **Branch:** `fix/<n>-bootstrap-memory-floor`.

**Why:** `max_connections` is unset (PG default 100); the only reason `work_mem(16MB) × conns × hash_mem_multiplier(2.0)` doesn't OOM is that the 3 pools (#719) bound actual conns to ~18. Make the bound explicit (≈30) so pool drift can't reopen the exposure.

**NOTE (Codex ckpt-1 [HIGH], corrected):** companyfacts ingest is ALREADY memory-bounded — it commits the start-row at `sec_companyfacts_ingest.py:177` then uses a **per-CIK `with conn.transaction()`** (`:207`) committed independently per entry. There is NO single 1.38GB outer transaction (the 9-agent map was wrong; verified by reading the file). So NO intermediate-commit work is needed — P1 is ONLY the `max_connections` cap.

**Files:**
- Modify: `docker-compose.yml` (postgres `command:` block — add `-c max_connections=30`).
- Test: assert compose carries `max_connections` at the floor (extend an existing compose/runtime-config test if present, else add a tiny one).

- [ ] **Step 1.1 — Add `max_connections=30` to the compose command.** Append `- -c` / `- max_connections=30` to the `postgres` service `command:` list in `docker-compose.yml`, with a comment: `# bound work_mem×conns product (pools cap ~18; 30 = headroom) — provably fits the 4GB executor budget`.

- [ ] **Step 1.2 — Validate the throwaway boot.** Run:
```bash
docker rm -f ebull-pg-cfgtest >/dev/null 2>&1
docker run -d --rm --name ebull-pg-cfgtest -e POSTGRES_PASSWORD=test --shm-size=1g --tmpfs /var/lib/postgresql/data postgres:17 \
  $(docker compose config | awk '/command:/{f=1;next} f&&/^[[:space:]]*-/{gsub(/^[[:space:]]*-[[:space:]]*/,"");printf "%s ",$0} f&&/environment:/{exit}')
sleep 3; docker exec ebull-pg-cfgtest psql -U postgres -tAc "SHOW max_connections; SHOW max_locks_per_transaction;"
docker rm -f ebull-pg-cfgtest
```
Expected: `30` and `1024`. (If the awk extraction is brittle, pass the 14 `-c` flags explicitly as in the #1410 validation.)

- [ ] **Step 1.3 — Gates + Codex ckpt-2 + commit + push.** `uv run ruff check . && uv run ruff format --check . && uv run pyright && uv run pytest` (or testmon scope). `codex exec` review the branch. Commit, push, open PR, poll bot+CI, resolve, merge.

**Acceptance:** fresh `docker compose up` boots with `max_connections=30` + `max_locks=1024`; no behaviour change to data; gates green. (companyfacts is already per-CIK-committed — no change.)

---

## Phase 2 — Collapse the per-CIK lane (bulk-only bootstrap)

**Issue:** `feature/<n>-bulk-only-bootstrap`. The core win.

**Drops (7 stages, each verified against code — see spec §4.1 table):** S14 `sec_submissions_files_walk`, S15 `filings_history_seed`, S17 `sec_def14a_bootstrap`, S19 `sec_insider_transactions_backfill`, S20 `sec_form3_ingest`, S22 `sec_13f_recent_sweep`, S23 `sec_n_port_ingest`. Bulk S8–S12 already produce the rollup data (S11 `sec_insider_dataset_ingest` docstring: "Replaces S9 `sec_insider_transactions_backfill` + S10 `sec_form3_ingest` on a fresh install"). S16 stays (DB-bound) with a new `follow_pagination=False` bootstrap param. S18/S21 stay (DB-bound metadata seed). S27 evaluated (likely drop — N-CSR worker-driven).

**Files:**
- Modify: `app/services/bootstrap_orchestrator.py` — `_BOOTSTRAP_STAGE_SPECS` (drop the 7 stages; update `assert len(...)`); delete their entries from `_STAGE_PROVIDES`/`_STAGE_PROVIDES_ON_SKIP`/`_STAGE_REQUIRES_CAPS`/`_STAGE_LANE_OVERRIDES`; delete `submissions_secondary_pages_walked` from the `Capability` Literal + S18/S21 `_STAGE_REQUIRES_CAPS`; **delete `_STRICT_CAP_PROVIDER_EXCLUSIONS[form3_inputs_seeded]`** (`:505-507`).
- Modify: `app/services/sec_insider_dataset_ingest.py` (S11) — surface a **Form-3-specific row count** in `InsiderIngestResult` (`:56`; it already splits form3/form4 via `_map_form_to_source` `:208`); ensure the orchestrator's cap accounting reads it so bulk S11 satisfies `form3_inputs_seeded` directly.
- Modify: `app/workers/scheduler.py` (`sec_first_install_drain` `:4790`, the hardcoded `follow_pagination=True` `:4879`) + `app/jobs/sec_first_install_drain.py` — add a `follow_pagination` param (default True for the steady-state safety-net; bootstrap dispatch passes `False` via `JOB_INTERNAL_KEYS`, like `use_bulk_zip`) gating the secondary-page HTTP walk.
- Create: master.idx recent-window gap-close stage (reuse `sec_master_idx_quarterly_sweep` logic — current+prev quarter, one download/quarter, zero per-CIK) seeding `filing_events`/`sec_filing_manifest` only.
- Modify: FE stage list + runbook + stage-count assertions.
- Test: `tests/test_bootstrap_orchestrator.py` (provider-subset invariants); `tests/test_bootstrap_rows_processed_gates.py` (currently asserts legacy S20 satisfies `form3_inputs_seeded` — **flip to bulk S11**).

- [ ] **Step 2.1 — Provider-subset invariant test FIRST (Codex ckpt-1 [HIGH]).** In `tests/test_bootstrap_orchestrator.py`, assert every key of `_STAGE_PROVIDES`, `_STAGE_PROVIDES_ON_SKIP`, `_STAGE_REQUIRES_CAPS`, `_STAGE_LANE_OVERRIDES` is a `stage_key` in `_BOOTSTRAP_STAGE_SPECS`. Run; expect PASS on `main` — the guardrail that FAILS the moment a stage is dropped without cleaning its cap-dict entries.

- [ ] **Step 2.2 — Remove `form3_inputs_seeded` end-to-end (verified design, supersedes "expose count").** VERIFIED against code: (a) bulk S11 `sec_insider_dataset_ingest` writes Form-3 initial-holdings directly to `ownership_insiders_observations` via the NONDERIV_HOLDING secondary path (`:593-639`, Form 3 unguarded/uncapped); (b) S24 `ownership_observations_backfill` is the #909 legacy→observations mirror (`sync_all`) — under bulk-only the legacy `insider_initial_holdings` table (S20's output) is empty so its form3 mirror is a no-op, while the form3 observations already exist from bulk S11; (c) **only S24 requires `form3_inputs_seeded`** (grep `_STAGE_REQUIRES_CAPS`). So the cap is a redundant legacy-path gate. The cap engine reads ONE aggregate `rows_processed` per stage-key (`_provider_meets_floor` `:709`), which is exactly why the per-cap distinction can't be expressed without rearchitecture — so don't try; remove the cap instead. **Delete `form3_inputs_seeded` from:** the `Capability` Literal (`:288`), `_STAGE_PROVIDES["sec_form3_ingest"]` (drops with S20 in Step 2.5), `_CAPABILITY_MIN_ROWS` (`:467`), `_STRICT_CAP_PROVIDER_EXCLUSIONS` (`:505`), and S24's `_STAGE_REQUIRES_CAPS.all_of` (`:641` — keep `cik_mapping_ready, insider_inputs_seeded, institutional_inputs_seeded, nport_inputs_seeded`; bulk S11/S10/S12 provide those). Update `tests/test_bootstrap_rows_processed_gates.py` (it asserts legacy S20 satisfies the cap — that test goes away) + the `_STRICT_CAP_PROVIDER_EXCLUSIONS` reference in `test_bootstrap_orchestrator.py:1414` if present. TDD: the existing `test_every_required_capability_has_a_provider` + Step-2.1 invariant must still pass with `form3_inputs_seeded` gone.

- [ ] **Step 2.3 — S16 `follow_pagination=False` bootstrap param (Codex ckpt-1 [HIGH]).** Add the param (default True). Bootstrap dispatch passes False. TDD: a bootstrap-context drain makes ZERO secondary-page (`CIK<10>-submissions-<NNN>.json`) HTTP calls and seeds the manifest purely from bulk `filing_events`. S16 no longer provides `submissions_secondary_pages_walked`.

- [ ] **Step 2.4 — master.idx recent-window gap-close stage.** New stage runs `sec_master_idx_quarterly_sweep` current+prev-quarter once, seeding `filing_events`/`sec_filing_manifest` for post-bulk-cutoff accessions. Zero per-CIK. Advances ONLY filing-metadata source watermarks (not ownership — P3). TDD with a fixture master.idx.

- [ ] **Step 2.5 — Drop the 7 stages + clean all four cap dicts + delete `submissions_secondary_pages_walked` + S18/S21 requires.** Update `assert len(...)`. Step-2.1 invariant + the existing `_capability_is_dead` catalogue test must pass. Re-homed caps: `filing_events_seeded`→S8, `insider_inputs_seeded`→S11, `form3_inputs_seeded`→S11 (Step 2.2), `institutional_inputs_seeded`→S10, `nport_inputs_seeded`→S12.

- [ ] **Step 2.6 — Verify strict-cap floors met by bulk rows (#1225).** `_resolve_stage_rows` source-1 (`bootstrap_archive_results`) yields `rows_processed ≥ _CAPABILITY_MIN_ROWS` for S10/S11/S12. Extend `tests/test_bootstrap_rows_processed_gates.py`.

- [ ] **Step 2.7 — Evaluate S27** (`sec_n_csr_bootstrap_drain`): confirm it provides no required cap (it requires `class_id_mapping_ready` from S26); if N-CSR fund-metadata is worker/lazy and the panel funds slice renders from bulk N-PORT (S12), drop S27 from bootstrap. Keep S26 (`mf_directory_sync` — bounded directory fetch). FE stage list + runbook + source registry (`tests/test_job_registry.py`, #1336) updated for any new stage `job_name`.

- [ ] **Step 2.8 — Gates + Codex ckpt-2 + pre-PR-fresh-agent-review (mandatory for filings ETL) + push + review cycle.** DoD 8–12 batched into P6.

**Acceptance:** bootstrap runs with ZERO per-CIK HTTP in the stage path (incl. S16 secondary pages); the catalogue + provider-subset invariants pass; `ownership_observations_backfill` (S24) still satisfied (form3 from bulk S11); the panel renders from bulk (verified in P6).

---

## Phase 3 — Watermark hardening (data-event-date, per-source)

**Issue:** `fix/<n>-bootstrap-watermark-hardening`. Small; may land beside P2.

**Files:**
- Modify: the bulk extractors that feed `record_manifest_entry`/`seed_scheduler_from_manifest` (`sec_submissions_ingest.py`, `sec_13f_dataset_ingest.py`, `sec_nport_dataset_ingest.py`, `sec_companyfacts_ingest.py`) — ensure they pass the archive's real `filed_at`/`period_of_report`, never load time.
- Test: `tests/test_data_freshness*.py` — invariant: after a bulk ingest, `data_freshness_index.last_known_filed_at` == `MAX(archive filed_at)`, NOT `now()`.
- Doc: spec §4.3 already states bulk-refresh-vs-discovery watermark ownership; add a code comment at the bulk-refresh jobs confirming they do NOT advance the freshness watermark (discovery layers own it).

- [ ] **Step 3.1 — Failing test: load-timestamp contamination guard (#650).** Seed a synthetic archive whose `filed_at` is 5 days before `now()`; ingest; assert `last_known_filed_at` equals the archive `filed_at` (within the day), not `now()`. Run; expect PASS if already correct (map_4 says the 3 writers are data-derived) — if PASS, this becomes a regression lock; if FAIL, fix the extractor.

- [ ] **Step 3.2 — Per-source advancement invariant (Codex ckpt-1 [HIGH]).** Assert the master.idx gap-close (P2 Step 2.3) advances ONLY filing-metadata source watermarks (`sec_8k`/`sec_10k`/`sec_def14a`/`sec_13d`/`sec_13g`), NOT the ownership-source watermarks (`sec_13f_hr`/`sec_n_port`), which advance only from the bulk observation tables actually loaded.

- [ ] **Step 3.3 — Gates + Codex + review cycle.**

**Acceptance:** no watermark is ever set from `now()`/load-time; ownership-source watermarks reflect only loaded observation data; filing-metadata watermarks reflect the master.idx gap-close.

---

## Phase 4 — Load-time validation stage

**Issue:** `feature/<n>-bootstrap-validation-stage`. Needs P2's stage-set settled.

**Files:**
- Modify: `app/services/bootstrap_orchestrator.py` — add a terminal validation stage (lane `db`/new `validation` lane; register in source registry, #1336); `_STAGE_REQUIRES_CAPS` so it runs after all data stages.
- Create: `app/services/bootstrap_validation.py` — the three checks.
- Migration (≥180): `bootstrap_runs.validation_gate_status` TEXT + CHECK mirroring the `stream_c_gate_status` ESCAPE-LIKE pattern (sql/173) — verdict column, NOT a new status enum.
- Modify: `app/services/reconciliation.py` — possibly `register_check` an offline panel reconciliation.
- Test: `tests/test_bootstrap_validation.py`.

- [ ] **Step 4.1 — Row-count floors (absolute, not spike-ratio).** `check_row_count_spike` no-ops on first install (no prior run) — instead assert absolute floors per `ownership_*_current`/observations table + `financial_facts_raw` + `filing_events`. **Floor VALUES are calibrated from P6's first clean run** (this step ships the mechanism + a conservative placeholder floor of `>0` per bulk-backed table; P6 tightens). A breach below a hard floor errors the stage.

- [ ] **Step 4.2 — Panel render (per-slice tolerant, Codex ckpt-1 [MED]).** Call `get_ownership_rollup` inside `snapshot_read` for AAPL/GME/MSFT/JPM/HD; assert `banner.state != 'no_data'` + `shares_outstanding` present + bulk-backed slices (insiders/institutions/funds/treasury) reconcile within one snapshot. Do NOT require blockholder/def14a slices (deferred). Per-instrument tolerant.

- [ ] **Step 4.3 — ≥1 cross-source reconciliation.** Prefer an OFFLINE golden-file check over live-SEC `run_spot_check` at the bootstrap tail (rate/flakiness). If live, sample only the panel set + respect the 9 req/s budget.

- [ ] **Step 4.4 — Verdict mapping (no new status enum).** Hard-floor failure → stage error → `finalize_run` counts it → `bootstrap_state.status='partial_error'` (gate stays closed). Soft warnings → stage success + write `validation_gate_status`. Preserve the FOR-UPDATE lock order (runs before state).

- [ ] **Step 4.5 — Gates + Codex + pre-PR-fresh-agent-review + review cycle.**

**Acceptance:** a clean run with healthy data → validation stage success → `complete`; a deliberately-starved run (drop a bulk table) → stage error → `partial_error`, gate closed.

---

## Phase 5 — Live timeline (#1409)

**Issue:** `feature/<n>-bootstrap-timeline-eta-heartbeat`. Independent of P2–P4 data.

**Files:**
- Modify: `app/api/processes.py` (`BootstrapTimelineStageResponse` — add `last_progress_at`, server-computed `rate`, `eta`); `app/services/processes/bootstrap_adapter.py`.
- Modify: bulk ingesters (S8–S12) to call `set_stage_target` up front (zip entry count / cohort length) + tick `set_stage_processed`.
- Modify: `frontend/src/pages/ProcessDetailPage.tsx` — render rate/ETA/heartbeat-age + stale chip; poll briefly through terminal + "last refreshed".
- Test: `tests/test_api_processes_timeline.py` + a FE unit test.

- [ ] **Step 5.1 — Add `last_progress_at` to the timeline payload** (currently dropped) + server-side `rate = processed_count/(last_progress_at − started_at)`, `eta = (target_count − processed_count)/rate` (NULL `target_count` ⟹ rate-only, no fake 100%). `processed_count` is ABSOLUTE (diff against `started_at`).
- [ ] **Step 5.2 — Bulk stages set `target_count` up front + tick `processed_count`** (fixes "0/N looks dead"; #1225 NULL-rows_processed ingesters must tick).
- [ ] **Step 5.3 — Per-stage heartbeat age + stale chip** (1800s bootstrap threshold from `stale_thresholds`); positive liveness signal (#1614).
- [ ] **Step 5.4 — Unify the two "latest run" reads** (`last_run_id` vs `ORDER BY id DESC`) → pick `last_run_id`; document. Guard rails: null-not-zero on DB failure (#150), fixed-string exceptions (#246), 503-not-401 (#256/#1325).
- [ ] **Step 5.5 — FE render + auto-poll through terminal + "last refreshed".**
- [ ] **Step 5.6 — Gates (incl. `pnpm --dir frontend typecheck && test:unit`) + Codex + review cycle.**

**Acceptance:** operator sees per-stage rows/sec + ETA + "updated Ns ago" + stale chip; a slow-but-alive stage is visibly distinct from a dead one; view auto-refreshes and doesn't freeze on terminal.

---

## Phase 6 — Clean-bootstrap drive (DoD 8–12)

**Issue:** operator-driven; no new code except P4 floor calibration (a follow-up commit to P4's branch or a tiny `fix/<n>-validation-floors` PR).

- [ ] **Step 6.1 — Pre-flight:** verify migration ≥179 applied on dev (`schema_migrations` PK=`filename`); PG healthy (not in recovery); compose `command:` live (`max_connections=30`, `max_locks=1024`); jobs worker on the merged redesign code (restart to reload — no auto-reload).
- [ ] **Step 6.2 — Trigger** a fresh/empty bootstrap (`POST /system/bootstrap/run`). Monitor DB-direct on `bootstrap_runs`/`bootstrap_stages` + `docker stats` (session cookie expires ~45min; reads don't refresh it).
- [ ] **Step 6.3 — Measure** wall-clock (target: minutes), PG memory peak (must not OOM; ideally fits a 4GB executor budget), per-source row counts → **calibrate P4 absolute floors** + ship them.
- [ ] **Step 6.4 — Verify DoD 8–12:** panel renders for AAPL/GME/MSFT/JPM/HD via `/instruments/{symbol}/ownership-rollup`; ≥1 cross-source reconciliation; watermarks set on data-event date; steady-state incremental picks up only post-watermark.
- [ ] **Step 6.5 — Record** every figure + commit SHA in the relevant PR descriptions (clauses 8–12).

**Acceptance (overall DoD):** fresh `ebull` → bulk-only bootstrap in minutes → no OOM → validation gates pass → panel renders + cross-source ok → timeline live with ETA/heartbeat → watermarks on data-event date → steady-state picks up only post-watermark. 100% confidence the data is correct.

---

## Self-review (spec coverage)

- Pillar 1 (bulk-only) → P2. Pillar 2 (memory) → P1 + P2 lane caps. Pillar 3 (watermarks) → P3. Pillar 4 (validation) → P4. Pillar 5 (timeline) → P5. DoD 8–12 → P6.
- Codex ckpt-1 findings folded: cap-dict subset invariant (P2 Step 2.1), per-source watermark advancement (P3 Step 3.2), per-slice-tolerant panel gate (P4 Step 4.2), form3 mandatory metadata-seed (P2 Step 2.2), §0 corrections (in spec).
- Floor VALUES (P4) intentionally calibrated in P6 — documented dependency, not a placeholder.
