# Bootstrap ETL redesign — design spec

- **Status:** PROPOSAL (unshipped). Supersedes the draft-1 `docs/proposals/etl/bulk-first-bootstrap.md` (which contained a hallucinated `_FILER_FORM_ALLOWLIST` — see §0).
- **Date:** 2026-06-01
- **Author:** Claude (week-6 ownership mandate)
- **Path note:** the mission named `docs/superpowers/specs/…`; that directory was migrated to `docs/specs/`/`docs/proposals/` and no longer exists, so this lands at `docs/proposals/etl/` (current convention for unshipped, co-located with the prior draft).
- **Grounding:** 9-agent read-only code map (`wf_37865540-844`); every symbol below verified at write time per data-engineer SKILL §0.0.

---

## §0 Grep proof (mandatory — Codex does NOT re-grep; it trusts what is pre-checked here)

Every table / column / function / constant this spec relies on, verified at write time:

### Orchestrator + stages
- `_BOOTSTRAP_STAGE_SPECS` — `app/services/bootstrap_orchestrator.py:1091-1261`, `tuple[StageSpec,…]` **len == 27** (asserted at `:2994`). Stage docstrings saying 24/26 are STALE — trust the assert.
- `StageSpec` = **5 fields only**: `stage_key, stage_order, lane, job_name, params` (`.claude/skills/data-engineer/etl-stage-declaration.md`). `row_budget` / `fetch_strategy` / `max_http_count` are PROPOSED/rejected — **do not reference as existing** (§6.5.16 hallucinated-API class).
- `_STAGE_REQUIRES_CAPS` `:543-675`; `_STAGE_PROVIDES` `:369-411`; `_STAGE_PROVIDES_ON_SKIP` `:420-446`; `_ORDERING_ONLY_CAPS` `:529-536` = `{submissions_processed, insider_dataset_processed, institutional_dataset_processed, nport_dataset_processed}`; `_CAPABILITY_MIN_ROWS` `:467-479`; `_STRICT_CAP_PROVIDER_EXCLUSIONS` `:505-507` = `{form3_inputs_seeded: {sec_insider_ingest_from_dataset}}`.
- `Capability` Literal `:288-350` (16 members incl. `filing_events_seeded`, `submissions_secondary_pages_walked`, `*_inputs_seeded`, `*_dataset_processed`, `bulk_archives_ready`, `cik_mapping_ready`, `cusip_mapping_ready`, `fundamentals_raw_seeded`, `class_id_mapping_ready`).
- `_LANE_MAX_CONCURRENCY` `:239-272` — every lane **= 1**. `Lane` Literal `app/jobs/sources.py:62-76` (13 members). `_STAGE_LANE_OVERRIDES` `:~1056-1069` (db-family split #1141).
- `_phase_batched_dispatch` `:1848-2329` — in-process per-lane `ThreadPoolExecutor` poll loop; **no pg_notify / queue**. `_run_one_stage` `:1432-1675`; `_resolve_stage_rows` `:1321-1429`; `run_bootstrap_orchestrator` `:2732-2963`.
- `_resolve_dynamic_params` + sentinels `:157-211`: `_13F_HR_CUTOFF` (`min_last_13f_hr_at`) and `_NPORT_CUTOFF` (`min_last_seen_filed_at`) → `datetime.now(tz=UTC).date() - 380d` at UTC midnight; **but `_13F_CUTOFF` (`min_period_of_report`) still uses `date.today()` (LOCAL tz)** — a pre-existing inconsistency (Codex ckpt-1; the 9-agent map over-claimed all-UTC). Moot in bootstrap once S22 is dropped, but flag for a tech-debt UTC-fix ticket. `_BOOTSTRAP_13F_RECENCY_DAYS=380` (`:128-129`), `_BOOTSTRAP_NPORT_RECENCY_DAYS` alias (`:137`, namespaced per #1451).

### State machine + completion
- `bootstrap_state` singleton `sql/129:101-114` (id=1 CHECK); `status` CHECK = `('pending','running','complete','partial_error','cancelled')` — **`'cancelled'` IS included**, widened by `sql/136:40-42` (the 9-agent map claimed it was excluded; Codex ckpt-1 corrected this against the live ALTER). `BootstrapStatus` Literal `app/services/bootstrap_state.py:48` includes `'cancelled'`.
- `bootstrap_runs` `sql/129:65-77` + ALTERs 136/167/169/170/173. Cols incl. `status`, `completed_at`, `coverage_floor_met` (167), `coverage_floor_ratio` NUMERIC(5,4) (173), `stream_c_gate_status` TEXT (173, CHECK `LIKE 'failed\_%' ESCAPE '\'`), `params` JSONB (169). **No `updated_at`/heartbeat column.**
- `bootstrap_stages` `sql/129:79-99` + ALTERs; progress cols `processed_count`/`target_count`/`last_progress_at`/`warnings_count`/`warning_classes` (`sql/140:61-66`), `target_cohort_fingerprint` (`sql/178`). `StageStatus` Literal `bootstrap_state.py:50`.
- `finalize_run` `bootstrap_state.py:944` — terminal = `partial_error` if `COUNT(status IN ('error','blocked','cancelled')) > 0` else `complete`. **This is the entire completion criterion today — no row-count / panel / reconciliation.**
- `reset_failed_stages_for_retry` `:1046-1159`; `force_mark_complete` `:1162`; `ensure_bootstrap_state_singleton` `:232` (issue **#1232**, autocommit-only; the singleton-guard rule is prevention-log **#1350**).
- `set_stage_target` `:~783-823` / `set_stage_processed` `:830-857` (absolute count, bumps `last_progress_at=now()`). `BootstrapProgressContext` / `resolve_progress_context` `:895-940`.
- `_bootstrap_complete` `app/workers/scheduler.py:451` = `SELECT EXISTS(... bootstrap_state WHERE id=1 AND status='complete')` — the gate ~20 ScheduledJobs prereq on.

### Bulk download + ingest
- `build_bulk_archive_inventory` `app/services/sec_bulk_download.py:239` (`n_quarters_13f=4, n_quarters_insider=8, n_quarters_nport=4`). Emits ONLY: `submissions.zip` (`:250`, ~1.54GB), `companyfacts.zip` (`:254`, ~1.38GB), `form13f_*.zip ×4` (`:261`), `<q>_form345.zip ×8` (`:268`), `<q>_nport.zip ×4` (`:275`). **No full-index/form.idx, no master.idx, no DERA financial-statement-datasets.**
- `assert_archive_belongs_to_run` `:1206`; `_ACCEPTED_REUSE_REASONS` `:1203` = `{'downloaded_in_run','etag_match_sha256_verified'}`; `write_run_manifest` `:1139`; `_FORCE_REDOWNLOAD_ENV` `:393` = `BOOTSTRAP_FORCE_REDOWNLOAD`; `DEFAULT_BANDWIDTH_THRESHOLD_MBPS=5.0` `:72`; `_stream_to_partial` `:1105` (1MB chunks).
- `ingest_submissions_archive` (`sec_submissions_ingest.py`): issuer CIK → **entire `filings.recent` block, ALL forms** → `filing_events` (10y cap); filer CIK → `sec_filing_manifest` for `_FILER_COHORT_FORMS` subset. **`files[]` overflow pages NOT followed** (`:588-598`). `KNOWN_FILING_AGENT_CIKS` skipped (`:193,578`).
- Bulk ingesters: `sec_companyfacts_ingest.py` → `financial_facts_raw` (XBRL numerics only, single outer tx, commit at `:271` — memory high-water mark); `sec_13f_dataset_ingest.py` → `ownership_institutions_observations` (`:356`, streaming COPY `_iter_tsv` `:266` + TEMP `_stg_13f` ON COMMIT DROP); `sec_nport_dataset_ingest.py` → `ownership_funds_observations` (`:373`; docstring `:20` "Replaces S14 sec_n_port_ingest entirely on a fresh install"); `sec_insider_ingest_from_dataset.py` → insider obs.
- `top_filer_discovery.py:96` `fetch_form_index` / `_FORM_IDX_URL='…/full-index/{year}/QTR{q}/form.idx'` — **only form.idx consumer; urllib, no ETag/SHA/manifest reuse.**

### Per-CIK lanes (collapse targets)
- `KNOWN_FILING_AGENT_CIKS` `app/providers/implementations/sec_edgar.py:98-110` — **9-member frozenset**, 10-digit zero-padded (`_zero_pad_cik:113`). Enforced by `scripts/check_archive_url_agent_guard.sh`. **No `_FILER_FORM_ALLOWLIST` exists** (draft-1 hallucination).
- S15 `filings_history_seed` reads only `filings.recent` (= S8 bulk output; own docstring: "fallback / idempotent no-op after bulk"). S22 `sec_13f_recent_sweep` writes `ownership_institutions_observations` (= bulk S10 table) **and** legacy `institutional_holdings` (`institutional_holdings.py:914`). S23 `sec_n_port_ingest` writes `ownership_funds_observations` (= bulk S12); already skips bulk-seeded accessions via `n_port_ingest_log` (#1340).
- **`institutional_holdings` IS read live** at `app/api/instruments.py:3457` `get_instrument_institutional_holdings` (`FROM institutional_holdings` `:3517/3568/3605`) — separate from the rollup. Bulk S10 does **NOT** seed `institutional_holdings_ingest_log` (verified empty grep) → S22 has no bulk-skip.
- Lazy-on-view (#1343) already built for S18 (`business_summary.py:1024` `fetch_business_summary_body_now`) + S21 (`eight_k_events.py:578` `fetch_eight_k_body_now`); manifest seeded `'deferred'` at `sec_first_install_drain.py:280`. **S17/S19/S20 still fetch bodies eagerly.**

### Watermarks + freshness (pillar 3 — already correct)
- `data_freshness_index` `sql/120` — `last_known_filed_at` is the watermark; written by 3 writers ALL data-event-derived: `seed_scheduler_from_manifest` (`data_freshness.py:245`, `MAX(manifest.filed_at)`), `seed_freshness_for_manifest_row` (`:136`, monotonic newer-wins from `filed_at`), `record_poll_outcome` (`:340`, `COALESCE(delta.last_filed_at)`). Only `expected_next_at` is `now()`-anchored (a PREDICTION via `predict_next_at` `:116`). `source` CHECK widened in `sql/153` (lock-step with manifest).
- `ownership_refresh_state` `sql/163` — `last_drained_observations_max_ingested_at` = `MAX(observations.ingested_at)`; 7-category CHECK; drift watermark (#1513). `ownership_observations.py` MERGE writers (insiders `:205` … esop `:1589`), clamp on BOTH ON + `NOT MATCHED BY SOURCE` DELETE (#1504), `refreshed_at` excluded from `IS DISTINCT FROM` (#1513).
- `external_data_watermarks` `sql/034` — `watermark_at` from provider Last-Modified, `fetched_at=now()` display-only (#650). `set_watermark` `watermarks.py:133` requires INTRANS.
- `sec_rebuild.py:117` — only place `last_known_filed_at` is set NULL (intentional reset).
- Steady-state: Layer 1 `sec_atom_fast_lane` (5min, prereq complete); Layer 2 `sec_daily_index_reconcile` (daily 04:00, **prereq None + `exempt_from_universal_bootstrap_gate=True`** #1181, reads yesterday's master.idx date-bounded); Layer 3 `sec_per_cik_poll` (hourly, prereq complete, If-Modified-Since); G12 `sec_master_idx_quarterly_sweep` (weekly, current+prev quarter full); bulk refreshers `sec_submissions/companyfacts_bulk_refresh` (daily) `sec_quarterly_datasets_bulk_refresh` (monthly) — HEAD ETag vs `.zip.etag`, bootstrap-fenced (`sec_bulk_refresh.py:228`).

### Completion-gate reusables
- `get_ownership_rollup` `app/services/ownership_rollup.py:1222` (MUST run inside `snapshot_read`); `OwnershipRollup` `:184` (`banner`, `slices`, `residual`, `coverage`); `no_data` banner when `shares_outstanding` None/≤0. Endpoint `app/api/instruments.py:4264`.
- `check_row_count_spike` `app/services/sync_orchestrator/row_count_spikes.py:39` (ratio 0.5 vs prior successful run → **no-ops on first install**, no prior run). `reconciliation.run_spot_check` / `register_check` `app/services/reconciliation.py:526/105` (only `shares_outstanding_freshness` registered `:401`; hits LIVE SEC). `stream_a_stream_c_gate.py` 7-check post-complete attestation (NOT wired to `finalize_run`). `CATEGORY_TO_MANIFEST_SOURCES` `capability_manifest_mapping.py`.

### Timeline (pillar 5)
- `GET /processes/{id}/timeline` `app/api/processes.py:654` (`BootstrapTimelineStageResponse` `:276-313` carries `processed_count`/`target_count`/`target_cohort_fingerprint` but **NOT `last_progress_at`/rate/ETA**). `bootstrap_adapter._build_active_run` `:260` (only place `MAX(last_progress_at)` surfaces, run-level). `stale_thresholds`: DEFAULT 300s, bootstrap override 1800s (`stale_thresholds.py:29,40`). FE `ProcessDetailPage.tsx:158-163` (5s poll, only while `status==='running'`; bar hides at `:1191` when `processed===0 && no target`).

### Manifest enums + memory model
- `sec_filing_manifest` CURRENT enums: `source` 15 members (`sql/153:49-61`), `ingest_status` 6 incl. `'deferred'` (`sql/179:69-71`), `subject_type` 5 (`sql/118:50-56`), `raw_status` 3 (`sql/118:103-107`). `chk_manifest_issuer_has_instrument` (`sql/118:121`) — non-issuer ⟹ `instrument_id IS NULL`. `record_manifest_entry` `sec_manifest.py:215` (`initial_ingest_status` INSERT-only). `_ALLOWED_TRANSITIONS` `:153-184`. `iter_pending`/`iter_retryable` never select `'deferred'`.
- `is_transient_upsert_error` `manifest_parsers/_classify.py:41` = `isinstance(exc, OperationalError)` → retry; else tombstone (#1271).
- `open_pool` `app/db/pool.py:41` — pools: API `db_pool(min1,max10)` + `audit_pool(min1,max2)`; jobs `jobs_pool(min1,max4)` + fence + heartbeat ≈ **~18 backend conns** vs `max_connections=100` (PG default, **unset**). `work_mem=16MB`, `hash_mem_multiplier=2` (verified live), `shared_buffers=2GB`, `mem_limit=6g`. `PG_LOCKS_FLOOR=1024` `pg_settings.py:31`; ~431 relation locks per unpruned 125-partition parent SELECT.
- Migration discipline: highest file = **179** (dev `schema_migrations` PK=`filename`); autocommit directive line-1 `-- runner: autocommit` (#1376); CHECK-in-ALTER (#1601); PG≥17 guard `postgres_version_guard.py` (MERGE `WHEN NOT MATCHED BY SOURCE`).

---

## 1. Problem & goals

Today a fresh/empty `ebull` bootstrap downloads bulk archives in minutes, then spends **~1 hour** in stages S14–S23 making ~10k–25k rate-limited **per-CIK HTTP** calls to SEC — re-deriving data the bulk archives already contain — and has OOM-killed its own Postgres mid-run. The completion signal is stage-success-only; nothing validates the data is correct, so data bugs surface in week 8 not at load.

**Goal (DoD):** fresh `ebull` → **bulk-only** bootstrap in **minutes** → bounded memory (no OOM, target survives a 4GB executor budget) → validation gates pass (row-count floors + AAPL/GME/MSFT/JPM/HD panel renders + ≥1 cross-source reconciliation) → live timeline with rows/sec + ETA + heartbeat (working-slowly vs dead obvious), auto-refresh → watermarks on **data-event date** → steady-state incremental picks up only post-watermark filings. Operator has 100% confidence the data is right.

**Five pillars** (approved direction, not re-litigated):
1. Bootstrap = BULK-ONLY (zero per-CIK HTTP in the bootstrap path).
2. Streams parallel by default, sequential only on real data deps; memory-bounded (no OOM at 4GB executor budget).
3. Watermark per source = MAX data-event date covered, never `now()`/completion.
4. Validation gates at load (row-count + panel + cross-source).
5. Live timeline: per-stage done/total + rows/sec + ETA + heartbeat + auto-refresh (#1409).

---

## 2. Settled decisions that apply (and how this preserves them)

- **#719 process topology** — orchestrator stays in the jobs process; IPC Postgres-only; no in-process scheduling in the API; pools via `open_pool`. Preserved: the dispatcher is unchanged; no new pool.
- **#1064 universal bootstrap-state gate** — bootstrap stages bypass the gate (not in `SCHEDULED_JOBS`); steady-state jobs are blocked until `bootstrap_state.status='complete'`. Preserved + load-bearing: the validation gate (pillar 4) is what makes `complete` trustworthy before the gate opens.
- **#1181 carve-out** — `sec_daily_index_reconcile` is the only `exempt_from_universal_bootstrap_gate` job; it closes the bulk-lag gap daily even pre-complete. Preserved; we lean on it (§5.3) rather than duplicate.
- **#1102 CIK=entity/CUSIP=security; #1171 fund-metadata priority; fundamentals `as_of_date`=period-end** — the data-event-date watermark (pillar 3) is endorsed by the `as_of_date` decision. Bulk fan-out respects share-class siblings.
- **Bulk archive reuse (ETag+SHA-256) + `assert_archive_belongs_to_run`** — reused as-is; new archive families (if any) extend `build_bulk_archive_inventory` + the same provenance plumbing.
- **OpenFIGI CUSIP fallback (dedicated `openfigi` lane)** — S13 unchanged; the only non-bulk "directory" call class permitted in bootstrap.
- **'partial_complete' status was deliberately rejected** (sql/167 + Codex v3) — the validation verdict goes in a **column**, never a new status enum (§4.4).

No settled decision needs changing.

---

## 3. Prevention-log entries that apply

Binding on this work: **#1480** (grep `KNOWN_FILING_AGENT_CIKS` before any archive-URL flow), **#1488** (`sec_filing_manifest` CHECKs before row shapes; non-issuer ⟹ `instrument_id` NULL), **#1601** (CHECK lives in ALTER too — manifest enums in sql/153+179), **#1540** (no phantom columns — §0 is the proof), **#1330** (universal gate vs `prerequisite=None` — preserve opt-outs explicitly), **#1336** (every new `job_name` must register in the source registry or `JobLock` crashes at boot), **#1451** (per-source recency constants namespaced), **#1010/#1347** (UTC date, not `date.today()`), **#1504/#1513** (MERGE clamp on both clauses; `refreshed_at` excluded from diff), **#1265/#1271** (transient vs deterministic parser errors), **#1376** (ALTER SYSTEM → autocommit directive), **#1394/#1361** (partition + retention sweep mega-tables; Docker macOS WAL-PANIC), **#1350** (singleton boot guard), **#1225** (`rows_processed=NULL` ⟹ below strict-cap floor → silently kills caps), **#1614** (positive liveness signal, not just a stale chip).

---

## 4. Design

### 4.1 Pillar 1 — Bulk-only bootstrap (collapse the per-CIK lane)

**What is ALREADY bulk-local and stays (S7–S13):** S7 download; S8 `sec_submissions_ingest` (issuer `filings.recent` → `filing_events`, all forms; filer cohort → manifest); S9 `sec_companyfacts_ingest` → `financial_facts_raw`; S10 `sec_13f_ingest_from_dataset` → `ownership_institutions_observations`; S11 insider dataset; S12 `sec_nport_ingest_from_dataset` → `ownership_funds_observations`; S13 OpenFIGI CUSIP sweep. These already write **event-dated** rows and are the operator-visible rollup source.

**Collapse decisions for S14–S23 + S26/S27 (each grounded in §0):**

| Stage | Decision | Rationale (code-grounded) |
|---|---|---|
| S15 `filings_history_seed` | **DROP from bootstrap** | Reads only `filings.recent` = byte-for-byte S8 bulk output; own docstring "no-op after bulk". Stays a steady-state safety-net job. |
| S22 `sec_13f_recent_sweep` | **DROP from bootstrap** | Writes `ownership_institutions_observations` = bulk S10 table → rollup renders from S10. No bulk-skip exists (no `institutional_holdings_ingest_log` seed). Legacy `institutional_holdings` (read at `instruments.py:3457`) backfills in steady-state. |
| S23 `sec_n_port_ingest` | **DROP from bootstrap** | Writes `ownership_funds_observations` = bulk S12 ("Replaces … entirely on fresh install"). Already skips bulk accessions. Steady-state job remains. |
| S14 `sec_submissions_files_walk` | **DROP from bootstrap** (defer overflow) | Walks `files[]` secondary pages (deep history beyond `recent`) — genuinely not in bulk, BUT prolific-filer overflow is not needed for the panel; deferred to steady-state Layer 2/3 + lazy. Provides `submissions_secondary_pages_walked` (dropped — see §4.1a). |
| S16 `sec_first_install_drain` | **KEEP but `follow_pagination=False` in bootstrap (Codex ckpt-1 [HIGH])** | Default path seeds `sec_filing_manifest` from bulk `filing_events` (DB-bound, no HTTP). BUT the invoker hardcodes `follow_pagination=True` (`scheduler.py:4879`) → secondary `CIK<10>-submissions-<NNN>.json` pages + zip-misses + non-issuer subjects hit HTTP. For TRUE zero-per-CIK, add a bootstrap-mode `follow_pagination=False` param (gated via `JOB_INTERNAL_KEYS` like `use_bulk_zip`): seed from `filing_events` only; secondary/overflow → steady-state. This also removes S16 as a `submissions_secondary_pages_walked` provider. |
| S17 `sec_def14a_bootstrap` | **DROP from bootstrap** | Provides NO capability (not in `_STAGE_PROVIDES`) → orphans nothing. DEF14A holder tables are body/manifest-worker-parsed (worker gated during bootstrap) → `ownership_def14a_current` legitimately empty at completion, fills post-complete (panel per-slice-tolerant, §4.4). No per-CIK HTTP in bootstrap. |
| S19 `sec_insider_transactions_backfill` (Form 4) | **DROP from bootstrap** | Bulk S11 `sec_insider_dataset_ingest` **explicitly replaces it** (docstring `:19`: "Replaces S9 `sec_insider_transactions_backfill` … on a fresh install"); writes `ownership_insiders_observations` (the rollup source). `insider_inputs_seeded` re-homed to S11 (already a co-provider). |
| S20 `sec_form3_ingest` | **DROP from bootstrap** | Bulk S11 also replaces it: Form 3 is in `form345.zip`, `_map_form_to_source` (`sec_insider_dataset_ingest.py:208`) maps Form 3/3-A→`form3`, and `:594-624` handle Form-3 initial-holdings → `ownership_insiders_observations`. The ONLY blocker is `_STRICT_CAP_PROVIDER_EXCLUSIONS[form3_inputs_seeded]`; §4.1a applies the fix the exclusion comment itself anticipates ("when per-family bulk row counts land … the bulk provider can satisfy form3 directly"). Zero per-CIK. |
| S18 `sec_business_summary_bootstrap`, S21 `sec_8k_events_ingest` | **KEEP as metadata-only seed stages** (already #1343-deferred) | At bootstrap they only seed typed metadata rows (`instrument_business_summary`/`eight_k_filings`, `body_deferred=TRUE`) from bulk `filing_events` — DB-bound, no HTTP. Bodies fetched lazily on view. Drop their `submissions_secondary_pages_walked` requirement (only need `filing_events_seeded`). |
| S26 `mf_directory_sync`, S27 `sec_n_csr_bootstrap_drain` | **Evaluate during P2** | S26 builds the mutual-fund directory (bounded index fetch, like S4/S5 — keep). S27 drains N-CSR (fund annual reports, body-parsed) → likely DROP (worker post-complete; fund-metadata panel slice tolerant). Confirm no required cap is orphaned before dropping. |

**Bulk-lag gap close — FILING-METADATA coverage only, NOT panel currency (Codex ckpt-1 [HIGH]):** the bulk `submissions.zip`/datasets lag real-time by days. A **recent-window index pass** reusing `sec_master_idx_quarterly_sweep` logic (current+prev quarter `master.idx`, one download/quarter, **zero per-CIK**) closes the gap in **filing-metadata coverage** — it seeds `sec_filing_manifest`/`filing_events` rows for accessions filed after the bulk cutoff. It does **NOT** make the ownership rollup real-time: the manifest worker is gated during bootstrap, so those rows are queued, not parsed, and the ownership observation tables (`ownership_institutions/funds/insiders_observations`) advance only when a new quarterly dataset drops or steady-state runs. **The ownership panel is therefore current-to-bulk-dataset-cutoff at completion, not current-to-now** — consistent with the existing depth-floor note (#1305: "13F≈12mo; deeper = re-bootstrap"). Real-time advance of the panel is steady-state's job (Layer 1/3 post-complete + the next quarterly drop). Do NOT claim "panel current at completion" — claim "panel renders correctly with bulk-cutoff data; filing-metadata coverage is closed to ~now". (Alternative to master.idx: add `full-index/form.idx` to `build_bulk_archive_inventory` + the ETag/SHA/manifest plumbing — heavier; master.idx reuse is preferred because the steady-state machinery already exists.)

#### 4.1a Cap re-homing (the orphaned-capability problem) — MANDATORY

**Cap-dict hygiene (Codex ckpt-1 [HIGH]):** `_CAPABILITY_PROVIDERS` (the dict the catalogue test + `_capability_is_dead` read) is built from `_STAGE_PROVIDES` **unfiltered by `_BOOTSTRAP_STAGE_SPECS`**. So removing a stage from `_BOOTSTRAP_STAGE_SPECS` while leaving its `_STAGE_PROVIDES` entry makes the catalogue test still pass (the provider key exists in the dict) while **runtime sees no status for that stage** → a cap can be silently treated dead or, worse, advertised by a stage that never runs. Therefore, dropping a stage MUST in the SAME PR delete its entries from `_STAGE_PROVIDES`, `_STAGE_PROVIDES_ON_SKIP`, `_STAGE_REQUIRES_CAPS`, and `_STAGE_LANE_OVERRIDES`. **Add a new invariant test: `set(_STAGE_PROVIDES) ⊆ {s.stage_key for s in _BOOTSTRAP_STAGE_SPECS}`** (and the same for the other three dicts) so a stale provider entry fails at test time, not runtime.

Dropping sec_rate stages orphans content caps; the catalogue-invariant test (`_capability_is_dead`) **fail-fasts** on an orphaned cap. Each must be re-homed onto a bulk producer in the same PR:
- `filing_events_seeded` → already provided by S8 success (currently S8/S15/S16). Drop S15/S14 providers; S8 remains the producer. ✓
- `submissions_secondary_pages_walked` → providers are S14 + S16-with-pagination. With S14 dropped AND S16 switched to `follow_pagination=False` (§4.1 table), **no producer remains**. **Decision:** delete the cap from `Capability`, delete S16's provider entry, and delete the `_STAGE_REQUIRES_CAPS` requirement on S18/S21 (they only need `filing_events_seeded`; S17 is dropped entirely). The Step-2.1 provider-subset invariant catches any leftover.
- `form3_inputs_seeded` → currently ONLY legacy S20 (bulk S11 excluded via `_STRICT_CAP_PROVIDER_EXCLUSIONS`). **Corrected decision (Codex ckpt-1 [MED]) — DROP S20, re-home onto bulk S11:** Form 3 lives in the bulk `form345.zip`; `sec_insider_dataset_ingest._map_form_to_source` already maps Form 3/3-A→`form3` and `:594-624` ingest Form-3 initial-holdings into `ownership_insiders_observations`. The strict exclusion exists ONLY because the bulk ingester records an *aggregate* `rows_processed` (can't prove ≥1 Form 3). Apply the fix its own comment anticipates: **expose a Form-3-specific row count from `sec_insider_dataset_ingest` (it already splits form3/form4 internally — surface it in `InsiderIngestResult`), then drop the `_STRICT_CAP_PROVIDER_EXCLUSIONS[form3_inputs_seeded]` entry** so bulk S11 satisfies the cap directly via its form3 count. Metadata-only-S20 was the wrong route (S20's real rows come from parsing Form 3 XML, which a deferred seed wouldn't produce). Update `tests/test_bootstrap_rows_processed_gates.py` (it currently asserts legacy S20 satisfies the cap).
- `institutional_inputs_seeded` (S10+S22) / `nport_inputs_seeded` (S12+S23) / `insider_inputs_seeded` (S11+S19) → bulk producers S10/S12/S11 remain; drop the legacy providers (S22/S23/S19). Verify `_CAPABILITY_MIN_ROWS` floors are met by bulk rows (#1225: a NULL `rows_processed` on the bulk stage silently kills the cap — `_resolve_stage_rows` source 1 must be load-bearing).

**Dispatcher:** unchanged. Dropping stages is a `_BOOTSTRAP_STAGE_SPECS` edit + cap-graph edit; `_phase_batched_dispatch` needs zero logic change. The 27→N stage-count assert + FE + runbook move in lockstep.

### 4.2 Pillar 2 — Streams, ordering, memory-bounded

- **Parallelism:** keep the existing lane model (`_LANE_MAX_CONCURRENCY`=1/lane, cross-lane parallel). eToro ∥ SEC bulk download ∥ FINRA; the db_* family lanes (`db_filings`/`db_fundamentals_raw`/`db_ownership_inst`/`_insider`/`_funds`) run the bulk ingesters in parallel. Sequential only on real deps via `_STAGE_REQUIRES_CAPS`: universe→cik_mapping_ready→keyed loads; cusip_mapping_ready→13F/NPORT.
- **Memory (no-OOM-at-4GB):**
  - **SET `max_connections≈30`** (compose `-c`, follow-up to #1410) so the work_mem×conns×hash_mem product is provably bounded regardless of pool drift. Actual conns ≈18; 30 = headroom. Today `max_connections=100` (verified) only fails to OOM because pools cap conns — make the bound explicit. **`mem_limit` stays ≥6g** (4g OOM-kills WAL recovery #1395); the "4GB" target is the executor budget, not the container limit.
  - Keep streaming COPY (`_iter_tsv`) for big TSVs; `_open_tsv` whole-load only for <100MB files; TEMP `_stg_*` ON COMMIT DROP; per-archive commit boundary (service ingesters MUST NOT open own tx — orchestrator owns the boundary, #915/#1208).
  - **companyfacts intermediate commits:** the single outer transaction over the 1.38GB archive is the high-water mark — add a commit every N CIKs (e.g. 1000) to bound undo/lock footprint. (Per-CIK savepoint stays.)
  - Keep partition-pruning predicates (`period_end` bounds) on every parent read — an unpruned 125-partition scan reserves ~431 locks; 2 concurrent ≈862 near the 1024 floor.

### 4.3 Pillar 3 — Data-event-date watermarks

**Finding: the invariant is already satisfied in steady-state.** `data_freshness_index.last_known_filed_at` and `ownership_refresh_state.last_drained_observations_max_ingested_at` are data-event-derived; only `expected_next_at` is `now()`-anchored (a prediction). The redesign **reuses** `predict_next_at`, `seed_scheduler_from_manifest`, the monotonic newer-wins gate — it does NOT invent watermark columns.

Two distinct concepts (do not conflate):
- **Cohort-depth floor** (wall-clock, e.g. 380/396/730d, gated on `resolve_progress_context()`) — bounds how far back bootstrap *loads*. Keep as-is (namespaced per #1451, UTC per #1010).
- **Data-event watermark** (`last_known_filed_at` = MAX filed_at provably covered) — bounds the steady-state *cursor*. Bootstrap seeds it from the bulk archives; steady-state advances it.

**Per-source advancement, never blanket (Codex ckpt-1 [HIGH]):** each source's watermark advances ONLY from that source's own loaded data — the 13F watermark from `MAX(period_of_report)` actually in `ownership_institutions_observations`, NPORT from what's in `ownership_funds_observations`, filing-metadata sources (8-K/10-K/DEF14A/13D-G) from `MAX(filed_at)` of manifest rows actually seeded (incl. the master.idx gap-close). Do **NOT** advance an ownership-source watermark from the master.idx filing-metadata pass — that would push the cursor ahead of the parsed observation data (the manifest worker is gated during bootstrap, so seeding a manifest row does not populate the observation table). This is exactly pillar 3's "advance only to what is provably covered", applied per source.

**What must be built/enforced:**
1. **Every bulk extractor passes the archive's real `filed_at`** (filing metadata), never load/extraction time, into `record_manifest_entry` / `seed_scheduler_from_manifest` (#650). This is the single contamination risk for pillar 3. Add an invariant test.
2. **Bulk-refresh jobs advance the freshness watermark.** Today `sec_*_bulk_refresh` only refresh the `.zip` + ETag sidecars; they do NOT call `seed_scheduler_from_manifest`/`record_manifest_entry`, so the freshness watermark advances only via the live-SEC discovery layers. Decide: either the bulk-refresh job re-extracts the changed archive into the manifest (advancing the watermark from the archive's filed_at), or document that the watermark is owned by the discovery layers and the bulk file is a separate cache. (Recommend: bootstrap seeds the watermark from bulk; steady-state Layer 2/3 + per-CIK poll own ongoing advancement; bulk-refresh remains a cache-freshness mechanism only — simplest, no double-writer.)
3. **Post-bootstrap continuity:** after `complete`, Layer 2 (exempt, daily) + Layer 1/3 (gated, post-complete) pick up filings with `event_date > last_known_filed_at`. The bulk-lag gap (§4.1) is closed at bootstrap so the watermark is current-as-of-completion.

### 4.4 Pillar 4 — Validation gates (catch data bugs at load)

**New: a validation stage** runs after all data stages (a new terminal stage in `_BOOTSTRAP_STAGE_SPECS`, `db`/validation lane). It performs:
1. **Per-source row-count floors** — ABSOLUTE floors (not `check_row_count_spike`, which no-ops on first install with no prior run). Floors per `ownership_*_current`/observations table + `financial_facts_raw` + `filing_events`, calibrated from the first clean run's baselines (recorded during the §6 drive). A breach below a hard floor = stage error.
2. **Panel render (per-slice tolerant — Codex ckpt-1 [MED]):** call `get_ownership_rollup` (inside `snapshot_read`) for AAPL/GME/MSFT/JPM/HD; assert `banner.state != 'no_data'` and `shares_outstanding` present. `get_ownership_rollup` reads SEVEN `ownership_*_current` tables, but only **insiders / institutions / funds** (bulk S11/S10/S12) + **treasury** (XBRL via S9/S25) are bulk-backed and loaded at completion. **`blockholders` (13D/G) and `def14a` are body/manifest-worker-driven (deferred during bootstrap) — they MAY be empty at completion and MUST NOT be gated.** So the gate = render OK + `shares_outstanding` present + at least the bulk-backed slices reconcile within one snapshot; per-instrument tolerant (GME/JPM/HD have different profiles). Treat blockholder/def14a/8-K emptiness as expected (fills via steady-state/lazy), not a failure.
3. **≥1 cross-source reconciliation** — reuse `reconciliation.run_spot_check` on a SMALL sample (the panel set) to respect the SEC rate budget; or an offline golden-file check (safer — no live-SEC flakiness at the bootstrap tail). Record findings to `data_reconciliation_findings`.

**Verdict mapping (honors the 'partial_complete' rejection):** the validation stage's hard-floor failure **errors the stage** → `finalize_run` counts it → `bootstrap_state.status='partial_error'` → the `_bootstrap_complete` gate stays closed. No new status enum. Soft warnings → stage success + a verdict written to a **column** (reuse the `stream_c_gate_status` / `coverage_floor_met` precedent; add `validation_gate_status` if a distinct signal is wanted). The detached **Stream-C gate** (steady-state-caught-up attestation) stays post-complete and unchanged.

### 4.5 Pillar 5 — Live timeline (#1409)

Backend (highest-leverage change first):
1. **Add `last_progress_at` to the timeline stage payload** (`BootstrapTimelineStageResponse`) — currently dropped. Compute server-side `rate = processed_count / (last_progress_at − started_at)` and `eta = (target_count − processed_count)/rate` (NULL `target_count` ⟹ unbounded ⟹ rate-only, no fake 100%). `processed_count` is ABSOLUTE (diff against `started_at`).
2. **Per-stage heartbeat age + stale chip** — surface `now − last_progress_at`; flag stale at the 1800s bootstrap threshold (already in `stale_thresholds`). Positive liveness signal, not just a stale chip (#1614).
3. **Every bulk stage sets `target_count` up front** (from the local archive: zip entry count / CIK-cohort length) and ticks `processed_count` — fixes "0/N looks dead" (the bar hides at `ProcessDetailPage.tsx:1191` when `processed===0 && no target`). #1225 bulk ingesters that write `rows_processed=NULL` must also tick.
4. **Auto-poll through a brief terminal window** + a "last refreshed" timestamp so a `partial_error`/`cancelled` run isn't a frozen frame.
5. **Unify the two "latest run" reads** (`/system/bootstrap-status` uses `last_run_id`; `/timeline` + `/system/bootstrap/status` use `ORDER BY id DESC`) — pick one (recommend `last_run_id` for consistency with the audit readout) and document.

Guardrails: null-not-zero on DB read failure (#150); fixed-string exceptions (#246); auth dep must not resolve conn eagerly → DB-down = 503 not 401 (#256/#1325).

---

## 5. Migrations & registry changes

- **compose:** add `-c max_connections=30` (pillar 2; follow-up to #1410). No new ALTER SYSTEM migration (compose `-c` is the source of truth).
- **New migration (≥180):** `validation_gate_status` column on `bootstrap_runs` (if a distinct verdict signal is chosen) — TEXT + CHECK, mirror the `stream_c_gate_status` ESCAPE-LIKE pattern.
- **Stage-set change:** `_BOOTSTRAP_STAGE_SPECS` shrinks (drop S14/S15/S22/S23; add validation stage); update the `len()` assert, FE stage rendering, runbook, and the lane-disjointness test. Any new stage `job_name` registers in `SCHEDULED_JOBS`/`_BOOTSTRAP_STAGE_SPECS`/`MANUAL_TRIGGER_JOB_SOURCES` (#1336).
- **No new manifest source** is required (the deferral reuses existing `sec_10k`/`sec_8k`/`sec_def14a`/`sec_form3`/`sec_form4` sources + `'deferred'`). If any new source appears, widen BOTH `sec_filing_manifest.source` AND `data_freshness_index.source` CHECKs in lock-step (sql/153 precedent) + the Python `ManifestSource` Literal.

---

## 6. Phasing (PR sequence)

- **P0 (shipped):** #1410 PG runtime tuning via compose `-c` (fresh-install boot + OOM-safe). 
- **P1 — memory floor:** compose `max_connections=30` + companyfacts intermediate commits + invariant test. Independently shippable; de-risks the OOM pillar before stage changes.
- **P2 — collapse the per-CIK lane:** drop S14/S15/S22/S23 from `_BOOTSTRAP_STAGE_SPECS`; re-home orphaned caps (§4.1a); extend #1343 deferral to S17/S19/S20; bulk-lag gap-close via master.idx recent-window. Catalogue-invariant test is the guardrail.
- **P3 — watermark hardening:** invariant test that every bulk extractor seeds `last_known_filed_at` from the archive's `filed_at`; document bulk-refresh-vs-discovery watermark ownership.
- **P4 — validation stage:** row-count floors + panel render + cross-source reconciliation; verdict column; error→partial_error wiring.
- **P5 — live timeline:** `last_progress_at`+rate+ETA+heartbeat in the payload; FE render; target_count up-front for bulk stages; auto-poll through terminal; unify latest-run read.
- **P6 — clean-bootstrap drive (DoD 8–12):** run a real fresh bootstrap; record per-source row counts (→ calibrate P4 floors); verify panel renders + cross-source; confirm watermarks on data-event date + steady-state pickup.

Each PR: branch → self-review → Codex checkpoint-2 → push → bot review → resolve → merge. Codex checkpoint-1 on this spec + on the implementation plan.

---

## 7. Risks & empirical verification still required

- **Panel sufficiency from bulk alone** — must verify (P6 drive) that the rollup renders fully for the 5-instrument panel with S14/S22/S23 dropped (bulk S10/S12 + companyfacts). High confidence (rollup reads observations = bulk tables) but unmeasured.
- **Row-count floors** — need the first clean run's baselines; P4 floors are calibrated from P6, not guessed.
- **N-CSR/mf_directory (S26/S27)** — confirm bulk coverage of fund-metadata for the panel before dropping/keeping.
- **Bulk-lag gap window** — measure how many post-cutoff accessions the master.idx recent-window adds; confirms the gap-close is bounded.
- **Migration 179 applied on the dev DB** before the drive (file present ≠ applied; `'deferred'` enum + `body_deferred` columns required for #1343 seeding).
- **legacy `institutional_holdings` endpoint** — decide whether to migrate `instruments.py:3457` to observations or keep the steady-state per-CIK sweep feeding it.

---

## 8. Definition of done (ETL clauses 8–12)

8. Smoke 3–5 instruments (AAPL/GME/MSFT/JPM/HD) in dev DB — recorded in each data-touching PR.
9. Cross-source verify ≥1 fixture vs an independent source.
10. Backfill executed (the clean bootstrap drive, P6) — not "queued for nightly".
11. Operator-visible figure verified on `/instruments/{symbol}/ownership-rollup` after backfill.
12. PR descriptions record the verification step + commit SHA for clauses 8–11.

Plus the redesign DoD: fresh `ebull` → bulk-only bootstrap in minutes → no OOM (4GB executor budget) → validation gates pass → panel renders + cross-source ok → timeline live with ETA/heartbeat → watermarks on data-event date → steady-state picks up only post-watermark.
