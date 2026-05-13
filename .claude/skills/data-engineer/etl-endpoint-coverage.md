# ETL endpoint coverage matrix

**Purpose.** Single answer to "are we covered?" for every official-source endpoint eBull consumes. Each row maps an endpoint to its five wiring layers — bootstrap stage, standard refresh, freshness index, watermark/retry, rate-limit pool — plus the manifest parser path where one applies.

**Read this before:** filing a "missing data" ticket; adding a new SEC fetcher; auditing why a steady-state poll isn't firing; deciding whether a `ManifestSource` is fully wired.

**Maintenance.** When a new endpoint is added, add a row and link the wiring layers to file:line. When a layer is wired or unwired, update the row and update [[us-source-coverage]] in memory.

Last audit pass: 2026-05-13 (post #1152 / #1154 / 10-K manifest adapter shipped).

---

## 1. Cross-cutting wiring layers

For any endpoint to be "covered" we need ALL of:

| # | Layer | Owner | Code anchor |
|---|---|---|---|
| 1 | **Bootstrap stage** — first-install drain | `_BOOTSTRAP_STAGE_SPECS` | `app/services/bootstrap_orchestrator.py:795-880` (24 stages) |
| 2 | **Standard refresh** — steady-state job | `SCHEDULED_JOBS` + `_INVOKERS` | `app/workers/scheduler.py:492` / `app/jobs/runtime.py:_INVOKERS` |
| 3 | **Freshness index** — per-(subject, source) cadence + last_known_*  | `_CADENCE` + `data_freshness_index` table | `app/services/data_freshness.py:69-102` + `sql/120` |
| 4 | **Watermark + retry** — `next_retry_at`, `last_known_filing_id` | `sec_filing_manifest` + `data_freshness_index` | `app/services/sec_manifest.py` + `sql/118`, `sql/120` |
| 5 | **Rate-limit pool** — shared per-IP budget | per-host clock + lock | `app/providers/implementations/sec_edgar.py:55-80` (SEC 10 req/s) |
| (6) | **Manifest parser** — typed-table materialisation | `manifest_parsers` registry | `app/services/manifest_parsers/__init__.py:register_all_parsers()` |

A "stranded" entry has rows in (4) but no resolver in (2) — manifest grows, never drains. A "dead-coded" entry has (1) + (6) but no (2) — bootstrap fills, steady-state never refreshes. See [§3](#3-discovery-layer-wiring) for the live example.

---

## 2. Per-source matrix — `ManifestSource` (14 enum values)

Definition: `app/services/sec_manifest.py:106-121` + CHECK constraint `sql/118:37-46`.

| Source | Bootstrap stage | Standard refresh | Freshness cadence | Watermark | Pool | Parser | Status |
|---|---|---|---|---|---|---|---|
| `sec_8k` | Stage 20 `sec_8k_events_ingest` | `JOB_SEC_8K_EVENTS_INGEST` cron (`scheduler.py:671`) + manifest worker | 14d | `data_freshness_index` + `sec_filing_manifest.next_retry_at` | `sec_rate` | ✅ `eight_k.py` (#1126) | **WIRED** |
| `sec_def14a` | Stage 16 `sec_def14a_bootstrap` | `JOB_SEC_DEF14A_INGEST` cron (`scheduler.py:742`) + manifest worker | 365d | both | `sec_rate` | ✅ `def14a.py` (#1128) | **WIRED** |
| `sec_13d` | Stage 14 `filings_history_seed` (730d) | manifest worker only | 90d | both | `sec_rate` | ✅ `sec_13dg.py` (#1129) | **WIRED**, but only manifest worker (no dedicated cron — depends on Layer 1/2/3, see §3) |
| `sec_13g` | Stage 14 | manifest worker only | 90d | both | `sec_rate` | ✅ `sec_13dg.py` (#1129) | **WIRED**, same caveat |
| `sec_form3` | Stage 19 `sec_form3_ingest` | `JOB_SEC_FORM3_INGEST` cron (`scheduler.py:722`) + manifest worker | 30d | both | `sec_rate` | ✅ `insider_345.py` (#1130) | **WIRED** |
| `sec_form4` | Stage 11 bulk + Stage 18 legacy backfill | `JOB_SEC_INSIDER_TRANSACTIONS_INGEST` cron (`scheduler.py:639`) + manifest worker | 30d | both | `sec_rate` | ✅ `insider_345.py` (#1130) | **WIRED** |
| `sec_form5` | Stage 18 legacy backfill | `JOB_SEC_INSIDER_TRANSACTIONS_INGEST` cron + manifest worker | 365d | both | `sec_rate` | ✅ `insider_345.py` `_parse_form5` (#1134) | **WIRED**, observation `source='form4'` (enum lacks form5; provenance via `insider_filings.document_type='5'` JOIN) |
| `sec_13f_hr` | Stage 10 bulk + Stage 21 recent-sweep | `JOB_SEC_13F_QUARTERLY_SWEEP` cron (`scheduler.py:926`) + manifest worker | 120d | both | `sec_rate` | ✅ `sec_13f_hr.py` (#1133) | **WIRED**, PRN drop + 2023-01-03 VALUE cutover applied parser-side |
| `sec_n_port` | Stage 12 bulk + Stage 22 legacy ingest | `JOB_SEC_N_PORT_INGEST` cron (`scheduler.py:1033`) + manifest worker | 90d | both | `sec_rate` | ✅ `sec_n_port.py` (#1133) | **WIRED** |
| `sec_10k` | Stage 17 `sec_business_summary_bootstrap` | `JOB_SEC_BUSINESS_SUMMARY_INGEST` cron (`scheduler.py:617`) + manifest worker | 120d | both | `sec_rate` | ✅ `sec_10k.py` (#1152, 2026-05-13) | **WIRED** — Option C `(filed_at, source_accession)` gate applied (sql/148) |
| `sec_10q` | — | — | 60d | manifest only | — | ❌ blocked on **#414** | **GAP** — 10-Q parser owned by fundamentals ingest redesign (#414); manifest rows drain to "no parser" |
| `sec_n_csr` | — | — | 200d | manifest only | — | ❌ pending re-spike **#918 REOPENED 2026-05-13** | **GAP** — original close cited only EdgarTools surface; operator wants sample-driven evidence on raw payloads + HTML SoI layout + commercial-use survey before "infeasible". Tech-debt #1153 on hold. |
| `sec_xbrl_facts` | Stage 9 `sec_companyfacts_ingest` (bulk-zip) + Stage 24 `fundamentals_sync` | `JOB_FUNDAMENTALS_SYNC` cron (`scheduler.py:562`) | 120d | manifest only (rows discovered but parser is bulk-path, not manifest dispatch) | `sec_rate` | ❌ by design — Company Facts API bulk path | **WIRED**, not a parser gap. Manifest rows may accumulate without drain; tracked tech-debt: either remove from enum or register synth no-op parser. |
| `finra_short_interest` | — | — | 20d | manifest only | — (FINRA host has no pool) | ❌ pending **#915** (bimonthly) + **#916** (RegSHO daily) | **GAP** — parent #845 closed but PR1/PR2 split open. ManifestSource enum entry has no fetcher anywhere. |

---

## 3. Discovery layer wiring — Layer 1 / Layer 2 / Layer 3

The #863-#873 ETL freshness redesign (spec at `docs/superpowers/specs/2026-05-04-etl-coverage-model.md`) ships three steady-state discovery layers, cheapest-first. They sit BETWEEN the bootstrap drain and the manifest worker — discovering new accessions and inserting `sec_filing_manifest` rows for the worker to drain.

| Layer | Endpoint | Code | Bootstrap-side caller | Steady-state caller | Status |
|---|---|---|---|---|---|
| 1 | Atom `getcurrent?action=getcurrent&output=atom` (every 5 min) | `run_atom_fast_lane` at `app/jobs/sec_atom_fast_lane.py:104` | — | — | ❌ **UNWIRED** — no `_INVOKERS[]` entry, no `SCHEDULED_JOBS` row, only test callers |
| 2 | Daily `master.YYYYMMDD.idx` (04:00 UTC reconciliation) | `run_daily_index_reconcile` at `app/jobs/sec_daily_index_reconcile.py:46` | — | — | ❌ **UNWIRED** — same shape |
| 3 | Per-CIK `submissions/CIK*.json` (per `data_freshness._CADENCE`) | `run_per_cik_poll` at `app/jobs/sec_per_cik_poll.py:39` | — | — | ❌ **UNWIRED** — same shape |

Tickets #867 / #868 / #870 marked CLOSED 2026-05-06 by the implementation PRs, but the wiring layer was never added. Reopened with audit pointer 2026-05-13. **Umbrella: #1155.**

### Impact

Steady-state filings discovery currently runs through the legacy per-form ingest crons that the redesign was meant to retire:

| Cron | File:line | What it does |
|---|---|---|
| `sec_insider_transactions_ingest` | `scheduler.py:639` | Blanket scan 500 filings/hour |
| `sec_form3_ingest` | `scheduler.py:722` | Blanket scan |
| `sec_def14a_ingest` | `scheduler.py:742` | Blanket scan |
| `sec_8k_events_ingest` | `scheduler.py:671` | Blanket scan |
| `sec_business_summary_ingest` | `scheduler.py:617` | 200 instruments/day |
| `sec_dividend_calendar_ingest` | `scheduler.py:601` | 500 filings/day |
| `sec_n_port_ingest` | `scheduler.py:1033` | Blanket scan |
| `sec_13f_quarterly_sweep` | `scheduler.py:926` | Weekly sweep |

`data_freshness._CADENCE` at `app/services/data_freshness.py:69` is queried by the per-CIK seeder (`seed_freshness_for_manifest_row`) on every manifest write, populating `expected_next_at`. `subjects_due_for_poll` at `app/services/data_freshness.py:485` is the consumer reader. `run_per_cik_poll` calls it correctly. But because Layer 3 is unwired (no `_INVOKERS[]` / `SCHEDULED_JOBS` row), no scheduled caller reaches that path. The table is read by tests + ad-hoc rebuild scripts only, not steady-state polling.

**Sub-gap G13:** even after Layer 3 is wired, `run_per_cik_poll` reads only `subjects_due_for_poll`. The companion `subjects_due_for_recheck` at `app/services/data_freshness.py:533` (handles `never_filed` + `error` state rechecks) is referenced only by tests — production never reaches it. `#1155` acceptance should require both reader paths to fire.

Once #1155 lands, the legacy crons above can be retired per spec §6. Each retirement is one PR + smoke per cron.

---

## 4. Reference + bulk-archive endpoint matrix (non-manifest)

These endpoints don't have a `ManifestSource` because they're not per-filing dispatched — they seed identifiers, populate bulk reference tables, or download tarballs.

| Endpoint | Code | Bootstrap stage | Steady-state | Pool | Notes |
|---|---|---|---|---|---|
| `www.sec.gov/files/company_tickers.json` | `app/providers/implementations/sec_edgar.py:52` | Stage 6 `cik_refresh` (`JOB_DAILY_CIK_REFRESH`) | `JOB_FUNDAMENTALS_SYNC` daily (`scheduler.py:562`) calls `daily_cik_refresh()` inline at `scheduler.py:3051` | `sec_rate` | Ticker → CIK bridge, ~10k operating-co rows; conditional GET ETag-aware. **Note:** `JOB_DAILY_CIK_REFRESH` is in `_INVOKERS` (manual trigger only) but absent from `SCHEDULED_JOBS` — the daily cadence lives inside the fundamentals_sync body, not as a standalone scheduled job. |
| `www.sec.gov/files/company_tickers_exchange.json` | NOT CONSUMED | — | — | — | ❌ **GAP** — sec-edgar skill §1 cites this as reference bridge; coverage closes pink-sheet/OTC/foreign-without-ADR gap left by `company_tickers.json`. No code consumer. Eligible for tech-debt ticket. |
| `www.sec.gov/files/company_tickers_mf.json` | NOT CONSUMED | — | — | — | ❌ **GAP** — same shape, ~28k mutual-fund rows with `seriesId` + `classId`. No consumer. Tech-debt eligible. |
| `www.sec.gov/files/investment/13flist{year}q{quarter}.txt` | `app/services/sec_13f_securities_list.py:77` | Stage 3 `cusip_universe_backfill` | `JOB_CUSIP_UNIVERSE_BACKFILL` (`scheduler.py:925`) | `sec_rate` | 13F Official List, ~24k rows; CUSIP → issuer-name authoritative bridge |
| `data.sec.gov/submissions/CIK*.json` | `app/providers/implementations/sec_submissions.py:239` | Stage 8 `sec_submissions_ingest` (bulk-zip) + Stage 13 `sec_submissions_files_walk` | manifest worker (when Layer 3 wired) + `JOB_SEC_INSIDER_TRANSACTIONS_INGEST` watermark walk | `sec_rate` | Per-CIK 1000-most-recent + overflow pages via `filings.files[]` |
| `data.sec.gov/submissions/CIK*-submissions-NNN.json` | `app/services/sec_submissions_files_walk.py` + `app/jobs/sec_rebuild.py:335` | Stage 13 | manual rebuild via `POST /jobs/sec_rebuild/run` | `sec_rate` | Overflow paging for deep-history parity |
| `data.sec.gov/api/xbrl/companyfacts/CIK*.json` | `app/providers/implementations/sec_fundamentals.py:594` (path built; `_BASE_URL` at :57) | Stage 9 `sec_companyfacts_ingest` (bulk-zip) | `JOB_FUNDAMENTALS_SYNC` (per-CIK API path) | `sec_rate` | All XBRL concepts |
| `data.sec.gov/api/xbrl/companyconcept/CIK*/{taxonomy}/{tag}.json` | NOT CONSUMED | — | — | — | ❌ **GAP** — single-tag smaller payload; would let `fundamentals_sync` avoid full Companyfacts when only N tags needed. Tech-debt eligible. |
| `data.sec.gov/api/xbrl/frames/...` | NOT CONSUMED | — | — | — | ❌ **GAP** — cross-sectional one-fact-per-filer; useful for sector aggregates. Not currently in the v1 metrics surface. Tech-debt eligible. |
| Bulk `submissions.zip` (~1.54 GB) | `app/services/sec_bulk_download.py:225-227` | Stage 7 `sec_bulk_download` | — (one-shot per bootstrap) | `sec_bulk_download` lane | Initial-install drain only |
| Bulk `companyfacts.zip` (~1.38 GB) | `app/services/sec_bulk_download.py:229-231` | Stage 7 | — | `sec_bulk_download` lane | Initial-install drain |
| Bulk `form-13f-data-sets/{q}_form13f.zip` | `app/services/sec_bulk_download.py:237` | Stage 7 + Stage 10 | — (quarterly via 13F sweep cron walks per-filing) | `sec_bulk_download` lane | Bulk dataset |
| Bulk `insider-transactions-data-sets/{q}_form345.zip` | `app/services/sec_bulk_download.py:244` | Stage 7 + Stage 11 | — | `sec_bulk_download` lane | Bulk dataset |
| Bulk `form-n-port-data-sets/{q}_nport.zip` | `app/services/sec_bulk_download.py:251` | Stage 7 + Stage 12 | — | `sec_bulk_download` lane | Bulk dataset |
| Daily `master.YYYYMMDD.idx` | `app/providers/implementations/sec_edgar.py:565` | — (Layer 2, see §3) | ❌ unscheduled (Layer 2 gap #868) | `sec_rate` | Yesterday's filings reconciliation |
| Full-index `master.idx` quarterly | NOT CONSUMED | — | — | — | ❌ **GAP** — cross-quarter discovery; sec-edgar skill §1 cites it but only `form.idx` is consumed (top-filer discovery at `top_filer_discovery.py:64`). Eligible for tech-debt if cross-quarter walks become needed. |
| Atom `getcurrent` | `app/providers/implementations/sec_getcurrent.py:50` | — | ❌ unscheduled (Layer 1 gap #867) | `sec_rate` | Live current-day filings; ISO-8859-1 |
| Atom `getcompany?CIK={cik}&type={form}` | NOT CONSUMED | — | — | — | ❌ **GAP** — per-CIK Atom alternative. Not consumed; per-CIK Atom is via Layer 1 (universe-wide Atom + filter). Likely fine — submissions.json is authoritative. No ticket needed unless operator wants per-CIK polling. |
| Filing-folder `/Archives/edgar/data/{cik}/{acc}/index.json` | `app/providers/implementations/sec_edgar.py:424` + `app/services/filing_documents.py` | — | `JOB_SEC_FILING_DOCUMENTS_INGEST` (`scheduler.py:655`) | `sec_rate` | Enumerate filing exhibits |
| Full-index `form.idx` quarterly | `app/services/top_filer_discovery.py:64` | — | manual / quarterly top-filer rebuild | `sec_rate` | 13F filer-directory bootstrap |
| eToro REST | `app/providers/implementations/etoro_broker.py` | Stage 2 `candle_refresh` | `JOB_DAILY_CANDLE_REFRESH` + orchestrator high-frequency-sync | `etoro` lane (separate from SEC) | Out of scope for SEC audit; covered by execution-track skill |
| FRED / BLS macro feeds | NOT CONSUMED | — | — | — | Not currently in scope. Settled-decisions §"Fundamentals provider posture" — free regulated only; no macro feed wired yet. |

---

## 5. Bootstrap stage table — for reference

`_BOOTSTRAP_STAGE_SPECS` at `app/services/bootstrap_orchestrator.py:795-880`. 24 stages, 5 rate lanes (`init`, `etoro`, `sec_rate`, `sec_bulk_download`, `db`).

| # | Stage | Lane | Job | Endpoints it seeds |
|---|---|---|---|---|
| 1 | `universe_sync` | init | `nightly_universe_sync` | eToro instruments universe |
| 2 | `candle_refresh` | etoro | `daily_candle_refresh` | eToro candles |
| 3 | `cusip_universe_backfill` | sec_rate | `cusip_universe_backfill` | 13F Official List |
| 4 | `sec_13f_filer_directory_sync` | sec_rate | `sec_13f_filer_directory_sync` | 13F filer directory walk |
| 5 | `sec_nport_filer_directory_sync` | sec_rate | `sec_nport_filer_directory_sync` | N-PORT filer directory |
| 6 | `cik_refresh` | sec_rate | `daily_cik_refresh` | `company_tickers.json` |
| 7 | `sec_bulk_download` | sec_bulk_download | `sec_bulk_download` | submissions.zip, companyfacts.zip, 13F/insider/NPORT bulk zips |
| 8 | `sec_submissions_ingest` | db | `sec_submissions_ingest` | submissions.zip ingest |
| 9 | `sec_companyfacts_ingest` | db | `sec_companyfacts_ingest` | companyfacts.zip ingest |
| 10 | `sec_13f_ingest_from_dataset` | db | `sec_13f_ingest_from_dataset` | 13F bulk ingest |
| 11 | `sec_insider_ingest_from_dataset` | db | `sec_insider_ingest_from_dataset` | Form 3/4/5 bulk ingest |
| 12 | `sec_nport_ingest_from_dataset` | db | `sec_nport_ingest_from_dataset` | N-PORT bulk ingest |
| 13 | `sec_submissions_files_walk` | sec_rate | `sec_submissions_files_walk` | Per-CIK overflow `filings.files[]` walk |
| 14 | `filings_history_seed` | sec_rate | `filings_history_seed` | 730-day history (any form in `_FILINGS_HISTORY_KEEP_FORMS_TUPLE`) |
| 15 | `sec_first_install_drain` | sec_rate | `sec_first_install_drain` | Per-CIK submissions.json drain |
| 16 | `sec_def14a_bootstrap` | sec_rate | `sec_def14a_bootstrap` | DEF 14A backfill |
| 17 | `sec_business_summary_bootstrap` | sec_rate | `sec_business_summary_bootstrap` | 10-K Item 1 backfill |
| 18 | `sec_insider_transactions_backfill` | sec_rate | `sec_insider_transactions_backfill` | Form 4 backfill |
| 19 | `sec_form3_ingest` | sec_rate | `sec_form3_ingest` | Form 3 |
| 20 | `sec_8k_events_ingest` | sec_rate | `sec_8k_events_ingest` | 8-K |
| 21 | `sec_13f_recent_sweep` | sec_rate | `sec_13f_quarterly_sweep` (`min_period_of_report=today-380d`) | 13F recent sweep |
| 22 | `sec_n_port_ingest` | sec_rate | `sec_n_port_ingest` | N-PORT |
| 23 | `ownership_observations_backfill` | db | `ownership_observations_backfill` | Recompute `_current` from observations |
| 24 | `fundamentals_sync` | db | `fundamentals_sync` | Per-CIK Companyfacts API top-up |

---

## 6. Rate-limit pool inventory

| Pool | Budget | Code | Consumers |
|---|---|---|---|
| `sec_rate` | 10 req/s shared per-IP (SEC fair-use ceiling) | `app/providers/implementations/sec_edgar.py:55-80` — `_MIN_REQUEST_INTERVAL_S = 0.11` + `_PROCESS_RATE_LIMIT_CLOCK` + `_PROCESS_RATE_LIMIT_LOCK` | Every `data.sec.gov` + `www.sec.gov` consumer via `ResilientClient`; `SecFilingsProvider`, `SecFundamentalsProvider`, all manifest parsers' `requires_raw_payload=True` fetchers |
| `sec_bulk_download` | Bandwidth-probe-bounded (slow-connection bypass switches to legacy path) | `app/services/sec_bulk_download.py:863` | Stage 7 bulk-zip download only |
| `etoro` | eToro's per-account REST quota (broker-side enforced) | `app/providers/implementations/etoro_broker.py:124,129` `ResilientClient` | Quotes, candles, orders, positions |
| `db` | Postgres pool, `max_concurrency=5` for Phase C ingesters | `app/db/pool.py::open_pool` + stage spec `max_concurrency` | Stages 8-12 DB ingesters; ownership_observations_backfill; fundamentals_sync |
| FINRA / FRED / BLS | none — no fetchers exist yet | — | — — file when #915 / #916 land. Until then no pool needed. |

---

## 7. Gap register — open / pending / by-design

| ID | Source | Status | Ticket | Resolution |
|---|---|---|---|---|
| G1 | Layer 1 Atom fast-lane unwired | OPEN | **#867 REOPENED 2026-05-13** | Wire under #1155 |
| G2 | Layer 2 daily-index reconcile unwired | OPEN | **#868 REOPENED 2026-05-13** | Wire under #1155 |
| G3 | Layer 3 per-CIK poll unwired | OPEN | **#870 REOPENED 2026-05-13** | Wire under #1155 |
| G4 | `sec_10q` parser | BLOCKED | **#414** | Owned by fundamentals ingest redesign |
| G5 | `sec_n_csr` parser feasibility | PENDING SPIKE | **#918 REOPENED 2026-05-13** | Sample-driven spike pending |
| G6 | `finra_short_interest` ingest | OPEN | **#915 + #916** | Bimonthly + RegSHO daily; parent #845 closed |
| G7 | `sec_xbrl_facts` ManifestSource has no parser | BY DESIGN | — | Company Facts API bulk path; tech-debt: either remove from enum or register synth no-op parser |
| G8 | `company_tickers_exchange.json` not consumed | OPEN (low) | — | Closes pink-sheet/OTC/foreign-without-ADR gap in CIK bridge. Eligible. |
| G9 | `company_tickers_mf.json` not consumed | OPEN (low) | — | ~28k mutual-fund rows with `seriesId` + `classId`. Eligible. |
| G10 | `companyconcept` API not consumed | OPEN (low) | — | Smaller-payload alternative to Companyfacts for known-tag pulls. Eligible. |
| G11 | `frames` API not consumed | OPEN (low) | — | Cross-sectional one-fact-per-filer; sector aggregates use case. Eligible. |
| G12 | Full-index `master.idx` quarterly not consumed | OPEN (low) | — | Cross-quarter discovery; only `form.idx` is consumed today. Eligible if cross-quarter walks become needed. |
| G13 | `subjects_due_for_recheck` reader unused | OPEN | **#1155** (sub-finding) | `app/services/data_freshness.py:533` — handles `never_filed` + `error` state rechecks. Only tests reference it; runtime Layer 3 (when wired) reads only `subjects_due_for_poll` at `:485`. #1155 acceptance must require BOTH reader paths to fire. |

G1-G3 are the **headline finding** of this audit — the freshness redesign's three steady-state polling layers are coded but never scheduled. Without them, the table at §3 (legacy per-form ingest crons) carries discovery; `data_freshness._CADENCE` is a write-only ledger.

G8-G12 are low-priority — they don't block any operator-visible figure today. Open as tech-debt only if a metric ticket would benefit.

---

## 8. How to read this matrix

- "Are we covered for source X?" → §2 row for X. ✅ in last column = yes. ❌ = gap, follow ticket.
- "Where does endpoint URL Y come from?" → §4 row. File:line cites the consumer.
- "Why isn't endpoint Y firing on cadence?" → cross-check (a) §2/§4 has a `JOB_*` listed under steady-state; (b) `app/jobs/runtime.py` has `_INVOKERS[JOB_*]` registered; (c) `SCHEDULED_JOBS` has a `ScheduledJob(name=JOB_*, ...)`. All three must hold. The #1155 finding is exactly the case where (b) and (c) are missing.
- "What's the rate-limit pool for fetcher Z?" → §6.
- "Adding a new source — what wiring do I need?" → §1 lists the five layers. New ManifestSource also needs §2 row + parser registration in `manifest_parsers/__init__.py`.

---

## 9. Cross-references

- `.claude/skills/data-sources/sec-edgar.md` — endpoint inventory authoritative source. §11.5 stranded-source map cross-checked.
- `.claude/skills/data-sources/edgartools.md` — library coverage matrix.
- `.claude/skills/data-engineer/SKILL.md` — schema invariants + per-source retention.
- Spec: `docs/superpowers/specs/2026-05-04-etl-coverage-model.md` — original #863-#873 redesign.
- Memory: [[us-source-coverage]], [[873-manifest-worker-parser-rollout]], [[etl-freshness-redesign]].
