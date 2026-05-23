# ETL endpoint coverage matrix

**Purpose.** Single answer to "are we covered?" for every official-source endpoint eBull consumes. Each row maps an endpoint to its five wiring layers — bootstrap stage, standard refresh, freshness index, watermark/retry, rate-limit pool — plus the manifest parser path where one applies.

**Read this before:** filing a "missing data" ticket; adding a new SEC fetcher; auditing why a steady-state poll isn't firing; deciding whether a `ManifestSource` is fully wired.

**Maintenance.** When a new endpoint is added, add a row and link the wiring layers to file:line. When a layer is wired or unwired, update the row and update [[us-source-coverage]] in memory.

Last audit pass: 2026-05-14 (post #1168 / sec_10q synth no-op parser shipped — closes G4).

---

## 1. Cross-cutting wiring layers

For any endpoint to be "covered" we need ALL of:

| # | Layer | Owner | Code anchor |
|---|---|---|---|
| 1 | **Bootstrap stage** — first-install drain | `_BOOTSTRAP_STAGE_SPECS` | `app/services/bootstrap_orchestrator.py:795-900` (26 stages; #1174 added S25 `mf_directory_sync` + S26 `sec_n_csr_bootstrap_drain`) |
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
| `sec_8k` | Stage 20 `sec_8k_events_ingest` | manifest worker (post-#1155 — `JOB_SEC_8K_EVENTS_INGEST` moved to on-demand; bootstrap stage 20 still dispatches via `_INVOKERS`) | 14d | `data_freshness_index` + `sec_filing_manifest.next_retry_at` | `sec_rate` | ✅ `eight_k.py` (#1126) | **WIRED** |
| `sec_def14a` | Stage 16 `sec_def14a_bootstrap` | manifest worker (post-#1155 — `JOB_SEC_DEF14A_INGEST` moved to on-demand) + weekly `sec_def14a_bootstrap` safety net | 365d | both | `sec_rate` | ✅ `def14a.py` (#1128) | **WIRED** |
| `sec_13d` | Stage 14 `filings_history_seed` (730d) | manifest worker only | 90d | both | `sec_rate` | ✅ `sec_13dg.py` (#1129) | **WIRED**, but only manifest worker (no dedicated cron — depends on Layer 1/2/3, see §3) |
| `sec_13g` | Stage 14 | manifest worker only | 90d | both | `sec_rate` | ✅ `sec_13dg.py` (#1129) | **WIRED**, same caveat |
| `sec_form3` | Stage 19 `sec_form3_ingest` | manifest worker (post-#1155 — `JOB_SEC_FORM3_INGEST` moved to on-demand; bootstrap stage 19 still dispatches via `_INVOKERS`) | 30d | both | `sec_rate` | ✅ `insider_345.py` (#1130) | **WIRED** |
| `sec_form4` | Stage 11 bulk + Stage 18 legacy backfill | manifest worker (post-#1155 — `JOB_SEC_INSIDER_TRANSACTIONS_INGEST` moved to on-demand) + round-robin `sec_insider_transactions_backfill` deep-tail drain | 30d | both | `sec_rate` | ✅ `insider_345.py` (#1130) | **WIRED** |
| `sec_form5` | Stage 18 legacy backfill | `JOB_SEC_INSIDER_TRANSACTIONS_INGEST` cron + manifest worker | 365d | both | `sec_rate` | ✅ `insider_345.py` `_parse_form5` (#1134) | **WIRED**, observation `source='form4'` (enum lacks form5; provenance via `insider_filings.document_type='5'` JOIN) |
| `sec_13f_hr` | Stage 10 bulk + Stage 21 recent-sweep | manifest worker (post-#1155 — `JOB_SEC_13F_QUARTERLY_SWEEP` moved to on-demand; bootstrap stage 21 still dispatches it via `_INVOKERS` with `min_period_of_report` from `MANUAL_TRIGGER_JOB_METADATA`) | 120d | both | `sec_rate` | ✅ `sec_13f_hr.py` (#1133) — EdgarTools wrapper via `sec_13f.py` (#931, 2026-05-05) | **WIRED**, PRN drop at `sec_13f_hr.py:415-417`; 2023-01-03 VALUE cutover applied service-side at `sec_13f_hr.py:103, 397, 419-421` (manifest adapter); the parser at `sec_13f.py` is a pure EdgarTools wrapper that preserves raw values per the #931 contract. Spike `docs/_archive/2026-05/spike-13f-hr-edgartools.md` certifies the EdgarTools drop-in is complete + REBUTS the plan §2 PR 9 alternative scope (Filing.obj() / ThirteenF manifest-adapter restructure) on five binding grounds — rate-limit-pool bypass, cutover-semantics divergence, no native PRN drop, no raw-payload persistence hook, no transient-vs-deterministic error classification. |
| `sec_n_port` | Stage 12 bulk + Stage 22 legacy ingest | manifest worker (post-#1155 — `JOB_SEC_N_PORT_INGEST` moved to on-demand; bootstrap stage 22 still dispatches via `_INVOKERS`) | 90d | both | `sec_rate` | ✅ `sec_n_port.py` (#1133) | **WIRED** |
| `sec_10k` | Stage 17 `sec_business_summary_bootstrap` | manifest worker (post-#1155 retirement of legacy `sec_business_summary_ingest` cron) + weekly `sec_business_summary_bootstrap` safety net | 120d | both | `sec_rate` | ✅ `sec_10k.py` (#1152, 2026-05-13) | **WIRED** — Option C `(filed_at, source_accession)` gate applied (sql/148). Legacy daily 03:15 cron retired in the first #1155 cron-retirement sweep — manifest path is sole steady-state writer. |
| `sec_10q` | — | manifest worker (synth no-op per #1168) | 60d | both | `sec_rate` (unused by parser) | ✅ `sec_10q.py` (#1168) | **WIRED** — synth no-op (sec-edgar §11.5.1). Financial data lands via Companyfacts XBRL; narrative HTML has no v1 consumer. Parser body is `return ParseOutcome(status='parsed', parser_version='10q-noop-v1')`. Closes G4. |
| `sec_n_csr` | S25 `mf_directory_sync` (classId resolver inputs) + S26 `sec_n_csr_bootstrap_drain` (last-2y per-trust manifest enqueue) | manifest worker (real fund-metadata parser, #1171) | 200d | both | `sec_rate` | ✅ `sec_n_csr.py` (#1171, 2026-05-15, replaced #918 / PR #1170 synth no-op) | **WIRED** — real fund-metadata parser + dedicated bootstrap drain landed in #1174. Extracts per-(series, class) Tier 1 scalars (expense ratio, NAV, returns, portfolio turnover, holdings count, inception, contact) + Tier 2 dimensional JSONB (sector / region / credit allocation, returns_pct, benchmark_returns_pct, growth_curve) + Tier 3 `raw_facts` fallback. `requires_raw_payload=False` (no `store_raw`; re-parse re-fetches iXBRL companion). Source-priority chain: `period_end DESC, filed_at DESC, source_accession DESC` (settled-decisions §"Source priority for fund metadata"). classId → instrument_id via `external_identifiers (provider='sec', identifier_type='class_id')` populated by the dedicated S25 `mf_directory_sync` bootstrap stage (#1174; daily cron `daily_cik_refresh` retains the bundled call as a drift-heal safety net). S26 `sec_n_csr_bootstrap_drain` walks distinct trust CIKs from `cik_refresh_mf_directory` and enqueues last-2-years N-CSR + N-CSRS accessions with `subject_type='institutional_filer' + instrument_id=NULL` for the manifest worker to drain. Spike `docs/_archive/2026-05/spike-n-csr-feasibility.md` INFEASIBLE-CONFIRMED verdict applied to holdings-attestation lane only (§10.6 scope narrowing); fund-metadata lane is orthogonal. |
| `sec_xbrl_facts` | Stage 9 `sec_companyfacts_ingest` (bulk-zip) + Stage 24 `fundamentals_sync` | `JOB_FUNDAMENTALS_SYNC` cron (`scheduler.py:562`) + manifest worker (synth no-op, G7) | 120d | both | `sec_rate` (unused by parser) | ✅ `sec_xbrl_facts.py` (G7) | **WIRED** — synth no-op (sec-edgar §11.5.1). XBRL facts land via the Companyfacts bulk JSON path; the manifest row IS the audit signal. Parser body is `return ParseOutcome(status='parsed', parser_version='xbrl-facts-noop-v1')`. Closes G7. |
| `finra_short_interest` | ScheduledJob `finra_short_interest_refresh` (daily 12:00 UTC) — discovery + fetch + write inline | manifest worker (synth no-op, G6/#915) | 20d | both | `finra` (1 req/s; new lane disjoint from sec_rate, sql/151+152 + sources.py) | ✅ `finra_short_interest.py` (G6/#915 bimonthly portion) | **WIRED 2026-05-18 (#915)** — ScheduledJob owns fetch + parse + UPSERT into `finra_short_interest_observations` + `finra_short_interest_current`; manifest parser is synth no-op (sec_xbrl_facts G7 precedent). Symbol resolution via `_normalise_symbol` strip-non-alnum+upper (BRK.A ↔ BRKA). Revision-window: two most-recent settlement dates always re-fetched regardless of manifest status (catches `revisionFlag='Y'` in-place corrections). New skill `.claude/skills/data-sources/finra.md` documents endpoint shape + cohort cliff (pre-June 2021 OTC-only). |
| `finra_regsho_daily` | ScheduledJob `finra_regsho_daily_refresh` (daily 23:00 UTC) — discovery × 6 prefixes + fetch + write inline | manifest worker (synth no-op, G6/#916) | 2d | both | `finra` (1 req/s; shares module-global throttle with bimonthly sibling, sql/153+154 + sources.py) | ✅ `finra_regsho_daily.py` (G6/#916 daily portion) | **WIRED 2026-05-18 (#916)** — ScheduledJob owns fetch + parse + UPSERT into `finra_regsho_daily_observations` (25 quarterly partitions 2024-Q1 → 2030-Q1; no `_current` — daily file IS the snapshot). 6 prefixes per trade-date (CNMS aggregate + FNQC/FNRA/FNSQ/FNYX/FORF facilities). Decimal volumes (`NUMERIC(18, 6)` — NOT integer); `Market` is `TEXT` (comma-joined `B,Q,N` on CNMS). Body-Date validation per row + footer-row-count validation per file. Revision-window: last-2 trade dates × 6 prefixes always re-fetched. Sibling provider imports the bimonthly module's throttle globals so combined 1 req/s budget is preserved. |

---

## 3. Discovery layer wiring — Layer 1 / Layer 2 / Layer 3 / Layer 4

The #863-#873 ETL freshness redesign (spec at `docs/specs/etl/coverage-model.md`) ships three steady-state discovery layers, cheapest-first. They sit BETWEEN the bootstrap drain and the manifest worker — discovering new accessions and inserting `sec_filing_manifest` rows for the worker to drain. **Layer 4** (G12, 2026-05-17) extends the family for cross-quarter recovery — the case Layer 1/2/3 cannot cover (tombstoned-CIK / deactivated-CIK / merged-CIK / late-amendment).

| Layer | Endpoint | Code | Bootstrap-side caller | Steady-state caller | Status |
|---|---|---|---|---|---|
| 1 | Atom `getcurrent?action=getcurrent&output=atom` (every 5 min) | `run_atom_fast_lane` at `app/jobs/sec_atom_fast_lane.py:104` | — | — | ❌ **UNWIRED** — no `_INVOKERS[]` entry, no `SCHEDULED_JOBS` row, only test callers |
| 2 | Daily `master.YYYYMMDD.idx` (04:00 UTC reconciliation) | `run_daily_index_reconcile` at `app/jobs/sec_daily_index_reconcile.py:46` | — | — | ❌ **UNWIRED** — same shape |
| 3 | Per-CIK `submissions/CIK*.json` (per `data_freshness._CADENCE`) | `run_per_cik_poll` at `app/jobs/sec_per_cik_poll.py:39` | — | — | ❌ **UNWIRED** — same shape |
| 4 | Full-index quarterly `master.idx` (weekly Sun 05:15 UTC; walks `[CQ, CQ-1]`) | `run_master_idx_quarterly_sweep` at `app/jobs/sec_master_idx_quarterly_sweep.py` | — | `sec_master_idx_quarterly_sweep` ScheduledJob (G12, 2026-05-17) | ✅ **WIRED 2026-05-17** — cross-quarter discovery safety net. Per-quarter txn isolation; strict-by-default 404; preloaded O(1) universe resolver. |

Tickets #867 / #868 / #870 marked CLOSED 2026-05-06 by the implementation PRs, but the wiring layer was never added. Reopened with audit pointer 2026-05-13. **Umbrella: #1155.**

### Impact

Steady-state filings discovery currently runs through the legacy per-form ingest crons that the redesign was meant to retire:

| Cron | File:line | What it does |
|---|---|---|
| `sec_insider_transactions_ingest` | `scheduler.py:639` | Blanket scan 500 filings/hour |
| `sec_form3_ingest` | `scheduler.py:722` | Blanket scan |
| `sec_def14a_ingest` | `scheduler.py:742` | Blanket scan |
| `sec_8k_events_ingest` | `scheduler.py:671` | Blanket scan |
| ~~`sec_business_summary_ingest`~~ | — | **retired post-#1155 — first legacy cron retired; manifest worker now sole steady-state writer for 10-K Item 1** |
| ~~`sec_dividend_calendar_ingest`~~ | — | **retired post-#1155 (#1166) — manifest worker + `eight_k.py` (#1158) sole steady-state writer; 8th and final retirement in the sweep** |
| `sec_n_port_ingest` | `scheduler.py:1033` | Blanket scan |
| `sec_13f_quarterly_sweep` | `scheduler.py:926` | Weekly sweep |

`data_freshness._CADENCE` at `app/services/data_freshness.py:69` is queried by the per-CIK seeder (`seed_freshness_for_manifest_row`) on every manifest write, populating `expected_next_at`. `subjects_due_for_poll` at `app/services/data_freshness.py:485` is the consumer reader. `run_per_cik_poll` calls it correctly. But because Layer 3 is unwired (no `_INVOKERS[]` / `SCHEDULED_JOBS` row), no scheduled caller reaches that path. The table is read by tests + ad-hoc rebuild scripts only, not steady-state polling.

**Sub-gap G13:** ✅ CLOSED 2026-05-17 — `run_per_cik_poll` drains BOTH `subjects_due_for_poll` AND `subjects_due_for_recheck` per tick (`app/jobs/sec_per_cik_poll.py:195-198`) with a bounded 2/3 + ~1/3 budget split (`max_subjects=100` → poll=66, recheck=34). Integration coverage at `tests/test_sec_per_cik_poll.py::TestG13RecheckPath` proves the never_filed-stays-in-queue contract + alongside-poll drain; static AST invariants at `tests/test_g13_recheck_reader_invariants.py` guarantee a future refactor cannot silently drop one reader path. Hourly cadence asserted at `tests/test_layer_123_wiring.py::test_layer3_per_cik_poll_registered`.

Once #1155 lands, the legacy crons above can be retired per spec §6. Each retirement is one PR + smoke per cron.

---

## 4. Reference + bulk-archive endpoint matrix (non-manifest)

These endpoints don't have a `ManifestSource` because they're not per-filing dispatched — they seed identifiers, populate bulk reference tables, or download tarballs.

| Endpoint | Code | Bootstrap stage | Steady-state | Pool | Notes |
|---|---|---|---|---|---|
| `www.sec.gov/files/company_tickers.json` | `app/providers/implementations/sec_edgar.py:52` | Stage 6 `cik_refresh` (`JOB_DAILY_CIK_REFRESH`) | `JOB_FUNDAMENTALS_SYNC` daily (`scheduler.py:562`) calls `daily_cik_refresh()` inline at `scheduler.py:3051` | `sec_rate` | Ticker → CIK bridge, ~10k operating-co rows; conditional GET ETag-aware. **Note:** `JOB_DAILY_CIK_REFRESH` is in `_INVOKERS` (manual trigger only) but absent from `SCHEDULED_JOBS` — the daily cadence lives inside the fundamentals_sync body, not as a standalone scheduled job. |
| `www.sec.gov/files/company_tickers_exchange.json` | `app/services/exchange_directory.py:refresh_exchange_directory` | Stage 6 `cik_refresh` Stage 7 sibling enrichment (`scheduler.py:daily_cik_refresh`) | Same — bundled into daily `daily_cik_refresh` | `sec_rate` | ✅ **WIRED 2026-05-17 (G8)** — `cik_refresh_exchange_directory` snapshot table, ticker-grain PK `(cik, ticker)`. Empirical 2026-05-17 payload: 10,353 rows / 7,996 unique CIKs / 1,446 multi-ticker CIKs (BAC=17 variants, JPM=9). Same CIK cohort COUNT as `company_tickers.json` but ticker-grain not CIK-grain — captures share-class siblings, preferred-series, ADR+OTC variants. Observed-ever semantics; `last_seen` is the watermark. No v1 consumer; consumers land via separate tickets. |
| `www.sec.gov/files/company_tickers_mf.json` | `app/services/mf_directory.py:refresh_mf_directory` | Stage 6 `cik_refresh` Stage 6 sibling enrichment (`scheduler.py:daily_cik_refresh`) + S25 `mf_directory_sync` (#1174) dedicated bootstrap stage | Same — bundled into daily `daily_cik_refresh` | `sec_rate` | ✅ **WIRED (#1171 + #1174)** — `cik_refresh_mf_directory` snapshot keyed by `classId` + `external_identifiers (sec, class_id)` write-through for in-universe symbols. ~28k mutual-fund rows with `seriesId` + `classId` per row. Consumed by `_fund_class_resolver.classify_resolver_miss` for N-CSR fund-metadata path. G9 in §7 closed by this entry; see also G8 stale-matrix correction in same PR. |
| `www.sec.gov/files/investment/13flist{year}q{quarter}.txt` | `app/services/sec_13f_securities_list.py:77` | Stage 3 `cusip_universe_backfill` | `JOB_CUSIP_UNIVERSE_BACKFILL` (`scheduler.py:925`) | `sec_rate` | 13F Official List, ~24k rows; CUSIP → issuer-name authoritative bridge |
| `data.sec.gov/submissions/CIK*.json` | `app/providers/implementations/sec_submissions.py:239` | Stage 8 `sec_submissions_ingest` (bulk-zip) + Stage 13 `sec_submissions_files_walk` | manifest worker (when Layer 3 wired) + `JOB_SEC_INSIDER_TRANSACTIONS_INGEST` watermark walk | `sec_rate` | Per-CIK 1000-most-recent + overflow pages via `filings.files[]` |
| `data.sec.gov/submissions/CIK*-submissions-NNN.json` | `app/services/sec_submissions_files_walk.py` + `app/jobs/sec_rebuild.py:335` | Stage 13 | manual rebuild via `POST /jobs/sec_rebuild/run` | `sec_rate` | Overflow paging for deep-history parity |
| `data.sec.gov/api/xbrl/companyfacts/CIK*.json` | `app/providers/implementations/sec_fundamentals.py:594` (path built; `_BASE_URL` at :57) | Stage 9 `sec_companyfacts_ingest` (bulk-zip) | `JOB_FUNDAMENTALS_SYNC` (per-CIK API path) | `sec_rate` | All XBRL concepts |
| `data.sec.gov/api/xbrl/companyconcept/CIK*/{taxonomy}/{tag}.json` | `app/providers/implementations/sec_fundamentals.py::fetch_concept` + `extract_concept_facts` (G10, PR #1198 merge `0ead989`) | — | — (no production consumer) | `sec_rate` | ✅ **PROVIDER PRIMITIVE 2026-05-18 (G10, PR #1198)** — thin HTTP wrapper exposed on `SecFundamentalsProvider`; general SEC `companyconcept` consumer (not bound to `TRACKED_CONCEPTS` / `DEI_TRACKED_CONCEPTS`). No production consumer in v1: under the 10 req/s shared SEC budget, wiring as a `fundamentals_sync` / `daily_financial_facts` replacement is wall-clock net-negative for any consumer needing ≥2 tags per CIK (snapshot path = 18 tags × 0.11 s ≈ 2.0 s vs companyfacts 1 × 0.11 s + ≈0.5-1.0 s ≈ 0.5-1.0 s). Primitive enables future single-tag refresh paths (#435 dilution-tracker per-CIK shares-outstanding topup; operator-driven concept probes). Spec `docs/_archive/2026-05/2026-05-17-g10-companyconcept-api-consumer.md`. |
| `data.sec.gov/api/xbrl/frames/...` | `app/providers/implementations/sec_fundamentals.py::fetch_frame` (G11, PR #1200 merge `c954c50`) | — | — (no production consumer) | `sec_rate` | ✅ **PROVIDER PRIMITIVE 2026-05-18 (G11, PR #1200)** — thin HTTP wrapper exposed on `SecFundamentalsProvider`; general SEC `frames` consumer. No production consumer in v1 by design: open downstream ticket #594 (peer-comparison radar + sector heatmap) has plausible demand but does NOT specifically commit to frames as the data source — #594 explicitly says "sector aggregates — needs sector median calculations server-side, OR client-side aggregation across the peer set." Wiring a full frames pipeline now would pre-commit before the UI/data-ingest shape settles. Primitive enables any future sector-aggregate consumer. Spec `docs/specs/etl/frames-api-consumer.md`. |
| Bulk `submissions.zip` (~1.54 GB) | `app/services/sec_bulk_download.py:225-227` | Stage 7 `sec_bulk_download` | — (one-shot per bootstrap) | `sec_bulk_download` lane | Initial-install drain only |
| Bulk `companyfacts.zip` (~1.38 GB) | `app/services/sec_bulk_download.py:229-231` | Stage 7 | — | `sec_bulk_download` lane | Initial-install drain |
| Bulk `form-13f-data-sets/{q}_form13f.zip` | `app/services/sec_bulk_download.py:237` | Stage 7 + Stage 10 | — (quarterly via 13F sweep cron walks per-filing) | `sec_bulk_download` lane | Bulk dataset |
| Bulk `insider-transactions-data-sets/{q}_form345.zip` | `app/services/sec_bulk_download.py:244` | Stage 7 + Stage 11 | — | `sec_bulk_download` lane | Bulk dataset |
| Bulk `form-n-port-data-sets/{q}_nport.zip` | `app/services/sec_bulk_download.py:251` | Stage 7 + Stage 12 | — | `sec_bulk_download` lane | Bulk dataset |
| Daily `master.YYYYMMDD.idx` | `app/providers/implementations/sec_edgar.py:565` | — (Layer 2, see §3) | ❌ unscheduled (Layer 2 gap #868) | `sec_rate` | Yesterday's filings reconciliation |
| Full-index `master.idx` quarterly | `app/providers/implementations/sec_full_index.py:read_master_idx` | — (Layer 4, see §3) | weekly Sun 05:15 UTC `sec_master_idx_quarterly_sweep` (`scheduler.py` ScheduledJob) | `sec_rate` | ✅ **WIRED 2026-05-17 (G12)** — cross-quarter discovery safety net. Walks `[CQ, CQ-1]` each fire (~50 MB / quarter), filters to (cik IN universe) + (form mapped to ManifestSource), UPSERTs missed accessions into `sec_filing_manifest`. Per-quarter txn isolation (commit/rollback boundary). Strict-by-default 404 — only the current quarter tolerates 404. Preloaded universe resolver (O(1) lookups). >1-quarter outage recovery is a Python REPL runbook against `run_master_idx_quarterly_sweep(..., quarters=[(YYYY,Q), ...])`. |
| Atom `getcurrent` | `app/providers/implementations/sec_getcurrent.py:50` | — | ❌ unscheduled (Layer 1 gap #867) | `sec_rate` | Live current-day filings; ISO-8859-1 |
| Atom `getcompany?CIK={cik}&type={form}` | NOT CONSUMED | — | — | — | ❌ **GAP** — per-CIK Atom alternative. Not consumed; per-CIK Atom is via Layer 1 (universe-wide Atom + filter). Likely fine — submissions.json is authoritative. No ticket needed unless operator wants per-CIK polling. |
| Filing-folder `/Archives/edgar/data/{cik}/{acc}/index.json` | `app/providers/implementations/sec_edgar.py:424` + `app/services/filing_documents.py` | — | `JOB_SEC_FILING_DOCUMENTS_INGEST` (`scheduler.py:655`) | `sec_rate` | Enumerate filing exhibits |
| Full-index `form.idx` quarterly | `app/services/top_filer_discovery.py:64` | — | manual / quarterly top-filer rebuild | `sec_rate` | 13F filer-directory bootstrap |
| eToro REST | `app/providers/implementations/etoro_broker.py` | Stage 2 `candle_refresh` | `JOB_DAILY_CANDLE_REFRESH` + orchestrator high-frequency-sync | `etoro` lane (separate from SEC) | Out of scope for SEC audit; covered by execution-track skill |
| FRED / BLS macro feeds | NOT CONSUMED | — | — | — | Not currently in scope. Settled-decisions §"Fundamentals provider posture" — free regulated only; no macro feed wired yet. |

---

## 5. Bootstrap stage table — for reference

`_BOOTSTRAP_STAGE_SPECS` at `app/services/bootstrap_orchestrator.py:795-900`. 26 stages (#1174 added S25 `mf_directory_sync` + S26 `sec_n_csr_bootstrap_drain`), 5 rate lanes (`init`, `etoro`, `sec_rate`, `sec_bulk_download`, `db`).

| # | Stage | Lane | Job | Endpoints it seeds |
|---|---|---|---|---|
| 1 | `universe_sync` | init | `nightly_universe_sync` | eToro instruments universe |
| 2 | `candle_refresh` | etoro | `daily_candle_refresh` | eToro candles |
| 3 | `cusip_universe_backfill` | sec_rate | `cusip_universe_backfill` | 13F Official List |
| 4 | `sec_13f_filer_directory_sync` | sec_rate | `sec_13f_filer_directory_sync` | 13F filer directory walk |
| 5 | `sec_nport_filer_directory_sync` | sec_rate | `sec_nport_filer_directory_sync` | N-PORT filer directory |
| 6 | `cik_refresh` | sec_rate | `daily_cik_refresh` | `company_tickers.json` + Stage 6 MF directory (#1171) + **Stage 7 exchange directory (G8, 2026-05-17)** |
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
| G4 | `sec_10q` parser | ✅ CLOSED 2026-05-14 | **#1168** | Synth no-op parser registered (sec-edgar §11.5.1). Owner-attribution to #414 was stale; #414 is the fundamentals_sync redesign, not a 10-Q parser ticket. |
| G5 | `sec_n_csr` parser feasibility | ✅ CLOSED 2026-05-14 | **#918** | Spike `docs/_archive/2026-05/spike-n-csr-feasibility.md` confirmed INFEASIBLE for v1; synth no-op landed (sec-edgar §11.5.1). N-CSR has zero per-holding identifier; fund holdings already covered by N-PORT-P. |
| G6 | `finra_short_interest` ingest | ✅ CLOSED — bimonthly 2026-05-18 (#915) + RegSHO daily 2026-05-18 (#916) | **#915 ✅ + #916 ✅** | Both portions landed via ScheduledJob + shared `finra` lane + module-global throttle. Bimonthly = `finra_short_interest_observations` + `_current` (23 quarterly partitions); daily = `finra_regsho_daily_observations` (25 quarterly partitions 2024-Q1 → 2030-Q1, no `_current`). Both manifest parsers are synth no-op (G7 precedent). |
| G7 | `sec_xbrl_facts` ManifestSource has no parser | ✅ CLOSED 2026-05-17 | — | Synth no-op parser registered (sec-edgar §11.5.1). XBRL facts continue to land via the Companyfacts bulk JSON path; the manifest row IS the audit signal. Pattern shares `sec_10q.py` (#1168). |
| G8 | `company_tickers_exchange.json` not consumed | ✅ CLOSED 2026-05-17 | **G8 PR** | Wired via `app/services/exchange_directory.py` + `sql/150` + Stage 7 in `daily_cik_refresh`. Empirical correction: cohort COUNT identical to `company_tickers.json` (10,353); the file is ticker-grain not CIK-grain (7,996 unique CIKs / 1,446 multi-ticker CIKs). Real value-add = `(ticker, exchange)` mapping for preferred series, ADR+OTC siblings, share-class siblings. In-scope fix to MF Stage 6 latent skip (now fires on every `daily_cik_refresh` invocation). |
| G9 | `company_tickers_mf.json` not consumed | ✅ CLOSED (predates audit) | **#1171 + #1174** | Stale audit entry corrected in same PR as G8. Consumed via `app/services/mf_directory.py::refresh_mf_directory` (Stage 6 sibling enrichment in `daily_cik_refresh`) + dedicated S25 `mf_directory_sync` bootstrap stage. Populates `cik_refresh_mf_directory` + `external_identifiers (sec, class_id)`. |
| G10 | `companyconcept` API not consumed | ✅ CLOSED 2026-05-18 | **PR #1198** (merge `0ead989`) | Provider primitive landed at `app/providers/implementations/sec_fundamentals.py::fetch_concept` + `extract_concept_facts`. No `fundamentals_sync` / `daily_financial_facts` wire-up by design (audit in spec §3.1 — under the 10 req/s shared SEC budget, companyconcept is wall-clock net-negative for any consumer needing ≥2 tags per CIK). Future single-tag consumer tickets re-open the wiring question. Future-consumer raw-payload invariant codified in spec §3.3. |
| G11 | `frames` API not consumed | ✅ CLOSED 2026-05-18 | **PR #1200** (merge `c954c50`) | Provider primitive landed at `app/providers/implementations/sec_fundamentals.py::fetch_frame`. No production consumer by design (open downstream ticket #594 does not specifically commit to frames; data-ingest design TBD). Re-open as a wiring ticket when #594's data-ingest scope settles. Future-consumer raw-payload invariant codified in spec §3.2. |
| G12 | Full-index `master.idx` quarterly not consumed | ✅ CLOSED 2026-05-17 | **PR #1196** (merge `e48eba3`) | Wired via `app/providers/implementations/sec_full_index.py` + `app/jobs/sec_master_idx_quarterly_sweep.py` + ScheduledJob `sec_master_idx_quarterly_sweep` (weekly Sun 05:15 UTC, source=`sec_rate`, prereq=`_bootstrap_complete`). Walks `[CQ, CQ-1]` per fire; per-quarter txn isolation; strict-by-default 404 (current quarter only tolerates 404); preloaded O(1) universe resolver. Invoker raises if any quarter fails so `job_runs.status='failure'` surfaces (Codex 2 pre-push HIGH). >1-quarter outage recovery via Python REPL runbook against the `quarters` kwarg. |
| G13 | `subjects_due_for_recheck` reader unused | ✅ CLOSED 2026-05-17 | **#1155** (sub-finding) + verification PR | Both readers drained per tick at `app/jobs/sec_per_cik_poll.py:195-198` with bounded 2/3+1/3 budget split. Static AST invariants at `tests/test_g13_recheck_reader_invariants.py` guard the wiring against future refactor; integration drain proved by `tests/test_sec_per_cik_poll.py::TestG13RecheckPath`. |

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
- Spec: `docs/specs/etl/coverage-model.md` — original #863-#873 redesign.
- Memory: [[us-source-coverage]], [[873-manifest-worker-parser-rollout]], [[etl-freshness-redesign]].
