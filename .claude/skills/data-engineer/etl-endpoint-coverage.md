# ETL endpoint coverage matrix

**Purpose.** Single answer to "are we covered?" for every official-source endpoint eBull consumes. Each row maps an endpoint to its five wiring layers — bootstrap stage, standard refresh, freshness index, watermark/retry, rate-limit pool — plus the manifest parser path where one applies.

**Read this before:** filing a "missing data" ticket; adding a new SEC fetcher; auditing why a steady-state poll isn't firing; deciding whether a `ManifestSource` is fully wired.

**Maintenance.** When a new endpoint is added, add a row and link the wiring layers to file:line. When a layer is wired or unwired, update the row and update [[us-source-coverage]] in memory.

Last audit pass: 2026-07-09 (reference refresh — `ManifestSource` now **18** values: `sec_nt` (#1015), `sec_pre14a` (#1892), `sec_424b` (#1816) added; `_BOOTSTRAP_STAGE_SPECS` rebuilt to **23** stages by the #1413 bulk-only redesign (per-CIK/per-form seed stages dropped); `JOB_SEC_INSIDER_TRANSACTIONS_INGEST` re-instated 2026-06-20; file:line anchors re-derived). Prior passes: 2026-05-24 (Layer 1/2/3 wiring shipped under #1155 / PR #1157); 2026-05-14 (#1168 sec_10q synth no-op — closes G4).

---

## 1. Cross-cutting wiring layers

For any endpoint to be "covered" we need ALL of:

| # | Layer | Owner | Code anchor |
|---|---|---|---|
| 1 | **Bootstrap stage** — first-install drain | `_BOOTSTRAP_STAGE_SPECS` | `app/services/bootstrap_orchestrator.py:1061-1234` (23 stages after the #1413 bulk-only redesign; see §5) |
| 2 | **Standard refresh** — steady-state job | `SCHEDULED_JOBS` + `_INVOKERS` | `SCHEDULED_JOBS` at `app/workers/scheduler.py:654` / `_INVOKERS` at `app/jobs/runtime.py:284` |
| 3 | **Freshness index** — per-(subject, source) cadence + last_known_*  | `_CADENCE` + `data_freshness_index` table | `app/services/data_freshness.py:69-102` + `sql/120` |
| 4 | **Watermark + retry** — `next_retry_at`, `last_known_filing_id` | `sec_filing_manifest` + `data_freshness_index` | `app/services/sec_manifest.py` + `sql/118`, `sql/120` |
| 5 | **Rate-limit pool** — shared per-IP budget | cross-process `sec_rate_gate` GCRA gate (#1484); per-process `_PROCESS_RATE_LIMIT_CLOCK` is test/fallback only | `app/providers/postgres_rate_gate.py` + `app/providers/sec_rate_gate_holder.py`; floor `SEC_MIN_REQUEST_INTERVAL_S = 0.11` at `app/providers/rate_gate.py:23`; per-process fallback clock at `sec_edgar.py:77-80` (SEC 10 req/s) |
| (6) | **Manifest parser** — typed-table materialisation | `manifest_parsers` registry | `app/services/manifest_parsers/__init__.py:register_all_parsers()` |

A "stranded" entry has rows in (4) but no resolver in (2) — manifest grows, never drains. A "dead-coded" entry has (1) + (6) but no (2) — bootstrap fills, steady-state never refreshes. See [§3](#3-discovery-layer-wiring) for the 2026-05 example (Layers 1/2/3, now resolved).

---

## 2. Per-source matrix — `ManifestSource` (18 enum values)

Definition: `app/services/sec_manifest.py:107-126` + `_FORM_TO_SOURCE` map at `:1000`. Source CHECK constraint born in `sql/118:37`, widened by `sql/153` (regsho), `sql/208` (nt), `sql/211` (pre14a), `sql/216` (424b).

**#1413 bulk-only bootstrap:** the per-CIK / per-form bootstrap *seed* stages were dropped (`filings_history_seed`, `sec_def14a_bootstrap` drain, `sec_form3_ingest`, `sec_insider_transactions_backfill`, `sec_13f_recent_sweep`, `sec_n_port_ingest`, `sec_n_csr_bootstrap_drain`). Bootstrap now seeds the manifest from bulk `sec_submissions_ingest` (S8) `filing_events` + `sec_first_install_drain` (S16), with `sec_master_idx_gap_close` (S15) advancing filing-metadata watermarks (8-K/10-K/10-Q/DEF14A/13D/13G only); ownership observations land from the bulk dataset ingesters (S10/S11/S12). The Bootstrap-stage column below reflects this.

| Source | Bootstrap stage | Standard refresh | Freshness cadence | Watermark | Pool | Parser | Status |
|---|---|---|---|---|---|---|---|
| `sec_8k` | `sec_8k_events_ingest` (stage_order 21) | manifest worker (post-#1155 — `JOB_SEC_8K_EVENTS_INGEST` retired from `SCHEDULED_JOBS`; bootstrap stage still dispatches via `_INVOKERS`) + `eight_k.py` dividend-events extraction (#1158) | 14d | `data_freshness_index` + `sec_filing_manifest.next_retry_at` | `sec_rate` | ✅ `eight_k.py` (#1126) | **WIRED** |
| `sec_def14a` | S8 `sec_submissions_ingest` → S16 `sec_first_install_drain` seed (dedicated `sec_def14a_bootstrap` bootstrap stage dropped #1413) | manifest worker (post-#1155 — `JOB_SEC_DEF14A_INGEST` retired) + weekly `sec_def14a_bootstrap` safety net (steady-state ScheduledJob, `scheduler.py:1051`) | 365d | both | `sec_rate` | ✅ `def14a.py` (#1128) | **WIRED** |
| `sec_13d` | S8 → S16 manifest seed (`filings_history_seed` bootstrap stage dropped #1413; S15 gap-close covers 13D/G metadata) | manifest worker only | 90d | both | `sec_rate` | ✅ `sec_13dg.py` (#1129) | **WIRED**, manifest worker + Layer 1/2/3/4 discovery (no dedicated cron, see §3) |
| `sec_13g` | S8 → S16 manifest seed (same as 13D) | manifest worker only | 90d | both | `sec_rate` | ✅ `sec_13dg.py` (#1129) | **WIRED**, same caveat |
| `sec_form3` | Stage 11 bulk `sec_insider_ingest_from_dataset` (Form 3/4/5) | manifest worker only (legacy `sec_form3_ingest` cron + bootstrap stage dropped #1413/#1155) | 30d | both | `sec_rate` | ✅ `insider_345.py` (#1130) | **WIRED** |
| `sec_form4` | Stage 11 bulk `sec_insider_ingest_from_dataset` | manifest worker + `JOB_SEC_INSIDER_TRANSACTIONS_INGEST` newest-first ingester (hourly :15, `scheduler.py:843`; **RE-INSTATED 2026-06-20** — #1155's retirement was falsified on dev: the oldest-first manifest drain against a ~1.46M-row backlog left recent Form 4 ~3mo stale) + round-robin `sec_insider_transactions_backfill` (hourly :45) deep-tail drain | 30d | both | `sec_rate` | ✅ `insider_345.py` (#1130) | **WIRED** |
| `sec_form5` | Stage 11 bulk | `JOB_SEC_INSIDER_TRANSACTIONS_INGEST` + manifest worker | 365d | both | `sec_rate` | ✅ `insider_345.py` `_parse_form5` (#1134) | **WIRED**, observation `source='form4'` (enum lacks form5; provenance via `insider_filings.document_type='5'` JOIN) |
| `sec_13f_hr` | Stage 10 bulk `sec_13f_ingest_from_dataset` | manifest worker only (legacy `sec_13f_quarterly_sweep` cron + recent-sweep bootstrap stage dropped #1413/#1155) | 120d | both | `sec_rate` | ✅ `sec_13f_hr.py` (#1133) — EdgarTools wrapper via `app/providers/implementations/sec_13f.py` (#931, 2026-05-05) | **WIRED**. PRN drop + 2023-01-03 VALUE×1000 cutover applied by the shared `app/services/thirteen_f_normalise.py` normaliser (`VALUE_DOLLARS_CUTOVER`), called from `sec_13f_hr.py` (~:465) and `sec_13f_dataset_ingest.py`; the parser at `sec_13f.py` is a pure EdgarTools wrapper preserving raw values per the #931 contract. Spike `docs/_archive/2026-05/spike-13f-hr-edgartools.md` certifies the EdgarTools drop-in is complete + REBUTS the plan §2 PR 9 alternative scope (Filing.obj() / ThirteenF manifest-adapter restructure) on five binding grounds — rate-limit-pool bypass, cutover-semantics divergence, no native PRN drop, no raw-payload persistence hook, no transient-vs-deterministic error classification. |
| `sec_n_port` | Stage 12 bulk `sec_nport_ingest_from_dataset` | manifest worker only (legacy `sec_n_port_ingest` cron + recent-ingest bootstrap stage dropped #1413/#1155) | 90d | both | `sec_rate` | ✅ `sec_n_port.py` (#1133) | **WIRED** |
| `sec_10k` | `sec_business_summary_bootstrap` (stage_order 18) | manifest worker (post-#1155 retirement of legacy `sec_business_summary_ingest` cron) + weekly `sec_business_summary_bootstrap` safety net (`scheduler.py:887`) | 120d | both | `sec_rate` | ✅ `sec_10k.py` (#1152, 2026-05-13) | **WIRED** — Option C `(filed_at, source_accession)` gate applied (sql/148). Legacy daily 03:15 cron retired in the first #1155 cron-retirement sweep — manifest path is sole steady-state writer. |
| `sec_10q` | — | manifest worker (synth no-op per #1168) | 60d | both | `sec_rate` (unused by parser) | ✅ `sec_10q.py` (#1168) | **WIRED** — synth no-op (sec-edgar §11.5.1). Financial data lands via Companyfacts XBRL; narrative HTML has no v1 consumer. Parser body is `return ParseOutcome(status='parsed', parser_version='10q-noop-v1')`. Closes G4. |
| `sec_n_csr` | `mf_directory_sync` (stage_order 26 — classId resolver inputs); N-CSR body discovery is steady-state (the `sec_n_csr_bootstrap_drain` bootstrap stage was dropped #1413, kept as an on-demand job only) | manifest worker (real fund-metadata parser, #1171) | 200d | both | `sec_rate` | ✅ `sec_n_csr.py` (#1171, 2026-05-15, replaced #918 / PR #1170 synth no-op) | **WIRED** — real fund-metadata parser (#1171). Extracts per-(series, class) Tier 1 scalars (expense ratio, NAV, returns, portfolio turnover, holdings count, inception, contact) + Tier 2 dimensional JSONB (sector / region / credit allocation, returns_pct, benchmark_returns_pct, growth_curve) + Tier 3 `raw_facts` fallback. `requires_raw_payload=False` (no `store_raw`; re-parse re-fetches iXBRL companion). Source-priority chain: `period_end DESC, filed_at DESC, source_accession DESC` (settled-decisions §"Source priority for fund metadata"). classId → instrument_id via `external_identifiers (provider='sec', identifier_type='class_id')` populated by the `mf_directory_sync` bootstrap stage (stage_order 26, #1174; daily cron `daily_cik_refresh` retains the bundled call as a drift-heal safety net). The on-demand `sec_n_csr_bootstrap_drain` job (`scheduler.py:5939`; dropped as a bootstrap stage #1413) walks distinct trust CIKs from `cik_refresh_mf_directory` and enqueues last-2-years N-CSR + N-CSRS accessions with `subject_type='institutional_filer' + instrument_id=NULL` for the manifest worker to drain. Spike `docs/_archive/2026-05/spike-n-csr-feasibility.md` INFEASIBLE-CONFIRMED verdict applied to holdings-attestation lane only (§10.6 scope narrowing); fund-metadata lane is orthogonal. |
| `sec_xbrl_facts` | Stage 9 `sec_companyfacts_ingest` (bulk-zip) + `fundamentals_sync` (stage_order 25, derivation-only) | `JOB_FUNDAMENTALS_SYNC` cron (`scheduler.py:781`) + manifest worker (synth no-op, G7) | 120d | both | `sec_rate` (unused by parser) | ✅ `sec_xbrl_facts.py` (G7) | **WIRED** — synth no-op (sec-edgar §11.5.1). XBRL facts land via the Companyfacts bulk JSON path; the manifest row IS the audit signal. Parser body returns `ParseOutcome(status='parsed', parser_version='xbrl-facts-noop-v1')`. Closes G7. |
| `finra_short_interest` | ScheduledJob `finra_short_interest_refresh` (daily 12:00 UTC) — discovery + fetch + write inline | manifest worker (synth no-op, G6/#915) | 20d | both | `finra` (1 req/s; new lane disjoint from sec_rate, sql/151+152 + sources.py) | ✅ `finra_short_interest.py` (G6/#915 bimonthly portion) | **WIRED 2026-05-18 (#915)** — ScheduledJob owns fetch + parse + UPSERT into `finra_short_interest_observations` + `finra_short_interest_current`; manifest parser is synth no-op (sec_xbrl_facts G7 precedent). Symbol resolution via `normalise_symbol` (`app/services/finra_short_interest_ingest.py:105`) strip-non-alnum+upper (BRK.A ↔ BRKA). Revision-window: two most-recent settlement dates always re-fetched regardless of manifest status (catches `revisionFlag='Y'` in-place corrections). New skill `.claude/skills/data-sources/finra.md` documents endpoint shape + cohort cliff (pre-June 2021 OTC-only). |
| `finra_regsho_daily` | ScheduledJob `finra_regsho_daily_refresh` (daily 23:00 UTC) — discovery × 6 prefixes + fetch + write inline | manifest worker (synth no-op, G6/#916) | 2d | both | `finra` (1 req/s; shares module-global throttle with bimonthly sibling, sql/153+154 + sources.py) | ✅ `finra_regsho_daily.py` (G6/#916 daily portion) | **WIRED 2026-05-18 (#916)** — ScheduledJob owns fetch + parse + UPSERT into `finra_regsho_daily_observations` (25 quarterly partitions 2024-Q1 → 2030-Q1; no `_current` — daily file IS the snapshot). 6 prefixes per trade-date (CNMS aggregate + FNQC/FNRA/FNSQ/FNYX/FORF facilities). Decimal volumes (`NUMERIC(18, 6)` — NOT integer); `Market` is `TEXT` (comma-joined `B,Q,N` on CNMS). Body-Date validation per row + footer-row-count validation per file. Revision-window: last-2 trade dates × 6 prefixes always re-fetched. Sibling provider imports the bimonthly module's throttle globals so combined 1 req/s budget is preserved. |
| `sec_nt` | S8 → S16 manifest seed (NT 10-K / NT 10-Q mapped in `_FORM_TO_SOURCE`) | manifest worker only | 400d (episodic) | both | `sec_rate` | ✅ `sec_nt.py` (#1015) — parse+raw (`requires_raw_payload=True`) | **WIRED** — Form 12b-25 late-filing notices → `nt_filing_notices` (sql/208). `/A` variants + NT 20-F stay metadata-only, out of scope. |
| `sec_pre14a` | S8 → S16 manifest seed (PRE 14A / PRER14A mapped in `_FORM_TO_SOURCE`) | manifest worker only | 400d (episodic) | both | `sec_rate` | ✅ `sec_pre14a.py` (#1892) — parse+raw | **WIRED** — preliminary-proxy proposal-signal parser → `pre14a_proposal_signals` (sql/211). A meeting-agenda signal, NOT an ownership source (#1320 concern unaffected — PRE rows never routed to `sec_def14a`). |
| `sec_424b` | S8 → S16 manifest seed (424B1/3/4/5/7 mapped; B8 unmapped) | manifest worker only | 400d (episodic) | both | `sec_rate` | ✅ `sec_424b.py` (#1816) — parse+raw | **WIRED** — Rule 424(b) prospectus offerings → `prospectus_offerings` (sql/216). 424B2 is volume-gated (#1975, sql/217): mapped, but the parser tombstones B2 WITHOUT fetch once a filer's lifetime B2 count exceeds `_424B2_VOLUME_CAP = 100` (bank/ETN structured-note factories — a fetch-cost bound, not a classification). |

---

## 3. Discovery layer wiring — Layer 1 / Layer 2 / Layer 3 / Layer 4

The #863-#873 ETL freshness redesign (spec at `docs/specs/etl/coverage-model.md`) ships three steady-state discovery layers, cheapest-first. They sit BETWEEN the bootstrap drain and the manifest worker — discovering new accessions and inserting `sec_filing_manifest` rows for the worker to drain. **Layer 4** (G12, 2026-05-17) extends the family for cross-quarter recovery — the case Layer 1/2/3 cannot cover (tombstoned-CIK / deactivated-CIK / merged-CIK / late-amendment).

| Layer | Endpoint | Code | Bootstrap-side caller | Steady-state caller | Status |
|---|---|---|---|---|---|
| 1 | Atom `getcurrent?action=getcurrent&output=atom` (every 5 min) | `run_atom_fast_lane` at `app/jobs/sec_atom_fast_lane.py:104` | — | `sec_atom_fast_lane` ScheduledJob (5 min cadence, `scheduler.py:1533`) | ✅ **WIRED 2026-05-13 (#1155 / PR #1157)** — `_INVOKERS[JOB_SEC_ATOM_FAST_LANE]` at `runtime.py:441`; ScheduledJob at `scheduler.py:1533` |
| 2 | Daily `master.YYYYMMDD.idx` (04:00 UTC reconciliation) | `run_daily_index_reconcile` at `app/jobs/sec_daily_index_reconcile.py:46` | — | `sec_daily_index_reconcile` ScheduledJob (daily 04:00 UTC, `scheduler.py:1554`) | ✅ **WIRED 2026-05-13 (#1155 / PR #1157)** — `_INVOKERS[JOB_SEC_DAILY_INDEX_RECONCILE]` at `runtime.py:442`; ScheduledJob at `scheduler.py:1554`; exempt from universal-bootstrap gate (#1181) |
| 3 | Per-CIK `submissions/CIK*.json` (per `data_freshness._CADENCE`) | `run_per_cik_poll` at `app/jobs/sec_per_cik_poll.py:302` | — | `sec_per_cik_poll` ScheduledJob (hourly :00, `scheduler.py:1608`) | ✅ **WIRED 2026-05-13 (#1155 / PR #1157)** — `_INVOKERS[JOB_SEC_PER_CIK_POLL]` at `runtime.py:443`; ScheduledJob at `scheduler.py:1608` |
| 4 | Full-index quarterly `master.idx` (weekly Sun 05:15 UTC; walks `[CQ, CQ-1]`) | `run_master_idx_quarterly_sweep` at `app/jobs/sec_master_idx_quarterly_sweep.py:170` | — | `sec_master_idx_quarterly_sweep` ScheduledJob (G12, 2026-05-17, `scheduler.py:1636`) | ✅ **WIRED 2026-05-17** — cross-quarter discovery safety net. Per-quarter txn isolation; strict-by-default 404; preloaded O(1) universe resolver. |

Tickets #867 / #868 / #870 CLOSED 2026-05-13 by **#1155 / PR #1157** which added the missing `_INVOKERS[]` + `ScheduledJob` rows. Wiring CONFIRMED LIVE via `grep` on `app/jobs/runtime.py:441-443` + `app/workers/scheduler.py:1533, 1554, 1608`.

### Impact (historical — resolved)

The redesign's three polling layers were coded but unscheduled until #1155 / PR #1157 wired them (2026-05-13); Layer 4 followed (G12). All four are now live ScheduledJobs (table above). The legacy per-form ingest crons that carried discovery pre-wiring have since been retired from `SCHEDULED_JOBS`:

| Legacy cron | Current status |
|---|---|
| `sec_insider_transactions_ingest` | **RE-INSTATED 2026-06-20** as hourly newest-first Form 4 keeper (`scheduler.py:843`) — the oldest-first manifest drain could not keep recent Form 4 fresh against the deep backlog |
| `sec_form3_ingest` | retired post-#1155 (`scheduler.py:939` comment) |
| `sec_def14a_ingest` | retired post-#1155 (weekly `sec_def14a_bootstrap` safety net remains) |
| `sec_8k_events_ingest` | retired from `SCHEDULED_JOBS` post-#1155 (`scheduler.py:879` comment; bootstrap stage still dispatches) |
| `sec_business_summary_ingest` | retired post-#1155 (weekly `sec_business_summary_bootstrap` remains) |
| `sec_dividend_calendar_ingest` | retired post-#1155 (#1166); manifest worker + `eight_k.py` (#1158) sole dividend-events writer |
| `sec_n_port_ingest` | retired |
| `sec_13f_quarterly_sweep` | retired |

`data_freshness._CADENCE` (`app/services/data_freshness.py:69`) is queried by the per-CIK seeder (`seed_freshness_for_manifest_row`, `:151`) on every manifest write, populating `expected_next_at`. The consumer readers `subjects_due_for_poll` (`:533`) + `subjects_due_for_recheck` (`:581`) are now reached on cadence by the wired Layer 3 `run_per_cik_poll` — no longer a write-only ledger.

**Sub-gap G13:** ✅ CLOSED 2026-05-17 — `run_per_cik_poll` drains BOTH `subjects_due_for_poll` AND `subjects_due_for_recheck` per tick (`app/jobs/sec_per_cik_poll.py:343-354`) with a bounded 2/3 + ~1/3 budget split (`max_subjects=100` → poll=66, recheck=34). Integration coverage at `tests/test_sec_per_cik_poll.py::TestG13RecheckPath` proves the never_filed-stays-in-queue contract + alongside-poll drain; static AST invariants at `tests/test_g13_recheck_reader_invariants.py` guarantee a future refactor cannot silently drop one reader path. Hourly cadence asserted at `tests/test_layer_123_wiring.py::test_layer3_per_cik_poll_registered`.

---

## 4. Reference + bulk-archive endpoint matrix (non-manifest)

These endpoints don't have a `ManifestSource` because they're not per-filing dispatched — they seed identifiers, populate bulk reference tables, or download tarballs.

| Endpoint | Code | Bootstrap stage | Steady-state | Pool | Notes |
|---|---|---|---|---|---|
| `www.sec.gov/files/company_tickers.json` | `app/providers/implementations/sec_edgar.py:57` (`_TICKERS_URL`) | Stage 6 `cik_refresh` (`JOB_DAILY_CIK_REFRESH`) | `JOB_FUNDAMENTALS_SYNC` daily (`scheduler.py:781`) calls `daily_cik_refresh()` inline at `scheduler.py:3958` | `sec_rate` | Ticker → CIK bridge, ~10k operating-co rows; conditional GET ETag-aware. **Note:** `JOB_DAILY_CIK_REFRESH` is in `_INVOKERS` (manual trigger only) but absent from `SCHEDULED_JOBS` — the daily cadence lives inside the fundamentals_sync body, not as a standalone scheduled job. |
| `www.sec.gov/files/company_tickers_exchange.json` | `app/services/exchange_directory.py:refresh_exchange_directory` | Stage 6 `cik_refresh` Stage 7 sibling enrichment (`scheduler.py:daily_cik_refresh`) | Same — bundled into daily `daily_cik_refresh` | `sec_rate` | ✅ **WIRED 2026-05-17 (G8)** — `cik_refresh_exchange_directory` snapshot table, ticker-grain PK `(cik, ticker)`. Empirical 2026-05-17 payload: 10,353 rows / 7,996 unique CIKs / 1,446 multi-ticker CIKs (BAC=17 variants, JPM=9). Same CIK cohort COUNT as `company_tickers.json` but ticker-grain not CIK-grain — captures share-class siblings, preferred-series, ADR+OTC variants. Observed-ever semantics; `last_seen` is the watermark. No v1 consumer; consumers land via separate tickets. |
| `www.sec.gov/files/company_tickers_mf.json` | `app/services/mf_directory.py:refresh_mf_directory` | Stage 6 `cik_refresh` Stage 6 sibling enrichment (`scheduler.py:daily_cik_refresh`) + S26 `mf_directory_sync` (#1174) dedicated bootstrap stage | Same — bundled into daily `daily_cik_refresh` | `sec_rate` | ✅ **WIRED (#1171 + #1174)** — `cik_refresh_mf_directory` snapshot keyed by `classId` + `external_identifiers (sec, class_id)` write-through for in-universe symbols. ~28k mutual-fund rows with `seriesId` + `classId` per row. Consumed by `_fund_class_resolver.classify_resolver_miss` for N-CSR fund-metadata path. G9 in §7 closed by this entry; see also G8 stale-matrix correction in same PR. |
| `www.sec.gov/files/investment/13flist{year}q{quarter}.txt` | `app/services/sec_13f_securities_list.py:77` (`_LIST_URL`) | Stage 3 `cusip_universe_backfill` | `JOB_CUSIP_UNIVERSE_BACKFILL` (`scheduler.py:1284`) | `sec_rate` | 13F Official List, ~24k rows; CUSIP → issuer-name authoritative bridge |
| `data.sec.gov/submissions/CIK*.json` | `app/providers/implementations/sec_submissions.py:238` (`check_freshness`) | Stage 8 `sec_submissions_ingest` (bulk-zip) — per-CIK `sec_submissions_files_walk` dropped from bootstrap #1413 | Layer 3 `sec_per_cik_poll` + `JOB_SEC_INSIDER_TRANSACTIONS_INGEST` watermark walk | `sec_rate` | Per-CIK 1000-most-recent + overflow pages via `filings.files[]` |
| `data.sec.gov/submissions/CIK*-submissions-NNN.json` | `app/services/sec_submissions_files_walk.py` + `app/jobs/sec_rebuild.py:312` | — (dropped from bootstrap #1413; on-demand/steady-state only) | manual rebuild via `POST /jobs/sec_rebuild/run` | `sec_rate` | Overflow paging for deep-history parity |
| `data.sec.gov/api/xbrl/companyfacts/CIK*.json` | `app/providers/implementations/sec_fundamentals.py:58` (`_BASE_URL`) | Stage 9 `sec_companyfacts_ingest` (bulk-zip) | `JOB_FUNDAMENTALS_SYNC` (per-CIK API path) | `sec_rate` | All XBRL concepts |
| `data.sec.gov/api/xbrl/companyconcept/CIK*/{taxonomy}/{tag}.json` | `app/providers/implementations/sec_fundamentals.py::fetch_concept` + `extract_concept_facts` (G10, PR #1198 merge `0ead989`) | — | — (no production consumer) | `sec_rate` | ✅ **PROVIDER PRIMITIVE 2026-05-18 (G10, PR #1198)** — thin HTTP wrapper exposed on `SecFundamentalsProvider`; general SEC `companyconcept` consumer (not bound to `TRACKED_CONCEPTS` / `DEI_TRACKED_CONCEPTS`). No production consumer in v1: under the 10 req/s shared SEC budget, wiring as a `fundamentals_sync` / `daily_financial_facts` replacement is wall-clock net-negative for any consumer needing ≥2 tags per CIK (snapshot path = 18 tags × 0.11 s ≈ 2.0 s vs companyfacts 1 × 0.11 s + ≈0.5-1.0 s ≈ 0.5-1.0 s). Primitive enables future single-tag refresh paths (#435 dilution-tracker per-CIK shares-outstanding topup; operator-driven concept probes). Spec `docs/_archive/2026-05/2026-05-17-g10-companyconcept-api-consumer.md`. |
| `data.sec.gov/api/xbrl/frames/...` | `app/providers/implementations/sec_fundamentals.py::fetch_frame` (G11, PR #1200 merge `c954c50`) | — | — (no production consumer) | `sec_rate` | ✅ **PROVIDER PRIMITIVE 2026-05-18 (G11, PR #1200)** — thin HTTP wrapper exposed on `SecFundamentalsProvider`; general SEC `frames` consumer. No production consumer in v1 by design: open downstream ticket #594 (peer-comparison radar + sector heatmap) has plausible demand but does NOT specifically commit to frames as the data source — #594 explicitly says "sector aggregates — needs sector median calculations server-side, OR client-side aggregation across the peer set." Wiring a full frames pipeline now would pre-commit before the UI/data-ingest shape settles. Primitive enables any future sector-aggregate consumer. Spec `docs/specs/etl/frames-api-consumer.md`. |
| Bulk `submissions.zip` (~1.54 GB) | `app/services/sec_bulk_download.py:266` (`build_bulk_archive_inventory`, `:255`) | Stage 7 `sec_bulk_download` | — (one-shot per bootstrap) | `sec_bulk_download` lane | Initial-install drain only |
| Bulk `companyfacts.zip` (~1.38 GB) | `app/services/sec_bulk_download.py:270` | Stage 7 | — | `sec_bulk_download` lane | Initial-install drain |
| Bulk `form-13f-data-sets/{q}_form13f.zip` | `app/services/sec_bulk_download.py:281` | Stage 7 + Stage 10 | — (quarterly refresh via `JOB_SEC_QUARTERLY_DATASETS_BULK_REFRESH`) | `sec_bulk_download` lane | Bulk dataset |
| Bulk `insider-transactions-data-sets/{q}_form345.zip` | `app/services/sec_bulk_download.py:289` | Stage 7 + Stage 11 | — | `sec_bulk_download` lane | Bulk dataset |
| Bulk `form-n-port-data-sets/{q}_nport.zip` | `app/services/sec_bulk_download.py:296` | Stage 7 + Stage 12 | — | `sec_bulk_download` lane | Bulk dataset |
| Daily `master.YYYYMMDD.idx` | `app/providers/implementations/sec_edgar.py:716` (`fetch_master_index`) | — (Layer 2, see §3) | ✅ Layer 2 `sec_daily_index_reconcile` (daily 04:00 UTC) — G2 closed #1155 | `sec_rate` | Yesterday's filings reconciliation |
| Full-index `master.idx` quarterly | `app/providers/implementations/sec_full_index.py:read_master_idx` | — (Layer 4, see §3) | weekly Sun 05:15 UTC `sec_master_idx_quarterly_sweep` (`scheduler.py` ScheduledJob) | `sec_rate` | ✅ **WIRED 2026-05-17 (G12)** — cross-quarter discovery safety net. Walks `[CQ, CQ-1]` each fire (~50 MB / quarter), filters to (cik IN universe) + (form mapped to ManifestSource), UPSERTs missed accessions into `sec_filing_manifest`. Per-quarter txn isolation (commit/rollback boundary). Strict-by-default 404 — only the current quarter tolerates 404. Preloaded universe resolver (O(1) lookups). >1-quarter outage recovery is a Python REPL runbook against `run_master_idx_quarterly_sweep(..., quarters=[(YYYY,Q), ...])`. |
| Atom `getcurrent` | `app/providers/implementations/sec_getcurrent.py:50` | — (Layer 1, see §3) | ✅ Layer 1 `sec_atom_fast_lane` (5 min) — G1 closed #1155 | `sec_rate` | Live current-day filings; ISO-8859-1 |
| Atom `getcompany?CIK={cik}&type={form}` | NOT CONSUMED | — | — | — | ❌ **GAP** — per-CIK Atom alternative. Not consumed; per-CIK Atom is via Layer 1 (universe-wide Atom + filter). Likely fine — submissions.json is authoritative. No ticket needed unless operator wants per-CIK polling. |
| Filing-folder `/Archives/edgar/data/{cik}/{acc}/index.json` | `app/providers/implementations/sec_edgar.py:403` (`fetch_filing_index`) + `app/services/filing_documents.py` | — | `JOB_SEC_FILING_DOCUMENTS_INGEST` (`scheduler.py:861`) | `sec_rate` | Enumerate filing exhibits |
| Full-index `form.idx` quarterly | `app/services/top_filer_discovery.py:64` | — | manual / quarterly top-filer rebuild | `sec_rate` | 13F filer-directory bootstrap |
| eToro REST | `app/providers/implementations/etoro_broker.py` | Stage 2 `candle_refresh` | `JOB_DAILY_CANDLE_REFRESH` + orchestrator high-frequency-sync | `etoro` lane (separate from SEC) | Out of scope for SEC audit; covered by execution-track skill |
| FRED / BLS macro feeds | NOT CONSUMED | — | — | — | Not currently in scope. Settled-decisions §"Fundamentals provider posture" — free regulated only; no macro feed wired yet. |

---

## 5. Bootstrap stage table — for reference

`_BOOTSTRAP_STAGE_SPECS` at `app/services/bootstrap_orchestrator.py:1061-1234`. **23 stages** after the #1413 bulk-only redesign (the per-CIK / per-form seed stages `sec_submissions_files_walk`, `filings_history_seed`, `sec_def14a_bootstrap`, `sec_insider_transactions_backfill`, `sec_form3_ingest`, `sec_13f_recent_sweep`, `sec_n_port_ingest`, `sec_n_csr_bootstrap_drain` were dropped; FSDS stages + gap-close + validation added). `stage_order` values 1-27 keep gaps (19/20/22/23 vacated) so surviving stages did not renumber. 5 base rate lanes (`init`, `etoro`, `sec_rate`, `sec_bulk_download`, `db`) + post-#1141 family split (`db_filings`, `db_fundamentals_raw`, `db_ownership_inst`, `db_ownership_insider`, `db_ownership_funds`) + post-PR-1b `openfigi` lane + post-#915 `finra` lane. See `.claude/skills/data-engineer/SKILL.md` §6.5 (Pipeline orchestration) for the lane concurrency model + cap rules.

**Lane → stage mapping** lives in `_STAGE_LANE_OVERRIDES` at `bootstrap_orchestrator.py:1018-1039`. Default lane (when a `stage_key` is absent from the override map) is the lane field on the StageSpec itself; the override map is consulted FIRST and wins on collision. The Lane column below is the EFFECTIVE lane (post-override). When ADDING a new bootstrap stage that needs a non-default lane, add the entry to `_STAGE_LANE_OVERRIDES` AND update this matrix's "Pool" column for the affected source row.

| stage_order | Stage | Effective lane | Job | Endpoints / data it seeds |
|---|---|---|---|---|
| 1 | `universe_sync` | init | `nightly_universe_sync` | eToro instruments universe |
| 2 | `candle_refresh` | etoro | `daily_candle_refresh` | eToro candles |
| 3 | `cusip_universe_backfill` | sec_rate | `cusip_universe_backfill` | 13F Official List |
| 4 | `sec_13f_filer_directory_sync` | sec_rate | `sec_13f_filer_directory_sync` | 13F filer directory walk |
| 5 | `sec_nport_filer_directory_sync` | sec_rate | `sec_nport_filer_directory_sync` | N-PORT filer directory |
| 6 | `cik_refresh` | sec_rate | `daily_cik_refresh` | `company_tickers.json` + MF directory (#1171) + exchange directory (G8) |
| 7 | `sec_bulk_download` | sec_bulk_download | `sec_bulk_download` | submissions.zip, companyfacts.zip, 13F/insider/NPORT/FSDS bulk zips |
| 8 | `sec_submissions_ingest` | db_filings | `sec_submissions_ingest` | submissions.zip ingest → `filing_events` (the manifest-seed source) |
| 9 | `sec_companyfacts_ingest` | db_fundamentals_raw | `sec_companyfacts_ingest` | companyfacts.zip ingest → `financial_facts_raw` |
| 10 | `sec_13f_ingest_from_dataset` | db_ownership_inst | `sec_13f_ingest_from_dataset` | 13F bulk → `ownership_institutions_observations` |
| 11 | `sec_insider_ingest_from_dataset` | db_ownership_insider | `sec_insider_ingest_from_dataset` | Form 3/4/5 bulk → `ownership_insiders_observations` |
| 12 | `sec_nport_ingest_from_dataset` | db_ownership_funds | `sec_nport_ingest_from_dataset` | N-PORT bulk → `ownership_funds_observations` |
| 13 | `cusip_resolver_post_bulk_sweep` | openfigi | `cusip_resolver_post_bulk_sweep` | OpenFIGI resolve of unresolved 13F/NPORT CUSIPs (#1233) |
| 14 | `sec_fsds_class_shares_ingest` | db | `sec_fsds_class_shares_ingest` | DERA FSDS per-class shares outstanding (#788) |
| 15 | `sec_master_idx_gap_close` | sec_rate | `sec_master_idx_gap_close` | Current+prev quarter `form.idx` metadata gap-close for 8-K/10-K/10-Q/DEF14A/13D/13G watermarks (#1415) |
| 16 | `sec_first_install_drain` | sec_rate | `sec_first_install_drain` (`max_subjects=None, use_bulk_zip=True, follow_pagination=False`) | Per-CIK manifest seed from bulk `filing_events` (#1413) |
| 17 | `sec_fsds_dimensional_ingest` | db | `sec_fsds_dimensional_ingest` | DERA FSDS segment/product/geographic dimensional facts (#1590) |
| 18 | `sec_business_summary_bootstrap` | sec_rate | `sec_business_summary_bootstrap` | 10-K Item 1 metadata seed |
| 21 | `sec_8k_events_ingest` | sec_rate | `sec_8k_events_ingest` | 8-K metadata seed |
| 24 | `ownership_observations_backfill` | db | `ownership_observations_backfill` | Recompute `_current` from observations |
| 25 | `fundamentals_sync` | db_fundamentals_raw | `fundamentals_sync_bootstrap` (derivation-only; NO HTTP) | Derive financial periods + TTM from `financial_facts_raw` (S9). 4-cap req (bulk_archives_ready + cik_mapping_ready + submissions_processed + fundamentals_raw_seeded) per PR-C1 / #1309. |
| 26 | `mf_directory_sync` | sec_rate | `mf_directory_sync` | classId → instrument_id mapping + fund-trust directory refresh (#1174) |
| 27 | `bootstrap_validation` | db | `bootstrap_validation` | Terminal load-time validation gate (#1419); raises on hard-floor breach → `partial_error`. |

---

## 6. Rate-limit pool inventory

| Pool | Budget | Code | Consumers |
|---|---|---|---|
| `sec_rate` | 10 req/s shared per-IP (SEC fair-use ceiling) | **cross-process** via `sec_rate_gate` GCRA gate (#1484, `app/providers/postgres_rate_gate.py`, floor `SEC_MIN_REQUEST_INTERVAL_S = 0.11` at `app/providers/rate_gate.py:23`); `_PROCESS_RATE_LIMIT_CLOCK`/`_LOCK` in `sec_edgar.py:77-80` is now per-process test/fallback only | Every SEC consumer via the injected gate — sync `ResilientClient` (`SecFilingsProvider`, `SecFundamentalsProvider`, manifest parsers' `requires_raw_payload=True` fetchers), async `_AsyncRateLimiter`/`PipelinedSecFetcher`, bulk refresh/download |
| `sec_bulk_download` | Bandwidth-probe-bounded (slow-connection bypass switches to legacy per-CIK path) | `app/services/sec_bulk_download.py:1301` (`download_bulk_archives`) | Stage 7 bulk-zip download only |
| `etoro` | eToro's per-account REST quota (broker-side enforced) | `app/providers/implementations/etoro_broker.py:134,139` `ResilientClient` | Quotes, candles, orders, positions |
| `db` (+ `db_filings` / `db_fundamentals_raw` / `db_ownership_inst` / `db_ownership_insider` / `db_ownership_funds`) | per-lane cap via `_LANE_MAX_CONCURRENCY` (`bootstrap_orchestrator.py:168`); heavy bulk ingesters bounded to 2 (`_HEAVY_INGEST_MAX_CONCURRENCY`, #1426) | `app/db/pool.py::open_pool` + `_LANE_MAX_CONCURRENCY` | Phase C DB ingesters (S8-S12 family lanes, S14/S17/S24/S27 on `db`); `ownership_observations_backfill`; `fundamentals_sync` |
| `finra` | 1 req/s polite floor (cdn.finra.org; disjoint from `sec_rate` — different host) | module-global throttle clock+lock at `app/providers/implementations/finra_short_interest.py:48-50` (`_FINRA_RATE_LIMIT_CLOCK` / `_LOCK` / `_FINRA_MIN_INTERVAL_S = 1.0`; daily RegSHO imports the same globals) | `finra_short_interest_refresh` (#915) + `finra_regsho_daily_refresh` (#916) |
| FRED / BLS macro | none — no fetchers wired | — | Not in scope (settled-decisions §"Fundamentals provider posture") |

---

## 7. Gap register — open / pending / by-design

| ID | Source | Status | Ticket | Resolution |
|---|---|---|---|---|
| G1 | Layer 1 Atom fast-lane unwired | ✅ CLOSED 2026-05-13 | **#867 / #1155** (PR #1157) | `_INVOKERS[JOB_SEC_ATOM_FAST_LANE]` at `runtime.py:441` + ScheduledJob at `scheduler.py:1533` (5 min cadence) |
| G2 | Layer 2 daily-index reconcile unwired | ✅ CLOSED 2026-05-13 | **#868 / #1155** (PR #1157) | `_INVOKERS[JOB_SEC_DAILY_INDEX_RECONCILE]` at `runtime.py:442` + ScheduledJob at `scheduler.py:1554` (daily 04:00 UTC; #1181 carve-out exempts from universal-bootstrap gate) |
| G3 | Layer 3 per-CIK poll unwired | ✅ CLOSED 2026-05-13 | **#870 / #1155** (PR #1157) | `_INVOKERS[JOB_SEC_PER_CIK_POLL]` at `runtime.py:443` + ScheduledJob at `scheduler.py:1608` (hourly :00) |
| G4 | `sec_10q` parser | ✅ CLOSED 2026-05-14 | **#1168** | Synth no-op parser registered (sec-edgar §11.5.1). Owner-attribution to #414 was stale; #414 is the fundamentals_sync redesign, not a 10-Q parser ticket. |
| G5 | `sec_n_csr` parser feasibility | ✅ CLOSED 2026-05-14 | **#918** | Spike `docs/_archive/2026-05/spike-n-csr-feasibility.md` confirmed INFEASIBLE for v1; synth no-op landed (sec-edgar §11.5.1). N-CSR has zero per-holding identifier; fund holdings already covered by N-PORT-P. |
| G6 | `finra_short_interest` ingest | ✅ CLOSED — bimonthly 2026-05-18 (#915) + RegSHO daily 2026-05-18 (#916) | **#915 ✅ + #916 ✅** | Both portions landed via ScheduledJob + shared `finra` lane + module-global throttle. Bimonthly = `finra_short_interest_observations` + `_current` (23 quarterly partitions); daily = `finra_regsho_daily_observations` (25 quarterly partitions 2024-Q1 → 2030-Q1, no `_current`). Both manifest parsers are synth no-op (G7 precedent). |
| G7 | `sec_xbrl_facts` ManifestSource has no parser | ✅ CLOSED 2026-05-17 | — | Synth no-op parser registered (sec-edgar §11.5.1). XBRL facts continue to land via the Companyfacts bulk JSON path; the manifest row IS the audit signal. Pattern shares `sec_10q.py` (#1168). |
| G8 | `company_tickers_exchange.json` not consumed | ✅ CLOSED 2026-05-17 | **G8 PR** | Wired via `app/services/exchange_directory.py` + `sql/150` + Stage 7 in `daily_cik_refresh`. Empirical correction: cohort COUNT identical to `company_tickers.json` (10,353); the file is ticker-grain not CIK-grain (7,996 unique CIKs / 1,446 multi-ticker CIKs). Real value-add = `(ticker, exchange)` mapping for preferred series, ADR+OTC siblings, share-class siblings. In-scope fix to MF Stage 6 latent skip (now fires on every `daily_cik_refresh` invocation). |
| G9 | `company_tickers_mf.json` not consumed | ✅ CLOSED (predates audit) | **#1171 + #1174** | Stale audit entry corrected in same PR as G8. Consumed via `app/services/mf_directory.py::refresh_mf_directory` (Stage 6 sibling enrichment in `daily_cik_refresh`) + dedicated `mf_directory_sync` bootstrap stage (stage_order 26). Populates `cik_refresh_mf_directory` + `external_identifiers (sec, class_id)`. |
| G10 | `companyconcept` API not consumed | ✅ CLOSED 2026-05-18 | **PR #1198** (merge `0ead989`) | Provider primitive landed at `app/providers/implementations/sec_fundamentals.py::fetch_concept` + `extract_concept_facts`. No `fundamentals_sync` / `daily_financial_facts` wire-up by design (audit in spec §3.1 — under the 10 req/s shared SEC budget, companyconcept is wall-clock net-negative for any consumer needing ≥2 tags per CIK). Future single-tag consumer tickets re-open the wiring question. Future-consumer raw-payload invariant codified in spec §3.3. |
| G11 | `frames` API not consumed | ✅ CLOSED 2026-05-18 | **PR #1200** (merge `c954c50`) | Provider primitive landed at `app/providers/implementations/sec_fundamentals.py::fetch_frame`. No production consumer by design (open downstream ticket #594 does not specifically commit to frames; data-ingest design TBD). Re-open as a wiring ticket when #594's data-ingest scope settles. Future-consumer raw-payload invariant codified in spec §3.2. |
| G12 | Full-index `master.idx` quarterly not consumed | ✅ CLOSED 2026-05-17 | **PR #1196** (merge `e48eba3`) | Wired via `app/providers/implementations/sec_full_index.py` + `app/jobs/sec_master_idx_quarterly_sweep.py` + ScheduledJob `sec_master_idx_quarterly_sweep` (weekly Sun 05:15 UTC, source=`sec_rate`, prereq=`_bootstrap_complete`). Walks `[CQ, CQ-1]` per fire; per-quarter txn isolation; strict-by-default 404 (current quarter only tolerates 404); preloaded O(1) universe resolver. Invoker raises if any quarter fails so `job_runs.status='failure'` surfaces (Codex 2 pre-push HIGH). >1-quarter outage recovery via Python REPL runbook against the `quarters` kwarg. |
| G13 | `subjects_due_for_recheck` reader unused | ✅ CLOSED 2026-05-17 | **#1155** (sub-finding) + verification PR | Both readers drained per tick at `app/jobs/sec_per_cik_poll.py:343-354` with bounded 2/3+1/3 budget split. Static AST invariants at `tests/test_g13_recheck_reader_invariants.py` guard the wiring against future refactor; integration drain proved by `tests/test_sec_per_cik_poll.py::TestG13RecheckPath`. |

G1-G3 were the **headline finding** of the original 2026-05 audit — the freshness redesign's three steady-state polling layers were coded but never scheduled. #1155 / PR #1157 wired all three (2026-05-13); `data_freshness._CADENCE` is no longer a write-only ledger and the legacy per-form ingest crons have since been retired (see §3).

G8-G12 are low-priority — they don't block any operator-visible figure today. Open as tech-debt only if a metric ticket would benefit.

---

## 8. How to read this matrix

- "Are we covered for source X?" → §2 row for X. ✅ in last column = yes. ❌ = gap, follow ticket.
- "Where does endpoint URL Y come from?" → §4 row. File:line cites the consumer.
- "Why isn't endpoint Y firing on cadence?" → cross-check (a) §2/§4 has a `JOB_*` listed under steady-state; (b) `app/jobs/runtime.py` has `_INVOKERS[JOB_*]` registered; (c) `SCHEDULED_JOBS` has a `ScheduledJob(name=JOB_*, ...)`. All three must hold. The #1155 finding was exactly the case where (b) and (c) were missing (since fixed).
- "What's the rate-limit pool for fetcher Z?" → §6.
- "Adding a new source — what wiring do I need?" → §1 lists the five layers. New ManifestSource also needs §2 row + parser registration in `manifest_parsers/__init__.py`.

---

## 9. Cross-references

- `.claude/skills/data-sources/sec-edgar.md` — endpoint inventory authoritative source. §11.5 stranded-source map cross-checked.
- `.claude/skills/data-sources/edgartools.md` — library coverage matrix.
- `.claude/skills/data-engineer/SKILL.md` — schema invariants + per-source retention; §6.5 pipeline orchestration / lane concurrency.
- Spec: `docs/specs/etl/coverage-model.md` — original #863-#873 redesign.
- Memory: [[us-source-coverage]], [[873-manifest-worker-parser-rollout]], [[etl-freshness-redesign]].

---

## 10. Machine-readable form — tech debt

This matrix is **prose-Markdown**, which is great for humans but unparseable by a pre-push gate. Concrete gaps a CI gate would catch but currently can't:

- A new `ManifestSource` enum value added to `sec_manifest.py` without a §2 row.
- A `JOB_*` constant referenced in a §2 row but not registered in `_INVOKERS` or `SCHEDULED_JOBS`.
- A stage_key in `_BOOTSTRAP_STAGE_SPECS` without a §5 entry.
- A `_STAGE_LANE_OVERRIDES` entry pointing to a non-existent lane.

**Proposed shape:** sibling `coverage.yaml` next to this skill, schema validated, with one block per `(ManifestSource | reference_endpoint | bootstrap_stage)` covering the 5-layer wiring + status. Prose Markdown stays as the human-readable summary; the YAML is the lint source-of-truth.

Status: **NOT YET IMPLEMENTED** — open as a tech-debt issue when the next "stranded source" finding lands. Until then, audit drift catches gaps via human re-read every 2-4 weeks.
