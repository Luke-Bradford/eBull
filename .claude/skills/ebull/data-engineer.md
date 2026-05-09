# eBull data engineer — what we own, how we store it, how we read it

> Read this when adding a new ingest path, schema migration, or operator-visible read endpoint. The non-negotiable directive from the operator: future agents must use this file to answer "where does X come from?" without guessing. Cross-reference: `docs/settled-decisions.md` for the decisions; `docs/review-prevention-log.md` for the recurring traps; `.claude/skills/data-sources/sec-edgar.md` for the source-of-truth knowledge.

## 0. Top-of-mind invariants — the don't-break list

| # | Rule | Enforced by |
|---|---|---|
| I1 | API authn ≠ authz; instrument-scoped endpoints must validate operator/instrument scope | per-endpoint code; e.g. `app/api/instruments.py:34-47` (`require_session_or_service_token`) |
| I2 | All SQL is parameterised — `%(name)s` / positional, never f-strings for values | `docs/review-prevention-log.md:334` |
| I3 | Every parsed filing carries `parser_version` | `sec_filing_manifest.parser_version` (`sql/118:97-102`); keyed on accession not row |
| I4 | Every rollup slice carries `denominator_basis ∈ {pie_wedge, institution_subset}` | `OwnershipSlice.denominator_basis` (`app/services/ownership_rollup.py:68, 125`) |
| I5 | Funds slice excluded from residual + concentration math (memo overlay) | `_compute_residual` + `_compute_concentration` filters (`app/services/ownership_rollup.py:923-955`) |
| I6 | Soft-delete via tombstones, never hard-delete observations | `ownership_*_observations.known_to`, `*_ingest_log` rows, manifest `tombstoned` state |
| I7 | Atomic versioning for `_current` writes: `pg_advisory_xact_lock` per instrument; PK is second-line guard | `refresh_*_current()` functions in `app/services/ownership_observations.py` |
| I8 | Every observation write triggers `_current` refresh (write-through). Plus weekly backfill catches legacy gaps | call-site pattern in ingesters; backfill `JOB_OWNERSHIP_OBSERVATIONS_BACKFILL` (`app/workers/scheduler.py:710-735`) |
| I9 | `sec_filing_manifest` is source of truth for "is filing X on file?". Bumping `parser_version` flips rows back to `pending` for rewash | manifest state machine + `sec_rebuild` job |
| I10 | Identity resolution: instruments are `BIGINT instrument_id`; filers are `TEXT cik` (10-digit zero-padded). They never collapse | manifest `subject_type` CHECK; observations split by category |
| I11 | Coverage is telemetry, not a gate: red/amber/green banner state never blocks an endpoint | `_compute_coverage` / `_banner_for_state` (`app/services/ownership_rollup.py:966-1093`); `OwnershipRollup.no_data` returns 200 with empty payload |
| I12 | eToro broker is sole execution boundary; all order writes go via `app/services/order_client.py` and audit `decision_audit` | settled-decisions Provider strategy; `app/api/orders.py` |

Hard-rule corollaries:
- `INSERT INTO instruments` fixtures must supply `is_tradable` (prev-log L615).
- `_PLANNER_TABLES` in `tests/fixtures/ebull_test_db.py` must list every new FK-child table.
- Never close positions without explicit user action — `close_position()` only via UI or EXIT recommendation.
- Never `ON DELETE CASCADE` on `*_audit` / `*_log` (prev-log L350).

## 1. Schema layer — by domain

### 1.1 Identity & instrument core

| Table | Sql | Purpose |
|---|---|---|
| `instruments` | sql/001 | Canonical entity row. `instrument_id BIGINT`, symbol, company_name, exchange, currency, sector, `is_tradable` |
| `external_identifiers` | sql/003 | `(provider, identifier_type, identifier_value) → instrument_id`. Globally unique. SEC: `provider='sec', identifier_type='cik' / 'cusip'` |
| `exchanges` | sql/067 | eToro `exchangeId` → semantic class. CHECK on `asset_class IN (us_equity, crypto, eu_equity, uk_equity, asia_equity, commodity, fx, index, unknown)` |
| `instrument_sec_profile` | sql/051 | 1:1 SEC submissions metadata (cik, sic, former_names, has_insider_issuer). ON DELETE CASCADE from instruments |
| `instrument_cik_history` | sql/102 | CIK chain per instrument with date ranges. `btree_gist` exclusion forbids overlapping ranges; partial UNIQUE WHERE `effective_to IS NULL` |
| `instrument_symbol_history` | sql/103 | Symbol chain (FB→META, BBBY→BBBYQ). Same temporal-overlap GiST exclude |
| `unresolved_13f_cusips` | sql/099 | Buffer for 13F-supplied CUSIPs not yet resolved to instruments |

**No dedicated `cusip_map` table.** CUSIP→instrument lives in `external_identifiers WHERE provider='sec' AND identifier_type='cusip'`. Promotion path: 13F-HR ingest → unresolved → CUSIP backfill (#914 weekly) writes `external_identifiers` → `cusip_resolver.sweep_resolvable_unresolved_cusips` rewashes the source 13F (`app/services/cusip_resolver.py:425+`).

**Identity-graph cheat sheet**:
- Instrument = `BIGINT instrument_id` (eToro-derived).
- CIK = 10-digit zero-padded TEXT (SEC).
- Mapping: `external_identifiers.provider='sec', identifier_type='cik', is_primary=TRUE` is canonical resolver. Historical CIKs in `instrument_cik_history`.
- 13F filer CIKs ≠ issuer CIKs. Live in `institutional_filers.cik` (sql/090).
- N-PORT trust CIKs ≠ 13F manager CIKs. Live in `sec_nport_filer_directory.cik` (sql/126). Disjoint from `institutional_filers`. Codex finding on #919: walking `institutional_filers` for N-PORT was the root cause of empty fund holdings.
- 13D/G primary filers in `blockholder_filers.cik` (sql/095). Per-row `reporter_cik` nullable (natural persons / family trusts).

### 1.2 Ownership observations + current (sql/113 → sql/127)

**Two-layer model** (#840 Phase 1):
- **Layer 1 — observations**: immutable append-only fact log, partitioned `RANGE(period_end)` quarterly 2010-2030 + `_default` partition.
- **Layer 2 — `_current`**: mutable materialised dedup snapshot, rebuilt by `refresh_<category>_current(instrument_id)`.

**Five-axis category split** (six tables of each shape):

| Category | Subject | observations | _current | Source(s) | Identity (_current) |
|---|---|---|---|---|---|
| insiders | issuer | `ownership_insiders_observations` (sql/113) | `ownership_insiders_current` | form4, form3 | `(instrument_id, holder_identity_key, ownership_nature)` |
| institutions | issuer | `ownership_institutions_observations` (sql/114) | `ownership_institutions_current` | 13f | `(instrument_id, filer_cik, ownership_nature, exposure_kind)` |
| blockholders | issuer | `ownership_blockholders_observations` (sql/115) | `ownership_blockholders_current` | 13d, 13g | `(instrument_id, reporter_cik, ownership_nature)` |
| treasury | issuer | `ownership_treasury_observations` (sql/116) | `ownership_treasury_current` | xbrl_dei | `(instrument_id)` |
| def14a | issuer | `ownership_def14a_observations` (sql/116) | `ownership_def14a_current` | def14a | `(instrument_id, holder_name_key, ownership_nature)` |
| funds | issuer (via fund_series) | `ownership_funds_observations` (sql/123) | `ownership_funds_current` | nport (CHECK pinned) | `(instrument_id, fund_series_id)` |
| esop | issuer | `ownership_esop_observations` (sql/127) | `ownership_esop_current` | def14a (CHECK pinned) | `(instrument_id, plan_name)` |

**Provenance block** (uniform across every `ownership_*_observations`):

| Column | Type | Meaning |
|---|---|---|
| source | TEXT | CHECK list: `form4, form3, 13d, 13g, def14a, 13f, nport, ncsr, xbrl_dei, 10k_note, finra_si, derived` |
| source_document_id | TEXT | Per-row identifier inside accession (e.g. `accession#row_num`) |
| source_accession | TEXT | SEC accession |
| source_field | TEXT | Free-form provenance (XPath, JSON pointer) |
| source_url | TEXT | Direct link |
| filed_at | TIMESTAMPTZ | When SEC stamped acceptance |
| period_start / period_end | DATE | Period coverage; period_end is partition key |
| known_from / known_to | TIMESTAMPTZ | Valid-time. NULL `known_to` = currently valid; non-NULL = soft-delete |
| ingest_run_id | UUID | Correlation id |
| ingested_at | TIMESTAMPTZ | System-time. DEFAULT `clock_timestamp()` so each row in batch gets distinct stamp |

**Generated identity keys handle NULL CIKs**:
- `holder_identity_key` (sql/113:49-53) = `'CIK:'||trim(cik)` if non-NULL, else `'NAME:'||lower(trim(holder_name))`. Stops legacy NULL-CIK rows from collapsing.
- `holder_name_key` (sql/116:110) = `lower(trim(holder_name))` for DEF 14A.

**Cross-column CHECK invariants**:
- Blockholders: submission_type ↔ status_flag (`13D|13D/A → active`, `13G|13G/A → passive`).
- Funds: `payoff_profile='Long'`, `asset_category='EC'`, `shares > 0`.
- ESOP: `source='def14a'`, `ownership_nature='beneficial'`, `shares > 0`.
- Treasury: `ownership_nature` defaults `'economic'`.
- Funds & N-PORT: `fund_series_id ~ '^S[0-9]{9}$'` regex CHECK.

**Partitioning**:
- All parents `PARTITION BY RANGE (period_end)` quarterly 2010-2030 + `_default`.
- `_default` MUST stay empty post-backfill — pinned by `tests/test_ownership_observations.py::TestProvenanceBlockUniformity::test_default_partition_is_empty_post_backfill`.
- `ALTER TABLE` on parent propagates to existing partitions on Postgres 14+.

**Residual-exclusion semantics** (`denominator_basis`):
- `pie_wedge` slices (insiders, blockholders, institutions, etfs, def14a_unmatched) sum to ≤ `shares_outstanding` and contribute to residual + concentration.
- `institution_subset` slices (today: funds; future: ESOP / DRS / short-interest) render as memo overlays. Their shares are NOT subtracted from residual nor counted toward concentration. N-PORT rows are fund-level detail INSIDE the 13F-HR institutional aggregate — additive accounting would double-count.

### 1.3 SEC reference / discovery / coverage tables

| Table | Sql | Purpose |
|---|---|---|
| `sec_filing_manifest` | sql/118 | Single source of truth for "is accession X on file?" PK `accession_number`. State machine `pending/fetched/parsed/tombstoned/failed`. `parser_version` rewash trigger. Self-FK `amends_accession`. CHECK enforces `subject_type='issuer' ⇔ instrument_id IS NOT NULL` |
| `data_freshness_index` | sql/120 | "Should I poll subject Y for source Z?" Subject-polymorphic. PK `(subject_type, subject_id, source)`. State `unknown/current/expected_filing_overdue/never_filed/error`. Cadence map at `app/services/data_freshness.py:69-100` |
| `sec_reference_documents` | sql/121 | Per-quarterly raw SEC ref docs (e.g. 13F Securities List). PK `(document_kind, period_year, period_quarter)` |
| `sec_fund_series` | sql/124 | series_id → name + filer_cik. PK `fund_series_id` with `~ '^S[0-9]{9}$'` |
| `n_port_ingest_log` | sql/125 | Per-accession N-PORT ingest tombstone. PK `accession_number`. Re-record overwrites prior attempt |
| `sec_nport_filer_directory` | sql/126 | RIC trust CIKs. PK `cik`. Populated by `sec_nport_filer_directory_sync`. Sibling of `institutional_filers` (#912). Disjoint universes |
| `institutional_filers` | sql/090 | 13F-HR filer CIKs (managers). UNIQUE `cik`. CHECK `filer_type ∈ ETF/INV/INS/BD/OTHER` |
| `institutional_holdings` | sql/090 | Legacy per-(accession, instrument, is_put_call) 13F holdings. Partial UNIQUE on `(accession_number, instrument_id, COALESCE(is_put_call,'EQUITY'))` |
| `blockholder_filers` / `blockholder_filings` | sql/095 | 13D/G filer registry + per-reporter rows |
| `def14a_beneficial_holdings` / `def14a_ingest_log` | sql/097 | DEF 14A bene-table holdings + per-accession tombstone. UNIQUE `(accession_number, holder_name)` |
| `insider_transactions` | sql/056 | Form 4 per-transaction rows. UNIQUE `(accession_number, txn_row_num)` |
| `insider_initial_holdings` | sql/093 | Form 3 initial-holdings snapshots. UNIQUE `(accession_number, row_num)` |
| `filing_raw_documents` | sql/107 | Per-accession raw body store |
| `cik_raw_documents` | sql/109 | Per-CIK raw documents (e.g. submissions.json snapshots) |
| `financial_facts_raw` | sql/032 | Individual XBRL facts from companyfacts. UNIQUE `(instrument_id, concept, unit, COALESCE(period_start,'0001-01-01'), period_end, accession_number)` |
| `financial_periods_raw` / `financial_periods` | sql/032 | Wide period rows + canonical one-per-period. Canonical PK `(instrument_id, period_end_date, period_type)`. `superseded_at` for restatement chain |
| `data_ingestion_runs` | sql/032 | Audit trail per provider batch |

**Read-side views**:
- `instrument_share_count_latest` — DEI > us-gaap precedence over `financial_facts_raw` (sql/052). Drives ownership rollup denominator.
- `share_count_history`, `instrument_dilution_summary`, `dividend_history`, `instrument_dividend_summary` (sql/050 / sql/052).
- `financial_periods_ttm` (sql/032) — TTM = SUM last 4 quarters of flow + latest-quarter stock. Powers `instrument_valuation`.
- `instrument_valuation` (sql/032) — P/E, P/B, EV/EBITDA, dividend yield, FCF yield, debt/equity. `priced` CTE picks best price from `quotes`.

### 1.4 Market data / quotes / candles

| Table | Sql | Purpose |
|---|---|---|
| `price_daily` | sql/001 | OHLCV per `(instrument_id, price_date)` + TA columns |
| `quotes` | sql/002 | 1:1 current snapshot per instrument; overwritten each refresh; LEFT JOIN safe |
| `intraday_candles` | (not persisted) | Provider pass-through (eToro REST + TTL cache) |

### 1.5 Operator auth / broker secrets / runtime

| Table | Sql | Purpose |
|---|---|---|
| `operators` | sql/016 | One row in v1; identity anchor. UUID `operator_id` |
| `operator_sessions` | sql/016 | Cookie sessions for operator UI |
| `broker_credentials` | sql/018 | Encrypted broker secrets per operator+provider+label+environment. Partial UNIQUE `(operator_id, provider, label) WHERE NOT revoked`. Health-state added in sql/128 |
| `broker_credentials_audit` | sql/018 | Append-only audit. ON DELETE SET NULL on credential_id (forensic preservation) |
| `runtime_config` | sql/015 | DB-backed feature flags + kill_switch. Distinct from deployment config |
| `pending_job_requests` | sql/084 | Durable trigger queue. State `pending → claimed → dispatched → completed`. pg_notify is wakeup hint |
| `job_runs` | sql/014 | Per-run audit. Status `running/success/failure/skipped` |
| `bootstrap_state` | sql/129 | Singleton (id=1) — first-install gate. `pending/running/complete/partial_error` |
| `bootstrap_runs` | sql/129 | Per "Run bootstrap" click. Unique partial index forbids two concurrent `running` rows |
| `bootstrap_stages` | sql/129 | Per-stage detail; lane ∈ init/etoro/sec; status ∈ pending/running/success/error/skipped |
| `bootstrap_archive_results` | sql/130 | Per-(run, stage_key, archive_name) audit |
| `decision_audit` | sql/001 | One row per guard invocation; per-rule results in `evidence_json` |
| `tax_lots` | sql/001 | Tax lot ledger; matched fills via `reference_fill_id` |
| `cash_ledger` | sql/001 | `amount` sign: positive=inflow, negative=outflow |

### 1.6 Tombstone / soft-delete inventory

- `ownership_*_observations.known_to` — sets to NOW() on retraction.
- `ownership_*_observations` `_default` partition — itself a tombstone for unexpected `period_end`.
- `sec_filing_manifest.ingest_status='tombstoned'` (terminal under normal flow).
- `def14a_ingest_log`, `n_port_ingest_log`, `institutional_holdings_ingest_log`, `blockholder_filings_ingest_log` — per-accession attempted tombstones.
- `unresolved_13f_cusips.resolution_status` ∈ unresolvable/ambiguous/conflict/manual_review/resolved_via_extid.
- `broker_credentials.revoked_at`.
- `financial_periods.superseded_at` (restatement chain).

ON DELETE CASCADE used on: `instruments → instrument_sec_profile / sec_filing_manifest / data_freshness_index / instrument_*_history / ownership_esop_observations`. ON DELETE SET NULL used on `broker_credentials_audit.credential_id`.

## 2. Service layer

Service modules under `app/services/` (~96 modules). Canonical entry points by domain:

### 2.1 Ownership domain

| Module | Role | Entry points |
|---|---|---|
| [app/services/ownership_observations.py](../../../app/services/ownership_observations.py) | Two-layer write side: `record_*_observation()` + `refresh_*_current()` | record_insider:110, record_institution:292, record_blockholder:483, record_treasury:631, record_def14a:747, record_fund:913, record_esop:1104; refresh_*_current at 181/390/568/689/843/1040/1194 |
| [app/services/ownership_observations_sync.py](../../../app/services/ownership_observations_sync.py) | Legacy → observations backfill (`sync_all`); `JOB_OWNERSHIP_OBSERVATIONS_BACKFILL` weekly Sun 03:00 UTC | |
| [app/services/ownership_rollup.py](../../../app/services/ownership_rollup.py) | **Canonical Tier 0 read service**: `get_ownership_rollup(conn, symbol, instrument_id) -> OwnershipRollup` | line 1222 |
| [app/services/ownership_history.py](../../../app/services/ownership_history.py) | Time-series payload for chart | |
| [app/services/ownership_drillthrough.py](../../../app/services/ownership_drillthrough.py) | Per-slice drill detail | |
| [app/services/holder_name_resolver.py](../../../app/services/holder_name_resolver.py) | DEF 14A holder_name → filer_cik via `external_identifiers` | called from rollup `_enrich_and_union_def14a` |
| [app/services/cusip_resolver.py](../../../app/services/cusip_resolver.py) | Sweep `unresolved_13f_cusips` → promote to `external_identifiers` | `MATCH_THRESHOLD=0.92` |
| [app/services/rewash_filings.py](../../../app/services/rewash_filings.py) | Re-parse a previously-parsed filing under updated parser | called by manifest rebuild |
| [app/services/sec_manifest.py](../../../app/services/sec_manifest.py) | Manifest record + state-machine helpers | `record_manifest_entry`, `transition_status`, `iter_pending`, `iter_retryable` |
| [app/services/data_freshness.py](../../../app/services/data_freshness.py) | Scheduler API: `seed_scheduler_from_manifest`, `record_poll_outcome`, `subjects_due_for_poll` | |

### 2.2 Identity / discovery

`app/services/cik_discovery.py`, `sec_13f_filer_directory.py`, `sec_nport_filer_directory.py`, `sec_13f_securities_list.py`, `sec_entity_profile.py`, `cusip_resolver.py`, `holder_name_resolver.py`, `instrument_history.py`, `cik_raw_filings.py`, `filer_seed_verification.py`.

### 2.3 SEC / filings ingest

`sec_companyfacts_ingest.py`, `sec_insider_dataset_ingest.py`, `sec_13f_dataset_ingest.py`, `sec_nport_dataset_ingest.py`, `sec_submissions_ingest.py`, `sec_submissions_files_walk.py`, `sec_pipelined_fetcher.py`, `sec_bulk_download.py`, `sec_bulk_orchestrator_jobs.py`, `sec_filing_items.py`, `raw_filings.py`, `raw_persistence.py`, `filing_documents.py`, `eight_k_events.py`, `business_summary.py`, `dilution.py`.

### 2.4 Market data / etoro

`market_data.py`, `intraday_candles.py`, `quote_stream.py`, `etoro_lookups.py`, `etoro_websocket.py`, `exchanges.py`, `fx.py`.

### 2.5 Portfolio / execution / reporting

`portfolio.py`, `portfolio_sync.py`, `position_monitor.py`, `order_client.py`, `execution_guard.py`, `return_attribution.py`, `reporting.py`, `budget.py`, `transaction_cost.py`, `tax_ledger.py`.

### 2.6 Coverage / fundamentals

`coverage.py`, `fundamentals.py`, `fundamentals_observability.py`, `dividends.py`, `dividend_calendar.py`, `scoring.py`, `technical_analysis.py`, `entry_timing.py`, `thesis.py`, `news.py`, `sentiment.py`, `xbrl_derived_stats.py`.

### 2.7 Operator + auth + ops

`operators.py`, `operator_setup.py`, `broker_credentials.py`, `credential_health.py`, `credential_health_cache.py`, `runtime_config.py`, `ops_monitor.py`, `bootstrap_orchestrator.py`, `bootstrap_preconditions.py`, `bootstrap_state.py`.

### 2.8 Sync orchestrator

`sync_orchestrator/{adapters,cascade,content_predicates,dispatcher,exception_classifier,executor,freshness,layer_failure_history,layer_state,layer_types,planner,progress,reaper,registry,row_count_spikes,types}.py` — DAG-driven sync replacing 12 legacy crons (#260; merged 2026-04-16/17). `dispatcher.publish_manual_job_request` is the API → jobs-process boundary.

### 2.9 Read paths — canonical example

Ownership rollup read flow:
```
GET /instruments/{symbol}/ownership-rollup
  └─ app/api/instruments.py:4069
       resolve symbol → instrument_id (instruments + is_primary_listing tiebreaker)
       with snapshot_read(conn):                    # REPEATABLE READ snapshot
         get_ownership_rollup(conn, symbol, instrument_id)
            ├─ _read_shares_outstanding             # instrument_share_count_latest VIEW
            ├─ historical_symbols_for(conn, instrument_id)
            ├─ _read_treasury_from_current
            ├─ _collect_canonical_holders_from_current
            │      ├─ ownership_insiders_current  (form4/form3)
            │      ├─ ownership_blockholders_current  (13d/13g)
            │      └─ ownership_institutions_current  (13f, exposure_kind='EQUITY')
            ├─ _read_def14a_unmatched_from_current
            ├─ _enrich_and_union_def14a (resolve_holder_to_filer)
            ├─ _collect_funds_from_current → ownership_funds_current
            ├─ partition into block-only vs other (separate-pool dedup, #837)
            ├─ _dedup_by_priority(other_candidates) (form4>form3>13d/g>def14a>13f>nport)
            ├─ _dedup_within_source(block_candidates) (latest amendment per CIK)
            ├─ _bucket_into_slices                  # insiders, blockholders, institutions, etfs, def14a_unmatched, funds(memo)
            ├─ _compute_residual                    # pie_wedge slices only
            ├─ _compute_concentration               # pie_wedge slices only
            ├─ _read_universe_estimates             # Tier 0: all NULL → banner=unknown_universe
            ├─ _compute_coverage / _banner_for_state
            └─ return OwnershipRollup
       _rollup_to_response(rollup) → Pydantic OwnershipRollupResponse
```

**No-data path returns 200 with empty slices + red banner — never 503** (`OwnershipRollup.no_data` at line 207).

### 2.10 Write paths — canonical examples

**13F-HR ingest (institutions)**:
```
sec_13f_quarterly_sweep job (Sat 02:00 UTC)
  → sec_13f_dataset_ingest.ingest_filer_13f
      → for each accession:
           parse XML
           upsert institutional_filers
           per holding:
             resolve CUSIP → instrument_id (external_identifiers)
                ├─ resolved → INSERT institutional_holdings (legacy)
                │             record_institution_observation (write-through)
                │             refresh_institutions_current(instrument_id)
                └─ unresolved → unresolved_13f_cusips upsert
           record manifest row (sec_filing_manifest, ingest_status=parsed)
```

**DEF 14A ingest**:
```
sec_def14a_ingest job (daily 04:35 UTC) | sec_def14a_bootstrap (weekly Sun 02:30)
  → def14a_ingest._record_def14a_observations_for_filing
      → record_def14a_observation (also routes ESOP names via record_esop_observation)
      → refresh_def14a_current(instrument_id) + refresh_esop_current(instrument_id)
      → def14a_ingest_log row
```

**N-PORT ingest**:
```
sec_n_port_ingest (monthly day 22 03:00 UTC)
  → n_port_ingest.ingest_n_port_for_filer
      → walk sec_nport_filer_directory CIKs
      → for each accession not in n_port_ingest_log:
           parse NPORT-P (stdlib ElementTree)
           equity-common-Long filter
           upsert sec_fund_series
           record_fund_observation (write-through)
           refresh_funds_current(instrument_id)
           n_port_ingest_log row
```

**Write-through is wired in ingester service modules. There is no trigger-based write-through** — Postgres TRIGGERs only touch `updated_at` on manifest + freshness. Any new ingest path **must explicitly call** both `record_<cat>_observation` AND `refresh_<cat>_current(instrument_id)` plus update manifest. Forgetting either leaves `_current` empty (prev-log L1162).

**What write-through does NOT update**:
- Legacy typed tables (`institutional_holdings`, `def14a_beneficial_holdings`, `insider_transactions`, `insider_initial_holdings`, `blockholder_filings`) — still written by ingesters, NOT read by rollup post-#905. Survive for chart history + drift detection.
- `data_freshness_index` — separate write-side via `record_poll_outcome`.
- `sec_filing_manifest.parser_version` and `raw_status` — bumped by manifest worker / parser, not by `record_*_observation`.

### 2.11 Worker / job entry points

[app/workers/scheduler.py:453](../../../app/workers/scheduler.py#L453) — `SCHEDULED_JOBS` declaration. [app/jobs/](../../../app/jobs/) package owns runtime side:
- `__main__.py` boots singleton-fenced jobs process (advisory lock, `JOBS_PROCESS_LOCK_KEY`).
- `runtime.py` invokes.
- `listener.py` consumes `pending_job_requests` + pg_notify.

SEC-specific jobs:
- `sec_atom_fast_lane.py` — Atom feed (5 min cadence — fastest discovery layer).
- `sec_daily_index_reconcile.py` — daily-index reconciliation.
- `sec_per_cik_poll.py` — per-CIK submissions.json poller (cadence-driven from `data_freshness_index`).
- `sec_first_install_drain.py` — first-install drain (#871).
- `sec_manifest_worker.py` — pulls `pending` + `failed AND next_retry_at<=NOW()` from manifest, dispatches to parsers.
- `sec_rebuild.py` — `POST /jobs/sec_rebuild/run` (operator-triggered targeted re-ingest).
- `ownership_observations_repair.py` — daily 03:30 UTC drift sweep.

**Process topology** (settled-decisions §"Process topology #719"):
- FastAPI process (`app.main`): HTTP only.
- Jobs process (`python -m app.jobs`): APScheduler + manual-trigger executor + reaper + queue dispatcher + heartbeat.
- IPC: Postgres only — `pending_job_requests` rows + `pg_notify('ebull_job_request', ...)`. No HTTP/Redis/shared memory.
- Both processes use `app/db/pool.open_pool` (`sql_pool` config). Never instantiate raw `ConnectionPool(...)`.

## 3. Operator-visible API surface

### 3.1 `/instruments/...` endpoints ([app/api/instruments.py](../../../app/api/instruments.py))

| Endpoint | Tables / services |
|---|---|
| `GET /instruments` | instruments + quotes + coverage + external_identifiers |
| `GET /instruments/{symbol}/financials` | financial_periods + financial_periods_ttm + instrument_valuation |
| `GET /instruments/{symbol}/candles` | price_daily |
| `GET /instruments/{symbol}/intraday-candles` | provider pass-through (eToro REST + TTL cache) |
| `GET /instruments/{symbol}/sec_profile` | instrument_sec_profile |
| `GET /instruments/{symbol}/employees` | financial_facts_raw (`dei:EntityNumberOfEmployees`) |
| `GET /instruments/{symbol}/eight_k_filings` | eight_k_filings + eight_k_items + eight_k_exhibits |
| `GET /instruments/{symbol}/business_sections` | instrument_business_summary_sections |
| `GET /instruments/{symbol}/filings/10-k/history` | filing_events + instrument_business_summary |
| `GET /instruments/{symbol}/dilution` | instrument_dilution_summary view |
| `GET /instruments/{symbol}/dividends` | dividend_history view + dividend_events |
| `GET /instruments/{symbol}/insider_summary` | insider_transactions aggregated |
| `GET /instruments/{symbol}/insider_transactions` | insider_transactions |
| `GET /instruments/{symbol}/insider_baseline` | insider_initial_holdings + insider_transactions cumulative |
| `GET /instruments/{symbol}/def14a_holdings/drill` | def14a_beneficial_holdings + drift detection |
| `GET /instruments/{symbol}/summary` | composite summary panel |
| `GET /instruments/{symbol}/institutional-holdings` | institutional_holdings + institutional_filers |
| `GET /instruments/{symbol}/blockholders` | blockholder_filings + chain aggregator |
| `GET /instruments/{symbol}/ownership-history` | ownership_history service (time series) |
| `GET /instruments/{symbol}/ownership-rollup` | **Tier 0 canonical read** — `ownership_rollup.get_ownership_rollup` |
| `GET /instruments/{symbol}/ownership-rollup/export.csv` | `build_rollup_csv` |

### 3.2 `/system/...` ([app/api/system.py](../../../app/api/system.py))

- `GET /system/status` — operator dashboard. Layer freshness via `check_all_layers`, job health via `check_job_health`, kill-switch via `get_kill_switch_status`. Returns `overall_status ∈ ok/degraded/down`. **503 on infra failure** (never 200).
- `GET /system/jobs` — declared schedule + computed `next_run_time` + most recent `job_runs` row.

Auth: router-level `require_session_or_service_token`. Reveals data-pipeline gaps so must not be public.

### 3.3 `/jobs/...` ([app/api/jobs.py](../../../app/api/jobs.py))

- `POST /jobs/{job_name}/run` — durable-queue manual trigger. INSERTs `pending_job_requests` + `pg_notify`. 202 on accept; 404 on unknown job_name.
- `GET /jobs/runs` — recent `job_runs`.
- `GET /jobs/requests` — recent `pending_job_requests`.

Notable triggers: `POST /jobs/sec_rebuild/run`, `POST /jobs/ownership_observations_backfill/run`, `POST /jobs/sec_def14a_bootstrap/run`, `POST /jobs/sec_business_summary_bootstrap/run`, `POST /jobs/cusip_universe_backfill/run`.

### 3.4 Other operator routers

`/sync` (sync_orchestrator), `/system/bootstrap`, `/auth/*`, `/broker-credentials/*`, `/operators/*`, `/coverage/*`, `/recommendations/*`, `/orders/*`, `/portfolio/*`, `/scores/*`, `/theses/*`, `/audit/*`, `/news/*`, `/filings/*`, `/watchlist/*`, `/budget/*`, `/copy_trading/*`, `/alerts/*`, `/reports/*`, `/attribution/*`, `/config/*`. Operator ingest ops: `/operator/ingest-status`, `/ingest-failures`, `/ingest-backfill-queue`, `/ingest-backfill`.

### 3.5 Pattern for adding a new operator-visible figure

1. **Schema**: migration (next `sql/NNN`) creating table or VIEW. If observation-shaped, mirror provenance block + `record_*_observation` + `refresh_*_current` + partition strategy. Add `_PLANNER_TABLES` entry. Add CHECK constraints + Literal types on app side.
2. **Write side**: ingester in `app/services/<source>_ingest.py`. Persist raw payload first (rule L1168). Write to typed table + write-through to observations + refresh `_current`. Manifest state-transition. `data_freshness_index` poll outcome.
3. **Read side**: dedicated service in `app/services/<feature>.py` exposing `get_<thing>(conn, ...)` reading from `_current` snapshot inside `snapshot_read(conn)`. Return frozen dataclasses. Tag every slice with `denominator_basis` if it touches the rollup.
4. **API**: endpoint in `app/api/instruments.py` or sibling. Call inside `with snapshot_read(conn):`. Return Pydantic mirror. Empty/no-data paths return 200 with empty payload + state flag, never 503.
5. **Frontend**: shape in `frontend/src/api/<feature>.ts` + page component. Match denominator_basis logic on FE so charts agree with rollup CSV invariants.
6. **Cron / job**: `ScheduledJob` entry in `app/workers/scheduler.py`. Gate behind `_bootstrap_complete`.
7. **Tests**: shape uniformity + ingest path + read path + frontend snapshot.
8. **Operator runbook**: backfill via `/jobs/sec_rebuild/run` for parser-version bumps.

## 4. "Where does X come from?" — common-questions FAQ

### Q1. AAPL institutional ownership %
- Subject: issuer.
- Tables: `ownership_institutions_current WHERE instrument_id=AAPL.id AND exposure_kind='EQUITY'` SUM(shares) / `instrument_share_count_latest.latest_shares`.
- Service: `get_ownership_rollup` → slice `category='institutions'`.
- Endpoint: `GET /instruments/AAPL/ownership-rollup`.
- Caveat: AAPL real institutional % ~62%; pre-#841 universe expansion the dev DB number is much lower because `institutional_filers` only contains the 11k-row form.idx universe and CUSIP coverage is still being expanded (#914 weekly job).

### Q2. Who holds the most TSLA?
- Same rollup endpoint. Holders sorted by shares within each slice.
- Per-filer drill: `GET /instruments/TSLA/institutional-holdings` (reads `institutional_holdings` directly with filer joins).
- 13D/G activists: `GET /instruments/TSLA/blockholders`.

### Q3. When was the last 10-K for MSFT?
- `sec_filing_manifest WHERE instrument_id=MSFT AND form='10-K' ORDER BY filed_at DESC LIMIT 1`. Or `filing_events` (legacy) joined to `instrument_business_summary`.
- Service: `app/services/filings.py`.
- Endpoint: `GET /filings/{instrument_id}` and `GET /instruments/MSFT/filings/10-k/history`.

### Q4. Why does GME insider % look low?
- Cohen Form 4 reports DIRECT shares ~38M (`ownership_nature='direct'`).
- Cohen 13D/A reports BENEFICIAL shares ~75M (`ownership_nature='beneficial'`).
- Pre-#840 / pre-#788: priority chain `form4 > 13d/g` collapsed into single 38M, losing beneficial. Two-axis model + parallel 13D/G dedup pool is what makes both render today (`app/services/ownership_rollup.py:1260-1283`).
- If only ~38M shows: legacy table read path still in effect (shouldn't be post-#905) OR `ownership_blockholders_current` empty. Rebuild: `POST /jobs/sec_rebuild/run` with `{"instrument_id": <id>, "source": "sec_13d"}`.
- denominator_basis: insiders + blockholders both `pie_wedge`, so chart shows them as separate wedges, not merged.

### Q5. How fresh is the filings data?
- "Is filing X on file?" → `sec_filing_manifest WHERE accession_number=X`.
- "Should we have polled subject Y?" → `data_freshness_index WHERE subject_type=... AND subject_id=... AND source=...`. State + `expected_next_at` show cadence health.
- Last bootstrap: `bootstrap_state` (singleton) or `GET /system/bootstrap/status`.
- Per-job freshness: `GET /system/status` aggregates `check_all_layers`.
- Per-job last run: `job_runs WHERE job_name=...` or `GET /system/jobs` / `GET /jobs/runs?job_name=...`.
- SEC ingest backlog: `sec_filing_manifest WHERE ingest_status IN ('pending','failed') AND (next_retry_at IS NULL OR next_retry_at <= NOW())`.

### Q6. Which CIK does ticker X map to?
- Current: `external_identifiers WHERE provider='sec' AND identifier_type='cik' AND is_primary=TRUE AND instrument_id=X.id` OR `instrument_sec_profile.cik`.
- Historical: `instrument_cik_history WHERE instrument_id=X.id ORDER BY effective_from`.
- Symbol history (FB → META): `instrument_symbol_history` analogous.
- Reverse "this CIK is whose?": `external_identifiers WHERE provider='sec' AND identifier_type='cik' AND identifier_value='0000xxxxxxx'`.
- For 13F/13D/N-PORT FILERS (not issuers): `institutional_filers.cik`, `blockholder_filers.cik`, `sec_nport_filer_directory.cik` — those are filer CIKs, NOT issuer CIKs.
- Unresolved: `unresolved_13f_cusips`. Promotion runs weekly via `cusip_universe_backfill` + `cusip_extid_sweep`.

### Q7. Does AAPL have an N-PORT mutual-fund slice?
- `ownership_funds_current WHERE instrument_id=AAPL.id`. Source always `nport`. denominator_basis always `institution_subset` (memo overlay). Series identity = `fund_series_id` matched against `sec_fund_series`.
- Fund manager (RIC trust) CIK in `fund_filer_cik`. To find which manager: `sec_nport_filer_directory WHERE cik=fund_filer_cik`. NOT in `institutional_filers` (that's 13F managers, disjoint).
- Slice does NOT contribute to residual or concentration; visualises fund-level detail of holdings already counted via 13F-HR.

### Q8. Why is this DEF 14A holder showing in `def14a_unmatched`?
- DEF 14A bene-table holders carry `holder_name` only — no CIK on proxy itself. Match-to-CIK happens at rollup-read time via `holder_name_resolver.resolve_holder_to_filer`. If resolver returns `matched=False`, candidate lands in `def14a_unmatched`.
- Common causes: name normalisation gap (try cleaning the holder_name spelling), or holder genuinely has no Form 4 / Form 3 / 13F filing in the DB (typical for non-officer 5%+ holders).

### Q9. Where do I look at raw SEC payloads?
- Per-accession bodies: `filing_raw_documents` (sql/107). `raw_status` on manifest tracks whether body is stored.
- Per-CIK documents (e.g. submissions.json snapshot): `cik_raw_documents` (sql/109).
- Per-quarter SEC reference docs (13F Securities List): `sec_reference_documents` (sql/121).
- Filesystem `data/raw/**` retention: `raw_data_retention_sweep` (daily 02:00 UTC). Compaction state in `raw_persistence_state` (sql/038).

### Q10. How are observations dedup'd into _current?
- `refresh_<category>_current(conn, instrument_id=X)` — DELETE rows for instrument X then INSERT one row per `(holder_identity_key, ownership_nature, ...)` group.
- Winner ordering: source priority (`form4 > form3 > 13d > 13g > def14a > 13f > nport > ncsr`) → `period_end DESC` → `filed_at DESC` (amendments win) → `source_document_id ASC`.
- Wrapped in `pg_advisory_xact_lock(<hash of instrument_id>)` so concurrent refreshes serialise; PK on `_current` is second-line guard.
- Cross-source dedup ONLY within compatible natures — Cohen direct + Cohen beneficial both survive.

### Q11. Why might a chart show non-zero figure but CSV export sum 0?
- CSV emits two memo rows: `__treasury__` and `__residual__` for additive reconciliation `treasury + residual + Σ pie_wedge = shares_outstanding`.
- Memo-overlay slices (today: funds) emitted with `__memo:funds__` category prefix AFTER residual row. A spreadsheet `SUM(shares)` over the whole file is inflated. Filter to non-`__memo:` categories before summing. See `build_rollup_csv` (`app/services/ownership_rollup.py:1326-1444`).

### Q12. How do I trigger a re-ingest after a parser bump?
- Bump `parser_version` constant in parser code → manifest queries detect rows on older version → operator runs `POST /jobs/sec_rebuild/run` with `{"source": "sec_form4"}` (or scoped `{"instrument_id": N, "source": "sec_13f_hr"}`).
- Job flips manifest rows back to `pending`, manifest worker drains at 10 r/s shared. Monitor via `GET /jobs/sec_manifest_worker/status` (or count of `manifest WHERE ingest_status='pending' AND source='...'`).

### Q13. Where does shares_outstanding come from?
- `instrument_share_count_latest` view (sql/052) → DEI > us-gaap precedence on `financial_facts_raw`.
- Producing accession + form_type enriched via re-query in `_read_shares_outstanding` (`app/services/ownership_rollup.py:1138-1200`).
- EDGAR archive URL computed backend-side (not frontend) to prevent the wrong endpoint shape (Claude-PR-800 caught a `filenum=`-using FE URL bug).

### Q14. Where is the kill switch?
- `runtime_config` row keyed by name. Read by `get_kill_switch_status` (`app/services/ops_monitor.py`).
- Toggle: `POST /system/config/kill-switch` (`app/api/config.py:233`).

## 5. Quick reference — file/line index

**Schema**:
- Two-layer ownership: `sql/113` insiders, `sql/114` institutions, `sql/115` blockholders, `sql/116` treasury+def14a, `sql/119` ingested_at, `sql/123` funds, `sql/127` esop.
- SEC manifest + freshness: `sql/118`, `sql/120`, `sql/121`.
- Fund directory: `sql/124`, `sql/125`, `sql/126`.
- Identity: `sql/001`, `sql/003`, `sql/051`, `sql/067`, `sql/099`, `sql/102`, `sql/103`.
- Filings legacy: `sql/056`, `sql/090`, `sql/093`, `sql/095`, `sql/097`.
- Fundamentals: `sql/032`, `sql/050`, `sql/052`.
- Bootstrap: `sql/129`, `sql/130`, `sql/131`, `sql/132`.
- Ops + auth: `sql/014`, `sql/015`, `sql/016`, `sql/018`, `sql/084`, `sql/087`, `sql/128`.

**Services (canonical entry points)**:
- Read: `app/services/ownership_rollup.py:1222` (`get_ownership_rollup`).
- Write/refresh: `app/services/ownership_observations.py`.
- Manifest: `app/services/sec_manifest.py`.
- Freshness: `app/services/data_freshness.py`.
- CUSIP: `app/services/cusip_resolver.py`.
- Holder name: `app/services/holder_name_resolver.py`.

**Workers**:
- Schedule: `app/workers/scheduler.py:453` (`SCHEDULED_JOBS`).
- Jobs runtime: `app/jobs/__main__.py`, `app/jobs/runtime.py`, `app/jobs/listener.py`.
- Singleton fence: `app/jobs/locks.py` (`JOBS_PROCESS_LOCK_KEY`).
- DB pool: `app/db/pool.py:open_pool`.
- Snapshot read: `app/db/snapshot.py:snapshot_read`.

**API**:
- `app/api/instruments.py` — `/instruments`.
- `app/api/system.py` — `/system/status`, `/system/jobs`.
- `app/api/jobs.py` — `/jobs/{name}/run`, `/jobs/runs`, `/jobs/requests`.
- `app/api/bootstrap.py` — `/system/bootstrap/status`, `/system/bootstrap/run`, `/system/bootstrap/mark-complete` (router prefix `/system/bootstrap`).

**Specs**:
- `docs/superpowers/specs/2026-05-03-ownership-tier0-and-cik-history-design.md`
- `docs/superpowers/specs/2026-05-04-ownership-full-decomposition-design.md` (Phase 1 + 3)
- `docs/superpowers/specs/2026-05-04-etl-coverage-model.md` (manifest + freshness + 3-tier polling)
- `docs/superpowers/specs/2026-05-06-def14a-bene-table-extension-design.md` (#843 ESOP)
- `docs/superpowers/specs/2026-05-07-first-install-bootstrap.md` (#993)
- `docs/superpowers/specs/2026-05-08-bootstrap-etl-orchestration.md`

**Settled decisions**: `docs/settled-decisions.md`.
**Review prevention log**: `docs/review-prevention-log.md`.

## 6.5. Pipeline orchestration — invariants

> **State:** This section describes the **post-#1064 target state.** PR1 introduces source-level `JobLock` + `ParamMetadata` + `params_snapshot`; PR3 unifies the `bootstrap_state` gate across scheduled-fire and manual-trigger paths. Pre-PR1 reality: `JobLock` keys on `job_name`, scheduled jobs are zero-arg, and manual `/processes/{id}/trigger` bypasses prerequisites. The "Pre-PRn history" notes below mark each transition.
>
> Read before adding a new scheduled job, bootstrap stage, or operator-exposed parameter on an existing job. Cross-reference: [`docs/wiki/job-registry-audit.md`](../../../docs/wiki/job-registry-audit.md) for the per-job parameter surface; [`docs/superpowers/specs/2026-05-08-admin-control-hub-rewrite.md`](../../../docs/superpowers/specs/2026-05-08-admin-control-hub-rewrite.md) for the umbrella decisions.

### 6.5.1 Source-level concurrency (PR1 target state)

`JobLock` keys on `source` (the rate-bucket bound), NOT on `job_name`. Same-source jobs serialise under one lock; cross-source jobs run in parallel. Sources:

| Source | What it bounds | Lock contention |
|---|---|---|
| `init` | universe-sync only | One job total |
| `etoro` | eToro REST budget | execute_approved_orders + candle_refresh + lookups serialise |
| `sec_rate` | SEC 10 req/s shared per-IP bucket | Every per-CIK fetch + every per-accession fetch competes here |
| `sec_bulk_download` | Fixed-URL SEC archive downloads | Disjoint from `sec_rate` — large fixed downloads, no per-issuer iteration |
| `db` | DB-bound bulk ingest of pre-staged data | Same-source jobs serialise under the source lock |

The reason this matters: SEC's 10 req/s is per-IP, not per-job. Two SEC jobs running concurrently DO compete for the same bucket — serialising them at the lock layer is the right model. But SEC's bulk-download endpoints (`sec_bulk_download` source) have a separate budget — a `cusip_universe_backfill` (12k rows of CUSIP map data) does NOT compete with a `daily_cik_refresh` (per-CIK submissions polls), so they should run concurrently. Per-job locking would conflate them.

Within-source same-`job_name` semantics: triggering `sec_def14a_ingest` twice with different `params` **still serialises** under the source lock — the second invocation waits for the first to release. Per-param-set lock identity (one lock per `(job_name, params_hash)`) is deferred to v2.

**Bootstrap dispatcher concurrency vs JobLock — separate mechanisms.** The bootstrap orchestrator's `_LANE_MAX_CONCURRENCY` map ([`bootstrap_orchestrator.py:98`](../../../app/services/bootstrap_orchestrator.py#L98)) historically allowed up to 5 parallel `db`-lane stages WITHIN a single bootstrap run. That is a dispatcher knob, not a `JobLock` semantic. Under PR1 source-level locking, same-source jobs serialise across the entire process — the dispatcher's lane-concurrency map is either retired or reinterpreted as a "max queued before the lock kicks in" hint. The locked decision is unambiguous: same-source = serialised at the lock; the lane-concurrency map does not override this.

**`sec_rate` starvation risk.** A 1-hour `sec_def14a_bootstrap` drain holding the `sec_rate` lock will skip every hourly SEC ingest fired during that window (`sec_form4`, `sec_filing_documents_ingest`, `sec_8k_events_ingest`). Weekly bounded — acceptable. Repeated manual drains chain together to multiple-hour starvation — operator visibility required. Admin process table must surface "skipped — `sec_rate` source lock held by `sec_def14a_bootstrap`" with the holder identified, otherwise the operator sees mysterious skips during a drain and triggers it again, deepening the starvation. This goes in PR1's REASON_TOOLTIP map under `lock_held_by_other_source_member`.

### 6.5.2 ParamMetadata discipline

Every job in `SCHEDULED_JOBS` declares a tuple of `ParamMetadata` describing its operator-exposable parameter surface:

```python
@dataclass(frozen=True)
class ParamMetadata:
    name: str
    label: str
    help_text: str
    field_type: Literal["string", "int", "float", "date", "quarter", "ticker", "cik", "bool", "enum", "multi_enum"]
    default: Any | None
    advanced_group: bool
    enum_values: tuple[str, ...] | None = None
```

The Pydantic mirror in [`frontend/src/api/types.ts`](../../../frontend/src/api/types.ts) is the API contract. **Drift between the BE model and the FE types is a PREVENTION-log-grade risk** — the FE renders generic Advanced disclosure fields off this metadata; if the contract drifts, operators see the wrong inputs or no inputs at all. Round-trip tests cover one job (canonical) every PR; full-mirror coverage is bot-enforced via `frontend/src/api/types.ts` review.

Field types collapse to ~10 archetypes — see [`job-registry-audit.md` §6](../../../docs/wiki/job-registry-audit.md). Ticker / CIK fields render as typeaheads resolving to internal IDs operator-side; the BE receives the resolved `int` / `str`.

### 6.5.3 Bootstrap orchestration is parameter overrides — not bespoke wrappers

A bootstrap stage is `(stage_key, stage_order, lane, job_name, params dict)`. The orchestrator passes `params` to the registered callable; the callable validates against its `ParamMetadata`. There are NO separate bootstrap-only callables.

Pre-PR1 history: [`bootstrap_orchestrator.py`](../../../app/services/bootstrap_orchestrator.py) carried three bespoke wrappers (`bootstrap_filings_history_seed`, `sec_first_install_drain_job`, `bootstrap_sec_13f_recent_sweep_job`) that re-implemented the dispatch + `_tracked_job` wrapping just to override default params. PR1 lifts the shared workflow into the lower-level helpers (`refresh_filings`, `run_first_install_drain`, `ingest_all_active_filers`); bootstrap stages become data, not code; bespoke wrapper files disappear.

Practical consequence: any job invokable from bootstrap is ALSO invokable post-bootstrap from the admin process table with the same param surface. Operators never get stuck in a "this only runs during bootstrap" UX trap.

### 6.5.4 `bootstrap_state.status` as universal gate

Both scheduled-fire AND manual-trigger paths check `bootstrap_state.status='complete'` before running any gated job. `partial_error` is NOT-complete: scheduled fires reject, manual triggers reject by default. Manual remediation requires explicit `?override_bootstrap_gate=true` and writes a `decision_audit` row.

Pre-PR3 history: scheduled-fire honoured `_bootstrap_complete` prerequisite; manual `/processes/{id}/trigger` bypassed it. PR3 collapses both paths through `_check_bootstrap_state_gate(conn, *, allow_manual_remediation=...)`.

The 409 surfaces with reason `bootstrap_not_complete` and copy "Bootstrap is not complete. Finish first-install before triggering scheduled jobs." in [`frontend/src/components/admin/processStatus.ts::REASON_TOOLTIP`](../../../frontend/src/components/admin/processStatus.ts).

### 6.5.5 `job_runs.params_snapshot JSONB`

Every job_runs row records the params dict that produced it. Manual triggers write the operator-supplied dict; scheduled fires write the registry default + cadence-derived overrides. Operator history visibility — clicking a row in the admin process table reveals "this run used `chunk_limit=200, since=2026-01-01`" rather than just "ran for 47s".

Default `'{}'::jsonb` for jobs with no operator-exposable params.

This is the audit trail for "why did this run pull only 200 rows instead of the usual 500?" — answer is in `params_snapshot`. The pre-PR1 path lost this signal.

### 6.5.6 Prerequisite enforcement is unified

Every job in `SCHEDULED_JOBS` declares a `prerequisite: PrerequisiteFn | None`. Scheduled fire and manual trigger both call the same prereq through `_check_bootstrap_state_gate`. There is no manual-bypass shortcut. If the operator needs to fire a job whose prereq is unmet (e.g. `_has_actionable_recommendations` is False but they want to dry-run), the path is `?override_bootstrap_gate=true` (or future `?override_prerequisite=true`) with audit row, NOT a separate untracked code path.

### 6.5.7 Discipline summary — when adding a new scheduled job

1. **Pick a source.** Match the rate-bucket reality. Don't invent a new source unless the budget really is disjoint.
2. **Declare `ParamMetadata`.** Even if empty tuple. The decl forces the question "what should the operator be able to override?". Reject implementation-strategy knobs (`prefetch_urls`, `follow_pagination`, `use_bulk_zip`) and provenance labels (`source_label`, `match_threshold` outside narrow bounds) — those belong in code review, not Advanced disclosure. The audit `docs/wiki/job-registry-audit.md` §6 has the canonical exposable-vs-internal split.
3. **Mirror in `frontend/src/api/types.ts`.** Same PR. Drift = bug.
4. **Wire `prerequisite` if applicable.** `_bootstrap_complete` is the most common; compose with `_all_of(...)` for additional gates.
5. **Decide `catch_up_on_boot`.** Rule of thumb: TRUE for cheap idempotent reads (lookups, classification refreshes); FALSE for anything that hits a rate budget or holds DB workers.
6. **Audit row schema.** `_tracked_job(JOB_NAME)` writes the `job_runs` row; ensure the constant is in `__all__` so the registry-shape test catches drift.
7. **Test invariants.** Every job needs a registry-shape test (`tests/test_job_registry.py`) covering source non-NULL + params_metadata validates + prerequisite is callable.
8. **Queue/audit terminal-state correctness.** When the prelude (source lock acquisition, prereq check, `bootstrap_state` gate, fence-held opt-out) skips the body without invoking the underlying job, the corresponding `pending_job_requests` row MUST transition to `rejected` (not `completed`) with a structured `error_msg`. `mark_request_completed` after a skipped body produces an audit row that says "ran successfully" when the job never ran. PREVENTION-log-grade hazard — see PR #1072 BLOCKING and PR #1078 (orchestrator opt-out fence). The skipped/rejected vs completed distinction is the operator's only signal that a manual trigger didn't actually do what they asked. Grep `mark_request_completed` and verify each call site has the matching `mark_request_rejected` for the prelude-skip path.

## 6. Known live caveats / tech debt

- **Coverage = telemetry not gate**: per-category universe estimates still NULL for Tier 0 (`_read_universe_estimates` returns all-None). Banner reports `unknown_universe` on most instruments. Real estimates seeded in #790 / Batch 2.
- **AAPL institutional %**: under-reported on dev DB until universe-expansion sweep finishes. Operator audit 2026-05-04.
- **Funds slice coverage**: only 2020-CIK panel harvested before #963 directory walker. Sweep 2026-05-05 finished filling trust universe but per-CIK drain gated on monthly N-PORT job. For panel verification today, use `POST /jobs/sec_n_port_ingest/run`. **Workaround scripts at `.claude/*.py` are a tell that the standing job is broken — fix the job, don't extend the workaround.**
- **CI pytest job dropped (#928)**: pre-push hook is sole test gate.
- **AS-OF semantics**: `as_of_date` everywhere = period end, never fetch time. `ingested_at` is system-time watermark for repair sweep. `known_from`/`known_to` are valid-time. Don't mix.
- **N-PORT validation cliff (#932)**: EdgarTools' Pydantic `FundReport.parse_fund_xml` rejects synthetic test fixtures the bespoke parser tolerates. Bespoke stdlib-ElementTree parser remains shipped; rewrite parked.
