# Job registry audit (PR0 — #1064 follow-up)

> Research output of PR0 in the admin control hub follow-up sequence. Single source of truth for every scheduled job and bootstrap stage's *current* parameter surface, source/lane, cadence, prerequisites, and the operator-visible vs internal split. Inputs to PR1 (job registry refactor: `ParamMetadata` + source-level `JobLock` + `params_snapshot`), PR2 (FE Advanced disclosure renderer), and PR4 (#1082 `display_name` + `description` + ⓘ tooltips).
>
> Reality check: every entry in [`SCHEDULED_JOBS`](../../app/workers/scheduler.py#L453) is a zero-arg `def foo() -> None:` body today. Operator-tunable parameters live as hardcoded constants inside the lower-level service helpers each body calls, or as `settings.*` values pulled at run time. PR1 lifts each surface into a per-job `ParamMetadata` declaration so the operator can override at trigger time.

---

## 1. Source / lane vocabulary

Locked decision in [`bootstrap_orchestrator.py:_LANE_MAX_CONCURRENCY`](../../app/services/bootstrap_orchestrator.py#L98) and [`_STAGE_LANE_OVERRIDES`](../../app/services/bootstrap_orchestrator.py#L164):

| Lane key | Concurrency | What it gates | Notes |
|---|---|---|---|
| `init` | 1 | universe-sync only | Pre-everything fence |
| `etoro` | 1 | eToro REST budget | Separate from SEC budgets |
| `sec_rate` | 1 | SEC 10 req/s shared bucket (per-IP) | Every per-CIK / per-accession SEC fetch |
| `sec_bulk_download` | 1 | Fixed-URL SEC archive downloads | Disjoint budget — large fixed downloads, no per-issuer iteration |
| `db` | 5 | DB-only ingest of pre-staged bulk data | Parallel-safe (no SEC HTTP, only psycopg I/O) |

Source-level `JobLock` (PR1): same-source jobs serialise; cross-source run parallel. SEC bulk download + per-CIK fetches do NOT compete (different rate budgets). DB-bound bulk ingesters don't block per-CIK polls. This is the rate-bucket reality formalised.

---

## 2. SCHEDULED_JOBS audit (27 entries)

For every entry: registry `name`, proposed `display_name`, proposed `description` (1-2 sentences), source/lane, cadence, prerequisite, current param surface (read from function body), proposed PR1 operator-exposable params + defaults.

### 2.1 Orchestrator triggers

#### `orchestrator_full_sync`
- **Display name:** Orchestrator full sync
- **Description:** Walks the cross-domain DAG and refreshes every stale layer in topological order. Replaces the 12 retired single-cron jobs (#260 Phase 4).
- **Source:** `db` (orchestrator coordinates; downstream layers call into their own sources)
- **Cadence:** daily 03:00 UTC; `catch_up_on_boot=False`
- **Prerequisite:** `_bootstrap_complete`
- **Current params (body):** zero-arg. Calls `run_full_sync()` with no scope filters.
- **Proposed PR1 params:**
  - `since: date | None` (advanced) — re-plan layers stale since this date instead of full DAG.
  - `force_full: bool = False` (advanced) — bypass freshness gates and re-fetch every layer.
  - `layer_allowlist: list[str] | None` (advanced) — restrict the walk to a layer subset.

#### `orchestrator_high_frequency_sync`
- **Display name:** Orchestrator high-frequency sync
- **Description:** 5-min refresh of independent high-frequency layers (`portfolio_sync` + `fx_rates`). Disjoint from the daily full DAG.
- **Source:** `db`
- **Cadence:** every 5 minutes; `catch_up_on_boot=False`
- **Prerequisite:** none
- **Current params:** zero-arg.
- **Proposed PR1 params:** none operator-exposable. Internal-only.

### 2.2 Outside-DAG operational jobs

#### `execute_approved_orders`
- **Display name:** Execute approved orders
- **Description:** Run the execution guard against actionable trade recommendations and submit approved orders to eToro.
- **Source:** `etoro`
- **Cadence:** daily 06:30 UTC; `catch_up_on_boot=False` (orders never fire as a surprise catch-up)
- **Prerequisite:** `_has_actionable_recommendations`
- **Current params:** zero-arg.
- **Proposed PR1 params:**
  - `dry_run: bool = False` (advanced) — guard-and-log without submitting orders.

#### `retry_deferred_recommendations`
- **Display name:** Retry deferred recommendations
- **Description:** Re-evaluate `timing_deferred` BUY/ADD recommendations against fresh TA + price data; promote any that re-qualify.
- **Source:** `db`
- **Cadence:** hourly :30; `catch_up_on_boot=False`
- **Prerequisite:** `_has_deferred_recommendations`
- **Current params:** zero-arg.
- **Proposed PR1 params:** none operator-exposable.

#### `monitor_positions`
- **Display name:** Monitor open positions
- **Description:** Check open positions for SL/TP breaches, thesis breaks, and forced-exit conditions.
- **Source:** `db`
- **Cadence:** hourly :15; `catch_up_on_boot=False`
- **Prerequisite:** `_has_open_positions`
- **Current params:** zero-arg.
- **Proposed PR1 params:** none operator-exposable.

### 2.3 Fundamentals + research

#### `fundamentals_sync`
- **Display name:** Fundamentals research refresh
- **Description:** Re-classify every tradable instrument's `coverage.filings_status`, backfill eligible instruments via SEC EDGAR, then re-evaluate coverage tier promote/demote rules. Collapses the legacy weekly_coverage_audit + weekly_coverage_review pair (2026-04-19 refocus).
- **Source:** `db` (matches the StageSpec.lane in [`bootstrap_orchestrator.py:247`](../../app/services/bootstrap_orchestrator.py#L247) — body is hybrid DB/SEC, but the SEC fetches go through individual provider calls inside the per-CIK loop. PR1 may need to revisit if the rate-budget contention becomes operator-visible — flagged tech-debt candidate)
- **Cadence:** daily 02:30 UTC; `catch_up_on_boot=False`
- **Prerequisite:** `_all_of(_bootstrap_complete, _has_any_coverage)`
- **Current params:** zero-arg.
- **Proposed PR1 params:**
  - `instrument_id: int | None` (primary) — restrict to a single instrument (operator triage path).
  - `since: date | None` (advanced) — re-classify only instruments with filings since this date.
  - `force_full: bool = False` (advanced) — bypass watermark; re-process every covered CIK.

### 2.4 SEC daily ingesters — `sec_rate` lane

These follow the same archetype: zero-arg body → calls `ingest_<thing>(conn, provider)` with hardcoded `chunk_limit` constants. Every one is a candidate for the same `instrument_id` / `since` / `chunk_limit` / `force_full` PR1 surface.

| Job | Display name | Cadence | Prereq | Body calls | Current internal limits |
|---|---|---|---|---|---|
| `sec_dividend_calendar_ingest` | SEC dividend calendar ingest | daily 03:00 | `_bootstrap_complete` | `ingest_dividend_calendar(conn, provider)` | bounded 500/run |
| `sec_business_summary_ingest` | SEC 10-K business-summary ingest | daily 03:15 | `_bootstrap_complete` | `ingest_business_summaries(conn, provider)` | bounded 200/run, 7d TTL |
| `sec_insider_transactions_ingest` | SEC Form 4 ingest | hourly :30 | `_bootstrap_complete` | `ingest_insider_transactions(conn, provider)` | bounded 500/run |
| `sec_filing_documents_ingest` | SEC filing-documents manifest ingest | hourly :35 | `_bootstrap_complete` | `ingest_filing_documents(conn, provider)` | bounded 500/run |
| `sec_8k_events_ingest` | SEC 8-K events ingest | hourly :20 | `_bootstrap_complete` | `ingest_8k_events(conn, provider)` | bounded 200/run |
| `sec_form3_ingest` | SEC Form 3 ingest | daily 04:20 | `_bootstrap_complete` | `ingest_form_3_filings(conn, provider)` | bounded |
| `sec_def14a_ingest` | SEC DEF 14A ingest | daily 04:35 | `_bootstrap_complete` | `ingest_def14a(conn, provider)` | bounded 100/run |

**Proposed PR1 surface (per job):**
- `instrument_id: int | None` (primary) — single-instrument targeting.
- `since: date | None` (advanced) — process accessions filed-at >= this date.
- `chunk_limit: int | None` (advanced) — override the hardcoded per-run cap; default = current constant.
- `force_full: bool = False` (advanced) — bypass any TTL / watermark filter.

### 2.5 SEC bootstrap drains — `sec_rate` lane

#### `sec_business_summary_bootstrap`
- **Display name:** SEC business-summary bootstrap drain
- **Description:** One-shot drain of the 10-K Item 1 candidate set. Loops the standard ingester at `chunk_limit=500` until the queue empties or the 1h deadline elapses (#535).
- **Source:** `sec_rate`
- **Cadence:** weekly Sun 04:00 UTC; `catch_up_on_boot=False`
- **Prerequisite:** `_bootstrap_complete`
- **Current params (body):** zero-arg → `bootstrap_business_summaries(conn, provider)` with internal `chunk_limit=500`, deadline ≈ 1h.
- **Proposed PR1 params:** `chunk_limit`, `deadline_seconds`, `instrument_id`, `force_full` (all advanced).
- **Bootstrap orchestrator invocation:** none direct (operator-only).

#### `sec_def14a_bootstrap`
- **Display name:** SEC DEF 14A bootstrap drain
- **Description:** One-shot drain of the DEF 14A candidate set. Loops `bootstrap_def14a` at `chunk_limit=500` with `prefetch_urls=True` until the queue empties or the 1h deadline elapses (#839).
- **Source:** `sec_rate`
- **Cadence:** weekly Sun 02:30 UTC; `catch_up_on_boot=False`
- **Prerequisite:** `_bootstrap_complete`
- **Current params (body):** zero-arg → `bootstrap_def14a(conn, provider, prefetch_urls=True, prefetch_user_agent=settings.sec_user_agent)`.
- **Proposed PR1 params:** `chunk_limit`, `deadline_seconds`, `instrument_id` (all advanced). `prefetch_urls` deliberately NOT exposed — implementation strategy knob, not an operator decision.

#### `sec_insider_transactions_backfill`
- **Display name:** SEC Form 4 round-robin backfill
- **Description:** Round-robin Form 4 backfill for instruments with deep historical backlogs. Picks the 25 instruments with the most pending Form 4 candidates and drains 50/instrument per run, oldest-first (#456).
- **Source:** `sec_rate`
- **Cadence:** hourly :45; `catch_up_on_boot=False`
- **Prerequisite:** `_bootstrap_complete`
- **Current params (body):** zero-arg → `backfill_insider_transactions(conn, provider)` with internal `instruments_per_run=25`, `filings_per_instrument=50`.
- **Proposed PR1 params:** `instruments_per_run`, `filings_per_instrument`, `instrument_id` (advanced).

### 2.6 Ownership repair + backfill — `db` lane

#### `ownership_observations_sync`
- **Display name:** Ownership repair sweep
- **Description:** Self-healing drift sweep — scans for `(instrument, category)` pairs where `_current.refreshed_at` is staler than `max(observations.ingested_at)` and refreshes the drifted instruments. Zero rows on a healthy install.
- **Source:** `db`
- **Cadence:** daily 03:30 UTC; `catch_up_on_boot=True`
- **Prerequisite:** `_bootstrap_complete`
- **Current params (body):** zero-arg → `run_observations_repair_sweep(conn)`.
- **Proposed PR1 params:** `instrument_id` (primary), `category` (advanced; enum: insiders/institutions/blockholders/treasury/def14a/funds/esop).

#### `ownership_observations_backfill`
- **Display name:** Legacy → observations backfill
- **Description:** One-shot legacy → ownership_*_observations backfill. Mirrors historical rows from typed legacy tables into the observations + `_current` model. Idempotent on natural keys (#909).
- **Source:** `db`
- **Cadence:** weekly Sun 03:00 UTC; `catch_up_on_boot=False`
- **Prerequisite:** none
- **Current params (body):** zero-arg → `sync_all(conn, since=None, limit=None)`.
- **Proposed PR1 params:** `since: date | None` (advanced), `limit: int | None` (advanced), `category` (advanced enum).

### 2.7 SEC universe / filer-directory jobs

#### `cusip_extid_sweep`
- **Display name:** CUSIP rewash sweep
- **Description:** Promote `unresolved_13f_cusips` rows whose CUSIP now matches an `external_identifiers` row, then rewash the source 13F-HR so the previously stranded holdings land in `institutional_holdings` (#836).
- **Source:** `db`
- **Cadence:** daily 04:50 UTC; `catch_up_on_boot=True`
- **Prerequisite:** none
- **Current params (body):** zero-arg → `sweep_resolvable_unresolved_cusips(conn)` with internal `LIMIT 1000` per pass.
- **Proposed PR1 params:** `limit` (advanced), `cusip_filter` (advanced).

#### `cusip_universe_backfill`
- **Display name:** CUSIP universe backfill
- **Description:** Quarterly walk of SEC's Official 13(f) Securities List → fuzzy-match each row against `instruments.company_name` (threshold 0.92) → INSERT confident matches into `external_identifiers`. Post-batch sweeps `unresolved_13f_cusips` (#914).
- **Source:** `sec_rate` (matches [`_STAGE_LANE_OVERRIDES`](../../app/services/bootstrap_orchestrator.py#L164) — fetches the official 13(f) list via SEC HTTP rate-limited path, not bulk archive)
- **Cadence:** weekly Sun 05:00 UTC; `catch_up_on_boot=True`
- **Prerequisite:** none
- **Current params (body):** zero-arg → `run_cusip_universe_backfill(conn)` with internal threshold=0.92.
- **Proposed PR1 params:** `quarter: str | None` (advanced; format `YYYY[Q1-4]`). `match_threshold` deliberately NOT exposed — fuzzy-matching cliff is a data-integrity hazard the operator should not tune day-to-day; threshold tweaks belong in code review, not Advanced disclosure.

#### `sec_13f_filer_directory_sync`
- **Display name:** 13F filer-directory sync
- **Description:** Discovery sweep of SEC's quarterly form.idx for every active 13F-HR / 13F-HR/A / 13F-NT filer CIK. UPSERTs into `institutional_filers` (#912). Idempotent.
- **Source:** `sec_rate` (matches [`_STAGE_LANE_OVERRIDES`](../../app/services/bootstrap_orchestrator.py#L164) — form.idx fetches go through the rate-limited SEC client, not the bulk-archive path)
- **Cadence:** weekly Sun 04:15 UTC; `catch_up_on_boot=False`
- **Prerequisite:** none
- **Current params (body):** zero-arg → `sync_filer_directory(conn)` with internal quarters_back=4.
- **Proposed PR1 params:** `quarters_back: int = 4` (advanced).

#### `sec_nport_filer_directory_sync`
- **Display name:** N-PORT filer-directory sync
- **Description:** Sibling of the 13F directory sync but for the disjoint RIC trust-CIK universe. UPSERTs into `sec_nport_filer_directory` (#963).
- **Source:** `sec_rate` (matches [`_STAGE_LANE_OVERRIDES`](../../app/services/bootstrap_orchestrator.py#L164))
- **Cadence:** weekly Sun 04:20 UTC; `catch_up_on_boot=False`
- **Prerequisite:** none
- **Current params (body):** zero-arg → `sync_nport_filer_directory(conn)` with internal quarters_back=4.
- **Proposed PR1 params:** `quarters_back: int = 4` (advanced).

#### `sec_13f_quarterly_sweep`
- **Display name:** 13F quarterly holdings sweep
- **Description:** Walk every CIK in `institutional_filers` and ingest each filer's pending 13F-HR / 13F-HR/A accessions through `ingest_filer_13f`. Soft 6h deadline; resumable via `institutional_holdings_ingest_log` tombstones (#913).
- **Source:** `sec_rate`
- **Cadence:** weekly Sat 02:00 UTC; `catch_up_on_boot=False`
- **Prerequisite:** `_bootstrap_complete`
- **Current params (body):** zero-arg → `ingest_all_active_filers(conn, sec, ciks=list_directory_filer_ciks(conn), deadline_seconds=settings.sec_13f_sweep_deadline_seconds, source_label="sec_edgar_13f_directory")`.
- **Proposed PR1 params:**
  - `cik: str | None` (primary; ticker typeahead resolves to filer CIK in advanced edit-mode).
  - `deadline_seconds: int | None` (advanced) — override the settings default.
  - `min_period_of_report: date | None` (advanced) — recency filter; matches the bootstrap variant's cutoff knob.
  - `source_label` deliberately NOT exposed — provenance/audit semantics. The bootstrap variant's `sec_edgar_13f_directory_bootstrap` value lives in the bootstrap stage's hardcoded params dict; operators never edit it.

#### `sec_n_port_ingest`
- **Display name:** N-PORT monthly fund-holdings sweep
- **Description:** Walk `sec_nport_filer_directory` and ingest each trust CIK's pending NPORT-P / NPORT-P/A accessions. Soft 6h deadline; resumable via `n_port_ingest_log` (#917).
- **Source:** `sec_rate`
- **Cadence:** monthly day 22 03:00 UTC; `catch_up_on_boot=False`
- **Prerequisite:** `_bootstrap_complete`
- **Current params (body):** zero-arg → `ingest_all_fund_filers(conn, sec, ciks=<select from sec_nport_filer_directory>, deadline_seconds=settings.sec_n_port_sweep_deadline_seconds, source_label="sec_n_port_ingest")`.
- **Proposed PR1 params:** `cik: str | None` (primary), `deadline_seconds: int | None` (advanced), `min_period_of_report: date | None` (advanced).

### 2.8 Operational housekeeping

#### `raw_data_retention_sweep`
- **Display name:** Raw data retention sweep
- **Description:** Per-source compaction + age-based sweep of `data/raw/**`. Reclaims disk from byte-identical duplicates and ages out old files. Dry-run by default.
- **Source:** `db` (filesystem-bound; no SEC HTTP)
- **Cadence:** daily 02:00 UTC; `catch_up_on_boot=False`
- **Prerequisite:** none
- **Current params (body):** zero-arg → `run_raw_data_retention_sweep(conn, dry_run=settings.raw_retention_dry_run)`.
- **Proposed PR1 params:** `dry_run: bool` (primary; default = settings.raw_retention_dry_run), `source_filter: list[str] | None` (advanced).

#### `exchanges_metadata_refresh`
- **Display name:** eToro exchanges metadata refresh
- **Description:** Pull `/api/v1/market-data/exchanges` and upsert `description` on the `exchanges` table. Operator-curated `country` / `asset_class` are NOT touched.
- **Source:** `etoro`
- **Cadence:** weekly Sun 04:00 UTC; `catch_up_on_boot=True`
- **Prerequisite:** none
- **Current params:** zero-arg.
- **Proposed PR1 params:** none operator-exposable.

#### `etoro_lookups_refresh`
- **Display name:** eToro lookup catalogues refresh
- **Description:** Refresh eToro instrument-types + stocks-industries lookup catalogues into `etoro_instrument_types` / `etoro_stocks_industries` (#515 PR1). FE renders `Stocks` / `Healthcare` instead of numeric IDs.
- **Source:** `etoro`
- **Cadence:** weekly Sun 04:30 UTC; `catch_up_on_boot=True`
- **Prerequisite:** none
- **Current params:** zero-arg.
- **Proposed PR1 params:** none operator-exposable.

---

## 3. _BOOTSTRAP_STAGE_SPECS audit (24 stages)

`StageSpec` shape: `stage_key`, `stage_order`, `lane`, `job_name`. The orchestrator dispatches each stage's `job_name` through the standard `_INVOKERS` registry. Stages 14, 15, 21 currently invoke bespoke wrappers (see §4); stages 1-13, 16-20, 22-24 invoke jobs that ALSO appear in `SCHEDULED_JOBS` with the same name.

`requires` graph from [`_STAGE_REQUIRES`](../../app/services/bootstrap_orchestrator.py#L112). `lane` from [`_STAGE_LANE_OVERRIDES`](../../app/services/bootstrap_orchestrator.py#L164) where present, else `StageSpec.lane`.

| # | stage_key | Lane (effective) | job_name | requires | Also in SCHEDULED_JOBS? |
|---|---|---|---|---|---|
| 1 | `universe_sync` | init | `nightly_universe_sync` | () | ✗ (manual + bootstrap only) |
| 2 | `candle_refresh` | etoro | `daily_candle_refresh` | (universe_sync) | ✗ (manual + bootstrap only) |
| 3 | `cusip_universe_backfill` | sec_rate | `cusip_universe_backfill` | (universe_sync) | ✓ |
| 4 | `sec_13f_filer_directory_sync` | sec_rate | `sec_13f_filer_directory_sync` | (universe_sync) | ✓ |
| 5 | `sec_nport_filer_directory_sync` | sec_rate | `sec_nport_filer_directory_sync` | (universe_sync) | ✓ |
| 6 | `cik_refresh` | sec_rate | `daily_cik_refresh` | (universe_sync) | ✗ (manual + bootstrap only) |
| 7 | `sec_bulk_download` | sec_bulk_download | `sec_bulk_download` | (universe_sync) | ✗ (bootstrap only) |
| 8 | `sec_submissions_ingest` | db | `sec_submissions_ingest` | (sec_bulk_download, cik_refresh) | ✗ (bootstrap only) |
| 9 | `sec_companyfacts_ingest` | db | `sec_companyfacts_ingest` | (sec_bulk_download, cik_refresh) | ✗ (bootstrap only) |
| 10 | `sec_13f_ingest_from_dataset` | db | `sec_13f_ingest_from_dataset` | (sec_bulk_download, cusip_universe_backfill) | ✗ (bootstrap only) |
| 11 | `sec_insider_ingest_from_dataset` | db | `sec_insider_ingest_from_dataset` | (sec_bulk_download, cik_refresh) | ✗ (bootstrap only) |
| 12 | `sec_nport_ingest_from_dataset` | db | `sec_nport_ingest_from_dataset` | (sec_bulk_download, cusip_universe_backfill) | ✗ (bootstrap only) |
| 13 | `sec_submissions_files_walk` | sec_rate | `sec_submissions_files_walk` | (sec_submissions_ingest) | ✗ (bootstrap only) |
| 14 | `filings_history_seed` | sec_rate | `filings_history_seed` (PR1c #1064 — promoted from `bootstrap_filings_history_seed`) | (cik_refresh) | ✗ |
| 15 | `sec_first_install_drain` | sec_rate | `sec_first_install_drain` (PR1c #1064 — promoted from `sec_first_install_drain_job`) | (cik_refresh) | ✗ |
| 16 | `sec_def14a_bootstrap` | sec_rate | `sec_def14a_bootstrap` | (sec_submissions_ingest, sec_submissions_files_walk) | ✓ |
| 17 | `sec_business_summary_bootstrap` | sec_rate | `sec_business_summary_bootstrap` | (sec_submissions_ingest, sec_submissions_files_walk) | ✓ |
| 18 | `sec_insider_transactions_backfill` | sec_rate | `sec_insider_transactions_backfill` | (cik_refresh) | ✓ |
| 19 | `sec_form3_ingest` | sec_rate | `sec_form3_ingest` | (cik_refresh) | ✓ |
| 20 | `sec_8k_events_ingest` | sec_rate | `sec_8k_events_ingest` | (sec_submissions_ingest, sec_submissions_files_walk) | ✓ |
| 21 | `sec_13f_recent_sweep` | sec_rate | `sec_13f_quarterly_sweep` (PR1c #1064 — folded `bootstrap_sec_13f_recent_sweep` into the existing scheduled body via `min_period_of_report` + `source_label` params) | (cik_refresh) | ✓ |
| 22 | `sec_n_port_ingest` | sec_rate | `sec_n_port_ingest` | (cik_refresh) | ✓ |
| 23 | `ownership_observations_backfill` | db | `ownership_observations_backfill` | (5 bulk + legacy chain stages) | ✓ |
| 24 | `fundamentals_sync` | db | `fundamentals_sync` | (sec_companyfacts_ingest) | ✓ |

**Observations:**
- 11 of 24 stages invoke jobs that *also* appear in SCHEDULED_JOBS — these are already operator-invokable. Bootstrap dispatches them with whatever defaults the zero-arg body picks. No collapse work needed in PR1; just confirm scheduled + bootstrap call paths reach the same registered callable.
- 10 of 24 stages invoke jobs that are bootstrap-or-manual only (not in SCHEDULED_JOBS). These have entries in `_INVOKERS` so the admin "Run now" button works, but no cron schedule. They appear in the admin process table only when bootstrap is running. Post-PR1, they should still appear in the admin table with `cadence="manual-only"` so the operator can trigger them post-bootstrap for remediation.
- 3 of 24 stages invoke bespoke wrappers (rows 14, 15, 21). These are the PR1 collapse targets — see §4.

---

## 4. Bespoke bootstrap wrappers (collapse targets in PR1)

Three current wrappers each duplicate the parameter-overrides pattern at the named-callable layer. The duplication that matters lives one level down — at `refresh_filings`, `run_first_install_drain`, `ingest_all_active_filers`. PR1 extracts shared workflow helpers there; the bespoke wrappers go away and bootstrap stage definitions become `(job_name, params dict)` data entries.

### 4.1 `bootstrap_filings_history_seed` ([bootstrap_orchestrator.py:751](../../app/services/bootstrap_orchestrator.py#L751))

| Aspect | Detail |
|---|---|
| Lower-level helper | [`refresh_filings`](../../app/services/filings.py#L226) |
| Wrapper hardcodes | `provider=sec`, `provider_name="sec"`, `identifier_type="cik"`, `start_date=date.today()-730d`, `end_date=date.today()`, `instrument_ids=<every CIK-mapped tradable>`, `filing_types=sorted(SEC_INGEST_KEEP_FORMS)` |
| What's bespoke vs scheduled equivalent | No `filings_history_seed` exists in SCHEDULED_JOBS today. The wrapper is a one-shot bootstrap-only invocation of `refresh_filings` with universe-wide scope and full 2-year window |
| PR1 collapse | Promote a parameterised `filings_history_seed` job into SCHEDULED_JOBS. Bootstrap stage 14 dispatches it with `{"days_back": 730, "filing_types": "<KEEP_FORMS>", "instrument_id": null}`. Operator post-bootstrap can re-trigger with `instrument_id=AAPL.id` for targeted rewash |

### 4.2 `sec_first_install_drain_job` ([bootstrap_orchestrator.py:847](../../app/services/bootstrap_orchestrator.py#L847))

| Aspect | Detail |
|---|---|
| Lower-level helper | [`run_first_install_drain`](../../app/jobs/sec_first_install_drain.py#L239) |
| Wrapper hardcodes | `http_get=<adapted SEC client>`, `follow_pagination=True`, `use_bulk_zip=False`, `max_subjects=None` |
| Adapter | `_make_sec_http_get(sec_provider)` — narrows `SecFilingsProvider._http.get()` → `HttpGet = Callable[[str, dict], tuple[int, bytes]]` |
| What's bespoke vs scheduled equivalent | No standalone `sec_first_install_drain` in SCHEDULED_JOBS today. The drain is bootstrap-only |
| PR1 collapse | Promote `sec_first_install_drain` into SCHEDULED_JOBS (cadence: manual-only or weekly safety net). Bootstrap stage 15 dispatches with `{"max_subjects": null}`. Operator post-bootstrap can re-trigger with `max_subjects=10` for triage. **Operator-exposable param surface = `max_subjects` only.** `follow_pagination` + `use_bulk_zip` are implementation strategy knobs frozen at the registered-callable layer. The HTTP-get adapter stays internal |

### 4.3 `bootstrap_sec_13f_recent_sweep_job` ([bootstrap_orchestrator.py:897](../../app/services/bootstrap_orchestrator.py#L897))

| Aspect | Detail |
|---|---|
| Lower-level helper | [`ingest_all_active_filers`](../../app/services/institutional_holdings.py#L895) |
| Wrapper hardcodes | `ciks=list_directory_filer_ciks(conn)`, `deadline_seconds=settings.sec_13f_sweep_deadline_seconds`, `source_label="sec_edgar_13f_directory_bootstrap"`, `min_period_of_report=date.today()-380d` (4×95d) |
| Sibling scheduled equivalent | `sec_13f_quarterly_sweep` calls the same helper with: `source_label="sec_edgar_13f_directory"`, `min_period_of_report=None` (full historical) |
| Diff vs scheduled | Bootstrap variant adds the `min_period_of_report` recency cutoff (4 quarters back) to keep first-install under 1h; scheduled does the full historical sweep. `source_label` differs purely for audit trail |
| PR1 collapse | One scheduled `sec_13f_quarterly_sweep` job with `min_period_of_report` exposed as ParamMetadata. Bootstrap stage 21 dispatches with `{"min_period_of_report": "<today-380d>", "source_label": "sec_edgar_13f_directory_bootstrap"}` — the bootstrap-only `source_label` lives in the stage's hardcoded params dict, NOT in the operator-facing ParamMetadata. **Provenance discipline:** `source_label` (and any other audit-only string identifying which code path produced the rows) stays bootstrap-controlled; operators never edit it from Advanced disclosure or the row would lose audit traceability. The bespoke wrapper file disappears |

---

## 5. Manual-only / on-demand jobs (not in either registry)

For completeness, the following job-name constants exist in [`scheduler.py`](../../app/workers/scheduler.py#L214) but are NOT in `SCHEDULED_JOBS` and NOT in `_BOOTSTRAP_STAGE_SPECS`. They live in `_INVOKERS` (`runtime.py`) so the admin "Run now" button works.

| Job | Purpose | Notes |
|---|---|---|
| `daily_research_refresh` | Tier-1/2 thesis research | Now part of `orchestrator_full_sync` DAG; standalone retained for manual triage |
| `daily_news_refresh` | News pipeline | Same — DAG layer |
| `daily_thesis_refresh` | Thesis re-evaluation | Same — DAG layer |
| `morning_candidate_review` | Pre-trading-window scoring run | Same — DAG layer |
| `daily_tax_reconciliation` | Tax lot ledger reconciliation | On-demand only — operator runs manually |
| `daily_portfolio_sync` | eToro portfolio sync | High-freq orchestrator layer |
| `fx_rates_refresh` | Frankfurter FX | High-freq orchestrator layer |
| `attribution_summary` | Position attribution snapshot | Retired from cadence (#2026-04-19 refocus); body retained for manual fire |
| `weekly_report` / `monthly_report` | Reporting outputs | Scheduled inside report-orchestrator wrappers, not as direct ScheduledJob |
| `seed_cost_models` | One-shot cost-model seed | First-install only |
| `daily_financial_facts` | Legacy companyfacts ingest | Replaced by bulk-archive path; retained for fallback |

These are out of scope for PR1 ParamMetadata declarations (no scheduled fires to gate). But they appear in the admin process table — PR4 (`display_name` + `description` + ⓘ) covers their tooltips.

---

## 6. Operator-exposable param surface — taxonomy

After the per-job audit, the operator-exposable parameter set across the registry collapses to the following `ParamMetadata.field_type` archetypes for PR1:

| field_type | Used by | UX | Example |
|---|---|---|---|
| `bool` | force_full, dry_run | checkbox | `force_full: bool = False` |
| `int` | chunk_limit, deadline_seconds, max_subjects, instruments_per_run, filings_per_instrument, quarters_back | number input (with `min`/`max` per-param) | `chunk_limit: int = 500` |
| `float` | (reserved — currently no operator-exposable float; data-integrity-cliff knobs like `match_threshold` deliberately NOT exposed) | number input + step | `min: 0.0` `max: 1.0` `step: 0.01` |
| `date` | since, start_date, end_date, min_period_of_report | date picker | `since: date \| None` |
| `string` | (reserved — currently no plain-string operator field; provenance labels like `source_label` deliberately NOT exposed) | text input | — |
| `enum` | category (insiders / institutions / blockholders / treasury / def14a / funds / esop) | single-select | `category: Literal[...]` |
| `multi_enum` | filing_types (e.g. `["10-K", "10-Q", "DEF 14A"]`), layer_allowlist, source_filter | multi-select | `filing_types: list[Literal[...]]` |
| `quarter` | `quarter` (`YYYY[Q1-4]` format used by `cusip_universe_backfill`) | bespoke quarter picker (year + quarter dropdowns) | `quarter: str = "2026Q1"` |
| `ticker` | instrument_id (operators type AAPL not 320193) | typeahead resolves to `int` | `instrument_id: int \| None` |
| `cik` | cik (operators type AAPL not 0000320193) | typeahead resolves to `str` | `cik: str \| None` |

Notes:
- `prefetch_urls`, `follow_pagination`, `use_bulk_zip`, `paginate`, `source_label`, `match_threshold` were considered and rejected — implementation-strategy knobs and provenance labels DO NOT belong in operator UX. Code review changes them, not the Advanced disclosure.
- `multi_enum` is a new field type vs the original brief — `filing_types` (an allow-list across the SEC form universe) cannot be a single-select `enum`, and rendering it as a free-string list invites typos.
- `quarter` could collapse into `string` with a regex validator, but a bespoke widget produces less operator error surface — explicitly distinct field type.
- The ticker / cik typeaheads are NEW components for PR2 (or reuse from instruments page if one exists). Every other field type is plain HTML + existing repo widgets.

---

## 7. PR1 readiness checklist

- [ ] Every entry in §2 has a curated `display_name` + `description` for `ScheduledJob` decl.
- [ ] Every entry in §2 has a chosen `source` (lane).
- [ ] Every entry in §2 has a proposed `params_metadata` tuple (potentially empty).
- [ ] Bespoke wrappers in §4 each have a target lower-level helper + bootstrap-default param-set documented.
- [ ] Stages 14, 15, 21 in §3 each have a chosen scheduled-equivalent (existing or new) for the bespoke wrapper to collapse into.
- [ ] Source-level `JobLock` keys: `init`, `etoro`, `sec_rate`, `sec_bulk_download`, `db` are the five buckets.

---

## 8. Out of scope for PR0

- Code changes (PR0 is research-only).
- Adding params to lower-level helpers that don't currently accept them. The audit documents reality. PR1 may extend helper signatures where operator capability needs it.
- Per-param-set lock identity — same `job_name` + different params still serialise under one lock in PR1 (per operator-locked decision).
- Pre-flight resource estimates ("this will hit SEC ~12,000 times"). Useful UX, deferred past PR2.
- Operator favourites / saved param-sets. v2 concern.

## 9. Cross-references

- Operator intent (locked decisions): [`.claude/projects/-Users-lukebradford-Dev-eBull/memory/project_admin_control_hub_rewrite.md`](../../.claude/projects/-Users-lukebradford-Dev-eBull/memory/project_admin_control_hub_rewrite.md)
- Umbrella spec (#1064): [`docs/superpowers/specs/2026-05-08-admin-control-hub-rewrite.md`](../superpowers/specs/2026-05-08-admin-control-hub-rewrite.md)
- Bootstrap orchestration spec: [`docs/superpowers/specs/2026-05-08-bootstrap-etl-orchestration.md`](../superpowers/specs/2026-05-08-bootstrap-etl-orchestration.md)
- First-install bootstrap spec: [`docs/superpowers/specs/2026-05-07-first-install-bootstrap.md`](../superpowers/specs/2026-05-07-first-install-bootstrap.md)
- Settled-decisions (process topology #719, cancel UX): [`docs/settled-decisions.md`](../settled-decisions.md)
- Data-engineer skill (PR0 §10 update target): [`.claude/skills/data-engineer/SKILL.md`](../../.claude/skills/data-engineer/SKILL.md)
- ScheduledJob declarations: [`app/workers/scheduler.py:453`](../../app/workers/scheduler.py#L453)
- Bootstrap stage declarations: [`app/services/bootstrap_orchestrator.py:202`](../../app/services/bootstrap_orchestrator.py#L202)
- Bespoke wrappers: [`app/services/bootstrap_orchestrator.py:751,847,897`](../../app/services/bootstrap_orchestrator.py#L751)
- JobLock (PR1 mutation site): [`app/jobs/locks.py:76`](../../app/jobs/locks.py#L76)
