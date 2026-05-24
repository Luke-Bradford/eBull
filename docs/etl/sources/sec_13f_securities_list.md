# sec_13f_securities_list

**Class.** SEC bulk reference (NOT `ManifestSource` — bulk reference; section 6 = N/A).
**Form / endpoint.** SEC Official List of Section 13(f) Securities — `https://www.sec.gov/files/investment/13flist{year}q{quarter}.txt`.

## 1. Origin

Fixed-width TXT file published quarterly. URL constant `_LIST_URL` at `app/services/sec_13f_securities_list.py:77`. Fetcher uses `urllib.request` directly (see imports at `app/services/sec_13f_securities_list.py:55`). ~24k rows per quarter. The canonical free regulated source for CUSIP ↔ issuer-name mapping for US-listed equities and ADRs. eBull's settled "free regulated-source-only" posture (#532) means we cannot license CUSIPs from CGS — this file is the only forward-backfill path. CUSIP shape is 9 alphanumeric per `_CUSIP_RE` at `app/services/sec_13f_securities_list.py:84` (accepts CUSIP for US issuers and CINS — CUSIP International Numbering System, same shape with alpha prefix — for foreign-domiciled securities). Parsed row shape: `ThirteenFSecurity (cusip, issuer_name, description, is_added_since_last, status)` at `app/services/sec_13f_securities_list.py:87-96`. Status code `'E'` existing / `'N'` new / `'D'` deleted.

## 2. Watermarking model

Quarter-driven. Latest closed quarter resolved via `_last_completed_quarter` from `app/services/sec_13f_filer_directory.py` (imported at `app/services/sec_13f_securities_list.py:72`). One walk per quarterly publication. No conditional-GET; the file changes meaningfully each quarter (new CUSIPs added, retired ones marked `D`).

## 3. Retry posture

Per-row defensive parsing. Column widths drift slightly across quarterly publications per `app/services/sec_13f_securities_list.py:45-48` — parser anchors on the leading 9-char CUSIP + trailing single-letter status code, splitting the middle on 2+-space gaps to recover issuer name + description. Rows failing CUSIP regex are dropped (not raised). HTTP errors propagate to the caller (`urllib.request.urlopen` raises `HTTPError` / `URLError`).

## 4. Bootstrap path

**Stage 3 in `_BOOTSTRAP_STAGE_SPECS`.** `_spec("cusip_universe_backfill", 3, "sec_rate", "cusip_universe_backfill")` at `app/services/bootstrap_orchestrator.py:1041`. Cap requirement `CapRequirement(all_of=("universe_seeded",))` at `app/services/bootstrap_orchestrator.py:530`. Provides cap `cusip_mapping_ready` at `app/services/bootstrap_orchestrator.py:360`. Lane `sec_rate`. Required by Phase D `cusip_resolver_post_bulk_sweep` (S13) and every downstream 13F-HR / NPORT joiner.

Expected wall-clock <30s (single ~600 KB SEC fetch + Python-side fuzzy-match over ~24k rows). Per `app/workers/scheduler.py:920-922` description: "one ~600KB SEC fetch + a Python-side fuzzy match over ~12k rows (~10s wall-clock)" — note row count has grown to ~24k as of 2026.

## 5. Steady-state path

`ScheduledJob(name=JOB_CUSIP_UNIVERSE_BACKFILL, source="sec_rate", cadence=Cadence.weekly(weekday=6, hour=5, minute=0), catch_up_on_boot=True)` at `app/workers/scheduler.py:893-923`. **Weekly Sunday 05:00 UTC** (not quarterly — the cron runs weekly but the underlying SEC publication is quarterly; re-running between publications is a cheap idempotent read). 30 min after `ownership_observations_backfill` (03:00) and 30 min after `etoro_lookups_refresh` (04:30). `catch_up_on_boot=True` so fresh install with empty `external_identifiers` benefits from running immediately.

## 6. Manifest insert

**N/A.** Bulk reference source. No `sec_filing_manifest` row written. `sec_13f_securities_list` is not listed in the `ManifestSource` Literal at `app/services/sec_manifest.py:106-122`.

## 7. Parser

In-line fixed-width parser inside the service. Walks the TXT line-by-line, regex-anchors on the 9-char CUSIP + trailing status code, splits the middle on 2+-space gaps to recover issuer name + description per `app/services/sec_13f_securities_list.py:35-48`. Fuzzy-matches each row's `issuer_name` against `instruments.company_name` via the `_normalise_name` + `_similarity` helpers imported from `app.services.cusip_resolver` at `app/services/sec_13f_securities_list.py:65-71`. Match threshold = `MATCH_THRESHOLD` (re-used from cusip_resolver). NOT a `manifest_parsers/` entry.

## 8. Observation insert

Destination: **`external_identifiers`** UPSERT with `provider='sec'`, `identifier_type='cusip'`, `is_primary=FALSE` (curated path takes precedence when one exists per `app/services/sec_13f_securities_list.py:21-23`). Post-batch the service calls `sweep_resolvable_unresolved_cusips` from `app.services.cusip_resolver` (imported at `app/services/sec_13f_securities_list.py:65-71`) to promote previously-stranded 13F holdings the moment the new mapping arrives. Tracks per-run rollup via `CusipCoverageBackfillResult` at `app/services/sec_13f_securities_list.py:98-109` (counts inserted, skipped_already_mapped, tombstoned_unresolvable, tombstoned_ambiguous, tombstoned_conflict + the sweep report).

## 9. Current table refresh

**N/A.** `external_identifiers` IS the current table — no separate `_current` snapshot. No MERGE writer.

## 10. Operator-visible endpoint

No dedicated endpoint. Coverage surfaces indirectly via:
- `GET /system/bootstrap-status` (S3 `cusip_universe_backfill` row state).
- `GET /admin/data-freshness` (panel reads `data_freshness_index`).
- Any 13F-HR-driven endpoint (`/instruments/<symbol>/ownership-rollup` and similar) — when CUSIP coverage is missing, the institutional holdings ingester drops rows into `unresolved_13f_cusips` and the rollup figures understate.

## 11. Verification queries

```sql
-- AAPL CUSIP resolution (expect '037833100').
SELECT identifier_value, is_primary, provider
  FROM external_identifiers
 WHERE provider IN ('sec', 'openfigi')
   AND identifier_type = 'cusip'
   AND instrument_id = (SELECT instrument_id FROM instruments WHERE symbol = 'AAPL');

-- Coverage stat — total mapped CUSIPs.
SELECT provider, COUNT(*) FROM external_identifiers
 WHERE identifier_type = 'cusip'
 GROUP BY provider;

-- Strand depth — how many 13F holdings remain unresolved.
SELECT source, COUNT(*) FROM unresolved_13f_cusips GROUP BY source;
```

Cross-source confirm: spot-check AAPL CUSIP `037833100` against `https://www.cusip.com/` or `https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=0000320193&type=13F-HR` (any 13F-HR INFOTABLE row referencing AAPL will carry the same CUSIP).

## 12. Smoke test

Path: `tests/smoke/test_etl_source_to_sink.py::test_sec_13f_securities_list`. Asserts: service importable; ScheduledJob `JOB_CUSIP_UNIVERSE_BACKFILL` exists in `SCHEDULED_JOBS`; bootstrap stage S3 `cusip_universe_backfill` present in `_BOOTSTRAP_STAGE_SPECS`; cap `cusip_mapping_ready` published by S3; `external_identifiers` row exists for the AAPL fixture with `provider='sec'`, `identifier_type='cusip'`.

## 13. Known gotchas

1. **Bulk reference — NOT a `ManifestSource`.** Section 6 = N/A by design.
2. **Highest-ROI residual.** Per Codex review (memory: project_etl_v3_consolidated_findings + project_stream_a_run_8_fixes_consolidated_findings), CUSIP resolution remains the **highest-ROI post-Stream-A residual**. As of 2026 the resolver still resolves only ~19 / 16M unresolved CUSIPs (#740). The Official List walk is the operator-priority bulk backfill path; the reverse `cusip_resolver` path handles the residual long tail.
3. **Fuzzy-match threshold is the precision-recall lever.** `MATCH_THRESHOLD` shared with `cusip_resolver` — lowering increases coverage at the cost of false positives that stamp the wrong issuer's CUSIP onto an instrument.
4. **CINS rows.** Foreign-domiciled securities use CINS (CUSIP International Numbering System) — same 9-alphanumeric shape with alpha prefix instead of digit prefix. Stored verbatim under the same `identifier_type='cusip'`.
5. **Column widths drift quarterly.** Anchored parsing (leading CUSIP + trailing status code) defends against width changes; do NOT switch to fixed-offset slicing without first verifying every quarter back to 2014.
6. **`is_primary=FALSE` writes.** The curated path takes precedence when one exists. A future curated-CUSIP override must keep `is_primary=TRUE` so the sec-backfill path does NOT overwrite it.
7. **`catch_up_on_boot=True`.** Fresh installs benefit from immediate run; cost is bounded.
8. **The reverse-direction sibling.** `cusip_resolver.resolve_unresolved_cusips` handles the reverse path (filer-reported CUSIPs in `unresolved_13f_cusips` → fuzzy-match against instruments). The two paths complement each other — do NOT collapse into a single job.
