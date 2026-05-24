# sec_n_csr

**Class.** SEC manifest.
**Form / endpoint.** N-CSR / N-CSRS — certified shareholder reports (semi-annual). SEC archive: iXBRL companion at `<basename>_htm.xml` next to the primary HTML doc.

## 1. Origin
iXBRL XML payload derived from the primary HTML document (`_ixbrl_companion_url` at `app/services/manifest_parsers/sec_n_csr.py:150-165`). Per spike §3.3, convention is `<basename>_htm.xml` in the same accession folder as `primary_doc.htm`. Provider: `app/providers/implementations/sec_edgar.py::SecFilingsProvider`. Real fund-metadata parser per #1171 (REPLACED #918 / PR #1170 synth no-op).

## 2. Watermarking model
Per-(subject, source) row in `data_freshness_index` keyed on `subject_type='fund_series'`, `subject_id=<class_id>`. Cadence ceiling 200d (`app/services/data_freshness.py:93`) — semi-annual cadence ~6mo. Manifest fed by S27 drain + atom-discovered freshness post-bootstrap.

## 3. Retry posture
- iXBRL fetch raise (transient) → `_failed_outcome` with `_FAILED_RETRY_DELAY = 1h` (`sec_n_csr.py:68, 128-137`).
- Resolver miss `PENDING_CIK_REFRESH` → 24h backoff via `_PENDING_CIK_REFRESH_DELAY` (`sec_n_csr.py:70-71`) — gives daily `cik_refresh` time to write the ext_id.
- Resolver miss `EXT_ID_NOT_YET_WRITTEN` → transient (1h).
- Resolver miss `INSTRUMENT_NOT_IN_UNIVERSE` (unanimous across all classes) → tombstone with that reason.
- Zero classes resolved + mixed miss-reasons OR any transient → `failed` (1h re-classify).
- **Post-parse retention gate `n_csr_within_retention(filed_at)`** (`sec_n_csr.py:106-125`) → tombstone with `outside_retention`.

## 4. Bootstrap path
Two stages contribute:
- Stage 26 `mf_directory_sync` (`app/services/bootstrap_orchestrator.py:1181`). Lane `sec_rate`. Dispatches `JOB_MF_DIRECTORY_SYNC = "mf_directory_sync"` (`app/workers/scheduler.py:320, 4782`). Refreshes the `classId` → instrument mapper. Advertises `class_id_mapping_ready` cap.
- Stage 27 `sec_n_csr_bootstrap_drain` (`bootstrap_orchestrator.py:1187-1192`, TERMINAL). Lane `sec_rate`. Dispatches `JOB_SEC_N_CSR_BOOTSTRAP_DRAIN = "sec_n_csr_bootstrap_drain"` (`app/workers/scheduler.py:321, 4819`). Per-trust enqueue of N-CSR + N-CSRS manifest rows for the #1171 fund-metadata parser to drain. Dispatches with NO params (730d cap is hard-pinned, see §13).

## 5. Steady-state path
Atom-discovered freshness drives the manifest worker post-bootstrap. No standalone cron — drain stage runs only at bootstrap.

## 6. Manifest insert
`sec_filing_manifest.source = 'sec_n_csr'`. `subject_type='fund_series'`, `subject_id=<class_id>` (resolved via `_fund_class_resolver`). `instrument_id` populated post-resolve. Option C `filed_at` gate at `record_manifest_entry`.

## 7. Parser
`app/services/manifest_parsers/sec_n_csr.py::_parse_n_csr`. Version `_PARSER_VERSION_N_CSR = "n-csr-fund-metadata-v1"` (`sec_n_csr.py:66`). Registered with `requires_raw_payload=False` (`sec_n_csr.py:35-37`) per operator choice (spec §2). Re-parse on parser-version bump re-fetches iXBRL from SEC.

Extraction flow (spec §8 at `sec_n_csr.py:9-24`):
1. Validate URL.
2. Resolve iXBRL companion URL from primary doc URL (`_ixbrl_companion_url`).
3. Fetch iXBRL via `SecFilingsProvider`.
4. `extract_fund_metadata_facts` (`app/services/n_csr_extractor.py`) → one `FundMetadataFacts` per `(series_id, class_id)`.
5. For each class: `resolve_class_id_to_instrument` (`_fund_class_resolver`); on miss, `classify_resolver_miss` discriminates pending vs deterministic.
6. Per resolved class (single transaction per class): soft-supersede prior rows for `(instrument_id, source_accession)` with `known_to=NOW()`; INSERT fresh observation; `refresh_fund_metadata_current`.

**Three tiers** extracted per #1171:
- **Tier 1 scalars**: trust_cik, trust_name, entity_inv_company_type, series_id, series_name, class_id, class_name, trading_symbol, exchange, inception_date, shareholder_report_type, expense_ratio_pct, expenses_paid_amt, net_assets_amt, advisory_fees_paid_amt, portfolio_turnover_pct, holdings_count, material_chng_date, contact info, prospectus info.
- **Tier 2 JSONB**: returns_pct, benchmark_returns_pct, sector_allocation, region_allocation, credit_quality_allocation, growth_curve.
- **Tier 3 `raw_facts` fallback**: the entire extracted-facts dict, JSON-serialised via `_json_serializer` (Decimal → str, date/datetime → ISO).

## 8. Observation insert
`fund_metadata_observations`. PK `(instrument_id, source_accession)`. Per resolved class. Tombstone via `known_to=NOW()` (soft-supersede on re-parse, `sec_n_csr.py:209-219`).

## 9. Current table refresh
`refresh_fund_metadata_current` (`app/services/fund_metadata.py`). MERGE writer per #1255 (note: `fund_metadata_current` is a separate category from the 7 in `_CATEGORIES` — it's not part of the ownership-rollup repair sweep; reconciliation is driven by per-parse `refresh_fund_metadata_current` calls only).

## 10. Operator-visible endpoint
- `GET /instruments/{symbol}/fund-metadata` — current row (`app/api/fund_metadata.py:116-117`).
- `GET /instruments/{symbol}/fund-metadata/history` — currently-valid observation timeline (`app/api/fund_metadata.py:8-9`).
- `GET /coverage/fund-metadata` — per-source coverage audit (`app/api/fund_metadata.py:11`).

## 11. Verification queries
```sql
-- Current fund-metadata row for SPY (or any ETF).
SELECT i.symbol, fmc.series_name, fmc.class_name, fmc.expense_ratio_pct,
       fmc.net_assets_amt, fmc.portfolio_turnover_pct, fmc.period_end
FROM fund_metadata_current fmc
JOIN instruments i ON i.id = fmc.instrument_id
WHERE i.symbol = 'SPY';
```
Smoke: `curl localhost:8000/instruments/SPY/fund-metadata | jq '.expense_ratio_pct, .net_assets_amt'`. Cross-source: spot-check `expense_ratio_pct` against the fund's published prospectus or `etfdb.com` expense-ratio page.

## 12. Smoke test
`tests/smoke/test_etl_source_to_sink.py::test_sec_n_csr_wired`. Asserts parser registered, stages 26 + 27 in `_BOOTSTRAP_STAGE_SPECS`, `fund_metadata_observations` + `fund_metadata_current` tables exist.

## 13. Known gotchas
1. **730d retention is hard-pinned** (`N_CSR_RETENTION_DAYS = 730` at `sec_n_csr.py:73-82` per `project_1233_pr8_ncsr_730d_cap.md`). Unlike PR6 (13F-HR) / PR7 (N-PORT), N-CSR has NO deep-dive override — `sec_rebuild` requeues a pre-cap accession and the parser gate tombstones with `outside_retention`. Pre-cap fund-metadata is NOT part of any consumer surface — accepting the loss is the explicit trade-off in spec §8 acceptance #6.
2. **`horizon_days` param chain removed end-to-end** (`bootstrap_orchestrator.py:1182-1192` comment). Bootstrap stage 27 dispatches with NO params — single source of truth at module level (`N_CSR_RETENTION_DAYS`).
3. **iXBRL companion URL convention** (`sec_n_csr.py:150-165`). `<basename>_htm.xml` in the same folder as the primary HTML doc. Spike §3.3 confirmed. If SEC changes convention, EVERY accession resolves to a 404 — empty body branch tombstones, log floods.
4. **Resolver-miss bucket discipline** (`sec_n_csr.py:26-33`). At-least-one-class resolved → `parsed` (partial-success per spec §7.4). Zero + unanimous `INSTRUMENT_NOT_IN_UNIVERSE` → tombstone. Zero + mixed → `failed`. Misrouting buckets means either re-fetch storms or silent data loss.
5. **Parser version bump = full re-fetch** since `requires_raw_payload=False` — no `filing_raw_documents` cache to re-parse from. Bump cautiously.
6. **3-tier extraction discipline** (`sec_n_csr.py:225-260`). Tier 1 typed columns + Tier 2 JSONB + Tier 3 `raw_facts` fallback. Adding a new field that should be Tier 1 but lands in Tier 3 makes it invisible to typed consumers.
7. **tz-naive `now` / `filed_at` is a `ValueError`** (`sec_n_csr.py:96-102, 120-125`) — PR7 Codex 2 lesson: `date.today()` / `datetime.now()` honour caller's local TZ.
