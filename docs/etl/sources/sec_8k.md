# sec_8k

**Class.** SEC manifest.
**Form / endpoint.** Form 8-K + 8-K/A — current report (material events). Primary document HTML at `row.primary_document_url`. Discovery via Atom (Layer 1, 5 min) + daily-index reconcile (Layer 2) + per-CIK poll (Layer 3).

## 1. Origin

URL pattern: `https://www.sec.gov/Archives/edgar/data/{cik_int}/{accession_nodash}/{primary_doc}`. Provider `SecFilingsProvider` at `app/providers/implementations/sec_edgar.py`. Fetch via `provider.fetch_document_text(url)` (`app/services/manifest_parsers/eight_k.py:244`). Body is HTML. 8-K is event-driven — issued within 4 business days of triggering event.

## 2. Watermarking model

- `data_freshness_index` row `(source='sec_8k')`. Cadence ceiling **14d** (`app/services/data_freshness.py:98` `_CADENCE`) — tightest of the SEC manifest cadences since 8-K is event-driven and high-velocity.
- `sec_filing_manifest` row keyed `(source='sec_8k', accession_number)`. `raw_status` lifecycle `pending → fetched → parsed → recorded`. `next_retry_at` set by `_failed_outcome` (`eight_k.py:124`) at `now()+1h` on transient errors.

Layer 1 Atom is the primary discovery path for 8-K — material events propagate to operator within minutes of the SEC publish.

## 3. Retry posture

- Fetch raises → `ParseOutcome(status='failed')` + 1h backoff (`eight_k.py:242-252`).
- Empty body / non-200 → write tombstone row in `eight_k_filings` AND return `tombstoned` (`eight_k.py:254-279`). Savepoint protects the worker's outer tx from a tombstone-INSERT failure.
- `store_raw` exception → `_failed_outcome` (`eight_k.py:288-303`).
- Parser raise after `store_raw` committed → `_failed_outcome` with `raw_status='stored'` (#938 invariant per `eight_k.py:305-328`).
- Parser returns `None` (no header fields or items extracted) → write tombstone + return `tombstoned`.
- Upsert exception: transient (`is_transient_upsert_error`) → 1h retry; deterministic → tombstone with `raw_status='stored'` (`eight_k.py:367-405`).
- **Dividend extraction failure is non-fatal**: transient → 1h retry whole row; deterministic → log + drop dividend write but preserve `parsed` outcome (the 8-K filing/items/exhibits rows already landed; `eight_k.py:419-439`).

SEC rate-limit pool: `sec_rate` shared 10 req/s.

## 4. Bootstrap path

Stage 21 `sec_8k_events_ingest` on the `sec_rate` lane (`app/services/bootstrap_orchestrator.py:1112`). Caps required: `filing_events_seeded` + `submissions_secondary_pages_walked` (per `_STAGE_REQUIRES_CAPS["sec_8k_events_ingest"]` at line 575).

Drives the manifest worker with `source='sec_8k'` scope until the manifest's `pending` count reaches zero. Wall-clock band bounded by `sec_rate` 10 req/s shared with insider / 13F / 10-K stages. 8-K volume is high (multiple per CIK per year) — ~50k+ accessions for a 4k-instrument universe.

## 5. Steady-state path

Manifest worker is the sole steady-state writer post-#1155. `JOB_SEC_8K_EVENTS_INGEST` moved to on-demand; bootstrap Stage 20 still dispatches via `_INVOKERS` (per `.claude/skills/data-engineer/etl-endpoint-coverage.md` row `sec_8k`).

Discovery cadence: Layer 1 Atom 5 min (`app/jobs/sec_atom_fast_lane.py:104`) — primary path for event-driven 8-K — + Layer 2 daily-index (`app/jobs/sec_daily_index_reconcile.py:46`) + Layer 3 per-CIK poll bounded by `cadence_for('sec_8k') = 14d` (`app/jobs/sec_per_cik_poll.py:39`).

## 6. Manifest insert

Row inserted by Layer 1/2/3 discovery via `record_manifest_entry`. Shape:

- `source = 'sec_8k'` (per `ManifestSource` Literal at `app/services/sec_manifest.py:106-122`).
- `subject_type = 'issuer'`, `subject_id = issuer CIK`.
- `accession_number` = SEC-assigned accession.
- `primary_document_url` populated at discovery time.
- `filed_at` = SEC submission timestamp. No Option C gate at the writer — `upsert_8k_filing` (`app/services/eight_k_events.py:400`) is PK=accession, single-row latest-write-wins.

## 7. Parser

Module `app/services/manifest_parsers/eight_k.py`. Function `_parse_eight_k` (line 189). Composite version `_PARSER_VERSION_EIGHT_K = f"8k:{_PARSER_VERSION}+dividend:{_PARSER_VERSION_DIVIDEND}"` (line 104). Registered at `register()` (line 448) with `requires_raw_payload=True`.

Extracts:

- 8-K header fields + items + exhibits via `parse_8k_filing` (delegated to `app/services/eight_k_events.py`).
- Dividend announcements via `parse_dividend_announcement` (`app/services/dividend_calendar.py`) — best-effort, gated by internal regex `_DIVIDEND_CONTEXT_RE`. Fans out to share-class siblings via `_resolve_siblings` (#1102 / #1117) per `eight_k.py:175-186`. Replaces legacy `sec_dividend_calendar_ingest` cron coverage (#1158).

Drop rules: missing `primary_document_url` → tombstone (`eight_k.py:217-230`); missing `instrument_id` → tombstone (`eight_k.py:231-240`); parser returns `None` → tombstone with `raw_status='stored'`.

## 8. Observation insert

- `eight_k_filings` — PK=`accession_number` (entity-level table per sec-edgar §3.6 / data-engineer §11). One row per 8-K accession with header fields + items + exhibits. The per-instrument read bridge runs through `filing_events` (sql/144).
- `dividend_events` — PK `(instrument_id, source_accession)`. UPSERT semantics make re-runs harmless (`eight_k.py:158-163`); re-running after `_PARSER_VERSION_DIVIDEND` bump rewrites with new regex output.

No tombstone columns on `eight_k_filings` — `_write_tombstone` (`app/services/eight_k_events.py`) writes a sentinel row with the document-type marker so dashboard counts match across legacy + manifest writers (`eight_k.py:25-27`).

## 9. Current table refresh

`eight_k_filings` IS the current table — PK=accession. No `refresh_*_current` helper; the table is its own write-through sink.

`dividend_events` is also write-through (PK = `(instrument_id, source_accession)`). The `_CATEGORIES` daily sweep at `app/jobs/ownership_observations_repair.py:69` does NOT cover dividends — dividends are a separate vertical from the 7-category ownership rollup.

## 10. Operator-visible endpoint

- `GET /instruments/{symbol}/eight_k_filings` — `app/api/instruments.py:1203`. Returns reverse-chronological 8-K filing list with items + exhibits.
- `GET /instruments/{symbol}/dividends` — `app/api/instruments.py:1696`. Returns dividend events (announcement-date / ex-date / record-date / pay-date).

## 11. Verification queries

```sql
-- Most-recent 8-K for AAPL
SELECT accession_number, filed_at, primary_document_url
  FROM eight_k_filings
 WHERE instrument_id = (SELECT id FROM instruments WHERE symbol='AAPL')
 ORDER BY filed_at DESC LIMIT 5;

-- Items extracted per accession (cross-check item array against parser output)
SELECT item_code, item_label FROM eight_k_filing_items
 WHERE accession_number = '<recent_accession>';

-- Dividend events extracted from 8-K bodies
SELECT source_accession, announcement_date, ex_date, amount_per_share
  FROM dividend_events
 WHERE instrument_id = (SELECT id FROM instruments WHERE symbol='AAPL')
 ORDER BY announcement_date DESC LIMIT 5;

-- Manifest drain audit
SELECT raw_status, COUNT(*) FROM sec_filing_manifest
 WHERE source='sec_8k' AND subject_type='issuer' AND subject_id='0000320193'
 GROUP BY raw_status;
```

Cross-source check: SEC EDGAR full-text search filtered to form=8-K + CIK. Dividend amounts cross-check against [marketbeat.com/stocks/NASDAQ/AAPL/dividend](https://www.marketbeat.com/stocks/NASDAQ/AAPL/dividend).

## 12. Smoke test

`tests/smoke/test_etl_source_to_sink.py` parametrized row for `sec_8k`. Asserts: provider importable; `registered_parser_sources()` contains `sec_8k`; Stage 21 exists in `_BOOTSTRAP_STAGE_SPECS`; `eight_k_filings` + `dividend_events` tables exist; 8-K endpoint responds for AAPL.

## 13. Known gotchas

- **Dividend extraction is NOT gated on `parsed.items`** (`eight_k.py:138-148`). 8-K item arrays often disagree with HTML body content — Item 8.01 covers buybacks / litigation / JVs; dividend announcements have been observed under 7.01. `parse_dividend_announcement` has its own regex gate (`_DIVIDEND_CONTEXT_RE` requires `$N.NN per share` in proximity to `dividend`); false positives on non-dividend bodies are extremely unlikely. Net effect: equal-or-better coverage than the legacy cron, since amendments (8-K/A) and items-array-misclassified filings now get extraction.
- **Composite parser version on the manifest column only.** `_PARSER_VERSION_EIGHT_K` ("8k:N+dividend:M") drives `sec_filing_manifest.parser_version` so a bump to either sub-version triggers rewash via `POST /jobs/sec_rebuild/run {"source": "sec_8k"}`. The typed `eight_k_filings.parser_version` stays at the bare `_PARSER_VERSION` integer (provenance for the typed-table writer; not consumed by rewash logic per `eight_k.py:99-103`).
- **`store_raw` parser_version stays bare** (`eight_k.py:295` `parser_version=str(_PARSER_VERSION)`). Raw-document provenance is NOT a rewash signal; tying it to dividend regex versions would invalidate the raw HTML cache on every regex bump for no integrity benefit (#1158 Codex pre-spec round 2 HIGH per `eight_k.py:99-103`).
- **Share-class fan-out for dividends only**: `eight_k_filings` is entity-level (PK=accession; per-instrument read via `filing_events` bridge). `dividend_events` IS fanned out per sibling so GOOG + GOOGL both show the same announcement (`eight_k.py:175-186`).
- **Dividend failure cannot tombstone the 8-K row** (`eight_k.py:419-440`). The 8-K filing + items + exhibits writes already landed successfully by the time dividend extraction runs; a dividend-side bug must not lose operator-visible 8-K data.
- **Tombstone on parse miss is intentional** (`eight_k.py:329-350`). Matches legacy `_write_tombstone` semantics so dashboard counts (`rows_tombstoned`) match across both writer paths.
