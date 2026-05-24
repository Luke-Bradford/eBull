# sec_10k

**Class.** SEC manifest.
**Form / endpoint.** Form 10-K + 10-K/A — annual report. Primary document HTML at `row.primary_document_url`. Discovery via `data.sec.gov/submissions/CIK{cik}.json` (Layer 3) + `/cgi-bin/browse-edgar?action=getcurrent&type=10-K` Atom (Layer 1) + `/Archives/edgar/full-index/{Y}/QTR{N}/form.idx` (Layer 2).

## 1. Origin

URL pattern: `https://www.sec.gov/Archives/edgar/data/{cik_int}/{accession_nodash}/{primary_doc}`. Provider `SecFilingsProvider` at `app/providers/implementations/sec_edgar.py`. Fetch via `provider.fetch_document_text(url)` (`app/services/manifest_parsers/sec_10k.py:128`). Body is HTML. Content-Type: `text/html` (occasionally text wrappers — parser handles both via `extract_business_section`).

## 2. Watermarking model

Two-axis watermark per #863 spec:

- `data_freshness_index` row keyed `(subject_type='issuer', subject_id=CIK, source='sec_10k')`. Cadence ceiling 120d (`app/services/data_freshness.py:96`). Layer 3 per-CIK poll respects `expected_next_at` for "what to poll next" ordering.
- `sec_filing_manifest` row keyed `(source='sec_10k', accession_number)`. `raw_status` lifecycle `pending → fetched → parsed → recorded` is the per-accession audit trail. `next_retry_at` set by `_failed_outcome` (`sec_10k.py:116`) at `now()+1h` on transient errors.

No conditional-GET. Layer 1 Atom is real-time push (5 min poll); Layer 2 daily-index reconciles missed Atom hits; Layer 3 per-CIK poll is the safety net.

## 3. Retry posture

- Fetch raises (transport / 5xx) → `ParseOutcome(status='failed')` + 1h backoff (`sec_10k.py:188-195`).
- Empty body / non-200 → `tombstoned` (`sec_10k.py:197-202`).
- 10-K/A with no Item 1 marker → fallback to prior plain 10-K via `_find_prior_plain_10k` (`sec_10k.py:251-264`); fallback fetch failure → 1h backoff; fallback also empty → tombstone.
- `store_raw` exception → `_failed_outcome` (`sec_10k.py:217-222`). Parser exception after `store_raw` committed → `_failed_outcome` with `raw_status='stored'` (preserves #938 invariant).
- Fan-out batch failure: transient (`psycopg.OperationalError` class per `is_transient_upsert_error`) → 1h retry; deterministic (IntegrityError / DataError) → tombstone with `raw_status='stored'`.

SEC rate-limit pool: `sec_rate` shared 10 req/s. Provider obeys the per-IP cap; manifest worker scheduling is the only fairness layer between sources sharing the lane.

## 4. Bootstrap path

Stage 18 `sec_business_summary_bootstrap` on the `sec_rate` lane (`app/services/bootstrap_orchestrator.py:1109`). Caps required: `submissions_processed` + `filing_events_seeded` + `bulk_archives_ready` (per `_STAGE_REQUIRES_CAPS["sec_business_summary_bootstrap"]` at line 562). Drives the manifest worker with `source='sec_10k'` scope; the stage completes when the manifest's `pending` count for that source reaches zero.

Expected wall-clock band: bounded by `sec_rate` 10 req/s shared with insider / 13F / 8-K stages. One fetch per accession; ~1 accession per CIK per year over 4-5y horizon = O(20k) fetches for a 4k-instrument universe ≈ 30-40 minutes assuming exclusive use of the lane.

## 5. Steady-state path

Manifest worker (`app/jobs/sec_manifest_worker.py`) is the sole steady-state writer post-#1155. Legacy daily 03:15 cron retired in the first #1155 cron-retirement sweep — see `.claude/skills/data-engineer/etl-endpoint-coverage.md` row `sec_10k`. The weekly `sec_business_summary_bootstrap` safety-net job remains as a top-up.

Discovery cadence: Layer 1 Atom every 5 min (`app/jobs/sec_atom_fast_lane.py:104`) + Layer 2 daily-index reconcile (`app/jobs/sec_daily_index_reconcile.py:46`) + Layer 3 per-CIK poll bounded by `cadence_for(source) = 120d` (`app/jobs/sec_per_cik_poll.py:39`).

## 6. Manifest insert

Row inserted by Layer 1/2/3 discovery via `record_manifest_entry` (`app/services/sec_manifest.py`). Shape:

- `source = 'sec_10k'` (per `ManifestSource` Literal at `app/services/sec_manifest.py:106-122`).
- `subject_type = 'issuer'`, `subject_id = issuer CIK`.
- `accession_number` = SEC-assigned accession.
- `primary_document_url` populated at discovery time.
- `filed_at` = SEC submission timestamp. Option C `(filed_at, source_accession)` gate at the **writer** (`upsert_business_summary` per `business_summary.py:892`) — not at the manifest layer. The gate suppresses stale arrivals during a `filed_at ASC` drain (sql/148).

## 7. Parser

Module `app/services/manifest_parsers/sec_10k.py`. Function `_parse_sec_10k` (line 132). Version `_PARSER_VERSION_10K = "10k-v1"` (line 95). Registered at `register()` (line 420) with `requires_raw_payload=True` — worker refuses to mark `parsed` when `raw_status='absent'`.

Extracts:

- Item 1 narrative body (`extract_business_section` at `business_summary.py`).
- Subsections best-effort (`extract_business_sections`; failure degrades to blob-only, `sec_10k.py:339-348`).
- Share-class siblings resolved via `_resolve_siblings(conn, instrument_id, issuer_cik)` (`_siblings.py`); GOOG + GOOGL share the body row per sibling.

Drop rules: missing `primary_document_url` → tombstone (`sec_10k.py:164-173`); missing `instrument_id` → tombstone (`sec_10k.py:174-183`); 10-K/A with no Item 1 + no prior plain 10-K → tombstone.

## 8. Observation insert

10-K is a **latest-only** source (issuer-level narrative; no observation history). Writes go directly to:

- `instrument_business_summary` — one row per instrument (~4031 in current universe).
- `instrument_business_summary_sections` — one row per (instrument, subsection).

Both tables are write-through (no separate observation table). The lint guard at `scripts/check_business_summary_latest_only.sh` enforces the 4 structural invariants (PK, no-demotion predicate, bounded writer surface, manifest exclusion) per `project_1233_pr10a_business_summary_latest_only.md`.

## 9. Current table refresh

`instrument_business_summary` IS the current table — no `refresh_*_current` helper. No `_CATEGORIES` repair sweep entry (the table is its own write-through sink).

The Option C `(filed_at, source_accession)` gate in `upsert_business_summary` (sql/148 + `business_summary.py:892`) is the integrity floor: re-runs are idempotent (suppressed-as-stale becomes a no-op return; manifest still marks `parsed`).

## 10. Operator-visible endpoint

- `GET /instruments/{symbol}/business_sections` — `app/api/instruments.py:1366`. Returns the latest Item 1 sections + parse status.
- `GET /instruments/{symbol}/filings/10-k/history` — `app/api/instruments.py:1504`. Returns reverse-chronological 10-K + 10-K/A list for the instrument (#559).

## 11. Verification queries

```sql
-- AAPL business summary present + recently parsed
SELECT filed_at, source_accession, length(body) AS body_len, last_parsed_at
  FROM instrument_business_summary
 WHERE instrument_id = (SELECT instrument_id FROM instruments WHERE symbol='AAPL')
 ORDER BY filed_at DESC NULLS LAST
 LIMIT 1;

-- Manifest drain audit for AAPL CIK 0000320193
SELECT raw_status, COUNT(*) FROM sec_filing_manifest
 WHERE source='sec_10k' AND subject_type='issuer' AND subject_id='0000320193'
 GROUP BY raw_status;
```

Cross-source check: SEC EDGAR full-text search `https://efts.sec.gov/LATEST/search-index?q=&forms=10-K&dateRange=custom&startdt=2024-01-01&enddt=2024-12-31&ciks=0000320193` should list the same accessions.

Smoke endpoint: `curl https://localhost/api/instruments/AAPL/business_sections | jq '.sections | length'` — expect > 0.

## 12. Smoke test

`tests/smoke/test_etl_source_to_sink.py` parametrized row for `sec_10k`. Asserts: provider importable; `registered_parser_sources()` contains `sec_10k`; Stage 18 exists in `_BOOTSTRAP_STAGE_SPECS`; `instrument_business_summary` table exists; business-sections endpoint responds for AAPL.

## 13. Known gotchas

- **10-K/A fallback path** (`sec_10k.py:241-331`) — amendments often omit Item 1. The parser fetches the **prior plain 10-K** for body extraction so amended filings still render. Stores the fallback raw payload under the fallback accession so audit / rewash can locate it.
- **Share-class fan-out** (sec_10k.py:355-393) — 10-K is issuer-level. Fan out across siblings (GOOG / GOOGL, BRK.A / BRK.B). One sections-upsert failure per sibling degrades to blob-only via nested savepoint; mirrors legacy `business_summary.py:1774` semantics.
- **Option C filed_at gate** (sql/148) — manifest drain is `filed_at ASC`. Without the gate, a fresh-DB drain would briefly render the 2018 Item 1 narrative before the 2024 update fires. The gate returns `'suppressed'` for stale arrivals; the adapter treats that as a successful drain.
- **Composite parser version on the typed table differs from the manifest column** (`_PARSER_VERSION_10K = "10k-v1"`). A bump triggers rewash via `POST /jobs/sec_rebuild/run {"source": "sec_10k"}`.
- **Pre-#1151 rows with NULL `filed_at`** — backfill in sql/148 matches by `source_accession` against `filing_events.provider_filing_id`. Rows that pre-date their ancestor stay NULL forever; the conditional `ON CONFLICT` treats NULL incumbents as "no incumbent" so first dated write re-baselines cleanly.
