# sec_10q

**Class.** SEC manifest (synth no-op parser per #1168).
**Form / endpoint.** Form 10-Q + 10-Q/A — quarterly report. Discovery via `data.sec.gov/submissions/CIK{cik}.json` (Layer 3) + Atom + daily-index reconcile.

## 1. Origin

URL pattern: `https://www.sec.gov/Archives/edgar/data/{cik_int}/{accession_nodash}/{primary_doc}`. Provider `SecFilingsProvider` at `app/providers/implementations/sec_edgar.py`. **The synth no-op parser at `app/services/manifest_parsers/sec_10q.py:67-88` does NOT call the provider** — no fetch occurs at manifest dispatch time.

Financial data lands via Companyfacts XBRL, not via the 10-Q parser. The Companyfacts JSON path (Stage 9 `sec_companyfacts_ingest` bulk + steady-state `JOB_FUNDAMENTALS_SYNC` at `app/workers/scheduler.py:616`) is the sole writer of 10-Q financial-statement fields into `financial_facts_raw`.

## 2. Watermarking model

- `data_freshness_index` row `(source='sec_10q')`. Cadence ceiling **60d** (`app/services/data_freshness.py:97`) — 10-Q due within 40-45d of quarter-end; 60d ceiling = filing window + buffer.
- `sec_filing_manifest` row keyed `(source='sec_10q', accession_number)`. `raw_status` transitions `pending → parsed` (the no-op shortcuts `fetched` since no fetch occurs).

No conditional-GET. The synth no-op parser holds zero fetch budget regardless.

## 3. Retry posture

There is no retry posture — the parser cannot fail. Body of `_parse_sec_10q` is a single `return ParseOutcome(status='parsed', parser_version='10q-noop-v1')` (`sec_10q.py:85-88`).

- No `tombstoned` branch — no failure mode requires permanent discard.
- No `failed` branch — no DB write that can raise; no fetch that can raise.
- `requires_raw_payload=False` (`sec_10q.py:101`) — the worker accepts `parsed` with `raw_status=None`.

SEC rate-limit pool: **unused by the parser**. Layer 1/2/3 discovery still spends budget; the dispatch path does not.

## 4. Bootstrap path

**No dedicated stage.** Companyfacts XBRL coverage rides Stage 9 `sec_companyfacts_ingest` (`app/services/bootstrap_orchestrator.py:1052`) + Stage 25 `fundamentals_sync` (line 1175). The `sec_10q` manifest source is drained passively as Layer 1/2/3 discovery populates rows — the synth no-op marks them `parsed` on the next worker tick.

The dispatch-layer drain is the only `sec_10q`-specific work that runs at bootstrap; everything else (financial-statement extraction, period normalization, treasury observations) happens on the Companyfacts XBRL path.

## 5. Steady-state path

Manifest worker drains discovered 10-Q rows at its standard tick interval. Steady-state Companyfacts ingest fires daily via `JOB_FUNDAMENTALS_SYNC` (`scheduler.py:616`, cadence 02:30 UTC — ~30 min after SEC's nightly XBRL publish window per `scheduler.py:626-629`).

Discovery cadence: Layer 1 Atom 5 min (`app/jobs/sec_atom_fast_lane.py:104`) + Layer 2 daily-index (`app/jobs/sec_daily_index_reconcile.py:46`) + Layer 3 per-CIK poll bounded by `cadence_for('sec_10q') = 60d` (`app/jobs/sec_per_cik_poll.py:39`).

## 6. Manifest insert

Row inserted at discovery via `record_manifest_entry`. Shape:

- `source = 'sec_10q'` (per `ManifestSource` Literal at `app/services/sec_manifest.py:106-122`).
- `subject_type = 'issuer'`, `subject_id = issuer CIK`.
- `accession_number` = SEC-assigned accession.
- `primary_document_url` populated at discovery — **the synth no-op parser does not consume it** (`sec_10q.py:68` `conn` and `row` both unused beyond `accession_number` for the debug log line).

No Option C `filed_at` gate at the dispatch layer — there is no writer to gate.

## 7. Parser

Module `app/services/manifest_parsers/sec_10q.py`. Function `_parse_sec_10q` (line 67). Version `_PARSER_VERSION_10Q = "10q-noop-v1"` (line 64). Registered at `register()` (line 91) with `requires_raw_payload=False`.

**Synth no-op pattern** per `.claude/skills/data-sources/sec-edgar.md §11.5.1` — the canonical fix for sources whose SQL coverage is complete via another path. `sec_10q.py` is the canonical exemplar of the pattern; `sec_xbrl_facts.py` is the second adopter.

The manifest discovery row IS the audit signal for this source. The parser body is:

```python
return ParseOutcome(status='parsed', parser_version='10q-noop-v1')
```

No fetch, no DB write, no exception path. The worker transitions the row to `parsed` on next tick.

## 8. Observation insert

**None.** No `*_observations` table for 10-Q narrative text — there is no v1 consumer for MD&A / risk-factors / controls. Financial-statement data lands in `financial_facts_raw` via the Companyfacts XBRL path — see `sec_xbrl_facts.md` for the full source-to-sink chain.

If a future PR introduces an MD&A / risk-factor extraction consumer, that PR adds the fetcher + the `tests/test_fetch_document_text_callers.py` allow-list update + the SQL normalisation pathway in lockstep, per the "Every structured field lands in SQL" prevention contract (`sec_10q.py:18-21`).

## 9. Current table refresh

None. No write-through chain to refresh.

## 10. Operator-visible endpoint

10-Q text has no v1 endpoint. Financial-statement data exposed via:
- `GET /instruments/{symbol}/financials` (`app/api/instruments.py:608`) — sourced from `financial_periods` (Companyfacts XBRL chain).

10-Q **discovery** is visible via:
- `GET /coverage/manifest-parsers` — confirms `has_registered_parser=True` for `sec_10q`.
- Manifest worker stats: `WorkerStats.skipped_no_parser_by_source['sec_10q']` MUST stay at 0 (the synth no-op exists to keep this counter clean per `sec_10q.py:12-15`).

## 11. Verification queries

```sql
-- AAPL 10-Q manifest drain status
SELECT raw_status, COUNT(*) FROM sec_filing_manifest
 WHERE source='sec_10q' AND subject_type='issuer' AND subject_id='0000320193'
 GROUP BY raw_status;

-- Companyfacts coverage for AAPL (real audit signal)
SELECT taxonomy, COUNT(*) FROM financial_facts_raw
 WHERE instrument_id = (SELECT instrument_id FROM instruments WHERE symbol='AAPL')
 GROUP BY taxonomy;
```

Cross-source check: SEC EDGAR full-text search for AAPL 10-Q accessions should list the same `accession_number` values present in the manifest. Cross-check `financial_periods` against [gurufocus.com/stock/AAPL/financials](https://www.gurufocus.com/stock/AAPL/financials) — match on a single recent quarter.

Smoke command: `curl https://localhost/api/coverage/manifest-parsers | jq '.parsers.sec_10q.has_registered_parser'` — expect `true`.

## 12. Smoke test

`tests/smoke/test_etl_source_to_sink.py` parametrized row for `sec_10q`. Asserts: provider importable; `registered_parser_sources()` contains `sec_10q`; Stage 9 + 25 (Companyfacts upstream) exist; manifest worker stats has 0 `skipped_no_parser_by_source['sec_10q']`. The smoke test **skips** observation-table assertions since the synth no-op writes none.

## 13. Known gotchas

- **The manifest row IS the audit signal.** "No observation table" is by design, not a gap. Future code that adds an MD&A consumer must replace the no-op in lockstep with the fetcher + allow-list + SQL writer (per `sec_10q.py:18-21`).
- **Companyfacts XBRL covers the SQL surface.** Pre-#1168 designs that re-fetched 10-Q HTML at manifest dispatch were rejected by Codex pre-spec review (round 1 BLOCKING ×3 — fetch allow-list violation; raw persistence redundant; raise_for_status body-loss timing; per `sec_10q.py:43-52`).
- **Owner-attribution to #414 was stale.** #414 is the fundamentals_sync redesign, not a 10-Q parser ticket. Closed under #1168 (`.claude/skills/data-engineer/etl-endpoint-coverage.md` row G4).
- **WorkerStats counter contract.** `skipped_no_parser_by_source['sec_10q']` must stay at 0; if non-zero, the registration in `register_all_parsers` regressed.
- **No fetch budget.** The synth no-op does NOT call `SecFilingsProvider.fetch_document_text` — `sec_10q` exerts zero pressure on the `sec_rate` 10 req/s shared pool from the dispatch layer.
