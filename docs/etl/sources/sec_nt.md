# sec_nt — SEC Form 12b-25 late-filing notices (NT 10-K / NT 10-Q)

Manifest source for SEC late-filing notices. Issue #1015. Upgrades NT 10-K /
NT 10-Q from metadata-only to PARSE+RAW: fetches the Form 12b-25 body and
extracts the late-filing reason + anticipated-results-change signal.

## 1. Origin

SEC EDGAR. Form NT 10-K (late annual report) / NT 10-Q (late quarterly report);
both are **Form 12b-25** under Rule 12b-25 (17 CFR 240.12b-25). A registrant
unable to file a periodic report on time files Form 12b-25 within one business
day of the due date. Discovered via the same per-CIK submissions poll / daily
index / Atom feeds as every other SEC form; mapped to ``sec_nt`` in
``app/services/sec_manifest.py::_FORM_TO_SOURCE`` (``NT 10-K`` / ``NT 10-Q``).
The ``/A`` amendment variants and NT 20-F (foreign deadline regime) stay
metadata-only — out of scope.

## 2. Watermarking model

Per-accession, like every issuer-scoped SEC source: one
``sec_filing_manifest`` row keyed on ``accession_number``,
``subject_type='issuer'``, ``subject_id=instrument_id``. The manifest
``ingest_status`` FSM (pending → parsed / tombstoned / failed) is the
watermark; there is no period/offset cursor. NT filings are **episodic** — a
healthy filer never files one — so ``data_freshness_index`` uses a generous
400-day cadence ceiling (``app/services/data_freshness.py``) purely to avoid
painting NT "overdue" for instruments that simply have nothing to file.

## 3. Retry posture

Transient fetch errors → ``failed`` with a 1-hour backoff (``_failed_outcome``
in ``app/services/manifest_parsers/sec_nt.py``, mirroring ``eight_k``). Empty /
non-200 body → ``tombstoned`` (nothing to store). A body that is not a
recognizable Form 12b-25 (no ``12b-25`` marker) → ``tombstoned`` with
``raw_status='stored'`` (the raw body is already persisted). Parser / upsert
exceptions after ``store_raw`` → ``failed`` with ``raw_status='stored'`` so the
manifest's view never diverges from the raw table (#938 invariant).

## 4. Bootstrap path

``app/jobs/sec_first_install_drain.py::seed_manifest_from_filing_events`` seeds
NT manifest rows directly from existing ``filing_events`` NT rows (issuer-scoped,
``initial_ingest_status='pending'``) — it picks up ``sec_nt`` automatically once
the form is mapped, no per-source branch needed. The manifest worker then drains
them at the shared 10 req/s SEC rate. The historical full-population backfill
(~4 200 NT rows) is tracked as its own drive ticket (mirrors the sec_n_csr
parser/backfill split #1174/#1176).

## 5. Steady-state path

New NT filings are discovered by the per-CIK submissions poll, written to the
manifest as ``pending`` (``record_manifest_entry`` via ``map_form_to_source``),
and drained by ``run_manifest_worker`` on the normal tick. No dedicated
scheduler lane — NT shares the SEC manifest worker.

## 6. Manifest insert

``record_manifest_entry(..., source='sec_nt', subject_type='issuer',
subject_id=instrument_id, instrument_id=instrument_id, form='NT 10-K'|'NT 10-Q',
primary_document_url=...)``. Source allowed by
``sec_filing_manifest_source_check`` (widened in sql/208).

## 7. Parser

``app/services/manifest_parsers/sec_nt.py::_parse_nt`` orchestrates: pre-fetch
gates (unexpected form / missing url / missing instrument_id → tombstone) →
``SecFilingsProvider.fetch_document_text`` → ``store_raw(document_kind='nt_body')``
in a savepoint → ``app.services.nt_notices.parse_nt_notice`` (pure extractor) →
``upsert_nt_notice``. All field logic lives in ``nt_notices`` (single
chokepoint). ``requires_raw_payload=True`` (#938). ``late_form`` is derived from
the manifest form (authoritative), not the cover checkbox.

Extracted fields: ``period_of_report`` ("For Period Ended" / transition line),
``is_transition_report``, ``grace_period_days`` (15 for 10-K, 5 for 10-Q —
Rule 12b-25(b)), ``reason_text`` (Part III narrative),
``results_change_anticipated`` (Part IV item 3 Yes/No — nullable when the
checkbox encoding is ambiguous), ``results_change_explanation``.

## 8. Observation insert

NT is not an ownership/observation source — there is no observation ladder. The
parsed row lands directly in the typed current table (next section). The
manifest ``ingest_status`` carries the per-accession audit state.

## 9. Current table refresh

``nt_filing_notices`` (sql/208), one row per accession, upserted via
``upsert_nt_notice`` (``ON CONFLICT (accession_number) DO UPDATE``). Tombstones
live in the manifest, not in this table.

## 10. Operator-visible endpoint

``GET /filings/{instrument_id}`` (``app/api/filings.py``) LEFT JOINs
``nt_filing_notices`` and attaches a ``nt_notice`` object (``late_form``,
``period_of_report``, ``grace_period_days``, ``reason_excerpt``,
``results_change_anticipated``) to NT rows. The red-flag badge / trend already
render via ``filing_events.red_flag_score`` (0.7 for NT, set at ingest by
``filings_risk``). A dedicated "Problem panel" FE component is a separate
follow-up.

## 11. Verification queries

```sql
-- parsed NT notices for an instrument
SELECT accession_number, late_form, period_of_report,
       results_change_anticipated, left(reason_text, 120)
FROM nt_filing_notices
WHERE instrument_id = (SELECT instrument_id FROM instruments WHERE symbol = 'DOMO')
ORDER BY period_of_report DESC;

-- manifest drain state for the source
SELECT ingest_status, raw_status, count(*)
FROM sec_filing_manifest WHERE source = 'sec_nt' GROUP BY 1, 2;
```

## 12. Smoke test

``tests/smoke/test_etl_source_to_sink.py`` covers ``sec_nt`` via the parametrized
source loop: registered parser (``registered_parser_sources``), sink table
(``nt_filing_notices``) existence, ``_PARSER_MODULE_BY_SOURCE`` parity, and this
doc's 13 sections. ``tests/test_manifest_parser_sec_nt.py`` drives the parser
end-to-end (fetch → store_raw → parse → upsert) with mocked fetch.

## 13. Known gotchas

- **Checkbox encoding varies** (2016→2026): Unicode ``☒``/``☑``/``☐``, HTML
  entities, or ASCII ``x``/``X``; the box sits before OR after its Yes/No label.
  The extractor tolerates all of these and leaves ``results_change_anticipated``
  NULL when the state is ambiguous — never a guessed boolean.
- **``PART IV`` heading is unreliable** (~69% of bodies). Item (3) is anchored on
  the question text ("results of operations"), not the heading.
- **Part IV(3) is NOT a restatement field.** It is "anticipated significant
  change in results of operations vs the corresponding prior-year period" — an
  earnings-direction disclosure.
- **``nt_body`` is write-only** (KEPT_NEGLIGIBLE): the parser always re-fetches
  on re-drain; reuse-on-redrain is deferred by volume.
