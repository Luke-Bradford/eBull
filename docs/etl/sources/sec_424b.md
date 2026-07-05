# sec_424b — SEC 424B prospectus offerings

Manifest source for Rule 424(b) final prospectuses. Issue #1816 (child of
#1015 item 2). Upgrades the tier-1 (equity-likely) subtypes 424B1 / 424B3 /
424B4 / 424B5 / 424B7 from metadata-only to PARSE+RAW: fetches the prospectus
body and extracts the Reg S-K Item 501(b)(3) cover offering disclosure into
`prospectus_offerings`. #1975 adds 424B2 as **volume-gated** (see §1).

## 1. Origin

SEC EDGAR. Rule 424(b)(1)-(8) (17 CFR 230.424(b)) governs filing the final
prospectus after a registration statement; the **subtype is a filing-trigger /
timing bucket, NOT an instrument taxonomy** — equity-vs-debt and
issuer-vs-resale are read from the parsed cover, never inferred from the
subtype. Discovered via the same per-CIK submissions poll / daily index / Atom
feeds as every other SEC form; mapped to ``sec_424b`` in
``app/services/sec_manifest.py::_FORM_TO_SOURCE`` (424B1/B3/B4/B5/B7).

| subtype | treatment |
|---|---|
| 424B1 / B3 / B4 / B5 / B7 | PARSE+RAW (tier-1, #1816) |
| 424B2 | **volume-gated** PARSE+RAW (#1975) |
| 424B8 | metadata-only (late-filing duplicate of another 424(b) paragraph) |

**424B2 volume gate (#1975):** B2 is mapped to ``sec_424b``, but the parser's
pre-fetch gate tombstones a B2 row (error ``"424B2 volume cap: high-volume
structured-note filer"``) when the filer's lifetime B2 count in
``filing_events`` exceeds ``_424B2_VOLUME_CAP = 100``. Full-population scan
(149,555 B2 rows / 739 instruments, 2026-07-05): every filer above 100 is a
bank/ETN/credit vehicle (JPM 30,268 … PRU 106); the ≤100 tail is 718
instruments / ~4,252 filings. The cap is a **fetch-cost bound, not a
classification** — Rule 424(b)(2) covers equity and debt alike, and
equity-vs-debt is read only from the parsed Item 501(b)(3) cover. Evaluated
against the live ``filing_events`` horizon at parse time (self-updating, no
allowlist); deliberately non-idempotent across rebuilds — a filer crossing the
cap self-excludes on later re-drains. Gated rows cost one COUNT query and no
SEC request; the #1591 prefetch hook applies the same gate (parity — otherwise
the pipelined prefetcher would fetch bodies the parser refuses).

## 2. Watermarking model

Per-accession, like every issuer-scoped SEC source: one
``sec_filing_manifest`` row keyed on ``accession_number``,
``subject_type='issuer'``, ``subject_id=instrument_id``. The manifest
``ingest_status`` FSM (pending → parsed / tombstoned / failed) is the
watermark; there is no period/offset cursor. Offerings are **episodic** —
most issuers never file a 424B — so ``data_freshness_index`` uses a generous
400-day cadence ceiling (``app/services/data_freshness.py``) purely to avoid
painting the source "overdue" for instruments with nothing to file.

## 3. Retry posture

Transient fetch errors → ``failed`` with a 1-hour backoff (``_failed_outcome``
in ``app/services/manifest_parsers/sec_424b.py``, mirroring ``sec_nt``). Empty
/ non-200 body → ``tombstoned`` (nothing to store). A body that is not a
recognizable prospectus → ``tombstoned`` with ``raw_status='stored'``. Parse /
upsert exceptions after ``store_raw`` → ``failed`` with ``raw_status='stored'``
so the manifest's view never diverges from the raw table (#938 invariant).

A recognizable prospectus whose cover presentation can't be resolved is **NOT
a tombstone**: it upserts a row with NULL money fields (records "an offering
happened"). Item 501(b)(3) mandates the disclosure only "where you offer
securities for cash" and does not mandate a table — resale shelves,
percent-of-principal note covers, and non-tabular presentations legitimately
yield NULL-money rows.

## 4. Bootstrap path

**NOT bootstrap-covered** (left out of the covered-source classification in
``app/services/processes/bootstrap_coverage.py``, like ``sec_nt`` /
``sec_pre14a``): going-forward discovery + the child-ticket backfill drive
seed it, so a never-run poll doesn't read a false green.
``app/jobs/sec_first_install_drain.py`` seeds any pre-existing 424B
filing_events rows with ``initial_ingest_status='deferred'`` (bodies are 100
KB-12 MB — same weight class as 10-K/8-K, never eagerly drained). The
historical **~43.7k tier-1 backfill** is its own drive ticket (mirrors the NT
split #1174/#1176); its acceptance gate is a full-population dry-run reporting
parse-hit-rate + tombstone-rate **by subtype**.

## 5. Steady-state path

New tier-1 424B filings are discovered by the per-CIK submissions poll,
written to the manifest as ``pending`` (``record_manifest_entry`` via
``map_form_to_source``), and drained by ``run_manifest_worker`` on the normal
tick. No dedicated scheduler lane — 424B shares the SEC manifest worker.

## 6. Manifest insert

``record_manifest_entry(..., source='sec_424b', subject_type='issuer',
subject_id=instrument_id, instrument_id=instrument_id,
form='424B1'|'424B2'|'424B3'|'424B4'|'424B5'|'424B7',
primary_document_url=...)``.
Source allowed by ``sec_filing_manifest_source_check`` (widened in sql/216).

## 7. Parser

``app/services/manifest_parsers/sec_424b.py::_parse_424b`` orchestrates:
pre-fetch gates (unexpected form / missing url / missing instrument_id /
#1975 B2 volume cap → tombstone) → ``SecFilingsProvider.fetch_document_text`` →
``store_raw(document_kind='prospectus_body')`` in a savepoint (born-compacted:
sha only, bytes never stored — see §13) →
``app.services.prospectus_offerings.parse_prospectus_offering`` (pure
extractor, runs on the in-memory body) → ``upsert_prospectus_offering``. All
field logic lives in ``prospectus_offerings`` (single chokepoint).
``requires_raw_payload=True`` (#938). ``subtype`` comes from the manifest form
(authoritative).

Extracted fields (Reg S-K Item 501(b)(3), 17 CFR 229.501(b)(3)):
``price_per_unit`` + ``unit_label`` ("Price to Public" per-unit cell),
``aggregate_offering_amount`` (gross total; NEVER computed as price ×
share-count — cover counts often exclude over-allotment),
``underwriting_discount``, ``net_proceeds_to_issuer``,
``proceeds_to_selling_holders``, ``is_issuer_offering`` (issuer-proceeds row
present ⇒ true; only selling-holders ⇒ false; unresolved ⇒ NULL — never
subtype-guessed), ``currency`` (glyph-detected, default USD),
``security_type`` (coarse cover-title label, advisory only).

Three physical cover layouts are handled (grounded on the real-fixture panel +
a 219-body stratified dry-run): row-major (``label $per $total``),
column-major (labels first, then ``Per Note $a $b $c Total $A $B $C``), and
percent-of-principal (structured notes: ``100.00%`` with empty ``$`` cells →
money NULL, never fabricated).

## 8. Observation insert

424B is not an ownership/observation source — there is no observation ladder.
The parsed row lands directly in the typed current table (next section). The
manifest ``ingest_status`` carries the per-accession audit state.

## 9. Current table refresh

``prospectus_offerings`` (sql/216), one row per accession, upserted via
``upsert_prospectus_offering`` (``ON CONFLICT (accession_number) DO UPDATE``).
Tombstones live in the manifest, not in this table.

## 10. Operator-visible endpoint

``GET /filings/{instrument_id}`` (``app/api/filings.py``) LEFT JOINs
``prospectus_offerings`` and attaches an ``offering`` object (``subtype``,
``is_issuer_offering``, ``price_per_unit``, ``unit_label``,
``aggregate_offering_amount``, ``underwriting_discount``,
``net_proceeds_to_issuer``, ``proceeds_to_selling_holders``, ``currency``,
``security_type``) to tier-1 424B rows. A dedicated FE "capital actions"
panel is a separate child ticket.

## 11. Verification queries

```sql
-- parsed offerings for an instrument
SELECT accession_number, subtype, is_issuer_offering, price_per_unit,
       aggregate_offering_amount, net_proceeds_to_issuer, security_type
FROM prospectus_offerings
WHERE instrument_id = (SELECT instrument_id FROM instruments WHERE symbol = 'JEF')
ORDER BY parsed_at DESC;

-- manifest drain state for the source
SELECT ingest_status, raw_status, count(*)
FROM sec_filing_manifest WHERE source = 'sec_424b' GROUP BY 1, 2;

-- #1975: B2 rows retired by the volume cap (auditable tombstone reason)
SELECT count(*)
FROM sec_filing_manifest
WHERE source = 'sec_424b' AND ingest_status = 'tombstoned'
  AND error = '424B2 volume cap: high-volume structured-note filer';

-- money-fill rate by subtype (dry-run acceptance shape)
SELECT subtype, count(*) AS n,
       count(aggregate_offering_amount) AS with_aggregate,
       count(*) FILTER (WHERE is_issuer_offering IS NOT NULL) AS with_flag
FROM prospectus_offerings GROUP BY subtype ORDER BY subtype;
```

## 12. Smoke test

``tests/smoke/test_etl_source_to_sink.py`` covers ``sec_424b`` via the
parametrized source loop: registered parser, sink table
(``prospectus_offerings``) existence, ``_PARSER_MODULE_BY_SOURCE`` parity, and
this doc's 13 sections. ``tests/test_manifest_parser_sec_424b.py`` drives the
parser end-to-end (fetch → store_raw → parse → upsert) with mocked fetch;
``tests/test_prospectus_offerings_parser.py`` table-tests the pure extractor
against five real fixtures.

## 13. Known gotchas

- **``prospectus_body`` is SWEPT (born-compacted)**, unlike tiny ``nt_body``:
  bodies run 100 KB-12 MB (B4s bundle full financial statements), so
  ``store_raw`` records only the sha256 — bytes are never stored. The parser
  runs on the in-memory fetched body; re-drain always re-fetches from EDGAR.
- **Low money-fill on B3/B5/B7 is source reality, not extraction failure**
  (verified by spot-reading all-NULL bodies on the 219-body dry-run): most B3
  supplements are merger/resale documents with prose-only covers; many B5s are
  ATM/shelf prose or percent-of-principal note covers; B7 resale shelves often
  carry no pricing table. Dry-run fill rates: B4 78%, B1 36%, B5 17%, B3 2%,
  B7 10% — with 0 parse exceptions and 0 false tombstones.
- **Percent-of-principal covers** (structured notes) price as ``100.00%`` with
  empty ``$`` cells; a trailing ``$ 1`` footnote marker must not be read as
  money. The extractor rejects a row whose ``%`` precedes its first ``$``
  value.
- **Two pricing tables can coexist** (TD B3 carries a per-note table and a
  second table ~1k chars later): proceeds rows are chained by row pitch (~180
  chars), never swept across the whole cover region.
- **A price range ("$8.00 to $10.00") is not a priced cover** — per-unit and
  aggregate stay NULL rather than reading the bounds as two cells.
