# sec_tender — SEC tender / going-private schedules

Manifest source for the tender-offer / going-private Schedule family. Issue
#1982 (child of #1015 item 4). Upgrades SC TO-T / SC TO-I / SC 14D9 / SC 13E3
(+ /A) from metadata-only to PARSE+RAW: fetches the primary document + the
EDGAR SGML filing header and extracts the Reg M-A cover disclosures into
`tender_offer_events` — one row per (accession, in-universe party), with
`role` (subject vs offeror) derived from the header CIK blocks.

## 1. Origin

SEC EDGAR. Governing rules: **Schedule TO** (17 CFR 240.14d-100) filed under
Rule 14d-1(g) (third-party tender, SC TO-T) or Rule 13e-4 (issuer self-tender,
SC TO-I); **Schedule 14D-9** (17 CFR 240.14d-101, Rule 14d-9) — the subject
company's solicitation/recommendation statement; **Schedule 13E-3** (17 CFR
240.13e-100, Rule 13e-3) — going-private transaction statement. Content items
per **Reg M-A** (17 CFR 229.1000–1016). Discovered via the same per-CIK
submissions poll / daily index / Atom feeds as every other SEC form; mapped to
``sec_tender`` in ``app/services/sec_manifest.py::_FORM_TO_SOURCE``.

| form | treatment |
|---|---|
| SC TO-T(/A), SC TO-I(/A), SC 14D9(/A), SC 13E3(/A) | PARSE+RAW (#1982) |
| SC TO-C | unmapped — Rule 14d-2(b)(1) pre-commencement communications; no Item 1004 terms attach |
| PREM14C / DEFM14C | metadata-only — Schedule 14A Item 14 prose (the #1659 free-text trap); the companion SC 13E3 carries the going-private signal |

**Attribution is header-derived, not manifest-derived.** A dual-party
accession (subject + offeror both in universe — 113 TO-T(/A) accessions in the
2026-07 population) appears in BOTH parties' ``filing_events`` via the
master-index path, but ``sec_filing_manifest`` is keyed on accession alone and
``record_manifest_entry`` is last-discovery-wins — so the manifest row's
``instrument_id`` is arbitrary between the parties. The parser fetches
``<acc>.hdr.sgml`` (same archive directory as the primary document) and maps
the ``SUBJECT-COMPANY`` / ``FILED-BY`` CIK blocks through
``external_identifiers`` to emit one typed row per matched instrument. A CIK
in both blocks (self-filed TO-I / 14D-9 / most 13E-3) collapses to
``role='subject'``.

## 2. Watermarking model

Per-accession, like every issuer-scoped SEC source: one ``sec_filing_manifest``
row keyed on ``accession_number``, ``subject_type='issuer'``. The manifest
``ingest_status`` FSM (pending → parsed / tombstoned / failed) is the
watermark; no period/offset cursor. Tender events are **episodic** — most
instruments are never a tender party — so ``data_freshness_index`` uses a
generous 400-day cadence ceiling (``app/services/data_freshness.py``) purely
to avoid painting a never-tendered name "overdue".

## 3. Retry posture

Transient body/header fetch exceptions → ``failed`` with a 1-hour backoff
(``_failed_outcome`` in ``app/services/manifest_parsers/sec_tender.py``,
mirroring ``sec_424b``). Empty / non-200 body → ``tombstoned`` (nothing to
store). Empty / non-200 **header** after a stored body → ``tombstoned`` with
``raw_status='stored'`` (a missing header on a live accession is not
transient, and no role row can be derived without it). Unusable header blocks
/ unrecognizable body → ``tombstoned`` with ``raw_status='stored'``. Parse /
upsert exceptions after ``store_raw`` → ``failed`` with ``raw_status='stored'``
(#938 invariant). Zero header parties mapping to in-universe instruments →
``tombstoned`` (event concerns nothing we track).

A recognizable schedule with unresolved cover fields is **NOT a tombstone**:
it upserts role rows with NULL extracted fields — "a tender event exists,
parties known" is itself the thesis signal.

## 4. Bootstrap path

**NOT bootstrap-covered** (excluded from
``app/services/processes/bootstrap_coverage.py``, like ``sec_nt`` /
``sec_pre14a`` / ``sec_424b``): going-forward discovery + the #1982 backfill
seed it. ``app/jobs/sec_first_install_drain.py`` seeds pre-existing schedule
``filing_events`` rows as ``pending`` (NOT deferred — ~1.8k bodies drain in
minutes, like NT; unlike the 100 KB-12 MB 424B tier). The historical backfill
has no dedicated drive ticket: ``scripts/backfill_1982_sec_tender.py`` seeds
from ``filing_events`` and drains inline (population ≈ 1,783 accessions ≈
3,600 requests — well under an hour at the shared 10 req/s).

## 5. Steady-state path

New schedule filings are discovered by the per-CIK submissions poll, written
to the manifest as ``pending`` (``record_manifest_entry`` via
``map_form_to_source``), and drained by ``run_manifest_worker`` on the normal
tick. No dedicated scheduler lane.

## 6. Manifest insert

``record_manifest_entry(..., source='sec_tender', subject_type='issuer',
subject_id=<either party's instrument_id>, form='SC TO-T'|'SC TO-T/A'|'SC
TO-I'|'SC TO-I/A'|'SC 14D9'|'SC 14D9/A'|'SC 13E3'|'SC 13E3/A',
primary_document_url=...)``. Source allowed by
``sec_filing_manifest_source_check`` (widened in sql/224). The row's
``instrument_id`` is a discovery artifact — roles come from the header at
parse time (§1).

## 7. Parser

``app/services/manifest_parsers/sec_tender.py::_parse_tender`` orchestrates:
pre-fetch gates (unexpected form / missing url → tombstone) →
``SecFilingsProvider.fetch_document_text`` (body, then ``<acc>.hdr.sgml`` —
URL derived from the primary document's archive directory, verified live) →
``store_raw(document_kind='tender_body')`` in a savepoint (born-compacted, sha
only — §13) → ``app.services.tender_offers.parse_tender_offer`` (pure
extractor) → ``map_ciks_to_instruments`` → ``upsert_tender_offer_events``. All
field logic lives in ``tender_offers`` (single chokepoint).
``requires_raw_payload=True`` (#938). The header is parsed in-memory and never
stored raw (1–2 KB, rehydratable; its durable facts land as ``subject_cik`` /
``offeror_names`` columns).

Extracted fields (Reg M-A):

- **Parties/role** — SGML header ``SUBJECT-COMPANY`` / ``FILED-BY`` blocks.
- **Transaction-type checkboxes** (Schedule TO cover): third-party (Rule
  14d-1), issuer (Rule 13e-4), going-private (Rule 13e-3), amends-13D,
  final-amendment. Label-anchored, checked-anywhere-wins on duplicate labels,
  glyphs ``☒``/``☐`` AND legacy ``x``/``¨``; unresolvable ⇒ NULL. 14D-9 and
  13E-3 have no Schedule TO boxes ⇒ all four NULL (never form-inferred).
- **Offer price + unit + currency** — Item 1004(a)(1)(ii) consideration via
  the anchored formulas ("for $124.00 per Share, net ... in cash" / "purchase
  price of $3.15 per Share"), scanned over the front 15k chars only (a long
  14D-9's Item 4 background recounts superseded bids deeper), with a hard
  ``par value`` exclusion window. Conflicting distinct amounts ⇒ NULL.
- **Expiration** — Item 1004(a)(1)(v) via "offer ... expire ... on <date>".
- **Board recommendation** (14D-9 only) — Item 1012(a)'s reg-enumerated
  4-state vocabulary (accept / reject / neutral / unable); unmatched prose ⇒
  NULL. Multiple distinct positions matching ⇒ NULL.
- **Class title / CUSIP / amendment no.** — cover captions.

Transaction value is NEVER computed (price × shares would fabricate a figure
— fee-table share counts drift; same rule as 424B's no-multiply).

## 8. Observation insert

Not an ownership/observation source — no observation ladder. Parsed rows land
directly in the typed table; the manifest carries the audit state.

## 9. Current table refresh

``tender_offer_events`` (sql/224), one row per (accession, instrument),
upserted via ``upsert_tender_offer_events`` (``ON CONFLICT (accession_number,
instrument_id) DO UPDATE``). Amendments are their own rows (form carries
``/A``); the amendment stream IS the signal evolution (price bumps,
extensions, the ``is_final_amendment`` results row). Readers wanting "current
state" take the latest row per instrument. Tombstones live in the manifest.

## 10. Operator-visible endpoint

``GET /filings/{instrument_id}`` (``app/api/filings.py``) LEFT JOINs
``tender_offer_events`` **on (accession, instrument)** — composite, so a
dual-attributed accession renders ``role='subject'`` on the target's feed and
``role='offeror'`` on the acquirer's — and attaches a ``tender`` object
(``role``, ``subject_company_name``, ``offeror_names``, checkbox booleans,
``offer_price_per_unit``, ``unit_label``, ``currency``, ``expiration_date``,
``board_recommendation``, ``amendment_no``, ``is_final_amendment``). FE render
on the Filings tab is a follow-up child ticket (the #1978 OfferingBlock
pattern). Scoring-neutral — no ``model_version`` bump.

## 11. Verification queries

```sql
-- tender events for an instrument (both roles)
SELECT accession_number, form, role, offer_price_per_unit, unit_label,
       board_recommendation, is_final_amendment, amendment_no
FROM tender_offer_events
WHERE instrument_id = (SELECT instrument_id FROM instruments WHERE symbol = 'GSK.US')
ORDER BY parsed_at DESC;

-- manifest drain state for the source
SELECT ingest_status, raw_status, count(*)
FROM sec_filing_manifest WHERE source = 'sec_tender' GROUP BY 1, 2;

-- dry-run acceptance shape: yield by form
SELECT form, count(*) AS rows,
       count(*) FILTER (WHERE role = 'offeror') AS offeror_rows,
       count(offer_price_per_unit) AS with_price,
       count(expiration_date) AS with_expiration,
       count(board_recommendation) AS with_recommendation
FROM tender_offer_events GROUP BY form ORDER BY form;
```

## 12. Smoke test

``tests/smoke/test_etl_source_to_sink.py`` covers ``sec_tender`` via the
parametrized source loop: registered parser, sink table
(``tender_offer_events``) existence, ``_PARSER_MODULE_BY_SOURCE`` parity, and
this doc's 13 sections. ``tests/test_manifest_parser_sec_tender.py`` drives
the adapter end-to-end (dual-party role rows, header-failure retry, unusable-
header tombstone) with mocked fetch; ``tests/test_tender_offers_parser.py``
table-tests the pure extractor against five real fixtures.

## 13. Known gotchas

- **``tender_body`` is SWEPT (born-compacted)**: mixed 13 KB–518 KB bodies,
  tiny population — sha only, bytes never stored. Re-drain / rewash re-fetches
  from EDGAR (~3 min for the full population at 10 req/s). The header is never
  stored at all.
- **The manifest row's instrument_id is arbitrary between the parties** of a
  dual-attributed accession (accession is the manifest PK;
  ``record_manifest_entry`` is last-discovery-wins). Never key role logic on
  it — the typed table resolves roles from the header.
- **Keep-list history**: before #1982, ``SEC_METADATA_ONLY`` carried the dead
  string ``DEF 13E-3`` (0 rows ever — EDGAR emits ``SC 13E3``) and SC TO-I /
  SC 13E3 were in NEITHER tier, so a ``filing_events_cleanup`` run would have
  deleted their filing_events rows. #1982 moved the real strings into
  ``SEC_PARSE_AND_RAW``.
- **Par value is the price false-positive** ("Common Stock, par value $0.01
  per share" — every cover); extraction anchors on the offer formulas and
  hard-excludes par-value contexts.
- **Checkbox glyph drift**: legacy filer agents still emit ``x``/``¨``
  (Wingdings) in 2026 (DSX amendments). NUVL's cover carries a filer typo
  (the third-party line appears twice, once checked once not — and the
  issuer-tender line is missing entirely): label-anchored,
  checked-anywhere-wins, absent-label ⇒ NULL.
- **A Schedule TO body with no priced formula is normal** (price incorporated
  by reference into the Offer-to-Purchase exhibit (a)(1)(A)) — price stays
  NULL; exhibit-chasing is a deliberate non-goal (spec Deferred §3).
