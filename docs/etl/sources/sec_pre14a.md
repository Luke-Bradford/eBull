# sec_pre14a — SEC PRE 14A / PRER14A meeting-agenda proposal signals

Manifest source for SEC preliminary proxy meeting-agenda proposals. Issue
#1892 (#1015 item 3). Upgrades PRE 14A / PRER14A from metadata-only to
PARSE+RAW: fetches the proxy body and extracts the Rule 14a-4(a)(3) numbered
"purposes" / "items of business" agenda list, classifying each proposal
against three categories for LLM/thesis consumption.

## 1. Origin

SEC EDGAR. Form PRE 14A (preliminary proxy statement) / PRER14A (revised
preliminary proxy statement), filed under Regulation 14A ahead of the
definitive DEF 14A. Discovered via the same per-CIK submissions poll / daily
index / Atom feeds as every other SEC form; mapped to ``sec_pre14a`` in
``app/services/sec_manifest.py::_FORM_TO_SOURCE``. **Deliberately NOT** mapped
to ``sec_def14a`` (#1320) — see ``docs/etl/sources/sec_def14a.md`` §6. This is
a wholly separate source/table; the ownership pipeline still ingests only the
definitive DEF 14A.

## 2. Watermarking model

Per-accession, like every issuer-scoped SEC source: one
``sec_filing_manifest`` row keyed on ``accession_number``,
``subject_type='issuer'``, ``subject_id=instrument_id``. The manifest
``ingest_status`` FSM (pending → parsed / tombstoned / failed) is the
watermark; there is no period/offset cursor. PRE 14A is **episodic** — an
issuer files one (or occasionally two, if a preliminary revision is filed)
per proxy season, so ``data_freshness_index`` uses a generous 400-day
cadence ceiling (``app/services/data_freshness.py``), mirroring ``sec_nt``.

## 3. Retry posture

Transient fetch errors → ``failed`` with a 1-hour backoff
(``_failed_outcome`` in ``app/services/manifest_parsers/sec_pre14a.py``,
mirroring ``sec_nt``). Empty / non-200 body → ``tombstoned`` (nothing to
store). A body with no recognizable Rule 14a-4(a)(3) numbered proposals list
→ ``tombstoned`` with ``raw_status='stored'`` (the raw body is already
persisted). Parser / upsert exceptions after ``store_raw`` → ``failed`` with
``raw_status='stored'`` so the manifest's view never diverges from the raw
table (#938 invariant).

## 4. Bootstrap path

``app/jobs/sec_first_install_drain.py::seed_manifest_from_filing_events``
seeds PRE 14A / PRER14A manifest rows directly from existing
``filing_events`` rows (issuer-scoped, ``initial_ingest_status='pending'``)
— it picks up ``sec_pre14a`` automatically once the form is mapped, no
per-source branch needed. The manifest worker then drains them at the shared
10 req/s SEC rate. The historical full-population backfill is triggered via
``POST /jobs/sec_rebuild/run`` with ``{"source": "sec_pre14a"}``.

## 5. Steady-state path

New PRE 14A / PRER14A filings are discovered by the per-CIK submissions
poll, written to the manifest as ``pending`` (``record_manifest_entry`` via
``map_form_to_source``), and drained by ``run_manifest_worker`` on the normal
tick. No dedicated scheduler lane — PRE 14A shares the SEC manifest worker.

## 6. Manifest insert

``record_manifest_entry(..., source='sec_pre14a', subject_type='issuer',
subject_id=instrument_id, instrument_id=instrument_id, form='PRE 14A'|'PRER14A',
primary_document_url=...)``. Source allowed by
``sec_filing_manifest_source_check`` (widened in sql/211).

## 7. Parser

``app/services/manifest_parsers/sec_pre14a.py::_parse_pre14a`` orchestrates:
pre-fetch gates (unexpected form / missing url / missing instrument_id →
tombstone) → ``SecFilingsProvider.fetch_document_text`` →
``store_raw(document_kind='pre14a_body')`` in a savepoint →
``app.services.pre14a_proposals.parse_pre14a_proposals`` (pure extractor) →
``upsert_pre14a_proposal_signal``. All field logic lives in
``pre14a_proposals`` (single chokepoint). ``requires_raw_payload=True``
(#938).

Extraction anchors on the Notice of Meeting's numbered "purposes"/"items of
business" list (Rule 14a-4(a)(3)) — NOT a whole-document keyword scan, which
a 10-filing full-population check showed produces false positives from
risk-factor / contingency prose. Extracted fields: ``proposal_count``,
``agenda_items`` (raw numbered-item text, bounded per item, for LLM/thesis
consumption), and three boolean category flags derived from item-scoped
keyword matching: ``reverse_stock_split_proposal`` (Item 19 — charter
amendment), ``authorized_share_increase_proposal`` (Item 11 — share
authorization), ``say_on_pay_advisory_vote`` (Item 24 / Rule 14a-21(a) —
deliberately excludes the distinct Rule 14a-21(b) say-on-frequency vote).

## 8. Observation insert

PRE 14A is not an ownership/observation source — there is no observation
ladder. The parsed row lands directly in the typed current table (next
section). The manifest ``ingest_status`` carries the per-accession audit
state.

## 9. Current table refresh

``pre14a_proposal_signals`` (sql/211), one row per accession, upserted via
``upsert_pre14a_proposal_signal`` (``ON CONFLICT (accession_number) DO
UPDATE``). Tombstones live in the manifest, not in this table.

## 10. Operator-visible endpoint

``GET /filings/{instrument_id}`` (``app/api/filings.py``) LEFT JOINs
``pre14a_proposal_signals`` and attaches a ``pre14a_signal`` object
(``proposal_count``, the three category flags, ``agenda_items``) to PRE 14A
/ PRER14A rows.

## 11. Verification queries

```sql
-- parsed proposal signals for an instrument
SELECT accession_number, proposal_count, reverse_stock_split_proposal,
       authorized_share_increase_proposal, say_on_pay_advisory_vote
FROM pre14a_proposal_signals
WHERE instrument_id = (SELECT instrument_id FROM instruments WHERE symbol = 'AAPL')
ORDER BY parsed_at DESC;

-- manifest drain state for the source
SELECT ingest_status, raw_status, count(*)
FROM sec_filing_manifest WHERE source = 'sec_pre14a' GROUP BY 1, 2;
```

## 12. Smoke test

``tests/smoke/test_etl_source_to_sink.py`` covers ``sec_pre14a`` via the
parametrized source loop: registered parser (``registered_parser_sources``),
sink table (``pre14a_proposal_signals``) existence, ``_PARSER_MODULE_BY_SOURCE``
parity, and this doc's 13 sections.
``tests/test_manifest_parser_sec_pre14a.py`` drives the parser end-to-end
(fetch → store_raw → parse → upsert) with mocked fetch.
``tests/test_pre14a_proposals.py`` table-tests the pure extractor against
real EDGAR fixture bodies (fetched 2026-07 from Faraday Future, Earth Science
Tech, byNordic Acquisition Corp, GameStop, Home Depot, and Nerdy Inc PRE 14A
filings — six distinct intro-phrase / agenda-numbering renderings). Dev-DB
smoke panel (2026-07-03): GME, HD, FFAI, NRDY, SHAZ — substituted for the
project's standard AAPL/GME/MSFT/JPM/HD panel because AAPL/MSFT/JPM had no
PRE 14A / PRER14A filings in the dev DB window; GME and HD are the two
standard-panel members that do. All 5 parsed correctly; figures cross-checked
against the actual filed document text (GME: authorized-share-increase +
say-on-pay True, matching its real "Authorized Shares Amendment" + executive-
compensation advisory-vote proposals; HD: say-on-pay True, matching its real
"Say-on-Pay" item; NRDY: reverse-split True, matching its real
"reversestocksplitprelimpro.htm" filing).

## 13. Known gotchas

- **Agenda numbering format varies by filer/table markup** — real renderings
  seen: ``1. To approve...`` (period-terminated, Faraday); ``1 A proposal
  to...`` (bare number, HTML-table cell stripped to plain text, Earth Science
  Tech); ``(1) Elect...`` / ``(1) To elect...`` (parenthesized, GameStop /
  Home Depot). The extractor accepts all three via one regex with two
  alternatives; sequential-only acceptance (item N only counts if item N-1
  was already accepted, starting at 1) rejects stray digit matches.
- **The INTRO phrase varies just as much as the numbering** — a naive single
  phrase ("following proposals") missed 3 of 6 real fixtures during
  development: GameStop uses "you will be asked to:"; Home Depot uses a bare
  "ITEMS OF BUSINESS" heading (no "following" prefix, no colon) directly
  followed by a proposal/recommendation/page-number table collapsed to
  numbered text; Nerdy Inc uses the singular "the purpose ... is the
  following:" (not the plural "following purposes"). The intro search is
  bounded to the first 15,000 characters of stripped text (``_INTRO_SEARCH_CHARS``)
  so broadening the phrase set doesn't reopen the whole-document
  false-positive class the full-population check (#1892) exists to avoid —
  the Notice of Meeting always opens a proxy per SEC convention.
- **A "more fully described" / similar summary clause can appear in the
  INTRO sentence, before any numbered item** (Earth Science Tech fixture:
  "...following items of business, which are more fully described in this
  proxy statement. Proposals 1 ..."). The end-anchor search only starts
  AFTER item 1 is located, so this doesn't truncate the block to zero items.
- **"Table of Contents" often repeats as a running page header/footer**
  throughout the document (byNordic fixture) — used as an end anchor, but
  only the first occurrence AFTER the located item-1 start is honored, else
  a pre-intro TOC nav link would end the block before it starts.
- **Say-on-pay vs say-on-frequency are distinct Rule 14a-21 provisions.**
  ``say_on_pay_advisory_vote`` matches Rule 14a-21(a) phrasing ("advisory
  vote on/to approve executive compensation") and deliberately does NOT
  match the Rule 14a-21(b) frequency vote ("approve the frequency of future
  votes on executive compensation") — different proposal, not requested by
  #1892.
- **Whole-document keyword scans produce false positives** (10-filing
  full-population check, #1892): "authorized shares of common stock" often
  appears in risk-factor / contingency prose unrelated to any proposal.
  Classification is scoped to the numbered agenda-item text only, never the
  whole document.
- **Contested board elections are explicitly out of scope** (checked
  "dissident"/"opposition" keyword hits in the sample — both were boilerplate
  advance-notice-bylaw disclosure). SEC EDGAR has dedicated form types for
  this (``PREC14A``, ``PRRN14A``, ``DFAN14A``, ``DEFC14A``); a future ticket
  should detect contested proxies via those form codes existing for an
  issuer, not via text classification of a routine PRE 14A.
