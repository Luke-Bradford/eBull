# 13D/G issuer-CUSIP extraction + resolution (#1628)

Blockholders (13D/G) never populate: `ownership_blockholders_current` = 0 despite ~150k filings + a working parser. Root cause is a parser tag bug, NOT a CIK-vs-CUSIP design decision.

## Root cause (empirically proven, acc 0001274173-26-000157 = CLRB)

The modern SEC Schedule 13 mandate XML (post-2024-12-18) carries the issuer CUSIP as:

```xml
<issuerInfo>
  <issuerCik>0001279704</issuerCik>
  <issuerCusips><issuerCusipNumber>15117F880</issuerCusipNumber></issuerCusips>
</issuerInfo>
```

Both parsers read STALE tag names:
- edgartools 5.30.2 `schedule13.py:191`: `child_text(issuer_el, 'issuerCUSIP')` → `''` (the documented edgartools drift cliff, #932/G15). CIK happens to extract OK.
- in-house `app/providers/implementations/sec_13dg.py::_extract_issuer` (`:333-334`): reads `issuerCIK` / `issuerCUSIP` — both wrong for the unified mandate schema (`issuerCik` / `issuerCusips/issuerCusipNumber`).

So `filing.issuer_cusip = ''` for every modern 13D/G → CUSIP-only resolution (`_parse_13dg:334`) returns None → observation write skipped → blockholders permanently empty.

Proof: direct XML parse gives `issuerCusipNumber=15117F880` → `external_identifiers` resolves to instrument 1049521; issuer CIK 0001279704 → same instrument.

## Why CUSIP, not "just use CIK" (settled #1102)

CUSIP = security (per-share-class); CIK = entity (settled-decisions "CIK = entity, CUSIP = security" #1102). 56 issuer CIKs in dev map to >1 instrument (GOOG/GOOGL-style share-class siblings). A 13D against GOOGL carries GOOGL's CUSIP; CIK-only resolution is ambiguous for those and would re-break the PR11 Codex-1b BLOCKING (routing GOOG-A 13D onto GOOGL-C). So CUSIP is the precise key; CIK is a single-class-only fallback.

## Fix

### 1. Correct-tag issuer extractor (pure)
New helper in `app/services/blockholders.py`:
`extract_issuer_identity_from_primary_doc(xml) -> IssuerIdentity(cik, cusip, name, class_title)` — namespace-agnostic walk reading the REAL tags `issuerCik`, `issuerCusipNumber`, `issuerName`, `securitiesClassTitle`. Single source of truth for the modern schema. Returns Nones for an absent/old-HTML body (defensive).

### 2. Backfill the manifest path (the drain)
`build_filing_from_edgartools_dict` gains a `raw_xml` arg; when edgartools returns empty `security_info.cusip` (always, today), backfill `issuer_cusip` from the helper. Issuer CIK also taken from the helper (zero-padded) so the filing's issuer identity no longer depends on edgartools' buggy issuer parse. edgartools still supplies reporting-persons + class title (those work).

### 3. Fix the rewash path
`_extract_issuer` (in-house parser, used by `rewash._apply_blockholders`) routes through the same helper.

### 4. Correct resolution: CUSIP-primary + single-class CIK fallback
Replace `_resolve_cusip_to_instrument_id(conn, cusip)` with
`_resolve_issuer_to_instrument_id(conn, *, cusip, cik) -> int | None`:
1. CUSIP → `external_identifiers WHERE provider IN ('sec','openfigi') AND identifier_type='cusip'`, ORDER BY is_primary DESC, external_identifier_id ASC (mirrors `_load_cusip_map` precedence + the approved OpenFIGI fallback, settled 2026-05-22). Precise, share-class-correct.
2. If unresolved AND `cik` present: `siblings = siblings_for_issuer_cik(conn, cik)`. If `len(siblings) == 1` → that instrument (single-class issuer — safe, broader coverage: 5,327 CIK mappings vs 2,927 sec CUSIP). If `len != 1` → None (multi-class with no resolvable CUSIP is genuinely ambiguous on share class — leave unresolved; never fabricate an attribution, never fan out: a holder owns ONE class).

Wire into all three callers: `_parse_13dg` (manifest), legacy `blockholders.py` ingest (`:668`), rewash `_apply_blockholders`.

### 5. parser_version bump → re-drain
Bump `_PARSER_VERSION_13DG` so the manifest flips `sec_13d`/`sec_13g` rows back to `pending` for re-parse through the fixed extractor (standard rewash trigger).

## Invariants preserved
- #1102 CUSIP=security / CIK=entity; multi-class needs CUSIP (don't re-break PR11).
- No fuzzy name/class matching (sec-edgar skill). securities_class_title is NOT used to guess a sibling.
- Manifest CHECK `subject_type='blockholder_filer' ⇒ instrument_id IS NULL` (prevention-log #1508) — unchanged; resolution stays at parse-time on the observation/`blockholder_filings` row, not the manifest row.
- ETF trust-CIK explosion (settled #1577) is N/A — ETFs hold no `(sec,cik)` row, so `siblings_for_issuer_cik` never returns a trust fan-out.
- Audit trail: CUSIP-unresolved-and-multi-class accessions still write `blockholder_filings` rows with `instrument_id=NULL` (status `partial`), as today.

## Tests
- Pure: `extract_issuer_identity_from_primary_doc` on a real-shape `<issuerCusips><issuerCusipNumber>` fixture → correct cik+cusip; on legacy `<issuerCUSIP>`/absent → Nones.
- Pure: `_resolve_issuer_to_instrument_id` — CUSIP resolves (precise); CUSIP-miss + single-class CIK → CIK instrument; CUSIP-miss + multi-class CIK → None; CUSIP-miss + no CIK → None. (DB-backed, minimal.)
- Adapter: edgartools-empty-cusip + raw_xml → filing carries the backfilled CUSIP.

## DoD (ETL clauses 8-12)
- Backfill: `POST /jobs/sec_rebuild/run {"source":"sec_13d"}` + `{"source":"sec_13g"}` on dev (parser_version bump auto-flips rows pending; re-drain through fixed extractor).
- Verify: `ownership_blockholders_current` > 0; `/instruments/{sym}/ownership-rollup` shows a blockholders wedge (deduped vs 13F by CIK, priority form4>form3>13d/g>def14a>13f). Panel incl. a known activist target.
- Cross-source: spot-check one issuer's 13D holder vs SEC EDGAR direct (e.g. the filing's reported % of class).
- Record commit SHA + figures per clause 12.

## Codex ckpt-1 — findings + resolutions (supersede above where noted)

- **HIGH — three parse paths, two extractors (corrects §2/§3).** Verified: (1) manifest `_parse_13dg` AND (3) rewash `_apply_blockholders` (rewash_filings.py:622-631) BOTH use edgartools + `build_filing_from_edgartools_dict`; (2) legacy `blockholders.py::_ingest_single_accession` (:646) uses in-house `parse_primary_doc`. So: fix the **adapter** (backfill from helper, given raw_xml) → covers manifest + rewash; fix the **in-house `_extract_13d`/`_extract_13g`** (via helper) → covers legacy ingest only. Rewash passes its stored body (`raw_doc` payload) into the adapter as `raw_xml`.
- **HIGH — deterministic provider precedence.** CLRB's `sec` CUSIP row is `is_primary=FALSE`, so `is_primary DESC` alone can let an `openfigi` row win on `external_identifier_id` tiebreak. Resolution ORDER BY = `CASE provider WHEN 'sec' THEN 0 ELSE 1 END, is_primary DESC, external_identifier_id ASC` (sec canonical).
- **MED — scope, don't replace.** Keep `blockholders._resolve_cusip_to_instrument_id` (cusip-only building block; other non-13D/G caller at blockholders.py:1001 untouched). ADD `_resolve_issuer_to_instrument_id(conn, *, cusip, cik)` and switch only the 3 13D/G callers (manifest :334, legacy :668, rewash :755).
- **MED — preserve non-empty edgartools identity.** Adapter: `issuer_cusip = helper_cusip or edgartools_cusip`; `issuer_cik = helper_cik or edgartools_cik` (helper preferred; edgartools retained when helper returns None on an old/odd shape — never NULL a valid parsed value).
- **MED — helper normalization.** CIK → strip + zero-pad-10 (None if non-numeric); CUSIP → `strip().upper()`, return None unless exactly 9 alphanumeric chars (CINS allowed — leading letter is alnum). Malformed → None (not poisoned logs / failed lookups).
- **MED — audit covers all unresolved.** `instrument_id=NULL` + log `partial` for EVERY unresolved case (CUSIP-miss + multi-class, + no-CIK, + zero-siblings) — not just multi-class. (Existing code already writes the row regardless; preserved.)
- **LOW — regression tests added:** provider precedence sec>openfigi; lowercase/whitespace CUSIP normalization; adapter `raw_xml=None`/helper-None preserves non-empty edgartools fields; manifest does NOT write `sec_filing_manifest.instrument_id` for `blockholder_filer` rows.
- **LOW — re-drain acceptance.** `sec_rebuild` flips parser-version-stale rows to `pending`; the manifest worker RE-FETCHES primary_doc.xml + re-parses (parse uses freshly-fetched XML, not the stored raw); `_record_ingest_attempt` upserts so old `partial` logs are overwritten with `success`.

## Out of scope
- Pre-2024-12-18 HTML 13D/G (retention floor; edgartools returns None — unchanged).
- Reporting-person field correctness in the in-house parser (rewash) beyond issuer identity — separate if needed.
- `blockholders.py:1001` (`holding.cusip`) non-13D/G resolver caller — untouched.
