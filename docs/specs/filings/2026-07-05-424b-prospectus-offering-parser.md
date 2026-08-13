# 424B prospectus offering parser (Rule 424(b) → structured)

Issue #1816 (child of #1015 item-2; item-1 NT 10-K/Q parser shipped PR1793).
Promotes the **tier-1 (equity-likely)** 424B subtypes from metadata-only to
PARSE+RAW: a new manifest source `sec_424b` that fetches the prospectus body and
best-effort-extracts the **cover-page offering disclosure** (Reg S-K Item
501(b)(3)) — gross offering size, price per security, underwriting discount,
issuer/selling-holder proceeds — into a structured table keyed to the filing.
Equity-vs-debt and issuer-vs-resale are read from the parsed cover, not inferred
from the subtype.

## Source rule

**Securities Act Rule 424(b)(1)–(8) (17 CFR 230.424(b))** governs the filing of
the final prospectus after a registration statement. The **subtype is a
filing-trigger / timing bucket, NOT an instrument taxonomy** — it does not by
itself tell us equity-vs-debt or issuer-vs-resale (Codex ckpt-1). The
authoritative economic facts come from the parsed cover disclosures, not the
subtype. Subtype paragraphs (17 CFR 230.424(b)(1)–(8)): B1 (prospectus not
previously filed / no 430A reliance), B2 (Rule 415 shelf/delayed offering with
pricing or security-description info — **covers equity AND debt/structured
notes**), B3 (prospectus reflecting a 424(b)(1)/(2) supplement), B4
(post-effective pricing prospectus, 430A), B5 (shelf prospectus supplement), B7
(selling-securityholder / Rule 430B info), B8 (late filing of a prospectus
otherwise required under another 424(b) paragraph — remains required under that
paragraph).

- **Reg S-K Item 501(b)(3) (17 CFR 229.501(b)(3))** requires, **"where you offer
  securities for cash,"** disclosure of **"Price to Public"**, **"Underwriting
  Discounts and Commissions"** (or "Selling Agent's Fees"), and **"Proceeds to
  Issuer"** (and, when applicable, "Proceeds to Selling Shareholders"), on a
  per-security and aggregate-total basis. The rule allows a **table, term sheet,
  or other clear presentation** — it does **NOT** mandate a table and is **NOT
  universally present** (non-cash / unit / at-the-market offerings may present it
  differently or omit it). It is therefore a **best-effort** extraction target,
  not a guaranteed one: when no resolvable presentation is found the money fields
  are NULL and the row still stores (see acceptance thresholds). Observed as a
  cover table on real filings: 424B4 (`0001193125-26-294982`, per-share "$49…")
  and 424B5 (JEF `0001140361-26-027261`, Public offering price / Underwriting
  discount / Proceeds-to clustered on the cover). Per-unit label varies ("Per
  Share" / "Per Note" / "Per Unit" / "Per ADS") — the extractor anchors on the
  **cluster of the three column labels**, not a fixed unit word, and tolerates
  non-tabular presentations degrading gracefully to NULL.

- **`is_issuer_offering` / `net_proceeds_to_issuer` are derived from the cover
  "Proceeds to Issuer" vs "Proceeds to Selling Shareholders" disclosures (Item
  501(b)(2)-(3)), NOT from the subtype.** A B7 (or mixed primary+resale) filing
  can still carry issuer proceeds; hard-zeroing from subtype alone is unsafe
  (Codex ckpt-1). Both fields are **nullable** — absent/unresolved → NULL, never
  a subtype-guessed value.

- **Reg S-K Item 504 (Use of Proceeds)** mandates a *section* but its content is
  **free-text prose** ("general corporate purposes", "repay indebtedness",
  "fund acquisitions"). Classifying it into a category is heuristic NLP over
  unstructured prose → **the prevention-log #1659 trap** (free-text disambiguation
  is not reliably machine-extractable). **NOT extracted** in this PR — see Deferred.

- **Reg S-K Item 506 (Dilution)** — free-text + a computed per-share dilution
  figure. A dilution/leverage *signal* would feed the scoring model → a
  `model_version` bump (operator-gated, same class as #1857/#1939/#1660).
  **Deferred** — this PR ships display/evidence data only, scoring-neutral.

## Premise check + full-population scoping (dev DB, 2026-07-05)

The issue's premise ("each row has `raw_payload_json` + `extracted_summary`") is
**partially falsified**: `extracted_summary` is **NULL for 0/193,424 rows**, and
`raw_payload_json` holds **metadata only** (`source, symbol, filed_at,
date_filed, filing_type, company_name, provider_filing_id`) — **no body text**.
The parser must fetch the prospectus `.htm` from `primary_document_url` (SEC
EDGAR, reachable from the loop env: 200 / 531 KB on a spot fetch).

Full-population subtype coverage + **filer composition**. The subtype split is a
**fetch-priority / yield heuristic** (which bodies to spend the rate-limited SEC
budget on first), NOT a legal equity-vs-debt classifier — that determination is
made per-filing from the parsed cover (Codex ckpt-1). Filer composition verified
on the full population (not a sample, per prevention #1659):

| subtype | n | fetch tier | why |
|---|---:|---|---|
| 424B2 | 149,555 | **deferred (tier-2)** | In OUR population, dominated by bank/ETN **structured-note shelf takedowns** — top filers JPM (30,268), MS (21,561), GS (17,668), BAC (15,009), C (14,820), BMO/RY/BNS/TD/DB/CM (Canadian/EU banks), VXX/DJP/NRGU (ETNs). Rule 424(b)(2) legally CAN carry equity, but 77% of volume here is debt/structured product with ~0 equity-dilution value. Deferred on **yield**, not by rule — revisitable via a targeted equity-issuer filter (a B2 backfill scoped to instruments whose base registration is equity is a child ticket). |
| 424B8 | 197 | **deferred (tier-2)** | Rule 424(b)(8) = late filing of a prospectus otherwise required under another paragraph; the underlying offering is generally captured by that other 424(b) row (linkage), so parsing B8 mostly duplicates. Deferred on low volume + duplication, revisitable. |
| 424B4 | 3,170 | **tier-1** | Post-effective pricing prospectuses — IPOs / follow-ons. Top filers small-cap equity (ABVC, NUWE, APVO, JRVR, INVH…). Highest signal density. |
| 424B5 | 17,916 | **tier-1** | Shelf prospectus supplements — ATM / follow-on raises. Highest tier-1 volume. |
| 424B3 | 20,991 | **tier-1** | Supplement prospectuses. |
| 424B1 | 14 | **tier-1** | Not-previously-filed prospectuses (rare). |
| 424B7 | 1,581 | **tier-1** | Selling-securityholder / mixed. Parsed; `is_issuer_offering` + proceeds **derived from the cover**, not assumed zero. |

**Tier-1 population ≈ 43,672 filings** (B1+B3+B4+B5+B7), vs 193k if naively
parsing everything. Deferring B2/B8 (149,752 mostly-structured-note rows) is why
this PR does NOT "move 424B to PARSE_AND_RAW" wholesale — only the tier-1
subtypes route to `sec_424b`; B2/B8 stay `SEC_METADATA_ONLY` pending a targeted
equity-filtered backfill. **Impl gate (prevention "verify on full population"):**
before declaring tier-1 done, a full-population dry-run over the ~43.7k tier-1
bodies must report parse-hit-rate + tombstone-rate **by subtype**; acceptance =
tombstone-rate within a documented threshold (e.g. ≤ the NT parser's observed
band) and no subtype silently at ~100% tombstone. A subtype that dry-runs mostly
un-parseable is itself a finding (its cover isn't the assumed shape) — file it,
don't ship a silent 0%-yield source.

## Extracted fields (`prospectus_offerings`)

| column | source | null? |
|--------|--------|-------|
| `accession_number` (PK) | manifest | no |
| `instrument_id` | manifest | no |
| `subtype` | manifest form (`424B4` … `424B7`) | no |
| `is_issuer_offering` (BOOL) | derived from cover: "Proceeds to Issuer" row present/non-zero ⇒ true; only "Proceeds to Selling Shareholders" ⇒ false | **yes** (unresolved → NULL; never subtype-guessed) |
| `price_per_unit` (NUMERIC) | Item 501(b)(3) "Price to Public" per-unit cell | yes (unparseable / range) |
| `unit_label` (TEXT) | the per-unit row label ("Per Share" / "Per Note" / "Per ADS" …) | yes |
| `aggregate_offering_amount` (NUMERIC) | Item 501(b)(3) "Price to Public" **total** column | yes |
| `underwriting_discount` (NUMERIC) | "Underwriting Discounts and Commissions" total | yes |
| `net_proceeds_to_issuer` (NUMERIC) | "Proceeds to Issuer" total (NULL when absent; NOT hard-zeroed from subtype) | yes |
| `proceeds_to_selling_holders` (NUMERIC) | "Proceeds to Selling Shareholders" total when disclosed | yes |
| `currency` (TEXT, CHECK enum) | detected from the table `$`/`€`/`£` glyph or explicit code; default `USD` | no |
| `security_type` (TEXT) | coarse label from the cover header ("Common Stock" / "Notes" / "Warrants" / "ADSs") — see note | yes |
| `parser_version` (INT) | const | no |
| `parsed_at` (TIMESTAMPTZ) | NOW() | no |

Every numeric is **nullable**: when the cover table can't be resolved
unambiguously the field is NULL, never a guessed value (mirrors the NT parser's
nullable-checkbox discipline). A row with all-NULL money fields but a stored raw
body is a valid outcome (records "an offering happened" + preserves the body for
audit) — it is NOT a parse failure/tombstone.

`security_type` is a **coarse, best-effort** label off the cover title, not a
taxonomy — it is advisory only, and drives no semantic flag. `is_issuer_offering`
is derived from the cover's proceeds rows (above), not from `security_type` and
not from the subtype.

`aggregate_offering_amount` (gross) is preferred over any body-computed total;
when only the per-unit price + share count are present and the total is absent,
the total is left NULL (do NOT multiply — share counts on the cover are
frequently "excluding over-allotment", and inferring the total would fabricate a
figure the source didn't state).

## Deferred (explicit — not silently dropped)

1. **Use-of-proceeds category** (Item 504) — free-text prose; heuristic
   classification is the prevention #1659 trap. If wanted later, do it as an
   LLM-extraction child ticket with a confidence field, NOT a keyword heuristic.
2. **Dilution / leverage signal** (Item 506) — would feed scoring →
   `model_version` bump (operator-gated). Ship display data first; a signal is a
   separate operator-gated ticket (sibling of #1857/#1660).
3. **B2/B8 parsing** — deferred on yield (not by rule; see scoping). A targeted
   B2 backfill scoped to equity-base-registration issuers is a child ticket; if a
   consumer ever needs structured-note terms, that is its own feature with its own
   source rule (MTN / structured-product term sheets).

## Schema

- `sql/216_prospectus_offerings.sql` — new table; PK `accession_number`, indexed
  `instrument_id` (BIGINT), columns above. Mirrors `nt_filing_notices` shape
  (accession-keyed, FK-free instrument_id). The SAME migration widens the
  source/kind CHECK constraints for the new source (each verified present on dev
  DB before widening — prevention "grep CREATE+ALTER constraints"):
  - `filing_raw_documents_document_kind_check` += `'prospectus_body'`.
  - `sec_filing_manifest_source_check` += `'sec_424b'`.
  - `data_freshness_index_source_check` += `'sec_424b'`.
  - a `prospectus_offerings_currency_check` enum CHECK (`USD`/`EUR`/`GBP`/`CAD`/…).
  Prospectus bodies are large (100 KB–8 MB; the B4 sample was 8.2 MB — it bundles
  full financial statements) → the `prospectus_body` `DocumentKind` is added to
  `SWEPT_DOCUMENT_KINDS` (chokepoint 8): do NOT retain the bodies (contrast NT
  bodies, which are tiny and kept). Store the raw only long enough to parse, then
  sweep; the structured row is the durable artifact.

## Parser

- `app/services/prospectus_offerings.py` — **pure** extractor
  `parse_prospectus_offering(html, subtype) -> ProspectusOffering | None`
  (None ⇒ no recognizable Item 501(b)(3) cover table ⇒ tombstone with raw kept
  through the sweep window) + `upsert_prospectus_offering(conn, instrument_id,
  accession, offering)`. Pure-fn so it table-tests against real fixtures with no
  DB. Extraction strategy: locate the cover-page pricing table by the cluster of
  the three Item 501(b)(3) column headers ("Price to Public" / "Underwriting
  Discount(s)…" / "Proceeds to…", tolerant of the standard label variants),
  parse the aligned per-unit + total cells, normalize currency from the glyph.
  Robustness rules (grounded on the two real fixtures, then validated by the
  **full-population tier-1 dry-run** described in the scoping section — NOT a
  small slice, per prevention "verify on the full population"): tolerate the
  TOC/running-header duplicate headings (anchor on the header CLUSTER + adjacent
  numeric cells, not the first heading hit); tolerate `$`, thousands separators,
  and footnote superscripts in money cells; range prices ("$8.00–$10.00") → NULL;
  non-tabular / absent Item 501(b)(3) presentation → money fields NULL, row still
  stored (not a tombstone unless the body is not a recognizable prospectus at all).
- `app/services/manifest_parsers/sec_424b.py` — `_parse_424b(conn, row) ->
  ParseOutcome` mirroring `sec_nt.py`/`eight_k.py`: pre-fetch gates (missing
  url/instrument_id → tombstone) → fetch via
  `SecFilingsProvider.fetch_document_text` → empty/non-200 → tombstone,
  fetch-exception → failed(retry) → `store_raw(document_kind='prospectus_body')`
  in a savepoint → parse → None → tombstone → else `upsert_prospectus_offering`.
  `_424b_fetch_url` prefetch hook + `register("sec_424b", …,
  requires_raw_payload=True)`. Registered in `manifest_parsers/__init__.py`.

  **`raw_status` invariant (#938):** once `store_raw` succeeds, EVERY subsequent
  outcome (parse-None tombstone, parse exception, upsert failure) MUST carry
  `raw_status="stored"`, never `absent` — same as the NT/8-K parsers.

## Chokepoint registrations (source-to-sink smoke `tests/smoke/test_etl_source_to_sink.py`)

Every new `ManifestSource` must be wired at each chokepoint or the smoke fails
(checklist proven by the NT parser PR1793):

1. `app/services/filings.py` — **only** the in-scope subtypes (`424B1`, `424B3`,
   `424B4`, `424B5`, `424B7`) move `SEC_METADATA_ONLY` → `SEC_PARSE_AND_RAW`.
   `424B2`, `424B8` stay `SEC_METADATA_ONLY` (documented inline with the
   structured-note rationale).
2. `app/services/sec_manifest.py` — `ManifestSource` Literal += `"sec_424b"`;
   `_FORM_TO_SOURCE` += the five in-scope subtypes → `sec_424b`.
3. `app/services/capability_manifest_mapping.py` — add `sec_424b` to
   `_UNMAPPED_MANIFEST_SOURCES`. **Decision:** offerings are **episodic events**
   (not every instrument issues; no standing per-instrument coverage expectation),
   exactly the NT-parser rationale — an episodic signal, not a standing
   data-coverage capability. Documented inline. (Not left to impl — Codex ckpt-1.)
4. `app/services/data_freshness.py` — episodic (offerings are event-driven);
   generous staleness cap so a non-issuing name never reads "stale".
5. `app/services/processes/param_metadata.py` — `sec_rebuild` source enum +=
   `sec_424b`.
6. `app/jobs/sec_first_install_drain.py` — bodies are **large** → seed
   `initial_ingest_status="deferred"` (like the heavy 10-K/8-K bodies, NOT
   pending like tiny NT bodies).
7. `scripts/_etl_source_inventory.py` — extend the sink-kind taxonomy with a new
   `prospectus_offering` kind; `MANIFEST_SOURCE_SINKS["sec_424b"] =
   (("prospectus_offerings",), "prospectus_offering")`.
8. `app/services/raw_filings.py` — `DocumentKind` Literal += `"prospectus_body"`,
   and add it to **`SWEPT_DOCUMENT_KINDS`** (raw_filings.py:95, canonical since
   #1615 — retention is classed by DocumentKind; `store_raw` at raw_filings.py:251
   gates the sweep on this set). Every `DocumentKind` must be classed swept-or-kept
   or `test_every_document_kind_is_classified` fails — bodies are large (100 KB–8
   MB) → **swept**, NOT `KEPT_NEGLIGIBLE_DOCUMENT_KINDS`. (Codex ckpt-1: the source
   sweep list is not the classifier — `SWEPT_DOCUMENT_KINDS` is.)
9. `tests/test_fetch_document_text_callers.py` — `_ALLOWED_CALLER_FILES` +=
   `"app/services/manifest_parsers/sec_424b.py"`.
10. `tests/smoke/test_etl_source_to_sink.py` — `_PARSER_MODULE_BY_SOURCE` +=
    `sec_424b` → its parser module.
11. `docs/etl/sources/sec_424b.md` — the 13 required source-doc sections.
12. `app/services/processes/bootstrap_coverage.py` — `sec_424b` is **NOT**
    bootstrap-covered (it is not bulk-seeded by a bootstrap stage; going-forward
    discovery + the child-ticket backfill drive seed it). Leave it out of
    `BOOTSTRAP_COVERED_FRESHNESS_SOURCES` (uncovered, like other episodic
    manifest sources) so a never-run poll doesn't read a false green — mirror the
    NT treatment; documented inline. (Codex ckpt-1.)

## Operator surface

`prospectus_offerings` LEFT JOINed in `GET /filings/{instrument_id}`
(`app/api/filings.py`): for in-scope 424B rows attach an `offering` object
(`subtype`, `aggregate_offering_amount`, `net_proceeds_to_issuer`,
`price_per_unit`, `unit_label`, `security_type`, `is_issuer_offering`). A
dedicated FE "capital actions" panel is a **separate FE child ticket** (backend
is the unit of work here). No scoring surface touched (scoring-neutral).

## Backfill (split)

Going-forward discovery is wired by this PR (`_FORM_TO_SOURCE` → submissions
ingest seeds new in-scope 424B rows). The historical **~43.7k in-scope backfill**
(seed manifest from existing `filing_events` + drain at 10 req/s shared — bodies
are large, so this is a multi-hour drive) is a **child ticket** "424B offering
backfill drive", mirroring the NT split (#1174/#1176). For DoD clauses 8–11 this
PR seeds + drains a **bounded sample** (a recent B4/B5 slice + any panel name
with a 424B) on dev, verifies the `/filings/{id}` figure renders, and does one
cross-source spot-check (SEC EDGAR cover table direct).

## Tests

- `tests/test_prospectus_offerings_parser.py` — pure: the B4 + B5 real fixtures
  (assert the three Item 501(b)(3) totals + per-unit price + currency); a B7
  resale-only fixture (cover shows only "Proceeds to Selling Shareholders") →
  `is_issuer_offering=false`, `net_proceeds_to_issuer=NULL`,
  `proceeds_to_selling_holders` set; a mixed primary+resale cover → both proceeds
  set + `is_issuer_offering=true` (proving the flag is cover-derived, NOT
  subtype-derived); a range-price cover → `price_per_unit=NULL`; a non-tabular /
  Item-501(b)(3)-absent prospectus body → money fields NULL but row stored (NOT a
  tombstone); a body that is not a recognizable prospectus → None (tombstone); a
  `€`/`£` cover → currency detected; a total-absent/per-unit-only cover →
  `aggregate_offering_amount=NULL` (no multiply-to-fabricate).
- `tests/test_manifest_parser_sec_424b.py` — integration mirroring
  `test_manifest_parser_sec_nt.py`: happy path (fetch→store_raw→parse→upsert),
  empty fetch → tombstone, fetch exception → failed+retry, parse-None →
  tombstone, all with `raw_status="stored"` after store_raw.

## Settled decisions / prevention log

- Settled "SEC EDGAR is the official filings source for US issuers" + "prefer the
  official filing" — preserved (parsing the authoritative SEC prospectus).
- Prevention #1659 (free-text prose is not reliably machine-extractable) — drives
  the deferral of use-of-proceeds classification + dilution signal; only the
  **Item 501(b)(3) mandated table** (structured) is extracted.
- Prevention "verify the signal on the full population, not a sample" — the
  B2/B8-exclusion scope is grounded in the full-population filer composition
  (149,752 rows), not a sample.
- Prevention "single chokepoint discipline" — extraction lives in the pure
  `prospectus_offerings.parse_prospectus_offering`; the manifest wrapper only
  orchestrates fetch/store/transition.
- Prevention "new ManifestSource must register at every chokepoint" — the
  source-to-sink smoke is the enforced checklist (above).
- Scoring-neutral by construction (no `model_version` bump) — the dilution signal
  that WOULD bump it is explicitly deferred.
```
