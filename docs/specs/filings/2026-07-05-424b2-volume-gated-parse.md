# 424B2 volume-gated parse (targeted equity-issuer B2 → sec_424b)

Child of #1816 (parent spec: `2026-07-05-424b-prospectus-offering-parser.md`,
shipped PR #1973). Issue: #1975.

Goal: stop dropping equity shelf takedowns filed under Rule 424(b)(2) while
NOT fetching the ~140k bank/ETN structured-note B2 bodies that carry zero
equity-dilution signal.

## Source rule

- **Rule 424(b)(2)** (17 CFR 230.424(b)(2)) is a *filing-trigger* bucket — a
  prospectus reflecting final terms of a takedown off an effective shelf. The
  rule does **not** discriminate equity vs debt: industrials file
  investment-grade note takedowns as B2 (AAPL, JNJ, DE in our population) and
  small caps file equity raises as B2. There is therefore **no reg-level
  pre-classification available** — the *only* documented classifier for the
  offering's economics is the parsed cover per **Reg S-K Item 501(b)(3)**
  (17 CFR 229.501(b)(3)), exactly as the parent spec established.
- Consequence (parent-spec model, PR #1973 wording): **subtype = fetch
  priority, NOT taxonomy.** Any B2 filter we add is a *fetch-cost bound*, not
  a classification; classification continues to come from
  `parse_prospectus_offering` on the Item 501(b)(3) cover.

## Premise check + full-population scoping (dev DB, 2026-07-05)

`filing_events.filing_type = '424B2'`: **149,555 filings across 739
instruments.**

Per-instrument lifetime B2 count distribution (full population, not a sample):

| bucket | instruments | filings |
|---|---|---|
| ≤5 | 498 | 1,097 |
| 6–10 | 107 | 801 |
| 11–25 | 91 | 1,416 |
| 26–100 | 22 | 938 |
| 101–1,000 | 8 | 4,439 |
| >1,000 | 13 | 140,864 |

Every instrument with >100 lifetime B2 filings was inspected individually —
all 21 are banks, ETNs, or credit vehicles: JPM 30,268 · MS 21,561 ·
GS 17,668 · BAC 15,009 · C 14,820 · VXX 9,685 · DJP 9,685 · NRGU 4,855 ·
BMO 4,855 · RY 3,866 · BNS 3,606 · TD 3,538 · JEF 1,448 · DB 988 · CM 839 ·
NMR.US 740 · WFC 734 · PSEC 471 · ALLY 352 · USB 209 · PRU 106. This is
*empirical fetch-cost rationale* (which issuers' B2 streams are worth
fetching), NOT an economic classification of any filing — classification
stays with the Item 501(b)(3) cover parse per the source rule above.

Sector-based filtering was evaluated and **rejected**: eToro sector 4
(Financials) covers 142,164 filings / 216 instruments, but the non-financial
remainder is dominated by *debt* note takedowns from megacaps (AAPL is a B2
filer), so sector ≠ equity precision in either direction. The volume cap
separates the two populations cleanly; the cover parse does the rest.

## Rule

**`424B2` maps to `sec_424b` at discovery; the parse chokepoint tombstones
without fetch when the filer's lifetime B2 count exceeds `_424B2_VOLUME_CAP =
100`.**

- Cap evaluated per `instrument_id` against `filing_events` at parse time
  (`count(*) where filing_type = '424B2' and instrument_id = …`), not a
  static allowlist — a new structured-note program crossing the cap
  self-excludes; no maintenance list to rot.
- **Count predicate is exact-match `'424B2'`** (the canonical EDGAR form
  string already stored by discovery; full-pop check 2026-07-05: zero
  `424B2/A` or other variants exist in `filing_events`). Amendment variants
  remain unmapped in `_FORM_TO_SOURCE` exactly as today — they neither
  count toward nor pass the gate. Rows with `instrument_id IS NULL` never
  reach the gate — the merged parser already tombstones them earlier
  (`missing instrument_id`).
- **Cap is deliberately non-idempotent across rebuilds**: it reads the
  current DB horizon, so a B2 that parsed when the issuer had 90 filings can
  tombstone on a later re-drain once the issuer crosses 100. Accepted: the
  cap is a fetch-cost policy, monotone toward excluding note factories, and
  rows it retires are treated as note-program output for fetch-cost policy
  (their economics were, and remain, whatever the cover parse said). Not a
  filing-time fact, and not claimed to be one.
- Tombstone error string: `"424B2 volume cap: high-volume structured-note
  filer"` — auditable, distinguishable from parse tombstones.
- Fetch budget at today's population: **≤ 4,252 bodies** (buckets ≤100:
  1,097 + 801 + 1,416 + 938). A 1,000 cap would admit 8,691, but the
  101–1,000 bucket is 100% banks (verified above) — raising the cap buys
  4,439 worthless fetches and zero additional issuers. Chosen cap: **100**.
- B8 stays unmapped (duplicate of the underlying paragraph's filing — parent
  spec).

## Placement (mirrors existing pre-fetch gate idiom)

Same placement class as the 13D/G retention pre-fetch gate and the DEF 14A
latest-N pre-fetch gate (both lint-enforced): the gate runs in
`_parse_424b` **before** `provider.fetch_document_text`, so a gated row
costs one COUNT query and no SEC request.

Touch points (complete list — verified against merged code, PR #1973):

1. `app/services/sec_manifest.py` — add `"424B2": "sec_424b"` to
   `_FORM_TO_SOURCE` + update the tier-1 comment block.
2. `app/services/prospectus_offerings.py` — add `"424B2"` to
   `IN_SCOPE_SUBTYPES` (line 45; the subtype guard at
   `parse_prospectus_offering` line 262 raises on B2 today). B8 stays out.
   The extractor itself already handles B2 cover shapes — the JEF/TD
   fixtures shipped in PR #1973 ARE B2-style covers.
3. `app/services/manifest_parsers/sec_424b.py` —
   `_424b2_within_volume_cap(conn, instrument_id) -> bool` helper +
   pre-fetch gate in `_parse_424b` (B2 rows only; tier-1 subtypes never
   pay the COUNT).
4. `app/services/manifest_parsers/sec_424b.py::_424b_fetch_url` — the #1591
   prefetch hook mirrors the parser's pre-fetch gates and currently returns
   `None` for B2 (not in `IN_SCOPE_SUBTYPES`). It MUST apply the SAME
   volume gate (it receives `conn`; the "conn unused" comment goes away):
   URL for under-cap B2, `None` for over-cap. Without this, the pipelined
   prefetcher fetches bodies `_parse_424b` then refuses to parse.
5. `app/services/filings.py` — move `"424B2"` from the `SEC_METADATA_ONLY`
   set (line ~273) to the PARSE+RAW allowlist (lines ~215-219) + comment;
   `tests/test_filings_form_allowlist.py` updated to match.
6. `docs/etl/sources/sec_424b.md` — coverage table: B2 moves from
   "deferred (yield)" to "volume-gated"; document the cap + rationale.
7. Backfill: the ~4.2k historical B2 rows for ≤100-count instruments ride
   the SAME drive as #1974 (`sec_rebuild {"source": "sec_424b"}`) once the
   operator restarts the jobs daemon — no separate drive. The 140k+ whale
   rows enter the manifest and tombstone at ~zero cost (COUNT query, no
   fetch); if manifest-row bloat is a concern at review time, the
   alternative is a discovery-side skip, but that forfeits the audit trail
   and the self-updating cap.

## Acceptance

- Unit: `parse_prospectus_offering(body, "424B2")` no longer raises (an
  existing test asserts the opposite today — flip it); gate helper table-tested (counts 99/100/101 → allow/allow/deny);
  `_parse_424b` with a >cap B2 row returns `tombstoned` with the cap error
  and **no fetch call** (spy); `_424b_fetch_url` returns `None` for the
  same row and the URL for an under-cap B2.
- Full-population dry-run, TWO halves (parent-spec pattern):
  (a) discovery split — gated/allowed counts over dev DB match the table
  above (±new filings since scan date);
  (b) **parse-yield on the admitted slice** — run the extractor over a full
  dry-run of the ~4.2k allowed B2 bodies (not a sample) and record
  fill/tombstone rates by the same acceptance framing as the parent's
  219-body dry-run. Low fill on B2 is EXPECTED (debt takedowns parse to
  notes/tombstone); the acceptance bar is "no crashes, tombstones carry
  reasons", not a fill floor.
- Smoke: `tests/smoke/test_etl_source_to_sink.py` — no new sink tables, no
  spec change needed beyond the source's existing entry.
- Post-merge operator step: single `sec_rebuild {"source":"sec_424b"}`
  (shared with #1974, which is already Blocked-operator on the VS Code jobs
  daemon restart).

## Deferred (explicit)

- No new operator-facing filter/summary for "gated" rows — tombstone reason
  is queryable; build a surface only if the operator asks.
- No attempt to classify debt-vs-equity pre-fetch beyond the volume cap —
  that is the cover parser's job (source rule above).

## Settled decisions / prevention log

- Parent spec's settled decisions apply unchanged (raw-before-parse #938,
  born-compacted `prospectus_body`, tombstone semantics).
- Prevention-log class "per-case patches → question the model": the cap is
  population-derived, self-updating, and carries no per-issuer list.
