# #2351 — DEF 14A Item 403 rows go only to the sibling whose class they report

Status: **DRAFT — do not build from slice 2 as written.** Slice 1 (the census) is measured
and shipped. Slice 2 failed Codex ckpt-1 round 1 (88 findings, 2026-09-24); the grouped
findings and the recommended reframe are in the #2351 handoff comment of the same date.
Supersedes the general binder spec that failed ckpt-1 twice (49 → 78 findings; #2351
handoff, 2026-09-24 03:56Z).

## Problem

Every DEF 14A writer fans an accession's Item 403 rows out to **every** instrument that
shares the issuer CIK (`siblings_for_issuer_cik`). Three writers do this:
`app/services/manifest_parsers/def14a.py::_parse_def14a` (live manifest drain),
`app/services/def14a_ingest.py::_ingest_single_accession` (legacy), and
`app/services/rewash_filings.py::_apply_def14a`. So warrants (`HTZWW`, `OPENW`,
`XRXDW`), preferred (`STRK`/`STRF`/`STRC`/`STRD`, `LILAP`) and unregistered classes
(`ATROB`) render the common-stock holders as their own, and dual-class pairs
(`GOOG`/`GOOGL`, `FOXA`/`FOX`) receive the same column's figures twice.

## Source rule

- **Item 403 is per-class data.** Reg S-K Item 403(a) and (b) (17 CFR 229.403) prescribe
  the table columns *(1) Title of class, (2) Name, (3) Amount and nature of beneficial
  ownership, (4) Percent of class*. The amount and percent are defined against ONE class.
  `.claude/skills/data-sources/sec-edgar.md` §3.6 lists DEF 14A as "no per-security data
  inside → fan out". That is right for the document's issuer-level content (8-K-style
  events, comp) and wrong for the Item 403 table; this PR corrects §3.6.
- **Which class an instrument is:** the 10-K/10-Q cover's Section 12(b) table, tagged
  `dei:Security12bTitle` + `dei:TradingSymbol` + `dei:SecurityExchangeName` in one
  shared context per registered class (Reg S-K Item 601(b)(104), Reg S-T Rule 406).
  Read with `scripts/census_2900_sec_cover_identity.py::parse_cover_contexts`. We do not
  store it today.
- **No published rule maps proxy class text to a registered class.** Where this spec
  classifies a cover title (common vs non-common) the rule is fixed by construction below
  and frozen in `RECIPIENT_RULE_VERSION`.

## Slice 1 — census (this PR, measurement only)

`scripts/census_2351_def14a_class_binding.py`. Population: the 49 CIKs with >1 instrument
and stored DEF 14A holdings, 103 accessions. For each proxy it takes the latest
10-K/10-K/A/10-Q/20-F filed **on or before** the proxy's filing date (never later), fetches
its `<stem>_htm.xml` instance, and reads the 12(b) pairs.
Reproduce: `PYTHONPATH=. uv run python -m scripts.census_2351_def14a_class_binding --out var/census_2351/report.json`.

At `97e20ec6` the run printed (every figure below is from that one run):

- point-in-time cover with ≥1 12(b) pair: **101 of 103** proxies, all at depth 0 (the
  newest eligible cover was usable every time). The 2 without are 2019/2020 proxies whose
  cover predates iXBRL (HTTP 404 on `_htm.xml`).
- per-(proxy, sibling) decision under the slice-2 rule below (224 decisions):
  `legacy_multiclass` 128 (63 proxies whose cover registers ≥2 common classes —
  `GOOG`/`GOOGL`, `FOXA`/`FOX`, `HEI`/`HEI.A`, Liberty entities …), `legacy_duplicate` 44,
  `withhold_not_on_cover` 22 (10 of them with no sibling on the cover at all),
  `receive` 11, `withhold_non_common` 11, `legacy_no_cover` 4.
  Holder-row × recipient writes: 3,507 today → 3,005 (502 withheld).
- **Why dual-class is NOT in slice 2.** A table-level binder (cover title minus its
  par-value clause, maximal whole-phrase match, reject any residual class word) binds
  **2 of those 128** decisions (`multiclass_decisions_table_bound`). H-shape (side-by-side class columns) and V-shape
  (*Title of class* column) tables carry both classes in one table, so a table-level rule
  cannot attribute rows; only the column the parser actually read can. Withholding on
  "unbound" would blank BOTH siblings on ~60 proxies (e.g. `GOOGL` loses every Item 403
  row) — trading one wrong figure for a larger gap on the most-viewed names. Slice 3 binds
  the read column.
- Exact-token/title rules from the handoff (R1 ticker / R2 title / R3 sole-common, as a
  per-sibling "is my class named anywhere in the selected tables") reach 31 / 7 / 5 of 224
  decisions (`sibling_rule`); reported for slice 3's design, not used by slice 2.
- `withhold_not_on_cover` includes three CIKs whose cover belongs to a **different
  company** than the instrument (`OSG` on CIK 874501 → cover `AMBC`; `ABX` on 1814287 →
  `ABL`; `NXH` on 1130713 → `BYON`/`BBBY`). Withholding there is correct: those holders
  are not the instrument's. The identifier defect itself is outside #2351.

## Slice 2 — cover-identity store + non-common / unregistered withhold

### Schema (one migration)

`sec_cover_securities` — one row per (cover accession, 12(b) pair):
`accession_number text`, `issuer_cik text`, `filing_date date`, `form text`,
`security_title text`, `trading_symbol text`, `exchange_name text`,
`fetched_at timestamptz`; PK `(accession_number, trading_symbol, security_title)`.
Plus `sec_cover_fetch_log(accession_number PK, status text, fetched_at)` so "fetched, no
12(b) pairs" and "HTTP 404 (pre-iXBRL)" are recorded states distinct from "never fetched".
Raw instance bytes go to `filing_raw_documents` (`document_kind = 'xbrl_cover_instance'`)
BEFORE parsing (prevention log: raw before parse).

### Fetch

A helper `ensure_cover_for_proxy(conn, *, issuer_cik, proxy_filing_date) -> CoverState`
picks the newest `filing_events` row for any sibling with `filing_type` in
{10-K, 10-K/A, 10-Q, 10-Q/A, 20-F, 20-F/A} and `filing_date <= proxy_filing_date`, and
fetches its instance through the existing SEC HTTP client + shared rate gate if not
already logged. It is called ONLY for multi-sibling CIKs, OUTSIDE the per-accession write
transaction (both live writers already fetch before taking the accession lock).
`rewash_filings` does no network I/O: it reads the store and treats "not fetched" as
unknown. A one-shot `scripts/backfill_2351_cover_store.py` populates the store for every
multi-sibling CIK before the rewash.

States: `known(pairs)` | `unknown` (no eligible cover row, fetch not attempted, transport
error, 5xx/403/429) | `absent` (404 / no 12(b) pairs — recorded, not retried).
Walk-back to an older cover happens only past `absent`, never past `unknown`
(unknown is not absence).

### Recipient rule (pure function, `RECIPIENT_RULE_VERSION = 1`)

`def14a_holding_recipients(siblings: [(instrument_id, symbol)], cover: CoverState) ->
{instrument_id: (receive: bool, reason)}`. Symbols compare as
`norm_symbol` = upper-case, drop a trailing eToro `.US`, strip `.`/`-`/`/`/space.

Evaluated in order, first match wins (the census's `recipient_decisions` is the
reference implementation):

1. Single sibling → receive (`single_sibling`; byte-identical path).
2. Cover `unknown` or `absent` → receive (`legacy_no_cover`). Today's behaviour; counted
   and logged so the gap is visible.
3. Sibling's `norm_symbol` matches >1 pair (one symbol, several titles) → receive
   (`legacy_ambiguous_cover`). 0 in the census.
4. Sibling's `norm_symbol` matches no pair → withhold (`withhold_not_on_cover`), with
   `no_sibling_matched = true` when no sibling matches any pair (cover is another
   company's). An eToro `.US` duplicate never lands here: `norm_symbol` drops `.US`, so it
   matches its listed base.
5. Matched title is **non-common** → withhold (`withhold_non_common`).
6. The matched COMMON titles across all siblings number ≥2 → receive
   (`legacy_multiclass`, deferred to slice 3). Unchanged from today.
7. Two siblings share the `norm_symbol` (`BALY`/`BALY.US`) → receive (`legacy_duplicate`).
8. Otherwise → receive (`receive`).

"Non-common", by construction: the title matches
`warrant|preferred|preference|unit|note|depositary|right|debenture|bond`
(case-insensitive), or does not match `common|ordinary|capital stock`. ⚠ A depositary
share over ordinary shares is therefore non-common, so a CIK whose only listed class is an
ADS would withhold from every sibling. Guard: rule 5 applies only when at least one
sibling matches a COMMON title; otherwise the non-common-matched siblings fall through to
receive (`legacy_no_common_on_cover`). The census population has no such CIK; the guard
exists for the general population.

⚠ Rule 4's all-withhold also fires on a ticker rename between the cover and eToro's
symbol. The cost is a missing figure until the next cover/rename sync, and it is counted
(`withhold_not_on_cover` with `no_sibling_matched = true`). Accepted: a wrong issuer's
holders is the worse state.

### Writers

All three writers call the rule once per accession, then:
- receivers: unchanged write path;
- **withheld** siblings: delete the accession's `def14a_beneficial_holdings` rows for that
  instrument, supersede its DEF 14A observations for that accession, refresh
  `def14a_current` and ESOP current. The withheld set is
  `siblings ∪ instruments already holding rows for this accession` so a sibling that lost
  its CIK link is also cleaned.
- the ingest log row records `recipient_rule_version` and per-reason counts.

Parser output is unchanged, so `_PARSER_VERSION_DEF14A` does not bump; the rewash is
scoped to the multi-sibling CIKs' accessions.

### Tests

Pure table test of `def14a_holding_recipients` over the census shapes (HTZ/HTZWW,
MSTR+preferred, ATRO/ATROB, ABX/ABX.US wrong-issuer, BALY/BALY.US duplicate, GOOG/GOOGL
legacy_multiclass, unknown cover). One DB test that a withheld sibling's existing typed
rows + observations are superseded and its `_current` no longer carries the accession.

### Acceptance (DoD 8-12)

- Full-population A/B over every multi-sibling CIK accession: per-instrument
  `def14a_current` holder count before/after; the ONLY instruments losing rows are those
  the census lists as `withhold_*`; receivers are byte-identical.
- Smoke: `HTZ` unchanged, `HTZWW` ownership panel shows no DEF 14A holders; `MSTR`
  unchanged; `GOOGL` unchanged (slice 3).
- Cross-source: `HTZWW`'s cover title "Warrants to purchase Common Stock" on
  `0001657853-26-…` 10-K vs EDGAR.

## Slice 3 — scoped only

Bind the column the parser READ (H-shape group header, V-shape class cell, per-table
caption) to a cover title, emit per-class rows keyed by instrument, and withhold the
unbound sibling. Needs its own spec + ckpt-1. The census records give its test set.

## Security

No auth surface. One new outbound fetch class (SEC Archives instance XML) through the
existing rate-gated client with the configured SEC User-Agent.
