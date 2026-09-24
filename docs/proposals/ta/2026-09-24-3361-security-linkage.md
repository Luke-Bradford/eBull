# #3361 — dated series ↔ CIK linkage with an abstention census

Step 2 of `docs/proposals/ta/2026-09-24-selection-programme-v2.md` ("Security linkage"). Refs #2899, #2721,
#3360 (the fundamentals bundle this joins to), #3362 (termination). Codex ckpt-1 rounds 1 (67 findings) and
2 (60) ran on 2026-09-24 and are folded in; see "Design history".

## Contract (deliberately narrow)
> For research price series S and decision date D: which SEC entity (CIK) did the SEC evidence **accepted
> strictly before D** name as S's issuer, or a typed abstention.

- It links a **series** (one vendor file under one vendor symbol) to an **entity** (a CIK). It does not pick a
  primary class, merge series, split a series, classify instrument type (ETF, fund, ADR), or give a
  succession or delisting any economic meaning. Those belong to each arm's declaration (#2901 first) or to
  #3362.
- **Scope: `icyDenev/Intrader` series.** Every other vendor's series are in the inventory and return
  `vendor_out_of_scope` until that vendor declares its own grammar.

**Declared residuals. The reader cannot remove these, and every arm inherits them:**
- **The series inventory is capture-time metadata.** A vendor symbol and bar bounds are the vendor's labels
  at capture (2023-11 for Intrader: research-price-corpus skill). They are applied to every earlier date, so
  a renamed file carries its later name backwards. No dated vendor-symbol history exists. The same holds for
  corpus-level survivorship: files the vendor never captured are absent. #2346 measured the 2023 Form 25
  cohort at 258/259 across the three archives, which covers only that cohort.
- A file that stitches two issuers together *may* surface as a succession. That is not guaranteed: the
  predecessor's evidence can be absent, carry another symbol, or expire first.
- The filing obligation (below) explains which issuers usually have evidence. It does not certify that the
  archive is complete, and nothing is gated on it.
- The causal-equality test (acceptance item 2) proves the reader uses no SEC evidence accepted on or after D
  **except through the snapshot-integrity mask** (accession conflicts, issuer integrity, collisions), which is
  computed on the full snapshot. That is conditional causality, as in #3360, given the snapshotted inventory
  and archives. It proves nothing about identity correctness; item 3
  measures agreement with two non-ground-truth comparators on subsets only.

## Why causal
A link at D that used evidence accepted after D would condition on the issuer existing after D, i.e. on
survival. The programme forbids this. So the reader has no intervals, end dates or lifetime summaries: each
of those is future information at its start. The cost is abstention and stale links around identity
changes, and the census describes them. The census does not measure their correctness.

## Source rules
- **Evidence: SEC Insider Transactions Data Sets** (`<YYYY>q<N>_form345.zip`, served from `2006q1`;
  `app/services/sec_bulk_download.py`). From `SUBMISSION.tsv`: `ACCESSION_NUMBER`, `DOCUMENT_TYPE`,
  `ISSUERCIK`, `ISSUERTRADINGSYMBOL`.
  - The data sets are SEC extractions of the filed XML. That the extracted issuer/symbol fields equal the
    originally accepted content is a property of the source, not verified here.
  - **Who files (explanatory only):** Exchange Act §16(a) (insiders of issuers with a §12-registered equity
    class); Investment Company Act §30(h) (closed-end funds); Rule 3a12-3(b) exempts foreign private issuers
    (17 CFR 240.3a12-3, current text read 2026-09-24 via LII; its effective history across 2006–2026 was not
    researched, because nothing depends on it). None of these is in sec-edgar skill §2.3 yet; `.claude/**` is
    not writable from the loop worktree, so the text is parked on #2403.
  - `issuerTradingSymbol` is filer-entered and issuer-level: one symbol per filing (sec-edgar skill §2.3). It
    is evidence, never a label.
  - **Amendments, by construction:** a `/A` is a separate observation at its own acceptance and replaces
    nothing. There is no documented rule tying an amendment's issuer symbol to its original's, and replacing
    retroactively would leak.
- **Clock: acceptance, as in #3360.** The clock is `acceptanceDateTime` of the accession in the **issuer
  CIK's** `submissions` index (skill §7.4, §7.8), validated by #3360's `normalise_acceptance`; public iff
  acceptance NY date < D. The index is the #3360 bundle's snapshotted `submissions.zip`, pinned by that
  bundle's manifest digest. `FILING_DATE` is not a clock: its acceptance is later on 752 accessions
  (measured below).
- **Form 25: an informational flag, never an input to the link.** Source: `sec_form25_register` (skill §2.6).
  - A Form 25 ends an exchange listing, not an issuer's identity (Rule 12d2-2).
  - Its clock is `filed_date` (< D). That is the only date the register stores, and a 25-NSE accession sits
    under the exchange CIK. It is **weaker than the link's clock**, which is acceptable only because the flag
    never feeds a link.
  - Register rows are today's enrichment (`issuer_cik`, `resolved_symbol`, `provision_class`). They are a
    snapshot property, stated as such.
- **Not evidence:**
  - `submissions` `tickers` and `company_tickers*.json`: today's mapping, which drops delisted names
    (trap 4);
  - eToro `instrument_id` and `instrument_cik_history`: today's broker universe, used as a cross-check only;
  - `sec_cover_12b_pairs`: a def14a-driven subset.
- **Vendor grammar (by construction).** No published Intrader naming rule was found. Rule 3 is an
  interpretation of the measured suffix census, frozen in `POLICY`. It abstains on every unrecognised shape.
  The result carries its grammar class, so an arm can refuse `class` series or `q_alias` links.
- **Prior-art shape** (not a rule for our data): CRSP/Compustat CCM dated links with a link-type code.

## Premise measurements
Command:

```
PYTHONPATH=. uv run python scripts/measure_3361_symbol_evidence.py --submissions <#3360 bundle>/inputs/submissions.zip
```

Run 2026-09-24. This run is unpinned (live DB, the on-disk bulk directory): it motivates the design, and the
implementation's census supersedes it.

- **Form 3/4/5 archives:** 81 quarters (`2006q1`–`2026q1`), 4,402,307 `SUBMISSION` rows.
- **Series:** `research_price_series` has 0 rows with a CIK. Intrader has 22,879 series. By the script's suffix
  regex, 19,994 have no suffix; the largest suffix groups are `P` 1,324, `WS` 579, `U` 351, `W` 288, `CL` 113
  and `R` 111. The script's "plain" means "the suffix regex did not match", not rule 3's plain.
- **Evidence in each series' whole bar range** (not the causal reader):
  - Unsuffixed series: 8,170 with one CIK, 790 with several, 1,553 whose symbol appears only outside the
    range, 9,481 whose symbol never appears.
  - Suffixed series: 65 with one CIK, 0 with several.
  - All 790 multi-CIK series are therefore unsuffixed. Of them, 267 are disjoint in time and 523 overlap.
  - Examples found by hand, not validated:
    - `ACET`: CIK 2034 until 2019-02, then CIK 1720580 from 2020-09.
    - `AB`: the Holding and the LP both cite `AB`.
    - `ACM`: one stray 2023 filing under another CIK.
- **Clock:** every row is a unique (issuer, accession) pair. By acceptance NY date vs `FILING_DATE`:

  | acceptance vs `FILING_DATE` | accessions |
  |---|---:|
  | equal | 4,400,949 |
  | later | 752 |
  | earlier | 1 |
  | missing from the issuer's index | 76 |
  | issuer fails #3360 integrity | 529 |

## Construction rules
1. **Inputs (snapshotted, hashed; the build fails on any violation):**
   - **Form 3/4/5 quarter zips.** Contiguous from `2006q1`. Member names unique. Exactly one `SUBMISSION.tsv`,
     whose header has unique names including the four columns used. Strict UTF-8.
     - Completeness of a quarter is **not certified**: SEC publishes no watermark.
   - **#3360 submissions.zip**, pinned by that bundle's manifest digest.
   - **Form 25 rows:** the sorted `sec_form25_register` equity-delisting rows read, plus the register's
     `[min, max](filed_date)`.
   - **Series inventory:** the sorted `research_price_series` rows of **every** vendor (`series_id, vendor,
     vendor_symbol, first_bar, last_bar`). `series_id` must be unique, the symbol non-empty, and `first_bar ≤
     last_bar`.

   Derived bounds:
   - `capture_end` = max `last_bar` per vendor.
   - `coverage_start` = 2006-01-01.
   - `supported_through` = the earliest of: the last quarter's end, the #3360 bundle's `supported_through`,
     and Intrader's `capture_end`.
2. **Observation admission.** Canonicalisation comes first:
   - CIK = int of ASCII digits → 10-digit zero-padded string.
   - Symbol = trimmed and upper-cased.
   - Placeholders `""`, `NONE`, `N/A`, `NA` are checked on that string **before** separators are unified. They
     are evidence-side conventions only; vendor roots are never tested against them.

   Every row then gets one outcome, in this precedence (first match wins):
   1. `malformed_row`: wrong column count, or the accession does not match ASCII `[0-9]{10}-[0-9]{2}-[0-9]{6}`.
   2. `malformed_cik`: not 1–10 ASCII digits, or zero.
   3. `unsupported_document_type`: not in `{3, 3/A, 4, 4/A, 5, 5/A}`.
   4. `empty_symbol`.
   5. `multi_symbol`: the symbol contains `,`, `;` or internal whitespace.

   Accession-level integrity, next:
   - Rows surviving 1–5 are grouped by accession on (CIK, unified symbol, document type).
   - Equal tuples collapse into one observation with a multiplicity.
   - An accession whose tuples differ is excluded whole as `accession_conflict`, which replaces those rows'
     outcomes.

   Then, per observation:
   - `issuer_integrity_excluded`: the issuer's submissions fail #3360 rule 5.
   - `no_acceptance`: the accession is absent from the issuer's index, has no valid acceptance, or its index
     form's family is not 3/4/5.
   - Otherwise `stored`.

   Accession conflicts and issuer integrity are **snapshot-integrity** properties, applied at every D exactly
   like #3360 rule 5. An answer is a function of (snapshot integrity state, public prefix). The ledger keeps a
   (quarter, row index) locator for every non-stored row, and reconciles rows in = Σ outcomes.
3. **Series grammar (Intrader).** The vendor symbol is split on `_`.
   - `root` must be 1–5 characters of `[A-Z0-9]`; otherwise the result is `unparsed_symbol_form`. The
     remaining tokens `T` decide:
     - `T` empty → `plain`.
     - `T` = one single letter not in `{P, W, U, R}` → `class c`.
     - The first token of `T` in `{P, WS, W, U, R, WD, CL}` → `non_common_symbol_form:<token>`.
     - Else → `unparsed_symbol_form`.
   - The single-letter-means-class reading is an interpretation. The result exposes it so an arm can refuse
     it.
   - Exchange test issues (`universe_selection.EXCHANGE_TEST_ISSUE_SYMBOLS`, #2912) → `vendor_test_symbol`.
4. **Matching.** Evidence symbols are separator-unified (`.`, `-`, `/`, `_` → `.`).
   - **Plain `r`:** matches `r`, or `rQ` when `len(r) = 4` (the **Q alias**: research-price-corpus skill
     §bankruptcy suffix; by construction the length is the shape of every example there, `BBBYQ`, `YELLQ`,
     `SRNEQ`). Alias observations are marked `q_alias`. What the rule misses is not measurable here.
   - **Class `r_c`:** matches only `r.c` (`BF.B`, `BF-B`, `BF/B`, `BF_B`). Never `rc`, never `r`. A plain
     series never matches `r.c`.
   - **Collision guard:** two in-scope series with an overlapping match set are both abstained whole as
     `vendor_symbol_collision`. For example, a plain `ABCD` and a plain `ABCDQ` both match `ABCDQ`. This is a
     property of the whole captured inventory, applied at every D, and declared as the same kind of
     snapshot-integrity exception.
5. **Link at D.** W = S's matching stored observations with acceptance NY date in `[D − 730 days, D)`.
   - **The window, by construction.** 730 days is the #3360 census population window, frozen in `POLICY`. No
     insider-cadence rule exists to derive one. It bounds the age of the **supporting evidence**, not the time
     since an identity change.
   - **Outcomes:**
     - W empty → `no_recent_evidence`, with sub-reason `never_seen` when no matching stored observation
       precedes D.
     - One CIK → `linked(cik, single_cik)`.
     - Several CIKs forming a **strict succession** (the CIKs ordered by first acceptance; each CIK's last
       acceptance is strictly earlier than the next CIK's first) → `linked(last cik, succession)`.
     - Otherwise, including equal acceptance timestamps across CIKs → `conflicting_evidence`.
   - **Accepted costs.** There is no majority, count or recency weighting; none has a source rule. The costs:
     - one stray later filing flips a link;
     - a stale predecessor persists while its evidence is under 730 days old;
     - a conflict lapses when its observations age out.
6. **Result algebra.** Precedence, first match wins:
   1. `series_not_in_bundle`
   2. `vendor_out_of_scope`
   3. `after_capture` (D > `supported_through`)
   4. `outside_series` (D ∉ `[first_bar, last_bar]`; bar presence ON D is the consumer's read)
   5. `vendor_test_symbol`
   6. `unparsed_symbol_form`
   7. `non_common_symbol_form:<token>`
   8. `vendor_symbol_collision`
   9. `before_coverage` (D − 730 < `coverage_start`)
   10. `no_recent_evidence[:never_seen]`
   11. `conflicting_evidence`
   12. `linked(cik, basis)`

   Every result from 5 on carries the grammar class. Results 10–12 also carry:
   - the W observations: accession, CIK, acceptance, multiplicity, `q_alias`;
   - `form25`: the register rows with `filed_date < D` whose `issuer_cik` is a CIK in W. Each is marked
     `symbol_match` (its `resolved_symbol` passes rule 4 against S), `symbol_other` or `symbol_null`.
     `form25_unobserved` is returned instead when D is outside the register span.
7. **Reader** `link_as_of(series_id, D)` returns the rule-6 result. Cross-series facts are census-only, except the collision guard (rule 4), a declared inventory-integrity exception.

## Artefact
Content-addressed like #3360 (`r6_pit_bundle.read_verified_document`, exclusive publish, policy-bound
loader):
- `<bundle>/inputs/` holds the snapshotted zips and row dumps, including the cross-check inputs:
  `instrument_id`, `instrument_cik_history`, and each series' `delisting_provision` / `delisting_filed_date`.
- `<bundle>/series/<series_id>.json` holds:
  - the series row and its grammar result;
  - its matching stored observations in total order (acceptance, accession, CIK);
  - the Form 25 rows of their CIKs.
- `<bundle>/manifest.json` holds:
  - `POLICY`: sha over the constants, the builder, the reader, `pit_fundamentals.py`,
    `universe_selection.py`, `r6_pit_bundle.py`, Python and `tzdata`;
  - the input digests, including the #3360 manifest digest;
  - `supported_through`, the ledger, and the per-series digests.

## Census (descriptive; before any strategy look)
- **Formations:** the #3360 grid (the last NYSE session of each June, 2011–2024). Every descriptive is
  counted on this grid only, one observation per (series, formation), so no statistic is date-weighted.
- **Population at D:** in-scope Intrader series, in three groups. Groups 2 and 3 are the missingness that
  matters here.
  1. `bar_on_d`: primary.
  2. `no_bar_on_d`: `first_bar ≤ D ≤ last_bar`, no bar ON D.
  3. `ended_in_window`: `last_bar` in `[D − 730, D)`.

  The bars read from `research_price_daily` (dates, close, volume) are written into the census evidence
  directory, and that is the snapshot the census reads. Series bounds come from the bundle inventory. A bar
  outside its series' bounds fails the census.
- **Cross-tab per formation:** result reason × liquidity decile × capture status. This is the issue's year ×
  reason × size × outcome.
  - **Liquidity decile** (liquidity, not company size):
    - Take the bars strictly before D.
    - Drop bars with NULL or non-positive close or volume.
    - Take the latest 21 that remain. Their oldest must be ≥ D − 42 calendar days (by construction: twice the
      count); otherwise `liquidity_unavailable`.
    - Value = the median of close × volume, as stored. The vendor's split-adjustment of volume is
      unverified, so the products are comparable only as far as it holds.
    - Zero-based rank over (value, series_id) within `bar_on_d` series that have a value; decile =
      ⌊10·rank/n⌋ (0–9); n = 0 → no deciles.
  - **Capture status:** `runs_to_capture` if `last_bar ≥ capture_end − 7 days`, else `ends_before_capture`.
    This is a vendor property, not an economic outcome.
  - **Form 25 outcome.** Rows filed in `[D, D + 730]`, reported per match kind:
    - for `linked` results, by the linked CIK;
    - for every result, by rule-4 symbol match on `resolved_symbol`, so abstentions are covered too;
    - with #3360's observed-span rule (`unobserved_horizon`). Completeness inside the span is not
      certified.
- **Counts:**
  - series and distinct linked CIKs;
  - CIKs with more than one linked series;
  - `q_alias` links;
  - collisions;
  - form25 flag kinds.
- **Identity descriptives (full population, formation grid; not correctness measures):**
  - the age of the newest supporting observation for linked results;
  - successions whose predecessor CIK is linked again at a later formation, reported with right-censoring:
    `reverted`, `not_reverted`, or `unobservable` (fewer than 730 days of later coverage).
- **Join to #3360:** linked CIKs at D that are, or are not, #3360 census members, and #3360 members with no
  linked series. Both directions are reported.

## Acceptance (this ticket)
1. **Pure fixtures:**
   - **Linking:** single CIK; strict succession, and a later reversion (the reader follows the prefix); equal
     timestamps across CIKs → `conflicting_evidence`; an interleaved stray → conflict, which lapses after 730
     days.
   - **Clock:** acceptance on D is not public at D; acceptance later than `FILING_DATE` → public by
     acceptance.
   - **Admission:** `no_acceptance`, invalid acceptance, index form not 3/4/5, `accession_conflict` and
     collapse-with-multiplicity; the `N/A` placeholder, and root `NA` still parsed as a vendor root;
     `unsupported_document_type`; `multi_symbol` (`BF B`).
   - **Matching:** `q_alias` for a 4-character root, none for 3; `BF_B` matches `BF.B`, `BF-B` and `BF/B`,
     not `BFB` or `BF`.
   - **Grammar:** `ABC_P_A_CL`, `ABC_WS`, `ABC_R_W` abstain; an unrecognised shape → `unparsed_symbol_form`.
   - **Collisions:** `ABCD` vs `ABCDQ`.
   - **Form 25:** flagged only when `filed_date < D`; `form25_unobserved` outside the span; never changes the
     link.
   - **Bounds:** `outside_series`, `before_coverage`, `after_capture`, `vendor_out_of_scope`,
     `series_not_in_bundle`.
   - **Build:** ledger reconciliation with locators; build failures (duplicate member or header, gap quarter,
     bad UTF-8, duplicate `series_id`, reversed bounds); loader refusals; rebuild determinism.
2. **Causal reference** (full population, as #3360 item 2):
   - **Integrity first.** Snapshot integrity (accession conflicts, issuer integrity, collisions) is computed
     on the **full** snapshot. Only then are the stored observations filtered to acceptance < D, and rules 5–6
     recomputed independently (without calling the reader).
   - **Decision dates.** Every observation acceptance NY date + 1 and + 731; every flagged Form 25
     `filed_date` + 1; `first_bar`; `last_bar` + 1; `supported_through` + 1; `coverage_start` + 730; and the
     day before the first observation. A series without observations gets the non-observation dates only.
   - Assert the **entire** result is equal.
3. **Cross-checks** (reported; subsets, so they establish nothing about the rest of the population). Each
   reports agree / disagree / not comparable with denominators, and lists every disagreement:
   - (a) **eToro:** series with `instrument_id` where exactly one `instrument_cik_history` row has
     `effective_from ≤ last_bar` and (`effective_to` NULL or `last_bar < effective_to`). Several rows or none →
     not comparable.
   - (b) **Form 25:** series with `delisting_provision`. The link is built **without** the Form 25 input and
     compared with `issuer_cik` at `delisting_filed_date`. The series↔Form 25 association is itself
     symbol-based (`symbol_exact`), so this is an independent **source**, not an independent association.
4. Census evidence manifest written, hashing: the bundle manifest, the bars snapshot, code and parameters.
5. **Registry** (`app/services/research_point_in_time.py`): the cells citing #3361 stay **fail**, with the
   clock and causal evidence added to their reasons. **Nothing becomes admissible.** How an arm consumes
   abstentions, `succession`, `class` and `q_alias` is that arm's declaration.

## Implementation-precision rules (round 3)
Each resolves one round-3 finding; the number in brackets is that finding.

**Admission and inputs**
- **Order [4].** Validate, then canonicalise. The ledger keeps the raw CIK string.
- **Quarter files [5].** Each quarter name appears once; a duplicate fails the build.
- **TSV parsing [6].** Tab-separated, no quoting (`QUOTE_NONE`), no BOM (a BOM fails the build), `\n` line
  ends. Blank lines are `malformed_row`.
- **Form 25 admission [7, 8, 9].**
  - Rows are selected by `provision_class = 'equity_delisting'` exactly.
  - `issuer_cik` is canonicalised by rule 2. A malformed `issuer_cik` or NULL `filed_date` fails the build.
  - A `resolved_symbol` that is empty or a placeholder counts as `symbol_null`.
  - An empty register makes every flag `form25_unobserved`.
- **No Intrader series [10].** The build fails.
- **Identifiers and sort order [11, 12].**
  - `series_id` is a positive bigint; its file name is its decimal form.
  - Every dump is sorted by its full column tuple, NULLs first. Form 25 rows on a result are sorted by
    (`filed_date`, accession).

**Grammar**
- **Letters [13].** Single letters are ASCII `A–Z` only.
- **Exchange test issues [14].** The test applies to the full vendor symbol and to its root.

**Links and flags**
- **Q alias [18].** A link is `q_alias` iff any W observation of the linked CIK is an alias.
- **Optional Form 25 input [19].** The builder accepts `--without-form25` for cross-check (b). Such a bundle's
  `POLICY` differs, and its manifest records the mode.
- **Series that ended before D [20].** For `ended_in_window` series the census reports `link_as_of(S,
  last_bar)` as the diagnostic.

**Liquidity**
- **Fewer than 21 bars [21].** `liquidity_unavailable`.
- **Groups 2 and 3 [22].** Liquidity `not_applicable`.
- **Bar rejections [23, 24].** Duplicate (series, date) bars, or non-finite values, fail the census.
- **Endpoint bars [25].** Every series must have its endpoint bars in the snapshot; otherwise the census fails.

**Form 25 outcome channels**
- **Two channels [26, 27, 28].** The Form 25 outcome is reported separately:
  - by the linked CIK;
  - by symbol match, only where rule 4 defines a match set.

  Channels are never merged. The unit is **series with ≥1 row** (binary); row counts are separate.
- **Counting units [29, 30].**
  - Collisions are counted in series and in groups, by final result.
  - Flag counts use results 10–12 as the denominator.

**Identity descriptives**
- **Observation age [31].** Measured in days, as D minus the observation's acceptance NY date.
- **Reversion [32–35].** For the same series, reversion means "the **immediate** predecessor CIK is linked at
  a later formation within 730 days of the switch formation". Coverage means grid formations ≤
  `supported_through`. Precedence: `reverted` > `unobservable` > `not_reverted`.

**Join to #3360**
- **Membership [36].** Recomputed from the same pinned `submissions.zip` with the #3360 census code
  (`scripts/census_3360_pit_fundamentals.py::membership_mask`).

**Cross-checks**
- **Query dates [37, 41].** Queries run at `min(last_bar, date)`: eToro at `last_bar`; Form 25 at
  `min(last_bar, delisting_filed_date)`.
- **Classification [38].** Reader abstentions and a missing comparator are `not_comparable`, each reported by
  reason.
- **History-row validity [39].** Malformed `instrument_cik_history` rows are `not_comparable`.
- **Register join [40].** Joined on `(issuer_cik)` of rows with `resolved_symbol` matching the series under
  rule 4 and `filed_date = delisting_filed_date`. Zero or several distinct CIKs → `not_comparable`.

**Causal test dates [42, 43, 44]**
- Add each acceptance date itself, `+730` (the last day inside the window), `first_bar − 1`, `last_bar`, and
  `supported_through`.
- Add the register span's `min` and `max + 1`.
- Form 25 dates come from the candidate rows of every CIK ever observed for the series, independent of D.

## Design history
**Round 1 (67 findings):**
- The acceptance clock replaced `FILING_DATE`, after measurement.
- A Form 25 no longer terminates a link.
- Interval compression and `shared_cik` were removed from the reader.
- Scope was restricted to Intrader.
- The grammar became closed.
- The `first_bar` clip on evidence was dropped.
- The snapshot-integrity exception was declared.
- The census stopped promising an FPI/ETF split it has no source for.
- The `N/A` bug in the script was fixed.

**Round 2 (60 findings):**
- The residuals are stated honestly: inventory labels, corpus survivorship, and what the causal test proves.
- `BF B` vs `multi_symbol` contradiction resolved (space removed from the separators).
- Admission tightened: CIK canonicalisation, a document-type check, collapse vs conflict, and per-row
  locators.
- Input validation added: archive validation, series-inventory validation, and every vendor in the inventory.
- New result `before_coverage`.
- Succession defined strictly, with ties counted as conflicts.
- Form 25: its weaker clock is confined to a flag, and `form25_unobserved` added.
- Causal test: integrity is computed on the full snapshot first, and the date set now covers expiry, capture,
  coverage and Form 25.
- Census: bars are snapshotted; liquidity is defined exactly; the population is three groups; outcomes
  include abstentions; descriptives sit on the formation grid with right-censoring.
- Cross-check inputs are snapshotted, and interval containment is defined.
- The count error was fixed (the old "104 other" figure).

**Round 3 (44 findings):** the section above resolves each one.

**Kept as stated costs** (no source rule exists for an alternative): the 730-day window, any interleaving
abstains, the Q-alias length, and the grammar interpretation.
