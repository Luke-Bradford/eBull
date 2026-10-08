# #3624 slice 2 build spec: structured 8-K item labels (2026-10-08)

Programme: `docs/research/2026-10-06-3624-llm-research-component.md` §5 "Slice 2", which lists what this spec must
contain. This is a **label producer, not a strategy**. Any factor or filter that uses these labels supplies its own
hypothesis, event-to-feature rule, persistence, expiry, horizon, track, power assessment, coverage requirement and
gate before it reads outcomes. The producer reads no prices and no returns.

**Design rule: when in doubt, `unknown`.** No rule below links, infers or estimates anything a structured field
does not state. Wherever the sources leave a question open, the answer is `unknown`, never `clear`.

**Three sources, three jobs.**
- **Inventory:** EDGAR's quarterly full-index `master.gz` files say which accessions exist, per CIK.
- **Item cache:** the bulk `submissions.zip` says which accessions *might* carry a label, so only those need a
  header.
- **Authority:** each such accession's EDGAR SGML header gives its items, form, filing date, acceptance time, last
  post-acceptance correction date and filer CIKs. Every label and every availability date comes from a header.

## Measurement and pinned inputs

Two commands, outputs committed beside this spec:

    RA="$HOME/Library/Application Support/eBull/research-artifacts/sha256"
    A="$RA/d6c42d554a703832a3a477a0b04b0f8cc96ce868180b1bb58e9be99ad717de8b/submissions.zip"
    B="$RA/928d67221c6e6183bc343e7234c1391448c15cd1dd644d36b425db2f99ba4350/submissions.zip"
    PYTHONPATH=. uv run python -m scripts.measure_3624_slice2_8k_items --archive "$A" --previous "$B" \
        --headers --negative-sample 2000 --out docs/research/3624-slice2-8k-items-measurement.json
    PYTHONPATH=. uv run python -m scripts.measure_3624_slice2_8k_items --archive "$A" \
        --index-cache var/research/3624/full-index --out docs/research/3624-slice2-full-index-reconciliation.json

Headers are cached under `var/research/3624/headers/` (and `full-index/headers/`), one `.sgml` per accession; each
output row carries its body's sha256.

**Pinned inputs:**
- **Archive A:** `submissions.zip` sha256 `d6c42d55…`, captured 2026-10-07T08:05:08Z; last 8-K-family filing date
  2026-10-06.
- **Archive B:** sha256 `928d6722…`, captured 2026-08-24; last 8-K filing date 2026-08-21.
- **Full index:** 90 quarterly `master.gz` files, 2004 Q3 to 2026 Q4, each named by sha256 with its `Last Data
  Received` line in the reconciliation JSON.
- **Our CIKs:** the 5,310 CIKs in `external_identifiers` (`provider='sec'`, `identifier_type='cik'`), written out in
  full in the measurement JSON (`ciks`, sha256 `d1c01c68…`). They scope the A/B and the header measurement; the
  producer reads every CIK.
- **Form 8-K:** SEC 873 (02-25), sha256 `730ab1de…`, from `https://www.sec.gov/files/form8-k.pdf`.
- **EDGAR PDS Technical Specification**, version 2.0, March 2025 (sha256 `fd9d0359…`), from
  `https://www.sec.gov/info/edgar/specifications/pds_dissemination_spec.pdf`.
- **Code:** the measurement script at this PR's head commit.

Database figures (`filing_events`, `eight_k_*`, `sec_filing_manifest`, `financial_facts_raw`) are as of 2026-10-08,
read by the first command (queries in the script). They are context for the design and the A/B population.

## Labels

A label is an **observation**: one accession, original or amendment, whose header lists the item code.

| label | item | source rule (Form 8-K, SEC 873 (02-25)) |
|---|---|---|
| `non_reliance` | `4.02` | Item 4.02 (a) and (b): a conclusion or an accountant's notice that previously issued statements, or a related audit report or interim review, should no longer be relied on |
| `accountant_change` | `4.01` | Item 4.01 (a) and (b): the former accountant resigns, declines re-appointment or is dismissed, or a new accountant is engaged (Reg S-K Item 304(a)) |

Each label is named for what the item requires. Not every restatement is filed under Item 4.02, and an accountant
change is not adverse by itself. Item 2.02 stays out: it is slice 3's event-timing marker.

**Every accession counts, amendments included.** An amendment can carry a target item because a rule requires the
accountant's letter by amendment (Item 4.02(c)(3); Reg S-K Item 304(a)(3)), to correct the original, or for another
reason. The two structured sources checked carry no amendment link: `amends_accession` is NULL on all 9,049 8-K/A
rows of `sec_filing_manifest`, and no key of any CIK's `recent` block in the archive names an amendment (0 keys
containing "amend"). The producer therefore neither links nor discards: each accession with the item is an
observation with its form recorded. Collapsing observations into events is a consumer's declared choice. The same
holds for Item 4.01, whose Instruction makes a resignation or dismissal "a reportable event separate from the
engagement of a new independent accountant".

## Source rules

- **Form 8-K**, SEC 873 (02-25): Items 4.01 and 4.02, the Item 4.01 Instruction, Item 4.02(c)(3). Its 33 item
  headings (`grep -o 'Item [0-9]\.[0-9][0-9]'` on the pinned PDF) are the **item vocabulary**.
- **Release 33-8400** (2004): the current item numbering took effect on **2004-08-23**. Earlier 8-Ks use other
  codes, so the producer starts there (`FIRST`).
- **Form 8-K submission types:** the PDS `<ITEMS>` definition lists the 8-K types it applies to: `8-K`, `8-K/A`,
  `8-K12B`, `8-K12B/A`, `8-K12G3`, `8-K12G3/A`, `8-K15D5`, `8-K15D5/A`. All eight occur in archive A.
- **Reg S-T Rule 13(a)** (17 CFR 232.13): a filing by direct transmission *commencing* on or before 5:30 p.m.
  Eastern on a business day is deemed filed that day; commencing later, the next business day. **Rule 13(b)** lets
  the Commission adjust the filing date when technical difficulties delayed a good-faith filing.
- **SEC dissemination guidance** (`https://www.sec.gov/search-filings/edgar-search-assistance/accessing-edgar-data`):
  "Some filing submissions that begin after 5:30 p.m. ET … will be disseminated the next business day, showing up
  in the following business day's index"; indexes incorporating a business day's filings are updated nightly from
  about 10:00 p.m. ET. It gives no hour by which a filing is public.
- **EDGAR PDS specification:**
  - `<FILING-DATE>`: "EDGAR assigned official filing date, or post acceptance new filing date (Post Acceptance
    Correction)";
  - `<DATE-OF-FILING-DATE-CHANGE>`: "Date when the last Post Acceptance occurred";
  - a post-acceptance correction (PAC) carries "header tag changes to previously-filed submissions, deletion notices
    … or form/document type changes", and "No document text is updated in a PAC";
  - `<ITEMS>`: "Identifies 1 or more items declared in the filings", format `#.##`;
  - filer CIKs sit inside `<FILER>` blocks; other roles (`<SUBJECT-COMPANY>` and the like) are separate.
- **EDGAR full-index:** the `master` index is the "Master Index of EDGAR Dissemination Feed", one row per
  `(CIK, form, date filed, file)`, with a `Last Data Received` date. A calendar date with at least one row is an
  **EDGAR dissemination day**.
- **EDGAR SGML header:** `<accession>.hdr.sgml` in the filing's folder (the `-index-headers.html` page is absent for
  older filings: 404 on 2006 accessions).
- **Submissions JSON:** `items` aligned by index with `accessionNumber`
  (`.claude/skills/data-sources/sec-edgar.md` §"Submissions"). The JSON's `acceptanceDateTime` is never used
  (#3714).

## Premises (measured)

1. **The archive is nearly complete against the dissemination index, and per-CIK files can be stale**
   (reconciliation JSON).
   - **Index against archive**, every 8-K-family row from `FIRST`, as `(cik, accession)`: index 1,814,389 rows,
     archive 1,814,246. Index-only rows filed by the archive's last date: **2**, both 2026 Q3, both under a CIK whose
     archive file exists. Index-only after it: 141. Archive-only: **0**.
   - **Both reconciled** (`index_only_by_archive_last_date`): each has a header (sha256 kept) naming the indexed CIK
     as a filer, and each CIK's archive file ends before the accession's date (2026-09-08 against 2026-09-23;
     2026-09-15 against 2026-09-17). The archive's per-CIK files are not one consistent snapshot. The first,
     `0001193125-26-399416`, is round 1's `filing_events`-only accession.
2. **Item metadata: identity, conflicts and validity** (all CIKs from `FIRST`; `identity`, `validity`):
   - **All CIKs:** 1,814,250 appearances, 1,814,246 `(cik, accession)` rows (4 rows appear on two pages), 1,765,527
     distinct accessions, 34,619 under more than one CIK.
   - **Our CIKs, rows restricted to them:** 710,849 appearances, 710,846 rows, 710,665 accessions.
   - **Conflicts:** 0 accessions have more than one `(form, filing_date, items)` variant, comparing raw invalid item
     strings as written. Round 1's "5 conflicts" came from the JSON acceptance time.
   - **Item validity**, exact ASCII codes against the vocabulary, per appearance: 1,814,207 valid, 40 invalid (4
     ours), 3 empty (1 ours), 0 missing; 0 blocks lack an `items` array. The non-vocabulary tokens are bare integers
     (`7`, `5`, `12`, `2`, `4`, `9`), the pre-2004 numbering. No code is normalised.
   - **Pages:** each CIK file's listed pages were all read and checked before any form filter: 5,391 listed, 0
     absent, 0 misaligned, 0 unlisted page files; 0 of our CIKs absent from the archive.
3. **The archive agrees with `filing_events` on every shared field** (`filing_events`). Population: our CIKs'
   8-K-family accessions from `FIRST` through 2026-10-06, `filing_events` joined to `external_identifiers` by
   instrument; each distinct non-NULL `filing_events` item set is compared on its own.
   - **Items:** 399,534 shared accessions with no NULL row: every item set equals the archive's. 21 with NULL and
     non-NULL rows: every non-NULL set equals it. 759 have only NULL rows. 0 differ. Per year in `by_year`, with event
     rows and event accessions as denominators.
   - **Form and filing date:** equal on all 400,314 shared accessions.
   - **Coverage:** archive-only accessions by year are in `by_year`: 30,124 in 2015, falling to 17 in 2026, the
     shape of the 8-K ingest horizon (skill §11.2). The cause is not audited.
   - **Captured near filing:** 15,661 shared accessions have their earliest `filing_events` row written 0 to 3
     Eastern days after the filing date (15,638 no-NULL equal, 17 mixed equal, 6 all NULL, 0 differ). This shows no
     item change between that ingest and archive A for those accessions; it is not evidence of the original
     dissemination.
4. **The typed body parser is not an independent completeness check** (`cross_source`). Population: the 72,339
   non-tombstone `eight_k_filings` accessions, left-joined to `eight_k_items`; 72,241 are in the archive, all with
   valid items there (0 excluded). The parser stored no item rows for 52,353 of them. Within the overlap it never
   holds a target code the archive lacks (Item 4.01: 109 of 541 archive positives; Item 4.02: 25 of 94), and its
   misses are mostly accessions with no item rows (404 of 432; 66 of 69).
5. **The JSON acceptance time is unreliable** (#3714, measured at this PR's first commit `b00a2bdd`, whose JSON
   `previous_snapshot` and `acceptance` blocks hold the figures). It is not read.
6. **Headers: every candidate accession of our CIKs (7,041) and a seeded sample of 2,000 of their 703,624
   non-candidates** (`headers`; 9,041 fetched, 0 failures):
   - **Non-candidates:** 0 of 2,000 differ from the archive on items, form or filing date. One has a post-acceptance
     correction (below). With 0 of 2,000, the 95% upper bound on the disagreement rate is 0.15% (rule of three).
   - **Truncation:** among candidates, 5 archive item lists are cut short of their header (13 archive codes, 14 or
     15 in the header). By archive list length (`archive_item_count_by_disagreement`): 10 codes 102 agree, 11 codes
     89, 12 codes 60, 13 codes 32 agree and 5 truncated, 14 codes 4 agree; no list shorter than 13 disagrees. Lists
     of 10 or more codes are candidates (`ITEM_CAP`), three codes below the shortest truncated length.
   - **Other item differences:** 4 candidates carry pre-2004 codes on both sides; 1 is empty on both sides.
   - **Form and filing date:** 0 differences on all 9,041 headers.
   - **Filer membership:** the header's `<FILER>` CIKs equal the archive's CIKs for the accession on all 9,041
     (0 missing either way).
   - **Filing date against acceptance date:** same day 8,354; later 685 (by 1 day: 579; 2: 4; 3: 87; 4: 15), every
     one accepted between 17:31:14 and 22:00:21 ET, consistent with Rule 13(a); earlier 1 (`0001640334-22-000691`,
     accepted 2022-04-01 14:22, dated 2022-03-31; cause not established). 60 filings accepted after 17:30 keep their
     acceptance date, which Rule 13(a)'s "commencing" allows.
   - **Missing acceptance:** 1 (`0001104659-09-022130`, an 8-K/A dated 2009-04-01).
   - **Post-acceptance corrections:** `DATE-OF-FILING-DATE-CHANGE` is after the acceptance date on 69 headers (68
     candidates, 1 non-candidate), by 1 to 726 days (median 6). On the others it equals the acceptance date. None of
     the 69 has a header item list that differs from the archive.
7. **Snapshot drift, archive B to A, all CIKs** (`previous_snapshot`): of 1,759,461 accessions both hold, 0 changed
   CIK membership and 0 `(cik, accession)` rows changed form, filing date or items. 0 accessions of B are absent from
   A, and 0 of A filed by B's last date are absent from B. Two snapshots six weeks apart are two observations.

## Construction

### Inventory and candidates

- **Inventory.** The `(cik, accession, form, date filed)` rows of every quarterly `master.gz` from 2004 Q3 to the
  quarter holding the coverage end, form in the 8-K family, date filed ≥ `FIRST`, unioned with the archive's rows of
  the same scope, plus any header filer CIK not already a row (a **header-only** row). A row's sources are recorded.
- **Archive reading.** Every `CIK##########.json` and exactly the pages it lists; every listed page is checked for
  presence and alignment before any form filter. The producer refuses (`ARCHIVE_INCOMPLETE`) if a listed page is
  absent or misaligned. A block without an `items` array gives `missing` items on each row. Unlisted page files are
  counted and not read.
- **Item states:** `valid` (every code in the vocabulary, exact ASCII), `missing`, `empty`, or `invalid` with the raw
  string kept.
- **Candidates.** An accession is a candidate when any of its rows:
  - is index-only (no archive items);
  - has items that are not `valid`;
  - has `valid` items including 4.01 or 4.02;
  - has `valid` items with 10 or more codes (`ITEM_CAP`, premise 6);
  - or differs from another of its rows in form, filing date or items (a conflict; every variant is kept).
- **Non-candidates** have `valid`, agreeing items of fewer than 10 codes and no target code. The producer reads
  their items from the archive, not a header: this is the negative-side limit stated under "Corrections and earlier
  states".

### Headers

- **One header per candidate accession**, fetched from the first CIK that lists it, at the shared SEC rate limit.
  Its bytes are stored under `headers/<accession>.sgml` with their sha256.
- **Reuse.** The manifest names its `parent` artefact version, or none. Every header the parent holds is copied with
  its sha256 checked, never re-fetched. A header is never corrected in place; a newer copy is a new version's input.
- **Retries.** Three attempts per accession per run, 2 s then 8 s apart.
- **Fields**, each with its own outcome:

  | field | ok when | otherwise |
  |---|---|---|
  | retrieval | the body was fetched | `no_header`, last error stored |
  | parse | one `<SEC-HEADER>` block | `malformed` |
  | `<ACCEPTANCE-DATETIME>` | present, 14 digits | `no_acceptance` |
  | `<FILING-DATE>` | present, a date | `no_filing_date` |
  | `<TYPE>` | in the 8-K family | `type_out_of_scope` |
  | `<ITEMS>` | non-empty, every code in the vocabulary | `items_empty` or `items_invalid` |
  | items against the archive | equal, or the row is index-only or header-only | `items_disagree` |
  | `<FILER>` CIKs | include the row's CIK | `not_a_filer` |
  | dates | filing date on or after the acceptance date | `date_before_acceptance` |
  | `<DATE-OF-FILING-DATE-CHANGE>` | absent, or equal to the acceptance date | `corrected` (a PAC after acceptance) |

### Row classes

Each candidate `(cik, accession)` row is exactly one of:
- **`ok`:** every field ok. It is an observation for each target code in its header items.
- **`label_uncertain`:** retrieval, parse, both dates and the date order are ok and there is no PAC, but the items,
  type or filer field is not ok. Its timing is known; whether it carries a label is not.
- **`timing_uncertain`:** any of `no_header`, `malformed`, `no_acceptance`, `no_filing_date`,
  `date_before_acceptance`, or `corrected`.

A row in more than one outcome takes the first class in this order that its outcomes reach: `timing_uncertain`,
then `label_uncertain`.

### Availability: a session, never an instant

**Timing-known rows** (`ok`, `label_uncertain`):
- **`available_session`** is the first regular US equity session on a calendar date strictly after the header's
  `<FILING-DATE>`.
- **Why the filing date.** Under Rule 13(a) the filing date is the business day on which the filing counts as
  filed: the day transmission commenced, up to 5:30 p.m., otherwise the next business day. The dissemination
  guidance puts the late submissions in that next business day's index. The filing date is therefore the latest
  dissemination day the sources imply, and it follows the SEC's own calendar (a Columbus Day, when EDGAR is closed
  and the equity market open, moves the filing date, not this rule). Premise 6 matches: every later-dated filing
  was accepted after 17:30.
- **Why the next session.** The guidance gives no hour by which a filing is public. Skipping the whole filing date
  is the rule fixed by construction, frozen in the construction hash.

**Timing-uncertain rows** carry an interval `[earliest_session, latest_session]`:
- **Lower date `L`:** the acceptance date when present; otherwise the earliest of the header filing date, the
  archive filing date and the index date filed. `D0` is the first EDGAR dissemination day on or after `L`.
  `earliest_session` is the first session strictly after `D0`.
- **`latest_session`:**
  - `corrected`: the first session strictly after the correction date. From then on the current header is the
    public record, so a `corrected` row whose current header carries a label is also an observation with
    `available_session` = `latest_session`.
  - `date_before_acceptance` with no PAC: the first session strictly after the first EDGAR dissemination day after
    `D0` (the guidance's "next business day"); a current label is an observation at that session.
  - otherwise: open.

The equity calendar is `app/services/market_calendar.py::us_market_status`. Its sessions from 2003-01-02 to the
coverage end plus 30 calendar days are written to the artefact as `sessions.json`, and `state()` reads only that
file. The EDGAR dissemination days come from the pinned index files. No time zone conversion occurs.

### Coverage

- **Coverage end `F`:** the day before the coverage-end index's `Last Data Received` date. The guidance has the
  nightly index build start about 10:00 p.m. and finish "usually … within a few hours", so the last received day may
  be partial. The producer refuses if `F` is after the archive's last filing date plus one day.
- **Upper bound `s_max`:** the last session on or before `F`. Any accession filed after `F` has an
  `available_session` after `F`, and a timing-uncertain one an `earliest_session` after `F`, except one accepted on
  or before `F` and dated later; the inventory holds it only if indexed by `F`, so such a row is a known limit within
  the one-day margin.
- **Lower bound `s_min`:** the first session on or after `FIRST` + 30 calendar days. An accession dated before
  `FIRST` reaches the window only if its dissemination came more than 30 days after its filing date. Among the
  9,041 headers, filing date precedes acceptance once, by 1 day. The producer refuses if any candidate of the full
  population has an acceptance date more than 30 days after its filing date.

### Output: a research artefact

Published under the research root with the step 1 reference-artefact protocol: exclusive `mkdir`, a clean checkout,
the manifest written last.

**The manifest names:** the archive sha256 and capture time; every `master.gz` sha256 and its `Last Data Received`;
`FIRST`, `F`, `s_min`, `s_max`; the `parent` version; the construction hash (the producer's import closure);
`sessions.json`'s sha256; every header's sha256; the stage-A manifest sha256 and the A/B baseline's sha256 (gates 5
and 6); the census.

**Files:**
1. **`observations.jsonl.gz`:** one row per observation: `cik`, `accession`, `form`, `filing_date`, `acceptance_et`,
   `items`, `labels`, `available_session`, `row_class`.
2. **`uncertain.jsonl.gz`:** one row per `label_uncertain` or `timing_uncertain` row: every field outcome, every
   archive and index variant, the header fields present, the stored error, `earliest_session` and `latest_session`
   (`null` for open; for `label_uncertain` both equal `available_session`), and `may_carry` (the labels it could
   carry: those in its header or archive items, or both labels when neither side has `valid` items).
3. **`coverage.jsonl.gz`:** one row per CIK with an archive file (987,854 in archive A) and per CIK seen only in the
   index: archive file present, pages complete, inventory rows by year.
4. **`sessions.json`**, **`edgar_days.json`**, **`headers/`**, **`ab_baseline.jsonl.gz`** (gate 6),
   **`census.json`**.

No database table, migration or operator-visible surface changes.

### Label state

```
state(cik, label, session, lookback) -> ("flagged" | "clear" | "unknown", reason)
```

**Inputs are validated first**, or `state()` raises: `label` is `non_reliance` or `accountant_change`; `cik` is a
10-digit zero-padded string; `lookback` is an `int` (not a `bool`) from 1 to 1,260; `session` is a session in
`sessions.json`.

The **window** is the `lookback` sessions ending at `session`, inclusive. The answer, in order:
1. **`unknown`** if any of the following holds:
   - the window's first session is before `s_min`, or `session` is after `s_max`;
   - the CIK has no coverage record, no archive file, or incomplete pages;
   - an uncertain row of the CIK has the label in `may_carry`, its `earliest_session` is on or before `session`, and
     its `latest_session` is open or on or after the window's first session.
2. **`flagged`** if an observation of the CIK with the label has `available_session` in the window.
3. **`clear`** otherwise.

A row whose timing is in doubt affects every window from its earliest possible dissemination; a row whose timing is
known affects the windows that hold it. A filing made later never changes an earlier state. Because an `unknown`
can rest on a correction learnt later, a consumer must treat `unknown` symmetrically (for example, exclude the name
from both the book and its comparison), never as a signal.

### Corrections and earlier states

- **One snapshot per artefact.** A later archive, index or header set is a new artefact version with its `parent`
  named; earlier versions are never rewritten.
- **Version reconciliation**, printed between a version and its parent: inventory rows added and removed; per-row
  changes in form, filing date, items, membership, every header field, every field outcome, `row_class`,
  `available_session`, `earliest_session` and `latest_session`; per-CIK coverage records; `F`, `s_min`, `s_max`,
  `sessions.json` and `edgar_days.json`; and `state()` changes over gate 6's grid. The grid is a fixed sample of
  states; the per-row diff is the complete change record.
- **Corrections on candidates** are handled in the construction: a PAC after acceptance makes the row
  `timing_uncertain` until the correction date, after which the current header governs (premise 6: 68 candidates).
- **Known limit: corrections and removals the sources do not date.**
  - A PAC on the acceptance date itself is indistinguishable from none.
  - Non-candidates are read from the archive without a header. A PAC that removed a target code, or the SEC's
    removal of an accession (the guidance: "removals processed on subsequent business days will not be reflected in
    any previous daily, feed, or oldload index"), would be invisible there.
  - **Measured against it:** 0 of 2,000 sampled non-candidate headers differ from the archive, and 1 of them carries
    a PAC (premise 6); 0 drift between two snapshots (premise 7); 0 item differences on 15,661 accessions captured
    near filing (premise 3).
  - **A contemporaneous source exists and is not used:** the daily dissemination Feed (`.pc` files carry each PAC
    as disseminated). Reading one Feed day per filing day since 2004 is out of proportion to two labels; a consumer
    whose registration needs it names it there.
  - Every output carries "retrospectively captured filing metadata", and every consumer registration names the
    artefact version and states this limit.

## Census (printed by every run, stored in the manifest)

Per filing year, all CIKs and our CIKs (rows restricted to our CIKs) separately, with `(cik, accession)` rows and
distinct accessions counted apart:
- inventory rows by form and by source (index, archive, header-only);
- candidates by reason;
- observations per label and form;
- rows per class, and per field outcome;
- header items against archive items by archive list length.

Plus `F`, `s_min`, `s_max`, the CIKs with incomplete pages, and the maximum acceptance-minus-filing-date gap.

## Registration

The producer and its fixtures are written first and read no data (build step 1). Then, before any data census
including the dry run, the frozen configuration is registered as a non-claiming `DeclaredTrial` in
`app/services/trial_register.py` (`3624-slice2-labels-v1`, one configuration, `searches=1`). Its `evidence` names
this spec's sha256, the construction hash, the archive sha256 and the index files' sha256s. Every configuration run
is counted: a later change of label definition, form scope, candidate rule, availability rule, class rule or
`unknown` policy is a new configuration in the programme ledger.

## Gates (decision recorded on #3624 before any downstream use)

**What acceptance means.** An accepted artefact may be *read by a registered consumer*. It does not say the labels
cover enough of any universe: no consumer may read it without a registration that names a coverage requirement for
its universe and passes it against gate 5's census, recorded on #3624.

**Progression.** Accepted when all eight hold:
1. **Refusals:** `ARCHIVE_INCOMPLETE`, the coverage-end check and the 30-day lower-bound check did not fire.
2. **Census reproduction:** for our CIKs, the census reproduces premise 2's identity and validity counts (rows
   restricted to our CIKs) and the per-label, per-form observation counts in the measurement JSON's `labels`, in both
   units. Any difference is explained row by row (a header moves a row between observation and uncertain) or fails.
3. **Headers:** 0 `no_header` rows.
4. **Usable timing:** `timing_uncertain` rows are at most 2% of candidate rows, all CIKs (premise 6 on our CIKs:
   70 of 7,041). The threshold is fixed by construction.
5. **Survivorship census (the #3609 development universe only).** Over every `(series, formation month)` row of the
   stage-A artefact (`STAGE_A_ARTEFACT`, manifest sha256 `e5087210…` = `STAGE_A_MANIFEST_SHA256`, read through
   `read_verified_artefact`): rows by `link_reason`; for `LINKED` rows (the only ones with a `cik`), rows whose CIK
   has no coverage record or incomplete pages; and for each label, `flagged`, `clear` and `unknown` at the formation
   month's last session with `lookback` 252. Distinct CIKs are counted beside rows. Series-to-CIK linking is #3609's
   rule (`scripts/build_3609_factor_panel.py`), not re-decided here. Stage B is not read. **Pass:** it ran and is
   posted. It is the input to each consumer's coverage requirement, not a threshold here.
6. **A/B against the operator layer.** No research input uses these labels today (`grep` finds no consumer of `4.01`
   or `4.02` in `app/` or `scripts/`), so the comparison is with the stored layer. **Baseline constructor:**
   `filing_events` rows joined to our CIKs through `external_identifiers`, frozen into `ab_baseline.jsonl.gz`; one
   row per `(cik, accession)`; its items are the union of its non-NULL item sets; an all-NULL row carries no label;
   `available_session` from its filing date by the rule above; every one of our CIKs has a coverage record; no
   uncertain rows. `state()` is evaluated on both at every month-end session from `s_min` to `s_max`, lookbacks 21
   and 252, for our CIKs. **Pass:** every differing state is explained by one row-level cause: an accession absent
   from `filing_events`; all-NULL `filing_events` items; an uncertain artefact row; a differing filing date; a
   header-only or index-only row. Any other difference fails.
7. **Document check (a sample, not population evidence).** The frame per label is the sorted list of
   `(accession, cik)` rows of our CIKs, `FIRST` to `F`. `random.Random(3624).sample` draws without replacement:
   - 10 observations that are originals and 10 that are amendments;
   - 10 non-candidate originals and 10 non-candidate amendments.

   A stratum smaller than 10 is taken whole. Each filing's primary document is read on EDGAR. **Pass:** every
   observation's document contains the item's heading and no non-candidate's does. An unreadable document counts as
   a failure, never a replacement. Every draw and result is posted.
8. **Going-concern search:** completed with its result recorded, or recorded as `incomplete` with the failed inputs
   listed (below).

**Scope of each gate.** Gates 2, 3, 4 and 6 cover the full population they name; gate 7 is a sample; gate 5 is the
stage-A development universe.

**Remediation.** A failure in 1–4, 6 or 8 is fixed in the producer and re-run under a new construction hash, as a
new configuration. A failure in 7 is reported per accession; if the metadata disagrees with the document, the slice
records the error rate within the draw and stops before acceptance; a corrected treatment is a new registration.

**No-go.** The slice ends without an accepted artefact if gate 7 shows any metadata error, or if gate 1 cannot pass.

## Going-concern search (build step 3; its result is recorded, nothing is built from it)

- **Already checked:** none of the 83 concepts in `financial_facts_raw` matches `going|substantial.?doubt`
  (measurement JSON `financial_facts_raw_concepts`, query included). 8-K metadata has no such field.
- **Sources, each pinned as a list of URLs with sha256s:**
  - **Taxonomies:** the `us-gaap` and `dei` taxonomy packages for every release year 2009 to 2026, element names
    and documentation labels. `dei` holds the auditor elements.
  - **Usage:** SEC's Financial Statement and Notes data sets: every quarterly package the SEC's data-set page lists,
    and every monthly package after the last quarterly one, through the last published before the build.
- **Failure:** a source that cannot be fetched or read makes the search `incomplete`; it is never read as absence.
- **Match:** case-insensitive `going\s*concern|substantial\s+doubt` on element names (split at case changes) and
  documentation, and on the FSN TAG table's `tag` and `doc`, standard and custom tags alike.
- **Recorded per matching element or tag:** name, taxonomy and `version`, `custom`, `datatype`, documentation;
  distinct filer CIKs and filings using it, from NUM and TXT rows joined to SUB by `adsh` on `(tag, version)`; the
  first package in which it is used, named "first observed in the searched window".
- **A candidate** is a standard element whose documentation defines it as the going-concern disclosure or the
  auditor's going-concern conclusion. Auditor identity elements (`AuditorName`, `AuditorFirmId`, `AuditorLocation`)
  are recorded as context only.
- **Outcomes:** a candidate becomes a label only through its own build spec. With none, going-concern becomes a
  slice-3 candidate task with a deterministic baseline, as the programme states, and the negative is recorded for the
  searched sources only.

## Build order

1. The producer and fixtures, reading no data:
   - a synthetic zip with recent and history pages, a missing page, a misaligned page, a block without `items`, a
     repeat, a co-registrant pair, a conflict, two different invalid strings, every item state and a 10-code list;
   - synthetic `master` files with an index-only row, a gap day and a `Last Data Received` line;
   - headers for every field outcome in the table, including a PAC, a non-filer CIK and a header-only filer.
   - `state()` tests for every branch, bound and input refusal, and a filing dated before a holiday and a weekend.
2. Freeze the construction hash and register (above).
3. The going-concern search.
4. A dry run: inventory, candidates and census, no header fetch.
5. The header fetch, then publish.
6. Gates 1–8, with the decision posted on #3624.

The build rung is behavioural with data semantics: fixtures, Codex checkpoint 2, and gates 1–8 as the evidence.

## Not done, and why

- **No change to `filing_events`** or its ingest horizon. That layer serves the operator; the producer reads the SEC
  sources.
- **No fix to the acceptance-time defect.** That is #3714; the producer reads headers only.
- **No event linking, feature, window, horizon or outcome read.** These belong to a consumer's registration (§4 of
  the programme).

## Checkpoint log

- **Round 1** (`var/research/3624/ckpt1_slice2_r1.txt`): 37 findings. The spec was rebuilt on the fail-closed rule:
  per-accession observations, explicit coverage bounds, four item states, `(cik, accession)` rows, registration,
  survivorship census, A/B and a document gate.
- **Round 2** (`var/research/3624/ckpt1_slice2_r2.txt`): 19 open, 23 new (38–60). Rebuilt around the header and the
  dissemination index: availability from the header's Rule 13 filing date; a header for every row that could carry
  a label; the full-index reconciliation; re-measurement under the producer's identity over the whole archive; the
  33-code vocabulary; gates restated.
- **Round 3** (`var/research/3624/ckpt1_slice2_r3.txt`): 16 open, 23 new (61–83).
  - **Corrections** (3, 61): the PDS specification defines `DATE-OF-FILING-DATE-CHANGE` as the last post-acceptance
    correction; a correction after acceptance now makes the row timing-uncertain until the correction date.
    Non-candidate corrections and removals remain a stated limit with three measured checks; the Feed is named and
    declined.
  - **Negative side** (62): candidates now include lists of 10 or more codes; truncation measured only at 13
    codes; 2,000 sampled non-candidates with a stated bound; the limit is explicit rather than claimed away.
  - **Status contract** (14, 56, 63, 64, 82): every header field has its own outcome; rows fall in three classes;
    an empty item list is not valid; both interval ends are stored; the earlier date is an anomaly with no cause
    asserted.
  - **Timing** (40, 41, 65): label-uncertain rows use the observation's own timing; timing-uncertain rows start at
    the first EDGAR dissemination day on or after acceptance, from the index; `s_min` is 30 days after `FIRST` with a
    refusal; `sessions.json` is padded and the lookback bounded.
  - **Membership** (75, 81): filer CIKs are parsed from `<FILER>` blocks only and compared both ways (0
    differences); header-only filers become rows.
  - **Measurement fidelity** (20, 43, 44, 46, 67, 68, 69, 70, 73, 76, 78): rows restricted to our CIKs; per-set item
    comparison; the parser filter reported; conflict reason added; raw invalid strings kept; every listed page
    validated; missing `items` arrays counted; the amendment-key inventory back; the acceptance range corrected;
    per-year reconciliation with denominators.
  - **Evidence provenance** (19, 71, 72, 74): the `financial_facts_raw` query and the near-filing capture are now
    computed by the script; premise 5 cites the commit that measured it; the stale rows carry headers and archive
    end dates.
  - **Gates** (26, 28, 29, 33, 51, 53, 79, 80): stage A pinned by its manifest sha256 and the A/B baseline frozen
    into the artefact; reconciliation covers every state field; consumers must pass a recorded coverage gate before
    reading; the search covers `us-gaap` and `dei` taxonomies as well as usage; a 2% timing-uncertain ceiling;
    linking is #3609's `link_reason`; the A/B baseline constructor is specified; registration follows the code.
  - **Text** (66, 77, 83): coverage records for every archive CIK; Rule 13's commencement criterion; premise 2's
    heading.
