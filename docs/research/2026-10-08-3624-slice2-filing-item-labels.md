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
- **Authority:** each such accession's EDGAR SGML header gives its items, form, filing date, acceptance time and
  filer CIKs. Every label and every availability date comes from a header.

## Measurement and pinned inputs

Two commands, outputs committed beside this spec:

    RA="$HOME/Library/Application Support/eBull/research-artifacts/sha256"
    A="$RA/d6c42d554a703832a3a477a0b04b0f8cc96ce868180b1bb58e9be99ad717de8b/submissions.zip"
    B="$RA/928d67221c6e6183bc343e7234c1391448c15cd1dd644d36b425db2f99ba4350/submissions.zip"
    PYTHONPATH=. uv run python -m scripts.measure_3624_slice2_8k_items --archive "$A" --previous "$B" \
        --headers --negative-sample 300 --out docs/research/3624-slice2-8k-items-measurement.json
    PYTHONPATH=. uv run python -m scripts.measure_3624_slice2_8k_items --archive "$A" \
        --index-cache var/research/3624/full-index --out docs/research/3624-slice2-full-index-reconciliation.json

**Pinned inputs:**
- **Archive A:** `submissions.zip` sha256 `d6c42d55…`, captured 2026-10-07T08:05:08Z; last 8-K-family filing date
  2026-10-06.
- **Archive B:** sha256 `928d6722…`, captured 2026-08-24; last 8-K filing date 2026-08-21.
- **Full index:** 90 quarterly `master.gz` files, 2004 Q3 to 2026 Q4, each named by sha256 with its `Last Data
  Received` line in the reconciliation JSON.
- **Our CIKs:** the 5,310 CIKs in `external_identifiers` (`provider='sec'`, `identifier_type='cik'`), written out
  in full in the measurement JSON (`ciks`, sha256 `d1c01c68…`). They scope the A/B and the header measurement only;
  the producer reads every CIK.
- **Form 8-K:** SEC 873 (02-25), sha256 `730ab1de…`, fetched 2026-10-08 from `https://www.sec.gov/files/form8-k.pdf`.
- **Code:** the measurement script at this PR's head commit.

Database counts (`filing_events`, `eight_k_*`, `sec_filing_manifest`) are as of 2026-10-08 and are context for the
design and the A/B population. No gate reproduces them.

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
reason. No source links an amendment to its original: `amends_accession` is NULL on all 9,049 8-K/A rows of
`sec_filing_manifest`, and no `recent` key in the archive names an amendment. The producer therefore neither links
nor discards: each accession with the item is an observation with its form recorded. Collapsing observations into
events is a consumer's declared choice. The same holds for Item 4.01, whose Instruction makes a resignation or
dismissal "a reportable event separate from the engagement of a new independent accountant".

## Source rules

- **Form 8-K**, SEC 873 (02-25): Items 4.01 and 4.02, the Item 4.01 Instruction, Item 4.02(c)(3). Its 33 item
  headings (`grep -o 'Item [0-9]\.[0-9][0-9]'` on the pinned PDF) are the **item vocabulary**.
- **Release 33-8400** (2004): the current item numbering took effect on **2004-08-23**. Earlier 8-Ks use other
  codes, so the producer starts there (`FIRST`).
- **Form 8-K submission types:** every EDGAR type of the form present since `FIRST`: `8-K`, `8-K/A`, `8-K12B`,
  `8-K12B/A`, `8-K12G3`, `8-K12G3/A`, `8-K15D5`, `8-K15D5/A`; all eight occur in archive A.
- **Reg S-T Rule 13(a)** (17 CFR 232.13): a filing received by direct transmission before 5:30 p.m. Eastern on a
  business day is dated that day; later, the next business day. **Rule 13(b)** allows an adjusted filing date when
  technical difficulties delayed a good-faith filing.
- **SEC dissemination guidance** (`https://www.sec.gov/search-filings/edgar-search-assistance/accessing-edgar-data`):
  submissions begun after 5:30 p.m. "will be disseminated the next business day, showing up in the following
  business day's index", and indexes incorporating a business day's filings are updated nightly from about
  10:00 p.m. ET. It gives no hour by which a filing is public.
- **EDGAR full-index:** the `master` index is the "Master Index of EDGAR Dissemination Feed", one row per
  `(CIK, form, date filed, file)`, with a `Last Data Received` date.
- **EDGAR SGML header** (`<accession>.hdr.sgml` in the filing's folder; the `-index-headers.html` page is absent
  for older filings, 404 on 2006 accessions): `<ACCEPTANCE-DATETIME>` (Eastern wall clock), `<TYPE>`, `<ITEMS>` (one
  per code), `<FILING-DATE>`, `<DATE-OF-FILING-DATE-CHANGE>` and each filer's `<CIK>`. No SEC document defines
  `<DATE-OF-FILING-DATE-CHANGE>`, and it mostly equals the acceptance date, not the filing date (premise 6). It is
  recorded and not used.
- **Submissions JSON:** `items` aligned by index with `accessionNumber`
  (`.claude/skills/data-sources/sec-edgar.md` §"Submissions"). The JSON's `acceptanceDateTime` is never used
  (premise 5; #3714).

## Premises (measured)

1. **The archive is nearly complete against the dissemination index, and per-CIK files can be stale.**
   - **Index against archive**, every 8-K-family row from `FIRST`, as `(cik, accession)`: index 1,814,389 rows,
     archive 1,814,246. Index-only rows filed by the archive's last date: **2**, both 2026 Q3, both under a CIK
     whose archive file exists. Index-only after it: 141. Archive-only: **0**.
   - **One of the two, reconciled:** `0001193125-26-399416` (CIK 873303, 8-K, filed 2026-09-23, item 1.01). Its
     EDGAR header exists, the live `data.sec.gov` JSON lists it, and the archive's file for that CIK ends at
     2026-09-08. The archive's per-CIK files are not one consistent snapshot. This is the `filing_events`-only
     accession of round 1.
2. **Item metadata is well formed and conflict-free under the producer's definitions** (all CIKs, from `FIRST`):
   - **Identity:** 1,814,250 appearances, 1,814,246 `(cik, accession)` rows (4 repeats of a row on two pages),
     1,765,527 distinct accessions, 34,619 under more than one CIK.
   - **Conflicts:** 0 accessions have more than one `(form, filing_date, items)` variant. Round 1's "5 conflicts"
     came from the JSON acceptance time, which the old script compared.
   - **Item validity** against the vocabulary, exact ASCII codes, per appearance: 1,814,207 valid; 40 invalid (4
     ours); 3 empty (1 ours); 0 missing. The non-vocabulary tokens are bare integers (`7`, `5`, `12`, `2`, `4`, `9`), the
     pre-2004 numbering. No code is normalised.
   - **Pages:** 5,391 history pages listed, 0 absent, 0 misaligned; 0 of our CIKs absent from the archive.
3. **The archive agrees with `filing_events` on every shared field.** Population: our CIKs' 8-K-family
   accessions from `FIRST` through 2026-10-06, `filing_events` joined to `external_identifiers` by instrument.
   - **Items:** 399,534 shared accessions with no NULL `filing_events` row agree; 21 with mixed NULL and non-NULL
     rows agree on their non-NULL rows; 759 have only NULL rows. 0 differ.
   - **Form and filing date:** equal on all 400,314 shared accessions.
   - **Coverage:** archive-only accessions by year are in the measurement JSON (`filing_events.by_year`): 30,124 in
     2015, falling to 17 in 2026, the pattern of the 8-K ingest horizon (skill §11.2). The cause is not audited.
     `filing_events` holds 1 accession the archive lacks (premise 1).
   - **A contemporaneous capture:** 16,069 `filing_events` 8-K rows were written within 3 days of their filing date
     (`created_at::date - filing_date <= 3`; query in the build record). They are inside the 0-differ comparison.
4. **The typed body parser is not an independent completeness check.** Population: non-tombstone
   `eight_k_filings` accessions, left-joined to `eight_k_items`, that the archive also holds: 72,241. The parser
   stored no item rows for 52,353 of them. Within this overlap it never holds a target code the archive lacks
   (Item 4.01: 109 of 541 archive positives; Item 4.02: 25 of 94), and its misses are mostly accessions with no item
   rows (404 of 432; 66 of 69).
5. **The JSON acceptance time is unreliable and drifts** (#3714). Over our CIKs' shared accessions, archives B and
   A disagree on `acceptanceDateTime` for 176,886 of 416,480, mostly by +4h or +5h. It is not used.
6. **Headers, fetched for every candidate accession of our CIKs (6,882) and a seeded sample of 300 of their
   non-candidates** (measurement JSON `headers`; 0 fetch failures):
   - **Items:** 285 + 15 = all 300 non-candidates agree with the archive on items, form and filing date. Among the
     candidates, 10 differ on items: 4 carry pre-2004 codes on both sides, 1 is empty on both sides, and **5 are
     truncated in the archive**: a 13-code archive list against 14 or 15 codes in the header. No archive list
     shorter than 13 codes differs from its header. Archive lists of 13 or more codes: 63 accessions, all CIKs (41
     ours); they are therefore candidates (`ITEM_CAP`).
   - **Form and filing date:** 0 differences on all 7,182 headers.
   - **Filing date against acceptance date:** same day 6,634; later 546 (by 1 day: 459; 2: 4; 3: 70; 4: 13), every
     one accepted between 17:31 and 22:00 ET, as Rule 13(a) predicts; earlier 1 (`0001640334-22-000691`, accepted
     2022-04-01 14:22, dated 2022-03-31: a Rule 13(b) adjustment). 52 filings accepted after 17:30 keep their
     acceptance date, as Rule 13(a)'s "begin" allows.
   - **Missing acceptance:** 1 (`0001104659-09-022130`, an 8-K/A of 2009).
   - **Filer CIKs:** every archive CIK of an accession is a filer in its header.
   - **`DATE-OF-FILING-DATE-CHANGE`** equals the acceptance date on 7,098 of 7,166 headers with both fields and the
     filing date on 6,560 of them; it is recorded, not used.
7. **Snapshot drift, archive B to A, all CIKs** (`previous_snapshot`): of 1,759,461 accessions both hold, 0 changed
   CIK membership and 0 `(cik, accession)` rows changed form, filing date or items. 0 accessions of B are absent
   from A, and 0 of A filed by B's last date (2026-08-21) are absent from B. Two snapshots six weeks apart are two
   observations, not a guarantee.

## Construction

### Inventory and candidates

- **Inventory.** The `(cik, accession, form, date filed)` rows of every quarterly `master.gz` from 2004 Q3 to the
  quarter holding the coverage end, with form in the 8-K family and date filed ≥ `FIRST`, unioned with the
  archive's rows of the same scope. A row in only one source is recorded with its side.
- **Archive reading.** Every `CIK##########.json` and every listed `-submissions-NNN.json`. The producer refuses
  (`ARCHIVE_INCOMPLETE`) if a listed page is absent or a page's arrays differ in length.
- **Candidates.** An accession is a candidate when any of its rows:
  - is index-only (no archive items);
  - has items that are not valid (missing, empty, or a token outside the vocabulary);
  - has valid items including 4.01 or 4.02;
  - has valid items with 13 or more codes (`ITEM_CAP`; premise 6 found the archive truncating such lists);
  - or differs from another of its rows in form, filing date or items (a conflict; all variants are kept).
- **Non-candidates** have valid, agreeing items without a target code. They cannot carry a label, so their timing
  does not matter.

### Headers

- **One header per candidate accession**, fetched from the first CIK that lists it, at the shared SEC rate limit.
  Its bytes are stored in the artefact under `headers/<accession>.sgml` with their sha256.
- **Reuse.** The manifest names its `parent` artefact version, or none. Every header the parent holds is copied
  with its sha256 checked, never re-fetched. A header is never corrected in place.
- **Retries.** Three attempts per accession per run, 2 s then 8 s apart. An accession that fails all three is
  `no_header`, with its last error stored.
- **Parsed fields:** `acceptance_et` (the literal Eastern wall-clock value), `type`, `items`, `filing_date`,
  `filing_date_change`, `filer_ciks`.

### Row status

Each candidate `(cik, accession)` row carries three independent fields:
- **`header`:** `ok` or `no_header`.
- **`items`:** `valid` when the header's codes are all in the vocabulary, else `not_valid`; plus
  `agrees_with_archive` (true, false, or null for index-only rows).
- **`date`:** `ok` when `filing_date` is on or after `acceptance_et`'s date; otherwise `anomaly` (a Rule 13(b)
  adjustment).

A row is **`ok`** when its header is `ok`, its items are `valid` and agree with the archive (or it is index-only),
its date is `ok`, its header type is in the 8-K family, and its CIK is among the header's filers. Any other row is
**uncertain**: it may carry a label at a time the sources do not settle.

### Availability: a session, never an instant

- **`available_session`** is the first regular US equity session on a calendar date strictly after the header's
  `filing_date`.
- **Why the filing date.** Under Rule 13(a) the filing date is the business day on which the filing counts as
  received: the submission day before 5:30 p.m., otherwise the next business day. The dissemination guidance puts
  the late submissions in that next business day's index. The producer therefore takes the filing date as the
  dissemination day. It already follows the SEC's own calendar: a Columbus Day, when EDGAR is closed and the
  equity market open, moves the filing date, not this rule.
- **Why the next session.** The guidance gives no hour by which a filing is public. Skipping the whole filing date
  is the rule fixed by construction, and it is frozen in the construction hash.
- **Uncertain rows** carry an interval instead:
  - **`earliest_session`:** the first session on a date strictly after the header's acceptance date, or after the
    archive filing date when there is no header;
  - **`latest_session`:** the `available_session` rule's result when the header is `ok` and its date is `ok` (only
    the label is in doubt); otherwise none, an open end.

The equity calendar is `app/services/market_calendar.py::us_market_status`. Its sessions from `FIRST` to the
coverage end are written to the artefact as `sessions.json`, and `state()` reads only that file, so the calendar's
code and its `pandas` holiday primitives are frozen by content. No time zone conversion occurs anywhere.

### Coverage

- **Coverage end `F`:** the day before the coverage-end index's `Last Data Received` date. The guidance has the
  nightly index build start about 10:00 p.m. and finish "usually … within a few hours", so the last received day
  may be partial. The producer refuses if `F` is after the archive's last filing date plus one day; the archive is
  then too old for the index.
- **Upper bound `s_max`:** the last session on or before `F`. Any accession filed after `F` has an
  `available_session` after `F`.
- **Lower bound `s_min`:** the first session after `FIRST`. An accession filed before `FIRST` is available by
  `FIRST` itself (a session), so it cannot fall in a window that starts at `s_min` or later, unless its filing date
  was adjusted by more than the gap (a known limit, below).

### Output: a research artefact

Published under the research root with the step 1 reference-artefact protocol: exclusive `mkdir`, a clean checkout,
the manifest written last.

**The manifest names:** the archive sha256 and capture time; every `master.gz` sha256 and its `Last Data Received`;
`FIRST`, `F`, `s_min`, `s_max`; the `parent` version; the construction hash (the producer's import closure);
`sessions.json`'s sha256; every header's sha256; the census.

**Files:**
1. **`observations.jsonl.gz`:** one row per `ok` `(cik, accession)` whose header items include 4.01 or 4.02:
   `cik`, `accession`, `form`, `filing_date`, `acceptance_et`, `items`, `labels`, `available_session`.
2. **`uncertain.jsonl.gz`:** one row per uncertain `(cik, accession)`: the three status fields, every archive and
   index variant, the header fields where present, the stored error, and `earliest_session`.
3. **`coverage.jsonl.gz`:** one row per CIK in the inventory: whether its archive file exists and its pages are
   complete, and its inventory row count by year.
4. **`sessions.json`**, **`headers/`**, **`census.json`**.

No database table, migration or operator-visible surface changes.

### Label state

```
state(cik, label, session, lookback) -> "flagged" | "clear" | "unknown"
```

**Inputs are validated first**, or `state()` raises: `label` is `non_reliance` or `accountant_change`; `cik` is a
10-digit zero-padded string; `lookback` is an `int` (not a `bool`) ≥ 1; `session` is in `sessions.json`.

The **window** is the `lookback` sessions ending at `session`, inclusive. The answer, in order:
1. **`unknown`** if any of the following holds:
   - the window's first session is before `s_min`, or `session` is after `s_max`;
   - the CIK has no archive file, or its pages are incomplete;
   - an uncertain row of the CIK might carry the label (its header or archive items include the code, or its items
     are not valid on either side), its `earliest_session` is on or before `session`, and its `latest_session` is
     open or on or after the window's first session.
2. **`flagged`** if an observation of the CIK with the label has `available_session` in the window.
3. **`clear`** otherwise.

An uncertain row thus affects only states as of sessions it could already have reached. A row whose timing is in
doubt affects every later window; a row whose timing is known affects the windows that hold it. A filing made later
never changes an earlier state.

### Corrections and earlier states

- **One snapshot per artefact.** A later archive, index or header set is a new artefact version with its `parent`
  named; earlier versions are never rewritten.
- **Version reconciliation**, printed between a version and its parent: inventory rows added and removed; per-row
  changes in form, filing date, items, membership, every header field, the three status fields and
  `available_session`; `F`, `s_min`, `s_max` and `sessions.json`; and `state()` changes over the grid of gate 6.
- **Known limit: retrospective metadata.** Each version holds the sources as of its capture.
  - A *positive* rests on the header, which is the dissemination record; a code present in the archive but absent
    from the header makes the row uncertain, not an observation.
  - A *negative* rests on the archive's items for non-candidates. A code removed from an old accession's archive
    items after dissemination, or an accession removed from EDGAR (the guidance: "removals processed on
    subsequent business days will not be reflected in any previous daily, feed, or oldload index"), would be
    invisible.
  - **Evidence against it, all measured:** 0 item differences against 16,069 `filing_events` rows captured within
    three days of filing (premise 3); the B-to-A drift (premise 7); the header sample of non-candidates
    (premise 6).
  - **A contemporaneous source exists and is not used:** the daily dissemination Feed archives hold each day's
    submissions with their headers. Reading one per filing day since 2004 is out of proportion to two labels; a
    consumer whose registration needs it names it there.
  - Every output carries "retrospectively captured filing metadata", and every consumer registration names the
    artefact version and states this limit.
- **Known limit: adjusted dates outside the window.** An accession filed before `FIRST` with an adjusted date can
  be available after `s_min`. Premise 6 gives the measured filing-minus-acceptance distribution on our candidates.

## Census (printed by every run, stored in the manifest)

Per filing year, all CIKs and our CIKs separately, with `(cik, accession)` rows and distinct accessions counted
apart:
- inventory rows by form and by side (index-only, archive-only, both);
- candidates by reason;
- observations per label and form;
- uncertain rows per status field;
- header items against archive items (agree, disagree);
- filing date against acceptance date (same day, later, earlier).

Plus `F`, `s_min`, `s_max`, and the CIKs with incomplete pages.

## Registration

Before the build's first census, including the dry run, the producer configuration is registered as a non-claiming
`DeclaredTrial` in `app/services/trial_register.py` (`3624-slice2-labels-v1`, one configuration, `searches=1`). Its
`evidence` names this spec's sha256, the construction hash, the archive sha256 and the index files' sha256s. Each
configuration actually run is counted: a later change of label definition, form scope, availability rule, status
rule or `unknown` policy is a new configuration in the programme ledger.

## Gates (decision recorded on #3624 before any downstream use)

**What acceptance means.** An accepted artefact is fit to be *read*. It does not say the labels cover enough of any
universe for a research question: each consumer's registration sets and checks its own coverage requirement
against gate 5's census for its universe.

**Progression.** Accepted when all seven hold:
1. **Refusals:** `ARCHIVE_INCOMPLETE` and the coverage-end check did not fire.
2. **Census reproduction:** for our CIKs, the census reproduces premise 2's identity and validity counts and the
   per-label, per-form observation counts in the measurement JSON's `labels`, in both units, under the same
   definitions. Any difference is explained row by row (a header disagreement moves a row from observation to
   uncertain) or fails.
3. **Headers:** 0 `no_header` rows. A failure is re-fetched in a new version whose `parent` is the failed one.
4. **Document check (a sample, not population evidence).** The frame per label is the sorted list of
   `(accession, cik)` rows, our CIKs, `FIRST` to `F`. `random.Random(3624).sample` draws without replacement:
   - 10 observations that are originals and 10 that are amendments;
   - 10 non-candidate originals and 10 non-candidate amendments.

   A stratum smaller than 10 is taken whole. Each filing's primary document is read on EDGAR. **Pass:** every
   observation's document contains the item's heading and no non-candidate's does. An unreadable document counts
   as a failure, never a replacement. Every draw and result is posted.
5. **Survivorship census (development universe only).** Over every admitted `(series, formation month)` of the
   #3609 stage-A artefact (`STAGE_A_ARTEFACT`, manifest sha256 `STAGE_A_MANIFEST_SHA256`, read through
   `read_verified_artefact`): series-months with no linked CIK; with a CIK absent from the inventory; with
   incomplete pages; and, for each label at the formation month's last session with `lookback` 252, the count of
   `flagged`, `clear` and `unknown`. Distinct issuers are counted beside series. Stage B is not read. **Pass:** it
   ran and is posted; it is the input to each consumer's coverage requirement, not a threshold here.
6. **A/B against the operator layer** (no research input uses these labels today; `grep` finds no consumer of
   `4.01` or `4.02` in `app/` or `scripts/`). For our CIKs, `state()` is evaluated twice at every month-end
   session from `s_min` to `s_max`, lookbacks 21 and 252: on the artefact, and on observations built the same way
   from `filing_events` (items, form and filing date; `available_session` from the filing date; no uncertain
   rows). **Pass:** every differing state falls in one listed cause: an accession absent from `filing_events`;
   NULL `filing_events` items; a header that disagrees with the archive; an uncertain row; a filing date that
   differs. Any other difference fails.
7. **Going-concern search:** completed with its result recorded, or recorded as `incomplete` with the failed
   inputs listed (below).

**Remediation.** A failure in 1–3, 6 or 7 is fixed in the producer and re-run under a new construction hash, as a
new configuration. A failure in 4 is reported per accession; if the metadata disagrees with the document, the slice
records the error rate within the draw and stops before acceptance; a corrected treatment is a new registration.

**No-go.** The slice ends without an accepted artefact if gate 4 shows any metadata error, or if gate 1 cannot pass.

## Going-concern search (build step 1; its result is recorded, nothing is built from it)

- **Already checked:** the 83 concepts in `financial_facts_raw` include none matching `going` or
  `substantial doubt` (query in the build record). 8-K metadata has no such field.
- **Source:** SEC's Financial Statement and Notes data sets: every quarterly package the SEC's data-set page lists,
  and every monthly package after the last quarterly one, through the last package published before the build. The inventory is
  frozen as a list of URLs with sha256s. A package that cannot be fetched or read makes the search `incomplete`;
  it is never read as absence.
- **Match:** case-insensitive `going\s*concern|substantial\s+doubt` on the TAG table's `tag` (split at case
  changes) and `doc`, standard and custom tags alike.
- **Recorded per matching tag:** `tag`, `version`, `custom`, `datatype`, `doc`; distinct filer CIKs and filings
  using it, from NUM and TXT rows joined to SUB by `adsh`; and the first package in which it is used, named "first
  observed in the searched window".
- **A candidate** is a standard tag whose `doc` defines it as the going-concern disclosure or the auditor's
  going-concern conclusion. Auditor identity tags (`AuditorName`, `AuditorFirmId`, `AuditorLocation`) are recorded
  as context only.
- **Outcomes:** a candidate becomes a label only through its own build spec. With none, going-concern becomes a
  slice-3 candidate task with a deterministic baseline, as the programme states, and the negative is recorded
  within the searched packages.

## Build order

1. The registration (above), then the going-concern search.
2. The producer and fixtures:
   - a synthetic zip with recent and history pages, a missing page, a misaligned page, a repeat, a co-registrant
     pair, a conflict, every item state and a non-vocabulary token;
   - synthetic `master` files with an index-only row and a `Last Data Received` line;
   - headers with a matching item set, a disagreeing one, a missing acceptance and an adjusted date.
3. `state()`, with a fixture test for every branch, every bound, every input refusal, and a filing date before a
   holiday and a weekend.
4. A dry run: inventory, candidates and census, no header fetch.
5. The header fetch, then publish.
6. Gates 1–7, with the decision posted on #3624.

The build rung is behavioural with data semantics: fixtures, Codex checkpoint 2, and gates 1–7 as the evidence.
Gates 2, 3 and 6 cover the full population of our CIKs; gate 4 is a sample; gate 5 is the stage-A development
universe.

## Not done, and why

- **No change to `filing_events`** or its ingest horizon. That layer serves the operator; the producer reads the
  SEC sources.
- **No fix to the acceptance-time defect.** That is #3714; the producer reads headers only.
- **No event linking, feature, window, horizon or outcome read.** These belong to a consumer's registration (§4 of
  the programme).

## Checkpoint log

- **Round 1** (`var/research/3624/ckpt1_slice2_r1.txt`): 37 findings. The spec was rebuilt on the fail-closed rule:
  per-accession observations, explicit coverage bounds, four item states, `(cik, accession)` rows, registration,
  survivorship census, A/B and a document gate.
- **Round 2** (`var/research/3624/ckpt1_slice2_r2.txt`): 19 round-1 findings open, 23 new (38–60). The construction
  was rebuilt around the header and the dissemination index:
  - **Availability** comes from the header's Rule 13 filing date, which already follows the SEC calendar; no hour is
    assumed, and no time zone is converted (1, 38, 36).
  - **Every row that could carry a label has a header**, so no filing-date margin remains. An uncertain row blocks
    `state()` from its earliest possible session, never earlier, and to an open end when its timing is in doubt
    (12, 14, 39, 40, 41).
  - **Completeness:** the full-index reconciliation found the round-1 unexplained accession is a stale archive
    file; index-only rows are now candidates (11, 21). The coverage end comes from the index's `Last Data Received`,
    one day back (10).
  - **Re-measured under the producer's identity**, over the whole archive: 0 conflicts, rows and accessions counted
    apart, mixed-NULL accessions compared, form and date agreement measured, drift by membership sets (20, 22, 42,
    43, 44, 45, 49, 57, 60). The typed-parser population is named (46).
  - **Vocabulary** of 33 codes from the pinned form, exact match, no normalisation (47). Conflicts keep every
    variant and send the accession to a header (48). The header check found the archive truncating 13-code item
    lists, so such lists are candidates too.
  - **Retrospective metadata:** positives rest on headers; the negative-side limit is stated with three measured
    checks, and the Feed source is named and declined (3).
  - **Gates:** acceptance is defined as fit-to-read with coverage left to consumers (29, 51, 53); gate scopes are
    stated (50); the A/B compares `state()` (30); gate 4's frame and strata are frozen (32); headers must be complete
    (51, 56); the search is a gate (58).
  - **Search:** FSN packages with monthly releases, usage counts from NUM/TXT joined to SUB, failure as `incomplete`
    (33, 54, 59); the `financial_facts_raw` check is a measured query (19).
  - **Pinning:** the CIK list is written out; stage A by manifest sha256; header bodies by sha256 (26). Reconciliation
    covers every state-affecting field (28). Registration precedes the dry run (31, 52). `state()` validates its
    inputs (55). Header lineage, retries and status fields are independent (56). The Item 304(a)(3) deadline
    summary is gone (24).
