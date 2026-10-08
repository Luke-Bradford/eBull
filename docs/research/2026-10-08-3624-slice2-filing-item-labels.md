# #3624 slice 2 build spec: structured 8-K item labels (2026-10-08)

Programme: `docs/research/2026-10-06-3624-llm-research-component.md` §5 "Slice 2", which lists what this spec must
contain. This is a **label producer, not a strategy**. Any factor or filter that uses these labels supplies its own
hypothesis, event-to-feature rule, persistence, expiry, horizon, track, power assessment, coverage requirement and
gate before it reads outcomes. The producer reads no prices and no returns.

**Design rule: when in doubt, `unknown`.** No rule below links, infers or estimates anything a structured field
does not state. Missing or unresolved item metadata and unresolved timing are `unknown`, never a negative. A window
is `flagged` or `unflagged` only when the answer is the same under every possibility the construction leaves open
(§"Label state").

**Estimand: acceptance-keyed labels under a declared availability rule.** The programme asks for labels "keyed to
each accession's acceptance time" (`docs/research/2026-10-06-3624-llm-research-component.md` §5, lines 180–184).
This PR amends that section (§5, "Estimand and guarantee") to say what the key means and what a negative
guarantees, because no pinned source records when a historical filing became public:
- **Acceptance-keyed:** every label belongs to one accession and is never available on or before that accession's
  acceptance date. `available_session` is used only for rows whose acceptance, filing date and date order are ok,
  and the filing date is then on or after the acceptance date, so `available_session` is always after it.
- **The declared rule:** an `ok` row is available from the first session after the later of its Rule 13 filing date
  and its first daily listing day `D`, or after the filing date alone when no daily file lists the row. The whole
  formula, the maximum, the fallback and the next-session step, is the declared construction, as the programme's
  data-treatment hierarchy (§4, lines 103–104) allows where no source rule fixes a degree of freedom. It is informed
  by Rule 13(a), which fixes the legal filing date, and the guidance's "following business day's index"; neither
  prescribes it. It may be later than the
  real public time, never earlier than acceptance; it is not a recorded publication time.
- **Doubt:** every non-`ok` row is in doubt from its earliest possible session to `capture`, so no bound depends on
  an index file's write time.
- **What a negative guarantees:** no label among the rows the pinned sources hold, under the declared rule. Rows no
  pinned source holds, hidden filers and same-day corrections are known limits (§"Coverage"), stated with what is
  measured about them and restated by every consumer; they are not turned into `unknown`.

The checkpoint judges point-in-time safety against this estimand and the documented rules, and reports any
departure from them.

**Four sources, four jobs.**
- **Dissemination record:** EDGAR's daily `master.YYYYMMDD.idx` files. Each lists the filings of one dissemination
  day. A row's first listing gives its listing day `D`. The directory listing's write time is recorded as a
  diagnostic (`listing_lag`) and bounds nothing.
- **Authority:** each accession's EDGAR SGML header gives its accession number, items, form, filing date, acceptance
  time, last post-acceptance correction date, filer CIKs and submission markers. Every build fetches a header afresh
  for **every** inventory row of a covered CIK.
- **Inventory cross-checks:** the quarterly full-index `master.gz` files and the bulk `submissions.zip` archive. Every
  row any of them holds is in the inventory.
- **Coverage population:** our CIKs (below). A CIK seen in any source but not ours has a coverage record with
  `covered = false`; a CIK seen nowhere has no record; both read `unknown`.

## Measurement and pinned inputs

Three commands, outputs committed beside this spec. The third reads the first's output.

    RA="$HOME/Library/Application Support/eBull/research-artifacts/sha256"
    A="$RA/d6c42d554a703832a3a477a0b04b0f8cc96ce868180b1bb58e9be99ad717de8b/submissions.zip"
    B="$RA/928d67221c6e6183bc343e7234c1391448c15cd1dd644d36b425db2f99ba4350/submissions.zip"
    M="docs/research/3624-slice2-8k-items-measurement.json"
    PYTHONPATH=. uv run python -m scripts.measure_3624_slice2_8k_items --archive "$A" --previous "$B" \
        --headers --negative-sample 2000 --out "$M"
    PYTHONPATH=. uv run python -m scripts.measure_3624_slice2_8k_items --archive "$A" \
        --index-cache var/research/3624/full-index --out docs/research/3624-slice2-full-index-reconciliation.json
    PYTHONPATH=. uv run python -m scripts.measure_3624_slice2_8k_items --archive "$A" \
        --daily-index-cache var/research/3624/daily-index --index-cache var/research/3624/full-index \
        --measurement "$M" --out docs/research/3624-slice2-daily-index.json

Headers and index files are cached under `var/research/3624/`, one file per accession or index day. Each header row
carries its body's sha256. The third output's `inputs` block holds the sha256 of the archive, of the first output,
of every quarterly file and of every daily directory listing. Each daily file entry binds its body to its listing:
the body's sha256, the listing's name and sha256, the body's cache time, and the listing's raw `last-modified`
with its precision (`second` or `date`; no time zone is stated). This is a recorded pairing of independently
captured inputs: nothing checks that a listing's `last-modified` describes the body version cached beside it,
which is acceptable only because `last-modified` bounds nothing.

**Pinned inputs:**
- **Archive A:** `submissions.zip` sha256 `d6c42d55…`, captured 2026-10-07T08:05:08Z; last 8-K-family filing date
  2026-10-06.
- **Archive B:** sha256 `928d6722…`, captured 2026-08-24; last 8-K filing date 2026-08-21.
- **Quarterly index:** 90 `master.gz` files, 2004 Q3 to 2026 Q4, each named by sha256 with its `Last Data Received`
  line in the reconciliation JSON.
- **Daily index:** 5,516 `master.YYYYMMDD.idx[.gz]` files from 2004-08-23 to 2026-10-07, each with its sha256, its
  `Last Data Received` line and its listing's `last-modified` (`file_list`). Six quarters publish only the `.idx.gz`
  form (2011 Q3 and Q4, 2013 Q1, Q3 and Q4, 2014 Q2; `file_list`).
- **Our CIKs:** the 5,310 CIKs in `external_identifiers` (`provider='sec'`, `identifier_type='cik'`), written out in
  full in the measurement JSON (`ciks`, sha256 `d1c01c68…`).
- **Form 8-K:** SEC 873 (02-25), sha256 `730ab1de…`, from `https://www.sec.gov/files/form8-k.pdf`.
- **EDGAR PDS Technical Specification**, version 2.0, March 2025 (sha256 `fd9d0359…`), from
  `https://www.sec.gov/info/edgar/specifications/pds_dissemination_spec.pdf`.
- **Code:** every output's `provenance` block records the executing script's sha256 (`dd48f0b4…`), `HEAD`
  (`c7ea8ba9`) and that the script was clean at `HEAD`. All three outputs carry the same block.

Database figures (`filing_events`, `eight_k_*`, `sec_filing_manifest`, `financial_facts_raw`, `sec_8k_item_codes`)
are as of 2026-10-08, read by the first command (queries in the script).

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
  headings (`grep -o 'Item [0-9]\.[0-9][0-9]'` on the pinned PDF) are the **item vocabulary**. A header item list is
  a label source only when every code is in it. **General Instruction D** (p. 4): "The report shall contain the
  number and caption of the applicable item"; items not required "may be omitted and no reference thereto need be
  made". Only Item 1.01 may go without its number and caption, so a report under Item 4.01 or 4.02 carries both
  (gate 7).
- **Release 33-8400** (2004): the current item numbering applies from **2004-08-23** (`FIRST`). The producer does
  not argue from it about older rows: every row the sources hold from `FIRST` on is read, and a row with the earlier
  numbering is decided by its header like any other (its items are not valid, so its label is `unknown`).
- **Form 8-K submission types:** the PDS `<ITEMS>` definition lists the 8-K types it applies to: `8-K`, `8-K/A`,
  `8-K12B`, `8-K12B/A`, `8-K12G3`, `8-K12G3/A`, `8-K15D5`, `8-K15D5/A`. All eight occur in archive A.
- **Reg S-T Rule 13(a)** (17 CFR 232.13): a filing by direct transmission *commencing* on or before 5:30 p.m.
  Eastern on a business day is deemed filed that day; commencing later, the next business day. **Rule 13(b)** lets
  the Commission adjust the filing date when technical difficulties delayed a good-faith filing.
- **SEC dissemination guidance** (`https://www.sec.gov/search-filings/edgar-search-assistance/accessing-edgar-data`):
  - "Some filing submissions that begin after 5:30 p.m. ET … will be disseminated the next business day, showing up
    in the following business day's index".
  - Indexes incorporating a business day's filings are updated nightly from about 10:00 p.m. ET.
  - "removals processed on subsequent business days will not be reflected in any previous daily, feed, or oldload
    index".
  - It gives no hour by which a filing is public.
- **EDGAR daily index:** "Daily Index of EDGAR Dissemination Feed", one row per `(CIK, form, date filed, file)` for
  one day, with a `Last Data Received` date. Data rows follow the `CIK|…` column header and its dashed separator.
  A file's directory listing (`index.json`) states its `last-modified`, to the second or to the day, with no time
  zone. No source makes that a publication time, and files are rewritten after their day (premise 8), so it is
  recorded and bounds nothing.
- **EDGAR PDS specification:**
  - `<FILING-DATE>`: "EDGAR assigned official filing date, or post acceptance new filing date (Post Acceptance
    Correction)";
  - `<DATE-OF-FILING-DATE-CHANGE>`: "Date when the last Post Acceptance occurred";
  - a post-acceptance correction (PAC) carries "header tag changes to previously-filed submissions, deletion notices
    … or form/document type changes", and "No document text is updated in a PAC";
  - so a header whose correction date equals its acceptance date has had no header change after the acceptance
    day, and its items are the items disseminated (the one undetectable case is a PAC on the acceptance day itself);
  - `<ITEMS>`: "Identifies 1 or more items declared in the filings", format `#.##`;
  - filer CIKs sit inside `<FILER>` blocks; other roles (`<SUBJECT-COMPANY>` and the like) are separate;
  - `<ACCESSION-NUMBER>`: "EDGAR assigned identifier unique to each submission. Required." (p. 42);
  - post-acceptance correction evidence: `<CORRECTION>` "identifies post acceptance correction transaction";
    `<DELETION>` identifies a correction deleting the filing (or, among filing values, a file number); and
    `<TIMESTAMP>` is the "Edgar Dissemination date-time, used for Post Acceptance Corrections only" (pp. 41–42,
    46). None of the 9,041 inspected `.hdr.sgml` headers carries any of the three (premise 6's raw tag census).
    Wherever one occurs the row is `items_unestablished` (field `correction evidence`); the producer never relies
    on their absence. The correction date is a processing date and is never used as a time;
  - three markers say a submission is not an ordinary electronic filing, and each is a field outcome below:
    - `<PAPER>` marks a paper submission, whose EDGAR document is a stub (pp. 6, 8, 42);
    - `<PRIVATE-TO-PUBLIC>` "identifies an SEC-approved private-to-public document dissemination" and "is only
      present in a *.pr dissemination" (p. 42), documents the SEC releases from private status later (p. 6);
    - `<CONFIRMING-COPY>` "defines this submission as being a confirming copy to a paper submission" (p. 43).
- **EDGAR SGML header:** `<accession>.hdr.sgml` in the filing's folder, the only header form the producer reads.
- **Submissions JSON:** `items` aligned by index with `accessionNumber`
  (`.claude/skills/data-sources/sec-edgar.md` §"Submissions"). It is an inventory cross-check only. The JSON's
  `acceptanceDateTime` is never used (#3714).

## Premises (measured)

1. **The sources nearly agree on membership; the daily files alone hold a few hundred unexplained rows**
   (reconciliation and daily-index JSONs; 8-K family, rows `(cik, accession)`).
   - **Quarterly against archive:** quarterly 1,814,389 rows, archive 1,814,246. Quarterly-only rows filed by the
     archive's last date: **2**, both 2026 Q3, both under a CIK whose archive file ends before the accession's date
     (2026-09-08 against 2026-09-23; 2026-09-15 against 2026-09-17). Each has a header naming the CIK as a filer.
     Quarterly-only after the archive's last date: 141. Archive-only: **0**.
   - **Daily against both** (`reconciliation`), against the quarterly and archive rows of every date, so both
     denominators are reported:
     - all 1,810,835 distinct daily rows: 1,810,284 in the quarterly index, 1,810,191 in the archive, **501 in
       neither**;
     - the 1,810,765 rows dated `FIRST` or later: 1,810,263, 1,810,121 and the same **501**
       (`daily_in_neither_by_year`, zero-filled: 0 to 70 a year, 0 only in 2025);
     - the 70 rows dated before `FIRST`: all 70 in the archive, 21 in the quarterly index, 0 in neither.
   - **The 501 are unexplained membership differences, not proven removals.** They are 490 distinct accessions.
     Rescanned against the quarterly files with no form or CIK filter, 498 rows (487 accessions) appear nowhere in
     them and 3 rows (3 accessions) appear under another CIK; none appears under the same CIK with another form. The guidance's removal rule is one explanation; a correction of form, CIK or
     date is another, and the sources do not say which.
   - **Both against daily:** 4,126 quarterly rows are in no daily file. 22 header rows are in none; all 20 listed
     in `examples.not_in_daily` fall on days with no daily file in the listing (for example 2010-07-13 to 07-16 and
     2023-07-06 to 07-14).
2. **Item metadata in the archive: identity, conflicts and validity** (all CIKs from `FIRST`; `identity`,
   `validity`):
   - **All CIKs:** 1,814,250 appearances, 1,814,246 rows, 1,765,527 distinct accessions, 34,619 under more than one
     CIK. The 4 excess appearances are 4 rows that each appear exactly twice (`all_rows_appearing_2_times`).
   - **Our CIKs, rows restricted to them:** 710,849 appearances, 710,846 rows, 710,665 accessions.
   - **Conflicts:** 0 accessions have more than one `(form, filing_date, items)` variant, comparing raw invalid item
     strings as written.
   - **Item validity**, exact ASCII codes against the vocabulary, per appearance: 1,814,207 valid, 40 invalid (4
     ours), 3 empty (1 ours), 0 missing; 0 blocks lack an `items` array. The non-vocabulary tokens are bare integers
     (`7`, `5`, `12`, `2`, `4`, `9`), the pre-2004 numbering. No code is normalised.
   - **Pages:** 987,854 CIK files; every file has a valid history listing (0 missing, 0 invalid); 5,391 pages
     listed, 0 absent, 0 missing a required array, 0 misaligned, 0 unlisted page files; 0 of our CIKs absent.
3. **The archive agrees with `filing_events` on every shared field** (`filing_events`). Population: our CIKs'
   8-K-family accessions from `FIRST` through 2026-10-06, `filing_events` joined to `external_identifiers` by
   instrument; each distinct non-NULL `filing_events` item set is compared on its own.
   - **Items:** 399,534 shared accessions with no NULL row: every item set equals the archive's. 21 with NULL and
     non-NULL rows: every non-NULL set equals it. 759 have only NULL rows. 0 differ.
   - **Denominators** (`by_year`): event rows and event accessions dated within the window. Accessions dated after
     the archive's last date are counted apart (97 accessions and 97 event rows in 2026). The 2026 in-window
     denominator is 39,604 accessions: 39,603 shared and 1 events-only.
   - **Form and filing date:** equal on all 400,314 shared accessions.
   - **Coverage:** archive-only accessions by year are in `by_year`: 30,124 in 2015, falling to 17 in 2026, the
     shape of the 8-K ingest horizon (skill §11.2). The cause is not audited.
   - **Captured near filing:** 15,661 shared accessions have their earliest `filing_events` row written 0 to 3
     Eastern days after the filing date. 15,655 of them have item sets, and the current stored sets agree with
     archive A (15,638 no-NULL, 17 mixed); the other 6 are all NULL. `filing_events` keeps current values only, so
     this shows current agreement, not that nothing changed.
4. **The typed body parser is not an independent completeness check** (`cross_source`). Population: the 72,364
   non-tombstone `eight_k_filings` accessions at this run (live ingest grows the table between runs;
   `cross_source.typed_accessions` is the figure of record), left-joined to `eight_k_items`; 72,241 are in the archive, all with
   valid items there. The parser stored no item rows for 52,353 of them. Within the overlap it never holds a target
   code the archive lacks (Item 4.01: 109 of 541 archive positives; Item 4.02: 25 of 94), and its misses are mostly
   accessions with no item rows (404 of 432; 66 of 69).
5. **The JSON acceptance time is unreliable** (#3714, measured at this PR's first commit `b00a2bdd`, whose JSON
   `previous_snapshot` and `acceptance` blocks hold the figures). It is not read.
6. **Headers on a selected set: every archive candidate of our CIKs (7,041 accessions) and a seeded sample of 2,000
   of their other accessions** (`headers`; 9,041 fetched, 0 failures). The production rule fetches a header for
   every row, so these figures show the field outcomes' shape, not the population's rates.
   - **Candidate** is the script's `candidate_reasons()`: an accession of our CIKs with more than one
     `(form, filing_date, items)` variant, any appearance whose items are not valid (missing, empty or invalid), a
     target code, or 10 or more codes (`ITEM_CAP`, set after round 2 found 13-code lists truncated). It is wider
     than the target positives; `labels` counts those.
   - **Structure:** every header has exactly one `<SEC-HEADER>` block (`header_block_counts`), and every one has
     exactly one `<ACCESSION-NUMBER>`, equal to the accession requested (`field_outcome_rows`). On these 9,041 the
     raw tag census (`raw_tag_presence`, headers carrying each tag) finds `<PRIVATE-TO-PUBLIC>` on 65 and
     `<CONFIRMING-COPY>` on 3, all inside the header block (`marker_presence_in_header_block`), and **0**
     `<TIMESTAMP>` and **0** `<PAPER>`. These are observations on the inspected set, not format rules.
   - **Items against the archive:** 5 archive lists are cut short of their header (13 archive codes, 14 or 15 in
     the header); no shorter archive list disagrees (`archive_item_count_by_disagreement`). 4 carry pre-2004 codes
     on both sides, all dated `FIRST` and accepted after 17:30 on Friday 2004-08-20; 1 is empty on both sides.
   - **Form, filing date and filer membership:** 0 differences from the archive on all 9,041.
   - **Filing date against acceptance date:** same day 8,354; later 685 (by 1 day: 579; 2: 4; 3: 87; 4: 15), every
     one accepted between 17:31:14 and 22:00:21 ET; earlier 1 (`0001640334-22-000691`, accepted 2022-04-01 14:22,
     dated 2022-03-31; cause not established). 60 filings accepted after 17:30 keep their acceptance date, which Rule
     13(a)'s "commencing" allows. 1 has an empty acceptance tag (`0001104659-09-022130`).
   - **The correction field** (`correction_field`): later than acceptance on 69 (68 candidates, 1 sampled), by 1 to
     726 days; equal on 8,953; absent on 18 (17 and 1); not comparable on 1 (the empty acceptance); 0 earlier; 0
     invalid. None of the 69 later ones has a header item list that differs from the archive.
   - **Classes under the production predicates** (`class_rows`, `class_accessions`; `field_outcomes` and
     `row_class` in the script implement §"Headers" and §"Row classes"). Of our CIKs' 7,042 candidate rows, 6,949
     are `ok`, 92 `items_unestablished`, 1 `timing_in_doubt` and 0 `label_in_doubt`. Of the 2,001 rows of the
     sampled accessions, 1,999 are `ok` and 2 `items_unestablished`. The 94 `items_unestablished` rows: 62 corrected
     and `<PRIVATE-TO-PUBLIC>`, 7 corrected only, 3 `<PRIVATE-TO-PUBLIC>` only, 3 `<CONFIRMING-COPY>`, 18 without a
     correction date (4 of them with pre-2004 items and 1 with empty items), and the 1 with an empty acceptance tag.
     The `timing_in_doubt` row is the earlier-dated `0001640334-22-000691`.
7. **Snapshot drift, archive B to A, all CIKs** (`previous_snapshot`): of 1,759,461 accessions both hold, 0 changed
   CIK membership and 0 rows changed form, filing date or items. 0 accessions of B are absent from A, and 0 of A
   filed by B's last date are absent from B.
8. **The index files' organisation and write times** (reconciliation `row_placement_by_date_filed`; daily-index
   `reconciliation`, `write_lag_days`, `headers_against_daily`):
   - **Quarterly:** all 23,506,845 rows of the 90 files carry a date filed inside their file's quarter. A quarter's
     file is organised by date filed.
   - **Daily, day against date filed:** 1,810,056 of 1,810,835 rows first appear in the file for their own date
     filed; 643 appear 1 to 5 days later and 136 further away. The header set agrees: 9,012 of the 9,019 listed
     headers first appear on their filing date, 6 one day later and 1 three days earlier
     (`0001213900-21-033058`). 22 headers are in no daily file (premise 1).
   - **Daily, write lag** (listing `last-modified` minus the file's day): 4,615 files written the same day, 314
     within 1 to 7 days, **587 more than 7 days later**. 250 of the 587 are the whole of 2012, rewritten on
     2014-07-30 (576 to 939 days). 1,486,301 rows first appear in a same-day file and 193,292 in a file written
     more than 7 days late.
   - **Index parsing** (`index_parse` in both JSONs): under the parse contract, 0 lines rejected and 0 names
     holding `|` in all 5,516 daily and 90 quarterly files. Every daily row is dated `YYYYMMDD` (23,708,147) and
     every quarterly row `YYYY-MM-DD` (23,506,845).
   - **Before `FIRST`:** 70 rows in daily files from `FIRST` on are dated before it. The archive holds 364,314 pre-`FIRST`
     8-K-family appearances (364,314 distinct rows): 364,271 with non-vocabulary items, 42 empty, 1 valid, and
     **0 with a 4.01 or 4.02 token**, counted token by token whatever the rest of the list (`pre_first`).
9. **Consumer audit** (`consumer_audit`, every hit listed with its command and revision; `severity`, with its
   query):
   - **Readers of the item codes:**
     - `app/services/filings_risk.py` reads `sec_8k_item_codes`. 4.01 and 4.02 are both `critical` there.
     - `app/services/sec_filing_items.py` writes `filing_events.red_flag_score` from `filing_events.items` through
       it, and `scripts/backfill_red_flag_score.py` recomputes it from the same column. The other three
       `score_filing_red_flag` call sites pass `None` for the items (`app/services/filings.py:865`, `:960`;
       `app/services/fundamentals/__init__.py:3042`; `git grep -n score_filing_red_flag -- app scripts` at this
       PR's head).
     - `app/services/eight_k_events.py` copies labels and severities onto `eight_k_items`.
     - `app/services/strategy_shock_mechanism.py` belongs to the shock-event family cut on 2026-08-22.
     - Two `scripts/*2900*` files audit the red-flag path.
   - **Readers of the score:** `red_flag_score` has 95 hits in 22 files, among them scoring, portfolio, the position
     monitor and the r6 research modules.
   - **Scope:** these are the literal hits of three text searches, classified by reading them. They are not a call
     graph or a data-flow analysis, they do not establish which hits are live, and a reader that names neither the
     codes, the table nor the score is outside them. Two paths are visible: `filing_events.items` (through
     `red_flag_score`) and the typed `eight_k_items`. Gate 6 compares with `filing_events.items` only and claims no
     more.

## Construction

### Inventory

- **Estimand.** Rows dated `FIRST` or later, in every source. A row dated before `FIRST` is outside the estimand by
  declaration, whatever its later item history, and is counted in the census, not inventoried. This is a declared
  restriction of what the labels describe (Release 33-8400's numbering starts at `FIRST`), not an argument that such
  rows cannot matter; premise 8 records their measured shape.
- **Rows.** Every 8-K-family `(cik, accession)` row dated `FIRST` or later in any source, with its sources recorded:
  - every daily file dated `FIRST` to `F`;
  - every quarterly file from 2004 Q3 to the quarter holding `F`;
  - the archive;
  - any header filer CIK not already a row (a **header-only** row).
- **Listing.** A row's **listing day** `D` is the first daily file listing it; a row in no daily file has none. The
  row records `listing_lag`, its first file's `last-modified` date minus `D`, or null, as a diagnostic.
- **Index reading.** One parse contract for daily and quarterly files. A file must have the `CIK|…` column header
  followed by a dashed separator. Every non-blank line after it must be a data row:
  - five `|`-separated fields; a company name holding `|` is recovered from the four fixed fields around it and
    counted;
  - an ASCII CIK of 1 to 10 digits; a real date, `YYYY-MM-DD` in quarterly files and `YYYYMMDD` or `YYYY-MM-DD`
    in daily files; and an `edgar/data/<digits>/<accession>.txt` file name whose accession has the PDS format
    (10 digits, 2, 6).

  Any other line refuses the build (`INDEX_UNREADABLE`), with every rejected line counted by reason. The build also
  refuses (`INDEX_INCOMPLETE`) if a quarterly file from 2004 Q3 to `F`'s quarter, or a `master` file the pinned
  listings name from `FIRST` to `F`, is missing or unreadable.
- **Archive reading.** Every `CIK##########.json`, its history listing and exactly the pages it lists. Before any
  form filter, the producer checks:
  - that `filings.files` is a list of objects with a string `name`;
  - that every listed page is present;
  - that `accessionNumber`, `filingDate` and `form` are arrays;
  - that all of a page's arrays have one length.

  It refuses (`ARCHIVE_INCOMPLETE`) if any check fails. A block without an `items` array gives `missing` items.
  Unlisted page files are counted and not read.
- **Covered CIKs** are our CIKs. Every inventory row of a covered CIK gets a header. A header is fetched once per
  accession per build, from the first covered CIK that lists it. Rows of other CIKs are inventoried and counted, but
  not fetched.

### Headers

- **Fetch.** Every build fetches every header itself, at the shared SEC rate limit (four workers, at most 5 requests
  a second). Each body is stored under `headers/<accession>.sgml` with a sidecar holding its sha256, the URL and the
  UTC capture time. Three attempts per accession per run, 2 s then 8 s apart. A fetch that fails after three
  attempts is retried in the next run before publication; `no_header` is final only after two runs on different
  days.
- **No reuse across versions.** A header from an earlier artefact version is never used: a correction can change a
  header without changing anything the inventory shows. Within one build the cache serves only resumability, and an
  entry is used only when its sidecar's capture time is at most 7 days old. Entries without a sidecar, which
  includes all 9,041 measured so far, are fetched again.
- **Fields.** Each field has its own outcome, evaluated independently and recorded. A field whose inputs are not ok
  records `not_evaluated`, which counts as not ok.

  | field | ok when | otherwise |
  |---|---|---|
  | retrieval | the body was fetched | `no_header`, last error stored; every other field `not_evaluated` |
  | parse | exactly one `<SEC-HEADER>` block; `<ACCESSION-NUMBER>`, `<ACCEPTANCE-DATETIME>`, `<TYPE>`, `<FILING-DATE>` and `<DATE-OF-FILING-DATE-CHANGE>` each at most once anywhere in the body; every opening tag at the start of its line (after blanks) | `malformed`, with the cause |
  | accession | exactly one `<ACCESSION-NUMBER>`, equal to the inventory key | `no_accession`, `accession_mismatch` |
  | `<PAPER>` | absent | `paper` |
  | `<PRIVATE-TO-PUBLIC>` | absent | `private_to_public` |
  | `<CONFIRMING-COPY>` | absent | `confirming_copy` |
  | correction evidence | none of `<CORRECTION>`, `<DELETION>`, `<TIMESTAMP>` anywhere in the body | `correction_evidence`, with the tags found |
  | `<ACCEPTANCE-DATETIME>` | 14 digits forming a real date and time | `no_acceptance`, `acceptance_invalid` |
  | `<FILING-DATE>` | 8 digits forming a real date | `no_filing_date`, `filing_date_invalid` |
  | `<TYPE>` | in the 8-K family | `type_out_of_scope` |
  | `<ITEMS>` | non-empty, every code in the vocabulary, no code twice | `items_empty`, `items_invalid` |
  | `<FILER>` CIKs | include the row's CIK | `not_a_filer` |
  | date order | filing date on or after the acceptance date | `date_before_acceptance`; `not_evaluated` without both dates |
  | `<DATE-OF-FILING-DATE-CHANGE>` | 8 digits forming a real date, equal to the acceptance date | `no_correction_date`, `correction_invalid`, `corrected` (later), `correction_before_acceptance` (earlier); `not_evaluated` without a valid acceptance date |

  An absent correction date is not read as "no correction": without it the header does not say whether a PAC
  occurred. **Header grammar:** every tag is counted wherever it occurs in the retrieved body, inside the header
  block or not, and markers and correction evidence are detected anywhere; a value is read only from a tag that
  starts its line, so a layout the grammar does not accept fails as `malformed` instead of hiding a tag. The
  measurement script implements this table (`parse_header`, `field_outcomes`), and premise 6's class counts come
  from it.

### Row classes, labels in doubt, and intervals

Every inventory row of a covered CIK falls in exactly one class, decided by this table from the top; the first row
that matches wins, and a row matching none is `ok`:

| class | when any of these fields is not ok | `may_carry` | doubt interval |
|---|---|---|---|
| `items_unestablished` | retrieval, parse, accession, `<PAPER>`, `<PRIVATE-TO-PUBLIC>`, `<CONFIRMING-COPY>`, correction evidence, correction | both labels | `[start, capture]` |
| `timing_in_doubt` | acceptance, filing date, date order | the header's target codes when `<ITEMS>` is ok, else both | `[start, capture]` |
| `label_in_doubt` | `<TYPE>`, `<ITEMS>`, `<FILER>` | the header's target codes when `<ITEMS>` is ok, else both | `[available_session, capture]` |
| `ok` | none | none | none |

The table is total: every combination of outcomes reaches exactly one class. A correction field is ok only with a
valid acceptance date, so a row with an unusable acceptance (`0001104659-09-022130`) is `items_unestablished`, and a
`timing_in_doubt` row always has a valid acceptance date. A `timing_in_doubt` row whose `<TYPE>`, `<ITEMS>` or
`<FILER>` is also not ok keeps its class, but its label is in doubt too, so it never flags (§"Label state").

- **`available_session`**, for `ok` and `label_in_doubt` rows: the first session strictly after the later of the
  header filing date and `D`, or after the filing date alone when the row has no `D`. Both classes have ok
  acceptance, filing date, date order and correction fields, so the rule is defined for both. This is the declared
  availability rule (top of this spec):
  - **the filing date:** under Rule 13(a) it is the business day the filing counts as filed, and the guidance puts
    late submissions in that day's index;
  - **`D`:** a row first listed later than its filing date is not taken as available before its listing;
  - **the next session:** the guidance gives no hour, so the whole day is skipped. This is fixed by construction.
- **`ok` rows are observations** for each target code in their header items, available at `available_session`.
- **`start`:** the first session strictly after the acceptance date, when the acceptance field is ok **and** the
  parse and accession fields are ok, since dissemination follows acceptance. A body that is malformed or belongs to
  another accession supplies no date at all.
  - **`start` is `s_min` unless a trusted body excludes the paper path:** when retrieval, parse or accession is
    not ok (no body, or one that cannot be trusted, says whether the submission was electronic), or when the body
    carries `<PAPER>` or `<CONFIRMING-COPY>`. A paper filing can be public before, and in an earlier year than, its
    EDGAR record, and no pinned source dates it.
  - Otherwise, a trusted electronic body without a usable acceptance date: `start` is the first session strictly
    after 1 January of the accession number's year (the inventory key's middle two digits). The dissemination
    guidance says the accession number is "assigned automatically to an accepted submission by EDGAR", and its
    middle two digits "represent the year". This deliberately covers sessions that may precede the real
    acceptance. The one measured case (`0001104659-09-022130`) starts in January 2009.
- **`capture`:** the first session strictly after the New York civil date of the capture instant. For a fetched
  header the instant is its sidecar's UTC capture time; for a `no_header` row, the UTC time of its last fetch
  attempt. Both are converted with `America/New_York` before the date is taken, so a weekend or holiday maps to the
  next session.
- **Ordered ends:** an interval's upper end is the later of `capture` and its lower end, so an interval is never
  empty (a filing date assigned after capture gives a one-session interval at `available_session`).
- **Why every doubt runs to `capture`:** a non-`ok` row is one the construction cannot certify, so the producer
  asserts no time for it. A correction can change items at an undated time, a missing or unreadable header shows
  nothing, and a marked submission follows a release path the header does not date. The doubt therefore covers
  every session from the earliest the construction allows to the first session the producer itself observed the
  content. No bound depends on an index file's write time.

### Coverage

- **Coverage end `F`:** the last day with a daily file in the pinned listing whose `Last Data Received` equals its
  day. The producer refuses if `F` is after the archive's last filing date plus one day.
- **`s_max`:** the last session on or before `F`.
- **`s_min`:** the first session strictly after `FIRST`.
- **Coverage record**, one per CIK seen in any source. A covered CIK is **complete** when:
  - its archive file and pages pass the checks;
  - every one of its inventory rows has a final header outcome;
  - no row has a `retrieval` outcome other than ok or `no_header`;
  - and every `no_header` row is listed only in daily files, absent from both the archive and the quarterly index:
    an unexplained membership difference (premise 1).

  Other CIKs have a record with `covered = false`.
- **Days without a daily file** between `FIRST` and `F`, and days whose file was written more than 7 days after
  its day, are listed in the manifest and the census. A row has no `D` only when no pinned daily file lists it; a
  row filed on a day without a file can still take `D` from a later file.
- **Known limits.** None of these is turned into `unknown`; each is stated with its measured size, and every
  consumer registration restates them.
  - **Availability** is the declared rule, not a recorded publication time.
  - **Rows no pinned source holds** are absent, for example one removed before its day's file was rewritten. Their
    number is unmeasured: no independent historical population exists among the pinned sources. Premise 1's 501
    unexplained membership differences are rows the daily files *do* hold, so they are in the inventory and are
    not a measure of this limit. Making every day with a rewritten file `unknown` would blank, for example, all of
    2012, whose daily files were all rewritten on 2014-07-30.
  - **Hidden filers.** An accession indexed only under an uncovered CIK, whose header names a covered CIK as a
    filer, is never fetched, so that covered row is never discovered. That population is never inspected, so its
    size is unmeasured; the 9,041 inspected headers (0 naming a filer the archive lacks, premise 6) are all indexed
    under a covered CIK and say nothing about it. Full discovery would need the header of every accession of every
    CIK: 1,765,527 in archive A (premise 2).
  - **Same-day corrections.** A PAC on the acceptance day itself is indistinguishable from none.

The equity calendar is `app/services/market_calendar.py::us_market_status`. Every date whose status is not `closed`
(`open` and `half_day` alike) from 2003-01-02 to `F` plus 30 calendar days is a session, written to the artefact as
`sessions.json`, and `state()` reads only that file. The producer refuses (`CALENDAR_RANGE`) if a session it
computes falls outside it. No time zone conversion occurs.

### Output: a research artefact

Published under the research root with the step 1 reference-artefact protocol: exclusive `mkdir`, a clean checkout,
the manifest written last, after every file below exists (build order).

**The manifest names:**
- the archive sha256 and capture time;
- every quarterly and daily file's sha256 and `Last Data Received`; every daily listing's sha256, and each daily
  file's raw `last-modified` with its precision;
- the days without a daily file and the days whose file was written more than 7 days late;
- `FIRST`, `F`, `s_min`, `s_max`, and the `parent` version;
- the construction hash (the producer's import closure) and `sessions.json`'s sha256;
- every header's sha256 and capture time;
- the stage-A manifest sha256 and the A/B baseline's sha256 (gates 5 and 6);
- the census.

**Files:**
1. **`observations.jsonl.gz`:** one row per `ok` row with a target code: `cik`, `accession`, `form`,
   `filing_date`, `acceptance_et`, `items`, `labels`, `D`, `listing_lag`, `available_session`.
2. **`doubt.jsonl.gz`:** one row per non-`ok` row: class, every field outcome, every source's variant, the header
   fields present, the stored error, `may_carry`, the interval's ends (`start` or `available_session`, and
   `capture`).
3. **`coverage.jsonl.gz`:** one row per CIK: `covered`, `complete`, the failing condition if any, and inventory rows
   by year and class.
4. **`sessions.json`**, **`headers/`** with their sidecars, **`ab_baseline.jsonl.gz`** (gate 6), **`census.json`**.

No database table, migration or operator-visible surface changes.

### Label state

```
state(cik, label, session, lookback) -> ("flagged" | "unflagged" | "unknown", reason)
```

**Inputs are validated first**, or `state()` raises: `label` is `non_reliance` or `accountant_change`; `cik` is a
10-digit zero-padded string; `lookback` is an `int` (not a `bool`) from 1 to 1,260; `session` is a session in
`sessions.json`.

The **window** is the `lookback` sessions ending at `session`, inclusive. The answer, in order:
1. **`unknown` (coverage)**, tested before any window is indexed, if any of these holds:
   - `session` is after `s_max`;
   - `session`'s position in `sessions.json` minus `lookback − 1` is below zero, or the session there is before
     `s_min`;
   - the CIK's coverage record is missing, not covered, or not complete.
2. **`flagged`** if either holds:
   - an observation of the CIK with the label has `available_session` in the window;
   - a `timing_in_doubt` row of the CIK whose `may_carry` holds the label, and whose `<TYPE>`, `<ITEMS>` and
     `<FILER>` fields are ok, has its whole interval `[start, capture]` inside the window. Wherever in the interval
     the row became available, it did so inside the window.
3. **`unknown` (doubt)** if a non-`ok` row of the CIK whose `may_carry` holds the label has a doubt interval that
   meets the window:
   - for a `timing_in_doubt` row that could flag (step 2), without lying wholly inside it;
   - for every other non-`ok` row, at all, because its label is not known.
4. **`unflagged`** otherwise.

**Why the answers are sound under the declared construction.**
- A certain observation in the window flags the window, whatever else is in doubt (step 2).
- A doubt that can change the answer gives `unknown` (step 3). A row whose label or time is uncertain could, within
  its interval, put an event in the window or leave it out.
- So `flagged` and `unflagged` are each the answer under every possibility the construction leaves open.
- **Step 1 is a deliberate conservative refusal:** incomplete coverage gives `unknown` even when a certain
  observation lies in the window. A consumer reading a CIK the producer could not complete gets no answer at all,
  rather than positives with an unstated negative side. So `unknown` covers both disagreeing possibilities and
  refused coverage; the reason field says which.

**`unflagged` means** no label in the window under every such possibility. It rests on:
- the header of every row, fetched in this build and read against the PDS correction rule;
- the declared availability rule;
- coverage being complete.

The known limits under Coverage are what it does not cover.

### Corrections and earlier states

- **One snapshot per artefact.** A later source set is a new artefact version with its `parent` named. Earlier
  versions are never rewritten, so a consumer that registered a version keeps that version's answers.
- **A later version can change an earlier answer.** It can add a row the pinned sources lacked, see a correction
  that happened since, or reach a header that failed before. The missing-acceptance fallback also opens doubt
  before acceptance on purpose. Immutable versions keep each answer reproducible; they do not make answers
  unchangeable across versions.
- **Version reconciliation**, printed between a version and its parent:
  - inventory rows added and removed, with sources;
  - per-row changes in form, filing date, items, membership, `D`, `listing_lag`, every header field and capture
    time, every field outcome, class, `may_carry`, `available_session`, `start` and `capture`;
  - per-CIK coverage records;
  - `F`, `s_min`, `s_max`, the days without a daily file, and `sessions.json`;
  - `state()` changes over gate 6's grid.

  The grid is a fixed sample of states; the per-row diff is the complete change record.
- **Within one version, an earlier state is protected by:**
  - an `ok` row's items being those disseminated (PDS rule above);
  - a corrected, marked or unreadable row being in doubt over every session up to capture;
  - a row listed only in daily files being classed by its own header like any other row, or, when that header
    cannot be retrieved, being `items_unestablished` over `[start, capture]`. The census reports the outcome
    partition of these rows after the fetch.

## Census (printed by every run, stored in the manifest)

Per year, with `(cik, accession)` rows and distinct accessions counted apart:
- **The year** of a covered row is the year of its header filing date when the parse, accession and filing-date
  fields are ok, else the year of its earliest date filed in any source under its own key, else (a header-only row
  with an unusable header filing date) the accession's earliest date filed under any CIK in any source, else the
  `unknown_year` bucket. An uncovered row uses the earliest date filed in any source. Coverage records use the same
  allocation. Inventory-by-source tables also give each source's own year, so a row whose sources
  disagree is visible in both.
- **all CIKs:** inventory rows by form and by source combination (daily, quarterly, archive, header-only), and rows
  dated before `FIRST` by source;
- **covered CIKs only**, because only their rows have headers: the same inventory counts, rows per class and per
  field outcome, observations per label and form, `may_carry` rows per label and class with their doubt intervals'
  lengths in sessions, and `listing_lag` of observations (same day, 1–7 days, more, none).

Plus:
- `F`, `s_min` and `s_max`;
- the index parse counts by reason, per file;
- the days without a daily file and the days whose file was written more than 7 days late;
- the unexplained membership differences (rows listed only in daily files), with how many of their accessions the
  quarterly files list under another form or CIK;
- the covered CIKs that are not complete, with their failing condition;
- rows dated after `F`;
- the maximum acceptance-to-filing-date gap.

**The dry run** (build step 4) fetches no header. It prints only the header-free part: inventory rows by form and
source, `D` coverage, the index parse counts, days without a daily file, the archive checks, the coverage records'
archive part and `F`. Every header-dependent field is printed as `pending`.

## Registration

**What has been run so far, and why it is outside the ledger.** The programme counts every configuration tried
(§4). The measurement behind this spec read no price, return or outcome and evaluated no label configuration
against anything: it counted source structure (membership, item validity, header fields) to fix the construction.
No producer configuration has run. Every checkpoint round's variant of the construction exists only as spec text
(this log), and none produced labels for a consumer. The registration below is therefore prospective: it counts
the one production configuration before its first run.

The producer and its fixtures are written first and read no data (build step 1). Then, before the production
build's first data read, the dry run included, the frozen configuration is registered as a non-claiming `DeclaredTrial` in
`app/services/trial_register.py` (`3624-slice2-labels-v1`, one configuration, `searches=1`). Its `evidence` names
this spec's sha256, the construction hash, the archive sha256, and the quarterly and daily files' sha256s. Every
configuration run is counted. A later change of any of these is a new configuration in the programme ledger: label
definition, form scope, covered population, class rule, `may_carry` rule, interval rule, availability rule or
`unknown` policy.

## Gates (decision recorded on #3624 before any downstream use)

**What acceptance means.** An accepted artefact may be *read by a registered consumer*. It does not say the labels
cover enough of any universe. No consumer may read it without a registration that:
- names a coverage requirement for its own registered universe, decision dates and lookbacks;
- passes it with `state()` evaluated on exactly that universe, those dates and those lookbacks, recorded on #3624.
  Gate 5's census may stand in only for a consumer whose universe, dates and lookback are gate 5's. That coverage
  evaluation is the one read allowed before admission: it calls `state()` and records the three counts and their
  reasons only, with no feature, event or outcome derived;
- names the artefact version and states the known limits.

**Progression.** Accepted when all eight hold:
1. **Refusals:** `ARCHIVE_INCOMPLETE`, `INDEX_UNREADABLE`, `INDEX_INCOMPLETE`, `CALENDAR_RANGE` and the
   coverage-end check did not fire.
2. **Census reproduction:** for covered CIKs, on version 1's pinned inputs, the census reproduces premise 2's
   archive identity and validity counts. A later version instead reconciles its own counts internally and explains
   every difference from its parent through the version reconciliation. Its observations are reconciled with the archive's target-code rows, row by row: each archive positive
   is an observation or a non-`ok` row, and each observation is an archive positive or has a header that disagrees
   with the archive, with its cause. Any unexplained row fails.
3. **Headers:** every covered CIK is complete. Every `no_header` row is an unexplained membership difference,
   listed only in daily files and absent from the archive and the quarterly index.
4. **Doubt ceiling:** non-`ok` rows of every class are together at most 2% of covered rows, reported in rows and
   accessions and per class. The threshold is fixed by construction. References on the selected header set
   under the production predicates (premise 6), our CIKs' rows: 93 of 7,042 archive-candidate rows, and 2 of the
   2,001 rows of the 2,000 sampled other accessions. Both are selected sets, not this gate's population.
5. **Survivorship census (the #3609 development universe only).** Over every `(series, formation month)` row of the
   stage-A artefact (`STAGE_A_ARTEFACT`, manifest sha256 `e5087210…` = `STAGE_A_MANIFEST_SHA256`, read through
   `read_verified_artefact`):
   - rows by `link_reason`, plus a separate category for rows with no link fields. Stage A emits a `not_priced` row
     before it attaches `link_reason`, `link_basis` and `cik` (`build_3609_factor_panel.py`, the `NOT_PRICED` yield
     before the `link_as_of` call), and those rows are counted by their `exclusion`. The categories must sum to the
     stage-A row total;
   - for `LINKED` rows (the only ones with a `cik`), rows whose CIK is not covered or not complete;
   - for each label, `flagged`, `unflagged` and `unknown` at the formation month's last session with `lookback`
     252, with `unknown` split into coverage and doubt.

   Distinct CIKs are counted beside rows. Series-to-CIK linking is #3609's rule, not re-decided here. Stage B is not
   read. **Pass:** it ran, the categories reconcile, and it is posted. It is the input to each consumer's coverage
   requirement, not a threshold here.
6. **A/B against `filing_events.items`**, the layer premise 9's direct readers use. The audit does not show that it
   is every relevant reader, and the gate claims only this comparison.
   - **Baseline constructor**, frozen into `ab_baseline.jsonl.gz`. Source rows are `filing_events` with
     `provider = 'sec'` and `filing_type` in the 8-K family, joined to our CIKs through `external_identifiers` (the
     measurement's `_EVENTS` query), with a filing date from `FIRST` to `F`. One row per `(cik, accession)` holds:
     - `forms` and `filing_dates`, the distinct values;
     - `item_sets`, each distinct non-NULL set, sorted;
     - `null_rows` and `rows`;
     - and, derived from those: `items`, the union of `item_sets`; `filing_date`, the earliest of `filing_dates`;
       `date_conflict`, more than one filing date; `set_conflict`, more than one item set; `labels`, the target
       codes in `items`; `available_session`, the first session strictly after `filing_date`.
   - **Baseline coverage:** the baseline uses the artefact's `sessions.json`, `F`, `s_min` and `s_max`, and treats
     every one of our CIKs as covered and complete, with no non-`ok` rows.
   - **Comparison:** `state()` is evaluated on both at every month-end session from `s_min` to `s_max`, lookbacks
     21 and 252, for our CIKs.
   - **Pass:** for each differing state, the producer lists every row of the CIK whose treatment differs between
     the two inside the window. Replaying the window with just those rows changed must reproduce the artefact's
     state from the baseline's. Each listed row must also have one of these causes:
     - an accession absent from `filing_events`;
     - all-NULL `filing_events` items;
     - a `date_conflict` or `set_conflict` row;
     - a non-`ok` artefact row;
     - a differing filing date, or a `D` later than the filing date;
     - an `ok` header whose items differ from the baseline's single item set, the header being the authority
       (premise 6's five archive lists cut at 13 codes are this case);
     - a baseline row whose `(cik, accession)` no pinned producer source holds (checked against every daily file,
       every quarterly file and the archive), recorded as an unexplained baseline-only row, not as a removal. The
       replay drops it from the baseline; conversely, an artefact row the baseline lacks is replayed by adding it;
     - a CIK the artefact marks incomplete.

     Any other difference fails.
7. **Document check (a sample, not population evidence).** The frame per label is the sorted list of
   `(accession, cik)` rows of covered CIKs, `FIRST` to `F`; only `ok` rows can be drawn, so marked submissions are
   excluded. `random.Random(3624).sample` draws without replacement:
   - 10 observations that are originals and 10 that are amendments;
   - 10 `ok` rows without the label that are originals and 10 that are amendments.

   A stratum smaller than 10 is taken whole. Each filing's primary document is read on EDGAR, with three attempts
   on each of two days.
   - **Candidate detection.** General Instruction D requires "the number and caption of the applicable item". The
     primary document is rendered to text lines: HTML with each `p`, `div`, `br`, `tr`, `li` and `h1`–`h6` element
     ending a line and whitespace collapsed; a text document as it is. Its **item region** runs from the first line
     that begins `Item <d>.<dd>` to the signature block (the first line that begins `SIGNATURE`). A **candidate
     heading** for a target item is a line in the item region that begins with its number (`Item 4.01` or
     `Item 4.02`, any case and spacing) and goes on with the first three words of its caption in the pinned PDF
     ("Changes in Registrant's", "Non-Reliance on Previously").
   - **Candidate classes:** `agree` (an observation with a candidate heading, or a non-label document with none),
     `candidate_disagree` (an observation without one, or a non-label document with one), and `ambiguous` (the
     item region cannot be delimited, or the number appears in the item region only outside a candidate heading).
   - **Review of every draw.** Candidate classes are provisional, `agree` included. Every sampled document is read
     in full by the same procedure: which items does the report itself declare as its own sections, and is each
     matched or missing heading such a section, or a quotation, a reference to another filing or a layout artefact?
     The reading is posted with the document passage and its outcome, `agree` or `disagree`, beside the candidate
     class. Ambiguity is recorded apart from disagreement.
   - **Pass:** no `disagree` after review.
   - A document still unreadable after both days is a gate-7 failure, never replaced by another draw.
   - Every draw and result is posted.
8. **Going-concern search:** completed with its result recorded, or recorded as `incomplete` with the failed inputs
   listed (below).

**Scope of each gate.** Gates 2, 3, 4 and 6 cover the full population they name; gate 7 is a sample; gate 5 is the
stage-A development universe.

**Remediation.**
- **Gates 1–4, 6 and 8:** a failure is fixed in the producer and re-run under a new construction hash, as a new
  configuration.
- **Gate 5:** a failure from malformed or unverifiable stage-A input stops the gate until #3609 supplies a verified
  artefact. A failure from the producer's own counting is fixed like gates 1–4.
- **Gate 7:** a failure is reported per accession, with its cause: metadata disagreeing with the document (a
  `disagree` draw, including an ambiguous one read as such), or a document unreadable after both days. The slice records the error rate within the draw and stops before
  acceptance. A corrected treatment is a new registration.

**No-go.** The slice ends without an accepted artefact if gate 7 fails for either cause, or if gate 1 cannot pass.

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
   - a synthetic zip with recent and history pages, a missing page, a misaligned page, a page missing its `form`
     array, a CIK file without `filings.files`, a block without `items`, a repeat, a co-registrant pair, every item
     state and two different invalid strings;
   - synthetic daily and quarterly files, with a row listed late, a day without a daily file, a rewritten file, a
     row in a daily file only (an unexplained membership difference), a row dated before `FIRST` listed after it,
     `Last Data Received` lines, a company name holding `|`, an unparseable data row (refused), a file without its
     separator (refused) and a missing quarter (refused);
   - headers for every field outcome in the table, including two `<SEC-HEADER>` blocks, a repeated tag, a
     mismatched and a missing `<ACCESSION-NUMBER>`, an impossible acceptance datetime, an empty acceptance tag,
     each of the three markers, absent, later and earlier correction dates, a non-filer CIK and a header-only
     filer;
   - one row of each class, with its `may_carry` and interval, and one of every outcome combination the decision
     table's precedence orders;
   - header cache entries with and without a sidecar, and one older than 7 days (re-fetched);
   - `state()` tests for every branch, bound and input refusal. They include lookback underflow at the start of
     `sessions.json`, a half-day session, a filing dated before a holiday and a weekend, and a `timing_in_doubt` row
     that flags when its interval lies inside the window and is `unknown` when the window cuts it. They also cover
     an `items_unestablished` row that leaves an `ok` observation in the same window `flagged`.
2. Freeze the construction hash and register (above).
3. The going-concern search.
4. A dry run: inventory, `D`, the index and archive checks and the header-free census, no header fetch.
5. The header fetch for every covered row: all 710,665 our-CIK accessions in the archive, plus any rows only the
   index files hold. The 9,041 cached bodies have no sidecar and are fetched again. At most 5 requests a second
   that is about 39.5 hours, run in the background, resumable within the build, with a second pass for failures on
   a later day.
6. Build the artefact's files, the A/B baseline and the complete census, then write the manifest last (publish).
7. Gates 1–8, with the decision posted on #3624.

The build rung is behavioural with data semantics: fixtures, Codex checkpoint 2, and gates 1–8 as the evidence.

## Not done, and why

- **No change to `filing_events`** or its ingest horizon. That layer serves the operator; the producer reads the SEC
  sources.
- **No fix to the acceptance-time defect.** That is #3714; the producer reads headers only.
- **No EDGAR Feed archive.** It holds each day's disseminated submissions, and for PACs their `<TIMESTAMP>`
  dissemination time. It might narrow the historical record of what was disseminated when; whether it would date
  original filings is not established here. Reading it for 2004 to 2026 means ingesting every day's full
  dissemination archive, out of proportion to two labels. Availability is the declared rule instead (top of this
  spec).
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
- **Round 4** (`var/research/3624/ckpt1_slice2_r4.txt`): 9 open (3, 14, 40, 41, 56, 62, 65, 72, 79), 17 new
  (84–100). The measurement was re-run at `a24131e1`.
  - **Uncertainty and point-in-time** (3, 40, 62, 84, 85, 86, 87, 88):
    - Uncertain rows no longer produce `unknown`. `state()` takes `resolution`. `low` ignores uncertain rows;
      `high` places each at its earliest possible session, for every label in `may_carry`.
    - Consumers must report both, and fail on a differing decision. So a decision resting on later-captured
      information fails rather than passing silently (40).
    - A corrected observation is no longer swallowed by its own interval (86).
    - Uncertain rows never become observations, so no timing resolution bypasses an identity or item failure (85).
    - No upper bound is computed. The correction date and the anomalous-date bound are gone, because neither is a
      dissemination time (87, 88).
    - `may_carry` is both labels unless an uncorrected, valid header states the items (84).
    - `clear` is renamed `unflagged`, with its retrospective meaning and measured bound stated. That is the
      unverified-negative state 62 asks for. Historically valid negatives remain a stated limit without the Feed (3).
  - **Calendars and boundaries** (41, 65, 89, 91):
    - The EDGAR dissemination-day calendar is removed: `earliest_session` needs only the acceptance date, and the
      index is shown to be organised by date filed (89).
    - The `FIRST` boundary now rests on Release 33-8400's domain: 0 of 364,314 pre-`FIRST` archive rows carry a
      target code. `s_min` = `FIRST`, and the 30-day buffer and its refusal are gone (41).
    - The inventory reads every pinned row with no upper date filter. The "indexed by `F`" claim is replaced by a
      stated limit that the next version's reconciliation reports (91).
    - Lookback underflow is tested before indexing, and `CALENDAR_RANGE` refuses a session outside
      `sessions.json` (65).
  - **Field contract** (14, 56, 93): real-calendar validation; repeated scalar tags are `malformed`; the correction
    field has four non-ok outcomes. An absent correction date is now timing-uncertain (17 of our candidates), giving
    87 timing-uncertain rows. The PAC field is reported in four categories.
  - **Measurement** (92, 94, 95, 97, 98):
    - in-window denominators, with post-cutoff accessions counted apart;
    - timing-uncertain reported in both units;
    - row multiplicities emitted;
    - required page arrays validated;
    - the consumer audit run by the script with its revision. It found the red-flag reader, which the earlier text
      had missed.
  - **Gates and text** (72, 79, 90, 96, 99, 100):
    - the near-filing claim is limited to current values;
    - the full A/B baseline schema, with date and set conflicts;
    - header reuse versus refresh, with lineage;
    - stage-A rows without link fields counted and reconciled;
    - unreadable documents get a retry budget and a no-go;
    - the dry-run census is header-free with `pending` fields.
- **Round 5** (`var/research/3624/ckpt1_slice2_r5.txt`): 5 open (3, 40, 41, 62, 91), 15 new (101–115). The
  measurement was extended and re-run at `77231ed1`. This round rebuilt the evidence base rather than patching
  the resolutions.
  - **Contemporaneous timing** (40, 101, 103):
    - EDGAR's daily index files are now read: 5,516 files, each with its write time. A row listed in a file was
      public by that time, so a timing-uncertain row's interval closes at a supported SEC record, not an invented
      delay. The 7-day fallback is gone.
    - The two resolutions are replaced by exact interval semantics. A window is `flagged` or `unflagged` only when
      every possibility agrees, and `unknown` otherwise (101).
    - `ok` rows are available after the later of the filing date and the first listing day.
  - **Negatives and earlier states** (3, 62, 91, 102, 105):
    - A header is fetched for every row of a covered CIK, about 39 hours at 5 requests a second. So a negative
      rests on that row's header under the PDS correction rule, not on a 2,000-row sample, and the sample's 0.15%
      bound is no longer used.
    - Corrected, uncorrectable and missing headers are `unknown` up to capture.
    - Missing item metadata is `unknown`, as the programme requires (102).
    - The 502 rows disseminated and later removed are in the inventory through the daily files (3).
    - The boundary rests on per-source membership, with stated limits (91).
  - **`FIRST`** (41, 104): no domain argument. Every row from `FIRST` on is read and decided by its header.
    `s_min` moves to the first session after `FIRST`. The pre-`FIRST` token count is now taken token by token.
  - **Measurement** (106–109, 111):
    - header block counts and a raw tag census, with 0 `<TIMESTAMP>` and 0 `<PAPER>`;
    - a provenance block in every output;
    - the severity query and the `red_flag_score` reader trace;
    - history-listing validation.
  - **Text** (110, 112–115):
    - paper submissions are never `ok` and are excluded from the document frame;
    - the baseline and census are built before the manifest;
    - gate-5 remediation;
    - half-day sessions included;
    - A/B differences may have several causes and must replay.
- **Round 6** (`var/research/3624/ckpt1_slice2_r6.txt`): 6 open (3, 40, 41, 91, 109, 110), 19 new (116–134), 13
  resolved. The measurement was extended and re-run at `44ac5fdc`.
  - **Framing** (3, 40, 91, 116, 117): the findings open since round 1 ask for proof of each filing's historical
    public time, which only the EDGAR Feed could give (declined, §"Not done"). Availability is now the declared
    rule the programme's acceptance-keyed standard and §4 hierarchy allow: the first session after the later of the
    Rule 13 filing date and `D`. The listing's write time `W` is a diagnostic only. Every non-`ok` doubt runs to
    `capture`, so no bound rests on `W`. Residual removal risk is a known limit with its measured rate, not
    `unknown`. The checkpoint prompt now judges point-in-time safety against this standard.
  - **Classes** (110, 119, 120, 121): a total decision table; an unusable acceptance makes the correction field
    `not_evaluated` and the row `items_unestablished`; `label_in_doubt` uses the `ok` availability rule;
    `<PAPER>`, `<PRIVATE-TO-PUBLIC>` and `<CONFIRMING-COPY>` are field outcomes that make a row
    `items_unestablished`. Class counts now come from the production predicates in the script.
  - **Headers** (118, 123, 124, 126): no reuse across versions; the cache serves one build's resumability, with a
    capture-time sidecar per header and a 7-day limit, so the 9,041 cached bodies are fetched again;
    `<ACCESSION-NUMBER>` must equal the key; each daily listing is bound to its body by sha256, and the third
    output records every input hash.
  - **Membership** (122): hidden filers under uncovered CIKs are a known limit, measured on the inspected set.
  - **Index integrity** (133): one parse contract for daily and quarterly files, with rejected lines counted and
    two new refusals, `INDEX_UNREADABLE` and `INDEX_INCOMPLETE`.
  - **Text and measurement** (41, 109, 125, 127–132, 134): pre-`FIRST` is a declared estimand restriction; the
    consumer-audit claim is limited to direct matches; the fallback is said to precede acceptance; 72,348 typed
    accessions (the dev DB grew since 72,343); both daily denominators; "unexplained membership differences";
    header-dependent census columns for covered CIKs only; gate 7's heading predicate on General Instruction D,
    with ambiguity recorded apart; consumer coverage on the consumer's own universe and grid; tag claims limited
    to the inspected headers.
- **Round 7** (`var/research/3624/ckpt1_slice2_r7.txt`): 10 open (3, 40, 91, 109, 110, 116, 118, 122, 131, 133),
  15 new (135–149), 15 resolved. The measurement was re-run at `22876703`.
  - **Estimand** (3, 40, 91, 116, 122): round 7 offered two fixes, propagate every unestablished period into
    `unknown`, or change the estimand and guarantee at programme level. This PR takes the second: the programme's §5
    now defines "acceptance-keyed" (never available on or before acceptance), the declared availability rule, and a
    negative's guarantee with its known limits. The spec's opening section restates it and shows
    `available_session` always follows acceptance.
  - **Bounds** (110, 136, 138): a malformed or wrong-accession body supplies no date (fallback start); paper and
    confirming copies start at `s_min`; capture maps a UTC instant through New York civil date to the next session
    for fetched and failed headers alike; an interval's upper end is never before its lower end.
  - **Correction evidence** (135, 149): `<TIMESTAMP>` is described as PDS defines it, PAC-only; `<CORRECTION>`,
    `<DELETION>` and `<TIMESTAMP>` anywhere in a body make the row `items_unestablished` (0 of 9,041).
  - **Limits as measured** (137, 122): all-source omissions and hidden filers are stated as unmeasured; the 501
    are a membership-disagreement count only, 490 distinct accessions (142).
  - **Index contract** (133): accession-format file names and per-index date formats, re-measured: 0 rejections.
  - **Gates** (109, 131, 139, 140, 141, 148): gate 6 claims only the `filing_events.items` comparison; gate 7's
    predicate is candidate detection with a defined rendering and a full-read review; the doubt ceiling counts every
    non-`ok` class; a header-items cause replaces gate 6's vague catch-all; gate 2's exact reproduction is for
    version 1 only; the pre-admission coverage read is authorised.
  - **Text** (118, 143–147): the listing pairing is described as unchecked; daily-only rows are classed by their
    own header outcome; "candidate" is defined; census year allocation is fixed; `D` absence is stated exactly;
    coverage-first `unknown` is called a deliberate refusal.
- **Round 8** (`var/research/3624/ckpt1_slice2_r8.txt`): 7 open (40, 109, 110, 116, 131, 145, 149), 7 new
  (150–156), 18 resolved, among them the estimand findings 3, 91 and 122 under the programme amendment. The
  measurement was re-run at `c7ea8ba9`.
  - **Bounds** (40, 110): `start` is `s_min` unless a trusted body excludes the paper path: no body, an untrusted
    body (parse or accession not ok), `<PAPER>` or `<CONFIRMING-COPY>`. The accession-year fallback is left for a
    trusted electronic body without a usable acceptance date.
  - **Header grammar** (149, 150): tags are counted anywhere in the body, markers and correction evidence detected
    anywhere, and an opening tag not at the start of its line makes the parse `malformed`. Re-measured on the 9,041:
    0 misplaced, 0 repeated, the same marker counts.
  - **Text** (109, 116, 145, 151–154): premise 9 is literal search hits, with the typed path named and no data-flow
    claim; the whole availability formula is the declared construction, informed by Rule 13(a) and the guidance;
    year allocation for header-only rows and an `unknown_year` bucket; the typed count is bound to the output; the
    six gzip-only quarters are listed; zero-filled years (0 to 70); uncovered CIKs' records stated once.
  - **Gates** (131, 155): every sampled document is read in full, `agree` included; a baseline-only row no
    producer source holds is a gate-6 cause, with replay defined for added and dropped rows.
  - **Registration** (156): the measurement read no outcome and ran no producer configuration; the registration is
    prospective for the one production configuration.
