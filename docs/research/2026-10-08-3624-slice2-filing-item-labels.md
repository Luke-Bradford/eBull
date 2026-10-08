# #3624 slice 2 build spec: structured 8-K item labels (2026-10-08)

Programme: `docs/research/2026-10-06-3624-llm-research-component.md` §5 "Slice 2", which lists what this spec must
contain. This is a **label producer, not a strategy**. Any factor or filter that uses these labels supplies its own
hypothesis, event-to-feature rule, persistence, expiry, horizon, track, power assessment and gate before it reads
outcomes. The producer reads no prices and no returns.

**Design rule: when in doubt, `unknown`.** No rule below links, infers or estimates anything a structured field
does not state. Wherever the source leaves a question open, the answer is `unknown`, never `clear`.

## Measurement and pinned inputs

Every count below comes from one command; its output is committed as
`docs/research/3624-slice2-8k-items-measurement.json`:

    PYTHONPATH=. uv run python -m scripts.measure_3624_slice2_8k_items \
        --archive "$RA/d6c42d554a703832a3a477a0b04b0f8cc96ce868180b1bb58e9be99ad717de8b/submissions.zip" \
        --previous "$RA/928d67221c6e6183bc343e7234c1391448c15cd1dd644d36b425db2f99ba4350/submissions.zip" \
        --header-sample 300 --out docs/research/3624-slice2-8k-items-measurement.json

`RA` is `~/Library/Application Support/eBull/research-artifacts/sha256`.

**Pinned inputs:**
- **Archive A:** the current `submissions.zip`, sha256 `d6c42d55…`, captured 2026-10-07T08:05:08Z. Its last 8-K
  filing date is 2026-10-06.
- **Archive B:** the previous snapshot, sha256 `928d6722…`, captured 2026-08-24, with last 8-K filing 2026-08-21.

Both copies sit content-addressed under `RA` with an `artifact.json`.

**Scope of the counts:**
- "Our CIKs" is the 5,310 CIKs in `external_identifiers` (`provider='sec'`, `identifier_type='cik'`); the list's
  sha256 is `d1c01c68…`, printed by the run.
- The measured window is forms `8-K` and `8-K/A` filed 2016-06-03 (the earliest `filing_events` 8-K) through the
  archive's last date.
- The database-derived counts (`filing_events`, `eight_k_*`, `sec_filing_manifest`) are as of 2026-10-08 and are
  context for the design. No gate reproduces them.

## Labels

A label is an **observation**: one accession, original or amendment, whose declared items include the code.

| label | item | source rule (Form 8-K, SEC 873 (02-25)) |
|---|---|---|
| `non_reliance` | `4.02` | Item 4.02 (a) and (b): a conclusion or an accountant's notice that previously issued statements, or a related audit report or interim review, should no longer be relied on |
| `accountant_change` | `4.01` | Item 4.01 (a) and (b): the former accountant resigns, declines re-appointment or is dismissed, or a new accountant is engaged (Reg S-K Item 304(a)) |

Each label is named for what the item requires. Not every restatement is filed under Item 4.02, and an accountant
change is not adverse by itself. Item 2.02 stays out: it is slice 3's event-timing marker.

**Why every accession counts, amendments included.** An amendment can carry a target item for several reasons:
- an accountant's letter that Item 4.02(c)(3) or Item 304(a)(3) requires by amendment;
- a correction of the original;
- or something else.

The submissions data carries no amendment link: `amends_accession` is NULL on all 9,049 8-K/A manifest rows, and
`recent` has no key naming an amendment, across all 5,310 CIK files. Under the design rule the producer therefore
neither links nor discards. Each accession with the item is an observation with its form recorded. Collapsing
observations into events or episodes is a consumer's declared choice, in its own registration. The same holds for
Item 4.01: the form's Instruction makes a resignation or dismissal "a reportable event separate from the engagement
of a new independent accountant", so one change can produce two originals.

## Source rules

- **Form 8-K**, SEC 873 (02-25). The PDF (sha256 `730ab1de5550870134639fff1ccad3385186c3a08eafd8bd387db44b8bac5d70`)
  was fetched 2026-10-08 from `https://www.sec.gov/files/form8-k.pdf`. The rules used:
  - Items 4.01 and 4.02, as above;
  - the Item 4.01 Instruction;
  - Item 4.02(c)(3): an amendment filing the accountant's letter "no later than two business days after the
    registrant's receipt of the letter".
- **Reg S-K Item 304(a)(3)** (17 CFR 229.304): the former accountant's letter goes in as an exhibit. If it is
  unavailable at filing, it follows by amendment within ten business days, with further provisions for an interim
  letter and receipt. These rules are cited only to show that amendments carry target items for reasons other than
  new events. Nothing is computed from their deadlines.
- **Exchange Act Rule 12b-15:** amendments are filed under cover of the form amended (`8-K/A`).
- **Item metadata:** the submissions JSON `items` string, aligned by index with `accessionNumber`
  (`.claude/skills/data-sources/sec-edgar.md` §"Submissions"). These are the item codes the filer declared on the
  EDGAR submission. This is the structured field the engineering rules require checking before any text rule.
- **Acceptance time:** the filing's EDGAR header `<ACCEPTANCE-DATETIME>`, in Eastern wall-clock time. The JSON's
  `acceptanceDateTime` is not used (premise 4; #3714).
- **Public availability:** SEC's EDGAR guidance (`https://www.sec.gov/search-filings/edgar-search-assistance/accessing-edgar-data`)
  says that some submissions accepted after 5:30 p.m. Eastern are disseminated the next business day. The producer
  therefore never treats acceptance as availability (below).

## Premises (measured)

1. **`filing_events` is incomplete before 2024; the missing accessions sit in SEC's history pages.**
   - **Archive-only accessions for our CIKs, by filing year, against the archive's 8-K + 8-K/A count:**

     | year | archive-only | archive total |
     |---|---|---|
     | 2016 | 4,042 | 16,312 |
     | 2017 | 6,158 | 32,227 |
     | 2018 | 4,158 | 33,437 |
     | 2019 | 2,727 | 34,777 |
     | 2020 | 1,679 | 40,075 |
     | 2021 | 868 | 42,361 |
     | 2022 | 553 | 41,643 |
     | 2023 | 280 | 44,124 |
     | 2024 | 153 | 46,290 |
     | 2025 | 17 | 50,084 |
     | 2026 | 6 | 39,593 |

     The per-form split is in the JSON's `by_year`. Of the 20,641 archive-only accessions, 20,479 sit in the CIKs'
     `-submissions-NNN.json` history pages and 162 in `recent`.
   - **The other direction:** one `filing_events` accession is absent from the archive under our CIKs
     (`0001193125-26-399416`, listed). It is unexplained; the producer reads the archive only, so it is not used.
   - **Cause not established.** The concentration in history pages is consistent with the 8-K ingest horizon (skill
     §11.2, "last 2 years"), but no ingest audit was run. Nothing here changes `filing_events`.
2. **The archive's item metadata is complete and well formed for our CIKs:**
   - 0 of 420,986 rows (411,311 `8-K`, 9,675 `8-K/A`) have missing, empty or invalid items (pattern
     `\d{1,2}\.\d{2}(,\d{1,2}\.\d{2})*`);
   - 0 of the 2,587 listed history pages are absent from the zip;
   - 0 pages have misaligned arrays.
   - **Co-registrants:** 63 accessions appear under a second CIK, 58 with identical filing-level fields and 5 with a
     conflict.
   - **Agreement with `filing_events`:** on the 399,503 shared accessions with non-NULL items, the item sets are
     equal. The raw NULL count is 819 of 408,559 rows. Shared accessions with a NULL contributor: 758 all-NULL and
     21 mixed. The archive has items for every one.
3. **The typed body parser is not an independent completeness check.**
   - **Coverage:** `eight_k_items` and the archive share 72,241 accessions. The parser stored no item rows for
     52,353 of them.
   - **Item 4.01:** archive positives 541, parser positives 109, all 109 in the archive.
   - **Item 4.02:** archive positives 94, parser positives 25, all 25 in the archive.
   - The parser never holds a target code the metadata lacks (0 for each label). But its misses are mostly
     accessions where it stored nothing (404 of 432 for 4.01; 66 of 69 for 4.02), so agreement says little.
   - The independent check is the filing document itself (gate 4).
4. **The JSON acceptance time is unreliable and drifts.**
   - **Header check:** a seeded random sample (seed 3624) of 300 archive accessions was checked against EDGAR
     headers; every row is kept in the JSON. The JSON equals the header in 190 and is late by the ET offset in 110
     (+4h: 72; +5h: 38).
   - **Drift:** between archives B and A, `acceptanceDateTime` changed on 176,886 of 416,480 shared accessions,
     mostly by +4h or +5h. Over the same accessions, CIK, form, filing date and items changed on 0.
   - This is a sample for the error rate and a full count for the drift. Detail is on #3714.

## Construction

### Inputs

- **The archive.** One pinned `submissions.zip`, named by its sha256. Every `CIK##########.json` and every
  `-submissions-NNN.json` page in the zip is read, for every CIK.
- **Rows kept.** Forms `8-K` and `8-K/A` from 2016-06-03. Specialised submission types (`8-K12B`, `8-K12G3`,
  `8-K15D5` and their amendments) are out of scope. An accession of those types never makes a state `clear`, and a
  CIK with one in a lookback is `unknown` there (below).
- **Page refusals.** The producer refuses (`ARCHIVE_INCOMPLETE`) if any `files[]` page a CIK's main file lists is
  missing from the zip, or if any page's arrays differ in length.
- **Headers.** For every `(cik, accession)` row whose items include 4.01 or 4.02, the EDGAR `-index-headers.html` is
  fetched once at the shared SEC rate limit. Its bytes are stored in the artefact with their sha256. A later artefact
  version **reuses** an earlier version's stored header for any accession both hold, never re-fetching it. A header
  is never corrected in place.

### Identity

- **Rows are `(cik, accession)`.** A co-registrant filing is one row per CIK.
- **Filing-level fields** are `form`, `filing_date` and `items`. They must agree across every appearance of an
  accession. If they do not (5 accessions in archive A for our CIKs), each `(cik, accession)` row of that accession
  is kept with status `conflict`.
- **The same `(cik, accession)`** appearing twice with identical fields is kept once; with different fields, it is
  `conflict`.

### Item validity

`items` is `valid` only when it is a string matching the pattern in premise 2. Otherwise it is `missing` (absent or
null), `empty` (`""`) or `invalid`. Only `valid` items can show that a code is absent.

### Availability: a session, never an instant

- **`acceptance_et`** is the header's `<ACCEPTANCE-DATETIME>`, read as `America/New_York`.
- **`available_session`** is the first regular US equity session (the `us_market_status` calendar) that opens on a
  calendar day after `acceptance_et`'s date.
  - This lies after any next-business-day dissemination, because that happens no later than the start of the next
    business day.
  - A filing accepted at 07:00 ET is thus available at the next day's open, not that day's. The day lost is a
    declared conservative choice.
- **A missing or failed header** (after retries) gives `available_session = null`, with status `no_acceptance`. The
  JSON timestamp and `filingDate` are never used in its place.

### Output: a research artefact

The producer publishes under the research root, using the step 1 reference-artefact protocol: exclusive `mkdir`, a
clean checkout, the manifest written last.

**The manifest names:**
- the archive sha256 and its capture time;
- the window;
- the construction hash (the producer's import closure);
- the Python, `zoneinfo` and market-calendar source hashes;
- every header file's sha256;
- the census.

**Three files:**

1. **`observations.jsonl.gz`**, one row per `(cik, accession)` whose valid items include 4.01 or 4.02:
   `cik`, `accession`, `form`, `filing_date`, `items`, `labels`, `acceptance_et`, `available_session`, `status`
   (`ok`, `no_acceptance` or `conflict`).
2. **`coverage.jsonl.gz`**, one row per CIK in the archive:
   - `pages_complete`;
   - for each kept `8-K` or `8-K/A` accession: `filing_date`, item validity (`valid`, `missing`, `empty` or
     `invalid`) and `status`;
   - for each out-of-scope 8-K-type accession: its `filing_date`.

   This file is what lets `state()` answer for any CIK and window.
3. **`census.json`** (below).

No database table, migration or operator-visible surface changes.

### Label state

```
state(cik, label, session, lookback) -> "flagged" | "clear" | "unknown"
```

- `session` is a regular US session date, and `lookback` is an integer number of sessions, ≥ 1.
- The window is the `lookback` sessions ending at `session`, inclusive.
- Its date span is from the Eastern date of its first session minus one calendar day, through the Eastern date of
  `session`. The day of margin covers acceptances on the evening before the window opens, so they belong to it.

**The answer, in order of precedence:**
1. **`unknown`** if any of the following holds:
   - the CIK is not in the archive, or its `pages_complete` is false;
   - the window's date span starts before 2016-06-03 or ends after the archive's coverage end;
   - any accession of the CIK whose `filing_date` lies within the date span plus the following calendar day has
     non-`valid` items, or status `conflict` or `no_acceptance`, or is an out-of-scope 8-K type.
2. **`flagged`** if an observation with the label, status `ok`, has `available_session` within the window.
3. **`clear`** otherwise.

**Coverage end** is the last calendar day before the archive's capture date (Eastern). SEC builds the bulk archive
nightly, so filings made on the capture date may be absent. The census verifies that the archive's last 8-K filing
date equals the coverage end or the business day before it, and refuses otherwise.

**Margins.** A filing's `filing_date` is on or after its Eastern acceptance date, unless the date was adjusted
(Reg S-T Rule 13). The extra day of margin on the filing-date check catches an evening acceptance that receives
the next day's filing date. A filing whose date was adjusted to precede acceptance is a known limit. It can matter
only if its items are non-`valid`, and premise 2 measured none of those for our CIKs.

**Input validation.** `session` must be a regular session and `lookback` ≥ 1, or `state()` raises.

### Corrections and earlier states

- **One snapshot per artefact.** Each artefact pins one archive snapshot. A later snapshot is a new artefact
  version, and earlier versions are never rewritten.
- **Version reconciliation.** Between two versions, the producer prints:
  - accessions added and removed;
  - per-field changes on shared rows (CIK membership, form, filing date, items, status);
  - observations whose `labels` changed.

  For archives B and A, the measured change is 0 on CIK, form, filing date and items over 416,480 shared accessions.
- **Known limit: retrospective metadata.** A snapshot holds item metadata as of its capture. A code added later to
  an old accession would appear at that accession's original session. That is a backdated observation, and no
  contemporaneous source exists to rule it out. Two snapshots six weeks apart showed none (above), but that is two
  observations, not a guarantee.
  - Every output carries the label "retrospectively captured filing metadata".
  - Any consumer's registration names the artefact version it uses, and states this limit.

## Census (printed by every run, stored in the manifest)

**Per filing year, all CIKs and our CIKs separately:**
- accessions by form, with item validity counts;
- conflicts;
- observations per label and form;
- `no_acceptance` count.

**Plus:**
- the coverage end, and the archive's last filing date;
- the CIKs with incomplete pages.

## Survivorship census (gate 5)

The #3609 step 1 stage-A artefact (`STAGE_A_ARTEFACT`, a development sample, read through `read_verified_artefact`)
holds the survivorship-free universe, with each admitted series' linked CIK per formation month (`cik` on its rows).
For each formation month, the census reports:
- linked CIKs;
- linked CIKs present in the archive with complete pages;
- linked CIKs absent.

The stage-B months are not read. This census is the slice's survivorship result: it states, period by period, what
share of a survivorship-free universe the labels can describe at all. A consumer on another universe repeats it for
that universe.

## A/B between the existing source and the artefact (gate 6)

- **The two sources.** The existing structured source is `filing_events.items`. The new one is the artefact.
- **Population.** For our CIKs, over the measured window, per label, form and year, the census counts accessions
  carrying the code in each source and in both.
- **Expected result, from premises 1 and 2:**
  - every artefact-only observation is an accession absent from `filing_events`, or one with NULL `items` there;
  - `filing_events`-only observations are 0, apart from accessions beyond the archive's coverage end.
- **Outcome.** Any other difference fails the gate.

## Going-concern search (run in the build; its result is recorded, nothing is built from it)

- **Ruled out today:**
  - the stored XBRL facts (`financial_facts_raw`) hold a curated concept set with no going-concern element (one
    query in the build's record, not a premise of this spec);
  - 8-K metadata has no such field.
- **The build searches** two sources: the us-gaap and dei taxonomy element lists for every taxonomy year 2016–2026
  (element name and documentation), and SEC's Financial Statement and Notes data-set tag lists for 2016q2 through
  the latest quarter. For each element whose name or documentation mentions going concern or substantial doubt, it
  records the element, its data type, its documentation, its first quarter of use and its filer counts.
- **A candidate** is an element whose documentation defines it as the going-concern disclosure, or the auditor's
  going-concern conclusion. Auditor identity elements (`AuditorName`, `AuditorFirmId`, `AuditorLocation`) are
  recorded as context only and are never a candidate.
- **Outcomes:**
  - A candidate becomes a label only through its own build spec.
  - With no candidate, going-concern becomes a slice-3 candidate task with a deterministic baseline, as the programme
    states. The negative is recorded within the searched scope (named taxonomies, years and quarters), not as
    absence everywhere.

## Registration

Before the build's first census, the producer configuration is registered as a non-claiming `DeclaredTrial` in
`app/services/trial_register.py` (`3624-slice2-labels-v1`, one configuration, `searches=1`). Its `evidence` names:
- this spec's sha256;
- the construction hash;
- the archive sha256.

No alternative configuration is tried. Any later change of label definition, form scope, availability rule or
`unknown` policy is a new configuration and is counted in the programme ledger.

## Gates (decision recorded on #3624 before any downstream use)

**Progression.** The artefact is accepted when all six hold:
1. **Refusals:** `ARCHIVE_INCOMPLETE` did not fire, and the coverage-end check passed.
2. **Census:** on archive A, the census for our CIKs reproduces premise 2's validity counts and conflict counts,
   and the per-label, per-form observation counts in the measurement JSON's `labels`, exactly.
3. **Acceptance:** every observation has `acceptance_et`, or it is listed as `no_acceptance` with its fetch error.
4. **Document check.** A seeded draw (seed 3624) of 20 observations per label, plus 20 accessions per label whose
   valid items lack the code (drawn from our CIKs' 8-Ks), is checked by reading each filing's primary document on
   EDGAR. **Pass:**
   - every observation's document contains the item's heading;
   - no non-observation's document does.

   Every draw and its result are posted.
5. **Survivorship census:** it ran and is posted.
6. **A/B:** as above.

**Remediation.**
- A failure in 1–3 or 6 is fixed in the producer, and the census re-run under a new construction hash.
- A failure in 4 is reported per accession. If the metadata disagrees with the document, the slice records a
  metadata error rate within the draw and stops before acceptance; the treatment is then a new registration.

**No-go.** The slice ends without an accepted artefact if gate 4 shows any metadata error, or if the archive cannot
pass gate 1.

## Build order

1. The producer and fixtures. Fixtures:
   - a synthetic zip with recent and history pages, a missing page, a misaligned page, a co-registrant pair, a
     conflict, every item-validity state, and an out-of-scope form;
   - header fixtures in EDT and EST, at 07:00 and 21:00 ET.
2. `state()`, with a fixture test for every branch and every margin.
3. A dry run over archive A: census and coverage only, with no header fetch.
4. The registration.
5. The header fetch, then publish.
6. Gates 1–6, with the decision posted on #3624.

The build rung is behavioural with data semantics: fixtures, Codex checkpoint 2, and gates 1–6 as the
full-population evidence.

## Not done, and why

- **No change to `filing_events`** or its ingest horizon. That layer serves the operator, and the producer reads the
  archive.
- **No fix to the acceptance-time defect.** That is #3714; the producer uses headers only.
- **No event linking, feature, window, horizon or outcome read.** These belong to a consumer's registration (§4 of
  the programme).

## Checkpoint log

- **Round 1** (`var/research/3624/ckpt1_slice2_r1.txt`): 37 findings. The spec was rebuilt on the fail-closed rule.
  - **Availability** became the first session after the header's acceptance date (1, 2, 14). Acceptance in the
    window's previous evening is caught by a one-day margin, and a missing header gives `unknown` (12).
  - **Labels** became per-accession observations. Amendment linking and the orphan rule are gone (5–8, 25, 35).
  - **Coverage:** an explicit lower bound (9) and a coverage end from the capture date (10). Page inventory and
    misalignment refuse (11). `coverage.jsonl.gz` serves `state()` (13). Item validity has four states (15).
    Eligibility is derived, not asserted (16).
  - **Measurement:** the script was rewritten, and premises 2–3 were re-measured (17–22). Rows are keyed
    `(cik, accession)` (23). The Item 304(a)(3) text is completed and no deadline arithmetic remains (24). All
    inputs are pinned, with the CIK-list hash and full header rows (26). The ingest cause is stated as unestablished
    (27).
  - **Gates and limits:** version reconciliation covers every state field (28). A survivorship census (29), an A/B
    gate (30) and a registration (31) were added. Gate 4 is a document check over positives and negatives with a
    pass rule (32).
  - **Going-concern search:** scoped to named sources, with a candidate defined by documentation, and auditor
    elements as context only (33, 34).
  - The temporal API is typed in sessions (36), and out-of-scope 8-K types give `unknown` (37).
  - Finding 3: the backdating risk is stated as a known limit, labelled on every output, with snapshot drift
    measured. No contemporaneous source exists to remove it.
  - Finding 4: headers are reused across versions and never re-fetched.
