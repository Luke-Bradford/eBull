# #3227: 13F duplicates through the manager's own Other Included Managers

Status: spec (2026-10-22), revision 3. It adds a fourth 13F-duplicate tier and the 13F capture it needs. The built
specs stand in full unless this document says otherwise:
- the 13D arm, `docs/specs/ownership/2026-10-21-3227-13f-position-scope.md` (built #3950);
- the 13G extension (U), `docs/specs/ownership/2026-10-22-3227-13f-13g-covers.md` (built #3962);
- the observation-block tier (O), `docs/specs/ownership/2026-10-22-3227-13f-13g-observation-block.md` (built #3964);
- statement selection by date of event, `docs/specs/ownership/2026-10-22-3227-13f-statement-as-of.md` (built #3966).

Research: the 2026-10-22 residual comment on #3227. Rung: corpus change. It adds a schema migration, a capture on every
13F ingest path, a backfill and a read-path metric change, so it needs the full-population A/B and the corpus evidence
list. This document is the judgement artefact (Codex checkpoint 1).

## Revision history

- **Revision 1. Round 1** raised 4 P1, 17 P2 and 3 P3 findings, all accepted except P1 #1, which is answered in §
  Source rule (§ Round 1 disposition). The changes in revision 2:
  - **Column 6 is a condition.** Every row must be `DFND` or `OTR`. INGM, whose row is `SOLE`, becomes a residual.
  - **Lossless storage.** The capture is stored per Column 6/Column 7 segment rather than as derived arrays.
  - **Capture-only backfill** that leaves the archives in place.
  - **The census** follows the frozen rule, with two declared differences: the digit test, and no per-candidate
    completeness check (§ Full-population verification).
- **Revision 2. Round 2** judged P1 #1 sound as a rollup policy, on I14 attribution: *"I would not repeat the original
  P1 on that basis"*. It raised 0 P1, 11 P2 and 1 P3, all accepted (§ Round 2 disposition). Revision 3 adds:
  - a per-accession capture record whose completeness is checked against the summary page's own controls (Special
    Instructions 6.a and 6.b);
  - capture coherence through a capture id;
  - one lexical grammar for sequence numbers;
  - an exact capture-population gate in the ingest A/B.
  It drops the I16 argument.
- **Round 3, the cap,** raised 0 P1, 7 P2 and 2 P3, fixed here without a fourth round (§ Round 3 disposition). The fixes:
  - fingerprinted preservation, correcting the sync's cadence to weekly;
  - the 6.c value control;
  - list entries stored as filed;
  - the census described as provisional;
  - matched-set and date-precondition gates on the A/B snapshot.

## Problem

Every built tier ties a manager's 13F figure to its twin through a Schedule 13D/G cover. A 13F filer that reports a
controlled or advised entity's holdings states the link in the 13F itself, and we discard it.

Example: CTNM. Johnson & Johnson (CIK 200406) files a 13F of 1,979,173 for 2026-06-30 (`0000904454-26-000435`).
- The issuer row is `DFND` with Column 7 = 1.
- The List of Other Included Managers gives number 1 as Johnson & Johnson Innovation - JJDC, Inc., CIK 924020.
- JJDC's Form 3 reports 1,979,173.
- The pie counts both, and CTNM's pie is 38,130,001 against 23,099,173 outstanding.

Example: HHH. Pershing Square Inc. (CIK 2026053) files a 13F of 27,852,064 for 2026-06-30. Its two issuer rows are `DFND`,
one naming Managers 1–4 in Column 7 and one naming 1–4 and 6. Manager 1 is Pershing Square Capital Management, L.P.
(CIK 1336528), whose 13D reports 27,852,064. The pie is 161,805,639 against 59,721,036 outstanding.

## Source rule

**Form 13F, Special Instructions** (`https://www.sec.gov/files/form13f.pdf`):
- **7.b.** *"If this Form 13F report reports the holdings of one or more Managers other than the Manager filing this
  report, enter in the List of Other Included Managers all such Managers together with any CRD Number or SEC file
  number assigned to each Manager and, if known, the Managers' respective Form 13F file numbers … Assign a number to
  each Manager … The List of Other Managers must include all other Managers identified in Column 7 of the Information
  Table."*
- **11.b.vi(B) and (C)** (Column 6). `DFND` and `OTR` holdings are segregated, each on its own line, into *"those
  securities over which investment discretion is shared with another Manager or Managers on whose behalf this Form 13F
  report is being filed, and those securities over which investment discretion is shared with any other person"*. The
  NOTE adds: *"A Manager is deemed to share discretion with respect to all accounts over which any person under its
  control exercises discretion."*
- **11.b.vii** (Column 7). *"Identify each other Manager on whose behalf this Form 13F report is being filed with whom
  investment discretion is shared as to any reported holding by entering in this column the number assigned to the
  Manager in the List of Other Included Managers. Enter this number in Column 7 opposite the segregated entries in
  Columns 4, 5, and 8 (and the relevant indication of shared discretion set forth in Column 6) …"*

So on a `DFND` or `OTR` row, a Column 7 number is the filer's own statement of two things: the row's shares are held
on behalf of that included Manager, and discretion over them is shared with it. A `SOLE` row with a Column 7 number
contradicts the cross-column requirement quoted above, and the instructions do not say which column wins. Such a row
identifies no one (§ Rule). 11.b.vii also requires that *"A Manager must report the conditions of sharing discretion with other Managers
consistently for all holdings reported on a single line"*, so a line's Column 7 attribution covers all of that line's shares.

**Why the attribution reaches k's other figures.** No Form 13F rule equates discretion with Rule 13d-3 beneficial
ownership. The 2026-10-21 refusal stands: two *different* identities with equal values are not shown to be one
position. This tier does not rely on equal values across identities.
- Column 7 attributes M's T shares to k by M's own structured statement. The 13F position is then k's position under
  another filer's CIK.
- From there, the rollup's settled owner-once invariant **I14** (`data-engineer` skill) applies: 13D/G against 13F
  against Form 4 for one owner are *"same shares, different lens"*, aggregated by MAX. So T and k's own figure of T are
  MAX-ed, the same as when they share a CIK.
- The new link is the attribution of one holding, structured and filed by M. It does not establish that M and k are one
  manager family (I16's curated registry), and it does not replace a value comparison.

**Structured fields.** No text classifier is involved.
- **Data set** (`data-sources/sec-edgar.md` §2.1). `INFOTABLE.tsv` carries `INVESTMENTDISCRETION` (`SOLE`/`DFND`/`OTR`)
  and `OTHERMANAGER` (sequence numbers). `OTHERMANAGER2.tsv` carries `ACCESSION_NUMBER`, `SEQUENCENUMBER`, `CIK`,
  `FORM13FFILENUMBER`, `CRDNUMBER`, `SECFILENUMBER` and `NAME`.
  - `form13f_01jun2025-31aug2025.zip` has no `OTHERMANAGER2.tsv`; the four later cached archives have it.
- **XML.** The information table has `otherManager` per row. The primary document has
  `summaryPage/otherManagers2Info/otherManager2` (`sequenceNumber`, `otherManager/cik`, `name`, `form13FFileNumber`).
  - EdgarTools 5.30.2, already a dependency, parses both: the `OtherManager` column
    (`edgar/thirteenf/parsers/infotable_xml.py`) and `OtherManager(cik, name, file_number, sequence_number)`
    (`primary_xml.py`).
  - `app/providers/implementations/sec_13f.py` (`parse_infotable`, `parse_primary_doc`) discards both.
- **The CIK is optional** in a list entry. RXT's and BOBS's managers carry none; a number whose entry has no CIK
  identifies no node here.
- **We store neither field.** `thirteen_f_normalise.normalise_13f_holdings` and the bulk `_INSERT_FROM_STG_SQL` sum the
  split rows (#1567) and drop Column 7.

## Rule (frozen)

**Tier (IM)** runs after (O). Nothing in it feeds back into an earlier tier.

1. **Considered.** Candidates (`_13f_candidates`, total T, period P) outside the pre-uniqueness matched sets of the 13D
   arm, (U) and (O). Every twin in those three sets is reserved.
2. **One filing.** M's kept rows all come from one 13F accession A, by `winning_accession`.
3. **The position.** Exactly one current observation (`ownership_institutions_observations`) matches:
   - instrument, M's `filer_cik`, `period_end` = P, `source_document_id` = A, `source = '13f'`,
     `ownership_nature = 'economic'`, `exposure_kind = 'EQUITY'` and `known_to IS NULL`;
   - its shares are T;
   - its `thirteen_f_segments` is captured and **self-consistent**: the segment shares sum to the observation's shares;
   - it is **coherent**: its capture id equals the capture id of A's capture record (§ Change 1);
   - that record is `complete`.
4. **Named sequence numbers.** Q is the set of sequence numbers present in Column 7 of **every** segment. Every segment
   must be `DFND` or `OTR`; otherwise Q is empty.
5. **Named Managers.** N maps Q through A's List of Other Included Managers to canonical CIKs. A number with no entry,
   or an entry with no CIK or a non-digit CIK, contributes nothing.
6. **Twin k.** A co-filer graph node (`_is_cofiler_node`) whose canonical CIK is in N and is not M's, with an owner
   total of T.
7. **Duplicate.** Exactly one twin, no other (IM) candidate's twin, and not reserved. Twins are counted over every (IM)
   candidate's twin set before reserved twins are refused, as the built tiers count.

**Every segment.** A segment without k gives no identification of k for its shares, so T would not be wholly
attributed to k. This is not a claim that discretion over those shares is not shared with k.

**The fold** is the built `_fold`: M's rows leave `institutions` onto k's largest row, with one
`cofiler_group_collapse` correction. **Wording:**

`13F duplicate counted once (#3227): {M name} ({M cik}) 13F {T} for {P}; its 13F {A} reports every line of the
position as shared discretion with included manager {k name} ({k cik}), Column 7. Counted once by policy: that the
manager's own figure counts the same shares is not verified.`

## Accepted identification risk (declared, not measured)

The built specs' risks 1–13 apply where they concern k and the fold. Added:

14. **Attribution is not set identity.** Column 7 attributes M's T shares to k. k's own figure of T may still be a
    different position of the same size; the owner-once MAX (§ Source rule) is what treats them as one.
15. **Turnover.** k's kept row can predate P by years: INTA's Form 4 is dated 2023-11-17, BCAL's Form 3 2023-05-10 and
    CTNM's Form 3 2024-04-09, all against P in 2025–26. If k's position was replaced by a different one of the same
    size before P, nothing detects it. This is the built specs' risk 3, accepted on the same terms. A same-size
    replacement folds, and a test pins that behaviour as declared, not detected.
16. **A wrong list entry.** A mis-keyed CIK in the List of Other Included Managers is not detected beyond the totals'
    equality.
17. **Coverage.** A position refuses when its capture is absent, `incomplete`, `invalid` or not coherent. That covers
    accessions outside the four archives and per-filing accessions whose cover is swept. The census counts archive
    availability only; the build PR measures stored capture coverage by status.
18. **Preservation through uncaptured writes.**
    - **The weekly sync.** `sync_institutions` is reached through `sync_all` by the weekly
      `ownership_observations_backfill` (Sunday 03:00 UTC, `app/workers/scheduler.py`), and cannot capture.
      Invalidating on its write would erase every capture weekly.
    - **The bulk path** writing from an archive without `OTHERMANAGER2.tsv` cannot capture either.
    - § Change 1 therefore preserves segments under a fingerprint, not under shares alone:
      - **Bulk:** an uncaptured write keeps them only when its shares *and* its CUSIP set equal the stored ones.
      - **Sync:** `institutional_holdings` has no CUSIP column, so the sync's fingerprint is its shares plus its
        provenance. Every 13F writer of `institutional_holdings` is a capture-capable per-filing path writing both
        tables for the accession in one transaction: legacy `institutional_holdings.py:1711`, manifest
        `sec_13f_hr.py:504` and rewash `rewash_filings.py:1801`. The sync therefore replays a population a capture
        already described.
    - **Declared residue.** An observation captured by the bulk path, under a different CUSIP resolution, and later
      replayed by the sync from an older per-filing population of exactly equal total keeps the bulk segments. A test
      pins this, and the ingest A/B's exact recomputation measures it on the backfilled population.

## Full-population verification (dev, 2026-10-22, census, read-only)

`~/Dev/.ebull-autonomy-scratch/3227/res/r_im_census3.py` ran from the repo root with `PYTHONPATH=.` on main `88327490`
(revision 3 of the script, log `r_im_census3.log`, cases in `r_im_census3.json`, 1,106 s, exit 0).
- **Inputs.** One `REPEATABLE READ` snapshot, session time zone UTC. The real rollup runs on every instrument and the
  fold's input is captured. The f13q oracle reproduces the built folds on every instrument (0 mismatches) and supplies
  the three matched sets.
- **Archive reads.** Column 6/7 and the lists are read from the four cached archives that have `OTHERMANAGER2.tsv`:
  - SH rows with positive shares, no put/call, the instrument's CUSIPs;
  - Column 7 split on commas and whitespace, with positive-integer tokens (`str.isdigit`, not yet § Change 1's ASCII
    grammar);
  - a list with a duplicate or non-positive sequence number is invalid.
- It does not check the completeness controls per candidate; the 13 case accessions are checked below.
- **Checks.** M's kept rows come from one accession, and the rows sum to T. The stored observation of § Rule 3 exists
  once with shares T. Sequence numbers are intersected before they are mapped.
- **Graph check.** It removes the duplicates' M rows from the post-fold input, re-runs the real `_cofiler_graph`, and
  fails on any change to #3898's edges or cliques.

This census is **provisional evidence**, not verification of the frozen rule. It reads the archives, not a stored
capture; it tests digits with `str.isdigit` rather than § Change 1's ASCII grammar; and it does not apply the completeness
controls per candidate. The build's A/B reads the stored capture against oracles that apply the final contract (§ Change
5).

**Population.** 5,510 instruments selected; 4,396 reach the fold; 0 errors; 0 baseline mismatches.
- 123 candidates are in a built matched set.
- 315,985 candidate instances have their accession outside the four archives (risk 17); 1,029,434 are inside.
- Of those 1,029,434 candidate instances:
  - 697,443 have an accession with no List of Other Included Managers;
  - 85,653 have lines with no valid Column 7 number (empty, or populated with an invalid value such as `0` or `NONE`;
    the script does not separate the two);
  - 246,338 have at least one valid Column 7 number.
- The three sum to 1,029,434.
- **Matched sets.** 123 candidates are in the f13q oracle's three matched sets, the same count as the built folds.
  Revision 1 of the census excluded only the built folds and their twins, and found the same duplicates under the same
  reading (before the parser fix). The (IM) set does not depend on the oracle's sets on this corpus.
- **Date precondition.** The oracle dates covers by accession, which requires every accession to carry one date of
  event. 0 accessions carry two dates, or a mix of NULL and non-NULL (dev, 2026-10-22).
- 0 refuse for spanning two accessions, 0 for rows not summing to T, and 0 for the stored observation.
- **Parser.** In the census's population (listed accessions; SH lines with positive shares, no put/call; all four
  archives), 207,375 lines carry a Column 7 value that is not a list of positive integers, and 9 accessions have an
  invalid list.
  - A separate scan of every line of every accession in `form13f_01jun2026-31aug2026.zip`, unfiltered, found 394,662
    such lines. 308,421 are `0`, followed by `NONE`, `N/A`, `NO` and `-`, and free-text names.
- **Completeness controls** (same archive, all 10,036 accessions):
  - the Number of Other Included Managers equals the list's rows on 10,036;
  - the Information Table Entry Total equals the line count on 9,874, and on 1,187 of the 1,206 listed accessions;
  - all 13 accessions behind the duplicates below pass both.
  - The null markers name no one. The names break 11.b.vii (*"Enter no other names or numbers in Column 7"*). Both
    identify no Manager under § Rule.

**Duplicates: 16 on 16 instruments, −110,742,341.**
- 0 refused by reservation or uniqueness; 0 #3898 graph changes; 3 twins in a clique.
- 0 instruments fixed and 0 newly oversubscribed. 14 of the 15 with a denominator are oversubscribed on main; YSWY has
  no denominator.
- **Counting `SOLE` lines as naming** (not the rule) would give 17 / −288,208,660, the addition being INGM.

| instrument | M (13F) | k | T | k's kept row | M's lines (Column 6, Column 7) |
|---|---|---|---:|---|---|
| HHH | PERSHING SQUARE INC. (2026053) | Pershing Square Capital Management, L.P. (1336528) | 27,852,064 | 13d 2026-06-08 | DFND 1-4; DFND 1-4,6 |
| INTA | Temasek Holdings (Private) Ltd (1021944) | Anderson Investments Pte. Ltd. (1767731) | 17,146,805 | form4 2023-11-17 | DFND 1,4,6,19,20,24 |
| IMDX | BROADWOOD CAPITAL INC (1278387) | BROADWOOD PARTNERS, L.P. (1278386) | 12,684,884 | form4 2026-04-27 | DFND 1 |
| ASPN | KIM, LLC (1888968) | Koch, Inc. (2027344) | 12,280,426 | form3 2024-08-01 | DFND 1 |
| RHI | Orbis Allan Gray Ltd (1663865) | ORBIS INVESTMENT MANAGEMENT LTD (940594) | 9,877,840 | 13g 2026-08-14 | DFND 1 |
| DAO | Orbis Allan Gray Ltd (1663865) | Orbis Investment Management Limited (940594) | 6,563,497 | 13g 2026-08-14 | DFND 1 |
| CAVA | Artal Group S.A. (1053906) | Artal Participations S.a r.l. (2048403) | 6,507,990 | form4 2026-06-15 | DFND 1-5 |
| AVBP | HHLR ADVISORS, LTD. (1762304) | Hillhouse Investment Management, Ltd. (1510589) | 3,929,117 | 13g 2026-02-17 | OTR 1 |
| ARVN | D. E. Shaw & Co., Inc. (1009207) | D. E. Shaw & Co., L.P. (1009268) | 3,495,153 | 13g 2026-08-14 | DFND 1; DFND 1,2 |
| YSWY | PRINCIPAL FINANCIAL GROUP INC (1126328) | PRINCIPAL GLOBAL INVESTORS (769734) | 2,654,505 | 13g 2026-08-05 | DFND 1,3,4; DFND 1,4 |
| BCAL | Castle Creek Capital Partners VI, LP (1666749) | Castle Creek Capital VI LLC (1671438) | 2,340,719 (P 2025-09-30) | form3 2023-05-10 | OTR 1 |
| WNC | D. E. Shaw & Co., Inc. (1009207) | D. E. Shaw & Co., L.P. (1009268) | 2,068,519 | 13g 2026-08-14 | DFND 1; DFND 1,2 |
| CTNM | JOHNSON & JOHNSON (200406) | Johnson & Johnson Innovation - JJDC, Inc. (924020) | 1,979,173 | form3 2024-04-09 | DFND 1 |
| AMR | CONTINENTAL GENERAL INSURANCE CO (24046) | Continental General Holdings LLC (1872137) | 644,025 | form4 2025-12-15 | DFND 1-3 |
| CABO | D. E. Shaw & Co., Inc. (1009207) | D. E. Shaw & Co., L.P. (1009268) | 477,500 | 13g 2026-08-14 | DFND 1,2; DFND 1; DFND 1,4 |
| HYPD | LASRY MARC (1259927) | Avenue Capital Management II, L.P. (1317338) | 240,124 (P 2025-09-30) | 13d 2025-09-19 | DFND 1,2 |

- YSWY is new in revision 3. Its two lines carry Column 7 as `'1 3 4'` and `'1 4'`, separated by spaces. Revision 2
  split only on commas.
- **Twins inside a #3898 clique:** IMDX, CAVA and AMR (§ Change 6, § Acceptance).

- **Cross-source.** For CTNM, INTA and HYPD, M's EDGAR `informationtable.xml` and `primary_doc.xml` carry the same
  Column 6, Column 7 and included-manager CIK as the data set (`res/r_om.log`).

## Change

### 1. Schema (one migration)

- **`institutional_13f_other_managers`**, one row per list entry as filed, invalid entries included:
  - columns: `accession_number TEXT`, `entry_index INTEGER` (source order), `sequence_number_raw TEXT`,
    `sequence_number INTEGER NULL` (NULL when the raw value breaks the grammar), `cik TEXT NULL` (as filed, trimmed),
    `name TEXT`, `form13f_file_number TEXT NULL`, `ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()`;
  - primary key `(accession_number, entry_index)`.
  - CRD and SEC file numbers are not stored. The rule uses only the CIK, and EdgarTools' XML object carries neither,
    so both paths store the same four fields.
- **`institutional_13f_captures`**, one row per captured accession. Columns:
  - `accession_number TEXT PRIMARY KEY`, `capture_id UUID NOT NULL`;
  - `source TEXT` (`dataset` or `xml`);
  - `status TEXT CHECK (status IN ('complete', 'incomplete', 'invalid'))`;
  - `list_count`, `expected_list_count`, `line_count`, `expected_line_count` (INTEGER), `value_total`,
    `expected_value_total` (NUMERIC);
  - `captured_at`.
- **Status**, against the summary page's own controls:
  - **`complete`:** the list's entry count equals the Number of Other Included Managers (Special Instruction 6.a: *"If
    none, enter the number zero"*), and the parsed information-table line count equals the Information Table Entry
    Total (6.b: *"the total number of line entries"*). A list of zero entries with a count of `0` is a complete,
    empty capture (7.a's `NONE`).
    - The summed Column 4 must also match the Information Table Value Total (6.c), within half a unit per line. 6.c
      requires a *"rounded figure, corresponding to the individual Column 4 entries"*, and Special Instruction 8 rounds
      each entry to the nearest dollar.
    - Measured on `form13f_01jun2026-31aug2026.zip`: exact on 9,745 of 10,036 accessions and within 0.01% on 168 more.
      YSWY's accession differs by 3 on 203,639,295,722 over 4,413 lines. The build reports the count within the bound.
    - The controls establish the cardinality and the value sum of what was parsed, not each line's content.
  - **`incomplete`:** either count differs, or a control is absent.
  - **`invalid`:** a list sequence number breaks the grammar below, or is duplicated. Its entries are written as
    filed, and nothing maps through them.
  - Only `complete` serves § Rule.
- **Sequence-number grammar**, one definition for list entries and Column 7 tokens: ASCII digits only, 1–9 of them
  (`^[0-9]{1,9}$`), value ≥ 1, compared as an integer. Anything else is invalid, never an exception.
- **Replacement.** A capture of an accession, in one transaction:
  - writes a new `capture_id`;
  - replaces the capture record and all list entries (delete, then insert);
  - writes the segments, with that `capture_id`, on every observation of the accession that it writes.
  An observation of the accession that this pass does not write keeps its old `capture_id`, and § Rule 3's coherence
  check refuses it. No reparse can map old sequence numbers through a new list.
- **`ownership_institutions_observations.thirteen_f_segments JSONB NULL`.** NULL means not captured. Otherwise it holds:

  `{"capture_id": "…", "cusips": ["…"], "lines": [{"discretion": "DFND", "other_managers": [1, 2], "shares": "123"}, …]}`

  - `lines` holds the position's information-table lines aggregated by `(Column 6, sorted Column 7 numbers)`, sorted.
    A line whose Column 7 breaks the grammar keeps its shares, with `"other_managers": null`.
  - `cusips` is the sorted set of CUSIPs whose lines were aggregated.
  - It is lossless for this rule, auditable against the source, and survives both aggregation stages (CUSIP, then
    instrument), where `investment_discretion` collapses to NULL today.
- **Upsert.** A capture-capable write replaces the segments. A write that cannot capture keeps them only under risk
  18's fingerprint, and sets them NULL otherwise:
  - the bulk path from an archive without `OTHERMANAGER2.tsv`: shares and CUSIP set equal;
  - `sync_institutions`: shares equal.
  The read's self-consistency check refuses any remaining mismatch.

### 2. Capture on every 13F path

The #1567 lesson (`docs/review-prevention-log.md`, "SUM at every locus") applies: one locus missed is a silent gap.
- **Parser** (one function, shared by every path). Split Column 7 on commas and whitespace. Every token must meet §
  Change 1's grammar; otherwise the line's Column 7 is invalid (`null`). The caller logs a WARN carrying the accession,
  because `parse_infotable(xml)` has no accession.
- **Controls.** The XML path reads `otherIncludedManagersCount`, `tableEntryTotal` and `tableValueTotal`, which
  EdgarTools parses from the summary page; the bulk path reads `SUMMARYPAGE.tsv`.
- **Provider.**
  - `ThirteenFHolding` gains `other_managers: tuple[int, ...] | None` from EdgarTools' `OtherManager`.
  - `ThirteenFFilerInfo` gains the list from EdgarTools' `OtherManager` objects.
- **`normalise_13f_holdings` / `merge_resolved_by_instrument`** aggregate the segments at both stages.
- **Per-filing writers** (`_record_13f_observations_for_filing`: manifest `_parse_13f_hr`, legacy
  `_ingest_single_accession`, rewash `_apply_13f_infotable`) write the segments and replace the accession's list.
- **Bulk** (`sec_13f_dataset_ingest`):
  - read `OTHERMANAGER2.tsv` when present, and stage `OTHERMANAGER` and `INVESTMENTDISCRETION` per line;
  - `_INSERT_FROM_STG_SQL` aggregates the segments in its `GROUP BY` (a pre-aggregation CTE by segment);
  - an archive without `OTHERMANAGER2.tsv` writes NULL segments under the § Change 1 upsert.
- **`sync_institutions`** cannot capture. That is stated, tested and counted.
- `scripts/check_13f_hr_retention.sh`'s call-site count and `tests/test_13f_normalise_applied_everywhere.py` are kept
  true.

### 3. Backfill

- **A capture-only script**, `scripts/backfill_13f_included_managers.py`, reads the four cached archives read-only. For
  each observation of an accession in them, it UPDATEs only `thirteen_f_segments` and replaces the accession's list
  under § Change 1's replacement rule. It writes no other column and deletes no archive; the archives stay until the
  gates pass.
  - The archives are public (SEC Form 13F Data Sets) and re-downloadable. The script records each archive's
    `.sha256`.
- **Per-filing accessions** outside those archives keep NULL and refuse (risk 17). No refetch is in scope: their primary
  documents are swept (`raw_filings.SWEPT_DOCUMENT_KINDS`). The PR counts them.
- **Ingest A/B (corpus rule)** over the whole `ownership_institutions_observations` population, before and after the
  backfill:
  - identical key population, with no row inserted or removed;
  - every pre-existing column identical, `ingest_run_id` and `ingested_at` included;
  - **Capture population, exact.**
    - Every accession in the four archives has a capture record. Its status, its four counts and its list entries
      (as filed) equal those an independent reader in the A/B script computes from `SUBMISSION`, `SUMMARYPAGE`,
      `OTHERMANAGER2` and `INFOTABLE`.
    - Every `source = '13f'` observation of those accessions, of every exposure, has non-NULL segments carrying its
      accession's `capture_id`, and none outside them gains segments.
    - Every segment set equals the independent reader's lines for that key, `cusips` included, and is self-consistent.
  - Status counts by `complete` / `incomplete` / `invalid` are reported.
  - Exit non-zero on any difference.

### 4. `ownership_rollup.py`

- **Bounded read for (IM)**, only when an (IM) candidate exists: M's observation by § Rule 3's full key and A's list
  status and entries. Served by the observation primary key and partition pruning on `period_end`.
- **`_fold_13f_duplicates`** gains the tier after (O). It takes a new pure input, (IM) evidence by candidate, so it stays
  pure. Its docstring names this spec.

### 5. The rollup A/B

A sibling of `scripts/ab_3227_13f_statement_date.py`, with base the real code at the merge base, run after the
backfill:
- **Built folds** compared by `(tier, M, k, T, C)` with zero tolerance.
- **One snapshot** holding base, treated and two oracles: one over the stored capture, one over the archives. The
  treated increment must equal both. Any difference between the two oracles is a capture error and fails the A/B.
  Differences from the census set must each be reconciled to source, or the A/B fails. A reconciliation is either live
  ingest after the census, or one of the census's declared method differences (digit test, completeness), evidenced
  per case.
- **Matched sets.** `_fold_13f_duplicates` exposes each tier's pre-uniqueness matched set through a pure debug return
  used only by tests and the A/B. The A/B compares them with the oracle's, per tier and per instrument, with zero
  tolerance.
- **Date precondition** on the A/B snapshot: 0 accessions with two dates of event, or a mix of NULL and non-NULL, or
  the A/B fails. The oracle dates by accession.
- **Pie** = base − Σ T per instrument; #3898's graph unchanged; latency p95 ≤ 10% over all instruments and the
  affected ones.

### 6. Case review in the PR

For every duplicate, from EDGAR:
- M's `informationtable.xml` lines for every captured CUSIP of the position, aggregated by § Change 1's segment key,
  equal to the stored segments;
- the accession's raw line count, equal to the capture record's `line_count`;
- M's `primary_doc.xml` summary-page controls, equal to the capture record;
- M's list entry for every number referenced on those lines, equal to the stored entry. At least one number common to
  every line maps to k's canonical CIK;
- every kept row of k, from its own filing (accession, figure, date), and the derivation of k's owner total T from
  them.

A mismatch stops the build. Each twin inside a #3898 clique records its final collapse outcome and correction trail.

## Acceptance (build PR)

The built specs' Acceptance tests still pass.

**Pure tests:**
- **Folds:**
  - k named on every segment, with one segment and with several segments of different Column 7 sets sharing k;
  - a twin whose owner total T comes from two kept rows (Form 3 and Form 4 lots);
  - a same-size different position: it folds, pinning risk 15 as declared rather than detected.
- **Refuses:**
  - a segment lacking k;
  - a `SOLE` segment, even one naming k;
  - k's number with no list entry, a blank or non-digit CIK, or an invalid list;
  - NULL segments, or segments not summing to shares;
  - a capture record `incomplete` or `invalid`, or a segments `capture_id` differing from the record's;
  - M's rows spanning two accessions;
  - M's observation shares ≠ T;
  - zero or two matching observations.
- k is not M: a list entry carrying M's own CIK is not a twin.
- **Uniqueness and reservation:**
  - two (IM) candidates sharing a twin both refuse;
  - an (IM) candidate with two twins refuses;
  - a multi-twin candidate sharing one twin with an otherwise unique candidate makes both refuse;
  - a twin matched by the 13D arm, (U) or (O) refuses even when that earlier match failed uniqueness, and is counted
    before reservation is applied.
- Wording matches § Rule.

**Capture tests**, as expected-output fixtures through every writer:
- Column 7 and list sequence-number grammar: `"1"`, `"1,2"`, `"1, 19, 4"`, `"1 3 4"`, `"01"`, empty, `"0"`, `"NONE"`,
  a name, a non-ASCII digit, and a 10-digit number;
- completeness: list count against the Number of Other Included Managers (including `0`/`NONE`), and line count against
  the Information Table Entry Total;
- capture coherence: a recapture that does not rewrite one observation of the accession leaves that observation
  refusing;
- an equal-total uncaptured rewrite keeps the segments (risk 18, declared), and a captured rewrite with a different line
  set replaces them;
- two CUSIPs resolving to one instrument;
- mixed discretion within one position across both aggregation stages;
- an invalid list (duplicate sequence number);
- list replacement on reparse;
- `sync_institutions` with equal and with changed shares;
- replay orders: bulk then per-filing, per-filing then bulk, and sync last;
- an archive without `OTHERMANAGER2.tsv`;
- Python and SQL segment aggregation agreeing on the same lines.

**DB tests:**
- a seeded 13F observation with captured segments, list rows and a Form 3 node folds through the real read under a
  non-UTC session;
- an integrated test where k is inside a #3898 clique, for k surviving and for k dropped, asserting the final
  correction trail and dropped sources.

**Corpus evidence** (CLAUDE.md § Corpus changes):
- smoke panel AAPL, GME, MSFT, JPM, HD (figure observed);
- cross-source check against EDGAR for CTNM;
- the backfill run with its ingest A/B;
- live re-check of `/instruments/CTNM/ownership-rollup` (−1,979,173) and `/instruments/HHH/ownership-rollup`
  (−27,852,064).

## Accepted residuals

Each was checked on EDGAR in the research comment.
- **INGM**, 177,466,319. Its row is `SOLE` with Column 7 = 1 (Platinum Equity, LLC), which § Rule refuses.
- **IFF**, 25,356,381. Split power: the Winder blocks vote and Freemont disposes. Only Item 4's text says one block.
- **AIRS**, 30,324,180. M's own cover gives dispositive 25,154,360; 5,169,820 are another holder's shares, held under a
  voting proxy.
- **DH**, 62,493,676 (statement-as-of spec).
- **Class C, GAMCO** (six pairs). GAMCO Investors' 13F has no included managers.
- **Class B, RXT.** Its included manager has no CIK, and its twin is not on the list.
- **Class B, BOBS.** Its line has no Column 7.
- **The other class-B pairs:** their lines are `SOLE`, or they name included managers that do not include the twin.

## Round 1 disposition

| # | finding | disposition |
|---|---|---|
| 1 | P1: Column 7 does not tie k's beneficial position to the block | Answered in § Source rule. Attribution plus the settled owner-once MAX (I14; the I16 argument was withdrawn in revision 3), not equal values across identities. Risk 14 states what stays unverified. |
| 2 | P1: `SOLE` contradicts 11.b.vii's cross-column wording | Accepted. `DFND`/`OTR` required; INGM is a residual. |
| 3 | P1: absence of k overstated | Accepted; § Rule "Every segment" reworded. |
| 4–7 | P2: census single accession, intersection order, parser, stored observation | Accepted. Census revision 3 implements each. |
| 8 | P1: Column-7-bearing count mislabelled | Accepted. Positions with a valid Column 7 number are counted, and empty and populated-invalid fields are reported together as not separated. |
| 9 | P2: capture completeness | Accepted. List status table, atomic replacement, NULL against captured. |
| 10 | P2: derived arrays lossy | Accepted. Lossless `thirteen_f_segments`; the A/B adds an independent archive oracle. |
| 11 | P2: stale preserved capture | Accepted. Preserved only on equal shares; read-time self-consistency check. |
| 12 | P2: sync path cannot restore | Accepted; stated, tested, counted (risk 17). |
| 13 | P2: list semantics | Accepted (§ Change 1). |
| 14 | P2: `ownership_nature` | Accepted (§ Rule 3). |
| 15 | P2: backfill invariant against `ingest_run_id` | Accepted. Capture-only UPDATE; the ingest A/B demands every other column identical. |
| 16 | P2: archives consumed before gates | Accepted. The backfill script deletes nothing. |
| 17 | P2: turnover untested | Accepted as declared risk 15, with a test pinning the declared behaviour. |
| 18 | P2: clique outcome | Accepted. Integrated DB test and case-review record. |
| 19 | P2: uniqueness tests | Accepted (§ Acceptance). |
| 20 | P2: capture fixtures | Accepted (§ Acceptance). |
| 21 | P2: A/B discrepancies | Accepted. Two oracles; unexplained differences fail. |
| 22 | P3: WARN context | Accepted; the caller logs. |
| 23 | P3: residual wording | Accepted. |
| 24 | P3: heading | Accepted; "Special Instructions", with the cross-column wording quoted. |

## Round 2 disposition

Round 2 marked round 1's #1–#7, #10, #14–#20 and #22–#24 resolved, and #8, #9, #11, #12, #13 and #21 partly resolved.
Its new findings are all accepted:

| # | finding | disposition |
|---|---|---|
| 1 | P2: I16 overreaches | Accepted. The I16 argument is removed; I14 attribution stands (§ Source rule). |
| 2 | P2: no completeness contract | Accepted. `institutional_13f_captures` status checked against Special Instructions 6.a and 6.b, measured on 10,036 accessions. |
| 3 | P2: equal-total preservation can keep stale segments | Accepted in part. Invalidation would erase every capture under the daily `ownership_observations_sync`. Preservation is argued by construction (risk 18), the ingest A/B measures it, and tests pin both behaviours. |
| 4 | P2: list replacement against unrewritten observations | Accepted. `capture_id` coherence (§ Change 1, § Rule 3). |
| 5 | P2: ingest A/B conditional on capture | Accepted. Exact capture-population gate, list entries and status included. |
| 6 | P2: oracle matched sets unvalidated; date precondition | Accepted. The (IM) set does not depend on the matched sets (census revision 1 against 3); the date precondition is measured at 0. |
| 7 | P2: census labels | Accepted. Candidate instances; populated-invalid noted as not separated. |
| 8 | P2: risk 17 against upsert | Accepted. Refusal is by capture state; census against build measurement separated. |
| 9 | P2: sequence-number grammar | Accepted. One ASCII grammar with an INTEGER-safe range; fixtures added. |
| 10 | P2: case review demands every number map to k | Accepted. Every referenced mapping is verified; at least one common number maps to k. |
| 11 | P2: k's total may be several rows | Accepted. The case review derives T from every kept row; multi-row fixture added. |
| 12 | P3: invalid-token denominators | Accepted. Both populations are stated. |

## Round 3 (cap) disposition

Round 3 marked round 2's #4 and #7–#12 resolved, #1, #2, #5 and #6 partly resolved, and #3 not resolved. Its findings
are fixed in place:

| # | finding | disposition |
|---|---|---|
| 1 | P2: the daily job does not call `sync_institutions` | Accepted. The sync is weekly (`ownership_observations_backfill`); risk 18 is corrected. |
| 2 | P2: "safe by construction" contradicted by the uncaptured bulk path | Accepted. Bulk preservation needs equal shares *and* CUSIP set; the sync's argument is the writer pairing; the residue is declared and tested (risk 18). |
| 3 | P2: 6.c value control omitted | Accepted. Added, with the half-unit-per-line bound from Special Instruction 8. |
| 4 | P2: invalid lists not stored, but compared as filed | Accepted. Every entry is stored as filed, with a raw and a parsed sequence number. |
| 5 | P2: census overstated as exact | Accepted. Described as provisional; method differences are reconcilable per case. |
| 6 | P2: matched sets and date precondition not gated on the build snapshot | Accepted. Both are A/B gates. |
| 7 | P2: CRD and SEC file numbers unobtainable from XML | Accepted. Not stored. |
| 8 | P3: case review compares raw lines with aggregated segments | Accepted. Compared after aggregation; raw line count checked separately. |
| 9 | P3: stale dispositions | Accepted. Round 1 rows #1 and #8 corrected. |
