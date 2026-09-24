# #2351 — store Item 403 at its own grain: (holder, title of class)

Status: spec, round 2 (2026-09-24). Supersedes further per-case suppression slices
(supervisor, #2351 08:05Z). Slices 1–3b of `2026-09-24-2351-def14a-class-recipients.md`
stay live until M3 retires them.

Round 1 (ckpt-1, 83 findings, `var/census_2351/ckpt1_r1.txt` on the author's worktree)
specified schema, binding and reader cutover at once. Most findings were about committing
to a store key and a binding before the class-grain extraction itself had been measured.
Round 2 re-orders: **interpret first (pure, measured), bind second (pure, measured), store
and switch readers last.** Nothing is written to the DB until M3.

## Problem

`parse_beneficial_ownership_table` returns one `Def14ABeneficialHolder` per holder per
accession. It reads ONE shares column (`_resolve_columns`) and keeps a holder's FIRST row
(`seen`, `sec_def14a.py::_extract_holder_rows`). The store key is
`(instrument_id, accession_number, holder_name)` (`sql/144`). So a holder's per-class lines
have nowhere to go, and the class of the kept line is never stored. The four shipped
slices each *withhold* a mis-bound row after the fact; none stores the class.

## Source rule

- 17 CFR 229.403(a) and (b) prescribe the columns *(1) Title of class, (2) Name [and
  address] of beneficial owner, (3) Amount and nature of beneficial ownership, (4) Percent
  of class*. Amount and percent are figures of the class in column (1). 403(b) covers
  "each class of equity securities of the registrant **or any of its parents or
  subsidiaries**", so a line's class need not be a class of the registrant. No
  instruction to Item 403 prescribes a layout for several classes (read at
  law.cornell.edu/cfr/text/17/229.403, 2026-09-24). The three layouts below are
  **observed** in the corpus, not prescribed, and M1 records evidence rather than
  resolving it.
- The markup is the source: the Item 403 table has no structured-data (XBRL) tagging requirement (`sec-edgar.md` §2.2; the proxy's Item 402(v) pay-versus-performance tagging does not reach it).
  Cell-to-caption geometry uses the HTML table model as `_layout_rows` implements it
  (slice 3, § Source rule).
- `sec-edgar.md` §3.6: entity-level content is PK=accession; per-instrument tables fan
  out. Item 403 lines are entity-level until bound to a class (M2).
- **No published rule** maps proxy class wording to a registered class. M2 fixes it by
  construction and freezes it in a rule version.

## Full-population measurement (dev, `3ac392ae`)

`PYTHONPATH=. uv run python -m scripts.census_2351_class_grain --out var/census_2351/grain.jsonl`,
then `--summarise` and `--read-class`. Runs the parser's own table selection
(`item403_table_htmls`) over every stored `def14a_body`, classifying each selected table on
`_layout_rows`:

| | all bodies | bodies with stored rows |
| --- | ---: | ---: |
| bodies | 43,291 (0 errors) | 7,313 |
| a multi-class shape detected | 808 | **800** (427 CIKs) |
| … H (share columns under different class captions) | 645 | 641 |
| … V (*Title of class* column) | 90 | 86 |
| … S (class-naming row with no share cell) | 98 | 97 |
| … CIK with ≤1 instrument in `external_identifiers` | 760 | **752** |
| … CIK with >1 instrument | 48 | 48 |

Shapes overlap, among the 800 stored-row bodies: `H+S` 7, `S+V` 15, `H+V` 2. "No shape detected" is NOT "single-class":
it includes layouts the detector misses. Bodies without stored rows carry no CIK, so the
all-bodies sibling split is a floor. The census is a detector with known false positives
and negatives (round 1, findings 72–77, 83); it sizes the problem and is not an
acceptance oracle.

On the 752 bodies, the stored rows' **matched** column (`share_locations` by name key and
equal count — not proven parser provenance) labels: `A` 7,133 rows, unbound 3,377, `B`
768, row-cell `A` 298, row-cell `B` 72, other letters 76. By body: 176 `A+B`, 20
`A+B+unbound`, 6 `A+B+C`, 14 row-cell `A+B`. Whether a `B` read is wrong depends on the
instrument's class, which this census does not resolve (M2 does).

Example — META `0001326801-25-000040`: `Class A {Shares, %} | Class B {Shares, %} | % of
Total Voting Power`. Stored for Zuckerberg: 141,000 (Class A). His Class B line,
342,606,985 / 99.8%, has nowhere to go.

## M1 — class-grain extractor (pure; no schema, no writer, no reader change)

`app/services/def14a_item403_lines.py::item403_lines(html_text) -> Item403Extraction` (`lines`, `errors`). Legacy
`parse_beneficial_ownership_table` and its `rows` are **not modified**; M1 only calls it.
Acceptance 0 below proves the legacy output is unchanged. Empty input → no lines; a
per-table exception is caught, recorded in `errors` with the table ordinal, and that
table contributes no lines (the parser's own best-effort contract).

M1 **interprets geometry** (which caption is over which cell, which row is whose). It
does **not** decide which class a line is, which amount is the total, or which percent is
percent-of-class: it exposes the evidence and a coarse `class_state`. Every rule below
fails toward `unlabelled` / `conflict` / no line — never toward a confident wrong label.

**Tables and order.** `_select_item403_tables`, iterated exactly as
`parse_beneficial_ownership_table` iterates (descending `_score_table_headers`, stable).
`table_ordinal` is that position; acceptance 0 asserts it.

**Holder rows.** Accepted holders = the parser's `rows` for the same body (its non-owner
filters, stacked-name split, ESOP detection and roles stay authoritative). On a table's
`_layout_rows` grid, a row's *owner cell* is its leftmost non-empty cell that is not an
amount cell. The row belongs to holder h iff the owner cell's `_layout_name_key` is
non-empty and equals h's key. If two accepted holders of the accession share a key, rows
with that key are `ambiguous_holder` and yield no line (the key is a 16-character
prefix). A row whose other cells match a different accepted holder's key is also skipped
(side-by-side blocks).

**Header block.** Slice 3's boundary: rows above the table's first row carrying a share
cell. A column's *captions* = its distinct non-numeric header-block texts, excluding any
text that also sits above the owner cell's column (a spanning title). *Interior* texts =
the column's non-numeric texts between the header block and the row (slice 3's
definition), so a mid-table re-header is evidence, not lost.

**Amount cells.** A cell is an amount cell iff its text parses as a share count
(`_is_share_cell`), or as a percent (`_parse_percent`) with a `%` sign or under a
`_is_percent_caption` caption, or is a marker (`*`, `—`, `-`, `N/A`) in a column where
some other row of the table holds a share or percent cell. Contiguous layout slots of one
row holding the identical text under identical captions are ONE cell (an expanded
`colspan`). Each amount cell carries `(first_column, captions, interior, raw_text,
shares | None, percent | None)`; `raw_text` is `_layout_rows`' stripped text, so a `*` or
dash is never converted here.

**Class evidence.** `class_evidence(texts)` → a frozen set of tokens:
`(keyword, letter)` pairs from `keyed_designators`, plus the normalised security kinds
present (`common` for common/ordinary/capital stock, `preferred` for
preferred/preference, `warrant`, `unit`, `right`, `note`). So Class A common and Class A
preferred are different evidence. ⚠ `keyed_designators("Class A and Series B")` returns
`class:B` for the second item (round 2 #27, confirmed). M1 uses a corrected variant in
which a keyword inside a list switches the keyword for the letters after it. The shipped
helper is left as is: slices 3/3b are rule-versioned ledgers and changing it would move
them silently; M2 retires them.

**Class groups.** A row's amount cells group by the class evidence of their
`captions ∪ interior`. Cells with empty evidence form the row's unlabelled group (META's
voting-power column). One line per (holder row, group).

**Section labels.** A row with no amount cell whose texts' class evidence is non-empty
and names at most one designator is a label; a later label replaces an earlier one, a
table boundary resets, and a label naming two or more designators (a table-wide
`Class A and Class B` title) is ignored rather than applied (round 2 #38–39). A bare
`Common Stock` label counts (evidence `{common}`) and so resets an earlier `Class B`.

**Continuation rows (V).** A row whose owner cell is empty or is a *Title of class*
cell, that has ≥1 amount cell and a non-empty *Title of class* cell, directly below a
holder row or its continuation in the same table (only fully empty rows between), is
that holder's line with its own `grid_row`, class cell and amounts. A row with any
non-empty owner-cell name key that matches no accepted holder is not a continuation.

**Line fields.** `holder_name`, `holder_role` (the parser's), `table_ordinal`,
`grid_row`, `continuation` (bool), `row_class_cells` (texts in *Title of class* columns),
`section_label` (text), `group_evidence`, `amount_cells`, and `class_state`:
`labelled` iff the union of the class evidence of row class cells, section label and the
group is non-empty and names at most one designator and at most one security kind;
`conflict` if it names more; `unlabelled` if empty.

**No de-dup.** Every located line is returned. Collapsing repeats (breakdown tables,
repeated holders across tables) is M2's rule.

### M1 acceptance (full population, all 43,291 bodies, offline)

`scripts/census_2351_class_grain.py --lines --out …` writes one JSON record per body and
exits non-zero on any failed assertion; `--lines-summary` prints the tallies below.

0. **Legacy untouched:** `scripts/ab_2140_def14a_parser.py` main vs branch → 0 holders
   lost, 0 gained, and identical values; `table_ordinal` equals the parser's iteration
   order on every body.
1. **Coverage:** every parser row with non-NULL shares has ≥1 line of that holder with an
   amount cell equal to those shares. Misses listed per accession/holder. Gate: every
   miss is either `ambiguous_holder` or listed and read in the PR.
2. **Precision on single-shape bodies:** on bodies the census shows no shape for, each
   holder's lines' share cells include the parser's shares, and no line has
   `class_state = conflict`. Exceptions listed.
3. **One owner per cell:** no amount cell (table, row, first_column) is attached to two
   holders. Asserted.
4. **Distribution:** `class_state` by census shape; every `conflict` line listed by
   accession; count of holders with ≥2 labelled groups (recovered lines) by shape.
5. **Named recoveries, exact:** META `0001326801-25-000040` Zuckerberg → a `class:A`
   group with 141,000 and a `class:B` group with 342,606,985 (and 99.8), plus the
   unlabelled voting group; LEN `0001193125-26-073504` Miller → `Class B Common Stock`
   21,851,560 / 70.2%; GOOGL 2026 (accession resolved in the PR) Page → a `class:B` group
   389,051,160. Each read against EDGAR.
6. **Runtime** per body (median, p99, max) for M2's sizing.

Label correctness is not provable full-population in M1 (no oracle). M2's gate —
reproducing the four shipped suppression ledgers, each derived independently — is the
full-population check on labels.

Tests (pure, `tests/test_def14a_item403_lines.py`): META-shaped H with voting column;
V with a continuation; S label, reset by the next label and by `Common Stock`; table-wide
two-class title ignored; owner-cell join through a footnoted/addressed cell; two
accepted holders sharing a key → no line; expanded colspan counted once; `*` kept raw;
row class cell A under a Class B caption → conflict; Class A preferred ≠ Class A common;
`keyed_designators("Class A and Series B")`.

### Accepted limits (round 2, 86 findings, classified by the author)

Fixed above: 1 (mandate wording), 9, 10, 13–16 (owner cell, ambiguous key, side-by-side),
17 (ordinal assertion), 18 (colspan), 23 (interior), 27 (helper bug), 29–31, 34, 36–37
(evidence tokens), 38–39 (labels), 44–46, 50 (amount cells), 52–54, 57 (continuation),
58–59 (group key), 60 (errors), 61–67, 69–71, 74–75 (acceptance). Fail-to-`unlabelled`,
accepted: 24, 25, 28, 40, 41 (context outside the table, unusual class spellings, nested
labels). Inherited from shipped helpers, accepted as in slices 3/3b: 5–8, 19–22 (the
`_layout_rows` model and header boundary). Carried to M2 (interpretation): 2, 3
(footnotes and the `*` legend), 26, 32–33, 35, 43, 47–49, 51, 55–56, 68 (label
correctness — M2's ledger gate). Census is a sizing tool, not an oracle: 72–73, 76–79,
81–86 accepted; 80 fixed in § measurement.

### M1 result (dev, full population, run 5)

`PYTHONPATH=. uv run python -m scripts.census_2351_class_grain --lines --out var/census_2351/lines5.jsonl`
then `--lines-summary var/census_2351/lines5.jsonl` (exit 0):

| check | result |
| --- | --- |
| bodies / unique / errors / table errors | 43,291 / 43,291 / 0 / 0 |
| 0 — legacy parser | untouched by construction: the branch adds files only, no line of `sec_def14a.py` changes |
| 1 — coverage misses | **516 of 110,415 parser rows (0.47%), 107 bodies**; 14 bodies miss every row (159 rows) |
| 3 — cells attached to two holders | **0** |
| 4 — no-shape bodies | 87,266 unlabelled · 14,503 labelled · **81 conflict** · 37 holders with ≥2 labelled groups |
| 4 — multi-class bodies | 11,311 unlabelled · 22,204 labelled · 587 conflict · **2,632 holders with ≥2 labelled groups** |
| 5 — META / LEN / GOOGL | Zuckerberg `class:B` 342,606,985 / 99.8; Miller `Class B Common Stock` 21,851,560 / 70.2%; Page `class:B` 389,051,160 / 46.5 (his `class:A` cells are `—`: the legacy row's 389,051,160 on GOOGL is this Class B cell) |
| 6 — runtime per body | median 2.0 ms, p99 603 ms, max 11.3 s |

Read from the stored EDGAR primary documents. **Deviation from gate 1:** the 516 misses were
not each read. Five runs moved them 6,742 → 1,863 → 772 → 516 → 516 by fixing mechanisms found in
random samples (owner cell on a zero-width cell, full-name key, `Title of class` column left
of the name, name row with figures on the row below, zero-width space inside an amount).
The remaining sampled one is a caption/column misalignment that parses a share count as a
percent. A miss means that holder has no line, so M2 must treat "no line" as "legacy row,
binding abstained" — never as absence. The list is `var/census_2351/misses5.tsv`.

**Deviation from gate 2:** 81 `conflict` lines remain on bodies the census shows no shape
for. The census detector misses layouts (see § measurement), so these are not all
extractor errors; they are listed per body in the run output for M2.

Amendments measured into the rule above during implementation:
- holder join uses the WHOLE cleaned name (longest-prefix of the owner cell), not the
  16-character `_layout_name_key`, which collides on `Entities affiliated with …`;
- one person emitted by the parser under two spellings (dot leaders) → the first name;
- owner cell = leftmost cell with a letter or digit that is not in a *Title of class* column;
- header block ends at the first share cell OR the first accepted holder's name row;
- a holder's name row without amounts hands its figures to the next amount row (≤3 address
  rows between);
- in an amount caption only designators and `preferred` are class evidence; warrant / unit /
  right words describe the nature of ownership (Rule 13d-3(d)(1)(i)); `common` never splits
  a shares column from its percent column;
- (ckpt-2) interior evidence is read only below the active section label, so a replaced
  label stops counting; a %-signed percent also opens the table body (percent-only tables);
  the census counts recovered groups on normalised evidence including section labels.

## M2 — binding (pure, measured; outline, its own spec + ckpt-1)

Input: M1 lines + the point-in-time cover (slice 2's `resolve_cover`) + siblings. Output
per (instrument, accession, holder): the figures that instrument receives, or an explicit
`abstained` / `other_class` with evidence. Must reproduce every shipped suppression
(slices 2, 2b, 3, 3b ledgers) — gate on projected exposure, not "no line of the class" —
and list every changed decision on multi- and single-instrument issuers alike. Carried
from round 1 and to be answered there: #2–4, #7, #8, #34–48, #50–51, #59–62, #81.

## M3 — storage + readers (outline, its own spec + ckpt-1)

Entity-level line store (normalised key column, not an expression PK — round 1 #12),
bindings table, per-instrument projection, writer integration across the three writers,
offline backfill with freshness checks, reader switch with a full-population value-level
A/B (shares, percent, role, date, class — not holder sets; #67, #70), DoD 8–12, then
ledger retirement after a frozen baseline. Carried: #5, #6, #13–15, #49, #52–58, #63–66,
#71.

## Security

No auth surface. M1 makes no DB write and no outbound fetch.
