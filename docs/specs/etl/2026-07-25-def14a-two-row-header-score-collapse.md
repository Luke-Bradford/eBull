# DEF 14A: a promoted column-label row must still score the table

Issue: #2158 (second finding). Parser: `app/providers/implementations/sec_def14a.py`.
Follow-up to #2140, whose D2 fix introduced the collapse this spec repairs.

## Source rule

**Reg S-K Item 403** (17 CFR § 229.403), incorporated into a DEF 14A by
**Schedule 14A Item 6(d)** (17 CFR § 240.14a-101), prescribes the table's
captions:

- 403(a) name column — **"Name and address of beneficial owner"**
- 403(b) name column — **"Name of beneficial owner"**
- both amount columns — **"Amount and nature of beneficial ownership"**
- both percent columns — **"Percent of class"**

Item 403(a) requires the disclosure "**of any class** of the registrant's voting
securities", and 403(b) requires it "**of each class**". Item 403 prescribes the
captions; it prescribes **no layout**, and neither Schedule 14A nor any EDGAR
rendering rule does. So the two-row shape below is an **observed issuer
convention**, not a cited rule: a registrant disclosing more than one class
*may* render a spanning share-class row above the caption row — `('Common
Stock', 'Series A Preferred Stock')`, `('', 'AEP Shares')` — and repeat the
prescribed captions underneath, once per class. Full-population frequency of the
shape is reported in §Full-population verification rather than assumed.

**The rule this fixes is the caption rule, not a layout rule:** Item 403's
prescribed captions identify the table, and wherever the issuer puts them, that
is the row a scorer deciding "is this the Item 403 table?" must read. The
share-class row carries none of the prescribed captions — it carries the
issuer's security names, which Item 403 does not prescribe.

## Premise check (the ticket's split is wrong)

Issue #2158 reports the guard-blocked set as **43 correct rejections + 138 pre-existing
coverage gap**. Re-derived against the live DB at `0fdb92f3`, the split does not
hold:

- The blocked set is **181 accessions** — `filing_raw_documents` rows of kind
  `def14a_body` still below `def14a-v8` that have typed rows in
  `def14a_beneficial_holdings` (152 at v7 + 29 at v6). That count matches the
  guard's fire count exactly.
- **180 of 181 return zero rows** under `def14a-v8`.
- The stored rows are **not** cleanly "43 garbage / 138 real". Both buckets are
  mixed: `0000950170-24-037190` stores five dates, `0000950170-25-076708` stores
  the single string `'10,759,078'`, `0000950170-25-048978` stores two bare
  addresses — while `0000004904-25-000043` (score 2) and `0000816956-26-000047`
  (score 4) store real Item 403 holders. **166 of the 181 hold at least one
  real-looking holder name; 1,798 stored names in total.**

The ticket's stated *cause* is also wrong. It attributes the zero rows to
"table selection / score floor on these older layouts". The filings are not
old — they are 2025/2026 proxies — and the floor is not the problem.

## Defect (D14) — label-row promotion strands `score_headers`

`_parse_table_html` (`sec_def14a.py:556`) detects a two-row header and promotes
row 1 to `column_headers`. It has two arms:

```python
legacy_arm = len(parent_headers) < max_data_width and _looks_like_legacy_subheader(body[0])
label_arm  = len(parent_headers) <= max_data_width and _looks_like_label_row(body[0])
if legacy_arm or label_arm:
    column_headers = body[0]
    score_headers = parent_headers + body[0] if legacy_arm else parent_headers   # <-- D14
    body = body[1:]
```

The **legacy** arm (Sole/Shared/Total under a merged "Amount and Nature" cell)
folds both rows into `score_headers`. The **label** arm — added by #2140 D2 for
exactly the spanning-share-class shape above — does not. So when row 0 is the
share-class row, the table is scored on `('Common Stock', 'Series A Preferred
Stock')` and Item 403's prescribed captions, sitting one row below in
`column_headers`, are **never scored at all**.

Measured on two of the blocked filings:

| Accession | `score_headers` (scored) | `column_headers` (ignored by scorer) | score | score if the caption row counted |
| --- | --- | --- | --- | --- |
| `0000908311-26-000065` | `('Common Stock', 'Series A Preferred Stock')` | `('Name of Beneficial Owner', 'No. of Shares', 'Percent of Class', 'No. of Shares', 'Percent of Class')` | **0** | **13** |
| `0000004904-25-000043` | `('', 'AEP Shares')` | `('Name and Address of Beneficial Owner', 'Amount of Beneficial Ownership', '', 'Percent of Class (a)')` | **1** | **11** |

Both are textbook Item 403 tables carrying the prescribed captions verbatim, and
both score below the floor of 3.

The collapse has two distinct consequences, which is why the blocked set splits
into two buckets that look unrelated:

- **58 accessions — below floor.** The real table scores 0-2, no window
  qualifies, `parse_beneficial_ownership_table` returns empty.
- **123 accessions — above floor, zero rows.** The real table still qualifies
  but its collapsed score is no longer the window best, so the sibling gate
  `sc >= _SIBLING_SCORE_FLOOR or sc == window_best_score` (`:1163`) **drops it**
  and keeps whichever junk table won. The junk that wins is invariably a layout
  `<table>` whose "header" is a paragraph: `0000898432-21-000355` is beaten by
  its own beneficial-ownership *footnote* (score 8) and
  `0000950170-25-058041` by a "Voting Instructions" prose block (score 5).

## Design

### 1. Fold the promoted row into `score_headers` in **both** arms

```python
score_headers = parent_headers + body[0]
```

### 2. Add Item 402(g)'s prescribed captions to the award disqualifier

Element 1 creates this hazard, so it is fixed in the same change.

Folding the label row makes a class of Item 402 table **visible to the scorer
for the first time**. The **Item 402(g) "Option Exercises and Stock Vested"**
table renders its column labels under a spanning `('Option Awards', 'Stock
Awards')` row, so unfolded it scored **0**; folded it scores **5** on `name` +
`number of shares` + `shares`, clearing the floor of 3. `_ITEM_402_AWARD_MARKERS`
carried no 402(g) caption, so nothing disqualified it.

This is not hypothetical. Measured on the guard-blocked set, the fold alone
"recovered" **5 accessions whose recovered rows were vesting data, not
ownership** — `0001193125-26-103020` read `('NAME', 'NUMBER OF SHARES ACQUIRED
ON VESTING (#)', 'VALUE REALIZED ON VESTING ($)')` and emitted five executives
as beneficial owners. The full-population promotion audit found the class in
**47 of the first 125 promoted header shapes**.

**Source rule:** Reg S-K **Item 402(g)(2)** (17 CFR § 229.402(g)(2)) prescribes
the captions "Number of Shares Acquired on Exercise", "Value Realized on
Exercise", "Number of Shares Acquired on Vesting", "Value Realized on Vesting".
Markers added: `acquired on exercise`, `acquired on vesting`, `value realized`.

**No Item 403 collision:** Item 403 reports a **holding as of a date**, never an
exercise or vesting **event**, so it has no occasion to caption a column with
either phrase. `acquired on exercise` is the safe form where a bare `exercise`
would not be — the existing note on `exercise price` records why (a real Item
403 table can carry an "Exercisable Stock Options" column).

### 3. A Rule 13d-3 "within 60 days" caption is a label, not data

`_looks_like_label_row` rejected any cell containing a 2+ digit run, on the
theory that a digit-bearing row is a data row. **Source rule:** Rule
**13d-3(d)(1)(i)** (17 CFR § 240.13d-3(d)(1)(i)) deems a person the beneficial
owner of securities they have the right to acquire "**within sixty days**", so
Item 403 tables caption a column `Company Stock Options Exercisable within 60
days of April 1, 2025` — the one numeric literal these captions legitimately
carry. The guard rejected that caption row, so `0001437749-25-011586`'s Item
403(b) table kept scoring **4** on its spanning title `BENEFICIAL OWNERSHIP OF
COMPANY STOCK` alone. Once element 1 lifted the *other* Item 403 table to 15,
the sibling gate (`sc >= 6 or sc == window_best_score`) dropped the whole
403(b) management subsection — 13 holders.

A **data** cell is a numeric **value**; a **label** cell is prose that may
mention a number. Test that instead. Footnote markers are stripped first with
the module's existing `_FOOTNOTE_RE`: without the strip `'1,000,000 [2]'` reads
as prose, and a data row whose holder is literally named "… Holder" promotes
over its own table — caught by two pre-existing tests. The legacy arm keeps
`_NUMERIC_LIKE_RE` verbatim.

### 4. A table with no data rows cannot win selection

Element 3 creates this hazard, so it is fixed in the same change.

`_score_table_headers` reads headers only. Proxies are full of layout `<table>`
blocks holding a single prose paragraph, and once a digit-bearing cell stopped
counting as data those paragraphs began promoting as "label rows" and scoring
6-11 — beating the real Item 403 table and taking the window while having no
holders to report at all. A voting-methods block beat the 5%-holder table on
`0000936468-25-000015` (Vanguard / BlackRock / State Street, 3 → 0); an
advance-notice-bylaw paragraph beat it on `0001104659-26-036909` (19 → 0). Both
winners had **zero** data rows.

Skip zero-row tables at candidate time. This is independently correct — a table
with no rows can only ever displace one that has them — and it clears every
regression element 3 introduced (58 holders across 6 accessions, all real
names).

### Why the #2140 counter-example no longer applies

The label arm's non-fold was deliberate. Its stated reason (`:640-647`) is that a
generic label row `('Name', 'Grant Date', 'Number of securities underlying
unexercised options…')` belongs to the Item 402(f) Outstanding Equity Awards
table just as readily as to Item 403, so folding it lifted that table to a tie
and it won on document order (`0001628280-25-020660`, 20 holders → 0).

That reasoning was correct **when it was written** and is now inverted by
the #2140 fix that landed after it. `_ITEM_402_AWARD_MARKERS` (`:382`) disqualifies a table
outright — `_score_table_headers` returns 0 — when its joined headers contain an
Item 402 award caption, and `unexercised` is one of them. Folding
the label row into `score_headers` therefore **feeds the disqualifier the very
text that identifies the award table**: the cited counter-example scores 0 after
the fold, not a tie. The fold makes the Item 402 rejection *stronger*, not
weaker.

`_looks_like_label_row`'s own guards are unchanged and remain the thing that
stops a false promotion: word-boundary matching, a **required** name class, ≥2
classes from **different** cells, and no multi-digit cell. CYH's PSU row
(`'% of Target Achieved'`, `'% of Granted Shares Earned'`, `''`,
`'Percentile Rank'`) carries no name-class token and is still not promoted, so
the fold never sees it.

### Blast radius: Item 402(c) SCT selection shares this helper

`parse_summary_compensation_table` (`:2193`) also calls `_parse_table_html` and
also selects on `score_headers` (`_score_sct_headers`, floor 6). This change
therefore moves SCT *table selection* too — the same shared-helper trap #2140 hit
when `_clean_holder_name` rewrote SCT output. The SCT fingerprint arm of the A/B
is the proof obligation, not an assumption; see below.

### Explicitly NOT in scope

**The guard signal (ticket option 1 — let the apply-fn declare "intentionally
empty").** After this change only **26** accessions stay blocked, and they are
irreducibly mixed: `0001193125-25-058955` and `0001193125-26-116042` each hold
**12 real Item 403 directors**, `0001048268-26-000045` and
`0001826470-26-000040` hold real names, while `0000950170-25-048615`,
`0001193125-26-103020` and `0001140361-25-012045` hold Item 402(g) vesting rows
that *should* be superseded. The signal the option
needs would have to separate those 11 from the 10 that hold only garbage — and
it cannot: both sub-sets present identically to the parser as *a table qualified
and every row was dropped* (`0001193125-25-058955` scores 6 with 14 real stored
holders; `0000799233-25-000020` scores 14 with one stored address fragment).
Shipping the signal would delete the 11. The guard is correct for all 21 and
stays. Residual tracked separately.

## Full-population verification

Row count is **not** the metric — #2140 twice found a drop to be the old code
losing garbage or the identity dedup working. The metric is **distinct holders
lost**, and gains are enumerated because #2140's last real defect (address
fragments) appeared only on the gain side.

`scripts/ab_2140_def14a_parser.py` re-parses all **42,505** stored
`def14a_body` payloads offline (no EDGAR fetch), `main` in a worktree vs branch,
and diffs. Extended for this ticket with per-accession holder-identity sets
(keyed `lower(trim(holder_name))`, matching `holder_name_key` at `sql/116:110`)
and a `--stored` arm.

**The `--stored` arm exists because the A/B has a blind spot that is the whole
point of #2158**: it compares parse-to-parse, so a filing where BOTH sides
return nothing is invisible to it. Those filings are precisely the ones the
rewash guard blocks, and their stored rows survive only because nothing has
force-rewashed them. Only a comparison against `def14a_beneficial_holdings`
sees them.

Three arms, each closing a gap the others cannot (all three raised at Codex
ckpt-1):

**Arm 1 — `--diff`, parse-to-parse.** Distinct holders lost and gained,
enumerated per accession.

**Arm 2 — `--stored MAIN BRANCH`, parse-to-stored.** The A/B compares
parse-to-parse and is therefore blind to filings where BOTH sides return
nothing — exactly the class the rewash guard blocks, whose stored rows survive
only because nothing has force-rewashed them. Passing both summaries lets this
arm state the regression invariant the single-summary form cannot: **stored
holders `main` reproduced and the branch does not**. "Real" is decided by the
parser's own `_looks_like_name_cell` — the invariant #2140 §3 settled, *a holder
name must carry name evidence* — not by a classifier written for this diff.

**Arm 3 — `--audit`, both scoring modes in one process.** Two things arms 1-2
cannot establish:

- **Every** label-arm promotion changes `score_headers`, not only newly-promoted
  shapes, so a "new shapes" audit measures the wrong set. This enumerates each
  promoted shape with unfolded score, folded score, whether the fold trips
  `_ITEM_402_AWARD_MARKERS`, and — the hazard that actually costs holders —
  whether it **newly clears the floor**. This arm is what found the Item 402(g)
  class in Design §2.
- A compressed SCT fingerprint can match across a **table swap**. This arm
  compares the **full** emitted Item 402(c) rows plus the selected table's score
  under both modes, so a swap that coincidentally preserves
  `(name, position, year, total)` still shows up.

### Result (round 3, all four elements, 42,505 bodies)

| | main | branch |
| --- | --- | --- |
| accessions yielding rows | 6,449 | **7,144** |
| rows | 93,347 | 104,607 |
| distinct holders **gained** | — | **11,652** |
| group rows (all correctly tagged `group`) | 5,563 | 6,261 |
| Item 402(c) SCT rows | 66,445 | **67,442** |
| numeric / newline holder names | 0 | 0 |

**Item 402(c) SCT: 84 accessions drift, all GAINING, zero losing.** Each went
from **0 rows to 12-18 real NEO rows** — the same stranded-caption defect, and
the fold repairs Item 402(c) selection as a side effect (`0000036966-25-000026`:
0 → 15, `D.B. Jordan ~ Chairman, President & CEO ~ 2024 ~ 9,429,980`). The
blast-radius arm is therefore not "unchanged" but "strictly better", which is
the claim the full-row comparison had to establish rather than assume.

**Losses, enumerated.** 79 accessions lose at least one distinct holder:

- **58 are table SWAPS** — lose 240, gain 740 in the same accession. The parser
  stops reading an Item 402 award table or a "New Plan Benefits" table and
  starts reading the real Item 403 one.
- **21 are pure losses, 152 holders.** Of these, **51 across 8 accessions are
  garbage `main` should never have emitted**: table-of-contents entries
  (`'additional information'`, `'changing your vote'` —
  `0000075288-26-000032`), Schedule 13G footnote paragraphs
  (`0001308179-25-000518`), Item 201(d) equity-compensation-plan rows
  (`'equity compensation plans approved by security holders'`,
  `0001506307-25-000017`), reverse-split ratios (`'post-reverse stock split
  1:10'`), award types (`'eip—psu'`) and `'target'` / `'threshold'`.
- **The remaining ~101 holders across 13 accessions are real Item 403 holders
  and are a genuine regression** — `0000950170-25-045737` (12, incl. BlackRock),
  `0001628280-26-044960` (14), `0001294133-26-000009` (11),
  `0001831631-25-000069` (8, the Parthenon funds).

**Mechanism of the residual, and why it is not fixed here.** It is one class:
raising the winning table's score widens its gap over a genuine *sibling* Item
403 table, which then fails the sibling gate `sc >= _SIBLING_SCORE_FLOOR or
sc == window_best_score` — the tie arm used to carry it. Element 3 fixes the
subset where the sibling's own caption row was being rejected; the rest need the
sibling gate itself re-derived, which is a scoring-model change requiring its own
full-population round. Tracked in **#2160** with this mechanism recorded.

Shipping is still correct on the measured trade: **+11,652 gained against ~101
real holders lost, 115:1**, every loss enumerated above rather than averaged
away, and the guard continues to protect the residual cohort.

Acceptance, as measured:

- distinct holders lost: enumerated and classified — 51 garbage, ~101 real, all
  from one named mechanism with a follow-up ticket. **Not zero; disclosed.**
- distinct holders gained: enumerated, inspected for the D12 address class and
  Item 402 leakage.
- Item 402(c) SCT: full-row + selected-table-score drift — 84 accessions, **all
  gains, zero losses**.
- promotion audit: full-population, both scoring modes in one process.
- guard-blocked set: **181 → 26**, residual classified (#2160).

### Blocked-set result (measured at `0fdb92f3` + this change)

| | before | fold only | fold + 402(g) markers |
| --- | --- | --- | --- |
| blocked accessions returning zero rows | 180 / 181 | 21 / 181 | **26 / 181** |
| stored holder names reproduced | 0 | 1,739 | — |
| stored names not reproduced | 1,773 | 34 | — |
| new holders recovered | — | 531 | — |

The five accessions the markers give back were **false** recoveries — Item
402(g) vesting rows (`0000950170-25-048615`, `0001140361-25-012045`,
`0001193125-26-103020`, `0001193125-26-138065`, `0001576427-24-000058`), each
verified by dumping the selected table's captions. Losing them is the point of
Design §2.

Of the 34 stored names not reproduced under fold-only: dates (`03/20/2024`),
plan labels (`espp`, `2013 Plan`, `option`), bare addresses, and share-ratio
strings (`5:1`). These are dropped because they fail `_looks_like_name_cell`,
the settled #2140 §3 invariant — not because this ticket judged them garbage.
Full-population counts by class, and the one person-name residual
(`b. buckler`, `0000950170-25-048615` — itself a 402(g) filing) plus two
curly-vs-straight apostrophe pairs, are enumerated in the PR.

### Backfill (ETL clause 10)

`PYTHONPATH=. uv run python scripts/rewash.py --kind def14a_body` — offline
re-parse of all stored bodies, no rate-limited EDGAR drain. `--dry-run` first.
Parser version bumps `def14a-v8` → `def14a-v9`.

### Operator-visible (ETL clauses 8, 9, 11)

- Panel: the accessions that carry the defect — **`0000908311-26-000065`
  (CIM/creative-media)**, **`0000004904-25-000043` (AEP)**,
  **`0000816956-26-000047`**, **`0000950170-25-058041` (HubSpot)**,
  **`0000898432-21-000355`** — plus `AAPL` / `GME` / `MSFT` from the default
  panel as a no-change control.
- Endpoint: `/instruments/{symbol}/ownership-rollup` after backfill.
- Cross-source: American Electric Power's DEF 14A, accession
  **`0000004904-25-000043`**, filing index
  `https://www.sec.gov/Archives/edgar/data/4904/000000490425000043/` — the Item
  403(a) 5%-holder rows read directly from the primary document
  (`filing_raw_documents.source_url` for this accession is the exact document
  the parser reads, so the comparison is against SEC's own rendering of the same
  bytes): Vanguard 49,224,906 / 9.22%; BlackRock 44,982,057 / 8.43%; State
  Street 28,190,434 / 5.28%.
