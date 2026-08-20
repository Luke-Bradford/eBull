# DEF 14A Item 403 — row-identity table selection

Issue: #2160 (guard-blocked zero-row residual) and #2163 (junk rows stored as
beneficial ownership). One change, both symptoms.

Supersedes the two levers proposed on #2160 (caption-shape guard; lower the
sibling score floor) — both falsified by the full-population census below.

## Source rule

**Schedule 14A Item 6(d)** (17 CFR 240.14a-101) is the rule that brings Item 403
into a proxy statement — it requires the registrant to furnish the information
called for by Item 403 of Regulation S-K. That is the hook; 229.403 is the
substance.

**Rule 13d-3** (17 CFR 240.13d-3) defines "beneficial owner" — any person with
voting or investment power, directly or indirectly. This is load-bearing for D1:
the row identity the reg constrains is a *person or entity holding voting or
investment power*, which is why a natural-person or legal-entity name is the
correct positive test and an accounting line-item is not.

**17 CFR 229.403** (Reg S-K Item 403) prescribes the table for both subsections:

| # | 403(a) — >5% owners | 403(b) — management + group |
|---|---|---|
| 1 | Title of class | Title of class |
| 2 | Name **and address** of beneficial owner | Name of beneficial owner |
| 3 | Amount and nature of beneficial ownership | Amount and nature of beneficial ownership |
| 4 | Percent of class | Percent of class |

Both are required "substantially in the tabular form indicated". The reg does
not prohibit combining them, and issuers routinely render them as two tables
(the reason `_SIBLING_SCORE_FLOOR` exists at all — #2140 D7).

**Instruction 5** to Item 403: *"in computing the aggregate number of shares
owned by directors and officers of the registrant as a group, the same shares
shall not be counted more than once"* — establishes the
directors-and-officers-as-a-group aggregate row, which only a 403(b) table has.

The load-bearing point for this spec: **the reg constrains the ROW IDENTITY, not
just the column captions.** Column 2 of both subsections is a *beneficial
owner* — a natural person or a legal entity. A table whose rows are `Revenue`,
`Authorized for issuance` or `50 th` is not an Item 403 table irrespective of
what its header says.

## Full-population verification

Census of all **42,566** stored `def14a_body` payloads, re-parsed on `main`
`73f721ef`, tracing every table `_score_table_headers` scored. Scripts and the
42,566-line JSONL are reproducible offline from `filing_raw_documents` with no
refetch.

### Both #2160 levers are falsified

`max_cell_len` of the winning table, by caption class:

```text
class                      n    p50    p90    p99     max
A_full_prescribed      10628     41     57     84     165
B_abbreviated           4456     40     64    922    3200
C_neither                608     37     88    753    1601
```

Junk headers are *shorter* at the median than genuine prescribed captions
(37 vs 41). No length threshold separates them — the caption-shape guard cannot
work.

Sentence punctuation: at a 1,740-accession partial run this read **0 of 660**
full-prescribed winners, which looked like a clean gate. Full population:
**198 of 10,628** (1.9%) against 80 of 608 (13.2%) for `C_neither`. Directional,
not a gate. (Recorded because it is the #1659 pattern — a partial that reads
clean is the dangerous one.)

### The premise about *what* wins is wrong

Issue #2160 assumes the competing tables are prose — table-of-contents entries,
Schedule 13G footnote paragraphs. Full population says they are overwhelmingly
**other legitimate tables that talk about shares**:

```text
'Revenue', 'Excise taxes', 'Net revenue'                    income statement
'Authorized for issuance', 'Issued and outstanding'         capitalisation table
'Total number of shares remaining available for future
 grants under the 2016 Long-Term Incentive Plan'            equity plan pool
'PSU shares earned', 'rTSRU shares earned'                  comp metrics
'Threshold', 'Target'                                       comp payout curve
'50 th Percentile', '25 th Percentile'                      TSR percentiles
'Auto', 'Marine'                                            segment table
'None', '1-for-5', '1-for'                                  reverse-split ratios
```

Every one has a header that *legitimately* mentions shares or percentages.
No header-only test — score-sum, caption match, or shape guard — can reject
them. This is why the selector has now needed four consecutive per-case patches
(#2140 D7, #2158 element 4, #2157, #2160).

### Row identity does separate

Fraction of a winning table's rows whose first cell is a beneficial-owner
identity (person name, entity designator, or Instruction 5's group row):

```text
bucket    accessions    cum%
   0-%           46     0.64
  10-%            3     0.68
  20-%           17     0.92
  30-%           11     1.08
  40-%            4     1.13
  50-%           12     1.30
  ...
 100-%         5856   100.00
```

81.8% of accessions sit at 100%. **61 accessions (0.85%) fall below 0.5.**

All 61 were classified by hand, not sampled:

- **~54 are genuine junk** — the classes listed above.
- **~6 are genuine holders the identity test wrongly rejects**, and they
  constrain the design:

```text
'MUFG 4-5, Marunouchi 1-chome Chiyoda-ku, Tokyo'   all-caps entity, no LLC/Inc token
'State Street 1 Congress Street, Boston, MA'        entity + 403(a) address in-cell
'*Stephen J. Bagley'                                leading footnote marker
```

## Design

### D0 — What the fraction is computed ON

**Not the raw first cell.** Item 403's prescribed column 1 is `Title of class`;
the beneficial owner is column 2. A genuine table rendering
`Common Stock | The Vanguard Group | 5,799,197 | 5.3%` would fail a first-cell
test outright. (Codex ckpt-1 BLOCKING.)

The fraction is computed on the **resolved name column**, reached through the
same path extraction uses:

```python
name_idx, shares_idx, percent_idx = _resolve_columns(table.column_headers)
_extract_holder_rows(table, name_idx=..., shares_idx=..., percent_idx=..., rows=holders, seen=set())
fraction = mean(_is_beneficial_owner_identity(h.holder_name) for h in holders)
```

Sharing the extraction path is required, not incidental: `_extract_holder_rows`
already drops section headings, address-continuation fragments and rows with no
value, and recovers ragged name/share/percent cells. Measuring on `parsed.rows`
instead would count a `Named Executive Officers` heading as an identity and
would penalise a genuine table whose rows are address continuations.
(Codex ckpt-1 HIGH.)

Denominator is the extracted holder count. A table extracting zero holders is
already ineligible under #2158's element 4 (zero-row tables cannot win) and is
not re-tested here.

### D1 — Owner-identity predicate

`_is_beneficial_owner_identity(cell: str) -> bool`, positive test only.

A blocklist of junk labels would need a new entry per junk table shape; the
prevention-log rule on hand-enumerated exception tuples applies. A positive test
is closed under new junk types.

Accept when, after stripping presentation debris (leader dots `\.{3,}`, leading
footnote markers `*` `†` `#`, trailing footnote digits, trailing `(1)`):

1. the cell matches `as a group` (Instruction 5's aggregate row); or
2. the cell contains an entity designator (`LLC`, `L.P.`, `Inc`, `Trust`,
   `Fund`, `Partners`, `Capital`, `Management`, `Advisors`, `Holdings`, `N.A.`,
   `plc`, …); or
3. the cell **starts with** a person-name pattern — two or more capitalised
   tokens, allowing initials, particles (`van`, `de`, `von`), and the
   surname-first comma form (`Bunch, Charles E.`).

Not anchored at the end: Item 403(a) prescribes name **and address** in one
column, and issuers append titles and credentials. Anchoring to `$` rejected
`'David J. Mazzo, Ph.D., President and Chief Executive Officer'` and
`'BlackRock, Inc. 50 Hudson Yards New York'` — measured, not assumed.

Must additionally accept, per the 6 measured false negatives:

4. an all-caps token of >=3 chars followed by address-like content
   (`MUFG 4-5, Marunouchi …`, `FMR LLC`), and
5. a known-entity token without a corporate suffix (`State Street`, `Vanguard`)
   — via the entity-designator list extended with the address-follows form,
   NOT via a hardcoded issuer list.

### D2 — Selection gate, not a row filter

`_ROW_IDENTITY_FLOOR: Final[float] = 0.5`.

In `parse_beneficial_ownership_table`'s window loop, a parsed table is
**ineligible to win its window** when it fails **either** limb of the Item 403
eligibility test:

```python
eligible := owner_identity_fraction(holders) >= _ROW_IDENTITY_FLOOR
            and item403_value_signature(table.column_headers)   # D4
```

**Both limbs gate WINNER selection, not only sibling membership** (correction
of 2026-07-29b; measured, see below). The draft applied D4 to D3's sibling set
only, which leaves the ticket's central case untouched: an Item 402 compensation
table's rows **are people** — `Kevin R.M. Smith`, `Dr. Hou`,
`Jennifer F. Scanlon` — so it scores `owner_identity_fraction = 1.00`, passes
the identity limb, and still wins its window.

Measured against #2163's 23-accession junk cohort:

```text
D2 identity limb kills          6
D4 value-signature limb kills  13     <- would NOT fire under a sibling-only D4
neither (3 unrelated defects)   3     <- stay on #2163
```

Worked cases that pass identity at 1.00 and are rejected only by D4:

```text
0000950170-25-045737  ['Name', '', '(%)', '', '', '($) (1)']
0001193125-25-093573  ['Named Executive Officer', 'Target Annual Cash Incentive as Percentage of Base Salary']
0001193125-26-140022  ['Named Executive Officer', '', 'Target 2025 Award (As a Percentage of Base Salary)', …]
0001193125-24-202132  ['Named Executive Officer', '', 'Pre-Separation Fiscal 2024 Bonus Opportunities (percentage of base salary)']
0001539497-26-000784  ['Name and Address', '', 'Age', '', 'Position', '']
```

It is a **selection** gate, deliberately not a row-level filter. ~6 of the 61
sub-floor accessions carry genuine holders; a row-level filter would delete
them. Under a selection gate, a genuine table that fails can only lose if it is
the sole candidate, in which case the parse emits zero rows — the existing
guard-blocked behaviour, which is safe and already covered by the rewash
zero-row guard.

### D3 — Sibling gate reads identity, not score

Replace

```python
qualifying = [t for sc, t in window_qualifying if sc >= _SIBLING_SCORE_FLOOR or sc == window_best_score]
```

with membership by row identity: every table in the winning window that clears
`_ROW_IDENTITY_FLOOR` is an Item 403 sibling and its rows are concatenated.
`_SIBLING_SCORE_FLOOR` is deleted.

This is what #2160 was actually about. The arbitrary floor of 6 exists only
because header score was the sole signal; once row identity carries the
decision, a genuine 403(b) table scoring 3 on a bare `Name|Shares|Percent`
header is admitted on its rows, and the `sc == window_best_score` tie arm that
that #2158 broke is no longer load-bearing.

Header score is retained for **window ranking**, where it works.

## Blast radius / what this change ADMITS

Per the #2158 lesson, count what the change newly admits, not only what it
rejects. This change is **narrowing** for selection (D2) and **widening** for
sibling membership (D3). Both directions must be counted:

- D2 narrowing: tables that previously won and now cannot → expect the ~54 junk
  accessions; any genuine loss is a defect.
- D3 widening: tables scoring 3-5 that were previously excluded and are now
  merged → **this is the direction with no existing coverage.** A low-scoring
  table that clears the row-identity floor could be a breakdown table
  (harmless — `_extract_holder_rows` dedups by identity) or a comp table whose
  rows happen to be person names (harmful). Must be enumerated.

## Verification plan

Per `.claude/skills/engineering/full-population-ab.md`, three arms:

1. **Arm 1 — parse vs parse.** `main` worktree vs branch, all 42,566 bodies.
   Metric: **distinct holders** keyed `lower(trim(holder_name))` (matching the
   `holder_name_key` generated column), never row count. Enumerate every
   accession that loses a holder and classify by hand. Inspect the gain side.
2. **Arm 2 — parse vs STORED.** The #2160 residual lives entirely in this blind
   spot (both sides return nothing in Arm 1). Both summaries passed so the arm
   can state: entities `main` reproduced that the branch does not.
3. **Arm 3 — mechanism audit.** Per-table `owner_identity_fraction` before/after
   with the selection outcome, enumerated — not an aggregate counter.

Acceptance — every item is blocking:

- The 61 sub-floor accessions: junk gone, **zero genuine holders lost**.
- Issue #2160's 26-accession guard-blocked residual: re-parse yields rows.
- No accession loses a holder that `main` produced.
- **Zero harmful admits from D3.** Enumerating the newly-admitted low-score
  tables is not sufficient — an Item 402 comp table whose rows are NEO names
  passes the person-name predicate, and admitting one emits compensation data as
  beneficial ownership (the #2163 defect this change exists to remove). Every
  newly-admitted table is classified by hand and the count of comp/plan/metric
  tables admitted must be **zero**. If it is not, D1 needs a discriminator
  beyond row identity — most likely requiring the table to carry a
  `percent_of_class`-resolvable column, which Item 403 prescribes and Item 402
  tables do not have. (Codex ckpt-1 HIGH.)

### D4 — Item 403 value-column signature (required; row identity is NOT sufficient)

Census pass 2 result: row identity alone admits **1,668** score-3-5 non-winning
tables, and they are dominated by Item 402 compensation tables whose rows *are*
people:

```text
'Named Executive Officer | Shares at Target | Final PSU Payout %'
'Named Executive Officer | Annual Base Salary | Target Bonus Opportunity'
'Name | Threshold (Percentage of Base Salary) | Target (Percentage of Base Salary)'
'Name of Individual or Identity of Group and Position | Shares Underlying Options'
'Beneficial Owner | Number of RSUs'
'Position | Minimum Dollar Value | Minimum Number of Shares'      (ownership guidelines)
```

D3 therefore does **not** ship on row identity alone. A table joins the winning
window's sibling set only if it ALSO carries Item 403's prescribed value
signature — 229.403 column 4 is **"Percent of class"**, class-denominated. A
compensation table's percent is of *salary*, *target*, *payout* or *vesting*.

Measured on the full admit cohort: the gate takes **1,668 -> 45 (2.7%)**, and
the survivors are the genuine shapes:

```text
'Name | Shares (1) | Percent of Outstanding Shares of Common Stock'
'Name | Number of Common Shares | Percent of Common Shares'
'Name | Number of Shares | Approximate Percentage of Outstanding Common'
```

### D4 — SUPERSEDED first form (kept for the record)

```python
# signature := not COMP_PCT and (CLASS_PCT or AMOUNT_NATURE)
```

Measured full-population it empties **1,897 accessions — 4.5% of the corpus**.
Do not implement it. Why it read clean is the subject of the next section.

### Full-population verification — pass 3, BOTH directions (2026-07-30)

Census pass 2 measured the gate **only on the 1,668 score-3-5 non-winning tables
D3 newly ADMITS**. It never re-measured the tables that already WIN. The
`1,668 -> 45` figure is therefore sound for what it measured and silent on the
regression — and D2/D4 gate winner selection, so the narrowing side is the side
that can destroy stored data.

This is the directionality rule in `.claude/skills/engineering/full-population-ab.md`
applied to a **narrowing** change: enumerate what the gate now REJECTS, not only
what it admits.

Pass 3 dumps every candidate table in every window across all **42,577** stored
`def14a_body` payloads (`scripts/census_def14a_window_tables.py`), so any gate
variant is evaluated offline in both directions with no re-parse per variant
(`scripts/analyse_def14a_window_tables.py`). The enumeration path is
byte-identical on `main` and this branch — only the selector changed — so one
dump is a valid substrate for replaying both rules. The harness imports the
SHIPPED `_item403_value_signature` as its `final` variant rather than
re-expressing it, so the measurement cannot describe a gate nobody merges.

| gate | accessions emptied | winning tables dropped | newly admitted |
|---|---|---|---|
| D4 first form, on `column_headers` | 1,897 | 2,479 | 172 |
| first form, on both header tuples | 1,816 | 2,377 | 175 |
| pair `(AMOUNT and PERCENT)` | 610 | 1,049 | 254 |
| **final form (below)** | **196** | **419** | **304** |
| loosest reference (any value vocabulary) | 176 | 402 | 561 |

### D4 final form — precedence, not vocabulary

Six defects, each found by the narrowing enumeration and none visible to pass 2.

1. **Wrong header tuple.** The signature was applied to `column_headers`; pass 2
   measured `score_headers`. They differ whenever a two-row header promotes the
   SUB row — `column_headers` becomes `('', 'Sole', 'Shared', 'Total', '')`
   while the parent `Amount and Nature of Beneficial Ownership | Percent of
   Class` survives only in `score_headers`. Reading the narrower tuple rejected
   the most prescribed shape the reg has, at scores 14 and 16. Read **both**.
2. **Bare percent caption.** `CLASS_PCT` required a class noun after the percent
   token, so `Name | Shares | Percent` was rejected — contradicting D3's own
   rationale in this spec.
3. **The comp veto cannot span the whole header.** Rule 13d-3(d)(1)(i) DEEMS a
   person the beneficial owner of shares acquirable **within 60 days**, so a
   genuine Item 403 table legitimately captions columns `Options Exercisable or
   Vesting Within 60 Days` and `Number of Performance Shares Granted`. A blanket
   `vesting` / `performance` veto deleted 18-, 22- and 10-holder Vanguard /
   BlackRock / First Eagle tables.
4. **Columns 1 and 2 have literal prescribed captions.** `Title of class` and
   `Name and address of beneficial owner` are 229.403's own words and nothing
   else in a proxy uses them. They are what recover a table whose VALUE column
   is merely the class name (`Name and Address of Beneficial Owner | Common
   Stock`). Bare `Beneficial Owner` is NOT sufficient — it admits
   `Beneficial Owner | Number of RSUs`, so the column-3 arm keys on
   `own(ed|ership)`, never on `owner`.
5. **Column 4 is often absent entirely.** Dual-class and direct/indirect tables
   caption their value columns `Class A Common Stock Owned | Class B Common
   Stock Owned | Total Voting Power` and carry no percent anywhere.
6. **Item 402 needs its own term of art.** Item 402(a)(3) DEFINES "named
   executive officer"; Item 403 says "name of beneficial owner". Without it,
   `Named Executive Officer | PSU Shares Granted (#) | Final Achievement %`
   leaked through the generic pair.

The resulting rule is an ORDERING. Reg-literal wording admits outright; only the
weak generic evidence is subject to the Item 402 vetoes:

```python
# 1. 229.403's own captions, or Rule 13d-3's acquisition window -> Item 403 on its face
STRONG = (BENEFICIAL          # 'shares beneficially owned', 'beneficial stock ownership'
          or OWNER_CAPTION    # col 2: 'name [and address] of beneficial owner'
          or AMOUNT_NATURE    # col 3: 'amount and nature' / sole|shared voting|dispositive
          or CLASS_PCT        # col 4: 'percent of class'
          or TITLE_OF_CLASS   # col 1: 'title of class'
          or RULE_13D3_60_DAY)# 'within 60 days' -- 240.13d-3(d)(1)(i)
# 2. otherwise Item 402 vocabulary vetoes
# 3. otherwise a weak pair admits
WEAK = AMOUNT_IND and (PERCENT_IND or OWNED_IND or BENEFICIALLY)
```

Ordering the reg's wording ABOVE the veto is what lets the veto stay broad.

`PERCENT_IND` is word-bounded so `Percentile` (TSR payout ladders) does not read
as a percent-of-class column.

**Residual, hand-classified (199 accessions emptied, 423 winning tables
dropped):** payout curves, TSR percentile ladders, vest-date and reverse-split
tables, ownership-guideline multiples, private-placement participation tables and
plan-amendment prose — all correctly dropped — plus a small tail whose headers
have degraded to empty cells. Distinct-holder loss is arm 1's question and is
reported there, not here.

### Codex ckpt-1 resolutions — measured, not asserted

`scripts/audit_def14a_gate_arms.py`, full population.

**The score floor of 3 stays, and it is LOAD-BEARING (HIGH).** D3 says header
score no longer decides sibling membership; that is true only ABOVE the floor.
`window_qualifying` still filters `score >= SCORE_FLOOR`, so
`_is_item403_eligible` is never evaluated on a score 0-2 table and the pass-3
admit census covers score 3-5 only. Removing the floor was measured rather than
argued: it would newly admit **704 tables / 5,794 rows**, dominated by Item 402
option-grant tables —

```text
'Name | Grant Date | Number of securities underlying the award |
 Exercise price of the award ($/Sh) | Grant Date Fair Value'      14 + 9 + 8 + 8 + 6 + 5 + 4 …
```

— which is precisely the leak this spec's acceptance forbids. The floor is
retained deliberately. It has a cost: a genuine
`Directors, Nominees and Named Executive Officers | Options Exercisable Within
60 Days` table scores below it and stays excluded. That cost is accepted; the
alternative admits Item 402 grant data as beneficial ownership.

**Strong-arm precision (MED).** "Nothing else in a proxy uses this caption" is a
safety claim, so each arm's SOLE admits — tables it and no other arm admits —
were enumerated and classified:

| arm | tables | shapes | classification (FULL population, hand-checked) |
|---|---|---|---|
| col3 `beneficially owned` | 643 | 372 | genuine Item 403 throughout |
| col4 `percent of class` | 369 | 252 | genuine |
| col2 `name … of beneficial owner` | 329 | 213 | genuine (Vanguard, BlackRock, Perceptive, Hercules) |
| col3 `amount and nature` | 16 | 9 | genuine (FMR, Macquarie, Dimensional) |
| Rule 13d-3 `within 60 days` | 11 | 11 | genuine |
| col1 `title of class` | 4 | 3 | genuine |

No compensation, plan or metric table appears among them. This is a
full-population enumeration, not a sample: a STRONG arm admits AHEAD of the
Item 402 veto, so sample evidence would not be sufficient (Codex ckpt-1 MED).

**And the arms were not near-perfect on first measurement.** Codex ckpt-1 found
`CLASS_PCT` admitting `Percentage of Shares Earned`, `Percentage of Shares
Vested` and `Percentage of Common Stock Earned` — all Item 402 outcomes, all
bypassing the veto. Fixed with a CLOSED rule rather than another blocklist:
229.403 column 4's denominator is a class of securities, so the class-noun run
ENDS the phrase, and only punctuation, a footnote marker or a continuation
preposition may follow it. The run is POSSESSIVE (`*+`) because otherwise the
engine backtracks — `Percentage of Common Stock Earned` matched by consuming
only `Common` and finding `Stock` in the allowed-follow set.

The 60-day arm was tightened on this finding: it now requires an ACQUISITION
VERB (`exercisable` / `acquire` / `issuable` / `vest` / `convert` / `settle`) in
the same header cell as the window. 13d-3(d)(1)(i) is about securities a person
has the right to *acquire* within sixty days; the bare phrase also appears in
change-in-control and termination tables, and those would otherwise be admitted
ahead of the Item 402 veto.

**The 120-char D1 cap is EMPIRICAL, not a documented SEC limit (MED).** No reg
bounds the length of a 403(a) name-and-address cell. Sensitivity, full
population:

```text
cap=120  emptied=199  winning tables dropped=423
cap=160  emptied=198  winning tables dropped=417
cap=200  emptied=198  winning tables dropped=417
cap=300  emptied=198  winning tables dropped=417
```

The cap is very nearly inert on selection — raising it 120 → 300 moves the whole
corpus by one accession and six tables. Of the 209 over-cap occurrences (163
distinct), roughly half are genuine long name-and-address cells (Wellington,
Kayne Anderson Rudnick, Hershey Trust) and half are director-BIO paragraphs,
which are what the cap exists to reject. It is retained at 120.

Note the cap can never drop a holder from OUTPUT: it feeds the identity
FRACTION that gates table selection, so an over-cap holder merely fails to vote
for its own table's eligibility. `_extract_holder_rows` still emits it.

### D1 — two further corrections found by the pass-3 narrowing enumeration

4. **Strip presentation debris BEFORE the length cap.** Issuers pad the name
   column with HTML leader dots to rule across to the figures —
   `Hotchkis & Wiley Capital Management, LLC` plus 63 dots. Testing the raw cell
   blew the 120-char cap, rejected the holder, took the table under
   `_ROW_IDENTITY_FLOOR` and dropped a genuine `Amount and Nature of Beneficial
   Ownership | Percent of Class` table (`0000074303-25-000056`). The clause
   below already said to strip debris first; the implementation did not.
5. **Clauses 4-5 below were specified and never implemented.** Item 403(a)
   prescribes name AND ADDRESS in ONE column, so an entity with no corporate
   suffix is identified by the address that follows it: `MUFG 4-5, Marunouchi
   1-chome Chiyoda-ku, Tokyo`, `Vanguard 100 Vanguard Boulevard`, `BlackRock 50
   Hudson Yards`. The second token is a street number, so none reaches the
   two-capitalised-token person pattern. `0001140361-25-012302` scored 0.25 on
   identity and was emptied. Matched on the reg's one-column name-and-address
   FORM — not a hardcoded issuer list.

### D1 final form (three corrections found by measurement)

The draft predicate admitted three junk classes. All three are fixed and
re-measured:

1. **Length cap 120 chars.** Without it, Schedule 13G footnote paragraphs
   ("Based solely on an amendment to a Schedule 13G filed by BlackRock…",
   "Consists of 10,500 shares held directly…") are extracted as holder names and
   pass the person-name test. 120 is sized for an Item 403(a) name **and
   address** cell, which is the longest legitimate form.
2. **Entity designators are case-SENSITIVE.** `trust interests` and
   `allocation interests` are instrument types; case-insensitive `\btrust\b`
   admitted them. An Item 403 owner is a proper noun — `Smith Family Trust`
   matches, `trust interests` does not.
3. **Instrument-type vocabulary guard.** A name composed *entirely* of equity /
   award nouns (`Equity`, `Stock Options`, `Restricted Share Units`) is an
   instrument, not a beneficial owner under Rule 13d-3. This is a closed ~40-word
   vocabulary set, deliberately not a table-shape blocklist — the prevention-log
   rule on hand-enumerated tuples targets the latter.

### Measured outcome — the D3 admit population

```text
score 3-5 non-winning tables extracting rows        2,091
  clearing row identity, DRAFT predicate            1,668   (79.8%)
  clearing row identity, FINAL predicate            1,615
  surviving D4 signature -> newly admitted by D3       73   (3.5%)
```

All 73 enumerated and hand-classified. They are genuine Item 403 tables —
BlackRock, The Vanguard Group, FMR LLC, Plains GP Holdings L.P., named directors
and officers. **One questionable:** `0001193125-25-064881`
(`C-Suite Executives, members of our Investor Relations, Legal, Finance and
Human Resources Departments`) is a stock-ownership-guidelines table, not Item
403. Verify against the body during implementation; if genuine junk, it is one
accession and does not change the design.

### Evidence gap — census pass 2 (RESOLVED; findings above)

Pass 1 measured row identity on **winning tables only**. D3 applies the signal
to **every table in the winning window**, including low-score tables never
previously admitted. The signal is therefore unproven on the population it will
newly admit, and the broad entity-designator list (`Trust`, `Fund`, `Capital`,
`Management`, `Holdings`) is unproven there too. (Codex ckpt-1 BLOCKING + MED.)

Pass 2 re-runs the corpus capturing every candidate table's extracted holder
names via `_resolve_columns` + `_extract_holder_rows`, so the identity fraction
is measured on the same basis D0 specifies and over the full admit population.
Implementation does not start until pass 2 reports the distribution of
row-identity fraction for **non-winning** tables scoring 3-5 — the exact cohort
D3 admits.

## Three-arm result — FINAL (round 3)

Two real checkouts, `main` @ `7b2ae45f` vs branch, all **42,577** stored bodies. Census
fidelity **100.000%** (the replay reproduces the real parser's rows/no-rows on every
accession — asserted, not assumed).

```text
accessions_with_rows        7,172 ->  7,145
rows                      105,400 -> 108,812
Item 402(c) SCT drift            0                <- clean
distinct holders GAINED     +4,491  (364 accessions)
distinct holders LOST       -1,079  (242 accessions)
```

**Arm 2 (parse vs STORED):** 238 accessions / 1,059 holders `main` reproduced and the
branch does not — consistent with arm 1, no additional blind-spot population.

**Arm 3 (value/role audit) — the arm arms 1 and 2 structurally cannot see:**

```text
role LOSSES (role -> None)      0     <- the #2164 regression pattern does not occur
role GAINS  (None -> role)    213
role RECLASSIFICATIONS          3     all corrections (main was reading a table whose
                                      "holders" were job titles)
share values changed          304 rows / 70 accessions
```

Of those 70, **56 had `main` sitting on an Item 402 table** — Tyson's round `1,200,000`
and `1,650,000` salary figures become real share counts `3,826,952` / `850,079` — and ~10
more were private-placement `Aggregate Purchase Price` tables. The remainder is the
Liberty Media multi-series shape, **pre-existing on `main`**, filed as #2175.

### Net-negative accessions, classified

156 accessions end net-negative. **107 are provably correct drops** (`main` was on an
Item 402 grant table, a capitalisation table, an equity-plan pool, a reverse-split ladder,
a TSR percentile ladder, a New Plan Benefits table, a table of contents or an income
statement). The remaining 49 are individually reviewed; several are junk the classifier
mis-flagged, and three genuine-loss classes are filed as **#2176**:

1. instrument nouns (`vested`, `rsu`) vetoing a genuine 403(b) table that reports vested-
   but-unsettled RSUs as a Rule 13d-3(d)(1)(i) component (ExlService, 2 accessions);
2. class-label rows sinking the identity fraction on a table that quotes column 3 verbatim
   (TDS, 2 accessions — same shape as #2175);
3. dual-class tables with a bare `Beneficial Owner` caption and no percent column
   (2 accessions) — accepting the bare caption measurably admits
   `Beneficial Owner | Number of RSUs`, so the other side of that trade was taken.

### The guard-blocked count still moves the wrong way, and that is now understood

```text
main>0  -> branch==0   89   of which have stored rows  87
main==0 -> branch>0    62   of which have stored rows   3
                                        net guard-blocked  +84
```

This ticket was filed to free a 26-accession guard-blocked residual. It frees 3. But the
87 newly-blocked accessions are, by the classification above, ones where `main` was
selecting a NON-Item-403 table — the parser is now correctly declining to reproduce junk,
and the rewash guard preserves the stored rows rather than losing them.

The blocking is therefore correct behaviour, and the ticket's original acceptance was
written before the full-population census showed what those accessions actually contain.
Flushing their stored junk is the CHOKEPOINT's job, not the selector's: #2173 (merged)
established the mechanism — release the zero-holder guard when the stored rows are
provably not Item 403 data — and extending its predicate from "13D/G cover labels" to
"stored rows fail the Item 403 identity/value test" is the follow-up that closes the
original 26 and these 87 together.

## Definition of done additions (ETL clauses 8-12)

- Panel `AAPL`/`GME`/`MSFT`/`JPM`/`HD` plus the 5 #2160 worked accessions.
- Cross-source: one 5% holder verified against SEC EDGAR direct.
- `POST /jobs/sec_rebuild/run` scoped `{"source": "sec_def14a"}`, executed.
- `/instruments/{symbol}/ownership-rollup` verified post-backfill.
- Parser version bumped v12 -> v13 (forces re-ingest; the conflict key is
  derived from the parsed value, so a corrected row lands under a NEW key and
  the stale row must be superseded — prevention-log #2140 entry).
- `_supersede_dropped_holdings`' empty-set guard preserved (`holder_name <> ALL('{}')`
  is vacuously TRUE — prevention-log #2140 BLOCKING).
- Rewash must use the `_rewash_def14a` chokepoint with sibling fan-out (#2157).
