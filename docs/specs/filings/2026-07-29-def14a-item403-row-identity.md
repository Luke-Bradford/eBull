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
**ineligible to win its window** when
`owner_identity_fraction(parsed.rows) < _ROW_IDENTITY_FLOOR`.

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

**Two known over-drops to fix before implementation** (enumerated from the
dropped set, not assumed):

1. `Beneficial Ownership | Sole Voting Power | Shared Voting Power` (8
   occurrences) — a genuine Item 403 table. Item 403 column 3 is "Amount and
   nature of beneficial ownership"; Rule 13d-3 defines beneficial ownership as
   voting **or** investment power, so issuers legitimately subdivide that column
   into Sole/Shared voting and dispositive power and carry no separate percent
   column. `_resolve_columns` already knows this subdivision (its tiered
   `Sole | Shared | Total` handling). The signature must accept it.
2. `Number of shares of Class A common stock | % of all shares` (4) — "% of all
   shares" is class-denominated but the noun after "of" is `all`, which the
   draft pattern's alternation misses.

So the signature is: a class-denominated percent column **OR** the
amount-and-nature voting/dispositive-power subdivision, and never a
salary/target/payout/vesting-denominated percent.

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

## Definition of done additions (ETL clauses 8-12)

- Panel `AAPL`/`GME`/`MSFT`/`JPM`/`HD` plus the 5 #2160 worked accessions.
- Cross-source: one 5% holder verified against SEC EDGAR direct.
- `POST /jobs/sec_rebuild/run` scoped `{"source": "sec_def14a"}`, executed.
- `/instruments/{symbol}/ownership-rollup` verified post-backfill.
- Parser version bumped v10 -> v11 (forces re-ingest; the conflict key is
  derived from the parsed value, so a corrected row lands under a NEW key and
  the stale row must be superseded — prevention-log #2140 entry).
- `_supersede_dropped_holdings`' empty-set guard preserved (`holder_name <> ALL('{}')`
  is vacuously TRUE — prevention-log #2140 BLOCKING).
- Rewash must use the `_rewash_def14a` chokepoint with sibling fan-out (#2157).
