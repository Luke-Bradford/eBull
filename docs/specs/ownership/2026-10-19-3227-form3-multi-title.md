# #3227 — Form 3 and DERA `:NDH:` line sums read several titles of one class as one title

Ticket: #3227, the Form 3 / DERA `:NDH:` multi-title residual left by #3889. The build is in the
same PR.

## Problem

#3889 taught the Form 4 helpers to read titles that name one class as one title
(`form4_title_class` / `form4_title_match`, frozen in `FORM4_TITLE_CLASS_VERSION`). The two holding
line sums that are not Form 4 helpers still require one exact `security_title`:

- **XML Form 3** — `insider_form3_ingest._line_sum_if_distinct`, called by
  `form3_observation_shares` from both Form 3 writers (the live XML writer and the weekly
  `sync_insiders`);
- **DERA `:NDH:`** — `INSIDER_HOLDING_LINE_SUM_LATERAL` (`ownership_observations.py`), the
  read-time sum of an accession's Table I holding lines for one owner, shared by both `_current`
  refreshers and `ownership_history._insiders_history`. DERA `NONDERIV_HOLDING` carries Form 3 and
  Form 4/5 holding lines alike.

So "Common Stock" beside "Common Stock 401(k)", "Common Stock (Restricted)" or "Common Stock, $01
Par Value" refuses, and the observation keeps one line.

A third reader agrees with them: `ownership_rollup._summable_scope` (#3886 line components) keeps a
Form 3 scope on exact equality "as `form3_observation_shares` does". It must move with the writer.

## Source rule

Form 3 General Instructions (`https://www.sec.gov/files/form3.pdf`, read 2026-10-19):

- **4(b):** *"The title of the security should clearly identify the class, even if the issuer has
  only one class of securities outstanding: for example, "Common Stock," "Class A Common Stock,"
  "Class B Convertible Preferred Stock," etc."* This is Form 4 General Instruction 3(b) word for
  word, the instruction `form4_title_class` reads.
- **Table I reminder:** *"Report on a separate line for each class of securities beneficially owned
  directly or indirectly."* So classes never sum.
- **5(b)(iii):** *"Report securities beneficially owned directly on a separate line from those
  beneficially owned indirectly. Report different forms of indirect ownership on separate lines."*

The DERA `:NDH:` rows are the same Table I lines (Form 3, and Form 4/5 holding lines under Form 4
Instr. 3(b) and 4(b)(iii)), read from `NONDERIV_HOLDING.SECURITY_TITLE`.

**Structured-field check** (the text-classifier rule): an XML Form 3 `<securityTitle>` is free text
with only `footnoteId` links, as #3889 recorded for Form 4. `NONDERIV_HOLDING.tsv`'s header
(read from the cached `insider_2017q2.zip`) is `ACCESSION_NUMBER`, `NONDERIV_HOLDING_SK`,
`SECURITY_TITLE`, `TRANS_FORM_TYPE`, `SHRS_OWND_FOLWNG_TRANS`, `VALU_OWND_FOLWNG_TRANS`,
`DIRECT_INDIRECT_OWNERSHIP`, `NATURE_OF_OWNERSHIP`, each with a `_FN` footnote column: no class or
CUSIP column. No structured source decides line class.

**So no new rule.** The instruction is the same text, so the reading is the same frozen rule,
`form4_title_class` at `FORM4_TITLE_CLASS_VERSION` `dfcd38a31f63`, unchanged. Everything #3889
accepted about it (the whitelist, nesting, par values, `restricted`) carries over.

## Change

### 1. XML Form 3 (`_line_sum_if_distinct`)

- Condition "one title" becomes `form4_title_match(titles)`. One distinct title is equality, so a
  single-title group's verdict cannot change.
- The equal-amount check ("no two of the filer's lines of that title carry the same amount, across
  D and I") compares the filer's lines the match accepts: the exact title for a single-title group,
  the class for a multi-title one. This is #3889's reading of "same title" for Form 4's
  holding-only groups.
- Every other condition is unchanged: a known D/I, more than one line, non-negative shares,
  non-blank titles, one reporting owner.

### 2. DERA `:NDH:` (`INSIDER_HOLDING_LINE_SUM_LATERAL`)

The class rule is Python and the line sum is SQL. Porting the rule to SQL would make a second copy
of a frozen classifier. Instead each reader passes the class of every live `:NDH:` title in its
scope as two parallel arrays, built by one helper (`insider_ndh_title_class_params`) that calls
`form4_title_class`:

- params `ndh_title text[]`, `ndh_class text[]` (the class's words joined by a space, which is
  injective because no class word contains a space); a title with no class is left out;
- the lateral `LEFT JOIN`s `unnest(...)` on the title, and the condition
  `count(DISTINCT o.security_title) = 1` becomes: one exact title, **or** every line has a class
  and all lines have the same one. That is `form4_title_match`, the same predicate.

The scope query is a `SELECT DISTINCT security_title` over the readers' instruments' live `:NDH:`
observations (dev 2026-10-19: 2,795 distinct titles over the whole corpus; 13 ms for one
instrument). The equal-amount, D/I, owner-count and D/I-label conditions are unchanged.

- **Not the helper alone.** `form4_title_match` accepts `[None]` or `[""]` by equality, and SQL's
  `count(DISTINCT)` skips NULLs. The fragment's existing guards (`count(security_title) =
  count(*)`, non-blank titles) stay, so the whole lateral equals the writers' test: non-blank
  titles, then one title or one class on every line (`count(class) = count(*)`). One boundary
  remains: SQL `btrim` strips spaces where Python `.strip()` strips all whitespace, so a title of
  only tabs or newlines is blank to the XML writer and not to the lateral. Dev 2026-10-19 has 0
  such titles in `:NDH:` observations and 0 in `insider_initial_holdings`, and it predates this
  change.
- **D/I domain.** The lateral checks `direct_indirect` is non-NULL where the XML writer checks
  `D`/`I`. `sql/400` puts a CHECK on the column and `_parse_direct_indirect` maps anything else to
  NULL; live `:NDH:` rows are `D` 180,213 and `I` 132,635 and nothing else (dev 2026-10-19).
- **Snapshot.** The map is read inside the refresher's transaction, after its advisory lock and
  watermark, one statement before the MERGE. A line committed in that gap is the same window the
  MERGE already has against the watermark. A title missing from the map only refuses the sum (the
  value before this change); it can never inflate one, and the writer's own refresh after its
  write reads a map that has it.

### 3. The rollup reads the same test (`_summable_scope`)

A Form 3 scope takes `form4_title_match` over its raw titles (the Form 3 writer does not strip
them). A Form 4 holding scope keeps its stripped titles. Without this, a newly summed Form 3
observation would have no line components and #3886's control-group collapse would stop matching
it.

The Form 3 scope also reads only share-bearing lines, the lines `form3_observation_lines` builds
from (`row_num` is NOT NULL in `sql/093`). Today a shareless line's title enters the scope, so a
group the writer sums could still refuse there (Codex checkpoint 1). The A/B adds every instrument
whose `_current` Form 3 row has such a line.

### 4. Fragments that compare titles and are NOT changed

- `_INSIDER_DUAL_PIPELINE_DECOLLISION` clause 2 matches each `:NDH:` line to an IDENTICAL XML line
  (title, D/I, shares). That is line identity across two pipelines' copies of one filing, not
  class. It drops a `:NDH:` winner only when the XML winner holds its whole D/I total. After
  change 1 more XML winners hold that total, so the clause can drop more `:NDH:` twins. That is the
  clause's own rule applied to a correct total, and the A/B counts it.
- `INSIDER_SUPERSEDED_BY_AMENDMENT_FILTER` matches an amendment's line to its original's by row
  number, title and shares. That is line identity too.

### 5. Monotonicity

Both sums admit only groups refused at the title test today, and a sum of non-negative lines
including the winner's own is at least that line. No observation value falls.

That is local. A holder's rollup total CAN fall: once an XML winner holds its D/I total, change 4's
de-collision drops the cross-accession `:NDH:` twin it used to sit beside, so 400 + 400 becomes
500 (Codex checkpoint 1). That is the clause removing a double count, but it is a fall, so the A/B
prints every removed key and every holder whose total falls, and each is read. The clause matches
lines by existence, not multiplicity (two equal DERA lines can match one XML line); the A/B checks
each removed key's line counts on both sides.

### 6. Backfill

- **XML Form 3:** the line tables live under the entity instrument while the live writer fans the
  observation out to share-class siblings, and `sync_insiders` writes the entity only. So the
  backfill writes the treatment value onto EVERY live `form3` observation of a changed group,
  matched by accession, holder and nature on any instrument (the #3227 Form 3 line-sum backfill's
  shape). No filing is re-parsed.
- **DERA `:NDH:`:** read-time, so a `_current` refresh.

`scripts/ab_3227_form3_multi_title.py --apply` and `--verify` load no control code, so they work
after the merge (Codex checkpoints 1 and 2: with `origin/main` as control they would compare the
treatment with itself). Each runs in ONE snapshot, its checks and writes included, and acts on the
**reach**, every instrument either rule can touch whatever its stored state: every instrument with
a live multi-title `:NDH:` group, every instrument holding an observation of a multi-line XML group,
and every instrument whose rollup Form 3 scope can move (change 3).
- `--apply` writes the treatment value onto every live observation of a multi-line XML group that
  does not hold it, batch-refreshes the reach's `_current`, and commits.
- `--verify` exits 1 unless every such observation holds the treatment value and each reach
  instrument's `_current` equals a fresh rolled-back refresh on every business column.

The A/B's control defaults to the pre-change base `98b332b0`, and the script refuses a ref that
already carries the change.

## Full-population verification (dev, 2026-10-19)

Run: `PYTHONPATH=. uv run python -m scripts.ab_3227_form3_multi_title` (control `98b332b0`; stage 0
read-only, stage 1 one rolled-back transaction per arm per instrument, then the batch refresher over
real 200-instrument batches from the same stored state). The two arms are separate transactions and
the dev jobs daemon runs meanwhile, so an ingest committed between them would appear as a move.
Every `_current` key that moves is printed with both arms' winner and shares, and every holder
total that moves (keyed by CIK, by name only without one); all were read and none traced to an
ingest. A removed `:NDH:` key also prints whether its DERA `(title, D/I, shares)` multiset is
covered by one XML accession's lines (Change 5's multiplicity concern); none was removed.

**Stage 0, XML Form 3:** 4,945 `(filer, D/I)` groups of more than one share-bearing line, 1,479 of
them with more than one title. **97 change, 0 fall** (79 D, 18 I). The script prints all 46 unified title sets and all were read:
re-spellings and case, par-value phrases, issuer prefixes ("Mid Penn Bancorp, Inc. Common
Restricted Stock"), and restricted, 401(k), ESPP, IRA, PSP/SIP, DRP, book-entry and street-name
lines of one class.

**Stage 0, DERA `:NDH:`:** 11,568 live accession groups with more than one title, every period.
**1,147 newly sum over 142 instruments; 0 stop summing.** All 153 unified title sets were read, of
the same kinds. By manifest form: 869 have no manifest row, 158 Form 4, 60 Form 3, 45 Form 5, 8
3/A, 7 4/A, 0 5/A.

**Cohort:** 510 instruments: 44 holding the 99 live `form3` observations of changed XML groups (all
99 at the control value today; they are the only multi-line observations of 5,125 not at the
treatment value), the 142 DERA instruments, and 383 whose rollup Form 3 scope carries more than one
title or a shareless line. The reach for `--apply` / `--verify` is 1,012 instruments.

**Stage 1** (`_current` and rollup per instrument, both arms):

| | |
|---|---:|
| `_current` keys: shares up | 54 |
| `_current` keys: down / removed / winner changed / added | **0 / 0 / 0 / 0** |
| per-instrument vs batch refresher disagreements | 0 |
| insider rollup total up / **down** | 12 / **0** |
| holder totals down | **0** |
| newly oversubscribed | 1 (JBHT, below) |
| smoke panel (AAPL, GME, MSFT, JPM, HD) in the cohort | none |

No de-collision drop fired (Change 5's concern), so no key was removed.

**Cross-source:**
- **JBHT, Johnelle D. Hunt, Form 5 `0001175136-26-000001`** (DERA `:NDH:`): 607,352 → **18,326,528**.
  EDGAR's Table I is D "Common Stock" 607,352, I 17,477,085 "By J.B. Hunt, LLC", I 240,000 "Via
  Johnelle Hunt, LLC", D "Common Stock 401(k)" 2,091. The JBHT DEF 14A row (`def14a_unmatched`
  slice) reads 18,326,528 for her, an independent source with the same figure. It goes
  **newly oversubscribed**: insiders 21.9% + 13F institutions 87.0% + restated blockholders 9.7%.
  Before, the sum was 99.8% only because her holding read at 3% of its size. The DEF 14A slice is a
  non-additive memo overlay, so it is not the overlap.
  - Every line of this filing is `transactionFormType` 3, the shape #3891 gates in the XML
    writers because the date its amount carries is unsettled. The amount is not in question. The
    DERA path has no such gate (accepted residual below).
- **SUZ, Walter Schalka, Form 3 `0001628280-26-019002`** (XML): D "Common shares" 5,433,836 +
  "Restricted Shares" 121,351 = **5,555,187**, from the stored lines; the DERA twin carries the same
  two lines.

## Accepted residuals

- Everything #3889 accepted about the rule itself.
- **Nesting**, inherited from #3889: unequal lines of one class sum, so "Common Stock" 1,000 that
  already includes its "Common Stock (Restricted)" 200 reads 1,200. The instructions do not settle
  it and footnotes are not read. The lateral's comment said "provably distinct"; it now says the
  lines are separate Table I lines of one class.
- **DERA Form 5 holding lines** reach the lateral ungated: #3891's Form 5 gates (5/A, exit box,
  code-3 line) are in the XML writers only, and `:NDH:` rows carry no `TRANS_FORM_TYPE` (dev
  2026-10-19: 7,214 live `:NDH:` rows under a Form 5 manifest row and 115 under a 5/A). That
  predates this change, which sums the same lines one-title groups already sum. 45 of the 1,147
  newly summed groups are Form 5 by their manifest row and 0 are 5/A; 869 have no manifest row, so
  45 is the known count, not a complete one. JBHT is one, and its amount is right. A 5/A gate could
  read the manifest form; the code-3 gate needs `TRANS_FORM_TYPE` carried on `:NDH:` rows. Both are
  the DERA path's, out of this change.
- **Inherited evidence limits** on the DERA nature compaction (the alignment-qualified 3,787 of
  38,299; 35 of 49 on EDGAR) are unchanged; this change does not reintroduce a nature test.
- **ADS beside ordinary refuses** (27 XML groups): different words, and an ADS can represent
  several ordinary shares, so a sum would also need the ratio.
- **Joint filings** still refuse (one-owner condition); MNSO carry-forward is untouched.
- **Line identity stays exact** in the de-collision and amendment fragments (change 4).

## Acceptance

1. **Pure tests:** `_line_sum_if_distinct` / `form3_observation_shares` sums one-class titles,
   refuses two classes, keeps single-title verdicts, scopes the equal-amount check to the class;
   `_summable_scope` accepts a one-class Form 3 scope; `insider_ndh_title_class_params` omits a
   classless title and is injective.
2. **DB test:** the lateral sums a two-title one-class `:NDH:` group and refuses a Class A + Class B
   group, through `refresh_insiders_current`, `refresh_insiders_current_batch` and
   `_insiders_history`.
3. **The A/B**, recorded on the PR: `_current` diff over every `:NDH:` instrument, 0 falls from the
   sums, every de-collision move named, the rollup insider total per moved instrument, newly
   oversubscribed counted.
4. **After merge and the jobs reload:** `--apply`, then `--verify` (0 differences); a live re-check
   of one unified holder on `/instruments/<symbol>/ownership-rollup`; one EDGAR cross-check; the
   smoke panel.

## Security

No authorisation or input surface. The new arrays are bound parameters built from stored titles;
SQL stays parameterised.
