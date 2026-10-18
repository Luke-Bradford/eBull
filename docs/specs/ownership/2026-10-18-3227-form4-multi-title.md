# #3227 — a Form 4 unit whose security titles differ but name one class is read as one title

Ticket: #3227, multi-title residual (handoff 2026-10-17 22:40Z). The build is in the same PR.

## Problem

All three Form 4 observation helpers refuse at condition 3 when a filer's lines in one accession
carry more than one `security_title` (exact match):

- `form4_observation_shares` (the `I` vehicle sum);
- `form4_fold_shares` (holding lines folded into a transaction observation);
- `form4_holding_observations` (holding-only groups, `refused:multi_title`, which writes nothing).

A refused key keeps the latest line's balance, or writes no observation at all for a holding-only
group. Different titles are often one class: re-spellings ("Common  Stock"), par-value variants,
issuer-name prefixes, and plan, IRA, custody or restricted-share lines ("Common Stock 401(k)",
"Restricted Stock"). Each of those is a separate Table I line of the class, and condition 3
drops it.

## Source rule

From the Form 4 General Instructions (`https://www.sec.gov/files/form4.pdf`, read 2026-10-18):

- **3(b):** *"The title of the security should clearly identify the class, even if the issuer has
  only one class of securities outstanding; for example, "Common Stock," "Class A Common Stock,"
  "Class B Convertible Preferred Stock," etc."*
- **4(a)(i):** *"Report total beneficial ownership following the reported transaction(s) for each
  class of securities in which a transaction was reported."* So classes never sum.
- **4(b)(iii):** *"Report transactions in securities beneficially owned directly on a separate line
  from those beneficially owned indirectly. Report different forms of indirect ownership on
  separate lines."*

**What the rule settles.** The title is where the class is stated. Two lines are the same class
when their titles state the same class.

**What it does not settle.** Which words of a free-text title state the class.

**Structured-field check** (the text-classifier rule):
- `<securityTitle>` is free text. A Table I line carries only `footnoteId` links, with no CUSIP
  or class code. Codex checkpoint 1 also found none in the ownership technical specification.
- `sec_cover_12b_pairs` (10-K cover title → symbol) covers 50 of the 172 instruments the 10-17
  handoff measured, and maps the issuer's cover titles, not a filer's line titles.

So the reading is **fixed by construction** and frozen in `FORM4_TITLE_CLASS_VERSION`. The hash
covers the dropped-word list and the source of both functions, and a test pins it.

## Change

### 1. The class a title is read as (`form4_title_class`)

The key is the title's own words, in order, minus only words that say where or how a share is
held, never which class it is. Any word the list does not explain stays, so the title cannot match
one that lacks it. This is a whitelist. Codex round 1 showed that a blacklist treats unknown words
("Prfd", "II", "(A)", "Cl. B") as irrelevant. The constructed collisions from rounds 2 and 3 are
refused, and they are in the test's `_REFUSE`.

1. NFC-normalise and lower-case the title.
2. Remove a **par-value phrase whole**, with its amount ("$.10 par value", "par value $0.01 per
   share", "no par value"). A trailing amount needs a "$". "No", "without" and numbers
   elsewhere stay, so "…par value 6% dividend" keeps its 6.
3. "class X-1" becomes the single word `class:x-1`, with its punctuation kept, so B-1 is not B1.
4. "401(k)" / "401k" / "(401)k" / "403(b)" become one dropped plan word, and "profit sharing
   plan" becomes "plan". The letter is required, so a bare "401" stays. A 401k after "class" or
   "series" stays, since it is an identifier.
5. Drop these words (`_TITLE_DROPPED_WORDS`):
   - the equity noun: `stock`, `share(s)`;
   - `restricted`, `unvested`, `vested`. `restricted` is kept when the title says "voting",
     because Canadian restricted voting shares are a class;
   - plan, account and custody names: `ira`, `roth`, `sep`, `esop`, `espp`, `sip`, `psp`,
     `drp`, `drip`, `savings`, `retirement`, `plan`, `book`, `entry`, `street`, `name`, `direct`,
     `indirect`, `account`;
   - ownership wording: `by`, `held`, `in`, `with`, `of`, `the`, `and`, `spouse`, `joint(ly)`,
     `owned`.

   The remaining words are kept in order. Only a base word is de-duplicated (the defined term
   "(the "Common Stock")"). Any other repeat stays, so "Non-Voting Non-Participating" keeps
   both. A token is Unicode `\w` with digits joined to it by `-`, `.`, `/` or a dash kept in it,
   so `a-1` ≠ `a/1` and `1/2` ≠ `1.2`.
6. `None` when the title has no equity noun and no base word (`common`, `ordinary`, `capital`). A
   title whose only base is its noun ("Restricted Stock") reads as `common`.

**Kept words, so these refuse:**
- any word naming a class or security: class letters wherever they appear ("Bio-Rad A", "(A)",
  "II"), `units`, `series`, `preferred`, `preference`, `depositary`;
- plan-unit words: `serp`, `sua`, `deferral`;
- other qualifiers: `estimated`, `opening balance`, `employee`, `trust`, `family`, `dividend`,
  `no`, `without`;
- years ("2024 Annual RSUs", "(2019 SIP)") and typos ("Commom");
- vehicle names.

### 2. Condition 3 (`form4_title_match`)

- **One distinct title:** the test is equality, which is the rule today. A single-title unit's
  verdict cannot change.
- **Several titles:** each must have a class, and all the same one. "Same title" then means
  "same class" wherever a helper compares titles:
  - the holding-only group's equal-amount and equal-to-transaction-balance checks;
  - condition 5c's earlier balances. `form4_earlier_balances` now selects every title of the
    filer × instrument and filters in Python.

  Every other condition is unchanged.

**Monotonicity, per helper:**
- **`form4_observation_shares`:** only units refused at 3 today are newly admitted. Their sum's
  terms are the final balances of the `I` forms, one of which is the writer's latest `I` line.
- **`form4_fold_shares`:** the same holds. Where the vehicle sum passed and the fold refused at
  3, today's value is the vehicle sum: the `I` forms' finals, which are a subset of the fold's
  terms.
- **`form4_holding_observations`:** a group that writes today has one title, or a single line,
  so its comparisons are unchanged. A multi-title group wrote nothing before.

No value falls. The A/B measures this over the population.

### 2b. The rollup reads the same test

`_INSIDER_LINE_COMPONENTS_SQL` (#3886) decides whether a stored sum's Table I lines are its
components. It required `count(DISTINCT title) = 1`. A multi-title sum would have had no
components, and the control-group collapse would stop matching it, which is #3886's failure mode.

The query now returns each scope's titles, and `_summable_scope` applies `form4_title_match` in
Python, so the read path and the writers agree:

- a Form 3 scope (any `insider_initial_holdings` line) keeps exact equality, as
  `form3_observation_shares` does;
- a Form 4 holding group's titles are stripped, as `form4_holding_observations` strips them.

### 3. Writers and backfill

Both Form 4 writers already call these helpers (#3874), so no writer code changes.

Backfill is `scripts/ab_3227_form4_multi_title.py --apply`, the mixed-groups procedure over this
cohort:
1. replay the cohort through `upsert_filing(refresh_current=False)`, which rewrites share-class
   siblings;
2. run a full `sync_insiders`;
3. batch-refresh `_current`.

`--verify` follows.

## Full-population verification (dev, 2026-10-18)

Run: `PYTHONPATH=. uv run python -m scripts.ab_3227_form4_multi_title` (stage 0 read-only; stage
1 one transaction, rolled back).

**Cohort:** 13,056 units (instrument, accession, filer) with more than one title, over 836
instruments. Of these, **3,854 units over 325 instruments** are read as one class.

- The script prints each key and **every** unified title set; all 232 were read. They are
  re-spellings and whitespace or case variants, par-value variants, issuer-name prefixes, and
  plan, IRA, custody and restriction lines.
- One kind is not a plain re-spelling: differing par values in one filing ($0.00001/$0.0001,
  $0.001/$0.01, $0.01/$0.10, $.10/$10, no par/$0.01). It is accepted below.
- 229 of the 232 were among the 252 read on the previous revision. The 3 new ones are par-phrase
  typos that the whole-phrase removal now absorbs: "par valu", "per shre", "$1par".
- Round 3's fixes changed no stored unit: the same 232 sets and the same counts below.

**Stage 0** (the weekly writer on this branch against stored observations):

| | observations |
|---|---:|
| cohort transaction, up | 1,618 |
| cohort transaction, unchanged | 11,262 |
| cohort holding-only, added | 313 |
| cohort holding-only, unchanged | 4,257 |
| **down, anywhere** | **0** |
| outside the cohort, moved | 1 (the #3884 BN.US mislink, not this change) |
| in-retention stored cohort observations no longer produced | 0 |

- Rises and additions total 345,083,028 shares.
- **Why stored is the control.** The 10-18 mixed-groups `--verify` (#3227 comment, 17:28Z) found
  stored equal to main's writer on every observation but #3884. Since then the only drift is
  `filed_at` on 4 observations of instrument 1049330, whose manifest arrived after recording; no
  shares changed.
- The 427 cohort observations outside the weekly writer's retention are reached by neither arm.
  Stage 1's live replay covers them.

**Stage 1** (live-writer replay of all 13,056 accessions, rolled back):

| | |
|---|---:|
| observations up / added / **down** | 1,621 / 382 / **0** |
| share-class sibling observations (3,225) | 1 up, the rest unchanged |
| `_current`: per-instrument vs batch refresher disagreements | 0 |
| `_current`: same winner, shares up | 192 |
| `_current`: winner changed | 31 |
| `_current`: added | 6 |
| `_current`: instruments moved | 61 |
| insider rollup total up / **down** | 36 / **0** |
| control-group collapses releasing shares | **0** |
| newly oversubscribed | 0 |
| smoke panel (AAPL, GME, MSFT, JPM, HD) moved | none |
| replay failures | 0 |

- **Winner changes** are a newer accession whose summed value now exceeds the old winner's, or a
  newly written XML observation superseding its accession's DERA `:NDH:` row (EXC, JBHT).
- **The 11 "same winner, other field" rows** are not title-related. The replay realigns stale
  sibling `filed_at` values to the manifest: BBDO.US `0001292814-26-004756` went from 10-01 00:42
  to the manifest's 09-30, which the entity row already carries. A rolled-back refresh of the same
  instruments with no replay shows 0 differences.

**Cost of the wider 5c query.** Lines per (instrument, filer), all titles: p50 6, p99 109, p99.9
455, max 4,150.

**Cross-source (EDGAR XML, fetched 2026-10-18).** EDGAR's Table I equals the stored lines in both
cases:
- **TTD `0001671445-26-000006`** (Jeffrey T. Green). The `I` value goes from 84,638 to
  **7,005,539**: the Jeff Green Trust 84,638 (transaction, F2), plus the Family Foundation 920,901
  (holding, F3), plus the limited partnership 6,000,000. The LP line is titled "Class A  Common
  Stock" with a double space, which was the only reason it refused.
- **GEO `0000950170-25-111955`** (George C. Zoley). D "Common Stock" 4,006,785 + "Restricted Stock"
  50,000 = **4,056,785**. Today there is no D observation. The filing's two footnotes concern the
  trusts; neither says the common line includes the restricted shares.

## Accepted residuals

- **The rule is a reading, not a proof of class identity.** Instruction 3(b) puts the class in the
  title. The dropped words are this rule's judgement of what never names a class. The A/B shows
  the arithmetic over the population and every set it unifies, but zero falls cannot detect a
  false rise. The guard is the whitelist plus the full read of the 232 sets.
- **Nesting (a decision, by policy).** A line that already contains another, unequal line passes
  ("Common Stock" including its "Restricted Stock"). The guards (5b, 5b′, 6, equal amounts) refuse
  only equal amounts. This is the vehicle-sum and mixed-groups policy: a separate Table I line
  counts as a separate holding unless the filing's arithmetic contradicts it. The instructions do
  not settle it. 4(b)(iv) allows a whole-entity amount, and 6(a) allows footnotes, which this rule
  does not read. GEO's footnotes are silent, which is absence of evidence.
- **Differing par values** within one filing are read as one class, since the par phrase is
  removed whole. A class distinguished only by par value is not separated.
- **A bare equity noun reads as common** ("Restricted Stock", "Class A Shares", "Shares").
- **`restricted` is read as a vesting state** unless the title says "voting". A class whose
  restriction is a right ("Restricted Dividend Rights", or a "Class A Restricted Ordinary" beside
  "Class A Ordinary") is not separated. These are constructed by Codex round 3, not found in the
  census.
- **Refused today and still refused:**
  - a classed and an unclassed spelling;
  - differently ordered words ("Common Class A" / "Class A Common Stock");
  - typos with different words;
  - differing issuer prefixes;
  - family or trust vehicle names in the title;
  - year-tagged plan lines;
  - any unit, preferred or plan-unit line beside common.

  Picking the instrument's own class would need a class→instrument map.
- **Share-class siblings** receive the entity's value, as they already do. A unit's class is not
  matched to a sibling's class.
- **Inherited holding-group behaviour** (not changed here):
  - a one-line group skips the multi-line checks;
  - an equal-amount pair elsewhere in the class refuses a group;
  - a single-title group compares only its exact spelling.
- **Form 3, the DERA `:NDH:` line sum and Form 5** keep exact-title matching.

## Acceptance

1. **Pure tests** (`tests/test_3227_form4_multi_title.py`):
   - unify and refuse sets, including Codex's constructed collisions from rounds 1 and 2;
   - single-title equality;
   - each helper admitting one-class titles and refusing two classes;
   - 5c receiving the class test;
   - a multi-title holding group refusing an equal transaction balance;
   - a one-title group unaffected by other spellings.
2. **DB tests:**
   - `tests/test_3227_form4_multi_title_db.py`:
     - `form4_earlier_balances` filters by the exact title or the class test, and never returns
       Class B;
     - both writers write the same summed holding observation;
   - `tests/test_3886_line_component_collapse_db.py`: a one-class two-title unit has its line
     components.
3. **The A/B**, stages 0 and 1, recorded on the PR.
4. **After merge and the jobs reload:**
   - run `--apply`, then `--verify`, which reads 0 differences except #3884;
   - live re-check `/instruments/TTD/ownership-rollup` for Green's indirect at 7,058,449, the
     later of the two TTD rises, if it is his winner;
   - run the smoke panel.

## Security

No authorisation or input surface. SQL stays parameterised. The two changed queries drop a
title predicate and return title columns instead.
