# #3227 — a Form 4's holding lines fold into the same filer's transaction observation

Status: spec (2026-10-18). Residual of #3227 named in
`2026-10-18-3227-form4-holding-rows.md` ("Mixed groups"). Rung: corpus change; this document is
the judgement artefact (Codex checkpoint 1).

## Problem

#3882 writes a Form 4 holding line (`insider_form4_holdings`) to an observation only when the
accession has no transaction line of the same filer and D/I. Where one exists, both writers keep
today's transaction value and ignore the holding lines:
- the latest transaction line's column 5; or,
- for `I`, the vehicle sum of `form4_observation_shares` when its conditions hold.

So a vehicle that the filing reports as a holding beside another vehicle's transaction drops out
of the observation.

| filing | key | today | the filing's other lines of that D/I |
|---|---|---:|---|
| SLSN `0000933430-25-000004` (Whitmore) | `I` | 49,043,149 | "By Grace Investments, LP" holding 675,515 |
| APO `0001858681-24-000036` (Kleinman) | `I` | 110,752 (HCM Series C) | nine holding vehicles: KRT Delaware 1,806,086, The Kleinman Children's Trust 702,496, HCM Series A 324,435, … |
| IP `0000051434-26-000058` | `D` | 26,092 (joint brokerage account, F1) | 55,634 "previously awarded … for service as a director" (F5) |

SLSN is a direct cross-check. Whitmore's Schedule 13D/A `0000933430-25-000005`, filed the same
day, states an aggregate of 49,826,805. Today's Form 4 figures sum to 108,141 + 49,043,149 =
49,151,290. Folding the Grace holding gives 108,141 + 49,718,664 = 49,826,805, which equals the
13D/A.

## Source rule

From the SEC Form 4 General Instructions (`https://www.sec.gov/files/form4.pdf`), as quoted
verbatim in the two #3227 Form 4 specs (`2026-10-17-3227-form4-vehicle-balance.md`,
`2026-10-18-3227-form4-holding-rows.md`):

- 4(c)(i): *"Report acquisitions or dispositions and holdings of non-derivative securities in
  Table I."*
- 4(a)(i): *"Report total beneficial ownership following the reported transaction(s) for each
  class of securities in which a transaction was reported."* Its note says column 5 *"should
  reflect those holdings reported or required to be reported by the date of the Form"*.
- 4(b)(iii): *"Report transactions in securities beneficially owned directly on a separate line
  from those beneficially owned indirectly. Report different forms of indirect ownership on
  separate lines."*
- 4(b)(iv): an indirect line may report the reporting person's proportionate interest or, at
  the filer's option, *"the entire amount of the entity's interest"*.

**What the rule settles:**
- A Table I holding line is a stated balance for its class and form of ownership, on its own
  line. It is the same kind of row whether or not another line of the filing transacts.
- A transaction line's column 5 is the balance after that transaction. A holding line states a
  balance with no transaction.

**What it does not settle:** whether a transaction line's column 5 already includes the balance
on a holding line (a running total, or a whole-entity restatement under 4(b)(iv)), or whether the
lines are separate blocks. The vehicle-balance spec measured both layouts among transaction
lines. So the fold is decided **by construction**, by the vehicle-balance spec's procedure and
refusal thresholds, extended to holding lines and frozen here. It is a policy, not a reading of
the instructions. It applies the #905 principle (separate Section 16 holdings sum) at the line
grain, as the Form 3 line sum and the holding-only sum already do.

## Full-population verification (dev, 2026-10-18, read-only)

Every count in this section comes from:

```bash
PYTHONPATH=. uv run python -m scripts.census_3227_form4_mixed_groups --examples 3 \
  --show 0001858681-24-000036 --show 0000933430-25-000004 --show 0001127602-25-003551
```

It was run at 2026-10-18 12:34Z, in one `REPEATABLE READ` snapshot, from the script as committed
in this PR (base `62c834e6`, after the #3882 backfill). It reads the stored lines
(`insider_form4_holdings`, `insider_transactions`, `insider_filers`), applies the Change
section's rule, and compares the result with the stored current observation (`known_to IS NULL`)
of the same accession, holder and nature.

**Cohort.**
- 23,929 mixed units `(instrument, accession, filer)` on live Form 4/4A filings, giving 24,155
  mixed keys `(unit, D/I)`.
- The mixed holding lines are 12,796 `D` and 43,613 `I`. That matches the holding-row spec's
  12,796 and 43,612, with one more `I` line since its run.
- 7 keys have no stored observation today. None of them sums.
- Of the mixed `I` lines, 7,078 share a normalised nature with a transacting form of the same
  filing (LGCY `0001493152-25-007246`: four "See Footnote" lines, one transacting); 36,535 do not.

**Verdict per mixed key** (the first failed condition, in the Change section's order):

| D/I | verdict | keys | instruments | 4/A | keys whose `_current` winner is this observation |
|---|---|---:|---:|---:|---:|
| D | 2 incomplete | 21 | 10 | 0 | 2 |
| D | 3 multi_title | 5,600 | 207 | 43 | 727 |
| D | 4 joint_or_owner_mismatch | 297 | 44 | 3 | 36 |
| D | 5a form_breaks_chain | 212 | 41 | 4 | 32 |
| D | 5b running_total | 502 | 55 | 7 | 81 |
| D | 5b holding_equals_transaction_balance | 76 | 16 | 3 | 15 |
| D | 5c running_total_across_filings | 4 | 4 | 0 | 0 |
| D | 6 equal_finals | 46 | 7 | 0 | 4 |
| D | **sums** | **2,275** | **142** | 43 | **310** |
| I | 2 incomplete | 33 | 16 | 1 | 2 |
| I | 3 multi_title | 1,751 | 236 | 17 | 225 |
| I | 4 joint_or_owner_mismatch | 1,774 | 274 | 19 | 270 |
| I | 5a form_breaks_chain | 491 | 189 | 7 | 58 |
| I | 5b running_total | 288 | 159 | 4 | 40 |
| I | 5b holding_equals_transaction_balance | 53 | 39 | 0 | 13 |
| I | 5c running_total_across_filings | 137 | 103 | 2 | 19 |
| I | 6 equal_finals | 1,432 | 192 | 5 | 166 |
| I | **sums** | **9,163** | **1,188** | 105 | **885** |

**Effect.**
- Against today's observation, the sums go up on 2,256 `D` and 9,149 `I` keys, stay equal on
  19 `D` and 14 `I` (the folded lines are 0), and go **down on 0**. A fall is impossible by
  construction (below).
- `_current` keys whose winner's value moves: **1,192 over 695 instruments**. Of these, 307 are
  `D` over 87 instruments and 885 are `I` over 631. The observation's `period_end` does not
  change, so no key changes winner here. The A/B, not this table, is the proof.

**Examples** (`--show`):
- **SLSN** `I`: 49,043,149 → 49,718,664, which matches the 13D/A (above).
- **APO** `I`: 110,752 → 3,531,785. HCM Series C's last balance plus the nine holding vehicles.
- **CHWY** `0001127602-25-003551`: `D` 820,752 → 1,981,342, and `I` 48,035 → 206,091. Each
  holding line is a footnoted RSU or PRSU grant (F3–F12), which the filer reports in Table I
  (residual below).

**A sample read against the filings' own footnotes.** These were picked at random from `sums`,
with seeds 7 and 11, using `/tmp`-only tooling and not committed. The D/I and nature are
`insider_transactions` / `insider_form4_holdings`, and the footnotes are
`insider_transaction_footnotes`. Each folded line is a separately footnoted block:
- WH: DSUs 20,338 + RSUs 3,742 + common 30,939.
- QRHC: F5 says the transaction balance "includes (a) 20,000 RSUs and (b) 92,585 shares". The
  holding is 75,609 DSUs (F6).
- TNL: common 181,107 + previously reported RSUs 74,481.
- CHD: PSUs 1,460 + four direct holdings.
- SBCF: the sold series 216,854 + joint 18,104 + Retirement Savings Plan 34,315.537 + IRA 9,356.
- CRGY: RSUs 19,669 + IRA 714,357.
- CDXS: Opaleye L.P. 9,605,000 + managed account 250,000.
- MDB: charitable foundation 80,462 + children's trust 490,896.
- CLBK (two filings): a deferral plan, 401(k), ESOP, SERP, stock awards.
- SCL: a living trust + ESOP II.

None of these reads as a total that includes another line. This is a sample, not proof (nested
blocks, residual below).

## Change

### 1. The rule

One helper decides each mixed key's value. Both Form 4 observation writers call it: the live
`_record_form4_observations_for_filing` and the weekly `derive_form4_observations`. A live-only
fix would be reverted by the weekly re-derivation (#3874).

- **Scope.** Form 4 and 4/A, and a unit whose filer CIK is non-blank after stripping.
- **Mixed key.** One filer's `D` (or `I`) in one accession on one instrument, where the unit has
  at least one non-derivative holding line of that D/I and at least one non-derivative
  transaction line of that D/I. As in #3882, a transaction line with no D/I counts as `D` here.
  Keys that are not mixed are untouched.
- **Forms.**
  - Transaction lines form series by `form4_form_key`, as today: the `D` lines are one series,
    and each normalised `I` nature is a series.
  - **Each holding line is a one-line form of its own, whatever its nature.** It has no
    transaction to chain, and 4(b)(iii) puts each form of ownership on its own line.
  - A shared nature text does not identify one vehicle: 7,078 of the mixed `I` lines share a
    transacting form's nature, such as LGCY's "See Footnote". A holding that restates a transacting
    vehicle's balance is refused by condition 5b below, not merged.
- **Conditions.** These are `form4_observation_shares`'s, applied to every non-derivative line
  of the unit, transactions **and** holdings, D and I. Every one must hold, and a NULL anywhere
  refuses:
  1. *(Holds by construction for a mixed key: it has a transaction form and a holding form.)*
  2. **Every line usable.**
     - A transaction line: as today.
     - A holding line: shares non-null and ≥ 0, `direct_indirect` in `D`/`I`, a non-blank
       title, and for `I` a non-blank normalised nature.
     - A transaction line with no D/I therefore refuses.
  3. **One security title** across the unit, exact match.
  4. **The accession's reporting owners are exactly the unit's filer.**
  5. The arithmetic does not contradict separate blocks:
     - (a) Within each transaction series, each line opens at the previous line's balance (as
       today).
     - (b) No transaction line opens at the non-zero balance of a line of another form, where a
       holding line is another form. A transaction that continues from a holding's amount means
       the holding is the same block before the transaction.
     - (b′) No holding amount equals a non-zero column-5 balance on any transaction line of the
       unit. This is #3882's guard (Instruction 4(b)(iv)), and it covers a holding that restates
       a transacting vehicle.
     - (c) As today, but over the **summed key's** transaction series: every `I` series for an
       `I` key, and the `D` series for a `D` key. The sources are unchanged: Form 4/4A
       transaction lines and Form 3/3A holding lines. A unit with no manifest time refuses when
       5c is reached.
  6. **No two non-zero final balances are equal** across all forms of the unit: the transaction
     series' finals and every holding line.
- **Value.** The sum over the summed key's forms: each transaction series' final balance
  (`txn_date DESC, txn_row_num DESC`), plus each of the key's holding lines.
- **Refused.** Today's value stands. That is the `I` vehicle sum where today's conditions give
  one, else the latest line. It is never a partial sum.
- **Never below today.** Conditions 2–6 range over a superset of the lines that today's `I` sum
  checks, and use the same 5c sources. So whenever the fold sums, today's `I` value was either
  that key's own transaction-series sum or its latest line (condition 1 failed). Both are terms
  of the fold. Every term is ≥ 0. For `D`, today's latest line is the `D` series' final. The
  census counts 0 falls.
- **Unchanged fields.** `period_end` stays the latest transaction line's `txn_date` for the key.
  `filed_at`, `source_document_id`, the holder name and the gates are each writer's as today.
  Only `shares` changes.

The `D` observation of a unit **without** holding lines stays the latest line, as today. The
vehicle-balance spec's "the `D` observation is unchanged in every case" was a scope decision for
transaction-only units. A `D` series is one form there, so it has nothing to sum.

### 2. The writers

- **Helper.** `form4_observation_shares` gains a `holdings` argument and a `direct_indirect`
  argument.
  - With no holding line of that D/I, it is today's function: `I` as today, and `D` out of scope.
  - With one, it applies the rule above. On a refusal, it returns today's `I` verdict and value.
  - The census's `fold_verdict` is the reference, and the A/B compares the two one for one.
- **Live writer.** It passes `parsed.holdings`, every line attributed to `filers[0]` as #3882
  does, matched to the unit by stripped CIK. It computes the `D` key as well as the `I` key.
- **Weekly writer.**
  - `_FORM4_HOLDINGS_SQL` adds `nature_of_ownership`.
  - Every unit's holding lines are passed to the helper. They are already fetched per accession
    chunk.
  - Unit selection is unchanged: a mixed unit already has an eligible transaction line.

No schema change, no parser change, and no re-parse: the holding lines are already stored and
backfilled (#3882).

### Unchanged

- Holding-only groups (#3882), Form 5, and the DERA ingest.
- `INSIDER_SUPERSEDED_BY_AMENDMENT_FILTER`, which matches transaction fields.
- **De-collision.** Every summed key already has an XML observation on its accession (the 7 keys
  with none all refuse), so clause 1 drops nothing new. Only shares change.

## Accepted residuals (not fixed here)

- **Refused mixed keys** (the table above): today's value stands.
  - Multi-title (5,600 `D` and 1,751 `I`) is the separate multi-title residual.
  - Joint filings (297 `D` and 1,774 `I`) have the same data-model limit as the vehicle sum.
- **Stock units in Table I.** 1,544 of the 25,949 holding lines folded into `sums` keys, on 870
  of the 11,438 keys, carry a footnote naming a unit: RSU, DSU, PSU, phantom or "stock unit".
  This is a regex over `insider_transaction_footnotes`, used as a measurement only and not in
  the rule (`/tmp` tooling, not committed).
  - Such units are arguably derivative securities. The filer reports them in Table I, and the
    observation records the attested Table I total, as #3882's holding-only sum and the Form 3
    line sum already do.
  - Transaction lines can carry units too (CRGY's RSU line, WH's DSU line). Today's value
    already holds them.
- **Nested blocks.** A holding line that includes a transacting series' balance without equalling
  any of its balances (a whole-person total) passes and double-counts. This is the residual the
  Form 3 line sum and the vehicle sum already accept.
- **5c sources exclude Form 4 holding lines.** An earlier Form 4 holding of the same form cannot
  clear a 5c match, and one of another form cannot trigger it. Widening the sources would also
  change existing transaction-only verdicts, so it is left for a measured slice of its own.
- **Dates.** A holding is dated by the form (#3882's rule 3), while the folded observation keeps
  the latest transaction date. Both describe the same filing's column 5 "by the date of the
  Form". The observation's `period_end` is left as it is, so winners do not move.
- **Amendments.** As in the vehicle sum: a 4/A that supersedes a summable base but itself fails
  a condition keeps one line or today's sum.
- **One-line classes and Form 5 holdings:** unchanged residuals of the multi-title and Form 5
  slices.

## Acceptance

Corpus-change evidence per `.claude/CLAUDE.md`, recorded on the build PR with SHAs:

1. **Unit tests**, both directions:
   - folds: APO (`I`, 3,531,785), SLSN (`I`, 49,718,664), IP (`D`, 81,726), and a same-nature
     "See footnote" holding beside a transacting form;
   - refusals:
     - 5b, a transaction opening at a holding's amount;
     - 5b′, a holding equal to a transaction balance;
     - 3, HEI's two classes;
     - 4, a joint filing;
     - 6, MXL's equal pairs;
     - 2, a transaction with no D/I and a value-only holding;
     - 5c on a `D` series.
     Each keeps today's value, including a today's `I` vehicle sum;
   - a unit with no holding line gives exactly today's verdicts (the vehicle-balance tests stay
     green); and Form 5 is unchanged.
   - A DB test drives both writers on one stored mixed filing and gets equal observations.
2. **Full-population A/B** over every Form 4/4A observation the weekly writer reaches. Report:
   - observations up, unchanged and down. Down must be 0. Up equals the census `sums` minus
     its equal ones, with any difference explained;
   - the helper's verdict equals the census's on every mixed key;
   - `_current` keys changed, from both refreshers;
   - rollups moved, and `ownership_history` points rewritten;
   - newly oversubscribed instruments (gate: 0, or each inspected).
3. **Smoke panel** (AAPL, GME, MSFT, JPM, HD): unchanged, or each move explained.
4. **Cross-source:**
   - SLSN against the 13D/A, 49,826,805;
   - against the EDGAR Form 4 rendering (sec.gov HTML): APO Kleinman, IP's two `D` lines,
     CLBK, and one 5b refusal.
5. **Backfill and verify.** Run `sync_insiders` with no `since` and no `LIMIT`, then the batch
   `_current` refresh. Each of these must show 0 differences:
   - every stored Form 4/4A observation against the writers' recompute;
   - stored `_current` against a fresh refresh.
6. **Live re-check:** `/instruments/SLSN/ownership-rollup`, where Whitmore's Form 4 total is
   49,826,805.

## Security

No authorisation surface: a derivation change on SEC public data, with no new endpoint or input.
