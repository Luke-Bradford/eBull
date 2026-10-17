# #3227 — a Form 4's indirect observation sums its vehicles' balances

Status: spec (2026-10-17). Residual of #3227 item 3 ("MNSO line-level supersession"), re-scoped
by measurement. Rung: corpus change; this document is the judgement artefact (Codex checkpoint 1).

## Problem

Both Form 4 XML observation writers emit one `ownership_insiders_observations` row per
`(filer_cik, direct_indirect)`, holding the column-5 balance of that group's LATEST line
(`txn_date DESC, txn_row_num DESC`):

- `app/services/insider_transactions.py::_record_form4_observations_for_filing` (live path);
- `app/services/ownership_observations_sync.py::sync_insiders` (weekly re-derivation;
  `DISTINCT ON (accession, filer_cik, filer_name, direct_indirect)`).

When a filer reports several forms of indirect ownership on one Form 4, **and column 5 carries
each form's own balance**, that latest line is one vehicle's balance and the others are
dropped. Examples (dev, 2026-10-17):

| accession | writer keeps | the filing's vehicles |
|---|---:|---|
| WOOF `0000950170-25-048788` | 7,874 ("By son") | By 2020 Trust 853,304 · By 2025 Trust 724,910 · By son 7,874 → 1,586,088 |
| WLTH `0001231919-25-000630` | 1,598,899 | DAG Ventures IV 1,167,469 · IV-QP 11,047,106 · IV-A 1,598,899 → 13,813,474 |
| MNSO `0001104659-26-069242` | 315,740,482 | Mini Investment 315,740,482 · YGF MN 195,115,382 → 510,855,864 |

## Source rule

From the SEC Form 4 General Instructions (`https://www.sec.gov/files/form4.pdf`, read
2026-10-17), verbatim:

- 4(a)(i): *"Every transaction must be reported even though acquisitions and dispositions are
  equal. Report total beneficial ownership following the reported transaction(s) for each class
  of securities in which a transaction was reported."*
- The note to 4(a)(i): *"The amount of securities beneficially owned following the reported
  transaction(s) specified in Column 5 of Table I and Column 9 of Table II should reflect those
  holdings reported or required to be reported by the date of the Form. Transactions and
  holdings eligible for deferred reporting on Form 5 need not be reflected in the month end
  total unless the transactions were reported earlier or are included on this Form."*
- 4(b)(iii): *"Report transactions in securities beneficially owned directly on a separate line
  from those beneficially owned indirectly. Report different forms of indirect ownership on
  separate lines."*
- 4(b)(iv): an indirect amount is *"the reporting person's proportionate interest … Alternatively,
  at the option of the reporting person, the entire amount of the entity's interest may be
  reported."*

What the rule settles:
- The class total of reported holdings, as of the form's date, must be reported.
- The lines are split by form of ownership.

What it does not settle is how that total is laid out across the per-form lines' column 5.
Filers do it two ways (measured below):
- each line carries its own form's balance as of the form, so the class total is the sum of the
  forms' last balances;
- each line carries a running total, so the last line is the class total.

So which way a filing does it is decided **by construction**: a decision procedure over the
filing's own arithmetic and the filer's earlier filings, under the conditions below. It does not
prove the filer's layout. It refuses wherever the arithmetic contradicts the per-form reading.
The procedure, and every refusal threshold in it, is a policy choice frozen here, not a reading
of the instructions. It applies the #905 principle (separate
Section 16 holdings sum) at the line grain, as #3227 item 3a did for Form 3
(`form3_observation_shares`, `INSIDER_HOLDING_LINE_SUM_LATERAL`).

## Full-population verification (dev, 2026-10-17, read-only)

Every count in this document comes from:

```bash
PYTHONPATH=. uv run python -m scripts.census_3227_form4_vehicle_balance --examples 5
```

It was run at 2026-10-17 23:06Z, in one `REPEATABLE READ` snapshot, from the script as
committed in this PR (base `2d91b41b`). The named-filing arithmetic was read from
`insider_transactions` for those accessions. GRMN and BROS were also checked against their
EDGAR XML (below).

The script applies the conditions below one at a time, in order, to every Form 4 and 4/A in
`insider_transactions`. Two terms used throughout:
- A **form** is an `I` line's normalised nature (ASCII alphanumerics only, lower case; empty
  means blank). The `D` lines are one further series.
- A line's **opening balance** is `post − signed shares`: `A` adds, `D` subtracts.

**Both layouts exist:**
- MNSO's lines chain only within a form: Mini 314,290,482 + 1,300,000 = 315,590,482, then
  + 150,000 = 315,740,482.
- Abdiel `0000950170-23-052960` and Pentwater `0000902664-26-002383` chain ACROSS forms.
  "Abdiel Qualified Master Fund" ends at 13,586,608; the next line, "Abdiel Capital LP", sells
  305 → 13,586,303.

So unconditional summing is wrong.

There are 425,156 `(instrument, accession, filer)` units. 5,299 of them are candidates, with `I`
lines under more than one form, over 1,022 instruments; 0 candidates have a null or blank CIK.
Each candidate is reported under its first failed condition:

| verdict | groups | instruments | live XML `indirect` winner keys | winner instruments |
|---|---:|---:|---:|---:|
| 2 incomplete | 16 | 12 | 4 | 4 |
| 3 multi_title | 293 | 107 | 80 | 56 |
| 4 joint_or_owner_mismatch | 1,032 | 179 | 170 | 129 |
| 5a form breaks its own chain | 206 | 115 | 46 | 43 |
| 5b running total in the filing | 336 | 168 | 82 | 78 |
| 5c running total across filings, or no filing time | 143 | 72 | 22 | 22 |
| 6 equal finals | 482 | 143 | 81 | 78 |
| **sums** | **2,791** | **678** | **534** | **424** |

In the `sums` verdict:
- The vehicle sum is greater than today's value on 2,354 groups, equal on 437 (the other vehicles
  are at 0), and below it on 0. 37 of the sums are 4/A.
- 1,360 have one line per form (D included), so they have no within-form chain to test.

**How openings relate to earlier filings, over every candidate.** This is a heuristic, not a
proof. If column 5 is per form, a form's opening balance should equal some earlier balance of
the same form for the filer, in any earlier filing (5c's sources). Over all candidates' `I`
forms with a non-zero opening (12,729):
- 7,304 equal an earlier balance of only the same form;
- 1,419 equal both a same-form and an other-form balance;
- 265 equal only another form's balance, which is running-total evidence and refused as 5c;
- 3,741 equal nothing earlier that we hold;
- 182 have no filing time and are refused as 5c.

Within `sums` (5,805 forms): 3,782 match only the same form, 125 match both, and 1,898 match
neither. The 0 other-only matches there follow from the verdict, so they are not independent
evidence.

## Change

One helper, `form4_observation_shares`, in `insider_transactions.py`, called by both writers.
This is the #3874 lesson: `sync_insiders` re-derives observations weekly, so a live-only fix
would be reverted.

**Scope.** Form 4 and 4/A.
- A 4/A is in scope. Instruction 9 makes it partial, but its own lines obey the same per-form
  arithmetic. Leaving it out would let a full-restatement 4/A that
  `INSIDER_SUPERSEDED_BY_AMENDMENT_FILTER` prefers over its base (which matches on transaction
  fields, not balances) replace a base's sum with one line.
- Form 5 keeps today's value; its instructions are not cited here.

**Unit.** One filer's non-derivative lines in one accession on one instrument (CIK stripped of
whitespace), D **and** I lines, including those the writers skip today. Tombstoned filings are
out.

For the `I` observation, the helper returns **today's value** unless every condition holds.
- Today's value is what each writer computes now, by its existing filter and order, unchanged.
  The live writer takes the latest line with a balance and a valid, non-null date. The weekly
  writer takes a valid date with nulls last, and its null-date difference stays as it is.
  Where today there is no `I` observation, there is still none.
- When every condition holds, the helper returns the sum over `I` forms of each form's latest
  balance (`txn_date DESC, txn_row_num DESC`).

"Usable" in condition 2 is a stricter, separate definition, used only to decide whether a sum
may be taken.

1. **More than one `I` form**, blank counted as a form here so that it fails condition 2. The
   `D` observation is unchanged in every case. That is a scope decision, not a measured
   property.
2. **Every line in the unit is usable.** Each condition below must be TRUE; a NULL anywhere
   refuses:
   - balance and shares non-null and ≥ 0, and the opening balance (`post − signed shares`) ≥ 0;
   - an `A`/`D` code;
   - a non-null `txn_date` with `txn_date_invalid` false;
   - `direct_indirect` in `D`/`I`;
   - a title not matching `^[[:space:]]*$`;
   - for `I`, a non-blank normalised nature.

   The Python and SQL forms share these definitions. Because every term is ≥ 0 and today's value
   is one of the terms, the `I` value never falls below today's.
3. **One security title across the whole unit, D lines included.** Exact match: classes never sum
   within the unit, and a re-spelled title refuses (the multi-title residual is separate).
4. **The accession's reporting owners are exactly the unit's filer.** The distinct trimmed
   `insider_filers.filer_cik` set equals `{unit CIK}`. A joint filing refuses: the parser assigns
   lines to one owner (`form3_observation_shares` notes the same for Form 3), so a sum could
   credit one co-filer with the others' vehicles. This is a data-model limitation, not an absence
   in the source; Instruction 4(b)(v) gives each joint filer's information.
5. **The arithmetic does not contradict per-form column 5:**
   - (a) Within each form, every consecutive line (by `txn_row_num`, unique per accession and NOT
     NULL) opens at the previous line's balance. This refuses interleaved sequences whose
     arithmetic breaks, and block-balance layouts where lines repeat one total. It does not prove
     that one nature is one vehicle: two vehicles under one nature whose lines happen to chain
     are summed as one, which under-counts.
   - (b) No line opens at the non-zero balance of a line of ANOTHER form (`D` included) in the
     same filing, in either row order. Row order is not proven chronological, so both orders
     count. A coincidental equality refuses a genuine per-form filing, to today's value. A
     zero-balance opening is ignored, because its zero term adds nothing.
   - (c) No `I` form's first line opens at a non-zero balance that equals another form's
     balance in an earlier filing by the filer, and no earlier balance of its own form. This is
     a running total carried across filings, and the check works as follows:
     - **Sources** are lines on the same instrument and of the unit's title. Each is a
       non-derivative, non-tombstoned Form 4/4A transaction line or Form 3/3A holding line, with
       `direct_indirect` in `D`/`I`. "Another form" means a `D` line or an `I` line of another
       nature. "Earlier" means `sec_filing_manifest.filed_at` strictly earlier.
     - A unit whose filing has no manifest time cannot be checked, so it refuses.
     - A same-form match clears an other-form one. That is a policy choice: an old balance
       later corrected still clears. The live writer runs this as one indexed query
     (`insider_transactions (instrument_id, filer_cik)`, `insider_initial_holdings (filer_cik,
     instrument_id, …)`) only for units that pass 1–5b.
6. **No two final balances are equal and non-zero**, across the `I` forms and the `D` series.
   This is a by-construction guard for the Instruction 4(b)(iv) shape: one block reported under
   an entity and again under its owner. It is extended across D/I as `form3_observation_shares`
   does. The cost is accepted, as with 3a's LYEL. Equal gifts to sibling GRATs
   (`0000950142-24-002955`: 50,000 + 50,000) keep a single line.

What every result keeps:
- Any failed condition keeps today's value, never a partial sum.
- `period_end` stays the latest `I` line's `txn_date`, and `source_document_id` stays the
  accession.

The sum adds each form's last-reported balance. Per the 4(a)(i) note, column 5 reflects holdings
"by the date of the Form", so a form not transacted after Monday still holds Monday's balance on
Thursday, except for holdings deferred to Form 5, which the note lets a filer leave out. This
slice does not correct that omission.

**The weekly writer must be restructured; a helper call alone is not enough.** Today's query
`DISTINCT ON`s to one row before Python sees it, and omits the fields the conditions read.
- **Select units first.** It selects the `(instrument, accession, filer_cik)` units in scope;
  `since` and `LIMIT` apply to units. It then loads every non-derivative line of those units.
  A `since` filter on line dates would cut a filing's earlier lines and rewrite a correct sum
  to a partial one.
- **Group on the stripped CIK.** `filer_name` leaves the grouping key. The name stays what each
  writer stores today: the latest line's `filer_name` (weekly) and `_primary_filer_name`
  (live).
- **Keep the rest as today.** A null or blank CIK keeps today's grouping and value (0
  candidates now). The retention gates are unchanged.

**Readers.** The A/B, not this document, is the proof for readers. Checked in code:
- `_INSIDER_DUAL_PIPELINE_DECOLLISION` clause 1 is accession-level.
- Clause 2 compares only `form3` winners' shares with `:NDH:` lines.
- `INSIDER_SUPERSEDED_BY_AMENDMENT_FILTER` compares `insider_transactions` fields.
- `INSIDER_HOLDING_LINE_SUM_LATERAL` fires only on `:NDH:` winners.

None compares a bare-accession Form 4 observation's shares with a line. The refreshers, the
history reader and the rollup read the observation's `shares`; the A/B diffs all of them.

## Accepted residuals (not fixed here)

- **Vehicles the filing does not mention.** The sum covers only this Form 4's vehicles. MNSO's
  other three Form 3 vehicles (14,000,000 · 8,936,000 · 257,849,197) stay dropped: the
  2026-06-01 Form 4 outranks the 2026-03-18 Form 3 on the `indirect` key.
  - `mnso_shape` counts 697 live XML `indirect` keys over 506 instruments whose owner filed a
    multi-vehicle Form 3 earlier. That is a candidate population, not a verified loss.
  - Carry-forward across filings is the next slice. The 8,723 same-form opening matches (7,304
    same-form only + 1,419 matching both) are
    where its identity evidence starts. They are not yet a key: equal balance plus a normalised
    name can coincide.
- **Running totals that leave no trace.** A running total passes only if no line opens at
  another form's balance, in this filing or an earlier one. 1,360 `sums` groups have one line
  per form, and 1,898 `sums` forms match nothing earlier. Both rest on 5b and 5c alone. So
  does a constructed case such as A `+5 → 105, +5 → 110`, B `+2 → 132, +3 → 135`, where the
  jump was reported elsewhere. The
  cross-source checks below sample exactly these.
- **Under-counts the rule leaves:**
  - two vehicles under one nature whose lines chain (5a);
  - coincidental refusals (5b, 5c);
  - equal GRATs (6).

  Each is today's value or more, never an over-count against today. ⚠ That bound is against
  today's observation, not against the truth. If an `I` balance already includes the direct
  holding (a whole-person total), a refusal keeps a value the rollup still adds to `D`, which
  is today's behaviour.
- **Amendment supersession does not preserve a sum.** `INSIDER_SUPERSEDED_BY_AMENDMENT_FILTER`
  matches transaction fields, not balances. So a 4/A that supersedes a summable base but itself
  fails a condition replaces the base's sum with one line. That is the same value as today.
- **Table I holding rows on a Form 4** (`nonDerivativeHolding`) are not stored in
  `insider_transactions`, so completeness (condition 2) is over the transaction lines. A
  holding-only vehicle is neither summed nor refused.
- **DERA `:NDT:` rows**: 80,004 of the 173,843 Form 3/4 `_current` rows. They carry no D/I,
  nature or title (`sec_insider_dataset_ingest.py:759-761` reads `SHRS_OWND_FOLWNG_TRANS` and
  the SK only), so the same collapse there needs an ingest change first. ⚠ DERA nature is
  positionally compacted on `NONDERIV_HOLDING` (#3875); check `NONDERIV_TRANS` for the same
  defect before relying on it.
- **Co-presence with a DERA row.** 62 `sums` XML winner keys belong to a holder who also has a
  DERA `_current` row on the instrument. In 9 of them it is a live `:NDH:` `I` line on the
  additive `direct` relationship key: the MNSO shape (`:NDH:` Mini Investment 314,290,482 beside
  the Form 4's Mini 315,740,482).
  - Summing adds the OTHER vehicles. It enlarges that duplicate only where an added vehicle
    also sits in the DERA row.
  - Co-presence does not prove overlap; the A/B lists all 62 with the rollup before and after.
- Unequal lines that overlap through nested entities: the class #905 already accepts.

## Acceptance

Corpus-change evidence per `.claude/CLAUDE.md`, recorded on the build PR with SHAs:

1. **Unit tests, one per condition and both directions:**
   - shapes that sum: WOOF, MNSO, a 4/A, and a `since` window starting mid-filing, which must
     still give the whole-filing sum;
   - shapes that refuse: Abdiel's in-filing running total, a reverse-order running total, a
     cross-filing running total (5c), two "By Trust" sequences that break the chain, block
     balances, equal finals across D/I, a NULL A/D code, a whitespace-only title, a joint
     filing, an owner-set mismatch, and a Form 5.
   - A unit with no `I` line that today's filter accepts produces no `I` observation.
   - A DB integration test drives the live writer (`_record_form4_observations_for_filing`,
     including its 5c query) on a stored filing, separately from the weekly writer.
2. **Full-population A/B.** Recompute every Form 4/4A observation the weekly writer reaches and
   diff it against stored. Report:
   - observations up / unchanged / down (down must be 0);
   - `_current` keys changed and rollups moved;
   - newly oversubscribed instruments (gate: 0, or each inspected);
   - the 62 DERA co-presence keys separately;
   - every rewritten observation's `ownership_history` point, and not only the moved `_current`
     keys, since a rewritten non-winning filing still moves the chart;
   - `_current` from both refreshers.

   The comparison population is the census's, minus units outside the weekly writer's
   retention and attribution gates, counted separately. On it, the helper's verdicts equal the
   census verdicts one for one.
3. **Smoke panel** (AAPL, GME, MSFT, JPM, HD): unchanged, or each move explained.
4. **Cross-source against the EDGAR Form 4 text:**
   - WOOF and WLTH;
   - the census's five single-line-per-form `sums` examples (BROS, GRMN, PSMT, CGBD, NSP);
   - two `5b` refusals (SHEN, ULH).

   Already checked, 2026-10-17: GRMN Kao "By Children" 12,025,649 + "By M&F Trust" 6,267,896
   (= 18,293,545), and BROS's two 10b5-1 aggregators, 698,718 + 416,277 (= 1,114,995), match
   EDGAR. Neither chains as a running total.

   Any `sums` example whose EDGAR column 5 reads as a running total blocks the build until the
   rule is revised.
5. **Backfill:** `sync_insiders` with no `since` and no `LIMIT`, run until every eligible unit
   has been processed, then the batch `_current` refresh. Acceptance needs both of these, with 0
   differences each:
   - every eligible stored Form 4/4A observation equals the helper's recompute;
   - stored `_current` equals a fresh recompute.

   Observations outside the retention gates are not rewritten; the A/B counts them.
6. **Live re-check:** `/instruments/WOOF/ownership-rollup` after the backfill.

## Security

No authorisation surface: an ingest-derivation change on SEC public data, with no new endpoint
or input.
