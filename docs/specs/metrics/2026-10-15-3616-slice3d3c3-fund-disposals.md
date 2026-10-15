# Non-reporting fund disposals (#3616 slice 3d-3c-3)

Source: #3616 slice 3d-3c, as 3d-3c-2 left it (`2026-10-14-3616-slice3d3c2-fund-determination.md`, "Not in this
slice"): "non-reporting fund disposals: identification, the basic gain, its character, and how a figure resting on
assumptions and an unresolved character is published". 3d-3a withheld every fund (`exchange_traded_fund`) and listed
what this needs (3d-3a spec, "Not in this slice"): relevant-securities identification, separate disposals before
reg 42's floor, and OIG routing.

**What this slice decides and builds.**
- A fund security whose reporting status (3d-3c-2 Design 3) is `non_reporting_assumed` at the date of every one of its
  admitted lots and at its holding date gets the character **`non_reporting_fund`** instead of being withheld
  (Design 1).
- Its lots are identified as *relevant securities*: no same-day rule, no s104 pool; each disposal first with
  acquisitions in the following thirty days, earliest first, then with earlier acquisitions, latest first (Design 2).
- Each disposal's TCGA gain is computed separately; its **basic gain** is that gain floored at nil (regs 39, 42), and
  its **character** is recorded: a loss or nil is capital whatever the fund's transparency; a gain is an offshore
  income gain only if the fund is opaque, and otherwise `oig_or_capital_unresolved`, published with no income or
  capital split (Design 3).

Shares are unchanged. Nothing is shown to the operator (3d-4).

**The outcome on dev today (evidence run 7):** SPY becomes `non_reporting_fund`. Its one disposal (2026-09-23,
0.29615500 units) is identified with the 2026-09-30 acquisition under the thirty-day rule: computed and basic gain
**£1.10443720572584**, character `oig_or_capital_unresolved`, resting on 3d-3c-2's A1–A9 and this slice's F1–F5.
3d-2 identified it under the share rules (£0.86063438888195); that was the wrong rule for a non-reporting fund (3d-3a)
and is superseded. IUSA.L and CSPX.L stay withheld.

## Facts established first

"Script" is `scripts/measure_3616_slice3d3c3_fund_disposals.py`, read-only, run on 2026-10-15. Output:
`var/research/3616/slice3d3c3/population.txt` (loop worktree; dates, amounts, symbols and lot keys, no account
identifier). In one REPEATABLE READ snapshot it reads the effective generation exactly as matching does
(`tax_matching._read_inputs`), computes today's matching in memory (`compute_matching`), and takes every security whose
members are all instrument type 6 (eToro ETF), independent of the character today's code gives it. For each it prints
its other withholding causes and `withheld_all`, reads the newest determination run and applies 3d-3c-2's
`reporting_status` per Design 1; only a security passing Design 1 with no other cause is identified, under both
readings of the thirty-day period (Source rules), with Design 2's refusals and invariants enforced, Design 3's
character and Design 4's reasons. It exits non-zero on a broken invariant, and when a fund security with lots and no
other cause is refused or not `non_reporting_assumed`. Legal sources fetched 2026-10-15 are under
`var/research/3616/slice3d3c3/legal/` (earlier ones under `…/slice3d3c2/legal/`), each with its URL and fetch time on
its first lines.

1. **Securities** (evidence run 7, `broker_evidence_v3`; history fetch started 2026-10-15 05:04:16Z;
   `withheld_all` false). Computed: AAPL, NXH (shares). Withheld: GME, ILMN (`unresolved_subject`); SPY
   (`character_unestablished` only); IUSA.L and CSPX.L (`character_unestablished` and `unresolved_subject`). The three
   type-6 securities are SPY, IUSA.L and CSPX.L.
2. **SPY's lots** (8, all on SPY.RTH, all `provisional`), execution instant UTC, UK date, units, £:

   | lot | executed_at | uk_date | units | £ | class reasons |
   |---|---|---|---|---|---|
   | `acq:demo:382257232` | 2026-09-18 15:36:45.083 | 09-18 | 0.29615500 | 168.43050669140000 | spread_inclusion_assumed |
   | `dis:demo:3601264304:…` | 2026-09-23 13:35:31.583 | 09-23 | 0.29615500 | 172.44628722262500 | disposal_formula_assumed |
   | `acq:demo:383458210` | 2026-09-23 14:02:07.123 | 09-23 | 0.16227400 | 94.27063701503500 | spread_inclusion_assumed |
   | `acq:demo:383458257` | 2026-09-23 14:10:19.957 | 09-23 | 0.12993500 | 75.03203920117100 | spread_inclusion_assumed |
   | `acq:demo:385158730` | 2026-09-30 15:37:50.750 | 09-30 | 20.62672000 | 11933.68460630607000 | spread_inclusion_assumed |
   | `acq:demo:385501993` | 2026-10-01 15:37:26.687 | 10-01 | 0.13442600 | 77.30850742070600 | spread_inclusion_assumed |
   | `acq:demo:387752115` | 2026-10-09 15:37:35.540 | 10-09 | 0.61384700 | 360.56710775200500 | spread_inclusion_assumed |
   | `acq:demo:388523915` | 2026-10-14 15:38:12.083 | 10-14 | 0.09032700 | pending FX | spread_inclusion_assumed |

   The disposal is one position's close (3c: a disposal lot is one position's close event); it closes the 2026-09-18
   position. Both 2026-09-23 acquisitions come *after* it on the same day. No two lots share an instant.
3. **Reporting status** (determination run 1, `succeeded`, cutoff 2026-10-15 05:04:43Z; file sha256 `6a9910ce…4dec`):
   SPY `non_reporting_assumed` at each of its six lot dates and at the holding date 2026-10-15, each resting on
   A1–A9; `transparent_fund` `unresolved`; search `not_found`. IUSA.L and CSPX.L `not_candidate`.
4. **SPY under Design 2.**
   - *Date reading* (the policy, F2): the period is UK dates 2026-09-24 .. 2026-10-23. The disposal is identified with
     0.29615500 of the 2026-09-30 acquisition, cost 0.29615500 × 11933.68460630607000 / 20.62672000 =
     **£171.34185001689916** (half-even at `1E-14`); computed gain **£1.10443720572584**. Holding: 7 acquisitions,
     21.75752900 units (2026-09-30's balance 20.33056500, £11762.34275628917084); total cost undefined while the
     10-14 lot's FX is pending.
   - *Time reading*: the two later same-day acquisitions first (0.16227400, 0.12993500), then 0.00394600 of 09-30
     (£2.28297661753705): cost £171.58565283374305, gain £0.86063438888195.
   - The readings differ by £0.24380281684389 on this disposal, and in which acquisitions remain.
5. **Window.** The disposal's thirty days elapse at London midnight beginning 2026-10-24 (2026-10-23 23:00Z); the
   generation's fetch started before that, so it is open.

## Source rules

- **Relevant securities.** TCGA 1992 s106A(10)(c): "securities which are interests in a non-reporting fund, within
  the meaning of the Offshore Funds (Tax) Regulations 2009 … (see regulation 4(2))". 3d-3c-2: non-reporting status
  alone decides this; transparency does not change reg 4(2) status.
- **No pool, no same-day rule.** s104(3): for s104, 105, 107, 110 and 114, "'securities' does not include relevant
  securities as defined in section 108", and s108(1)(c) includes "interests in a non-reporting fund". So neither the
  s104 holding nor s105's same-day aggregation and identification applies. CG51565: relevant securities "do not form
  part of a Section 104 holding" and are identified "With acquisitions within the 30 days following the disposal on a
  first-in, first-out basis (FIFO), then With acquisitions made before the disposal on a last-in, first-out basis
  (LIFO)"; "This rule is subject only to those set out in TCGA92/S106A(4) & (5)". CG51652 (corporation tax, same
  s104(3) exclusion): "The provisions of TCGA92/S105 do not apply to same day acquisitions and disposals of relevant
  securities … Disposals made at different times in the day should be identified with acquisitions in the order of
  disposal."
- **Order.** s106A(4): "Securities disposed of on an earlier date shall be identified before securities disposed of
  on a later date". s106A(5): "if within the period of thirty days after the disposal the person making it acquires
  securities of the same class", identify "with securities acquired by him within that period" and "with securities
  acquired at an earlier time within that period, rather than with securities acquired at a later time". s106A(5A):
  not with securities acquired while "not resident in the United Kingdom" or while "resident … but … Treaty
  non-resident". s106A(6): "relevant securities disposed of shall be identified with relevant securities acquired at a
  later time, rather than with relevant securities acquired at an earlier time". s106A(9): not "with securities
  acquired after the disposal except in accordance with [s105] or subsection (5)".
- **The thirty-day period (F2).** Whether it includes the rest of the disposal's day matters only for relevant
  securities: for shares s105(1) identifies same-day acquisitions first. TCGA s105(2)(b) describes the s106A(5)
  securities as those "required by section 106A(5) to be identified with securities acquired after the day of the
  disposal", and CG51560's example (sold 1 July, bought 31 July: "within the 30 day period") counts calendar days to
  d+30. The policy reads the period as UK dates d+1 .. d+30: an acquisition later on the disposal's own day is not in
  it, and s106A(9) bars identifying it with that disposal. **What remains uncertain:** s105(2) is in a section whose
  "securities" exclude relevant securities, so its description is context, not a rule that applies to them; and no
  HMRC page addresses a same-day later acquisition of a relevant security. The other reading (from the disposal's
  instant to the end of d+30) is carried as a reason wherever it could identify differently (Design 4).
- **The computation.** reg 39(1): for an income-tax payer the basic gain is "the amount which would be the gain on
  that disposal for the purposes of TCGA 1992 if the gain were computed without regard to any charge to income tax
  arising under this Part". The TCGA gain is 3d-2's: consideration for the disposal less the allowable expenditure on
  the securities identified (TCGA s15, s38(1)(a) and (c)), with the terms mapped as follows.
  - **Consideration** is the disposal lot's amount: 3d-1's basis, assumption A1, reason `disposal_formula_assumed`.
  - **Acquisition cost** is each identified acquisition's amount: assumption A4, reason `spread_inclusion_assumed`.
    Identification names particular securities (s106A(2): "identified … with securities of the same class that have
    been acquired"), so each disposal is of whole identified securities, and their cost is the consideration given
    for them: the acquisition's amount pro rata to the units identified. Nothing is a disposal of part of an asset
    (unlike a s104 holding, s104(6)), so s42's apportionment does not arise, and reg 45(5)–(6), which keeps the OIG
    out of the s42 fraction, has nothing to act on.
  - **Incidental costs** are recognised only when evidenced; none is, while 3c's charge indicators are zero (3d-1).
  - **No enhancement expenditure** is possible for a fund unit bought and sold through the broker.
  - **s37 and s39.** Assumption **F3**: no sum in the consideration is charged to income tax otherwise than under
    this Part (s37), and no expenditure counted is deductible for income tax (s39). A distribution's character is
    3d-3b's A8 (`distribution_character_assumed`), already carried by any lot it affects.
- **Losses and the charge.** reg 42: a computation producing a loss makes "the basic gain on the disposal … nil" and
  "no loss is to be treated as arising" for the Regulations; IFM13550: "any loss arising can be relieved only as a
  capital loss … the capital loss cannot be offset against offshore income gains". reg 17(1)–(2) (Condition A): a
  charge if the asset "is an interest in a non-reporting fund at the time of the disposal" and an OIG arises. reg 33:
  a TCGA disposal is a disposal here. reg 38: an OIG arises "if a basic gain arises", equal to it. reg 18(1), (3): the
  OIG is income arising "at the time of the disposal", charged under ITTOIA 2005 Part 5 Chapter 8 "for the year of
  assessment in which the disposal is made". reg 44–45: where a material disposal gives an OIG and is also the TCGA
  disposal, reg 45 replaces s37(1) and "a sum equal to the offshore income gain shall be deducted from" the
  consideration, so the chargeable gain on a disposal whose whole gain is an OIG is nil.
- **Exceptions and the supported population.**
  - reg 25's conditions are corporation-tax treatments (A–C), excluded indexed securities (D) and insurance rights
    (E); reg 27 is insurers' long-term funds; reg 28 is a loan that is not a participating loan; reg 31 is charities;
    regs 31A–31C need a fund whose "sole or main purpose" is investing in qualifying unlisted trading companies.
  - reg 26: "the interest in the fund is held as trading stock" or its disposal "is taken into account in computing
    the profits of a trade".
  - regs 30 and 43: rights acquired before 1 December 2009, or "in accordance with a legally enforceable agreement in
    writing that was entered into by the participant before 30th April 2009".
  - **Assumption F4**, stated by 3d-4: the account holder is an individual holding the units as investments, not as
    trading stock or in the course of a trade, and acquiring each by an ordinary broker purchase, not under a written
    agreement entered into before 30 April 2009. Every admitted acquisition is a broker order the account placed (3c);
    an acquisition dated before 2009-12-01 is refused (Design 2). The fund's own facts (not a loan, no
    unlisted-trading-company purpose) are the determination's (`s355_limb`, s356 conditions) and its A1–A9.
- **Residence and the basis of taxation.**
  - **F5**: the account holder was UK resident and not Treaty non-resident at every acquisition (s106A(5A)(a) and
    (b)). This extends 3d-2's A5, which names residence only; both are stated by 3d-4.
  - **F1**: reg 19 (remittance basis for a non-domiciled individual) is not applied. IFM13414: it applies "For tax
    years up to 2024/25"; "From 6 April 2025, all UK residents are taxed on the arising basis … unless they are a
    qualifying new resident" under the Foreign Income and Gains provisions, and OIGs "are capable of being qualifying
    foreign income". F1 is that no remittance-basis year applies to a disposal before 2025/26 and no FIG claim exists
    from 2025/26. The OIG stored is the gross amount arising; any relief is 3d-4's.
- **Transparency.** reg 29(1): no reg 17 charge if the disposal is "of an interest in an offshore fund falling within
  paragraph (b) or (c) of section 40A(2) of FA 2008" and "the fund is a transparent fund", subject to (2), a charge if
  during the holding the fund held interests in other non-reporting funds "more than 5% by value" (with (4) excluding
  interests whose disposal would itself be exempt under this regulation), and (3), a charge if the fund is
  non-reporting and fails to make "sufficient information available". SPY's limb is (b), assumed (3d-3c-2). Under
  `fund_determination_v1` both (2) and (3) are `unresolved` always, so a `transparent` fund's gain stays unresolved
  and only an `opaque` fund's gain is established as an OIG (Design 3).
- **Rounding** is 3d-2 Design 6, unchanged.
- **No source rule** fixes how an equal execution instant is ordered, how a figure with an unresolved character is
  stored, or which later figures an open window or the F2 reading affects; Designs 2–4 fix these by construction.

## Design

### 1. Character (`tax_character_v2`)

3d-3a's table is unchanged except its `exchange_traded_fund` line, which becomes, for a type-6 security that passes
the earlier lines:

| character | condition | `detail` |
|---|---|---|
| unestablished | Design 1's status is not `non_reporting_assumed` | `exchange_traded_fund` (with `fund_status`) |
| **non_reporting_fund** | otherwise | NULL |

**The status**, from 3d-3c-2's `reporting_status` over the newest determination run *visible in the matching run's
snapshot*, with `evaluated_at` the matching run's `as_of`:
1. If that function returns `no_run`, `run_failed` or `not_observed` (its steps 1 and 2 do not depend on the date),
   that is the status, with no date.
2. Otherwise it is evaluated at each *status date*: every distinct UK date of the security's admitted lots,
   acquisitions and disposals, ascending, then the *holding date*, the run's cutoff as a UTC date (the last date the
   determination answers). The first result that is not `non_reporting_assumed` is the status, with its date.
3. Otherwise the status is `non_reporting_assumed`; the character row records the determination id, the run id,
   `transparent_fund`'s answer and basis, and the union of the results' assumptions (A1–A9 for SPY).

Every admitted lot date must be inside the determination's interval (`valid_from` .. cutoff date), or the status is
`outside_interval`. Acquisitions are included because relevant-securities identification and Condition A presuppose
non-reporting status for the securities identified, not only at the disposal. A *known* reporting episode is one the
determination or the list search shows (a `listed` or `unresolved` search, a determination for another status): it
withholds. An undetected episode between status dates, or a status change within the holding (reg 17 Condition B,
reg 48), is the residual of A5 (`A5_part3_not_applying`), which every figure carries; the statuses are evaluated
point by point and do not reconstruct transitions.

A newer determination run that is `running` or `failed` when matching reads (another job's step, concurrent) gives
`run_failed`, withholding the funds for that matching run; the next matching run after a succeeded determination run
publishes them again. That is the contract: the newest run visible in the snapshot, not "this job's run".

### 2. Identification of relevant securities

For a `non_reporting_fund` security, from its admitted lots.
1. **Disposals.** Each disposal lot is one position's close at its own instant (3c Design 4); there is no same-day
   aggregation for relevant securities (s104(3)). Assumption **F6**: each such close is a separate disposal, not a
   fragment of one bargain recorded as several closes. Its evidence: a broker close is placed per position and
   executes at its own instant. Two lots of the security sharing an `executed_at` (where one bargain may have been
   recorded as several closes, or no order exists) **refuse** the security (`time_order_unestablished`).
2. **Other refusals:** an acquisition dated before 2009-12-01 (`pre_2009_rights`, regs 30 and 43).
3. **Disposals in turn**, ordered by `(uk_date, executed_at, lot_key)` (s106A(4); CG51652 within a day). For each
   disposal on UK date *d* at instant *t*:
   - **thirty-day**: acquisitions with remaining units and `d < uk_date ≤ d + 30`, ordered by `(executed_at,
     lot_key)`, earliest first (s106A(5), F2);
   - **LIFO**: then acquisitions with remaining units and `executed_at < t`, latest first (s106A(6)).

   A disposal left with units refuses the security (`identification_incomplete`; s106A(9), and s105(2) does not apply
   to relevant securities). 3c's units conservation and `close_before_open` make this unreachable for sound evidence,
   as 3d-2 Design 3 argues for step 5.
4. **Withdrawal.** Taking all remaining units of an acquisition takes its exact remaining cost; a partial take is
   `apportion` (3d-2 Design 6). A pending-FX acquisition has an undefined amount, so every cost withdrawn from it and
   its balance are undefined, recorded by lot key (3d-2 Design 6).
5. **The holding** is the remaining acquisitions: each one's remaining units and cost, and their totals.
6. **Invariants**, checked before anything is written; a failure raises and the run writes nothing:
   - per acquisition, units taken plus units remaining equal its units, and, when its amount is defined, every cost
     taken is defined and costs taken plus cost remaining equal its amount;
   - per disposal, identified units equal its units;
   - every thirty-day acquisition is in `(d, d+30]`, and every LIFO acquisition is before `t`;
   - the holding's totals equal its balances' sums;
   - every value fits its column (3d-2 Design 6's capacities).

### 3. The gain and its character

Per disposal, not per day (reg 42 applies to each disposal): `computed_gain = proceeds − Σ identified cost`, exact.
A missing amount gives status `amount_missing`; then the gain, the character and both splits are NULL. Each computed
disposal has:

| `gain_character` | `computed_gain` | `basic_gain` | `offshore_income_gain` | `chargeable_gain` |
|---|---|---|---|---|
| `capital_loss` | < 0 | 0 (reg 42) | 0 | the computed gain: the capital loss on the TCGA computation (IFM13550) |
| `nil` | = 0 | 0 | 0 | 0 |
| `offshore_income_gain` | > 0, `transparent_fund` `opaque` | the computed gain | the basic gain (reg 38) | 0 (reg 45(3)) |
| `oig_or_capital_unresolved` | > 0, otherwise | the computed gain | NULL | NULL |

- **A loss is capital under both characters.** If the gain would be an OIG, reg 42 makes it nil, no OIG arises, reg
  44 does not apply, and the TCGA computation's loss stands; if reg 29 removes the charge, the TCGA loss stands. The
  two computations coincide here because the basic gain *is* the TCGA computation without this Part's income-tax
  charge (reg 39(1)), and F3 excludes every other income-tax interaction. The stored loss is the computed amount;
  whether it is allowable in a year (s16, its claim, s16A) is 3d-4's.
- **Only a positive gain's character depends on transparency.**
- **Publication.** An unresolved character is not a missing figure: the computed and basic gains are stored, and the
  income/capital split is NULL. The two characters are *conditional scenarios for this disposal*, not bounds on a tax
  year: a transparent fund's holders are also taxed on income as it arises, which is not computed (3d-3b). A consumer
  must carry an unresolved split as unresolved and never read NULL as zero. The tax year is the disposal's (reg 18(3);
  TCGA).

### 4. Reasons

Every row of a `non_reporting_fund` security carries `history_coverage_unestablished` (3d-2 Design 7) and
`non_reporting_status_assumed` (the character row's assumptions, which 3d-4 states beside it). Two propagated reasons
are defined per disposal and *carried* from it onwards:
- `window_open` on a disposal until its window elapses (3d-2 Design 7's instant; no same-day exemption: a later
  acquisition in its period displaces whatever it now takes);
- `thirty_day_period_assumed` on a disposal with an acquisition of the security later on its own UK date. It marks a
  *potential* difference, conservatively: that acquisition may already be taken by an earlier disposal.

A carried reason applies to that disposal and to every row after it in identification order, because changing an
earlier disposal's identification frees or takes acquisitions that later disposals and the holding use.

| row | reasons |
|---|---|
| disposal | its own lot's consideration reasons, those of every acquisition it identifies, the two above, and every reason carried from it or an earlier disposal |
| identification | the acquisition's consideration reasons, the two always-present reasons, and the reasons carried at its disposal |
| acquisition balance | the acquisition's consideration reasons, the two always-present reasons, and every reason carried from any disposal |
| holding (security row) | the union of its balances' reasons, plus every carried reason; `window_open_since` is the earliest open disposal's date |

With no disposal, nothing is carried. Per acquisition, not per day: a balance keeps only its own acquisition's
reasons, since there is no averaging. F1, F3–F6 and 3d-2's A5 are stated by 3d-4, not stored per row, as A5 is today.

### 5. Storage (migration `sql/494_tax_fund_disposals.sql`)

Every new column is NOT NULL unless it says NULL is allowed; arrays default to none and are never NULL; units are
`NUMERIC(24,8)` and sterling `NUMERIC(44,14)` (3d-2's capacities).
- **`tax_matching_characters`**: `character` admits `non_reporting_fund`. New columns, NULL allowed: `fund_status`
  (3d-3c-2 Design 3's codes), `fund_status_reason` (its structured reason code, `list_match` or
  `search_unavailable`, else NULL), `fund_status_date DATE`, `fund_determination_id`, `fund_determination_run_id`,
  `transparent_fund` (`transparent`, `opaque`, `unresolved`), `transparent_basis` (`established`, `assumed`,
  `unresolved`), `fund_assumptions TEXT[]`. CHECKs: `non_reporting_fund` ⇔ `fund_determination_id`,
  `fund_determination_run_id`, `transparent_fund`, `transparent_basis` and `fund_assumptions` non-NULL and
  `fund_status` NULL; `fund_status` non-NULL ⇒ detail `exchange_traded_fund`; `fund_status_date` NULL when
  `fund_status` is `no_run`, `run_failed` or `not_observed`. A trigger checks that a `non_reporting_fund` row's
  `(fund_determination_run_id, tax_security_id, fund_determination_id)` is a `determined` row of
  `tax_fund_determination_observations`.
- **`tax_matching_securities`** gains `holding_basis` (`s104_pool`, `relevant_securities`; default `s104_pool`). For
  `relevant_securities` the five pool columns hold the holding's totals and reasons. Code checks before the seal that
  a computed security's basis is `relevant_securities` exactly when its character is `non_reporting_fund`.
- **Routing triggers:** `tax_statutory_disposals` (and so `tax_identifications`) refuse a security whose basis is not
  `s104_pool`; the three tables below refuse one whose basis is not `relevant_securities` or that is not `computed`.
- `tax_matching_dependencies.reason` admits `time_order_unestablished`, `pre_2009_rights`,
  `identification_incomplete`.
- **`tax_fund_disposals`**: PK `(matching_run_id, tax_security_id, lot_key)`, FK to the security row; `seq INTEGER >
  0` (identification order, unique per security); `executed_at TIMESTAMPTZ`, `uk_date DATE`, `tax_year TEXT`;
  `units > 0`; `proceeds_gbp`, `cost_gbp`, `computed_gain_gbp`, `basic_gain_gbp`, `offshore_income_gain_gbp`,
  `chargeable_gain_gbp` (sterling, NULL allowed); `status` (`computed`, `amount_missing`); `gain_character` (NULL
  allowed); `missing_lot_keys TEXT[]`, `reasons TEXT[]`. CHECKs, each `(…) IS TRUE`:
  - `computed` ⇔ proceeds, cost, computed gain, basic gain and `gain_character` non-NULL, `missing_lot_keys` empty and
    `computed_gain_gbp = proceeds_gbp − cost_gbp`;
  - `amount_missing` ⇔ computed gain, basic gain, character and both splits NULL, and `missing_lot_keys` non-empty;
  - Design 3's table row by row: the character agrees with the computed gain's sign and fixes `basic_gain_gbp` and
    both splits as listed.
  A trigger refuses `offshore_income_gain` unless the security's character row has `transparent_fund = 'opaque'`.
- **`tax_fund_identifications`**: PK `(matching_run_id, tax_security_id, disposal_lot_key, seq)`, FK to the disposal;
  `seq > 0`; `rule` (`thirty_day`, `lifo`); `acquisition_lot_key`, `acquisition_executed_at`, `acquisition_uk_date`;
  `units > 0`; `cost_gbp` (NULL allowed) NULL iff `missing_lot_keys` non-empty; `missing_lot_keys`, `reasons`.
- **`tax_fund_acquisition_balances`**: PK `(matching_run_id, tax_security_id, lot_key)`, FK to the security row;
  `units_left > 0`; `cost_left_gbp` (NULL allowed) NULL iff `missing_lot_keys` non-empty; `missing_lot_keys`,
  `reasons`.
- All three take 3d-2's child guard (insert only while the run is `building`; no update or delete), and
  `tax_matching_runs_seal()`'s refused-run check includes them. The cross-row equations (Design 2's invariants; the
  identification's acquisition in its disposal's period for `thirty_day` and before it for `lifo`; every lot key in
  the generation and the security) are checked in code before insert and asserted in the DB tests.

### 6. The run, its inputs and selection

- `run_tax_matching` reads, inside its snapshot, each lot's `executed_at`, the newest determination run, its
  observation for each type-6 security, the determination body and file hash, and the fund observation's
  `search_outcome`.
- **`inputs_digest`** document `matching_inputs_v4`: v3's plus `"funds": [[tax_security_id, status], …]` sorted by
  security, for every type-6 security, where `status` is `[status_code, status_reason_code, status_date,
  determination_id, file_sha256, transparent_fund, transparent_basis, assumptions]` for Design 1's result: the failing
  status with its date, or `non_reporting_assumed` with a NULL date. Free-text reasons and the cutoff are not entered,
  so a new determination run with the same results, including the same failing date, makes no new matching run; a
  status change (a new filing making the determination `stale`, `review_out_of_date`, a list match, a lot outside
  the interval) makes one. `executed_at` already enters through each lot's fingerprint (3c Design 4.10).
- **Policy ids:** `TAX_MATCHING_POLICY_ID = "identification_v2"`, `TAX_CHARACTER_POLICY_ID = "tax_character_v2"`.
  `effective_tax_matching` requires both, so after deployment the result is *unavailable* until a v2 run for the
  effective generation is published (no fallback to run 7, 3d-2 Design 9), and a refused v2 run stays unavailable.
- **Order.** Both invokers run the determination step before matching (3d-3c-2 Design 2); `daily_tax_reconciliation`
  matches against the newest determination run it finds. `reporting_status` re-evaluates `review_out_of_date`
  against each run's `as_of`, so a matching run after `review_by` withholds even without a new determination run.

## Local policy (frozen under `identification_v2` and `tax_character_v2`)

F1–F6; the date reading of s106A(5)'s period; time order within a day and the equal-instant refusal; the pre-2009
refusal; Design 1's status rule; Design 3's table and its storage; Design 4's matrix. Changing any is a new policy id.

## Not in this slice

- **Transparency and reg 29(2)–(4)**: a successor determination policy (3d-3c-2, Not in this slice). If SPY were
  transparent, its holders would also be taxed on income as it arises, which bears on 3d-3b's deferred income figure.
- **3d-3c-4**: list matches and reporting funds, including the London-listed UCITS ETFs, ERI (reg 99), Condition B,
  reg 48, and a fund whose status changes within the holding.
- **3d-4**: the tax-year view (OIGs as income, not offsettable by capital losses, IFM13550; loss allowability), the
  scenarios of an unresolved character, F1, F3–F6 and A5 stated beside the figures, the budget reserve.
- Reconsidering F2 when an HMRC statement or case on the question is found.

## Tests

Pure, each figure computed by hand in the test:
1. **Order:** LIFO across days (bought 10 then 10, sold 15: the later 10, then 5 of the earlier); thirty-day before
   LIFO; thirty-day FIFO across two later acquisitions; two disposals on one date in time order (the earlier takes the
   later acquisition under LIFO first); an earlier disposal's thirty-day claim before a later disposal's LIFO.
2. **The period (F2):** an acquisition on *d* after the disposal is not thirty-day and never identified with it; with
   no other acquisition the disposal is `identification_incomplete`; an acquisition on *d* + 30 is thirty-day, on
   *d* + 31 not; an acquisition earlier on *d* is LIFO; a disposal on 2027-03-20 with an acquisition on 2027-04-10
   is thirty-day and keeps tax year 2026/27; SPY's lots give fact 4's date-reading figures exactly.
3. **Refusals:** two lots at one instant, including two closes of one order (`time_order_unestablished`); an
   acquisition on 2009-11-30 (`pre_2009_rights`; 2009-12-01 passes).
4. **Gain and character:** a loss (`capital_loss`: basic 0, OIG 0, chargeable = loss); zero (`nil`); a gain with
   `opaque` (OIG = basic = gain, chargeable 0), `transparent` and `unresolved` (`oig_or_capital_unresolved`, splits
   NULL); a gain and a loss on one day stay two disposals with their own characters.
5. **Withdrawals:** one acquisition drawn by three disposals in three parts, the last taking the exact remainder
   (costs sum to its amount); pending proceeds (`amount_missing`, all NULL); a pending acquisition partly taken (the
   taken cost and its balance undefined); a pending acquisition untouched (the holding's total undefined, disposals
   defined).
6. **Reasons:** Design 4's matrix on every row type; an open window on the first of two disposals marks the second,
   its identifications, every balance and the holding; it clears at elapse (2026-09-23 open at 2026-10-23 22:59:59Z,
   elapsed at 23:00:00Z; a disposal on 2026-10-10 elapses 2026-11-10 00:00Z across the clock change);
   `thirty_day_period_assumed` from the first disposal with a later same-day acquisition onwards, not on earlier rows.
7. **Status rule:** `non_reporting_assumed` at every date gives `non_reporting_fund`; each other status at an
   acquisition date, at a disposal date and at the holding date alone gives `exchange_traded_fund` with that status
   and date; an acquisition before `valid_from` (`outside_interval`); `no_run`, `run_failed` (a newer `running` run)
   and `not_observed` with no date; a run that succeeded before `review_by` and a matching `as_of` after it
   (`review_out_of_date`); a security with no lots evaluated at the holding date only.
8. **Digest:** a new determination run with identical results, and one whose cutoff moved while the same lot stays
   `outside_interval`, give identical documents; a status change, a new file hash and a changed transparency answer
   each change it; a pinned fixture's bytes and SHA-256.

DB (one module):
9. The CHECKs and triggers of the three tables and the new columns: each Design 3 row accepted and its sign
   violations refused; `amount_missing` with a split refused; `offshore_income_gain` under a non-opaque character
   refused; a character row naming a determination its run did not determine refused; a fund disposal or balance on
   an `s104_pool` or withheld security, and a statutory disposal on a `relevant_securities` security, refused;
   inserts after sealing refused.
10. **Run and selection:** after the policy change and before any v2 run, `effective_tax_matching` is unavailable; a
    run over SPY's lots publishes one disposal, one identification and seven balances, and is then selected; a
    refused v2 run is not selected; shares' rows equal those of the v1 run over the same generation.

## Acceptance (dev DB)

**Pinned (evidence run 7):** fact 4's date-reading figures are a unit-test fixture (Test 2).

**After deployment:** one `tax_broker_evidence_refresh`, then rerun the Script against the same generation and
expect the new run to hold exactly its date-reading output:
1. a new matching run under `identification_v2` / `tax_character_v2`, selected by `effective_tax_matching`;
2. SPY present and `computed`, character `non_reporting_fund` with the Script's determination, assumptions and
   transparency, and every disposal, identification, balance and reason as the Script prints them (for evidence run
   7: one disposal, computed and basic gain £1.10443720572584, `oig_or_capital_unresolved`, splits NULL, one
   `thirty_day` identification of `acq:demo:385158730`; reasons `disposal_formula_assumed`,
   `history_coverage_unestablished`, `non_reporting_status_assumed`, `spread_inclusion_assumed`,
   `thirty_day_period_assumed`, and `window_open` until a generation whose fetch starts at or after
   2026-10-23 23:00Z);
3. every other security's character, status, dependencies and share figures equal those the newest v1 run holds for
   the same evidence run, if one exists; otherwise those the Script prints for today's matching.

## Full-population verification

The Script reads every security of the effective generation and every lot of each type-6 security, with no sample.
Today that is one fund with lots (SPY: 8 lots, one disposal) and two without lots, both withheld by unresolved
subjects. It measures the rules against those rows and enforces Design 2's invariants on them; it does not verify the
legal reading, which is this spec's checkpoint 1.

## Finding dispositions

Round 1 (`var/research/3616/slice3d3c3/ckpt1_r1_final.txt`, loop worktree):

| findings | disposition |
|---|---|
| 1 (reg 42) | `computed_gain` stored signed; `basic_gain` floored at nil; Design 3 and storage |
| 2, 4 (the TCGA computation) | Source rules map each s38 term to 3d-1's evidence or a named assumption; F3 for s37 and s39; the loss row rests on the same computation, its allowability 3d-4's |
| 3 (s42, reg 45(5)–(6), s37) | identified securities are whole assets, so s42 does not arise; reg 45 replaces s37(1) |
| 5, 6 (exceptions, protected rights) | each regulation's condition quoted from its fetched text; F4 for the participant's facts; the agreement route in F4 |
| 7 (reg 29) | full predicate with (2)–(4) and the legal-form condition |
| 8 (FIG) | F1 restated for both regimes from IFM13414 |
| 9 (treaty non-residence) | F5 |
| 10, 11 (coverage, no run) | status at every lot date and the holding date; no-run statuses first; residual stated as A5's |
| 12 (F2 support) | s105(2)(b) cited; the uncertainty stated precisely; the unsourced general proposition removed |
| 13 (disposal identity) | F6, the equal-instant refusal including closes of one order |
| 14 (bounds) | conditional scenarios; NULL never zero |
| 15, 16 (propagation) | Design 4's matrix; the F2 reason described as potential |
| 17–21 (Script) | selection by instrument type; Design 1 enforced; `withheld_all` and other causes respected; refusals stop computation; every invariant checked; non-zero exit |
| 22, 23 (digest) | structured codes and the failing date only; unique sorted dates; holding separate |
| 24, 25, 26 (storage) | routing triggers, the observation trigger, the opaque trigger, full column contracts |
| 27 (selection) | unavailable until a v2 run is published |
| 28 (tests) | 5 April, clock change, window boundary, `review_by`, three-part withdrawal |
| 29 (acceptance) | run-7 figures pinned as a fixture; acceptance against the Script's rerun and the v1 run of the same generation |
| 30 (run ownership) | the newest run visible in the snapshot, with its consequence |

## Security

No order or authorisation path changes. Matching stays a DB-only derivation over stored evidence and stored
determination runs; the Script is read-only and prints no account identifier. The budget's CGT reserve is unchanged
(3d-2 Design 11; 3d-4).
