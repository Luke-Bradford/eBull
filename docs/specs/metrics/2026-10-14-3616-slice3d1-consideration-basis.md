# The consideration basis and its publication classes (#3616 slice 3d-1)

Source: #3616 slice 3d. 3c left 3d "reconciling the broker's stated amounts as consideration", said 3d "must not
publish a tax figure from an unreconciled lot", and left 3d to decide what `execution_evidence = sync_only` and an
unobserved `totalFees` suffice for (3c spec, opening and "Not in this slice"). The reconciliation requirement came from
slice-3 checkpoint-1 round 1, findings 10 and 11: acquisition charges must be checked, and the disposal amount
`investment + netProfit` is a formula over two broker fields that needed establishing against cash.

**What this slice decides.** What the tax ledger takes as the amount given and received, under which rule and on what
evidence; which assumptions remain; and the *consideration class* each admitted lot carries into 3d-2:
`final_eligible` or `provisional`, with its reasons. It builds nothing: the classes derive from fields 3c already
stores. 3d-2 implements them.

**The outcome under this policy:** every lot is `provisional`. No available evidence discharges the spread assumption
for acquisitions or the formula assumption for disposals (Consideration basis), so every figure built from them says
so. The classes exist so that each reason can be discharged by named evidence later without re-deciding the rest.

**What it replaces.** 3c's sentence that 3d "must not publish a tax figure from an unreconciled lot". No usable
per-trade cash source was established among those evaluated (fact 5); a figure may instead be published, carrying the
reasons of its inputs (Consideration classes), subject to every other gate.

## 3d split

| part | scope | slice-3 round-1 findings it owns |
|---|---|---|
| **3d-1** (this) | consideration basis; consideration classes; the evidence-level decision 3c delegated | 10, 11 (re-routed from 3c; dispositions below) |
| **3d-2** | statutory matching from the effective generation: tax-security identity (29); same-day aggregation of acquisitions and of disposals into the statutory disposal (4); 30-day rule and its provisional window with late evidence (6); complete-quantity rule (7); withholding a security when an unresolved subject could change it (5); acquisition-only pools (33); rebuilding obsolete keys (31); the rounding policy for matched and pooled amounts (3c Design 1); retiring `tax_lots`; expected-result tests (42); per-disposal tax-year acceptance (43); class propagation (this spec). Gains are computed in GBP and marked `character_pending` | 4, 5, 6, 7, 29, 31, 33, 42, 43 |
| **3d-3** | tax character per share class and effective period (18); OIG routing on the statutory disposal; character keyed to the 3d-2 tax security (30); excess reportable income as a basis dependency, gated or ingested (17); citations (44, 45); dividends: their income treatment, the evidence that a dividend was paid, and an admission policy for it. Today 3c's gate makes an order `unresolved` when any retained observation of a gated indicator is non-zero, including sync `totalFees`, documented as "overnight fees and dividends" (3c fact 9, Design 4.9); that gate does not detect every dividend (a position the sync never observed has no `totalFees` observation). A character or basis change re-runs 3d-2's affected securities | 17, 18, 30, 44, 45 |
| **3d-4** | tax-year view: current gains less current losses, brought-forward losses down to the AEA, the AEA (36); account-only hypothetical labelling (37); unknown ≠ zero coverage flags (38), including the excluded non-share products 3c reports as omitted; each figure's class and reasons beside it; every aggregate's tax character (40); read-surface wording (46); no raw Bank of England rate beyond the operator; no figure for a `real` ledger scope (all evidence is demo, fact 1 and 3c) | 36, 37, 38, 40, 46 |
| **later, unscheduled** | evidence that discharges a reason: a per-close statement of the amount received for A1 (candidate: the close-order lookup's per-position `amount`, fact 5); evidence on how the spread is charged for A4 | — |

The text of each round-1 finding is in `var/research/3616/ckpt1_slice3_r1_final.txt` (loop worktree); the routing was
posted on #3616 (2026-10-13 13:46Z). Each part has its own spec and checkpoint 1.

**Dispositions of findings 10 and 11.**
- *10, acquisition charges.* Satisfied by 3c for every observed indicator: each gated charge indicator must be zero,
  else the order is not admitted (3c Design 4.9). A position the sync never observed has no `totalFees` observation:
  reason `sync_charges_unobserved` here; an acquisition without a lookup never had the lookup's charge indicators
  observed: reason `lookup_evidence_unobserved`. A separately debited spread would appear in no field: reason
  `spread_inclusion_assumed` (A4). So no acquisition is final while any applicable consideration reason is open.
- *11, the proceeds identity.* Not established: `investment + netProfit` stays assumption A1, reason
  `disposal_formula_assumed` on every disposal, and no gain is final while it is open. Discharging it is the later
  item above.

## Facts established first

"Script" is `scripts/measure_3616_slice3d_cash_deltas.py`, read-only, run on 2026-10-14 over run 2 (the latest published
generation of the ledger scope, policy `broker_evidence_v2`) in one REPEATABLE READ transaction; an explicit run id must
also belong to the ledger scope. Its output is `var/research/3616/slice3d/cash_deltas.txt` (gitignored; dates, amounts,
counts, symbols, and the demo account's order ids, no account identifier). Run 2's input watermarks equal the current
maxima (events 163416, lookups 21, observations 7), so the events it reads are run 2's. Numbers are parsed from JSON
text; a missing or invalid operand, or a missing lookup charge, makes the affected figure undefined rather than zero.

1. **The cash record** ("[snapshots]"; code as cited).
   - `broker_account_equity_snapshots` holds 49 `demo` rows on 49 distinct `snapshot_date` values (sql/324 checks that
     `snapshot_date` is the UTC date of `observed_at`), the first observed 2026-08-11
     23:55Z, with one gap spanning missing dates (2026-08-26 to 2026-09-12). `daily_portfolio_sync` writes it only when
     `settings.etoro_env == "demo"`, after the portfolio and trade-history requests of the same session.
   - eToro's "Calculate Available Cash" guide (read 2026-10-14): `Available Cash = credit - (Σ(ordersForOpen[i].amount
     where mirrorID = 0) + Σ(orders[i].amount))`; `credit` is "the total credit balance in your account"; available
     cash "only refers to your USD balance". It does not say when a trade, fee or dividend reaches credit.
   - The provider computes `available_cash` that way and stores the subtracted amount as
     `official_pending_order_amount`: non-NULL on 38 rows (from 2026-08-22 23:54Z), 0 on all 38. *Credit*
     (`available_cash` + pending) is known on those 38.
   - The row has no account id. Comparing it with the trade record assumes the demo credentials reached one account
     throughout (A2).
2. **Intervals holding trades** ("[intervals holding events]"). An interval runs from one snapshot's `observed_at`
   (exclusive) to the next one's (inclusive). The trade record's amount for an interval counts each order's stated
   initial investment once, negatively, at its earliest open event (an order split by a partial close has one open
   row per position, each stating the whole order's investment, 3c fact 5), and each close row's `investment +
   netProfit` positively.
   - 9 of the 48 intervals hold events: 27 of the 36 stored events. The other 9 events (2025-08-12 to 2025-11-14,
     listed under "events_outside_intervals") precede the first snapshot. All 9 intervals have known credit at both
     ends. All 13 close rows name a `positionId` and `orderId` pair that an open event also names.
   - With no charge term, 7 of the 9 balance exactly.
   - 2026-09-27 → 09-28 (4 orders: GME ×2 closing, unresolved; SVXY and SVOL opening, excluded CFDs) has residual
     −2.22 with no charge term. The two CFD orders' `found` lookups state `totalCosts` 1.50 and 0.72; their sum 2.22
     offsets it to 0. That `totalCosts` is debited from credit beside the investment is an assumption (A3); no page
     says so.
   - 2026-10-08 → 10-09 (3 orders: SVXY and SVOL closing; one SPY.RTH order opening, admitted) has residual −2.25,
     unexplained.
3. **Intervals with one participating order** (lines marked `single_order`). Six intervals hold the events of one
   order, so the comparison there is per order:
   - three SPY.RTH acquisitions (2026-09-18 225.04, 2026-09-30 15,853.90, 2026-10-01 102.14), each admitted with a
     lookup: credit fell by exactly the stated initial investment;
   - three CSPX.L orders, each opened and closed inside one interval (ending 2026-09-16, 09-17 and 09-22),
     unresolved: each states initial investment 50.0 and closes with `investment` 50.0 and `netProfit` −0.01, 0.0
     and −0.01, and credit moved by exactly −0.01, 0 and −0.01. That is joint consistency of the opening amount and
     the formula's proceeds (49.99, 50.00, 49.99) on a GBP-priced instrument: an error common to both would cancel.
4. **The admitted disposals** ("[lots of the run]"). Run 2 has three: AAPL ×2 (25.00, 24.99) and SPY.RTH ×1 (228.75),
   all in 2026-09-22 → 09-23. That interval holds 11 events of 5 participating orders (AAPL; IUSA.L, unresolved;
   SPY.RTH ×3) and balances with no charge term. No admitted disposal has an isolated comparison.
5. **No usable per-trade cash source was established among those evaluated.** These are the author's reads of the
   cited pages and code on 2026-10-14 and the recorded 2026-08-11 probe; the script does not reproduce them.
   - `GET /api/v1/money/accounts/cash/{accountId}/transactions` (portal page read 2026-10-14) lists a "cash account"'s
     transactions, keyed by a UUID; its `transactionType` enum is `card`, `internalTransfer`, `bankTransfer`,
     `balanceAdjustment`. No type is a trade. Not probed against the account.
   - `GET /api/v1/trading/info/demo/close-orders/{orderId}` (page read 2026-10-14) returns per-position close `amount`
     and `conversionRate` for a close-order id. The 13 stored history close rows carry no separately identified
     close-order id. The engine's own closes use this endpoint (`etoro_broker.py`, `_close_order_prefix`), so their
     ids exist at close time; whether ids of other closes can be recovered is not evaluated.
   - `GET /api/v1/balances/history` was refused (HTTP 403) to the demo credentials when probed on 2026-08-11, as
     recorded in `data-sources/etoro-api.md`; it documents daily snapshots. Inaccessible to this account, not shown
     absent.
   - `cash_ledger` is written by the app itself: `INSERT INTO cash_ledger` appears in `app/services/order_client.py`,
     `app/api/orders.py` and `app/services/portfolio_sync.py`.
6. **Credit elsewhere** ("eventless_intervals"). Of the 39 intervals without events, credit is unchanged in 28 and
   unknown in 11 (no pending amount at one end).
7. **What the broker documents** (list-trading-history page, read 2026-10-14): `investment` "The investment amount of
   the trade", `netProfit` "The net profit of the trade", `initialInvestment` "The initial investment amount of the
   trade", `fees` "The fees of the trade". It names no currency and no amount received.

## Consideration basis

- **The rule.** TCGA 1992 s38(1)(a) and s38(2): acquisition cost is the consideration given plus the incidental costs
  s38(2) lists (HMRC CG15250); s15 and s38(1)(c): disposal consideration less the incidental costs of disposal. HMRC's
  record-keeping guidance (gov.uk/capital-gains-tax/records, read 2026-10-14) tells the taxpayer to keep records
  "that show the date and the amount: you paid for an asset … you received for the asset", and "any contracts for
  buying and selling the asset (for example from solicitors or stockbrokers)". It says which records evidence an
  amount; it does not say what a broker field means. Which field is the amount is decided below, on evidence, and
  each unproven step is named.
- **Acquisition: the order's stated initial investment.** The broker states it per order; 3c checks it, within 0.1%, against
  the execution lookup where one exists, otherwise against the sync-reported units, price and conversion and conserves it across positions; three isolated acquisitions match credit exactly
  (fact 3). Open assumption **A4**: any spread is inside the executed price and so inside the investment, not debited
  separately (3c Source rules: "a spread debited separately from the investment would appear in no field read here").
  Three isolated matches do not establish it for the population.
- **Disposal: `investment + netProfit`, in USD — assumption A1.** No documented field is the amount received
  (fact 7). Evidence: three isolated round trips consistent jointly with their openings (fact 3) and one balancing
  aggregate holding all three admitted disposals (fact 4).
- **Incidental costs.** None is recognised while every *observed* gated charge indicator is zero (3c Design 4.9).
- **Assumptions**, named where a class depends on them: A1 (disposal formula), A4 (spread inclusion); A2 (one account
  behind the snapshot) and A3 (`totalCosts` debited beside the investment) are used only in the facts.

## Consideration classes

**Policy id:** `consideration_classes_v1`. It covers effective generations whose `evidence_policy_id` is
`broker_evidence_v2`; 3d-2 refuses to compute from a generation under any other evidence policy (an older one is
superseded by regeneration; a newer one needs this table reviewed and a new class policy id).

Each admitted lot is classified from its own row and its subject's row **in the same generation**, with every
applicable reason:

| lot | reason | condition |
|---|---|---|
| acquisition | `spread_inclusion_assumed` | always (A4) |
| acquisition | `execution_assumed` | `execution_evidence = 'sync_only'`: no lookup established the filled status or the single execution |
| acquisition | `lookup_evidence_unobserved` | `execution_evidence = 'sync_only'`: the lookup's gated charge and adjustment indicators were never observed: `totalCosts`, `openingData.fees`, `taxes` and `markup`, and `positionExecutions[].addedFunds` (3c gates them only where the lookup exists). Discharged only by a `found` lookup whose indicators pass 3c's gate; confirming the execution alone does not discharge it |
| disposal | `disposal_formula_assumed` | always (A1) |
| either | `sync_charges_unobserved` | the subject's `total_fees_unobserved_position_ids` is non-empty: those positions have no retained sync payload, so their `totalFees`, `totalExternalFees` and `totalExternalTaxes` were never observed. Discharged only by a retained sync payload for each such position whose indicators pass 3c's gate |

A lot with no reason is `final_eligible`. A required field that is NULL or outside its domain (the subject's and the
lot's `execution_evidence` not both `lookup` or both `sync_only`; `total_fees_unobserved_position_ids` NULL; a
`direction` other than `acquisition` or `disposal`) makes the lot `unavailable` (never `final_eligible`), and 3d-2 treats that as missing evidence for the lot's security.

**Composition with every other gate (precedence, highest first).**
1. A missing required input (an `amount_gbp` NULL under pending FX, an `unavailable` lot) prevents the calculation
   that needs it; a class never makes a missing input usable.
2. Withholding (3d-2: an unresolved subject that could change a security's identification) suppresses the affected
   figures whatever their class.
3. Provisional dependencies add reasons: a lot's consideration reasons, an open 30-day window (3d-2),
   `character_pending` and basis adjustments (3d-3), and coverage limits (3d-4).
4. A figure is `final_eligible` only if every input and every dependency above is; `final_eligible` on lots alone
   never authorises a final figure.

**Propagation.** A figure 3d-2 computes takes the union of the reasons of every lot it uses, transitively. A pool's
cost carries the reasons of every acquisition that ever contributed to it; a partial disposal from the pool does not
retire any contributor's reasons, because pooled cost is not traced to particular shares. They leave the pool only when
its quantity reaches zero, or when a recomputation after a discharge removes them. A gain keeps the reasons it had
when computed. 3d-2's tests include this case: 10 shares with `execution_assumed` and 10 without, 10 sold, and the
remaining cost still `execution_assumed`.
Classes are recomputed whenever the generation, the class policy id or a subject's evidence fields change; an
unchanged lot fingerprint is not evidence of an unchanged class (`total_fees_unobserved_position_ids` is a subject
field, not a fingerprint input).

**Run 2 under this policy** ("[publication classes]"): all 11 lots `provisional`: 6 SPY.RTH acquisitions
(`spread_inclusion_assumed`); AAPL acquisition (`spread_inclusion_assumed`, `sync_charges_unobserved`: 2 unobserved
positions); NXH acquisition (`spread_inclusion_assumed`, `execution_assumed`, `lookup_evidence_unobserved`); SPY.RTH disposal
(`disposal_formula_assumed`); AAPL disposals ×2 (`disposal_formula_assumed`, `sync_charges_unobserved`).

**Discharging a reason.** Under `consideration_classes_v1`, A1 and A4 apply always and cannot be discharged. A later
class policy, with a new id, defines the qualifying evidence, the lots or population it covers and the revised
predicate, after source research establishing that evidence's currency and charge treatment. Conditional reasons
(`execution_assumed`, `lookup_evidence_unobserved`, `sync_charges_unobserved`) change through ordinary regeneration
when the subject's evidence changes. Either way only that reason goes, and every dependent figure is recomputed. A per-close statement agreeing with A1 would
remove `disposal_formula_assumed` only: an AAPL disposal would stay provisional for `sync_charges_unobserved`.

## Not built: a running cash check

A daily check of credit against the admitted lots was specified and reviewed twice (checkpoint 1 rounds 1–2,
`var/research/3616/slice3d/ckpt1_r1_final.txt` and `ckpt1_r2_final.txt`) and is deferred. Its value would be
discrepancy detection: a day like 2026-10-08 → 10-09 flagging a disagreement between the snapshot change and the
modelled flows beside an admitted acquisition, whatever its cause (an unstated movement, a wrong formula, missing
evidence or timing). It is not built because what it can say is weak and costly to make sound: one equation per day attributes
nothing to a lot; on days holding unresolved or excluded subjects a raw-flow comparison is possible (facts 2–4) but
neither a reconciliation from admitted lots alone nor attribution of a residual to one; and a sound version needs the snapshot
bound to the ledger scope at capture, a finality rule coordinated with the same-day upsert, coverage of opens and
closes after the day ends, and enforced storage invariants. **Treatment of unattributed discrepancies meanwhile:**
recorded, not attributed. The −2.25 does not change the 2026-10-09 SPY.RTH acquisition's class, which is
`provisional` on its own reasons; 3d-4 must disclose it, dated, as unexplained and unattributed, beside the
account's coverage information for its tax year; it does not enter any lot's reasons. If a later slice needs cash
corroboration, it starts from those requirements.

## Limits

- Facts 2–4 rest on A2 and are exhaustive for this account's stored record, not evidence about unobserved shapes.
- The −2.25 of 2026-10-08 → 10-09 is unexplained and unattributed.
- All evidence is from the demo account (3c Limits); 3d-4 owns the live prerequisite.

## Full-population verification

Every stored snapshot (49), event (36) and admitted lot of run 2 (11) is in the measurement, and the classes of all 11
lots are computed by the script from their stored fields ("[publication classes]"), not sampled.

## Security

No order, capital or authorisation path; no code path changes. The script is read-only against the dev DB and prints
no account identifier.
