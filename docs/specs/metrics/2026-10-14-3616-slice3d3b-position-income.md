# Dividends: position income does not withhold a held share, and the income figure is a later slice (#3616 slice 3d-3b)

Source: #3616 slice 3d, split by 3d-3a (`2026-10-14-3616-slice3d3a-tax-character.md`, "Not in this slice"). **3d-3b**
covers dividends. It owns three things: their income treatment, the evidence that one was paid, and an admission policy
for orders whose observed `totalFees` is non-zero. Today 3c's gate makes such an order `unresolved` (3c Design 4.9),
and 3d-2 then withholds the whole security for every tax year, permanently, because amendments are never deleted (fact
5).

**What this slice decides; the build implements Design 1–2.**
- **Admission.** Under evidence policy `broker_evidence_v3`, an order whose positions are all still held is no longer
  refused because its `totalFees` became non-zero. eToro documents that field as *"Total overnight fees and dividends
  charged/paid on the position"*. For the shape 3c admits (real, long, x1) the overnight fee is zero (Source rules), so
  the field holds distributions only. A distribution is not consideration for the shares, and an income distribution
  is not a disposal. A capital distribution requires TCGA s122's treatment, a part disposal or a cost reduction,
  which nothing here implements. The broker's field does not say which
  kind a distribution is, so the admitted lots carry a new named assumption, **A8** (`distribution_character_assumed`).
- **Disposals.** An order with any close row keeps v2's treatment of `totalFees`: a non-zero value in any retained
  payload of any of its positions refuses it (check 9a). The precise invariant: the `totalFees` relaxation admits no
  order with a close row that v2 refused. The new open-reason check (change 4) can refuse orders v2 admitted.
  - **Disposals of other orders can now be computed.** 3d-2 withholds a whole security while any of its orders is
    unresolved. A held order newly admitted under v3 removes its own contribution. If no other subject withholds the
    security and every other prerequisite passes, the disposals of its other orders become computable. A released gain carries A8 only when its identification or pool
    used the affected acquisition (3d-1 propagation, Design 2).
- **The income figure is not computed here.** The rules it follows are cited below. Its inputs include a per-payment
  gross amount, the foreign tax actually withheld and the arising date. None of them is exposed by the API pages
  inspected (Source rules). Its route and full prerequisite list are under Not in this slice. 3d-4 shows dividends as
  `not_computed`, never as zero.

**The outcome on dev today:** no admission, lot value or computed figure is expected to change. No retained
`totalFees` payload of any product is non-zero (fact 3), and all 21 retained sync open payloads carry `openPositionActionType` 0 (fact 4). Every lot
fingerprint changes with the evidence policy, and no lot gains A8.

## Facts established first

"Script" is `scripts/measure_3616_slice3d3b_dividends.py`, read-only, run on 2026-10-14 in one REPEATABLE READ
transaction over the ledger scope (`demo`). Output: `var/research/3616/slice3d3b/facts.txt` (gitignored; no account
identifier). Its sections are cited in brackets.

1. **An issuer-declared distribution on held CFDs, with no corresponding movement found** ("[distribution]", "[cash]").
   - **The distribution.** Icahn Enterprises' 8-K `0001104659-26-090743`, Exhibit 99.1 (read 2026-10-14): *"$0.50 per
     depositary unit, which will be paid on or about September 23, 2026 … to depositary unitholders of record at the
     close of business on August 17, 2026"*.
   - **The holding.** The demo account holds IEP as two positions, both in `broker_positions` ("portfolio_state
     held"), opened 2025-08-12 and 2025-09-15, together
     315.94810800 units. Both are in the ledger scope, long, leverage 1 and `settlementTypeID` 0 (CFD). Neither has a
     close event, and each has units equal to its initial units. Both portfolio rows carry the scope's account
     ("row_cid_is_scope_account"). The latest `trade_history_fetches` row, which identifies the refresh the portfolio
     tables hold (3c Design 2), has the scope's environment and account ("portfolio refresh provenance"). At the issuer's rate the units come to a hypothetical 157.974054 USD.
   - **What is not established.** The issuer's default election is additional depositary units (Exhibit 99.1). eToro
     says a CFD dividend *"will be reflected in your available balance on the ex-dividend date"* (help,
     `when-will-i-receive-my-dividend-payment`). How eToro treats a unit-default distribution on a CFD, and the CFD's
     entitlement, amount, adjustment date and settlement, are taken from no source here.
   - **What the record shows.** Every retained sync `totalFees` of both positions is 0.0. `available_cash` was 1703.46 on every snapshot
     from 2026-08-11 to 2026-09-15, which covers the record date. Its four changes up to 2026-09-23 fall in intervals
     ending 09-16, 09-18, 09-22 and 09-23 ("[cash]"), the trades of 3d-1 facts 3–4. Credit
     (`available_cash + official_pending_order_amount`) is first known at 2026-08-22T23:54Z. It changed in 0 of the 28
     known-credit intervals without a trade event. The interval spanning 2026-09-23 holds 11 trade events. 3d-1 fact
     2 reconciles every interval holding events: 7 balance exactly; one residual of −2.22 is explained by two CFD
     costs, and one of −2.25 is unexplained. Neither residual equals the hypothetical 157.974054 USD gross credit.
     These net interval residuals cannot identify or exclude a distribution adjustment.
   - **Bound of the finding.** This is net daily movement and per-position fields, not movement-level evidence, and
     it is one CFD distribution with no established adjustment obligation. It is consistent with eToro's statement that the demo account is *"completely free,
     with no fees"*, and with the demo not simulating distributions at all. No eToro page found says whether it does.
2. **Real positions held now** ("[held_real]"). Five SPY.RTH positions (opened 2026-09-23 to 2026-10-09) and one NXH
   position (opened 2025-11-14), all `settlementTypeID` 1 and all in the ledger scope's `trade_events`.
3. **No retained `totalFees` is non-zero** ("[total_fees]"). The scope holds 21 retained sync open payloads: 16 stored
   rows and 5 observations. There are 0 `unscoped` sync observations, attached or not. By product:
   - `settlementTypeID` 0: 10 payloads, all zero;
   - `settlementTypeID` 1: 11 payloads, all zero;
   - absent or not a number: none.
   None of the scope's 20 history payloads carries `totalFees` ("[total_fees]"). A position the sync never observed has no observation (3c Limits).
4. **Open reason** ("[open_reason]"). All 21 payloads carry `openPositionActionType` 0. `orderType` is 17 on 19 and 18
   on 2. None of the 20 history payloads carries either key.
5. **What a `totalFees` change does under v2** (3c Design 2, 4.6, 4.9; `tax_broker_derivation.py`, checks 6 and 9).
   - `totalFees` is a field of 3c's field table, so ingestion retains a changed value as an `amendment`
     observation, and the observation is never deleted.
   - A sync open amendment differing in `totalfees` is not a re-rendering, so it adds `source_amended`, sticky for
     the order's life.
   - A non-zero value in any retained payload adds `charges_nonzero:totalFees`.
   - Today's 5 amendments differ from their stored rows only in `opendatetime` ("[observations]").
6. **Runs** ("[runs]", which lists every run). Evidence runs 1 (v1) and 2 (v2) are published. Matching run 2
   (evidence 2, `consideration_classes_v1`, `tax_character_v1`) is the latest published one; 3d-3a's acceptance
   recorded `effective_tax_matching` returning it.

## Source rules

Pages read on 2026-10-14, account-free:

| short name | location |
|---|---|
| eToro PnL | api-portal.etoro.com/api-reference/trading--real/get-account-pnl-and-portfolio-details.md |
| eToro history | api-portal.etoro.com/api-reference/trading--real/list-trading-history.md |
| eToro balance history | api-portal.etoro.com/api-reference/balances/get-account-balance-history.md |
| eToro fees | www.etoro.com/trading/fees/ |
| eToro help | `https://help.etoro.com/s/article/<slug>?language=en_GB`, slugs: `how-are-the-overnight-fees-on-stocks-calculated`, `where-can-i-see-the-overnight-fees-that-will-be-charged-and-or-i-have-paid`, `does-etoro-pay-dividends`, `when-will-i-receive-my-dividend-payment`, `is-my-position-eligible-to-receive-a-dividend-payment`, `how-much-tax-is-deducted-from-my-dividends`, `Can-I-reduce-the-tax-that-is-deducted-from-my-dividends`, `where-can-i-see-my-dividend-payments`, `what-is-the-etoro-account-statement` |
| eToro demo | www.etoro.com/trading/demo-account/ |
| IEP | www.sec.gov/Archives/edgar/data/813762/000110465926090743/tm2622273d1_ex99-1.htm |
| legislation | `https://www.legislation.gov.uk/ukpga/1992/12/section/<n>` (TCGA 1992 ss38, 122, 126, 142); `…/ukpga/2005/5/section/<n>` (ITTOIA 2005 ss378A, 384, 402, 403, and Part 4 Chapter 5, ss409–414, "Stock dividends from UK resident companies"); `…/ukpga/2010/8/section/<n>` (TIOPA 2010 ss18, 36); `…/uksi/2002/2848/schedule/made` (UK–US convention art 10); `…/uksi/2009/3001/regulation/94` |
| HMRC | `https://www.gov.uk/hmrc-internal-manuals/<manual>/<page>`: capital-gains-manual cg57800, cg58750; savings-and-investment-manual saim5210; investment-funds ifm13356; double-taxation-relief dt19852. SA106 Notes 2026 (assets.publishing.service.gov.uk/media/69bbd6144c478213a823ce14/SA106_Notes_2026.pdf); HS263 2026 (gov.uk/government/publications/calculating-foreign-tax-credit-relief-on-income-hs263-self-assessment-helpsheet); www.gov.uk/tax-on-dividends |

The help-centre pages render client-side and were read in a headless browser. The API pages are the portal's
Markdown.

**What `totalFees` holds, for the admitted shape.**
- eToro PnL: `totalFees` is *"Total overnight fees and dividends charged/paid on the position in USD. Negative amount
  represents refund"*. `totalExternalFees` is *"Total fees in USD charged on the position. Example - TicketFee. This
  value does not include overnight fees and dividends"*.
- eToro help (overnight fees on stocks): *"Buy position | Non-leveraged | Zero fee"*. eToro fees: overnight fees are
  charged *"for open CFD positions"*.
- eToro help (where to see fees): the portfolio Fees column *"also includes any dividends credited or debited as a
  result of holding the position"*.

So for a real, long, x1 position, `totalFees` is documented to hold distributions only. Whether eToro's real-account
documentation describes the demo record exactly is not established (fact 1).

**A distribution and the shares' CGT figures.**
- TCGA 1992 s38(1)–(2): allowable expenditure is the acquisition consideration, enhancement and title costs, and the
  incidental costs of acquisition and disposal. A distribution received is none of these.
- s122(5)(b): a *capital distribution* is a distribution *"in money or money's worth except a distribution which in
  the hands of the recipient constitutes income for the purposes of income tax"*. s122(1) treats receiving one as a
  disposal of an interest in the shares. CG57800: *"Most distributions, for example, dividend payments, will be income
  distributions."*
- So a **cash income** distribution, paid apart from the shares' acquisition consideration, changes no CGT figure of
  the shares. A **capital** distribution requires s122's treatment, a part disposal or a cost reduction, which 3d-2
  would not apply. The broker's field does not classify a
  distribution, and no source read classifies one per payment. That is A8.
- **Shares received as a distribution** are not a purchase, and are not classified here.
  - TCGA s142(3) prices shares received as stock dividend income at their cash equivalent. CG58750: *"the new shares
    are treated as a new acquisition of shares"*. That treatment is confined to the stock-dividend-income conditions of
    ITTOIA Part 4 Chapter 5, *"Stock dividends from UK resident companies"*.
  - A bonus issue may instead be a reorganisation under s126.
  - Share distributions by non-UK companies and by partnerships are left unclassified.
  - eToro help: *"In the case of a stock dividend, it may be distributed as a new open position"*. eToro PnL documents
    `openPositionActionType` only as *"Position open reason"*, with no values.
- **Broker amount fields.** 3c records that the PnL page documents held `amount` as the initial investment plus
  additional margin (3c Design 4.9). The consideration v2 uses is the stated initial investment.
  - **What the checks show.** v2 requires that value to be one value across every retained payload
    (`consideration_conflict`), and requires held `amount` to conserve with it (`investment_not_conserved`).
    A retained change of the initial investment is `consideration_conflict`. A current held `amount` inconsistent
    with the consideration is `investment_not_conserved`. Neither field is observed continuously, so a change that
    reverts before a retained observation or before the held row is read reaches neither check.
  - **What they do not show.** They do not detect income already included in both at the first observation:
    10,005 stated against a 10,000 execution passes the 0.1% plausibility tolerance. eToro does not say these fields
    exclude income. Income included from the first observation is an inherited, unresolved limitation of v2. It is
    not A4, which concerns only the spread (`spread_inclusion_assumed`), and no class reason expresses it.

**How a dividend is taxed: the rules the figure slice computes by.**
- ITTOIA 2005 s384(1): a UK resident company's dividends are charged *"on the amount or value of the dividends paid and
  other distributions made in the tax year"*.
- s402(1), s403(1): a non-UK resident company's dividends are charged on *"the … amount of the dividends arising in
  the tax year"*. SAIM5210: *"the full amount of the dividends arising in the tax year"*. SA106 column B: the amount
  *"before any tax taken off"*.
- Foreign tax credit relief, TIOPA 2010 s18 and s36(2). SA106: *"Foreign tax is the lower of the foreign tax actually
  withheld and the amount of tax credit allowed under the terms of a DTA."* HS263: the credit is *"the lower of the:
  foreign tax paid … UK tax liability on the income"*, computed per item.
- UK–US convention art 10(2)(b) (SI 2002/2848): US tax on a portfolio dividend *"shall not exceed … 15 per cent. of the
  gross amount of the dividends"*.
- Rates from 6 April 2026: 10.75% / 35.75% / 39.35%, above a £500 allowance (gov.uk "Tax on dividends").
- **Fund distributions** (3d-3c's, with the fund's character):
  - ITTOIA s378A: an offshore fund's dividend is *"treated as interest"* when the fund fails the qualifying
    investments test (more than 60%);
  - IFM13356: a corporate fund's distributions *"will normally be treated as foreign dividends"*, while non-transparent
    unit trusts are *"taxable as miscellaneous foreign income"*;
  - reporting funds add excess reportable income (SI 2009/3001 reg 94).

**What the broker exposes of a real-asset dividend.**
- eToro help: *"If you hold a position secured with the real asset, you will receive the dividend payment in your
  available balance on the payment date"*.
- For a cash dividend paid by a US corporation, withholding is 30%. It is 15% for a UK resident who has *"signed the
  W-8BEN form and entered your Tax Identification Number"*, on long positions in US stocks and ETFs. Other issuers'
  countries have their own rates, listed by eToro. A payment *"may be subject to changes after they have been made"*, including *"the tax withholding
  percentage applied and the payout amount"*.
- The account statement's Dividends section shows *"the net amount in USD, along with the amount of withholding tax
  applied in USD, and the withholding tax rate"*. eToro help: *"There is no account statement for the virtual
  portfolio."*
- Of the API pages read, PnL's `totalFees` is the only per-position income field, and it is cumulative. eToro history
  documents only *"The fees of the trade"*. eToro balance history returns end-of-day balances with no movement types.
  No page read exposes a withholding amount, a gross amount or a payment date per dividend.

## Design

### 1. Evidence policy `broker_evidence_v3` (amends 3c Design 4)

Four changes. Every other check, field and key is v2's.

1. **Check 6: a `totalFees`-only change does not add `source_amended`.**
   - **Re-rendering test.** v2's `_re_rendered_only` asks whether one position's sync open payloads are equal in every
     key except `opendatetime`. v3 excludes `totalfees` as well, and keeps v2's `opendatetime` condition unchanged:
     every value parses, the values span under one second, and all fall on one Europe/London date.
   - **What it waives.** The amendment row stays an `amendment`; only its consequence changes. The test runs over the
     position's whole record, so it waives nothing when any other key also differs. That includes v2's unwaived
     `isPartiallyAltered` lifecycle change and an unparseable `opendatetime`. Both still add `source_amended`.
   - **Unscoped observations.** An attached `unscoped` sync open is not `unscoped_conflict` under the same test, but
     only after its `totalFees` passes the field check that retained sync payloads get. That check rejects an absent
     value (`missing_field:totalFees`), one outside the money domain (`invalid_field:totalFees`), and two spellings
     with different values (`field_conflict:totalFees`).
   - **Products.** The test applies before the product exclusion point. A CFD whose `totalFees` alone changes stays
     `excluded_non_share_product`.
2. **Check 9: `totalFees` leaves the gated charge indicators of an order with no close row.** It stays a *real* field
   (money, any sign). Every other indicator stays gated: `totalExternalFees`, `totalExternalTaxes`, the lookup's
   indicators and the close row's `fees`. Their documentation names fees and taxes, not distributions.
3. **Check 9a: `income_in_proceeds_unestablished`.** A real-asset order with any close row is `unresolved` if any
   retained sync payload of any of its positions has a non-zero `totalFees`. This covers stored, supplemental,
   amendment and lifecycle payloads, and attached `unscoped` ones.
   - **What it preserves.** It is v2's gate restated at order level for every order that has a close row, whether or
     not a piece is still held. 3c can admit a single execution in state `open` that has both close rows and a held
     balance (check 8). 9a applies to it as to a fully closed order.
   - **v2's residual, unchanged.** Retained values are cumulative samples. Three kinds of income escape them: income
     credited after the last retained observation and before the close; income credited and reversed between two
     observations; and income on a position the sync never observed. Retained zeros therefore do not establish that
     no distribution was paid, or that proceeds are free of income. v2 has the same exposure. It is an open limit of A1
     (`disposal_formula_assumed`, on every disposal). `netProfit` is documented only as *"The net profit of the
     trade"*, and 3d-1's corroboration of A1 used positions whose retained `totalFees` were zero.
4. **Check 7: open reason.** Sync `openPositionActionType` becomes a *real* field with the domain non-negative
   integer, checked like `leverage`.
   - **The shape check.** Check 7 requires it to be 0, as it requires `leverage` 1. Any other value is
     `unsupported_shape:openPositionActionType`. Absence is `missing_field:…`, and a boolean, string, null, negative or
     fractional value is `invalid_field:…`.
   - **Effect on ingestion.** Ingestion compares every field of the field table, so the field becomes a retained
     difference from the first v3 sync onwards. Changes before that were not compared and cannot be recovered.
   - **What it does and does not prove.** It is an allowlist of the one observed value (fact 4), not proof that 0
     means a purchase. It refuses any position whose stated open reason differs from every retained sync open payload
     inspected in fact 4. A non-purchase opening that also carries 0 is not detected. The residual is v2's: such a position is
     admitted only on the evidence v2 requires, with `execution_assumed` when no lookup binds it.
   - **Unobserved and history-only positions.** A position the sync never observed has no such field. With no
     lookup it is already `settlement_unestablished`. With a lookup it is bound by the lookup's `action` `open` and
     `transaction` `buy`, which is v2's residual too.
   - **`orderType` stays unchecked.** v2 does not check it, eToro PnL documents it as the order's type, and
     restricting it would change admission for a reason no source supports.

**Policy ids.** `TAX_EVIDENCE_POLICY_ID = "broker_evidence_v3"`. Every lot fingerprint changes once, as at v2 (3c Design
4.11).

### 2. Consideration classes `consideration_classes_v2` (amends 3d-1)

3d-1 requires a new class policy id for a newer evidence policy. v2 of the class table covers `broker_evidence_v3`
only. It reviews each v1 reason against the population v3 newly admits: real orders with no close row whose
positions carry a non-zero or changed `totalFees`.
- **v1 reasons.** Their predicates read subject and lot fields that v3 does not change. Each stays as it is.
  - `sync_charges_unobserved` still concerns positions with no retained sync payload. Its discharge clause, "a
    retained sync payload … whose indicators pass 3c's gate", now reads the v3 gate, which no longer includes
    `totalFees` for an order with no close row.
  - `execution_assumed` and `lookup_evidence_unobserved` are unchanged.
- **New reason** (acquisition lots): `distribution_character_assumed` (**A8**). It applies when any retained sync
  payload of any of the order's positions has a non-zero `totalFees`. It assumes that every distribution in that
  field is a separately paid **cash income** distribution: income, not a capital distribution (s122), and settled in
  cash, not in shares. It propagates as 3d-1 states: a pool's cost
  carries it until the pool's quantity reaches zero.
  - **Discharge.** A later class policy, with its own source research, must classify each distribution of the
    holding. It needs two kinds of evidence. Issuer evidence establishes each distribution's tax character.
    Account-specific evidence establishes that it was paid, settled in cash, paid apart from any acquisition
    consideration, and in what amount after any revision. A8 is removed only when both establish a separately paid
    cash income distribution for every one. A distribution that is capital, or settled in shares, does not discharge
    A8. It needs a policy that first adds the applicable sourced treatment before any dependent figure is
    recomputed: s122's part disposal or cost reduction, or the share-distribution treatment, which may be an
    acquisition or a reorganisation with basis allocation. Unclassified cases stay with A8.
  - **The required subject field.** It needs one new field on `tax_evidence_subjects`,
    `total_fees_nonzero_position_ids BIGINT[]`. It is a sorted, duplicate-free array of positive ids, each a
    position of the subject, and empty when none qualifies. A NULL array, a NULL element, a non-positive,
    duplicated or unsorted id, or an id that is not the subject's makes the lot `unavailable`, as 3d-1's other
    required fields do. An `unavailable` lot prevents every calculation that needs it.

`CONSIDERATION_CLASS_POLICY_ID = "consideration_classes_v2"` and `SUPPORTED_EVIDENCE_POLICY_ID = "broker_evidence_v3"`,
with their comments updated.
- **Matching run selection** is unchanged in mechanism. `effective_tax_matching` selects a run only when its class,
  matching and character policy ids equal the code's constants and its digest equals the current one (3d-2, 3d-3a).
  `TAX_MATCHING_POLICY_ID` and `TAX_CHARACTER_POLICY_ID` are unchanged.
- **During the deployment window.** From the moment the new constants are deployed, the v2-based run 2 no longer
  matches them, so the selection returns *unavailable*. It stays unavailable after a v3 generation is published,
  and after a failed or unpublished matching attempt, until a matching run over the v3 generation is published.
  `run_tax_matching` refuses a v2 generation with its existing unsupported-policy outcome.

### 3. Disclosure for 3d-4

3d-4's tax-year view reports dividends as `not_computed` for every tax year, never 0 and never omitted, and shows
A8 beside every figure that carries it. `tax_ledger`'s `dividend_total_gbp` sums `tax_lots`, which has had no writer
since 3d-2. Replacing it is 3d-4's work. This slice changes no read surface.

## Storage (migration `sql/491_tax_evidence_v3.sql`)

`tax_evidence_subjects` gains `total_fees_nonzero_position_ids BIGINT[]`. It is NULL on rows of earlier runs, and the
v3 derivation always writes it (test 7). The migration makes no other change.

## Local policy

Frozen under `broker_evidence_v3`:
- `opendatetime` (under v2's condition) and `totalfees` are excluded from the re-rendering comparison;
- `totalFees` is ungated for orders with no close row;
- check 9a's scope is any close row of the order, any retained payload, any non-zero value;
- `openPositionActionType` must be 0.

Frozen under `consideration_classes_v2`: A8 and its predicate.

## Not in this slice

- **The dividend income figure: a later slice.** Issuer-declared amount × units held at entitlement gives an
  *expected gross entitlement*, not an amount received. Its prerequisites, each sourced in that slice:
  - **Dated per-share amounts.** One per distribution, from issuer filings or a dividend calendar. #3740 slice F is
    measuring exactly this for the backtest: its concordance between eToro's dividend calendar and EDGAR is due once
    the capture tables hold 20 business days, about 2026-11-09. Its verdict is the starting point.
  - **Units held at entitlement.** Reconstructed from executions. Entitlement is holding *"at the market close on the
    trading day before the ex-dividend date"* (eToro help, eligibility).
  - **Receipt.** Evidence that the payment was made and in what amount. The broker revises payments after the fact,
    and the demo may not simulate them (fact 1).
  - **Withholding actually applied.** The documented US rates depend on the account's W-8BEN status, which no
    inspected API page shows. Other countries have other rates. An estimated rate is not tax actually withheld, and
    SA106 credits the lower of withheld and treaty-allowed.
  - **The rest.** Income character, including s378A for funds; the issuer's residence, for s384 versus s402–403; the
    arising or paid date; and the GBP translation rule.

  Any of these that stays unavailable leaves the figure `not_computed`, or labelled estimated, never final. The
  broker's cumulative `totalFees` is a cross-check there, not the source. Income on positions closed between
  entitlement and payment is reached by the entitlement route; a route through the sync would miss it.
- **Stock dividends and bonus issues.** Check 7 keeps a differently-flagged opening out. Identifying them under s142
  or s126 needs an observed case.
- **Fund distributions** (s378A, IFM13356, reg 94): 3d-3c.
- **CFD dividend adjustments.** CFDs are `excluded_non_share_product`, and 3d-4 reports them as omitted.

## Tests

Pure, on the derivation:
1. **Check 6.**
   - Payloads differing only in `totalFees` (0 → 1.23) do not add `source_amended`; the observation stays an
     amendment row.
   - Payloads differing in `totalFees` and in an `openDateTime` re-rendered 0.4 s later on one London date: none.
   - Payloads differing in `totalFees` and in an `openDateTime` 1.0 s later: `source_amended`.
   - Payloads differing in `totalFees` and in `units`: `source_amended`.
   - A `totalFees` change and an `isPartiallyAltered` lifecycle change: `source_amended`.
   - A CFD whose only change is `totalFees`: `excluded_non_share_product`.
   - A CFD whose `openDateTime` does not parse: `unresolved`.
   - An attached `unscoped` sync open differing only in `totalFees`: not `unscoped_conflict`.
   - The same with `totalFees` absent (`missing_field:totalFees`), null or `"abc"` (`invalid_field:totalFees`), and
     with `totalFees` 1 and `TotalFees` 2 (`field_conflict:totalFees`).
2. **Check 9.**
   - A held real order with `totalFees` 1.23 in one payload and −0.40 in a later one is admitted, with the same lots
     as with 0.
   - The same order with `totalExternalFees` 0.5: `charges_nonzero:totalExternalFees`.
   - `totalFees` absent: `missing_field:totalFees`.
3. **Check 9a.**
   - A closed single-position order whose earlier payload had `totalFees` 1.23 and whose last had 0:
     `income_in_proceeds_unestablished`.
   - A single execution in state `open`, with close rows and a held balance, and `totalFees` 1.23 on the held
     position: `income_in_proceeds_unestablished`.
   - A split order, all pieces closed, where one piece's earlier payload had 1.23: `unresolved` with the same reason.
   - The same order with every payload at 0: admitted.
   - A closed position the sync never observed: not 9a, so admitted as under v2 with `sync_charges_unobserved` (v2's
     residual, asserted so that a change to it is visible).
4. **Check 7.**
   - `openPositionActionType` 1: `unsupported_shape:openPositionActionType`.
   - Absent: `missing_field:openPositionActionType`.
   - `true`, `"0"`, −1, 0.5 and null: `invalid_field:openPositionActionType`.
   - A CFD with 1: `excluded_non_share_product`.
5. **Classes.**
   - A held order with a non-zero `totalFees` gets A8 on its acquisition.
   - A pool built from it carries A8 until its quantity reaches zero.
   - A held order with every value at 0 does not get A8.
   - Propagation into a disposal of another order. Order Y buys 10 shares on 2026-01-05 and is held, with
     `totalFees` 1.23. Order X buys 10 on 2026-03-02 and sells 5 on 2026-06-01, with no other acquisition within 30
     days, so X's disposal is matched against the s104 pool. Under v2, Y is unresolved and the security is withheld.
     Under v3, X's gain is computed from the pool and carries A8. The pool keeps A8 after the partial disposal.
     Emptying the pool later does not remove A8 from the gain already computed.
   - Release without propagation. Order X buys 10 on 2026-01-05 and sells all 10 on 2026-01-05 (same day). Order Y
     buys 10 on 2026-06-01 and is held, with `totalFees` 1.23. Under v3 the security is released, and X's gain, which
     the same-day rule matched, carries no A8.
   - The array's derivation, for each payload origin (stored, supplemental, amendment, lifecycle, attached
     `unscoped`). A non-zero `totalFees` there puts the position in the array, whether or not the order is admitted.
     That holds when an earlier non-zero value is followed by 0, and when the only non-zero value is an attached
     `unscoped` observation. The exact sorted array is asserted for every origin. A8 is asserted only on otherwise
     admissible fixtures. For an unwaived lifecycle change the array is still populated, `source_amended` is
     present and there are no lots. For an order with a close row, the same origins give 9a.
   - Release blocked. Two held orders of one security each carry `totalFees` 1.23. One is admitted under v3; the
     other has `units_not_conserved`. The security stays withheld.
   - `total_fees_nonzero_position_ids` makes the lot `unavailable`, and prevents the dependent calculation, when it
     is NULL, holds a NULL element, holds 0 or −1, is unsorted, has a duplicate, or names another subject's position.
6. **Policy.** The fingerprint document carries `broker_evidence_v3`, and a pinned lot's bytes and SHA-256 are fixed
   under v3.

DB:
7. **Migration and selection.**
   - v3 subject rows have a non-NULL `total_fees_nonzero_position_ids`.
   - A v2 generation is refused by `run_tax_matching`.
   - `effective_tax_matching` returns *unavailable* in three states: with a published v2-based run under the new
     constants and no v3 generation; after a v3 generation is published with no matching run over it; and after a
     matching attempt over it fails, leaving no published run.
   - After both a v3 generation and a matching run over it are published, it returns the run with
     `consideration_classes_v2`.

## Acceptance (dev DB)

**The baseline is pinned by watermarks and by the subjects' own inputs.**
- **Before deployment.** Run `tax_broker_evidence_refresh` under v2 and record its evidence run *B* and matching run
  *MB*.
- **After deployment.** Run it once more and record evidence run *E* and matching run *M*.
- **What pins the inputs.** The derivation also reads current `broker_positions` (units, amount, CID) and the app's
  instrument resolution. Each subject row records what it read of them, in `held_positions` and `instrument_id`, and
  the comparison below includes both. The pair is **pinned** when *B* and *E* have equal `environment` and
  `account_id`, equal watermarks (`trade_events_max_event_id`, `lookups_max_id`, `observations_max_id`,
  `history_fetch_latest_id`, `history_fetch_ok_id`, `fx_run_id`, `fx_policy_id`), and every subject has equal
  `held_positions` and `instrument_id`. Then every remaining difference is the policy's.
- **When the pair is not pinned.** A v2 run cannot be retaken after deployment. The comparison becomes a
  supplementary observation: every difference is listed and attributed either to an input change (named) or to checks
  6, 7, 9 or 9a.

With a pinned pair:
- **The evidence run.** *E* is `published` under `broker_evidence_v3`.
  - Its subjects equal *B*'s in every pre-existing column except `run_id`. The new array is NULL on *B*, so it is
    exempt here and checked separately below.
  - Its lots equal *B*'s in every column except `run_id` and `fingerprint`.
  - Each *E* subject's `total_fees_nonzero_position_ids` lists exactly its positions with a non-zero retained
    `totalFees`. On today's evidence (fact 3) every array is `'{}'`.
  - A subject or lot difference is acceptable only when checks 6, 7, 9 or 9a cause it. It is listed with that cause.
    On today's evidence none is expected (facts 3–4).
- **The matching run.** *M* is `published` over *E* with `consideration_classes_v2`.
  - **Expected figures.** They are derived from the listed subject differences and the security's complete withholding
    state after them. A newly admitted subject removes only its own contribution to withholding. Its security's
    figures become computable only when no other unresolved subject withholds it and every other prerequisite passes
    (3d-1 precedence: FX, `unavailable` lots). Figures that use its acquisition then carry A8. A newly refused subject
    (check 7) withholds its security. Each expected difference is calculated independently, before the comparison.
  - **With no subject difference,** *M* equals *MB* in every disposal, identification, pool and dependency figure,
    and no row carries A8. On today's evidence these are 3d-3a's figures: AAPL gain −£0.00753863550700; NXH pool
    1305.05709600 / £6048.55403340328000; SPY, IUSA.L and CSPX.L withheld; 10 dependency rows.
- **Selection.** `effective_tax_matching` returns *M*.

```sql
-- subjects: every pre-existing column but run_id (expect none). total_fees_unobserved_position_ids is 3c's
-- (sql/485, 3c Design 1); total_fees_nonzero_position_ids is new and NULL on B, so it is checked apart below
WITH s AS (SELECT run_id, subject, order_id, etoro_instrument_id, instrument_id, product, outcome, reasons,
                  trade_event_ids, lookup_id, held_positions, first_event_at, last_event_at, execution_evidence,
                  total_fees_unobserved_position_ids FROM tax_evidence_subjects)
(SELECT subject, order_id, etoro_instrument_id, instrument_id, product, outcome, reasons, trade_event_ids, lookup_id,
        held_positions, first_event_at, last_event_at, execution_evidence, total_fees_unobserved_position_ids
   FROM s WHERE run_id = :E
 EXCEPT ALL
 SELECT subject, order_id, etoro_instrument_id, instrument_id, product, outcome, reasons, trade_event_ids, lookup_id,
        held_positions, first_event_at, last_event_at, execution_evidence, total_fees_unobserved_position_ids
   FROM s WHERE run_id = :B)
UNION ALL
(SELECT subject, order_id, etoro_instrument_id, instrument_id, product, outcome, reasons, trade_event_ids, lookup_id,
        held_positions, first_event_at, last_event_at, execution_evidence, total_fees_unobserved_position_ids
   FROM s WHERE run_id = :B
 EXCEPT ALL
 SELECT subject, order_id, etoro_instrument_id, instrument_id, product, outcome, reasons, trade_event_ids, lookup_id,
        held_positions, first_event_at, last_event_at, execution_evidence, total_fees_unobserved_position_ids
   FROM s WHERE run_id = :E);
-- lots: every column but run_id and fingerprint (expect none); :B and :E are run ids
WITH l AS (SELECT run_id, lot_key, subject, order_id, instrument_id, etoro_instrument_id, direction, position_ids,
                  trade_event_ids, executed_at, uk_date, tax_year, units, usd_amount, consideration_status,
                  execution_evidence, fx_rate_date, fx_status, fx_observation_date, fx_usd_per_gbp, fx_gbp_per_usd,
                  fx_policy_id, fx_run_id, amount_gbp FROM tax_evidence_lots),
     e AS (SELECT lot_key, subject, order_id, instrument_id, etoro_instrument_id, direction, position_ids,
                  trade_event_ids, executed_at, uk_date, tax_year, units, usd_amount, consideration_status,
                  execution_evidence, fx_rate_date, fx_status, fx_observation_date, fx_usd_per_gbp, fx_gbp_per_usd,
                  fx_policy_id, fx_run_id, amount_gbp FROM l WHERE run_id = :E),
     b AS (SELECT lot_key, subject, order_id, instrument_id, etoro_instrument_id, direction, position_ids,
                  trade_event_ids, executed_at, uk_date, tax_year, units, usd_amount, consideration_status,
                  execution_evidence, fx_rate_date, fx_status, fx_observation_date, fx_usd_per_gbp, fx_gbp_per_usd,
                  fx_policy_id, fx_run_id, amount_gbp FROM l WHERE run_id = :B)
(SELECT 'only_in_E' AS side, * FROM (SELECT * FROM e EXCEPT ALL SELECT * FROM b) x)
UNION ALL
(SELECT 'only_in_B', * FROM (SELECT * FROM b EXCEPT ALL SELECT * FROM e) y);
-- the new array on E: on today's evidence (fact 3) expect 0; otherwise list and check against the retained payloads
SELECT subject, total_fees_nonzero_position_ids FROM tax_evidence_subjects
 WHERE run_id = :E AND total_fees_nonzero_position_ids IS DISTINCT FROM '{}';
```

The matching comparison uses 3d-3a's acceptance queries on *M* and *MB*.

## Full-population verification

The Script reads every retained sync open payload of the scope (21) and every known-credit snapshot interval (37). The
v2 → v3 comparison in Acceptance covers every subject and lot of a watermark-pinned pair of generations. Fact 1 is one distribution on one
instrument. It bounds what the record showed for that payment; it says nothing about the population of payments.

## Review

Codex checkpoint 1 took 7 rounds. Round 1 raised 46 findings, 8 of them BLOCKING. Those led to A8, order-level 9a,
the input-pinned acceptance and the move of the income figure to an entitlement-based later slice. Rounds 2–6 found
no BLOCKING finding (22, 12, 9, 4 and 2 findings respectively). Round 7 left one NITPICK, which is applied. The prompts
and outputs are in `var/research/3616/slice3d3b/ckpt1_r{1..7}*.txt` in the autonomy worktree.

## Security

No order, capital or authorisation path changes. The derivation reads stored broker payloads only. The Script is
read-only against the dev DB and prints no account identifier. Every source is public and account-free (operator rule
2026-10-10).
