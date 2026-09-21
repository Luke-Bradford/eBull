# The core sleeve's household currency caveat — what each clause rests on

Reference for `CoreSleeveResponse.household_currency_caveat` in `app/api/strategies.py`.
Extracted from the field's inline comment on review, which had grown to dozens of lines for
one string constant (PRs #3272, #3273).

Shipped text:

> Sterling is not the unit here. Where the core sleeve holds a USD-quoted instrument, a
> household measuring it in GBP carries GBP/USD exposure on the whole position value and not
> only on its return, and this engine does not hedge it. A £ sign in the positions table is
> the broker's own label carried through unchanged, not a conversion this engine performed.
> Converting when you fund or withdraw is a household cost that the preregistered ceiling,
> which prices round-trip spread, does not include.

## Why it is not #2833's verbatim wording

#2833's "On pass" clause asks for the caveat *"unhedged USD exposure on a GBP account"*
verbatim. The words **"on a GBP account"** are dropped, and only those.

```sql
SELECT environment, currency, account_currency_id, count(*)
FROM broker_account_equity_snapshots GROUP BY 1, 2, 3;
```

Every row is `demo`. Rows with a non-NULL `account_currency_id` are broker-observed USD;
rows with NULL are not observations at all, but the pre-migration bound `'USD'` literal that
`sql/341_account_equity_observed_currency.sql` exists to stop us reading as evidence
(`account_currency_assumed_not_observed`).

⚠ That makes the clause **unsupportable, not false**. "On a GBP account" plausibly meant
valuation exposure rather than a per-trade conversion, so nothing here refutes it and both of
its claims are kept. The precedent for deviating at all is the sibling field
`household_tax_caveat`, which #2915's measurement turned from "strictly tax-dominates" into
"mixed".

## Clause by clause

| clause | rests on |
| --- | --- |
| "Where the core sleeve **holds** a USD-quoted instrument" | conditional on purpose: this response is also served in `cash`, `evidence_collecting` and `unavailable`, where no instrument is selected. An unconditional sentence is false in those states. |
| "on the whole position value and not only on its return" | translating a USD holding into GBP moves the whole of it, not just the P&L. Pinned by its own test assertion so the qualifier cannot be dropped. |
| "this engine does not hedge it" | `strategy_core_mandate_events.base_currency` is `CHECK (base_currency = 'USD')` (`sql/336:26`); no hedging instrument or FX overlay exists on the core path. |
| "a £ sign ... is the broker's own label carried through unchanged" | `app/api/strategies.py`, `currency=broker_position.currency` on the owned-positions read model. The value is passed through and no amount is converted. |
| "the preregistered ceiling, which prices round-trip spread, does not include" it | the declaration's `all_in_cost_rule` in `docs/proposals/ta/2026-08-24-core-selection-declaration.json`: `p75_full_round_trip_spread_bps` plus a documented 0 bps entry-sizing markup. Household funding/withdrawal conversion is outside it — the half of #2833's spike step 3 the measured verdict never priced. |

## Two claims that were shipped or drafted and are now retracted

Both are pinned negatively in `tests/test_2603_core_mandate_api.py` so they cannot return.

1. **"this engine reports no sterling figure"** — merged in `7b0cd043` and **false**: the
   owned-positions table prints `£` for this sleeve. Verified by grepping
   `StrategyPortfolioLens.tsx` alone; the label is formatted one component down, in
   `StrategyPositions.tsx`, from `position.currency`.
2. **"no amount on this page has been converted by us"** — the first correction, false for the
   same reason one level out: `local_eod_value_in_account_currency` is a conversion we perform
   and the account-evidence panel renders it on this route.

A third draft cited the selection declaration's `fx_rule` as proof of "neither hedges nor
models". That rule governs instrument-quote-currency → USD conversion, a **different currency
pair** from the household's, and never supported the sentence.

The recurrence is recorded in `docs/review-prevention-log.md` under *"A negative claim is only
as wide as the thing you grepped"*.

## The conversion boundary on this route

So that a later widening has something to check rather than re-derive:
`account_equity_evidence._convert_local_total` is the **only** converting call reachable from
`/strategies`. It feeds exactly four rendered fields —
`local_eod_value_in_account_currency`, `local_eod_value_at_official_marks` (that value plus
the mark correction), and the `difference` / `tolerance` derived from them. Every other money
field on the route is carried through in the currency its source reported.

Reproduce before relying on it:

```bash
rg -n 'in_account_currency|_convert_' app/api/strategies.py app/services/account_equity_evidence.py
```

⚠ Do **not** widen the caveat on the strength of this page. Re-run the command — taking a
document's word for a scope claim is precisely the failure the retractions above record.

## Related

- #3274 — whether the broker's `GBP` label is itself correct for a USD-quoted instrument is a
  separate operator-visible defect. This caveat deliberately claims only what we *do*, never
  what is true of the number.
- #2833's other "On pass" deliverable, the net benchmark line, is still unshipped; the page
  renders `BENCHMARK_REFUSALS` (#2602 item 5).
