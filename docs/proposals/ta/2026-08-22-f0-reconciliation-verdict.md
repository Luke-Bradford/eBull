# F-0 reconciliation verdict — compare like with like, then declare a tolerance

#2602 item 4, second half. The first half (#2602, `sql/350`) gave the local valuation a
measured effective-mark date. This half produces the divergence number and the verdict.

## The premise the existing panel was built on is wrong

`load_account_equity_evidence` compares `broker_account_equity_snapshots.equity` against
`portfolio_eod_snapshots.total_value` and hard-codes `comparable = False`. Measured on
the dev DB (2026-08-22):

| | value |
|---|---|
| `broker_account_equity_snapshots` | 12 rows, 2026-08-11 → 2026-08-22, unbroken |
| `portfolio_eod_snapshots` | 47 rows, 2026-06-12 → 2026-08-19 |
| overlapping days | **6** |
| official currency / local `display_currency` | **USD / GBP on every overlap day** |
| rows producing a `difference` | **0** — `local_eod_currency_mismatch` fires first |

Convert past the currency and the real defect appears. On 2026-08-19:

| | USD |
|---|---|
| official `equity` | 99,395.65 |
| official `total_invested` | 104,060.06 |
| `sum(broker_positions.amount)` | 64,529.06 |
| **gap** | **39,531.00 (39.8% of equity)** |

The two sides do not value the same population.
`_parse_account_risk_snapshot` (`app/providers/implementations/etoro_broker.py:1275-1325`)
folds **mirrors and pending orders** into `total_invested`; `portfolio_eod._read_positions`
(`app/services/portfolio_eod.py:295-345`) values **direct positions only**
(`broker_positions` + `cash_ledger`). `BrokerInstrumentInvestment`'s docstring records the
same fact from the other end (#2704): of 38 reported instruments, **33 had no direct
position at all**. The 2026-08-22 sandbox decision says the quiet part out loud — *"the
account is shared with non-engine holdings"* (`docs/settled-decisions.md:1178`).

Declaring a tolerance between `equity` and `total_value` would be declaring a tolerance on
a 39.8% structural mismatch. So the comparand changes first.

## Source rule

**No published rule fixes a broker-reconciliation tolerance, and none is invented here.**
This is an accounting reconciliation between two private feeds, not a regulated disclosure.
Searched; nothing governs it. The tolerance is therefore fixed **by construction** and
frozen in `RECONCILIATION_RULE_VERSION = "f0-reconcile-v1"`.

⚠ An earlier draft of this spec cited **SEC Reg NMS Rule 612** as the governing rule and
derived the tolerance from its minimum pricing increments. That citation is withdrawn:
Rule 612 fixes the increments on which NMS stocks may be *quoted*, which is not the same
question as how far two feeds valuing the same holding may legitimately differ, and it does
not reach the CFD and non-US products this account can hold. Caught at Codex checkpoint 1.
The construction below stands on its own and cites nothing it does not govern.

**The construction.** Both sides mark the **same units** with the **same formula** — local
`value_native = amount ± units × (close − open_rate)` (`portfolio_eod.py:186-191`) against
broker `amount + unrealizedPnL.pnL` (`etoro_broker.py:1286-1288`) — and both marks
originate at the same venue, since `price_daily` is eToro-fed. So the tolerance is set at
the tightest bound that is defensible without measurement: the **rounding of the stored
mark**, one cent of price per unit held, plus one cent of cash.

```
tolerance(account ccy) = Σ_priced convert(units × 0.01, native → display → account) + 0.01
```

Three things this bound deliberately does **not** absorb, each of which will therefore
present as `diverged` rather than be silently swallowed:

- the broker's mark is read at the snapshot instant (≈23:55 UTC) while `price_daily.close`
  is the session close, so an **extended-hours** print can move them apart;
- dividend cash the broker has credited and our `cash_ledger` has not (#2602 item 1, out of
  scope here);
- any corporate-action or unit-precision disagreement.

That is the intended posture, not an oversight. **v1 declares the tightest bound and lets
the data widen it.** Widening requires a measured justification and a
`RECONCILIATION_RULE_VERSION` bump — never a silent edit to the constant. `f0-reconcile-v1`
is returned alongside the verdict so the operator can see which rule produced it.

The cash cent has a repo precedent for the same decision on the same ledger:
`portfolio_sync._CASH_SYNC_TOLERANCE`.

## What changes

### `sql/363`

- `broker_account_equity_snapshots.official_direct_long_market_value numeric` — Σ
  `BrokerInstrumentInvestment.direct_long_market_value`, the field the provider already
  computes. ⚠ #2704 checked it against our quote feed on **one account, four of seven
  positions**; that is a sample, so it is evidence the field means what it says and *not* a
  full-population validation. The verdict is the validation.
- `broker_account_equity_snapshots.official_direct_long_positions integer` — Σ
  `direct_long_count`.
- `broker_account_equity_snapshots.official_direct_short_positions integer` — Σ
  `direct_short_count`. A direct short has **no** entry in `direct_long_market_value`, so a
  non-zero count means the comparand is incomplete and must refuse, not silently under-state.
- `broker_account_equity_snapshots.official_pending_order_amount numeric` — the parser's
  `pending_amount`, which is **subtracted from `available_cash`** and added to
  `total_invested` (`etoro_broker.py:1307-1327`). Our `cash_ledger` knows nothing of pending
  orders, so a non-zero value makes the two cash legs incomparable and must refuse. Requires
  a new `pending_order_amount` field on `BrokerAccountRiskSnapshot` (defaulted `None`, so no
  existing constructor breaks).
- `portfolio_eod_snapshots.mark_rounding_tolerance numeric` — the `Σ units × 0.01` sum
  above, in the snapshot's display currency.

All five are nullable. The 12 existing official rows and 47 existing local rows predate
them; each absence gets its own named reason rather than a zero. NULL is never read as 0 —
the counts refuse on NULL exactly as they refuse on a non-zero value.

**Position-count reconciliation closes the offsetting-error hole.** Comparing two sums
alone lets one missing holding and one extra holding of equal value net to `reconciled`. So
`portfolio_eod_snapshots.positions_total` (rows in `broker_positions`) must equal
`official_direct_long_positions + official_direct_short_positions`, and a mismatch refuses
with `direct_position_count_mismatch`. The counts are the cheap structural check the value
comparison cannot do for itself.

### `app/services/portfolio_eod.py`

`compute_eod_equity` accumulates `mark_rounding_tolerance` over **priced positions only**
— a `no_price` or `no_fx` position contributes nothing to `positions_value`, so it cannot
contribute an allowance to a total it is not in, which is the same rule `summarise_marks`
already applies. The allowance is accumulated inside the branch that has already succeeded
at `convert()`, using that same rates dict, so it cannot raise where the value did not.
`_write_snapshot` persists it.

### `app/services/account_equity_evidence.py`

`record_account_equity_snapshot` persists the two new official fields.

`load_account_equity_evidence` gains:

- `local_eod_value_in_account_currency` — `total_value` converted display → account
  currency via `load_fx_rates_for_date(conn, portfolio_eod_snapshots.fx_rate_date)`, the
  same carry-forward rate set the local total was built from. No new FX source, no new
  as-of date. ⚠ It is a re-load at the same date, not the identical in-memory dict, so a
  later revision of a rate row would move it; that is accepted and named here rather than
  claimed away. When display currency **equals** the account currency no FX is required at
  all, and a NULL `fx_rate_date` is not a refusal in that case.
- `official_comparand` = `official_available_cash + official_direct_long_market_value`.
- `difference` = `official_comparand − local_eod_value_in_account_currency`. ⚠ This
  **replaces** the old `equity − total_value`; the old one was never populated (currency
  mismatch on every stored row), so no consumer changes meaning.
- `residual_not_in_local_book` = `official_equity − official_comparand` — mirrors, pending
  orders and any direct short. Reported and sized, **never** folded into `difference`. ⚠ It
  is a residual, not an attribution: it also absorbs any provider-parse or valuation error
  on the official side, so neither the field name nor the UI copy may claim it *is* the
  non-engine holdings.
- `tolerance` = `mark_rounding_tolerance` converted to the account currency, `+ 0.01`.
- `reconciliation_rule_version` = `"f0-reconcile-v1"`, returned so the verdict carries the
  rule that produced it.
- `reconciliation_state`: `unavailable` | `refused` | `reconciled` | `diverged`.
  `comparable` widens from `Literal[False]` to `bool`, true exactly in the two decided states.

**Verdict precedence, fail-closed and total:**

1. no official row → `unavailable`;
2. **any** entry in `incomplete_reasons` → `refused`. Every existing reason keeps its
   blocking force — `account_currency_assumed_not_observed`,
   `account_currency_not_documented`, `same_day_local_eod_snapshot_missing`,
   `local_eod_valuation_incomplete`, `local_eod_effective_time_unknown` and
   `local_eod_marks_carried_forward` all refuse, because each names an input the comparison
   depends on. One rule, no per-reason exemption list to fall out of date;
3. otherwise `|difference| <= tolerance` → `reconciled`, else `diverged`.

**The money fields are NOT gated on the verdict, and the implication runs one way only.**
`difference`, `official_comparand`, `local_eod_value_in_account_currency` and
`residual_not_in_local_book` are populated whenever their *inputs* are computable, refused
or not — an operator repairing a refusal needs the numbers, and today every real row is
refused, so blanking them would ship an empty panel. `official_pending_orders_outstanding`
and `mark_rounding_tolerance_not_recorded` in particular can fire while `difference` is a
perfectly good number.

So the invariant is the **implication**, not a biconditional:

> `comparable` is true ⟹ `difference` and `tolerance` are both non-NULL.

`comparable` is the single load-bearing flag; a populated `difference` beside
`comparable = false` is a diagnostic, not a verdict, and no consumer may read it as one.
`test_a_populated_difference_never_implies_a_verdict` asserts both directions of that —
that a refused row can still carry its numbers, and that a decided one can never be
missing them.

New refusal reasons, each naming one missing or incomparable input:

| reason | fires when |
|---|---|
| `account_currency_fx_rate_missing` | currencies differ and no rate bridges them |
| `official_direct_position_value_not_recorded` | `official_direct_long_market_value IS NULL` (row predates `sql/363`) |
| `official_direct_short_positions_unvalued` | `official_direct_short_positions` is NULL **or** `> 0` |
| `official_pending_orders_outstanding` | `official_pending_order_amount` is NULL **or** `<> 0` |
| `direct_position_count_mismatch` | local `positions_total` ≠ official direct long + short counts |
| `mark_rounding_tolerance_not_recorded` | `mark_rounding_tolerance IS NULL` (row predates `sql/363`) |
| `reconciliation_inputs_out_of_bounds` | a negative tolerance, negative direct-long value or negative count survived the write |

`local_eod_currency_mismatch` is **retired as a refusal**: a GBP display currency against a
USD account is the ordinary configured state, not a defect. Its job passes to
`account_currency_fx_rate_missing`, which fires only when the mismatch is actually
unbridgeable. Its FE label stays, so any row still carrying the slug renders.

### `app/api/strategies.py` + `frontend/`

`AccountEquityEvidenceView` carries the new fields; `StrategiesPage`'s
`"Comparison tolerance not defined"` string is replaced by the verdict, the signed
divergence against its tolerance, and the residual described as *not represented in the
local book* — a residual, never an attribution. Reason labels added for the new slugs.

## Expected verdict on today's data

`refused`, on `official_direct_position_value_not_recorded` — every stored official row
predates the migration. The first post-migration snapshot decides for real, and that first
decided verdict is the acceptance evidence, recorded on the ticket.

Predicted from the numbers above: `official_comparand ≈ 1,703 + 59,181 = 60,884` against a
local total of `≈ 60,884`, with `residual_not_in_local_book ≈ 38,511`. ⚠ **This prediction
is arithmetic on one account on one day and supports nothing** — not enablement, not
acceptance, not a safety claim. It is written down so that a *different* outcome is visibly
a surprise rather than quietly renormalised.

## Out of scope, stated so it is not assumed

Items 1 (dividend cash), 3 (product identity) and 5 (benchmark refusal states) of #2602 are
untouched, and the ticket stays open. No backfill: the new columns are forward-only by
construction, because neither the broker's direct-position split nor the mark ticks were
retained for a past snapshot.
