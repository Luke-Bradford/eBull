# The core/cash rebalance decision (#2603 scope item 3, decision half)

Status: proposed. The **deterministic rebalance decision** only — mandate plus observed
sleeve state in, one verdict out. No order intent, no execution plane, no endpoint, no
migration, no persistence.

Predecessor: `docs/proposals/ta/2026-08-13-core-cash-mandate.md` (item 1, shipped as
`sql/336` + `app/services/strategy_core_mandate.py`). That spec closes with a list titled
*"Left to item 3 (execution semantics, recorded so they are not rediscovered)"*. This
document answers **four** of its five entries and states precisely which half of the fifth
it does not answer.

## Why the slice stops where it does

Item 3 reads *"drift outside band → order intent → the existing demo execution plane"*.
The first arrow is arithmetic over an operator authority. The second is the execution
plane, which item 1's spec already established **cannot be reused as-is** (Q5 below), and
whose acceptance #2603 reserves for an operator-attended demo session.

So the split is at the first arrow. What ships is `evaluate_core_rebalance`: pure — no
connection, no clock, no broker.

⚠ **No endpoint ships either, and that is a decision, not an oversight.** A read-only
preview endpoint needs the observed sleeve state, which comes from a broker portfolio read,
and Definition-of-Done clause 11 requires an operator-visible figure verified on the live
endpoint after the change. A headless worktree cannot do that. An endpoint shipped without
it would be an unverified operator surface.

⚠ **This module authorises nothing.** Item 1's table now has its first reader, which is
what that spec said item 3 would be; the reader itself has no caller. #2437's R4 comment
records the *control on a path the decision does not take* seven times over, so the state
is named rather than implied: **nothing calls `evaluate_core_rebalance`, and no verdict it
returns causes anything to happen.**

## Source rule

The one genuine formulation choice with a published answer is **where a triggered rebalance
trades to** — the target weight, or the near edge of the band.

**Leland, Hayne E. (2000), "Optimal Portfolio Management with Transactions Costs and
Capital Gains Taxes", Research Program in Finance working paper RPF-290, Haas School of
Business, U.C. Berkeley (SSRN 206871).** Under proportional transaction costs the optimal
policy is a **no-trade region** about the target proportions; when actual proportions fall
outside it, trade **to the region's boundary, not to the target**. The existence of the
no-trade region under proportional costs is Constantinides (1986) and Davis & Norman
(1990); Leland is the one that answers *how far to trade*, which is the question here.

We adopt the **boundary-targeting result** and nothing else. Three limits on that adoption,
stated so a later reader cannot cite this spec for more than it says:

1. **The band's width is not Leland's.** He derives the region from a cost model; ours is
   an operator declaration, validated but never defaulted. This spec selects no threshold
   and needs no constant.
2. **Leland's ~50% turnover reduction does not transfer.** That figure compares his
   optimised region against periodic rebalancing to target at matched tracking error. Our
   region is declared, not optimised, and our trigger is threshold-based rather than
   periodic. Neither the magnitude nor the dominance claim carries over, and neither is
   claimed here. What carries is the *direction*: at an identical trigger, trading to the
   near edge moves strictly less than trading to target.
3. **The model assumptions differ** — symmetric declared band, a currency floor on trade
   size, discrete instrument quantities, broker frictions. The mapping is one published
   result applied to a differently-derived region, not a re-derivation of his optimum.

Rebalancing to target is also a documented practice, and it is fair to note current
Vanguard material treats the destination as a *policy parameter* (target, midpoint, or
another interior point) rather than a settled rule. Given both are available, we take the
one that moves least per trigger, because turnover is this repo's declared first-order cost
filter (`.claude/skills/quant/strategy-evidence.md`).

**Where no published rule governs, the choice is made by construction and labelled as
such.** Four decisions below are of that kind and each is marked **[by construction]**:
the strict trigger (Q2), floor-over-reserve precedence (Q4), refusal precedence, and the
rounding direction. None is presented as derived.

## The decision

Denominator `V = core_market_value + cash_balance`, the core sleeve. Two holdings and
nothing else, so weights are exact complements (item 1's spec, "Cash is derived").

```
core_pct   = 100 * core_market_value / V
upper      = core_target_pct + rebalance_band_pct
lower      = core_target_pct - rebalance_band_pct

core_pct > upper   -> sell_core, sized to leave core_pct == upper
core_pct < lower   -> buy_core,  sized to leave core_pct == lower
otherwise          -> hold
```

Pre-cost, a core/cash trade leaves `V` unchanged (cash falls by exactly what core rises
by), so the amount is the gap to the near edge:

```
raw_amount = | edge/100 * V - core_market_value |
```

`action` carries the direction; `amount` is a **non-negative magnitude in
`base_currency`**, never a signed delta. It is a **currency amount, not a quantity** —
conversion to instrument units, unit rounding and the residual that leaves is the execution
half's, and is not modelled here.

### Rounding **[by construction]**

`raw_amount` is quantised to six decimal places with `ROUND_DOWN`, matching the
`NUMERIC(18,6)` amount shape item 1 fixed. Rounding down **never trades more than the
boundary demands** (it trades the same, when the value is already representable, or less),
so a rounding step cannot overshoot the edge into the far side of the band.

It does leave the state fractionally outside the band, and that cannot loop: the residual
is strictly below one quantum, `1e-6`, while `min_rebalance_amount > 0` on a `NUMERIC(18,6)`
column is at least `1e-6`. So on the next evaluation the residual is strictly below the
effective floor and suppresses — for every mandate the schema can hold. Tested.

### The five open questions

**Q1 — a price gap can put cash below the reserve between rebalances.** It never needs a
second trigger. For any mandate satisfying item 1's CHECKs:

```
reserve breached  <=>  cash_pct < reserve  <=>  core_pct > 100 - reserve
sql/336 CHECK     :    100 - (core_target + band) >= reserve
                  <=>  core_target + band <= 100 - reserve
therefore         :    core_pct > 100 - reserve >= core_target + band  =>  core_pct > upper
```

So **a reserve breach strictly implies an upper-band breach**. The band dominates the
reserve; a separate reserve trigger would be unreachable code.

⚠ **This holds for schema-valid mandates only, and `CoreMandate` is a public frozen
dataclass anyone can construct directly.** The allocator therefore re-runs
`validate_core_mandate` over the mandate it is given and refuses
`core_mandate_invalid` rather than computing on a state the proof does not cover. Without
that step the "always" above is false, not merely unproven.

The verdict still carries `reserve_breached`, because "the rebalance that would have fixed
it was suppressed" is a state an operator has to be able to see (Q4).

**Q2 — does the band trigger at equality, or strictly outside? Strictly outside. [by
construction]** This is a choice, not a derivation: that `sql/336`'s CHECK is *written
about* `core_target + band` shows the schema treats the edge as a storable state, but
storability does not settle actionability, and the earlier draft of this spec claimed it
did. The construction is that a band is an **allowance**: an allowance consumed exactly is
still within the allowance, and drift must exceed it to spend a trade. The schema is
consistent with that reading — at equality the reserve CHECK is satisfied, so the strict
trigger never leaves a reserve breach unactioned (Q1's chain uses `>=` on the CHECK and
`>` on the trigger, and holds at equality).

**Two degenerate mandates make one trigger unreachable, and the schema permits both.**
`sql/336`'s `core_target_pct - rebalance_band_pct >= 0` permits `band == target`, giving
`lower == 0` — and `core_pct < 0` is impossible for a non-negative sleeve, so the lower
trigger can never fire. That is exactly the "silently one-sided" state the CHECK's own
comment says it exists to prevent; the comment is right and the comparator is off by one.
Symmetrically, `band == 100 - target` with `reserve == 0` gives `upper == 100`, which
`core_pct > 100` can never exceed. Both are narrow, neither is unsound arithmetic here —
the allocator computes correctly, one side simply never triggers — and fixing the CHECK is
a migration outside this slice. **Filed as its own ticket; recorded here so the allocator's
behaviour on such a mandate is documented rather than surprising.**

**Q3 — target or edge, and what about costs? Edge, per Leland. Costs are NOT answered
here, and this spec does not claim they are.**

What can be stated exactly, for the one cost shape the algebra covers — **a fee deducted
from cash, not embedded in the execution price**: the fee shrinks `V`, and for a ratio
`a/b` with `a < b`, `(a-f)/(b-f) < a/b`. So such a fee pushes post-trade cash percentage
below its pre-cost value in both directions. Two consequences, and the earlier draft caught
only the first:

- **The reserve.** Pre-cost post-trade cash is `100 - core_target - band` on a `sell_core`,
  which `sql/336`'s CHECK guarantees is `>= reserve` but permits to be **exactly** equal.
  On such a mandate any fee breaches the reserve.
- **The band itself.** A sell sized to the pre-cost upper edge lands at
  `post_core_pct = upper·V/(V-f) > upper` — still outside the band it was meant to restore.
  Costs threaten the trigger invariant, not only the reserve.

The buy side's pre-cost margin over the reserve is `100 - core_target + band` minus
`reserve`, which is at least `2 * band` by the CHECK. That is a statement about the
pre-cost margin and **not** a claim that no fee can breach it: no bound on fees or floor on
sleeve value is available here, so no such claim is made.

**Why this slice does not solve it.** #2598 closed on exactly this point: the banded cost
model is the *backtest's*, and **execution holds the broker's own number at entry time**.
The allocator sits upstream of that number, and inventing one would repeat the mistake
#2598 was closed to avoid. Nor is "widen the sell by the fee" the right formula — reaching
`upper` after a cash-deducted fee `f` requires adding `upper/100 · f`, restoring the
reserve requires adding `(1 - reserve/100) · f`, and if the fee is size-dependent it is an
implicit equation rather than a widening at all.

So the allocator returns `reserve_margin_pct` — the pre-cost margin, defined below — and
the obligation is stated where it belongs: **the execution half must re-solve the size
against the cost it is actually quoted, and refuse if it cannot restore both the band and
the reserve.** A margin of zero is the signal that it has no slack to do it from.

**Q4 — precedence when the floor suppresses the reserve-restoring trade. The floor wins;
no trade; `reserve_breached` is reported. [by construction]**

The allocator has no authority to trade through one operator declaration to satisfy
another, and this repo's posture on an unresolvable gate is to refuse and name it rather
than pick a side silently. *(The earlier draft also argued the floor exists because a
sub-floor trade costs more than the drift it corrects. That is unsupported — an operator
minimum may equally encode broker mechanics or policy — and it is withdrawn.)*

The breach this leaves is bounded. A reserve breach implies `core_pct > 100 - reserve >=
upper`, so the sell-to-edge amount is at least the currency shortfall:

```
sell amount = (core_pct - upper)/100 * V  >=  (core_pct - (100 - reserve))/100 * V = shortfall
```

If the sell is suppressed, the shortfall is below **the effective floor** —
`max(min_rebalance_amount, broker minimum)`, not `min_rebalance_amount` alone, since the
broker half may be the binding one. Two limits on reading that as safety: it is a
**nominal** bound in `base_currency`, **not a proportional one** — on a small sleeve a
sub-floor shortfall can still be a large share of the reserve — and it is proved on the raw
pre-cost amount, so the implementation compares the **rounded** amount against the floor and
the bound inherits one quantum of slack. Stated, tested, not called safe.

**Q5 — the core position class. Unchanged, and still deferred.** Item 1's spec verified the
entry/position plane cannot hold a stop-less, take-profit-less core position: `sql/287:116`
CHECKs `stop_loss_rate > 0 AND take_profit_rate > 0` on an `allocated` verdict;
`app/services/strategy_position_manager.py:816` makes a stop-less holding a permanent
`fixed_exit_repair` condition; `:817` exact-matches the take-profit, so a core holding would
need one forever. This slice emits no order intent, so it neither needs nor creates that
class — it does not answer the question, it stays out of its way.

## Contract

### Input — `CoreSleeveState`

| field | rule |
| --- | --- |
| `core_instrument_id` | `int > 0`. Must equal `mandate.core_instrument_id` or the verdict is `sleeve_instrument_mismatch` — an allocator that weighs some *other* holding as the core is the failure this closes |
| `core_market_value` | `Decimal`, finite, `>= 0`, in `currency` |
| `cash_balance` | `Decimal`, finite, `>= 0`, in `currency` |
| `currency` | must equal `mandate.base_currency`; both components already converted into it |
| `as_of` | `datetime`, one valuation instant for **both** components |

**Caller obligations, stated because the arithmetic depends on them and the allocator
cannot check them.** The supplier of a `CoreSleeveState` warrants that: both components are
valued at `as_of` from one snapshot, not two; `core_market_value` is that one instrument's
net long value, with lots netted and no other holding folded in; `cash_balance` is settled
and unreserved, with pending orders, unsettled proceeds and accrued charges already
deducted; and no rebalance from a previous verdict is still in flight — this function is
stateless and will re-recommend an in-flight trade. `as_of` staleness is the caller's rule
to set; the allocator holds no clock and enforces none.

**Eligibility is not gated here.** Item 2 owns the proof that the core instrument is the
underlying product and not a CFD, and it has no table yet. Adding a placeholder input would
be the provenance theatre item 1's spec refused. So it is a caller obligation: **a
`buy_core` verdict is not an eligibility finding, and the execution half must obtain item
2's proof before acting on one.**

### Output — `CoreRebalanceDecision`

| field | rule |
| --- | --- |
| `action` | `"hold" \| "buy_core" \| "sell_core" \| "refused"` |
| `reason_code` | `None` on an **in-band** hold — inside the band is the mandate working, and that is not a refusal. Set on a **suppressed** hold (`below_min_rebalance_amount`) and on every `refused` |
| `amount` | non-negative `Decimal`, quantised; `0` on `hold`/`refused` |
| `core_pct`, `target_pct`, `lower_pct`, `upper_pct` | the arithmetic the verdict used; `None` when no weight could be computed |
| `effective_floor`, `floor_source` | the floor applied and whether `"mandate"` or `"broker"` won |
| `reserve_breached` | current-state breach, `None` when no weight could be computed |
| `reserve_margin_pct` | **the pre-cost margin over the reserve in the state this verdict leaves you in** — post-trade for a trade, current-state for a `hold`, `None` on a refusal or an uncomputable weight. One meaning, stated, because the earlier draft left it ambiguous between three |

`broker_minimum` is an optional input. `None` means **the caller has no applicable minimum
to supply**, and the mandate floor stands alone; the verdict says so via `floor_source`.
⚠ Whether a given broker minimum applies to an incremental buy or a partial sell is the
**caller's** determination — item 1's spec cited `min_position_amount`, which is an
entry-path field, and nothing here establishes it governs a rebalance leg.

### Refusals, in precedence order **[by construction]**

A **refusal is "cannot decide"**, and it nulls the arithmetic fields because there is no
arithmetic to report. It is a different outcome from **"decided not to trade"**, which is a
`hold` — either in-band (no code) or suppressed by the floor (`below_min_rebalance_amount`,
carrying the full arithmetic). Keeping the suppressed case a `hold` is what makes Q4
observable: a refusal that nulled `reserve_breached` and `reserve_margin_pct` would hide the
exact state Q4 exists to surface.

Evaluated in this order, first match wins; one code per verdict, matching the single
`reason_code` shape the paper executor already uses. Mandate validity precedes state
validity because a decision computed from an invalid authority is meaningless whatever the
state was. Within the mandate half, the **policy version is checked before validity**:
`validate_core_mandate` implements *this* version's arithmetic, so reporting "invalid" for a
row written under a later policy would blame the row for our own staleness.

| # | code | when |
| --- | --- | --- |
| 1 | `core_mandate_absent` | no mandate ever configured. `load_core_mandate` returns `None`, which item 1 declared a state, not a default |
| 2 | `core_mandate_policy_unsupported` | `policy_version` is not the one this arithmetic implements. What item 1 stamped the column *for* |
| 3 | `core_mandate_invalid` | the mandate fails `validate_core_mandate`. Closes the directly-constructed-object hole in Q1's proof |
| 4 | `core_mandate_disabled` | latest revision has `enabled = false` |
| 5 | `core_instrument_unset` | enabled with no instrument. Unreachable via the CHECK and via `validate_core_mandate`; kept because the dataclass is constructible |
| 6 | `sleeve_currency_mismatch` | observed currency is not the mandate's. Otherwise the allocator weighs two currencies as one |
| 7 | `sleeve_instrument_mismatch` | observed instrument is not the mandate's |
| 8 | `sleeve_valuation_invalid` | a component is non-finite or negative, or the **sleeve** reaches `NUMERIC(18,6)`'s 12 integer digits. Negative cash is borrowed money and leverage is barred (`.claude/CLAUDE.md` risk posture: *"No leverage — still barred"*); non-finite is refused before any comparison, since `Decimal("NaN") < 0` raises rather than returning False. The magnitude bound is checked **per component and then on the sum**, in that order. The sum is the bound that carries the contract — an amount is at most the sleeve, so bounding the sleeve is what makes every `amount` expressible in the shape the contract claims and makes the quantise step provably unable to raise. The per-component check is not a shortcut to it: two finite components near `Decimal`'s `Emax` raise `decimal.Overflow` **on the addition itself**, escaping the refusal entirely. Found at Codex checkpoint 2, by probe rather than by reading |
| 9 | `broker_minimum_invalid` | a supplied broker minimum is non-finite or `<= 0` |
| 10 | `core_sleeve_empty` | `V == 0`. A zero denominator is a state, not a division |

And the one non-refusal outcome code:

| code | when |
| --- | --- |
| `below_min_rebalance_amount` | outside the band, but the **rounded** amount is `< effective_floor`. `action` is `hold`, with the full arithmetic. Comparison is on the rounded amount because that is what would execute; equality with the floor **acts**. This also absorbs the degenerate case where rounding leaves `amount == 0`, so no zero-amount trade can ever be emitted |

## What this does NOT do

- No order intent, no broker call, no execution, no eligibility proof.
- No endpoint, no job, no scheduler entry.
- **No migration and no persistence.** The decision is recomputed from state; storing it
  would create a verdict with a lifetime and no consumer to expire it.
- No FX — the currency guard refuses a mismatch rather than converting. #2363 owns FX and
  it is unmodelled.
- No cost model, and no post-cost guarantee about either the band or the reserve (Q3).
- No reconciliation against the broker's position list, and no staleness rule.

Refs #2603. Refs #2525. Refs #2437. Refs #2598.
