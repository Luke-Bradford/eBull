# Core rebalance sizing against the cost actually quoted (#2603 item 3, step 3b-2 item 1)

Status: proposal. Supersedes the sizing correction derived in
`docs/proposals/ta/2026-08-13-core-cash-allocator.md` §Q3, which does not cover our cost
shape.

## Why the inherited derivation does not apply

Q3 derives its two corrections for **a fee deducted from cash, not embedded in the
execution price**, and says so explicitly. #2598 decoded what we are actually quoted, and
for an unleveraged long the only non-zero component is `marketSpread` — the cost of
crossing the book, which is **embedded in the execution price**. So the one formula already
written down is derived for a cost shape we are not given.

The falsification is recorded on #2603 (2026-08-14). This document does the derivation that
comment says is owed.

## Source rule

No published formulation governs "re-solve a rebalance size against a price-embedded
spread"; the arithmetic below is derived **by construction** from the sleeve's own
definition and frozen in `CORE_SIZING_POLICY_VERSION = "core-sizing-v1"`. What is *not*
invented, and what the derivation stands on:

| fact | source |
| --- | --- |
| the sleeve is marked at `sum(amount + unrealizedPnL.pnL)`, which lands on `quotes.last` | `app/providers/broker.py:384-391` (`direct_long_market_value` docstring, #2704) |
| `marketSpread` is **monetary**, in the row's own `currency` — the live response ships `value` and omits `amount` **as a key**, re-verified against the portal 2026-08-12 | #2598; `.claude/skills/data-sources/etoro-api.md` (scaling · same-quantity · rounding-quantum) |
| `value / ticket` matches the **full** quoted spread, median 0.995x over 60 instruments — compare against `p75_spread_pct`, **never** `half_spread_pct` | same |
| it is order-of-magnitude, **not** an identity (AAPL 0.3 → 1.3 → 1.3 bps against a quote moving 0.33 → 3.63) | same |
| an **absent** `marketSpread` row is not a zero cost; costs round to a 0.01 USD quantum | same |
| `marketSpread` scales ~linearly with ticket size (1x→10x moves it 9.93-10.14x) | same |
| `upper <= 100 - reserve` for every schema-valid mandate | `sql/336_strategy_core_mandate.sql:50` — `CHECK (100 - (core_target_pct + rebalance_band_pct) >= liquidity_reserve_pct)` |
| `lower >= 0` | `sql/336:43` |

### Full-population verification — where the mark sits in the book

The derivation would need to know the mark's position between bid and ask, if it estimated
rather than bracketed. Measured on both stored populations, 2026-08-14:

```sql
select count(*) n,
       count(*) filter (where last is null)                  last_null,
       count(*) filter (where last < bid)                    below_bid,
       count(*) filter (where last = bid)                    eq_bid,
       count(*) filter (where last > bid and last < ask)     inside,
       count(*) filter (where last = ask)                    eq_ask,
       count(*) filter (where last > ask)                    above_ask
from quotes;                       -- and again on strategy_quote_observations
```

| population | n | null | `< bid` | `= bid` | inside | `= ask` | `> ask` |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `quotes` — one current row per instrument | 1,632 | 8 | 0 | **1,623** | 1 | 0 | 0 |
| `strategy_quote_observations` — 8 instruments, 2026-08-10 → 08-14 | 1,640 | 0 | 0 | **1,640** | 0 | 0 | 0 |

The single exception is **EURUSD**. That is consistent with the etoro-api skill's arm table
— `LastExecution == BidDiscounted` at 100.0% on US equity, 97.8% on FX — so the one
disagreement sits in the arm already documented to disagree; consistent is not the same as
proved, and one row cannot establish its own cause. `quotes.last` is `lastExecution`
(`app/providers/implementations/etoro.py:716-727`) and `quotes.bid` is the REST `bid`
(`:674`); their equality is what identifies our stored bid as the same series the skill
calls `BidDiscounted`.

⚠ **Neither population is the population that matters.** The first is broad (1,632
instruments) but one instant; the second is deep (4 days) but eight instruments. And
neither observes a **fill** — nothing in this repo ever has. This is why the sizing
brackets rather than estimates: the reading below is an explanation, not a load-bearing
input.

**Reading, and it is asymmetric.** A buy executes at the ask against a bid mark, so it
books close to the full spread immediately. That is the same order of magnitude as the
measured `marketSpread ≈ full quoted spread`, which makes the two readings *coherent* —
⚠ but **not mutually confirming**: the skill's own correction records that `quotes` and the
what-if endpoint are two endpoints of one venue, so they are not independent evidence for
any question about the level. A sell executes at the bid *into* a bid mark, so to first
order it books nothing. That is a prediction the operator-attended session can check.

## The derivation

All quantities on the mark, all fractions of 1. `V = M + C` is the pre-trade sleeve;
`e` is the near edge the allocator chose (`lower` for a buy, `upper` for a sell);
`μ` is the fraction of the **ticket** that leaves the sleeve as cost.

### Buy of ticket `A`

```
core' = M + A(1 - μ)      cash' = C - A      V' = V - μA
f(μ)  = (M + A(1 - μ)) / (V - μA)          strictly DECREASING in μ
```

Solving `f(μ) = e`:

```
A = (eV - M) / (1 - μ(1 - e))
```

### Sell of mark-value `S`

```
core' = M - S      cash' = C + S(1 - μ)      V' = V - μS
g(μ)  = (M - S) / (V - μS)                 strictly INCREASING in μ
```

Solving `g(μ) = e`:

```
S = (M - eV) / (1 - μe)
```

Both are **larger** than the pre-cost amount `A₀ = |eV - M|`, and both correct by a factor
involving the *other* side's weight. Neither resembles Q3's `+ upper/100 · f`: a
cash-deducted fee makes a buy **smaller** and a sell **larger**, a price-embedded one makes
both larger, and the multipliers differ. Substituting `f` for `μA` is not a small error.

⚠ Q3 warned the size-dependence could make this an implicit equation. Holding **μ constant**
collapses the fixed point into the closed form above, and that constancy is an
**approximation**, not an elimination: the supporting evidence is 9.93-10.14x over a factor
of ten (about ±1.4% on the rate) at selected instruments, two ticket sizes, one direction.
It is not a proof that `γ` bounds the rate at every size. What keeps the approximation
honest is that the solve is a *small correction* on the quoted ticket, and
`cost_quote_ticket_mismatch` refuses rather than extrapolating when it is not.

### The bracket — what it does and does not buy

`μ` is not known. What is known is an upper bound `γ` from the quote. So instead of
picking a point estimate we **size at `γ` and require the answer to hold across the whole
interval `μ ∈ [0, γ]`**:

- `f` is decreasing, so a buy sized at `γ` satisfies `f(μ) ≥ lower` for every `μ ≤ γ`.
  The remaining risk is at the *other* end: `f(0) = (M + A)/V` must not overshoot `upper`.
- `g` is increasing, so a sell sized at `γ` satisfies `g(μ) ≤ upper` for every `μ ≤ γ`.
  The remaining risk is `g(0) = (M - S)/V` undershooting `lower`.

One check at each end of the bracket, and **the mark's position never enters**. If the
bid-mark reading above is wrong, or a fill lands elsewhere in the book, the answer is still
admissible.

⚠ **What bracketing does NOT buy is independence from the quote.** The whole guarantee is
`μ ≤ γ`, so it rests entirely on `γ` being a true upper bound on the realised cost rate.
Bracketing removes one unknown (where the mark sits), not two. Three named consequences:

- A cost the response does not enumerate is outside `γ` and outside the guarantee. This is
  why §"Where `γ` comes from" sums **every** returned row rather than `marketSpread` alone.
- `μ` is treated as a scalar rate. Slippage, a partial fill at several prices, and any
  price move between sizing and execution are **not** representable that way and are not
  bounded here. They belong to the execution half.
- Rounding of the *returned* size must not break the near-edge end (see §Contract).

### The reserve is not a second constraint

Post-trade cash fraction is `1 - core fraction` in every case above, because both are
ratios of the same post-trade sleeve `V'`. So:

```
core fraction ≤ upper   ⟹   cash fraction ≥ 1 - upper   ≥ reserve      (sql/336:50)
```

On a buy the far-edge check bounds `f(0) ≤ upper` directly, and `f` is decreasing so every
`μ` is bounded too. On a sell the near-edge solve gives `g(γ) = upper` and `g` is
increasing, so `upper` is the maximum over the bracket. Either way the post-trade core
fraction is at most `upper`, so **restoring the band restores the reserve** for a
schema-valid mandate. Q3's second correction (`(1 - reserve/100)·f`) is the weaker of its
two requirements and is subsumed, not contradicted. `reserve_margin_pct == 0` still means
no slack — it now means the far-edge check is the one that will bite.

⚠ Three limits on reading that as safety. It needs `V' > 0`, which `γ < 1` gives. It is an
implication of `core ≤ upper` and nothing more — *any* policy with that guarantee gets it,
so edge-solving is how we obtain the bound, not why it implies the reserve. And it is a
statement about the **two-component model**: charges outside the sleeve, delayed fees,
reservations, unsettled proceeds and accrued charges are exactly the cash warranties
`CoreSleeveState` says are only partly sourced (`app/providers/broker.py`, #2704). The
reserve is protected against the cost we are quoted, not against everything.

## Where `γ` comes from

`γ = cost_upper_bound / quoted_ticket`, decoded from one `BrokerWhatIfCostResponse`:

1. **Sum every returned component**, not `marketSpread` alone. The vocabulary is
   provider-owned and `markup` / `overnightFee` read `0.0` today but are refused the moment
   they do not (#2598). A component we have never seen must widen the bound, never be
   dropped.
2. **`amount` if present, else `value`.** The portal documents `amount`; the live response
   ships `value` and omits `amount` **as a key**. #2598 decoded `value` as monetary. Neither
   is silently substituted for the other — the source is recorded on the result.
3. **One rounding quantum PER COMPONENT is added to the sum, so an absent row can never
   read as zero.** Costs come back rounded to 0.01, so a component that rounds away is
   `< 0.01` — a bound, not a coercion.

   ⚠ Adding it *only on absence* would be the subtler bug: a response whose rows are all
   present can still be one rounded-away component short, and membership testing cannot see
   that. Paying it always removes the need to reason about `"marketSpread" in rows` at all.

   ⚠⚠ And adding **one** quantum per *response* is the subtler bug still, caught at
   checkpoint 2. Rounding is applied per row, so N components each understate
   independently: three rows reported as zero can each stand for a real cost just under
   0.01, bounding a realised 0.03 at 0.01. That is an under-statement of `γ`, which is the
   single direction the `μ ≤ γ` guarantee cannot survive. The slack is therefore
   `0.01 × max(rows, 6)` — **6** being the documented vocabulary (`markup`, `marketSpread`,
   `transactionFee`, `overnightFee`, `overWeekendFee`, `sdrt`), used as a *count*, not as a
   validation list, because an omitted component is precisely the one that rounded away and
   contributes no row to count. At most 0.06 on any ticket.

   ⚠ **A refusal here risks the 3b-1 trap.** Omission has been observed on AAPL and,
   separately, on SPY — two observations, from which the skill reads the trigger as the
   live spread rather than the instrument. That is a small record and is not asserted as a
   rule here; what it does establish is that omission happens on exactly the tight, heavily
   quoted names a core sleeve holds (SPY's stored spread is 0.0026%). A refusal on absence
   would therefore stand a real chance of refusing the core rebalance permanently while
   looking transient. The quantum bound is the explicit widening #2603's comment asks for,
   and it is safe in the direction that matters: `< 0.01 USD` is an upper bound whether the
   row was omitted for rounding or for any other reason, provided the component is one the
   census establishes this product and side **does** return.
4. **A present-but-null value is a refusal**, not a quantum bound. That is a malformed row,
   not an omitted one. It has not been observed (`market_spread_value_null` is `false` on
   every observation so far, which is a limited record, not a schema guarantee) — and an
   unobserved shape is precisely the one to refuse rather than to bound by analogy with the
   omitted case.
5. **`amount` and `value` both present and disagreeing is a refusal.** Neither may be
   silently preferred when the response contradicts itself; that is drift, and #2598's rule
   is to preserve both and fail rather than pick.
6. **Every included row's `currency` must be the mandate's base currency** — per row, not
   once for the response. Duplicated `costType` rows are summed rather than de-duplicated:
   summing over-bounds, de-duplicating could under-bound, and only one of those directions
   is safe.
7. **The quote must not be older than the valuation it is sizing.** `lastUpdated` staleness
   is per-instrument and has been observed at 26 days (#2598). The bound is relative and so
   invents no constant: refuse when `last_updated < state.as_of - _CLOCK_SKEW`. ⚠ Two
   clocks — `state.as_of` is our receipt time and carries no broker valuation stamp
   (#2704) — which is what the skew allowance is for, and it is named rather than tuned.

   ⚠ Unlike the quantum case, a chronically stale instrument refusing **for ever** is the
   correct outcome, not the 3b-1 trap: we genuinely cannot price the trade, and no producer
   of ours is failing to cover a population. The census read SPY and AAPL seconds old.

## Refusals

Named, closed vocabulary, and all four are reachable:

| code | when |
| --- | --- |
| `cost_quote_unusable` | no cost rows · a present row with a null value · `amount` and `value` both present and disagreeing · a non-finite or negative figure · a non-positive quoted ticket · a row currency that is not the mandate's base · a response for another instrument |
| `cost_quote_stale` | `last_updated < state.as_of - _CLOCK_SKEW` (rule 7 above) |
| `cost_rate_implausible` | `γ ≥ 1` — the quote claims the cost is the whole ticket. Also the guard that keeps both denominators positive |
| `cost_quote_ticket_mismatch` | the solved size and the quoted ticket differ by more than `_MAX_TICKET_EXTRAPOLATION` in **either** direction (`max(a,b)/min(a,b) > 2`). The linearity evidence spans one decade at selected instruments and two sizes; the solve is a small correction on the quoted ticket, so this should not fire — and it fires rather than extrapolating silently |
| `cost_breaches_far_edge` | the `μ = 0` end lands outside the band, checked on the **returned, rounded** amount. The spec's obligation — *refuse if it cannot restore both* — with the reserve folded in by the proof above |

### ⚠ There is deliberately NO fundability check

An earlier draft carried `cost_exceeds_available_cash`. It cannot fire. A buy exceeds cash
exactly when `A > C`; substituting `A = A₀/(1 - γ(1-e))` and `C = V(1-e) + A₀` reduces that
to **`γ > V/C`**, and `C ≤ V` makes the threshold at least 1 — so `cost_rate_implausible`
has already refused. The sell side is likewise always fundable: `S ≤ M` follows from `γ < 1`.

Removed rather than kept-and-documented, because a refusal that cannot fire is #2437's R4
shape — *a control on a path the decision does not take* — which is the single most repeated
defect on this ticket. Confirmed empirically as well as algebraically: a sweep over every
`(target, band, reserve) × sleeve × γ` combination the schema admits produced
`cost_breaches_far_edge` and `cost_quote_ticket_mismatch` and **never once** produced this
one. The sweep is the reachability test in the test module.

⚠ The floor is **not** re-applied. `evaluate_core_rebalance` already compared the pre-cost
amount against `max(min_rebalance_amount, broker minimum)`, and the re-solve only ever
*increases* the size, so an amount that cleared the floor still clears it. Re-checking would
be dead code that reads as a control. This is sound for the floor **as the allocator defines
it** — a currency amount. Conversion to units, unit rounding, and any broker minimum
expressed in units are the execution half's, and are not done here.

### ⚠⚠ The sell path cannot be quoted at all today — and that is a real blocker

`EtoroBrokerProvider.get_what_if_costs` hardcodes `"action": "open"`
(`app/providers/implementations/etoro_broker.py:750`) and `TradeDirection` is
`Literal["buy", "sellShort"]` (`app/providers/broker.py:23`). Both arms are **opens**.
A core `sell_core` is a partial close of a long, and there is no endpoint call in this repo
that will price one.

So `resolve_core_trade_size` will size a sell correctly the moment it is handed a bound,
and nothing can hand it one. Naming it here rather than letting the sell path look
implemented: this is the 3b-1 shape (*a refusal that looks transient and is not*) pointed at
the direction that **restores the reserve**. Obtaining a close-side cost quote is the broker
half of the refusal vocabulary — 3b-2 item 3 — and is filed separately so it is not lost.
This slice does not fabricate a bound from the opposing arm.

## Contract

`app/services/strategy_core_sizing.py`, pure: no connection, no clock, no broker, no writes.

```python
resolve_core_trade_size(mandate, state, decision, cost) -> CoreSizingResult
decode_quoted_trade_cost(response, *, ticket_amount, base_currency) -> QuotedTradeCost | CoreSizingRefusal
```

`resolve_core_trade_size` takes the edges and the action from the `decision` and re-derives
`V` from `state`. It refuses `hold` / `refused` actions outright, and **enforces** the pair
it was handed rather than trusting it: `decision.core_pct`, `lower_pct` and `upper_pct` are
recomputed from `(mandate, state)` and must match, else `ValueError`. A structurally valid
decision from a *different* state would otherwise size against the wrong sleeve, and
"caller obligation" is not a control.

⚠ **Rounding is `ROUND_UP` on magnitude, and the reversal from `_quantise_down` is
deliberate.** Rounding down moves the answer back toward the pre-cost size, so at `μ = γ` a
rounded-down buy still sits **below** `lower` and a rounded-down sell still sits **above**
`upper` — the near edge the solve exists to restore is the one rounding would break, and the
"residual below one quantum, suppressed next pass" argument does not cover it because that
residual now also carries cost-model error. Rounding up overshoots the near edge into the
band instead, which is admissible, and the far-edge check runs on the **rounded** value, so
what the function guarantees is a property of what it returns.

### What the response cannot tell us

`BrokerWhatIfCostResponse` carries `instrument_id`, the cost rows and `last_updated`. It
does **not** echo the ticket, the direction, the settlement type, the leverage or the
account. So `ticket_amount` is caller-declared and **unverifiable against the response** —
a quote fetched for the wrong size or side decodes to a plausible `γ` with nothing to catch
it. The instrument is the one identity that *is* checkable, and it is checked. The rest is
named in the docstring as a caller obligation, because inventing a check that cannot fail
would be worse than the gap.

## What this does NOT do

- **It authorises nothing.** No caller in `app/` or `scripts/`; nothing calls the broker to
  obtain the quote it decodes. Sixteenth time this arc has had to say so, and the reason is
  #2437's R4: a control on a path the decision does not take.
- It does not observe a fill. Every claim about execution price is inference from quotes,
  and slippage, partial fills and price movement between sizing and execution are unbounded
  here.
- It does not settle the sell-side cost, and it does not make the sell path usable — no
  close-side quote can be obtained today (see above).
- It does not touch `evaluate_core_rebalance`. The stored verdict stays cost-free, which is
  correct: the quote is obtained at submission, downstream of `record_core_rebalance_intent`.

Refs #2603. Refs #2598. Refs #2704. Refs #2437.
