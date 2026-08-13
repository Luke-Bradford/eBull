# The core/cash mandate object (#2603 scope item 1)

Status: proposed. Slice 1 of #2603 only — the mandate object and its validation. The
allocator, the eligibility proof and the rebalance path are **not** in this slice.

## Why this slice, and why it is only a slice

#2603's stop-condition outcome is "return to core/cash": hold benchmark exposure and cash
per the operator's mandate, take no active trade, rebalance inside bands. Its dependency
note sanctions starting here — *"schema and allocator logic do not [need #2598/#2602/#2363]
— start now, finish behind them."*

Items 2 (account-specific eligibility proof that the core instrument is the underlying
product, not a CFD) and 3 (the rebalance path) need a live eToro session and an
operator-attended demo. Neither is reachable from a headless worktree, so neither is here.

## What the ticket's premise got wrong

Both corrections are measured, not argued.

**A mandate object already exists.** #2603's measured gap greps
`core_allocation|core_weight|core_exposure|cash_allocation` — which does return nothing —
and concludes the mandate is unbuilt. But `PortfolioMandate`
(`app/services/strategy_control_plane.py:98`) is already a versioned, audited mandate:
`MANDATE_POLICY_VERSION` (`:40`) freezes the label→limits table, `mandate_for_profile`
(`:166`) resolves it, and every field is persisted as a column on the append-only
`strategy_paper_pool_events` by `configure_paper_pool`
(`app/services/strategy_control_plane.py:304-334`). It already carries `cash_reserve_pct`,
which is a liquidity reserve — though not this one; see the denominator section.

So item 1 is an extension, and the only real design question is where it attaches.

**There are five USD lock sites, not three.** The ticket names
`strategy_paper_executor.py:372/:553/:591` (now `:374/:555/:593`, shifted by the #2363
merge). Two more sit in the capital-authority layer, above the executor:

| site | form |
| --- | --- |
| `sql/290_strategy_live_promotion_gate.sql:96` | `currency TEXT NOT NULL CHECK (currency = 'USD')` |
| `app/services/strategy_control_plane.py:313` | `'USD'` written as a SQL literal in the INSERT |

That makes item 4's *"never a partial lift"* a stronger constraint than the ticket knew:
the base currency is not merely unhandled downstream, it is unrepresentable upstream. This
slice adds a **sixth** such site (below), so the coordinated lift is six, not three.

## Item 3's open question, answered

The ticket asks whether the execution plane supports indefinite stop-loss-less core
holdings, and says to verify rather than assume. **It does not.** Three independent
reasons:

1. `sql/287_strategy_paper_execution.sql:116-121` — `strategy_entry_preflights` CHECKs that
   an `allocated` verdict carries `stop_loss_rate > 0 AND take_profit_rate > 0`. A funded
   position cannot be recorded without both exits.
2. `app/services/strategy_position_manager.py:816` —
   `stop_gap = position.is_no_stop_loss or current_stop is None or current_stop < owned.entry_stop`.
   A stop-less holding is a permanent `fixed_exit_repair` condition, re-driven every pass;
   a repair state, not a supported one.
3. `:817` — `take_gap = position.is_no_take_profit or position.take_profit_rate != desired_take`
   is an exact-match test, so a core holding would need a take-profit forever. A
   take-profit on a benchmark holding sells it at a fixed level, which contradicts holding
   benchmark exposure indefinitely.

**Consequence for #2603 item 3:** core/cash rebalance cannot reuse the entry/position plane
as-is. It needs either an explicit core position class exempt from fixed-exit repair, or a
separate lifecycle. Recorded here so it is not rediscovered at implementation time.

## Design: a separate authority, not more columns on the pool

`PortfolioMandate` resolves a **presentation label** to immutable limits — its docstring
says so, and its equality is what `configure_paper_pool`
(`app/services/strategy_control_plane.py:251-259`) uses to detect a material change. Core
allocation is not label-derived: the core instrument is an operator choice of a specific
instrument. Folding operator input into a profile-resolved object would break that
invariant.

So core/cash gets its own append-only authority, mirroring the shape the repo already uses
for the pool and for `strategy_execution_policies` / `..._policy_events`: one revisioned
event table, no in-place mutation.

`sql/336_strategy_core_mandate.sql` → `strategy_core_mandate_events`. Every column is
`NOT NULL` unless stated; percentages are `NUMERIC(8,4)` and amounts `NUMERIC(18,6)`,
matching `sql/311_strategy_portfolio_mandate.sql`.

| column | rule |
| --- | --- |
| `core_mandate_event_id` | `BIGSERIAL PRIMARY KEY` |
| `revision` | `>= 1`, `UNIQUE` |
| `enabled` | `BOOLEAN` |
| `base_currency` | `CHECK (base_currency = 'USD')` — the sixth USD site, deliberately |
| `core_instrument_id` | `BIGINT REFERENCES instruments(instrument_id) ON DELETE RESTRICT`; nullable, required when `enabled` |
| `core_target_pct` | `>= 0 AND <= 100`. Cash is the complement — see below |
| `liquidity_reserve_pct` | `>= 0 AND < 100`, must fit inside cash |
| `rebalance_band_pct` | `> 0`, units **percentage points of core weight, absolute** |
| `min_rebalance_amount` | `> 0` in `base_currency`; an operator floor, not the broker's. Bounded to the column's 12 integer digits so an oversized value is a named error, not `numeric field overflow` |
| `policy_version` | `= CORE_MANDATE_POLICY_VERSION` |
| `changed_by`, `reason` | `char_length BETWEEN 1 AND 200` / `1 AND 1000`, per neighbours |
| `changed_at` | `TIMESTAMPTZ DEFAULT now()` |

**Cash is derived, not stored.** `cash_target_pct = 100 - core_target_pct` by construction.
A stored second column permits a state that disagrees with the first and makes the
weights-sum CHECK something to chase; deriving it removes the state. Item 1's "weights sum"
validation is therefore satisfied structurally rather than by constraint. This is the
definition of a two-holding mandate — core and cash, nothing else — and that definition is
what makes the complement exact.

### The invariant that carries weight

Item 1 asks for "weights sum, reserve interaction, minimum order size". Sum is structural
(above); minimum order size is not knowable here (below). The reserve interaction is the
one worth deriving:

```
worst-case core weight  = core_target_pct + rebalance_band_pct
worst-case cash weight  = 100 - (core_target_pct + rebalance_band_pct)
require                   100 - (core_target_pct + rebalance_band_pct) >= liquidity_reserve_pct
```

Without it a band can authorise drifting straight through the liquidity reserve — the
reserve would be a number the mandate states and the band contradicts. The CHECK makes the
two agree at write time, and it also bounds `rebalance_band_pct` from above, since it
implies `band <= 100 - core_target_pct - liquidity_reserve_pct`.

A second, narrower one: `core_target_pct - rebalance_band_pct >= 0`, so the band keeps both
triggers inside `[0, 100]`. A band wider than the target leaves the lower trigger
unreachable except by the core going to zero, which makes the band silently one-sided.

`rebalance_band_pct > 0` rather than `>= 0` is a construction choice, not arithmetic: a
zero band authorises a rebalance on any drift at all, and turnover is the first-order cost
filter (`.claude/skills/quant/strategy-evidence.md`). A mandate that cannot state a band
should not be storable.

**Minimum order size is not knowable here.** eToro's minimum arrives on the eligibility
response — `min_position_amount` (`app/providers/broker.py:209`, populated from
`minPositionAmount` at `app/providers/implementations/etoro_broker.py:962`, selected and
checked at `app/services/strategy_paper_executor.py:570-572`). It varies by instrument and
arm, and that check sits on the active-entry path which item 3 cannot reuse anyway. So
`min_rebalance_amount` here is explicitly an **operator floor**, and the effective floor at
execution is `max(declared, broker minimum at the time)` — the broker half, and what to do
when it is unavailable or has risen, is item 3's to build.

## Denominator: what the percentages are shares of

All three percentages are shares of **one core-sleeve denominator**, and every CHECK above
is scale-free in it — sum, reserve containment and band arithmetic hold whatever the
denominator turns out to be.

This slice deliberately does not decide what it is. Whether the core sleeve is the whole
account or a carve-out of the paper pool is #2525's mandate-driven allocation question, and
answering it here would be inventing a second capital authority alongside
`strategy_paper_pool_events.capital_limit`.

⚠ `liquidity_reserve_pct` is **not** the existing `cash_reserve_pct` and must not be
reconciled with it by assumption. The latter is documented as *"Minimum uncommitted cash
share of the effective pot"* (`sql/311_strategy_portfolio_mandate.sql:66-67`) and is
consumed against `pool_base` for the active sleeve
(`app/services/strategy_paper_executor.py:736`). Different denominator, different claimant.
When #2525 fixes the sleeve boundary it must reconcile the two explicitly; until then
neither constrains the other, and nothing in this slice reads either as if it did.

## Source rule

No published formulation governs this, and none is invented.

- **The invariants** are internal authority arithmetic, fixed **by construction** per
  `.claude/CLAUDE.md`'s rule for the case where no published rule exists.
- **The rebalance band takes no default.** Published tolerance-band formulations exist, but
  this slice ships no default value — the band is operator input and the validator only
  bounds it — so no threshold is selected and none needs a citation. The ticket that ships a
  default must find and cite one then.
- **`CORE_MANDATE_POLICY_VERSION` stamps, it does not freeze.** A version string cannot
  freeze a CHECK constraint. What it buys is that every row records which arithmetic it was
  written under, so a later policy change is detectable per row and old rows stay
  interpretable instead of being silently reinterpreted. A change to the invariants is a
  migration plus a new version, never a redefinition of the old one.

**Settled decision that binds the FK:** `docs/settled-decisions.md:901` — *"core allocation
(#2603) — the core instrument is a mandate/eligibility question, not a strategy-validation
one. A non-US-listed core instrument is permitted if its eligibility proof passes."* So
`core_instrument_id` is deliberately **not** constrained to the validated (US, USD)
universe and must not be. It is a plain FK to `instruments`.

**Why this table constrains `enabled` where `sql/311` deliberately does not.** `sql/311:25-27`
refuses an `enabled ⇒ configured` constraint because a legacy enabled event must stay
readable. This table is new and has no legacy rows, so the shape can be enforced from row
one; the writer never needs an unconfigured escape hatch.

## Concurrency and no-ops

`revision` is allocated as `max(revision) + 1` under a dedicated advisory transaction lock,
mirroring `configure_paper_pool`'s use of `pg_advisory_xact_lock`
(`app/services/strategy_control_plane.py:249`) — the `UNIQUE` on `revision` is the backstop,
not the mechanism. A configure call whose values equal the current row is refused rather
than appended, so the audit trail carries material changes only, as
`configure_paper_pool:251-259` already does.

`load_core_mandate` returns `None` on an empty table: no mandate configured is a state, not
a default.

## The deferrals, stated as what this does NOT do

- **It authorises nothing.** No allocator, no order intent, no scheduler entry, no endpoint
  reads this table. Its first consumer is #2603 item 3. Saying so is the point: #2437's R4
  comment records the same defect seven times in one day — *a control that exists, is
  tested, and sits on a path the decision does not take.* This table is state, not a gate,
  and must not be cited as one until something calls it.
- **No actionability predicate and no refusal codes ship here.** Both would have exactly
  that shape — a computed verdict with no caller. Item 3 computes actionability when it has
  something to gate.
- **No eligibility columns ship here either.** A timestamp plus a product-type label cannot
  prove "underlying, not CFD": that needs account identity, the eligibility arm, the raw
  response reference and a freshness rule. Item 2 owns that evidence shape and gets its own
  table. Adding placeholder columns now would be a recorded input nothing generates —
  provenance theatre.
- **Non-USD stays refused, at the writer.** `CHECK (base_currency = 'USD')` is item 4's
  *"one explicit operator-visible refusal if deferred"*, enforced by the only writer that
  exists rather than by a refusal code nothing reads. The service uppercases and validates
  before the CHECK sees it, so `usd` gets a service error rather than a raw constraint
  violation. Lifting it is part of the same change that lifts the other five sites — never a
  partial lift.

## Left to item 3 (execution semantics, recorded so they are not rediscovered)

The mandate is a write-time contract; none of these are answerable by a CHECK constraint:

- A price gap can put cash below the reserve between rebalances. Holding the reserve is an
  execution obligation, not a storage invariant.
- Whether the band triggers at `>=` / `<=` or strictly outside; at equality the CHECK above
  permits cash exactly equal to the reserve.
- Whether a rebalance targets the band edge or the target weight, and how fees, spread and
  slippage are kept from pushing post-trade cash below the reserve.
- Precedence when `min_rebalance_amount` suppresses the very trade that would restore the
  reserve.
- The core position class question above — fixed-exit repair must not apply to a core
  holding.

Refs #2603. Refs #2525. Refs #2437. Refs #2472.
