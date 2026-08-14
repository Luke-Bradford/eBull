# The core rebalance intent record (#2603 scope item 3, execution half — step 1)

`evaluate_core_rebalance` is pure and has no caller. This slice gives it one, and gives
the verdict somewhere durable to land: `strategy_core_rebalance_intents`, the
mandate-driven analogue of `strategy_entry_preflights`.

⚠ **It authorises nothing and submits nothing.** No broker call, no order, no position,
no trade row. See "What this does NOT do" for why that is mechanical here and not a
promise.

## Why the queue's description of the blocker was wrong

#2437's R4 comment and #2603's own prior note describe item 3's execution half as gated
by *"the core position class (`sql/287:116` / `strategy_position_manager.py:816-817`
block a stop-less, take-profit-less holding)"* — i.e. two CHECK-shaped exemptions.

That is not the binding constraint. `sql/287` declares, and the applied dev schema
confirms:

| column | nullable |
| --- | --- |
| `strategy_entry_preflights.signal_id` (also the PK) | NO |
| `strategy_funding_decisions.signal_id` | NO |
| `strategy_trades.funding_decision_id` | NO (also UNIQUE) |

and `strategy_position_manager.py:279-283` is an INNER JOIN through all three. A
mandate-driven holding has no `signal_id`, so it cannot be written at all — and if the
two CHECKs were exempted, it would be **dropped by the join rather than exempted by the
manager**: no age-out, no reconcile, no close path, while looking managed.

## The design decision: an exclusive arc, not a sibling path

Two options were on the issue. Both are rejected in their stated form.

**Option 1 — synthesise a `strategy_signals` row per rebalance.** Rejected, and *not*
for the reason recorded. The prior note said `strategy_signal_daily_counts` "is populated
from `strategy_signals` verdicts", so a synthetic signal would contaminate #2623 gap 2's
fire-rate denominators. **That mechanism is false.** The census is written by
`strategy_observation_storage.py:148` straight from scan observations, and
`signal_ledger.py:296` is the only `INSERT INTO strategy_signals` **in this repository**
(the claim is a grep over our own writers, which is the population for it — it says
nothing about hand-run SQL). They are two writers off one scan, not a derivation, so a
synthetic signal would not reach the census.

The real objection is semantic, and it is the ticket's own. #2603 scope clause 5:
*"**Explicitly NO alpha input**: reads mandate + current positions only; never reads
`strategy_signals`."* The clause separates two populations — signal-derived decisions and
mandate-derived ones — and writing a rebalance into the signal table merges them from the
other side, defeating the separation the clause exists to create. Every downstream
consumer of `strategy_signals` (outcome attribution, the live gate, monitoring) would
then have to re-separate them by a predicate none of them currently carries.

The NOT NULL columns are a *symptom* of that, not the argument: `strategy_id`,
`strategy_version`, `signal_bar_date`, `signal_kind`, `universe` and
`input_rule_set_versions` are all required and none has a mandate-side meaning, so every
one would be a fabricated value in an audit table.

**Option 2 — a sibling ownership path with its own manager pass.** Rejected as stated.
The objection is narrower than "two managers": separate *selection* need not duplicate
management logic, and a shared trades table does not by itself guarantee one lifecycle.
What a separate key does guarantee is that **every existing query that reaches a position
through `strategy_trades` must be dual-written** — measured this run, that is five
modules (`strategy_position_manager`, `strategy_paper_executor`, `strategy_control_plane`,
`strategy_monitoring`, `strategy_live_gate`) plus `app/api/strategies.py`. Each one that
is missed is a core position invisible to reconciliation, gating or status, which is the
same failure the exemption produced.

**Adopted: one exclusive arc.** `strategy_trades.funding_decision_id` becomes nullable
and gains a sibling `core_rebalance_intent_id`, with exactly one of the two non-null.
Consumers that must see both arms then change by relaxing a join rather than by acquiring
a second query, and one that is missed fails loudly on a NULL rather than silently on an
absent row.

⚠ **The arc is NOT in this slice.** It is inert without the manager change and dangerous
without it — it would make exactly the invisible-but-writable core trade this document
opens by rejecting. Arc and manager land together in the next slice, against
`strategy_core_rebalance_intents` as the FK target, which is why the intent record comes
first.

## Source rule

The record's *content* is fixed by `CoreRebalanceDecision`, which
`docs/proposals/ta/2026-08-13-core-cash-allocator.md` already sources (band-boundary
targeting from Leland 2000, no-trade region from Constantinides 1986 / Davis & Norman
1990). Nothing is re-derived here: this slice stores the fields that type already has,
one column each, and adds no arithmetic.

Three treatment decisions are this document's own and are fixed **by construction**, since
no external rule governs any of them:

- **Append-only, one row per evaluation, including holds and refusals.** Same posture as
  `strategy_core_eligibility_proofs` (`sql/346`): a verdict is evidence. Storing only
  actionable verdicts would make "the mandate was evaluated and declined to trade"
  indistinguishable from "the mandate was never evaluated" — and a core sleeve that has
  correctly held for a month is the normal case, so the silent state would be the common
  one.
- **The governing mandate revision is stored as an FK to `strategy_core_mandate_events`,
  not copied.** That table is already append-only and versioned, so the join is lossless;
  copying `core_target_pct` and friends would create a second place for them to disagree.
  The *derived* band edges (`target_pct`, `lower_pct`, `upper_pct`) ARE stored, because
  they are the allocator's output and reconstructing them would re-run its arithmetic to
  audit its arithmetic.
- **The observed state inputs are stored as observed, with no FK and no coercion** — see
  "The unstorable-input trap" below, which is the reason this needed deciding at all.

## The unstorable-input trap **[by construction]**

`_state_refusal` refuses `sleeve_valuation_invalid` on a component that is non-finite,
negative, or `>= _MAX_AMOUNT` (10^12), and `sleeve_currency_mismatch` /
`sleeve_instrument_mismatch` are checked **before** it — so a refusal row can carry a
`Decimal("NaN")` or a 10^30 valuation.

Naively storing the three state inputs into `NUMERIC(18,6)` therefore fails on exactly
the refusals that exist to catch an unrepresentable valuation: the INSERT raises
`numeric field overflow` and the evidence for the failure is the one row that cannot be
written. Another instance of #2437's standing pattern — *the control cannot express a
state the system can reach*.

Construction: **`core_market_value` and `cash_balance` are NULLABLE, and NULL means "the
observed value was not representable in this column's shape; the reason code says what
was wrong with it".** Enforceable half: a NULL in either column implies
`action = 'refused'`, because every non-refused path has already passed `_state_refusal`
and is therefore storable. That is a CHECK, not a comment.

`core_instrument_id` is stored as a plain `BIGINT` with **no FK to `instruments`**, for
the same reason: on `sleeve_instrument_mismatch` the observed id is whatever the caller
supplied and need not resolve. It is an observed input, not a resolved reference.

## Contract

### Writer

```python
record_core_rebalance_intent(
    conn, *, state: CoreSleeveState, recorded_by: str,
) -> CoreRebalanceIntent
```

Loads the live mandate via `load_core_mandate`, calls `evaluate_core_rebalance`, inserts
one row inside the caller's transaction, and returns it with its id. Pure DB — no broker,
no clock beyond `now()`.

A refusal is a returned verdict, not a raise — the allocator's own posture, so a caller
never has to catch to learn the mandate is disabled.

⚠ **`broker_minimum` is deliberately NOT a parameter of this writer**, though the
allocator accepts one. This slice has no broker I/O, so it has no way to *source* a
minimum; accepting one would store a caller assertion with no provenance and no record of
which provider rule was applied. The allocator's own docstring already flags that whether
eToro's `min_position_amount` governs an incremental buy or a partial sell is unsettled.
The executor holds the eligibility response and can answer it with evidence; this writer
passes `None`, which the allocator defines as *the caller has no applicable minimum to
supply*. Consequence, stated so it is not read as a finding: `floor_source` can only be
`'mandate'` in this slice, and `broker_minimum_invalid` is unreachable. The column and
the enum still admit both, because the executor will produce them.

### Stored columns

`core_rebalance_intent_id`, `evaluated_at`, `core_mandate_event_id`,
`allocator_policy_version`, `recorded_by`; the observed state
(`core_instrument_id`, `core_market_value`, `cash_balance`, `currency`, `state_as_of`);
and all **eleven** `CoreRebalanceDecision` fields — `action`, `reason_code`, `amount`,
and the **eight** derived ones (`core_pct`, `target_pct`, `lower_pct`, `upper_pct`,
`effective_floor`, `floor_source`, `reserve_breached`, `reserve_margin_pct`).

⚠ `allocator_policy_version` is `CORE_MANDATE_POLICY_VERSION` — the version **this
evaluation ran under**, which is always ours and always known. The *mandate's* own
`policy_version` is on the event row and is deliberately not copied: on
`core_mandate_policy_unsupported` the two differ, and one column called `policy_version`
could not say which it held.

### Constraints **[by construction]**

Every equality below whose left side is nullable is written `IS NOT DISTINCT FROM`.
`docs/review-prevention-log.md` records this trap twice in eight days (#2679
`settlement_type = 'real'`, #2602 `sql/341` `currency = 'USD'`): a CHECK passes on NULL,
so `col = 'x'` does not require `col = 'x'`, and the constraint admits precisely the
omission it was written to catch.

1. `action IN ('hold', 'buy_core', 'sell_core', 'refused')`.
2. `reason_code` is a closed enum of the allocator's eleven codes:
   `core_mandate_absent`, `core_mandate_policy_unsupported`, `core_mandate_invalid`,
   `core_mandate_disabled`, `core_instrument_unset`, `sleeve_currency_mismatch`,
   `sleeve_instrument_mismatch`, `sleeve_valuation_invalid`, `broker_minimum_invalid`,
   `core_sleeve_empty`, `below_min_rebalance_amount`.
3. `refused` ⟺ `reason_code IS NOT NULL` **and** `amount = 0` **and** all eight derived
   fields are NULL. A refusal computed no weights, so a non-NULL derived field on a
   refusal is a writer bug and not a value judgement.
4. `hold` ⟹ `amount = 0` and all eight derived fields NOT NULL. ⚠ **A hold MAY carry a
   `reason_code`, and it is not an error**: `strategy_core_allocator.py:303` returns
   `_decide("hold", _ZERO, "below_min_rebalance_amount", core_pct)` when the gap to the
   band edge is under the floor. Constrained to that one code — no other code can reach
   a hold.
5. `buy_core` / `sell_core` ⟹ `amount > 0`, `reason_code IS NULL`, all eight derived
   fields NOT NULL.
6. `floor_source IN ('mandate', 'broker')`, NULL exactly when `action = 'refused'`
   (covered by constraint 3's derived-field clause; named because it is the one derived
   field that is not numeric).
7. Band ordering, on the non-refused arm: `lower_pct <= target_pct <= upper_pct` and
   `effective_floor > 0`. Both hold by construction in the allocator
   (`lower = target - band`, `upper = target + band`, `band > 0`,
   `min_rebalance_amount > 0`); the CHECK is what makes a writer that reorders the
   positional column list fail loudly. ⚠ That is not hypothetical — #2623 shipped a
   value into the wrong block through exactly that mistake, and an unrelated
   all-or-nothing CHECK is what caught it.
8. `core_mandate_event_id` is **nullable**, NULL exactly when
   `reason_code = 'core_mandate_absent'` — the only verdict reachable with no mandate
   row to point at. Every other refusal has a loaded `CoreMandate` and therefore an
   `event_id`. FK `ON DELETE RESTRICT`, matching `sql/346`: an evidence row may not be
   orphaned by deleting what it cites.
9. `state_as_of <= evaluated_at`. A valuation from the future is a caller bug, and the
   allocator holds no clock to catch it.
10. `core_market_value IS NULL OR cash_balance IS NULL` ⟹ `action = 'refused'`
    (the unstorable-input rule above).
11. `currency`, `recorded_by`, `allocator_policy_version`: NOT NULL, length-bounded,
    non-blank.

One constraint test per NULL-bearing combination, not just the happy path — per the
prevention-log generalisation that a test asserting only the happy path cannot see a
NULL leak.

## What this does NOT do

- No broker I/O, no order, no position, no trade row, no API endpoint, no UI.
- **"Authorises nothing" is mechanical here, not a promise.** No table has an FK to
  `strategy_core_rebalance_intents` and no module reads it, so no code path can turn one
  into an action. Both halves are asserted by a test rather than left to the docstring —
  the same shape as the existing check that no file under `scripts/` reaches a mutating
  broker method. The FK arrives in the next slice, together with the manager that makes
  the resulting position visible.
- **No in-flight suppression, and no expiry.** `evaluate_core_rebalance`'s docstring
  warns it is stateless and will re-recommend a trade already in flight; repeated calls
  against unchanged state produce several separately-shaped `buy_core` rows, and an
  intent recorded a month ago reads identically to one recorded now. Nothing here fixes
  that and nothing here can — suppression needs the trade linkage. **Owed by the next
  slice, explicitly:** one trade per intent (UNIQUE) so one intent cannot be executed
  twice; a "no open core trade" precondition so a second intent cannot be executed while
  the first is live; and a freshness bound on `evaluated_at` at submission, so a stale
  intent is not mistaken for current authority. Recorded as an obligation with an owner
  rather than as a silence.
- **No eligibility re-proof.** `sql/346`'s gate is write-time on `configure_core_mandate`
  and explicitly not an execution control; an enabled mandate stays enabled after its
  proof ages out. The executor re-proves at submission and must bind the intent to the
  exact proof, account and credential pair it used. An intent carries no proof and must
  not be read as carrying one.
- **No position-lifecycle rules for the core arm.** Whether a core holding ages out,
  ratchets, closes on mandate disable or instrument change, aggregates with an existing
  holding of the same instrument, or handles partial lots — all unanswered, all owned by
  the manager slice. The answer is expected to be "reconcile and close yes, stop/take and
  age-out no", because a stop on a benchmark holding converts a mandate into a strategy;
  that is a recommendation, not a decision, and it is not made here.

Refs #2603. Refs #2437. Refs #2525.
