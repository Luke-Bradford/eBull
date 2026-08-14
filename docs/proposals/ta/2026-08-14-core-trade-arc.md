# The core trade arc (#2603 item 3, execution half — step 2): the plan, corrected

⚠ **NOT IMPLEMENTED.** This is a plan document. Step 1 (`8af38991`, `sql/348`) shipped the
rebalance intent record; this describes what step 2 has to be, after its first draft was
falsified at Codex checkpoint 1. Nothing here is built.

## The scope of this item has now been mis-stated twice, in the same direction

**First mis-statement** (#2437's R4 comment, corrected by step 1): item 3's execution half
was described as gated by *two CHECK-shaped exemptions* (`sql/287:116`,
`strategy_position_manager.py:816-817`). It is not — the ownership chain is NOT NULL at
every link, so a core holding cannot be written at all, and exempting the CHECKs would
leave it dropped by an INNER JOIN rather than exempted.

**Second mis-statement** (this document's own first draft, 2026-08-14): the corrected
version said the fix was the arc plus *relaxing one join* in
`strategy_position_manager._load_owned`. Also wrong, and wrong the same way — it fixes the
function that manages a position while leaving the query that **selects positions to
manage** untouched.

```sql
-- strategy_paper_runtime.py:398-400 — the paper cycle's owned-position batch
FROM strategy_position_ownership own
JOIN strategy_trades t              ON t.strategy_trade_id=own.strategy_trade_id
JOIN strategy_funding_decisions fd  ON fd.funding_decision_id=t.funding_decision_id
```

`manage_owned_position` is never called for a trade this batch does not return. So the
draft would have shipped a manager path capable of handling a core position that nothing
ever invokes for one — **#2437's R4 pattern exactly, one level up from where the previous
correction found it.**

The lesson generalises past this ticket: *when a chain is being widened, the widening has
to reach the query that SELECTS the work, not only the one that DOES it.* Fixing the
worker and not the dispatcher looks complete at every level a diff review inspects.

## Consumer inventory — by query, not by module

Measured 2026-08-14 by grepping every `FROM`/`JOIN`/`UPDATE`/`INSERT` against
`strategy_trades` and `strategy_position_ownership` under `app/`. Reproduce with:

```bash
grep -rn "FROM strategy_trades\|JOIN strategy_trades\|FROM strategy_position_ownership\|JOIN strategy_position_ownership\|UPDATE strategy_trades\|INSERT INTO strategy_trades" app/ --include="*.py"
```

The classification is what matters, and it is not the module list the prior notes used.

### A. Reaches a trade via ownership, then INNER JOINs funding — **DROPS a core trade**

These are in scope for step 2. Each silently omits a core position, and the omission is
invisible because a missing row looks like no work rather than like an error.

| query | consequence of omitting core |
| --- | --- |
| `strategy_paper_runtime.py:398-400` — the cycle's owned batch | **nothing ever manages a core position.** The decisive one. |
| `strategy_paper_runtime.py:240-243` — quote-freshness health | the owned-position health population is incomplete, so the execution block cannot trip on core |
| `strategy_position_manager.py:279-283` — `_load_owned` | a core position cannot be verified, reconciled or closed |
| `api/strategies.py:1861-1866` — owned-position list | a core position is invisible to the operator, so the identifiers needed to close it are unobtainable |
| `api/strategies.py:1998`, `2054` — P&L history | its docstring says "every exact-owned lifecycle"; core closes are silently excluded |
| `api/strategies.py:2462` | as above |
| `strategy_paper_executor.py:770-772` | as above |

⚠ `api/strategies.py:1861` also joins `strategy_signals` to project `strategy_id` /
`strategy_version`. A core position has neither, so the response model needs them nullable
and the frontend types follow. **That is the reason step 2 is not a backend-only change**,
and it was invisible in the draft.

### B. Starts from a funding decision — signal-arm-only **by design**, correctly unchanged

`strategy_monitoring.py:202, 445, 655`; `strategy_live_gate.py:411, 464, 491`;
`strategy_paper_executor.py:189, 256, 266, 733`; `strategy_control_plane.py:995, 1037`;
`api/strategies.py:1806`. These answer questions *about signals* ("did this funding
decision become a trade"). A core trade has no funding decision and belongs in none of
them.

### C. Keys on `strategy_trade_id` / `broker_position_id` — **arm-agnostic already**

`strategy_order_reconciliation.py:141, 260, 365`; `strategy_wealth.py:48, 60, 77`;
`strategy_paper_executor.py`'s status `UPDATE`s. These work on a core trade unchanged,
which is the strongest argument for the exclusive arc over a sibling table: an arc costs
these nothing, a sibling table would need every one of them dual-written.

## The arc

`strategy_trades` gains `core_rebalance_intent_id`, `funding_decision_id` becomes
nullable, exactly one non-null:

```sql
CONSTRAINT strategy_trades_exactly_one_authorisation
    CHECK (num_nonnulls(funding_decision_id, core_rebalance_intent_id) = 1)
```

plus `UNIQUE (core_rebalance_intent_id)` so each arm keeps one-trade-per-authorisation.

**`DROP NOT NULL` audit**, per `docs/review-prevention-log.md` — every CHECK, index
predicate and generated column mentioning the column silently changes meaning from
"enforced" to "enforced except on NULL". Catalogue query, so the result is reproducible
rather than asserted:

```sql
SELECT conrelid::regclass, conname, pg_get_constraintdef(oid) FROM pg_constraint
 WHERE pg_get_constraintdef(oid) LIKE '%funding_decision_id%';
SELECT tablename, indexname FROM pg_indexes WHERE indexdef LIKE '%funding_decision_id%';
SELECT table_name, column_name FROM information_schema.columns
 WHERE generation_expression LIKE '%funding_decision%';
```

Result on dev: three constraints (`strategy_funding_decisions_pkey`,
`strategy_trades_funding_decision_id_key` UNIQUE, `strategy_trades_funding_decision_id_fkey`),
two indexes (both the above), **zero** CHECKs, zero partial-index predicates, zero
generated columns. The UNIQUE weakens from "every trade has a funding decision, at most one
each" to "at most one each", which is intended. ⚠ The catalogue is only the database half —
the **application** half is the same audit and is larger: casts like
`int(row["max_quote_age_seconds"])` and `Decimal(str(row["entry_stop"]))` assume non-null,
and the class-A queries above assume a funding-backed trade.

## Three correctness points the draft got wrong

### 1. The arm discriminator must witness the deployment, not the preflight

The draft proposed:

```sql
AND ( (t.funding_decision_id IS NOT NULL AND pre.signal_id IS NOT NULL) OR ... )
```

That does **not** preserve the paper gate. `pre` joins on `funding.signal_id` and is
independent of `d`, so with LEFT JOINs a **live** deployment yields `d = NULL` while `pre`
resolves — and the row loads. The current INNER chain also requires
`strategy_execution_policies` to resolve; neither `pre` nor `d` proves that, so a signal
trade could load with `max_quote_age_seconds = NULL` and crash at construction or at the
quote check. Every link needs its own witness:

```sql
AND ( (t.funding_decision_id IS NOT NULL
       AND d.deployment_id IS NOT NULL          -- carries mode='paper'
       AND execution.deployment_id IS NOT NULL  -- carries the quote-age policy
       AND pre.signal_id IS NOT NULL)           -- carries verdict='allocated'
   OR (t.core_rebalance_intent_id IS NOT NULL
       AND intent.core_rebalance_intent_id IS NOT NULL
       AND mandate.core_mandate_event_id IS NOT NULL) )
```

**A join predicate that also filters is a safety control, and converting it to a LEFT JOIN
moves that control into the WHERE clause or deletes it.** There is no third outcome.

### 2. A `hold` or `refused` intent must not be able to authorise a trade

`sql/348` stores every verdict, including holds and refusals — deliberately, because a
verdict is evidence. The FK alone would therefore let a `refused` intent back a trade.
Step 2 must require an actionable verdict structurally, not by convention: the load
predicate requires `intent.action IN ('buy_core','sell_core')`, and the executor's
INSERT is gated the same way.

### 3. The trade's instrument must equal the intent's

Nothing in the arc constrains `strategy_trades.instrument_id` against
`intent.core_instrument_id`. Without it, a malformed core trade authorises the manager to
close a **different instrument's** position. This is a CHECK-able invariant only via a
trigger or a load-time predicate, since the two live in different tables; the load
predicate is the cheaper and is where the manager acts.

## The core arm's paper gate, and what it is honestly worth

The signal arm's demo property is `strategy_deployments.mode = 'paper'`. A mandate has no
deployment, so the core arm would ship with no equivalent. `strategy_core_mandate_events`
gains `mode TEXT NOT NULL CHECK (mode = 'paper')` — **required on insert, not defaulted**,
so a writer that forgets it fails rather than inherits safety. Free to add: 0 rows.

⚠ **State plainly what it is not.** An event-level constant records the authority's
declared mode. It does not record which account, environment or broker credentials a trade
actually used. The real backstop remains the demo-only credential configuration and
`app/security/unattended_guard.py`. Calling the mandate column "the equivalent gate" would
overstate it.

## What the manager applies to a core position

Measured — every policy field in `_OwnedPosition` and where it is read:

| field | read at | core |
| --- | --- | --- |
| `max_position_age_seconds` | 783-787 (timeout) | not applied |
| `entry_stop`, `entry_take_profit` | 814-816 (fixed-exit repair) | not applied |
| `max_quote_age_seconds` | 832, 891 | unreachable |
| `ratchet_variant_id`, ATR multiples | 554, 871 | not applied |
| `deployment_id` | **nowhere** | already a dead field in this module |

**Applied: reconciliation and explicit close.** Not applied: stop-forcing,
take-profit-forcing, ratcheting, age-out — because each converts a mandate into a strategy.
A stop on a benchmark holding sells the benchmark on a drawdown, and "return to core/cash"
is the stop-condition outcome the viability plan falls back *to*; a stop underneath it
would give the fallback a fallback. Age-out closes a position for being old, and a core
holding has no horizon by construction.

⚠ This is an exemption, and the earlier research warned that exemptions produce an
unmanaged position that looks managed. What makes it safe is that the position is still
**selected, loaded, reconciled and closable** — exempt from three behaviours, not absent
from the system. That distinction is only true if class A above is fixed in full.

Three placement details, each of which silently defeats the exemption if got wrong:

- The core return must precede **line 813**, not merely guard the `if stop_gap` body —
  lines 814-816 already dereference the now-nullable stop.
- Age-out needs an explicit `is_core` gate, **not** reliance on `max_position_age_seconds`
  being NULL. Null-by-absence conflates "core is exempt" with "this deployment has no age
  policy", so a later default would silently start ageing out core holdings.
- `_resume_operation` runs immediately after load, **before** any gate. A pending edit
  operation attached to a core ownership would resume a stop/take mutation despite the
  exemption. It must reject non-close operations on the core arm.
- The early return must carry a core-specific reason code. Reusing `position_protected`
  would assert a stop exists.

## Sequencing

Class A is not deferrable to the executor: the migration permits a core trade row from the
moment it applies, and this project's own tests and fixtures create rows directly. "Correct
today because no core trade exists" stops being true at the migration, not at the writer.

So step 2 is: `sql/349` + `strategy_position_manager` + `strategy_paper_runtime` (both
queries) + `api/strategies.py` (four queries) + the nullable `strategy_id`/`strategy_version`
in the owned-position response and its frontend types. That is the coherent unit. It is
substantially larger than "relax one join", which is the finding this document exists to
record.

Step 3 (the executor) still owes: one trade per intent, a no-open-core-trade precondition,
an `evaluated_at` freshness bound, and the eligibility re-proof bound to the exact proof,
account and credential pair used.

Refs #2603. Refs #2437. Refs #2525.
