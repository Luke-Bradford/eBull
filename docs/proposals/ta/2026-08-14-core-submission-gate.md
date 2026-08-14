# The core submission gate (#2603 item 3, execution half — step 3a)

⚠ **Step 3 is the executor. This is the half of it that REFUSES.** Step 3a builds the
admission decision — may this recorded rebalance verdict be turned into an order right
now? — and nothing else. Step 3b builds the observe → record → submit path that consumes
it. The refusing half ships first deliberately: the opposite order produces a writer whose
preconditions are a comment.

Prior steps: step 1 `8af38991` (`sql/348`, the intent record), step 2 `7513d77a`
(`sql/349`, the exclusive arc), entry condition `14d1bf8b` (`GET/PUT
/strategies/core-mandate`).

## The four inherited scope items, re-falsified against the tree

The step-3 handoff (#2603 comment, 2026-08-14T09:38Z) lists five. A prior session's
conclusion is evidence, not a finding, so each was re-run. **Two of the four in scope
changed, and both changed the design rather than the priority.**

| # | inherited statement | verdict | evidence |
| --- | --- | --- | --- |
| 1 | one trade per intent **at submission** | **stands** | `sql/349`'s `strategy_trades_core_rebalance_intent_id_key` is a UNIQUE index: it refuses the second INSERT. It does not stop two callers both reaching the broker before either inserts, which is the leg that costs money. |
| 2 | a **no-open-core-trade** precondition | ⚠ **WRONG AS WRITTEN** | an open core position is the mandate's steady state; the rule would forbid every rebalance after the first. §2 below. |
| 3 | an **`evaluated_at` freshness bound** | ⚠ **WRONG SHAPE** | the premise that some other producer records intents is false, so an age threshold measures nothing. §3 below. |
| 4 | eligibility re-proof bound to the exact proof/account/credential pair | **stands — wire, do not build** … ⚠ **plus one gap it does not cover** | `require_core_eligibility` (`strategy_core_eligibility.py:375-428`) already raises on all four failure kinds. It proves only `allow_open_position`. §4 below. |

Item 5 (`strategy_paper_executor.py:925-935`, the uncertain-submission resume path) needs
an entry *order* to be reachable and only step 3b creates one. Still deferred, still fails
closed today.

## §2 — "no open core trade" forbids the mandate's own steady state

`strategy_trades.status` is CHECK-pinned to `planned | submitted | open | closing | closed
| failed | reconcile_required` (measured on dev, `pg_constraint`). A core holding sits at
`open` for as long as the mandate holds it — that is the *point* of a core sleeve. A
precondition reading "refuse while an open core trade exists" would admit the first
rebalance and refuse every later one, permanently, and would look like a working control
the whole time.

**What the hazard actually is** is already written down, in
`strategy_core_rebalance_intent.py:16-18`: *"no in-flight suppression (the allocator is
stateless and will re-recommend a trade already in flight)"*. A submitted-but-unfilled buy
is not yet in the broker's position snapshot, so the next sleeve observation still shows
the pre-trade core value and the allocator re-recommends the same buy. That is a double
buy, and it is the only state that produces one.

### The predicate is sourced, not inferred from lifecycle names

A first draft of this document classified each `status` value as "reflected in the broker
snapshot" or not, and derived the blocking set from that table. **That is inference from
lifecycle names and it is wrong in both directions**: `failed` is written both after a
definitively rejected submission (`strategy_paper_executor.py:1272`) and on the resume
path (`:998`); `closed` and `open` are local lifecycle transitions that carry no claim
about when a snapshot catches up.

The system already stores the answer. `sql/285` gives
`strategy_order_reconciliation_state` a closed state vocabulary and its own settled
terminal set — the backlog index is defined `WHERE state NOT IN ('resolved','rejected')`,
and the `strategy_order_reconciliation_resolved_shape` CHECK ties exactly those two to a
non-NULL `reconciled_at`. **"Is this order's broker effect known" is that table's entire
purpose.** So:

> A core trade blocks admission while any order linked to it has a reconciliation state
> outside `('resolved','rejected')`.

`unresolved`, `pending`, `not_found`, `ambiguous` and `error` all block — unknown fails
closed, and `not_found`/`ambiguous` are precisely the states in which a second order might
be a duplicate or might be the only one.

⚠ **Belt, for a state the reconciliation table cannot see.** A core trade with **no linked
order at all** in a non-terminal status also blocks. In the executor, trade creation,
order INSERT and `link_strategy_order` are one transaction, so this is unreachable through
that path — but this repo's tests and fixtures insert rows directly, and `sql/349`'s own
rule applies: the invariant changes at the migration, not at the writer.

⚠ **This can deny service, deliberately.** A core trade whose reconciliation never reaches
a terminal state blocks every later core submission. That is the correct direction for a
duplicate-order control, and it is not silent: `enforce_reconciliation_slo` already exists
and already raises an operator-visible execution block on exactly this backlog.

## §3 — the freshness bound is structural, because there is no second producer

**Source rule.** No published formulation governs how long a portfolio-rebalance verdict
stays actionable, and this document does not invent a citation. Per `.claude/CLAUDE.md`
("source-rule before design"), the rule is fixed **by construction** and frozen in
`CORE_SUBMISSION_POLICY_VERSION`.

The construction rests on one measured fact:

```bash
grep -rn "record_core_rebalance_intent" app scripts   # → the definition and one docstring. No caller.
```

`record_core_rebalance_intent` has **no caller in `app/` or `scripts/`**. So an intent the
executor submits against is one the executor itself recorded moments earlier, from a sleeve
it itself observed. A wall-clock bound on `evaluated_at` would be measuring the distance
between two lines of one function — a threshold with no phenomenon under it, which is
exactly the invented constant the source-rule rule exists to stop.

What the bound is *for* is the case an age threshold reaches at: **a verdict that has been
overtaken.** That is expressible exactly, with no parameter:

- **`core_intent_superseded`** — refuse unless this intent is the newest row in
  `strategy_core_rebalance_intents`. Any later evaluation supersedes an earlier one,
  including a `hold` or a `refused`: the allocator is a pure function of mandate and
  observed state, so a later row is a later observation of the same sleeve, and acting on
  the earlier verdict would mean acting on a world we have since re-measured.
- **`core_mandate_revision_stale`** — refuse unless the intent's `core_mandate_event_id` is
  the newest revision. `strategy_core_mandate_events` is append-only and revision-ordered;
  a verdict computed under a superseded mandate is one the operator has replaced.

⚠ **Scope of "newest" is the whole table, and that is correct only because the mandate is a
singleton.** `load_core_mandate` takes no account, operator or sleeve argument — it is
`ORDER BY revision DESC LIMIT 1` over one table (`strategy_core_mandate.py:271-299`), and
`strategy_core_rebalance_intents` carries no operator/provider/environment column. One
mandate, one sleeve, one intent series. **If a second mandate is ever introduced, this
predicate silently starts refusing unrelated intents** — recorded here because the failure
would be a refusal, and refusals are the failures nobody notices.

⚠ **What supersession does NOT do, stated rather than left silent.** It bounds *relative*
staleness, not absolute. An intent that is newest, under the newest mandate, and two hours
old is admitted, because no server-side fact distinguishes it from one recorded two seconds
ago by the caller now asking. **The absolute bound is 3b's obligation**: observe → record →
admit → submit inside a single lock hold, so that `evaluated_at` is by construction within
one invocation. The gate cannot verify that and does not claim to.

⚠ Both predicates are evaluated **server-side against the table**, never against a
caller-supplied timestamp. A gate that takes the caller's word for "when now is" fails open
on a caller bug.

⚠ Ordering is by `core_rebalance_intent_id`, the BIGSERIAL primary key. Sequence order is
not commit order in general; it is here, because 3b records and admits inside the same
advisory lock (§5).

## §4 — a rebalance sell is a PARTIAL CLOSE, and the existing proof does not cover one

`require_core_eligibility` returns a proof whose `verdict='underlying'` requires
`allow_open_position` (`strategy_core_eligibility.py:202`, `sql/346:110`). It says nothing
about closing. `sql/346` already **stores** `allow_close_position` and
`allow_partial_close_position` (`:72-74`); `CoreEligibilityProof` simply does not read them
back.

A `sell_core` therefore reaches the broker on a proof that the instrument can be *bought*.
That is a different capability, documented separately by the provider, and assuming one
from the other is the inference this repo keeps paying for.

**Which of the two a rebalance sell needs is settled by construction, not chosen.**
`validate_core_mandate` requires `core_target_pct - rebalance_band_pct > 0`
(`strategy_core_mandate.py:246`), and the allocator sells only down to the lower band edge
(`strategy_core_allocator.py:313-325`). So the post-trade core value is strictly positive:
**a rebalance sell can never be a full close.** The capability required is
`allow_partial_close_position`.

Gate rule: on `action='sell_core'`, refuse `core_partial_close_unproved` unless the proof
carries `allow_partial_close_position IS TRUE`. `IS TRUE`, not truthiness — the column is
nullable and NULL means the response did not say.

Reading the two columns back onto `CoreEligibilityProof` is the whole change; nothing about
the proof's capture, digest or freshness moves.

## §5 — the gate

`app/services/strategy_core_submission_gate.py`

```python
def admit_core_rebalance_intent(
    conn, *, intent_id: int, operator_id: UUID, provider: str, environment: str
) -> CoreSubmissionAdmission
```

Returns a verdict for every input; **never raises to signal a refusal**. Same posture as
`evaluate_core_rebalance`, for the same reason: a caller that must catch in order to learn
the mandate is disabled will eventually catch too broadly. A raise is reserved for a caller
*contract* breach — the lock, below.

`CoreSubmissionAdmission` carries `admitted`, `reason_code`, `detail`, the intent's
`action`, `amount`, `core_instrument_id`, the mandate `event_id`, and the eligibility
`proof_id` when one was obtained. **`detail` is not decoration**: three refusals collapse
materially different causes and the caller cannot re-derive them (§the vocabulary, below).

### One snapshot, one statement

Every table read the gate makes — the intent, its mandate, the newest intent, the newest
mandate revision, an existing trade on this intent, and the in-flight population — is **one
SQL statement**. Under `READ COMMITTED` a sequence of statements sees a sequence of
snapshots, so "newest intent" and "no trade yet" could each be true of a different instant
and false together. Eligibility is the one read that follows, because it takes `FOR SHARE`
on the credential rows and answers a different question.

### The closed refusal vocabulary

Declared as a `Literal`, so pyright checks every `return` site against it and a code cannot
be introduced without appearing in the vocabulary (the allocator's own device).

**Precedence is fixed and is the order below.** Without one, an input that is simultaneously
superseded, already submitted and ineligible would record whichever cause the implementation
happened to test first, and the recorded explanation would move with a refactor. Cheapest
and most-specific first; the two that need a second query are last.

| # | reason code | refuses when | owed by |
| --- | --- | --- | --- |
| 1 | `core_intent_missing` | no such intent row | — |
| 2 | `core_intent_not_actionable` | `action NOT IN ('buy_core','sell_core')` | step 2's `core_arm_authorised` |
| 3 | `core_intent_superseded` | a newer intent row exists (`detail` names it) | §3 |
| 4 | `core_mandate_revision_stale` | the intent's mandate is not the newest revision | §3 |
| 5 | `core_mandate_not_paper` | the mandate's `mode <> 'paper'` | step 2 |
| 6 | `core_mandate_disabled` | the current mandate revision is not `enabled` | ⚠ new, below |
| 7 | `core_intent_already_submitted` | a `strategy_trades` row cites this intent (`detail` carries its status — an uncertain prior submission needs reconciliation, not a retry) | item 1 |
| 8 | `core_trade_in_flight` | a core trade has an unreconciled order, or none at all (`detail` names the trade and state) | item 2 / §2 |
| 9 | `core_eligibility_unproved` | `require_core_eligibility` raised (`detail` carries its message, which distinguishes all four causes) | item 4 |
| 10 | `core_partial_close_unproved` | `sell_core` with `allow_partial_close_position` not TRUE | §4 |

⚠ **`core_mandate_disabled` is not in the inherited list and is not scope creep.** Once
`core_mandate_revision_stale` requires the intent's revision to be the *current* one, the
gate already holds that row; a mandate the operator disabled between evaluation and
submission would otherwise pass every check here, because the allocator's own
`core_mandate_disabled` refusal belongs to a revision this intent predates. Reading the row
and not testing its flag would be the same defect this ticket keeps finding.

⚠⚠ **This vocabulary is NOT the complete submission refusal vocabulary, and must not be
cited as one.** The kill switch, `enable_auto_trading`, the execution block, market-session
state, quote availability and staleness, account-risk availability, broker minimums, cost
assessment and broker rejection are all real refusals of a core submission and **none of
them is here** — they belong to 3b, which is the code that holds a quote and a broker. A
reader who takes this table for the full set will conclude the core arm has no kill-switch
check. It has none *yet*.

### The lock, and why the gate verifies it rather than documenting it

Item 1's gap is a race: the UNIQUE index refuses the second INSERT, but by then both callers
have placed an order. Serialising is the only fix, so admission is meaningful only while a
session-level advisory lock is held across observe → record → admit → submit → insert.

`core_submission_lock(conn)` takes **two** locks, in this order:

1. `CORE_MANDATE_ADVISORY_LOCK = (2603, 1)` — the same key `configure_core_mandate` takes
   as an *xact* lock. Without it the gate's `core_mandate_revision_stale` check is a
   TOCTOU: a revision appended between the check and the INSERT leaves a trade citing a
   mandate the operator has replaced. Session and transaction advisory locks share one lock
   manager, so holding it blocks mandate writes for the duration of a submission — which is
   the intended behaviour, not a side effect.
2. `CORE_SUBMISSION_ADVISORY_LOCK = (2603, 3)` — submissions against each other.

One acquisition order, and `configure_core_mandate` takes only the first, so there is no
deadlock cycle. Shape and unlock-ownership assertion follow `_allocator_lock`
(`strategy_paper_executor.py:166-179`).

⚠⚠ **The gate asserts the lock is held, and this is the load-bearing part.** A precondition
that lives in a docstring is exactly #2437's standing pattern — *the control exists on a
path the decision does not take*. One catalogue read converts it into a control:

```sql
SELECT 1 FROM pg_locks
 WHERE locktype='advisory' AND classid=%s AND objid=%s AND objsubid=2
   AND pid=pg_backend_pid() AND granted
```

⚠ `objsubid=2` is required, not tidiness: Postgres encodes a two-`int4` advisory key as
`classid`/`objid` with `objsubid=2`, and a one-`int8` key as the high/low halves of the
bigint with `objsubid=1`. Without it, an unrelated `pg_advisory_lock(bigint)` whose halves
happen to match satisfies the assertion.

⚠ `classid`/`objid` are OID-width (unsigned) while `pg_advisory_lock(int,int)` takes signed
`int4`, so a negative key component would not compare equal without conversion. Both keys
here are positive literals; a future negative key breaks this silently.

**What the assertion proves, exactly:** that this backend currently holds a matching
advisory lock. It does not prove *this* call acquired it, that the context manager owns it,
or that no reentrant acquisition is outstanding (`pg_locks` does not expose the reference
count). That is sufficient for the property the gate needs — the critical section is open —
and insufficient as a claim of exclusive ownership, so it is not made.

Not held → `StrategyCoreSubmissionError`. That is the one raise, and it is right that it
raises: an unserialised caller is a **caller bug**, not a state of the world, and returning
it as a refusal would let the caller log it and carry on.

⚠ **One TOCTOU the lock does not close, so it is named as 3b's obligation.**
`require_core_eligibility` takes `FOR SHARE` on the live credential rows, and that lock ends
at the next commit. If 3b commits between admission and submission, a credential swap can
land in the gap. 3b must keep the eligibility read and the order submission inside one
transaction, or re-admit after committing.

## What this gate is not

- It authorises nothing on its own: it has no caller until step 3b, and it writes nothing.
  Stated plainly, as steps 1 and 2 stated it, rather than left to be inferred.
- It is not a demo-mode gate. `mode='paper'` records the authority's declaration only. The
  backstop stays the demo-only credential configuration and
  `app/security/unattended_guard.py` (`sql/349`'s own warning, repeated because a gate
  module is where it would most easily be misread).
- It does not size anything. The intent's stored `amount` was computed against a sleeve
  observed at `state_as_of`; re-solving it against the cost actually quoted is 3b's
  (`strategy_core_allocator.py:334-339`).

## Acceptance

The gate performs no broker mutation and makes no broker call at all, so it is fully
testable unattended.

Refusals 1-8 are table-testable against seeded rows. ⚠ 9 and 10 are **not**: they need a
`strategy_core_eligibility_proofs` row and a live `broker_credentials` pair, and the age
comparison is against database time — so they are DB-backed tests, not pure-logic ones.

⚠ #2603's own acceptance — initial allocation, forced drift rebalance, kill-switch drill —
is operator-attended and is not this step's.

Refs #2603. Refs #2437.
