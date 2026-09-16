# The read page's inability to observe headroom must not disable the endpoint that can (#3123)

Status: proposal, revised after Codex checkpoint 1 (20 findings; two changed the scope and
several corrected claims I had got wrong). Scope: `read_core_sleeve`'s `capital_ready` term,
plus one frontend message that this change makes reachable.

## ⚠⚠ Read this first — what the change actually enables

Enabling the Rebalance affordance while the sleeve holds a position makes a **UI-triggered
top-up BUY** newly reachable: `evaluate_core_rebalance` can return `buy_core` for an
underweight position, and `execute_core_rebalance` will submit it through
`place_demo_core_order`.

It does **not** restore a sell path. `assess_core_broker_preflight` returns
`core_close_side_cost_quote_unavailable` for `sell_core` before any broker call
(`strategy_core_broker_preflight.py:317`), `admit_core_rebalance_intent` can refuse it as
`core_partial_close_unproved` first, and `core_order_shape_for` has no `sell_core` entry at
all. So the ticket's motivating sentence — "the sell leg has no attended entry point" — is
true and stays true; the sell leg is #2603 item 3 and is not built.

Demo only (`execute_core_rebalance` refuses any non-demo environment), operator-initiated,
and bounded by the sandbox. Stated up front because the ticket's framing does not make it
obvious, and a reviewer should agree to it deliberately.

## The defect

`read_core_sleeve` sets `capital_ready = False` whenever
`capital_authority.core_active_position_ids` is non-empty, because with an active core
position the read-only page cannot compute headroom from DB-recorded figures alone — the
position's committed amount is a broker fact. That refusal to compute is correct.

But `capital_ready` feeds only two things: the `core_sandbox_exceeded` blocker inside its own
`else` branch, and

```python
base_ready = core_pool_ready and capital_ready
can_rebalance = base_ready and resume_authority is None
```

`StrategyPortfolioLens.tsx:322` disables the Rebalance button on `!can_rebalance` (together
with `busy`, `mandateDirty`, and `can_resume`). So while the core sleeve holds a position,
the operator cannot ask for a rebalance evaluation from the UI at all — the allocator's only
attended affordance is withdrawn in exactly the state it exists to manage.

⚠ Precisely: a direct `POST /strategies/core-sleeve/rebalance` still reaches the endpoint
today (it consults neither flag), and `execution_action="resume"` is still reachable when a
pending order exists. What is lost is the UI path for an ordinary evaluation.

Pinned today by
`tests/test_2603_core_mandate_api.py::test_operator_view_does_not_advertise_unavailable_core_headroom`,
which parametrises the active-position case and the sandbox-exhausted case through ONE
assertion body, so the affordance claim and the advertising claim cannot be read apart.

⚠ Latency: measured on the dev DB on 2026-09-16 at `730a28b3` —
`strategy_position_ownership` active = 0, `strategy_trades` = 0,
`strategy_paper_pool_events` = 0 — so `load_engine_capital_authority` returns `None` and the
branch is unreachable today. It becomes live when the first **resolved core trade with an
active ownership row** exists, which is what populates `core_active_position_ids`.

## Source rule — the in-repo precedents that decide it

Not reasoned from first principles.

1. `_core_pool_activation_ready`'s docstring (`app/api/strategies.py:1093-1099`) states the
   governing principle for exactly this class of flag:

   > "This deliberately excludes the pot itself: **requiring an enabled pot here would make
   > the only endpoint that enables it unreachable.** It also grants no order authority. The
   > attended executor separately re-proves current broker eligibility, capital headroom,
   > kill-switch state and every submission guard."

   Folding "the page cannot observe headroom" into `can_rebalance` is that same failure mode
   one function along. ⚠ Codex's fair objection: that endpoint CLEARS its own prerequisite,
   whereas rebalancing does not cure the page's observational limit. The transferable half is
   narrower and is the half being used — *a readiness flag on a read endpoint grants no order
   authority, so it must not be the thing that withdraws an affordance.*

2. **Settled decision, 2026-08-22, "The allocation boundary is the ONLY safety net"**
   (`docs/settled-decisions.md:1204`): exposure is bounded *"enforced at the execution guard
   with a named refusal (`sandbox_exceeded`)"*. The safety net is named as the guard. A UI
   flag that any direct POST bypasses is not one, and treating it as one adds a second,
   weaker surface the decision explicitly did not want.

3. The same file already draws this distinction by hand: the `core_verdict_cash` branch
   carries the source comment *"⚠ This blocks new core ENTRY only. Recovery of a pending
   order does not key on selection readiness, so `execution_action='resume'` stays
   reachable."*

`grep` run before writing: `docs/settled-decisions.md` has no entry for `can_rebalance`,
`core_live_snapshot_required` or the advertising question. ⚠ Absence proves no policy either
way — decision 2 above is the one that bears.

## The change

1. `capital_ready` → `capital_permits_rebalance`: "may the operator ask the endpoint to
   evaluate", not "has the page proved headroom". Only the active-position branch moves.

   | branch (in elif order) | `capital_permits_rebalance` | blocker |
   | --- | --- | --- |
   | `load_engine_capital_authority` raises | `False` | `core_capital_authority_incomplete` |
   | authority is `None` | `False` | `core_paper_pool_unconfigured` |
   | pool disabled | `False` | `core_paper_pool_disabled` |
   | **active core positions** | **`True`** (was `False`) | `core_live_snapshot_required`, unchanged |
   | recorded headroom within bound AND remaining > 0 | `True` | none |
   | recorded headroom exhausted or overbound | `False` | `core_sandbox_exceeded` |

   ⚠ **Precedence consequence, and it is not cosmetic.** The active-position branch is an
   `elif`, so it SKIPS the recorded-headroom calculation entirely. A state where
   `alpha_committed + core_pending_committed` already exhausts the bound will now report
   `can_rebalance=True` where it previously reported `False`, and will emit no
   `core_sandbox_exceeded` blocker. That is not a weakening — the recorded figure omits the
   active position's committed amount, so it was never the authority here, and
   `execute_core_rebalance` re-computes `within_bound` from the exact snapshot and refuses
   `sandbox_exceeded` — but it IS an observable change and is covered by its own test.

2. **Frontend, newly reachable:** `StrategyPortfolioLens.tsx` renders every `held` result as
   *"No trade required; the sleeve remains inside its band."* The allocator also returns
   `held` with `reason_code="below_min_rebalance_amount"` when the sleeve is **outside** the
   band but the gap is under `min_rebalance_amount` (`strategy_core_allocator.py:325-330`,
   whose comment says *"The floor wins and the breach, if any, is reported"*). That message
   is false in that case, and this change is what makes it reachable, so it is fixed here
   rather than filed.

3. The blocker's detail text is **unchanged**. A first draft appended "Rebalance to evaluate
   against a live snapshot"; Codex pointed out the blocker is emitted independently of
   mandate, selection, environment and resume state, so that instruction can be wrong in
   several combinations. The blocker states a fact about the page; it should not also issue
   an instruction it cannot qualify.

`can_enable_pool` is `core_pool_ready` and is untouched. `capital_ready` is not a response
field, so the response SCHEMA is unchanged — but `can_rebalance` and `execution_action`
change observable MEANING for this state, which a schema-compatibility claim does not cover.
A repo-wide search found no production consumer other than the button; `execution_action` is
emitted, typed and tested but not branched on in production frontend code. External consumers
are unverified.

## Why this is not a weakening — accurately this time

Codex falsified three claims in the first draft. Corrected:

- ⚠ **The sandbox is charged on `row.amount`, not `market_value`.**
  `resolve_engine_capital_usage` accumulates `core_committed` from `_money(row.amount, …)`;
  `core_market_value` is a separate term that feeds the ALLOCATOR's sleeve valuation. "The
  bound is enforced on the exact market value" was wrong.
- ⚠ **`within_bound` alone does not bound the proposed order.** It bounds existing
  commitment. What bounds the new order is the composition: `observe_core_sleeve` receives
  `assigned_cash_available=min(snapshot.available_cash, usage.headroom.remaining)` so the
  allocator can never size against more cash than the pot allows; the broker preflight
  re-sizes against the actually-quoted cost; the authority and mandate are re-proved under
  `core_submission_lock` (which also holds `PAPER_ALLOCATOR_ADVISORY_LOCK`, so core and alpha
  cannot spend the same headroom); and `admit_core_rebalance_intent` + `preflight_core_submission`
  run before any durable order authority exists.
- ⚠ **"Every guard is re-proved" is false for sells**, and the reason is the opposite of
  reassuring in a useful way: a sell is refused BEFORE its snapshot, freshness, ownership,
  minimum and cost checks run, so those guards are untested on that path. Safety on the sell
  side today comes from the refusal plus `core_order_shape_for`'s buy-only backstop, not from
  a proof.
- ⚠ Two account snapshots are taken on a buy (the executor's, then the broker preflight's),
  and eligibility is a STORED proof re-bound to the live credentials rather than a fresh
  fetch. The first snapshot's wall-clock freshness is not checked before the intent and
  drawdown writes.

Side effects a reviewer should know are newly reachable per click: a
`strategy_core_rebalance_intents` row and a `strategy_paper_account_risk_state` high-water
update are persisted even for holds and refusals, and the broker reads happen while the
shared allocator and mandate advisory locks are held, so repeated clicks consume quota and
can delay other execution or mandate configuration.

## Out of scope

- **The disabled-pot branch has the same shape** — `core_paper_pool_disabled` says "existing
  holdings remain owned but no entry is allowed" yet also withdraws the affordance. So do
  drawdown refusals, a disabled or mismatched mandate, an unready selection, and an
  overbound sleeve. What a de-risking rebalance should be permitted to do through each of
  those is one question, it belongs with the sell leg (#2603 item 3), and it needs its own
  evidence. Recorded, not bundled.
- No change to `execute_core_rebalance`, to any guard, or to the response schema.

## Acceptance

1. A table test over **all six** branches of the chain asserting
   `(can_rebalance, execution_action, blocker codes)` together, so a later change cannot move
   one silently. Includes the two recorded-headroom cases separately (exactly at the bound,
   and overbound).
2. Active core positions + healthy selection/mandate/pool → `can_rebalance is True`,
   `execution_action == "rebalance"`, and the `core_live_snapshot_required` blocker still
   emitted.
3. Active core positions + recorded commitment already exhausting the bound →
   `can_rebalance is True` and **no** `core_sandbox_exceeded` blocker, with the test's own
   docstring carrying why that is correct and where the real bound is enforced.
4. Active core positions + a resume authority → `execution_action == "resume"` and
   `can_rebalance is False`; and active positions + a disabled pool → `False`.
5. Frontend: a `held` result with `reason_code="below_min_rebalance_amount"` does NOT render
   "the sleeve remains inside its band".
