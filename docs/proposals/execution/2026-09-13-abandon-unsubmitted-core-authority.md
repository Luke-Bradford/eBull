# An operator-attested terminal state for a core authority the broker never received (#2961)

Status: proposal · 2026-09-13 · Refs #2961, #2949, #2962, #2603, #2602, #2844

## The state being fixed

`execute_core_rebalance` commits a durable authority
(`app/services/strategy_core_executor.py:460-588`) and *then* calls
`place_demo_core_order`. A crash in that window leaves:

- `orders` row with `strategy_request_id` set and `broker_order_ref` **NULL**;
- `strategy_order_reconciliation_state.state = 'unresolved'`;
- `strategy_trades.status = 'submitted'`;
- broker mutation count **0**.

Measured on `79720491` by
`tests/test_2949_core_restart_recovery_db.py::test_scenario_2_stranded_authority_is_unreachable_by_any_unattended_caller`
with a real `SIGKILL`ed child and a file-backed fake broker.

Nothing can terminalise it:

- `reconcile_backlog` cannot see it — `AND trade.core_rebalance_intent_id IS NULL`
  (`strategy_order_reconciliation.py:532`), so the core arm has no unattended
  reconciler at all (#2962);
- the attended resume is correct and useless: a lookup miss is rightly not treated
  as permission to resubmit (`strategy_core_executor.py:263-268`), so the row sits
  at `not_found`. `_apply_detail` is reached only from a *successful* lookup, which
  will never happen for an order that does not exist;
- `core_trade_in_flight` (`strategy_core_submission_gate.py:440`) then refuses every
  future core authority — **silently**, because `refresh_strategy_health` clears the
  reconciliation block whenever no enabled paper deployment supplies a policy
  (`strategy_paper_runtime.py:235-250`), which is today's core-only configuration.

## What this is NOT

**Not resubmission.** The refusal to retry on a lookup miss stays exactly as it is.
This adds a terminal state, never a second broker write. The new authority an
operator may create afterwards is a *new* order with a *new* `strategy_request_id`,
not a retry of the abandoned one.

## Why the automatic discriminator cannot be built yet

#2961 requires the discriminator to distinguish "the broker does not have this
request" from "the broker has not answered yet". The only evidence available is
`orders:lookup?referenceId=`, and **its coverage is undocumented** — the same
capability question that blocks #2942 half 2. The live portal (OpenAPI v1.375.0,
verified 2026-09-13 while working #2965) documents the endpoint and its
`referenceId` selector but states no coverage guarantee, so a `not_found` cannot be
proven to mean "never accepted". Settling that empirically requires submitting an
order and is `loop-ineligible`.

So this ships #2961's explicitly sanctioned first cut — *"An operator-facing
resolution is acceptable as a first cut, but it must EXIST — today there is none at
any level."* The operator supplies the judgement the machine evidence cannot.

## Design

An operator-attended, demo-only terminalisation guarded by **three independent
conditions**, all of which must hold. Two are machine-checked at the moment of the
act; the third is the operator's.

### G1 — we never recorded broker acceptance

`orders.broker_order_ref IS NULL`. If acceptance was ever persisted, a position may
exist and this path is refused outright — that case reconciles normally.

Also refused: any active `strategy_position_ownership` row on the trade, and any
reconciliation state already terminal.

### G2 — a FRESH lookup still says the broker does not have it

The stored `not_found` is not sufficient evidence; it may be minutes or days old.
The service re-runs `broker.lookup_order(reference_id=...)` inside the act and
proceeds **only** on `BrokerOrderNotFound`. A successful lookup means the order
does exist and must reconcile normally, so the act refuses and returns that result.
A lookup *error* refuses too — an unreachable broker is not evidence of absence.

### G3 — no unowned position on the core instrument

`broker.get_account_risk_snapshot()` is read, and the act refuses if any
`direct_positions` row on the core instrument is not already claimed by a strategy
trade. This is the check that would catch a false `not_found`: if the stranded
submission *did* reach the broker, its position is sitting in the account unowned,
and the machine can see it even though the order lookup cannot.

Both G2 and G3 are **informational** broker reads. Neither mutates order or
position state, so neither is blocked by
`app/security/unattended_guard.py::refuse_broker_mutation_if_unattended`, and the
act's own acceptance runs with a broker mutation count of 0 — which is what #2961's
acceptance asserts.

### G4 — the operator attests

A non-empty `attestation` string is required and stored verbatim. It is the audit
record of the human judgement, not a machine gate, and it is the honest place where
the undecidable coverage question is absorbed. The residual risk is stated plainly:
if the original submission did reach the broker, was invisible to `referenceId`
lookup, **and** produced no position visible in the account snapshot, a later new
authority could double the position. G3 is what makes that conjunction unlikely;
the attestation is what makes it the operator's call.

### The terminal write

One transaction, reusing the vocabulary `_submit_core_authority` already writes on
a *definite* broker rejection (`strategy_core_executor.py:313-326`) so no new state
names are introduced:

- `orders.status = 'rejected'`;
- `strategy_trades.status = 'failed'`;
- `strategy_order_reconciliation_state.state = 'rejected'`, `reconciled_at = now()`,
  `last_error_code = 'operator_abandoned_unsubmitted'`.

⚠ `reconciled_at` must be set: `sql/285`'s
`strategy_order_reconciliation_resolved_shape` CHECK ties both terminal states to a
non-NULL `reconciled_at`.

`rejected` is the correct terminal state under the #2844 reader
(`strategy_engine_capital.py:274-277`): a terminal trade must have a terminal
reconciliation state and **no** active ownership, which G1 has already proven.

The whole act runs under `core_submission_lock` (`strategy_core_submission_gate.py:244`),
the same three-lock section execution takes, so it cannot interleave with a
concurrent rebalance.

### Audit — `sql/377`

New append-only table `strategy_core_authority_abandonments`:

| column | notes |
| --- | --- |
| `core_authority_abandonment_id` | BIGSERIAL PK |
| `order_id` | NOT NULL, FK `orders` ON DELETE RESTRICT, **UNIQUE** — one abandonment per authority, ever |
| `strategy_trade_id` | NOT NULL, FK `strategy_trades` |
| `core_rebalance_intent_id` | NOT NULL, FK `strategy_core_rebalance_intents` |
| `strategy_request_id` | UUID NOT NULL — the durable UUID whose absence was proven |
| `abandoned_at` | `TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()`, never a parameter (sql/346, sql/348 rule) |
| `operator_id` | NOT NULL |
| `recorded_by` | NOT NULL, non-empty, ≤128 |
| `attestation` | NOT NULL, non-empty, ≤2000 |
| `api_key_credential_id`, `user_key_credential_id` | NOT NULL — the account the two proofs were taken against |
| `account_snapshot_position_count` | NOT NULL — how many direct positions G3 examined, so a later reader can tell an empty account from an unread one |

`abandoned_at` is `clock_timestamp()` and not a parameter for the reason sql/348
states at length: a caller supplying its own time can backdate a verdict.

### Endpoint

`POST /strategies/core-sleeve/abandon-authority`, mirroring
`rebalance_core_sleeve` (`api/strategies.py:3764-3835`): `require_session`,
`settings.etoro_env != "demo"` → 409, demo credential load through
`load_credential_with_id_for_provider_use` with the `audit_pool`, and the **same**
credential-provenance guard at `:3801` — an unresolved core order belonging to
credentials that are no longer live is refused, which `sql/373` also enforces at the
database.

Body carries `order_id` (the authority the operator intends, so a different one
appearing since cannot be terminalised by a stale request) and `attestation`.

## Acceptance (#2961)

`test_scenario_2_stranded_authority_is_unreachable_by_any_unattended_caller`
inverts: after the act, the stranded authority is terminal, a new core authority is
admitted, and the SLO block clears — with the broker's mutation count still 0.

## Tests

1. **DB-backed** — the #2961 acceptance above, driven through the service against
   the #2949 harness's fake broker. Assert broker mutation count 0 throughout.
2. **DB-backed** — G1: an authority with a non-NULL `broker_order_ref` is refused;
   an authority with active ownership is refused; an already-terminal state is refused.
3. **DB-backed** — G2: a fake broker whose lookup *succeeds* causes a refusal and
   normal reconciliation instead; a fake broker whose lookup raises
   `BrokerOrderLookupError` causes a refusal (unreachable ≠ absent).
4. **DB-backed** — G3: a snapshot carrying an unowned position on the core
   instrument causes a refusal, even with a fresh `not_found`. This is the check
   that protects against a false negative, so it needs its own test.
5. **DB-backed** — the UNIQUE on `order_id` makes a second abandonment of the same
   authority fail.
6. **Pure logic** — an empty or whitespace attestation is rejected before any
   broker I/O.

## Scope

No resubmission. No change to `reconcile_backlog`'s core exclusion (#2962 owns
that). No change to the lookup-miss refusal. No frontend in this change — the
endpoint is the surface #2961 asks to exist; wiring a button is separable.
