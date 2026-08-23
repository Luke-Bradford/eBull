# Core submission preflight — the BROKER half (#2603 item 3, step 3b-2)

Status: proposed, 2026-08-23. Revised after Codex checkpoint 1, which falsified the
first draft's age-bound derivation and its sell arm. Both corrections are recorded below
rather than quietly applied.

## Why

Step 3b-1 (`app/services/strategy_core_preflight.py`) closes by naming what it does not
carry: *"account-risk availability, broker minimums, cost assessment and broker
rejection"*. It calls those "the broker half" and defers them to 3b-2.

3b-2 has shipped one of the four — the broker minimum (`578e5393`,
`broker_settlement_arms.effective_open_minimum`). This slice ships the other two
preflight-shaped ones. Broker rejection is deliberately excluded: it is an OUTCOME of
submission, not a precondition, and it belongs with the submitter alongside the
uncertain-submission resume path.

Measured before speccing, not inherited from the step-3b-2 close-out comment:

| owed item | claim re-checked against the tree | verdict |
| --- | --- | --- |
| account-risk | `get_account_risk_snapshot` callers are `strategy_paper_executor.py:1183`, `:305` and `scheduler.py:5748` (the core-sleeve OBSERVATION chain) | no submission-time re-read on the core arm — confirmed absent |
| cost assessment | `get_what_if_costs` callers are `strategy_paper_executor.py:1206` and two `scripts/` probes | `decode_quoted_trade_cost` is a decoder with no producer on this arm — confirmed |
| broker minimum | `effective_open_minimum` callers: `strategy_paper_executor.py:656` only | shipped, but unreachable from the core arm until this module calls it |

## What it is

One module, `app/services/strategy_core_broker_preflight.py`, symmetric with 3b-1:
3b-1 is the DB-and-clock half (no broker handle), this is the broker half (holds a
broker handle, makes only informational reads).

Input: an already-decided `(mandate, decision)` pair, the broker handle, the eligibility
figures already proved by `strategy_core_eligibility`, and `now`.
Output: `CoreBrokerPreflightVerdict` — admitted with a cost-adjusted size, or one named
refusal.

It **authorises nothing**: no caller in `app/` or `scripts/`, and it writes nothing.

### BUY ONLY — the sell arm refuses, and why

`sell_core` returns `core_close_side_cost_quote_unavailable`, unconditionally.

`BrokerWhatIfOrder`'s own docstring records the two measurements that force this
(#2712, 2026-08-14): the close arm **requires `position_ids`** — without them the
endpoint returns 400 *"PositionIds must be provided for close action"* — and an open-arm
quote **does not bound the close-arm cost**, measured dearer on 4 of 5 held positions by
5.7x, 8.5x, 13.0x and 18.5x. Neither `CoreSleeveState` nor `BrokerInstrumentInvestment`
carries a position id, so a close quote cannot be constructed from this module's inputs,
and the one substitution available would under-state a cost bound by an order of
magnitude — the single direction a cost bound must never be wrong in.

⚠ A refused sell is a REAL limitation of the core arm, not a formality: a rebalance that
can only buy is half an allocator. What unblocks it is threading position ids from
`broker_positions` plus a close-side floor rule, and the floor half is **unknown from the
source** (below). Named here so the submitter inherits a stated gap rather than a
silence.

### Sequence, and the refusal at each step

1. `broker.get_account_risk_snapshot()` — any exception → `core_account_risk_unavailable`.
2. Age bound on `snapshot.observed_at` → `core_account_risk_stale`.
   A stamp further into the future than the skew tolerance is stale, not maximally
   fresh — `strategy_core_preflight._FUTURE_SKEW`'s rule, for its reason.

   ⚠⚠ **Tested TWICE, and the second time is the binding one.** The entry point takes a
   `clock`, not a `now`. Codex checkpoint 2 caught the draft that took a single `now` and
   tested the age only here — before the one call whose duration the bound exists to
   cover. Since that call is the assembly's only wall clock, the bound could never fire:
   a what-if delayed by throttling, retries or an uncapped `Retry-After` still returned
   an ADMITTED verdict on an hour-old snapshot. This first test is a cheap pre-check that
   avoids spending a write-lane request on an already-stale snapshot; step 7a is the one
   that binds.
3. `observe_core_sleeve(snapshot, ...)` — `CoreSleeveObservationError` →
   `core_account_risk_unobservable`.
4. **Drift re-proof.** Re-run `evaluate_core_rebalance(mandate, fresh_state)`. If the
   fresh decision is not the one handed in → `core_sleeve_moved_since_decision`.
   This is what the account-risk read is FOR; fetching it and not comparing it would be
   a call with no consequence. Distinct from the submission gate's supersession check,
   which asks whether a NEWER STORED ROW exists — this asks whether the WORLD still
   matches the row. It also means a mismatched input decision is REFUSED here rather
   than raising later out of `resolve_core_trade_size._assert_decision_describes`.
5. Minimum-currency guard: the eligibility response currency must be USD, else
   `core_minimum_currency_unsupported`. This runs BEFORE the cost fetch so
   `effective_open_minimum`'s documented `ValueError` on non-USD stays unreachable —
   its docstring already contracts that "both callers refuse a mismatch first".
6. `broker.get_what_if_costs(...)` for the decision's pre-cost amount — any exception →
   `core_cost_assessment_unavailable`.
7a. **Re-test the snapshot age against a second clock reading** → `core_account_risk_stale`.
   Same condition, so deliberately the same code. This is the check the bound is for.
7. `decode_quoted_trade_cost(...)` — returns a `CoreSizingRefusalCode` on failure, which
   is passed through unchanged rather than re-coded.
8. `resolve_core_trade_size(...)` — likewise passes its own refusal code through. Its
   `_MAX_TICKET_EXTRAPOLATION` guard is what bounds quoting the PRE-cost amount and
   sizing to a different final amount: a solve landing more than 2x from its own quote
   refuses rather than extrapolating.
9. Broker floor: `effective_open_minimum(...)`; `None` →
   `core_broker_open_minimum_unquoted` (fail closed — `None` means the broker quoted no
   usable threshold, not that any size is permitted); sized amount below it →
   `core_below_broker_open_minimum`.

⚠ **No close-side floor exists to apply.** The portal documents both minimums for
OPENING and says nothing about closing or partial closing (`effective_open_minimum`,
portal 2026-08-23). That is UNKNOWN, not "no constraint" — and with the sell arm refused
outright, nothing in this module proceeds through the unknown.

## Source rule — the account-risk age bound

There is no producer cadence to derive from: the snapshot is a LIVE read stamped at
receipt (`_parse_account_risk_snapshot` takes `observed_at` as a parameter; the payload
carries no broker valuation stamp). So `strategy_core_preflight._freshness_bound`, which
derives from a producer's nominal period, does not apply and is deliberately not reused.

⚠⚠ **The first draft derived this bound from the worst-case duration of the intervening
what-if call. That derivation is unsound and is recorded here so it is not re-attempted.**
Measured in the transport:

- `get_what_if_costs` posts on `_http_write` (`etoro_broker.py:775`), whose interval is
  `_ETORO_WRITE_INTERVAL_S = 3.5`, not the 1.1 s read lane.
- The throttle runs INSIDE the retry loop (`resilient_client.py:186`), so it is paid once
  per attempt, not once per call.
- On a 429, `Retry-After` **overrides** the backoff schedule with no upper cap — only a
  0.1 s floor (`resilient_client.py:259`, `_parse_retry_after`). A server sending
  `Retry-After: 3600` is honoured three times over.

So the worst case is **unbounded**, and a bound derived from it would be a constant that
can never fire — decoration wearing a derivation.

The bound is therefore fixed by construction at the **nominal single-attempt duration**
of the one intervening call:

| input | value | source |
| --- | --- | --- |
| one write-lane throttle wait | 3.5 s | `etoro_broker._ETORO_WRITE_INTERVAL_S` |
| one HTTP round trip | 30.0 s | `etoro_broker` `httpx.Client(timeout=...)` |

`3.5 + 30.0 = 33.5` → **34 s**, rounded up.

⚠ `httpx`'s `timeout=30.0` sets connect, read, write and pool timeouts to 30 s each; it
is NOT a total wall-clock cap on one attempt. It is used here as a single-phase proxy for
the nominal round trip, which is what "nominal" means. It is not claimed as a hard bound.

**A retrying call will typically exceed 34 s, and refusing is the intended outcome.** A
submission whose cost quote needed retries is one whose account view we no longer trust;
the caller re-runs from a fresh snapshot. The direction matters: this bound is meant to
bite, whereas one sized to the transport's worst case never would.

⚠ It bounds a STALL, not market movement. Nothing here makes the snapshot and the what-if
simultaneous, and no second snapshot is taken after the cost call — cash, holdings and
pending orders can move inside the window and still be admitted. The sleeve-vs-cost
coherence question is separately bounded, by `decode_quoted_trade_cost`'s
`cost_quote_stale` against `CoreSleeveState.as_of`.

⚠ Frozen in `CORE_BROKER_PREFLIGHT_POLICY_VERSION`. Widening it is a version bump, never
an edit to the constant — the rule every stamped policy in this arc already follows.

A coupling test asserts both inputs still hold, so a provider retuning its throttle or
timeout fails there rather than silently invalidating the derivation. `timeout=30.0` is
hoisted to a named constant in `etoro_broker.py` for that test to import; the service
module does NOT import the provider implementation.

## Not carried — stated, not silent

- **Broker rejection**, and the **uncertain-submission resume path**: outcomes of
  submission.
- **`max_units_per_order`**: quoted by eligibility in UNITS while this module sizes in
  currency, so applying it needs the price, which 3b-1 holds and this module does not.
  Owed to the submitter that holds both.
- **One-world coherence.** Eligibility, account state, quote, cost quote and the DB
  preflight are five reads at five instants. The advisory lock serialises OUR actors, not
  the broker's. The ticket already owes "eligibility read and submission in ONE
  transaction" and "the intent's absolute freshness bound via one lock hold" to the
  submitter; this module does not discharge either.
- **The acting caller.** Shipping the vocabulary before the caller is this arc's
  established order (3a before 3b, each naming itself AUTHORISES NOTHING).
- **Any broker mutation.** Both reads are informational and deliberately NOT covered by
  `refuse_broker_mutation_if_unattended` (#2645).

`CoreSizingContractError` is allowed to propagate rather than being caught into a
refusal: a decision that does not describe its own (mandate, state) is a CALLER DEFECT,
the distinction `CoreSleeveObservationError` and `StrategyCorePreflightError` both draw.
Step 4's drift re-proof means it is unreachable through this module's own path.

## Tests

Pure-logic, table-driven, with a broker double: one test per refusal code; the age-bound
boundary (exactly at, one second past, and a future stamp beyond skew); pass-through of
`decode_quoted_trade_cost` and `resolve_core_trade_size` codes; the sell arm refusing
before any broker call is made; the request shape handed to `get_what_if_costs` (action,
transaction, settlement type, leverage, amount) asserted against the decision, since the
response echoes none of them; and the coupling test above.
