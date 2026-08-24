# Core/cash operator surface and attended submitter (#2603)

Status: proposed, 2026-08-24.

## Outcome

`/strategies` must remain useful when no alpha strategy has earned capital. It must show
the programme's honest fallback — cash until the cap-weighted core sleeve passes its
preregistered cost gate — and, after that gate passes, let a named operator configure
the mandate and request one guarded demo rebalance.

This does **not** adopt a sleeve before #2833 has a verdict. On 2026-08-24 the dev store
contains one trading day of observations and #2833 requires five; the earliest honest
decision is 2026-08-28. Until a selected instrument is frozen in code from that verdict,
the server reports `evidence_collecting`, offers no instrument and refuses enablement.

## Existing contracts retained

- The mandate, allocator, immutable intent, trade arc, submission gate, DB/clock
  preflight and broker preflight remain the decision spine.
- The core path reads mandate plus current account holdings; it never reads alpha
  signals or treats an alpha candidate as approved.
- Demo only, underlying long, leverage 1, USD order currency. The global kill switch,
  runtime auto-trading flag, execution block, market session, halt feed, fresh quote,
  fresh account snapshot, fresh eligibility proof and cost quote all remain binding.
- One server-side selection invariant is called by mandate configuration **and** by the
  acting path while the mandate lock is held. A legacy enabled mandate naming any other
  instrument is refused; checking only the endpoint would be a bypass.
- `core_submission_lock` spans the DB decision phase: record -> admit -> DB preflight ->
  durable order authority. The request UUID is committed before mutating broker I/O and
  reused after uncertainty. No second request identity is generated.
- Broker mutation remains refused from linked worktrees. The UI calls the main-checkout
  dev API; tests use broker doubles. A real demo acceptance is an attended post-merge
  check only after #2833 passes and the operator has deliberately cleared every gate.
- Sell remains refused as `core_close_side_cost_quote_unavailable`; the page says the
  allocator is buy-only rather than presenting a complete rebalance loop.

## Server API

### `GET /strategies/core-sleeve`

One canonical operator view, assembled server-side:

- selection: `evidence_collecting | ready | unavailable`, selected instrument identity
  when frozen, evidence window/trading-day coverage and the #2833 threshold;
- current mandate revision (the existing `CoreMandateResponse` fields);
- latest intent plus the independently selected blocking non-terminal core trade/order
  (which can belong to an older intent) and its reconciliation state;
- `can_configure`, `can_rebalance` and ordered blocker codes with server-authored detail;
- explicit limitations: demo-only, buy-only, no alpha input, and ISA household caveat.

The first slice freezes no selected instrument. A later evidence-verdict change is a
small, reviewed constant update carrying the #2833 evidence reference and chosen
instrument; it is not an operator-selectable numeric id.

### `PUT /strategies/core-mandate`

Keep the existing authenticated append-only writer, but put the invariant in
`configure_core_mandate`: an enabled request must name the server-selected instrument
and the selection must be `ready`. This closes every caller, not only HTTP. Disabling
remains available regardless of selection state; a legacy enabled/non-selected mandate
must still be disableable from the page.

### `POST /strategies/core-sleeve/rebalance`

Authenticated, attended request; no amount or instrument in the body. The server:

1. requires global demo configuration and loads the session operator's current demo
   credential ids and secrets. Those exact ids remain attached to the broker handle;
2. outside a DB transaction, loads the fresh broker account snapshot and informational
   cost quote inputs. No database lock is held across retryable network I/O;
3. opens `core_submission_lock` and one short transaction, re-loads the mandate, checks
   the server selection invariant, observes the already-fetched sleeve and records one
   immutable intent;
4. admits that exact intent, runs DB/clock preflight, and verifies the admission proof's
   credential ids equal the already-loaded pair. The `FOR SHARE` lock prevents a swap
   until this transaction commits; the broker handle then continues with the exact
   proved secrets rather than re-resolving labels after commit;
5. applies broker cost sizing from the fresh observations. `maxUnitsPerOrder` is retained
   as broker rejection evidence, not converted into an invented amount bound: a
   market-by-amount order has no source-backed pre-trade conversion that guarantees its
   eventual units. The broker remains the authoritative enforcer and an explicit reject
   is terminal evidence;
6. for a hold/refusal, commits the evidence and returns without broker mutation;
7. for an admitted buy, inserts the core `strategy_trade`, strategy-origin `orders` row,
   link and reconciliation authority, assigns the durable request UUID, and stores the
   exact eligibility proof id on the trade as account provenance;
8. commits before mutating broker I/O and submits through a dedicated demo-only method;
   on acceptance, persists the normalized order id, reference id and response digest
   before advancing order/trade status. The response token and raw body are not retained,
   following the settled #471 eToro retention contract;
9. on explicit rejection sets order `rejected`, trade `failed`, reconciliation
   `rejected`; on transport,
   server or response ambiguity leaves the durable authority `reconcile_required` and
   resolves only by lookup using the same UUID.

Reconciliation resolves the core trade's proof id to the exact credential ids used at
submission. If those credentials have been revoked and the same account cannot be
proved, reconciliation fails closed with a named account-provenance blocker; it never
looks up the request UUID through the sole operator's newly-current account.

The endpoint returns `held | refused | submitted | submission_uncertain`, the intent,
trade/order identities, amount, reason and policy versions. A 409 is reserved for a
control-plane refusal before an intent can be evaluated; allocator/preflight refusals
are successful evidence-bearing responses.

## Broker contract

Add `BrokerCoreOrder` and `place_demo_core_order`. It hardcodes the v2 demo endpoint,
`open/buy/real/mkt/x1/usd`, accepts a caller-owned UUID, and deliberately sends no
stop-loss or take-profit. It has the same exact reference check and uncertainty
classification as `place_demo_strategy_order`, calls the unattended mutation guard
before I/O, and never falls back to `place_order`.

The accepted result returns normalized order/reference identity plus a canonical response
digest. Those fields are durably written before the order/trade status transition. The
raw body and response token are not retained: the repository's #471 coverage audit
settled eToro persistence on normalized decision-bearing facts plus a fingerprint.

## Page

Add a `Core & cash` panel above alpha automation. It owns one async source and renders
loading/error/empty explicitly. In today's state it shows cash as the active fallback,
the server-derived trading-day coverage and earliest possible decision date, and a
disabled setup action with the server reason. It does not ask for an instrument id.

When selection is ready it exposes mandate fields (core target, liquidity reserve,
rebalance band, minimum rebalance amount and required audit reason), an enable checkbox,
Save, and a separate
`Rebalance demo now` button. The mutation button opens a confirmation modal naming the
instrument, maximum mandate exposure, demo environment and buy-only limitation. It is
disabled with visible reasons whenever the server says `can_rebalance=false`. A
`submitted` result is labelled broker-accepted/pending reconciliation, never filled.

Alpha Automation remains separate and disabled when no strategy passed. Core holdings
continue through the existing positions/P&L panels under the `Core / cash mandate`
presentation identity.

## Tests and verification

- Pure and DB tests for selection refusal, mandate bypass closure (including a legacy
  enabled eligible-but-unselected mandate), orchestration order,
  every early refusal, hold, accepted buy, explicit rejection, uncertainty, duplicate
  request/reconcile, credential replacement, exact proof provenance, older blocking
  trades and lock/transaction boundaries.
- Provider tests for exact core request shape, demo-only refusal, reference mismatch,
  raw payload and unattended guard inventory.
- API tests for auth, honest collecting state, enable refusal and response shapes.
- Frontend tests for loading/error/collecting, server-derived progression, editable ready
  state, legacy disable, audit reason, blocker rendering, confirmation, post-mutation
  refetch and each mutation outcome including accepted-versus-filled wording.
- Before first push: repository Codex review, scoped backend/frontend tests, mandatory
  gates. After merge: main-checkout page smoke. A broker-accepted demo order is attempted
  only once #2833 is complete and a mandate is deliberately enabled; until then the
  verified lifecycle ends at the expected evidence refusal.
