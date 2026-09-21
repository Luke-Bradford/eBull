# Broker status vocabulary, and why #2965's ownership half is blocked

Status: verdict + narrow fix · 2026-09-13 · Refs #2965, #2961, #2962, #2602, #2844, #2949

> ⛔ **Two claims below are STALE as of 2026-09-21 and were quoted forward as current,
> costing a design.** See `2026-09-21-closed-execution-ownership-claim.md`.
>
> 1. **"`reconcile_backlog` excludes core outright … The window is unbounded"** (under
>    "Rejected design A") — the exclusion was **removed by #2962**. The backlog now covers
>    both arms on the 5-minute strategy cycle; see
>    `strategy_order_reconciliation.py:1041-1045`.
> 2. **"both release paths require that *we* authored the close"** —
>    `release_exact_position` needs only an active pair and a reason. The real gap is that
>    it has **no production caller**.
>
> The blocked verdict itself still stands, re-derived against the live portal on
> 2026-09-21: the page documents no partial-fill representation.

Two designs for #2965 were written and both were killed at Codex checkpoint 1. The
reasons are the substance of this document; the shipped change is the small,
separable part that survived.

## What ships

Five strings added to the broker-status classifier in
`app/services/strategy_order_reconciliation.py:61-63`, plus a table test. Nothing
else. The ownership half of #2965 is **blocked** — see "Verdict" below.

## Source rule — eToro live portal, OpenAPI **v1.375.0**, verified 2026-09-13

`GET /api/v2/trading/info/{demo|real}/orders:lookup`, portal slug
`trading--demo/get-order-information-and-position-details`, fetched with WebFetch
per `.claude/skills/data-sources/etoro-api.md` "Verification protocol" (curl is
Cloudflare-blocked). Documented `status.name` enum, verbatim:

> Received, Placed, Filled, Rejected, PartiallyFilled, PendingCancel, Canceled,
> Expired, CanceledPartiallyFilled, RejectedPartiallyFilled, WaitingForMarket,
> PendingTriggeredRate

Our classifier names seven strings, of which **three** appear in that enum:

| constant | members | documented? |
| --- | --- | --- |
| `_KNOWN_PENDING_BROKER_STATES` | `Pending` | **no** — no bare `Pending` exists |
| `_KNOWN_FILLED_BROKER_STATES` | `Filled`, `Executed` | `Filled` yes; `Executed` no |
| `_KNOWN_REJECTED_BROKER_STATES` | `Rejected`, `Failed`, `Cancelled`, `Canceled` | `Rejected`/`Canceled` yes; `Failed`/`Cancelled` no |

So **nine of the twelve documented values** reach
`raise StrategyReconciliationError(f"unknown broker order status: {broker_status}")`
(`:331`). `_apply_detail` runs inside a `try` whose handler is
`_record_failure(..., state="ambiguous")`, and `ambiguous` is not in
`_TERMINAL_RECONCILIATION_STATES` — so a **documented** broker status parks the
order in a non-terminal state. That is the #2961/#2962 wedge, reachable today from
nine strings eToro says it can send.

⚠ The four undocumented strings (`Pending`, `Executed`, `Failed`, `Cancelled`) are
**retained, not removed.** No evidence says the demo connection never emits them,
and removing one is a narrowing change measured only on its admit side. This change
is purely additive.

## The fix

| constant | added | why safe |
| --- | --- | --- |
| `_KNOWN_PENDING_BROKER_STATES` | `Received`, `Placed`, `WaitingForMarket`, `PendingTriggeredRate` | all four are pre-fill working states, so they carry no `positionExecutions` and cannot reach the ownership claim at `:341-348` |
| `_KNOWN_REJECTED_BROKER_STATES` | `Expired` | terminal with no fill; an `Expired` response that *did* carry executions still raises on the existing `:336` guard, which is the correct fail-closed answer for contradictory broker data (eToro documents no `ExpiredPartiallyFilled`) |

**Deliberately still unknown, so they keep raising:** `PartiallyFilled`,
`PendingCancel`, `CanceledPartiallyFilled`, `RejectedPartiallyFilled`. These are
exactly the four that can carry `positionExecutions` on a non-`Filled` order, and
admitting any of them activates the unsettled ownership lifecycle below. Raising is
no worse than today's behaviour for these strings, and it is honest about what is
unsettled.

`app/providers/implementations/etoro_broker.py:114` `_STATUS_MAP` is **not**
touched. It is shared between the v2 `status.name` parse (`:1294`) and a second v1
status parse (`:1867`), and `BrokerOrderDetail.status` has no consumer in the
reconciliation path — only `broker_status` is read. Aligning it would be risk
without benefit here. Separable finding.

## Verdict — #2965's ownership half is blocked, on two independent grounds

The issue asks whether the claim (`strategy_order_reconciliation.py:341-348`,
unconditional on state) or the reader (`strategy_engine_capital.py:278-280`, raises
on pending-with-ownership) is wrong. Both answers were designed and both fail.

### Rejected design A — defer the claim until the state is terminal

A partially-filled order holds a real broker position, and the ownership row is the
handle its safety machinery uses. Deferring makes it invisible to the position
manager and close paths, the core mandate's active-position guard
(`strategy_core_mandate.py:396`), the retirement guard (`api/strategies.py:4375`),
stale-quote health (`strategy_paper_runtime.py:240`), the daily-loss population
(`strategy_paper_executor.py:806`), and NAV history — where
`strategy_wealth.py:48` marks from `claimed_at`, so the partial-fill days are
**permanently** missing and later claiming does not repair them.

The "bounded by one 5-minute cycle" argument is false for the arm that matters:
`reconcile_backlog` excludes core outright
(`strategy_order_reconciliation.py:532`, `AND trade.core_rebalance_intent_id IS
NULL`), so core has no unattended reconciler at all (#2962). The window is
unbounded.

### Rejected design B — keep the claim, narrow the reader to an orphan check

Killed by its own justification. Every consumer named above gates on **trade
status**, not on ownership: `t.status IN ('open','closing','reconcile_required')`
(`strategy_position_manager.py:332`, `api/strategies.py:3201`, `:3342`), while
`_apply_detail:383` writes `submitted` for any non-terminal entry. So retaining the
ownership row does not make a partial fill manageable or closeable — design B buys
nothing it claimed to buy.

It also loses real protection: `recon_state != "resolved"` covers `ambiguous`,
`error`, `not_found` and `rejected`, and the original raise blocked ownership in
all of them. And it leaves the instrument-change hole open, because
`configure_core_mandate` checks `core_active_position_ids`, which design B keeps
empty for a partial fill.

### The blocking fact — loop-ineligible

Underneath both designs sits an unanswered empirical question about the broker:
**does a partial fill grow one `positionId`, or emit a new one per fill?**

It decides the ticket. `_record_execution`'s `ON CONFLICT` treats
`openingData.units`, `avgPrice`, `executionTime` and `fees` as immutable
(`:227-243`) and raises `"broker changed immutable opening execution facts"` when a
later poll disagrees. If eToro grows one position, then admitting `PartiallyFilled`
creates a **new** guaranteed wedge on the second poll of every partial fill. The
portal reports `openingData.avgPrice` — an *average*, which is evidence for the
growing-position reading, but it is evidence and not proof.

Settling it requires observing a real partial fill, which requires submitting an
order. That is a broker mutation, so it is `loop-ineligible` under the autonomy
loop's hard safety rules and cannot be done unattended.

### What would unblock #2965

1. One attended demo order large enough to partially fill, polled twice through
   `orders:lookup`, recording whether the second poll returns the same `positionId`
   with changed `units`/`avgPrice`. That single observation decides whether
   `_record_execution`'s immutability contract survives contact with a partial fill.
2. A trade-status lifecycle decision on #2602's surface: what status a
   partially-filled-but-still-working entry carries, given that `submitted`
   excludes it from every management and close path. Without this, no ownership
   design can make a partial fill safe.

Both are prerequisites, and (1) cannot be done by this loop.

## Separable defects found during this pass — recorded, not fixed

Each is on #2602's reconciliation surface and none is load-bearing for the five
strings shipped here:

- an earlier response's executions can be abandoned when a later response returns a
  different or empty set (`:336`, `:341`);
- `_record_failure` can overwrite a terminal row with `ambiguous` (`:148`); the
  `:333` regression guard does not prevent it — this is #2962's finding;
- `_claim_entry_execution` treats an existing *released* same-trade row as a
  successful claim (`:278-285`);
- `reconcile_strategy_order` returns early on a terminal row (`:434`), so a broker
  correction cannot repair ownership;
- an execution whose `state` is `closed` or whose `remainingUnits` is 0 is still
  claimed as active ownership;
- `api/strategies.py:2959` overrides `execution_status` to `'filled'` whenever an
  execution average price exists, so a partial fill would report as filled on that
  operator surface regardless of any `orders.status` label;
- `_STATUS_MAP` (`etoro_broker.py:114`) is a second, unaligned classification
  shared across a v1 and a v2 status field.

## Test

Pure logic, no DB: a table test over the classifier covering all twelve documented
values plus the four retained undocumented ones. The nine that now classify assert
their `(reconciliation state, orders.status)` pair; `PartiallyFilled`,
`PendingCancel`, `CanceledPartiallyFilled`, `RejectedPartiallyFilled` and an
arbitrary unknown string each assert the raise — so a future session that admits
one of the four must delete an explicit assertion that says why it is excluded.
