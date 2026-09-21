# The closed-execution ownership claim — REFUSED, and why the family is refused

Status: verdict · 2026-09-21 · Refs #2965, #2602, #2844, #2962

A third design for #2965 was written and killed at Codex checkpoint 1, joining designs A
and B from `2026-09-13-partial-fill-ownership-and-status-vocabulary.md`. The reasons are
the substance of this document. **Nothing ships.**

What is new and durable is the source rule, one measurement, and the structural reason —
which was available in the same function the ticket already cites and was not read.

## Source rule — eToro live portal, verified 2026-09-21

`GET /api/v2/trading/info/{demo|real}/orders:lookup`, portal slug
`trading--demo/get-order-information-and-position-details`, fetched with WebFetch per
`.claude/skills/data-sources/etoro-api.md` "Verification protocol" (curl is
Cloudflare-blocked). OpenAPI 3.0.1. Verbatim:

| field | documented description |
| --- | --- |
| `positionExecutions[].state` | "The **current** state of the position. Possible values: **open, closed**." |
| `positionExecutions[].remainingUnits` | "Remaining units **in the position**." |

Neither appears in #2965 or in the 2026-09-13 proposal. Two consequences:

1. `state` is a **closed two-value enum**. We parse it as any non-empty string
   (`etoro_broker.py:1414-1418`).
2. `remainingUnits` is scoped to the **position**, so it measures partial *close*
   progress, not partial *fill* progress. It can never gate an ownership claim, because
   `0 < remainingUnits < openingData.units` is an ordinary partially-closed position that
   is still owned.

**The portal documents no partial-fill representation** — there is no sentence on how
multiple fills of one order appear. So #2965's prerequisite (1), *does a partial fill grow
one `positionId` or emit a new one*, is genuinely unanswerable read-only. That half of the
2026-09-13 blocked verdict is re-derived, not inherited, and it **holds**.

## Measurement

`position_state` is **written and never read**. Over `app/`, grep returns exactly two
sites, both inside `_record_execution`'s INSERT (`strategy_order_reconciliation.py:549`,
`:554`). The only other occurrences anywhere are the column add (`sql/285:45`) and a
length bound (`sql/286:27`). So a `closed` execution is indistinguishable from an `open`
one at every consumer.

Observed corpus: `strategy_order_position_executions` holds one row —
`position_state='open'`, `remaining_units=0.296155`, the live SPY.RTH core position from
2026-09-18. The one observed value is inside the documented enum. **n=1: that states what
has been observed, not a rate.**

## Rejected design C — do not claim an execution the broker reports `closed`

The claim loop (`strategy_order_reconciliation.py:679-686`) calls
`_claim_entry_execution` for every entry execution without reading `execution.state`, so a
`Filled` order polled after its position closed claims a dead position as
`status='active'`. Design C gated the claim on the documented enum.

### Killed by the capital reader's *other* invariant

`load_engine_capital_authority` has two opposed raises on the same core branch, not one:

```python
# strategy_engine_capital.py:342-347   — the one #2965 quotes
if recon_state != "resolved":
    if owned_ids:
        raise ... f"core trade {trade_id} owns positions before entry resolution"

# strategy_engine_capital.py:354-357   — the one it does not
if not owned_ids:
    raise ... f"resolved core trade {trade_id} has no active exact ownership"
```

Together they pin a biconditional: **a non-terminal core entry is `resolved` if and only
if it holds at least one active ownership row.** Design C produces resolved-with-zero-
ownership, which is exactly the second raise. It does not remove the wedge; it moves it
from the pending branch to the resolved branch, and the core arm goes down identically.

Claim-then-release fails the same way — a `released` row is not in `owned_ids`, so the end
state is identical.

⚠ This is the structural reason the whole family is refused, and it generalises: any
disposition that lets a resolved core entry hold zero active ownership must also change
the reader, and the reader is #2602's accounting rule. Design A was killed on 2026-09-13
for deferring the claim; design C is design A arriving through a different door.

### Five further defects in design C, each independently sufficient

1. **Skipping loses enforced identity.** `strategy_order_position_executions` is unique on
   `(order_id, broker_position_id)`; `strategy_position_ownership.broker_position_id` is
   **globally** unique (`sql/281:188`), and `_claim_entry_execution` is what raises
   `StrategyOwnershipError` when a second trade claims the same position. Skipping the
   claim silently permits conflicting cross-trade attribution that raises today.
2. **Execution persistence is not an accounting substitute.** Realised capital delta, NAV
   marking (`strategy_wealth.py:48`, from `claimed_at`), the daily-loss population and
   promotion fee evidence all join through ownership, not through executions. #2965's own
   hint — *"`_record_execution` keeps the id independently of ownership, which may mean
   ownership can safely wait"* — is therefore false for accounting.
3. **`reconcile_required` does not keep the trade manageable.** The management, listing and
   close paths require `t.status IN ('open','closing','reconcile_required')` **and** an
   active ownership row (`api/strategies.py:3412`). With none, the close endpoint 404s.
   This is precisely what killed design B, re-derived against design C.
4. **`reconcile_required` has no recovery transition and reserves capital.** It is
   non-terminal, so the trade keeps its allocation and a mandate position slot forever,
   and no specified writer finishes the accounting. That trades a stuck ownership row for
   a stuck trade plus stuck capital — strictly worse.
5. **The unknown-state raise is a narrowing change measured only on its admit side.** Any
   casing or vocabulary drift outside the two documented strings would retry forever,
   reserve capital and eventually trip reconciliation-health blocking. The 2026-09-13 doc
   recorded exactly this lesson for order statuses; design C reintroduced it one field
   over. A single unknown execution also rolls back the whole transaction, leaving valid
   `open` siblings in the same response unclaimed and unmanaged.

## Corrections to `2026-09-13-partial-fill-ownership-and-status-vocabulary.md`

Both were quoted into design C as current and are stale. Grep before cite: the 09-13 doc
was right on 09-13.

- ⛔ **"`reconcile_backlog` excludes core outright (`:532`, `AND
  trade.core_rebalance_intent_id IS NULL`), so core has no unattended reconciler at all
  (#2962). The window is unbounded."** — **The exclusion was removed.**
  `strategy_order_reconciliation.py:1041-1045` now reads *"The selection covers BOTH arms
  (#2962). It carried `AND trade.core_rebalance_intent_id IS NULL` from `608dc879` until
  now"*. Core is reconciled unattended on the 5-minute strategy cycle. The fill→poll
  window is bounded, not unbounded, which lowers this ticket's urgency rather than raising
  it.
- ⛔ **"both release paths require that *we* authored the close"** — `release_exact_position`
  (`strategy_control_plane.py:1325`) requires only an active pair and a reason. It has
  **no production caller** (grep: one export, one test), which is a stronger statement of
  the same gap: nothing automatic releases ownership of an externally-closed position.

## What is genuinely open, with named unblocks

1. **The partial-fill half** — unchanged, `loop-ineligible`, needs one attended demo order
   large enough to partially fill, polled twice. Re-derived above against the portal.
2. **Externally-closed positions** — the durable defect, and it is *not* a reconciliation
   defect: `reconcile_strategy_order:959-966` returns early on a terminal state with no
   broker call, so reconciliation can never be the detector. `record_trade_events`
   (`trade_events.py:533`) already ingests broker closed-trade history through
   `sync_portfolio`, so the data to release ownership exists; the missing piece is a
   consumer that joins close events against active ownership and calls the
   already-written, never-called `release_exact_position`. **That is the buildable
   increment on this surface, and it is not refused.**
3. **`claim_exact_position` (`strategy_control_plane.py:1247`) is a second claim path** that
   also ignores `position_state`. Any future disposition has to cover both.

## Process lesson

**The function you are already citing can hold the invariant that refuses you.** #2965
quotes `load_engine_capital_authority:278-280` and frames the ticket as "the claim versus
the reader". The reader has two opposed raises twelve lines apart, and the second one
(`:354`) pins the first. Reading the quoted lines rather than the enclosing function cost
a whole design — after two prior designs died in the same place. When a ticket frames a
conflict as A-versus-B, read B completely before proposing a change to A; a one-sided
invariant is usually half of a biconditional.
