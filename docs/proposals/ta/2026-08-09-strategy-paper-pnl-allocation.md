# Strategy paper P&L, attribution and allocation controls (#2453)

Status: implemented MVP on 2026-08-09. This is control-plane slice 9 of
`2026-08-09-strategy-automation-control-plane.md`.

## Decision boundary

The strategy page has two independent performance arms:

```text
every fired entry -> immutable outcome -> shadow performance
                  -> funding verdict -> exact owned lifecycle -> actual P&L
```

Changing a deployment ceiling never changes a signal, outcome, funding
decision, fill or ownership row. This makes the shadow record immune to later
allocation changes and keeps selection/capture bias measurable.

The account portfolio remains the complete manual plus automated view. The
strategy view is a strict subset selected by exact broker position ownership;
it does not modify or feed the account-wide calculation.

## Formula and provenance register

All percentages below are stored percentage points unless explicitly called a
fraction.

| Operator figure | Formula | Durable provenance | Unknown rule |
| --- | --- | --- | --- |
| Realised strategy P&L | `Σ close.realized_pnl_usd` | `strategy_position_ownership.broker_position_id = trade_events.position_id`, close events only | null if a released owned position has no close history or any close has null P&L |
| Unrealised strategy P&L | `direction × units × (mark − open_rate) × open_conversion_rate` | active exact ownership joined to `broker_positions`; mark from current `quotes`, then latest `price_daily` | null if ownership is unreconciled, the broker snapshot is missing, or no positive mark exists |
| Total strategy P&L | realised + unrealised | the two figures above | null unless both arms are known |
| Shadow per signal | `AVG(strategy_outcomes.gross_return_pct)` | every resolved fired entry under the current resolver/input versions | null when no outcomes have resolved |
| Captured | allocated fired entries / all fired entries | immutable `strategy_funding_decisions`; missing legacy decision is displayed as not funded / not evaluated | null when no entry has fired |
| Filled | allocated entries with a positive exact detailed-lookup execution / allocated entries | `strategy_order_position_executions` reached only through the strategy entry order | null when no entry was allocated |
| Broker rejection rate | allocated entry orders rejected / allocated entries | exact strategy entry order status | null when no entry was allocated |
| Slippage | `AVG((actual weighted fill − expected fill) / expected fill × 100)` | detailed entry executions versus `strategy_signals.fill_price`; current strategies are long-only | null without a reconciled fill |
| Skipped versus funded | average rejected shadow return − average funded shadow return | simultaneous shadow outcomes split by immutable funding verdict | null until both groups have a resolved outcome; this is a selection comparison, not a cash-profit claim |

`quotes.last` must be strictly positive. Otherwise the standard positive
bid/ask midpoint is used, then the latest positive daily close. Unlike the
account page's cost-basis display fallback, strategy attribution reports an
unavailable mark as unknown so a lack of market evidence cannot look like zero
P&L.

Manual positions cannot enter a strategy formula by instrument match, FIFO,
symbol or temporal proximity. A same-instrument manual position has no
`strategy_position_ownership` row and is therefore outside the query
denominator.

## Allocation gate

The picker permits a positive/new paper allocation only when the exact current
strategy version is runnable and all of the following are true:

- every declared primary 2022+ and rolling 24/36-month arm is complete;
- the arms have no promotion refusal and their lower expectancy bounds remain
  positive after the registered cost model;
- the current stage is `paper_enabled` or `live_enabled`;
- pinned promotion results pass the same recent, survivorship, carry, trial,
  effective-sample-size, DSR and synthetic-control contract used by the paper
  executor;
- the scan frontier is current;
- an explicit execution policy exists; and
- the deployment currency is USD for the MVP.

The request pins `strategy_version`; stale browser state receives a conflict
instead of changing a newly deployed version. Operator identity comes only
from the authenticated session. `strategy_deployments` is updated together
with a complete immutable `strategy_deployment_events` revision. There is no
automatic allocation optimiser or winner-chasing job.

If evidence becomes invalid, a new/increased allocation is refused. An
existing allocation can still be disabled and/or have its ceiling reduced;
a disabled allocation cannot be re-enabled through this exception. Evidence
failure must never trap capital in an enabled sleeve. A legacy non-USD sleeve
preserves its currency during such a reduction; it cannot be converted or
newly enabled because USD remains the only supported allocation currency.

Global kill and reconciliation block state is visible in the picker and blocks
new order entry in the executor. It does not erase the allocation audit and it
does not remove exact-position risk reduction.

The default operator view is a money workspace, not an evidence dump. It leads
with total exact-owned strategy P&L, capital state, open positions, success rate
and average return. The read-time cumulative close-event line is rendered only
after a close exists; an empty chart does not consume the page. Until automated
outcomes resolve, success and average-return fields are explicitly labelled as
representative backtest evidence. Observed and backtest populations are never
silently pooled in one aggregate.

Each strategy occupies one summary row with P&L, paired success/average return,
time to outcome, trailing 30-day signal count and an individual next-run switch.
One strategy's bounded evidence windows can expand inline. Instrument-level
events are a separate Activity view, explicitly filtered to one selected
strategy and paginated at 15 rows. Opening evidence never loads or displays the
instrument ledger.

The overview response carries the configured broker connection mode. A demo
connection does not repeat paper/live caveats across the workspace. If a
real-money-capable connection is configured while live strategy activation is
refused, the page shows one concise activation-unavailable warning.

The shared strategy pot is an additional hard ceiling across every enabled
paper strategy deployment; future live reservations are explicitly excluded.
Under the existing allocator advisory lock, sizing takes
the minimum of the shared remaining pot, per-strategy remaining ceiling,
available cash and the existing portfolio/instrument risk limits. The master
workspace switch updates that ceiling and the account-wide automatic-trading
flag under the allocator lock so the displayed state is effective, not cosmetic.
It does not override the account kill switch, per-strategy evidence gate or the
unconditional live-activation refusal.

## Database and performance impact

The read model adds no snapshot or periodic writer. Shared-pot authority uses
one narrow `strategy_paper_pool_events` row per human revision; the latest row
is current state and there is no heartbeat/current-state duplicate. Everything
else reads bounded/existing ledgers:

- fired signals and outcomes already retained by the observation policy;
- one funding/preflight row per fired entry;
- one current deployment plus immutable operator-authored revisions;
- exact order executions, position ownership and material position operations;
- the existing single-account `trade_events` ledger (hundreds of rows/year);
- current quote/position rows and bounded daily prices.

Repeated page refreshes and P&L changes write zero rows. There is therefore no
new retention job or database-growth allowance. Existing keys/indexes cover the
joins: unique funding decision per signal, trade-order indexes, unique broker
position ownership, the partial trade-event open/close indexes, current quote
PK and daily-price PK. Scan freshness reads the ingest-maintained
`research_price_series.last_bar` census rather than aggregating the full
`research_price_daily` bar heap; this avoids adding a large date-only index just
for the page. A future material scale change must be demonstrated with
`EXPLAIN (ANALYZE, BUFFERS)` before adding an aggregate snapshot table.

Dev-DB measurement on 2026-08-09 (warm cache, exact current versions) was
9.673 ms for attribution, 0.090 ms for owned P&L, 0.109 ms for controls and
0.515 ms for the 50-row signal page; all four reported zero shared-block reads.
These are evidence for the read model, not a production latency SLO.

## Validation

The integration matrix pins:

- different P&L on a strategy and manual position sharing one instrument;
- manual lifecycle exclusion from strategy totals;
- an unavailable mark returning null rather than zero;
- funded/rejected shadow attribution and exact fill slippage;
- identical shadow statistics before and after an allocation change;
- evidence-invalid allocation producing no deployment or audit row; and
- a successful update taking `changed_by` from the real session and appending
  the immutable event.
