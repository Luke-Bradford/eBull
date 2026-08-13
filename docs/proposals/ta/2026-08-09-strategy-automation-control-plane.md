# Strategy automation control plane — ownership, allocation and monitoring

Date: 2026-08-09
Status: Slices 1–10 implemented; bounded demo paper loop available, real-money strategy activation evidence-blocked
Parent: #2437
Companion: `2026-08-09-evidence-backed-signal-engine.md`

## North star and non-promise

The product goal is a hands-off **strategy sleeve** that can observe a validated
signal, reserve explicitly allocated capital, place an exact broker order with a
fixed SL/TP, manage only the position it opened, reconcile the full lifecycle and
report strategy P&L separately from manual holdings.

No system can promise winning trades. The enforceable target is narrower and
testable: act only on pre-registered signals whose lower-bound net expectancy is
positive under current broker constraints, and stop new entries when the live
evidence or data quality breaches a declared limit.

## Current repo reality

Already present and reusable:

- a complete versioned strategy manifest for S-1 through S-4;
- daily `strategy_signal_scan`, scheduled after stored market data, recording
  fired, not-fired and not-evaluable verdicts independently of funding;
- signal, outcome and result ledgers;
- next-bar fills, causal masks, ambiguity arms, portfolio backtesting, purged
  folds/embargo, block bootstrap, Deflated Sharpe and synthetic controls;
- per-position broker snapshots, exact position close API, fixed/trailing SL/TP
  on entry, portfolio risk, execution guard and global kill switches;
- current v2 eligibility and what-if-cost adapters, implemented without a writer.

Two bounded authenticated demo censuses (2026-08-09) proved that preflight
cannot be replaced by local catalogue state. The versioned follow-up used four
stocks and four ETFs selected at deterministic liquidity quantiles: all eight
resolved, but one locally tradable instrument was refused for opening. It then
round-robin sampled 20 complete 1x/10x long-real and x1 short-CFD cost arms
across seven permitted instruments, within the endpoint's 20/minute cap. Every
response used undocumented `value` and omitted documented `amount`, so **zero
of 20 were execution-usable**. Eighteen were about 41 hours stale and only two
were within 24 hours. Exact scaling relationships were mixed: proportional,
invariant, rounded/other and zero-only. The earlier five-month-stale refused
instrument is no longer sent to the cost endpoint. These are measured blockers,
not an execution-ready cost contract.

The developer-database census on the same date found 34,698 signal-detail rows
from the latest successful scan, zero resolved outcomes, 36 result rows and 48
fold rows. The signal relation was already 32 MB (8 MB heap, 24 MB indexes).
Every stored result covered 1962-01-02 through 2026-07-08, and S-4 had no result.
Those result rows are legacy/stress evidence only: none satisfies the primary
2022+ plus rolling 24/36-month relevance contract and none is allocation-ready.

The #2447 read paths were measured on that population after implementation and
after the primary recent-window run added 12 compact result rows
(`PYTHONPATH=. uv run python scripts/verify_2447_strategy_monitoring.py`).
Latest and cursor fired-signal pages used the `strategy_signals` primary key
backwards and returned 50 fully joined rows in **under 1 ms**. Current-version
scan aggregates deliberately used a 34,698-row sequential scan and completed in
**15.6 ms**. The exact-provenance query returned the 12 recent arms in **0.1 ms**
and the separate all-result count completed in **0.03 ms** across 48 stored rows. Those figures do
not justify another index today; #2448 re-measures after retention/partition
design rather than paying write amplification speculatively.

The audited primary 2022-01-01 through 2026-07-08 run is complete for all four
ambiguity/quarantine arms of S-1 through S-3. Its admitted/best-case results
were S-1: -84.75% total return, -2.23 Sharpe and -84.98% maximum drawdown;
S-2: +41.33%, 0.44 Sharpe and -30.55% drawdown; S-3: -48.45%, -0.46 Sharpe and
-52.41% drawdown. None is allocation-ready: every arm records the measured
survivor-only universe, unmodelled carry and absent synthetic control as
refusals. S-4 remains explicitly excluded because its level-based entry has no
outcome at the pinned version pair. These results validate the storage and
monitoring path; they do not validate a winning strategy.

The original execution gaps below are now implemented for the demo paper
lifecycle: explicit allocation and policies, crash-safe reconciliation, exact
ownership, owned-position management, strategy-only P&L, health blocks and the
recurring bounded scheduler. The remaining blockers are deliberately narrower:

- no registered strategy version currently passes the recent evidence and
  paper-observation requirements;
- the measured eToro cost response is not execution-usable, because it supplies
  undocumented `value` rather than current documented monetary `amount`;
- a live strategy writer has therefore not been validated or made selectable;
- the historical outcome resolver is long-only. Short research may use the new
  preflight to measure feasibility, but short execution remains blocked until
  labels, returns, costs, guards and position accounting are side-aware.

## The three ledgers must remain independent

```text
all evaluated bars -> signal ledger -> outcome ledger -> shadow performance
                           |
                           +-> funded candidate -> owned trade -> actual P&L
```

The monitoring surface reads both arms. A signal does not disappear because cash
was unavailable, the strategy was paused, eligibility failed, or another trade
already consumed the risk budget. This preserves the allocation-unbiased shadow
track record and makes capture rate measurable.

"All signals" on the operator page means every **fired** entry/exit signal,
whether funded or not. Rendering millions of `not_fired` rows is neither useful
nor the requirement. Those rows support coverage/exclusion statistics, shown as
aggregates by date, strategy and reason.

## Promotion and allocation are different decisions

One immutable strategy version moves through:

```text
research_candidate -> historical_validated -> forward_observation
                   -> paper_enabled -> live_enabled
any enabled state  -> paused -> retired
```

- A computed metric never changes the stage automatically.
- Each promotion pins the evidence/result ids, gate version, operator, time and
  reason. A new code/parameter/universe/cost version starts again at research.
- `paper_enabled` and `live_enabled` are separate promotions. The existing
  `enable_auto_trading` and `enable_live_trading` switches remain higher-level
  gates; neither promotes a strategy.
- Allocation is an operator-entered maximum pot, not a command to spend it. The
  gate sizes each order within available sleeve cash and portfolio-wide risk.
- Capital is never automatically moved toward last month's winner. That creates
  a second, unregistered timing strategy and performance-chasing bias. The picker
  supplies comparable evidence; the operator changes allocations deliberately.

## Strategy picker contract

Each picker row must pin one strategy version and show, without favourable
default sorting:

- stage and whether observation/paper/live is enabled;
- rules, universe, side, fill convention, SL/TP/timeout and cost model;
- primary 2022+ and rolling 24/36-month net expectancy with confidence interval;
- forward-observation and paper figures in separate columns, never pooled with
  backtest results;
- trade count and effective sample size, win rate, average win/loss, profit
  factor, total/CAGR return, Sharpe/Sortino, maximum drawdown, turnover and
  exposure time;
- return versus buy-and-hold and matched random-entry control;
- base and stressed costs, realised slippage, fill/rejection rate and unknown
  cost coverage;
- signal frequency, funded capture rate, concurrent-position demand and an
  estimated capacity warning;
- stability by year/regime/price/liquidity, dominance checks, DSR/trial count,
  promotion refusals and live kill state;
- allocated capital, currently reserved/invested capital, realised/unrealised
  strategy P&L and remaining sleeve cash.

Win rate is contextual metadata, never the ranking objective. A 35% hit-rate
strategy can dominate a 70% one if its payoff distribution and costs are better.
The primary comparison is lower-confidence-bound net expectancy alongside
drawdown and capacity.

A result missing the primary 2022+ and both rolling windows is visible as
`legacy/stress only` and cannot be allocated. A missing strategy result, as for
S-4 in the current database, is a displayed refusal rather than an omitted
picker row. The UI must not reinterpret the current 1962-2026 aggregate as a
recent result.

## Manual-position isolation — hard invariant

Ownership is a chain, not a label on an instrument:

```text
deployment -> fired signal -> strategy trade -> entry order
           -> order lookup positionExecution -> broker positionId
           -> stop changes / exact-position close -> exit order(s)
```

Rules:

1. Never infer ownership from instrument id, symbol, `positions.source`, open
   time, units or FIFO order.
2. A strategy can close or patch SL/TP only when the exact `positionId` has one
   active ownership record belonging to that strategy trade.
3. Manual positions, including manual orders placed through eBull, never acquire
   an ownership record and are therefore unmodifiable by strategy code.
4. Manual positions still count toward portfolio cash, instrument/sector
   concentration and total risk. Automation observes that exposure but does not
   own or mutate it.
5. eToro documents exact-position closes and detailed order lookup returning
   `positionExecutions[].positionId`. The demo probe must still open two small
   same-instrument positions through distinct flows and prove distinct ids survive
   sync. If the broker ever nets or returns an ambiguous one-to-many execution,
   the system enters `reconcile_required` and places no exit/ratchet. Until that
   probe passes, a strategy entry on an instrument with a manual position must
   fail closed.
6. A disappeared owned position is not silently marked successful. Reconcile it
   against order detail and trade history to distinguish broker SL/TP, manual
   close, rejected/pending state and missing data.

The existing `_load_position_id_for_exit()` FIFO lookup must never be called by a
strategy executor. It can remain for the legacy recommendation path until that
path receives its own ownership semantics.

## Execution state machine

One candidate progresses monotonically:

```text
observed -> rejected
observed -> approved -> intent_persisted -> submitted
submitted -> open | failed | reconcile_required
open -> closing -> closed | reconcile_required
```

Before an entry:

1. pin completed signal bar, feature snapshot and strategy/data versions;
2. recheck stage, global switches, quote freshness, session and halt state;
3. request current eligibility for the exact settlement/direction;
4. request current what-if costs for the exact proposed ticket;
5. recompute net EV and size from available **sleeve** capital;
6. apply portfolio-wide cash, exposure, concentration, drawdown and gap-risk
   limits, including manual holdings;
7. persist a durable intent before broker I/O;
8. place the order, reconcile order to exact position id, and verify broker SL/TP;
9. refuse further action while identity or asynchronous state is ambiguous.

For a stop ratchet, a completed causal bar proposes a new stop. The manager
checks `new_stop > current_stop` for a long (opposite for a future short), broker
eligibility, owned position id and current quote; persists an intent; sends the
v2 PATCH; then re-syncs before recording it as applied. Every actual change is an
event. Per-bar non-changes are metrics, not rows.

## Real-time process boundaries

- **Daily shadow scan (exists):** all registered daily strategies, no money.
- **Intraday bar closer (new):** closes bounded 30m/5m/1m bars; never trades a
  forming candle.
- **Signal evaluator (extend):** writes every fired candidate before allocation.
- **Allocation/execution gate (new):** consumes only enabled deployments and
  current preflight data. One invocation, one audit verdict.
- **Order reconciler (new, required before paper):** polls submitted/pending
  orders, resolves exact position ids and detects orphan broker effects after a
  crash.
- **Owned-position manager (new):** SL/TP verification, causal ratchets, timeout
  and strategy exit. It receives a trade id, never just an instrument id.
- **Portfolio sync (exists, extend):** observes all positions but preserves the
  separate durable ownership record.
- **Health/kill monitor (new):** data freshness, scan heartbeat, queue lag,
  reconciliation backlog, slippage/cost drift, drawdown and control-relative
  alpha. A breach stops new entries; emergency owned-position exits remain
  possible.

"Real time" is strategy-relative and measured: completed bar time, evaluation
time, preflight time, intent time, broker acceptance, fill and reconciliation
time are all retained. A daily strategy does not need tick-level decisions; a
five-minute strategy that finishes after the next bar is unhealthy.

## Bounded database shape

Do not add derived-indicator time series. After the capability/storage probes,
the smallest control-plane schema is:

| relation | grain | retention |
| --- | --- | --- |
| strategy promotions | one immutable stage change | durable; tens of rows |
| strategy deployments | one current allocation per strategy version/mode | durable config + audit history |
| candidate decisions | fired signal x allocation verdict | 24 months detailed; durable daily aggregates |
| strategy trades | one funded lifecycle | durable |
| strategy trade orders | one order linked to one owned trade and purpose | durable; joins existing orders/fills |
| order position executions | exact `positionExecutions[].positionId` returned for one strategy order | durable; typically one/few rows per entry |
| position ownership | one broker position id claimed by one trade | durable, including released ownership |
| stop changes | one requested/applied/failed material change | durable; no heartbeat rows |
| preflight observations | candidate request or changed eligibility state | 24 months; no unchanged polling rows |
| strategy daily metrics | strategy/version/day/stage arm | durable compact aggregate |

The #2452 position-manager schema measured **128 KiB including indexes when
empty** on PostgreSQL 17. It adds four narrow relations: registered variants,
one current policy, policy revisions, and material operations. A protected
position/bar poll writes zero rows; an actual stop change or close writes one
compact operation and repeated rejection of the same material edit is unique.
There is no tick, bar, eligibility or portfolio payload copy in these
relations. Each material operation retains at most the latest small broker
response object (and the linked close order retains its submission response),
so auditability does not create an append-only polling heap.

P&L is derived from owned trade order/fill links and broker close history; it is
not copied into the general `positions` aggregate and not duplicated on every
signal. The strategy page uses strategy-owned rows only. The main portfolio page
continues to show the complete account, including manual and automated positions.

The 2026-08-09 scan evaluated 34,698 logical rows; the old all-detail shape
projected **8.42 GB/year**. Slice 4 now stores only fired detail durably, retains
routine detail in 90-day monthly partitions, and keeps a durable daily census.
The one-time move proved zero aggregate/detail mismatches and zero outcome
dependencies on routine verdicts before moving 29,527 rows. Monitoring's scan
aggregate fell from 15.6 ms to 0.05 ms.

The accepted intraday candidate stores only completed OHLCV bars under the
fixed 30m/1,000/24m, 5m/250/12m and 1m/50/30d caps. Two earlier physical shapes
were rejected at 2.99 GB and 2.28 GB. The watermark + BRIN shape measured 117.5
bytes/row and **1.410 GB** at all caps, with an 8.7 ms representative read.
Re-run `scripts/verify_2437_observation_storage.py --benchmark`; details are in
`2026-08-09-strategy-observation-storage.md`. The shipped policy is:

- fired entry/exit signals and their outcomes: durable;
- individual not-fired/not-evaluable daily rows: 90 days after a checked daily
  aggregate exists;
- aggregate counts by strategy/version/date/verdict/reason: durable;
- intraday evaluations: write fired candidates plus aggregates, not one
  `not_fired` row per instrument per minute.

The deletion policy shipped only after the verifier proved aggregate/detail
equality and enumerated the one inbound dependency (`strategy_outcomes`), which
cannot reference a routine verdict through its writer. Recurring retention drops
partitions; it never issues mass row deletes.

## What "test every strategy or combination" means

Exhaustively searching every subset, threshold and signal combination is
impossible and would manufacture winners through multiple testing. The enforceable
contract is:

- every strategy implementation in the tree is in the manifest or CI fails;
- every **permitted, pre-registered** strategy/version/combination runs through
  the same test matrix and appears in the picker, including failures;
- combinations are explicit interaction hypotheses with an economic rationale,
  not the powerset of indicators;
- every attempted variant increments the trial register, including abandoned
  branches and manual inspection;
- selection happens on train/validation only; one frozen holdout is opened once;
- promotion uses recent-window stability, lower-bound net expectancy, costs,
  portfolio simulation, random controls and untouched forward evidence;
- paper/live performance is compared with the simultaneous unfunded shadow arm.

Property tests should exhaust finite state/vocabulary combinations (ownership
states, outcome classes, missing fields, side/settlement arms). Statistical
hypotheses use bounded registered trials, not combinatorial enumeration.

## Ordered implementation slices

These are the implemented #2437 child-ticket slices.

1. **Current v2 preflight adapters — implemented:** strict eligibility and
   what-if types, bounded request validation, raw response retention in memory,
   no writes and no execution use.
2. **Authenticated demo capability census — complete, execution cost blocked:**
   the deterministic equity/ETF population, long/short scaling arms, payload
   bytes, mismatch denominator and freshness are measured. Undocumented
   `value` semantics and mostly stale rows refuse cost consumption. Exact
   position-id cardinality and v1-versus-v2 execution compatibility move to the
   ownership/reconciliation slice, where an actual demo order exists.
3. **Recent-window result arms + read-only strategy observability — implemented:** compute the
   fixed 2022+, rolling 24/36-month and per-year arms without overwriting legacy
   evidence; then expose every manifest strategy, fired funded/unfunded signal,
   outcome, exclusion, scan health, pinned metrics and explicit refusal state.
   The runner accepts only a code-pinned window id and hashes its exact dates;
   the API/page are read-only, exact-current-version and keyset-paginated.
   Legacy-only rows are never allocatable. The primary 2022+ window now renders
   `complete` for S-1 through S-3, while the remaining registered windows render
   their measured `missing`/`partial` state and S-4 retains its explicit builder
   exclusion.
4. **Storage benchmark and retention — implemented:** actual temporary-table
   bytes/query plans rejected two oversized candidates; durable fired signals,
   90-day routine-detail partitions, daily aggregates, capped intraday bar
   partitions, monotonic watermarks, BRIN reads and whole-partition retention
   now enforce the measured 1.5 GB retained-tier budget.
5. **Promotion/deployment + ownership schema — implemented:** no broker writer.
   `sql/281` stores append-only ordered promotion events and pinned result ids,
   one current paper/live capital ceiling with a complete revision history, one
   funding decision per fired signal, strategy trade/order links, exact detailed
   order `positionExecutions[]`, and durable broker-position ownership. Existing orders default to `manual`; a
   manual order cannot be linked as strategy authority. The service accepts a
   `(strategy_trade_id, broker_position_id)` pair and never an instrument-only
   mutation target. A real-Postgres same-instrument test proves an unowned manual
   position remains inaccessible while the separately claimed strategy position
   can be released. Global auto/live switches are not read by promotion and
   cannot change stage. Broker I/O remains absent and live execution remains
   disabled.
6. **Order/position reconciler — implemented:** persist an immutable submission
   UUID before broker I/O, use it as both the documented v2 idempotency key and
   `orders:lookup.referenceId`, and retain exact position cardinality, partial
   units, average price, fees and execution time. Restart polling updates one
   compact state row per order; malformed/unknown/rejected-with-position shapes
   fail closed. An explicit reconciliation-age policy activates one bounded
   new-entry kill row rather than appending polling heartbeats. Real-Postgres
   crash-point tests cover before-call identity, during-call uncertainty,
   after-acceptance replay, partial fills and a same-instrument manual position
   that remains unclaimed. Detailed lookup follows the settled eToro carve-out
   in `docs/review-prevention-log.md` (#471): all decision-bearing identity,
   status, execution, units, price, time and fee fields land in compact SQL;
   the full body remains process-local and one SHA-256 fingerprint is retained
   per order. Repeated polls append no JSON. No broker writer or paper allocator
   is enabled.
7. **Paper allocator/executor — implemented:** exact strategy version, sleeve
   cash, current eligibility/cost/account-risk preflight, fixed SL/TP and
   portfolio guard. The writer has one hard-coded demo path and no live-key
   selection.
8. **Owned-position manager — implemented:** exact-position fixed-exit repair,
   timeout/strategy/emergency closes, and separately promoted ratchet variants.
   Every material PATCH/close intent is durable before broker I/O; PATCH is
   re-synced and close detail must name the owned position before application.
   An unowned id fails before I/O, same-instrument manual positions are never
   mutated, kill switches leave risk reduction available, and unchanged bars or
   polling heartbeats add no database rows. Live credentials are refused.
9. **Paper P&L and picker allocation controls — implemented:** strategy-only
   realised P&L is the sum of close-history net profit for exact owned broker
   position ids; unrealised P&L uses only active exact ownership and a positive
   live/daily mark. Missing lifecycle evidence is unknown, never zero, and
   same-instrument manual positions are structurally excluded. The picker shows
   allocation-unbiased shadow results, funded capture, fills/rejections,
   slippage, skipped-versus-funded comparison, sleeve usage and explicit
   refusal state. Operator/session-authenticated paper ceiling changes reuse
   the immutable deployment event ledger; evidence-invalid versions may only
   be disabled/reduced. The read model adds no schema or periodic P&L writer.
   Formula, storage and test register:
   `2026-08-09-strategy-paper-pnl-allocation.md`.
10. **Live promotion control — implemented, activation blocked by evidence:**
    one immutable policy is preregistered before paper observation. A read-only
    report measures forward/paper sample and duration, shadow-control alpha,
    actual-vs-stressed cost drift, slippage, drawdown, reconciliation, source
    freshness and all five kill drills. An explicit operator attempt stores one
    compact evidence hash/refusal record. Generic promotion cannot bypass this
    path; the strategy page states that real-money activation is unavailable.
    Pause disables paper/live allocations and retirement requires a prior pause
    plus zero active owned positions. The five-minute demo loop is bounded to 20
    uncertain orders, five owned positions and five new signals. Live remains
    unreachable until the broker cost and order contract is separately measured
    and validated. See `2026-08-09-strategy-live-promotion-runbook.md`.

## Acceptance tests before paper trading

- strategy/manual positions on the same instrument retain distinct ids through
  open, sync, partial close, SL/TP patch and strategy close;
- attempting to close or patch an unowned/manual id fails before broker I/O;
- a crash before call, during call and after broker acceptance reconciles without
  a duplicate order or orphan position;
- unknown/malformed/missing eligibility or cost fields refuse entry;
- current total broker costs are included once and stressed, never selected by a
  closed favourable vocabulary;
- manual holdings affect risk capacity but never strategy P&L or lifecycle;
- every fired signal appears with funded/rejected reason and later shadow outcome;
- picker rows cannot mix strategy, data, cost, resolver, universe or ambiguity
  versions;
- no picker row is allocatable unless its fixed 2022+ and rolling 24/36-month
  result arms exist and agree in sign after costs; pre-2000 data cannot rescue it;
- strategy P&L reconciles to owned fills/history and the account-wide portfolio
  reconciles separately to the broker;
- retention preserves fired outcomes and daily census equality while staying
  inside the declared database growth budget;
- kill switch, stale data, scan lag, reconciliation backlog and drawdown tests
  block new entries while leaving audited owned-position risk reduction usable.
