# Strategy paper operation and live-promotion runbook

Date: 2026-08-09
Status: #2450 implemented; paper lifecycle operational, live activation refused
Parent: #2437

## What the MVP can do

The unattended scheduler runs every five minutes against eToro **demo** only. It
reconciles uncertain submissions, refreshes current health blocks, repairs or
reduces risk on exact strategy-owned positions, then considers the newest fired
signals that have an enabled paper deployment and explicit execution policy.
Manual positions affect account risk but cannot acquire strategy ownership or be
patched/closed by this path.

The cycle is deliberately bounded to 20 reconciliation lookups, five owned
positions and five new signals. The owned-position batch rotates by five-minute
slot so a larger sleeve cannot starve later positions. Repeated health observations update five rows in
place (`order_reconciliation`, `scan_freshness`, `quote_freshness`,
`broker_availability`, `drawdown`). Unchanged position polling writes no row.

No current strategy is suitable for real-money activation. The API and picker
surface the stable refusal `live_strategy_broker_contract_not_validated`; setting
the global live flag cannot bypass it.

## Evidence policy and formulas

There is no universal defensible value for minimum sample size, observation
duration, drawdown or capital. Each strategy/version must therefore register
one immutable policy while it is in `forward_observation`, before paper results
exist. Values have no defaults and cannot be tuned after seeing the paper arm.

The report evaluates:

- `shadow alpha = mean funded shadow return - mean unfunded shadow return`;
- `entry slippage % = mean((actual entry price - signal fill price) / signal fill price * 100)`;
- `cost drift % = (actual close fees - stressed expected cost) / closed allocated capital * 100`;
- `drawdown % = (account equity high-water - equity) / high-water * 100`;
- resolved signal/trade counts and elapsed days measured only after the relevant
  promotion timestamps, so old or backfilled bars cannot create duration;
- reconciliation breaches and scan, quote, halt-feed and broker-health age;
- complete exact-owned paper P&L and successful quote-lag, scan-lag,
  broker-outage, reconciliation-backlog and drawdown drills;
- requested capital at or below the preregistered USD cap, with leverage fixed
  by schema to x1; and
- global auto/live switches, kill state and all current execution blocks.

The final live-contract refusal remains unconditional until a separate versioned
broker activation change proves the real cost vocabulary, idempotent write,
lookup, exact-position ownership, SL/TP and rollback path. A passing metric never
promotes automatically.

## Operator sequence

1. Promote a version through historical validation to forward observation using
   pinned recent evidence. Existing S-1 through S-4 results are not promotable.
2. During forward observation, register the complete immutable live-gate policy
   with `POST /strategies/{id}/live-gate/policy`.
3. After untouched forward evidence, explicitly promote to paper, configure an
   execution policy and a bounded paper capital ceiling, then enable that paper
   deployment. Keep eToro environment set to demo.
4. Monitor the strategy page for every fired signal, funded/rejected status,
   simultaneous shadow result, exact-owned P&L, sleeve usage and health blocks.
5. Run all five drills with
   `POST /strategies/{id}/live-gate/drills/{kind}`. A drill commits a visible
   entry block, proves the gate observes it, restores prior state and records one
   material event. Integration tests separately prove risk-reducing exact closes
   remain available while global and per-strategy entry blocks are active.
6. Inspect `GET /strategies/{id}/live-gate?requested_capital=...`. An explicit
   `POST /strategies/{id}/live-promotion-attempt` records a compact refusal and
   evidence SHA-256, including when the policy itself is missing; it does not
   place an order or create live authority.

## Pause, emergency action and retirement

- Pause with `POST /strategies/{id}/lifecycle` action `pause`. It atomically
  disables every enabled deployment for that version and appends the stage
  event. Existing owned positions remain manageable for risk reduction.
- For an emergency, activate the existing global kill switch. New entries stop;
  exact-owned protection/close operations remain usable.
- Retire with lifecycle action `retire` only after pause and after every exact
  owned position has closed. Retirement is a separate audited event and cannot
  be reversed for the same immutable version.
- If evidence/code/rules drift, pause the old version and register a new version
  from research; never amend its old thresholds or evidence.

## Database budget

`sql/290_strategy_live_promotion_gate.sql` adds one policy row per immutable
strategy version, one row per material drill, and one row per explicit promotion
attempt. `sql/291_strategy_live_attempt_audit_identity.sql` makes a policy-less
refusal identifiable and auditable in that same narrow ledger. It adds no tick,
bar, indicator, health-heartbeat or periodic P&L table.
The recurring loop changes five keyed health rows, one account high-water row
and one paper-period high-water/maximum-drawdown row per deployment. It uses the existing one-row
per fired signal/order/material mutation ledgers. Its only new indexes support
one policy lookup and latest drill/assessment history; none amplify market-data
writes.

Measured on PostgreSQL 17 after migration, all five empty relations plus their
indexes total **104 KiB** (the policy and drill relations are 24 KiB each, the
live-attempt audit relation is 32 KiB,
the per-deployment risk-state relation is 8 KiB and the shared paper-pool event
ledger is 16 KiB). It adds one bounded current row per
configured paper sleeve. This is fixed catalogue overhead; growth
is driven only by human policy/drill/promotion actions, not the five-minute loop.

Retention remains as defined by #2448: routine daily detail is partition-dropped
after 90 days once census equality is proved; fired signals/outcomes and material
trade authority remain durable. Re-measure relation growth and query plans before
adding another index or retaining broker payload polling.

## Evidence basis and activation boundary

- [SEC market-access controls](https://www.sec.gov/rules-regulations/staff-guidance/trading-markets-frequently-asked-questions/divisionsmarketregfaq-0)
  require controls before orders enter the market; the strategy gate is a
  second, narrower authority layer rather than a replacement for account-wide
  controls.
- [MiFID II Article 17](https://www.esma.europa.eu/publications-and-data/interactive-single-rulebook/mifid-ii/article-17-algorithmic-trading)
  requires effective systems, thresholds, limits, monitoring and continuity;
  [ESMA's reconciliation guidance](https://www.esma.europa.eu/publications-data/questions-answers/1608)
  supports the explicit reconciliation SLO and kill drills.
- Bailey and López de Prado's
  [Deflated Sharpe Ratio](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2460551)
  motivates accounting for multiple trials and sample length rather than
  selecting a recent winner by headline Sharpe.
- The current [eToro real-order contract](https://api-portal.etoro.com/api-reference/trading--real/create-an-order)
  documents a distinct live endpoint, request idempotency and SL/TP support.
  Endpoint existence is not evidence that this repository's observed costs or
  execution path are safe; the measured cost-contract mismatch keeps it blocked.

This is a controlled path toward live operation, not a claim of winning trades.
Promotion requires positive, stable, cost-aware evidence and operational health;
losses remain possible after every gate passes.
