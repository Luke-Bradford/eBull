# Core/cash uses the assigned pot, never the broker account (#2525, #2603)

Status: implementation proposal. This corrects the denominator deliberately deferred by
`2026-08-13-core-cash-mandate.md`; it does not change the mandate percentages or select an
instrument.

## Defect and invariant

The attended core executor currently gives `observe_core_sleeve` the selected instrument's
**whole-account** direct-long market value and the broker account's **whole-account**
available cash. A manual holding of the same instrument is silently claimed as core, and a
small assigned pot can size against cash outside that pot. That violates the operator's
settled 2026-08-22 decision: the only capital safety boundary is
`strategy_paper_pool_events.capital_limit` (`fixed` = capped, `compound` = expanding), and
the broker account is shared with non-engine holdings.

The invariant after this change is:

```text
engine committed capital = open/unresolved alpha authority
                         + open/unresolved core entry authority

effective assigned bound = sandbox_bound(
    paper_pool.capital_limit,
    paper_pool.capital_mode,
    exact-owned realised engine P&L,
)

assigned cash available to core = min(
    broker available cash,
    effective assigned bound - engine committed capital,
)

core/cash sleeve = exact-owned selected-core market value
                 + assigned cash available to core
```

Every term fails closed when its identity or completeness is unavailable. Manual positions
have no `strategy_position_ownership` row and therefore contribute zero to the core sleeve.
The whole-account broker values remain independent risk/cash ceilings; they never become
allocation authority.

## Source rule

The current eToro portal page, **Get account PnL and portfolio details**, fetched
2026-08-24, documents that `GET /api/v1/trading/info/demo/pnl` returns the current
portfolio and shows every direct `clientPortfolio.positions[]` row carrying `positionId`,
`instrumentId`, `isBuy`, `amount`, and P&L. The current calculation guides separately
establish the account formulas already implemented by `_parse_account_risk_snapshot`.

Live read-only verification on the configured demo account at 2026-08-24 found every row in
that account's complete returned direct-position array (7/7) had a positive unique exact ID,
instrument ID, boolean direction, amount, and `unrealizedPnL.pnL`. This is evidence for the
configured account, not a provider-wide guarantee; required-field parsing is what makes any
other account/shape fail closed. The endpoint exposes no pagination or continuation member,
so the returned array is the only completeness boundary the source makes available. The
response used legacy spellings
`positionID`, `instrumentID`, and `settlementTypeID`, while the portal example uses lower
camel case. The parser therefore accepts exactly those two observed/documented spellings,
requires a positive unique position ID, and never falls back to instrument identity. If both
spellings are present they must decode to the same value or the response is refused.

The per-position market-value construction is unchanged from #2704:
`amount + unrealizedPnL.pnL`. The portal's equity and total-invested guides establish those
as the direct position's two additive contributions to account equity but do not name the
restricted per-position sum “market value”; #2704 therefore labels that interpretation as
measurement-backed, not published. It was independently cross-checked against the quote feed
on every position returned by this configured demo account (7/7) on 2026-08-14. That is the
full observed population, not a claim about every provider account. This proposal changes
which exact rows are included, not that measured valuation rule; an unobserved shape refuses.

No published source governs an application's virtual capital carve-out. The assigned-pot
arithmetic is therefore fixed by the operator's settled invariant and the existing
`strategy_capital_sandbox.sandbox_bound`; no new threshold, mode, or capital column is
introduced.

## Provider contract

Add an immutable `BrokerDirectPositionInvestment` to `BrokerAccountRiskSnapshot`:

```python
position_id: int
instrument_id: int
is_buy: bool
amount: Decimal
unrealized_pnl: Decimal
market_value: Decimal
is_partially_altered: bool
```

The P&L parser emits one row per direct position while retaining the existing aggregate
instrument rows. Exact IDs must be JSON integers (booleans and lossy numeric coercion are
refused), positive, unique, and within signed-BIGINT range. `amount` and P&L must be finite,
and `amount >= 0`; a negative derived market value is carried so the allocator can report its
existing `sleeve_valuation_invalid` refusal rather than misclassifying valid-but-extreme
broker state as response drift. `isPartiallyAltered`/`is_partially_altered` is required and
boolean because v1 does not have a sourced residual-cost-basis rule for externally partial
core positions. The typed source terms make the derived value auditable without widening raw
payload persistence.

## Shared capital observation

Add one service-owned database read for the shared sandbox rather than copying SQL into the
core executor, alpha executor, withdrawal guard, and overview.

- Pool membership is one continuous epoch beginning at the first paper-pool event's
  `changed_at`. It includes every funding/trade authority created at or after that instant:
  every paper deployment (disabled and retired versions included) and every core trade whose
  cited mandate mode is `paper`, regardless of later pool/mandate revisions. Closed pre-epoch
  history is excluded from the assigned pot; any pre-epoch non-terminal strategy lifecycle
  makes the observation unavailable rather than being silently adopted or ignored. Disabling
  pauses entries; it does not reset P&L or free exposure. Principal revisions are external
  flows, not epoch boundaries.
- Alpha committed capital remains positive finite allocated
  `strategy_funding_decisions.amount` for a paper deployment whose trade is absent or whose
  authority/ownership/reconciliation state is non-terminal.
- Core committed capital has two disjoint sources. Before entry reconciliation is terminal it
  is the positive finite entry order `requested_amount`. After a filled entry has exact active
  ownership it is the sum of the current P&L snapshot's per-position `amount` across every
  owned execution. This authoritative remaining cost basis handles partial fills and releases
  principal after a broker-side partial close; because v1 refuses an externally partially
  altered core position, that release cannot silently fund another order until the state is
  reconciled/closed. `planned`, submitted-unreconciled and `reconcile_required` therefore
  cannot reopen headroom. A terminal trade with unresolved entry reconciliation or active
  ownership is inconsistent and makes the observation unavailable.
- The entry must be one `BUY MARKET` order with `execution_origin='strategy'`, the same
  instrument as its trade, one `purpose='entry'` link, and a reconciliation row. Missing,
  duplicate, null, malformed, or cross-linked authority is incomplete, never zero. Queries
  aggregate each authority in its own CTE before summing, so later one-to-many joins cannot
  multiply money.
- The existing core gate allows multiple resolved open core trades/lots, but at most one
  **unresolved entry**: `core_trade_in_flight` is produced only when a non-terminal trade has
  a missing/non-terminal entry reconciliation row. Therefore a submitted-but-unowned core
  order is included in commitment and categorically blocks another evaluation; resolved open
  lots are all included in exact ownership and may be rebalanced with another entry.
- Pool realised P&L reuses #2602's existing exact-owned F-0 rule over both arms: every close
  slice for an owned position is included, `SUM` is accompanied by `BOOL_AND` non-null
  completeness, and released ownership without reconciled close history is unavailable.
  Active-position partial-close rows are subject to the same non-null check. The existing
  trade ledger defines `realized_pnl_usd` as the broker's realised P&L and keeps fees
  separately; until #2602 proves whether that field is net of every cost, the sandbox uses
  the same reconciled value already used today and the limitation remains named. No new,
  more optimistic P&L interpretation is introduced here. Fixed mode includes losses and
  excludes profits; compound mode includes both through `sandbox_bound`.
- With no active core ownership, page `committed` is the same combined authority total
  execution uses and `available = max(0, bound - committed)`. While core ownership is active,
  the database can identify the exact broker position IDs but cannot value their remaining
  cost basis without a live snapshot. The page therefore marks the capital observation
  incomplete and renders `working`, `committed`, and `available` unavailable rather than
  presenting the original request as current commitment. The database-only core status
  endpoint does not advertise rebalance readiness in that state; scheduled or explicitly
  invoked execution obtains the exact-position snapshot and applies the authoritative bound
  before it can submit. These are deliberately not claims to be simultaneous broker
  valuations; live value/P&L remains on the exact-position and EOD wealth surfaces.

The existing per-deployment alpha realised/commitment calculations remain for deployment
ceilings. Only the shared pool figures widen to both authorised arms.

## Core observation and submission

`observe_core_sleeve` receives the active exact core position IDs and an explicitly derived
`available_core_cash`; it no longer reads whole-account aggregate instrument value or treats
broker cash as the sleeve cash. The existing account-currency, timezone, freshness/future
timestamp, and P&L-pending-order checks remain in their current observer/preflight layers;
the pool and snapshot must both resolve to USD before money is compared.

It refuses when:

- an active owned ID is absent from the broker snapshot;
- an owned row belongs to another instrument;
- an owned row is short;
- an owned row is externally partially altered;
- duplicate ownership IDs are supplied;
- derived assigned cash is negative or otherwise invalid.

An empty ownership set is a genuine empty core sleeve only when the core submission gate also
proves there is no non-terminal core trade. It remains empty even if the operator manually
owns the selected instrument.

Before any core decision, the executor requires a configured, enabled USD paper pool and a
complete sandbox observation. The core critical section acquires the existing shared paper
allocator lock before its mandate/submission locks. This serialises core submission against
alpha reservation and paper-pool revision. Under the lock it re-reads the pool event,
ownership and commitments, obtains the one broker snapshot, derives the sleeve, and later
re-proves the same pool revision before durable authority is committed. Exact-position close
and ownership-release paths take the shared allocator lock too. The final transaction locks
the relevant trade/order/ownership rows and compares a deterministic fingerprint of pool
event ID, mandate event ID, active core ownership IDs, non-terminal core trade/order IDs,
combined committed amount, and realised-P&L total. A changed fingerprint refuses; a new or
released position cannot slip through the network interval.

The broker preflight's second P&L snapshot is filtered through the same frozen exact-owned ID
set and assigned-cash ceiling. A moved market value can change the trade verdict as today;
a changed DB authority or pool revision refuses before submission. The final requested amount
must be no greater than both assigned headroom and broker available cash. The measured eToro
cost is price-embedded: the requested cash amount already includes it and the sizing solver
reduces acquired core value accordingly, so adding cost a second time to commitment would
double-charge it. Manual account activity after the final snapshot remains a broker race the
API cannot make atomic; rejection/uncertainty follows the existing durable reconciliation
path and never retries as a new order.

The kill switch, demo-only guard, eligibility proof, account credential binding, submission
UUID, reconciliation-only resume, and buy-only limitation remain mandatory and unchanged.
Resume reloads the original request UUID, order ID and amount, performs lookup/reconciliation
only, and cannot re-price, insert an entry order, or run after its reconciliation becomes
terminal. Resume never creates a new capital commitment.

The execution arm remains buy-only because eToro's close-cost quote requires exact position
IDs and one cash amount may span several lots; no sourced lot-allocation rule exists yet.
An overweight holding returns `core_close_side_cost_quote_unavailable`, remains visible, and
can be fully closed through the existing exact-owned operator control. This is a disclosed
v1 limitation, not a claim that both mandate directions are automated.

## Control plane and page

Any principal or mode change whose resulting effective bound is below **combined** alpha and
core commitment refuses. Disabling is always allowed because it removes entry authority and
does not release or resize holdings. Market appreciation may make current market value exceed
the original committed cash; that is investment return, not newly committed capital, and does
not force a sale. The boundary limits additional cash authority, while the page separately
shows live market value and P&L.

The `/strategies` paper-pool view reports combined committed, working, and remaining capital
when those figures are complete. With active core ownership it reports the observation as
incomplete and withholds those three figures, so the visible `Pot`, `Working`, `Committed`,
and `Available` surface never overstates executable headroom. The core card names `paper_pool_unconfigured`,
`paper_pool_disabled`, `sandbox_observation_incomplete`, or `sandbox_exceeded` as blockers and
does not offer rebalance while any applies.

The existing form remains the sole place to assign capital and choose fixed/compound mode;
the core mandate form continues to express only percentages and rebalance policy. This avoids
a fourth capital surface.

## Tests and acceptance

Provider tests cover documented lower-camel IDs, live legacy-uppercase IDs, absent/malformed/
duplicate IDs, and preservation of aggregate account formulas.

Service/DB/API tests prove:

1. a manual same-instrument position contributes zero to core market value;
2. two exact-owned core lots sum and a missing/wrong-instrument/short owned lot refuses;
3. a pending core order consumes headroom before it appears at the broker;
4. an open core holding and an alpha allocation cannot together exceed the assigned bound;
5. fixed and compound modes use combined exact-owned realised P&L correctly;
6. lowering principal below combined commitment refuses;
7. core and alpha submissions serialize on the shared allocator lock;
8. a concurrent pool revision is detected before core authority is committed;
9. broker cash below assigned headroom caps the sleeve; broker cash above it cannot enlarge it;
10. the overview and page report the same bound/commitment/headroom used by execution;
11. unconfigured, disabled, incomplete, exhausted, kill-switch, real-environment, and uncertain
    submission paths remain non-mutating;
12. the existing operator close path can close an exact core position and the released close
    P&L becomes available only after authoritative history reconciliation and subsequently
    changes only the mode-permitted bound;
13. repeated calls while a core entry is pending cannot pyramid, lifecycle inconsistencies
    refuse, and external partial alteration refuses;
14. USD mismatch, costs near headroom, mandate-instrument revision, and concurrent
    ownership/reconciliation mutation all fail in the conservative direction.

After local semantic tests and full gates, checkpoint 2 reviews the staged branch before its
first push. Demo mutation remains prohibited until the PR is approved, green, merged, and the
operator is present for the lifecycle.
