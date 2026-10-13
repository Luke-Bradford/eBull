# The account's broker trades as UK tax evidence (#3616 slice 3c)

Source: #3616 slice 3, split on 2026-10-13 into 3a (matcher, #3789), 3b (tax FX, #3790/#3793), 3c (this) and 3d. The
routing of the slice 3 checkpoint-1 round 1 findings is on #3616 (2026-10-13 13:46Z); "Finding routing" at the end maps
the findings this slice owns to its sections.

**What this slice does.** It turns the account's broker record into tax evidence: one acquisition per order execution
and one disposal per broker close piece, each with its account, environment, product, units, US-dollar amount and
sterling value at the 3b rate. Each run rebuilds the whole set from evidence and publishes it as one generation.

**It admits only shapes it has verified.** Every order ends in one of three outcomes:
- `admitted`: a long, unleveraged, uncopied real-asset order with one execution (which a partial close may split into
  several positions), every charge indicator zero, and every close piece priced in US dollars;
- `excluded_non_share_product`: the order is established as a CFD or another documented non-share product;
- `unresolved`, with every reason: anything else, including any shape not yet verified on observed data.

**What it does not do.** It changes no published tax figure. Matching, security identity across listings, tax
character, excess reportable income, the tax-year view and the page are slice 3d. Until 3d, nothing reads the
generations except tests and the acceptance queries. This slice also retires the `fills` route into `tax_lots` and the
unused `fx_rates` table.

## Facts established first

"Script" is `scripts/measure_3616_slice3c_order_lookup.py`, run on 2026-10-13 against the dev DB and the demo account.
- The default mode makes one `orders:lookup` GET per stored order and saves the raw bodies to the gitignored
  `var/research/3616/slice3c/order_lookup_raw.json`, because they carry account identifiers and the repo is public.
  Its printed summary is `var/research/3616/slice3c_lookup_summary.txt`.
- `--checks` recomputes the per-order checks from the saved bodies and the DB, with no network
  (`var/research/3616/slice3c_checks.txt`).
- `--facts` prints the DB and lookup-shape facts as counts and dates, never an identifier
  (`var/research/3616/slice3c_facts.txt`).

Every count below is from the stored population on 2026-10-13: 21 orders, 36 events. That is not the account's complete
history (Limits).

1. **The stored record** (`--facts`): 16 `etoro_sync/open`, 7 `etoro_history/open`, 13 `etoro_history/close`. The
   `order_id` column is filled on all 20 history rows and on none of the 16 sync rows, which carry it in
   `raw_payload->>'orderID'`.
2. **The order lookup answered for recent orders only** (`--facts`). 13 orders returned HTTP 200, opened 2026-09-16 to
   2026-10-09. 8 returned 404 with `"Order category for <id> not found"`, opened 2025-08-12 to 2025-11-14. No stored
   order was opened between those ranges. Age-based retention, with a boundary somewhere from 27 to 333 days on this
   evidence, is a hypothesis; the portal page states no retention. Early capture is therefore precautionary policy.
3. **The shape of the 13 found lookups** (`--facts`, "lookup shape"): every one has `status.name` `Filled`,
   `transaction` `buy`, `action` `open`, `asset.side` `long`, `asset.leverage` 1 and `orderCurrency` `usd`.
   `asset.settlementType` is `REAL` on 11 and `CFD` on 2. `asset.currency` is `USD` on 9 (7 real, 2 CFD), `GBP` on 3
   (CSPX.L) and `GBX` on 1 (IUSA.L). They hold 15 executions, and every execution's `openingData.orderId` equals the
   order id. On every body, `totalCosts` equals the execution's `fees + taxes`. The page documents `settlementType`
   values `cfd`, `real`, `realFutures` and `marginTrade`.
4. **`instruments.currency` is not pricing-currency evidence** (`--facts`). For the 3 CSPX.L orders it says `USD` where
   the lookup says `GBP`; it agrees on the other 10. This slice takes pricing currency from the lookup only.
5. **A partial close splits a position, and the lookup lists the pieces with one shared opening** (summary). IUSA.L
   order 383293149 and AAPL order 383320185 each return two executions whose `openingData` objects are identical, each
   carrying the whole order's units (0.646048 and 0.147071). Summing `openingData.units` over executions double-counts.
   The history rows of the pieces carry `parentPositionId` 0, so the order id is the only stored lineage. The other 11
   found orders have one execution. ⚠ The sql/194 and `app/services/trade_events.py` docstrings say a partial close
   reduces the same `positionId`; on this account it mints a new id. The build corrects the Python docstring; the
   applied migration's comment stays, with a pointer from the Python one.
6. **Each execution's state binds to the stored record** (`--checks`, `position_state_binding`, all 13 found orders).
   An execution in state `closed` has no held position, and the stored close rows of its position sum to its
   `remainingUnits`; an execution in any other state has a held position whose units equal its `remainingUnits`.
7. **Units and money are conserved per order** (`--checks`, all 20 orders whose acquired units are known: the lookup's
   one distinct opening, or the sync `initialUnits` of a one-position order). Acquired units equal closed units plus the
   units `broker_positions` holds under the order's positions, exactly. The stored initial investment (sync
   `initialAmountInDollars`, history `initialInvestment`) has one value per order, and equals Σ close rows' `investment`
   plus Σ held positions' `amount`, exactly. All 10 real-id `broker_positions` rows appear in `trade_events`, and all 10
   have `amount = initial_amount_in_dollars` and leverage 1 (`--facts`). ILMN (order 272136682) has neither a lookup nor
   a sync observation.
8. **The broker's investment against the execution record** (`--checks`). For the 20 orders, the stored initial
   investment differs from `units × open rate × open conversion rate` by a relative gap printed in full per order; the
   largest is 0.0000936852 (AAPL). This corroborates the amount; it is not an identity (Source rules).
   - `frozenAmount` is not a reconciliation. The page documents it as "the amount frozen/reserved for the order
     including costs", which is the request, not the execution.
9. **Charges** (`--checks`, every indicator printed separately per order). The PnL page documents `totalExternalFees`
   as *"Total fees in USD charged on the position. Example - TicketFee. This value does not include overnight fees and
   dividends"*, `totalExternalTaxes` as *"Total taxes in USD charged on the position. Example - SDRT"* and `totalFees`
   as *"Total overnight fees and dividends charged/paid on the position"*. The lookup documents `openingData.fees` as
   *"Fees charged for the execution"*, `taxes` as *"Taxes applied to the execution"* and `totalCosts` as the order's
   total costs. On the 14 real-asset orders with a lookup or a sync observation, every one of these indicators that is
   present is zero; the two non-zero cases are CFDs (SVXY 1.5, SVOL 0.72, equal across lookup `fees`, `totalCosts` and
   sync `totalExternalFees`). The history page documents close `fees` as "the fees of the trade"; it is 0 on all 13
   close rows (`totals.close_fees_zero`).
10. **Disposal proceeds of USD-priced pieces** (`--checks`). For the 3 close pieces of orders whose lookup
    `asset.currency` is `USD` and whose product is real (SPY.RTH ×1, AAPL ×2), `investment + netProfit` differs from
    `units × closeRate` by at most a relative 0.0002339584; the 2 CFD pieces (SVXY, SVOL) are within 0.0000096. The
    history row carries no close conversion rate: 0 history payload keys contain "conversion"
    (`--checks`), and the PnL page's `closeConversionRate` describes an open position's current valuation, not an
    executed close. The daily account snapshots hold one row per day (48 rows, 48 dates; `--facts`), so a close's cash
    credit cannot be isolated from other same-day movements.
11. **One account in the trading record, and the private stream reports another** (`--facts`). The 16 sync rows carry
    one `CID`, none NULL; the 13 found lookups carry one `accountId`, none missing, equal to it. The 5 private-stream
    `Trading.*` events in `broker_private_events` carry one other `CID`, and none carries the sync's. The environment
    label therefore does not identify the account.
12. **Every broker credential this database has used is labelled demo** (`--facts`). `broker_credentials` holds two
    rows, `api_key` and `user_key`, both `demo`. `broker_credential_access_log` holds rows from 2026-06-03, all joined
    to a demo credential, with 0 NULL credentials under an `ON DELETE SET NULL` foreign key. `trade_events.recorded_at`
    starts 2026-06-13.
13. **Settlement type, where both sources speak, agrees** (summary). For the 9 orders with both a sync
    `settlementTypeID` and a lookup `settlementType`, `1` pairs with `REAL` and `0` with `CFD`. Orders by product: 20
    established (14 real, 6 CFD), 1 unknown (ILMN).
14. **Copy and parent ids** (`--checks`, `totals.history_social_parent_zero`): all 20 history rows have `socialTradeId`
    0 and `parentPositionId` 0.
15. **Field spellings differ between documentation renderings.** The stored sync payloads use `CID`, `orderID`,
    `settlementTypeID`, `mirrorID` and `parentPositionID`; one rendering of the PnL page shows those spellings and
    another shows `cid`, `orderId`, `settlementTypeId`, `mirrorId` and `parentPositionId`. History rows use `orderId`,
    `positionId` and `parentPositionId`.
16. **`trade_events` keeps the first observation of a row** (`trade_events.ingest_trade_events`). One open per position
    whatever its source, and one close per `(position_id, executed_at)` (`sql/194_trade_events.sql`, the two unique
    indexes). A later observation that disagrees on units or realised P&L is counted as `conflict_anomaly` and logged,
    not stored; other fields are not compared. The history fetch re-walks a 7-day overlap behind its watermark
    (`_WATERMARK_OVERLAP`); the watermark is global (`compute_history_min_date`). A failed history fetch is passed on as
    `None` (`fetch_trade_history_safely`) and stored nowhere. `sync_portfolio` writes `broker_positions` and then calls
    `record_trade_events` in one transaction that its caller commits.
17. **Stored payloads were decoded as binary floats.** The provider decodes with `response.json()` before storing JSONB,
    so a stored number is the shortest decimal that round-trips its double. A decimal of 15 or fewer significant digits
    survives that exactly; one with more may not.
18. **`tax_lots` and `fx_rates` hold 0 rows; `fills` holds 1** (`--facts`), whose order's `broker_order_ref` starts
    `DEMO-`. `grep -rnE 'INTO fx_rates\b' app scripts sql` finds no writer (the one `INTO fx_rates` match is
    `fx_rates_daily`); the one reader is `tax_ledger._load_fx_rate`, called only by `_ingest_fills`.
    `daily_tax_reconciliation` is on-demand only (the comment above the on-demand list in `scheduler.SCHEDULED_JOBS`).

## Source rules

- **Acquisition cost.** TCGA 1992 s38(1)(a): *"the amount or value of the consideration, in money or money's worth,
  given by him … wholly and exclusively for the acquisition of the asset, together with the incidental costs to him of
  the acquisition"*; s38(2) defines incidental costs (broker fees, transfer costs, stamp duty; HMRC CG15250).
- **Disposal consideration.** TCGA s15 and s38(1)(c): the amount received for the disposal, less the costs of disposal.
- **Where the amounts come from.** For a retail account the broker is the only party that records what the account paid
  and received; its trade record is the evidence of consideration. This slice takes the broker's stated amounts: the
  order's initial investment (the USD the account committed) and, per close piece, `investment + netProfit` (the USD
  returned). It admits them only when (a) every documented charge indicator is zero, so no incidental cost needs
  classifying, (b) the execution record corroborates them (Design 4.6), and (c) the position ledger conserves them
  (Design 4.6). No eToro page documents these fields as an accounting identity; that they are the consideration is
  local policy, stated as such, and an adjustment the broker applied without changing any of these fields is not
  detectable (Limits).
- **Conversion to sterling.** HMRC CG78310, applied by 3b: each USD amount is converted at the rate for the date it is
  given or received, from `resolve_tax_fx` under 3b's consumer contract (3b spec, Design 3).
- **The unit of acquisition.** Share identification acts on acquisitions and disposals of securities of the same class
  by the same person in the same capacity (TCGA s104, s105, s106A; HMRC CG51555–CG51575). A position record is not an
  acquisition: one execution split into positions by a partial close is one acquisition (fact 5). Same-day
  aggregation and security identity are matching, in 3d.

## Design

### 1. Storage (migration `sql/485_tax_broker_evidence.sql`)

The migration first raises, naming the table, unless `tax_lots` and `fx_rates` are both empty (fact 18). It then:
- drops `fx_rates` (Design 8);
- adds `trade_events.environment`, backfills it and constrains it (Design 2);
- creates `trade_event_observation_conflicts` and `trade_history_fetches` (Design 2);
- creates the tables below.

**`tax_evidence_scope`.** One row, `(environment, account_id, established_at, established_by_run_id)`, written by the
first run that publishes (Design 6) and changed only by a reviewed migration.

**`tax_order_lookups`.** Append-only, one row per lookup attempt:
- `lookup_id` identity, `environment`, `order_id`, `requested_at`, `policy_id` (Design 4.9);
- `outcome`: `found`, `not_found`, `rate_limited`, `auth_failed`, `http_error`, `transport_error` or `malformed`;
- `http_status` (NULL for a transport error), `error_detail`;
- `body_text TEXT` and `body_sha256` (NULL when no body arrived), `account_id` (the body's `accountId` when `found`);
- `order_event_max_id`, the order's largest `trade_events.event_id` when the attempt was made.

**`tax_evidence_runs`.** One row per run, created at entry (Design 6):
- `run_id`, `environment`, `account_id` (NULL until established), `started_at`, `finished_at`;
- `state`: `in_progress`, `published`, `refused` or `abandoned`; `reason`;
- watermarks of what it read: `trade_events_max_event_id`, `lookups_max_id`, `conflicts_max_id`;
- `history_fetch_latest_id` (the latest `trade_history_fetches` row, any outcome) and `history_fetch_ok_id` (the latest
  `ok` one);
- `positions_observed_at`, the `fetched_at` of `history_fetch_latest_id`: the fetch row is written in the same
  transaction as the `broker_positions` it describes (fact 16), so it dates the portfolio observation the run read;
- `fx_run_id`, `fx_policy_id`, `evidence_policy_id`, `counts JSONB` per outcome, `lookups_deferred`.

**`tax_evidence_subjects`.** One row per subject per published run. A subject is an order (`order:<order_id>`) or, when
no order id can be established, a position (`position:<position_id>`). All events of one position with no order id form
one position subject.
- `run_id`, `subject`; `order_id` (NULL for a position subject);
- `etoro_instrument_id` (the broker's id; NULL when the subject's events disagree or none carries one) and
  `instrument_id` (NULL when unresolved by the app);
- `product`: `real`, `cfd`, `real_futures`, `margin_trade`, `swap`, `crypto_margin` or NULL when not established;
- `outcome`: `admitted`, `excluded_non_share_product` or `unresolved`; `reasons TEXT[]` (every failing check, in Design
  4 order; empty when admitted);
- `trade_event_ids BIGINT[]` (sorted; empty for an orphan), `lookup_id` (the *F* used, or NULL);
- `held_positions JSONB`: position id → `{units, amount, cid}` read from `broker_positions`, for every position of the
  subject; a position with no `broker_positions` row maps to `null`;
- `first_event_at`, `last_event_at` (NULL for an orphan).
- Primary key `(run_id, subject)`.

**`tax_evidence_lots`.** One row per admitted acquisition or disposal per published run:
- `run_id`, `lot_key` (Design 4.7), `subject`, `order_id`, `instrument_id`, `etoro_instrument_id`, `direction`;
- `position_ids BIGINT[]`, `trade_event_ids BIGINT[]` (both sorted);
- `executed_at`, `uk_date`, `tax_year`;
- `units NUMERIC(20,8)`, `usd_amount NUMERIC(20,4)`;
- the FX fields of 3b's consumer contract: `fx_rate_date`, `fx_status`, `fx_observation_date`, `fx_usd_per_gbp`,
  `fx_gbp_per_usd NUMERIC(20,10)`, `fx_policy_id`, `fx_run_id`;
- `amount_gbp NUMERIC(40,14)`, the exact product `usd_amount × fx_gbp_per_usd`; NULL when `fx_status` is `pending`;
- `consideration_basis` = `broker_stated` (the only value in this policy);
- `fingerprint` (Design 4.7).
- Primary key `(run_id, lot_key)`.

**Precision.**
- Every decimal is read as a `Decimal`: stored payloads through their JSONB text, lookup bodies with
  `json.loads(body_text, parse_float=Decimal, parse_int=Decimal)`.
- A stored-payload number with more than 15 significant digits fails its field check (fact 17): it cannot be shown to
  be the value the broker sent. A value with more decimal places than its column, or outside its column's range, fails
  its field check rather than being rounded: units `NUMERIC(20,8)`, money `NUMERIC(20,4)`, rates `NUMERIC(20,10)`.
- A computed disposal amount (`investment + netProfit`) outside `NUMERIC(20,4)` fails as `invalid_field:proceeds`.
- `amount_gbp` is the product of a value of at most 16 integer and 4 fractional digits and one of at most 10 and 10,
  so at most 26 integer and 14 fractional digits: `NUMERIC(40,14)` holds it exactly.
- Conservation and plausibility arithmetic runs under a `Decimal` context of 50 digits with traps for `Inexact` off
  and `InvalidOperation` on; conservation compares exactly, plausibility compares a ratio against the tolerance.
- The rounding policy for matched and pooled amounts is 3d's.

### 2. Provenance

**One environment and one account per ledger, enforced at ingestion.** `sync_portfolio` and `record_trade_events` gain
a required `environment` argument, passed by both callers from the provider's environment (`settings.etoro_env` in
`daily_portfolio_sync`, `self._env` in the websocket re-sync). Before writing any `trade_events` row,
`record_trade_events` checks the batch against the ledger:
- every existing `trade_events` row has this `environment`;
- every sync payload in the batch has one `CID`, equal to the `CID` of every existing sync row.

If either fails, it writes no `trade_events` row, records the fetch as `scope_refused` (below) and logs an error; the
rest of the portfolio sync proceeds. So no row of another environment or account can enter the ledger, suppress a row
under the global unique indexes, or move the global watermark. A batch with no open positions carries no `CID`; its
history rows are checked against the environment only, and the lookups' `accountId` covers the account (Design 4.1).
Moving the ledger to another environment or account is a reviewed migration, never a side effect.

The run binds `broker_positions` the same way: every real-id held position's payload `CID` must equal the scope's
account, else the run is `refused` (`positions_out_of_scope`).

**The environment backfill.** The migration sets `environment = 'demo'` on existing rows inside a guard that raises
unless `trade_events` is empty, or all of these hold:
- no `broker_credentials` row has `environment <> 'demo'`;
- `broker_credential_access_log` is non-empty, and no row has a NULL `credential_id`;
- the earliest access-log row precedes the earliest `trade_events.recorded_at`.

This is a classification resting on assumptions, not a proof: it assumes (a) no credential's `environment` column was
edited in place, (b) the access log has recorded every credential read since its first row, and (c) every
`trade_events` row was written by a sync that loaded its credentials through the logged path
(`scheduler._load_etoro_credentials` and the websocket's re-sync, both via `load_credential_for_provider_use`). None of
the three can be checked from stored data. The column is then `NOT NULL` with `CHECK (environment IN ('demo', 'real'))`.

**History fetches.** `fetch_trade_history_safely` returns a `TradeHistoryFetch(min_date, started_at, finished_at,
rows)`, `rows` being `None` on failure; both callers pass it through `sync_portfolio` to `record_trade_events`, which
writes one `trade_history_fetches` row per call in the sync's transaction: `fetch_id`, `environment`, `min_date`,
`started_at`, `fetched_at` (= `finished_at`), `outcome` (`ok`, `failed` or `scope_refused`) and `row_count`. Coverage
before the first recorded `ok` fetch is unknown, and the run says which fetches it saw; how 3d labels coverage is 3d's.

**Amended observations.** When `ingest_trade_events` meets an existing row on either unique index, it compares the new
observation with the stored one on every field of the field table (Design 4) that the two payloads share, normalised by
Design 4.2's rules, except the lifecycle field `isPartiallyAltered`:
- same source and kind: every field of that source's rows in the field table;
- a history open beside a stored sync open, or the reverse: `orderID`/`orderId`, `instrumentID`/`instrumentId`,
  `isBuy`, `leverage`, `openRate`, the open time (`openDateTime` / `openTimestamp`) and the investment
  (`initialAmountInDollars` / `initialInvestment`).

Where any differs, a `trade_event_observation_conflicts` row is written in the same transaction: `conflict_id`,
`event_id` of the stored row, `observed_at`, `source`, `stored_normalised JSONB`, `observed_normalised JSONB`,
`observed_raw JSONB`. The stored row is never changed (sql/194's contract). Any change counts, including a running total
such as `totalExternalFees`: a conflict makes the order `unresolved` (Design 4.8), never silently admitted.

### 3. Capturing order lookups

Lookups may expire (fact 2), so each run first asks the broker about the orders of its environment. A new provider
method, `EtoroBrokerProvider.lookup_order_raw(order_id) -> RawOrderLookup(http_status, body_text, error_detail)`, makes
the same GET as `lookup_order` under the same `D_order_info` quota lane, and returns the status and body text without
raising on HTTP status and without parsing; a transport error returns `http_status` NULL. The service classifies:

| response | outcome |
|---|---|
| 200, body parses as a JSON object | `found` |
| 200, body not JSON, or JSON but not an object | `malformed` |
| 404 | `not_found` |
| 429 | `rate_limited`; the run stops asking |
| 401 or 403 | `auth_failed`; the run stops asking |
| any other status | `http_error` |
| no response | `transport_error` |

Each attempt is one `tax_order_lookups` row, committed as it lands. Lookups are informational reads; the unattended
broker-mutation guard does not apply.

**Candidates.** Per order, from its latest attempt (*A*), its latest `found` attempt (*F*) and its current largest event
id (*E*):

| state | ask? |
|---|---|
| no attempt | yes, group 1 |
| *A* is `rate_limited`, `transport_error`, `http_error` or `auth_failed` | yes, group 2 |
| *A* is `malformed`, and *E* > *A*`.order_event_max_id`, or *A* is 7 or more days old, or *A*`.policy_id` differs from the current policy | yes, group 2 |
| *A* is `not_found` and *E* > *A*`.order_event_max_id` | yes, group 2 |
| *F* exists and *E* > *F*`.order_event_max_id` | yes, group 2 |
| *F* exists, no position of the order is held, and no attempt was made 7 or more days after its last event | yes, group 2 (the settling lookup) |
| otherwise | no |

Group 1 is asked first, newest order first; then group 2, oldest latest attempt first; at most 40 per run. Each run's
group-2 order rotates, so a retried order cannot hold a slot ahead of one waiting longer. Group 2 can be starved only by
more than 40 never-attempted orders per run, sustained; the account placed 21 orders in 14 months (fact 1). The run
records how many candidates it deferred.

**Which body is used.** Derivation uses *F*. A later `not_found` or error never discards it. Two `found` bodies of one
order are compared on their opening facts: `orderId`, `accountId`, `transaction`, `action`, `orderCurrency`, `asset`
(`instrumentId`, `settlementType`, `currency`, `leverage`, `side`) and each execution's normalised `openingData`. If any
differs, the order is `unresolved` with `lookup_amended`. The execution list may grow (a partial close adds a piece,
fact 5) and an execution's `state` and `remainingUnits` may change; *F*'s list is the one bound to the record. An
execution present in an earlier `found` body and absent from *F* is `lookup_amended`. A correction made by the broker
after the settling lookup is not observed (Limits).

### 4. Deriving the evidence for one order (pure)

`derive_order(order_id, events, lookups, held, conflicts) -> OrderEvidence` takes:
- the order's `trade_events` rows;
- its `found` lookup bodies, oldest first, *F* last;
- `held`: for each of its position ids, the `broker_positions` row (`units`, `amount`, payload `CID`) or `None`;
- its conflict rows.

It runs the checks below in order and records every one that fails. A check whose inputs an earlier failure left
undefined is skipped, so `reasons` names causes, not their consequences.

1. **Identity.** Before grouping, each event's order id, position id and broker instrument id is read from its column
   and from its payload (every spelling of Design 4.2); where two disagree the event fails `identity_conflict:<name>`.
   Every event of the order names one broker instrument id (else `instrument_conflict`).
2. **Account.** The set of account ids from sync `CID`, *F*'s `accountId` and held positions' `CID` has exactly one
   member (else `account_conflict`, or `account_unestablished` when empty).
3. **Fields.** Each field a later check reads is present and in its domain (table below). Keys are matched
   case-insensitively, so `orderID` and `orderId` are one field; two spellings of one field with different values in
   one payload are `field_conflict:<name>`. Enum values are compared case-insensitively. Absent is never zero: a missing
   `fees` fails as `missing_field:fees`; a value outside its domain fails as `invalid_field:<name>`.
4. **Product.** *F*'s `asset.settlementType` and the sync `settlementTypeID` of every position observed open must all
   map to one product (else `settlement_conflict`). The mapping is closed:

   | source value | product |
   |---|---|
   | lookup `real`; sync `1` | `real` |
   | lookup `cfd`; sync `0` | `cfd` |
   | lookup `realFutures`; sync `4` | `real_futures` |
   | lookup `marginTrade` | `margin_trade` |
   | sync `2` | `swap` |
   | sync `3` | `crypto_margin` |

   Any other value fails the field check. With no *F*, every position of the order must have a sync observation (else
   `settlement_unestablished`). The lookup documents one `settlementType` per order, so two products within one order
   are a contradiction.
   - Any product other than `real`, with checks 1–4 passing, is `excluded_non_share_product`: established, and outside
     share identification, with no further checks and no lots. 3d reports those as omitted.
5. **Shape.** A filled, long, unleveraged, uncopied buy:
   - *F*: `status.name` `Filled`, `transaction` `buy`, `action` `open`, `asset.side` `long`, `asset.leverage` 1,
     `orderCurrency` `usd`;
   - sync rows: `isBuy` true, `leverage` 1, `mirrorID` 0, `parentPositionID` 0;
   - history rows: `isBuy` true, `leverage` 1, `socialTradeId` 0, `parentPositionId` 0.

   Else `unsupported_shape:<field>`.
6. **Bindings and executions.**
   - *F*'s `orderId` and every execution's `openingData.orderId` equal the order id; its `asset.instrumentId` equals
     the order's broker instrument id; the set of its executions' `positionId` equals the set of the order's stored
     position ids (else `lookup_mismatch:<what>`).
   - **With *F*:** exactly one distinct opening. Every execution's `openingData`, normalised over `executionTime`,
     `openTime`, `units`, `avgPrice`, `avgConversionRate`, `fees`, `taxes` and `orderId`, is equal (else
     `multiple_executions`). Every execution binds to the record as in fact 6: state `closed` → no held position, and
     its position's close rows sum to its `remainingUnits`; any other state → a held position whose units equal its
     `remainingUnits` (else `piece_coverage_incomplete`). This is what makes the shared opening one execution: every
     piece the broker lists is accounted for by a stored close or a held position.
   - **Without *F*:** exactly one position, observed open by the sync with `isPartiallyAltered` false (else
     `executions_unestablished`).
   - **Acquisition date.** Every time candidate — *F*'s `executionTime` and `openTime`, sync `openDateTime`, each
     history row's `openTimestamp` — falls on one Europe/London date (else `acquisition_date_conflict`). The lot takes
     `executionTime`, else `openDateTime`. Each close row's `closeTimestamp` is at or after it (else
     `close_before_open`).
7. **Quantities and money.**
   - **Charges.** Every indicator present is zero: *F*'s `totalCosts` and each execution's `fees` and `taxes`; every
     sync row's `totalExternalFees` and `totalExternalTaxes`; every close row's `fees`. Else `charges_nonzero`.
   - **Units.** Execution units (*F*'s `openingData.units`, else the sync `initialUnits`) = Σ close-row units + Σ held
     units of the order's positions, exactly (else `units_not_conserved`).
   - **Consideration.** Every stored initial investment of the order (sync `initialAmountInDollars`, history
     `initialInvestment`) is equal (else `consideration_conflict`). That amount is the acquisition's `usd_amount`.
   - **Money.** Σ close rows' `investment` + Σ held positions' `amount` = the consideration, exactly (else
     `investment_not_conserved`). The PnL page documents `amount` as the initial investment plus additional margin; a
     long x1 real position has no margin, and all 10 held real-id positions show `amount = initial_amount_in_dollars`
     (fact 7).
   - **Consideration against the execution.** |units × rate × conversion − consideration| ≤ 0.001 × consideration,
     with rate and conversion from *F*'s `avgPrice` and `avgConversionRate`, else the sync row's `openRate` and
     `openConversionRate` (else `consideration_implausible`). The conversion rate turns the pricing currency into USD,
     so this check holds in any pricing currency.
   - **Pricing currency of the disposals.** An order with close rows needs *F* with `asset.currency` `USD` (else
     `pricing_currency_unestablished` with no *F*, or `close_conversion_unestablished` when *F* names another
     currency): the history row carries no close conversion (fact 10), so only a USD-priced piece's proceeds can be
     checked.
   - **Disposal amount.** Each close row's `investment + netProfit`.
   - **Proceeds against the execution.** |units × closeRate − (investment + netProfit)| ≤ 0.001 × (investment +
     netProfit) (else `proceeds_implausible`).
8. **Lots and keys.** An admitted order yields one acquisition lot and one disposal lot per close row.
   - Acquisition `lot_key` = `acq:<environment>:<order_id>`.
   - Disposal `lot_key` = `dis:<environment>:<position_id>:<closeTimestamp>`, the identity of `uq_trade_events_close`.
   - `fingerprint` = SHA-256 of canonical JSON over exactly these keys: `canonical_version` (`1`),
     `evidence_policy_id`, `lot_key`, `subject`, `order_id`, `instrument_id`, `etoro_instrument_id`, `direction`,
     `position_ids`, `trade_event_ids`, `executed_at`, `uk_date`, `tax_year`, `units`, `usd_amount`, `fx_rate_date`,
     `fx_status`, `fx_observation_date`, `fx_usd_per_gbp`, `fx_gbp_per_usd`, `fx_policy_id`, `amount_gbp`,
     `consideration_basis`. Keys sorted; arrays sorted ascending; decimals as their normalised string with no exponent;
     NULL as JSON null; timestamps as UTC ISO 8601 with microseconds.
9. **Amendments.** A conflict row for any of the order's events adds `source_amended`; differing `found` bodies add
   `lookup_amended` (Design 3).
10. **Policy.** `TAX_EVIDENCE_POLICY_ID = "broker_evidence_v1"` names the checks, the field table, the product mapping,
    the normalisation, the tolerances, the candidate rules and the keys. Changing any of them is a new policy id.

An order with no failing check is `admitted`. Any failing check makes it `unresolved`, with no lots: an order is never
part-admitted.

**Subjects without an order.** An event whose order id is missing, unparseable or in conflict becomes part of the
subject `position:<position_id>`, which collects every event of that position, with `missing_field:order_id`,
`invalid_field:order_id` or `identity_conflict:order_id`. A real-id `broker_positions` row with no `trade_events` row is
the subject `position:<position_id>` with `orphan_position`, unless its payload's order id names an order already
present, which then gains `orphan_position`. Position subjects are always `unresolved`. There are none today (fact 7).

**Field table.** "Ts" is an ISO 8601 timestamp with an offset or `Z`, normalised to UTC; "id" is a positive integer;
"money" fits `NUMERIC(20,4)`; "units" is positive and fits `NUMERIC(20,8)`; "rate" is positive and fits
`NUMERIC(20,10)`; every number is finite and, in a stored payload, has at most 15 significant digits.

| source | field | domain |
|---|---|---|
| sync open | `CID`, `orderID`, `positionID`, `instrumentID` | id |
| sync open | `settlementTypeID` | a key of the product mapping |
| sync open | `isBuy`, `isPartiallyAltered` | boolean |
| sync open | `leverage` | positive integer |
| sync open | `mirrorID`, `parentPositionID` | non-negative integer |
| sync open | `initialUnits` | units |
| sync open | `initialAmountInDollars` | money, positive |
| sync open | `totalExternalFees`, `totalExternalTaxes` | money, non-negative |
| sync open | `openRate`, `openConversionRate` | rate |
| sync open | `openDateTime` | Ts |
| history | `orderId`, `positionId`, `instrumentId` | id |
| history | `isBuy` | boolean |
| history | `leverage` | positive integer |
| history | `socialTradeId`, `parentPositionId` | non-negative integer |
| history | `units` | units |
| history | `investment`, `initialInvestment` | money, positive |
| history | `netProfit` | money (any sign) |
| history | `fees` | money, non-negative |
| history | `openRate`, `closeRate` | rate |
| history | `openTimestamp`, `closeTimestamp` | Ts |
| lookup | `orderId`, `accountId`, `asset.instrumentId`, executions' `positionId`, `openingData.orderId` | id |
| lookup | `status.name`, `transaction`, `action`, `asset.side`, `asset.currency`, `orderCurrency`, executions' `state` | non-empty string |
| lookup | `asset.settlementType` | a key of the product mapping |
| lookup | `asset.leverage` | positive integer |
| lookup | `totalCosts` | money, non-negative |
| lookup | `openingData.units`, executions' `remainingUnits` | units (`remainingUnits` may be 0) |
| lookup | `openingData.avgPrice`, `openingData.avgConversionRate` | rate |
| lookup | `openingData.fees`, `openingData.taxes` | money, non-negative |
| lookup | `openingData.executionTime`, `openingData.openTime` | Ts |
| `broker_positions` | `units` | units or 0 |
| `broker_positions` | `amount` | money, non-negative |
| `broker_positions` | payload `CID` | id |

### 5. Sterling

For each lot, `resolve_tax_fx(state, uk_date)` (3b), with `state` from `load_tax_fx_state` loaded on an idle connection
before the snapshot (Design 6):
- `published` or `carried_forward`: the rate fields are stored and `amount_gbp` computed;
- `pending` (a date after the latest accepted observation, such as today's): the lot is stored with `amount_gbp` NULL and
  `fx_status` `pending`; the next run recomputes it;
- `unavailable` or `out_of_range` for any lot: the run is `refused` with that reason and publishes nothing.

Every run recomputes every lot from the current state, so a lot computed from an older state never enters a new
generation.

### 6. A run

`run_tax_broker_evidence(conn, broker)`, on one connection. The body runs inside `try`/`finally`; the `finally` rolls
back any open transaction and then calls `pg_advisory_unlock(TAX_EVIDENCE_LOCK_KEY)` if the lock was taken.
1. Take the session-level `pg_try_advisory_lock(TAX_EVIDENCE_LOCK_KEY)`. If another run holds it, return `skipped`
   without writing. Holding it, mark any `in_progress` run row `abandoned` (its process died), then insert this run's
   row `in_progress` and commit.
2. Capture lookups (Design 3).
3. Load the FX state on the idle connection.
4. In one `snapshot_read` (REPEATABLE READ) block, read the environment's `trade_events`, the found lookups, the conflict
   rows, the latest and latest `ok` history fetches, `broker_positions` and `tax_evidence_scope`, with their watermarks.
5. Scope. Every account id in the evidence must be one value (else `refused`, `account_ambiguous`), and every held
   real-id position must carry it (else `refused`, `positions_out_of_scope`). With no scope row, that value and the
   run's environment become the scope at publication. With a scope row, both must equal it (else `refused`,
   `scope_mismatch`).
6. Derive every subject (Design 4) and its sterling (Design 5), in memory.
7. Publish in one READ COMMITTED transaction: call `tax_fx_state_is_current(conn, state)`; if false, the run is
   `refused` (`fx_state_changed`). Otherwise write the scope row if new, set the run `published` with its watermarks and
   counts, and insert its subjects and lots.

**Terminal states.** A refusal at steps 5–7 rolls back, then sets the run `refused` with its reason in its own
transaction. An exception anywhere rolls back, sets the run `abandoned` with the exception's class and message in its
own transaction, and re-raises. Either way the `finally` releases the lock, so the connection is reusable.

The effective generation is the latest `published` run whose `(environment, account_id)` equals the scope. Earlier
generations stay for audit; a reader diffs two by `lot_key` and `fingerprint`. Because the lock covers the whole run, no
older snapshot can publish after a newer one. A generation is internally consistent because it comes from one snapshot
and one FX state, written in one transaction, and its `held_positions` record exactly what was read.

### 7. Job

`tax_broker_evidence_refresh` runs daily at 12:13 UTC, after `tax_fx_refresh` (11:43) and the morning portfolio syncs.
`grep -n 'minute=13' app/workers/scheduler.py` finds no scheduled job at minute 13. It runs on the existing `db` lane, so
no lane or connection is added, and builds its provider the way `daily_portfolio_sync` does. It raises, so
`_tracked_job` records `failure`, on a `refused` or `abandoned` run; a `skipped` run logs and returns. Registration
mirrors `tax_fx_refresh`: `SCHEDULED_JOBS`, the lane map in `app/jobs/sources.py`, and `_INVOKERS`.

### 8. Retiring the fills route and `fx_rates`

- `_ingest_fills` and `_load_fx_rate` are deleted. `fills` is the app's own order record (`order_client._persist_fill`,
  `app/api/orders.py`), a subset of the account's trades; reading it beside the broker record would count those trades
  twice.
- `ingest_tax_events` keeps its signature and returns zero fills. `daily_tax_reconciliation` keeps running the matcher
  over `tax_lots`, which now has no writer; 3d moves matching onto the effective generation and retires or reshapes
  `tax_lots`.
- `fx_rates` is dropped. The settled decision "Own EOD NAV-snapshot table (#1594)" names `fx_rates` and `_load_fx_rate`
  as the tax path ECB rows must stay out of; its wording is updated to name the 3b tables. The decision is unchanged.
- The tests that seed `fx_rates` or `fills` for the tax path are rewritten or removed with the code they test; the
  matcher's pure tests stay.

## Local policy (no published rule; frozen under `TAX_EVIDENCE_POLICY_ID`)

- **Supported shapes only.** One execution per order, every charge indicator zero, filled long x1 uncopied real assets,
  USD-priced close pieces. Each extension needs an observed case and a new policy id.
- **The broker's stated amounts are the consideration** (Source rules), corroborated by the execution record within
  0.1% and conserved exactly across pieces.
- **A shared opening is one execution** when every listed piece is bound to a stored close or a held position (Design
  4.6, fact 6).
- **Tolerances of 0.1%.** About four times the largest measured proceeds gap (fact 10) and ten times the largest
  acquisition gap (fact 8). They catch a scale or currency error or a misread field, not cents.
- **An order is admitted whole or not at all.**
- **Early lookup capture, retries and the settling lookup** (Design 3), against an undocumented and possibly age-based
  retention.
- **The environment backfill** rests on three stated assumptions (Design 2).
- **One environment and account per ledger**, fixed at ingestion and by the first publishing run.

## Not in this slice, and limits

- **3d:** matching from the effective generation, including same-day aggregation; security identity across listings;
  tax character and its effective periods; excess reportable income; how unresolved and excluded subjects gate a
  published result; how history coverage is labelled; the tax-year view; the page.
- **History completeness.** The stored record starts at the first event the sync saw; coverage before the first
  recorded `ok` fetch is unknown. The history endpoint documents no page totals, and #2991 records that its lookback
  rule cannot be met. An order absent from `trade_events` is invisible unless a position of it is still held (orphans).
  The documented closed-position counts endpoint (`/api/v1/data/positions/closed-events/history`) identifies the caller
  by GCID; probed once, it returned close years 2021–2026, which the demo ledger does not have, so it describes another
  account and cannot check this one's completeness.
- **Undetectable adjustments.** An amount the broker changed without changing the investment, the units, the prices or a
  charge field is not seen.
- **Amendments** older than the 7-day history overlap are never re-observed (fact 16), and a lookup changed after its
  settling lookup is not observed.
- **Partial closes on the sync side.** Whether a partial close changes a stored sync open's `initialUnits` or other
  compared fields is not yet observed; if it does, those orders are `source_amended` at acceptance and stay unresolved
  until a new policy.
- **Retention.** An order whose lookup failed before capture keeps only its sync evidence: the 8 orders of 2025 today.
  Without a lookup, an order with closes cannot establish its pricing currency.
- **Close conversion.** Non-USD close pieces stay unresolved until a close's executed conversion or cash credit is
  captured. The private stream carries close events, but on another account (fact 11).
- **Demo.** All evidence today is from the demo account; nothing here is a real tax figure.

## Tests

Pure (`tests/test_tax_broker_evidence.py`), each asserting outcome, reasons and lots:
- the AAPL split shape (USD, two pieces): admitted, one acquisition and two disposals;
- the IUSA.L split shape: one opening reconstructed from two executions, then `unresolved` with
  `close_conversion_unestablished`, no lots;
- a split order still partly open: unit and money conservation against held units and `amount`, admitted;
- capture before a partial close, then a refresh listing the new piece: not `lookup_amended`, admitted; a refresh that
  drops a piece or changes `openingData`: `lookup_amended`;
- two distinct identical executions where one piece's close is missing (Codex r2 finding 19): `piece_coverage_incomplete`;
- an order with two different openings: `multiple_executions`;
- every failing check in Design 4, including: a missing `fees` (not zero); `orderID` and `orderId` disagreeing in one
  payload; a column order id disagreeing with its payload; events disagreeing on instrument; a lowercase enum; an
  unknown `settlementType` (`invalid_field`, not excluded); a CFD/REAL conflict; a position without a sync observation
  and no lookup; a lookup missing one stored position; `openingData.orderId` differing; `status` not `Filled`; a copied
  history row; mixed acquisition dates; a close before its open; an account conflict; a held position with another
  `CID`; a non-zero `totalCosts` with zero execution fees; a non-zero sync `totalExternalFees` beside a zero lookup fee;
  a no-lookup order with closes (`pricing_currency_unestablished`); a stored number with 16 significant digits; a
  proceeds sum outside `NUMERIC(20,4)`; amended observations of each kind;
- every reason recorded when several checks fail, and dependent checks skipped;
- position subjects: an event with no order id, two events of one position with no order id (one subject), an orphan
  held position;
- the fingerprint is stable under array reordering, excludes `run_id`, `fx_run_id` and itself, and changes with
  `evidence_policy_id`;
- lookup classification: every row of the response table, including a non-JSON 200 and a JSON array 200;
- lookup candidates: every row of the candidate table, group order, the 40 cap, and rotation (an order failing every run
  does not keep an older waiting order out);
- FX: `published`, `carried_forward`, `pending` (lot stored, `amount_gbp` NULL), `unavailable` (run refused).

DB (`tests/test_tax_broker_evidence_db.py`), one per new SQL mechanism:
- a run over seeded `trade_events`, lookups and `broker_positions` publishes one generation and the scope; a second run
  with new evidence publishes a second, and the first is unchanged;
- a second run started while the first holds the lock returns `skipped`; a stale `in_progress` row is marked
  `abandoned`; after a run raises, the lock is free and the connection runs another query;
- `tax_fx_state_is_current` false at publication: run `refused`, nothing but the run row written;
- a second account in the evidence: `refused` (`account_ambiguous`); a held position of another account:
  `positions_out_of_scope`; a scope mismatch: `scope_mismatch`;
- the migration guard refuses a non-empty `tax_lots`; the backfill guard refuses a non-demo credential and an empty
  access log;
- `record_trade_events` with another environment, or a sync `CID` differing from the ledger's, writes no
  `trade_events` row and one `scope_refused` fetch row, and the portfolio sync still commits;
- `ingest_trade_events` writes a conflict row for a history close re-observed with a different `investment`, and for a
  history open beside a sync open with a different `openRate`, in both arrival orders; none when they agree;
- `record_trade_events` writes a `failed` fetch row for a failed fetch, with its `min_date`.

Provider calls are faked; no test reaches the broker.

## Acceptance (dev DB)

After the build merges, the jobs process reloads and one run publishes. The counts below are expectations from the
measured checks (facts 2–14). The script did not run the identity, field, shape, date, amendment, scope or
piece-coverage checks as the build will, so the first run is the full-population derivation; any difference from these
counts is investigated before 3d starts.
- `tax_order_lookups`: 21 attempts from the first run, 13 `found` and 8 `not_found`, plus one per order opened since
  2026-10-13.
- `tax_evidence_scope`: one row, environment `demo`.
- `tax_evidence_subjects` for the run, over the 21 orders known on 2026-10-13:
  - `admitted`: 8 (NXH, AAPL, SPY.RTH ×6);
  - `excluded_non_share_product`: 6 (VOO, QQQ, IEP ×2, SVXY, SVOL);
  - `unresolved`: 7. ILMN: `account_unestablished`, `settlement_unestablished`, `executions_unestablished`. GME ×2:
    `pricing_currency_unestablished`. CSPX.L ×3 and IUSA.L: `close_conversion_unestablished`.
- `tax_evidence_lots`: 8 acquisitions and 3 disposals (AAPL ×2, SPY.RTH ×1), each disposal with `fx_status`
  `published` or `carried_forward`.
- One acquisition and one disposal checked by hand: units, USD amount, the 3b rate for the UK date (compared on the DB,
  not printed) and `amount_gbp`.
- `trade_events.environment` is `demo` on every row; `fx_rates` does not exist; `tax_lots` is empty;
  `trade_history_fetches` has an `ok` row from the first sync after deploy.

## Full-population verification

Over every stored order (21) and event (36), not a sample: product sources, every charge indicator, unit and money
conservation, piece binding for the 13 found orders, copy and parent ids, the two corroboration ratios, account
agreement and the lookup shapes (`--checks`, `--facts`, default mode). ILMN is unmeasured for everything but its lookup
status. The remaining checks run on the full population for the first time at acceptance, which is why the acceptance
counts are expectations.

## Finding routing (slice 3 checkpoint-1 round 1)

| finding | where |
|---|---|
| 8 one acquisition per order | Design 4.6 (one execution, every piece bound), facts 5–6 |
| 9 settlement union | Design 4.4 (closed mapping), fact 13 |
| 10 acquisition charges | Design 4.7 (every indicator zero), fact 9 |
| 11 proceeds identity | Source rules, Design 4.7 (USD only, corroborated), fact 10 |
| 12 excluded lots cannot become eligible | Design 6 (full rebuild per generation) |
| 13 later failure, changed values | Design 2 (conflicts), 3 (re-lookup, settling lookup), 4.9, 6 |
| 14 account provenance | Design 2 (ingestion scope), 4.2, 6, facts 11–12 |
| 19 facts attributed to the script | each fact names its mode, output file or command |
| 20 tautological units | Design 4.7: execution units from the lookup or sync |
| 21 timestamps and lineage | Design 4.6 |
| 22 full population ≠ full history | Design 2 (fetch outcomes), Limits, orphan subjects |
| 23 missing and invalid values | Design 4.3 and the field table |
| 32 existing fill lots | Design 1 guard, Design 8 |
| 34 precision (quantities, money) | Design 1 precision, fact 17 |
| 35 refused events invisible | `tax_evidence_subjects`, subjects without an order |
| 39 snapshot and publication | Design 6 |
| 41 schema spellings | Design 4.3, fact 15 |

## Security

No order, capital or authorisation path changes. The new broker call is the documented read-only `orders:lookup`, already
used by order reconciliation and metered on its shared quota lane. The ingestion scope check refuses ledger writes only;
it never blocks the portfolio sync. The writes are the job's own tables, one new column and two new tables beside
`trade_events`. Account identifiers stay in the database and the gitignored `var/`: the repo is public, so no lookup
body, account id or customer id is committed.
