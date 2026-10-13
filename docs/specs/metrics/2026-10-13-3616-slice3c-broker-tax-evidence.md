# The account's broker trades as UK tax evidence (#3616 slice 3c)

Source: #3616 slice 3, split on 2026-10-13 into 3a (matcher, #3789), 3b (tax FX, #3790/#3793), 3c (this) and 3d. The
routing of the slice 3 checkpoint-1 round 1 findings is on #3616 (2026-10-13 13:46Z); "Finding routing" at the end maps
the findings this slice owns to its sections.

**What this slice does.** It turns the account's broker record into evidence for the tax ledger: one acquisition per
order execution and one disposal per broker close piece, each with its account, environment, product, units, the
broker's stated US-dollar amount and that amount's sterling value at the 3b rate. Each run rebuilds the whole set from
evidence and publishes it as one generation.

**It admits only shapes it has verified.** Every order ends in one of three outcomes:
- `admitted`: a filled, long, unleveraged, uncopied real-asset order with one execution (which a partial close may have
  split into several fully closed pieces, or left as one position), every charge and adjustment indicator zero, and
  every close piece priced in US dollars;
- `excluded_non_share_product`: the order's evidence is bound and unamended, and it is established as a CFD or another
  documented non-share product;
- `unresolved`, with every reason: anything else.

**Admitted is not yet tax consideration.** An admitted lot carries the broker's stated amount with
`consideration_status = broker_stated_unreconciled`. Whether those amounts are the TCGA consideration needs a
reconciliation against an independent record of the account's cash, which no stored source provides (Source rules,
Limits). 3d decides that reconciliation and must not publish a tax figure from an unreconciled lot.

**What it does not do.** It changes no published tax figure. Reconciling consideration, matching, security identity
across listings, tax character, excess reportable income, the tax-year view and the page are slice 3d. Until 3d, nothing
reads the generations except tests and the acceptance queries. This slice also retires the `fills` route into
`tax_lots` and the unused `fx_rates` table.

## Facts established first

"Script" is `scripts/measure_3616_slice3c_order_lookup.py`, run on 2026-10-13 against the dev DB and the demo account.
- The default mode makes one `orders:lookup` GET per stored order and saves the raw bodies to the gitignored
  `var/research/3616/slice3c/order_lookup_raw.json`, because they carry account identifiers and the repo is public.
  Its printed summary is `var/research/3616/slice3c_lookup_summary.txt`.
- `--checks` recomputes the per-order checks from the saved bodies and the DB, with no network
  (`var/research/3616/slice3c_checks.txt`; its `totals` line carries every population count with its denominator).
- `--facts` prints the DB and lookup-shape facts as counts and dates, never an identifier
  (`var/research/3616/slice3c_facts.txt`).
- Code searches are recorded with their commands in `var/research/3616/slice3c_code_search.txt`.

Every count below is from the stored population on 2026-10-13: 21 orders, 36 events, 23 positions. That is not the
account's complete history (Limits).

1. **The stored record** (`--facts`): 16 `etoro_sync/open`, 7 `etoro_history/open`, 13 `etoro_history/close`. The
   `order_id` column is filled on all 20 history rows and on none of the 16 sync rows, which carry it in
   `raw_payload->>'orderID'`. The earliest stored order opened 2025-08-12.
2. **The order lookup answered for recent orders only** (`--facts`). 13 orders returned HTTP 200, opened 2026-09-16 to
   2026-10-09. 8 returned 404 with `"Order category for <id> not found"`, opened 2025-08-12 to 2025-11-14. No stored
   order was opened between those ranges. Age-based retention, with a boundary somewhere from 27 to 333 days on this
   evidence, is a hypothesis; the portal page states no retention. Early capture is therefore precautionary policy.
3. **The shape of the 13 found lookups** (`--facts`, "lookup shape"): every one has `status.name` `Filled`,
   `transaction` `buy`, `action` `open`, `asset.side` `long`, `asset.leverage` 1 and `orderCurrency` `usd`.
   `asset.settlementType` is `REAL` on 11 and `CFD` on 2. `asset.currency` is `USD` on 9 (7 real, 2 CFD), `GBP` on 3
   (CSPX.L) and `GBX` on 1 (IUSA.L). They hold 15 executions, and every execution's `openingData.orderId` equals the
   order id. Execution `state` is `closed` on 10 and `open` on 5 (summary). On every body, `totalCosts` equals the
   first execution's `fees + taxes`. The page documents `settlementType` values `cfd`, `real`, `realFutures` and
   `marginTrade`.
4. **`instruments.currency` is not pricing-currency evidence** (`--facts`). For the 3 CSPX.L orders it says `USD` where
   the lookup says `GBP`; it agrees on the other 10. This slice takes pricing currency from the lookup only.
5. **A partial close splits a position, and the lookup lists the pieces with one shared opening** (summary). IUSA.L
   order 383293149 and AAPL order 383320185 each return two executions, both `closed`, whose `openingData` objects are
   identical, each carrying the whole order's units (0.646048 and 0.147071). Summing `openingData.units` over
   executions double-counts. The history rows of the pieces carry `parentPositionId` 0, so the order id is the only
   stored lineage. The other 11 found orders have one execution. No stored order has a split with a piece still open.
   ⚠ The sql/194 and `app/services/trade_events.py` docstrings say a partial close reduces the same `positionId`; on
   this account it mints a new id. The build corrects the Python docstring; the applied migration's comment stays,
   with a pointer from the Python one.
6. **Each execution's state binds to the stored record** (`--checks`, `position_state_binding`, all 13 found orders).
   An execution in state `closed` has no held position, and the stored close rows of its position sum to its
   `remainingUnits`, which is positive on all 10; an execution in state `open` has a held position whose units equal
   its `remainingUnits`.
7. **Units and money are conserved per order** (`--checks`, all 20 orders whose acquired units are known: the lookup's
   one distinct opening, or the sync `initialUnits` of a one-position order). Acquired units equal closed units plus the
   units `broker_positions` holds under the order's positions, exactly. The stored initial investment (sync
   `initialAmountInDollars`, history `initialInvestment`) has one value per order, and equals Σ close rows' `investment`
   plus Σ held positions' `amount`, exactly. All 10 real-id `broker_positions` rows appear in `trade_events`, and all 10
   have `amount = initial_amount_in_dollars` and leverage 1 (`--facts`). ILMN (order 272136682) has neither a lookup nor
   a sync observation.
8. **The broker's investment against the execution record** (`--checks`). For the 20 orders, the stored initial
   investment differs from `units × open rate × open conversion rate` by a relative gap printed in full per order; the
   largest is 0.0000936852 (AAPL). This corroborates the amount; it does not establish it (Source rules).
   - `frozenAmount` is not a reconciliation. The page documents it as "the amount frozen/reserved for the order
     including costs", which is the request, not the execution.
9. **Charge and adjustment indicators** (`--checks`, `totals`, each indicator counted separately per observation). The
   PnL page documents `totalExternalFees` as *"Total fees in USD charged on the position. Example - TicketFee. This
   value does not include overnight fees and dividends"*, `totalExternalTaxes` as *"Total taxes in USD charged on the
   position. Example - SDRT"* and `totalFees` as *"Total overnight fees and dividends charged/paid on the position"*.
   The lookup documents `openingData.fees` as *"Fees charged for the execution"*, `taxes` as *"Taxes applied to the
   execution"* and `totalCosts` as the order's total costs. The history page documents close `fees` as "the fees of the
   trade". Non-zero / observed:

   | indicator | non-zero | observed |
   |---|---|---|
   | sync `totalExternalFees` | 2 | 16 |
   | sync `totalExternalTaxes` | 0 | 16 |
   | held `broker_positions.total_fees` | 0 | 10 |
   | archived `broker_positions_closed.total_fees` (latest per position) | 0 | 6 |
   | lookup `totalCosts` | 2 | 13 |
   | lookup execution `fees` | 2 | 15 |
   | lookup execution `taxes` | 0 | 15 |
   | history close `fees` | 0 | 13 |

   The non-zero values are the two CFD orders (SVXY 1.5, SVOL 0.72), equal across the three indicators that carry
   them. 7 positions, all history-only, have no `totalFees` observation.
10. **Disposal proceeds of USD-priced pieces** (`--checks`). For the 3 close pieces of real orders whose lookup
    `asset.currency` is `USD` (SPY.RTH ×1, AAPL ×2), `investment + netProfit` differs from `units × closeRate` by at
    most a relative 0.0002339584; the 2 CFD pieces (SVXY, SVOL) are within 0.0000096. The history row carries no close
    conversion rate: 0 history payload keys contain "conversion" (`--checks`), and the PnL page's
    `closeConversionRate` describes an open position's current valuation, not an executed close. The daily account
    snapshots hold one row per day (48 rows, 48 dates; `--facts`), so a close's cash credit cannot be isolated from
    other same-day movements.
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
14. **Copy and parent ids** (`--checks`, `totals`): all 20 history rows have `socialTradeId` 0 and `parentPositionId` 0;
    all 16 sync rows have `mirrorID` 0 and `parentPositionID` 0.
15. **Field spellings differ between documentation renderings.** The stored sync payloads use `CID`, `orderID`,
    `settlementTypeID`, `mirrorID` and `parentPositionID`; one rendering of the PnL page shows those spellings and
    another shows `cid`, `orderId`, `settlementTypeId`, `mirrorId` and `parentPositionId`. History rows use `orderId`,
    `positionId` and `parentPositionId`.
16. **How the ledger is written** (code). `trade_events` keeps the first observation of a row
    (`ingest_trade_events`): one open per position whatever its source, and one close per `(position_id, executed_at)`
    (`sql/194_trade_events.sql`, the two unique indexes). A later observation that disagrees on units or realised P&L is
    counted as `conflict_anomaly` and logged, not stored; other fields are not compared. The history watermark is the
    latest stored history close's `executed_at`, less 7 days (`compute_history_min_date`), so a history row not written
    is fetched again next time. A failed history fetch is passed on as `None` (`fetch_trade_history_safely`) and stored
    nowhere. `daily_portfolio_sync` fetches the portfolio, then the history, in one provider session
    (`scheduler.py`, the block after `compute_history_min_date`); `sync_portfolio` writes `broker_positions` and then
    calls `record_trade_events` in one transaction that its caller commits.
17. **Stored payloads were decoded as binary floats.** The provider decodes with `response.json()` before storing JSONB,
    so a stored number is the shortest decimal that round-trips the double the broker's text decoded to. A number the
    broker sent with more precision than a double holds was altered before storage, and nothing stored can show it.
18. **The retired route** (`--facts`, code search file). `tax_lots` and `fx_rates` hold 0 rows; `fills` holds 1, whose
    order's `broker_order_ref` starts `DEMO-`. No code writes `fx_rates` (`INTO fx_rates\b`: no match); its one reader
    is `tax_ledger._load_fx_rate`, called only by `_ingest_fills`; the migrations name it only in comments.
    `daily_tax_reconciliation` is on-demand only (the comment above the on-demand list in `scheduler.SCHEDULED_JOBS`).

## Source rules

- **Acquisition cost.** TCGA 1992 s38(1)(a): *"the amount or value of the consideration, in money or money's worth,
  given by him … wholly and exclusively for the acquisition of the asset, together with the incidental costs to him of
  the acquisition"*; s38(2) lists the incidental costs (fees for professional services such as a broker's, costs of
  transfer or conveyance including stamp duty, advertising for a buyer or seller, and costs of valuation; HMRC
  CG15250).
- **Disposal consideration.** TCGA s15 and s38(1)(c): the amount received for the disposal, less the incidental costs
  of disposal.
- **What this slice can and cannot establish.** The broker's trade record states what the account committed (the
  order's initial investment) and what each close returned (`investment + netProfit`). No eToro page documents these
  fields as the cash debited or credited, and no stored source records the account's cash per trade (fact 10). This
  slice therefore records them as the broker's stated amounts, with three bounds on their being wrong: every documented
  charge and adjustment indicator observed is zero, so nothing in s38(2) arises from any documented field; the execution
  record corroborates them (Design 4.9); the position ledger conserves them exactly (Design 4.9). Turning them into
  consideration needs an independent cash record, which is 3d's.
- **`totalFees`.** Documented as overnight fees and dividends. Neither is an s38(2) incidental cost, but a non-zero value
  on a long x1 real position is outside the verified shape, so it refuses admission rather than being classified.
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
- creates `trade_ledger_scope`, adds `trade_events.environment`, backfills both and constrains the column (Design 2);
- creates `trade_event_observations` and `trade_history_fetches` (Design 2);
- creates the tables below.

**`tax_order_lookups`.** Append-only, one row per lookup attempt:
- `lookup_id` identity, `environment`, `order_id`, `requested_at`, `policy_id` (Design 4.11);
- `outcome`: `found`, `malformed`, `not_found`, `rate_limited`, `auth_failed`, `http_error` or `transport_error`;
- `http_status` (NULL for a transport error), `error_detail`;
- `body_text TEXT` and `body_sha256` (NULL when no body arrived), `account_id` (the body's `accountId` when `found`);
- `order_event_max_id`, the order's largest `trade_events.event_id` when the attempt was made.

**`tax_evidence_runs`.** One row per run, created at entry (Design 6):
- `run_id`, `environment`, `account_id` (from `trade_ledger_scope`; NULL if none), `started_at`, `finished_at`;
- `state`: `in_progress`, `published`, `refused` or `abandoned`; `reason`;
- watermarks of what it read: `trade_events_max_event_id`, `lookups_max_id`, `observations_max_id`;
- `history_fetch_latest_id` (the latest `trade_history_fetches` row, any outcome) and `history_fetch_ok_id` (the latest
  `ok` one); the fetch row carries the portfolio observation time it was written with (Design 2);
- `fx_run_id`, `fx_policy_id`, `evidence_policy_id`, `counts JSONB` per outcome, `lookups_deferred`.

**`tax_evidence_subjects`.** One row per subject per published run. A subject is an order (`order:<order_id>`) or a
position whose order cannot be established (`position:<position_id>`); Design 4 partitions the events so that each
belongs to exactly one subject.
- `run_id`, `subject`; `order_id` (NULL for a position subject);
- `etoro_instrument_id` (NULL when the subject's events disagree or none carries one) and `instrument_id` (NULL when the
  app has not resolved it);
- `product`: `real`, `cfd`, `real_futures`, `margin_trade`, `swap`, `crypto_margin`, or NULL when not established;
- `outcome`: `admitted`, `excluded_non_share_product` or `unresolved`; `reasons TEXT[]` (every failing check, in Design
  4 order; empty unless `unresolved`);
- `trade_event_ids BIGINT[]` (sorted; empty for an orphan), `lookup_id` (the *F* used, or NULL);
- `held_positions JSONB`: position id → `{units, amount, total_fees, cid}` read from `broker_positions`, or `null` when
  the position has no row; `archived_total_fees JSONB`: position id → the latest `broker_positions_closed.total_fees`,
  for positions that have one;
- `first_event_at`, `last_event_at` (NULL for an orphan).
- Primary key `(run_id, subject)`.

**`tax_evidence_lots`.** One row per admitted acquisition or disposal per published run, built by the table in Design
4.10:
- `run_id`, `lot_key`, `subject`, `order_id`, `instrument_id` (NOT NULL), `etoro_instrument_id`, `direction`;
- `position_ids BIGINT[]`, `trade_event_ids BIGINT[]` (both sorted, non-empty);
- `executed_at`, `uk_date`, `tax_year`;
- `units NUMERIC(20,8)`, `usd_amount NUMERIC(20,4)`, `consideration_status` (`broker_stated_unreconciled`);
- the FX fields of 3b's consumer contract: `fx_rate_date`, `fx_status`, `fx_observation_date`, `fx_usd_per_gbp`,
  `fx_gbp_per_usd NUMERIC(20,10)`, `fx_policy_id`, `fx_run_id`;
- `amount_gbp NUMERIC(40,14)`, the exact product `usd_amount × fx_gbp_per_usd`; NULL when `fx_status` is `pending`;
- `fingerprint` (Design 4.10).
- Primary key `(run_id, lot_key)`.

**Precision.**
- Every decimal is read as a `Decimal`: stored payloads from their JSONB text, lookup bodies with
  `json.loads(body_text, parse_float=Decimal)`. Lookup bodies therefore keep the broker's decimal text; stored ledger
  payloads keep what survived float decoding (fact 17, Limits).
- A value with more decimal places than its column, or outside its column's range, fails its field check rather than
  being rounded: units `NUMERIC(20,8)`, money `NUMERIC(20,4)`, rates `NUMERIC(20,10)`.
- A computed disposal amount (`investment + netProfit`) outside `NUMERIC(20,4)` fails as `invalid_field:proceeds`.
- All arithmetic and canonical formatting run inside `decimal.localcontext()` with `prec = 60`, `Inexact` and
  `Rounded` trapped: an operand of at most 16+4 digits times one of at most 10+10 has at most 40 significant digits,
  so the product is exact, and a rounding anywhere raises instead of passing silently. `amount_gbp` (at most 26
  integer and 14 fractional digits) fits `NUMERIC(40,14)` exactly. The plausibility ratios are the one division; they
  are computed as `|a − b| ≤ 0.001 × b`, with no division.
- The rounding policy for matched and pooled amounts is 3d's.

### 2. Provenance

**One environment and one account per ledger, fixed before anything is written.** `trade_ledger_scope` holds one row,
`(environment, account_id, established_at)`. The migration creates it from the existing ledger: `demo` (the backfill
below) and the one distinct sync `CID` (fact 11); it raises if the sync rows carry more than one `CID`, and leaves the
table empty if the ledger is empty.

`sync_portfolio` and `record_trade_events` gain a required `environment` argument, passed by both callers from the
provider's environment (`settings.etoro_env` in `daily_portfolio_sync`, `self._env` in the websocket re-sync). Before
writing any `trade_events` row, `record_trade_events` establishes the batch's account: the one `CID` its sync payloads
carry (two or more → refused). Then:
- **no scope row:** a batch with an account establishes the scope `(environment, CID)` in the same transaction and is
  written; a batch with no account writes no `trade_events` row;
- **a scope row:** the batch is written only if its environment equals the scope's and its account equals the scope's.
  A batch with no account (no open positions) writes no `trade_events` row.

A batch not written records its fetch as `scope_unverified` (no account) or `scope_refused` (a mismatch), logs, and
lets the rest of the portfolio sync commit. Because the watermark moves only on written history rows (fact 16), the
next batch that carries the account fetches the skipped closes again, as far back as the endpoint serves them. So no
row of an unverified or other account enters the ledger, suppresses a row under its global unique indexes, or moves the
watermark. Moving the ledger to another environment or account is a reviewed migration, never a side effect.

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

**Fetches.** Both callers record the portfolio fetch time (`portfolio_fetched_at`, taken when `get_portfolio` returns)
and pass it with a `TradeHistoryFetch(min_date, started_at, finished_at, rows)` from `fetch_trade_history_safely`
(`rows` is `None` on failure) through `sync_portfolio` to `record_trade_events`. It writes one `trade_history_fetches`
row per call, in the sync's transaction: `fetch_id`, `environment`, `account_id` (the batch's, or NULL),
`portfolio_fetched_at`, `min_date`, `started_at`, `finished_at`, `outcome` (`ok`, `failed`, `scope_unverified` or
`scope_refused`) and `row_count`. The row and the `broker_positions` it accompanies commit together, so the run's
latest fetch row dates the portfolio observation it read. A sync that fails before `record_trade_events` leaves no row
and no `broker_positions` change. Coverage before the first recorded `ok` fetch is unknown; how 3d labels coverage is
3d's.

**Observations the ledger does not keep.** When `ingest_trade_events` meets an existing row on either unique index, it
compares the new observation with the stored one, field by field, over every field of the field table (Design 4) for
the two payloads' sources, normalised by Design 4.3's rules. A field present in one and absent in the other is a
difference. `isPartiallyAltered` is excluded as a lifecycle field. It writes a `trade_event_observations` row in the same
transaction when:
- the sources or kinds differ (a history open beside a sync open, either way round): always, `kind = 'supplemental'`,
  unless an identical supplemental observation is already stored;
- the source and kind are the same and any compared field differs: `kind = 'amendment'`.

Columns: `observation_id`, `event_id` of the stored row, `kind`, `observed_at`, `source`, `stored_normalised JSONB`,
`observed_normalised JSONB`, `observed_raw JSONB`. The stored row is never changed (sql/194's contract). Derivation
treats a supplemental observation as one more payload of the order (every field check of its source applies to it, so
a sync observation with a non-zero `mirrorID` fails the copy gate whichever arrived first) and an amendment as
`source_amended`.

### 3. Capturing order lookups

Lookups may expire (fact 2), so each run asks the broker about the orders of its environment after the scope pre-check
(Design 6). A new provider method, `EtoroBrokerProvider.lookup_order_raw(order_id) -> RawOrderLookup(http_status,
body_text, error_detail)`, makes the same GET as `lookup_order` under the same `D_order_info` quota lane, and returns the
status and body text without raising on HTTP status and without parsing; a transport error returns `http_status` NULL.
The service classifies:

| response | outcome |
|---|---|
| 200, body parses as a JSON object whose `orderId` is the asked id, with an `asset` object and a non-empty `positionExecutions` array of objects | `found` |
| 200, anything else | `malformed` |
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
| no attempt | yes, first-attempt queue |
| *A* is `rate_limited`, `transport_error`, `http_error` or `auth_failed` | yes, retry queue |
| *A* is `malformed`, and *E* > *A*`.order_event_max_id`, or *A* is 7 or more days old, or *A*`.policy_id` differs from the current policy | yes, retry queue |
| *A* is `not_found` and *E* > *A*`.order_event_max_id` | yes, retry queue |
| *F* exists and *E* > *F*`.order_event_max_id` | yes, retry queue |
| *F* exists, no position of the order is held, and no attempt was made 7 or more days after its last event | yes, retry queue (the settling lookup) |
| otherwise | no |

A run asks at most 40: up to 30 from the first-attempt queue, newest order first, because retention (if any) loses the
oldest first; and up to 10 from the retry queue, oldest latest attempt first. A queue's unused slots go to the other.
Every retry candidate is reached within ⌈retry queue length / 10⌉ runs. Under sustained overload of more than 30 new
orders per run, the oldest first attempts wait; the stored ledger holds 21 orders in total (fact 1). The run records how
many candidates it deferred.

**Which body is used.** Derivation uses *F*. A later `not_found` or error never discards it. Two `found` bodies of one
order are compared on every lookup field of the field table except the lifecycle ones: the execution list may grow (a
partial close adds a piece, fact 5), and an execution's `state` and `remainingUnits` may change. Any other difference,
including presence, makes the order `unresolved` with `lookup_amended`, as does an execution present in an earlier
`found` body and absent from *F*. A correction made by the broker after the settling lookup is not observed (Limits).

### 4. Deriving the evidence (pure)

**Partition.** Events are first grouped by position. A position's order id is the set of order ids its events and their
observations name, each read from the column and from every payload spelling:
- one id: the position belongs to that order; any of its events or observations lacking it adds `missing_field:order_id`
  to the order;
- no id: the position is the subject `position:<position_id>` with `missing_field:order_id` (or `invalid_field:order_id`
  when one is present but not an id);
- two or more: the position is the subject `position:<position_id>` with `identity_conflict:order_id`, and every order it
  names gains `position_quarantined`.

A real-id `broker_positions` row with no `trade_events` row is the subject `position:<position_id>` with
`orphan_position`, unless its payload names an order already present, which then gains `orphan_position`. Position
subjects are always `unresolved`. Every event belongs to exactly one subject. There are none today (fact 7).

`derive_order(order_id, events, observations, lookups, held, archived_fees) -> OrderEvidence` takes the order's events,
their supplemental and amendment observations, its `found` bodies (oldest first, *F* last), and per position the
`broker_positions` row (`units`, `amount`, `total_fees`, payload `CID`) or `None`, and the latest archived `total_fees`
if any. It runs the checks below in order and records every one that fails. A check whose inputs an earlier failure
left undefined is skipped, so `reasons` names causes, not their consequences.

**Checks that bind the evidence** (all orders):

1. **Identity.** Each event's position id and broker instrument id agree between column and payload, and each close
   row's `units` and `executed_at` columns equal its payload `units` and `closeTimestamp` (else
   `identity_conflict:<name>`). The columns are authoritative thereafter. Every event of the order names one broker
   instrument id (else `instrument_conflict`).
2. **Account.** The set of account ids from sync `CID`, *F*'s `accountId` and held positions' `CID` has exactly one
   member, equal to the run's scope (else `account_conflict`, or `account_unestablished` when empty).
3. **Fields.** Each field the binding checks read (the rows of the field table marked *bind*) is present and in its
   domain. Keys are matched case-insensitively, so `orderID` and `orderId` are one field; two spellings of one field
   with different values in one payload are `field_conflict:<name>`. Enum values are compared case-insensitively.
   Absent is never zero: a missing field fails as `missing_field:<name>`, a value outside its domain as
   `invalid_field:<name>`.
4. **Product.** *F*'s `asset.settlementType` and the sync `settlementTypeID` of every position observed open by the sync
   (stored or supplemental) must all map to one product (else `settlement_conflict`). The mapping is closed:

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
5. **Lookup binding.** *F*'s `orderId` and every execution's `openingData.orderId` equal the order id; its
   `asset.instrumentId` equals the order's broker instrument id; its executions' `positionId` values are distinct and
   equal, as a set, the order's position ids (else `lookup_mismatch:<what>`).
6. **Amendments.** An amendment observation of any of the order's events adds `source_amended`; differing `found` bodies
   add `lookup_amended` (Design 3).

If checks 1–6 pass and the product is not `real`, the order is `excluded_non_share_product`: established, bound,
unamended, and outside share identification. No further checks run and it has no lots; 3d reports it as omitted. Any
failure in checks 1–6 makes the order `unresolved` whatever its product.

**Checks for real-asset orders.** The remaining fields of the field table (marked *real*) are checked as in check 3.

7. **Shape.** A filled, long, unleveraged, uncopied buy:
   - *F*: `status.name` `Filled`, `transaction` `buy`, `action` `open`, `asset.side` `long`, `asset.leverage` 1,
     `orderCurrency` `usd`;
   - every sync payload: `isBuy` true, `leverage` 1, `mirrorID` 0, `parentPositionID` 0;
   - every history payload: `isBuy` true, `leverage` 1, `socialTradeId` 0, `parentPositionId` 0.

   Else `unsupported_shape:<field>`.
8. **Executions.**
   - **With *F*:** exactly one distinct opening: every execution's `openingData`, normalised over `executionTime`,
     `openTime`, `units`, `avgPrice`, `avgConversionRate`, `fees`, `taxes` and `orderId`, is equal (else
     `multiple_executions`). Every execution's `state` is `open` or `closed` (else `invalid_field:state`). With more
     than one execution, every one must be `closed` (else `unsupported_shape:partially_open_split`: no such case is
     observed, fact 5). Every execution binds to the record: `closed` → positive `remainingUnits`, no held position,
     at least one stored close row for its position, and those rows' units summing to `remainingUnits`; `open` → a
     held position whose units equal `remainingUnits` (else `piece_coverage_incomplete`). Every piece the broker lists
     is then accounted for by positive stored closes or a held position, which is what makes the shared opening one
     execution.
   - **Without *F*:** exactly one position, observed open by the sync with `isPartiallyAltered` false (else
     `executions_unestablished`).
   - **Acquisition date.** Every time candidate — *F*'s `executionTime` and `openTime`, sync `openDateTime`, each
     history row's `openTimestamp` — falls on one Europe/London date (else `acquisition_date_conflict`). Each close
     row's `executed_at` is at or after the acquisition time (else `close_before_open`).
9. **Quantities and money.**
   - **Charges and adjustments.** Every indicator observed is zero: *F*'s `totalCosts` and each execution's `fees` and
     `taxes`; every sync payload's `totalExternalFees` and `totalExternalTaxes`; each held position's `total_fees`
     and each archived `total_fees`; every close row's `fees`. Else `charges_nonzero:<indicator>`. Indicators are
     required where their source exists (a sync payload, a held or archived row, *F*, a close row); a position never
     observed by the sync has no `totalFees` observation, which is recorded, not failed (Limits).
   - **Units.** Execution units (*F*'s `openingData.units`, else the sync `initialUnits`) = Σ close-row units + Σ held
     units of the order's positions, exactly (else `units_not_conserved`).
   - **Consideration.** Every stored initial investment (sync `initialAmountInDollars`, history `initialInvestment`) is
     equal (else `consideration_conflict`). That amount is the acquisition's `usd_amount`.
   - **Money.** Σ close rows' `investment` + Σ held positions' `amount` = the consideration, exactly (else
     `investment_not_conserved`). The PnL page documents `amount` as the initial investment plus additional margin; a
     long x1 real position has no margin, and all 10 held real-id positions show `amount = initial_amount_in_dollars`
     (fact 7).
   - **Against the execution.** |units × rate × conversion − consideration| ≤ 0.001 × consideration, with rate and
     conversion from *F*'s `avgPrice` and `avgConversionRate`, else the sync `openRate` and `openConversionRate` (else
     `consideration_implausible`). The conversion rate turns the pricing currency into USD, so this holds in any pricing
     currency.
   - **Pricing currency of the disposals.** An order with close rows needs *F* with `asset.currency` `USD` (else
     `pricing_currency_unestablished` with no *F*, or `close_conversion_unestablished` when *F* names another
     currency): the history row carries no close conversion (fact 10).
   - **Proceeds against the execution.** For each close row, |units × closeRate − (investment + netProfit)| ≤ 0.001 ×
     (investment + netProfit) (else `proceeds_implausible`).
   - **Instrument.** The app's `instrument_id` for the order's events is one non-NULL value (else
     `instrument_unresolved`): 3d matches by it.

An order with no failing check is `admitted`. Any failing check makes it `unresolved`, with no lots: an order is never
part-admitted.

**Field table.** "Ts" is an ISO 8601 timestamp with an offset or `Z`, normalised to UTC; "id" is a positive integer;
"money" fits `NUMERIC(20,4)`; "units" is positive and fits `NUMERIC(20,8)`; "rate" is positive and fits
`NUMERIC(20,10)`; every number is finite. *bind* fields are required of every order; *real* fields only of real-asset
orders, after the exclusion point. Each is required where its source is present.

| phase | source | field | domain |
|---|---|---|---|
| bind | sync | `CID`, `orderID`, `positionID`, `instrumentID` | id |
| bind | sync | `settlementTypeID` | a key of the product mapping |
| bind | history | `orderId`, `positionId`, `instrumentId` | id |
| bind | history close | `units` | units |
| bind | history close | `closeTimestamp` | Ts |
| bind | lookup | `orderId`, `accountId`, `asset.instrumentId`, executions' `positionId`, `openingData.orderId` | id |
| bind | lookup | `asset`, each execution, `openingData` | object |
| bind | lookup | `positionExecutions` | non-empty array |
| bind | lookup | `asset.settlementType` | a key of the product mapping |
| bind | `broker_positions` | payload `CID` | id |
| real | sync | `isBuy`, `isPartiallyAltered` | boolean |
| real | sync | `leverage` | positive integer |
| real | sync | `mirrorID`, `parentPositionID` | non-negative integer |
| real | sync | `initialUnits` | units |
| real | sync | `initialAmountInDollars` | money, positive |
| real | sync | `totalExternalFees`, `totalExternalTaxes` | money, non-negative |
| real | sync | `openRate`, `openConversionRate` | rate |
| real | sync | `openDateTime` | Ts |
| real | history | `isBuy` | boolean |
| real | history | `leverage` | positive integer |
| real | history | `socialTradeId`, `parentPositionId` | non-negative integer |
| real | history close | `investment` | money, positive |
| real | history | `initialInvestment` | money, positive |
| real | history close | `netProfit` | money (any sign) |
| real | history close | `fees` | money, non-negative |
| real | history | `openRate` | rate |
| real | history close | `closeRate` | rate |
| real | history | `openTimestamp` | Ts |
| real | lookup | `status.name`, `transaction`, `action`, `asset.side`, `asset.currency`, `orderCurrency` | non-empty string |
| real | lookup | executions' `state` | `open` or `closed` |
| real | lookup | `asset.leverage` | positive integer |
| real | lookup | `totalCosts` | money, non-negative |
| real | lookup | `openingData.units`, executions' `remainingUnits` | units |
| real | lookup | `openingData.avgPrice`, `openingData.avgConversionRate` | rate |
| real | lookup | `openingData.fees`, `openingData.taxes` | money, non-negative |
| real | lookup | `openingData.executionTime`, `openingData.openTime` | Ts |
| real | `broker_positions` | `units` | units or 0 |
| real | `broker_positions` | `amount`, `total_fees` | money (`amount` non-negative) |
| real | `broker_positions_closed` | `total_fees` | money |

**10. Lots and keys.** An admitted order yields one acquisition lot and one disposal lot per close row:

| field | acquisition | disposal |
|---|---|---|
| `lot_key` | `acq:<environment>:<order_id>` | `dis:<environment>:<position_id>:<executed_at>`, the identity of `uq_trade_events_close` |
| `position_ids` | every position of the order | the close row's position |
| `trade_event_ids` | every open event of the order | the close row |
| `executed_at` | *F*'s `executionTime`, else the sync `openDateTime` | the close row's `executed_at` |
| `units` | execution units | the close row's `units` |
| `usd_amount` | the consideration | the close row's `investment + netProfit` |
| `instrument_id`, `etoro_instrument_id`, `order_id`, `subject` | the order's | the order's |

`uk_date` is the Europe/London date of `executed_at`; `tax_year` the UK tax year containing it. `fingerprint` is the
SHA-256 of canonical JSON over exactly these keys: `canonical_version` (`1`), `evidence_policy_id`, `lot_key`,
`subject`, `order_id`, `instrument_id`, `etoro_instrument_id`, `direction`, `position_ids`, `trade_event_ids`,
`executed_at`, `uk_date`, `tax_year`, `units`, `usd_amount`, `consideration_status`, `fx_rate_date`, `fx_status`,
`fx_observation_date`, `fx_usd_per_gbp`, `fx_gbp_per_usd`, `fx_policy_id`, `amount_gbp`. Keys sorted; arrays sorted
ascending; NULL as JSON null; timestamps as UTC ISO 8601 with microseconds; each decimal as `format(d, 'f')` with
trailing fractional zeros and a trailing point removed, inside the Design 1 context (never `normalize()`).

**11. Policy.** `TAX_EVIDENCE_POLICY_ID = "broker_evidence_v1"` names the checks, the field table, the product mapping, the
normalisation, the tolerances, the candidate rules and the keys. Changing any of them is a new policy id.

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
2. Scope pre-check: a `trade_ledger_scope` row exists and its environment equals the provider's (else `refused`,
   `scope_unestablished` or `scope_mismatch`). Nothing is asked of the broker before this passes.
3. Capture lookups (Design 3).
4. Load the FX state on the idle connection.
5. In one `snapshot_read` (REPEATABLE READ) block, read the scope, the environment's `trade_events` and observations,
   the found lookups, the latest and latest `ok` history fetches, `broker_positions` and the latest
   `broker_positions_closed.total_fees` per position, with their watermarks.
6. Scope. Every held real-id position's `CID` equals the scope's account (else `refused`, `positions_out_of_scope`).
   Per-order account checks are Design 4.2.
7. Derive every subject (Design 4) and its sterling (Design 5), in memory.
8. Publish in one READ COMMITTED transaction: call `tax_fx_state_is_current(conn, state)`; if false, the run is
   `refused` (`fx_state_changed`). Otherwise set the run `published` with its scope, watermarks and counts, and insert
   its subjects and lots.

**Terminal states.** A refusal rolls back, then sets the run `refused` with its reason in its own transaction. An
exception anywhere rolls back, sets the run `abandoned` with the exception's class and message in its own transaction,
and re-raises. Either way the `finally` releases the lock, so the connection is reusable.

The effective generation is the latest `published` run whose `(environment, account_id)` equals `trade_ledger_scope`.
Earlier generations stay for audit; a reader diffs two by `lot_key` and `fingerprint`. Because the lock covers the whole
run, no older snapshot can publish after a newer one. A generation is internally consistent because it comes from one
snapshot and one FX state, written in one transaction, and its subjects record exactly what was read.

### 7. Job

`tax_broker_evidence_refresh` runs daily at 12:13 UTC, after `tax_fx_refresh` (11:43) and the morning portfolio syncs,
on the existing `db` lane, so no lane or connection is added. It builds its provider the way `daily_portfolio_sync`
does. It raises, so `_tracked_job` records `failure`, on a `refused` or `abandoned` run; a `skipped` run logs and
returns. Registration mirrors `tax_fx_refresh`: `SCHEDULED_JOBS`, the lane map in `app/jobs/sources.py`, and
`_INVOKERS`.

### 8. Retiring the fills route and `fx_rates`

- `_ingest_fills` and `_load_fx_rate` are deleted. `fills` is the app's own order record (`order_client._persist_fill`,
  `app/api/orders.py`), a subset of the account's trades; reading it beside the broker record would count those trades
  twice.
- `ingest_tax_events` keeps its signature and returns zero fills. `daily_tax_reconciliation` keeps running the matcher
  over `tax_lots`, which now has no writer; 3d moves matching onto the effective generation and retires or reshapes
  `tax_lots`.
- `fx_rates` is dropped. The settled decision "Own EOD NAV-snapshot table (#1594)" names `fx_rates` and `_load_fx_rate`
  as the tax path ECB rows must stay out of; its wording is updated to name the 3b tables. The decision is unchanged.
  The sync layer named `fx_rates` is a different thing and is untouched.
- The tests that seed `fx_rates` or `fills` for the tax path are rewritten or removed with the code they test; the
  matcher's pure tests stay.

## Local policy (no published rule; frozen under `TAX_EVIDENCE_POLICY_ID`)

- **Supported shapes only.** One execution per order, either one position or fully closed pieces; every charge and
  adjustment indicator zero; filled long x1 uncopied real assets; USD-priced close pieces. Each extension needs an
  observed case and a new policy id.
- **The broker's stated amounts are recorded, not yet reconciled** (Source rules); 3d owns reconciliation.
- **A shared opening is one execution** when every listed piece is bound to positive stored closes or a held position
  (Design 4.8).
- **Tolerances of 0.1%.** About four times the largest measured proceeds gap (fact 10) and ten times the largest
  acquisition gap (fact 8). They catch a scale or currency error or a misread field, not cents.
- **An order is admitted whole or not at all.**
- **Early lookup capture, retries and the settling lookup** (Design 3), against an undocumented and possibly age-based
  retention.
- **The environment backfill** rests on three stated assumptions (Design 2).
- **One environment and account per ledger**, fixed at ingestion.

## Not in this slice, and limits

- **3d:** reconciling the broker's stated amounts as consideration; matching from the effective generation, including
  same-day aggregation; security identity across listings; tax character and its effective periods; excess reportable
  income; dividends; how unresolved and excluded subjects gate a published result; how history coverage is labelled;
  the tax-year view; the page.
- **History completeness.** The stored record starts at the first event the sync saw; coverage before the first
  recorded `ok` fetch is unknown. The history endpoint documents no page totals, and #2991 records that its lookback
  rule cannot be met. An order absent from `trade_events` is invisible unless a position of it is still held (orphans).
  The documented closed-position counts endpoint identifies the caller by GCID, not by account, so it is not used as a
  completeness check.
- **Closes while nothing is held.** History arriving in a batch with no open positions is not written until a later
  batch carries the account (Design 2). Closes older than the endpoint then serves are lost; this is visible as
  `scope_unverified` fetch rows.
- **Precision of stored payloads.** Ledger payloads already passed through a binary double (fact 17); a broker value
  beyond a double's precision could have been altered before storage. Lookup bodies keep their text.
- **Undetectable adjustments.** An amount the broker changed without changing the investment, the units, the prices or a
  documented charge field is not seen. A position the sync never observed has no `totalFees` observation (7 today).
- **Amendments** older than the 7-day history overlap are never re-observed (fact 16), and a lookup changed after its
  settling lookup is not observed.
- **Partial closes on the sync side.** Whether a partial close changes a stored sync open's `initialUnits` or another
  compared field is not yet observed; if it does, those orders are `source_amended` at acceptance and stay unresolved
  until a new policy.
- **Retention.** An order whose lookup failed before capture keeps only its sync evidence: the 8 orders of 2025 today.
  Without a lookup, an order with closes cannot establish its pricing currency.
- **Close conversion.** Non-USD close pieces stay unresolved until a close's executed conversion or cash credit is
  captured. The private stream carries close events, but on another account (fact 11).
- **Demo.** All evidence today is from the demo account; nothing here is a real tax figure.

## Tests

Pure (`tests/test_tax_broker_evidence.py`), each asserting outcome, reasons and lots:
- the AAPL split shape (USD, two closed pieces): admitted, one acquisition and two disposals, built per the Design 4.10
  table;
- the IUSA.L split shape: one opening reconstructed from two executions, then `unresolved` with
  `close_conversion_unestablished`, no lots;
- a split with one piece still `open`: `unsupported_shape:partially_open_split`;
- capture before a partial close, then a refresh listing the new piece: not `lookup_amended`; a refresh that drops a
  piece, changes `openingData` or `totalCosts`, or drops a field: `lookup_amended`;
- two identical executions where one `closed` piece has no stored close, with `remainingUnits` positive and with it 0:
  `piece_coverage_incomplete` both times;
- an order with two different openings: `multiple_executions`;
- exclusion: a CFD order with every binding check passing is excluded; a CFD order with a lookup instrument mismatch, a
  quarantined position or an amendment is `unresolved`, not excluded; an unknown `settlementType` is `invalid_field`;
- every other failing check in Design 4, including: a missing `fees` (not zero); `orderID` and `orderId` disagreeing in
  one payload; a close row's `units` column differing from its payload; events disagreeing on instrument; a lowercase
  enum; a CFD/REAL conflict; a position without a sync observation and no lookup; a lookup missing one stored position
  or listing one twice; `openingData.orderId` differing; `status` not `Filled`; an unknown execution `state`; a copied
  history row; a supplemental sync observation with `mirrorID` non-zero beside a history-first open; mixed acquisition
  dates; a close before its open; an account conflict; a non-zero `totalCosts` with zero execution fees; a non-zero sync
  `totalExternalFees` beside a zero lookup fee; a non-zero held or archived `total_fees`; a no-lookup order with closes;
  a proceeds sum outside `NUMERIC(20,4)`; an unresolved app instrument; amendments of each kind;
- every reason recorded when several checks fail, and dependent checks skipped;
- partition: an event with no order id beside events naming one (joins the order, `missing_field:order_id`); two events
  of one position naming different orders (one position subject, both orders `position_quarantined`); an orphan held
  position; every event in exactly one subject;
- arithmetic at the boundary: a `NUMERIC(20,4)` maximum times a `NUMERIC(20,10)` maximum is stored and fingerprinted
  without rounding; a rounding raises;
- the fingerprint is stable under array reordering, excludes `run_id`, `fx_run_id` and itself, and changes with
  `evidence_policy_id`;
- lookup classification: every row of the response table, including a non-JSON 200, a JSON array 200 and `{}`;
- lookup candidates: every row of the candidate table, the 30/10 split with spill-over, and that an order failing every
  run does not keep a retry candidate waiting longer than ⌈queue / 10⌉ runs;
- FX: `published`, `carried_forward`, `pending` (lot stored, `amount_gbp` NULL), `unavailable` (run refused).

DB (`tests/test_tax_broker_evidence_db.py`), one per new SQL mechanism:
- a run over seeded `trade_events`, lookups and `broker_positions` publishes one generation; a second run with new
  evidence publishes a second, and the first is unchanged;
- a second run started while the first holds the lock returns `skipped`; a stale `in_progress` row is marked
  `abandoned`; after a run raises, the lock is free and the connection runs another query;
- `tax_fx_state_is_current` false at publication: run `refused`, nothing but the run row written;
- no scope row: `refused` (`scope_unestablished`) with no lookup attempted; a held position of another account:
  `positions_out_of_scope`;
- the migration guard refuses a non-empty `tax_lots`; the backfill guard refuses a non-demo credential and an empty
  access log; the scope backfill refuses two sync `CID`s;
- `record_trade_events`: a first batch with a `CID` establishes the scope; a batch with another environment or `CID`
  writes no `trade_events` row and a `scope_refused` fetch row; a batch with history but no open positions writes a
  `scope_unverified` fetch row and no ledger row, and the next batch with the account ingests those closes; the
  portfolio sync commits in every case;
- `ingest_trade_events` writes an amendment for a history close re-observed with a different `investment` or a missing
  field, and a supplemental observation for a history open beside a sync open in both arrival orders (once, not on
  every re-observation);
- `record_trade_events` writes a `failed` fetch row for a failed fetch, with its `min_date` and `portfolio_fetched_at`.

Provider calls are faked; no test reaches the broker.

## Acceptance (dev DB)

After the build merges, the jobs process reloads and one run publishes. The counts below are expectations from the
measured checks (facts 2–14). The script did not run the partition, identity, field, shape, date, amendment, scope or
instrument checks as the build will, so the first run is the full-population derivation; any difference from these
counts is investigated before 3d starts.
- `trade_ledger_scope`: one row, environment `demo`, account equal to the sync `CID` (compared on the DB, not printed).
- `tax_order_lookups`: 21 attempts from the first run, 13 `found` and 8 `not_found`, plus one per order opened since
  2026-10-13.
- `tax_evidence_subjects` for the run, over the 21 orders known on 2026-10-13:
  - `admitted`: 8 (NXH, AAPL, SPY.RTH ×6);
  - `excluded_non_share_product`: 6 (VOO, QQQ, IEP ×2, SVXY, SVOL);
  - `unresolved`: 7. ILMN: `account_unestablished`, `settlement_unestablished`. GME ×2:
    `pricing_currency_unestablished`. CSPX.L ×3 and IUSA.L: `close_conversion_unestablished`.
- `tax_evidence_lots`: 8 acquisitions and 3 disposals (AAPL ×2, SPY.RTH ×1), all `broker_stated_unreconciled`, each
  disposal with `fx_status` `published` or `carried_forward`.
- One acquisition and one disposal checked by hand: units, USD amount, the 3b rate for the UK date (compared on the DB,
  not printed) and `amount_gbp`.
- `trade_events.environment` is `demo` on every row; `fx_rates` does not exist; `tax_lots` is empty;
  `trade_history_fetches` has a row from the first sync after deploy, `ok` if any position is open.

## Full-population verification

Measured over every stored order (21), event (36) and position (23), not a sample: product sources, each charge and
adjustment indicator, unit and money conservation, piece binding for the 13 found orders, copy and parent ids on both
sources, the two corroboration ratios, account agreement and the lookup shapes (`--checks`, `--facts`, default mode).
ILMN is unmeasured beyond its lookup status. Not yet run as the build will: partition, identity, field domains, dates,
amendments and instrument resolution; they run on the full population for the first time at acceptance, which is why
the acceptance counts are expectations.

## Finding routing (slice 3 checkpoint-1 round 1)

| finding | where |
|---|---|
| 8 one acquisition per order | Design 4.8 (one execution, every piece bound), facts 5–6 |
| 9 settlement union | Design 4.4 (closed mapping), fact 13 |
| 10 acquisition charges | Design 4.9 (every indicator zero), fact 9 |
| 11 proceeds identity | Source rules (stated, unreconciled), Design 4.9 (USD only, corroborated), fact 10 |
| 12 excluded lots cannot become eligible | Design 6 (full rebuild per generation) |
| 13 later failure, changed values | Design 2 (observations), 3 (re-lookup, settling lookup), 4.6, 6 |
| 14 account provenance | Design 2 (ledger scope at ingestion), 4.2, 6, facts 11–12 |
| 19 facts attributed to the script | each fact names its mode, output file or code |
| 20 tautological units | Design 4.9: execution units from the lookup or sync |
| 21 timestamps and lineage | Design 4.1, 4.8 |
| 22 full population ≠ full history | Design 2 (fetch outcomes), Limits, orphan subjects |
| 23 missing and invalid values | Design 4.3 and the field table |
| 32 existing fill lots | Design 1 guard, Design 8 |
| 34 precision (quantities, money) | Design 1 precision, fact 17 |
| 35 refused events invisible | Design 4 partition, `tax_evidence_subjects` |
| 39 snapshot and publication | Design 6 |
| 41 schema spellings | Design 4.3, fact 15 |

## Security

No order, capital or authorisation path changes. The new broker call is the documented read-only `orders:lookup`, already
used by order reconciliation and metered on its shared quota lane, and it runs only after the scope pre-check. The
ingestion scope check refuses ledger writes only; it never blocks the portfolio sync. The writes are the job's own
tables, one new column and three new tables beside `trade_events`. Account identifiers stay in the database and the
gitignored `var/`: the repo is public, so no lookup body, account id or customer id is committed.
