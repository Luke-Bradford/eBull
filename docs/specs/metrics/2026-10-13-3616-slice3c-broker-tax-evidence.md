# The account's broker trades as UK tax evidence (#3616 slice 3c)

Source: #3616 slice 3, split on 2026-10-13 into 3a (matcher, #3789), 3b (tax FX, #3790/#3793), 3c (this) and 3d. The
routing of the slice 3 checkpoint-1 round 1 findings is on #3616 (2026-10-13 13:46Z); "Finding routing" at the end maps
the findings this slice owns to its sections.

**What this slice does.** It turns the account's broker record into tax evidence: one acquisition per order execution
and one disposal per broker close piece, each with its account, environment, product, units, US-dollar amount and
sterling value at the 3b rate, or the reasons it cannot be admitted. Each run rebuilds the whole set from evidence and
publishes it as one generation.

**It admits only shapes it has verified, and refuses the rest with a reason.** The supported shape is a long,
unleveraged, uncopied real-asset order with one execution (which may be split into several positions by partial
closes), zero documented charges, and close pieces with zero fees. Every other shape is recorded `unresolved` with its
reason until a later slice verifies it on observed data.

**What it does not do.** It changes no published tax figure. Matching, security identity across listings, tax
character, excess reportable income, the tax-year view and the page are slice 3d. Until 3d, nothing reads the
generations except tests and the acceptance queries. This slice also retires the `fills` route into `tax_lots` and the
unused `fx_rates` table.

## Facts established first

"Script" is `scripts/measure_3616_slice3c_order_lookup.py`, run on 2026-10-13 against the dev DB and the demo account.
- The default mode makes one `orders:lookup` GET per stored order and saves the raw bodies to the gitignored
  `var/research/3616/slice3c/order_lookup_raw.json`, because they carry account identifiers and the repo is public.
- `--checks` recomputes the per-order unit and price checks from the saved bodies and the DB, with no network.
- `--facts` prints the DB facts below as counts and dates, never an identifier.

1. **The stored broker record holds 21 orders and 36 events** (`--facts`): 16 `etoro_sync/open`, 7 `etoro_history/open`,
   13 `etoro_history/close`. The `order_id` column is filled on all 20 history rows and on none of the 16 sync rows,
   which carry it in `raw_payload->>'orderID'` (default mode groups all 36 rows into 21 orders). This is the stored
   population, not the account's complete history (Limits).
2. **The order lookup answered for recent orders only** (default mode, `--facts`). 13 orders returned HTTP 200, opened
   2026-09-16 to 2026-10-09. 8 returned 404 with `"Order category for <id> not found"`, opened 2025-08-12 to 2025-11-14.
   No stored order was opened between those ranges. Whether the 404s are age-based retention, and where its boundary
   lies (somewhere from 27 to 333 days on this evidence), is a hypothesis. The portal page states no retention. Early
   capture is therefore precautionary policy (Design 3).
3. **The lookup carries what the position payloads do not** (default mode; portal page
   `trading--demo/get-order-information-and-position-details`): `accountId`, `asset.instrumentId`,
   `asset.settlementType`, `asset.currency`, `asset.leverage`, `asset.side`, `transaction`, `status`, and per position
   execution `positionId`, `state` and `openingData.{executionTime, openTime, units, avgPrice, avgConversionRate, fees,
   taxes}`. Observed `settlementType` values are `REAL` and `CFD`; the page documents `cfd`, `real`, `realFutures` and
   `marginTrade`. The lookup establishes the product of the four orders the sync never observed open: CSPX.L ×3 and AAPL
   are `REAL`.
4. **A partial close splits a position, and the lookup lists the pieces with one shared opening** (default mode). IUSA.L
   order 383293149 and AAPL order 383320185 each return two executions whose `openingData` objects are identical, each
   carrying the whole order's units (0.646048 and 0.147071). Summing `openingData.units` over executions double-counts
   (1.292096 for IUSA.L). The history rows of the pieces carry `parentPositionId` 0, so the order id is the only stored
   lineage. All other 11 found orders have one execution. ⚠ The sql/194 and `app/services/trade_events.py` docstrings say
   a partial close reduces the same `positionId`; on this account it mints a new id. The build corrects the Python
   docstring; the applied migration's comment stays, with a pointer from the Python one.
5. **Units are conserved per order** (`--checks`). For the 20 orders whose acquired units are known (lookup
   `openingData.units` of the one distinct opening, or sync `initialUnits`), acquired units equal closed units plus the
   units `broker_positions` holds under the order's positions, exactly at stored precision. The 10 real-id
   `broker_positions` rows all appear in `trade_events`. ILMN (order 272136682) has neither a lookup nor a sync
   observation.
6. **The broker's USD investment is plausible against price × units** (`--checks`). For all 20 orders with known units,
   the stored initial investment (sync `initialAmountInDollars`, history `initialInvestment`; where both exist they
   agree) is within a relative 0.000094 of `units × open rate × open conversion rate`. This is a plausibility check, not
   a reconciliation: a gap of that size is consistent with the spread, and nothing smaller than the tolerance is
   detectable by it.
   - `frozenAmount` is not a reconciliation either. It differs from investment + charges by up to $0.52 (order
     383458257); the page documents it as "the amount frozen/reserved for the order including costs", which is the
     request.
7. **Documented charges.** The PnL page documents `totalExternalFees` as *"Total fees in USD charged on the position.
   Example - TicketFee. This value does not include overnight fees and dividends"*, `totalExternalTaxes` as *"Total
   taxes in USD charged on the position. Example - SDRT"*, and `totalFees` as *"Total overnight fees and dividends
   charged/paid on the position"*. The lookup documents `openingData.fees` as *"Fees charged for the execution"* and
   `taxes` as *"Taxes applied to the execution"*. On the stored population (`--checks`), the 14 real-asset orders with a
   lookup or a sync observation show zero fees and zero taxes; the two non-zero cases are CFDs (SVXY $1.50, SVOL $0.72).
   The history page documents close `fees` only as "the fees of the trade"; it is 0 on all 13 close rows.
8. **Disposal proceeds of USD-priced pieces are plausible against price × units** (`--checks`). For the 7 close pieces
   whose `units × closeRate` is in US dollars, `investment + netProfit` is within a relative 0.000234 of it. For the 5
   pieces priced in GBP or GBX (CSPX.L ×3, IUSA.L ×2), the history row carries no close conversion rate, and no stored
   payload carries one; the PnL page's `closeConversionRate` describes an open position's current valuation, not an
   executed close. ILMN's piece is not checked because its pricing currency is unknown.
   - The daily account snapshots hold one row per day (48 rows, 48 dates; `--facts`), so a close's cash credit cannot
     be isolated from other same-day movements.
9. **One account, and the private stream reports a different one** (`--facts`). The 16 sync rows carry one `CID`, none
   NULL; the 13 found lookups carry one `accountId`, none missing, equal to it. The 5 private-stream trading events in
   `broker_private_events` (`Trading.*`) carry a different `CID`, and none carries the sync's. The environment label
   therefore does not identify the account; why the stream differs is not investigated here (noted on the PR).
10. **Every broker credential this database has used is a demo credential** (`--facts`). `broker_credentials` holds two
    rows, `api_key` and `user_key`, both `demo`. `broker_credential_access_log` holds 264,884 rows from 2026-06-03, all
    joined to a demo credential; its foreign key is `ON DELETE SET NULL` and 0 rows have a NULL credential, so no
    credential used since logging began has been deleted. `trade_events.recorded_at` starts 2026-06-13. The sync job
    loads credentials for `settings.etoro_env` (`scheduler._load_etoro_credentials`), and credential reads are logged
    by `load_credential_for_provider_use`. Nothing records an in-place edit of a credential's `environment` column.
11. **Settlement type, where both sources speak, agrees** (default mode). For the 9 orders with both a sync
    `settlementTypeID` and a lookup `settlementType`, `1` pairs with `REAL` and `0` with `CFD` in every case. Orders by
    product: 20 established (14 real, 6 CFD), 1 unknown (ILMN).
12. **Field spellings differ between documentation renderings.** The stored sync payloads use `CID`, `orderID`,
    `settlementTypeID`, `mirrorID` and `parentPositionID`; one rendering of the PnL page shows those spellings and
    another shows `cid`, `orderId`, `settlementTypeId`, `mirrorId` and `parentPositionId`. History rows use `orderId`,
    `positionId` and `parentPositionId`.
13. **`trade_events` keeps the first observation of a row.** A later observation that disagrees on units or realised
    P&L is counted as `conflict_anomaly` and logged, not stored (`trade_events.ingest_trade_events`); other fields are
    not compared. The history fetch re-walks a 7-day overlap behind its watermark (`_WATERMARK_OVERLAP`). A failed
    history fetch is passed on as `None` (`fetch_trade_history_safely`) and is not stored anywhere.
14. **`tax_lots` and `fx_rates` hold 0 rows; `fills` holds 1, a demo stub** (`--facts`). `fx_rates` has no writer in
    `app/`, `scripts/` or `sql/` (grep `INTO fx_rates`); its one reader is `tax_ledger._load_fx_rate`, called only by
    `_ingest_fills`. `daily_tax_reconciliation` is on-demand only (the comment above the on-demand list in
    `scheduler.SCHEDULED_JOBS`).

## Source rules

- **Acquisition cost.** TCGA 1992 s38(1)(a): *"the amount or value of the consideration, in money or money's worth,
  given by him … wholly and exclusively for the acquisition of the asset, together with the incidental costs to him of
  the acquisition"*; s38(2) defines incidental costs (fees for the services of a broker, costs of transfer, stamp duty;
  HMRC CG15250). The account gives US dollars, so the consideration is the USD the order invested. This slice admits
  only orders whose documented charges are zero (fact 7), so it never classifies a charge; a non-zero charge is
  `unresolved` until its category is established.
- **Disposal consideration.** The amount received for the disposal (TCGA s15, s38(1)(c) for the costs of disposal):
  the USD returned to the account for the close piece. Only pieces with zero `fees` are admitted.
- **Conversion to sterling.** HMRC CG78310, applied by 3b: each USD amount is converted at the rate for the date it is
  given or received, taken from `resolve_tax_fx` under 3b's consumer contract (3b spec Design 3).
- **The unit of acquisition.** Share identification acts on acquisitions and disposals of securities of the same class
  by the same person in the same capacity (TCGA s104, s105, s106A; HMRC CG51555–CG51575). A position record is not an
  acquisition: one execution split into positions by a partial close is one acquisition (fact 4). Same-day
  aggregation and security identity are matching, in 3d.
- **The broker fields.** The eToro portal pages named in the facts. They document field meanings, not a tax
  reconciliation. Where a broker figure is used as consideration this slice applies a plausibility check (Design 4.6)
  and says so.

## Design

### 1. Storage (migration `sql/485_tax_broker_evidence.sql`)

The migration first raises, naming the table, unless `tax_lots` and `fx_rates` are both empty (fact 14). The guard
exists because no deployment other than dev is observed. It then:
- drops `fx_rates` (Design 8);
- adds `trade_events.environment`, backfills it and constrains it (Design 2);
- creates `trade_event_observation_conflicts` and `trade_history_fetches` (Design 2);
- creates the tables below.

**`tax_evidence_scope`.** One row, `(environment, account_id, established_at, established_by_run_id)`. It is written by
the first run that establishes exactly one account (Design 6) and never changed except by a reviewed migration.

**`tax_order_lookups`.** Append-only, one row per lookup attempt:
- `lookup_id` identity, `environment`, `order_id`, `requested_at`;
- `outcome`: `found`, `not_found`, `rate_limited`, `auth_failed`, `http_error`, `transport_error` or `malformed`;
- `http_status` (NULL for a transport error), `error_detail`;
- `account_id` (the body's `accountId`, NULL unless `found`), `body JSONB` (NULL unless `found` or `malformed`),
  `body_sha256`;
- `order_event_max_id`, the order's largest `trade_events.event_id` when the attempt was made.

**`tax_evidence_runs`.** One row per run, created at entry (Design 6):
- `run_id`, `environment`, `account_id` (the scope it ran under, NULL until established), `started_at`, `finished_at`;
- `state`: `in_progress`, `published`, `refused` or `abandoned`; `reason`;
- the evidence it read: `trade_events_max_event_id`, `lookups_max_id`, `conflicts_max_id`, `history_fetch_id` (the
  latest successful `trade_history_fetches` row) and `positions_read_at`;
- `fx_run_id`, `fx_policy_id`, `evidence_policy_id`, and `counts JSONB` per outcome.

**`tax_evidence_subjects`.** One row per subject per published run. A subject is an order (`order:<order_id>`) or, when
no order id can be established, a position (`position:<position_id>`):
- `run_id`, `subject`, `order_id` (NULL for a position subject), `instrument_id`, `product`;
- `outcome`: `admitted`, `excluded_non_share_product` or `unresolved`; `reasons TEXT[]` (every failing check, in Design
  4 order; empty when admitted);
- `trade_event_ids BIGINT[]`, `lookup_id`, `held_units JSONB` (position id → units read from `broker_positions`),
  `first_event_at`, `last_event_at`.
- Primary key `(run_id, subject)`.

**`tax_evidence_lots`.** One row per admitted acquisition or disposal per published run:
- `run_id`, `lot_key` (Design 4.7), `subject`, `order_id`, `instrument_id`, `direction`;
- `position_ids BIGINT[]`, `trade_event_ids BIGINT[]` (both sorted);
- `executed_at`, `uk_date`, `tax_year`;
- `units NUMERIC(20,8)`, `usd_amount NUMERIC(20,4)`;
- the FX fields of 3b's consumer contract: `fx_rate_date`, `fx_status`, `fx_observation_date`, `fx_usd_per_gbp`,
  `fx_gbp_per_usd NUMERIC(20,10)`, `fx_policy_id`, `fx_run_id`;
- `amount_gbp NUMERIC(34,14)`, the exact product `usd_amount × fx_gbp_per_usd`; NULL when `fx_status` is `pending`;
- `fingerprint` (Design 4.7).
- Primary key `(run_id, lot_key)`.

**Precision.** Inputs are read at the scales `trade_events` already enforces: units `NUMERIC(20,8)`, money
`NUMERIC(20,4)`. A lookup or payload value with more decimal places than its column, or outside its range, fails the
field check (Design 4.2) rather than being rounded. Nothing is apportioned in this slice: a supported order has one
execution, and a disposal's amount is its own row's. `amount_gbp` is the exact product of a 4-place and a 10-place
decimal, so 14 places and 20 integer digits hold it without rounding, computed under a Decimal context of 50 digits.
The rounding policy for matched and pooled amounts is 3d's, stated against these scales.

### 2. Provenance

**Environment on `trade_events`.** `sync_portfolio` and `record_trade_events` gain a required `environment` argument,
passed by both callers from the provider's environment (`settings.etoro_env` in `daily_portfolio_sync`, `self._env` in
the websocket re-sync); every row written carries it. The migration backfills existing rows to `demo` inside a guard that
raises unless all of these hold:
- `trade_events` is empty, or: no `broker_credentials` row has `environment <> 'demo'`; no
  `broker_credential_access_log` row has a NULL `credential_id`; and the earliest access-log row precedes the earliest
  `trade_events.recorded_at`.

The backfill rests on fact 10 and one assumption nothing can check: that no credential's `environment` column was
edited in place. It is recorded as local policy. The column is then `NOT NULL` with `CHECK (environment IN ('demo',
'real'))`.

**Account.** An order's account is the `CID` of its sync rows and the `accountId` of its found lookups (fact 9). Design
4.1 requires them to agree, and Design 6 requires every account seen to equal the scope's.

**History fetch outcomes.** `record_trade_events` writes one `trade_history_fetches` row per call: `fetch_id`,
`environment`, `fetched_at`, `min_date`, `outcome` (`ok` or `failed`) and `row_count`. A failed fetch (`None`) is now
visible. The run records the latest `ok` fetch, so a reader knows the history coverage interval the generation saw:
from `HISTORY_EPOCH` (the first fetch on an empty ledger) to that fetch's time, as far as the endpoint returns it.

**Amended observations.** When `ingest_trade_events` meets an existing row, it compares the new observation with the
stored one on that source's normalised fields:
- history close: `units`, `investment`, `netProfit`, `fees`, `closeRate`, `openTimestamp`, `orderId`;
- sync open: `initialUnits`, `initialAmountInDollars`, `openRate`, `openConversionRate`, `openDateTime`, `orderID`,
  `settlementTypeID`.

Only an observation from the same source and kind is compared: a history open beside a sync open is two sources
describing one position, which Design 4 corroborates, not an amendment. Fields are normalised by the rules of Design
4.2. Where any differs, a `trade_event_observation_conflicts` row is written in the same transaction:
- `conflict_id`, `event_id` of the stored row, `observed_at`, `source`;
- `stored_normalised JSONB`, `observed_normalised JSONB`, `observed_raw JSONB`.

The stored row is never changed (sql/194's contract). Fields outside these lists, such as `totalExternalFees`, which
the PnL page documents as a running total "charged on the position", are not compared.

### 3. Capturing order lookups

Lookups may expire (fact 2), so each run first asks the broker about the orders of its environment, through
`EtoroBrokerProvider.lookup_order`. That method is metered under the `D_order_info` quota lane shared with the other
order-info callers. Candidates are decided per order from its latest attempt (*A*), its latest `found` attempt (*F*)
and its current largest event id (*E*):

| state | ask? |
|---|---|
| no attempt | yes |
| *A* is `rate_limited`, `transport_error` or `http_error` | yes |
| *A* is `auth_failed` | no: the run stops asking and continues with stored evidence |
| *A* is `not_found` and *E* > *A*`.order_event_max_id` | yes |
| *A* is `not_found` otherwise | no |
| *F* exists and *E* > *F*`.order_event_max_id` | yes |
| *F* exists, the order has no held position, and no attempt was made at least 7 days after its last event | yes, once (the settling lookup) |
| otherwise | no |

Candidates are asked newest order first, at most 40 per run. An order not asked this run is asked by a later one; the
run records how many candidates it deferred.

Outcomes: HTTP 200 parsed by `lookup_order` → `found`. `BrokerOrderNotFound` (404) → `not_found`. HTTP 429 →
`rate_limited`, which also stops asking for the rest of the run. HTTP 401 or 403 → `auth_failed`. Other HTTP errors →
`http_error`. Transport errors → `transport_error`. `OrderDetailParseError` or a non-object body → `malformed`. Each
attempt is one `tax_order_lookups` row, committed as it lands.

Derivation uses *F*, the latest `found` body. A later `not_found` or error never discards it. When two `found` bodies of
one order differ on the normalised fields Design 4 reads, the order is `unresolved` with `lookup_amended`. A correction
made by the broker after the settling lookup is not observed (Limits).

Lookups are informational reads; the unattended broker-mutation guard does not apply to them.

### 4. Deriving the evidence for one order (pure)

`derive_order(order_id, events, lookups, held_units, conflicts) -> OrderEvidence` takes:
- the order's `trade_events` rows;
- its `found` lookup bodies, latest last;
- the units `broker_positions` holds under each of its position ids;
- its conflict rows.

It runs the checks below in order and records every one that fails. A check whose inputs an earlier failure left
undefined is skipped rather than failed, so `reasons` names causes, not their consequences.

1. **Account.** The set of account ids from sync `CID` and found-lookup `accountId` has exactly one member (else
   `account_conflict`, or `account_unestablished` when empty).
2. **Fields.** Each field read by a later check is present and in its domain (table below). Keys are matched
   case-insensitively, so `orderID` and `orderId` are one field; two spellings of one field with different values in one
   payload are `field_conflict:<name>`. Enum values are compared case-insensitively. Absent is never zero: a missing
   `fees` fails here as `missing_field:fees`; a value outside its domain fails as `invalid_field:<name>`.
3. **Product.** The lookup's `asset.settlementType`, and the sync `settlementTypeID` of every position observed open
   (`0` CFD, `1` real asset, `2` swap, `3` crypto margin trade, `4` future, per the PnL page), must all name one product
   (else `settlement_conflict`). With no found lookup, every position of the order must have a sync observation (else
   `settlement_unestablished`). The lookup documents one `settlementType` per order, so two products within one order
   are a contradiction, not two products.
   - A non-real product with checks 1–3 passing is `excluded_non_share_product`: established, and outside share
     identification, with no further checks and no lots. 3d reports those as omitted.
4. **Shape.** Long, unleveraged and not copied:
   - lookup: `transaction` `buy`, `asset.side` `long`, `asset.leverage` 1;
   - sync rows: `isBuy` true, `leverage` 1, `mirrorID` 0, `parentPositionID` 0;
   - history rows: `isBuy` true, `leverage` 1, `socialTradeId` 0, `parentPositionId` 0.

   Else `unsupported_shape:<field>`.
5. **Bindings and executions.**
   - The lookup's `orderId` equals the order id, its `asset.instrumentId` equals every event's `etoro_instrument_id`,
     and the set of its executions' `positionId` equals the set of the order's stored position ids (else
     `lookup_mismatch:<what>`).
   - **With a found lookup**, the order must have exactly one distinct opening: every execution's `openingData`,
     normalised over `executionTime`, `units`, `avgPrice`, `avgConversionRate`, `fees`, `taxes` and `orderId`, is
     equal (else `multiple_executions`, unsupported). Positions sharing it are pieces of that one execution.
   - **Without a found lookup**, the order must have exactly one position, observed open by the sync with
     `isPartiallyAltered` false (else `executions_unestablished`). Its execution is that observation.
   - **Acquisition date.** Every time candidate — lookup `executionTime` and `openTime`, sync `openDateTime`, each
     history row's `openTimestamp` — falls on one Europe/London date (else `acquisition_date_conflict`). The lot takes
     `executionTime`, else `openDateTime`. Each close row's `closeTimestamp` must be at or after it (else
     `close_before_open`).
6. **Quantities and money.**
   - **Charges:** the execution's `fees` and `taxes` (lookup), or the sync row's `totalExternalFees` and
     `totalExternalTaxes` (no lookup), are all zero, and every close row's `fees` is zero (else `charges_nonzero`).
   - **Conservation:** execution units = Σ close-row units + Σ held units of the order's positions, exactly (else
     `units_not_conserved`).
   - **Consideration:** every stored initial investment of the order (sync `initialAmountInDollars`, history
     `initialInvestment`) is equal (else `consideration_conflict`). That amount is the acquisition's `usd_amount`.
   - **Money conservation:** Σ close rows' `investment` + Σ held positions' `broker_positions.amount` = the
     consideration, to the cent (else `investment_not_conserved`). The PnL page documents `amount` as the initial
     investment plus additional margin; a long x1 real position has no margin, as every held real position shows (its
     `amount` equals its `initialAmountInDollars` unless split).
   - **Plausibility of the consideration:** |units × open rate × open conversion rate − consideration| ≤ 0.1% of the
     consideration, with the open rate and conversion from the lookup's `avgPrice` and `avgConversionRate`, else the
     sync row's `openRate` and `openConversionRate` (else `consideration_implausible`).
   - **Disposal amount:** each close row's `investment + netProfit`.
   - **Plausibility of the proceeds:** |units × closeRate − (investment + netProfit)| ≤ 0.1% of the proceeds (else
     `proceeds_implausible`). The history row carries no close conversion rate, so this compares in US dollars only:
     a piece priced in another currency fails unless that currency stood within 0.1% of the dollar, in which case the
     error it lets through is within the same tolerance. That is why the five GBP and GBX pieces fail (fact 8).
7. **Lots and keys.** An admitted order yields one acquisition lot and one disposal lot per close row.
   - Acquisition `lot_key` = `acq:<environment>:<order_id>`: a supported order has one execution.
   - Disposal `lot_key` = `dis:<environment>:<position_id>:<closeTimestamp>`, the identity of `uq_trade_events_close`.
   - `fingerprint` = SHA-256 of canonical JSON: keys sorted; every lot field except `run_id` and `fx_run_id`, plus
     `evidence_policy_id`; arrays sorted ascending; decimals as their normalised string with no exponent; timestamps as
     UTC ISO 8601 with microseconds.
8. **Amendments.** A conflict row for any of the order's events adds `source_amended`; two differing found lookups add
   `lookup_amended`.
9. **Policy.** `TAX_EVIDENCE_POLICY_ID = "broker_evidence_v1"` names the checks, the field table, the normalisation,
   the tolerances and the keys. Changing any of them is a new policy id.

An order with no failing check is `admitted`. Any failing check makes it `unresolved`, with no lots: an order is never
part-admitted.

**Subjects without an order.** An event whose order id is missing or unparseable becomes the subject
`position:<position_id>` with `missing_field:order_id` or `invalid_field:order_id`. A `broker_positions` real-id row with
no `trade_events` row becomes a subject `position:<position_id>` with `orphan_position`, unless its payload's order id
names an order already present, in which case that order gains `orphan_position`. There are none today (fact 5).

**Field table.** "Ts" is an ISO 8601 timestamp with an offset or `Z`, normalised to UTC; "id" is a positive integer;
"money" is a finite decimal fitting `NUMERIC(20,4)`; "units" is a positive finite decimal fitting `NUMERIC(20,8)`;
"rate" is a positive finite decimal.

| source | field | domain |
|---|---|---|
| sync open | `CID`, `orderID`, `positionID`, `instrumentID` | id |
| sync open | `settlementTypeID` | 0–4 |
| sync open | `isBuy`, `isPartiallyAltered` | boolean |
| sync open | `leverage` | 1 for admission (checked in 4.4); any positive integer here |
| sync open | `mirrorID`, `parentPositionID` | non-negative integer |
| sync open | `initialUnits` | units |
| sync open | `initialAmountInDollars`, `totalExternalFees`, `totalExternalTaxes` | money, non-negative |
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
| lookup | `orderId`, `accountId`, `asset.instrumentId`, executions' `positionId` | id |
| lookup | `transaction`, `asset.side`, `asset.settlementType`, `asset.currency` | non-empty string |
| lookup | `asset.leverage` | positive integer |
| lookup | `openingData.units` | units |
| lookup | `openingData.avgPrice`, `openingData.avgConversionRate` | rate |
| lookup | `openingData.fees`, `openingData.taxes` | money, non-negative |
| lookup | `openingData.executionTime`, `openingData.openTime` | Ts |
| `broker_positions` | `units` | units or 0 |
| `broker_positions` | `amount` | money, non-negative |

### 5. Sterling

For each lot, `resolve_tax_fx(state, uk_date)` (3b), with `state` from `load_tax_fx_state` loaded on an idle connection
before the snapshot (Design 6):
- `published` or `carried_forward`: the rate fields are stored and `amount_gbp` computed;
- `pending` (a date after the latest accepted observation, such as today's): the lot is stored with `amount_gbp` NULL and
  `fx_status` `pending`; the next run recomputes it;
- `unavailable` or `out_of_range` for any lot: the run is `refused` with that reason and publishes nothing.

Every run recomputes every lot from the current state, so 3b's staleness rule is met by construction: a lot computed
from an older state never enters a new generation.

### 6. A run

`run_tax_broker_evidence(conn, broker)`, on one connection:
1. Take the session-level `pg_try_advisory_lock(TAX_EVIDENCE_LOCK_KEY)`. If another run holds it, return `skipped`
   without writing; the lock serialises every run from start to publication. Holding it, mark any `in_progress` run row
   `abandoned` (its process died), then insert this run's row `in_progress` and commit.
2. Capture lookups (Design 3).
3. Load the FX state on the idle connection.
4. In one `snapshot_read` (REPEATABLE READ) block, read the environment's `trade_events`, the found lookups, the conflict
   rows, the latest `ok` history fetch, `broker_positions` and `tax_evidence_scope`, with their watermarks.
5. Scope. Every account id in the evidence must be one value (else `refused`, `account_ambiguous`). With no scope row,
   that value and the run's environment become the scope at publication. With a scope row, both must equal it (else
   `refused`, `scope_mismatch`). A real-environment run never reads demo rows, and the reverse, because step 4 filters
   by environment.
6. Derive every subject (Design 4) and its sterling (Design 5), in memory.
7. Publish in one READ COMMITTED transaction: call `tax_fx_state_is_current(conn, state)`; if false, the run is
   `refused` (`fx_state_changed`). Otherwise write the scope row if new, set the run `published` with its watermarks and
   counts, and insert its subjects and lots.
8. Release the lock. Any exception marks the run `abandoned` with the exception's class and message, then re-raises.

The effective generation is the latest `published` run whose `(environment, account_id)` equals the scope. Earlier
generations stay for audit; a reader diffs two by `lot_key` and `fingerprint`. Because the lock covers the whole run, no
older snapshot can publish after a newer one. A generation is internally consistent because it comes from one snapshot
and one FX state, written in one transaction, and its `held_units` record exactly what was read.

### 7. Job

`tax_broker_evidence_refresh` runs daily at 12:13 UTC, after `tax_fx_refresh` (11:43) and the morning portfolio syncs.
No daily job is scheduled at minute 13 (grep of `Cadence.daily(` in `scheduler.py`). It runs on the existing `db` lane,
so no lane or connection is added (the dev cluster's connection limit has no headroom), and builds its provider the way
`daily_portfolio_sync` does. It raises, so `_tracked_job` records `failure`, on a `refused` or `abandoned` run; a
`skipped` run (another holds the lock) logs and returns. Registration mirrors `tax_fx_refresh`: `SCHEDULED_JOBS`, the
lane map in `app/jobs/sources.py`, and `_INVOKERS`.

### 8. Retiring the fills route and `fx_rates`

- `_ingest_fills` and `_load_fx_rate` are deleted. `fills` is the app's own order record (`order_client._persist_fill`,
  `app/api/orders.py`), a subset of the account's trades; reading it beside the broker record would count those trades
  twice.
- `ingest_tax_events` keeps its signature and returns zero fills. `daily_tax_reconciliation` keeps running the matcher
  over `tax_lots`, which now has no writer; 3d moves matching onto the effective generation and retires or reshapes
  `tax_lots`.
- `fx_rates` is dropped. The settled decision "Own EOD NAV-snapshot table (#1594)" names `fx_rates` and `_load_fx_rate`
  as the tax path ECB rows must stay out of; its wording is updated to name the 3b tables. The decision itself is
  unchanged.
- The tests that seed `fx_rates` or `fills` for the tax path are rewritten or removed with the code they test; the
  matcher's pure tests stay.

## Local policy (no published rule; frozen under `TAX_EVIDENCE_POLICY_ID`)

- **Supported shapes only.** One execution per order, zero documented charges, long x1 uncopied real assets. Each
  extension needs an observed case and a new policy id.
- **A shared opening is one execution.** eToro documents no partial-close lineage; the rule rests on fact 4 and is
  bounded by the conservation checks: two genuinely distinct executions with identical economic fields would show more
  closed and held units than the one opening holds.
- **Plausibility tolerances of 0.1%.** About four times the largest measured gap. They catch a scale or currency error
  or a misread field, not cents.
- **An order is admitted whole or not at all.**
- **Early lookup capture and the settling lookup** (Design 3), against an undocumented and possibly age-based
  retention.
- **The environment backfill** assumes no credential's `environment` was edited in place (Design 2).
- **The account scope** is fixed by the first run that establishes it.

## Not in this slice, and limits

- **3d:** matching from the effective generation, including same-day aggregation; security identity across listings;
  tax character and its effective periods; excess reportable income; how unresolved and excluded subjects gate a
  published result; the tax-year view; the page.
- **History completeness.** The stored record starts at the first event the sync saw. The history endpoint documents no
  page totals, and #2991 records that its lookback rule cannot be met. An order absent from `trade_events` is invisible
  unless a position of it is still held (the orphan check). The run records the history fetch it relied on; 3d decides
  how to label coverage.
- **Amendments** older than the 7-day history overlap are never re-observed (fact 13), and a lookup changed after its
  settling lookup is not observed.
- **Retention.** An order whose lookup failed before capture keeps only its sync evidence: the 8 orders of 2025 today.
  Of those, only ILMN lacks sync evidence.
- **Close conversion.** The five GBP and GBX close pieces stay unresolved until a close's executed conversion or cash
  credit is captured. The private stream carries close events, but on another account (fact 9).
- **Demo.** All evidence today is from the demo account; nothing here is a real tax figure.

## Tests

Pure (`tests/test_tax_broker_evidence.py`), each asserting outcome, reasons and lots:
- the IUSA.L and AAPL split shapes: one acquisition, two disposals;
- a split order still partly open: unit and money conservation against held units and `amount`, admitted, with the
  expected acquisition amount;
- an order with two different openings: `multiple_executions`;
- every failing check in Design 4, including a missing `fees` (not zero), `orderID` and `orderId` disagreeing in one
  payload, a lowercase enum, a CFD/REAL conflict, a position without a sync observation and no lookup, a lookup missing
  one stored position, a copied history row (`socialTradeId` non-zero), mixed acquisition dates, a close before its
  open, an account conflict, a non-zero ticket fee, a non-USD close piece, and amended observations of each kind;
- every reason recorded when several checks fail, and dependent checks skipped;
- the canonical fingerprint is stable under array reordering and changes with `evidence_policy_id`;
- lookup candidate selection: every row of the Design 3 table;
- FX: `published`, `carried_forward`, `pending` (lot stored, `amount_gbp` NULL), `unavailable` (run refused).

DB (`tests/test_tax_broker_evidence_db.py`), one per new SQL mechanism:
- a run over seeded `trade_events`, lookups and `broker_positions` publishes one generation and the scope; a second run
  with new evidence publishes a second, and the first is unchanged;
- a second run started while the first holds the lock returns `skipped`; a stale `in_progress` row is marked
  `abandoned` by the next run;
- `tax_fx_state_is_current` false at publication: run `refused`, nothing but the run row written;
- evidence with a second account: run `refused` (`account_ambiguous`); a scope mismatch is refused;
- the migration guard refuses a non-empty `tax_lots`; the backfill guard refuses a non-demo credential;
- `ingest_trade_events` writes a conflict row for a history close re-observed with a different `investment`, and none
  for a history open beside a sync open;
- `record_trade_events` writes a `failed` history fetch row for `None`.

Provider calls are faked; no test reaches the broker.

## Acceptance (dev DB)

After the build merges, the jobs process reloads and one run publishes. The counts below are expected from the measured
checks (facts 2–11, `--checks`, `--facts`); the script measured the account, product, execution, unit and price checks
but not the field, shape, date or amendment checks, so the first run is the full-population derivation, and any
difference from these counts is investigated before 3d starts.
- `tax_order_lookups`: 21 attempts from the first run, 13 `found` and 8 `not_found`, plus one per order opened since
  2026-10-13.
- `tax_evidence_scope`: one row, environment `demo`.
- `tax_evidence_subjects` for the run, over the 21 orders known on 2026-10-13:
  - `admitted`: 10 (GME ×2, NXH, AAPL, SPY.RTH ×6);
  - `excluded_non_share_product`: 6 (VOO, QQQ, IEP ×2, SVXY, SVOL);
  - `unresolved`: 5. ILMN has `account_unestablished`, `settlement_unestablished` and `executions_unestablished`; CSPX.L
    ×3 and IUSA.L have `proceeds_implausible`.
- `tax_evidence_lots`: 10 acquisitions and 5 disposals (GME ×2, AAPL ×2, SPY.RTH ×1), each disposal with `fx_status`
  `published` or `carried_forward`.
- One acquisition and one disposal checked by hand: units, USD amount, the 3b rate for the UK date (compared on the DB,
  not printed) and `amount_gbp`.
- `trade_events.environment` is `demo` on every row; `fx_rates` does not exist; `tax_lots` is empty;
  `trade_history_fetches` has an `ok` row from the sync after deploy.

## Full-population verification

The unit, price, product, account and lookup checks were run over every stored order (21) and event (36), not a sample
(`--checks`, `--facts` and the default mode). The remaining checks run on the full population for the first time at
acceptance, which is why the acceptance counts are expectations to be confirmed, not results.

## Finding routing (slice 3 checkpoint-1 round 1)

| finding | where |
|---|---|
| 8 one acquisition per order | Design 4.5 (one execution supported, others refused), fact 4 |
| 9 settlement union | Design 4.3, fact 11 |
| 10 acquisition charges | Design 4.6 (non-zero refused), fact 7, source rule |
| 11 proceeds identity | Design 4.6 (plausibility, stated as such), fact 8 |
| 12 excluded lots cannot become eligible | Design 6 (full rebuild per generation) |
| 13 later failure, changed values | Design 2 (conflicts), 3 (re-lookup, settling lookup), 4.8, 6 |
| 14 account provenance | Design 2, 4.1, 6 (scope), facts 9–10 |
| 19 facts attributed to the script | each fact names its mode or code citation |
| 20 tautological units | Design 4.6 conservation uses the lookup or sync execution units |
| 21 timestamps and lineage | Design 4.5 |
| 22 full population ≠ full history | Design 2 (fetch outcomes), Limits, orphan subjects |
| 23 missing and invalid values | Design 4.2 and the field table |
| 32 existing fill lots | Design 1 guard, Design 8 |
| 34 precision (quantities, money) | Design 1 precision |
| 35 refused events invisible | `tax_evidence_subjects`, subjects without an order |
| 39 snapshot and publication | Design 6 |
| 41 schema spellings | Design 4.2, fact 12 |

## Security

No order, capital or authorisation path changes. The new broker calls are the documented read-only `orders:lookup`,
already used by order reconciliation and metered on its shared quota lane. The writes are the job's own tables, one new
column and two new tables beside `trade_events`, written by the existing writer. Account identifiers stay in the
database and the gitignored `var/`: the repo is public, so no lookup body, account id or customer id is committed.
