# eToro API Reference

Source of truth for how eBull integrates with eToro. Derived from the
official OpenAPI spec at `https://api-portal.etoro.com/api-reference/openapi.json`
(v1.279.0 — last verified 2026-07-04; the portal drifts fast, re-verify
before citing capabilities: `.claude/skills/data-sources/etoro-api.md`).

Portal: `https://api-portal.etoro.com/`
LLM index: `https://api-portal.etoro.com/llms.txt` (per-endpoint markdown at
`api-reference/<section>/<slug>.md`). Cloudflare-fronted — CLI `curl` gets
403; fetch with a browser-agent tool (WebFetch).

---

## Base URL

```
https://public-api.etoro.com
```

WebSocket: `wss://ws.etoro.com/ws`

NOT `https://api.etoro.com` (speculative URL from early development).

---

## Authentication

Every request requires **three headers**:

| Header | Purpose | Value |
|--------|---------|-------|
| `x-api-key` | Public API key (identifies the application) | From eToro Settings > Trading > API Key Management |
| `x-user-key` | User key (identifies the account) | From the same key management page |
| `x-request-id` | Unique request identifier | Fresh UUID per request |

**Not** Bearer token. **Not** a single API key. Two separate keys plus a request ID.

### Key generation

1. eToro account must be verified.
2. Go to Settings > Trading > API Key Management.
3. Create a key with: name, environment (Demo or Real), permissions (Read or Write).
4. Complete 2FA via SMS.
5. Copy both the public API key and the user key.

### Demo vs Real

Each key operates in exactly one environment. If you need both demo and
real, create separate keys. The trading endpoints have explicit `/demo/`
prefix for demo, or no prefix for real.

### Optional security

- IP whitelisting
- Key expiration dates

### Credential storage in eBull

eBull stores **two** values per eToro environment in `broker_credentials`:

| eBull label | eToro field | Purpose |
|-------------|-------------|---------|
| `api_key` | Public API Key (`x-api-key`) | Application-level auth |
| `user_key` | User Key (`x-user-key`) | Account-level auth |

Both stored with `provider='etoro'`, `environment='demo'|'real'`.
Loading: `load_credential_for_provider_use()` in `app/services/broker_credentials.py`.

---

## Rate limits

Two-tier system, tracked per user key over a **1-minute rolling window**:

| Tier | Limit | Applies to |
|------|-------|------------|
| **Market data** | **120 req/min shared** | Instruments, rates, candles, closing prices, search and lookup catalogues; live portal index verified 2026-08-09 |
| **Standard** | **60 req/min** | Ordinary portfolio/social/watchlist reads unless an endpoint declares another pool |
| **Heavy** | **20 req/min** | All POST/PUT/DELETE: trade execution, watchlist writes, social writes |

Exceeding returns **429 Too Many Requests**:
```json
{"errorCode": "TooManyRequests", "errorMessage": "Too many requests"}
```

### eBull throttle implementation

Configured in `ResilientClient` (`app/providers/resilient_client.py`):

| Operation | Inter-request interval | Effective rate | Headroom |
|-----------|----------------------|----------------|----------|
| GET (market data) | 1.1s | ~55/min | ~8% |
| GET (broker info) | 1.1s | ~55/min | ~8% |
| POST (trading) | 3.5s | ~17/min | ~15% |

Both read and write clients share `_last_request_at` so combined
GET+POST requests cannot exceed the API limit.

### Retry logic

- Max 3 retries (4 total attempts)
- Backoff schedule: 1s, 2s, 4s (exponential)
- 429: respects `Retry-After` header if present, otherwise backoff
- 5xx (500, 502, 503, 504): same exponential backoff
- Final attempt: raises `HTTPStatusError`

### Best practices

- Cache static data locally (instrument IDs are immutable)
- Batch rate requests (max 100 IDs per call; eBull uses 50 for safety)
- Sequence per-instrument calls with throttle delay
- Land every structured field in SQL — raw disk dumps for eToro were retired in #471 (`instruments` / `price_daily` / `quotes` / `exchanges` tables ARE the market-data audit trail). Automated broker mutations additionally retain one small latest response object on their material operation/order row; they do not append polling payloads.

---

## All endpoints (57 paths)

### Identity

| Method | Path | Description | eBull status |
|--------|------|-------------|-------------|
| GET | `/api/v1/me` | Returns `{gcid, realCid, demoCid}` | Not used (credential validation done via portfolio call) |

### Market data

| Method | Path | Description | eBull status |
|--------|------|-------------|-------------|
| GET | `/api/v1/market-data/instruments` | Instrument metadata by filters | **Active** — universe sync |
| GET | `/api/v1/market-data/instruments/rates` | Live bid/ask/last for up to 100 IDs | **Active** — quote refresh |
| GET | `/api/v1/market-data/instruments/{id}/history/candles/{dir}/{interval}/{count}` | OHLCV candles (max 1000) | **Active** — daily candles |
| GET | `/api/v1/market-data/instruments/history/closing-price` | Bulk closing prices (daily/weekly/monthly) | Not used — candles preferred |
| GET | `/api/v1/market-data/search` | Paginated projected market snapshot | **Screening adapter** — two complete pages; never an execution quote |
| GET | `/api/v1/market-data/exchanges` | Exchange ID → name mapping | Not used — IDs stored raw |
| GET | `/api/v1/market-data/instrument-types` | Asset class ID → name mapping | Not used — IDs stored raw |
| GET | `/api/v1/market-data/stocks-industries` | Industry ID → name mapping | Not used — IDs stored raw |

### Trading — Real

| Method | Path | Description | eBull status |
|--------|------|-------------|-------------|
| POST | `/api/v1/trading/execution/market-open-orders/by-amount` | Open position by USD amount | **Active** |
| POST | `/api/v1/trading/execution/market-open-orders/by-units` | Open position by unit count | **Active** |
| DELETE | `/api/v1/trading/execution/market-open-orders/{orderId}` | Cancel pending open order | Not used (v1) |
| POST | `/api/v1/trading/execution/market-close-orders/positions/{positionId}` | Close position — body `UnitsToDeduct` nullable → partial close (omit = full). Live doc also lists `InstrumentID` required; our impl omits it — verify on demo before relying | **Active** (full close; partial plumbed in provider, unexposed) |
| DELETE | `/api/v1/trading/execution/market-close-orders/{orderId}` | Cancel pending close order | Not used (v1) |
| PATCH | `/api/v2/trading/{real\|demo}/positions/{positionId}` | Edit TP/SL on one exact open position: `stopLossRate`, `takeProfitRate`, `stopLossType` (`fixed`\|`trailing`), `clearStopLoss`, `clearTakeProfit` (≥1 field). Async acceptance `{operationId, positionId, referenceId}` — re-sync before treating as landed | **Demo active for exact strategy-owned fixed-exit repair and registered ratchets**; real path refused |
| POST | `/api/v2/trading/info/{real\|demo}/eligibility` | Up to 100 ids/symbols; account-specific open/close/partial-close permission, min exposure, max units, order/fill types, and leverage + SL/TP constraints by settlement/direction | **Active as a just-in-time paper gate**; response is process-local |
| POST | `/api/v2/trading/info/{real\|demo}/costs` | What-if open cost rows for `buy` / `sellShort`: open vocabulary including spread, markup, transaction, overnight/weekend and tax; returns `lastUpdated`. Demo returned `value` while omitting documented `amount`; controlled scaling did not establish one unit equation | **Active fail-closed paper gate**: documented fresh USD `amount` only; `value`/unknown horizon refuses |
| GET | `/api/v2/trading/info/{real\|demo}/orders:lookup` | Exactly one of numeric `orderId` or submission `referenceId`; returns status and every `positionExecutions[].positionId` with opening units/average price. Shared 60 GET/min quota | **Active for strategy reconciliation** — strict adapter, exact execution persistence and restart backlog |
| POST | `/api/v1/trading/execution/limit-orders` | Limit/MIT order | Not used (v1 is market-only) |
| DELETE | `/api/v1/trading/execution/limit-orders/{orderId}` | Cancel limit order | Not used (v1) |
| GET | `/api/v1/trading/info/portfolio` | Full portfolio: positions, orders, mirrors, credit | **Active** — portfolio sync |
| GET | `/api/v1/trading/info/{real\|demo}/pnl` | Positions, mirrors, pending orders, credit and P&L inputs for the published cash/invested/equity formula | **Demo active as a just-in-time paper risk gate**; raw response is process-local |
| GET | `/api/v1/trading/info/real/orders/{orderId}` | Single order status | **Active** — order polling |
| GET | `/api/v1/trading/info/{real\|demo}/close-orders/{orderId}` | Close-order status plus exact affected `positions[].positionID` | **Demo active for exact strategy close reconciliation**; disappearance alone is not success |
| GET | `/api/v1/trading/info/trade/history` | Trade history (`minDate` required) | Not used (v1) |

⚠ **Live portal drift, detail pages verified 2026-08-09:** both preflights are
v2 POSTs and each has a dedicated **20 requests/minute** limit. Their documented
schemas are now represented by the non-persisting `BrokerEligibilityResponse`
and `BrokerWhatIfCostResponse` adapters. Documentation examples are not population
evidence: probe a bounded demo cohort before deciding which fields change often,
which settlement/direction arms are actually returned, and whether observed
`sellShort` eligibility and costs are adequate for a short strategy.

The current v2 create-order contract also documents a unique `X-Request-Id`
UUID as the idempotency key, and `orders:lookup.referenceId` as that same
submission header. Strategy execution must commit this immutable UUID before
network I/O and reuse it on every retry. A missing response therefore becomes a
lookup/retry of one identity, never a newly keyed duplicate order.

`POST /api/v2/trading/execution/demo/orders` is now the sole automated paper
writer. Its adapter refuses non-demo credentials and its accepted shape is
long `buy`, `real`, market, x1, USD amount with fixed SL/TP. It validates a
positive `orderId` and exact `referenceId == X-Request-Id`; the generic legacy
writer remains manual and is not an automated fallback.

The automated position manager is equally demo-bound. It commits one compact
material intent before calling either exact-position endpoint, sends the stored
request UUID, and treats PATCH acceptance as pending until a portfolio re-sync
shows the requested fixed rates. A close is applied only when the close-order
detail names the one owned `positionID`. There is no request-id lookup for a
PATCH or legacy close, so a crash before the broker identity is stored becomes
`reconcile_required`; the writer is not blindly replayed. Verified against the
live demo endpoint pages on 2026-08-09 (the downloadable base snapshot remains
v1.279.0 as recorded above).

### Authenticated demo census (2026-08-09)

`etoro-preflight-v2` selected four Stocks and four ETFs at deterministic latest
dollar-volume quantiles. Eligibility resolved 8/8, but refused opening for 1/8
despite local `is_tradable=true`; the canonical parsed response was 12,579
bytes. Within the dedicated 20-request budget, complete 1x/10x long-real and x1
short-CFD pairs covered seven permitted instruments. All 20 calls succeeded
(5,717 canonical parsed bytes total), all cost rows were USD-labelled `value`,
and none supplied documented `amount`. Eighteen response timestamps were about
41 hours old; only two were within the local conservative 24-hour ceiling.
Scaling varied by component between ticket-proportional, invariant,
rounded/other and zero-only. This evidence does not identify `value` as a
monetary amount, rate or price-unit and **0/20 responses pass the execution-use
contract**. Unknown fields are never coerced to zero.

Reproduce against demo only:

```bash
PYTHONPATH=. uv run python scripts/verify_2437_trading_preflight.py --apply --limit 8 --max-cost-requests 20
```

The first authenticated demo census (2026-08-09, four exact members of the
validated US-equity universe) found that HTTP success is not equivalent to
orderability or current cost evidence. One instrument had
`allowOpenPosition=false` despite eBull's stored `is_tradable=true`; the returned
settlement/direction/leverage arms varied materially; minimum exposure ranged
from USD 10 to USD 1,000; cost rows used `value` rather than documented
`amount`; and one response's `lastUpdated` was more than five months old. This
is sufficient to reject the current local tradability flag as an execution
gate, but not sufficient to infer `value` units or full-universe coverage.

### Trading — Demo

Same operations as Real with environment-specific paths (e.g., legacy v1 open:
`/api/v1/trading/execution/demo/market-open-orders/by-amount`; current v2 TP/SL
edit: `/api/v2/trading/demo/positions/{positionId}`; current v2 info:
`/api/v2/trading/info/demo/...`).

Automated paper entry uses the fixed path
`/api/v2/trading/execution/demo/orders`; it is intentionally not formed from a
real/demo environment prefix. Demo account risk uses
`/api/v1/trading/info/demo/pnl` and applies the published formulas below.
The live response shape was verified on 2026-08-11: formula inputs are nested
under the required `clientPortfolio` object and available cash starts from its
singular `credit` field. The provider rejects an absent/malformed envelope or
missing component array; it does not interpret response drift as a zero balance.

### Account base currency — `clientPortfolio.accountCurrencyId`

The same P&L response carries the account's own base currency. The portal schema
for `api-reference/trading--demo/get-account-pnl-and-portfolio-details` (fetched
2026-08-13) documents `accountCurrencyId` as *"Currency ID of the account
(1 = USD)"*, and **1 is the only id it documents** — there is no published
id → ISO-4217 table. So an id other than 1 is stored as an id with a NULL code
and refused by name (`account_currency_not_documented`); it is never mapped to a
code eBull inferred. Adding a member to `DOCUMENTED_ACCOUNT_CURRENCIES`
(`app/services/account_equity_evidence.py`) requires a portal citation.

Before #2602 item 2 the provider discarded this field and
`record_account_equity_snapshot` bound a `'USD'` SQL literal against
`sql/324`'s `CHECK (currency = 'USD')`, so eBull's one table of *official broker
evidence* recorded its own assumption — the gap #2363 named as "nobody has
checked the account currency". It is now carried on
`BrokerAccountRiskSnapshot.account_currency_id` and persisted beside the code
(`sql/341`).

Do not substitute `GET /api/v1/balances` for this. That endpoint takes a
`displayCurrency` **conversion** parameter defaulting to USD and returns both a
per-account `currency` and a converted `displayCurrency`, so a naive read there
returns the display currency requested rather than the account's own. Its
history sibling is 403 on this demo connection (below).

### Account-balance history — documented, demo access refused

The live portal documented `GET /api/v1/balances/history` on 2026-08-11. It
returns no more than 365 daily observations from the preceding 12 months, with
cash, invested, profit/loss and total-balance fields in the requested display
currency. A read-only authenticated probe with eBull's configured demo
credentials returned HTTP 403 and only the standard error keys. Consequently,
eBull does not claim or synthesize a historical broker-equity backfill.

Foundation F-0 starts a compact prospective ledger from the working
`/api/v1/trading/info/demo/pnl` endpoint instead: one observation per UTC day
and environment, aggregate monetary fields only, with no raw response or
per-position duplication. The newest observation wins within the current UTC
day. The local EOD row currently records its computation time, not the effective
time of every closing price, so a same-date difference remains diagnostic and
must not be labelled reconciled. A collection failure must not block the
existing portfolio sync.

Primary page: `balances/get-historical-balance-snapshots`.

### Agent portfolios (copy-trading management)

| Method | Path | Description | eBull status |
|--------|------|-------------|-------------|
| GET | `/api/v1/agent-portfolios` | List agent-portfolios | Not used — mirrors read from /portfolio |
| POST | `/api/v1/agent-portfolios` | Create agent-portfolio (deducts funds to copy-trade) | Not used (v1 read-only) |
| DELETE | `/api/v1/agent-portfolios/{id}` | Delete agent-portfolio | Not used |
| POST | `/api/v1/agent-portfolios/{id}/user-tokens` | Create user token | Not used |
| DELETE | `/api/v1/agent-portfolios/{id}/user-tokens/{tokenId}` | Delete user token | Not used |
| PATCH | `/api/v1/agent-portfolios/{id}/user-tokens/{tokenId}` | Update user token | Not used |

### Users info (trader discovery)

| Method | Path | Description | eBull status |
|--------|------|-------------|-------------|
| GET | `/api/v1/user-info/people` | User profiles by `usernames[]` or `cidList[]` | **Planned** (Track 2) |
| GET | `/api/v1/user-info/people/search` | Advanced user search with filters | **Planned** (Track 2) |
| GET | `/api/v1/user-info/people/{username}/daily-gain` | Daily gain data | **Planned** (Track 2) |
| GET | `/api/v1/user-info/people/{username}/gain` | Monthly/yearly gain history | **Planned** (Track 2) |
| GET | `/api/v1/user-info/people/{username}/portfolio/live` | User's live portfolio | **Planned** (Track 2) |
| GET | `/api/v1/user-info/people/{username}/tradeinfo` | User trade info | **Planned** (Track 2) |

**Search filter params**: `popularInvestor`, `gainMax`, `maxDailyRiskScoreMin/Max`, `maxMonthlyRiskScoreMin/Max`, `weeksSinceRegistrationMin`, `countryId`, `instrumentId`, `instrumentPctMin/Max`, `isTestAccount`, `sort`, `page`, `pageSize`

**Search periods**: `CurrMonth`, `CurrQuarter`, `CurrYear`, `LastYear`, `LastTwoYears`, `OneMonthAgo`, `TwoMonthsAgo`, `ThreeMonthsAgo`, `SixMonthsAgo`, `OneYearAgo`

### PI data

| Method | Path | Description | eBull status |
|--------|------|-------------|-------------|
| GET | `/api/v1/pi-data/copiers` | Public copier info | Not used |

### Social (feeds & comments)

| Method | Path | Description | eBull status |
|--------|------|-------------|-------------|
| GET | `/api/v1/feeds/instrument/{marketId}` | Instrument feed posts | Not used — out of scope |
| GET | `/api/v1/feeds/user/{userId}` | User feed posts | Not used |
| POST | `/api/v1/feeds/post` | Create discussion post | Not used |
| POST | `/api/v1/reactions/{postId}/comment` | Comment on a post | Not used |

### Watchlists

| Method | Path | Description | eBull status |
|--------|------|-------------|-------------|
| GET | `/api/v1/watchlists` | List user watchlists | Not used — out of scope |
| POST | `/api/v1/watchlists` | Create watchlist | Not used |
| GET | `/api/v1/watchlists/{id}` | Get single watchlist | Not used |
| PUT | `/api/v1/watchlists/{id}` | Rename watchlist | Not used |
| DELETE | `/api/v1/watchlists/{id}` | Delete watchlist | Not used |
| POST | `/api/v1/watchlists/{id}/items` | Add instrument IDs | Not used |
| PUT | `/api/v1/watchlists/{id}/items` | Update items | Not used |
| DELETE | `/api/v1/watchlists/{id}/items` | Remove items | Not used |
| PUT | `/api/v1/watchlists/rank/{id}` | Change rank | Not used |
| PUT | `/api/v1/watchlists/setUserSelectedUserDefault/{id}` | Set default | Not used |
| POST | `/api/v1/watchlists/default-watchlist/selected-items` | Create default with items | Not used |
| GET | `/api/v1/watchlists/default-watchlists/items` | Get default items | Not used |
| POST | `/api/v1/watchlists/newasdefault-watchlist` | Create and set as default | Not used |
| GET | `/api/v1/watchlists/public/{userId}` | Public watchlists | Not used |
| GET | `/api/v1/watchlists/public/{userId}/{id}` | Single public watchlist | Not used |
| GET | `/api/v1/curated-lists` | Curated lists | Not used |
| GET | `/api/v1/market-recommendations/{itemsCount}` | Market recommendations | Not used |

---

## Key schemas

### Instrument (from `/market-data/instruments`)

50+ fields. Key ones:

| Field | Type | Notes |
|-------|------|-------|
| `instrumentID` | int | Immutable — cache permanently |
| `instrumentDisplayName` | string | e.g., "Apple" |
| `symbolFull` | string | e.g., "AAPL" |
| `instrumentTypeID` | int | Maps to instrument-types |
| `exchangeID` | int | Maps to exchanges |
| `stocksIndustryId` | int | Sector/industry |
| `priceSource` | string | e.g., "Nasdaq", "LSE" |
| `isInternalInstrument` | bool | If true, restricted from access |
| `hasExpirationDate` | bool | Futures/options flag |
| `isDelisted` | bool | Available via search endpoint |
| `isOpen` | bool | Market currently open |
| `isCurrentlyTradable` | bool | Can be traded right now |
| `isBuyEnabled` | bool | Buy orders accepted |
| `currentRate` | float | Available via search |
| `dailyPriceChange` | float | Available via search |

The live search contract was reverified on 2026-08-12 against the
[official endpoint reference](https://api-portal.etoro.com/api-reference/market-data/search-for-instruments).
Despite that page documenting `pageNumber`, the live API ignores it; `page=2`
returns page 2 and the response `page` must match. `pageSize=10000` returned
12,187 rows in two pages (10,000 + 2,187) in 8.493 seconds. One provider row
had a negative ID and was discarded; 12,185 normalised rows supplied both
`currentRate` and `dailyPriceChange`.

Search is not the full `/instruments` universe and carries no per-row quote
timestamp or bid/ask. On the same dev census it covered 3,591/6,083 current
NYSE/Nasdaq common-stock classifications; coverage rose to 1,480/1,601
(92.44%) after the independently computed ≥$5 and ≥$25m recent-dollar-volume
cohort, and 637/669 (95.22%) above $100m. Therefore callers must join an exact
point-in-time local cohort, report the denominator and fail closed below their
preregistered threshold. Search values may screen or form aggregate context;
fresh `/instruments/rates` quotes remain mandatory before a candidate or order.

### Live rates (from `/market-data/instruments/rates`)

| Field | Type | Notes |
|-------|------|-------|
| `instrumentID` | int | |
| `ask` | float | Buy price |
| `bid` | float | Sell price |
| `lastExecution` | float | Last trade price |
| `conversionRateAsk` | float | Instrument currency → USD |
| `conversionRateBid` | float | Instrument currency → USD |
| `date` | datetime | Price timestamp |

**Spread** = `ask - bid`. eBull computes spread_pct = `(ask - bid) / mid`.

### Candles (from `/market-data/instruments/{id}/history/candles`)

Path parameters:

| Param | Values |
|-------|--------|
| `instrumentId` | eToro instrument ID (integer) |
| `direction` | `asc` (oldest first) or `desc` (newest first) |
| `interval` | `OneMinute`, `FiveMinutes`, `TenMinutes`, `FifteenMinutes`, `ThirtyMinutes`, `OneHour`, `FourHours`, `OneDay`, `OneWeek` |
| `candlesCount` | 1–1000 |

Response: `{ candles: [{ instrumentId, candles: [{ fromDate, open, high, low, close, volume }] }] }`

**Critical**: candles are fetched by count and direction, NOT by date range.
To get 400 days of daily candles: `direction=asc&interval=OneDay&candlesCount=400`.

### Position (from `/trading/info/portfolio`)

30+ fields. Key ones:

| Field | Type | Notes |
|-------|------|-------|
| `positionID` | int | |
| `CID` | int | Account CID |
| `instrumentID` | int | |
| `mirrorID` | int | 0 = manual position; >0 = copy-trading |
| `parentPositionID` | int | Parent trader's position ID |
| `isBuy` | bool | |
| `leverage` | int | |
| `amount` | float | USD, includes collateral |
| `units` | float | |
| `openRate` | float | Entry price |
| `openDateTime` | datetime | |
| `initialAmountInDollars` | float | Original investment |
| `initialUnits` | float | |
| `takeProfitRate` | float | |
| `stopLossRate` | float | |
| `isTslEnabled` | bool | Trailing stop loss |
| `totalFees` | float | Overnight + dividends |
| `totalExternalFees` | float | |
| `totalExternalTaxes` | float | |
| `settlementTypeID` | int | 0=CFD, 1=Real Asset, 2=SWAP, 3=Crypto MarginTrade, 4=Future Contract |
| `isPartiallyAltered` | bool | |

### Mirror (from `/trading/info/portfolio`)

| Field | Type | Notes |
|-------|------|-------|
| `mirrorID` | int | |
| `parentCID` | int | Copied trader's CID |
| `parentUsername` | string | |
| `isPaused` | bool | |
| `availableAmount` | float | Uninvested cash in mirror (USD) |
| `initialInvestment` | float | Original allocation (USD) |
| `depositSummary` | float | Additional deposits (USD) |
| `withdrawalSummary` | float | Withdrawals (USD) |
| `closedPositionsNetProfit` | float | Realised P&L from closed positions (USD) |
| `stopLossPercentage` | float | |
| `stopLossAmount` | float | |
| `mirrorStatusID` | int | 0=Active, 1=Paused, 2=Pending Closure, 3=In Alignment |
| `positions[]` | array | Nested position objects (same schema as Position above) |
| `startedCopyDate` | datetime | |

### Order request (`createOrderRequest`)

| Field | Type | Required | Notes |
|-------|------|----------|-------|
| `instrumentId` | int | Yes | |
| `isBuy` | bool | Yes | Always `true` in eBull v1 |
| `leverage` | int | Yes | Always `1` in eBull v1 |
| `investment` | number | Conditional | For by-amount |
| `units` | number | Conditional | For by-units |
| `orderType` | string | No | `"MKT"` or `"LMT"` |
| `executionType` | string | No | `"GTC"` or `"IOC"` |
| `stopLossRate` | number | No | |
| `stopLossPct` | number | No | |
| `takeProfitRate` | number | No | |
| `takeProfitPct` | number | No | |
| `limitRate` | number | No | For limit orders |
| `isTrailingStopLoss` | bool | No | |

Response: `{"token": "..."}` — unique operation identifier.

### Close request (`createExitOrderRequest`)

| Field | Type | Notes |
|-------|------|-------|
| `instrumentId` | int | |
| `units` | number | `null` = close entire position |
| `executionType` | string | |
| `positionId` | int | |

---

## Portfolio calculations

eToro's official formulae (from portal guides):

### Available cash

```
credit
  - SUM(ordersForOpen[i].amount WHERE mirrorID=0)
  - SUM(orders[i].amount)
```

Only manual positions (mirrorID=0). Always in USD.

### Total invested

```
SUM(positions.amount)
  + SUM(mirrors.positions.amount)
  + SUM(mirrors.availableAmount - mirrors.closedPositionsNetProfit)
  + SUM(ordersForOpen.amount WHERE mirrorID=0)
  + SUM(orders.amount)
  + SUM(ordersForOpen.totalExternalCosts WHERE mirrorID=0)
```

### Unrealised P&L

```
SUM(positions.unrealizedPnL.pnL)
  + SUM(mirrors.positions.unrealizedPnL.pnL)
  + SUM(mirrors.closedPositionsNetProfit)
```

### Equity

```
Available Cash + Total Invested + Unrealised P&L
```

---

## WebSocket API

### Connection

```
wss://ws.etoro.com/ws
```

### Authentication

```json
{
  "id": "<uuid>",
  "operation": "Authenticate",
  "data": {"userKey": "...", "apiKey": "..."}
}
```

Response: `{"success": true/false, "errorCode": "...", "errorMessage": "..."}`

### Subscribe to instrument rates

```json
{
  "id": "<uuid>",
  "operation": "Subscribe",
  "data": {"topics": ["instrument:<instrumentId>"], "snapshot": true}
}
```

Rate message fields: `Ask`, `Bid`, `LastExecution`, `Date` (ISO 8601),
`PriceRateID`, plus margin-derived prices (`NewUnitMargin`, `UnitMarginAsk`,
`UnitMarginBid`, `BidDiscounted`, `AskDiscounted`,
`UnitMarginBidDiscounted`, `UnitMarginAskDiscounted`).

**Undocumented fields present on the snapshot push** (measured #2241,
2026-08-04 — absent from `api-reference/websocket/topics.md`, therefore
unversioned; re-verify anything load-bearing): `InstrumentID`,
`IsInstrumentActive`, `OfficialClosingPrice`, `IsMarketOpen`,
`IsExchangeOpen`, `ConversionRateBid`, `ConversionRateAsk`, `AllowBuy`,
`AllowSell`, `MaxPositionUnits`. `IsMarketOpen` and `IsExchangeOpen` are
independent and do disagree (EURUSD: `true` / `false` at 23:37 UTC).

**Rate pushes are FIELD-LEVEL SPARSE DELTAS** (measured #2243, 2026-08-04;
this corrects the "two push shapes" model recorded under #2241). A push
carries only the fields that changed — any subset of `Bid`, `Ask`,
`LastExecution`, `BidDiscounted`, `AskDiscounted` may arrive alone. The
instrument is on the envelope's `topic`, not in the payload, on every shape.

Census over 180,666 messages (240 instruments, 608 s, crypto + FX + Tokyo
equities):

| share | shape |
| --- | --- |
| 59.8% | heartbeat — `{"Date","PriceRateID"}` only, no price field |
| 16.8% | `Bid`+`Ask` — the only shape a stateless parser can use on its own |
| 10.5% | `Bid`+`BidDiscounted`+`LastExecution` |
| 10.1% | `Ask`+`AskDiscounted` |
| 1.6% | `LastExecution` alone |
| ~1.2% | 12 further partial combinations |

**A consumer must carry per-instrument state and merge deltas** — requiring a
complete payload sees under half the market. Until #2252, `_parse_rate_content`
required **both** `Bid` and `Ask` and discarded every partial: **58.1% of
price-CHANGING messages dropped** (21,245 of 36,564), making the stored quote
1.5–2.6× staler than the feed allows (median gap between usable updates vs
actual price changes: crypto 3.01s vs 1.15s, JP 3.99s vs 1.85s, FX 1.53s vs
1.02s).

Fixed in #2252: `parse_rate_deltas()` → `RateStateStore.apply()` in
`app/services/etoro_websocket.py`. Paired-arm acceptance (184 crypto/FX
instruments, 300 s, one captured stream through both parsers, 46,037 deltas):
share of wire price-changes captured **39.9% → 91.5%**; median usable-update
gap **4.09s → 1.27s** against a 1.27s wire gap, i.e. staleness **3.21× →
1.00×** (crypto 4.58× → 1.00×, FX 2.20× → 1.00×). Two rules fall out of the
merge and bind any reimplementation:

- **Presence ≠ value.** An absent `LastExecution` means "unchanged"; a present
  one that is ≤ 0 means "not a real trade → NULL" (#1429). A single nullable
  field cannot carry both, so the delta type needs explicit presence flags.
- **A heartbeat must not advance the ordering watermark.** Heartbeats are the
  majority of the wire; if one stamps the merge state's `quoted_at`, the
  out-of-order guard then rejects the next genuine price delta behind it.

**Size ingest against the wire, not against parsed ticks.** The earlier "23%
parse" figure is right but was read as "77% are inert"; only ~60% are.

**`LastExecution` is NOT a trade print** (measured #2243). Over 26,741
observations it **never left `[bid, ask]`** (0.00%), which a real print would.
It is bid-side, and how tightly is asset-class-dependent: `== BidDiscounted` on
**100.0%** of Tokyo-equity and **97.8%** of FX observations, but only **58.7%**
of crypto (bid-*near*: median spread position 0.000, mean 0.041). `==
AskDiscounted` is 0.0% everywhere. ⚠ Do not carry a single global label across
asset classes — crypto does not fit the clean story.

⚠ Against eToro's *marked-up* `Bid`/`Ask` the FX series sits mid-spread (mean
position 0.399) and looks like a derived mid — **the excursion test must be run
against `BidDiscounted`/`AskDiscounted` or it returns the wrong answer.**

**Build 1-minute bars from `Bid`** — an empirical compatibility rule: `Bid` is
the series that reproduces eToro's own `OneMinute` REST candle. Over 131
complete minutes, `Bid` matched 77.1% of closes / 60.3% of full OHLC;
`LastExecution` 55.0% / 48.1%; mid or ask 0%. The residual is granularity, not a
second series — attributing all 42 mismatches, **zero were Tokyo equities**, and
all were **within 0.20%** of the REST close (median 0.0019%, max 0.0716%).

`OneMinute` volume is **equity-only** — populated for US and Tokyo equities,
always `None` for crypto and FX.

⚠ **Scope: demo env, one 10-minute window, crypto / FX / Tokyo equities.** Live
env, US equities (#2243 arm outstanding), HK (shut during capture) and stressed
regimes (auction, halt, wide spread, FX rollover) are all unmeasured.

### WebSocket limits (measured, #2241 — the portal documents NONE)

| limit | value | behaviour at the boundary |
| --- | --- | --- |
| topics per **session** | **4,999** | Subscribe rejected with `errorCode: SubscribeFailed`, `errorMessage: "Too many subscriptions for session"`. Connection survives and keeps serving. Rejection is **per-frame and atomic** — a frame straddling the cap bounces whole. |
| bytes per **frame** | **25 KiB** (25,600) | **No ack, no error — the socket is dropped** (`1006 ABNORMAL_CLOSURE`, empty reason, i.e. no close frame). Subscription not applied. Bracket: 25,529 B acked / 25,719 B dropped. |

4,999 was replicated exactly on a second independent session. An over-cap
rejection does **not** poison the session — 3,141 rate messages arrived in
the following 20s on already-subscribed topics, and both `Subscribe` and
`Unsubscribe` still acked, so a rejection needs handling rather than a
reconnect. The over-size drop is **not** a client-side send limit:
`ws.send()` returns normally and no close frame is received, so
attribution is server-or-intermediary.

The session cap is **per connection, not per API key**: concurrent sessions
each get their own 4,999, so the full 12,684-instrument universe fits in 3
(measured: 12,684/12,684 accepted, 0 failures, 2.21s, no disconnect over a
120s hold). `Unsubscribe` frees capacity, so it is live occupancy rather
than a session lifetime budget.

Consequences for any caller: **chunk Subscribe/Unsubscribe by BYTES**, not
topic count (instrument-id width varies) — 500 topics is ~9.4 KB and is
proven at scale; and **correlate each frame uuid to its ack**, because an
over-size frame is silent and a missing ack is the only signal. eToro acks
both ops as `{"id": …, "success": true, "operation": "Subscribe"}`.

Time-to-first-tick after subscribe held at 0.03–0.18s from 10 through 4,999
topics — no degradation with topic count.

**No volume / size / quantity field exists on any WS topic** (#608, verified
2026-06-27 against both api-portal `/websocket/topics` and
`websocket-doc.html`). eToro's WS has exactly two topics — `instrument:<id>`
(rate) and `private` (order/position) — and neither carries per-trade or
per-tick volume. There is **no** `Trading.Instrument.Trade` topic. The only
volume eToro exposes is the **per-bar** `volume` field on the REST candles
endpoint (cumulative for the in-progress bar). Live intraday volume on the
chart would therefore require polling candles, not the WS — out of #608 scope,
deferred.

### Subscribe to private channel (order/position updates)

```json
{
  "id": "<uuid>",
  "operation": "Subscribe",
  "data": {"topics": ["private"], "snapshot": true}
}
```

Private channel message types include: `Trading.OrderForCloseMultiple.Update`
with fields: `OrderID`, `StatusID`, `InstrumentID`, `ExecutedUnits`,
`EndRate`, `NetProfit`, `CloseReason`, etc.

### WebSocket error codes

`SessionAlreadyAuthenticated`, `DataRequired`, `ApiKeyRequired`,
`UserKeyRequired`, `TooManyRequests`, `Forbidden`,
`UnhandledException`, `InvalidKey`, `Unauthorized`

### eBull WebSocket status

Implemented (`app/services/etoro_websocket.py`, #274). Live price streaming via
`instrument:<id>` (rate fan-out to SSE `/sse/quotes`) + `private` (debounced
portfolio reconcile), with a 5s REST `/instruments/rates` poll as a freshness
floor. Live in-progress-bar OHLC rides this stream (#602); **volume stays
static** because the WS push carries no volume (#607/#608 — see rate-fields
note above).

---

## Error responses

Standard shape:
```json
{"errorCode": "...", "errorMessage": "..."}
```

| Code | HTTP | Meaning |
|------|------|---------|
| `Unauthorized` | 401 | Invalid or missing API/user key |
| `TooManyRequests` | 429 | Rate limit exceeded |
| `UnhandledException` | 500 | Server error |

---

## eBull data pipeline summary

### What runs and when

| Job | Schedule | Endpoint(s) | Purpose |
|-----|----------|-------------|---------|
| `nightly_universe_sync` | 22:00 UTC | `GET /market-data/instruments` | Sync tradable instrument universe |
| `daily_candle_refresh` | 22:15 UTC | `GET /market-data/instruments/{id}/history/candles/...` (per-instrument) | Historical OHLCV bars |
| `daily_portfolio_sync` | 22:30 UTC | `GET /trading/info/{env}/portfolio` | Positions, cash, mirrors |
| `hourly_fx_rates_refresh` | Every hour | `GET /market-data/instruments/rates` (batch) | Current quotes for all held instruments |
| `execute_approved_orders` | Every 5 min | `POST /trading/execution/{env}/...` | Execute approved buy/sell orders |

### Data flow

```
eToro API call
  -> Normalisation (pure functions, unit-testable)
  -> Database UPSERT (instruments / price_daily / quotes / exchanges)
  -> Feature computation (price_features, etc.)
```

Raw disk dumps were retired in #471 — the SQL tables above ARE the
audit trail. See `docs/review-prevention-log.md` §"Raw payload
persistence" for the scope-narrowed rule.

### Key implementation files

| Component | File |
|-----------|------|
| Market data provider | `app/providers/implementations/etoro.py` |
| Broker provider | `app/providers/implementations/etoro_broker.py` |
| Resilient HTTP client | `app/providers/resilient_client.py` |
| Universe sync service | `app/services/universe.py` |
| Market data service | `app/services/market_data.py` |
| Portfolio sync service | `app/services/portfolio_sync.py` |
| Strategy position manager | `app/services/strategy_position_manager.py` |
| Credentials service | `app/services/broker_credentials.py` |
| Scheduled jobs | `app/workers/scheduler.py` |
| Configuration | `app/config.py` |

---

## Critical integration notes

### Instrument ID is an integer, not a symbol

The eToro API uses integer `instrumentID` everywhere. Symbols exist
(`symbolFull`) but are metadata, not lookup keys. Instrument IDs are
**immutable** — cache them permanently.

### Candles are by count, not date range

No `from`/`to` date parameters. Use `direction` + `interval` +
`candlesCount`. To get historical data from a specific date, compute
the count needed.

### Two credentials per environment

The provider constructor accepts both `api_key` and `user_key`. Both
are required for every request.

### Portfolio returns everything in one call

The `/trading/info/{env}/portfolio` endpoint returns positions, orders,
mirrors (copy-trading), and credit (cash) in a single response. This
is the sole source for copy-trading data — there is no separate
"get mirrors" endpoint.

### Copy-trading position lifecycle

- Active mirrors appear in `/portfolio` response with nested positions
- When a mirror is closed on eToro, it disappears from the next `/portfolio` response
- eBull soft-closes missing mirrors: `active=FALSE`, `closed_at=sync_timestamp`
- Positions are never deleted (preserved for audit trail)
- Guard: if broker returns empty mirrors but local active mirrors exist, sync raises RuntimeError

### All monetary values from eToro are in USD

Positions, mirrors, cash — all denominated in USD. Currency conversion
to display currency (e.g., GBP) is done by eBull using FX rates from
the `quotes` table.

### `open_conversion_rate` on mirror positions

This is the FX rate at entry time (instrument native currency -> USD).
Critical for non-USD instruments. Without it, P&L calculations for GBP,
JPY, ILS, EUR instruments would be nonsensical. Track 2 defers
current-rate recalculation.
