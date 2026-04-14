# eToro API Reference

Source of truth for how eBull integrates with eToro. Derived from the
official OpenAPI spec at `https://api-portal.etoro.com/api-reference/openapi.json`
(v1.158.0, 57 paths, 128 schemas — last verified 2026-04-14).

Portal: `https://api-portal.etoro.com/`
LLM index: `https://api-portal.etoro.com/llms.txt`

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
| **Standard** | **60 req/min** | All GET requests: market data, portfolio info, social reads, watchlist reads |
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
- Persist raw responses to `data/raw/etoro/` before normalisation

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
| GET | `/api/v1/market-data/search` | Search instruments with field projection | Not used — full universe synced |
| GET | `/api/v1/market-data/exchanges` | Exchange ID → name mapping | Not used — IDs stored raw |
| GET | `/api/v1/market-data/instrument-types` | Asset class ID → name mapping | Not used — IDs stored raw |
| GET | `/api/v1/market-data/stocks-industries` | Industry ID → name mapping | Not used — IDs stored raw |

### Trading — Real

| Method | Path | Description | eBull status |
|--------|------|-------------|-------------|
| POST | `/api/v1/trading/execution/market-open-orders/by-amount` | Open position by USD amount | **Active** |
| POST | `/api/v1/trading/execution/market-open-orders/by-units` | Open position by unit count | **Active** |
| DELETE | `/api/v1/trading/execution/market-open-orders/{orderId}` | Cancel pending open order | Not used (v1) |
| POST | `/api/v1/trading/execution/market-close-orders/positions/{positionId}` | Close position | **Active** |
| DELETE | `/api/v1/trading/execution/market-close-orders/{orderId}` | Cancel pending close order | Not used (v1) |
| POST | `/api/v1/trading/execution/limit-orders` | Limit/MIT order | Not used (v1 is market-only) |
| DELETE | `/api/v1/trading/execution/limit-orders/{orderId}` | Cancel limit order | Not used (v1) |
| GET | `/api/v1/trading/info/portfolio` | Full portfolio: positions, orders, mirrors, credit | **Active** — portfolio sync |
| GET | `/api/v1/trading/info/real/pnl` | Portfolio with P&L details | Not used — computed locally |
| GET | `/api/v1/trading/info/real/orders/{orderId}` | Single order status | **Active** — order polling |
| GET | `/api/v1/trading/info/trade/history` | Trade history (`minDate` required) | Not used (v1) |

### Trading — Demo

Same operations as Real, all prefixed with `/demo/` (e.g., `/api/v1/trading/execution/demo/market-open-orders/by-amount`).

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

Rate message fields: `Ask`, `Bid`, `LastExecution`, `Date` (ISO 8601), `PriceRateID`

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

Not yet implemented. Planned for live price streaming (alternative to polling
`/instruments/rates`). Would eliminate the 1.1s throttle overhead for
real-time dashboard updates.

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
  -> Raw JSON persisted to data/raw/etoro/ (timestamped)
  -> Normalisation (pure functions, unit-testable)
  -> Database UPSERT
  -> Feature computation (price_features, etc.)
```

### Key implementation files

| Component | File |
|-----------|------|
| Market data provider | `app/providers/implementations/etoro.py` |
| Broker provider | `app/providers/implementations/etoro_broker.py` |
| Resilient HTTP client | `app/providers/resilient_client.py` |
| Universe sync service | `app/services/universe.py` |
| Market data service | `app/services/market_data.py` |
| Portfolio sync service | `app/services/portfolio_sync.py` |
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
