# eToro API reference

Source of truth for how eBull integrates with eToro. Derived from the
official OpenAPI spec at `https://api-portal.etoro.com/api-reference/openapi.json`
(fetched 2026-04-09).

---

## Base URL

```
https://public-api.etoro.com
```

NOT `https://api.etoro.com` (the speculative URL we had before).

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
or no prefix in the path for real.

### Credential storage in eBull

eBull stores **two** values per eToro environment:

| eBull label | eToro field | Purpose |
|-------------|-------------|---------|
| `api_key` | Public API Key (`x-api-key`) | Application-level auth |
| `user_key` | User Key (`x-user-key`) | Account-level auth |

Both are stored in `broker_credentials` with provider=`etoro`.

---

## Endpoints used by eBull

### Identity

```
GET /api/v1/me
```

Returns `{ gcid, realCid, demoCid }`. Useful for verifying credentials
are valid and identifying which CID to use for trading endpoints.

### Instruments (universe sync)

```
GET /api/v1/market-data/instruments
    ?instrumentIds=1,2,3           (optional, comma-separated)
    &exchangeIds=1,2               (optional)
    &instrumentTypeIds=1,2         (optional)
    &stocksIndustryIds=1,2         (optional)
```

Returns `{ instrumentDisplayDatas: [...] }` with fields:

| Field | Type | Notes |
|-------|------|-------|
| `instrumentID` | int | eToro's unique instrument ID (our external identifier) |
| `instrumentDisplayName` | string | e.g. "Apple" |
| `symbolFull` | string | e.g. "AAPL" |
| `instrumentTypeID` | int | Maps to instrument-types endpoint |
| `exchangeID` | int | Maps to exchanges endpoint |
| `stocksIndustryId` | int | Sector/industry |
| `priceSource` | string | e.g. "Nasdaq", "LSE" |
| `isInternalInstrument` | bool | If true, restricted from public access |
| `hasExpirationDate` | bool | Futures/options flag |

**Pagination**: not documented in the spec. Likely returns all instruments
in a single response. Confirm empirically.

### Instrument search (alternative universe discovery)

```
GET /api/v1/market-data/search
    ?searchText=Apple               (optional)
    &fields=<comma-separated>       (required)
    &pageSize=100                   (optional)
    &pageNumber=1                   (optional)
    &sort=popularityUniques7Day desc (optional)
```

Returns `{ page, pageSize, totalItems, items: [...] }` with richer data
per instrument (including `isDelisted`, `isOpen`, `dailyPriceChange`, etc).

The `fields` parameter controls which fields are returned. Example:
`fields=instrumentId,displayname,symbol,exchangeID,instrumentTypeID`

### Instrument types

```
GET /api/v1/market-data/instrument-types
    ?instrumentTypeIds=1,2          (optional)
```

Maps `instrumentTypeID` to human-readable names (Stocks, ETFs, Crypto, etc).

### Exchanges

```
GET /api/v1/market-data/exchanges
    ?exchangeIds=1,2                (optional)
```

Maps `exchangeID` to exchange names and metadata.

### Industries

```
GET /api/v1/market-data/stocks-industries
```

Maps `stocksIndustryId` to industry names.

### Live rates (quotes)

```
GET /api/v1/market-data/instruments/rates
    ?instrumentIds=1,2,3            (required, comma-separated)
```

Returns `{ rates: [...] }` with:

| Field | Type | Notes |
|-------|------|-------|
| `instrumentID` | int | |
| `ask` | float | Buy price |
| `bid` | float | Sell price |
| `lastExecution` | float | Last trade price |
| `conversionRateAsk` | float | Instrument currency to USD |
| `conversionRateBid` | float | Instrument currency to USD |
| `date` | datetime | Price timestamp |

**Spread** = `ask - bid`. This is what we need for spread checks.

### Historical candles

```
GET /api/v1/market-data/instruments/{instrumentId}/history/candles/{direction}/{interval}/{candlesCount}
```

Path parameters:

| Param | Values |
|-------|--------|
| `instrumentId` | eToro instrument ID (integer) |
| `direction` | `asc` or `desc` |
| `interval` | `OneMinute`, `FiveMinutes`, `TenMinutes`, `FifteenMinutes`, `ThirtyMinutes`, `OneHour`, `FourHours`, `OneDay`, `OneWeek` |
| `candlesCount` | 1-1000 |

Returns `{ interval, candles: [{ instrumentId, candles: [{ instrumentID, fromDate, open, high, low, close, volume }, ...] }] }`.

**Key difference from our speculative API**: candles are fetched by count
and direction, not by date range. To get 400 days of daily candles, use
`direction=desc&interval=OneDay&candlesCount=400`.

### Historical closing prices

```
GET /api/v1/market-data/instruments/history/closing-price
```

Returns closing prices for **all** instruments at daily, weekly, monthly,
and yearly intervals. Useful for bulk price snapshots.

---

## Trading endpoints

All trading endpoints require Write permission on the API key.

### Demo trading

| Method | Path | Purpose |
|--------|------|---------|
| POST | `/api/v1/trading/execution/demo/market-open-orders/by-amount` | Open position by cash amount |
| POST | `/api/v1/trading/execution/demo/market-open-orders/by-units` | Open position by unit count |
| DELETE | `/api/v1/trading/execution/demo/market-open-orders/{orderId}` | Cancel pending open order |
| POST | `/api/v1/trading/execution/demo/market-close-orders/positions/{positionId}` | Close position (full or partial) |
| DELETE | `/api/v1/trading/execution/demo/market-close-orders/{orderId}` | Cancel pending close order |
| POST | `/api/v1/trading/execution/demo/limit-orders` | Limit/MIT order |
| DELETE | `/api/v1/trading/execution/demo/limit-orders/{orderId}` | Cancel limit order |

### Real trading

Same paths without the `/demo/` segment.

### Open order request body

```json
{
  "InstrumentID": 1,
  "IsBuy": true,
  "Leverage": 1,
  "Amount": 100.0,
  "StopLossRate": null,
  "TakeProfitRate": null,
  "IsTslEnabled": false,
  "IsNoStopLoss": true,
  "IsNoTakeProfit": true
}
```

Required: `InstrumentID`, `IsBuy`, `Leverage`, `Amount`.

**eBull constraints**: `IsBuy` is always `true` (long only in v1),
`Leverage` is always `1` (no leverage in v1).

### Close order request body

```json
{
  "InstrumentID": 1,
  "UnitsToDeduct": null
}
```

`UnitsToDeduct` null = close entire position.

### Portfolio

```
GET /api/v1/trading/info/demo/portfolio
GET /api/v1/trading/info/portfolio       (real)
```

Returns positions, orders, mirrors, and `credit` (available cash in USD).

### PnL

```
GET /api/v1/trading/info/demo/pnl
GET /api/v1/trading/info/real/pnl
```

### Order status

```
GET /api/v1/trading/info/demo/orders/{orderId}
GET /api/v1/trading/info/real/orders/{orderId}
```

### Trade history

```
GET /api/v1/trading/info/trade/history
```

---

## What eBull does NOT use

These endpoints exist but are out of scope:

- Agent portfolios (`/api/v1/agent-portfolios`) — copy trading management
- Social feeds (`/api/v1/feeds/`) — discussion posts
- Watchlists (`/api/v1/watchlists/`) — eToro UI watchlists
- User info (`/api/v1/user-info/`) — social/people search
- Market recommendations (`/api/v1/market-recommendations/`) — eToro's own recs
- Curated lists (`/api/v1/curated-lists`) — eToro editorial

---

## Key differences from our speculative provider

| What | We had | Real API |
|------|--------|----------|
| Base URL | `https://api.etoro.com` | `https://public-api.etoro.com` |
| Auth | `Authorization: Bearer <key>` | `x-api-key` + `x-user-key` + `x-request-id` |
| Instruments | `GET /v1/instruments` | `GET /api/v1/market-data/instruments` |
| Candles | `GET /v1/candles/day?symbol=X&from=...&to=...` | `GET /api/v1/market-data/instruments/{id}/history/candles/{dir}/{interval}/{count}` |
| Quotes | `GET /v1/quotes?symbol=X` | `GET /api/v1/market-data/instruments/rates?instrumentIds=1,2` |
| Instrument ID | symbol-based lookups | integer `instrumentID` throughout |
| Credentials | single API key | two keys: `x-api-key` (app) + `x-user-key` (account) |

### Critical: instrument ID is an integer, not a symbol

The eToro API uses integer `instrumentID` everywhere — candles, rates,
trading. Symbols exist (`symbolFull`) but are metadata, not lookup keys.
Our `instruments.symbol` must be mapped to `instrumentID` before any API call.

### Critical: candles are by count, not date range

No `from`/`to` date parameters. Instead: `direction` + `interval` + `candlesCount`.
To get historical data from a specific date, compute the count needed.

### Critical: two credentials per environment

The provider constructor must accept both `api_key` and `user_key`, not a
single `api_key`. Both are stored in `broker_credentials`.
