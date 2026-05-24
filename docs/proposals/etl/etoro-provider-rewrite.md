# eToro provider rewrite against real API

**Issue**: #139
**Date**: 2026-04-09
**Status**: Approved design

## Problem

The entire eToro provider was built speculatively against a made-up API.
Every endpoint path, the auth mechanism, and the data model are wrong.
The real eToro API lives at `https://public-api.etoro.com` and uses a
three-header auth scheme (`x-api-key`, `x-user-key`, `x-request-id`)
with integer `instrumentID` throughout. The speculative code uses
`https://api.etoro.com`, Bearer auth, symbol-based lookups, and
date-range candle queries -- none of which exist.

This spec covers a full provider rewrite, credential model fix, and
validation endpoint, decomposed into four PRs.

**Reference**: `docs/etoro-api-reference.md` documents the real API
surface derived from the official OpenAPI spec.

---

## 1. Credential model

### 1.1 Identity tuple

Credential identity is `(operator_id, provider, label, environment)`.

eToro keys are scoped to both an environment (Demo / Real) and a
permission level (Read / Write). The identity tuple must capture
environment from day one so that demo and real credentials coexist
without ambiguity.

### 1.2 Environment values

Environment is a canonical lowercase string everywhere: DB column,
API payloads, loader contract, frontend display. The only accepted
values are `demo` and `real`. No mixed casing, no abbreviations.

### 1.3 One user_key per environment in v1

v1 is demo-only. Demo trading requires write permission, so a
read-only key would block demo fills. The operator controls what
permissions they grant when creating the key on eToro's platform.

- One `api_key` row and one `user_key` row per environment.
- When live trading lands, the operator creates a separate pair for
  `environment="real"`. That is new rows, not a schema change.
- If least-privilege read/write separation becomes a real need later,
  we add labels `user_key_read` / `user_key_write`. The schema already
  supports it because label is part of the identity tuple.

### 1.4 Credential rows per environment

| Label      | Environment | HTTP Header  | Notes                                   |
|------------|-------------|--------------|-----------------------------------------|
| `api_key`  | `demo`      | `x-api-key`  | Public API key (app-level), env-specific |
| `user_key` | `demo`      | `x-user-key` | Account-level key, env-specific          |
| `api_key`  | `real`      | `x-api-key`  | Later, when live trading lands           |
| `user_key` | `real`      | `x-user-key` | Later                                   |

### 1.5 Schema change

Add `environment VARCHAR NOT NULL DEFAULT 'demo'` to
`broker_credentials`.

Unique constraint becomes
`(operator_id, provider, label, environment) WHERE revoked_at IS NULL`.

### 1.5a CredentialMetadata update

Add `environment: str` to the `CredentialMetadata` dataclass. This
field appears in list responses and is needed for the frontend to
display which environment a credential belongs to.

```python
@dataclass(frozen=True)
class CredentialMetadata:
    id: UUID
    operator_id: UUID
    provider: str
    label: str
    environment: str      # new
    last_four: str
    created_at: datetime
    last_used_at: datetime | None
    revoked_at: datetime | None
```

### 1.6 Loader contract

```python
def load_credential_for_provider_use(
    conn: psycopg.Connection[object],
    *,
    operator_id: UUID,
    provider: str,
    label: str,           # required -- no default, no fallback
    environment: str,     # required -- no silent env fallback
    caller: str,
) -> str:
```

Both `label` and `environment` are required. No defaults. No fallback
from env-specific to global. If the credential is missing for the
requested `(provider, label, environment)`, raises `CredentialNotFound`
with the specific label and environment in the message -- loud,
specific, and the caller skips the job.

This is a **breaking change** to the existing function signature.
All callers must be updated in the same PR.

### 1.7 Scheduler helper

```python
def _load_etoro_credentials(job_name: str) -> tuple[str, str] | None:
    """Load (api_key, user_key) for settings.etoro_env.

    Returns None if either credential is missing. Failures are logged
    at ERROR with the specific missing label and environment.
    """
```

Missing either key is a hard skip with structured log. Never silently
fall back from one environment to another.

### 1.8 Migration script

The legacy env-var-backed setup was only ever demo. The migration
script hard-codes `environment="demo"` for all migrated credentials.
This is stated explicitly -- it does not guess.

Labels change from `"read"` / `"write"` to `"api_key"` / `"user_key"`.

---

## 2. MarketDataProvider interface

### 2.1 Interface changes

```python
class MarketDataProvider(ABC):

    @abstractmethod
    def get_tradable_instruments(self) -> list[InstrumentRecord]:
        """Return the full list of currently tradable instruments.

        Unchanged from current interface.
        """

    @abstractmethod
    def get_daily_candles(
        self,
        instrument_id: int,
        lookback_days: int,
    ) -> list[OHLCVBar]:
        """Return daily OHLCV bars for an instrument.

        Returns completed daily bars only -- any still-forming
        current-day bar from the API is excluded.

        Ordering: oldest-first.

        lookback_days is a hint, not a guarantee. The provider returns
        up to that many trading days of data, which may be fewer
        calendar days than requested due to weekends and holidays.
        The eToro candle endpoint caps at 1000 candles per request;
        the current 400-day lookback is well within that limit.
        """

    @abstractmethod
    def get_quote(self, instrument_id: int) -> Quote | None:
        """Return the current quote for a single instrument.

        Returns None if the instrument is not recognised or not
        currently quoted.
        """

    @abstractmethod
    def get_quotes(self, instrument_ids: list[int]) -> list[Quote]:
        """Batch quote fetch.

        Implementations handle any provider-specific batching limits
        internally. Callers pass the full list of IDs.

        Returns a list of Quote objects with no ordering guarantee.
        Each Quote carries instrument_id so callers match results
        by ID, not by position.

        Instruments that are not recognised or not currently quoted
        are silently omitted from the result list.
        """
```

### 2.2 Dataclass changes

**`OHLCVBar`**: Remove `symbol` field. The service layer knows the
instrument_id from its own loop -- it does not need the provider to
echo it back.

```python
@dataclass(frozen=True)
class OHLCVBar:
    price_date: date
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int | None
```

**`Quote`**: Remove `symbol`, add `instrument_id: int`. Needed for
batch `get_quotes` to distinguish results.

```python
@dataclass(frozen=True)
class Quote:
    instrument_id: int
    timestamp: datetime
    bid: Decimal
    ask: Decimal
    last: Decimal | None
```

**`InstrumentRecord`**: Unchanged. `provider_id` stays as `str`
(eToro integer cast to string). Maps directly to
`instruments.instrument_id` (BIGINT PK) via `sync_universe`.

---

## 3. EtoroMarketDataProvider rewrite

### 3.1 Constructor

```python
class EtoroMarketDataProvider(MarketDataProvider):
    def __init__(
        self,
        api_key: str,
        user_key: str,
        env: str = "demo",
    ) -> None:
```

### 3.2 HTTP client setup

```python
base_url = "https://public-api.etoro.com"
headers = {
    "x-api-key": api_key,
    "x-user-key": user_key,
    "Content-Type": "application/json",
}
# x-request-id is set per-request via a fresh UUID
```

### 3.3 Endpoint mapping

| Method   | Old (speculative, wrong)                            | New (real)                                                                          |
|----------|----------------------------------------------------|-------------------------------------------------------------------------------------|
| Universe | `GET /v1/instruments`                              | `GET /api/v1/market-data/instruments`                                               |
| Candles  | `GET /v1/candles/day?symbol=X&from=...&to=...`     | `GET /api/v1/market-data/instruments/{id}/history/candles/desc/OneDay/{count}`       |
| Quote    | `GET /v1/quotes?symbol=X`                          | `GET /api/v1/market-data/instruments/rates?instrumentIds=1,2,3`                     |

### 3.4 Normaliser rules

No more dual camelCase / snake_case guessing. The real API returns
specific field names from the OpenAPI spec:

**Instruments** (`instrumentDisplayDatas[]`):
- `instrumentID` (int) -- maps to `InstrumentRecord.provider_id` (as str)
- `symbolFull` (str) -- maps to `InstrumentRecord.symbol`
- `instrumentDisplayName` (str) -- maps to `InstrumentRecord.company_name`
- `exchangeID` (int) -- stored as str in `InstrumentRecord.exchange`
- `instrumentTypeID` (int) -- metadata, not directly mapped
- `stocksIndustryId` (int) -- store as integer string for now; secondary
  lookup via `/api/v1/market-data/stocks-industries` deferred to a
  follow-up (not blocking universe sync)
- `isInternalInstrument` (bool) -- skip if true
- `priceSource` (str) -- raw metadata only; not verified as a currency
  field. Stored in `InstrumentRecord.exchange` as supplementary context
  (e.g. "Nasdaq", "LSE"). `InstrumentRecord.currency` defaults to
  `"USD"` until a reliable currency source is confirmed from real
  API responses

**Candles** (`candles[].candles[]`):
- `fromDate` (datetime) -- parse date portion for `OHLCVBar.price_date`
- `open`, `high`, `low`, `close` (float) -- convert to Decimal
- `volume` (int) -- nullable

**Rates** (`rates[]`):
- `instrumentID` (int) -- maps to `Quote.instrument_id`
- `ask` (float) -- convert to Decimal
- `bid` (float) -- convert to Decimal
- `lastExecution` (float) -- maps to `Quote.last`
- `date` (datetime) -- maps to `Quote.timestamp`

### 3.5 Quote batch chunking

The eToro rates endpoint accepts comma-separated `instrumentIds` with
a maximum of 100 IDs per request. `EtoroMarketDataProvider.get_quotes`
handles this internally: it splits the input list into chunks of 100
and aggregates the results. This is an eToro implementation detail,
not part of the abstract `MarketDataProvider` interface.

### 3.6 Float-to-Decimal conversion

All numeric API values are converted via `Decimal(str(value))`, never
`Decimal(float_value)` directly. This avoids floating-point
representation artifacts. The existing codebase already follows this
pattern.

### 3.7 Raw payload persistence

`_persist_raw` stays. Writing raw API responses to disk before
normalisation is a practical debugging win. Raw payloads are tagged
by instrument ID (e.g. `candles_12345`), not by symbol.

### 3.8 Market data environment routing

Market data endpoints do not vary by environment -- there is no
`/demo/` segment in any market data path. The `env` parameter on
`EtoroMarketDataProvider` is accepted for consistency with the
credential model but does not affect endpoint routing. All market
data requests hit the same paths regardless of whether the stored
credentials are demo or real.

---

## 4. EtoroBrokerProvider rewrite

### 4.1 Constructor

```python
class EtoroBrokerProvider(BrokerProvider):
    def __init__(
        self,
        api_key: str,
        user_key: str,
        env: str = "demo",
    ) -> None:
```

### 4.2 Demo vs real routing

The `env` parameter controls whether trading endpoints include
`/demo/` in the path. Market data endpoints are the same regardless
of environment.

```python
# env="demo" -> "/api/v1/trading/execution/demo/..."
# env="real" -> "/api/v1/trading/execution/..."
```

### 4.3 Endpoint mapping

| Method              | Old (speculative, wrong)            | New (real, demo example)                                                     |
|---------------------|-------------------------------------|------------------------------------------------------------------------------|
| Place order (amount)| `POST /v1/orders`                   | `POST /api/v1/trading/execution/demo/market-open-orders/by-amount`           |
| Place order (units) | same                                | `POST /api/v1/trading/execution/demo/market-open-orders/by-units`            |
| Close position      | `POST /v1/positions/{id}/close`     | `POST /api/v1/trading/execution/demo/market-close-orders/positions/{posId}`  |
| Cancel open order   | none                                | `DELETE /api/v1/trading/execution/demo/market-open-orders/{orderId}`         |
| Cancel close order  | none                                | `DELETE /api/v1/trading/execution/demo/market-close-orders/{orderId}`        |
| Limit order         | none                                | `POST /api/v1/trading/execution/demo/limit-orders`                           |
| Order status        | `GET /v1/orders/{ref}`              | `GET /api/v1/trading/info/demo/orders/{orderId}`                             |
| Portfolio           | none                                | `GET /api/v1/trading/info/demo/portfolio`                                    |
| PnL                 | none                                | `GET /api/v1/trading/info/demo/pnl`                                          |

Real endpoints: same paths without `/demo/`.

### 4.4 Request body

eToro open-order body:

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

v1 constraints: `IsBuy` always `true` (long only), `Leverage` always
`1` (no leverage).

Close-order body:

```json
{
  "InstrumentID": 1,
  "UnitsToDeduct": null
}
```

`UnitsToDeduct` null = close entire position.

### 4.5 Domain action preservation

BUY and ADD both map to `IsBuy=true` on the wire. The provider is a
thin adapter -- it translates `action` to the eToro request shape.
But the original domain action (BUY / ADD / EXIT) is preserved in the
`BrokerOrderResult.raw_payload` and in the audit trail upstream in
`order_client`. The provider does not collapse BUY and ADD into one
semantic.

### 4.6 Position ID resolution for close

The eToro close endpoint uses `positionId` in the path, but
`BrokerProvider.close_position` takes `instrument_id`. The eToro
provider resolves this by calling `GET /api/v1/trading/info/{env}/portfolio`
to find the open position for the given instrument, then uses the
returned position ID in the close request. This lookup happens inside
the provider -- it is an eToro implementation detail, not an interface
change.

### 4.7 BrokerProvider interface

Already uses `instrument_id: int`. No change to the abstract
interface. The `action: str` parameter maps to `IsBuy=true` for
BUY/ADD and the close endpoint for EXIT.

---

## 5. Service layer changes

### 5.1 Market data service

`refresh_market_data` in `app/services/market_data.py`:

- Parameter change: `symbols: list[tuple[str, str]]` becomes
  `instruments: list[tuple[int, str]]` -- `(instrument_id, symbol)`
  where `instrument_id` is the integer and `symbol` is for logging.
- Candle fetch: `provider.get_daily_candles(instrument_id, lookback_days=400)`.
- Quote fetch: `provider.get_quotes(all_instrument_ids)` once for the
  whole batch, then match results by `instrument_id`.

### 5.2 Type corrections

`_upsert_candles` and `_upsert_quote`: `instrument_id` parameter type
changes from `str` to `int`. It was always an integer in the DB; the
`str` was a historical artifact from casting `instrument_id::text` in
the scheduler query.

### 5.3 Scheduler changes

- `_load_etoro_api_key` replaced by `_load_etoro_credentials`.
- Both `api_key` and `user_key` passed to provider constructors.
- Scheduler queries no longer cast `instrument_id::text` -- they pass
  the integer directly.

### 5.4 Credential failure discipline

Missing any required label for the selected environment is a hard
skip with structured log:

```
ERROR nightly_universe_sync: credential 'user_key' for environment 'demo' not found, skipping
```

Never silently fall back from env-specific to unlabelled or global
credentials.

---

## 6. Credential validation endpoint

### 6.1 Endpoint

`POST /broker-credentials/validate`

### 6.2 Contract: transient validation, no persistence

The endpoint validates credentials supplied in the request body. It
does NOT read from `broker_credentials`. The caller passes candidate
`api_key`, `user_key`, and `environment` directly; the endpoint uses
them transiently for the validation calls and returns the result
without persisting anything.

This enables the "test before save" UX: the operator enters
credentials on the setup page, clicks "Test connection", sees the
result, and only then saves. If the endpoint only validated stored
rows, the UX would be "save first, then test" -- backwards for setup.

Request body:

```json
{
  "api_key": "...",
  "user_key": "...",
  "environment": "demo"
}
```

### 6.3 Two validation levels

**Level 1 -- Basic auth validation**:
- Calls `GET /api/v1/me` with the supplied credentials
- Proves: the headers are accepted, the key pair maps to a valid
  identity
- Returns: `gcid`, `realCid`, `demoCid`
- Does NOT prove: environment usability, permission scope, or trading
  capability

**Level 2 -- Environment validation**:
- After `/me` succeeds, calls a safe read-only endpoint scoped to the
  supplied environment:
  - Demo: `GET /api/v1/trading/info/demo/pnl`
  - Real: `GET /api/v1/trading/info/real/pnl`
- Proves: the env-scoped trading-info surface is reachable with these
  credentials
- Does NOT prove: write permission. True write-permission validation
  cannot be done safely without side effects. The endpoint
  acknowledges this limitation explicitly.

### 6.4 Response shape

```json
{
  "auth_valid": true,
  "identity": { "gcid": "...", "demo_cid": "...", "real_cid": "..." },
  "environment": "demo",
  "env_valid": true,
  "env_check": "trading/info/demo/pnl reachable",
  "note": "Does not verify write permission"
}
```

Field names in the response are normalised to snake_case (e.g.
`demo_cid`, `real_cid`) regardless of the raw eToro casing
(`demoCid`, `realCid`).

---

## 7. PR decomposition

### PR A -- Credential identity model + validation endpoint

- Migration: add `environment` column to `broker_credentials`
- Update unique constraint to
  `(operator_id, provider, label, environment) WHERE revoked_at IS NULL`
- `label` and `environment` become required params on
  `load_credential_for_provider_use`
- `_load_etoro_credentials` scheduler helper
- Update migration script: labels `api_key` / `user_key`,
  hard-coded `environment="demo"`
- `POST /broker-credentials/validate` endpoint (basic + env validation)
- Update credential store/list API endpoints for two-key +
  environment entry
- Tests

### PR B -- Market data provider rewrite

- `MarketDataProvider` interface changes: candle by `lookback_days`,
  quote by `instrument_id`, batch `get_quotes`
- `OHLCVBar` / `Quote` dataclass changes (remove symbol, add
  instrument_id on Quote)
- Full `EtoroMarketDataProvider` rewrite: URL, auth, endpoints,
  normalisers, 100-ID chunking in `get_quotes`
- `refresh_market_data` service updates: new parameter types, batch
  quote usage
- Scheduler caller updates: pass both credentials to provider, stop
  casting `instrument_id::text`
- Tests with fixture data matching real API response shapes

### PR C -- Broker provider rewrite

- Full `EtoroBrokerProvider` rewrite: URL, auth, endpoints, request
  bodies, demo/real path routing
- Preserve domain action (BUY/ADD/EXIT) in audit trail before
  translating to eToro's `IsBuy` bool
- Order client integration (minimal -- interface already fits)
- Tests

### PR D -- Frontend updates

- Two-key credential input on setup / settings
- Environment is hard-defaulted to `demo` and not user-selectable in
  the UI until live trading is actually supported. The backend accepts
  `environment` as a parameter (architectural readiness), but the
  frontend does not expose a selector for `real` in v1. This avoids
  confused operators entering real credentials into a system that
  cannot use them yet.
- "Test connection" button calling validate endpoint (test before save)
- Display credential labels + environment in credentials list

---

## Settled decisions preserved

- "eToro is the source of truth for tradable universe, quotes and
  candles in v1" -- preserved.
- "Providers are thin adapters, service layer resolves identifiers" --
  preserved. The service passes `instrument_id` to the provider; no
  DB lookups in the provider.
- "Provider design rule: providers do not own DB lookups" -- preserved.
- "Instrument ID strategy" -- `instruments.instrument_id` (BIGINT PK)
  directly stores eToro's integer ID. No surrogate key.
- "Operator auth and broker-secret storage" -- extended with
  `environment` column; compatible with ADR-0001.

## Review prevention log entries addressed

- "No-op ORDER BY" -- all new queries will be checked.
- "Missing data on hard-rule path silently passes" -- credential
  loading fails loudly if either key is missing. No silent fallback.

---

## What this spec does NOT cover

- Wiring new endpoints that eBull does not currently use (agent
  portfolios, social feeds, watchlists, curated lists).
- Multi-operator support.
- Automatic key rotation.
- Full-text write-permission validation.
- WebSocket streaming (eToro's public API does include a WebSocket
  surface with real-time market data, but it is out of scope for this
  rewrite).
