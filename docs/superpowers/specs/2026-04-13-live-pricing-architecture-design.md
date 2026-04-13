# Live pricing, data freshness, and currency conversion

**Date**: 2026-04-13
**Status**: Approved design (Codex adversarial review: 7 rounds, all findings resolved)

## Problem

eBull's dashboard shows a £22k discrepancy vs eToro. Root causes:

1. **No currency conversion.** All eToro API values are in USD. The backend stores raw USD. The frontend formats with `£` via a hardcoded GBP `Intl.NumberFormat` (`frontend/src/lib/format.ts:11`). A $100k portfolio displays as "£100k" when eToro shows £78k.

2. **No live prices.** Portfolio values depend on `quotes` table data refreshed hourly by a scheduled job. Prices are stale, and the job runs 24/7 including when markets are closed.

3. **Candle freshness bug.** `_candles_are_fresh()` in `app/services/market_data.py:130` uses a 3-day window that misses mid-week gaps (Wednesday candle → Friday job = 2 days → skip → Thursday/Friday missing).

4. **No real-time chart capability.** No WebSocket connection, no SSE, no sub-minute price delivery.

This spec designs the end-to-end architecture for live pricing, currency conversion, and market-hours-aware scheduling.

---

## 1. Three-channel architecture

### 1.1 WebSocket (eToro → backend): real-time prices

A single persistent WebSocket connection to `wss://ws.etoro.com/ws` streams live tick data.

**Topics:**
- `instrument:<id>` — bid, ask, last price per instrument
- `private` — portfolio events (position opened/closed, cash movement)

**Ownership:** Exactly one process owns the WebSocket connection. In a multi-worker deployment, a Postgres advisory lock (`pg_try_advisory_lock(hash('ebull_ws_owner'))`) determines the owner. This extends the existing `JobLock` pattern used by the scheduler.

- On lifespan startup, after starting `JobRuntime`, attempt the advisory lock.
- If acquired: start WebSocket loop as a background asyncio task. Lock held for connection lifetime.
- If not acquired: skip WebSocket startup. Worker serves API requests only.
- On shutdown: lock released automatically when DB connection closes.

**Tick processing pipeline:**
1. Tick arrives → update in-memory dict (`{instrument_id: {bid, ask, last, timestamp}}`)
2. Publish to Redis: `PUBLISH tick:{instrument_id}` (for SSE fanout) + `HSET latest:{instrument_id}` (for late joiners and REST reads)
3. Every ~30 seconds: batch-write in-memory dict to `quotes` table (throttled DB snapshots)

**Private events:** Used as **invalidation triggers only**, not direct writes. When a position-change event arrives, schedule a REST portfolio reconciliation. This avoids duplicates and partial state from streaming events.

### 1.2 REST (eToro → backend): bulk and scheduled operations

| Operation | Schedule | Notes |
|---|---|---|
| Daily candles | 22:00 UTC | After US market close. One job for all instruments. |
| Portfolio reconciliation | 05:30 UTC + on private event trigger | Full position/cash sync from eToro REST API. |
| FX rates refresh | Every 5 minutes | From eToro REST `/rates` endpoint. Writes to `live_fx_rates`. |
| Universe sync | Nightly | Existing job. Extended with FMP currency enrichment. |
| Order execution | On demand | Existing path. No change. |

### 1.3 Frontend ← backend: delivery to the UI

| Channel | Use case | Mechanism |
|---|---|---|
| REST polling | Portfolio page, instrument lists, rankings | React Query, 30–60s refresh interval |
| SSE | Live chart tick overlay | Redis pub/sub fanout from WS-owning worker to API workers |

**SSE flow:**
1. Frontend opens `GET /sse/ticks/{instrument_id}`
2. API worker reads `latest:{instrument_id}` from Redis → sends as initial snapshot
3. API worker subscribes to Redis `tick:{instrument_id}` → forwards each message as SSE event
4. On disconnect: unsubscribe from Redis channel

---

## 2. Currency conversion

### 2.1 Instrument currency enrichment

**Problem:** `etoro.py:258` sets `currency="USD"` as a placeholder for all instruments. `universe.py:75` upserts with `currency = EXCLUDED.currency`, overwriting any enriched value on every sync.

**Resolution:**

a) eToro provider returns `currency=None` instead of `"USD"` placeholder.

b) Universe sync upsert changes to: `currency = COALESCE(EXCLUDED.currency, instruments.currency)`. This preserves existing enriched values when eToro provides null.

c) New column: `instruments.currency_enriched_at TIMESTAMPTZ`. Set only after successful FMP enrichment.

d) FMP profile endpoint (`GET /api/v3/profile/{symbol}`) called during/after universe sync to enrich currency.

e) Enrichment predicate (Python):
```python
instruments_to_enrich = [i for i in instruments
                         if i.currency is None
                         or i.currency_enriched_at is None
                         or i.currency_enriched_at < (now - timedelta(days=90))]
```

The `currency_enriched_at is None` condition catches all existing rows that were never enriched, including rows with the placeholder `currency='USD'`. On first enrichment run, all existing rows get enriched. Failed FMP lookups do not set `currency_enriched_at` — the instrument remains eligible for re-enrichment.

**Symbol mapping:** `instruments.symbol` is the FMP lookup key. Non-US symbols may need an exchange suffix (e.g., `BP.L` for LSE). The design includes exchange-to-suffix mapping using exchange info already stored in the instruments table.

**Fallback:** Instruments not in FMP (eToro-specific, crypto) derive currency from exchange mapping or are flagged in ops monitor for manual review.

### 2.2 Live FX rates

**New table** (separate from the existing `fx_rates` used for tax):

```sql
CREATE TABLE IF NOT EXISTS live_fx_rates (
    from_currency TEXT NOT NULL,
    to_currency   TEXT NOT NULL,
    rate          NUMERIC(18,10) NOT NULL,
    quoted_at     TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (from_currency, to_currency)
);
```

- One row per currency pair, overwritten on each refresh (every 5 minutes).
- Both directions stored explicitly (USD→GBP and GBP→USD) to avoid runtime division.
- Daily job: after market close, copy `live_fx_rates` → `fx_rates` with `rate_date = today` for tax history.
- Existing `fx_rates` table (`sql/013_tax_disposal_matching.sql`) is untouched.

**FX invariant:** `rate` = units of `to_currency` per 1 unit of `from_currency`.
- Row `(from='USD', to='GBP', rate=0.78)`: 1 USD = 0.78 GBP
- Row `(from='GBP', to='USD', rate=1.28)`: 1 GBP = 1.28 USD

**Conversion function:**
```python
def convert(amount: Decimal, from_ccy: str, to_ccy: str, rates: dict) -> Decimal:
    if from_ccy == to_ccy:
        return amount
    key = (from_ccy, to_ccy)
    if key in rates:
        return amount * rates[key]
    inv_key = (to_ccy, from_ccy)
    if inv_key in rates:
        return amount / rates[inv_key]
    raise ValueError(f"No FX rate for {from_ccy} → {to_ccy}")
```

### 2.3 Display currency setting

**Modification to `runtime_config`** (extends the existing typed singleton pattern):

```sql
ALTER TABLE runtime_config
    ADD COLUMN IF NOT EXISTS display_currency TEXT NOT NULL DEFAULT 'GBP';

ALTER TABLE runtime_config_audit
    DROP CONSTRAINT IF EXISTS runtime_config_audit_field_check;
ALTER TABLE runtime_config_audit
    ADD CONSTRAINT runtime_config_audit_field_check
    CHECK (field IN ('enable_auto_trading', 'enable_live_trading',
                     'kill_switch', 'display_currency'));
```

- `app/services/runtime_config.py` updated to expose `display_currency`.
- `PATCH /config` endpoint extended to accept `display_currency` changes.
- Every change audited in `runtime_config_audit` (existing pattern).

### 2.4 Backend conversion contract

All money values in API responses are converted to `display_currency` **before** being sent. The frontend never converts amounts — it only formats (symbol, decimals, grouping).

**Response metadata structure:**

```json
{
  "display_currency": "GBP",
  "fx_rates_used": {
    "USD": { "rate": 0.78, "quoted_at": "2026-04-13T14:30:00Z" },
    "EUR": { "rate": 0.86, "quoted_at": "2026-04-13T14:30:00Z" }
  }
}
```

Rules:
- `fx_rates_used` is a map keyed by source currency (native currency converted FROM).
- Target currency is always `display_currency` (implicit).
- Only currencies actually used in the response are included.
- If `display_currency` matches native currency, that currency is omitted from the map.
- Single-instrument endpoints may flatten to `fx_rate` + `fx_quoted_at` for simplicity; portfolio/list endpoints always use the map.

### 2.5 Frontend formatting

**Current state:** `format.ts:11` hardcodes `currency: "GBP"` in `Intl.NumberFormat`.

**Change:** `formatMoney()` gains a `currency` parameter:

```typescript
const formatters: Record<string, Intl.NumberFormat> = {};
function getFormatter(currency: string): Intl.NumberFormat {
  if (!formatters[currency]) {
    formatters[currency] = new Intl.NumberFormat("en-GB", {
      style: "currency",
      currency,
      maximumFractionDigits: 2,
    });
  }
  return formatters[currency];
}

export function formatMoney(
  value: number | null | undefined,
  currency = "GBP"
): string {
  if (value === null || value === undefined) return "—";
  return getFormatter(currency).format(value);
}
```

**React context:** `DisplayCurrencyContext` provides `display_currency` to all components:
- On app load, fetch `/config` → get `display_currency`.
- Store in context. All `formatMoney` calls read from context.
- When operator changes `display_currency` in settings, context updates and all pages re-render.

**Migration plan:** The `currency = "GBP"` default is a temporary migration aid. Implementation plan will include auditing all `formatMoney()` call sites, removing the default after migration, and adding a lint rule or test to catch unparameterised calls.

---

## 3. Live charts

### 3.1 TradingView Lightweight Charts

Library: `lightweight-charts` (npm). Renders candlestick + line charts in the browser.

**Data sources:**
- **Historical candles** (daily): from `price_daily` table, fetched via REST on chart load.
- **Intraday candles** (1-min, today only): from `price_intraday` table (optional, populated from WebSocket ticks aggregated into 1-min OHLCV bars).
- **Live edge**: SSE ticks from `GET /sse/ticks/{instrument_id}`, applied as real-time updates to the rightmost candle.

### 3.2 SSE via Redis pub/sub

The WS-owning worker publishes ticks to Redis. API workers serving SSE connections subscribe and forward. This decouples the WebSocket owner from the HTTP request handlers.

**Late joiners:** On SSE connect, read `latest:{instrument_id}` from Redis hash for an immediate snapshot before subscribing to the channel.

---

## 4. Market hours awareness

### 4.1 Adaptive detection

No hardcoded holiday calendar. The WebSocket connection is inherently adaptive — no ticks arrive when markets are closed.

For scheduled jobs:
- Daily candle job at 22:00 UTC (after US market close).
- If the fetch returns nothing new, accept the previous day's data silently.
- Per-instrument staleness tracking in ops monitor with expected-freshness rules.

### 4.2 Candle freshness fix

**Current bug:** `_candles_are_fresh()` at `market_data.py:130` uses `(today - latest_date).days <= 3`. Misses mid-week gaps.

**Fix:** Replace with `_most_recent_trading_day()` — compare latest candle date to the most recent weekday:

```python
def _most_recent_trading_day(today: date) -> date:
    """Return the most recent weekday (Mon-Fri)."""
    weekday = today.weekday()  # 0=Mon, 6=Sun
    if weekday == 5:  # Saturday
        return today - timedelta(days=1)
    if weekday == 6:  # Sunday
        return today - timedelta(days=2)
    return today

def _candles_are_fresh(latest_date: date, today: date) -> bool:
    return latest_date >= _most_recent_trading_day(today)
```

No holiday calendar — if a holiday causes a gap, the next run fetches the missing data. Holidays don't cause false staleness alerts because the candle endpoint simply returns nothing new.

### 4.3 Hourly market refresh changes

The existing `JOB_HOURLY_MARKET_REFRESH` (every hour at :05) currently fetches candles + quotes for all Tier 1/2 instruments. With WebSocket providing live quotes:

- **Remove** quote fetching from the hourly job (WebSocket replaces it).
- **Move** candle fetching to a single daily job at 22:00 UTC.
- The hourly job is either removed entirely or repurposed for non-price tasks.

---

## 5. Redis as IPC layer

### 5.1 Why Redis

API workers cannot read the WS-owning worker's in-memory dict (separate processes). Redis is the IPC layer — not optional, the primary design.

### 5.2 Architecture

1. WS-owning worker publishes each tick:
   - `PUBLISH tick:{instrument_id}` (SSE fanout)
   - `HSET latest:{instrument_id}` with fields `ask`, `bid`, `last`, `timestamp` (late joiners + REST reads)

2. API workers serving SSE: subscribe to Redis channels, forward as SSE events.

3. API workers serving REST `/portfolio`: read `latest:{instrument_id}` from Redis for current prices. Fall back to `quotes` table if Redis is unavailable.

4. DB snapshot writes (every ~30s) from WS-owning worker for:
   - Pages that don't need sub-second freshness (rankings, instruments list)
   - Persistence across Redis restarts
   - Ops monitor historical data

### 5.3 Dependencies

- Python: `redis[hiredis]` in `pyproject.toml`
- Docker: `redis:7-alpine` service in `docker-compose.yml`
- Config: `REDIS_URL` setting (default `redis://localhost:6379/0`)

---

## 6. Schema changes

### 6.1 New tables

```sql
-- Live FX rates for display conversion
CREATE TABLE IF NOT EXISTS live_fx_rates (
    from_currency TEXT NOT NULL,
    to_currency   TEXT NOT NULL,
    rate          NUMERIC(18,10) NOT NULL,
    quoted_at     TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (from_currency, to_currency)
);

-- Broker WebSocket events (for reconciliation audit)
CREATE TABLE IF NOT EXISTS broker_events (
    event_id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    broker_event_type TEXT NOT NULL,
    broker_ref       TEXT UNIQUE,
    raw_payload      JSONB NOT NULL,
    received_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    reconciled       BOOLEAN NOT NULL DEFAULT FALSE
);

-- Intraday price bars (optional, for intraday chart history)
CREATE TABLE IF NOT EXISTS price_intraday (
    instrument_id BIGINT NOT NULL REFERENCES instruments(instrument_id),
    candle_time   TIMESTAMPTZ NOT NULL,
    open          NUMERIC(18,6) NOT NULL,
    high          NUMERIC(18,6) NOT NULL,
    low           NUMERIC(18,6) NOT NULL,
    close         NUMERIC(18,6) NOT NULL,
    volume        BIGINT,
    PRIMARY KEY (instrument_id, candle_time)
);
```

### 6.2 Modified tables

```sql
-- instruments: add enrichment tracking
ALTER TABLE instruments
    ADD COLUMN IF NOT EXISTS currency_enriched_at TIMESTAMPTZ;

-- runtime_config: add display currency
ALTER TABLE runtime_config
    ADD COLUMN IF NOT EXISTS display_currency TEXT NOT NULL DEFAULT 'GBP';

-- runtime_config_audit: extend field CHECK
ALTER TABLE runtime_config_audit
    DROP CONSTRAINT IF EXISTS runtime_config_audit_field_check;
ALTER TABLE runtime_config_audit
    ADD CONSTRAINT runtime_config_audit_field_check
    CHECK (field IN ('enable_auto_trading', 'enable_live_trading',
                     'kill_switch', 'display_currency'));
```

### 6.3 Unchanged tables

- `quotes` — write cadence changes (WebSocket → throttled snapshots), no schema change.
- `fx_rates` — untouched, tax use only (`sql/013_tax_disposal_matching.sql`).
- `price_daily` — no change, daily candle job still writes here.

---

## 7. New dependencies

| Dependency | Where | Purpose |
|---|---|---|
| `redis[hiredis]` | `pyproject.toml` | Redis client with C extension for pub/sub IPC |
| `redis:7-alpine` | `docker-compose.yml` | Redis server |
| `websockets` | `pyproject.toml` | eToro WebSocket client |
| `lightweight-charts` | `frontend/package.json` | TradingView chart rendering |

---

## 8. eToro API rate limits (permanent reference)

| Method | Limit | Window |
|---|---|---|
| GET | 60 requests/min | Rolling 1-minute |
| POST | 20 requests/min | Rolling 1-minute |

**Current safeguard:** `app/providers/resilient_client.py` enforces `min_request_interval_s = 1.1` (54 GET/min, ~8% headroom). Shared timestamp coordinates throttling across read/write clients. Retry on 429/5xx with backoff.

**Design impact:** WebSocket replaces the majority of REST polling. Remaining REST calls (daily candles, portfolio reconciliation, FX rates, universe sync) are well within limits.

---

## 9. Non-goals (explicit scope boundaries)

- **No intraday candle history beyond today.** `price_intraday` is optional and only stores today's bars. Historical intraday data is not fetched or stored.
- **No multi-user support.** Single operator, single `display_currency` setting.
- **No frontend currency conversion.** All conversion happens in the backend. The frontend only formats.
- **No holiday calendar.** Adaptive detection via WebSocket tick arrival and weekday-based freshness checks.
- **No Redis persistence requirements.** Redis is a cache/IPC layer. Loss of Redis falls back to DB reads. No data is exclusively in Redis.

---

## 10. Codex review history

| Round | Findings | Status |
|---|---|---|
| 1 | 12 (5 blockers, 7 warnings): tick-to-DB thrashing, private events as direct writes, in-memory cache, REST fallback, FX source, candle freshness, etc. | All resolved |
| 2 | 6: scheduler topology, SSE IPC model, fx_rates table conflict, FX direction ambiguity, display currency storage, FMP enrichment gap | All resolved |
| 3 | 2: FMP currency enrichment overwrite, frontend formatMoney hardcoded GBP | All resolved |
| 4 | 0 new, but Codex confused design spec with code review | Clarified with round 5 prompt |
| 5 | 2 clarification requests: backfill predicate for existing USD rows, backend vs frontend conversion contract | Addressed in round 6 |
| 6 | 1 detail: FX metadata should be a rates map, not a single rate | Addressed in round 7 |
| 7 | 0 | **READY TO PLAN** |
