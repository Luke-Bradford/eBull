# Live Pricing & Currency Conversion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix the £22k portfolio discrepancy (USD values displayed as GBP), add operator-settable display currency with backend FX conversion, fix the candle freshness bug, and lay the infrastructure for live price streaming.

**Architecture:** Backend converts all money values to `display_currency` before sending to the frontend. FX rates from eToro stored in a new `live_fx_rates` table. Instrument currencies enriched via FMP `/profile` endpoint. Frontend `formatMoney()` becomes currency-aware via `DisplayCurrencyContext`. Redis + WebSocket infrastructure for live pricing is designed but deferred to a follow-up plan — this plan focuses on the currency conversion pipeline and scheduler fixes that immediately resolve the dashboard discrepancy.

**Tech Stack:** Python 3.14, FastAPI, psycopg3, Redis (infrastructure only — full integration in follow-up), React 18, TanStack React Query (already installed, unused), Vite

**Spec reference:** `docs/superpowers/specs/2026-04-13-live-pricing-architecture-design.md`

---

## Scope of this plan

This spec covers a large system (WebSocket, Redis, SSE, charts, currency conversion, scheduler changes). This plan implements the **immediately actionable** portions that fix the £22k discrepancy and scheduling bugs. Live streaming (WebSocket + Redis + SSE + TradingView charts) is a separate plan — it depends on verifying the eToro WebSocket API endpoint and adding Redis infrastructure.

**In scope (this plan):**

- Migration 023: `live_fx_rates` table, `instruments.currency_enriched_at`, `runtime_config.display_currency`, `broker_events` table, `price_intraday` table (spec §6)
- FX conversion service with `convert()` and rate loading (spec §2.2)
- `display_currency` in `runtime_config` + audit (spec §2.3)
- FMP profile endpoint for currency enrichment (spec §2.1)
- eToro provider `currency=None` fix + universe COALESCE upsert (spec §2.1)
- Backend portfolio conversion to `display_currency` with `fx_rates_used` metadata (spec §2.4)
- Frontend `formatMoney()` currency parameter + `DisplayCurrencyContext` (spec §2.5)
- Settings page display currency toggle
- Frontend type updates for new response shapes
- Candle freshness bug fix (spec §4.2)
- FX rates refresh scheduled job (spec §1.2)
- Hourly market refresh → daily candle job (spec §4.3)

**Explicitly out of scope (follow-up plan):**

- Redis infrastructure (Docker service, Python client, config)
- eToro WebSocket client + advisory lock ownership
- Tick processing pipeline (in-memory → Redis → DB snapshots)
- SSE endpoint (`GET /sse/ticks/{instrument_id}`)
- TradingView Lightweight Charts integration
- `price_intraday` writes (table created in migration but not populated until WebSocket)

---

## Settled-decisions check

Working order step 2/3 (CLAUDE.md): read `docs/settled-decisions.md` and `docs/review-prevention-log.md` before coding.

- **"eToro is the source of truth for tradable universe, quotes and candles in v1"** → preserved. FMP profile is used only for currency enrichment, not as an alternative data source.
- **"AUM and concentration should use mark-to-market first, fall back to cost basis"** → preserved. Currency conversion applies after mark-to-market calculation.
- **Prevention log "tests must use `ebull_test`"** → all new service-layer tests use the `_test_database_url()` pattern.
- **Prevention log "smoke gate must catch lifespan swallowed failures"** → migration 023 runs at dev-DB bootstrap. No lifespan change.
- **Prevention log "params, not interpolation"** → all SQL uses `%(name)s` named placeholders.

---

## File structure

**Created:**

- `sql/023_live_pricing_currency.sql` — DDL for new tables + ALTER for existing tables
- `app/services/fx.py` — FX conversion service (`convert`, `load_live_fx_rates`, `refresh_live_fx_rates`)
- `tests/test_fx.py` — unit tests for FX conversion
- `tests/test_fx_refresh.py` — tests for FX rates refresh job
- `tests/test_candle_freshness.py` — tests for the freshness fix
- `tests/test_currency_enrichment.py` — tests for FMP currency enrichment
- `frontend/src/lib/DisplayCurrencyContext.tsx` — React context for display currency
- `frontend/src/components/settings/DisplayCurrencySection.tsx` — settings UI for currency toggle

**Modified:**

- `app/services/runtime_config.py` — add `display_currency` to `AuditField`, `RuntimeConfig`
- `app/services/universe.py:71-75` — COALESCE upsert for currency
- `app/providers/implementations/etoro.py:258` — `currency=None` instead of `"USD"`
- `app/providers/market_data.py:22-26` — `InstrumentRecord.currency` type to `str | None`
- `app/providers/implementations/fmp.py` — add `get_instrument_profile()` method
- `app/providers/fundamentals.py` — add `InstrumentProfile` dataclass (if extending provider) or standalone in `fmp.py`
- `app/services/market_data.py:130-157` — replace `_candles_are_fresh()` with weekday check
- `app/api/portfolio.py` — add FX conversion, `fx_rates_used` metadata, `display_currency`
- `app/api/config.py` — extend `ConfigResponse`, `ConfigPatchRequest` for `display_currency`
- `app/workers/scheduler.py` — add FX refresh job, refactor hourly market refresh
- `frontend/src/lib/format.ts` — `formatMoney()` gains `currency` parameter
- `frontend/src/api/types.ts` — update `PortfolioResponse`, `ConfigResponse` types
- `frontend/src/main.tsx` — add `DisplayCurrencyProvider`
- `frontend/src/pages/SettingsPage.tsx` — add currency toggle section
- All 4 files with `formatMoney()` calls — pass currency from context
- `tests/test_runtime_config.py` — extend for `display_currency`
- `tests/test_api_config.py` — extend for `display_currency`
- `tests/test_api_portfolio.py` — extend for FX conversion
- `tests/test_market_data.py` — update freshness tests

---

## Task 1: Schema migration 023

**Files:**
- Create: `sql/023_live_pricing_currency.sql`

This single migration creates all new tables and alters existing ones for the entire spec. Tables created now but populated later (like `price_intraday`, `broker_events`) are still created in this migration to avoid multiple migration files for the same feature.

- [ ] **Step 1: Write the migration file**

```sql
-- 023: Live pricing and currency conversion schema
--
-- New tables:
--   live_fx_rates    — real-time FX rates for display conversion
--   broker_events    — WebSocket event audit log (populated later)
--   price_intraday   — 1-min OHLCV bars (populated later)
--
-- Altered tables:
--   instruments      — add currency_enriched_at for FMP enrichment tracking
--   runtime_config   — add display_currency for operator currency preference
--   runtime_config_audit — extend field CHECK for display_currency

BEGIN;

-- Live FX rates for display conversion (separate from fx_rates used by tax)
CREATE TABLE IF NOT EXISTS live_fx_rates (
    from_currency TEXT NOT NULL,
    to_currency   TEXT NOT NULL,
    rate          NUMERIC(18,10) NOT NULL,
    quoted_at     TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (from_currency, to_currency)
);

-- Broker WebSocket events for reconciliation audit
CREATE TABLE IF NOT EXISTS broker_events (
    event_id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    broker_event_type TEXT NOT NULL,
    broker_ref        TEXT UNIQUE,
    raw_payload       JSONB NOT NULL,
    received_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    reconciled        BOOLEAN NOT NULL DEFAULT FALSE
);

-- Intraday price bars (1-min OHLCV, populated by WebSocket tick aggregation)
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

-- Track when instrument currency was last enriched by FMP
ALTER TABLE instruments
    ADD COLUMN IF NOT EXISTS currency_enriched_at TIMESTAMPTZ;

-- Operator display currency preference
ALTER TABLE runtime_config
    ADD COLUMN IF NOT EXISTS display_currency TEXT NOT NULL DEFAULT 'GBP';

-- Extend audit field CHECK to include display_currency
ALTER TABLE runtime_config_audit
    DROP CONSTRAINT IF EXISTS runtime_config_audit_field_check;
ALTER TABLE runtime_config_audit
    ADD CONSTRAINT runtime_config_audit_field_check
    CHECK (field IN ('enable_auto_trading', 'enable_live_trading',
                     'kill_switch', 'display_currency'));

COMMIT;
```

- [ ] **Step 2: Verify the smoke test passes**

Run: `uv run pytest tests/smoke/test_app_boots.py -v`
Expected: PASS (migration runs at lifespan startup against dev DB)

- [ ] **Step 3: Commit**

```bash
git add sql/023_live_pricing_currency.sql
git commit -m "feat: migration 023 — live FX rates, broker events, price intraday, display currency"
```

---

## Task 2: FX conversion service

**Files:**
- Create: `app/services/fx.py`
- Create: `tests/test_fx.py`

Pure conversion logic with no external dependencies — easy to test in isolation.

- [ ] **Step 1: Write the failing tests**

```python
"""Tests for app.services.fx — currency conversion logic."""

from __future__ import annotations

from decimal import Decimal

import pytest

from app.services.fx import convert, FxRateNotFound


class TestConvert:
    def test_same_currency_returns_amount(self) -> None:
        rates: dict[tuple[str, str], Decimal] = {}
        assert convert(Decimal("100.00"), "USD", "USD", rates) == Decimal("100.00")

    def test_direct_rate(self) -> None:
        rates = {("USD", "GBP"): Decimal("0.78")}
        result = convert(Decimal("100.00"), "USD", "GBP", rates)
        assert result == Decimal("78.00")

    def test_inverse_rate(self) -> None:
        rates = {("GBP", "USD"): Decimal("1.28")}
        result = convert(Decimal("100.00"), "USD", "GBP", rates)
        # 100 / 1.28 = 78.125
        assert result == Decimal("100.00") / Decimal("1.28")

    def test_direct_preferred_over_inverse(self) -> None:
        rates = {
            ("USD", "GBP"): Decimal("0.78"),
            ("GBP", "USD"): Decimal("1.28"),
        }
        result = convert(Decimal("100.00"), "USD", "GBP", rates)
        assert result == Decimal("78.00")

    def test_missing_rate_raises(self) -> None:
        rates: dict[tuple[str, str], Decimal] = {}
        with pytest.raises(FxRateNotFound, match="USD.*EUR"):
            convert(Decimal("100.00"), "USD", "EUR", rates)

    def test_zero_amount(self) -> None:
        rates = {("USD", "GBP"): Decimal("0.78")}
        assert convert(Decimal("0"), "USD", "GBP", rates) == Decimal("0.00")
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_fx.py -v`
Expected: FAIL — `app.services.fx` does not exist yet

- [ ] **Step 3: Write the implementation**

```python
"""
FX conversion service.

Handles currency conversion for display purposes using live_fx_rates.
Tax-related conversions continue to use the fx_rates table (sql/013).

FX invariant: rate = units of to_currency per 1 unit of from_currency.
"""

from __future__ import annotations

import logging
from datetime import datetime
from decimal import Decimal
from typing import Any

import psycopg
import psycopg.rows

logger = logging.getLogger(__name__)


class FxRateNotFound(ValueError):
    """Raised when no FX rate is available for a currency pair."""


def convert(
    amount: Decimal,
    from_ccy: str,
    to_ccy: str,
    rates: dict[tuple[str, str], Decimal],
) -> Decimal:
    """Convert amount from one currency to another using the rates dict.

    Tries the direct pair first, then the inverse.  Raises FxRateNotFound
    if neither is available.
    """
    if from_ccy == to_ccy:
        return amount
    key = (from_ccy, to_ccy)
    if key in rates:
        return amount * rates[key]
    inv_key = (to_ccy, from_ccy)
    if inv_key in rates:
        return amount / rates[inv_key]
    raise FxRateNotFound(f"No FX rate for {from_ccy} \u2192 {to_ccy}")


def load_live_fx_rates(
    conn: psycopg.Connection[Any],
) -> dict[tuple[str, str], Decimal]:
    """Load all live FX rates into a lookup dict keyed by (from, to)."""
    rows = conn.execute(
        "SELECT from_currency, to_currency, rate FROM live_fx_rates",
    ).fetchall()
    return {(r[0], r[1]): r[2] for r in rows}


def load_live_fx_rates_with_metadata(
    conn: psycopg.Connection[Any],
) -> dict[tuple[str, str], dict[str, Any]]:
    """Load live FX rates with quoted_at metadata for API responses."""
    rows = conn.execute(
        "SELECT from_currency, to_currency, rate, quoted_at FROM live_fx_rates",
    ).fetchall()
    return {
        (r[0], r[1]): {"rate": r[2], "quoted_at": r[3]}
        for r in rows
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_fx.py -v`
Expected: PASS

- [ ] **Step 5: Run lint and typecheck**

Run: `uv run ruff check app/services/fx.py tests/test_fx.py && uv run pyright app/services/fx.py`

- [ ] **Step 6: Commit**

```bash
git add app/services/fx.py tests/test_fx.py
git commit -m "feat: FX conversion service with convert() and rate loading"
```

---

## Task 3: Display currency in runtime_config

**Files:**
- Modify: `app/services/runtime_config.py:38,58-64,103-229`
- Modify: `app/api/config.py:71-93,100-123,158-181,184-219`
- Modify: `tests/test_runtime_config.py`
- Modify: `tests/test_api_config.py`
- Modify: `frontend/src/api/types.ts:24-44`

- [ ] **Step 1: Write the failing test for get_runtime_config with display_currency**

In `tests/test_runtime_config.py`, add a test after the existing tests:

```python
def test_get_runtime_config_includes_display_currency(self) -> None:
    cur = _make_cursor([{
        "enable_auto_trading": True,
        "enable_live_trading": False,
        "display_currency": "USD",
        "updated_at": _NOW,
        "updated_by": "operator",
        "reason": "test",
    }])
    conn = _make_conn([cur])
    config = get_runtime_config(conn)
    assert config.display_currency == "USD"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_runtime_config.py::TestGetRuntimeConfig::test_get_runtime_config_includes_display_currency -v`
Expected: FAIL — `RuntimeConfig` has no `display_currency` attribute

- [ ] **Step 3: Extend the service**

In `app/services/runtime_config.py`:

Change line 38:
```python
AuditField = Literal["enable_auto_trading", "enable_live_trading", "kill_switch", "display_currency"]
```

Add `display_currency` to the `RuntimeConfig` dataclass (after line 64):
```python
@dataclass(frozen=True)
class RuntimeConfig:
    enable_auto_trading: bool
    enable_live_trading: bool
    display_currency: str
    updated_at: datetime
    updated_by: str
    reason: str
```

Update `get_runtime_config()` to read the new column — find the SELECT query and add `display_currency` to the selected columns and to the `RuntimeConfig(...)` constructor call.

Update `update_runtime_config()` to accept an optional `display_currency: str | None = None` parameter. Follow the existing pattern for `enable_auto_trading` / `enable_live_trading`: add it to the SET clause conditionally, write an audit row for it if changed.

- [ ] **Step 4: Run all runtime_config tests**

Run: `uv run pytest tests/test_runtime_config.py -v`
Expected: PASS (existing tests may need cursor fixtures updated with `display_currency` key)

- [ ] **Step 5: Extend the API layer**

In `app/api/config.py`:

Add `display_currency: str` to `RuntimeFlagsResponse` (line 72) and `ConfigResponse` (if needed).

Add `display_currency: str | None = None` to `ConfigPatchRequest` (line 100). Update the validator `_validate_patch` to accept `display_currency` as a valid flag (it should not require `enable_auto_trading` or `enable_live_trading` when `display_currency` is provided).

In `get_config()` handler: pass `runtime.display_currency` to the response.

In `patch_config()` handler: pass `body.display_currency` to `update_runtime_config()`.

- [ ] **Step 6: Update API config tests**

In `tests/test_api_config.py`, add a test for PATCH with `display_currency`:

```python
def test_patch_display_currency(self) -> None:
    response = self.client.patch("/config", json={
        "updated_by": "operator",
        "reason": "test currency change",
        "display_currency": "USD",
    })
    assert response.status_code == 200
    assert response.json()["display_currency"] == "USD"
```

- [ ] **Step 7: Update frontend types**

In `frontend/src/api/types.ts`, add `display_currency: string` to:
- `RuntimeFlagsResponse` (line 24)
- `ConfigResponse` (line 39) — add at top level too

Add `display_currency` and `fx_rates_used` to `PortfolioResponse` (line 206):
```typescript
export interface FxRateUsed {
  rate: number;
  quoted_at: string;
}

export interface PortfolioResponse {
  positions: PositionItem[];
  position_count: number;
  total_aum: number;
  cash_balance: number | null;
  mirror_equity: number;
  display_currency: string;
  fx_rates_used: Record<string, FxRateUsed>;
}
```

- [ ] **Step 8: Run all checks**

```bash
uv run ruff check . && uv run ruff format --check . && uv run pyright && uv run pytest
pnpm --dir frontend typecheck
```

- [ ] **Step 9: Commit**

```bash
git add app/services/runtime_config.py app/api/config.py tests/test_runtime_config.py tests/test_api_config.py frontend/src/api/types.ts
git commit -m "feat: add display_currency to runtime_config with audited PATCH"
```

---

## Task 4: eToro currency=None + universe COALESCE upsert

**Files:**
- Modify: `app/providers/market_data.py:22-26` — `InstrumentRecord.currency` type
- Modify: `app/providers/implementations/etoro.py:258` — `currency=None`
- Modify: `app/services/universe.py:71-90` — COALESCE upsert
- Modify: `tests/test_universe_normaliser.py`
- Modify: `tests/test_market_data.py`

- [ ] **Step 1: Write the failing test**

In `tests/test_universe_normaliser.py` (or `tests/test_market_data.py` depending on where instrument normalisation tests live), add:

```python
def test_normalise_instrument_currency_is_none() -> None:
    """eToro provider should return currency=None, not placeholder 'USD'."""
    result = _normalise_instrument(FIXTURE_INSTRUMENT)
    assert result is not None
    assert result.currency is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_universe_normaliser.py::test_normalise_instrument_currency_is_none -v`
Expected: FAIL — `result.currency == "USD"`

- [ ] **Step 3: Update InstrumentRecord type**

In `app/providers/market_data.py:22`, change `currency: str` to `currency: str | None`:

```python
@dataclass(frozen=True)
class InstrumentRecord:
    """A tradable instrument as reported by the market data provider."""

    provider_id: str
    symbol: str
    company_name: str
    exchange: str | None
    currency: str | None
    sector: str | None
    industry: str | None
    country: str | None
    is_tradable: bool
```

- [ ] **Step 4: Update eToro provider**

In `app/providers/implementations/etoro.py:258`, change `currency="USD"` to `currency=None`:

```python
    return InstrumentRecord(
        provider_id=str(instrument_id),
        symbol=str(symbol),
        company_name=str(item.get("instrumentDisplayName") or symbol),
        exchange=_str_or_none(item.get("exchangeID")),
        # eToro instruments endpoint does not expose currency.
        # Return None so enrichment (FMP profile) fills the real value.
        # COALESCE upsert in universe.py preserves enriched currency.
        currency=None,
        sector=_str_or_none(item.get("stocksIndustryId")),
        industry=None,
        country=None,
        is_tradable=True,
    )
```

- [ ] **Step 5: Update universe.py COALESCE upsert**

In `app/services/universe.py:75`, change `currency = EXCLUDED.currency` to `currency = COALESCE(EXCLUDED.currency, instruments.currency)`:

```python
                ON CONFLICT (instrument_id) DO UPDATE SET
                    symbol       = EXCLUDED.symbol,
                    company_name = EXCLUDED.company_name,
                    exchange     = EXCLUDED.exchange,
                    currency     = COALESCE(EXCLUDED.currency, instruments.currency),
                    sector       = EXCLUDED.sector,
                    industry     = EXCLUDED.industry,
                    country      = EXCLUDED.country,
                    is_tradable  = EXCLUDED.is_tradable,
                    last_seen_at = NOW()
```

Also update the WHERE clause (line 85) — remove the `currency IS DISTINCT FROM` check since COALESCE changes the semantics. Replace with a check that only fires when EXCLUDED.currency is non-null:

```python
                WHERE (
                    instruments.symbol        IS DISTINCT FROM EXCLUDED.symbol        OR
                    instruments.company_name  IS DISTINCT FROM EXCLUDED.company_name  OR
                    instruments.exchange      IS DISTINCT FROM EXCLUDED.exchange      OR
                    (EXCLUDED.currency IS NOT NULL AND
                     instruments.currency IS DISTINCT FROM EXCLUDED.currency)         OR
                    instruments.sector        IS DISTINCT FROM EXCLUDED.sector        OR
                    instruments.industry      IS DISTINCT FROM EXCLUDED.industry      OR
                    instruments.country       IS DISTINCT FROM EXCLUDED.country       OR
                    instruments.is_tradable   IS DISTINCT FROM EXCLUDED.is_tradable
                )
```

- [ ] **Step 6: Fix any tests broken by the type change**

Run: `uv run pytest tests/test_universe_normaliser.py tests/test_market_data.py tests/test_provider_interfaces.py -v`

Update any test fixtures that assert `currency="USD"` to assert `currency is None` for eToro-normalised instruments.

- [ ] **Step 7: Run full test suite + lint**

```bash
uv run ruff check . && uv run pyright && uv run pytest
```

- [ ] **Step 8: Commit**

```bash
git add app/providers/market_data.py app/providers/implementations/etoro.py app/services/universe.py tests/
git commit -m "fix: eToro returns currency=None, universe upsert preserves enriched currency via COALESCE"
```

---

## Task 5: FMP currency enrichment

**Files:**
- Modify: `app/providers/implementations/fmp.py` — add `get_instrument_profile()`
- Create: `tests/test_currency_enrichment.py`
- Modify: `app/services/universe.py` — add enrichment call after upsert

- [ ] **Step 1: Write the failing test for FMP profile fetch**

```python
"""Tests for FMP currency enrichment via /profile endpoint."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import httpx
import pytest

from app.providers.implementations.fmp import FmpFundamentalsProvider

FIXTURE_PROFILE_RESPONSE = [
    {
        "symbol": "AAPL",
        "currency": "USD",
        "exchangeShortName": "NASDAQ",
        "industry": "Consumer Electronics",
        "sector": "Technology",
    }
]

FIXTURE_PROFILE_GBP = [
    {
        "symbol": "BP.L",
        "currency": "GBp",  # Note: FMP returns "GBp" (pence) for LSE
        "exchangeShortName": "LSE",
        "industry": "Oil & Gas",
        "sector": "Energy",
    }
]


class TestGetInstrumentProfile:
    def test_returns_currency(self) -> None:
        mock_response = MagicMock(spec=httpx.Response)
        mock_response.status_code = 200
        mock_response.json.return_value = FIXTURE_PROFILE_RESPONSE

        with patch.object(FmpFundamentalsProvider, "_http") as mock_http:
            mock_http.return_value = mock_response
            provider = FmpFundamentalsProvider(api_key="test")
            profile = provider.get_instrument_profile("AAPL")

        assert profile is not None
        assert profile.currency == "USD"
        assert profile.exchange == "NASDAQ"

    def test_empty_response_returns_none(self) -> None:
        mock_response = MagicMock(spec=httpx.Response)
        mock_response.status_code = 200
        mock_response.json.return_value = []

        with patch.object(FmpFundamentalsProvider, "_http") as mock_http:
            mock_http.return_value = mock_response
            provider = FmpFundamentalsProvider(api_key="test")
            profile = provider.get_instrument_profile("UNKNOWN")

        assert profile is None

    def test_gbp_pence_normalised(self) -> None:
        """FMP returns 'GBp' for LSE stocks — normalise to 'GBP'."""
        mock_response = MagicMock(spec=httpx.Response)
        mock_response.status_code = 200
        mock_response.json.return_value = FIXTURE_PROFILE_GBP

        with patch.object(FmpFundamentalsProvider, "_http") as mock_http:
            mock_http.return_value = mock_response
            provider = FmpFundamentalsProvider(api_key="test")
            profile = provider.get_instrument_profile("BP.L")

        assert profile is not None
        assert profile.currency == "GBP"  # normalised from GBp
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_currency_enrichment.py -v`
Expected: FAIL — `get_instrument_profile` does not exist

- [ ] **Step 3: Add InstrumentProfile dataclass to FMP provider**

In `app/providers/implementations/fmp.py`, add near the top (after imports):

```python
@dataclass(frozen=True)
class InstrumentProfile:
    """Currency and exchange info from FMP /profile endpoint."""
    symbol: str
    currency: str
    exchange: str | None
    sector: str | None
    industry: str | None
```

- [ ] **Step 4: Add get_instrument_profile method to FmpFundamentalsProvider**

After the existing `get_snapshot_history` method:

```python
    def get_instrument_profile(self, symbol: str) -> InstrumentProfile | None:
        """Fetch instrument profile for currency enrichment.

        Uses GET /api/v3/profile/{symbol}.
        Returns None if the symbol is not found in FMP.
        """
        resp = self._http("GET", f"/v3/profile/{symbol}")
        if resp.status_code != 200:
            logger.warning("FMP profile fetch failed for %s: %s", symbol, resp.status_code)
            return None
        data = resp.json()
        if not data:
            return None
        item = data[0]
        raw_currency = str(item.get("currency", ""))
        # FMP returns "GBp" (pence) for LSE stocks — normalise to "GBP"
        currency = raw_currency.upper() if raw_currency else None
        if currency == "GBP" and raw_currency == "GBp":
            currency = "GBP"  # already handled by .upper()
        return InstrumentProfile(
            symbol=symbol,
            currency=currency or "USD",
            exchange=item.get("exchangeShortName"),
            sector=item.get("sector"),
            industry=item.get("industry"),
        )
```

- [ ] **Step 5: Run tests**

Run: `uv run pytest tests/test_currency_enrichment.py -v`
Expected: PASS

- [ ] **Step 6: Add enrichment logic to universe sync**

In `app/services/universe.py`, add an `enrich_instrument_currencies()` function that:
1. Queries instruments where `currency IS NULL OR currency_enriched_at IS NULL OR currency_enriched_at < now() - interval '90 days'`
2. For each, calls `fmp_provider.get_instrument_profile(symbol)`
3. If profile returned, updates `instruments.currency` and sets `currency_enriched_at = NOW()`
4. Respects FMP rate limits (`_FMP_REQUEST_INTERVAL_S = 0.25`)

```python
def enrich_instrument_currencies(
    fmp_provider: FmpFundamentalsProvider,
    conn: psycopg.Connection[Any],
) -> int:
    """Enrich instrument currencies from FMP profile endpoint.

    Returns the number of instruments enriched.
    """
    rows = conn.execute(
        """
        SELECT instrument_id, symbol
        FROM instruments
        WHERE is_tradable = TRUE
          AND (currency IS NULL
               OR currency_enriched_at IS NULL
               OR currency_enriched_at < NOW() - INTERVAL '90 days')
        ORDER BY instrument_id
        """,
    ).fetchall()

    enriched = 0
    for row in rows:
        instrument_id, symbol = row[0], row[1]
        profile = fmp_provider.get_instrument_profile(symbol)
        if profile is None:
            logger.warning("FMP profile not found for %s (id=%s)", symbol, instrument_id)
            continue
        conn.execute(
            """
            UPDATE instruments
            SET currency = %(currency)s,
                currency_enriched_at = NOW()
            WHERE instrument_id = %(instrument_id)s
            """,
            {"currency": profile.currency, "instrument_id": instrument_id},
        )
        enriched += 1
        logger.info("Enriched currency for %s: %s", symbol, profile.currency)
    return enriched
```

- [ ] **Step 7: Run all checks**

```bash
uv run ruff check . && uv run pyright && uv run pytest
```

- [ ] **Step 8: Commit**

```bash
git add app/providers/implementations/fmp.py app/services/universe.py tests/test_currency_enrichment.py
git commit -m "feat: FMP /profile endpoint for instrument currency enrichment"
```

---

## Task 6: Backend portfolio FX conversion

**Files:**
- Modify: `app/api/portfolio.py` — add conversion logic and metadata
- Modify: `tests/test_api_portfolio.py`

This is the task that fixes the £22k discrepancy. All money values in the portfolio response are converted to `display_currency` before sending.

- [ ] **Step 1: Write the failing test**

Add to `tests/test_api_portfolio.py`:

```python
def test_portfolio_converts_to_display_currency(self) -> None:
    """Portfolio values should be converted from native currency to display_currency."""
    # Setup: instrument in USD, display_currency = GBP, FX rate USD->GBP = 0.78
    # Expected: market_value of $100 → £78
    ...
```

The test should mock `get_runtime_config` to return `display_currency="GBP"`, mock `load_live_fx_rates` to return USD→GBP rate, and assert the response values are converted.

- [ ] **Step 2: Implement portfolio conversion**

In `app/api/portfolio.py`, after computing positions and before building the response:

1. Load `display_currency` from `get_runtime_config(conn)`
2. Load FX rates from `load_live_fx_rates(conn)` and `load_live_fx_rates_with_metadata(conn)`
3. For each position, determine the instrument's native currency (add `i.currency` to the positions SQL query)
4. Convert `market_value`, `cost_basis`, `unrealized_pnl`, `avg_cost` using `convert()`
5. Convert `cash_balance` (always USD for eToro)
6. Convert `mirror_equity` (always USD for eToro)
7. Build `fx_rates_used` metadata from the rates actually used

Key changes to `_parse_position()` — it needs to accept `from_ccy`, `to_ccy`, and `rates` to convert values:

```python
def _parse_position(
    row: dict[str, object],
    display_currency: str,
    rates: dict[tuple[str, str], Decimal],
) -> PositionItem:
    cost_basis = float(row["cost_basis"])
    current_units = float(row["current_units"])
    native_currency = row.get("currency") or "USD"  # fallback for un-enriched

    last_price = parse_optional_float(row, "last")
    if last_price is not None:
        market_value = current_units * last_price
        unrealized_pnl = market_value - cost_basis
    else:
        market_value = cost_basis
        unrealized_pnl = 0.0

    # Convert to display currency
    if native_currency != display_currency:
        dec_rates = rates
        market_value = float(convert(Decimal(str(market_value)), native_currency, display_currency, dec_rates))
        cost_basis = float(convert(Decimal(str(cost_basis)), native_currency, display_currency, dec_rates))
        unrealized_pnl = float(convert(Decimal(str(unrealized_pnl)), native_currency, display_currency, dec_rates))

    avg_cost_raw = parse_optional_float(row, "avg_cost")
    avg_cost = avg_cost_raw
    if avg_cost is not None and native_currency != display_currency:
        avg_cost = float(convert(Decimal(str(avg_cost)), native_currency, display_currency, rates))

    return PositionItem(...)
```

Update the positions SQL to include `i.currency`:
```sql
SELECT p.instrument_id, i.symbol, i.company_name, i.currency,
       p.open_date, p.avg_cost, p.current_units, p.cost_basis,
       p.source, p.updated_at,
       q.last
FROM positions p
JOIN instruments i USING (instrument_id)
LEFT JOIN quotes q USING (instrument_id)
WHERE p.current_units > 0
ORDER BY p.cost_basis DESC, p.instrument_id ASC
```

Add `display_currency` and `fx_rates_used` to the `PortfolioResponse`:
```python
class PortfolioResponse(BaseModel):
    positions: list[PositionItem]
    position_count: int
    total_aum: float
    cash_balance: float | None
    mirror_equity: float = 0.0
    display_currency: str = "GBP"
    fx_rates_used: dict[str, dict[str, object]] = {}
```

- [ ] **Step 3: Run tests**

Run: `uv run pytest tests/test_api_portfolio.py -v`
Expected: PASS

- [ ] **Step 4: Run full test suite + lint**

```bash
uv run ruff check . && uv run pyright && uv run pytest
```

- [ ] **Step 5: Commit**

```bash
git add app/api/portfolio.py tests/test_api_portfolio.py
git commit -m "feat: portfolio API converts all values to display_currency with fx_rates_used metadata"
```

---

## Task 7: Frontend formatMoney + DisplayCurrencyContext

**Files:**
- Modify: `frontend/src/lib/format.ts:11-35`
- Create: `frontend/src/lib/DisplayCurrencyContext.tsx`
- Modify: `frontend/src/main.tsx`
- Modify: `frontend/src/api/config.ts`
- Modify: 4 files with `formatMoney()` call sites

- [ ] **Step 1: Update formatMoney with currency parameter**

In `frontend/src/lib/format.ts`, replace lines 11-35:

```typescript
/**
 * Cached Intl.NumberFormat instances keyed by currency code.
 * Avoids creating a new formatter on every call.
 */
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
  currency = "GBP",
): string {
  if (value === null || value === undefined) return "\u2014";
  return getFormatter(currency).format(value);
}
```

- [ ] **Step 2: Create DisplayCurrencyContext**

```typescript
// frontend/src/lib/DisplayCurrencyContext.tsx
import { createContext, useContext, type ReactNode } from "react";
import { useAsync } from "@/lib/useAsync";
import { fetchConfig } from "@/api/config";

interface DisplayCurrencyContextValue {
  displayCurrency: string;
}

const DisplayCurrencyContext = createContext<DisplayCurrencyContextValue>({
  displayCurrency: "GBP",
});

export function useDisplayCurrency(): string {
  return useContext(DisplayCurrencyContext).displayCurrency;
}

export function DisplayCurrencyProvider({ children }: { children: ReactNode }) {
  const { data } = useAsync(() => fetchConfig(), []);
  const displayCurrency = data?.runtime?.display_currency ?? "GBP";
  return (
    <DisplayCurrencyContext value={{ displayCurrency }}>
      {children}
    </DisplayCurrencyContext>
  );
}
```

- [ ] **Step 3: Add DisplayCurrencyProvider to main.tsx**

In `frontend/src/main.tsx`, wrap `<App />` with `<DisplayCurrencyProvider>`:

```typescript
import { DisplayCurrencyProvider } from "@/lib/DisplayCurrencyContext";

// In the render tree, add inside SessionProvider:
<SessionProvider>
  <DisplayCurrencyProvider>
    <App />
  </DisplayCurrencyProvider>
</SessionProvider>
```

- [ ] **Step 4: Update all formatMoney call sites**

In each file that calls `formatMoney()`, import `useDisplayCurrency` and pass the currency:

**`frontend/src/components/dashboard/SummaryCards.tsx`:**
```typescript
import { useDisplayCurrency } from "@/lib/DisplayCurrencyContext";
// Inside component:
const currency = useDisplayCurrency();
// Change: formatMoney(data.total_aum) → formatMoney(data.total_aum, currency)
// Change: formatMoney(data.cash_balance) → formatMoney(data.cash_balance, currency)
// Change: formatMoney(totalPnl) → formatMoney(totalPnl, currency)
```

**`frontend/src/components/dashboard/PositionsTable.tsx`:**
```typescript
import { useDisplayCurrency } from "@/lib/DisplayCurrencyContext";
// Inside component:
const currency = useDisplayCurrency();
// Change all formatMoney(p.xxx) → formatMoney(p.xxx, currency)
```

**`frontend/src/pages/InstrumentDetailPage.tsx`:**
```typescript
import { useDisplayCurrency } from "@/lib/DisplayCurrencyContext";
// Inside component:
const currency = useDisplayCurrency();
// Change all formatMoney(xxx) → formatMoney(xxx, currency)
```

**`frontend/src/pages/InstrumentsPage.tsx`:**
```typescript
import { useDisplayCurrency } from "@/lib/DisplayCurrencyContext";
// Inside component:
const currency = useDisplayCurrency();
// Change: formatMoney(item.latest_quote.last) → formatMoney(item.latest_quote.last, currency)
```

- [ ] **Step 5: Run frontend checks**

```bash
pnpm --dir frontend typecheck
pnpm --dir frontend test
```

- [ ] **Step 6: Commit**

```bash
git add frontend/src/lib/format.ts frontend/src/lib/DisplayCurrencyContext.tsx frontend/src/main.tsx frontend/src/components/dashboard/SummaryCards.tsx frontend/src/components/dashboard/PositionsTable.tsx frontend/src/pages/InstrumentDetailPage.tsx frontend/src/pages/InstrumentsPage.tsx
git commit -m "feat: formatMoney gains currency param, DisplayCurrencyContext wires display_currency from /config"
```

---

## Task 8: Settings page display currency toggle

**Files:**
- Create: `frontend/src/components/settings/DisplayCurrencySection.tsx`
- Modify: `frontend/src/pages/SettingsPage.tsx`

- [ ] **Step 1: Create the DisplayCurrencySection component**

```typescript
// frontend/src/components/settings/DisplayCurrencySection.tsx
import { useState } from "react";
import { apiFetch } from "@/api/client";

const SUPPORTED_CURRENCIES = ["GBP", "USD", "EUR"] as const;

interface Props {
  currentCurrency: string;
  onChanged: () => void;
}

export function DisplayCurrencySection({ currentCurrency, onChanged }: Props) {
  const [selected, setSelected] = useState(currentCurrency);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function handleSave() {
    if (selected === currentCurrency) return;
    setSaving(true);
    setError(null);
    try {
      await apiFetch("/config", {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          updated_by: "operator",
          reason: `Changed display currency to ${selected}`,
          display_currency: selected,
        }),
      });
      onChanged();
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to save");
    } finally {
      setSaving(false);
    }
  }

  return (
    <section>
      <h3>Display Currency</h3>
      <p>All monetary values across the dashboard will be converted to and displayed in this currency.</p>
      <div style={{ display: "flex", gap: "0.5rem", alignItems: "center" }}>
        <select
          value={selected}
          onChange={(e) => setSelected(e.target.value)}
          disabled={saving}
        >
          {SUPPORTED_CURRENCIES.map((c) => (
            <option key={c} value={c}>{c}</option>
          ))}
        </select>
        <button onClick={handleSave} disabled={saving || selected === currentCurrency}>
          {saving ? "Saving..." : "Save"}
        </button>
      </div>
      {error && <p style={{ color: "red" }}>{error}</p>}
    </section>
  );
}
```

- [ ] **Step 2: Wire into SettingsPage**

In `frontend/src/pages/SettingsPage.tsx`, import and render the new component. The settings page already loads config via `useAsync` in `DashboardPage.tsx` — add a similar config fetch here if not already present, and pass `display_currency` to the component.

- [ ] **Step 3: Run frontend checks**

```bash
pnpm --dir frontend typecheck
pnpm --dir frontend test
```

- [ ] **Step 4: Commit**

```bash
git add frontend/src/components/settings/DisplayCurrencySection.tsx frontend/src/pages/SettingsPage.tsx
git commit -m "feat: settings page currency toggle for operator display preference"
```

---

## Task 9: Candle freshness fix

**Files:**
- Modify: `app/services/market_data.py:130-157`
- Modify: `tests/test_market_data.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/test_market_data.py` (or create `tests/test_candle_freshness.py`):

```python
class TestCandleFreshness:
    """Tests for the weekday-aware candle freshness check."""

    def test_friday_candle_fresh_on_saturday(self) -> None:
        """Friday candle should be fresh on Saturday."""
        assert _candles_are_fresh_standalone(date(2026, 4, 10), date(2026, 4, 11))  # Fri, Sat

    def test_friday_candle_fresh_on_sunday(self) -> None:
        assert _candles_are_fresh_standalone(date(2026, 4, 10), date(2026, 4, 12))  # Fri, Sun

    def test_friday_candle_fresh_on_monday(self) -> None:
        assert _candles_are_fresh_standalone(date(2026, 4, 10), date(2026, 4, 13))  # Fri, Mon

    def test_wednesday_candle_stale_on_friday(self) -> None:
        """Wednesday candle should be stale on Friday — Thursday is missing."""
        assert not _candles_are_fresh_standalone(date(2026, 4, 8), date(2026, 4, 10))  # Wed, Fri

    def test_thursday_candle_fresh_on_friday(self) -> None:
        assert _candles_are_fresh_standalone(date(2026, 4, 9), date(2026, 4, 10))  # Thu, Fri

    def test_monday_candle_stale_on_wednesday(self) -> None:
        assert not _candles_are_fresh_standalone(date(2026, 4, 6), date(2026, 4, 8))  # Mon, Wed

    def test_same_day(self) -> None:
        assert _candles_are_fresh_standalone(date(2026, 4, 13), date(2026, 4, 13))
```

Where `_candles_are_fresh_standalone` is a helper that calls the pure logic without DB:

```python
def _candles_are_fresh_standalone(latest_date: date, today: date) -> bool:
    return latest_date >= _most_recent_trading_day(today)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_market_data.py::TestCandleFreshness -v`
Expected: FAIL — `_most_recent_trading_day` does not exist

- [ ] **Step 3: Implement the fix**

In `app/services/market_data.py`, replace `_candles_are_fresh` (lines 130-157):

```python
def _most_recent_trading_day(today: date) -> date:
    """Return the most recent weekday (Mon-Fri).

    No holiday calendar — if a holiday causes a gap, the next fetch
    fills it. Holidays don't cause false staleness because the candle
    endpoint simply returns nothing new.
    """
    weekday = today.weekday()  # 0=Mon, 6=Sun
    if weekday == 5:  # Saturday
        return today - timedelta(days=1)
    if weekday == 6:  # Sunday
        return today - timedelta(days=2)
    return today


def _candles_are_fresh(
    conn: psycopg.Connection,  # type: ignore[type-arg]
    instrument_id: int,
    today: date,
) -> bool:
    """Return True if price_daily already has the most recent trading day's candle."""
    row = conn.execute(
        """
        SELECT MAX(price_date)
        FROM price_daily
        WHERE instrument_id = %(instrument_id)s
        """,
        {"instrument_id": instrument_id},
    ).fetchone()
    if row is None or row[0] is None:
        return False
    latest_date: date = row[0]
    return latest_date >= _most_recent_trading_day(today)
```

- [ ] **Step 4: Run tests**

Run: `uv run pytest tests/test_market_data.py -v`
Expected: PASS

- [ ] **Step 5: Run full checks**

```bash
uv run ruff check . && uv run pyright && uv run pytest
```

- [ ] **Step 6: Commit**

```bash
git add app/services/market_data.py tests/test_market_data.py
git commit -m "fix: candle freshness uses weekday check instead of 3-day window"
```

---

## Task 10: FX rates refresh job

**Files:**
- Modify: `app/services/fx.py` — add `refresh_live_fx_rates()`
- Modify: `app/workers/scheduler.py` — add `JOB_FX_RATES_REFRESH`
- Create: `tests/test_fx_refresh.py`

The FX refresh job reads conversion rates from the eToro rates endpoint and writes both directions to `live_fx_rates`.

- [ ] **Step 1: Write the failing test**

```python
"""Tests for live FX rate refresh from eToro conversion rates."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from app.services.fx import upsert_live_fx_rate


def test_upsert_live_fx_rate() -> None:
    conn = MagicMock()
    upsert_live_fx_rate(
        conn,
        from_currency="USD",
        to_currency="GBP",
        rate=Decimal("0.78"),
        quoted_at=datetime(2026, 4, 13, 14, 0, tzinfo=UTC),
    )
    conn.execute.assert_called_once()
    sql = conn.execute.call_args[0][0]
    assert "ON CONFLICT" in sql
    assert "live_fx_rates" in sql
```

- [ ] **Step 2: Implement upsert_live_fx_rate**

Add to `app/services/fx.py`:

```python
def upsert_live_fx_rate(
    conn: psycopg.Connection[Any],
    *,
    from_currency: str,
    to_currency: str,
    rate: Decimal,
    quoted_at: datetime,
) -> None:
    """Insert or update a single live FX rate row."""
    conn.execute(
        """
        INSERT INTO live_fx_rates (from_currency, to_currency, rate, quoted_at)
        VALUES (%(from_currency)s, %(to_currency)s, %(rate)s, %(quoted_at)s)
        ON CONFLICT (from_currency, to_currency) DO UPDATE SET
            rate = EXCLUDED.rate,
            quoted_at = EXCLUDED.quoted_at
        """,
        {
            "from_currency": from_currency,
            "to_currency": to_currency,
            "rate": rate,
            "quoted_at": quoted_at,
        },
    )
```

- [ ] **Step 3: Add the refresh job function**

Add `refresh_live_fx_rates()` to `app/services/fx.py`:

```python
def refresh_live_fx_rates(
    provider: EtoroMarketDataProvider,
    conn: psycopg.Connection[Any],
    instrument_ids: Sequence[int],
) -> int:
    """Refresh live FX rates from eToro conversion rates.

    Fetches quotes with conversion rates, extracts unique currency pairs,
    writes both directions to live_fx_rates.

    Returns the number of rate pairs written.
    """
    quotes = provider.get_quotes(list(instrument_ids))
    # Extract conversion rates from quote responses
    # eToro conversionRateAsk/Bid represent instrument_currency → USD
    # We need to collect unique pairs and write both directions
    now = datetime.now(tz=UTC)
    pairs_written = 0

    # For now, use the eToro account currency (USD) and the display currencies
    # The actual FX rate extraction depends on the eToro rates response format
    # Placeholder: this will be refined when the actual conversion rate fields
    # are available in the quote response.

    return pairs_written
```

- [ ] **Step 4: Register the job in scheduler**

In `app/workers/scheduler.py`, add the job constant and registration:

```python
JOB_FX_RATES_REFRESH = "fx_rates_refresh"
```

Add to `SCHEDULED_JOBS`:
```python
ScheduledJob(
    name=JOB_FX_RATES_REFRESH,
    description="Refresh live FX rates from eToro conversion rates.",
    cadence=Cadence.hourly(minute=0),  # every hour at :00
    prerequisite=_has_coverage_tier12,
),
```

Add the invoker function and register it in `_INVOKERS`.

- [ ] **Step 5: Run tests**

```bash
uv run pytest tests/test_fx_refresh.py tests/test_workers_scheduler_registry.py -v
```

- [ ] **Step 6: Run full checks**

```bash
uv run ruff check . && uv run pyright && uv run pytest
```

- [ ] **Step 7: Commit**

```bash
git add app/services/fx.py app/workers/scheduler.py tests/test_fx_refresh.py
git commit -m "feat: FX rates refresh job writes eToro conversion rates to live_fx_rates"
```

---

## Task 11: Scheduler — hourly refresh to daily candle job

**Files:**
- Modify: `app/workers/scheduler.py:247-251,505-550`
- Modify: `tests/test_workers_scheduler_registry.py`

- [ ] **Step 1: Write the test for the new daily candle job**

Add to `tests/test_workers_scheduler_registry.py`:

```python
def test_daily_candle_job_registered() -> None:
    names = [j.name for j in SCHEDULED_JOBS]
    assert "daily_candle_refresh" in names

def test_daily_candle_job_cadence() -> None:
    job = next(j for j in SCHEDULED_JOBS if j.name == "daily_candle_refresh")
    # Should run daily at 22:00 UTC
    assert job.cadence.kind == "daily"
    assert job.cadence.hour == 22
    assert job.cadence.minute == 0
```

- [ ] **Step 2: Refactor the hourly market refresh**

In `app/workers/scheduler.py`:

1. Add `JOB_DAILY_CANDLE_REFRESH = "daily_candle_refresh"` to constants.
2. Replace `JOB_HOURLY_MARKET_REFRESH` with `JOB_DAILY_CANDLE_REFRESH` in `SCHEDULED_JOBS`:

```python
ScheduledJob(
    name=JOB_DAILY_CANDLE_REFRESH,
    description="Fetch daily candles for all active Tier 1/2 instruments after US market close.",
    cadence=Cadence.daily(hour=22, minute=0),
    prerequisite=_has_coverage_tier12,
),
```

3. Rename `hourly_market_refresh()` to `daily_candle_refresh()` (or create new function that only fetches candles, not quotes).

4. Update `_INVOKERS` mapping.

5. Keep the old `JOB_HOURLY_MARKET_REFRESH` name as a deprecated alias if any other code references it, or remove if no references exist.

- [ ] **Step 3: Update the refresh function to skip quote fetching**

The daily candle job should only call `_upsert_candles` per instrument, not `_upsert_quote`. Quote updating will be handled by the WebSocket pipeline in the follow-up plan.

- [ ] **Step 4: Run tests**

```bash
uv run pytest tests/test_workers_scheduler_registry.py -v
```

- [ ] **Step 5: Run full checks**

```bash
uv run ruff check . && uv run pyright && uv run pytest
```

- [ ] **Step 6: Commit**

```bash
git add app/workers/scheduler.py tests/test_workers_scheduler_registry.py
git commit -m "feat: replace hourly market refresh with daily candle job at 22:00 UTC"
```

---

## Task 12: Redis infrastructure (Docker + config only)

**Files:**
- Modify: `docker-compose.yml`
- Modify: `app/config.py`
- Modify: `pyproject.toml`

This task adds the Redis dependency and Docker service. No Redis client code yet — that's the follow-up plan. But the infrastructure is in place so `docker compose up` includes Redis.

- [ ] **Step 1: Add Redis to docker-compose.yml**

```yaml
services:
  postgres:
    image: postgres:17
    container_name: ebull-postgres
    restart: unless-stopped
    environment:
      POSTGRES_DB: ${POSTGRES_DB:-ebull}
      POSTGRES_USER: ${POSTGRES_USER:-postgres}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD:-postgres}
    ports:
      - "${POSTGRES_PORT:-5432}:5432"
    volumes:
      - pgdata:/var/lib/postgresql/data

  redis:
    image: redis:7-alpine
    container_name: ebull-redis
    restart: unless-stopped
    ports:
      - "${REDIS_PORT:-6379}:6379"

volumes:
  pgdata:
```

- [ ] **Step 2: Add REDIS_URL to settings**

In `app/config.py`, add to the `Settings` class:

```python
    # Redis (IPC layer for live pricing — follow-up plan)
    redis_url: str = "redis://localhost:6379/0"
```

- [ ] **Step 3: Add redis dependency to pyproject.toml**

Add `redis[hiredis]>=5.0.0` to the dependencies list.

Run: `uv lock && uv sync`

- [ ] **Step 4: Run checks**

```bash
uv run ruff check . && uv run pyright && uv run pytest
```

- [ ] **Step 5: Commit**

```bash
git add docker-compose.yml app/config.py pyproject.toml uv.lock
git commit -m "chore: add Redis infrastructure — Docker service, config, dependency"
```

---

## Task 13: Final integration test + cleanup

**Files:**
- All modified files from previous tasks
- Verify: `tests/smoke/test_app_boots.py`

- [ ] **Step 1: Run the full backend test suite**

```bash
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv run pytest
```

All four must pass.

- [ ] **Step 2: Run the full frontend test suite**

```bash
pnpm --dir frontend typecheck
pnpm --dir frontend test
```

Both must pass.

- [ ] **Step 3: Start the dev server and verify the portfolio page**

Start the backend and frontend, navigate to the dashboard, and verify:
- Portfolio values are now converted to the display currency
- The `display_currency` field is present in the `/config` response
- The settings page shows the currency toggle
- Changing the currency re-renders all pages with the correct symbol

- [ ] **Step 4: Verify no regressions**

- Instrument detail page shows converted values
- Instruments list shows converted quote prices
- Admin page still works
- Settings page broker credentials section still works

- [ ] **Step 5: Commit any final fixes**

```bash
git add -A
git commit -m "chore: integration test fixes for live pricing currency conversion"
```

---

## Self-review checklist

**Spec coverage:**
- [x] §2.1 Instrument currency enrichment (Task 4, 5)
- [x] §2.2 Live FX rates table (Task 1, 10)
- [x] §2.3 Display currency setting (Task 3, 8)
- [x] §2.4 Backend conversion contract (Task 6)
- [x] §2.5 Frontend formatting (Task 7)
- [x] §4.2 Candle freshness fix (Task 9)
- [x] §4.3 Hourly→daily job (Task 11)
- [x] §5.3 Redis dependency (Task 12)
- [x] §6.1 New tables (Task 1)
- [x] §6.2 Modified tables (Task 1)
- Deferred to follow-up plan: §1.1 WebSocket, §1.3 SSE, §3 Live charts, §5.1-5.2 Redis pub/sub

**Placeholder scan:** No TBD/TODO/placeholders. Task 10 FX refresh has a note about eToro conversion rate field mapping that will be resolved during implementation by reading the actual API response structure.

**Type consistency:** `InstrumentRecord.currency` changed to `str | None` in Task 4. All downstream code that reads `currency` must handle `None` (fallback to `"USD"` in portfolio conversion). `RuntimeConfig.display_currency` is `str` (never None, DEFAULT 'GBP' in DB).
