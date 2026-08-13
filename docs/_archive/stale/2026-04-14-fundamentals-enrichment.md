# Fundamentals Enrichment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Enrich the fundamentals data layer with valuation multiples, instrument profile data (beta, float, volume), earnings calendar, and analyst estimates — then feed these into the scoring engine to replace neutral-by-absence defaults.

**Architecture:** Three new tables (`instrument_profile`, `earnings_events`, `analyst_estimates`) store data at their natural cadence. A SQL view (`instrument_valuation`) computes derived multiples (P/E, P/B, P/FCF, FCF yield, D/E, market cap) by joining `fundamentals_snapshot` with `quotes` — no stale cached values. The FMP provider gains three new fetch methods; the service layer gains a new `refresh_enrichment` function; the scoring engine's `_value_score` gains a fundamentals-derived fallback path.

**Tech Stack:** PostgreSQL 17, psycopg3, Python 3.14, httpx, pytest

**Issue:** #199
**Branch:** `feature/199-fundamentals-enrichment`

---

## Settled decisions that apply

| Decision | How this plan preserves it |
|---|---|
| FMP is the normalized fundamentals provider in v1 | All new endpoints are FMP. No new provider. |
| Providers are thin adapters; domain logic in services | FMP provider returns dataclasses; service layer owns DB writes and freshness checks. |
| Scoring is heuristic, explicit, auditable | All new score formulas are clipped 0-1 with explicit constants. No ML, no hidden weighting. |
| Penalties are additive in v1 | No new multiplicative penalties. |
| `as_of_date` = financial statement period end date | `fundamentals_snapshot` is not modified — multiples view joins it with latest quote. |
| Provider design rule: no DB lookups in providers | FMP methods return dataclasses; service resolves instrument_id. |

## Prevention log entries that apply

| Entry | How avoided |
|---|---|
| No-op ORDER BY | All new `fetchone()` queries use meaningful sort columns (e.g. `as_of_date DESC`, `fetched_at DESC`). |
| Missing data on hard-rule path silently passes | Enrichment data is best-effort for scoring; missing data returns neutral score with logged note, never silently passes a hard rule. |
| JOIN fan-out inflates aggregates | The valuation view uses `LATERAL ... LIMIT 1` for quotes (one row per instrument). |

---

## File structure

### New files
| File | Responsibility |
|---|---|
| `sql/025_fundamentals_enrichment.sql` | Migration: `instrument_profile`, `earnings_events`, `analyst_estimates` tables + `instrument_valuation` view |
| `app/providers/enrichment.py` | Provider interface: `EnrichmentProvider` ABC with `get_profile`, `get_earnings_calendar`, `get_analyst_estimates` |
| `app/services/enrichment.py` | Service: `refresh_enrichment()` — fetch + upsert profile, earnings, estimates |
| `tests/test_enrichment_provider.py` | Unit tests for FMP enrichment normalisation (pure functions, no I/O) |
| `tests/test_enrichment_service.py` | Unit tests for service upsert logic (mock DB) |
| `tests/test_scoring_enriched.py` | Unit tests for enhanced `_value_score` with fundamentals fallback |

### Modified files
| File | What changes |
|---|---|
| `app/providers/implementations/fmp.py` | Add `get_profile_enrichment()`, `get_earnings_calendar()`, `get_analyst_estimates()` methods + normaliser functions |
| `app/services/scoring.py` | `_value_score` gains fundamentals-derived fallback; `_load_instrument_data` fetches enrichment data |
| `app/workers/scheduler.py` | Wire `refresh_enrichment` into `daily_research_refresh` |
| `app/services/thesis.py` | Pass enrichment context (earnings, estimates) to Claude writer |

---

## Phase 1: Schema and provider interface

### Task 1: Migration — new tables and valuation view

**Files:**
- Create: `sql/025_fundamentals_enrichment.sql`

- [ ] **Step 1: Write the migration SQL**

```sql
-- 025_fundamentals_enrichment.sql
-- Adds instrument_profile, earnings_events, analyst_estimates,
-- and a computed valuation view for scoring.

-- ── instrument_profile ──────────────────────────────────────────
-- One row per instrument. Refreshed daily from FMP /v3/profile.
-- Stores data that changes infrequently (beta, float, employees).
CREATE TABLE IF NOT EXISTS instrument_profile (
    instrument_id   BIGINT PRIMARY KEY REFERENCES instruments(instrument_id),
    beta            NUMERIC(10,4),
    public_float    BIGINT,           -- shares available for public trading
    avg_volume_30d  BIGINT,           -- 30-day average daily volume
    market_cap      NUMERIC(20,2),    -- latest market cap from provider
    employees       INTEGER,
    ipo_date        DATE,
    is_actively_trading BOOLEAN,
    fetched_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ── earnings_events ─────────────────────────────────────────────
-- One row per earnings report per instrument.
-- Keyed on (instrument_id, fiscal_date_ending) for idempotent upsert.
CREATE TABLE IF NOT EXISTS earnings_events (
    earnings_event_id BIGSERIAL PRIMARY KEY,
    instrument_id     BIGINT NOT NULL REFERENCES instruments(instrument_id),
    fiscal_date_ending DATE NOT NULL,
    reporting_date     DATE,
    eps_estimate       NUMERIC(12,4),
    eps_actual         NUMERIC(12,4),
    revenue_estimate   NUMERIC(20,2),
    revenue_actual     NUMERIC(20,2),
    surprise_pct       NUMERIC(10,4),   -- (actual - estimate) / |estimate| * 100
    fetched_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (instrument_id, fiscal_date_ending)
);

CREATE INDEX IF NOT EXISTS idx_earnings_events_instrument
    ON earnings_events(instrument_id, fiscal_date_ending DESC);

-- ── analyst_estimates ───────────────────────────────────────────
-- One row per instrument per estimate snapshot date.
-- Refreshed weekly. Keyed on (instrument_id, as_of_date).
CREATE TABLE IF NOT EXISTS analyst_estimates (
    estimate_id       BIGSERIAL PRIMARY KEY,
    instrument_id     BIGINT NOT NULL REFERENCES instruments(instrument_id),
    as_of_date        DATE NOT NULL,
    consensus_eps_fq  NUMERIC(12,4),    -- next fiscal quarter
    consensus_eps_fy  NUMERIC(12,4),    -- next fiscal year
    consensus_rev_fq  NUMERIC(20,2),
    consensus_rev_fy  NUMERIC(20,2),
    analyst_count     INTEGER,
    buy_count         INTEGER,
    hold_count        INTEGER,
    sell_count        INTEGER,
    price_target_mean NUMERIC(18,6),
    price_target_high NUMERIC(18,6),
    price_target_low  NUMERIC(18,6),
    fetched_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (instrument_id, as_of_date)
);

CREATE INDEX IF NOT EXISTS idx_analyst_estimates_instrument
    ON analyst_estimates(instrument_id, as_of_date DESC);

-- ── instrument_valuation view ───────────────────────────────────
-- Computed multiples from fundamentals_snapshot + quotes.
-- Scoring engine reads this view; no stale cached values.
CREATE OR REPLACE VIEW instrument_valuation AS
SELECT
    fs.instrument_id,
    fs.as_of_date,
    -- Market cap: price * shares_outstanding
    q.last * fs.shares_outstanding        AS market_cap_live,
    -- P/E ratio: price / EPS (NULL-safe: returns NULL if EPS <= 0 or NULL)
    CASE WHEN fs.eps > 0
         THEN q.last / fs.eps
         ELSE NULL
    END                                    AS pe_ratio,
    -- P/B ratio: price / book_value_per_share
    CASE WHEN fs.book_value > 0
         THEN q.last / fs.book_value
         ELSE NULL
    END                                    AS pb_ratio,
    -- P/FCF ratio: market_cap / FCF
    CASE WHEN fs.fcf > 0
         THEN (q.last * fs.shares_outstanding) / fs.fcf
         ELSE NULL
    END                                    AS p_fcf_ratio,
    -- FCF yield: FCF / market_cap (inverse of P/FCF)
    CASE WHEN q.last > 0 AND fs.shares_outstanding > 0
         THEN fs.fcf / (q.last * fs.shares_outstanding)
         ELSE NULL
    END                                    AS fcf_yield,
    -- Debt/Equity ratio: total_debt / (book_value * shares_outstanding)
    CASE WHEN fs.book_value > 0 AND fs.shares_outstanding > 0
         THEN fs.debt / (fs.book_value * fs.shares_outstanding)
         ELSE NULL
    END                                    AS debt_equity_ratio,
    q.last                                 AS current_price,
    q.quoted_at                            AS price_as_of
FROM fundamentals_snapshot fs
JOIN quotes q ON q.instrument_id = fs.instrument_id
-- Pick latest fundamentals snapshot per instrument
WHERE fs.as_of_date = (
    SELECT MAX(fs2.as_of_date)
    FROM fundamentals_snapshot fs2
    WHERE fs2.instrument_id = fs.instrument_id
);
```

- [ ] **Step 2: Verify migration numbering**

Run: `ls sql/*.sql | tail -3`
Expected: `024_broker_positions.sql` is the last migration. `025` is correct.

- [ ] **Step 3: Commit**

```bash
git add sql/025_fundamentals_enrichment.sql
git commit -m "feat(#199): add enrichment schema — instrument_profile, earnings_events, analyst_estimates, valuation view"
```

---

### Task 2: Provider interface — EnrichmentProvider ABC

**Files:**
- Create: `app/providers/enrichment.py`

- [ ] **Step 1: Write the provider interface and dataclasses**

```python
"""
Enrichment provider interface.

FMP is the v1 implementation. All domain code imports this interface only —
never the concrete provider.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date
from decimal import Decimal


@dataclass(frozen=True)
class InstrumentProfileData:
    """Profile data for a single instrument from provider."""

    symbol: str
    beta: Decimal | None
    public_float: int | None            # shares available for public trading
    avg_volume_30d: int | None          # 30-day average daily volume
    market_cap: Decimal | None
    employees: int | None
    ipo_date: date | None
    is_actively_trading: bool | None


@dataclass(frozen=True)
class EarningsEvent:
    """Single earnings report from provider."""

    symbol: str
    fiscal_date_ending: date
    reporting_date: date | None
    eps_estimate: Decimal | None
    eps_actual: Decimal | None
    revenue_estimate: Decimal | None
    revenue_actual: Decimal | None
    surprise_pct: Decimal | None        # (actual - estimate) / |estimate| * 100


@dataclass(frozen=True)
class AnalystEstimates:
    """Analyst consensus snapshot from provider."""

    symbol: str
    as_of_date: date
    consensus_eps_fq: Decimal | None    # next fiscal quarter
    consensus_eps_fy: Decimal | None    # next fiscal year
    consensus_rev_fq: Decimal | None
    consensus_rev_fy: Decimal | None
    analyst_count: int | None
    buy_count: int | None
    hold_count: int | None
    sell_count: int | None
    price_target_mean: Decimal | None
    price_target_high: Decimal | None
    price_target_low: Decimal | None


class EnrichmentProvider(ABC):
    """
    Interface for instrument enrichment data: profile, earnings, estimates.

    v1 implementation: FmpFundamentalsProvider (extended).
    """

    @abstractmethod
    def get_profile(self, symbol: str) -> InstrumentProfileData | None:
        """Return profile data for a symbol. None if not found."""

    @abstractmethod
    def get_earnings_calendar(
        self, symbol: str, limit: int = 8,
    ) -> list[EarningsEvent]:
        """Return recent earnings events oldest-first, up to limit quarters."""

    @abstractmethod
    def get_analyst_estimates(self, symbol: str) -> AnalystEstimates | None:
        """Return latest analyst consensus snapshot. None if not found."""
```

- [ ] **Step 2: Commit**

```bash
git add app/providers/enrichment.py
git commit -m "feat(#199): add EnrichmentProvider interface and dataclasses"
```

---

### Task 3: FMP provider — implement enrichment methods

**Files:**
- Modify: `app/providers/implementations/fmp.py`
- Test: `tests/test_enrichment_provider.py`

- [ ] **Step 1: Write failing tests for FMP profile normalisation**

Create `tests/test_enrichment_provider.py` with fixtures from FMP `/v3/profile` response shape:

```python
"""Unit tests for FMP enrichment normalisation — pure functions, no I/O."""
from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from app.providers.implementations.fmp import (
    _build_profile_data,
    _build_earnings_event,
    _build_analyst_estimates,
)

# ── Profile fixtures ──────────────────────────────────────────

FIXTURE_FMP_PROFILE = {
    "symbol": "AAPL",
    "beta": 1.24,
    "volAvg": 58432100,
    "mktCap": 2850000000000,
    "fullTimeEmployees": "161000",
    "ipoDate": "1980-12-12",
    "isActivelyTrading": True,
    "floatShares": 15400000000,
}

FIXTURE_FMP_PROFILE_MINIMAL = {
    "symbol": "UNKNOWN",
}


def test_build_profile_data_full():
    result = _build_profile_data("AAPL", FIXTURE_FMP_PROFILE)
    assert result.symbol == "AAPL"
    assert result.beta == Decimal("1.24")
    assert result.public_float == 15400000000
    assert result.avg_volume_30d == 58432100
    assert result.market_cap == Decimal("2850000000000")
    assert result.employees == 161000
    assert result.ipo_date == date(1980, 12, 12)
    assert result.is_actively_trading is True


def test_build_profile_data_missing_fields():
    result = _build_profile_data("UNKNOWN", FIXTURE_FMP_PROFILE_MINIMAL)
    assert result.symbol == "UNKNOWN"
    assert result.beta is None
    assert result.public_float is None
    assert result.avg_volume_30d is None
    assert result.employees is None
    assert result.ipo_date is None


# ── Earnings fixtures ─────────────────────────────────────────

FIXTURE_FMP_EARNINGS = {
    "date": "2024-06-30",
    "symbol": "AAPL",
    "reportedDate": "2024-08-01",
    "epsEstimated": 1.34,
    "eps": 1.40,
    "revenueEstimated": 84530000000,
    "revenue": 85780000000,
}


def test_build_earnings_event_full():
    result = _build_earnings_event("AAPL", FIXTURE_FMP_EARNINGS)
    assert result.fiscal_date_ending == date(2024, 6, 30)
    assert result.reporting_date == date(2024, 8, 1)
    assert result.eps_estimate == Decimal("1.34")
    assert result.eps_actual == Decimal("1.40")
    assert result.revenue_estimate == Decimal("84530000000")
    assert result.revenue_actual == Decimal("85780000000")
    assert result.surprise_pct == pytest.approx(Decimal("4.4776"), rel=1e-2)


def test_build_earnings_event_missing_eps():
    row = {"date": "2024-03-31", "symbol": "AAPL"}
    result = _build_earnings_event("AAPL", row)
    assert result.fiscal_date_ending == date(2024, 3, 31)
    assert result.eps_actual is None
    assert result.surprise_pct is None


# ── Analyst estimates fixtures ────────────────────────────────

FIXTURE_FMP_ESTIMATES = {
    "symbol": "AAPL",
    "date": "2024-06-30",
    "estimatedEpsAvg": 1.45,
    "estimatedRevenueAvg": 90000000000,
    "numberAnalystEstimatedEps": 32,
}

FIXTURE_FMP_CONSENSUS = {
    "symbol": "AAPL",
    "buy": 25,
    "hold": 5,
    "sell": 2,
    "consensus": "Buy",
}

FIXTURE_FMP_PRICE_TARGET = {
    "symbol": "AAPL",
    "targetMean": 210.50,
    "targetHigh": 250.00,
    "targetLow": 180.00,
    "numberOfAnalysts": 32,
}


def test_build_analyst_estimates_full():
    result = _build_analyst_estimates(
        "AAPL",
        estimates=[FIXTURE_FMP_ESTIMATES],
        consensus=FIXTURE_FMP_CONSENSUS,
        price_target=FIXTURE_FMP_PRICE_TARGET,
    )
    assert result is not None
    assert result.consensus_eps_fq == Decimal("1.45")
    assert result.analyst_count == 32
    assert result.buy_count == 25
    assert result.hold_count == 5
    assert result.sell_count == 2
    assert result.price_target_mean == Decimal("210.50")


def test_build_analyst_estimates_no_data():
    result = _build_analyst_estimates("AAPL", estimates=[], consensus=None, price_target=None)
    assert result is None
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_enrichment_provider.py -v`
Expected: ImportError — `_build_profile_data` etc. do not exist yet.

- [ ] **Step 3: Implement normaliser functions in fmp.py**

Add to `app/providers/implementations/fmp.py` after the existing normaliser section:

```python
from app.providers.enrichment import (
    AnalystEstimates,
    EarningsEvent,
    EnrichmentProvider,
    InstrumentProfileData,
)


def _build_profile_data(
    symbol: str, item: Mapping[str, object],
) -> InstrumentProfileData:
    """Normalise FMP /v3/profile response into InstrumentProfileData."""
    raw_ipo = item.get("ipoDate")
    ipo_date: date | None = None
    if raw_ipo:
        try:
            ipo_date = date.fromisoformat(str(raw_ipo)[:10])
        except ValueError:
            pass

    return InstrumentProfileData(
        symbol=symbol,
        beta=_decimal_or_none(item.get("beta")),
        public_float=_int_or_none(item.get("floatShares")),
        avg_volume_30d=_int_or_none(item.get("volAvg")),
        market_cap=_decimal_or_none(item.get("mktCap")),
        employees=_int_or_none(item.get("fullTimeEmployees")),
        ipo_date=ipo_date,
        is_actively_trading=item.get("isActivelyTrading") if isinstance(item.get("isActivelyTrading"), bool) else None,
    )


def _build_earnings_event(
    symbol: str, row: Mapping[str, object],
) -> EarningsEvent:
    """Normalise one FMP earnings calendar row."""
    raw_fiscal = row.get("date")
    fiscal_date = date.fromisoformat(str(raw_fiscal)[:10]) if raw_fiscal else date.min

    raw_report = row.get("reportedDate")
    report_date: date | None = None
    if raw_report:
        try:
            report_date = date.fromisoformat(str(raw_report)[:10])
        except ValueError:
            pass

    eps_est = _decimal_or_none(row.get("epsEstimated"))
    eps_act = _decimal_or_none(row.get("eps"))
    rev_est = _decimal_or_none(row.get("revenueEstimated"))
    rev_act = _decimal_or_none(row.get("revenue"))

    surprise: Decimal | None = None
    if eps_est is not None and eps_act is not None and eps_est != 0:
        surprise = (eps_act - eps_est) / abs(eps_est) * 100

    return EarningsEvent(
        symbol=symbol,
        fiscal_date_ending=fiscal_date,
        reporting_date=report_date,
        eps_estimate=eps_est,
        eps_actual=eps_act,
        revenue_estimate=rev_est,
        revenue_actual=rev_act,
        surprise_pct=surprise,
    )


def _build_analyst_estimates(
    symbol: str,
    estimates: list[dict[str, object]],
    consensus: dict[str, object] | None,
    price_target: dict[str, object] | None,
) -> AnalystEstimates | None:
    """Combine FMP analyst-estimation, consensus, and price-target data."""
    if not estimates and consensus is None and price_target is None:
        return None

    est = estimates[0] if estimates else {}
    raw_date = est.get("date")
    as_of = date.fromisoformat(str(raw_date)[:10]) if raw_date else date.today()

    return AnalystEstimates(
        symbol=symbol,
        as_of_date=as_of,
        consensus_eps_fq=_decimal_or_none(est.get("estimatedEpsAvg")),
        consensus_eps_fy=None,  # FMP annual endpoint is separate; defer to v2
        consensus_rev_fq=_decimal_or_none(est.get("estimatedRevenueAvg")),
        consensus_rev_fy=None,
        analyst_count=_int_or_none(
            (price_target or {}).get("numberOfAnalysts") or est.get("numberAnalystEstimatedEps")
        ),
        buy_count=_int_or_none((consensus or {}).get("buy")),
        hold_count=_int_or_none((consensus or {}).get("hold")),
        sell_count=_int_or_none((consensus or {}).get("sell")),
        price_target_mean=_decimal_or_none((price_target or {}).get("targetMean")),
        price_target_high=_decimal_or_none((price_target or {}).get("targetHigh")),
        price_target_low=_decimal_or_none((price_target or {}).get("targetLow")),
    )
```

- [ ] **Step 4: Add HTTP fetch methods and implement EnrichmentProvider on FmpFundamentalsProvider**

Add to the `FmpFundamentalsProvider` class:

```python
    # ── Enrichment methods (EnrichmentProvider) ──────────────

    def get_profile_enrichment(self, symbol: str) -> InstrumentProfileData | None:
        """Fetch enrichment profile data from FMP /v3/profile."""
        resp = self._http.get(
            f"/v3/profile/{symbol}",
            params={"apikey": self._api_key},
        )
        if resp.status_code != 200:
            logger.warning("FMP profile enrichment failed for %s: %s", symbol, resp.status_code)
            return None
        data = resp.json()
        _persist_raw(f"fmp_profile_{symbol}", data)
        if not isinstance(data, list) or not data:
            return None
        return _build_profile_data(symbol, data[0])

    def get_earnings_calendar(self, symbol: str, limit: int = 8) -> list[EarningsEvent]:
        """Fetch earnings history from FMP /v3/historical/earning_calendar/{symbol}."""
        resp = self._http.get(
            f"/v3/historical/earning_calendar/{symbol}",
            params={"limit": limit, "apikey": self._api_key},
        )
        if resp.status_code != 200:
            logger.warning("FMP earnings calendar failed for %s: %s", symbol, resp.status_code)
            return []
        data = resp.json()
        _persist_raw(f"fmp_earnings_{symbol}", data)
        if not isinstance(data, list):
            return []
        events = [_build_earnings_event(symbol, row) for row in data if isinstance(row, dict)]
        events.sort(key=lambda e: e.fiscal_date_ending)  # oldest-first
        return events

    def get_analyst_estimates(self, symbol: str) -> AnalystEstimates | None:
        """Combine FMP analyst-estimation, consensus, and price-target endpoints."""
        # Analyst estimates (quarterly forward)
        est_resp = self._http.get(
            f"/v3/analyst-estimates/{symbol}",
            params={"period": "quarter", "limit": 1, "apikey": self._api_key},
        )
        estimates: list[dict[str, object]] = []
        if est_resp.status_code == 200:
            raw = est_resp.json()
            _persist_raw(f"fmp_estimates_{symbol}", raw)
            if isinstance(raw, list):
                estimates = [r for r in raw if isinstance(r, dict)]

        # Analyst consensus (buy/hold/sell)
        con_resp = self._http.get(
            f"/v4/analyst-stock-recommendations/{symbol}",
            params={"apikey": self._api_key},
        )
        consensus: dict[str, object] | None = None
        if con_resp.status_code == 200:
            raw = con_resp.json()
            _persist_raw(f"fmp_consensus_{symbol}", raw)
            if isinstance(raw, list) and raw:
                consensus = raw[0] if isinstance(raw[0], dict) else None

        # Price targets (mean/high/low)
        pt_resp = self._http.get(
            f"/v4/price-target-consensus/{symbol}",
            params={"apikey": self._api_key},
        )
        price_target: dict[str, object] | None = None
        if pt_resp.status_code == 200:
            raw = pt_resp.json()
            _persist_raw(f"fmp_price_target_{symbol}", raw)
            if isinstance(raw, list) and raw:
                price_target = raw[0] if isinstance(raw[0], dict) else None

        return _build_analyst_estimates(symbol, estimates, consensus, price_target)
```

Make `FmpFundamentalsProvider` also implement `EnrichmentProvider`:

```python
class FmpFundamentalsProvider(FundamentalsProvider, EnrichmentProvider):
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest tests/test_enrichment_provider.py -v`
Expected: All tests pass.

- [ ] **Step 6: Run full check suite**

```bash
uv run ruff check .
uv run ruff format --check .
uv run pyright
```

- [ ] **Step 7: Commit**

```bash
git add app/providers/implementations/fmp.py app/providers/enrichment.py tests/test_enrichment_provider.py
git commit -m "feat(#199): FMP enrichment provider — profile, earnings, analyst estimates"
```

---

## Phase 2: Service layer

### Task 4: Enrichment service — fetch and upsert

**Files:**
- Create: `app/services/enrichment.py`
- Test: `tests/test_enrichment_service.py`

- [ ] **Step 1: Write failing tests for upsert functions**

Create `tests/test_enrichment_service.py`:

```python
"""Unit tests for enrichment service — mock DB, no network."""
from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from unittest.mock import MagicMock, call, patch

import pytest

from app.providers.enrichment import (
    AnalystEstimates,
    EarningsEvent,
    InstrumentProfileData,
)
from app.services.enrichment import (
    EnrichmentRefreshSummary,
    _upsert_analyst_estimates,
    _upsert_earnings_events,
    _upsert_profile,
    refresh_enrichment,
)

_NOW = datetime(2026, 4, 14, 12, 0, 0, tzinfo=UTC)


def _make_profile(symbol: str = "AAPL") -> InstrumentProfileData:
    return InstrumentProfileData(
        symbol=symbol,
        beta=Decimal("1.24"),
        public_float=15400000000,
        avg_volume_30d=58000000,
        market_cap=Decimal("2850000000000"),
        employees=161000,
        ipo_date=date(1980, 12, 12),
        is_actively_trading=True,
    )


def _make_earnings(symbol: str = "AAPL") -> list[EarningsEvent]:
    return [
        EarningsEvent(
            symbol=symbol,
            fiscal_date_ending=date(2024, 3, 31),
            reporting_date=date(2024, 5, 2),
            eps_estimate=Decimal("1.50"),
            eps_actual=Decimal("1.53"),
            revenue_estimate=Decimal("90000000000"),
            revenue_actual=Decimal("90800000000"),
            surprise_pct=Decimal("2.0"),
        ),
    ]


def _make_estimates(symbol: str = "AAPL") -> AnalystEstimates:
    return AnalystEstimates(
        symbol=symbol,
        as_of_date=date(2024, 6, 30),
        consensus_eps_fq=Decimal("1.45"),
        consensus_eps_fy=None,
        consensus_rev_fq=Decimal("90000000000"),
        consensus_rev_fy=None,
        analyst_count=32,
        buy_count=25,
        hold_count=5,
        sell_count=2,
        price_target_mean=Decimal("210.50"),
        price_target_high=Decimal("250.00"),
        price_target_low=Decimal("180.00"),
    )


def test_upsert_profile_executes_insert():
    conn = MagicMock()
    _upsert_profile(conn, 42, _make_profile(), _NOW)
    conn.execute.assert_called_once()
    sql = conn.execute.call_args[0][0]
    assert "INSERT INTO instrument_profile" in sql
    assert "ON CONFLICT (instrument_id)" in sql


def test_upsert_earnings_executes_insert_per_event():
    conn = MagicMock()
    events = _make_earnings()
    _upsert_earnings_events(conn, 42, events)
    assert conn.execute.call_count == len(events)
    sql = conn.execute.call_args[0][0]
    assert "INSERT INTO earnings_events" in sql


def test_upsert_analyst_estimates_executes_insert():
    conn = MagicMock()
    _upsert_analyst_estimates(conn, 42, _make_estimates())
    conn.execute.assert_called_once()
    sql = conn.execute.call_args[0][0]
    assert "INSERT INTO analyst_estimates" in sql
    assert "ON CONFLICT (instrument_id, as_of_date)" in sql


def test_refresh_enrichment_counts_correctly():
    mock_provider = MagicMock()
    mock_provider.get_profile_enrichment.return_value = _make_profile()
    mock_provider.get_earnings_calendar.return_value = _make_earnings()
    mock_provider.get_analyst_estimates.return_value = _make_estimates()
    mock_conn = MagicMock()

    symbols = [("AAPL", "42"), ("MSFT", "43")]
    summary = refresh_enrichment(mock_provider, mock_conn, symbols)
    assert summary.symbols_attempted == 2
    assert summary.profiles_upserted == 2
    assert summary.symbols_skipped == 0
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_enrichment_service.py -v`
Expected: ImportError.

- [ ] **Step 3: Implement the enrichment service**

Create `app/services/enrichment.py`:

```python
"""
Enrichment service.

Fetches and upserts instrument profile, earnings calendar, and analyst
estimates from an EnrichmentProvider (FMP in v1).

The service layer owns identifier resolution and DB writes.
The provider is a pure HTTP client.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import psycopg

if TYPE_CHECKING:
    from collections.abc import Sequence

    from app.providers.enrichment import (
        AnalystEstimates,
        EarningsEvent,
        EnrichmentProvider,
        InstrumentProfileData,
    )

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class EnrichmentRefreshSummary:
    symbols_attempted: int
    profiles_upserted: int
    earnings_upserted: int
    estimates_upserted: int
    symbols_skipped: int


def refresh_enrichment(
    provider: EnrichmentProvider,
    conn: psycopg.Connection,  # type: ignore[type-arg]
    symbols: Sequence[tuple[str, str]],  # [(symbol, instrument_id), ...]
) -> EnrichmentRefreshSummary:
    """
    For each symbol, fetch profile, earnings, and analyst estimates
    and upsert them. Each symbol is independent — a failure on one
    does not abort the batch.
    """
    profiles = 0
    earnings = 0
    estimates = 0
    skipped = 0
    now = datetime.now(UTC)

    for symbol, instrument_id in symbols:
        iid = int(instrument_id)
        try:
            # Profile
            profile = provider.get_profile_enrichment(symbol)
            if profile is not None:
                _upsert_profile(conn, iid, profile, now)
                profiles += 1

            # Earnings
            events = provider.get_earnings_calendar(symbol)
            if events:
                _upsert_earnings_events(conn, iid, events)
                earnings += len(events)

            # Analyst estimates
            est = provider.get_analyst_estimates(symbol)
            if est is not None:
                _upsert_analyst_estimates(conn, iid, est)
                estimates += 1

        except Exception:
            logger.warning("Enrichment: failed for %s, skipping", symbol, exc_info=True)
            skipped += 1

    return EnrichmentRefreshSummary(
        symbols_attempted=len(symbols),
        profiles_upserted=profiles,
        earnings_upserted=earnings,
        estimates_upserted=estimates,
        symbols_skipped=skipped,
    )


def _upsert_profile(
    conn: psycopg.Connection,  # type: ignore[type-arg]
    instrument_id: int,
    profile: InstrumentProfileData,
    now: datetime,
) -> None:
    """Upsert instrument_profile. Keyed on instrument_id (one row per instrument)."""
    conn.execute(
        """
        INSERT INTO instrument_profile (
            instrument_id, beta, public_float, avg_volume_30d,
            market_cap, employees, ipo_date, is_actively_trading, fetched_at
        )
        VALUES (
            %(instrument_id)s, %(beta)s, %(public_float)s, %(avg_volume_30d)s,
            %(market_cap)s, %(employees)s, %(ipo_date)s, %(is_actively_trading)s,
            %(fetched_at)s
        )
        ON CONFLICT (instrument_id) DO UPDATE SET
            beta               = EXCLUDED.beta,
            public_float       = EXCLUDED.public_float,
            avg_volume_30d     = EXCLUDED.avg_volume_30d,
            market_cap         = EXCLUDED.market_cap,
            employees          = EXCLUDED.employees,
            ipo_date           = EXCLUDED.ipo_date,
            is_actively_trading = EXCLUDED.is_actively_trading,
            fetched_at         = EXCLUDED.fetched_at
        WHERE (
            instrument_profile.beta               IS DISTINCT FROM EXCLUDED.beta               OR
            instrument_profile.public_float       IS DISTINCT FROM EXCLUDED.public_float       OR
            instrument_profile.avg_volume_30d     IS DISTINCT FROM EXCLUDED.avg_volume_30d     OR
            instrument_profile.market_cap         IS DISTINCT FROM EXCLUDED.market_cap         OR
            instrument_profile.employees          IS DISTINCT FROM EXCLUDED.employees          OR
            instrument_profile.ipo_date           IS DISTINCT FROM EXCLUDED.ipo_date           OR
            instrument_profile.is_actively_trading IS DISTINCT FROM EXCLUDED.is_actively_trading
        )
        """,
        {
            "instrument_id": instrument_id,
            "beta": profile.beta,
            "public_float": profile.public_float,
            "avg_volume_30d": profile.avg_volume_30d,
            "market_cap": profile.market_cap,
            "employees": profile.employees,
            "ipo_date": profile.ipo_date,
            "is_actively_trading": profile.is_actively_trading,
            "fetched_at": now,
        },
    )


def _upsert_earnings_events(
    conn: psycopg.Connection,  # type: ignore[type-arg]
    instrument_id: int,
    events: Sequence[EarningsEvent],
) -> None:
    """Upsert earnings events. Keyed on (instrument_id, fiscal_date_ending)."""
    for ev in events:
        conn.execute(
            """
            INSERT INTO earnings_events (
                instrument_id, fiscal_date_ending, reporting_date,
                eps_estimate, eps_actual, revenue_estimate, revenue_actual,
                surprise_pct
            )
            VALUES (
                %(instrument_id)s, %(fiscal_date_ending)s, %(reporting_date)s,
                %(eps_estimate)s, %(eps_actual)s, %(revenue_estimate)s,
                %(revenue_actual)s, %(surprise_pct)s
            )
            ON CONFLICT (instrument_id, fiscal_date_ending) DO UPDATE SET
                reporting_date   = EXCLUDED.reporting_date,
                eps_estimate     = EXCLUDED.eps_estimate,
                eps_actual       = EXCLUDED.eps_actual,
                revenue_estimate = EXCLUDED.revenue_estimate,
                revenue_actual   = EXCLUDED.revenue_actual,
                surprise_pct     = EXCLUDED.surprise_pct,
                fetched_at       = NOW()
            WHERE (
                earnings_events.eps_actual       IS DISTINCT FROM EXCLUDED.eps_actual       OR
                earnings_events.revenue_actual   IS DISTINCT FROM EXCLUDED.revenue_actual   OR
                earnings_events.surprise_pct     IS DISTINCT FROM EXCLUDED.surprise_pct
            )
            """,
            {
                "instrument_id": instrument_id,
                "fiscal_date_ending": ev.fiscal_date_ending,
                "reporting_date": ev.reporting_date,
                "eps_estimate": ev.eps_estimate,
                "eps_actual": ev.eps_actual,
                "revenue_estimate": ev.revenue_estimate,
                "revenue_actual": ev.revenue_actual,
                "surprise_pct": ev.surprise_pct,
            },
        )


def _upsert_analyst_estimates(
    conn: psycopg.Connection,  # type: ignore[type-arg]
    instrument_id: int,
    est: AnalystEstimates,
) -> None:
    """Upsert analyst estimates. Keyed on (instrument_id, as_of_date)."""
    conn.execute(
        """
        INSERT INTO analyst_estimates (
            instrument_id, as_of_date,
            consensus_eps_fq, consensus_eps_fy,
            consensus_rev_fq, consensus_rev_fy,
            analyst_count, buy_count, hold_count, sell_count,
            price_target_mean, price_target_high, price_target_low
        )
        VALUES (
            %(instrument_id)s, %(as_of_date)s,
            %(consensus_eps_fq)s, %(consensus_eps_fy)s,
            %(consensus_rev_fq)s, %(consensus_rev_fy)s,
            %(analyst_count)s, %(buy_count)s, %(hold_count)s, %(sell_count)s,
            %(price_target_mean)s, %(price_target_high)s, %(price_target_low)s
        )
        ON CONFLICT (instrument_id, as_of_date) DO UPDATE SET
            consensus_eps_fq  = EXCLUDED.consensus_eps_fq,
            consensus_eps_fy  = EXCLUDED.consensus_eps_fy,
            consensus_rev_fq  = EXCLUDED.consensus_rev_fq,
            consensus_rev_fy  = EXCLUDED.consensus_rev_fy,
            analyst_count     = EXCLUDED.analyst_count,
            buy_count         = EXCLUDED.buy_count,
            hold_count        = EXCLUDED.hold_count,
            sell_count        = EXCLUDED.sell_count,
            price_target_mean = EXCLUDED.price_target_mean,
            price_target_high = EXCLUDED.price_target_high,
            price_target_low  = EXCLUDED.price_target_low,
            fetched_at        = NOW()
        """,
        {
            "instrument_id": instrument_id,
            "as_of_date": est.as_of_date,
            "consensus_eps_fq": est.consensus_eps_fq,
            "consensus_eps_fy": est.consensus_eps_fy,
            "consensus_rev_fq": est.consensus_rev_fq,
            "consensus_rev_fy": est.consensus_rev_fy,
            "analyst_count": est.analyst_count,
            "buy_count": est.buy_count,
            "hold_count": est.hold_count,
            "sell_count": est.sell_count,
            "price_target_mean": est.price_target_mean,
            "price_target_high": est.price_target_high,
            "price_target_low": est.price_target_low,
        },
    )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_enrichment_service.py -v`
Expected: All tests pass.

- [ ] **Step 5: Commit**

```bash
git add app/services/enrichment.py tests/test_enrichment_service.py
git commit -m "feat(#199): enrichment service — profile, earnings, analyst estimates upsert"
```

---

## Phase 3: Scoring engine enhancement

### Task 5: Enhanced value_score with fundamentals fallback

**Files:**
- Test: `tests/test_scoring_enriched.py`
- Modify: `app/services/scoring.py`

- [ ] **Step 1: Write failing tests for enhanced _value_score**

Create `tests/test_scoring_enriched.py`:

```python
"""Tests for scoring engine enrichment — fundamentals-derived value signals."""
from __future__ import annotations

import pytest

from app.services.scoring import _value_score


def _approx(v: float, rel: float = 1e-4) -> object:
    return pytest.approx(v, rel=rel)


# ── Thesis-based scoring still works (existing behaviour) ────

def test_value_score_thesis_based_unchanged():
    """When thesis valuation bands exist, they dominate."""
    score, notes = _value_score(
        base_value=150.0, bear_value=80.0, current_price=100.0,
        pe_ratio=None, fcf_yield=None, price_target_mean=None,
    )
    # 50% upside => upside_score=1.0; downside=(100-80)/100=0.20 => penalty=0.40
    # 0.75 * 1.0 + 0.25 * (1 - 0.40) = 0.75 + 0.15 = 0.90
    assert score == _approx(0.90)
    assert not any("fundamentals fallback" in n for n in notes)


# ── Fundamentals fallback when thesis is missing ─────────────

def test_value_score_fundamentals_fallback_cheap_stock():
    """P/E = 10, FCF yield = 8%, price target 20% above => attractive."""
    score, notes = _value_score(
        base_value=None, bear_value=None, current_price=100.0,
        pe_ratio=10.0, fcf_yield=0.08, price_target_mean=120.0,
    )
    assert score > 0.6
    assert any("fundamentals fallback" in n for n in notes)


def test_value_score_fundamentals_fallback_expensive_stock():
    """P/E = 60, FCF yield = 0.5%, price target below current => expensive."""
    score, notes = _value_score(
        base_value=None, bear_value=None, current_price=100.0,
        pe_ratio=60.0, fcf_yield=0.005, price_target_mean=85.0,
    )
    assert score < 0.4
    assert any("fundamentals fallback" in n for n in notes)


def test_value_score_no_thesis_no_fundamentals():
    """No thesis and no enrichment data => neutral 0.5."""
    score, notes = _value_score(
        base_value=None, bear_value=None, current_price=100.0,
        pe_ratio=None, fcf_yield=None, price_target_mean=None,
    )
    assert score == _approx(0.5)


def test_value_score_partial_fundamentals():
    """Only P/E available, no FCF yield or price target."""
    score, notes = _value_score(
        base_value=None, bear_value=None, current_price=100.0,
        pe_ratio=15.0, fcf_yield=None, price_target_mean=None,
    )
    assert 0.3 < score < 0.8  # reasonable range for moderate P/E
    assert any("fundamentals fallback" in n for n in notes)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_scoring_enriched.py -v`
Expected: TypeError — `_value_score` does not accept enrichment params yet.

- [ ] **Step 3: Modify `_value_score` to accept enrichment parameters**

In `app/services/scoring.py`, update `_value_score` signature and add fundamentals fallback path:

```python
def _value_score(
    base_value: float | None,
    bear_value: float | None,
    current_price: float | None,
    *,
    pe_ratio: float | None = None,
    fcf_yield: float | None = None,
    price_target_mean: float | None = None,
) -> tuple[float, list[str]]:
    """
    Thesis valuation upside as the primary value proxy.
    Falls back to fundamentals-derived signals when thesis is absent.
    """
    notes: list[str] = []

    # ── Primary path: thesis-based valuation ──────────────────
    if base_value is not None and current_price is not None and current_price > 0:
        if bear_value is None:
            notes.append("bear_value missing")

        upside_to_base = (base_value - current_price) / current_price
        upside_score = _clip(upside_to_base / 0.50)

        if bear_value is not None:
            downside_to_bear = (current_price - bear_value) / current_price
            downside_penalty = _clip(downside_to_bear / 0.50)
        else:
            downside_penalty = 0.5
            notes.append("bear_value missing; assuming 0.5 downside penalty")

        score = 0.75 * upside_score + 0.25 * (1.0 - downside_penalty)
        return _clip(score), notes

    # ── Fallback path: fundamentals-derived signals ───────────
    # Used when thesis is missing. Blends P/E attractiveness,
    # FCF yield, and analyst price target upside.
    components: list[tuple[float, float]] = []  # (score, weight)

    if current_price is None or current_price <= 0:
        notes.append("current_price missing or zero")
        return 0.5, notes

    if pe_ratio is not None and pe_ratio > 0:
        # P/E score: lower is better. P/E=10 => 1.0, P/E=30 => 0.33, P/E=50+ => ~0
        pe_score = _clip(1.0 - (pe_ratio - 10.0) / 40.0)
        components.append((pe_score, 0.35))
    else:
        notes.append("pe_ratio missing or non-positive")

    if fcf_yield is not None:
        # FCF yield score: higher is better. 8% => 1.0, 0% => 0.0
        fy_score = _clip(fcf_yield / 0.08)
        components.append((fy_score, 0.35))
    else:
        notes.append("fcf_yield missing")

    if price_target_mean is not None and price_target_mean > 0:
        # Price target upside: (target - price) / price, scaled like thesis upside
        pt_upside = (price_target_mean - current_price) / current_price
        pt_score = _clip(pt_upside / 0.50)
        components.append((pt_score, 0.30))
    else:
        notes.append("price_target_mean missing")

    if not components:
        return 0.5, notes  # neutral-by-absence

    notes.append("fundamentals fallback (no thesis)")
    total_weight = sum(w for _, w in components)
    score = sum(s * w / total_weight for s, w in components)
    return _clip(score), notes
```

- [ ] **Step 4: Update `_load_instrument_data` to fetch enrichment data**

Add to the existing cursor block in `_load_instrument_data`:

```python
        # Valuation multiples from view (enrichment)
        cur.execute(
            """
            SELECT pe_ratio, pb_ratio, p_fcf_ratio, fcf_yield,
                   debt_equity_ratio, market_cap_live, current_price
            FROM instrument_valuation
            WHERE instrument_id = %(id)s
            """,
            {"id": instrument_id},
        )
        valuation_row: dict[str, Any] | None = cur.fetchone()

        # Analyst estimates (latest)
        cur.execute(
            """
            SELECT price_target_mean, price_target_high, price_target_low,
                   analyst_count, buy_count, hold_count, sell_count
            FROM analyst_estimates
            WHERE instrument_id = %(id)s
            ORDER BY as_of_date DESC
            LIMIT 1
            """,
            {"id": instrument_id},
        )
        estimates_row: dict[str, Any] | None = cur.fetchone()
```

Add to the return dict:

```python
        "valuation_row": valuation_row,
        "estimates_row": estimates_row,
```

- [ ] **Step 5: Update `compute_score` to pass enrichment data to `_value_score`**

In the `compute_score` function, where `_value_score` is called, update to pass enrichment params:

```python
    val_row = data.get("valuation_row")
    est_row = data.get("estimates_row")

    value, value_notes = _value_score(
        base_value=_to_float(thesis_row["base_value"]) if thesis_row else None,
        bear_value=_to_float(thesis_row["bear_value"]) if thesis_row else None,
        current_price=current_price,
        pe_ratio=_to_float(val_row["pe_ratio"]) if val_row else None,
        fcf_yield=_to_float(val_row["fcf_yield"]) if val_row else None,
        price_target_mean=_to_float(est_row["price_target_mean"]) if est_row else None,
    )
```

- [ ] **Step 6: Run all scoring tests**

```bash
uv run pytest tests/test_scoring.py tests/test_scoring_enriched.py -v
```

Expected: All pass. Existing tests should not break because the new params have defaults.

- [ ] **Step 7: Run full check suite**

```bash
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv run pytest
```

- [ ] **Step 8: Commit**

```bash
git add app/services/scoring.py tests/test_scoring_enriched.py
git commit -m "feat(#199): enhance value_score with fundamentals-derived fallback"
```

---

## Phase 4: Scheduler integration

### Task 6: Wire enrichment into daily_research_refresh

**Files:**
- Modify: `app/workers/scheduler.py`

- [ ] **Step 1: Add enrichment call after FMP fundamentals refresh**

In `daily_research_refresh()`, after the FMP fundamentals block (~line 722), add:

```python
        # Enrichment — profile, earnings, analyst estimates (FMP only)
        if settings.fmp_api_key:
            try:
                with (
                    FmpFundamentalsProvider(api_key=settings.fmp_api_key) as fmp,
                    psycopg.connect(settings.database_url) as conn,
                ):
                    from app.services.enrichment import refresh_enrichment

                    enrich_summary = refresh_enrichment(fmp, conn, symbols)
                    conn.commit()
                total_rows += enrich_summary.profiles_upserted + enrich_summary.earnings_upserted
                logger.info(
                    "Enrichment refresh: attempted=%d profiles=%d earnings=%d estimates=%d skipped=%d",
                    enrich_summary.symbols_attempted,
                    enrich_summary.profiles_upserted,
                    enrich_summary.earnings_upserted,
                    enrich_summary.estimates_upserted,
                    enrich_summary.symbols_skipped,
                )
            except Exception:
                logger.warning("Enrichment refresh failed", exc_info=True)
```

Note: The import should be at the top of the file, not inline. Move it to the imports section:

```python
from app.services.enrichment import refresh_enrichment
```

- [ ] **Step 2: Run smoke test**

```bash
uv run pytest tests/smoke/test_app_boots.py -v
```

Expected: App boots — the new migration and import don't break startup.

- [ ] **Step 3: Commit**

```bash
git add app/workers/scheduler.py
git commit -m "feat(#199): wire enrichment refresh into daily_research_refresh job"
```

---

## Phase 5: Thesis context enrichment

### Task 7: Pass earnings and estimates context to thesis writer

**Files:**
- Modify: `app/services/thesis.py`

- [ ] **Step 1: Add earnings and estimates to _assemble_context**

In `_assemble_context`, add after the fundamentals fetch block:

```python
        # Earnings history (latest 4 quarters)
        cur.execute(
            """
            SELECT fiscal_date_ending, reporting_date,
                   eps_estimate, eps_actual, revenue_estimate, revenue_actual,
                   surprise_pct
            FROM earnings_events
            WHERE instrument_id = %(id)s
              AND eps_actual IS NOT NULL
            ORDER BY fiscal_date_ending DESC
            LIMIT 4
            """,
            {"id": instrument_id},
        )
        earnings_rows = cur.fetchall()

        # Analyst estimates (latest)
        cur.execute(
            """
            SELECT consensus_eps_fq, analyst_count, buy_count, hold_count,
                   sell_count, price_target_mean, price_target_high, price_target_low
            FROM analyst_estimates
            WHERE instrument_id = %(id)s
            ORDER BY as_of_date DESC
            LIMIT 1
            """,
            {"id": instrument_id},
        )
        estimates_row = cur.fetchone()
```

Add to the returned context dict:

```python
    "earnings_history": [
        {
            "fiscal_date": str(r["fiscal_date_ending"]),
            "eps_estimate": _to_float(r["eps_estimate"]),
            "eps_actual": _to_float(r["eps_actual"]),
            "revenue_actual": _to_float(r["revenue_actual"]),
            "surprise_pct": _to_float(r["surprise_pct"]),
        }
        for r in earnings_rows
    ],
    "analyst_estimates": {
        "consensus_eps": _to_float(estimates_row["consensus_eps_fq"]),
        "analyst_count": estimates_row["analyst_count"],
        "buy_count": estimates_row["buy_count"],
        "hold_count": estimates_row["hold_count"],
        "sell_count": estimates_row["sell_count"],
        "price_target_mean": _to_float(estimates_row["price_target_mean"]),
        "price_target_high": _to_float(estimates_row["price_target_high"]),
        "price_target_low": _to_float(estimates_row["price_target_low"]),
    } if estimates_row else None,
```

- [ ] **Step 2: Run existing thesis tests**

```bash
uv run pytest tests/test_thesis.py -v
```

Expected: All pass — the new context fields are additive and don't affect existing mock setup.

- [ ] **Step 3: Commit**

```bash
git add app/services/thesis.py
git commit -m "feat(#199): pass earnings history and analyst estimates to thesis writer context"
```

---

## Phase 6: Final checks and PR

### Task 8: Pre-flight review, Codex review, push

- [ ] **Step 1: Run full check suite**

```bash
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv run pytest
```

All four must pass.

- [ ] **Step 2: Self-review the diff**

```bash
git diff origin/main...HEAD
```

Run through the pre-flight review checklist (`.claude/skills/engineering/pre-flight-review.md`).

- [ ] **Step 3: Run Codex review**

```bash
codex.cmd review --base main
```

Evaluate suggestions critically. Fix real issues, rebut false positives.

- [ ] **Step 4: Push and open PR**

```bash
git push -u origin feature/199-fundamentals-enrichment
gh pr create --title "feat(#199): fundamentals enrichment — valuation multiples, earnings, analyst estimates" --body "..."
```

- [ ] **Step 5: Poll review and CI**

```bash
gh pr checks <n>
gh pr view <n> --comments
```

Wait for Claude review + CI green. Resolve all comments per the review comment resolution contract.
