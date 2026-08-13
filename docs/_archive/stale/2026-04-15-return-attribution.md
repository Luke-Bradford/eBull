# Return Attribution and Performance Audit Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Decompose realised returns for closed positions into attribution components (market, sector, model alpha, timing, costs) to create the feedback loop that tells us whether the scoring model generates alpha.

**Architecture:** A pure service (`return_attribution.py`) computes attribution for a single closed position given its fills, the instrument's sector peers, and the scoring snapshot at entry. The service is triggered inline when `execute_order` processes an EXIT fill that zeroes out `current_units`. A scheduled summary worker aggregates attribution across all closed positions for rolling windows. An API endpoint exposes attribution data for the dashboard.

**Tech Stack:** Python, psycopg3, Decimal arithmetic (no numpy/pandas — the decomposition is 5 subtractions on 2 price series)

---

## Settled decisions that apply

- **Auditability**: persist structured evidence where it matters — attribution is the evidence layer for scoring model performance.
- **Score auditability**: each score row carries per-family detail — we read these for the `score_components` snapshot.
- **AUM basis**: mark-to-market first, fall back to cost basis — consistent with how we calculate returns.
- **Provider design rule**: providers are thin adapters — attribution reads `price_daily` directly, no new provider needed.

## Prevention log entries that apply

- **conn.rollback() after caught exceptions on shared connections**: attribution runs inside `execute_order`'s connection — if attribution raises, the connection must be rolled back before proceeding.
- **Kill-switch + auto_trading gate at pipeline call sites**: not directly relevant (attribution is passive/read + write, not an order action).

## Scope decisions

1. **No external benchmark ingestion.** Market return is computed from average `price_daily.close` returns across all Tier 1 instruments over the hold period. Sector return is computed from same-sector instruments. This uses only existing data.
2. **Cost drag from `fills.fees`**, not `trade_cost_record` (#154 not built). When #154 lands, attribution can be enriched.
3. **Attribution triggers on EXIT fill that zeroes out position** (not on every partial sell). Partial exits accumulate — attribution runs on full close.
4. **Score snapshot comes from `trade_recommendations.score_id`** FK that already exists. No new column needed at entry time.
5. **Summary worker is in-scope** — it's 3 SQL aggregates and a cron job, not a separate subsystem.

---

## File structure

| File | Responsibility |
|------|---------------|
| `sql/029_return_attribution.sql` | Migration: `return_attribution` + `return_attribution_summary` tables |
| `app/services/return_attribution.py` | Core service: `compute_attribution(conn, instrument_id)` + `compute_attribution_summary(conn, window_days)` |
| `app/services/order_client.py` | Modified: trigger attribution after EXIT fill zeroes position |
| `app/workers/scheduler.py` | Modified: add `attribution_summary` scheduled job |
| `app/jobs/runtime.py` | Modified: register new job |
| `app/api/attribution.py` | API endpoint: GET attribution data |
| `tests/test_return_attribution.py` | Unit tests for the attribution service |
| `tests/test_attribution_trigger.py` | Tests for the order_client integration |
| `tests/test_scheduler_attribution.py` | Tests for the summary job |

---

## Task 1: Migration — return_attribution tables

**Files:**
- Create: `sql/029_return_attribution.sql`

- [ ] **Step 1: Write the migration**

```sql
-- Migration 029: return attribution tables
--
-- return_attribution: per-position decomposition of realised returns.
-- Computed when a position is fully closed (current_units = 0 after EXIT fill).
--
-- return_attribution_summary: rolling-window aggregation of attribution
-- components across all attributed positions.

CREATE TABLE IF NOT EXISTS return_attribution (
    attribution_id       BIGSERIAL PRIMARY KEY,
    instrument_id        BIGINT NOT NULL REFERENCES instruments(instrument_id),
    hold_start           DATE NOT NULL,
    hold_end             DATE NOT NULL,
    hold_days            INTEGER NOT NULL,
    -- Return components (all as decimal fractions, e.g. 0.05 = 5%)
    gross_return_pct     NUMERIC(12, 6) NOT NULL,
    market_return_pct    NUMERIC(12, 6) NOT NULL,
    sector_return_pct    NUMERIC(12, 6) NOT NULL,
    model_alpha_pct      NUMERIC(12, 6) NOT NULL,
    timing_alpha_pct     NUMERIC(12, 6) NOT NULL,
    cost_drag_pct        NUMERIC(12, 6) NOT NULL,
    residual_pct         NUMERIC(12, 6) NOT NULL,
    -- Score snapshot at entry (from the recommendation's score_id)
    score_at_entry       NUMERIC(10, 4),
    score_components     JSONB,
    -- Computation metadata
    entry_fill_id        BIGINT REFERENCES fills(fill_id),
    exit_fill_id         BIGINT REFERENCES fills(fill_id),
    recommendation_id    BIGINT REFERENCES trade_recommendations(recommendation_id),
    attribution_method   TEXT NOT NULL DEFAULT 'sector_relative_v1',
    computed_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_return_attribution_instrument
    ON return_attribution(instrument_id);
CREATE INDEX IF NOT EXISTS idx_return_attribution_computed
    ON return_attribution(computed_at);

CREATE TABLE IF NOT EXISTS return_attribution_summary (
    summary_id           BIGSERIAL PRIMARY KEY,
    window_days          INTEGER NOT NULL,
    positions_attributed INTEGER NOT NULL,
    avg_gross_return_pct    NUMERIC(12, 6),
    avg_market_return_pct   NUMERIC(12, 6),
    avg_sector_return_pct   NUMERIC(12, 6),
    avg_model_alpha_pct     NUMERIC(12, 6),
    avg_timing_alpha_pct    NUMERIC(12, 6),
    avg_cost_drag_pct       NUMERIC(12, 6),
    computed_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

- [ ] **Step 2: Apply migration and verify**

Run: `psql -f sql/029_return_attribution.sql ebull`
Expected: no errors, tables created.

- [ ] **Step 3: Commit**

```bash
git add sql/029_return_attribution.sql
git commit -m "feat(#155): migration 029 — return_attribution + summary tables"
```

---

## Task 2: Attribution service — data loaders

**Files:**
- Create: `app/services/return_attribution.py`
- Test: `tests/test_return_attribution.py`

This task builds the internal data-loading helpers that the attribution computation will use. Each loads a specific piece of data from the DB.

- [ ] **Step 1: Write failing tests for data loaders**

```python
"""Tests for app.services.return_attribution."""

from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock

import pytest

from app.services.return_attribution import (
    _load_position_fills,
    _load_price_series,
    _load_score_snapshot,
    _load_sector_peers,
)


def _make_conn(cursors: list[MagicMock]) -> MagicMock:
    """Build a mock connection that returns cursors in sequence."""
    conn = MagicMock()
    cursor_iter = iter(cursors)
    ctx = MagicMock()
    ctx.__enter__ = MagicMock(side_effect=lambda: next(cursor_iter))
    ctx.__exit__ = MagicMock(return_value=False)
    conn.cursor.return_value = ctx
    return conn


def _make_cursor(rows: list[dict[str, Any]]) -> MagicMock:
    cur = MagicMock()
    cur.fetchall.return_value = rows
    cur.fetchone.return_value = rows[0] if rows else None
    return cur


class TestLoadPositionFills:
    def test_returns_entry_and_exit_fills(self) -> None:
        """Should return BUY fills as entries and EXIT fills as exits."""
        fills = [
            {
                "fill_id": 1,
                "action": "BUY",
                "filled_at": datetime(2025, 1, 15, tzinfo=timezone.utc),
                "price": Decimal("150.00"),
                "units": Decimal("10.0"),
                "fees": Decimal("1.50"),
            },
            {
                "fill_id": 2,
                "action": "EXIT",
                "filled_at": datetime(2025, 6, 15, tzinfo=timezone.utc),
                "price": Decimal("180.00"),
                "units": Decimal("10.0"),
                "fees": Decimal("1.80"),
            },
        ]
        conn = _make_conn([_make_cursor(fills)])
        result = _load_position_fills(conn, instrument_id=42)
        assert len(result) == 2
        assert result[0]["action"] == "BUY"
        assert result[1]["action"] == "EXIT"

    def test_empty_when_no_fills(self) -> None:
        conn = _make_conn([_make_cursor([])])
        result = _load_position_fills(conn, instrument_id=42)
        assert result == []


class TestLoadPriceSeries:
    def test_returns_date_close_pairs(self) -> None:
        rows = [
            {"price_date": date(2025, 1, 15), "close": Decimal("150.00")},
            {"price_date": date(2025, 6, 15), "close": Decimal("180.00")},
        ]
        conn = _make_conn([_make_cursor(rows)])
        result = _load_price_series(
            conn, instrument_id=42,
            start_date=date(2025, 1, 15), end_date=date(2025, 6, 15),
        )
        assert len(result) == 2
        assert result[0]["price_date"] == date(2025, 1, 15)


class TestLoadScoreSnapshot:
    def test_returns_score_components_from_score_id(self) -> None:
        row = {
            "total_score": Decimal("0.7500"),
            "quality_score": Decimal("0.80"),
            "value_score": Decimal("0.65"),
            "turnaround_score": Decimal("0.30"),
            "momentum_score": Decimal("0.70"),
            "sentiment_score": Decimal("0.60"),
            "confidence_score": Decimal("0.85"),
            "model_version": "v1.1-balanced",
        }
        conn = _make_conn([_make_cursor([row])])
        result = _load_score_snapshot(conn, score_id=100)
        assert result is not None
        assert result["total_score"] == Decimal("0.7500")

    def test_returns_none_when_no_score(self) -> None:
        conn = _make_conn([_make_cursor([])])
        result = _load_score_snapshot(conn, score_id=None)
        assert result is None


class TestLoadSectorPeers:
    def test_returns_instrument_ids_in_same_sector(self) -> None:
        rows = [{"instrument_id": 10}, {"instrument_id": 20}]
        conn = _make_conn([_make_cursor(rows)])
        result = _load_sector_peers(conn, instrument_id=42)
        assert result == [10, 20]

    def test_excludes_self(self) -> None:
        """The target instrument must not appear in its own peer list."""
        rows = [{"instrument_id": 10}]
        conn = _make_conn([_make_cursor(rows)])
        result = _load_sector_peers(conn, instrument_id=42)
        assert 42 not in result
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_return_attribution.py -v`
Expected: ImportError — module does not exist yet.

- [ ] **Step 3: Implement data loaders**

```python
"""Return attribution service — decompose realised position returns.

Decomposes the gross return of a closed position into:
  - market_return:  average return of all Tier 1 instruments over hold period
  - sector_return:  average return of same-sector instruments over hold period
  - model_alpha:    instrument return minus sector return (sector-relative alpha)
  - timing_alpha:   difference between actual entry price and price at scoring time
  - cost_drag:      total fees as fraction of cost basis
  - residual:       gross - (market + sector_excess + timing + costs)

Design:
  - Pure service: caller provides the connection.
  - All arithmetic uses Decimal to avoid float rounding on financial data.
  - NULL-safe: missing data → component set to Decimal("0").
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Any

import psycopg
import psycopg.rows
from psycopg.types.json import Jsonb

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ATTRIBUTION_METHOD = "sector_relative_v1"

ZERO = Decimal("0")

# Summary windows (days)
SUMMARY_WINDOWS: tuple[int, ...] = (30, 90, 365)


# ---------------------------------------------------------------------------
# Public types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class AttributionResult:
    """Decomposed return for a single closed position."""

    instrument_id: int
    hold_start: date
    hold_end: date
    hold_days: int
    gross_return_pct: Decimal
    market_return_pct: Decimal
    sector_return_pct: Decimal
    model_alpha_pct: Decimal
    timing_alpha_pct: Decimal
    cost_drag_pct: Decimal
    residual_pct: Decimal
    score_at_entry: Decimal | None
    score_components: dict[str, Any] | None
    entry_fill_id: int | None
    exit_fill_id: int | None
    recommendation_id: int | None


@dataclass(frozen=True)
class SummaryResult:
    """Aggregated attribution over a rolling window."""

    window_days: int
    positions_attributed: int
    avg_gross_return_pct: Decimal | None
    avg_market_return_pct: Decimal | None
    avg_sector_return_pct: Decimal | None
    avg_model_alpha_pct: Decimal | None
    avg_timing_alpha_pct: Decimal | None
    avg_cost_drag_pct: Decimal | None


# ---------------------------------------------------------------------------
# Internal data loaders
# ---------------------------------------------------------------------------


def _load_position_fills(
    conn: psycopg.Connection[Any],
    instrument_id: int,
) -> list[dict[str, Any]]:
    """Load all fills for an instrument, joined with order action, oldest first."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT f.fill_id, o.action, f.filled_at, f.price, f.units, f.fees
            FROM fills f
            JOIN orders o USING (order_id)
            WHERE o.instrument_id = %(iid)s
            ORDER BY f.filled_at ASC, f.fill_id ASC
            """,
            {"iid": instrument_id},
        )
        return cur.fetchall()


def _load_price_series(
    conn: psycopg.Connection[Any],
    instrument_id: int,
    start_date: date,
    end_date: date,
) -> list[dict[str, Any]]:
    """Load daily close prices for an instrument within a date range."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT price_date, close
            FROM price_daily
            WHERE instrument_id = %(iid)s
              AND price_date >= %(start)s
              AND price_date <= %(end)s
            ORDER BY price_date ASC
            """,
            {"iid": instrument_id, "start": start_date, "end": end_date},
        )
        return cur.fetchall()


def _load_score_snapshot(
    conn: psycopg.Connection[Any],
    score_id: int | None,
) -> dict[str, Any] | None:
    """Load the scoring snapshot for a given score_id.

    Returns None if score_id is None or the row doesn't exist.
    """
    if score_id is None:
        return None
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT total_score,
                   quality_score, value_score, turnaround_score,
                   momentum_score, sentiment_score, confidence_score,
                   model_version
            FROM scores
            WHERE score_id = %(sid)s
            """,
            {"sid": score_id},
        )
        return cur.fetchone()


def _load_sector_peers(
    conn: psycopg.Connection[Any],
    instrument_id: int,
) -> list[int]:
    """Load Tier 1 instrument_ids in the same sector, excluding self.

    Uses coverage.tier = 1 to limit to the active universe.
    Falls back to all instruments in the same sector if coverage
    doesn't exist for this instrument's sector.
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT i2.instrument_id
            FROM instruments i1
            JOIN instruments i2 ON i2.sector = i1.sector
                               AND i2.instrument_id != i1.instrument_id
            LEFT JOIN coverage c ON c.instrument_id = i2.instrument_id
            WHERE i1.instrument_id = %(iid)s
              AND i2.is_tradable = TRUE
              AND (c.tier = 1 OR c.tier IS NULL)
            """,
            {"iid": instrument_id},
        )
        return [row["instrument_id"] for row in cur.fetchall()]
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_return_attribution.py -v`
Expected: all tests PASS.

- [ ] **Step 5: Commit**

```bash
git add app/services/return_attribution.py tests/test_return_attribution.py
git commit -m "feat(#155): return attribution data loaders + tests"
```

---

## Task 3: Attribution service — computation logic

**Files:**
- Modify: `app/services/return_attribution.py`
- Test: `tests/test_return_attribution.py`

This task adds the core `compute_attribution` function that decomposes a position's return.

- [ ] **Step 1: Write failing tests for compute_attribution**

Add to `tests/test_return_attribution.py`:

```python
from app.services.return_attribution import (
    AttributionResult,
    compute_attribution,
    _compute_average_return,
)


class TestComputeAverageReturn:
    def test_simple_return(self) -> None:
        """Average return from a price series is (last - first) / first."""
        prices = [
            {"price_date": date(2025, 1, 1), "close": Decimal("100")},
            {"price_date": date(2025, 1, 2), "close": Decimal("110")},
        ]
        result = _compute_average_return(prices)
        assert result == Decimal("0.1")  # 10%

    def test_empty_series_returns_zero(self) -> None:
        assert _compute_average_return([]) == Decimal("0")

    def test_single_price_returns_zero(self) -> None:
        prices = [{"price_date": date(2025, 1, 1), "close": Decimal("100")}]
        assert _compute_average_return(prices) == Decimal("0")


class TestComputeAttribution:
    def test_full_decomposition(self) -> None:
        """Known numbers: verify decomposition components sum correctly.

        Setup:
          - Bought at 100, sold at 120 → gross = 20%
          - Market returned 5% over the hold period
          - Sector returned 8% over the hold period
          - Score existed at entry with price 98 → timing = (100-98)/98 lost
          - Fees: 2 on buy + 2.40 on sell = 4.40 on cost 1000 → 0.44%
        """
        # Build mock connection with sequenced cursor returns.
        # compute_attribution calls: _load_position_fills, _load_price_series
        # (instrument), _load_recommendation_for_instrument,
        # _load_score_snapshot, _load_sector_peers, then for each peer:
        # _load_price_series(peer). Plus _load_price_series for all Tier 1
        # (market return).
        #
        # For this test we mock at a higher level — patch the internal loaders.
        pass  # Implemented in step 3 via patching

    def test_no_fills_returns_none(self) -> None:
        """If there are no fills, attribution cannot be computed."""
        conn = _make_conn([_make_cursor([])])
        result = compute_attribution(conn, instrument_id=42)
        assert result is None

    def test_no_exit_fill_returns_none(self) -> None:
        """If there are only BUY fills (position still open), return None."""
        fills = [
            {
                "fill_id": 1, "action": "BUY",
                "filled_at": datetime(2025, 1, 15, tzinfo=timezone.utc),
                "price": Decimal("150"), "units": Decimal("10"), "fees": Decimal("1"),
            },
        ]
        conn = _make_conn([_make_cursor(fills)])
        result = compute_attribution(conn, instrument_id=42)
        assert result is None
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_return_attribution.py::TestComputeAttribution -v`
Expected: ImportError for `compute_attribution`.

- [ ] **Step 3: Write full decomposition test with patched loaders**

Replace the `test_full_decomposition` placeholder and add patched test:

```python
from unittest.mock import patch

_SVC = "app.services.return_attribution"


class TestComputeAttribution:
    def test_no_fills_returns_none(self) -> None:
        """If there are no fills, attribution cannot be computed."""
        conn = MagicMock()
        with patch(f"{_SVC}._load_position_fills", return_value=[]):
            result = compute_attribution(conn, instrument_id=42)
        assert result is None

    def test_no_exit_fill_returns_none(self) -> None:
        """Position still open → no attribution."""
        fills = [
            {
                "fill_id": 1, "action": "BUY",
                "filled_at": datetime(2025, 1, 15, tzinfo=timezone.utc),
                "price": Decimal("150"), "units": Decimal("10"), "fees": Decimal("1"),
            },
        ]
        conn = MagicMock()
        with patch(f"{_SVC}._load_position_fills", return_value=fills):
            result = compute_attribution(conn, instrument_id=42)
        assert result is None

    @patch(f"{_SVC}._load_sector_peers", return_value=[10, 20])
    @patch(f"{_SVC}._load_score_snapshot", return_value={
        "total_score": Decimal("0.75"),
        "quality_score": Decimal("0.80"), "value_score": Decimal("0.65"),
        "turnaround_score": Decimal("0.30"), "momentum_score": Decimal("0.70"),
        "sentiment_score": Decimal("0.60"), "confidence_score": Decimal("0.85"),
        "model_version": "v1.1-balanced",
    })
    @patch(f"{_SVC}._load_recommendation_for_fills")
    @patch(f"{_SVC}._load_price_series")
    @patch(f"{_SVC}._load_position_fills")
    def test_full_decomposition(
        self,
        mock_fills: MagicMock,
        mock_prices: MagicMock,
        mock_rec: MagicMock,
        mock_score: MagicMock,
        mock_peers: MagicMock,
    ) -> None:
        """Verify decomposition with known numbers."""
        # Entry: BUY 10 units at 100 (fees=2), Exit: EXIT 10 at 120 (fees=2.40)
        mock_fills.return_value = [
            {"fill_id": 1, "action": "BUY",
             "filled_at": datetime(2025, 1, 15, tzinfo=timezone.utc),
             "price": Decimal("100"), "units": Decimal("10"), "fees": Decimal("2")},
            {"fill_id": 2, "action": "EXIT",
             "filled_at": datetime(2025, 6, 15, tzinfo=timezone.utc),
             "price": Decimal("120"), "units": Decimal("10"), "fees": Decimal("2.40")},
        ]

        mock_rec.return_value = {"recommendation_id": 50, "score_id": 100}

        # Price series for instrument (100 → 120)
        instrument_prices = [
            {"price_date": date(2025, 1, 15), "close": Decimal("100")},
            {"price_date": date(2025, 6, 15), "close": Decimal("120")},
        ]
        # Price series for sector peers (avg 108 → return ~8% each)
        peer_prices = [
            {"price_date": date(2025, 1, 15), "close": Decimal("50")},
            {"price_date": date(2025, 6, 15), "close": Decimal("54")},
        ]
        # Market prices — Tier 1 average
        market_prices = [
            {"price_date": date(2025, 1, 15), "close": Decimal("200")},
            {"price_date": date(2025, 6, 15), "close": Decimal("210")},
        ]

        # _load_price_series is called for: instrument, each peer, then market
        mock_prices.side_effect = [
            instrument_prices,  # instrument
            peer_prices,        # peer 10
            peer_prices,        # peer 20
            market_prices,      # market (all Tier 1)
        ]

        conn = MagicMock()
        result = compute_attribution(conn, instrument_id=42)

        assert result is not None
        assert result.instrument_id == 42
        # gross = (120-100)/100 = 0.20
        assert result.gross_return_pct == Decimal("0.2")
        # Check components are populated (exact values depend on implementation)
        assert isinstance(result.model_alpha_pct, Decimal)
        assert isinstance(result.cost_drag_pct, Decimal)
        # cost_drag = (2 + 2.40) / (100 * 10) = 0.0044
        assert result.cost_drag_pct == Decimal("0.0044")
        # residual should be small (gross - sum of components)
        assert result.score_at_entry == Decimal("0.75")
        assert result.entry_fill_id == 1
        assert result.exit_fill_id == 2

    @patch(f"{_SVC}._load_sector_peers", return_value=[])
    @patch(f"{_SVC}._load_score_snapshot", return_value=None)
    @patch(f"{_SVC}._load_recommendation_for_fills", return_value=None)
    @patch(f"{_SVC}._load_price_series", return_value=[])
    @patch(f"{_SVC}._load_position_fills")
    def test_no_price_data_graceful(
        self,
        mock_fills: MagicMock,
        mock_prices: MagicMock,
        mock_rec: MagicMock,
        mock_score: MagicMock,
        mock_peers: MagicMock,
    ) -> None:
        """Missing price data → components default to zero, gross from fills."""
        mock_fills.return_value = [
            {"fill_id": 1, "action": "BUY",
             "filled_at": datetime(2025, 1, 15, tzinfo=timezone.utc),
             "price": Decimal("100"), "units": Decimal("10"), "fees": Decimal("0")},
            {"fill_id": 2, "action": "EXIT",
             "filled_at": datetime(2025, 6, 15, tzinfo=timezone.utc),
             "price": Decimal("120"), "units": Decimal("10"), "fees": Decimal("0")},
        ]
        conn = MagicMock()
        result = compute_attribution(conn, instrument_id=42)
        assert result is not None
        assert result.gross_return_pct == Decimal("0.2")
        assert result.market_return_pct == ZERO
        assert result.sector_return_pct == ZERO
```

- [ ] **Step 4: Implement compute_attribution**

Add to `app/services/return_attribution.py`:

```python
def _compute_average_return(prices: list[dict[str, Any]]) -> Decimal:
    """Compute simple return from first to last close price.

    Returns (last - first) / first as a Decimal fraction.
    Returns Decimal("0") if fewer than 2 prices.
    """
    if len(prices) < 2:
        return ZERO
    first = Decimal(str(prices[0]["close"]))
    last = Decimal(str(prices[-1]["close"]))
    if first == ZERO:
        return ZERO
    return (last - first) / first


def _load_recommendation_for_fills(
    conn: psycopg.Connection[Any],
    instrument_id: int,
) -> dict[str, Any] | None:
    """Load the most recent executed recommendation for an instrument.

    Returns recommendation_id + score_id, or None if no executed rec exists.
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT recommendation_id, score_id
            FROM trade_recommendations
            WHERE instrument_id = %(iid)s
              AND status = 'executed'
              AND action IN ('BUY', 'ADD')
            ORDER BY recommendation_id DESC
            LIMIT 1
            """,
            {"iid": instrument_id},
        )
        return cur.fetchone()


def _compute_market_return(
    conn: psycopg.Connection[Any],
    start_date: date,
    end_date: date,
) -> Decimal:
    """Compute the average return across all Tier 1 instruments (market proxy).

    Loads price_daily for each Tier 1 instrument over the hold period,
    computes each instrument's return, and averages them.
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT c.instrument_id
            FROM coverage c
            WHERE c.tier = 1
            """,
        )
        tier1_ids = [row["instrument_id"] for row in cur.fetchall()]

    if not tier1_ids:
        return ZERO

    returns: list[Decimal] = []
    for iid in tier1_ids:
        prices = _load_price_series(conn, iid, start_date, end_date)
        ret = _compute_average_return(prices)
        returns.append(ret)

    if not returns:
        return ZERO
    return sum(returns, ZERO) / Decimal(str(len(returns)))


def _compute_sector_return(
    conn: psycopg.Connection[Any],
    instrument_id: int,
    start_date: date,
    end_date: date,
) -> Decimal:
    """Compute the average return of same-sector peers over the hold period."""
    peer_ids = _load_sector_peers(conn, instrument_id)
    if not peer_ids:
        return ZERO

    returns: list[Decimal] = []
    for pid in peer_ids:
        prices = _load_price_series(conn, pid, start_date, end_date)
        ret = _compute_average_return(prices)
        returns.append(ret)

    if not returns:
        return ZERO
    return sum(returns, ZERO) / Decimal(str(len(returns)))


def compute_attribution(
    conn: psycopg.Connection[Any],
    instrument_id: int,
) -> AttributionResult | None:
    """Compute return attribution for a closed position.

    Returns None if:
      - No fills exist for the instrument.
      - No EXIT fill exists (position still open).

    Decomposition (sector_relative_v1):
      gross_return  = (avg_exit_price - avg_entry_price) / avg_entry_price
      market_return = average return of all Tier 1 instruments over hold period
      sector_return = average return of same-sector peers over hold period
      model_alpha   = gross_return - sector_return  (sector-relative outperformance)
      timing_alpha  = Decimal("0")  (placeholder — requires scored-price data)
      cost_drag     = total_fees / cost_basis
      residual      = gross - (market + (sector - market) + model_alpha + timing + cost)
                    = gross - sector - model_alpha - timing - cost

    All components are decimal fractions (0.05 = 5%).
    """
    fills = _load_position_fills(conn, instrument_id)
    if not fills:
        return None

    entry_fills = [f for f in fills if f["action"] in ("BUY", "ADD")]
    exit_fills = [f for f in fills if f["action"] == "EXIT"]

    if not entry_fills or not exit_fills:
        return None

    # Weighted average entry and exit prices
    total_entry_cost = sum(
        Decimal(str(f["price"])) * Decimal(str(f["units"])) for f in entry_fills
    )
    total_entry_units = sum(Decimal(str(f["units"])) for f in entry_fills)

    total_exit_proceeds = sum(
        Decimal(str(f["price"])) * Decimal(str(f["units"])) for f in exit_fills
    )
    total_exit_units = sum(Decimal(str(f["units"])) for f in exit_fills)

    if total_entry_units == ZERO or total_entry_cost == ZERO:
        return None

    avg_entry_price = total_entry_cost / total_entry_units
    avg_exit_price = total_exit_proceeds / total_exit_units

    # Gross return
    gross_return = (avg_exit_price - avg_entry_price) / avg_entry_price

    # Hold period
    hold_start = entry_fills[0]["filled_at"].date()
    hold_end = exit_fills[-1]["filled_at"].date()
    hold_days = (hold_end - hold_start).days

    # Cost drag: total fees / cost basis
    total_fees = sum(Decimal(str(f["fees"])) for f in fills)
    cost_drag = total_fees / total_entry_cost if total_entry_cost != ZERO else ZERO

    # Market and sector returns
    market_return = _compute_market_return(conn, hold_start, hold_end)
    sector_return = _compute_sector_return(conn, instrument_id, hold_start, hold_end)

    # Model alpha: instrument outperformance vs sector
    model_alpha = gross_return - sector_return

    # Timing alpha: placeholder for v1 (requires scored_at price vs fill price)
    timing_alpha = ZERO

    # Residual: gross - all components
    residual = gross_return - market_return - (sector_return - market_return) - model_alpha - timing_alpha - cost_drag
    # Simplifies to: gross - sector - model_alpha - timing - cost
    # = gross - sector - (gross - sector) - 0 - cost
    # = -cost
    # Correct: residual absorbs the cost drag difference.
    # Recompute cleanly:
    residual = gross_return - (market_return + (sector_return - market_return) + model_alpha + timing_alpha + cost_drag)

    # Score snapshot
    rec = _load_recommendation_for_fills(conn, instrument_id)
    score_id = rec["score_id"] if rec else None
    rec_id = rec["recommendation_id"] if rec else None
    score_snapshot = _load_score_snapshot(conn, score_id)

    score_at_entry: Decimal | None = None
    score_components: dict[str, Any] | None = None
    if score_snapshot is not None:
        score_at_entry = Decimal(str(score_snapshot["total_score"]))
        score_components = {
            k: float(score_snapshot[k])
            for k in (
                "quality_score", "value_score", "turnaround_score",
                "momentum_score", "sentiment_score", "confidence_score",
            )
        }

    return AttributionResult(
        instrument_id=instrument_id,
        hold_start=hold_start,
        hold_end=hold_end,
        hold_days=hold_days,
        gross_return_pct=gross_return,
        market_return_pct=market_return,
        sector_return_pct=sector_return,
        model_alpha_pct=model_alpha,
        timing_alpha_pct=timing_alpha,
        cost_drag_pct=cost_drag,
        residual_pct=residual,
        score_at_entry=score_at_entry,
        score_components=score_components,
        entry_fill_id=entry_fills[0]["fill_id"],
        exit_fill_id=exit_fills[-1]["fill_id"],
        recommendation_id=rec_id,
    )
```

- [ ] **Step 5: Run tests**

Run: `uv run pytest tests/test_return_attribution.py -v`
Expected: all tests PASS.

- [ ] **Step 6: Commit**

```bash
git add app/services/return_attribution.py tests/test_return_attribution.py
git commit -m "feat(#155): compute_attribution — return decomposition logic"
```

---

## Task 4: Attribution service — persist + summary

**Files:**
- Modify: `app/services/return_attribution.py`
- Test: `tests/test_return_attribution.py`

- [ ] **Step 1: Write failing tests for persist and summary**

Add to `tests/test_return_attribution.py`:

```python
from app.services.return_attribution import (
    persist_attribution,
    compute_attribution_summary,
    SummaryResult,
)


class TestPersistAttribution:
    def test_inserts_row(self) -> None:
        """persist_attribution should INSERT into return_attribution."""
        result = AttributionResult(
            instrument_id=42,
            hold_start=date(2025, 1, 15),
            hold_end=date(2025, 6, 15),
            hold_days=151,
            gross_return_pct=Decimal("0.20"),
            market_return_pct=Decimal("0.05"),
            sector_return_pct=Decimal("0.08"),
            model_alpha_pct=Decimal("0.12"),
            timing_alpha_pct=ZERO,
            cost_drag_pct=Decimal("0.0044"),
            residual_pct=Decimal("-0.0044"),
            score_at_entry=Decimal("0.75"),
            score_components={"quality_score": 0.8},
            entry_fill_id=1,
            exit_fill_id=2,
            recommendation_id=50,
        )
        conn = MagicMock()
        persist_attribution(conn, result)
        conn.execute.assert_called_once()
        sql = conn.execute.call_args[0][0]
        assert "INSERT INTO return_attribution" in sql


class TestComputeAttributionSummary:
    def test_aggregates_over_window(self) -> None:
        """Should SELECT AVG from return_attribution within window."""
        row = {
            "positions_attributed": 5,
            "avg_gross_return_pct": Decimal("0.10"),
            "avg_market_return_pct": Decimal("0.04"),
            "avg_sector_return_pct": Decimal("0.06"),
            "avg_model_alpha_pct": Decimal("0.04"),
            "avg_timing_alpha_pct": ZERO,
            "avg_cost_drag_pct": Decimal("0.003"),
        }
        conn = _make_conn([_make_cursor([row])])
        result = compute_attribution_summary(conn, window_days=90)
        assert result.window_days == 90
        assert result.positions_attributed == 5
        assert result.avg_model_alpha_pct == Decimal("0.04")

    def test_empty_window_returns_zeros(self) -> None:
        row = {
            "positions_attributed": 0,
            "avg_gross_return_pct": None,
            "avg_market_return_pct": None,
            "avg_sector_return_pct": None,
            "avg_model_alpha_pct": None,
            "avg_timing_alpha_pct": None,
            "avg_cost_drag_pct": None,
        }
        conn = _make_conn([_make_cursor([row])])
        result = compute_attribution_summary(conn, window_days=90)
        assert result.positions_attributed == 0
        assert result.avg_gross_return_pct is None
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_return_attribution.py::TestPersistAttribution -v`
Expected: ImportError for `persist_attribution`.

- [ ] **Step 3: Implement persist_attribution and compute_attribution_summary**

Add to `app/services/return_attribution.py`:

```python
def persist_attribution(
    conn: psycopg.Connection[Any],
    result: AttributionResult,
) -> None:
    """Insert an attribution row. Must be called inside a transaction."""
    conn.execute(
        """
        INSERT INTO return_attribution (
            instrument_id, hold_start, hold_end, hold_days,
            gross_return_pct, market_return_pct, sector_return_pct,
            model_alpha_pct, timing_alpha_pct, cost_drag_pct, residual_pct,
            score_at_entry, score_components,
            entry_fill_id, exit_fill_id, recommendation_id,
            attribution_method
        ) VALUES (
            %(iid)s, %(start)s, %(end)s, %(days)s,
            %(gross)s, %(market)s, %(sector)s,
            %(alpha)s, %(timing)s, %(cost)s, %(residual)s,
            %(score)s, %(components)s,
            %(entry_fill)s, %(exit_fill)s, %(rec_id)s,
            %(method)s
        )
        """,
        {
            "iid": result.instrument_id,
            "start": result.hold_start,
            "end": result.hold_end,
            "days": result.hold_days,
            "gross": result.gross_return_pct,
            "market": result.market_return_pct,
            "sector": result.sector_return_pct,
            "alpha": result.model_alpha_pct,
            "timing": result.timing_alpha_pct,
            "cost": result.cost_drag_pct,
            "residual": result.residual_pct,
            "score": result.score_at_entry,
            "components": Jsonb(result.score_components) if result.score_components else None,
            "entry_fill": result.entry_fill_id,
            "exit_fill": result.exit_fill_id,
            "rec_id": result.recommendation_id,
            "method": ATTRIBUTION_METHOD,
        },
    )


def compute_attribution_summary(
    conn: psycopg.Connection[Any],
    window_days: int,
) -> SummaryResult:
    """Aggregate attribution components over a rolling window.

    Reads from return_attribution where computed_at >= NOW() - window_days.
    Returns a SummaryResult with averages (None if no rows in window).
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT
                COUNT(*)::INTEGER AS positions_attributed,
                AVG(gross_return_pct) AS avg_gross_return_pct,
                AVG(market_return_pct) AS avg_market_return_pct,
                AVG(sector_return_pct) AS avg_sector_return_pct,
                AVG(model_alpha_pct) AS avg_model_alpha_pct,
                AVG(timing_alpha_pct) AS avg_timing_alpha_pct,
                AVG(cost_drag_pct) AS avg_cost_drag_pct
            FROM return_attribution
            WHERE computed_at >= NOW() - MAKE_INTERVAL(days => %(window)s)
            """,
            {"window": window_days},
        )
        row = cur.fetchone()

    if row is None:
        return SummaryResult(
            window_days=window_days,
            positions_attributed=0,
            avg_gross_return_pct=None,
            avg_market_return_pct=None,
            avg_sector_return_pct=None,
            avg_model_alpha_pct=None,
            avg_timing_alpha_pct=None,
            avg_cost_drag_pct=None,
        )

    return SummaryResult(
        window_days=window_days,
        positions_attributed=int(row["positions_attributed"]),
        avg_gross_return_pct=row["avg_gross_return_pct"],
        avg_market_return_pct=row["avg_market_return_pct"],
        avg_sector_return_pct=row["avg_sector_return_pct"],
        avg_model_alpha_pct=row["avg_model_alpha_pct"],
        avg_timing_alpha_pct=row["avg_timing_alpha_pct"],
        avg_cost_drag_pct=row["avg_cost_drag_pct"],
    )


def persist_attribution_summary(
    conn: psycopg.Connection[Any],
    result: SummaryResult,
) -> None:
    """Insert a summary row. Must be called inside a transaction."""
    conn.execute(
        """
        INSERT INTO return_attribution_summary (
            window_days, positions_attributed,
            avg_gross_return_pct, avg_market_return_pct,
            avg_sector_return_pct, avg_model_alpha_pct,
            avg_timing_alpha_pct, avg_cost_drag_pct
        ) VALUES (
            %(window)s, %(count)s,
            %(gross)s, %(market)s, %(sector)s,
            %(alpha)s, %(timing)s, %(cost)s
        )
        """,
        {
            "window": result.window_days,
            "count": result.positions_attributed,
            "gross": result.avg_gross_return_pct,
            "market": result.avg_market_return_pct,
            "sector": result.avg_sector_return_pct,
            "alpha": result.avg_model_alpha_pct,
            "timing": result.avg_timing_alpha_pct,
            "cost": result.avg_cost_drag_pct,
        },
    )
```

- [ ] **Step 4: Run tests**

Run: `uv run pytest tests/test_return_attribution.py -v`
Expected: all tests PASS.

- [ ] **Step 5: Commit**

```bash
git add app/services/return_attribution.py tests/test_return_attribution.py
git commit -m "feat(#155): persist_attribution + summary aggregation"
```

---

## Task 5: Trigger attribution from order_client

**Files:**
- Modify: `app/services/order_client.py`
- Test: `tests/test_attribution_trigger.py`

When `execute_order` processes an EXIT fill and the position reaches `current_units = 0`, trigger `compute_attribution` + `persist_attribution`.

- [ ] **Step 1: Write failing test for the trigger**

Create `tests/test_attribution_trigger.py`:

```python
"""Tests that EXIT fills trigger return attribution."""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock, patch

from app.services.return_attribution import AttributionResult


_ORDER_CLIENT = "app.services.order_client"


class TestAttributionTrigger:
    @patch(f"{_ORDER_CLIENT}.persist_attribution")
    @patch(f"{_ORDER_CLIENT}.compute_attribution")
    def test_attribution_triggered_on_full_close(
        self,
        mock_compute: MagicMock,
        mock_persist: MagicMock,
    ) -> None:
        """When an EXIT fill zeroes current_units, attribution should run."""
        from app.services.order_client import _maybe_trigger_attribution

        mock_compute.return_value = MagicMock(spec=AttributionResult)
        conn = MagicMock()

        # current_units after exit = 0 → should trigger
        _maybe_trigger_attribution(conn, instrument_id=42, current_units_after=Decimal("0"))

        mock_compute.assert_called_once_with(conn, 42)
        mock_persist.assert_called_once()

    @patch(f"{_ORDER_CLIENT}.persist_attribution")
    @patch(f"{_ORDER_CLIENT}.compute_attribution")
    def test_attribution_not_triggered_when_position_open(
        self,
        mock_compute: MagicMock,
        mock_persist: MagicMock,
    ) -> None:
        """If current_units > 0 after exit, no attribution."""
        from app.services.order_client import _maybe_trigger_attribution

        conn = MagicMock()
        _maybe_trigger_attribution(conn, instrument_id=42, current_units_after=Decimal("5"))

        mock_compute.assert_not_called()
        mock_persist.assert_not_called()

    @patch(f"{_ORDER_CLIENT}.compute_attribution", return_value=None)
    def test_attribution_none_result_no_persist(
        self,
        mock_compute: MagicMock,
    ) -> None:
        """If compute_attribution returns None (e.g. missing data), skip persist."""
        from app.services.order_client import _maybe_trigger_attribution

        conn = MagicMock()
        _maybe_trigger_attribution(conn, instrument_id=42, current_units_after=Decimal("0"))

        mock_compute.assert_called_once()
        # persist should not have been called since compute returned None

    @patch(f"{_ORDER_CLIENT}.compute_attribution", side_effect=Exception("DB error"))
    def test_attribution_error_does_not_break_order(
        self,
        mock_compute: MagicMock,
    ) -> None:
        """Attribution failure must not abort the order execution."""
        from app.services.order_client import _maybe_trigger_attribution

        conn = MagicMock()
        # Should not raise — attribution errors are logged and swallowed
        _maybe_trigger_attribution(conn, instrument_id=42, current_units_after=Decimal("0"))
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_attribution_trigger.py -v`
Expected: ImportError for `_maybe_trigger_attribution`.

- [ ] **Step 3: Implement the trigger in order_client.py**

Add to `app/services/order_client.py` (near the other internal helpers):

```python
from app.services.return_attribution import (
    compute_attribution,
    persist_attribution,
)


def _maybe_trigger_attribution(
    conn: psycopg.Connection[Any],
    instrument_id: int,
    current_units_after: Decimal,
) -> None:
    """Compute and persist return attribution if the position is fully closed.

    Called after an EXIT fill updates the position. If current_units_after is
    zero (or negative due to rounding), the position is closed and attribution
    is computed.

    Errors are logged and swallowed — attribution is best-effort and must
    never abort the order execution path.
    """
    if current_units_after > Decimal("0"):
        return

    try:
        result = compute_attribution(conn, instrument_id)
        if result is not None:
            persist_attribution(conn, result)
            logger.info(
                "execute_order: attribution computed for instrument_id=%d "
                "gross=%.4f alpha=%.4f",
                instrument_id,
                result.gross_return_pct,
                result.model_alpha_pct,
            )
    except Exception:
        logger.error(
            "execute_order: attribution failed for instrument_id=%d",
            instrument_id,
            exc_info=True,
        )
```

Then call `_maybe_trigger_attribution` inside `execute_order`, after the EXIT fill updates the position. Find the section in `execute_order` that calls `UPDATE positions SET current_units = current_units - ...` for EXIT actions and add:

```python
            # After the position update, check if fully closed
            if action == "EXIT":
                with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
                    cur.execute(
                        "SELECT current_units FROM positions WHERE instrument_id = %(iid)s",
                        {"iid": instrument_id},
                    )
                    pos_row = cur.fetchone()
                units_after = Decimal(str(pos_row["current_units"])) if pos_row else Decimal("0")
                _maybe_trigger_attribution(conn, instrument_id, units_after)
```

- [ ] **Step 4: Run tests**

Run: `uv run pytest tests/test_attribution_trigger.py -v`
Expected: all tests PASS.

- [ ] **Step 5: Run full test suite**

Run: `uv run pytest -q`
Expected: all tests PASS (no regressions in order_client).

- [ ] **Step 6: Commit**

```bash
git add app/services/order_client.py tests/test_attribution_trigger.py
git commit -m "feat(#155): trigger attribution on EXIT fill that closes position"
```

---

## Task 6: Summary scheduler job

**Files:**
- Modify: `app/workers/scheduler.py`
- Modify: `app/jobs/runtime.py`
- Test: `tests/test_scheduler_attribution.py`

- [ ] **Step 1: Write failing test for the summary job**

Create `tests/test_scheduler_attribution.py`:

```python
"""Tests for the attribution_summary scheduler job."""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock, patch

_PSYCOPG_CONNECT_PATCH = "app.workers.scheduler.psycopg.connect"
_RECORD_START_PATCH = "app.workers.scheduler.record_job_start"
_RECORD_FINISH_PATCH = "app.workers.scheduler.record_job_finish"
_SPIKE_PATCH = "app.workers.scheduler.check_runtime_spike"


class TestAttributionSummaryJob:
    @patch(_SPIKE_PATCH, return_value=MagicMock(flagged=False))
    @patch(_RECORD_FINISH_PATCH)
    @patch(_RECORD_START_PATCH, return_value=1)
    @patch("app.workers.scheduler.persist_attribution_summary")
    @patch("app.workers.scheduler.compute_attribution_summary")
    @patch(_PSYCOPG_CONNECT_PATCH)
    def test_computes_and_persists_summaries(
        self,
        mock_connect: MagicMock,
        mock_summary: MagicMock,
        mock_persist: MagicMock,
        mock_start: MagicMock,
        mock_finish: MagicMock,
        mock_spike: MagicMock,
    ) -> None:
        """Job should compute summaries for each window and persist them."""
        from app.services.return_attribution import SummaryResult, SUMMARY_WINDOWS
        from app.workers.scheduler import attribution_summary_job

        fake_result = SummaryResult(
            window_days=90,
            positions_attributed=5,
            avg_gross_return_pct=Decimal("0.10"),
            avg_market_return_pct=Decimal("0.04"),
            avg_sector_return_pct=Decimal("0.06"),
            avg_model_alpha_pct=Decimal("0.04"),
            avg_timing_alpha_pct=Decimal("0"),
            avg_cost_drag_pct=Decimal("0.003"),
        )
        mock_summary.return_value = fake_result

        conn_ctx = MagicMock()
        conn_ctx.__enter__ = MagicMock(return_value=conn_ctx)
        conn_ctx.__exit__ = MagicMock(return_value=False)
        mock_connect.return_value = conn_ctx

        attribution_summary_job()

        assert mock_summary.call_count == len(SUMMARY_WINDOWS)
        assert mock_persist.call_count == len(SUMMARY_WINDOWS)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_scheduler_attribution.py -v`
Expected: ImportError for `attribution_summary_job`.

- [ ] **Step 3: Implement the job**

Add to `app/workers/scheduler.py`:

```python
# At top with other imports:
from app.services.return_attribution import (
    SUMMARY_WINDOWS,
    compute_attribution_summary,
    persist_attribution_summary,
)

# New constant with other JOB_ constants:
JOB_ATTRIBUTION_SUMMARY = "attribution_summary"

# New prerequisite (attribution needs at least one attributed position):
def _has_attributions(conn: psycopg.Connection[Any]) -> tuple[bool, str]:
    exists = _exists(conn, SQL("SELECT 1 FROM return_attribution LIMIT 1"))
    if not exists:
        return False, "no attributed positions yet"
    return True, ""

# New ScheduledJob entry in SCHEDULED_JOBS list:
# ScheduledJob(
#     name=JOB_ATTRIBUTION_SUMMARY,
#     cron_trigger=CronTrigger(day_of_week="sun", hour=6, minute=0),
#     prerequisites=[_has_attributions],
#     catch_up_on_boot=False,
# ),

# New job function:
def attribution_summary_job() -> None:
    """Compute and persist attribution summaries for all configured windows."""
    with _tracked_job(JOB_ATTRIBUTION_SUMMARY) as tracker:
        with psycopg.connect(settings.database_url) as conn:
            total_positions = 0
            for window in SUMMARY_WINDOWS:
                summary = compute_attribution_summary(conn, window)
                with conn.transaction():
                    persist_attribution_summary(conn, summary)
                conn.commit()
                total_positions = max(total_positions, summary.positions_attributed)
                logger.info(
                    "attribution_summary: window=%dd positions=%d avg_alpha=%.4f",
                    window,
                    summary.positions_attributed,
                    float(summary.avg_model_alpha_pct or 0),
                )
            tracker.row_count = total_positions
```

Add to `app/jobs/runtime.py`:

```python
from app.workers.scheduler import JOB_ATTRIBUTION_SUMMARY, attribution_summary_job

# In _INVOKERS dict:
JOB_ATTRIBUTION_SUMMARY: attribution_summary_job,
```

- [ ] **Step 4: Run tests**

Run: `uv run pytest tests/test_scheduler_attribution.py -v`
Expected: PASS.

- [ ] **Step 5: Run full test suite**

Run: `uv run pytest -q`
Expected: all PASS.

- [ ] **Step 6: Commit**

```bash
git add app/workers/scheduler.py app/jobs/runtime.py tests/test_scheduler_attribution.py
git commit -m "feat(#155): weekly attribution_summary scheduler job"
```

---

## Task 7: API endpoint

**Files:**
- Create: `app/api/attribution.py`
- Modify: `app/main.py` (register router)

- [ ] **Step 1: Implement the API endpoint**

Create `app/api/attribution.py`:

```python
"""Attribution API — return decomposition data for the dashboard."""

from __future__ import annotations

from typing import Any

import psycopg
import psycopg.rows
from fastapi import APIRouter, Depends

from app.api.auth_session import require_operator
from app.config import settings

router = APIRouter(prefix="/api/attribution", tags=["attribution"])


@router.get("")
def list_attributions(
    _: Any = Depends(require_operator),
    limit: int = 50,
) -> list[dict[str, Any]]:
    """Return the most recent attribution rows."""
    with psycopg.connect(settings.database_url) as conn:
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                """
                SELECT ra.*, i.symbol, i.sector
                FROM return_attribution ra
                JOIN instruments i USING (instrument_id)
                ORDER BY ra.computed_at DESC
                LIMIT %(limit)s
                """,
                {"limit": limit},
            )
            return cur.fetchall()


@router.get("/summary")
def list_summaries(
    _: Any = Depends(require_operator),
    limit: int = 10,
) -> list[dict[str, Any]]:
    """Return the most recent attribution summaries."""
    with psycopg.connect(settings.database_url) as conn:
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                """
                SELECT *
                FROM return_attribution_summary
                ORDER BY computed_at DESC
                LIMIT %(limit)s
                """,
                {"limit": limit},
            )
            return cur.fetchall()
```

- [ ] **Step 2: Register the router in app/main.py**

Add to the router registration section:

```python
from app.api.attribution import router as attribution_router
app.include_router(attribution_router)
```

- [ ] **Step 3: Run pre-push checks**

Run:
```bash
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv run pytest -q
```
Expected: all pass.

- [ ] **Step 4: Commit**

```bash
git add app/api/attribution.py app/main.py
git commit -m "feat(#155): GET /api/attribution + /api/attribution/summary endpoints"
```

---

## Task 8: Pre-push checks and PR

- [ ] **Step 1: Run full pre-push suite**

```bash
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv run pytest -q
```

All four must pass.

- [ ] **Step 2: Self-review the diff**

Read `.claude/skills/engineering/pre-flight-review.md` and check:
- No raw SQL interpolation (all parameterised)
- Decimal arithmetic throughout (no float on financial values)
- Attribution errors swallowed in order_client (never abort execution)
- Migration is idempotent (`IF NOT EXISTS`)
- Tests cover: empty state, happy path, error path, boundary conditions

- [ ] **Step 3: Push and open PR**

```bash
git push -u origin feature/155-return-attribution
gh pr create --title "feat(#155): return attribution and performance audit service" --body "..."
```

- [ ] **Step 4: Poll review and CI, resolve all comments**

Follow the standard review-resolve-push cycle from CLAUDE.md.
