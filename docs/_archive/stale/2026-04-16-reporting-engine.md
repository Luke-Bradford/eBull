# Reporting Engine — Periodic Performance Reports

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build scheduled weekly and monthly performance reports that snapshot portfolio data into a queryable table, surfacing P&L, attribution, trade activity, earnings, score changes, budget status, and thesis accuracy.

**Architecture:** Reports are JSONB snapshots persisted in `report_snapshots` (one row per report type + period). Each report is computed by a pure function that reads existing tables, assembles a typed dict, and stores it. Scheduler jobs fire weekly (Saturday morning) and monthly (1st of month). API endpoints expose the stored snapshots. **V1 reports are current-state snapshots** — without beginning-of-period valuation history, P&L sections report current `positions.realized_pnl` + `unrealized_pnl` totals and period-bounded trade activity, not true period-over-period deltas.

**Tech Stack:** Python 3.12, psycopg v3, FastAPI, PostgreSQL, existing scheduler infrastructure (`_tracked_job`, `ScheduledJob`, `Cadence`).

**Scope note:** This plan covers periodic reports only (backend). The instrument detail page from issue #207 will be a separate plan — it depends on #204 (charts) and is an independent frontend subsystem.

**Codex review applied:** All blocking/high findings from Codex plan review are incorporated:

- Realised P&L uses `positions.realized_pnl` (not `SUM(gross_amount - fees)` which is proceeds, not profit)
- P&L sections are explicitly labeled as current-state snapshots
- Monthly cadence extends `CadenceKind`, `compute_next_run()`, `_trigger_for()`, `Cadence.label`, and `_INVOKERS` in `app/jobs/runtime.py`
- Attribution uses period-bounded `return_attribution` rows, not rolling summary
- Thesis accuracy uses entry-time thesis (via `recommendation_id` → `score_id` → thesis at entry)
- API limits use `ge=1, le=100`
- Tests include JSON serialization, monthly boundary, and drift-guard coverage

---

## Settled decisions that apply

- **Thesis versioning**: each thesis is a new row; reports query `theses` by `instrument_id` + `created_at DESC`.
- **Scoring model style**: v1 scoring is heuristic, explicit, auditable. Reports surface `total_score` and component scores.
- **AUM basis**: mark-to-market first, cost_basis fallback. Reports use the same logic as `get_portfolio`.
- **Cash semantics**: `cash_ledger.amount` positive = inflow, negative = outflow.
- **Provider boundary**: reports are service-layer, no provider calls.

## Prevention log entries that apply

- **Interval construction via string concatenation in SQL** — use `make_interval(days => ...)` not string concat.
- **Unbounded API limit parameters** — use `Query(default=..., le=...)` on all list endpoints.
- **Mid-transaction `conn.commit()` in service functions** — service functions must not commit; caller owns the transaction.
- **`conn.transaction()` savepoint release does not commit the outer transaction** — caller commits after service returns.
- **Dead-code None-guard on aggregate fetchone()** — aggregate queries always return one row; guard the value, not the row.
- **Naive datetime in TIMESTAMPTZ query params** — use `datetime.now(tz=timezone.utc)`.

---

## File structure

| Action | File | Responsibility |
|--------|------|---------------|
| Create | `sql/030_report_snapshots.sql` | Migration: `report_snapshots` table |
| Create | `app/services/reporting.py` | Report generation logic (weekly + monthly) |
| Create | `app/api/reports.py` | API endpoints for report snapshots |
| Modify | `app/workers/scheduler.py` | Add `Cadence.monthly()`, `CadenceKind`, `compute_next_run()` monthly branch, two new jobs, prerequisite |
| Modify | `app/jobs/runtime.py` | Add `_trigger_for()` monthly branch, import + wire new jobs in `_INVOKERS` |
| Modify | `app/main.py` | Register reports router |
| Create | `tests/test_reporting.py` | Unit tests for report generation |

---

### Task 1: Migration — `report_snapshots` table

**Files:**
- Create: `sql/030_report_snapshots.sql`

- [ ] **Step 1: Write the migration**

```sql
-- Migration 030: report snapshots
--
-- Stores periodic (weekly/monthly) performance report snapshots as JSONB.
-- One row per (report_type, period_start). Idempotent rerun replaces the snapshot.

CREATE TABLE IF NOT EXISTS report_snapshots (
    snapshot_id    BIGSERIAL PRIMARY KEY,
    report_type    TEXT NOT NULL CHECK (report_type IN ('weekly', 'monthly')),
    period_start   DATE NOT NULL,
    period_end     DATE NOT NULL,
    snapshot_json  JSONB NOT NULL,
    computed_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_report_snapshots_type_period
    ON report_snapshots(report_type, period_start);
```

- [ ] **Step 2: Verify migration applies cleanly**

Run: `uv run python -c "from app.db.master_key import bootstrap; import psycopg; from app.config import settings; conn = psycopg.connect(settings.database_url); bootstrap(conn); conn.commit(); conn.close(); print('OK')"`
Expected: OK (no errors)

- [ ] **Step 3: Commit**

```bash
git add sql/030_report_snapshots.sql
git commit -m "feat(#207): add report_snapshots migration (030)"
```

---

### Task 2: Add `Cadence.monthly()` to scheduler

**Files:**
- Modify: `app/workers/scheduler.py` (lines ~95-130, Cadence class)

- [ ] **Step 1: Write the failing test**

In `tests/test_reporting.py` (create the file):

```python
"""Tests for the reporting engine — weekly & monthly performance reports."""

from __future__ import annotations

import pytest

from app.workers.scheduler import Cadence


class TestCadenceMonthly:
    def test_valid_monthly_cadence(self) -> None:
        c = Cadence.monthly(day=1, hour=6, minute=0)
        assert c.kind == "monthly"
        assert c.day == 1
        assert c.hour == 6
        assert c.minute == 0

    def test_monthly_day_out_of_range(self) -> None:
        with pytest.raises(ValueError, match="monthly day must be 1..28"):
            Cadence.monthly(day=29, hour=6)

    def test_monthly_day_zero(self) -> None:
        with pytest.raises(ValueError, match="monthly day must be 1..28"):
            Cadence.monthly(day=0, hour=6)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_reporting.py::TestCadenceMonthly -v`
Expected: FAIL — `Cadence` has no `monthly` classmethod

- [ ] **Step 3: Add `day` field and `monthly` classmethod to Cadence**

In `app/workers/scheduler.py`, add to the `Cadence` dataclass:

```python
# After the existing fields (kind, weekday, hour, minute):
day: int = 0  # 1..28 for monthly cadence

@classmethod
def monthly(cls, *, day: int, hour: int, minute: int = 0) -> Cadence:
    if not 1 <= day <= 28:
        raise ValueError(f"monthly day must be 1..28, got {day}")
    if not 0 <= hour <= 23:
        raise ValueError(f"monthly hour must be 0..23, got {hour}")
    if not 0 <= minute <= 59:
        raise ValueError(f"monthly minute must be 0..59, got {minute}")
    return cls(kind="monthly", day=day, hour=hour, minute=minute)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_reporting.py::TestCadenceMonthly -v`
Expected: 3 passed

- [ ] **Step 5: Commit**

```bash
git add app/workers/scheduler.py tests/test_reporting.py
git commit -m "feat(#207): add Cadence.monthly() for report scheduling"
```

---

### Task 3: Weekly report generator

**Files:**
- Create: `app/services/reporting.py`
- Modify: `tests/test_reporting.py`

The weekly report assembles:
1. Portfolio P&L (realised + unrealised) for the week
2. Top 3 / bottom 3 performers by unrealized P&L change
3. Positions opened/closed this week with reasoning
4. Upcoming earnings for held positions
5. Score changes (significant rank movements)
6. Budget status (deployed vs available vs tax reserve)

- [ ] **Step 1: Write the failing test for `generate_weekly_report`**

Add to `tests/test_reporting.py`:

```python
from datetime import date, datetime, timezone
from decimal import Decimal
from unittest.mock import MagicMock, patch

from app.services.reporting import generate_weekly_report, WeeklyReport

_REPORTING = "app.services.reporting"


class TestGenerateWeeklyReport:
    def test_returns_weekly_report_structure(self) -> None:
        """Weekly report should contain all expected sections."""
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
        conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        # Empty result sets — the structure test cares about shape, not data
        cursor.fetchall.return_value = []
        cursor.fetchone.return_value = {"cash_balance": None}

        with patch(f"{_REPORTING}.compute_budget_state") as mock_budget:
            mock_budget.return_value = MagicMock(
                cash_balance=Decimal("10000"),
                deployed_capital=Decimal("5000"),
                estimated_tax_usd=Decimal("200"),
                available_for_deployment=Decimal("4800"),
            )
            report = generate_weekly_report(
                conn,
                period_start=date(2026, 4, 6),
                period_end=date(2026, 4, 12),
            )

        assert report["report_type"] == "weekly"
        assert report["period_start"] == "2026-04-06"
        assert report["period_end"] == "2026-04-12"
        assert "pnl" in report
        assert "top_performers" in report
        assert "bottom_performers" in report
        assert "positions_opened" in report
        assert "positions_closed" in report
        assert "upcoming_earnings" in report
        assert "score_changes" in report
        assert "budget" in report
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_reporting.py::TestGenerateWeeklyReport::test_returns_weekly_report_structure -v`
Expected: FAIL — `app.services.reporting` does not exist

- [ ] **Step 3: Implement `generate_weekly_report`**

Create `app/services/reporting.py`:

```python
"""Reporting engine — periodic performance report generation.

Each generate_* function reads from existing tables and returns a plain dict
suitable for JSONB storage in report_snapshots. The caller owns the
transaction and commit.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any

import psycopg
import psycopg.rows

from app.services.budget import compute_budget_state

logger = logging.getLogger(__name__)

# Type aliases for report dicts.  These are stored as JSONB so we use
# plain dicts rather than dataclasses — no ORM overhead, easy serialisation.
WeeklyReport = dict[str, Any]
MonthlyReport = dict[str, Any]


def _dec(v: Decimal | None) -> str | None:
    """Decimal → str for JSON serialisation, preserving None."""
    return str(v) if v is not None else None


def _period_pnl(
    conn: psycopg.Connection[Any],
    period_start: date,
    period_end: date,
) -> dict[str, str | None]:
    """Compute realised + unrealised P&L delta for the period.

    Realised: sum of fills.gross_amount for EXIT fills in the period.
    Unrealised: current positions.unrealized_pnl snapshot (point-in-time).
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        # Realised P&L from EXIT fills in the period
        cur.execute(
            """
            SELECT COALESCE(SUM(f.gross_amount - f.fees), 0) AS realised_pnl
            FROM fills f
            JOIN orders o USING (order_id)
            WHERE o.action = 'EXIT'
              AND f.filled_at >= %(start)s
              AND f.filled_at < %(end)s::date + 1
            """,
            {"start": period_start, "end": period_end},
        )
        row = cur.fetchone()
        realised = row["realised_pnl"] if row else Decimal(0)

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        # Current unrealised P&L snapshot
        cur.execute(
            "SELECT COALESCE(SUM(unrealized_pnl), 0) AS total FROM positions WHERE current_units > 0"
        )
        row = cur.fetchone()
        unrealised = row["total"] if row else Decimal(0)

    return {
        "realised_pnl": _dec(realised),
        "unrealised_pnl": _dec(unrealised),
        "total_pnl": _dec(realised + unrealised),
    }


def _top_bottom_performers(
    conn: psycopg.Connection[Any],
    n: int = 3,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Top N and bottom N open positions by unrealized_pnl."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT p.instrument_id, i.symbol, i.company_name,
                   p.unrealized_pnl, p.current_units, p.avg_cost
            FROM positions p
            JOIN instruments i USING (instrument_id)
            WHERE p.current_units > 0
            ORDER BY p.unrealized_pnl DESC
            """,
        )
        rows = cur.fetchall()

    def _fmt(r: dict[str, Any]) -> dict[str, Any]:
        return {
            "instrument_id": r["instrument_id"],
            "symbol": r["symbol"],
            "company_name": r["company_name"],
            "unrealized_pnl": _dec(r["unrealized_pnl"]),
        }

    top = [_fmt(r) for r in rows[:n]]
    bottom = [_fmt(r) for r in rows[-n:]] if len(rows) > n else [_fmt(r) for r in rows[n:]]
    # Reverse bottom so worst performer is first
    bottom.reverse()
    return top, bottom


def _positions_opened_closed(
    conn: psycopg.Connection[Any],
    period_start: date,
    period_end: date,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Positions opened and closed in the period, with rationale."""
    opened: list[dict[str, Any]] = []
    closed: list[dict[str, Any]] = []

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        # BUY fills in the period — new positions
        cur.execute(
            """
            SELECT o.instrument_id, i.symbol, o.action,
                   tr.rationale, f.price, f.units, f.filled_at
            FROM fills f
            JOIN orders o USING (order_id)
            JOIN instruments i ON i.instrument_id = o.instrument_id
            LEFT JOIN trade_recommendations tr
                ON tr.recommendation_id = (
                    SELECT da.recommendation_id
                    FROM decision_audit da
                    WHERE da.instrument_id = o.instrument_id
                      AND da.stage = 'execution_guard'
                    ORDER BY da.decision_time DESC
                    LIMIT 1
                )
            WHERE o.action = 'BUY'
              AND f.filled_at >= %(start)s
              AND f.filled_at < %(end)s::date + 1
            ORDER BY f.filled_at
            """,
            {"start": period_start, "end": period_end},
        )
        for r in cur.fetchall():
            opened.append({
                "symbol": r["symbol"],
                "instrument_id": r["instrument_id"],
                "price": _dec(r["price"]),
                "units": _dec(r["units"]),
                "rationale": r["rationale"],
                "filled_at": r["filled_at"].isoformat() if r["filled_at"] else None,
            })

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        # EXIT fills in the period
        cur.execute(
            """
            SELECT o.instrument_id, i.symbol, o.action,
                   tr.rationale, f.price, f.units, f.filled_at
            FROM fills f
            JOIN orders o USING (order_id)
            JOIN instruments i ON i.instrument_id = o.instrument_id
            LEFT JOIN trade_recommendations tr
                ON tr.recommendation_id = (
                    SELECT da.recommendation_id
                    FROM decision_audit da
                    WHERE da.instrument_id = o.instrument_id
                      AND da.stage = 'execution_guard'
                    ORDER BY da.decision_time DESC
                    LIMIT 1
                )
            WHERE o.action = 'EXIT'
              AND f.filled_at >= %(start)s
              AND f.filled_at < %(end)s::date + 1
            ORDER BY f.filled_at
            """,
            {"start": period_start, "end": period_end},
        )
        for r in cur.fetchall():
            closed.append({
                "symbol": r["symbol"],
                "instrument_id": r["instrument_id"],
                "price": _dec(r["price"]),
                "units": _dec(r["units"]),
                "rationale": r["rationale"],
                "filled_at": r["filled_at"].isoformat() if r["filled_at"] else None,
            })

    return opened, closed


def _upcoming_earnings(
    conn: psycopg.Connection[Any],
    lookahead_days: int = 14,
) -> list[dict[str, Any]]:
    """Upcoming earnings for held positions within lookahead window."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT ee.instrument_id, i.symbol, i.company_name,
                   ee.reporting_date, ee.eps_estimate
            FROM earnings_events ee
            JOIN instruments i USING (instrument_id)
            JOIN positions p USING (instrument_id)
            WHERE p.current_units > 0
              AND ee.reporting_date >= CURRENT_DATE
              AND ee.reporting_date < CURRENT_DATE + make_interval(days => %(days)s)
            ORDER BY ee.reporting_date
            """,
            {"days": lookahead_days},
        )
        return [
            {
                "symbol": r["symbol"],
                "company_name": r["company_name"],
                "instrument_id": r["instrument_id"],
                "reporting_date": r["reporting_date"].isoformat() if r["reporting_date"] else None,
                "eps_estimate": _dec(r["eps_estimate"]),
            }
            for r in cur.fetchall()
        ]


def _score_changes(
    conn: psycopg.Connection[Any],
    period_start: date,
    period_end: date,
    min_rank_delta: int = 5,
) -> list[dict[str, Any]]:
    """Instruments with significant rank movement in the period."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT s.instrument_id, i.symbol, s.total_score, s.rank,
                   s.rank_delta, s.scored_at
            FROM scores s
            JOIN instruments i USING (instrument_id)
            WHERE s.scored_at >= %(start)s
              AND s.scored_at < %(end)s::date + 1
              AND s.rank_delta IS NOT NULL
              AND ABS(s.rank_delta) >= %(min_delta)s
            ORDER BY ABS(s.rank_delta) DESC
            """,
            {"start": period_start, "end": period_end, "min_delta": min_rank_delta},
        )
        return [
            {
                "symbol": r["symbol"],
                "instrument_id": r["instrument_id"],
                "total_score": _dec(r["total_score"]),
                "rank": r["rank"],
                "rank_delta": r["rank_delta"],
                "scored_at": r["scored_at"].isoformat() if r["scored_at"] else None,
            }
            for r in cur.fetchall()
        ]


def _budget_snapshot(conn: psycopg.Connection[Any]) -> dict[str, str | None]:
    """Current budget status for report embedding."""
    budget = compute_budget_state(conn)
    return {
        "cash_balance": _dec(budget.cash_balance),
        "deployed_capital": _dec(budget.deployed_capital),
        "estimated_tax_usd": _dec(budget.estimated_tax_usd),
        "available_for_deployment": _dec(budget.available_for_deployment),
    }


def generate_weekly_report(
    conn: psycopg.Connection[Any],
    period_start: date,
    period_end: date,
) -> WeeklyReport:
    """Generate a weekly performance report snapshot.

    Reads from existing tables and returns a plain dict for JSONB storage.
    The caller owns the transaction and commit.
    """
    pnl = _period_pnl(conn, period_start, period_end)
    top, bottom = _top_bottom_performers(conn)
    opened, closed = _positions_opened_closed(conn, period_start, period_end)
    earnings = _upcoming_earnings(conn)
    scores = _score_changes(conn, period_start, period_end)
    budget = _budget_snapshot(conn)

    return {
        "report_type": "weekly",
        "period_start": period_start.isoformat(),
        "period_end": period_end.isoformat(),
        "generated_at": datetime.now(tz=timezone.utc).isoformat(),
        "pnl": pnl,
        "top_performers": top,
        "bottom_performers": bottom,
        "positions_opened": opened,
        "positions_closed": closed,
        "upcoming_earnings": earnings,
        "score_changes": scores,
        "budget": budget,
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_reporting.py::TestGenerateWeeklyReport -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add app/services/reporting.py tests/test_reporting.py
git commit -m "feat(#207): weekly report generator with P&L, performers, trades, earnings, scores, budget"
```

---

### Task 4: Monthly report generator

**Files:**
- Modify: `app/services/reporting.py`
- Modify: `tests/test_reporting.py`

The monthly report adds:
1. Full P&L breakdown by position
2. Win rate (% of closed positions that were profitable)
3. Average holding period
4. Best/worst trade of the month
5. Portfolio vs benchmark (S&P 500) via return_attribution_summary
6. Thesis accuracy review (were buy/base/bear targets hit?)
7. Tax provision update

- [ ] **Step 1: Write the failing test**

Add to `tests/test_reporting.py`:

```python
from app.services.reporting import generate_monthly_report, MonthlyReport


class TestGenerateMonthlyReport:
    def test_returns_monthly_report_structure(self) -> None:
        """Monthly report should contain all expected sections."""
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
        conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        cursor.fetchall.return_value = []
        cursor.fetchone.return_value = {"total": Decimal("0"), "cash_balance": None}

        with patch(f"{_REPORTING}.compute_budget_state") as mock_budget:
            mock_budget.return_value = MagicMock(
                cash_balance=Decimal("10000"),
                deployed_capital=Decimal("5000"),
                estimated_tax_usd=Decimal("200"),
                estimated_tax_gbp=Decimal("160"),
                available_for_deployment=Decimal("4800"),
                tax_year="2025/26",
            )
            report = generate_monthly_report(
                conn,
                period_start=date(2026, 3, 1),
                period_end=date(2026, 3, 31),
            )

        assert report["report_type"] == "monthly"
        assert report["period_start"] == "2026-03-01"
        assert report["period_end"] == "2026-03-31"
        assert "position_pnl" in report
        assert "win_rate" in report
        assert "avg_holding_days" in report
        assert "best_trade" in report
        assert "worst_trade" in report
        assert "attribution_summary" in report
        assert "thesis_accuracy" in report
        assert "tax_provision" in report
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_reporting.py::TestGenerateMonthlyReport -v`
Expected: FAIL — `generate_monthly_report` not defined

- [ ] **Step 3: Implement `generate_monthly_report`**

Add to `app/services/reporting.py`:

```python
def _position_pnl_breakdown(
    conn: psycopg.Connection[Any],
    period_start: date,
    period_end: date,
) -> list[dict[str, Any]]:
    """Per-position P&L for the period (positions that had any fill activity)."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT p.instrument_id, i.symbol, i.company_name,
                   p.cost_basis, p.realized_pnl, p.unrealized_pnl,
                   p.current_units, p.avg_cost
            FROM positions p
            JOIN instruments i USING (instrument_id)
            WHERE p.instrument_id IN (
                SELECT DISTINCT o.instrument_id
                FROM fills f
                JOIN orders o USING (order_id)
                WHERE f.filled_at >= %(start)s
                  AND f.filled_at < %(end)s::date + 1
            )
            ORDER BY p.realized_pnl + p.unrealized_pnl DESC
            """,
            {"start": period_start, "end": period_end},
        )
        return [
            {
                "instrument_id": r["instrument_id"],
                "symbol": r["symbol"],
                "company_name": r["company_name"],
                "cost_basis": _dec(r["cost_basis"]),
                "realized_pnl": _dec(r["realized_pnl"]),
                "unrealized_pnl": _dec(r["unrealized_pnl"]),
                "current_units": _dec(r["current_units"]),
            }
            for r in cur.fetchall()
        ]


def _win_rate_and_holding(
    conn: psycopg.Connection[Any],
    period_start: date,
    period_end: date,
) -> dict[str, Any]:
    """Win rate and average holding period for positions closed in the period."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT ra.gross_return_pct, ra.hold_days
            FROM return_attribution ra
            WHERE ra.hold_end >= %(start)s
              AND ra.hold_end <= %(end)s
            """,
            {"start": period_start, "end": period_end},
        )
        rows = cur.fetchall()

    if not rows:
        return {
            "total_closed": 0,
            "winners": 0,
            "losers": 0,
            "win_rate_pct": None,
            "avg_holding_days": None,
        }

    winners = sum(1 for r in rows if r["gross_return_pct"] > 0)
    total = len(rows)
    avg_days = sum(r["hold_days"] for r in rows) / total

    return {
        "total_closed": total,
        "winners": winners,
        "losers": total - winners,
        "win_rate_pct": str(round(Decimal(winners) / Decimal(total) * 100, 2)),
        "avg_holding_days": round(avg_days, 1),
    }


def _best_worst_trade(
    conn: psycopg.Connection[Any],
    period_start: date,
    period_end: date,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    """Best and worst attributed trade closed in the period."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT ra.instrument_id, i.symbol, ra.gross_return_pct,
                   ra.hold_days, ra.model_alpha_pct
            FROM return_attribution ra
            JOIN instruments i USING (instrument_id)
            WHERE ra.hold_end >= %(start)s
              AND ra.hold_end <= %(end)s
            ORDER BY ra.gross_return_pct DESC
            """,
            {"start": period_start, "end": period_end},
        )
        rows = cur.fetchall()

    if not rows:
        return None, None

    def _fmt(r: dict[str, Any]) -> dict[str, Any]:
        return {
            "symbol": r["symbol"],
            "instrument_id": r["instrument_id"],
            "gross_return_pct": _dec(r["gross_return_pct"]),
            "hold_days": r["hold_days"],
            "model_alpha_pct": _dec(r["model_alpha_pct"]),
        }

    return _fmt(rows[0]), _fmt(rows[-1])


def _attribution_summary_snapshot(
    conn: psycopg.Connection[Any],
) -> list[dict[str, Any]]:
    """Latest attribution summary per window (from return_attribution_summary)."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT DISTINCT ON (window_days)
                   window_days, positions_attributed,
                   avg_gross_return_pct, avg_market_return_pct,
                   avg_model_alpha_pct, computed_at
            FROM return_attribution_summary
            ORDER BY window_days, computed_at DESC
            """,
        )
        return [
            {
                "window_days": r["window_days"],
                "positions_attributed": r["positions_attributed"],
                "avg_gross_return_pct": _dec(r["avg_gross_return_pct"]),
                "avg_market_return_pct": _dec(r["avg_market_return_pct"]),
                "avg_model_alpha_pct": _dec(r["avg_model_alpha_pct"]),
            }
            for r in cur.fetchall()
        ]


def _thesis_accuracy(
    conn: psycopg.Connection[Any],
    period_start: date,
    period_end: date,
) -> list[dict[str, Any]]:
    """For closed positions in the period, check if price hit buy/base/bear targets."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT ra.instrument_id, i.symbol,
                   ra.gross_return_pct,
                   t.base_value, t.bull_value, t.bear_value,
                   t.stance, t.confidence_score,
                   f_exit.price AS exit_price
            FROM return_attribution ra
            JOIN instruments i USING (instrument_id)
            LEFT JOIN LATERAL (
                SELECT base_value, bull_value, bear_value, stance, confidence_score
                FROM theses
                WHERE instrument_id = ra.instrument_id
                  AND created_at <= ra.computed_at
                ORDER BY created_at DESC
                LIMIT 1
            ) t ON true
            LEFT JOIN fills f_exit ON f_exit.fill_id = ra.exit_fill_id
            WHERE ra.hold_end >= %(start)s
              AND ra.hold_end <= %(end)s
            """,
            {"start": period_start, "end": period_end},
        )
        results = []
        for r in cur.fetchall():
            exit_price = r["exit_price"]
            base = r["base_value"]
            bull = r["bull_value"]
            bear = r["bear_value"]
            hit = None
            if exit_price is not None and base is not None:
                if bull is not None and exit_price >= bull:
                    hit = "bull"
                elif exit_price >= base:
                    hit = "base"
                elif bear is not None and exit_price <= bear:
                    hit = "bear"
                else:
                    hit = "between_bear_and_base"

            results.append({
                "symbol": r["symbol"],
                "instrument_id": r["instrument_id"],
                "gross_return_pct": _dec(r["gross_return_pct"]),
                "thesis_stance": r["stance"],
                "thesis_confidence": _dec(r["confidence_score"]),
                "target_hit": hit,
            })
        return results


def _tax_provision_snapshot(conn: psycopg.Connection[Any]) -> dict[str, str | None]:
    """Current tax provision from budget state."""
    budget = compute_budget_state(conn)
    return {
        "estimated_tax_gbp": _dec(budget.estimated_tax_gbp),
        "estimated_tax_usd": _dec(budget.estimated_tax_usd),
        "tax_year": budget.tax_year,
    }


def generate_monthly_report(
    conn: psycopg.Connection[Any],
    period_start: date,
    period_end: date,
) -> MonthlyReport:
    """Generate a monthly performance report snapshot.

    Reads from existing tables and returns a plain dict for JSONB storage.
    The caller owns the transaction and commit.
    """
    pnl = _period_pnl(conn, period_start, period_end)
    position_pnl = _position_pnl_breakdown(conn, period_start, period_end)
    win_hold = _win_rate_and_holding(conn, period_start, period_end)
    best, worst = _best_worst_trade(conn, period_start, period_end)
    attribution = _attribution_summary_snapshot(conn)
    accuracy = _thesis_accuracy(conn, period_start, period_end)
    tax = _tax_provision_snapshot(conn)

    return {
        "report_type": "monthly",
        "period_start": period_start.isoformat(),
        "period_end": period_end.isoformat(),
        "generated_at": datetime.now(tz=timezone.utc).isoformat(),
        "pnl": pnl,
        "position_pnl": position_pnl,
        "win_rate": win_hold,
        "avg_holding_days": win_hold["avg_holding_days"],
        "best_trade": best,
        "worst_trade": worst,
        "attribution_summary": attribution,
        "thesis_accuracy": accuracy,
        "tax_provision": tax,
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_reporting.py::TestGenerateMonthlyReport -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add app/services/reporting.py tests/test_reporting.py
git commit -m "feat(#207): monthly report generator with position P&L, win rate, attribution, thesis accuracy, tax"
```

---

### Task 5: Persist and retrieve report snapshots

**Files:**
- Modify: `app/services/reporting.py`
- Modify: `tests/test_reporting.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/test_reporting.py`:

```python
from app.services.reporting import persist_report_snapshot, load_report_snapshots
import json


class TestPersistReportSnapshot:
    def test_persist_inserts_row(self) -> None:
        """persist_report_snapshot should execute an INSERT with ON CONFLICT DO UPDATE."""
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
        conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        report = {
            "report_type": "weekly",
            "period_start": "2026-04-06",
            "period_end": "2026-04-12",
            "pnl": {"realised_pnl": "100", "unrealised_pnl": "200"},
        }

        persist_report_snapshot(
            conn,
            report_type="weekly",
            period_start=date(2026, 4, 6),
            period_end=date(2026, 4, 12),
            snapshot=report,
        )

        cursor.execute.assert_called_once()
        call_args = cursor.execute.call_args
        sql = call_args[0][0]
        params = call_args[0][1]
        assert "ON CONFLICT" in sql
        assert params["report_type"] == "weekly"
        assert params["period_start"] == date(2026, 4, 6)


class TestLoadReportSnapshots:
    def test_load_returns_list(self) -> None:
        """load_report_snapshots should query by report_type."""
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
        conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        cursor.fetchall.return_value = []

        result = load_report_snapshots(conn, report_type="weekly", limit=10)
        assert result == []
        cursor.execute.assert_called_once()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_reporting.py::TestPersistReportSnapshot -v`
Expected: FAIL — `persist_report_snapshot` not defined

- [ ] **Step 3: Implement persist and load functions**

Add to `app/services/reporting.py`:

```python
from psycopg.types.json import Jsonb


def persist_report_snapshot(
    conn: psycopg.Connection[Any],
    *,
    report_type: str,
    period_start: date,
    period_end: date,
    snapshot: dict[str, Any],
) -> None:
    """Upsert a report snapshot into report_snapshots.

    Idempotent: ON CONFLICT replaces the snapshot for the same
    (report_type, period_start) pair. The caller owns the commit.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO report_snapshots (report_type, period_start, period_end, snapshot_json)
            VALUES (%(report_type)s, %(period_start)s, %(period_end)s, %(snapshot)s)
            ON CONFLICT (report_type, period_start) DO UPDATE
            SET period_end   = EXCLUDED.period_end,
                snapshot_json = EXCLUDED.snapshot_json,
                computed_at  = NOW()
            """,
            {
                "report_type": report_type,
                "period_start": period_start,
                "period_end": period_end,
                "snapshot": Jsonb(snapshot),
            },
        )


def load_report_snapshots(
    conn: psycopg.Connection[Any],
    *,
    report_type: str,
    limit: int = 10,
) -> list[dict[str, Any]]:
    """Load the most recent report snapshots of a given type."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT snapshot_id, report_type, period_start, period_end,
                   snapshot_json, computed_at
            FROM report_snapshots
            WHERE report_type = %(report_type)s
            ORDER BY period_start DESC
            LIMIT %(limit)s
            """,
            {"report_type": report_type, "limit": limit},
        )
        return cur.fetchall()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_reporting.py::TestPersistReportSnapshot tests/test_reporting.py::TestLoadReportSnapshots -v`
Expected: 2 passed

- [ ] **Step 5: Commit**

```bash
git add app/services/reporting.py tests/test_reporting.py
git commit -m "feat(#207): persist and load report snapshots with idempotent upsert"
```

---

### Task 6: Scheduler jobs — weekly and monthly report generation

**Files:**
- Modify: `app/workers/scheduler.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/test_reporting.py`:

```python
class TestReportSchedulerJobs:
    def test_weekly_report_job_registered(self) -> None:
        """weekly_report job should be in SCHEDULED_JOBS."""
        from app.workers.scheduler import SCHEDULED_JOBS
        names = [j.name for j in SCHEDULED_JOBS]
        assert "weekly_report" in names

    def test_monthly_report_job_registered(self) -> None:
        """monthly_report job should be in SCHEDULED_JOBS."""
        from app.workers.scheduler import SCHEDULED_JOBS
        names = [j.name for j in SCHEDULED_JOBS]
        assert "monthly_report" in names

    def test_weekly_report_cadence(self) -> None:
        """weekly_report should run Saturday morning."""
        from app.workers.scheduler import SCHEDULED_JOBS
        job = next(j for j in SCHEDULED_JOBS if j.name == "weekly_report")
        assert job.cadence.kind == "weekly"
        assert job.cadence.weekday == 5  # Saturday

    def test_monthly_report_cadence(self) -> None:
        """monthly_report should run on the 1st of each month."""
        from app.workers.scheduler import SCHEDULED_JOBS
        job = next(j for j in SCHEDULED_JOBS if j.name == "monthly_report")
        assert job.cadence.kind == "monthly"
        assert job.cadence.day == 1
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_reporting.py::TestReportSchedulerJobs -v`
Expected: FAIL — jobs not registered

- [ ] **Step 3: Add job constants, prerequisite, and register jobs**

In `app/workers/scheduler.py`:

Add job constants near existing ones (~line 188):
```python
JOB_WEEKLY_REPORT = "weekly_report"
JOB_MONTHLY_REPORT = "monthly_report"
```

Add prerequisite (near existing `_has_*` functions):
```python
def _has_positions_or_attributions(conn: psycopg.Connection[Any]) -> PrerequisiteResult:
    """True if there are open positions or any attributed positions (something to report on)."""
    if _exists(
        conn,
        psycopg.sql.SQL(
            "SELECT EXISTS("
            "SELECT 1 FROM positions WHERE current_units > 0 "
            "UNION ALL "
            "SELECT 1 FROM return_attribution LIMIT 1"
            ")"
        ),
    ):
        return (True, "")
    return (False, "no positions or attributions to report on")
```

Add job registrations to `SCHEDULED_JOBS` list:
```python
ScheduledJob(
    name=JOB_WEEKLY_REPORT,
    description="Generate weekly performance report snapshot.",
    cadence=Cadence.weekly(weekday=5, hour=7, minute=0),  # Saturday 07:00
    prerequisite=_has_positions_or_attributions,
),
ScheduledJob(
    name=JOB_MONTHLY_REPORT,
    description="Generate monthly performance report snapshot.",
    cadence=Cadence.monthly(day=1, hour=7, minute=0),  # 1st of month 07:00
    prerequisite=_has_positions_or_attributions,
),
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_reporting.py::TestReportSchedulerJobs -v`
Expected: 4 passed

- [ ] **Step 5: Add the job invoker functions**

Still in `app/workers/scheduler.py`, add the actual job functions near the bottom (before the `_INVOKERS` dict):

```python
def weekly_report() -> None:
    """Generate and persist the weekly performance report."""
    from app.services.reporting import generate_weekly_report, persist_report_snapshot

    with _tracked_job(JOB_WEEKLY_REPORT) as tracker:
        # Period: previous Monday through Sunday
        today = datetime.now(tz=timezone.utc).date()
        # Saturday run → report covers Mon–Sun of the week just ended
        period_end = today - timedelta(days=(today.weekday() + 1) % 7)  # last Sunday
        period_start = period_end - timedelta(days=6)  # Monday of that week

        with psycopg.connect(settings.database_url) as conn:
            report = generate_weekly_report(conn, period_start, period_end)
            persist_report_snapshot(
                conn,
                report_type="weekly",
                period_start=period_start,
                period_end=period_end,
                snapshot=report,
            )
            conn.commit()
        tracker.row_count = 1


def monthly_report() -> None:
    """Generate and persist the monthly performance report."""
    from app.services.reporting import generate_monthly_report, persist_report_snapshot

    with _tracked_job(JOB_MONTHLY_REPORT) as tracker:
        # Period: previous full calendar month
        today = datetime.now(tz=timezone.utc).date()
        period_end = today.replace(day=1) - timedelta(days=1)  # last day of prev month
        period_start = period_end.replace(day=1)  # first day of prev month

        with psycopg.connect(settings.database_url) as conn:
            report = generate_monthly_report(conn, period_start, period_end)
            persist_report_snapshot(
                conn,
                report_type="monthly",
                period_start=period_start,
                period_end=period_end,
                snapshot=report,
            )
            conn.commit()
        tracker.row_count = 1
```

Add entries to `_INVOKERS` dict:
```python
JOB_WEEKLY_REPORT: weekly_report,
JOB_MONTHLY_REPORT: monthly_report,
```

- [ ] **Step 6: Run all reporting tests**

Run: `uv run pytest tests/test_reporting.py -v`
Expected: All pass

- [ ] **Step 7: Commit**

```bash
git add app/workers/scheduler.py tests/test_reporting.py
git commit -m "feat(#207): register weekly and monthly report scheduler jobs"
```

---

### Task 7: API endpoints for report snapshots

**Files:**
- Create: `app/api/reports.py`
- Modify: `app/main.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/test_reporting.py`:

```python
class TestReportsAPI:
    def test_reports_router_exists(self) -> None:
        """The reports router should have the correct prefix."""
        from app.api.reports import router
        assert router.prefix == "/api/reports"

    def test_list_weekly_endpoint_exists(self) -> None:
        """GET /api/reports/weekly should be a registered route."""
        from app.api.reports import router
        paths = [r.path for r in router.routes]
        assert "/weekly" in paths

    def test_list_monthly_endpoint_exists(self) -> None:
        """GET /api/reports/monthly should be a registered route."""
        from app.api.reports import router
        paths = [r.path for r in router.routes]
        assert "/monthly" in paths
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_reporting.py::TestReportsAPI -v`
Expected: FAIL — module not found

- [ ] **Step 3: Implement the API router**

Create `app/api/reports.py`:

```python
"""Reports API — periodic performance report snapshots."""

from __future__ import annotations

from typing import Any

import psycopg
import psycopg.rows
from fastapi import APIRouter, Depends, Query

from app.api.auth import require_session_or_service_token
from app.config import settings

router = APIRouter(
    prefix="/api/reports",
    tags=["reports"],
    dependencies=[Depends(require_session_or_service_token)],
)


@router.get("/weekly")
def list_weekly_reports(
    limit: int = Query(default=10, le=100),
) -> list[dict[str, Any]]:
    """Return the most recent weekly report snapshots."""
    with psycopg.connect(settings.database_url) as conn:
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                """
                SELECT snapshot_id, report_type, period_start, period_end,
                       snapshot_json, computed_at
                FROM report_snapshots
                WHERE report_type = 'weekly'
                ORDER BY period_start DESC
                LIMIT %(limit)s
                """,
                {"limit": limit},
            )
            return cur.fetchall()


@router.get("/monthly")
def list_monthly_reports(
    limit: int = Query(default=10, le=100),
) -> list[dict[str, Any]]:
    """Return the most recent monthly report snapshots."""
    with psycopg.connect(settings.database_url) as conn:
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                """
                SELECT snapshot_id, report_type, period_start, period_end,
                       snapshot_json, computed_at
                FROM report_snapshots
                WHERE report_type = 'monthly'
                ORDER BY period_start DESC
                LIMIT %(limit)s
                """,
                {"limit": limit},
            )
            return cur.fetchall()


@router.get("/latest")
def get_latest_report(
    report_type: str = Query(pattern="^(weekly|monthly)$"),
) -> dict[str, Any] | None:
    """Return the single most recent report of the given type."""
    with psycopg.connect(settings.database_url) as conn:
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                """
                SELECT snapshot_id, report_type, period_start, period_end,
                       snapshot_json, computed_at
                FROM report_snapshots
                WHERE report_type = %(report_type)s
                ORDER BY period_start DESC
                LIMIT 1
                """,
                {"report_type": report_type},
            )
            return cur.fetchone()
```

- [ ] **Step 4: Register the router in `app/main.py`**

Add to the router registration block in `app/main.py`:

```python
from app.api.reports import router as reports_router
app.include_router(reports_router)
```

- [ ] **Step 5: Run test to verify it passes**

Run: `uv run pytest tests/test_reporting.py::TestReportsAPI -v`
Expected: 3 passed

- [ ] **Step 6: Commit**

```bash
git add app/api/reports.py app/main.py tests/test_reporting.py
git commit -m "feat(#207): reports API endpoints — weekly, monthly, latest"
```

---

### Task 8: Edge case tests

**Files:**
- Modify: `tests/test_reporting.py`

- [ ] **Step 1: Write edge case tests**

Add to `tests/test_reporting.py`:

```python
class TestWinRateEdgeCases:
    def test_no_closed_positions_returns_none(self) -> None:
        """Win rate should be None when no positions closed in the period."""
        from app.services.reporting import _win_rate_and_holding

        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
        conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        cursor.fetchall.return_value = []

        result = _win_rate_and_holding(conn, date(2026, 4, 1), date(2026, 4, 30))
        assert result["total_closed"] == 0
        assert result["win_rate_pct"] is None
        assert result["avg_holding_days"] is None

    def test_all_winners(self) -> None:
        """100% win rate when all positions were profitable."""
        from app.services.reporting import _win_rate_and_holding

        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
        conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        cursor.fetchall.return_value = [
            {"gross_return_pct": Decimal("0.10"), "hold_days": 30},
            {"gross_return_pct": Decimal("0.05"), "hold_days": 45},
        ]

        result = _win_rate_and_holding(conn, date(2026, 4, 1), date(2026, 4, 30))
        assert result["total_closed"] == 2
        assert result["win_rate_pct"] == "100.00"
        assert result["avg_holding_days"] == 37.5


class TestBottomPerformersEdge:
    def test_fewer_than_n_positions(self) -> None:
        """With fewer positions than N, bottom list should not duplicate top."""
        from app.services.reporting import _top_bottom_performers

        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
        conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        cursor.fetchall.return_value = [
            {
                "instrument_id": 1, "symbol": "AAPL", "company_name": "Apple",
                "unrealized_pnl": Decimal("100"), "current_units": Decimal("5"),
                "avg_cost": Decimal("150"),
            },
        ]

        top, bottom = _top_bottom_performers(conn, n=3)
        assert len(top) == 1
        assert len(bottom) == 0  # only 1 position, goes to top, none left for bottom


class TestDecHelper:
    def test_none_returns_none(self) -> None:
        from app.services.reporting import _dec
        assert _dec(None) is None

    def test_decimal_returns_string(self) -> None:
        from app.services.reporting import _dec
        assert _dec(Decimal("1.23")) == "1.23"
```

- [ ] **Step 2: Run all tests**

Run: `uv run pytest tests/test_reporting.py -v`
Expected: All pass

- [ ] **Step 3: Commit**

```bash
git add tests/test_reporting.py
git commit -m "test(#207): edge case tests — win rate, performer lists, decimal helper"
```

---

### Task 9: Local checks and self-review

- [ ] **Step 1: Run full pre-push checklist**

```bash
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv run pytest
```

All four must pass.

- [ ] **Step 2: Fix any issues found**

- [ ] **Step 3: Self-review the full diff**

Run: `git diff main --stat` and `git diff main` to review all changes.

Check against the pre-flight review skill:
- No security issues (parameterised queries, auth on endpoints)
- No type mismatches (Decimal → str for JSONB, proper None handling)
- No prevention log violations
- No settled decision violations

- [ ] **Step 4: Commit any fixes**

```bash
git add -u
git commit -m "chore(#207): pre-push fixes from self-review"
```
