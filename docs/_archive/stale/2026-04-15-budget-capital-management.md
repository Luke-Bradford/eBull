# Budget and Capital Management Service — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give eBull a working-budget concept — track capital deposits/withdrawals, provision estimated tax liability, enforce a cash buffer, and expose "available for deployment" to the execution guard and dashboard.

**Architecture:** Pure service module (`budget.py`) that computes budget state on the fly from existing tables (`cash_ledger`, `positions`, `disposal_matches`, `live_fx_rates`) plus two new tables (`capital_events`, `budget_config`). No materialized views, no scheduler job — budget state is computed fresh on every read. The execution guard replaces its raw `cash > 0` check with `available_for_deployment > 0`.

**Tech Stack:** Python 3.12, psycopg 3, FastAPI, PostgreSQL, Pydantic, pytest

---

## Settled Decisions That Apply

| Decision | How this plan preserves it |
|----------|--------------------------|
| **Cash semantics** (positive = inflow, negative = outflow) | Budget service reads `SUM(cash_ledger.amount)` unchanged; capital_events uses its own sign convention (always positive, type implies direction) to avoid confusion. |
| **Unknown cash rule** (empty ledger blocks execution, not recs) | Budget service returns `cash_balance=None` when ledger is empty; execution guard fails closed on `None`. |
| **AUM basis** (MTM first, cost basis fallback) | `deployed_capital` computation reuses the `WHERE current_units > 0` + LATERAL quote join pattern from `portfolio.py`. |
| **Guard auditability** (one decision_audit row per invocation) | Budget-aware check is a new rule in the existing rule list — evidence_json captures the budget snapshot. |
| **Guard re-check rule** (re-check current state) | Budget state is computed fresh inside the guard, never cached. |
| **Provider boundary** (thin providers, domain in services) | Budget is a pure service module — no HTTP client, no provider. |

## Prevention Log Entries That Apply

| Entry | How avoided |
|-------|------------|
| **Audit reads outside the write transaction** | `budget_config` audit rows written in same `conn.transaction()` as the UPDATE. |
| **Mid-transaction conn.commit() in service functions** | `budget.py` functions accept caller connections, never call `conn.commit()`. |
| **Zero-unit position inflates AUM** | Deployed capital query includes `WHERE current_units > 0`. |
| **Dead-code None-guard on aggregate fetchone()** | `SUM()` always returns one row; None-guard is on the column value, not the row. |
| **Single-row UPDATE silent no-op** | Budget config UPDATE checks `rowcount == 0` and raises. |
| **Read-then-write outside transaction** | Config update reads old values and writes audit in one `conn.transaction()`. |

---

## File Structure

| File | Responsibility |
|------|---------------|
| `sql/027_budget_capital.sql` | Migration: `capital_events` table, `budget_config` singleton, `budget_config_audit` |
| `app/services/budget.py` | Pure service: `BudgetState`, `BudgetConfig`, `compute_budget_state()`, `record_capital_event()`, `list_capital_events()`, `get_budget_config()`, `update_budget_config()` |
| `tests/test_budget.py` | Unit tests for all budget service functions |
| `app/api/budget.py` | API endpoints: GET /budget, POST /budget/events, GET /budget/events, PATCH /budget/config |
| `app/services/execution_guard.py` | Modify: replace `_check_cash` with budget-aware `_check_budget` |
| `tests/test_execution_guard.py` | Modify: update cash-check tests for budget-aware logic |
| `app/main.py` | Modify: register budget router |

## Scope Boundaries

**In scope (this PR):**
- Schema: `capital_events`, `budget_config`, `budget_config_audit`
- Service: compute budget state, record/list capital events, get/update config
- API: four endpoints for budget state and management
- Execution guard: budget-aware BUY/ADD cash check
- Tests: full unit coverage

**Out of scope (deferred):**
- Auto-detection of capital changes during broker sync → follow-up PR
- Order client sizing from `available_for_deployment` → follow-up PR (guard is the enforcement point for now)
- Fresh capital deployment strategy (wait 1-3 days) → #205 autonomous loop
- Dashboard UI integration → separate frontend PR
- Scheduler budget snapshot job → not needed (computed on the fly)

---

## Task 1: Migration 027 — Schema

**Files:**
- Create: `sql/027_budget_capital.sql`

- [ ] **Step 1: Write the migration**

```sql
-- Migration 027: budget and capital management tables
--
-- capital_events: tracks operator deposits, withdrawals, and system tax provisions.
--   amount is always positive; event_type implies direction (injection = inflow,
--   withdrawal = outflow, tax_provision = reserved, tax_release = un-reserved).
--
-- budget_config: singleton (same pattern as runtime_config) with operator-
--   configurable budget parameters.  Seeded with sensible defaults.
--
-- budget_config_audit: per-field audit trail for config changes.

-- A. Capital events ledger
CREATE TABLE IF NOT EXISTS capital_events (
    event_id    BIGSERIAL PRIMARY KEY,
    event_time  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    event_type  TEXT NOT NULL,
    amount      NUMERIC(18,6) NOT NULL,
    currency    TEXT NOT NULL DEFAULT 'USD',
    source      TEXT NOT NULL DEFAULT 'operator',
    note        TEXT,
    created_by  TEXT
);

DO $$
BEGIN
    ALTER TABLE capital_events
        DROP CONSTRAINT IF EXISTS chk_capital_event_type;
    ALTER TABLE capital_events
        ADD CONSTRAINT chk_capital_event_type
        CHECK (event_type IN ('injection', 'withdrawal', 'tax_provision', 'tax_release'));
END $$;

DO $$
BEGIN
    ALTER TABLE capital_events
        DROP CONSTRAINT IF EXISTS chk_capital_event_source;
    ALTER TABLE capital_events
        ADD CONSTRAINT chk_capital_event_source
        CHECK (source IN ('operator', 'system', 'broker_sync'));
END $$;

-- Positive-amount invariant
DO $$
BEGIN
    ALTER TABLE capital_events
        DROP CONSTRAINT IF EXISTS chk_capital_event_amount_positive;
    ALTER TABLE capital_events
        ADD CONSTRAINT chk_capital_event_amount_positive
        CHECK (amount > 0);
END $$;

-- B. Budget config singleton
CREATE TABLE IF NOT EXISTS budget_config (
    id               BOOLEAN PRIMARY KEY DEFAULT TRUE,
    cash_buffer_pct  NUMERIC(5,4) NOT NULL DEFAULT 0.05,
    cgt_scenario     TEXT NOT NULL DEFAULT 'higher',
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_by       TEXT NOT NULL DEFAULT 'system',
    reason           TEXT NOT NULL DEFAULT 'initial seed'
);

DO $$
BEGIN
    ALTER TABLE budget_config
        DROP CONSTRAINT IF EXISTS chk_cgt_scenario;
    ALTER TABLE budget_config
        ADD CONSTRAINT chk_cgt_scenario
        CHECK (cgt_scenario IN ('basic', 'higher'));
END $$;

DO $$
BEGIN
    ALTER TABLE budget_config
        DROP CONSTRAINT IF EXISTS chk_cash_buffer_pct_range;
    ALTER TABLE budget_config
        ADD CONSTRAINT chk_cash_buffer_pct_range
        CHECK (cash_buffer_pct >= 0 AND cash_buffer_pct <= 0.50);
END $$;

-- Singleton constraint: only id=TRUE allowed
DO $$
BEGIN
    ALTER TABLE budget_config
        DROP CONSTRAINT IF EXISTS chk_budget_config_singleton;
    ALTER TABLE budget_config
        ADD CONSTRAINT chk_budget_config_singleton
        CHECK (id = TRUE);
END $$;

-- Seed the singleton row
INSERT INTO budget_config (id) VALUES (TRUE) ON CONFLICT DO NOTHING;

-- C. Budget config audit
CREATE TABLE IF NOT EXISTS budget_config_audit (
    audit_id    BIGSERIAL PRIMARY KEY,
    changed_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    changed_by  TEXT NOT NULL,
    field       TEXT NOT NULL,
    old_value   TEXT,
    new_value   TEXT,
    reason      TEXT
);
```

- [ ] **Step 2: Run the migration against local dev DB**

Run: `psql "$DATABASE_URL" -f sql/027_budget_capital.sql`
Expected: no errors, tables created, singleton row seeded.

- [ ] **Step 3: Verify idempotency**

Run: `psql "$DATABASE_URL" -f sql/027_budget_capital.sql`
Expected: no errors on second run (IF NOT EXISTS, ON CONFLICT DO NOTHING, DROP CONSTRAINT IF EXISTS).

- [ ] **Step 4: Commit**

```bash
git add sql/027_budget_capital.sql
git commit -m "feat(#203): migration 027 — capital_events and budget_config tables"
```

---

## Task 2: Budget Service — Types and Config

**Files:**
- Create: `app/services/budget.py`

- [ ] **Step 1: Write the failing test for BudgetConfig loading**

Create `tests/test_budget.py`:

```python
"""Tests for the budget and capital management service.

Mock approach: mock psycopg.Connection with controlled cursor return values.
Pattern matches test_entry_timing.py — _make_cursor() + _make_conn().
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

import psycopg.rows

from app.services.budget import BudgetConfig, BudgetConfigCorrupt, get_budget_config


def _make_cursor(rows: list[dict] | None = None, single: dict | None = None) -> MagicMock:
    """Build a mock cursor whose fetchone/fetchall return controlled values."""
    cur = MagicMock()
    if single is not None:
        cur.execute.return_value.fetchone.return_value = single
        cur.fetchone.return_value = single
    if rows is not None:
        cur.execute.return_value.fetchall.return_value = rows
        cur.fetchall.return_value = rows
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    return cur


def _make_conn(cursors: list[MagicMock]) -> MagicMock:
    """Build a mock connection that yields cursors in order."""
    conn = MagicMock()
    conn.cursor.side_effect = cursors
    return conn


class TestGetBudgetConfig:
    def test_returns_config_from_db(self) -> None:
        cur = _make_cursor(single={
            "cash_buffer_pct": Decimal("0.05"),
            "cgt_scenario": "higher",
            "updated_at": "2026-04-15T00:00:00+00:00",
            "updated_by": "operator",
            "reason": "initial seed",
        })
        conn = _make_conn([cur])

        config = get_budget_config(conn)

        assert config.cash_buffer_pct == Decimal("0.05")
        assert config.cgt_scenario == "higher"

    def test_raises_corrupt_when_row_missing(self) -> None:
        cur = _make_cursor(single=None)
        conn = _make_conn([cur])

        import pytest
        with pytest.raises(BudgetConfigCorrupt):
            get_budget_config(conn)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_budget.py::TestGetBudgetConfig -v`
Expected: FAIL — `ImportError: cannot import name 'BudgetConfig'`

- [ ] **Step 3: Write the types and get_budget_config()**

Create `app/services/budget.py`:

```python
"""Budget and capital management service.

Computes the working budget — how much capital is available for deployment —
from existing tables (cash_ledger, positions, disposal_matches, live_fx_rates)
plus two new tables (capital_events, budget_config).

Key concepts:
  - working_budget = cash_balance + deployed_capital + mirror_equity
  - available_for_deployment = cash_balance - estimated_tax_usd - cash_buffer_reserve
  - cash_buffer_reserve = working_budget * cash_buffer_pct
  - estimated_tax_usd = estimated CGT (from tax_year_summary) converted via live_fx_rates

Design choices:
  - Pure service module: reads DB, returns results. No side effects except
    explicit write functions (record_capital_event, update_budget_config).
  - Budget state is computed fresh on every read — no caching, no snapshots.
  - capital_events.amount is always positive; event_type implies direction.
  - budget_config is a singleton (same pattern as runtime_config).

Issue #203.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Literal

import psycopg
import psycopg.rows

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CapitalEventType = Literal["injection", "withdrawal", "tax_provision", "tax_release"]
CapitalEventSource = Literal["operator", "system", "broker_sync"]
CgtScenario = Literal["basic", "higher"]

_ZERO = Decimal("0")


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class BudgetConfigCorrupt(RuntimeError):
    """Raised when the budget_config singleton row is missing.

    Callers must fail closed — never default to unconstrained spending.
    """


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class BudgetConfig:
    cash_buffer_pct: Decimal
    cgt_scenario: str  # 'basic' or 'higher'
    updated_at: datetime
    updated_by: str
    reason: str


@dataclass(frozen=True)
class BudgetState:
    """Snapshot of the current budget position."""

    # Observed values (USD)
    cash_balance: Decimal | None  # None = ledger empty (unknown)
    deployed_capital: Decimal     # SUM(cost_basis) for open positions
    mirror_equity: Decimal        # from copy-trading mirrors

    # Computed (USD)
    working_budget: Decimal | None  # cash + deployed + mirrors; None if cash unknown

    # Reservations (USD)
    estimated_tax_gbp: Decimal    # from tax_year_summary
    estimated_tax_usd: Decimal    # converted at latest GBP→USD rate
    gbp_usd_rate: Decimal | None  # rate used for conversion; None if unavailable
    cash_buffer_reserve: Decimal  # working_budget * cash_buffer_pct

    # Bottom line (USD)
    available_for_deployment: Decimal | None  # cash - tax - buffer; None if cash unknown

    # Config echo
    cash_buffer_pct: Decimal
    cgt_scenario: str
    tax_year: str


@dataclass(frozen=True)
class CapitalEvent:
    """A single capital event row."""

    event_id: int
    event_time: datetime
    event_type: str
    amount: Decimal
    currency: str
    source: str
    note: str | None
    created_by: str | None


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------


def get_budget_config(conn: psycopg.Connection[Any]) -> BudgetConfig:
    """Load the budget_config singleton.

    Raises BudgetConfigCorrupt if the row is missing.
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT cash_buffer_pct, cgt_scenario,
                   updated_at, updated_by, reason
            FROM budget_config
            WHERE id = TRUE
            """
        )
        row = cur.fetchone()
    if row is None:
        raise BudgetConfigCorrupt("budget_config singleton row missing")
    return BudgetConfig(
        cash_buffer_pct=Decimal(str(row["cash_buffer_pct"])),
        cgt_scenario=str(row["cgt_scenario"]),
        updated_at=row["updated_at"],
        updated_by=str(row["updated_by"]),
        reason=str(row["reason"]),
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_budget.py::TestGetBudgetConfig -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add app/services/budget.py tests/test_budget.py
git commit -m "feat(#203): budget service types, BudgetConfig, get_budget_config()"
```

---

## Task 3: Budget Service — update_budget_config()

**Files:**
- Modify: `app/services/budget.py`
- Modify: `tests/test_budget.py`

- [ ] **Step 1: Write the failing test for update_budget_config**

Add to `tests/test_budget.py`:

```python
from app.services.budget import update_budget_config


class TestUpdateBudgetConfig:
    def test_updates_config_and_writes_audit(self) -> None:
        """PATCH cash_buffer_pct: verify UPDATE + audit INSERT."""
        # Current config
        config_cur = _make_cursor(single={
            "cash_buffer_pct": Decimal("0.05"),
            "cgt_scenario": "higher",
        })
        # UPDATE result
        update_cur = MagicMock()
        update_cur.execute.return_value.rowcount = 1
        update_cur.__enter__ = MagicMock(return_value=update_cur)
        update_cur.__exit__ = MagicMock(return_value=False)
        # Audit INSERT
        audit_cur = MagicMock()
        audit_cur.__enter__ = MagicMock(return_value=audit_cur)
        audit_cur.__exit__ = MagicMock(return_value=False)

        conn = _make_conn([config_cur, update_cur, audit_cur])
        # Mock transaction context manager
        conn.transaction.return_value.__enter__ = MagicMock()
        conn.transaction.return_value.__exit__ = MagicMock(return_value=False)

        result = update_budget_config(
            conn,
            cash_buffer_pct=Decimal("0.08"),
            updated_by="operator",
            reason="increase buffer",
        )

        assert result.cash_buffer_pct == Decimal("0.08")

    def test_raises_on_missing_singleton(self) -> None:
        """UPDATE affects 0 rows → raise."""
        config_cur = _make_cursor(single={
            "cash_buffer_pct": Decimal("0.05"),
            "cgt_scenario": "higher",
        })
        update_cur = MagicMock()
        update_cur.execute.return_value.rowcount = 0
        update_cur.__enter__ = MagicMock(return_value=update_cur)
        update_cur.__exit__ = MagicMock(return_value=False)

        conn = _make_conn([config_cur, update_cur])
        conn.transaction.return_value.__enter__ = MagicMock()
        conn.transaction.return_value.__exit__ = MagicMock(return_value=False)

        import pytest
        with pytest.raises(BudgetConfigCorrupt):
            update_budget_config(
                conn,
                cash_buffer_pct=Decimal("0.08"),
                updated_by="operator",
                reason="test",
            )
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_budget.py::TestUpdateBudgetConfig -v`
Expected: FAIL — `ImportError: cannot import name 'update_budget_config'`

- [ ] **Step 3: Implement update_budget_config()**

Add to `app/services/budget.py`:

```python
def update_budget_config(
    conn: psycopg.Connection[Any],
    *,
    cash_buffer_pct: Decimal | None = None,
    cgt_scenario: CgtScenario | None = None,
    updated_by: str,
    reason: str,
) -> BudgetConfig:
    """Partial update of budget_config with per-field audit trail.

    At least one of cash_buffer_pct or cgt_scenario must be provided.
    Reads old values, writes UPDATE + audit rows in one transaction.
    Returns the new config state.

    Raises BudgetConfigCorrupt if the singleton row is missing.
    Raises ValueError if no fields are provided.
    """
    if cash_buffer_pct is None and cgt_scenario is None:
        raise ValueError("at least one field must be provided")

    now = datetime.now(tz=UTC)

    with conn.transaction():
        # Read current values inside the transaction (prevention: audit reads
        # outside the write transaction).
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                "SELECT cash_buffer_pct, cgt_scenario FROM budget_config WHERE id = TRUE"
            )
            old = cur.fetchone()
        if old is None:
            raise BudgetConfigCorrupt("budget_config singleton row missing")

        # Build SET clause for changed fields only
        changes: dict[str, tuple[str, str]] = {}  # field -> (old, new)
        set_parts: list[str] = []
        params: dict[str, Any] = {"by": updated_by, "reason": reason, "now": now}

        if cash_buffer_pct is not None and Decimal(str(old["cash_buffer_pct"])) != cash_buffer_pct:
            changes["cash_buffer_pct"] = (str(old["cash_buffer_pct"]), str(cash_buffer_pct))
            set_parts.append("cash_buffer_pct = %(new_buffer)s")
            params["new_buffer"] = cash_buffer_pct

        if cgt_scenario is not None and str(old["cgt_scenario"]) != cgt_scenario:
            changes["cgt_scenario"] = (str(old["cgt_scenario"]), cgt_scenario)
            set_parts.append("cgt_scenario = %(new_cgt)s")
            params["new_cgt"] = cgt_scenario

        if not changes:
            raise ValueError("no fields changed")

        set_parts.append("updated_at = %(now)s")
        set_parts.append("updated_by = %(by)s")
        set_parts.append("reason = %(reason)s")

        with conn.cursor() as cur:
            result = cur.execute(
                f"UPDATE budget_config SET {', '.join(set_parts)} WHERE id = TRUE",  # noqa: S608
                params,
            )
            if result.rowcount == 0:
                raise BudgetConfigCorrupt("budget_config singleton row missing after UPDATE")

        # Write audit rows — one per changed field
        with conn.cursor() as cur:
            for field, (old_val, new_val) in changes.items():
                cur.execute(
                    """
                    INSERT INTO budget_config_audit
                        (changed_at, changed_by, field, old_value, new_value, reason)
                    VALUES (%(at)s, %(by)s, %(field)s, %(old)s, %(new)s, %(reason)s)
                    """,
                    {
                        "at": now,
                        "by": updated_by,
                        "field": field,
                        "old": old_val,
                        "new": new_val,
                        "reason": reason,
                    },
                )

    return BudgetConfig(
        cash_buffer_pct=cash_buffer_pct if cash_buffer_pct is not None else Decimal(str(old["cash_buffer_pct"])),
        cgt_scenario=cgt_scenario if cgt_scenario is not None else str(old["cgt_scenario"]),
        updated_at=now,
        updated_by=updated_by,
        reason=reason,
    )
```

**Note on the `f"UPDATE..."` line:** The SET clause is built from a fixed set of column names (never user input). The `# noqa: S608` suppresses the Bandit warning for this safe dynamic SQL. All values use parameterised placeholders.

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_budget.py::TestUpdateBudgetConfig -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add app/services/budget.py tests/test_budget.py
git commit -m "feat(#203): update_budget_config() with per-field audit trail"
```

---

## Task 4: Budget Service — Capital Events

**Files:**
- Modify: `app/services/budget.py`
- Modify: `tests/test_budget.py`

- [ ] **Step 1: Write the failing test for record_capital_event**

Add to `tests/test_budget.py`:

```python
from app.services.budget import record_capital_event, list_capital_events, CapitalEvent


class TestRecordCapitalEvent:
    def test_inserts_event_and_returns_it(self) -> None:
        cur = MagicMock()
        cur.execute.return_value.fetchone.return_value = {
            "event_id": 1,
            "event_time": datetime(2026, 4, 15, tzinfo=UTC),
            "event_type": "injection",
            "amount": Decimal("10000"),
            "currency": "USD",
            "source": "operator",
            "note": "initial deposit",
            "created_by": "luke",
        }
        cur.__enter__ = MagicMock(return_value=cur)
        cur.__exit__ = MagicMock(return_value=False)
        conn = _make_conn([cur])

        event = record_capital_event(
            conn,
            event_type="injection",
            amount=Decimal("10000"),
            currency="USD",
            note="initial deposit",
            created_by="luke",
        )

        assert event.event_id == 1
        assert event.event_type == "injection"
        assert event.amount == Decimal("10000")

    def test_rejects_non_positive_amount(self) -> None:
        conn = _make_conn([])
        import pytest
        with pytest.raises(ValueError, match="positive"):
            record_capital_event(
                conn,
                event_type="injection",
                amount=Decimal("0"),
                created_by="test",
            )


class TestListCapitalEvents:
    def test_returns_events_ordered_by_time(self) -> None:
        cur = _make_cursor(rows=[
            {
                "event_id": 2,
                "event_time": datetime(2026, 4, 15, tzinfo=UTC),
                "event_type": "injection",
                "amount": Decimal("5000"),
                "currency": "USD",
                "source": "operator",
                "note": "top-up",
                "created_by": "luke",
            },
            {
                "event_id": 1,
                "event_time": datetime(2026, 4, 10, tzinfo=UTC),
                "event_type": "injection",
                "amount": Decimal("10000"),
                "currency": "USD",
                "source": "operator",
                "note": "initial",
                "created_by": "luke",
            },
        ])
        conn = _make_conn([cur])

        events = list_capital_events(conn)

        assert len(events) == 2
        assert events[0].event_id == 2  # most recent first

    def test_returns_empty_list_when_no_events(self) -> None:
        cur = _make_cursor(rows=[])
        conn = _make_conn([cur])

        events = list_capital_events(conn)

        assert events == []
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_budget.py::TestRecordCapitalEvent -v`
Expected: FAIL — `ImportError: cannot import name 'record_capital_event'`

- [ ] **Step 3: Implement record_capital_event() and list_capital_events()**

Add to `app/services/budget.py`:

```python
def record_capital_event(
    conn: psycopg.Connection[Any],
    *,
    event_type: CapitalEventType,
    amount: Decimal,
    currency: str = "USD",
    source: CapitalEventSource = "operator",
    note: str | None = None,
    created_by: str,
) -> CapitalEvent:
    """Insert a capital event and return the created row.

    amount must be positive — the event_type implies direction.
    """
    if amount <= 0:
        raise ValueError("amount must be positive")

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        row = cur.execute(
            """
            INSERT INTO capital_events
                (event_type, amount, currency, source, note, created_by)
            VALUES (%(type)s, %(amount)s, %(currency)s, %(source)s, %(note)s, %(by)s)
            RETURNING event_id, event_time, event_type, amount, currency,
                      source, note, created_by
            """,
            {
                "type": event_type,
                "amount": amount,
                "currency": currency,
                "source": source,
                "note": note,
                "by": created_by,
            },
        ).fetchone()

    if row is None:
        raise RuntimeError("INSERT RETURNING produced no row")

    return CapitalEvent(
        event_id=int(row["event_id"]),
        event_time=row["event_time"],
        event_type=str(row["event_type"]),
        amount=Decimal(str(row["amount"])),
        currency=str(row["currency"]),
        source=str(row["source"]),
        note=row["note"],
        created_by=row["created_by"],
    )


def list_capital_events(
    conn: psycopg.Connection[Any],
    *,
    limit: int = 50,
    offset: int = 0,
) -> list[CapitalEvent]:
    """List capital events, most recent first."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT event_id, event_time, event_type, amount, currency,
                   source, note, created_by
            FROM capital_events
            ORDER BY event_time DESC
            LIMIT %(limit)s OFFSET %(offset)s
            """,
            {"limit": limit, "offset": offset},
        )
        rows = cur.fetchall()

    return [
        CapitalEvent(
            event_id=int(r["event_id"]),
            event_time=r["event_time"],
            event_type=str(r["event_type"]),
            amount=Decimal(str(r["amount"])),
            currency=str(r["currency"]),
            source=str(r["source"]),
            note=r["note"],
            created_by=r["created_by"],
        )
        for r in rows
    ]
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_budget.py::TestRecordCapitalEvent tests/test_budget.py::TestListCapitalEvents -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add app/services/budget.py tests/test_budget.py
git commit -m "feat(#203): record_capital_event() and list_capital_events()"
```

---

## Task 5: Budget Service — compute_budget_state()

This is the core function. It reads from five tables and returns a single `BudgetState` snapshot.

**Files:**
- Modify: `app/services/budget.py`
- Modify: `tests/test_budget.py`

- [ ] **Step 1: Write the failing tests for compute_budget_state**

Add to `tests/test_budget.py`:

```python
from app.services.budget import compute_budget_state, BudgetState


class TestComputeBudgetState:
    """Tests for the core budget state computation.

    Connection cursor sequence (one cursor per query):
      0. budget_config
      1. cash_balance (SUM from cash_ledger)
      2. deployed_capital (SUM cost_basis from positions)
      3. mirror_equity (SUM from copy_mirrors + copy_mirror_positions)
      4. tax_year_summary (disposal_matches aggregates)
      5. gbp_usd rate (live_fx_rates)
    """

    def _config_cursor(
        self,
        buffer_pct: str = "0.05",
        cgt: str = "higher",
    ) -> MagicMock:
        return _make_cursor(single={
            "cash_buffer_pct": Decimal(buffer_pct),
            "cgt_scenario": cgt,
            "updated_at": datetime(2026, 4, 15, tzinfo=UTC),
            "updated_by": "system",
            "reason": "seed",
        })

    def _cash_cursor(self, balance: str | None) -> MagicMock:
        val = Decimal(balance) if balance is not None else None
        return _make_cursor(single={"balance": val})

    def _deployed_cursor(self, total: str = "0") -> MagicMock:
        return _make_cursor(single={"deployed": Decimal(total)})

    def _mirror_cursor(self, equity: str = "0") -> MagicMock:
        return _make_cursor(single={"mirror_equity": Decimal(equity)})

    def _tax_cursor(self, basic: str = "0", higher: str = "0") -> MagicMock:
        return _make_cursor(single={
            "estimated_cgt_basic": Decimal(basic),
            "estimated_cgt_higher": Decimal(higher),
        })

    def _fx_cursor(self, rate: str | None = "1.27") -> MagicMock:
        val = Decimal(rate) if rate is not None else None
        return _make_cursor(single={"rate": val} if val is not None else None)

    def test_full_budget_computation(self) -> None:
        """cash=10000, deployed=5000, mirrors=2000, tax_higher_gbp=500, rate=1.27, buffer=5%"""
        conn = _make_conn([
            self._config_cursor(),         # 0: config
            self._cash_cursor("10000"),     # 1: cash
            self._deployed_cursor("5000"),  # 2: deployed
            self._mirror_cursor("2000"),    # 3: mirrors
            self._tax_cursor(higher="500"), # 4: tax
            self._fx_cursor("1.27"),        # 5: fx
        ])

        state = compute_budget_state(conn)

        assert state.cash_balance == Decimal("10000")
        assert state.deployed_capital == Decimal("5000")
        assert state.mirror_equity == Decimal("2000")
        # working_budget = 10000 + 5000 + 2000 = 17000
        assert state.working_budget == Decimal("17000")
        # tax_usd = 500 * 1.27 = 635
        assert state.estimated_tax_gbp == Decimal("500")
        assert state.estimated_tax_usd == Decimal("635")
        # buffer = 17000 * 0.05 = 850
        assert state.cash_buffer_reserve == Decimal("850")
        # available = 10000 - 635 - 850 = 8515
        assert state.available_for_deployment == Decimal("8515")

    def test_unknown_cash_returns_none_available(self) -> None:
        """Empty cash_ledger → cash=None, available=None."""
        conn = _make_conn([
            self._config_cursor(),
            self._cash_cursor(None),
            self._deployed_cursor("5000"),
            self._mirror_cursor("0"),
            self._tax_cursor(),
            self._fx_cursor(),
        ])

        state = compute_budget_state(conn)

        assert state.cash_balance is None
        assert state.working_budget is None
        assert state.available_for_deployment is None

    def test_no_fx_rate_uses_zero_tax(self) -> None:
        """Missing GBP→USD rate → tax_usd=0, logged as warning."""
        conn = _make_conn([
            self._config_cursor(),
            self._cash_cursor("10000"),
            self._deployed_cursor("0"),
            self._mirror_cursor("0"),
            self._tax_cursor(higher="500"),
            self._fx_cursor(None),
        ])

        state = compute_budget_state(conn)

        assert state.gbp_usd_rate is None
        assert state.estimated_tax_usd == Decimal("0")
        # buffer = 10000 * 0.05 = 500
        # available = 10000 - 0 - 500 = 9500
        assert state.available_for_deployment == Decimal("9500")

    def test_negative_available_when_over_reserved(self) -> None:
        """Tax + buffer > cash → available is negative (no new orders)."""
        conn = _make_conn([
            self._config_cursor(buffer_pct="0.10"),
            self._cash_cursor("1000"),
            self._deployed_cursor("20000"),
            self._mirror_cursor("0"),
            self._tax_cursor(higher="5000"),
            self._fx_cursor("1.27"),
        ])

        state = compute_budget_state(conn)

        # working_budget = 1000 + 20000 + 0 = 21000
        # buffer = 21000 * 0.10 = 2100
        # tax_usd = 5000 * 1.27 = 6350
        # available = 1000 - 6350 - 2100 = -7450
        assert state.available_for_deployment is not None
        assert state.available_for_deployment < 0

    def test_basic_cgt_scenario_uses_basic_estimate(self) -> None:
        """cgt_scenario='basic' reads the basic estimate, not higher."""
        conn = _make_conn([
            self._config_cursor(cgt="basic"),
            self._cash_cursor("10000"),
            self._deployed_cursor("0"),
            self._mirror_cursor("0"),
            self._tax_cursor(basic="300", higher="500"),
            self._fx_cursor("1.27"),
        ])

        state = compute_budget_state(conn)

        assert state.estimated_tax_gbp == Decimal("300")
        assert state.cgt_scenario == "basic"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_budget.py::TestComputeBudgetState -v`
Expected: FAIL — `ImportError: cannot import name 'compute_budget_state'`

- [ ] **Step 3: Implement compute_budget_state()**

Add to `app/services/budget.py`:

```python
def _current_uk_tax_year() -> str:
    """Return the current UK tax year string (e.g. '2025/26').

    UK tax year runs 6 April to 5 April.
    """
    now = datetime.now(tz=UTC)
    year = now.year
    month = now.month
    day = now.day
    if month < 4 or (month == 4 and day <= 5):
        start_year = year - 1
    else:
        start_year = year
    return f"{start_year}/{str(start_year + 1)[-2:]}"


def _load_cash_balance(conn: psycopg.Connection[Any]) -> Decimal | None:
    """SUM(cash_ledger.amount) or None if ledger is empty."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute("SELECT SUM(amount) AS balance FROM cash_ledger")
        row = cur.fetchone()
    # Aggregate always returns one row; column is NULL when table is empty.
    if row is None or row["balance"] is None:
        return None
    return Decimal(str(row["balance"]))


def _load_deployed_capital(conn: psycopg.Connection[Any]) -> Decimal:
    """SUM(cost_basis) for open positions (current_units > 0)."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT COALESCE(SUM(cost_basis), 0) AS deployed
            FROM positions
            WHERE current_units > 0
            """
        )
        row = cur.fetchone()
    # Aggregate always returns one row.
    if row is None:
        raise RuntimeError("deployed capital aggregate returned no rows")
    return Decimal(str(row["deployed"]))


def _load_mirror_equity(conn: psycopg.Connection[Any]) -> Decimal:
    """Total mirror equity from copy-trading mirrors.

    Mirrors that are paused or have no positions still contribute their
    available_amount to the equity figure (funds are allocated to the mirror).
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT COALESCE(
                (SELECT SUM(cm.available_amount) FROM copy_mirrors cm
                 WHERE cm.status = 'active')
                +
                (SELECT COALESCE(SUM(cmp.current_value), 0)
                 FROM copy_mirror_positions cmp
                 JOIN copy_mirrors cm2 ON cm2.mirror_id = cmp.mirror_id
                 WHERE cm2.status = 'active'),
                0
            ) AS mirror_equity
            """
        )
        row = cur.fetchone()
    if row is None:
        raise RuntimeError("mirror equity aggregate returned no rows")
    return Decimal(str(row["mirror_equity"]))


def _load_tax_estimates(
    conn: psycopg.Connection[Any],
    tax_year: str,
) -> tuple[Decimal, Decimal]:
    """Load estimated CGT for both scenarios from disposal_matches.

    Returns (basic_estimate_gbp, higher_estimate_gbp).
    Both are zero if no disposals exist for the tax year.

    This is a simplified read — it replicates the tax_year_summary logic
    for just the CGT estimates, avoiding a circular import of tax_ledger.
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT
                COALESCE(SUM(CASE WHEN gain_or_loss_gbp > 0
                    THEN gain_or_loss_gbp ELSE 0 END), 0) AS total_gains,
                COALESCE(SUM(gain_or_loss_gbp), 0) AS net_gain
            FROM disposal_matches
            WHERE tax_year = %(ty)s
            """,
            {"ty": tax_year},
        )
        agg = cur.fetchone()
    if agg is None:
        raise RuntimeError("tax aggregate returned no rows")

    net_gain = Decimal(str(agg["net_gain"]))
    total_gains = Decimal(str(agg["total_gains"]))

    # Apply annual exemption
    from app.services.tax_ledger import ANNUAL_EXEMPT

    taxable_net = max(net_gain - ANNUAL_EXEMPT, _ZERO)
    if total_gains <= 0 or taxable_net <= 0:
        return _ZERO, _ZERO

    # Simplified: use current-year rates (post-Autumn-Budget)
    # For more accurate per-disposal weighting, callers should use
    # tax_year_summary() directly. This is sufficient for budget estimates.
    basic_rate = Decimal("0.18")
    higher_rate = Decimal("0.24")

    scale = taxable_net / total_gains
    basic_est = total_gains * basic_rate * scale
    higher_est = total_gains * higher_rate * scale

    return basic_est, higher_est


def _load_gbp_usd_rate(conn: psycopg.Connection[Any]) -> Decimal | None:
    """Latest GBP→USD rate from live_fx_rates, or None if unavailable."""
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT rate
            FROM live_fx_rates
            WHERE from_currency = 'GBP' AND to_currency = 'USD'
            """
        )
        row = cur.fetchone()
    if row is None or row["rate"] is None:
        return None
    return Decimal(str(row["rate"]))


def compute_budget_state(conn: psycopg.Connection[Any]) -> BudgetState:
    """Compute the current budget state from all source tables.

    This is the core function — called by the API and execution guard.
    Reads from: budget_config, cash_ledger, positions, copy_mirrors,
    disposal_matches, live_fx_rates.

    Returns a frozen snapshot. Never caches — always reads fresh state.
    """
    config = get_budget_config(conn)
    tax_year = _current_uk_tax_year()

    # 1. Cash balance
    cash_balance = _load_cash_balance(conn)

    # 2. Deployed capital (open positions)
    deployed_capital = _load_deployed_capital(conn)

    # 3. Mirror equity
    mirror_equity = _load_mirror_equity(conn)

    # 4. Working budget
    if cash_balance is not None:
        working_budget: Decimal | None = cash_balance + deployed_capital + mirror_equity
    else:
        working_budget = None

    # 5. Tax provision
    basic_tax_gbp, higher_tax_gbp = _load_tax_estimates(conn, tax_year)
    if config.cgt_scenario == "basic":
        estimated_tax_gbp = basic_tax_gbp
    else:
        estimated_tax_gbp = higher_tax_gbp

    # Convert GBP tax estimate to USD
    gbp_usd_rate = _load_gbp_usd_rate(conn)
    if gbp_usd_rate is not None and gbp_usd_rate > 0:
        estimated_tax_usd = estimated_tax_gbp * gbp_usd_rate
    else:
        # No FX rate available — cannot convert. Log warning, treat as zero
        # to avoid blocking all trades on missing FX data.
        if estimated_tax_gbp > 0:
            logger.warning(
                "No GBP→USD rate available; tax provision of %.2f GBP "
                "cannot be converted — treating as 0 USD for budget",
                estimated_tax_gbp,
            )
        estimated_tax_usd = _ZERO

    # 6. Cash buffer reserve
    if working_budget is not None:
        cash_buffer_reserve = working_budget * config.cash_buffer_pct
    else:
        cash_buffer_reserve = _ZERO

    # 7. Available for deployment
    if cash_balance is not None:
        available_for_deployment: Decimal | None = (
            cash_balance - estimated_tax_usd - cash_buffer_reserve
        )
    else:
        available_for_deployment = None

    return BudgetState(
        cash_balance=cash_balance,
        deployed_capital=deployed_capital,
        mirror_equity=mirror_equity,
        working_budget=working_budget,
        estimated_tax_gbp=estimated_tax_gbp,
        estimated_tax_usd=estimated_tax_usd,
        gbp_usd_rate=gbp_usd_rate,
        cash_buffer_reserve=cash_buffer_reserve,
        available_for_deployment=available_for_deployment,
        cash_buffer_pct=config.cash_buffer_pct,
        cgt_scenario=config.cgt_scenario,
        tax_year=tax_year,
    )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_budget.py::TestComputeBudgetState -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add app/services/budget.py tests/test_budget.py
git commit -m "feat(#203): compute_budget_state() — core budget computation"
```

---

## Task 6: API Endpoints

**Files:**
- Create: `app/api/budget.py`
- Modify: `app/main.py` (register router)

- [ ] **Step 1: Write the API router**

Create `app/api/budget.py`:

```python
"""Budget and capital management API endpoints (issue #203).

Endpoints:
  - GET    /budget          — current budget state snapshot
  - GET    /budget/events   — list capital events (paginated)
  - POST   /budget/events   — record a capital event (injection/withdrawal)
  - PATCH  /budget/config   — update budget configuration

All endpoints require operator auth.
"""

from __future__ import annotations

import logging
from datetime import datetime
from decimal import Decimal
from typing import Literal

import psycopg
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from app.api.auth import require_session_or_service_token
from app.db import get_conn
from app.services.budget import (
    BudgetConfigCorrupt,
    BudgetState,
    CapitalEvent,
    compute_budget_state,
    get_budget_config,
    list_capital_events,
    record_capital_event,
    update_budget_config,
)

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/budget",
    tags=["budget"],
    dependencies=[Depends(require_session_or_service_token)],
)


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------


class BudgetStateResponse(BaseModel):
    cash_balance: float | None
    deployed_capital: float
    mirror_equity: float
    working_budget: float | None
    estimated_tax_gbp: float
    estimated_tax_usd: float
    gbp_usd_rate: float | None
    cash_buffer_reserve: float
    available_for_deployment: float | None
    cash_buffer_pct: float
    cgt_scenario: str
    tax_year: str


class CapitalEventResponse(BaseModel):
    event_id: int
    event_time: datetime
    event_type: str
    amount: float
    currency: str
    source: str
    note: str | None
    created_by: str | None


class BudgetConfigResponse(BaseModel):
    cash_buffer_pct: float
    cgt_scenario: str
    updated_at: datetime
    updated_by: str
    reason: str


# ---------------------------------------------------------------------------
# Request models
# ---------------------------------------------------------------------------


class CreateCapitalEventRequest(BaseModel):
    event_type: Literal["injection", "withdrawal"]
    amount: float = Field(gt=0)
    currency: str = "USD"
    note: str | None = None


class UpdateBudgetConfigRequest(BaseModel):
    cash_buffer_pct: float | None = Field(default=None, ge=0, le=0.50)
    cgt_scenario: Literal["basic", "higher"] | None = None
    updated_by: str
    reason: str


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get("", response_model=BudgetStateResponse)
def get_budget(conn: psycopg.Connection = Depends(get_conn)) -> BudgetStateResponse:
    """Current budget state snapshot."""
    try:
        state = compute_budget_state(conn)
    except BudgetConfigCorrupt:
        raise HTTPException(status_code=503, detail="budget configuration unavailable")
    return BudgetStateResponse(
        cash_balance=float(state.cash_balance) if state.cash_balance is not None else None,
        deployed_capital=float(state.deployed_capital),
        mirror_equity=float(state.mirror_equity),
        working_budget=float(state.working_budget) if state.working_budget is not None else None,
        estimated_tax_gbp=float(state.estimated_tax_gbp),
        estimated_tax_usd=float(state.estimated_tax_usd),
        gbp_usd_rate=float(state.gbp_usd_rate) if state.gbp_usd_rate is not None else None,
        cash_buffer_reserve=float(state.cash_buffer_reserve),
        available_for_deployment=float(state.available_for_deployment) if state.available_for_deployment is not None else None,
        cash_buffer_pct=float(state.cash_buffer_pct),
        cgt_scenario=state.cgt_scenario,
        tax_year=state.tax_year,
    )


@router.get("/events", response_model=list[CapitalEventResponse])
def get_capital_events(
    conn: psycopg.Connection = Depends(get_conn),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
) -> list[CapitalEventResponse]:
    """List capital events, most recent first."""
    events = list_capital_events(conn, limit=limit, offset=offset)
    return [
        CapitalEventResponse(
            event_id=e.event_id,
            event_time=e.event_time,
            event_type=e.event_type,
            amount=float(e.amount),
            currency=e.currency,
            source=e.source,
            note=e.note,
            created_by=e.created_by,
        )
        for e in events
    ]


@router.post("/events", response_model=CapitalEventResponse, status_code=201)
def create_capital_event(
    body: CreateCapitalEventRequest,
    conn: psycopg.Connection = Depends(get_conn),
) -> CapitalEventResponse:
    """Record a capital event (deposit or withdrawal).

    Only 'injection' and 'withdrawal' are operator-creatable.
    'tax_provision' and 'tax_release' are system-only.
    """
    event = record_capital_event(
        conn,
        event_type=body.event_type,
        amount=Decimal(str(body.amount)),
        currency=body.currency,
        note=body.note,
        created_by="operator",
        source="operator",
    )
    conn.commit()
    return CapitalEventResponse(
        event_id=event.event_id,
        event_time=event.event_time,
        event_type=event.event_type,
        amount=float(event.amount),
        currency=event.currency,
        source=event.source,
        note=event.note,
        created_by=event.created_by,
    )


@router.patch("/config", response_model=BudgetConfigResponse)
def patch_budget_config(
    body: UpdateBudgetConfigRequest,
    conn: psycopg.Connection = Depends(get_conn),
) -> BudgetConfigResponse:
    """Update budget configuration."""
    if body.cash_buffer_pct is None and body.cgt_scenario is None:
        raise HTTPException(status_code=422, detail="at least one field must be provided")

    try:
        config = update_budget_config(
            conn,
            cash_buffer_pct=Decimal(str(body.cash_buffer_pct)) if body.cash_buffer_pct is not None else None,
            cgt_scenario=body.cgt_scenario,
            updated_by=body.updated_by,
            reason=body.reason,
        )
    except BudgetConfigCorrupt:
        raise HTTPException(status_code=503, detail="budget configuration unavailable")
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    conn.commit()
    return BudgetConfigResponse(
        cash_buffer_pct=float(config.cash_buffer_pct),
        cgt_scenario=config.cgt_scenario,
        updated_at=config.updated_at,
        updated_by=config.updated_by,
        reason=config.reason,
    )


@router.get("/config", response_model=BudgetConfigResponse)
def get_budget_config_endpoint(
    conn: psycopg.Connection = Depends(get_conn),
) -> BudgetConfigResponse:
    """Current budget configuration."""
    try:
        config = get_budget_config(conn)
    except BudgetConfigCorrupt:
        raise HTTPException(status_code=503, detail="budget configuration unavailable")
    return BudgetConfigResponse(
        cash_buffer_pct=float(config.cash_buffer_pct),
        cgt_scenario=config.cgt_scenario,
        updated_at=config.updated_at,
        updated_by=config.updated_by,
        reason=config.reason,
    )
```

- [ ] **Step 2: Register the router in main.py**

Add to `app/main.py` imports:

```python
from app.api.budget import router as budget_router
```

Add to the router registration block:

```python
app.include_router(budget_router)
```

- [ ] **Step 3: Run the smoke test to verify the app boots**

Run: `uv run pytest tests/smoke/test_app_boots.py -v`
Expected: PASS (migration 027 must be applied to dev DB first)

- [ ] **Step 4: Commit**

```bash
git add app/api/budget.py app/main.py
git commit -m "feat(#203): budget API — GET /budget, events CRUD, config PATCH"
```

---

## Task 7: Execution Guard Integration

Replace the raw `_check_cash` rule with a budget-aware `_check_budget` rule.

**Files:**
- Modify: `app/services/execution_guard.py`
- Modify: `tests/test_execution_guard.py`

- [ ] **Step 1: Read the current execution guard cash check**

Read `app/services/execution_guard.py` around lines 215-222 (`_load_cash`) and lines 410-430 (`_check_cash` and its usage in the rule list).

- [ ] **Step 2: Write the failing test for the budget-aware check**

Add to `tests/test_execution_guard.py`:

```python
class TestBudgetAwareCheck:
    """The execution guard should use available_for_deployment, not raw cash."""

    @patch("app.services.execution_guard.compute_budget_state")
    def test_passes_when_budget_available(self, mock_budget: MagicMock) -> None:
        from app.services.budget import BudgetState
        mock_budget.return_value = BudgetState(
            cash_balance=Decimal("10000"),
            deployed_capital=Decimal("5000"),
            mirror_equity=Decimal("0"),
            working_budget=Decimal("15000"),
            estimated_tax_gbp=Decimal("0"),
            estimated_tax_usd=Decimal("0"),
            gbp_usd_rate=Decimal("1.27"),
            cash_buffer_reserve=Decimal("750"),
            available_for_deployment=Decimal("9250"),
            cash_buffer_pct=Decimal("0.05"),
            cgt_scenario="higher",
            tax_year="2025/26",
        )
        from app.services.execution_guard import _check_budget
        result = _check_budget(mock_budget.return_value)
        assert result.passed is True

    @patch("app.services.execution_guard.compute_budget_state")
    def test_fails_when_budget_exhausted(self, mock_budget: MagicMock) -> None:
        from app.services.budget import BudgetState
        mock_budget.return_value = BudgetState(
            cash_balance=Decimal("1000"),
            deployed_capital=Decimal("20000"),
            mirror_equity=Decimal("0"),
            working_budget=Decimal("21000"),
            estimated_tax_gbp=Decimal("5000"),
            estimated_tax_usd=Decimal("6350"),
            gbp_usd_rate=Decimal("1.27"),
            cash_buffer_reserve=Decimal("2100"),
            available_for_deployment=Decimal("-7450"),
            cash_buffer_pct=Decimal("0.10"),
            cgt_scenario="higher",
            tax_year="2025/26",
        )
        from app.services.execution_guard import _check_budget
        result = _check_budget(mock_budget.return_value)
        assert result.passed is False
        assert "budget" in result.detail.lower() or "available" in result.detail.lower()

    @patch("app.services.execution_guard.compute_budget_state")
    def test_fails_when_cash_unknown(self, mock_budget: MagicMock) -> None:
        from app.services.budget import BudgetState
        mock_budget.return_value = BudgetState(
            cash_balance=None,
            deployed_capital=Decimal("0"),
            mirror_equity=Decimal("0"),
            working_budget=None,
            estimated_tax_gbp=Decimal("0"),
            estimated_tax_usd=Decimal("0"),
            gbp_usd_rate=None,
            cash_buffer_reserve=Decimal("0"),
            available_for_deployment=None,
            cash_buffer_pct=Decimal("0.05"),
            cgt_scenario="higher",
            tax_year="2025/26",
        )
        from app.services.execution_guard import _check_budget
        result = _check_budget(mock_budget.return_value)
        assert result.passed is False
        assert "unknown" in result.detail.lower()
```

- [ ] **Step 3: Run test to verify it fails**

Run: `uv run pytest tests/test_execution_guard.py::TestBudgetAwareCheck -v`
Expected: FAIL — `ImportError: cannot import name '_check_budget'`

- [ ] **Step 4: Replace _check_cash with _check_budget in execution_guard.py**

In `app/services/execution_guard.py`:

1. Add import at top:
```python
from app.services.budget import BudgetState, compute_budget_state
```

2. Replace `_check_cash` function:
```python
def _check_budget(budget: BudgetState) -> RuleResult:
    """Check budget availability for BUY/ADD orders.

    Replaces the raw cash > 0 check with budget-aware logic that accounts
    for tax provisions and cash buffer reserve.

    Fail-closed: unknown cash (None) or exhausted budget (≤ 0) fails.
    """
    if budget.available_for_deployment is None:
        return RuleResult(
            rule="budget_available",
            passed=False,
            detail="cash_ledger is empty; cannot verify budget availability",
        )
    if budget.available_for_deployment <= 0:
        return RuleResult(
            rule="budget_available",
            passed=False,
            detail=(
                f"budget exhausted: available_for_deployment="
                f"{budget.available_for_deployment:.2f} "
                f"(cash={budget.cash_balance}, tax_reserved={budget.estimated_tax_usd:.2f}, "
                f"buffer={budget.cash_buffer_reserve:.2f})"
            ),
        )
    return RuleResult(
        rule="budget_available",
        passed=True,
        detail=(
            f"budget ok: available_for_deployment="
            f"{budget.available_for_deployment:.2f}"
        ),
    )
```

3. In `evaluate_recommendation()`, replace the cash-loading and checking:

Where the guard currently does:
```python
cash = _load_cash(conn)
# ... later in rule list for BUY/ADD:
rules.append(_check_cash(cash))
```

Change to:
```python
budget = compute_budget_state(conn)
# ... later in rule list for BUY/ADD:
rules.append(_check_budget(budget))
```

4. Include the budget snapshot in `evidence_json` for auditability:
```python
"budget_snapshot": {
    "cash_balance": float(budget.cash_balance) if budget.cash_balance is not None else None,
    "available_for_deployment": float(budget.available_for_deployment) if budget.available_for_deployment is not None else None,
    "estimated_tax_usd": float(budget.estimated_tax_usd),
    "cash_buffer_reserve": float(budget.cash_buffer_reserve),
    "cash_buffer_pct": float(budget.cash_buffer_pct),
    "cgt_scenario": budget.cgt_scenario,
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest tests/test_execution_guard.py -v`
Expected: PASS (existing tests may need mock updates for the new budget call)

- [ ] **Step 6: Update existing execution guard tests**

Existing tests that mock `_load_cash` will need updating to either:
- Mock `compute_budget_state` instead, or
- Provide budget state where the old cash check was

Review each test class in `tests/test_execution_guard.py` and update accordingly.

- [ ] **Step 7: Commit**

```bash
git add app/services/execution_guard.py tests/test_execution_guard.py
git commit -m "feat(#203): execution guard — budget-aware cash check replaces raw cash"
```

---

## Task 8: Full Check Suite

**Files:** None new — verification only.

- [ ] **Step 1: Run linter**

Run: `uv run ruff check .`
Expected: no errors

- [ ] **Step 2: Run formatter check**

Run: `uv run ruff format --check .`
Expected: no formatting issues

- [ ] **Step 3: Run type checker**

Run: `uv run pyright`
Expected: no errors

- [ ] **Step 4: Run full test suite**

Run: `uv run pytest`
Expected: all tests pass, including smoke test

- [ ] **Step 5: Fix any issues found in steps 1-4**

Address all lint, type, and test failures before proceeding.

- [ ] **Step 6: Final commit if fixes were needed**

```bash
git add -u
git commit -m "fix(#203): address lint/type/test issues"
```

---

## Task 9: PR and Review

- [ ] **Step 1: Self-review the diff**

Read `.claude/skills/engineering/pre-flight-review.md` and review the full diff against it.

- [ ] **Step 2: Push and open PR**

```bash
git push -u origin feature/203-budget-capital-management
gh pr create --title "feat(#203): budget and capital management service" --body "..."
```

- [ ] **Step 3: Poll review + CI, resolve all comments, iterate until APPROVE**

Follow the branch and PR workflow from CLAUDE.md:
1. `gh pr checks <n>` — wait for CI green
2. `gh pr view <n> --comments` — read review
3. Address every comment (FIXED/DEFERRED/REBUTTED)
4. Re-run checks, push follow-up
5. Repeat until APPROVE on latest commit

- [ ] **Step 4: Merge and clean up**

```bash
gh pr merge <n> --squash --delete-branch
git checkout main && git pull
gh issue close 203
```
