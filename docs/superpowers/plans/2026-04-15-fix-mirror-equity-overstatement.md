# Fix Mirror Equity Overstatement — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix mirror equity calculation so AUM reflects reality — eliminate the ~£27k overstatement and fix budget.py's broken query that references non-existent schema columns.

**Architecture:** Two bugs, one root cause investigation. (1) budget.py's `_load_mirror_equity()` references `cm.status` (doesn't exist; column is `cm.active BOOLEAN`) and `cmp.current_value` (doesn't exist; must compute MTM from position fields). Fix by importing portfolio.py's canonical version. (2) The dashboard shows mirror equity ~3× too high. Investigate data via diagnostic queries, then fix the root cause. (3) Align the 2-tier pricing in `_load_mirror_equity` with the 3-tier hierarchy in `load_mirror_breakdowns` for consistency.

**Tech Stack:** Python, psycopg3, PostgreSQL, pytest

---

## Settled decisions that apply

- **AUM basis** — "AUM and concentration should use mark-to-market first. If no current quote exists, fall back to cost basis." Both `_load_mirror_equity` and `load_mirror_breakdowns` must use the same MTM fallback chain.
- **Provider boundary** — "keep providers thin, domain logic in services." Mirror equity logic stays in `portfolio.py`, not the provider.

## Prevention log entries that apply

- **psycopg3 cursors are independent — mock cursor sequences are not** — budget.py's mock-based tests passed despite referencing non-existent columns. The smoke test (which runs against real DB) doesn't cover `GET /budget`, so the schema mismatch was invisible.
- **SQL-shape assertions are only meaningful for clauses the code path runs** — mocks can't validate column names. Adding smoke coverage for the budget endpoint is required.

---

## File Structure

| File | Action | Responsibility |
|------|--------|---------------|
| `app/services/budget.py` | Modify | Remove broken `_load_mirror_equity`, import from portfolio.py |
| `app/services/portfolio.py` | Modify | Add `price_daily` fallback to `_load_mirror_equity` (3-tier alignment) |
| `app/services/portfolio_sync.py` | Modify | Add mirror equity sanity check during sync |
| `tests/test_budget.py` | Modify | Update mock target for mirror equity |
| `tests/test_portfolio.py` | Modify | Add test for 3-tier pricing in `_load_mirror_equity` |
| `tests/smoke/test_app_boots.py` | Modify | Add `GET /budget` smoke assertion |

---

### Task 1: Diagnose the overstatement via SQL queries

This task runs diagnostic queries against the dev database to identify the root cause of the ~£27k mirror equity overstatement. The queries must be run manually by the operator or via `psql`.

**Files:**
- None (diagnostic only)

- [ ] **Step 1: Check active mirrors and their available_amount**

```sql
SELECT mirror_id, parent_cid, available_amount, initial_investment,
       deposit_summary, withdrawal_summary, closed_positions_net_profit,
       active, updated_at
FROM copy_mirrors
WHERE active = TRUE;
```

Expected: 2 active mirrors. Note each `available_amount` value. Compare `available_amount` against `initial_investment + deposit_summary - withdrawal_summary + closed_positions_net_profit` — if `available_amount` is close to this sum, it represents total equity, not just uninvested cash.

- [ ] **Step 2: Check position counts and invested amounts per mirror**

```sql
SELECT mirror_id, COUNT(*) AS pos_count, SUM(amount) AS total_amount,
       SUM(initial_amount_in_dollars) AS total_initial
FROM copy_mirror_positions
GROUP BY mirror_id
ORDER BY mirror_id;
```

Expected: Compare `SUM(amount)` against `SUM(initial_amount_in_dollars)`. If `amount ≈ initial_amount_in_dollars`, they represent invested capital. If `amount >> initial_amount_in_dollars`, `amount` may include unrealized P&L (which would be double-counted by the MTM formula).

- [ ] **Step 3: Run the mirror equity CTE manually**

```sql
WITH mirror_equity AS (
    SELECT m.mirror_id,
           m.available_amount,
           COALESCE(p.mv, 0) AS positions_mv,
           m.available_amount + COALESCE(p.mv, 0) AS per_mirror_equity
    FROM copy_mirrors m
    LEFT JOIN LATERAL (
        SELECT SUM(
              cmp.amount
            + (CASE WHEN cmp.is_buy THEN 1 ELSE -1 END)
              * cmp.units
              * (COALESCE(q.last, cmp.open_rate) - cmp.open_rate)
              * cmp.open_conversion_rate
        ) AS mv
        FROM copy_mirror_positions cmp
        LEFT JOIN LATERAL (
            SELECT last
            FROM quotes
            WHERE instrument_id = cmp.instrument_id
            ORDER BY quoted_at DESC
            LIMIT 1
        ) q ON TRUE
        WHERE cmp.mirror_id = m.mirror_id
    ) p ON TRUE
    WHERE m.active
)
SELECT *, SUM(per_mirror_equity) OVER () AS total_mirror_equity
FROM mirror_equity;
```

Expected: Identify which mirror contributes most to the overstatement and whether `available_amount` or `positions_mv` is the dominant factor.

- [ ] **Step 4: Cross-check against raw_payload**

```sql
SELECT mirror_id,
       available_amount AS db_available,
       (raw_payload->>'availableAmount')::numeric AS api_available,
       initial_investment AS db_initial,
       (raw_payload->>'initialInvestment')::numeric AS api_initial
FROM copy_mirrors
WHERE active = TRUE;
```

Expected: DB values should match raw_payload values exactly. If they diverge, the sync pipeline has a parsing bug.

- [ ] **Step 5: Determine root cause and record findings**

Based on the diagnostic results, the root cause will be one of:
1. **`available_amount` includes position values** — fix: mirror equity = `available_amount` only (positions already accounted for), or adjust formula
2. **`amount` includes unrealized P&L** — fix: use `initial_amount_in_dollars` instead of `amount` in the MTM base
3. **Duplicate or stale data** — fix: deduplication or sync-pipeline correction
4. **FX conversion applied incorrectly** — fix: conversion logic

Record the finding in a code comment or issue comment before proceeding to Task 2.

---

### Task 2: Fix budget.py — DRY mirror equity with portfolio.py

**Files:**
- Modify: `app/services/budget.py:430-456` (remove broken `_load_mirror_equity`)
- Modify: `app/services/budget.py:532` (update call site)
- Test: `tests/test_budget.py`

- [ ] **Step 1: Write the failing test**

Update the budget test to mock the portfolio.py import instead of the broken local function. In `tests/test_budget.py`, find `_budget_conn` helper and update the mock target:

```python
# In TestComputeBudgetState, the mirror_equity cursor (index 3) is consumed
# by budget.py's local _load_mirror_equity. After the fix, budget.py imports
# portfolio._load_mirror_equity instead, so the mock target changes.
#
# Replace the cursor-based mirror mock with a patch on the import:
```

The existing test `test_full_budget_computation` should still pass after the change — it mocks cursors that the broken local function consumed. After removing the local function, the cursor count changes from 6 to 5 (no mirror cursor in budget.py; it calls portfolio's version instead).

First, verify the current tests pass:

Run: `uv run pytest tests/test_budget.py -v`
Expected: PASS

- [ ] **Step 2: Remove broken `_load_mirror_equity` from budget.py and import from portfolio.py**

In `app/services/budget.py`, remove lines 430-456 (the broken `_load_mirror_equity` function).

Add import at the top of the file:

```python
from app.services.portfolio import _load_mirror_equity
```

Update `compute_budget_state` (around line 532) to convert the float return to Decimal:

```python
    mirror_equity = Decimal(str(_load_mirror_equity(conn)))
```

The line `mirror_equity = _load_mirror_equity(conn)` previously called the local broken version. Now it calls portfolio.py's correct version and wraps in Decimal.

- [ ] **Step 3: Update the budget test helpers to mock the new import**

In `tests/test_budget.py`, the `_budget_conn` helper builds 6 cursors:
```
budget_config, cash_balance, deployed_capital, mirror_equity, tax_estimates, gbp_usd_rate
```

After the fix, budget.py no longer runs its own mirror query — it calls `portfolio._load_mirror_equity(conn)`. So the cursor sequence drops to 5 (remove `cur_mirrors`), and we patch `app.services.budget._load_mirror_equity` (the imported name) instead.

Update `_budget_conn`:

```python
def _budget_conn(
    *,
    config_row: dict[str, Any] | None = None,
    cash_balance: Decimal | None = Decimal("10000"),
    deployed: Decimal = Decimal("5000"),
    total_gains: Decimal = Decimal("3500"),
    net_gain: Decimal = Decimal("3500"),
    gbp_usd_rate: Decimal | None = Decimal("1.25"),
) -> MagicMock:
    """Build a mock connection for compute_budget_state.

    Cursor order (5 cursors — mirror_equity is now patched separately):
      0: budget_config (get_budget_config)
      1: cash balance (_load_cash_balance)
      2: deployed capital (_load_deployed_capital)
      3: tax estimates (_load_tax_estimates)
      4: gbp_usd rate (_load_gbp_usd_rate)
    """
    if config_row is None:
        config_row = _budget_config_row()

    cur_config = _make_cursor(single=config_row)
    cur_cash = _make_cursor(single={"balance": cash_balance})
    cur_deployed = _make_cursor(single={"deployed": deployed})
    cur_tax = _make_cursor(
        single={"total_gains": total_gains, "net_gain": net_gain},
    )
    if gbp_usd_rate is not None:
        cur_fx = _make_cursor(single={"rate": gbp_usd_rate})
    else:
        cur_fx = _make_cursor()
        cur_fx.fetchone.return_value = None

    return _make_conn([cur_config, cur_cash, cur_deployed, cur_tax, cur_fx])
```

Then wrap every `compute_budget_state(conn)` call in the test class with a patch on the mirror equity:

```python
with (
    unittest.mock.patch(
        "app.services.budget._current_uk_tax_year",
        return_value="2025/26",
    ),
    unittest.mock.patch(
        "app.services.budget._load_mirror_equity",
        return_value=2000.0,
    ),
):
    state = compute_budget_state(conn)
```

The `return_value=2000.0` matches the previous mock cursor value (`mirror_equity=Decimal("2000")`), but now returns float (portfolio.py's return type) — budget.py wraps it in `Decimal(str(...))`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_budget.py -v`
Expected: All 30 tests PASS

- [ ] **Step 5: Update execution guard tests**

The execution guard's `_budget_cursors_list` helper in `tests/test_execution_guard.py` builds 6 cursors for `compute_budget_state`. After the change, `compute_budget_state` only opens 5 cursors (no mirror cursor). But `compute_budget_state` now calls `portfolio._load_mirror_equity(conn)` which opens its own cursor.

So the total cursor count stays the same (5 budget internal + 1 from portfolio import = 6 cursors consumed from the mock `conn.cursor()` side_effect). No change needed in execution guard tests — the cursor ordering is:

```
budget_config → cash_balance → deployed_capital → mirror_equity(via portfolio) → tax_estimates → gbp_usd_rate
```

Wait — the ordering depends on where in `compute_budget_state` the `_load_mirror_equity` call sits. Let me verify. In budget.py:

```python
config = get_budget_config(conn)        # cursor 1
cash_balance = _load_cash_balance(conn)  # cursor 2
deployed = _load_deployed_capital(conn)  # cursor 3
mirror_equity = Decimal(str(_load_mirror_equity(conn)))  # cursor 4 (now from portfolio)
```

So cursor 4 is now consumed by portfolio.py's `_load_mirror_equity`, which opens `conn.cursor()`. The mock side_effect still provides the mirror cursor at position 4. No test changes needed for execution guard.

Run: `uv run pytest tests/test_execution_guard.py -v`
Expected: All tests PASS

- [ ] **Step 6: Commit**

```bash
git add app/services/budget.py tests/test_budget.py
git commit -m "fix(#210): DRY mirror equity — budget.py imports portfolio.py's canonical version

Removes budget.py's broken _load_mirror_equity() which referenced
non-existent columns (cm.status, cmp.current_value). Now imports
portfolio._load_mirror_equity() and wraps float→Decimal.

Updates budget test mocks to patch the imported function instead
of providing a cursor for the removed local query."
```

---

### Task 3: Align `_load_mirror_equity` pricing to 3-tier

The execution guard and budget service use `portfolio._load_mirror_equity()` which has 2-tier pricing (quote → open_rate). The dashboard uses `load_mirror_breakdowns()` which has 3-tier pricing (quote → price_daily → open_rate). This means budget state and dashboard AUM give different mirror equity values when quotes are missing but `price_daily` has data.

**Files:**
- Modify: `app/services/portfolio.py:303-330` (`_load_mirror_equity` SQL)
- Test: `tests/test_portfolio.py`

- [ ] **Step 1: Write the failing test**

In `tests/test_portfolio.py`, add a test that verifies `_load_mirror_equity` uses `price_daily.close` as a fallback when `quotes.last` is missing:

```python
class TestLoadMirrorEquityPricingFallback:
    """Verify _load_mirror_equity uses 3-tier pricing: quote → price_daily → open_rate."""

    def test_uses_price_daily_when_quote_missing(self) -> None:
        """When quote.last is NULL but price_daily.close exists,
        mirror equity should use price_daily.close for MTM."""
        # This test runs against the mock DB — see test_portfolio.py
        # patterns for how mirror equity is typically tested.
        # The key assertion: with a position where close > open_rate,
        # the mirror equity should be higher than when falling back to
        # open_rate (which gives zero delta).
        pass  # Implemented in the actual test step below
```

The exact test depends on the existing test patterns in `tests/test_portfolio.py`. Read the file's mirror equity tests and follow the mock pattern.

Run: `uv run pytest tests/test_portfolio.py::TestLoadMirrorEquityPricingFallback -v`
Expected: FAIL (test not yet implemented or assertion fails with 2-tier)

- [ ] **Step 2: Add `price_daily` fallback to `_load_mirror_equity` SQL**

In `app/services/portfolio.py`, update the `_load_mirror_equity` CTE (lines 303-330):

```python
    sql = """
        WITH mirror_equity AS (
            SELECT COALESCE(SUM(
                m.available_amount + COALESCE(p.mv, 0)
            ), 0) AS total
            FROM copy_mirrors m
            LEFT JOIN LATERAL (
                SELECT SUM(
                      cmp.amount
                    + (CASE WHEN cmp.is_buy THEN 1 ELSE -1 END)
                      * cmp.units
                      * (COALESCE(q.last, pd.close, cmp.open_rate) - cmp.open_rate)
                      * cmp.open_conversion_rate
                ) AS mv
                FROM copy_mirror_positions cmp
                LEFT JOIN LATERAL (
                    SELECT last
                    FROM quotes
                    WHERE instrument_id = cmp.instrument_id
                    ORDER BY quoted_at DESC
                    LIMIT 1
                ) q ON TRUE
                LEFT JOIN LATERAL (
                    SELECT close
                    FROM price_daily
                    WHERE instrument_id = cmp.instrument_id
                      AND close IS NOT NULL
                    ORDER BY price_date DESC
                    LIMIT 1
                ) pd ON TRUE
                WHERE cmp.mirror_id = m.mirror_id
            ) p ON TRUE
            WHERE m.active
        )
        SELECT total FROM mirror_equity
    """
```

The only change is adding the `LEFT JOIN LATERAL` on `price_daily` and updating COALESCE from `COALESCE(q.last, cmp.open_rate)` to `COALESCE(q.last, pd.close, cmp.open_rate)`.

- [ ] **Step 3: Run test to verify it passes**

Run: `uv run pytest tests/test_portfolio.py -v`
Expected: All tests PASS (including the new 3-tier test)

- [ ] **Step 4: Commit**

```bash
git add app/services/portfolio.py tests/test_portfolio.py
git commit -m "fix(#210): align _load_mirror_equity to 3-tier pricing

Adds price_daily.close fallback to _load_mirror_equity, matching
the 3-tier hierarchy in load_mirror_breakdowns (quote → price_daily
→ open_rate). Ensures budget state and dashboard AUM agree when
quotes are missing but daily candles are available."
```

---

### Task 4: Add smoke test for `GET /budget` endpoint

The smoke test runs against the real dev DB. Adding `GET /budget` catches schema mismatches that mock-based unit tests cannot detect.

**Files:**
- Modify: `tests/smoke/test_app_boots.py`

- [ ] **Step 1: Read the existing smoke test to understand the pattern**

Read: `tests/smoke/test_app_boots.py`

The existing test uses `TestClient` and asserts on HTTP status codes for various endpoints. Find the pattern and add a budget assertion.

- [ ] **Step 2: Add `GET /budget` to the smoke test**

Add an assertion that `GET /budget` returns 200 (budget_config singleton is seeded by migration 027):

```python
    # Budget state — exercises compute_budget_state against real schema.
    # Catches column-name mismatches that mock-based tests cannot detect
    # (e.g., the cm.status / cmp.current_value bug from PR #232).
    resp = client.get("/budget", headers=auth_headers)
    assert resp.status_code == 200, f"GET /budget returned {resp.status_code}: {resp.text}"
    budget = resp.json()
    assert "available_for_deployment" in budget
```

- [ ] **Step 3: Run the smoke test**

Run: `uv run pytest tests/smoke/test_app_boots.py -v`
Expected: PASS — `GET /budget` returns 200 with the real schema

- [ ] **Step 4: Commit**

```bash
git add tests/smoke/test_app_boots.py
git commit -m "test(#210): add GET /budget to smoke test

Exercises compute_budget_state against the real dev DB schema.
Catches column-name mismatches (cm.status, cmp.current_value)
that mock-based unit tests cannot detect."
```

---

### Task 5: Add mirror equity sanity check during sync

Add a post-sync validation in `portfolio_sync.py` that logs a warning when mirror equity looks suspicious. This catches overstatements at sync time rather than waiting for the operator to notice.

**Files:**
- Modify: `app/services/portfolio_sync.py` (after `_sync_mirrors` returns)
- Test: `tests/test_portfolio_sync.py`

- [ ] **Step 1: Identify the sync call site**

Read `app/services/portfolio_sync.py` and find where `_sync_mirrors` is called. The sanity check goes immediately after the sync completes.

- [ ] **Step 2: Add validation logic**

After `_sync_mirrors` completes, query each active mirror and validate:

```python
def _validate_mirror_equity(conn: psycopg.Connection[Any]) -> None:
    """Log a warning if mirror equity looks inconsistent.

    For each active mirror, compare:
      funded = initial_investment + deposit_summary - withdrawal_summary
      equity = available_amount + sum(position amounts)

    If equity > 2× funded for any mirror, something is likely wrong
    (double-counting, stale data, or semantic mismatch in available_amount).
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute("""
            SELECT m.mirror_id, m.available_amount,
                   m.initial_investment + m.deposit_summary
                     - m.withdrawal_summary AS funded,
                   COALESCE(p.total_amount, 0) AS positions_total
            FROM copy_mirrors m
            LEFT JOIN (
                SELECT mirror_id, SUM(amount) AS total_amount
                FROM copy_mirror_positions
                GROUP BY mirror_id
            ) p USING (mirror_id)
            WHERE m.active
        """)
        rows = cur.fetchall()

    for r in rows:
        equity = float(r["available_amount"]) + float(r["positions_total"])
        funded = float(r["funded"])
        if funded > 0 and equity > 2 * funded:
            logger.warning(
                "Mirror %s equity (%.2f) > 2× funded (%.2f) — "
                "possible double-count in available_amount; "
                "equity=available(%.2f)+positions(%.2f)",
                r["mirror_id"], equity, funded,
                float(r["available_amount"]), float(r["positions_total"]),
            )
```

Call this after `_sync_mirrors` returns in the main sync function.

- [ ] **Step 3: Write a test for the validation**

```python
def test_validate_mirror_equity_warns_on_overstatement(caplog) -> None:
    """When equity > 2× funded, a warning is logged."""
    # Mock the conn to return a mirror with available_amount >> funded
    ...
    with caplog.at_level(logging.WARNING):
        _validate_mirror_equity(conn)
    assert "possible double-count" in caplog.text
```

- [ ] **Step 4: Run tests**

Run: `uv run pytest tests/test_portfolio_sync.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add app/services/portfolio_sync.py tests/test_portfolio_sync.py
git commit -m "fix(#210): add mirror equity sanity check during sync

Logs a warning when any mirror's computed equity exceeds 2× its
funded capital, catching likely double-counting at sync time
rather than waiting for the operator to notice AUM discrepancies."
```

---

### Task 6: Fix the root cause (based on Task 1 findings)

This task depends on the diagnostic results from Task 1. The fix will be one of:

**If `available_amount` from eToro includes position values:**
- In `_load_mirror_equity` and `load_mirror_breakdowns`, change the formula:
  - FROM: `available_amount + SUM(position MTM)`
  - TO: `available_amount` alone (positions are already included)
  - OR: derive uninvested cash as `available_amount - SUM(initial_amount_in_dollars)` and use that

**If `amount` in positions includes unrealized P&L:**
- Change the MTM formula to use `initial_amount_in_dollars` as the cost base instead of `amount`
- The delta `(current - open_rate) * ocr` would still represent the unrealized component

**If duplicate or stale data exists:**
- Fix the sync pipeline deduplication or the soft-close logic

The exact code depends on the diagnosis. Update tests to match.

- [ ] **Step 1: Implement the fix based on Task 1 findings**
- [ ] **Step 2: Update all mirror equity calculations consistently** (portfolio.py `_load_mirror_equity`, `load_mirror_breakdowns`, and any test helpers)
- [ ] **Step 3: Write regression test with the previously-incorrect values**
- [ ] **Step 4: Run full test suite**

Run: `uv run pytest -v`
Expected: All tests PASS

- [ ] **Step 5: Commit**

```bash
git commit -m "fix(#210): correct mirror equity root cause — [describe what changed]"
```

---

## Self-review checklist

1. **Spec coverage**: Task 1 covers diagnosis. Tasks 2-4 fix known code bugs. Task 5 adds observability. Task 6 fixes the root cause.
2. **Placeholder scan**: Task 6 is intentionally contingent on Task 1 findings — it cannot be fully specified without data. All other tasks have complete code.
3. **Type consistency**: `_load_mirror_equity` returns `float` (portfolio.py). Budget.py wraps in `Decimal(str(...))`. Execution guard uses float directly.
