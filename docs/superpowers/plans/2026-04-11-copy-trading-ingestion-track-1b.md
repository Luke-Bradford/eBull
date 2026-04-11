# Copy-Trading Ingestion — Track 1b Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend eBull's AUM computation to include mirror_equity across all three call sites (execution guard, API, portfolio review) so the dashboard, the guard concentration denominator, and the recommendation engine all see the same AUM figure.

**Architecture:** One shared helper `_load_mirror_equity(conn) -> float` in `app/services/portfolio.py` runs the §3.4 CTE SQL once; three call sites import it and add its return value into their existing `total_aum` running totals. Schema, sync, and parser were shipped in Track 1a (#183). This PR is surgical: one new helper, three additive integrations, one new `PortfolioResponse.mirror_equity` field, three new test fixtures, one new §8.4 test module, one new §8.5 test module, one new §8.6 API test module, and one addition to the existing review test suite. No schema changes, no new tables, no new REST endpoints, no frontend UI beyond the TS interface sync.

**Tech Stack:** Python 3.12 / psycopg3 / Postgres 15 / FastAPI / pytest / TypeScript (types only).

**Issue:** #187
**Spec:** `docs/superpowers/specs/2026-04-11-copy-trading-ingestion-design.md` (§3, §6.0–§6.3, §8.0, §8.4–§8.6)
**Branch:** `feature/187-mirror-equity-aum-correction`

---

## Scope boundary

**In scope (this plan):**
- `_load_mirror_equity(conn)` helper in `app/services/portfolio.py`
- Integration at three AUM call sites:
  - `app/services/execution_guard.py:_load_sector_exposure` (line 286)
  - `app/api/portfolio.py:get_portfolio` (line 166) + `PortfolioResponse.mirror_equity` field + frontend `types.ts` sync
  - `app/services/portfolio.py:run_portfolio_review` (line 752-753)
- New fixtures (§8.0): `mirror_aum_fixture`, `no_quote_mirror_fixture`, `mtm_delta_mirror_fixture` in `tests/fixtures/copy_mirrors.py`
- §8.4 AUM identity tests (no-quote cost-basis, MTM delta with FX, short delta, closed mirror excluded, empty-table regression)
- §8.5 guard integration tests (empty baseline, active mirror, closed mirror, sector numerator unchanged)
- §8.6 per-call-site delta tests (API, review, guard symmetry)
- Frontend type sync: `frontend/src/api/types.ts` `PortfolioResponse` interface + 3 fixture literal updates

**Out of scope (other tickets):**
- Track 1.5 (#188) — REST endpoint `GET /api/portfolio/copy-trading`, frontend copy-trading panel
- Track 2 (#189) — currency-aware FX, per-mirror audit snapshots, long-horizon MTM correction
- `GuardResult.total_aum` field — intentionally not added (§8.6 "what this test explicitly does NOT assert")
- `PortfolioReviewResult.mirror_equity` field — intentionally not added
- AUM persistence to any audit table — no `portfolio_reviews` table exists

---

## Settled decisions and prevention log — preserved

- **Settled — AUM basis** (`docs/settled-decisions.md:206-209`): MTM first, fall back to cost basis. §3.2's formula preserves this: the MTM delta term is zero when `q.last IS NULL`, leaving `cmp.amount` (cost-basis) as the only contribution.
- **Settled — Provider design rule**: The helper lives in `app/services/portfolio.py`, not in any provider. Providers remain thin.
- **Settled — Guard auditability**: `evaluate_recommendation` and `GuardResult` are NOT modified; only the private helper `_load_sector_exposure` changes. One `decision_audit` row per invocation, unchanged.
- **Prevention — JOIN fan-out (#45)**: §3.4's `LEFT JOIN LATERAL (SELECT last FROM quotes ... ORDER BY quoted_at DESC LIMIT 1)` is the one-row-per-key pattern. No `JOIN quotes` inside the aggregate.
- **Prevention — Dead-code None-guard on aggregate fetchone() (#75)**: The helper's `fetchone()` always returns exactly one row because `COALESCE(SUM(...), 0)` guarantees it. The helper must NOT write `row["total"] if row else 0.0` — that is dead code. Cast `row["total"]` directly.
- **Prevention — float(None) crash (#73)**: `COALESCE(SUM(...), 0)` guarantees a non-NULL numeric. `float(row["total"])` is safe. The §8.4 empty-table test pins this.
- **Prevention — Shared cursor across unrelated queries (#77)**: `_load_mirror_equity` uses its own `with conn.cursor(...) as cur:` block.
- **Prevention — Early return inside `with conn.transaction()` (#286)**: The helper does NOT open its own transaction; it runs inside the caller's scope, same pattern as `_load_cash` / `_load_positions`.
- **Prevention — Zero-unit position inflates AUM via cost_basis fallback (#77)**: Applies to `positions`, not `copy_mirror_positions`. `copy_mirror_positions` does not have a `current_units` column in the same sense — Track 1a's sync deletes disappeared nested positions (§2.3.3). No zero-unit ghost rows in `copy_mirror_positions`. Documented, not guarded.

---

## File structure

**Created:**
- `tests/test_mirror_equity.py` — §8.4 AUM identity tests (no-quote, MTM+FX, short, closed-mirror, empty-table)
- `tests/test_execution_guard_mirror_aum.py` — §8.5 guard integration + §8.6 Test 3
- `tests/test_api_portfolio_mirror_equity.py` — §8.6 Test 1 (API path)
- `tests/test_portfolio_review_mirror_equity.py` — §8.6 Test 2 (review path)

**Modified:**
- `tests/fixtures/copy_mirrors.py` — add `mirror_aum_fixture`, `no_quote_mirror_fixture`, `mtm_delta_mirror_fixture`
- `app/services/portfolio.py` — add `_load_mirror_equity` module-level helper; sum it into `run_portfolio_review.total_aum`
- `app/services/execution_guard.py` — import `_load_mirror_equity`, sum into `_load_sector_exposure.total_aum`
- `app/api/portfolio.py` — import `_load_mirror_equity`, sum into `get_portfolio.total_aum`, add `mirror_equity: float = 0.0` field to `PortfolioResponse`
- `frontend/src/api/types.ts` — add `mirror_equity: number` to `PortfolioResponse` interface
- `frontend/src/api/mocks.ts` — add `mirror_equity: 0` to `fetchPortfolioMock` literal
- `frontend/src/pages/InstrumentDetailPage.test.tsx` — add `mirror_equity: 0` to 2 fixture literals (lines 136 and ~409)

---

## Task list

### Task 1: DB fixtures for mirror equity tests

**Files:**
- Modify: `tests/fixtures/copy_mirrors.py` (add 3 new helper functions at end of file)

**Rationale:** Every test in §8.4, §8.5, and §8.6 imports one of these three fixtures. Ship them first so subsequent tasks can TDD against them without touching fixture code again.

- [ ] **Step 1: Add `mirror_aum_fixture` helper at end of `tests/fixtures/copy_mirrors.py`**

Insert this function below `two_mirror_seed_rows`:

```python
def mirror_aum_fixture(conn: psycopg.Connection[Any]) -> None:
    """Seed the load-bearing DB state for §8.4 AUM identity, §8.5
    guard integration, and §8.6 per-call-site delta tests.

    Seeds:
      1. Two mirrors in copy_mirrors: one active (#8001),
         one closed (#8002, active=FALSE, closed_at=_NOW).
      2. copy_mirror_positions: one long each, on distinct
         instrument_ids. The active mirror's position is on
         _GUARD_INSTRUMENT_ID so §8.5's sector-numerator
         resolution lands a valid row.
      3. quotes rows for both mirror positions (last prices set
         such that the MTM delta is non-zero but hand-computable).
      4. An instruments row for _GUARD_INSTRUMENT_ID with
         sector=_GUARD_INSTRUMENT_SECTOR.
      5. A scores row with model_version='v1-balanced', rank=1,
         total_score=0.5, instrument_id=_GUARD_INSTRUMENT_ID —
         required by run_portfolio_review's _load_ranked_scores
         WHERE rank IS NOT NULL clause (portfolio.py:203).
      6. Empty positions and cash_ledger — leaves the
         eBull-owned contribution at 0 so tests that call
         _load_mirror_equity get exactly the mirror_equity term.

    Numbers are chosen to be hand-computable:
      active_available = 1000.00
      active_amount    =  500.00  (cost basis)
      active_units     =   10.0
      active_open_rate =   50.00
      active_conv_rate =    1.00
      active_quote_last=   55.00  (delta = +5/unit)
      active_mtm_delta = 1 * 10.0 * (55.00 - 50.00) * 1.00 = 50.00
      active_equity    = 1000.00 + 500.00 + 50.00 = 1550.00

      closed_available =  200.00  (but WHERE m.active filters)
      closed_amount    =  100.00  (but WHERE m.active filters)
      Expected _load_mirror_equity(conn) = 1550.00

    Caller owns commit / rollback. Safe against ebull_test only
    — caller enforces via _assert_test_db.
    """
    with conn.cursor() as cur:
        # Parent trader rows (required by copy_mirrors.parent_cid FK)
        cur.execute(
            """
            INSERT INTO copy_traders (parent_cid, parent_username,
                                      first_seen_at, updated_at)
            VALUES
                (801, 'aum_fixture_active', %(now)s, %(now)s),
                (802, 'aum_fixture_closed', %(now)s, %(now)s)
            ON CONFLICT (parent_cid) DO NOTHING
            """,
            {"now": _NOW},
        )
        # Active mirror
        cur.execute(
            """
            INSERT INTO copy_mirrors (
                mirror_id, parent_cid, initial_investment,
                deposit_summary, withdrawal_summary,
                available_amount, closed_positions_net_profit,
                stop_loss_percentage, stop_loss_amount,
                mirror_status_id, mirror_calculation_type,
                pending_for_closure, started_copy_date,
                active, closed_at, raw_payload, updated_at
            ) VALUES (
                8001, 801, 5000.00, 0, 0,
                1000.00, 0,
                NULL, NULL, NULL, NULL, FALSE, %(now)s,
                TRUE, NULL, '{}'::jsonb, %(now)s
            )
            """,
            {"now": _NOW},
        )
        # Closed mirror
        cur.execute(
            """
            INSERT INTO copy_mirrors (
                mirror_id, parent_cid, initial_investment,
                deposit_summary, withdrawal_summary,
                available_amount, closed_positions_net_profit,
                stop_loss_percentage, stop_loss_amount,
                mirror_status_id, mirror_calculation_type,
                pending_for_closure, started_copy_date,
                active, closed_at, raw_payload, updated_at
            ) VALUES (
                8002, 802, 5000.00, 0, 0,
                200.00, 0,
                NULL, NULL, NULL, NULL, FALSE, %(now)s,
                FALSE, %(now)s, '{}'::jsonb, %(now)s
            )
            """,
            {"now": _NOW},
        )
        # Instruments row for guard test instrument.
        # NOTE: `instruments` columns per sql/001_init.sql:1-13 are
        # (instrument_id, symbol, company_name, exchange, currency,
        # sector, industry, country, is_tradable, first_seen_at,
        # last_seen_at). first_seen_at and last_seen_at both DEFAULT
        # NOW(). There is NO `tier`, NO `created_at`, NO `updated_at`.
        cur.execute(
            """
            INSERT INTO instruments (instrument_id, symbol, company_name,
                                     sector, is_tradable)
            VALUES (%(iid)s, 'AUMTEST', 'AUM Fixture Instrument',
                    %(sector)s, TRUE)
            ON CONFLICT (instrument_id) DO NOTHING
            """,
            {
                "iid": _GUARD_INSTRUMENT_ID,
                "sector": _GUARD_INSTRUMENT_SECTOR,
            },
        )
        # A second instrument for the "sector numerator unchanged" §8.5 scenario
        cur.execute(
            """
            INSERT INTO instruments (instrument_id, symbol, company_name,
                                     sector, is_tradable)
            VALUES (990002, 'AUMTEST2', 'AUM Fixture Instrument 2',
                    'healthcare', TRUE)
            ON CONFLICT (instrument_id) DO NOTHING
            """,
        )
        # Active mirror's nested position — on guard test instrument
        cur.execute(
            """
            INSERT INTO copy_mirror_positions (
                mirror_id, position_id, parent_position_id,
                instrument_id, is_buy, units, amount,
                initial_amount_in_dollars, open_rate,
                open_conversion_rate, open_date_time,
                take_profit_rate, stop_loss_rate,
                total_fees, leverage, raw_payload, updated_at
            ) VALUES (
                8001, 80011, 10001,
                %(iid)s, TRUE, 10.0, 500.00,
                500.00, 50.00,
                1.00, %(now)s,
                NULL, NULL, 0, 1, '{}'::jsonb, %(now)s
            )
            """,
            {"iid": _GUARD_INSTRUMENT_ID, "now": _NOW},
        )
        # Closed mirror's nested position — would contribute 100 if not filtered
        cur.execute(
            """
            INSERT INTO copy_mirror_positions (
                mirror_id, position_id, parent_position_id,
                instrument_id, is_buy, units, amount,
                initial_amount_in_dollars, open_rate,
                open_conversion_rate, open_date_time,
                take_profit_rate, stop_loss_rate,
                total_fees, leverage, raw_payload, updated_at
            ) VALUES (
                8002, 80021, 10002,
                990002, TRUE, 5.0, 100.00,
                100.00, 20.00,
                1.00, %(now)s,
                NULL, NULL, 0, 1, '{}'::jsonb, %(now)s
            )
            """,
            {"now": _NOW},
        )
        # Quote for active mirror position — last=55.00 → delta=+5/unit
        cur.execute(
            """
            INSERT INTO quotes (instrument_id, last, bid, ask,
                                quoted_at)
            VALUES (%(iid)s, 55.00, 54.95, 55.05, %(now)s)
            ON CONFLICT (instrument_id) DO UPDATE
              SET last = EXCLUDED.last,
                  bid  = EXCLUDED.bid,
                  ask  = EXCLUDED.ask,
                  quoted_at = EXCLUDED.quoted_at
            """,
            {"iid": _GUARD_INSTRUMENT_ID, "now": _NOW},
        )
        # Quote for closed mirror's position (cosmetic — filter masks it)
        cur.execute(
            """
            INSERT INTO quotes (instrument_id, last, bid, ask,
                                quoted_at)
            VALUES (990002, 22.00, 21.95, 22.05, %(now)s)
            ON CONFLICT (instrument_id) DO UPDATE
              SET last = EXCLUDED.last,
                  bid  = EXCLUDED.bid,
                  ask  = EXCLUDED.ask,
                  quoted_at = EXCLUDED.quoted_at
            """,
            {"now": _NOW},
        )
        # Scores row so run_portfolio_review does NOT early-return
        # at portfolio.py:733 — required by §8.6 Test 2.
        cur.execute(
            """
            INSERT INTO scores (instrument_id, model_version,
                                total_score, rank, scored_at)
            VALUES (%(iid)s, 'v1-balanced', 0.5, 1, %(now)s)
            """,
            {"iid": _GUARD_INSTRUMENT_ID, "now": _NOW},
        )
```

- [ ] **Step 2: Add `no_quote_mirror_fixture` helper immediately below**

```python
def no_quote_mirror_fixture(conn: psycopg.Connection[Any]) -> None:
    """Seed the empirically-reconciled mirror 15712187 shape
    with no matching `quotes` rows. Used by §8.4's cost-basis
    fallback identity test.

    Expected _load_mirror_equity(conn) = 2800.33 + 50.00 + 17039.33
                                       = 19889.66

    Caller owns commit / rollback. ebull_test only.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO copy_traders (parent_cid, parent_username,
                                      first_seen_at, updated_at)
            VALUES (901, 'no_quote_fixture', %(now)s, %(now)s)
            ON CONFLICT (parent_cid) DO NOTHING
            """,
            {"now": _NOW},
        )
        cur.execute(
            """
            INSERT INTO copy_mirrors (
                mirror_id, parent_cid, initial_investment,
                deposit_summary, withdrawal_summary,
                available_amount, closed_positions_net_profit,
                stop_loss_percentage, stop_loss_amount,
                mirror_status_id, mirror_calculation_type,
                pending_for_closure, started_copy_date,
                active, closed_at, raw_payload, updated_at
            ) VALUES (
                9001, 901, 20000.00, 0, 0,
                2800.33, -110.34,
                NULL, NULL, NULL, NULL, FALSE, %(now)s,
                TRUE, NULL, '{}'::jsonb, %(now)s
            )
            """,
            {"now": _NOW},
        )
        cur.execute(
            """
            INSERT INTO copy_mirror_positions (
                mirror_id, position_id, parent_position_id,
                instrument_id, is_buy, units, amount,
                initial_amount_in_dollars, open_rate,
                open_conversion_rate, open_date_time,
                take_profit_rate, stop_loss_rate,
                total_fees, leverage, raw_payload, updated_at
            ) VALUES
                (9001, 90011, 90101, 4301, TRUE, 1.0, 50.00, 50.00,
                 50.00, 1.00, %(now)s,
                 NULL, NULL, 0, 1, '{}'::jsonb, %(now)s),
                (9001, 90012, 90102, 4302, TRUE, 1.0, 17039.33, 17039.33,
                 17039.33, 1.00, %(now)s,
                 NULL, NULL, 0, 1, '{}'::jsonb, %(now)s)
            """,
            {"now": _NOW},
        )
        # No quotes rows — the §3.4 query falls back to open_rate,
        # so each MTM delta term is zero and only `amount` contributes.
```

- [ ] **Step 3: Add `mtm_delta_mirror_fixture` helper immediately below**

```python
def mtm_delta_mirror_fixture(
    conn: psycopg.Connection[Any],
    *,
    is_buy: bool = True,
    quote_last: Decimal = Decimal("1400.0"),
) -> Decimal:
    """Seed one long (or short) position with a non-zero MTM
    delta and a matching quote, so §8.4 can assert FX-aware
    delta accounting. Returns the expected mirror equity as a
    Decimal so the test can assert an exact value.

    Computation (is_buy=True, quote_last=1400.0):
        delta_per_unit    = 1400.0 - 1207.4994 = 192.5006
        usd_delta_per_pos = +1 * 6.28927 * 192.5006 * 0.01331
                          ≈ +16.1122
        equity            = available + amount + usd_delta
                          = 2800.33  + 101.08 + 16.1122
                          ≈ 2917.5222

    Caller owns commit / rollback. ebull_test only.
    """
    open_rate = Decimal("1207.4994")
    units = Decimal("6.28927")
    conv_rate = Decimal("0.01331")
    amount = Decimal("101.08")
    available = Decimal("2800.33")

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO copy_traders (parent_cid, parent_username,
                                      first_seen_at, updated_at)
            VALUES (911, 'mtm_fixture', %(now)s, %(now)s)
            ON CONFLICT (parent_cid) DO NOTHING
            """,
            {"now": _NOW},
        )
        cur.execute(
            """
            INSERT INTO copy_mirrors (
                mirror_id, parent_cid, initial_investment,
                deposit_summary, withdrawal_summary,
                available_amount, closed_positions_net_profit,
                stop_loss_percentage, stop_loss_amount,
                mirror_status_id, mirror_calculation_type,
                pending_for_closure, started_copy_date,
                active, closed_at, raw_payload, updated_at
            ) VALUES (
                9101, 911, 20000.00, 0, 0,
                %(available)s, 0,
                NULL, NULL, NULL, NULL, FALSE, %(now)s,
                TRUE, NULL, '{}'::jsonb, %(now)s
            )
            """,
            {"available": available, "now": _NOW},
        )
        cur.execute(
            """
            INSERT INTO copy_mirror_positions (
                mirror_id, position_id, parent_position_id,
                instrument_id, is_buy, units, amount,
                initial_amount_in_dollars, open_rate,
                open_conversion_rate, open_date_time,
                take_profit_rate, stop_loss_rate,
                total_fees, leverage, raw_payload, updated_at
            ) VALUES (
                9101, 91011, 91111,
                4201, %(is_buy)s, %(units)s, %(amount)s,
                %(amount)s, %(open_rate)s,
                %(conv_rate)s, %(now)s,
                NULL, NULL, 0, 1, '{}'::jsonb, %(now)s
            )
            """,
            {
                "is_buy": is_buy,
                "units": units,
                "amount": amount,
                "open_rate": open_rate,
                "conv_rate": conv_rate,
                "now": _NOW,
            },
        )
        cur.execute(
            """
            INSERT INTO quotes (instrument_id, last, bid, ask,
                                quoted_at)
            VALUES (4201, %(last)s, %(last)s, %(last)s, %(now)s)
            ON CONFLICT (instrument_id) DO UPDATE
              SET last = EXCLUDED.last,
                  bid  = EXCLUDED.bid,
                  ask  = EXCLUDED.ask,
                  quoted_at = EXCLUDED.quoted_at
            """,
            {"last": quote_last, "now": _NOW},
        )

    sign = Decimal("1") if is_buy else Decimal("-1")
    usd_delta = sign * units * (quote_last - open_rate) * conv_rate
    return available + amount + usd_delta
```

- [ ] **Step 4: Run existing sync tests to confirm fixture file still imports cleanly**

Run: `uv run pytest tests/test_portfolio_sync.py tests/test_portfolio_sync_mirrors.py tests/test_copy_mirrors_parser.py -x -q`
Expected: all pass (no behavioural change to existing fixtures; new helpers are added at end of file).

- [ ] **Step 5: Commit**

```bash
git add tests/fixtures/copy_mirrors.py
git commit -m "test(#187): add AUM fixtures for mirror equity tests

Adds mirror_aum_fixture (load-bearing for §8.4/§8.5/§8.6),
no_quote_mirror_fixture (cost-basis fallback identity),
mtm_delta_mirror_fixture (FX-aware MTM delta).

Numbers are hand-computable so §8.4 identity tests can assert
exact _load_mirror_equity return values."
```

---

### Task 2: `_load_mirror_equity` helper + §8.4 identity tests

**Files:**
- Modify: `app/services/portfolio.py` (add module-level helper alongside `_load_cash` / `_load_positions`)
- Create: `tests/test_mirror_equity.py`

**Rationale:** This is the §3.4 SQL, encapsulated once. TDD order: §8.4's empty-table test + no-quote test first (the simplest and most prevention-log-relevant); let them drive the helper's implementation; then layer on MTM / short / closed-mirror.

- [ ] **Step 1: Write the failing empty-table test in `tests/test_mirror_equity.py`**

Create the new test file:

```python
"""§8.4 AUM identity tests for _load_mirror_equity.

Real test DB (ebull_test) — same isolation pattern as
tests/test_portfolio_sync_mirrors.py.
"""

from __future__ import annotations

from collections.abc import Iterator
from decimal import Decimal
from typing import Any

import psycopg
import pytest

from app.services.portfolio import _load_mirror_equity
from tests.fixtures.copy_mirrors import (
    mirror_aum_fixture,
    mtm_delta_mirror_fixture,
    no_quote_mirror_fixture,
)
from tests.test_operator_setup_race import (
    _assert_test_db,
    _test_database_url,
    _test_db_available,
)

pytestmark = pytest.mark.skipif(
    not _test_db_available(),
    reason="ebull_test DB unavailable — skipping real-DB mirror equity test",
)


@pytest.fixture
def conn() -> Iterator[psycopg.Connection[Any]]:
    """Yield a fresh ebull_test connection with every table this
    test suite touches truncated at the start of each test.
    """
    with psycopg.connect(_test_database_url()) as c:
        _assert_test_db(c)
        with c.cursor() as cur:
            cur.execute(
                "TRUNCATE copy_mirror_positions, copy_mirrors, "
                "copy_traders, quotes, scores, positions, "
                "cash_ledger, instruments RESTART IDENTITY CASCADE"
            )
        c.commit()
        yield c
        c.rollback()


def test_empty_copy_mirrors_returns_zero_not_none(
    conn: psycopg.Connection[Any],
) -> None:
    """§8.4 + §6.4 contract: empty copy_mirrors → float 0.0,
    not None. Regression test for the COALESCE(SUM(...), 0)
    contract and the dead-code-None-guard prevention rule.
    """
    result = _load_mirror_equity(conn)
    assert result == 0.0
    assert isinstance(result, float)
```

- [ ] **Step 2: Run the test to confirm it fails**

Run: `uv run pytest tests/test_mirror_equity.py::test_empty_copy_mirrors_returns_zero_not_none -x -v`
Expected: ImportError — `_load_mirror_equity` does not yet exist in `app.services.portfolio`.

- [ ] **Step 3: Add `_load_mirror_equity` to `app/services/portfolio.py`**

Insert the new helper immediately below `_load_positions` (currently around line ~180 — place after the closing of `_load_positions`, before the next helper). Use Read to locate the exact insertion point first, then Edit.

```python
def _load_mirror_equity(conn: psycopg.Connection[Any]) -> float:
    """Return the summed mirror_equity across all active mirrors.

    Runs the §3.4 mirror-equity CTE against the caller's
    connection and returns a float. The value is `0.0` when
    `copy_mirrors` is empty or every row is `active = FALSE` —
    `COALESCE(SUM(...), 0)` in the SQL turns an empty result set
    into `0.0`, never `NULL`, so the return type is `float` and
    not `float | None`. See spec §6.0 and §6.4 for rationale.

    The value is usually non-negative but is NOT mathematically
    floored at zero: a leveraged position with a large adverse
    MTM delta could push a per-mirror contribution negative, and
    the aggregate could go negative too. Callers sum this
    directly into `total_aum` without assuming positivity.

    This helper does NOT open its own transaction; it reads
    under the caller's scope, matching `_load_cash` /
    `_load_positions`.
    """
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
        SELECT total FROM mirror_equity
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(sql)
        row = cur.fetchone()
    # COALESCE(SUM(...), 0) guarantees exactly one row with a
    # non-NULL numeric, so row should never be None. Use an
    # explicit RuntimeError (not `assert`) so the guard survives
    # `python -O` — see prevention log entry "`assert` as a
    # runtime guard in service code" (#109).
    if row is None:  # pragma: no cover — driver/CTE invariant violation
        raise RuntimeError(
            "_load_mirror_equity: COALESCE(SUM(...), 0) CTE returned no rows; "
            "driver invariant violated"
        )
    return float(row["total"])
```

- [ ] **Step 4: Run the empty-table test to verify it passes**

Run: `uv run pytest tests/test_mirror_equity.py::test_empty_copy_mirrors_returns_zero_not_none -x -v`
Expected: PASS (empty `copy_mirrors` → `0.0`, `isinstance(result, float)` → `True`).

- [ ] **Step 5: Add the remaining §8.4 identity tests to `tests/test_mirror_equity.py`**

Append to the same file:

```python
def test_no_quote_cost_basis_identity(
    conn: psycopg.Connection[Any],
) -> None:
    """§8.4: available + SUM(amount) identity when no quotes exist.
    Matches the empirically-reconciled mirror 15712187 shape:
    2800.33 + 50.00 + 17039.33 = 19889.66.
    """
    no_quote_mirror_fixture(conn)
    conn.commit()

    result = _load_mirror_equity(conn)
    assert result == pytest.approx(19889.66, abs=1e-6)


def test_mtm_delta_with_fx(
    conn: psycopg.Connection[Any],
) -> None:
    """§8.4: MTM delta is converted to USD using the entry-time
    conversion rate. Long position, quote above entry.
    """
    expected = mtm_delta_mirror_fixture(conn)
    conn.commit()

    result = _load_mirror_equity(conn)
    assert result == pytest.approx(float(expected), abs=1e-6)


def test_short_delta_positive_when_price_falls(
    conn: psycopg.Connection[Any],
) -> None:
    """§8.4: short position is profitable when price falls —
    sign(-1) * positive_units * negative_delta * conv_rate → +USD.
    """
    expected = mtm_delta_mirror_fixture(
        conn,
        is_buy=False,
        quote_last=Decimal("1000.0"),
    )
    conn.commit()

    result = _load_mirror_equity(conn)
    assert result == pytest.approx(float(expected), abs=1e-6)
    # Sanity: short with price below entry should beat available+amount
    # by a positive delta.
    assert result > (2800.33 + 101.08)


def test_closed_mirror_excluded(
    conn: psycopg.Connection[Any],
) -> None:
    """§8.4: WHERE m.active filter excludes closed mirrors.
    mirror_aum_fixture seeds one active (equity=1550.00) and one
    closed (would be 300.00 if not filtered). Expect 1550.00.
    """
    mirror_aum_fixture(conn)
    conn.commit()

    result = _load_mirror_equity(conn)
    assert result == pytest.approx(1550.00, abs=1e-6)


def test_all_mirrors_closed_returns_zero(
    conn: psycopg.Connection[Any],
) -> None:
    """§8.4: if every mirror is active=FALSE, the WHERE filter
    leaves an empty result set → COALESCE returns 0.0.
    """
    mirror_aum_fixture(conn)
    with conn.cursor() as cur:
        cur.execute("UPDATE copy_mirrors SET active = FALSE")
    conn.commit()

    result = _load_mirror_equity(conn)
    assert result == 0.0
```

- [ ] **Step 6: Run all §8.4 tests to verify**

Run: `uv run pytest tests/test_mirror_equity.py -x -v`
Expected: all 5 tests pass.

- [ ] **Step 7: Commit**

```bash
git add app/services/portfolio.py tests/test_mirror_equity.py
git commit -m "feat(#187): _load_mirror_equity helper + §8.4 identity tests

Adds the shared AUM contribution helper used by all three call
sites (execution guard, API, portfolio review). Uses the §3.4
CTE with LATERAL quote lookup and WHERE m.active filter.

Tests pin:
- empty copy_mirrors → float 0.0 (not None)
- cost-basis fallback identity (no quotes) = available + SUM(amount)
- MTM delta uses open_conversion_rate (USD conversion)
- short sign inverts delta contribution
- closed mirror excluded via WHERE m.active
- all-closed → 0.0

COALESCE(SUM(...), 0) guarantees exactly one row — no dead-code
None guard (prevention #75)."
```

---

### Task 3: Guard integration (`_load_sector_exposure`) + §8.5 tests

**Files:**
- Modify: `app/services/execution_guard.py` (import helper, sum into `total_aum`)
- Create: `tests/test_execution_guard_mirror_aum.py`

**Rationale:** Smallest integration surgery in the codebase — one extra line inside the helper. §8.5 tests at the `_load_sector_exposure` level (not `evaluate_recommendation`) to avoid wiring kill-switch / runtime-config / trade_recommendations for a change that only affects a local variable.

- [ ] **Step 1: Write the failing baseline test in `tests/test_execution_guard_mirror_aum.py`**

```python
"""§8.5 guard AUM integration tests and §8.6 Test 3 guard-delta.

Tests `_load_sector_exposure` directly — see spec §6.1 and §8.5
for why this is the correct test surface (GuardResult has no
total_aum field, AUM is a local inside evaluate_recommendation).
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import psycopg
import pytest

from app.services.execution_guard import _load_sector_exposure
from app.services.portfolio import _load_mirror_equity
from tests.fixtures.copy_mirrors import (
    _GUARD_INSTRUMENT_ID,
    _GUARD_INSTRUMENT_SECTOR,
    _NOW,
    mirror_aum_fixture,
)
from tests.test_operator_setup_race import (
    _assert_test_db,
    _test_database_url,
    _test_db_available,
)

pytestmark = pytest.mark.skipif(
    not _test_db_available(),
    reason="ebull_test DB unavailable — skipping real-DB guard AUM test",
)


@pytest.fixture
def conn() -> Iterator[psycopg.Connection[Any]]:
    with psycopg.connect(_test_database_url()) as c:
        _assert_test_db(c)
        with c.cursor() as cur:
            cur.execute(
                "TRUNCATE copy_mirror_positions, copy_mirrors, "
                "copy_traders, quotes, scores, positions, "
                "cash_ledger, instruments RESTART IDENTITY CASCADE"
            )
        c.commit()
        yield c
        c.rollback()


def _seed_ebull_position_and_cash(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
    sector: str,
    units: float,
    cost_basis: float,
    quote_last: float | None,
    cash: float,
) -> None:
    """Add one eBull position + one cash ledger row on top of the
    mirror_aum_fixture base. Instrument must not collide with
    _GUARD_INSTRUMENT_ID — §8.5 scenarios use a separate
    instrument for the eBull position so the sector-numerator
    test has a distinct id.
    """
    # NOTE — schema references: sql/001_init.sql:1-13 (instruments),
    # sql/001_init.sql:159-168 (positions), sql/021_positions_source.sql
    # (positions.source: 'ebull' | 'broker_sync'), sql/001_init.sql:170-177
    # (cash_ledger event_type / amount / currency / note, no 'reason' or
    # 'recorded_at'), sql/002_market_data_features.sql (quotes). Do NOT
    # use 'tier', 'created_at', 'updated_at' on instruments — they do
    # not exist. Do NOT use 'broker' as a positions.source — the CHECK
    # constraint rejects it.
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO instruments (instrument_id, symbol, company_name,
                                     sector, is_tradable)
            VALUES (%(iid)s, 'EBULL', 'eBull Position',
                    %(sector)s, TRUE)
            ON CONFLICT (instrument_id) DO NOTHING
            """,
            {"iid": instrument_id, "sector": sector},
        )
        cur.execute(
            """
            INSERT INTO positions (instrument_id, current_units,
                                   cost_basis, avg_cost, open_date,
                                   source, updated_at)
            VALUES (%(iid)s, %(units)s, %(cb)s,
                    %(cb)s / NULLIF(%(units)s, 0),
                    %(today)s, 'broker_sync', %(now)s)
            """,
            {
                "iid": instrument_id,
                "units": units,
                "cb": cost_basis,
                "today": _NOW.date(),
                "now": _NOW,
            },
        )
        if quote_last is not None:
            cur.execute(
                """
                INSERT INTO quotes (instrument_id, last, bid, ask,
                                    quoted_at)
                VALUES (%(iid)s, %(last)s, %(last)s, %(last)s, %(now)s)
                ON CONFLICT (instrument_id) DO UPDATE
                  SET last = EXCLUDED.last,
                      bid  = EXCLUDED.bid,
                      ask  = EXCLUDED.ask,
                      quoted_at = EXCLUDED.quoted_at
                """,
                {"iid": instrument_id, "last": quote_last, "now": _NOW},
            )
        cur.execute(
            """
            INSERT INTO cash_ledger (event_type, amount, currency)
            VALUES ('deposit', %(amt)s, 'GBP')
            """,
            {"amt": cash},
        )


def test_empty_baseline_no_mirrors(
    conn: psycopg.Connection[Any],
) -> None:
    """§8.5: with no copy_mirrors rows at all, guard AUM is the
    pre-PR contract: positions_mv + cash.
    """
    # Seed ONLY the instruments row for the guard query + one
    # eBull position + cash.
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO instruments (instrument_id, symbol, company_name,
                                     sector, is_tradable)
            VALUES (%(iid)s, 'AUMTEST', 'guard instrument',
                    %(sector)s, TRUE)
            """,
            {
                "iid": _GUARD_INSTRUMENT_ID,
                "sector": _GUARD_INSTRUMENT_SECTOR,
            },
        )
    _seed_ebull_position_and_cash(
        conn,
        instrument_id=770001,
        sector="healthcare",
        units=10.0,
        cost_basis=200.0,
        quote_last=25.0,  # mv = 250.0
        cash=100.0,
    )
    conn.commit()

    found, sector, pct, total_aum = _load_sector_exposure(
        conn, _GUARD_INSTRUMENT_ID
    )
    assert found is True
    assert sector == _GUARD_INSTRUMENT_SECTOR
    assert total_aum == pytest.approx(250.0 + 100.0, abs=1e-6)


def test_active_mirror_adds_to_denominator(
    conn: psycopg.Connection[Any],
) -> None:
    """§8.5: active mirror's equity is added to total_aum on top of
    positions + cash. Sector numerator is untouched.
    """
    mirror_aum_fixture(conn)
    _seed_ebull_position_and_cash(
        conn,
        instrument_id=770001,
        sector="healthcare",  # DIFFERENT sector from guard instrument
        units=10.0,
        cost_basis=200.0,
        quote_last=25.0,  # mv = 250.0
        cash=100.0,
    )
    conn.commit()

    expected_mirror = _load_mirror_equity(conn)
    assert expected_mirror == pytest.approx(1550.0, abs=1e-6)

    found, sector, pct, total_aum = _load_sector_exposure(
        conn, _GUARD_INSTRUMENT_ID
    )
    assert found is True
    assert sector == _GUARD_INSTRUMENT_SECTOR
    assert total_aum == pytest.approx(250.0 + 100.0 + 1550.0, abs=1e-6)


def test_closed_mirror_contributes_nothing(
    conn: psycopg.Connection[Any],
) -> None:
    """§8.5: flipping active=FALSE on all mirrors returns total_aum
    to the baseline (positions + cash).
    """
    mirror_aum_fixture(conn)
    with conn.cursor() as cur:
        cur.execute("UPDATE copy_mirrors SET active = FALSE")
    _seed_ebull_position_and_cash(
        conn,
        instrument_id=770001,
        sector="healthcare",
        units=10.0,
        cost_basis=200.0,
        quote_last=25.0,
        cash=100.0,
    )
    conn.commit()

    found, _, _, total_aum = _load_sector_exposure(
        conn, _GUARD_INSTRUMENT_ID
    )
    assert found is True
    assert total_aum == pytest.approx(250.0 + 100.0, abs=1e-6)


def test_sector_numerator_unchanged_by_mirror(
    conn: psycopg.Connection[Any],
) -> None:
    """§4 / §8.5: mirrors expand the denominator only. Query under
    an instrument whose sector != the mirror's sector; mirror
    contributes to total_aum but NOT to current_sector_pct.
    """
    mirror_aum_fixture(conn)
    # Add one eBull position in `healthcare` (different from
    # _GUARD_INSTRUMENT_SECTOR='technology') so the sector split
    # is visible.
    _seed_ebull_position_and_cash(
        conn,
        instrument_id=770001,
        sector="healthcare",
        units=10.0,
        cost_basis=200.0,
        quote_last=25.0,  # mv = 250.0 in healthcare
        cash=100.0,
    )
    conn.commit()

    # Query FOR a healthcare instrument → sector numerator should
    # cover only the 770001 position (which the query itself
    # excludes via instrument_id != iid, so numerator = 0).
    # The denominator still includes the mirror.
    found_hc, sector_hc, pct_hc, aum_hc = _load_sector_exposure(
        conn, 770001
    )
    assert found_hc is True
    assert sector_hc == "healthcare"
    # Expected: the sole healthcare position is the iid being
    # queried, so the numerator is 0 (the query EXCLUDES the
    # queried instrument to avoid counting itself). Denominator
    # is mirror equity only (250 is being excluded).
    assert pct_hc == pytest.approx(0.0, abs=1e-6)
    # Denominator = positions - self + cash + mirror_equity
    #             = (250 - 250) + 100 + 1550 = 1650
    assert aum_hc == pytest.approx(100.0 + 1550.0, abs=1e-6)


def test_guard_delta_matches_mirror_equity(
    conn: psycopg.Connection[Any],
) -> None:
    """§8.6 Test 3: the additive delta on the guard path equals
    _load_mirror_equity(conn). Symmetry with §8.6 Tests 1 and 2
    so the per-call-site delta contract is visible.
    """
    mirror_aum_fixture(conn)
    _seed_ebull_position_and_cash(
        conn,
        instrument_id=770001,
        sector="healthcare",
        units=10.0,
        cost_basis=200.0,
        quote_last=25.0,
        cash=100.0,
    )
    conn.commit()

    expected_mirror_contribution = _load_mirror_equity(conn)
    _, _, _, with_mirror = _load_sector_exposure(
        conn, _GUARD_INSTRUMENT_ID
    )

    with conn.cursor() as cur:
        cur.execute("UPDATE copy_mirrors SET active = FALSE")
    conn.commit()

    _, _, _, without_mirror = _load_sector_exposure(
        conn, _GUARD_INSTRUMENT_ID
    )

    assert (with_mirror - without_mirror) == pytest.approx(
        expected_mirror_contribution, abs=1e-6
    )
```

- [ ] **Step 2: Run the tests to confirm baseline passes (no integration yet)**

Run: `uv run pytest tests/test_execution_guard_mirror_aum.py::test_empty_baseline_no_mirrors -x -v`
Expected: PASS (no mirrors, pre-PR behaviour already holds).

Run: `uv run pytest tests/test_execution_guard_mirror_aum.py::test_active_mirror_adds_to_denominator -x -v`
Expected: FAIL — `total_aum` returns `350.0`, not `1900.0`, because `_load_sector_exposure` is not yet summing in `_load_mirror_equity`.

- [ ] **Step 3: Wire `_load_mirror_equity` into `_load_sector_exposure`**

In `app/services/execution_guard.py`, at the top of the file add an import:

```python
from app.services.portfolio import _load_mirror_equity
```

(Place the import next to the other `from app.services.portfolio import ...` imports if any exist; otherwise add it alongside the existing app-level imports.)

Then edit `_load_sector_exposure` — change the current line 286:

```python
    total_aum = total_positions + cash
```

to:

```python
    mirror_equity = _load_mirror_equity(conn)
    total_aum = total_positions + cash + mirror_equity
```

- [ ] **Step 4: Re-run the guard integration tests**

Run: `uv run pytest tests/test_execution_guard_mirror_aum.py -x -v`
Expected: all 5 tests pass.

- [ ] **Step 5: Run the existing guard tests to confirm no regression**

Run: `uv run pytest tests/test_execution_guard.py -x -q`
Expected: all existing tests still pass. None of the existing tests seed `copy_mirrors`, so the new sum is always `+ 0.0` and behaviour is preserved.

- [ ] **Step 6: Commit**

```bash
git add app/services/execution_guard.py tests/test_execution_guard_mirror_aum.py
git commit -m "feat(#187): guard AUM now includes mirror_equity (§6.1 + §8.5)

_load_sector_exposure sums _load_mirror_equity into total_aum.
evaluate_recommendation and GuardResult are unchanged — AUM
remains a local variable consumed by the concentration rule.

Tests (directly against _load_sector_exposure):
- empty baseline (no mirrors) — pre-PR contract holds
- active mirror adds to denominator
- closed mirror contributes nothing (WHERE m.active)
- sector numerator unchanged (mirrors inflate denominator only)
- guard-delta matches _load_mirror_equity (§8.6 Test 3 symmetry)

Existing guard tests untouched — they seed no copy_mirrors rows,
so the new sum is +0.0 and behaviour is preserved."
```

---

### Task 4: API integration + `PortfolioResponse.mirror_equity` field + frontend sync

**Files:**
- Modify: `app/api/portfolio.py`
- Modify: `frontend/src/api/types.ts`
- Modify: `frontend/src/api/mocks.ts`
- Modify: `frontend/src/pages/InstrumentDetailPage.test.tsx` (2 literals)
- Create: `tests/test_api_portfolio_mirror_equity.py` (§8.6 Test 1)

**Rationale:** The API call site is the only place where a new response field is introduced. The frontend `types.ts` MUST update in the same PR per the api-shape-and-types rule; three existing test/mock literals that construct a `PortfolioResponse` need `mirror_equity: 0` added.

- [ ] **Step 1: Write the failing API test in `tests/test_api_portfolio_mirror_equity.py`**

```python
"""§8.6 Test 1 — AUM delta test for the API path.

Uses TestClient + app.dependency_overrides[get_conn] to point
at ebull_test, seeds mirror_aum_fixture + one eBull position +
cash, and asserts:
- PortfolioResponse.mirror_equity == _load_mirror_equity(conn)
- PortfolioResponse.total_aum == positions_mv + cash + mirror_equity
- soft-close baseline returns to positions + cash
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import psycopg
import pytest
from fastapi.testclient import TestClient

from app.db import get_conn
from app.main import app
from app.services.portfolio import _load_mirror_equity
from tests.fixtures.copy_mirrors import (
    _GUARD_INSTRUMENT_ID,
    _GUARD_INSTRUMENT_SECTOR,
    _NOW,
    mirror_aum_fixture,
)
from tests.test_operator_setup_race import (
    _assert_test_db,
    _test_database_url,
    _test_db_available,
)

pytestmark = pytest.mark.skipif(
    not _test_db_available(),
    reason="ebull_test DB unavailable — skipping real-DB API mirror equity test",
)


@pytest.fixture
def conn() -> Iterator[psycopg.Connection[Any]]:
    with psycopg.connect(_test_database_url()) as c:
        _assert_test_db(c)
        with c.cursor() as cur:
            cur.execute(
                "TRUNCATE copy_mirror_positions, copy_mirrors, "
                "copy_traders, quotes, scores, positions, "
                "cash_ledger, instruments RESTART IDENTITY CASCADE"
            )
        c.commit()
        yield c
        c.rollback()


@pytest.fixture
def client(conn: psycopg.Connection[Any]) -> Iterator[TestClient]:
    """Overrides get_conn to reuse the test fixture connection, so
    assertions read the same DB state the endpoint sees.
    """
    # Also bypass auth — this endpoint is session-protected.
    from app.api.auth import require_session_or_service_token

    def _override_conn() -> Iterator[psycopg.Connection[Any]]:
        yield conn

    def _override_auth() -> None:
        return None

    app.dependency_overrides[get_conn] = _override_conn
    app.dependency_overrides[require_session_or_service_token] = _override_auth
    try:
        yield TestClient(app)
    finally:
        app.dependency_overrides.pop(get_conn, None)
        app.dependency_overrides.pop(require_session_or_service_token, None)


def _seed_ebull_position_and_cash(
    conn: psycopg.Connection[Any],
) -> tuple[float, float]:
    """Add one eBull position + one cash row on top of
    mirror_aum_fixture. Returns (positions_mv, cash).

    Schema references — same as the §8.5 helper: sql/001_init.sql:1-13
    (instruments), :159-168 (positions), :170-177 (cash_ledger),
    sql/021_positions_source.sql (positions.source allows only
    'ebull' | 'broker_sync'). No 'tier'/'created_at'/'updated_at' on
    instruments; no 'reason'/'recorded_at' on cash_ledger.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO instruments (instrument_id, symbol, company_name,
                                     sector, is_tradable)
            VALUES (770001, 'EBULL', 'eBull Position',
                    'healthcare', TRUE)
            """
        )
        cur.execute(
            """
            INSERT INTO positions (instrument_id, current_units,
                                   cost_basis, avg_cost, open_date,
                                   source, updated_at)
            VALUES (770001, 10.0, 200.0, 20.0,
                    %(today)s, 'broker_sync', %(now)s)
            """,
            {"today": _NOW.date(), "now": _NOW},
        )
        cur.execute(
            """
            INSERT INTO quotes (instrument_id, last, bid, ask, quoted_at)
            VALUES (770001, 25.0, 24.95, 25.05, %(now)s)
            ON CONFLICT (instrument_id) DO UPDATE
              SET last = EXCLUDED.last,
                  bid  = EXCLUDED.bid,
                  ask  = EXCLUDED.ask,
                  quoted_at = EXCLUDED.quoted_at
            """,
            {"now": _NOW},
        )
        cur.execute(
            """
            INSERT INTO cash_ledger (event_type, amount, currency)
            VALUES ('deposit', 100.0, 'GBP')
            """
        )
    return 250.0, 100.0  # mv = units * last = 10 * 25


def test_api_portfolio_mirror_equity_present_in_response(
    conn: psycopg.Connection[Any], client: TestClient
) -> None:
    """§8.6 Test 1: GET /api/portfolio exposes mirror_equity and
    sums it into total_aum.
    """
    mirror_aum_fixture(conn)
    positions_mv, cash = _seed_ebull_position_and_cash(conn)
    conn.commit()

    expected_mirror = _load_mirror_equity(conn)
    assert expected_mirror == pytest.approx(1550.0, abs=1e-6)

    response = client.get("/api/portfolio")
    assert response.status_code == 200
    body = response.json()
    assert body["mirror_equity"] == pytest.approx(expected_mirror, abs=1e-6)
    assert body["total_aum"] == pytest.approx(
        positions_mv + cash + expected_mirror, abs=1e-6
    )


def test_api_portfolio_soft_close_baseline(
    conn: psycopg.Connection[Any], client: TestClient
) -> None:
    """§8.6 Test 1 baseline: flip mirrors to active=FALSE →
    mirror_equity returns to 0.0 and total_aum to positions + cash.
    """
    mirror_aum_fixture(conn)
    positions_mv, cash = _seed_ebull_position_and_cash(conn)
    with conn.cursor() as cur:
        cur.execute("UPDATE copy_mirrors SET active = FALSE")
    conn.commit()

    response = client.get("/api/portfolio")
    assert response.status_code == 200
    body = response.json()
    assert body["mirror_equity"] == 0.0
    assert body["total_aum"] == pytest.approx(positions_mv + cash, abs=1e-6)


def test_api_portfolio_no_mirrors_field_default(
    conn: psycopg.Connection[Any], client: TestClient
) -> None:
    """§6.4 contract: with no copy_mirrors rows at all,
    mirror_equity is the float 0.0 (not None, not absent).
    """
    _seed_ebull_position_and_cash(conn)
    conn.commit()

    response = client.get("/api/portfolio")
    assert response.status_code == 200
    body = response.json()
    assert body["mirror_equity"] == 0.0
    assert "mirror_equity" in body  # field is always present
```

- [ ] **Step 2: Run the tests to confirm they fail**

Run: `uv run pytest tests/test_api_portfolio_mirror_equity.py -x -v`
Expected: FAIL — `mirror_equity` is not on `PortfolioResponse`, either as `KeyError` or a `422` validation fail (depending on how FastAPI serializes missing-from-model keys).

- [ ] **Step 3: Add `mirror_equity` to `PortfolioResponse` in `app/api/portfolio.py`**

Edit the `PortfolioResponse` class (lines 63-67):

```python
class PortfolioResponse(BaseModel):
    positions: list[PositionItem]
    position_count: int
    total_aum: float
    cash_balance: float | None
    mirror_equity: float = 0.0
```

- [ ] **Step 4: Import `_load_mirror_equity` and wire it into `get_portfolio`**

Add the import near the top of `app/api/portfolio.py`:

```python
from app.services.portfolio import _load_mirror_equity
```

Edit `get_portfolio`: replace the block at lines 164-176 (currently `total_market = ...` through `return PortfolioResponse(...)`) with:

```python
    # AUM: sum of position market_values + cash (if known) + mirror_equity.
    total_market = sum(p.market_value for p in positions)
    mirror_equity = _load_mirror_equity(conn)
    total_aum = (
        total_market
        + (cash_balance if cash_balance is not None else 0.0)
        + mirror_equity
    )

    # Re-sort by market_value DESC (computed value, not a DB column) with stable tiebreak.
    positions.sort(key=lambda p: (-p.market_value, p.instrument_id))

    return PortfolioResponse(
        positions=positions,
        position_count=len(positions),
        total_aum=total_aum,
        cash_balance=cash_balance,
        mirror_equity=mirror_equity,
    )
```

Also update the module-level docstring (top of file, line ~20) to append "+ mirror_equity":

Change:
```python
AUM = SUM(market_value across all positions) + cash_balance.
```
to:
```python
AUM = SUM(market_value across all positions) + cash_balance + mirror_equity.
```

- [ ] **Step 5: Run the API tests**

Run: `uv run pytest tests/test_api_portfolio_mirror_equity.py -x -v`
Expected: all 3 tests pass.

- [ ] **Step 6: Update frontend TypeScript types**

Edit `frontend/src/api/types.ts` — extend the `PortfolioResponse` interface:

```typescript
export interface PortfolioResponse {
  positions: PositionItem[];
  position_count: number;
  total_aum: number;
  cash_balance: number | null;
  mirror_equity: number;
}
```

- [ ] **Step 7: Update the 3 existing frontend fixture literals**

Edit `frontend/src/api/mocks.ts:34`:

Before:
```typescript
  return { positions: [], position_count: 0, total_aum: 0, cash_balance: null };
```
After:
```typescript
  return { positions: [], position_count: 0, total_aum: 0, cash_balance: null, mirror_equity: 0 };
```

Edit `frontend/src/pages/InstrumentDetailPage.test.tsx:136-141` (emptyPortfolio literal):

Before:
```typescript
const emptyPortfolio: PortfolioResponse = {
  positions: [],
  position_count: 0,
  total_aum: 0,
  cash_balance: null,
};
```
After:
```typescript
const emptyPortfolio: PortfolioResponse = {
  positions: [],
  position_count: 0,
  total_aum: 0,
  cash_balance: null,
  mirror_equity: 0,
};
```

Edit `frontend/src/pages/InstrumentDetailPage.test.tsx:~409` — the second literal with `total_aum: 1910`. Use Read to locate the exact lines first, then add `mirror_equity: 0,` alongside the other fields.

- [ ] **Step 8: Run frontend typecheck and tests**

Run:
```bash
pnpm --dir frontend typecheck
pnpm --dir frontend test
```
Expected: both pass. Any additional `PortfolioResponse` literal discovered by the typechecker must also gain `mirror_equity: 0` — grep `frontend/src/` for `total_aum` to catch them.

- [ ] **Step 9: Run the existing API portfolio tests to confirm no regression**

Run: `uv run pytest tests/test_api_portfolio.py -x -q`
Expected: all existing tests still pass. They use the real test DB with no `copy_mirrors` rows, so `mirror_equity == 0.0` — `total_aum` is unchanged from before.

- [ ] **Step 10: Commit**

```bash
git add app/api/portfolio.py tests/test_api_portfolio_mirror_equity.py \
    frontend/src/api/types.ts frontend/src/api/mocks.ts \
    frontend/src/pages/InstrumentDetailPage.test.tsx
git commit -m "feat(#187): API portfolio exposes mirror_equity (§6.2 + §8.6 Test 1)

PortfolioResponse grows one new field \`mirror_equity: float = 0.0\`
so the frontend can render AUM = positions + cash + mirrors
without a null branch (see §6.4 rationale).

get_portfolio calls _load_mirror_equity and sums it into
total_aum. Existing API tests unchanged — they seed no mirrors,
so mirror_equity=0 and total_aum is identical to before.

Frontend:
- types.ts PortfolioResponse gains mirror_equity: number
- 3 existing fixture literals updated with mirror_equity: 0
- SummaryCards already consumes total_aum; no UI surgery needed
  (copy-trading panel is Track 1.5)."
```

---

### Task 5: `run_portfolio_review` integration + §8.6 Test 2

**Files:**
- Modify: `app/services/portfolio.py` (`run_portfolio_review`, line 751-753)
- Create: `tests/test_portfolio_review_mirror_equity.py` (§8.6 Test 2)

**Rationale:** The review path is the last of three call sites. `_load_mirror_equity` is now a sibling in the same module, so the integration is a two-line addition. Test must NOT early-return at line 733 — `mirror_aum_fixture`'s scores row prevents that.

- [ ] **Step 1: Write the failing review integration test in `tests/test_portfolio_review_mirror_equity.py`**

```python
"""§8.6 Test 2 — AUM delta test for the run_portfolio_review path.

The mirror_aum_fixture's `scores` row is load-bearing: without
it, run_portfolio_review returns early at portfolio.py:733
before touching the AUM block, and this test is silently a
no-op (prevention discipline, spec §8.0 component 5).
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import psycopg
import pytest

from app.services.portfolio import _load_mirror_equity, run_portfolio_review
from tests.fixtures.copy_mirrors import _NOW, mirror_aum_fixture
from tests.test_operator_setup_race import (
    _assert_test_db,
    _test_database_url,
    _test_db_available,
)

pytestmark = pytest.mark.skipif(
    not _test_db_available(),
    reason="ebull_test DB unavailable — skipping real-DB review mirror equity test",
)


@pytest.fixture
def conn() -> Iterator[psycopg.Connection[Any]]:
    with psycopg.connect(_test_database_url()) as c:
        _assert_test_db(c)
        with c.cursor() as cur:
            # TRUNCATE list — real tables only. sql/009_trade_recommendations_score_ref.sql
            # shows the table is `trade_recommendations`, not `recommendations`.
            # We do not need to truncate it here: run_portfolio_review inserts
            # into trade_recommendations but the test only asserts
            # result.total_aum, not DB state. CASCADE from scores handles
            # downstream rows anyway. If a later assertion needs it, add it.
            cur.execute(
                "TRUNCATE copy_mirror_positions, copy_mirrors, "
                "copy_traders, quotes, scores, positions, "
                "cash_ledger, coverage, theses, trade_recommendations, "
                "instruments RESTART IDENTITY CASCADE"
            )
        c.commit()
        yield c
        c.rollback()


def _seed_review_preconditions(conn: psycopg.Connection[Any]) -> None:
    """Add the minimum rows beyond `mirror_aum_fixture` that
    `run_portfolio_review` reads from during its evaluation:
    coverage (Tier 1 for the ranked instrument) so the instrument
    survives to the AUM block.

    Schema reference — sql/001_init.sql:78-85. The column is
    `coverage_tier` (not `tier`), and there is no `updated_at`
    column. `last_reviewed_at` is the only timestamp.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO coverage (instrument_id, coverage_tier,
                                  review_frequency, last_reviewed_at)
            VALUES (990001, 1, 'weekly', %(now)s)
            ON CONFLICT (instrument_id) DO UPDATE
              SET coverage_tier     = EXCLUDED.coverage_tier,
                  review_frequency  = EXCLUDED.review_frequency,
                  last_reviewed_at  = EXCLUDED.last_reviewed_at
            """,
            {"now": _NOW},
        )


def test_run_portfolio_review_total_aum_includes_mirror_equity(
    conn: psycopg.Connection[Any],
) -> None:
    """§8.6 Test 2: result.total_aum carries the mirror contribution
    on top of positions + cash.
    """
    mirror_aum_fixture(conn)
    _seed_review_preconditions(conn)
    conn.commit()

    expected_mirror = _load_mirror_equity(conn)
    assert expected_mirror == pytest.approx(1550.0, abs=1e-6)

    result = run_portfolio_review(conn)
    # Base fixture has empty positions + cash, so expected total_aum
    # is exactly the mirror contribution.
    assert result.total_aum == pytest.approx(expected_mirror, abs=1e-6)


def test_run_portfolio_review_soft_close_baseline(
    conn: psycopg.Connection[Any],
) -> None:
    """§8.6 Test 2 baseline: flip all mirrors to active=FALSE →
    result.total_aum returns to positions + cash (0.0 with the
    fixture's empty-positions invariant).
    """
    mirror_aum_fixture(conn)
    _seed_review_preconditions(conn)
    with conn.cursor() as cur:
        cur.execute("UPDATE copy_mirrors SET active = FALSE")
    conn.commit()

    result = run_portfolio_review(conn)
    assert result.total_aum == 0.0
```

- [ ] **Step 2: Run the tests to confirm they fail**

Run: `uv run pytest tests/test_portfolio_review_mirror_equity.py -x -v`
Expected: FAIL — `run_portfolio_review` currently computes `total_aum = total_market_value + cash` with no mirror term, so the assertion `result.total_aum == 1550.0` fails with `0.0`.

- [ ] **Step 3: Wire `_load_mirror_equity` into `run_portfolio_review`**

In `app/services/portfolio.py`, edit lines 751-753:

Before:
```python
    # AUM
    total_market_value = sum(p.market_value for p in positions.values())
    total_aum = total_market_value + (cash if cash_known else 0.0)
```

After:
```python
    # AUM — positions + cash + mirror_equity (§6.3).
    # _load_mirror_equity is a sibling helper in this module.
    total_market_value = sum(p.market_value for p in positions.values())
    mirror_equity = _load_mirror_equity(conn)
    total_aum = total_market_value + (cash if cash_known else 0.0) + mirror_equity
```

Update the log line at lines 755-762 to include mirror equity for operator visibility:

Before:
```python
    logger.info(
        "run_portfolio_review: positions=%d cash=%s aum=%.2f ranked=%d model=%s",
        len(positions),
        f"{cash:.2f}" if cash_known else "unknown",
        total_aum,
        len(ranked_ids),
        model_version,
    )
```

After:
```python
    logger.info(
        "run_portfolio_review: positions=%d cash=%s mirror_equity=%.2f aum=%.2f ranked=%d model=%s",
        len(positions),
        f"{cash:.2f}" if cash_known else "unknown",
        mirror_equity,
        total_aum,
        len(ranked_ids),
        model_version,
    )
```

- [ ] **Step 4: Run the review integration tests**

Run: `uv run pytest tests/test_portfolio_review_mirror_equity.py -x -v`
Expected: both tests pass.

- [ ] **Step 5: Run the existing portfolio review suite to confirm no regression**

Run: `uv run pytest tests/test_portfolio.py tests/test_portfolio_review.py -x -q 2>&1 | tail -30`

(Use whatever test modules currently cover `run_portfolio_review` — grep `tests/` for the function name if unsure.)

Expected: all existing tests still pass. None seed copy_mirrors, so `mirror_equity == 0.0` and behaviour is identical.

- [ ] **Step 6: Commit**

```bash
git add app/services/portfolio.py tests/test_portfolio_review_mirror_equity.py
git commit -m "feat(#187): run_portfolio_review AUM includes mirror_equity (§6.3 + §8.6 Test 2)

Adds mirror_equity = _load_mirror_equity(conn) before the AUM
sum in run_portfolio_review. The log line now reports the
per-mirror contribution so operators can reconcile against the
broker payload.

No new field on PortfolioReviewResult — the review path consumes
total_aum as a scalar and does not expose mirror_equity
separately (see spec §8.6 'what this test does NOT assert').
No audit persistence — no portfolio_reviews table exists.

Test seeds mirror_aum_fixture + coverage Tier 1 for the ranked
instrument so the function reaches the AUM block (early-return
at portfolio.py:733 requires at least one rankable candidate)."
```

---

### Task 6: Pre-flight review, final local checks, PR

**Files:**
- No code changes; this task is the pre-push gate defined in `.claude/skills/engineering/pre-flight-review.md`.

**Rationale:** This plan has touched 3 service-layer files, 4 new test modules, and 3 frontend files. Do the full same-class scan (every `fetchone()`, every `row[...]` access in the three modified files), confirm the diff reflects only the surgical changes the spec asks for, then run every gate command once more.

- [ ] **Step 1: Run the full pre-push checklist**

```bash
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv run pytest
```

All four must pass. If any fail, investigate root cause — **do not** skip.

- [ ] **Step 2: Frontend gates**

```bash
pnpm --dir frontend typecheck
pnpm --dir frontend test
```

Both must pass.

- [ ] **Step 3: Run the pre-flight review checklist (`.claude/skills/engineering/pre-flight-review.md`)**

For each of the 3 modified service-layer files (`portfolio.py`, `execution_guard.py`, `api/portfolio.py`):

- **Section A (first-row / empty-state)**: verify the empty-table test exists for `_load_mirror_equity` and that the helper returns `0.0` (not `None`, not a crash).
- **Section B (SQL correctness)**: grep for `fetchone()` in the modified files. Confirm every `fetchone()` call that follows an aggregate (`SUM`, `COUNT`, `MAX`, `MIN` without `GROUP BY`) has no dead `if row is None` guard.
- **Section C (Python hygiene)**: confirm `from __future__ import annotations` is present in every new test file and in `app/services/portfolio.py`.
- **Section D (tests)**: §8.4 empty + 4 identity variants, §8.5 4 scenarios, §8.6 3 per-path tests, 3 soft-close baseline repeats. Confirm every new assertion proves behaviour, not just "no crash."
- **Section E (auditability)**: no new audit surface was added; settled decisions preserve the pre-PR audit contract.
- **Section F (concurrency / idempotency)**: `_load_mirror_equity` is a pure read helper, idempotent across re-entry; no new raises inside shared-transaction helpers.
- **Section G (interface cleanliness)**: helper lives in `services/portfolio.py`, not in a provider; matches settled decision.
- **Section H (scope discipline)**: confirm no frontend UI changes beyond type sync; no new REST endpoint; no schema changes; no `GuardResult.total_aum` field.
- **Section I (settled decisions)**: already documented at the top of this plan.
- **Section J (prevention log)**: already documented at the top of this plan; verify none of the listed anti-patterns appear in the diff.
- **Section K (frontend diff branch)**: PR touches `frontend/`, so read the async-data-loading, loading-error-empty-states, safety-state-ui, api-shape-and-types, and operator-ui-conventions skills. The only frontend changes are the type interface + 3 fixture literal updates — no new async surfaces, no new loading/error/empty states, no new DOM.

- [ ] **Step 4: Run the same-class scan on modified files**

```bash
# fetchone() occurrences
grep -nE "fetchone\(\)" app/services/portfolio.py \
    app/services/execution_guard.py app/api/portfolio.py

# row[0] positional access
grep -nE "row\[0\]" app/services/portfolio.py \
    app/services/execution_guard.py app/api/portfolio.py
```

Each match must be deliberate. `_load_mirror_equity`'s `fetchone()` is the new one; confirm the commentary covers why no None guard.

- [ ] **Step 5: Push the branch and open the PR**

```bash
git push -u origin feature/187-mirror-equity-aum-correction
gh pr create --title "feat(#187): mirror_equity AUM correction across 3 call sites" --body "$(cat <<'EOF'
## Summary

- Adds `_load_mirror_equity(conn) -> float` helper in `app/services/portfolio.py` running the spec §3.4 CTE (LATERAL-joined quote lookup, `WHERE m.active` filter, `COALESCE(SUM(...), 0)` guarantee).
- Wires the helper into all three AUM call sites:
  - `app/services/execution_guard.py:_load_sector_exposure` (the private helper `evaluate_recommendation` calls — `GuardResult` is unchanged, AUM remains a local variable)
  - `app/api/portfolio.py:get_portfolio` (adds `mirror_equity: float = 0.0` to `PortfolioResponse` and frontend `types.ts` for contract parity)
  - `app/services/portfolio.py:run_portfolio_review` (summed into the existing `total_aum` scalar; log line now reports mirror contribution)
- Adds 3 new fixtures to `tests/fixtures/copy_mirrors.py`: `mirror_aum_fixture` (load-bearing for §8.4/§8.5/§8.6), `no_quote_mirror_fixture` (cost-basis identity), `mtm_delta_mirror_fixture` (FX-aware delta with long/short toggle).
- Adds 4 new test modules totalling 15 tests covering §8.4 (identity), §8.5 (guard), §8.6 (per-call-site delta).

## Why

Spec §3 identified that `closed_positions_net_profit` is double-counted when added on top of `available + SUM(amount)`, and that `units * open_rate` is cross-currency nonsense for non-USD mirror positions. Before this PR, mirror equity was entirely absent from eBull's AUM denominator — the guard, the dashboard, and the recommendation engine all saw a figure that excluded ~40% of the operator's committed capital. This PR closes that gap at every call site that reads AUM.

## Security model

- No new external inputs. `_load_mirror_equity` takes a psycopg connection and runs a static CTE with no user-controlled identifiers.
- No new authorization surface. `GET /api/portfolio` remains session-protected; the new `mirror_equity` field is read from the same DB state the caller is already authorized for.
- No new provider calls. The helper reads from `copy_mirrors` / `copy_mirror_positions` / `quotes`, all populated by the Track 1a sync.

## Conscious tradeoffs

- **`mirror_equity` returns `float`, not `float | None`**: `COALESCE(SUM(...), 0)` guarantees a numeric result. `mirror_equity = None` would be a type lie and force the frontend to branch on null. See spec §6.4.
- **No `GuardResult.total_aum` field**: AUM is a local variable inside `evaluate_recommendation`, consumed by the concentration rule and not exposed on the return value. Adding a field for a test to assert against would be theatre.
- **No audit persistence**: no `portfolio_reviews` table exists. If future tickets want a per-review AUM snapshot, they get their own migration.
- **MTM delta uses entry-time FX rate, not current FX**: An approximation — a proper current-FX MTM requires a currency-aware `quotes` table (Track 2 scope). Documented in the helper docstring.
- **Frontend types sync, but no new UI**: `PortfolioResponse.mirror_equity` is added to `types.ts` in the same PR per api-shape-and-types rule. The dashboard already renders `total_aum` and picks up the corrected value for free. A dedicated "positions + cash + mirrors" breakdown panel lives in Track 1.5 (#188).
- **Prevention discipline**: `_load_mirror_equity`'s `fetchone()` has no dead `if row is None` guard because the CTE's `COALESCE(SUM(...), 0)` always returns exactly one row. Enforced in review-prevention-log #75.

## Test plan

- [x] `uv run ruff check .`
- [x] `uv run ruff format --check .`
- [x] `uv run pyright`
- [x] `uv run pytest` — including the 4 new test modules and the `tests/smoke/test_app_boots.py` gate
- [x] `pnpm --dir frontend typecheck`
- [x] `pnpm --dir frontend test`
- [x] §8.4: empty copy_mirrors → 0.0, no-quote identity, MTM+FX, short sign, closed mirror, all-closed
- [x] §8.5: empty baseline, active mirror, closed mirror, sector numerator unchanged
- [x] §8.6: API (Test 1), review (Test 2), guard (Test 3) — each with soft-close baseline repeat

Closes #187.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

- [ ] **Step 6: Poll the PR for review and CI**

```bash
# Repeat until the Claude review has posted and CI has reported
gh pr view <pr_number> --comments
gh pr checks <pr_number>
```

Follow the post-push cycle discipline from CLAUDE.md: do not push a follow-up until the Claude review on the latest commit has posted. Resolve every comment per the FIXED / DEFERRED / REBUTTED contract. Do NOT re-rebut comments that were already rebutted on a prior review (memory: `feedback_review_repeat_comments.md`).

- [ ] **Step 7: Merge after APPROVE on the most recent commit with CI green**

Squash-merge, then delete local and remote branches, and close issue #187.
