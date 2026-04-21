# Alerts strip — guard rejections (#315 Phase 3) implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship #315 Phase 3: a guard-rejection alerts strip on the operator dashboard with a decision_id-cursor ack model, normal "Mark all read" and overflow "Dismiss all" paths.

**Architecture:** One migration (`sql/044_operators_alerts_seen.sql`); one new FastAPI router (`app/api/alerts.py`) exposing `GET /alerts/guard-rejections`, `POST /alerts/seen`, `POST /alerts/dismiss-all`; one new React component (`frontend/src/components/dashboard/AlertsStrip.tsx`) wired into `DashboardPage.tsx` between `RollingPnlStrip` and `PortfolioValueChart`. Cursor is `operators.alerts_last_seen_decision_id BIGINT` — keyed off `decision_audit.decision_id` (BIGSERIAL) rather than timestamps to avoid clock-skew ordering holes and tie races.

**Tech Stack:** Python 3.13, FastAPI, psycopg3, Pydantic v2, pytest, TypeScript, React 19, Vite, vitest, Tailwind, React Router v7.

**Spec:** [docs/superpowers/specs/2026-04-21-alerts-strip-guard-rejections.md](../specs/2026-04-21-alerts-strip-guard-rejections.md)

---

## File structure

**Create:**

- `sql/044_operators_alerts_seen.sql` — migration: `operators.alerts_last_seen_decision_id BIGINT` column + partial index on `decision_audit (decision_time DESC) WHERE pass_fail='FAIL' AND stage='execution_guard'`.
- `app/api/alerts.py` — FastAPI router with three endpoints, Pydantic response models, operator-scoped auth via `sole_operator_id`.
- `tests/test_api_alerts.py` — backend tests: operator-resolution 503/501 (×2 endpoints), GET empty/row-shape/null-action/HOLD/SQL-shape/unseen-count-shape, POST /seen validation/monotonic/clamp/SQL-shape/503/501, POST /dismiss-all happy/scope/noop/503/501, plus four ebull_test integration tests (clock-skew ordering, clamp-to-MAX, empty-window no-op, non-guard exclusion).
- `frontend/src/api/alerts.ts` — `fetchGuardRejections`, `markAlertsSeen`, `dismissAllAlerts`.
- `frontend/src/components/dashboard/AlertsStrip.tsx` — strip component.
- `frontend/src/components/dashboard/AlertsStrip.test.tsx` — 11 component tests.

**Modify:**

- `app/main.py` — register `alerts_router` in the alphabetised `include_router` block.
- `frontend/src/api/types.ts` — add `GuardRejectionAction`, `GuardRejection`, `GuardRejectionsResponse` types.
- `frontend/src/lib/format.ts` — add `formatRelativeTime(iso: string | null | undefined): string` helper.
- `frontend/src/pages/DashboardPage.tsx` — mount `<AlertsStrip />` between `<RollingPnlStrip />` and `<PortfolioValueChart />`. Rationale: operator's reading flow is totals → short-horizon delta → **alerts requiring action** → long-horizon trajectory → positions. Alerts belong above the narrative chart because they are the "needs me now" surface. The component owns its own `useAsync(fetchGuardRejections)` for render-surface isolation (matches `RollingPnlStrip`); no page-level fetch is added.

All work lands on `feature/315-phase3-alerts-strip`. The spec commit is already on that branch.

---

## Task 1: Migration 044 — schema

**Files:**

- Create: `sql/044_operators_alerts_seen.sql`

- [ ] **Step 1: Write the migration**

```sql
-- Migration 044: operator-scoped alert acknowledgement + guard-rejection read index
--
-- 1. operators.alerts_last_seen_decision_id — NULL = never acknowledged (all in-window rows unseen).
--    Integer cursor keyed off decision_audit.decision_id (BIGSERIAL, unique).
--    Timestamp-based cursor was rejected: decision_time is TIMESTAMPTZ (microsecond resolution,
--    NOT unique under load), which leaves a tie-break race where a row inserted between GET
--    and POST at the same decision_time as rejections[0] would be silently acked. decision_id
--    is monotonic for the guard stage (single-threaded scheduler invocations; no concurrent
--    writers), so a strict > comparison fully closes the race.
-- 2. Partial index on decision_audit supports the dashboard GET scan.
--    Narrowed to stage='execution_guard' + pass_fail='FAIL' because
--    (a) the /alerts endpoint filters on both, (b) other stages write to
--    decision_audit (e.g. order_execution, deferred_retry) and must not
--    be indexed as guard rejections.

ALTER TABLE operators
    ADD COLUMN IF NOT EXISTS alerts_last_seen_decision_id BIGINT;

CREATE INDEX IF NOT EXISTS idx_decision_audit_guard_failed_recent
    ON decision_audit (decision_time DESC)
    WHERE pass_fail = 'FAIL' AND stage = 'execution_guard';
```

- [ ] **Step 2: Apply the migration against the dev DB**

Run: `psql "$DATABASE_URL" -f sql/044_operators_alerts_seen.sql`
Expected: two `ALTER/CREATE` notices; exit code 0. Re-running is a no-op (both `IF NOT EXISTS`).

- [ ] **Step 3: Verify the column and index exist**

Run:

```bash
psql "$DATABASE_URL" -c "\d operators" | grep alerts_last_seen_decision_id
psql "$DATABASE_URL" -c "\d decision_audit" | grep idx_decision_audit_guard_failed_recent
```

Expected: each grep returns one line.

- [ ] **Step 4: Commit**

```bash
git add sql/044_operators_alerts_seen.sql
git commit -m "feat(#315): migration 044 — alerts_last_seen_decision_id + guard-failed index"
```

---

## Task 2: Pydantic models and router skeleton

**Files:**

- Create: `app/api/alerts.py`
- Modify: `app/main.py`

- [ ] **Step 1: Write the failing test — router is mounted and returns 503 when no operator exists**

Create `tests/test_api_alerts.py`:

```python
"""Tests for the alerts API (#315 Phase 3)."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch
from uuid import UUID

import psycopg
import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.services.operators import AmbiguousOperatorError, NoOperatorError


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


@pytest.fixture(autouse=True)
def cleanup() -> Iterator[None]:
    yield
    from app.db import get_conn

    app.dependency_overrides.pop(get_conn, None)


def _install_conn(
    fetchone_returns: list[object] | None = None,
    fetchall_returns: list[object] | None = None,
    rowcount: int = 1,
) -> MagicMock:
    """Stub DB whose cursor feeds fetchone/fetchall in the order supplied."""
    conn = MagicMock()
    cur = MagicMock()
    cur.__enter__.return_value = cur
    cur.__exit__.return_value = None
    if fetchone_returns is not None:
        cur.fetchone.side_effect = list(fetchone_returns)
    if fetchall_returns is not None:
        cur.fetchall.return_value = fetchall_returns
    cur.rowcount = rowcount
    conn.cursor.return_value = cur
    conn.commit = MagicMock()

    def _dep() -> Iterator[MagicMock]:
        yield conn

    from app.db import get_conn

    app.dependency_overrides[get_conn] = _dep
    return cur


def test_get_returns_503_when_no_operator(client: TestClient) -> None:
    with patch("app.api.alerts.sole_operator_id", side_effect=NoOperatorError()):
        _install_conn()
        resp = client.get("/alerts/guard-rejections")
    assert resp.status_code == 503


def test_get_returns_501_when_multiple_operators(client: TestClient) -> None:
    with patch(
        "app.api.alerts.sole_operator_id",
        side_effect=AmbiguousOperatorError(),
    ):
        _install_conn()
        resp = client.get("/alerts/guard-rejections")
    assert resp.status_code == 501
```

- [ ] **Step 2: Run the tests and verify they fail**

Run: `uv run pytest tests/test_api_alerts.py -v`
Expected: two tests collected, both FAIL (either 404 because the router isn't mounted, or ImportError on `app.api.alerts`).

- [ ] **Step 3: Write the router skeleton**

Create `app/api/alerts.py`:

```python
"""Alerts API (#315 Phase 3).

Guard-rejection alerts strip. Scope is intentionally narrow — this is
the execution-guard read surface only. Thesis breaches (#394) and
filings-status drops (#395) are deferred; #396 wires them into the
same strip once their event persistence lands.

Cursor model: operators.alerts_last_seen_decision_id (BIGINT). See
``docs/superpowers/specs/2026-04-21-alerts-strip-guard-rejections.md``
for why a decision_id cursor rather than decision_time.

Routes:
  GET  /alerts/guard-rejections   — 7-day window, 500-row cap, ORDER BY decision_id DESC
  POST /alerts/seen               — body {seen_through_decision_id}, monotonic GREATEST + LEAST clamp
  POST /alerts/dismiss-all        — no body, atomic MAX-in-window advance, no-op on empty window
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal
from uuid import UUID

import psycopg
import psycopg.rows
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field

from app.api.auth import require_session_or_service_token
from app.db import get_conn
from app.services.operators import AmbiguousOperatorError, NoOperatorError, sole_operator_id

router = APIRouter(
    prefix="/alerts",
    tags=["alerts"],
    dependencies=[Depends(require_session_or_service_token)],
)

GuardAction = Literal["BUY", "ADD", "HOLD", "EXIT"]


class GuardRejection(BaseModel):
    decision_id: int
    decision_time: datetime
    instrument_id: int | None
    symbol: str | None
    action: GuardAction | None
    explanation: str


class GuardRejectionsResponse(BaseModel):
    alerts_last_seen_decision_id: int | None
    unseen_count: int
    rejections: list[GuardRejection]


class MarkSeenRequest(BaseModel):
    seen_through_decision_id: int = Field(gt=0)


def _resolve_operator(conn: psycopg.Connection[object]) -> UUID:
    try:
        return sole_operator_id(conn)
    except NoOperatorError as exc:
        raise HTTPException(status_code=503, detail="no operator configured") from exc
    except AmbiguousOperatorError as exc:
        raise HTTPException(
            status_code=501,
            detail="multiple operators present — alerts require a per-session operator context",
        ) from exc


@router.get("/guard-rejections", response_model=GuardRejectionsResponse)
def get_guard_rejections(
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> GuardRejectionsResponse:
    _resolve_operator(conn)
    # Implementation in Task 3.
    return GuardRejectionsResponse(
        alerts_last_seen_decision_id=None,
        unseen_count=0,
        rejections=[],
    )


@router.post("/seen", status_code=status.HTTP_204_NO_CONTENT)
def mark_seen(
    body: MarkSeenRequest,
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> None:
    _resolve_operator(conn)
    # Implementation in Task 4.


@router.post("/dismiss-all", status_code=status.HTTP_204_NO_CONTENT)
def dismiss_all(
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> None:
    _resolve_operator(conn)
    # Implementation in Task 5.
```

- [ ] **Step 4: Register the router**

Edit `app/main.py`. Find the alphabetised import block (line ~14) and insert:

```python
from app.api.alerts import router as alerts_router
```

Find the `include_router` block (line ~157) and insert in alphabetical order (before `attribution_router`):

```python
app.include_router(alerts_router)
```

- [ ] **Step 5: Run the tests and verify they pass**

Run: `uv run pytest tests/test_api_alerts.py -v`
Expected: both tests PASS.

- [ ] **Step 6: Commit**

```bash
git add app/api/alerts.py app/main.py tests/test_api_alerts.py
git commit -m "feat(#315): alerts router skeleton + 503/501 operator-resolution tests"
```

---

## Task 3: `GET /alerts/guard-rejections` — tests + implementation

**Files:**

- Modify: `tests/test_api_alerts.py`
- Modify: `app/api/alerts.py`

- [ ] **Step 1: Add failing tests — empty state, row shape, unseen_count anchor, ordering**

Append to `tests/test_api_alerts.py`:

```python
_OP_ID = UUID("00000000-0000-0000-0000-000000000001")


def _rejection_row(
    *,
    decision_id: int,
    decision_time: datetime | None = None,
    instrument_id: int | None = 42,
    symbol: str | None = "AAPL",
    action: str | None = "BUY",
    explanation: str = "FAIL — cash_available: need £200, have £50",
) -> dict[str, object]:
    return {
        "decision_id": decision_id,
        "decision_time": decision_time or datetime(2026, 4, 21, tzinfo=timezone.utc),
        "instrument_id": instrument_id,
        "symbol": symbol,
        "action": action,
        "explanation": explanation,
    }


def test_get_empty_state(client: TestClient) -> None:
    with patch("app.api.alerts.sole_operator_id", return_value=_OP_ID):
        # fetchone #1 = operator's alerts_last_seen_decision_id (NULL)
        # fetchone #2 = unseen_count (0)
        _install_conn(
            fetchone_returns=[{"alerts_last_seen_decision_id": None}, {"unseen_count": 0}],
            fetchall_returns=[],
        )
        resp = client.get("/alerts/guard-rejections")
    assert resp.status_code == 200
    body = resp.json()
    assert body == {
        "alerts_last_seen_decision_id": None,
        "unseen_count": 0,
        "rejections": [],
    }


def test_get_returns_row_shape_and_unseen_count(client: TestClient) -> None:
    rows = [
        _rejection_row(decision_id=501, action="BUY"),
        _rejection_row(decision_id=500, action="ADD", symbol="MSFT", instrument_id=43),
    ]
    with patch("app.api.alerts.sole_operator_id", return_value=_OP_ID):
        _install_conn(
            fetchone_returns=[
                {"alerts_last_seen_decision_id": 499},
                {"unseen_count": 2},
            ],
            fetchall_returns=rows,
        )
        resp = client.get("/alerts/guard-rejections")
    assert resp.status_code == 200
    body = resp.json()
    assert body["alerts_last_seen_decision_id"] == 499
    assert body["unseen_count"] == 2
    assert len(body["rejections"]) == 2
    assert body["rejections"][0]["decision_id"] == 501
    assert body["rejections"][0]["symbol"] == "AAPL"
    assert body["rejections"][0]["action"] == "BUY"


def test_get_null_instrument_and_action_serialise(client: TestClient) -> None:
    rows = [
        _rejection_row(decision_id=1, instrument_id=None, symbol=None, action=None),
    ]
    with patch("app.api.alerts.sole_operator_id", return_value=_OP_ID):
        _install_conn(
            fetchone_returns=[
                {"alerts_last_seen_decision_id": None},
                {"unseen_count": 1},
            ],
            fetchall_returns=rows,
        )
        resp = client.get("/alerts/guard-rejections")
    assert resp.status_code == 200
    row = resp.json()["rejections"][0]
    assert row["instrument_id"] is None
    assert row["symbol"] is None
    assert row["action"] is None


def test_get_hold_action_round_trip(client: TestClient) -> None:
    rows = [_rejection_row(decision_id=7, action="HOLD")]
    with patch("app.api.alerts.sole_operator_id", return_value=_OP_ID):
        _install_conn(
            fetchone_returns=[
                {"alerts_last_seen_decision_id": None},
                {"unseen_count": 1},
            ],
            fetchall_returns=rows,
        )
        resp = client.get("/alerts/guard-rejections")
    assert resp.json()["rejections"][0]["action"] == "HOLD"


def test_get_sql_shape_pins_window_and_scope_and_cap(client: TestClient) -> None:
    """Pin SQL-shape invariants that the contract depends on:
      - 7-day window filter (INTERVAL '7 days')
      - pass_fail = 'FAIL' (uppercase) + stage = 'execution_guard'
      - ORDER BY decision_id DESC (NOT decision_time DESC)
      - LIMIT 500
    """
    with patch("app.api.alerts.sole_operator_id", return_value=_OP_ID):
        cur = _install_conn(
            fetchone_returns=[
                {"alerts_last_seen_decision_id": None},
                {"unseen_count": 0},
            ],
            fetchall_returns=[],
        )
        resp = client.get("/alerts/guard-rejections")
    assert resp.status_code == 200
    list_sql = next(
        c.args[0]
        for c in cur.execute.call_args_list
        if "FROM decision_audit da" in c.args[0]
    )
    assert "pass_fail = 'FAIL'" in list_sql
    assert "stage = 'execution_guard'" in list_sql
    assert "INTERVAL '7 days'" in list_sql
    assert "ORDER BY da.decision_id DESC" in list_sql
    assert "LIMIT 500" in list_sql


def test_get_unseen_count_query_uses_strict_gt_on_decision_id(client: TestClient) -> None:
    """unseen_count query uses strict `decision_id > last_id` (race-safety)."""
    with patch("app.api.alerts.sole_operator_id", return_value=_OP_ID):
        cur = _install_conn(
            fetchone_returns=[
                {"alerts_last_seen_decision_id": 100},
                {"unseen_count": 3},
            ],
            fetchall_returns=[],
        )
        client.get("/alerts/guard-rejections")
    count_sql = next(
        c.args[0]
        for c in cur.execute.call_args_list
        if "SELECT COUNT(*)" in c.args[0]
    )
    # Strict `>` ties are structurally impossible on a unique PK.
    assert "decision_id > %(last_id)s" in count_sql
    # Filter matches list query so counts and rows agree.
    assert "pass_fail = 'FAIL'" in count_sql
    assert "stage = 'execution_guard'" in count_sql
    assert "INTERVAL '7 days'" in count_sql
    # NULL last-seen path counts everything in window.
    assert "%(last_id)s IS NULL" in count_sql
```

- [ ] **Step 2: Run the tests and verify they fail**

Run: `uv run pytest tests/test_api_alerts.py -v`
Expected: the four new tests FAIL (the stub returns an empty list and `unseen_count=0`; only `test_get_empty_state` passes by coincidence — the others fail because `rejections` is empty and fields don't match). `test_get_null_instrument_and_action_serialise` may pass if it also sees an empty list. That's OK — you still need the real query.

- [ ] **Step 3: Implement the GET endpoint**

In `app/api/alerts.py`, replace the `get_guard_rejections` body with:

```python
@router.get("/guard-rejections", response_model=GuardRejectionsResponse)
def get_guard_rejections(
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> GuardRejectionsResponse:
    operator_id = _resolve_operator(conn)
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        # 1. Read operator's cursor.
        cur.execute(
            "SELECT alerts_last_seen_decision_id FROM operators WHERE operator_id = %(op)s",
            {"op": operator_id},
        )
        op_row = cur.fetchone()
        last_seen: int | None = op_row["alerts_last_seen_decision_id"] if op_row else None

        # 2. Count unseen in-window rows (uncapped).
        cur.execute(
            """
            SELECT COUNT(*) AS unseen_count
            FROM decision_audit
            WHERE pass_fail = 'FAIL'
              AND stage = 'execution_guard'
              AND decision_time >= now() - INTERVAL '7 days'
              AND (%(last_id)s IS NULL OR decision_id > %(last_id)s)
            """,
            {"last_id": last_seen},
        )
        count_row = cur.fetchone()
        unseen_count: int = int(count_row["unseen_count"]) if count_row else 0

        # 3. Fetch the list (capped at 500). Ordering is by decision_id DESC
        # (the PK sequence), not decision_time DESC — decision_time is app-supplied
        # via _utcnow() and can be clock-skewed, which would break the invariant
        # that rejections[0].decision_id === MAX(decision_id).
        cur.execute(
            """
            SELECT
                da.decision_id,
                da.decision_time,
                da.instrument_id,
                i.symbol,
                tr.action,
                da.explanation
            FROM decision_audit da
            LEFT JOIN instruments i ON i.instrument_id = da.instrument_id
            LEFT JOIN trade_recommendations tr ON tr.recommendation_id = da.recommendation_id
            WHERE da.pass_fail = 'FAIL'
              AND da.stage = 'execution_guard'
              AND da.decision_time >= now() - INTERVAL '7 days'
            ORDER BY da.decision_id DESC
            LIMIT 500
            """
        )
        rows = cur.fetchall()

    return GuardRejectionsResponse(
        alerts_last_seen_decision_id=last_seen,
        unseen_count=unseen_count,
        rejections=[GuardRejection.model_validate(r) for r in rows],
    )
```

- [ ] **Step 4: Run the tests and verify they pass**

Run: `uv run pytest tests/test_api_alerts.py -v`
Expected: all seven tests PASS.

- [ ] **Step 5: Commit**

```bash
git add app/api/alerts.py tests/test_api_alerts.py
git commit -m "feat(#315): GET /alerts/guard-rejections — 7d window, 500 cap, decision_id ordering"
```

---

## Task 4: `POST /alerts/seen` — tests + implementation

**Files:**

- Modify: `tests/test_api_alerts.py`
- Modify: `app/api/alerts.py`

- [ ] **Step 1: Add failing tests — validation, monotonic, clamp**

Append to `tests/test_api_alerts.py`:

```python
def test_post_seen_rejects_missing_field(client: TestClient) -> None:
    with patch("app.api.alerts.sole_operator_id", return_value=_OP_ID):
        _install_conn()
        resp = client.post("/alerts/seen", json={})
    assert resp.status_code == 422


def test_post_seen_rejects_non_integer(client: TestClient) -> None:
    with patch("app.api.alerts.sole_operator_id", return_value=_OP_ID):
        _install_conn()
        resp = client.post("/alerts/seen", json={"seen_through_decision_id": "abc"})
    assert resp.status_code == 422


def test_post_seen_rejects_non_positive(client: TestClient) -> None:
    with patch("app.api.alerts.sole_operator_id", return_value=_OP_ID):
        _install_conn()
        resp = client.post("/alerts/seen", json={"seen_through_decision_id": 0})
    assert resp.status_code == 422


def test_post_seen_writes_update(client: TestClient) -> None:
    with patch("app.api.alerts.sole_operator_id", return_value=_OP_ID):
        cur = _install_conn(rowcount=1)
        resp = client.post("/alerts/seen", json={"seen_through_decision_id": 1234})
    assert resp.status_code == 204
    # One UPDATE call was issued with the posted value.
    calls = cur.execute.call_args_list
    assert any("UPDATE operators" in c.args[0] for c in calls)
    params = [c.args[1] for c in calls if "UPDATE operators" in c.args[0]][0]
    assert params["seen_through_decision_id"] == 1234
    assert params["op"] == _OP_ID


def test_post_seen_sql_shape_pins_greatest_and_least_and_scope(client: TestClient) -> None:
    """SQL must be: GREATEST(COALESCE(current, 0), LEAST(posted, MAX-in-window-or-0))
    with MAX subselect filtered to FAIL + execution_guard + 7-day window."""
    with patch("app.api.alerts.sole_operator_id", return_value=_OP_ID):
        cur = _install_conn(rowcount=1)
        resp = client.post("/alerts/seen", json={"seen_through_decision_id": 99999})
    assert resp.status_code == 204
    sql = next(c.args[0] for c in cur.execute.call_args_list if "UPDATE operators" in c.args[0])
    assert "GREATEST" in sql
    assert "LEAST" in sql
    assert "SELECT MAX(decision_id)" in sql
    assert "pass_fail = 'FAIL'" in sql
    assert "stage = 'execution_guard'" in sql
    assert "INTERVAL '7 days'" in sql


def test_post_seen_returns_503_when_no_operator(client: TestClient) -> None:
    with patch("app.api.alerts.sole_operator_id", side_effect=NoOperatorError()):
        _install_conn()
        resp = client.post("/alerts/seen", json={"seen_through_decision_id": 1})
    assert resp.status_code == 503


def test_post_seen_returns_501_when_multiple_operators(client: TestClient) -> None:
    with patch("app.api.alerts.sole_operator_id", side_effect=AmbiguousOperatorError()):
        _install_conn()
        resp = client.post("/alerts/seen", json={"seen_through_decision_id": 1})
    assert resp.status_code == 501
```

- [ ] **Step 2: Run the tests and verify they fail**

Run: `uv run pytest tests/test_api_alerts.py -v -k post_seen`
Expected: the validation tests pass (Pydantic handles those). `test_post_seen_writes_update` FAILS because the stub endpoint doesn't execute an UPDATE.

- [ ] **Step 3: Implement the endpoint**

In `app/api/alerts.py`, replace the `mark_seen` body with:

```python
@router.post("/seen", status_code=status.HTTP_204_NO_CONTENT)
def mark_seen(
    body: MarkSeenRequest,
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> None:
    operator_id = _resolve_operator(conn)
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE operators
            SET alerts_last_seen_decision_id = GREATEST(
                COALESCE(alerts_last_seen_decision_id, 0),
                LEAST(
                    %(seen_through_decision_id)s,
                    COALESCE((
                        SELECT MAX(decision_id)
                        FROM decision_audit
                        WHERE pass_fail = 'FAIL'
                          AND stage = 'execution_guard'
                          AND decision_time >= now() - INTERVAL '7 days'
                    ), 0)
                )
            )
            WHERE operator_id = %(op)s
            """,
            {
                "seen_through_decision_id": body.seen_through_decision_id,
                "op": operator_id,
            },
        )
    conn.commit()
```

- [ ] **Step 4: Run the tests and verify they pass**

Run: `uv run pytest tests/test_api_alerts.py -v`
Expected: all tests PASS.

- [ ] **Step 5: Commit**

```bash
git add app/api/alerts.py tests/test_api_alerts.py
git commit -m "feat(#315): POST /alerts/seen — monotonic GREATEST + LEAST clamp to in-window MAX"
```

---

## Task 5: `POST /alerts/dismiss-all` — tests + implementation

**Files:**

- Modify: `tests/test_api_alerts.py`
- Modify: `app/api/alerts.py`

- [ ] **Step 1: Add failing tests — happy path, empty window no-op, non-guard excluded**

Append to `tests/test_api_alerts.py`:

```python
def test_post_dismiss_all_issues_update(client: TestClient) -> None:
    with patch("app.api.alerts.sole_operator_id", return_value=_OP_ID):
        cur = _install_conn(rowcount=1)
        resp = client.post("/alerts/dismiss-all")
    assert resp.status_code == 204
    calls = cur.execute.call_args_list
    assert any(
        "UPDATE operators" in c.args[0] and "SELECT MAX(decision_id)" in c.args[0]
        for c in calls
    )


def test_post_dismiss_all_filters_scope_to_guard_fails_in_window(client: TestClient) -> None:
    # Inspect the SQL shape — scope is execution_guard + FAIL + 7-day window.
    with patch("app.api.alerts.sole_operator_id", return_value=_OP_ID):
        cur = _install_conn(rowcount=1)
        resp = client.post("/alerts/dismiss-all")
    assert resp.status_code == 204
    update_sql = next(
        c.args[0] for c in cur.execute.call_args_list if "UPDATE operators" in c.args[0]
    )
    assert "pass_fail = 'FAIL'" in update_sql
    assert "stage = 'execution_guard'" in update_sql
    assert "INTERVAL '7 days'" in update_sql
    assert "m.max_id IS NOT NULL" in update_sql


def test_post_dismiss_all_is_noop_on_zero_rowcount(client: TestClient) -> None:
    # rowcount=0 mimics the empty-window case (WHERE m.max_id IS NOT NULL excludes the row).
    with patch("app.api.alerts.sole_operator_id", return_value=_OP_ID):
        _install_conn(rowcount=0)
        resp = client.post("/alerts/dismiss-all")
    assert resp.status_code == 204  # No-op still returns 204.


def test_post_dismiss_all_returns_503_when_no_operator(client: TestClient) -> None:
    with patch("app.api.alerts.sole_operator_id", side_effect=NoOperatorError()):
        _install_conn()
        resp = client.post("/alerts/dismiss-all")
    assert resp.status_code == 503


def test_post_dismiss_all_returns_501_when_multiple_operators(client: TestClient) -> None:
    with patch("app.api.alerts.sole_operator_id", side_effect=AmbiguousOperatorError()):
        _install_conn()
        resp = client.post("/alerts/dismiss-all")
    assert resp.status_code == 501
```

- [ ] **Step 2: Run the tests and verify they fail**

Run: `uv run pytest tests/test_api_alerts.py -v -k dismiss_all`
Expected: all three FAIL — the stub doesn't issue any UPDATE.

- [ ] **Step 3: Implement the endpoint**

In `app/api/alerts.py`, replace the `dismiss_all` body with:

```python
@router.post("/dismiss-all", status_code=status.HTTP_204_NO_CONTENT)
def dismiss_all(
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> None:
    operator_id = _resolve_operator(conn)
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE operators AS op
            SET alerts_last_seen_decision_id = GREATEST(
                COALESCE(op.alerts_last_seen_decision_id, 0),
                m.max_id
            )
            FROM (
                SELECT MAX(decision_id) AS max_id
                FROM decision_audit
                WHERE pass_fail = 'FAIL'
                  AND stage = 'execution_guard'
                  AND decision_time >= now() - INTERVAL '7 days'
            ) AS m
            WHERE op.operator_id = %(op)s
              AND m.max_id IS NOT NULL
            """,
            {"op": operator_id},
        )
    conn.commit()
```

- [ ] **Step 4: Run the tests and verify they pass**

Run: `uv run pytest tests/test_api_alerts.py -v`
Expected: all tests PASS.

- [ ] **Step 5: Commit**

```bash
git add app/api/alerts.py tests/test_api_alerts.py
git commit -m "feat(#315): POST /alerts/dismiss-all — atomic MAX advance, no-op on empty window"
```

---

## Task 6: Backend integration tests against real `ebull_test`

The mock-cursor tests above verify contract and SQL shape. This task adds integration tests that hit the real `ebull_test` DB using the existing `ebull_test_conn` fixture at [tests/fixtures/ebull_test_db.py](../../tests/fixtures/ebull_test_db.py). Four scenarios only — ordering under simulated clock skew, clamp-to-MAX, dismiss-all empty-window no-op, non-guard exclusion.

**Files:**

- Modify: `tests/test_api_alerts.py`
- Modify: `tests/fixtures/ebull_test_db.py` — extend `_PLANNER_TABLES` (or add a new tuple) to include `operators`, `decision_audit`, `trade_recommendations`, `instruments` so they are TRUNCATEd between tests.

- [ ] **Step 1: Extend the TRUNCATE list**

Read `tests/fixtures/ebull_test_db.py:45-57`. The existing `_PLANNER_TABLES` tuple already includes `instruments`. Append the two new tables needed here:

```python
_PLANNER_TABLES: tuple[str, ...] = (
    "cascade_retry_queue",
    "financial_facts_raw",
    "data_ingestion_runs",
    "external_identifiers",
    "external_data_watermarks",
    "instruments",
    "job_runs",
    "financial_periods_raw",
    "financial_periods",
    "filing_events",
    "decision_audit",          # new — #315 Phase 3 alerts
    "trade_recommendations",   # new — #315 Phase 3 alerts (FK parent of decision_audit)
    "operators",               # new — #315 Phase 3 alerts (cursor column)
)
```

Dependency order matters: `decision_audit` → `trade_recommendations` (FK via recommendation_id) → `instruments`. TRUNCATE with CASCADE handles the rest. Keep `operators` last in the group since nothing FKs into it from the rows above.

- [ ] **Step 2: Add integration tests**

Append to `tests/test_api_alerts.py`. `import psycopg` is already in the top-level imports (added in Task 2's scaffold). Do NOT re-import mid-file — ruff `E402` forbids that.

```python
# --- Integration tests (real ebull_test DB) ----------------------------------

from tests.fixtures.ebull_test_db import ebull_test_conn, test_db_available  # noqa: F401,E402

_INT_OP_ID = UUID("11111111-1111-1111-1111-111111111111")


def _seed_operator(conn: psycopg.Connection[object]) -> None:
    """Insert a known operator row so sole_operator_id resolves and the
    UPDATE in /alerts/seen / /dismiss-all has a target row. `username`
    and `password_hash` are NOT NULL per sql/016_operators_sessions.sql;
    use dummy values — these rows are created for the alerts tests only
    and never authenticate."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO operators (operator_id, username, password_hash)
            VALUES (%s, 'alerts_test_op', 'x')
            ON CONFLICT DO NOTHING
            """,
            (_INT_OP_ID,),
        )
    conn.commit()


def _bind_test_client(conn: psycopg.Connection[object]) -> TestClient:
    """Bind TestClient's get_conn dep to the ebull_test connection + patch
    the operator resolver to return the seeded operator id. Returns a
    client whose requests run against ebull_test."""
    def _dep() -> Iterator[psycopg.Connection[object]]:
        yield conn

    from app.db import get_conn

    app.dependency_overrides[get_conn] = _dep
    return TestClient(app)


@pytest.mark.skipif("not test_db_available()")
def test_integration_get_orders_by_decision_id_under_clock_skew(
    ebull_test_conn: psycopg.Connection[object],
) -> None:
    """Row B gets a later decision_time but an earlier decision_id.
    GET must still put the row with the higher decision_id first."""
    _seed_operator(ebull_test_conn)

    with ebull_test_conn.cursor() as cur:
        # instruments.instrument_id is BIGINT PRIMARY KEY with no default
        # (sql/001_init.sql), so the caller supplies the id explicitly.
        cur.execute(
            "INSERT INTO instruments (instrument_id, symbol, company_name, currency, is_tradable) "
            "VALUES (1, 'ZZZ', 'Test', 'USD', TRUE)"
        )
        iid = 1

        # Insert Row B first (gets lower decision_id) with the LATER decision_time.
        cur.execute(
            "INSERT INTO decision_audit "
            "(decision_time, instrument_id, stage, pass_fail, explanation) "
            "VALUES (now(), %s, 'execution_guard', 'FAIL', 'B-later-time') "
            "RETURNING decision_id",
            (iid,),
        )
        id_b = cur.fetchone()[0]

        # Insert Row A second (gets HIGHER decision_id) with the EARLIER decision_time.
        cur.execute(
            "INSERT INTO decision_audit "
            "(decision_time, instrument_id, stage, pass_fail, explanation) "
            "VALUES (now() - INTERVAL '1 hour', %s, 'execution_guard', 'FAIL', 'A-earlier-time') "
            "RETURNING decision_id",
            (iid,),
        )
        id_a = cur.fetchone()[0]
    ebull_test_conn.commit()

    assert id_a > id_b  # sanity check on the natural PK ordering

    client = _bind_test_client(ebull_test_conn)
    try:
        with patch("app.api.alerts.sole_operator_id", return_value=_INT_OP_ID):
            resp = client.get("/alerts/guard-rejections")
        assert resp.status_code == 200
        rejections = resp.json()["rejections"]
        assert rejections[0]["decision_id"] == id_a
        assert rejections[1]["decision_id"] == id_b
    finally:
        app.dependency_overrides.pop("app.db.get_conn", None)
        from app.db import get_conn
        app.dependency_overrides.pop(get_conn, None)


@pytest.mark.skipif("not test_db_available()")
def test_integration_post_seen_clamps_to_in_window_max(
    ebull_test_conn: psycopg.Connection[object],
) -> None:
    _seed_operator(ebull_test_conn)
    with ebull_test_conn.cursor() as cur:
        cur.execute(
            "INSERT INTO decision_audit "
            "(decision_time, stage, pass_fail, explanation) "
            "VALUES (now(), 'execution_guard', 'FAIL', 'in-window') RETURNING decision_id"
        )
        max_in_window = cur.fetchone()[0]
    ebull_test_conn.commit()

    client = _bind_test_client(ebull_test_conn)
    try:
        with patch("app.api.alerts.sole_operator_id", return_value=_INT_OP_ID):
            resp = client.post(
                "/alerts/seen",
                json={"seen_through_decision_id": max_in_window + 99999},
            )
        assert resp.status_code == 204

        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT alerts_last_seen_decision_id FROM operators WHERE operator_id = %s",
                (_INT_OP_ID,),
            )
            stored = cur.fetchone()[0]
        assert stored == max_in_window
    finally:
        from app.db import get_conn
        app.dependency_overrides.pop(get_conn, None)


@pytest.mark.skipif("not test_db_available()")
def test_integration_dismiss_all_empty_window_stays_null(
    ebull_test_conn: psycopg.Connection[object],
) -> None:
    _seed_operator(ebull_test_conn)
    # No guard rows in window; cursor NULL; POST dismiss-all; cursor stays NULL.
    client = _bind_test_client(ebull_test_conn)
    try:
        with patch("app.api.alerts.sole_operator_id", return_value=_INT_OP_ID):
            resp = client.post("/alerts/dismiss-all")
        assert resp.status_code == 204
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT alerts_last_seen_decision_id FROM operators WHERE operator_id = %s",
                (_INT_OP_ID,),
            )
            assert cur.fetchone()[0] is None
    finally:
        from app.db import get_conn
        app.dependency_overrides.pop(get_conn, None)


@pytest.mark.skipif("not test_db_available()")
def test_integration_non_guard_stage_excluded_from_list_and_dismiss(
    ebull_test_conn: psycopg.Connection[object],
) -> None:
    _seed_operator(ebull_test_conn)
    with ebull_test_conn.cursor() as cur:
        cur.execute(
            "INSERT INTO decision_audit "
            "(decision_time, stage, pass_fail, explanation) "
            "VALUES (now(), 'order_execution', 'FAIL', 'not a guard') RETURNING decision_id"
        )
        id_non_guard = cur.fetchone()[0]
    ebull_test_conn.commit()

    client = _bind_test_client(ebull_test_conn)
    try:
        with patch("app.api.alerts.sole_operator_id", return_value=_INT_OP_ID):
            resp = client.get("/alerts/guard-rejections")
        ids = [r["decision_id"] for r in resp.json()["rejections"]]
        assert id_non_guard not in ids

        # dismiss-all MAX subselect is NULL (no guard rows) → no-op.
        with patch("app.api.alerts.sole_operator_id", return_value=_INT_OP_ID):
            resp = client.post("/alerts/dismiss-all")
        assert resp.status_code == 204
        with ebull_test_conn.cursor() as cur:
            cur.execute(
                "SELECT alerts_last_seen_decision_id FROM operators WHERE operator_id = %s",
                (_INT_OP_ID,),
            )
            assert cur.fetchone()[0] is None
    finally:
        from app.db import get_conn
        app.dependency_overrides.pop(get_conn, None)
```

Notes on the integration pattern:

- `ebull_test_conn` is imported from `tests/fixtures/ebull_test_db.py`; no new fixture is invented.
- Ordering under clock skew is exercised by inserting rows in the order that makes the natural BIGSERIAL sequence produce the needed id layout — **never** by hand-setting `decision_id`. Row B first (lower id, later time), Row A second (higher id, earlier time).
- Operator is seeded before each test because `sole_operator_id` is still patched for the integration tests (the fixture TRUNCATEs operators between tests, so seeding is required each time). The patch stays — test DB's operator table is empty and seeding one known row is simpler than running bootstrap.
- `TestClient` is rebound to the `ebull_test` connection via `app.dependency_overrides[get_conn]`; the `finally` block clears it even on test failure.
- `@pytest.mark.skipif("not test_db_available()")` skips gracefully when the test DB is unreachable (CI may not have it available).

- [ ] **Step 3: Run the integration tests**

Run: `uv run pytest tests/test_api_alerts.py -v`
Expected: all integration tests PASS if `ebull_test` is reachable; otherwise SKIPPED (not failed).

- [ ] **Step 4: Commit**

```bash
git add tests/test_api_alerts.py tests/fixtures/ebull_test_db.py
git commit -m "test(#315): integration tests for alerts API against ebull_test (ordering, clamp, dismiss-all scope)"
```

---

## Task 7: Frontend types + API client

**Files:**

- Modify: `frontend/src/api/types.ts`
- Create: `frontend/src/api/alerts.ts`

- [ ] **Step 1: Add types**

In `frontend/src/api/types.ts`, append:

```ts
// #315 Phase 3 — alerts strip
export type GuardRejectionAction = "BUY" | "ADD" | "HOLD" | "EXIT";

export interface GuardRejection {
  decision_id: number;
  decision_time: string;  // ISO TIMESTAMPTZ
  instrument_id: number | null;
  symbol: string | null;
  action: GuardRejectionAction | null;
  explanation: string;
}

export interface GuardRejectionsResponse {
  alerts_last_seen_decision_id: number | null;
  unseen_count: number;
  rejections: GuardRejection[];
}
```

- [ ] **Step 2: Create the API client**

Create `frontend/src/api/alerts.ts`:

```ts
import { apiFetch } from "@/api/client";
import type { GuardRejectionsResponse } from "@/api/types";

export function fetchGuardRejections(): Promise<GuardRejectionsResponse> {
  return apiFetch<GuardRejectionsResponse>("/alerts/guard-rejections");
}

export function markAlertsSeen(seenThroughDecisionId: number): Promise<void> {
  return apiFetch<void>("/alerts/seen", {
    method: "POST",
    body: JSON.stringify({ seen_through_decision_id: seenThroughDecisionId }),
  });
}

export function dismissAllAlerts(): Promise<void> {
  return apiFetch<void>("/alerts/dismiss-all", { method: "POST" });
}
```

- [ ] **Step 3: Verify apiFetch signature**

Run: `grep -n "export function apiFetch\|export async function apiFetch" frontend/src/api/client.ts`

Confirm the second argument accepts `method` and `body`. If the existing signature differs (e.g. takes a separate `init` object shape), adjust the calls above to match. Do not change `apiFetch` itself.

- [ ] **Step 4: Typecheck**

Run: `pnpm --dir frontend typecheck`
Expected: green.

- [ ] **Step 5: Commit**

```bash
git add frontend/src/api/types.ts frontend/src/api/alerts.ts
git commit -m "feat(#315): frontend types + API client for alerts strip"
```

---

## Task 8: `formatRelativeTime` helper

**Files:**

- Modify: `frontend/src/lib/format.ts`
- Create: `frontend/src/lib/format.test.ts` (or append if it exists)

- [ ] **Step 1: Check if a test file exists**

Run: `ls frontend/src/lib/format.test.ts 2>/dev/null || echo "no test file"`

If the file does not exist, create it with the standard header. If it exists, append the new tests.

- [ ] **Step 2: Write failing tests**

Create or append to `frontend/src/lib/format.test.ts`:

```ts
import { describe, expect, it, vi } from "vitest";
import { formatRelativeTime } from "@/lib/format";

describe("formatRelativeTime", () => {
  const NOW = new Date("2026-04-21T12:00:00Z");

  it("renders '—' for null / undefined / empty string", () => {
    expect(formatRelativeTime(null)).toBe("—");
    expect(formatRelativeTime(undefined)).toBe("—");
    expect(formatRelativeTime("")).toBe("—");
  });

  it("renders 'just now' for <60s delta", () => {
    vi.useFakeTimers();
    vi.setSystemTime(NOW);
    expect(formatRelativeTime("2026-04-21T11:59:30Z")).toBe("just now");
    vi.useRealTimers();
  });

  it("renders minutes for <1h delta", () => {
    vi.useFakeTimers();
    vi.setSystemTime(NOW);
    expect(formatRelativeTime("2026-04-21T11:55:00Z")).toBe("5m ago");
    vi.useRealTimers();
  });

  it("renders hours for <1d delta", () => {
    vi.useFakeTimers();
    vi.setSystemTime(NOW);
    expect(formatRelativeTime("2026-04-21T09:00:00Z")).toBe("3h ago");
    vi.useRealTimers();
  });

  it("renders days for <7d delta", () => {
    vi.useFakeTimers();
    vi.setSystemTime(NOW);
    expect(formatRelativeTime("2026-04-19T12:00:00Z")).toBe("2d ago");
    vi.useRealTimers();
  });

  it("falls back to formatDate for >=7d delta", () => {
    vi.useFakeTimers();
    vi.setSystemTime(NOW);
    const result = formatRelativeTime("2026-04-10T12:00:00Z");
    expect(result).toMatch(/2026/);
    vi.useRealTimers();
  });
});
```

- [ ] **Step 3: Run tests to verify failure**

Run: `pnpm --dir frontend test -- format.test.ts`
Expected: all six tests FAIL with "formatRelativeTime is not exported".

- [ ] **Step 4: Implement the helper**

In `frontend/src/lib/format.ts`, append:

```ts
/** Relative-time formatter for strip rows. Uses local system clock.
 *  <60s → "just now", <1h → "Nm ago", <1d → "Nh ago", <7d → "Nd ago",
 *  else → formatDate fallback. */
export function formatRelativeTime(iso: string | null | undefined): string {
  if (iso === null || iso === undefined || iso === "") return "—";
  const then = new Date(iso).getTime();
  if (Number.isNaN(then)) return "—";
  const deltaS = Math.floor((Date.now() - then) / 1000);
  if (deltaS < 60) return "just now";
  if (deltaS < 3600) return `${Math.floor(deltaS / 60)}m ago`;
  if (deltaS < 86400) return `${Math.floor(deltaS / 3600)}h ago`;
  if (deltaS < 604800) return `${Math.floor(deltaS / 86400)}d ago`;
  return formatDate(iso);
}
```

- [ ] **Step 5: Run tests to verify pass**

Run: `pnpm --dir frontend test -- format.test.ts`
Expected: all six PASS.

- [ ] **Step 6: Commit**

```bash
git add frontend/src/lib/format.ts frontend/src/lib/format.test.ts
git commit -m "feat(#315): formatRelativeTime helper"
```

---

## Task 9: `AlertsStrip` — hidden-when-empty, silent-on-error, row rendering

Build the component incrementally: first the empty / error states + row rendering, then the normal-path "Mark all read" (Task 10), then the overflow-path "Dismiss all" (Task 11).

**Files:**

- Create: `frontend/src/components/dashboard/AlertsStrip.test.tsx`
- Create: `frontend/src/components/dashboard/AlertsStrip.tsx`

- [ ] **Step 1: Write failing tests — empty, error, row shape, instrument link, unseen borders**

Create `frontend/src/components/dashboard/AlertsStrip.test.tsx`:

```tsx
import { render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { describe, expect, it, vi, beforeEach } from "vitest";

import { AlertsStrip } from "@/components/dashboard/AlertsStrip";
import type { GuardRejectionsResponse } from "@/api/types";

vi.mock("@/api/alerts", () => ({
  fetchGuardRejections: vi.fn(),
  markAlertsSeen: vi.fn(),
  dismissAllAlerts: vi.fn(),
}));

import * as alertsApi from "@/api/alerts";

const baseRow = {
  decision_id: 501,
  decision_time: new Date(Date.now() - 5 * 60 * 1000).toISOString(),
  instrument_id: 42,
  symbol: "AAPL",
  action: "BUY" as const,
  explanation: "FAIL — cash_available: need £200, have £50",
};

function stubFetch(data: GuardRejectionsResponse | null) {
  (alertsApi.fetchGuardRejections as unknown as vi.Mock).mockResolvedValue(data ?? {});
}

function stubFetchError() {
  (alertsApi.fetchGuardRejections as unknown as vi.Mock).mockRejectedValue(new Error("boom"));
}

function renderStrip() {
  return render(
    <MemoryRouter>
      <AlertsStrip />
    </MemoryRouter>,
  );
}

beforeEach(() => {
  vi.clearAllMocks();
});

describe("AlertsStrip", () => {
  it("renders nothing when rejections list is empty", async () => {
    stubFetch({
      alerts_last_seen_decision_id: null,
      unseen_count: 0,
      rejections: [],
    });
    const { container } = renderStrip();
    await vi.waitFor(() => {
      expect(alertsApi.fetchGuardRejections).toHaveBeenCalled();
    });
    expect(container).toBeEmptyDOMElement();
  });

  it("renders nothing on fetch error (silent-on-error)", async () => {
    stubFetchError();
    const { container } = renderStrip();
    await vi.waitFor(() => {
      expect(alertsApi.fetchGuardRejections).toHaveBeenCalled();
    });
    expect(container).toBeEmptyDOMElement();
  });

  it("renders row symbol / action / explanation", async () => {
    stubFetch({
      alerts_last_seen_decision_id: null,
      unseen_count: 1,
      rejections: [baseRow],
    });
    renderStrip();
    expect(await screen.findByText("AAPL")).toBeInTheDocument();
    expect(screen.getByText("BUY")).toBeInTheDocument();
    expect(
      screen.getByText(/cash_available: need £200/),
    ).toBeInTheDocument();
  });

  it("wraps row in a Link when instrument_id is non-null", async () => {
    stubFetch({
      alerts_last_seen_decision_id: null,
      unseen_count: 1,
      rejections: [baseRow],
    });
    renderStrip();
    const link = await screen.findByRole("link");
    expect(link.getAttribute("href")).toBe("/instruments/42");
  });

  it("renders plain row (no link) when instrument_id is null", async () => {
    stubFetch({
      alerts_last_seen_decision_id: null,
      unseen_count: 1,
      rejections: [{ ...baseRow, instrument_id: null, symbol: null }],
    });
    renderStrip();
    await screen.findByText(/cash_available/);
    expect(screen.queryByRole("link")).toBeNull();
  });

  it("applies amber border for unseen rows, slate for seen", async () => {
    stubFetch({
      alerts_last_seen_decision_id: 500,
      unseen_count: 1,
      rejections: [
        { ...baseRow, decision_id: 501 },  // unseen (501 > 500)
        { ...baseRow, decision_id: 499 },  // seen (499 <= 500)
      ],
    });
    renderStrip();
    const rows = await screen.findAllByTestId("alerts-row");
    expect(rows[0].className).toMatch(/border-amber/);
    expect(rows[1].className).toMatch(/border-slate/);
  });

  it("shows unseen_count pill when unseen_count > 0", async () => {
    stubFetch({
      alerts_last_seen_decision_id: null,
      unseen_count: 3,
      rejections: [baseRow],
    });
    renderStrip();
    expect(await screen.findByText(/3 new/)).toBeInTheDocument();
  });

  it("omits unseen_count pill when unseen_count === 0", async () => {
    stubFetch({
      alerts_last_seen_decision_id: 999,
      unseen_count: 0,
      rejections: [baseRow],  // still shown, just all seen
    });
    renderStrip();
    await screen.findByText("AAPL");
    expect(screen.queryByText(/new$/)).toBeNull();
  });

  it("truncates explanation visually but preserves full text in title", async () => {
    const long = "FAIL — cash_available: need £200; thesis_stale: 14 days old; spread_wide: 0.12%";
    stubFetch({
      alerts_last_seen_decision_id: null,
      unseen_count: 1,
      rejections: [{ ...baseRow, explanation: long }],
    });
    renderStrip();
    const node = await screen.findByText(long);
    expect(node.getAttribute("title")).toBe(long);
    expect(node.className).toMatch(/truncate/);
  });
});
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pnpm --dir frontend test -- AlertsStrip.test.tsx`
Expected: all eight FAIL (module cannot be imported).

- [ ] **Step 3: Implement the component**

Create `frontend/src/components/dashboard/AlertsStrip.tsx`:

```tsx
/**
 * AlertsStrip — guard-rejection alerts on the operator dashboard (#315 Phase 3).
 *
 * Sits between RollingPnlStrip and PortfolioValueChart. Hidden when empty;
 * silent on fetch error (matches the RollingPnlStrip pattern — a failing
 * /alerts must not blank the dashboard).
 *
 * Cursor is decision_id (not decision_time) — see spec
 * docs/superpowers/specs/2026-04-21-alerts-strip-guard-rejections.md for why.
 */
import { Link } from "react-router-dom";

import {
  fetchGuardRejections,
  markAlertsSeen,
  dismissAllAlerts,
} from "@/api/alerts";
import type { GuardRejection } from "@/api/types";
import { formatRelativeTime } from "@/lib/format";
import { useAsync } from "@/lib/useAsync";

function isUnseen(
  row: GuardRejection,
  lastSeen: number | null,
): boolean {
  return lastSeen === null || row.decision_id > lastSeen;
}

function RowView({
  row,
  lastSeen,
}: {
  row: GuardRejection;
  lastSeen: number | null;
}) {
  const unseen = isUnseen(row, lastSeen);
  const border = unseen
    ? "border-l-4 border-amber-400"
    : "border-l-4 border-slate-200";
  const content = (
    <div
      data-testid="alerts-row"
      className={`flex items-center gap-3 px-3 py-2 text-sm ${border} bg-white`}
    >
      <span className="w-16 font-semibold tabular-nums">{row.symbol ?? "—"}</span>
      <span className="w-12 text-xs uppercase text-slate-500">{row.action ?? "—"}</span>
      <span
        className="flex-1 truncate text-slate-700"
        title={row.explanation}
      >
        {row.explanation}
      </span>
      <span className="w-20 text-right text-xs text-slate-400">
        {formatRelativeTime(row.decision_time)}
      </span>
    </div>
  );
  if (row.instrument_id !== null) {
    return (
      <Link to={`/instruments/${row.instrument_id}`} className="block hover:bg-slate-50">
        {content}
      </Link>
    );
  }
  return content;
}

export function AlertsStrip(): JSX.Element | null {
  const { data, error } = useAsync(fetchGuardRejections, []);

  if (error !== null || data === null) return null;
  if (data.rejections.length === 0) return null;

  const lastSeen = data.alerts_last_seen_decision_id;

  return (
    <section className="rounded-md border border-slate-200 bg-white shadow-sm">
      <header className="flex items-center justify-between border-b border-slate-100 px-4 py-3">
        <div className="flex items-center gap-2">
          <h2 className="text-sm font-semibold text-slate-700">Guard rejections</h2>
          {data.unseen_count > 0 ? (
            <span className="rounded-full bg-amber-100 px-2 py-0.5 text-xs font-medium text-amber-700">
              {data.unseen_count} new
            </span>
          ) : null}
        </div>
        {/* Action buttons land in Tasks 10 and 11 */}
      </header>
      <div className="max-h-96 overflow-y-auto divide-y divide-slate-100">
        {data.rejections.map((row) => (
          <RowView key={row.decision_id} row={row} lastSeen={lastSeen} />
        ))}
      </div>
    </section>
  );
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pnpm --dir frontend test -- AlertsStrip.test.tsx`
Expected: all eight PASS.

- [ ] **Step 5: Commit**

```bash
git add frontend/src/components/dashboard/AlertsStrip.tsx frontend/src/components/dashboard/AlertsStrip.test.tsx
git commit -m "feat(#315): AlertsStrip base — row rendering, hidden on empty, silent on error"
```

---

## Task 10: Normal-path "Mark all read" button

**Files:**

- Modify: `frontend/src/components/dashboard/AlertsStrip.test.tsx`
- Modify: `frontend/src/components/dashboard/AlertsStrip.tsx`

- [ ] **Step 1: Add failing tests — button render, click behaviour, cap-positive branch**

Append to `AlertsStrip.test.tsx`:

```tsx
import userEvent from "@testing-library/user-event";

describe("AlertsStrip — Mark all read (normal path)", () => {
  it("renders 'Mark all read' when unseen_count > 0 and <= rejections.length", async () => {
    stubFetch({
      alerts_last_seen_decision_id: 499,
      unseen_count: 2,
      rejections: [
        { ...baseRow, decision_id: 501 },
        { ...baseRow, decision_id: 500 },
      ],
    });
    renderStrip();
    expect(await screen.findByRole("button", { name: /mark all read/i })).toBeInTheDocument();
  });

  it("hides 'Mark all read' when unseen_count === 0 (all rows already seen)", async () => {
    stubFetch({
      alerts_last_seen_decision_id: 999,
      unseen_count: 0,
      rejections: [{ ...baseRow, decision_id: 500 }],  // seen (500 < 999)
    });
    renderStrip();
    await screen.findByText("AAPL");
    expect(screen.queryByRole("button", { name: /mark all read/i })).toBeNull();
  });

  it("stays visible at the 500-row cap when unseen_count === rejections.length === 500", async () => {
    const rejections = Array.from({ length: 500 }, (_, i) => ({
      ...baseRow,
      decision_id: 500 - i,
    }));
    stubFetch({
      alerts_last_seen_decision_id: null,
      unseen_count: 500,
      rejections,
    });
    renderStrip();
    expect(await screen.findByRole("button", { name: /mark all read/i })).toBeInTheDocument();
  });

  it("click posts rejections[0].decision_id and refetches", async () => {
    stubFetch({
      alerts_last_seen_decision_id: 499,
      unseen_count: 2,
      rejections: [
        { ...baseRow, decision_id: 501 },
        { ...baseRow, decision_id: 500 },
      ],
    });
    (alertsApi.markAlertsSeen as unknown as vi.Mock).mockResolvedValue(undefined);
    renderStrip();

    const btn = await screen.findByRole("button", { name: /mark all read/i });
    await userEvent.click(btn);

    expect(alertsApi.markAlertsSeen).toHaveBeenCalledWith(501);  // MAX(decision_id) in payload
    // Refetch was triggered (second fetch call).
    await vi.waitFor(() => {
      expect((alertsApi.fetchGuardRejections as unknown as vi.Mock).mock.calls.length).toBeGreaterThanOrEqual(2);
    });
  });
});
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pnpm --dir frontend test -- AlertsStrip.test.tsx`
Expected: the three new tests FAIL — no button is rendered.

- [ ] **Step 3: Implement the button**

In `AlertsStrip.tsx`, replace the current header/component body to add the button + `refetch`:

```tsx
export function AlertsStrip(): JSX.Element | null {
  const { data, error, refetch } = useAsync(fetchGuardRejections, []);

  if (error !== null || data === null) return null;
  if (data.rejections.length === 0) return null;

  const lastSeen = data.alerts_last_seen_decision_id;
  const normalAck =
    data.unseen_count > 0 && data.unseen_count <= data.rejections.length;

  async function onMarkAllRead() {
    // rejections is non-empty here (strip is hidden otherwise),
    // and is ordered decision_id DESC on the server so index 0 is MAX.
    const seenThroughDecisionId = data!.rejections[0].decision_id;
    await markAlertsSeen(seenThroughDecisionId);
    refetch();
  }

  return (
    <section className="rounded-md border border-slate-200 bg-white shadow-sm">
      <header className="flex items-center justify-between border-b border-slate-100 px-4 py-3">
        <div className="flex items-center gap-2">
          <h2 className="text-sm font-semibold text-slate-700">Guard rejections</h2>
          {data.unseen_count > 0 ? (
            <span className="rounded-full bg-amber-100 px-2 py-0.5 text-xs font-medium text-amber-700">
              {data.unseen_count} new
            </span>
          ) : null}
        </div>
        {normalAck ? (
          <button
            type="button"
            onClick={onMarkAllRead}
            className="rounded border border-slate-300 bg-white px-2 py-1 text-xs font-medium text-slate-700 hover:bg-slate-50"
          >
            Mark all read
          </button>
        ) : null}
      </header>
      <div className="max-h-96 overflow-y-auto divide-y divide-slate-100">
        {data.rejections.map((row) => (
          <RowView key={row.decision_id} row={row} lastSeen={lastSeen} />
        ))}
      </div>
    </section>
  );
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pnpm --dir frontend test -- AlertsStrip.test.tsx`
Expected: all tests PASS.

- [ ] **Step 5: Commit**

```bash
git add frontend/src/components/dashboard/AlertsStrip.tsx frontend/src/components/dashboard/AlertsStrip.test.tsx
git commit -m "feat(#315): AlertsStrip — normal-path Mark all read"
```

---

## Task 11: Overflow-path "Dismiss all" button + confirm dialog

**Files:**

- Modify: `frontend/src/components/dashboard/AlertsStrip.test.tsx`
- Modify: `frontend/src/components/dashboard/AlertsStrip.tsx`

- [ ] **Step 1: Add failing tests — render, confirm, cancel, `/recommendations` link**

Append to `AlertsStrip.test.tsx`:

```tsx
describe("AlertsStrip — Dismiss all (overflow path)", () => {
  function overflowStub() {
    stubFetch({
      alerts_last_seen_decision_id: null,
      unseen_count: 600,
      rejections: Array.from({ length: 500 }, (_, i) => ({
        ...baseRow,
        decision_id: 600 - i,
      })),
    });
  }

  it("renders 'Dismiss all (600) as acknowledged' and a /recommendations link when unseen_count > rejections.length", async () => {
    overflowStub();
    renderStrip();
    expect(
      await screen.findByRole("button", { name: /dismiss all \(600\)/i }),
    ).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /mark all read/i })).toBeNull();
    const recLink = screen.getByRole("link", { name: /recommendations/i });
    expect(recLink.getAttribute("href")).toBe("/recommendations");
  });

  it("confirm dialog: confirm calls dismissAllAlerts + refetch", async () => {
    overflowStub();
    (alertsApi.dismissAllAlerts as unknown as vi.Mock).mockResolvedValue(undefined);
    const confirmSpy = vi.spyOn(window, "confirm").mockReturnValue(true);
    renderStrip();

    const btn = await screen.findByRole("button", { name: /dismiss all \(600\)/i });
    await userEvent.click(btn);

    expect(confirmSpy).toHaveBeenCalled();
    expect(alertsApi.dismissAllAlerts).toHaveBeenCalled();
    await vi.waitFor(() => {
      expect((alertsApi.fetchGuardRejections as unknown as vi.Mock).mock.calls.length).toBeGreaterThanOrEqual(2);
    });
    confirmSpy.mockRestore();
  });

  it("confirm dialog: cancel does NOT call dismissAllAlerts or refetch", async () => {
    overflowStub();
    (alertsApi.dismissAllAlerts as unknown as vi.Mock).mockResolvedValue(undefined);
    const confirmSpy = vi.spyOn(window, "confirm").mockReturnValue(false);
    renderStrip();

    const fetchCallsBefore = (alertsApi.fetchGuardRejections as unknown as vi.Mock).mock.calls.length;
    const btn = await screen.findByRole("button", { name: /dismiss all \(600\)/i });
    await userEvent.click(btn);

    expect(confirmSpy).toHaveBeenCalled();
    expect(alertsApi.dismissAllAlerts).not.toHaveBeenCalled();
    expect((alertsApi.fetchGuardRejections as unknown as vi.Mock).mock.calls.length).toBe(fetchCallsBefore);
    confirmSpy.mockRestore();
  });
});
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pnpm --dir frontend test -- AlertsStrip.test.tsx`
Expected: the three new tests FAIL.

- [ ] **Step 3: Implement the overflow branch**

In `AlertsStrip.tsx`, extend the header action area. Replace the `{normalAck ? ... : null}` block with:

```tsx
        {normalAck ? (
          <button
            type="button"
            onClick={onMarkAllRead}
            className="rounded border border-slate-300 bg-white px-2 py-1 text-xs font-medium text-slate-700 hover:bg-slate-50"
          >
            Mark all read
          </button>
        ) : null}
        {overflowAck ? (
          <div className="flex items-center gap-2">
            <button
              type="button"
              onClick={onDismissAll}
              className="rounded border border-amber-300 bg-amber-50 px-2 py-1 text-xs font-medium text-amber-800 hover:bg-amber-100"
            >
              Dismiss all ({data.unseen_count}) as acknowledged
            </button>
            <Link
              to="/recommendations"
              className="text-xs text-slate-500 underline hover:text-slate-700"
            >
              Triage at /recommendations
            </Link>
          </div>
        ) : null}
```

Add `overflowAck` + `onDismissAll` above the return:

```tsx
  const overflowAck = data.unseen_count > data.rejections.length;

  async function onDismissAll() {
    const hiddenCount = data!.unseen_count - data!.rejections.length;
    const msg = `Dismiss all ${data!.unseen_count} unseen rejections? ${hiddenCount} are not shown above. Review them at /recommendations before dismissing if they might matter.`;
    if (!window.confirm(msg)) return;
    await dismissAllAlerts();
    refetch();
  }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pnpm --dir frontend test -- AlertsStrip.test.tsx`
Expected: all tests PASS.

- [ ] **Step 5: Commit**

```bash
git add frontend/src/components/dashboard/AlertsStrip.tsx frontend/src/components/dashboard/AlertsStrip.test.tsx
git commit -m "feat(#315): AlertsStrip — overflow-path Dismiss all + confirm dialog"
```

---

## Task 12: Wire into `DashboardPage`

**Files:**

- Modify: `frontend/src/pages/DashboardPage.tsx`

- [ ] **Step 1: Read the current layout**

Run: `grep -n "RollingPnlStrip\|PortfolioValueChart\|PositionsTable" frontend/src/pages/DashboardPage.tsx`

Current order is `RollingPnlStrip` → `PortfolioValueChart` → `<Section title="Positions">`. `<AlertsStrip />` goes **between `RollingPnlStrip` and `PortfolioValueChart`** — operator reads totals → short-horizon delta → alerts → long-horizon trajectory → positions.

- [ ] **Step 2: Add the import**

In `frontend/src/pages/DashboardPage.tsx`, next to the other dashboard-component imports:

```ts
import { AlertsStrip } from "@/components/dashboard/AlertsStrip";
```

- [ ] **Step 3: Mount the component**

In the JSX, insert between `<RollingPnlStrip />` and `<PortfolioValueChart />`:

```tsx
<RollingPnlStrip />
{/* Needs-action surface — guard rejections since last visit.
    Hidden when empty, silent on error. Sits above the narrative
    chart so action-required signal precedes trajectory context. */}
<AlertsStrip />
<PortfolioValueChart />
```

- [ ] **Step 4: Update the layout comment**

At the top of `DashboardPage`, locate the ASCII-art layout comment and insert the strip between the rolling-P&L pills and the value chart:

```text
Layout:
  ┌ SummaryCards (AUM · Cash · P&L · Deployment) ┐
  │ RollingPnlStrip (1d · 1w · 1m)               │
  │ AlertsStrip (guard rejections)               │
  │ PortfolioValueChart                          │
  │                                              │
  │ Positions                                    │
  │ Needs action (proposed recs)                 │
  │ Watchlist                                    │
  └──────────────────────────────────────────────┘
```

- [ ] **Step 5: Typecheck + build the frontend**

Run:

```bash
pnpm --dir frontend typecheck
pnpm --dir frontend build
```

Expected: both green.

- [ ] **Step 6: Smoke test in the running dev stack**

Per `feedback_keep_stack_running.md`: do NOT stop/restart the VS Code-managed backend/frontend tasks. They're already running. Just open the dashboard in the browser.

Visual check:

- No alerts in DB → strip is not rendered (dashboard looks unchanged).
- One FAIL guard row in DB → strip appears between rolling-P&L and Positions with one row; clicking "Mark all read" clears the amber border on next render.
- Exit the test rows via `DELETE FROM decision_audit WHERE stage='execution_guard' AND pass_fail='FAIL' AND explanation LIKE 'TEST%'` once done.

- [ ] **Step 7: Commit**

```bash
git add frontend/src/pages/DashboardPage.tsx
git commit -m "feat(#315): wire AlertsStrip into DashboardPage between RollingPnlStrip and PortfolioValueChart"
```

---

## Task 13: Pre-push gates + Codex review

**Files:** none (verification only)

- [ ] **Step 1: Full backend gate**

Run:

```bash
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv run pytest
```

All four must pass. `uv run pytest` includes the smoke test (`tests/smoke/test_app_boots.py`) that boots the app via `TestClient`. The new `alerts_router` must register cleanly there.

- [ ] **Step 2: Full frontend gate**

Run:

```bash
pnpm --dir frontend typecheck
pnpm --dir frontend test
```

Both must pass.

- [ ] **Step 3: Self-review the diff**

Run: `git diff main...HEAD -- ':!docs/superpowers'`

Read the full diff. Apply the pre-flight review skill — read as an adversary. Check the spec's 22 backend + 11 frontend test cases all have matching assertions. Check the SQL matches the spec literally (casing, LEAST/GREATEST, m.max_id IS NOT NULL).

- [ ] **Step 4: Codex pre-push review (CLAUDE.md checkpoint 2)**

Run:

```bash
codex.cmd exec "Review the branch feature/315-phase3-alerts-strip before first push. Diff is the full implementation of docs/superpowers/specs/2026-04-21-alerts-strip-guard-rejections.md. Focus on correctness of the SQL queries (ordering, LEAST/GREATEST, NULL handling), ack race-safety, and React state flow around refetch. Reply terse." < /dev/null
```

Address any real findings before pushing. Report unresolved points as `REBUTTED {reason}` in the commit body.

- [ ] **Step 5: Push + open PR**

```bash
git push -u origin feature/315-phase3-alerts-strip
gh pr create --title "feat(#315): Phase 3 — guard-rejection alerts strip" --body "$(cat <<'EOF'
## What
Alerts strip for execution-guard rejections on the operator dashboard. Closes the final phase of #315.

## Why
Operator needs a "since last visit" view of guard rejections without leaving the dashboard. Scope limited to guard rejections because thesis breaches (#394) and filings-status drops (#395) each need their own event-persistence design.

## Test plan
- [x] Backend unit tests: operator-resolution 503/501 (GET + both POSTs), GET shape + SQL-shape pin + null-action + HOLD + unseen-count SQL shape, POST /seen validation + SQL-shape (GREATEST/LEAST/MAX scope), POST /dismiss-all happy + scope + empty-window noop
- [x] Integration tests vs ebull_test: clock-skew ordering (decision_id wins), POST /seen clamps to in-window MAX, dismiss-all empty-window no-op, non-guard stage excluded
- [x] Frontend tests: empty/error hidden, row shape, instrument link vs plain, amber/slate borders, unseen pill, title-truncation, Mark all read (normal + cap-positive + unseen=0 hides), Dismiss all (overflow render + confirm + cancel)
- [x] Smoke test (`tests/smoke/test_app_boots.py`) green with new router
- [x] Dashboard smoke-tested against dev DB
- [x] Codex pre-spec (10 rounds) + pre-push reviews clean

## Follow-ups filed on merge
- #394 position-alert event persistence
- #395 coverage status transition log
- #396 wire #394 + #395 into the strip

Spec: docs/superpowers/specs/2026-04-21-alerts-strip-guard-rejections.md

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

- [ ] **Step 6: Poll review + CI**

Per `feedback_post_push_cycle.md`: immediately start polling `gh pr view <n> --comments` and `gh pr checks <n>`. Resolve every comment as `FIXED {sha}` / `DEFERRED #{id}` / `REBUTTED {reason}`. Each follow-up push resets the review — keep polling until APPROVE on the most recent commit + CI green.

- [ ] **Step 7: On merge**

1. File #394, #395, #396 with the follow-up scope from the spec.
2. Close #315.
3. Delete local + remote branch.

---

## Self-review checklist

**1. Spec coverage:**

- Schema migration → Task 1 ✓
- Router + operator-resolution errors → Task 2 ✓
- GET /alerts/guard-rejections (7d window, 500 cap, decision_id ordering, unseen_count) → Task 3 ✓
- POST /alerts/seen (validation, LEAST clamp, monotonic GREATEST) → Task 4 ✓
- POST /alerts/dismiss-all (atomic MAX, WHERE m.max_id IS NOT NULL) → Task 5 ✓
- Race-safety + ordering-by-decision_id integration tests → Task 6 ✓
- Frontend types + API client → Task 7 ✓
- formatRelativeTime helper → Task 8 ✓
- AlertsStrip empty / error / rows / link / border → Task 9 ✓
- AlertsStrip Mark all read (normal + cap-positive branch) → Task 10 ✓
- AlertsStrip Dismiss all (overflow + confirm/cancel) → Task 11 ✓
- DashboardPage wire-up → Task 12 ✓
- Pre-push gates + Codex + PR + follow-up tickets → Task 13 ✓

**2. Placeholder scan:** No TBD/TODO/"similar to", no "add error handling" hand-waves, every code step shows actual code.

**3. Type consistency:**

- `seenThroughDecisionId` param name matches across `alerts.ts`, test assertions, and the POST body shape `{seen_through_decision_id}`.
- `GuardRejectionAction` Literal matches backend `Literal["BUY", "ADD", "HOLD", "EXIT"]` exactly.
- `alerts_last_seen_decision_id` column/field name matches across migration, backend queries, response envelope, frontend types.
- `onMarkAllRead` and `onDismissAll` are both defined in Task 10/11 before use in Task 11's JSX diff.
- `decision_id` typed as `number` in TS and `int` in Pydantic throughout.

---

Plan complete and saved to `docs/superpowers/plans/2026-04-21-alerts-strip-guard-rejections.md`. Two execution options:

1. **Subagent-Driven (recommended)** — I dispatch a fresh subagent per task, review between tasks, fast iteration.
2. **Inline Execution** — Execute tasks in this session using executing-plans, batch execution with checkpoints.

Which approach?
