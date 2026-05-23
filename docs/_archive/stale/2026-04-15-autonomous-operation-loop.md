# Autonomous Operation Loop — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the three gaps preventing autonomous end-to-end investment lifecycle: retry deferred recommendations, monitor positions intraday, and wire pipeline awareness between scheduled jobs.

**Architecture:** Three independent subsystems layered onto the existing scheduler and service modules. (1) A new hourly `retry_deferred_recommendations` job re-evaluates `timing_deferred` BUY/ADD recs using fresh TA data. (2) A new hourly `monitor_positions` job detects SL/TP hits and thesis breaks between daily syncs. (3) Pipeline triggers — post-completion hooks on existing jobs — chain outputs to downstream job invocation so the morning pipeline fires as a connected sequence rather than isolated cron jobs.

**Tech Stack:** Python, psycopg3, APScheduler (existing), PostgreSQL, existing TA/entry-timing/execution-guard services.

---

## Settled decisions that apply

- **Kill switch stops all autonomous trading immediately** — every new job must check the kill switch before any write path.
- **Execution guard hard rules always apply** — retried recs go through the full guard, not a shortcut.
- **EXIT recs always pass through timing** — never defer protective exits (existing settled decision preserved).
- **Guard re-check rule** — guard must re-check constraints against current state; never trust old recommendation state.
- **Paper trading mode must work identically to live mode** — new jobs must operate in both modes.
- **Decision audit per guard invocation** — every guard/timing evaluation writes an audit row.
- **enable_auto_trading is not the same as enable_live_trading** — new autonomous jobs check `enable_auto_trading`.

## Prevention log entries that apply

- **decision_id received but not written back to decision_audit** — every pipeline stage must write its own audit row.
- **Early return inside `_tracked_job` without `row_count`** — all new jobs must set `tracker.row_count` on every path.
- **Read-then-write cap enforcement outside transaction** — retry status changes must be atomic with their audit rows.
- **ON CONFLICT DO NOTHING counter overcount** — counters gated on `rowcount > 0`.
- **Shared column vocabulary mismatch across stages** — reuse existing `pass_fail` values (`PASS`, `FAIL`, `DEFER`).

---

## File Structure

| File | Responsibility |
|------|---------------|
| `sql/028_autonomous_loop.sql` | Migration: add `timing_retry_count` + `timing_deferred_at` columns to `trade_recommendations`; add `deferred_recommendation_id` FK column for retry-created recs |
| `app/services/deferred_retry.py` | Service: re-evaluate deferred recs, transition them back to `proposed` or expire them |
| `app/services/position_monitor.py` | Service: lightweight intraday position health check (SL/TP hit detection, thesis break) |
| `app/workers/scheduler.py` | Modified: add 2 new scheduled jobs + 2 new job functions + pipeline trigger hooks |
| `tests/test_deferred_retry.py` | Tests for deferred retry service |
| `tests/test_position_monitor.py` | Tests for position monitoring service |
| `tests/test_scheduler_autonomous.py` | Tests for new scheduler jobs and pipeline triggers |

---

## Task 1: Migration — deferred retry tracking columns

**Files:**
- Create: `sql/028_autonomous_loop.sql`

- [ ] **Step 1: Write the migration SQL**

```sql
-- Migration 028: autonomous operation loop — deferred retry tracking
--
-- timing_retry_count: how many times a timing_deferred rec has been
--   re-evaluated. Starts at 0, incremented on each retry attempt.
--   Used to cap retries (max 3 per cycle) and for observability.
--
-- timing_deferred_at: when the rec was first deferred. Used to expire
--   stale deferred recs (>24h old) so they don't retry indefinitely.
--
-- deferred_recommendation_id: when a deferred rec expires, a new
--   recommendation may be generated in the next morning cycle. This
--   FK links the retry lineage for auditability.

ALTER TABLE trade_recommendations
    ADD COLUMN IF NOT EXISTS timing_retry_count      INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS timing_deferred_at       TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS deferred_recommendation_id BIGINT
        REFERENCES trade_recommendations(recommendation_id);
```

- [ ] **Step 2: Apply migration locally**

Run: `uv run python -c "import psycopg; conn = psycopg.connect('$(grep DATABASE_URL .env | cut -d= -f2-)'); conn.execute(open('sql/028_autonomous_loop.sql').read()); conn.commit(); print('OK')"`
Expected: OK (no errors)

- [ ] **Step 3: Verify columns exist**

Run: `uv run python -c "import psycopg; from app.config import settings; conn = psycopg.connect(settings.database_url); cur = conn.execute(\"SELECT column_name FROM information_schema.columns WHERE table_name='trade_recommendations' AND column_name IN ('timing_retry_count','timing_deferred_at','deferred_recommendation_id')\"); print([r[0] for r in cur.fetchall()])"`
Expected: `['timing_retry_count', 'timing_deferred_at', 'deferred_recommendation_id']`

- [ ] **Step 4: Commit**

```bash
git add sql/028_autonomous_loop.sql
git commit -m "feat(#205): migration 028 — deferred retry tracking columns"
```

---

## Task 2: Deferred retry service

**Files:**
- Create: `app/services/deferred_retry.py`
- Test: `tests/test_deferred_retry.py`

### Step-by-step

- [ ] **Step 1: Write the failing test — retry re-evaluates a deferred rec**

```python
"""Tests for app.services.deferred_retry."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from app.services.deferred_retry import (
    MAX_RETRY_ATTEMPTS,
    RETRY_EXPIRY_HOURS,
    RetryResult,
    retry_deferred_recommendations,
)


def _mock_conn() -> MagicMock:
    """Create a mock psycopg connection with cursor context manager."""
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    conn.execute.return_value = MagicMock()
    conn.transaction.return_value.__enter__ = MagicMock()
    conn.transaction.return_value.__exit__ = MagicMock(return_value=False)
    return conn


class TestRetryDeferredRecommendations:
    """Tests for the retry_deferred_recommendations service function."""

    def test_no_deferred_recs_returns_zero_counts(self) -> None:
        conn = _mock_conn()
        cursor = conn.cursor.return_value.__enter__.return_value
        cursor.fetchall.return_value = []

        result = retry_deferred_recommendations(conn)

        assert result.retried == 0
        assert result.re_proposed == 0
        assert result.re_deferred == 0
        assert result.expired == 0

    def test_deferred_rec_within_retry_limit_is_re_evaluated(self) -> None:
        """A deferred rec with retry_count < MAX should be re-evaluated via entry timing."""
        conn = _mock_conn()
        cursor = conn.cursor.return_value.__enter__.return_value
        now = datetime.now(UTC)
        cursor.fetchall.return_value = [
            {
                "recommendation_id": 42,
                "instrument_id": 100,
                "action": "BUY",
                "timing_retry_count": 0,
                "timing_deferred_at": now - timedelta(hours=1),
            },
        ]

        with patch("app.services.deferred_retry.evaluate_entry_conditions") as mock_eval:
            mock_eval.return_value = MagicMock(
                verdict="pass",
                stop_loss_rate=Decimal("95.00"),
                take_profit_rate=Decimal("120.00"),
                rationale="RSI recovered, MACD positive",
                condition_details={"rsi": 55.0},
            )
            result = retry_deferred_recommendations(conn)

        assert result.retried == 1
        assert result.re_proposed == 1
        mock_eval.assert_called_once_with(conn, 42)

    def test_deferred_rec_exceeding_retry_limit_is_expired(self) -> None:
        """A rec that has been retried MAX_RETRY_ATTEMPTS times gets expired."""
        conn = _mock_conn()
        cursor = conn.cursor.return_value.__enter__.return_value
        now = datetime.now(UTC)
        cursor.fetchall.return_value = [
            {
                "recommendation_id": 42,
                "instrument_id": 100,
                "action": "BUY",
                "timing_retry_count": MAX_RETRY_ATTEMPTS,
                "timing_deferred_at": now - timedelta(hours=1),
            },
        ]

        result = retry_deferred_recommendations(conn)

        assert result.expired == 1
        assert result.retried == 0

    def test_deferred_rec_older_than_expiry_is_expired(self) -> None:
        """A rec deferred longer than RETRY_EXPIRY_HOURS gets expired."""
        conn = _mock_conn()
        cursor = conn.cursor.return_value.__enter__.return_value
        cursor.fetchall.return_value = [
            {
                "recommendation_id": 42,
                "instrument_id": 100,
                "action": "BUY",
                "timing_retry_count": 0,
                "timing_deferred_at": datetime.now(UTC) - timedelta(hours=RETRY_EXPIRY_HOURS + 1),
            },
        ]

        result = retry_deferred_recommendations(conn)

        assert result.expired == 1
        assert result.retried == 0

    def test_re_evaluation_still_unfavorable_increments_retry_count(self) -> None:
        """When timing still says defer, retry_count is incremented."""
        conn = _mock_conn()
        cursor = conn.cursor.return_value.__enter__.return_value
        now = datetime.now(UTC)
        cursor.fetchall.return_value = [
            {
                "recommendation_id": 42,
                "instrument_id": 100,
                "action": "BUY",
                "timing_retry_count": 1,
                "timing_deferred_at": now - timedelta(hours=2),
            },
        ]

        with patch("app.services.deferred_retry.evaluate_entry_conditions") as mock_eval:
            mock_eval.return_value = MagicMock(
                verdict="defer",
                stop_loss_rate=Decimal("95.00"),
                take_profit_rate=None,
                rationale="RSI still overbought",
                condition_details={"rsi": 78.0},
            )
            result = retry_deferred_recommendations(conn)

        assert result.re_deferred == 1
        assert result.re_proposed == 0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_deferred_retry.py -v`
Expected: FAIL (module not found)

- [ ] **Step 3: Write the deferred retry service**

```python
"""Deferred recommendation retry service.

Re-evaluates timing_deferred BUY/ADD recommendations using fresh TA data.
Recs that now pass are transitioned back to 'proposed' for the next
guard+execute cycle. Recs that exceed the retry cap or age out are
expired to 'timing_expired'.

Design choices:
  - Reuses evaluate_entry_conditions() — same TA checks, same SL/TP compute.
  - Each retry attempt writes a decision_audit row for auditability.
  - Retry cap (MAX_RETRY_ATTEMPTS) and time cap (RETRY_EXPIRY_HOURS) prevent
    infinite retry loops on persistently unfavorable instruments.
  - timing_deferred_at is set on first deferral (by the scheduler's Phase 0),
    not on retry — so expiry is measured from original deferral time.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import psycopg
import psycopg.rows
from psycopg.types.json import Jsonb

from app.services.entry_timing import evaluate_entry_conditions

logger = logging.getLogger(__name__)

# Maximum number of retry attempts before expiring a deferred rec.
# 3 retries at hourly cadence = 3 hours of re-checks after deferral.
MAX_RETRY_ATTEMPTS: int = 3

# Hours after which a deferred rec expires regardless of retry count.
# Prevents stale recs from lingering across trading days.
RETRY_EXPIRY_HOURS: int = 24


@dataclass(frozen=True)
class RetryResult:
    """Summary of a deferred retry run."""

    retried: int
    re_proposed: int
    re_deferred: int
    expired: int
    errors: int = 0


def retry_deferred_recommendations(conn: psycopg.Connection[Any]) -> RetryResult:
    """Re-evaluate all timing_deferred BUY/ADD recs.

    For each deferred rec:
    1. If retry_count >= MAX or deferred_at + EXPIRY has passed → expire.
    2. Otherwise, re-run evaluate_entry_conditions:
       - verdict=pass → transition to 'proposed', update SL/TP.
       - verdict=defer → increment retry_count, stay deferred.
       - verdict=skip/error → increment retry_count, stay deferred.

    Each transition writes a decision_audit row in the same transaction
    as the status update (prevention log: read-then-write in same txn).

    Returns a RetryResult summary.
    """
    now = datetime.now(UTC)
    expiry_cutoff = now - timedelta(hours=RETRY_EXPIRY_HOURS)

    # Load all timing_deferred recs (BUY/ADD only — EXIT/HOLD never deferred).
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT recommendation_id, instrument_id, action,
                   timing_retry_count, timing_deferred_at
            FROM trade_recommendations
            WHERE status = 'timing_deferred'
              AND action IN ('BUY', 'ADD')
            ORDER BY recommendation_id
            """,
        )
        deferred = cur.fetchall()

    retried = 0
    re_proposed = 0
    re_deferred = 0
    expired = 0
    errors = 0

    for rec in deferred:
        rec_id: int = rec["recommendation_id"]
        instrument_id: int = rec["instrument_id"]
        retry_count: int = rec["timing_retry_count"]
        deferred_at = rec["timing_deferred_at"]

        # --- Expiry check ---
        should_expire = (
            retry_count >= MAX_RETRY_ATTEMPTS
            or (deferred_at is not None and deferred_at < expiry_cutoff)
        )
        if should_expire:
            with conn.transaction():
                conn.execute(
                    """
                    INSERT INTO decision_audit
                        (decision_time, instrument_id, recommendation_id,
                         stage, pass_fail, explanation)
                    VALUES
                        (NOW(), %(iid)s, %(rid)s,
                         'deferred_retry', 'FAIL',
                         %(expl)s)
                    """,
                    {
                        "iid": instrument_id,
                        "rid": rec_id,
                        "expl": f"expired: retry_count={retry_count}, "
                        f"deferred_at={deferred_at}, cutoff={expiry_cutoff}",
                    },
                )
                conn.execute(
                    """
                    UPDATE trade_recommendations
                    SET status = 'timing_expired',
                        timing_verdict = 'error',
                        timing_rationale = %(rationale)s
                    WHERE recommendation_id = %(rid)s
                    """,
                    {
                        "rid": rec_id,
                        "rationale": f"expired after {retry_count} retries "
                        f"(max={MAX_RETRY_ATTEMPTS}, expiry={RETRY_EXPIRY_HOURS}h)",
                    },
                )
            conn.commit()
            expired += 1
            logger.info(
                "deferred_retry: expired rec=%d (retries=%d, deferred_at=%s)",
                rec_id,
                retry_count,
                deferred_at,
            )
            continue

        # --- Re-evaluate timing ---
        try:
            evaluation = evaluate_entry_conditions(conn, rec_id)
        except Exception:
            logger.error(
                "deferred_retry: evaluation failed for rec=%d, incrementing retry count",
                rec_id,
                exc_info=True,
            )
            with conn.transaction():
                conn.execute(
                    """
                    UPDATE trade_recommendations
                    SET timing_retry_count = timing_retry_count + 1
                    WHERE recommendation_id = %(rid)s
                    """,
                    {"rid": rec_id},
                )
            conn.commit()
            errors += 1
            continue

        retried += 1

        if evaluation.verdict == "pass":
            # Conditions now favorable — transition back to proposed
            with conn.transaction():
                conn.execute(
                    """
                    INSERT INTO decision_audit
                        (decision_time, instrument_id, recommendation_id,
                         stage, pass_fail, explanation, evidence_json)
                    VALUES
                        (NOW(), %(iid)s, %(rid)s,
                         'deferred_retry', 'PASS', %(expl)s, %(ev)s)
                    """,
                    {
                        "iid": instrument_id,
                        "rid": rec_id,
                        "expl": evaluation.rationale,
                        "ev": Jsonb(evaluation.condition_details),
                    },
                )
                conn.execute(
                    """
                    UPDATE trade_recommendations
                    SET status = 'proposed',
                        stop_loss_rate = %(sl)s,
                        take_profit_rate = %(tp)s,
                        timing_verdict = %(verdict)s,
                        timing_rationale = %(rationale)s,
                        timing_retry_count = timing_retry_count + 1
                    WHERE recommendation_id = %(rid)s
                    """,
                    {
                        "sl": evaluation.stop_loss_rate,
                        "tp": evaluation.take_profit_rate,
                        "verdict": evaluation.verdict,
                        "rationale": evaluation.rationale,
                        "rid": rec_id,
                    },
                )
            conn.commit()
            re_proposed += 1
            logger.info(
                "deferred_retry: rec=%d re-proposed (verdict=pass, sl=%s, tp=%s)",
                rec_id,
                evaluation.stop_loss_rate,
                evaluation.take_profit_rate,
            )
        else:
            # Still unfavorable — increment retry count, stay deferred
            with conn.transaction():
                conn.execute(
                    """
                    INSERT INTO decision_audit
                        (decision_time, instrument_id, recommendation_id,
                         stage, pass_fail, explanation, evidence_json)
                    VALUES
                        (NOW(), %(iid)s, %(rid)s,
                         'deferred_retry', 'DEFER', %(expl)s, %(ev)s)
                    """,
                    {
                        "iid": instrument_id,
                        "rid": rec_id,
                        "expl": evaluation.rationale,
                        "ev": Jsonb(evaluation.condition_details),
                    },
                )
                conn.execute(
                    """
                    UPDATE trade_recommendations
                    SET timing_retry_count = timing_retry_count + 1,
                        stop_loss_rate = %(sl)s,
                        take_profit_rate = %(tp)s,
                        timing_verdict = %(verdict)s,
                        timing_rationale = %(rationale)s
                    WHERE recommendation_id = %(rid)s
                    """,
                    {
                        "sl": evaluation.stop_loss_rate,
                        "tp": evaluation.take_profit_rate,
                        "verdict": evaluation.verdict,
                        "rationale": evaluation.rationale,
                        "rid": rec_id,
                    },
                )
            conn.commit()
            re_deferred += 1
            logger.info(
                "deferred_retry: rec=%d still deferred (retry_count=%d, verdict=%s)",
                rec_id,
                retry_count + 1,
                evaluation.verdict,
            )

    return RetryResult(
        retried=retried,
        re_proposed=re_proposed,
        re_deferred=re_deferred,
        expired=expired,
        errors=errors,
    )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_deferred_retry.py -v`
Expected: 5 PASSED

- [ ] **Step 5: Commit**

```bash
git add app/services/deferred_retry.py tests/test_deferred_retry.py
git commit -m "feat(#205): deferred retry service — re-evaluate timing_deferred recs"
```

---

## Task 3: Position monitoring service

**Files:**
- Create: `app/services/position_monitor.py`
- Test: `tests/test_position_monitor.py`

### Step-by-step

- [ ] **Step 1: Write the failing test — position monitor detects SL hit**

```python
"""Tests for app.services.position_monitor."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock

import pytest

from app.services.position_monitor import (
    MonitorAlert,
    MonitorResult,
    check_position_health,
)


def _mock_conn() -> MagicMock:
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    conn.execute.return_value = MagicMock()
    conn.transaction.return_value.__enter__ = MagicMock()
    conn.transaction.return_value.__exit__ = MagicMock(return_value=False)
    return conn


class TestCheckPositionHealth:
    """Tests for the check_position_health service function."""

    def test_no_open_positions_returns_empty(self) -> None:
        conn = _mock_conn()
        cursor = conn.cursor.return_value.__enter__.return_value
        cursor.fetchall.return_value = []

        result = check_position_health(conn)

        assert result.positions_checked == 0
        assert result.alerts == []

    def test_position_below_stop_loss_generates_alert(self) -> None:
        """When current price < SL rate, an alert is generated."""
        conn = _mock_conn()
        cursor = conn.cursor.return_value.__enter__.return_value
        cursor.fetchall.return_value = [
            {
                "instrument_id": 100,
                "symbol": "AAPL",
                "current_units": Decimal("10"),
                "stop_loss_rate": Decimal("140.00"),
                "take_profit_rate": Decimal("200.00"),
                "latest_bid": Decimal("135.00"),
                "latest_ask": Decimal("135.50"),
                "red_flag_score": Decimal("0.30"),
            },
        ]

        result = check_position_health(conn)

        assert result.positions_checked == 1
        assert len(result.alerts) == 1
        assert result.alerts[0].alert_type == "sl_breach"
        assert result.alerts[0].instrument_id == 100

    def test_position_above_take_profit_generates_alert(self) -> None:
        """When current price >= TP rate, an alert is generated."""
        conn = _mock_conn()
        cursor = conn.cursor.return_value.__enter__.return_value
        cursor.fetchall.return_value = [
            {
                "instrument_id": 100,
                "symbol": "AAPL",
                "current_units": Decimal("10"),
                "stop_loss_rate": Decimal("140.00"),
                "take_profit_rate": Decimal("200.00"),
                "latest_bid": Decimal("205.00"),
                "latest_ask": Decimal("205.50"),
                "red_flag_score": Decimal("0.30"),
            },
        ]

        result = check_position_health(conn)

        assert result.positions_checked == 1
        assert len(result.alerts) == 1
        assert result.alerts[0].alert_type == "tp_breach"

    def test_position_with_high_red_flag_generates_thesis_break_alert(self) -> None:
        """When red_flag_score >= 0.80, a thesis_break alert is generated."""
        conn = _mock_conn()
        cursor = conn.cursor.return_value.__enter__.return_value
        cursor.fetchall.return_value = [
            {
                "instrument_id": 100,
                "symbol": "AAPL",
                "current_units": Decimal("10"),
                "stop_loss_rate": Decimal("140.00"),
                "take_profit_rate": Decimal("200.00"),
                "latest_bid": Decimal("160.00"),
                "latest_ask": Decimal("160.50"),
                "red_flag_score": Decimal("0.85"),
            },
        ]

        result = check_position_health(conn)

        assert result.positions_checked == 1
        assert len(result.alerts) == 1
        assert result.alerts[0].alert_type == "thesis_break"

    def test_healthy_position_generates_no_alert(self) -> None:
        """Price within SL/TP range and low red flag → no alert."""
        conn = _mock_conn()
        cursor = conn.cursor.return_value.__enter__.return_value
        cursor.fetchall.return_value = [
            {
                "instrument_id": 100,
                "symbol": "AAPL",
                "current_units": Decimal("10"),
                "stop_loss_rate": Decimal("140.00"),
                "take_profit_rate": Decimal("200.00"),
                "latest_bid": Decimal("160.00"),
                "latest_ask": Decimal("160.50"),
                "red_flag_score": Decimal("0.30"),
            },
        ]

        result = check_position_health(conn)

        assert result.positions_checked == 1
        assert result.alerts == []

    def test_null_sl_tp_does_not_crash(self) -> None:
        """Positions without SL/TP (pre-migration) should not generate SL/TP alerts."""
        conn = _mock_conn()
        cursor = conn.cursor.return_value.__enter__.return_value
        cursor.fetchall.return_value = [
            {
                "instrument_id": 100,
                "symbol": "AAPL",
                "current_units": Decimal("10"),
                "stop_loss_rate": None,
                "take_profit_rate": None,
                "latest_bid": Decimal("160.00"),
                "latest_ask": Decimal("160.50"),
                "red_flag_score": None,
            },
        ]

        result = check_position_health(conn)

        assert result.positions_checked == 1
        assert result.alerts == []
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_position_monitor.py -v`
Expected: FAIL (module not found)

- [ ] **Step 3: Write the position monitoring service**

```python
"""Intraday position monitoring service.

Provides lightweight position health checks between daily broker sync
cycles. Detects:
  - SL breaches: current bid < stop_loss_rate
  - TP breaches: current bid >= take_profit_rate
  - Thesis breaks: red_flag_score >= EXIT_RED_FLAG_THRESHOLD

Does NOT place orders. Generates alerts that the scheduler logs and
(in future) could trigger an out-of-cycle EXIT recommendation.

Design choices:
  - Uses latest quotes from the quotes table (refreshed hourly by
    fx_rates_refresh). No broker API call in the monitor itself.
  - Checks broker_positions for per-position SL/TP (most accurate).
  - Falls back to positions table if no broker_positions data exists
    (pre-migration positions without SL/TP).
  - NULL SL/TP/red_flag = skip that check (never block on missing data,
    matching entry_timing convention).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Literal

import psycopg
import psycopg.rows

logger = logging.getLogger(__name__)

# Reuse the portfolio manager's red flag threshold (settled decision).
EXIT_RED_FLAG_THRESHOLD = Decimal("0.80")

AlertType = Literal["sl_breach", "tp_breach", "thesis_break"]


@dataclass(frozen=True)
class MonitorAlert:
    """A single position health alert."""

    instrument_id: int
    symbol: str
    alert_type: AlertType
    detail: str
    current_bid: Decimal | None = None


@dataclass(frozen=True)
class MonitorResult:
    """Summary of a position monitoring run."""

    positions_checked: int
    alerts: list[MonitorAlert] = field(default_factory=list)


def check_position_health(conn: psycopg.Connection[Any]) -> MonitorResult:
    """Check all open positions for SL/TP breaches and thesis breaks.

    Queries open positions joined with latest quotes and the most recent
    thesis red_flag_score. Returns alerts for any breaches detected.

    This is a read-only operation — no state mutations.
    """
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT
                p.instrument_id,
                i.symbol,
                p.current_units,
                bp.stop_loss_rate,
                bp.take_profit_rate,
                q.bid AS latest_bid,
                q.ask AS latest_ask,
                t.red_flag_score
            FROM positions p
            JOIN instruments i ON i.instrument_id = p.instrument_id
            LEFT JOIN LATERAL (
                SELECT stop_loss_rate, take_profit_rate
                FROM broker_positions
                WHERE instrument_id = p.instrument_id
                ORDER BY updated_at DESC
                LIMIT 1
            ) bp ON TRUE
            LEFT JOIN LATERAL (
                SELECT bid, ask
                FROM quotes
                WHERE instrument_id = p.instrument_id
                ORDER BY quoted_at DESC
                LIMIT 1
            ) q ON TRUE
            LEFT JOIN LATERAL (
                SELECT red_flag_score
                FROM theses
                WHERE instrument_id = p.instrument_id
                ORDER BY created_at DESC
                LIMIT 1
            ) t ON TRUE
            WHERE p.current_units > 0
            ORDER BY p.instrument_id
            """,
        )
        positions = cur.fetchall()

    alerts: list[MonitorAlert] = []

    for pos in positions:
        instrument_id: int = pos["instrument_id"]
        symbol: str = pos["symbol"]
        sl = pos["stop_loss_rate"]
        tp = pos["take_profit_rate"]
        bid = pos["latest_bid"]
        red_flag = pos["red_flag_score"]

        # SL breach: current bid < stop_loss_rate
        if sl is not None and bid is not None and bid < sl:
            alerts.append(
                MonitorAlert(
                    instrument_id=instrument_id,
                    symbol=symbol,
                    alert_type="sl_breach",
                    detail=f"bid={bid} < sl={sl}",
                    current_bid=bid,
                )
            )

        # TP breach: current bid >= take_profit_rate
        if tp is not None and bid is not None and bid >= tp:
            alerts.append(
                MonitorAlert(
                    instrument_id=instrument_id,
                    symbol=symbol,
                    alert_type="tp_breach",
                    detail=f"bid={bid} >= tp={tp}",
                    current_bid=bid,
                )
            )

        # Thesis break: red_flag_score >= threshold
        if red_flag is not None and red_flag >= EXIT_RED_FLAG_THRESHOLD:
            alerts.append(
                MonitorAlert(
                    instrument_id=instrument_id,
                    symbol=symbol,
                    alert_type="thesis_break",
                    detail=f"red_flag={red_flag} >= {EXIT_RED_FLAG_THRESHOLD}",
                    current_bid=bid,
                )
            )

    return MonitorResult(
        positions_checked=len(positions),
        alerts=alerts,
    )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_position_monitor.py -v`
Expected: 6 PASSED

- [ ] **Step 5: Commit**

```bash
git add app/services/position_monitor.py tests/test_position_monitor.py
git commit -m "feat(#205): position monitor service — intraday SL/TP/thesis break detection"
```

---

## Task 4: Wire scheduler — deferred retry job

**Files:**
- Modify: `app/workers/scheduler.py` (add job constant, ScheduledJob entry, job function)
- Test: `tests/test_scheduler_autonomous.py`

### Step-by-step

- [ ] **Step 1: Write the failing test — scheduler job invokes retry service**

```python
"""Tests for autonomous operation loop scheduler jobs."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


class TestRetryDeferredRecommendationsJob:
    """Tests for the retry_deferred_recommendations scheduler job."""

    @patch("app.workers.scheduler.psycopg.connect")
    @patch("app.workers.scheduler.retry_deferred_recommendations")
    @patch("app.workers.scheduler.record_job_start", return_value=1)
    @patch("app.workers.scheduler.record_job_finish")
    @patch("app.workers.scheduler.check_row_count_spike")
    def test_job_calls_retry_service(
        self,
        mock_spike: MagicMock,
        mock_finish: MagicMock,
        mock_start: MagicMock,
        mock_retry: MagicMock,
        mock_connect: MagicMock,
    ) -> None:
        from app.services.deferred_retry import RetryResult
        from app.workers.scheduler import retry_deferred_recommendations_job

        mock_retry.return_value = RetryResult(
            retried=2, re_proposed=1, re_deferred=1, expired=0, errors=0
        )
        mock_conn = MagicMock()
        mock_connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_connect.return_value.__exit__ = MagicMock(return_value=False)
        mock_spike.return_value = MagicMock(flagged=False)

        retry_deferred_recommendations_job()

        mock_retry.assert_called_once()

    @patch("app.workers.scheduler.psycopg.connect")
    @patch("app.workers.scheduler.retry_deferred_recommendations")
    @patch("app.workers.scheduler.record_job_start", return_value=1)
    @patch("app.workers.scheduler.record_job_finish")
    @patch("app.workers.scheduler.check_row_count_spike")
    def test_job_checks_kill_switch(
        self,
        mock_spike: MagicMock,
        mock_finish: MagicMock,
        mock_start: MagicMock,
        mock_retry: MagicMock,
        mock_connect: MagicMock,
    ) -> None:
        """Job must check kill switch before proceeding."""
        from app.workers.scheduler import retry_deferred_recommendations_job

        mock_conn = MagicMock()
        mock_connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_connect.return_value.__exit__ = MagicMock(return_value=False)
        mock_spike.return_value = MagicMock(flagged=False)

        # Simulate kill switch active
        with patch("app.workers.scheduler.load_runtime_config") as mock_config:
            mock_config.return_value = MagicMock(
                kill_switch_active=True,
                enable_auto_trading=True,
            )
            from app.services.deferred_retry import RetryResult

            mock_retry.return_value = RetryResult(
                retried=0, re_proposed=0, re_deferred=0, expired=0, errors=0
            )
            retry_deferred_recommendations_job()

        # When kill switch is active, retry service should NOT be called
        mock_retry.assert_not_called()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_scheduler_autonomous.py -v`
Expected: FAIL (import error — function doesn't exist yet)

- [ ] **Step 3: Add job constant, import, and ScheduledJob entry**

In `app/workers/scheduler.py`, add the job name constant alongside the existing ones (around line 177):

```python
JOB_RETRY_DEFERRED = "retry_deferred_recommendations"
```

Add the import at the top (around line 46):

```python
from app.services.deferred_retry import retry_deferred_recommendations
```

Add the ScheduledJob entry in SCHEDULED_JOBS (after the `execute_approved_orders` entry, before `weekly_coverage_review`):

```python
ScheduledJob(
    name=JOB_RETRY_DEFERRED,
    description="Re-evaluate timing_deferred recommendations with fresh TA data.",
    cadence=Cadence.hourly(minute=30),
    prerequisite=_has_deferred_recommendations,
    catch_up_on_boot=False,
),
```

Add the prerequisite function (after `_has_actionable_recommendations`):

```python
def _has_deferred_recommendations(conn: psycopg.Connection[Any]) -> PrerequisiteResult:
    """True if at least one timing_deferred BUY/ADD recommendation exists."""
    if _exists(
        conn,
        psycopg.sql.SQL(
            "SELECT EXISTS(SELECT 1 FROM trade_recommendations "
            "WHERE status = 'timing_deferred' AND action IN ('BUY', 'ADD'))"
        ),
    ):
        return (True, "")
    return (False, "no timing_deferred BUY/ADD recommendations")
```

- [ ] **Step 4: Add the `load_runtime_config` import and write the job function**

Import `load_runtime_config` (check existing imports first — it may already be imported).

Add the job function (after `execute_approved_orders`):

```python
def retry_deferred_recommendations_job() -> None:
    """Re-evaluate timing_deferred recommendations hourly.

    Checks kill switch and auto-trading flag before proceeding.
    Deferred recs that now pass timing are transitioned to 'proposed'
    so they enter the next execute_approved_orders cycle.
    """
    from app.services.runtime_config import load_runtime_config

    with _tracked_job(JOB_RETRY_DEFERRED) as tracker:
        # Safety gate: kill switch + auto-trading check
        try:
            with psycopg.connect(settings.database_url) as conn:
                config = load_runtime_config(conn)
        except Exception:
            logger.error("retry_deferred: failed to load runtime config", exc_info=True)
            tracker.row_count = 0
            return

        if config.kill_switch_active:
            logger.warning("retry_deferred: kill switch active, skipping")
            tracker.row_count = 0
            return

        if not config.enable_auto_trading:
            logger.info("retry_deferred: auto_trading disabled, skipping")
            tracker.row_count = 0
            return

        try:
            with psycopg.connect(settings.database_url) as conn:
                result = retry_deferred_recommendations(conn)
        except Exception:
            logger.error("retry_deferred: service call failed", exc_info=True)
            tracker.row_count = 0
            return

        tracker.row_count = result.retried + result.expired + result.errors
        logger.info(
            "retry_deferred: retried=%d re_proposed=%d re_deferred=%d expired=%d errors=%d",
            result.retried,
            result.re_proposed,
            result.re_deferred,
            result.expired,
            result.errors,
        )
```

- [ ] **Step 5: Run tests**

Run: `uv run pytest tests/test_scheduler_autonomous.py -v`
Expected: PASSED

- [ ] **Step 6: Commit**

```bash
git add app/workers/scheduler.py tests/test_scheduler_autonomous.py
git commit -m "feat(#205): retry_deferred_recommendations scheduler job — hourly deferred rec retry"
```

---

## Task 5: Wire scheduler — position monitoring job

**Files:**
- Modify: `app/workers/scheduler.py` (add job constant, ScheduledJob entry, job function)
- Modify: `tests/test_scheduler_autonomous.py` (add monitor job tests)

### Step-by-step

- [ ] **Step 1: Write the failing test — position monitor job**

Append to `tests/test_scheduler_autonomous.py`:

```python
class TestMonitorPositionsJob:
    """Tests for the monitor_positions scheduler job."""

    @patch("app.workers.scheduler.psycopg.connect")
    @patch("app.workers.scheduler.check_position_health")
    @patch("app.workers.scheduler.record_job_start", return_value=1)
    @patch("app.workers.scheduler.record_job_finish")
    @patch("app.workers.scheduler.check_row_count_spike")
    def test_job_calls_monitor_service(
        self,
        mock_spike: MagicMock,
        mock_finish: MagicMock,
        mock_start: MagicMock,
        mock_monitor: MagicMock,
        mock_connect: MagicMock,
    ) -> None:
        from app.services.position_monitor import MonitorResult
        from app.workers.scheduler import monitor_positions_job

        mock_monitor.return_value = MonitorResult(positions_checked=5, alerts=[])
        mock_conn = MagicMock()
        mock_connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_connect.return_value.__exit__ = MagicMock(return_value=False)
        mock_spike.return_value = MagicMock(flagged=False)

        monitor_positions_job()

        mock_monitor.assert_called_once()

    @patch("app.workers.scheduler.psycopg.connect")
    @patch("app.workers.scheduler.check_position_health")
    @patch("app.workers.scheduler.record_job_start", return_value=1)
    @patch("app.workers.scheduler.record_job_finish")
    @patch("app.workers.scheduler.check_row_count_spike")
    def test_job_logs_alerts(
        self,
        mock_spike: MagicMock,
        mock_finish: MagicMock,
        mock_start: MagicMock,
        mock_monitor: MagicMock,
        mock_connect: MagicMock,
    ) -> None:
        from app.services.position_monitor import MonitorAlert, MonitorResult
        from app.workers.scheduler import monitor_positions_job

        mock_monitor.return_value = MonitorResult(
            positions_checked=1,
            alerts=[
                MonitorAlert(
                    instrument_id=100,
                    symbol="AAPL",
                    alert_type="sl_breach",
                    detail="bid=135 < sl=140",
                ),
            ],
        )
        mock_conn = MagicMock()
        mock_connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_connect.return_value.__exit__ = MagicMock(return_value=False)
        mock_spike.return_value = MagicMock(flagged=False)

        # Should not raise — alerts are logged, not acted upon
        monitor_positions_job()

        mock_monitor.assert_called_once()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_scheduler_autonomous.py::TestMonitorPositionsJob -v`
Expected: FAIL (import error)

- [ ] **Step 3: Add job constant and ScheduledJob entry**

In `app/workers/scheduler.py`, add the job name constant:

```python
JOB_MONITOR_POSITIONS = "monitor_positions"
```

Add the import:

```python
from app.services.position_monitor import check_position_health
```

Add the ScheduledJob entry (after retry_deferred, before weekly_coverage_review):

```python
ScheduledJob(
    name=JOB_MONITOR_POSITIONS,
    description="Check open positions for SL/TP breaches and thesis breaks.",
    cadence=Cadence.hourly(minute=15),
    prerequisite=_has_open_positions,
    catch_up_on_boot=False,
),
```

Add the prerequisite function:

```python
def _has_open_positions(conn: psycopg.Connection[Any]) -> PrerequisiteResult:
    """True if at least one open position exists."""
    if _exists(
        conn,
        psycopg.sql.SQL("SELECT EXISTS(SELECT 1 FROM positions WHERE current_units > 0)"),
    ):
        return (True, "")
    return (False, "no open positions")
```

- [ ] **Step 4: Write the job function**

```python
def monitor_positions_job() -> None:
    """Hourly position health check.

    Detects SL/TP breaches and thesis breaks between daily sync cycles.
    Alerts are logged for now; future work may trigger out-of-cycle
    EXIT recommendations or operator notifications.

    Read-only — does not place orders or modify positions.
    """
    with _tracked_job(JOB_MONITOR_POSITIONS) as tracker:
        try:
            with psycopg.connect(settings.database_url) as conn:
                result = check_position_health(conn)
        except Exception:
            logger.error("monitor_positions: health check failed", exc_info=True)
            tracker.row_count = 0
            return

        tracker.row_count = result.positions_checked

        if result.alerts:
            for alert in result.alerts:
                logger.warning(
                    "monitor_positions: ALERT %s on %s (instrument_id=%d): %s",
                    alert.alert_type,
                    alert.symbol,
                    alert.instrument_id,
                    alert.detail,
                )
        else:
            logger.info(
                "monitor_positions: %d positions checked, no alerts",
                result.positions_checked,
            )
```

- [ ] **Step 5: Run tests**

Run: `uv run pytest tests/test_scheduler_autonomous.py -v`
Expected: All PASSED

- [ ] **Step 6: Commit**

```bash
git add app/workers/scheduler.py tests/test_scheduler_autonomous.py
git commit -m "feat(#205): monitor_positions scheduler job — hourly intraday position health check"
```

---

## Task 6: Pipeline triggers — stamp `timing_deferred_at` on first deferral

**Files:**
- Modify: `app/workers/scheduler.py` (Phase 0 in `execute_approved_orders`)

### Step-by-step

- [ ] **Step 1: Write the failing test — timing_deferred_at is set on deferral**

Append to `tests/test_scheduler_autonomous.py`:

```python
class TestTimingDeferredAtStamp:
    """Verify that Phase 0 stamps timing_deferred_at on first deferral."""

    def test_deferred_at_column_set_in_update_sql(self) -> None:
        """The Phase 0 defer UPDATE must include timing_deferred_at = NOW()."""
        import inspect
        from app.workers.scheduler import execute_approved_orders

        source = inspect.getsource(execute_approved_orders)
        # The defer path that sets status='timing_deferred' must also set
        # timing_deferred_at. We check for it in the SQL string.
        assert "timing_deferred_at" in source, (
            "execute_approved_orders must stamp timing_deferred_at when deferring"
        )
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_scheduler_autonomous.py::TestTimingDeferredAtStamp -v`
Expected: FAIL (timing_deferred_at not in source)

- [ ] **Step 3: Modify the Phase 0 defer UPDATE in `execute_approved_orders`**

In `scheduler.py` around line 1181, the UPDATE that sets `status = 'timing_deferred'` needs an additional column:

Change:
```sql
UPDATE trade_recommendations
SET status = 'timing_deferred',
    stop_loss_rate = %(sl)s,
    take_profit_rate = %(tp)s,
    timing_verdict = %(verdict)s,
    timing_rationale = %(rationale)s
WHERE recommendation_id = %(rid)s
```

To:
```sql
UPDATE trade_recommendations
SET status = 'timing_deferred',
    stop_loss_rate = %(sl)s,
    take_profit_rate = %(tp)s,
    timing_verdict = %(verdict)s,
    timing_rationale = %(rationale)s,
    timing_deferred_at = COALESCE(timing_deferred_at, NOW())
WHERE recommendation_id = %(rid)s
```

Similarly update the `_timing_error_defer` helper function (around line 1062) to also stamp `timing_deferred_at`:

Change:
```sql
UPDATE trade_recommendations
SET status = 'timing_deferred',
    timing_verdict = 'error',
    timing_rationale = %(rationale)s
WHERE recommendation_id = %(rid)s
```

To:
```sql
UPDATE trade_recommendations
SET status = 'timing_deferred',
    timing_verdict = 'error',
    timing_rationale = %(rationale)s,
    timing_deferred_at = COALESCE(timing_deferred_at, NOW())
WHERE recommendation_id = %(rid)s
```

The `COALESCE` ensures that if a rec is deferred multiple times (error → re-proposed → deferred again), the original deferral timestamp is preserved.

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_scheduler_autonomous.py::TestTimingDeferredAtStamp -v`
Expected: PASS

- [ ] **Step 5: Run full test suite to verify no regressions**

Run: `uv run pytest -v`
Expected: All PASSED

- [ ] **Step 6: Commit**

```bash
git add app/workers/scheduler.py tests/test_scheduler_autonomous.py
git commit -m "feat(#205): stamp timing_deferred_at on first deferral for retry expiry tracking"
```

---

## Task 7: Pipeline triggers — chain morning jobs

**Files:**
- Modify: `app/workers/scheduler.py` (add post-completion trigger hooks)

### Step-by-step

- [ ] **Step 1: Write the test — morning_candidate_review triggers execute_approved_orders**

Append to `tests/test_scheduler_autonomous.py`:

```python
class TestPipelineTriggers:
    """Verify that completed pipeline stages trigger downstream jobs."""

    def test_morning_candidate_review_triggers_execution(self) -> None:
        """After morning_candidate_review produces recs, it should
        log a pipeline trigger for execute_approved_orders."""
        import inspect
        from app.workers.scheduler import morning_candidate_review

        source = inspect.getsource(morning_candidate_review)
        # The function must contain a trigger/invocation of execute_approved_orders
        assert "execute_approved_orders" in source, (
            "morning_candidate_review should trigger execute_approved_orders after producing recs"
        )
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_scheduler_autonomous.py::TestPipelineTriggers -v`
Expected: FAIL (`execute_approved_orders` not in `morning_candidate_review` source)

- [ ] **Step 3: Add pipeline trigger to `morning_candidate_review`**

At the end of `morning_candidate_review()`, after the logging of recommendations, add a direct invocation:

```python
# --- Pipeline trigger: if recs were generated, run the execution pipeline ---
actionable_count = sum(
    1 for r in rec_result.recommendations if r.action in ("BUY", "ADD", "EXIT")
)
if actionable_count > 0:
    logger.info(
        "morning_candidate_review: %d actionable recs → triggering execute_approved_orders",
        actionable_count,
    )
    try:
        execute_approved_orders()
    except Exception:
        logger.error(
            "morning_candidate_review: pipeline trigger to execute_approved_orders failed",
            exc_info=True,
        )
```

This replaces the need for a separate cron trigger — when the morning review produces actionable recommendations, execution runs immediately as a pipeline stage rather than waiting for the 06:30 cron. The 06:30 cron remains as a catch-all for approved recs from other sources (e.g., manual approval via API).

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_scheduler_autonomous.py::TestPipelineTriggers -v`
Expected: PASS

- [ ] **Step 5: Run full test suite**

Run: `uv run pytest -v`
Expected: All PASSED

- [ ] **Step 6: Commit**

```bash
git add app/workers/scheduler.py tests/test_scheduler_autonomous.py
git commit -m "feat(#205): pipeline trigger — morning_candidate_review chains execute_approved_orders"
```

---

## Task 8: Add `timing_expired` to status CHECK constraint

**Files:**
- Modify: `sql/028_autonomous_loop.sql`

### Step-by-step

- [ ] **Step 1: Check existing CHECK constraint on `trade_recommendations.status`**

Search for any existing CHECK constraint on the status column. If none exists (the init schema uses bare TEXT), add one. If one exists, expand it.

- [ ] **Step 2: Add the constraint update to the migration**

Append to `sql/028_autonomous_loop.sql`:

```sql
-- Add timing_expired to the status vocabulary.
-- No existing CHECK constraint on status (001_init.sql uses bare TEXT),
-- so we add one now for the expanded set used by the autonomous loop.
-- Wrapped in DO $$ for idempotency.
DO $$
BEGIN
    ALTER TABLE trade_recommendations
        DROP CONSTRAINT IF EXISTS chk_recommendation_status;
    ALTER TABLE trade_recommendations
        ADD CONSTRAINT chk_recommendation_status
        CHECK (status IN (
            'proposed', 'approved', 'rejected', 'executed',
            'execution_failed', 'timing_deferred', 'timing_expired',
            'cancelled'
        ));
END $$;
```

- [ ] **Step 3: Re-apply migration**

Run: `uv run python -c "import psycopg; conn = psycopg.connect('$(grep DATABASE_URL .env | cut -d= -f2-)'); conn.execute(open('sql/028_autonomous_loop.sql').read()); conn.commit(); print('OK')"`
Expected: OK

- [ ] **Step 4: Commit**

```bash
git add sql/028_autonomous_loop.sql
git commit -m "feat(#205): add timing_expired to status CHECK constraint"
```

---

## Task 9: Register new jobs in runtime.py invokers

**Files:**
- Modify: `app/jobs/runtime.py` (add new jobs to `_INVOKERS` dict)

### Step-by-step

- [ ] **Step 1: Read current runtime.py to understand the invoker pattern**

Read `app/jobs/runtime.py` to find the `_INVOKERS` dict and registration pattern.

- [ ] **Step 2: Add new job invokers**

Add entries for the two new jobs:

```python
JOB_RETRY_DEFERRED: retry_deferred_recommendations_job,
JOB_MONITOR_POSITIONS: monitor_positions_job,
```

Import the necessary names from `app.workers.scheduler`.

- [ ] **Step 3: Run full test suite**

Run: `uv run pytest -v`
Expected: All PASSED

- [ ] **Step 4: Run linting and type checks**

Run: `uv run ruff check . && uv run ruff format --check . && uv run pyright`
Expected: All pass

- [ ] **Step 5: Commit**

```bash
git add app/jobs/runtime.py
git commit -m "feat(#205): register retry_deferred and monitor_positions in job runtime"
```

---

## Task 10: Pre-push checks and PR

- [ ] **Step 1: Run full pre-push checklist**

```bash
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv run pytest
```

All four must pass.

- [ ] **Step 2: Fix any failures**

Address lint, format, type, or test errors.

- [ ] **Step 3: Self-review the diff**

Read `.claude/skills/engineering/pre-flight-review.md` and run through the checklist against the full diff.

- [ ] **Step 4: Push and open PR**

```bash
git push -u origin feature/205-autonomous-operation-loop
gh pr create --title "feat(#205): autonomous operation loop" --body "..."
```

- [ ] **Step 5: Wait for review and CI, resolve comments**

Poll `gh pr view <n> --comments` and `gh pr checks <n>` until review posts and CI is green.

---

## What this PR does NOT include (conscious deferrals)

These are explicitly out of scope for this PR and should be tracked as follow-up issues:

1. **Automated EXIT from position monitor alerts** — The monitor detects breaches and logs them, but does not auto-generate EXIT recommendations. This requires careful design around the execution guard's re-check rule and the operator's approval posture. Track as a follow-up issue.

2. **Event-driven pipeline triggers beyond morning chain** — Only the morning_candidate_review → execute_approved_orders chain is implemented. Research → score → rank triggers are deferred because the research jobs are expensive and their cadence should remain operator-controlled.

3. **Operator notification on alerts** — Position monitor alerts are logged only. Slack/email/webhook notifications require infrastructure not yet built.

4. **Retry of re-proposed recs through guard+execute** — When a deferred rec is re-proposed, it waits for the next `execute_approved_orders` cycle (either the pipeline trigger from morning review or the 06:30 cron). An immediate guard+execute chain from the retry job is deferred to keep the retry job lightweight and read-mostly.
